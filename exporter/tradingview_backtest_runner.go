package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	corepkg "github.com/misterchenleiya/tradingbot/core"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/execution"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"github.com/misterchenleiya/tradingbot/risk"
	"github.com/misterchenleiya/tradingbot/singleton"
	"github.com/misterchenleiya/tradingbot/storage"
	"github.com/misterchenleiya/tradingbot/strategy"
	"go.uber.org/zap"
)

const (
	tradingViewBacktestMode         = "back-test"
	tradingViewDefaultVersionString = "unknown"
	tradingViewDefaultSingletonTTL  = 30 * time.Second
)

type tradingViewBacktestRunnerConfig struct {
	Logger             *zap.Logger
	Store              *storage.SQLite
	Version            string
	SingletonTTL       time.Duration
	RequestController  *market.RequestController
	MarketClients      map[string]iface.ExchangeMarketDataSource
	RequireMarketPlane map[string]bool
}

type tradingViewBacktestRunner struct {
	logger *zap.Logger
	cfg    tradingViewBacktestRunnerConfig

	mu      sync.Mutex
	active  *tradingViewBacktestExecution
	closing bool
}

type tradingViewBacktestExecution struct {
	taskID int64
	cancel context.CancelFunc
	done   chan struct{}
}

type tradingViewNamedService struct {
	name string
	svc  iface.Service
}

func newTradingViewBacktestRunner(cfg tradingViewBacktestRunnerConfig) *tradingViewBacktestRunner {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	cfg.Logger = logger
	if strings.TrimSpace(cfg.Version) == "" {
		cfg.Version = tradingViewDefaultVersionString
	}
	if cfg.SingletonTTL <= 0 {
		cfg.SingletonTTL = tradingViewDefaultSingletonTTL
	}
	return &tradingViewBacktestRunner{
		logger: logger,
		cfg:    cfg,
	}
}

func (r *tradingViewBacktestRunner) Launch(task models.BacktestTask) error {
	if r == nil {
		return fmt.Errorf("nil tradingview back-test runner")
	}
	if r.cfg.Store == nil || r.cfg.Store.DB == nil {
		return fmt.Errorf("history store unavailable")
	}
	if r.cfg.RequestController == nil {
		return fmt.Errorf("shared back-test request controller unavailable")
	}
	if len(r.cfg.MarketClients) == 0 {
		return fmt.Errorf("shared back-test market clients unavailable")
	}
	taskID := task.ID
	ctx, cancel := context.WithCancel(context.Background())
	exec := &tradingViewBacktestExecution{
		taskID: taskID,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closing {
		cancel()
		return fmt.Errorf("tradingview back-test runner closing")
	}
	if r.active != nil {
		cancel()
		return fmt.Errorf("back-test already running")
	}
	r.active = exec
	go r.run(exec, ctx, task)
	return nil
}

func (r *tradingViewBacktestRunner) Close(timeout time.Duration) {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.closing = true
	active := r.active
	r.mu.Unlock()
	if active == nil {
		return
	}
	active.cancel()
	if timeout <= 0 {
		<-active.done
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-active.done:
	case <-timer.C:
		r.logger.Warn("timed out waiting for in-process tradingview back-test to stop",
			zap.Int64("task_id", active.taskID),
			zap.Duration("timeout", timeout),
		)
	}
}

func (r *tradingViewBacktestRunner) run(exec *tradingViewBacktestExecution, ctx context.Context, task models.BacktestTask) {
	defer close(exec.done)
	defer func() {
		r.mu.Lock()
		if r.active == exec {
			r.active = nil
		}
		r.mu.Unlock()
	}()

	status := models.BacktestTaskStatusSucceeded
	errorMessage := ""
	if err := r.runTask(ctx, task); err != nil {
		status = models.BacktestTaskStatusFailed
		errorMessage = err.Error()
		r.logger.Error("tradingview in-process back-test failed",
			zap.Int64("task_id", task.ID),
			zap.String("exchange", task.Exchange),
			zap.String("symbol", task.Symbol),
			zap.Error(err),
		)
	}
	if err := r.cfg.Store.MarkBacktestTaskFinished(task.ID, status, errorMessage); err != nil {
		r.logger.Warn("mark tradingview back-test finished failed",
			zap.Int64("task_id", task.ID),
			zap.String("status", status),
			zap.Error(err),
		)
	}
}

func (r *tradingViewBacktestRunner) runTask(ctx context.Context, task models.BacktestTask) (err error) {
	exchange := strings.ToLower(strings.TrimSpace(task.Exchange))
	if exchange == "" {
		return fmt.Errorf("missing back-test exchange")
	}
	if _, ok := r.cfg.MarketClients[exchange]; !ok {
		return fmt.Errorf("shared back-test market client unavailable: exchange=%s", exchange)
	}
	taskStartedAt := time.Now()
	runtimeStore, isolated, err := r.openRuntimeStore()
	if err != nil {
		return err
	}
	if isolated {
		defer func() {
			if closeErr := runtimeStore.Close(); closeErr != nil {
				if err == nil {
					err = fmt.Errorf("close isolated back-test sqlite failed: %w", closeErr)
					return
				}
				r.logger.Warn("close isolated tradingview back-test sqlite failed",
					zap.Int64("task_id", task.ID),
					zap.String("exchange", exchange),
					zap.String("symbol", task.Symbol),
					zap.Error(closeErr),
				)
			}
		}()
	}

	logger := r.logger.With(
		zap.Int64("task_id", task.ID),
		zap.String("exchange", exchange),
		zap.String("symbol", task.Symbol),
		zap.Strings("trade_timeframes", task.TradeTimeframes),
	)
	logger.Info("tradingview in-process back-test runtime ready",
		zap.Bool("isolated_sqlite", isolated),
		zap.String("db_path", strings.TrimSpace(runtimeStore.Path)),
	)
	lockStatus := singleton.StatusCompleted
	singletonSvc := singleton.NewService(singleton.ServiceConfig{
		Store:   runtimeStore,
		TTL:     r.cfg.SingletonTTL,
		Version: r.cfg.Version,
		Mode:    tradingViewBacktestMode,
		Source:  strings.TrimSpace(task.Source),
		Logger:  logger,
	})
	if startErr := singletonSvc.Start(ctx); startErr != nil {
		return fmt.Errorf("start in-process back-test singleton: %w", startErr)
	}
	defer func() {
		singletonSvc.SetStatus(lockStatus)
		if closeErr := singletonSvc.Close(); closeErr != nil {
			if err == nil {
				err = fmt.Errorf("close in-process back-test singleton: %w", closeErr)
				return
			}
			logger.Warn("close in-process back-test singleton failed", zap.Error(closeErr))
		}
	}()
	runID := ""
	singletonID := int64(0)
	if singletonSvc.Lock != nil {
		runID = singletonSvc.Lock.UUID
		singletonID = singletonSvc.Lock.ID
	}
	if err := r.cfg.Store.MarkBacktestTaskRunning(task.ID, singletonID, runID); err != nil {
		lockStatus = singleton.StatusAborted
		return fmt.Errorf("mark in-process back-test task running: %w", err)
	}

	strategyManager, err := loadTradingViewBacktestStrategyManager(runtimeStore)
	if err != nil {
		lockStatus = singleton.StatusAborted
		return err
	}
	strategyCombos, err := loadTradingViewBacktestStrategyCombos(runtimeStore)
	if err != nil {
		lockStatus = singleton.StatusAborted
		return err
	}
	if strategyManager != nil {
		strategyManager.SetLogger(logger)
	}

	activeStrategyNames := []string(nil)
	if strategyManager != nil {
		activeStrategyNames = strategyManager.StrategyNames()
	}
	riskSvc := risk.NewBackTest(risk.BackTestConfig{
		Logger:           logger,
		Store:            runtimeStore,
		StrategyCombos:   strategyCombos,
		ActiveStrategies: activeStrategyNames,
		RunID:            runID,
		SingletonUUID:    runID,
		SingletonID:      singletonID,
		Mode:             tradingViewBacktestMode,
	})
	executionSvc := execution.NewBackTest(execution.BackTestConfig{
		Logger:        logger,
		Store:         runtimeStore,
		Mode:          tradingViewBacktestMode,
		SingletonUUID: runID,
	})
	coreSvc := corepkg.NewBackTest(corepkg.BackTestConfig{
		Strategy:       strategyManager,
		StrategyCombos: strategyCombos,
		OHLCVStore:     runtimeStore,
		Risk:           riskSvc,
		Executor:       executionSvc,
		Logger:         logger,
	})

	backTestFetcher := &market.HTTPFetcher{
		Logger:             logger,
		Controller:         r.cfg.RequestController,
		Exchanges:          r.cfg.MarketClients,
		RequireMarketPlane: r.cfg.RequireMarketPlane,
		FetchUnclosedOHLCV: false,
	}
	historyBars := task.HistoryBars
	if historyBars <= 0 {
		historyBars = tradingViewBacktestHistoryBarsDefault
	}
	btDone := make(chan error, 1)
	service := market.NewBackTestService(market.BackTestConfig{
		Source:         strings.TrimSpace(task.Source),
		HistoryBars:    historyBars,
		Fetcher:        backTestFetcher,
		Store:          runtimeStore,
		PreloadHandler: coreSvc.PreloadOHLCV,
		Handler:        coreSvc.OnOHLCV,
		Done:           btDone,
		Logger:         logger,
	})

	runtimeServices := []tradingViewNamedService{
		{name: "strategy", svc: strategyManager},
		{name: "risk", svc: riskSvc},
		{name: "execution", svc: executionSvc},
		{name: "core", svc: coreSvc},
		{name: "exchange", svc: service},
	}
	started, failed, err := startTradingViewServices(ctx, runtimeServices)
	if err != nil {
		lockStatus = singleton.StatusAborted
		closeTradingViewServices(started, logger)
		if errors.Is(err, context.Canceled) {
			return fmt.Errorf("start in-process back-test canceled")
		}
		return fmt.Errorf("start in-process back-test service %s: %w", failed.name, err)
	}
	defer closeTradingViewServices(started, logger)

	select {
	case err := <-btDone:
		if err != nil {
			lockStatus = singleton.StatusAborted
			return fmt.Errorf("run in-process back-test: %w", err)
		}
	case <-ctx.Done():
		lockStatus = singleton.StatusAborted
		return fmt.Errorf("in-process back-test canceled: %w", ctx.Err())
	case err := <-singletonSvc.Errors():
		if err != nil {
			lockStatus = singleton.StatusAborted
			return fmt.Errorf("in-process back-test singleton heartbeat failed: %w", err)
		}
	}
	logger.Info("tradingview in-process back-test completed",
		zap.Bool("isolated_sqlite", isolated),
		zap.Duration("duration", time.Since(taskStartedAt)),
	)
	return nil
}

func (r *tradingViewBacktestRunner) openRuntimeStore() (*storage.SQLite, bool, error) {
	if r == nil || r.cfg.Store == nil || r.cfg.Store.DB == nil {
		return nil, false, fmt.Errorf("history store unavailable")
	}
	path := strings.TrimSpace(r.cfg.Store.Path)
	if path == "" {
		return r.cfg.Store, false, nil
	}
	runtimeStore := storage.NewSQLite(storage.Config{
		Path:   path,
		Logger: r.logger,
	})
	if err := runtimeStore.Start(context.Background()); err != nil {
		return nil, false, fmt.Errorf("open isolated back-test sqlite failed: %w", err)
	}
	return runtimeStore, true, nil
}

func loadTradingViewBacktestStrategyManager(store *storage.SQLite) (*strategy.Manager, error) {
	names, err := loadTradingViewBacktestStrategyNames(store)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	strategies, err := strategy.BuildStrategies(names)
	if err != nil {
		return nil, err
	}
	return strategy.NewManager(strategies...), nil
}

func loadTradingViewBacktestStrategyNames(store *storage.SQLite) ([]string, error) {
	cfg, err := loadTradingViewBacktestStrategyConfig(store)
	if err != nil {
		return nil, err
	}
	return normalizeTradingViewStrategyNameList(cfg.BackTest)
}

func loadTradingViewBacktestStrategyCombos(store *storage.SQLite) ([]models.StrategyComboConfig, error) {
	cfg, err := loadTradingViewBacktestStrategyConfig(store)
	if err != nil {
		return nil, err
	}
	return append([]models.StrategyComboConfig(nil), cfg.Combo...), nil
}

func loadTradingViewBacktestStrategyConfig(store *storage.SQLite) (tradingViewStrategyConfig, error) {
	if store == nil {
		return tradingViewStrategyConfig{}, fmt.Errorf("nil store")
	}
	value, found, err := store.GetConfigValue("strategy")
	if err != nil {
		return tradingViewStrategyConfig{}, fmt.Errorf("get config.strategy: %w", err)
	}
	if !found {
		return tradingViewStrategyConfig{}, fmt.Errorf("missing config.strategy, run -mode=init")
	}
	if strings.TrimSpace(value) == "" {
		return tradingViewStrategyConfig{}, fmt.Errorf("empty config.strategy")
	}
	var cfg tradingViewStrategyConfig
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return tradingViewStrategyConfig{}, fmt.Errorf("invalid config.strategy json: %w", err)
	}
	normalizedCombos, err := normalizeTradingViewStrategyComboConfigs(cfg.Combo)
	if err != nil {
		return tradingViewStrategyConfig{}, err
	}
	cfg.Combo = normalizedCombos
	return cfg, nil
}

func normalizeTradingViewStrategyNameList(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeTradingViewStrategyComboConfigs(values []models.StrategyComboConfig) ([]models.StrategyComboConfig, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]models.StrategyComboConfig, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	tradeEnabledCount := 0
	for i, combo := range values {
		_, timeframes, comboKey := common.NormalizeStrategyIdentity("", combo.Timeframes, "")
		if len(timeframes) == 0 || comboKey == "" {
			return nil, fmt.Errorf("invalid config.strategy.combo[%d]: empty timeframes", i)
		}
		for _, timeframe := range timeframes {
			if _, ok := market.TimeframeDuration(timeframe); !ok {
				return nil, fmt.Errorf("invalid config.strategy.combo[%d]: unsupported timeframe %q", i, timeframe)
			}
		}
		if _, exists := seen[comboKey]; exists {
			return nil, fmt.Errorf("duplicate config.strategy.combo timeframes: %s", comboKey)
		}
		seen[comboKey] = struct{}{}
		normalized := combo
		normalized.Timeframes = append([]string(nil), timeframes...)
		if normalized.TradeEnabled {
			tradeEnabledCount++
		}
		out = append(out, normalized)
	}
	if len(out) > 0 && tradeEnabledCount == 0 {
		return nil, fmt.Errorf("invalid config.strategy.combo: at least one combo must set trade_enabled=true")
	}
	return out, nil
}

func startTradingViewServices(ctx context.Context, services []tradingViewNamedService) ([]tradingViewNamedService, tradingViewNamedService, error) {
	started := make([]tradingViewNamedService, 0, len(services))
	for _, svc := range services {
		if svc.svc == nil {
			continue
		}
		if ctx != nil {
			select {
			case <-ctx.Done():
				return started, svc, ctx.Err()
			default:
			}
		}
		if err := svc.svc.Start(ctx); err != nil {
			return started, svc, err
		}
		started = append(started, svc)
	}
	return started, tradingViewNamedService{}, nil
}

func closeTradingViewServices(services []tradingViewNamedService, logger *zap.Logger) {
	for i := len(services) - 1; i >= 0; i-- {
		svc := services[i]
		if svc.svc == nil {
			continue
		}
		if err := svc.svc.Close(); err != nil && logger != nil {
			logger.Error("close in-process tradingview back-test service failed",
				zap.String("service", svc.name),
				zap.Error(err),
			)
		}
	}
}
