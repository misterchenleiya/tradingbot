package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/core"
	coreexchange "github.com/misterchenleiya/tradingbot/exchange"
	exchangecfg "github.com/misterchenleiya/tradingbot/exchange/config"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	_ "github.com/misterchenleiya/tradingbot/exchange/okx"
	exchangetransport "github.com/misterchenleiya/tradingbot/exchange/transport"
	"github.com/misterchenleiya/tradingbot/execution"
	"github.com/misterchenleiya/tradingbot/exporter"
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
	modeInit          = "init"
	modeLive          = "live"
	modePaper         = "paper"
	modeBackTest      = "back-test"
	modeSQL           = "sql"
	modeResetCooldown = "reset-cooldown"

	singletonTTL                    = 30 * time.Second
	runtimeShutdownTimeout          = 5 * time.Second
	bootstrapShutdownTimeout        = 2 * time.Second
	defaultExporterWSOriginPatterns = "example.com,127.0.0.1:8081,localhost:8081,127.0.0.1:3100,localhost:3100,127.0.0.1:5173,localhost:5173"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		dbPath         string
		mode           string
		source         string
		sqlArg         string
		historyBars    int
		backtestTaskID int64
		version        bool
	)

	flag.StringVar(&dbPath, "db", "./gobot.db", "SQLite db path")
	flag.StringVar(&mode, "mode", modePaper, "init/live/paper/back-test/sql/reset-cooldown")
	flag.StringVar(&source, "source", "", "back-test source")
	flag.IntVar(&historyBars, "history-bars", 500, "back-test warmup bars loaded before source start for indicator stabilization")
	flag.Int64Var(&backtestTaskID, "backtest-task-id", 0, "internal back-test task id")
	flag.StringVar(&sqlArg, "sql", "", "SQL to execute (mode=sql)")
	flag.BoolVar(&version, "version", false, "print version and exit")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Back-test source formats:")
		fmt.Fprintln(os.Stderr, "  exchange:exchange:symbol:timeframes:start[-end][@replay_start]")
		fmt.Fprintln(os.Stderr, "  db:exchange:symbol:timeframes:start[-end][@replay_start]")
		fmt.Fprintln(os.Stderr, "  csv:/path/to/file.csv[,/path/to/file2.csv][:start[-end]][@replay_start]")
		fmt.Fprintln(os.Stderr, "  time format: YYYYMMDD or YYYYMMDD_HHMM (system local timezone), append Z for UTC; date-only inputs default HHMM to 0000")
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:btcusdtp:15m/1h:20260101_1200-20260115_1600\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:solusdtp:3m/15m/1h:20260101-20260118@20260102\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260102_0600\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260102_0600 -history-bars=500\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:solusdtp:15m/1h:20251230_0000Z-20260108_1000Z@20260102_0600Z\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=exchange:okx:btcusdtp:15m/1h/4h:20260101_1200\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=db:okx:btcusdtp:15m/1h/4h:20260101_1200\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=csv:/path/to/okx_btcusdtp_1h_20260101_1200-20260101_2300.csv\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -mode=back-test -source=csv:/path/to/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110_1200\n", os.Args[0])
	}
	flag.Parse()
	if version {
		printVersion()
		return 0
	}

	status := common.NewStatus()
	bootLogger := newInitLogger()
	signalCtx, forceShutdownCh, stopSignal := newShutdownSignalController()
	defer stopSignal()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	lockStatus := singleton.StatusCompleted
	logger := bootLogger
	logFileTimestamp := ""

	store := storage.NewSQLite(storage.Config{Path: dbPath, Logger: bootLogger})
	singletonSvc := singleton.NewService(singleton.ServiceConfig{
		Store:   store,
		TTL:     singletonTTL,
		Version: buildVersionString(),
		Mode:    mode,
		Source:  source,
		Logger:  bootLogger,
	})

	bootstrapServices := []namedService{
		{name: "storage", svc: store},
		{name: "singleton", svc: singletonSvc},
	}
	var startedBootstrap []namedService
	var startedRuntime []namedService

	defer func() {
		cancel()
		runtimeClosed := closeServicesWithinTimeout(startedRuntime, logger, runtimeShutdownTimeout, forceShutdownCh, "runtime")
		if !runtimeClosed && logger != nil {
			logger.Warn("runtime shutdown did not finish cleanly before singleton release")
		}
		if singletonSvc != nil {
			singletonSvc.SetStatus(lockStatus)
		}
		closeServicesWithinTimeout(startedBootstrap, logger, bootstrapShutdownTimeout, forceShutdownCh, "bootstrap")
		if logger != nil {
			logGobotStopped(logger, mode, lockStatus, status.RuntimeString())
			if err := logger.Sync(); err != nil && !isIgnorableLoggerSyncError(err) {
				fmt.Fprintf(os.Stderr, "logger sync failed: %v\n", err)
			}
		}
	}()

	var failedService namedService
	var err error
	startedBootstrap, failedService, err = startServices(ctx, bootstrapServices)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			lockStatus = singleton.StatusAborted
			logger.Info("startup canceled")
			return 0
		}
		lockStatus = singleton.StatusAborted
		if failedService.name == "singleton" && errors.Is(err, singleton.ErrLocked) {
			logger.Error("singleton already running")
		} else {
			logger.Error("start service failed", zap.String("service", failedService.name), zap.Error(err))
		}
		return 1
	}

	logger = withBaseLogger(bootLogger, singletonSvc.Lock)
	updateServiceLogger(logger, startedBootstrap)
	if mode != modeInit {
		configLogger, fileTS, err := newLogger(store)
		if err != nil {
			logger.Error("logger init failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		logger = withBaseLogger(configLogger, singletonSvc.Lock)
		logFileTimestamp = fileTS
		updateServiceLogger(logger, startedBootstrap)
	}
	if singletonSvc.Lock != nil {
		logger.Debug("singleton acquired", zap.Int64("singleton_id", singletonSvc.Lock.ID))
	}
	logGobotStarted(logger, mode)

	switch mode {
	case modeInit:
		if err := initDB(store, dbPath, logger); err != nil {
			logger.Error("init db failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		if configLogger, fileTS, logErr := newLogger(store); logErr != nil {
			logger.Warn("init logger reconfigure failed, keep bootstrap logger", zap.Error(logErr))
		} else {
			logger = withBaseLogger(configLogger, singletonSvc.Lock)
			logFileTimestamp = fileTS
			updateServiceLogger(logger, startedBootstrap)
		}
		select {
		case err := <-singletonSvc.Errors():
			if err != nil {
				logger.Error("singleton heartbeat failed", zap.Error(err))
				lockStatus = singleton.StatusAborted
				return 1
			}
		default:
		}
		logger.Info("app started", zap.String("db", dbPath))
		return 0
	case modeResetCooldown:
		if err := ensureRuntimeSchema(store); err != nil {
			logger.Error("ensure schema failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		tradeDate := time.Now().Format("2006-01-02")
		logger.Info("app started",
			zap.String("db", dbPath),
			zap.String("trade_date", tradeDate),
		)
		affected, exchanges, err := store.ResetTradeCooldownForTradeDate(tradeDate)
		if err != nil {
			logger.Error("reset trade cooldown failed",
				zap.String("trade_date", tradeDate),
				zap.Error(err),
			)
			lockStatus = singleton.StatusAborted
			return 1
		}
		logger.Info("trade cooldown reset",
			zap.String("trade_date", tradeDate),
			zap.Int64("affected", affected),
			zap.Strings("exchanges", exchanges),
		)
		select {
		case err := <-singletonSvc.Errors():
			if err != nil {
				logger.Error("singleton heartbeat failed", zap.Error(err))
				lockStatus = singleton.StatusAborted
				return 1
			}
		default:
		}
		return 0
	case modeLive, modePaper:
		isPaper := mode == modePaper
		if err := ensureRuntimeSchema(store); err != nil {
			logger.Error("ensure schema failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		historyPolicy, err := store.LoadHistoryPolicy()
		if err != nil {
			logger.Error("load history policy config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		retentionSvc := storage.NewOHLCVRetentionService(store, historyPolicy, logger)

		dynamicMarket, err := loadDynamicMarketConfig(store)
		if err != nil {
			logger.Error("load dynamic market config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		exchangeRuntimeCfg, err := loadExchangeModeConfig(store)
		if err != nil {
			logger.Error("load exchange runtime config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}

		strategyManager, err := buildStrategyManagerForMode(store, mode)
		if err != nil {
			logger.Error("load strategy config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		strategyCombos, err := loadStrategyCombos(store)
		if err != nil {
			logger.Error("load strategy combo config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		if strategyManager != nil {
			strategyManager.SetLogger(logger)
		} else {
			logger.Warn("no strategy configured; running without strategy")
		}
		activeStrategyNames := []string(nil)
		if strategyManager != nil {
			activeStrategyNames = strategyManager.StrategyNames()
		}
		coreSvc := core.New(core.Config{
			Strategy:           strategyManager,
			StrategyCombos:     strategyCombos,
			OHLCVStore:         store,
			History:            true,
			FetchUnclosedOHLCV: exchangeRuntimeCfg.FetchUnclosedOHLCV,
			Logger:             logger,
		})
		exporterAddress, err := loadExporterAddress(store)
		if err != nil {
			logger.Error("load exporter config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		exporterWSOriginPatterns, err := loadExporterWSOriginPatterns(store)
		if err != nil {
			logger.Error("load exporter ws origin patterns failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}

		exchanges, err := store.ListExchanges()
		if err != nil {
			logger.Error("load exchanges failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		if err := validateExchangeProxyConfig(exchanges); err != nil {
			logger.Error("validate exchange proxy config failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		planeSources := buildExchangePlaneSources(exchanges)
		for _, source := range planeSources {
			if err := source.Validate(); err != nil {
				logger.Error("invalid exchange plane config",
					zap.String("exchange", source.Name),
					zap.Error(err),
				)
				lockStatus = singleton.StatusAborted
				return 1
			}
		}
		logger.Info("exchange planes prepared",
			zap.Int("exchanges", len(planeSources)),
			zap.Int("market_proxy_configured", countExchangePlaneProxy(planeSources, coreexchange.PlaneMarket)),
			zap.Int("trade_proxy_configured", countExchangePlaneProxy(planeSources, coreexchange.PlaneTrade)),
		)

		runtimeClients, err := buildExchangeRuntimeClients(exchanges, planeSources, logger)
		if err != nil {
			logger.Error("build exchange runtime clients failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		marketClients := runtimeClients.Market
		logger.Info("market planes prepared",
			zap.Int("market_exchanges_ready", len(marketClients)),
		)
		tradeClients := runtimeClients.Trade
		logger.Info("trade planes prepared",
			zap.Int("trade_exchanges_ready", len(tradeClients)),
		)

		strictMarketPlane := buildStrictMarketPlaneMap(exchanges)
		marketProxyByExchange := buildExchangePlaneProxyMap(exchanges, coreexchange.PlaneMarket)
		exchangeIntervals := market.ExchangeIntervalsFrom(exchanges)
		apiRules := market.DefaultAPILimitRules()
		requestController := market.NewRequestController(market.RequestControllerConfig{
			Logger:            logger,
			ExchangeIntervals: exchangeIntervals,
			APIRules:          apiRules,
		})
		coreSvc.SetRequestController(requestController)

		historyFetcher := &market.HTTPFetcher{
			Logger:             logger,
			Controller:         requestController,
			Exchanges:          marketClients,
			RequireMarketPlane: strictMarketPlane,
			FetchUnclosedOHLCV: false,
		}
		realtimeFetcher := &market.HTTPFetcher{
			Logger:             logger,
			Controller:         requestController,
			Exchanges:          marketClients,
			RequireMarketPlane: strictMarketPlane,
			FetchUnclosedOHLCV: exchangeRuntimeCfg.FetchUnclosedOHLCV,
		}
		coreSvc.SetHistoryFetcher(historyFetcher)
		ws := market.NewMultiWS(logger,
			market.NewBinanceWS(market.BinanceWSConfig{Logger: logger, Proxy: marketProxyByExchange["binance"]}),
			market.NewOKXWS(market.OKXWSConfig{Logger: logger, Proxy: marketProxyByExchange["okx"]}),
			market.NewBitgetWS(market.BitgetWSConfig{Logger: logger, Proxy: marketProxyByExchange["bitget"]}),
			market.NewHyperliquidWS(market.HyperliquidWSConfig{Logger: logger, Proxy: marketProxyByExchange["hyperliquid"]}),
		)
		ws.SetAllowUnclosedOHLCV(exchangeRuntimeCfg.FetchUnclosedOHLCV)
		service := market.NewRealTimeService(market.RealTimeConfig{
			Store:              store,
			Fetcher:            realtimeFetcher,
			DynamicFetcher:     realtimeFetcher,
			WS:                 ws,
			Controller:         requestController,
			Handler:            coreSvc.OnOHLCV,
			ExchangeStatus:     coreSvc.ExchangeStatus,
			DynamicMarket:      dynamicMarket,
			WSEnabled:          true,
			FetchUnclosedOHLCV: exchangeRuntimeCfg.FetchUnclosedOHLCV,
			Timeframe:          "1m",
			Interval:           time.Second,
			Logger:             logger,
		})
		defaultRiskExchange := firstActiveExchangeName(exchanges)
		singletonID := func() int64 {
			if singletonSvc == nil || singletonSvc.Lock == nil {
				return 0
			}
			return singletonSvc.Lock.ID
		}()
		singletonUUID := func() string {
			if singletonSvc == nil || singletonSvc.Lock == nil {
				return ""
			}
			return singletonSvc.Lock.UUID
		}()

		var riskSvc iface.Evaluator
		var executionSvc iface.Executor
		var accountProvider exporter.AccountPositionProvider
		var riskStatus iface.StatusProvider
		var executionStatus iface.StatusProvider

		if isPaper {
			paperRisk := risk.NewBackTest(risk.BackTestConfig{
				Logger:           logger,
				Store:            store,
				StrategyCombos:   strategyCombos,
				ActiveStrategies: activeStrategyNames,
				RunID:            singletonUUID,
				SingletonUUID:    singletonUUID,
				SingletonID:      singletonID,
				Mode:             mode,
			})
			paperExecution := execution.NewBackTest(execution.BackTestConfig{
				Logger:        logger,
				Store:         store,
				Mode:          mode,
				SingletonUUID: singletonUUID,
			})
			riskSvc = paperRisk
			executionSvc = paperExecution
			accountProvider = paperRisk
			riskStatus = paperRisk
			executionStatus = paperExecution
		} else {
			liveRisk := risk.NewLive(risk.LiveConfig{
				Logger:           logger,
				Store:            store,
				Exchanges:        tradeClients,
				StrategyCombos:   strategyCombos,
				ActiveStrategies: activeStrategyNames,
				DefaultExchange:  defaultRiskExchange,
				DefaultTimeframe: "15m",
				SingletonID:      singletonID,
				SingletonUUID:    singletonUUID,
			})
			liveExecution := execution.NewLive(execution.LiveConfig{
				Logger:            logger,
				Exchanges:         tradeClients,
				PosMode:           buildExecutionPosModeMap(exchanges),
				DefaultMarginMode: models.MarginModeIsolated,
				Store:             store,
				Mode:              mode,
				SingletonUUID:     singletonUUID,
			})
			riskSvc = liveRisk
			executionSvc = liveExecution
			accountProvider = liveRisk
			riskStatus = liveRisk
			executionStatus = liveExecution
		}
		coreSvc.SetEvaluator(riskSvc)
		coreSvc.SetExecutor(executionSvc)

		var exporterSvc iface.Service
		if strings.TrimSpace(exporterAddress) != "" {
			exporterSvc = exporter.New(exporter.Config{
				Address:                    exporterAddress,
				Version:                    buildVersionString(),
				Mode:                       mode,
				Provider:                   coreSvc,
				SymbolProvider:             store,
				TradingViewRuntime:         service,
				AccountProvider:            accountProvider,
				HistoryStore:               store,
				HistoryRequester:           historyFetcher,
				TradeEvaluator:             riskSvc,
				TradeExecutor:              executionSvc,
				Status:                     status,
				StatusProviders:            []iface.StatusProvider{status, coreSvc, service, riskStatus, executionStatus},
				SingletonUUID:              singletonUUID,
				BacktestRequestController:  requestController,
				BacktestMarketClients:      marketClients,
				BacktestRequireMarketPlane: strictMarketPlane,
				BacktestSingletonTTL:       singletonTTL,
				Logger:                     logger,
				UpdateInterval:             time.Second,
				WSOriginPatterns:           exporterWSOriginPatterns,
			})
		}

		runtimeServices := []namedService{
			{name: "strategy", svc: strategyManager},
			{name: "exporter", svc: exporterSvc},
			{name: "risk", svc: riskSvc},
			{name: "execution", svc: executionSvc},
			{name: "ohlcv_retention", svc: retentionSvc},
		}
		runtimeServices = append(runtimeServices,
			namedService{name: "core", svc: coreSvc},
			namedService{name: "exchange", svc: service},
		)
		startedRuntime, failedService, err = startServices(ctx, runtimeServices)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("startup canceled")
				lockStatus = singleton.StatusAborted
				return 0
			}
			logger.Error("start service failed", zap.String("service", failedService.name), zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}

		logger.Info("app started", zap.String("db", dbPath))
		select {
		case <-ctx.Done():
			logger.Info("shutdown signal received")
		case err := <-singletonSvc.Errors():
			if err != nil {
				logger.Error("singleton heartbeat failed", zap.Error(err))
				lockStatus = singleton.StatusAborted
				return 1
			}
		}
		return 0
	case modeBackTest:
		backtestTaskStatus := ""
		backtestTaskError := ""
		if backtestTaskID > 0 {
			defer func() {
				if backtestTaskStatus == "" {
					return
				}
				if err := store.MarkBacktestTaskFinished(backtestTaskID, backtestTaskStatus, backtestTaskError); err != nil {
					logger.Warn("update back-test task final status failed",
						zap.Int64("task_id", backtestTaskID),
						zap.Error(err),
					)
				}
			}()
		}
		if source == "" {
			fmt.Printf("usage: %s -mode=back-test -source=exchange:okx:btcusdtp:15m:20260101_1200-20260115_1600\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=exchange:okx:solusdtp:3m/15m/1h:20260101-20260118@20260102\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260102_0600\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=exchange:okx:solusdtp:15m/1h:20251230_0000-20260108_1000@20260102_0600 -history-bars=500\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=exchange:okx:btcusdtp:15m/1h/4h:20260101_1200\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=db:okx:btcusdtp:15m/1h/4h:20260101_1200\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=csv:/path/to/okx_btcusdtp_1h_20260101_1200-20260101_2300.csv\n", os.Args[0])
			fmt.Printf("       %s -mode=back-test -source=csv:/path/to/okx_btcusdtp_1h_20260101_1200-20260131_2300.csv:20260110_1200\n", os.Args[0])
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = "missing back-test source"
			lockStatus = singleton.StatusAborted
			return 1
		}
		if err := ensureRuntimeSchema(store); err != nil {
			logger.Error("ensure schema failed", zap.Error(err))
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = err.Error()
			lockStatus = singleton.StatusAborted
			return 1
		}
		logger.Info("skip ohlcv retention in back-test mode")
		strategyManager, err := buildStrategyManagerForMode(store, mode)
		if err != nil {
			logger.Error("load strategy config failed", zap.Error(err))
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = err.Error()
			lockStatus = singleton.StatusAborted
			return 1
		}
		strategyCombos, err := loadStrategyCombos(store)
		if err != nil {
			logger.Error("load strategy combo config failed", zap.Error(err))
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = err.Error()
			lockStatus = singleton.StatusAborted
			return 1
		}
		if strategyManager != nil {
			strategyManager.SetLogger(logger)
		} else {
			logger.Warn("no strategy configured; running without strategy")
		}
		activeStrategyNames := []string(nil)
		if strategyManager != nil {
			activeStrategyNames = strategyManager.StrategyNames()
		}
		backTestMarketClients := map[string]iface.ExchangeMarketDataSource{}
		btExchanges, listErr := store.ListExchanges()
		if listErr != nil {
			logger.Warn("load exchanges for back-test market-plane failed", zap.Error(listErr))
		} else {
			if err := validateExchangeProxyConfig(btExchanges); err != nil {
				logger.Error("validate exchange proxy config failed", zap.Error(err))
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
			runtimeClients, err := buildExchangeRuntimeClients(btExchanges, buildExchangePlaneSources(btExchanges), logger)
			if err != nil {
				logger.Error("build back-test exchange runtime clients failed", zap.Error(err))
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
			backTestMarketClients = runtimeClients.Market
			logger.Info("back-test market planes prepared", zap.Int("market_exchanges_ready", len(backTestMarketClients)))
		}
		strictMarketPlane := buildStrictMarketPlaneMap(btExchanges)
		backTestRequestController := market.NewRequestController(market.RequestControllerConfig{
			Logger:            logger,
			ExchangeIntervals: market.ExchangeIntervalsFrom(btExchanges),
			APIRules:          market.DefaultAPILimitRules(),
		})

		backTestRunID := func() string {
			if singletonSvc == nil || singletonSvc.Lock == nil {
				return ""
			}
			return singletonSvc.Lock.UUID
		}()
		backTestSingletonID := func() int64 {
			if singletonSvc == nil || singletonSvc.Lock == nil {
				return 0
			}
			return singletonSvc.Lock.ID
		}()
		if backtestTaskID > 0 {
			if err := store.MarkBacktestTaskRunning(backtestTaskID, backTestSingletonID, backTestRunID); err != nil {
				logger.Error("mark back-test task running failed",
					zap.Int64("task_id", backtestTaskID),
					zap.Error(err),
				)
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
		}
		riskSvc := risk.NewBackTest(risk.BackTestConfig{
			Logger:           logger,
			Store:            store,
			StrategyCombos:   strategyCombos,
			ActiveStrategies: activeStrategyNames,
			RunID:            backTestRunID,
			SingletonUUID:    backTestRunID,
			SingletonID:      backTestSingletonID,
		})
		executionSvc := execution.NewBackTest(execution.BackTestConfig{
			Logger:        logger,
			Store:         store,
			Mode:          mode,
			SingletonUUID: backTestRunID,
		})

		coreSvc := core.NewBackTest(core.BackTestConfig{
			Strategy:       strategyManager,
			StrategyCombos: strategyCombos,
			OHLCVStore:     store,
			Risk:           riskSvc,
			Executor:       executionSvc,
			Logger:         logger,
		})

		btDone := make(chan error, 1)
		backTestFetcher := &market.HTTPFetcher{
			Logger:             logger,
			Controller:         backTestRequestController,
			Exchanges:          backTestMarketClients,
			RequireMarketPlane: strictMarketPlane,
			FetchUnclosedOHLCV: false,
		}
		if historyBars < 0 {
			logger.Error("invalid history bars", zap.Int("history_bars", historyBars))
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = "invalid history bars"
			lockStatus = singleton.StatusAborted
			return 1
		}
		service := market.NewBackTestService(market.BackTestConfig{
			Source:         source,
			HistoryBars:    historyBars,
			Fetcher:        backTestFetcher,
			Store:          store,
			PreloadHandler: coreSvc.PreloadOHLCV,
			Handler:        coreSvc.OnOHLCV,
			Done:           btDone,
			Logger:         logger,
		})

		var runtimeServices []namedService
		runtimeServices = append(runtimeServices, namedService{name: "strategy", svc: strategyManager})
		runtimeServices = append(runtimeServices,
			namedService{name: "risk", svc: riskSvc},
			namedService{name: "execution", svc: executionSvc},
		)
		runtimeServices = append(runtimeServices,
			namedService{name: "core", svc: coreSvc},
			namedService{name: "exchange", svc: service},
		)
		startedRuntime, failedService, err = startServices(ctx, runtimeServices)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info("startup canceled")
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = "startup canceled"
				lockStatus = singleton.StatusAborted
				return 0
			}
			logger.Error("start service failed", zap.String("service", failedService.name), zap.Error(err))
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = err.Error()
			lockStatus = singleton.StatusAborted
			return 1
		}

		logger.Info("app started", zap.String("db", dbPath), zap.String("source", source))
		select {
		case err := <-btDone:
			if err != nil {
				logger.Error("back-test failed", zap.Error(err))
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
		case <-ctx.Done():
			logger.Info("shutdown signal received")
			backtestTaskStatus = models.BacktestTaskStatusFailed
			backtestTaskError = "shutdown signal received"
			lockStatus = singleton.StatusAborted
			return 1
		case err := <-singletonSvc.Errors():
			if err != nil {
				logger.Error("singleton heartbeat failed", zap.Error(err))
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
		}
		summary, ok := service.Summary()
		if ok {
			coreReport := coreSvc.Finalize()
			riskReport := riskSvc.Finalize()
			riskReport.Trades = coreSvc.ReplayTrades(riskReport.Trades)
			outputDir, outputDirErr := os.Getwd()
			if outputDirErr != nil {
				logger.Warn("resolve back-test summary output dir failed",
					zap.Error(outputDirErr),
				)
				outputDir = "."
			}
			if err := outputBackTestSummary(
				logger,
				summary,
				coreReport,
				riskReport,
				outputDir,
				logFileTimestamp,
				singletonSvc.Lock.ID,
				singletonSvc.Lock.UUID,
			); err != nil {
				logger.Error("print back-test summary failed", zap.Error(err))
				backtestTaskStatus = models.BacktestTaskStatusFailed
				backtestTaskError = err.Error()
				lockStatus = singleton.StatusAborted
				return 1
			}
		} else {
			logger.Warn("back-test summary unavailable")
		}
		backtestTaskStatus = models.BacktestTaskStatusSucceeded
		backtestTaskError = ""
		return 0
	case modeSQL:
		if sqlArg == "" {
			fmt.Printf("usage: %s -mode=sql -sql \"SQL\"\n", os.Args[0])
			lockStatus = singleton.StatusAborted
			return 1
		}
		logger.Info("app started", zap.String("db", dbPath))
		if err := runSQL(store, sqlArg); err != nil {
			logger.Error("sql failed", zap.Error(err))
			lockStatus = singleton.StatusAborted
			return 1
		}
		select {
		case err := <-singletonSvc.Errors():
			if err != nil {
				logger.Error("singleton heartbeat failed", zap.Error(err))
				lockStatus = singleton.StatusAborted
				return 1
			}
		default:
		}
		return 0
	default:
		fmt.Fprintf(os.Stderr, "unknown mode: %s\n", mode)
		lockStatus = singleton.StatusAborted
		return 1
	}
}

type namedService struct {
	name string
	svc  iface.Service
}

type loggerAware interface {
	SetLogger(logger *zap.Logger)
}

func startServices(ctx context.Context, services []namedService) ([]namedService, namedService, error) {
	started := make([]namedService, 0, len(services))
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
	return started, namedService{}, nil
}

func closeServices(services []namedService, logger *zap.Logger) {
	for i := len(services) - 1; i >= 0; i-- {
		svc := services[i]
		if svc.svc == nil {
			continue
		}
		if err := svc.svc.Close(); err != nil && logger != nil {
			logger.Error("close service failed", zap.String("service", svc.name), zap.Error(err))
		}
	}
}

func closeServicesWithinTimeout(services []namedService, logger *zap.Logger, timeout time.Duration, force <-chan struct{}, phase string) bool {
	if len(services) == 0 {
		return true
	}
	doneCh := make(chan struct{})
	go func() {
		closeServices(services, logger)
		close(doneCh)
	}()

	var timerCh <-chan time.Time
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		timerCh = timer.C
		defer timer.Stop()
	}

	select {
	case <-doneCh:
		return true
	case <-timerCh:
		if logger != nil {
			logger.Warn("service shutdown timed out",
				zap.String("phase", phase),
				zap.Int("service_count", len(services)),
				zap.Duration("timeout", timeout),
			)
		}
		return false
	case <-force:
		if logger != nil {
			logger.Warn("forced shutdown requested by second signal",
				zap.String("phase", phase),
				zap.Int("service_count", len(services)),
			)
		}
		return false
	}
}

func newShutdownSignalController() (context.Context, <-chan struct{}, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	forceShutdownCh := make(chan struct{})
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	var forceOnce sync.Once

	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-stopCh:
			return
		}
		select {
		case <-sigCh:
			forceOnce.Do(func() { close(forceShutdownCh) })
		case <-stopCh:
		}
	}()

	stop := func() {
		stopOnce.Do(func() {
			signal.Stop(sigCh)
			close(stopCh)
			cancel()
		})
	}
	return ctx, forceShutdownCh, stop
}

func updateServiceLogger(logger *zap.Logger, services []namedService) {
	if logger == nil {
		return
	}
	for _, svc := range services {
		aware, ok := svc.svc.(loggerAware)
		if !ok {
			continue
		}
		aware.SetLogger(logger)
	}
}

func initDB(store *storage.SQLite, path string, logger *zap.Logger) (err error) {
	if store == nil || store.DB == nil {
		return fmt.Errorf("nil db")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	exists, err := fileExists(path)
	if err != nil {
		return err
	}

	if !exists {
		logger.Info("init db create", zap.String("db", path))
		if err := store.EnsureSchema(); err != nil {
			return err
		}
		if err := storage.SeedDefaults(store.DB); err != nil {
			return err
		}
		return nil
	}

	logger.Info("init db existing", zap.String("db", path))
	if err := store.EnsureSchema(); err != nil {
		return err
	}
	if err := storage.ApplyRuntimeModeIsolationMigrations(store.DB); err != nil {
		return err
	}
	if err := store.EnsureSchema(); err != nil {
		return err
	}
	schemaPlan, err := storage.PlanSchema(store.DB)
	if err != nil {
		return err
	}

	if err := storage.ApplySchemaAutoPlan(store.DB, schemaPlan); err != nil {
		return err
	}
	if schemaPlan.HasRebuild() {
		ok, err := promptInitConfirm(initConfirmPrompt{
			ShowNotice: true,
			Stage:      "Schema Risk",
			Header:     "以下变更可能导致数据丢失：",
			Lines:      buildSchemaDangerSummary(schemaPlan),
			YesAction:  "执行以上 schema 风险变更",
			NoAction:   "跳过以上 schema 风险变更，继续后续检查",
		})
		if err != nil {
			return err
		}
		if ok {
			if err := storage.ApplySchemaRebuildPlan(store.DB, schemaPlan); err != nil {
				return err
			}
		} else {
			fmt.Fprintln(os.Stdout, "[init] 已跳过 schema 风险变更。")
		}
	}

	defaultPlan, err := storage.PlanDefaults(store.DB, storage.SchemaPlan{})
	if err != nil {
		return err
	}
	if err := storage.ApplyDefaultAutoPlan(store.DB, defaultPlan); err != nil {
		return err
	}
	if defaultPlan.HasOverrides() {
		ok, err := promptInitConfirm(initConfirmPrompt{
			Stage:     "Default Overrides",
			Header:    "以下现有值与当前默认值不同：",
			Lines:     buildDefaultOverrideSummary(defaultPlan),
			YesAction: "将以上项目恢复为当前默认值",
			NoAction:  "保留当前数据库中的现有值",
		})
		if err != nil {
			return err
		}
		if ok {
			if err := storage.ApplyDefaultOverridePlan(store.DB, defaultPlan); err != nil {
				return err
			}
		} else {
			fmt.Fprintln(os.Stdout, "[init] 已保留现有默认值差异项，未执行覆盖。")
		}
	}
	return nil
}

func ensureRuntimeSchema(store *storage.SQLite) error {
	if store == nil || store.DB == nil {
		return fmt.Errorf("nil db")
	}
	if err := store.EnsureSchema(); err != nil {
		return err
	}
	if err := storage.ApplyRuntimeModeIsolationMigrations(store.DB); err != nil {
		return err
	}
	if err := store.EnsureSchema(); err != nil {
		return err
	}
	plan, err := storage.PlanSchema(store.DB)
	if err != nil {
		return err
	}
	if plan.HasRebuild() {
		return fmt.Errorf("schema rebuild required, run -mode=init")
	}
	if plan.HasChanges() {
		if err := storage.ApplySchemaAutoPlan(store.DB, plan); err != nil {
			return err
		}
	}
	return nil
}

func withBaseLogger(logger *zap.Logger, lock *singleton.Lock) *zap.Logger {
	if logger == nil {
		return zap.NewNop()
	}
	if lock == nil {
		return logger
	}
	return logger.With(
		zap.Int64("singleton_id", lock.ID),
	)
}

func buildVersionString() string {
	if common.Tag != "" && common.Commit != "" {
		return fmt.Sprintf("%s-%s", common.Tag, common.Commit)
	}
	if common.Tag != "" {
		return common.Tag
	}
	if common.Commit != "" {
		return common.Commit
	}
	return "unknown"
}

func isIgnorableLoggerSyncError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, syscall.ENOTTY) || errors.Is(err, syscall.EINVAL)
}

func printVersion() {
	fmt.Println("tag:", common.Tag)
	fmt.Println("commit:", common.Commit)
	fmt.Println("build_time:", common.BuildTime)
}

func logGobotStarted(logger *zap.Logger, mode string) {
	if logger == nil {
		return
	}
	logger.Info("gobot started",
		zap.String("mode", mode),
		zap.String("tag", common.Tag),
		zap.String("commit", common.Commit),
		zap.String("build_time", common.BuildTime),
	)
}

func logGobotStopped(logger *zap.Logger, mode, status, runtime string) {
	if logger == nil {
		return
	}
	logger.Info("gobot stopped",
		zap.String("mode", mode),
		zap.String("tag", common.Tag),
		zap.String("commit", common.Commit),
		zap.String("build_time", common.BuildTime),
		zap.String("status", status),
		zap.String("runtime", runtime),
	)
}

func newLogger(store *storage.SQLite) (*zap.Logger, string, error) {
	cfg, err := loadLogConfig(store)
	if err != nil {
		return nil, "", err
	}
	fileTS, err := extractTimestampFromLogFilePath(cfg.FilePath)
	if err != nil {
		return nil, "", err
	}
	logger, err := glog.New(cfg)
	if err != nil {
		return nil, "", err
	}
	return logger, fileTS, nil
}

func newInitLogger() *zap.Logger {
	cfg := glog.Config{
		Level:   "info",
		Console: true,
		File:    false,
	}
	logger, err := glog.New(cfg)
	if err != nil {
		return zap.NewNop()
	}
	return logger
}

func loadLogConfig(store *storage.SQLite) (cfg glog.Config, err error) {
	return loadLogConfigOnce(store, true)
}

type runtimeStrategyConfig struct {
	Live     []string                     `json:"live"`
	Paper    []string                     `json:"paper"`
	BackTest []string                     `json:"back-test"`
	Combo    []models.StrategyComboConfig `json:"combo"`
}

type runtimeExchangeConfig struct {
	FetchUnclosedOHLCV bool `json:"fetch_unclosed_ohlcv"`
}

func buildStrategyManagerForMode(store *storage.SQLite, mode string) (*strategy.Manager, error) {
	strategyNames, err := loadStrategyNamesForMode(store, mode)
	if err != nil {
		return nil, err
	}
	if len(strategyNames) == 0 {
		return nil, nil
	}
	strategies, err := strategy.BuildStrategies(strategyNames)
	if err != nil {
		return nil, err
	}
	return strategy.NewManager(strategies...), nil
}

func loadStrategyConfig(store *storage.SQLite) (runtimeStrategyConfig, error) {
	if store == nil {
		return runtimeStrategyConfig{}, fmt.Errorf("nil store")
	}
	value, found, err := store.GetConfigValue("strategy")
	if err != nil {
		return runtimeStrategyConfig{}, err
	}
	if !found {
		return runtimeStrategyConfig{}, fmt.Errorf("missing config.strategy, run -mode=init")
	}
	if strings.TrimSpace(value) == "" {
		return runtimeStrategyConfig{}, fmt.Errorf("empty config.strategy")
	}
	var cfg runtimeStrategyConfig
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return runtimeStrategyConfig{}, fmt.Errorf("invalid config.strategy json: %w", err)
	}
	normalizedCombos, err := normalizeStrategyComboConfigs(cfg.Combo)
	if err != nil {
		return runtimeStrategyConfig{}, err
	}
	cfg.Combo = normalizedCombos
	return cfg, nil
}

func loadStrategyNamesForMode(store *storage.SQLite, mode string) ([]string, error) {
	cfg, err := loadStrategyConfig(store)
	if err != nil {
		return nil, err
	}
	switch mode {
	case modeLive:
		return normalizeStrategyNameList(cfg.Live)
	case modePaper:
		return normalizeStrategyNameList(cfg.Paper)
	case modeBackTest:
		return normalizeStrategyNameList(cfg.BackTest)
	default:
		return nil, fmt.Errorf("unsupported mode for strategy config: %s", mode)
	}
}

func loadStrategyCombos(store *storage.SQLite) ([]models.StrategyComboConfig, error) {
	cfg, err := loadStrategyConfig(store)
	if err != nil {
		return nil, err
	}
	return append([]models.StrategyComboConfig(nil), cfg.Combo...), nil
}

func normalizeStrategyNameList(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, name := range values {
		normalized := strings.ToLower(strings.TrimSpace(name))
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}

func normalizeStrategyComboConfigs(values []models.StrategyComboConfig) ([]models.StrategyComboConfig, error) {
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
			return nil, fmt.Errorf("invalid config.strategy.combo[%d]: duplicated combo %q", i, comboKey)
		}
		seen[comboKey] = struct{}{}
		if combo.TradeEnabled {
			tradeEnabledCount++
			if tradeEnabledCount > 1 {
				return nil, fmt.Errorf("invalid config.strategy.combo: more than one trade_enabled=true combo")
			}
		}
		out = append(out, models.StrategyComboConfig{
			Timeframes:   timeframes,
			TradeEnabled: combo.TradeEnabled,
		})
	}
	return out, nil
}

func loadLogConfigOnce(store *storage.SQLite, allowSeed bool) (cfg glog.Config, err error) {
	if store == nil || store.DB == nil {
		return cfg, fmt.Errorf("nil db")
	}

	rows, err := store.DB.Query(`SELECT name, value FROM config;`)
	if err != nil {
		if allowSeed && strings.Contains(err.Error(), "no such table: config") {
			if schemaErr := store.EnsureSchema(); schemaErr != nil {
				return cfg, schemaErr
			}
			if seedErr := storage.SeedDefaults(store.DB); seedErr != nil {
				return cfg, seedErr
			}
			return loadLogConfigOnce(store, false)
		}
		return cfg, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()

	values := make(map[string]string)
	for rows.Next() {
		var name, value string
		if scanErr := rows.Scan(&name, &value); scanErr != nil {
			return cfg, scanErr
		}
		values[name] = value
	}
	if err := rows.Err(); err != nil {
		return cfg, err
	}

	required := []string{
		"app_name",
		"log_path",
		"log_level",
		"with_console",
		"rotate_time",
		"max_age",
	}
	var missing []string
	for _, key := range required {
		if _, ok := values[key]; !ok {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		if allowSeed && len(values) == 0 {
			if seedErr := storage.SeedDefaults(store.DB); seedErr != nil {
				return cfg, seedErr
			}
			return loadLogConfigOnce(store, false)
		}
		return cfg, fmt.Errorf("missing config keys: %s", strings.Join(missing, ", "))
	}

	appName := strings.TrimSpace(values["app_name"])
	if appName == "" {
		appName = "gobot"
	}

	logPath := strings.TrimSpace(values["log_path"])
	if logPath == "" {
		logPath = "."
	}

	levelStr, err := mapLogLevel(values["log_level"])
	if err != nil {
		return cfg, err
	}

	withConsole, err := parseBool01(values["with_console"])
	if err != nil {
		return cfg, err
	}
	withLogFile := false
	if raw, ok := values["with_log_file"]; ok {
		withLogFile, err = parseBool01(raw)
		if err != nil {
			return cfg, err
		}
	}

	rotateHours, err := parsePositiveInt(values["rotate_time"], "rotate_time")
	if err != nil {
		return cfg, err
	}

	maxAgeDays, err := parsePositiveInt(values["max_age"], "max_age")
	if err != nil {
		return cfg, err
	}

	ts := time.Now().In(time.Local).Format("2006-01-02_150405")
	fileName := fmt.Sprintf("%s_%s.log", ts, appName)
	filePath := filepath.Join(logPath, fileName)
	linkPath := filepath.Join(logPath, fmt.Sprintf("%s.log", appName))

	cfg = glog.Config{
		Level:       levelStr,
		Console:     withConsole,
		File:        withLogFile,
		FilePath:    filePath,
		LinkPath:    linkPath,
		MaxSizeMB:   100,
		MaxBackups:  0,
		MaxAgeDays:  maxAgeDays,
		Compress:    false,
		Development: false,
		RotateHours: rotateHours,
	}
	return cfg, nil
}

func loadDynamicMarketConfig(store *storage.SQLite) (bool, error) {
	if store == nil {
		return false, fmt.Errorf("nil store")
	}
	value, ok, err := store.GetConfigValue("dynamic_market")
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return parseBool01(value)
}

func loadExchangeModeConfig(store *storage.SQLite) (runtimeExchangeConfig, error) {
	cfg := runtimeExchangeConfig{FetchUnclosedOHLCV: false}
	if store == nil {
		return cfg, fmt.Errorf("nil store")
	}
	value, ok, err := store.GetConfigValue("exchange")
	if err != nil {
		return cfg, err
	}
	if !ok || strings.TrimSpace(value) == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return cfg, fmt.Errorf("invalid config.exchange json: %w", err)
	}
	return cfg, nil
}

func loadExporterAddress(store *storage.SQLite) (string, error) {
	if store == nil {
		return "", fmt.Errorf("nil store")
	}
	value, ok, err := store.GetConfigValue("exporter_address")
	if err != nil {
		return "", err
	}
	value = strings.TrimSpace(value)
	if !ok || value == "" {
		return "http://127.0.0.1:8081", nil
	}
	return value, nil
}

func loadExporterWSOriginPatterns(store *storage.SQLite) ([]string, error) {
	if store == nil {
		return nil, fmt.Errorf("nil store")
	}
	value, ok, err := store.GetConfigValue("exporter_ws_origin_patterns")
	if err != nil {
		return nil, err
	}
	if !ok || strings.TrimSpace(value) == "" {
		return splitCSVPatterns(defaultExporterWSOriginPatterns), nil
	}
	return splitCSVPatterns(value), nil
}

func splitCSVPatterns(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func mapLogLevel(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	level, err := strconv.Atoi(raw)
	if err != nil {
		return "", fmt.Errorf("invalid log_level: %s", raw)
	}
	switch level {
	case -1:
		return "debug", nil
	case 0:
		return "info", nil
	case 1:
		return "warn", nil
	case 2:
		return "error", nil
	case 3:
		return "panic", nil
	case 4:
		return "fatal", nil
	default:
		return "", fmt.Errorf("unsupported log_level: %d", level)
	}
}

func fileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

type initConfirmPrompt struct {
	ShowNotice bool
	Stage      string
	Header     string
	Lines      []string
	YesAction  string
	NoAction   string
}

func promptInitConfirm(prompt initConfirmPrompt) (bool, error) {
	if len(prompt.Lines) == 0 {
		return true, nil
	}
	fmt.Fprintln(os.Stdout)
	if prompt.ShowNotice {
		fmt.Fprintln(os.Stdout, "[init] 检测到需要人工确认的数据库变更。")
		fmt.Fprintln(os.Stdout)
	}
	fmt.Fprint(os.Stdout, formatInitConfirmBlock(prompt))
	fmt.Fprint(os.Stdout, "Proceed? [y/N]: ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	input = strings.TrimSpace(strings.ToLower(input))
	return input == "y" || input == "yes", nil
}

func formatInitConfirmBlock(prompt initConfirmPrompt) string {
	var b strings.Builder
	b.WriteString("============================================================\n")
	b.WriteString(fmt.Sprintf("[init confirm] %s\n", prompt.Stage))
	if strings.TrimSpace(prompt.Header) != "" {
		b.WriteString(prompt.Header)
		b.WriteByte('\n')
	}
	for _, line := range prompt.Lines {
		b.WriteString("  - ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
	if prompt.YesAction != "" || prompt.NoAction != "" {
		b.WriteByte('\n')
		b.WriteString("说明：\n")
		if prompt.YesAction != "" {
			b.WriteString("  - 选择 y：")
			b.WriteString(prompt.YesAction)
			b.WriteByte('\n')
		}
		if prompt.NoAction != "" {
			b.WriteString("  - 选择 n：")
			b.WriteString(prompt.NoAction)
			b.WriteByte('\n')
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func buildSchemaDangerSummary(schemaPlan storage.SchemaPlan) []string {
	lines := make([]string, 0, len(schemaPlan.Rebuild))
	for _, item := range schemaPlan.Rebuild {
		detail := strings.Join(item.Reasons, "; ")
		lines = append(lines, fmt.Sprintf("rebuild table %s (%s)", item.Table, detail))
	}
	return lines
}

func buildDefaultOverrideSummary(defaultPlan storage.DefaultPlan) []string {
	lines := make([]string, 0, len(defaultPlan.Overrides))
	for _, item := range defaultPlan.Overrides {
		lines = append(lines, item.Summary)
	}
	return lines
}

func parseBool01(raw string) (bool, error) {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool value: %s", raw)
	}
}

func parsePositiveInt(raw, name string) (int, error) {
	raw = strings.TrimSpace(raw)
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid %s: %s", name, raw)
	}
	return value, nil
}

func runSQL(store *storage.SQLite, stmt string) (err error) {
	stmt = strings.TrimSpace(stmt)
	if stmt == "" {
		return fmt.Errorf("empty sql")
	}

	if store == nil || store.DB == nil {
		return fmt.Errorf("nil db")
	}

	if isQuerySQL(stmt) {
		rows, err := store.DB.Query(stmt)
		if err != nil {
			return err
		}
		defer func() {
			if closeErr := rows.Close(); closeErr != nil {
				if err == nil {
					err = closeErr
				} else {
					err = fmt.Errorf("close rows: %v; %w", closeErr, err)
				}
			}
		}()
		if err := printRows(rows); err != nil {
			return err
		}
		return nil
	}

	result, err := store.DB.Exec(stmt)
	if err != nil {
		return err
	}
	if rowsAffected, err := result.RowsAffected(); err != nil {
		fmt.Fprintf(os.Stderr, "rows affected unavailable: %v\n", err)
	} else {
		fmt.Printf("rows_affected: %d\n", rowsAffected)
	}
	if lastID, err := result.LastInsertId(); err != nil {
		fmt.Fprintf(os.Stderr, "last insert id unavailable: %v\n", err)
	} else {
		fmt.Printf("last_insert_id: %d\n", lastID)
	}
	return nil
}

func isQuerySQL(stmt string) bool {
	trimmed := strings.TrimSpace(stmt)
	if trimmed == "" {
		return false
	}
	trimmed = strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(trimmed, "select"),
		strings.HasPrefix(trimmed, "pragma"),
		strings.HasPrefix(trimmed, "with"),
		strings.HasPrefix(trimmed, "explain"):
		return true
	default:
		return false
	}
}

func printRows(rows *sql.Rows) (err error) {
	if rows == nil {
		return fmt.Errorf("nil rows")
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		fmt.Println("no columns")
		return nil
	}

	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}

	colWidths := make([]int, len(columns))
	for i, col := range columns {
		colWidths[i] = len(col)
	}

	var rowsData [][]string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		row := make([]string, len(columns))
		for i, val := range values {
			row[i] = formatValue(val)
			if len(row[i]) > colWidths[i] {
				colWidths[i] = len(row[i])
			}
		}
		rowsData = append(rowsData, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	printBorder(colWidths)
	printRow(columns, colWidths)
	printBorder(colWidths)
	for _, row := range rowsData {
		printRow(row, colWidths)
	}
	printBorder(colWidths)
	return nil
}

func formatValue(val any) string {
	if val == nil {
		return "NULL"
	}
	switch v := val.(type) {
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func outputBackTestSummary(
	logger *zap.Logger,
	summary market.BackTestSummary,
	coreReport core.BackTestReport,
	riskReport risk.BackTestReport,
	outputDir string,
	logFileTimestamp string,
	singletonID int64,
	singletonUUID string,
) error {
	location := time.Local
	tzLabel := formatLocationLabel(location)

	summaryRows := buildBackTestSummaryRows(
		summary,
		coreReport,
		riskReport,
		location,
		tzLabel,
		singletonID,
		singletonUUID,
	)
	tradeColumns := buildBackTestTradeColumns(tzLabel)
	tradeRows := buildBackTestTradeRows(riskReport.Trades, location)
	tradeSummaryRows := buildBackTestTradeSummaryRows(riskReport)
	openPositionColumns := buildBackTestOpenPositionColumns(tzLabel)
	openPositionRows := buildBackTestOpenPositionRows(riskReport.OpenPositions, location)
	positionEventColumns := buildBackTestPositionEventColumns(tzLabel)
	positionEventRows := buildBackTestPositionEventRows(riskReport.PositionEvents, location)
	eventColumns := buildBackTestEventColumns(tzLabel)
	eventRows := buildBackTestEventRows(coreReport.Events, location)
	tableLines, err := buildBackTestTableLines(
		summaryRows,
		tradeColumns,
		tradeRows,
		tradeSummaryRows,
		openPositionColumns,
		openPositionRows,
		positionEventColumns,
		positionEventRows,
		eventColumns,
		eventRows,
	)
	if err != nil {
		return err
	}
	if err := writeBackTestSummaryFile(tableLines, outputDir, logFileTimestamp); err != nil {
		return err
	}
	logLinesInfo(logger, tableLines)
	printLines(tableLines)
	return nil
}

func writeBackTestSummaryFile(lines []string, outputDir string, logFileTimestamp string) error {
	ts := strings.TrimSpace(logFileTimestamp)
	if ts == "" {
		return fmt.Errorf("empty log file timestamp")
	}
	if _, err := time.Parse("20060102_150405", ts); err != nil {
		return fmt.Errorf("invalid log file timestamp %q: %w", ts, err)
	}
	dir := strings.TrimSpace(outputDir)
	if dir == "" {
		dir = "."
	}
	filePath := filepath.Join(dir, fmt.Sprintf("backtest_summary_%s.txt", ts))
	content := strings.Join(lines, "\n")
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write back-test summary file failed: %w", err)
	}
	linkPath := filepath.Join(dir, "backtest_summary.txt")
	if err := ensureBackTestSummarySymlink(linkPath, filePath); err != nil {
		return fmt.Errorf("update back-test summary symlink failed: %w", err)
	}
	return nil
}

func ensureBackTestSummarySymlink(linkPath, targetPath string) error {
	if strings.TrimSpace(linkPath) == "" {
		return fmt.Errorf("empty summary symlink path")
	}
	if strings.TrimSpace(targetPath) == "" {
		return fmt.Errorf("empty summary target path")
	}
	info, err := os.Lstat(linkPath)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(linkPath); err != nil {
				return err
			}
		} else {
			backup := fmt.Sprintf("%s.%s.bak", linkPath, time.Now().In(time.Local).Format("20060102_150405"))
			if err := os.Rename(linkPath, backup); err != nil {
				return err
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	target := targetPath
	if filepath.Dir(linkPath) == filepath.Dir(targetPath) {
		target = filepath.Base(targetPath)
	}
	return os.Symlink(target, linkPath)
}

func extractTimestampFromLogFilePath(path string) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", fmt.Errorf("empty log file path")
	}
	base := filepath.Base(trimmed)
	ext := filepath.Ext(base)
	if ext == "" {
		return "", fmt.Errorf("log file extension missing: %s", base)
	}
	name := strings.TrimSuffix(base, ext)
	parts := strings.Split(name, "_")
	if len(parts) >= 2 {
		newTS := parts[0] + "_" + parts[1]
		parsed, err := time.Parse("2006-01-02_150405", newTS)
		if err == nil {
			return parsed.Format("20060102_150405"), nil
		}
	}
	if len(parts) >= 3 {
		oldTS := parts[len(parts)-2] + "_" + parts[len(parts)-1]
		if _, err := time.Parse("20060102_150405", oldTS); err == nil {
			return oldTS, nil
		}
	}
	return "", fmt.Errorf("log file name timestamp invalid: %s", base)
}

func buildBackTestTableLines(
	summaryRows [][]string,
	tradeColumns []string,
	tradeRows [][]string,
	tradeSummaryRows [][]string,
	openPositionColumns []string,
	openPositionRows [][]string,
	positionEventColumns []string,
	positionEventRows [][]string,
	eventColumns []string,
	eventRows [][]string,
) ([]string, error) {
	var lines []string
	lines = append(lines, "[back-test summary]")
	tableLines, err := common.BuildTableLines([]string{"item", "value"}, summaryRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	lines = append(lines, "")

	lines = append(lines, "[back-test strategy events]")
	tableLines, err = common.BuildTableLines(eventColumns, eventRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	lines = append(lines, "")

	lines = append(lines, "[back-test risk events]")
	tableLines, err = common.BuildTableLines(positionEventColumns, positionEventRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	lines = append(lines, "")

	lines = append(lines, "[back-test current positions]")
	tableLines, err = common.BuildTableLines(openPositionColumns, openPositionRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	lines = append(lines, "")

	lines = append(lines, "[back-test history positions]")
	tableLines, err = common.BuildTableLines(tradeColumns, tradeRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	lines = append(lines, "")

	lines = append(lines, "[back-test trades summary]")
	tableLines, err = common.BuildTableLines([]string{"item", "value"}, tradeSummaryRows)
	if err != nil {
		return nil, err
	}
	lines = append(lines, tableLines...)
	return lines, nil
}

func buildBackTestSummaryRows(
	summary market.BackTestSummary,
	coreReport core.BackTestReport,
	riskReport risk.BackTestReport,
	location *time.Location,
	tzLabel string,
	singletonID int64,
	singletonUUID string,
) [][]string {
	rows := [][]string{}
	rows = append(rows, []string{"singleton_id", strconv.FormatInt(singletonID, 10)})
	rows = append(rows, []string{"singleton_uuid", strings.TrimSpace(singletonUUID)})
	rows = appendBackTestSourceRows(rows, summary)
	if summary.Source != "" {
		rows = append(rows, []string{"seed", fmt.Sprint(summary.Seed)})
	}
	rows = append(rows, []string{"source_type", summary.SourceType})
	rows = append(rows, []string{"exchange", summary.Exchange})
	rows = append(rows, []string{"symbol", summary.Symbol})
	if len(summary.Timeframes) > 0 {
		rows = append(rows, []string{"timeframes", strings.Join(summary.Timeframes, ",")})
	}
	if timeRange := formatTimeRangeInLocation(summary, location); timeRange != "" {
		rows = append(rows, []string{fmt.Sprintf("time_range(%s)", tzLabel), timeRange})
	}
	if !summary.ReplayStart.IsZero() {
		rows = append(rows, []string{fmt.Sprintf("replay_start(%s)", tzLabel), formatTimeInLocation(summary.ReplayStart, location)})
	}
	rows = append(rows, []string{"history_bars", strconv.Itoa(summary.HistoryBars)})
	if summary.SourceType == "exchange" && len(summary.ExportFiles) > 0 {
		rows = appendExportFileRows(rows, summary.ExportFiles)
	}
	if counts := formatSeriesCounts(summary); counts != "" {
		rows = append(rows, []string{"ohlcv_counts", counts})
	}
	if counts := formatPreloadCounts(summary); counts != "" {
		rows = append(rows, []string{"preload_ohlcv_counts", counts})
	}
	if !summary.StartedAtUTC.IsZero() {
		rows = append(rows, []string{fmt.Sprintf("started_at(%s)", tzLabel), formatTimeInLocation(summary.StartedAtUTC, location)})
	}
	if !summary.EndedAtUTC.IsZero() {
		rows = append(rows, []string{fmt.Sprintf("ended_at(%s)", tzLabel), formatTimeInLocation(summary.EndedAtUTC, location)})
	}
	if duration := summary.Duration(); duration > 0 {
		rows = append(rows, []string{"duration", duration.String()})
	}

	rows = append(rows, []string{"total_return_rate", formatPercent(riskReport.ReturnRate)})
	rows = append(rows, []string{"circuit_breaker", strconv.FormatBool(riskReport.CircuitBreaker)})
	rows = append(rows, []string{fmt.Sprintf("cooldown_until(%s)", tzLabel), formatTimestampInLocation(riskReport.CooldownUntilTS, location)})
	rows = append(rows, []string{"total_trades", strconv.Itoa(riskReport.TotalTrades)})
	rows = append(rows, []string{"closed_trades", strconv.Itoa(riskReport.ClosedTrades)})
	rows = append(rows, []string{"win_trades", strconv.Itoa(riskReport.WinTrades)})
	rows = append(rows, []string{"loss_trades", strconv.Itoa(riskReport.LossTrades)})
	rows = append(rows, []string{"flat_trades", strconv.Itoa(riskReport.FlatTrades)})
	rows = append(rows, []string{"win_rate", formatPercent(riskReport.WinRate)})
	rows = append(rows, []string{"total_pnl", formatFloat(riskReport.TotalPnL)})
	rows = append(rows, []string{"avg_pnl", formatFloat(riskReport.AvgPnL)})
	rows = append(rows, []string{"avg_pnl_rate", formatPercent(riskReport.AvgPnLRate)})
	rows = append(rows, []string{"max_profit", formatFloat(riskReport.MaxProfit)})
	rows = append(rows, []string{"max_loss", formatFloat(riskReport.MaxLoss)})
	rows = append(rows, []string{"forced_closed_trades", strconv.Itoa(riskReport.ForcedCloseCount)})
	rows = append(rows, []string{"open_positions", strconv.Itoa(len(riskReport.OpenPositions))})
	rows = append(rows, []string{"total_position_events", strconv.Itoa(riskReport.TotalPositionEvents)})
	rows = append(rows, []string{"total_events", strconv.Itoa(coreReport.TotalEvents)})
	rows = append(rows, []string{"get_events", strconv.Itoa(coreReport.GetEvents)})
	rows = append(rows, []string{"update_events", strconv.Itoa(coreReport.UpdateEvents)})
	rows = append(rows, []string{"open_signal_events", strconv.Itoa(coreReport.OpenSignalEvents)})
	rows = append(rows, []string{"close_signal_events", strconv.Itoa(coreReport.CloseSignalEvents)})
	rows = append(rows, []string{"risk_rejected_events", strconv.Itoa(coreReport.RiskRejectedEvents)})
	rows = append(rows, []string{"execution_error_events", strconv.Itoa(coreReport.ExecutionErrorEvents)})
	return rows
}

func appendExportFileRows(rows [][]string, exports []string) [][]string {
	if len(exports) == 0 {
		return append(rows, []string{"export_files", "-"})
	}
	rows = append(rows, []string{"export_files", exports[0]})
	for _, file := range exports[1:] {
		rows = append(rows, []string{"", file})
	}
	return rows
}

func appendBackTestSourceRows(rows [][]string, summary market.BackTestSummary) [][]string {
	if summary.SourceType != "csv" {
		return append(rows, []string{"source", summary.Source})
	}
	if len(summary.CSVFiles) == 0 {
		return appendCSVSourceRowsFromRaw(rows, summary.Source)
	}
	pathSet := make(map[string]struct{})
	for _, spec := range summary.CSVFiles {
		sourceName := spec.DisplayFile
		if sourceName == "" {
			sourceName = filepath.Base(spec.Path)
			if sourceName == "." {
				sourceName = ""
			}
			if sourceName == "" {
				sourceName = spec.Path
			}
		}
		if sourceName != "" {
			rows = append(rows, []string{"source", sourceName})
		}
		if spec.DisplayDir != "" {
			pathSet[spec.DisplayDir] = struct{}{}
		}
	}
	rows = append(rows, []string{"file_path", joinSortedKeys(pathSet)})
	return rows
}

func appendCSVSourceRowsFromRaw(rows [][]string, raw string) [][]string {
	files := splitCSVSource(raw)
	pathSet := make(map[string]struct{})
	for _, item := range files {
		filePath, fileName := splitCSVSourceDisplayPath(item)
		sourceName := fileName
		if sourceName == "" {
			sourceName = item
		}
		if sourceName != "" {
			rows = append(rows, []string{"source", sourceName})
		}
		if filePath != "" {
			pathSet[filePath] = struct{}{}
		}
	}
	rows = append(rows, []string{"file_path", joinSortedKeys(pathSet)})
	if len(files) == 0 && strings.TrimSpace(raw) != "" {
		rows = append(rows, []string{"source", raw})
	}
	return rows
}

func joinSortedKeys(values map[string]struct{}) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func splitCSVSource(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if strings.HasPrefix(strings.ToLower(raw), "csv:") {
		raw = strings.TrimSpace(raw[len("csv:"):])
	}
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	files := make([]string, 0, len(parts))
	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		files = append(files, item)
	}
	return files
}

func splitCSVSourceDisplayPath(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	file := filepath.Base(raw)
	if file == "." {
		file = ""
	}
	sepIndex := strings.LastIndexAny(raw, "/\\")
	if sepIndex < 0 {
		return "", file
	}
	dir := raw[:sepIndex]
	if sepIndex == 0 {
		dir = raw[:1]
	}
	if sepIndex == 1 && strings.HasPrefix(raw, "./") {
		dir = "./"
	}
	if sepIndex == 2 && strings.HasPrefix(raw, "../") {
		dir = "../"
	}
	return dir, file
}

func formatSeriesCounts(summary market.BackTestSummary) string {
	if len(summary.Timeframes) == 0 || len(summary.SeriesCounts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(summary.Timeframes))
	for _, timeframe := range summary.Timeframes {
		if count, ok := summary.SeriesCounts[timeframe]; ok {
			parts = append(parts, fmt.Sprintf("%s=%d", timeframe, count))
		}
	}
	return strings.Join(parts, ", ")
}

func formatPreloadCounts(summary market.BackTestSummary) string {
	if len(summary.Timeframes) == 0 || len(summary.PreloadCounts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(summary.Timeframes))
	for _, timeframe := range summary.Timeframes {
		if count, ok := summary.PreloadCounts[timeframe]; ok {
			parts = append(parts, fmt.Sprintf("%s=%d", timeframe, count))
		}
	}
	return strings.Join(parts, ", ")
}

func formatTimeRangeInLocation(summary market.BackTestSummary, location *time.Location) string {
	if summary.RangeStart.IsZero() || summary.RangeEnd.IsZero() {
		return summary.TimeRange
	}
	return fmt.Sprintf("%s ~ %s", formatTimeInLocation(summary.RangeStart, location), formatTimeInLocation(summary.RangeEnd, location))
}

func formatTimeInLocation(value time.Time, location *time.Location) string {
	if value.IsZero() {
		return ""
	}
	if location == nil {
		location = time.Local
	}
	return value.In(location).Format("2006-01-02 15:04:05")
}

func formatTimeStringInLocation(raw string, location *time.Location) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if location == nil {
		location = time.Local
	}
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006/01/02 15:04:05",
		time.RFC3339,
	}
	for _, layout := range layouts {
		if parsed, err := time.ParseInLocation(layout, raw, location); err == nil {
			return parsed.In(location).Format("2006-01-02 15:04:05")
		}
	}
	return raw
}

func formatLocationLabel(location *time.Location) string {
	if location == nil {
		location = time.Local
	}
	now := time.Now().In(location)
	name, offset := now.Zone()
	sign := "+"
	if offset < 0 {
		sign = "-"
		offset = -offset
	}
	hour := offset / 3600
	minute := (offset % 3600) / 60
	return fmt.Sprintf("%s%s%02d:%02d", name, sign, hour, minute)
}

func buildBackTestTradeColumns(tzLabel string) []string {
	return []string{
		"trade_id",
		"exchange",
		"symbol",
		"timeframe",
		"side",
		"entry_price",
		fmt.Sprintf("entry_time(%s)", tzLabel),
		"exit_price",
		fmt.Sprintf("exit_time(%s)", tzLabel),
		"entry_quantity",
		"margin",
		"leverage",
		"profit",
		"profit_rate",
		"max_drawdown_rate",
		"max_profit_rate",
		"close_reason",
		"strategy",
		"strategy_version",
		"status",
	}
}

func buildBackTestTradeRows(trades []risk.BackTestTrade, location *time.Location) [][]string {
	if len(trades) == 0 {
		return nil
	}
	ordered := append([]risk.BackTestTrade(nil), trades...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].TradeID < ordered[j].TradeID
	})
	rows := make([][]string, 0, len(ordered))
	for _, trade := range ordered {
		rows = append(rows, []string{
			strconv.FormatInt(trade.TradeID, 10),
			trade.Exchange,
			trade.Symbol,
			trade.Timeframe,
			trade.Side,
			formatFloat(trade.EntryPrice),
			formatTimestampInLocation(trade.EntryTS, location),
			formatFloat(trade.ExitPrice),
			formatTimestampInLocation(trade.ExitTS, location),
			formatFloat(trade.EntryQuantity),
			formatFloat(trade.Margin),
			formatFloat(trade.Leverage),
			formatFloat(trade.Profit),
			formatPercent(trade.ProfitRate),
			formatPercent(trade.MaxDrawdownRate),
			formatPercent(trade.MaxProfitRate),
			trade.CloseReason,
			trade.Strategy,
			trade.StrategyVersion,
			trade.Status,
		})
	}
	return rows
}

func buildBackTestTradeSummaryRows(report risk.BackTestReport) [][]string {
	takeProfitCount, stopLossCount := countBackTestTradeCloseReasons(report.Trades)
	return [][]string{
		{"total_return_rate", formatPercent(report.ReturnRate)},
		{"trade_count", strconv.Itoa(report.TotalTrades)},
		{"take_profit_count", strconv.Itoa(takeProfitCount)},
		{"stop_loss_count", strconv.Itoa(stopLossCount)},
	}
}

func countBackTestTradeCloseReasons(trades []risk.BackTestTrade) (takeProfitCount int, stopLossCount int) {
	for _, trade := range trades {
		switch trade.CloseReason {
		case "take_profit":
			takeProfitCount++
		case "stop_loss":
			stopLossCount++
		}
	}
	return takeProfitCount, stopLossCount
}

func buildBackTestOpenPositionColumns(tzLabel string) []string {
	return []string{
		"exchange",
		"symbol",
		"timeframe",
		"side",
		"margin_mode",
		"leverage",
		"margin",
		"entry_price",
		fmt.Sprintf("entry_time(%s)", tzLabel),
		"current_price",
		"unrealized_profit",
		"unrealized_profit_rate",
		"tp",
		"sl",
		fmt.Sprintf("updated_time(%s)", tzLabel),
		"strategy",
		"status",
	}
}

func buildBackTestOpenPositionRows(positions []models.Position, location *time.Location) [][]string {
	if len(positions) == 0 {
		return nil
	}
	ordered := append([]models.Position(nil), positions...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Exchange != ordered[j].Exchange {
			return ordered[i].Exchange < ordered[j].Exchange
		}
		if ordered[i].Symbol != ordered[j].Symbol {
			return ordered[i].Symbol < ordered[j].Symbol
		}
		if ordered[i].Timeframe != ordered[j].Timeframe {
			return ordered[i].Timeframe < ordered[j].Timeframe
		}
		return ordered[i].PositionSide < ordered[j].PositionSide
	})

	rows := make([][]string, 0, len(ordered))
	for _, pos := range ordered {
		rows = append(rows, []string{
			pos.Exchange,
			pos.Symbol,
			pos.Timeframe,
			pos.PositionSide,
			pos.MarginMode,
			formatFloat(pos.LeverageMultiplier),
			formatFloat(pos.MarginAmount),
			formatFloat(pos.EntryPrice),
			formatTimeStringInLocation(pos.EntryTime, location),
			formatFloat(pos.CurrentPrice),
			formatFloat(pos.UnrealizedProfitAmount),
			formatPercent(pos.UnrealizedProfitRate),
			formatFloat(pos.TakeProfitPrice),
			formatFloat(pos.StopLossPrice),
			formatTimeStringInLocation(pos.UpdatedTime, location),
			pos.StrategyName,
			pos.Status,
		})
	}
	return rows
}

func buildBackTestPositionEventColumns(tzLabel string) []string {
	return []string{
		"event_id",
		fmt.Sprintf("event_time(%s)", tzLabel),
		fmt.Sprintf("kline_time(%s)", tzLabel),
		"exchange",
		"symbol",
		"timeframe",
		"side",
		"strategy",
		"action",
		"price",
		"quantity",
		"remaining_quantity",
		"margin",
		"leverage",
		"tp",
		"sl",
		"tp_rate",
		"sl_rate",
		"result",
	}
}

func buildBackTestPositionEventRows(events []risk.BackTestPositionEvent, location *time.Location) [][]string {
	if len(events) == 0 {
		return nil
	}
	ordered := append([]risk.BackTestPositionEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftTS := firstPositiveInt64(ordered[i].KlineTS, ordered[i].EventTS)
		rightTS := firstPositiveInt64(ordered[j].KlineTS, ordered[j].EventTS)
		if leftTS != rightTS {
			return leftTS < rightTS
		}
		if ordered[i].EventTS != ordered[j].EventTS {
			return ordered[i].EventTS < ordered[j].EventTS
		}
		return ordered[i].EventID < ordered[j].EventID
	})
	rows := make([][]string, 0, len(ordered))
	for _, event := range ordered {
		rows = append(rows, []string{
			strconv.FormatInt(event.EventID, 10),
			formatTimestampInLocation(event.EventTS, location),
			formatTimestampInLocation(firstPositiveInt64(event.KlineTS, event.EventTS), location),
			event.Exchange,
			event.Symbol,
			event.Timeframe,
			event.Side,
			event.Strategy,
			event.Action,
			formatFloat(event.Price),
			formatFloat(event.Quantity),
			formatFloat(event.RemainingQuantity),
			formatFloat(event.Margin),
			formatFloat(event.Leverage),
			formatFloat(event.TakeProfitPrice),
			formatFloat(event.StopLossPrice),
			formatBackTestTargetRate(event.Side, event.Price, event.TakeProfitPrice),
			formatBackTestTargetRate(event.Side, event.Price, event.StopLossPrice),
			event.Result,
		})
	}
	return rows
}

func buildBackTestEventColumns(tzLabel string) []string {
	return []string{
		"event_id",
		fmt.Sprintf("event_time(%s)", tzLabel),
		"call",
		"event_type",
		"exchange",
		"symbol",
		"timeframe",
		"strategy",
		"strategy_version",
		"changed_fields",
		"action",
		"high_side",
		"mid_side",
		"entry",
		"exit",
		"sl",
		"tp",
		fmt.Sprintf("trigger_time(%s)", tzLabel),
		"exec_result",
	}
}

func buildBackTestEventRows(events []core.BackTestEvent, location *time.Location) [][]string {
	if len(events) == 0 {
		return nil
	}
	ordered := append([]core.BackTestEvent(nil), events...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].EventTS != ordered[j].EventTS {
			return ordered[i].EventTS < ordered[j].EventTS
		}
		return ordered[i].EventID < ordered[j].EventID
	})
	rows := make([][]string, 0, len(ordered))
	for _, event := range ordered {
		rows = append(rows, []string{
			strconv.FormatInt(event.EventID, 10),
			formatTimestampInLocation(event.EventTS, location),
			event.Call,
			event.EventType,
			event.Exchange,
			event.Symbol,
			event.Timeframe,
			event.Strategy,
			event.StrategyVersion,
			event.ChangedFields,
			strconv.Itoa(event.Action),
			strconv.Itoa(event.HighSide),
			strconv.Itoa(event.MidSide),
			formatFloat(event.Entry),
			formatFloat(event.Exit),
			formatFloat(event.SL),
			formatFloat(event.TP),
			formatTimestampInLocation(event.TriggerTS, location),
			event.ExecResult,
		})
	}
	return rows
}

func formatTimestampInLocation(ts int64, location *time.Location) string {
	normalized := normalizeTimestampMS(ts)
	if normalized <= 0 {
		return ""
	}
	if location == nil {
		location = time.Local
	}
	return time.UnixMilli(normalized).In(location).Format("2006-01-02 15:04:05")
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func formatPercent(value float64) string {
	return strconv.FormatFloat(value*100, 'f', 2, 64) + "%"
}

func formatBackTestTargetRate(side string, price float64, target float64) string {
	if price <= 0 || target <= 0 {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "long":
		return formatPercent((target - price) / price)
	case "short":
		return formatPercent((price - target) / price)
	default:
		return ""
	}
}

func buildExchangePlaneSources(exchanges []models.Exchange) []coreexchange.PlaneSource {
	if len(exchanges) == 0 {
		return nil
	}
	sources := make([]coreexchange.PlaneSource, 0, len(exchanges))
	for _, ex := range exchanges {
		name := strings.TrimSpace(ex.Name)
		if name == "" {
			continue
		}
		sources = append(sources, coreexchange.PlaneSource{
			Name:        name,
			RateLimitMS: ex.RateLimitMS,
			MarketProxy: ex.MarketProxy,
			TradeProxy:  ex.TradeProxy,
		})
	}
	return sources
}

func validateExchangeProxyConfig(exchanges []models.Exchange) error {
	for _, item := range exchanges {
		exchange := strings.ToLower(strings.TrimSpace(item.Name))
		if exchange == "" {
			continue
		}
		if err := validateExchangeProxyField(exchange, "market_proxy", item.MarketProxy); err != nil {
			return err
		}
		if err := validateExchangeProxyField(exchange, "trade_proxy", item.TradeProxy); err != nil {
			return err
		}
	}
	return nil
}

func validateExchangeProxyField(exchange, field, raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	if _, err := exchangetransport.NewProxyDialer(value); err != nil {
		return fmt.Errorf("invalid proxy config: exchange=%s field=%s: %w", exchange, field, err)
	}
	return nil
}

func buildExchangePlaneProxyMap(exchanges []models.Exchange, plane coreexchange.Plane) map[string]string {
	out := make(map[string]string, len(exchanges))
	for _, ex := range exchanges {
		key := strings.ToLower(strings.TrimSpace(ex.Name))
		if key == "" {
			continue
		}
		switch plane {
		case coreexchange.PlaneMarket:
			if value := strings.TrimSpace(ex.MarketProxy); value != "" {
				out[key] = value
			}
		case coreexchange.PlaneTrade:
			if value := strings.TrimSpace(ex.TradeProxy); value != "" {
				out[key] = value
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildStrictMarketPlaneMap(exchanges []models.Exchange) map[string]bool {
	out := make(map[string]bool, len(exchanges))
	for _, ex := range exchanges {
		key := strings.ToLower(strings.TrimSpace(ex.Name))
		if key == "" {
			continue
		}
		if strings.TrimSpace(ex.MarketProxy) == "" {
			continue
		}
		out[key] = true
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func buildExecutionPosModeMap(exchanges []models.Exchange) map[string]string {
	out := make(map[string]string, len(exchanges))
	for _, ex := range exchanges {
		key := strings.ToLower(strings.TrimSpace(ex.Name))
		if key == "" {
			continue
		}
		cfg := loadExchangeRuntimeConfig(ex)
		mode := strings.ToLower(strings.TrimSpace(cfg.PosMode))
		if mode == "" {
			continue
		}
		out[key] = mode
	}
	return out
}

func countExchangePlaneProxy(sources []coreexchange.PlaneSource, plane coreexchange.Plane) int {
	count := 0
	for _, source := range sources {
		switch plane {
		case coreexchange.PlaneMarket:
			if strings.TrimSpace(source.MarketProxy) != "" {
				count++
			}
		case coreexchange.PlaneTrade:
			if strings.TrimSpace(source.TradeProxy) != "" {
				count++
			}
		}
	}
	return count
}

type exchangeRuntimeClients struct {
	Market map[string]iface.ExchangeMarketDataSource
	Trade  map[string]iface.Exchange
}

func buildExchangeRuntimeClients(exchanges []models.Exchange, sources []coreexchange.PlaneSource, logger *zap.Logger) (exchangeRuntimeClients, error) {
	out := exchangeRuntimeClients{
		Market: make(map[string]iface.ExchangeMarketDataSource),
		Trade:  make(map[string]iface.Exchange),
	}
	index := make(map[string]models.Exchange, len(exchanges))
	for _, item := range exchanges {
		key := strings.ToLower(strings.TrimSpace(item.Name))
		if key == "" {
			continue
		}
		index[key] = item
	}
	for _, source := range sources {
		key := strings.ToLower(strings.TrimSpace(source.Name))
		if key == "" {
			continue
		}
		item, ok := index[key]
		if !ok {
			item = models.Exchange{Name: source.Name, RateLimitMS: source.RateLimitMS}
		}
		base := loadExchangeRuntimeConfig(item)
		pairCfg, err := coreexchange.BuildPlanePairConfig(source, base)
		if err != nil {
			return out, fmt.Errorf("build exchange plane config failed: exchange=%s: %w", source.Name, err)
		}
		shared, err := shouldSharePlaneInstance(source.MarketProxy, source.TradeProxy)
		if err != nil {
			return out, fmt.Errorf("resolve exchange plane topology failed: exchange=%s: %w", source.Name, err)
		}
		exchangeName := strings.ToLower(strings.TrimSpace(pairCfg.Exchange))
		if exchangeName == "" {
			return out, fmt.Errorf("resolve exchange name failed: exchange=%s", source.Name)
		}
		if shared {
			if err := prepareSharedPlaneClients(exchangeName, pairCfg, &out, logger); err != nil {
				return out, err
			}
			continue
		}
		if err := prepareSplitPlaneClients(exchangeName, pairCfg, &out, logger); err != nil {
			return out, err
		}
	}
	return out, nil
}

func prepareSharedPlaneClients(exchangeName string, pairCfg coreexchange.PlanePairConfig, out *exchangeRuntimeClients, logger *zap.Logger) error {
	sharedCfg := pairCfg.Trade
	if strings.TrimSpace(sharedCfg.Proxy) == "" {
		sharedCfg.Proxy = strings.TrimSpace(pairCfg.Market.Proxy)
	}
	client, clientErr := coreexchange.New(pairCfg.Exchange, sharedCfg)
	if clientErr != nil {
		marketClient, marketErr := coreexchange.NewMarketDataSource(pairCfg.Exchange, pairCfg.Market)
		if marketErr != nil {
			return fmt.Errorf("create shared exchange runtime failed: exchange=%s trade_err=%v market_err=%v", exchangeName, clientErr, marketErr)
		}
		out.Market[exchangeName] = marketClient
		if validateErr := pairCfg.Trade.Validate(); validateErr == nil {
			logger.Warn("skip shared trade-plane exchange due to unsupported trade exchange factory",
				zap.String("exchange", exchangeName),
				zap.Error(clientErr),
			)
		} else {
			logger.Warn("skip trade-plane exchange due to invalid credentials",
				zap.String("exchange", exchangeName),
				zap.Error(validateErr),
			)
		}
		logger.Info("exchange runtime prepared",
			zap.String("exchange", exchangeName),
			zap.String("topology", "market_only"),
		)
		return nil
	}
	marketClient, ok := client.(iface.ExchangeMarketDataSource)
	if !ok {
		return fmt.Errorf("shared exchange runtime missing market-data capability: exchange=%s", exchangeName)
	}
	out.Market[exchangeName] = marketClient
	if err := pairCfg.Trade.Validate(); err != nil {
		logger.Warn("skip trade-plane exchange due to invalid credentials",
			zap.String("exchange", exchangeName),
			zap.Error(err),
		)
	} else {
		out.Trade[exchangeName] = client
	}
	logger.Info("exchange runtime prepared",
		zap.String("exchange", exchangeName),
		zap.String("topology", "shared"),
	)
	return nil
}

func prepareSplitPlaneClients(exchangeName string, pairCfg coreexchange.PlanePairConfig, out *exchangeRuntimeClients, logger *zap.Logger) error {
	marketClient, err := coreexchange.NewMarketDataSource(pairCfg.Exchange, pairCfg.Market)
	if err != nil {
		return fmt.Errorf("create market-plane source failed: exchange=%s: %w", exchangeName, err)
	}
	out.Market[exchangeName] = marketClient
	if err := pairCfg.Trade.Validate(); err != nil {
		logger.Warn("skip trade-plane exchange due to invalid credentials",
			zap.String("exchange", exchangeName),
			zap.Error(err),
		)
		logger.Info("exchange runtime prepared",
			zap.String("exchange", exchangeName),
			zap.String("topology", "split_market_only"),
		)
		return nil
	}
	tradeClient, err := coreexchange.New(pairCfg.Exchange, pairCfg.Trade)
	if err != nil {
		return fmt.Errorf("create trade-plane exchange failed: exchange=%s: %w", exchangeName, err)
	}
	out.Trade[exchangeName] = tradeClient
	logger.Info("exchange runtime prepared",
		zap.String("exchange", exchangeName),
		zap.String("topology", "split"),
	)
	return nil
}

func shouldSharePlaneInstance(marketProxy, tradeProxy string) (bool, error) {
	marketCanonical, err := canonicalPlaneProxy(marketProxy)
	if err != nil {
		return false, err
	}
	tradeCanonical, err := canonicalPlaneProxy(tradeProxy)
	if err != nil {
		return false, err
	}
	if marketCanonical == "" && tradeCanonical == "" {
		return true, nil
	}
	return marketCanonical == tradeCanonical, nil
}

func canonicalPlaneProxy(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	canonical, err := exchangetransport.CanonicalProxyAddress(value)
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(canonical)), nil
}

func loadExchangeRuntimeConfig(exchange models.Exchange) exchangecfg.ExchangeConfig {
	cfg := loadExchangeRuntimeConfigFromEnv(exchange.Name)
	if exchange.RateLimitMS > 0 {
		cfg.RateLimitMS = exchange.RateLimitMS
	}
	keyPayload := strings.TrimSpace(exchange.APIKey)
	if keyPayload == "" {
		return cfg
	}
	var payload exchangeAPIKey
	if err := json.Unmarshal([]byte(keyPayload), &payload); err != nil {
		return cfg
	}
	if strings.TrimSpace(payload.APIKey) != "" {
		cfg.APIKey = strings.TrimSpace(payload.APIKey)
	}
	if strings.TrimSpace(payload.SecretKey) != "" {
		cfg.SecretKey = strings.TrimSpace(payload.SecretKey)
	}
	if strings.TrimSpace(payload.Passphrase) != "" {
		cfg.Passphrase = strings.TrimSpace(payload.Passphrase)
	}
	if strings.TrimSpace(payload.Proxy) != "" {
		cfg.Proxy = strings.TrimSpace(payload.Proxy)
	}
	if strings.TrimSpace(payload.MarginMode) != "" {
		cfg.MarginMode = strings.ToLower(strings.TrimSpace(payload.MarginMode))
	}
	if strings.TrimSpace(payload.PosMode) != "" {
		cfg.PosMode = strings.ToLower(strings.TrimSpace(payload.PosMode))
	}
	if payload.RateLimitMS > 0 {
		cfg.RateLimitMS = payload.RateLimitMS
	}
	if payload.Leverage > 0 {
		cfg.Leverage = payload.Leverage
	}
	if payload.Simulated != nil {
		cfg.Simulated = *payload.Simulated
	}
	return cfg
}

func loadExchangeRuntimeConfigFromEnv(exchange string) exchangecfg.ExchangeConfig {
	prefix := exchangeEnvPrefix(exchange)
	cfg := exchangecfg.ExchangeConfig{
		Name:       strings.ToLower(strings.TrimSpace(exchange)),
		APIKey:     readEnv(prefix + "API_KEY"),
		SecretKey:  readEnv(prefix + "SECRET_KEY"),
		Passphrase: readEnv(prefix + "PASSPHRASE"),
		Proxy:      readEnv(prefix + "PROXY"),
		MarginMode: strings.ToLower(readEnv(prefix + "MARGIN_MODE")),
		PosMode:    strings.ToLower(readEnv(prefix + "POS_MODE")),
	}
	if value, ok := readEnvInt(prefix + "RATE_LIMIT_MS"); ok {
		cfg.RateLimitMS = value
	}
	if value, ok := readEnvInt(prefix + "LEVERAGE"); ok {
		cfg.Leverage = value
	}
	if value, ok := readEnvBool(prefix + "SIMULATED"); ok {
		cfg.Simulated = value
	}
	return cfg
}

func firstActiveExchangeName(exchanges []models.Exchange) string {
	for _, item := range exchanges {
		if !item.Active {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(item.Name))
		if name != "" {
			return name
		}
	}
	for _, item := range exchanges {
		name := strings.ToLower(strings.TrimSpace(item.Name))
		if name != "" {
			return name
		}
	}
	return ""
}

type exchangeAPIKey struct {
	APIKey      string `json:"api_key"`
	SecretKey   string `json:"secret_key"`
	Passphrase  string `json:"passphrase"`
	Proxy       string `json:"proxy"`
	MarginMode  string `json:"margin_mode"`
	PosMode     string `json:"pos_mode"`
	RateLimitMS int    `json:"rate_limit_ms"`
	Leverage    int    `json:"leverage"`
	Simulated   *bool  `json:"simulated"`
}

func exchangeEnvPrefix(exchange string) string {
	replacer := strings.NewReplacer("-", "_", "/", "_", ".", "_", " ", "_")
	name := replacer.Replace(strings.ToUpper(strings.TrimSpace(exchange)))
	return "GOBOT_EXCHANGE_" + name + "_"
}

func readEnv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

func readEnvInt(key string) (int, bool) {
	raw := readEnv(key)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	return value, true
}

func readEnvBool(key string) (bool, bool) {
	raw := readEnv(key)
	if raw == "" {
		return false, false
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, false
	}
	return value, true
}

func normalizeTimestampMS(ts int64) int64 {
	if ts <= 0 {
		return ts
	}
	switch {
	case ts < 1e11:
		return ts * 1000
	case ts > 1e16:
		return ts / 1e6
	case ts > 1e14:
		return ts / 1e3
	default:
		return ts
	}
}

func firstPositiveInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func formatBorder(widths []int) string {
	var b strings.Builder
	b.WriteString("+")
	for _, w := range widths {
		b.WriteString(strings.Repeat("-", w+2))
		b.WriteString("+")
	}
	return b.String()
}

func formatRow(values []string, widths []int) string {
	var b strings.Builder
	b.WriteString("|")
	for i, val := range values {
		b.WriteString(" ")
		b.WriteString(val)
		if padding := widths[i] - len(val); padding > 0 {
			b.WriteString(strings.Repeat(" ", padding))
		}
		b.WriteString(" |")
	}
	return b.String()
}

func logLinesInfo(logger *zap.Logger, lines []string) {
	if logger == nil {
		return
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		logger.Info(line)
	}
}

func printLines(lines []string) {
	for _, line := range lines {
		fmt.Println(line)
	}
}

func printBorder(widths []int) {
	fmt.Println(formatBorder(widths))
}

func printRow(values []string, widths []int) {
	fmt.Println(formatRow(values, widths))
}
