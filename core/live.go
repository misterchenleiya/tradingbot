package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type Config struct {
	Strategy           iface.Strategy
	StrategyCombos     []models.StrategyComboConfig
	Cache              *OHLCVCache
	OHLCVStore         iface.OHLCVStore
	HistoryFetcher     iface.HistoryRequester
	HistoryArchive     iface.HistoryArchiveMirror
	RequestController  *market.RequestController
	Risk               iface.Evaluator
	Executor           iface.Executor
	History            bool
	HistoryInt         time.Duration
	FetchUnclosedOHLCV bool
	SkipWarmup         bool
	Logger             *zap.Logger
}

type Live struct {
	Strategy           iface.Strategy
	strategyCombos     []strategyComboSpec
	Cache              *OHLCVCache
	ohlcvStore         iface.OHLCVStore
	historyFetcher     iface.HistoryRequester
	historyArchive     iface.HistoryArchiveMirror
	requestController  *market.RequestController
	timeframePlan      *timeframePlan
	assembler          *timeframeAssembler
	risk               iface.Evaluator
	executor           iface.Executor
	persistMu          sync.Mutex
	lastPersisted      map[string]int64
	logger             *zap.Logger
	historyOn          bool
	historyInt         time.Duration
	fetchUnclosedOHLCV bool
	skipWarmup         bool
	warmupCancel       context.CancelFunc
	warmupWG           sync.WaitGroup
	historyCancel      context.CancelFunc
	historyCtx         context.Context
	historyController  *market.RequestController
	historyWG          sync.WaitGroup
	historyMu          sync.Mutex
	historyStates      map[string]*historyState
	historyStarted     map[string]struct{}
	historyNotifyMu    sync.Mutex
	historyNotify      chan historySyncTask
	historyPending     map[string]struct{}
	historyLastNotify  map[string]int64
	historyDebounce    time.Duration
	started            atomic.Bool
	statusMu           sync.RWMutex
	status             iface.ModuleStatus
	exchangeStateMu    sync.RWMutex
	exchangeStates     map[string]exchangeRuntimeState
	boundsMu           sync.RWMutex
	bounds             map[string]symbolBoundState
}

type symbolBoundState struct {
	checked bool
	exists  bool
	ts      int64
}

func New(cfg Config) *Live {
	if cfg.Cache == nil {
		cfg.Cache = NewOHLCVCache()
	}
	if cfg.Logger == nil {
		cfg.Logger = glog.Nop()
	}
	historyInt := cfg.HistoryInt
	if historyInt <= 0 {
		historyInt = time.Minute
	}
	var lastPersisted map[string]int64
	if cfg.OHLCVStore != nil {
		lastPersisted = make(map[string]int64)
	}
	b := &Live{
		Strategy:           cfg.Strategy,
		strategyCombos:     normalizeCoreStrategyCombos(cfg.StrategyCombos),
		Cache:              cfg.Cache,
		ohlcvStore:         cfg.OHLCVStore,
		historyFetcher:     cfg.HistoryFetcher,
		historyArchive:     cfg.HistoryArchive,
		requestController:  cfg.RequestController,
		timeframePlan:      newTimeframePlan(cfg.OHLCVStore, cfg.Logger),
		assembler:          newTimeframeAssembler(),
		risk:               cfg.Risk,
		executor:           cfg.Executor,
		lastPersisted:      lastPersisted,
		logger:             cfg.Logger,
		historyOn:          cfg.History,
		historyInt:         historyInt,
		fetchUnclosedOHLCV: cfg.FetchUnclosedOHLCV,
		skipWarmup:         cfg.SkipWarmup,
		historyStates:      make(map[string]*historyState),
		historyStarted:     make(map[string]struct{}),
		historyNotify:      make(chan historySyncTask, 2048),
		historyPending:     make(map[string]struct{}),
		historyLastNotify:  make(map[string]int64),
		historyDebounce:    historyEventSyncDebounce,
		exchangeStates:     make(map[string]exchangeRuntimeState),
		bounds:             make(map[string]symbolBoundState),
	}
	b.setStatus(coreStateInit, "")
	return b
}

func (b *Live) Start(ctx context.Context) (err error) {
	if b == nil {
		return errors.New("nil core")
	}
	logger := b.logger
	if logger == nil {
		logger = glog.Nop()
	}
	fields := []zap.Field{
		zap.String("strategy", strategyName(b.Strategy)),
		zap.Bool("history_enabled", b.historyOn),
		zap.Duration("history_interval", b.historyInt),
		zap.Bool("skip_warmup", b.skipWarmup),
	}
	logger.Info("core start", fields...)
	defer func() {
		logger.Info("core started")
	}()
	if !b.started.CompareAndSwap(false, true) {
		return errors.New("core already started")
	}
	activeExchanges, err := b.listActiveExchanges()
	if err != nil {
		b.setStatus(coreStateError, err.Error())
		b.started.Store(false)
		return err
	}
	b.initExchangeReadiness(activeExchanges)
	if b.historyOn {
		if err := b.startHistorySync(ctx); err != nil {
			b.setStatus(coreStateError, err.Error())
			b.started.Store(false)
			return err
		}
	}
	if !b.skipWarmup {
		if err := b.startWarmup(ctx); err != nil {
			b.setStatus(coreStateError, err.Error())
			b.started.Store(false)
			return err
		}
		return nil
	}
	if b.historyOn {
		for _, exchange := range activeExchanges {
			b.markExchangeReady(exchange)
			b.startHistoryExchange(exchange)
		}
	} else {
		for _, exchange := range activeExchanges {
			b.markExchangeReady(exchange)
		}
	}
	b.refreshRuntimeStatus()
	return nil
}

func (b *Live) OnOHLCV(data models.MarketData) {
	timeframes := b.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	events := b.expandOHLCVEvents(data, timeframes)
	processed := b.cacheOHLCVEvents(events)
	if len(processed) == 0 {
		return
	}
	b.applyMarketDataEvents(processed)
	if len(b.strategyCombos) > 0 {
		for _, item := range processed {
			b.processStrategyCombos(item)
		}
		return
	}
	if b.Strategy == nil {
		return
	}
	for _, item := range processed {
		if !b.shouldEvaluateStrategyEvent(item, timeframes) {
			continue
		}
		b.evaluatePairStrategy(item)
	}
}

func (b *Live) expandOHLCVEvents(data models.MarketData, timeframes []string) []models.MarketData {
	events := []models.MarketData{data}
	if b == nil || b.assembler == nil {
		return events
	}
	incoming := strings.ToLower(strings.TrimSpace(data.Timeframe))
	smallest := strings.ToLower(strings.TrimSpace(smallestConfiguredTimeframe(timeframes)))
	if incoming != oneMinuteTimeframe && (smallest == "" || incoming != smallest) {
		return events
	}
	assembled := b.assembler.OnTimeframe(data, timeframes)
	if len(assembled) == 0 {
		return events
	}
	return append(events, assembled...)
}

func (b *Live) cacheOHLCVEvents(events []models.MarketData) []models.MarketData {
	if len(events) == 0 {
		return nil
	}
	processed := make([]models.MarketData, 0, len(events))
	for _, item := range events {
		if !b.cacheOHLCVEvent(item) {
			continue
		}
		processed = append(processed, item)
	}
	return processed
}

func (b *Live) cacheOHLCVEvent(data models.MarketData) bool {
	logger := b.logger
	if b.handleLateClosedOHLCV(data) {
		return false
	}
	b.persistOHLCV(data)
	logger.Debug("ohlcv received",
		zap.String("exchange", data.Exchange),
		zap.String("symbol", data.Symbol),
		zap.String("timeframe", data.Timeframe),
	)
	lastTS, ok := b.Cache.LastTS(data.Exchange, data.Symbol, data.Timeframe)
	if ok && data.OHLCV.TS < lastTS {
		deletedCache := b.Cache.DropBeforeOrEqual(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS)
		logger.Warn("out-of-order ohlcv received, cache truncated",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.Int64("ts", data.OHLCV.TS),
			zap.Int64("last_ts", lastTS),
			zap.Int("cache_deleted", deletedCache),
		)
		return false
	}

	b.Cache.AppendOrReplace(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV, data.Closed)
	return true
}

func (b *Live) applyMarketDataEvents(events []models.MarketData) {
	if b == nil || b.risk == nil {
		return
	}
	logger := b.logger
	for _, item := range events {
		if !b.shouldProcessDecisionEvent(item) {
			continue
		}
		if err := b.risk.OnMarketData(item); err != nil {
			logger.Error("risk on market data failed",
				zap.Error(err),
				zap.String("exchange", item.Exchange),
				zap.String("symbol", item.Symbol),
				zap.String("timeframe", item.Timeframe),
				zap.Int64("ts", item.OHLCV.TS),
			)
		}
	}
}

func (b *Live) evaluatePairStrategy(data models.MarketData) {
	logger := b.logger
	snapshot := b.strategySnapshot(data)

	hasPosition, err := hasOpenPositionForPair(b.risk, data.Exchange, data.Symbol)
	if err != nil {
		logger.Warn("skip position-aware strategy flow due to position query error",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.Error(err),
		)
	}
	cachedSignals, err := b.listRiskSignalsByPair(data.Exchange, data.Symbol)
	if err != nil {
		logger.Warn("load risk signal cache failed",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.Error(err),
		)
		cachedSignals = nil
	}
	runSignalFlow(b.signalFlowHooks(), data, snapshot, cachedSignals, hasPosition)
}

func (b *Live) processStrategyCombos(data models.MarketData) {
	if b == nil || b.Strategy == nil || !b.shouldProcessDecisionEvent(data) {
		return
	}
	timeframes := b.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	snapshot := b.strategySnapshot(data)
	covered := coveredStrategyCombos(b.strategyCombos, timeframes)
	combos := triggeredStrategyCombos(b.strategyCombos, timeframes, data.Timeframe)
	b.evaluateStrategyCombos(data, snapshot, covered, combos)
}

func (b *Live) evaluateStrategyCombos(data models.MarketData, snapshot models.MarketSnapshot, covered []strategyComboSpec, combos []strategyComboSpec) {
	if b == nil || b.Strategy == nil {
		return
	}
	logger := b.logger
	if logger == nil {
		logger = glog.Nop()
	}
	cachedSignals, err := b.listRiskSignalsByPair(data.Exchange, data.Symbol)
	if err != nil {
		logger.Warn("load risk signal cache failed",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.Error(err),
		)
		cachedSignals = nil
	}
	allowedCombos := comboKeySet(covered)
	cachedSignals, staleSignals := partitionSignalsByAllowedCombos(cachedSignals, allowedCombos)
	for _, stale := range staleSignals {
		b.evaluateAndExecuteLiveSignal(data, snapshot, models.ClearSignalForRemoval(stale))
	}
	if len(combos) == 0 {
		return
	}
	positionCombos, err := openPositionComboKeysForPair(b.risk, data.Exchange, data.Symbol)
	if err != nil {
		logger.Warn("skip combo-aware position lookup due to position query error",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.Error(err),
		)
		positionCombos = nil
	}
	for _, combo := range combos {
		comboSnapshot, ok := buildComboSnapshot(snapshot, combo)
		if !ok {
			continue
		}
		comboSignals := filterSignalsByComboKey(cachedSignals, combo.ComboKey)
		_, hasPosition := positionCombos[combo.ComboKey]
		runSignalFlow(b.signalFlowHooks(), data, comboSnapshot, comboSignals, hasPosition)
	}
}

func (b *Live) signalFlowHooks() signalFlowHooks {
	return signalFlowHooks{
		strategy: b.Strategy,
		logger:   b.logger,
		applySignal: func(_ string, data models.MarketData, snapshot models.MarketSnapshot, _ *models.Signal, signal models.Signal) {
			b.evaluateAndExecuteLiveSignal(data, snapshot, signal)
		},
		onUpdateNoChange: func(data models.MarketData, snapshot models.MarketSnapshot, current models.Signal) {
			b.refreshTrendGuardCandidateOnUpdate(b.logger, data, snapshot, current)
		},
	}
}

func (b *Live) refreshTrendGuardCandidateOnUpdate(logger *zap.Logger, data models.MarketData, snapshot models.MarketSnapshot, signal models.Signal) {
	if b == nil || b.risk == nil || models.IsEmptySignal(signal) {
		return
	}
	refresher, ok := b.risk.(iface.TrendGuardRefresher)
	if !ok {
		return
	}
	evalCtx := models.RiskEvalContext{
		MarketData: data,
		Snapshot:   &snapshot,
	}
	if err := refresher.RefreshTrendGuardCandidate(signal, evalCtx); err != nil {
		if logger == nil {
			logger = glog.Nop()
		}
		logger.Warn("refresh grouped trend-guard candidate failed",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.Error(err),
		)
	}
}

func (b *Live) strategySnapshot(data models.MarketData) models.MarketSnapshot {
	if b == nil || b.Cache == nil {
		return models.MarketSnapshot{
			Exchange:       data.Exchange,
			Symbol:         data.Symbol,
			EventTimeframe: data.Timeframe,
			EventTS:        data.OHLCV.TS,
			EventClosed:    data.Closed,
		}
	}
	timeframes := b.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	snapshot := b.Cache.SnapshotForTimeframes(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS, timeframes)
	if len(snapshot.Series) == 0 {
		snapshot = b.Cache.Snapshot(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS)
	}
	snapshot.EventClosed = data.Closed
	return snapshot
}

func (b *Live) resolvePairTimeframes(exchange, symbol, incomingTimeframe string) []string {
	fallback := []string{oneMinuteTimeframe}
	if strings.TrimSpace(incomingTimeframe) != "" {
		fallback = normalizePlanTimeframes([]string{incomingTimeframe})
	}
	if b == nil || b.timeframePlan == nil {
		return fallback
	}
	if b.timeframePlan.store == nil {
		configured := b.timeframePlan.ResolveConfigured(exchange, symbol)
		if len(configured) > 0 {
			return configured
		}
		return fallback
	}
	timeframes := b.timeframePlan.Resolve(exchange, symbol)
	if len(timeframes) == 0 {
		return fallback
	}
	return timeframes
}

func (b *Live) shouldEvaluateStrategyEvent(data models.MarketData, configured []string) bool {
	if b == nil || b.Strategy == nil {
		return false
	}
	if !b.shouldProcessDecisionEvent(data) {
		return false
	}
	executionTimeframe := smallestConfiguredTimeframe(configured)
	if executionTimeframe == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(data.Timeframe), executionTimeframe)
}

func (b *Live) shouldProcessDecisionEvent(data models.MarketData) bool {
	if data.Closed {
		return true
	}
	if b == nil {
		return false
	}
	return b.fetchUnclosedOHLCV
}

func smallestConfiguredTimeframe(timeframes []string) string {
	best := ""
	bestDur := time.Duration(0)
	for _, timeframe := range timeframes {
		normalized := strings.ToLower(strings.TrimSpace(timeframe))
		if normalized == "" {
			continue
		}
		dur, ok := market.TimeframeDuration(normalized)
		if !ok || dur <= 0 {
			if best == "" {
				best = normalized
			}
			continue
		}
		if best == "" || bestDur <= 0 || dur < bestDur {
			best = normalized
			bestDur = dur
		}
	}
	return best
}

func (b *Live) handleLateClosedOHLCV(data models.MarketData) bool {
	if b == nil || b.Cache == nil {
		return false
	}
	if !data.Closed {
		return false
	}
	if !b.shouldPersistPairTimeframe(data.Exchange, data.Symbol, data.Timeframe) {
		return false
	}
	lastPersisted, ok := b.lastPersistedTS(data.Exchange, data.Symbol, data.Timeframe)
	if !ok || data.OHLCV.TS >= lastPersisted {
		return false
	}

	cacheExists := b.Cache.HasTS(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS)
	dbExists := false
	if shouldPersistSource(data.Source) && b.ohlcvStore != nil {
		var err error
		dbExists, err = b.ohlcvStore.HasOHLCV(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS)
		if err != nil {
			b.logger.Error("check ohlcv existence failed",
				zap.Error(err),
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.String("timeframe", data.Timeframe),
				zap.Int64("ts", data.OHLCV.TS),
			)
			return true
		}
	}

	if cacheExists && (dbExists || !shouldPersistSource(data.Source) || b.ohlcvStore == nil) {
		b.logger.Debug("late closed ohlcv ignored, already exists",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.Int64("ts", data.OHLCV.TS),
			zap.Int64("last_persisted_ts", lastPersisted),
		)
		return true
	}

	if !cacheExists {
		b.Cache.MergeMarketData(data.Exchange, data.Symbol, data.Timeframe, []models.MarketData{data})
	}
	if !dbExists && shouldPersistSource(data.Source) && b.ohlcvStore != nil {
		if err := b.ohlcvStore.SaveOHLCV(data); err != nil {
			b.logger.Error("save late closed ohlcv failed",
				zap.Error(err),
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.String("timeframe", data.Timeframe),
				zap.Int64("ts", data.OHLCV.TS),
			)
			return true
		}
	}
	if b.historyArchive != nil {
		if err := b.historyArchive.MirrorClosedCandle(data); err != nil {
			b.logger.Error("mirror late closed ohlcv failed",
				zap.Error(err),
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.String("timeframe", data.Timeframe),
				zap.Int64("ts", data.OHLCV.TS),
			)
		}
	}
	b.requestHistoryPairSync(data.Exchange, data.Symbol, "late_closed_candle")
	b.logger.Info("late closed ohlcv backfilled",
		zap.String("exchange", data.Exchange),
		zap.String("symbol", data.Symbol),
		zap.String("timeframe", data.Timeframe),
		zap.Int64("ts", data.OHLCV.TS),
		zap.Int64("last_persisted_ts", lastPersisted),
		zap.Bool("cache_exists", cacheExists),
		zap.Bool("db_exists", dbExists),
	)
	return true
}

func (b *Live) persistOHLCV(data models.MarketData) {
	if b == nil || b.ohlcvStore == nil {
		return
	}
	if !data.Closed {
		return
	}
	if !shouldPersistSource(data.Source) {
		return
	}
	if !b.shouldPersistPairTimeframe(data.Exchange, data.Symbol, data.Timeframe) {
		return
	}
	key := b.stateKey(data.Exchange, data.Symbol, data.Timeframe)
	b.persistMu.Lock()
	lastTS, ok := b.lastPersisted[key]
	b.persistMu.Unlock()
	if ok && data.OHLCV.TS < lastTS {
		deletedDB, err := b.ohlcvStore.DeleteOHLCVBeforeOrEqual(data.Exchange, data.Symbol, data.Timeframe, data.OHLCV.TS)
		if err != nil {
			b.logger.Error("delete out-of-order ohlcv failed",
				zap.Error(err),
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.String("timeframe", data.Timeframe),
				zap.Int64("ts", data.OHLCV.TS),
			)
			return
		}
		b.logger.Warn("out-of-order ohlcv received, db truncated",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.Int64("ts", data.OHLCV.TS),
			zap.Int64("last_ts", lastTS),
			zap.Int64("db_deleted", deletedDB),
		)
		return
	}
	if err := b.ohlcvStore.SaveOHLCV(data); err != nil {
		b.logger.Error("save ohlcv failed",
			zap.Error(err),
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.Int64("ts", data.OHLCV.TS),
		)
		return
	}
	b.persistMu.Lock()
	if !ok || data.OHLCV.TS >= lastTS {
		b.lastPersisted[key] = data.OHLCV.TS
	}
	b.persistMu.Unlock()
	if b.historyArchive != nil {
		if err := b.historyArchive.MirrorClosedCandle(data); err != nil {
			b.logger.Error("mirror closed ohlcv failed",
				zap.Error(err),
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.String("timeframe", data.Timeframe),
				zap.Int64("ts", data.OHLCV.TS),
			)
		}
	}
	b.requestHistoryPairSync(data.Exchange, data.Symbol, "closed_candle")
}

func (b *Live) shouldPersistPairTimeframe(exchange, symbol, timeframe string) bool {
	if strings.TrimSpace(timeframe) == "" {
		return false
	}
	if b == nil || b.timeframePlan == nil {
		return true
	}
	if b.timeframePlan.store == nil {
		return true
	}
	configured := b.timeframePlan.ResolveConfigured(exchange, symbol)
	if len(configured) == 0 {
		return false
	}
	return containsTimeframe(configured, timeframe)
}

func shouldPersistSource(source string) bool {
	_ = source
	return true
}

func (b *Live) lastPersistedTS(exchange, symbol, timeframe string) (int64, bool) {
	if b == nil || b.lastPersisted == nil {
		return 0, false
	}
	key := b.stateKey(exchange, symbol, timeframe)
	b.persistMu.Lock()
	lastTS, ok := b.lastPersisted[key]
	b.persistMu.Unlock()
	return lastTS, ok
}

func (b *Live) stateKey(exchange, symbol, timeframe string) string {
	return exchange + "|" + symbol + "|" + timeframe
}

func strategyName(strategy iface.Strategy) string {
	if strategy == nil {
		return ""
	}
	return strategy.Name()
}

func (b *Live) ListSignals() map[string]map[string]models.Signal {
	if b == nil || b.risk == nil {
		return map[string]map[string]models.Signal{}
	}
	provider, ok := b.risk.(interface {
		ListSignals() map[string]map[string]models.Signal
	})
	if !ok {
		return map[string]map[string]models.Signal{}
	}
	grouped := provider.ListSignals()
	if grouped == nil {
		return map[string]map[string]models.Signal{}
	}
	return grouped
}

func (b *Live) ListRecentClosedOHLCV(exchange, symbol, timeframe string, limit int) ([]models.OHLCV, error) {
	if b == nil || b.Cache == nil {
		return nil, nil
	}
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)
	if exchange == "" || symbol == "" || timeframe == "" {
		return nil, nil
	}
	if limit <= 0 {
		return nil, nil
	}
	series, lastClosedTS := b.Cache.SeriesSnapshot(exchange, symbol, timeframe)
	if len(series) == 0 || lastClosedTS <= 0 {
		return nil, nil
	}
	out := make([]models.OHLCV, 0, limit)
	for i := len(series) - 1; i >= 0 && len(out) < limit; i-- {
		item := series[i]
		if item.TS <= lastClosedTS {
			out = append(out, item)
		}
	}
	return out, nil
}

func (b *Live) Close() (err error) {
	if b == nil {
		return nil
	}
	logger := b.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("core close")
	defer func() {
		logger.Info("core closed")
	}()
	if b.started.CompareAndSwap(true, false) {
		if b.warmupCancel != nil {
			b.warmupCancel()
		}
		b.warmupWG.Wait()
		if b.historyCancel != nil {
			b.historyCancel()
		}
		b.historyWG.Wait()
		b.historyMu.Lock()
		b.historyCtx = nil
		b.historyController = nil
		b.historyStarted = make(map[string]struct{})
		b.historyMu.Unlock()
		b.setStatus(coreStateStopped, "")
		return nil
	}
	return nil
}

func (b *Live) SetLogger(logger *zap.Logger) {
	if b == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	b.logger = logger
}

func (b *Live) SetRequestController(controller *market.RequestController) {
	if b == nil {
		return
	}
	b.requestController = controller
}

func (b *Live) SetHistoryFetcher(fetcher iface.HistoryRequester) {
	if b == nil {
		return
	}
	b.historyFetcher = fetcher
}

func (b *Live) fetchHistoryRangePaged(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time, maxPerRequest int) ([]models.MarketData, error) {
	if b == nil || b.historyFetcher == nil {
		return nil, fmt.Errorf("nil history fetcher")
	}
	return b.historyFetcher.FetchOHLCVRangePaged(ctx, exchange, symbol, timeframe, start, end, maxPerRequest)
}

func (b *Live) fetchHistoryByLimitPaged(ctx context.Context, exchange, symbol, timeframe string, limit, maxPerRequest int) ([]models.MarketData, error) {
	if b == nil || b.historyFetcher == nil {
		return nil, fmt.Errorf("nil history fetcher")
	}
	return b.historyFetcher.FetchOHLCVByLimitPaged(ctx, exchange, symbol, timeframe, limit, maxPerRequest)
}

func (b *Live) loadOHLCVBound(ctx context.Context, exchange, symbol string) (int64, bool) {
	if b == nil || b.ohlcvStore == nil {
		return 0, false
	}
	key := pairKey(exchange, symbol)
	b.boundsMu.RLock()
	state, ok := b.bounds[key]
	b.boundsMu.RUnlock()
	if ok && state.checked {
		return state.ts, state.exists
	}
	ts, exists, err := b.ohlcvStore.GetOHLCVBound(exchange, symbol)
	if err != nil {
		b.logger.Warn("load ohlcv bound failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Error(err),
		)
		return 0, false
	}
	if exists {
		b.cacheOHLCVBound(exchange, symbol, ts, true)
		return ts, true
	}
	if fetcher, ok := b.historyFetcher.(iface.SymbolListTimeFetcher); ok {
		listTime, err := fetcher.FetchSymbolListTime(ctx, exchange, symbol)
		if err != nil {
			b.logger.Warn("fetch symbol listTime failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(err),
			)
			return 0, false
		}
		if listTime > 0 {
			if err := b.ohlcvStore.UpsertOHLCVBound(exchange, symbol, listTime); err != nil {
				b.logger.Warn("persist listTime ohlcv bound failed",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.Int64("earliest_available_ts", listTime),
					zap.Error(err),
				)
			}
			b.cacheOHLCVBound(exchange, symbol, listTime, true)
			return listTime, true
		}
	}
	b.cacheOHLCVBound(exchange, symbol, 0, false)
	return 0, false
}

func (b *Live) cacheOHLCVBound(exchange, symbol string, ts int64, exists bool) {
	if b == nil {
		return
	}
	key := pairKey(exchange, symbol)
	b.boundsMu.Lock()
	b.bounds[key] = symbolBoundState{checked: true, exists: exists, ts: ts}
	b.boundsMu.Unlock()
}

func (b *Live) recordOHLCVBound(exchange, symbol string, earliestAvailableTS int64) {
	if b == nil || b.ohlcvStore == nil || earliestAvailableTS <= 0 {
		return
	}
	if err := b.ohlcvStore.UpsertOHLCVBound(exchange, symbol, earliestAvailableTS); err != nil {
		b.logger.Warn("persist ohlcv bound failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Int64("earliest_available_ts", earliestAvailableTS),
			zap.Error(err),
		)
		return
	}
	b.cacheOHLCVBound(exchange, symbol, earliestAvailableTS, true)
}

func (b *Live) clipBackfillRangeByBound(ctx context.Context, exchange, symbol, timeframe string, fromTS, toTS int64) (int64, int64, bool) {
	if fromTS <= 0 || toTS <= 0 || toTS < fromTS {
		return 0, 0, false
	}
	boundTS, ok := b.loadOHLCVBound(ctx, exchange, symbol)
	if !ok || boundTS <= 0 {
		return fromTS, toTS, true
	}
	aligned := alignBoundToTimeframe(exchange, boundTS, timeframe)
	if aligned <= 0 {
		aligned = boundTS
	}
	if toTS < aligned {
		return 0, 0, false
	}
	if fromTS < aligned {
		fromTS = aligned
	}
	if toTS < fromTS {
		return 0, 0, false
	}
	return fromTS, toTS, true
}

func (b *Live) isSmallestConfiguredTimeframe(exchange, symbol, timeframe string) bool {
	if b == nil || b.timeframePlan == nil {
		return true
	}
	configured := b.timeframePlan.ResolveConfigured(exchange, symbol)
	if len(configured) == 0 {
		return true
	}
	return strings.EqualFold(strings.TrimSpace(configured[0]), strings.TrimSpace(timeframe))
}

func alignBoundToTimeframe(exchange string, boundTS int64, timeframe string) int64 {
	if boundTS <= 0 {
		return 0
	}
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok || dur <= 0 {
		return boundTS
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return boundTS
	}
	shift := int64(0)
	if strings.EqualFold(strings.TrimSpace(exchange), "okx") {
		shift = 8 * int64(time.Hour/time.Millisecond)
	}
	adjusted := boundTS + shift
	if adjusted <= 0 {
		return boundTS
	}
	return ((adjusted + step - 1) / step * step) - shift
}

func (b *Live) SetEvaluator(evaluator iface.Evaluator) {
	if b == nil {
		return
	}
	b.risk = evaluator
}

func (b *Live) SetExecutor(executor iface.Executor) {
	if b == nil {
		return
	}
	b.executor = executor
}

func (b *Live) listRiskSignalsByPair(exchange, symbol string) ([]models.Signal, error) {
	if b == nil || b.risk == nil {
		return nil, nil
	}
	return b.risk.ListSignalsByPair(exchange, symbol)
}

func flatSignalKey(signal models.Signal) string {
	_, _, comboKey := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
	if strings.TrimSpace(signal.Exchange) == "" || strings.TrimSpace(signal.Symbol) == "" ||
		strings.TrimSpace(signal.Timeframe) == "" || strings.TrimSpace(signal.Strategy) == "" ||
		strings.TrimSpace(comboKey) == "" {
		return ""
	}
	return signal.Exchange + "|" + signal.Symbol + "|" + signal.Strategy + "|" + comboKey
}

func liveSignalsEqual(left, right models.Signal) bool {
	_, _, leftCombo := common.NormalizeStrategyIdentity(left.Timeframe, left.StrategyTimeframes, left.ComboKey)
	_, _, rightCombo := common.NormalizeStrategyIdentity(right.Timeframe, right.StrategyTimeframes, right.ComboKey)
	return left.Exchange == right.Exchange &&
		left.Symbol == right.Symbol &&
		left.Timeframe == right.Timeframe &&
		leftCombo == rightCombo &&
		left.Strategy == right.Strategy &&
		left.StrategyVersion == right.StrategyVersion &&
		left.Action == right.Action &&
		left.HighSide == right.HighSide &&
		left.MidSide == right.MidSide &&
		floatcmp.EQ(left.Entry, right.Entry) &&
		floatcmp.EQ(left.Exit, right.Exit) &&
		floatcmp.EQ(left.SL, right.SL) &&
		floatcmp.EQ(left.TP, right.TP) &&
		left.TriggerTimestamp == right.TriggerTimestamp &&
		left.TrendingTimestamp == right.TrendingTimestamp &&
		triggerHistoryEqual(left.TriggerHistory, right.TriggerHistory)
}

func (b *Live) evaluateAndExecuteLiveSignal(data models.MarketData, snapshot models.MarketSnapshot, signal models.Signal) signalExecutionResult {
	result := executeSignalAction(b.risk, b.executor, models.RiskEvalContext{
		MarketData: data,
		Snapshot:   &snapshot,
	}, data, signal)
	logger := b.logger
	if logger == nil {
		logger = glog.Nop()
	}
	if result.RiskError != "" {
		logger.Warn("risk evaluate failed",
			zap.String("error", result.RiskError),
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
			zap.String("strategy", signal.Strategy),
			zap.Int("signal_action", signal.Action),
		)
		return result
	}
	if result.ExecutionError != "" {
		logger.Error("execution failed",
			zap.String("exchange", signal.Exchange),
			zap.String("symbol", signal.Symbol),
			zap.String("timeframe", signal.Timeframe),
			zap.Float64("entry", signal.Entry),
			zap.Float64("exit", signal.Exit),
			zap.Float64("tp", signal.TP),
			zap.Float64("sl", signal.SL),
			zap.Int("action", signal.Action),
			zap.Int("high_side", signal.HighSide),
			zap.String("error", result.ExecutionError),
		)
		return result
	}
	if signal.Action != 0 && result.Applied {
		if !strings.EqualFold(strings.TrimSpace(data.Source), startupBootstrapSource) {
			b.requestHistoryPairSync(signal.Exchange, signal.Symbol, "signal_applied")
		}
		logger.Info("execution success",
			zap.String("exchange", signal.Exchange),
			zap.String("symbol", signal.Symbol),
			zap.String("timeframe", signal.Timeframe),
			zap.Float64("entry", signal.Entry),
			zap.Float64("exit", signal.Exit),
			zap.Float64("tp", result.Decision.TakeProfitPrice),
			zap.Float64("sl", result.Decision.StopLossPrice),
			zap.Int("action", signal.Action),
			zap.Int("high_side", signal.HighSide),
		)
	}
	return result
}

func (b *Live) historyPairKey(exchange, symbol string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + strings.ToUpper(strings.TrimSpace(symbol))
}

func (b *Live) requestHistoryPairSync(exchange, symbol, reason string) {
	_ = reason
	if b == nil || !b.historyOn || b.historyFetcher == nil || b.ohlcvStore == nil {
		return
	}
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	if exchange == "" || symbol == "" {
		return
	}
	if !b.started.Load() {
		return
	}
	task := historySyncTask{
		Exchange: exchange,
		Symbol:   symbol,
		Reason:   strings.TrimSpace(reason),
	}
	key := b.historyPairKey(exchange, symbol)
	nowMS := time.Now().UnixMilli()
	var (
		ch chan historySyncTask
		ok bool
	)
	b.historyNotifyMu.Lock()
	if b.historyNotify != nil {
		if last, exists := b.historyLastNotify[key]; exists && b.historyDebounce > 0 {
			if nowMS-last < b.historyDebounce.Milliseconds() {
				b.historyNotifyMu.Unlock()
				return
			}
		}
		if _, pending := b.historyPending[key]; !pending {
			b.historyPending[key] = struct{}{}
			b.historyLastNotify[key] = nowMS
			ch = b.historyNotify
			ok = true
		}
	}
	b.historyNotifyMu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- task:
	default:
		b.historyNotifyMu.Lock()
		delete(b.historyPending, key)
		b.historyNotifyMu.Unlock()
	}
}

func (b *Live) finishHistoryPairSync(exchange, symbol string) {
	if b == nil {
		return
	}
	key := b.historyPairKey(exchange, symbol)
	b.historyNotifyMu.Lock()
	delete(b.historyPending, key)
	b.historyNotifyMu.Unlock()
}
