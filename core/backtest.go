package core

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	riskpkg "github.com/misterchenleiya/tradingbot/risk"
	"go.uber.org/zap"
)

const (
	backTestActionOpen     = 8
	backTestActionCloseAll = 64
	replayMinRangeMS       = 1
)

type BackTestConfig struct {
	Strategy       iface.Strategy
	StrategyCombos []models.StrategyComboConfig
	Cache          *OHLCVCache
	OHLCVStore     iface.OHLCVStore
	HistoryArchive iface.HistoryArchiveMirror
	Risk           iface.Evaluator
	Executor       iface.Executor
	Logger         *zap.Logger
}

type BackTestEvent struct {
	EventID            int64
	EventTS            int64
	Call               string
	EventType          string
	Exchange           string
	Symbol             string
	Timeframe          string
	Strategy           string
	StrategyVersion    string
	StrategyTimeframes []string
	ComboKey           string
	ChangedFields      string
	Action             int
	HighSide           int
	MidSide            int
	Entry              float64
	Exit               float64
	SL                 float64
	TP                 float64
	TriggerTS          int64
	TrendingTS         int64
	ExecResult         string
	RiskError          string
	ExecutionError     string
}

type BackTestReport struct {
	TotalEvents          int
	GetEvents            int
	UpdateEvents         int
	OpenSignalEvents     int
	CloseSignalEvents    int
	RiskRejectedEvents   int
	ExecutionErrorEvents int
	Events               []BackTestEvent
}

type BackTest struct {
	core *Live

	risk     iface.Evaluator
	executor iface.Executor

	mu          sync.Mutex
	events      []BackTestEvent
	nextEventID int64
	report      BackTestReport
	reportReady bool
}

func NewBackTest(cfg BackTestConfig) *BackTest {
	core := New(Config{
		Strategy:       cfg.Strategy,
		StrategyCombos: cfg.StrategyCombos,
		Cache:          cfg.Cache,
		OHLCVStore:     cfg.OHLCVStore,
		HistoryArchive: cfg.HistoryArchive,
		Risk:           cfg.Risk,
		Executor:       cfg.Executor,
		SkipWarmup:     true,
		Logger:         cfg.Logger,
	})
	return &BackTest{
		core:     core,
		risk:     cfg.Risk,
		executor: cfg.Executor,
	}
}

func (b *BackTest) Start(ctx context.Context) error {
	if b == nil || b.core == nil {
		return fmt.Errorf("nil core")
	}
	logger := b.logger()
	logger.Info("back-test core start",
		zap.String("strategy", strategyName(b.core.Strategy)),
		zap.Bool("history_enabled", b.core.historyOn),
		zap.Duration("history_interval", b.core.historyInt),
		zap.Bool("skip_warmup", b.core.skipWarmup),
	)
	defer logger.Info("back-test core started")
	return b.core.Start(ctx)
}

func (b *BackTest) Close() error {
	if b == nil || b.core == nil {
		return nil
	}
	logger := b.logger()
	logger.Info("back-test core close")
	defer logger.Info("back-test core closed")
	return b.core.Close()
}

func (b *BackTest) SetLogger(logger *zap.Logger) {
	if b == nil || b.core == nil {
		return
	}
	b.core.SetLogger(logger)
}

func (b *BackTest) OnOHLCV(data models.MarketData) {
	if b == nil || b.core == nil {
		return
	}
	timeframes := b.core.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	events := b.core.expandOHLCVEvents(data, timeframes)
	processed := b.core.cacheOHLCVEvents(events)
	if len(processed) == 0 {
		return
	}
	b.core.applyMarketDataEvents(processed)

	if b.core.Strategy == nil {
		return
	}
	if len(b.core.strategyCombos) > 0 {
		for _, item := range processed {
			snapshot := b.core.strategySnapshot(item)
			b.processStrategyCombos(item, snapshot)
		}
		return
	}
	executionTimeframe := smallestConfiguredTimeframe(timeframes)
	for _, item := range processed {
		if !item.Closed || !strings.EqualFold(strings.TrimSpace(item.Timeframe), executionTimeframe) {
			continue
		}
		snapshot := b.core.strategySnapshot(item)
		logger := b.logger()
		hasPosition, err := hasOpenPositionForPair(b.risk, item.Exchange, item.Symbol)
		if err != nil {
			logger.Warn("skip position-aware strategy flow due to position query error",
				zap.String("exchange", item.Exchange),
				zap.String("symbol", item.Symbol),
				zap.Error(err),
			)
		}
		cachedSignals, err := b.core.listRiskSignalsByPair(item.Exchange, item.Symbol)
		if err != nil {
			logger.Warn("load risk signal cache failed",
				zap.String("exchange", item.Exchange),
				zap.String("symbol", item.Symbol),
				zap.Error(err),
			)
			cachedSignals = nil
		}
		runSignalFlow(b.signalFlowHooks(), item, snapshot, cachedSignals, hasPosition)
	}
}

func (b *BackTest) processStrategyCombos(data models.MarketData, snapshot models.MarketSnapshot) {
	if b == nil || b.core == nil || b.core.Strategy == nil || !data.Closed {
		return
	}
	timeframes := b.core.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	covered := coveredStrategyCombos(b.core.strategyCombos, timeframes)
	combos := triggeredStrategyCombos(b.core.strategyCombos, timeframes, data.Timeframe)
	logger := b.logger()
	cachedSignals, err := b.core.listRiskSignalsByPair(data.Exchange, data.Symbol)
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
		cleared := models.ClearSignalForRemoval(stale)
		execResult := b.executeSignal(data, snapshot, cleared)
		if canRecordSignalEvent(cleared) {
			b.recordSignalEvent("get", "cleared", data, cleared, []string{"signal_disappeared"}, execResult.Result, execResult.RiskError, execResult.ExecutionError)
		}
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

func (b *BackTest) signalFlowHooks() signalFlowHooks {
	return signalFlowHooks{
		strategy: b.core.Strategy,
		logger:   b.logger(),
		applySignal: func(call string, data models.MarketData, snapshot models.MarketSnapshot, previous *models.Signal, signal models.Signal) {
			execResult := b.executeSignal(data, snapshot, signal)
			if !canRecordSignalEvent(signal) {
				return
			}
			eventType := "new"
			changedFields := []string{"new_signal"}
			if previous != nil {
				if call == "get" && models.IsEmptySignal(signal) {
					eventType = "cleared"
					changedFields = []string{"signal_disappeared"}
				} else {
					eventType = signalEventType(*previous, signal)
					changedFields = signalChangedFields(*previous, signal)
				}
			}
			if len(changedFields) == 0 {
				return
			}
			b.recordSignalEvent(call, eventType, data, signal, changedFields, execResult.Result, execResult.RiskError, execResult.ExecutionError)
		},
		onUpdateNoChange: func(data models.MarketData, snapshot models.MarketSnapshot, current models.Signal) {
			b.refreshTrendGuardCandidateOnUpdate(b.logger(), data, snapshot, current)
		},
	}
}

func (b *BackTest) refreshTrendGuardCandidateOnUpdate(logger *zap.Logger, data models.MarketData, snapshot models.MarketSnapshot, signal models.Signal) {
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

func smallestSnapshotTimeframe(snapshot models.MarketSnapshot, fallback string) string {
	timeframes := make([]string, 0, len(snapshot.Series))
	for timeframe := range snapshot.Series {
		timeframes = append(timeframes, timeframe)
	}
	best := smallestConfiguredTimeframe(timeframes)
	if best != "" {
		return best
	}
	return strings.ToLower(strings.TrimSpace(fallback))
}

// PreloadOHLCV injects historical closed candles into cache before replay starts.
// It does not trigger strategy/risk/execution/event recording.
func (b *BackTest) PreloadOHLCV(data models.MarketData) {
	if b == nil || b.core == nil || b.core.Cache == nil {
		return
	}
	if data.Exchange == "" || data.Symbol == "" || data.Timeframe == "" {
		return
	}
	ts := normalizeTimestampMS(data.OHLCV.TS)
	if ts <= 0 {
		return
	}
	ohlcv := data.OHLCV
	ohlcv.TS = ts
	data.OHLCV = ohlcv
	data.Closed = true
	timeframes := b.core.resolvePairTimeframes(data.Exchange, data.Symbol, data.Timeframe)
	events := b.core.expandOHLCVEvents(data, timeframes)
	for _, item := range events {
		b.core.Cache.AppendOrReplace(item.Exchange, item.Symbol, item.Timeframe, item.OHLCV, true)
	}
}

func (b *BackTest) executeSignal(
	data models.MarketData,
	snapshot models.MarketSnapshot,
	signal models.Signal,
) signalExecutionResult {
	result := executeSignalAction(b.risk, b.executor, models.RiskEvalContext{
		MarketData: data,
		Snapshot:   &snapshot,
	}, data, signal)
	if result.ExecutionError != "" {
		b.logExecutionFailed(signal, errors.New(result.ExecutionError))
		return result
	}
	if signal.Action != 0 && result.Applied {
		b.logExecutionSuccess(signal)
	}
	return result
}

func (b *BackTest) logExecutionSuccess(signal models.Signal) {
	logger := b.logger()
	logger.Info("execution success",
		zap.String("exchange", signal.Exchange),
		zap.String("symbol", signal.Symbol),
		zap.String("timeframe", signal.Timeframe),
		zap.Float64("entry", signal.Entry),
		zap.Float64("exit", signal.Exit),
		zap.Float64("tp", signal.TP),
		zap.Float64("sl", signal.SL),
		zap.Int("action", signal.Action),
		zap.Int("high_side", signal.HighSide),
	)
}

func (b *BackTest) logExecutionFailed(signal models.Signal, err error) {
	logger := b.logger()
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
		zap.Error(err),
	)
}

func canRecordSignalEvent(signal models.Signal) bool {
	if flatSignalKey(signal) == "" {
		return false
	}
	return true
}

func signalEventType(previous, next models.Signal) string {
	if previous.HighSide != 0 && next.HighSide == 0 {
		return "cleared"
	}
	return "changed"
}

func signalChangedFields(previous, next models.Signal) []string {
	changed := make([]string, 0, 16)
	if previous.Exchange != next.Exchange {
		changed = append(changed, "exchange")
	}
	if previous.Symbol != next.Symbol {
		changed = append(changed, "symbol")
	}
	if previous.Timeframe != next.Timeframe {
		changed = append(changed, "timeframe")
	}
	if previous.Strategy != next.Strategy {
		changed = append(changed, "strategy")
	}
	if previous.StrategyVersion != next.StrategyVersion {
		changed = append(changed, "strategy_version")
	}
	if previous.Action != next.Action {
		changed = append(changed, "action")
		if previous.Action == 4 || next.Action == 4 {
			changed = append(changed, "armed")
		}
	}
	if previous.OrderType != next.OrderType {
		changed = append(changed, "order_type")
	}
	if previous.HighSide != next.HighSide {
		changed = append(changed, "high_side")
	}
	if previous.MidSide != next.MidSide {
		changed = append(changed, "mid_side")
	}
	if previous.TrendEntryCount != next.TrendEntryCount {
		changed = append(changed, "trend_entry_count")
	}
	if previous.MidPullbackCount != next.MidPullbackCount {
		changed = append(changed, "mid_pullback_count")
	}
	if previous.LastMidPullbackTS != next.LastMidPullbackTS {
		changed = append(changed, "last_mid_pullback_ts")
	}
	if previous.RequireHighPullbackReset != next.RequireHighPullbackReset {
		changed = append(changed, "require_high_pullback_reset")
	}
	if !floatcmp.EQ(previous.Entry, next.Entry) {
		changed = append(changed, "entry")
	}
	if !floatcmp.EQ(previous.Amount, next.Amount) {
		changed = append(changed, "amount")
	}
	if !floatcmp.EQ(previous.Exit, next.Exit) {
		changed = append(changed, "exit")
	}
	if !floatcmp.EQ(previous.SL, next.SL) {
		changed = append(changed, "sl")
	}
	if !floatcmp.EQ(previous.TP, next.TP) {
		changed = append(changed, "tp")
	}
	if previous.TriggerTimestamp != next.TriggerTimestamp {
		changed = append(changed, "trigger_timestamp")
	}
	if previous.TrendingTimestamp != next.TrendingTimestamp {
		changed = append(changed, "trending_timestamp")
	}
	if !triggerHistoryEqual(previous.TriggerHistory, next.TriggerHistory) {
		changed = append(changed, "trigger_history")
	}
	return changed
}

func (b *BackTest) recordSignalEvent(
	call string,
	eventType string,
	data models.MarketData,
	signal models.Signal,
	changedFields []string,
	execResult string,
	riskError string,
	executionError string,
) {
	if b == nil {
		return
	}
	if len(changedFields) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reportReady = false
	b.nextEventID++
	signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey = common.NormalizeStrategyIdentity(
		signal.Timeframe,
		signal.StrategyTimeframes,
		signal.ComboKey,
	)
	event := BackTestEvent{
		EventID:            b.nextEventID,
		EventTS:            normalizeTimestampMS(data.OHLCV.TS),
		Call:               call,
		EventType:          eventType,
		Exchange:           signal.Exchange,
		Symbol:             signal.Symbol,
		Timeframe:          signal.Timeframe,
		Strategy:           signal.Strategy,
		StrategyVersion:    signal.StrategyVersion,
		StrategyTimeframes: append([]string(nil), signal.StrategyTimeframes...),
		ComboKey:           strings.TrimSpace(signal.ComboKey),
		ChangedFields:      strings.Join(changedFields, ","),
		Action:             signal.Action,
		HighSide:           signal.HighSide,
		MidSide:            signal.MidSide,
		Entry:              signal.Entry,
		Exit:               signal.Exit,
		SL:                 signal.SL,
		TP:                 signal.TP,
		TriggerTS:          normalizeTimestampMS(int64(signal.TriggerTimestamp)),
		TrendingTS:         normalizeTimestampMS(int64(signal.TrendingTimestamp)),
		ExecResult:         execResult,
		RiskError:          riskError,
		ExecutionError:     executionError,
	}
	b.events = append(b.events, event)
}

func (b *BackTest) Finalize() BackTestReport {
	if b == nil {
		return BackTestReport{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reportReady {
		return cloneBackTestReport(b.report)
	}
	b.report = buildBackTestReport(b.events)
	b.reportReady = true
	return cloneBackTestReport(b.report)
}

func (b *BackTest) ReplayTrades(trades []riskpkg.BackTestTrade) []riskpkg.BackTestTrade {
	out := append([]riskpkg.BackTestTrade(nil), trades...)
	if len(out) == 0 {
		return out
	}
	if b == nil || b.core == nil || b.core.ohlcvStore == nil {
		return out
	}

	logger := b.logger()
	for idx := range out {
		if strings.TrimSpace(out[idx].Timeframe) == "" {
			continue
		}
		maxDrawdownRate, maxProfitRate, err := b.replayTradeRange(out[idx])
		if err != nil {
			logger.Warn("back-test trade replay failed",
				zap.Int64("trade_id", out[idx].TradeID),
				zap.String("exchange", out[idx].Exchange),
				zap.String("symbol", out[idx].Symbol),
				zap.String("timeframe", out[idx].Timeframe),
				zap.Error(err),
			)
			continue
		}
		out[idx].MaxDrawdownRate = maxDrawdownRate
		out[idx].MaxProfitRate = maxProfitRate
	}
	return out
}

func (b *BackTest) replayTradeRange(trade riskpkg.BackTestTrade) (float64, float64, error) {
	entryTS := normalizeTimestampMS(trade.EntryTS)
	exitTS := normalizeTimestampMS(trade.ExitTS)
	if entryTS <= 0 || exitTS <= 0 {
		return 0, 0, fmt.Errorf("invalid trade entry/exit timestamp")
	}
	if trade.EntryPrice <= 0 {
		return 0, 0, fmt.Errorf("invalid trade entry price")
	}

	startTS := entryTS
	endTS := exitTS
	if endTS < startTS {
		startTS, endTS = endTS, startTS
	}
	if endTS == startTS {
		endTS += replayMinRangeMS
	}

	candles, err := b.core.ohlcvStore.ListOHLCVRange(
		trade.Exchange,
		trade.Symbol,
		trade.Timeframe,
		time.UnixMilli(startTS),
		time.UnixMilli(endTS),
	)
	if err != nil {
		return 0, 0, err
	}

	maxDrawdownRate, maxProfitRate := replayTradeRangeRates(trade, candles)
	return maxDrawdownRate, maxProfitRate, nil
}

func replayTradeRangeRates(trade riskpkg.BackTestTrade, candles []models.OHLCV) (float64, float64) {
	entryPrice := trade.EntryPrice
	leverage := math.Abs(trade.Leverage)
	if entryPrice <= 0 || leverage <= 0 {
		return 0, 0
	}

	maxDrawdownRate := 0.0
	maxProfitRate := 0.0
	for _, candle := range candles {
		high, low, ok := ohlcvHighLow(candle)
		if !ok {
			continue
		}
		maxDrawdownRate, maxProfitRate = updateTradeRates(
			trade.Side,
			entryPrice,
			leverage,
			high,
			low,
			maxDrawdownRate,
			maxProfitRate,
		)
	}
	if trade.ExitPrice > 0 {
		maxDrawdownRate, maxProfitRate = updateTradeRates(
			trade.Side,
			entryPrice,
			leverage,
			trade.ExitPrice,
			trade.ExitPrice,
			maxDrawdownRate,
			maxProfitRate,
		)
	}
	return maxDrawdownRate, maxProfitRate
}

func ohlcvHighLow(candle models.OHLCV) (float64, float64, bool) {
	values := []float64{candle.High, candle.Low, candle.Open, candle.Close}
	var (
		high   float64
		low    float64
		hasVal bool
	)
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if !hasVal {
			high = value
			low = value
			hasVal = true
			continue
		}
		if value > high {
			high = value
		}
		if value < low {
			low = value
		}
	}
	if !hasVal {
		return 0, 0, false
	}
	if high < low {
		high, low = low, high
	}
	return high, low, true
}

func updateTradeRates(
	side string,
	entryPrice float64,
	leverage float64,
	high float64,
	low float64,
	currentDrawdownRate float64,
	currentProfitRate float64,
) (float64, float64) {
	if entryPrice <= 0 || leverage <= 0 || high <= 0 || low <= 0 {
		return currentDrawdownRate, currentProfitRate
	}

	switch strings.ToLower(strings.TrimSpace(side)) {
	case "short":
		favorableRate := (entryPrice - low) / entryPrice * leverage
		adverseRate := (high - entryPrice) / entryPrice * leverage
		if favorableRate > currentProfitRate {
			currentProfitRate = favorableRate
		}
		if adverseRate > currentDrawdownRate {
			currentDrawdownRate = adverseRate
		}
	default:
		favorableRate := (high - entryPrice) / entryPrice * leverage
		adverseRate := (entryPrice - low) / entryPrice * leverage
		if favorableRate > currentProfitRate {
			currentProfitRate = favorableRate
		}
		if adverseRate > currentDrawdownRate {
			currentDrawdownRate = adverseRate
		}
	}

	if currentProfitRate < 0 {
		currentProfitRate = 0
	}
	if currentDrawdownRate < 0 {
		currentDrawdownRate = 0
	}
	return currentDrawdownRate, currentProfitRate
}

func (b *BackTest) Report() (BackTestReport, bool) {
	if b == nil {
		return BackTestReport{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.reportReady {
		return BackTestReport{}, false
	}
	return cloneBackTestReport(b.report), true
}

func buildBackTestReport(events []BackTestEvent) BackTestReport {
	report := BackTestReport{
		TotalEvents: len(events),
		Events:      append([]BackTestEvent(nil), events...),
	}
	for _, event := range events {
		switch event.Call {
		case "get":
			report.GetEvents++
		case "update":
			report.UpdateEvents++
		}
		switch event.Action {
		case backTestActionOpen:
			report.OpenSignalEvents++
		case backTestActionCloseAll:
			report.CloseSignalEvents++
		}
		if strings.TrimSpace(event.RiskError) != "" {
			report.RiskRejectedEvents++
		}
		if strings.TrimSpace(event.ExecutionError) != "" {
			report.ExecutionErrorEvents++
		}
	}
	return report
}

func cloneBackTestReport(report BackTestReport) BackTestReport {
	report.Events = append([]BackTestEvent(nil), report.Events...)
	return report
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func (b *BackTest) logger() *zap.Logger {
	if b == nil || b.core == nil || b.core.logger == nil {
		return glog.Nop()
	}
	return b.core.logger
}
