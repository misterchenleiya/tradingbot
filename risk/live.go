package risk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const (
	liveRiskRequestTimeout = 5 * time.Second
	liveRiskTickInterval   = time.Second
	liveRiskHistorySyncGap = 10 * time.Second
	livePendingOpenTTL     = 5 * time.Second
	liveDefaultTimeframe   = "15m"
	tpModeFixed            = "fixed"
	tpModeDisabled         = "disabled"
)

type openRejectKind int

const (
	openRejectNone openRejectKind = iota
	openRejectRisk
	openRejectTrendGuard
)

type openRejectError struct {
	kind openRejectKind
	err  error
}

func (e *openRejectError) Error() string {
	if e == nil || e.err == nil {
		return ""
	}
	return e.err.Error()
}

func (e *openRejectError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

func newRiskOpenRejectError(format string, args ...any) error {
	return &openRejectError{
		kind: openRejectRisk,
		err:  fmt.Errorf(format, args...),
	}
}

func newTrendGuardOpenRejectError(format string, args ...any) error {
	return &openRejectError{
		kind: openRejectTrendGuard,
		err:  fmt.Errorf(format, args...),
	}
}

func classifyOpenRejectSignalAction(err error) int {
	var rejectErr *openRejectError
	if !errors.As(err, &rejectErr) || rejectErr == nil {
		return 0
	}
	switch rejectErr.kind {
	case openRejectRisk:
		return models.SignalActionOpenRiskRejected
	case openRejectTrendGuard:
		return models.SignalActionOpenTrendGuardRejected
	default:
		return 0
	}
}

type LiveStore interface {
	GetConfigValue(name string) (value string, found bool, err error)
	UpsertConfigValue(name, value, common string) error
	GetRiskAccountState(mode, exchange string) (models.RiskAccountState, bool, error)
	UpsertRiskAccountState(state models.RiskAccountState) error
	ListRiskSymbolCooldownStates(mode, exchange string) ([]models.RiskSymbolCooldownState, error)
	UpsertRiskSymbolCooldownState(state models.RiskSymbolCooldownState) error
	ListRiskTrendGroups(mode string) ([]models.RiskTrendGroup, error)
	ListRiskTrendGroupCandidates(mode string) ([]models.RiskTrendGroupCandidate, error)
	UpsertRiskTrendGroup(group *models.RiskTrendGroup) error
	UpsertRiskTrendGroupCandidate(candidate *models.RiskTrendGroupCandidate) error
	ListRiskOpenPositions(mode, exchange string) ([]models.RiskOpenPosition, error)
	ListRiskHistoryPositions(mode, exchange string) ([]models.Position, error)
	AppendSignalChange(record models.SignalChangeRecord) error
	InsertRiskDecision(record models.RiskDecisionRecord) error
	ListSignalChangesByPair(mode, exchange, symbol string) ([]models.SignalChangeRecord, error)
	SyncRiskHistoryPositions(mode, exchange string, closedPositions []models.RiskClosedPosition) error
	SyncRiskPositions(mode, exchange string, openPositions []models.RiskOpenPosition, closedPositions []models.RiskClosedPosition) error
}

type LiveConfig struct {
	Logger           *zap.Logger
	Store            LiveStore
	HistoryArchive   iface.HistoryArchiveMirror
	Exchanges        map[string]iface.Exchange
	StrategyCombos   []models.StrategyComboConfig
	ActiveStrategies []string
	DefaultExchange  string
	DefaultTimeframe string
	SingletonID      int64
	SingletonUUID    string
}

type RiskConfig struct {
	AllowHedge       bool                     `json:"allow_hedge"`
	AllowScaleIn     bool                     `json:"allow_scale_in"`
	MaxOpenPositions int                      `json:"max_open_positions"`
	TrendGuard       RiskTrendGuardConfig     `json:"trend_guard"`
	TP               RiskTPConfig             `json:"tp"`
	SL               RiskSLConfig             `json:"sl"`
	Leverage         RiskLeverageConfig       `json:"leverage"`
	Account          RiskAccountConfig        `json:"account"`
	PerTrade         RiskPerTradeConfig       `json:"per_trade"`
	SymbolCooldown   RiskSymbolCooldownConfig `json:"symbol_cooldown"`
	TradeCooldown    RiskTradeCooldownConfig  `json:"trade_cooldown"`
}

type RiskTPConfig struct {
	Mode              string  `json:"mode"`
	DefaultPct        float64 `json:"default_pct"`
	OnlyRaiseOnUpdate bool    `json:"only_raise_on_update"`
}

type RiskSLConfig struct {
	MaxLossPct        float64 `json:"max_loss_pct"`
	OnlyRaiseOnUpdate bool    `json:"only_raise_on_update"`
	RequireSignal     bool    `json:"require_signal"`
}

type RiskLeverageConfig struct {
	Min int `json:"min"`
	Max int `json:"max"`
}

type RiskAccountConfig struct {
	Currency     string  `json:"currency"`
	BaselineUSDT float64 `json:"baseline_usdt"`
}

type RiskPerTradeConfig struct {
	Ratio float64 `json:"ratio"`
}

type RiskTrendGuardConfig struct {
	Enabled                bool    `json:"enabled"`
	Mode                   string  `json:"mode"`
	MaxStartLagBars        int     `json:"max_start_lag_bars"`
	LeaderMinPriorityScore float64 `json:"leader_min_priority_score"`
}

type RiskSymbolCooldownConfig struct {
	Enabled             bool   `json:"enabled"`
	ConsecutiveStopLoss int    `json:"consecutive_stop_loss"`
	Cooldown            string `json:"cooldown"`
	Window              string `json:"window"`
}

type RiskTradeCooldownConfig struct {
	Enabled             bool    `json:"enabled"`
	LossRatioOfPerTrade float64 `json:"loss_ratio_of_per_trade"`
	LossLimitUSDT       float64 `json:"loss_limit_usdt,omitempty"`
}

type Live struct {
	logger            *zap.Logger
	store             LiveStore
	historyArchive    iface.HistoryArchiveMirror
	exchanges         map[string]iface.Exchange
	comboTradeEnabled map[string]bool
	activeSet         map[string]struct{}
	defaultExchange   string
	defaultTimeframe  string
	singletonID       int64
	singletonUUID     string
	started           atomic.Bool

	mu            sync.RWMutex
	cfg           RiskConfig
	accountStates map[string]models.RiskAccountState
	availableUSDT map[string]float64
	positions     map[string]models.Position
	openPositions map[string]models.RiskOpenPosition
	cooldowns     map[string]models.RiskSymbolCooldownState
	historySyncAt map[string]int64
	pendingOpens  map[string]int64
	pendingMeta   map[string]models.StrategyContextMeta
	signalCache   *SignalCache
	trendGuard    *groupedTrendGuard

	runCtx    context.Context
	runCancel context.CancelFunc
	runWG     sync.WaitGroup
}

func NewLive(cfg LiveConfig) *Live {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	timeframe := strings.TrimSpace(cfg.DefaultTimeframe)
	if timeframe == "" {
		timeframe = liveDefaultTimeframe
	}
	return &Live{
		logger:            logger,
		store:             cfg.Store,
		historyArchive:    cfg.HistoryArchive,
		exchanges:         cloneExchangeMap(cfg.Exchanges),
		comboTradeEnabled: buildComboTradeEnabledMap(cfg.StrategyCombos),
		activeSet:         buildActiveStrategySet(cfg.ActiveStrategies),
		defaultExchange:   normalizeExchange(cfg.DefaultExchange),
		defaultTimeframe:  timeframe,
		singletonID:       cfg.SingletonID,
		singletonUUID:     strings.TrimSpace(cfg.SingletonUUID),
		cfg:               defaultRiskConfig(),
		accountStates:     make(map[string]models.RiskAccountState),
		availableUSDT:     make(map[string]float64),
		positions:         make(map[string]models.Position),
		openPositions:     make(map[string]models.RiskOpenPosition),
		cooldowns:         make(map[string]models.RiskSymbolCooldownState),
		historySyncAt:     make(map[string]int64),
		pendingOpens:      make(map[string]int64),
		pendingMeta:       make(map[string]models.StrategyContextMeta),
		signalCache:       NewSignalCache(),
		trendGuard:        newGroupedTrendGuard(logger, cfg.Store, liveMode),
	}
}

func (r *Live) Start(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("nil risk live")
	}
	logger := r.logger
	if logger == nil {
		logger = glog.Nop()
	}

	if err := r.loadRiskConfig(); err != nil {
		return err
	}
	if err := r.loadPersistedState(); err != nil {
		return err
	}

	cfg := r.currentConfig()
	logger.Info("risk live start",
		zap.Int("exchange_count", len(r.exchanges)),
		zap.Int("active_strategy_count", len(r.activeSet)),
		zap.Strings("active_strategies", listActiveStrategies(r.activeSet)),
		zap.String("default_exchange", r.defaultExchange),
		zap.String("default_timeframe", r.defaultTimeframe),
		zap.Bool("allow_hedge", cfg.AllowHedge),
		zap.Bool("allow_scale_in", cfg.AllowScaleIn),
		zap.Int("max_open_positions", cfg.MaxOpenPositions),
		zap.Bool("trend_guard_enabled", cfg.TrendGuard.Enabled),
		zap.String("trend_guard_mode", cfg.TrendGuard.Mode),
		zap.Int("trend_guard_max_start_lag_bars", cfg.TrendGuard.MaxStartLagBars),
		zap.Float64("trend_guard_leader_min_priority_score", cfg.TrendGuard.LeaderMinPriorityScore),
		zap.String("tp_mode", cfg.TP.Mode),
		zap.Float64("tp_default_pct", cfg.TP.DefaultPct),
		zap.Float64("sl_max_loss_pct", cfg.SL.MaxLossPct),
		zap.Int("leverage_min", cfg.Leverage.Min),
		zap.Int("leverage_max", cfg.Leverage.Max),
	)
	defer logger.Info("risk live started")
	r.syncHistoryOnStart()
	r.refreshAccountStates(true)
	r.refreshPositions()
	r.restoreSignalsFromStore()
	r.reconcileTrendGuardCandidates()

	if ctx == nil {
		ctx = context.Background()
	}
	r.runCtx, r.runCancel = context.WithCancel(ctx)

	r.runWG.Add(2)
	go r.account()
	go r.position()
	r.started.Store(true)
	return nil
}

func (r *Live) Close() error {
	if r == nil {
		return nil
	}
	logger := r.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("risk live close")
	defer logger.Info("risk live closed")
	if r.runCancel != nil {
		r.runCancel()
	}
	r.runWG.Wait()
	r.started.Store(false)
	return nil
}

func (r *Live) OnMarketData(_ models.MarketData) error {
	return nil
}

func (r *Live) EvaluateOpenBatch(signals []models.Signal, accountState any) (models.Decision, error) {
	if len(signals) == 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	evalCtx := extractRiskEvalContext(accountState)
	data := evalCtx.MarketData
	signal := normalizeRiskSignal(signals[0], data.Exchange, data.Symbol, data.Timeframe)
	position := models.Position{}
	hasPosition := false
	if signal.Exchange != "" && signal.Symbol != "" {
		if cached, ok := r.getCachedPosition(signal.Exchange, signal.Symbol); ok {
			position = cached
			hasPosition = true
		}
	}
	updatedSignal, _, err := r.applySignalLifecycle(signal, data, position, hasPosition)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}
	r.observeTrendGuardSignal(updatedSignal, evalCtx)
	decision, evalErr := r.evaluate(updatedSignal, position, hasPosition, accountState)
	r.auditRiskDecision(updatedSignal, decision, evalErr, normalizeTimestampMS(data.OHLCV.TS))
	if evalErr != nil {
		if rejectAction := classifyOpenRejectSignalAction(evalErr); updatedSignal.Action == 8 && rejectAction != 0 {
			eventTS := normalizeTimestampMS(data.OHLCV.TS)
			rejectedSignal, changed, markErr := r.markOpenRejectedSignal(updatedSignal, rejectAction, eventTS)
			if markErr != nil && r.logger != nil {
				r.logger.Warn("mark rejected open signal failed",
					zap.String("exchange", updatedSignal.Exchange),
					zap.String("symbol", updatedSignal.Symbol),
					zap.Int("reject_action", rejectAction),
					zap.Error(markErr),
				)
			}
			if changed {
				r.observeTrendGuardSignal(rejectedSignal, evalCtx)
			}
		}
		return decision, evalErr
	}
	_ = r.syncSignalCacheFromDecision(updatedSignal, decision, normalizeTimestampMS(data.OHLCV.TS))
	return decision, nil
}

func (r *Live) EvaluateUpdate(signal models.Signal, position models.Position, accountState any) (models.Decision, error) {
	evalCtx := extractRiskEvalContext(accountState)
	data := evalCtx.MarketData
	hasPosition := isPositionOpen(position)
	if !hasPosition {
		exchangeName := normalizeExchange(firstNonEmpty(signal.Exchange, position.Exchange, data.Exchange, r.defaultExchange))
		symbol := strings.TrimSpace(firstNonEmpty(signal.Symbol, position.Symbol, data.Symbol))
		cached, ok := r.getCachedPosition(exchangeName, symbol)
		if ok {
			position = cached
			hasPosition = true
		}
	}
	if hasPosition && len(signal.StrategyTimeframes) == 0 && strings.TrimSpace(signal.ComboKey) == "" {
		signal.StrategyTimeframes = append([]string(nil), position.StrategyTimeframes...)
		signal.ComboKey = strings.TrimSpace(position.ComboKey)
	}
	signal = normalizeRiskSignal(signal, firstNonEmpty(position.Exchange, data.Exchange), firstNonEmpty(position.Symbol, data.Symbol), firstNonEmpty(position.Timeframe, data.Timeframe, r.defaultTimeframe))
	signal.Strategy = strings.TrimSpace(firstNonEmpty(signal.Strategy, position.StrategyName))
	updatedSignal, _, err := r.applySignalLifecycle(signal, data, position, hasPosition)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}
	r.observeTrendGuardSignal(updatedSignal, evalCtx)
	decision, evalErr := r.evaluate(updatedSignal, position, hasPosition, accountState)
	r.auditRiskDecision(updatedSignal, decision, evalErr, normalizeTimestampMS(data.OHLCV.TS))
	if evalErr != nil {
		return decision, evalErr
	}
	_ = r.syncSignalCacheFromDecision(updatedSignal, decision, normalizeTimestampMS(data.OHLCV.TS))
	return decision, nil
}

func shouldAuditRiskDecision(signal models.Signal) bool {
	return signal.Action == 8
}

func riskDecisionResultStatus(decision models.Decision, evalErr error) string {
	if evalErr != nil {
		return "rejected"
	}
	if decision.Action == "" || decision.Action == models.DecisionActionIgnore {
		return "ignored"
	}
	return "allowed"
}

func (r *Live) auditRiskDecision(signal models.Signal, decision models.Decision, evalErr error, eventTS int64) {
	if r == nil || r.store == nil || !shouldAuditRiskDecision(signal) {
		return
	}
	nowMS := time.Now().UnixMilli()
	if eventTS <= 0 {
		eventTS = nowMS
	}
	signalJSON, err := json.Marshal(signal)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("marshal risk decision signal failed", zap.Error(err))
		}
		signalJSON = []byte("{}")
	}
	decisionJSON, err := json.Marshal(decision)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("marshal risk decision decision failed", zap.Error(err))
		}
		decisionJSON = []byte("{}")
	}
	record := models.RiskDecisionRecord{
		SingletonID:         r.singletonID,
		SingletonUUID:       r.singletonUUID,
		Mode:                "live",
		Exchange:            strings.TrimSpace(signal.Exchange),
		Symbol:              strings.TrimSpace(signal.Symbol),
		Timeframe:           strings.TrimSpace(signal.Timeframe),
		Strategy:            strings.TrimSpace(signal.Strategy),
		ComboKey:            strings.TrimSpace(signal.ComboKey),
		GroupID:             strings.TrimSpace(signal.GroupID),
		SignalAction:        signal.Action,
		HighSide:            signal.HighSide,
		DecisionAction:      strings.TrimSpace(decision.Action),
		ResultStatus:        riskDecisionResultStatus(decision, evalErr),
		RejectReason:        strings.TrimSpace(errorString(evalErr)),
		EventAtMS:           eventTS,
		TriggerTimestampMS:  int64(signal.TriggerTimestamp),
		TrendingTimestampMS: int64(signal.TrendingTimestamp),
		SignalJSON:          string(signalJSON),
		DecisionJSON:        string(decisionJSON),
		CreatedAtMS:         nowMS,
	}
	if err := r.store.InsertRiskDecision(record); err != nil && r.logger != nil {
		r.logger.Warn("persist risk decision failed",
			zap.String("exchange", record.Exchange),
			zap.String("symbol", record.Symbol),
			zap.Int("signal_action", record.SignalAction),
			zap.String("decision_action", record.DecisionAction),
			zap.Error(err),
		)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (r *Live) evaluate(signal models.Signal, position models.Position, hasPosition bool, accountState any) (models.Decision, error) {
	if r == nil {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	if signal.Action == 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	data := extractMarketData(accountState)
	exchangeName := normalizeExchange(firstNonEmpty(signal.Exchange, position.Exchange, data.Exchange, r.defaultExchange))
	symbol := strings.TrimSpace(firstNonEmpty(signal.Symbol, position.Symbol, data.Symbol))
	timeframe := strings.TrimSpace(firstNonEmpty(signal.Timeframe, position.Timeframe, data.Timeframe, r.defaultTimeframe))
	if exchangeName == "" || symbol == "" || timeframe == "" {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: missing exchange/symbol/timeframe")
	}
	switch signal.Action {
	case 8:
		if !r.tradeAllowed(exchangeName) {
			return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: trade disabled for exchange %s", exchangeName)
		}
		if !r.comboTradeAllowed(signal) {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: trade disabled for combo %s", signalComboKey(signal.StrategyTimeframes, signal.Timeframe, signal.ComboKey))
		}
		return r.evaluateOpen(exchangeName, symbol, timeframe, signal, position, hasPosition, data)
	case 16:
		return r.evaluateMove(exchangeName, symbol, timeframe, signal, position, hasPosition, data)
	case 32, 64:
		return r.evaluateClose(exchangeName, symbol, timeframe, signal, position, hasPosition, data)
	default:
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
}

func (r *Live) evaluateOpen(
	exchangeName, symbol, timeframe string,
	signal models.Signal,
	position models.Position,
	hasPosition bool,
	data models.MarketData,
) (decision models.Decision, err error) {
	cfg := r.currentConfig()
	now := time.Now()
	nowMS := now.UnixMilli()

	side, ok := signalSide(signal.HighSide)
	if !ok {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: unsupported high_side %d", signal.HighSide)
	}
	signalOwnerCombo := signalComboKey(signal.StrategyTimeframes, timeframe, signal.ComboKey)
	exposureKey := common.ExposureKey(symbol)
	var exposurePosition models.Position
	hasExposurePosition := false
	r.mu.RLock()
	for _, candidate := range r.positions {
		if !isOpenPositionForExposure(candidate, symbol) {
			continue
		}
		exposurePosition = candidate
		hasExposurePosition = true
		break
	}
	r.mu.RUnlock()
	if hasExposurePosition {
		position = exposurePosition
		hasPosition = true
	}

	currentSide := ""
	if hasPosition {
		currentSide = normalizePositionSide(position.PositionSide, 0)
		sameMarket := isSameMarketPosition(position, exchangeName, symbol)
		sameOwner := positionOwnedBy(position, signal.Strategy, timeframe, signal.StrategyTimeframes, signalOwnerCombo)
		switch {
		case !sameMarket:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk live: exposure %s already occupied by %s/%s",
				exposureKey,
				position.Exchange,
				position.Symbol,
			)
		case !sameOwner:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk live: exposure %s already owned by strategy=%s combo=%s",
				exposureKey,
				strings.TrimSpace(position.StrategyName),
				positionComboKey(position),
			)
		case currentSide == side && !cfg.AllowScaleIn:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: scale-in disabled")
		case currentSide != "" && currentSide != side:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: exposure %s already has open position", exposureKey)
		}
	}
	opensNewPosition := !hasPosition || currentSide == "" || currentSide != side
	if hasPosition && !opensNewPosition {
		if err := ensurePositionOwnership(position, signal.Strategy, timeframe, signal.StrategyTimeframes, signalOwnerCombo); err != nil {
			return models.Decision{Action: models.DecisionActionIgnore}, err
		}
	}
	if opensNewPosition {
		if rejectReason, shouldReject := r.matchTrendGuardOpenReason(signal, cfg.TrendGuard); shouldReject {
			return models.Decision{Action: models.DecisionActionIgnore}, newTrendGuardOpenRejectError("risk live: trend guard rejected open: %s", rejectReason)
		}
	}
	if cfg.MaxOpenPositions > 0 && opensNewPosition {
		openCount, reserved := r.tryReservePendingOpen(exchangeName, symbol, side, nowMS, cfg.MaxOpenPositions)
		if !reserved {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk live: max open positions reached (%d/%d)",
				openCount,
				cfg.MaxOpenPositions,
			)
		}
		defer func() {
			if err != nil {
				r.clearPendingOpen(exchangeName, symbol, side)
			}
		}()
		r.setPendingOpenMeta(exchangeName, symbol, side, r.buildOpenSignalMeta(signal, timeframe))
	}

	accountState, err := r.ensureAccountState(exchangeName)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}
	if cfg.TradeCooldown.Enabled && accountState.DailyLossLimitUSDT > 0 && accountState.DailyRealizedUSDT >= accountState.DailyLossLimitUSDT {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: trade cooldown active, daily loss %.4f/%.4f usdt", accountState.DailyRealizedUSDT, accountState.DailyLossLimitUSDT)
	}
	if cooldown := r.currentCooldown(exchangeName, symbol); cooldown.CooldownUntilMS > nowMS {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: symbol cooldown until %s", time.UnixMilli(cooldown.CooldownUntilMS).Format("2006-01-02 15:04:05"))
	}

	signalOrderType, err := normalizeSignalOrderType(signal.OrderType)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: %w", err)
	}
	entryPrice := resolveOpenPrice(signal, data)
	if signalOrderType == models.OrderTypeMarket {
		entryPrice = marketPrice(data, 0)
	}
	if signalOrderType == models.OrderTypeLimit {
		entryPrice = signal.Entry
		if entryPrice <= 0 {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: limit order requires entry price")
		}
	}
	client, err := r.exchangeFor(exchangeName)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}
	instID, err := normalizeTradeInstID(client, symbol)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}
	if entryPrice <= 0 {
		if signalOrderType == models.OrderTypeLimit {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: invalid entry price")
		}
		ctx, cancel := context.WithTimeout(context.Background(), liveRiskRequestTimeout)
		entryPrice, err = client.GetTickerPrice(ctx, instID)
		cancel()
		if err != nil {
			return models.Decision{Action: models.DecisionActionIgnore}, err
		}
	}
	if entryPrice <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: invalid entry price")
	}
	available := r.currentAvailableUSDT(exchangeName)
	ctx, cancel := context.WithTimeout(context.Background(), liveRiskRequestTimeout)
	inst, err := client.GetInstrument(ctx, instID)
	cancel()
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: load instrument %s failed: %w", instID, err)
	}
	plan, err := evaluateRiskOpenPlan(cfg, signal, signalOrderType, side, entryPrice, available, accountState, func(perTradeUSDT float64, leverage int, entryPrice float64) (float64, error) {
		return calculateRiskOpenSize(perTradeUSDT, leverage, entryPrice, inst)
	})
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk live: %w", err)
	}

	action := models.DecisionActionOpenLong
	if side == positionSideShort {
		action = models.DecisionActionOpenShort
	}
	decision = models.Decision{
		Exchange:           exchangeName,
		Symbol:             symbol,
		Timeframe:          timeframe,
		Action:             action,
		Strategy:           signal.Strategy,
		OrderType:          plan.DecisionOrderType,
		PositionSide:       side,
		MarginMode:         models.MarginModeIsolated,
		Size:               plan.Size,
		LeverageMultiplier: float64(plan.Leverage),
		Price:              plan.EntryPrice,
		StopLossPrice:      plan.StopLossPrice,
		TakeProfitPrice:    plan.TakeProfitPrice,
		ClientOrderID:      newRiskClientOrderID(exchangeName, symbol),
	}
	return decision, nil
}

func (r *Live) evaluateMove(
	exchangeName, symbol, timeframe string,
	signal models.Signal,
	position models.Position,
	hasPosition bool,
	data models.MarketData,
) (models.Decision, error) {
	if !hasPosition {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: no open position for update")
	}
	if !isManualTradeSignal(signal) && !positionOwnedBy(position, signal.Strategy, timeframe, signal.StrategyTimeframes, signal.ComboKey) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	cfg := r.currentConfig()
	side := normalizePositionSide(position.PositionSide, signal.HighSide)
	if side == "" {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: missing position side")
	}
	price := resolveClosePrice(signal, data, firstPositive(position.CurrentPrice, position.EntryPrice, signal.Entry))
	if price <= 0 {
		price = firstPositive(position.CurrentPrice, position.EntryPrice, signal.Entry)
	}
	if price <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: invalid update price")
	}

	tp := resolveUpdateTakeProfit(cfg.TP, signal.TP, position.TakeProfitPrice)
	if tp > 0 && position.TakeProfitPrice > 0 && !isTPMoveAllowed(side, position.TakeProfitPrice, tp) {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: TP can only move with trend")
	}

	sl := signal.SL
	if sl <= 0 {
		if cfg.SL.RequireSignal {
			return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: SL required")
		}
		sl = position.StopLossPrice
	}
	if sl <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: invalid SL")
	}
	if cfg.SL.OnlyRaiseOnUpdate && position.StopLossPrice > 0 && !isSLMoveAllowed(side, position.StopLossPrice, sl) {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: SL can only move with trend")
	}
	if err := validateTPSLAgainstPrice(side, price, tp, sl); err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}

	qty := estimatePositionQuantity(position, signal, data)
	if qty <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: invalid position quantity")
	}

	return models.Decision{
		Exchange:           exchangeName,
		Symbol:             symbol,
		Timeframe:          timeframe,
		Action:             models.DecisionActionUpdate,
		Strategy:           signal.Strategy,
		PositionSide:       side,
		MarginMode:         normalizeMarginMode(position.MarginMode, models.MarginModeIsolated),
		Size:               qty,
		LeverageMultiplier: chooseLeverage(position.LeverageMultiplier, float64(cfg.Leverage.Min)),
		Price:              price,
		StopLossPrice:      sl,
		TakeProfitPrice:    tp,
		ClientOrderID:      newRiskClientOrderID(exchangeName, symbol),
	}, nil
}

func (r *Live) evaluateClose(
	exchangeName, symbol, timeframe string,
	signal models.Signal,
	position models.Position,
	hasPosition bool,
	data models.MarketData,
) (models.Decision, error) {
	if !hasPosition {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: no open position for close")
	}
	if !isManualTradeSignal(signal) && !positionOwnedBy(position, signal.Strategy, timeframe, signal.StrategyTimeframes, signal.ComboKey) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	side := normalizePositionSide(position.PositionSide, signal.HighSide)
	if side == "" {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: missing position side")
	}
	qty := estimatePositionQuantity(position, signal, data)
	if signal.Action == 32 {
		qty = qty * 0.8
	}
	if qty <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk live: invalid close quantity")
	}
	price := resolveClosePrice(signal, data, firstPositive(position.CurrentPrice, position.EntryPrice, signal.Entry))
	if price <= 0 {
		price = firstPositive(position.CurrentPrice, position.EntryPrice, signal.Entry)
	}
	closeReason := ""
	stopLossPrice := 0.0
	takeProfitPrice := 0.0
	if signal.Action == 32 {
		closeReason = "signal_partial_close"
		stopLossPrice = signal.SL
		takeProfitPrice = signal.TP
	}
	return models.Decision{
		Exchange:           exchangeName,
		Symbol:             symbol,
		Timeframe:          timeframe,
		Action:             models.DecisionActionClose,
		CloseReason:        closeReason,
		Strategy:           signal.Strategy,
		PositionSide:       side,
		MarginMode:         normalizeMarginMode(position.MarginMode, models.MarginModeIsolated),
		Size:               qty,
		LeverageMultiplier: position.LeverageMultiplier,
		Price:              price,
		StopLossPrice:      stopLossPrice,
		TakeProfitPrice:    takeProfitPrice,
		ClientOrderID:      newRiskClientOrderID(exchangeName, symbol),
	}, nil
}

func (r *Live) HasOpenPosition(exchange, symbol string) (bool, error) {
	if r == nil {
		return false, nil
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	if exchange == "" && r.defaultExchange != "" {
		exchange = r.defaultExchange
	}
	if symbol == "" {
		return false, nil
	}
	targetSymbol := canonicalSymbol(symbol)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pos := range r.positions {
		if exchange != "" && normalizeExchange(pos.Exchange) != exchange {
			continue
		}
		if canonicalSymbol(pos.Symbol) != targetSymbol {
			continue
		}
		if isPositionOpen(pos) {
			return true, nil
		}
	}
	return false, nil
}

func (r *Live) ListOpenPositions(exchange, symbol, timeframe string) ([]models.Position, error) {
	return r.listOpenPositions(exchange, symbol, timeframe, true), nil
}

func (r *Live) ListAllOpenPositions() ([]models.Position, error) {
	return r.listOpenPositions("", "", "", false), nil
}

func (r *Live) listOpenPositions(exchange, symbol, timeframe string, useDefaultExchange bool) []models.Position {
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)
	if useDefaultExchange && exchange == "" && r.defaultExchange != "" {
		exchange = r.defaultExchange
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]models.Position, 0)
	for _, pos := range r.positions {
		if exchange != "" && normalizeExchange(pos.Exchange) != exchange {
			continue
		}
		if symbol != "" && canonicalSymbol(pos.Symbol) != canonicalSymbol(symbol) {
			continue
		}
		if timeframe != "" && strings.TrimSpace(pos.Timeframe) != "" && strings.TrimSpace(pos.Timeframe) != timeframe {
			continue
		}
		out = append(out, pos)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		if out[i].Timeframe != out[j].Timeframe {
			return out[i].Timeframe < out[j].Timeframe
		}
		return out[i].PositionSide < out[j].PositionSide
	})
	return out
}

func (r *Live) ListHistoryPositions(exchange, symbol string) ([]models.Position, error) {
	if r == nil {
		return nil, fmt.Errorf("risk live: nil evaluator")
	}
	if r.store == nil {
		return nil, fmt.Errorf("risk live: history store unavailable")
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	canonical := canonicalSymbol(symbol)

	targetExchanges := make([]string, 0)
	if exchange != "" {
		targetExchanges = append(targetExchanges, exchange)
	} else {
		targetExchanges = make([]string, 0, len(r.exchanges))
		for name := range r.exchanges {
			key := normalizeExchange(name)
			if key == "" {
				continue
			}
			targetExchanges = append(targetExchanges, key)
		}
		sort.Strings(targetExchanges)
	}
	if len(targetExchanges) == 0 {
		return nil, nil
	}

	out := make([]models.Position, 0)
	for _, exchangeName := range targetExchanges {
		rows, err := r.store.ListRiskHistoryPositions(liveMode, exchangeName)
		if err != nil {
			return nil, err
		}
		for _, item := range rows {
			if canonical != "" && canonicalSymbol(item.Symbol) != canonical {
				continue
			}
			out = append(out, item)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		if out[i].ExitTime != out[j].ExitTime {
			return out[i].ExitTime > out[j].ExitTime
		}
		if out[i].EntryTime != out[j].EntryTime {
			return out[i].EntryTime > out[j].EntryTime
		}
		return out[i].PositionSide < out[j].PositionSide
	})
	return out, nil
}

func (r *Live) GetAccountFunds(exchange string) (models.RiskAccountFunds, error) {
	if r == nil {
		return models.RiskAccountFunds{}, fmt.Errorf("risk live: nil evaluator")
	}
	exchange = normalizeExchange(exchange)
	if exchange == "" {
		exchange = r.defaultExchange
	}
	if exchange == "" {
		return models.RiskAccountFunds{}, fmt.Errorf("risk live: exchange is required")
	}

	state := r.currentAccountState(exchange)
	if state.UpdatedAtMS <= 0 {
		r.refreshAccountStates(false)
		state = r.currentAccountState(exchange)
	}
	if state.UpdatedAtMS <= 0 {
		return models.RiskAccountFunds{}, fmt.Errorf("risk live: account funds unavailable for %s", exchange)
	}
	unrealized := r.totalUnrealizedProfit(exchange)
	return models.RiskAccountFunds{
		Exchange:        exchange,
		Currency:        "USDT",
		FundingUSDT:     state.FundingUSDT,
		TradingUSDT:     state.TradingUSDT,
		TotalUSDT:       state.TotalUSDT,
		PerTradeUSDT:    state.PerTradeUSDT,
		DailyProfitUSDT: state.DailyClosedProfitUSDT + unrealized,
		UpdatedAtMS:     state.UpdatedAtMS,
	}, nil
}

func (r *Live) account() {
	defer r.runWG.Done()
	ticker := time.NewTicker(liveRiskTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.runCtx.Done():
			return
		case <-ticker.C:
			r.refreshAccountStates(false)
		}
	}
}

func (r *Live) refreshAccountStates(forceRecalculate bool) {
	if r == nil {
		return
	}
	now := time.Now()
	cfg := r.currentConfig()
	for exchangeName, client := range r.exchanges {
		if !r.tradeAllowed(exchangeName) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), liveRiskRequestTimeout)
		balance, err := client.GetBalance(ctx)
		positions, posErr := client.GetPositions(ctx, "")
		cancel()
		if err != nil {
			r.logger.Error("risk account update failed", zap.String("exchange", exchangeName), zap.Error(err))
			continue
		}
		if posErr != nil {
			r.logger.Error("risk account load positions failed", zap.String("exchange", exchangeName), zap.Error(posErr))
			continue
		}

		funding := pickUSDTValue(balance.Funding, false)
		available := pickUSDTValue(balance.Trading, true)
		margin := sumPositionMargin(positions)
		trading := available + margin
		if trading <= 0 {
			trading = pickUSDTValue(balance.Trading, false)
		}
		total := funding + trading
		if total <= 0 {
			continue
		}

		state := r.currentAccountState(exchangeName)
		tradeDate := now.Format("2006-01-02")
		if state.TradeDate != tradeDate {
			baseline := trading
			if baseline <= 0 {
				baseline = state.TradingUSDT
			}
			state.TradeDate = tradeDate
			state.DailyRealizedUSDT = 0
			state.DailyClosedProfitUSDT = 0
			state.PerTradeUSDT = baseline * cfg.PerTrade.Ratio
			state.DailyLossLimitUSDT = state.PerTradeUSDT * cfg.TradeCooldown.LossRatioOfPerTrade
			state.TotalUSDT = total
			state.FundingUSDT = funding
			state.TradingUSDT = trading
			state.UpdatedAtMS = now.UnixMilli()
			r.saveRiskConfigDerivedFields(baseline, state.DailyLossLimitUSDT)
		} else {
			state.TotalUSDT = total
			state.FundingUSDT = funding
			state.TradingUSDT = trading
			state.UpdatedAtMS = now.UnixMilli()
			if forceRecalculate {
				state.PerTradeUSDT = trading * cfg.PerTrade.Ratio
				state.DailyLossLimitUSDT = state.PerTradeUSDT * cfg.TradeCooldown.LossRatioOfPerTrade
			} else if state.PerTradeUSDT <= 0 {
				state.PerTradeUSDT = trading * cfg.PerTrade.Ratio
			}
			if !forceRecalculate && state.DailyLossLimitUSDT <= 0 {
				state.DailyLossLimitUSDT = state.PerTradeUSDT * cfg.TradeCooldown.LossRatioOfPerTrade
			}
		}
		state.Mode = liveMode

		r.mu.Lock()
		r.availableUSDT[exchangeName] = available
		r.accountStates[exchangeName] = state
		r.mu.Unlock()
		if r.store != nil {
			if err := r.store.UpsertRiskAccountState(state); err != nil {
				r.logger.Error("risk account persist failed", zap.String("exchange", exchangeName), zap.Error(err))
			}
		}
	}
}

func (r *Live) position() {
	defer r.runWG.Done()
	ticker := time.NewTicker(liveRiskTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.runCtx.Done():
			return
		case <-ticker.C:
			r.refreshPositions()
			r.syncHistory(false)
		}
	}
}

func (r *Live) refreshPositions() {
	if r == nil {
		return
	}
	for exchangeName, client := range r.exchanges {
		if !r.tradeAllowed(exchangeName) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), liveRiskRequestTimeout)
		rows, err := client.GetPositions(ctx, "")
		tpsl := []iface.TPSLOrder(nil)
		tpslErr := error(nil)
		if manager, ok := client.(iface.TPSLManager); ok {
			tpsl, tpslErr = manager.GetOpenTPSLOrders(ctx, "")
		}
		cancel()
		if err != nil {
			r.logger.Error("risk position update failed", zap.String("exchange", exchangeName), zap.Error(err))
			continue
		}
		if tpslErr != nil {
			r.logger.Error("risk position tpsl update failed", zap.String("exchange", exchangeName), zap.Error(tpslErr))
		}
		openSnapshots := r.syncOpenPositions(exchangeName, rows, tpsl)
		if r.store != nil {
			if syncErr := r.store.SyncRiskPositions(liveMode, exchangeName, openSnapshots, nil); syncErr != nil {
				r.logger.Error("risk position persist failed", zap.String("exchange", exchangeName), zap.Error(syncErr))
			} else if r.historyArchive != nil {
				if archiveErr := r.historyArchive.SyncOpenPositions(exchangeName, openSnapshots); archiveErr != nil {
					r.logger.Error("history archive sync open positions failed",
						zap.String("exchange", exchangeName),
						zap.Error(archiveErr),
					)
				}
			}
		}
	}
}

func (r *Live) syncHistoryOnStart() {
	r.syncHistory(true)
}

func (r *Live) syncHistory(force bool) {
	if r == nil {
		return
	}
	nowMS := time.Now().UnixMilli()
	for exchangeName, client := range r.exchanges {
		if !r.tradeAllowed(exchangeName) {
			continue
		}
		if !r.allowHistorySync(exchangeName, nowMS, force) {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), liveRiskRequestTimeout)
		rows, err := client.GetPositionsHistory(ctx, "")
		cancel()
		if err != nil {
			r.logger.Error("risk position history sync failed", zap.String("exchange", exchangeName), zap.Error(err))
			continue
		}
		closedSnapshots := buildRiskClosedPositions(exchangeName, rows, models.PositionRuntimeMeta{
			RunID:       r.singletonUUID,
			SingletonID: r.singletonID,
		})
		if r.store != nil {
			if syncErr := r.store.SyncRiskHistoryPositions(liveMode, exchangeName, closedSnapshots); syncErr != nil {
				r.logger.Error("risk position history sync persist failed", zap.String("exchange", exchangeName), zap.Error(syncErr))
			}
		}
		r.applyPositionHistory(exchangeName, rows)
	}
}

func (r *Live) allowHistorySync(exchange string, nowMS int64, force bool) bool {
	if r == nil {
		return false
	}
	key := normalizeExchange(exchange)
	if key == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.historySyncAt == nil {
		r.historySyncAt = make(map[string]int64)
	}
	if force {
		r.historySyncAt[key] = nowMS
		return true
	}
	last := r.historySyncAt[key]
	if last > 0 && nowMS-last < liveRiskHistorySyncGap.Milliseconds() {
		return false
	}
	r.historySyncAt[key] = nowMS
	return true
}

func (r *Live) syncOpenPositions(exchangeName string, rows []iface.Position, tpslOrders []iface.TPSLOrder) []models.RiskOpenPosition {
	currView := make(map[string]models.Position)
	currOpen := make(map[string]models.RiskOpenPosition)
	prevOpen := make(map[string]models.RiskOpenPosition)
	disappeared := make([]models.RiskOpenPosition, 0)
	pendingMeta := make(map[string]models.StrategyContextMeta)
	prefix := normalizeExchange(exchangeName) + "|"
	r.mu.RLock()
	for key, item := range r.openPositions {
		if strings.HasPrefix(key, prefix) {
			prevOpen[key] = item
		}
	}
	for key, item := range r.pendingMeta {
		pendingMeta[key] = item
	}
	r.mu.RUnlock()
	tpslIndex := buildTPSLIndex(tpslOrders)
	nowMS := time.Now().UnixMilli()
	for _, row := range rows {
		qtySigned := parseFloatOrZero(row.Pos)
		qty := math.Abs(qtySigned)
		if qty <= 0 {
			continue
		}
		side := normalizePositionSide(row.PosSide, highSideFromQuantity(qtySigned))
		if side == "" {
			continue
		}
		symbol := symbolFromInstID(row.InstID)
		instID := strings.ToUpper(strings.TrimSpace(row.InstID))
		if instID == "" {
			continue
		}
		margin := parseFloatOrZero(row.Margin)
		leverage := parseFloatOrZero(row.Lever)
		entryPrice := parseFloatOrZero(row.AvgPx)
		currentPrice := parseFloatOrZero(row.MarkPx)
		entryValue := parseFloatOrZero(row.NotionalUsd)
		if entryValue <= 0 && margin > 0 && leverage > 0 {
			entryValue = margin * leverage
		}
		unrealizedAmount := parseFloatOrZero(row.Upl)
		unrealizedRate := parseFloatOrZero(row.UplRatio)
		entryTime := formatExchangeTimestampMS(row.OpenTime)
		updatedTime := formatExchangeTimestampMS(row.UpdateTime)
		if updatedTime == "" {
			updatedTime = entryTime
		}
		tp := parseFloatOrZero(row.TPTriggerPx)
		sl := parseFloatOrZero(row.SLTriggerPx)
		if pairTPSL, ok := tpslIndex[tpslIndexKey(symbol, side)]; ok {
			if tp <= 0 {
				tp = pairTPSL.tp
			}
			if sl <= 0 {
				sl = pairTPSL.sl
			}
		}
		positionKey := livePositionKey(exchangeName, instID, side, row.MgnMode)
		if positionKey == "" {
			continue
		}
		prev := prevOpen[positionKey]
		pendingKey := livePendingOpenKey(exchangeName, symbol, side)
		meta := models.MergeStrategyContextMeta(
			pendingMeta[pendingKey],
			models.MergeStrategyContextMeta(
				models.ExtractStrategyContextMeta(prev.RowJSON),
				r.inferStrategyContextMeta(exchangeName, symbol, side),
			),
		)
		runtimeMeta := preserveInitialPositionRuntimeMeta(prev.RowJSON, models.PositionRuntimeMeta{
			RunID:       r.singletonUUID,
			SingletonID: r.singletonID,
		})
		rowJSON := models.MarshalPositionRowEnvelopeWithRuntime(row, meta, runtimeMeta)
		if strings.TrimSpace(rowJSON) == "" {
			rowJSON = strings.TrimSpace(prev.RowJSON)
		}
		maxLoss := prev.MaxFloatingLossAmount
		maxProfit := prev.MaxFloatingProfitAmount
		if upl := parseFloatOrZero(row.Upl); upl < 0 {
			if value := -upl; value > maxLoss {
				maxLoss = value
			}
		} else if upl > 0 && upl > maxProfit {
			maxProfit = upl
		}
		openItem := models.RiskOpenPosition{
			SingletonID:             runtimeMeta.SingletonID,
			Exchange:                normalizeExchange(exchangeName),
			Symbol:                  symbol,
			InstID:                  instID,
			Pos:                     row.Pos,
			PosSide:                 side,
			MgnMode:                 normalizeMarginMode(row.MgnMode, models.MarginModeIsolated),
			Margin:                  row.Margin,
			Lever:                   row.Lever,
			AvgPx:                   row.AvgPx,
			Upl:                     row.Upl,
			UplRatio:                row.UplRatio,
			NotionalUSD:             row.NotionalUsd,
			MarkPx:                  row.MarkPx,
			LiqPx:                   row.LiqPx,
			TPTriggerPx:             formatNumericText(tp),
			SLTriggerPx:             formatNumericText(sl),
			OpenTimeMS:              normalizeTimestampMS(parseInt64OrZero(row.OpenTime)),
			UpdateTimeMS:            normalizeTimestampMS(parseInt64OrZero(row.UpdateTime)),
			RowJSON:                 rowJSON,
			MaxFloatingLossAmount:   maxLoss,
			MaxFloatingProfitAmount: maxProfit,
			UpdatedAtMS:             nowMS,
		}
		timeframe := strategyPrimaryTimeframe(meta)
		comboKey := strategyComboKey(meta)
		currOpen[positionKey] = openItem
		currView[positionKey] = models.Position{
			SingletonID:             runtimeMeta.SingletonID,
			Exchange:                exchangeName,
			Symbol:                  symbol,
			Timeframe:               timeframe,
			PositionSide:            side,
			GroupID:                 strings.TrimSpace(meta.GroupID),
			MarginMode:              normalizeMarginMode(row.MgnMode, models.MarginModeIsolated),
			LeverageMultiplier:      leverage,
			MarginAmount:            margin,
			EntryPrice:              entryPrice,
			EntryQuantity:           qty,
			EntryValue:              entryValue,
			EntryTime:               entryTime,
			CurrentPrice:            currentPrice,
			TakeProfitPrice:         tp,
			StopLossPrice:           sl,
			UnrealizedProfitAmount:  unrealizedAmount,
			UnrealizedProfitRate:    unrealizedRate,
			MaxFloatingProfitAmount: maxProfit,
			MaxFloatingLossAmount:   maxLoss,
			Status:                  models.PositionStatusOpen,
			StrategyName:            strings.TrimSpace(meta.StrategyName),
			StrategyVersion:         strings.TrimSpace(meta.StrategyVersion),
			StrategyTimeframes:      append([]string(nil), meta.StrategyTimeframes...),
			ComboKey:                comboKey,
			UpdatedTime:             updatedTime,
		}
	}

	r.mu.Lock()
	for key := range r.positions {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, ok := currView[key]; !ok {
			delete(r.positions, key)
		}
	}
	for key, pos := range currView {
		r.positions[key] = pos
	}
	for key := range r.openPositions {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if _, ok := currOpen[key]; !ok {
			disappeared = append(disappeared, prevOpen[key])
			delete(r.openPositions, key)
		}
	}
	for key, item := range currOpen {
		r.openPositions[key] = item
		pendingKey := livePendingOpenKey(item.Exchange, item.Symbol, item.PosSide)
		delete(r.pendingOpens, pendingKey)
		delete(r.pendingMeta, pendingKey)
	}
	r.prunePendingOpensLocked(nowMS)
	r.mu.Unlock()

	for _, pos := range currView {
		_ = r.syncSignalCacheFromPosition(pos, nowMS)
	}
	for _, item := range disappeared {
		_ = r.syncSignalCacheClosedByPositionDisappearance(item, currView, nowMS)
	}
	if r.trendGuard != nil {
		r.trendGuard.syncPositions(r.snapshotPositions(), nowMS)
	}

	out := make([]models.RiskOpenPosition, 0, len(currOpen))
	for _, item := range currOpen {
		out = append(out, item)
	}
	return out
}

func preserveInitialPositionRuntimeMeta(prevRowJSON string, current models.PositionRuntimeMeta) models.PositionRuntimeMeta {
	current = models.NormalizePositionRuntimeMeta(current)
	prev := models.ExtractPositionRuntimeMeta(prevRowJSON)
	if prev.SingletonID > 0 {
		current.SingletonID = prev.SingletonID
	}
	if strings.TrimSpace(prev.RunID) != "" {
		current.RunID = strings.TrimSpace(prev.RunID)
	}
	return models.NormalizePositionRuntimeMeta(current)
}

func buildRiskClosedPositions(exchangeName string, history []iface.PositionHistory, runtimeMeta models.PositionRuntimeMeta) []models.RiskClosedPosition {
	nowMS := time.Now().UnixMilli()
	out := make([]models.RiskClosedPosition, 0, len(history))
	for _, item := range history {
		instID := strings.ToUpper(strings.TrimSpace(item.InstID))
		if instID == "" {
			continue
		}
		closeMS := normalizeTimestampMS(parseInt64OrZero(item.CloseTime))
		if closeMS <= 0 {
			continue
		}
		side := resolveHistoryPositionSide(item)
		out = append(out, models.RiskClosedPosition{
			Exchange:     normalizeExchange(exchangeName),
			Symbol:       symbolFromInstID(instID),
			InstID:       instID,
			PosSide:      side,
			MgnMode:      normalizeMarginMode(item.MgnMode, models.MarginModeIsolated),
			Lever:        item.Lever,
			OpenAvgPx:    item.OpenAvgPx,
			CloseAvgPx:   item.CloseAvgPx,
			RealizedPnl:  item.RealizedPnl,
			PnlRatio:     item.PnlRatio,
			Fee:          item.Fee,
			FundingFee:   item.FundingFee,
			OpenTimeMS:   normalizeTimestampMS(parseInt64OrZero(item.OpenTime)),
			CloseTimeMS:  closeMS,
			State:        item.State,
			CloseRowJSON: models.MarshalPositionRowEnvelopeWithRuntime(item, models.StrategyContextMeta{}, runtimeMeta),
			UpdatedAtMS:  nowMS,
		})
	}
	return out
}

func riskOpenPositionToView(item models.RiskOpenPosition) models.Position {
	symbol := strings.TrimSpace(item.Symbol)
	if symbol == "" {
		symbol = symbolFromInstID(item.InstID)
	}
	meta := models.ExtractStrategyContextMeta(item.RowJSON)
	runtimeMeta := models.ExtractPositionRuntimeMeta(item.RowJSON)
	singletonID := item.SingletonID
	if singletonID <= 0 {
		singletonID = runtimeMeta.SingletonID
	}
	comboKey := strategyComboKey(meta)
	return models.Position{
		SingletonID:             singletonID,
		Exchange:                item.Exchange,
		Symbol:                  symbol,
		Timeframe:               strategyPrimaryTimeframe(meta),
		PositionSide:            normalizePositionSide(item.PosSide, 0),
		GroupID:                 strings.TrimSpace(meta.GroupID),
		MarginMode:              normalizeMarginMode(item.MgnMode, models.MarginModeIsolated),
		LeverageMultiplier:      parseFloatOrZero(item.Lever),
		MarginAmount:            parseFloatOrZero(item.Margin),
		EntryPrice:              parseFloatOrZero(item.AvgPx),
		EntryQuantity:           math.Abs(parseFloatOrZero(item.Pos)),
		EntryValue:              parseFloatOrZero(item.NotionalUSD),
		EntryTime:               formatTimestampMS(item.OpenTimeMS),
		CurrentPrice:            parseFloatOrZero(item.MarkPx),
		TakeProfitPrice:         parseFloatOrZero(item.TPTriggerPx),
		StopLossPrice:           parseFloatOrZero(item.SLTriggerPx),
		UnrealizedProfitAmount:  parseFloatOrZero(item.Upl),
		UnrealizedProfitRate:    parseFloatOrZero(item.UplRatio),
		MaxFloatingProfitAmount: item.MaxFloatingProfitAmount,
		MaxFloatingLossAmount:   item.MaxFloatingLossAmount,
		Status:                  models.PositionStatusOpen,
		StrategyName:            strings.TrimSpace(meta.StrategyName),
		StrategyVersion:         strings.TrimSpace(meta.StrategyVersion),
		StrategyTimeframes:      append([]string(nil), meta.StrategyTimeframes...),
		ComboKey:                comboKey,
		UpdatedTime:             formatTimestampMS(item.UpdateTimeMS),
	}
}

func formatTimestampMS(ts int64) string {
	ts = normalizeTimestampMS(ts)
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func formatNumericText(value float64) string {
	if value == 0 {
		return "0"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func marshalJSONOrEmpty(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(raw)
}

func (r *Live) inferStrategyContextMeta(exchange, symbol, side string) models.StrategyContextMeta {
	if r == nil || r.signalCache == nil {
		return models.StrategyContextMeta{}
	}
	signals := r.signalCache.ListByPair(exchange, symbol)
	if len(signals) == 0 {
		return models.StrategyContextMeta{}
	}
	best := models.StrategyContextMeta{}
	bestScore := -1
	bestTS := -1
	for _, signal := range signals {
		meta := models.BuildStrategyContextMetaFromSignal(signal)
		if meta.IsEmpty() {
			continue
		}
		score := 1
		if signalMatchesPositionSide(signal, side) {
			score += 2
		}
		if signal.Action == 8 || signal.HasPosition == models.SignalHasOpenPosition {
			score++
		}
		ts := signal.TriggerTimestamp
		if signal.TrendingTimestamp > ts {
			ts = signal.TrendingTimestamp
		}
		if score > bestScore || (score == bestScore && ts > bestTS) {
			best = meta
			bestScore = score
			bestTS = ts
		}
	}
	return best
}

func signalMatchesPositionSide(signal models.Signal, side string) bool {
	side = strings.ToLower(strings.TrimSpace(side))
	if side == "" {
		return true
	}
	switch {
	case signal.HighSide > 0 || signal.MidSide > 0:
		return side == positionSideLong
	case signal.HighSide < 0 || signal.MidSide < 0:
		return side == positionSideShort
	default:
		return false
	}
}

func (r *Live) buildOpenSignalMeta(signal models.Signal, timeframe string) models.StrategyContextMeta {
	signal.Timeframe = strings.TrimSpace(firstNonEmpty(signal.Timeframe, timeframe))
	if len(signal.StrategyTimeframes) == 0 && signal.Timeframe != "" {
		signal.StrategyTimeframes = []string{signal.Timeframe}
	}
	signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey = common.NormalizeStrategyIdentity(
		signal.Timeframe,
		signal.StrategyTimeframes,
		signal.ComboKey,
	)
	if strings.TrimSpace(signal.GroupID) == "" {
		if grouped, ok := r.LookupSignalGrouped(signal); ok {
			signal.GroupID = strings.TrimSpace(grouped.GroupID)
		}
	}
	return models.BuildStrategyContextMetaFromSignal(signal)
}

func strategyPrimaryTimeframe(meta models.StrategyContextMeta) string {
	meta = models.NormalizeStrategyContextMeta(meta)
	primary, _, _ := common.NormalizeStrategyIdentity("", meta.StrategyTimeframes, meta.ComboKey)
	return primary
}

func strategyComboKey(meta models.StrategyContextMeta) string {
	meta = models.NormalizeStrategyContextMeta(meta)
	_, _, comboKey := common.NormalizeStrategyIdentity("", meta.StrategyTimeframes, meta.ComboKey)
	return comboKey
}

func positionComboKey(position models.Position) string {
	_, _, comboKey := common.NormalizeStrategyIdentity(
		position.Timeframe,
		position.StrategyTimeframes,
		position.ComboKey,
	)
	return comboKey
}

func signalComboKey(signalStrategyTimeframes []string, signalTimeframe, signalComboKey string) string {
	_, _, comboKey := common.NormalizeStrategyIdentity(signalTimeframe, signalStrategyTimeframes, signalComboKey)
	return comboKey
}

func positionOwnedBy(position models.Position, signalStrategy, signalTimeframe string, signalTimeframes []string, signalCombo string) bool {
	positionStrategy := strings.TrimSpace(position.StrategyName)
	positionCombo := positionComboKey(position)
	positionPrimary, _, _ := common.NormalizeStrategyIdentity(position.Timeframe, position.StrategyTimeframes, position.ComboKey)
	implicitPositionPrimaryOnly := len(position.StrategyTimeframes) == 0 &&
		(strings.TrimSpace(position.ComboKey) == "" || strings.EqualFold(strings.TrimSpace(position.ComboKey), positionPrimary))
	signalStrategy = strings.TrimSpace(signalStrategy)
	signalPrimary, _, resolvedSignalCombo := common.NormalizeStrategyIdentity(signalTimeframe, signalTimeframes, signalCombo)

	if positionStrategy != "" && signalStrategy != "" && !strings.EqualFold(positionStrategy, signalStrategy) {
		return false
	}
	if positionCombo != "" && resolvedSignalCombo != "" && strings.EqualFold(positionCombo, resolvedSignalCombo) {
		return true
	}
	if implicitPositionPrimaryOnly &&
		positionPrimary != "" && signalPrimary != "" && strings.EqualFold(positionPrimary, signalPrimary) {
		return true
	}
	implicitPrimaryOnly := len(signalTimeframes) == 0 &&
		(strings.TrimSpace(signalCombo) == "" || strings.EqualFold(strings.TrimSpace(signalCombo), signalPrimary))
	if implicitPrimaryOnly &&
		positionPrimary != "" && signalPrimary != "" && strings.EqualFold(positionPrimary, signalPrimary) {
		return true
	}
	if positionCombo != "" && resolvedSignalCombo != "" && !strings.EqualFold(positionCombo, resolvedSignalCombo) {
		return false
	}
	return true
}

func isSameMarketPosition(position models.Position, exchange, symbol string) bool {
	return normalizeExchange(position.Exchange) == normalizeExchange(exchange) &&
		common.CanonicalSymbol(position.Symbol) == common.CanonicalSymbol(symbol)
}

func isOpenPositionForExposure(position models.Position, symbol string) bool {
	if !isPositionOpen(position) {
		return false
	}
	return common.ExposureKey(position.Symbol) != "" &&
		common.ExposureKey(position.Symbol) == common.ExposureKey(symbol)
}

type positionTPSL struct {
	tp float64
	sl float64
}

func buildTPSLIndex(orders []iface.TPSLOrder) map[string]positionTPSL {
	index := make(map[string]positionTPSL, len(orders))
	for _, order := range orders {
		symbol := symbolFromInstID(order.InstID)
		if symbol == "" {
			continue
		}
		side := normalizePositionSide(order.PosSide, 0)
		if side == "" {
			side = inferPositionSideFromTPSLOrder(order.Side)
		}
		if side == "" {
			continue
		}
		key := tpslIndexKey(symbol, side)
		item := index[key]
		if item.tp <= 0 {
			item.tp = parseFloatOrZero(order.TPTriggerPx)
		}
		if item.sl <= 0 {
			item.sl = trailingStopVisiblePrice(order)
		}
		index[key] = item
	}
	return index
}

func trailingStopVisiblePrice(order iface.TPSLOrder) float64 {
	if sl := parseFloatOrZero(order.SLTriggerPx); sl > 0 {
		return sl
	}
	if !strings.EqualFold(strings.TrimSpace(order.OrdType), "move_order_stop") {
		return 0
	}
	if trigger := parseFloatOrZero(order.TriggerPx); trigger > 0 {
		return trigger
	}
	if active := parseFloatOrZero(order.ActivePx); active > 0 {
		return active
	}
	return 0
}

func tpslIndexKey(symbol, side string) string {
	return canonicalSymbol(symbol) + "|" + strings.ToLower(strings.TrimSpace(side))
}

func inferPositionSideFromTPSLOrder(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "sell":
		return positionSideLong
	case "buy":
		return positionSideShort
	default:
		return ""
	}
}

func formatExchangeTimestampMS(raw string) string {
	ts := normalizeTimestampMS(parseInt64OrZero(raw))
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func (r *Live) applyPositionHistory(exchangeName string, history []iface.PositionHistory) {
	type historyEvent struct {
		symbol  string
		closeMS int64
		pnl     float64
	}
	events := make([]historyEvent, 0, len(history))
	for _, item := range history {
		closeMS := normalizeTimestampMS(parseInt64OrZero(item.CloseTime))
		if closeMS <= 0 {
			continue
		}
		symbol := symbolFromInstID(item.InstID)
		if symbol == "" {
			continue
		}
		events = append(events, historyEvent{
			symbol:  symbol,
			closeMS: closeMS,
			pnl:     parseFloatOrZero(item.RealizedPnl),
		})
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].symbol != events[j].symbol {
			return events[i].symbol < events[j].symbol
		}
		return events[i].closeMS < events[j].closeMS
	})

	cfg := r.currentConfig()
	currentTradeDate := time.Now().Format("2006-01-02")
	for _, ev := range events {
		key := livePairKey(exchangeName, ev.symbol)
		state := r.currentCooldown(exchangeName, ev.symbol)
		if ev.closeMS <= state.LastProcessedCloseMS {
			continue
		}

		if tradeDateForTimestampMS(ev.closeMS) == currentTradeDate {
			r.addDailyClosedProfit(exchangeName, ev.pnl)
		}
		if ev.pnl < 0 {
			if tradeDateForTimestampMS(ev.closeMS) == currentTradeDate {
				r.addDailyLoss(exchangeName, -ev.pnl)
			}
			state = applyStopLossCooldown(cfg, state, ev.closeMS)
		} else {
			state.ConsecutiveStopLoss = 0
			state.WindowStartAtMS = 0
			state.LastStopLossAtMS = 0
		}
		state.Exchange = exchangeName
		state.Symbol = ev.symbol
		state.Mode = liveMode
		state.LastProcessedCloseMS = ev.closeMS
		state.UpdatedAtMS = time.Now().UnixMilli()

		r.mu.Lock()
		r.cooldowns[key] = state
		r.mu.Unlock()
		if r.store != nil {
			if err := r.store.UpsertRiskSymbolCooldownState(state); err != nil {
				r.logger.Error("risk symbol cooldown persist failed",
					zap.String("exchange", exchangeName),
					zap.String("symbol", ev.symbol),
					zap.Error(err),
				)
			}
		}
	}
}

func applyStopLossCooldown(cfg RiskConfig, state models.RiskSymbolCooldownState, closeMS int64) models.RiskSymbolCooldownState {
	if !cfg.SymbolCooldown.Enabled {
		return state
	}
	window := parseDurationOrDefault(cfg.SymbolCooldown.Window, 24*time.Hour)
	cooldown := parseDurationOrDefault(cfg.SymbolCooldown.Cooldown, 6*time.Hour)
	threshold := cfg.SymbolCooldown.ConsecutiveStopLoss
	if threshold <= 0 {
		threshold = 2
	}

	if state.WindowStartAtMS <= 0 || closeMS-state.WindowStartAtMS > window.Milliseconds() {
		state.WindowStartAtMS = closeMS
		state.ConsecutiveStopLoss = 1
	} else {
		state.ConsecutiveStopLoss++
	}
	state.LastStopLossAtMS = closeMS
	if state.ConsecutiveStopLoss >= threshold {
		state.CooldownUntilMS = closeMS + cooldown.Milliseconds()
		state.ConsecutiveStopLoss = 0
		state.WindowStartAtMS = closeMS
	}
	return state
}

func (r *Live) loadRiskConfig() error {
	cfg := defaultRiskConfig()
	if r.store == nil {
		r.mu.Lock()
		r.cfg = cfg
		r.mu.Unlock()
		return nil
	}
	value, found, err := r.store.GetConfigValue("risk")
	if err != nil {
		return err
	}
	if !found || strings.TrimSpace(value) == "" {
		raw, marshalErr := json.Marshal(cfg)
		if marshalErr != nil {
			return marshalErr
		}
		if upsertErr := r.store.UpsertConfigValue("risk", string(raw), "Risk config in JSON: hedge/scale-in switches, TP/SL, leverage, sizing, and cooldown rules."); upsertErr != nil {
			return upsertErr
		}
		r.mu.Lock()
		r.cfg = cfg
		r.mu.Unlock()
		return nil
	}
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return fmt.Errorf("risk live: invalid risk config json: %w", err)
	}
	normalizeRiskConfig(&cfg)
	r.mu.Lock()
	r.cfg = cfg
	r.mu.Unlock()
	return nil
}

func (r *Live) loadPersistedState() error {
	if r.store == nil {
		return nil
	}
	for exchangeName := range r.exchanges {
		state, found, err := r.store.GetRiskAccountState(liveMode, exchangeName)
		if err != nil {
			return err
		}
		if found {
			state.Mode = liveMode
			r.accountStates[exchangeName] = state
		}
		items, err := r.store.ListRiskSymbolCooldownStates(liveMode, exchangeName)
		if err != nil {
			return err
		}
		for _, item := range items {
			item.Mode = liveMode
			r.cooldowns[livePairKey(item.Exchange, item.Symbol)] = item
		}
		openPositions, err := r.store.ListRiskOpenPositions(liveMode, exchangeName)
		if err != nil {
			return err
		}
		for _, item := range openPositions {
			item.Mode = liveMode
			key := livePositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode)
			if key == "" {
				continue
			}
			r.openPositions[key] = item
			r.positions[key] = riskOpenPositionToView(item)
		}
	}
	if r.trendGuard != nil {
		groups, err := r.store.ListRiskTrendGroups(liveMode)
		if err != nil {
			return err
		}
		candidates, err := r.store.ListRiskTrendGroupCandidates(liveMode)
		if err != nil {
			return err
		}
		r.trendGuard.restore(groups, candidates)
		r.trendGuard.syncPositions(r.snapshotPositions(), time.Now().UnixMilli())
	}
	return nil
}

func (r *Live) saveRiskConfigDerivedFields(baselineUSDT, dailyLossLimitUSDT float64) {
	if r.store == nil {
		return
	}
	cfg := r.currentConfig()
	cfg.Account.BaselineUSDT = baselineUSDT
	cfg.TradeCooldown.LossLimitUSDT = dailyLossLimitUSDT
	raw, err := json.Marshal(cfg)
	if err != nil {
		r.logger.Error("risk config marshal failed", zap.Error(err))
		return
	}
	if err := r.store.UpsertConfigValue("risk", string(raw), "Risk config in JSON: hedge/scale-in switches, TP/SL, leverage, sizing, and cooldown rules."); err != nil {
		r.logger.Error("risk config persist failed", zap.Error(err))
	}
}

func (r *Live) currentConfig() RiskConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cfg
}

func (r *Live) observeTrendGuardSignal(signal models.Signal, evalCtx models.RiskEvalContext) {
	if r == nil || r.trendGuard == nil {
		return
	}
	cfg := r.currentConfig()
	if !trendGuardGroupedEnabled(cfg.TrendGuard) || models.IsEmptySignal(signal) {
		return
	}
	eventTS := normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.observeSignal(cfg.TrendGuard, signal, evalCtx, eventTS)
}

func (r *Live) RefreshTrendGuardCandidate(signal models.Signal, accountState any) error {
	if r == nil || r.trendGuard == nil {
		return nil
	}
	evalCtx, _ := accountState.(models.RiskEvalContext)
	exchangeName := firstNonEmpty(signal.Exchange, evalCtx.MarketData.Exchange, r.defaultExchange)
	symbol := firstNonEmpty(signal.Symbol, evalCtx.MarketData.Symbol)
	timeframe := firstNonEmpty(signal.Timeframe, evalCtx.MarketData.Timeframe, r.defaultTimeframe)
	signal = normalizeRiskSignal(signal, exchangeName, symbol, timeframe)
	if models.IsEmptySignal(signal) {
		return nil
	}
	cfg := r.currentConfig()
	if !trendGuardGroupedEnabled(cfg.TrendGuard) {
		return nil
	}
	eventTS := normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.refreshCandidate(cfg.TrendGuard, signal, evalCtx, eventTS)
	return nil
}

func (r *Live) reconcileTrendGuardCandidates() {
	if r == nil || r.trendGuard == nil {
		return
	}
	cfg := r.currentConfig()
	if !trendGuardGroupedEnabled(cfg.TrendGuard) {
		return
	}
	r.trendGuard.reconcileCandidates(cfg.TrendGuard, r.ListSignals(), time.Now().UnixMilli())
}

func (r *Live) snapshotPositions() map[string]models.Position {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]models.Position, len(r.positions))
	for key, pos := range r.positions {
		out[key] = pos
	}
	return out
}

func (r *Live) currentAccountState(exchange string) models.RiskAccountState {
	r.mu.RLock()
	state := r.accountStates[exchange]
	r.mu.RUnlock()
	state.Mode = liveMode
	state.Exchange = exchange
	if state.TradeDate == "" {
		state.TradeDate = time.Now().Format("2006-01-02")
	}
	return state
}

func (r *Live) ensureAccountState(exchange string) (models.RiskAccountState, error) {
	state := r.currentAccountState(exchange)
	if state.PerTradeUSDT > 0 {
		return state, nil
	}
	r.refreshAccountStates(false)
	state = r.currentAccountState(exchange)
	if state.PerTradeUSDT <= 0 {
		return state, fmt.Errorf("risk live: account state unavailable for %s", exchange)
	}
	return state, nil
}

func (r *Live) addDailyLoss(exchange string, loss float64) {
	if loss <= 0 {
		return
	}
	r.mu.Lock()
	state := r.accountStates[exchange]
	state.Exchange = exchange
	state.Mode = liveMode
	tradeDate := time.Now().Format("2006-01-02")
	if state.TradeDate != tradeDate {
		state.TradeDate = tradeDate
		state.DailyRealizedUSDT = 0
		state.DailyClosedProfitUSDT = 0
	}
	state.DailyRealizedUSDT += loss
	state.UpdatedAtMS = time.Now().UnixMilli()
	r.accountStates[exchange] = state
	r.mu.Unlock()
	if r.store != nil {
		if err := r.store.UpsertRiskAccountState(state); err != nil {
			r.logger.Error("risk account persist failed", zap.String("exchange", exchange), zap.Error(err))
		}
	}
}

func (r *Live) addDailyClosedProfit(exchange string, profit float64) {
	if profit == 0 {
		return
	}
	r.mu.Lock()
	state := r.accountStates[exchange]
	state.Exchange = exchange
	state.Mode = liveMode
	tradeDate := time.Now().Format("2006-01-02")
	if state.TradeDate != tradeDate {
		state.TradeDate = tradeDate
		state.DailyRealizedUSDT = 0
		state.DailyClosedProfitUSDT = 0
	}
	state.DailyClosedProfitUSDT += profit
	state.UpdatedAtMS = time.Now().UnixMilli()
	r.accountStates[exchange] = state
	r.mu.Unlock()
	if r.store != nil {
		if err := r.store.UpsertRiskAccountState(state); err != nil {
			r.logger.Error("risk account persist failed", zap.String("exchange", exchange), zap.Error(err))
		}
	}
}

func (r *Live) totalUnrealizedProfit(exchange string) float64 {
	if r == nil {
		return 0
	}
	target := normalizeExchange(exchange)
	if target == "" {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := 0.0
	for _, pos := range r.positions {
		if normalizeExchange(pos.Exchange) != target {
			continue
		}
		total += pos.UnrealizedProfitAmount
	}
	return total
}

func (r *Live) currentAvailableUSDT(exchange string) float64 {
	r.mu.RLock()
	value := r.availableUSDT[exchange]
	r.mu.RUnlock()
	return value
}

func (r *Live) openPositionCount() int {
	if r == nil {
		return 0
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, pos := range r.positions {
		if isPositionOpen(pos) {
			count++
		}
	}
	return count
}

func (r *Live) openPositionCountWithPending(nowMS int64) int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prunePendingOpensLocked(nowMS)
	return r.openPositionCountWithPendingLocked()
}

func (r *Live) openPositionCountWithPendingLocked() int {
	count := 0
	openKeys := make(map[string]struct{}, len(r.positions))
	for _, pos := range r.positions {
		if !isPositionOpen(pos) {
			continue
		}
		count++
		side := normalizePositionSide(pos.PositionSide, 0)
		if side == "" {
			continue
		}
		openKeys[livePendingOpenKey(pos.Exchange, pos.Symbol, side)] = struct{}{}
	}
	for key := range r.pendingOpens {
		if _, exists := openKeys[key]; exists {
			continue
		}
		count++
	}
	return count
}

// tryReservePendingOpen atomically checks (open + pending) against limit and reserves one pending slot.
func (r *Live) tryReservePendingOpen(exchange, symbol, side string, nowMS int64, maxOpenPositions int) (int, bool) {
	if r == nil {
		return 0, false
	}
	key := livePendingOpenKey(exchange, symbol, side)
	if key == "" {
		return 0, false
	}
	expiresAt := nowMS + livePendingOpenTTL.Milliseconds()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingOpens == nil {
		r.pendingOpens = make(map[string]int64)
	}
	r.prunePendingOpensLocked(nowMS)
	openCount := r.openPositionCountWithPendingLocked()
	if _, exists := r.pendingOpens[key]; exists {
		return openCount, false
	}
	exposureKey := common.ExposureKey(symbol)
	if exposureKey != "" {
		for _, pos := range r.positions {
			if isOpenPositionForExposure(pos, symbol) {
				return openCount, false
			}
		}
		for pendingKey := range r.pendingOpens {
			if pendingOpenExposureKey(pendingKey) == exposureKey {
				return openCount, false
			}
		}
	}
	if maxOpenPositions > 0 && openCount >= maxOpenPositions {
		return openCount, false
	}
	r.pendingOpens[key] = expiresAt
	return openCount + 1, true
}

func (r *Live) clearPendingOpen(exchange, symbol, side string) {
	if r == nil {
		return
	}
	key := livePendingOpenKey(exchange, symbol, side)
	if key == "" {
		return
	}
	r.mu.Lock()
	delete(r.pendingOpens, key)
	delete(r.pendingMeta, key)
	r.mu.Unlock()
}

func (r *Live) setPendingOpenMeta(exchange, symbol, side string, meta models.StrategyContextMeta) {
	if r == nil {
		return
	}
	key := livePendingOpenKey(exchange, symbol, side)
	if key == "" {
		return
	}
	meta = models.NormalizeStrategyContextMeta(meta)
	if meta.IsEmpty() {
		return
	}
	r.mu.Lock()
	if r.pendingMeta == nil {
		r.pendingMeta = make(map[string]models.StrategyContextMeta)
	}
	r.pendingMeta[key] = meta
	r.mu.Unlock()
}

func (r *Live) DelegatePositionStrategy(exchange, symbol, side, strategy string, timeframes []string) error {
	if r == nil {
		return fmt.Errorf("risk live: nil evaluator")
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	side = normalizePositionSide(side, 0)
	strategy = strings.TrimSpace(strategy)
	primary, normalizedTimeframes, comboKey := common.NormalizeStrategyIdentity("", timeframes, "")
	if exchange == "" || symbol == "" || side == "" {
		return fmt.Errorf("risk live: delegate requires exchange, symbol and side")
	}
	if strategy == "" || len(normalizedTimeframes) == 0 {
		return fmt.Errorf("risk live: delegate requires strategy and trade-enabled timeframes")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	updatedExchange := ""
	updated := false
	for key, item := range r.positions {
		if normalizeExchange(item.Exchange) != exchange {
			continue
		}
		if canonicalSymbol(item.Symbol) != canonicalSymbol(symbol) {
			continue
		}
		if normalizePositionSide(item.PositionSide, 0) != side {
			continue
		}
		item.Timeframe = primary
		item.StrategyName = strategy
		item.StrategyVersion = ""
		item.StrategyTimeframes = append([]string(nil), normalizedTimeframes...)
		item.ComboKey = comboKey
		r.positions[key] = item

		openItem, ok := r.openPositions[key]
		if ok {
			env, parsed := models.ParsePositionRowEnvelope(openItem.RowJSON)
			raw := env.ExchangeRaw
			if !parsed {
				raw = nil
			}
			openItem.RowJSON = models.MarshalPositionRowEnvelopeWithRuntime(raw, models.StrategyContextMeta{
				StrategyName:       strategy,
				StrategyTimeframes: normalizedTimeframes,
				ComboKey:           comboKey,
			}, preserveInitialPositionRuntimeMeta(openItem.RowJSON, models.ExtractPositionRuntimeMeta(openItem.RowJSON)))
			openItem.UpdatedAtMS = time.Now().UnixMilli()
			r.openPositions[key] = openItem
		}
		updatedExchange = normalizeExchange(item.Exchange)
		updated = true
	}
	if !updated {
		return fmt.Errorf("risk live: open position not found")
	}

	if r.store != nil && updatedExchange != "" {
		openSnapshots := make([]models.RiskOpenPosition, 0)
		for _, item := range r.openPositions {
			if normalizeExchange(item.Exchange) == updatedExchange {
				openSnapshots = append(openSnapshots, item)
			}
		}
		if err := r.store.SyncRiskPositions(liveMode, updatedExchange, openSnapshots, nil); err != nil {
			return err
		}
		if r.historyArchive != nil {
			if err := r.historyArchive.SyncOpenPositions(updatedExchange, openSnapshots); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Live) prunePendingOpensLocked(nowMS int64) {
	if len(r.pendingOpens) == 0 {
		return
	}
	for key, expiresAt := range r.pendingOpens {
		if expiresAt <= 0 || expiresAt > nowMS {
			continue
		}
		delete(r.pendingOpens, key)
		delete(r.pendingMeta, key)
	}
}

// NotifyExecutionResult lets risk reconcile optimistic pending-open records with actual execution outcome.
func (r *Live) NotifyExecutionResult(decision models.Decision, execErr error) {
	if r == nil {
		return
	}
	side := openSideFromDecision(decision)
	if side == "" {
		return
	}
	if execErr != nil {
		r.clearPendingOpen(decision.Exchange, decision.Symbol, side)
		return
	}
	// Keep pending until position refresh confirms, then syncOpenPositions clears it.
}

func (r *Live) currentCooldown(exchange, symbol string) models.RiskSymbolCooldownState {
	key := livePairKey(exchange, symbol)
	r.mu.RLock()
	state := r.cooldowns[key]
	r.mu.RUnlock()
	state.Mode = liveMode
	state.Exchange = exchange
	state.Symbol = symbol
	return state
}

func (r *Live) getCachedPosition(exchange, symbol string) (models.Position, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, pos := range r.positions {
		if normalizeExchange(pos.Exchange) != normalizeExchange(exchange) {
			continue
		}
		if canonicalSymbol(pos.Symbol) != canonicalSymbol(symbol) {
			continue
		}
		return pos, true
	}
	return models.Position{}, false
}

func (r *Live) exchangeFor(exchange string) (iface.Exchange, error) {
	if r == nil {
		return nil, fmt.Errorf("risk live: nil evaluator")
	}
	if len(r.exchanges) == 0 {
		return nil, fmt.Errorf("risk live: trade exchange map is empty")
	}
	key := normalizeExchange(exchange)
	ex, ok := r.exchanges[key]
	if !ok || ex == nil {
		return nil, fmt.Errorf("risk live: exchange not configured: %s", key)
	}
	return ex, nil
}

func cloneExchangeMap(input map[string]iface.Exchange) map[string]iface.Exchange {
	if len(input) == 0 {
		return map[string]iface.Exchange{}
	}
	out := make(map[string]iface.Exchange, len(input))
	for name, ex := range input {
		key := normalizeExchange(name)
		if key == "" || ex == nil {
			continue
		}
		out[key] = ex
	}
	return out
}

func buildComboTradeEnabledMap(input []models.StrategyComboConfig) map[string]bool {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]bool, len(input))
	for _, combo := range input {
		comboKey := signalComboKey(combo.Timeframes, "", "")
		if comboKey == "" {
			continue
		}
		out[comboKey] = combo.TradeEnabled
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (r *Live) tradeAllowed(exchange string) bool {
	return normalizeExchange(exchange) != ""
}

func (r *Live) comboTradeAllowed(signal models.Signal) bool {
	if isManualTradeSignal(signal) {
		return true
	}
	if r == nil || len(r.comboTradeEnabled) == 0 {
		return true
	}
	comboKey := signalComboKey(signal.StrategyTimeframes, signal.Timeframe, signal.ComboKey)
	enabled, ok := r.comboTradeEnabled[comboKey]
	if !ok {
		return false
	}
	return enabled
}

func comboConfigured(configured map[string]bool, signal models.Signal) bool {
	if isManualTradeSignal(signal) {
		return true
	}
	if len(configured) == 0 {
		return true
	}
	comboKey := signalComboKey(signal.StrategyTimeframes, signal.Timeframe, signal.ComboKey)
	if comboKey == "" {
		return false
	}
	_, ok := configured[comboKey]
	return ok
}

func isManualTradeSignal(signal models.Signal) bool {
	return strings.EqualFold(strings.TrimSpace(signal.Strategy), "manual")
}

func defaultRiskConfig() RiskConfig {
	cfg := RiskConfig{
		AllowHedge:       false,
		AllowScaleIn:     false,
		MaxOpenPositions: 3,
		TrendGuard: RiskTrendGuardConfig{
			Enabled:                true,
			Mode:                   trendGuardModeLegacy,
			MaxStartLagBars:        12,
			LeaderMinPriorityScore: 50,
		},
		TP: RiskTPConfig{
			Mode:              tpModeFixed,
			DefaultPct:        1.0,
			OnlyRaiseOnUpdate: true,
		},
		SL: RiskSLConfig{
			MaxLossPct:        0.05,
			OnlyRaiseOnUpdate: true,
			RequireSignal:     true,
		},
		Leverage: RiskLeverageConfig{
			Min: 1,
			Max: 10,
		},
		Account: RiskAccountConfig{
			Currency: "USDT",
		},
		PerTrade: RiskPerTradeConfig{
			Ratio: 0.10,
		},
		SymbolCooldown: RiskSymbolCooldownConfig{
			Enabled:             true,
			ConsecutiveStopLoss: 2,
			Cooldown:            "6h",
			Window:              "24h",
		},
		TradeCooldown: RiskTradeCooldownConfig{
			Enabled:             true,
			LossRatioOfPerTrade: 0.50,
		},
	}
	normalizeRiskConfig(&cfg)
	return cfg
}

func normalizeRiskConfig(cfg *RiskConfig) {
	if cfg == nil {
		return
	}
	if cfg.MaxOpenPositions <= 0 {
		cfg.MaxOpenPositions = 3
	}
	cfg.TrendGuard.Mode = normalizeTrendGuardMode(cfg.TrendGuard.Mode)
	if cfg.TrendGuard.MaxStartLagBars <= 0 {
		cfg.TrendGuard.MaxStartLagBars = 12
	}
	if cfg.TrendGuard.LeaderMinPriorityScore <= 0 {
		cfg.TrendGuard.LeaderMinPriorityScore = 50
	}
	cfg.TP.Mode = normalizeTPMode(cfg.TP.Mode)
	if cfg.TP.Mode == tpModeFixed && cfg.TP.DefaultPct <= 0 {
		cfg.TP.DefaultPct = 1.0
	}
	if cfg.SL.MaxLossPct <= 0 {
		cfg.SL.MaxLossPct = 0.05
	}
	if cfg.Leverage.Min <= 0 {
		cfg.Leverage.Min = 1
	}
	if cfg.Leverage.Max <= 0 {
		cfg.Leverage.Max = 10
	}
	if cfg.Leverage.Max < cfg.Leverage.Min {
		cfg.Leverage.Max = cfg.Leverage.Min
	}
	if strings.TrimSpace(cfg.Account.Currency) == "" {
		cfg.Account.Currency = "USDT"
	}
	if cfg.PerTrade.Ratio <= 0 {
		cfg.PerTrade.Ratio = 0.10
	}
	if cfg.SymbolCooldown.ConsecutiveStopLoss <= 0 {
		cfg.SymbolCooldown.ConsecutiveStopLoss = 2
	}
	if strings.TrimSpace(cfg.SymbolCooldown.Cooldown) == "" {
		cfg.SymbolCooldown.Cooldown = "6h"
	}
	if strings.TrimSpace(cfg.SymbolCooldown.Window) == "" {
		cfg.SymbolCooldown.Window = "24h"
	}
	if cfg.TradeCooldown.LossRatioOfPerTrade <= 0 {
		cfg.TradeCooldown.LossRatioOfPerTrade = 0.50
	}
}

func normalizeExchange(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}

func normalizeTradeInstID(client iface.Exchange, symbol string) (string, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "", fmt.Errorf("risk live: symbol is required")
	}
	text := strings.ToUpper(symbol)
	if strings.HasSuffix(text, "-SWAP") {
		return text, nil
	}
	if idx := strings.Index(text, ":"); idx > 0 {
		text = text[:idx]
	}
	if strings.Contains(text, "/") && !strings.HasSuffix(text, ".P") {
		text += ".P"
	}
	instID, err := client.NormalizeSymbol(text)
	if err != nil {
		return "", fmt.Errorf("risk live: normalize symbol %s failed: %w", symbol, err)
	}
	return instID, nil
}

func symbolFromInstID(instID string) string {
	text := strings.ToUpper(strings.TrimSpace(instID))
	if text == "" {
		return ""
	}
	if strings.HasSuffix(text, "-SWAP") {
		parts := strings.Split(text, "-")
		if len(parts) >= 3 && parts[0] != "" && parts[1] != "" {
			return parts[0] + "/" + parts[1]
		}
	}
	if parts := strings.Split(text, "-"); len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
		return parts[0] + "/" + parts[1]
	}
	if compact, ok := splitCompactSymbol(text); ok {
		return compact
	}
	return text
}

func canonicalSymbol(symbol string) string {
	return common.CanonicalSymbol(symbol)
}

func splitCompactSymbol(raw string) (string, bool) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" || strings.Contains(raw, "/") || strings.Contains(raw, "-") {
		return "", false
	}
	for _, quote := range []string{"USDT", "USDC", "BUSD", "USD", "BTC", "ETH"} {
		if !strings.HasSuffix(raw, quote) || len(raw) <= len(quote) {
			continue
		}
		base := strings.TrimSpace(raw[:len(raw)-len(quote)])
		if base == "" {
			continue
		}
		return base + "/" + quote, true
	}
	return "", false
}

func pickUSDTValue(items []iface.Balance, preferAvailable bool) float64 {
	for _, item := range items {
		if !strings.EqualFold(strings.TrimSpace(item.Ccy), "USDT") {
			continue
		}
		if preferAvailable {
			for _, value := range []string{item.AvailBal, item.AvailEq, item.Eq, item.Bal} {
				if parsed := parseFloatOrZero(value); parsed > 0 {
					return parsed
				}
			}
			continue
		}
		for _, value := range []string{item.Eq, item.Bal, item.AvailEq, item.AvailBal} {
			if parsed := parseFloatOrZero(value); parsed > 0 {
				return parsed
			}
		}
	}
	return 0
}

func sumPositionMargin(positions []iface.Position) float64 {
	total := 0.0
	for _, pos := range positions {
		margin := parseFloatOrZero(pos.Margin)
		if margin > 0 {
			total += margin
		}
	}
	return total
}

func parseFloatOrZero(raw string) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return value
}

func parseInt64OrZero(raw string) int64 {
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func normalizePositionSide(side string, highSide int) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case positionSideLong:
		return positionSideLong
	case positionSideShort:
		return positionSideShort
	}
	switch highSide {
	case 1:
		return positionSideLong
	case -1:
		return positionSideShort
	default:
		return ""
	}
}

func resolveHistoryPositionSide(item iface.PositionHistory) string {
	if side := normalizePositionSide(item.PosSide, 0); side != "" {
		return side
	}
	switch strings.ToLower(strings.TrimSpace(item.Direction)) {
	case positionSideLong, "buy":
		return positionSideLong
	case positionSideShort, "sell":
		return positionSideShort
	}
	pos := parseFloatOrZero(item.Pos)
	if pos > 0 {
		return positionSideLong
	}
	if pos < 0 {
		return positionSideShort
	}
	return "net"
}

func highSideFromQuantity(qty float64) int {
	if qty > 0 {
		return 1
	}
	if qty < 0 {
		return -1
	}
	return 0
}

func normalizeMarginMode(current, fallback string) string {
	value := strings.ToLower(strings.TrimSpace(current))
	if value != "" {
		return value
	}
	value = strings.ToLower(strings.TrimSpace(fallback))
	if value != "" {
		return value
	}
	return models.MarginModeIsolated
}

func chooseLeverage(current, fallback float64) float64 {
	if current > 0 {
		return current
	}
	if fallback > 0 {
		return fallback
	}
	return 1
}

func estimatePositionQuantity(position models.Position, signal models.Signal, data models.MarketData) float64 {
	if position.EntryQuantity > 0 {
		return position.EntryQuantity
	}
	price := firstPositive(position.CurrentPrice, position.EntryPrice, signal.Entry, marketPrice(data, 0))
	if price <= 0 {
		return 0
	}
	if position.MarginAmount > 0 && position.LeverageMultiplier > 0 {
		return position.MarginAmount * position.LeverageMultiplier / price
	}
	return 0
}

func ensurePositionOwnership(position models.Position, signalStrategy, signalTimeframe string, signalTimeframes []string, signalCombo string) error {
	positionStrategy := strings.TrimSpace(position.StrategyName)
	positionCombo := positionComboKey(position)
	positionPrimary, _, _ := common.NormalizeStrategyIdentity(position.Timeframe, position.StrategyTimeframes, position.ComboKey)
	implicitPositionPrimaryOnly := len(position.StrategyTimeframes) == 0 &&
		(strings.TrimSpace(position.ComboKey) == "" || strings.EqualFold(strings.TrimSpace(position.ComboKey), positionPrimary))
	signalStrategy = strings.TrimSpace(signalStrategy)
	signalPrimary, _, resolvedSignalCombo := common.NormalizeStrategyIdentity(signalTimeframe, signalTimeframes, signalCombo)

	if implicitPositionPrimaryOnly &&
		positionPrimary != "" && signalPrimary != "" && strings.EqualFold(positionPrimary, signalPrimary) &&
		(positionStrategy == "" || signalStrategy == "" || strings.EqualFold(positionStrategy, signalStrategy)) {
		return nil
	}
	implicitPrimaryOnly := len(signalTimeframes) == 0 &&
		(strings.TrimSpace(signalCombo) == "" || strings.EqualFold(strings.TrimSpace(signalCombo), signalPrimary))
	if implicitPrimaryOnly &&
		positionPrimary != "" && signalPrimary != "" && strings.EqualFold(positionPrimary, signalPrimary) &&
		(positionStrategy == "" || signalStrategy == "" || strings.EqualFold(positionStrategy, signalStrategy)) {
		return nil
	}

	if positionStrategy != "" && signalStrategy != "" && !strings.EqualFold(positionStrategy, signalStrategy) {
		return fmt.Errorf(
			"risk live: position ownership mismatch, strategy %s/%s",
			positionStrategy,
			signalStrategy,
		)
	}
	if positionCombo != "" && resolvedSignalCombo != "" && !strings.EqualFold(positionCombo, resolvedSignalCombo) {
		return fmt.Errorf(
			"risk live: position ownership mismatch, combo %s/%s",
			positionCombo,
			resolvedSignalCombo,
		)
	}
	return nil
}

func firstPositive(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func isPositionOpen(position models.Position) bool {
	status := strings.ToLower(strings.TrimSpace(position.Status))
	if status == "" {
		return position.EntryQuantity > 0
	}
	return status == models.PositionStatusOpen
}

func newRiskClientOrderID(exchange, symbol string) string {
	return buildRiskClientOrderID("risk", exchange, symbol, "")
}

func buildRiskClientOrderID(prefix, exchange, symbol, suffix string) string {
	const maxLen = 32
	prefix = strings.ToLower(sanitizeClientOrderToken(prefix))
	if prefix == "" {
		prefix = "x"
	}
	exchange = strings.ToLower(sanitizeClientOrderToken(normalizeExchange(exchange)))
	if exchange == "" {
		exchange = "x"
	}
	symbol = strings.ToLower(sanitizeClientOrderToken(symbol))
	if symbol == "" {
		symbol = "x"
	}
	suffix = strings.ToLower(sanitizeClientOrderToken(suffix))
	timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
	head := prefix + exchange
	middle := symbol + suffix
	maxMiddle := maxLen - len(head) - len(timestamp)
	if maxMiddle < 0 {
		maxMiddle = 0
	}
	if len(middle) > maxMiddle {
		middle = middle[:maxMiddle]
	}
	id := head + middle + timestamp
	if len(id) > maxLen {
		id = id[:maxLen]
	}
	if len(id) == 0 {
		return "x"
	}
	return id
}

func sanitizeClientOrderToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, ch := range raw {
		switch {
		case ch >= 'a' && ch <= 'z':
			b.WriteRune(ch)
		case ch >= 'A' && ch <= 'Z':
			b.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			b.WriteRune(ch)
		}
	}
	return b.String()
}

func livePairKey(exchange, symbol string) string {
	return normalizeExchange(exchange) + "|" + canonicalSymbol(symbol)
}

func livePendingOpenKey(exchange, symbol, side string) string {
	exchange = normalizeExchange(exchange)
	symbol = canonicalSymbol(symbol)
	side = normalizePositionSide(side, 0)
	if exchange == "" || symbol == "" || side == "" {
		return ""
	}
	return exchange + "|" + symbol + "|" + side
}

func pendingOpenExposureKey(key string) string {
	parts := strings.Split(key, "|")
	if len(parts) < 3 {
		return ""
	}
	return common.ExposureKey(parts[1])
}

func openSideFromDecision(decision models.Decision) string {
	switch decision.Action {
	case models.DecisionActionOpenLong:
		return positionSideLong
	case models.DecisionActionOpenShort:
		return positionSideShort
	default:
		return ""
	}
}

func livePositionKey(exchange, instID, posSide, mgnMode string) string {
	exchange = normalizeExchange(exchange)
	instID = strings.ToUpper(strings.TrimSpace(instID))
	posSide = strings.ToLower(strings.TrimSpace(posSide))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || posSide == "" || mgnMode == "" {
		return ""
	}
	return exchange + "|" + instID + "|" + posSide + "|" + mgnMode
}

func defaultTP(side string, entry float64, targetProfitPct float64, leverage float64) float64 {
	if entry <= 0 || targetProfitPct <= 0 {
		return 0
	}
	if leverage <= 0 {
		leverage = 1
	}
	movePct := targetProfitPct / leverage
	if movePct <= 0 {
		return 0
	}
	switch side {
	case positionSideLong:
		return entry * (1 + movePct)
	case positionSideShort:
		if movePct >= 1 {
			return 0
		}
		return entry * (1 - movePct)
	default:
		return 0
	}
}

func normalizeTPMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case tpModeDisabled:
		return tpModeDisabled
	default:
		return tpModeFixed
	}
}

func resolveOpenTakeProfit(cfg RiskTPConfig, side string, entryPrice float64, signalTP float64, leverage float64) float64 {
	if signalTP > 0 {
		return signalTP
	}
	if normalizeTPMode(cfg.Mode) == tpModeDisabled {
		return 0
	}
	return defaultTP(side, entryPrice, cfg.DefaultPct, leverage)
}

func resolveUpdateTakeProfit(cfg RiskTPConfig, signalTP float64, currentTP float64) float64 {
	if signalTP > 0 {
		return signalTP
	}
	if normalizeTPMode(cfg.Mode) == tpModeDisabled {
		return 0
	}
	return currentTP
}

func isTPMoveAllowed(side string, currentTP, nextTP float64) bool {
	if nextTP <= 0 {
		return false
	}
	switch side {
	case positionSideLong:
		return nextTP >= currentTP
	case positionSideShort:
		return nextTP <= currentTP
	default:
		return false
	}
}

func isSLMoveAllowed(side string, currentSL, nextSL float64) bool {
	if nextSL <= 0 {
		return false
	}
	switch side {
	case positionSideLong:
		return nextSL >= currentSL
	case positionSideShort:
		return nextSL <= currentSL
	default:
		return false
	}
}

func validateTPSLAgainstPrice(side string, refPrice, tp, sl float64) error {
	if refPrice <= 0 {
		return nil
	}
	switch side {
	case positionSideLong:
		if tp > 0 && tp <= refPrice {
			return fmt.Errorf("risk live: TP for long must be greater than reference price %.8f", refPrice)
		}
		if sl > 0 && sl >= refPrice {
			return fmt.Errorf("risk live: SL for long must be less than reference price %.8f", refPrice)
		}
	case positionSideShort:
		if tp > 0 && tp >= refPrice {
			return fmt.Errorf("risk live: TP for short must be less than reference price %.8f", refPrice)
		}
		if sl > 0 && sl <= refPrice {
			return fmt.Errorf("risk live: SL for short must be greater than reference price %.8f", refPrice)
		}
	}
	return nil
}

func parseDurationOrDefault(raw string, fallback time.Duration) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func calculateRiskOpenSize(perTradeUSDT float64, leverage int, entryPrice float64, inst iface.Instrument) (float64, error) {
	if perTradeUSDT <= 0 {
		return 0, fmt.Errorf("risk live: invalid per-trade budget")
	}
	if leverage <= 0 {
		return 0, fmt.Errorf("risk live: invalid leverage")
	}
	if entryPrice <= 0 {
		return 0, fmt.Errorf("risk live: invalid entry price")
	}
	notional := perTradeUSDT * float64(leverage)
	denominator := entryPrice
	if inst.CtVal > 0 {
		denominator = entryPrice * inst.CtVal
	}
	if denominator <= 0 {
		return 0, fmt.Errorf("risk live: invalid size denominator")
	}
	size := notional / denominator
	if inst.LotSz > 0 {
		size = math.Floor(size/inst.LotSz+1e-12) * inst.LotSz
	}
	if size <= 0 {
		if inst.LotSz > 0 {
			return 0, fmt.Errorf("risk live: order size %.8f is below lot size %.8f", size, inst.LotSz)
		}
		return 0, fmt.Errorf("risk live: invalid order size")
	}
	if inst.MinSz > 0 && size < inst.MinSz {
		return 0, fmt.Errorf("risk live: order size %.8f is below min size %.8f", size, inst.MinSz)
	}
	return size, nil
}

func tradeDateForTimestampMS(ts int64) string {
	ts = normalizeTimestampMS(ts)
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).Format("2006-01-02")
}

var _ iface.Evaluator = (*Live)(nil)
