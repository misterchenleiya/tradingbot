package risk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const (
	simulationAvailableBudget = 1_000_000_000.0
	simulationMarginBasis     = 1.0
	backTestMaxLossRate       = 0.05
	backTestCooldown          = 6 * time.Hour

	liveMode     = "live"
	backTestMode = "back-test"
	paperMode    = "paper"

	positionSideLong  = "long"
	positionSideShort = "short"
)

type BackTestStore interface {
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
	ListRiskHistorySnapshots(mode, exchange string) ([]models.RiskHistoryPosition, error)
	ListExecutionOrders(mode, singletonUUID string) ([]models.ExecutionOrderRecord, error)
	AppendSignalChange(record models.SignalChangeRecord) error
	ListSignalChangesByPair(mode, exchange, symbol string) ([]models.SignalChangeRecord, error)
	SyncRiskPositions(mode, exchange string, openPositions []models.RiskOpenPosition, closedPositions []models.RiskClosedPosition) error
}

type BackTestConfig struct {
	Logger           *zap.Logger
	Store            BackTestStore
	HistoryArchive   iface.HistoryArchiveMirror
	StrategyCombos   []models.StrategyComboConfig
	ActiveStrategies []string
	RunID            string
	SingletonUUID    string
	SingletonID      int64
	Mode             string
}

type BackTestTrade struct {
	TradeID         int64
	Exchange        string
	Symbol          string
	Timeframe       string
	GroupID         string
	Side            string
	Strategy        string
	StrategyVersion string
	Margin          float64
	Leverage        float64
	EntryPrice      float64
	EntryQuantity   float64
	EntryTS         int64
	ExitPrice       float64
	ExitQuantity    float64
	ExitTS          int64
	Profit          float64
	ProfitRate      float64
	MaxDrawdownRate float64
	MaxProfitRate   float64
	Status          string
	CloseReason     string
}

type BackTestPositionEvent struct {
	EventID           int64
	EventTS           int64
	KlineTS           int64
	Exchange          string
	Symbol            string
	Timeframe         string
	Side              string
	Strategy          string
	StrategyVersion   string
	Action            string
	Price             float64
	Quantity          float64
	RemainingQuantity float64
	Margin            float64
	Leverage          float64
	TakeProfitPrice   float64
	StopLossPrice     float64
	CloseReason       string
	Result            string
	Reason            string
}

type BackTestReport struct {
	ReturnRate          float64
	CircuitBreaker      bool
	CooldownUntilTS     int64
	TotalTrades         int
	ClosedTrades        int
	WinTrades           int
	LossTrades          int
	FlatTrades          int
	WinRate             float64
	TotalPnL            float64
	AvgPnL              float64
	AvgPnLRate          float64
	MaxProfit           float64
	MaxLoss             float64
	ForcedCloseCount    int
	TotalPositionEvents int
	Trades              []BackTestTrade
	PositionEvents      []BackTestPositionEvent
	OpenPositions       []models.Position
}

type backTestPosition struct {
	Exchange                string
	Symbol                  string
	Timeframe               string
	ComboKey                string
	GroupID                 string
	Side                    string
	Strategy                string
	StrategyVersion         string
	StrategyTimeframes      []string
	StrategyIndicators      map[string][]string
	Margin                  float64
	Leverage                float64
	EntryPrice              float64
	EntryQuantity           float64
	RemainingQty            float64
	ClosedQty               float64
	ExitNotional            float64
	RealizedPnL             float64
	EntryTS                 int64
	TakeProfitPrice         float64
	StopLossPrice           float64
	MaxFloatingLossAmount   float64
	MaxFloatingProfitAmount float64
}

type backTestMark struct {
	TS    int64
	Price float64
	High  float64
	Low   float64
}

type backTestOpenMeta struct {
	SingletonID     int64  `json:"singleton_id,omitempty"`
	RunID           string `json:"run_id"`
	Timeframe       string `json:"timeframe"`
	GroupID         string `json:"group_id,omitempty"`
	Strategy        string `json:"strategy"`
	StrategyVersion string `json:"strategy_version"`
	EntryTS         int64  `json:"entry_ts"`
}

type backTestCloseMeta struct {
	SingletonID     int64  `json:"singleton_id,omitempty"`
	RunID           string `json:"run_id"`
	CloseReason     string `json:"close_reason"`
	EventReason     string `json:"event_reason"`
	Strategy        string `json:"strategy"`
	StrategyVersion string `json:"strategy_version"`
}

type BackTest struct {
	logger            *zap.Logger
	store             BackTestStore
	historyArchive    iface.HistoryArchiveMirror
	activeSet         map[string]struct{}
	runID             string
	mode              string
	singletonUUID     string
	singletonID       int64
	comboTradeEnabled map[string]bool

	mu      sync.Mutex
	started atomic.Bool

	cfg           RiskConfig
	startedAtMS   int64
	accountStates map[string]models.RiskAccountState
	cooldowns     map[string]models.RiskSymbolCooldownState
	positions     map[string]*backTestPosition
	marks         map[string]backTestMark
	signalCache   *SignalCache
	trendGuard    *groupedTrendGuard

	trades            []BackTestTrade
	nextTradeID       int64
	positionEvents    []BackTestPositionEvent
	nextPositionEvent int64

	report      BackTestReport
	reportReady bool
}

func NewBackTest(cfg BackTestConfig) *BackTest {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	runID := strings.TrimSpace(cfg.RunID)
	if runID == "" {
		runID = fmt.Sprintf("backtest-%d", time.Now().UnixMilli())
	}
	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = backTestMode
	}
	singletonUUID := strings.TrimSpace(cfg.SingletonUUID)
	if singletonUUID == "" {
		singletonUUID = runID
	}
	return &BackTest{
		logger:            logger,
		store:             cfg.Store,
		historyArchive:    cfg.HistoryArchive,
		activeSet:         buildActiveStrategySet(cfg.ActiveStrategies),
		runID:             runID,
		mode:              mode,
		singletonUUID:     singletonUUID,
		singletonID:       cfg.SingletonID,
		comboTradeEnabled: buildComboTradeEnabledMap(cfg.StrategyCombos),
		cfg:               defaultRiskConfig(),
		accountStates:     make(map[string]models.RiskAccountState),
		cooldowns:         make(map[string]models.RiskSymbolCooldownState),
		positions:         make(map[string]*backTestPosition),
		marks:             make(map[string]backTestMark),
		signalCache:       NewSignalCache(),
		trendGuard:        newGroupedTrendGuard(logger, cfg.Store, mode),
	}
}

func (r *BackTest) Start(_ context.Context) error {
	if r == nil {
		return fmt.Errorf("nil risk back-test")
	}
	logger := r.logger
	if logger == nil {
		logger = glog.Nop()
	}
	if err := r.loadRiskConfig(); err != nil {
		return err
	}
	r.startedAtMS = time.Now().UnixMilli()
	logger.Info("risk back-test start",
		zap.String("mode", r.reportMode()),
		zap.Float64("simulation_margin_basis", simulationMarginBasis),
		zap.String("run_id", r.runID),
		zap.Int64("singleton_id", r.singletonID),
		zap.Int("active_strategy_count", len(r.activeSet)),
		zap.Strings("active_strategies", listActiveStrategies(r.activeSet)),
		zap.Int("max_open_positions", r.cfg.MaxOpenPositions),
		zap.Bool("trend_guard_enabled", r.cfg.TrendGuard.Enabled),
		zap.String("trend_guard_mode", r.cfg.TrendGuard.Mode),
		zap.Int("trend_guard_max_start_lag_bars", r.cfg.TrendGuard.MaxStartLagBars),
		zap.String("tp_mode", r.cfg.TP.Mode),
		zap.Float64("per_trade_ratio", r.cfg.PerTrade.Ratio),
		zap.Float64("sl_max_loss_pct", r.cfg.SL.MaxLossPct),
		zap.Int("leverage_min", r.cfg.Leverage.Min),
		zap.Int("leverage_max", r.cfg.Leverage.Max),
		zap.Bool("allow_hedge", r.cfg.AllowHedge),
		zap.Bool("allow_scale_in", r.cfg.AllowScaleIn),
	)
	defer logger.Info("risk back-test started")
	r.mu.Lock()
	r.restoreSignalsFromStoreLocked()
	if err := r.restoreTrendGuardLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()
	r.started.Store(true)
	return nil
}

func (r *BackTest) Close() error {
	if r == nil {
		return nil
	}
	logger := r.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("risk back-test close")
	defer logger.Info("risk back-test closed")
	r.started.Store(false)
	return nil
}

func (r *BackTest) OnMarketData(data models.MarketData) error {
	if r == nil {
		return nil
	}
	price := marketPrice(data, 0)
	ts := normalizeTimestampMS(data.OHLCV.TS)
	if price <= 0 || ts <= 0 {
		return nil
	}

	pair := pairKey(data.Exchange, data.Symbol)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.reportReady = false

	mark := r.marks[pair]
	if ts >= mark.TS {
		mark = backTestMark{
			TS:    ts,
			Price: price,
			High:  firstPositive(data.OHLCV.High, price),
			Low:   firstPositive(data.OHLCV.Low, price),
		}
		if mark.Low <= 0 {
			mark.Low = mark.Price
		}
		r.marks[pair] = mark
	}

	pos := r.positions[pair]
	if pos == nil {
		return nil
	}

	r.updateFloatingPnLLocked(pos, mark)
	if executionTimeframe := backTestExecutionTimeframe(pos); executionTimeframe != "" &&
		!strings.EqualFold(strings.TrimSpace(data.Timeframe), executionTimeframe) {
		r.persistPositionSnapshotsLocked(normalizeExchange(pos.Exchange), nil)
		return nil
	}

	switch pos.Side {
	case positionSideLong:
		if pos.StopLossPrice > 0 && data.OHLCV.Low > 0 && data.OHLCV.Low <= pos.StopLossPrice {
			r.closePositionLocked(pair, pos, pos.StopLossPrice, ts, pos.RemainingQty, "stop_loss", "stop_loss_triggered", false)
			return nil
		}
		if pos.TakeProfitPrice > 0 && data.OHLCV.High > 0 && data.OHLCV.High >= pos.TakeProfitPrice {
			r.closePositionLocked(pair, pos, pos.TakeProfitPrice, ts, pos.RemainingQty, "take_profit", "take_profit_triggered", false)
			return nil
		}
	case positionSideShort:
		if pos.StopLossPrice > 0 && data.OHLCV.High > 0 && data.OHLCV.High >= pos.StopLossPrice {
			r.closePositionLocked(pair, pos, pos.StopLossPrice, ts, pos.RemainingQty, "stop_loss", "stop_loss_triggered", false)
			return nil
		}
		if pos.TakeProfitPrice > 0 && data.OHLCV.Low > 0 && data.OHLCV.Low <= pos.TakeProfitPrice {
			r.closePositionLocked(pair, pos, pos.TakeProfitPrice, ts, pos.RemainingQty, "take_profit", "take_profit_triggered", false)
			return nil
		}
	}

	r.persistPositionSnapshotsLocked(normalizeExchange(pos.Exchange), nil)
	return nil
}

func (r *BackTest) EvaluateOpenBatch(signals []models.Signal, accountState any) (models.Decision, error) {
	if len(signals) == 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	return r.evaluateSignalLocked(signals[0], extractRiskEvalContext(accountState), models.Position{})
}

func (r *BackTest) EvaluateUpdate(signal models.Signal, position models.Position, accountState any) (models.Decision, error) {
	return r.evaluateSignalLocked(signal, extractRiskEvalContext(accountState), position)
}

func (r *BackTest) evaluateSignalLocked(signal models.Signal, evalCtx models.RiskEvalContext, fallback models.Position) (models.Decision, error) {
	if r == nil {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	data := evalCtx.MarketData

	exchange := normalizeExchange(firstNonEmpty(signal.Exchange, data.Exchange, fallback.Exchange))
	symbol := firstNonEmpty(signal.Symbol, data.Symbol, fallback.Symbol)
	if exchange == "" || symbol == "" {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: missing exchange/symbol")
	}
	timeframe := firstNonEmpty(signal.Timeframe, data.Timeframe, fallback.Timeframe)
	ts := resolveSignalTimestamp(data.OHLCV.TS, signal.TriggerTimestamp)
	if ts <= 0 {
		ts = normalizeTimestampMS(data.OHLCV.TS)
	}
	if ts <= 0 {
		ts = time.Now().UnixMilli()
	}
	pair := pairKey(exchange, symbol)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.reportReady = false
	signal = normalizeRiskSignal(signal, exchange, symbol, timeframe)
	position, hasPosition := r.currentPositionForPairLocked(pair)
	if !hasPosition && fallback.Exchange != "" && fallback.Symbol != "" {
		position = fallback
		hasPosition = isPositionOpen(fallback)
	}
	if hasPosition && len(signal.StrategyTimeframes) == 0 && strings.TrimSpace(signal.ComboKey) == "" {
		signal.StrategyTimeframes = append([]string(nil), position.StrategyTimeframes...)
		signal.ComboKey = strings.TrimSpace(position.ComboKey)
	}
	signal.Strategy = strings.TrimSpace(firstNonEmpty(signal.Strategy, position.StrategyName))
	signal.Timeframe = strings.TrimSpace(firstNonEmpty(signal.Timeframe, position.Timeframe, timeframe))
	signal = normalizeRiskSignal(signal, exchange, symbol, signal.Timeframe)
	if !isSignalKeyReady(signal) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	var lifecycleErr error
	signal, _, lifecycleErr = r.applySignalLifecycleLocked(signal, position, hasPosition, ts)
	if lifecycleErr != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, lifecycleErr
	}
	r.observeTrendGuardSignalLocked(signal, evalCtx, ts)

	action := signal.Action
	if action == 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}

	switch action {
	case 8:
		if !r.comboTradeAllowed(signal) {
			err := newRiskOpenRejectError("risk: trade disabled for combo %s", signalComboKey(signal.StrategyTimeframes, signal.Timeframe, signal.ComboKey))
			if rejectAction := classifyOpenRejectSignalAction(err); rejectAction != 0 {
				rejectedSignal, changed, markErr := r.markOpenRejectedSignalLocked(signal, rejectAction, ts)
				if markErr != nil && r.logger != nil {
					r.logger.Warn("mark rejected open signal failed",
						zap.String("exchange", signal.Exchange),
						zap.String("symbol", signal.Symbol),
						zap.Int("reject_action", rejectAction),
						zap.Error(markErr),
					)
				}
				if changed {
					r.observeTrendGuardSignalLocked(rejectedSignal, evalCtx, ts)
				}
			}
			return models.Decision{Action: models.DecisionActionIgnore}, err
		}
		decision, err := r.handleOpenLocked(pair, exchange, symbol, timeframe, signal, data, ts)
		if err != nil {
			if rejectAction := classifyOpenRejectSignalAction(err); rejectAction != 0 {
				rejectedSignal, changed, markErr := r.markOpenRejectedSignalLocked(signal, rejectAction, ts)
				if markErr != nil && r.logger != nil {
					r.logger.Warn("mark rejected open signal failed",
						zap.String("exchange", signal.Exchange),
						zap.String("symbol", signal.Symbol),
						zap.Int("reject_action", rejectAction),
						zap.Error(markErr),
					)
				}
				if changed {
					r.observeTrendGuardSignalLocked(rejectedSignal, evalCtx, ts)
				}
			}
		}
		return decision, err
	case 16:
		return r.handleMoveLocked(pair, exchange, symbol, timeframe, signal, data, ts)
	case 32:
		return r.handlePartialCloseLocked(pair, exchange, symbol, timeframe, signal, data, ts)
	case 64:
		return r.handleCloseAllLocked(pair, exchange, symbol, timeframe, signal, data, ts, "signal_close_all", "signal_close_all")
	default:
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
}

func (r *BackTest) comboTradeAllowed(signal models.Signal) bool {
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

func (r *BackTest) handleOpenLocked(
	pair, exchange, symbol, timeframe string,
	signal models.Signal,
	data models.MarketData,
	ts int64,
) (models.Decision, error) {
	cfg := r.cfg
	side, ok := signalSide(signal.HighSide)
	if !ok {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: unsupported high_side %d", signal.HighSide)
	}
	signalOwnerCombo := signalComboKey(signal.StrategyTimeframes, timeframe, signal.ComboKey)
	exposureKey := common.ExposureKey(symbol)

	for existingPair, existing := range r.positions {
		if existing == nil || existing.RemainingQty <= 0 {
			continue
		}
		if common.ExposureKey(existing.Symbol) != exposureKey {
			continue
		}
		sameMarket := existingPair == pair
		sameOwner := backTestPositionOwnedBy(existing, signal.Strategy, timeframe, signal.StrategyTimeframes, signalOwnerCombo)
		switch {
		case !sameMarket:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk: exposure %s already occupied by %s/%s",
				exposureKey,
				existing.Exchange,
				existing.Symbol,
			)
		case !sameOwner:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk: exposure %s already owned by strategy=%s combo=%s",
				exposureKey,
				strings.TrimSpace(existing.Strategy),
				strings.TrimSpace(existing.ComboKey),
			)
		case existing.Side == side && !cfg.AllowScaleIn:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: scale-in disabled")
		case existing.Side != side:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: exposure %s already has open position", exposureKey)
		default:
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: existing position not openable")
		}
	}
	if rejectReason, shouldReject := r.matchTrendGuardOpenReasonLocked(signal, cfg.TrendGuard); shouldReject {
		return models.Decision{Action: models.DecisionActionIgnore}, newTrendGuardOpenRejectError("risk: trend guard rejected open: %s", rejectReason)
	}
	if cfg.MaxOpenPositions > 0 {
		openCount := r.openPositionCountLocked()
		if openCount >= cfg.MaxOpenPositions {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError(
				"risk: max open positions reached (%d/%d)",
				openCount,
				cfg.MaxOpenPositions,
			)
		}
	}

	accountState := r.ensureAccountStateLocked(exchange, ts)
	if cfg.TradeCooldown.Enabled && accountState.DailyLossLimitUSDT > 0 && accountState.DailyRealizedUSDT >= accountState.DailyLossLimitUSDT {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: trade cooldown active, daily loss %.4f/%.4f usdt", accountState.DailyRealizedUSDT, accountState.DailyLossLimitUSDT)
	}
	if cooldown := r.currentCooldownLocked(exchange, symbol); cooldown.CooldownUntilMS > ts {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: symbol cooldown until %s", time.UnixMilli(cooldown.CooldownUntilMS).Format("2006-01-02 15:04:05"))
	}

	signalOrderType, err := normalizeSignalOrderType(signal.OrderType)
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: %w", err)
	}
	entryPrice := resolveOpenPrice(signal, data)
	if signalOrderType == models.OrderTypeMarket {
		entryPrice = marketPrice(data, 0)
	}
	if signalOrderType == models.OrderTypeLimit {
		entryPrice = signal.Entry
		if entryPrice <= 0 {
			return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: limit order requires entry price")
		}
	}
	if entryPrice <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: invalid entry price")
	}
	available := r.availableUSDTLocked(exchange)
	plan, err := evaluateRiskOpenPlan(cfg, signal, signalOrderType, side, entryPrice, available, accountState, func(perTradeUSDT float64, leverage int, entryPrice float64) (float64, error) {
		qty := perTradeUSDT * float64(leverage) / entryPrice
		if qty <= 0 {
			return 0, fmt.Errorf("invalid quantity")
		}
		return qty, nil
	})
	if err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, newRiskOpenRejectError("risk: %w", err)
	}
	signalMeta := buildBackTestOpenSignalMeta(signal, timeframe)
	strategyName := strings.TrimSpace(firstNonEmpty(signalMeta.StrategyName, signal.Strategy))
	strategyVersion := strings.TrimSpace(firstNonEmpty(signalMeta.StrategyVersion, signal.StrategyVersion))
	if strategyName == "" {
		strategyName = strings.TrimSpace(signal.Strategy)
	}
	if strategyVersion == "" {
		strategyVersion = strings.TrimSpace(signal.StrategyVersion)
	}

	pos := &backTestPosition{
		Exchange:        exchange,
		Symbol:          symbol,
		Timeframe:       timeframe,
		ComboKey:        strings.TrimSpace(signalMeta.ComboKey),
		GroupID:         strings.TrimSpace(signalMeta.GroupID),
		Side:            side,
		Strategy:        strategyName,
		StrategyVersion: strategyVersion,
		StrategyTimeframes: append([]string(nil),
			signalMeta.StrategyTimeframes...,
		),
		StrategyIndicators: cloneStrategyIndicators(signalMeta.StrategyIndicators),
		Margin:             plan.MarginUSDT,
		Leverage:           float64(plan.Leverage),
		EntryPrice:         plan.EntryPrice,
		EntryQuantity:      plan.Size,
		RemainingQty:       plan.Size,
		EntryTS:            ts,
		TakeProfitPrice:    plan.TakeProfitPrice,
		StopLossPrice:      plan.StopLossPrice,
	}
	r.positions[pair] = pos
	_ = r.syncSignalCacheFromPositionLocked(signal, pos, ts)
	r.syncTrendGuardPositionsLocked(ts)
	r.recordPositionEventLocked(pos, ts, "open", plan.EntryPrice, plan.Size, pos.RemainingQty, "", "filled", "")
	r.persistPositionSnapshotsLocked(exchange, nil)
	r.persistAccountStateLocked(exchange, ts)

	action := models.DecisionActionOpenLong
	if side == positionSideShort {
		action = models.DecisionActionOpenShort
	}
	return models.Decision{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		EventTS:            ts,
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
		ClientOrderID:      newRiskClientOrderID(exchange, symbol),
	}, nil
}

func (r *BackTest) handleMoveLocked(
	pair, exchange, symbol, timeframe string,
	signal models.Signal,
	data models.MarketData,
	ts int64,
) (models.Decision, error) {
	pos := r.positions[pair]
	if pos == nil {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: no open position for update")
	}
	if !isManualTradeSignal(signal) && !backTestPositionOwnedBy(pos, signal.Strategy, timeframe, signal.StrategyTimeframes, signal.ComboKey) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	cfg := r.cfg
	price := resolveClosePrice(signal, data, pos.EntryPrice)
	if price <= 0 {
		price = pos.EntryPrice
	}

	tp := resolveUpdateTakeProfit(cfg.TP, signal.TP, pos.TakeProfitPrice)
	if tp > 0 && cfg.TP.OnlyRaiseOnUpdate && pos.TakeProfitPrice > 0 && !isTPMoveAllowed(pos.Side, pos.TakeProfitPrice, tp) {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: TP can only move with trend")
	}

	sl := signal.SL
	if sl <= 0 {
		if cfg.SL.RequireSignal {
			return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: SL required")
		}
		sl = pos.StopLossPrice
	}
	if sl <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: invalid SL")
	}
	if cfg.SL.OnlyRaiseOnUpdate && pos.StopLossPrice > 0 && !isSLMoveAllowed(pos.Side, pos.StopLossPrice, sl) {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: SL can only move with trend")
	}
	if err := validateTPSLAgainstPrice(pos.Side, price, tp, sl); err != nil {
		return models.Decision{Action: models.DecisionActionIgnore}, err
	}

	if !math.IsNaN(tp) && tp >= 0 && !floatAlmostEqual(pos.TakeProfitPrice, tp) {
		pos.TakeProfitPrice = tp
		r.recordPositionEventLocked(pos, ts, "move_tp", price, 0, pos.RemainingQty, "", "updated", "")
	}
	if !math.IsNaN(sl) && sl > 0 && !floatAlmostEqual(pos.StopLossPrice, sl) {
		pos.StopLossPrice = sl
		r.recordPositionEventLocked(pos, ts, "move_sl", price, 0, pos.RemainingQty, "", "updated", "")
	}
	_ = r.syncSignalCacheFromPositionLocked(signal, pos, ts)
	r.persistPositionSnapshotsLocked(exchange, nil)

	return models.Decision{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		EventTS:            ts,
		Action:             models.DecisionActionUpdate,
		Strategy:           signal.Strategy,
		PositionSide:       pos.Side,
		MarginMode:         models.MarginModeIsolated,
		Size:               pos.RemainingQty,
		LeverageMultiplier: pos.Leverage,
		Price:              price,
		StopLossPrice:      pos.StopLossPrice,
		TakeProfitPrice:    pos.TakeProfitPrice,
		ClientOrderID:      newRiskClientOrderID(exchange, symbol),
	}, nil
}

func (r *BackTest) handlePartialCloseLocked(
	pair, exchange, symbol, timeframe string,
	signal models.Signal,
	data models.MarketData,
	ts int64,
) (models.Decision, error) {
	pos := r.positions[pair]
	if pos == nil {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: no open position for close")
	}
	if !isManualTradeSignal(signal) && !backTestPositionOwnedBy(pos, signal.Strategy, timeframe, signal.StrategyTimeframes, signal.ComboKey) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	closeQty := pos.EntryQuantity * 0.8
	if closeQty > pos.RemainingQty {
		closeQty = pos.RemainingQty
	}
	if closeQty <= 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: invalid close quantity")
	}
	closePrice := resolveClosePrice(signal, data, pos.EntryPrice)
	r.closePositionLocked(pair, pos, closePrice, ts, closeQty, "signal_partial_close", "partial_close", false)
	if pos.RemainingQty > 1e-12 {
		cfg := r.cfg
		tp := resolveUpdateTakeProfit(cfg.TP, signal.TP, pos.TakeProfitPrice)
		sl := signal.SL
		if sl <= 0 {
			sl = pos.StopLossPrice
		}
		if err := validateTPSLAgainstPrice(pos.Side, closePrice, tp, sl); err == nil {
			if !math.IsNaN(tp) && tp >= 0 && !floatAlmostEqual(pos.TakeProfitPrice, tp) {
				pos.TakeProfitPrice = tp
				r.recordPositionEventLocked(pos, ts, "move_tp", closePrice, 0, pos.RemainingQty, "", "updated", "")
			}
			if !math.IsNaN(sl) && sl > 0 && !floatAlmostEqual(pos.StopLossPrice, sl) {
				pos.StopLossPrice = sl
				r.recordPositionEventLocked(pos, ts, "move_sl", closePrice, 0, pos.RemainingQty, "", "updated", "")
			}
		}
		_ = r.syncSignalCacheFromPositionLocked(signal, pos, ts)
		r.persistPositionSnapshotsLocked(exchange, nil)
	}
	return models.Decision{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		EventTS:            ts,
		Action:             models.DecisionActionClose,
		CloseReason:        "signal_partial_close",
		Strategy:           signal.Strategy,
		PositionSide:       pos.Side,
		MarginMode:         models.MarginModeIsolated,
		Size:               closeQty,
		LeverageMultiplier: pos.Leverage,
		Price:              closePrice,
		StopLossPrice:      pos.StopLossPrice,
		TakeProfitPrice:    pos.TakeProfitPrice,
		ClientOrderID:      newRiskClientOrderID(exchange, symbol),
	}, nil
}

func (r *BackTest) handleCloseAllLocked(
	pair, exchange, symbol, timeframe string,
	signal models.Signal,
	data models.MarketData,
	ts int64,
	closeReason string,
	eventReason string,
) (models.Decision, error) {
	pos := r.positions[pair]
	if pos == nil {
		return models.Decision{Action: models.DecisionActionIgnore}, fmt.Errorf("risk: no open position for close")
	}
	if !isManualTradeSignal(signal) && !backTestPositionOwnedBy(pos, signal.Strategy, timeframe, signal.StrategyTimeframes, signal.ComboKey) {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	closePrice := resolveClosePrice(signal, data, pos.EntryPrice)
	r.closePositionLocked(
		pair,
		pos,
		closePrice,
		ts,
		pos.RemainingQty,
		closeReason,
		eventReason,
		shouldRemoveSignalAfterTrendEndedClose(signal),
	)
	return models.Decision{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		EventTS:            ts,
		Action:             models.DecisionActionClose,
		CloseReason:        closeReason,
		Strategy:           signal.Strategy,
		PositionSide:       pos.Side,
		MarginMode:         models.MarginModeIsolated,
		Size:               pos.EntryQuantity,
		LeverageMultiplier: pos.Leverage,
		Price:              closePrice,
		ClientOrderID:      newRiskClientOrderID(exchange, symbol),
	}, nil
}

func (r *BackTest) closePositionLocked(
	pair string,
	pos *backTestPosition,
	price float64,
	ts int64,
	closeQty float64,
	closeReason string,
	eventReason string,
	removeSignalAfterClose bool,
) {
	if pos == nil {
		return
	}
	if price <= 0 {
		price = pos.EntryPrice
	}
	if closeQty <= 0 {
		return
	}
	if closeQty > pos.RemainingQty {
		closeQty = pos.RemainingQty
	}

	pnl := positionPnL(pos.Side, pos.EntryPrice, price, closeQty)
	pos.RealizedPnL += pnl
	pos.RemainingQty -= closeQty
	pos.ClosedQty += closeQty
	pos.ExitNotional += closeQty * price
	r.recordPositionEventLocked(pos, ts, "close", price, closeQty, pos.RemainingQty, closeReason, "filled", eventReason)

	if pos.RemainingQty > 1e-12 {
		r.syncTrendGuardPositionsLocked(ts)
		r.persistPositionSnapshotsLocked(normalizeExchange(pos.Exchange), nil)
		return
	}

	exitPrice := price
	if pos.ClosedQty > 0 {
		exitPrice = pos.ExitNotional / pos.ClosedQty
	}
	r.nextTradeID++
	trade := BackTestTrade{
		TradeID:         r.nextTradeID,
		Exchange:        pos.Exchange,
		Symbol:          pos.Symbol,
		Timeframe:       pos.Timeframe,
		GroupID:         pos.GroupID,
		Side:            pos.Side,
		Strategy:        pos.Strategy,
		StrategyVersion: pos.StrategyVersion,
		Margin:          pos.Margin,
		Leverage:        pos.Leverage,
		EntryPrice:      pos.EntryPrice,
		EntryQuantity:   pos.EntryQuantity,
		EntryTS:         pos.EntryTS,
		ExitPrice:       exitPrice,
		ExitQuantity:    pos.ClosedQty,
		ExitTS:          ts,
		Profit:          pos.RealizedPnL,
		Status:          models.PositionStatusClosed,
		CloseReason:     closeReason,
	}
	if pos.Margin > 0 {
		trade.ProfitRate = trade.Profit / pos.Margin
		trade.MaxDrawdownRate = pos.MaxFloatingLossAmount / pos.Margin
		trade.MaxProfitRate = pos.MaxFloatingProfitAmount / pos.Margin
	}
	if removeSignalAfterClose {
		if err := r.removeSignalAfterTrendEndedCloseLocked(pos, ts); err != nil {
			r.logger.Warn("risk back-test remove trend-ended signal failed",
				zap.String("exchange", pos.Exchange),
				zap.String("symbol", pos.Symbol),
				zap.String("timeframe", pos.Timeframe),
				zap.String("strategy", pos.Strategy),
				zap.Error(err),
			)
		}
	} else {
		if err := r.markSignalClosedByPnLLocked(pos, trade.Profit, ts); err != nil {
			r.logger.Warn("risk back-test mark signal closed failed",
				zap.String("exchange", pos.Exchange),
				zap.String("symbol", pos.Symbol),
				zap.String("timeframe", pos.Timeframe),
				zap.String("strategy", pos.Strategy),
				zap.Float64("profit", trade.Profit),
				zap.Error(err),
			)
		}
	}
	r.trades = append(r.trades, trade)
	delete(r.positions, pair)
	r.syncTrendGuardPositionsLocked(ts)

	exchange := normalizeExchange(pos.Exchange)
	symbol := pos.Symbol
	r.applyCloseStateLocked(exchange, symbol, trade.Profit, ts, closeReason)

	closed := r.buildClosedSnapshotLocked(pos, trade, closeReason, eventReason, ts)
	r.persistPositionSnapshotsLocked(exchange, []models.RiskClosedPosition{closed})
}

func (r *BackTest) HasOpenPosition(exchange, symbol string) (bool, error) {
	if r == nil {
		return false, nil
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return false, nil
	}
	targetSymbol := canonicalSymbol(symbol)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, pos := range r.positions {
		if pos == nil || pos.RemainingQty <= 0 {
			continue
		}
		if exchange != "" && normalizeExchange(pos.Exchange) != exchange {
			continue
		}
		if canonicalSymbol(pos.Symbol) != targetSymbol {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (r *BackTest) ListOpenPositions(exchange, symbol, timeframe string) ([]models.Position, error) {
	if r == nil {
		return nil, nil
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)

	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listOpenPositionsFromMemoryLocked(exchange, symbol, timeframe), nil
}

func (r *BackTest) ListAllOpenPositions() ([]models.Position, error) {
	if r == nil {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listOpenPositionsFromMemoryLocked("", "", ""), nil
}

func (r *BackTest) listOpenPositionsFromMemoryLocked(exchange, symbol, timeframe string) []models.Position {
	out := make([]models.Position, 0, len(r.positions))
	for pair, pos := range r.positions {
		if pos == nil {
			continue
		}
		if exchange != "" && normalizeExchange(pos.Exchange) != exchange {
			continue
		}
		if symbol != "" && canonicalSymbol(pos.Symbol) != canonicalSymbol(symbol) {
			continue
		}
		if timeframe != "" && pos.Timeframe != "" && pos.Timeframe != timeframe {
			continue
		}
		mark := r.marks[pair]
		current := mark.Price
		if current <= 0 {
			current = pos.EntryPrice
		}
		unrealized := positionPnL(pos.Side, pos.EntryPrice, current, pos.RemainingQty)
		unrealizedRate := 0.0
		if pos.Margin > 0 {
			unrealizedRate = unrealized / pos.Margin
		}
		out = append(out, models.Position{
			Exchange:               pos.Exchange,
			Symbol:                 pos.Symbol,
			Timeframe:              pos.Timeframe,
			ComboKey:               pos.ComboKey,
			GroupID:                pos.GroupID,
			PositionSide:           pos.Side,
			MarginMode:             models.MarginModeIsolated,
			LeverageMultiplier:     pos.Leverage,
			MarginAmount:           pos.Margin,
			EntryPrice:             pos.EntryPrice,
			EntryQuantity:          pos.EntryQuantity,
			EntryValue:             pos.EntryPrice * pos.EntryQuantity,
			EntryTime:              formatBackTestTimestampMS(pos.EntryTS),
			TakeProfitPrice:        pos.TakeProfitPrice,
			StopLossPrice:          pos.StopLossPrice,
			CurrentPrice:           current,
			UnrealizedProfitAmount: unrealized,
			UnrealizedProfitRate:   unrealizedRate,
			Status:                 models.PositionStatusOpen,
			StrategyName:           pos.Strategy,
			StrategyVersion:        pos.StrategyVersion,
			StrategyTimeframes:     append([]string(nil), pos.StrategyTimeframes...),
			UpdatedTime:            formatBackTestTimestampMS(mark.TS),
		})
	}
	return out
}

func (r *BackTest) currentPositionForPairLocked(pair string) (models.Position, bool) {
	if r == nil {
		return models.Position{}, false
	}
	pos := r.positions[pair]
	if pos == nil || pos.RemainingQty <= 0 {
		return models.Position{}, false
	}
	mark := r.marks[pair]
	currentPrice := mark.Price
	if currentPrice <= 0 {
		currentPrice = pos.EntryPrice
	}
	unrealized := positionPnL(pos.Side, pos.EntryPrice, currentPrice, pos.RemainingQty)
	unrealizedRate := 0.0
	if pos.Margin > 0 {
		unrealizedRate = unrealized / pos.Margin
	}
	return models.Position{
		Exchange:               pos.Exchange,
		Symbol:                 pos.Symbol,
		Timeframe:              pos.Timeframe,
		ComboKey:               pos.ComboKey,
		GroupID:                pos.GroupID,
		PositionSide:           pos.Side,
		MarginMode:             models.MarginModeIsolated,
		LeverageMultiplier:     pos.Leverage,
		MarginAmount:           pos.Margin,
		EntryPrice:             pos.EntryPrice,
		EntryQuantity:          pos.RemainingQty,
		EntryValue:             pos.EntryPrice * pos.EntryQuantity,
		EntryTime:              formatBackTestTimestampMS(pos.EntryTS),
		TakeProfitPrice:        pos.TakeProfitPrice,
		StopLossPrice:          pos.StopLossPrice,
		CurrentPrice:           currentPrice,
		UnrealizedProfitAmount: unrealized,
		UnrealizedProfitRate:   unrealizedRate,
		Status:                 models.PositionStatusOpen,
		StrategyName:           pos.Strategy,
		StrategyVersion:        pos.StrategyVersion,
		StrategyTimeframes:     append([]string(nil), pos.StrategyTimeframes...),
		UpdatedTime:            formatBackTestTimestampMS(mark.TS),
	}, true
}

func (r *BackTest) ListHistoryPositions(exchange, symbol string) ([]models.Position, error) {
	if r == nil {
		return nil, nil
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	canonical := canonicalSymbol(symbol)

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.store == nil {
		out := make([]models.Position, 0, len(r.trades))
		for _, trade := range r.trades {
			if exchange != "" && normalizeExchange(trade.Exchange) != exchange {
				continue
			}
			if canonical != "" && canonicalSymbol(trade.Symbol) != canonical {
				continue
			}
			out = append(out, tradeToHistoryPosition(trade))
		}
		return out, nil
	}

	rows, err := r.store.ListRiskHistorySnapshots(r.mode, exchange)
	if err != nil {
		return nil, err
	}
	out := make([]models.Position, 0, len(rows))
	for _, row := range rows {
		meta := parseBackTestOpenMeta(row.OpenRowJSON)
		closeMeta := parseBackTestCloseMeta(row.CloseRowJSON)
		if !r.matchRunID(meta.RunID) && !r.matchRunID(closeMeta.RunID) {
			continue
		}
		item := historySnapshotToPosition(row, meta)
		if canonical != "" && canonicalSymbol(item.Symbol) != canonical {
			continue
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].ExitTime > out[j].ExitTime
	})
	return out, nil
}

func (r *BackTest) GetAccountFunds(exchange string) (models.RiskAccountFunds, error) {
	if r == nil {
		return models.RiskAccountFunds{}, fmt.Errorf("risk back-test: nil evaluator")
	}
	exchange = normalizeExchange(exchange)
	if exchange == "" {
		exchange = r.reportMode()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	closedProfitRate := r.totalClosedProfitRateLocked()
	floatingProfitRate := r.totalUnrealizedRateLocked(exchange)

	return models.RiskAccountFunds{
		Exchange:           exchange,
		Currency:           "RATE",
		ClosedProfitRate:   closedProfitRate,
		FloatingProfitRate: floatingProfitRate,
		TotalProfitRate:    closedProfitRate + floatingProfitRate,
		UpdatedAtMS:        time.Now().UnixMilli(),
	}, nil
}

func (r *BackTest) DelegatePositionStrategy(exchange, symbol, side, strategy string, timeframes []string) error {
	if r == nil {
		return fmt.Errorf("risk back-test: nil evaluator")
	}
	exchange = normalizeExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	side = normalizePositionSide(side, 0)
	strategy = strings.TrimSpace(strategy)
	primary, normalizedTimeframes, comboKey := common.NormalizeStrategyIdentity("", timeframes, "")
	if exchange == "" || symbol == "" || side == "" {
		return fmt.Errorf("risk back-test: delegate requires exchange, symbol and side")
	}
	if strategy == "" || len(normalizedTimeframes) == 0 {
		return fmt.Errorf("risk back-test: delegate requires strategy and trade-enabled timeframes")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	updated := false
	for key, item := range r.positions {
		if item == nil || item.RemainingQty <= 0 {
			continue
		}
		if normalizeExchange(item.Exchange) != exchange {
			continue
		}
		if canonicalSymbol(item.Symbol) != canonicalSymbol(symbol) {
			continue
		}
		if normalizePositionSide(item.Side, 0) != side {
			continue
		}
		item.Timeframe = primary
		item.Strategy = strategy
		item.StrategyVersion = ""
		item.StrategyTimeframes = append([]string(nil), normalizedTimeframes...)
		item.ComboKey = comboKey
		r.positions[key] = item
		updated = true
	}
	if !updated {
		return fmt.Errorf("risk back-test: open position not found")
	}
	r.persistPositionSnapshotsLocked(exchange, nil)
	return nil
}

func formatBackTestTimestampMS(ts int64) string {
	ts = normalizeTimestampMS(ts)
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func (r *BackTest) Finalize() BackTestReport {
	if r == nil {
		return BackTestReport{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reportReady {
		return cloneReport(r.report)
	}

	if r.store != nil {
		report, err := r.buildReportFromStoreLocked()
		if err != nil {
			r.logger.Error("risk back-test build report from store failed", zap.Error(err))
			report = r.buildReportFromMemoryLocked()
		}
		r.report = report
		r.reportReady = true
		return cloneReport(r.report)
	}

	r.report = r.buildReportFromMemoryLocked()
	r.reportReady = true
	return cloneReport(r.report)
}

func (r *BackTest) Report() (BackTestReport, bool) {
	if r == nil {
		return BackTestReport{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.reportReady {
		return BackTestReport{}, false
	}
	return cloneReport(r.report), true
}

func (r *BackTest) buildReportFromMemoryLocked() BackTestReport {
	open := r.listOpenPositionsFromMemoryLocked("", "", "")
	report := buildReport(false, r.maxCooldownUntilLocked(), r.trades, r.positionEvents)
	report.OpenPositions = open
	return report
}

func (r *BackTest) buildReportFromStoreLocked() (BackTestReport, error) {
	historyRows, err := r.store.ListRiskHistorySnapshots(r.mode, "")
	if err != nil {
		return BackTestReport{}, err
	}
	openRows, err := r.store.ListRiskOpenPositions(r.mode, "")
	if err != nil {
		return BackTestReport{}, err
	}
	orders, err := r.store.ListExecutionOrders(r.reportMode(), r.singletonUUID)
	if err != nil {
		return BackTestReport{}, err
	}

	trades := buildBackTestTradesFromHistoryRows(historyRows, r.runID)
	orderEvents := buildBackTestPositionEventsFromOrders(orders, r.runID, r.singletonUUID)
	closeEvents := buildBackTestCloseEventsFromHistoryRows(historyRows, r.runID)
	positionEvents := mergeBackTestPositionEvents(orderEvents, closeEvents)
	openPositions := buildBackTestOpenPositionsFromRows(openRows, r.runID)

	report := buildReport(false, r.maxCooldownUntilLocked(), trades, positionEvents)
	report.OpenPositions = openPositions
	return report, nil
}

func (r *BackTest) recordPositionEventLocked(
	pos *backTestPosition,
	ts int64,
	action string,
	price float64,
	qty float64,
	remainingQty float64,
	closeReason string,
	result string,
	reason string,
) {
	r.nextPositionEvent++
	r.positionEvents = append(r.positionEvents, BackTestPositionEvent{
		EventID:           r.nextPositionEvent,
		EventTS:           ts,
		KlineTS:           ts,
		Exchange:          pos.Exchange,
		Symbol:            pos.Symbol,
		Timeframe:         pos.Timeframe,
		Side:              pos.Side,
		Strategy:          pos.Strategy,
		StrategyVersion:   pos.StrategyVersion,
		Action:            action,
		Price:             price,
		Quantity:          qty,
		RemainingQuantity: remainingQty,
		Margin:            pos.Margin,
		Leverage:          pos.Leverage,
		TakeProfitPrice:   pos.TakeProfitPrice,
		StopLossPrice:     pos.StopLossPrice,
		CloseReason:       closeReason,
		Result:            result,
		Reason:            reason,
	})
}

func (r *BackTest) updateFloatingPnLLocked(pos *backTestPosition, mark backTestMark) {
	if pos == nil {
		return
	}
	high := firstPositive(mark.High, mark.Price, pos.EntryPrice)
	low := firstPositive(mark.Low, mark.Price, pos.EntryPrice)
	if low <= 0 {
		low = high
	}
	switch pos.Side {
	case positionSideLong:
		loss := positionPnL(pos.Side, pos.EntryPrice, low, pos.RemainingQty)
		if loss < 0 {
			value := -loss
			if value > pos.MaxFloatingLossAmount {
				pos.MaxFloatingLossAmount = value
			}
		}
		profit := positionPnL(pos.Side, pos.EntryPrice, high, pos.RemainingQty)
		if profit > pos.MaxFloatingProfitAmount {
			pos.MaxFloatingProfitAmount = profit
		}
	case positionSideShort:
		loss := positionPnL(pos.Side, pos.EntryPrice, high, pos.RemainingQty)
		if loss < 0 {
			value := -loss
			if value > pos.MaxFloatingLossAmount {
				pos.MaxFloatingLossAmount = value
			}
		}
		profit := positionPnL(pos.Side, pos.EntryPrice, low, pos.RemainingQty)
		if profit > pos.MaxFloatingProfitAmount {
			pos.MaxFloatingProfitAmount = profit
		}
	}
}

func (r *BackTest) persistPositionSnapshotsLocked(exchange string, closed []models.RiskClosedPosition) {
	if r.store == nil {
		return
	}
	open := r.buildOpenSnapshotsLocked(exchange)
	if err := r.store.SyncRiskPositions(r.mode, exchange, open, closed); err != nil {
		r.logger.Error("risk back-test sync positions failed",
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		return
	}
	if r.historyArchive != nil {
		if err := r.historyArchive.SyncOpenPositions(exchange, open); err != nil {
			r.logger.Error("risk back-test history archive sync open positions failed",
				zap.String("exchange", exchange),
				zap.Error(err),
			)
		}
	}
}

func (r *BackTest) buildOpenSnapshotsLocked(exchange string) []models.RiskOpenPosition {
	nowMS := time.Now().UnixMilli()
	out := make([]models.RiskOpenPosition, 0, len(r.positions))
	for pair, pos := range r.positions {
		if pos == nil || normalizeExchange(pos.Exchange) != exchange {
			continue
		}
		mark := r.marks[pair]
		currentPrice := firstPositive(mark.Price, pos.EntryPrice)
		upl := positionPnL(pos.Side, pos.EntryPrice, currentPrice, pos.RemainingQty)
		uplRate := 0.0
		if pos.Margin > 0 {
			uplRate = upl / pos.Margin
		}
		qty := pos.RemainingQty
		if pos.Side == positionSideShort {
			qty = -qty
		}
		meta := backTestOpenMeta{
			SingletonID:     r.singletonID,
			RunID:           r.runID,
			Timeframe:       pos.Timeframe,
			GroupID:         pos.GroupID,
			Strategy:        pos.Strategy,
			StrategyVersion: pos.StrategyVersion,
			EntryTS:         pos.EntryTS,
		}
		rowJSON := models.MarshalPositionRowEnvelopeWithRuntime(meta, backTestStrategyContextMeta(pos), models.PositionRuntimeMeta{
			RunID:       r.runID,
			SingletonID: r.singletonID,
		})
		if strings.TrimSpace(rowJSON) == "" {
			rowJSON = marshalJSONOrEmpty(meta)
		}
		out = append(out, models.RiskOpenPosition{
			Mode:                    r.mode,
			Exchange:                exchange,
			Symbol:                  pos.Symbol,
			InstID:                  backTestInstID(pos.Symbol),
			Pos:                     formatNumericText(qty),
			PosSide:                 pos.Side,
			MgnMode:                 models.MarginModeIsolated,
			Margin:                  formatNumericText(pos.Margin),
			Lever:                   formatNumericText(pos.Leverage),
			AvgPx:                   formatNumericText(pos.EntryPrice),
			Upl:                     formatNumericText(upl),
			UplRatio:                formatNumericText(uplRate),
			NotionalUSD:             formatNumericText(math.Abs(currentPrice * pos.RemainingQty)),
			MarkPx:                  formatNumericText(currentPrice),
			LiqPx:                   "0",
			TPTriggerPx:             formatNumericText(pos.TakeProfitPrice),
			SLTriggerPx:             formatNumericText(pos.StopLossPrice),
			OpenTimeMS:              normalizeTimestampMS(pos.EntryTS),
			UpdateTimeMS:            normalizeTimestampMS(mark.TS),
			RowJSON:                 rowJSON,
			MaxFloatingLossAmount:   pos.MaxFloatingLossAmount,
			MaxFloatingProfitAmount: pos.MaxFloatingProfitAmount,
			UpdatedAtMS:             nowMS,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].PosSide < out[j].PosSide
	})
	return out
}

func (r *BackTest) buildClosedSnapshotLocked(
	pos *backTestPosition,
	trade BackTestTrade,
	closeReason string,
	eventReason string,
	ts int64,
) models.RiskClosedPosition {
	meta := backTestCloseMeta{
		SingletonID:     r.singletonID,
		RunID:           r.runID,
		CloseReason:     closeReason,
		EventReason:     eventReason,
		Strategy:        pos.Strategy,
		StrategyVersion: pos.StrategyVersion,
	}
	closeRowJSON := models.MarshalPositionRowEnvelopeWithRuntime(meta, backTestStrategyContextMeta(pos), models.PositionRuntimeMeta{
		RunID:       r.runID,
		SingletonID: r.singletonID,
	})
	if strings.TrimSpace(closeRowJSON) == "" {
		closeRowJSON = marshalJSONOrEmpty(meta)
	}
	return models.RiskClosedPosition{
		Mode:         r.mode,
		Exchange:     normalizeExchange(pos.Exchange),
		Symbol:       pos.Symbol,
		InstID:       backTestInstID(pos.Symbol),
		PosSide:      pos.Side,
		MgnMode:      models.MarginModeIsolated,
		Lever:        formatNumericText(pos.Leverage),
		OpenAvgPx:    formatNumericText(pos.EntryPrice),
		CloseAvgPx:   formatNumericText(trade.ExitPrice),
		RealizedPnl:  formatNumericText(trade.Profit),
		PnlRatio:     formatNumericText(trade.ProfitRate),
		Fee:          "0",
		FundingFee:   "0",
		OpenTimeMS:   normalizeTimestampMS(pos.EntryTS),
		CloseTimeMS:  normalizeTimestampMS(ts),
		State:        models.PositionStatusClosed,
		CloseRowJSON: closeRowJSON,
		UpdatedAtMS:  time.Now().UnixMilli(),
	}
}

func (r *BackTest) ensureAccountStateLocked(exchange string, ts int64) models.RiskAccountState {
	exchange = normalizeExchange(exchange)
	state := r.accountStates[exchange]
	state.Mode = r.mode
	tradeDate := tradeDateForTimestampMS(ts)
	if tradeDate == "" {
		tradeDate = time.Now().Format("2006-01-02")
	}
	if state.Exchange == "" {
		state.Exchange = exchange
	}
	if state.TradeDate != tradeDate {
		state.TradeDate = tradeDate
		state.DailyRealizedUSDT = 0
		state.DailyClosedProfitUSDT = 0
	}
	state.TotalUSDT = simulationAvailableBudget
	state.FundingUSDT = 0
	state.TradingUSDT = simulationAvailableBudget
	state.PerTradeUSDT = simulationMarginBasis
	state.DailyLossLimitUSDT = simulationMarginBasis * r.cfg.TradeCooldown.LossRatioOfPerTrade
	state.UpdatedAtMS = time.Now().UnixMilli()
	r.accountStates[exchange] = state
	return state
}

func (r *BackTest) persistAccountStateLocked(exchange string, ts int64) {
	if r.store == nil {
		return
	}
	state := r.ensureAccountStateLocked(exchange, ts)
	if err := r.store.UpsertRiskAccountState(state); err != nil {
		r.logger.Error("risk back-test persist account state failed",
			zap.String("exchange", exchange),
			zap.Error(err),
		)
	}
}

func (r *BackTest) applyCloseStateLocked(exchange, symbol string, pnl float64, ts int64, closeReason string) {
	state := r.ensureAccountStateLocked(exchange, ts)
	if pnl < 0 {
		state.DailyRealizedUSDT += -pnl
	}
	state.DailyClosedProfitUSDT += pnl
	state.TotalUSDT = simulationAvailableBudget
	state.TradingUSDT = simulationAvailableBudget
	state.PerTradeUSDT = simulationMarginBasis
	state.DailyLossLimitUSDT = simulationMarginBasis * r.cfg.TradeCooldown.LossRatioOfPerTrade
	state.UpdatedAtMS = time.Now().UnixMilli()
	r.accountStates[exchange] = state
	if r.store != nil {
		if err := r.store.UpsertRiskAccountState(state); err != nil {
			r.logger.Error("risk back-test persist account state failed",
				zap.String("exchange", exchange),
				zap.Error(err),
			)
		}
	}

	key := livePairKey(exchange, symbol)
	cooldown := r.cooldowns[key]
	cooldown.Mode = r.mode
	cooldown.Exchange = exchange
	cooldown.Symbol = symbol
	if closeReason == "stop_loss" {
		cooldown = applyStopLossCooldown(r.cfg, cooldown, ts)
	} else {
		cooldown.ConsecutiveStopLoss = 0
		cooldown.WindowStartAtMS = 0
		cooldown.LastStopLossAtMS = 0
	}
	cooldown.LastProcessedCloseMS = ts
	cooldown.UpdatedAtMS = time.Now().UnixMilli()
	r.cooldowns[key] = cooldown
	if r.store != nil {
		if err := r.store.UpsertRiskSymbolCooldownState(cooldown); err != nil {
			r.logger.Error("risk back-test persist cooldown failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(err),
			)
		}
	}
}

func (r *BackTest) currentCooldownLocked(exchange, symbol string) models.RiskSymbolCooldownState {
	key := livePairKey(exchange, symbol)
	state := r.cooldowns[key]
	state.Mode = r.mode
	state.Exchange = normalizeExchange(exchange)
	state.Symbol = symbol
	return state
}

func (r *BackTest) availableUSDTLocked(exchange string) float64 {
	state := r.ensureAccountStateLocked(exchange, time.Now().UnixMilli())
	used := 0.0
	for _, pos := range r.positions {
		if pos == nil || normalizeExchange(pos.Exchange) != normalizeExchange(exchange) {
			continue
		}
		used += pos.Margin
	}
	available := state.TradingUSDT - used
	if available < 0 {
		return 0
	}
	return available
}

func (r *BackTest) openPositionCountLocked() int {
	if r == nil {
		return 0
	}
	count := 0
	for _, pos := range r.positions {
		if pos != nil && pos.RemainingQty > 1e-12 {
			count++
		}
	}
	return count
}

func (r *BackTest) totalUnrealizedLocked(exchange string) float64 {
	out := 0.0
	for pair, pos := range r.positions {
		if pos == nil {
			continue
		}
		if exchange != "" && normalizeExchange(pos.Exchange) != normalizeExchange(exchange) {
			continue
		}
		mark := r.marks[pair]
		price := firstPositive(mark.Price, pos.EntryPrice)
		out += positionPnL(pos.Side, pos.EntryPrice, price, pos.RemainingQty)
	}
	return out
}

func (r *BackTest) maxCooldownUntilLocked() int64 {
	maxTS := int64(0)
	for _, state := range r.cooldowns {
		if state.CooldownUntilMS > maxTS {
			maxTS = state.CooldownUntilMS
		}
	}
	return maxTS
}

func (r *BackTest) matchRunID(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	return value == r.runID
}

func (r *BackTest) loadRiskConfig() error {
	cfg := defaultRiskConfig()
	if r.store == nil {
		r.cfg = cfg
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
		r.cfg = cfg
		return nil
	}
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return fmt.Errorf("risk back-test: invalid risk config json: %w", err)
	}
	normalizeRiskConfig(&cfg)
	r.cfg = cfg
	return nil
}

func (r *BackTest) restoreTrendGuardLocked() error {
	if r == nil || r.trendGuard == nil || r.store == nil {
		return nil
	}
	groups, err := r.store.ListRiskTrendGroups(r.mode)
	if err != nil {
		return err
	}
	candidates, err := r.store.ListRiskTrendGroupCandidates(r.mode)
	if err != nil {
		return err
	}
	r.trendGuard.restore(groups, candidates)
	r.trendGuard.syncPositions(r.snapshotPositionsLocked(), time.Now().UnixMilli())
	return nil
}

func (r *BackTest) snapshotPositionsLocked() map[string]models.Position {
	if r == nil {
		return nil
	}
	out := make(map[string]models.Position, len(r.positions))
	for key := range r.positions {
		if pos, ok := r.currentPositionForPairLocked(key); ok {
			out[key] = pos
		}
	}
	return out
}

func (r *BackTest) observeTrendGuardSignalLocked(signal models.Signal, evalCtx models.RiskEvalContext, eventTS int64) {
	if r == nil || r.trendGuard == nil {
		return
	}
	if models.IsEmptySignal(signal) {
		return
	}
	if !trendGuardGroupedEnabled(r.cfg.TrendGuard) {
		return
	}
	if eventTS <= 0 {
		eventTS = normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.observeSignal(r.cfg.TrendGuard, signal, evalCtx, eventTS)
}

func (r *BackTest) RefreshTrendGuardCandidate(signal models.Signal, accountState any) error {
	if r == nil || r.trendGuard == nil {
		return nil
	}
	evalCtx, _ := accountState.(models.RiskEvalContext)
	exchangeName := firstNonEmpty(signal.Exchange, evalCtx.MarketData.Exchange)
	symbol := firstNonEmpty(signal.Symbol, evalCtx.MarketData.Symbol)
	timeframe := firstNonEmpty(signal.Timeframe, evalCtx.MarketData.Timeframe)
	signal = normalizeRiskSignal(signal, exchangeName, symbol, timeframe)
	if models.IsEmptySignal(signal) {
		return nil
	}
	if !trendGuardGroupedEnabled(r.cfg.TrendGuard) {
		return nil
	}
	eventTS := normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.refreshCandidate(r.cfg.TrendGuard, signal, evalCtx, eventTS)
	return nil
}

func (r *BackTest) markTrendGuardSignalGoneLocked(signal models.Signal, eventTS int64) {
	if r == nil || r.trendGuard == nil {
		return
	}
	if !trendGuardGroupedEnabled(r.cfg.TrendGuard) {
		return
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.markSignalGone(r.cfg.TrendGuard, signal, eventTS)
}

func (r *BackTest) syncTrendGuardPositionsLocked(eventTS int64) {
	if r == nil || r.trendGuard == nil {
		return
	}
	if !trendGuardGroupedEnabled(r.cfg.TrendGuard) {
		return
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	r.trendGuard.syncPositions(r.snapshotPositionsLocked(), eventTS)
}

func (r *BackTest) LookupSignalGrouped(signal models.Signal) (models.SignalGroupedInfo, bool) {
	if r == nil || r.trendGuard == nil {
		return models.SignalGroupedInfo{}, false
	}
	if !trendGuardGroupedEnabled(r.cfg.TrendGuard) {
		return models.SignalGroupedInfo{}, false
	}
	return r.trendGuard.lookupSignalGrouped(r.cfg.TrendGuard, signal)
}

func buildBackTestTradesFromHistoryRows(rows []models.RiskHistoryPosition, runID string) []BackTestTrade {
	out := make([]BackTestTrade, 0, len(rows))
	for _, row := range rows {
		openMeta := parseBackTestOpenMeta(row.OpenRowJSON)
		closeMeta := parseBackTestCloseMeta(row.CloseRowJSON)
		if runID != "" && openMeta.RunID != runID && closeMeta.RunID != runID {
			continue
		}

		entryQty := math.Abs(parseFloatOrZero(row.Pos))
		entryPrice := parseFloatOrZero(row.AvgPx)
		notional := parseFloatOrZero(row.NotionalUSD)
		if entryQty <= 0 && entryPrice > 0 && notional > 0 {
			entryQty = notional / entryPrice
		}
		margin := parseFloatOrZero(row.Margin)
		leverage := parseFloatOrZero(row.Lever)
		if margin <= 0 && leverage > 0 && notional > 0 {
			margin = notional / leverage
		}
		exitPrice := parseFloatOrZero(row.CloseAvgPx)
		profit := parseFloatOrZero(row.RealizedPnl)
		profitRate := parseFloatOrZero(row.PnlRatio)
		if margin > 0 {
			profitRate = profit / margin
		}
		status := strings.ToLower(strings.TrimSpace(row.State))
		if status == "" {
			status = models.PositionStatusClosed
		}
		closeReason := strings.TrimSpace(closeMeta.CloseReason)
		if closeReason == "" {
			closeReason = "closed"
		}

		side := normalizePositionSide(row.PosSide, highSideFromQuantity(parseFloatOrZero(row.Pos)))
		if side == "" {
			side = row.PosSide
		}

		trade := BackTestTrade{
			Exchange:        row.Exchange,
			Symbol:          row.Symbol,
			Timeframe:       openMeta.Timeframe,
			GroupID:         openMeta.GroupID,
			Side:            side,
			Strategy:        openMeta.Strategy,
			StrategyVersion: openMeta.StrategyVersion,
			Margin:          margin,
			Leverage:        leverage,
			EntryPrice:      entryPrice,
			EntryQuantity:   entryQty,
			EntryTS:         normalizeTimestampMS(row.OpenTimeMS),
			ExitPrice:       exitPrice,
			ExitQuantity:    entryQty,
			ExitTS:          normalizeTimestampMS(row.CloseTimeMS),
			Profit:          profit,
			ProfitRate:      profitRate,
			Status:          status,
			CloseReason:     closeReason,
		}
		if margin > 0 {
			trade.MaxDrawdownRate = row.MaxFloatingLossAmount / margin
			trade.MaxProfitRate = row.MaxFloatingProfitAmount / margin
		}
		out = append(out, trade)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].ExitTS != out[j].ExitTS {
			return out[i].ExitTS < out[j].ExitTS
		}
		if out[i].EntryTS != out[j].EntryTS {
			return out[i].EntryTS < out[j].EntryTS
		}
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		return out[i].Symbol < out[j].Symbol
	})
	for i := range out {
		out[i].TradeID = int64(i + 1)
	}
	return out
}

func buildBackTestOpenPositionsFromRows(rows []models.RiskOpenPosition, runID string) []models.Position {
	out := make([]models.Position, 0, len(rows))
	for _, row := range rows {
		meta := parseBackTestOpenMeta(row.RowJSON)
		if runID != "" && meta.RunID != runID {
			continue
		}
		pos := riskOpenPositionToView(row)
		pos.Timeframe = meta.Timeframe
		pos.StrategyName = meta.Strategy
		out = append(out, pos)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].PositionSide < out[j].PositionSide
	})
	return out
}

func buildBackTestPositionEventsFromOrders(
	orders []models.ExecutionOrderRecord,
	runID string,
	singletonUUID string,
) []BackTestPositionEvent {
	out := make([]BackTestPositionEvent, 0, len(orders))
	filterBySingleton := strings.TrimSpace(singletonUUID) != ""
	for _, item := range orders {
		if filterBySingleton && strings.TrimSpace(item.SingletonUUID) != singletonUUID {
			continue
		}
		if !filterBySingleton && runID != "" {
			orderRunID := strings.TrimSpace(item.SingletonUUID)
			if orderRunID != "" && orderRunID != runID {
				continue
			}
		}

		decision := models.Decision{}
		_ = json.Unmarshal([]byte(item.RequestJSON), &decision)
		eventTS := normalizeTimestampMS(firstPositiveInt64(item.StartedAtMS, item.CreatedAtMS, item.FinishedAtMS))
		klineTS := normalizeTimestampMS(decision.EventTS)
		if klineTS <= 0 {
			klineTS = eventTS
		}
		side := firstNonEmpty(item.PositionSide, decision.PositionSide)
		price := firstPositive(item.Price, decision.Price)
		qty := firstPositive(item.Size, decision.Size)
		leverage := firstPositive(item.LeverageMultiplier, decision.LeverageMultiplier)
		margin := 0.0
		if leverage > 0 && price > 0 && qty > 0 {
			margin = qty * price / leverage
		}
		tp := firstPositive(item.TakeProfitPrice, decision.TakeProfitPrice)
		sl := firstPositive(item.StopLossPrice, decision.StopLossPrice)
		reason := strings.TrimSpace(item.FailReason)
		if reason == "" {
			reason = strings.TrimSpace(item.FailStage)
		}
		closeReason := strings.TrimSpace(decision.CloseReason)
		if reason == "" {
			reason = closeReason
		}

		out = append(out, BackTestPositionEvent{
			EventTS:           eventTS,
			KlineTS:           klineTS,
			Exchange:          item.Exchange,
			Symbol:            item.Symbol,
			Timeframe:         decision.Timeframe,
			Side:              side,
			Strategy:          firstNonEmpty(item.Strategy, decision.Strategy),
			StrategyVersion:   "",
			Action:            mapExecutionActionToPositionEvent(item.Action),
			Price:             price,
			Quantity:          qty,
			RemainingQuantity: 0,
			Margin:            margin,
			Leverage:          leverage,
			TakeProfitPrice:   tp,
			StopLossPrice:     sl,
			CloseReason:       closeReason,
			Result:            item.ResultStatus,
			Reason:            reason,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].EventTS != out[j].EventTS {
			return out[i].EventTS < out[j].EventTS
		}
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		return out[i].Symbol < out[j].Symbol
	})
	for i := range out {
		out[i].EventID = int64(i + 1)
	}
	return out
}

func buildBackTestCloseEventsFromHistoryRows(rows []models.RiskHistoryPosition, runID string) []BackTestPositionEvent {
	out := make([]BackTestPositionEvent, 0, len(rows))
	for _, row := range rows {
		openMeta := parseBackTestOpenMeta(row.OpenRowJSON)
		closeMeta := parseBackTestCloseMeta(row.CloseRowJSON)
		if runID != "" && openMeta.RunID != runID && closeMeta.RunID != runID {
			continue
		}
		closeReason := normalizeBackTestCloseReason(closeMeta.CloseReason, closeMeta.EventReason)
		if !shouldIncludeHistoryCloseEvent(closeReason) {
			continue
		}
		klineTS := normalizeTimestampMS(row.CloseTimeMS)
		if klineTS <= 0 {
			continue
		}
		eventTS := normalizeTimestampMS(firstPositiveInt64(row.UpdatedAtMS, row.CloseTimeMS))
		if eventTS <= 0 {
			eventTS = klineTS
		}
		qty := math.Abs(parseFloatOrZero(row.Pos))
		price := parseFloatOrZero(row.CloseAvgPx)
		margin := parseFloatOrZero(row.Margin)
		leverage := parseFloatOrZero(row.Lever)
		strategy := firstNonEmpty(strings.TrimSpace(closeMeta.Strategy), openMeta.Strategy)
		strategyVersion := firstNonEmpty(strings.TrimSpace(closeMeta.StrategyVersion), openMeta.StrategyVersion)
		reason := strings.TrimSpace(closeMeta.EventReason)
		if reason == "" {
			reason = closeReason
		}
		side := normalizePositionSide(row.PosSide, highSideFromQuantity(parseFloatOrZero(row.Pos)))
		if side == "" {
			side = row.PosSide
		}
		out = append(out, BackTestPositionEvent{
			EventTS:           eventTS,
			KlineTS:           klineTS,
			Exchange:          row.Exchange,
			Symbol:            row.Symbol,
			Timeframe:         openMeta.Timeframe,
			Side:              side,
			Strategy:          strategy,
			StrategyVersion:   strategyVersion,
			Action:            "close",
			Price:             price,
			Quantity:          qty,
			RemainingQuantity: 0,
			Margin:            margin,
			Leverage:          leverage,
			TakeProfitPrice:   parseFloatOrZero(row.TPTriggerPx),
			StopLossPrice:     parseFloatOrZero(row.SLTriggerPx),
			CloseReason:       closeReason,
			Result:            "filled",
			Reason:            reason,
		})
	}
	return out
}

func mergeBackTestPositionEvents(groups ...[]BackTestPositionEvent) []BackTestPositionEvent {
	total := 0
	for _, group := range groups {
		total += len(group)
	}
	if total == 0 {
		return nil
	}
	out := make([]BackTestPositionEvent, 0, total)
	for _, group := range groups {
		out = append(out, group...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		leftTS := firstPositiveInt64(out[i].KlineTS, out[i].EventTS)
		rightTS := firstPositiveInt64(out[j].KlineTS, out[j].EventTS)
		if leftTS != rightTS {
			return leftTS < rightTS
		}
		if out[i].EventTS != out[j].EventTS {
			return out[i].EventTS < out[j].EventTS
		}
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		if out[i].Action != out[j].Action {
			return out[i].Action < out[j].Action
		}
		return out[i].CloseReason < out[j].CloseReason
	})
	for i := range out {
		out[i].EventID = int64(i + 1)
	}
	return out
}

func normalizeBackTestCloseReason(closeReason string, eventReason string) string {
	reason := strings.ToLower(strings.TrimSpace(closeReason))
	if reason != "" {
		return reason
	}
	event := strings.ToLower(strings.TrimSpace(eventReason))
	switch {
	case strings.Contains(event, "take_profit"):
		return "take_profit"
	case strings.Contains(event, "stop_loss"):
		return "stop_loss"
	case strings.Contains(event, "forced_settle"):
		return "forced_settle"
	default:
		return ""
	}
}

func shouldIncludeHistoryCloseEvent(closeReason string) bool {
	switch strings.ToLower(strings.TrimSpace(closeReason)) {
	case "take_profit", "stop_loss", "forced_settle":
		return true
	default:
		return false
	}
}

func parseBackTestOpenMeta(raw string) backTestOpenMeta {
	meta := backTestOpenMeta{}
	if strings.TrimSpace(raw) == "" {
		return meta
	}
	if err := json.Unmarshal([]byte(raw), &meta); err == nil {
		if meta != (backTestOpenMeta{}) {
			return meta
		}
	}
	env, ok := models.ParsePositionRowEnvelope(raw)
	if !ok {
		return meta
	}
	if len(env.ExchangeRaw) > 0 {
		_ = json.Unmarshal(env.ExchangeRaw, &meta)
	}
	runtimeMeta := models.ExtractPositionRuntimeMeta(raw)
	if meta.SingletonID <= 0 {
		meta.SingletonID = runtimeMeta.SingletonID
	}
	if strings.TrimSpace(meta.RunID) == "" {
		meta.RunID = runtimeMeta.RunID
	}
	if strings.TrimSpace(meta.Strategy) == "" {
		meta.Strategy = strings.TrimSpace(env.GobotMeta.StrategyName)
	}
	if strings.TrimSpace(meta.StrategyVersion) == "" {
		meta.StrategyVersion = strings.TrimSpace(env.GobotMeta.StrategyVersion)
	}
	if strings.TrimSpace(meta.Timeframe) == "" {
		meta.Timeframe = strings.TrimSpace(strategyPrimaryTimeframe(env.GobotMeta))
	}
	if strings.TrimSpace(meta.GroupID) == "" {
		meta.GroupID = strings.TrimSpace(env.GobotMeta.GroupID)
	}
	return meta
}

func buildBackTestOpenSignalMeta(signal models.Signal, timeframe string) models.StrategyContextMeta {
	signal.Timeframe = strings.TrimSpace(firstNonEmpty(signal.Timeframe, timeframe))
	if len(signal.StrategyTimeframes) == 0 && signal.Timeframe != "" {
		signal.StrategyTimeframes = []string{signal.Timeframe}
	}
	signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey = common.NormalizeStrategyIdentity(
		signal.Timeframe,
		signal.StrategyTimeframes,
		signal.ComboKey,
	)
	return models.BuildStrategyContextMetaFromSignal(signal)
}

func backTestStrategyContextMeta(pos *backTestPosition) models.StrategyContextMeta {
	if pos == nil {
		return models.StrategyContextMeta{}
	}
	meta := models.StrategyContextMeta{
		StrategyName:       strings.TrimSpace(pos.Strategy),
		StrategyVersion:    strings.TrimSpace(pos.StrategyVersion),
		StrategyTimeframes: append([]string(nil), pos.StrategyTimeframes...),
		ComboKey:           strings.TrimSpace(pos.ComboKey),
		GroupID:            strings.TrimSpace(pos.GroupID),
		StrategyIndicators: cloneStrategyIndicators(pos.StrategyIndicators),
	}
	if len(meta.StrategyTimeframes) == 0 && strings.TrimSpace(pos.Timeframe) != "" {
		meta.StrategyTimeframes = []string{strings.TrimSpace(pos.Timeframe)}
	}
	return models.NormalizeStrategyContextMeta(meta)
}

func cloneStrategyIndicators(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string, len(input))
	for name, values := range input {
		key := strings.TrimSpace(name)
		if key == "" {
			continue
		}
		cloned := make([]string, 0, len(values))
		for _, value := range values {
			item := strings.TrimSpace(value)
			if item == "" {
				continue
			}
			cloned = append(cloned, item)
		}
		if len(cloned) == 0 {
			continue
		}
		out[key] = cloned
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseBackTestCloseMeta(raw string) backTestCloseMeta {
	meta := backTestCloseMeta{}
	if strings.TrimSpace(raw) == "" {
		return meta
	}
	if err := json.Unmarshal([]byte(raw), &meta); err == nil && meta != (backTestCloseMeta{}) {
		return meta
	}
	env, ok := models.ParsePositionRowEnvelope(raw)
	if ok && len(env.ExchangeRaw) > 0 {
		_ = json.Unmarshal(env.ExchangeRaw, &meta)
	}
	runtimeMeta := models.ExtractPositionRuntimeMeta(raw)
	if meta.SingletonID <= 0 {
		meta.SingletonID = runtimeMeta.SingletonID
	}
	if strings.TrimSpace(meta.RunID) == "" {
		meta.RunID = runtimeMeta.RunID
	}
	return meta
}

func historySnapshotToPosition(row models.RiskHistoryPosition, meta backTestOpenMeta) models.Position {
	entryQty := math.Abs(parseFloatOrZero(row.Pos))
	entryPrice := parseFloatOrZero(row.AvgPx)
	entryValue := parseFloatOrZero(row.NotionalUSD)
	if entryValue <= 0 && entryQty > 0 && entryPrice > 0 {
		entryValue = entryQty * entryPrice
	}
	exitPrice := parseFloatOrZero(row.CloseAvgPx)
	exitValue := 0.0
	if exitPrice > 0 && entryQty > 0 {
		exitValue = exitPrice * entryQty
	}
	realized := parseFloatOrZero(row.RealizedPnl)
	fee := parseFloatOrZero(row.Fee) + parseFloatOrZero(row.FundingFee)
	profit := realized
	profitRate := parseFloatOrZero(row.PnlRatio)
	if margin := parseFloatOrZero(row.Margin); margin > 0 {
		profitRate = profit / margin
	}
	status := strings.ToLower(strings.TrimSpace(row.State))
	if status == "" {
		status = models.PositionStatusClosed
	}
	side := normalizePositionSide(row.PosSide, highSideFromQuantity(parseFloatOrZero(row.Pos)))
	if side == "" {
		side = row.PosSide
	}
	return models.Position{
		Exchange:               row.Exchange,
		Symbol:                 row.Symbol,
		Timeframe:              meta.Timeframe,
		GroupID:                meta.GroupID,
		PositionSide:           side,
		MarginMode:             normalizeMarginMode(row.MgnMode, models.MarginModeIsolated),
		LeverageMultiplier:     parseFloatOrZero(row.Lever),
		MarginAmount:           parseFloatOrZero(row.Margin),
		EntryPrice:             entryPrice,
		EntryQuantity:          entryQty,
		EntryValue:             entryValue,
		EntryTime:              formatBackTestTimestampMS(row.OpenTimeMS),
		TakeProfitPrice:        parseFloatOrZero(row.TPTriggerPx),
		StopLossPrice:          parseFloatOrZero(row.SLTriggerPx),
		CurrentPrice:           parseFloatOrZero(row.MarkPx),
		ExitPrice:              exitPrice,
		ExitQuantity:           entryQty,
		ExitValue:              exitValue,
		ExitTime:               formatBackTestTimestampMS(row.CloseTimeMS),
		FeeAmount:              fee,
		ProfitAmount:           profit,
		ProfitRate:             profitRate,
		Status:                 status,
		StrategyName:           meta.Strategy,
		UpdatedTime:            formatBackTestTimestampMS(row.UpdatedAtMS),
		UnrealizedProfitAmount: 0,
		UnrealizedProfitRate:   0,
	}
}

func tradeToHistoryPosition(trade BackTestTrade) models.Position {
	return models.Position{
		PositionID:         trade.TradeID,
		Exchange:           trade.Exchange,
		Symbol:             trade.Symbol,
		Timeframe:          trade.Timeframe,
		GroupID:            trade.GroupID,
		PositionSide:       trade.Side,
		MarginMode:         models.MarginModeIsolated,
		LeverageMultiplier: trade.Leverage,
		MarginAmount:       trade.Margin,
		EntryPrice:         trade.EntryPrice,
		EntryQuantity:      trade.EntryQuantity,
		EntryValue:         trade.EntryPrice * trade.EntryQuantity,
		EntryTime:          formatBackTestTimestampMS(trade.EntryTS),
		ExitPrice:          trade.ExitPrice,
		ExitQuantity:       trade.ExitQuantity,
		ExitValue:          trade.ExitPrice * trade.ExitQuantity,
		ExitTime:           formatBackTestTimestampMS(trade.ExitTS),
		FeeAmount:          0,
		ProfitAmount:       trade.Profit,
		ProfitRate:         trade.ProfitRate,
		Status:             strings.ToLower(strings.TrimSpace(trade.Status)),
		StrategyName:       trade.Strategy,
		UpdatedTime:        formatBackTestTimestampMS(trade.ExitTS),
	}
}

func sumUnrealizedRateFromPositions(positions []models.Position) float64 {
	total := 0.0
	for _, pos := range positions {
		total += pos.UnrealizedProfitRate
	}
	return total
}

func sumTradeProfitRates(trades []BackTestTrade) float64 {
	total := 0.0
	for _, trade := range trades {
		total += trade.ProfitRate
	}
	return total
}

func (r *BackTest) totalClosedProfitRateLocked() float64 {
	if r == nil {
		return 0
	}
	return sumTradeProfitRates(r.trades)
}

func (r *BackTest) totalUnrealizedRateLocked(exchange string) float64 {
	if r == nil {
		return 0
	}
	open := r.listOpenPositionsFromMemoryLocked(exchange, "", "")
	return sumUnrealizedRateFromPositions(open)
}

func (r *BackTest) reportMode() string {
	if r == nil {
		return backTestMode
	}
	mode := strings.TrimSpace(r.mode)
	if mode == "" {
		return backTestMode
	}
	return mode
}

func mapExecutionActionToPositionEvent(action string) string {
	switch action {
	case models.DecisionActionOpenLong, models.DecisionActionOpenShort:
		return "open"
	case models.DecisionActionUpdate:
		return "move_tpsl"
	case models.DecisionActionClose:
		return "close"
	default:
		return action
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

func floatAlmostEqual(left, right float64) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return diff <= 1e-12
}

func backTestInstID(symbol string) string {
	text := strings.ToUpper(strings.TrimSpace(symbol))
	if text == "" {
		return ""
	}
	if strings.HasSuffix(text, "-SWAP") {
		return text
	}
	text = canonicalSymbol(text)
	if strings.Contains(text, "/") {
		parts := strings.Split(text, "/")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return parts[0] + "-" + parts[1] + "-SWAP"
		}
	}
	return strings.ReplaceAll(text, "/", "-")
}

func buildReport(
	circuitBreaker bool,
	cooldownUntilTS int64,
	trades []BackTestTrade,
	positionEvents []BackTestPositionEvent,
) BackTestReport {
	report := BackTestReport{
		CircuitBreaker:      circuitBreaker,
		CooldownUntilTS:     cooldownUntilTS,
		TotalTrades:         len(trades),
		ClosedTrades:        len(trades),
		Trades:              append([]BackTestTrade(nil), trades...),
		PositionEvents:      append([]BackTestPositionEvent(nil), positionEvents...),
		TotalPositionEvents: len(positionEvents),
	}
	if len(trades) == 0 {
		return report
	}
	report.MaxProfit = trades[0].Profit
	report.MaxLoss = trades[0].Profit
	totalRate := 0.0
	for _, trade := range trades {
		report.TotalPnL += trade.Profit
		totalRate += trade.ProfitRate
		if trade.Profit > 0 {
			report.WinTrades++
		} else if trade.Profit < 0 {
			report.LossTrades++
		} else {
			report.FlatTrades++
		}
		if trade.Profit > report.MaxProfit {
			report.MaxProfit = trade.Profit
		}
		if trade.Profit < report.MaxLoss {
			report.MaxLoss = trade.Profit
		}
		if trade.CloseReason == "forced_settle" {
			report.ForcedCloseCount++
		}
	}
	report.AvgPnL = report.TotalPnL / float64(len(trades))
	report.AvgPnLRate = totalRate / float64(len(trades))
	report.ReturnRate = totalRate
	if report.ClosedTrades > 0 {
		report.WinRate = float64(report.WinTrades) / float64(report.ClosedTrades)
	}
	return report
}

func cloneReport(report BackTestReport) BackTestReport {
	report.Trades = append([]BackTestTrade(nil), report.Trades...)
	report.PositionEvents = append([]BackTestPositionEvent(nil), report.PositionEvents...)
	report.OpenPositions = append([]models.Position(nil), report.OpenPositions...)
	return report
}

func pairKey(exchange, symbol string) string {
	return exchange + "|" + symbol
}

func backTestExecutionTimeframe(pos *backTestPosition) string {
	if pos == nil {
		return ""
	}
	best := ""
	bestDur := time.Duration(0)
	choose := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		dur, ok := market.TimeframeDuration(value)
		if !ok || dur <= 0 {
			if best == "" {
				best = value
			}
			return
		}
		if best == "" || bestDur <= 0 || dur < bestDur {
			best = value
			bestDur = dur
		}
	}
	for _, timeframe := range pos.StrategyTimeframes {
		choose(timeframe)
	}
	if best == "" {
		choose(pos.Timeframe)
	}
	return best
}

func signalSide(highSide int) (string, bool) {
	switch highSide {
	case 1:
		return positionSideLong, true
	case -1:
		return positionSideShort, true
	default:
		return "", false
	}
}

func adverseMoveRate(side string, entryPrice, stopLoss float64) (float64, error) {
	if entryPrice <= 0 || stopLoss <= 0 {
		return 0, fmt.Errorf("risk: invalid entry/sl price")
	}
	switch side {
	case positionSideLong:
		if stopLoss >= entryPrice {
			return 0, fmt.Errorf("risk: invalid SL for long")
		}
		return (entryPrice - stopLoss) / entryPrice, nil
	case positionSideShort:
		if stopLoss <= entryPrice {
			return 0, fmt.Errorf("risk: invalid SL for short")
		}
		return (stopLoss - entryPrice) / entryPrice, nil
	default:
		return 0, fmt.Errorf("risk: invalid position side")
	}
}

func backTestPositionOwnedBy(pos *backTestPosition, signalStrategy, signalTimeframe string, signalTimeframes []string, signalCombo string) bool {
	if pos == nil {
		return false
	}
	positionStrategy := strings.TrimSpace(pos.Strategy)
	positionPrimary, _, positionCombo := common.NormalizeStrategyIdentity(pos.Timeframe, pos.StrategyTimeframes, pos.ComboKey)
	implicitPositionPrimaryOnly := len(pos.StrategyTimeframes) == 0 &&
		(strings.TrimSpace(pos.ComboKey) == "" || strings.EqualFold(strings.TrimSpace(pos.ComboKey), positionPrimary))
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

func ensureBackTestPositionOwnership(pos *backTestPosition, signalStrategy, signalTimeframe string, signalTimeframes []string, signalCombo string) error {
	if backTestPositionOwnedBy(pos, signalStrategy, signalTimeframe, signalTimeframes, signalCombo) {
		return nil
	}
	positionStrategy := ""
	positionCombo := ""
	positionPrimary := ""
	if pos != nil {
		positionStrategy = strings.TrimSpace(pos.Strategy)
		positionPrimary, _, positionCombo = common.NormalizeStrategyIdentity(pos.Timeframe, pos.StrategyTimeframes, pos.ComboKey)
	}
	implicitPositionPrimaryOnly := pos != nil && len(pos.StrategyTimeframes) == 0 &&
		(strings.TrimSpace(pos.ComboKey) == "" || strings.EqualFold(strings.TrimSpace(pos.ComboKey), positionPrimary))
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
		return fmt.Errorf("risk: position ownership mismatch, strategy %s/%s", positionStrategy, signalStrategy)
	}
	return fmt.Errorf("risk: position ownership mismatch, combo %s/%s", positionCombo, resolvedSignalCombo)
}

func isStopMoveWithTrend(side string, currentSL, nextSL float64) bool {
	if nextSL <= 0 {
		return false
	}
	switch side {
	case positionSideLong:
		return nextSL > currentSL
	case positionSideShort:
		return nextSL < currentSL
	default:
		return false
	}
}

func resolveOpenPrice(signal models.Signal, data models.MarketData) float64 {
	if signal.Entry > 0 {
		return signal.Entry
	}
	return marketPrice(data, 0)
}

func normalizeSignalOrderType(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case models.OrderTypeMarket:
		return models.OrderTypeMarket, nil
	case models.OrderTypeLimit:
		return models.OrderTypeLimit, nil
	default:
		return "", fmt.Errorf("unsupported order_type %s", raw)
	}
}

func decisionOpenOrderType(signalOrderType string) string {
	if signalOrderType == models.OrderTypeLimit {
		return models.OrderTypeLimit
	}
	return models.OrderTypeMarket
}

func resolveClosePrice(signal models.Signal, data models.MarketData, fallback float64) float64 {
	if signal.Exit > 0 {
		return signal.Exit
	}
	return marketPrice(data, fallback)
}

func marketPrice(data models.MarketData, fallback float64) float64 {
	if data.OHLCV.Close > 0 {
		return data.OHLCV.Close
	}
	if data.OHLCV.Open > 0 {
		return data.OHLCV.Open
	}
	return fallback
}

func extractMarketData(accountState any) models.MarketData {
	return extractRiskEvalContext(accountState).MarketData
}

func extractRiskEvalContext(accountState any) models.RiskEvalContext {
	if accountState == nil {
		return models.RiskEvalContext{}
	}
	switch value := accountState.(type) {
	case models.RiskEvalContext:
		return normalizeRiskEvalContext(value)
	case *models.RiskEvalContext:
		if value == nil {
			return models.RiskEvalContext{}
		}
		return normalizeRiskEvalContext(*value)
	case models.MarketData:
		return models.RiskEvalContext{MarketData: value}
	case *models.MarketData:
		if value == nil {
			return models.RiskEvalContext{}
		}
		return models.RiskEvalContext{MarketData: *value}
	default:
		return models.RiskEvalContext{}
	}
}

func normalizeRiskEvalContext(ctx models.RiskEvalContext) models.RiskEvalContext {
	if ctx.Snapshot == nil {
		return ctx
	}
	snapshot := *ctx.Snapshot
	ctx.Snapshot = &snapshot
	return ctx
}

func resolveSignalTimestamp(defaultTS int64, triggerTimestamp int) int64 {
	defaultTS = normalizeTimestampMS(defaultTS)
	triggerTS := normalizeTimestampMS(int64(triggerTimestamp))
	if triggerTS <= 0 {
		return defaultTS
	}
	return triggerTS
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

func positionPnL(side string, entryPrice, exitPrice, qty float64) float64 {
	if qty <= 0 {
		return 0
	}
	switch side {
	case positionSideLong:
		return (exitPrice - entryPrice) * qty
	case positionSideShort:
		return (entryPrice - exitPrice) * qty
	default:
		return 0
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

var _ iface.Evaluator = (*BackTest)(nil)
