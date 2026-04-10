package exporter

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

const (
	tradingViewTradeStrategyManual = "manual"
	manualOrderReconcileInterval   = time.Second
	tradingViewDefaultSLMaxLossPct = 0.05
	tradingViewDefaultSLSafetyRate = 0.0001
)

type tradingViewTradeRequest struct {
	Action    string  `json:"action"`
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Side      string  `json:"side"`
	Timeframe string  `json:"timeframe,omitempty"`
	OrderType string  `json:"order_type,omitempty"`
	Amount    float64 `json:"amount,omitempty"`
	Entry     float64 `json:"entry,omitempty"`
	TP        float64 `json:"tp,omitempty"`
	SL        float64 `json:"sl,omitempty"`
	Strategy  string  `json:"strategy,omitempty"`
}

type tradingViewPositionDelegateRequest struct {
	Exchange        string   `json:"exchange"`
	Symbol          string   `json:"symbol"`
	Side            string   `json:"side"`
	StrategyName    string   `json:"strategy_name"`
	TradeTimeframes []string `json:"trade_timeframes"`
}

type tradingViewPositionDelegateResponse struct {
	Delegated bool `json:"delegated"`
}

type tradingViewTradeResponse struct {
	PositionFound  bool                  `json:"position_found"`
	Position       *positionItem         `json:"position,omitempty"`
	Decision       *models.Decision      `json:"decision,omitempty"`
	Executed       bool                  `json:"executed"`
	ManualOrder    *tradingViewOrderItem `json:"manual_order,omitempty"`
	RiskError      string                `json:"risk_error,omitempty"`
	ExecutionError string                `json:"execution_error,omitempty"`
}

type preparedTradingViewTrade struct {
	response tradeResponse
	signal   models.Signal
	decision models.Decision
}

type riskSLConfigEnvelope struct {
	SL struct {
		MaxLossPct float64 `json:"max_loss_pct"`
	} `json:"sl"`
}

type tradingViewPositionDelegator interface {
	DelegatePositionStrategy(exchange, symbol, side, strategy string, timeframes []string) error
}

func (s *Server) handleTradingViewTrade(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.TradeEvaluator == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "trade evaluator unavailable"})
		return
	}
	if s.cfg.TradeExecutor == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "trade executor unavailable"})
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	if strings.EqualFold(strings.TrimSpace(s.cfg.Mode), "back-test") {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "manual trading unavailable in back-test mode"})
		return
	}

	var req tradingViewTradeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid request body: %v", err)})
		return
	}

	baseReq := tradeRequest{
		Action:    req.Action,
		Exchange:  req.Exchange,
		Symbol:    req.Symbol,
		Side:      req.Side,
		OrderType: req.OrderType,
		Amount:    req.Amount,
		Entry:     req.Entry,
		TP:        req.TP,
		SL:        req.SL,
		Strategy:  req.Strategy,
	}
	orderType, err := parseTradeOrderType(baseReq.OrderType)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	actionCode, actionName, err := parseTradeAction(baseReq.Action)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	if actionCode == 8 && orderType == models.OrderTypeLimit {
		resp, statusCode, err := s.createTradingViewLimitManualOrder(baseReq, strings.TrimSpace(req.Timeframe))
		if err != nil {
			writeJSON(w, statusCode, errorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if actionName == tradeActionOpen && baseReq.SL <= 0 {
		baseReq.SL, err = s.defaultTradingViewStopLoss(baseReq.Entry, baseReq.Side)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
			return
		}
	}

	resp, statusCode, err := s.executeTradingViewTrade(baseReq, strings.TrimSpace(req.Timeframe))
	if err != nil {
		writeJSON(w, statusCode, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleTradingViewPositionDelegate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	delegator, ok := s.cfg.TradeEvaluator.(tradingViewPositionDelegator)
	if !ok || delegator == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "position delegator unavailable"})
		return
	}
	var req tradingViewPositionDelegateRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid request body: %v", err)})
		return
	}
	exchange := strings.TrimSpace(req.Exchange)
	symbol := strings.TrimSpace(req.Symbol)
	side, _, err := parseTradeSide(req.Side)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	strategy := strings.TrimSpace(req.StrategyName)
	_, timeframes, _ := normalizeTradingViewDelegateIdentity(req.TradeTimeframes)
	if exchange == "" || symbol == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "exchange and symbol are required"})
		return
	}
	if strategy == "" || len(timeframes) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "strategy_name and trade_timeframes are required"})
		return
	}
	if err := delegator.DelegatePositionStrategy(exchange, symbol, side, strategy, timeframes); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tradingViewPositionDelegateResponse{Delegated: true})
}

func (s *Server) executeTradingViewTrade(req tradeRequest, timeframe string) (tradingViewTradeResponse, int, error) {
	prepared, err := s.prepareTradingViewTrade(req, timeframe)
	if err != nil {
		return tradingViewTradeResponse{}, http.StatusServiceUnavailable, err
	}
	resp := tradingViewTradeResponse{
		PositionFound: prepared.response.PositionFound,
		Position:      prepared.response.Position,
		Decision:      prepared.response.Decision,
		RiskError:     prepared.response.RiskError,
	}
	if prepared.response.RiskError != "" || prepared.decision.Action == "" || prepared.decision.Action == models.DecisionActionIgnore {
		return resp, http.StatusOK, nil
	}
	if err := s.cfg.TradeExecutor.Place(prepared.decision); err != nil {
		if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
			notifier.NotifyExecutionResult(prepared.decision, err)
		}
		resp.ExecutionError = err.Error()
		return resp, http.StatusOK, nil
	}
	if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
		notifier.NotifyExecutionResult(prepared.decision, nil)
	}
	resp.Executed = true
	return resp, http.StatusOK, nil
}

func (s *Server) createTradingViewLimitManualOrder(req tradeRequest, timeframe string) (tradingViewTradeResponse, int, error) {
	if req.SL <= 0 {
		var err error
		req.SL, err = s.defaultTradingViewStopLoss(req.Entry, req.Side)
		if err != nil {
			return tradingViewTradeResponse{}, http.StatusBadRequest, err
		}
	}
	prepared, err := s.prepareTradingViewTrade(req, timeframe)
	if err != nil {
		return tradingViewTradeResponse{}, http.StatusServiceUnavailable, err
	}
	resp := tradingViewTradeResponse{
		PositionFound: prepared.response.PositionFound,
		Position:      prepared.response.Position,
		Decision:      prepared.response.Decision,
		RiskError:     prepared.response.RiskError,
	}
	if prepared.response.RiskError != "" || prepared.decision.Action == "" || prepared.decision.Action == models.DecisionActionIgnore {
		return resp, http.StatusOK, nil
	}
	if exists, err := s.hasPendingManualOrder(prepared.decision.Exchange, prepared.decision.Symbol, prepared.decision.PositionSide); err != nil {
		return tradingViewTradeResponse{}, http.StatusInternalServerError, err
	} else if exists {
		resp.ExecutionError = "existing pending manual order"
		return resp, http.StatusOK, nil
	}

	nowMS := time.Now().UnixMilli()
	record := models.ManualOrder{
		Mode:               strings.TrimSpace(s.cfg.Mode),
		Exchange:           prepared.decision.Exchange,
		Symbol:             prepared.decision.Symbol,
		InstID:             "",
		Timeframe:          timeframe,
		PositionSide:       prepared.decision.PositionSide,
		MarginMode:         prepared.decision.MarginMode,
		OrderType:          models.OrderTypeLimit,
		Status:             models.ManualOrderStatusPending,
		StrategyName:       tradingViewTradeStrategyManual,
		LeverageMultiplier: prepared.decision.LeverageMultiplier,
		Amount:             prepared.signal.Amount,
		Size:               prepared.decision.Size,
		Price:              prepared.decision.Price,
		TakeProfitPrice:    prepared.decision.TakeProfitPrice,
		StopLossPrice:      prepared.decision.StopLossPrice,
		ClientOrderID:      prepared.decision.ClientOrderID,
		DecisionJSON:       marshalTradingViewManualDecision(prepared.decision),
		CreatedAtMS:        nowMS,
		UpdatedAtMS:        nowMS,
	}

	mode := strings.TrimSpace(s.cfg.Mode)
	switch mode {
	case "live":
		if err := s.cfg.TradeExecutor.Place(prepared.decision); err != nil {
			if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
				notifier.NotifyExecutionResult(prepared.decision, err)
			}
			resp.ExecutionError = err.Error()
			return resp, http.StatusOK, nil
		}
		if notifier, ok := s.cfg.TradeEvaluator.(tradeExecutionNotifier); ok {
			notifier.NotifyExecutionResult(prepared.decision, nil)
		}
		record.SubmittedAtMS = nowMS
		if executionOrder, found, err := s.cfg.HistoryStore.FindLatestExecutionOrderByClientOrderID(mode, prepared.decision.ClientOrderID); err == nil && found {
			record.ExchangeOrderID = strings.TrimSpace(executionOrder.ExchangeOrderID)
			record.ExchangeAlgoOrderID = strings.TrimSpace(executionOrder.ExchangeAlgoOrderID)
			record.InstID = strings.TrimSpace(executionOrder.InstID)
		}
	case "paper":
		// 纸面模式先进入 pending，等待后台撮合后再真正执行。
	default:
		return tradingViewTradeResponse{}, http.StatusConflict, fmt.Errorf("manual limit order unsupported in mode %s", mode)
	}

	order, err := s.cfg.HistoryStore.CreateManualOrder(record)
	if err != nil {
		return tradingViewTradeResponse{}, http.StatusInternalServerError, err
	}
	item := s.buildTradingViewOrderItem(order, nil)
	resp.ManualOrder = &item
	resp.Executed = mode == "live"
	return resp, http.StatusOK, nil
}

func (s *Server) buildTradingViewOrderItem(order models.ManualOrder, metaBySymbol map[string]tradingViewSymbolMeta) tradingViewOrderItem {
	metaType := ""
	if metaBySymbol != nil {
		metaType = strings.TrimSpace(metaBySymbol[order.Symbol].Type)
	}
	return tradingViewOrderItem{
		ID:                 order.ID,
		Exchange:           strings.ToLower(strings.TrimSpace(order.Exchange)),
		Symbol:             strings.TrimSpace(order.Symbol),
		DisplaySymbol:      tradingViewDisplaySymbol(order.Symbol, metaType),
		Action:             tradingViewManualOrderActionLabel(order.PositionSide),
		OrderType:          strings.TrimSpace(order.OrderType),
		PositionSide:       strings.TrimSpace(order.PositionSide),
		LeverageMultiplier: order.LeverageMultiplier,
		Price:              order.Price,
		Size:               firstPositiveFloat64(order.Size, order.Amount),
		TakeProfitPrice:    order.TakeProfitPrice,
		StopLossPrice:      order.StopLossPrice,
		ResultStatus:       strings.TrimSpace(order.Status),
		ErrorMessage:       strings.TrimSpace(order.ErrorMessage),
		StartedAtMS:        firstPositiveInt64(order.SubmittedAtMS, order.CreatedAtMS),
		UpdatedAtMS:        firstPositiveInt64(order.UpdatedAtMS, order.LastCheckedAtMS, order.CreatedAtMS),
	}
}

func tradingViewManualOrderActionLabel(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case tradeSideLongCanonical:
		return "开多"
	case tradeSideShortCanonical:
		return "开空"
	default:
		return "下单"
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

func firstPositiveFloat64(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func (s *Server) runTradingViewManualOrderLoop(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(manualOrderReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.reconcileTradingViewManualOrders(); err != nil && s.logger != nil {
				s.logger.Warn("reconcile tradingview manual orders failed", zap.Error(err))
			}
		}
	}
}

func (s *Server) reconcileTradingViewManualOrders() error {
	if s == nil || s.cfg.HistoryStore == nil {
		return nil
	}
	pending, err := s.cfg.HistoryStore.ListPendingManualOrders(strings.TrimSpace(s.cfg.Mode))
	if err != nil {
		return err
	}
	for _, item := range pending {
		var reconcileErr error
		switch strings.TrimSpace(strings.ToLower(s.cfg.Mode)) {
		case "paper":
			reconcileErr = s.reconcileTradingViewPaperManualOrder(item)
		case "live":
			reconcileErr = s.reconcileTradingViewLiveManualOrder(item)
		}
		if reconcileErr != nil && s.logger != nil {
			s.logger.Warn("reconcile manual order item failed",
				zap.Int64("order_id", item.ID),
				zap.String("exchange", item.Exchange),
				zap.String("symbol", item.Symbol),
				zap.Error(reconcileErr),
			)
		}
	}
	return nil
}

func (s *Server) reconcileTradingViewPaperManualOrder(order models.ManualOrder) error {
	provider, ok := s.cfg.TradingViewRuntime.(tradingViewRuntimeCandleProvider)
	if !ok || provider == nil {
		return nil
	}
	timeframe := strings.TrimSpace(order.Timeframe)
	if timeframe == "" {
		timeframe = firstNonEmpty(s.defaultTradingViewTimeframe(), "15m")
	}
	snapshot, found := provider.LookupRuntimeOHLCV(order.Exchange, order.Symbol, timeframe)
	if !found || snapshot.OHLCV.TS <= 0 {
		return nil
	}
	if !tradingViewManualOrderTriggered(order, snapshot.OHLCV) {
		next := order
		next.LastCheckedAtMS = time.Now().UnixMilli()
		next.UpdatedAtMS = next.LastCheckedAtMS
		return s.cfg.HistoryStore.UpdateManualOrder(next)
	}
	decision, err := decodeTradingViewManualDecision(order.DecisionJSON)
	if err != nil {
		next := order
		next.Status = models.ManualOrderStatusRejected
		next.ErrorMessage = err.Error()
		next.LastCheckedAtMS = time.Now().UnixMilli()
		next.UpdatedAtMS = next.LastCheckedAtMS
		return s.cfg.HistoryStore.UpdateManualOrder(next)
	}
	if err := s.cfg.TradeExecutor.Place(decision); err != nil {
		next := order
		next.Status = models.ManualOrderStatusRejected
		next.ErrorMessage = err.Error()
		next.LastCheckedAtMS = time.Now().UnixMilli()
		next.UpdatedAtMS = next.LastCheckedAtMS
		return s.cfg.HistoryStore.UpdateManualOrder(next)
	}
	return s.markTradingViewManualOrderFilled(order)
}

func (s *Server) reconcileTradingViewLiveManualOrder(order models.ManualOrder) error {
	positions, err := s.cfg.TradeEvaluator.ListOpenPositions(order.Exchange, order.Symbol, "")
	if err != nil {
		return err
	}
	position, found := selectTradePosition(positions, order.PositionSide, "")
	if !found {
		next := order
		next.LastCheckedAtMS = time.Now().UnixMilli()
		next.UpdatedAtMS = next.LastCheckedAtMS
		return s.cfg.HistoryStore.UpdateManualOrder(next)
	}
	next := order
	next.Status = models.ManualOrderStatusFilled
	next.PositionID = position.PositionID
	next.EntryPrice = position.EntryPrice
	next.FilledSize = position.EntryQuantity
	next.FilledAtMS = time.Now().UnixMilli()
	next.LastCheckedAtMS = next.FilledAtMS
	next.UpdatedAtMS = next.FilledAtMS
	return s.cfg.HistoryStore.UpdateManualOrder(next)
}

func (s *Server) markTradingViewManualOrderFilled(order models.ManualOrder) error {
	positions, err := s.cfg.TradeEvaluator.ListOpenPositions(order.Exchange, order.Symbol, "")
	if err != nil {
		return err
	}
	position, found := selectTradePosition(positions, order.PositionSide, "")
	next := order
	next.Status = models.ManualOrderStatusFilled
	nowMS := time.Now().UnixMilli()
	next.FilledAtMS = nowMS
	next.LastCheckedAtMS = nowMS
	next.UpdatedAtMS = nowMS
	if found {
		next.PositionID = position.PositionID
		next.EntryPrice = position.EntryPrice
		next.FilledSize = position.EntryQuantity
	}
	return s.cfg.HistoryStore.UpdateManualOrder(next)
}

func tradingViewManualOrderTriggered(order models.ManualOrder, candle models.OHLCV) bool {
	if order.Price <= 0 || candle.High <= 0 || candle.Low <= 0 {
		return false
	}
	return candle.Low <= order.Price && candle.High >= order.Price
}

func (s *Server) hasPendingManualOrder(exchange, symbol, side string) (bool, error) {
	items, err := s.cfg.HistoryStore.ListManualOrders(strings.TrimSpace(s.cfg.Mode), strings.TrimSpace(exchange), models.ManualOrderStatusPending)
	if err != nil {
		return false, err
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	symbol = strings.TrimSpace(symbol)
	side = strings.ToLower(strings.TrimSpace(side))
	for _, item := range items {
		if strings.ToLower(strings.TrimSpace(item.Exchange)) != exchange {
			continue
		}
		if strings.TrimSpace(item.Symbol) != symbol {
			continue
		}
		if strings.ToLower(strings.TrimSpace(item.PositionSide)) != side {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (s *Server) prepareTradingViewTrade(req tradeRequest, timeframe string) (preparedTradingViewTrade, error) {
	actionCode, _, err := parseTradeAction(req.Action)
	if err != nil {
		return preparedTradingViewTrade{}, err
	}
	orderType, err := parseTradeOrderType(req.OrderType)
	if err != nil {
		return preparedTradingViewTrade{}, err
	}
	if actionCode != 8 {
		orderType = ""
	}
	exchange := strings.TrimSpace(strings.ToLower(req.Exchange))
	symbol := strings.TrimSpace(req.Symbol)
	if exchange == "" || symbol == "" {
		return preparedTradingViewTrade{}, fmt.Errorf("exchange and symbol are required")
	}
	side, highSide, err := parseTradeSide(req.Side)
	if err != nil {
		return preparedTradingViewTrade{}, err
	}
	strategy := strings.TrimSpace(req.Strategy)
	if strategy == "" {
		strategy = tradingViewTradeStrategyManual
	}
	timeframe = strings.TrimSpace(firstNonEmpty(timeframe, s.defaultTradingViewTimeframe()))

	signal := models.Signal{
		Exchange:  exchange,
		Symbol:    symbol,
		Timeframe: timeframe,
		OrderType: orderType,
		Amount:    req.Amount,
		Entry:     req.Entry,
		TP:        req.TP,
		SL:        req.SL,
		Action:    actionCode,
		HighSide:  highSide,
		Strategy:  strategy,
	}
	response := tradeResponse{}
	positions, err := s.cfg.TradeEvaluator.ListOpenPositions(exchange, symbol, "")
	if err != nil {
		return preparedTradingViewTrade{}, err
	}
	position, hasPosition := selectTradePosition(positions, side, "")
	response.PositionFound = hasPosition
	if hasPosition {
		itemList := buildPositionItems([]models.Position{position})
		if len(itemList) > 0 {
			response.Position = &itemList[0]
		}
	}
	marketData := models.MarketData{
		Exchange:  exchange,
		Symbol:    symbol,
		Timeframe: timeframe,
		OHLCV: models.OHLCV{
			TS:    time.Now().UnixMilli(),
			Open:  req.Entry,
			Close: req.Entry,
			High:  req.Entry,
			Low:   req.Entry,
		},
	}

	var decision models.Decision
	switch {
	case actionCode == 8 && !hasPosition:
		decision, err = s.cfg.TradeEvaluator.EvaluateOpenBatch([]models.Signal{signal}, marketData)
	case hasPosition:
		decision, err = s.cfg.TradeEvaluator.EvaluateUpdate(signal, position, marketData)
	default:
		response.RiskError = "no open position found for requested action"
		return preparedTradingViewTrade{response: response}, nil
	}
	if err != nil {
		response.RiskError = err.Error()
		return preparedTradingViewTrade{response: response}, nil
	}
	response.Decision = &decision
	return preparedTradingViewTrade{
		response: response,
		signal:   signal,
		decision: decision,
	}, nil
}

func normalizeTradingViewDelegateIdentity(timeframes []string) (string, []string, string) {
	return common.NormalizeStrategyIdentity("", timeframes, "")
}

func (s *Server) defaultTradingViewTimeframe() string {
	if s == nil || s.cfg.HistoryStore == nil {
		return "15m"
	}
	timeframes, err := s.loadTradingViewTimeframes()
	if err != nil || len(timeframes) == 0 {
		return "15m"
	}
	return strings.TrimSpace(timeframes[0])
}

func (s *Server) defaultTradingViewStopLoss(entryPrice float64, side string) (float64, error) {
	if entryPrice <= 0 {
		return 0, fmt.Errorf("invalid entry price")
	}
	maxLossPct := tradingViewDefaultSLMaxLossPct
	if s != nil && s.cfg.HistoryStore != nil {
		if raw, found, err := s.cfg.HistoryStore.GetConfigValue("risk"); err == nil && found && strings.TrimSpace(raw) != "" {
			var envelope riskSLConfigEnvelope
			if jsonErr := json.Unmarshal([]byte(raw), &envelope); jsonErr == nil && envelope.SL.MaxLossPct > 0 {
				maxLossPct = envelope.SL.MaxLossPct
			}
		}
	}
	if maxLossPct > 0 {
		maxLossPct = math.Max(0, maxLossPct*(1-tradingViewDefaultSLSafetyRate))
	}
	side = strings.ToLower(strings.TrimSpace(side))
	switch side {
	case tradeSideLongCanonical:
		return entryPrice * (1 - maxLossPct), nil
	case tradeSideShortCanonical:
		return entryPrice * (1 + maxLossPct), nil
	default:
		return 0, fmt.Errorf("unsupported side: %s", side)
	}
}

func marshalTradingViewManualDecision(decision models.Decision) string {
	raw, err := json.Marshal(decision)
	if err != nil {
		return ""
	}
	return string(raw)
}

func decodeTradingViewManualDecision(raw string) (models.Decision, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return models.Decision{}, fmt.Errorf("manual order decision payload empty")
	}
	var decision models.Decision
	if err := json.Unmarshal([]byte(raw), &decision); err != nil {
		return models.Decision{}, err
	}
	return decision, nil
}
