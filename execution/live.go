package execution

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/core"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const (
	liveExecutionTimeout       = 8 * time.Second
	liveExecutionPollInterval  = 150 * time.Millisecond
	liveExecutionDefaultTdMode = models.MarginModeIsolated
	executionPosModeNet        = "net_mode"
	executionPosModeLongShort  = "long_short_mode"
)

const (
	executionResultSuccess       = "success"
	executionResultFailed        = "failed"
	executionResultPartialFailed = "partial_failed"
	executionFailSourceLocal     = "local"
	executionFailSourceExchange  = "exchange"
)

var executionAttemptSeq atomic.Int64

type LiveStore interface {
	InsertExecutionOrder(record models.ExecutionOrderRecord) error
}

type LiveConfig struct {
	Logger            *zap.Logger
	Exchanges         map[string]iface.Exchange
	PosMode           map[string]string
	DefaultMarginMode string
	PlaceTimeout      time.Duration
	Store             LiveStore
	Mode              string
	SingletonUUID     string
}

type Live struct {
	logger            *zap.Logger
	exchanges         map[string]iface.Exchange
	posMode           map[string]string
	defaultMarginMode string
	placeTimeout      time.Duration
	store             LiveStore
	mode              string
	singletonUUID     string
	started           atomic.Bool
}

func NewLive(cfg LiveConfig) *Live {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	defaultMarginMode := strings.ToLower(strings.TrimSpace(cfg.DefaultMarginMode))
	if defaultMarginMode == "" {
		defaultMarginMode = liveExecutionDefaultTdMode
	}
	placeTimeout := cfg.PlaceTimeout
	if placeTimeout <= 0 {
		placeTimeout = liveExecutionTimeout
	}
	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "live"
	}
	return &Live{
		logger:            logger,
		exchanges:         cloneExecutionExchangeMap(cfg.Exchanges),
		posMode:           cloneExecutionPosModeMap(cfg.PosMode),
		defaultMarginMode: defaultMarginMode,
		placeTimeout:      placeTimeout,
		store:             cfg.Store,
		mode:              mode,
		singletonUUID:     strings.TrimSpace(cfg.SingletonUUID),
	}
}

func (e *Live) Start(_ context.Context) error {
	if e == nil {
		return fmt.Errorf("nil execution live")
	}
	logger := e.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("execution live start",
		zap.Int("exchange_count", len(e.exchanges)),
		zap.Int("pos_mode_count", countExecutionPosMode(e.posMode)),
		zap.String("default_margin_mode", e.defaultMarginMode),
		zap.Duration("place_timeout", e.placeTimeout),
	)
	defer logger.Info("execution live started")
	e.started.Store(true)
	return nil
}

func (e *Live) Close() error {
	if e == nil {
		return nil
	}
	logger := e.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("execution live close")
	defer logger.Info("execution live closed")
	e.started.Store(false)
	return nil
}

func (e *Live) Place(decision models.Decision) (err error) {
	if decision.Action == "" || decision.Action == models.DecisionActionIgnore {
		return nil
	}
	startedAtMS := time.Now().UnixMilli()
	attemptID := newExecutionAttemptID(decision.Exchange, decision.Symbol, decision.Action)
	failStage := ""
	steps := make([]models.ExecutionStepResult, 0, 8)
	instID := ""
	exchangeOrderID := ""
	exchangeAlgoOrderID := ""
	hasSideEffect := false
	defer func() {
		record := models.ExecutionOrderRecord{
			AttemptID:          attemptID,
			SingletonUUID:      e.singletonUUID,
			Mode:               e.mode,
			Source:             strings.TrimSpace(decision.Strategy),
			Exchange:           decision.Exchange,
			Symbol:             decision.Symbol,
			InstID:             instID,
			Action:             decision.Action,
			OrderType:          decision.OrderType,
			PositionSide:       decision.PositionSide,
			MarginMode:         decision.MarginMode,
			Size:               decision.Size,
			LeverageMultiplier: decision.LeverageMultiplier,
			Price:              decision.Price,
			TakeProfitPrice:    decision.TakeProfitPrice,
			StopLossPrice:      decision.StopLossPrice,
			ClientOrderID:      strings.TrimSpace(decision.ClientOrderID),
			Strategy:           strings.TrimSpace(decision.Strategy),
			HasSideEffect:      hasSideEffect,
			StartedAtMS:        startedAtMS,
		}

		record.ResultStatus = executionResultSuccess
		if err != nil {
			record.ResultStatus = executionResultFailed
			if hasSideEffect {
				record.ResultStatus = executionResultPartialFailed
			}
			record.FailSource = classifyExecutionFailSource(err)
			record.FailStage = strings.TrimSpace(failStage)
			record.FailReason = err.Error()
			code, msg, orderIDFromErr, algoIDFromErr := parseExecutionExchangeError(err)
			record.ExchangeCode = code
			record.ExchangeMessage = msg
			if exchangeOrderID == "" {
				exchangeOrderID = orderIDFromErr
			}
			if exchangeAlgoOrderID == "" {
				exchangeAlgoOrderID = algoIDFromErr
			}
		}
		record.ExchangeOrderID = exchangeOrderID
		record.ExchangeAlgoOrderID = exchangeAlgoOrderID
		record.StepResultsJSON = marshalJSONOrEmpty(steps)
		record.RequestJSON = marshalJSONOrEmpty(decision)
		record.ResponseJSON = marshalJSONOrEmpty(map[string]string{
			"exchange_order_id":      exchangeOrderID,
			"exchange_algo_order_id": exchangeAlgoOrderID,
		})
		record.FinishedAtMS = time.Now().UnixMilli()
		record.DurationMS = record.FinishedAtMS - startedAtMS
		record.CreatedAtMS = startedAtMS
		record.UpdatedAtMS = record.FinishedAtMS
		if persistErr := e.persistExecutionRecord(record); persistErr != nil {
			logger := e.logger
			if logger == nil {
				logger = glog.Nop()
			}
			logger.Error("execution persist order failed",
				zap.String("attempt_id", attemptID),
				zap.String("exchange", decision.Exchange),
				zap.String("symbol", decision.Symbol),
				zap.String("action", decision.Action),
				zap.Error(persistErr),
			)
		}
	}()

	exchange := normalizeExecutionExchange(decision.Exchange)
	steps = appendExecutionStep(steps, "trade_enabled", nil)
	client, err := e.exchangeFor(decision.Exchange)
	if err != nil {
		failStage = "exchange_lookup"
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}
	steps = appendExecutionStep(steps, "exchange_lookup", nil)
	instID, err = normalizeOrderInstID(client, decision.Symbol)
	if err != nil {
		failStage = "normalize_symbol"
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}
	steps = appendExecutionStep(steps, "normalize_symbol", nil)
	ctx, cancel := context.WithTimeout(context.Background(), e.placeTimeout)
	defer cancel()
	inst, err := client.GetInstrument(ctx, instID)
	if err != nil {
		failStage = "load_instrument"
		err = fmt.Errorf("execution live: load instrument %s failed: %w", instID, err)
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}
	steps = appendExecutionStep(steps, "load_instrument", nil)
	posMode, err := e.resolvePositionMode(ctx, client, exchange)
	if err != nil {
		failStage = "position_mode"
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}
	steps = appendExecutionStep(steps, "position_mode", nil)

	switch decision.Action {
	case models.DecisionActionOpenLong, models.DecisionActionOpenShort:
		failStage = "place_open"
		var algoID string
		hasSideEffect, exchangeOrderID, algoID, err = e.placeOpen(ctx, client, instID, inst, decision, posMode)
		if exchangeAlgoOrderID == "" {
			exchangeAlgoOrderID = algoID
		}
		steps = appendExecutionStep(steps, failStage, err)
		return err
	case models.DecisionActionClose:
		failStage = "place_close"
		hasSideEffect, exchangeOrderID, err = e.placeClose(ctx, client, instID, inst, decision, posMode)
		steps = appendExecutionStep(steps, failStage, err)
		return err
	case models.DecisionActionUpdate:
		failStage = "place_tpsl"
		hasSideEffect, exchangeAlgoOrderID, err = e.placeTPSL(ctx, client, instID, inst, decision, posMode)
		steps = appendExecutionStep(steps, failStage, err)
		return err
	default:
		failStage = "unsupported_action"
		err = fmt.Errorf("execution live: unsupported decision action %s", decision.Action)
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}
}

func (e *Live) placeOpen(ctx context.Context, client iface.Exchange, instID string, inst iface.Instrument, decision models.Decision, posMode string) (hasSideEffect bool, orderID string, algoID string, err error) {
	side, openPosSide, err := sideFromOpenAction(decision.Action)
	if err != nil {
		return false, "", "", err
	}
	reqPosSide := posSideByMode(posMode, openPosSide)
	size, err := normalizeOrderSize(decision.Size, inst, true)
	if err != nil {
		return false, "", "", err
	}
	marginMode := normalizeTdMode(decision.MarginMode, e.defaultMarginMode)
	if decision.LeverageMultiplier > 0 {
		leverage := int(math.Round(decision.LeverageMultiplier))
		if leverage < 1 {
			leverage = 1
		}
		if err := client.SetLeverage(ctx, instID, marginMode, leverage, reqPosSide); err != nil {
			return false, "", "", err
		}
		hasSideEffect = true
	}

	req := iface.OrderRequest{
		InstID:        instID,
		TdMode:        marginMode,
		Side:          side,
		PosSide:       reqPosSide,
		Sz:            formatOrderNumber(size),
		ReduceOnly:    false,
		ClientOrderID: ensureClientOrderID(decision.ClientOrderID, decision.Exchange, decision.Symbol, decision.Action),
	}
	orderType := normalizeDecisionOrderType(decision.OrderType)
	req.OrdType = orderType
	if orderType == models.OrderTypeLimit {
		if decision.Price <= 0 {
			return false, "", "", fmt.Errorf("execution live: invalid limit order price")
		}
		req.Px = formatOrderNumber(decision.Price)
	}
	if supportsAttachAlgoOrders(client) {
		req.AttachAlgoOrds = buildAttachAlgoOrders(decision)
	}
	orderID, err = client.PlaceOrder(ctx, req)
	if err != nil {
		return false, "", "", err
	}
	hasSideEffect = true
	if len(req.AttachAlgoOrds) == 0 && (decision.StopLossPrice > 0 || decision.TakeProfitPrice > 0) {
		var tpslSideEffect bool
		tpslSideEffect, algoID, err = e.placeTPSL(ctx, client, instID, inst, models.Decision{
			Exchange:           decision.Exchange,
			Symbol:             decision.Symbol,
			Timeframe:          decision.Timeframe,
			Action:             models.DecisionActionUpdate,
			Strategy:           decision.Strategy,
			PositionSide:       openPosSide,
			MarginMode:         marginMode,
			Size:               size,
			LeverageMultiplier: decision.LeverageMultiplier,
			StopLossPrice:      decision.StopLossPrice,
			TakeProfitPrice:    decision.TakeProfitPrice,
			ClientOrderID:      decision.ClientOrderID,
		}, posMode)
		hasSideEffect = hasSideEffect || tpslSideEffect
		if err != nil {
			return hasSideEffect, orderID, algoID, err
		}
	}
	return hasSideEffect, orderID, algoID, nil
}

func (e *Live) placeClose(ctx context.Context, client iface.Exchange, instID string, inst iface.Instrument, decision models.Decision, posMode string) (hasSideEffect bool, orderID string, err error) {
	positionSide := normalizeClosePositionSide(decision.PositionSide)
	if positionSide == "" {
		return false, "", fmt.Errorf("execution live: invalid position side for close")
	}
	size, err := normalizeOrderSize(decision.Size, inst, false)
	if err != nil {
		return false, "", fmt.Errorf("execution live: invalid close size: %w", err)
	}
	side := "sell"
	if positionSide == "short" {
		side = "buy"
	}
	refreshRemainderTPSL := strings.EqualFold(strings.TrimSpace(decision.CloseReason), "signal_partial_close") &&
		(decision.StopLossPrice > 0 || decision.TakeProfitPrice > 0)
	positionSizeBefore := 0.0
	if refreshRemainderTPSL {
		positionSizeBefore, err = resolvePositionTPSLSize(ctx, client, instID, positionSide)
		if err != nil {
			return false, "", err
		}
	}
	req := iface.OrderRequest{
		InstID:        instID,
		TdMode:        normalizeTdMode(decision.MarginMode, e.defaultMarginMode),
		Side:          side,
		PosSide:       posSideByMode(posMode, positionSide),
		OrdType:       "market",
		Sz:            formatOrderNumber(size),
		ReduceOnly:    true,
		ClientOrderID: ensureClientOrderID(decision.ClientOrderID, decision.Exchange, decision.Symbol, decision.Action),
	}
	orderID, err = client.PlaceOrder(ctx, req)
	if err != nil {
		return false, "", err
	}
	hasSideEffect = true
	if !refreshRemainderTPSL || !floatGTExecution(positionSizeBefore, size) {
		return hasSideEffect, orderID, nil
	}
	hasRemaining, err := waitForRemainingPositionAfterClose(ctx, client, instID, positionSide, positionSizeBefore)
	if err != nil {
		return hasSideEffect, orderID, err
	}
	if !hasRemaining {
		return hasSideEffect, orderID, nil
	}
	tpslSideEffect, _, err := e.placeTPSL(ctx, client, instID, inst, decision, posMode)
	hasSideEffect = hasSideEffect || tpslSideEffect
	if err != nil {
		return hasSideEffect, orderID, err
	}
	return hasSideEffect, orderID, nil
}

func (e *Live) placeTPSL(ctx context.Context, client iface.Exchange, instID string, inst iface.Instrument, decision models.Decision, posMode string) (hasSideEffect bool, algoID string, err error) {
	manager, ok := client.(iface.TPSLManager)
	if !ok {
		if decision.TakeProfitPrice <= 0 && decision.StopLossPrice <= 0 {
			return false, "", nil
		}
		return false, "", fmt.Errorf("execution live: exchange does not support TPSL")
	}

	openOrders, err := manager.GetOpenTPSLOrders(ctx, instID)
	if err != nil {
		return false, "", err
	}
	positionSide := normalizeClosePositionSide(decision.PositionSide)
	if positionSide == "" {
		return false, "", fmt.Errorf("execution live: invalid position side for TPSL")
	}
	positionOrders := filterTPSLOrdersByPositionSide(openOrders, positionSide)

	if decision.TakeProfitPrice <= 0 && decision.StopLossPrice <= 0 {
		canceled, err := cancelOpenTPSLOrders(ctx, manager, instID, positionOrders)
		if err != nil {
			return canceled, "", err
		}
		return canceled, "", nil
	}
	side := "sell"
	if positionSide == "short" {
		side = "buy"
	}
	sizeValue, err := resolvePositionTPSLSize(ctx, client, instID, positionSide)
	if err != nil {
		return false, "", err
	}
	size, err := normalizeOrderSize(sizeValue, inst, false)
	if err != nil {
		return false, "", fmt.Errorf("execution live: invalid TPSL size: %w", err)
	}
	refPrice := decision.Price
	if refPrice <= 0 {
		if tickerPrice, tickerErr := client.GetTickerPrice(ctx, instID); tickerErr == nil {
			refPrice = tickerPrice
		}
	}
	if err := validateTPSLAgainstPriceForExecution(positionSide, refPrice, decision.TakeProfitPrice, decision.StopLossPrice); err != nil {
		return false, "", err
	}
	canceled, err := cancelOpenTPSLOrders(ctx, manager, instID, positionOrders)
	if err != nil {
		return canceled, "", err
	}
	tpTriggerPx := formatTPSLTrigger(decision.TakeProfitPrice)
	slTriggerPx := formatTPSLTrigger(decision.StopLossPrice)
	algoID, err = manager.PlaceTPSLOrder(ctx, iface.TPSLOrderRequest{
		InstID:        instID,
		TdMode:        normalizeTdMode(decision.MarginMode, e.defaultMarginMode),
		Side:          side,
		PosSide:       posSideByMode(posMode, positionSide),
		Sz:            formatOrderNumber(size),
		TPTriggerPx:   tpTriggerPx,
		SLTriggerPx:   slTriggerPx,
		ReduceOnly:    true,
		ClientOrderID: ensureClientOrderID(decision.ClientOrderID, decision.Exchange, decision.Symbol, "tpsl"),
	})
	if err != nil {
		return canceled, "", err
	}
	return true, algoID, nil
}

func cancelOpenTPSLOrders(ctx context.Context, manager iface.TPSLManager, instID string, openOrders []iface.TPSLOrder) (bool, error) {
	if len(openOrders) == 0 {
		return false, nil
	}
	reqs := make([]iface.CancelTPSLOrderRequest, 0, len(openOrders))
	for _, item := range openOrders {
		orderID := strings.TrimSpace(item.OrderID)
		if orderID == "" {
			continue
		}
		reqs = append(reqs, iface.CancelTPSLOrderRequest{OrderID: orderID, InstID: instID})
	}
	if len(reqs) == 0 {
		return false, nil
	}
	if err := manager.CancelTPSLOrders(ctx, reqs); err != nil {
		return false, err
	}
	return true, nil
}

func buildAttachAlgoOrders(decision models.Decision) []iface.AttachAlgoOrder {
	if decision.TakeProfitPrice <= 0 && decision.StopLossPrice <= 0 {
		return nil
	}
	order := iface.AttachAlgoOrder{}
	if tp := formatTPSLTrigger(decision.TakeProfitPrice); tp != "" {
		order.TPTriggerPx = tp
		order.TPOrdPx = "-1"
	}
	if sl := formatTPSLTrigger(decision.StopLossPrice); sl != "" {
		order.SLTriggerPx = sl
		order.SLOrdPx = "-1"
	}
	if order.TPTriggerPx == "" && order.SLTriggerPx == "" {
		return nil
	}
	return []iface.AttachAlgoOrder{order}
}

func supportsAttachAlgoOrders(client iface.Exchange) bool {
	support, ok := client.(iface.AttachAlgoOrderSupport)
	return ok && support.SupportsAttachAlgoOrders()
}

func filterTPSLOrdersByPositionSide(orders []iface.TPSLOrder, positionSide string) []iface.TPSLOrder {
	if len(orders) == 0 {
		return nil
	}
	filtered := make([]iface.TPSLOrder, 0, len(orders))
	for _, order := range orders {
		side := normalizeClosePositionSide(order.PosSide)
		if side == "" {
			side = inferTPSLOrderPositionSide(order.Side)
		}
		if side != positionSide {
			continue
		}
		filtered = append(filtered, order)
	}
	return filtered
}

func resolvePositionTPSLSize(ctx context.Context, client iface.Exchange, instID, positionSide string) (float64, error) {
	rows, err := client.GetPositions(ctx, instID)
	if err != nil {
		return 0, fmt.Errorf("execution live: load current positions for TPSL failed: %w", err)
	}
	total := 0.0
	targetInstID := strings.ToUpper(strings.TrimSpace(instID))
	for _, row := range rows {
		if targetInstID != "" && strings.ToUpper(strings.TrimSpace(row.InstID)) != targetInstID {
			continue
		}
		if executionPositionSide(row) != positionSide {
			continue
		}
		total += math.Abs(parseExecutionFloat(row.Pos))
	}
	if total <= 0 {
		return 0, fmt.Errorf("execution live: current position size unavailable for TPSL")
	}
	return total, nil
}

func waitForRemainingPositionAfterClose(ctx context.Context, client iface.Exchange, instID, positionSide string, previousSize float64) (bool, error) {
	if !floatGTExecution(previousSize, 0) {
		return false, nil
	}
	ticker := time.NewTicker(liveExecutionPollInterval)
	defer ticker.Stop()
	for {
		currentSize, err := resolvePositionTPSLSize(ctx, client, instID, positionSide)
		if err == nil {
			if !floatGTExecution(currentSize, 0) {
				return false, nil
			}
			if currentSize < previousSize {
				return true, nil
			}
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("execution live: wait remaining position after partial close failed: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func floatGTExecution(a, b float64) bool {
	return a > b+1e-9
}

func executionPositionSide(position iface.Position) string {
	if side := normalizeClosePositionSide(position.PosSide); side != "" {
		return side
	}
	pos := parseExecutionFloat(position.Pos)
	switch {
	case pos > 0:
		return "long"
	case pos < 0:
		return "short"
	default:
		return ""
	}
}

func inferTPSLOrderPositionSide(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case "sell":
		return "long"
	case "buy":
		return "short"
	default:
		return ""
	}
}

func parseExecutionFloat(raw string) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0
	}
	return value
}

func validateTPSLAgainstPriceForExecution(posSide string, refPrice, tp, sl float64) error {
	if refPrice <= 0 {
		return nil
	}
	switch posSide {
	case "long":
		if tp > 0 && tp <= refPrice {
			return fmt.Errorf("execution live: TP for long must be greater than reference price %.8f", refPrice)
		}
		if sl > 0 && sl >= refPrice {
			return fmt.Errorf("execution live: SL for long must be less than reference price %.8f", refPrice)
		}
	case "short":
		if tp > 0 && tp >= refPrice {
			return fmt.Errorf("execution live: TP for short must be less than reference price %.8f", refPrice)
		}
		if sl > 0 && sl <= refPrice {
			return fmt.Errorf("execution live: SL for short must be greater than reference price %.8f", refPrice)
		}
	}
	return nil
}

func formatTPSLTrigger(price float64) string {
	if price <= 0 {
		return ""
	}
	return formatOrderNumber(price)
}

func normalizeOrderSize(size float64, inst iface.Instrument, enforceMin bool) (float64, error) {
	if size <= 0 {
		return 0, fmt.Errorf("size must be greater than zero")
	}
	if inst.LotSz > 0 {
		size = core.FloorToStep(size, inst.LotSz)
	}
	if size <= 0 {
		return 0, fmt.Errorf("size %.8f is below lot size %.8f", size, inst.LotSz)
	}
	if enforceMin && inst.MinSz > 0 && size < inst.MinSz {
		return 0, fmt.Errorf("size %.8f is below min size %.8f", size, inst.MinSz)
	}
	return size, nil
}

func (e *Live) exchangeFor(exchange string) (iface.Exchange, error) {
	if e == nil {
		return nil, fmt.Errorf("execution live: nil executor")
	}
	if len(e.exchanges) == 0 {
		return nil, fmt.Errorf("execution live: trade exchange map is empty")
	}
	key := normalizeExecutionExchange(exchange)
	ex, ok := e.exchanges[key]
	if !ok || ex == nil {
		return nil, fmt.Errorf("execution live: exchange not configured: %s", key)
	}
	return ex, nil
}

func cloneExecutionExchangeMap(input map[string]iface.Exchange) map[string]iface.Exchange {
	if len(input) == 0 {
		return map[string]iface.Exchange{}
	}
	out := make(map[string]iface.Exchange, len(input))
	for name, ex := range input {
		key := normalizeExecutionExchange(name)
		if key == "" || ex == nil {
			continue
		}
		out[key] = ex
	}
	return out
}

func cloneExecutionPosModeMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(input))
	for name, mode := range input {
		key := normalizeExecutionExchange(name)
		if key == "" {
			continue
		}
		normalized := normalizeExecutionPosMode(mode)
		if normalized == "" {
			continue
		}
		out[key] = normalized
	}
	return out
}

func countExecutionPosMode(input map[string]string) int {
	return len(input)
}

func (e *Live) resolvePositionMode(ctx context.Context, client iface.Exchange, exchange string) (string, error) {
	if e == nil {
		return "", fmt.Errorf("execution live: nil executor")
	}
	exchange = normalizeExecutionExchange(exchange)
	desired := normalizeExecutionPosMode(e.posMode[exchange])
	reader, hasReader := client.(iface.PositionModeReader)
	if desired != "" {
		if hasReader {
			currentRaw, err := reader.GetPositionMode(ctx)
			if err != nil {
				return "", fmt.Errorf("execution live: get position mode failed: %w", err)
			}
			current := normalizeExecutionPosMode(currentRaw)
			if current == "" {
				return "", fmt.Errorf("execution live: unknown account pos_mode: %s", strings.TrimSpace(currentRaw))
			}
			if current != desired {
				if err := client.SetPositionMode(ctx, desired); err != nil {
					return "", fmt.Errorf("execution live: set position mode to %s failed: %w", desired, err)
				}
				return desired, nil
			}
			return current, nil
		}
		if err := client.SetPositionMode(ctx, desired); err != nil {
			return "", fmt.Errorf("execution live: set position mode to %s failed: %w", desired, err)
		}
		return desired, nil
	}
	if hasReader {
		currentRaw, err := reader.GetPositionMode(ctx)
		if err != nil {
			return "", fmt.Errorf("execution live: get position mode failed: %w", err)
		}
		current := normalizeExecutionPosMode(currentRaw)
		if current == "" {
			return "", fmt.Errorf("execution live: unknown account pos_mode: %s", strings.TrimSpace(currentRaw))
		}
		return current, nil
	}
	return executionPosModeLongShort, nil
}

func normalizeExecutionPosMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "net", "net_mode":
		return executionPosModeNet
	case "long_short", "long_short_mode":
		return executionPosModeLongShort
	default:
		return ""
	}
}

func posSideByMode(posMode, positionSide string) string {
	if normalizeExecutionPosMode(posMode) != executionPosModeLongShort {
		return ""
	}
	return normalizeClosePositionSide(positionSide)
}

func normalizeExecutionExchange(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}

func normalizeOrderInstID(client iface.Exchange, symbol string) (string, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return "", fmt.Errorf("execution live: symbol is required")
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
		return "", fmt.Errorf("execution live: normalize symbol %s failed: %w", symbol, err)
	}
	return instID, nil
}

func sideFromOpenAction(action string) (side string, posSide string, err error) {
	switch action {
	case models.DecisionActionOpenLong:
		return "buy", "long", nil
	case models.DecisionActionOpenShort:
		return "sell", "short", nil
	default:
		return "", "", fmt.Errorf("execution live: invalid open action %s", action)
	}
}

func normalizeClosePositionSide(side string) string {
	side = strings.ToLower(strings.TrimSpace(side))
	switch side {
	case "long", "short":
		return side
	default:
		return ""
	}
}

func normalizeTdMode(mode, fallback string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode != "" {
		return mode
	}
	fallback = strings.ToLower(strings.TrimSpace(fallback))
	if fallback != "" {
		return fallback
	}
	return liveExecutionDefaultTdMode
}

func ensureClientOrderID(current, exchange, symbol, suffix string) string {
	current = strings.TrimSpace(current)
	if isValidClientOrderID(current) {
		return current
	}
	if current != "" {
		current = normalizeClientOrderID(current)
		if isValidClientOrderID(current) {
			return current
		}
	}
	return buildExecutionClientOrderID("exec", exchange, symbol, suffix)
}

func buildExecutionClientOrderID(prefix, exchange, symbol, suffix string) string {
	const maxLen = 32
	prefix = strings.ToLower(sanitizeClientOrderToken(prefix))
	if prefix == "" {
		prefix = "x"
	}
	exchange = strings.ToLower(sanitizeClientOrderToken(normalizeExecutionExchange(exchange)))
	if exchange == "" {
		exchange = "x"
	}
	symbol = strings.ToLower(sanitizeClientOrderToken(symbol))
	if symbol == "" {
		symbol = "x"
	}
	suffix = strings.ToLower(sanitizeClientOrderToken(suffix))
	if suffix == "" {
		suffix = "order"
	}
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

func isValidClientOrderID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		default:
			return false
		}
	}
	return true
}

func normalizeClientOrderID(raw string) string {
	raw = sanitizeClientOrderToken(raw)
	if len(raw) > 32 {
		raw = raw[:32]
	}
	return raw
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

func formatOrderNumber(value float64) string {
	if value <= 0 {
		return ""
	}
	text := strconv.FormatFloat(value, 'f', 10, 64)
	text = strings.TrimRight(text, "0")
	text = strings.TrimRight(text, ".")
	if text == "" {
		return "0"
	}
	return text
}

func normalizeDecisionOrderType(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), models.OrderTypeLimit) {
		return models.OrderTypeLimit
	}
	return models.OrderTypeMarket
}

func (e *Live) persistExecutionRecord(record models.ExecutionOrderRecord) error {
	if e == nil || e.store == nil {
		return nil
	}
	return e.store.InsertExecutionOrder(record)
}

func appendExecutionStep(steps []models.ExecutionStepResult, stage string, err error) []models.ExecutionStepResult {
	step := models.ExecutionStepResult{
		Stage:   strings.TrimSpace(stage),
		Success: err == nil,
	}
	if err != nil {
		step.Error = err.Error()
	}
	return append(steps, step)
}

func marshalJSONOrEmpty(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(raw)
}

func newExecutionAttemptID(exchange, symbol, action string) string {
	const maxLen = 96
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" {
		exchange = "unknown"
	}
	symbol = strings.ToLower(strings.TrimSpace(symbol))
	if symbol == "" {
		symbol = "unknown"
	}
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = "unknown"
	}
	normalizedSymbol := strings.ReplaceAll(symbol, "/", "_")
	normalizedSymbol = strings.ReplaceAll(normalizedSymbol, ":", "_")
	normalizedSymbol = strings.ReplaceAll(normalizedSymbol, ".", "_")
	normalizedAction := strings.ReplaceAll(action, " ", "_")
	seq := executionAttemptSeq.Add(1)
	id := fmt.Sprintf("exec_%s_%s_%s_%d_%d", exchange, normalizedSymbol, normalizedAction, time.Now().UnixNano(), seq)
	if len(id) > maxLen {
		id = id[:maxLen]
	}
	return id
}

func classifyExecutionFailSource(err error) string {
	if err == nil {
		return ""
	}
	parsedCode, _, _, _ := parseExecutionExchangeError(err)
	if parsedCode != "" {
		return executionFailSourceExchange
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(msg, "okx error ") || strings.Contains(msg, "binance error ") || strings.Contains(msg, "bitget error ") {
		return executionFailSourceExchange
	}
	return executionFailSourceLocal
}

func parseExecutionExchangeError(err error) (code string, message string, orderID string, algoID string) {
	if err == nil {
		return "", "", "", ""
	}
	text := strings.TrimSpace(err.Error())
	if text == "" {
		return "", "", "", ""
	}
	code = extractExecutionErrorSegment(text, "code=", " msg=")
	message = extractExecutionErrorMessage(text)
	dataRaw := extractExecutionErrorData(text)
	if strings.TrimSpace(dataRaw) == "" {
		return strings.TrimSpace(code), strings.TrimSpace(message), "", ""
	}
	var rows []map[string]any
	if jsonErr := json.Unmarshal([]byte(dataRaw), &rows); jsonErr == nil {
		for _, row := range rows {
			if sCode, ok := row["sCode"].(string); ok && strings.TrimSpace(sCode) != "" {
				code = strings.TrimSpace(sCode)
			}
			if sMsg, ok := row["sMsg"].(string); ok && strings.TrimSpace(sMsg) != "" {
				message = strings.TrimSpace(sMsg)
			}
			if orderID == "" {
				if ordID, ok := row["ordId"].(string); ok && strings.TrimSpace(ordID) != "" {
					orderID = strings.TrimSpace(ordID)
				}
			}
			if algoID == "" {
				if parsedAlgoID, ok := row["algoId"].(string); ok && strings.TrimSpace(parsedAlgoID) != "" {
					algoID = strings.TrimSpace(parsedAlgoID)
				}
			}
			if code != "" && message != "" && orderID != "" && algoID != "" {
				break
			}
		}
	}
	return strings.TrimSpace(code), strings.TrimSpace(message), strings.TrimSpace(orderID), strings.TrimSpace(algoID)
}

func extractExecutionErrorSegment(text, startFlag, endFlag string) string {
	startIdx := strings.Index(text, startFlag)
	if startIdx < 0 {
		return ""
	}
	start := startIdx + len(startFlag)
	end := len(text)
	if endFlag != "" {
		endIdx := strings.Index(text[start:], endFlag)
		if endIdx >= 0 {
			end = start + endIdx
		}
	}
	if start >= end || start >= len(text) {
		return ""
	}
	return strings.TrimSpace(text[start:end])
}

func extractExecutionErrorMessage(text string) string {
	msg := extractExecutionErrorSegment(text, "msg=", " data=")
	if msg != "" {
		return msg
	}
	return extractExecutionErrorSegment(text, "msg=", "")
}

func extractExecutionErrorData(text string) string {
	start := strings.Index(text, " data=")
	if start < 0 {
		return ""
	}
	return strings.TrimSpace(text[start+len(" data="):])
}

var _ iface.Executor = (*Live)(nil)
