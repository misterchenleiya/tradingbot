package execution

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type BackTestStore interface {
	InsertExecutionOrder(record models.ExecutionOrderRecord) error
}

type BackTestConfig struct {
	Logger        *zap.Logger
	Store         BackTestStore
	Mode          string
	SingletonUUID string
}

type BackTest struct {
	logger        *zap.Logger
	store         BackTestStore
	mode          string
	singletonUUID string
	started       atomic.Bool
}

func NewBackTest(cfg BackTestConfig) *BackTest {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	mode := strings.TrimSpace(cfg.Mode)
	if mode == "" {
		mode = "back-test"
	}
	return &BackTest{
		logger:        logger,
		store:         cfg.Store,
		mode:          mode,
		singletonUUID: strings.TrimSpace(cfg.SingletonUUID),
	}
}

func (e *BackTest) Start(_ context.Context) error {
	if e == nil {
		return fmt.Errorf("nil execution back-test")
	}
	logger := e.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("execution back-test start",
		zap.String("mode", e.mode),
	)
	defer logger.Info("execution back-test started")
	e.started.Store(true)
	return nil
}

func (e *BackTest) Close() error {
	if e == nil {
		return nil
	}
	logger := e.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("execution back-test close")
	defer logger.Info("execution back-test closed")
	e.started.Store(false)
	return nil
}

func (e *BackTest) Place(decision models.Decision) (err error) {
	if decision.Action == "" || decision.Action == models.DecisionActionIgnore {
		return nil
	}
	if e == nil {
		return fmt.Errorf("execution back-test: nil executor")
	}

	startedAtMS := time.Now().UnixMilli()
	attemptID := newExecutionAttemptID(decision.Exchange, decision.Symbol, decision.Action)
	failStage := ""
	steps := make([]models.ExecutionStepResult, 0, 6)
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
			InstID:             "",
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
			record.FailSource = executionFailSourceLocal
			record.FailStage = strings.TrimSpace(failStage)
			record.FailReason = err.Error()
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
			logger.Error("execution back-test persist order failed",
				zap.String("attempt_id", attemptID),
				zap.String("exchange", decision.Exchange),
				zap.String("symbol", decision.Symbol),
				zap.String("action", decision.Action),
				zap.Error(persistErr),
			)
		}
	}()

	steps = appendExecutionStep(steps, "trade_enabled_ignored", nil)

	switch decision.Action {
	case models.DecisionActionOpenLong, models.DecisionActionOpenShort:
		failStage = "validate_open"
		err = validateBackTestOpenDecision(decision)
		steps = appendExecutionStep(steps, failStage, err)
		if err != nil {
			return err
		}
		exchangeOrderID = "bt-open-" + strconvBase36(time.Now().UnixNano())
		if decision.TakeProfitPrice > 0 || decision.StopLossPrice > 0 {
			exchangeAlgoOrderID = "bt-algo-" + strconvBase36(time.Now().UnixNano())
		}
		hasSideEffect = true
	case models.DecisionActionClose:
		failStage = "validate_close"
		err = validateBackTestCloseDecision(decision)
		steps = appendExecutionStep(steps, failStage, err)
		if err != nil {
			return err
		}
		exchangeOrderID = "bt-close-" + strconvBase36(time.Now().UnixNano())
		hasSideEffect = true
	case models.DecisionActionUpdate:
		failStage = "validate_update"
		err = validateBackTestUpdateDecision(decision)
		steps = appendExecutionStep(steps, failStage, err)
		if err != nil {
			return err
		}
		exchangeAlgoOrderID = "bt-update-" + strconvBase36(time.Now().UnixNano())
		hasSideEffect = true
	default:
		failStage = "unsupported_action"
		err = fmt.Errorf("execution back-test: unsupported decision action %s", decision.Action)
		steps = appendExecutionStep(steps, failStage, err)
		return err
	}

	e.logger.Debug("execution back-test place",
		zap.String("exchange", decision.Exchange),
		zap.String("symbol", decision.Symbol),
		zap.String("timeframe", decision.Timeframe),
		zap.String("action", decision.Action),
		zap.String("side", decision.PositionSide),
		zap.Float64("size", decision.Size),
		zap.Float64("price", decision.Price),
	)
	return nil
}

func (e *BackTest) persistExecutionRecord(record models.ExecutionOrderRecord) error {
	if e == nil || e.store == nil {
		return nil
	}
	return e.store.InsertExecutionOrder(record)
}

func validateBackTestOpenDecision(decision models.Decision) error {
	if decision.Size <= 0 {
		return fmt.Errorf("execution back-test: open size must be greater than zero")
	}
	orderType := normalizeDecisionOrderType(decision.OrderType)
	if orderType == models.OrderTypeLimit && decision.Price <= 0 {
		return fmt.Errorf("execution back-test: invalid limit order price")
	}
	if decision.LeverageMultiplier <= 0 {
		return fmt.Errorf("execution back-test: leverage must be greater than zero")
	}
	_, _, err := sideFromOpenAction(decision.Action)
	return err
}

func validateBackTestCloseDecision(decision models.Decision) error {
	if decision.Size <= 0 {
		return fmt.Errorf("execution back-test: close size must be greater than zero")
	}
	if normalizeClosePositionSide(decision.PositionSide) == "" {
		return fmt.Errorf("execution back-test: invalid position side for close")
	}
	return nil
}

func validateBackTestUpdateDecision(decision models.Decision) error {
	if normalizeClosePositionSide(decision.PositionSide) == "" {
		return fmt.Errorf("execution back-test: invalid position side for update")
	}
	if decision.TakeProfitPrice <= 0 && decision.StopLossPrice <= 0 {
		return fmt.Errorf("execution back-test: TP/SL update requires at least one trigger price")
	}
	return nil
}

func strconvBase36(value int64) string {
	if value < 0 {
		value = -value
	}
	const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
	if value == 0 {
		return "0"
	}
	buf := make([]byte, 0, 16)
	for value > 0 {
		buf = append(buf, alphabet[value%36])
		value /= 36
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

var _ iface.Executor = (*BackTest)(nil)
