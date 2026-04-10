package core

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type signalExecutionResult struct {
	Applied        bool
	Result         string
	HadPosition    bool
	RiskError      string
	ExecutionError string
	Decision       models.Decision
	RiskEvaluated  bool
	ExecutionTried bool
}

type executionResultNotifier interface {
	NotifyExecutionResult(decision models.Decision, execErr error)
}

func notifyExecutionResult(evaluator iface.Evaluator, decision models.Decision, execErr error) {
	if evaluator == nil {
		return
	}
	notifier, ok := evaluator.(executionResultNotifier)
	if !ok {
		return
	}
	notifier.NotifyExecutionResult(decision, execErr)
}

func executeSignalAction(
	evaluator iface.Evaluator,
	executor iface.Executor,
	accountState any,
	data models.MarketData,
	signal models.Signal,
) signalExecutionResult {
	result := signalExecutionResult{Result: "ignored"}
	if evaluator == nil {
		result.Result = "risk_disabled"
		return result
	}
	if strings.TrimSpace(signal.Exchange) == "" || strings.TrimSpace(signal.Symbol) == "" || strings.TrimSpace(signal.Timeframe) == "" {
		return result
	}

	positions, err := evaluator.ListOpenPositions(signal.Exchange, signal.Symbol, signal.Timeframe)
	if err != nil {
		result.RiskEvaluated = true
		result.Result = "risk_rejected"
		result.RiskError = err.Error()
		return result
	}
	position := models.Position{}
	hasPosition := len(positions) > 0
	result.HadPosition = hasPosition
	if hasPosition {
		position = positions[0]
	}

	var decision models.Decision
	result.RiskEvaluated = true
	if signal.Action == 8 && !hasPosition {
		decision, err = evaluator.EvaluateOpenBatch([]models.Signal{signal}, accountState)
	} else {
		decision, err = evaluator.EvaluateUpdate(signal, position, accountState)
	}
	result.Decision = decision
	if err != nil {
		result.Result = "risk_rejected"
		result.RiskError = err.Error()
		return result
	}
	if decision.Action == "" || decision.Action == models.DecisionActionIgnore {
		return result
	}

	if executor != nil {
		result.ExecutionTried = true
		if err := executor.Place(decision); err != nil {
			notifyExecutionResult(evaluator, decision, err)
			result.Result = "execution_failed"
			result.ExecutionError = err.Error()
			return result
		}
		notifyExecutionResult(evaluator, decision, nil)
	}
	result.Applied = true
	result.Result = decisionExecResult(decision.Action)
	return result
}

func decisionExecResult(action string) string {
	switch action {
	case models.DecisionActionOpenLong, models.DecisionActionOpenShort:
		return "open_filled"
	case models.DecisionActionClose:
		return "close_filled"
	case models.DecisionActionUpdate:
		return "update_filled"
	default:
		return action
	}
}

func normalizeSignalForRuntime(next models.Signal, current models.Signal, data models.MarketData) models.Signal {
	next.Exchange = firstNonEmpty(next.Exchange, current.Exchange, data.Exchange)
	next.Symbol = firstNonEmpty(next.Symbol, current.Symbol, data.Symbol)
	next.Strategy = firstNonEmpty(next.Strategy, current.Strategy)
	next.Timeframe = firstNonEmpty(next.Timeframe, current.Timeframe, data.Timeframe)
	return next
}
