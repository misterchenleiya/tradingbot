package core

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/iface"
)

type pairPositionChecker interface {
	HasOpenPosition(exchange, symbol string) (bool, error)
}

func hasOpenPositionForPair(evaluator iface.Evaluator, exchange, symbol string) (bool, error) {
	if evaluator == nil {
		return false, nil
	}
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	if exchange == "" || symbol == "" {
		return false, nil
	}
	if checker, ok := evaluator.(pairPositionChecker); ok {
		return checker.HasOpenPosition(exchange, symbol)
	}
	positions, err := evaluator.ListOpenPositions(exchange, symbol, "")
	if err != nil {
		return false, err
	}
	return len(positions) > 0, nil
}

func openPositionComboKeysForPair(evaluator iface.Evaluator, exchange, symbol string) (map[string]struct{}, error) {
	if evaluator == nil {
		return nil, nil
	}
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	if exchange == "" || symbol == "" {
		return nil, nil
	}
	positions, err := evaluator.ListOpenPositions(exchange, symbol, "")
	if err != nil {
		return nil, err
	}
	if len(positions) == 0 {
		return nil, nil
	}
	out := make(map[string]struct{}, len(positions))
	for _, position := range positions {
		_, _, comboKey := common.NormalizeStrategyIdentity(position.Timeframe, position.StrategyTimeframes, position.ComboKey)
		if comboKey == "" {
			continue
		}
		out[comboKey] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
