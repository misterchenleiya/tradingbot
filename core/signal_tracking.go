package core

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

func trackedSignalsForUpdate(signals []models.Signal) []models.Signal {
	if len(signals) == 0 {
		return nil
	}
	out := make([]models.Signal, 0, len(signals))
	for _, signal := range signals {
		if models.IsEmptySignal(signal) {
			continue
		}
		out = append(out, signal)
	}
	return out
}

func filterSignalsByComboKey(signals []models.Signal, comboKey string) []models.Signal {
	comboKey = strings.TrimSpace(comboKey)
	if comboKey == "" || len(signals) == 0 {
		return nil
	}
	out := make([]models.Signal, 0, len(signals))
	for _, signal := range signals {
		if models.IsEmptySignal(signal) {
			continue
		}
		_, _, normalized := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
		if normalized == comboKey {
			out = append(out, signal)
		}
	}
	return out
}

func comboKeySet(combos []strategyComboSpec) map[string]struct{} {
	if len(combos) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(combos))
	for _, combo := range combos {
		key := strings.TrimSpace(combo.ComboKey)
		if key == "" {
			continue
		}
		out[key] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func partitionSignalsByAllowedCombos(signals []models.Signal, allowed map[string]struct{}) ([]models.Signal, []models.Signal) {
	if len(signals) == 0 {
		return nil, nil
	}
	valid := make([]models.Signal, 0, len(signals))
	stale := make([]models.Signal, 0, len(signals))
	for _, signal := range signals {
		if models.IsEmptySignal(signal) {
			continue
		}
		_, _, comboKey := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
		if comboKey == "" {
			stale = append(stale, signal)
			continue
		}
		if _, ok := allowed[comboKey]; ok {
			valid = append(valid, signal)
			continue
		}
		stale = append(stale, signal)
	}
	if len(valid) == 0 {
		valid = nil
	}
	if len(stale) == 0 {
		stale = nil
	}
	return valid, stale
}
