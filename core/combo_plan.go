package core

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type strategyComboSpec struct {
	Timeframes         []string
	ComboKey           string
	ExecutionTimeframe string
	TradeEnabled       bool
}

func normalizeCoreStrategyCombos(values []models.StrategyComboConfig) []strategyComboSpec {
	if len(values) == 0 {
		return nil
	}
	out := make([]strategyComboSpec, 0, len(values))
	for _, combo := range values {
		_, timeframes, comboKey := common.NormalizeStrategyIdentity("", combo.Timeframes, "")
		if len(timeframes) == 0 || comboKey == "" {
			continue
		}
		out = append(out, strategyComboSpec{
			Timeframes:         append([]string(nil), timeframes...),
			ComboKey:           comboKey,
			ExecutionTimeframe: smallestConfiguredTimeframe(timeframes),
			TradeEnabled:       combo.TradeEnabled,
		})
	}
	return out
}

func triggeredStrategyCombos(combos []strategyComboSpec, pairTimeframes []string, eventTimeframe string) []strategyComboSpec {
	if len(combos) == 0 {
		return nil
	}
	eventTimeframe = strings.ToLower(strings.TrimSpace(eventTimeframe))
	if eventTimeframe == "" {
		return nil
	}
	allowed := allowedStrategyTimeframes(pairTimeframes)
	out := make([]strategyComboSpec, 0, len(combos))
	for _, combo := range combos {
		if combo.ExecutionTimeframe != eventTimeframe {
			continue
		}
		if !comboCoveredByPair(combo, allowed) {
			continue
		}
		out = append(out, combo)
	}
	return out
}

func coveredStrategyCombos(combos []strategyComboSpec, pairTimeframes []string) []strategyComboSpec {
	if len(combos) == 0 {
		return nil
	}
	allowed := allowedStrategyTimeframes(pairTimeframes)
	if len(allowed) == 0 {
		return nil
	}
	out := make([]strategyComboSpec, 0, len(combos))
	for _, combo := range combos {
		if !comboCoveredByPair(combo, allowed) {
			continue
		}
		out = append(out, combo)
	}
	return out
}

func allowedStrategyTimeframes(pairTimeframes []string) map[string]struct{} {
	allowed := make(map[string]struct{}, len(pairTimeframes))
	for _, timeframe := range pairTimeframes {
		timeframe = strings.ToLower(strings.TrimSpace(timeframe))
		if timeframe == "" {
			continue
		}
		if _, ok := market.TimeframeDuration(timeframe); !ok {
			continue
		}
		allowed[timeframe] = struct{}{}
	}
	return allowed
}

func comboCoveredByPair(combo strategyComboSpec, pair map[string]struct{}) bool {
	if len(combo.Timeframes) == 0 {
		return false
	}
	for _, timeframe := range combo.Timeframes {
		if _, ok := pair[strings.ToLower(strings.TrimSpace(timeframe))]; !ok {
			return false
		}
	}
	return true
}

func buildComboSnapshot(snapshot models.MarketSnapshot, combo strategyComboSpec) (models.MarketSnapshot, bool) {
	if len(combo.Timeframes) == 0 {
		return models.MarketSnapshot{}, false
	}
	series := make(map[string][]models.OHLCV, len(combo.Timeframes))
	meta := make(map[string]models.SeriesMeta, len(combo.Timeframes))
	for _, timeframe := range combo.Timeframes {
		items, ok := snapshot.Series[timeframe]
		if !ok || len(items) == 0 {
			return models.MarketSnapshot{}, false
		}
		info, ok := snapshot.Meta[timeframe]
		if !ok || info.LastIndex < 0 || info.LastIndex >= len(items) {
			return models.MarketSnapshot{}, false
		}
		series[timeframe] = items
		meta[timeframe] = info
	}
	return models.MarketSnapshot{
		Exchange:       snapshot.Exchange,
		Symbol:         snapshot.Symbol,
		EventTimeframe: snapshot.EventTimeframe,
		EventTS:        snapshot.EventTS,
		EventClosed:    snapshot.EventClosed,
		Series:         series,
		Meta:           meta,
	}, true
}
