package core

import (
	"context"
	"strings"

	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const startupBootstrapSource = "startup_bootstrap"

func (b *Live) bootstrapStrategyCombosForExchange(ctx context.Context, plan warmupExchangePlan) {
	if b == nil || b.Strategy == nil || b.Cache == nil || len(b.strategyCombos) == 0 {
		return
	}
	logger := b.logger
	if logger == nil {
		logger = glog.Nop()
	}
	pairsEvaluated := 0
	combosEvaluated := 0
	for _, sym := range plan.items {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		timeframes, err := parseSymbolTimeframes(sym.Timeframes, plan.defaultTimeframes)
		if err != nil {
			logger.Warn("parse symbol timeframes failed during startup combo bootstrap",
				zap.String("exchange", plan.exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(err),
			)
			timeframes = plan.defaultTimeframes
		}
		timeframes = normalizePlanTimeframes(timeframes)
		combos := coveredStrategyCombos(b.strategyCombos, timeframes)
		if len(combos) == 0 {
			continue
		}
		pairComboCount := 0
		for _, combo := range combos {
			if ctx != nil && ctx.Err() != nil {
				return
			}
			comboSnapshot, data, ok := b.bootstrapComboSnapshot(plan.exchange, sym.Symbol, timeframes, combo)
			if !ok {
				continue
			}
			b.evaluateStrategyCombos(data, comboSnapshot, []strategyComboSpec{combo}, []strategyComboSpec{combo})
			pairComboCount++
			combosEvaluated++
		}
		if pairComboCount > 0 {
			pairsEvaluated++
		}
	}
	if combosEvaluated == 0 {
		return
	}
	logger.Info("startup combo bootstrap completed",
		zap.String("exchange", plan.exchange),
		zap.Int("pairs_evaluated", pairsEvaluated),
		zap.Int("combos_evaluated", combosEvaluated),
	)
}

func (b *Live) bootstrapComboSnapshot(exchange, symbol string, pairTimeframes []string, combo strategyComboSpec) (models.MarketSnapshot, models.MarketData, bool) {
	if b == nil || b.Cache == nil {
		return models.MarketSnapshot{}, models.MarketData{}, false
	}
	executionTimeframe := strings.TrimSpace(combo.ExecutionTimeframe)
	if executionTimeframe == "" {
		return models.MarketSnapshot{}, models.MarketData{}, false
	}
	snapshot := b.Cache.SnapshotForTimeframes(exchange, symbol, executionTimeframe, 0, pairTimeframes)
	comboSnapshot, ok := buildComboSnapshot(snapshot, combo)
	if !ok {
		return models.MarketSnapshot{}, models.MarketData{}, false
	}
	items := comboSnapshot.Series[executionTimeframe]
	meta, ok := comboSnapshot.Meta[executionTimeframe]
	if !ok || meta.LastIndex < 0 || meta.LastIndex >= len(items) {
		return models.MarketSnapshot{}, models.MarketData{}, false
	}
	event := items[meta.LastIndex]
	comboSnapshot.EventTimeframe = executionTimeframe
	comboSnapshot.EventTS = event.TS
	comboSnapshot.EventClosed = true
	return comboSnapshot, models.MarketData{
		Exchange:  exchange,
		Symbol:    symbol,
		Timeframe: executionTimeframe,
		Closed:    true,
		OHLCV:     event,
		Source:    startupBootstrapSource,
	}, true
}
