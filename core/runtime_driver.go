package core

import (
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type signalFlowHooks struct {
	strategy         iface.Strategy
	logger           *zap.Logger
	applySignal      func(call string, data models.MarketData, snapshot models.MarketSnapshot, previous *models.Signal, signal models.Signal)
	onUpdateNoChange func(data models.MarketData, snapshot models.MarketSnapshot, current models.Signal)
}

func runSignalFlow(hooks signalFlowHooks, data models.MarketData, snapshot models.MarketSnapshot, cachedSignals []models.Signal, hasPosition bool) {
	if hooks.strategy == nil || hooks.applySignal == nil {
		return
	}
	logger := hooks.logger
	if logger == nil {
		logger = glog.Nop()
	}

	updateSignals := trackedSignalsForUpdate(cachedSignals)
	if hasPosition || len(updateSignals) > 0 {
		if len(updateSignals) == 0 {
			logger.Warn("skip strategy update due to missing active risk signal cache",
				zap.String("exchange", data.Exchange),
				zap.String("symbol", data.Symbol),
				zap.Bool("has_position", hasPosition),
			)
			return
		}
		for _, current := range updateSignals {
			if current.Strategy == "" {
				logger.Warn("skip strategy update due to missing strategy name",
					zap.String("exchange", current.Exchange),
					zap.String("symbol", current.Symbol),
					zap.String("timeframe", current.Timeframe),
				)
				continue
			}
			next, updated := hooks.strategy.Update(current.Strategy, current, snapshot)
			if !updated {
				if hooks.onUpdateNoChange != nil {
					hooks.onUpdateNoChange(data, snapshot, current)
				}
				continue
			}
			next = normalizeSignalForRuntime(next, current, data)
			hooks.applySignal("update", data, snapshot, &current, next)
		}
		return
	}

	processGetSignalsFlow(hooks, data, snapshot, cachedSignals, hooks.strategy.Get(snapshot))
}

func processGetSignalsFlow(hooks signalFlowHooks, data models.MarketData, snapshot models.MarketSnapshot, previousSignals []models.Signal, signals []models.Signal) {
	if hooks.applySignal == nil {
		return
	}
	previousByKey := make(map[string]models.Signal, len(previousSignals))
	for _, previous := range previousSignals {
		key := flatSignalKey(previous)
		if key == "" {
			continue
		}
		previousByKey[key] = previous
	}
	for _, signal := range signals {
		signal = normalizeSignalForRuntime(signal, models.Signal{}, data)
		key := flatSignalKey(signal)
		if key == "" {
			continue
		}
		previous, hasPrevious := previousByKey[key]
		if hasPrevious && liveSignalsEqual(previous, signal) {
			delete(previousByKey, key)
			continue
		}
		if hasPrevious {
			hooks.applySignal("get", data, snapshot, &previous, signal)
		} else {
			hooks.applySignal("get", data, snapshot, nil, signal)
		}
		delete(previousByKey, key)
	}
	for _, stale := range previousByKey {
		cleared := models.ClearSignalForRemoval(stale)
		hooks.applySignal("get", data, snapshot, &stale, cleared)
	}
}
