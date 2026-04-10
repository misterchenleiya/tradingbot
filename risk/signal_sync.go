package risk

import (
	"strings"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type signalChangeAppendFunc func(signal models.Signal, changeStatus int, changedFields string, eventTS int64) error

func markOpenRejectedSignalInCache(
	cache *SignalCache,
	signal models.Signal,
	rejectAction int,
	eventTS int64,
	appendChange signalChangeAppendFunc,
) (models.Signal, bool, error) {
	if cache == nil || appendChange == nil {
		return signal, false, nil
	}
	signal = normalizeRiskSignal(signal, signal.Exchange, signal.Symbol, signal.Timeframe)
	if !isSignalKeyReady(signal) {
		return signal, false, nil
	}
	previous, ok := cache.Find(signal.Exchange, signal.Symbol, signal.Strategy, signal.ComboKey)
	if !ok {
		return signal, false, nil
	}
	next := previous
	next.Action = rejectAction
	if eventTS > 0 {
		next.TriggerTimestamp = int(eventTS)
	}
	if isSignalEqual(previous, next) {
		return next, false, nil
	}
	changedFields := signalChangedFields(previous, next)
	switch rejectAction {
	case models.SignalActionOpenRiskRejected:
		changedFields = appendChangedField(changedFields, "open_rejected_risk")
	case models.SignalActionOpenTrendGuardRejected:
		changedFields = appendChangedField(changedFields, "open_rejected_trend_guard")
	}
	if strings.TrimSpace(changedFields) == "" {
		changedFields = "action"
	}
	cache.Upsert(next)
	if err := appendChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS); err != nil {
		return next, true, err
	}
	return next, true, nil
}
