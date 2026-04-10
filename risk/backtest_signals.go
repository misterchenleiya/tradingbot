package risk

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

func (r *BackTest) ListSignals() map[string]map[string]models.Signal {
	if r == nil || r.signalCache == nil {
		return map[string]map[string]models.Signal{}
	}
	return filterGroupedSignalsByActiveStrategies(r.signalCache.ListGrouped(), r.activeSet)
}

func (r *BackTest) ListSignalsByPair(exchange, symbol string) ([]models.Signal, error) {
	if r == nil || r.signalCache == nil {
		return nil, nil
	}
	exchange = normalizeExchange(exchange)
	symbol = canonicalSymbol(symbol)
	if exchange == "" || symbol == "" {
		return nil, nil
	}
	hasPosition, err := r.HasOpenPosition(exchange, symbol)
	if err != nil {
		return nil, err
	}
	signals := r.signalCache.ListByPair(exchange, symbol)
	signals = filterSignalsByActiveStrategies(signals, r.activeSet)
	if len(signals) > 0 {
		if hasPosition {
			for i := range signals {
				signals[i] = enrichSignalPositionState(signals[i], models.Position{}, true)
			}
		}
		return signals, nil
	}
	if !hasPosition {
		return nil, nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if restored := r.recoverPairSignalsLocked(exchange, symbol); restored > 0 {
		signals = r.signalCache.ListByPair(exchange, symbol)
		signals = filterSignalsByActiveStrategies(signals, r.activeSet)
		if len(signals) == 0 {
			return nil, nil
		}
		for i := range signals {
			signals[i] = enrichSignalPositionState(signals[i], models.Position{}, true)
		}
		return signals, nil
	}
	return nil, nil
}

func (r *BackTest) restoreSignalsFromStoreLocked() {
	if r == nil || r.signalCache == nil {
		return
	}
	pairs := r.openPositionPairsLocked()
	if len(pairs) == 0 {
		return
	}
	total := 0
	for _, pair := range pairs {
		total += r.recoverPairSignalsLocked(pair.Exchange, pair.Symbol)
	}
	r.logger.Info("risk back-test signal cache restored",
		zap.Int("open_pairs", len(pairs)),
		zap.Int("restored_signals", total),
	)
}

func (r *BackTest) recoverPairSignalsLocked(exchange, symbol string) int {
	exchange = normalizeExchange(exchange)
	symbol = canonicalSymbol(symbol)
	if exchange == "" || symbol == "" {
		return 0
	}
	restored := 0
	if r.store != nil {
		records, err := r.store.ListSignalChangesByPair(r.mode, exchange, symbol)
		if err != nil {
			r.logger.Warn("risk back-test load signal changes failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(err),
			)
		} else {
			grouped := groupSignalEvents(records)
			for key, chain := range grouped {
				if !isStrategyAllowed(r.activeSet, key.Strategy) {
					continue
				}
				signal, ok := rebuildSignalFromEvents(chain)
				if !ok {
					continue
				}
				signal = normalizeRiskSignal(signal, exchange, symbol, signal.Timeframe)
				if !comboConfigured(r.comboTradeEnabled, signal) {
					continue
				}
				signal.HasPosition = models.SignalHasOpenPosition
				r.signalCache.Upsert(signal)
				restored++
			}
			if restored > 0 {
				return restored
			}
		}
	}
	for posKey, pos := range r.positions {
		if pos == nil || pos.RemainingQty <= 0 {
			continue
		}
		if posKey != pairKey(exchange, symbol) {
			continue
		}
		signal, ok := r.signalFromBackTestPosition(pos)
		if !ok {
			continue
		}
		if !isStrategyAllowed(r.activeSet, signal.Strategy) {
			continue
		}
		if !comboConfigured(r.comboTradeEnabled, signal) {
			continue
		}
		r.signalCache.Upsert(signal)
		_ = r.appendSignalChange(signal, models.SignalChangeStatusNew, "recovered_from_position", time.Now().UnixMilli())
		restored++
	}
	return restored
}

func (r *BackTest) applySignalLifecycleLocked(signal models.Signal, position models.Position, hasPosition bool, eventTS int64) (models.Signal, bool, error) {
	if r == nil || r.signalCache == nil {
		return signal, false, nil
	}
	signal = normalizeRiskSignal(signal, signal.Exchange, signal.Symbol, signal.Timeframe)
	allowedStrategy := isStrategyAllowed(r.activeSet, signal.Strategy)
	if !allowedStrategy && !models.IsEmptySignal(signal) {
		return signal, false, nil
	}
	signal = enrichSignalPositionState(signal, position, hasPosition)
	previous, hasPrevious := r.signalCache.Find(signal.Exchange, signal.Symbol, signal.Strategy, signal.ComboKey)

	if models.IsEmptySignal(signal) {
		if !hasPrevious {
			return signal, false, nil
		}
		r.markTrendGuardSignalGoneLocked(previous, eventTS)
		r.signalCache.Remove(previous)
		recordSignal := models.ClearSignalForRemoval(previous)
		if err := r.appendSignalChange(recordSignal, models.SignalChangeStatusGone, "signal_disappeared", eventTS); err != nil {
			return signal, true, err
		}
		return signal, true, nil
	}

	if hasPrevious && isSignalEqual(previous, signal) {
		return signal, false, nil
	}
	changeStatus := models.SignalChangeStatusNew
	changedFields := "new_signal"
	if hasPrevious {
		changeStatus = models.SignalChangeStatusUpdated
		changedFields = signalChangedFields(previous, signal)
		if changedFields == "" {
			changedFields = "signal_updated"
		}
	}
	r.signalCache.Upsert(signal)
	if err := r.appendSignalChange(signal, changeStatus, changedFields, eventTS); err != nil {
		return signal, true, err
	}
	return signal, true, nil
}

func (r *BackTest) markOpenRejectedSignalLocked(signal models.Signal, rejectAction int, eventTS int64) (models.Signal, bool, error) {
	if r == nil {
		return signal, false, nil
	}
	return markOpenRejectedSignalInCache(r.signalCache, signal, rejectAction, eventTS, r.appendSignalChange)
}

func (r *BackTest) syncSignalCacheFromPositionLocked(
	signal models.Signal,
	pos *backTestPosition,
	eventTS int64,
) error {
	if r == nil || r.signalCache == nil || pos == nil {
		return nil
	}
	exchange := normalizeExchange(firstNonEmpty(signal.Exchange, pos.Exchange))
	symbol := canonicalSymbol(firstNonEmpty(signal.Symbol, pos.Symbol))
	timeframe := strings.TrimSpace(firstNonEmpty(signal.Timeframe, pos.Timeframe))
	strategy := strings.TrimSpace(firstNonEmpty(signal.Strategy, pos.Strategy))
	comboKey := signalComboKey(pos.StrategyTimeframes, timeframe, signal.ComboKey)
	if exchange == "" || symbol == "" || timeframe == "" || strategy == "" || comboKey == "" {
		return nil
	}
	if !isStrategyAllowed(r.activeSet, strategy) {
		return nil
	}

	previous, ok := r.signalCache.Find(exchange, symbol, strategy, comboKey)
	if !ok {
		return nil
	}

	next := previous
	if previous.HasPosition == models.SignalHasPartialClose {
		next.HasPosition = models.SignalHasPartialClose
	} else {
		next.HasPosition = models.SignalHasOpenPosition
	}
	if !floatAlmostEqual(next.Entry, pos.EntryPrice) {
		next.Entry = pos.EntryPrice
	}
	if !floatAlmostEqual(next.TP, pos.TakeProfitPrice) {
		next.TP = pos.TakeProfitPrice
	}
	if !floatAlmostEqual(next.SL, pos.StopLossPrice) {
		next.SL = pos.StopLossPrice
	}
	if isSignalEqual(previous, next) {
		return nil
	}

	changedFields := signalChangedFields(previous, next)
	if changedFields == "" {
		changedFields = "signal_runtime_synced"
	}
	r.signalCache.Upsert(next)
	return r.appendSignalChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS)
}

func (r *BackTest) markSignalClosedByPnLLocked(pos *backTestPosition, profit float64, eventTS int64) error {
	if r == nil || r.signalCache == nil || pos == nil {
		return nil
	}
	exchange := normalizeExchange(pos.Exchange)
	symbol := canonicalSymbol(pos.Symbol)
	timeframe := strings.TrimSpace(pos.Timeframe)
	strategy := strings.TrimSpace(pos.Strategy)
	comboKey := signalComboKey(pos.StrategyTimeframes, timeframe, "")
	if exchange == "" || symbol == "" || timeframe == "" || strategy == "" || comboKey == "" {
		return nil
	}

	previous, hasPrevious := r.signalCache.Find(exchange, symbol, strategy, comboKey)
	if !hasPrevious {
		return nil
	}

	next := previous
	next.Action = 0
	next.OrderType = ""
	next.Amount = 0
	next.Entry = 0
	next.Exit = 0
	next.SL = 0
	next.TP = 0
	next.InitialSL = 0
	next.InitialRiskPct = 0
	next.MaxFavorableProfitPct = 0
	next.ProfitProtectStage = models.SignalProfitProtectStageNone
	next.Plan1LastProfitLockMFER = 0
	next.Plan1LastProfitLockHighBucketTS = 0
	next.Plan1LastProfitLockStructPrice = 0
	next.PostHighPullbackFirstEntryState = models.SignalPostHighPullbackFirstEntryNone
	next.EntryWatchTimestamp = 0
	if eventTS > 0 {
		next.TriggerTimestamp = int(eventTS)
	}
	switch {
	case profit > 0:
		next.HasPosition = models.SignalHasClosedProfit
	case profit < 0:
		next.HasPosition = models.SignalHasClosedLoss
	default:
		next.HasPosition = models.SignalHasNoPosition
	}
	if isSignalEqual(previous, next) {
		return nil
	}
	changedFields := signalChangedFields(previous, next)
	if changedFields == "" {
		changedFields = "signal_closed"
	}
	r.signalCache.Upsert(next)
	return r.appendSignalChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS)
}

func shouldRemoveSignalAfterTrendEndedClose(signal models.Signal) bool {
	return signal.Action == 64 &&
		signal.HighSide == 0 &&
		signal.MidSide == 0 &&
		signal.TrendingTimestamp == 0
}

func (r *BackTest) removeSignalAfterTrendEndedCloseLocked(pos *backTestPosition, eventTS int64) error {
	if r == nil || r.signalCache == nil || pos == nil {
		return nil
	}
	exchange := normalizeExchange(pos.Exchange)
	symbol := canonicalSymbol(pos.Symbol)
	timeframe := strings.TrimSpace(pos.Timeframe)
	strategy := strings.TrimSpace(pos.Strategy)
	comboKey := signalComboKey(pos.StrategyTimeframes, timeframe, pos.ComboKey)
	if exchange == "" || symbol == "" || timeframe == "" || strategy == "" || comboKey == "" {
		return nil
	}

	previous, ok := r.signalCache.Find(exchange, symbol, strategy, comboKey)
	if !ok {
		return nil
	}
	r.markTrendGuardSignalGoneLocked(previous, eventTS)
	r.signalCache.Remove(previous)
	recordSignal := models.ClearSignalForRemoval(previous)
	return r.appendSignalChange(recordSignal, models.SignalChangeStatusGone, "signal_disappeared,trend_end_close_all", eventTS)
}

func (r *BackTest) appendSignalChange(signal models.Signal, changeStatus int, changedFields string, eventTS int64) error {
	if r == nil || r.store == nil {
		return nil
	}
	payload, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal signal change failed: %w", err)
	}
	record := models.SignalChangeRecord{
		SingletonID:     r.singletonID,
		Mode:            r.mode,
		Exchange:        normalizeExchange(signal.Exchange),
		Symbol:          canonicalSymbol(signal.Symbol),
		Timeframe:       strings.TrimSpace(signal.Timeframe),
		Strategy:        strings.TrimSpace(signal.Strategy),
		StrategyVersion: strings.TrimSpace(signal.StrategyVersion),
		ChangeStatus:    changeStatus,
		ChangedFields:   strings.TrimSpace(changedFields),
		SignalJSON:      string(payload),
		EventAtMS:       normalizeTimestampMS(eventTS),
		CreatedAtMS:     time.Now().UnixMilli(),
	}
	if record.EventAtMS <= 0 {
		record.EventAtMS = record.CreatedAtMS
	}
	return r.store.AppendSignalChange(record)
}

func (r *BackTest) signalFromBackTestPosition(pos *backTestPosition) (models.Signal, bool) {
	if pos == nil {
		return models.Signal{}, false
	}
	exchange := normalizeExchange(pos.Exchange)
	symbol := canonicalSymbol(pos.Symbol)
	strategy := strings.TrimSpace(pos.Strategy)
	timeframe := strings.TrimSpace(pos.Timeframe)
	if exchange == "" || symbol == "" || strategy == "" || timeframe == "" {
		return models.Signal{}, false
	}
	highSide := 1
	if pos.Side == positionSideShort {
		highSide = -1
	}
	triggerTS := int(normalizeTimestampMS(pos.EntryTS))
	if triggerTS <= 0 {
		triggerTS = int(time.Now().UnixMilli())
	}
	return models.Signal{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		ComboKey:           signalComboKey(pos.StrategyTimeframes, timeframe, ""),
		HasPosition:        models.SignalHasOpenPosition,
		OrderType:          models.OrderTypeMarket,
		Entry:              pos.EntryPrice,
		SL:                 pos.StopLossPrice,
		TP:                 pos.TakeProfitPrice,
		Action:             8,
		HighSide:           highSide,
		MidSide:            highSide,
		TrendingTimestamp:  triggerTS,
		TriggerTimestamp:   triggerTS,
		Strategy:           strategy,
		StrategyVersion:    pos.StrategyVersion,
		StrategyTimeframes: append([]string(nil), pos.StrategyTimeframes...),
	}, true
}

type backTestPair struct {
	Exchange string
	Symbol   string
}

func (r *BackTest) openPositionPairsLocked() []backTestPair {
	seen := make(map[string]backTestPair)
	for key, pos := range r.positions {
		if pos == nil || pos.RemainingQty <= 0 {
			continue
		}
		parts := strings.Split(key, "|")
		if len(parts) != 2 {
			continue
		}
		exchange := normalizeExchange(parts[0])
		symbol := canonicalSymbol(parts[1])
		if exchange == "" || symbol == "" {
			continue
		}
		seen[exchange+"|"+symbol] = backTestPair{Exchange: exchange, Symbol: symbol}
	}
	out := make([]backTestPair, 0, len(seen))
	for _, item := range seen {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}
