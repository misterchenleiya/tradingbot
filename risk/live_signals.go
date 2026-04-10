package risk

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

const runtimeSLMinUpdatePct = 0.001

func (r *Live) ListSignals() map[string]map[string]models.Signal {
	if r == nil || r.signalCache == nil {
		return map[string]map[string]models.Signal{}
	}
	return filterGroupedSignalsByActiveStrategies(r.signalCache.ListGrouped(), r.activeSet)
}

func (r *Live) ListSignalsByPair(exchange, symbol string) ([]models.Signal, error) {
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
	restored := r.recoverPairSignals(exchange, symbol)
	if restored == 0 {
		return nil, nil
	}
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

func (r *Live) restoreSignalsFromStore() {
	if r == nil || r.signalCache == nil {
		return
	}
	pairs := r.openPositionPairs()
	if len(pairs) == 0 {
		return
	}
	total := 0
	for _, pair := range pairs {
		total += r.recoverPairSignals(pair.Exchange, pair.Symbol)
	}
	r.logger.Info("risk signal cache restored",
		zap.Int("open_pairs", len(pairs)),
		zap.Int("restored_signals", total),
	)
}

func (r *Live) recoverPairSignals(exchange, symbol string) int {
	exchange = normalizeExchange(exchange)
	symbol = canonicalSymbol(symbol)
	if exchange == "" || symbol == "" {
		return 0
	}

	restored := 0
	if r.store != nil {
		records, err := r.store.ListSignalChangesByPair(liveMode, exchange, symbol)
		if err != nil {
			r.logger.Warn("risk load signal changes failed",
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
					if len(chain) > 0 && chain[len(chain)-1].ChangeStatus == models.SignalChangeStatusGone {
						r.logger.Warn("signal history ended with disappeared state while position exists",
							zap.String("exchange", exchange),
							zap.String("symbol", symbol),
							zap.String("combo_key", key.ComboKey),
							zap.String("strategy", key.Strategy),
						)
					}
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

	positions, err := r.ListOpenPositions(exchange, symbol, "")
	if err != nil {
		r.logger.Warn("risk fallback position-to-signal failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Error(err),
		)
		return 0
	}
	for _, pos := range positions {
		signal, ok := r.signalFromPosition(pos)
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
	if restored > 0 {
		r.logger.Warn("risk recovered signals from positions without usable signal history",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Int("restored", restored),
		)
	}
	return restored
}

func (r *Live) applySignalLifecycle(signal models.Signal, data models.MarketData, position models.Position, hasPosition bool) (models.Signal, bool, error) {
	if r == nil || r.signalCache == nil {
		return signal, false, nil
	}
	eventTS := normalizeTimestampMS(data.OHLCV.TS)
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	signal = normalizeRiskSignal(signal, data.Exchange, data.Symbol, data.Timeframe)
	if !isSignalKeyReady(signal) {
		return signal, false, nil
	}

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
		r.signalCache.Remove(previous)
		r.markTrendGuardSignalGone(previous, eventTS)
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

func (r *Live) markOpenRejectedSignal(signal models.Signal, rejectAction int, eventTS int64) (models.Signal, bool, error) {
	if r == nil {
		return signal, false, nil
	}
	return markOpenRejectedSignalInCache(r.signalCache, signal, rejectAction, eventTS, r.appendSignalChange)
}

func (r *Live) markTrendGuardSignalGone(signal models.Signal, eventTS int64) {
	if r == nil || r.trendGuard == nil {
		return
	}
	cfg := r.currentConfig()
	if !trendGuardGroupedEnabled(cfg.TrendGuard) {
		return
	}
	r.trendGuard.markSignalGone(cfg.TrendGuard, signal, eventTS)
}

func (r *Live) LookupSignalGrouped(signal models.Signal) (models.SignalGroupedInfo, bool) {
	if r == nil || r.trendGuard == nil {
		return models.SignalGroupedInfo{}, false
	}
	cfg := r.currentConfig()
	if !trendGuardGroupedEnabled(cfg.TrendGuard) {
		return models.SignalGroupedInfo{}, false
	}
	return r.trendGuard.lookupSignalGrouped(cfg.TrendGuard, signal)
}

func (r *Live) appendSignalChange(signal models.Signal, changeStatus int, changedFields string, eventTS int64) error {
	if r == nil || r.store == nil {
		return nil
	}
	payload, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal signal change failed: %w", err)
	}
	record := models.SignalChangeRecord{
		SingletonID:     r.singletonID,
		Mode:            liveMode,
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

func (r *Live) syncSignalCacheFromDecision(signal models.Signal, decision models.Decision, eventTS int64) error {
	if r == nil || r.signalCache == nil {
		return nil
	}
	if decision.Action != models.DecisionActionOpenLong &&
		decision.Action != models.DecisionActionOpenShort &&
		decision.Action != models.DecisionActionUpdate &&
		decision.Action != models.DecisionActionClose {
		return nil
	}

	exchange := normalizeExchange(firstNonEmpty(signal.Exchange, decision.Exchange))
	symbol := canonicalSymbol(firstNonEmpty(signal.Symbol, decision.Symbol))
	timeframe := strings.TrimSpace(firstNonEmpty(signal.Timeframe, decision.Timeframe))
	strategy := strings.TrimSpace(signal.Strategy)
	comboKey := signalComboKey(signal.StrategyTimeframes, timeframe, signal.ComboKey)
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
	switch {
	case decision.Action == models.DecisionActionClose && previous.Action == 32:
		next.HasPosition = models.SignalHasPartialClose
	case previous.HasPosition == models.SignalHasPartialClose:
		next.HasPosition = models.SignalHasPartialClose
	default:
		next.HasPosition = models.SignalHasOpenPosition
	}
	if !math.IsNaN(decision.TakeProfitPrice) && decision.TakeProfitPrice >= 0 && !floatAlmostEqual(next.TP, decision.TakeProfitPrice) {
		next.TP = decision.TakeProfitPrice
	}
	slChanged, slSignificant := false, false
	if !math.IsNaN(decision.StopLossPrice) && decision.StopLossPrice > 0 {
		var changed bool
		next.SL, changed, slSignificant = mergeRuntimeSyncedStopLoss(next.SL, decision.StopLossPrice, next.HighSide)
		slChanged = changed
	}
	if isSignalEqual(previous, next) {
		return nil
	}

	changedFields := signalChangedFields(previous, next)
	if slChanged && !slSignificant {
		changedFields = stripChangedField(changedFields, "sl")
	}
	if changedFields == "" {
		r.signalCache.Upsert(next)
		return nil
	}
	r.signalCache.Upsert(next)
	return r.appendSignalChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS)
}

func (r *Live) syncSignalCacheFromPosition(position models.Position, eventTS int64) error {
	if r == nil || r.signalCache == nil {
		return nil
	}
	exchange := normalizeExchange(position.Exchange)
	symbol := canonicalSymbol(position.Symbol)
	if exchange == "" || symbol == "" {
		return nil
	}

	signals := r.signalCache.ListByPair(exchange, symbol)
	if len(signals) == 0 {
		return nil
	}
	for _, previous := range signals {
		if !isStrategyAllowed(r.activeSet, previous.Strategy) {
			continue
		}
		if strings.TrimSpace(previous.Strategy) == "" || strings.TrimSpace(previous.Timeframe) == "" {
			continue
		}
		if strategyName := strings.TrimSpace(position.StrategyName); strategyName != "" &&
			!strings.EqualFold(strings.TrimSpace(previous.Strategy), strategyName) {
			continue
		}
		if !positionOwnedBy(position, previous.Strategy, previous.Timeframe, previous.StrategyTimeframes, previous.ComboKey) {
			continue
		}
		side := normalizePositionSide(position.PositionSide, 0)
		if (previous.HighSide == 1 && side == positionSideShort) ||
			(previous.HighSide == -1 && side == positionSideLong) {
			continue
		}

		next := previous
		if previous.HasPosition == models.SignalHasPartialClose {
			next.HasPosition = models.SignalHasPartialClose
		} else {
			next.HasPosition = models.SignalHasOpenPosition
		}
		if !math.IsNaN(position.EntryPrice) && position.EntryPrice > 0 && !floatAlmostEqual(next.Entry, position.EntryPrice) {
			next.Entry = position.EntryPrice
		}
		if !math.IsNaN(position.TakeProfitPrice) && position.TakeProfitPrice >= 0 && !floatAlmostEqual(next.TP, position.TakeProfitPrice) {
			next.TP = position.TakeProfitPrice
		}
		slChanged, slSignificant := false, false
		if !math.IsNaN(position.StopLossPrice) && position.StopLossPrice > 0 {
			var changed bool
			next.SL, changed, slSignificant = mergeRuntimeSyncedStopLoss(next.SL, position.StopLossPrice, next.HighSide)
			slChanged = changed
		}
		if isSignalEqual(previous, next) {
			continue
		}

		changedFields := signalChangedFields(previous, next)
		if slChanged && !slSignificant {
			changedFields = stripChangedField(changedFields, "sl")
		}
		if changedFields == "" {
			r.signalCache.Upsert(next)
			continue
		}
		r.signalCache.Upsert(next)
		if err := r.appendSignalChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS); err != nil {
			return err
		}
	}
	return nil
}

func (r *Live) syncSignalCacheClosedByPositionDisappearance(previous models.RiskOpenPosition, current map[string]models.Position, eventTS int64) error {
	if r == nil || r.signalCache == nil {
		return nil
	}

	exchange := normalizeExchange(previous.Exchange)
	symbol := canonicalSymbol(previous.Symbol)
	if exchange == "" || symbol == "" {
		return nil
	}

	meta := models.ExtractStrategyContextMeta(previous.RowJSON)
	strategy := strings.TrimSpace(meta.StrategyName)
	timeframe := strings.TrimSpace(strategyPrimaryTimeframe(meta))
	comboKey := strategyComboKey(meta)
	side := normalizePositionSide(previous.PosSide, 0)
	if strategy == "" || timeframe == "" || comboKey == "" || side == "" {
		return nil
	}
	if !isStrategyAllowed(r.activeSet, strategy) {
		return nil
	}

	for _, pos := range current {
		if normalizeExchange(pos.Exchange) != exchange || canonicalSymbol(pos.Symbol) != symbol {
			continue
		}
		if !positionOwnedBy(pos, strategy, timeframe, meta.StrategyTimeframes, comboKey) {
			continue
		}
		if normalizePositionSide(pos.PositionSide, 0) != side {
			continue
		}
		if isPositionOpen(pos) {
			return nil
		}
	}

	signals := r.signalCache.ListByPair(exchange, symbol)
	for _, cached := range signals {
		if !isStrategyAllowed(r.activeSet, cached.Strategy) {
			continue
		}
		cachedCombo := signalComboKey(cached.StrategyTimeframes, cached.Timeframe, cached.ComboKey)
		cachedHasExplicitCombo := len(cached.StrategyTimeframes) > 0 || strings.TrimSpace(cached.ComboKey) != ""
		if !strings.EqualFold(strings.TrimSpace(cached.Strategy), strategy) ||
			(cachedHasExplicitCombo && !strings.EqualFold(cachedCombo, comboKey)) ||
			(!cachedHasExplicitCombo && !strings.EqualFold(strings.TrimSpace(cached.Timeframe), timeframe)) {
			continue
		}
		if !signalMatchesPositionSide(cached, side) {
			continue
		}
		if shouldRemoveSignalAfterTrendEndedClose(cached) {
			r.signalCache.Remove(cached)
			r.markTrendGuardSignalGone(cached, eventTS)
			recordSignal := models.ClearSignalForRemoval(cached)
			if err := r.appendSignalChange(recordSignal, models.SignalChangeStatusGone, "signal_disappeared,trend_end_close_all", eventTS); err != nil {
				return err
			}
			continue
		}

		next := cached
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
		next.HasPosition = models.SignalHasNoPosition
		if eventTS > 0 {
			next.TriggerTimestamp = int(eventTS)
		}
		if isSignalEqual(cached, next) {
			continue
		}

		changedFields := signalChangedFields(cached, next)
		if changedFields == "" {
			changedFields = "signal_closed"
		}
		r.signalCache.Upsert(next)
		if err := r.appendSignalChange(next, models.SignalChangeStatusUpdated, changedFields, eventTS); err != nil {
			return err
		}
	}
	return nil
}

func enrichSignalPositionState(signal models.Signal, position models.Position, hasPosition bool) models.Signal {
	if hasPosition {
		switch signal.Action {
		case 32:
			signal.HasPosition = models.SignalHasPartialClose
		default:
			signal.HasPosition = models.SignalHasOpenPosition
		}
		return signal
	}
	if signal.HasPosition != models.SignalHasClosedProfit && signal.HasPosition != models.SignalHasClosedLoss {
		signal.HasPosition = models.SignalHasNoPosition
	}
	return signal
}

func mergeRuntimeSyncedStopLoss(current, synced float64, side int) (float64, bool, bool) {
	if synced <= 0 {
		return current, false, false
	}
	if current <= 0 {
		return synced, !floatAlmostEqual(current, synced), true
	}
	if floatAlmostEqual(current, synced) {
		return current, false, false
	}

	switch side {
	case 1:
		if synced < current {
			return current, false, false
		}
		return synced, true, slSyncDeltaSignificant(current, synced)
	case -1:
		if synced > current {
			return current, false, false
		}
		return synced, true, slSyncDeltaSignificant(current, synced)
	default:
		return synced, true, slSyncDeltaSignificant(current, synced)
	}
}

func slSyncDeltaSignificant(current, next float64) bool {
	if current <= 0 || next <= 0 {
		return false
	}
	diff := math.Abs(next-current) / current
	return diff >= runtimeSLMinUpdatePct
}

func stripChangedField(changedFields, target string) string {
	if strings.TrimSpace(changedFields) == "" {
		return ""
	}
	parts := strings.Split(changedFields, ",")
	filtered := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == target {
			continue
		}
		filtered = append(filtered, strings.TrimSpace(part))
	}
	return strings.Join(filtered, ",")
}

func appendChangedField(changedFields, target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return strings.TrimSpace(changedFields)
	}
	parts := strings.Split(strings.TrimSpace(changedFields), ",")
	filtered := make([]string, 0, len(parts)+1)
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == target {
			return strings.Join(append(filtered, part), ",")
		}
		filtered = append(filtered, part)
	}
	filtered = append(filtered, target)
	return strings.Join(filtered, ",")
}

func (r *Live) signalFromPosition(position models.Position) (models.Signal, bool) {
	exchange := normalizeExchange(position.Exchange)
	symbol := canonicalSymbol(position.Symbol)
	strategy := strings.TrimSpace(position.StrategyName)
	if exchange == "" || symbol == "" || strategy == "" {
		return models.Signal{}, false
	}
	timeframe := strings.TrimSpace(position.Timeframe)
	if timeframe == "" {
		timeframe = r.defaultTimeframe
	}
	side := normalizePositionSide(position.PositionSide, 0)
	if side == "" {
		return models.Signal{}, false
	}
	highSide := 1
	if side == positionSideShort {
		highSide = -1
	}
	triggerTS := inferPositionSignalTimestamp(position)
	return models.Signal{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          timeframe,
		ComboKey:           signalComboKey(position.StrategyTimeframes, timeframe, position.ComboKey),
		HasPosition:        models.SignalHasOpenPosition,
		OrderType:          models.OrderTypeMarket,
		Entry:              position.EntryPrice,
		SL:                 position.StopLossPrice,
		TP:                 position.TakeProfitPrice,
		Action:             8,
		HighSide:           highSide,
		MidSide:            highSide,
		TrendingTimestamp:  triggerTS,
		TriggerTimestamp:   triggerTS,
		Strategy:           strategy,
		StrategyTimeframes: append([]string(nil), position.StrategyTimeframes...),
		StrategyVersion:    strings.TrimSpace(position.StrategyVersion),
	}, true
}

func inferPositionSignalTimestamp(position models.Position) int {
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"}
	for _, raw := range []string{position.EntryTime, position.UpdatedTime} {
		value := strings.TrimSpace(raw)
		if value == "" {
			continue
		}
		for _, layout := range layouts {
			var (
				parsed time.Time
				err    error
			)
			if layout == "2006-01-02 15:04:05" {
				parsed, err = time.ParseInLocation(layout, value, time.Local)
			} else {
				parsed, err = time.Parse(layout, value)
			}
			if err == nil {
				return int(parsed.UnixMilli())
			}
		}
	}
	return int(time.Now().UnixMilli())
}

type openPair struct {
	Exchange string
	Symbol   string
}

func (r *Live) openPositionPairs() []openPair {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]openPair)
	for _, pos := range r.positions {
		if !isPositionOpen(pos) {
			continue
		}
		exchange := normalizeExchange(pos.Exchange)
		symbol := canonicalSymbol(pos.Symbol)
		if exchange == "" || symbol == "" {
			continue
		}
		key := exchange + "|" + symbol
		seen[key] = openPair{Exchange: exchange, Symbol: symbol}
	}
	out := make([]openPair, 0, len(seen))
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
