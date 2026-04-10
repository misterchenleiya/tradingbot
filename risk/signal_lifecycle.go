package risk

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

func normalizeRiskSignal(signal models.Signal, exchange, symbol, timeframe string) models.Signal {
	signal.Exchange = firstNonEmpty(signal.Exchange, exchange)
	signal.Symbol = firstNonEmpty(signal.Symbol, symbol)
	signal.Timeframe = firstNonEmpty(signal.Timeframe, timeframe)
	signal.Exchange = normalizeExchange(signal.Exchange)
	signal.Symbol = common.CanonicalSymbol(signal.Symbol)
	signal.Strategy = strings.TrimSpace(signal.Strategy)
	signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey = common.NormalizeStrategyIdentity(
		signal.Timeframe,
		signal.StrategyTimeframes,
		signal.ComboKey,
	)
	return signal
}

func isSignalKeyReady(signal models.Signal) bool {
	return normalizeExchange(signal.Exchange) != "" &&
		common.CanonicalSymbol(signal.Symbol) != "" &&
		strings.TrimSpace(signal.Timeframe) != "" &&
		strings.TrimSpace(signal.ComboKey) != "" &&
		strings.TrimSpace(signal.Strategy) != ""
}

func isSignalEqual(left, right models.Signal) bool {
	return normalizeExchange(left.Exchange) == normalizeExchange(right.Exchange) &&
		common.CanonicalSymbol(left.Symbol) == common.CanonicalSymbol(right.Symbol) &&
		strings.TrimSpace(left.Timeframe) == strings.TrimSpace(right.Timeframe) &&
		strings.TrimSpace(left.ComboKey) == strings.TrimSpace(right.ComboKey) &&
		strings.TrimSpace(left.Strategy) == strings.TrimSpace(right.Strategy) &&
		strings.TrimSpace(left.StrategyVersion) == strings.TrimSpace(right.StrategyVersion) &&
		strings.TrimSpace(left.OrderType) == strings.TrimSpace(right.OrderType) &&
		left.HasPosition == right.HasPosition &&
		left.Action == right.Action &&
		left.HighSide == right.HighSide &&
		left.MidSide == right.MidSide &&
		left.TrendEntryCount == right.TrendEntryCount &&
		left.MidPullbackCount == right.MidPullbackCount &&
		left.LastMidPullbackTS == right.LastMidPullbackTS &&
		left.RequireHighPullbackReset == right.RequireHighPullbackReset &&
		left.StageEntryUsed == right.StageEntryUsed &&
		left.PostHighPullbackFirstEntryState == right.PostHighPullbackFirstEntryState &&
		left.EntryWatchTimestamp == right.EntryWatchTimestamp &&
		left.TriggerTimestamp == right.TriggerTimestamp &&
		left.TrendingTimestamp == right.TrendingTimestamp &&
		floatcmp.EQ(left.Amount, right.Amount) &&
		floatcmp.EQ(left.Entry, right.Entry) &&
		floatcmp.EQ(left.Exit, right.Exit) &&
		floatcmp.EQ(left.SL, right.SL) &&
		floatcmp.EQ(left.TP, right.TP) &&
		floatcmp.EQ(left.InitialSL, right.InitialSL) &&
		floatcmp.EQ(left.InitialRiskPct, right.InitialRiskPct) &&
		floatcmp.EQ(left.MaxFavorableProfitPct, right.MaxFavorableProfitPct) &&
		left.ProfitProtectStage == right.ProfitProtectStage &&
		floatcmp.EQ(left.Plan1LastProfitLockMFER, right.Plan1LastProfitLockMFER) &&
		left.Plan1LastProfitLockHighBucketTS == right.Plan1LastProfitLockHighBucketTS &&
		floatcmp.EQ(left.Plan1LastProfitLockStructPrice, right.Plan1LastProfitLockStructPrice) &&
		strategyTimeframesEqual(left.StrategyTimeframes, right.StrategyTimeframes) &&
		strategyIndicatorsEqual(left.StrategyIndicators, right.StrategyIndicators) &&
		triggerHistoryEqual(left.TriggerHistory, right.TriggerHistory)
}

func signalChangedFields(previous, next models.Signal) string {
	changed := make([]string, 0, 24)
	if previous.HasPosition != next.HasPosition {
		changed = append(changed, "has_position")
	}
	if previous.Action != next.Action {
		changed = append(changed, "action")
		if previous.Action == 4 || next.Action == 4 {
			changed = append(changed, "armed")
		}
	}
	if previous.HighSide != next.HighSide {
		changed = append(changed, "high_side")
	}
	if previous.MidSide != next.MidSide {
		changed = append(changed, "mid_side")
	}
	if previous.TrendEntryCount != next.TrendEntryCount {
		changed = append(changed, "trend_entry_count")
	}
	if previous.MidPullbackCount != next.MidPullbackCount {
		changed = append(changed, "mid_pullback_count")
	}
	if previous.LastMidPullbackTS != next.LastMidPullbackTS {
		changed = append(changed, "last_mid_pullback_ts")
	}
	if previous.RequireHighPullbackReset != next.RequireHighPullbackReset {
		changed = append(changed, "require_high_pullback_reset")
	}
	if previous.StageEntryUsed != next.StageEntryUsed {
		changed = append(changed, "stage_entry_used")
	}
	if previous.PostHighPullbackFirstEntryState != next.PostHighPullbackFirstEntryState {
		changed = append(changed, "post_high_pullback_first_entry_state")
	}
	if previous.EntryWatchTimestamp != next.EntryWatchTimestamp {
		changed = append(changed, "entry_watch_timestamp")
	}
	if previous.OrderType != next.OrderType {
		changed = append(changed, "order_type")
	}
	if !floatcmp.EQ(previous.Amount, next.Amount) {
		changed = append(changed, "amount")
	}
	if !floatcmp.EQ(previous.Entry, next.Entry) {
		changed = append(changed, "entry")
	}
	if !floatcmp.EQ(previous.Exit, next.Exit) {
		changed = append(changed, "exit")
	}
	if !floatcmp.EQ(previous.SL, next.SL) {
		changed = append(changed, "sl")
	}
	if !floatcmp.EQ(previous.TP, next.TP) {
		changed = append(changed, "tp")
	}
	if !floatcmp.EQ(previous.InitialSL, next.InitialSL) {
		changed = append(changed, "initial_sl")
	}
	if !floatcmp.EQ(previous.InitialRiskPct, next.InitialRiskPct) {
		changed = append(changed, "initial_risk_pct")
	}
	if !floatcmp.EQ(previous.MaxFavorableProfitPct, next.MaxFavorableProfitPct) {
		changed = append(changed, "max_favorable_profit_pct")
	}
	if previous.ProfitProtectStage != next.ProfitProtectStage {
		changed = append(changed, "profit_protect_stage")
	}
	if !floatcmp.EQ(previous.Plan1LastProfitLockMFER, next.Plan1LastProfitLockMFER) {
		changed = append(changed, "plan1_last_profit_lock_mfer")
	}
	if previous.Plan1LastProfitLockHighBucketTS != next.Plan1LastProfitLockHighBucketTS {
		changed = append(changed, "plan1_last_profit_lock_high_bucket_ts")
	}
	if !floatcmp.EQ(previous.Plan1LastProfitLockStructPrice, next.Plan1LastProfitLockStructPrice) {
		changed = append(changed, "plan1_last_profit_lock_struct_price")
	}
	if previous.TriggerTimestamp != next.TriggerTimestamp {
		changed = append(changed, "trigger_timestamp")
	}
	if previous.TrendingTimestamp != next.TrendingTimestamp {
		changed = append(changed, "trending_timestamp")
	}
	if previous.StrategyVersion != next.StrategyVersion {
		changed = append(changed, "strategy_version")
	}
	if !strategyTimeframesEqual(previous.StrategyTimeframes, next.StrategyTimeframes) {
		changed = append(changed, "strategy_timeframes")
	}
	if !strategyIndicatorsEqual(previous.StrategyIndicators, next.StrategyIndicators) {
		changed = append(changed, "strategy_indicators")
	}
	if !triggerHistoryEqual(previous.TriggerHistory, next.TriggerHistory) {
		changed = append(changed, "trigger_history")
	}
	if len(changed) == 0 {
		return ""
	}
	sort.Strings(changed)
	return strings.Join(changed, ",")
}

func triggerHistoryEqual(left, right []models.TriggerHistoryRecord) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Action != right[i].Action {
			return false
		}
		if left[i].MidSide != right[i].MidSide {
			return false
		}
		if left[i].TriggerTimestamp != right[i].TriggerTimestamp {
			return false
		}
	}
	return true
}

func strategyTimeframesEqual(left, right []string) bool {
	left = normalizeSignalTextSlice(left)
	right = normalizeSignalTextSlice(right)
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func strategyIndicatorsEqual(left, right map[string][]string) bool {
	left = normalizeSignalIndicatorMap(left)
	right = normalizeSignalIndicatorMap(right)
	if len(left) != len(right) {
		return false
	}
	for name, leftValues := range left {
		rightValues, ok := right[name]
		if !ok {
			return false
		}
		if !strategyTimeframesEqual(leftValues, rightValues) {
			return false
		}
	}
	return true
}

func normalizeSignalTextSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, item := range values {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeSignalIndicatorMap(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for rawName, values := range input {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		normalizedValues := normalizeSignalTextSlice(values)
		if len(normalizedValues) == 0 {
			continue
		}
		out[name] = normalizedValues
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func decodeSignal(raw string) (models.Signal, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return models.Signal{}, false
	}
	var signal models.Signal
	if err := json.Unmarshal([]byte(raw), &signal); err != nil {
		return models.Signal{}, false
	}
	return signal, true
}

func groupSignalEvents(records []models.SignalChangeRecord) map[SignalKey][]models.SignalChangeRecord {
	grouped := make(map[SignalKey][]models.SignalChangeRecord)
	for _, item := range records {
		signal, _ := decodeSignal(item.SignalJSON)
		signal = normalizeRiskSignal(signal, item.Exchange, item.Symbol, item.Timeframe)
		key := SignalKey{
			Exchange: normalizeExchange(item.Exchange),
			Symbol:   common.CanonicalSymbol(item.Symbol),
			Strategy: strings.TrimSpace(item.Strategy),
			ComboKey: strings.TrimSpace(signal.ComboKey),
		}
		if key.Exchange == "" || key.Symbol == "" || key.Strategy == "" || key.ComboKey == "" {
			continue
		}
		grouped[key] = append(grouped[key], item)
	}
	return grouped
}

func rebuildSignalFromEvents(records []models.SignalChangeRecord) (models.Signal, bool) {
	if len(records) == 0 {
		return models.Signal{}, false
	}
	start := -1
	for i := len(records) - 1; i >= 0; i-- {
		if records[i].ChangeStatus == models.SignalChangeStatusNew {
			start = i
			break
		}
	}
	if start < 0 {
		return models.Signal{}, false
	}
	current := models.Signal{}
	found := false
	for i := start; i < len(records); i++ {
		row := records[i]
		if row.ChangeStatus == models.SignalChangeStatusGone {
			return models.Signal{}, false
		}
		signal, ok := decodeSignal(row.SignalJSON)
		if !ok {
			continue
		}
		current = signal
		found = true
	}
	if !found || !isSignalKeyReady(current) {
		return models.Signal{}, false
	}
	return current, true
}
