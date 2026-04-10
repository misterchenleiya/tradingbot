package models

import (
	"encoding/json"
	"sort"
	"strings"
	"time"
)

type StrategyContextMeta struct {
	StrategyName       string              `json:"strategy_name,omitempty"`
	StrategyVersion    string              `json:"strategy_version,omitempty"`
	TrendingTimestamp  int                 `json:"trending_timestamp,omitempty"`
	StrategyTimeframes []string            `json:"strategy_timeframes,omitempty"`
	ComboKey           string              `json:"combo_key,omitempty"`
	GroupID            string              `json:"group_id,omitempty"`
	StrategyIndicators map[string][]string `json:"strategy_indicators,omitempty"`
}

type PositionRuntimeMeta struct {
	RunID       string `json:"run_id,omitempty"`
	SingletonID int64  `json:"singleton_id,omitempty"`
}

type PositionRowEnvelope struct {
	ExchangeRaw json.RawMessage     `json:"exchange_raw,omitempty"`
	GobotMeta   StrategyContextMeta `json:"gobot_meta,omitempty"`
	RuntimeMeta PositionRuntimeMeta `json:"runtime_meta,omitempty"`
}

func BuildStrategyContextMetaFromSignal(signal Signal) StrategyContextMeta {
	return NormalizeStrategyContextMeta(StrategyContextMeta{
		StrategyName:       signal.Strategy,
		StrategyVersion:    signal.StrategyVersion,
		TrendingTimestamp:  signal.TrendingTimestamp,
		StrategyTimeframes: signal.StrategyTimeframes,
		GroupID:            signal.GroupID,
		StrategyIndicators: signal.StrategyIndicators,
	})
}

func NormalizeStrategyContextMeta(meta StrategyContextMeta) StrategyContextMeta {
	meta.StrategyName = strings.TrimSpace(meta.StrategyName)
	meta.StrategyVersion = strings.TrimSpace(meta.StrategyVersion)
	if meta.TrendingTimestamp < 0 {
		meta.TrendingTimestamp = 0
	}
	_, meta.StrategyTimeframes, meta.ComboKey = normalizeStrategyIdentityMeta("", meta.StrategyTimeframes, meta.ComboKey)
	meta.GroupID = strings.TrimSpace(meta.GroupID)
	meta.StrategyIndicators = normalizeIndicatorMap(meta.StrategyIndicators)
	return meta
}

func MergeStrategyContextMeta(primary, fallback StrategyContextMeta) StrategyContextMeta {
	primary = NormalizeStrategyContextMeta(primary)
	fallback = NormalizeStrategyContextMeta(fallback)
	if primary.StrategyName == "" {
		primary.StrategyName = fallback.StrategyName
	}
	if primary.StrategyVersion == "" {
		primary.StrategyVersion = fallback.StrategyVersion
	}
	if primary.TrendingTimestamp <= 0 {
		primary.TrendingTimestamp = fallback.TrendingTimestamp
	}
	if len(primary.StrategyTimeframes) == 0 {
		primary.StrategyTimeframes = fallback.StrategyTimeframes
	}
	if primary.ComboKey == "" {
		primary.ComboKey = fallback.ComboKey
	}
	if primary.GroupID == "" {
		primary.GroupID = fallback.GroupID
	}
	if len(primary.StrategyIndicators) == 0 {
		primary.StrategyIndicators = fallback.StrategyIndicators
	}
	return NormalizeStrategyContextMeta(primary)
}

func (m StrategyContextMeta) IsEmpty() bool {
	normalized := NormalizeStrategyContextMeta(m)
	return normalized.StrategyName == "" &&
		normalized.StrategyVersion == "" &&
		normalized.TrendingTimestamp <= 0 &&
		len(normalized.StrategyTimeframes) == 0 &&
		normalized.ComboKey == "" &&
		normalized.GroupID == "" &&
		len(normalized.StrategyIndicators) == 0
}

func NormalizePositionRuntimeMeta(meta PositionRuntimeMeta) PositionRuntimeMeta {
	meta.RunID = strings.TrimSpace(meta.RunID)
	if meta.SingletonID < 0 {
		meta.SingletonID = 0
	}
	return meta
}

func (m PositionRuntimeMeta) IsEmpty() bool {
	normalized := NormalizePositionRuntimeMeta(m)
	return normalized.RunID == "" && normalized.SingletonID <= 0
}

func MarshalPositionRowEnvelope(exchangeRaw any, meta StrategyContextMeta) string {
	return MarshalPositionRowEnvelopeWithRuntime(exchangeRaw, meta, PositionRuntimeMeta{})
}

func MarshalPositionRowEnvelopeWithRuntime(exchangeRaw any, meta StrategyContextMeta, runtime PositionRuntimeMeta) string {
	var raw json.RawMessage
	switch value := exchangeRaw.(type) {
	case nil:
		raw = nil
	case json.RawMessage:
		raw = append(json.RawMessage(nil), value...)
	case []byte:
		raw = append(json.RawMessage(nil), value...)
	default:
		payload, err := json.Marshal(exchangeRaw)
		if err != nil {
			return ""
		}
		raw = payload
	}
	meta = NormalizeStrategyContextMeta(meta)
	runtime = NormalizePositionRuntimeMeta(runtime)
	if len(raw) == 0 && meta.IsEmpty() && runtime.IsEmpty() {
		return ""
	}
	env := PositionRowEnvelope{
		ExchangeRaw: raw,
		GobotMeta:   meta,
		RuntimeMeta: runtime,
	}
	payload, err := json.Marshal(env)
	if err != nil {
		if len(raw) > 0 {
			return string(raw)
		}
		return ""
	}
	return string(payload)
}

func ParsePositionRowEnvelope(raw string) (PositionRowEnvelope, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return PositionRowEnvelope{}, false
	}
	var env PositionRowEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return PositionRowEnvelope{}, false
	}
	env.GobotMeta = NormalizeStrategyContextMeta(env.GobotMeta)
	env.RuntimeMeta = NormalizePositionRuntimeMeta(env.RuntimeMeta)
	if len(env.ExchangeRaw) == 0 && env.GobotMeta.IsEmpty() && env.RuntimeMeta.IsEmpty() {
		return PositionRowEnvelope{}, false
	}
	return env, true
}

func ExtractStrategyContextMeta(raw string) StrategyContextMeta {
	env, ok := ParsePositionRowEnvelope(raw)
	if !ok {
		return StrategyContextMeta{}
	}
	return env.GobotMeta
}

func ExtractPositionRuntimeMeta(raw string) PositionRuntimeMeta {
	env, ok := ParsePositionRowEnvelope(raw)
	if !ok {
		return PositionRuntimeMeta{}
	}
	return NormalizePositionRuntimeMeta(env.RuntimeMeta)
}

func normalizeStringSlice(values []string) []string {
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

func normalizeIndicatorMap(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for rawName, values := range input {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		normalizedValues := normalizeStringSlice(values)
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

func normalizeStrategyIdentityMeta(primary string, timeframes []string, comboKey string) (string, []string, string) {
	out := make([]string, 0, len(timeframes)+4)
	appendTimeframe := func(value string) {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			return
		}
		out = append(out, value)
	}
	appendTimeframe(primary)
	for _, item := range timeframes {
		appendTimeframe(item)
	}
	for _, item := range strings.Split(strings.TrimSpace(comboKey), "/") {
		appendTimeframe(item)
	}
	if len(out) == 0 {
		return "", nil, ""
	}
	seen := make(map[string]struct{}, len(out))
	normalized := make([]string, 0, len(out))
	for _, item := range out {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		normalized = append(normalized, item)
	}
	sort.SliceStable(normalized, func(i, j int) bool {
		leftDur, leftOK := timeframeDurationMeta(normalized[i])
		rightDur, rightOK := timeframeDurationMeta(normalized[j])
		switch {
		case leftOK && rightOK:
			if leftDur != rightDur {
				return leftDur < rightDur
			}
		case leftOK:
			return true
		case rightOK:
			return false
		}
		return normalized[i] < normalized[j]
	})
	primary = normalized[len(normalized)-1]
	return primary, normalized, strings.Join(normalized, "/")
}

func timeframeDurationMeta(timeframe string) (time.Duration, bool) {
	switch strings.ToLower(strings.TrimSpace(timeframe)) {
	case "1m":
		return time.Minute, true
	case "3m":
		return 3 * time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return time.Hour, true
	case "2h":
		return 2 * time.Hour, true
	case "4h":
		return 4 * time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "8h":
		return 8 * time.Hour, true
	case "12h":
		return 12 * time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}
