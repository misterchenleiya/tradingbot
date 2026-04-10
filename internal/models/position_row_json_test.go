package models

import (
	"encoding/json"
	"testing"
)

func TestStrategyContextMetaBuildAndNormalize(t *testing.T) {
	signal := Signal{
		Strategy:          " turtle ",
		StrategyVersion:   " v0.0.5 ",
		TrendingTimestamp: 123456,
		GroupID:           " turtle|1h|long|123456 ",
		StrategyTimeframes: []string{
			"15m",
			"1h",
			"15m",
			" ",
		},
		StrategyIndicators: map[string][]string{
			"ema": []string{"5", "20", "60", "20"},
			"":    []string{"x"},
		},
	}
	meta := BuildStrategyContextMetaFromSignal(signal)
	if meta.StrategyName != "turtle" {
		t.Fatalf("unexpected strategy name: %s", meta.StrategyName)
	}
	if meta.StrategyVersion != "v0.0.5" {
		t.Fatalf("unexpected strategy version: %s", meta.StrategyVersion)
	}
	if meta.GroupID != "turtle|1h|long|123456" {
		t.Fatalf("unexpected group_id: %s", meta.GroupID)
	}
	if len(meta.StrategyTimeframes) != 2 || meta.StrategyTimeframes[0] != "15m" || meta.StrategyTimeframes[1] != "1h" {
		t.Fatalf("unexpected strategy_timeframes: %#v", meta.StrategyTimeframes)
	}
	values := meta.StrategyIndicators["ema"]
	if len(values) != 3 || values[0] != "5" || values[1] != "20" || values[2] != "60" {
		t.Fatalf("unexpected strategy_indicators.ema: %#v", values)
	}
}

func TestMergeStrategyContextMeta(t *testing.T) {
	base := StrategyContextMeta{
		StrategyName:    "turtle",
		StrategyVersion: "v0.0.5",
	}
	fallback := StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.3",
		TrendingTimestamp:  100,
		StrategyTimeframes: []string{"15m", "1h"},
		GroupID:            "turtle|1h|short|100",
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
	}
	merged := MergeStrategyContextMeta(base, fallback)
	if merged.StrategyName != "turtle" {
		t.Fatalf("unexpected merged strategy_name: %s", merged.StrategyName)
	}
	if merged.TrendingTimestamp != 100 {
		t.Fatalf("unexpected merged trending_timestamp: %d", merged.TrendingTimestamp)
	}
	if merged.GroupID != "turtle|1h|short|100" {
		t.Fatalf("unexpected merged group_id: %s", merged.GroupID)
	}
	if len(merged.StrategyTimeframes) != 2 {
		t.Fatalf("unexpected merged strategy_timeframes: %#v", merged.StrategyTimeframes)
	}
}

func TestPositionRowEnvelopeRoundTrip(t *testing.T) {
	type raw struct {
		InstID string `json:"instId"`
		Pos    string `json:"pos"`
	}
	payload := raw{InstID: "BTC-USDT-SWAP", Pos: "1"}
	meta := StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.5",
		TrendingTimestamp:  10,
		StrategyTimeframes: []string{"15m", "1h"},
		GroupID:            "turtle|1h|long|10",
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
	}
	runtime := PositionRuntimeMeta{
		RunID:       "run-a",
		SingletonID: 7,
	}
	encoded := MarshalPositionRowEnvelopeWithRuntime(payload, meta, runtime)
	if encoded == "" {
		t.Fatalf("marshal envelope returned empty string")
	}
	env, ok := ParsePositionRowEnvelope(encoded)
	if !ok {
		t.Fatalf("parse envelope failed")
	}
	var gotRaw raw
	if err := json.Unmarshal(env.ExchangeRaw, &gotRaw); err != nil {
		t.Fatalf("unmarshal exchange_raw failed: %v", err)
	}
	if gotRaw.InstID != payload.InstID || gotRaw.Pos != payload.Pos {
		t.Fatalf("unexpected exchange_raw: %+v", gotRaw)
	}
	if env.GobotMeta.StrategyName != "turtle" {
		t.Fatalf("unexpected gobot_meta.strategy_name: %s", env.GobotMeta.StrategyName)
	}
	if env.GobotMeta.GroupID != "turtle|1h|long|10" {
		t.Fatalf("unexpected gobot_meta.group_id: %s", env.GobotMeta.GroupID)
	}
	if env.RuntimeMeta.RunID != runtime.RunID || env.RuntimeMeta.SingletonID != runtime.SingletonID {
		t.Fatalf("unexpected runtime_meta: %+v", env.RuntimeMeta)
	}
}

func TestPositionRowEnvelopeRuntimeOnly(t *testing.T) {
	encoded := MarshalPositionRowEnvelopeWithRuntime(nil, StrategyContextMeta{}, PositionRuntimeMeta{
		RunID:       "run-b",
		SingletonID: 9,
	})
	if encoded == "" {
		t.Fatalf("marshal envelope returned empty string")
	}
	env, ok := ParsePositionRowEnvelope(encoded)
	if !ok {
		t.Fatalf("parse envelope failed")
	}
	if env.RuntimeMeta.RunID != "run-b" || env.RuntimeMeta.SingletonID != 9 {
		t.Fatalf("unexpected runtime_meta: %+v", env.RuntimeMeta)
	}
}

func TestParsePositionRowEnvelopeLegacyRawJSON(t *testing.T) {
	legacy := `{"instId":"BTC-USDT-SWAP","pos":"1"}`
	if _, ok := ParsePositionRowEnvelope(legacy); ok {
		t.Fatalf("legacy exchange raw json should not be treated as envelope")
	}
}
