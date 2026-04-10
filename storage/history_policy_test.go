package storage

import "testing"

func TestParseHistoryPolicy_DefaultAndNormalize(t *testing.T) {
	raw := `{
		"version": 1,
		"default": {"max_history_bars": 2000},
		"cleanup": {"enabled": true}
	}`
	policy, err := ParseHistoryPolicy(raw)
	if err != nil {
		t.Fatalf("parse history policy failed: %v", err)
	}
	if !policy.Enabled {
		t.Fatalf("history policy should be enabled")
	}
	if policy.Cleanup.IntervalSeconds != defaultHistoryPolicyCleanupIntervalSecs {
		t.Fatalf("cleanup interval = %d, want %d", policy.Cleanup.IntervalSeconds, defaultHistoryPolicyCleanupIntervalSecs)
	}
	got, ok := policy.MaxBarsFor("OKX", "BTC/USDT")
	if !ok || got != 2000 {
		t.Fatalf("default max bars mismatch, got=%d ok=%v", got, ok)
	}
}

func TestParseHistoryPolicy_PrecedenceAndUnlimited(t *testing.T) {
	raw := `{
		"version": 1,
		"default": {"max_history_bars": 2000},
		"exchanges": {"okx": {"max_history_bars": 1500}},
		"symbols": {
			"okx|btc/usdt": {"max_history_bars": 0},
			"okx|eth/usdt": {"max_history_bars": 1800}
		},
		"cleanup": {"enabled": true, "interval_seconds": 3600}
	}`
	policy, err := ParseHistoryPolicy(raw)
	if err != nil {
		t.Fatalf("parse history policy failed: %v", err)
	}
	if got, ok := policy.MaxBarsFor("okx", "BTC/USDT"); ok || got != 0 {
		t.Fatalf("symbol max_history_bars=0 should disable retention, got=%d ok=%v", got, ok)
	}
	if got, ok := policy.MaxBarsFor("okx", "ETH/USDT"); !ok || got != 1800 {
		t.Fatalf("symbol override mismatch, got=%d ok=%v", got, ok)
	}
	if got, ok := policy.MaxBarsFor("okx", "SOL/USDT"); !ok || got != 1500 {
		t.Fatalf("exchange override mismatch, got=%d ok=%v", got, ok)
	}
	if got, ok := policy.MaxBarsFor("binance", "SOL/USDT"); !ok || got != 2000 {
		t.Fatalf("default fallback mismatch, got=%d ok=%v", got, ok)
	}
}

func TestParseHistoryPolicy_InvalidSymbolKey(t *testing.T) {
	raw := `{
		"version": 1,
		"default": {"max_history_bars": 2000},
		"symbols": {"okx|btc/usdt|1h": {"max_history_bars": 100}},
		"cleanup": {"enabled": true, "interval_seconds": 3600}
	}`
	if _, err := ParseHistoryPolicy(raw); err == nil {
		t.Fatalf("expected parse history policy to reject invalid symbol key")
	}
}
