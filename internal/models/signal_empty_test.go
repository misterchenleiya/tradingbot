package models

import "testing"

func TestIsEmptySignal_ZeroValue(t *testing.T) {
	if !IsEmptySignal(Signal{}) {
		t.Fatalf("zero-value signal should be empty")
	}
}

func TestIsEmptySignal_IgnoresRoutingFields(t *testing.T) {
	signal := Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		GroupID:            "turtle|1h|long|1773943200000",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.2",
		StrategyTimeframes: []string{"15m", "1h"},
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
		OHLCV: []OHLCV{
			{TS: 1, Close: 100},
		},
	}
	if !IsEmptySignal(signal) {
		t.Fatalf("signal should still be empty when only routing fields are set")
	}
}

func TestIsEmptySignal_NonEmptyWhenAnyBusinessFieldPresent(t *testing.T) {
	cases := []Signal{
		{HighSide: 1},
		{Action: 8},
		{Entry: 100},
		{StageEntryUsed: true},
		{PostHighPullbackFirstEntryState: SignalPostHighPullbackFirstEntryPending},
		{EntryWatchTimestamp: 456},
		{TriggerTimestamp: 123},
		{TriggerHistory: []TriggerHistoryRecord{{Action: 8, TriggerTimestamp: 123}}},
	}
	for i, signal := range cases {
		signal.Exchange = "okx"
		signal.Symbol = "BTC/USDT"
		signal.Strategy = "turtle"
		if IsEmptySignal(signal) {
			t.Fatalf("case %d should be non-empty: %+v", i, signal)
		}
	}
}

func TestClearSignalForRemoval_AlignedWithIsEmptySignal(t *testing.T) {
	original := Signal{
		Exchange:                        "okx",
		Symbol:                          "BTC/USDT",
		Timeframe:                       "1h",
		Strategy:                        "turtle",
		StrategyVersion:                 "v0.0.2",
		StrategyTimeframes:              []string{"15m", "1h"},
		StrategyIndicators:              map[string][]string{"ema": []string{"5", "20", "60"}},
		HasPosition:                     SignalHasOpenPosition,
		OrderType:                       OrderTypeLimit,
		Amount:                          100,
		Entry:                           101,
		Exit:                            99,
		SL:                              95,
		TP:                              110,
		Action:                          16,
		HighSide:                        1,
		MidSide:                         1,
		TrendingTimestamp:               123456,
		StageEntryUsed:                  true,
		PostHighPullbackFirstEntryState: SignalPostHighPullbackFirstEntryArmed,
		EntryWatchTimestamp:             123600,
		TriggerTimestamp:                123789,
		TriggerHistory: []TriggerHistoryRecord{
			{Action: 8, MidSide: 1, TriggerTimestamp: 123456},
		},
	}

	cleared := ClearSignalForRemoval(original)
	if !IsEmptySignal(cleared) {
		t.Fatalf("cleared signal should be empty: %+v", cleared)
	}
	if cleared.Exchange != original.Exchange ||
		cleared.Symbol != original.Symbol ||
		cleared.Timeframe != original.Timeframe ||
		cleared.Strategy != original.Strategy ||
		cleared.StrategyVersion != original.StrategyVersion {
		t.Fatalf("routing fields should be preserved, got %+v", cleared)
	}
	if len(cleared.StrategyTimeframes) != 2 || len(cleared.StrategyIndicators["ema"]) != 3 {
		t.Fatalf("strategy metadata fields should be preserved, got %+v", cleared)
	}
}
