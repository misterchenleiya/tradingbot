package main

import (
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/core"
	"github.com/misterchenleiya/tradingbot/risk"
)

func TestBuildBackTestEventColumnsAndRowsOmitRemovedFields(t *testing.T) {
	t.Helper()

	columns := buildBackTestEventColumns("CST+08:00")
	for _, disallowed := range []string{"trending_time(CST+08:00)", "risk_error", "execution_error"} {
		for _, col := range columns {
			if col == disallowed {
				t.Fatalf("unexpected column %q", disallowed)
			}
		}
	}

	rows := buildBackTestEventRows([]core.BackTestEvent{{
		EventID:         1,
		EventTS:         1710000000000,
		Call:            "update",
		EventType:       "changed",
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		Timeframe:       "1h",
		Strategy:        "turtle",
		StrategyVersion: "v0.0.7",
		ChangedFields:   "action",
		Action:          8,
		HighSide:        1,
		MidSide:         1,
		Entry:           1,
		Exit:            2,
		SL:              3,
		TP:              4,
		TriggerTS:       1710000000000,
		TrendingTS:      1710000000000,
		ExecResult:      "open_filled",
		RiskError:       "risk rejected",
		ExecutionError:  "execution failed",
	}}, time.FixedZone("CST", 8*3600))
	if len(rows) != 1 {
		t.Fatalf("unexpected row count: got %d want 1", len(rows))
	}
	if len(rows[0]) != len(columns) {
		t.Fatalf("row/column length mismatch: got %d columns %d", len(rows[0]), len(columns))
	}
}

func TestBuildBackTestPositionEventColumnsAndRowsOmitRemovedFields(t *testing.T) {
	t.Helper()

	columns := buildBackTestPositionEventColumns("CST+08:00")
	for _, disallowed := range []string{"close_reason", "reason", "strategy_version"} {
		for _, col := range columns {
			if col == disallowed {
				t.Fatalf("unexpected column %q", disallowed)
			}
		}
	}

	rows := buildBackTestPositionEventRows([]risk.BackTestPositionEvent{{
		EventID:           1,
		EventTS:           1710000000000,
		KlineTS:           1710000000000,
		Exchange:          "okx",
		Symbol:            "BTC/USDT",
		Timeframe:         "1h",
		Side:              "long",
		Strategy:          "turtle",
		StrategyVersion:   "v0.0.7",
		Action:            "open",
		Price:             100,
		Quantity:          1,
		RemainingQuantity: 1,
		Margin:            100,
		Leverage:          3,
		TakeProfitPrice:   110,
		StopLossPrice:     95,
		CloseReason:       "signal_full_close",
		Result:            "success",
		Reason:            "should be hidden",
	}}, time.FixedZone("CST", 8*3600))
	if len(rows) != 1 {
		t.Fatalf("unexpected row count: got %d want 1", len(rows))
	}
	if len(rows[0]) != len(columns) {
		t.Fatalf("row/column length mismatch: got %d columns %d", len(rows[0]), len(columns))
	}
}
