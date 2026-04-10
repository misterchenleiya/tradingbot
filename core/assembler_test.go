package core

import (
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestTimeframeAssembler_On1mAggregatesAndCloses(t *testing.T) {
	assembler := newTimeframeAssembler()
	targets := []string{"3m"}

	events := []models.MarketData{
		{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "1m",
			OHLCV:     models.OHLCV{TS: 1, Open: 100, High: 102, Low: 99, Close: 101, Volume: 10},
			Closed:    true,
			Source:    "live",
		},
		{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "1m",
			OHLCV:     models.OHLCV{TS: 60_001, Open: 101, High: 103, Low: 100, Close: 102, Volume: 20},
			Closed:    true,
			Source:    "live",
		},
		{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "1m",
			OHLCV:     models.OHLCV{TS: 120_001, Open: 102, High: 104, Low: 101, Close: 103, Volume: 30},
			Closed:    true,
			Source:    "live",
		},
	}

	var last models.MarketData
	for i, item := range events {
		out := assembler.On1m(item, targets)
		if len(out) != 1 {
			t.Fatalf("event %d expected 1 assembled item, got %d", i, len(out))
		}
		last = out[0]
	}

	if last.Timeframe != "3m" {
		t.Fatalf("unexpected timeframe: %s", last.Timeframe)
	}
	if !last.Closed {
		t.Fatalf("expected last assembled bar to be closed")
	}
	if last.OHLCV.TS != 0 {
		t.Fatalf("unexpected bucket ts: %d", last.OHLCV.TS)
	}
	if last.OHLCV.Open != 100 || last.OHLCV.High != 104 || last.OHLCV.Low != 99 || last.OHLCV.Close != 103 {
		t.Fatalf("unexpected ohlcv: %+v", last.OHLCV)
	}
	if last.OHLCV.Volume != 60 {
		t.Fatalf("unexpected volume: %v", last.OHLCV.Volume)
	}
}

func TestTimeframeAssembler_On1mStartsNewBucket(t *testing.T) {
	assembler := newTimeframeAssembler()
	targets := []string{"3m"}

	_ = assembler.On1m(models.MarketData{
		Exchange:  "okx",
		Symbol:    "ETH/USDT",
		Timeframe: "1m",
		OHLCV:     models.OHLCV{TS: 120_000, Open: 10, High: 12, Low: 9, Close: 11, Volume: 1},
		Closed:    true,
	}, targets)

	out := assembler.On1m(models.MarketData{
		Exchange:  "okx",
		Symbol:    "ETH/USDT",
		Timeframe: "1m",
		OHLCV:     models.OHLCV{TS: 180_000, Open: 20, High: 21, Low: 19, Close: 20.5, Volume: 2},
		Closed:    true,
		Source:    "rest",
	}, targets)
	if len(out) != 1 {
		t.Fatalf("expected 1 assembled item, got %d", len(out))
	}
	item := out[0]
	if item.OHLCV.TS != 180_000 {
		t.Fatalf("unexpected new bucket ts: %d", item.OHLCV.TS)
	}
	if item.Closed {
		t.Fatalf("first 1m of a 3m bucket must be unclosed")
	}
	if item.Source != "assembled:rest" {
		t.Fatalf("unexpected source: %s", item.Source)
	}
}

func TestTimeframeAssembler_OnTimeframeAggregatesAndClosesFrom3m(t *testing.T) {
	assembler := newTimeframeAssembler()
	targets := []string{"3m", "15m", "1h"}
	base := int64(900000)

	events := []models.MarketData{
		{
			Exchange:  "okx",
			Symbol:    "SOL/USDT",
			Timeframe: "3m",
			OHLCV:     models.OHLCV{TS: base, Open: 100, High: 102, Low: 99, Close: 101, Volume: 10},
			Closed:    true,
			Source:    "back-test",
		},
		{
			Exchange:  "okx",
			Symbol:    "SOL/USDT",
			Timeframe: "3m",
			OHLCV:     models.OHLCV{TS: base + 180_000, Open: 101, High: 103, Low: 100, Close: 102, Volume: 20},
			Closed:    true,
			Source:    "back-test",
		},
		{
			Exchange:  "okx",
			Symbol:    "SOL/USDT",
			Timeframe: "3m",
			OHLCV:     models.OHLCV{TS: base + 360_000, Open: 102, High: 104, Low: 101, Close: 103, Volume: 30},
			Closed:    true,
			Source:    "back-test",
		},
		{
			Exchange:  "okx",
			Symbol:    "SOL/USDT",
			Timeframe: "3m",
			OHLCV:     models.OHLCV{TS: base + 540_000, Open: 103, High: 105, Low: 102, Close: 104, Volume: 40},
			Closed:    true,
			Source:    "back-test",
		},
		{
			Exchange:  "okx",
			Symbol:    "SOL/USDT",
			Timeframe: "3m",
			OHLCV:     models.OHLCV{TS: base + 720_000, Open: 104, High: 106, Low: 103, Close: 105, Volume: 50},
			Closed:    true,
			Source:    "back-test",
		},
	}

	var got models.MarketData
	for i, item := range events {
		out := assembler.OnTimeframe(item, targets)
		if len(out) == 0 {
			t.Fatalf("event %d expected assembled items, got 0", i)
		}
		found := false
		for _, assembled := range out {
			if assembled.Timeframe != "15m" {
				continue
			}
			got = assembled
			found = true
			break
		}
		if !found {
			t.Fatalf("event %d expected assembled 15m item, got %v", i, out)
		}
	}

	if got.Timeframe != "15m" {
		t.Fatalf("unexpected timeframe: %s", got.Timeframe)
	}
	if !got.Closed {
		t.Fatalf("expected 15m bar to be closed")
	}
	if got.OHLCV.TS != base {
		t.Fatalf("unexpected bucket ts: %d", got.OHLCV.TS)
	}
	if got.OHLCV.Open != 100 || got.OHLCV.High != 106 || got.OHLCV.Low != 99 || got.OHLCV.Close != 105 {
		t.Fatalf("unexpected ohlcv: %+v", got.OHLCV)
	}
	if got.OHLCV.Volume != 150 {
		t.Fatalf("unexpected volume: %v", got.OHLCV.Volume)
	}
}

func TestNormalizePlanTimeframes_KeepConfiguredOnly(t *testing.T) {
	got := normalizePlanTimeframes([]string{"15m", "1h"})
	if len(got) != 2 {
		t.Fatalf("expected 2 timeframes, got %d: %#v", len(got), got)
	}
	if got[0] != "15m" || got[1] != "1h" {
		t.Fatalf("unexpected order: %#v", got)
	}
}

func TestEnsureOneMinuteTimeframe_AppendsWhenMissing(t *testing.T) {
	got := ensureOneMinuteTimeframe([]string{"15m", "1h"})
	if len(got) != 3 {
		t.Fatalf("expected 3 timeframes, got %d: %#v", len(got), got)
	}
	if got[0] != "1m" || got[1] != "15m" || got[2] != "1h" {
		t.Fatalf("unexpected order: %#v", got)
	}
}
