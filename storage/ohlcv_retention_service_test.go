package storage

import (
	"context"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestDeleteOHLCVKeepLatestBars(t *testing.T) {
	store := NewSQLite(Config{Path: ":memory:"})
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("start sqlite failed: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	if err := store.EnsureSchema(); err != nil {
		t.Fatalf("ensure schema failed: %v", err)
	}
	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	const totalBars = 10
	const keepBars = 3
	step := int64(time.Hour / time.Millisecond)
	baseTS := int64(1700000000000)
	for i := 0; i < totalBars; i++ {
		ts := baseTS + int64(i)*step
		if err := store.SaveOHLCV(models.MarketData{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "1h",
			OHLCV: models.OHLCV{
				TS:     ts,
				Open:   1,
				High:   2,
				Low:    1,
				Close:  2,
				Volume: 1,
			},
			Closed: true,
			Source: "live",
		}); err != nil {
			t.Fatalf("save ohlcv failed: %v", err)
		}
	}

	targets, err := store.ListOHLCVRetentionTargets()
	if err != nil {
		t.Fatalf("list retention targets failed: %v", err)
	}
	if len(targets) == 0 {
		t.Fatalf("expected retention targets")
	}

	deleted, err := store.DeleteOHLCVKeepLatestBars("okx", "BTC/USDT", "1h", keepBars)
	if err != nil {
		t.Fatalf("delete keep latest failed: %v", err)
	}
	if deleted != totalBars-keepBars {
		t.Fatalf("deleted bars = %d, want %d", deleted, totalBars-keepBars)
	}

	series, err := store.ListRecentOHLCV("okx", "BTC/USDT", "1h", 20)
	if err != nil {
		t.Fatalf("list recent ohlcv failed: %v", err)
	}
	if len(series) != keepBars {
		t.Fatalf("remaining bars = %d, want %d", len(series), keepBars)
	}
	wantFirstTS := baseTS + int64(totalBars-keepBars)*step
	if series[0].TS != wantFirstTS {
		t.Fatalf("first ts = %d, want %d", series[0].TS, wantFirstTS)
	}
}
