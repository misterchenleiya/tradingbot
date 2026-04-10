package storage

import (
	"context"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestListOHLCVRangeAllowSingleTimestamp(t *testing.T) {
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

	ts := int64(1700000000000)
	if err := store.SaveOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		OHLCV: models.OHLCV{
			TS:     ts,
			Open:   100,
			High:   110,
			Low:    90,
			Close:  105,
			Volume: 1,
		},
		Closed: true,
		Source: "live",
	}); err != nil {
		t.Fatalf("save ohlcv failed: %v", err)
	}

	items, err := store.ListOHLCVRange("okx", "BTC/USDT", "1h", time.UnixMilli(ts), time.UnixMilli(ts))
	if err != nil {
		t.Fatalf("list ohlcv single-point range failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("single-point range len=%d, want 1", len(items))
	}
	if items[0].TS != ts {
		t.Fatalf("single-point range ts=%d, want %d", items[0].TS, ts)
	}
}
