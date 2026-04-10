package storage

import (
	"context"
	"testing"
)

func TestResetTradeCooldownForTradeDate(t *testing.T) {
	store := newCooldownTestStore(t)

	rows := []struct {
		exchange      string
		tradeDate     string
		lossLimitUSDT float64
		realizedUSDT  float64
		updatedAtMS   int64
	}{
		{exchange: "okx", tradeDate: "2026-03-04", lossLimitUSDT: 100, realizedUSDT: 120, updatedAtMS: 10},
		{exchange: "binance", tradeDate: "2026-03-04", lossLimitUSDT: 50, realizedUSDT: 50, updatedAtMS: 20},
		{exchange: "bitget", tradeDate: "2026-03-04", lossLimitUSDT: 0, realizedUSDT: 100, updatedAtMS: 30},
		{exchange: "hyperliquid", tradeDate: "2026-03-03", lossLimitUSDT: 10, realizedUSDT: 20, updatedAtMS: 40},
		{exchange: "kraken", tradeDate: "2026-03-04", lossLimitUSDT: 50, realizedUSDT: 40, updatedAtMS: 50},
	}
	for _, row := range rows {
		if _, err := store.DB.Exec(
			`INSERT INTO risk_account_states (
			     exchange, trade_date, daily_loss_limit_usdt, daily_realized_usdt, updated_at_ms
			 )
			 VALUES (?, ?, ?, ?, ?);`,
			row.exchange,
			row.tradeDate,
			row.lossLimitUSDT,
			row.realizedUSDT,
			row.updatedAtMS,
		); err != nil {
			t.Fatalf("insert risk_account_states failed: %v", err)
		}
	}

	affected, exchanges, err := store.ResetTradeCooldownForTradeDate("2026-03-04")
	if err != nil {
		t.Fatalf("reset trade cooldown failed: %v", err)
	}
	if affected != 2 {
		t.Fatalf("affected=%d, want=2", affected)
	}

	wantExchanges := []string{"binance", "okx"}
	if len(exchanges) != len(wantExchanges) {
		t.Fatalf("exchanges len=%d, want=%d, got=%v", len(exchanges), len(wantExchanges), exchanges)
	}
	for i := range wantExchanges {
		if exchanges[i] != wantExchanges[i] {
			t.Fatalf("exchanges[%d]=%q, want=%q", i, exchanges[i], wantExchanges[i])
		}
	}

	okxRealized, okxUpdated := queryCooldownState(t, store, "okx")
	if okxRealized != 0 {
		t.Fatalf("okx daily_realized_usdt=%.4f, want=0", okxRealized)
	}
	if okxUpdated <= 10 {
		t.Fatalf("okx updated_at_ms=%d, want>10", okxUpdated)
	}

	binanceRealized, binanceUpdated := queryCooldownState(t, store, "binance")
	if binanceRealized != 0 {
		t.Fatalf("binance daily_realized_usdt=%.4f, want=0", binanceRealized)
	}
	if binanceUpdated <= 20 {
		t.Fatalf("binance updated_at_ms=%d, want>20", binanceUpdated)
	}

	bitgetRealized, bitgetUpdated := queryCooldownState(t, store, "bitget")
	if bitgetRealized != 100 {
		t.Fatalf("bitget daily_realized_usdt=%.4f, want=100", bitgetRealized)
	}
	if bitgetUpdated != 30 {
		t.Fatalf("bitget updated_at_ms=%d, want=30", bitgetUpdated)
	}

	hyperRealized, hyperUpdated := queryCooldownState(t, store, "hyperliquid")
	if hyperRealized != 20 {
		t.Fatalf("hyperliquid daily_realized_usdt=%.4f, want=20", hyperRealized)
	}
	if hyperUpdated != 40 {
		t.Fatalf("hyperliquid updated_at_ms=%d, want=40", hyperUpdated)
	}

	krakenRealized, krakenUpdated := queryCooldownState(t, store, "kraken")
	if krakenRealized != 40 {
		t.Fatalf("kraken daily_realized_usdt=%.4f, want=40", krakenRealized)
	}
	if krakenUpdated != 50 {
		t.Fatalf("kraken updated_at_ms=%d, want=50", krakenUpdated)
	}
}

func TestResetTradeCooldownForTradeDateNoMatch(t *testing.T) {
	store := newCooldownTestStore(t)

	if _, err := store.DB.Exec(
		`INSERT INTO risk_account_states (
		     exchange, trade_date, daily_loss_limit_usdt, daily_realized_usdt, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?);`,
		"okx",
		"2026-03-03",
		100.0,
		120.0,
		int64(100),
	); err != nil {
		t.Fatalf("insert risk_account_states failed: %v", err)
	}

	affected, exchanges, err := store.ResetTradeCooldownForTradeDate("2026-03-04")
	if err != nil {
		t.Fatalf("reset trade cooldown failed: %v", err)
	}
	if affected != 0 {
		t.Fatalf("affected=%d, want=0", affected)
	}
	if len(exchanges) != 0 {
		t.Fatalf("exchanges len=%d, want=0, got=%v", len(exchanges), exchanges)
	}

	realized, updated := queryCooldownState(t, store, "okx")
	if realized != 120 {
		t.Fatalf("okx daily_realized_usdt=%.4f, want=120", realized)
	}
	if updated != 100 {
		t.Fatalf("okx updated_at_ms=%d, want=100", updated)
	}
}

func newCooldownTestStore(t *testing.T) *SQLite {
	t.Helper()
	store := NewSQLite(Config{Path: ":memory:"})
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("start sqlite failed: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	})
	if err := store.EnsureSchema(); err != nil {
		t.Fatalf("ensure schema failed: %v", err)
	}
	return store
}

func queryCooldownState(t *testing.T, store *SQLite, exchange string) (realized float64, updated int64) {
	t.Helper()
	if err := store.DB.QueryRow(
		`SELECT daily_realized_usdt, updated_at_ms
		   FROM risk_account_states
		  WHERE exchange = ?;`,
		exchange,
	).Scan(&realized, &updated); err != nil {
		t.Fatalf("query risk_account_states failed: %v", err)
	}
	return realized, updated
}
