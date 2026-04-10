package storage

import (
	"context"
	"math"
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestListRiskHistoryPositionsParsesOpenRowJSONMeta(t *testing.T) {
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

	openMeta := models.StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.5",
		TrendingTimestamp:  1000,
		StrategyTimeframes: []string{"15m", "1h"},
		GroupID:            "turtle|1h|long|1700000000000",
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
	}
	openRowJSON := models.MarshalPositionRowEnvelope(
		map[string]string{"instId": "BTC-USDT-SWAP", "pos": "1"},
		openMeta,
	)
	if _, err := store.DB.Exec(
		`INSERT INTO history_positions (
		     singleton_id, exchange, symbol, inst_id, pos_side, mgn_mode, open_time_ms, close_time_ms, open_row_json,
		     avg_px, close_avg_px, pos, realized_pnl, pnl_ratio, fee, funding_fee,
		     max_floating_profit_amount, max_floating_loss_amount, state, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		int64(42),
		"okx",
		"BTC/USDT",
		"BTC-USDT-SWAP",
		"long",
		"isolated",
		int64(1700000000000),
		int64(1700003600000),
		openRowJSON,
		"100",
		"110",
		"1",
		"10",
		"0.1",
		"-1",
		"0",
		12.34,
		5.67,
		models.PositionStatusClosed,
		int64(1700003600000),
	); err != nil {
		t.Fatalf("insert history row failed: %v", err)
	}

	rows, err := store.ListRiskHistoryPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskHistoryPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected row count: %d", len(rows))
	}
	if rows[0].SingletonID != 42 {
		t.Fatalf("unexpected singleton_id: %d", rows[0].SingletonID)
	}
	if rows[0].StrategyName != "turtle" {
		t.Fatalf("unexpected strategy_name: %s", rows[0].StrategyName)
	}
	if rows[0].StrategyVersion != "v0.0.5" {
		t.Fatalf("unexpected strategy_version: %s", rows[0].StrategyVersion)
	}
	if rows[0].Timeframe != "1h" {
		t.Fatalf("unexpected timeframe: %s", rows[0].Timeframe)
	}
	if rows[0].GroupID != "turtle|1h|long|1700000000000" {
		t.Fatalf("unexpected group_id: %s", rows[0].GroupID)
	}
	if rows[0].MaxFloatingProfitAmount != 12.34 || rows[0].MaxFloatingLossAmount != 5.67 {
		t.Fatalf("unexpected max floating values: profit=%.2f loss=%.2f", rows[0].MaxFloatingProfitAmount, rows[0].MaxFloatingLossAmount)
	}
}

func TestSyncRiskPositionsCreatesHiddenPlaceholderWhenClosedSnapshotMissing(t *testing.T) {
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

	open := models.RiskOpenPosition{
		Exchange:                "okx",
		Symbol:                  "BTC/USDT",
		InstID:                  "BTC-USDT-SWAP",
		Pos:                     "1",
		PosSide:                 "long",
		MgnMode:                 "isolated",
		Margin:                  "100",
		Lever:                   "5",
		AvgPx:                   "100",
		NotionalUSD:             "500",
		MarkPx:                  "102",
		TPTriggerPx:             "120",
		SLTriggerPx:             "95",
		OpenTimeMS:              1700000000000,
		UpdateTimeMS:            1700000005000,
		UpdatedAtMS:             1700000010000,
		MaxFloatingLossAmount:   4.2,
		MaxFloatingProfitAmount: 12.8,
		RowJSON: models.MarshalPositionRowEnvelope(
			map[string]string{"instId": "BTC-USDT-SWAP", "pos": "1"},
			models.StrategyContextMeta{
				StrategyName:    "turtle",
				StrategyVersion: "v0.0.5",
				GroupID:         "turtle|1h|long|1700000000000",
			},
		),
	}
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{open}, nil); err != nil {
		t.Fatalf("seed open position failed: %v", err)
	}
	if err := store.SyncRiskPositions("live", "okx", nil, nil); err != nil {
		t.Fatalf("sync without closed snapshots failed: %v", err)
	}

	var (
		historyCount int
		state        string
		openRowJSON  string
		maxLoss      float64
		maxProfit    float64
	)
	if err := store.DB.QueryRow(
		`SELECT COUNT(*), COALESCE(MIN(state), ''), COALESCE(MIN(open_row_json), ''), COALESCE(MIN(max_floating_loss_amount), 0), COALESCE(MIN(max_floating_profit_amount), 0)
		   FROM history_positions
		  WHERE exchange = 'okx';`,
	).Scan(&historyCount, &state, &openRowJSON, &maxLoss, &maxProfit); err != nil {
		t.Fatalf("count history positions failed: %v", err)
	}
	if historyCount != 1 {
		t.Fatalf("unexpected placeholder history row count: %d", historyCount)
	}
	if state != riskHistoryStateSyncPending {
		t.Fatalf("unexpected placeholder state: %s", state)
	}
	if openRowJSON == "" {
		t.Fatalf("expected placeholder open_row_json to be preserved")
	}
	if math.Abs(maxLoss-4.2) > 1e-9 || math.Abs(maxProfit-12.8) > 1e-9 {
		t.Fatalf("unexpected placeholder floating values: loss=%.8f profit=%.8f", maxLoss, maxProfit)
	}

	var openCount int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM positions WHERE exchange = 'okx';`).Scan(&openCount); err != nil {
		t.Fatalf("count open positions failed: %v", err)
	}
	if openCount != 0 {
		t.Fatalf("unexpected stale open rows: %d", openCount)
	}

	rows, err := store.ListRiskHistoryPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskHistoryPositions failed: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("placeholder rows should be hidden from history api, got: %d", len(rows))
	}
}

func TestSyncRiskPositionsPersistsAndPreservesSingletonID(t *testing.T) {
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

	buildRowJSON := func(singletonID int64, runID string) string {
		return models.MarshalPositionRowEnvelopeWithRuntime(
			map[string]string{"instId": "BTC-USDT-SWAP", "pos": "1"},
			models.StrategyContextMeta{StrategyName: "turtle"},
			models.PositionRuntimeMeta{RunID: runID, SingletonID: singletonID},
		)
	}

	first := models.RiskOpenPosition{
		Exchange:                "okx",
		Symbol:                  "BTC/USDT",
		InstID:                  "BTC-USDT-SWAP",
		Pos:                     "1",
		PosSide:                 "long",
		MgnMode:                 "isolated",
		Margin:                  "100",
		Lever:                   "5",
		AvgPx:                   "100",
		NotionalUSD:             "500",
		MarkPx:                  "102",
		TPTriggerPx:             "120",
		SLTriggerPx:             "95",
		OpenTimeMS:              1700000000000,
		UpdateTimeMS:            1700000005000,
		UpdatedAtMS:             1700000010000,
		MaxFloatingLossAmount:   4.2,
		MaxFloatingProfitAmount: 12.8,
		RowJSON:                 buildRowJSON(7, "run-a"),
	}
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{first}, nil); err != nil {
		t.Fatalf("seed open position failed: %v", err)
	}

	var storedSingletonID int64
	if err := store.DB.QueryRow(
		`SELECT singleton_id FROM positions WHERE exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ?;`,
		"okx", "BTC-USDT-SWAP", "long", "isolated",
	).Scan(&storedSingletonID); err != nil {
		t.Fatalf("query positions singleton_id failed: %v", err)
	}
	if storedSingletonID != 7 {
		t.Fatalf("unexpected stored singleton_id after first sync: %d", storedSingletonID)
	}

	second := first
	second.SingletonID = 9
	second.RowJSON = buildRowJSON(9, "run-b")
	second.MarkPx = "103"
	second.UpdateTimeMS = 1700000008000
	second.UpdatedAtMS = 1700000015000
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{second}, nil); err != nil {
		t.Fatalf("second sync open position failed: %v", err)
	}

	if err := store.DB.QueryRow(
		`SELECT singleton_id FROM positions WHERE exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ?;`,
		"okx", "BTC-USDT-SWAP", "long", "isolated",
	).Scan(&storedSingletonID); err != nil {
		t.Fatalf("query positions singleton_id after second sync failed: %v", err)
	}
	if storedSingletonID != 7 {
		t.Fatalf("expected opening singleton_id preserved as 7, got %d", storedSingletonID)
	}

	rows, err := store.ListRiskOpenPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskOpenPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected open positions count: %d", len(rows))
	}
	if rows[0].SingletonID != 7 {
		t.Fatalf("unexpected open position singleton_id: %d", rows[0].SingletonID)
	}
}

func TestSyncRiskHistoryPositionsMergesAndRemovesPlaceholderRows(t *testing.T) {
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

	openMeta := models.StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.5",
		TrendingTimestamp:  1000,
		StrategyTimeframes: []string{"15m", "1h"},
		GroupID:            "turtle|1h|long|1700000000000",
	}
	openRowJSON := models.MarshalPositionRowEnvelope(
		map[string]string{"instId": "BTC-USDT-SWAP", "pos": "1"},
		openMeta,
	)
	if _, err := store.DB.Exec(
		`INSERT INTO history_positions (
		     exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount, open_row_json,
		     close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee, close_time_ms, state,
		     close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"okx",
		"BTC/USDT",
		"BTC-USDT-SWAP",
		"1",
		"long",
		"isolated",
		"100",
		"5",
		"100",
		"500",
		"102",
		"80",
		"120",
		"95",
		int64(1700000000000),
		int64(1700000060000),
		4.2,
		12.8,
		openRowJSON,
		"102",
		"0",
		"0",
		"0",
		"0",
		int64(1700000090000),
		riskHistoryStateSyncPending,
		"",
		int64(1700000090000),
		int64(1700000090000),
	); err != nil {
		t.Fatalf("insert placeholder history row failed: %v", err)
	}

	closed := models.RiskClosedPosition{
		Exchange:    "okx",
		Symbol:      "BTC/USDT",
		InstID:      "BTC-USDT-SWAP",
		PosSide:     "long",
		MgnMode:     "isolated",
		Lever:       "5",
		OpenAvgPx:   "100",
		CloseAvgPx:  "108.5",
		RealizedPnl: "8.5",
		PnlRatio:    "0.085",
		Fee:         "-0.5",
		FundingFee:  "-0.1",
		OpenTimeMS:  1700000000000,
		CloseTimeMS: 1700000120000,
		State:       models.PositionStatusClosed,
		UpdatedAtMS: 1700000120000,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{closed}); err != nil {
		t.Fatalf("SyncRiskHistoryPositions failed: %v", err)
	}

	var historyCount int
	if err := store.DB.QueryRow(
		`SELECT COUNT(*) FROM history_positions WHERE exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ? AND open_time_ms = ?;`,
		"okx", "BTC-USDT-SWAP", "long", "isolated", int64(1700000000000),
	).Scan(&historyCount); err != nil {
		t.Fatalf("count merged history rows failed: %v", err)
	}
	if historyCount != 1 {
		t.Fatalf("unexpected merged history row count: %d", historyCount)
	}

	rows, err := store.ListRiskHistoryPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskHistoryPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows length: %d", len(rows))
	}
	got := rows[0]
	if math.Abs(got.ProfitAmount-8.5) > 1e-9 {
		t.Fatalf("unexpected profit_amount: %.8f", got.ProfitAmount)
	}
	if math.Abs(got.ProfitRate-0.085) > 1e-9 {
		t.Fatalf("unexpected profit_rate: %.8f", got.ProfitRate)
	}
	if math.Abs(got.MaxFloatingProfitAmount-12.8) > 1e-9 || math.Abs(got.MaxFloatingLossAmount-4.2) > 1e-9 {
		t.Fatalf("unexpected max floating values: profit=%.8f loss=%.8f", got.MaxFloatingProfitAmount, got.MaxFloatingLossAmount)
	}
	if math.Abs(got.MarginAmount-100) > 1e-9 {
		t.Fatalf("unexpected margin amount: %.8f", got.MarginAmount)
	}
	if got.StrategyName != "turtle" || got.StrategyVersion != "v0.0.5" {
		t.Fatalf("unexpected strategy meta: %s/%s", got.StrategyName, got.StrategyVersion)
	}
	if got.GroupID != "turtle|1h|long|1700000000000" {
		t.Fatalf("unexpected group_id: %s", got.GroupID)
	}
}

func TestSyncRiskHistoryPositionsMergesNetSideWithPlaceholderOpenFields(t *testing.T) {
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

	openRowJSON := models.MarshalPositionRowEnvelope(
		map[string]string{"instId": "TRUMP-USDT-SWAP", "pos": "2"},
		models.StrategyContextMeta{
			StrategyName:    "turtle",
			StrategyVersion: "v0.0.5",
			GroupID:         "turtle|30m|short|1772521800000",
		},
	)
	if _, err := store.DB.Exec(
		`INSERT INTO history_positions (
		     exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount, open_row_json,
		     close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee, close_time_ms, state,
		     close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"okx",
		"TRUMP/USDT",
		"TRUMP-USDT-SWAP",
		"2",
		"long",
		"isolated",
		"2.72",
		"3",
		"3.406",
		"9.264",
		"3.42",
		"0",
		"3.8",
		"3.2",
		int64(1772521800000),
		int64(1772521810000),
		0.09,
		0.18,
		openRowJSON,
		"3.437",
		"0",
		"0",
		"0",
		"0",
		int64(1772525700000),
		riskHistoryStateSyncPending,
		"",
		int64(1772525700000),
		int64(1772525700000),
	); err != nil {
		t.Fatalf("insert sync_pending placeholder failed: %v", err)
	}

	closed := models.RiskClosedPosition{
		Exchange:    "okx",
		Symbol:      "TRUMP/USDT",
		InstID:      "TRUMP-USDT-SWAP",
		PosSide:     "net",
		MgnMode:     "isolated",
		Lever:       "3",
		OpenAvgPx:   "3.406",
		CloseAvgPx:  "3.437",
		RealizedPnl: "-0.03",
		PnlRatio:    "-0.037",
		Fee:         "-0.01",
		FundingFee:  "0",
		OpenTimeMS:  1772521800000,
		CloseTimeMS: 1772535310000,
		State:       models.PositionStatusClosed,
		UpdatedAtMS: 1772535310000,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{closed}); err != nil {
		t.Fatalf("SyncRiskHistoryPositions failed: %v", err)
	}

	var pendingCount int
	if err := store.DB.QueryRow(
		`SELECT COUNT(*) FROM history_positions WHERE exchange='okx' AND inst_id='TRUMP-USDT-SWAP' AND state=?;`,
		riskHistoryStateSyncPending,
	).Scan(&pendingCount); err != nil {
		t.Fatalf("count sync_pending rows failed: %v", err)
	}
	if pendingCount != 0 {
		t.Fatalf("sync_pending rows should be removed after merge, got %d", pendingCount)
	}

	rows, err := store.ListRiskHistoryPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskHistoryPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows length: %d", len(rows))
	}
	got := rows[0]
	if math.Abs(got.TakeProfitPrice-3.8) > 1e-9 || math.Abs(got.StopLossPrice-3.2) > 1e-9 {
		t.Fatalf("unexpected TP/SL after net merge: tp=%.8f sl=%.8f", got.TakeProfitPrice, got.StopLossPrice)
	}
	if got.PositionSide != "long" {
		t.Fatalf("expected fallback side long, got %s", got.PositionSide)
	}
	if got.StrategyName != "turtle" || got.StrategyVersion != "v0.0.5" {
		t.Fatalf("unexpected strategy meta after net merge: %s/%s", got.StrategyName, got.StrategyVersion)
	}
	if got.GroupID != "turtle|30m|short|1772521800000" {
		t.Fatalf("unexpected group_id after net merge: %s", got.GroupID)
	}
}

func TestSyncRiskHistoryPositionsPreservesOpeningSingletonID(t *testing.T) {
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

	open := models.RiskOpenPosition{
		Exchange:     "okx",
		Symbol:       "BTC/USDT",
		InstID:       "BTC-USDT-SWAP",
		Pos:          "1",
		PosSide:      "long",
		MgnMode:      "isolated",
		Margin:       "100",
		Lever:        "5",
		AvgPx:        "100",
		NotionalUSD:  "500",
		MarkPx:       "102",
		TPTriggerPx:  "120",
		SLTriggerPx:  "95",
		OpenTimeMS:   1700000000000,
		UpdateTimeMS: 1700000005000,
		UpdatedAtMS:  1700000010000,
		RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
			map[string]string{"instId": "BTC-USDT-SWAP", "pos": "1"},
			models.StrategyContextMeta{StrategyName: "turtle"},
			models.PositionRuntimeMeta{RunID: "run-a", SingletonID: 7},
		),
	}
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{open}, nil); err != nil {
		t.Fatalf("seed open position failed: %v", err)
	}
	if err := store.SyncRiskPositions("live", "okx", nil, nil); err != nil {
		t.Fatalf("sync placeholder history failed: %v", err)
	}

	var placeholderSingletonID int64
	if err := store.DB.QueryRow(
		`SELECT singleton_id FROM history_positions WHERE exchange = ? AND inst_id = ? LIMIT 1;`,
		"okx",
		"BTC-USDT-SWAP",
	).Scan(&placeholderSingletonID); err != nil {
		t.Fatalf("query placeholder singleton_id failed: %v", err)
	}
	if placeholderSingletonID != 7 {
		t.Fatalf("unexpected placeholder singleton_id: %d", placeholderSingletonID)
	}

	closed := models.RiskClosedPosition{
		Exchange:    "okx",
		Symbol:      "BTC/USDT",
		InstID:      "BTC-USDT-SWAP",
		PosSide:     "long",
		MgnMode:     "isolated",
		Lever:       "5",
		OpenAvgPx:   "100",
		CloseAvgPx:  "108",
		RealizedPnl: "8",
		PnlRatio:    "0.08",
		Fee:         "-0.2",
		FundingFee:  "0",
		OpenTimeMS:  1700000000000,
		CloseTimeMS: 1700000120000,
		State:       models.PositionStatusClosed,
		UpdatedAtMS: 1700000120000,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{closed}); err != nil {
		t.Fatalf("SyncRiskHistoryPositions failed: %v", err)
	}

	var finalSingletonID int64
	if err := store.DB.QueryRow(
		`SELECT singleton_id FROM history_positions WHERE exchange = ? AND inst_id = ? AND close_time_ms = ? LIMIT 1;`,
		"okx",
		"BTC-USDT-SWAP",
		int64(1700000120000),
	).Scan(&finalSingletonID); err != nil {
		t.Fatalf("query final singleton_id failed: %v", err)
	}
	if finalSingletonID != 7 {
		t.Fatalf("unexpected final singleton_id: %d", finalSingletonID)
	}

	rows, err := store.ListRiskHistoryPositions("live", "okx")
	if err != nil {
		t.Fatalf("ListRiskHistoryPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows length: %d", len(rows))
	}
	if rows[0].SingletonID != 7 {
		t.Fatalf("unexpected history singleton_id: %d", rows[0].SingletonID)
	}
}

func TestSyncRiskHistoryPositionsSkipsClosedSnapshotWhilePositionStillOpen(t *testing.T) {
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

	open := models.RiskOpenPosition{
		Exchange:                "okx",
		Symbol:                  "CC/USDT",
		InstID:                  "CC-USDT-SWAP",
		Pos:                     "100",
		PosSide:                 "short",
		MgnMode:                 "isolated",
		Margin:                  "50",
		Lever:                   "3",
		AvgPx:                   "0.1",
		NotionalUSD:             "150",
		MarkPx:                  "0.09",
		OpenTimeMS:              1774362181229,
		UpdateTimeMS:            1774389721772,
		MaxFloatingLossAmount:   1.2,
		MaxFloatingProfitAmount: 5.6,
		UpdatedAtMS:             1774389721772,
	}
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{open}, nil); err != nil {
		t.Fatalf("seed open position failed: %v", err)
	}

	closed := models.RiskClosedPosition{
		Exchange:     "okx",
		Symbol:       "CC/USDT",
		InstID:       "CC-USDT-SWAP",
		PosSide:      "short",
		MgnMode:      "isolated",
		Lever:        "3",
		OpenAvgPx:    "0.1",
		CloseAvgPx:   "0.095",
		RealizedPnl:  "2.5",
		PnlRatio:     "0.05",
		Fee:          "-0.1",
		FundingFee:   "0",
		OpenTimeMS:   1774362181229,
		CloseTimeMS:  1774389721772,
		State:        models.PositionStatusClosed,
		CloseRowJSON: `{"close_reason":"signal_partial_close"}`,
		UpdatedAtMS:  1774389721772,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{closed}); err != nil {
		t.Fatalf("SyncRiskHistoryPositions failed: %v", err)
	}

	var historyCount int
	if err := store.DB.QueryRow(
		`SELECT COUNT(*) FROM history_positions WHERE exchange = ? AND inst_id = ? AND open_time_ms = ?;`,
		"okx", "CC-USDT-SWAP", int64(1774362181229),
	).Scan(&historyCount); err != nil {
		t.Fatalf("count history rows failed: %v", err)
	}
	if historyCount != 0 {
		t.Fatalf("expected no history rows while position remains open, got %d", historyCount)
	}
}

func TestSyncRiskPositionsRemovesFinalizedHistoryRowsWhenPositionStillOpen(t *testing.T) {
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

	if _, err := store.DB.Exec(
		`INSERT INTO history_positions (
		     exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount, open_row_json,
		     close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee, close_time_ms, state,
		     close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"okx",
		"CC/USDT",
		"CC-USDT-SWAP",
		"100",
		"net",
		"isolated",
		"50",
		"3",
		"0.1",
		"150",
		"0.09",
		"0",
		"0",
		"0",
		int64(1774362181229),
		int64(1774362181229),
		0.5,
		3.4,
		"",
		"0.095",
		"2.5",
		"0.05",
		"-0.1",
		"0",
		int64(1774389721772),
		models.PositionStatusClosed,
		`{"close_reason":"signal_partial_close"}`,
		int64(1774389721772),
		int64(1774389721772),
	); err != nil {
		t.Fatalf("insert stale finalized history row failed: %v", err)
	}

	open := models.RiskOpenPosition{
		Exchange:     "okx",
		Symbol:       "CC/USDT",
		InstID:       "CC-USDT-SWAP",
		Pos:          "50",
		PosSide:      "short",
		MgnMode:      "isolated",
		Margin:       "25",
		Lever:        "3",
		AvgPx:        "0.1",
		NotionalUSD:  "75",
		MarkPx:       "0.09",
		OpenTimeMS:   1774362181229,
		UpdateTimeMS: 1774390000000,
		UpdatedAtMS:  1774390000000,
	}
	if err := store.SyncRiskPositions("live", "okx", []models.RiskOpenPosition{open}, nil); err != nil {
		t.Fatalf("SyncRiskPositions failed: %v", err)
	}

	var historyCount int
	if err := store.DB.QueryRow(
		`SELECT COUNT(*) FROM history_positions WHERE exchange = ? AND inst_id = ? AND open_time_ms = ?;`,
		"okx", "CC-USDT-SWAP", int64(1774362181229),
	).Scan(&historyCount); err != nil {
		t.Fatalf("count history rows failed: %v", err)
	}
	if historyCount != 0 {
		t.Fatalf("expected stale finalized history rows removed after open sync, got %d", historyCount)
	}
}

func TestSyncRiskHistoryPositionsKeepsOnlyLatestFinalizedRowPerOpenKey(t *testing.T) {
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

	firstClose := models.RiskClosedPosition{
		Exchange:     "okx",
		Symbol:       "CC/USDT",
		InstID:       "CC-USDT-SWAP",
		PosSide:      "short",
		MgnMode:      "isolated",
		Lever:        "3",
		OpenAvgPx:    "0.1",
		CloseAvgPx:   "0.095",
		RealizedPnl:  "2.5",
		PnlRatio:     "0.05",
		Fee:          "-0.1",
		FundingFee:   "0",
		OpenTimeMS:   1774362181229,
		CloseTimeMS:  1774389721772,
		State:        models.PositionStatusClosed,
		CloseRowJSON: `{"close_reason":"signal_partial_close"}`,
		UpdatedAtMS:  1774389721772,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{firstClose}); err != nil {
		t.Fatalf("sync first close failed: %v", err)
	}

	finalClose := models.RiskClosedPosition{
		Exchange:     "okx",
		Symbol:       "CC/USDT",
		InstID:       "CC-USDT-SWAP",
		PosSide:      "short",
		MgnMode:      "isolated",
		Lever:        "3",
		OpenAvgPx:    "0.1",
		CloseAvgPx:   "0.091",
		RealizedPnl:  "4.8",
		PnlRatio:     "0.096",
		Fee:          "-0.2",
		FundingFee:   "0",
		OpenTimeMS:   1774362181229,
		CloseTimeMS:  1774396273415,
		State:        models.PositionStatusClosed,
		CloseRowJSON: `{"close_reason":"signal_full_close"}`,
		UpdatedAtMS:  1774396273415,
	}
	if err := store.SyncRiskHistoryPositions("live", "okx", []models.RiskClosedPosition{finalClose}); err != nil {
		t.Fatalf("sync final close failed: %v", err)
	}

	var (
		historyCount int
		closeTimeMS  int64
		realizedPnl  string
	)
	if err := store.DB.QueryRow(
		`SELECT COUNT(*), MAX(close_time_ms), MAX(realized_pnl)
		   FROM history_positions
		  WHERE exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ? AND open_time_ms = ?;`,
		"okx", "CC-USDT-SWAP", "short", "isolated", int64(1774362181229),
	).Scan(&historyCount, &closeTimeMS, &realizedPnl); err != nil {
		t.Fatalf("query latest history row failed: %v", err)
	}
	if historyCount != 1 {
		t.Fatalf("expected exactly one finalized history row, got %d", historyCount)
	}
	if closeTimeMS != 1774396273415 {
		t.Fatalf("expected latest close_time_ms kept, got %d", closeTimeMS)
	}
	if realizedPnl != "4.8" {
		t.Fatalf("expected latest realized_pnl kept, got %s", realizedPnl)
	}
}

func TestGetSingletonByIDAndUUID(t *testing.T) {
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
	if _, err := store.DB.Exec(
		`INSERT INTO singleton(id, uuid, version, mode, source, status, created, updated, closed, heartbeat, lease_expires, start_time, end_time, runtime)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		int64(9), "run-9", "v1.2.3", "live", "okx", "completed",
		int64(100), int64(200), int64(300), int64(250), int64(260), "2026-03-18 10:00:00", "2026-03-18 11:00:00", "0d1h0m0s",
	); err != nil {
		t.Fatalf("insert singleton failed: %v", err)
	}

	byID, found, err := store.GetSingleton(9, "")
	if err != nil {
		t.Fatalf("GetSingleton by id failed: %v", err)
	}
	if !found || byID.UUID != "run-9" {
		t.Fatalf("unexpected by-id singleton: found=%v uuid=%q", found, byID.UUID)
	}

	byUUID, found, err := store.GetSingleton(0, "run-9")
	if err != nil {
		t.Fatalf("GetSingleton by uuid failed: %v", err)
	}
	if !found || byUUID.ID != 9 {
		t.Fatalf("unexpected by-uuid singleton: found=%v id=%d", found, byUUID.ID)
	}

	if _, found, err := store.GetSingleton(9, "mismatch"); err != nil {
		t.Fatalf("GetSingleton by id+uuid failed: %v", err)
	} else if found {
		t.Fatalf("expected id+uuid AND query to miss")
	}
}
