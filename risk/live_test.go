package risk

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"github.com/misterchenleiya/tradingbot/storage"
)

func TestApplyPositionHistoryOnlyCountsCurrentTradeDateLoss(t *testing.T) {
	now := time.Now()
	loc := now.Location()
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
	currentTradeDate := dayStart.Format("2006-01-02")
	yesterdayCloseMS := dayStart.Add(-12 * time.Hour).UnixMilli()
	todayCloseMS := dayStart.Add(12 * time.Hour).UnixMilli()

	r := newLiveForTest(currentTradeDate)
	r.applyPositionHistory("okx", []iface.PositionHistory{
		{
			InstID:      "ETH-USDT-SWAP",
			CloseTime:   int64ToString(yesterdayCloseMS),
			RealizedPnl: "20",
			Fee:         "-1",
			FundingFee:  "0.5",
		},
		{
			InstID:      "ETH-USDT-SWAP",
			CloseTime:   int64ToString(todayCloseMS),
			RealizedPnl: "-5",
			Fee:         "-0.2",
			FundingFee:  "0.1",
		},
	})

	state := r.currentAccountState("okx")
	if state.DailyRealizedUSDT != 5 {
		t.Fatalf("unexpected daily realized loss: got %.8f want 5", state.DailyRealizedUSDT)
	}
	if math.Abs(state.DailyClosedProfitUSDT-(-5)) > 1e-9 {
		t.Fatalf("unexpected daily closed profit: got %.8f want -5", state.DailyClosedProfitUSDT)
	}
}

func TestAddDailyLossResetOnTradeDateChange(t *testing.T) {
	yesterday := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	r := newLiveForTest(yesterday)
	r.accountStates["okx"] = models.RiskAccountState{
		Exchange:          "okx",
		TradeDate:         yesterday,
		DailyRealizedUSDT: 99,
	}

	r.addDailyLoss("okx", 1.5)

	state := r.currentAccountState("okx")
	if state.TradeDate != time.Now().Format("2006-01-02") {
		t.Fatalf("unexpected trade date: got %s", state.TradeDate)
	}
	if state.DailyRealizedUSDT != 1.5 {
		t.Fatalf("unexpected daily realized loss after reset: got %.8f want 1.5", state.DailyRealizedUSDT)
	}
}

func newLiveForTest(tradeDate string) *Live {
	return &Live{
		cfg: defaultRiskConfig(),
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:           "okx",
				TradeDate:          tradeDate,
				PerTradeUSDT:       100,
				DailyLossLimitUSDT: 50,
			},
		},
		availableUSDT: map[string]float64{},
		positions:     map[string]models.Position{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
	}
}

func int64ToString(value int64) string {
	return strconv.FormatInt(value, 10)
}

func TestResolveHistoryPositionSideFallback(t *testing.T) {
	cases := []struct {
		name string
		item iface.PositionHistory
		want string
	}{
		{
			name: "posSide long",
			item: iface.PositionHistory{PosSide: "long"},
			want: positionSideLong,
		},
		{
			name: "direction sell",
			item: iface.PositionHistory{Direction: "sell"},
			want: positionSideShort,
		},
		{
			name: "position sign positive",
			item: iface.PositionHistory{Pos: "2.5"},
			want: positionSideLong,
		},
		{
			name: "unknown fallback net",
			item: iface.PositionHistory{},
			want: "net",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveHistoryPositionSide(tc.item); got != tc.want {
				t.Fatalf("unexpected side: got %s want %s", got, tc.want)
			}
		})
	}
}

func TestBuildRiskClosedPositionsAllowsMissingPosSide(t *testing.T) {
	rows := []iface.PositionHistory{
		{
			InstID:      "TRUMP-USDT-SWAP",
			CloseTime:   int64ToString(time.Now().UnixMilli()),
			OpenTime:    int64ToString(time.Now().Add(-time.Hour).UnixMilli()),
			PosSide:     "",
			Direction:   "",
			Pos:         "0",
			RealizedPnl: "0.123",
		},
	}
	closed := buildRiskClosedPositions("okx", rows, models.PositionRuntimeMeta{})
	if len(closed) != 1 {
		t.Fatalf("unexpected closed rows: %d", len(closed))
	}
	if closed[0].PosSide != "net" {
		t.Fatalf("unexpected pos side: %s", closed[0].PosSide)
	}
	if closed[0].InstID != "TRUMP-USDT-SWAP" {
		t.Fatalf("unexpected inst id: %s", closed[0].InstID)
	}
}

func TestLiveListSignalsByPairFillsHasPositionWhenOpen(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
		positions: map[string]models.Position{
			"okx|BTC-USDT-SWAP|long|isolated": {
				Exchange:      "okx",
				Symbol:        "BTC/USDT",
				Status:        models.PositionStatusOpen,
				EntryQuantity: 1,
			},
		},
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    0,
		HighSide:  1,
	})

	signals, err := r.ListSignalsByPair("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("ListSignalsByPair failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("unexpected signal count: got %d want 1", len(signals))
	}
	if signals[0].HasPosition != models.SignalHasOpenPosition {
		t.Fatalf("expected has_position=%d, got %d", models.SignalHasOpenPosition, signals[0].HasPosition)
	}
}

func TestLiveListSignalsByPairFiltersInactiveStrategies(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
		activeSet:   buildActiveStrategySet([]string{"turtle"}),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
	})
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
	})

	signals, err := r.ListSignalsByPair("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("ListSignalsByPair failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("unexpected signal count: got %d want 1", len(signals))
	}
	if signals[0].Strategy != "turtle" {
		t.Fatalf("unexpected strategy: got %s want turtle", signals[0].Strategy)
	}
}

func TestLiveEvaluateClosePartialUsesEightyPercentSize(t *testing.T) {
	r := &Live{}
	signal := models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             32,
		HighSide:           1,
	}
	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		PositionSide:       "long",
		EntryQuantity:      1,
		EntryPrice:         100,
		CurrentPrice:       101,
		StrategyName:       "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
	}
	data := testMarketDataTimeframe("3m", time.Now().UnixMilli(), 101, 99, 101)

	decision, err := r.evaluateClose("okx", "BTC/USDT", "1h", signal, position, true, data)
	if err != nil {
		t.Fatalf("evaluateClose failed: %v", err)
	}
	if decision.Size != 0.8 {
		t.Fatalf("expected partial close size 0.8, got %.8f", decision.Size)
	}
	if decision.CloseReason != "signal_partial_close" {
		t.Fatalf("expected close reason signal_partial_close, got %s", decision.CloseReason)
	}
}

func TestNormalizeRiskConfigDefaults(t *testing.T) {
	cfg := RiskConfig{
		TrendGuard: RiskTrendGuardConfig{
			Enabled: true,
		},
		TP: RiskTPConfig{
			Mode:       tpModeFixed,
			DefaultPct: 0,
		},
	}
	normalizeRiskConfig(&cfg)
	if cfg.MaxOpenPositions != 3 {
		t.Fatalf("unexpected max_open_positions default: got %d want 3", cfg.MaxOpenPositions)
	}
	if !cfg.TrendGuard.Enabled {
		t.Fatalf("unexpected trend_guard.enabled default: got %v want true", cfg.TrendGuard.Enabled)
	}
	if cfg.TrendGuard.MaxStartLagBars != 12 {
		t.Fatalf("unexpected trend_guard.max_start_lag_bars default: got %d want 12", cfg.TrendGuard.MaxStartLagBars)
	}
	if cfg.TrendGuard.Mode != trendGuardModeLegacy {
		t.Fatalf("unexpected trend_guard.mode default: got %s want %s", cfg.TrendGuard.Mode, trendGuardModeLegacy)
	}
	if math.Abs(cfg.TrendGuard.LeaderMinPriorityScore-50) > 1e-12 {
		t.Fatalf("unexpected trend_guard.leader_min_priority_score default: got %.8f want 50", cfg.TrendGuard.LeaderMinPriorityScore)
	}
	if math.Abs(cfg.TP.DefaultPct-1.0) > 1e-12 {
		t.Fatalf("unexpected tp.default_pct default: got %.8f want 1", cfg.TP.DefaultPct)
	}
}

func TestNormalizeRiskConfigKeepsTrendGuardExplicitlyDisabled(t *testing.T) {
	cfg := RiskConfig{
		TrendGuard: RiskTrendGuardConfig{
			Enabled: false,
		},
	}

	normalizeRiskConfig(&cfg)

	if cfg.TrendGuard.Enabled {
		t.Fatalf("expected trend_guard.enabled to remain false")
	}
	if cfg.TrendGuard.MaxStartLagBars != 12 {
		t.Fatalf("unexpected trend_guard.max_start_lag_bars default: got %d want 12", cfg.TrendGuard.MaxStartLagBars)
	}
	if cfg.TrendGuard.Mode != trendGuardModeLegacy {
		t.Fatalf("unexpected trend_guard.mode default: got %s want %s", cfg.TrendGuard.Mode, trendGuardModeLegacy)
	}
	if math.Abs(cfg.TrendGuard.LeaderMinPriorityScore-50) > 1e-12 {
		t.Fatalf("unexpected trend_guard.leader_min_priority_score default: got %.8f want 50", cfg.TrendGuard.LeaderMinPriorityScore)
	}
}

func TestEvaluateOpenRejectsWhenMaxOpenPositionsReached(t *testing.T) {
	r := &Live{
		cfg: defaultRiskConfig(),
		positions: map[string]models.Position{
			"okx|BTC-USDT-SWAP|long|isolated": {
				Exchange:      "okx",
				Symbol:        "BTC/USDT",
				EntryQuantity: 1,
				Status:        models.PositionStatusOpen,
			},
		},
	}
	r.cfg.MaxOpenPositions = 1
	decision, err := r.evaluateOpen(
		"okx",
		"ETH/USDT",
		"15m",
		models.Signal{HighSide: 1, Strategy: "turtle"},
		models.Position{},
		false,
		models.MarketData{},
	)
	if err == nil || !strings.Contains(err.Error(), "max open positions reached") {
		t.Fatalf("expected max open positions error, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: got %s want %s", decision.Action, models.DecisionActionIgnore)
	}
}

func TestEvaluateOpenBatchPersistsRejectedRiskDecision(t *testing.T) {
	store := storage.NewSQLite(storage.Config{Path: ":memory:"})
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
		`INSERT INTO exchanges (id, name, created_at, updated_at) VALUES (1, 'okx', ?, ?);`,
		time.Now().UnixMilli(),
		time.Now().UnixMilli(),
	); err != nil {
		t.Fatalf("insert exchange failed: %v", err)
	}
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	r := NewLive(LiveConfig{
		Store:            store,
		DefaultExchange:  "okx",
		DefaultTimeframe: "30m",
		Logger:           glog.Nop(),
	})
	r.cfg = defaultRiskConfig()
	r.cfg.MaxOpenPositions = 1
	r.positions["okx|BTC-USDT-SWAP|short|isolated"] = models.Position{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		EntryQuantity: 1,
		Status:        models.PositionStatusOpen,
	}

	ts := time.Date(2026, 3, 26, 13, 43, 0, 0, time.Local).UnixMilli()
	decision, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:  "okx",
		Symbol:    "ETH/USDT",
		Timeframe: "30m",
		Strategy:  "turtle",
		ComboKey:  "1m/5m/30m",
		Action:    8,
		HighSide:  -1,
		SL:        2110,
	}}, testMarketDataTimeframe("30m", ts, 2100, 2100, 2100))
	if err == nil || !strings.Contains(err.Error(), "max open positions reached") {
		t.Fatalf("expected max open positions error, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: got %s want %s", decision.Action, models.DecisionActionIgnore)
	}

	var (
		count          int
		resultStatus   string
		decisionAction string
		rejectReason   string
	)
	if scanErr := store.DB.QueryRow(
		`SELECT COUNT(*), COALESCE(MAX(result_status), ''), COALESCE(MAX(decision_action), ''), COALESCE(MAX(reject_reason), '')
		   FROM risk_decisions WHERE exchange = ? AND symbol = ?;`,
		"okx",
		"ETH/USDT",
	).Scan(&count, &resultStatus, &decisionAction, &rejectReason); scanErr != nil {
		t.Fatalf("query risk decisions failed: %v", scanErr)
	}
	if count != 1 {
		t.Fatalf("unexpected risk decision count: got %d want 1", count)
	}
	if resultStatus != "rejected" {
		t.Fatalf("unexpected result status: got %s want rejected", resultStatus)
	}
	if decisionAction != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action persisted: got %s want %s", decisionAction, models.DecisionActionIgnore)
	}
	if !strings.Contains(rejectReason, "max open positions reached") {
		t.Fatalf("unexpected reject reason: %s", rejectReason)
	}

	cached, ok := r.signalCache.Find("okx", "ETH/USDT", "turtle", "1m/5m/30m")
	if !ok {
		t.Fatalf("expected rejected open signal cached")
	}
	if cached.Action != models.SignalActionOpenRiskRejected {
		t.Fatalf("expected cached action %d, got %d", models.SignalActionOpenRiskRejected, cached.Action)
	}
	if cached.TriggerTimestamp != int(ts) {
		t.Fatalf("expected cached trigger timestamp %d, got %d", ts, cached.TriggerTimestamp)
	}
}

func TestRecoverPairSignalsSkipsRemovedConfiguredCombo(t *testing.T) {
	store := storage.NewSQLite(storage.Config{Path: ":memory:"})
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
	nowMS := time.Now().UnixMilli()
	if _, err := store.DB.Exec(
		`INSERT INTO exchanges (id, name, created_at, updated_at) VALUES (1, 'okx', ?, ?);`,
		nowMS,
		nowMS,
	); err != nil {
		t.Fatalf("insert exchange failed: %v", err)
	}
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}
	signal := models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "30m",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.6d",
		StrategyTimeframes: []string{"1m", "5m", "30m"},
		ComboKey:           "1m/5m/30m",
		Action:             4,
		HighSide:           -1,
	}
	signalJSON, err := json.Marshal(signal)
	if err != nil {
		t.Fatalf("marshal signal failed: %v", err)
	}
	if err := store.AppendSignalChange(models.SignalChangeRecord{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		Mode:            "live",
		Timeframe:       "30m",
		Strategy:        "turtle",
		StrategyVersion: "v0.0.6d",
		ChangeStatus:    models.SignalChangeStatusNew,
		ChangedFields:   "new_signal",
		SignalJSON:      string(signalJSON),
		EventAtMS:       nowMS,
		CreatedAtMS:     nowMS,
	}); err != nil {
		t.Fatalf("append signal change failed: %v", err)
	}

	r := &Live{
		logger: glog.Nop(),
		store:  store,
		activeSet: map[string]struct{}{
			"turtle": {},
		},
		comboTradeEnabled: map[string]bool{
			"3m/15m/1h": true,
			"1h/4h/1d":  false,
		},
		signalCache: NewSignalCache(),
	}

	restored := r.recoverPairSignals("okx", "BTC/USDT")
	if restored != 0 {
		t.Fatalf("expected removed combo signal not restored, got %d", restored)
	}
	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "turtle", "1m/5m/30m"); ok {
		t.Fatalf("expected removed combo signal absent from cache")
	}
}

func TestEvaluateOpenBatchMarksTrendGuardRejectedSignal(t *testing.T) {
	ts := time.Date(2026, 3, 29, 0, 24, 0, 0, time.Local).UnixMilli()
	r := &Live{
		cfg:           defaultRiskConfig(),
		logger:        glog.Nop(),
		signalCache:   NewSignalCache(),
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
	}
	r.cfg.TrendGuard.Enabled = true
	r.cfg.TrendGuard.Mode = trendGuardModeLegacy
	r.positions["okx|BTC-USDT-SWAP|long|isolated"] = models.Position{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		Timeframe:     "30m",
		PositionSide:  positionSideLong,
		EntryQuantity: 1,
		Status:        models.PositionStatusOpen,
		StrategyName:  "turtle",
	}
	r.openPositions["okx|BTC-USDT-SWAP|long|isolated"] = models.RiskOpenPosition{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		InstID:   "BTC-USDT-SWAP",
		PosSide:  positionSideLong,
		MgnMode:  models.MarginModeIsolated,
		RowJSON: models.MarshalPositionRowEnvelope(
			map[string]string{"instId": "BTC-USDT-SWAP"},
			models.StrategyContextMeta{
				StrategyName:      "turtle",
				TrendingTimestamp: int(ts),
			},
		),
	}

	decision, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:          "okx",
		Symbol:            "ETH/USDT",
		Timeframe:         "30m",
		Strategy:          "turtle",
		ComboKey:          "1m/5m/30m",
		Action:            8,
		HighSide:          1,
		SL:                99,
		TriggerTimestamp:  int(ts),
		TrendingTimestamp: int(ts),
	}}, testMarketDataTimeframe("30m", ts, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected trend guard rejection, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: got %s want %s", decision.Action, models.DecisionActionIgnore)
	}

	cached, ok := r.signalCache.Find("okx", "ETH/USDT", "turtle", "1m/5m/30m")
	if !ok {
		t.Fatalf("expected trend-guard rejected open signal cached")
	}
	if cached.Action != models.SignalActionOpenTrendGuardRejected {
		t.Fatalf("expected cached action %d, got %d", models.SignalActionOpenTrendGuardRejected, cached.Action)
	}
	if cached.TriggerTimestamp != int(ts) {
		t.Fatalf("expected cached trigger timestamp %d, got %d", ts, cached.TriggerTimestamp)
	}
}

func TestEvaluateOpenBatchMarksComboTradeDisabledAsRiskRejected(t *testing.T) {
	ts := time.Date(2026, 3, 29, 0, 36, 0, 0, time.Local).UnixMilli()
	r := &Live{
		cfg:               defaultRiskConfig(),
		logger:            glog.Nop(),
		signalCache:       NewSignalCache(),
		comboTradeEnabled: map[string]bool{"3m/15m/1h": false},
		positions:         map[string]models.Position{},
		openPositions:     map[string]models.RiskOpenPosition{},
		cooldowns:         map[string]models.RiskSymbolCooldownState{},
	}

	decision, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		ComboKey:           "3m/15m/1h",
		Action:             8,
		HighSide:           1,
		SL:                 95,
		TriggerTimestamp:   int(ts),
		TrendingTimestamp:  int(ts),
	}}, testMarketDataTimeframe("3m", ts, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "trade disabled for combo") {
		t.Fatalf("expected combo trade disabled error, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: got %s want %s", decision.Action, models.DecisionActionIgnore)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "turtle", "3m/15m/1h")
	if !ok {
		t.Fatalf("expected combo-rejected open signal cached")
	}
	if cached.Action != models.SignalActionOpenRiskRejected {
		t.Fatalf("expected cached action %d, got %d", models.SignalActionOpenRiskRejected, cached.Action)
	}
}

func TestNewRiskClientOrderIDFormat(t *testing.T) {
	id := newRiskClientOrderID("okx", "BTC/USDT")
	if !isValidClientOrderIDForTest(id) {
		t.Fatalf("invalid client order id: %s", id)
	}
}

func TestCalculateRiskOpenSizeUsesContractValue(t *testing.T) {
	size, err := calculateRiskOpenSize(10, 10, 67000, iface.Instrument{CtVal: 0.01, LotSz: 0.01, MinSz: 0.01})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size <= 0 {
		t.Fatalf("unexpected size: %.8f", size)
	}
	if size < 0.14 || size > 0.15 {
		t.Fatalf("unexpected contract size: got %.8f, want around 0.14", size)
	}
}

func TestValidateTPSLAgainstPriceRejectsInvalidLongSL(t *testing.T) {
	err := validateTPSLAgainstPrice(positionSideLong, 70000, 81000, 686490)
	if err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestResolveOpenTakeProfitDisabledMode(t *testing.T) {
	tp := resolveOpenTakeProfit(
		RiskTPConfig{Mode: tpModeDisabled, DefaultPct: 0.10},
		positionSideLong,
		70000,
		0,
		3,
	)
	if tp != 0 {
		t.Fatalf("expected disabled mode open TP=0, got %.8f", tp)
	}
}

func TestResolveOpenTakeProfitFixedMode(t *testing.T) {
	tp := resolveOpenTakeProfit(
		RiskTPConfig{Mode: tpModeFixed, DefaultPct: 0.10},
		positionSideLong,
		70000,
		0,
		1,
	)
	if tp != 77000 {
		t.Fatalf("expected fixed mode open TP=77000, got %.8f", tp)
	}
}

func TestResolveOpenTakeProfitFixedModeUsesProfitRateByLeverage(t *testing.T) {
	tp := resolveOpenTakeProfit(
		RiskTPConfig{Mode: tpModeFixed, DefaultPct: 0.10},
		positionSideLong,
		70000,
		0,
		3,
	)
	want := 70000 * (1 + 0.10/3)
	if math.Abs(tp-want) > 1e-6 {
		t.Fatalf("expected TP %.8f, got %.8f", want, tp)
	}
}

func TestResolveUpdateTakeProfitDisabledMode(t *testing.T) {
	tp := resolveUpdateTakeProfit(
		RiskTPConfig{Mode: tpModeDisabled, DefaultPct: 0.10},
		0,
		81000,
	)
	if tp != 0 {
		t.Fatalf("expected disabled mode update TP=0, got %.8f", tp)
	}
}

func TestEvaluateMoveAllowsMissingTP(t *testing.T) {
	r := newLiveForTest(time.Now().Format("2006-01-02"))
	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "15m",
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		TakeProfitPrice:    81000,
		StopLossPrice:      69000,
		LeverageMultiplier: 5,
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Action:    16,
		HighSide:  1,
		TP:        0,
		SL:        69100,
	}
	decision, err := r.EvaluateUpdate(signal, position, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		OHLCV: models.OHLCV{
			Close: 70000,
		},
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if decision.Action != models.DecisionActionUpdate {
		t.Fatalf("unexpected decision action: %s", decision.Action)
	}
	if decision.TakeProfitPrice != 81000 {
		t.Fatalf("unexpected tp: got %.8f want 81000", decision.TakeProfitPrice)
	}
}

func TestEvaluateUpdateWritesTPBackToSignalCache(t *testing.T) {
	r := &Live{
		cfg:              defaultRiskConfig(),
		logger:           glog.Nop(),
		signalCache:      NewSignalCache(),
		defaultExchange:  "okx",
		defaultTimeframe: "15m",
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
		TP:        0,
		SL:        69000,
	})

	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "15m",
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		TakeProfitPrice:    81000,
		StopLossPrice:      69000,
		LeverageMultiplier: 5,
		StrategyName:       "turtle",
		Status:             models.PositionStatusOpen,
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Strategy:  "turtle",
		Action:    16,
		HighSide:  1,
		TP:        0,
		SL:        69100,
	}

	decision, err := r.EvaluateUpdate(signal, position, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		OHLCV: models.OHLCV{
			TS:    1700000000000,
			Close: 70000,
		},
	})
	if err != nil {
		t.Fatalf("EvaluateUpdate failed: %v", err)
	}
	if decision.Action != models.DecisionActionUpdate {
		t.Fatalf("unexpected decision action: got %s want %s", decision.Action, models.DecisionActionUpdate)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "15m", "turtle")
	if !ok {
		t.Fatalf("expected signal in cache")
	}
	if !floatAlmostEqual(cached.TP, decision.TakeProfitPrice) {
		t.Fatalf("expected cached TP %.8f, got %.8f", decision.TakeProfitPrice, cached.TP)
	}
}

func TestSyncSignalCacheFromPositionRefreshUpdatesTP(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Strategy:  "turtle",
		HighSide:  1,
		TP:        81000,
		SL:        69000,
	})

	err := r.syncSignalCacheFromPosition(models.Position{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		Timeframe:       "15m",
		PositionSide:    "long",
		StrategyName:    "turtle",
		EntryPrice:      70000,
		TakeProfitPrice: 82000,
		StopLossPrice:   69100,
		Status:          models.PositionStatusOpen,
	}, 1700000001000)
	if err != nil {
		t.Fatalf("syncSignalCacheFromPosition failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "15m", "turtle")
	if !ok {
		t.Fatalf("expected signal in cache")
	}
	if !floatAlmostEqual(cached.TP, 82000) {
		t.Fatalf("expected refreshed cached TP 82000, got %.8f", cached.TP)
	}
	if !floatAlmostEqual(cached.SL, 69100) {
		t.Fatalf("expected refreshed cached SL 69100, got %.8f", cached.SL)
	}
	if !floatAlmostEqual(cached.Entry, 70000) {
		t.Fatalf("expected refreshed cached Entry 70000, got %.8f", cached.Entry)
	}
}

func TestSyncSignalCacheFromPosition_LongSLDoesNotRollback(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Strategy:  "turtle",
		HighSide:  1,
		Entry:     70000,
		SL:        69100,
	})

	err := r.syncSignalCacheFromPosition(models.Position{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		Timeframe:     "15m",
		PositionSide:  "long",
		StrategyName:  "turtle",
		EntryPrice:    70000,
		StopLossPrice: 69000,
		Status:        models.PositionStatusOpen,
	}, 1700000001000)
	if err != nil {
		t.Fatalf("syncSignalCacheFromPosition failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "15m", "turtle")
	if !ok {
		t.Fatalf("expected signal in cache")
	}
	if !floatAlmostEqual(cached.SL, 69100) {
		t.Fatalf("expected long SL rollback ignored, got %.8f", cached.SL)
	}
}

func TestSyncSignalCacheFromPosition_ShortSLDoesNotRollback(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Strategy:  "turtle",
		HighSide:  -1,
		Entry:     70000,
		SL:        70900,
	})

	err := r.syncSignalCacheFromPosition(models.Position{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		Timeframe:     "15m",
		PositionSide:  "short",
		StrategyName:  "turtle",
		EntryPrice:    70000,
		StopLossPrice: 71000,
		Status:        models.PositionStatusOpen,
	}, 1700000001000)
	if err != nil {
		t.Fatalf("syncSignalCacheFromPosition failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "15m", "turtle")
	if !ok {
		t.Fatalf("expected signal in cache")
	}
	if !floatAlmostEqual(cached.SL, 70900) {
		t.Fatalf("expected short SL rollback ignored, got %.8f", cached.SL)
	}
}

func TestEvaluateMoveRejectsBackwardTP(t *testing.T) {
	r := newLiveForTest(time.Now().Format("2006-01-02"))
	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "15m",
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		TakeProfitPrice:    81000,
		StopLossPrice:      69000,
		LeverageMultiplier: 5,
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Action:    16,
		HighSide:  1,
		TP:        80000,
		SL:        68900,
	}
	_, err := r.EvaluateUpdate(signal, position, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		OHLCV: models.OHLCV{
			Close: 70000,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "TP can only move with trend") {
		t.Fatalf("expected TP trend error, got %v", err)
	}
}

func TestEvaluateMoveRejectsOwnershipStrategyMismatch(t *testing.T) {
	r := newLiveForTest(time.Now().Format("2006-01-02"))
	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "15m",
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		TakeProfitPrice:    81000,
		StopLossPrice:      69000,
		LeverageMultiplier: 5,
		StrategyName:       "manual",
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Action:    16,
		HighSide:  1,
		SL:        69100,
		Strategy:  "turtle",
	}
	_, err := r.EvaluateUpdate(signal, position, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		OHLCV: models.OHLCV{
			Close: 70000,
		},
	})
	if err != nil {
		t.Fatalf("expected non-owner update to pass, got %v", err)
	}
}

func TestEvaluateCloseRejectsOwnershipTimeframeMismatch(t *testing.T) {
	r := newLiveForTest(time.Now().Format("2006-01-02"))
	position := models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		TakeProfitPrice:    81000,
		StopLossPrice:      69000,
		LeverageMultiplier: 5,
		StrategyName:       "turtle",
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Action:    64,
		HighSide:  1,
		Strategy:  "turtle",
	}
	_, err := r.EvaluateUpdate(signal, position, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		OHLCV: models.OHLCV{
			Close: 70000,
		},
	})
	if err != nil {
		t.Fatalf("expected non-owner close to pass, got %v", err)
	}
}

func TestRefreshAccountStatesForceRecalculateOnStart(t *testing.T) {
	today := time.Now().Format("2006-01-02")
	r := &Live{
		logger: glog.Nop(),
		cfg:    defaultRiskConfig(),
		exchanges: map[string]iface.Exchange{
			"okx": stubRiskExchange{
				balance: iface.BalanceSnapshot{
					Trading: []iface.Balance{
						{Ccy: "USDT", AvailBal: "4000", Eq: "4000"},
					},
				},
				positions: []iface.Position{
					{Margin: "200"},
				},
			},
		},
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:           "okx",
				TradeDate:          today,
				PerTradeUSDT:       102.5,
				DailyLossLimitUSDT: 51.25,
			},
		},
		availableUSDT: map[string]float64{},
	}

	r.refreshAccountStates(true)
	state := r.currentAccountState("okx")
	if math.Abs(state.PerTradeUSDT-420) > 1e-9 {
		t.Fatalf("expected forced startup per_trade_usdt=420, got %.8f", state.PerTradeUSDT)
	}
	if math.Abs(state.DailyLossLimitUSDT-210) > 1e-9 {
		t.Fatalf("expected forced startup daily_loss_limit_usdt=210, got %.8f", state.DailyLossLimitUSDT)
	}
}

func TestApplySignalLifecycle_ActionZeroButNonEmptyDoesNotClear(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
	}
	previous := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	r.signalCache.Upsert(previous)

	next := previous
	next.Action = 0
	next.Entry = 0
	next.SL = 0
	data := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		OHLCV:     models.OHLCV{TS: 1700000000000},
	}
	if _, changed, err := r.applySignalLifecycle(next, data, models.Position{}, false); err != nil {
		t.Fatalf("applySignalLifecycle returned error: %v", err)
	} else if !changed {
		t.Fatalf("expected non-empty action=0 signal to be treated as update")
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle")
	if !ok {
		t.Fatalf("expected signal to stay in cache")
	}
	if cached.Action != 0 || cached.HighSide != 1 {
		t.Fatalf("unexpected cached signal: %+v", cached)
	}
}

func TestApplySignalLifecycle_RoutingOnlySignalClearsCache(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
	}
	previous := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	r.signalCache.Upsert(previous)

	cleared := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
	}
	data := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		OHLCV:     models.OHLCV{TS: 1700000000001},
	}
	if _, changed, err := r.applySignalLifecycle(cleared, data, models.Position{}, false); err != nil {
		t.Fatalf("applySignalLifecycle returned error: %v", err)
	} else if !changed {
		t.Fatalf("expected routing-only signal to clear cached signal")
	}

	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected signal removed from cache")
	}
}

func TestApplySignalLifecycle_IgnoresInactiveStrategyNonEmptySignal(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
		activeSet:   buildActiveStrategySet([]string{"turtle"}),
	}
	signal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	data := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		OHLCV:     models.OHLCV{TS: 1700000000002},
	}
	if _, changed, err := r.applySignalLifecycle(signal, data, models.Position{}, false); err != nil {
		t.Fatalf("applySignalLifecycle returned error: %v", err)
	} else if changed {
		t.Fatalf("expected inactive non-empty signal to be ignored")
	}
	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected inactive signal not cached")
	}
}

func TestApplySignalLifecycle_AllowsInactiveStrategyClearSignal(t *testing.T) {
	r := &Live{
		signalCache: NewSignalCache(),
		activeSet:   buildActiveStrategySet([]string{"turtle"}),
	}
	previous := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	r.signalCache.Upsert(previous)
	cleared := models.ClearSignalForRemoval(previous)
	data := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		OHLCV:     models.OHLCV{TS: 1700000000003},
	}
	if _, changed, err := r.applySignalLifecycle(cleared, data, models.Position{}, false); err != nil {
		t.Fatalf("applySignalLifecycle returned error: %v", err)
	} else if !changed {
		t.Fatalf("expected cleared signal to remove stale cache")
	}
	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected cleared stale signal removed from cache")
	}
}

func TestSignalCacheAllowsDistinctComboKeysForSamePair(t *testing.T) {
	cache := NewSignalCache()
	cache.Upsert(models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "30m",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"1m", "5m", "30m"},
	})
	cache.Upsert(models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
	})

	signals := cache.ListByPair("okx", "BTC/USDT")
	if len(signals) != 2 {
		t.Fatalf("expected 2 signals, got %d", len(signals))
	}
}

func TestEvaluateOpenRejectsExposureAcrossStableQuoteSymbols(t *testing.T) {
	r := newLiveForTest(time.Now().Format("2006-01-02"))
	r.positions["existing"] = models.Position{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		ComboKey:           "3m/15m/1h",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		PositionSide:       "long",
		EntryPrice:         70000,
		EntryQuantity:      0.1,
		LeverageMultiplier: 5,
		StrategyName:       "turtle",
		Status:             models.PositionStatusOpen,
	}

	_, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:           "okx",
		Symbol:             "BTC/USDC",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             8,
		HighSide:           1,
		SL:                 69000,
	}}, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDC",
		Timeframe: "1h",
		OHLCV:     models.OHLCV{TS: 1700000000000, Close: 70000},
	})
	if err == nil || !strings.Contains(err.Error(), "already occupied") {
		t.Fatalf("expected exposure conflict error, got %v", err)
	}
}

func TestRefreshAccountStatesSameDayKeepsPerTradeWithoutForce(t *testing.T) {
	today := time.Now().Format("2006-01-02")
	r := &Live{
		logger: glog.Nop(),
		cfg:    defaultRiskConfig(),
		exchanges: map[string]iface.Exchange{
			"okx": stubRiskExchange{
				balance: iface.BalanceSnapshot{
					Trading: []iface.Balance{
						{Ccy: "USDT", AvailBal: "4000", Eq: "4000"},
					},
				},
				positions: []iface.Position{
					{Margin: "200"},
				},
			},
		},
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:           "okx",
				TradeDate:          today,
				PerTradeUSDT:       102.5,
				DailyLossLimitUSDT: 51.25,
			},
		},
		availableUSDT: map[string]float64{},
	}

	r.refreshAccountStates(false)
	state := r.currentAccountState("okx")
	if math.Abs(state.TradingUSDT-4200) > 1e-9 {
		t.Fatalf("expected trading_usdt=4200, got %.8f", state.TradingUSDT)
	}
	if math.Abs(state.PerTradeUSDT-102.5) > 1e-9 {
		t.Fatalf("expected per_trade_usdt keep 102.5 without force, got %.8f", state.PerTradeUSDT)
	}
	if math.Abs(state.DailyLossLimitUSDT-51.25) > 1e-9 {
		t.Fatalf("expected daily_loss_limit_usdt keep 51.25 without force, got %.8f", state.DailyLossLimitUSDT)
	}
}

func TestGetAccountFundsUsesActiveExchangeState(t *testing.T) {
	r := &Live{
		defaultExchange: "okx",
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:    "okx",
				UpdatedAtMS: time.Now().UnixMilli(),
			},
		},
	}

	funds, err := r.GetAccountFunds("okx")
	if err != nil {
		t.Fatalf("GetAccountFunds failed: %v", err)
	}
	if funds.Exchange != "okx" {
		t.Fatalf("unexpected exchange: got %s want okx", funds.Exchange)
	}
}

func TestGetAccountFundsIncludesDailyClosedProfitAndUnrealized(t *testing.T) {
	now := time.Now().UnixMilli()
	r := &Live{
		defaultExchange: "okx",
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:              "okx",
				TradeDate:             time.Now().Format("2006-01-02"),
				DailyClosedProfitUSDT: 12.5,
				FundingUSDT:           100,
				TradingUSDT:           900,
				TotalUSDT:             1000,
				PerTradeUSDT:          90,
				UpdatedAtMS:           now,
			},
		},
		positions: map[string]models.Position{
			"okx|BTC-USDT-SWAP|long|isolated": {
				Exchange:               "okx",
				Symbol:                 "BTC/USDT",
				UnrealizedProfitAmount: 3.3,
			},
			"okx|ETH-USDT-SWAP|short|isolated": {
				Exchange:               "okx",
				Symbol:                 "ETH/USDT",
				UnrealizedProfitAmount: -1.2,
			},
			"binance|BTC-USDT|long|isolated": {
				Exchange:               "binance",
				Symbol:                 "BTC/USDT",
				UnrealizedProfitAmount: 99,
			},
		},
	}

	funds, err := r.GetAccountFunds("okx")
	if err != nil {
		t.Fatalf("GetAccountFunds failed: %v", err)
	}
	if math.Abs(funds.DailyProfitUSDT-14.6) > 1e-9 {
		t.Fatalf("unexpected daily_profit_usdt: got %.8f want 14.6", funds.DailyProfitUSDT)
	}
	if math.Abs(funds.PerTradeUSDT-90) > 1e-9 {
		t.Fatalf("unexpected per_trade_usdt: got %.8f want 90", funds.PerTradeUSDT)
	}
}

func TestRefreshAccountStatesRefreshesActiveExchange(t *testing.T) {
	r := &Live{
		logger: glog.Nop(),
		cfg:    defaultRiskConfig(),
		exchanges: map[string]iface.Exchange{
			"okx": stubRiskExchange{
				balance: iface.BalanceSnapshot{
					Trading: []iface.Balance{
						{Ccy: "USDT", AvailBal: "4000", Eq: "4000"},
					},
				},
				positions: []iface.Position{
					{Margin: "200"},
				},
			},
		},
		accountStates: map[string]models.RiskAccountState{},
		availableUSDT: map[string]float64{},
	}

	r.refreshAccountStates(false)
	state := r.currentAccountState("okx")
	if state.UpdatedAtMS == 0 {
		t.Fatalf("expected active exchange account state to refresh")
	}
	if got := r.currentAvailableUSDT("okx"); got <= 0 {
		t.Fatalf("expected active exchange available usdt updated, got %.8f", got)
	}
}

func TestSyncHistoryRuntimeAppliesSymbolCooldown(t *testing.T) {
	now := time.Now().UnixMilli()
	history := []iface.PositionHistory{
		{
			InstID:      "BTC-USDT-SWAP",
			CloseTime:   int64ToString(now - 2*60*1000),
			RealizedPnl: "-1.2",
		},
		{
			InstID:      "BTC-USDT-SWAP",
			CloseTime:   int64ToString(now - 60*1000),
			RealizedPnl: "-0.8",
		},
	}
	r := &Live{
		logger: glog.Nop(),
		cfg:    defaultRiskConfig(),
		exchanges: map[string]iface.Exchange{
			"okx": stubRiskExchange{
				history: history,
			},
		},
		accountStates: map[string]models.RiskAccountState{
			"okx": {
				Exchange:           "okx",
				TradeDate:          time.Now().Format("2006-01-02"),
				PerTradeUSDT:       100,
				DailyLossLimitUSDT: 50,
			},
		},
		availableUSDT: map[string]float64{},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
		historySyncAt: map[string]int64{},
	}

	r.syncHistory(false)
	cooldown := r.currentCooldown("okx", "BTC/USDT")
	if cooldown.CooldownUntilMS <= now {
		t.Fatalf("expected runtime history sync to trigger symbol cooldown, got %+v", cooldown)
	}
	state := r.currentAccountState("okx")
	if state.DailyRealizedUSDT < 2 {
		t.Fatalf("expected daily realized loss updated from history sync, got %.8f", state.DailyRealizedUSDT)
	}
}

func TestSyncHistoryRuntimeProcessesActiveExchange(t *testing.T) {
	historyCalls := 0
	r := &Live{
		logger: glog.Nop(),
		cfg:    defaultRiskConfig(),
		exchanges: map[string]iface.Exchange{
			"okx": stubRiskExchange{
				history: []iface.PositionHistory{
					{
						InstID:      "BTC-USDT-SWAP",
						CloseTime:   int64ToString(time.Now().UnixMilli()),
						RealizedPnl: "-1",
					},
				},
				historyCalls: &historyCalls,
			},
		},
		accountStates: map[string]models.RiskAccountState{},
		availableUSDT: map[string]float64{},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
		historySyncAt: map[string]int64{},
	}

	r.syncHistory(false)
	if historyCalls != 1 {
		t.Fatalf("expected active exchange history sync, got calls=%d", historyCalls)
	}
}

func TestOpenPositionCountWithPending(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	r := &Live{
		positions: map[string]models.Position{
			"okx|ETH-USDT-SWAP|long|isolated": {
				Exchange:     "okx",
				Symbol:       "ETH/USDT",
				PositionSide: "long",
				Status:       models.PositionStatusOpen,
			},
			"okx|BTC-USDT-SWAP|long|isolated": {
				Exchange:     "okx",
				Symbol:       "BTC/USDT",
				PositionSide: "long",
				Status:       models.PositionStatusClosed,
			},
		},
		pendingOpens: map[string]int64{
			livePendingOpenKey("okx", "SOL/USDT", "long"):  nowMS + 5000,
			livePendingOpenKey("okx", "ETH/USDT", "long"):  nowMS + 5000,
			livePendingOpenKey("okx", "XRP/USDT", "short"): nowMS - 1,
		},
	}

	got := r.openPositionCountWithPending(nowMS)
	if got != 2 {
		t.Fatalf("unexpected open position count with pending: got %d want 2", got)
	}
	if _, exists := r.pendingOpens[livePendingOpenKey("okx", "XRP/USDT", "short")]; exists {
		t.Fatalf("expected expired pending open to be pruned")
	}
}

func TestTryReservePendingOpenRespectsLimit(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	r := &Live{
		positions: map[string]models.Position{
			"okx|ETH-USDT-SWAP|long|isolated": {
				Exchange:     "okx",
				Symbol:       "ETH/USDT",
				PositionSide: "long",
				Status:       models.PositionStatusOpen,
			},
			"okx|BTC-USDT-SWAP|long|isolated": {
				Exchange:     "okx",
				Symbol:       "BTC/USDT",
				PositionSide: "long",
				Status:       models.PositionStatusOpen,
			},
		},
		pendingOpens: map[string]int64{},
	}

	countAfterReserve, ok := r.tryReservePendingOpen("okx", "SOL/USDT", "long", nowMS, 3)
	if !ok {
		t.Fatalf("expected first reserve success")
	}
	if countAfterReserve != 3 {
		t.Fatalf("unexpected count after first reserve: got %d want 3", countAfterReserve)
	}

	countOnReject, ok := r.tryReservePendingOpen("okx", "WLFI/USDT", "long", nowMS, 3)
	if ok {
		t.Fatalf("expected second reserve rejected by max_open_positions")
	}
	if countOnReject != 3 {
		t.Fatalf("unexpected count on reject: got %d want 3", countOnReject)
	}
}

func TestTryReservePendingOpenRejectsDuplicateKey(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	r := &Live{
		positions:     map[string]models.Position{},
		pendingOpens:  map[string]int64{},
		openPositions: map[string]models.RiskOpenPosition{},
	}

	countAfterReserve, ok := r.tryReservePendingOpen("okx", "SOL/USDT", "long", nowMS, 3)
	if !ok {
		t.Fatalf("expected first reserve success")
	}
	if countAfterReserve != 1 {
		t.Fatalf("unexpected count after first reserve: got %d want 1", countAfterReserve)
	}

	countOnReject, ok := r.tryReservePendingOpen("okx", "SOL/USDT", "long", nowMS+1, 3)
	if ok {
		t.Fatalf("expected duplicate reserve rejected")
	}
	if countOnReject != 1 {
		t.Fatalf("unexpected count on duplicate reject: got %d want 1", countOnReject)
	}
}

func TestBuildTPSLIndexUsesTrailingStopPriceAsSL(t *testing.T) {
	index := buildTPSLIndex([]iface.TPSLOrder{{
		InstID:    "AXS-USDT-SWAP",
		OrdType:   "move_order_stop",
		Side:      "sell",
		PosSide:   "long",
		TriggerPx: "1.1471",
	}})
	item, ok := index[tpslIndexKey("AXS/USDT", "long")]
	if !ok {
		t.Fatalf("expected trailing stop index item")
	}
	if math.Abs(item.sl-1.1471) > 1e-9 {
		t.Fatalf("unexpected trailing stop sl: got %.8f want %.8f", item.sl, 1.1471)
	}
}

func TestEvaluateOpenRollsBackPendingOnValidationFailure(t *testing.T) {
	r := &Live{
		cfg:           defaultRiskConfig(),
		accountStates: map[string]models.RiskAccountState{},
		availableUSDT: map[string]float64{},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
		pendingOpens:  map[string]int64{},
		historySyncAt: map[string]int64{},
	}
	r.cfg.MaxOpenPositions = 3

	_, err := r.evaluateOpen(
		"okx",
		"SOL/USDT",
		"15m",
		models.Signal{HighSide: 1, Action: 8, Strategy: "turtle"},
		models.Position{},
		false,
		models.MarketData{},
	)
	if err == nil {
		t.Fatalf("expected evaluateOpen validation failure")
	}
	key := livePendingOpenKey("okx", "SOL/USDT", "long")
	if _, exists := r.pendingOpens[key]; exists {
		t.Fatalf("expected pending open rollback on evaluateOpen failure")
	}
}

func TestNotifyExecutionResultClearsPendingOnFailure(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	key := livePendingOpenKey("okx", "ETH/USDT", "long")
	r := &Live{
		pendingOpens: map[string]int64{
			key: nowMS + 5000,
		},
	}
	r.NotifyExecutionResult(models.Decision{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Action:   models.DecisionActionOpenLong,
	}, fmt.Errorf("mock execution failed"))
	if _, ok := r.pendingOpens[key]; ok {
		t.Fatalf("expected pending open removed on execution failure")
	}
}

func TestNotifyExecutionResultKeepsPendingOnSuccess(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	key := livePendingOpenKey("okx", "ETH/USDT", "long")
	r := &Live{
		pendingOpens: map[string]int64{
			key: nowMS + 5000,
		},
	}
	r.NotifyExecutionResult(models.Decision{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Action:   models.DecisionActionOpenLong,
	}, nil)
	if _, ok := r.pendingOpens[key]; !ok {
		t.Fatalf("expected pending open kept until position refresh")
	}
}

func TestSyncOpenPositionsClearsPendingOnConfirmedOpen(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	key := livePendingOpenKey("okx", "SOL/USDT", "long")
	r := &Live{
		pendingOpens: map[string]int64{
			key: nowMS + 5000,
		},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
	}
	r.syncOpenPositions("okx", []iface.Position{
		{
			InstID:   "SOL-USDT-SWAP",
			Pos:      "1",
			PosSide:  "long",
			MgnMode:  "isolated",
			Margin:   "10",
			Lever:    "3",
			AvgPx:    "100",
			Upl:      "0",
			UplRatio: "0",
			MarkPx:   "101",
			OpenTime: int64ToString(nowMS),
		},
	}, nil)
	if _, ok := r.pendingOpens[key]; ok {
		t.Fatalf("expected pending open removed after position refresh confirms open position")
	}
}

func TestSyncOpenPositionsAppliesPendingMetaBeforeSignalInference(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	pendingKey := livePendingOpenKey("okx", "SOL/USDT", "long")
	r := &Live{
		signalCache: NewSignalCache(),
		pendingOpens: map[string]int64{
			pendingKey: nowMS + 5000,
		},
		pendingMeta: map[string]models.StrategyContextMeta{
			pendingKey: {
				StrategyName:       "manual",
				StrategyVersion:    "v1",
				StrategyTimeframes: []string{"15m"},
			},
		},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
	}
	// Add a conflicting cached signal; pending meta should still win for this new open.
	r.signalCache.Upsert(models.Signal{
		Exchange:           "okx",
		Symbol:             "SOL/USDT",
		Timeframe:          "1h",
		GroupID:            "turtle|1h|long|999",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.5",
		HighSide:           1,
		TriggerTimestamp:   1000,
		TrendingTimestamp:  900,
		StrategyTimeframes: []string{"15m", "1h"},
	})
	r.syncOpenPositions("okx", []iface.Position{
		{
			InstID:   "SOL-USDT-SWAP",
			Pos:      "1",
			PosSide:  "long",
			MgnMode:  "isolated",
			Margin:   "10",
			Lever:    "3",
			AvgPx:    "100",
			Upl:      "0",
			UplRatio: "0",
			MarkPx:   "101",
			OpenTime: int64ToString(nowMS),
		},
	}, nil)

	key := livePositionKey("okx", "SOL-USDT-SWAP", "long", "isolated")
	view, ok := r.positions[key]
	if !ok {
		t.Fatalf("position view not found")
	}
	if view.StrategyName != "manual" {
		t.Fatalf("expected strategy_name=manual, got %s", view.StrategyName)
	}
	if view.Timeframe != "15m" {
		t.Fatalf("expected timeframe=15m, got %s", view.Timeframe)
	}
	if _, exists := r.pendingMeta[pendingKey]; exists {
		t.Fatalf("expected pending meta cleared after position confirmation")
	}
}

func TestSyncOpenPositionsWritesRowJSONEnvelopeWithSignalMeta(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	r := &Live{
		singletonID:   123,
		singletonUUID: "run-open-123",
		signalCache:   NewSignalCache(),
		pendingOpens:  map[string]int64{},
		positions:     map[string]models.Position{},
		openPositions: map[string]models.RiskOpenPosition{},
		cooldowns:     map[string]models.RiskSymbolCooldownState{},
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:           "okx",
		Symbol:             "SOL/USDT",
		Timeframe:          "1h",
		GroupID:            "turtle|1h|long|999",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.5",
		HighSide:           1,
		TriggerTimestamp:   1000,
		TrendingTimestamp:  900,
		StrategyTimeframes: []string{"15m", "1h"},
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
	})
	r.syncOpenPositions("okx", []iface.Position{
		{
			InstID:   "SOL-USDT-SWAP",
			Pos:      "1",
			PosSide:  "long",
			MgnMode:  "isolated",
			Margin:   "10",
			Lever:    "3",
			AvgPx:    "100",
			Upl:      "0",
			UplRatio: "0",
			MarkPx:   "101",
			OpenTime: int64ToString(nowMS),
		},
	}, nil)
	key := livePositionKey("okx", "SOL-USDT-SWAP", "long", "isolated")
	item, ok := r.openPositions[key]
	if !ok {
		t.Fatalf("open position not found")
	}
	env, ok := models.ParsePositionRowEnvelope(item.RowJSON)
	if !ok {
		t.Fatalf("row_json is not envelope: %s", item.RowJSON)
	}
	if len(env.ExchangeRaw) == 0 {
		t.Fatalf("exchange_raw should not be empty")
	}
	if env.GobotMeta.StrategyName != "turtle" || env.GobotMeta.StrategyVersion != "v0.0.5" {
		t.Fatalf("unexpected gobot_meta: %#v", env.GobotMeta)
	}
	if len(env.GobotMeta.StrategyTimeframes) != 2 || env.GobotMeta.StrategyTimeframes[0] != "15m" || env.GobotMeta.StrategyTimeframes[1] != "1h" {
		t.Fatalf("unexpected strategy_timeframes: %#v", env.GobotMeta.StrategyTimeframes)
	}
	if env.RuntimeMeta.SingletonID != 123 || env.RuntimeMeta.RunID != "run-open-123" {
		t.Fatalf("unexpected runtime_meta: %#v", env.RuntimeMeta)
	}
	if env.GobotMeta.GroupID != "turtle|1h|long|999" {
		t.Fatalf("unexpected gobot_meta.group_id: %s", env.GobotMeta.GroupID)
	}
	view := r.positions[key]
	if item.SingletonID != 123 {
		t.Fatalf("unexpected open singleton_id: %d", item.SingletonID)
	}
	if view.SingletonID != 123 {
		t.Fatalf("unexpected view singleton_id: %d", view.SingletonID)
	}
	if view.StrategyName != "turtle" {
		t.Fatalf("unexpected view strategy_name: %s", view.StrategyName)
	}
	if view.Timeframe != "1h" {
		t.Fatalf("unexpected view timeframe: %s", view.Timeframe)
	}
	if view.GroupID != "turtle|1h|long|999" {
		t.Fatalf("unexpected view group_id: %s", view.GroupID)
	}
}

func TestSyncOpenPositionsKeepsExistingMetaWithoutSignal(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	prevMeta := models.StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.5",
		TrendingTimestamp:  888,
		StrategyTimeframes: []string{"15m", "1h"},
		GroupID:            "turtle|1h|long|888",
		StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
	}
	key := livePositionKey("okx", "SOL-USDT-SWAP", "long", "isolated")
	r := &Live{
		signalCache:  NewSignalCache(),
		pendingOpens: map[string]int64{},
		positions: map[string]models.Position{
			key: {
				Exchange: "okx",
				Symbol:   "SOL/USDT",
				Status:   models.PositionStatusOpen,
			},
		},
		openPositions: map[string]models.RiskOpenPosition{
			key: {
				Exchange: "okx",
				Symbol:   "SOL/USDT",
				InstID:   "SOL-USDT-SWAP",
				PosSide:  "long",
				MgnMode:  "isolated",
				RowJSON:  models.MarshalPositionRowEnvelope(map[string]string{"instId": "SOL-USDT-SWAP"}, prevMeta),
			},
		},
		cooldowns: map[string]models.RiskSymbolCooldownState{},
	}
	r.syncOpenPositions("okx", []iface.Position{
		{
			InstID:     "SOL-USDT-SWAP",
			Pos:        "1",
			PosSide:    "long",
			MgnMode:    "isolated",
			Margin:     "11",
			Lever:      "3",
			AvgPx:      "101",
			Upl:        "0.1",
			UplRatio:   "0.01",
			MarkPx:     "102",
			OpenTime:   int64ToString(nowMS - 1000),
			UpdateTime: int64ToString(nowMS),
		},
	}, nil)
	item := r.openPositions[key]
	meta := models.ExtractStrategyContextMeta(item.RowJSON)
	if meta.StrategyName != prevMeta.StrategyName || meta.StrategyVersion != prevMeta.StrategyVersion {
		t.Fatalf("expected strategy metadata preserved, got %#v", meta)
	}
	if len(meta.StrategyTimeframes) != 2 || meta.StrategyTimeframes[0] != "15m" || meta.StrategyTimeframes[1] != "1h" {
		t.Fatalf("expected strategy_timeframes preserved, got %#v", meta.StrategyTimeframes)
	}
	if meta.GroupID != "turtle|1h|long|888" {
		t.Fatalf("expected group_id preserved, got %s", meta.GroupID)
	}
}

func TestSyncOpenPositionsPreservesExistingRuntimeMeta(t *testing.T) {
	nowMS := time.Now().UnixMilli()
	key := livePositionKey("okx", "SOL-USDT-SWAP", "long", "isolated")
	r := &Live{
		singletonID:   200,
		singletonUUID: "run-new",
		signalCache:   NewSignalCache(),
		pendingOpens:  map[string]int64{},
		positions: map[string]models.Position{
			key: {
				Exchange: "okx",
				Symbol:   "SOL/USDT",
				Status:   models.PositionStatusOpen,
			},
		},
		openPositions: map[string]models.RiskOpenPosition{
			key: {
				Exchange: "okx",
				Symbol:   "SOL/USDT",
				InstID:   "SOL-USDT-SWAP",
				PosSide:  "long",
				MgnMode:  "isolated",
				RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
					map[string]string{"instId": "SOL-USDT-SWAP"},
					models.StrategyContextMeta{},
					models.PositionRuntimeMeta{SingletonID: 100, RunID: "run-old"},
				),
			},
		},
		cooldowns: map[string]models.RiskSymbolCooldownState{},
	}
	r.syncOpenPositions("okx", []iface.Position{
		{
			InstID:     "SOL-USDT-SWAP",
			Pos:        "1",
			PosSide:    "long",
			MgnMode:    "isolated",
			Margin:     "11",
			Lever:      "3",
			AvgPx:      "101",
			Upl:        "0.1",
			UplRatio:   "0.01",
			MarkPx:     "102",
			OpenTime:   int64ToString(nowMS - 1000),
			UpdateTime: int64ToString(nowMS),
		},
	}, nil)
	env, ok := models.ParsePositionRowEnvelope(r.openPositions[key].RowJSON)
	if !ok {
		t.Fatalf("row_json is not envelope: %s", r.openPositions[key].RowJSON)
	}
	if env.RuntimeMeta.SingletonID != 100 || env.RuntimeMeta.RunID != "run-old" {
		t.Fatalf("expected runtime_meta preserved, got %#v", env.RuntimeMeta)
	}
	if r.openPositions[key].SingletonID != 100 {
		t.Fatalf("expected open singleton_id preserved, got %d", r.openPositions[key].SingletonID)
	}
	if r.positions[key].SingletonID != 100 {
		t.Fatalf("expected view singleton_id preserved, got %d", r.positions[key].SingletonID)
	}
}

func TestSyncOpenPositionsFillsMissingRuntimeMetaFields(t *testing.T) {
	tests := []struct {
		name string
		prev models.PositionRuntimeMeta
		want models.PositionRuntimeMeta
	}{
		{
			name: "fill missing run id",
			prev: models.PositionRuntimeMeta{SingletonID: 100},
			want: models.PositionRuntimeMeta{SingletonID: 100, RunID: "run-new"},
		},
		{
			name: "fill missing singleton id",
			prev: models.PositionRuntimeMeta{RunID: "run-old"},
			want: models.PositionRuntimeMeta{SingletonID: 200, RunID: "run-old"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nowMS := time.Now().UnixMilli()
			key := livePositionKey("okx", "SOL-USDT-SWAP", "long", "isolated")
			r := &Live{
				singletonID:   200,
				singletonUUID: "run-new",
				signalCache:   NewSignalCache(),
				pendingOpens:  map[string]int64{},
				positions: map[string]models.Position{
					key: {
						Exchange: "okx",
						Symbol:   "SOL/USDT",
						Status:   models.PositionStatusOpen,
					},
				},
				openPositions: map[string]models.RiskOpenPosition{
					key: {
						Exchange: "okx",
						Symbol:   "SOL/USDT",
						InstID:   "SOL-USDT-SWAP",
						PosSide:  "long",
						MgnMode:  "isolated",
						RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
							map[string]string{"instId": "SOL-USDT-SWAP"},
							models.StrategyContextMeta{},
							tt.prev,
						),
					},
				},
				cooldowns: map[string]models.RiskSymbolCooldownState{},
			}
			r.syncOpenPositions("okx", []iface.Position{
				{
					InstID:     "SOL-USDT-SWAP",
					Pos:        "1",
					PosSide:    "long",
					MgnMode:    "isolated",
					Margin:     "11",
					Lever:      "3",
					AvgPx:      "101",
					Upl:        "0.1",
					UplRatio:   "0.01",
					MarkPx:     "102",
					OpenTime:   int64ToString(nowMS - 1000),
					UpdateTime: int64ToString(nowMS),
				},
			}, nil)
			env, ok := models.ParsePositionRowEnvelope(r.openPositions[key].RowJSON)
			if !ok {
				t.Fatalf("row_json is not envelope: %s", r.openPositions[key].RowJSON)
			}
			if env.RuntimeMeta != tt.want {
				t.Fatalf("unexpected runtime_meta: got %#v want %#v", env.RuntimeMeta, tt.want)
			}
		})
	}
}

func TestSyncSignalCacheClosedByPositionDisappearanceMarksSignalClosed(t *testing.T) {
	eventTS := int64(1700000001000)
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:                        "okx",
		Symbol:                          "SOL/USDT",
		Timeframe:                       "1h",
		Strategy:                        "turtle",
		Action:                          8,
		HighSide:                        1,
		MidSide:                         1,
		HasPosition:                     models.SignalHasOpenPosition,
		Entry:                           100,
		SL:                              95,
		TP:                              120,
		Plan1LastProfitLockMFER:         1.8,
		Plan1LastProfitLockHighBucketTS: 1700000000000,
		Plan1LastProfitLockStructPrice:  98,
		PostHighPullbackFirstEntryState: models.SignalPostHighPullbackFirstEntryArmed,
		EntryWatchTimestamp:             54321,
		TriggerTimestamp:                12345,
	})

	err := r.syncSignalCacheClosedByPositionDisappearance(models.RiskOpenPosition{
		Exchange: "okx",
		Symbol:   "SOL/USDT",
		PosSide:  "long",
		RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
			nil,
			models.StrategyContextMeta{
				StrategyName:       "turtle",
				StrategyTimeframes: []string{"15m", "1h"},
			},
			models.PositionRuntimeMeta{},
		),
	}, nil, eventTS)
	if err != nil {
		t.Fatalf("syncSignalCacheClosedByPositionDisappearance failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "SOL/USDT", "1h", "turtle")
	if !ok {
		t.Fatalf("expected cached signal to remain present")
	}
	if cached.HasPosition != models.SignalHasNoPosition {
		t.Fatalf("expected has_position=%d, got %d", models.SignalHasNoPosition, cached.HasPosition)
	}
	if cached.Action != 0 {
		t.Fatalf("expected action reset to 0, got %d", cached.Action)
	}
	if cached.TriggerTimestamp != int(eventTS) {
		t.Fatalf("expected trigger timestamp updated to %d, got %d", eventTS, cached.TriggerTimestamp)
	}
	if cached.Entry != 0 || cached.TP != 0 || cached.SL != 0 {
		t.Fatalf("expected entry/tp/sl reset, got entry=%.8f tp=%.8f sl=%.8f", cached.Entry, cached.TP, cached.SL)
	}
	if cached.EntryWatchTimestamp != 0 {
		t.Fatalf("expected entry watch timestamp reset, got %d", cached.EntryWatchTimestamp)
	}
	if cached.PostHighPullbackFirstEntryState != models.SignalPostHighPullbackFirstEntryNone {
		t.Fatalf("expected post-high-pullback state reset, got %d", cached.PostHighPullbackFirstEntryState)
	}
	if cached.Plan1LastProfitLockMFER != 0 || cached.Plan1LastProfitLockHighBucketTS != 0 || cached.Plan1LastProfitLockStructPrice != 0 {
		t.Fatalf("expected plan1 profit-lock state reset, got mfer=%.8f bucket=%d struct=%.8f", cached.Plan1LastProfitLockMFER, cached.Plan1LastProfitLockHighBucketTS, cached.Plan1LastProfitLockStructPrice)
	}
}

func TestSyncSignalCacheClosedByPositionDisappearanceRemovesTrendEndedCloseAllSignal(t *testing.T) {
	eventTS := int64(1700000001000)
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:           "okx",
		Symbol:             "SOL/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"15m", "1h"},
		Action:             64,
		HighSide:           0,
		MidSide:            0,
		HasPosition:        models.SignalHasOpenPosition,
		TriggerTimestamp:   12345,
	})

	err := r.syncSignalCacheClosedByPositionDisappearance(models.RiskOpenPosition{
		Exchange: "okx",
		Symbol:   "SOL/USDT",
		PosSide:  "long",
		RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
			nil,
			models.StrategyContextMeta{
				StrategyName:       "turtle",
				StrategyTimeframes: []string{"15m", "1h"},
			},
			models.PositionRuntimeMeta{},
		),
	}, nil, eventTS)
	if err != nil {
		t.Fatalf("syncSignalCacheClosedByPositionDisappearance failed: %v", err)
	}

	if _, ok := r.signalCache.Find("okx", "SOL/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected trend-ended close-all signal removed from cache")
	}
}

func TestSyncSignalCacheClosedByPositionDisappearanceSkipsWhenMatchingPositionStillOpen(t *testing.T) {
	eventTS := int64(1700000001000)
	r := &Live{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:         "okx",
		Symbol:           "SOL/USDT",
		Timeframe:        "1h",
		Strategy:         "turtle",
		Action:           8,
		HighSide:         1,
		MidSide:          1,
		HasPosition:      models.SignalHasOpenPosition,
		Entry:            100,
		SL:               95,
		TP:               120,
		TriggerTimestamp: 12345,
	})

	err := r.syncSignalCacheClosedByPositionDisappearance(
		models.RiskOpenPosition{
			Exchange: "okx",
			Symbol:   "SOL/USDT",
			PosSide:  "long",
			RowJSON: models.MarshalPositionRowEnvelopeWithRuntime(
				nil,
				models.StrategyContextMeta{
					StrategyName:       "turtle",
					StrategyTimeframes: []string{"15m", "1h"},
				},
				models.PositionRuntimeMeta{},
			),
		},
		map[string]models.Position{
			"okx|SOL-USDT-SWAP|long|isolated": {
				Exchange:      "okx",
				Symbol:        "SOL/USDT",
				Timeframe:     "1h",
				PositionSide:  "long",
				StrategyName:  "turtle",
				Status:        models.PositionStatusOpen,
				EntryQuantity: 1,
			},
		},
		eventTS,
	)
	if err != nil {
		t.Fatalf("syncSignalCacheClosedByPositionDisappearance failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "SOL/USDT", "1h", "turtle")
	if !ok {
		t.Fatalf("expected cached signal to remain present")
	}
	if cached.HasPosition != models.SignalHasOpenPosition {
		t.Fatalf("expected cached signal to remain open, got %d", cached.HasPosition)
	}
	if cached.TriggerTimestamp != 12345 {
		t.Fatalf("expected trigger timestamp unchanged, got %d", cached.TriggerTimestamp)
	}
}

type stubRiskExchange struct {
	balance      iface.BalanceSnapshot
	positions    []iface.Position
	history      []iface.PositionHistory
	historyCalls *int
}

func (s stubRiskExchange) Name() string { return "okx" }

func (s stubRiskExchange) NormalizeSymbol(raw string) (string, error) { return raw, nil }

func (s stubRiskExchange) GetInstrument(ctx context.Context, instID string) (iface.Instrument, error) {
	return iface.Instrument{}, fmt.Errorf("not implemented")
}

func (s stubRiskExchange) GetTickerPrice(ctx context.Context, instID string) (float64, error) {
	return 0, fmt.Errorf("not implemented")
}

func (s stubRiskExchange) GetPositions(ctx context.Context, instID string) ([]iface.Position, error) {
	return append([]iface.Position(nil), s.positions...), nil
}

func (s stubRiskExchange) GetPositionsHistory(ctx context.Context, instID string) ([]iface.PositionHistory, error) {
	if s.historyCalls != nil {
		(*s.historyCalls)++
	}
	return append([]iface.PositionHistory(nil), s.history...), nil
}

func (s stubRiskExchange) GetBalance(ctx context.Context) (iface.BalanceSnapshot, error) {
	return s.balance, nil
}

func (s stubRiskExchange) SetPositionMode(ctx context.Context, mode string) error {
	return fmt.Errorf("not implemented")
}

func (s stubRiskExchange) SetLeverage(ctx context.Context, instID, marginMode string, leverage int, posSide string) error {
	return fmt.Errorf("not implemented")
}

func (s stubRiskExchange) PlaceOrder(ctx context.Context, req iface.OrderRequest) (string, error) {
	return "", fmt.Errorf("not implemented")
}

func (s stubRiskExchange) GetOrder(ctx context.Context, instID, ordID string) (iface.Order, error) {
	return iface.Order{}, fmt.Errorf("not implemented")
}

func isValidClientOrderIDForTest(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
		case ch >= 'A' && ch <= 'Z':
		case ch >= '0' && ch <= '9':
		default:
			return false
		}
	}
	return true
}
