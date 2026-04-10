package risk

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestBackTestRejectsInvalidStopLoss(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	accountState := testMarketData(1704067200000, 100, 100, 100)

	_, err := r.EvaluateOpenBatch([]models.Signal{
		testOpenSignal(accountState.OHLCV.TS, 100, 0),
	}, accountState)
	if err == nil || !strings.Contains(err.Error(), "SL required") {
		t.Fatalf("expected SL required error, got: %v", err)
	}

	_, err = r.EvaluateOpenBatch([]models.Signal{
		testOpenSignal(accountState.OHLCV.TS, 100, 92),
	}, accountState)
	if err == nil || !strings.Contains(err.Error(), "SL too large") {
		t.Fatalf("expected SL too large error, got: %v", err)
	}
}

func TestBackTestStatusUsesConfiguredGroupedTrendGuardMode(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TrendGuard.Enabled = true
	r.cfg.TrendGuard.Mode = trendGuardModeGrouped

	status := r.Status()
	detailMap, ok := status.Details.(map[string]any)
	if !ok {
		t.Fatalf("unexpected details type: %T", status.Details)
	}
	details, ok := detailMap["trend_guard"].(trendGuardStatusDetails)
	if !ok {
		t.Fatalf("unexpected trend_guard detail type: %T", detailMap["trend_guard"])
	}
	if details.Mode != trendGuardModeGrouped {
		t.Fatalf("unexpected trend_guard mode: got %s want %s", details.Mode, trendGuardModeGrouped)
	}
}

func TestBackTestGroupedTrendGuardRejectsNonLeaderOpen(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TrendGuard.Enabled = true
	r.cfg.TrendGuard.Mode = trendGuardModeGrouped
	r.cfg.TrendGuard.MaxStartLagBars = 12
	r.cfg.TrendGuard.LeaderMinPriorityScore = 50

	baseTS := int64(1_773_840_000_000)
	leader := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	leader.Entry = 100
	leader.SL = 95
	leader.Action = 8

	decision, err := r.EvaluateOpenBatch([]models.Signal{leader}, groupedTrendGuardEvalContextForPair("AGLD/USDT", []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}))
	if err != nil {
		t.Fatalf("expected leader open allowed, got err=%v", err)
	}
	if decision.Action != models.DecisionActionOpenLong {
		t.Fatalf("unexpected leader decision action: %s", decision.Action)
	}

	follower := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	follower.Entry = 100
	follower.SL = 95
	follower.Action = 8

	_, err = r.EvaluateOpenBatch([]models.Signal{follower}, groupedTrendGuardEvalContextForPair("FIL/USDT", []float64{100, 104, 108, 112, 116, 120, 124, 128, 132}))
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected grouped trend guard reject error, got: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "FIL/USDT", "turtle", "1m/5m/30m")
	if !ok {
		t.Fatalf("expected rejected follower signal in cache")
	}
	if cached.Action != models.SignalActionOpenTrendGuardRejected {
		t.Fatalf("unexpected follower action: got %d want %d", cached.Action, models.SignalActionOpenTrendGuardRejected)
	}
}

func TestBackTestRefreshTrendGuardCandidateClearsGroupedWaitingRefreshReject(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TrendGuard.Enabled = true
	r.cfg.TrendGuard.Mode = trendGuardModeGrouped
	r.cfg.TrendGuard.MaxStartLagBars = 12
	r.cfg.TrendGuard.LeaderMinPriorityScore = 50

	baseTS := int64(1_773_840_000_000)
	signal := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	open := signal
	open.Entry = 100
	open.SL = 95
	open.Action = 8
	evalCtx := groupedTrendGuardEvalContextForPair("FIL/USDT", []float64{100, 103, 106, 109, 112, 115, 118, 121, 124})

	r.mu.Lock()
	r.observeTrendGuardSignalLocked(signal, evalCtx, baseTS)
	r.mu.Unlock()

	candidateKey := "okx|FIL/USDT"
	candidate, ok := groupedTrendGuardCandidateFromSignal(open)
	if !ok {
		t.Fatalf("expected valid trend-guard candidate context")
	}
	r.trendGuard.mu.Lock()
	runtime := r.trendGuard.findRuntimeLocked(candidate, r.cfg.TrendGuard)
	if runtime == nil {
		r.trendGuard.mu.Unlock()
		t.Fatalf("expected grouped runtime")
	}
	delete(runtime.scoreCtx, candidateKey)
	r.trendGuard.mu.Unlock()

	r.mu.Lock()
	reason, reject := r.matchTrendGuardOpenReasonLocked(open, r.cfg.TrendGuard)
	r.mu.Unlock()
	if !reject || !strings.Contains(reason, "waiting candidate score refresh") {
		t.Fatalf("expected waiting candidate score refresh reject, got reject=%v reason=%q", reject, reason)
	}

	if err := r.RefreshTrendGuardCandidate(signal, evalCtx); err != nil {
		t.Fatalf("refresh trend guard candidate failed: %v", err)
	}

	decision, err := r.EvaluateOpenBatch([]models.Signal{open}, evalCtx)
	if err != nil {
		t.Fatalf("expected grouped trend guard open allowed after refresh, got err=%v", err)
	}
	if decision.Action != models.DecisionActionOpenLong {
		t.Fatalf("unexpected decision action after refresh: got %s want %s", decision.Action, models.DecisionActionOpenLong)
	}
}

func TestBackTestCooldownAfterTwoStopLosses(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	baseTS := int64(1704067200000)

	for i := 0; i < 2; i++ {
		openTS := baseTS + int64(i)*3600*1000
		accountState := testMarketData(openTS, 100, 100, 100)
		_, err := r.EvaluateOpenBatch([]models.Signal{
			testOpenSignal(openTS, 100, 95),
		}, accountState)
		if err != nil {
			t.Fatalf("open #%d failed: %v", i+1, err)
		}
		if err := r.OnMarketData(testMarketData(openTS+1, 100, 95, 95)); err != nil {
			t.Fatalf("on market data #%d failed: %v", i+1, err)
		}
	}

	_, err := r.EvaluateOpenBatch([]models.Signal{
		testOpenSignal(baseTS+2*3600*1000, 100, 95),
	}, testMarketData(baseTS+2*3600*1000, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "cooldown until") {
		t.Fatalf("expected cooldown error, got: %v", err)
	}
}

func TestBackTestAccumulatesNegativeReturnWithoutCircuitBreaker(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	startTS := int64(1704067200000)
	step := int64((7 * time.Hour) / time.Millisecond)

	for i := 0; i < 7; i++ {
		openTS := startTS + int64(i)*step
		accountState := testMarketData(openTS, 100, 100, 100)
		_, err := r.EvaluateOpenBatch([]models.Signal{
			testOpenSignal(openTS, 100, 95),
		}, accountState)
		if err != nil {
			t.Fatalf("open #%d failed: %v", i+1, err)
		}
		if err := r.OnMarketData(testMarketData(openTS+1, 100, 95, 95)); err != nil {
			t.Fatalf("on market data #%d failed: %v", i+1, err)
		}
	}

	report := r.Finalize()
	if report.CircuitBreaker {
		t.Fatalf("expected circuit breaker=false, got true")
	}
	if report.ReturnRate >= 0 {
		t.Fatalf("expected negative return rate, got %.8f", report.ReturnRate)
	}

	_, err := r.EvaluateOpenBatch([]models.Signal{
		testOpenSignal(startTS+8*step, 100, 95),
	}, testMarketData(startTS+8*step, 100, 100, 100))
	if err != nil {
		t.Fatalf("expected open to remain available without circuit breaker, got: %v", err)
	}
}

func TestBackTestOnMarketDataTriggersOnlyOnSmallestStrategyTimeframe(t *testing.T) {
	t.Run("stop_loss", func(t *testing.T) {
		r := NewBackTest(BackTestConfig{})
		openTS := int64(1704067200000)
		signal := testOpenSignal(openTS, 100, 95)
		signal.StrategyTimeframes = []string{"3m", "15m", "1h"}
		if _, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
			t.Fatalf("open failed: %v", err)
		}

		if err := r.OnMarketData(testMarketDataTimeframe("15m", openTS+15*60*1000, 100, 94, 94)); err != nil {
			t.Fatalf("15m on market data failed: %v", err)
		}
		if positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h"); err != nil {
			t.Fatalf("list open positions failed: %v", err)
		} else if len(positions) != 1 {
			t.Fatalf("expected position to stay open on 15m event, got %d", len(positions))
		}

		if err := r.OnMarketData(testMarketDataTimeframe("3m", openTS+18*60*1000, 100, 94, 94)); err != nil {
			t.Fatalf("3m on market data failed: %v", err)
		}
		if positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h"); err != nil {
			t.Fatalf("list open positions failed: %v", err)
		} else if len(positions) != 0 {
			t.Fatalf("expected position to close on 3m event, got %d", len(positions))
		}
		if len(r.trades) != 1 || r.trades[0].CloseReason != "stop_loss" {
			t.Fatalf("expected stop_loss trade, got %+v", r.trades)
		}
	})

	t.Run("take_profit", func(t *testing.T) {
		r := NewBackTest(BackTestConfig{})
		openTS := int64(1704067200000)
		signal := testOpenSignal(openTS, 100, 95)
		signal.StrategyTimeframes = []string{"3m", "15m", "1h"}
		if _, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
			t.Fatalf("open failed: %v", err)
		}
		positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h")
		if err != nil {
			t.Fatalf("list open positions failed: %v", err)
		}
		if len(positions) != 1 {
			t.Fatalf("unexpected open position count: got %d want 1", len(positions))
		}
		tp := positions[0].TakeProfitPrice
		if tp <= 0 {
			t.Fatalf("expected stored take profit price > 0, got %.8f", tp)
		}

		if err := r.OnMarketData(testMarketDataTimeframe("15m", openTS+15*60*1000, tp, 100, tp)); err != nil {
			t.Fatalf("15m on market data failed: %v", err)
		}
		if positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h"); err != nil {
			t.Fatalf("list open positions failed: %v", err)
		} else if len(positions) != 1 {
			t.Fatalf("expected position to stay open on 15m event, got %d", len(positions))
		}

		if err := r.OnMarketData(testMarketDataTimeframe("3m", openTS+18*60*1000, tp, 100, tp)); err != nil {
			t.Fatalf("3m on market data failed: %v", err)
		}
		if positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h"); err != nil {
			t.Fatalf("list open positions failed: %v", err)
		} else if len(positions) != 0 {
			t.Fatalf("expected position to close on 3m event, got %d", len(positions))
		}
		if len(r.trades) != 1 || r.trades[0].CloseReason != "take_profit" {
			t.Fatalf("expected take_profit trade, got %+v", r.trades)
		}
	})
}

func TestBackTestMarketOrderIgnoresSignalEntry(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)
	signal := testOpenSignal(ts, 999, 95)
	signal.OrderType = models.OrderTypeMarket

	decision, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketData(ts, 100, 100, 100))
	if err != nil {
		t.Fatalf("market open failed: %v", err)
	}
	if decision.OrderType != models.OrderTypeMarket {
		t.Fatalf("unexpected decision order type: got %s want %s", decision.OrderType, models.OrderTypeMarket)
	}
	if decision.Price != 100 {
		t.Fatalf("unexpected market decision price: got %.8f want 100", decision.Price)
	}
}

func TestBackTestGetAccountFundsUsesDailyClosedProfitAndUnrealized(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	nowMS := time.Now().UnixMilli()
	exchange := "back-test"
	pair := pairKey(exchange, "BTC/USDT")

	r.mu.Lock()
	r.accountStates[exchange] = models.RiskAccountState{
		Exchange:              exchange,
		TradeDate:             tradeDateForTimestampMS(nowMS),
		TotalUSDT:             simulationAvailableBudget,
		TradingUSDT:           simulationAvailableBudget,
		PerTradeUSDT:          simulationMarginBasis,
		DailyClosedProfitUSDT: 5,
		UpdatedAtMS:           nowMS,
	}
	r.trades = []BackTestTrade{{ProfitRate: 5}}
	r.positions[pair] = &backTestPosition{
		Exchange:      exchange,
		Symbol:        "BTC/USDT",
		Side:          positionSideLong,
		Margin:        1,
		EntryPrice:    100,
		RemainingQty:  2,
		EntryQuantity: 2,
	}
	r.marks[pair] = backTestMark{Price: 105}
	r.mu.Unlock()

	funds, err := r.GetAccountFunds(exchange)
	if err != nil {
		t.Fatalf("GetAccountFunds failed: %v", err)
	}
	if funds.Currency != "RATE" {
		t.Fatalf("unexpected currency: got %s want RATE", funds.Currency)
	}
	if funds.ClosedProfitRate != 5 {
		t.Fatalf("unexpected closed profit rate: got %.8f want 5", funds.ClosedProfitRate)
	}
	if funds.FloatingProfitRate != 10 {
		t.Fatalf("unexpected floating profit rate: got %.8f want 10", funds.FloatingProfitRate)
	}
	if funds.TotalProfitRate != 15 {
		t.Fatalf("unexpected total profit rate: got %.8f want 15", funds.TotalProfitRate)
	}
}

func TestBackTestLimitOrderRequiresEntry(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)
	signal := testOpenSignal(ts, 0, 95)
	signal.OrderType = models.OrderTypeLimit

	_, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketData(ts, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "limit order requires entry price") {
		t.Fatalf("expected limit entry error, got: %v", err)
	}
}

func TestBackTestOpenRejectsWhenMaxOpenPositionsReached(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.MaxOpenPositions = 1
	r.positions[pairKey("okx", "ETH/USDT")] = &backTestPosition{
		Exchange:      "okx",
		Symbol:        "ETH/USDT",
		Timeframe:     "1h",
		Side:          positionSideLong,
		EntryPrice:    2000,
		EntryQuantity: 1,
		RemainingQty:  1,
		Margin:        100,
		Leverage:      2,
		EntryTS:       1704067200000,
	}

	_, err := r.EvaluateOpenBatch(
		[]models.Signal{testOpenSignal(1704070800000, 100, 95)},
		testMarketData(1704070800000, 100, 100, 100),
	)
	if err == nil || !strings.Contains(err.Error(), "max open positions reached") {
		t.Fatalf("expected max open positions error, got: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "turtle", "1h")
	if !ok {
		t.Fatalf("expected rejected open signal cached")
	}
	if cached.Action != models.SignalActionOpenRiskRejected {
		t.Fatalf("expected cached action %d, got %d", models.SignalActionOpenRiskRejected, cached.Action)
	}
}

func TestBackTestTrendGuardRejectWritesTemporarySignalAction(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TrendGuard.Enabled = true
	r.cfg.TrendGuard.Mode = trendGuardModeLegacy
	r.positions[pairKey("okx", "ETH/USDT")] = &backTestPosition{
		Exchange:      "okx",
		Symbol:        "ETH/USDT",
		Timeframe:     "1h",
		Side:          positionSideLong,
		Strategy:      "turtle",
		EntryPrice:    2000,
		EntryQuantity: 1,
		RemainingQty:  1,
		Leverage:      2,
		EntryTS:       1704067200000,
	}

	_, err := r.EvaluateOpenBatch(
		[]models.Signal{{
			Exchange:          "okx",
			Symbol:            "BTC/USDT",
			Timeframe:         "1h",
			Strategy:          "turtle",
			Action:            8,
			HighSide:          1,
			SL:                95,
			TriggerTimestamp:  1704067200000,
			TrendingTimestamp: 1704067200000,
		}},
		testMarketData(1704067200000, 100, 100, 100),
	)
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected trend guard rejection, got: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "turtle", "1h")
	if !ok {
		t.Fatalf("expected trend-guard rejected open signal cached")
	}
	if cached.Action != models.SignalActionOpenTrendGuardRejected {
		t.Fatalf("expected cached action %d, got %d", models.SignalActionOpenTrendGuardRejected, cached.Action)
	}
}

func TestBackTestOpenRejectWritesComboTradeDisabledAsRiskRejected(t *testing.T) {
	r := NewBackTest(BackTestConfig{
		StrategyCombos: []models.StrategyComboConfig{
			{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: false},
		},
	})
	ts := int64(1704067200000)

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

func TestBackTestOpenDisablesDefaultTPWhenModeDisabled(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TP.Mode = tpModeDisabled
	ts := int64(1704067200000)

	decision, err := r.EvaluateOpenBatch([]models.Signal{testOpenSignal(ts, 100, 95)}, testMarketData(ts, 100, 100, 100))
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if decision.TakeProfitPrice != 0 {
		t.Fatalf("expected disabled mode open TP=0, got %.8f", decision.TakeProfitPrice)
	}
}

func TestBackTestOpenWritesComputedTPBackToSignalCache(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)

	decision, err := r.EvaluateOpenBatch([]models.Signal{testOpenSignal(ts, 100, 95)}, testMarketData(ts, 100, 100, 100))
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if decision.TakeProfitPrice <= 0 {
		t.Fatalf("expected computed TP > 0, got %.8f", decision.TakeProfitPrice)
	}

	signals, err := r.ListSignalsByPair("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("ListSignalsByPair failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("unexpected signal count: got %d want 1", len(signals))
	}
	if !floatAlmostEqual(signals[0].TP, decision.TakeProfitPrice) {
		t.Fatalf("expected cached signal TP %.8f, got %.8f", decision.TakeProfitPrice, signals[0].TP)
	}
}

func TestBackTestMoveClearsTPWhenModeDisabled(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)
	openSignal := testOpenSignal(ts, 100, 95)
	openSignal.TP = 120

	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketData(ts, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	r.cfg.TP.Mode = tpModeDisabled
	moveSignal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Action:    16,
		HighSide:  1,
		TP:        0,
		SL:        96,
	}
	decision, err := r.EvaluateUpdate(moveSignal, models.Position{}, testMarketData(ts+3600*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("move failed: %v", err)
	}
	if decision.TakeProfitPrice != 0 {
		t.Fatalf("expected disabled mode update TP=0, got %.8f", decision.TakeProfitPrice)
	}

	positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h")
	if err != nil {
		t.Fatalf("list open positions failed: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("unexpected open position count: got %d want 1", len(positions))
	}
	if positions[0].TakeProfitPrice != 0 {
		t.Fatalf("expected stored TP cleared to 0, got %.8f", positions[0].TakeProfitPrice)
	}
}

func TestBackTestSyncSignalCacheFromPositionRefreshUpdatesEntryTPSL(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.signalCache.Upsert(models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		HighSide:  1,
		Entry:     0,
		TP:        81000,
		SL:        69000,
	})

	err := r.syncSignalCacheFromPositionLocked(models.Signal{}, &backTestPosition{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		Timeframe:       "1h",
		Strategy:        "turtle",
		EntryPrice:      70000,
		TakeProfitPrice: 82000,
		StopLossPrice:   69100,
		RemainingQty:    0.1,
	}, 1700000000000)
	if err != nil {
		t.Fatalf("syncSignalCacheFromPositionLocked failed: %v", err)
	}

	signals, err := r.ListSignalsByPair("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("ListSignalsByPair failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("unexpected signal count: got %d want 1", len(signals))
	}
	if !floatAlmostEqual(signals[0].Entry, 70000) {
		t.Fatalf("expected cached signal Entry 70000, got %.8f", signals[0].Entry)
	}
	if !floatAlmostEqual(signals[0].TP, 82000) {
		t.Fatalf("expected cached signal TP 82000, got %.8f", signals[0].TP)
	}
	if !floatAlmostEqual(signals[0].SL, 69100) {
		t.Fatalf("expected cached signal SL 69100, got %.8f", signals[0].SL)
	}
}

func TestBackTestMoveRejectsOwnershipStrategyMismatch(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)
	openSignal := testOpenSignal(ts, 100, 95)
	openSignal.Strategy = "manual"
	openSignal.Timeframe = "1h"
	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketData(ts, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	moveSignal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Action:    16,
		HighSide:  1,
		TP:        0,
		SL:        96,
		Strategy:  "turtle",
	}
	_, err := r.EvaluateUpdate(moveSignal, models.Position{}, testMarketData(ts+3600*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("expected non-owner update to pass, got %v", err)
	}
}

func TestBackTestMoveRejectsOwnershipTimeframeMismatch(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	ts := int64(1704067200000)
	openSignal := testOpenSignal(ts, 100, 95)
	openSignal.Strategy = "turtle"
	openSignal.Timeframe = "1h"
	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketData(ts, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	moveSignal := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Action:    16,
		HighSide:  1,
		TP:        0,
		SL:        96,
		Strategy:  "turtle",
	}
	_, err := r.EvaluateUpdate(moveSignal, models.Position{}, testMarketData(ts+3600*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("expected non-owner update to pass, got %v", err)
	}
}

func TestBackTestListSignalsByPairFillsHasPositionWhenOpen(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	cached := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
		Action:    0,
		HighSide:  1,
	}
	r.signalCache.Upsert(cached)
	r.positions[pairKey("okx", "BTC/USDT")] = &backTestPosition{
		Exchange:     "okx",
		Symbol:       "BTC/USDT",
		Timeframe:    "1h",
		Side:         positionSideLong,
		RemainingQty: 1,
	}

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

func TestBackTestListSignalsByPairFiltersInactiveStrategies(t *testing.T) {
	r := NewBackTest(BackTestConfig{
		ActiveStrategies: []string{"turtle"},
	})
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

func TestResolveSignalTimestampPrefersTriggerTimestamp(t *testing.T) {
	const (
		klineTS   = int64(1_700_000_000_000)
		futureTS  = 1_700_000_300_000
		earlierTS = 1_699_999_900_000
	)
	if got := resolveSignalTimestamp(klineTS, futureTS); got != int64(futureTS) {
		t.Fatalf("expected future trigger kept %d, got %d", futureTS, got)
	}
	if got := resolveSignalTimestamp(klineTS, earlierTS); got != earlierTS {
		t.Fatalf("expected earlier trigger kept %d, got %d", earlierTS, got)
	}
	if got := resolveSignalTimestamp(klineTS, 0); got != klineTS {
		t.Fatalf("expected zero trigger to fallback to kline ts %d, got %d", klineTS, got)
	}
}

func TestBackTestCloseAllUsesTriggerTimestamp(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	openTS := int64(1_704_067_200_000)
	openSignal := testOpenSignal(openTS, 100, 95)
	openSignal.StrategyTimeframes = []string{"3m", "15m", "1h"}
	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	triggerTS := openTS + 15*60*1000
	closeSignal := models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.2",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             64,
		HighSide:           1,
		Exit:               101,
		TriggerTimestamp:   int(triggerTS),
		TrendingTimestamp:  int(openTS),
	}

	decision, err := r.EvaluateUpdate(closeSignal, models.Position{}, testMarketDataTimeframe("15m", openTS+12*60*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("close all failed: %v", err)
	}
	if decision.EventTS != triggerTS {
		t.Fatalf("expected decision event ts %d, got %d", triggerTS, decision.EventTS)
	}
	if positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h"); err != nil {
		t.Fatalf("list open positions failed: %v", err)
	} else if len(positions) != 0 {
		t.Fatalf("expected position closed, got %d open positions", len(positions))
	}
	if len(r.trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(r.trades))
	}
	if r.trades[0].ExitTS != triggerTS {
		t.Fatalf("expected trade exit ts %d, got %d", triggerTS, r.trades[0].ExitTS)
	}
	if r.trades[0].CloseReason != "signal_close_all" {
		t.Fatalf("expected signal_close_all, got %s", r.trades[0].CloseReason)
	}

	signals, err := r.ListSignalsByPair("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("list signals failed: %v", err)
	}
	if len(signals) != 1 {
		t.Fatalf("expected 1 cached signal after close, got %d", len(signals))
	}
	if signals[0].TriggerTimestamp != int(triggerTS) {
		t.Fatalf("expected cached signal trigger ts updated to close ts %d, got %d", triggerTS, signals[0].TriggerTimestamp)
	}
}

func TestBackTestCloseAllWithTrendGoneRemovesSignalFromCache(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	openTS := int64(1_704_067_200_000)
	openSignal := testOpenSignal(openTS, 100, 95)
	openSignal.Strategy = "turtle"
	openSignal.StrategyVersion = "v0.0.7"
	openSignal.StrategyTimeframes = []string{"3m", "15m", "1h"}
	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	triggerTS := openTS + 15*60*1000
	closeSignal := models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.7",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             64,
		HighSide:           0,
		MidSide:            0,
		TriggerTimestamp:   int(triggerTS),
	}

	decision, err := r.EvaluateUpdate(closeSignal, models.Position{}, testMarketDataTimeframe("15m", openTS+12*60*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("close all failed: %v", err)
	}
	if decision.Action != models.DecisionActionClose {
		t.Fatalf("expected close decision, got %s", decision.Action)
	}
	if signals, err := r.ListSignalsByPair("okx", "BTC/USDT"); err != nil {
		t.Fatalf("list signals failed: %v", err)
	} else if len(signals) != 0 {
		t.Fatalf("expected signal removed from cache after trend-ended close-all, got %d", len(signals))
	}
}

func TestBackTestPartialCloseUsesEightyPercentSize(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	openTS := int64(1_704_067_200_000)
	openSignal := testOpenSignal(openTS, 100, 95)
	openSignal.StrategyTimeframes = []string{"3m", "15m", "1h"}
	if _, err := r.EvaluateOpenBatch([]models.Signal{openSignal}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	triggerTS := openTS + 15*60*1000
	closeSignal := models.Signal{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyVersion:    "v0.0.2",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             32,
		HighSide:           1,
		TP:                 110,
		SL:                 98,
		TriggerTimestamp:   int(triggerTS),
		TrendingTimestamp:  int(openTS),
	}

	decision, err := r.EvaluateUpdate(closeSignal, models.Position{}, testMarketDataTimeframe("15m", openTS+12*60*1000, 101, 99, 101))
	if err != nil {
		t.Fatalf("partial close failed: %v", err)
	}
	if decision.Size != 0.008 {
		t.Fatalf("expected partial close size 0.008, got %.8f", decision.Size)
	}
	positions, err := r.ListOpenPositions("okx", "BTC/USDT", "1h")
	if err != nil {
		t.Fatalf("list open positions failed: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("expected one remaining position, got %d", len(positions))
	}
	pos := r.positions[pairKey("okx", "BTC/USDT")]
	if pos == nil {
		t.Fatalf("expected runtime position to remain after partial close")
	}
	if math.Abs(pos.RemainingQty-0.002) > 1e-9 {
		t.Fatalf("expected remaining quantity 0.002, got %.8f", pos.RemainingQty)
	}
}

func testOpenSignal(ts int64, entry float64, sl float64) models.Signal {
	return models.Signal{
		Exchange:          "okx",
		Symbol:            "BTC/USDT",
		Timeframe:         "1h",
		Entry:             entry,
		SL:                sl,
		TP:                0,
		Action:            8,
		HighSide:          1,
		Strategy:          "turtle",
		StrategyVersion:   "v0.0.2",
		TriggerTimestamp:  int(ts),
		TrendingTimestamp: int(ts),
	}
}

func testMarketData(ts int64, high float64, low float64, close float64) models.MarketData {
	return testMarketDataTimeframe("1h", ts, high, low, close)
}

func testMarketDataTimeframe(timeframe string, ts int64, high float64, low float64, close float64) models.MarketData {
	return models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: timeframe,
		Closed:    true,
		Source:    "back-test-exchange",
		OHLCV: models.OHLCV{
			TS:    ts,
			Open:  close,
			High:  high,
			Low:   low,
			Close: close,
		},
	}
}

func groupedTrendGuardEvalContextForPair(symbol string, midCloses []float64) models.RiskEvalContext {
	const midTimeframe = "5m"
	const primaryTimeframe = "30m"
	bars := make([]models.OHLCV, 0, len(midCloses))
	baseTS := int64(1_773_840_000_000)
	for idx, closePrice := range midCloses {
		barTS := baseTS + int64(idx)*5*60*1000
		bars = append(bars, models.OHLCV{
			TS:     barTS,
			Open:   closePrice - 1,
			High:   closePrice + 1,
			Low:    closePrice - 2,
			Close:  closePrice,
			Volume: 1,
		})
	}
	primaryBars := []models.OHLCV{}
	for idx := 0; idx < len(bars); idx += 6 {
		bar := bars[idx]
		primaryBars = append(primaryBars, models.OHLCV{
			TS:     bar.TS,
			Open:   bar.Open,
			High:   bar.High,
			Low:    bar.Low,
			Close:  bar.Close,
			Volume: bar.Volume,
		})
	}
	if len(primaryBars) == 0 {
		primaryBars = append(primaryBars, models.OHLCV{
			TS:     baseTS,
			Open:   100,
			High:   101,
			Low:    99,
			Close:  100,
			Volume: 1,
		})
	}
	return models.RiskEvalContext{
		MarketData: models.MarketData{
			Exchange:  "okx",
			Symbol:    symbol,
			Timeframe: primaryTimeframe,
			OHLCV: models.OHLCV{
				TS:    bars[len(bars)-1].TS,
				Close: bars[len(bars)-1].Close,
			},
			Closed: true,
		},
		Snapshot: &models.MarketSnapshot{
			Exchange:       "okx",
			Symbol:         symbol,
			EventTimeframe: primaryTimeframe,
			EventTS:        bars[len(bars)-1].TS,
			EventClosed:    true,
			Series: map[string][]models.OHLCV{
				midTimeframe:     bars,
				primaryTimeframe: primaryBars,
			},
		},
	}
}

func TestBackTestApplySignalLifecycle_ActionZeroButNonEmptyDoesNotClear(t *testing.T) {
	r := &BackTest{
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
	if _, changed, err := r.applySignalLifecycleLocked(next, models.Position{}, false, 1700000000000); err != nil {
		t.Fatalf("applySignalLifecycleLocked returned error: %v", err)
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

func TestBackTestApplySignalLifecycle_RoutingOnlySignalClearsCache(t *testing.T) {
	r := &BackTest{
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
	if _, changed, err := r.applySignalLifecycleLocked(cleared, models.Position{}, false, 1700000000001); err != nil {
		t.Fatalf("applySignalLifecycleLocked returned error: %v", err)
	} else if !changed {
		t.Fatalf("expected routing-only signal to clear cached signal")
	}

	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected signal removed from cache")
	}
}

func TestMarkSignalClosedByPnLLocked_ClearsEntryWatchTimestamp(t *testing.T) {
	r := &BackTest{
		signalCache: NewSignalCache(),
	}
	r.signalCache.Upsert(models.Signal{
		Exchange:                        "okx",
		Symbol:                          "BTC/USDT",
		Timeframe:                       "1h",
		Strategy:                        "turtle",
		Action:                          8,
		HighSide:                        1,
		HasPosition:                     models.SignalHasOpenPosition,
		EntryWatchTimestamp:             45678,
		PostHighPullbackFirstEntryState: models.SignalPostHighPullbackFirstEntryArmed,
		Entry:                           100,
		SL:                              95,
		TP:                              120,
		Plan1LastProfitLockMFER:         1.8,
		Plan1LastProfitLockHighBucketTS: 1700000000000,
		Plan1LastProfitLockStructPrice:  98,
	})

	err := r.markSignalClosedByPnLLocked(&backTestPosition{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "turtle",
	}, 10, 1700000000000)
	if err != nil {
		t.Fatalf("markSignalClosedByPnLLocked failed: %v", err)
	}

	cached, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle")
	if !ok {
		t.Fatalf("expected signal to remain cached")
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
	if cached.HasPosition != models.SignalHasClosedProfit {
		t.Fatalf("expected closed profit state, got %d", cached.HasPosition)
	}
}

func TestBackTestApplySignalLifecycle_IgnoresInactiveStrategyNonEmptySignal(t *testing.T) {
	r := NewBackTest(BackTestConfig{
		ActiveStrategies: []string{"turtle"},
	})
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
	if _, changed, err := r.applySignalLifecycleLocked(signal, models.Position{}, false, 1700000000002); err != nil {
		t.Fatalf("applySignalLifecycleLocked returned error: %v", err)
	} else if changed {
		t.Fatalf("expected inactive non-empty signal to be ignored")
	}
	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected inactive signal not cached")
	}
}

func TestBackTestApplySignalLifecycle_AllowsInactiveStrategyClearSignal(t *testing.T) {
	r := NewBackTest(BackTestConfig{
		ActiveStrategies: []string{"turtle"},
	})
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
	if _, changed, err := r.applySignalLifecycleLocked(cleared, models.Position{}, false, 1700000000003); err != nil {
		t.Fatalf("applySignalLifecycleLocked returned error: %v", err)
	} else if !changed {
		t.Fatalf("expected cleared signal to remove stale cache")
	}
	if _, ok := r.signalCache.Find("okx", "BTC/USDT", "1h", "turtle"); ok {
		t.Fatalf("expected cleared stale signal removed from cache")
	}
}

func TestBackTestSignalCacheAllowsDistinctComboKeysForSamePair(t *testing.T) {
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

func TestBackTestOpenRejectsExposureAcrossStableQuoteSymbols(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	openTS := int64(1704067200000)
	if _, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             8,
		HighSide:           1,
		Entry:              100,
		SL:                 95,
		TriggerTimestamp:   int(openTS),
		TrendingTimestamp:  int(openTS),
	}}, testMarketDataTimeframe("1h", openTS, 100, 100, 100)); err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	_, err := r.EvaluateOpenBatch([]models.Signal{{
		Exchange:           "okx",
		Symbol:             "BTC/USDC",
		Timeframe:          "1h",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		Action:             8,
		HighSide:           1,
		Entry:              100,
		SL:                 95,
		TriggerTimestamp:   int(openTS + 3600*1000),
		TrendingTimestamp:  int(openTS),
	}}, models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDC",
		Timeframe: "1h",
		Closed:    true,
		Source:    "back-test-exchange",
		OHLCV:     models.OHLCV{TS: openTS + 3600*1000, Open: 100, High: 100, Low: 100, Close: 100},
	})
	if err == nil || !strings.Contains(err.Error(), "already occupied") {
		t.Fatalf("expected exposure conflict error, got %v", err)
	}
}
