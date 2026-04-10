package core

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestBackTestOnOHLCV_GetSignalRecordsEvent(t *testing.T) {
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			return []models.Signal{{
				Exchange:  "okx",
				Symbol:    "BTC/USDT",
				Timeframe: "1h",
				Strategy:  "s1",
				Action:    8,
				HighSide:  1,
				Entry:     100,
				SL:        95,
			}}
		},
	}
	risk := &liveMockRisk{}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	report := bt.Finalize()
	if report.TotalEvents == 0 {
		t.Fatalf("expected back-test get event recorded")
	}
}

func TestBackTestOnOHLCV_StrategyCombosRouteSnapshotsByConfiguredCombo(t *testing.T) {
	var got [][]string
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			keys := make([]string, 0, len(snapshot.Series))
			for timeframe := range snapshot.Series {
				keys = append(keys, timeframe)
			}
			sort.Strings(keys)
			got = append(got, keys)
			return nil
		},
	}
	risk := &liveMockRisk{}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{{
			Name:       "okx",
			Active:     true,
			Timeframes: `["3m","15m","1h","4h","1d"]`,
		}},
		symbols: []models.Symbol{{
			Exchange:   "okx",
			Symbol:     "BTC/USDT",
			Timeframes: "",
			Active:     true,
		}},
	}
	bt := NewBackTest(BackTestConfig{
		Strategy:   strategy,
		OHLCVStore: store,
		StrategyCombos: []models.StrategyComboConfig{
			{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true},
			{Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false},
		},
		Risk: risk,
	})

	base := int64(1700000000000)
	preload := []models.MarketData{
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "15m", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 101, Low: 99, Close: 100}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1h", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 102, Low: 99, Close: 101}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "4h", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 103, Low: 98, Close: 102}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1d", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 104, Low: 97, Close: 103}},
	}
	for _, item := range preload {
		bt.PreloadOHLCV(item)
	}

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(3*time.Minute/time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100},
	})
	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(1*time.Hour/time.Millisecond), Open: 101, High: 103, Low: 100, Close: 102},
	})

	if len(got) != 2 {
		t.Fatalf("expected two combo evaluations, got %d (%v)", len(got), got)
	}
	if strings.Join(got[0], "/") != "15m/1h/3m" {
		t.Fatalf("unexpected first combo snapshot: got %v", got[0])
	}
	if strings.Join(got[1], "/") != "1d/1h/4h" {
		t.Fatalf("unexpected second combo snapshot: got %v", got[1])
	}
}

func TestBackTestOnOHLCV_WithPositionUsesUpdate(t *testing.T) {
	current := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "s1",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	strategy := &liveMockStrategy{
		updateFn: func(_ string, _ models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			next := current
			next.Action = 16
			next.SL = 98
			return next, true
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
		positions: []models.Position{{
			Exchange:      "okx",
			Symbol:        "BTC/USDT",
			Timeframe:     "1h",
			Status:        models.PositionStatusOpen,
			PositionSide:  "long",
			EntryQuantity: 1,
		}},
	}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 2000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.updateCalls == 0 {
		t.Fatalf("expected EvaluateUpdate called")
	}
}

func TestBackTestOnOHLCV_NoPositionWithCachedSignalUsesUpdate(t *testing.T) {
	current := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "s1",
		Action:    0,
		HighSide:  1,
		MidSide:   1,
	}
	getCalls := 0
	updateCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return nil
		},
		updateFn: func(_ string, signal models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			updateCalls++
			return signal, true
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
	}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 2500, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if updateCalls != 1 {
		t.Fatalf("expected Strategy.Update called once, got %d", updateCalls)
	}
	if getCalls != 0 {
		t.Fatalf("expected Get not called when cached signal exists, got %d", getCalls)
	}
	if risk.updateCalls == 0 {
		t.Fatalf("expected EvaluateUpdate called")
	}
}

func TestBackTestOnOHLCV_SnapshotIncludesNewlyClosedHigherTimeframe(t *testing.T) {
	var seen15mLastClosed int64
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			getCalls++
			if meta, ok := snapshot.Meta["15m"]; ok {
				seen15mLastClosed = meta.LastClosedTS
			}
			return nil
		},
	}
	risk := &liveMockRisk{}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})
	bt.core.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	base := int64(900000)
	for i := 0; i < 5; i++ {
		ts := base + int64(i)*int64(3*time.Minute/time.Millisecond)
		bt.OnOHLCV(models.MarketData{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "3m",
			Closed:    true,
			OHLCV:     models.OHLCV{TS: ts, Open: 100, High: 101, Low: 99, Close: 100},
		})
	}

	if getCalls != 5 {
		t.Fatalf("expected strategy evaluation on each 3m close, got %d", getCalls)
	}
	if seen15mLastClosed != base {
		t.Fatalf("expected 15m last closed ts=%d, got %d", base, seen15mLastClosed)
	}
}

func TestBackTestOnOHLCV_OpenEvaluationUsesRiskEvalContextSnapshot(t *testing.T) {
	base := int64(900000)
	targetTS := base + 4*int64(3*time.Minute/time.Millisecond)
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			if snapshot.EventTS != targetTS {
				return nil
			}
			return []models.Signal{{
				Exchange:           snapshot.Exchange,
				Symbol:             snapshot.Symbol,
				Timeframe:          "1h",
				Strategy:           "turtle",
				StrategyVersion:    "v0.0.7",
				StrategyTimeframes: []string{"3m", "15m", "1h"},
				Action:             8,
				HighSide:           1,
				Entry:              100,
				SL:                 95,
				TriggerTimestamp:   int(targetTS),
				TrendingTimestamp:  int(base),
			}}
		},
	}
	risk := &liveMockRisk{}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})
	bt.core.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	for i := 0; i < 5; i++ {
		ts := base + int64(i)*int64(3*time.Minute/time.Millisecond)
		bt.OnOHLCV(models.MarketData{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "3m",
			Closed:    true,
			OHLCV:     models.OHLCV{TS: ts, Open: 100, High: 101, Low: 99, Close: 100},
		})
	}

	if risk.openCalls != 1 {
		t.Fatalf("expected one open evaluation, got %d", risk.openCalls)
	}
	if risk.lastOpenCtx.Snapshot == nil {
		t.Fatalf("expected back-test open evaluation to receive snapshot")
	}
	if risk.lastOpenCtx.MarketData.OHLCV.TS != targetTS {
		t.Fatalf("expected market data ts %d, got %d", targetTS, risk.lastOpenCtx.MarketData.OHLCV.TS)
	}
	if meta, ok := risk.lastOpenCtx.Snapshot.Meta["15m"]; !ok {
		t.Fatalf("expected snapshot meta for 15m timeframe")
	} else if meta.LastClosedTS != base {
		t.Fatalf("expected 15m last closed ts=%d, got %d", base, meta.LastClosedTS)
	}
}

func TestBackTestOnOHLCV_StrategyCombosClearsRemovedComboSignalsWithoutTriggeredCombo(t *testing.T) {
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {{
				Exchange:           "okx",
				Symbol:             "BTC/USDT",
				Timeframe:          "30m",
				Strategy:           "turtle",
				StrategyVersion:    "v0.0.6d",
				StrategyTimeframes: []string{"1m", "5m", "30m"},
				ComboKey:           "1m/5m/30m",
				Action:             4,
				HighSide:           -1,
			}},
		},
	}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{{
			Name:       "okx",
			Active:     true,
			Timeframes: `["3m","15m","1h","4h","1d"]`,
		}},
		symbols: []models.Symbol{{
			Exchange:   "okx",
			Symbol:     "BTC/USDT",
			Timeframes: "",
			Active:     true,
		}},
	}
	bt := NewBackTest(BackTestConfig{
		Strategy:   &liveMockStrategy{},
		OHLCVStore: store,
		Risk:       risk,
		StrategyCombos: []models.StrategyComboConfig{
			{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true},
			{Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false},
		},
	})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 2600, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.updateCalls == 0 {
		t.Fatalf("expected removed combo signal cleanup to trigger EvaluateUpdate")
	}
	if risk.lastUpdate.ComboKey != "1m/5m/30m" {
		t.Fatalf("expected removed combo key cleared, got %q", risk.lastUpdate.ComboKey)
	}
	if risk.lastUpdate.Action != 0 || risk.lastUpdate.HighSide != 0 {
		t.Fatalf("expected removed combo signal cleared, got %+v", risk.lastUpdate)
	}
}

func TestBackTestOnOHLCV_ClosedOutcomeSignalSkipStaleCleanup(t *testing.T) {
	updateCalls := 0
	strategy := &liveMockStrategy{
		updateFn: func(_ string, current models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			updateCalls++
			return current, false
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {{
				Exchange:    "okx",
				Symbol:      "BTC/USDT",
				Timeframe:   "1h",
				Strategy:    "s1",
				Action:      0,
				HighSide:    1,
				MidSide:     1,
				HasPosition: models.SignalHasClosedProfit,
			}},
		},
	}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 3000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if updateCalls != 1 {
		t.Fatalf("expected closed outcome signal still routed to Strategy.Update, got %d", updateCalls)
	}
	if risk.updateCalls != 0 {
		t.Fatalf("expected no EvaluateUpdate call when strategy returns unchanged closed outcome signal, got %d", risk.updateCalls)
	}
}

func TestBackTestOnOHLCV_ClosedOutcomeSignalProcessesNonEmptyActionZeroGet(t *testing.T) {
	updateCalls := 0
	strategy := &liveMockStrategy{
		updateFn: func(_ string, current models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			updateCalls++
			next := current
			next.HighSide = -1
			next.MidSide = -1
			return next, true
		},
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			return []models.Signal{{
				Exchange:  "okx",
				Symbol:    "BTC/USDT",
				Timeframe: "1h",
				Strategy:  "s1",
				Action:    0,
				HighSide:  -1,
				MidSide:   -1,
			}}
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {{
				Exchange:    "okx",
				Symbol:      "BTC/USDT",
				Timeframe:   "1h",
				Strategy:    "s1",
				Action:      0,
				HighSide:    1,
				MidSide:     1,
				HasPosition: models.SignalHasClosedLoss,
			}},
		},
	}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 4000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if updateCalls != 1 {
		t.Fatalf("expected Strategy.Update called once for closed outcome signal, got %d", updateCalls)
	}
	if risk.updateCalls == 0 {
		t.Fatalf("expected EvaluateUpdate call for non-empty updated signal from closed outcome cache")
	}
}

func TestBackTestOnOHLCV_OnlySmallestTimeframeDrivesRiskAndStrategy(t *testing.T) {
	base := int64(1700000000000)
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return nil
		},
	}
	risk := &liveMockRisk{}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})
	bt.core.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	bt.PreloadOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base, Open: 100, High: 101, Low: 99, Close: 100},
	})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(15*time.Minute/time.Millisecond), Open: 100, High: 102, Low: 99, Close: 101},
	})
	if risk.marketDataCalls != 1 {
		t.Fatalf("expected closed market data to reach risk before strategy gating, got %d", risk.marketDataCalls)
	}
	if getCalls != 0 {
		t.Fatalf("expected non-smallest timeframe to skip strategy evaluation, got %d", getCalls)
	}

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    false,
		OHLCV:     models.OHLCV{TS: base + int64(18*time.Minute/time.Millisecond), Open: 101, High: 103, Low: 100, Close: 102},
	})
	if risk.marketDataCalls != 1 {
		t.Fatalf("expected unclosed smallest timeframe to skip risk market processing, got %d", risk.marketDataCalls)
	}
	if getCalls != 0 {
		t.Fatalf("expected smallest timeframe intrabar to skip closed-bar strategy evaluation, got %d", getCalls)
	}

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(21*time.Minute/time.Millisecond), Open: 102, High: 104, Low: 101, Close: 103},
	})
	if risk.marketDataCalls != 2 {
		t.Fatalf("expected smallest closed timeframe to hit risk market processing, got %d", risk.marketDataCalls)
	}
	if getCalls != 1 {
		t.Fatalf("expected smallest closed timeframe to trigger one strategy evaluation, got %d", getCalls)
	}
}

func TestBackTestOnOHLCV_CachedSignalUpdateFalseRefreshesTrendGuardCandidate(t *testing.T) {
	current := models.Signal{
		Exchange:          "okx",
		Symbol:            "BTC/USDT",
		Timeframe:         "1h",
		Strategy:          "s1",
		Action:            0,
		HighSide:          1,
		TrendingTimestamp: 12345,
	}
	strategy := &liveMockStrategy{
		updateFn: func(_ string, signal models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			return signal, false
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
	}
	bt := NewBackTest(BackTestConfig{Strategy: strategy, Risk: risk})

	bt.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 4000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.refreshCalls != 1 {
		t.Fatalf("expected grouped refresh called once on unchanged update, got %d", risk.refreshCalls)
	}
	if risk.updateCalls != 0 {
		t.Fatalf("expected EvaluateUpdate not called when update=false, got %d", risk.updateCalls)
	}
	if risk.lastRefresh.Symbol != "BTC/USDT" || risk.lastRefresh.Strategy != "s1" {
		t.Fatalf("unexpected refreshed signal: %+v", risk.lastRefresh)
	}
}

type backTestMockExecutor struct {
	calls int
}

func (m *backTestMockExecutor) Start(_ context.Context) error { return nil }
func (m *backTestMockExecutor) Close() error                  { return nil }
func (m *backTestMockExecutor) Place(_ models.Decision) error {
	m.calls++
	return nil
}
