package risk

import (
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestTrendGuardTimeframeDuration(t *testing.T) {
	cases := []struct {
		timeframe string
		want      time.Duration
		ok        bool
	}{
		{timeframe: "15m", want: 15 * time.Minute, ok: true},
		{timeframe: "1h", want: time.Hour, ok: true},
		{timeframe: "1d", want: 24 * time.Hour, ok: true},
		{timeframe: "1w", want: 7 * 24 * time.Hour, ok: true},
		{timeframe: "0h", want: 0, ok: false},
		{timeframe: "abc", want: 0, ok: false},
	}
	for _, tc := range cases {
		got, ok := trendGuardTimeframeDuration(tc.timeframe)
		if ok != tc.ok {
			t.Fatalf("timeframe %s parse ok=%v, want %v", tc.timeframe, ok, tc.ok)
		}
		if ok && got != tc.want {
			t.Fatalf("timeframe %s duration=%s, want %s", tc.timeframe, got, tc.want)
		}
	}
}

func TestMatchTrendGuardMatched(t *testing.T) {
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		MaxStartLagBars: 12,
	}
	candidate := trendGuardContext{
		Exchange:   "okx",
		Symbol:     "ETH/USDT",
		Strategy:   "turtle",
		Timeframe:  "15m",
		HighSide:   1,
		TrendingTS: 1_700_000_000_000,
	}
	existing := trendGuardContext{
		Exchange:   "okx",
		Symbol:     "BTC/USDT",
		Strategy:   "turtle",
		Timeframe:  "15m",
		HighSide:   1,
		TrendingTS: candidate.TrendingTS - int64(6*(15*time.Minute/time.Millisecond)),
	}

	matched, _ := matchTrendGuard(candidate, existing, cfg)
	if !matched {
		t.Fatalf("expected trend guard matched")
	}
}

func TestLiveEvaluateOpenRejectsSameTrendByTrendGuard(t *testing.T) {
	ts := int64(1_700_000_000_000)
	key := livePositionKey("okx", "BTC-USDT-SWAP", "long", models.MarginModeIsolated)
	r := &Live{
		cfg: defaultRiskConfig(),
		positions: map[string]models.Position{
			key: {
				Exchange:      "okx",
				Symbol:        "BTC/USDT",
				Timeframe:     "15m",
				PositionSide:  "long",
				StrategyName:  "turtle",
				Status:        models.PositionStatusOpen,
				EntryQuantity: 1,
			},
		},
		openPositions: map[string]models.RiskOpenPosition{
			key: {
				Exchange: "okx",
				Symbol:   "BTC/USDT",
				InstID:   "BTC-USDT-SWAP",
				PosSide:  "long",
				MgnMode:  models.MarginModeIsolated,
				Pos:      "1",
				RowJSON: models.MarshalPositionRowEnvelope(nil, models.StrategyContextMeta{
					StrategyName:       "turtle",
					StrategyVersion:    "v0.0.5",
					TrendingTimestamp:  int(ts),
					StrategyTimeframes: []string{"15m"},
				}),
			},
		},
	}
	signal := models.Signal{
		Exchange:          "okx",
		Symbol:            "ETH/USDT",
		Timeframe:         "15m",
		Strategy:          "turtle",
		HighSide:          1,
		TrendingTimestamp: int(ts + int64(6*(15*time.Minute/time.Millisecond))),
	}
	decision, err := r.evaluateOpen("okx", "ETH/USDT", "15m", signal, models.Position{}, false, models.MarketData{})
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected trend guard reject error, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: %s", decision.Action)
	}
}

func TestBackTestOpenRejectsSameTrendByTrendGuard(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	baseTS := int64(1_700_000_000_000)
	r.positions[pairKey("okx", "BTC/USDT")] = &backTestPosition{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		Timeframe:     "1h",
		Side:          positionSideLong,
		Strategy:      "turtle",
		EntryPrice:    100,
		EntryQuantity: 1,
		RemainingQty:  1,
		Margin:        100,
		Leverage:      2,
		EntryTS:       baseTS,
	}

	signal := testOpenSignal(baseTS+6*int64(time.Hour/time.Millisecond), 100, 95)
	signal.Symbol = "ETH/USDT"
	decision, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketData(baseTS, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected trend guard reject error, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: %s", decision.Action)
	}
}

func TestBackTestOpenRejectsSameTrendByTrendGuardInGroupedMode(t *testing.T) {
	r := NewBackTest(BackTestConfig{})
	r.cfg.TrendGuard.Mode = trendGuardModeGrouped
	baseTS := int64(1_700_000_000_000)
	r.positions[pairKey("okx", "BTC/USDT")] = &backTestPosition{
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		Timeframe:     "1h",
		Side:          positionSideLong,
		Strategy:      "turtle",
		EntryPrice:    100,
		EntryQuantity: 1,
		RemainingQty:  1,
		Margin:        100,
		Leverage:      2,
		EntryTS:       baseTS,
	}

	signal := testOpenSignal(baseTS+6*int64(time.Hour/time.Millisecond), 100, 95)
	signal.Symbol = "ETH/USDT"
	decision, err := r.EvaluateOpenBatch([]models.Signal{signal}, testMarketData(baseTS, 100, 100, 100))
	if err == nil || !strings.Contains(err.Error(), "trend guard rejected open") {
		t.Fatalf("expected grouped mode to fall back to legacy rejection, got: %v", err)
	}
	if decision.Action != models.DecisionActionIgnore {
		t.Fatalf("unexpected decision action: %s", decision.Action)
	}
}
