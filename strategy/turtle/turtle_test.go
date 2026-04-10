package turtle

import (
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestGetOpenSignalOnBreakout(t *testing.T) {
	strategy := &Strategy{}
	bars := buildTrendBars(30, 100, 0.4)
	last := len(bars) - 1
	prevHigh := maxHigh(bars[:last])
	bars[last].Open = prevHigh + 1
	bars[last].Close = prevHigh + 4
	bars[last].High = bars[last].Close + 1
	bars[last].Low = bars[last].Open - 1

	signals := strategy.Get(buildSnapshot("1h", bars))
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	got := signals[0]
	if got.Action != actionOpen {
		t.Fatalf("expected action=%d, got %d", actionOpen, got.Action)
	}
	if got.HighSide != trendSideLong {
		t.Fatalf("expected high_side=%d, got %d", trendSideLong, got.HighSide)
	}
	if got.Timeframe != "1h" {
		t.Fatalf("expected timeframe=1h, got %s", got.Timeframe)
	}
	if got.SL <= 0 || got.SL >= got.Entry {
		t.Fatalf("expected 0 < sl < entry, got sl=%.6f entry=%.6f", got.SL, got.Entry)
	}
	if len(got.StrategyTimeframes) != 1 || got.StrategyTimeframes[0] != "1h" {
		t.Fatalf("unexpected strategy_timeframes: %#v", got.StrategyTimeframes)
	}
	if len(got.StrategyIndicators["donchian"]) != 2 || got.StrategyIndicators["donchian"][0] != "20" || got.StrategyIndicators["donchian"][1] != "10" {
		t.Fatalf("unexpected strategy_indicators.donchian: %#v", got.StrategyIndicators)
	}
	if len(got.StrategyIndicators["atr"]) != 1 || got.StrategyIndicators["atr"][0] != "20" {
		t.Fatalf("unexpected strategy_indicators.atr: %#v", got.StrategyIndicators)
	}
}

func TestUpdateCloseSignalOnExitBreakdown(t *testing.T) {
	strategy := &Strategy{}
	bars := buildTrendBars(30, 100, 0.6)
	last := len(bars) - 1
	prevLow := minLow(bars[last-10 : last])
	bars[last].Open = prevLow - 1
	bars[last].Close = prevLow - 3
	bars[last].High = bars[last].Open + 1
	bars[last].Low = bars[last].Close - 1

	current := models.Signal{
		Timeframe:         "1h",
		Action:            actionOpen,
		HighSide:          trendSideLong,
		MidSide:           midSideLong,
		Entry:             bars[last-1].Close,
		SL:                bars[last-1].Close - 5,
		TrendingTimestamp: int(bars[last-1].TS),
		TriggerTimestamp:  int(bars[last-1].TS),
	}
	next, ok := strategy.Update("turtle", current, buildSnapshot("1h", bars))
	if !ok {
		t.Fatalf("expected updated signal")
	}
	if next.Action != actionCloseAll {
		t.Fatalf("expected action=%d, got %d", actionCloseAll, next.Action)
	}
	if next.Exit != bars[last].Close {
		t.Fatalf("expected exit=%.6f, got %.6f", bars[last].Close, next.Exit)
	}
}

func TestUpdateClearsInactiveSignalWhenNoBreakout(t *testing.T) {
	strategy := &Strategy{}
	bars := buildFlatBars(30, 100)
	current := models.Signal{
		Timeframe: "1h",
		Action:    0,
		HighSide:  trendSideLong,
		MidSide:   midSideLong,
	}
	next, ok := strategy.Update("turtle", current, buildSnapshot("1h", bars))
	if !ok {
		t.Fatalf("expected updated signal")
	}
	if next.Action != 0 {
		t.Fatalf("expected cleared action 0, got %d", next.Action)
	}
	if next.HighSide != trendSideNone {
		t.Fatalf("expected cleared high_side=%d, got %d", trendSideNone, next.HighSide)
	}
	if next.MidSide != midSideNone {
		t.Fatalf("expected cleared mid_side=%d, got %d", midSideNone, next.MidSide)
	}
}

func TestUpdateMoveSignalWhenStopCanTrail(t *testing.T) {
	strategy := &Strategy{}
	bars := buildTrendBars(30, 100, 1.0)
	last := len(bars) - 1
	current := models.Signal{
		Timeframe:         "1h",
		Action:            actionOpen,
		HighSide:          trendSideLong,
		MidSide:           midSideLong,
		Entry:             bars[last-5].Close,
		SL:                bars[last-5].Close - 10,
		TrendingTimestamp: int(bars[last-5].TS),
		TriggerTimestamp:  int(bars[last-5].TS),
	}

	next, ok := strategy.Update("turtle", current, buildSnapshot("1h", bars))
	if !ok {
		t.Fatalf("expected updated signal")
	}
	if next.Action != actionMove {
		t.Fatalf("expected action=%d, got %d", actionMove, next.Action)
	}
	if next.SL <= current.SL {
		t.Fatalf("expected sl increase: old=%.6f new=%.6f", current.SL, next.SL)
	}
}

func buildSnapshot(timeframe string, bars []models.OHLCV) models.MarketSnapshot {
	last := len(bars) - 1
	lastTS := int64(0)
	lastIndex := -1
	if last >= 0 {
		lastTS = bars[last].TS
		lastIndex = last
	}
	return models.MarketSnapshot{
		Exchange:       "okx",
		Symbol:         "BTC/USDT",
		EventTimeframe: timeframe,
		EventTS:        lastTS,
		Series: map[string][]models.OHLCV{
			timeframe: bars,
		},
		Meta: map[string]models.SeriesMeta{
			timeframe: {
				LastClosedTS: lastTS,
				LastIndex:    lastIndex,
			},
		},
	}
}

func buildTrendBars(count int, base, step float64) []models.OHLCV {
	out := make([]models.OHLCV, 0, count)
	ts := int64(1700000000000)
	for i := 0; i < count; i++ {
		open := base + float64(i)*step
		close := open + 0.4
		high := close + 0.3
		low := open - 0.3
		out = append(out, models.OHLCV{
			TS:     ts,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  close,
			Volume: 10 + float64(i),
		})
		ts += 3600_000
	}
	return out
}

func buildFlatBars(count int, price float64) []models.OHLCV {
	out := make([]models.OHLCV, 0, count)
	ts := int64(1700000000000)
	for i := 0; i < count; i++ {
		out = append(out, models.OHLCV{
			TS:     ts,
			Open:   price,
			High:   price + 0.3,
			Low:    price - 0.3,
			Close:  price,
			Volume: 10 + float64(i),
		})
		ts += 3600_000
	}
	return out
}

func maxHigh(bars []models.OHLCV) float64 {
	if len(bars) == 0 {
		return 0
	}
	max := bars[0].High
	for i := 1; i < len(bars); i++ {
		if bars[i].High > max {
			max = bars[i].High
		}
	}
	return max
}

func minLow(bars []models.OHLCV) float64 {
	if len(bars) == 0 {
		return 0
	}
	min := bars[0].Low
	for i := 1; i < len(bars); i++ {
		if bars[i].Low < min {
			min = bars[i].Low
		}
	}
	return min
}
