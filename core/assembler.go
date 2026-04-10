package core

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type assembledState struct {
	bucketStart int64
	bars        map[int64]models.OHLCV

	hasEmitted  bool
	emittedBar  models.OHLCV
	emittedDone bool
}

type timeframeAssembler struct {
	mu    sync.Mutex
	state map[string]*assembledState
}

func newTimeframeAssembler() *timeframeAssembler {
	return &timeframeAssembler{
		state: make(map[string]*assembledState),
	}
}

func (a *timeframeAssembler) On1m(data models.MarketData, targetTimeframes []string) []models.MarketData {
	return a.OnTimeframe(data, targetTimeframes)
}

func (a *timeframeAssembler) OnTimeframe(data models.MarketData, targetTimeframes []string) []models.MarketData {
	if a == nil {
		return nil
	}
	baseTimeframe := strings.ToLower(strings.TrimSpace(data.Timeframe))
	if baseTimeframe == "" {
		return nil
	}
	baseDur, ok := market.TimeframeDuration(baseTimeframe)
	if !ok || baseDur <= 0 {
		return nil
	}
	timeframes := normalizeAssembleTargets(targetTimeframes, baseDur)
	if len(timeframes) == 0 {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	out := make([]models.MarketData, 0, len(timeframes))
	for _, timeframe := range timeframes {
		next, assembled := a.onTimeframeForTarget(data, timeframe, baseDur)
		if !assembled {
			continue
		}
		out = append(out, next)
	}
	return out
}

func (a *timeframeAssembler) onTimeframeForTarget(data models.MarketData, timeframe string, baseDur time.Duration) (models.MarketData, bool) {
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok || dur <= 0 {
		return models.MarketData{}, false
	}
	stepMS := baseDur.Milliseconds()
	tfMS := dur.Milliseconds()
	if tfMS <= stepMS {
		return models.MarketData{}, false
	}
	ts := data.OHLCV.TS
	if ts <= 0 {
		return models.MarketData{}, false
	}
	bucketStart := (ts / tfMS) * tfMS
	key := a.stateKey(data.Exchange, data.Symbol, timeframe)
	state := a.state[key]
	if state == nil {
		state = &assembledState{
			bucketStart: bucketStart,
			bars:        make(map[int64]models.OHLCV),
		}
		a.state[key] = state
	}
	if bucketStart < state.bucketStart {
		return models.MarketData{}, false
	}
	if bucketStart > state.bucketStart {
		state.bucketStart = bucketStart
		state.bars = make(map[int64]models.OHLCV)
		state.hasEmitted = false
		state.emittedBar = models.OHLCV{}
		state.emittedDone = false
	}
	state.bars[ts] = data.OHLCV
	bar, hasBar := composeBucketOHLCV(state.bucketStart, state.bars)
	if !hasBar {
		return models.MarketData{}, false
	}
	closed := data.Closed && ts >= (state.bucketStart+tfMS-stepMS)
	if state.hasEmitted && state.emittedDone == closed && ohlcvAlmostEqual(state.emittedBar, bar) {
		return models.MarketData{}, false
	}
	state.hasEmitted = true
	state.emittedBar = bar
	state.emittedDone = closed

	return models.MarketData{
		Exchange:  data.Exchange,
		Symbol:    data.Symbol,
		Timeframe: timeframe,
		OHLCV:     bar,
		Closed:    closed,
		Source:    assembleSource(data.Source),
		Seq:       data.Seq,
	}, true
}

func (a *timeframeAssembler) stateKey(exchange, symbol, timeframe string) string {
	return normalizeExchange(exchange) + "|" + strings.TrimSpace(symbol) + "|" + strings.TrimSpace(timeframe)
}

func composeBucketOHLCV(bucketStart int64, minuteBars map[int64]models.OHLCV) (models.OHLCV, bool) {
	if len(minuteBars) == 0 {
		return models.OHLCV{}, false
	}
	keys := make([]int64, 0, len(minuteBars))
	for ts := range minuteBars {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	first, ok := minuteBars[keys[0]]
	if !ok {
		return models.OHLCV{}, false
	}
	out := models.OHLCV{
		TS:     bucketStart,
		Open:   first.Open,
		High:   first.High,
		Low:    first.Low,
		Close:  first.Close,
		Volume: 0,
	}
	for _, ts := range keys {
		item, ok := minuteBars[ts]
		if !ok {
			continue
		}
		if item.High > out.High {
			out.High = item.High
		}
		if item.Low < out.Low {
			out.Low = item.Low
		}
		out.Close = item.Close
		if !math.IsNaN(item.Volume) && !math.IsInf(item.Volume, 0) {
			out.Volume += item.Volume
		}
	}
	return out, true
}

func normalizeAssembleTargets(in []string, minDur time.Duration) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]string, 0, len(in))
	for _, item := range in {
		tf := strings.TrimSpace(item)
		if tf == "" {
			continue
		}
		dur, ok := market.TimeframeDuration(tf)
		if !ok || dur <= minDur {
			continue
		}
		if _, ok := seen[tf]; ok {
			continue
		}
		seen[tf] = struct{}{}
		out = append(out, tf)
	}
	sort.Slice(out, func(i, j int) bool {
		di, _ := market.TimeframeDuration(out[i])
		dj, _ := market.TimeframeDuration(out[j])
		if di == dj {
			return out[i] < out[j]
		}
		return di < dj
	})
	return out
}

func assembleSource(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "assembled"
	}
	return "assembled:" + raw
}

func ohlcvAlmostEqual(left, right models.OHLCV) bool {
	return left.TS == right.TS &&
		floatcmp.EQ(left.Open, right.Open) &&
		floatcmp.EQ(left.High, right.High) &&
		floatcmp.EQ(left.Low, right.Low) &&
		floatcmp.EQ(left.Close, right.Close) &&
		floatcmp.EQ(left.Volume, right.Volume)
}
