package core

import (
	"sort"
	"strings"
	"sync"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

const maxOHLCVCacheSize = 1024

type OHLCVCache struct {
	mu     sync.RWMutex
	series map[string]map[string][]models.OHLCV
	closed map[string]map[string]int64
}

func NewOHLCVCache() *OHLCVCache {
	return &OHLCVCache{
		series: make(map[string]map[string][]models.OHLCV),
		closed: make(map[string]map[string]int64),
	}
}

func (c *OHLCVCache) MaxSize() int {
	return maxOHLCVCacheSize
}

func (c *OHLCVCache) LastTS(exchange, symbol, timeframe string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pairKey(exchange, symbol)
	byTimeframe, ok := c.series[key]
	if !ok {
		return 0, false
	}
	seq := byTimeframe[timeframe]
	if len(seq) == 0 {
		return 0, false
	}
	return seq[len(seq)-1].TS, true
}

func (c *OHLCVCache) HasTS(exchange, symbol, timeframe string, ts int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pairKey(exchange, symbol)
	byTimeframe, ok := c.series[key]
	if !ok {
		return false
	}
	seq := byTimeframe[timeframe]
	if len(seq) == 0 {
		return false
	}
	idx := sort.Search(len(seq), func(i int) bool {
		return seq[i].TS >= ts
	})
	return idx < len(seq) && seq[idx].TS == ts
}

func (c *OHLCVCache) AppendOrReplace(exchange, symbol, timeframe string, ohlcv models.OHLCV, closed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	byTimeframe := c.ensureSeries(exchange, symbol)
	seq := byTimeframe[timeframe]
	if len(seq) == 0 {
		seq = append(seq, ohlcv)
	} else {
		last := seq[len(seq)-1]
		if ohlcv.TS == last.TS {
			seq[len(seq)-1] = ohlcv
		} else if ohlcv.TS > last.TS {
			seq = append(seq, ohlcv)
		} else {
			return
		}
	}
	if len(seq) > maxOHLCVCacheSize {
		seq = seq[len(seq)-maxOHLCVCacheSize:]
	}
	byTimeframe[timeframe] = seq
	if closed {
		byClosed := c.ensureClosed(exchange, symbol)
		byClosed[timeframe] = ohlcv.TS
	}
}

func (c *OHLCVCache) DropBeforeOrEqual(exchange, symbol, timeframe string, ts int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := pairKey(exchange, symbol)
	byTimeframe, ok := c.series[key]
	if !ok {
		return 0
	}
	seq := byTimeframe[timeframe]
	if len(seq) == 0 {
		return 0
	}
	cut := 0
	for cut < len(seq) && seq[cut].TS <= ts {
		cut++
	}
	removed := cut
	if removed == 0 {
		return 0
	}
	seq = seq[cut:]
	if byClosed := c.closed[key]; byClosed != nil {
		if lastClosed, ok := byClosed[timeframe]; ok && lastClosed <= ts {
			delete(byClosed, timeframe)
			if len(byClosed) == 0 {
				delete(c.closed, key)
			}
		}
	}
	if len(seq) == 0 {
		delete(byTimeframe, timeframe)
		if len(byTimeframe) == 0 {
			delete(c.series, key)
			delete(c.closed, key)
		}
		return removed
	}
	byTimeframe[timeframe] = seq
	return removed
}

func (c *OHLCVCache) Snapshot(exchange, symbol, eventTimeframe string, eventTS int64) models.MarketSnapshot {
	return c.snapshotFiltered(exchange, symbol, eventTimeframe, eventTS, nil)
}

func (c *OHLCVCache) SnapshotForTimeframes(exchange, symbol, eventTimeframe string, eventTS int64, timeframes []string) models.MarketSnapshot {
	return c.snapshotFiltered(exchange, symbol, eventTimeframe, eventTS, timeframes)
}

func (c *OHLCVCache) snapshotFiltered(exchange, symbol, eventTimeframe string, eventTS int64, timeframes []string) models.MarketSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pairKey(exchange, symbol)
	byTimeframe := c.series[key]
	byClosed := c.closed[key]
	allow := make(map[string]struct{}, len(timeframes))
	filterByTimeframe := len(timeframes) > 0
	if filterByTimeframe {
		for _, timeframe := range timeframes {
			normalized := strings.ToLower(strings.TrimSpace(timeframe))
			if normalized == "" {
				continue
			}
			allow[normalized] = struct{}{}
		}
		if len(allow) == 0 {
			filterByTimeframe = false
		}
	}
	series := make(map[string][]models.OHLCV, len(byTimeframe))
	meta := make(map[string]models.SeriesMeta, len(byTimeframe))
	for timeframe, seq := range byTimeframe {
		if filterByTimeframe {
			if _, ok := allow[strings.ToLower(strings.TrimSpace(timeframe))]; !ok {
				continue
			}
		}
		copied := make([]models.OHLCV, len(seq))
		copy(copied, seq)
		series[timeframe] = copied
		lastClosedTS := int64(0)
		if byClosed != nil {
			lastClosedTS = byClosed[timeframe]
		}
		lastIndex := -1
		if lastClosedTS > 0 {
			for i := len(seq) - 1; i >= 0; i-- {
				if seq[i].TS <= lastClosedTS {
					lastIndex = i
					break
				}
			}
		}
		meta[timeframe] = models.SeriesMeta{
			LastClosedTS: lastClosedTS,
			LastIndex:    lastIndex,
		}
	}
	return models.MarketSnapshot{
		Exchange:       exchange,
		Symbol:         symbol,
		EventTimeframe: eventTimeframe,
		EventTS:        eventTS,
		Series:         series,
		Meta:           meta,
	}
}

func (c *OHLCVCache) SeriesSnapshot(exchange, symbol, timeframe string) ([]models.OHLCV, int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := pairKey(exchange, symbol)
	byTimeframe, ok := c.series[key]
	if !ok {
		return nil, 0
	}
	seq := byTimeframe[timeframe]
	if len(seq) == 0 {
		return nil, 0
	}
	copied := make([]models.OHLCV, len(seq))
	copy(copied, seq)
	lastClosed := int64(0)
	if byClosed := c.closed[key]; byClosed != nil {
		lastClosed = byClosed[timeframe]
	}
	return copied, lastClosed
}

func (c *OHLCVCache) MergeMarketData(exchange, symbol, timeframe string, data []models.MarketData) int {
	if len(data) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	key := pairKey(exchange, symbol)
	byTimeframe := c.ensureSeries(exchange, symbol)
	seq := byTimeframe[timeframe]
	merged := make(map[int64]models.OHLCV, len(seq)+len(data))
	for _, item := range seq {
		merged[item.TS] = item
	}
	lastClosed := int64(0)
	if byClosed := c.closed[key]; byClosed != nil {
		lastClosed = byClosed[timeframe]
	}
	for _, item := range data {
		merged[item.OHLCV.TS] = item.OHLCV
		if item.Closed && item.OHLCV.TS > lastClosed {
			lastClosed = item.OHLCV.TS
		}
	}
	out := make([]models.OHLCV, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	if len(out) > maxOHLCVCacheSize {
		out = out[len(out)-maxOHLCVCacheSize:]
	}
	byTimeframe[timeframe] = out
	if lastClosed > 0 {
		byClosed := c.ensureClosed(exchange, symbol)
		if len(out) == 0 || lastClosed < out[0].TS {
			delete(byClosed, timeframe)
			if len(byClosed) == 0 {
				delete(c.closed, key)
			}
		} else {
			byClosed[timeframe] = lastClosed
		}
	}
	return len(out)
}

func (c *OHLCVCache) ensureSeries(exchange, symbol string) map[string][]models.OHLCV {
	key := pairKey(exchange, symbol)
	byTimeframe, ok := c.series[key]
	if !ok {
		byTimeframe = make(map[string][]models.OHLCV)
		c.series[key] = byTimeframe
	}
	return byTimeframe
}

func (c *OHLCVCache) ensureClosed(exchange, symbol string) map[string]int64 {
	key := pairKey(exchange, symbol)
	byClosed, ok := c.closed[key]
	if !ok {
		byClosed = make(map[string]int64)
		c.closed[key] = byClosed
	}
	return byClosed
}

func pairKey(exchange, symbol string) string {
	return exchange + "|" + symbol
}
