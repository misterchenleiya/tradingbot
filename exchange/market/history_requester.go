package market

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

const (
	historySourceRange = "request-range"
	historySourceLimit = "request-limit"
)

func (f *HTTPFetcher) FetchOHLCVRangePaged(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time, maxPerRequest int) ([]models.MarketData, error) {
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)
	if exchange == "" || symbol == "" || timeframe == "" {
		return nil, fmt.Errorf("missing exchange/symbol/timeframe")
	}
	if end.Before(start) {
		return nil, fmt.Errorf("invalid time range")
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	if maxPerRequest <= 0 {
		return f.fetchOHLCVRange(ctx, exchange, symbol, timeframe, start, end, historySourceRange, 0)
	}
	total := int(end.Sub(start)/dur) + 1
	if total <= maxPerRequest {
		return f.fetchOHLCVRange(ctx, exchange, symbol, timeframe, start, end, historySourceRange, maxPerRequest)
	}
	out := make(map[int64]models.MarketData)
	cur := start
	remain := total
	for remain > 0 {
		chunk := maxPerRequest
		if remain < chunk {
			chunk = remain
		}
		chunkEnd := cur.Add(time.Duration(chunk-1) * dur)
		data, err := f.fetchOHLCVRange(ctx, exchange, symbol, timeframe, cur, chunkEnd, historySourceRange, maxPerRequest)
		if err != nil {
			return nil, err
		}
		for _, item := range data {
			out[item.OHLCV.TS] = item
		}
		remain -= chunk
		cur = chunkEnd.Add(dur)
	}
	if len(out) == 0 {
		return nil, ErrEmptyOHLCV
	}
	items := make([]models.MarketData, 0, len(out))
	for _, item := range out {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].OHLCV.TS < items[j].OHLCV.TS
	})
	return items, nil
}

func (f *HTTPFetcher) FetchOHLCVByLimitPaged(ctx context.Context, exchange, symbol, timeframe string, limit, maxPerRequest int) ([]models.MarketData, error) {
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)
	if exchange == "" || symbol == "" || timeframe == "" {
		return nil, fmt.Errorf("missing exchange/symbol/timeframe")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit")
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	now := time.Now().UTC()
	end := latestOHLCVLimitEnd(now, dur, f.FetchUnclosedOHLCV)
	start := end.Add(-time.Duration(limit-1) * dur)
	if maxPerRequest <= 0 || limit <= maxPerRequest {
		data, err := f.fetchOHLCVRange(ctx, exchange, symbol, timeframe, start, end, historySourceLimit, maxPerRequest)
		if err != nil {
			return nil, err
		}
		if len(data) > limit {
			data = data[len(data)-limit:]
		}
		return data, nil
	}
	out := make(map[int64]models.MarketData)
	cur := start
	remain := limit
	for remain > 0 {
		chunk := maxPerRequest
		if remain < chunk {
			chunk = remain
		}
		chunkEnd := cur.Add(time.Duration(chunk-1) * dur)
		data, err := f.fetchOHLCVRange(ctx, exchange, symbol, timeframe, cur, chunkEnd, historySourceLimit, maxPerRequest)
		if err != nil {
			return nil, err
		}
		for _, item := range data {
			out[item.OHLCV.TS] = item
		}
		remain -= chunk
		cur = chunkEnd.Add(dur)
	}
	if len(out) == 0 {
		return nil, ErrEmptyOHLCV
	}
	items := make([]models.MarketData, 0, len(out))
	for _, item := range out {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].OHLCV.TS < items[j].OHLCV.TS
	})
	if len(items) > limit {
		items = items[len(items)-limit:]
	}
	if len(items) == 0 {
		return nil, ErrEmptyOHLCV
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	filtered := filterOHLCVRange(items, startMS, endMS)
	if len(filtered) == 0 {
		return nil, ErrEmptyOHLCV
	}
	return filtered, nil
}

func latestOHLCVLimitEnd(now time.Time, dur time.Duration, fetchUnclosed bool) time.Time {
	if dur <= 0 {
		return now
	}
	currentStart := now.Truncate(dur)
	if fetchUnclosed {
		return currentStart
	}
	return currentStart.Add(-dur)
}

func (f *HTTPFetcher) fetchOHLCVRange(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time, source string, maxPerRequest int) ([]models.MarketData, error) {
	if f == nil {
		return nil, fmt.Errorf("nil fetcher")
	}
	if end.Before(start) {
		return nil, fmt.Errorf("invalid time range")
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	fetchEnd := end
	if dur > 0 {
		fetchEnd = end.Add(dur)
		if strings.EqualFold(exchange, "okx") && maxPerRequest > 0 {
			expected := int(fetchEnd.Sub(start)/dur) + 1
			if expected > maxPerRequest {
				fetchEnd = end
			}
		}
	}
	items, err := f.FetchOHLCVRange(ctx, exchange, symbol, timeframe, start, fetchEnd)
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, ErrEmptyOHLCV
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	now := time.Now().UTC()
	byTS := make(map[int64]models.MarketData, len(items))
	for _, item := range items {
		ts := normalizeTimestampMS(item.TS)
		if ts < startMS || ts > endMS {
			continue
		}
		item.TS = ts
		byTS[ts] = models.MarketData{
			Exchange:  exchange,
			Symbol:    symbol,
			Timeframe: timeframe,
			OHLCV:     item,
			Closed:    isOHLCVClosed(ts, timeframe, now),
			Source:    source,
		}
	}
	if len(byTS) == 0 {
		return nil, ErrEmptyOHLCV
	}
	out := make([]models.MarketData, 0, len(byTS))
	for _, item := range byTS {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].OHLCV.TS < out[j].OHLCV.TS
	})
	return out, nil
}

func filterOHLCVRange(items []models.MarketData, startMS, endMS int64) []models.MarketData {
	if len(items) == 0 {
		return items
	}
	out := items[:0]
	for _, item := range items {
		ts := item.OHLCV.TS
		if ts < startMS || ts > endMS {
			continue
		}
		out = append(out, item)
	}
	return out
}
