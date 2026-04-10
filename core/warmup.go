package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

const warmupTimeLayout = "20060102_1504"

var errWarmupExchangePaused = errors.New("warmup exchange paused")

type cachedBar struct {
	ohlcv  models.OHLCV
	closed bool
}

type tsRange struct {
	start int64
	end   int64
}

type warmupExchangePlan struct {
	exchange          string
	items             []models.Symbol
	defaultTimeframes []string
	limit             int
	cacheSize         int
}

func (b *Live) warmUpCache(ctx context.Context) error {
	if b == nil || b.Cache == nil || b.ohlcvStore == nil {
		return nil
	}
	plans, requestCtx, err := b.buildWarmupPlans(ctx)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	for _, plan := range plans {
		plan := plan
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, runErr := b.warmUpExchangePlan(requestCtx, plan)
			if runErr != nil && ctx != nil && ctx.Err() == nil {
				b.logger.Warn("warmup exchange failed",
					zap.String("exchange", plan.exchange),
					zap.Error(runErr),
				)
			}
		}()
	}
	wg.Wait()
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}

func (b *Live) buildWarmupPlans(ctx context.Context) ([]warmupExchangePlan, context.Context, error) {
	if b == nil || b.Cache == nil || b.ohlcvStore == nil {
		return nil, ctx, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, ctx, err
	}
	symbols, err := b.ohlcvStore.ListSymbols()
	if err != nil {
		b.logger.Warn("load symbols failed", zap.Error(err))
		return nil, ctx, nil
	}
	exchanges, err := b.ohlcvStore.ListExchanges()
	if err != nil {
		b.logger.Warn("load exchanges failed", zap.Error(err))
		return nil, ctx, nil
	}
	exchangeTimeframes := make(map[string][]string, len(exchanges))
	exchangeLimits := make(map[string]int, len(exchanges))
	exchangeRates := make(map[string]time.Duration, len(exchanges))
	activeExchanges := make(map[string]bool, len(exchanges))
	for _, ex := range exchanges {
		if !ex.Active {
			continue
		}
		timeframes, err := parseExchangeTimeframes(ex.Timeframes)
		if err != nil {
			b.logger.Warn("parse exchange timeframes failed",
				zap.String("exchange", ex.Name),
				zap.String("timeframes", ex.Timeframes),
				zap.Error(err),
			)
			timeframes = []string{"1m"}
		}
		exchangeTimeframes[ex.Name] = timeframes
		exchangeLimits[ex.Name] = ex.OHLCVLimit
		if ex.RateLimitMS > 0 {
			exchangeRates[ex.Name] = time.Duration(ex.RateLimitMS) * time.Millisecond
		}
		activeExchanges[ex.Name] = true
	}
	requestController := b.requestController
	if requestController == nil {
		requestController = market.NewRequestController(market.RequestControllerConfig{
			Logger:            b.logger,
			ExchangeIntervals: exchangeRates,
			APIRules:          market.DefaultAPILimitRules(),
		})
	}
	requestCtx := market.WithRequestController(ctx, requestController)

	cacheSize := b.Cache.MaxSize()
	symbolsByExchange := make(map[string][]models.Symbol, len(exchanges))
	for _, sym := range symbols {
		if !sym.Active {
			continue
		}
		if !activeExchanges[sym.Exchange] {
			continue
		}
		symbolsByExchange[sym.Exchange] = append(symbolsByExchange[sym.Exchange], sym)
	}
	plans := make([]warmupExchangePlan, 0, len(activeExchanges))
	for exchange := range activeExchanges {
		items := symbolsByExchange[exchange]
		plans = append(plans, warmupExchangePlan{
			exchange:          exchange,
			items:             items,
			defaultTimeframes: exchangeTimeframes[exchange],
			limit:             exchangeLimits[exchange],
			cacheSize:         cacheSize,
		})
	}
	sort.Slice(plans, func(i, j int) bool {
		return strings.ToLower(plans[i].exchange) < strings.ToLower(plans[j].exchange)
	})
	return plans, requestCtx, nil
}

func (b *Live) warmUpExchangePlan(ctx context.Context, plan warmupExchangePlan) (bool, error) {
	if b == nil {
		return false, nil
	}
	exchange := plan.exchange
	if b.warmupExchangePaused(exchange) {
		return true, nil
	}
	for _, sym := range plan.items {
		if ctx != nil && ctx.Err() != nil {
			return false, ctx.Err()
		}
		timeframes, err := parseSymbolTimeframes(sym.Timeframes, plan.defaultTimeframes)
		if err != nil {
			b.logger.Warn("parse symbol timeframes failed",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(err),
			)
			timeframes = plan.defaultTimeframes
		}
		if len(timeframes) == 0 {
			b.logger.Warn("symbol timeframes missing",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
			)
			continue
		}
		timeframes = normalizePlanTimeframes(timeframes)
		if len(timeframes) == 0 {
			b.logger.Warn("symbol timeframes invalid after normalization",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
			)
			continue
		}
		for _, timeframe := range timeframes {
			if ctx != nil && ctx.Err() != nil {
				return false, ctx.Err()
			}
			err := b.warmUpSeries(ctx, exchange, sym.Symbol, timeframe, plan.cacheSize, plan.limit)
			if errors.Is(err, errWarmupExchangePaused) {
				return true, nil
			}
			if err != nil {
				return false, err
			}
		}
	}
	return false, nil
}

func (b *Live) warmUpSeries(ctx context.Context, exchange, symbol, timeframe string, cacheSize, exchangeLimit int) error {
	if cacheSize <= 0 {
		return nil
	}
	ctx = market.WithRequestLogger(ctx, b.logger)
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	data, err := b.ohlcvStore.ListRecentOHLCV(exchange, symbol, timeframe, cacheSize)
	if err != nil {
		b.logger.Warn("load recent ohlcv failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Error(err),
		)
		return nil
	}
	series := make([]cachedBar, len(data))
	for i, item := range data {
		series[i] = cachedBar{ohlcv: item, closed: true}
	}
	if len(series) > 1 {
		ordered, ok := b.ensureOrdered(exchange, symbol, timeframe, series)
		if !ok {
			return nil
		}
		series = ordered
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	series, err = b.fillMissing(ctx, exchange, symbol, timeframe, series, exchangeLimit)
	if err != nil {
		return err
	}
	if b.warmupExchangePaused(exchange) {
		return errWarmupExchangePaused
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	series, err = b.ensureTimely(ctx, exchange, symbol, timeframe, series, cacheSize, exchangeLimit)
	if err != nil {
		return err
	}
	if b.warmupExchangePaused(exchange) {
		return errWarmupExchangePaused
	}
	series = trimUnclosedHigherTimeframe(series, timeframe)
	series = trimSeries(series, cacheSize)
	if len(series) == 0 {
		return nil
	}
	for _, item := range series {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		b.Cache.AppendOrReplace(exchange, symbol, timeframe, item.ohlcv, item.closed)
	}
	b.updateLastPersisted(exchange, symbol, timeframe, series)
	return nil
}

func (b *Live) ensureOrdered(exchange, symbol, timeframe string, series []cachedBar) ([]cachedBar, bool) {
	outOfOrder := false
	for i := 1; i < len(series); i++ {
		if series[i].ohlcv.TS <= series[i-1].ohlcv.TS {
			outOfOrder = true
			break
		}
	}
	if !outOfOrder {
		return series, true
	}
	b.logger.Warn("ohlcv out-of-order detected",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
	)
	sort.Slice(series, func(i, j int) bool {
		return series[i].ohlcv.TS < series[j].ohlcv.TS
	})
	for i := 1; i < len(series); i++ {
		if series[i].ohlcv.TS <= series[i-1].ohlcv.TS {
			b.logger.Warn("ohlcv reorder failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
			)
			return nil, false
		}
	}
	b.logger.Info("ohlcv reordered",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
	)
	return series, true
}

func (b *Live) fillMissing(ctx context.Context, exchange, symbol, timeframe string, series []cachedBar, exchangeLimit int) ([]cachedBar, error) {
	if len(series) < 2 {
		return series, nil
	}
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok {
		return series, nil
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return series, nil
	}
	missing := missingTimestamps(series, step)
	if len(missing) == 0 {
		return series, nil
	}
	b.logger.Warn("ohlcv missing detected",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int64s("missing_ts", missing),
	)
	ranges := missingRanges(missing, step)
	for _, r := range ranges {
		if ctx != nil && ctx.Err() != nil {
			return series, ctx.Err()
		}
		from := formatWarmupTime(r.start)
		to := formatWarmupTime(r.end)
		data, err := b.fetchHistoryRangePaged(
			ctx,
			exchange,
			symbol,
			timeframe,
			time.UnixMilli(r.start).UTC(),
			time.UnixMilli(r.end).UTC(),
			exchangeLimit,
		)
		if err != nil {
			b.logger.Warn("ohlcv missing fetch failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.String("from", from),
				zap.String("to", to),
				zap.Int64("drop_before_ts", missing[0]),
				zap.Error(err),
			)
			if b.pauseWarmupExchange(exchange, err) {
				return series, errWarmupExchangePaused
			}
			return dropBefore(series, missing[0]), nil
		}
		b.logger.Info("ohlcv missing fetch success",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.String("from", from),
			zap.String("to", to),
			zap.Int("count", len(data)),
		)
		b.persistMarketData(data)
		series = mergeMarketData(series, data)
	}
	return series, nil
}

func (b *Live) ensureTimely(ctx context.Context, exchange, symbol, timeframe string, series []cachedBar, cacheSize, exchangeLimit int) ([]cachedBar, error) {
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok {
		return series, nil
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return series, nil
	}
	if ctx != nil && ctx.Err() != nil {
		return series, ctx.Err()
	}
	if len(series) == 0 {
		b.logger.Warn("ohlcv cache empty, requesting latest",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int("limit", cacheSize),
		)
		if ctx != nil && ctx.Err() != nil {
			return series, ctx.Err()
		}
		return b.refreshByLimit(ctx, exchange, symbol, timeframe, cacheSize, exchangeLimit)
	}
	lastTS := series[len(series)-1].ohlcv.TS
	now := time.Now().UTC()
	expectedLatest := expectedLatestOHLCVStart(now, dur, false)
	if expectedLatest <= 0 || lastTS >= expectedLatest {
		return series, nil
	}
	missingCount := (expectedLatest - lastTS) / step
	if missingCount <= 0 {
		return series, nil
	}
	if missingCount > int64(cacheSize) {
		b.logger.Warn("ohlcv cache too stale, requesting latest",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int64("missing_count", missingCount),
			zap.Int("limit", cacheSize),
		)
		if ctx != nil && ctx.Err() != nil {
			return series, ctx.Err()
		}
		return b.refreshByLimit(ctx, exchange, symbol, timeframe, cacheSize, exchangeLimit)
	}
	b.logger.Warn("ohlcv cache lag detected, requesting recent",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int64("missing_count", missingCount),
	)
	if ctx != nil && ctx.Err() != nil {
		return series, ctx.Err()
	}
	data, err := b.fetchHistoryByLimitPaged(ctx, exchange, symbol, timeframe, int(missingCount), exchangeLimit)
	if err != nil {
		b.logger.Warn("ohlcv lag fetch failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int64("missing_count", missingCount),
			zap.Error(err),
		)
		if b.pauseWarmupExchange(exchange, err) {
			return series, errWarmupExchangePaused
		}
		return series, nil
	}
	b.logger.Info("ohlcv lag fetch success",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int("count", len(data)),
	)
	b.persistMarketData(data)
	return mergeMarketData(series, data), nil
}

func (b *Live) refreshByLimit(ctx context.Context, exchange, symbol, timeframe string, limit, exchangeLimit int) ([]cachedBar, error) {
	if limit <= 0 {
		return nil, nil
	}
	data, err := b.fetchHistoryByLimitPaged(ctx, exchange, symbol, timeframe, limit, exchangeLimit)
	if err != nil {
		b.logger.Warn("ohlcv refresh fetch failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int("limit", limit),
			zap.Error(err),
		)
		if b.pauseWarmupExchange(exchange, err) {
			return nil, errWarmupExchangePaused
		}
		if errors.Is(err, market.ErrEmptyOHLCV) {
			return b.retryRefreshByLimit(ctx, exchange, symbol, timeframe, limit, exchangeLimit)
		}
		return nil, nil
	}
	b.logger.Info("ohlcv refresh fetch success",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int("count", len(data)),
	)
	b.persistMarketData(data)
	return marketDataToBars(data), nil
}

func (b *Live) retryRefreshByLimit(ctx context.Context, exchange, symbol, timeframe string, limit, exchangeLimit int) ([]cachedBar, error) {
	const maxRetries = 3
	retryLimit := limit
	for i := 0; i < maxRetries; i++ {
		retryLimit = reduceWarmupLimit(retryLimit, exchangeLimit)
		if retryLimit <= 0 || retryLimit >= limit {
			return nil, nil
		}
		data, err := b.fetchHistoryByLimitPaged(ctx, exchange, symbol, timeframe, retryLimit, exchangeLimit)
		if err != nil {
			b.logger.Warn("ohlcv refresh fetch retry failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Int("limit", retryLimit),
				zap.Error(err),
			)
			if b.pauseWarmupExchange(exchange, err) {
				return nil, errWarmupExchangePaused
			}
			if !errors.Is(err, market.ErrEmptyOHLCV) {
				return nil, nil
			}
			continue
		}
		b.logger.Info("ohlcv refresh fetch success",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int("count", len(data)),
		)
		b.persistMarketData(data)
		return marketDataToBars(data), nil
	}
	return nil, nil
}

func reduceWarmupLimit(current, exchangeLimit int) int {
	if current <= 1 {
		return 0
	}
	next := current / 2
	if next < 1 {
		next = 1
	}
	if exchangeLimit > 0 && next > exchangeLimit {
		next = exchangeLimit
	}
	if next >= current {
		return current - 1
	}
	return next
}

func (b *Live) pauseWarmupExchange(exchange string, err error) bool {
	until, ok := market.PauseExchangeOnError(exchange, err)
	if !ok {
		return false
	}
	b.logger.Warn("warmup exchange paused",
		zap.String("exchange", exchange),
		zap.Time("paused_until", until),
		zap.Error(err),
	)
	return true
}

func (b *Live) warmupExchangePaused(exchange string) bool {
	_, ok := market.ExchangePaused(exchange, time.Now().UTC())
	return ok
}

func (b *Live) persistMarketData(data []models.MarketData) {
	if b == nil || b.ohlcvStore == nil {
		return
	}
	for _, item := range data {
		if !item.Closed {
			continue
		}
		if !shouldPersistSource(item.Source) {
			continue
		}
		if !b.shouldPersistPairTimeframe(item.Exchange, item.Symbol, item.Timeframe) {
			continue
		}
		if err := b.ohlcvStore.SaveOHLCV(item); err != nil {
			b.logger.Error("save ohlcv failed",
				zap.String("exchange", item.Exchange),
				zap.String("symbol", item.Symbol),
				zap.String("timeframe", item.Timeframe),
				zap.Int64("ts", item.OHLCV.TS),
				zap.Error(err),
			)
		}
	}
}

func (b *Live) updateLastPersisted(exchange, symbol, timeframe string, series []cachedBar) {
	if b == nil || b.lastPersisted == nil {
		return
	}
	var lastClosed int64
	for _, item := range series {
		if item.closed && item.ohlcv.TS > lastClosed {
			lastClosed = item.ohlcv.TS
		}
	}
	if lastClosed == 0 {
		return
	}
	key := b.stateKey(exchange, symbol, timeframe)
	b.persistMu.Lock()
	if last, ok := b.lastPersisted[key]; !ok || lastClosed > last {
		b.lastPersisted[key] = lastClosed
	}
	b.persistMu.Unlock()
}

func parseExchangeTimeframes(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty timeframes")
	}
	var items []string
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]bool)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		if _, ok := market.TimeframeDuration(item); !ok {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid timeframes")
	}
	return out, nil
}

func parseSymbolTimeframes(raw string, fallback []string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	var items []string
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]bool)
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		if _, ok := market.TimeframeDuration(item); !ok {
			continue
		}
		seen[item] = true
		out = append(out, item)
	}
	if len(out) == 0 {
		return fallback, nil
	}
	return out, nil
}

func missingTimestamps(series []cachedBar, step int64) []int64 {
	if len(series) < 2 || step <= 0 {
		return nil
	}
	var out []int64
	for i := 1; i < len(series); i++ {
		prev := series[i-1].ohlcv.TS
		curr := series[i].ohlcv.TS
		if curr <= prev {
			continue
		}
		delta := curr - prev
		if delta == step {
			continue
		}
		for ts := prev + step; ts < curr; ts += step {
			out = append(out, ts)
		}
	}
	return out
}

func missingRanges(missing []int64, step int64) []tsRange {
	if len(missing) == 0 {
		return nil
	}
	out := []tsRange{{start: missing[0], end: missing[0]}}
	for i := 1; i < len(missing); i++ {
		last := &out[len(out)-1]
		if missing[i] == last.end+step {
			last.end = missing[i]
			continue
		}
		out = append(out, tsRange{start: missing[i], end: missing[i]})
	}
	return out
}

func mergeMarketData(series []cachedBar, data []models.MarketData) []cachedBar {
	if len(data) == 0 {
		return series
	}
	merged := make(map[int64]cachedBar, len(series)+len(data))
	for _, item := range series {
		merged[item.ohlcv.TS] = item
	}
	for _, item := range data {
		merged[item.OHLCV.TS] = cachedBar{ohlcv: item.OHLCV, closed: item.Closed}
	}
	out := make([]cachedBar, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ohlcv.TS < out[j].ohlcv.TS
	})
	return out
}

func marketDataToBars(data []models.MarketData) []cachedBar {
	if len(data) == 0 {
		return nil
	}
	out := make([]cachedBar, 0, len(data))
	for _, item := range data {
		out = append(out, cachedBar{ohlcv: item.OHLCV, closed: item.Closed})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ohlcv.TS < out[j].ohlcv.TS
	})
	return out
}

func trimSeries(series []cachedBar, maxSize int) []cachedBar {
	if maxSize <= 0 || len(series) <= maxSize {
		return series
	}
	return series[len(series)-maxSize:]
}

func trimUnclosedHigherTimeframe(series []cachedBar, timeframe string) []cachedBar {
	if len(series) == 0 || strings.EqualFold(strings.TrimSpace(timeframe), oneMinuteTimeframe) {
		return series
	}
	out := series[:0]
	for _, item := range series {
		if !item.closed {
			continue
		}
		out = append(out, item)
	}
	return out
}

func dropBefore(series []cachedBar, ts int64) []cachedBar {
	if len(series) == 0 {
		return series
	}
	idx := 0
	for idx < len(series) && series[idx].ohlcv.TS <= ts {
		idx++
	}
	return series[idx:]
}

func formatWarmupTime(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).UTC().Format(warmupTimeLayout)
}

func (b *Live) primeUnclosedSeriesFrom1mFetch(ctx context.Context, exchange, symbol string, targetTimeframes []string, exchangeLimit int) {
	if b == nil || b.Cache == nil || b.assembler == nil || b.historyFetcher == nil {
		return
	}
	targets := normalizeAssembleTargets(targetTimeframes, time.Minute)
	if len(targets) == 0 {
		return
	}
	limit := oneMinuteAssembleLimit(targets)
	if limit <= 0 {
		return
	}
	data, err := b.fetchHistoryByLimitPaged(ctx, exchange, symbol, oneMinuteTimeframe, limit, exchangeLimit)
	if err != nil {
		b.logger.Warn("prime unclosed timeframe from 1m failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Int("limit", limit),
			zap.Error(err),
		)
		return
	}
	if len(data) == 0 {
		return
	}
	sort.Slice(data, func(i, j int) bool {
		return data[i].OHLCV.TS < data[j].OHLCV.TS
	})
	minBucketStart := minBucketStartForTargets(time.Now().UTC(), targets)
	for _, item := range data {
		if item.OHLCV.TS <= 0 {
			continue
		}
		if minBucketStart > 0 && item.OHLCV.TS < minBucketStart {
			continue
		}
		item.Exchange = exchange
		item.Symbol = symbol
		item.Timeframe = oneMinuteTimeframe
		if strings.TrimSpace(item.Source) == "" {
			item.Source = "warmup-1m"
		}
		assembled := b.assembler.On1m(item, targets)
		for _, out := range assembled {
			if out.Closed {
				continue
			}
			b.Cache.AppendOrReplace(out.Exchange, out.Symbol, out.Timeframe, out.OHLCV, false)
		}
	}
}

func oneMinuteAssembleLimit(targetTimeframes []string) int {
	const maxAssembleLimit = 7200
	maxMinutes := 0
	for _, timeframe := range targetTimeframes {
		dur, ok := market.TimeframeDuration(timeframe)
		if !ok || dur <= time.Minute {
			continue
		}
		minutes := int(dur/time.Minute) + 2
		if minutes > maxMinutes {
			maxMinutes = minutes
		}
	}
	if maxMinutes <= 0 {
		return 0
	}
	if maxMinutes > maxAssembleLimit {
		return maxAssembleLimit
	}
	return maxMinutes
}

func minBucketStartForTargets(now time.Time, targetTimeframes []string) int64 {
	var minTS int64
	for _, timeframe := range targetTimeframes {
		dur, ok := market.TimeframeDuration(timeframe)
		if !ok || dur <= time.Minute {
			continue
		}
		start := now.Truncate(dur).UnixMilli()
		if minTS == 0 || start < minTS {
			minTS = start
		}
	}
	return minTS
}
