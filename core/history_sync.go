package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

const (
	historyNoProgressLimit   = 3
	historyCooldown          = time.Hour
	historyEventSyncDebounce = 5 * time.Second
)

type historyState struct {
	lastMinTS     int64
	noProgress    int
	cooldownUntil time.Time
}

type historySyncTask struct {
	Exchange string
	Symbol   string
	Reason   string
}

func (b *Live) startHistorySync(ctx context.Context) error {
	if b == nil || !b.historyOn {
		return nil
	}
	if b.Cache == nil || b.ohlcvStore == nil {
		b.logger.Warn("history sync disabled: missing cache or store")
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	historyCtx, cancel := context.WithCancel(ctx)
	b.historyCancel = cancel
	b.historyCtx = historyCtx

	exchanges, err := b.ohlcvStore.ListExchanges()
	if err != nil {
		b.logger.Warn("load exchanges failed", zap.Error(err))
		return nil
	}
	rateByExchange := make(map[string]time.Duration, len(exchanges))
	for _, ex := range exchanges {
		if !ex.Active {
			continue
		}
		if ex.RateLimitMS > 0 {
			rateByExchange[ex.Name] = time.Duration(ex.RateLimitMS) * time.Millisecond
		}
	}
	requestController := b.requestController
	if requestController == nil {
		requestController = market.NewRequestController(market.RequestControllerConfig{
			Logger:            b.logger,
			ExchangeIntervals: rateByExchange,
			APIRules:          market.DefaultAPILimitRules(),
		})
	}
	b.historyController = requestController
	b.historyWG.Add(1)
	go b.runHistoryOnDemand(historyCtx, requestController)
	return nil
}

func (b *Live) startHistoryExchange(exchange string) {
	if b == nil || !b.historyOn {
		return
	}
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return
	}
	b.historyMu.Lock()
	if b.historyCtx == nil || b.historyController == nil {
		b.historyMu.Unlock()
		return
	}
	if _, ok := b.historyStarted[exchange]; ok {
		b.historyMu.Unlock()
		return
	}
	b.historyStarted[exchange] = struct{}{}
	historyCtx := b.historyCtx
	controller := b.historyController
	b.historyMu.Unlock()
	b.historyWG.Add(1)
	go b.runHistoryExchange(historyCtx, exchange, controller)
}

func (b *Live) runHistoryExchange(ctx context.Context, exchange string, controller *market.RequestController) {
	defer b.historyWG.Done()
	nextDelay := time.Duration(0)
	for {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if nextDelay > 0 {
			timer := time.NewTimer(nextDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if delay, paused := b.historyPauseDelay(exchange); paused {
			b.logger.Warn("history sync skipped due to pause",
				zap.String("exchange", exchange),
				zap.Duration("resume_in", delay),
			)
			nextDelay = delay
			continue
		}
		b.historyCheckExchange(ctx, exchange, controller)
		if delay, paused := b.historyPauseDelay(exchange); paused {
			nextDelay = delay
			continue
		}
		nextDelay = b.historyInt
	}
}

func (b *Live) historyCheckExchange(ctx context.Context, exchange string, controller *market.RequestController) {
	if b == nil || b.Cache == nil || b.ohlcvStore == nil {
		return
	}
	if ctx != nil && ctx.Err() != nil {
		return
	}
	exchanges, err := b.ohlcvStore.ListExchanges()
	if err != nil {
		b.logger.Warn("load exchanges failed", zap.Error(err))
		return
	}
	var cfg *models.Exchange
	for i := range exchanges {
		if strings.EqualFold(exchanges[i].Name, exchange) {
			cfg = &exchanges[i]
			break
		}
	}
	if cfg == nil || !cfg.Active {
		return
	}
	timeframes, err := parseExchangeTimeframes(cfg.Timeframes)
	if err != nil {
		b.logger.Warn("parse exchange timeframes failed",
			zap.String("exchange", exchange),
			zap.String("timeframes", cfg.Timeframes),
			zap.Error(err),
		)
		timeframes = []string{"1m"}
	}
	symbols, err := b.ohlcvStore.ListSymbols()
	if err != nil {
		b.logger.Warn("load symbols failed", zap.Error(err))
		return
	}
	cacheSize := b.Cache.MaxSize()
	requestCtx := market.WithRequestController(ctx, controller)
	for _, sym := range symbols {
		if !strings.EqualFold(sym.Exchange, exchange) {
			continue
		}
		if !sym.Active {
			continue
		}
		if b.historyExchangePaused(exchange) {
			return
		}
		symbolTimeframes, err := parseSymbolTimeframes(sym.Timeframes, timeframes)
		if err != nil {
			b.logger.Warn("parse symbol timeframes failed",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(err),
			)
			symbolTimeframes = timeframes
		}
		if len(symbolTimeframes) == 0 {
			b.logger.Warn("symbol timeframes missing",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
			)
			continue
		}
		symbolTimeframes = normalizePlanTimeframes(symbolTimeframes)
		if len(symbolTimeframes) == 0 {
			b.logger.Warn("symbol timeframes invalid after normalization",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
			)
			continue
		}
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if b.historyExchangePaused(exchange) {
			return
		}
		for _, timeframe := range symbolTimeframes {
			if ctx != nil && ctx.Err() != nil {
				return
			}
			if b.historyExchangePaused(exchange) {
				return
			}
			b.checkHistorySymbol(requestCtx, exchange, sym.Symbol, timeframe, cacheSize, cfg.OHLCVLimit, true)
		}
		b.primeUnclosedSeriesFrom1mFetch(requestCtx, exchange, sym.Symbol, symbolTimeframes, cfg.OHLCVLimit)
	}
}

func (b *Live) runHistoryOnDemand(ctx context.Context, controller *market.RequestController) {
	defer b.historyWG.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-b.historyNotify:
			if !ok {
				return
			}
			b.historyCheckPair(ctx, task, controller)
		}
	}
}

func (b *Live) historyCheckPair(ctx context.Context, task historySyncTask, controller *market.RequestController) {
	if b == nil || b.Cache == nil || b.ohlcvStore == nil {
		return
	}
	exchange := strings.TrimSpace(task.Exchange)
	symbol := strings.TrimSpace(task.Symbol)
	if exchange == "" || symbol == "" {
		return
	}
	defer b.finishHistoryPairSync(exchange, symbol)
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if _, paused := b.historyPauseDelay(exchange); paused {
		b.logger.Debug("history on-demand sync skipped due to exchange pause",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("reason", task.Reason),
		)
		return
	}
	cacheSize := b.Cache.MaxSize()
	if cacheSize <= 0 {
		return
	}
	timeframes := b.resolvePairTimeframes(exchange, symbol, oneMinuteTimeframe)
	if len(timeframes) == 0 {
		return
	}
	b.logger.Debug("history on-demand sync triggered",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("reason", task.Reason),
		zap.Strings("timeframes", timeframes),
	)
	exchangeLimit := b.loadExchangeOHLCVLimit(exchange)
	requestCtx := market.WithRequestController(ctx, controller)
	for _, timeframe := range timeframes {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if b.historyExchangePaused(exchange) {
			return
		}
		b.checkHistorySymbol(requestCtx, exchange, symbol, timeframe, cacheSize, exchangeLimit, false)
	}
}

func (b *Live) loadExchangeOHLCVLimit(exchange string) int {
	if b == nil || b.ohlcvStore == nil {
		return 0
	}
	exchanges, err := b.ohlcvStore.ListExchanges()
	if err != nil {
		b.logger.Warn("load exchanges failed for on-demand history sync",
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		return 0
	}
	for _, item := range exchanges {
		if !strings.EqualFold(item.Name, exchange) {
			continue
		}
		if !item.Active {
			return 0
		}
		return item.OHLCVLimit
	}
	return 0
}

func (b *Live) checkHistorySymbol(ctx context.Context, exchange, symbol, timeframe string, cacheSize, exchangeLimit int, allowBackfill bool) {
	if cacheSize <= 0 {
		return
	}
	ctx = market.WithRequestLogger(ctx, b.logger)
	dur, ok := market.TimeframeDuration(timeframe)
	if !ok {
		return
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return
	}
	series, lastClosed := b.Cache.SeriesSnapshot(exchange, symbol, timeframe)
	bars := cachedBarsFromSeries(series, lastClosed)
	if len(bars) > 1 {
		ordered, ok := b.ensureOrdered(exchange, symbol, timeframe, bars)
		if !ok {
			return
		}
		bars = ordered
	}
	bars = b.fillMissingHistory(ctx, exchange, symbol, timeframe, bars, exchangeLimit, step)
	bars = b.ensureTimelyHistory(ctx, exchange, symbol, timeframe, bars, cacheSize, exchangeLimit, step, dur)
	bars = trimUnclosedHigherTimeframe(bars, timeframe)
	bars = trimSeries(bars, cacheSize)
	if !allowBackfill {
		return
	}

	if len(bars) >= cacheSize {
		return
	}
	key := b.stateKey(exchange, symbol, timeframe)
	if b.historyInCooldown(key) {
		return
	}
	prevMin := firstSeriesTS(bars)
	updated, progressed := b.backfillHistory(ctx, exchange, symbol, timeframe, bars, cacheSize, exchangeLimit, step)
	if len(updated) > 0 {
		bars = updated
	}
	newMin := firstSeriesTS(bars)
	b.updateHistoryState(key, prevMin, newMin, progressed)
}

func (b *Live) fillMissingHistory(ctx context.Context, exchange, symbol, timeframe string, series []cachedBar, exchangeLimit int, step int64) []cachedBar {
	if len(series) < 2 || step <= 0 {
		return series
	}
	missing := missingTimestamps(series, step)
	if len(missing) == 0 {
		return series
	}
	b.logger.Warn("ohlcv missing detected",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.String("timeframe", timeframe),
		zap.Int64s("missing_ts", missing),
	)
	ranges := missingRanges(missing, step)
	for _, r := range ranges {
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
				zap.Error(err),
			)
			b.pauseHistoryExchange(exchange, err)
			continue
		}
		if len(data) == 0 {
			continue
		}
		b.persistMarketData(data)
		b.Cache.MergeMarketData(exchange, symbol, timeframe, data)
		series = mergeMarketData(series, data)
	}
	return series
}

func (b *Live) ensureTimelyHistory(ctx context.Context, exchange, symbol, timeframe string, series []cachedBar, cacheSize, exchangeLimit int, step int64, dur time.Duration) []cachedBar {
	if step <= 0 {
		return series
	}
	if len(series) == 0 {
		data, err := b.fetchByLimit(ctx, exchange, symbol, timeframe, cacheSize, exchangeLimit)
		if err != nil {
			b.logger.Warn("ohlcv refresh fetch failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Int("limit", cacheSize),
				zap.Error(err),
			)
			return series
		}
		return marketDataToBars(data)
	}
	lastTS := series[len(series)-1].ohlcv.TS
	now := time.Now().UTC()
	expectedLatest := expectedLatestOHLCVStart(now, dur, false)
	if expectedLatest <= 0 || lastTS >= expectedLatest {
		return series
	}
	missingCount := (expectedLatest - lastTS) / step
	if missingCount <= 0 {
		return series
	}
	limit := int(missingCount)
	if missingCount > int64(cacheSize) {
		limit = cacheSize
	}
	data, err := b.fetchByLimit(ctx, exchange, symbol, timeframe, limit, exchangeLimit)
	if err != nil {
		b.logger.Warn("ohlcv lag fetch failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.Int64("missing_count", missingCount),
			zap.Error(err),
		)
		return series
	}
	return mergeMarketData(series, data)
}

func (b *Live) backfillHistory(ctx context.Context, exchange, symbol, timeframe string, series []cachedBar, cacheSize, exchangeLimit int, step int64) ([]cachedBar, bool) {
	if cacheSize <= 0 || step <= 0 {
		return series, false
	}
	if len(series) == 0 {
		data, err := b.fetchByLimit(ctx, exchange, symbol, timeframe, cacheSize, exchangeLimit)
		if err != nil {
			b.logger.Warn("ohlcv backfill failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Int("limit", cacheSize),
				zap.Error(err),
			)
			return series, false
		}
		updated := marketDataToBars(data)
		return updated, len(updated) > 0
	}
	if len(series) >= cacheSize {
		return series, false
	}
	needed := cacheSize - len(series)
	earliest := series[0].ohlcv.TS
	fromTS := earliest - int64(needed)*step
	toTS := earliest - step
	if fromTS <= 0 || toTS <= 0 || toTS < fromTS {
		return series, false
	}
	fromTS, toTS, allowed := b.clipBackfillRangeByBound(ctx, exchange, symbol, timeframe, fromTS, toTS)
	if !allowed {
		return series, false
	}
	from := formatWarmupTime(fromTS)
	to := formatWarmupTime(toTS)
	data, err := b.fetchHistoryRangePaged(
		ctx,
		exchange,
		symbol,
		timeframe,
		time.UnixMilli(fromTS).UTC(),
		time.UnixMilli(toTS).UTC(),
		exchangeLimit,
	)
	if err != nil {
		b.logger.Warn("ohlcv backfill failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("timeframe", timeframe),
			zap.String("from", from),
			zap.String("to", to),
			zap.Error(err),
		)
		if errors.Is(err, market.ErrEmptyOHLCV) && b.isSmallestConfiguredTimeframe(exchange, symbol, timeframe) {
			b.recordOHLCVBound(exchange, symbol, earliest)
		}
		b.pauseHistoryExchange(exchange, err)
		return series, false
	}
	if len(data) == 0 {
		return series, false
	}
	b.persistMarketData(data)
	b.Cache.MergeMarketData(exchange, symbol, timeframe, data)
	updated := mergeMarketData(series, data)
	updated = trimSeries(updated, cacheSize)
	progressed := len(updated) > 0 && updated[0].ohlcv.TS < earliest
	return updated, progressed
}

func (b *Live) fetchByLimit(ctx context.Context, exchange, symbol, timeframe string, limit, exchangeLimit int) ([]models.MarketData, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit")
	}
	data, err := b.fetchHistoryByLimitPaged(ctx, exchange, symbol, timeframe, limit, exchangeLimit)
	if err != nil {
		b.pauseHistoryExchange(exchange, err)
		return nil, err
	}
	if len(data) == 0 {
		return nil, market.ErrEmptyOHLCV
	}
	b.persistMarketData(data)
	b.Cache.MergeMarketData(exchange, symbol, timeframe, data)
	return data, nil
}

func cachedBarsFromSeries(series []models.OHLCV, lastClosed int64) []cachedBar {
	if len(series) == 0 {
		return nil
	}
	out := make([]cachedBar, 0, len(series))
	for _, item := range series {
		out = append(out, cachedBar{
			ohlcv:  item,
			closed: lastClosed > 0 && item.TS <= lastClosed,
		})
	}
	return out
}

func firstSeriesTS(series []cachedBar) int64 {
	if len(series) == 0 {
		return 0
	}
	return series[0].ohlcv.TS
}

func (b *Live) historyInCooldown(key string) bool {
	now := time.Now()
	b.historyMu.Lock()
	defer b.historyMu.Unlock()
	state := b.historyStates[key]
	if state == nil {
		return false
	}
	if state.cooldownUntil.IsZero() {
		return false
	}
	if now.After(state.cooldownUntil) {
		state.cooldownUntil = time.Time{}
		return false
	}
	return true
}

func (b *Live) updateHistoryState(key string, prevMin, newMin int64, progressed bool) {
	now := time.Now()
	b.historyMu.Lock()
	defer b.historyMu.Unlock()
	state := b.historyStates[key]
	if state == nil {
		state = &historyState{}
		b.historyStates[key] = state
	}
	state.lastMinTS = newMin
	if progressed || (prevMin == 0 && newMin > 0) {
		state.noProgress = 0
		state.cooldownUntil = time.Time{}
		return
	}
	state.noProgress++
	if state.noProgress >= historyNoProgressLimit {
		state.noProgress = 0
		state.cooldownUntil = now.Add(historyCooldown)
	}
}

func (b *Live) pauseHistoryExchange(exchange string, err error) bool {
	until, ok := market.PauseExchangeOnError(exchange, err)
	if !ok {
		return false
	}
	b.logger.Warn("history exchange paused",
		zap.String("exchange", exchange),
		zap.Time("paused_until", until),
		zap.Error(err),
	)
	return true
}

func (b *Live) historyPauseDelay(exchange string) (time.Duration, bool) {
	now := time.Now().UTC()
	until, ok := market.ExchangePaused(exchange, now)
	if !ok {
		return 0, false
	}
	delay := until.Sub(now)
	if delay < time.Second {
		delay = time.Second
	}
	return delay, true
}

func (b *Live) historyExchangePaused(exchange string) bool {
	_, ok := market.ExchangePaused(exchange, time.Now().UTC())
	return ok
}
