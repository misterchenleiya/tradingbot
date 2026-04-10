package market

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const backTestAutoSymbolType = "swap"
const backTestDefaultOHLCVFetchLimit = 300

type BackTestConfig struct {
	Source         string
	HistoryBars    int
	Fetcher        iface.HistoryFetcher
	Store          iface.OHLCVStore
	PreloadHandler iface.MarketHandler
	Handler        iface.MarketHandler
	Done           chan<- error
	Logger         *zap.Logger
}

type BackTestService struct {
	cfg     BackTestConfig
	stopCh  chan struct{}
	started atomic.Bool
	stopped atomic.Bool
	doneCh  chan error
	ctx     context.Context

	seqMu sync.Mutex
	seq   map[string]int64

	summaryMu    sync.Mutex
	summaryReady bool
	summary      BackTestSummary
}

func NewBackTestService(cfg BackTestConfig) *BackTestService {
	if cfg.Logger == nil {
		cfg.Logger = glog.Nop()
	}
	return &BackTestService{
		cfg: cfg,
	}
}

func (s *BackTestService) Start(ctx context.Context) (err error) {
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	fields := []zap.Field{
		zap.String("source", s.cfg.Source),
	}
	logger.Info("market back-test start", fields...)
	defer func() {
		logger.Info("market back-test started")
	}()
	if s.cfg.Handler == nil {
		return errors.New("nil handler")
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("service already started")
	}
	s.stopCh = make(chan struct{})
	s.stopped.Store(false)
	s.resetSummary()
	s.seqMu.Lock()
	s.seq = make(map[string]int64)
	s.seqMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx = ctx
	s.doneCh = make(chan error, 1)
	go s.run(s.cfg.Handler)
	return nil
}

func (s *BackTestService) run(handler iface.MarketHandler) {
	err := s.runOnce(handler)
	if s.doneCh != nil {
		s.doneCh <- err
		close(s.doneCh)
	}
	if s.cfg.Done != nil {
		select {
		case s.cfg.Done <- err:
		default:
		}
	}
}

func (s *BackTestService) runOnce(handler iface.MarketHandler) (err error) {
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}

	var source BackTestSource
	var series map[string][]models.OHLCV
	startedAt := time.Now().UTC()
	seed := seedFromSource(s.cfg.Source)
	summary := BackTestSummary{
		Source:       s.cfg.Source,
		Seed:         seed,
		HistoryBars:  normalizeBackTestHistoryBars(s.cfg.HistoryBars),
		StartedAtUTC: startedAt,
	}
	defer func() {
		result := "success"
		if err != nil {
			result = "failed"
		}
		if source.Type != "" {
			resultFields := []zap.Field{
				zap.String("result", result),
				zap.String("source", s.cfg.Source),
				zap.String("source_type", source.Type),
				zap.String("exchange", source.Exchange),
				zap.String("symbol", source.Symbol),
				zap.String("time_range", source.TimeRange),
				zap.Int64("seed", summary.Seed),
			}
			logger.Debug("market back-test result", resultFields...)
			if err == nil {
				for timeframe, data := range series {
					logger.Debug("market back-test kline count",
						zap.String("result", result),
						zap.String("exchange", source.Exchange),
						zap.String("symbol", source.Symbol),
						zap.String("timeframe", timeframe),
						zap.Int("kline_count", len(data)),
					)
				}
			}
			logger.Info("market back-test stopped",
				zap.String("result", result),
				zap.String("source", s.cfg.Source),
				zap.String("exchange", source.Exchange),
				zap.String("symbol", source.Symbol),
			)
		} else {
			logger.Debug("market back-test result", zap.String("result", result))
			logger.Info("market back-test stopped", zap.String("result", result))
		}
	}()

	source, err = parseBackTestSource(s.cfg.Source)
	if err != nil {
		s.started.Store(false)
		return err
	}
	normalizeHyperliquidBackTestSource(logger, &source)
	if err := s.ensureExchangeSourceSymbolConfigured(source); err != nil {
		s.started.Store(false)
		return err
	}
	summary.SourceType = source.Type
	summary.Exchange = source.Exchange
	summary.Symbol = source.Symbol
	summary.Timeframes = append([]string(nil), source.Timeframes...)
	summary.TimeRange = source.TimeRange
	summary.RangeStart = source.Start
	summary.RangeEnd = source.End
	if source.HasReplayTS {
		summary.ReplayStart = source.ReplayStart
	}
	if source.Type == sourceTypeCSV {
		summary.CSVFiles = append([]CSVFileSpec(nil), source.Files...)
	}
	logger.Info("market back-test replay started", zap.String("source", s.cfg.Source), zap.Int64("seed", summary.Seed))
	logger.Debug("market back-test start",
		zap.String("source", s.cfg.Source),
		zap.String("source_type", source.Type),
		zap.String("exchange", source.Exchange),
		zap.String("symbol", source.Symbol),
		zap.Strings("timeframes", source.Timeframes),
		zap.String("time_range", source.TimeRange),
		zap.String("replay_start", formatTimePoint(source.ReplayStart, source.HasReplayTS)),
		zap.Int64("seed", summary.Seed),
	)

	series, err = s.loadSeries(source)
	if err != nil {
		s.started.Store(false)
		return err
	}
	preloadSeries, replaySeries, splitErr := splitBackTestReplaySeries(series, source, summary.HistoryBars)
	if splitErr != nil {
		s.started.Store(false)
		return splitErr
	}
	if len(preloadSeries) > 0 && s.cfg.PreloadHandler != nil {
		if preloadErr := s.preload(s.cfg.PreloadHandler, source, preloadSeries); preloadErr != nil {
			s.started.Store(false)
			return preloadErr
		}
	}
	summary.PreloadCounts = make(map[string]int, len(preloadSeries))
	for timeframe, data := range preloadSeries {
		summary.PreloadCounts[timeframe] = len(data)
	}
	series = replaySeries
	summary.SeriesCounts = make(map[string]int, len(series))
	for timeframe, data := range series {
		summary.SeriesCounts[timeframe] = len(data)
	}

	if err := s.replay(handler, source, series); err != nil {
		s.started.Store(false)
		return err
	}

	summary.EndedAtUTC = time.Now().UTC()
	s.setSummary(summary)
	s.started.Store(false)
	return nil
}

func (s *BackTestService) Close() (err error) {
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("market back-test close")
	defer func() {
		logger.Info("market back-test closed")
	}()
	if s.stopped.CompareAndSwap(false, true) {
		if s.stopCh != nil {
			close(s.stopCh)
		}
	}
	if doneCh := s.doneCh; doneCh != nil {
		<-doneCh
	}
	return nil
}

func (s *BackTestService) Stop() error {
	return s.Close()
}

func (s *BackTestService) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	s.cfg.Logger = logger
}

func (s *BackTestService) Summary() (BackTestSummary, bool) {
	if s == nil {
		return BackTestSummary{}, false
	}
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	if !s.summaryReady {
		return BackTestSummary{}, false
	}
	return s.summary, true
}

type backTestSymbolStore interface {
	ListExchanges() ([]models.Exchange, error)
	ListSymbols() ([]models.Symbol, error)
	UpsertSymbol(sym models.Symbol) error
}

func (s *BackTestService) ensureExchangeSourceSymbolConfigured(source BackTestSource) error {
	if source.Type != sourceTypeExchange {
		return nil
	}
	store, ok := s.cfg.Store.(backTestSymbolStore)
	if !ok {
		return fmt.Errorf("back-test exchange source requires symbol-capable store")
	}
	return ensureExchangeSourceSymbolConfigured(store, source, s.cfg.Logger)
}

func ensureExchangeSourceSymbolConfigured(store backTestSymbolStore, source BackTestSource, logger *zap.Logger) error {
	exchanges, err := store.ListExchanges()
	if err != nil {
		return fmt.Errorf("load exchanges for back-test source failed: %w", err)
	}
	var exchangeCfg models.Exchange
	exchangeFound := false
	for _, item := range exchanges {
		if strings.EqualFold(strings.TrimSpace(item.Name), source.Exchange) {
			exchangeCfg = item
			exchangeFound = true
			break
		}
	}
	if !exchangeFound {
		return fmt.Errorf("exchange not configured in exchanges table: %s", source.Exchange)
	}

	symbols, err := store.ListSymbols()
	if err != nil {
		return fmt.Errorf("load symbols for back-test source failed: %w", err)
	}
	for _, item := range symbols {
		if strings.EqualFold(strings.TrimSpace(item.Exchange), source.Exchange) &&
			strings.EqualFold(strings.TrimSpace(item.Symbol), source.Symbol) {
			return nil
		}
	}

	base, quote, err := splitSymbolBaseQuote(source.Symbol)
	if err != nil {
		return fmt.Errorf("invalid source symbol %q: %w", source.Symbol, err)
	}
	if err := store.UpsertSymbol(models.Symbol{
		Exchange:   exchangeCfg.Name,
		Symbol:     source.Symbol,
		Base:       base,
		Quote:      quote,
		Type:       backTestAutoSymbolType,
		Timeframes: strings.TrimSpace(exchangeCfg.Timeframes),
		Active:     true,
	}); err != nil {
		return fmt.Errorf("auto add back-test symbol failed (exchange=%s symbol=%s): %w", source.Exchange, source.Symbol, err)
	}

	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("back-test source symbol auto added",
		zap.String("exchange", source.Exchange),
		zap.String("symbol", source.Symbol),
	)
	return nil
}

func splitSymbolBaseQuote(symbol string) (string, string, error) {
	parts := strings.Split(strings.TrimSpace(symbol), "/")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expect BASE/QUOTE")
	}
	base := strings.TrimSpace(parts[0])
	quote := strings.TrimSpace(parts[1])
	if base == "" || quote == "" {
		return "", "", fmt.Errorf("expect BASE/QUOTE")
	}
	return base, quote, nil
}

func (s *BackTestService) resetSummary() {
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	s.summaryReady = false
	s.summary = BackTestSummary{}
}

func (s *BackTestService) setSummary(summary BackTestSummary) {
	s.summaryMu.Lock()
	defer s.summaryMu.Unlock()
	s.summaryReady = true
	s.summary = summary
}

func (s *BackTestService) loadSeries(source BackTestSource) (map[string][]models.OHLCV, error) {
	switch source.Type {
	case sourceTypeExchange:
		if s.cfg.Fetcher == nil {
			return nil, errors.New("nil fetcher")
		}
		return s.loadExchangeSeries(source)
	case sourceTypeDB:
		return s.loadDBSeries(source)
	case sourceTypeCSV:
		return s.loadCSVSeries(source)
	default:
		return nil, fmt.Errorf("unsupported source type: %s", source.Type)
	}
}

func applyBackTestReplayStart(series map[string][]models.OHLCV, source BackTestSource) (map[string][]models.OHLCV, error) {
	_, replaySeries, err := splitBackTestReplaySeries(series, source, 0)
	if err != nil {
		return nil, err
	}
	return replaySeries, nil
}

func splitBackTestReplaySeries(
	series map[string][]models.OHLCV,
	source BackTestSource,
	historyBars int,
) (map[string][]models.OHLCV, map[string][]models.OHLCV, error) {
	if len(series) == 0 {
		return nil, series, nil
	}
	historyBars = normalizeBackTestHistoryBars(historyBars)
	replayStartMS := source.ReplayStart.UnixMilli()
	matched := false
	preloadSeries := make(map[string][]models.OHLCV, len(series))
	replaySeries := make(map[string][]models.OHLCV, len(series))
	for timeframe, data := range series {
		if len(data) == 0 {
			return nil, nil, fmt.Errorf("empty ohlcv data (exchange=%s symbol=%s timeframe=%s)", source.Exchange, source.Symbol, timeframe)
		}
		dur, ok := timeframeDuration(timeframe)
		if !ok {
			return nil, nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
		}
		step := dur.Milliseconds()
		warmupIdx := backTestWarmupStartIndex(data, source, step)
		if historyBars > 0 && warmupIdx < historyBars {
			return nil, nil, fmt.Errorf("insufficient history bars for warmup (source=%s exchange=%s symbol=%s timeframe=%s required=%d got=%d)",
				source.Type,
				source.Exchange,
				source.Symbol,
				timeframe,
				historyBars,
				warmupIdx,
			)
		}
		idx := backTestReplayStartIndex(data, source, step)
		if idx >= len(data) {
			replayLabel := formatTimestamp(replayStartMS)
			if !source.HasReplayTS {
				replayLabel = formatTimestamp(source.Start.UnixMilli())
			}
			return nil, nil, fmt.Errorf("replay start out of range (exchange=%s symbol=%s timeframe=%s replay_start=%s data_end=%s)",
				source.Exchange,
				source.Symbol,
				timeframe,
				replayLabel,
				formatTimestamp(normalizeTimestampMS(data[len(data)-1].TS)),
			)
		}
		trimmedReplay := data[idx:]
		if source.HasReplayTS && normalizeTimestampMS(trimmedReplay[0].TS) == replayStartMS {
			matched = true
		}
		if idx > 0 {
			preloadSeries[timeframe] = data[:idx]
		}
		replaySeries[timeframe] = trimmedReplay
	}
	if source.HasReplayTS && !matched {
		return nil, nil, fmt.Errorf("replay start is not a valid kline open time in selected timeframes (exchange=%s symbol=%s replay_start=%s timeframes=%s)",
			source.Exchange,
			source.Symbol,
			formatTimestamp(replayStartMS),
			strings.Join(source.Timeframes, "/"),
		)
	}
	return preloadSeries, replaySeries, nil
}

func backTestWarmupStartIndex(data []models.OHLCV, source BackTestSource, step int64) int {
	if len(data) == 0 {
		return 0
	}
	startMS := source.Start.UnixMilli()
	return sort.Search(len(data), func(i int) bool {
		ts := normalizeTimestampMS(data[i].TS)
		return ts+step > startMS
	})
}

func backTestReplayStartIndex(data []models.OHLCV, source BackTestSource, step int64) int {
	if len(data) == 0 {
		return 0
	}
	if source.HasReplayTS {
		replayStartMS := source.ReplayStart.UnixMilli()
		return sort.Search(len(data), func(i int) bool {
			return normalizeTimestampMS(data[i].TS) >= replayStartMS
		})
	}
	startMS := source.Start.UnixMilli()
	return sort.Search(len(data), func(i int) bool {
		ts := normalizeTimestampMS(data[i].TS)
		return ts+step > startMS
	})
}

func normalizeBackTestHistoryBars(historyBars int) int {
	if historyBars <= 0 {
		return 0
	}
	return historyBars
}

func (s *BackTestService) preload(
	handler iface.MarketHandler,
	source BackTestSource,
	series map[string][]models.OHLCV,
) error {
	if handler == nil || len(series) == 0 {
		return nil
	}
	type preloadPoint struct {
		timeframe string
		ohlcv     models.OHLCV
	}
	points := make([]preloadPoint, 0)
	for timeframe, data := range series {
		for _, item := range data {
			item.TS = normalizeTimestampMS(item.TS)
			if item.TS <= 0 {
				continue
			}
			points = append(points, preloadPoint{
				timeframe: timeframe,
				ohlcv:     item,
			})
		}
	}
	sort.Slice(points, func(i, j int) bool {
		if points[i].ohlcv.TS != points[j].ohlcv.TS {
			return points[i].ohlcv.TS < points[j].ohlcv.TS
		}
		return points[i].timeframe < points[j].timeframe
	})
	sourceTag := fmt.Sprintf("back-test-preload-%s", source.Type)
	for _, point := range points {
		if s.stopped.Load() {
			return errors.New("back-test stopped")
		}
		handler(models.MarketData{
			Exchange:  source.Exchange,
			Symbol:    source.Symbol,
			Timeframe: point.timeframe,
			OHLCV:     point.ohlcv,
			Closed:    true,
			Source:    sourceTag,
			Seq:       s.nextSeq(source.Exchange, source.Symbol, point.timeframe),
		})
	}
	return nil
}

func (s *BackTestService) loadExchangeSeries(source BackTestSource) (map[string][]models.OHLCV, error) {
	ctx := s.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	out := make(map[string][]models.OHLCV, len(source.Timeframes))
	for _, timeframe := range source.Timeframes {
		data, err := s.loadExchangeSeriesTimeframe(ctx, source, timeframe)
		if err != nil {
			return nil, err
		}
		out[timeframe] = data
	}
	return out, nil
}

type ohlcvMissingRange struct {
	startTS int64
	endTS   int64
}

func (s *BackTestService) loadExchangeSeriesTimeframe(ctx context.Context, source BackTestSource, timeframe string) ([]models.OHLCV, error) {
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return nil, fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}

	effectiveEnd, err := resolveBackTestEnd(source, timeframe, dur)
	if err != nil {
		return nil, err
	}
	queryStart, err := resolveBackTestQueryStart(source, timeframe, dur, normalizeBackTestHistoryBars(s.cfg.HistoryBars))
	if err != nil {
		return nil, err
	}

	merged, err := s.loadStoreOHLCVRange(source.Exchange, source.Symbol, timeframe, queryStart, effectiveEnd, dur)
	if err != nil {
		logger.Warn("back-test load ohlcv from store failed",
			zap.String("exchange", source.Exchange),
			zap.String("symbol", source.Symbol),
			zap.String("timeframe", timeframe),
			zap.Error(err),
		)
	}

	missing, err := missingOHLCVRanges(merged, queryStart, effectiveEnd, step)
	if err != nil {
		return nil, err
	}
	if len(missing) > 0 {
		logger.Info("back-test ohlcv gaps detected",
			zap.String("exchange", source.Exchange),
			zap.String("symbol", source.Symbol),
			zap.String("timeframe", timeframe),
			zap.Int("gap_count", len(missing)),
		)
	}

	fetchLimit := s.backTestOHLCVLimit(source.Exchange)
	for _, gap := range missing {
		chunks, err := splitOHLCVMissingRange(gap, step, fetchLimit)
		if err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			rangeStart := time.UnixMilli(chunk.startTS).UTC()
			rangeEnd := time.UnixMilli(chunk.endTS + step).UTC()
			fetched, err := s.cfg.Fetcher.FetchOHLCVRange(ctx, source.Exchange, source.Symbol, timeframe, rangeStart, rangeEnd)
			if err != nil {
				return nil, fmt.Errorf("fetch ohlcv gap failed (exchange=%s symbol=%s timeframe=%s range=%s-%s): %w",
					source.Exchange,
					source.Symbol,
					timeframe,
					formatTimestamp(chunk.startTS),
					formatTimestamp(chunk.endTS),
					err,
				)
			}
			merged = mergeAndNormalizeOHLCV(merged, fetched)
			s.persistBackTestFetchedOHLCV(source.Exchange, source.Symbol, timeframe, fetched, chunk.startTS, chunk.endTS)
		}
	}

	remaining, err := missingOHLCVRanges(merged, queryStart, effectiveEnd, step)
	if err != nil {
		return nil, err
	}
	if len(remaining) > 0 {
		firstGap := remaining[0]
		return nil, fmt.Errorf("ohlcv data gaps remain after refill (exchange=%s symbol=%s timeframe=%s first_gap=%s-%s gap_count=%d)",
			source.Exchange,
			source.Symbol,
			timeframe,
			formatTimestamp(firstGap.startTS),
			formatTimestamp(firstGap.endTS),
			len(remaining),
		)
	}

	filtered, err := filterAndValidateOHLCV(merged, source.Type, source.Exchange, source.Symbol, timeframe, queryStart, effectiveEnd)
	if err != nil {
		return nil, err
	}
	return filtered, nil
}

func (s *BackTestService) backTestOHLCVLimit(exchange string) int {
	if s == nil || s.cfg.Store == nil {
		return backTestDefaultOHLCVFetchLimit
	}
	items, err := s.cfg.Store.ListExchanges()
	if err != nil {
		logger := s.cfg.Logger
		if logger == nil {
			logger = glog.Nop()
		}
		logger.Warn("load exchanges for back-test fetch limit failed", zap.Error(err))
		return backTestDefaultOHLCVFetchLimit
	}
	for _, item := range items {
		if !strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(exchange)) {
			continue
		}
		if item.OHLCVLimit > 0 {
			return item.OHLCVLimit
		}
		break
	}
	return backTestDefaultOHLCVFetchLimit
}

func splitOHLCVMissingRange(gap ohlcvMissingRange, step int64, maxPerRequest int) ([]ohlcvMissingRange, error) {
	if step <= 0 {
		return nil, fmt.Errorf("invalid ohlcv step")
	}
	if gap.endTS < gap.startTS {
		return nil, fmt.Errorf("invalid ohlcv missing range")
	}
	if maxPerRequest <= 0 {
		maxPerRequest = backTestDefaultOHLCVFetchLimit
	}
	bars := int((gap.endTS-gap.startTS)/step) + 1
	if bars <= maxPerRequest {
		return []ohlcvMissingRange{gap}, nil
	}
	chunks := make([]ohlcvMissingRange, 0, bars/maxPerRequest+1)
	cursor := gap.startTS
	remaining := bars
	for remaining > 0 {
		chunkBars := maxPerRequest
		if remaining < chunkBars {
			chunkBars = remaining
		}
		chunkEnd := cursor + int64(chunkBars-1)*step
		chunks = append(chunks, ohlcvMissingRange{
			startTS: cursor,
			endTS:   chunkEnd,
		})
		cursor = chunkEnd + step
		remaining -= chunkBars
	}
	return chunks, nil
}

func (s *BackTestService) loadDBSeries(source BackTestSource) (map[string][]models.OHLCV, error) {
	if s == nil || s.cfg.Store == nil {
		return nil, errors.New("nil store")
	}
	out := make(map[string][]models.OHLCV, len(source.Timeframes))
	for _, timeframe := range source.Timeframes {
		data, err := s.loadDBSeriesTimeframe(source, timeframe)
		if err != nil {
			return nil, err
		}
		out[timeframe] = data
	}
	return out, nil
}

func (s *BackTestService) loadDBSeriesTimeframe(source BackTestSource, timeframe string) ([]models.OHLCV, error) {
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	effectiveEnd, err := resolveBackTestEnd(source, timeframe, dur)
	if err != nil {
		return nil, err
	}
	queryStart, err := resolveBackTestQueryStart(source, timeframe, dur, normalizeBackTestHistoryBars(s.cfg.HistoryBars))
	if err != nil {
		return nil, err
	}
	data, err := s.loadStoreOHLCVRange(source.Exchange, source.Symbol, timeframe, queryStart, effectiveEnd, dur)
	if err != nil {
		return nil, fmt.Errorf("load ohlcv from db failed (exchange=%s symbol=%s timeframe=%s): %w",
			source.Exchange, source.Symbol, timeframe, err)
	}
	filtered, err := filterAndValidateOHLCV(data, source.Type, source.Exchange, source.Symbol, timeframe, queryStart, effectiveEnd)
	if err != nil {
		return nil, err
	}
	return filtered, nil
}

func resolveBackTestEnd(source BackTestSource, timeframe string, duration time.Duration) (time.Time, error) {
	effectiveEnd := source.End
	if source.AutoEnd {
		effectiveEnd = source.End.Add(-duration)
		if !effectiveEnd.After(source.Start) {
			return time.Time{}, fmt.Errorf("auto end time must be after start time (exchange=%s symbol=%s timeframe=%s start=%s end=%s)",
				source.Exchange,
				source.Symbol,
				timeframe,
				source.Start.UTC().Format(time.RFC3339),
				effectiveEnd.UTC().Format(time.RFC3339),
			)
		}
	}
	return effectiveEnd, nil
}

func resolveBackTestQueryStart(source BackTestSource, timeframe string, duration time.Duration, historyBars int) (time.Time, error) {
	if duration <= 0 {
		return time.Time{}, fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}
	if historyBars <= 0 {
		return source.Start, nil
	}
	queryStartMS := source.Start.UnixMilli() - int64(historyBars)*duration.Milliseconds()
	return time.UnixMilli(queryStartMS).UTC(), nil
}

func (s *BackTestService) loadStoreOHLCVRange(exchange, symbol, timeframe string, start, end time.Time, step time.Duration) ([]models.OHLCV, error) {
	if s == nil || s.cfg.Store == nil {
		return nil, nil
	}
	queryStart := start.Add(-step)
	data, err := s.cfg.Store.ListOHLCVRange(exchange, symbol, timeframe, queryStart, end)
	if err != nil {
		return nil, err
	}
	return mergeAndNormalizeOHLCV(nil, data), nil
}

func (s *BackTestService) persistBackTestFetchedOHLCV(exchange, symbol, timeframe string, data []models.OHLCV, startTS, endTS int64) {
	if s == nil || s.cfg.Store == nil || len(data) == 0 {
		return
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	for _, item := range data {
		item.TS = normalizeTimestampMS(item.TS)
		if item.TS <= 0 {
			continue
		}
		if startTS > 0 && item.TS < startTS {
			continue
		}
		if endTS > 0 && item.TS > endTS {
			continue
		}
		saveErr := s.cfg.Store.SaveOHLCV(models.MarketData{
			Exchange:  exchange,
			Symbol:    symbol,
			Timeframe: timeframe,
			OHLCV:     item,
			Closed:    true,
			Source:    "back-test-exchange",
		})
		if saveErr != nil {
			logger.Warn("persist back-test fetched ohlcv failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Int64("ts", item.TS),
				zap.Error(saveErr),
			)
		}
	}
}

func mergeAndNormalizeOHLCV(base, extra []models.OHLCV) []models.OHLCV {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	merged := make(map[int64]models.OHLCV, len(base)+len(extra))
	appendData := func(items []models.OHLCV) {
		for _, item := range items {
			item.TS = normalizeTimestampMS(item.TS)
			if item.TS <= 0 {
				continue
			}
			merged[item.TS] = item
		}
	}
	appendData(base)
	appendData(extra)
	out := make([]models.OHLCV, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	return out
}

func missingOHLCVRanges(data []models.OHLCV, start, end time.Time, step int64) ([]ohlcvMissingRange, error) {
	if step <= 0 {
		return nil, fmt.Errorf("invalid ohlcv step")
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}
	firstTS := floorTimestamp(startMS, step)
	lastTS := floorTimestamp(endMS, step)
	if lastTS < firstTS {
		return nil, fmt.Errorf("invalid expected ohlcv window")
	}
	present := make(map[int64]bool, len(data))
	for _, item := range data {
		ts := normalizeTimestampMS(item.TS)
		if ts+step <= startMS || ts > endMS {
			continue
		}
		present[ts] = true
	}
	ranges := make([]ohlcvMissingRange, 0)
	inGap := false
	current := ohlcvMissingRange{}
	for ts := firstTS; ts <= lastTS; ts += step {
		if present[ts] {
			if inGap {
				ranges = append(ranges, current)
				inGap = false
			}
			continue
		}
		if !inGap {
			current = ohlcvMissingRange{startTS: ts, endTS: ts}
			inGap = true
			continue
		}
		current.endTS = ts
	}
	if inGap {
		ranges = append(ranges, current)
	}
	return ranges, nil
}

func floorTimestamp(ts, step int64) int64 {
	if step <= 0 {
		return ts
	}
	if ts >= 0 {
		return (ts / step) * step
	}
	return ((ts - step + 1) / step) * step
}

func (s *BackTestService) loadCSVSeries(source BackTestSource) (map[string][]models.OHLCV, error) {
	out := make(map[string][]models.OHLCV, len(source.Files))
	for _, file := range source.Files {
		if _, ok := out[file.Timeframe]; ok {
			return nil, fmt.Errorf("duplicate timeframe in csv: %s", file.Timeframe)
		}
		dur, ok := timeframeDuration(file.Timeframe)
		if !ok {
			return nil, fmt.Errorf("unsupported timeframe: %s", file.Timeframe)
		}
		effectiveEnd, err := resolveBackTestEnd(source, file.Timeframe, dur)
		if err != nil {
			return nil, err
		}
		queryStart, err := resolveBackTestQueryStart(source, file.Timeframe, dur, normalizeBackTestHistoryBars(s.cfg.HistoryBars))
		if err != nil {
			return nil, err
		}
		data, err := readCSV(file.Path)
		if err != nil {
			return nil, err
		}
		filtered, err := filterAndValidateOHLCV(data, source.Type, file.Exchange, file.Symbol, file.Timeframe, queryStart, effectiveEnd)
		if err != nil {
			return nil, err
		}
		out[file.Timeframe] = filtered
	}
	printBackTestCSVSources(source.Files)
	return out, nil
}

func (s *BackTestService) replay(handler iface.MarketHandler, source BackTestSource, series map[string][]models.OHLCV) error {
	if len(series) == 0 {
		return fmt.Errorf("empty series")
	}
	executionTimeframe, err := smallestBackTestTimeframe(source.Timeframes)
	if err != nil {
		return err
	}
	data, ok := series[executionTimeframe]
	if !ok || len(data) == 0 {
		return fmt.Errorf("missing execution timeframe series: %s", executionTimeframe)
	}
	dur, ok := timeframeDuration(executionTimeframe)
	if !ok || dur <= 0 {
		return fmt.Errorf("unsupported timeframe: %s", executionTimeframe)
	}
	cursor, err := newFeedCursor(source.Exchange, source.Symbol, executionTimeframe, data, 1, true, dur)
	if err != nil {
		return err
	}
	h := &feedHeap{}
	heap.Push(h, cursor)

	for h.Len() > 0 {
		if s.stopped.Load() {
			return errors.New("back-test stopped")
		}
		cursor := heap.Pop(h).(*feedCursor)
		event, done, err := cursor.next()
		if err != nil {
			return err
		}
		event.Seq = s.nextSeq(event.Exchange, event.Symbol, event.Timeframe)
		event.Source = fmt.Sprintf("back-test-%s", source.Type)
		handler(event)
		if !done {
			heap.Push(h, cursor)
		}
	}
	return nil
}

func (s *BackTestService) nextSeq(exchange, symbol, timeframe string) int64 {
	key := exchange + "|" + symbol + "|" + timeframe
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	s.seq[key]++
	return s.seq[key]
}

type feedCursor struct {
	exchange    string
	symbol      string
	timeframe   string
	steps       int
	emitAtClose bool
	durationMS  int64

	data  []models.OHLCV
	index int
	step  int

	prices []float64
	high   float64
	low    float64

	nextEventTS int64
}

func newFeedCursor(exchange, symbol, timeframe string, data []models.OHLCV, steps int, emitAtClose bool, duration time.Duration) (*feedCursor, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty ohlcv data")
	}
	if steps <= 0 {
		return nil, fmt.Errorf("invalid steps for timeframe %s", timeframe)
	}
	durationMS := duration.Milliseconds()
	if durationMS <= 0 {
		return nil, fmt.Errorf("invalid timeframe duration for %s", timeframe)
	}
	cursor := &feedCursor{
		exchange:    exchange,
		symbol:      symbol,
		timeframe:   timeframe,
		steps:       steps,
		emitAtClose: emitAtClose,
		durationMS:  durationMS,
		data:        data,
		index:       0,
		step:        0,
	}
	if err := cursor.initBar(); err != nil {
		return nil, err
	}
	return cursor, nil
}

func (c *feedCursor) initBar() error {
	if c.index >= len(c.data) {
		return nil
	}
	bar := c.data[c.index]
	if bar.High < bar.Low {
		return fmt.Errorf("invalid ohlcv range at %d", bar.TS)
	}
	if bar.Open < bar.Low || bar.Open > bar.High {
		return fmt.Errorf("open out of range at %d", bar.TS)
	}
	if bar.Close < bar.Low || bar.Close > bar.High {
		return fmt.Errorf("close out of range at %d", bar.TS)
	}

	var err error
	if c.steps == 1 {
		c.prices = []float64{bar.Close}
	} else {
		c.prices, err = buildMinutePrices(bar, c.steps)
		if err != nil {
			return err
		}
	}
	c.step = 0
	c.high = bar.Open
	c.low = bar.Open
	c.nextEventTS = bar.TS
	if c.emitAtClose {
		c.nextEventTS = bar.TS + c.durationMS - int64(time.Minute/time.Millisecond)
	}
	return nil
}

func (c *feedCursor) next() (models.MarketData, bool, error) {
	if c.index >= len(c.data) {
		return models.MarketData{}, true, nil
	}
	bar := c.data[c.index]
	if c.step >= len(c.prices) {
		return models.MarketData{}, true, fmt.Errorf("invalid step state")
	}

	price := c.prices[c.step]
	if c.step == 0 {
		c.high = bar.Open
		c.low = bar.Open
	}
	c.high = math.Max(c.high, price)
	c.low = math.Min(c.low, price)

	closed := c.step == c.steps-1
	volume := bar.Volume * float64(c.step+1) / float64(c.steps)
	high := c.high
	low := c.low
	closePx := price
	if closed {
		high = bar.High
		low = bar.Low
		closePx = bar.Close
		volume = bar.Volume
	}

	event := models.MarketData{
		Exchange:  c.exchange,
		Symbol:    c.symbol,
		Timeframe: c.timeframe,
		OHLCV: models.OHLCV{
			TS:     bar.TS,
			Open:   bar.Open,
			High:   high,
			Low:    low,
			Close:  closePx,
			Volume: volume,
		},
		Closed: closed,
	}

	c.step++
	if c.step >= c.steps {
		c.index++
		if c.index >= len(c.data) {
			return event, true, nil
		}
		if err := c.initBar(); err != nil {
			return event, true, err
		}
		return event, false, nil
	}
	c.nextEventTS = bar.TS + int64(c.step)*int64(time.Minute/time.Millisecond)
	return event, false, nil
}

func buildMinutePrices(bar models.OHLCV, steps int) ([]float64, error) {
	if steps <= 1 {
		return []float64{bar.Close}, nil
	}
	if bar.High < bar.Low {
		return nil, fmt.Errorf("invalid ohlcv range at %d", bar.TS)
	}
	if bar.High == bar.Low {
		out := make([]float64, steps)
		for i := range out {
			out[i] = bar.High
		}
		out[0] = bar.Open
		out[steps-1] = bar.Close
		return out, nil
	}

	out := make([]float64, steps)
	out[0] = bar.Open
	out[steps-1] = bar.Close

	if steps == 2 {
		return out, nil
	}
	if steps == 3 {
		if bar.Close >= bar.Open {
			out[1] = bar.High
		} else {
			out[1] = bar.Low
		}
		return out, nil
	}

	hiIndex := 1
	loIndex := steps - 2
	if bar.Close >= bar.Open {
		loIndex = 1
		hiIndex = steps - 2
	} else {
		hiIndex = 1
		loIndex = steps - 2
	}
	out[hiIndex] = bar.High
	out[loIndex] = bar.Low
	fillLinear(out, 0, 1)
	fillLinear(out, 1, steps-2)
	fillLinear(out, steps-2, steps-1)
	return out, nil
}

func smallestBackTestTimeframe(timeframes []string) (string, error) {
	best := ""
	bestDur := time.Duration(0)
	for _, timeframe := range timeframes {
		normalized := strings.ToLower(strings.TrimSpace(timeframe))
		if normalized == "" {
			continue
		}
		dur, ok := timeframeDuration(normalized)
		if !ok || dur <= 0 {
			return "", fmt.Errorf("unsupported timeframe: %s", timeframe)
		}
		if best == "" || dur < bestDur {
			best = normalized
			bestDur = dur
		}
	}
	if best == "" {
		return "", fmt.Errorf("empty timeframes")
	}
	return best, nil
}

func fillLinear(out []float64, startIdx, endIdx int) {
	if endIdx-startIdx <= 1 {
		return
	}
	start := out[startIdx]
	end := out[endIdx]
	gap := float64(endIdx - startIdx)
	for i := startIdx + 1; i < endIdx; i++ {
		pos := float64(i-startIdx) / gap
		out[i] = start + (end-start)*pos
	}
}

func seedFromSource(source string) int64 {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" {
		return 0
	}
	hasher := fnv.New64a()
	if _, err := hasher.Write([]byte(trimmed)); err != nil {
		return 0
	}
	return int64(hasher.Sum64())
}

func filterAndValidateOHLCV(data []models.OHLCV, sourceType, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	filtered, err := filterOHLCVByRange(data, start, end, timeframe)
	if err != nil {
		return nil, fmt.Errorf("back-test range filter failed (source=%s exchange=%s symbol=%s timeframe=%s range=%s): %w",
			sourceType, exchange, symbol, timeframe, formatRange(start, end), err)
	}
	if err := validateOHLCVSequence(filtered, timeframe); err != nil {
		return nil, err
	}
	return filtered, nil
}

func filterOHLCVByRange(data []models.OHLCV, start, end time.Time, timeframe string) ([]models.OHLCV, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty ohlcv data")
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return nil, fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}

	out := make([]models.OHLCV, 0, len(data))
	var minTS int64
	var maxTS int64
	for i, item := range data {
		ts := normalizeTimestampMS(item.TS)
		if i == 0 || ts < minTS {
			minTS = ts
		}
		if i == 0 || ts > maxTS {
			maxTS = ts
		}
		barStart := ts
		barEnd := ts + step
		if barEnd <= startMS || barStart > endMS {
			continue
		}
		item.TS = ts
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("ohlcv data out of range: range=[%d(%s),%d(%s)] data=[%d(%s),%d(%s)]",
			startMS, formatTimestamp(startMS),
			endMS, formatTimestamp(endMS),
			minTS, formatTimestamp(minTS),
			maxTS, formatTimestamp(maxTS),
		)
	}
	return out, nil
}

func validateOHLCVSequence(data []models.OHLCV, timeframe string) error {
	if len(data) == 0 {
		return fmt.Errorf("empty ohlcv data")
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}
	for i := 1; i < len(data); i++ {
		delta := data[i].TS - data[i-1].TS
		if delta != step {
			return fmt.Errorf("ohlcv not continuous at index %d: delta %d != %d", i, delta, step)
		}
	}
	return nil
}

func formatRange(start, end time.Time) string {
	return fmt.Sprintf("%s-%s", start.In(time.Local).Format("20060102_1504"), end.In(time.Local).Format("20060102_1504"))
}

func formatTimePoint(value time.Time, enabled bool) string {
	if !enabled || value.IsZero() {
		return ""
	}
	return value.In(time.Local).Format("20060102_1504")
}

func formatTimestamp(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).In(time.Local).Format("20060102_1504")
}

type feedHeap []*feedCursor

func (h feedHeap) Len() int { return len(h) }

func (h feedHeap) Less(i, j int) bool {
	if h[i].nextEventTS != h[j].nextEventTS {
		return h[i].nextEventTS < h[j].nextEventTS
	}
	if h[i].durationMS != h[j].durationMS {
		return h[i].durationMS < h[j].durationMS
	}
	return h[i].timeframe < h[j].timeframe
}

func (h feedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *feedHeap) Push(x any) {
	*h = append(*h, x.(*feedCursor))
}

func (h *feedHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
