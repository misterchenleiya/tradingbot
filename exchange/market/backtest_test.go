package market

import (
	"container/heap"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestResolveBackTestEndWithExplicitEnd(t *testing.T) {
	source := BackTestSource{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Start:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC),
		AutoEnd:  false,
	}
	got, err := resolveBackTestEnd(source, "1h", time.Hour)
	if err != nil {
		t.Fatalf("resolveBackTestEnd returned error: %v", err)
	}
	if !got.Equal(source.End) {
		t.Fatalf("unexpected end: got %s want %s", got, source.End)
	}
}

func TestResolveBackTestEndAutoEnd(t *testing.T) {
	source := BackTestSource{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Start:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 1, 2, 10, 7, 0, 0, time.UTC),
		AutoEnd:  true,
	}
	got, err := resolveBackTestEnd(source, "4h", 4*time.Hour)
	if err != nil {
		t.Fatalf("resolveBackTestEnd returned error: %v", err)
	}
	want := time.Date(2026, 1, 2, 6, 7, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("unexpected end: got %s want %s", got, want)
	}
}

func TestResolveBackTestEndAutoEndRequiresEndAfterStart(t *testing.T) {
	source := BackTestSource{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Start:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		End:      time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC),
		AutoEnd:  true,
	}
	_, err := resolveBackTestEnd(source, "1h", time.Hour)
	if err == nil {
		t.Fatalf("expected error for auto end <= start")
	}
}

func TestResolveBackTestQueryStartWithReplayStart(t *testing.T) {
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "BTC/USDT",
		Start:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ReplayStart: time.Date(2026, 1, 3, 12, 0, 0, 0, time.UTC),
		HasReplayTS: true,
	}
	got, err := resolveBackTestQueryStart(source, "1h", time.Hour, 10)
	if err != nil {
		t.Fatalf("resolveBackTestQueryStart returned error: %v", err)
	}
	want := source.Start.Add(-10 * time.Hour)
	if !got.Equal(want) {
		t.Fatalf("unexpected query start: got %s want %s", got, want)
	}
}

func TestResolveBackTestQueryStartWithoutReplayStartUsesSourceStartAnchor(t *testing.T) {
	source := BackTestSource{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Start:    time.Date(2026, 1, 1, 12, 5, 0, 0, time.UTC),
	}
	got, err := resolveBackTestQueryStart(source, "15m", 15*time.Minute, 2)
	if err != nil {
		t.Fatalf("resolveBackTestQueryStart returned error: %v", err)
	}
	want := time.Date(2026, 1, 1, 11, 35, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("unexpected query start: got %s want %s", got, want)
	}
}

func TestEnsureExchangeSourceSymbolConfiguredExchangeMissing(t *testing.T) {
	store := &backTestSymbolStoreStub{
		exchanges: []models.Exchange{
			{Name: "okx"},
		},
	}
	source := BackTestSource{
		Type:     sourceTypeExchange,
		Exchange: "bitget",
		Symbol:   "ASTER/USDT",
	}

	err := ensureExchangeSourceSymbolConfigured(store, source, nil)
	if err == nil {
		t.Fatalf("expected exchange missing error")
	}
	if !strings.Contains(err.Error(), "exchange not configured in exchanges table") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.upserted) != 0 {
		t.Fatalf("unexpected upsert when exchange missing")
	}
}

func TestEnsureExchangeSourceSymbolConfiguredAutoAddSymbol(t *testing.T) {
	store := &backTestSymbolStoreStub{
		exchanges: []models.Exchange{
			{Name: "OKX", Timeframes: `["5m","15m","1h"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT"},
		},
	}
	source := BackTestSource{
		Type:     sourceTypeExchange,
		Exchange: "okx",
		Symbol:   "ASTER/USDT",
	}

	if err := ensureExchangeSourceSymbolConfigured(store, source, nil); err != nil {
		t.Fatalf("ensureExchangeSourceSymbolConfigured returned error: %v", err)
	}
	if len(store.upserted) != 1 {
		t.Fatalf("unexpected upsert count: %d", len(store.upserted))
	}
	got := store.upserted[0]
	if got.Exchange != "OKX" || got.Symbol != "ASTER/USDT" {
		t.Fatalf("unexpected symbol upserted: %+v", got)
	}
	if got.Base != "ASTER" || got.Quote != "USDT" {
		t.Fatalf("unexpected base/quote: %+v", got)
	}
	if got.Type != backTestAutoSymbolType {
		t.Fatalf("unexpected type: %s", got.Type)
	}
	if got.Timeframes != `["5m","15m","1h"]` {
		t.Fatalf("unexpected timeframes: %s", got.Timeframes)
	}
	if !got.Active {
		t.Fatalf("auto-added symbol should be active")
	}
}

func TestEnsureExchangeSourceSymbolConfiguredNoUpsertWhenSymbolExists(t *testing.T) {
	store := &backTestSymbolStoreStub{
		exchanges: []models.Exchange{
			{Name: "okx"},
		},
		symbols: []models.Symbol{
			{Exchange: "OKX", Symbol: "aster/usdt"},
		},
	}
	source := BackTestSource{
		Type:     sourceTypeExchange,
		Exchange: "okx",
		Symbol:   "ASTER/USDT",
	}

	if err := ensureExchangeSourceSymbolConfigured(store, source, nil); err != nil {
		t.Fatalf("ensureExchangeSourceSymbolConfigured returned error: %v", err)
	}
	if len(store.upserted) != 0 {
		t.Fatalf("unexpected upsert when symbol already exists")
	}
}

func TestEnsureExchangeSourceSymbolConfiguredPropagatesUpsertError(t *testing.T) {
	store := &backTestSymbolStoreStub{
		exchanges: []models.Exchange{
			{Name: "okx"},
		},
		upsertErr: errors.New("write failed"),
	}
	source := BackTestSource{
		Type:     sourceTypeExchange,
		Exchange: "okx",
		Symbol:   "ASTER/USDT",
	}

	err := ensureExchangeSourceSymbolConfigured(store, source, nil)
	if err == nil {
		t.Fatalf("expected upsert error")
	}
	if !strings.Contains(err.Error(), "auto add back-test symbol failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSplitOHLCVMissingRangeNoSplit(t *testing.T) {
	gap := ohlcvMissingRange{
		startTS: 0,
		endTS:   4 * 60 * 1000,
	}
	chunks, err := splitOHLCVMissingRange(gap, 60*1000, 10)
	if err != nil {
		t.Fatalf("splitOHLCVMissingRange returned error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("unexpected chunk count: %d", len(chunks))
	}
	if chunks[0] != gap {
		t.Fatalf("unexpected chunk: %+v", chunks[0])
	}
}

func TestSplitOHLCVMissingRangePaged(t *testing.T) {
	gap := ohlcvMissingRange{
		startTS: 0,
		endTS:   9 * 60 * 1000,
	}
	chunks, err := splitOHLCVMissingRange(gap, 60*1000, 4)
	if err != nil {
		t.Fatalf("splitOHLCVMissingRange returned error: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("unexpected chunk count: %d", len(chunks))
	}
	expected := []ohlcvMissingRange{
		{startTS: 0, endTS: 3 * 60 * 1000},
		{startTS: 4 * 60 * 1000, endTS: 7 * 60 * 1000},
		{startTS: 8 * 60 * 1000, endTS: 9 * 60 * 1000},
	}
	for idx := range expected {
		if chunks[idx] != expected[idx] {
			t.Fatalf("unexpected chunk[%d]: got %+v want %+v", idx, chunks[idx], expected[idx])
		}
	}
}

func TestSplitOHLCVMissingRangeInvalid(t *testing.T) {
	_, err := splitOHLCVMissingRange(ohlcvMissingRange{startTS: 2, endTS: 1}, 60*1000, 300)
	if err == nil {
		t.Fatalf("expected invalid range error")
	}
}

func TestApplyBackTestReplayStartTrimsSeries(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(15*time.Minute/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"15m", "1h"},
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"15m": {
			{TS: base},
			{TS: base + int64(15*time.Minute/time.Millisecond)},
			{TS: base + int64(30*time.Minute/time.Millisecond)},
		},
		"1h": {
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
			{TS: base + int64(2*time.Hour/time.Millisecond)},
		},
	}

	got, err := applyBackTestReplayStart(series, source)
	if err != nil {
		t.Fatalf("applyBackTestReplayStart returned error: %v", err)
	}
	if len(got["15m"]) != 2 || got["15m"][0].TS != replayStart {
		t.Fatalf("unexpected 15m replay trim: %+v", got["15m"])
	}
	if len(got["1h"]) != 2 || got["1h"][0].TS != base+int64(1*time.Hour/time.Millisecond) {
		t.Fatalf("unexpected 1h replay trim: %+v", got["1h"])
	}
}

func TestBackTestReplay_UsesOnlySmallestTimeframeSeries(t *testing.T) {
	svc := NewBackTestService(BackTestConfig{})
	svc.seq = make(map[string]int64)
	source := BackTestSource{
		Type:       sourceTypeExchange,
		Exchange:   "okx",
		Symbol:     "BTC/USDT",
		Timeframes: []string{"3m", "15m", "1h"},
	}
	series := map[string][]models.OHLCV{
		"3m": {
			{TS: 900000, Open: 100, High: 101, Low: 99, Close: 100, Volume: 1},
			{TS: 1080000, Open: 100, High: 102, Low: 99, Close: 101, Volume: 1},
		},
		"15m": {
			{TS: 900000, Open: 100, High: 103, Low: 98, Close: 102, Volume: 5},
		},
		"1h": {
			{TS: 0, Open: 100, High: 104, Low: 97, Close: 103, Volume: 20},
		},
	}

	var seen []string
	handler := func(data models.MarketData) {
		seen = append(seen, data.Timeframe)
		if !data.Closed {
			t.Fatalf("expected replay events to be closed, got open event for %s", data.Timeframe)
		}
	}

	if err := svc.replay(handler, source, series); err != nil {
		t.Fatalf("replay returned error: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 replay events, got %d: %v", len(seen), seen)
	}
	for _, timeframe := range seen {
		if timeframe != "3m" {
			t.Fatalf("expected only 3m replay events, got %v", seen)
		}
	}
}

func TestSplitBackTestReplaySeriesBuildsPreloadAndReplay(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(15*time.Minute/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"15m", "1h"},
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"15m": {
			{TS: base},
			{TS: base + int64(15*time.Minute/time.Millisecond)},
			{TS: base + int64(30*time.Minute/time.Millisecond)},
		},
		"1h": {
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
			{TS: base + int64(2*time.Hour/time.Millisecond)},
		},
	}

	preload, replay, err := splitBackTestReplaySeries(series, source, 0)
	if err != nil {
		t.Fatalf("splitBackTestReplaySeries returned error: %v", err)
	}
	if len(preload["15m"]) != 1 || preload["15m"][0].TS != base {
		t.Fatalf("unexpected 15m preload: %+v", preload["15m"])
	}
	if len(preload["1h"]) != 1 || preload["1h"][0].TS != base {
		t.Fatalf("unexpected 1h preload: %+v", preload["1h"])
	}
	if len(replay["15m"]) != 2 || replay["15m"][0].TS != replayStart {
		t.Fatalf("unexpected 15m replay: %+v", replay["15m"])
	}
	if len(replay["1h"]) != 2 || replay["1h"][0].TS != base+int64(1*time.Hour/time.Millisecond) {
		t.Fatalf("unexpected 1h replay: %+v", replay["1h"])
	}
}

func TestSplitBackTestReplaySeriesWithHistoryBarsWithoutReplayStart(t *testing.T) {
	base := int64(1700000000000)
	source := BackTestSource{
		Exchange:   "okx",
		Symbol:     "SOL/USDT",
		Timeframes: []string{"1h"},
		Start:      time.UnixMilli(base + int64(5*time.Minute/time.Millisecond)).UTC(),
	}
	series := map[string][]models.OHLCV{
		"1h": {
			{TS: base - int64(2*time.Hour/time.Millisecond)},
			{TS: base - int64(1*time.Hour/time.Millisecond)},
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
		},
	}

	preload, replay, err := splitBackTestReplaySeries(series, source, 2)
	if err != nil {
		t.Fatalf("splitBackTestReplaySeries returned error: %v", err)
	}
	if len(preload["1h"]) != 2 || preload["1h"][0].TS != base-int64(2*time.Hour/time.Millisecond) {
		t.Fatalf("unexpected preload: %+v", preload["1h"])
	}
	if len(replay["1h"]) != 2 || replay["1h"][0].TS != base {
		t.Fatalf("unexpected replay: %+v", replay["1h"])
	}
}

func TestSplitBackTestReplaySeriesWithReplayStartRequiresHistoryBars(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(2*time.Hour/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"1h"},
		Start:       time.UnixMilli(base).UTC(),
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"1h": {
			{TS: base - int64(2*time.Hour/time.Millisecond)},
			{TS: base - int64(1*time.Hour/time.Millisecond)},
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
			{TS: replayStart},
		},
	}

	preload, replay, err := splitBackTestReplaySeries(series, source, 2)
	if err != nil {
		t.Fatalf("splitBackTestReplaySeries returned error: %v", err)
	}
	if len(preload["1h"]) != 4 {
		t.Fatalf("unexpected preload count: %+v", preload["1h"])
	}
	if len(replay["1h"]) != 1 || replay["1h"][0].TS != replayStart {
		t.Fatalf("unexpected replay: %+v", replay["1h"])
	}
}

func TestSplitBackTestReplaySeriesWithReplayStartCountsHistoryBarsFromSourceStart(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(3*time.Hour/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"1h"},
		Start:       time.UnixMilli(base).UTC(),
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"1h": {
			{TS: base - int64(1*time.Hour/time.Millisecond)},
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
			{TS: base + int64(2*time.Hour/time.Millisecond)},
			{TS: replayStart},
		},
	}

	_, _, err := splitBackTestReplaySeries(series, source, 2)
	if err == nil {
		t.Fatalf("expected insufficient history bars error")
	}
	if !strings.Contains(err.Error(), "insufficient history bars for warmup") {
		t.Fatalf("unexpected error: %v", err)
	}
}
func TestSplitBackTestReplaySeriesRejectsInsufficientHistoryBars(t *testing.T) {
	base := int64(1700000000000)
	source := BackTestSource{
		Exchange:   "okx",
		Symbol:     "SOL/USDT",
		Timeframes: []string{"1h"},
		Start:      time.UnixMilli(base + int64(5*time.Minute/time.Millisecond)).UTC(),
	}
	series := map[string][]models.OHLCV{
		"1h": {
			{TS: base - int64(1*time.Hour/time.Millisecond)},
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
		},
	}

	_, _, err := splitBackTestReplaySeries(series, source, 2)
	if err == nil {
		t.Fatalf("expected insufficient history bars error")
	}
	if !strings.Contains(err.Error(), "insufficient history bars for warmup") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyBackTestReplayStartRejectsInvalidOpenTime(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(5*time.Minute/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"15m", "1h"},
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"15m": {
			{TS: base},
			{TS: base + int64(15*time.Minute/time.Millisecond)},
			{TS: base + int64(30*time.Minute/time.Millisecond)},
		},
		"1h": {
			{TS: base},
			{TS: base + int64(1*time.Hour/time.Millisecond)},
			{TS: base + int64(2*time.Hour/time.Millisecond)},
		},
	}
	_, err := applyBackTestReplayStart(series, source)
	if err == nil {
		t.Fatalf("expected invalid replay open time error")
	}
	if !strings.Contains(err.Error(), "valid kline open time") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestApplyBackTestReplayStartRejectsOutOfRange(t *testing.T) {
	base := int64(1700000000000)
	replayStart := base + int64(6*time.Hour/time.Millisecond)
	source := BackTestSource{
		Exchange:    "okx",
		Symbol:      "SOL/USDT",
		Timeframes:  []string{"15m"},
		ReplayStart: time.UnixMilli(replayStart).UTC(),
		HasReplayTS: true,
	}
	series := map[string][]models.OHLCV{
		"15m": {
			{TS: base},
			{TS: base + int64(15*time.Minute/time.Millisecond)},
			{TS: base + int64(30*time.Minute/time.Millisecond)},
		},
	}
	_, err := applyBackTestReplayStart(series, source)
	if err == nil {
		t.Fatalf("expected replay out-of-range error")
	}
	if !strings.Contains(err.Error(), "replay start out of range") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewFeedCursor_NonExecutionTimeframeEmitsSingleClosedEventAtBarClose(t *testing.T) {
	base := int64(1700000000000)
	bar := models.OHLCV{
		TS:     base,
		Open:   100,
		High:   110,
		Low:    95,
		Close:  108,
		Volume: 12,
	}
	cursor, err := newFeedCursor("okx", "BTC/USDT", "15m", []models.OHLCV{bar}, 1, true, 15*time.Minute)
	if err != nil {
		t.Fatalf("newFeedCursor returned error: %v", err)
	}
	wantEventTS := base + int64(14*time.Minute/time.Millisecond)
	if cursor.nextEventTS != wantEventTS {
		t.Fatalf("unexpected close event ts: got %d want %d", cursor.nextEventTS, wantEventTS)
	}

	event, done, err := cursor.next()
	if err != nil {
		t.Fatalf("cursor.next returned error: %v", err)
	}
	if !done {
		t.Fatalf("expected single-step non-execution timeframe cursor to finish after one event")
	}
	if !event.Closed {
		t.Fatalf("expected single-step non-execution timeframe event to be closed")
	}
	if event.OHLCV.TS != base {
		t.Fatalf("unexpected bar open timestamp: got %d want %d", event.OHLCV.TS, base)
	}
	if event.OHLCV.High != bar.High || event.OHLCV.Low != bar.Low || event.OHLCV.Close != bar.Close {
		t.Fatalf("unexpected ohlcv payload: got %+v want %+v", event.OHLCV, bar)
	}
}

func TestFeedHeapSameTimestampPrefersSmallerTimeframe(t *testing.T) {
	base := int64(1700000000000)
	h := &feedHeap{}
	heap.Push(h, &feedCursor{timeframe: "1h", durationMS: int64(time.Hour / time.Millisecond), nextEventTS: base})
	heap.Push(h, &feedCursor{timeframe: "15m", durationMS: int64(15 * time.Minute / time.Millisecond), nextEventTS: base})
	heap.Push(h, &feedCursor{timeframe: "3m", durationMS: int64(3 * time.Minute / time.Millisecond), nextEventTS: base})

	got := heap.Pop(h).(*feedCursor).timeframe
	if got != "3m" {
		t.Fatalf("unexpected first timeframe popped: got %s want %s", got, "3m")
	}
	got = heap.Pop(h).(*feedCursor).timeframe
	if got != "15m" {
		t.Fatalf("unexpected second timeframe popped: got %s want %s", got, "15m")
	}
}

type backTestSymbolStoreStub struct {
	exchanges []models.Exchange
	symbols   []models.Symbol
	upserted  []models.Symbol
	upsertErr error
}

func (s *backTestSymbolStoreStub) ListExchanges() ([]models.Exchange, error) {
	return append([]models.Exchange(nil), s.exchanges...), nil
}

func (s *backTestSymbolStoreStub) ListSymbols() ([]models.Symbol, error) {
	return append([]models.Symbol(nil), s.symbols...), nil
}

func (s *backTestSymbolStoreStub) UpsertSymbol(sym models.Symbol) error {
	if s.upsertErr != nil {
		return s.upsertErr
	}
	s.upserted = append(s.upserted, sym)
	return nil
}
