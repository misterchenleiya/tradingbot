package market

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	coreexchange "github.com/misterchenleiya/tradingbot/exchange"
	glog "github.com/misterchenleiya/tradingbot/log"
)

type okxRangeTestTransport struct {
	roundTrip func(req *http.Request) (*http.Response, error)
}

func (t okxRangeTestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return t.roundTrip(req)
}

func TestFetchOKXOHLCVRangeContinuesAfterPartialWindowOverlap(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 25, 6, 10, 0, 0, time.UTC)
	end := time.Date(2026, 3, 25, 6, 12, 0, 0, time.UTC)
	partial := okxRangeTestRows(
		start.Add(1*time.Minute).UnixMilli(),
		start.Add(2*time.Minute).UnixMilli(),
	)
	full := okxRangeTestRows(
		start.UnixMilli(),
		start.Add(1*time.Minute).UnixMilli(),
		start.Add(2*time.Minute).UnixMilli(),
	)

	requests := 0
	client := &http.Client{
		Transport: okxRangeTestTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				requests++
				query := req.URL.Query()
				limit := query.Get("limit")
				if limit == "300" && strings.Contains(req.URL.Path, okxHistoryCandlesEndpoint) {
					return okxRangeTestResponse(full), nil
				}
				return okxRangeTestResponse(partial), nil
			},
		},
	}

	data, err := fetchOKXOHLCVRange(context.Background(), client, glog.Nop(), nil, "okx", "SHELL/USDT", "1m", start, end)
	if err != nil {
		t.Fatalf("fetchOKXOHLCVRange returned error: %v", err)
	}
	if !ohlcvCoversRequestedRange(data, "1m", start, end) {
		t.Fatalf("expected full coverage after fallback, got %+v", data)
	}
	if requests < 2 {
		t.Fatalf("expected partial overlap to trigger further fallback attempts, got requests=%d", requests)
	}
}

func TestFetchOKXOHLCVRangeFailsWhenAllFallbacksRemainPartial(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 3, 25, 6, 10, 0, 0, time.UTC)
	end := time.Date(2026, 3, 25, 6, 12, 0, 0, time.UTC)
	partial := okxRangeTestRows(
		start.Add(1*time.Minute).UnixMilli(),
		start.Add(2*time.Minute).UnixMilli(),
	)

	client := &http.Client{
		Transport: okxRangeTestTransport{
			roundTrip: func(req *http.Request) (*http.Response, error) {
				return okxRangeTestResponse(partial), nil
			},
		},
	}

	_, err := fetchOKXOHLCVRange(context.Background(), client, glog.Nop(), nil, "okx", "SHELL/USDT", "1m", start, end)
	if err == nil {
		t.Fatalf("expected incomplete range coverage error")
	}
	if !strings.Contains(err.Error(), "incomplete data coverage") {
		t.Fatalf("expected incomplete coverage error, got %v", err)
	}
}

func okxRangeTestRows(timestamps ...int64) [][]string {
	rows := make([][]string, 0, len(timestamps))
	for idx, ts := range timestamps {
		price := 100.0 + float64(idx)
		rows = append(rows, []string{
			fmt.Sprintf("%d", ts),
			fmt.Sprintf("%.2f", price),
			fmt.Sprintf("%.2f", price+1),
			fmt.Sprintf("%.2f", price-1),
			fmt.Sprintf("%.2f", price+0.5),
			"10",
		})
	}
	return rows
}

func okxRangeTestResponse(rows [][]string) *http.Response {
	payload := fmt.Sprintf(`{"code":"0","data":%s}`, okxRangeTestRowsJSON(rows))
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(payload)),
		Header:     make(http.Header),
	}
}

func okxRangeTestRowsJSON(rows [][]string) string {
	if len(rows) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf(`["%s","%s","%s","%s","%s","%s"]`, row[0], row[1], row[2], row[3], row[4], row[5]))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

type historyRangeCaptureSource struct {
	lastStart time.Time
	lastEnd   time.Time
}

func (s *historyRangeCaptureSource) FetchLatestOHLCV(context.Context, string, string) (coreexchange.OHLCV, error) {
	return coreexchange.OHLCV{}, fmt.Errorf("not implemented")
}

func (s *historyRangeCaptureSource) FetchOHLCVRange(_ context.Context, _ string, timeframe string, start, end time.Time) ([]coreexchange.OHLCV, error) {
	s.lastStart = start
	s.lastEnd = end
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	if !end.After(start) {
		return nil, fmt.Errorf("invalid range")
	}
	step := dur.Milliseconds()
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	rows := make([]coreexchange.OHLCV, 0)
	for ts := startMS; ts < endMS; ts += step {
		rows = append(rows, coreexchange.OHLCV{
			TS:     ts,
			Open:   1,
			High:   2,
			Low:    0.5,
			Close:  1.5,
			Volume: 10,
		})
	}
	return rows, nil
}

func (s *historyRangeCaptureSource) LoadPerpUSDTMarkets(context.Context) ([]coreexchange.MarketSymbol, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *historyRangeCaptureSource) FetchDailyVolumesUSDT(context.Context, string, int) ([]float64, error) {
	return nil, fmt.Errorf("not implemented")
}

func TestFetchOHLCVByLimitPaged_ClosedOnlyUsesLatestClosedBucket(t *testing.T) {
	t.Parallel()

	source := &historyRangeCaptureSource{}
	fetcher := &HTTPFetcher{
		Exchanges: map[string]coreexchange.MarketDataSource{
			"okx": source,
		},
		FetchUnclosedOHLCV: false,
		Logger:             glog.Nop(),
	}

	limit := 5
	dur := time.Minute
	now := time.Now().UTC()
	currentStart := now.Truncate(dur)

	data, err := fetcher.FetchOHLCVByLimitPaged(context.Background(), "okx", "BTC/USDT", "1m", limit, 300)
	if err != nil {
		t.Fatalf("FetchOHLCVByLimitPaged returned error: %v", err)
	}
	if len(data) != limit {
		t.Fatalf("unexpected data length: got=%d want=%d", len(data), limit)
	}

	wantEnd := currentStart
	if !source.lastEnd.Equal(wantEnd) {
		t.Fatalf("unexpected source end: got=%s want=%s", source.lastEnd.UTC().Format(time.RFC3339), wantEnd.UTC().Format(time.RFC3339))
	}
	wantStart := currentStart.Add(-time.Duration(limit) * dur)
	if !source.lastStart.Equal(wantStart) {
		t.Fatalf("unexpected source start: got=%s want=%s", source.lastStart.UTC().Format(time.RFC3339), wantStart.UTC().Format(time.RFC3339))
	}
}

func TestFetchOHLCVByLimitPaged_WithUnclosedUsesCurrentBucket(t *testing.T) {
	t.Parallel()

	source := &historyRangeCaptureSource{}
	fetcher := &HTTPFetcher{
		Exchanges: map[string]coreexchange.MarketDataSource{
			"okx": source,
		},
		FetchUnclosedOHLCV: true,
		Logger:             glog.Nop(),
	}

	limit := 5
	dur := time.Minute
	now := time.Now().UTC()
	currentStart := now.Truncate(dur)

	data, err := fetcher.FetchOHLCVByLimitPaged(context.Background(), "okx", "BTC/USDT", "1m", limit, 300)
	if err != nil {
		t.Fatalf("FetchOHLCVByLimitPaged returned error: %v", err)
	}
	if len(data) != limit {
		t.Fatalf("unexpected data length: got=%d want=%d", len(data), limit)
	}

	wantEnd := currentStart.Add(dur)
	if !source.lastEnd.Equal(wantEnd) {
		t.Fatalf("unexpected source end: got=%s want=%s", source.lastEnd.UTC().Format(time.RFC3339), wantEnd.UTC().Format(time.RFC3339))
	}
	wantStart := currentStart.Add(-time.Duration(limit-1) * dur)
	if !source.lastStart.Equal(wantStart) {
		t.Fatalf("unexpected source start: got=%s want=%s", source.lastStart.UTC().Format(time.RFC3339), wantStart.UTC().Format(time.RFC3339))
	}
}
