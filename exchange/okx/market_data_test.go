package okx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestLoadPerpUSDTMarketsUsesSettleCcyAndInstIDFallback(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/api/v5/public/instruments" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		}
		if got := req.URL.Query().Get("instType"); got != "SWAP" {
			t.Fatalf("unexpected instType: %s", got)
		}
		payload := map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]string{
				{
					"instId":    "BTC-USDT-SWAP",
					"baseCcy":   "BTC",
					"quoteCcy":  "",
					"settleCcy": "USDT",
					"state":     "live",
					"listTime":  "1774262400000",
				},
				{
					"instId":    "DOGE-USDT-SWAP",
					"baseCcy":   "",
					"quoteCcy":  "",
					"settleCcy": "USDT",
					"state":     "live",
					"listTime":  "1774348800000",
				},
				{
					"instId":    "BTC-USD-SWAP",
					"baseCcy":   "BTC",
					"quoteCcy":  "",
					"settleCcy": "USD",
					"state":     "live",
				},
			},
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode payload failed: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(raw))),
			Header:     make(http.Header),
		}, nil
	})

	client := &Client{
		baseURL: "https://okx.test",
		client: &http.Client{
			Transport: transport,
		},
	}
	markets, err := client.LoadPerpUSDTMarkets(context.Background())
	if err != nil {
		t.Fatalf("LoadPerpUSDTMarkets failed: %v", err)
	}

	if len(markets) != 2 {
		t.Fatalf("unexpected markets count: got=%d want=2", len(markets))
	}
	if markets[0].Symbol != "BTC/USDT" || markets[0].Base != "BTC" || markets[0].Quote != "USDT" {
		t.Fatalf("unexpected first market: %+v", markets[0])
	}
	if markets[0].ListTime != 1774262400000 {
		t.Fatalf("unexpected first market listTime: %+v", markets[0])
	}
	if markets[1].Symbol != "DOGE/USDT" || markets[1].Base != "DOGE" || markets[1].Quote != "USDT" {
		t.Fatalf("unexpected second market: %+v", markets[1])
	}
	if markets[1].ListTime != 1774348800000 {
		t.Fatalf("unexpected second market listTime: %+v", markets[1])
	}
}

func TestFetchSymbolListTime(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/api/v5/public/instruments" {
			t.Fatalf("unexpected path: %s", req.URL.Path)
		}
		if got := req.URL.Query().Get("instType"); got != "SWAP" {
			t.Fatalf("unexpected instType: %s", got)
		}
		if got := req.URL.Query().Get("instId"); got != "AZTEC-USDT-SWAP" {
			t.Fatalf("unexpected instId: %s", got)
		}
		payload := map[string]any{
			"code": "0",
			"msg":  "",
			"data": []map[string]string{
				{"listTime": "1770796800000"},
			},
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("encode payload failed: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(raw))),
			Header:     make(http.Header),
		}, nil
	})

	client := &Client{
		baseURL: "https://okx.test",
		client: &http.Client{
			Transport: transport,
		},
	}
	listTime, err := client.FetchSymbolListTime(context.Background(), "AZTEC/USDT.P")
	if err != nil {
		t.Fatalf("FetchSymbolListTime failed: %v", err)
	}
	if listTime != 1770796800000 {
		t.Fatalf("unexpected listTime: got=%d want=%d", listTime, int64(1770796800000))
	}
}

func TestFetchOHLCVRangeByCursorBeforeUsesAfterParam(t *testing.T) {
	const (
		startMS = int64(1700000000000)
		endMS   = int64(1700021600000)
		barTS   = int64(1700018000000)
	)
	call := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		call++
		query := req.URL.Query()
		switch call {
		case 1:
			if got, want := query.Get("after"), strconv.FormatInt(endMS+1, 10); got != want {
				t.Fatalf("first request after mismatch: got=%s want=%s", got, want)
			}
			if got := query.Get("before"); got != "" {
				t.Fatalf("first request before should be empty, got=%s", got)
			}
			return okxTestHTTPResponse(t, [][]string{{strconv.FormatInt(barTS, 10), "1", "2", "0.5", "1.5", "100"}}), nil
		case 2:
			if got, want := query.Get("after"), strconv.FormatInt(barTS-1, 10); got != want {
				t.Fatalf("second request after mismatch: got=%s want=%s", got, want)
			}
			if got := query.Get("before"); got != "" {
				t.Fatalf("second request before should be empty, got=%s", got)
			}
			return okxTestHTTPResponse(t, nil), nil
		default:
			t.Fatalf("unexpected request count: %d", call)
			return nil, nil
		}
	})

	client := &Client{
		baseURL: "https://okx.test",
		client: &http.Client{
			Transport: transport,
		},
	}
	rows, err := client.fetchOHLCVRangeByCursor(
		context.Background(),
		"/api/v5/market/history-candles",
		"BTC-USDT-SWAP",
		"1H",
		startMS,
		endMS,
		300,
		"before",
		false,
	)
	if err != nil {
		t.Fatalf("fetchOHLCVRangeByCursor returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows length: got=%d want=1", len(rows))
	}
	if rows[0].TS != barTS {
		t.Fatalf("unexpected ts: got=%d want=%d", rows[0].TS, barTS)
	}
}

func TestFetchOHLCVRangeByCursorAfterUsesBeforeParam(t *testing.T) {
	const (
		startMS = int64(1700000000000)
		endMS   = int64(1700021600000)
		barTS   = int64(1700003600000)
	)
	call := 0
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		call++
		query := req.URL.Query()
		switch call {
		case 1:
			if got, want := query.Get("before"), strconv.FormatInt(startMS-1, 10); got != want {
				t.Fatalf("first request before mismatch: got=%s want=%s", got, want)
			}
			if got := query.Get("after"); got != "" {
				t.Fatalf("first request after should be empty, got=%s", got)
			}
			return okxTestHTTPResponse(t, [][]string{{strconv.FormatInt(barTS, 10), "1", "2", "0.5", "1.5", "100"}}), nil
		case 2:
			if got, want := query.Get("before"), strconv.FormatInt(barTS+1, 10); got != want {
				t.Fatalf("second request before mismatch: got=%s want=%s", got, want)
			}
			if got := query.Get("after"); got != "" {
				t.Fatalf("second request after should be empty, got=%s", got)
			}
			return okxTestHTTPResponse(t, nil), nil
		default:
			t.Fatalf("unexpected request count: %d", call)
			return nil, nil
		}
	})

	client := &Client{
		baseURL: "https://okx.test",
		client: &http.Client{
			Transport: transport,
		},
	}
	rows, err := client.fetchOHLCVRangeByCursor(
		context.Background(),
		"/api/v5/market/history-candles",
		"BTC-USDT-SWAP",
		"1H",
		startMS,
		endMS,
		300,
		"after",
		false,
	)
	if err != nil {
		t.Fatalf("fetchOHLCVRangeByCursor returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("unexpected rows length: got=%d want=1", len(rows))
	}
	if rows[0].TS != barTS {
		t.Fatalf("unexpected ts: got=%d want=%d", rows[0].TS, barTS)
	}
}

func okxTestHTTPResponse(t *testing.T, rows [][]string) *http.Response {
	t.Helper()
	payload := map[string]any{
		"code": "0",
		"msg":  "",
		"data": rows,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload failed: %v", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader(string(raw))),
		Header:     make(http.Header),
	}
}
