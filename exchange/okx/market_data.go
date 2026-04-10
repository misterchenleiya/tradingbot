package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange"
)

func (c *Client) FetchLatestOHLCV(ctx context.Context, symbol, timeframe string) (exchange.OHLCV, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	instID, err := c.normalizeInstID(symbol)
	if err != nil {
		return exchange.OHLCV{}, err
	}

	query := url.Values{}
	query.Set("instId", instID)
	query.Set("bar", bar)
	query.Set("limit", "1")
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/market/candles", query, nil, false)
	if err != nil {
		return exchange.OHLCV{}, err
	}

	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return exchange.OHLCV{}, err
	}
	if len(rows) == 0 {
		return exchange.OHLCV{}, fmt.Errorf("okx empty candles")
	}
	return parseOKXOHLCVRow(rows[0])
}

func (c *Client) FetchOHLCVRange(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]exchange.OHLCV, error) {
	if !end.After(start) {
		return nil, fmt.Errorf("invalid time range")
	}
	bar, err := okxBar(timeframe)
	if err != nil {
		return nil, err
	}
	instID, err := c.normalizeInstID(symbol)
	if err != nil {
		return nil, err
	}

	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	const maxLimit = 300

	var merged []exchange.OHLCV
	attempts := []struct {
		endpoint string
		cursor   string
		seconds  bool
	}{
		{endpoint: "/api/v5/market/history-candles", cursor: "before", seconds: false},
		{endpoint: "/api/v5/market/history-candles", cursor: "before", seconds: true},
		{endpoint: "/api/v5/market/history-candles", cursor: "after", seconds: false},
		{endpoint: "/api/v5/market/history-candles", cursor: "after", seconds: true},
		{endpoint: "/api/v5/market/candles", cursor: "before", seconds: false},
		{endpoint: "/api/v5/market/candles", cursor: "after", seconds: false},
	}
	for _, attempt := range attempts {
		items, rangeErr := c.fetchOHLCVRangeByCursor(ctx, attempt.endpoint, instID, bar, startMS, endMS, maxLimit, attempt.cursor, attempt.seconds)
		if rangeErr != nil {
			continue
		}
		merged = append(merged, items...)
		if ohlcvHasRangeOverlap(merged, startMS, endMS) {
			break
		}
	}

	out := normalizeOHLCVRange(merged, startMS, endMS)
	if len(out) == 0 {
		return nil, fmt.Errorf("okx empty candles range")
	}
	return out, nil
}

func (c *Client) LoadPerpUSDTMarkets(ctx context.Context) ([]exchange.MarketSymbol, error) {
	query := url.Values{}
	query.Set("instType", "SWAP")
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/public/instruments", query, nil, false)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		InstID    string `json:"instId"`
		BaseCcy   string `json:"baseCcy"`
		QuoteCcy  string `json:"quoteCcy"`
		SettleCcy string `json:"settleCcy"`
		State     string `json:"state"`
		ListTime  string `json:"listTime"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}

	out := make([]exchange.MarketSymbol, 0, len(rows))
	for _, row := range rows {
		quote := strings.TrimSpace(row.QuoteCcy)
		if quote == "" {
			quote = strings.TrimSpace(row.SettleCcy)
		}
		if !strings.EqualFold(quote, "USDT") {
			continue
		}
		state := strings.ToLower(strings.TrimSpace(row.State))
		if state != "" && state != "live" {
			continue
		}
		base := strings.TrimSpace(row.BaseCcy)
		if base == "" {
			parts := strings.Split(strings.TrimSpace(row.InstID), "-")
			if len(parts) >= 2 {
				base = parts[0]
				if quote == "" {
					quote = parts[1]
				}
			}
		}
		if base == "" || quote == "" {
			continue
		}
		out = append(out, exchange.MarketSymbol{
			Symbol:   base + "/" + quote,
			Base:     base,
			Quote:    quote,
			Type:     "swap",
			ListTime: parseInt64Safe(row.ListTime),
		})
	}
	return out, nil
}

func (c *Client) FetchSymbolListTime(ctx context.Context, symbol string) (int64, error) {
	instID, err := c.normalizeInstID(symbol)
	if err != nil {
		return 0, err
	}
	query := url.Values{}
	query.Set("instType", "SWAP")
	query.Set("instId", instID)
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/public/instruments", query, nil, false)
	if err != nil {
		return 0, err
	}
	var rows []struct {
		ListTime string `json:"listTime"`
	}
	if err := json.Unmarshal(data, &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return parseInt64Safe(rows[0].ListTime), nil
}

func (c *Client) FetchDailyVolumesUSDT(ctx context.Context, symbol string, limit int) ([]float64, error) {
	instID, err := c.normalizeInstID(symbol)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 15
	}

	query := url.Values{}
	query.Set("instId", instID)
	query.Set("bar", "1Dutc")
	query.Set("limit", strconv.Itoa(limit+1))
	data, err := c.doJSON(ctx, http.MethodGet, "/api/v5/market/candles", query, nil, false)
	if err != nil {
		return nil, err
	}

	var rows [][]string
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("okx empty candles")
	}

	volumes := make([]dailyVolume, 0, len(rows))
	for _, row := range rows {
		if len(row) < 8 {
			return nil, fmt.Errorf("okx invalid daily candle row")
		}
		openTime, err := strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
		if err != nil {
			return nil, err
		}
		closePx, err := strconv.ParseFloat(strings.TrimSpace(row[4]), 64)
		if err != nil {
			return nil, err
		}
		volBase, err := strconv.ParseFloat(strings.TrimSpace(row[5]), 64)
		if err != nil {
			return nil, err
		}
		volQuote := volBase * closePx
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(row[7]), 64); err == nil && parsed > 0 {
			volQuote = parsed
		}
		volumes = append(volumes, dailyVolume{
			openTime: openTime,
			volume:   volQuote,
		})
	}
	return dailyVolumesToUSDT(dropCurrentDailyVolumes(volumes)), nil
}

func (c *Client) fetchOHLCVRangeByCursor(
	ctx context.Context,
	endpoint string,
	instID string,
	bar string,
	startMS int64,
	endMS int64,
	limit int,
	cursorType string,
	useSeconds bool,
) ([]exchange.OHLCV, error) {
	if limit <= 0 {
		limit = 300
	}
	if limit > 300 {
		limit = 300
	}
	if cursorType != "before" && cursorType != "after" {
		return nil, fmt.Errorf("unsupported cursor type: %s", cursorType)
	}

	// OKX cursor semantics are exchange-specific:
	// - query "after": returns older rows before the cursor timestamp.
	// - query "before": returns newer rows after the cursor timestamp.
	//
	// Keep cursorType as internal range direction:
	// - "before" => walk older side from range end.
	// - "after"  => walk newer side from range start.
	queryCursorKey := "before"
	cursorMS := startMS - 1
	if cursorType == "before" {
		queryCursorKey = "after"
		cursorMS = endMS + 1
	}

	const maxPages = 200
	out := make([]exchange.OHLCV, 0, limit*2)
	for page := 0; page < maxPages; page++ {
		query := url.Values{}
		query.Set("instId", instID)
		query.Set("bar", bar)
		query.Set("limit", strconv.Itoa(limit))
		cursorValue := cursorMS
		if useSeconds {
			cursorValue = cursorValue / 1000
		}
		query.Set(queryCursorKey, strconv.FormatInt(cursorValue, 10))

		data, err := c.doJSON(ctx, http.MethodGet, endpoint, query, nil, false)
		if err != nil {
			return nil, err
		}
		var rows [][]string
		if err := json.Unmarshal(data, &rows); err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}

		batch := make([]exchange.OHLCV, 0, len(rows))
		minTS := int64(0)
		maxTS := int64(0)
		for _, row := range rows {
			item, parseErr := parseOKXOHLCVRow(row)
			if parseErr != nil {
				return nil, parseErr
			}
			ts := normalizeTimestampMS(item.TS)
			item.TS = ts
			batch = append(batch, item)
			if minTS == 0 || ts < minTS {
				minTS = ts
			}
			if maxTS == 0 || ts > maxTS {
				maxTS = ts
			}
		}
		out = append(out, batch...)

		if cursorType == "before" {
			if minTS <= startMS {
				break
			}
			if minTS >= cursorMS {
				break
			}
			cursorMS = minTS - 1
			if cursorMS <= 0 {
				break
			}
			continue
		}
		if maxTS >= endMS {
			break
		}
		if maxTS <= cursorMS {
			break
		}
		cursorMS = maxTS + 1
		if cursorMS <= 0 {
			break
		}
	}
	return out, nil
}

func (c *Client) normalizeInstID(symbol string) (string, error) {
	text := strings.ToUpper(strings.TrimSpace(symbol))
	if text == "" {
		return "", fmt.Errorf("symbol is required")
	}
	if strings.Contains(text, "-SWAP") {
		return text, nil
	}
	if idx := strings.Index(text, ":"); idx > 0 {
		text = text[:idx]
	}
	if strings.Contains(text, "/") && !strings.HasSuffix(text, ".P") {
		text += ".P"
	}
	if strings.HasSuffix(text, ".P") {
		return c.NormalizeSymbol(text)
	}
	return c.NormalizeSymbol(text)
}

func parseOKXOHLCVRow(row []string) (exchange.OHLCV, error) {
	if len(row) < 6 {
		return exchange.OHLCV{}, fmt.Errorf("okx invalid candle row")
	}
	openTime, err := strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	open, err := strconv.ParseFloat(strings.TrimSpace(row[1]), 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	high, err := strconv.ParseFloat(strings.TrimSpace(row[2]), 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	low, err := strconv.ParseFloat(strings.TrimSpace(row[3]), 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	closePx, err := strconv.ParseFloat(strings.TrimSpace(row[4]), 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	vol, err := strconv.ParseFloat(strings.TrimSpace(row[5]), 64)
	if err != nil {
		return exchange.OHLCV{}, err
	}
	return exchange.OHLCV{
		TS:     openTime,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closePx,
		Volume: vol,
	}, nil
}

func normalizeTimestampMS(ts int64) int64 {
	switch {
	case ts > 9_999_999_999_999:
		return ts / 1000
	case ts > 999_999_999_999:
		return ts
	default:
		return ts * 1000
	}
}

func ohlcvHasRangeOverlap(items []exchange.OHLCV, startMS, endMS int64) bool {
	for _, item := range items {
		ts := normalizeTimestampMS(item.TS)
		if ts >= startMS && ts <= endMS {
			return true
		}
	}
	return false
}

func normalizeOHLCVRange(items []exchange.OHLCV, startMS, endMS int64) []exchange.OHLCV {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[int64]exchange.OHLCV, len(items))
	for _, item := range items {
		ts := normalizeTimestampMS(item.TS)
		if ts < startMS || ts > endMS {
			continue
		}
		item.TS = ts
		seen[ts] = item
	}
	if len(seen) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(seen))
	for ts := range seen {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	out := make([]exchange.OHLCV, 0, len(keys))
	for _, ts := range keys {
		out = append(out, seen[ts])
	}
	return out
}

func okxBar(tf string) (string, error) {
	switch tf {
	case "1m", "3m", "5m", "15m", "30m":
		return tf, nil
	case "1h":
		return "1H", nil
	case "2h":
		return "2H", nil
	case "4h":
		return "4H", nil
	case "6h":
		return "6Hutc", nil
	case "8h":
		return "8H", nil
	case "12h":
		return "12Hutc", nil
	case "1d":
		return "1Dutc", nil
	case "1w":
		return "1Wutc", nil
	default:
		return "", fmt.Errorf("unsupported timeframe for okx: %s", tf)
	}
}

type dailyVolume struct {
	openTime int64
	volume   float64
}

func dropCurrentDailyVolumes(items []dailyVolume) []dailyVolume {
	if len(items) == 0 {
		return items
	}
	latestIdx := 0
	latestTime := items[0].openTime
	for i := 1; i < len(items); i++ {
		if items[i].openTime > latestTime {
			latestTime = items[i].openTime
			latestIdx = i
		}
	}
	if isCurrentDailyCandle(latestTime) {
		return append(items[:latestIdx], items[latestIdx+1:]...)
	}
	return items
}

func isCurrentDailyCandle(openTimeMS int64) bool {
	if openTimeMS <= 0 {
		return false
	}
	openTime := time.UnixMilli(openTimeMS).UTC()
	return openTime.Add(24 * time.Hour).After(time.Now().UTC())
}

func dailyVolumesToUSDT(items []dailyVolume) []float64 {
	out := make([]float64, 0, len(items))
	for _, item := range items {
		out = append(out, item.volume)
	}
	return out
}

func parseInt64Safe(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return value
}

var _ exchange.MarketDataSource = (*Client)(nil)
