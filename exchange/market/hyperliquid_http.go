package market

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

const hyperliquidInfoURL = "https://api.hyperliquid.xyz/info"

func fetchHyperliquidOHLCV(ctx context.Context, client *http.Client, symbol, timeframe string) (models.OHLCV, error) {
	interval, err := hyperliquidInterval(timeframe)
	if err != nil {
		return models.OHLCV{}, err
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return models.OHLCV{}, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	coin, err := hyperliquidCoinFromSymbol(symbol)
	if err != nil {
		return models.OHLCV{}, err
	}

	end := time.Now().UTC()
	start := end.Add(-2 * dur)
	candles, err := fetchHyperliquidCandles(ctx, client, coin, interval, start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return models.OHLCV{}, err
	}
	if len(candles) == 0 {
		return models.OHLCV{}, errors.New("hyperliquid empty data")
	}
	last := candles[len(candles)-1]
	return models.OHLCV{
		TS:     last.OpenTime,
		Open:   last.Open,
		High:   last.High,
		Low:    last.Low,
		Close:  last.Close,
		Volume: last.Volume,
	}, nil
}

func fetchHyperliquidOHLCVRange(ctx context.Context, client *http.Client, controller *RequestController, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	interval, err := hyperliquidInterval(timeframe)
	if err != nil {
		return nil, err
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return nil, fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}
	coin, err := hyperliquidCoinFromSymbol(symbol)
	if err != nil {
		return nil, err
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}

	cur := startMS
	var out []models.OHLCV
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	for cur <= endMS {
		candles, err := fetchHyperliquidCandlesWithController(ctx, client, controller, requestMeta, coin, interval, cur, endMS)
		if err != nil {
			return nil, err
		}
		if len(candles) == 0 {
			break
		}
		for _, candle := range candles {
			out = append(out, models.OHLCV{
				TS:     candle.OpenTime,
				Open:   candle.Open,
				High:   candle.High,
				Low:    candle.Low,
				Close:  candle.Close,
				Volume: candle.Volume,
			})
		}
		lastOpen := candles[len(candles)-1].OpenTime
		next := lastOpen + step
		if next <= cur {
			return nil, fmt.Errorf("hyperliquid range fetch stalled at %d", lastOpen)
		}
		cur = next
	}
	if len(out) == 0 {
		return nil, errors.New("hyperliquid empty data")
	}
	out = dedupeOHLCV(out)
	return out, nil
}

func fetchHyperliquidPerpUSDCMarkets(ctx context.Context, client *http.Client, exchange string) ([]models.Symbol, error) {
	payload := map[string]any{
		"type": "meta",
	}
	data, err := hyperliquidInfo(ctx, client, payload)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Universe []struct {
			Name       string `json:"name"`
			IsDelisted bool   `json:"isDelisted"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	out := make([]models.Symbol, 0, len(resp.Universe))
	for _, item := range resp.Universe {
		if strings.TrimSpace(item.Name) == "" || item.IsDelisted {
			continue
		}
		coin, err := hyperliquidNormalizeCoin(item.Name)
		if err != nil {
			continue
		}
		out = append(out, models.Symbol{
			Exchange: exchange,
			Symbol:   coin + "/USDC",
			Base:     coin,
			Quote:    "USDC",
			Type:     "swap",
		})
	}
	return out, nil
}

func fetchHyperliquidDailyVolumesUSDT(ctx context.Context, client *http.Client, symbol string, limit int) ([]float64, error) {
	if limit <= 0 {
		limit = 15
	}
	coin, err := hyperliquidCoinFromSymbol(symbol)
	if err != nil {
		return nil, err
	}
	interval := "1d"
	end := time.Now().UTC()
	start := end.Add(-time.Duration(limit+1) * 24 * time.Hour)
	candles, err := fetchHyperliquidCandles(ctx, client, coin, interval, start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return nil, err
	}
	if len(candles) == 0 {
		return nil, errors.New("hyperliquid empty data")
	}
	items := make([]dailyVolume, 0, len(candles))
	for _, candle := range candles {
		volQuote := candle.Volume * candle.Close
		items = append(items, dailyVolume{openTime: candle.OpenTime, volume: volQuote})
	}
	return dailyVolumesToUSDT(dropCurrentDailyVolumes(items)), nil
}

type hyperliquidCandle struct {
	OpenTime  int64
	CloseTime int64
	Interval  string
	Coin      string
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
}

func fetchHyperliquidCandles(ctx context.Context, client *http.Client, coin, interval string, startMS, endMS int64) ([]hyperliquidCandle, error) {
	return fetchHyperliquidCandlesWithController(ctx, client, nil, RequestMeta{}, coin, interval, startMS, endMS)
}

func fetchHyperliquidCandlesWithController(
	ctx context.Context,
	client *http.Client,
	controller *RequestController,
	meta RequestMeta,
	coin,
	interval string,
	startMS,
	endMS int64,
) ([]hyperliquidCandle, error) {
	payload := map[string]any{
		"type": "candleSnapshot",
		"req": map[string]any{
			"coin":      coin,
			"interval":  interval,
			"startTime": startMS,
			"endTime":   endMS,
		},
	}
	data, err := hyperliquidInfoWithController(ctx, client, controller, meta, payload)
	if err != nil {
		return nil, err
	}
	candles, err := decodeHyperliquidCandles(data)
	if err != nil {
		return nil, err
	}
	if len(candles) == 0 {
		return nil, nil
	}
	for i := range candles {
		if candles[i].Interval == "" {
			candles[i].Interval = interval
		}
		if candles[i].Coin == "" {
			candles[i].Coin = coin
		}
	}
	return candles, nil
}

func hyperliquidInfoWithController(ctx context.Context, client *http.Client, controller *RequestController, meta RequestMeta, payload any) ([]byte, error) {
	if controller == nil {
		return hyperliquidInfo(ctx, client, payload)
	}
	if strings.TrimSpace(meta.Exchange) == "" {
		meta.Exchange = "hyperliquid"
	}
	if strings.TrimSpace(meta.Endpoint) == "" {
		meta.Endpoint = EndpointOHLCVRange
	}
	var out []byte
	err := controller.Do(ctx, meta, func(execCtx context.Context) error {
		data, err := hyperliquidInfo(execCtx, client, payload)
		if err != nil {
			return err
		}
		out = data
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func decodeHyperliquidCandles(data []byte) ([]hyperliquidCandle, error) {
	rows, err := decodeHyperliquidCandleRows(data)
	if err != nil {
		return nil, err
	}
	candles := make([]hyperliquidCandle, 0, len(rows))
	for _, row := range rows {
		candle, err := parseHyperliquidCandle(row)
		if err != nil {
			return nil, err
		}
		candles = append(candles, candle)
	}
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].OpenTime < candles[j].OpenTime
	})
	return candles, nil
}

func decodeHyperliquidCandleRows(data []byte) ([]map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err == nil {
		return rows, nil
	}
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var row map[string]any
	if err := dec.Decode(&row); err == nil {
		return []map[string]any{row}, nil
	}
	return nil, errors.New("hyperliquid invalid candle data")
}

func parseHyperliquidCandle(row map[string]any) (hyperliquidCandle, error) {
	openTime, err := toInt64(row["t"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	closeTime, err := toInt64(row["T"])
	if err != nil {
		closeTime = 0
	}
	open, err := toFloat64(row["o"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	high, err := toFloat64(row["h"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	low, err := toFloat64(row["l"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	closePx, err := toFloat64(row["c"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	vol, err := toFloat64(row["v"])
	if err != nil {
		return hyperliquidCandle{}, err
	}
	interval := readHyperliquidString(row["i"])
	coin := readHyperliquidString(row["s"])
	return hyperliquidCandle{
		OpenTime:  openTime,
		CloseTime: closeTime,
		Interval:  interval,
		Coin:      coin,
		Open:      open,
		High:      high,
		Low:       low,
		Close:     closePx,
		Volume:    vol,
	}, nil
}

func readHyperliquidString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case int64:
		return strconv.FormatInt(t, 10)
	case int:
		return strconv.Itoa(t)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func hyperliquidInfo(ctx context.Context, client *http.Client, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, hyperliquidInfoURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	respBytes, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		msg := strings.TrimSpace(string(respBytes))
		if msg == "" {
			return nil, fmt.Errorf("hyperliquid status: %s", resp.Status)
		}
		return nil, fmt.Errorf("hyperliquid status %s: %s", resp.Status, msg)
	}
	if err := parseHyperliquidInfoError(respBytes); err != nil {
		return nil, err
	}
	return respBytes, nil
}

func parseHyperliquidInfoError(data []byte) error {
	var resp struct {
		Error  string `json:"error"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil
	}
	if strings.TrimSpace(resp.Error) == "" {
		return nil
	}
	if strings.TrimSpace(resp.Status) != "" {
		return fmt.Errorf("%s: %s", resp.Status, resp.Error)
	}
	return errors.New(resp.Error)
}
