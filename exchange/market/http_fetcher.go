package market

import (
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

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type HTTPFetcher struct {
	Client             *http.Client
	Logger             *zap.Logger
	Controller         *RequestController
	Exchanges          map[string]iface.ExchangeMarketDataSource
	RequireMarketPlane map[string]bool
	FetchUnclosedOHLCV bool
}

func (f *HTTPFetcher) httpClient() *http.Client {
	if f != nil && f.Client != nil {
		return f.Client
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func (f *HTTPFetcher) fetchLogger() *zap.Logger {
	if f != nil && f.Logger != nil {
		return f.Logger
	}
	return glog.Nop()
}

func (f *HTTPFetcher) controller(ctx context.Context) *RequestController {
	if f != nil && f.Controller != nil {
		return f.Controller
	}
	return requestController(ctx)
}

func (f *HTTPFetcher) marketDataSource(exchange string) (iface.ExchangeMarketDataSource, bool) {
	if f == nil || len(f.Exchanges) == 0 {
		return nil, false
	}
	name := strings.ToLower(strings.TrimSpace(exchange))
	source, ok := f.Exchanges[name]
	if !ok || source == nil {
		return nil, false
	}
	return source, true
}

func (f *HTTPFetcher) requireMarketPlane(exchange string) bool {
	if f == nil || len(f.RequireMarketPlane) == 0 {
		return false
	}
	name := strings.ToLower(strings.TrimSpace(exchange))
	if name == "" {
		return false
	}
	return f.RequireMarketPlane[name]
}

func strictMarketProxyError(exchange, endpoint, summary string, cause error) error {
	err := fmt.Errorf("market proxy strict mode: exchange=%s endpoint=%s %s", exchange, endpoint, summary)
	if cause != nil {
		err = fmt.Errorf("%w: %v", err, cause)
	}
	return ClassifyMarketError(exchange, endpoint, err)
}

func doGet(ctx context.Context, client *http.Client, url string) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func doGetWithController(ctx context.Context, client *http.Client, controller *RequestController, meta RequestMeta, url string) (*http.Response, error) {
	if controller == nil {
		return doGet(ctx, client, url)
	}
	var out *http.Response
	err := controller.Do(ctx, meta, func(execCtx context.Context) error {
		resp, err := doGet(execCtx, client, url)
		if err != nil {
			return err
		}
		out = resp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (f *HTTPFetcher) FetchLatest(ctx context.Context, exchange, symbol, timeframe string) (models.OHLCV, error) {
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVLatest,
		Realtime: true,
	}
	controller := f.controller(ctx)
	if controller == nil {
		if until, ok := ExchangePaused(exchange, time.Now().UTC()); ok {
			return models.OHLCV{}, fmt.Errorf("exchange %s paused until %s", exchange, until.Format(time.RFC3339))
		}
	}
	client := f.httpClient()

	var out models.OHLCV
	doFetch := func(ctx context.Context) error {
		var err error
		requiresMarketPlane := f.requireMarketPlane(exchange)
		if source, ok := f.marketDataSource(exchange); ok {
			var item iface.ExchangeOHLCV
			item, err = source.FetchLatestOHLCV(ctx, symbol, timeframe)
			if err == nil {
				out = models.OHLCV{
					TS:     item.TS,
					Open:   item.Open,
					High:   item.High,
					Low:    item.Low,
					Close:  item.Close,
					Volume: item.Volume,
				}
				return nil
			}
			if requiresMarketPlane {
				return strictMarketProxyError(exchange, meta.Endpoint, "market-plane latest request failed", err)
			}
		} else if requiresMarketPlane {
			return strictMarketProxyError(exchange, meta.Endpoint, "market-plane source unavailable", nil)
		}
		switch strings.ToLower(exchange) {
		case "binance":
			out, err = fetchBinanceOHLCV(ctx, client, symbol, timeframe)
		case "okx":
			out, err = fetchOKXOHLCV(ctx, client, symbol, timeframe)
		case "bitget":
			out, err = fetchBitgetOHLCV(ctx, client, symbol, timeframe)
		case "hyperliquid":
			out, err = fetchHyperliquidOHLCV(ctx, client, symbol, timeframe)
		default:
			err = fmt.Errorf("unsupported exchange: %s", exchange)
		}
		if err != nil {
			return ClassifyMarketError(exchange, meta.Endpoint, err)
		}
		return nil
	}

	if controller != nil {
		if err := controller.Do(ctx, meta, doFetch); err != nil {
			return models.OHLCV{}, err
		}
		return out, nil
	}
	if err := doFetch(ctx); err != nil {
		return models.OHLCV{}, err
	}
	return out, nil
}

func (f *HTTPFetcher) FetchOHLCVRange(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	controller := f.controller(ctx)
	if controller == nil {
		if until, ok := ExchangePaused(exchange, time.Now().UTC()); ok {
			return nil, fmt.Errorf("exchange %s paused until %s", exchange, until.Format(time.RFC3339))
		}
	}
	client := f.httpClient()
	logger := f.fetchLogger()

	var out []models.OHLCV
	doFetch := func(ctx context.Context) error {
		var err error
		requiresMarketPlane := f.requireMarketPlane(exchange)
		if source, ok := f.marketDataSource(exchange); ok {
			var rows []iface.ExchangeOHLCV
			sourceCtx := ctx
			if controller != nil {
				sourceCtx = WithRequestController(sourceCtx, controller)
			}
			rows, err = source.FetchOHLCVRange(sourceCtx, symbol, timeframe, start, end)
			if err == nil {
				out = make([]models.OHLCV, 0, len(rows))
				for _, row := range rows {
					out = append(out, models.OHLCV{
						TS:     row.TS,
						Open:   row.Open,
						High:   row.High,
						Low:    row.Low,
						Close:  row.Close,
						Volume: row.Volume,
					})
				}
				if ohlcvCoversRequestedRange(out, timeframe, start, end) {
					return nil
				}
				if requiresMarketPlane {
					return strictMarketProxyError(exchange, meta.Endpoint, "market-plane ohlcv range incomplete", nil)
				}
				logger.Warn("market-plane ohlcv range incomplete, fallback to legacy fetch",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.String("timeframe", timeframe),
					zap.Time("range_start", start),
					zap.Time("range_end", end),
					zap.Int("rows", len(out)),
				)
				err = fmt.Errorf("incomplete market-plane ohlcv range")
			} else {
				if requiresMarketPlane {
					return strictMarketProxyError(exchange, meta.Endpoint, "market-plane ohlcv range request failed", err)
				}
				logger.Warn("market-plane ohlcv range failed, fallback to legacy fetch",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.String("timeframe", timeframe),
					zap.Time("range_start", start),
					zap.Time("range_end", end),
					zap.Error(err),
				)
			}
		} else if requiresMarketPlane {
			return strictMarketProxyError(exchange, meta.Endpoint, "market-plane source unavailable", nil)
		}
		switch strings.ToLower(exchange) {
		case "binance":
			out, err = fetchBinanceOHLCVRange(ctx, client, controller, exchange, symbol, timeframe, start, end)
		case "okx":
			out, err = fetchOKXOHLCVRange(ctx, client, logger, controller, exchange, symbol, timeframe, start, end)
		case "bitget":
			out, err = fetchBitgetOHLCVRange(ctx, client, controller, exchange, symbol, timeframe, start, end)
		case "hyperliquid":
			out, err = fetchHyperliquidOHLCVRange(ctx, client, controller, exchange, symbol, timeframe, start, end)
		default:
			err = fmt.Errorf("unsupported exchange: %s", exchange)
		}
		if err != nil {
			return ClassifyMarketError(exchange, meta.Endpoint, err)
		}
		return nil
	}

	if err := doFetch(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *HTTPFetcher) LoadPerpUSDTMarkets(ctx context.Context, exchange string) ([]models.Symbol, error) {
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointMarkets,
	}
	controller := f.controller(ctx)
	if controller == nil {
		if until, ok := ExchangePaused(exchange, time.Now().UTC()); ok {
			return nil, fmt.Errorf("exchange %s paused until %s", exchange, until.Format(time.RFC3339))
		}
	}
	client := f.httpClient()

	var out []models.Symbol
	doFetch := func(ctx context.Context) error {
		var err error
		requiresMarketPlane := f.requireMarketPlane(exchange)
		if source, ok := f.marketDataSource(exchange); ok {
			var rows []iface.ExchangeMarketSymbol
			rows, err = source.LoadPerpUSDTMarkets(ctx)
			if err == nil {
				out = make([]models.Symbol, 0, len(rows))
				for _, row := range rows {
					out = append(out, models.Symbol{
						Exchange: exchange,
						Symbol:   row.Symbol,
						Base:     row.Base,
						Quote:    row.Quote,
						Type:     row.Type,
						ListTime: row.ListTime,
					})
				}
				return nil
			}
			if requiresMarketPlane {
				return strictMarketProxyError(exchange, meta.Endpoint, "market-plane market list request failed", err)
			}
		} else if requiresMarketPlane {
			return strictMarketProxyError(exchange, meta.Endpoint, "market-plane source unavailable", nil)
		}
		switch strings.ToLower(exchange) {
		case "binance":
			out, err = fetchBinancePerpUSDTMarkets(ctx, client, exchange)
		case "okx":
			out, err = fetchOKXPerpUSDTMarkets(ctx, client, exchange)
		case "bitget":
			out, err = fetchBitgetPerpUSDTMarkets(ctx, client, exchange)
		case "hyperliquid":
			out, err = fetchHyperliquidPerpUSDCMarkets(ctx, client, exchange)
		default:
			err = fmt.Errorf("unsupported exchange: %s", exchange)
		}
		if err != nil {
			return ClassifyMarketError(exchange, meta.Endpoint, err)
		}
		return nil
	}

	if controller != nil {
		if err := controller.Do(ctx, meta, doFetch); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := doFetch(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (f *HTTPFetcher) FetchSymbolListTime(ctx context.Context, exchange, symbol string) (int64, error) {
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointMarkets,
	}
	controller := f.controller(ctx)
	client := f.httpClient()

	var out int64
	doFetch := func(ctx context.Context) error {
		if source, ok := f.marketDataSource(exchange); ok {
			if listTimeSource, ok := source.(iface.ExchangeSymbolListTimeSource); ok {
				ts, err := listTimeSource.FetchSymbolListTime(ctx, symbol)
				if err == nil {
					out = ts
					return nil
				}
			}
		}
		switch strings.ToLower(strings.TrimSpace(exchange)) {
		case "okx":
			ts, err := fetchOKXSymbolListTime(ctx, client, symbol)
			if err != nil {
				return ClassifyMarketError(exchange, meta.Endpoint, err)
			}
			out = ts
			return nil
		default:
			return nil
		}
	}

	if controller != nil {
		if err := controller.Do(ctx, meta, doFetch); err != nil {
			return 0, err
		}
		return out, nil
	}
	if err := doFetch(ctx); err != nil {
		return 0, err
	}
	return out, nil
}

func (f *HTTPFetcher) FetchDailyVolumesUSDT(ctx context.Context, exchange, symbol string, limit int) ([]float64, error) {
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointDailyVolumes,
	}
	controller := f.controller(ctx)
	if controller == nil {
		if until, ok := ExchangePaused(exchange, time.Now().UTC()); ok {
			return nil, fmt.Errorf("exchange %s paused until %s", exchange, until.Format(time.RFC3339))
		}
	}
	client := f.httpClient()
	if limit <= 0 {
		limit = 15
	}

	var out []float64
	doFetch := func(ctx context.Context) error {
		var err error
		requiresMarketPlane := f.requireMarketPlane(exchange)
		if source, ok := f.marketDataSource(exchange); ok {
			out, err = source.FetchDailyVolumesUSDT(ctx, symbol, limit)
			if err == nil {
				return nil
			}
			if requiresMarketPlane {
				return strictMarketProxyError(exchange, meta.Endpoint, "market-plane daily volume request failed", err)
			}
		} else if requiresMarketPlane {
			return strictMarketProxyError(exchange, meta.Endpoint, "market-plane source unavailable", nil)
		}
		switch strings.ToLower(exchange) {
		case "binance":
			out, err = fetchBinanceDailyVolumesUSDT(ctx, client, symbol, limit)
		case "okx":
			out, err = fetchOKXDailyVolumesUSDT(ctx, client, symbol, limit)
		case "bitget":
			out, err = fetchBitgetDailyVolumesUSDT(ctx, client, symbol, limit)
		case "hyperliquid":
			out, err = fetchHyperliquidDailyVolumesUSDT(ctx, client, symbol, limit)
		default:
			err = fmt.Errorf("unsupported exchange: %s", exchange)
		}
		if err != nil {
			return ClassifyMarketError(exchange, meta.Endpoint, err)
		}
		return nil
	}

	if controller != nil {
		if err := controller.Do(ctx, meta, doFetch); err != nil {
			return nil, err
		}
		return out, nil
	}
	if err := doFetch(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func fetchBinanceOHLCV(ctx context.Context, client *http.Client, symbol, timeframe string) (models.OHLCV, error) {
	interval, err := binanceInterval(timeframe)
	if err != nil {
		return models.OHLCV{}, err
	}
	sym := strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&limit=1", sym, interval)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return models.OHLCV{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return models.OHLCV{}, readBinanceError(resp)
	}
	defer resp.Body.Close()

	var data [][]any
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		return models.OHLCV{}, err
	}
	if len(data) == 0 || len(data[0]) < 6 {
		return models.OHLCV{}, errors.New("binance empty data")
	}

	openTime, err := toInt64(data[0][0])
	if err != nil {
		return models.OHLCV{}, err
	}
	open, err := toFloat64(data[0][1])
	if err != nil {
		return models.OHLCV{}, err
	}
	high, err := toFloat64(data[0][2])
	if err != nil {
		return models.OHLCV{}, err
	}
	low, err := toFloat64(data[0][3])
	if err != nil {
		return models.OHLCV{}, err
	}
	closePx, err := toFloat64(data[0][4])
	if err != nil {
		return models.OHLCV{}, err
	}
	vol, err := toFloat64(data[0][5])
	if err != nil {
		return models.OHLCV{}, err
	}

	return models.OHLCV{
		TS:     openTime,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closePx,
		Volume: vol,
	}, nil
}

func fetchBinanceOHLCVRange(ctx context.Context, client *http.Client, controller *RequestController, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	interval, err := binanceInterval(timeframe)
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

	sym := strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}

	limit := 1500
	cur := startMS
	var out []models.OHLCV
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	for cur <= endMS {
		url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d&limit=%d", sym, interval, cur, endMS, limit)
		resp, err := doGetWithController(ctx, client, controller, requestMeta, url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return nil, readBinanceError(resp)
		}
		var data [][]any
		dec := json.NewDecoder(resp.Body)
		dec.UseNumber()
		if err := dec.Decode(&data); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if len(data) == 0 {
			break
		}
		var lastOpen int64
		for _, row := range data {
			if len(row) < 6 {
				return nil, errors.New("binance invalid kline data")
			}
			openTime, err := toInt64(row[0])
			if err != nil {
				return nil, err
			}
			open, err := toFloat64(row[1])
			if err != nil {
				return nil, err
			}
			high, err := toFloat64(row[2])
			if err != nil {
				return nil, err
			}
			low, err := toFloat64(row[3])
			if err != nil {
				return nil, err
			}
			closePx, err := toFloat64(row[4])
			if err != nil {
				return nil, err
			}
			vol, err := toFloat64(row[5])
			if err != nil {
				return nil, err
			}
			out = append(out, models.OHLCV{
				TS:     openTime,
				Open:   open,
				High:   high,
				Low:    low,
				Close:  closePx,
				Volume: vol,
			})
			lastOpen = openTime
		}
		next := lastOpen + step
		if next <= cur {
			return nil, fmt.Errorf("binance range fetch stalled at %d", lastOpen)
		}
		cur = next
	}
	if len(out) == 0 {
		return nil, errors.New("binance empty data")
	}
	return out, nil
}

func fetchBinancePerpUSDTMarkets(ctx context.Context, client *http.Client, exchange string) ([]models.Symbol, error) {
	url := "https://fapi.binance.com/fapi/v1/exchangeInfo"
	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, readBinanceError(resp)
	}
	defer resp.Body.Close()

	var payload struct {
		Symbols []struct {
			Symbol       string `json:"symbol"`
			BaseAsset    string `json:"baseAsset"`
			QuoteAsset   string `json:"quoteAsset"`
			ContractType string `json:"contractType"`
			Status       string `json:"status"`
		} `json:"symbols"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	out := make([]models.Symbol, 0, len(payload.Symbols))
	for _, item := range payload.Symbols {
		if item.ContractType != "PERPETUAL" {
			continue
		}
		if strings.ToUpper(item.QuoteAsset) != "USDT" {
			continue
		}
		if item.Status != "" && item.Status != "TRADING" {
			continue
		}
		out = append(out, models.Symbol{
			Exchange: exchange,
			Symbol:   fmt.Sprintf("%s/%s", item.BaseAsset, item.QuoteAsset),
			Base:     item.BaseAsset,
			Quote:    item.QuoteAsset,
			Type:     "swap",
		})
	}
	return out, nil
}

func fetchBinanceDailyVolumesUSDT(ctx context.Context, client *http.Client, symbol string, limit int) ([]float64, error) {
	sym := strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	url := fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=1d&limit=%d", sym, limit)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, readBinanceError(resp)
	}
	defer resp.Body.Close()

	var data [][]any
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("binance empty data")
	}

	items := make([]dailyVolume, 0, len(data))
	for _, row := range data {
		if len(row) < 8 {
			return nil, errors.New("binance invalid kline data")
		}
		openTime, err := toInt64(row[0])
		if err != nil {
			return nil, err
		}
		quoteVol, err := toFloat64(row[7])
		if err != nil {
			return nil, err
		}
		items = append(items, dailyVolume{openTime: openTime, volume: quoteVol})
	}

	return dailyVolumesToUSDT(dropCurrentDailyVolumes(items)), nil
}

func fetchOKXOHLCV(ctx context.Context, client *http.Client, symbol, timeframe string) (models.OHLCV, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return models.OHLCV{}, err
	}
	instID := okxInstID(symbol)
	url := fmt.Sprintf("https://www.okx.com/api/v5/market/candles?instId=%s&bar=%s&limit=1", instID, bar)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return models.OHLCV{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return models.OHLCV{}, fmt.Errorf("okx status: %s", resp.Status)
	}

	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return models.OHLCV{}, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return models.OHLCV{}, fmt.Errorf("okx error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 || len(payload.Data[0]) < 6 {
		return models.OHLCV{}, errors.New("okx empty data")
	}

	openTime, err := strconv.ParseInt(payload.Data[0][0], 10, 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	open, err := strconv.ParseFloat(payload.Data[0][1], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	high, err := strconv.ParseFloat(payload.Data[0][2], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	low, err := strconv.ParseFloat(payload.Data[0][3], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	closePx, err := strconv.ParseFloat(payload.Data[0][4], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	vol, err := strconv.ParseFloat(payload.Data[0][5], 64)
	if err != nil {
		return models.OHLCV{}, err
	}

	return models.OHLCV{
		TS:     openTime,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closePx,
		Volume: vol,
	}, nil
}

func fetchOKXOHLCVRange(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	maxBars := okxWindowMaxBars(ctx)
	if notBefore, ok := okxAvailability.get(symbol, timeframe); ok {
		if end.Before(notBefore) {
			startMS := start.UnixMilli()
			endMS := end.UnixMilli()
			nbMS := notBefore.UnixMilli()
			return nil, fmt.Errorf("okx range before available window: range=[%d(%s),%d(%s)] available_from=%d(%s): %w",
				startMS, formatTimestampMS(startMS),
				endMS, formatTimestampMS(endMS),
				nbMS, formatTimestampMS(nbMS),
				ErrEmptyOHLCV,
			)
		}
		if start.Before(notBefore) {
			start = notBefore
		}
	}
	if expected, _, _, _, err := okxExpectedBars(timeframe, start, end); err == nil && expected > maxBars {
		chunked, chunkErr := fetchOKXOHLCVRangeChunked(ctx, client, logger, controller, exchange, okxHistoryCandlesEndpoint, symbol, timeframe, start, end, maxBars)
		if chunkErr == nil && ohlcvCoversRequestedRange(chunked, timeframe, start, end) {
			return chunked, nil
		}
		recentChunked, recentErr := fetchOKXOHLCVRangeChunked(ctx, client, logger, controller, exchange, okxRecentCandlesEndpoint, symbol, timeframe, start, end, maxBars)
		if recentErr == nil && ohlcvCoversRequestedRange(recentChunked, timeframe, start, end) {
			return recentChunked, nil
		}
	}

	window, windowErr := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, okxHistoryCandlesEndpoint, symbol, timeframe, start, end, maxBars, false)
	if windowErr == nil && ohlcvCoversRequestedRange(window, timeframe, start, end) {
		return window, nil
	}
	windowSec, windowSecErr := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, okxHistoryCandlesEndpoint, symbol, timeframe, start, end, maxBars, true)
	if windowSecErr == nil && ohlcvCoversRequestedRange(windowSec, timeframe, start, end) {
		return windowSec, nil
	}

	recentWindow, recentWindowErr := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, okxRecentCandlesEndpoint, symbol, timeframe, start, end, maxBars, false)
	if recentWindowErr == nil && ohlcvCoversRequestedRange(recentWindow, timeframe, start, end) {
		return recentWindow, nil
	}
	recentWindowSec, recentWindowSecErr := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, okxRecentCandlesEndpoint, symbol, timeframe, start, end, maxBars, true)
	if recentWindowSecErr == nil && ohlcvCoversRequestedRange(recentWindowSec, timeframe, start, end) {
		return recentWindowSec, nil
	}

	data, err := fetchOKXOHLCVRangeBefore(ctx, client, logger, controller, exchange, okxHistoryCandlesEndpoint, symbol, timeframe, start, end, maxBars)
	if err == nil && ohlcvCoversRequestedRange(data, timeframe, start, end) {
		return data, nil
	}

	fallback, fallbackErr := fetchOKXOHLCVRangeAfter(ctx, client, logger, controller, exchange, okxHistoryCandlesEndpoint, symbol, timeframe, start, end, maxBars)
	if fallbackErr == nil && ohlcvCoversRequestedRange(fallback, timeframe, start, end) {
		return fallback, nil
	}

	recentBefore, recentBeforeErr := fetchOKXOHLCVRangeBefore(ctx, client, logger, controller, exchange, okxRecentCandlesEndpoint, symbol, timeframe, start, end, maxBars)
	if recentBeforeErr == nil && ohlcvCoversRequestedRange(recentBefore, timeframe, start, end) {
		return recentBefore, nil
	}

	recentAfter, recentAfterErr := fetchOKXOHLCVRangeAfter(ctx, client, logger, controller, exchange, okxRecentCandlesEndpoint, symbol, timeframe, start, end, maxBars)
	if recentAfterErr == nil && ohlcvCoversRequestedRange(recentAfter, timeframe, start, end) {
		return recentAfter, nil
	}

	merged := mergeOHLCV(window, windowSec, recentWindow, recentWindowSec, data, fallback, recentBefore, recentAfter)
	if len(merged) == 0 {
		return nil, fmt.Errorf("okx range fetch failed: %s", aggregateErrors([]labeledError{
			{label: "history_window", err: windowErr},
			{label: "history_window_sec", err: windowSecErr},
			{label: "recent_window", err: recentWindowErr},
			{label: "recent_window_sec", err: recentWindowSecErr},
			{label: "history_before", err: err},
			{label: "history_after", err: fallbackErr},
			{label: "recent_before", err: recentBeforeErr},
			{label: "recent_after", err: recentAfterErr},
		}))
	}
	if !ohlcvCoversRequestedRange(merged, timeframe, start, end) {
		minTS := normalizeTimestampMS(merged[0].TS)
		maxTS := minTS
		for _, item := range merged[1:] {
			ts := normalizeTimestampMS(item.TS)
			if ts < minTS {
				minTS = ts
			}
			if ts > maxTS {
				maxTS = ts
			}
		}
		startMS := start.UnixMilli()
		endMS := end.UnixMilli()
		okxAvailability.observe(symbol, timeframe, minTS)
		return nil, fmt.Errorf("okx range fetch returned incomplete data coverage: range=[%d(%s),%d(%s)] data=[%d(%s),%d(%s)]: %w",
			startMS, formatTimestampMS(startMS),
			endMS, formatTimestampMS(endMS),
			minTS, formatTimestampMS(minTS),
			maxTS, formatTimestampMS(maxTS),
			ErrEmptyOHLCV,
		)
	}
	return merged, nil
}

const (
	okxHistoryCandlesEndpoint = "history-candles"
	okxRecentCandlesEndpoint  = "candles"
	okxMaxWindowBars          = 300
)

func okxWindowMaxBars(ctx context.Context) int {
	maxBars := okxMaxWindowBars
	if ctxMax := requestMaxBars(ctx); ctxMax > 0 && ctxMax < maxBars {
		maxBars = ctxMax
	}
	return maxBars
}

func fetchOKXOHLCVRangeWindow(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, endpoint, symbol, timeframe string, start, end time.Time, maxBars int, useSeconds bool) ([]models.OHLCV, error) {
	expected, _, startMS, endMS, err := okxExpectedBars(timeframe, start, end)
	if err != nil {
		return nil, err
	}
	if expected <= 0 {
		expected = 1
	}
	if expected > maxBars {
		return nil, fmt.Errorf("range too large for window: %d bars", expected)
	}
	limit := expected + 2
	if limit > maxBars {
		limit = maxBars
	}
	after := endMS + 1
	before := startMS - 1
	if useSeconds {
		after /= 1000
		before /= 1000
	}

	data, err := fetchOKXOHLCVWindow(ctx, client, logger, controller, exchange, endpoint, symbol, timeframe, after, 0, limit)
	if err == nil && ohlcvOverlapsRange(data, start, end) {
		return data, nil
	}

	alt, altErr := fetchOKXOHLCVWindow(ctx, client, logger, controller, exchange, endpoint, symbol, timeframe, 0, before, limit)
	if altErr == nil && ohlcvOverlapsRange(alt, start, end) {
		return alt, nil
	}

	if err != nil && altErr != nil {
		if err.Error() == altErr.Error() {
			return nil, fmt.Errorf("window fetch failed: %v", err)
		}
		return nil, fmt.Errorf("window fetch failed: after=%v; before=%v", err, altErr)
	}
	if err == nil {
		return data, nil
	}
	return alt, altErr
}

type labeledError struct {
	label string
	err   error
}

func aggregateErrors(items []labeledError) string {
	if len(items) == 0 {
		return "no error details"
	}
	labelsByMessage := make(map[string][]string, len(items))
	order := make([]string, 0, len(items))
	for _, item := range items {
		if item.err == nil {
			continue
		}
		message := canonicalErrorMessage(item.err.Error())
		if _, ok := labelsByMessage[message]; !ok {
			order = append(order, message)
		}
		labelsByMessage[message] = append(labelsByMessage[message], item.label)
	}
	if len(order) == 0 {
		return "no error details"
	}
	parts := make([]string, 0, len(order))
	for _, message := range order {
		labels := labelsByMessage[message]
		if len(labels) == 0 {
			parts = append(parts, message)
			continue
		}
		parts = append(parts, fmt.Sprintf("%s (%s)", message, strings.Join(labels, ", ")))
	}
	return strings.Join(parts, "; ")
}

func canonicalErrorMessage(message string) string {
	const windowPrefix = "window fetch failed: "
	if strings.HasPrefix(message, windowPrefix) {
		trimmed := strings.TrimPrefix(message, windowPrefix)
		if !strings.Contains(trimmed, "after_before=") && !strings.Contains(trimmed, "before_after=") {
			return trimmed
		}
	}
	return message
}

func fetchOKXOHLCVWindow(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, endpoint, symbol, timeframe string, after, before int64, limit int) ([]models.OHLCV, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return nil, err
	}
	instID := okxInstID(symbol)
	if limit <= 0 {
		limit = 100
	}
	baseURL := fmt.Sprintf("https://www.okx.com/api/v5/market/%s?instId=%s&bar=%s&limit=%d", endpoint, instID, bar, limit)
	url := baseURL
	if after > 0 {
		url += fmt.Sprintf("&after=%d", after)
	}
	if before > 0 {
		url += fmt.Sprintf("&before=%d", before)
	}
	logOKXRangeRequest(logger, url, endpoint, instID, bar, after, before, limit)
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	resp, err := doGetWithController(ctx, client, controller, requestMeta, url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("okx status: %s", resp.Status)
	}

	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		resp.Body.Close()
		return nil, err
	}
	resp.Body.Close()
	if payload.Code != "" && payload.Code != "0" {
		return nil, fmt.Errorf("okx error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 {
		logOKXRangeResponse(logger, endpoint, url, 0, 0, 0, 0, 0)
		return nil, errors.New("okx empty data")
	}
	out := make([]models.OHLCV, 0, len(payload.Data))
	var minRaw int64
	var maxRaw int64
	for _, row := range payload.Data {
		if len(row) < 6 {
			return nil, errors.New("okx invalid kline data")
		}
		openTimeRaw, err := strconv.ParseInt(row[0], 10, 64)
		if err != nil {
			return nil, err
		}
		openTime := normalizeTimestampMS(openTimeRaw)
		open, err := strconv.ParseFloat(row[1], 64)
		if err != nil {
			return nil, err
		}
		high, err := strconv.ParseFloat(row[2], 64)
		if err != nil {
			return nil, err
		}
		low, err := strconv.ParseFloat(row[3], 64)
		if err != nil {
			return nil, err
		}
		closePx, err := strconv.ParseFloat(row[4], 64)
		if err != nil {
			return nil, err
		}
		vol, err := strconv.ParseFloat(row[5], 64)
		if err != nil {
			return nil, err
		}
		if minRaw == 0 || openTimeRaw < minRaw {
			minRaw = openTimeRaw
		}
		if maxRaw == 0 || openTimeRaw > maxRaw {
			maxRaw = openTimeRaw
		}
		out = append(out, models.OHLCV{
			TS:     openTime,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePx,
			Volume: vol,
		})
	}
	logOKXRangeResponse(logger, endpoint, url, len(out), minRaw, maxRaw, normalizeTimestampMS(minRaw), normalizeTimestampMS(maxRaw))
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	out = dedupeOHLCV(out)
	return out, nil
}

func fetchOKXOHLCVRangeBefore(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, endpoint, symbol, timeframe string, start, end time.Time, maxBars int) ([]models.OHLCV, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return nil, err
	}
	instID := okxInstID(symbol)
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}

	limit := maxBars
	after := endMS + 1
	var out []models.OHLCV
	var prevMinTS int64
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	for {
		url := fmt.Sprintf("https://www.okx.com/api/v5/market/%s?instId=%s&bar=%s&limit=%d&after=%d", endpoint, instID, bar, limit, after)
		logOKXRangeRequest(logger, url, endpoint, instID, bar, after, 0, limit)
		resp, err := doGetWithController(ctx, client, controller, requestMeta, url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("okx status: %s", resp.Status)
		}

		var payload struct {
			Code string     `json:"code"`
			Msg  string     `json:"msg"`
			Data [][]string `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if payload.Code != "" && payload.Code != "0" {
			return nil, fmt.Errorf("okx error: %s", payload.Msg)
		}
		if len(payload.Data) == 0 {
			logOKXRangeResponse(logger, endpoint, url, 0, 0, 0, 0, 0)
			break
		}

		oldest := after
		var minTS int64
		var maxTS int64
		var minRaw int64
		var maxRaw int64
		for _, row := range payload.Data {
			if len(row) < 6 {
				return nil, errors.New("okx invalid kline data")
			}
			openTimeRaw, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, err
			}
			openTime := normalizeTimestampMS(openTimeRaw)
			open, err := strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}
			high, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return nil, err
			}
			low, err := strconv.ParseFloat(row[3], 64)
			if err != nil {
				return nil, err
			}
			closePx, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, err
			}
			vol, err := strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, err
			}
			if minRaw == 0 || openTimeRaw < minRaw {
				minRaw = openTimeRaw
			}
			if maxRaw == 0 || openTimeRaw > maxRaw {
				maxRaw = openTimeRaw
			}
			if minTS == 0 || openTime < minTS {
				minTS = openTime
			}
			if maxTS == 0 || openTime > maxTS {
				maxTS = openTime
			}
			out = append(out, models.OHLCV{
				TS:     openTime,
				Open:   open,
				High:   high,
				Low:    low,
				Close:  closePx,
				Volume: vol,
			})
			if openTime < oldest {
				oldest = openTime
			}
		}
		logOKXRangeResponse(logger, endpoint, url, len(payload.Data), minRaw, maxRaw, normalizeTimestampMS(minRaw), normalizeTimestampMS(maxRaw))
		if minTS >= after || (prevMinTS != 0 && minTS == prevMinTS) {
			return nil, fmt.Errorf("okx range before stalled: after=%d(%s) min_ts=%d(%s) max_ts=%d(%s)",
				after, formatTimestampMS(after),
				minTS, formatTimestampMS(minTS),
				maxTS, formatTimestampMS(maxTS),
			)
		}
		prevMinTS = minTS
		if oldest <= startMS {
			break
		}
		nextAfter := oldest - 1
		if nextAfter <= 0 || nextAfter >= after {
			break
		}
		after = nextAfter
	}

	if len(out) == 0 {
		return nil, errors.New("okx empty data")
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	out = dedupeOHLCV(out)
	return out, nil
}

func fetchOKXOHLCVRangeAfter(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, endpoint, symbol, timeframe string, start, end time.Time, maxBars int) ([]models.OHLCV, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return nil, err
	}
	instID := okxInstID(symbol)
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}

	limit := maxBars
	before := startMS - 1
	var out []models.OHLCV
	var prevMaxTS int64
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	for {
		url := fmt.Sprintf("https://www.okx.com/api/v5/market/%s?instId=%s&bar=%s&limit=%d&before=%d", endpoint, instID, bar, limit, before)
		logOKXRangeRequest(logger, url, endpoint, instID, bar, 0, before, limit)
		resp, err := doGetWithController(ctx, client, controller, requestMeta, url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("okx status: %s", resp.Status)
		}

		var payload struct {
			Code string     `json:"code"`
			Msg  string     `json:"msg"`
			Data [][]string `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if payload.Code != "" && payload.Code != "0" {
			return nil, fmt.Errorf("okx error: %s", payload.Msg)
		}
		if len(payload.Data) == 0 {
			logOKXRangeResponse(logger, endpoint, url, 0, 0, 0, 0, 0)
			break
		}

		var minRaw int64
		var maxRaw int64
		var minTS int64
		var maxTS int64
		for idx, row := range payload.Data {
			if len(row) < 6 {
				return nil, errors.New("okx invalid kline data")
			}
			openTimeRaw, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, err
			}
			openTime := normalizeTimestampMS(openTimeRaw)
			open, err := strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}
			high, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return nil, err
			}
			low, err := strconv.ParseFloat(row[3], 64)
			if err != nil {
				return nil, err
			}
			closePx, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, err
			}
			vol, err := strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, err
			}
			out = append(out, models.OHLCV{
				TS:     openTime,
				Open:   open,
				High:   high,
				Low:    low,
				Close:  closePx,
				Volume: vol,
			})
			if idx == 0 || openTimeRaw < minRaw {
				minRaw = openTimeRaw
			}
			if idx == 0 || openTimeRaw > maxRaw {
				maxRaw = openTimeRaw
			}
			if idx == 0 || openTime < minTS {
				minTS = openTime
			}
			if idx == 0 || openTime > maxTS {
				maxTS = openTime
			}
		}
		logOKXRangeResponse(logger, endpoint, url, len(payload.Data), minRaw, maxRaw, normalizeTimestampMS(minRaw), normalizeTimestampMS(maxRaw))
		if (prevMaxTS != 0 && maxTS == prevMaxTS) || maxTS == 0 || minTS == 0 {
			return nil, fmt.Errorf("okx range after stalled: before=%d(%s) min_ts=%d(%s) max_ts=%d(%s)",
				before, formatTimestampMS(before),
				minTS, formatTimestampMS(minTS),
				maxTS, formatTimestampMS(maxTS),
			)
		}
		prevMaxTS = maxTS
		if maxTS >= endMS {
			break
		}
		nextBefore := maxTS + 1
		if nextBefore <= 0 || nextBefore == before || nextBefore < before {
			break
		}
		before = nextBefore
	}

	if len(out) == 0 {
		return nil, errors.New("okx empty data")
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	out = dedupeOHLCV(out)
	return out, nil
}

func okxExpectedBars(timeframe string, start, end time.Time) (int, int64, int64, int64, error) {
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return 0, 0, 0, 0, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return 0, 0, 0, 0, fmt.Errorf("invalid timeframe duration: %s", timeframe)
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return 0, 0, 0, 0, fmt.Errorf("invalid time range")
	}
	expected := int((endMS-startMS)/step) + 1
	return expected, step, startMS, endMS, nil
}

func fetchOKXOHLCVRangeChunked(ctx context.Context, client *http.Client, logger *zap.Logger, controller *RequestController, exchange, endpoint, symbol, timeframe string, start, end time.Time, maxBars int) ([]models.OHLCV, error) {
	expected, step, startMS, endMS, err := okxExpectedBars(timeframe, start, end)
	if err != nil {
		return nil, err
	}
	if expected <= maxBars {
		return fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, endpoint, symbol, timeframe, start, end, maxBars, false)
	}

	maxBars64 := int64(maxBars)
	var out []models.OHLCV
	for cur := startMS; cur <= endMS; {
		chunkEnd := cur + step*(maxBars64-1)
		if chunkEnd > endMS {
			chunkEnd = endMS
		}
		chunkStartTime := time.UnixMilli(cur)
		chunkEndTime := time.UnixMilli(chunkEnd)

		chunk, err := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, endpoint, symbol, timeframe, chunkStartTime, chunkEndTime, maxBars, false)
		if err != nil || !ohlcvCoversRequestedRange(chunk, timeframe, chunkStartTime, chunkEndTime) {
			chunkSec, secErr := fetchOKXOHLCVRangeWindow(ctx, client, logger, controller, exchange, endpoint, symbol, timeframe, chunkStartTime, chunkEndTime, maxBars, true)
			if secErr != nil || !ohlcvCoversRequestedRange(chunkSec, timeframe, chunkStartTime, chunkEndTime) {
				startLabel := formatTimestampMS(cur)
				endLabel := formatTimestampMS(chunkEnd)
				return nil, fmt.Errorf("okx chunk fetch failed: endpoint=%s range=[%s-%s] err=%v; sec_err=%v",
					endpoint, startLabel, endLabel, err, secErr)
			}
			chunk = chunkSec
		}

		out = append(out, chunk...)
		if chunkEnd == endMS {
			break
		}
		cur = chunkEnd + step
	}

	if len(out) == 0 {
		return nil, errors.New("okx empty data")
	}
	out = dedupeOHLCV(out)
	return out, nil
}

func fetchOKXPerpUSDTMarkets(ctx context.Context, client *http.Client, exchange string) ([]models.Symbol, error) {
	url := "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("okx status: %s", resp.Status)
	}

	var payload struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID    string `json:"instId"`
			BaseCcy   string `json:"baseCcy"`
			QuoteCcy  string `json:"quoteCcy"`
			SettleCcy string `json:"settleCcy"`
			State     string `json:"state"`
			ListTime  string `json:"listTime"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return nil, fmt.Errorf("okx error: %s", payload.Msg)
	}

	out := make([]models.Symbol, 0, len(payload.Data))
	for _, item := range payload.Data {
		quote := item.QuoteCcy
		if quote == "" {
			quote = item.SettleCcy
		}
		if !strings.EqualFold(quote, "USDT") {
			continue
		}
		if item.State != "" && item.State != "live" {
			continue
		}
		base := item.BaseCcy
		if base == "" {
			parts := strings.Split(item.InstID, "-")
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
		out = append(out, models.Symbol{
			Exchange: exchange,
			Symbol:   fmt.Sprintf("%s/%s", base, quote),
			Base:     base,
			Quote:    quote,
			Type:     "swap",
			ListTime: parseInt64Safe(item.ListTime),
		})
	}
	return out, nil
}

func fetchOKXSymbolListTime(ctx context.Context, client *http.Client, symbol string) (int64, error) {
	instID := okxInstID(symbol)
	url := fmt.Sprintf("https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=%s", instID)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("okx status: %s", resp.Status)
	}

	var payload struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			ListTime string `json:"listTime"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return 0, fmt.Errorf("okx error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 {
		return 0, nil
	}
	return parseInt64Safe(payload.Data[0].ListTime), nil
}

func fetchOKXDailyVolumesUSDT(ctx context.Context, client *http.Client, symbol string, limit int) ([]float64, error) {
	instID := okxInstID(symbol)
	url := fmt.Sprintf("https://www.okx.com/api/v5/market/candles?instId=%s&bar=1D&limit=%d", instID, limit)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("okx status: %s", resp.Status)
	}

	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Code != "" && payload.Code != "0" {
		return nil, fmt.Errorf("okx error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 {
		return nil, errors.New("okx empty data")
	}

	items := make([]dailyVolume, 0, len(payload.Data))
	for _, row := range payload.Data {
		if len(row) < 6 {
			return nil, errors.New("okx invalid kline data")
		}
		openTime, err := toInt64(row[0])
		if err != nil {
			return nil, err
		}
		closePx, err := toFloat64(row[4])
		if err != nil {
			return nil, err
		}
		volBase, err := toFloat64(row[5])
		if err != nil {
			return nil, err
		}
		volQuote := volBase * closePx
		if len(row) >= 7 {
			if parsed, err := toFloat64(row[6]); err == nil {
				volQuote = parsed
			}
		}
		items = append(items, dailyVolume{openTime: openTime, volume: volQuote})
	}

	return dailyVolumesToUSDT(dropCurrentDailyVolumes(items)), nil
}

func fetchBitgetOHLCV(ctx context.Context, client *http.Client, symbol, timeframe string) (models.OHLCV, error) {
	granularity, err := bitgetGranularity(timeframe)
	if err != nil {
		return models.OHLCV{}, err
	}
	sym := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(symbol, "/", ""), "-", ""))
	url := fmt.Sprintf("https://api.bitget.com/api/v2/mix/market/candles?symbol=%s&productType=USDT-FUTURES&granularity=%s&limit=1", sym, granularity)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return models.OHLCV{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return models.OHLCV{}, fmt.Errorf("bitget status: %s", resp.Status)
	}

	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return models.OHLCV{}, err
	}
	if payload.Code != "" && payload.Code != "00000" {
		return models.OHLCV{}, fmt.Errorf("bitget error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 || len(payload.Data[0]) < 6 {
		return models.OHLCV{}, errors.New("bitget empty data")
	}

	openTime, err := strconv.ParseInt(payload.Data[0][0], 10, 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	open, err := strconv.ParseFloat(payload.Data[0][1], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	high, err := strconv.ParseFloat(payload.Data[0][2], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	low, err := strconv.ParseFloat(payload.Data[0][3], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	closePx, err := strconv.ParseFloat(payload.Data[0][4], 64)
	if err != nil {
		return models.OHLCV{}, err
	}
	vol, err := strconv.ParseFloat(payload.Data[0][5], 64)
	if err != nil {
		return models.OHLCV{}, err
	}

	return models.OHLCV{
		TS:     openTime,
		Open:   open,
		High:   high,
		Low:    low,
		Close:  closePx,
		Volume: vol,
	}, nil
}

func fetchBitgetOHLCVRange(ctx context.Context, client *http.Client, controller *RequestController, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	granularity, err := bitgetGranularity(timeframe)
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

	sym := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(symbol, "/", ""), "-", ""))
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid time range")
	}

	limit := 200
	cur := startMS
	var out []models.OHLCV
	requestMeta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointOHLCVRange,
	}
	for cur <= endMS {
		chunkEnd := cur + step*int64(limit-1)
		if chunkEnd > endMS {
			chunkEnd = endMS
		}
		url := fmt.Sprintf("https://api.bitget.com/api/v2/mix/market/candles?symbol=%s&productType=USDT-FUTURES&granularity=%s&startTime=%d&endTime=%d&limit=%d", sym, granularity, cur, chunkEnd, limit)
		resp, err := doGetWithController(ctx, client, controller, requestMeta, url)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("bitget status: %s", resp.Status)
		}

		var payload struct {
			Code string     `json:"code"`
			Msg  string     `json:"msg"`
			Data [][]string `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if payload.Code != "" && payload.Code != "00000" {
			return nil, fmt.Errorf("bitget error: %s", payload.Msg)
		}
		if len(payload.Data) == 0 {
			break
		}

		batch := make([]models.OHLCV, 0, len(payload.Data))
		for _, row := range payload.Data {
			if len(row) < 6 {
				return nil, errors.New("bitget invalid kline data")
			}
			openTime, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				return nil, err
			}
			open, err := strconv.ParseFloat(row[1], 64)
			if err != nil {
				return nil, err
			}
			high, err := strconv.ParseFloat(row[2], 64)
			if err != nil {
				return nil, err
			}
			low, err := strconv.ParseFloat(row[3], 64)
			if err != nil {
				return nil, err
			}
			closePx, err := strconv.ParseFloat(row[4], 64)
			if err != nil {
				return nil, err
			}
			vol, err := strconv.ParseFloat(row[5], 64)
			if err != nil {
				return nil, err
			}
			batch = append(batch, models.OHLCV{
				TS:     openTime,
				Open:   open,
				High:   high,
				Low:    low,
				Close:  closePx,
				Volume: vol,
			})
		}
		if len(batch) == 0 {
			break
		}
		sort.Slice(batch, func(i, j int) bool {
			return batch[i].TS < batch[j].TS
		})
		out = append(out, batch...)
		lastTS := batch[len(batch)-1].TS
		next := lastTS + step
		if next <= cur {
			return nil, fmt.Errorf("bitget range fetch stalled at %d", lastTS)
		}
		cur = next
	}

	if len(out) == 0 {
		return nil, errors.New("bitget empty data")
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TS < out[j].TS
	})
	out = dedupeOHLCV(out)
	return out, nil
}

func fetchBitgetPerpUSDTMarkets(ctx context.Context, client *http.Client, exchange string) ([]models.Symbol, error) {
	url := "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bitget status: %s", resp.Status)
	}

	var payload struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			Symbol       string `json:"symbol"`
			BaseCoin     string `json:"baseCoin"`
			QuoteCoin    string `json:"quoteCoin"`
			SymbolStatus string `json:"symbolStatus"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Code != "" && payload.Code != "00000" {
		return nil, fmt.Errorf("bitget error: %s", payload.Msg)
	}

	out := make([]models.Symbol, 0, len(payload.Data))
	for _, item := range payload.Data {
		status := strings.ToLower(item.SymbolStatus)
		if status != "" && status != "normal" && status != "online" {
			continue
		}
		base := item.BaseCoin
		quote := item.QuoteCoin
		if base == "" || quote == "" {
			sym := strings.ToUpper(item.Symbol)
			if strings.HasSuffix(sym, "USDT") && len(sym) > 4 {
				base = sym[:len(sym)-4]
				quote = "USDT"
			}
		}
		if !strings.EqualFold(quote, "USDT") || base == "" {
			continue
		}
		out = append(out, models.Symbol{
			Exchange: exchange,
			Symbol:   fmt.Sprintf("%s/%s", strings.ToUpper(base), strings.ToUpper(quote)),
			Base:     strings.ToUpper(base),
			Quote:    strings.ToUpper(quote),
			Type:     "swap",
		})
	}
	return out, nil
}

func fetchBitgetDailyVolumesUSDT(ctx context.Context, client *http.Client, symbol string, limit int) ([]float64, error) {
	granularity := "86400"
	sym := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(symbol, "/", ""), "-", ""))
	url := fmt.Sprintf("https://api.bitget.com/api/v2/mix/market/candles?symbol=%s&productType=USDT-FUTURES&granularity=%s&limit=%d", sym, granularity, limit)

	resp, err := doGet(ctx, client, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bitget status: %s", resp.Status)
	}

	var payload struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	if payload.Code != "" && payload.Code != "00000" {
		return nil, fmt.Errorf("bitget error: %s", payload.Msg)
	}
	if len(payload.Data) == 0 {
		return nil, errors.New("bitget empty data")
	}

	items := make([]dailyVolume, 0, len(payload.Data))
	for _, row := range payload.Data {
		if len(row) < 6 {
			return nil, errors.New("bitget invalid kline data")
		}
		openTime, err := toInt64(row[0])
		if err != nil {
			return nil, err
		}
		closePx, err := toFloat64(row[4])
		if err != nil {
			return nil, err
		}
		volBase, err := toFloat64(row[5])
		if err != nil {
			return nil, err
		}
		volQuote := volBase * closePx
		if len(row) >= 7 {
			if parsed, err := toFloat64(row[6]); err == nil {
				volQuote = parsed
			}
		}
		items = append(items, dailyVolume{openTime: openTime, volume: volQuote})
	}

	return dailyVolumesToUSDT(dropCurrentDailyVolumes(items)), nil
}

func binanceInterval(tf string) (string, error) {
	switch tf {
	case "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M":
		return tf, nil
	default:
		return "", fmt.Errorf("unsupported timeframe for binance: %s", tf)
	}
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

func bitgetGranularity(tf string) (string, error) {
	switch tf {
	case "1m":
		return "60", nil
	case "3m":
		return "180", nil
	case "5m":
		return "300", nil
	case "15m":
		return "900", nil
	case "30m":
		return "1800", nil
	case "1h":
		return "3600", nil
	case "2h":
		return "7200", nil
	case "4h":
		return "14400", nil
	case "6h":
		return "21600", nil
	case "12h":
		return "43200", nil
	case "1d":
		return "86400", nil
	default:
		return "", fmt.Errorf("unsupported timeframe for bitget: %s", tf)
	}
}

func okxInstID(symbol string) string {
	s := strings.ToUpper(strings.ReplaceAll(symbol, "/", "-"))
	if strings.Contains(s, "-SWAP") {
		return s
	}
	return s + "-SWAP"
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

func ohlcvOverlapsRange(data []models.OHLCV, start, end time.Time) bool {
	if len(data) == 0 {
		return false
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS < startMS {
		return false
	}
	for _, item := range data {
		ts := normalizeTimestampMS(item.TS)
		if ts <= 0 {
			continue
		}
		if ts >= startMS && ts <= endMS {
			return true
		}
	}
	return false
}

func ohlcvCoversRequestedRange(data []models.OHLCV, timeframe string, start, end time.Time) bool {
	if len(data) == 0 {
		return false
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok {
		return ohlcvOverlapsRange(data, start, end)
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return false
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS <= startMS {
		return false
	}
	rangeStart := alignTimestampForward(startMS, step)
	rangeEnd := alignTimestampBackward(endMS-step, step)
	if rangeEnd < rangeStart {
		return ohlcvOverlapsRange(data, start, end)
	}
	seen := make(map[int64]struct{}, len(data))
	for _, item := range data {
		ts := normalizeTimestampMS(item.TS)
		if ts < rangeStart || ts > rangeEnd {
			continue
		}
		seen[ts] = struct{}{}
	}
	for ts := rangeStart; ts <= rangeEnd; ts += step {
		if _, ok := seen[ts]; !ok {
			return false
		}
	}
	return true
}

func alignTimestampForward(ts, step int64) int64 {
	if step <= 0 {
		return ts
	}
	remainder := ts % step
	if remainder == 0 {
		return ts
	}
	if remainder < 0 {
		remainder += step
	}
	return ts + (step - remainder)
}

func alignTimestampBackward(ts, step int64) int64 {
	if step <= 0 {
		return ts
	}
	if ts >= 0 {
		return (ts / step) * step
	}
	return ((ts - step + 1) / step) * step
}

func mergeOHLCV(groups ...[]models.OHLCV) []models.OHLCV {
	var out []models.OHLCV
	for _, group := range groups {
		if len(group) == 0 {
			continue
		}
		out = append(out, group...)
	}
	if len(out) == 0 {
		return nil
	}
	out = dedupeOHLCV(out)
	return out
}

func logOKXRangeRequest(logger *zap.Logger, url, endpoint, instID, bar string, after, before int64, limit int) {
	if logger == nil {
		return
	}
	logger.Debug("okx range request",
		zap.String("endpoint", endpoint),
		zap.String("inst_id", instID),
		zap.String("bar", bar),
		zap.Int64("after", after),
		zap.Int64("before", before),
		zap.Int("limit", limit),
		zap.String("url", url),
	)
}

func logOKXRangeResponse(logger *zap.Logger, endpoint, url string, count int, minRaw, maxRaw, minMS, maxMS int64) {
	if logger == nil {
		return
	}
	logger.Debug("okx range response",
		zap.String("endpoint", endpoint),
		zap.Int("count", count),
		zap.Int64("min_ts_raw", minRaw),
		zap.Int64("max_ts_raw", maxRaw),
		zap.Int64("min_ts_ms", minMS),
		zap.Int64("max_ts_ms", maxMS),
		zap.String("min_time", formatTimestampMS(minMS)),
		zap.String("max_time", formatTimestampMS(maxMS)),
		zap.String("url", url),
	)
}

func formatTimestampMS(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).UTC().Format("20060102_1504")
}

func dedupeOHLCV(items []models.OHLCV) []models.OHLCV {
	if len(items) == 0 {
		return items
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TS < items[j].TS
	})
	j := 0
	for i := 1; i < len(items); i++ {
		if items[i].TS == items[j].TS {
			continue
		}
		j++
		items[j] = items[i]
	}
	return items[:j+1]
}

func readBinanceError(resp *http.Response) error {
	if resp == nil {
		return errors.New("binance nil response")
	}
	body, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	var payload struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && strings.TrimSpace(payload.Msg) != "" {
		message := strings.TrimSpace(payload.Msg)
		return fmt.Errorf("binance error: %s", message)
	}
	if len(body) > 0 {
		message := strings.TrimSpace(string(body))
		return fmt.Errorf("binance status %s: %s", resp.Status, message)
	}
	return fmt.Errorf("binance status: %s", resp.Status)
}

func toFloat64(v any) (float64, error) {
	switch t := v.(type) {
	case string:
		return strconv.ParseFloat(t, 64)
	case json.Number:
		return t.Float64()
	case float64:
		return t, nil
	case int64:
		return float64(t), nil
	case int:
		return float64(t), nil
	default:
		return 0, fmt.Errorf("unsupported number type: %T", v)
	}
}

func toInt64(v any) (int64, error) {
	switch t := v.(type) {
	case string:
		return strconv.ParseInt(t, 10, 64)
	case json.Number:
		return t.Int64()
	case float64:
		return int64(t), nil
	case int64:
		return t, nil
	case int:
		return int64(t), nil
	default:
		return 0, fmt.Errorf("unsupported number type: %T", v)
	}
}

func parseInt64Safe(raw string) int64 {
	value, err := toInt64(strings.TrimSpace(raw))
	if err != nil {
		return 0
	}
	return value
}
