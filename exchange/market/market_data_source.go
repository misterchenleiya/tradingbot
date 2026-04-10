package market

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	coreexchange "github.com/misterchenleiya/tradingbot/exchange"
	exchangecfg "github.com/misterchenleiya/tradingbot/exchange/config"
	exchangetransport "github.com/misterchenleiya/tradingbot/exchange/transport"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const marketDataHTTPTimeout = 10 * time.Second

type marketDataSource struct {
	exchange string
	client   *http.Client
}

func init() {
	registerMarketDataSource("binance")
	registerMarketDataSource("bitget")
}

func registerMarketDataSource(name string) {
	if err := coreexchange.RegisterMarketDataSource(name, func(cfg exchangecfg.ExchangeConfig) (coreexchange.MarketDataSource, error) {
		return newMarketDataSource(name, cfg)
	}); err != nil {
		panic(fmt.Sprintf("register %s market-data source failed: %v", name, err))
	}
}

func newMarketDataSource(exchangeName string, cfg exchangecfg.ExchangeConfig) (coreexchange.MarketDataSource, error) {
	client, err := newMarketDataHTTPClient(cfg.Proxy)
	if err != nil {
		return nil, err
	}
	return &marketDataSource{
		exchange: strings.ToLower(strings.TrimSpace(exchangeName)),
		client:   client,
	}, nil
}

func newMarketDataHTTPClient(proxyAddr string) (*http.Client, error) {
	proxyAddr = strings.TrimSpace(proxyAddr)
	if proxyAddr == "" {
		return &http.Client{Timeout: marketDataHTTPTimeout}, nil
	}
	dialer, err := exchangetransport.NewProxyDialer(proxyAddr)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: marketDataHTTPTimeout,
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
		},
	}, nil
}

func (s *marketDataSource) FetchLatestOHLCV(ctx context.Context, symbol, timeframe string) (coreexchange.OHLCV, error) {
	item, err := s.fetchLatest(ctx, symbol, timeframe)
	if err != nil {
		return coreexchange.OHLCV{}, err
	}
	return coreexchange.OHLCV{
		TS:     item.TS,
		Open:   item.Open,
		High:   item.High,
		Low:    item.Low,
		Close:  item.Close,
		Volume: item.Volume,
	}, nil
}

func (s *marketDataSource) FetchOHLCVRange(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]coreexchange.OHLCV, error) {
	items, err := s.fetchRange(ctx, symbol, timeframe, start, end)
	if err != nil {
		return nil, err
	}
	out := make([]coreexchange.OHLCV, 0, len(items))
	for _, item := range items {
		out = append(out, coreexchange.OHLCV{
			TS:     item.TS,
			Open:   item.Open,
			High:   item.High,
			Low:    item.Low,
			Close:  item.Close,
			Volume: item.Volume,
		})
	}
	return out, nil
}

func (s *marketDataSource) LoadPerpUSDTMarkets(ctx context.Context) ([]coreexchange.MarketSymbol, error) {
	items, err := s.fetchMarkets(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]coreexchange.MarketSymbol, 0, len(items))
	for _, item := range items {
		out = append(out, coreexchange.MarketSymbol{
			Symbol:   item.Symbol,
			Base:     item.Base,
			Quote:    item.Quote,
			Type:     item.Type,
			ListTime: item.ListTime,
		})
	}
	return out, nil
}

func (s *marketDataSource) FetchDailyVolumesUSDT(ctx context.Context, symbol string, limit int) ([]float64, error) {
	switch s.exchange {
	case "binance":
		return fetchBinanceDailyVolumesUSDT(ctx, s.client, symbol, limit)
	case "bitget":
		return fetchBitgetDailyVolumesUSDT(ctx, s.client, symbol, limit)
	default:
		return nil, fmt.Errorf("unsupported market-data exchange: %s", s.exchange)
	}
}

func (s *marketDataSource) fetchLatest(ctx context.Context, symbol, timeframe string) (models.OHLCV, error) {
	switch s.exchange {
	case "binance":
		return fetchBinanceOHLCV(ctx, s.client, symbol, timeframe)
	case "bitget":
		return fetchBitgetOHLCV(ctx, s.client, symbol, timeframe)
	default:
		return models.OHLCV{}, fmt.Errorf("unsupported market-data exchange: %s", s.exchange)
	}
}

func (s *marketDataSource) fetchRange(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	controller := requestController(ctx)
	switch s.exchange {
	case "binance":
		return fetchBinanceOHLCVRange(ctx, s.client, controller, s.exchange, symbol, timeframe, start, end)
	case "bitget":
		return fetchBitgetOHLCVRange(ctx, s.client, controller, s.exchange, symbol, timeframe, start, end)
	default:
		return nil, fmt.Errorf("unsupported market-data exchange: %s", s.exchange)
	}
}

func (s *marketDataSource) fetchMarkets(ctx context.Context) ([]models.Symbol, error) {
	switch s.exchange {
	case "binance":
		return fetchBinancePerpUSDTMarkets(ctx, s.client, s.exchange)
	case "bitget":
		return fetchBitgetPerpUSDTMarkets(ctx, s.client, s.exchange)
	default:
		return nil, fmt.Errorf("unsupported market-data exchange: %s", s.exchange)
	}
}

var _ coreexchange.MarketDataSource = (*marketDataSource)(nil)
