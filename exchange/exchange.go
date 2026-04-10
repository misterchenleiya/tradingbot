package exchange

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/config"
)

type Exchange interface {
	Name() string
	NormalizeSymbol(raw string) (string, error)

	GetInstrument(ctx context.Context, instID string) (Instrument, error)
	GetTickerPrice(ctx context.Context, instID string) (float64, error)
	GetPositions(ctx context.Context, instID string) ([]Position, error)
	GetPositionsHistory(ctx context.Context, instID string) ([]PositionHistory, error)
	GetBalance(ctx context.Context) (BalanceSnapshot, error)

	SetPositionMode(ctx context.Context, mode string) error
	SetLeverage(ctx context.Context, instID, marginMode string, leverage int, posSide string) error

	PlaceOrder(ctx context.Context, req OrderRequest) (string, error)
	GetOrder(ctx context.Context, instID, ordID string) (Order, error)
}

type PriceNormalizer interface {
	NormalizePrice(price float64, inst Instrument) (float64, error)
}

type WSPriceSource interface {
	GetTickerPriceWS(ctx context.Context, instID string) (float64, error)
}

type PositionModeReader interface {
	GetPositionMode(ctx context.Context) (string, error)
}

type TPSLManager interface {
	GetOpenTPSLOrders(ctx context.Context, instID string) ([]TPSLOrder, error)
	CancelTPSLOrders(ctx context.Context, reqs []CancelTPSLOrderRequest) error
	PlaceTPSLOrder(ctx context.Context, req TPSLOrderRequest) (string, error)
}

type AttachAlgoOrderSupport interface {
	SupportsAttachAlgoOrders() bool
}

type MarketDataSource interface {
	FetchLatestOHLCV(ctx context.Context, symbol, timeframe string) (OHLCV, error)
	FetchOHLCVRange(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]OHLCV, error)
	LoadPerpUSDTMarkets(ctx context.Context) ([]MarketSymbol, error)
	FetchDailyVolumesUSDT(ctx context.Context, symbol string, limit int) ([]float64, error)
}

type SymbolListTimeSource interface {
	FetchSymbolListTime(ctx context.Context, symbol string) (int64, error)
}

type Factory func(cfg config.ExchangeConfig) (Exchange, error)
type MarketDataFactory func(cfg config.ExchangeConfig) (MarketDataSource, error)

var registry = map[string]Factory{}
var marketDataRegistry = map[string]MarketDataFactory{}

func Register(name string, factory Factory) error {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return errors.New("empty exchange name")
	}
	if factory == nil {
		return fmt.Errorf("nil factory for exchange %s", normalized)
	}
	if _, exists := registry[normalized]; exists {
		return fmt.Errorf("exchange already registered: %s", normalized)
	}
	registry[normalized] = factory
	return nil
}

func New(name string, cfg config.ExchangeConfig) (Exchange, error) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return nil, errors.New("exchange name is required")
	}
	factory, ok := registry[normalized]
	if !ok {
		return nil, fmt.Errorf("exchange not registered: %s", normalized)
	}
	return factory(cfg)
}

func RegisterMarketDataSource(name string, factory MarketDataFactory) error {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return errors.New("empty exchange name")
	}
	if factory == nil {
		return fmt.Errorf("nil market-data factory for exchange %s", normalized)
	}
	if _, exists := marketDataRegistry[normalized]; exists {
		return fmt.Errorf("market-data source already registered: %s", normalized)
	}
	marketDataRegistry[normalized] = factory
	return nil
}

func NewMarketDataSource(name string, cfg config.ExchangeConfig) (MarketDataSource, error) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return nil, errors.New("exchange name is required")
	}
	factory, ok := marketDataRegistry[normalized]
	if !ok {
		return nil, fmt.Errorf("market-data source not registered: %s", normalized)
	}
	return factory(cfg)
}
