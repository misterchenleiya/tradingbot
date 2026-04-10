package iface

import (
	"context"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type MarketHandler func(data models.MarketData)

type HistoryFetcher interface {
	FetchOHLCVRange(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error)
}

type HistoryRequester interface {
	FetchOHLCVRangePaged(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time, maxPerRequest int) ([]models.MarketData, error)
	FetchOHLCVByLimitPaged(ctx context.Context, exchange, symbol, timeframe string, limit, maxPerRequest int) ([]models.MarketData, error)
}

type SymbolStore interface {
	ListSymbols() ([]models.Symbol, error)
	ListExchanges() ([]models.Exchange, error)
	UpsertSymbol(sym models.Symbol) error
	UpdateSymbolActive(exchange, symbol string, active bool) error
}

type Fetcher interface {
	FetchLatest(ctx context.Context, exchange, symbol, timeframe string) (models.OHLCV, error)
}

type DynamicFetcher interface {
	LoadPerpUSDTMarkets(ctx context.Context, exchange string) ([]models.Symbol, error)
	FetchDailyVolumesUSDT(ctx context.Context, exchange, symbol string, limit int) ([]float64, error)
}

type SymbolListTimeFetcher interface {
	FetchSymbolListTime(ctx context.Context, exchange, symbol string) (int64, error)
}

type OHLCVBoundsWriter interface {
	UpsertOHLCVBound(exchange, symbol string, earliestAvailableTS int64) error
}

type Streamer interface {
	SupportsExchange(exchange string) bool
	Start(ctx context.Context) error
	Subscribe(ctx context.Context, exchange, symbol, timeframe string) error
	Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error
	Events() <-chan models.MarketData
	Errors() <-chan error
}
