package iface

import (
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type OHLCVStore interface {
	SaveOHLCV(data models.MarketData) error
	HasOHLCV(exchange, symbol, timeframe string, ts int64) (bool, error)
	DeleteOHLCVBeforeOrEqual(exchange, symbol, timeframe string, ts int64) (int64, error)
	GetOHLCVBound(exchange, symbol string) (int64, bool, error)
	UpsertOHLCVBound(exchange, symbol string, earliestAvailableTS int64) error
	ListSymbols() ([]models.Symbol, error)
	ListExchanges() ([]models.Exchange, error)
	ListRecentOHLCV(exchange, symbol, timeframe string, limit int) ([]models.OHLCV, error)
	ListOHLCVRange(exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error)
}
