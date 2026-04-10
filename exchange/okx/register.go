package okx

import (
	"fmt"

	"github.com/misterchenleiya/tradingbot/exchange"
	"github.com/misterchenleiya/tradingbot/exchange/config"
)

func init() {
	if err := exchange.Register("okx", func(cfg config.ExchangeConfig) (exchange.Exchange, error) {
		return New(cfg)
	}); err != nil {
		panic(fmt.Sprintf("register okx exchange failed: %v", err))
	}
	if err := exchange.RegisterMarketDataSource("okx", func(cfg config.ExchangeConfig) (exchange.MarketDataSource, error) {
		return New(cfg)
	}); err != nil {
		panic(fmt.Sprintf("register okx market-data source failed: %v", err))
	}
}
