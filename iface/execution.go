package iface

import "github.com/misterchenleiya/tradingbot/internal/models"

type Executor interface {
	Service
	Place(decision models.Decision) error
}

type MarketAware interface {
	OnMarketData(data models.MarketData)
	Finalize() error
}
