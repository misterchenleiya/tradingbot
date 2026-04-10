package iface

import (
	coreexchange "github.com/misterchenleiya/tradingbot/exchange"
	exchangecfg "github.com/misterchenleiya/tradingbot/exchange/config"
)

type Exchange = coreexchange.Exchange
type PriceNormalizer = coreexchange.PriceNormalizer
type WSPriceSource = coreexchange.WSPriceSource
type PositionModeReader = coreexchange.PositionModeReader
type TPSLManager = coreexchange.TPSLManager
type AttachAlgoOrderSupport = coreexchange.AttachAlgoOrderSupport
type ExchangeMarketDataSource = coreexchange.MarketDataSource
type ExchangeSymbolListTimeSource = coreexchange.SymbolListTimeSource
type ExchangeFactory = coreexchange.Factory
type ExchangeMarketDataFactory = coreexchange.MarketDataFactory
type ExchangePlane = coreexchange.Plane

type Instrument = coreexchange.Instrument
type ExchangeOHLCV = coreexchange.OHLCV
type ExchangeMarketSymbol = coreexchange.MarketSymbol
type Position = coreexchange.Position
type PositionHistory = coreexchange.PositionHistory
type Balance = coreexchange.Balance
type BalanceSnapshot = coreexchange.BalanceSnapshot
type Order = coreexchange.Order
type OrderRequest = coreexchange.OrderRequest
type AttachAlgoOrder = coreexchange.AttachAlgoOrder
type TPSLOrder = coreexchange.TPSLOrder
type TPSLOrderRequest = coreexchange.TPSLOrderRequest
type CancelTPSLOrderRequest = coreexchange.CancelTPSLOrderRequest
type ExchangePlaneSource = coreexchange.PlaneSource
type ExchangePlanePairConfig = coreexchange.PlanePairConfig
type ExchangePlanePair = coreexchange.PlanePair
type ExchangeConfig = exchangecfg.ExchangeConfig

const (
	ExchangePlaneMarket = coreexchange.PlaneMarket
	ExchangePlaneTrade  = coreexchange.PlaneTrade
)

func RegisterExchange(name string, factory ExchangeFactory) error {
	return coreexchange.Register(name, factory)
}

func NewExchange(name string, cfg ExchangeConfig) (Exchange, error) {
	return coreexchange.New(name, cfg)
}

func RegisterExchangeMarketDataSource(name string, factory ExchangeMarketDataFactory) error {
	return coreexchange.RegisterMarketDataSource(name, factory)
}

func NewExchangeMarketDataSource(name string, cfg ExchangeConfig) (ExchangeMarketDataSource, error) {
	return coreexchange.NewMarketDataSource(name, cfg)
}

func BuildExchangePlanePairConfig(source ExchangePlaneSource, base ExchangeConfig) (ExchangePlanePairConfig, error) {
	return coreexchange.BuildPlanePairConfig(source, base)
}

func NewExchangePlanePair(source ExchangePlaneSource, base ExchangeConfig) (ExchangePlanePair, error) {
	return coreexchange.NewPlanePair(source, base)
}
