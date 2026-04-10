package exchange

import (
	"fmt"
	"strings"

	"github.com/misterchenleiya/tradingbot/exchange/config"
)

type Plane string

const (
	PlaneMarket Plane = "market"
	PlaneTrade  Plane = "trade"
)

type PlaneSource struct {
	Name        string
	RateLimitMS int
	MarketProxy string
	TradeProxy  string
}

type PlanePairConfig struct {
	Exchange string
	Market   config.ExchangeConfig
	Trade    config.ExchangeConfig
}

type PlanePair struct {
	Market Exchange
	Trade  Exchange
}

func (s PlaneSource) Validate() error {
	name := strings.ToLower(strings.TrimSpace(s.Name))
	if name == "" {
		return fmt.Errorf("exchange name is required")
	}
	if s.RateLimitMS < 0 {
		return fmt.Errorf("invalid rate_limit_ms: %d", s.RateLimitMS)
	}
	return nil
}

func BuildPlanePairConfig(source PlaneSource, base config.ExchangeConfig) (PlanePairConfig, error) {
	if err := source.Validate(); err != nil {
		return PlanePairConfig{}, err
	}
	name := strings.ToLower(strings.TrimSpace(source.Name))

	marketCfg := base
	tradeCfg := base

	marketCfg.Name = name
	tradeCfg.Name = name
	if source.RateLimitMS > 0 {
		marketCfg.RateLimitMS = source.RateLimitMS
		tradeCfg.RateLimitMS = source.RateLimitMS
	}
	marketCfg.Proxy = strings.TrimSpace(source.MarketProxy)
	tradeCfg.Proxy = strings.TrimSpace(source.TradeProxy)

	// market-plane should stay unauthenticated even when base has credentials.
	marketCfg.APIKey = ""
	marketCfg.SecretKey = ""
	marketCfg.Passphrase = ""
	marketCfg.Simulated = false

	return PlanePairConfig{
		Exchange: name,
		Market:   marketCfg,
		Trade:    tradeCfg,
	}, nil
}

func NewPlanePair(source PlaneSource, base config.ExchangeConfig) (PlanePair, error) {
	pairCfg, err := BuildPlanePairConfig(source, base)
	if err != nil {
		return PlanePair{}, err
	}
	marketClient, err := New(pairCfg.Exchange, pairCfg.Market)
	if err != nil {
		return PlanePair{}, fmt.Errorf("create market-plane exchange (%s): %w", pairCfg.Exchange, err)
	}
	tradeClient, err := New(pairCfg.Exchange, pairCfg.Trade)
	if err != nil {
		return PlanePair{}, fmt.Errorf("create trade-plane exchange (%s): %w", pairCfg.Exchange, err)
	}
	return PlanePair{
		Market: marketClient,
		Trade:  tradeClient,
	}, nil
}
