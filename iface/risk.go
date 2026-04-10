package iface

import "github.com/misterchenleiya/tradingbot/internal/models"

type Evaluator interface {
	Service
	OnMarketData(data models.MarketData) error
	EvaluateOpenBatch(signals []models.Signal, accountState any) (models.Decision, error)
	EvaluateUpdate(signal models.Signal, position models.Position, accountState any) (models.Decision, error)
	ListOpenPositions(exchange, symbol, timeframe string) ([]models.Position, error)
	ListSignalsByPair(exchange, symbol string) ([]models.Signal, error)
	GetAccountFunds(exchange string) (models.RiskAccountFunds, error)
}

type TrendGuardRefresher interface {
	RefreshTrendGuardCandidate(signal models.Signal, accountState any) error
}
