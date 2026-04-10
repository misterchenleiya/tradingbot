package iface

import "github.com/misterchenleiya/tradingbot/internal/models"

type Strategy interface {
	Service
	Get(snapshot models.MarketSnapshot) []models.Signal
	Update(strategy string, signal models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool)
	Name() string
	Version() string
}
