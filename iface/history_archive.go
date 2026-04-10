package iface

import "github.com/misterchenleiya/tradingbot/internal/models"

type HistoryArchiveMirror interface {
	SyncOpenPositions(exchange string, openPositions []models.RiskOpenPosition) error
	MirrorClosedCandle(data models.MarketData) error
}
