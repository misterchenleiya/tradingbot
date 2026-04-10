package strategy

import (
	"context"
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type stubMetadataStrategy struct {
	getSignals []models.Signal
	updateNext models.Signal
	updateOK   bool
}

func (s *stubMetadataStrategy) Name() string { return "stub" }

func (s *stubMetadataStrategy) Version() string { return "v1" }

func (s *stubMetadataStrategy) Start(_ context.Context) error { return nil }

func (s *stubMetadataStrategy) Close() error { return nil }

func (s *stubMetadataStrategy) Get(_ models.MarketSnapshot) []models.Signal {
	return append([]models.Signal(nil), s.getSignals...)
}

func (s *stubMetadataStrategy) Update(_ string, _ models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
	return s.updateNext, s.updateOK
}

func TestManagerUpdateKeepsStrategyMetadataFromCurrent(t *testing.T) {
	strat := &stubMetadataStrategy{
		getSignals: []models.Signal{
			{
				Timeframe:          "1h",
				Action:             8,
				HighSide:           1,
				StrategyTimeframes: []string{"15m", "1h"},
				StrategyIndicators: map[string][]string{"ema": []string{"5", "20", "60"}},
			},
		},
		updateNext: models.Signal{
			Timeframe:          "1h",
			Action:             16,
			HighSide:           1,
			StrategyTimeframes: []string{"1m"},
			StrategyIndicators: map[string][]string{"atr": []string{"14"}},
		},
		updateOK: true,
	}
	manager := NewManager(strat)
	snapshot := models.MarketSnapshot{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
	}
	signals := manager.Get(snapshot)
	if len(signals) != 1 {
		t.Fatalf("unexpected signal count: %d", len(signals))
	}
	current := signals[0]
	next, ok := manager.Update("stub", current, snapshot)
	if !ok {
		t.Fatalf("expected update=true")
	}
	if len(next.StrategyTimeframes) != 2 || next.StrategyTimeframes[0] != "15m" || next.StrategyTimeframes[1] != "1h" {
		t.Fatalf("strategy_timeframes should inherit from current, got %#v", next.StrategyTimeframes)
	}
	ema := next.StrategyIndicators["ema"]
	if len(ema) != 3 || ema[0] != "5" || ema[1] != "20" || ema[2] != "60" {
		t.Fatalf("strategy_indicators should inherit from current, got %#v", next.StrategyIndicators)
	}
}
