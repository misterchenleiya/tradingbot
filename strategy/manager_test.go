package strategy

import (
	"context"
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type managerMockStrategy struct {
	name     string
	version  string
	getFn    func(models.MarketSnapshot) []models.Signal
	updateFn func(string, models.Signal, models.MarketSnapshot) (models.Signal, bool)
}

func (m *managerMockStrategy) Start(context.Context) error { return nil }
func (m *managerMockStrategy) Close() error                { return nil }
func (m *managerMockStrategy) Name() string                { return m.name }
func (m *managerMockStrategy) Version() string             { return m.version }

func (m *managerMockStrategy) Get(snapshot models.MarketSnapshot) []models.Signal {
	if m.getFn == nil {
		return nil
	}
	return m.getFn(snapshot)
}

func (m *managerMockStrategy) Update(strategy string, current models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool) {
	if m.updateFn == nil {
		return models.Signal{}, false
	}
	return m.updateFn(strategy, current, snapshot)
}

func TestManagerUpdate_AllowsEmptySignalClear(t *testing.T) {
	strat := &managerMockStrategy{
		name:    "s1",
		version: "v1",
		updateFn: func(_ string, _ models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			return models.Signal{}, true
		},
	}
	manager := NewManager(strat)
	current := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "s1",
		HighSide:  1,
		MidSide:   1,
	}
	snapshot := models.MarketSnapshot{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
	}

	next, ok := manager.Update("s1", current, snapshot)
	if !ok {
		t.Fatalf("expected manager update to propagate empty clear signal")
	}
	if next.Exchange != "okx" || next.Symbol != "BTC/USDT" {
		t.Fatalf("expected normalized exchange/symbol, got %#v", next)
	}
	if next.Strategy != "s1" || next.StrategyVersion != "v1" {
		t.Fatalf("expected normalized strategy metadata, got %#v", next)
	}
	if !models.IsEmptySignal(next) {
		t.Fatalf("expected propagated signal to remain empty for lifecycle clear, got %#v", next)
	}
}

func TestManagerUpdate_UnknownStrategyReturnsClearedSignal(t *testing.T) {
	manager := NewManager(&managerMockStrategy{name: "turtle", version: "v1"})
	current := models.Signal{
		Exchange:         "okx",
		Symbol:           "GPS/USDT",
		Timeframe:        "1h",
		Strategy:         "turtle",
		StrategyVersion:  "v0.0.5",
		Action:           8,
		HighSide:         1,
		TriggerTimestamp: 1772919900000,
	}
	snapshot := models.MarketSnapshot{
		Exchange: "okx",
		Symbol:   "GPS/USDT",
	}

	next, ok := manager.Update("turtle", current, snapshot)
	if !ok {
		t.Fatalf("expected update=true for unknown strategy cleanup")
	}
	if !models.IsEmptySignal(next) {
		t.Fatalf("expected cleared signal, got %#v", next)
	}
	if next.Exchange != current.Exchange || next.Symbol != current.Symbol ||
		next.Timeframe != current.Timeframe || next.Strategy != current.Strategy {
		t.Fatalf("expected routing fields preserved, got %#v", next)
	}
}
