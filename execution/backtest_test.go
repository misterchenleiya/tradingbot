package execution

import (
	"strings"
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestBackTestPlaceAllowsOpenDecision(t *testing.T) {
	executor := NewBackTest(BackTestConfig{})

	decision := models.Decision{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Action:             models.DecisionActionOpenLong,
		OrderType:          models.OrderTypeMarket,
		Size:               1,
		LeverageMultiplier: 2,
	}

	if err := executor.Place(decision); err != nil {
		t.Fatalf("expected back-test place to accept open decision, got error: %v", err)
	}
}

func TestBackTestPlaceStillValidatesOpenDecision(t *testing.T) {
	executor := NewBackTest(BackTestConfig{})

	decision := models.Decision{
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Action:             models.DecisionActionOpenLong,
		OrderType:          models.OrderTypeMarket,
		Size:               0,
		LeverageMultiplier: 2,
	}

	err := executor.Place(decision)
	if err == nil {
		t.Fatalf("expected validation error")
	}
	if !strings.Contains(err.Error(), "open size must be greater than zero") {
		t.Fatalf("unexpected error: %v", err)
	}
}
