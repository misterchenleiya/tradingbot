package strategy

import (
	"fmt"
	"sort"
	"strings"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/strategy/turtle"
)

var strategyFactories = map[string]func() iface.Strategy{
	"turtle": func() iface.Strategy { return &turtle.Strategy{} },
}

var strategyAliases = map[string]string{
	"turtle": "turtle",
}

func BuildStrategies(names []string) ([]iface.Strategy, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("empty strategy list")
	}
	out := make([]iface.Strategy, 0, len(names))
	for i, name := range names {
		strat, err := NewByName(name)
		if err != nil {
			return nil, fmt.Errorf("strategy[%d]: %w", i, err)
		}
		out = append(out, strat)
	}
	return out, nil
}

func NewByName(name string) (iface.Strategy, error) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return nil, fmt.Errorf("empty strategy name")
	}
	canonical, ok := strategyAliases[normalized]
	if !ok {
		return nil, fmt.Errorf("unknown strategy: %q (available: %s)", name, strings.Join(AvailableStrategyNames(), ", "))
	}
	factory := strategyFactories[canonical]
	if factory == nil {
		return nil, fmt.Errorf("strategy factory missing: %s", canonical)
	}
	return factory(), nil
}

func AvailableStrategyNames() []string {
	names := make([]string, 0, len(strategyFactories))
	for name := range strategyFactories {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
