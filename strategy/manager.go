package strategy

import (
	"context"
	"sort"
	"strings"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type Manager struct {
	strategies []iface.Strategy
	byName     map[string]iface.Strategy
	logger     *zap.Logger
}

func NewManager(strategies ...iface.Strategy) *Manager {
	filtered := make([]iface.Strategy, 0, len(strategies))
	byName := make(map[string]iface.Strategy)
	for _, strat := range strategies {
		if strat == nil {
			continue
		}
		filtered = append(filtered, strat)
		name := strat.Name()
		if name != "" {
			byName[name] = strat
		}
	}
	return &Manager{
		strategies: filtered,
		byName:     byName,
		logger:     glog.Nop(),
	}
}

func (m *Manager) Start(ctx context.Context) error {
	if m == nil {
		return nil
	}
	logger := m.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("strategy start",
		zap.Int("strategy_count", len(m.strategies)),
		zap.Strings("strategy_names", managerStrategyNames(m.strategies)),
	)
	defer logger.Info("strategy started")
	for _, strat := range m.strategies {
		if strat == nil {
			continue
		}
		if err := strat.Start(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	logger := m.logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("strategy close")
	defer logger.Info("strategy closed")
	for i := len(m.strategies) - 1; i >= 0; i-- {
		strat := m.strategies[i]
		if strat == nil {
			continue
		}
		if err := strat.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Get(snapshot models.MarketSnapshot) []models.Signal {
	if m == nil {
		return nil
	}
	var out []models.Signal
	for _, strat := range m.strategies {
		if strat == nil {
			continue
		}
		signals := strat.Get(snapshot)
		if len(signals) == 0 {
			continue
		}
		name := strat.Name()
		version := strat.Version()
		for _, signal := range signals {
			if models.IsEmptySignal(signal) {
				continue
			}
			signal.Exchange = snapshot.Exchange
			signal.Symbol = snapshot.Symbol
			signal.Strategy = name
			signal.StrategyVersion = version
			signal.StrategyTimeframes = cloneNonEmptyStrings(signal.StrategyTimeframes)
			signal.StrategyIndicators = cloneIndicators(signal.StrategyIndicators)
			out = append(out, signal)
		}
	}
	return out
}

func (m *Manager) Update(strategyName string, current models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool) {
	if m == nil {
		return models.Signal{}, false
	}
	strat := m.byName[strategyName]
	if strat == nil {
		// The signal belongs to a strategy no longer loaded in this runtime.
		// Return a cleared signal so upstream lifecycle logic can remove stale cache entries.
		return models.ClearSignalForRemoval(current), true
	}
	next, ok := strat.Update(strategyName, current, snapshot)
	if !ok {
		return models.Signal{}, false
	}
	next.Exchange = snapshot.Exchange
	next.Symbol = snapshot.Symbol
	next.Strategy = strat.Name()
	next.StrategyVersion = strat.Version()
	next.StrategyTimeframes = cloneNonEmptyStrings(current.StrategyTimeframes)
	next.StrategyIndicators = cloneIndicators(current.StrategyIndicators)
	return next, true
}

func (m *Manager) Name() string {
	return "manager"
}

func (m *Manager) Version() string {
	return ""
}

func (m *Manager) StrategyNames() []string {
	if m == nil {
		return nil
	}
	return managerStrategyNames(m.strategies)
}

func (m *Manager) SetLogger(logger *zap.Logger) {
	if m == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	m.logger = logger
	for _, strat := range m.strategies {
		if strat == nil {
			continue
		}
		setter, ok := strat.(interface{ SetLogger(*zap.Logger) })
		if !ok {
			continue
		}
		setter.SetLogger(logger)
	}
}

func managerStrategyNames(strategies []iface.Strategy) []string {
	if len(strategies) == 0 {
		return nil
	}
	names := make([]string, 0, len(strategies))
	for _, strat := range strategies {
		if strat == nil {
			continue
		}
		names = append(names, strat.Name())
	}
	sort.Strings(names)
	return names
}

func cloneNonEmptyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, item := range values {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func cloneIndicators(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for rawName, values := range input {
		name := strings.TrimSpace(rawName)
		normalized := cloneNonEmptyStrings(values)
		if name == "" || len(normalized) == 0 {
			continue
		}
		out[name] = normalized
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
