package risk

import (
	"sort"
	"strings"
	"sync"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type SignalKey struct {
	Exchange string
	Symbol   string
	Strategy string
	ComboKey string
}

type SignalCache struct {
	mu      sync.RWMutex
	signals map[SignalKey]models.Signal
}

func NewSignalCache() *SignalCache {
	return &SignalCache{
		signals: make(map[SignalKey]models.Signal),
	}
}

func signalKey(signal models.Signal) SignalKey {
	_, _, comboKey := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
	return SignalKey{
		Exchange: normalizeExchange(signal.Exchange),
		Symbol:   common.CanonicalSymbol(signal.Symbol),
		Strategy: strings.TrimSpace(signal.Strategy),
		ComboKey: comboKey,
	}
}

func (c *SignalCache) Upsert(signal models.Signal) {
	if c == nil {
		return
	}
	key := signalKey(signal)
	if key.Exchange == "" || key.Symbol == "" || key.Strategy == "" || key.ComboKey == "" {
		return
	}
	c.mu.Lock()
	c.signals[key] = signal
	c.mu.Unlock()
}

func (c *SignalCache) Remove(signal models.Signal) {
	if c == nil {
		return
	}
	key := signalKey(signal)
	if key.Exchange == "" || key.Symbol == "" || key.Strategy == "" || key.ComboKey == "" {
		return
	}
	c.mu.Lock()
	delete(c.signals, key)
	c.mu.Unlock()
}

func (c *SignalCache) Find(exchange, symbol, strategy, comboKey string) (models.Signal, bool) {
	if c == nil {
		return models.Signal{}, false
	}
	strategy, comboKey = resolveSignalLookupIdentity(strategy, comboKey)
	key := SignalKey{
		Exchange: normalizeExchange(exchange),
		Symbol:   common.CanonicalSymbol(symbol),
		Strategy: strings.TrimSpace(strategy),
		ComboKey: comboKey,
	}
	if key.Exchange == "" || key.Symbol == "" || key.Strategy == "" || key.ComboKey == "" {
		return models.Signal{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	signal, ok := c.signals[key]
	return signal, ok
}

func resolveSignalLookupIdentity(strategy, comboKey string) (string, string) {
	strategy = strings.TrimSpace(strategy)
	comboKey = strings.TrimSpace(comboKey)
	if looksLikeComboKey(strategy) && !looksLikeComboKey(comboKey) {
		_, _, normalizedCombo := common.NormalizeStrategyIdentity(strategy, nil, strategy)
		return comboKey, normalizedCombo
	}
	_, _, normalizedCombo := common.NormalizeStrategyIdentity("", nil, comboKey)
	return strategy, normalizedCombo
}

func looksLikeComboKey(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parts := strings.Split(value, "/")
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		switch strings.ToLower(strings.TrimSpace(part)) {
		case "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d":
		default:
			return false
		}
	}
	return true
}

func (c *SignalCache) ListByPair(exchange, symbol string) []models.Signal {
	if c == nil {
		return nil
	}
	exchange = normalizeExchange(exchange)
	symbol = common.CanonicalSymbol(symbol)
	if exchange == "" || symbol == "" {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]models.Signal, 0)
	for key, signal := range c.signals {
		if key.Exchange != exchange || key.Symbol != symbol {
			continue
		}
		out = append(out, signal)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ComboKey != out[j].ComboKey {
			return out[i].ComboKey < out[j].ComboKey
		}
		if out[i].Timeframe != out[j].Timeframe {
			return out[i].Timeframe < out[j].Timeframe
		}
		return out[i].Strategy < out[j].Strategy
	})
	return out
}

func (c *SignalCache) ListGrouped() map[string]map[string]models.Signal {
	if c == nil {
		return map[string]map[string]models.Signal{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]map[string]models.Signal)
	for key, signal := range c.signals {
		outer := key.Exchange + "|" + key.Symbol
		inner := key.Strategy + "|" + key.ComboKey
		bucket := out[outer]
		if bucket == nil {
			bucket = make(map[string]models.Signal)
			out[outer] = bucket
		}
		bucket[inner] = signal
	}
	return out
}
