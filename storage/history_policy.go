package storage

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	defaultHistoryPolicyMaxBars             int64 = 2000
	defaultHistoryPolicyCleanupIntervalSecs       = 3600
)

type HistoryPolicyRule struct {
	MaxHistoryBars int64 `json:"max_history_bars"`
}

type HistoryPolicyCleanup struct {
	Enabled         bool `json:"enabled"`
	IntervalSeconds int  `json:"interval_seconds"`
}

type HistoryPolicy struct {
	Version   int                          `json:"version"`
	Default   HistoryPolicyRule            `json:"default"`
	Exchanges map[string]HistoryPolicyRule `json:"exchanges,omitempty"`
	Symbols   map[string]HistoryPolicyRule `json:"symbols,omitempty"`
	Cleanup   HistoryPolicyCleanup         `json:"cleanup"`
	Enabled   bool                         `json:"-"`
}

func DefaultHistoryPolicy() HistoryPolicy {
	return HistoryPolicy{
		Version: 1,
		Default: HistoryPolicyRule{
			MaxHistoryBars: defaultHistoryPolicyMaxBars,
		},
		Exchanges: map[string]HistoryPolicyRule{},
		Symbols:   map[string]HistoryPolicyRule{},
		Cleanup: HistoryPolicyCleanup{
			Enabled:         true,
			IntervalSeconds: defaultHistoryPolicyCleanupIntervalSecs,
		},
		Enabled: true,
	}
}

func DefaultHistoryPolicyJSON() (string, error) {
	payload, err := json.Marshal(DefaultHistoryPolicy())
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func defaultHistoryPolicyConfigValue() string {
	value, err := DefaultHistoryPolicyJSON()
	if err != nil {
		return `{"version":1,"default":{"max_history_bars":2000},"cleanup":{"enabled":true,"interval_seconds":3600}}`
	}
	return value
}

func ParseHistoryPolicy(raw string) (HistoryPolicy, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return HistoryPolicy{}, nil
	}
	var policy HistoryPolicy
	if err := json.Unmarshal([]byte(value), &policy); err != nil {
		return HistoryPolicy{}, err
	}
	normalizeHistoryPolicy(&policy)
	if err := validateHistoryPolicy(policy); err != nil {
		return HistoryPolicy{}, err
	}
	policy.Enabled = true
	return policy, nil
}

func (s *SQLite) LoadHistoryPolicy() (HistoryPolicy, error) {
	if s == nil {
		return HistoryPolicy{}, fmt.Errorf("nil store")
	}
	value, found, err := s.GetConfigValue("history_policy")
	if err != nil {
		return HistoryPolicy{}, err
	}
	if !found {
		return DefaultHistoryPolicy(), nil
	}
	if strings.TrimSpace(value) == "" {
		return HistoryPolicy{}, nil
	}
	policy, err := ParseHistoryPolicy(value)
	if err != nil {
		return HistoryPolicy{}, fmt.Errorf("invalid config.history_policy json: %w", err)
	}
	return policy, nil
}

func (p HistoryPolicy) MaxBarsFor(exchange, symbol string) (int64, bool) {
	if !p.Enabled {
		return 0, false
	}
	exchangeKey := normalizeHistoryPolicyExchangeKey(exchange)
	symbolKey := normalizeHistoryPolicySymbolKey(exchange, symbol)
	if item, ok := p.Symbols[symbolKey]; ok {
		if item.MaxHistoryBars <= 0 {
			return 0, false
		}
		return item.MaxHistoryBars, true
	}
	if item, ok := p.Exchanges[exchangeKey]; ok {
		if item.MaxHistoryBars <= 0 {
			return 0, false
		}
		return item.MaxHistoryBars, true
	}
	if p.Default.MaxHistoryBars > 0 {
		return p.Default.MaxHistoryBars, true
	}
	return 0, false
}

func normalizeHistoryPolicy(policy *HistoryPolicy) {
	if policy == nil {
		return
	}
	if policy.Version <= 0 {
		policy.Version = 1
	}
	if policy.Exchanges == nil {
		policy.Exchanges = map[string]HistoryPolicyRule{}
	}
	if policy.Symbols == nil {
		policy.Symbols = map[string]HistoryPolicyRule{}
	}
	if policy.Cleanup.IntervalSeconds <= 0 {
		policy.Cleanup.IntervalSeconds = defaultHistoryPolicyCleanupIntervalSecs
	}

	if len(policy.Exchanges) > 0 {
		exchanges := make(map[string]HistoryPolicyRule, len(policy.Exchanges))
		for key, item := range policy.Exchanges {
			name := normalizeHistoryPolicyExchangeKey(key)
			if name == "" {
				continue
			}
			exchanges[name] = item
		}
		policy.Exchanges = exchanges
	}
	if len(policy.Symbols) > 0 {
		symbols := make(map[string]HistoryPolicyRule, len(policy.Symbols))
		for key, item := range policy.Symbols {
			normalized := normalizeHistoryPolicyRawSymbolKey(key)
			if normalized == "" {
				continue
			}
			symbols[normalized] = item
		}
		policy.Symbols = symbols
	}
}

func validateHistoryPolicy(policy HistoryPolicy) error {
	if policy.Default.MaxHistoryBars <= 0 {
		return fmt.Errorf("history_policy.default.max_history_bars must be > 0")
	}
	for name, item := range policy.Exchanges {
		if name == "" {
			return fmt.Errorf("history_policy.exchanges key must not be empty")
		}
		if item.MaxHistoryBars < 0 {
			return fmt.Errorf("history_policy.exchanges[%s].max_history_bars must be >= 0", name)
		}
	}
	for name, item := range policy.Symbols {
		if err := validateHistoryPolicySymbolKey(name); err != nil {
			return fmt.Errorf("history_policy.symbols[%s] key invalid: %w", name, err)
		}
		if item.MaxHistoryBars < 0 {
			return fmt.Errorf("history_policy.symbols[%s].max_history_bars must be >= 0", name)
		}
	}
	if policy.Cleanup.IntervalSeconds <= 0 {
		return fmt.Errorf("history_policy.cleanup.interval_seconds must be > 0")
	}
	return nil
}

func normalizeHistoryPolicyExchangeKey(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}

func normalizeHistoryPolicySymbolKey(exchange, symbol string) string {
	return normalizeHistoryPolicyExchangeKey(exchange) + "|" + strings.ToLower(strings.TrimSpace(symbol))
}

func normalizeHistoryPolicyRawSymbolKey(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, "|")
	if len(parts) != 2 {
		return raw
	}
	return normalizeHistoryPolicySymbolKey(parts[0], parts[1])
}

func validateHistoryPolicySymbolKey(key string) error {
	parts := strings.Split(strings.TrimSpace(key), "|")
	if len(parts) != 2 {
		return fmt.Errorf("expect exchange|symbol")
	}
	if normalizeHistoryPolicyExchangeKey(parts[0]) == "" {
		return fmt.Errorf("empty exchange")
	}
	if strings.TrimSpace(parts[1]) == "" {
		return fmt.Errorf("empty symbol")
	}
	return nil
}
