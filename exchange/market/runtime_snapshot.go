package market

import (
	"sort"
	"strings"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type RuntimeSymbolSnapshot struct {
	Exchange     string `json:"exchange"`
	Symbol       string `json:"symbol"`
	Dynamic      bool   `json:"dynamic"`
	Active       bool   `json:"active"`
	WSSubscribed bool   `json:"ws_subscribed"`
	LastWSAtMS   int64  `json:"last_ws_at_ms,omitempty"`
}

type RuntimeOHLCVSnapshot struct {
	Exchange    string       `json:"exchange"`
	Symbol      string       `json:"symbol"`
	Timeframe   string       `json:"timeframe"`
	OHLCV       models.OHLCV `json:"ohlcv"`
	Closed      bool         `json:"closed"`
	Source      string       `json:"source,omitempty"`
	Seq         int64        `json:"seq,omitempty"`
	UpdatedAtMS int64        `json:"updated_at_ms,omitempty"`
}

func (s *RealTimeService) ListRuntimeSymbols(exchange string) []RuntimeSymbolSnapshot {
	if s == nil {
		return nil
	}
	exchange = strings.TrimSpace(exchange)
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()

	if len(s.groups) == 0 {
		return nil
	}
	out := make([]RuntimeSymbolSnapshot, 0)
	for name, group := range s.groups {
		if exchange != "" && !strings.EqualFold(name, exchange) {
			continue
		}
		if group == nil {
			continue
		}
		group.mu.RLock()
		for symbol, states := range group.bySymbol {
			if len(states) == 0 {
				continue
			}
			item := RuntimeSymbolSnapshot{
				Exchange: name,
				Symbol:   symbol,
			}
			for _, state := range states {
				if state == nil {
					continue
				}
				item.Dynamic = item.Dynamic || state.dynamic
				item.Active = item.Active || state.active.Load()
				item.WSSubscribed = item.WSSubscribed || state.wsSubscribed.Load()
				lastWSAtMS := state.lastWSAt.Load() / int64(1_000_000)
				if lastWSAtMS > item.LastWSAtMS {
					item.LastWSAtMS = lastWSAtMS
				}
			}
			out = append(out, item)
		}
		group.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

func (s *RealTimeService) ExchangeRuntimeState(exchange string) (string, string) {
	if s == nil {
		return "", ""
	}
	return s.exchangeStatus(exchange)
}

func (s *RealTimeService) LookupRuntimeOHLCV(exchange, symbol, timeframe string) (RuntimeOHLCVSnapshot, bool) {
	if s == nil {
		return RuntimeOHLCVSnapshot{}, false
	}
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	timeframe = strings.TrimSpace(timeframe)
	if exchange == "" || symbol == "" || timeframe == "" {
		return RuntimeOHLCVSnapshot{}, false
	}

	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()

	for name, group := range s.groups {
		if !strings.EqualFold(name, exchange) || group == nil {
			continue
		}
		group.mu.RLock()
		state := group.byKey[symbolKey(symbol, timeframe)]
		group.mu.RUnlock()
		if state == nil {
			return RuntimeOHLCVSnapshot{}, false
		}
		return state.runtimeSnapshot()
	}
	return RuntimeOHLCVSnapshot{}, false
}

func (s *RealTimeService) ListRuntimeOHLCV(exchange, timeframe string) []RuntimeOHLCVSnapshot {
	if s == nil {
		return nil
	}
	exchange = strings.TrimSpace(exchange)
	timeframe = strings.TrimSpace(timeframe)

	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()

	out := make([]RuntimeOHLCVSnapshot, 0)
	for name, group := range s.groups {
		if exchange != "" && !strings.EqualFold(name, exchange) {
			continue
		}
		if group == nil {
			continue
		}
		group.mu.RLock()
		for _, state := range group.byKey {
			if state == nil {
				continue
			}
			snapshot, ok := state.runtimeSnapshot()
			if !ok {
				continue
			}
			if timeframe != "" && !strings.EqualFold(snapshot.Timeframe, timeframe) {
				continue
			}
			out = append(out, snapshot)
		}
		group.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Exchange != out[j].Exchange {
			return out[i].Exchange < out[j].Exchange
		}
		if out[i].Symbol != out[j].Symbol {
			return out[i].Symbol < out[j].Symbol
		}
		return out[i].Timeframe < out[j].Timeframe
	})
	return out
}
