package core

import (
	"sort"
	"strings"
	"sync"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/iface"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const oneMinuteTimeframe = "1m"

type timeframePlan struct {
	store iface.OHLCVStore
	log   *zap.Logger

	mu               sync.RWMutex
	exchangeDefaults map[string][]string
	pairTimeframes   map[string][]string
}

func newTimeframePlan(store iface.OHLCVStore, logger *zap.Logger) *timeframePlan {
	if logger == nil {
		logger = glog.Nop()
	}
	plan := &timeframePlan{
		store:            store,
		log:              logger,
		exchangeDefaults: make(map[string][]string),
		pairTimeframes:   make(map[string][]string),
	}
	_ = plan.reload()
	return plan
}

func (p *timeframePlan) Resolve(exchange, symbol string) []string {
	configured := p.ResolveConfigured(exchange, symbol)
	if len(configured) > 0 {
		return configured
	}
	return []string{oneMinuteTimeframe}
}

func (p *timeframePlan) ResolveConfigured(exchange, symbol string) []string {
	if p == nil {
		return nil
	}
	key := pairKey(exchange, symbol)
	p.mu.RLock()
	timeframes, ok := p.pairTimeframes[key]
	p.mu.RUnlock()
	if ok && len(timeframes) > 0 {
		return cloneTimeframes(timeframes)
	}
	if err := p.reload(); err != nil {
		p.log.Warn("reload timeframe plan failed",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Error(err),
		)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if timeframes, ok := p.pairTimeframes[key]; ok && len(timeframes) > 0 {
		return cloneTimeframes(timeframes)
	}
	if defaults, ok := p.exchangeDefaults[normalizeExchange(exchange)]; ok && len(defaults) > 0 {
		return cloneTimeframes(defaults)
	}
	return nil
}

func (p *timeframePlan) reload() error {
	if p == nil || p.store == nil {
		return nil
	}
	exchanges, err := p.store.ListExchanges()
	if err != nil {
		return err
	}
	symbols, err := p.store.ListSymbols()
	if err != nil {
		return err
	}
	nextDefaults := make(map[string][]string)
	for _, ex := range exchanges {
		if !ex.Active {
			continue
		}
		defaultTF := []string{oneMinuteTimeframe}
		timeframes, parseErr := parseExchangeTimeframes(ex.Timeframes)
		if parseErr != nil {
			p.log.Warn("parse exchange timeframes failed for timeframe plan",
				zap.String("exchange", ex.Name),
				zap.String("timeframes", ex.Timeframes),
				zap.Error(parseErr),
			)
		} else {
			defaultTF = timeframes
		}
		nextDefaults[normalizeExchange(ex.Name)] = normalizePlanTimeframes(defaultTF)
	}

	nextPairs := make(map[string][]string)
	for _, sym := range symbols {
		if !sym.Active {
			continue
		}
		exchange := normalizeExchange(sym.Exchange)
		defaultTF := nextDefaults[exchange]
		if len(defaultTF) == 0 {
			defaultTF = []string{oneMinuteTimeframe}
		}
		target, parseErr := parseSymbolTimeframes(sym.Timeframes, defaultTF)
		if parseErr != nil {
			p.log.Warn("parse symbol timeframes failed for timeframe plan",
				zap.String("exchange", sym.Exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(parseErr),
			)
			target = defaultTF
		}
		nextPairs[pairKey(sym.Exchange, sym.Symbol)] = normalizePlanTimeframes(target)
	}

	p.mu.Lock()
	p.exchangeDefaults = nextDefaults
	p.pairTimeframes = nextPairs
	p.mu.Unlock()
	return nil
}

func normalizePlanTimeframes(in []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(in))
	for _, item := range in {
		tf := strings.TrimSpace(item)
		if tf == "" {
			continue
		}
		if _, ok := market.TimeframeDuration(tf); !ok {
			continue
		}
		if _, ok := seen[tf]; ok {
			continue
		}
		seen[tf] = struct{}{}
		out = append(out, tf)
	}
	sort.Slice(out, func(i, j int) bool {
		di, _ := market.TimeframeDuration(out[i])
		dj, _ := market.TimeframeDuration(out[j])
		if di == dj {
			return out[i] < out[j]
		}
		return di < dj
	})
	return out
}

func ensureOneMinuteTimeframe(in []string) []string {
	out := normalizePlanTimeframes(in)
	if len(out) == 0 {
		return []string{oneMinuteTimeframe}
	}
	if containsTimeframe(out, oneMinuteTimeframe) {
		return out
	}
	merged := append(cloneTimeframes(out), oneMinuteTimeframe)
	return normalizePlanTimeframes(merged)
}

func cloneTimeframes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func containsTimeframe(timeframes []string, timeframe string) bool {
	for _, item := range timeframes {
		if strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(timeframe)) {
			return true
		}
	}
	return false
}

func normalizeExchange(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}
