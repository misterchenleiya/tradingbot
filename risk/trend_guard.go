package risk

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type trendGuardContext struct {
	Exchange   string
	Symbol     string
	Strategy   string
	Timeframe  string
	HighSide   int
	TrendingTS int64
}

func (r *Live) matchTrendGuardOpenReason(signal models.Signal, cfg RiskTrendGuardConfig) (string, bool) {
	if trendGuardGroupedEnabled(cfg) {
		if r == nil || r.trendGuard == nil {
			return "", false
		}
		return r.trendGuard.authorizeOpen(cfg, signal)
	}
	return r.matchTrendGuardReason(signal, cfg)
}

func (r *Live) matchTrendGuardReason(signal models.Signal, cfg RiskTrendGuardConfig) (string, bool) {
	if r == nil || !cfg.Enabled {
		return "", false
	}
	if r.openPositionCount() == 0 {
		return "", false
	}
	candidate, ok := trendGuardCandidateFromSignal(signal)
	if !ok {
		return "", false
	}
	existing := r.liveTrendGuardContexts()
	for _, ctx := range existing {
		matched, reason := matchTrendGuard(candidate, ctx, cfg)
		if matched {
			return reason, true
		}
	}
	return "", false
}

func (r *Live) liveTrendGuardContexts() []trendGuardContext {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]trendGuardContext, 0, len(r.positions))
	for key, pos := range r.positions {
		if !isPositionOpen(pos) {
			continue
		}
		ctx := trendGuardContext{
			Exchange:  normalizeExchange(pos.Exchange),
			Symbol:    canonicalSymbol(pos.Symbol),
			Strategy:  strings.TrimSpace(pos.StrategyName),
			Timeframe: normalizeTrendGuardTimeframe(pos.Timeframe),
			HighSide:  trendGuardSideFromPosition(pos.PositionSide),
		}
		if item, ok := r.openPositions[key]; ok {
			meta := models.ExtractStrategyContextMeta(item.RowJSON)
			if ctx.Strategy == "" {
				ctx.Strategy = strings.TrimSpace(meta.StrategyName)
			}
			if ctx.Timeframe == "" {
				ctx.Timeframe = normalizeTrendGuardTimeframe(strategyPrimaryTimeframe(meta))
			}
			ctx.TrendingTS = normalizeTimestampMS(int64(meta.TrendingTimestamp))
		}
		if ctx.HighSide == 0 {
			continue
		}
		out = append(out, ctx)
	}
	return out
}

func (r *BackTest) matchTrendGuardReasonLocked(signal models.Signal, cfg RiskTrendGuardConfig) (string, bool) {
	if r == nil || !cfg.Enabled {
		return "", false
	}
	if r.openPositionCountLocked() == 0 {
		return "", false
	}
	candidate, ok := trendGuardCandidateFromSignal(signal)
	if !ok {
		return "", false
	}
	for _, pos := range r.positions {
		if pos == nil || pos.RemainingQty <= 1e-12 {
			continue
		}
		ctx := trendGuardContext{
			Exchange:   normalizeExchange(pos.Exchange),
			Symbol:     canonicalSymbol(pos.Symbol),
			Strategy:   strings.TrimSpace(pos.Strategy),
			Timeframe:  normalizeTrendGuardTimeframe(pos.Timeframe),
			HighSide:   trendGuardSideFromPosition(pos.Side),
			TrendingTS: normalizeTimestampMS(pos.EntryTS),
		}
		matched, reason := matchTrendGuard(candidate, ctx, cfg)
		if matched {
			return reason, true
		}
	}
	return "", false
}

func (r *BackTest) matchTrendGuardOpenReasonLocked(signal models.Signal, cfg RiskTrendGuardConfig) (string, bool) {
	if trendGuardGroupedEnabled(cfg) {
		if r == nil || r.trendGuard == nil {
			return "", false
		}
		return r.trendGuard.authorizeOpen(cfg, signal)
	}
	return r.matchTrendGuardReasonLocked(signal, cfg)
}

func trendGuardCandidateFromSignal(signal models.Signal) (trendGuardContext, bool) {
	ctx := trendGuardContext{
		Exchange:   normalizeExchange(signal.Exchange),
		Symbol:     canonicalSymbol(signal.Symbol),
		Strategy:   strings.TrimSpace(signal.Strategy),
		Timeframe:  normalizeTrendGuardTimeframe(signal.Timeframe),
		HighSide:   trendGuardSideFromHighSide(signal.HighSide),
		TrendingTS: normalizeTimestampMS(int64(signal.TrendingTimestamp)),
	}
	if ctx.Strategy == "" || ctx.Timeframe == "" || ctx.HighSide == 0 || ctx.TrendingTS <= 0 {
		return trendGuardContext{}, false
	}
	return ctx, true
}

func matchTrendGuard(candidate, existing trendGuardContext, cfg RiskTrendGuardConfig) (bool, string) {
	if candidate.HighSide == 0 || existing.HighSide == 0 {
		return false, ""
	}
	if candidate.HighSide != existing.HighSide {
		return false, ""
	}
	if candidate.Strategy == "" || existing.Strategy == "" || candidate.Strategy != existing.Strategy {
		return false, ""
	}
	if candidate.Timeframe == "" || existing.Timeframe == "" || candidate.Timeframe != existing.Timeframe {
		return false, ""
	}
	if candidate.TrendingTS <= 0 || existing.TrendingTS <= 0 {
		return false, ""
	}
	timeframeDuration, ok := trendGuardTimeframeDuration(candidate.Timeframe)
	if !ok {
		return false, ""
	}
	maxLagMS := int64(cfg.MaxStartLagBars) * timeframeDuration.Milliseconds()
	if maxLagMS <= 0 {
		return false, ""
	}
	diffMS := absInt64(candidate.TrendingTS - existing.TrendingTS)
	if diffMS > maxLagMS {
		return false, ""
	}
	lagBars := float64(diffMS) / float64(timeframeDuration.Milliseconds())
	reason := fmt.Sprintf(
		"same trend with %s/%s strategy=%s timeframe=%s lag_bars=%.2f<=%d",
		existing.Exchange,
		existing.Symbol,
		candidate.Strategy,
		candidate.Timeframe,
		lagBars,
		cfg.MaxStartLagBars,
	)
	return true, reason
}

func trendGuardSideFromHighSide(highSide int) int {
	switch {
	case highSide > 0:
		return 1
	case highSide < 0:
		return -1
	default:
		return 0
	}
}

func trendGuardSideFromPosition(side string) int {
	switch normalizePositionSide(side, 0) {
	case positionSideLong:
		return 1
	case positionSideShort:
		return -1
	default:
		return 0
	}
}

func normalizeTrendGuardTimeframe(timeframe string) string {
	return strings.ToLower(strings.TrimSpace(timeframe))
}

func trendGuardTimeframeDuration(timeframe string) (time.Duration, bool) {
	timeframe = normalizeTrendGuardTimeframe(timeframe)
	if len(timeframe) < 2 {
		return 0, false
	}
	value, err := strconv.Atoi(timeframe[:len(timeframe)-1])
	if err != nil || value <= 0 {
		return 0, false
	}
	switch timeframe[len(timeframe)-1] {
	case 'm':
		return time.Duration(value) * time.Minute, true
	case 'h':
		return time.Duration(value) * time.Hour, true
	case 'd':
		return time.Duration(value) * 24 * time.Hour, true
	case 'w':
		return time.Duration(value) * 7 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func absInt64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}
