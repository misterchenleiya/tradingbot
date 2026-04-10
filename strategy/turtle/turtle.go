package turtle

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"github.com/misterchenleiya/tradingbot/ta"
	"go.uber.org/zap"
)

type Strategy struct {
	cfg    Config
	logger *zap.Logger
}

func (s *Strategy) Start(_ context.Context) (err error) {
	cfg := normalizeConfig(s.cfg)
	logger := s.getLogger()
	logger.Info("turtle strategy start",
		zap.String("strategy", s.Name()),
		zap.String("version", s.Version()),
		zap.Int("entry_period", cfg.EntryPeriod),
		zap.Int("exit_period", cfg.ExitPeriod),
		zap.Int("atr_period", cfg.ATRPeriod),
		zap.Float64("stop_loss_atr_multiplier", cfg.StopLossATRMultiplier),
		zap.Float64("trailing_atr_multiplier", cfg.TrailingATRMultiplier),
	)
	defer logger.Info("turtle strategy started")
	return nil
}

func (s *Strategy) Close() (err error) {
	logger := s.getLogger()
	logger.Info("turtle strategy close")
	defer logger.Info("turtle strategy closed")
	return nil
}

func (s *Strategy) Name() string {
	return "turtle"
}

func (s *Strategy) Version() string {
	return turtleStrategyVersion
}

func (s *Strategy) Get(snapshot models.MarketSnapshot) []models.Signal {
	cfg := normalizeConfig(s.cfg)
	timeframe, series, ok := selectClosedSeries(snapshot, "")
	if !ok || !hasEnoughBars(series, cfg) {
		return nil
	}
	signal, ok := buildOpenSignal(timeframe, series, cfg)
	if !ok {
		return nil
	}
	signal.StrategyTimeframes = turtleStrategyTimeframes(timeframe)
	signal.StrategyIndicators = turtleStrategyIndicators(cfg)
	return []models.Signal{signal}
}

func (s *Strategy) Update(_ string, current models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool) {
	cfg := normalizeConfig(s.cfg)
	timeframeHint := strings.TrimSpace(current.Timeframe)
	timeframe, series, ok := selectClosedSeries(snapshot, timeframeHint)
	if !ok || !hasEnoughBars(series, cfg) {
		if current.Action != 0 {
			return models.Signal{}, false
		}
		cleared := clearedSignal(firstNonEmpty(timeframeHint, snapshot.EventTimeframe))
		if sameSignalCore(current, cleared) {
			return models.Signal{}, false
		}
		return cleared, true
	}

	last := len(series) - 1
	lastTS := int(series[last].TS)
	if lastTS <= 0 {
		return models.Signal{}, false
	}

	if current.Action == 0 {
		next, openOK := buildOpenSignal(timeframe, series, cfg)
		if openOK {
			if sameSignalCore(current, next) {
				return models.Signal{}, false
			}
			return next, true
		}
		cleared := clearedSignal(timeframe)
		if sameSignalCore(current, cleared) {
			return models.Signal{}, false
		}
		return cleared, true
	}

	if shouldCloseLong(series, cfg.ExitPeriod) {
		next := current
		next.Timeframe = timeframe
		next.Action = actionCloseAll
		next.OrderType = ""
		next.Exit = series[last].Close
		next.HighSide = trendSideLong
		next.MidSide = midSideLong
		next.TriggerTimestamp = lastTS
		if next.TrendingTimestamp <= 0 {
			next.TrendingTimestamp = lastTS
		}
		if sameSignalCore(current, next) {
			return models.Signal{}, false
		}
		return next, true
	}

	atrValue := latestATR(series, cfg.ATRPeriod)
	if atrValue <= 0 {
		return models.Signal{}, false
	}
	candidateSL := series[last].Close - atrValue*cfg.TrailingATRMultiplier
	if !floatcmp.GT(candidateSL, 0) || !floatcmp.LT(candidateSL, series[last].Close) {
		return models.Signal{}, false
	}
	if !floatcmp.GT(candidateSL, current.SL) {
		return models.Signal{}, false
	}

	next := current
	next.Timeframe = timeframe
	next.Action = actionMove
	next.OrderType = ""
	next.HighSide = trendSideLong
	next.MidSide = midSideLong
	next.SL = candidateSL
	next.Exit = 0
	next.TriggerTimestamp = lastTS
	if next.TrendingTimestamp <= 0 {
		next.TrendingTimestamp = lastTS
	}
	if sameSignalCore(current, next) {
		return models.Signal{}, false
	}
	return next, true
}

func (s *Strategy) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	s.logger = logger
}

func (s *Strategy) getLogger() *zap.Logger {
	if s == nil || s.logger == nil {
		return glog.Nop()
	}
	return s.logger
}

func buildOpenSignal(timeframe string, series []models.OHLCV, cfg Config) (models.Signal, bool) {
	if !shouldOpenLong(series, cfg.EntryPeriod) {
		return models.Signal{}, false
	}
	last := len(series) - 1
	entry := series[last].Close
	if !floatcmp.GT(entry, 0) {
		return models.Signal{}, false
	}
	atrValue := latestATR(series, cfg.ATRPeriod)
	sl := entry - atrValue*cfg.StopLossATRMultiplier
	if !floatcmp.GT(sl, 0) || !floatcmp.LT(sl, entry) {
		sl = entry * (1 - cfg.StopLossFallbackRate)
	}
	if !floatcmp.GT(sl, 0) || !floatcmp.LT(sl, entry) {
		return models.Signal{}, false
	}

	lastTS := int(series[last].TS)
	if lastTS <= 0 {
		return models.Signal{}, false
	}
	return models.Signal{
		Timeframe:         timeframe,
		OrderType:         models.OrderTypeMarket,
		Entry:             entry,
		SL:                sl,
		TP:                0,
		Exit:              0,
		Action:            actionOpen,
		HighSide:          trendSideLong,
		MidSide:           midSideLong,
		TrendingTimestamp: lastTS,
		TriggerTimestamp:  lastTS,
	}, true
}

func shouldOpenLong(series []models.OHLCV, period int) bool {
	if len(series) < period+1 {
		return false
	}
	last := len(series) - 1
	previous := series[:last]
	channel, err := ta.Donchian(previous, period)
	if err != nil || len(channel.Upper) == 0 {
		return false
	}
	upper := channel.Upper[len(previous)-1]
	return floatcmp.GT(series[last].Close, upper)
}

func shouldCloseLong(series []models.OHLCV, period int) bool {
	if len(series) < period+1 {
		return false
	}
	last := len(series) - 1
	previous := series[:last]
	channel, err := ta.Donchian(previous, period)
	if err != nil || len(channel.Lower) == 0 {
		return false
	}
	lower := channel.Lower[len(previous)-1]
	return floatcmp.LT(series[last].Close, lower)
}

func latestATR(series []models.OHLCV, period int) float64 {
	result, err := ta.ATR(series, period)
	if err != nil || len(result.Values) == 0 {
		return 0
	}
	last := len(series) - 1
	if last < 0 || last >= len(result.Values) {
		return 0
	}
	if !floatcmp.GT(result.Values[last], 0) {
		return 0
	}
	return result.Values[last]
}

func selectClosedSeries(snapshot models.MarketSnapshot, preferred string) (string, []models.OHLCV, bool) {
	preferred = strings.TrimSpace(preferred)
	if preferred != "" {
		if series, ok := closedSeries(snapshot, preferred); ok {
			return preferred, series, true
		}
	}
	timeframe, ok := selectTimeframe(snapshot)
	if !ok {
		return "", nil, false
	}
	series, ok := closedSeries(snapshot, timeframe)
	if !ok {
		return "", nil, false
	}
	return timeframe, series, true
}

func selectTimeframe(snapshot models.MarketSnapshot) (string, bool) {
	var (
		selected string
		bestDur  time.Duration
		ok       bool
	)
	for timeframe := range snapshot.Series {
		dur, durOK := market.TimeframeDuration(timeframe)
		if !durOK {
			continue
		}
		meta, metaOK := snapshot.Meta[timeframe]
		if !metaOK || meta.LastIndex < 0 {
			continue
		}
		if !ok || dur > bestDur {
			selected = timeframe
			bestDur = dur
			ok = true
		}
	}
	if ok {
		return selected, true
	}
	if snapshot.EventTimeframe != "" {
		return snapshot.EventTimeframe, true
	}
	return "", false
}

func turtleStrategyTimeframes(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func turtleStrategyIndicators(cfg Config) map[string][]string {
	return map[string][]string{
		"donchian": []string{
			strconv.Itoa(cfg.EntryPeriod),
			strconv.Itoa(cfg.ExitPeriod),
		},
		"atr": []string{
			strconv.Itoa(cfg.ATRPeriod),
		},
	}
}

func closedSeries(snapshot models.MarketSnapshot, timeframe string) ([]models.OHLCV, bool) {
	series, ok := snapshot.Series[timeframe]
	if !ok || len(series) == 0 {
		return nil, false
	}
	meta, ok := snapshot.Meta[timeframe]
	if !ok || meta.LastIndex < 0 || meta.LastIndex >= len(series) {
		return nil, false
	}
	return series[:meta.LastIndex+1], true
}

func hasEnoughBars(series []models.OHLCV, cfg Config) bool {
	minCount := maxInt(cfg.EntryPeriod+1, cfg.ExitPeriod+1, cfg.ATRPeriod+1)
	return len(series) >= minCount
}

func clearedSignal(timeframe string) models.Signal {
	return models.Signal{
		Timeframe: timeframe,
		Action:    0,
		HighSide:  trendSideNone,
		MidSide:   midSideNone,
	}
}

func sameSignalCore(left, right models.Signal) bool {
	return left.Timeframe == right.Timeframe &&
		left.OrderType == right.OrderType &&
		floatcmp.EQ(left.Amount, right.Amount) &&
		floatcmp.EQ(left.Entry, right.Entry) &&
		floatcmp.EQ(left.Exit, right.Exit) &&
		floatcmp.EQ(left.SL, right.SL) &&
		floatcmp.EQ(left.TP, right.TP) &&
		left.Action == right.Action &&
		left.HighSide == right.HighSide &&
		left.MidSide == right.MidSide &&
		left.TrendingTimestamp == right.TrendingTimestamp &&
		left.TriggerTimestamp == right.TriggerTimestamp
}

func maxInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	max := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] > max {
			max = values[i]
		}
	}
	return max
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
