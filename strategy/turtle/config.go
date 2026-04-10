package turtle

const (
	defaultEntryPeriod           = 20
	defaultExitPeriod            = 10
	defaultATRPeriod             = 20
	defaultStopLossATRMultiplier = 2.0
	defaultTrailingATRMultiplier = 2.0
	defaultStopLossFallbackRate  = 0.02
	turtleStrategyVersion        = "v0.0.1"
	trendSideNone                = 0
	trendSideLong                = 1
	midSideNone                  = 0
	midSideLong                  = 1
	actionOpen                   = 8
	actionMove                   = 16
	actionCloseAll               = 64
)

type Config struct {
	// EntryPeriod controls the Donchian upper-channel lookback used for long entries.
	// Valid range: integer > 1. When <=1, runtime falls back to default 20.
	// Effect:
	// - Larger: fewer entries, stronger breakout filter, more lag.
	// - Smaller: more entries, higher sensitivity, more false breakouts.
	EntryPeriod int
	// ExitPeriod controls the Donchian lower-channel lookback used for full close signals.
	// Valid range: integer > 1. When <=1, runtime falls back to default 10.
	// Effect:
	// - Larger: later exits, wider tolerance, higher profit giveback risk.
	// - Smaller: earlier exits, faster de-risking, easier churn/stopout.
	ExitPeriod int
	// ATRPeriod controls ATR lookback for both initial SL and trailing SL calculations.
	// Valid range: integer > 1. When <=1, runtime falls back to default 20.
	// Effect:
	// - Larger: smoother ATR, less reactive stop movement.
	// - Smaller: more reactive ATR, faster stop movement.
	ATRPeriod int
	// StopLossATRMultiplier sets initial stop-loss distance: entry - ATR * multiplier.
	// Valid range: float > 0. When <=0, runtime falls back to default 2.0.
	// Effect:
	// - Larger: wider initial SL, fewer early stopouts, larger single-loss exposure.
	// - Smaller: tighter initial SL, lower loss per trade, easier to be stopped.
	StopLossATRMultiplier float64
	// TrailingATRMultiplier sets trailing stop-loss distance on updates: close - ATR * multiplier.
	// Valid range: float > 0. When <=0, runtime falls back to default 2.0.
	// Effect:
	// - Larger: looser trailing, better trend hold, larger pullback tolerance.
	// - Smaller: tighter trailing, faster lock-in, more stop-loss exits.
	TrailingATRMultiplier float64
	// StopLossFallbackRate is used only when ATR-based initial SL is invalid.
	// Fallback SL formula (long): entry * (1 - rate).
	// Valid range: 0 < rate < 1. Outside range falls back to default 0.02.
	// Effect:
	// - Larger: wider fallback SL, fewer invalid-SL rejects, higher fallback risk.
	// - Smaller: tighter fallback SL, lower fallback risk, may still be invalid.
	StopLossFallbackRate float64
}

// normalizeConfig applies defaults when incoming turtle parameters are out of range.
func normalizeConfig(cfg Config) Config {
	if cfg.EntryPeriod <= 1 {
		cfg.EntryPeriod = defaultEntryPeriod
	}
	if cfg.ExitPeriod <= 1 {
		cfg.ExitPeriod = defaultExitPeriod
	}
	if cfg.ATRPeriod <= 1 {
		cfg.ATRPeriod = defaultATRPeriod
	}
	if cfg.StopLossATRMultiplier <= 0 {
		cfg.StopLossATRMultiplier = defaultStopLossATRMultiplier
	}
	if cfg.TrailingATRMultiplier <= 0 {
		cfg.TrailingATRMultiplier = defaultTrailingATRMultiplier
	}
	if cfg.StopLossFallbackRate <= 0 || cfg.StopLossFallbackRate >= 1 {
		cfg.StopLossFallbackRate = defaultStopLossFallbackRate
	}
	return cfg
}
