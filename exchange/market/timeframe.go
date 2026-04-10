package market

import (
	"fmt"
	"time"
)

func timeframeDuration(timeframe string) (time.Duration, bool) {
	switch timeframe {
	case "1m":
		return time.Minute, true
	case "3m":
		return 3 * time.Minute, true
	case "5m":
		return 5 * time.Minute, true
	case "15m":
		return 15 * time.Minute, true
	case "30m":
		return 30 * time.Minute, true
	case "1h":
		return time.Hour, true
	case "2h":
		return 2 * time.Hour, true
	case "4h":
		return 4 * time.Hour, true
	case "6h":
		return 6 * time.Hour, true
	case "8h":
		return 8 * time.Hour, true
	case "12h":
		return 12 * time.Hour, true
	case "1d":
		return 24 * time.Hour, true
	default:
		return 0, false
	}
}

func TimeframeDuration(timeframe string) (time.Duration, bool) {
	return timeframeDuration(timeframe)
}

func timeframeMinutes(timeframe string) (int, error) {
	d, ok := timeframeDuration(timeframe)
	if !ok {
		return 0, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}
	if d%time.Minute != 0 {
		return 0, fmt.Errorf("timeframe is not minute-aligned: %s", timeframe)
	}
	minutes := int(d / time.Minute)
	if minutes <= 0 {
		return 0, fmt.Errorf("invalid timeframe minutes: %s", timeframe)
	}
	return minutes, nil
}

func normalizeTimestampMS(ts int64) int64 {
	if ts <= 0 {
		return ts
	}
	switch {
	case ts < 1e11:
		return ts * 1000
	case ts > 1e16:
		return ts / 1e6
	case ts > 1e14:
		return ts / 1e3
	default:
		return ts
	}
}
