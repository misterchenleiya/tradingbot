package core

import "time"

func expectedLatestOHLCVStart(now time.Time, dur time.Duration, fetchUnclosed bool) int64 {
	if dur <= 0 {
		return 0
	}
	currentStart := now.Truncate(dur).UnixMilli()
	if fetchUnclosed {
		return currentStart
	}
	step := dur.Milliseconds()
	if step <= 0 {
		return currentStart
	}
	expected := currentStart - step
	if expected < 0 {
		return 0
	}
	return expected
}
