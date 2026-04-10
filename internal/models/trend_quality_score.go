package models

import "strings"

const (
	TrendQualitySideLong  = "long"
	TrendQualitySideShort = "short"
)

type TrendQualityScoreRequest struct {
	Side           string
	ScoreTimeframe string
	Series         []OHLCV
	StartTS        int64
	BoundaryTS     int64
}

type TrendQualityScoreBreakdown struct {
	FinalScore            float64 `json:"final_score"`
	ScoreTimeframe        string  `json:"score_timeframe,omitempty"`
	CloseHoldEMA5Ratio    float64 `json:"close_hold_ema5_ratio"`
	CloseHoldEMA20Ratio   float64 `json:"close_hold_ema20_ratio"`
	WickBreakEMA5Penalty  float64 `json:"wick_break_ema5_penalty"`
	WickTouchEMA20Penalty float64 `json:"wick_touch_ema20_penalty"`
	TrendEfficiency       float64 `json:"trend_efficiency"`
	BreakoutConsistency   float64 `json:"breakout_consistency"`
	ExtensionPenalty      float64 `json:"extension_penalty"`
	BarsCount             int     `json:"bars_count"`
	WindowStartTS         int64   `json:"window_start_ts,omitempty"`
	WindowBoundaryTS      int64   `json:"window_boundary_ts,omitempty"`
	WindowEndTS           int64   `json:"window_end_ts,omitempty"`
	SnapshotMissing       bool    `json:"snapshot_missing,omitempty"`
}

func ScoreTrendQuality(req TrendQualityScoreRequest) (float64, TrendQualityScoreBreakdown) {
	breakdown := TrendQualityScoreBreakdown{
		ScoreTimeframe:  strings.TrimSpace(req.ScoreTimeframe),
		SnapshotMissing: true,
	}
	side := normalizeTrendQualitySide(req.Side)
	if side == "" {
		return 0, breakdown
	}
	bars := normalizeTrendQualityBars(req.Series)
	if len(bars) == 0 {
		return 0, breakdown
	}
	startTS := normalizeTrendQualityTimestamp(req.StartTS)
	if startTS <= 0 {
		return 0, breakdown
	}
	startIdx := -1
	for idx, bar := range bars {
		if normalizeTrendQualityTimestamp(bar.TS) >= startTS {
			startIdx = idx
			break
		}
	}
	if startIdx < 0 || startIdx >= len(bars) {
		return 0, breakdown
	}
	boundaryTS := normalizeTrendQualityTimestamp(req.BoundaryTS)
	if boundaryTS <= 0 || boundaryTS < startTS {
		boundaryTS = normalizeTrendQualityTimestamp(bars[len(bars)-1].TS)
	}
	breakdown.WindowStartTS = normalizeTrendQualityTimestamp(bars[startIdx].TS)
	breakdown.WindowBoundaryTS = boundaryTS

	endBoundaryIdx := -1
	for idx := startIdx; idx < len(bars); idx++ {
		if normalizeTrendQualityTimestamp(bars[idx].TS) <= boundaryTS {
			endBoundaryIdx = idx
		} else {
			break
		}
	}
	if endBoundaryIdx < startIdx {
		return 0, breakdown
	}

	extremeIdx := startIdx
	for idx := startIdx + 1; idx <= endBoundaryIdx; idx++ {
		switch side {
		case TrendQualitySideLong:
			if bars[idx].High >= bars[extremeIdx].High {
				extremeIdx = idx
			}
		case TrendQualitySideShort:
			if bars[idx].Low <= bars[extremeIdx].Low {
				extremeIdx = idx
			}
		}
	}

	bars = append([]OHLCV(nil), bars[startIdx:extremeIdx+1]...)
	breakdown.WindowEndTS = normalizeTrendQualityTimestamp(bars[len(bars)-1].TS)
	breakdown.BarsCount = len(bars)
	if len(bars) < 3 {
		return 0, breakdown
	}

	breakdown.SnapshotMissing = false
	closes := make([]float64, len(bars))
	for i, bar := range bars {
		closes[i] = bar.Close
	}
	ema5 := trendQualityEMASeries(closes, 5)
	ema20 := trendQualityEMASeries(closes, 20)

	var closeHold5 float64
	var closeHold20 float64
	var wickBreak5 float64
	var wickTouch20 float64
	var directionConsistency float64
	var path float64

	for i, bar := range bars {
		closePrice := bar.Close
		if closePrice <= 0 {
			continue
		}
		if trendQualityCloseHolds(side, closePrice, ema5[i]) {
			closeHold5++
		}
		if trendQualityCloseHolds(side, closePrice, ema20[i]) {
			closeHold20++
		}
		if trendQualityWickBreaks(side, bar, ema5[i]) {
			wickBreak5++
		}
		if trendQualityWickBreaks(side, bar, ema20[i]) {
			wickTouch20++
		}
		if i > 0 {
			move := closePrice - bars[i-1].Close
			path += absFloat64(move)
			if trendQualityDirectionalMove(side, move) {
				directionConsistency++
			}
		}
	}

	n := float64(len(bars))
	breakdown.CloseHoldEMA5Ratio = clampTrendQualityScore(closeHold5 / n)
	breakdown.CloseHoldEMA20Ratio = clampTrendQualityScore(closeHold20 / n)
	breakdown.WickBreakEMA5Penalty = clampTrendQualityScore(wickBreak5 / n)
	breakdown.WickTouchEMA20Penalty = clampTrendQualityScore(wickTouch20 / n)
	if len(bars) > 1 && path > 0 {
		netMove := absFloat64(bars[len(bars)-1].Close - bars[0].Close)
		breakdown.TrendEfficiency = clampTrendQualityScore(netMove / path)
		breakdown.BreakoutConsistency = clampTrendQualityScore(directionConsistency / float64(len(bars)-1))
	}
	lastClose := bars[len(bars)-1].Close
	lastEMA20 := ema20[len(ema20)-1]
	if lastClose > 0 && lastEMA20 > 0 {
		extension := absFloat64(lastClose-lastEMA20) / lastClose
		breakdown.ExtensionPenalty = clampTrendQualityScore(extension)
	}

	finalScore := 100 * (0.24*breakdown.CloseHoldEMA5Ratio +
		0.18*breakdown.CloseHoldEMA20Ratio +
		0.20*breakdown.TrendEfficiency +
		0.18*breakdown.BreakoutConsistency -
		0.10*breakdown.WickBreakEMA5Penalty -
		0.06*breakdown.WickTouchEMA20Penalty -
		0.04*breakdown.ExtensionPenalty)
	if finalScore < 0 {
		finalScore = 0
	}
	if finalScore > 100 {
		finalScore = 100
	}
	breakdown.FinalScore = finalScore
	return finalScore, breakdown
}

func normalizeTrendQualityBars(series []OHLCV) []OHLCV {
	bars := make([]OHLCV, 0, len(series))
	for _, bar := range series {
		if normalizeTrendQualityTimestamp(bar.TS) <= 0 {
			continue
		}
		bars = append(bars, bar)
	}
	return bars
}

func normalizeTrendQualityTimestamp(ts int64) int64 {
	if ts <= 0 {
		return 0
	}
	return ts
}

func normalizeTrendQualitySide(side string) string {
	switch strings.ToLower(strings.TrimSpace(side)) {
	case TrendQualitySideLong:
		return TrendQualitySideLong
	case TrendQualitySideShort:
		return TrendQualitySideShort
	default:
		return ""
	}
}

func trendQualityEMASeries(values []float64, period int) []float64 {
	out := make([]float64, len(values))
	if len(values) == 0 {
		return out
	}
	if period <= 1 {
		copy(out, values)
		return out
	}
	multiplier := 2.0 / float64(period+1)
	out[0] = values[0]
	for i := 1; i < len(values); i++ {
		out[i] = ((values[i] - out[i-1]) * multiplier) + out[i-1]
	}
	return out
}

func trendQualityCloseHolds(side string, closePrice, ema float64) bool {
	if closePrice <= 0 || ema <= 0 {
		return false
	}
	switch side {
	case TrendQualitySideLong:
		return closePrice >= ema
	case TrendQualitySideShort:
		return closePrice <= ema
	default:
		return false
	}
}

func trendQualityWickBreaks(side string, bar OHLCV, ema float64) bool {
	if ema <= 0 {
		return false
	}
	switch side {
	case TrendQualitySideLong:
		return bar.Low < ema
	case TrendQualitySideShort:
		return bar.High > ema
	default:
		return false
	}
}

func trendQualityDirectionalMove(side string, move float64) bool {
	switch side {
	case TrendQualitySideLong:
		return move >= 0
	case TrendQualitySideShort:
		return move <= 0
	default:
		return false
	}
}

func clampTrendQualityScore(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func absFloat64(value float64) float64 {
	if value < 0 {
		return -value
	}
	return value
}
