package ta

import (
	"fmt"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func validateOHLCV(ohlcv []models.OHLCV, minCount int) error {
	if minCount <= 0 {
		return fmt.Errorf("invalid minCount: %d", minCount)
	}
	if len(ohlcv) < minCount {
		return fmt.Errorf("ohlcv length %d is less than required %d", len(ohlcv), minCount)
	}
	if len(ohlcv) < 2 {
		return nil
	}
	step := ohlcv[1].TS - ohlcv[0].TS
	if step <= 0 {
		return fmt.Errorf("ohlcv timestamps not strictly increasing")
	}
	for i := 1; i < len(ohlcv); i++ {
		delta := ohlcv[i].TS - ohlcv[i-1].TS
		if delta != step {
			return fmt.Errorf("ohlcv not continuous at index %d: delta %d != %d", i, delta, step)
		}
	}
	return nil
}

func closeSeries(ohlcv []models.OHLCV) []float64 {
	out := make([]float64, len(ohlcv))
	for i, bar := range ohlcv {
		out[i] = bar.Close
	}
	return out
}

func highSeries(ohlcv []models.OHLCV) []float64 {
	out := make([]float64, len(ohlcv))
	for i, bar := range ohlcv {
		out[i] = bar.High
	}
	return out
}

func lowSeries(ohlcv []models.OHLCV) []float64 {
	out := make([]float64, len(ohlcv))
	for i, bar := range ohlcv {
		out[i] = bar.Low
	}
	return out
}

func highLowCloseSeries(ohlcv []models.OHLCV) ([]float64, []float64, []float64) {
	high := make([]float64, len(ohlcv))
	low := make([]float64, len(ohlcv))
	close := make([]float64, len(ohlcv))
	for i, bar := range ohlcv {
		high[i] = bar.High
		low[i] = bar.Low
		close[i] = bar.Close
	}
	return high, low, close
}
