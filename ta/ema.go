package ta

import (
	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

func EMA(ohlcv []models.OHLCV) (MAResult, error) {
	if err := validateOHLCV(ohlcv, maxMAPeriod); err != nil {
		return MAResult{}, err
	}
	close := closeSeries(ohlcv)
	out := make(map[int][]float64, len(defaultMAPeriods))
	for _, period := range defaultMAPeriods {
		out[period] = talib.Ema(close, period)
	}
	return MAResult{Periods: out}, nil
}
