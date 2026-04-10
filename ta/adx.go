package ta

import (
	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const adxPeriod = 14

type ADXResult struct {
	Period int
	Values []float64
}

func ADX(ohlcv []models.OHLCV) (ADXResult, error) {
	minCount := adxPeriod * 2
	if err := validateOHLCV(ohlcv, minCount); err != nil {
		return ADXResult{}, err
	}
	high, low, close := highLowCloseSeries(ohlcv)
	values := talib.Adx(high, low, close, adxPeriod)
	return ADXResult{Period: adxPeriod, Values: values}, nil
}
