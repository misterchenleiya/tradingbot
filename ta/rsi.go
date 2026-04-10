package ta

import (
	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const rsiPeriod = 14

type RSIResult struct {
	Period int
	Values []float64
}

func RSI(ohlcv []models.OHLCV) (RSIResult, error) {
	minCount := rsiPeriod + 1
	if err := validateOHLCV(ohlcv, minCount); err != nil {
		return RSIResult{}, err
	}
	close := closeSeries(ohlcv)
	values := talib.Rsi(close, rsiPeriod)
	return RSIResult{Period: rsiPeriod, Values: values}, nil
}
