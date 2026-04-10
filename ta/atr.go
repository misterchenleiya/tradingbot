package ta

import (
	"fmt"

	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const atrDefaultPeriod = 14

type ATRResult struct {
	Period int
	Values []float64
}

func ATR(ohlcv []models.OHLCV, period int) (ATRResult, error) {
	if period <= 0 {
		return ATRResult{}, fmt.Errorf("invalid atr period: %d", period)
	}
	minCount := period + 1
	if err := validateOHLCV(ohlcv, minCount); err != nil {
		return ATRResult{}, err
	}
	high := highSeries(ohlcv)
	low := lowSeries(ohlcv)
	close := closeSeries(ohlcv)
	values := talib.Atr(high, low, close, period)
	return ATRResult{Period: period, Values: values}, nil
}

func ATRDefault(ohlcv []models.OHLCV) (ATRResult, error) {
	return ATR(ohlcv, atrDefaultPeriod)
}
