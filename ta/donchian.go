package ta

import (
	"fmt"

	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type DonchianResult struct {
	Period int
	Upper  []float64
	Middle []float64
	Lower  []float64
}

func Donchian(ohlcv []models.OHLCV, period int) (DonchianResult, error) {
	if period <= 1 {
		return DonchianResult{}, fmt.Errorf("invalid donchian period: %d", period)
	}
	if err := validateOHLCV(ohlcv, period); err != nil {
		return DonchianResult{}, err
	}
	high := highSeries(ohlcv)
	low := lowSeries(ohlcv)
	upper := talib.Max(high, period)
	lower := talib.Min(low, period)
	middle := make([]float64, len(upper))
	for i := range upper {
		middle[i] = (upper[i] + lower[i]) / 2
	}
	return DonchianResult{
		Period: period,
		Upper:  upper,
		Middle: middle,
		Lower:  lower,
	}, nil
}
