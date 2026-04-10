package ta

import (
	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const (
	bollingerPeriod = 20
	bollingerUpDev  = 2.0
	bollingerDnDev  = 2.0
)

type BollingerBandsResult struct {
	Period int
	UpDev  float64
	DnDev  float64
	Upper  []float64
	Middle []float64
	Lower  []float64
}

func BollingerBands(ohlcv []models.OHLCV) (BollingerBandsResult, error) {
	if err := validateOHLCV(ohlcv, bollingerPeriod); err != nil {
		return BollingerBandsResult{}, err
	}
	close := closeSeries(ohlcv)
	upper, middle, lower := talib.BBands(close, bollingerPeriod, bollingerUpDev, bollingerDnDev, talib.SMA)
	return BollingerBandsResult{
		Period: bollingerPeriod,
		UpDev:  bollingerUpDev,
		DnDev:  bollingerDnDev,
		Upper:  upper,
		Middle: middle,
		Lower:  lower,
	}, nil
}
