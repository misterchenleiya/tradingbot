package ta

import (
	"fmt"

	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const (
	stochFastKPeriod = 14
	stochSlowKPeriod = 3
	stochSlowDPeriod = 3
)

type StochasticResult struct {
	FastKPeriod int
	SlowKPeriod int
	SlowDPeriod int
	SlowK       []float64
	SlowD       []float64
}

func Stochastic(ohlcv []models.OHLCV) (StochasticResult, error) {
	return StochasticWithPeriods(ohlcv, stochFastKPeriod, stochSlowKPeriod, stochSlowDPeriod)
}

func StochasticWithPeriods(ohlcv []models.OHLCV, fastK, slowK, slowD int) (StochasticResult, error) {
	if fastK <= 0 || slowK <= 0 || slowD <= 0 {
		return StochasticResult{}, fmt.Errorf("invalid stochastic periods: fastK=%d slowK=%d slowD=%d", fastK, slowK, slowD)
	}
	minCount := (fastK - 1) + (slowK - 1) + (slowD - 1) + 1
	if err := validateOHLCV(ohlcv, minCount); err != nil {
		return StochasticResult{}, err
	}
	high, low, close := highLowCloseSeries(ohlcv)
	outK, outD := talib.Stoch(high, low, close, fastK, slowK, talib.SMA, slowD, talib.SMA)
	return StochasticResult{
		FastKPeriod: fastK,
		SlowKPeriod: slowK,
		SlowDPeriod: slowD,
		SlowK:       outK,
		SlowD:       outD,
	}, nil
}
