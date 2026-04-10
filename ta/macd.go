package ta

import (
	"fmt"

	"github.com/markcheno/go-talib"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

const (
	macdFastPeriod   = 12
	macdSlowPeriod   = 26
	macdSignalPeriod = 9
)

type MACDResult struct {
	FastPeriod   int
	SlowPeriod   int
	SignalPeriod int
	MACD         []float64
	Signal       []float64
	Hist         []float64
}

func MACD(ohlcv []models.OHLCV) (MACDResult, error) {
	return MACDWithPeriods(ohlcv, macdFastPeriod, macdSlowPeriod, macdSignalPeriod)
}

func MACDWithPeriods(ohlcv []models.OHLCV, fast, slow, signal int) (MACDResult, error) {
	if fast <= 0 || slow <= 0 || signal <= 0 {
		return MACDResult{}, fmt.Errorf("invalid macd periods: fast=%d slow=%d signal=%d", fast, slow, signal)
	}
	minCount := slow + signal - 1
	if err := validateOHLCV(ohlcv, minCount); err != nil {
		return MACDResult{}, err
	}
	close := closeSeries(ohlcv)
	outMACD, outSignal, outHist := talib.Macd(close, fast, slow, signal)
	return MACDResult{
		FastPeriod:   fast,
		SlowPeriod:   slow,
		SignalPeriod: signal,
		MACD:         outMACD,
		Signal:       outSignal,
		Hist:         outHist,
	}, nil
}
