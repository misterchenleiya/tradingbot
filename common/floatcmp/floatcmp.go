package floatcmp

import "math"

const (
	DefaultAbsTolerance = 1e-9
	DefaultRelTolerance = 1e-9
)

type Config struct {
	AbsTolerance float64
	RelTolerance float64
}

func Tolerance(a, b float64) float64 {
	return ToleranceWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func ToleranceWithConfig(a, b float64, cfg Config) float64 {
	absTol := cfg.AbsTolerance
	relTol := cfg.RelTolerance
	if absTol < 0 {
		absTol = 0
	}
	if relTol < 0 {
		relTol = 0
	}
	scale := math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	return absTol + relTol*scale
}

func GT(a, b float64) bool {
	return GTWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func GTWithConfig(a, b float64, cfg Config) bool {
	return a-b > ToleranceWithConfig(a, b, cfg)
}

func LT(a, b float64) bool {
	return LTWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func LTWithConfig(a, b float64, cfg Config) bool {
	return b-a > ToleranceWithConfig(a, b, cfg)
}

func LE(a, b float64) bool {
	return LEWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func LEWithConfig(a, b float64, cfg Config) bool {
	return !GTWithConfig(a, b, cfg)
}

func GE(a, b float64) bool {
	return GEWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func GEWithConfig(a, b float64, cfg Config) bool {
	return !LTWithConfig(a, b, cfg)
}

func EQ(a, b float64) bool {
	return EQWithConfig(a, b, Config{
		AbsTolerance: DefaultAbsTolerance,
		RelTolerance: DefaultRelTolerance,
	})
}

func EQWithConfig(a, b float64, cfg Config) bool {
	diff := math.Abs(a - b)
	return diff <= ToleranceWithConfig(a, b, cfg)
}
