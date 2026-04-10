package core

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func NormalizeLeverage(val float64) int {
	if val <= 0 {
		return 0
	}
	return int(math.Floor(val))
}

func ParseFloat(raw string) (float64, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return 0, fmt.Errorf("empty number")
	}
	return strconv.ParseFloat(text, 64)
}

func FormatFloat(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

func FloorToStep(value, step float64) float64 {
	if step <= 0 {
		return value
	}
	return math.Floor(value/step+1e-12) * step
}

func CombineErrors(primary, secondary error) error {
	if secondary == nil {
		return primary
	}
	if primary == nil {
		return secondary
	}
	return fmt.Errorf("%v; close error: %w", primary, secondary)
}
