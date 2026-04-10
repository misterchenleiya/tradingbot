package models

import (
	"reflect"
	"strings"
	"sync"

	"github.com/misterchenleiya/tradingbot/common/floatcmp"
)

var (
	signalEmptyCheckOnce    sync.Once
	signalEmptyFieldIndexes []int
)

func IsEmptySignal(signal Signal) bool {
	signalEmptyCheckOnce.Do(initSignalEmptyFieldIndexes)
	value := reflect.ValueOf(signal)
	for _, idx := range signalEmptyFieldIndexes {
		if !isSignalFieldEmpty(value.Field(idx)) {
			return false
		}
	}
	return true
}

func ClearSignalForRemoval(signal Signal) Signal {
	signalEmptyCheckOnce.Do(initSignalEmptyFieldIndexes)
	value := reflect.ValueOf(&signal).Elem()
	for _, idx := range signalEmptyFieldIndexes {
		field := value.Field(idx)
		field.Set(reflect.Zero(field.Type()))
	}
	return signal
}

func initSignalEmptyFieldIndexes() {
	t := reflect.TypeOf(Signal{})
	signalEmptyFieldIndexes = make([]int, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		name := t.Field(i).Name
		if signalFieldIgnoredForEmptyCheck(name) {
			continue
		}
		signalEmptyFieldIndexes = append(signalEmptyFieldIndexes, i)
	}
}

func signalFieldIgnoredForEmptyCheck(name string) bool {
	switch name {
	case "Exchange", "Symbol", "Timeframe", "ComboKey", "GroupID", "Strategy", "StrategyVersion", "StrategyTimeframes", "StrategyIndicators", "OHLCV":
		return true
	default:
		return false
	}
}

func isSignalFieldEmpty(field reflect.Value) bool {
	switch field.Kind() {
	case reflect.String:
		return strings.TrimSpace(field.String()) == ""
	case reflect.Float32, reflect.Float64:
		return floatcmp.EQ(field.Float(), 0)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return field.Uint() == 0
	case reflect.Bool:
		return !field.Bool()
	case reflect.Array, reflect.Slice, reflect.Map:
		return field.Len() == 0
	case reflect.Interface, reflect.Pointer:
		return field.IsNil()
	default:
		return field.IsZero()
	}
}
