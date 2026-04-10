package market

import (
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestMultiWS_AllowsUnclosedByDefault(t *testing.T) {
	ws := NewMultiWS(nil)
	data := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    false,
	}

	ws.emit(data)

	select {
	case got := <-ws.Events():
		if got.Exchange != data.Exchange || got.Symbol != data.Symbol || got.Timeframe != data.Timeframe || got.Closed != data.Closed {
			t.Fatalf("unexpected event forwarded: %+v", got)
		}
	default:
		t.Fatalf("expected unclosed event to be forwarded by default")
	}
}

func TestMultiWS_DropsOnlyUnclosedWhenDisabled(t *testing.T) {
	ws := NewMultiWS(nil)
	ws.SetAllowUnclosedOHLCV(false)

	unclosed := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    false,
	}
	closed := models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    true,
	}

	ws.emit(unclosed)

	select {
	case got := <-ws.Events():
		t.Fatalf("expected unclosed event to be dropped, got %+v", got)
	default:
	}

	ws.emit(closed)

	select {
	case got := <-ws.Events():
		if !got.Closed {
			t.Fatalf("expected closed event to pass through, got %+v", got)
		}
	default:
		t.Fatalf("expected closed event to be forwarded")
	}
}
