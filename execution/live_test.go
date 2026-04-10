package execution

import (
	"context"
	"errors"
	"testing"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestEnsureClientOrderIDNormalizeInvalidCurrent(t *testing.T) {
	id := ensureClientOrderID("risk_okx_btc_usdt_1770992845815", "okx", "BTC/USDT", "open")
	if !isValidClientOrderID(id) {
		t.Fatalf("invalid client order id: %s", id)
	}
}

func TestEnsureClientOrderIDGenerateValid(t *testing.T) {
	id := ensureClientOrderID("", "okx", "BTC/USDT", "open")
	if !isValidClientOrderID(id) {
		t.Fatalf("invalid client order id: %s", id)
	}
}

func TestNormalizeOrderSizeFloorToLot(t *testing.T) {
	size, err := normalizeOrderSize(0.000151, iface.Instrument{LotSz: 0.0001, MinSz: 0.0001}, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if size != 0.0001 {
		t.Fatalf("unexpected size: got %.8f want %.8f", size, 0.0001)
	}
}

func TestNormalizeOrderSizeRejectBelowMin(t *testing.T) {
	_, err := normalizeOrderSize(0.00015, iface.Instrument{LotSz: 0.0001, MinSz: 0.0002}, true)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestValidateTPSLAgainstPriceForExecutionRejectsInvalidLongSL(t *testing.T) {
	err := validateTPSLAgainstPriceForExecution("long", 70000, 81000, 686490)
	if err == nil {
		t.Fatalf("expected validation error")
	}
}

func TestFormatTPSLTriggerZeroReturnsEmpty(t *testing.T) {
	if got := formatTPSLTrigger(0); got != "" {
		t.Fatalf("expected empty trigger for zero price, got %q", got)
	}
}

func TestFormatTPSLTriggerPositiveReturnsNumber(t *testing.T) {
	if got := formatTPSLTrigger(81000); got == "" {
		t.Fatalf("expected non-empty trigger for positive price")
	}
}

func TestNormalizeExecutionPosMode(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "net mode", raw: "net_mode", want: executionPosModeNet},
		{name: "net alias", raw: "net", want: executionPosModeNet},
		{name: "long short mode", raw: "long_short_mode", want: executionPosModeLongShort},
		{name: "long short alias", raw: "long_short", want: executionPosModeLongShort},
		{name: "invalid", raw: "x", want: ""},
	}
	for _, tc := range tests {
		if got := normalizeExecutionPosMode(tc.raw); got != tc.want {
			t.Fatalf("%s: got %q want %q", tc.name, got, tc.want)
		}
	}
}

func TestPosSideByMode(t *testing.T) {
	if got := posSideByMode(executionPosModeNet, "long"); got != "" {
		t.Fatalf("net mode should not send posSide, got %q", got)
	}
	if got := posSideByMode(executionPosModeLongShort, "long"); got != "long" {
		t.Fatalf("long_short mode should keep posSide, got %q", got)
	}
}

func TestResolvePositionModeSwitchesWhenDesiredDiffers(t *testing.T) {
	ex := &posModeStubExchange{accountMode: executionPosModeNet}
	live := &Live{
		posMode: map[string]string{
			"okx": executionPosModeLongShort,
		},
	}
	mode, err := live.resolvePositionMode(context.Background(), ex, "okx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != executionPosModeLongShort {
		t.Fatalf("unexpected mode: got %q want %q", mode, executionPosModeLongShort)
	}
	if !ex.setCalled || ex.setMode != executionPosModeLongShort {
		t.Fatalf("expected set position mode to be called with %q", executionPosModeLongShort)
	}
}

func TestResolvePositionModeUsesAccountWhenDesiredEmpty(t *testing.T) {
	ex := &posModeStubExchange{accountMode: executionPosModeNet}
	live := &Live{
		posMode: map[string]string{},
	}
	mode, err := live.resolvePositionMode(context.Background(), ex, "okx")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mode != executionPosModeNet {
		t.Fatalf("unexpected mode: got %q want %q", mode, executionPosModeNet)
	}
	if ex.setCalled {
		t.Fatalf("did not expect set position mode")
	}
}

func TestResolvePositionModeUnknownAccountModeReturnsError(t *testing.T) {
	ex := &posModeStubExchange{accountMode: "invalid"}
	live := &Live{
		posMode: map[string]string{},
	}
	_, err := live.resolvePositionMode(context.Background(), ex, "okx")
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestBuildAttachAlgoOrdersOmitsUnsetSide(t *testing.T) {
	orders := buildAttachAlgoOrders(models.Decision{StopLossPrice: 1.1568})
	if len(orders) != 1 {
		t.Fatalf("unexpected attach algo count: got %d want 1", len(orders))
	}
	if orders[0].SLTriggerPx == "" || orders[0].SLOrdPx != "-1" {
		t.Fatalf("expected sl attach algo populated, got %+v", orders[0])
	}
	if orders[0].TPTriggerPx != "" || orders[0].TPOrdPx != "" {
		t.Fatalf("expected tp attach algo omitted, got %+v", orders[0])
	}
}

func TestPlaceOpenPrefersAttachAlgoOrders(t *testing.T) {
	ex := &tpslStubExchange{supportAttach: true}
	live := &Live{defaultMarginMode: models.MarginModeIsolated}

	_, _, _, err := live.placeOpen(context.Background(), ex, "AXS-USDT-SWAP", iface.Instrument{LotSz: 1, MinSz: 1}, models.Decision{
		Action:             models.DecisionActionOpenLong,
		Exchange:           "okx",
		Symbol:             "AXS/USDT",
		Size:               5,
		MarginMode:         models.MarginModeIsolated,
		LeverageMultiplier: 6,
		StopLossPrice:      1.1568,
		TakeProfitPrice:    1.2050,
		ClientOrderID:      "riskokxaxs",
	}, executionPosModeLongShort)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ex.placeOrderReq.AttachAlgoOrds) != 1 {
		t.Fatalf("expected attachAlgoOrds on open request")
	}
	if ex.placeTPSLCalled {
		t.Fatalf("did not expect standalone TPSL placement when attachAlgoOrds is supported")
	}
}

func TestPlaceTPSLUsesCurrentPositionTotalSizeAndSideScopedCancel(t *testing.T) {
	ex := &tpslStubExchange{
		positions: []iface.Position{
			{InstID: "AXS-USDT-SWAP", PosSide: "long", Pos: "5"},
			{InstID: "AXS-USDT-SWAP", PosSide: "long", Pos: "4"},
			{InstID: "AXS-USDT-SWAP", PosSide: "short", Pos: "2"},
		},
		openOrders: []iface.TPSLOrder{
			{OrderID: "algo-long", InstID: "AXS-USDT-SWAP", PosSide: "long", Side: "sell"},
			{OrderID: "algo-short", InstID: "AXS-USDT-SWAP", PosSide: "short", Side: "buy"},
		},
	}
	live := &Live{defaultMarginMode: models.MarginModeIsolated}

	_, _, err := live.placeTPSL(context.Background(), ex, "AXS-USDT-SWAP", iface.Instrument{LotSz: 1, MinSz: 1}, models.Decision{
		Action:          models.DecisionActionUpdate,
		Exchange:        "okx",
		Symbol:          "AXS/USDT",
		PositionSide:    "long",
		MarginMode:      models.MarginModeIsolated,
		StopLossPrice:   1.1568,
		TakeProfitPrice: 1.2050,
		ClientOrderID:   "riskokxaxs",
	}, executionPosModeLongShort)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(ex.cancelReqs) != 1 || ex.cancelReqs[0].OrderID != "algo-long" {
		t.Fatalf("expected only long-side TPSL cancel, got %+v", ex.cancelReqs)
	}
	if ex.placeTPSLReq.Sz != "9" {
		t.Fatalf("expected total current long size 9, got %q", ex.placeTPSLReq.Sz)
	}
}

func TestPlaceTPSLDoesNotCancelWhenCurrentSizeUnavailable(t *testing.T) {
	ex := &tpslStubExchange{
		openOrders: []iface.TPSLOrder{
			{OrderID: "algo-long", InstID: "AXS-USDT-SWAP", PosSide: "long", Side: "sell"},
		},
	}
	live := &Live{defaultMarginMode: models.MarginModeIsolated}

	_, _, err := live.placeTPSL(context.Background(), ex, "AXS-USDT-SWAP", iface.Instrument{LotSz: 1, MinSz: 1}, models.Decision{
		Action:          models.DecisionActionUpdate,
		Exchange:        "okx",
		Symbol:          "AXS/USDT",
		PositionSide:    "long",
		MarginMode:      models.MarginModeIsolated,
		StopLossPrice:   1.1568,
		TakeProfitPrice: 1.2050,
		ClientOrderID:   "riskokxaxs",
	}, executionPosModeLongShort)
	if err == nil {
		t.Fatalf("expected missing current position size error")
	}
	if len(ex.cancelReqs) != 0 {
		t.Fatalf("did not expect cancel before current size is known")
	}
}

type posModeStubExchange struct {
	accountMode string
	getErr      error
	setErr      error
	setCalled   bool
	setMode     string
}

func (s *posModeStubExchange) Name() string { return "okx" }

func (s *posModeStubExchange) NormalizeSymbol(raw string) (string, error) { return raw, nil }

func (s *posModeStubExchange) GetInstrument(ctx context.Context, instID string) (iface.Instrument, error) {
	return iface.Instrument{}, nil
}

func (s *posModeStubExchange) GetTickerPrice(ctx context.Context, instID string) (float64, error) {
	return 0, nil
}

func (s *posModeStubExchange) GetPositions(ctx context.Context, instID string) ([]iface.Position, error) {
	return nil, nil
}

func (s *posModeStubExchange) GetPositionsHistory(ctx context.Context, instID string) ([]iface.PositionHistory, error) {
	return nil, nil
}

func (s *posModeStubExchange) GetBalance(ctx context.Context) (iface.BalanceSnapshot, error) {
	return iface.BalanceSnapshot{}, nil
}

func (s *posModeStubExchange) SetPositionMode(ctx context.Context, mode string) error {
	s.setCalled = true
	s.setMode = mode
	if s.setErr != nil {
		return s.setErr
	}
	s.accountMode = mode
	return nil
}

func (s *posModeStubExchange) SetLeverage(ctx context.Context, instID, marginMode string, leverage int, posSide string) error {
	return nil
}

func (s *posModeStubExchange) PlaceOrder(ctx context.Context, req iface.OrderRequest) (string, error) {
	return "", nil
}

func (s *posModeStubExchange) GetOrder(ctx context.Context, instID, ordID string) (iface.Order, error) {
	return iface.Order{}, nil
}

func (s *posModeStubExchange) GetPositionMode(ctx context.Context) (string, error) {
	if s.getErr != nil {
		return "", s.getErr
	}
	if s.accountMode == "" {
		return "", errors.New("empty mode")
	}
	return s.accountMode, nil
}

type tpslStubExchange struct {
	positions       []iface.Position
	openOrders      []iface.TPSLOrder
	placeOrderReq   iface.OrderRequest
	placeTPSLReq    iface.TPSLOrderRequest
	cancelReqs      []iface.CancelTPSLOrderRequest
	supportAttach   bool
	placeTPSLCalled bool
}

func (s *tpslStubExchange) Name() string { return "okx" }

func (s *tpslStubExchange) NormalizeSymbol(raw string) (string, error) { return raw, nil }

func (s *tpslStubExchange) GetInstrument(ctx context.Context, instID string) (iface.Instrument, error) {
	return iface.Instrument{}, nil
}

func (s *tpslStubExchange) GetTickerPrice(ctx context.Context, instID string) (float64, error) {
	return 1.18, nil
}

func (s *tpslStubExchange) GetPositions(ctx context.Context, instID string) ([]iface.Position, error) {
	return append([]iface.Position(nil), s.positions...), nil
}

func (s *tpslStubExchange) GetPositionsHistory(ctx context.Context, instID string) ([]iface.PositionHistory, error) {
	return nil, nil
}

func (s *tpslStubExchange) GetBalance(ctx context.Context) (iface.BalanceSnapshot, error) {
	return iface.BalanceSnapshot{}, nil
}

func (s *tpslStubExchange) SetPositionMode(ctx context.Context, mode string) error { return nil }

func (s *tpslStubExchange) SetLeverage(ctx context.Context, instID, marginMode string, leverage int, posSide string) error {
	return nil
}

func (s *tpslStubExchange) PlaceOrder(ctx context.Context, req iface.OrderRequest) (string, error) {
	s.placeOrderReq = req
	return "order-1", nil
}

func (s *tpslStubExchange) GetOrder(ctx context.Context, instID, ordID string) (iface.Order, error) {
	return iface.Order{}, nil
}

func (s *tpslStubExchange) GetOpenTPSLOrders(ctx context.Context, instID string) ([]iface.TPSLOrder, error) {
	return append([]iface.TPSLOrder(nil), s.openOrders...), nil
}

func (s *tpslStubExchange) CancelTPSLOrders(ctx context.Context, reqs []iface.CancelTPSLOrderRequest) error {
	s.cancelReqs = append([]iface.CancelTPSLOrderRequest(nil), reqs...)
	return nil
}

func (s *tpslStubExchange) PlaceTPSLOrder(ctx context.Context, req iface.TPSLOrderRequest) (string, error) {
	s.placeTPSLCalled = true
	s.placeTPSLReq = req
	return "algo-1", nil
}

func (s *tpslStubExchange) SupportsAttachAlgoOrders() bool {
	return s.supportAttach
}
