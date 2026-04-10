package exporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"github.com/misterchenleiya/tradingbot/storage"
)

type stubAccountProvider struct {
	positions []models.Position
	history   []models.Position
	grouped   map[string]models.SignalGroupedInfo
	funds     models.RiskAccountFunds
	err       error
}

func (s stubAccountProvider) GetAccountFunds(exchange string) (models.RiskAccountFunds, error) {
	return s.funds, nil
}

func (s stubAccountProvider) ListAllOpenPositions() ([]models.Position, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.positions, nil
}

func (s stubAccountProvider) ListOpenPositions(exchange, symbol, timeframe string) ([]models.Position, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.positions, nil
}

func (s stubAccountProvider) ListHistoryPositions(exchange, symbol string) ([]models.Position, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.history, nil
}

func (s stubAccountProvider) LookupSignalGrouped(signal models.Signal) (models.SignalGroupedInfo, bool) {
	if s.grouped == nil {
		return models.SignalGroupedInfo{}, false
	}
	info, ok := s.grouped[flatKey(signal)]
	return info, ok
}

type stubSignalProvider struct {
	signals map[string]map[string]models.Signal
}

func (s stubSignalProvider) ListSignals() map[string]map[string]models.Signal {
	return s.signals
}

type stubSymbolProvider struct {
	exchanges []models.Exchange
	symbols   []models.Symbol
}

func (s stubSymbolProvider) ListSymbols() ([]models.Symbol, error) {
	return s.symbols, nil
}

func (s stubSymbolProvider) ListExchanges() ([]models.Exchange, error) {
	return s.exchanges, nil
}

type stubStatusProvider struct {
	status iface.ModuleStatus
	fn     func() iface.ModuleStatus
}

func (s stubStatusProvider) Status() iface.ModuleStatus {
	if s.fn != nil {
		return s.fn()
	}
	return s.status
}

type stubTradingViewRuntimeProvider struct {
	symbols []market.RuntimeSymbolSnapshot
	state   map[string][2]string
	candles map[string]market.RuntimeOHLCVSnapshot
}

func (s stubTradingViewRuntimeProvider) ListRuntimeSymbols(exchange string) []market.RuntimeSymbolSnapshot {
	if strings.TrimSpace(exchange) == "" {
		return append([]market.RuntimeSymbolSnapshot(nil), s.symbols...)
	}
	out := make([]market.RuntimeSymbolSnapshot, 0, len(s.symbols))
	for _, item := range s.symbols {
		if strings.EqualFold(item.Exchange, exchange) {
			out = append(out, item)
		}
	}
	return out
}

func (s stubTradingViewRuntimeProvider) ExchangeRuntimeState(exchange string) (string, string) {
	item, ok := s.state[strings.ToLower(strings.TrimSpace(exchange))]
	if !ok {
		return "", ""
	}
	return item[0], item[1]
}

func (s stubTradingViewRuntimeProvider) LookupRuntimeOHLCV(exchange, symbol, timeframe string) (market.RuntimeOHLCVSnapshot, bool) {
	if s.candles == nil {
		return market.RuntimeOHLCVSnapshot{}, false
	}
	item, ok := s.candles[strings.ToLower(strings.TrimSpace(exchange))+"|"+strings.TrimSpace(symbol)+"|"+strings.TrimSpace(timeframe)]
	if !ok {
		return market.RuntimeOHLCVSnapshot{}, false
	}
	return item, true
}

func (s stubTradingViewRuntimeProvider) ListRuntimeOHLCV(exchange, timeframe string) []market.RuntimeOHLCVSnapshot {
	if s.candles == nil {
		return nil
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	timeframe = strings.TrimSpace(timeframe)
	out := make([]market.RuntimeOHLCVSnapshot, 0, len(s.candles))
	for _, item := range s.candles {
		if exchange != "" && !strings.EqualFold(item.Exchange, exchange) {
			continue
		}
		if timeframe != "" && !strings.EqualFold(item.Timeframe, timeframe) {
			continue
		}
		out = append(out, item)
	}
	return out
}

func testSignal(exchange, symbol string) models.Signal {
	return models.Signal{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          "30m",
		Strategy:           "turtle",
		ComboKey:           "1m/5m/30m",
		StrategyTimeframes: []string{"1m", "5m", "30m"},
		Action:             8,
		HighSide:           -1,
		TrendingTimestamp:  1773943200000,
	}
}

func testGroupedInfo(candidateKey, candidateState, selectedKey string, candidates []models.SignalGroupedCandidate) models.SignalGroupedInfo {
	return models.SignalGroupedInfo{
		GroupID:                   "turtle|30m|short|1773943200000",
		Strategy:                  "turtle",
		PrimaryTimeframe:          "30m",
		Side:                      "short",
		AnchorTrendingTimestampMS: 1773943200000,
		State:                     "soft_locked",
		LockStage:                 "soft",
		SelectedCandidateKey:      selectedKey,
		EntryCount:                0,
		CandidateKey:              candidateKey,
		CandidateState:            candidateState,
		IsSelected:                candidateKey == selectedKey,
		PriorityScore:             40.347531644814964,
		HasOpenPosition:           false,
		Candidates:                candidates,
	}
}

func newSingletonTestStore(t *testing.T) *storage.SQLite {
	t.Helper()
	store := storage.NewSQLite(storage.Config{Path: ":memory:"})
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("start sqlite failed: %v", err)
	}
	if err := store.EnsureSchema(); err != nil {
		_ = store.Close()
		t.Fatalf("ensure schema failed: %v", err)
	}
	return store
}

func upsertTestTrendGroup(t *testing.T, store *storage.SQLite, group models.RiskTrendGroup, candidates ...models.RiskTrendGroupCandidate) models.RiskTrendGroup {
	t.Helper()
	if strings.TrimSpace(group.Mode) == "" {
		group.Mode = "live"
	}
	if err := store.UpsertRiskTrendGroup(&group); err != nil {
		t.Fatalf("upsert trend group failed: %v", err)
	}
	for i := range candidates {
		candidates[i].GroupID = group.ID
		if strings.TrimSpace(candidates[i].Mode) == "" {
			candidates[i].Mode = group.Mode
		}
		if err := store.UpsertRiskTrendGroupCandidate(&candidates[i]); err != nil {
			t.Fatalf("upsert trend group candidate failed: %v", err)
		}
	}
	return group
}

func insertTestExchange(t *testing.T, store *storage.SQLite, exchange models.Exchange) {
	t.Helper()
	active := 0
	if exchange.Active {
		active = 1
	}
	_, err := store.DB.Exec(
		`INSERT INTO exchanges (
			name, api_key, rate_limit, ohlcv_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		exchange.Name,
		exchange.APIKey,
		exchange.RateLimitMS,
		exchange.OHLCVLimit,
		exchange.VolumeFilter,
		exchange.MarketProxy,
		exchange.TradeProxy,
		exchange.Timeframes,
		active,
		0,
		0,
	)
	if err != nil {
		t.Fatalf("insert exchange failed: %v", err)
	}
}

func saveTestOHLCV(t *testing.T, store *storage.SQLite, exchange, symbol, timeframe string, items []models.OHLCV) {
	t.Helper()
	for _, item := range items {
		if err := store.SaveOHLCV(models.MarketData{
			Exchange:  exchange,
			Symbol:    symbol,
			Timeframe: timeframe,
			OHLCV:     item,
			Closed:    true,
			Source:    "test",
		}); err != nil {
			t.Fatalf("save ohlcv failed: %v", err)
		}
	}
}

func insertTestHistorySnapshot(t *testing.T, store *storage.SQLite, row models.RiskHistoryPosition) {
	t.Helper()
	if _, err := store.DB.Exec(
		`INSERT INTO history_positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, open_update_time_ms,
		     max_floating_loss_amount, max_floating_profit_amount, open_row_json, close_avg_px, realized_pnl,
		     pnl_ratio, fee, funding_fee, close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		row.SingletonID,
		row.Mode,
		row.Exchange,
		row.Symbol,
		row.InstID,
		row.Pos,
		row.PosSide,
		row.MgnMode,
		row.Margin,
		row.Lever,
		row.AvgPx,
		row.NotionalUSD,
		row.MarkPx,
		row.LiqPx,
		row.TPTriggerPx,
		row.SLTriggerPx,
		row.OpenTimeMS,
		row.OpenUpdateTimeMS,
		row.MaxFloatingLossAmount,
		row.MaxFloatingProfitAmount,
		row.OpenRowJSON,
		row.CloseAvgPx,
		row.RealizedPnl,
		row.PnlRatio,
		row.Fee,
		row.FundingFee,
		row.CloseTimeMS,
		row.State,
		row.CloseRowJSON,
		row.CreatedAtMS,
		row.UpdatedAtMS,
	); err != nil {
		t.Fatalf("insert history snapshot failed: %v", err)
	}
}

func insertTestRiskOpenPosition(t *testing.T, store *storage.SQLite, row models.RiskOpenPosition) {
	t.Helper()
	if _, err := store.DB.Exec(
		`INSERT INTO positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
		     upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, update_time_ms,
		     row_json, max_floating_loss_amount, max_floating_profit_amount, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		row.SingletonID,
		row.Mode,
		row.Exchange,
		row.Symbol,
		row.InstID,
		row.Pos,
		row.PosSide,
		row.MgnMode,
		row.Margin,
		row.Lever,
		row.AvgPx,
		row.Upl,
		row.UplRatio,
		row.NotionalUSD,
		row.MarkPx,
		row.LiqPx,
		row.TPTriggerPx,
		row.SLTriggerPx,
		row.OpenTimeMS,
		row.UpdateTimeMS,
		row.RowJSON,
		row.MaxFloatingLossAmount,
		row.MaxFloatingProfitAmount,
		row.UpdatedAtMS,
		row.UpdatedAtMS,
	); err != nil {
		t.Fatalf("insert open position failed: %v", err)
	}
}

func insertTestExecutionOrder(t *testing.T, store *storage.SQLite, record models.ExecutionOrderRecord) {
	t.Helper()
	if err := store.InsertExecutionOrder(record); err != nil {
		t.Fatalf("insert execution order failed: %v", err)
	}
}

func TestHandleHistoryIncludesSingletonID(t *testing.T) {
	now := time.Now().In(time.Local)
	exitTime := now.Add(-time.Hour).Truncate(time.Second)
	entryTime := exitTime.Add(-time.Hour)

	server := New(Config{
		AccountProvider: stubAccountProvider{
			history: []models.Position{{
				SingletonID:             7,
				Exchange:                "okx",
				Symbol:                  "BTC/USDT",
				Timeframe:               "1h",
				PositionSide:            "long",
				GroupID:                 "turtle|1h|long|1700000000000",
				MarginMode:              "isolated",
				MarginAmount:            100,
				EntryPrice:              100,
				EntryQuantity:           1,
				EntryValue:              100,
				EntryTime:               entryTime.Format("2006-01-02 15:04:05"),
				ExitPrice:               105,
				ExitQuantity:            1,
				ExitValue:               105,
				ExitTime:                exitTime.Format("2006-01-02 15:04:05"),
				FeeAmount:               -0.1,
				ProfitAmount:            4.9,
				ProfitRate:              0.049,
				MaxFloatingProfitAmount: 6,
				MaxFloatingLossAmount:   1,
				Status:                  models.PositionStatusClosed,
				StrategyName:            "turtle",
				StrategyVersion:         "v0.0.5",
				UpdatedTime:             exitTime.Format("2006-01-02 15:04:05"),
			}},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/history?exchange=okx&range=7d", nil)
	rr := httptest.NewRecorder()
	server.handleHistory(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp historyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Positions) != 1 {
		t.Fatalf("unexpected history payload: count=%d len=%d", resp.Count, len(resp.Positions))
	}
	if resp.Positions[0].SingletonID != 7 {
		t.Fatalf("unexpected singleton_id: %d", resp.Positions[0].SingletonID)
	}
	if resp.Positions[0].GroupID != "turtle|1h|long|1700000000000" {
		t.Fatalf("unexpected group_id: %s", resp.Positions[0].GroupID)
	}
	if resp.Groups == nil || len(resp.Groups) != 0 {
		t.Fatalf("expected empty history groups, got %+v", resp.Groups)
	}
}

func TestHandlePositionEventsResolvesCurrentOpenPositionFromPositionsTable(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	openTime := time.Date(2026, 4, 10, 13, 21, 0, 0, time.UTC)
	openMS := openTime.UnixMilli()
	meta := models.StrategyContextMeta{
		StrategyName:       "turtle",
		StrategyVersion:    "v0.0.7",
		StrategyTimeframes: []string{"3m", "15m", "1h"},
		ComboKey:           "3m/15m/1h",
	}
	rowJSON := models.MarshalPositionRowEnvelopeWithRuntime(nil, meta, models.PositionRuntimeMeta{
		RunID:       "run-trb-open",
		SingletonID: 7,
	})
	insertTestRiskOpenPosition(t, store, models.RiskOpenPosition{
		SingletonID:             7,
		Mode:                    "live",
		Exchange:                "okx",
		Symbol:                  "TRB/USDT",
		InstID:                  "TRB-USDT-SWAP",
		Pos:                     "1",
		PosSide:                 "long",
		MgnMode:                 "isolated",
		Margin:                  "0.505",
		Lever:                   "3",
		AvgPx:                   "15.15",
		Upl:                     "0.004",
		UplRatio:                "0.0079",
		NotionalUSD:             "1.519",
		MarkPx:                  "15.19",
		LiqPx:                   "0",
		TPTriggerPx:             "16.16",
		SLTriggerPx:             "14.92",
		OpenTimeMS:              openMS,
		UpdateTimeMS:            openMS,
		RowJSON:                 rowJSON,
		MaxFloatingLossAmount:   0.011,
		MaxFloatingProfitAmount: 0.007,
		UpdatedAtMS:             openMS,
	})

	insertTestHistorySnapshot(t, store, models.RiskHistoryPosition{
		SingletonID:             7,
		Mode:                    "live",
		Exchange:                "okx",
		Symbol:                  "TRB/USDT",
		InstID:                  "TRB-USDT-SWAP",
		Pos:                     "1",
		PosSide:                 "long",
		MgnMode:                 "isolated",
		Margin:                  "0.400",
		Lever:                   "3",
		AvgPx:                   "14.80",
		NotionalUSD:             "1.480",
		MarkPx:                  "14.80",
		TPTriggerPx:             "15.50",
		SLTriggerPx:             "14.20",
		OpenTimeMS:              openMS - 24*int64(time.Hour/time.Millisecond),
		OpenUpdateTimeMS:        openMS - 24*int64(time.Hour/time.Millisecond),
		MaxFloatingLossAmount:   0.02,
		MaxFloatingProfitAmount: 0.03,
		OpenRowJSON:             rowJSON,
		CloseAvgPx:              "15.00",
		RealizedPnl:             "0.1",
		PnlRatio:                "0.02",
		Fee:                     "0",
		FundingFee:              "0",
		CloseTimeMS:             openMS - 23*int64(time.Hour/time.Millisecond),
		State:                   models.PositionStatusClosed,
		CloseRowJSON:            "{}",
		CreatedAtMS:             openMS - 24*int64(time.Hour/time.Millisecond),
		UpdatedAtMS:             openMS - 23*int64(time.Hour/time.Millisecond),
	})

	server := New(Config{
		Mode:         "live",
		HistoryStore: store,
	})

	req := httptest.NewRequest(http.MethodGet, "/positions/events?position_id=0&exchange=okx&symbol=TRB%2FUSDT&position_side=long&margin_mode=isolated&entry_time=2026-04-10%2013:21:00&strategy=turtle&version=v0.0.7&event_limit=50", nil)
	rr := httptest.NewRecorder()
	server.handlePositionEvents(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d body=%s", rr.Code, rr.Body.String())
	}

	var resp positionEventsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if !resp.IsOpen {
		t.Fatalf("expected open position response")
	}
	expectedKey := buildVisualHistoryPositionKey("okx", "TRB-USDT-SWAP", "long", "isolated", openMS)
	if resp.PositionKey != expectedKey {
		t.Fatalf("unexpected position key: %s", resp.PositionKey)
	}
	if resp.Count == 0 || len(resp.Events) == 0 {
		t.Fatalf("expected current open position events, got count=%d", resp.Count)
	}
	if resp.Events[0].Type != "ENTRY" {
		t.Fatalf("expected first event ENTRY, got %s", resp.Events[0].Type)
	}
	if resp.Events[0].EventAt != openMS {
		t.Fatalf("expected entry event at %d, got %d", openMS, resp.Events[0].EventAt)
	}
}

func TestHandleExecutionOrdersReturnsArchivedRecords(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	startedAt := time.Date(2026, 3, 26, 13, 43, 0, 0, time.Local).UnixMilli()
	finishedAt := startedAt + 3000
	if err := store.InsertExecutionOrder(models.ExecutionOrderRecord{
		AttemptID:          "attempt-eth",
		SingletonUUID:      "run-eth",
		Mode:               "live",
		Source:             "exchange:okx:ethusdtp",
		Exchange:           "okx",
		Symbol:             "ETH/USDT",
		Action:             "open",
		OrderType:          "market",
		PositionSide:       "short",
		Size:               1,
		LeverageMultiplier: 4,
		Price:              2100,
		TakeProfitPrice:    2050,
		StopLossPrice:      2125,
		Strategy:           "turtle",
		ResultStatus:       "failed",
		FailSource:         "execution",
		FailStage:          "place_order",
		FailReason:         "reject",
		ExchangeCode:       "51000",
		ExchangeMessage:    "insufficient balance",
		ExchangeOrderID:    "oid-1",
		StartedAtMS:        startedAt,
		FinishedAtMS:       finishedAt,
		DurationMS:         3000,
	}); err != nil {
		t.Fatalf("insert eth execution order failed: %v", err)
	}
	if err := store.InsertExecutionOrder(models.ExecutionOrderRecord{
		AttemptID:          "attempt-btc",
		SingletonUUID:      "run-btc",
		Mode:               "live",
		Source:             "exchange:okx:btcusdtp",
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		Action:             "open",
		OrderType:          "market",
		PositionSide:       "short",
		Size:               1,
		LeverageMultiplier: 3,
		Price:              80000,
		TakeProfitPrice:    78000,
		StopLossPrice:      80500,
		Strategy:           "turtle",
		ResultStatus:       "success",
		ExchangeOrderID:    "oid-2",
		HasSideEffect:      true,
		StartedAtMS:        startedAt,
		FinishedAtMS:       finishedAt,
		DurationMS:         3000,
	}); err != nil {
		t.Fatalf("insert btc execution order failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	req := httptest.NewRequest(
		http.MethodGet,
		"/execution-orders?exchange=okx&symbol=ETH/USDT&start_time="+url.QueryEscape("2026/03/26 13:40")+"&end_time="+url.QueryEscape("2026/03/26 13:45"),
		nil,
	)
	rr := httptest.NewRecorder()
	server.handleExecutionOrders(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp executionOrdersResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Orders) != 1 {
		t.Fatalf("unexpected execution orders payload: %+v", resp)
	}
	order := resp.Orders[0]
	if order.Symbol != "ETH/USDT" || order.Action != "open" || order.ResultStatus != "failed" {
		t.Fatalf("unexpected execution order item: %+v", order)
	}
	if order.StartedTime != "2026-03-26 13:43:00" || order.FinishedTime != "2026-03-26 13:43:03" {
		t.Fatalf("unexpected formatted times: %+v", order)
	}
}

func TestHandleTradingViewRuntimeReturnsHeldSymbolsAndTimeframes(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert eth symbol failed: %v", err)
	}
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Base:     "BTC",
		Quote:    "USDT",
		Type:     "spot",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert btc symbol failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"combo":[{"timeframes":["3m","15m","1h"],"trade_enabled":true}]}`, "test"); err != nil {
		t.Fatalf("upsert config.strategy failed: %v", err)
	}

	now := time.Now().UTC().Truncate(3 * time.Minute)
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "3m", []models.OHLCV{
		{TS: now.Add(-6 * time.Minute).UnixMilli(), Open: 2000, High: 2010, Low: 1995, Close: 2005, Volume: 10},
		{TS: now.Add(-3 * time.Minute).UnixMilli(), Open: 2005, High: 2030, Low: 2000, Close: 2025, Volume: 12},
	})
	saveTestOHLCV(t, store, "okx", "BTC/USDT", "3m", []models.OHLCV{
		{TS: now.Add(-6 * time.Minute).UnixMilli(), Open: 65000, High: 65200, Low: 64800, Close: 65100, Volume: 2},
		{TS: now.Add(-3 * time.Minute).UnixMilli(), Open: 65100, High: 65500, Low: 65050, Close: 65400, Volume: 2.5},
	})

	server := New(Config{
		SymbolProvider: store,
		TradingViewRuntime: stubTradingViewRuntimeProvider{
			symbols: []market.RuntimeSymbolSnapshot{
				{Exchange: "okx", Symbol: "ETH/USDT", Active: true, WSSubscribed: true},
				{Exchange: "okx", Symbol: "BTC/USDT", Active: true},
			},
			state: map[string][2]string{
				"okx": {"ready", "ok"},
			},
		},
		AccountProvider: stubAccountProvider{
			funds: models.RiskAccountFunds{
				Exchange:           "okx",
				Currency:           "USDT",
				TotalUSDT:          1000,
				TradingUSDT:        800,
				FundingUSDT:        200,
				PerTradeUSDT:       100,
				DailyProfitUSDT:    20,
				FloatingProfitRate: 0.03,
			},
			positions: []models.Position{{
				Exchange:               "okx",
				Symbol:                 "ETH/USDT",
				PositionSide:           "long",
				MarginMode:             "isolated",
				LeverageMultiplier:     3,
				MarginAmount:           120,
				EntryPrice:             1980,
				EntryQuantity:          1.2,
				EntryValue:             2400,
				CurrentPrice:           2025,
				UnrealizedProfitAmount: 54,
				UnrealizedProfitRate:   0.45,
				UpdatedTime:            now.Format("2006-01-02 15:04:05"),
			}},
			history: []models.Position{{
				Exchange:           "okx",
				Symbol:             "BTC/USDT",
				PositionSide:       "short",
				MarginMode:         "isolated",
				LeverageMultiplier: 2,
				EntryPrice:         66000,
				ExitPrice:          65000,
				ProfitAmount:       100,
				ProfitRate:         0.1,
				EntryTime:          now.Add(-2 * time.Hour).Format("2006-01-02 15:04:05"),
				ExitTime:           now.Add(-time.Hour).Format("2006-01-02 15:04:05"),
			}},
		},
		HistoryStore: store,
	})

	req := httptest.NewRequest(http.MethodGet, "/tradingview/api/v1/runtime?exchange=okx", nil)
	rr := httptest.NewRecorder()
	server.handleTradingViewRuntime(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp tradingViewRuntimeResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.SelectedExchange != "okx" {
		t.Fatalf("unexpected exchange: %s", resp.SelectedExchange)
	}
	if got, want := len(resp.Timeframes), 3; got != want {
		t.Fatalf("unexpected timeframes len: got=%d want=%d", got, want)
	}
	if resp.DefaultSymbol != "ETH/USDT" {
		t.Fatalf("unexpected default symbol: %s", resp.DefaultSymbol)
	}
	if len(resp.Symbols) != 2 {
		t.Fatalf("unexpected symbols len: %d", len(resp.Symbols))
	}
	if resp.Symbols[0].DisplaySymbol != "ETH/USDT.P" {
		t.Fatalf("unexpected held display symbol: %s", resp.Symbols[0].DisplaySymbol)
	}
	if !resp.Symbols[0].IsHeld {
		t.Fatalf("expected held symbol first")
	}
	if len(resp.Positions) != 1 || len(resp.HistoryPositions) != 1 {
		t.Fatalf("unexpected positions payload: open=%d history=%d", len(resp.Positions), len(resp.HistoryPositions))
	}
}

func TestHandleTradingViewRuntimeLiteSkipsHeavySections(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert eth symbol failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"paper":["turtle"],"combo":[{"timeframes":["3m","15m","1h"],"trade_enabled":true}]}`, "test"); err != nil {
		t.Fatalf("upsert config.strategy failed: %v", err)
	}

	now := time.Now().UTC().Truncate(3 * time.Minute)
	server := New(Config{
		Mode:           "paper",
		SymbolProvider: store,
		TradingViewRuntime: stubTradingViewRuntimeProvider{
			symbols: []market.RuntimeSymbolSnapshot{
				{Exchange: "okx", Symbol: "ETH/USDT", Active: true, WSSubscribed: true},
			},
			state: map[string][2]string{
				"okx": {"warming", "warming=okx"},
			},
		},
		AccountProvider: stubAccountProvider{
			funds: models.RiskAccountFunds{
				Exchange:    "okx",
				Currency:    "USDT",
				TotalUSDT:   1000,
				TradingUSDT: 800,
				FundingUSDT: 200,
			},
			positions: []models.Position{{
				Exchange:               "okx",
				Symbol:                 "ETH/USDT",
				PositionSide:           "long",
				MarginMode:             "isolated",
				LeverageMultiplier:     3,
				MarginAmount:           120,
				EntryPrice:             1980,
				CurrentPrice:           2025,
				UnrealizedProfitAmount: 54,
				UnrealizedProfitRate:   0.45,
				UpdatedTime:            now.Format("2006-01-02 15:04:05"),
			}},
			history: []models.Position{{
				Exchange:     "okx",
				Symbol:       "BTC/USDT",
				PositionSide: "short",
				EntryPrice:   66000,
				ExitPrice:    65000,
				ProfitAmount: 100,
				ProfitRate:   0.1,
			}},
		},
		HistoryStore: store,
	})

	req := httptest.NewRequest(http.MethodGet, "/tradingview/api/v1/runtime?exchange=okx&lite=1", nil)
	rr := httptest.NewRecorder()
	server.handleTradingViewRuntime(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp tradingViewRuntimeResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.BootstrapComplete {
		t.Fatalf("expected bootstrap_complete=false for lite runtime")
	}
	if got := len(resp.Positions); got != 1 {
		t.Fatalf("unexpected lite positions len: %d", got)
	}
	if got := len(resp.HistoryPositions); got != 0 {
		t.Fatalf("expected lite history positions empty, got=%d", got)
	}
	if got := len(resp.Orders); got != 0 {
		t.Fatalf("expected lite orders empty, got=%d", got)
	}
	if resp.Funds.Exchange != "okx" {
		t.Fatalf("unexpected lite funds exchange: %s", resp.Funds.Exchange)
	}
}

func TestLoadTradingViewHistoryPositionItemsPreservesFirstPartialCloseOrder(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() { _ = store.Close() }()

	baseMS := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC).UnixMilli()
	insertTestHistorySnapshot(t, store, models.RiskHistoryPosition{
		SingletonID: 1,
		Mode:        "live",
		Exchange:    "okx",
		Symbol:      "BTC/USDT",
		Pos:         "1",
		PosSide:     "long",
		MgnMode:     "isolated",
		Margin:      "100",
		Lever:       "7",
		AvgPx:       "60000",
		NotionalUSD: "60000",
		OpenTimeMS:  baseMS,
		CloseAvgPx:  "61600",
		RealizedPnl: "1600",
		PnlRatio:    "16",
		CloseTimeMS: baseMS + int64(5*time.Hour/time.Millisecond),
		State:       models.PositionStatusClosed,
		UpdatedAtMS: baseMS + int64(5*time.Hour/time.Millisecond),
	})
	insertTestHistorySnapshot(t, store, models.RiskHistoryPosition{
		SingletonID: 2,
		Mode:        "live",
		Exchange:    "okx",
		Symbol:      "ETH/USDT",
		Pos:         "1",
		PosSide:     "long",
		MgnMode:     "isolated",
		Margin:      "80",
		Lever:       "20",
		AvgPx:       "3000",
		NotionalUSD: "3000",
		OpenTimeMS:  baseMS + int64(30*time.Minute/time.Millisecond),
		CloseAvgPx:  "3090",
		RealizedPnl: "90",
		PnlRatio:    "11.25",
		CloseTimeMS: baseMS + int64(3*time.Hour/time.Millisecond),
		State:       models.PositionStatusClosed,
		UpdatedAtMS: baseMS + int64(3*time.Hour/time.Millisecond),
	})

	insertTestExecutionOrder(t, store, models.ExecutionOrderRecord{
		AttemptID:          "btc-partial-close",
		Mode:               "live",
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		InstID:             "BTC-USDT-SWAP",
		Action:             models.DecisionActionClose,
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		Size:               0.4,
		LeverageMultiplier: 7,
		Price:              61000,
		ResultStatus:       "success",
		RequestJSON:        `{"EventTS":1775613600000,"Action":"close","PositionSide":"long"}`,
		StartedAtMS:        baseMS + int64(2*time.Hour/time.Millisecond),
		FinishedAtMS:       baseMS + int64(2*time.Hour/time.Millisecond),
	})
	insertTestExecutionOrder(t, store, models.ExecutionOrderRecord{
		AttemptID:          "btc-full-close",
		Mode:               "live",
		Exchange:           "okx",
		Symbol:             "BTC/USDT",
		InstID:             "BTC-USDT-SWAP",
		Action:             models.DecisionActionClose,
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		Size:               0.6,
		LeverageMultiplier: 7,
		Price:              62000,
		ResultStatus:       "success",
		RequestJSON:        `{"EventTS":1775624400000,"Action":"close","PositionSide":"long"}`,
		StartedAtMS:        baseMS + int64(5*time.Hour/time.Millisecond),
		FinishedAtMS:       baseMS + int64(5*time.Hour/time.Millisecond),
	})
	insertTestExecutionOrder(t, store, models.ExecutionOrderRecord{
		AttemptID:          "eth-full-close",
		Mode:               "live",
		Exchange:           "okx",
		Symbol:             "ETH/USDT",
		InstID:             "ETH-USDT-SWAP",
		Action:             models.DecisionActionClose,
		PositionSide:       "long",
		MarginMode:         models.MarginModeIsolated,
		Size:               1,
		LeverageMultiplier: 20,
		Price:              3090,
		ResultStatus:       "success",
		RequestJSON:        `{"EventTS":1775617200000,"Action":"close","PositionSide":"long"}`,
		StartedAtMS:        baseMS + int64(3*time.Hour/time.Millisecond),
		FinishedAtMS:       baseMS + int64(3*time.Hour/time.Millisecond),
	})

	server := &Server{
		cfg: Config{
			Mode: "live",
			AccountProvider: stubAccountProvider{
				history: nil,
			},
			HistoryStore: store,
		},
	}

	items, err := server.loadTradingViewHistoryPositionItems("okx", nil)
	if err != nil {
		t.Fatalf("load tradingview history positions failed: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 history rows, got %d", len(items))
	}
	if items[0].Symbol != "ETH/USDT" {
		t.Fatalf("expected ETH row to stay ahead because its first close happened later, got %+v", items[0])
	}
	if items[1].Symbol != "BTC/USDT" {
		t.Fatalf("expected BTC row second, got %+v", items[1])
	}
	if items[1].CloseStatus != tradeActionFullClose {
		t.Fatalf("expected BTC row to be full close, got %q", items[1].CloseStatus)
	}
	if items[1].ExitTime != formatLocalTimeMS(baseMS+int64(5*time.Hour/time.Millisecond)) {
		t.Fatalf("expected BTC row to keep final close time, got %q", items[1].ExitTime)
	}
}

func TestLoadTradingViewHistoryPositionItemsSynthesizesOpenPartialClose(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() { _ = store.Close() }()

	baseMS := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC).UnixMilli()
	openPosition := models.Position{
		PositionID:              11,
		SingletonID:             3,
		Exchange:                "okx",
		Symbol:                  "SOL/USDT",
		Timeframe:               "15m",
		PositionSide:            "short",
		MarginMode:              models.MarginModeIsolated,
		LeverageMultiplier:      20,
		MarginAmount:            50,
		EntryPrice:              100,
		EntryQuantity:           2,
		EntryValue:              200,
		EntryTime:               formatLocalTimeMS(baseMS),
		Status:                  models.PositionStatusOpen,
		UpdatedTime:             formatLocalTimeMS(baseMS + int64(3*time.Hour/time.Millisecond)),
		MaxFloatingLossAmount:   -15,
		MaxFloatingProfitAmount: 30,
	}
	insertTestExecutionOrder(t, store, models.ExecutionOrderRecord{
		AttemptID:          "sol-partial-close",
		Mode:               "live",
		Exchange:           "okx",
		Symbol:             "SOL/USDT",
		InstID:             "SOL-USDT-SWAP",
		Action:             models.DecisionActionClose,
		PositionSide:       "short",
		MarginMode:         models.MarginModeIsolated,
		Size:               1,
		LeverageMultiplier: 20,
		Price:              90,
		ResultStatus:       "success",
		RequestJSON:        `{"EventTS":1775613600000,"Action":"close","PositionSide":"short"}`,
		StartedAtMS:        baseMS + int64(2*time.Hour/time.Millisecond),
		FinishedAtMS:       baseMS + int64(2*time.Hour/time.Millisecond),
	})

	server := &Server{
		cfg: Config{
			Mode: "live",
			AccountProvider: stubAccountProvider{
				history: nil,
			},
			HistoryStore: store,
		},
	}

	items, err := server.loadTradingViewHistoryPositionItems("okx", []models.Position{openPosition})
	if err != nil {
		t.Fatalf("load tradingview history positions failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 synthesized partial-close row, got %d", len(items))
	}
	item := items[0]
	if item.CloseStatus != tradeActionPartial {
		t.Fatalf("expected partial close status, got %q", item.CloseStatus)
	}
	if item.LeverageMultiplier != 20 {
		t.Fatalf("expected leverage 20, got %v", item.LeverageMultiplier)
	}
	if item.ExitTime != formatLocalTimeMS(baseMS+int64(2*time.Hour/time.Millisecond)) {
		t.Fatalf("unexpected partial close time: %q", item.ExitTime)
	}
	if item.ProfitAmount != 10 {
		t.Fatalf("expected realized profit 10 for short partial close, got %v", item.ProfitAmount)
	}
}

func TestHandleTradingViewCandlesReturnsEMA(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Minute)
	items := make([]models.OHLCV, 0, 12)
	for i := 0; i < 12; i++ {
		base := 2000 + float64(i*5)
		items = append(items, models.OHLCV{
			TS:     now.Add(time.Duration(i-12) * time.Minute).UnixMilli(),
			Open:   base,
			High:   base + 4,
			Low:    base - 3,
			Close:  base + 2,
			Volume: 10 + float64(i),
		})
	}
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "1m", items)

	server := New(Config{
		SymbolProvider: store,
		HistoryStore:   store,
	})

	req := httptest.NewRequest(http.MethodGet, "/tradingview/api/v1/candles?exchange=okx&symbol=ETH/USDT&timeframe=1m", nil)
	rr := httptest.NewRecorder()
	server.handleTradingViewCandles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp tradingViewCandlesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.DisplaySymbol != "ETH/USDT.P" {
		t.Fatalf("unexpected display symbol: %s", resp.DisplaySymbol)
	}
	if len(resp.Candles) != len(items) {
		t.Fatalf("unexpected candles len: got=%d want=%d", len(resp.Candles), len(items))
	}
	if len(resp.Indicators) != 4 {
		t.Fatalf("unexpected indicators len: %d", len(resp.Indicators))
	}
	if resp.Indicators[0].ID != "ema-5" {
		t.Fatalf("unexpected first indicator id: %s", resp.Indicators[0].ID)
	}
}

func TestHandleTradingViewCandlesIncludesRuntimeCurrentCandle(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"combo":[{"strategy":"turtle","timeframes":["3m","15m","1h"]}]}`, "test"); err != nil {
		t.Fatalf("upsert strategy config failed: %v", err)
	}

	now := time.Now().UTC().Truncate(3 * time.Minute)
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "3m", []models.OHLCV{
		{TS: now.Add(-6 * time.Minute).UnixMilli(), Open: 2000, High: 2010, Low: 1995, Close: 2005, Volume: 10},
		{TS: now.Add(-3 * time.Minute).UnixMilli(), Open: 2005, High: 2030, Low: 2000, Close: 2025, Volume: 12},
	})

	server := New(Config{
		SymbolProvider: store,
		HistoryStore:   store,
		TradingViewRuntime: stubTradingViewRuntimeProvider{
			candles: map[string]market.RuntimeOHLCVSnapshot{
				"okx|ETH/USDT|3m": {
					Exchange:  "okx",
					Symbol:    "ETH/USDT",
					Timeframe: "3m",
					OHLCV: models.OHLCV{
						TS:     now.UnixMilli(),
						Open:   2025,
						High:   2038,
						Low:    2020,
						Close:  2036,
						Volume: 9,
					},
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/tradingview/api/v1/candles?exchange=okx&symbol=ETH/USDT&timeframe=3m", nil)
	rr := httptest.NewRecorder()
	server.handleTradingViewCandles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}

	var resp tradingViewCandlesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if len(resp.Candles) != 3 {
		t.Fatalf("unexpected candles len: got=%d want=3", len(resp.Candles))
	}
	last := resp.Candles[len(resp.Candles)-1]
	if last.TS != now.UnixMilli() {
		t.Fatalf("unexpected runtime candle ts: got=%d want=%d", last.TS, now.UnixMilli())
	}
	if last.Close != 2036 {
		t.Fatalf("unexpected runtime candle close: got=%v want=2036", last.Close)
	}
}

func TestBuildTradingViewWSTailIncludesRuntimeCurrentCandle(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Base:     "BTC",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"combo":[{"strategy":"turtle","timeframes":["3m","15m","1h"]}]}`, "test"); err != nil {
		t.Fatalf("upsert strategy config failed: %v", err)
	}

	now := time.Now().UTC().Truncate(3 * time.Minute)
	saveTestOHLCV(t, store, "okx", "BTC/USDT", "3m", []models.OHLCV{
		{TS: now.Add(-6 * time.Minute).UnixMilli(), Open: 68000, High: 68100, Low: 67900, Close: 68050, Volume: 1},
		{TS: now.Add(-3 * time.Minute).UnixMilli(), Open: 68050, High: 68120, Low: 68010, Close: 68090, Volume: 1.2},
	})

	server := New(Config{
		SymbolProvider: store,
		HistoryStore:   store,
		TradingViewRuntime: stubTradingViewRuntimeProvider{
			candles: map[string]market.RuntimeOHLCVSnapshot{
				"okx|BTC/USDT|3m": {
					Exchange:  "okx",
					Symbol:    "BTC/USDT",
					Timeframe: "3m",
					OHLCV: models.OHLCV{
						TS:     now.UnixMilli(),
						Open:   68090,
						High:   68210,
						Low:    68080,
						Close:  68200,
						Volume: 0.8,
					},
				},
			},
		},
	})

	resp, err := server.buildTradingViewWSTail("okx", "BTC/USDT", "3m", 320)
	if err != nil {
		t.Fatalf("build ws tail failed: %v", err)
	}
	if len(resp.Candles) != 3 {
		t.Fatalf("unexpected candles len: got=%d want=3", len(resp.Candles))
	}
	last := resp.Candles[len(resp.Candles)-1]
	if last.TS != now.UnixMilli() {
		t.Fatalf("unexpected runtime candle ts: got=%d want=%d", last.TS, now.UnixMilli())
	}
	if last.Close != 68200 {
		t.Fatalf("unexpected runtime candle close: got=%v want=68200", last.Close)
	}
}

func TestBuildWSSnapshotStateLenientIncludesTradingViewStreams(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "BTC/USDT",
		Base:     "BTC",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"combo":[{"strategy":"turtle","timeframes":["3m","15m","1h"]}]}`, "test"); err != nil {
		t.Fatalf("upsert strategy config failed: %v", err)
	}

	now := time.Now().UTC().Truncate(3 * time.Minute)
	saveTestOHLCV(t, store, "okx", "BTC/USDT", "3m", []models.OHLCV{
		{TS: now.Add(-6 * time.Minute).UnixMilli(), Open: 68000, High: 68100, Low: 67900, Close: 68050, Volume: 1},
		{TS: now.Add(-3 * time.Minute).UnixMilli(), Open: 68050, High: 68120, Low: 68010, Close: 68090, Volume: 1.2},
	})

	server := New(Config{
		SymbolProvider:  store,
		HistoryStore:    store,
		AccountProvider: stubAccountProvider{},
		TradingViewRuntime: stubTradingViewRuntimeProvider{
			symbols: []market.RuntimeSymbolSnapshot{{
				Exchange:     "okx",
				Symbol:       "BTC/USDT",
				Active:       true,
				WSSubscribed: true,
				LastWSAtMS:   now.UnixMilli(),
			}},
			candles: map[string]market.RuntimeOHLCVSnapshot{
				"okx|BTC/USDT|3m": {
					Exchange:  "okx",
					Symbol:    "BTC/USDT",
					Timeframe: "3m",
					OHLCV: models.OHLCV{
						TS:     now.UnixMilli(),
						Open:   68090,
						High:   68210,
						Low:    68080,
						Close:  68200,
						Volume: 0.8,
					},
				},
			},
		},
	})

	sub, err := normalizeWSSubscription(wsSubscription{
		Streams: []string{wsStreamSymbols, wsStreamCandles},
		SymbolsFilter: wsSymbolsFilter{
			Exchange: "okx",
		},
		CandlesFilter: wsCandlesFilter{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "3m",
			Limit:     320,
		},
	})
	if err != nil {
		t.Fatalf("normalize subscription failed: %v", err)
	}

	snapshotState, errs := server.buildWSSnapshotStateLenient("req-1", sub)
	if len(errs) > 0 {
		t.Fatalf("unexpected snapshot errors: %v", errs)
	}
	if snapshotState.snapshot.Symbols == nil {
		t.Fatalf("expected symbols snapshot")
	}
	if len(snapshotState.snapshot.Symbols.Symbols) != 1 {
		t.Fatalf("unexpected symbols len: %d", len(snapshotState.snapshot.Symbols.Symbols))
	}
	if snapshotState.snapshot.Symbols.Symbols[0].LastPrice != 68200 {
		t.Fatalf("unexpected realtime symbol last price: %v", snapshotState.snapshot.Symbols.Symbols[0].LastPrice)
	}
	if snapshotState.snapshot.Candles == nil {
		t.Fatalf("expected candles snapshot")
	}
	if len(snapshotState.snapshot.Candles.Candles) != 3 {
		t.Fatalf("unexpected candles len: %d", len(snapshotState.snapshot.Candles.Candles))
	}
}

func TestHandleTradingViewCandlesReturnsEmptyArraysWhenNoHistory(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "XAU/USDT",
		Base:     "XAU",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	server := New(Config{
		SymbolProvider: store,
		HistoryStore:   store,
	})

	req := httptest.NewRequest(http.MethodGet, "/tradingview/api/v1/candles?exchange=okx&symbol=XAU/USDT&timeframe=1h", nil)
	rr := httptest.NewRecorder()
	server.handleTradingViewCandles(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}

	var raw map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw response failed: %v", err)
	}
	if _, ok := raw["candles"].([]any); !ok {
		t.Fatalf("candles should encode as array, got %T", raw["candles"])
	}
	if _, ok := raw["indicators"].([]any); !ok {
		t.Fatalf("indicators should encode as array, got %T", raw["indicators"])
	}

	var resp tradingViewCandlesResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode typed response failed: %v", err)
	}
	if resp.Candles == nil {
		t.Fatalf("candles should not be nil")
	}
	if resp.Indicators == nil {
		t.Fatalf("indicators should not be nil")
	}
	if len(resp.Candles) != 0 {
		t.Fatalf("unexpected candles len: %d", len(resp.Candles))
	}
	if len(resp.Indicators) != 0 {
		t.Fatalf("unexpected indicators len: %d", len(resp.Indicators))
	}
}

func TestResolveTradingViewBacktestSourcePrefersDBWhenCoverageComplete(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	startMS := time.Date(2026, 4, 8, 9, 0, 0, 0, time.UTC).UnixMilli()
	endMS := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli()

	saveTestOHLCV(t, store, "okx", "ETH/USDT", "3m", generateOHLCVSeries(startMS-(2*3*60*1000), endMS, 3*60*1000, 2000))
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "15m", generateOHLCVSeries(startMS-(2*15*60*1000), endMS, 15*60*1000, 2000))

	server := New(Config{HistoryStore: store})
	source, err := server.resolveTradingViewBacktestSource("okx", "ETH/USDT", []string{"3m", "15m"}, startMS, endMS, 2)
	if err != nil {
		t.Fatalf("resolve backtest source failed: %v", err)
	}
	if !strings.HasPrefix(source, "db:okx:ETH/USDT:3m/15m:") {
		t.Fatalf("expected db backtest source, got %q", source)
	}
}

func TestResolveTradingViewBacktestSourcePlanPrefersDBAndClampsWarmupToLocalCoverage(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	startMS := time.Date(2026, 4, 8, 9, 0, 0, 0, time.UTC).UnixMilli()
	endMS := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli()

	// 3m/15m replay window is complete and has enough warmup.
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "3m", generateOHLCVSeries(startMS-(3*3*60*1000), endMS, 3*60*1000, 2000))
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "15m", generateOHLCVSeries(startMS-(3*15*60*1000), endMS, 15*60*1000, 2000))
	// 1h replay window is complete but only one warmup bar exists before the start boundary.
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "1h", generateOHLCVSeries(startMS-(1*60*60*1000), endMS, 60*60*1000, 2000))

	server := New(Config{HistoryStore: store})
	plan, err := server.resolveTradingViewBacktestSourcePlan("okx", "ETH/USDT", []string{"3m", "15m", "1h"}, startMS, endMS, 3)
	if err != nil {
		t.Fatalf("resolve backtest source plan failed: %v", err)
	}
	if !strings.HasPrefix(plan.Source, "db:okx:ETH/USDT:3m/15m/1h:") {
		t.Fatalf("expected db backtest source plan, got %q", plan.Source)
	}
	if plan.HistoryBars != 1 {
		t.Fatalf("expected history bars to clamp to local warmup coverage, got %d", plan.HistoryBars)
	}
}

func TestNormalizeTradingViewBacktestExecutionRangePreservesChartDisplayRange(t *testing.T) {
	displayStartMS := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC).UnixMilli()
	displayEndMS := time.Date(2026, 4, 8, 21, 0, 0, 0, time.UTC).UnixMilli()

	normalizedDisplayStartMS, normalizedDisplayEndMS, err := normalizeTradingViewSelectionDisplayRange("1h", displayStartMS, displayEndMS)
	if err != nil {
		t.Fatalf("normalize display range failed: %v", err)
	}
	if normalizedDisplayStartMS != displayStartMS {
		t.Fatalf("unexpected display start: got %d want %d", normalizedDisplayStartMS, displayStartMS)
	}
	if normalizedDisplayEndMS != displayEndMS {
		t.Fatalf("unexpected display end: got %d want %d", normalizedDisplayEndMS, displayEndMS)
	}

	executionStartMS, executionEndMS, err := normalizeTradingViewBacktestExecutionRange(
		"1h",
		normalizedDisplayStartMS,
		normalizedDisplayEndMS,
		[]string{"3m", "15m", "1h"},
	)
	if err != nil {
		t.Fatalf("normalize execution range failed: %v", err)
	}
	wantExecutionStartMS := time.Date(2026, 4, 8, 0, 0, 0, 0, time.UTC).UnixMilli()
	wantExecutionEndMS := time.Date(2026, 4, 8, 21, 57, 0, 0, time.UTC).UnixMilli()
	if executionStartMS != wantExecutionStartMS {
		t.Fatalf("unexpected execution start: got %d want %d", executionStartMS, wantExecutionStartMS)
	}
	if executionEndMS != wantExecutionEndMS {
		t.Fatalf("unexpected execution end: got %d want %d", executionEndMS, wantExecutionEndMS)
	}
}

func TestResolveTradingViewBacktestSourceFallsBackToExchangeWhenDBCoverageIncomplete(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   300,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "ETH/USDT",
		Base:     "ETH",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	startMS := time.Date(2026, 4, 8, 9, 0, 0, 0, time.UTC).UnixMilli()
	endMS := time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli()

	saveTestOHLCV(t, store, "okx", "ETH/USDT", "3m", generateOHLCVSeries(startMS-(2*3*60*1000), endMS, 3*60*1000, 2000))
	incomplete15m := generateOHLCVSeries(startMS-(2*15*60*1000), endMS, 15*60*1000, 2000)
	incomplete15m = incomplete15m[:len(incomplete15m)-1]
	saveTestOHLCV(t, store, "okx", "ETH/USDT", "15m", incomplete15m)

	server := New(Config{HistoryStore: store})
	source, err := server.resolveTradingViewBacktestSource("okx", "ETH/USDT", []string{"3m", "15m"}, startMS, endMS, 2)
	if err != nil {
		t.Fatalf("resolve backtest source failed: %v", err)
	}
	if !strings.HasPrefix(source, "exchange:okx:ETH/USDT:3m/15m:") {
		t.Fatalf("expected exchange backtest source fallback, got %q", source)
	}
}

func TestReconcileTradingViewBacktestTasksMarksOrphansFailed(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	pendingTask, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "ETH/USDT",
		DisplaySymbol:   "ETH/USDT.P",
		ChartTimeframe:  "1h",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    time.Date(2026, 4, 8, 8, 0, 0, 0, time.UTC).UnixMilli(),
		RangeEndMS:      time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli(),
		Source:          "db:okx:ETH/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create pending task failed: %v", err)
	}
	oldPendingCreatedAtMS := time.Now().Add(-time.Minute).UnixMilli()
	if _, err := store.DB.Exec(`UPDATE backtest_tasks SET created_at_ms = ?, updated_at_ms = ? WHERE id = ?;`, oldPendingCreatedAtMS, oldPendingCreatedAtMS, pendingTask.ID); err != nil {
		t.Fatalf("age pending task failed: %v", err)
	}

	runningTask, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		DisplaySymbol:   "BTC/USDT.P",
		ChartTimeframe:  "1h",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    time.Date(2026, 4, 8, 8, 0, 0, 0, time.UTC).UnixMilli(),
		RangeEndMS:      time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli(),
		Source:          "exchange:okx:BTC/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create running task failed: %v", err)
	}
	if err := store.MarkBacktestTaskRunning(runningTask.ID, 9999, "missing-singleton"); err != nil {
		t.Fatalf("mark task running failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	if err := server.reconcileTradingViewBacktestTasks(); err != nil {
		t.Fatalf("reconcile backtest tasks failed: %v", err)
	}

	pendingTaskAfter, found, err := store.GetBacktestTask(pendingTask.ID)
	if err != nil {
		t.Fatalf("get pending task failed: %v", err)
	}
	if !found {
		t.Fatalf("pending task disappeared")
	}
	if pendingTaskAfter.Status != models.BacktestTaskStatusFailed {
		t.Fatalf("expected stale pending task to fail, got %q", pendingTaskAfter.Status)
	}
	if pendingTaskAfter.ErrorMessage != "back-test launch interrupted" {
		t.Fatalf("unexpected pending task error message: %q", pendingTaskAfter.ErrorMessage)
	}

	runningTaskAfter, found, err := store.GetBacktestTask(runningTask.ID)
	if err != nil {
		t.Fatalf("get running task failed: %v", err)
	}
	if !found {
		t.Fatalf("running task disappeared")
	}
	if runningTaskAfter.Status != models.BacktestTaskStatusFailed {
		t.Fatalf("expected orphan running task to fail, got %q", runningTaskAfter.Status)
	}
	if runningTaskAfter.ErrorMessage != "back-test interrupted" {
		t.Fatalf("unexpected running task error message: %q", runningTaskAfter.ErrorMessage)
	}
}

func TestReconcileTradingViewBacktestTasksMarksCompletedSingletonSucceeded(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	startedAt := time.Date(2026, 4, 8, 12, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(2 * time.Minute)
	result, err := store.DB.Exec(
		`INSERT INTO singleton (
			uuid, version, mode, source, status, created, updated, closed, heartbeat, lease_expires, start_time, end_time, runtime
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"completed-singleton",
		"dev",
		"back-test",
		"db:okx:BTC/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		"completed",
		startedAt.Unix(),
		finishedAt.Unix(),
		finishedAt.Unix(),
		startedAt.Unix(),
		startedAt.Add(30*time.Second).Unix(),
		startedAt.Format(time.RFC3339),
		finishedAt.Format(time.RFC3339),
		"2m0s",
	)
	if err != nil {
		t.Fatalf("insert completed singleton failed: %v", err)
	}
	singletonID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id failed: %v", err)
	}

	task, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		DisplaySymbol:   "BTC/USDT.P",
		ChartTimeframe:  "1h",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    time.Date(2026, 4, 8, 8, 0, 0, 0, time.UTC).UnixMilli(),
		RangeEndMS:      time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli(),
		Source:          "db:okx:BTC/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create task failed: %v", err)
	}
	if _, err := store.DB.Exec(
		`UPDATE backtest_tasks
		    SET status = ?, singleton_id = ?, singleton_uuid = ?, error_message = ?, started_at_ms = ?, updated_at_ms = ?
		  WHERE id = ?;`,
		models.BacktestTaskStatusRunning,
		singletonID,
		"completed-singleton",
		"",
		startedAt.UnixMilli(),
		startedAt.UnixMilli(),
		task.ID,
	); err != nil {
		t.Fatalf("mark task running via sql failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	if err := server.reconcileTradingViewBacktestTasks(); err != nil {
		t.Fatalf("reconcile backtest tasks failed: %v", err)
	}

	taskAfter, found, err := store.GetBacktestTask(task.ID)
	if err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if !found {
		t.Fatalf("task disappeared")
	}
	if taskAfter.Status != models.BacktestTaskStatusSucceeded {
		t.Fatalf("expected completed singleton task to become succeeded, got %q", taskAfter.Status)
	}
	if taskAfter.ErrorMessage != "" {
		t.Fatalf("expected completed singleton task error cleared, got %q", taskAfter.ErrorMessage)
	}
}

func TestReconcileTradingViewBacktestTasksRepairsInterruptedCompletedTask(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	startedAt := time.Date(2026, 4, 8, 13, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(90 * time.Second)
	result, err := store.DB.Exec(
		`INSERT INTO singleton (
			uuid, version, mode, source, status, created, updated, closed, heartbeat, lease_expires, start_time, end_time, runtime
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"finished-singleton",
		"dev",
		"back-test",
		"db:okx:XAU/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		"completed",
		startedAt.Unix(),
		finishedAt.Unix(),
		finishedAt.Unix(),
		startedAt.Unix(),
		startedAt.Add(30*time.Second).Unix(),
		startedAt.Format(time.RFC3339),
		finishedAt.Format(time.RFC3339),
		"1m30s",
	)
	if err != nil {
		t.Fatalf("insert completed singleton failed: %v", err)
	}
	singletonID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id failed: %v", err)
	}

	task, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "XAU/USDT",
		DisplaySymbol:   "XAU/USDT.P",
		ChartTimeframe:  "1h",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    time.Date(2026, 4, 8, 8, 0, 0, 0, time.UTC).UnixMilli(),
		RangeEndMS:      time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC).UnixMilli(),
		Source:          "db:okx:XAU/USDT:3m/15m/1h:20260408_0800Z-20260408_1000Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create task failed: %v", err)
	}
	if _, err := store.DB.Exec(
		`UPDATE backtest_tasks
		    SET status = ?, singleton_id = ?, singleton_uuid = ?, error_message = ?, started_at_ms = ?, finished_at_ms = ?, updated_at_ms = ?
		  WHERE id = ?;`,
		models.BacktestTaskStatusFailed,
		singletonID,
		"finished-singleton",
		"back-test interrupted",
		startedAt.UnixMilli(),
		finishedAt.UnixMilli(),
		finishedAt.UnixMilli(),
		task.ID,
	); err != nil {
		t.Fatalf("mark task failed via sql failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	if err := server.reconcileTradingViewBacktestTasks(); err != nil {
		t.Fatalf("reconcile backtest tasks failed: %v", err)
	}

	taskAfter, found, err := store.GetBacktestTask(task.ID)
	if err != nil {
		t.Fatalf("get task failed: %v", err)
	}
	if !found {
		t.Fatalf("task disappeared")
	}
	if taskAfter.Status != models.BacktestTaskStatusSucceeded {
		t.Fatalf("expected interrupted completed task to be repaired to succeeded, got %q", taskAfter.Status)
	}
	if taskAfter.ErrorMessage != "" {
		t.Fatalf("expected repaired task error cleared, got %q", taskAfter.ErrorMessage)
	}
}

func generateOHLCVSeries(startMS, endMS, stepMS int64, basePrice float64) []models.OHLCV {
	if stepMS <= 0 || endMS < startMS {
		return nil
	}
	out := make([]models.OHLCV, 0, int((endMS-startMS)/stepMS)+1)
	index := 0
	for ts := startMS; ts <= endMS; ts += stepMS {
		price := basePrice + float64(index)
		out = append(out, models.OHLCV{
			TS:     ts,
			Open:   price,
			High:   price + 4,
			Low:    price - 3,
			Close:  price + 2,
			Volume: 10 + float64(index),
		})
		index++
	}
	return out
}

func TestHandleExecutionOrdersReadsMainDB(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	startedAt := time.Date(2026, 3, 26, 13, 43, 0, 0, time.Local).UnixMilli()
	if err := store.InsertExecutionOrder(models.ExecutionOrderRecord{
		AttemptID:          "attempt-eth-main",
		SingletonUUID:      "run-eth",
		Mode:               "live",
		Source:             "exchange:okx:ethusdtp",
		Exchange:           "okx",
		Symbol:             "ETH/USDT",
		Action:             "open",
		OrderType:          "market",
		PositionSide:       "short",
		Size:               1,
		LeverageMultiplier: 4,
		Price:              2100,
		TakeProfitPrice:    2050,
		StopLossPrice:      2125,
		Strategy:           "turtle",
		ResultStatus:       "failed",
		FailSource:         "execution",
		FailStage:          "place_order",
		FailReason:         "reject",
		StartedAtMS:        startedAt,
		FinishedAtMS:       startedAt + 2000,
		DurationMS:         2000,
	}); err != nil {
		t.Fatalf("insert execution order failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	req := httptest.NewRequest(
		http.MethodGet,
		"/execution-orders?exchange=okx&symbol=ETH/USDT&start_time="+url.QueryEscape("2026/03/26 13:40")+"&end_time="+url.QueryEscape("2026/03/26 13:45"),
		nil,
	)
	rr := httptest.NewRecorder()
	server.handleExecutionOrders(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp executionOrdersResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Orders) != 1 {
		t.Fatalf("unexpected execution orders payload: %+v", resp)
	}
	order := resp.Orders[0]
	if order.AttemptID != "attempt-eth-main" || order.Symbol != "ETH/USDT" {
		t.Fatalf("unexpected recovered execution order item: %+v", order)
	}
}

func TestHandleRiskDecisionsReturnsRecordsFromMainDB(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	eventAt := time.Date(2026, 3, 26, 13, 43, 0, 0, time.Local).UnixMilli()
	if err := store.InsertRiskDecision(models.RiskDecisionRecord{
		SingletonID:         170,
		SingletonUUID:       "singleton-170",
		Mode:                "live",
		Exchange:            "okx",
		Symbol:              "ETH/USDT",
		Timeframe:           "30m",
		Strategy:            "turtle",
		ComboKey:            "1m/5m/30m",
		GroupID:             "turtle|30m|short|1774492200000",
		SignalAction:        8,
		HighSide:            -1,
		DecisionAction:      models.DecisionActionIgnore,
		ResultStatus:        "rejected",
		RejectReason:        "risk live: trend guard rejected open: grouped leader not authorized",
		EventAtMS:           eventAt,
		TriggerTimestampMS:  eventAt,
		TrendingTimestampMS: time.Date(2026, 3, 26, 13, 5, 0, 0, time.Local).UnixMilli(),
	}); err != nil {
		t.Fatalf("insert risk decision failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	req := httptest.NewRequest(
		http.MethodGet,
		"/risk-decisions?exchange=okx&symbol=ETH/USDT&signal_action=8&start_time="+url.QueryEscape("2026/03/26 13:40")+"&end_time="+url.QueryEscape("2026/03/26 13:45"),
		nil,
	)
	rr := httptest.NewRecorder()
	server.handleRiskDecisions(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp riskDecisionsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Decisions) != 1 {
		t.Fatalf("unexpected risk decisions payload: %+v", resp)
	}
	item := resp.Decisions[0]
	if item.Symbol != "ETH/USDT" || item.ResultStatus != "rejected" || item.DecisionAction != models.DecisionActionIgnore {
		t.Fatalf("unexpected risk decision item: %+v", item)
	}
	if item.EventTime != "2026-03-26 13:43:00" {
		t.Fatalf("unexpected formatted event time: %+v", item)
	}
}

func TestHandleHistoryIncludesMatchedTrendGroups(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	now := time.Now().UTC()
	newerExit := now.Add(-30 * time.Minute).Format("2006-01-02 15:04:05")
	olderExit := now.Add(-2 * time.Hour).Format("2006-01-02 15:04:05")

	groupA := upsertTestTrendGroup(t, store, models.RiskTrendGroup{
		Strategy:                  "turtle",
		PrimaryTimeframe:          "30m",
		Side:                      "short",
		AnchorTrendingTimestampMS: 1773943200000,
		State:                     "finished",
		LockStage:                 "hard",
		SelectedCandidateKey:      "okx|RIVER/USDT",
		EntryCount:                1,
		CreatedAtMS:               now.Add(-3 * time.Hour).UnixMilli(),
		UpdatedAtMS:               now.Add(-20 * time.Minute).UnixMilli(),
		FinishedAtMS:              now.Add(-20 * time.Minute).UnixMilli(),
	}, models.RiskTrendGroupCandidate{
		CandidateKey:   "okx|NEO/USDT",
		Exchange:       "okx",
		Symbol:         "NEO/USDT",
		CandidateState: "blocked",
		PriorityScore:  40.1,
		UpdatedAtMS:    now.Add(-20 * time.Minute).UnixMilli(),
	}, models.RiskTrendGroupCandidate{
		CandidateKey:    "okx|RIVER/USDT",
		Exchange:        "okx",
		Symbol:          "RIVER/USDT",
		CandidateState:  "selected",
		IsSelected:      true,
		PriorityScore:   41.2,
		HasOpenPosition: false,
		UpdatedAtMS:     now.Add(-20 * time.Minute).UnixMilli(),
	})
	groupB := upsertTestTrendGroup(t, store, models.RiskTrendGroup{
		Strategy:                  "turtle",
		PrimaryTimeframe:          "1h",
		Side:                      "long",
		AnchorTrendingTimestampMS: 1773946800000,
		State:                     "finished",
		LockStage:                 "hard",
		SelectedCandidateKey:      "okx|BTC/USDT",
		EntryCount:                2,
		CreatedAtMS:               now.Add(-5 * time.Hour).UnixMilli(),
		UpdatedAtMS:               now.Add(-90 * time.Minute).UnixMilli(),
		FinishedAtMS:              now.Add(-90 * time.Minute).UnixMilli(),
	}, models.RiskTrendGroupCandidate{
		CandidateKey:    "okx|BTC/USDT",
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		CandidateState:  "selected",
		IsSelected:      true,
		PriorityScore:   55.6,
		HasOpenPosition: false,
		UpdatedAtMS:     now.Add(-90 * time.Minute).UnixMilli(),
	})

	server := New(Config{
		AccountProvider: stubAccountProvider{
			history: []models.Position{
				{
					Exchange:        "okx",
					Symbol:          "RIVER/USDT",
					Timeframe:       "30m",
					PositionSide:    "short",
					GroupID:         historyGroupPublicID(groupA),
					StrategyName:    "turtle",
					StrategyVersion: "v1",
					EntryTime:       now.Add(-90 * time.Minute).Format("2006-01-02 15:04:05"),
					ExitTime:        newerExit,
					Status:          models.PositionStatusClosed,
				},
				{
					Exchange:        "okx",
					Symbol:          "BTC/USDT",
					Timeframe:       "1h",
					PositionSide:    "long",
					GroupID:         historyGroupPublicID(groupB),
					StrategyName:    "turtle",
					StrategyVersion: "v1",
					EntryTime:       now.Add(-3 * time.Hour).Format("2006-01-02 15:04:05"),
					ExitTime:        olderExit,
					Status:          models.PositionStatusClosed,
				},
			},
		},
		HistoryStore: store,
	})

	req := httptest.NewRequest(http.MethodGet, "/history?exchange=okx&range=24h", nil)
	rr := httptest.NewRecorder()
	server.handleHistory(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp historyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if len(resp.Groups) != 2 {
		t.Fatalf("expected 2 history groups, got %+v", resp.Groups)
	}
	if resp.Groups[0].GroupID != historyGroupPublicID(groupA) {
		t.Fatalf("expected most recent history group first, got %+v", resp.Groups)
	}
	if len(resp.Groups[0].Candidates) != 2 || resp.Groups[0].Candidates[0].CandidateKey != "okx|NEO/USDT" {
		t.Fatalf("unexpected candidate ordering: %+v", resp.Groups[0].Candidates)
	}
	if resp.Groups[1].GroupID != historyGroupPublicID(groupB) {
		t.Fatalf("unexpected second history group: %+v", resp.Groups[1])
	}
}

func TestHandlePositionIncludesSingletonID(t *testing.T) {
	server := New(Config{
		AccountProvider: stubAccountProvider{
			positions: []models.Position{{
				SingletonID:             11,
				Exchange:                "okx",
				Symbol:                  "BTC/USDT",
				Timeframe:               "1h",
				PositionSide:            "long",
				GroupID:                 "turtle|1h|long|1700000000000",
				MarginMode:              "isolated",
				LeverageMultiplier:      5,
				MarginAmount:            100,
				EntryPrice:              100,
				EntryQuantity:           1,
				EntryValue:              100,
				EntryTime:               "2026-03-18 10:00:00",
				TakeProfitPrice:         120,
				StopLossPrice:           95,
				CurrentPrice:            105,
				UnrealizedProfitAmount:  5,
				UnrealizedProfitRate:    0.05,
				MaxFloatingProfitAmount: 6,
				MaxFloatingLossAmount:   1,
				Status:                  models.PositionStatusOpen,
				StrategyName:            "turtle",
				StrategyVersion:         "v0.0.5",
				UpdatedTime:             "2026-03-18 10:05:00",
			}},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/position?exchange=okx", nil)
	rr := httptest.NewRecorder()
	server.handlePosition(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp positionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Positions) != 1 {
		t.Fatalf("unexpected position payload: count=%d len=%d", resp.Count, len(resp.Positions))
	}
	if resp.Positions[0].SingletonID != 11 {
		t.Fatalf("unexpected singleton_id: %d", resp.Positions[0].SingletonID)
	}
	if resp.Positions[0].GroupID != "turtle|1h|long|1700000000000" {
		t.Fatalf("unexpected group_id: %s", resp.Positions[0].GroupID)
	}
}

func TestHandleSingletonRequiresIDOrUUID(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	server := New(Config{HistoryStore: store})
	req := httptest.NewRequest(http.MethodGet, "/singleton", nil)
	rr := httptest.NewRecorder()
	server.handleSingleton(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleSingletonUsesANDQueryAndPositiveCache(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	if _, err := store.DB.Exec(
		`INSERT INTO singleton(id, uuid, version, mode, source, status, created, updated, closed, heartbeat, lease_expires, start_time)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?);`,
		int64(1), "run-a", "v1", "live", "okx", "running", int64(100), int64(100), int64(100), int64(130), "2026-03-18 10:00:00",
	); err != nil {
		t.Fatalf("insert singleton failed: %v", err)
	}

	server := New(Config{HistoryStore: store})
	server.singletonRunningTTL = time.Hour
	server.singletonClosedTTL = time.Hour
	server.singletonNotFoundTTL = time.Hour

	firstReq := httptest.NewRequest(http.MethodGet, "/singleton?id=1&uuid=run-a", nil)
	firstRR := httptest.NewRecorder()
	server.handleSingleton(firstRR, firstReq)
	if firstRR.Code != http.StatusOK {
		t.Fatalf("unexpected first status: %d body=%s", firstRR.Code, firstRR.Body.String())
	}
	var first models.SingletonRecord
	if err := json.Unmarshal(firstRR.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first response failed: %v", err)
	}
	if first.Version != "v1" {
		t.Fatalf("unexpected first version: %s", first.Version)
	}

	if _, err := store.DB.Exec(`UPDATE singleton SET version = ?, updated = ? WHERE id = ?;`, "v2", int64(200), int64(1)); err != nil {
		t.Fatalf("update singleton failed: %v", err)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/singleton?id=1&uuid=run-a", nil)
	secondRR := httptest.NewRecorder()
	server.handleSingleton(secondRR, secondReq)
	if secondRR.Code != http.StatusOK {
		t.Fatalf("unexpected second status: %d body=%s", secondRR.Code, secondRR.Body.String())
	}
	var second models.SingletonRecord
	if err := json.Unmarshal(secondRR.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode second response failed: %v", err)
	}
	if second.Version != "v1" {
		t.Fatalf("expected cached singleton version v1, got %s", second.Version)
	}

	andReq := httptest.NewRequest(http.MethodGet, "/singleton?id=1&uuid=other", nil)
	andRR := httptest.NewRecorder()
	server.handleSingleton(andRR, andReq)
	if andRR.Code != http.StatusNotFound {
		t.Fatalf("expected AND query miss, got status=%d body=%s", andRR.Code, andRR.Body.String())
	}
}

func TestHandleSingletonCachesNotFoundResults(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	server := New(Config{HistoryStore: store})
	server.singletonRunningTTL = time.Hour
	server.singletonClosedTTL = time.Hour
	server.singletonNotFoundTTL = time.Hour

	firstReq := httptest.NewRequest(http.MethodGet, "/singleton?id=9", nil)
	firstRR := httptest.NewRecorder()
	server.handleSingleton(firstRR, firstReq)
	if firstRR.Code != http.StatusNotFound {
		t.Fatalf("unexpected first miss status: %d body=%s", firstRR.Code, firstRR.Body.String())
	}

	if _, err := store.DB.Exec(
		`INSERT INTO singleton(id, uuid, version, mode, source, status, created, updated)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		int64(9), "run-9", "v9", "live", "okx", "completed", int64(100), int64(100),
	); err != nil {
		t.Fatalf("insert singleton after miss failed: %v", err)
	}

	secondReq := httptest.NewRequest(http.MethodGet, "/singleton?id=9", nil)
	secondRR := httptest.NewRecorder()
	server.handleSingleton(secondRR, secondReq)
	if secondRR.Code != http.StatusNotFound {
		t.Fatalf("expected cached miss, got status=%d body=%s", secondRR.Code, secondRR.Body.String())
	}
}

func TestHandleSignalsIncludesGroupID(t *testing.T) {
	signal := testSignal("okx", "NEO/USDT")
	groupedCandidates := []models.SignalGroupedCandidate{
		{
			CandidateKey:    "okx|NEO/USDT",
			CandidateState:  "no_trade",
			IsSelected:      false,
			PriorityScore:   40.347531644814964,
			HasOpenPosition: false,
		},
		{
			CandidateKey:    "okx|RIVER/USDT",
			CandidateState:  "selected",
			IsSelected:      true,
			PriorityScore:   40.99867053636889,
			HasOpenPosition: false,
		},
	}
	server := New(Config{
		Provider: stubSignalProvider{
			signals: map[string]map[string]models.Signal{
				"okx|NEO/USDT": {
					"turtle|1m/5m/30m": signal,
				},
			},
		},
		AccountProvider: stubAccountProvider{
			grouped: map[string]models.SignalGroupedInfo{
				flatKey(signal): testGroupedInfo("okx|NEO/USDT", "no_trade", "okx|RIVER/USDT", groupedCandidates),
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/signals", nil)
	rr := httptest.NewRecorder()
	server.handleSignals(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}

	var resp map[string]map[string]models.Signal
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	got := resp["okx|NEO/USDT"]["turtle|1m/5m/30m"]
	if got.GroupID != "turtle|30m|short|1773943200000" {
		t.Fatalf("unexpected group_id: %s", got.GroupID)
	}
	if strings.Contains(rr.Body.String(), "\"grouped\"") {
		t.Fatalf("signal response should not contain grouped payload: %s", rr.Body.String())
	}
}

func TestBuildWSDiffUpdatesGroupsWithoutTouchingSignals(t *testing.T) {
	signal := testSignal("okx", "NEO/USDT")
	grouped := map[string]models.SignalGroupedInfo{
		flatKey(signal): testGroupedInfo(
			"okx|NEO/USDT",
			"no_trade",
			"okx|RIVER/USDT",
			[]models.SignalGroupedCandidate{
				{
					CandidateKey:    "okx|NEO/USDT",
					CandidateState:  "no_trade",
					IsSelected:      false,
					PriorityScore:   40.347531644814964,
					HasOpenPosition: false,
				},
				{
					CandidateKey:    "okx|RIVER/USDT",
					CandidateState:  "selected",
					IsSelected:      true,
					PriorityScore:   40.99867053636889,
					HasOpenPosition: false,
				},
			},
		),
	}
	currentRiskStatus := iface.ModuleStatus{
		Name:  "risk",
		State: "running",
		Details: map[string]any{
			"trend_guard": map[string]any{
				"enabled":       true,
				"mode":          "grouped",
				"groups_total":  1,
				"groups_active": 1,
				"groups": []map[string]any{
					{
						"group_id":                     "turtle|30m|short|1773943200000",
						"strategy":                     "turtle",
						"primary_timeframe":            "30m",
						"side":                         "short",
						"anchor_trending_timestamp_ms": float64(1773943200000),
						"state":                        "soft_locked",
						"lock_stage":                   "soft",
						"selected_candidate_key":       "okx|RIVER/USDT",
						"entry_count":                  0,
						"candidates": []map[string]any{
							{
								"candidate_key":     "okx|NEO/USDT",
								"candidate_state":   "no_trade",
								"is_selected":       false,
								"priority_score":    40.347531644814964,
								"has_open_position": false,
							},
							{
								"candidate_key":     "okx|RIVER/USDT",
								"candidate_state":   "selected",
								"is_selected":       true,
								"priority_score":    40.99867053636889,
								"has_open_position": false,
							},
						},
					},
				},
			},
		},
	}
	server := New(Config{
		Provider: stubSignalProvider{
			signals: map[string]map[string]models.Signal{
				"okx|NEO/USDT": {
					"turtle|1m/5m/30m": signal,
				},
			},
		},
		AccountProvider: stubAccountProvider{grouped: grouped},
		StatusProviders: []iface.StatusProvider{
			stubStatusProvider{
				fn: func() iface.ModuleStatus {
					return currentRiskStatus
				},
			},
		},
	})

	sub, err := normalizeWSSubscription(wsSubscription{Streams: []string{wsStreamSignals, wsStreamGroups}})
	if err != nil {
		t.Fatalf("normalize ws subscription failed: %v", err)
	}
	snapshotState, snapshotErrors := server.buildWSSnapshotStateLenient("", sub)
	if len(snapshotErrors) > 0 {
		t.Fatalf("unexpected snapshot errors: %v", snapshotErrors)
	}

	currentRiskStatus = iface.ModuleStatus{
		Name:  "risk",
		State: "running",
		Details: map[string]any{
			"trend_guard": map[string]any{
				"enabled":       true,
				"mode":          "grouped",
				"groups_total":  1,
				"groups_active": 1,
				"groups": []map[string]any{
					{
						"group_id":                     "turtle|30m|short|1773943200000",
						"strategy":                     "turtle",
						"primary_timeframe":            "30m",
						"side":                         "short",
						"anchor_trending_timestamp_ms": float64(1773943200000),
						"state":                        "soft_locked",
						"lock_stage":                   "soft",
						"selected_candidate_key":       "okx|NEO/USDT",
						"entry_count":                  0,
						"candidates": []map[string]any{
							{
								"candidate_key":     "okx|NEO/USDT",
								"candidate_state":   "selected",
								"is_selected":       true,
								"priority_score":    41.501,
								"has_open_position": false,
							},
							{
								"candidate_key":     "okx|RIVER/USDT",
								"candidate_state":   "blocked",
								"is_selected":       false,
								"priority_score":    40.99867053636889,
								"has_open_position": false,
							},
						},
					},
				},
			},
		},
	}

	diff, _, _, _, _, _, _, _, _, _, _, changed, diffErrors := server.buildWSDiff(
		sub,
		snapshotState.signalFlat,
		snapshotState.groups,
		accountResponse{},
		false,
		nil,
		historyResponse{},
		wsSymbolsResponse{},
		false,
		tradingViewCandlesResponse{},
		false,
	)
	if len(diffErrors) > 0 {
		t.Fatalf("unexpected diff errors: %v", diffErrors)
	}
	if !changed {
		t.Fatalf("expected diff changed when groups change")
	}
	if diff.Signals != nil {
		t.Fatalf("signals diff should stay nil when only group internals change: %+v", diff.Signals)
	}
	if diff.Groups == nil || len(diff.Groups.Groups) != 1 {
		t.Fatalf("expected groups diff payload, got %+v", diff.Groups)
	}
	group := diff.Groups.Groups[0]
	if group.SelectedCandidateKey != "okx|NEO/USDT" {
		t.Fatalf("unexpected selected candidate key: %s", group.SelectedCandidateKey)
	}
	if len(group.Candidates) != 2 || group.Candidates[1].CandidateState != "blocked" {
		t.Fatalf("unexpected groups candidates after update: %+v", group.Candidates)
	}
}

func TestHandleGroupsReturnsTrendGuardSummary(t *testing.T) {
	server := New(Config{
		StatusProviders: []iface.StatusProvider{
			stubStatusProvider{
				status: iface.ModuleStatus{
					Name:  "risk",
					State: "running",
					Details: map[string]any{
						"trend_guard": map[string]any{
							"enabled":       true,
							"mode":          "grouped",
							"groups_total":  6,
							"groups_active": 1,
							"groups": []map[string]any{
								{
									"group_id":                     "turtle|30m|short|1773943200000",
									"strategy":                     "turtle",
									"primary_timeframe":            "30m",
									"side":                         "short",
									"anchor_trending_timestamp_ms": float64(1773943200000),
									"state":                        "soft_locked",
									"lock_stage":                   "soft",
									"selected_candidate_key":       "okx|RIVER/USDT",
									"entry_count":                  0,
									"candidates": []map[string]any{
										{
											"candidate_key":     "okx|NEO/USDT",
											"candidate_state":   "no_trade",
											"is_selected":       false,
											"priority_score":    40.347531644814964,
											"has_open_position": false,
										},
										{
											"candidate_key":     "okx|RIVER/USDT",
											"candidate_state":   "selected",
											"is_selected":       true,
											"priority_score":    40.99867053636889,
											"has_open_position": false,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	rr := httptest.NewRecorder()
	server.handleGroups(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}

	var resp groupsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode groups response failed: %v", err)
	}
	if !resp.Enabled || resp.Mode != "grouped" || resp.GroupsActive != 1 || len(resp.Groups) != 1 {
		t.Fatalf("unexpected groups response: %+v", resp)
	}
	group := resp.Groups[0]
	if group.SelectedCandidateKey != "okx|RIVER/USDT" || len(group.Candidates) != 2 {
		t.Fatalf("unexpected group payload: %+v", group)
	}
	if group.Candidates[0].CandidateKey != "okx|NEO/USDT" || group.Candidates[1].CandidateKey != "okx|RIVER/USDT" {
		t.Fatalf("unexpected group candidates: %+v", group.Candidates)
	}
}

func TestHandleStatusOmitsTrendGuardGroups(t *testing.T) {
	server := New(Config{
		StatusProviders: []iface.StatusProvider{
			stubStatusProvider{
				status: iface.ModuleStatus{
					Name:  "risk",
					State: "running",
					Details: map[string]any{
						"trend_guard": map[string]any{
							"enabled":       true,
							"mode":          "grouped",
							"groups_total":  1,
							"groups_active": 1,
							"groups": []map[string]any{
								{
									"group_id":                     "turtle|30m|short|1773943200000",
									"strategy":                     "turtle",
									"primary_timeframe":            "30m",
									"side":                         "short",
									"anchor_trending_timestamp_ms": float64(1773943200000),
									"state":                        "soft_locked",
									"selected_candidate_key":       "okx|RIVER/USDT",
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rr := httptest.NewRecorder()
	server.handleStatus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode status failed: %v", err)
	}
	modules, ok := resp["modules"].(map[string]any)
	if !ok {
		t.Fatalf("missing modules: %+v", resp)
	}
	riskModule, ok := modules["risk"].(map[string]any)
	if !ok {
		t.Fatalf("missing risk module: %+v", modules)
	}
	details, ok := riskModule["details"].(map[string]any)
	if !ok {
		t.Fatalf("missing risk details: %+v", riskModule)
	}
	trendGuard, ok := details["trend_guard"].(map[string]any)
	if !ok {
		t.Fatalf("missing trend_guard: %+v", details)
	}
	if _, ok := trendGuard["groups"]; ok {
		t.Fatalf("expected trend_guard.groups to be omitted, got %+v", trendGuard)
	}
	if got := trendGuard["groups_total"]; got == nil || got.(float64) != 1 {
		t.Fatalf("unexpected groups_total: %+v", trendGuard)
	}
	if got := trendGuard["groups_active"]; got == nil || got.(float64) != 1 {
		t.Fatalf("unexpected groups_active: %+v", trendGuard)
	}
}

func TestHandleOHLCVStatusReturnsCoverageAndBounds(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	insertTestExchange(t, store, models.Exchange{
		Name:         "okx",
		RateLimitMS:  100,
		OHLCVLimit:   100,
		VolumeFilter: 1,
		Timeframes:   `["3m","15m","1h","4h","1d"]`,
		Active:       true,
	})
	if err := store.UpsertSymbol(models.Symbol{
		Exchange: "okx",
		Symbol:   "AZTEC/USDT",
		Base:     "AZTEC",
		Quote:    "USDT",
		Type:     "swap",
		Active:   true,
	}); err != nil {
		t.Fatalf("upsert symbol failed: %v", err)
	}

	fourHourStart := time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC).UnixMilli()
	fourHourEnd := time.Date(2026, 3, 29, 8, 0, 0, 0, time.UTC).UnixMilli()
	oneDayStart := time.Date(2026, 2, 12, 0, 0, 0, 0, time.UTC).UnixMilli()
	oneDayEnd := time.Date(2026, 3, 29, 0, 0, 0, 0, time.UTC).UnixMilli()
	for _, item := range []models.MarketData{
		{Exchange: "okx", Symbol: "AZTEC/USDT", Timeframe: "4h", OHLCV: models.OHLCV{TS: fourHourStart, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100}, Closed: true},
		{Exchange: "okx", Symbol: "AZTEC/USDT", Timeframe: "4h", OHLCV: models.OHLCV{TS: fourHourEnd, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100}, Closed: true},
		{Exchange: "okx", Symbol: "AZTEC/USDT", Timeframe: "1d", OHLCV: models.OHLCV{TS: oneDayStart, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100}, Closed: true},
		{Exchange: "okx", Symbol: "AZTEC/USDT", Timeframe: "1d", OHLCV: models.OHLCV{TS: oneDayEnd, Open: 1, High: 2, Low: 0.5, Close: 1.5, Volume: 100}, Closed: true},
	} {
		if err := store.SaveOHLCV(item); err != nil {
			t.Fatalf("save ohlcv failed: %v", err)
		}
	}
	if err := store.UpsertOHLCVBound("okx", "AZTEC/USDT", fourHourStart); err != nil {
		t.Fatalf("upsert ohlcv bound failed: %v", err)
	}

	server := New(Config{
		SymbolProvider: stubSymbolProvider{
			exchanges: []models.Exchange{{
				Name:       "okx",
				Timeframes: `["3m","15m","1h","4h","1d"]`,
				Active:     true,
			}},
			symbols: []models.Symbol{{
				Exchange: "okx",
				Symbol:   "AZTEC/USDT",
				Active:   true,
			}},
		},
		HistoryStore: store,
	})

	req := httptest.NewRequest(http.MethodGet, "/ohlcv-status", nil)
	rr := httptest.NewRecorder()
	server.handleOHLCVStatus(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp ohlcvStatusResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode ohlcv status failed: %v", err)
	}
	if resp.TimeFormat != "YYYY-MM-DD HH:MM" {
		t.Fatalf("unexpected time_format: %+v", resp)
	}
	if resp.Count != 1 || len(resp.Items) != 1 {
		t.Fatalf("unexpected item count: %+v", resp)
	}
	item := resp.Items[0]
	if item.Exchange != "okx" || item.Symbol != "AZTEC/USDT" {
		t.Fatalf("unexpected item identity: %+v", item)
	}
	wantConfigured := []string{"3m", "15m", "1h", "4h", "1d"}
	if strings.Join(item.ConfiguredTimeframes, ",") != strings.Join(wantConfigured, ",") {
		t.Fatalf("unexpected configured_timeframes: %+v", item.ConfiguredTimeframes)
	}
	if len(item.AvailableTimeframes) != 2 {
		t.Fatalf("unexpected available_timeframes: %+v", item.AvailableTimeframes)
	}
	if item.AvailableTimeframes[0].Timeframe != "4h" || item.AvailableTimeframes[0].Bars != 2 {
		t.Fatalf("unexpected first timeframe range: %+v", item.AvailableTimeframes[0])
	}
	if item.AvailableTimeframes[0].StartTime != time.UnixMilli(fourHourStart).In(time.Local).Format("2006-01-02 15:04") {
		t.Fatalf("unexpected 4h start time: %+v", item.AvailableTimeframes[0])
	}
	if item.AvailableTimeframes[1].Timeframe != "1d" || item.AvailableTimeframes[1].Bars != 2 {
		t.Fatalf("unexpected second timeframe range: %+v", item.AvailableTimeframes[1])
	}
	if !item.OHLCVBounds.Exists || item.OHLCVBounds.EarliestAvailableTSMS != fourHourStart {
		t.Fatalf("unexpected ohlcv_bounds: %+v", item.OHLCVBounds)
	}
	if item.OHLCVBounds.EarliestAvailableTime != time.UnixMilli(fourHourStart).In(time.Local).Format("2006-01-02 15:04") {
		t.Fatalf("unexpected ohlcv_bounds time: %+v", item.OHLCVBounds)
	}
}

func TestBuildWSDiffDetectsHistoryGroupChanges(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	now := time.Now().UTC()
	group := upsertTestTrendGroup(t, store, models.RiskTrendGroup{
		Strategy:                  "turtle",
		PrimaryTimeframe:          "30m",
		Side:                      "short",
		AnchorTrendingTimestampMS: 1773943200000,
		State:                     "finished",
		LockStage:                 "hard",
		SelectedCandidateKey:      "okx|RIVER/USDT",
		EntryCount:                1,
		CreatedAtMS:               now.Add(-3 * time.Hour).UnixMilli(),
		UpdatedAtMS:               now.Add(-40 * time.Minute).UnixMilli(),
		FinishedAtMS:              now.Add(-40 * time.Minute).UnixMilli(),
	}, models.RiskTrendGroupCandidate{
		CandidateKey:   "okx|NEO/USDT",
		Exchange:       "okx",
		Symbol:         "NEO/USDT",
		CandidateState: "blocked",
		PriorityScore:  40.1,
		UpdatedAtMS:    now.Add(-40 * time.Minute).UnixMilli(),
	}, models.RiskTrendGroupCandidate{
		CandidateKey:    "okx|RIVER/USDT",
		Exchange:        "okx",
		Symbol:          "RIVER/USDT",
		CandidateState:  "selected",
		IsSelected:      true,
		PriorityScore:   41.2,
		HasOpenPosition: false,
		UpdatedAtMS:     now.Add(-40 * time.Minute).UnixMilli(),
	})

	server := New(Config{
		AccountProvider: stubAccountProvider{
			history: []models.Position{{
				Exchange:        "okx",
				Symbol:          "RIVER/USDT",
				Timeframe:       "30m",
				PositionSide:    "short",
				GroupID:         historyGroupPublicID(group),
				StrategyName:    "turtle",
				StrategyVersion: "v1",
				EntryTime:       now.Add(-90 * time.Minute).Format("2006-01-02 15:04:05"),
				ExitTime:        now.Add(-30 * time.Minute).Format("2006-01-02 15:04:05"),
				Status:          models.PositionStatusClosed,
			}},
		},
		HistoryStore: store,
	})

	sub := wsSubscription{
		Streams: []string{wsStreamHistory},
		HistoryFilter: HistoryFilter{
			Exchange: "okx",
			Range:    "24h",
		},
	}
	snapshotState, errs := server.buildWSSnapshotStateLenient("", sub)
	if len(errs) > 0 {
		t.Fatalf("unexpected snapshot errors: %v", errs)
	}
	if snapshotState.snapshot.History == nil || len(snapshotState.snapshot.History.Groups) != 1 {
		t.Fatalf("expected initial history groups, got %+v", snapshotState.snapshot.History)
	}

	updatedCandidate := models.RiskTrendGroupCandidate{
		Mode:           "live",
		GroupID:        group.ID,
		CandidateKey:   "okx|NEO/USDT",
		Exchange:       "okx",
		Symbol:         "NEO/USDT",
		CandidateState: "selected",
		IsSelected:     true,
		PriorityScore:  42.5,
		UpdatedAtMS:    now.Add(-10 * time.Minute).UnixMilli(),
	}
	if err := store.UpsertRiskTrendGroupCandidate(&updatedCandidate); err != nil {
		t.Fatalf("update candidate failed: %v", err)
	}
	blockedLeader := models.RiskTrendGroupCandidate{
		Mode:           "live",
		GroupID:        group.ID,
		CandidateKey:   "okx|RIVER/USDT",
		Exchange:       "okx",
		Symbol:         "RIVER/USDT",
		CandidateState: "blocked",
		PriorityScore:  41.2,
		UpdatedAtMS:    now.Add(-10 * time.Minute).UnixMilli(),
	}
	if err := store.UpsertRiskTrendGroupCandidate(&blockedLeader); err != nil {
		t.Fatalf("update previous leader failed: %v", err)
	}
	group.SelectedCandidateKey = "okx|NEO/USDT"
	group.UpdatedAtMS = now.Add(-10 * time.Minute).UnixMilli()
	if err := store.UpsertRiskTrendGroup(&group); err != nil {
		t.Fatalf("update group failed: %v", err)
	}

	diff, _, _, _, _, _, nextHistory, _, _, _, _, changed, diffErrors := server.buildWSDiff(
		sub,
		nil,
		groupsResponse{},
		accountResponse{},
		false,
		nil,
		snapshotState.history,
		wsSymbolsResponse{},
		false,
		tradingViewCandlesResponse{},
		false,
	)
	if len(diffErrors) > 0 {
		t.Fatalf("unexpected diff errors: %v", diffErrors)
	}
	if !changed {
		t.Fatalf("expected history diff changed when only history groups change")
	}
	if diff.History == nil || len(diff.History.Groups) != 1 {
		t.Fatalf("expected history diff payload with groups, got %+v", diff.History)
	}
	if diff.History.Groups[0].SelectedCandidateKey != "okx|NEO/USDT" {
		t.Fatalf("unexpected updated selected candidate key: %+v", diff.History.Groups[0])
	}
	if nextHistory.Groups[0].SelectedCandidateKey != "okx|NEO/USDT" {
		t.Fatalf("unexpected next history baseline: %+v", nextHistory.Groups[0])
	}
}

func TestHandleGroupsAlwaysReturnsGroupsArray(t *testing.T) {
	server := New(Config{
		StatusProviders: []iface.StatusProvider{
			stubStatusProvider{
				status: iface.ModuleStatus{
					Name: "risk",
					Details: map[string]any{
						"trend_guard": map[string]any{
							"enabled":       true,
							"mode":          "grouped",
							"groups_total":  1,
							"groups_active": 0,
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/groups", nil)
	rr := httptest.NewRecorder()
	server.handleGroups(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode groups response failed: %v", err)
	}
	raw, ok := payload["groups"]
	if !ok {
		t.Fatalf("expected groups key in payload: %s", rr.Body.String())
	}
	items, ok := raw.([]any)
	if !ok {
		t.Fatalf("expected groups array, got %T", raw)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty groups array, got %d items", len(items))
	}
}

func TestHandleSignalsSupportsExtendedFilters(t *testing.T) {
	signalA := testSignal("okx", "NEO/USDT")
	signalA.StrategyVersion = "v1"
	signalB := testSignal("okx", "RIVER/USDT")
	signalB.ComboKey = "5m/15m/1h"
	signalB.StrategyTimeframes = []string{"5m", "15m", "1h"}
	signalB.StrategyVersion = "v2"

	server := New(Config{
		Provider: stubSignalProvider{
			signals: map[string]map[string]models.Signal{
				"okx|NEO/USDT": {
					"turtle|1m/5m/30m": signalA,
				},
				"okx|RIVER/USDT": {
					"turtle|5m/15m/1h": signalB,
				},
			},
		},
		AccountProvider: stubAccountProvider{
			grouped: map[string]models.SignalGroupedInfo{
				flatKey(signalA): testGroupedInfo("okx|NEO/USDT", "selected", "okx|NEO/USDT", nil),
				flatKey(signalB): testGroupedInfo("okx|RIVER/USDT", "blocked", "okx|NEO/USDT", nil),
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/signals?combo_key=1m/5m/30m&group_id=turtle|30m|short|1773943200000&strategy_version=v1", nil)
	rr := httptest.NewRecorder()
	server.handleSignals(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]map[string]models.Signal
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if len(resp) != 1 {
		t.Fatalf("expected single outer bucket, got %d", len(resp))
	}
	got := resp["okx|NEO/USDT"]["turtle|1m/5m/30m"]
	if got.Symbol != "NEO/USDT" || got.StrategyVersion != "v1" {
		t.Fatalf("unexpected filtered signal: %+v", got)
	}
}

func TestHandleGroupsSupportsSymbolFilter(t *testing.T) {
	server := New(Config{
		StatusProviders: []iface.StatusProvider{
			stubStatusProvider{
				status: iface.ModuleStatus{
					Name: "risk",
					Details: map[string]any{
						"trend_guard": map[string]any{
							"enabled":       true,
							"mode":          "grouped",
							"groups_total":  2,
							"groups_active": 2,
							"groups": []map[string]any{
								{
									"group_id":                     "turtle|30m|short|1773943200000",
									"strategy":                     "turtle",
									"primary_timeframe":            "30m",
									"side":                         "short",
									"anchor_trending_timestamp_ms": float64(1773943200000),
									"state":                        "soft_locked",
									"selected_candidate_key":       "okx|RIVER/USDT",
									"entry_count":                  0,
									"candidates": []map[string]any{
										{"candidate_key": "okx|RIVER/USDT", "candidate_state": "selected", "is_selected": true, "priority_score": 1.0, "has_open_position": false},
									},
								},
								{
									"group_id":                     "turtle|1h|long|1773946800000",
									"strategy":                     "turtle",
									"primary_timeframe":            "1h",
									"side":                         "long",
									"anchor_trending_timestamp_ms": float64(1773946800000),
									"state":                        "tracking",
									"selected_candidate_key":       "okx|BTC/USDT",
									"entry_count":                  0,
									"candidates": []map[string]any{
										{"candidate_key": "okx|BTC/USDT", "candidate_state": "selected", "is_selected": true, "priority_score": 2.0, "has_open_position": false},
									},
								},
							},
						},
					},
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/groups?symbol=BTC/USDT", nil)
	rr := httptest.NewRecorder()
	server.handleGroups(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp groupsResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode groups response failed: %v", err)
	}
	if resp.GroupsTotal != 1 || resp.GroupsActive != 1 || len(resp.Groups) != 1 {
		t.Fatalf("unexpected filtered groups response: %+v", resp)
	}
	if resp.Groups[0].GroupID != "turtle|1h|long|1773946800000" {
		t.Fatalf("unexpected filtered group: %+v", resp.Groups[0])
	}
}

func TestHandlePositionsWithoutFiltersReturnsAllAndSupportsExtendedFilters(t *testing.T) {
	server := New(Config{
		AccountProvider: stubAccountProvider{
			positions: []models.Position{
				{
					Exchange:           "okx",
					Symbol:             "BTC/USDT",
					Timeframe:          "1h",
					PositionSide:       "long",
					GroupID:            "turtle|1h|long|1",
					StrategyName:       "turtle",
					StrategyVersion:    "v1",
					Status:             models.PositionStatusOpen,
					UpdatedTime:        "2026-03-20 10:00:00",
					MarginMode:         "isolated",
					LeverageMultiplier: 3,
				},
				{
					Exchange:           "okx",
					Symbol:             "ETH/USDT",
					Timeframe:          "15m",
					PositionSide:       "short",
					GroupID:            "turtle|15m|short|2",
					StrategyName:       "turtle",
					StrategyVersion:    "v2",
					Status:             models.PositionStatusOpen,
					UpdatedTime:        "2026-03-20 10:01:00",
					MarginMode:         "isolated",
					LeverageMultiplier: 2,
				},
			},
		},
	})

	allReq := httptest.NewRequest(http.MethodGet, "/positions", nil)
	allRR := httptest.NewRecorder()
	server.handlePosition(allRR, allReq)
	if allRR.Code != http.StatusOK {
		t.Fatalf("unexpected all status code: %d body=%s", allRR.Code, allRR.Body.String())
	}
	var allResp positionResponse
	if err := json.Unmarshal(allRR.Body.Bytes(), &allResp); err != nil {
		t.Fatalf("decode all response failed: %v", err)
	}
	if allResp.Count != 2 || len(allResp.Positions) != 2 {
		t.Fatalf("expected all positions, got %+v", allResp)
	}

	filterReq := httptest.NewRequest(http.MethodGet, "/positions?position_side=short&strategy_name=turtle&strategy_version=v2", nil)
	filterRR := httptest.NewRecorder()
	server.handlePosition(filterRR, filterReq)
	if filterRR.Code != http.StatusOK {
		t.Fatalf("unexpected filter status code: %d body=%s", filterRR.Code, filterRR.Body.String())
	}
	var filterResp positionResponse
	if err := json.Unmarshal(filterRR.Body.Bytes(), &filterResp); err != nil {
		t.Fatalf("decode filter response failed: %v", err)
	}
	if filterResp.Count != 1 || len(filterResp.Positions) != 1 {
		t.Fatalf("expected single filtered position, got %+v", filterResp)
	}
	if filterResp.Positions[0].GroupID != "turtle|15m|short|2" {
		t.Fatalf("unexpected filtered position group_id: %s", filterResp.Positions[0].GroupID)
	}
}

func TestHandlePositionCompatibilityAliasStillWorks(t *testing.T) {
	server := New(Config{
		AccountProvider: stubAccountProvider{
			positions: []models.Position{
				{
					Exchange:        "okx",
					Symbol:          "BTC/USDT",
					Timeframe:       "1h",
					PositionSide:    "long",
					StrategyName:    "turtle",
					StrategyVersion: "v1",
					Status:          models.PositionStatusOpen,
				},
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/position", nil)
	rr := httptest.NewRecorder()
	server.handlePosition(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d body=%s", rr.Code, rr.Body.String())
	}
	var resp positionResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	if resp.Count != 1 || len(resp.Positions) != 1 {
		t.Fatalf("expected compatibility alias to return one position, got %+v", resp)
	}
}

func TestHandleHistoryDefaultsToTodayAndSupportsRangeLimitAndAbsoluteTime(t *testing.T) {
	now := time.Now().In(time.Local)
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	elapsedToday := now.Sub(startOfDay)
	todayExit := startOfDay.Add(elapsedToday / 3)
	midExit := startOfDay.Add((2 * elapsedToday) / 3)
	oldExit := startOfDay.Add(-2 * time.Hour)

	server := New(Config{
		AccountProvider: stubAccountProvider{
			history: []models.Position{
				{
					Exchange:        "okx",
					Symbol:          "BTC/USDT",
					Timeframe:       "1h",
					PositionSide:    "long",
					GroupID:         "turtle|1h|long|1",
					StrategyName:    "turtle",
					StrategyVersion: "v1",
					EntryTime:       todayExit.Add(-time.Hour).Format("2006-01-02 15:04:05"),
					ExitTime:        todayExit.Format("2006-01-02 15:04:05"),
					Status:          models.PositionStatusClosed,
					UpdatedTime:     todayExit.Format("2006-01-02 15:04:05"),
				},
				{
					Exchange:        "okx",
					Symbol:          "ETH/USDT",
					Timeframe:       "15m",
					PositionSide:    "short",
					GroupID:         "turtle|15m|short|2",
					StrategyName:    "turtle",
					StrategyVersion: "v2",
					EntryTime:       midExit.Add(-30 * time.Minute).Format("2006-01-02 15:04:05"),
					ExitTime:        midExit.Format("2006-01-02 15:04:05"),
					Status:          models.PositionStatusClosed,
					UpdatedTime:     midExit.Format("2006-01-02 15:04:05"),
				},
				{
					Exchange:        "okx",
					Symbol:          "SOL/USDT",
					Timeframe:       "4h",
					PositionSide:    "long",
					GroupID:         "turtle|4h|long|3",
					StrategyName:    "turtle",
					StrategyVersion: "v1",
					EntryTime:       oldExit.Add(-time.Hour).Format("2006-01-02 15:04:05"),
					ExitTime:        oldExit.Format("2006-01-02 15:04:05"),
					Status:          models.PositionStatusClosed,
					UpdatedTime:     oldExit.Format("2006-01-02 15:04:05"),
				},
			},
		},
		SymbolProvider: stubSymbolProvider{exchanges: []models.Exchange{{Name: "okx", Active: true}}},
	})

	defaultReq := httptest.NewRequest(http.MethodGet, "/history", nil)
	defaultRR := httptest.NewRecorder()
	server.handleHistory(defaultRR, defaultReq)
	if defaultRR.Code != http.StatusOK {
		t.Fatalf("unexpected default status code: %d body=%s", defaultRR.Code, defaultRR.Body.String())
	}
	var defaultResp historyResponse
	if err := json.Unmarshal(defaultRR.Body.Bytes(), &defaultResp); err != nil {
		t.Fatalf("decode default history failed: %v", err)
	}
	if defaultResp.Count != 2 || len(defaultResp.Positions) != 2 {
		t.Fatalf("expected today history only, got %+v", defaultResp)
	}
	if defaultResp.Groups == nil || len(defaultResp.Groups) != 0 {
		t.Fatalf("expected empty history groups, got %+v", defaultResp.Groups)
	}

	rangeReq := httptest.NewRequest(http.MethodGet, "/history?range=24h&limit=1", nil)
	rangeRR := httptest.NewRecorder()
	server.handleHistory(rangeRR, rangeReq)
	if rangeRR.Code != http.StatusOK {
		t.Fatalf("unexpected range status code: %d body=%s", rangeRR.Code, rangeRR.Body.String())
	}
	var rangeResp historyResponse
	if err := json.Unmarshal(rangeRR.Body.Bytes(), &rangeResp); err != nil {
		t.Fatalf("decode range history failed: %v", err)
	}
	if rangeResp.Count != 1 || len(rangeResp.Positions) != 1 {
		t.Fatalf("expected limited history response, got %+v", rangeResp)
	}
	if rangeResp.Positions[0].Symbol != "ETH/USDT" {
		t.Fatalf("expected most recent history item first, got %+v", rangeResp.Positions[0])
	}

	start := todayExit.Add(-time.Minute).Truncate(time.Minute).Format("2006/01/02 15:04")
	end := todayExit.Add(2 * time.Minute).Truncate(time.Minute).Format("2006/01/02 15:04")
	absReq := httptest.NewRequest(http.MethodGet, "/history?start_time="+url.QueryEscape(start)+"&end_time="+url.QueryEscape(end)+"&strategy_name=turtle", nil)
	absRR := httptest.NewRecorder()
	server.handleHistory(absRR, absReq)
	if absRR.Code != http.StatusOK {
		t.Fatalf("unexpected absolute status code: %d body=%s", absRR.Code, absRR.Body.String())
	}
	var absResp historyResponse
	if err := json.Unmarshal(absRR.Body.Bytes(), &absResp); err != nil {
		t.Fatalf("decode absolute history failed: %v", err)
	}
	if absResp.Count != 1 || len(absResp.Positions) != 1 || absResp.Positions[0].Symbol != "BTC/USDT" {
		t.Fatalf("unexpected absolute filtered history: %+v", absResp)
	}
}

func TestEnrichTradingViewBacktestTaskSummary(t *testing.T) {
	task := models.BacktestTask{
		ID:            7,
		Exchange:      "okx",
		Symbol:        "BTC/USDT",
		DisplaySymbol: "BTC/USDT.P",
	}
	positions := []visualHistoryPositionRow{
		{
			PosSide:     "long",
			Lever:       "7",
			AvgPx:       "68000.5",
			OpenTimeMS:  1775606400000,
			CloseAvgPx:  "68220.5",
			CloseTimeMS: 1775610000000,
			PnlRatio:    "0.031",
		},
		{
			Lever:       "7",
			AvgPx:       "68220.5",
			OpenTimeMS:  1775610600000,
			CloseAvgPx:  "68600.25",
			CloseTimeMS: 1775613600000,
			PnlRatio:    "0.014",
		},
	}

	got := enrichTradingViewBacktestTaskSummary(task, positions)
	if got.OpenPrice != 68000.5 {
		t.Fatalf("expected open price 68000.5, got %v", got.OpenPrice)
	}
	if got.PositionSide != "long" {
		t.Fatalf("expected position side long, got %q", got.PositionSide)
	}
	if got.LeverageMultiplier != 7 {
		t.Fatalf("expected leverage 7, got %v", got.LeverageMultiplier)
	}
	if got.ClosePrice != 68600.25 {
		t.Fatalf("expected close price 68600.25, got %v", got.ClosePrice)
	}
	if got.OpenTimeMS != positions[0].OpenTimeMS {
		t.Fatalf("expected open time %d, got %d", positions[0].OpenTimeMS, got.OpenTimeMS)
	}
	if got.CloseTimeMS != positions[1].CloseTimeMS {
		t.Fatalf("expected close time %d, got %d", positions[1].CloseTimeMS, got.CloseTimeMS)
	}
	if got.HoldingDurationMS != positions[1].CloseTimeMS-positions[0].OpenTimeMS {
		t.Fatalf("unexpected holding duration: %d", got.HoldingDurationMS)
	}
	if diff := got.RealizedProfitRate - 0.045; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("expected realized profit rate 0.045, got %v", got.RealizedProfitRate)
	}
}

func TestEnrichTradingViewBacktestTaskSummaryFromRuntimeOpenPosition(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() { _ = store.Close() }()

	task, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "XAU/USDT",
		DisplaySymbol:   "XAU/USDT.P",
		ChartTimeframe:  "15m",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    1775571300000,
		RangeEndMS:      1775631600000,
		Source:          "db:okx:XAU/USDT:3m/15m/1h:20260407_1415Z-20260408_0712Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create backtest task failed: %v", err)
	}
	if err := store.MarkBacktestTaskRunning(task.ID, 61, "13d00ffd-e72b-4e1e-a4c7-c3c5bf04b79c"); err != nil {
		t.Fatalf("mark backtest task running failed: %v", err)
	}
	task, found, err := store.GetBacktestTask(task.ID)
	if err != nil || !found {
		t.Fatalf("reload backtest task failed: found=%v err=%v", found, err)
	}

	openPayload, err := json.Marshal(map[string]any{
		"EventTS":      int64(1775626020000),
		"Action":       models.DecisionActionOpenLong,
		"PositionSide": "long",
	})
	if err != nil {
		t.Fatalf("marshal open payload failed: %v", err)
	}
	updatePayload, err := json.Marshal(map[string]any{
		"EventTS":      int64(1775631600000),
		"Action":       models.DecisionActionUpdate,
		"PositionSide": "long",
	})
	if err != nil {
		t.Fatalf("marshal update payload failed: %v", err)
	}

	if _, err := store.DB.Exec(
		`INSERT INTO orders (
		     attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
		     position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
		     stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
		     fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
		     has_side_effect, step_results_json, request_json, response_json, started_at_ms,
		     finished_at_ms, duration_ms, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, 'back-test', '', 'okx', 'XAU/USDT', 'XAU-USDT-SWAP', ?, '', ?, 'isolated', 1, 10, ?, 0, 0, '', 'turtle', 'success', '', '', '', '', '', '', '', 1, '[]', ?, '{}', ?, ?, 1, ?, ?);`,
		"attempt-open",
		task.SingletonUUID,
		models.DecisionActionOpenLong,
		"long",
		4798.6,
		string(openPayload),
		int64(1775658302390),
		int64(1775658302391),
		int64(1775658302390),
		int64(1775658302391),
	); err != nil {
		t.Fatalf("insert open order failed: %v", err)
	}
	if _, err := store.DB.Exec(
		`INSERT INTO orders (
		     attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
		     position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
		     stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
		     fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
		     has_side_effect, step_results_json, request_json, response_json, started_at_ms,
		     finished_at_ms, duration_ms, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, 'back-test', '', 'okx', 'XAU/USDT', 'XAU-USDT-SWAP', ?, '', ?, 'isolated', 1, 10, ?, 0, 0, '', 'turtle', 'success', '', '', '', '', '', '', '', 1, '[]', ?, '{}', ?, ?, 1, ?, ?);`,
		"attempt-update",
		task.SingletonUUID,
		models.DecisionActionUpdate,
		"long",
		4804.0,
		string(updatePayload),
		int64(1775658302415),
		int64(1775658302416),
		int64(1775658302415),
		int64(1775658302416),
	); err != nil {
		t.Fatalf("insert update order failed: %v", err)
	}

	if _, err := store.DB.Exec(
		`INSERT INTO positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
		     upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, update_time_ms,
		     row_json, max_floating_loss_amount, max_floating_profit_amount, created_at_ms, updated_at_ms
		 )
		 VALUES (?, 'back-test', 'okx', 'XAU/USDT', 'XAU-USDT-SWAP', '1', 'long', 'isolated', '0', '10', ?, '0', ?, '0', '0', '0', '0', '0', ?, ?, '{}', 0, 0, ?, ?);`,
		task.SingletonID,
		4798.6,
		0.0021256199724917495,
		int64(1775626020000),
		int64(1775631600000),
		int64(1775658302415),
		int64(1775658302415),
	); err != nil {
		t.Fatalf("insert backtest open position failed: %v", err)
	}

	server := &Server{cfg: Config{HistoryStore: store}}
	got, err := server.enrichTradingViewBacktestTask(task)
	if err != nil {
		t.Fatalf("enrich tradingview backtest task failed: %v", err)
	}
	if got.OpenPrice != 4798.6 {
		t.Fatalf("expected open price 4798.6, got %v", got.OpenPrice)
	}
	if got.PositionSide != "long" {
		t.Fatalf("expected position side long, got %q", got.PositionSide)
	}
	if got.LeverageMultiplier != 10 {
		t.Fatalf("expected leverage 10, got %v", got.LeverageMultiplier)
	}
	if got.OpenTimeMS != 1775626020000 {
		t.Fatalf("expected open time 1775626020000, got %d", got.OpenTimeMS)
	}
	if got.ClosePrice != 0 || got.CloseTimeMS != 0 {
		t.Fatalf("expected open position to keep close fields empty, got close_price=%v close_time=%d", got.ClosePrice, got.CloseTimeMS)
	}
	if got.HoldingDurationMS != 1775631600000-1775626020000 {
		t.Fatalf("unexpected holding duration: %d", got.HoldingDurationMS)
	}
	if got.RealizedProfitRate != 0 {
		t.Fatalf("expected realized profit rate to remain 0 for open position, got %v", got.RealizedProfitRate)
	}
}

func TestEnrichTradingViewBacktestTaskSummaryFromRuntimeClosedOrders(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() { _ = store.Close() }()

	task, err := store.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:        "okx",
		Symbol:          "BTC/USDT",
		DisplaySymbol:   "BTC/USDT.P",
		ChartTimeframe:  "1h",
		TradeTimeframes: []string{"3m", "15m", "1h"},
		RangeStartMS:    1775577600000,
		RangeEndMS:      1775653020000,
		Source:          "db:okx:BTC/USDT:3m/15m/1h:20260407_1600Z-20260408_1257Z",
		HistoryBars:     500,
	})
	if err != nil {
		t.Fatalf("create backtest task failed: %v", err)
	}
	if err := store.MarkBacktestTaskRunning(task.ID, 56, "ef95cc3e-d32d-451d-a2e4-46434ed00ffd"); err != nil {
		t.Fatalf("mark backtest task running failed: %v", err)
	}
	task, found, err := store.GetBacktestTask(task.ID)
	if err != nil || !found {
		t.Fatalf("reload backtest task failed: found=%v err=%v", found, err)
	}

	openPayload, err := json.Marshal(map[string]any{
		"EventTS":      int64(1775623200000),
		"Action":       models.DecisionActionOpenLong,
		"PositionSide": "long",
	})
	if err != nil {
		t.Fatalf("marshal open payload failed: %v", err)
	}
	closePayload, err := json.Marshal(map[string]any{
		"EventTS":      int64(1775630400000),
		"Action":       models.DecisionActionClose,
		"PositionSide": "long",
	})
	if err != nil {
		t.Fatalf("marshal close payload failed: %v", err)
	}

	if _, err := store.DB.Exec(
		`INSERT INTO orders (
		     attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
		     position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
		     stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
		     fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
		     has_side_effect, step_results_json, request_json, response_json, started_at_ms,
		     finished_at_ms, duration_ms, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, 'back-test', '', 'okx', 'BTC/USDT', 'BTC-USDT-SWAP', ?, '', ?, 'isolated', 1, 10, ?, 0, 0, '', 'turtle', 'success', '', '', '', '', '', '', '', 1, '[]', ?, '{}', ?, ?, 1, ?, ?);`,
		"attempt-open-closed",
		task.SingletonUUID,
		models.DecisionActionOpenLong,
		"long",
		68000.0,
		string(openPayload),
		int64(1775658303000),
		int64(1775658303001),
		int64(1775658303000),
		int64(1775658303001),
	); err != nil {
		t.Fatalf("insert open order failed: %v", err)
	}
	if _, err := store.DB.Exec(
		`INSERT INTO orders (
		     attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
		     position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
		     stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
		     fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
		     has_side_effect, step_results_json, request_json, response_json, started_at_ms,
		     finished_at_ms, duration_ms, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, 'back-test', '', 'okx', 'BTC/USDT', 'BTC-USDT-SWAP', ?, '', ?, 'isolated', 1, 10, ?, 0, 0, '', 'turtle', 'success', '', '', '', '', '', '', '', 1, '[]', ?, '{}', ?, ?, 1, ?, ?);`,
		"attempt-close-closed",
		task.SingletonUUID,
		models.DecisionActionClose,
		"long",
		68680.0,
		string(closePayload),
		int64(1775658303999),
		int64(1775658304000),
		int64(1775658303999),
		int64(1775658304000),
	); err != nil {
		t.Fatalf("insert close order failed: %v", err)
	}

	server := &Server{cfg: Config{HistoryStore: store}}
	got, err := server.enrichTradingViewBacktestTask(task)
	if err != nil {
		t.Fatalf("enrich tradingview backtest task failed: %v", err)
	}
	if got.OpenPrice != 68000.0 {
		t.Fatalf("expected open price 68000, got %v", got.OpenPrice)
	}
	if got.PositionSide != "long" {
		t.Fatalf("expected position side long, got %q", got.PositionSide)
	}
	if got.LeverageMultiplier != 10 {
		t.Fatalf("expected leverage 10, got %v", got.LeverageMultiplier)
	}
	if got.ClosePrice != 68680.0 {
		t.Fatalf("expected close price 68680, got %v", got.ClosePrice)
	}
	if got.OpenTimeMS != 1775623200000 {
		t.Fatalf("expected open time 1775623200000, got %d", got.OpenTimeMS)
	}
	if got.CloseTimeMS != 1775630400000 {
		t.Fatalf("expected close time 1775630400000, got %d", got.CloseTimeMS)
	}
	if got.HoldingDurationMS != 1775630400000-1775623200000 {
		t.Fatalf("unexpected holding duration: %d", got.HoldingDurationMS)
	}
	if diff := got.RealizedProfitRate - 0.01; diff < -1e-9 || diff > 1e-9 {
		t.Fatalf("expected realized profit rate 0.01, got %v", got.RealizedProfitRate)
	}
}
