package core

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

type liveMockStrategy struct {
	getFn    func(snapshot models.MarketSnapshot) []models.Signal
	updateFn func(strategy string, signal models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool)
}

func (m *liveMockStrategy) Start(context.Context) error { return nil }
func (m *liveMockStrategy) Close() error                { return nil }
func (m *liveMockStrategy) Name() string                { return "test-strategy" }
func (m *liveMockStrategy) Version() string             { return "v0.0.1" }
func (m *liveMockStrategy) Get(snapshot models.MarketSnapshot) []models.Signal {
	if m.getFn == nil {
		return nil
	}
	return m.getFn(snapshot)
}
func (m *liveMockStrategy) Update(strategy string, signal models.Signal, snapshot models.MarketSnapshot) (models.Signal, bool) {
	if m.updateFn == nil {
		return models.Signal{}, false
	}
	return m.updateFn(strategy, signal, snapshot)
}

type liveMockRisk struct {
	signalsByPair   map[string][]models.Signal
	positions       []models.Position
	marketDataCalls int
	marketDataTFs   []string
	openCalls       int
	lastOpenCtx     models.RiskEvalContext
	updateCalls     int
	lastUpdateCtx   models.RiskEvalContext
	lastUpdate      models.Signal
	refreshCalls    int
	lastRefresh     models.Signal
}

func (m *liveMockRisk) Start(context.Context) error { return nil }
func (m *liveMockRisk) Close() error                { return nil }
func (m *liveMockRisk) OnMarketData(data models.MarketData) error {
	m.marketDataCalls++
	m.marketDataTFs = append(m.marketDataTFs, data.Timeframe)
	return nil
}
func (m *liveMockRisk) EvaluateOpenBatch(signals []models.Signal, accountState any) (models.Decision, error) {
	m.openCalls++
	if ctx, ok := accountState.(models.RiskEvalContext); ok {
		m.lastOpenCtx = ctx
	}
	if len(signals) == 0 {
		return models.Decision{Action: models.DecisionActionIgnore}, nil
	}
	return models.Decision{
		Action:   models.DecisionActionOpenLong,
		Exchange: signals[0].Exchange,
		Symbol:   signals[0].Symbol,
	}, nil
}
func (m *liveMockRisk) EvaluateUpdate(signal models.Signal, _ models.Position, accountState any) (models.Decision, error) {
	m.updateCalls++
	m.lastUpdate = signal
	if ctx, ok := accountState.(models.RiskEvalContext); ok {
		m.lastUpdateCtx = ctx
	}
	return models.Decision{Action: models.DecisionActionIgnore}, nil
}
func (m *liveMockRisk) ListOpenPositions(exchange, symbol, _ string) ([]models.Position, error) {
	out := make([]models.Position, 0, len(m.positions))
	for _, pos := range m.positions {
		if exchange != "" && pos.Exchange != exchange {
			continue
		}
		if symbol != "" && pos.Symbol != symbol {
			continue
		}
		out = append(out, pos)
	}
	return out, nil
}
func (m *liveMockRisk) ListSignalsByPair(exchange, symbol string) ([]models.Signal, error) {
	if m.signalsByPair == nil {
		return nil, nil
	}
	return m.signalsByPair[exchange+"|"+symbol], nil
}
func (m *liveMockRisk) GetAccountFunds(string) (models.RiskAccountFunds, error) {
	return models.RiskAccountFunds{}, nil
}
func (m *liveMockRisk) RefreshTrendGuardCandidate(signal models.Signal, _ any) error {
	m.refreshCalls++
	m.lastRefresh = signal
	return nil
}

type liveMockOHLCVStore struct {
	saved     []models.MarketData
	symbols   []models.Symbol
	exchanges []models.Exchange
	bounds    map[string]int64
}

type liveMockHistoryRequester struct {
	blockByExchange map[string]chan struct{}
	listTimes       map[string]int64
	rangeErr        error
	byLimitCalls    int
}

func (m *liveMockOHLCVStore) SaveOHLCV(data models.MarketData) error {
	m.saved = append(m.saved, data)
	return nil
}

func (m *liveMockOHLCVStore) HasOHLCV(exchange, symbol, timeframe string, ts int64) (bool, error) {
	_ = exchange
	_ = symbol
	_ = timeframe
	_ = ts
	return false, nil
}

func (m *liveMockOHLCVStore) DeleteOHLCVBeforeOrEqual(exchange, symbol, timeframe string, ts int64) (int64, error) {
	_ = exchange
	_ = symbol
	_ = timeframe
	_ = ts
	return 0, nil
}

func (m *liveMockOHLCVStore) GetOHLCVBound(exchange, symbol string) (int64, bool, error) {
	if m == nil || m.bounds == nil {
		return 0, false, nil
	}
	ts, ok := m.bounds[pairKey(exchange, symbol)]
	return ts, ok, nil
}

func (m *liveMockOHLCVStore) UpsertOHLCVBound(exchange, symbol string, earliestAvailableTS int64) error {
	if m.bounds == nil {
		m.bounds = make(map[string]int64)
	}
	key := pairKey(exchange, symbol)
	if current, ok := m.bounds[key]; !ok || current <= 0 || earliestAvailableTS < current {
		m.bounds[key] = earliestAvailableTS
	}
	return nil
}

func (m *liveMockOHLCVStore) ListSymbols() ([]models.Symbol, error) {
	return append([]models.Symbol(nil), m.symbols...), nil
}

func (m *liveMockOHLCVStore) ListExchanges() ([]models.Exchange, error) {
	return append([]models.Exchange(nil), m.exchanges...), nil
}

func (m *liveMockOHLCVStore) ListRecentOHLCV(exchange, symbol, timeframe string, limit int) ([]models.OHLCV, error) {
	_ = exchange
	_ = symbol
	_ = timeframe
	_ = limit
	return nil, nil
}

func (m *liveMockOHLCVStore) ListOHLCVRange(exchange, symbol, timeframe string, start, end time.Time) ([]models.OHLCV, error) {
	_ = exchange
	_ = symbol
	_ = timeframe
	_ = start
	_ = end
	return nil, nil
}

func (m *liveMockHistoryRequester) FetchOHLCVRangePaged(ctx context.Context, exchange, symbol, timeframe string, start, end time.Time, maxPerRequest int) ([]models.MarketData, error) {
	_ = exchange
	_ = symbol
	_ = timeframe
	_ = start
	_ = end
	_ = maxPerRequest
	if m.rangeErr != nil {
		return nil, m.rangeErr
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return nil, nil
}

func (m *liveMockHistoryRequester) FetchSymbolListTime(_ context.Context, exchange, symbol string) (int64, error) {
	if m == nil || m.listTimes == nil {
		return 0, nil
	}
	return m.listTimes[pairKey(exchange, symbol)], nil
}

func (m *liveMockHistoryRequester) FetchOHLCVByLimitPaged(ctx context.Context, exchange, symbol, timeframe string, limit, maxPerRequest int) ([]models.MarketData, error) {
	_ = symbol
	_ = timeframe
	_ = limit
	_ = maxPerRequest
	m.byLimitCalls++
	if ch, ok := m.blockByExchange[exchange]; ok && ch != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
	return nil, nil
}

func TestLiveOnOHLCV_NoPositionUsesGet(t *testing.T) {
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return []models.Signal{{
				Exchange:  "okx",
				Symbol:    "BTC/USDT",
				Timeframe: "1h",
				Strategy:  "s1",
				Action:    8,
				HighSide:  1,
				Entry:     100,
				SL:        95,
			}}
		},
	}
	risk := &liveMockRisk{}
	live := New(Config{Strategy: strategy, Risk: risk})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.openCalls != 1 {
		t.Fatalf("expected EvaluateOpenBatch called once, got %d", risk.openCalls)
	}
	if getCalls != 1 {
		t.Fatalf("expected Get called once, got %d", getCalls)
	}
}

func TestLiveOnOHLCV_StrategyCombosRouteSnapshotsByConfiguredCombo(t *testing.T) {
	var got [][]string
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			keys := make([]string, 0, len(snapshot.Series))
			for timeframe := range snapshot.Series {
				keys = append(keys, timeframe)
			}
			sort.Strings(keys)
			got = append(got, keys)
			return nil
		},
	}
	risk := &liveMockRisk{}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{{
			Name:       "okx",
			Active:     true,
			Timeframes: `["3m","15m","1h","4h","1d"]`,
		}},
		symbols: []models.Symbol{{
			Exchange:   "okx",
			Symbol:     "BTC/USDT",
			Timeframes: "",
			Active:     true,
		}},
	}
	live := New(Config{
		Strategy:       strategy,
		StrategyCombos: []models.StrategyComboConfig{{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true}, {Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false}},
		Risk:           risk,
		OHLCVStore:     store,
	})

	base := int64(1700000000000)
	preload := []models.MarketData{
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "15m", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 101, Low: 99, Close: 100}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1h", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 102, Low: 99, Close: 101}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "4h", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 103, Low: 98, Close: 102}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1d", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 104, Low: 97, Close: 103}},
	}
	for _, item := range preload {
		live.OnOHLCV(item)
	}
	if len(got) != 0 {
		t.Fatalf("expected preload events not to trigger complete combo evaluation, got %v", got)
	}

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(3*time.Minute/time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100},
	})
	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: base + int64(1*time.Hour/time.Millisecond), Open: 101, High: 103, Low: 100, Close: 102},
	})

	if len(got) != 2 {
		t.Fatalf("expected two combo evaluations, got %d (%v)", len(got), got)
	}
	if strings.Join(got[0], "/") != "15m/1h/3m" {
		t.Fatalf("unexpected first combo snapshot: got %v", got[0])
	}
	if strings.Join(got[1], "/") != "1d/1h/4h" {
		t.Fatalf("unexpected second combo snapshot: got %v", got[1])
	}
}

func TestLiveOnOHLCV_StrategyCombosAllowUnclosedWhenEnabled(t *testing.T) {
	var got [][]string
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			keys := make([]string, 0, len(snapshot.Series))
			for timeframe := range snapshot.Series {
				keys = append(keys, timeframe)
			}
			sort.Strings(keys)
			got = append(got, keys)
			return nil
		},
	}
	risk := &liveMockRisk{}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{{
			Name:       "okx",
			Active:     true,
			Timeframes: `["3m","15m","1h","4h","1d"]`,
		}},
		symbols: []models.Symbol{{
			Exchange:   "okx",
			Symbol:     "BTC/USDT",
			Timeframes: "",
			Active:     true,
		}},
	}
	live := New(Config{
		Strategy:           strategy,
		StrategyCombos:     []models.StrategyComboConfig{{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true}},
		Risk:               risk,
		OHLCVStore:         store,
		FetchUnclosedOHLCV: true,
	})

	base := int64(1700000000000)
	preload := []models.MarketData{
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "15m", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 101, Low: 99, Close: 100}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1h", Closed: true, OHLCV: models.OHLCV{TS: base, Open: 100, High: 102, Low: 99, Close: 101}},
	}
	for _, item := range preload {
		live.OnOHLCV(item)
	}
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "3m", models.OHLCV{
		TS:    base,
		Open:  100,
		High:  101,
		Low:   99,
		Close: 100,
	}, true)

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    false,
		OHLCV:     models.OHLCV{TS: base + int64(3*time.Minute/time.Millisecond), Open: 100, High: 101, Low: 99, Close: 100},
	})

	if len(got) != 1 {
		t.Fatalf("expected one combo evaluation on unclosed smallest timeframe when fetch_unclosed_ohlcv=true, got %d (%v)", len(got), got)
	}
	if strings.Join(got[0], "/") != "15m/1h/3m" {
		t.Fatalf("unexpected combo snapshot: got %v", got[0])
	}
}

func TestLiveOnOHLCV_NoPositionWithCachedSignalUsesUpdate(t *testing.T) {
	current := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "s1",
		Action:    0,
		HighSide:  1,
		MidSide:   1,
	}
	getCalls := 0
	updateCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return nil
		},
		updateFn: func(_ string, signal models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			updateCalls++
			return signal, true
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
	}
	live := New(Config{Strategy: strategy, Risk: risk})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1500, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if updateCalls != 1 {
		t.Fatalf("expected Strategy.Update called once, got %d", updateCalls)
	}
	if getCalls != 0 {
		t.Fatalf("expected Get not called when cached signal exists, got %d", getCalls)
	}
	if risk.updateCalls == 0 {
		t.Fatalf("expected EvaluateUpdate called")
	}
	if risk.refreshCalls != 0 {
		t.Fatalf("expected no grouped refresh when update returned changed signal, got %d", risk.refreshCalls)
	}
}

func TestLiveOnOHLCV_WithPositionUsesUpdate(t *testing.T) {
	current := models.Signal{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Strategy:  "s1",
		Action:    8,
		HighSide:  1,
		Entry:     100,
		SL:        95,
	}
	strategy := &liveMockStrategy{
		updateFn: func(_ string, _ models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			next := current
			next.Action = 16
			next.SL = 98
			return next, true
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
		positions: []models.Position{{
			Exchange:      "okx",
			Symbol:        "BTC/USDT",
			Timeframe:     "1h",
			Status:        models.PositionStatusOpen,
			PositionSide:  "long",
			EntryQuantity: 1,
		}},
	}
	live := New(Config{Strategy: strategy, Risk: risk})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 2000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.updateCalls == 0 {
		t.Fatalf("expected EvaluateUpdate called")
	}
}

func TestLiveOnOHLCV_NoPositionCachedSignalUsesUpdateToClearSignal(t *testing.T) {
	strategy := &liveMockStrategy{
		updateFn: func(_ string, _ models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			return models.Signal{}, true
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {{
				Exchange:  "okx",
				Symbol:    "BTC/USDT",
				Timeframe: "1h",
				Strategy:  "s1",
				Action:    16,
				HighSide:  1,
			}},
		},
	}
	live := New(Config{Strategy: strategy, Risk: risk})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 3000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.updateCalls == 0 {
		t.Fatalf("expected cleanup EvaluateUpdate called")
	}
	if risk.lastUpdate.Action != 0 {
		t.Fatalf("expected cleanup signal action=0, got %d", risk.lastUpdate.Action)
	}
	if risk.refreshCalls != 0 {
		t.Fatalf("expected no grouped refresh when update clears signal, got %d", risk.refreshCalls)
	}
}

func TestLiveOnOHLCV_StrategyCombosClearsRemovedComboSignalsWithoutTriggeredCombo(t *testing.T) {
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {{
				Exchange:           "okx",
				Symbol:             "BTC/USDT",
				Timeframe:          "30m",
				Strategy:           "turtle",
				StrategyVersion:    "v0.0.6d",
				StrategyTimeframes: []string{"1m", "5m", "30m"},
				ComboKey:           "1m/5m/30m",
				Action:             4,
				HighSide:           -1,
			}},
		},
	}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{{
			Name:       "okx",
			Active:     true,
			Timeframes: `["3m","15m","1h","4h","1d"]`,
		}},
		symbols: []models.Symbol{{
			Exchange:   "okx",
			Symbol:     "BTC/USDT",
			Timeframes: "",
			Active:     true,
		}},
	}
	live := New(Config{
		Strategy:   &liveMockStrategy{},
		Risk:       risk,
		OHLCVStore: store,
		StrategyCombos: []models.StrategyComboConfig{
			{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true},
			{Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false},
		},
	})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 4500, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.updateCalls == 0 {
		t.Fatalf("expected removed combo signal cleanup to trigger EvaluateUpdate")
	}
	if risk.lastUpdate.ComboKey != "1m/5m/30m" {
		t.Fatalf("expected removed combo key cleared, got %q", risk.lastUpdate.ComboKey)
	}
	if risk.lastUpdate.Action != 0 || risk.lastUpdate.HighSide != 0 {
		t.Fatalf("expected removed combo signal cleared, got %+v", risk.lastUpdate)
	}
}

func TestLiveOnOHLCV_CachedSignalUpdateFalseRefreshesTrendGuardCandidate(t *testing.T) {
	current := models.Signal{
		Exchange:          "okx",
		Symbol:            "BTC/USDT",
		Timeframe:         "1h",
		Strategy:          "s1",
		Action:            0,
		HighSide:          1,
		TrendingTimestamp: 12345,
	}
	strategy := &liveMockStrategy{
		updateFn: func(_ string, signal models.Signal, _ models.MarketSnapshot) (models.Signal, bool) {
			return signal, false
		},
	}
	risk := &liveMockRisk{
		signalsByPair: map[string][]models.Signal{
			"okx|BTC/USDT": {current},
		},
	}
	live := New(Config{Strategy: strategy, Risk: risk})

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1h",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 4000, Open: 100, High: 101, Low: 99, Close: 100},
	})

	if risk.refreshCalls != 1 {
		t.Fatalf("expected grouped refresh called once on unchanged update, got %d", risk.refreshCalls)
	}
	if risk.updateCalls != 0 {
		t.Fatalf("expected EvaluateUpdate not called when update=false, got %d", risk.updateCalls)
	}
	if risk.lastRefresh.Symbol != "BTC/USDT" || risk.lastRefresh.Strategy != "s1" {
		t.Fatalf("unexpected refreshed signal: %+v", risk.lastRefresh)
	}
}

func TestLiveListRecentClosedOHLCV_UsesClosedBoundaryNewestFirst(t *testing.T) {
	live := New(Config{})
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1h", models.OHLCV{TS: 1, Close: 101}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1h", models.OHLCV{TS: 2, Close: 102}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1h", models.OHLCV{TS: 3, Close: 103}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1h", models.OHLCV{TS: 4, Close: 104}, false)

	got, err := live.ListRecentClosedOHLCV("okx", "BTC/USDT", "1h", 10)
	if err != nil {
		t.Fatalf("ListRecentClosedOHLCV error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 closed bars, got %d", len(got))
	}
	if got[0].TS != 3 || got[1].TS != 2 || got[2].TS != 1 {
		t.Fatalf("unexpected order/result: %#v", got)
	}
}

func TestLiveShouldPersistPairTimeframe_UsesConfiguredPlan(t *testing.T) {
	store := &liveMockOHLCVStore{}
	live := New(Config{OHLCVStore: store})
	live.timeframePlan = &timeframePlan{
		store: store,
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	if live.shouldPersistPairTimeframe("okx", "BTC/USDT", "1m") {
		t.Fatalf("expected 1m not persisted for configured higher-timeframe pair")
	}
	if !live.shouldPersistPairTimeframe("okx", "BTC/USDT", "15m") {
		t.Fatalf("expected 15m persisted for configured pair")
	}
}

func TestLivePersistOHLCV_OnlyConfiguredTimeframes(t *testing.T) {
	store := &liveMockOHLCVStore{}
	live := New(Config{OHLCVStore: store})
	live.timeframePlan = &timeframePlan{
		store: store,
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	live.persistOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1000},
	})
	live.persistOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1000},
	})

	if len(store.saved) != 1 {
		t.Fatalf("expected exactly one persisted row, got %d", len(store.saved))
	}
	if store.saved[0].Timeframe != "3m" {
		t.Fatalf("expected persisted timeframe=3m, got %s", store.saved[0].Timeframe)
	}
}

func TestLivePersistOHLCV_UnclosedStillIgnoredWhenEnabled(t *testing.T) {
	store := &liveMockOHLCVStore{}
	live := New(Config{
		OHLCVStore:         store,
		FetchUnclosedOHLCV: true,
	})
	live.timeframePlan = &timeframePlan{
		store: store,
		exchangeDefaults: map[string][]string{
			"okx": {"3m"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m"},
		},
	}

	live.persistOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    false,
		OHLCV:     models.OHLCV{TS: 1000},
	})

	if len(store.saved) != 0 {
		t.Fatalf("expected unclosed ohlcv to remain unpersisted even when fetch_unclosed_ohlcv=true, got %d rows", len(store.saved))
	}
}

func TestLiveOnOHLCV_StrategySnapshotUsesConfiguredTimeframesOnly(t *testing.T) {
	gotTimeframes := make([]string, 0)
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			gotTimeframes = snapshotTimeframes(snapshot.Series)
			return nil
		},
	}
	live := New(Config{Strategy: strategy, Risk: &liveMockRisk{}})
	live.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1m", models.OHLCV{TS: 1000, Open: 1, High: 1, Low: 1, Close: 1}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "3m", models.OHLCV{TS: 1080, Open: 1, High: 1, Low: 1, Close: 1}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "15m", models.OHLCV{TS: 900, Open: 1, High: 1, Low: 1, Close: 1}, true)
	live.Cache.AppendOrReplace("okx", "BTC/USDT", "1h", models.OHLCV{TS: 0, Open: 1, High: 1, Low: 1, Close: 1}, true)

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1260, Open: 1, High: 1, Low: 1, Close: 1},
	})

	want := []string{"15m", "1h", "3m"}
	if len(gotTimeframes) != len(want) {
		t.Fatalf("expected snapshot timeframes %v, got %v", want, gotTimeframes)
	}
	for i := range want {
		if gotTimeframes[i] != want[i] {
			t.Fatalf("expected snapshot timeframes %v, got %v", want, gotTimeframes)
		}
	}
}

func TestLiveOnOHLCV_OnlySmallestClosedTimeframeEvaluatesStrategy(t *testing.T) {
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return nil
		},
	}
	live := New(Config{Strategy: strategy, Risk: &liveMockRisk{}})
	live.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "15m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 900000, Open: 1, High: 1, Low: 1, Close: 1},
	})
	if getCalls != 0 {
		t.Fatalf("expected no strategy evaluation on non-smallest timeframe close, got %d", getCalls)
	}

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    false,
		OHLCV:     models.OHLCV{TS: 1080000, Open: 1, High: 1, Low: 1, Close: 1},
	})
	if getCalls != 0 {
		t.Fatalf("expected no strategy evaluation on unclosed smallest timeframe, got %d", getCalls)
	}

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    true,
		OHLCV:     models.OHLCV{TS: 1080000, Open: 1, High: 1, Low: 1, Close: 1},
	})
	if getCalls != 1 {
		t.Fatalf("expected exactly one strategy evaluation on smallest timeframe close, got %d", getCalls)
	}
}

func TestLiveOnOHLCV_FetchUnclosedEnabledAllowsUnclosedDecisionFlow(t *testing.T) {
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(_ models.MarketSnapshot) []models.Signal {
			getCalls++
			return nil
		},
	}
	risk := &liveMockRisk{}
	live := New(Config{
		Strategy:           strategy,
		Risk:               risk,
		FetchUnclosedOHLCV: true,
	})
	live.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	live.OnOHLCV(models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "3m",
		Closed:    false,
		OHLCV:     models.OHLCV{TS: 1080000, Open: 1, High: 1, Low: 1, Close: 1},
	})

	if risk.marketDataCalls == 0 {
		t.Fatalf("expected unclosed smallest timeframe to reach risk when fetch_unclosed_ohlcv=true, got 0 calls")
	}
	if !containsTimeframe(risk.marketDataTFs, "3m") {
		t.Fatalf("expected risk to receive 3m unclosed event when fetch_unclosed_ohlcv=true, got %v", risk.marketDataTFs)
	}
	if getCalls != 1 {
		t.Fatalf("expected unclosed smallest timeframe to trigger strategy when fetch_unclosed_ohlcv=true, got %d", getCalls)
	}
}

func TestLiveOnOHLCV_EvaluationSeesSameTickHigherTimeframeClose(t *testing.T) {
	var seen15mLastClosed int64
	getCalls := 0
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			getCalls++
			if meta, ok := snapshot.Meta["15m"]; ok {
				seen15mLastClosed = meta.LastClosedTS
			}
			return nil
		},
	}
	live := New(Config{Strategy: strategy, Risk: &liveMockRisk{}})
	live.timeframePlan = &timeframePlan{
		exchangeDefaults: map[string][]string{
			"okx": {"3m", "15m", "1h"},
		},
		pairTimeframes: map[string][]string{
			pairKey("okx", "BTC/USDT"): {"3m", "15m", "1h"},
		},
	}

	base := int64(900000)
	for i := 0; i < 15; i++ {
		ts := base + int64(i)*int64(time.Minute/time.Millisecond)
		live.OnOHLCV(models.MarketData{
			Exchange:  "okx",
			Symbol:    "BTC/USDT",
			Timeframe: "1m",
			Closed:    true,
			OHLCV:     models.OHLCV{TS: ts, Open: 100, High: 101, Low: 99, Close: 100},
		})
	}

	if getCalls != 5 {
		t.Fatalf("expected five 3m evaluations, got %d", getCalls)
	}
	if seen15mLastClosed != base {
		t.Fatalf("expected 15m last closed ts=%d, got %d", base, seen15mLastClosed)
	}
}

func TestLiveStart_PerExchangeWarmupReadiness(t *testing.T) {
	blockBinance := make(chan struct{})
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{
			{Name: "okx", Active: true, Timeframes: `["1m"]`},
			{Name: "binance", Active: true, Timeframes: `["1m"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT", Active: true},
			{Exchange: "binance", Symbol: "ETH/USDT", Active: true},
		},
	}
	live := New(Config{
		OHLCVStore:     store,
		HistoryFetcher: &liveMockHistoryRequester{blockByExchange: map[string]chan struct{}{"binance": blockBinance}},
	})
	defer live.Close()

	if err := live.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if live.ExchangeReady("okx") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live.ExchangeReady("okx") {
		t.Fatalf("expected okx warmup to complete independently")
	}
	if live.ExchangeReady("binance") {
		t.Fatalf("expected binance to remain warming before release")
	}
	if got := live.Status().State; got != coreStateRunning {
		t.Fatalf("expected core status running after one exchange ready, got %s", got)
	}

	close(blockBinance)
	deadline = time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if live.ExchangeReady("binance") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live.ExchangeReady("binance") {
		t.Fatalf("expected binance warmup to complete after release")
	}
}

func TestLiveStart_BootstrapEvaluatesCompleteStrategyCombosBeforeReady(t *testing.T) {
	var got [][]string
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			got = append(got, snapshotTimeframes(snapshot.Series))
			return nil
		},
	}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{
			{Name: "okx", Active: true, Timeframes: `["3m","15m","1h","4h","1d"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT", Active: true},
		},
	}
	live := New(Config{
		Strategy:       strategy,
		StrategyCombos: []models.StrategyComboConfig{{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true}, {Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false}},
		OHLCVStore:     store,
		HistoryFetcher: &liveMockHistoryRequester{},
		Risk:           &liveMockRisk{},
		Logger:         zap.NewNop(),
	})
	defer live.Close()

	base := int64(1700000000000)
	preload := []models.MarketData{
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "3m", Closed: true, OHLCV: models.OHLCV{TS: base + int64(3*time.Minute/time.Millisecond), Close: 100}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "15m", Closed: true, OHLCV: models.OHLCV{TS: base + int64(15*time.Minute/time.Millisecond), Close: 100}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1h", Closed: true, OHLCV: models.OHLCV{TS: base + int64(1*time.Hour/time.Millisecond), Close: 101}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "4h", Closed: true, OHLCV: models.OHLCV{TS: base + int64(4*time.Hour/time.Millisecond), Close: 102}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1d", Closed: true, OHLCV: models.OHLCV{TS: base + int64(24*time.Hour/time.Millisecond), Close: 103}},
	}
	for _, item := range preload {
		live.Cache.AppendOrReplace(item.Exchange, item.Symbol, item.Timeframe, item.OHLCV, true)
	}

	if err := live.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if live.ExchangeReady("okx") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live.ExchangeReady("okx") {
		t.Fatalf("expected okx warmup/bootstrap to complete")
	}
	if len(got) != 2 {
		t.Fatalf("expected two bootstrap combo evaluations, got %d (%v)", len(got), got)
	}
	if strings.Join(got[0], "/") != "15m/1h/3m" {
		t.Fatalf("unexpected first bootstrap combo snapshot: got %v", got[0])
	}
	if strings.Join(got[1], "/") != "1d/1h/4h" {
		t.Fatalf("unexpected second bootstrap combo snapshot: got %v", got[1])
	}
}

func TestLiveStart_BootstrapSkipsIncompleteStrategyCombos(t *testing.T) {
	var got [][]string
	strategy := &liveMockStrategy{
		getFn: func(snapshot models.MarketSnapshot) []models.Signal {
			got = append(got, snapshotTimeframes(snapshot.Series))
			return nil
		},
	}
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{
			{Name: "okx", Active: true, Timeframes: `["3m","15m","1h","4h","1d"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT", Active: true},
		},
	}
	live := New(Config{
		Strategy:       strategy,
		StrategyCombos: []models.StrategyComboConfig{{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true}, {Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false}},
		OHLCVStore:     store,
		HistoryFetcher: &liveMockHistoryRequester{},
		Risk:           &liveMockRisk{},
		Logger:         zap.NewNop(),
	})
	defer live.Close()

	base := int64(1700000000000)
	preload := []models.MarketData{
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1h", Closed: true, OHLCV: models.OHLCV{TS: base + int64(1*time.Hour/time.Millisecond), Close: 101}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "4h", Closed: true, OHLCV: models.OHLCV{TS: base + int64(4*time.Hour/time.Millisecond), Close: 102}},
		{Exchange: "okx", Symbol: "BTC/USDT", Timeframe: "1d", Closed: true, OHLCV: models.OHLCV{TS: base + int64(24*time.Hour/time.Millisecond), Close: 103}},
	}
	for _, item := range preload {
		live.Cache.AppendOrReplace(item.Exchange, item.Symbol, item.Timeframe, item.OHLCV, true)
	}

	if err := live.Start(context.Background()); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if live.ExchangeReady("okx") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !live.ExchangeReady("okx") {
		t.Fatalf("expected okx warmup/bootstrap to complete")
	}
	if len(got) != 1 {
		t.Fatalf("expected only one bootstrap combo evaluation, got %d (%v)", len(got), got)
	}
	if strings.Join(got[0], "/") != "1d/1h/4h" {
		t.Fatalf("unexpected bootstrap combo snapshot: got %v", got[0])
	}
}

func TestClipBackfillRangeByBoundPersistsListTime(t *testing.T) {
	store := &liveMockOHLCVStore{}
	listTime := time.Date(2026, 3, 29, 10, 35, 0, 0, time.UTC).UnixMilli()
	history := &liveMockHistoryRequester{
		listTimes: map[string]int64{
			pairKey("okx", "BTC/USDT"): listTime,
		},
	}
	live := New(Config{
		OHLCVStore:     store,
		HistoryFetcher: history,
	})

	from := time.Date(2026, 3, 29, 8, 0, 0, 0, time.UTC).UnixMilli()
	to := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli()
	gotFrom, gotTo, ok := live.clipBackfillRangeByBound(context.Background(), "okx", "BTC/USDT", "1h", from, to)
	if !ok {
		t.Fatalf("expected clipped range to remain valid")
	}
	wantFrom := time.Date(2026, 3, 29, 11, 0, 0, 0, time.UTC).UnixMilli()
	if gotFrom != wantFrom {
		t.Fatalf("unexpected clipped from: got=%d want=%d", gotFrom, wantFrom)
	}
	if gotTo != to {
		t.Fatalf("unexpected clipped to: got=%d want=%d", gotTo, to)
	}
	gotBound, exists, err := store.GetOHLCVBound("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("get bound failed: %v", err)
	}
	if !exists || gotBound != listTime {
		t.Fatalf("expected listTime to persist as bound, got=%d exists=%v", gotBound, exists)
	}
}

func TestClipBackfillRangeByBoundAlignsOKXDailyBoundsToUTCPlus8(t *testing.T) {
	store := &liveMockOHLCVStore{}
	listTime := time.Date(2026, 3, 29, 10, 35, 0, 0, time.UTC).UnixMilli()
	history := &liveMockHistoryRequester{
		listTimes: map[string]int64{
			pairKey("okx", "BTC/USDT"): listTime,
		},
	}
	live := New(Config{
		OHLCVStore:     store,
		HistoryFetcher: history,
	})

	from := time.Date(2026, 3, 28, 0, 0, 0, 0, time.UTC).UnixMilli()
	to := time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC).UnixMilli()
	gotFrom, gotTo, ok := live.clipBackfillRangeByBound(context.Background(), "okx", "BTC/USDT", "1d", from, to)
	if !ok {
		t.Fatalf("expected clipped daily range to remain valid")
	}
	wantFrom := time.Date(2026, 3, 29, 16, 0, 0, 0, time.UTC).UnixMilli()
	if gotFrom != wantFrom {
		t.Fatalf("unexpected clipped from: got=%d want=%d", gotFrom, wantFrom)
	}
	if gotTo != to {
		t.Fatalf("unexpected clipped to: got=%d want=%d", gotTo, to)
	}
}

func TestClipBackfillRangeByBoundAlignsOKXFourHourBoundsUpward(t *testing.T) {
	store := &liveMockOHLCVStore{}
	listTime := time.Date(2026, 2, 23, 11, 55, 0, 0, time.UTC).UnixMilli()
	history := &liveMockHistoryRequester{
		listTimes: map[string]int64{
			pairKey("okx", "OPN/USDT"): listTime,
		},
	}
	live := New(Config{
		OHLCVStore:     store,
		HistoryFetcher: history,
	})

	from := time.Date(2026, 2, 23, 8, 0, 0, 0, time.UTC).UnixMilli()
	to := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC).UnixMilli()
	gotFrom, gotTo, ok := live.clipBackfillRangeByBound(context.Background(), "okx", "OPN/USDT", "4h", from, to)
	if !ok {
		t.Fatalf("expected clipped 4h range to remain valid")
	}
	wantFrom := time.Date(2026, 2, 23, 12, 0, 0, 0, time.UTC).UnixMilli()
	if gotFrom != wantFrom {
		t.Fatalf("unexpected clipped 4h from: got=%d want=%d", gotFrom, wantFrom)
	}
	if gotTo != to {
		t.Fatalf("unexpected clipped 4h to: got=%d want=%d", gotTo, to)
	}
}

func TestBackfillHistoryRecordsBoundOnSmallestTimeframeEmptyRange(t *testing.T) {
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{
			{Name: "okx", Active: true, Timeframes: `["3m","15m","1h"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT", Active: true, Timeframes: `["3m","15m","1h"]`},
		},
	}
	live := New(Config{
		OHLCVStore: store,
		HistoryFetcher: &liveMockHistoryRequester{
			rangeErr: market.ErrEmptyOHLCV,
		},
		Logger: zap.NewNop(),
	})

	earliest := time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC).UnixMilli()
	series := []cachedBar{{ohlcv: models.OHLCV{TS: earliest}}}
	_, progressed := live.backfillHistory(context.Background(), "okx", "BTC/USDT", "3m", series, 2000, 300, int64((3 * time.Minute).Milliseconds()))
	if progressed {
		t.Fatalf("expected no progress on empty-range backfill")
	}
	gotBound, exists, err := store.GetOHLCVBound("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("get bound failed: %v", err)
	}
	if !exists || gotBound != earliest {
		t.Fatalf("expected smallest timeframe backfill to persist bound, got=%d exists=%v", gotBound, exists)
	}
}

func TestBackfillHistoryIgnoresEmptyRangeOnNonSmallestTimeframe(t *testing.T) {
	store := &liveMockOHLCVStore{
		exchanges: []models.Exchange{
			{Name: "okx", Active: true, Timeframes: `["3m","15m","1h"]`},
		},
		symbols: []models.Symbol{
			{Exchange: "okx", Symbol: "BTC/USDT", Active: true, Timeframes: `["3m","15m","1h"]`},
		},
	}
	live := New(Config{
		OHLCVStore: store,
		HistoryFetcher: &liveMockHistoryRequester{
			rangeErr: market.ErrEmptyOHLCV,
		},
		Logger: zap.NewNop(),
	})

	earliest := time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC).UnixMilli()
	series := []cachedBar{{ohlcv: models.OHLCV{TS: earliest}}}
	_, _ = live.backfillHistory(context.Background(), "okx", "BTC/USDT", "1h", series, 2000, 300, int64(time.Hour.Milliseconds()))
	gotBound, exists, err := store.GetOHLCVBound("okx", "BTC/USDT")
	if err != nil {
		t.Fatalf("get bound failed: %v", err)
	}
	if exists || gotBound != 0 {
		t.Fatalf("expected non-smallest timeframe not to persist bound, got=%d exists=%v", gotBound, exists)
	}
}

func TestExpectedLatestOHLCVStart_ClosedOnlyUsesPreviousBucket(t *testing.T) {
	now := time.Date(2026, 3, 29, 12, 43, 52, 0, time.UTC)
	got := expectedLatestOHLCVStart(now, 4*time.Hour, false)
	want := time.Date(2026, 3, 29, 8, 0, 0, 0, time.UTC).UnixMilli()
	if got != want {
		t.Fatalf("unexpected latest closed start: got=%d want=%d", got, want)
	}
}

func TestExpectedLatestOHLCVStart_WithUnclosedUsesCurrentBucket(t *testing.T) {
	now := time.Date(2026, 3, 29, 12, 43, 52, 0, time.UTC)
	got := expectedLatestOHLCVStart(now, 4*time.Hour, true)
	want := time.Date(2026, 3, 29, 12, 0, 0, 0, time.UTC).UnixMilli()
	if got != want {
		t.Fatalf("unexpected latest current start: got=%d want=%d", got, want)
	}
}

func TestEnsureTimely_ClosedOnlyLatestClosedBucketDoesNotFetchRecent(t *testing.T) {
	history := &liveMockHistoryRequester{}
	live := New(Config{
		HistoryFetcher:     history,
		FetchUnclosedOHLCV: false,
		Logger:             zap.NewNop(),
	})

	now := time.Now().UTC()
	dur := 4 * time.Hour
	step := dur.Milliseconds()
	lastClosed := expectedLatestOHLCVStart(now, dur, false)
	if lastClosed <= 0 {
		t.Fatalf("expected positive latest closed start")
	}
	series := []cachedBar{{ohlcv: models.OHLCV{TS: lastClosed}, closed: true}}

	got, err := live.ensureTimely(context.Background(), "okx", "BTC/USDT", "4h", series, 2000, 300)
	if err != nil {
		t.Fatalf("ensureTimely returned error: %v", err)
	}
	if history.byLimitCalls != 0 {
		t.Fatalf("expected no recent fetch, got %d", history.byLimitCalls)
	}
	if len(got) != 1 || got[0].ohlcv.TS != lastClosed {
		t.Fatalf("unexpected series after ensureTimely: %+v", got)
	}
	if expectedLatestOHLCVStart(now, dur, true)-lastClosed != step {
		t.Fatalf("test setup invalid: expected current bucket to be exactly one step ahead")
	}
}

func TestEnsureTimelyHistory_ClosedOnlyLatestClosedBucketDoesNotFetchRecent(t *testing.T) {
	history := &liveMockHistoryRequester{}
	live := New(Config{
		HistoryFetcher:     history,
		FetchUnclosedOHLCV: false,
		Logger:             zap.NewNop(),
	})

	now := time.Now().UTC()
	dur := 24 * time.Hour
	step := dur.Milliseconds()
	lastClosed := expectedLatestOHLCVStart(now, dur, false)
	if lastClosed <= 0 {
		t.Fatalf("expected positive latest closed start")
	}
	series := []cachedBar{{ohlcv: models.OHLCV{TS: lastClosed}, closed: true}}

	got := live.ensureTimelyHistory(context.Background(), "okx", "BTC/USDT", "1d", series, 2000, 300, step, dur)
	if history.byLimitCalls != 0 {
		t.Fatalf("expected no recent fetch, got %d", history.byLimitCalls)
	}
	if len(got) != 1 || got[0].ohlcv.TS != lastClosed {
		t.Fatalf("unexpected series after ensureTimelyHistory: %+v", got)
	}
}

func TestEnsureTimely_IgnoresFetchUnclosedFlagForWarmup(t *testing.T) {
	history := &liveMockHistoryRequester{}
	live := New(Config{
		HistoryFetcher:     history,
		FetchUnclosedOHLCV: true,
		Logger:             zap.NewNop(),
	})

	now := time.Now().UTC()
	dur := 4 * time.Hour
	lastClosed := expectedLatestOHLCVStart(now, dur, false)
	if lastClosed <= 0 {
		t.Fatalf("expected positive latest closed start")
	}
	series := []cachedBar{{ohlcv: models.OHLCV{TS: lastClosed}, closed: true}}

	got, err := live.ensureTimely(context.Background(), "okx", "BTC/USDT", "4h", series, 2000, 300)
	if err != nil {
		t.Fatalf("ensureTimely returned error: %v", err)
	}
	if history.byLimitCalls != 0 {
		t.Fatalf("expected warmup recent fetch to ignore fetch_unclosed_ohlcv, got %d", history.byLimitCalls)
	}
	if len(got) != 1 || got[0].ohlcv.TS != lastClosed {
		t.Fatalf("unexpected series after ensureTimely: %+v", got)
	}
}

func TestEnsureTimelyHistory_IgnoresFetchUnclosedFlag(t *testing.T) {
	history := &liveMockHistoryRequester{}
	live := New(Config{
		HistoryFetcher:     history,
		FetchUnclosedOHLCV: true,
		Logger:             zap.NewNop(),
	})

	now := time.Now().UTC()
	dur := 24 * time.Hour
	step := dur.Milliseconds()
	lastClosed := expectedLatestOHLCVStart(now, dur, false)
	if lastClosed <= 0 {
		t.Fatalf("expected positive latest closed start")
	}
	series := []cachedBar{{ohlcv: models.OHLCV{TS: lastClosed}, closed: true}}

	got := live.ensureTimelyHistory(context.Background(), "okx", "BTC/USDT", "1d", series, 2000, 300, step, dur)
	if history.byLimitCalls != 0 {
		t.Fatalf("expected history recent fetch to ignore fetch_unclosed_ohlcv, got %d", history.byLimitCalls)
	}
	if len(got) != 1 || got[0].ohlcv.TS != lastClosed {
		t.Fatalf("unexpected series after ensureTimelyHistory: %+v", got)
	}
}

func snapshotTimeframes(series map[string][]models.OHLCV) []string {
	out := make([]string, 0, len(series))
	for timeframe := range series {
		out = append(out, timeframe)
	}
	sort.Strings(out)
	return out
}

var _ iface.Evaluator = (*liveMockRisk)(nil)
var _ iface.TrendGuardRefresher = (*liveMockRisk)(nil)
var _ iface.OHLCVStore = (*liveMockOHLCVStore)(nil)
var _ iface.HistoryRequester = (*liveMockHistoryRequester)(nil)
var _ iface.SymbolListTimeFetcher = (*liveMockHistoryRequester)(nil)
