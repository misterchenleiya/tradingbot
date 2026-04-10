package market

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
)

type testFetcher struct {
	calls int
}

type testDynamicFetcher struct {
	markets   []models.Symbol
	volumes   map[string][]float64
	listTimes map[string]int64
	loadCalls atomic.Int32
}

type testSymbolStore struct {
	upserts []models.Symbol
	bounds  map[string]int64
}

func (f *testFetcher) FetchLatest(ctx context.Context, exchange, symbol, timeframe string) (models.OHLCV, error) {
	_ = ctx
	_ = exchange
	_ = symbol
	_ = timeframe
	f.calls++
	return models.OHLCV{}, nil
}

func (f *testDynamicFetcher) LoadPerpUSDTMarkets(_ context.Context, _ string) ([]models.Symbol, error) {
	if f != nil {
		f.loadCalls.Add(1)
	}
	return append([]models.Symbol(nil), f.markets...), nil
}

func (f *testDynamicFetcher) FetchDailyVolumesUSDT(_ context.Context, exchange, symbol string, _ int) ([]float64, error) {
	if f == nil || f.volumes == nil {
		return nil, nil
	}
	return append([]float64(nil), f.volumes[exchange+"|"+symbol]...), nil
}

func (f *testDynamicFetcher) FetchSymbolListTime(_ context.Context, exchange, symbol string) (int64, error) {
	if f == nil || f.listTimes == nil {
		return 0, nil
	}
	return f.listTimes[exchange+"|"+symbol], nil
}

func (s *testSymbolStore) ListSymbols() ([]models.Symbol, error) {
	return nil, nil
}

func (s *testSymbolStore) ListExchanges() ([]models.Exchange, error) {
	return nil, nil
}

func (s *testSymbolStore) UpsertSymbol(sym models.Symbol) error {
	s.upserts = append(s.upserts, sym)
	return nil
}

func (s *testSymbolStore) UpdateSymbolActive(exchange, symbol string, active bool) error {
	_ = exchange
	_ = symbol
	_ = active
	return nil
}

func (s *testSymbolStore) UpsertOHLCVBound(exchange, symbol string, earliestAvailableTS int64) error {
	if s.bounds == nil {
		s.bounds = make(map[string]int64)
	}
	key := exchange + "|" + symbol
	if current, ok := s.bounds[key]; !ok || current <= 0 || earliestAvailableTS < current {
		s.bounds[key] = earliestAvailableTS
	}
	return nil
}

func TestRealTimeServiceStatusIncludesWarmingExchange(t *testing.T) {
	svc := NewRealTimeService(RealTimeConfig{
		ExchangeStatus: func(exchange string) (string, string) {
			if exchange == "okx" {
				return "warming", "warming"
			}
			return "ready", ""
		},
	})
	svc.started.Store(true)
	svc.groups = map[string]*exchangeGroup{
		"okx":     newExchangeGroup(0, 0, nil),
		"binance": newExchangeGroup(0, 0, nil),
	}

	status := svc.Status()
	if status.State != "running" {
		t.Fatalf("expected overall status running, got %s", status.State)
	}
	details, ok := status.Details.(marketStatusDetails)
	if !ok {
		t.Fatalf("expected marketStatusDetails, got %T", status.Details)
	}
	if details.Exchanges["okx"].State != "warming" {
		t.Fatalf("expected okx warming, got %s", details.Exchanges["okx"].State)
	}
	if details.Exchanges["binance"].State != "running" {
		t.Fatalf("expected binance running, got %s", details.Exchanges["binance"].State)
	}
}

func TestExchangeWorkerSkipsFetcherWhenExchangeNotReady(t *testing.T) {
	fetcher := &testFetcher{}
	svc := NewRealTimeService(RealTimeConfig{
		Fetcher: fetcher,
		ExchangeStatus: func(exchange string) (string, string) {
			if exchange == "okx" {
				return "warming", ""
			}
			return "ready", ""
		},
	})
	svc.stopCh = make(chan struct{})

	task := &symbolState{exchange: "okx", symbol: "BTC/USDT", timeframe: "1m"}
	task.active.Store(true)
	workCh := make(chan *symbolState, 1)
	workCh <- task
	close(workCh)

	svc.wg.Add(1)
	svc.exchangeWorker("okx", workCh, iface.MarketHandler(func(models.MarketData) {}))

	if fetcher.calls != 0 {
		t.Fatalf("expected fetcher not called for warming exchange, got %d", fetcher.calls)
	}
}

func TestLoadMarketAndFilterWithVolumePersistsListTimeBoundOnFirstAdd(t *testing.T) {
	store := &testSymbolStore{}
	fetcher := &testDynamicFetcher{
		markets: []models.Symbol{{
			Exchange: "okx",
			Symbol:   "AZTEC/USDT",
			Base:     "AZTEC",
			Quote:    "USDT",
			Type:     "swap",
			ListTime: 1770796800000,
		}},
		volumes: map[string][]float64{
			"okx|AZTEC/USDT": {2_000_000},
		},
	}
	svc := NewRealTimeService(RealTimeConfig{
		Store:          store,
		DynamicFetcher: fetcher,
		Timeframe:      "1m",
	})
	svc.stopCh = make(chan struct{})

	group := newExchangeGroup(0, 1, []string{"3m", "15m", "1h", "4h", "1d"})
	svc.loadMarketAndFilterWithVolume("okx", group)

	if len(store.upserts) != 1 {
		t.Fatalf("expected symbol to be inserted once, got %d", len(store.upserts))
	}
	gotBound := store.bounds["okx|AZTEC/USDT"]
	if gotBound != 1770796800000 {
		t.Fatalf("expected listTime bound to be persisted, got %d", gotBound)
	}
}

func TestDynamicExchangeWaitsForReadyBeforeInitialLoad(t *testing.T) {
	fetcher := &testDynamicFetcher{}
	var ready atomic.Bool
	svc := NewRealTimeService(RealTimeConfig{
		DynamicFetcher: fetcher,
		ExchangeStatus: func(exchange string) (string, string) {
			if exchange == "okx" && !ready.Load() {
				return "warming", ""
			}
			return "ready", ""
		},
	})
	svc.stopCh = make(chan struct{})

	group := newExchangeGroup(0, 1, []string{"1m"})
	svc.wg.Add(1)
	go svc.runDynamicExchange("okx", group)

	time.Sleep(200 * time.Millisecond)
	if got := fetcher.loadCalls.Load(); got != 0 {
		close(svc.stopCh)
		svc.wg.Wait()
		t.Fatalf("expected no dynamic market load while exchange warming, got %d", got)
	}

	ready.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fetcher.loadCalls.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	close(svc.stopCh)
	svc.wg.Wait()

	if got := fetcher.loadCalls.Load(); got == 0 {
		t.Fatalf("expected dynamic market load after exchange ready")
	}
}

func TestHandleOHLCV_DropsUnclosedWhenFetchUnclosedDisabled(t *testing.T) {
	svc := NewRealTimeService(RealTimeConfig{
		FetchUnclosedOHLCV: false,
	})

	calls := 0
	svc.handleOHLCV(iface.MarketHandler(func(models.MarketData) {
		calls++
	}), models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    false,
	})

	if calls != 0 {
		t.Fatalf("expected unclosed ohlcv to be dropped when fetch_unclosed_ohlcv=false, got %d handler calls", calls)
	}
}

func TestHandleOHLCV_AllowsUnclosedWhenFetchUnclosedEnabled(t *testing.T) {
	svc := NewRealTimeService(RealTimeConfig{
		FetchUnclosedOHLCV: true,
	})

	calls := 0
	svc.handleOHLCV(iface.MarketHandler(func(models.MarketData) {
		calls++
	}), models.MarketData{
		Exchange:  "okx",
		Symbol:    "BTC/USDT",
		Timeframe: "1m",
		Closed:    false,
	})

	if calls != 1 {
		t.Fatalf("expected unclosed ohlcv to reach handler when fetch_unclosed_ohlcv=true, got %d handler calls", calls)
	}
}

var _ iface.Fetcher = (*testFetcher)(nil)
var _ iface.DynamicFetcher = (*testDynamicFetcher)(nil)
var _ iface.SymbolListTimeFetcher = (*testDynamicFetcher)(nil)
var _ iface.SymbolStore = (*testSymbolStore)(nil)
var _ iface.OHLCVBoundsWriter = (*testSymbolStore)(nil)
