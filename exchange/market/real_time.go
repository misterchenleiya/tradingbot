package market

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const (
	dynamicInterval    = 5 * time.Minute
	dynamicRetryWait   = time.Second
	volumeLookbackDays = 14
	volumeUnitUSDT     = 1_000_000
)

type RealTimeConfig struct {
	Store                      iface.SymbolStore
	Fetcher                    iface.Fetcher
	DynamicFetcher             iface.DynamicFetcher
	WS                         iface.Streamer
	Handler                    iface.MarketHandler
	ExchangeStatus             func(exchange string) (state string, message string)
	DynamicMarket              bool
	WSEnabled                  bool
	WSStaleThreshold           time.Duration
	WSStaleThresholdByExchange map[string]time.Duration
	Controller                 *RequestController
	FetchUnclosedOHLCV         bool
	Timeframe                  string
	Interval                   time.Duration
	Logger                     *zap.Logger
	Concurrency                int
}

type RealTimeService struct {
	cfg     RealTimeConfig
	stopCh  chan struct{}
	wg      sync.WaitGroup
	started atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	wsEnabled             bool
	wsThresholdByExchange map[string]time.Duration

	groupsMu sync.RWMutex
	groups   map[string]*exchangeGroup

	seqMu sync.Mutex
	seq   map[string]int64
}

type symbolState struct {
	exchange     string
	symbol       string
	timeframe    string
	active       atomic.Bool
	dynamic      bool
	wsSubscribed atomic.Bool
	lastWSAt     atomic.Int64
	runtimeMu    sync.RWMutex
	runtimeData  models.MarketData
	runtimeOK    bool
	runtimeAtMS  int64
}

type exchangeGroup struct {
	rate         time.Duration
	volumeFilter float64
	timeframes   []string
	mu           sync.RWMutex
	tasks        []*symbolState
	byKey        map[string]*symbolState
	bySymbol     map[string][]*symbolState
}

func (s *symbolState) storeRuntimeData(data models.MarketData) {
	if s == nil || data.OHLCV.TS <= 0 {
		return
	}
	s.runtimeMu.Lock()
	s.runtimeData = data
	s.runtimeOK = true
	s.runtimeAtMS = time.Now().UTC().UnixMilli()
	s.runtimeMu.Unlock()
}

func (s *symbolState) runtimeSnapshot() (RuntimeOHLCVSnapshot, bool) {
	if s == nil {
		return RuntimeOHLCVSnapshot{}, false
	}
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	if !s.runtimeOK || s.runtimeData.OHLCV.TS <= 0 {
		return RuntimeOHLCVSnapshot{}, false
	}
	return RuntimeOHLCVSnapshot{
		Exchange:    s.runtimeData.Exchange,
		Symbol:      s.runtimeData.Symbol,
		Timeframe:   s.runtimeData.Timeframe,
		OHLCV:       s.runtimeData.OHLCV,
		Closed:      s.runtimeData.Closed,
		Source:      s.runtimeData.Source,
		Seq:         s.runtimeData.Seq,
		UpdatedAtMS: s.runtimeAtMS,
	}, true
}

func newExchangeGroup(rate time.Duration, volumeFilter float64, timeframes []string) *exchangeGroup {
	return &exchangeGroup{
		rate:         rate,
		volumeFilter: volumeFilter,
		timeframes:   timeframes,
		byKey:        make(map[string]*symbolState),
		bySymbol:     make(map[string][]*symbolState),
	}
}

func (g *exchangeGroup) addSymbol(sym models.Symbol, timeframes []string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	added := false
	if len(timeframes) == 0 {
		timeframes = g.timeframes
	}
	if len(timeframes) == 0 {
		timeframes = []string{"1m"}
	}
	for _, timeframe := range timeframes {
		key := symbolKey(sym.Symbol, timeframe)
		if state, ok := g.byKey[key]; ok {
			state.active.Store(sym.Active)
			state.dynamic = sym.Dynamic
			continue
		}
		state := &symbolState{
			exchange:  sym.Exchange,
			symbol:    sym.Symbol,
			timeframe: timeframe,
			dynamic:   sym.Dynamic,
		}
		state.active.Store(sym.Active)
		g.byKey[key] = state
		g.bySymbol[sym.Symbol] = append(g.bySymbol[sym.Symbol], state)
		g.tasks = append(g.tasks, state)
		added = true
	}
	return added
}

func (g *exchangeGroup) nextTask(idx *int) *symbolState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.tasks) == 0 {
		return nil
	}
	if *idx >= len(g.tasks) {
		*idx = 0
	}
	task := g.tasks[*idx]
	*idx = (*idx + 1) % len(g.tasks)
	return task
}

func (g *exchangeGroup) taskCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.tasks)
}

func (g *exchangeGroup) symbolStates(symbol string) []*symbolState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	states := g.bySymbol[symbol]
	if len(states) == 0 {
		return nil
	}
	out := make([]*symbolState, len(states))
	copy(out, states)
	return out
}

func (g *exchangeGroup) stateByKey(symbol, timeframe string) *symbolState {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.byKey[symbolKey(symbol, timeframe)]
}

func (g *exchangeGroup) setSymbolActive(symbol string, active bool, dynamic bool) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	states := g.bySymbol[symbol]
	if len(states) == 0 {
		return false
	}
	for _, state := range states {
		state.active.Store(active)
		state.dynamic = dynamic
	}
	return true
}

func symbolKey(symbol, timeframe string) string {
	return symbol + "|" + timeframe
}

func NewRealTimeService(cfg RealTimeConfig) *RealTimeService {
	if cfg.Timeframe == "" {
		cfg.Timeframe = "1m"
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = glog.Nop()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	return &RealTimeService{
		cfg: cfg,
	}
}

func (s *RealTimeService) exchangeReady(exchange string) bool {
	if s == nil || s.cfg.ExchangeStatus == nil {
		return true
	}
	state, _ := s.cfg.ExchangeStatus(exchange)
	state = strings.ToLower(strings.TrimSpace(state))
	if state == "" {
		return true
	}
	return state == "ready"
}

func (s *RealTimeService) exchangeStatus(exchange string) (string, string) {
	if s == nil || s.cfg.ExchangeStatus == nil {
		return "", ""
	}
	return s.cfg.ExchangeStatus(exchange)
}

func (s *RealTimeService) Start(ctx context.Context) (err error) {
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	fields := []zap.Field{
		zap.String("timeframe", s.cfg.Timeframe),
		zap.Duration("interval", s.cfg.Interval),
		zap.Int("concurrency", s.cfg.Concurrency),
		zap.Bool("dynamic_market", s.cfg.DynamicMarket),
		zap.Bool("ws_enabled", s.cfg.WSEnabled),
		zap.Bool("fetch_unclosed_ohlcv", s.cfg.FetchUnclosedOHLCV),
		zap.Duration("ws_stale_threshold", s.cfg.WSStaleThreshold),
		zap.Int("ws_stale_threshold_exchange_count", len(s.cfg.WSStaleThresholdByExchange)),
	}
	logger.Info("market live start", fields...)
	defer func() {
		logger.Info("market live started")
	}()
	handler := s.cfg.Handler
	if handler == nil {
		return errors.New("nil handler")
	}
	if s.cfg.Store == nil {
		return errors.New("nil store")
	}
	if s.cfg.Fetcher == nil {
		return errors.New("nil fetcher")
	}
	if s.cfg.DynamicMarket && s.cfg.DynamicFetcher == nil {
		return errors.New("nil dynamic fetcher")
	}
	if s.cfg.Controller != nil {
		s.attachRequestController()
	}
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("service already started")
	}
	s.stopCh = make(chan struct{})
	s.seqMu.Lock()
	s.seq = make(map[string]int64)
	s.seqMu.Unlock()

	exchanges, err := s.cfg.Store.ListExchanges()
	if err != nil {
		s.started.Store(false)
		return err
	}
	if len(exchanges) == 0 {
		s.started.Store(false)
		return errors.New("no exchanges configured")
	}

	symbols, err := s.cfg.Store.ListSymbols()
	if err != nil {
		s.started.Store(false)
		return err
	}
	symbols = normalizeHyperliquidSymbols(s.cfg.Logger, s.cfg.Store, symbols)
	if len(symbols) == 0 && !s.cfg.DynamicMarket {
		s.started.Store(false)
		return errors.New("no symbols configured")
	}

	groups := make(map[string]*exchangeGroup)
	inactiveExchanges := make(map[string]bool)
	for _, ex := range exchanges {
		if !ex.Active {
			inactiveExchanges[ex.Name] = true
			continue
		}
		rate := s.exchangeRate(ex)
		if _, err := parseExchangeTimeframes(ex.Timeframes, s.cfg.Timeframe); err != nil {
			s.cfg.Logger.Warn("invalid exchange timeframes",
				zap.String("exchange", ex.Name),
				zap.String("timeframes", ex.Timeframes),
				zap.Error(err),
			)
		}
		groups[ex.Name] = newExchangeGroup(rate, ex.VolumeFilter, []string{s.cfg.Timeframe})
	}
	if len(groups) == 0 {
		s.started.Store(false)
		return errors.New("no active exchanges configured")
	}

	activeCount := 0
	for _, sym := range symbols {
		group, ok := groups[sym.Exchange]
		if !ok {
			if !inactiveExchanges[sym.Exchange] {
				s.cfg.Logger.Warn("symbol exchange not configured",
					zap.String("exchange", sym.Exchange),
					zap.String("symbol", sym.Symbol),
				)
			}
			continue
		}
		if !sym.Active {
			continue
		}
		activeCount++
		if _, err := parseSymbolTimeframes(sym.Timeframes, group.timeframes); err != nil {
			s.cfg.Logger.Warn("invalid symbol timeframes",
				zap.String("exchange", sym.Exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(err),
			)
		}
		group.addSymbol(sym, []string{s.cfg.Timeframe})
	}

	s.cfg.Logger.Info("market live prepared",
		zap.Int("symbols", len(symbols)),
		zap.Int("active_symbols", activeCount),
		zap.Int("exchanges", len(groups)),
		zap.String("timeframe", s.cfg.Timeframe),
		zap.Duration("default_interval", s.cfg.Interval),
		zap.Int("concurrency", s.cfg.Concurrency),
		zap.Bool("dynamic_market", s.cfg.DynamicMarket),
	)

	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wsThresholdByExchange = normalizeWSStaleThresholds(s.cfg.WSStaleThresholdByExchange)
	s.groupsMu.Lock()
	s.groups = groups
	s.groupsMu.Unlock()

	s.wsEnabled = s.cfg.WSEnabled && s.cfg.WS != nil
	if s.wsEnabled {
		if err := s.cfg.WS.Start(s.ctx); err != nil {
			s.cfg.Logger.Warn("market ws start failed", zap.Error(err))
			s.wsEnabled = false
		} else {
			s.wg.Add(1)
			go s.runWS(handler)
		}
	}

	for exchange, group := range groups {
		s.wg.Add(1)
		go s.runExchange(exchange, group, handler)
	}

	if s.cfg.DynamicMarket {
		for exchange, group := range groups {
			s.wg.Add(1)
			go s.runDynamicExchange(exchange, group)
		}
	}

	if s.wsEnabled {
		for _, sym := range symbols {
			if !sym.Active {
				continue
			}
			s.subscribeWS(sym.Exchange, sym.Symbol)
		}
	}

	return nil
}

func (s *RealTimeService) Close() (err error) {
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	logger.Info("market live close")
	defer func() {
		logger.Info("market live closed")
	}()
	if !s.started.CompareAndSwap(true, false) {
		return nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	close(s.stopCh)
	s.wg.Wait()
	return nil
}

func (s *RealTimeService) Stop() error {
	return s.Close()
}

func (s *RealTimeService) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = glog.Nop()
	}
	s.cfg.Logger = logger
}

func (s *RealTimeService) attachRequestController() {
	if s == nil || s.cfg.Controller == nil {
		return
	}
	logger := s.cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	attachFetcher := func(name string, fetcher any) {
		if fetcher == nil {
			return
		}
		if f, ok := fetcher.(*HTTPFetcher); ok {
			f.Controller = s.cfg.Controller
			if f.Logger == nil {
				f.Logger = logger
			}
			return
		}
		logger.Warn("fetcher does not support request controller", zap.String("fetcher", name))
	}
	attachFetcher("fetcher", s.cfg.Fetcher)
	attachFetcher("dynamic_fetcher", s.cfg.DynamicFetcher)
	if s.cfg.WS != nil {
		s.cfg.WS = NewControlledStreamer(s.cfg.WS, s.cfg.Controller, logger)
	}
}

func (s *RealTimeService) runExchange(exchange string, group *exchangeGroup, handler iface.MarketHandler) {
	defer s.wg.Done()
	workCh := make(chan *symbolState, s.cfg.Concurrency)
	for i := 0; i < s.cfg.Concurrency; i++ {
		s.wg.Add(1)
		go s.exchangeWorker(exchange, workCh, handler)
	}

	s.cfg.Logger.Info("market exchange polling started",
		zap.String("exchange", exchange),
		zap.Int("tasks", group.taskCount()),
		zap.Duration("rate_limit", group.rate),
		zap.Int("concurrency", s.cfg.Concurrency),
	)

	ticker := time.NewTicker(group.rate)
	defer ticker.Stop()
	idx := 0
	paused := false
	var pauseUntil time.Time

	for {
		select {
		case <-ticker.C:
			now := time.Now().UTC()
			if until, ok := ExchangePaused(exchange, now); ok {
				if !paused || !until.Equal(pauseUntil) {
					s.cfg.Logger.Warn("market exchange paused",
						zap.String("exchange", exchange),
						zap.Time("paused_until", until),
						zap.Duration("resume_in", time.Until(until)),
					)
					paused = true
					pauseUntil = until
				}
				continue
			}
			if !s.exchangeReady(exchange) {
				continue
			}
			if paused {
				s.cfg.Logger.Info("market exchange resumed",
					zap.String("exchange", exchange),
				)
				paused = false
				pauseUntil = time.Time{}
			}
			task := group.nextTask(&idx)
			if task == nil {
				continue
			}
			select {
			case workCh <- task:
			case <-s.stopCh:
				close(workCh)
				s.cfg.Logger.Info("market exchange polling stopped", zap.String("exchange", exchange))
				return
			}
		case <-s.stopCh:
			close(workCh)
			s.cfg.Logger.Info("market exchange polling stopped", zap.String("exchange", exchange))
			return
		}
	}
}

func (s *RealTimeService) exchangeWorker(exchange string, workCh <-chan *symbolState, handler iface.MarketHandler) {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case task, ok := <-workCh:
			if !ok {
				return
			}
			if !task.active.Load() {
				continue
			}
			if _, ok := ExchangePaused(task.exchange, time.Now().UTC()); ok {
				continue
			}
			if !s.exchangeReady(task.exchange) {
				continue
			}
			if s.shouldSkipREST(task) {
				continue
			}
			ohlcv, err := s.cfg.Fetcher.FetchLatest(s.requestContext(), task.exchange, task.symbol, task.timeframe)
			if err != nil {
				if s.stopped() {
					return
				}
				if s.handleDelistedSymbol(task, err) {
					continue
				}
				s.cfg.Logger.Warn("fetch ohlcv failed",
					zap.String("exchange", task.exchange),
					zap.String("symbol", task.symbol),
					zap.Error(err),
				)
				continue
			}
			seq := s.nextSeq(task.exchange, task.symbol, task.timeframe)
			closed := isOHLCVClosed(ohlcv.TS, task.timeframe, time.Now().UTC())
			data := models.MarketData{
				Exchange:  task.exchange,
				Symbol:    task.symbol,
				Timeframe: task.timeframe,
				OHLCV:     ohlcv,
				Closed:    closed,
				Source:    "live",
				Seq:       seq,
			}
			task.storeRuntimeData(data)
			s.handleOHLCV(handler, data)
		}
	}
}

func (s *RealTimeService) runDynamicExchange(exchange string, group *exchangeGroup) {
	defer s.wg.Done()
	s.cfg.Logger.Info("market dynamic queue started",
		zap.String("exchange", exchange),
		zap.Float64("volume_filter_million", group.volumeFilter),
	)

	if s.stopped() {
		return
	}
	tryLoad := func() bool {
		if !s.exchangeReady(exchange) {
			return false
		}
		if _, ok := ExchangePaused(exchange, time.Now().UTC()); ok {
			return false
		}
		s.loadMarketAndFilterWithVolume(exchange, group)
		return true
	}

	ticker := time.NewTicker(dynamicInterval)
	retryTicker := time.NewTicker(dynamicRetryWait)
	defer ticker.Stop()
	defer retryTicker.Stop()

	retryCh := retryTicker.C
	if tryLoad() {
		retryTicker.Stop()
		retryCh = nil
	}

	for {
		select {
		case <-retryCh:
			if tryLoad() {
				retryTicker.Stop()
				retryCh = nil
			}
		case <-ticker.C:
			_ = tryLoad()
		case <-s.stopCh:
			s.cfg.Logger.Info("market dynamic queue stopped", zap.String("exchange", exchange))
			return
		}
	}
}

type observationSymbol struct {
	symbol models.Symbol
	exists bool
}

func (s *RealTimeService) loadMarketAndFilterWithVolume(exchange string, group *exchangeGroup) {
	if s.stopped() {
		return
	}
	markets, err := s.cfg.DynamicFetcher.LoadPerpUSDTMarkets(s.requestContext(), exchange)
	if err != nil {
		if s.stopped() {
			return
		}
		s.cfg.Logger.Warn("load markets failed",
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		return
	}
	if len(markets) == 0 {
		return
	}

	var observations []observationSymbol
	var activeDynamicSymbols []string

	if s.stopped() {
		return
	}
	group.mu.RLock()
	for _, market := range markets {
		states := group.bySymbol[market.Symbol]
		if len(states) == 0 {
			observations = append(observations, observationSymbol{symbol: market, exists: false})
			continue
		}
		state := states[0]
		if !state.active.Load() && state.dynamic {
			observations = append(observations, observationSymbol{symbol: market, exists: true})
		}
	}
	for symbol, states := range group.bySymbol {
		if len(states) == 0 {
			continue
		}
		state := states[0]
		if state.dynamic && state.active.Load() {
			activeDynamicSymbols = append(activeDynamicSymbols, symbol)
		}
	}
	group.mu.RUnlock()

	threshold := group.volumeFilter * volumeUnitUSDT

	for _, item := range observations {
		if s.stopped() {
			return
		}
		avg, days, err := s.avgDailyVolumeUSDT(exchange, item.symbol.Symbol)
		if err != nil {
			if s.stopped() {
				return
			}
			s.cfg.Logger.Warn("calculate volume failed",
				zap.String("exchange", exchange),
				zap.String("symbol", item.symbol.Symbol),
				zap.Error(err),
			)
			continue
		}
		if days == 0 || avg < threshold {
			continue
		}

		sym := item.symbol
		sym.Exchange = exchange
		sym.Active = true
		sym.Dynamic = true

		if item.exists {
			if err := s.cfg.Store.UpdateSymbolActive(exchange, sym.Symbol, true); err != nil {
				s.cfg.Logger.Warn("activate symbol failed",
					zap.String("exchange", exchange),
					zap.String("symbol", sym.Symbol),
					zap.Error(err),
				)
				continue
			}
			shouldSubscribe := group.setSymbolActive(sym.Symbol, true, true)
			if shouldSubscribe {
				s.subscribeWS(exchange, sym.Symbol)
			}
			s.cfg.Logger.Info("market symbol activated by volume filter",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.Float64("avg_volume_usdt", avg),
				zap.Int("days", days),
				zap.Float64("volume_filter_million", group.volumeFilter),
			)
			continue
		}

		if err := s.cfg.Store.UpsertSymbol(sym); err != nil {
			s.cfg.Logger.Warn("add symbol failed",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.Error(err),
			)
			continue
		}
		s.persistSymbolListTimeBound(exchange, sym)
		if _, err := parseSymbolTimeframes(sym.Timeframes, group.timeframes); err != nil {
			s.cfg.Logger.Warn("invalid symbol timeframes",
				zap.String("exchange", sym.Exchange),
				zap.String("symbol", sym.Symbol),
				zap.String("timeframes", sym.Timeframes),
				zap.Error(err),
			)
		}
		if added := group.addSymbol(sym, []string{s.cfg.Timeframe}); added {
			s.subscribeWS(exchange, sym.Symbol)
			s.cfg.Logger.Info("market symbol added to work queue",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.Float64("avg_volume_usdt", avg),
				zap.Int("days", days),
				zap.Float64("volume_filter_million", group.volumeFilter),
			)
		}
	}

	for _, symbol := range activeDynamicSymbols {
		if s.stopped() {
			return
		}
		avg, days, err := s.avgDailyVolumeUSDT(exchange, symbol)
		if err != nil {
			if s.stopped() {
				return
			}
			s.cfg.Logger.Warn("calculate volume failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(err),
			)
			continue
		}
		if days == 0 || avg >= threshold {
			continue
		}
		if err := s.cfg.Store.UpdateSymbolActive(exchange, symbol, false); err != nil {
			s.cfg.Logger.Warn("deactivate symbol failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(err),
			)
			continue
		}
		group.setSymbolActive(symbol, false, true)
		s.unsubscribeWS(exchange, symbol)
		s.cfg.Logger.Info("market symbol deactivated by volume filter",
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.Float64("avg_volume_usdt", avg),
			zap.Int("days", days),
			zap.Float64("volume_filter_million", group.volumeFilter),
		)
	}
}

func (s *RealTimeService) persistSymbolListTimeBound(exchange string, sym models.Symbol) {
	if s == nil || s.cfg.DynamicFetcher == nil {
		return
	}
	writer, ok := s.cfg.Store.(iface.OHLCVBoundsWriter)
	if !ok {
		return
	}
	listTime := sym.ListTime
	if listTime <= 0 {
		fetcher, ok := s.cfg.DynamicFetcher.(iface.SymbolListTimeFetcher)
		if !ok {
			return
		}
		ts, err := fetcher.FetchSymbolListTime(s.requestContext(), exchange, sym.Symbol)
		if err != nil {
			s.cfg.Logger.Warn("fetch symbol listTime failed",
				zap.String("exchange", exchange),
				zap.String("symbol", sym.Symbol),
				zap.Error(err),
			)
			return
		}
		listTime = ts
	}
	if listTime <= 0 {
		return
	}
	if err := writer.UpsertOHLCVBound(exchange, sym.Symbol, listTime); err != nil {
		s.cfg.Logger.Warn("persist symbol ohlcv bound failed",
			zap.String("exchange", exchange),
			zap.String("symbol", sym.Symbol),
			zap.Int64("earliest_available_ts", listTime),
			zap.Error(err),
		)
	}
}

func (s *RealTimeService) avgDailyVolumeUSDT(exchange, symbol string) (float64, int, error) {
	limit := volumeLookbackDays + 1
	volumes, err := s.cfg.DynamicFetcher.FetchDailyVolumesUSDT(s.requestContext(), exchange, symbol, limit)
	if err != nil {
		return 0, 0, err
	}
	if len(volumes) == 0 {
		return 0, 0, nil
	}
	if len(volumes) > volumeLookbackDays {
		volumes = volumes[len(volumes)-volumeLookbackDays:]
	}
	var sum float64
	for _, v := range volumes {
		sum += v
	}
	avg := sum / float64(len(volumes))
	return avg, len(volumes), nil
}

func (s *RealTimeService) exchangeRate(ex models.Exchange) time.Duration {
	if ex.RateLimitMS > 0 {
		return time.Duration(ex.RateLimitMS) * time.Millisecond
	}
	return s.cfg.Interval
}

func (s *RealTimeService) nextSeq(exchange, symbol, timeframe string) int64 {
	key := s.stateKey(exchange, symbol, timeframe)
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	s.seq[key]++
	return s.seq[key]
}

func (s *RealTimeService) stateKey(exchange, symbol, timeframe string) string {
	return exchange + "|" + symbol + "|" + timeframe
}

func (s *RealTimeService) stopped() bool {
	if s.ctx != nil {
		select {
		case <-s.ctx.Done():
			return true
		default:
		}
	}
	select {
	case <-s.stopCh:
		return true
	default:
		return false
	}
}

func (s *RealTimeService) requestContext() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *RealTimeService) handleOHLCV(handler iface.MarketHandler, data models.MarketData) {
	if !s.cfg.FetchUnclosedOHLCV && !data.Closed {
		return
	}
	handler(data)
}

func (s *RealTimeService) runWS(handler iface.MarketHandler) {
	defer s.wg.Done()
	events := s.cfg.WS.Events()
	errs := s.cfg.WS.Errors()
	for {
		select {
		case <-s.stopCh:
			return
		case data, ok := <-events:
			if !ok {
				return
			}
			if data.Timeframe == "" {
				data.Timeframe = s.cfg.Timeframe
			}
			state := s.lookupState(data.Exchange, data.Symbol, data.Timeframe)
			if state == nil {
				continue
			}
			if !state.active.Load() {
				continue
			}
			if !s.exchangeReady(data.Exchange) {
				continue
			}
			state.lastWSAt.Store(time.Now().UnixNano())
			data.Seq = s.nextSeq(data.Exchange, data.Symbol, data.Timeframe)
			if data.Source == "" {
				data.Source = "ws"
			}
			state.storeRuntimeData(data)
			s.handleOHLCV(handler, data)
		case err, ok := <-errs:
			if !ok {
				return
			}
			if s.stopped() {
				return
			}
			if err != nil {
				if s.handleDelistedWSError(err) {
					continue
				}
				s.cfg.Logger.Warn("market ws error", zap.Error(err))
				s.handleWSError(err)
			}
		}
	}
}

func (s *RealTimeService) handleDelistedSymbol(task *symbolState, err error) bool {
	if task == nil || err == nil {
		return false
	}
	if !IsInvalidSymbolError(task.exchange, err) {
		return false
	}
	return s.deactivateSymbol(task.exchange, task.symbol, err)
}

func (s *RealTimeService) handleDelistedWSError(err error) bool {
	if err == nil {
		return false
	}
	exchange, symbol := parseDelistedSymbolFromWSError(err)
	if exchange == "" || symbol == "" {
		return false
	}
	if !isDelistedSymbolError(exchange, err) {
		return false
	}
	return s.deactivateSymbol(exchange, symbol, err)
}

func (s *RealTimeService) handleWSError(err error) {
	if s == nil || err == nil {
		return
	}
	exchange := inferExchangeFromWSError(err)
	if exchange == "" {
		return
	}
	if until, ok := PauseExchangeOnError(exchange, err); ok {
		s.cfg.Logger.Warn("market exchange paused",
			zap.String("exchange", exchange),
			zap.Time("paused_until", until),
			zap.Duration("resume_in", time.Until(until)),
		)
	}
}

func (s *RealTimeService) deactivateSymbol(exchange, symbol string, err error) bool {
	if exchange == "" || symbol == "" {
		return false
	}
	dynamic, active, found := s.symbolStateSnapshot(exchange, symbol)
	if found && !active {
		return true
	}
	if group := s.lookupGroup(exchange); group != nil {
		group.setSymbolActive(symbol, false, dynamic)
	}
	s.unsubscribeWS(exchange, symbol)
	if s.cfg.Store != nil {
		if updateErr := s.cfg.Store.UpdateSymbolActive(exchange, symbol, false); updateErr != nil {
			s.cfg.Logger.Warn("deactivate symbol failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.Error(updateErr),
			)
		}
	}
	s.cfg.Logger.Warn("market symbol deactivated by exchange error",
		zap.String("exchange", exchange),
		zap.String("symbol", symbol),
		zap.Error(err),
	)
	return true
}

func (s *RealTimeService) symbolStateSnapshot(exchange, symbol string) (dynamic bool, active bool, found bool) {
	group := s.lookupGroup(exchange)
	if group == nil {
		return false, false, false
	}
	states := group.symbolStates(symbol)
	if len(states) == 0 {
		return false, false, false
	}
	state := states[0]
	return state.dynamic, state.active.Load(), true
}

func isDelistedSymbolError(exchange string, err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	switch strings.ToLower(exchange) {
	case "okx":
		return (strings.Contains(msg, "instrument id") || strings.Contains(msg, "instid")) &&
			(strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "does not exist"))
	case "binance":
		return strings.Contains(msg, "invalid symbol") ||
			(strings.Contains(msg, "symbol") && (strings.Contains(msg, "not exist") || strings.Contains(msg, "does not exist")))
	case "bitget":
		return strings.Contains(msg, "symbol") &&
			(strings.Contains(msg, "not exist") || strings.Contains(msg, "does not exist") || strings.Contains(msg, "doesn't exist") || strings.Contains(msg, "invalid"))
	case "hyperliquid":
		return strings.Contains(msg, "unknown coin") ||
			(strings.Contains(msg, "coin") && (strings.Contains(msg, "not found") || strings.Contains(msg, "not exist") || strings.Contains(msg, "does not exist")))
	default:
		return false
	}
}

func parseDelistedSymbolFromWSError(err error) (string, string) {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "okx ws error"):
		instID := parseInstIDFromError(err.Error())
		if instID == "" {
			return "", ""
		}
		return "okx", okxSymbolFromInstID(instID)
	case strings.Contains(msg, "bitget ws error"):
		instID := parseValueFromError(err.Error(), "instid:", "instid=", "instId:", "instId=")
		if instID == "" {
			return "", ""
		}
		return "bitget", bitgetSymbolFromInstID(instID)
	case strings.Contains(msg, "hyperliquid ws error"):
		coin := parseValueFromError(err.Error(), "coin:", "coin=")
		if coin == "" {
			return "", ""
		}
		return "hyperliquid", hyperliquidSymbolFromCoin(coin)
	case strings.Contains(msg, "binance ws error"):
		symbol := parseValueFromError(err.Error(), "symbol:", "symbol=")
		if symbol == "" {
			return "", ""
		}
		return "binance", binanceNormalizeSymbol(symbol)
	default:
		return "", ""
	}
}

func inferExchangeFromWSError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "okx ws"):
		return "okx"
	case strings.Contains(msg, "bitget ws"):
		return "bitget"
	case strings.Contains(msg, "hyperliquid ws"):
		return "hyperliquid"
	case strings.Contains(msg, "binance ws"):
		return "binance"
	default:
		return ""
	}
}

func parseInstIDFromError(raw string) string {
	return parseValueFromError(raw, "instid:", "instid=", "instId:", "instId=")
}

func parseValueFromError(raw string, keys ...string) string {
	lower := strings.ToLower(raw)
	for _, key := range keys {
		keyLower := strings.ToLower(key)
		idx := strings.Index(lower, keyLower)
		if idx == -1 {
			continue
		}
		rest := raw[idx+len(key):]
		rest = strings.TrimLeft(rest, " ")
		if rest == "" {
			continue
		}
		end := len(rest)
		for i, r := range rest {
			switch r {
			case ' ', ',', ';', ')':
				end = i
				goto found
			}
		}
	found:
		return strings.TrimSpace(rest[:end])
	}
	return ""
}

func (s *RealTimeService) shouldSkipREST(task *symbolState) bool {
	if !s.wsEnabledFor(task.exchange) {
		return false
	}
	if !task.wsSubscribed.Load() {
		return false
	}
	last := task.lastWSAt.Load()
	if last == 0 {
		return false
	}
	threshold := s.wsStaleThreshold(task.exchange, task.timeframe)
	if threshold <= 0 {
		return false
	}
	return time.Since(time.Unix(0, last)) <= threshold
}

func (s *RealTimeService) wsEnabledFor(exchange string) bool {
	if !s.wsEnabled || s.cfg.WS == nil || !s.cfg.WS.SupportsExchange(exchange) {
		return false
	}
	return s.lookupGroup(exchange) != nil
}

func (s *RealTimeService) subscribeWS(exchange, symbol string) {
	if !s.wsEnabledFor(exchange) {
		return
	}
	group := s.lookupGroup(exchange)
	if group == nil {
		return
	}
	states := group.symbolStates(symbol)
	if len(states) == 0 {
		timeframes := group.timeframes
		if len(timeframes) == 0 {
			timeframes = []string{s.cfg.Timeframe}
		}
		for _, timeframe := range timeframes {
			if err := s.cfg.WS.Subscribe(s.requestContext(), exchange, symbol, timeframe); err != nil {
				s.cfg.Logger.Warn("market ws subscribe failed",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.String("timeframe", timeframe),
					zap.Error(err),
				)
				continue
			}
		}
		return
	}
	for _, state := range states {
		timeframe := state.timeframe
		if err := s.cfg.WS.Subscribe(s.requestContext(), exchange, symbol, timeframe); err != nil {
			s.cfg.Logger.Warn("market ws subscribe failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Error(err),
			)
			continue
		}
		state.wsSubscribed.Store(true)
	}
}

func (s *RealTimeService) unsubscribeWS(exchange, symbol string) {
	if !s.wsEnabledFor(exchange) {
		return
	}
	group := s.lookupGroup(exchange)
	if group == nil {
		return
	}
	states := group.symbolStates(symbol)
	if len(states) == 0 {
		timeframes := group.timeframes
		if len(timeframes) == 0 {
			timeframes = []string{s.cfg.Timeframe}
		}
		for _, timeframe := range timeframes {
			if err := s.cfg.WS.Unsubscribe(s.requestContext(), exchange, symbol, timeframe); err != nil {
				s.cfg.Logger.Warn("market ws unsubscribe failed",
					zap.String("exchange", exchange),
					zap.String("symbol", symbol),
					zap.String("timeframe", timeframe),
					zap.Error(err),
				)
				continue
			}
		}
		return
	}
	for _, state := range states {
		timeframe := state.timeframe
		if err := s.cfg.WS.Unsubscribe(s.requestContext(), exchange, symbol, timeframe); err != nil {
			s.cfg.Logger.Warn("market ws unsubscribe failed",
				zap.String("exchange", exchange),
				zap.String("symbol", symbol),
				zap.String("timeframe", timeframe),
				zap.Error(err),
			)
			continue
		}
		state.wsSubscribed.Store(false)
	}
}

func (s *RealTimeService) lookupGroup(exchange string) *exchangeGroup {
	s.groupsMu.RLock()
	group := s.groups[exchange]
	s.groupsMu.RUnlock()
	return group
}

func (s *RealTimeService) lookupState(exchange, symbol, timeframe string) *symbolState {
	group := s.lookupGroup(exchange)
	if group == nil {
		return nil
	}
	return group.stateByKey(symbol, timeframe)
}

func isOHLCVClosed(ts int64, timeframe string, now time.Time) bool {
	if ts <= 0 {
		return true
	}
	dur, ok := timeframeDuration(timeframe)
	if !ok || dur <= 0 {
		return true
	}
	end := ts + dur.Milliseconds()
	return now.UnixMilli() >= end
}

func defaultWSStaleThreshold(timeframe string) time.Duration {
	if d, ok := timeframeDuration(timeframe); ok {
		return 2 * d
	}
	return 2 * time.Minute
}

func (s *RealTimeService) wsStaleThreshold(exchange, timeframe string) time.Duration {
	if s == nil {
		return defaultWSStaleThreshold(timeframe)
	}
	if s.wsThresholdByExchange != nil {
		if v, ok := s.wsThresholdByExchange[normalizeExchangeName(exchange)]; ok && v > 0 {
			return v
		}
	}
	if s.cfg.WSStaleThreshold > 0 {
		return s.cfg.WSStaleThreshold
	}
	return defaultWSStaleThreshold(timeframe)
}

func normalizeWSStaleThresholds(in map[string]time.Duration) map[string]time.Duration {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]time.Duration, len(in))
	for exchange, threshold := range in {
		exchange = normalizeExchangeName(exchange)
		if exchange == "" || threshold <= 0 {
			continue
		}
		out[exchange] = threshold
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseExchangeTimeframes(raw, fallback string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallbackTimeframes(fallback), nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	normalized := make([]string, 0, len(out))
	seen := make(map[string]bool)
	for _, item := range out {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		normalized = append(normalized, item)
	}
	if len(normalized) == 0 {
		return fallbackTimeframes(fallback), nil
	}
	return normalized, nil
}

func parseSymbolTimeframes(raw string, fallback []string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	normalized := make([]string, 0, len(out))
	seen := make(map[string]bool)
	for _, item := range out {
		item = strings.TrimSpace(item)
		if item == "" || seen[item] {
			continue
		}
		seen[item] = true
		normalized = append(normalized, item)
	}
	if len(normalized) == 0 {
		return fallback, nil
	}
	return normalized, nil
}

func fallbackTimeframes(fallback string) []string {
	if fallback == "" {
		return nil
	}
	return []string{fallback}
}
