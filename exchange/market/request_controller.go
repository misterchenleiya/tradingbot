package market

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/common/control"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const defaultRequestQueueSize = 2048

type APILimitRule struct {
	Exchange    string
	Endpoint    string
	MinInterval time.Duration
	MaxRequests int
	Window      time.Duration
}

// OKXAPILimitRules returns the endpoint-level rules currently used by gobot
// for OKX public market APIs.
func OKXAPILimitRules() []APILimitRule {
	return []APILimitRule{
		// /api/v5/market/candles: 40 requests / 2 seconds (IP)
		{Exchange: "okx", Endpoint: EndpointOHLCVLatest, MinInterval: 50 * time.Millisecond, MaxRequests: 40, Window: 2 * time.Second},
		// /api/v5/market/candles (1D volume lookup): 40 requests / 2 seconds (IP)
		{Exchange: "okx", Endpoint: EndpointDailyVolumes, MinInterval: 50 * time.Millisecond, MaxRequests: 40, Window: 2 * time.Second},
		// /api/v5/market/history-candles: 20 requests / 2 seconds (IP)
		{Exchange: "okx", Endpoint: EndpointOHLCVRange, MinInterval: 100 * time.Millisecond, MaxRequests: 20, Window: 2 * time.Second},
		// /api/v5/public/instruments: 20 requests / 2 seconds (IP + instType)
		{Exchange: "okx", Endpoint: EndpointMarkets, MinInterval: 100 * time.Millisecond, MaxRequests: 20, Window: 2 * time.Second},
	}
}

// BinanceAPILimitRules returns the endpoint-level rules currently used by gobot
// for Binance USD-M public market APIs and WS stream subscriptions.
func BinanceAPILimitRules() []APILimitRule {
	return []APILimitRule{
		// /fapi/v1/klines (range mode, limit=1500): keep heavy (weight=10) requests bounded.
		{Exchange: "binance", Endpoint: EndpointOHLCVRange, MaxRequests: 100, Window: time.Minute},
		// /fapi/v1/klines (latest, limit=1): lightweight requests.
		{Exchange: "binance", Endpoint: EndpointOHLCVLatest, MaxRequests: 1400, Window: time.Minute},
		// /fapi/v1/klines (1d volume lookup): lightweight requests.
		{Exchange: "binance", Endpoint: EndpointDailyVolumes, MaxRequests: 1400, Window: time.Minute},
		// /fapi/v1/exchangeInfo
		{Exchange: "binance", Endpoint: EndpointMarkets, MaxRequests: 120, Window: time.Minute},
		// WS stream subscription controls (incoming messages per connection).
		{Exchange: "binance", Endpoint: EndpointWSSubscribe, MinInterval: 125 * time.Millisecond, MaxRequests: 8, Window: time.Second},
		{Exchange: "binance", Endpoint: EndpointWSUnsubscribe, MinInterval: 125 * time.Millisecond, MaxRequests: 8, Window: time.Second},
	}
}

// DefaultAPILimitRules returns the default market API rules used by gobot.
func DefaultAPILimitRules() []APILimitRule {
	rules := append([]APILimitRule{}, OKXAPILimitRules()...)
	rules = append(rules, BinanceAPILimitRules()...)
	return rules
}

// APILimitRulesExample returns example rules for manual tuning.
// These rules are not applied unless you pass them into RequestControllerConfig.
func APILimitRulesExample() []APILimitRule {
	rules := append([]APILimitRule{}, DefaultAPILimitRules()...)
	rules = append(rules, APILimitRule{Exchange: "okx", Endpoint: EndpointWSSubscribe, MaxRequests: 6, Window: time.Second})
	return rules
}

type RequestControllerConfig struct {
	Logger            *zap.Logger
	DefaultInterval   time.Duration
	ExchangeIntervals map[string]time.Duration
	APIRules          []APILimitRule
	QueueSize         int
}

type RequestController struct {
	logger    *zap.Logger
	queueSize int
	throttle  *control.Controller

	mu    sync.Mutex
	lanes map[string]*requestLane
}

func NewRequestController(cfg RequestControllerConfig) *RequestController {
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = defaultRequestQueueSize
	}
	return &RequestController{
		logger:    logger,
		queueSize: queueSize,
		throttle: control.NewController(control.Config{
			DefaultInterval: cfg.DefaultInterval,
			ScopeIntervals:  normalizeExchangeIntervals(cfg.ExchangeIntervals),
			Rules:           convertControlRules(cfg.APIRules),
			QueueSize:       queueSize,
		}),
		lanes: make(map[string]*requestLane),
	}
}

func ExchangeIntervalsFrom(exchanges []models.Exchange) map[string]time.Duration {
	if len(exchanges) == 0 {
		return nil
	}
	out := make(map[string]time.Duration, len(exchanges))
	for _, ex := range exchanges {
		if ex.RateLimitMS <= 0 {
			continue
		}
		out[normalizeExchangeName(ex.Name)] = time.Duration(ex.RateLimitMS) * time.Millisecond
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c *RequestController) Do(ctx context.Context, meta RequestMeta, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("nil request handler")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	exchange := normalizeExchangeName(meta.Exchange)
	if exchange == "" {
		return fn(ctx)
	}
	meta.Exchange = exchange
	task := requestTask{
		ctx:    ctx,
		meta:   meta,
		handle: fn,
		done:   make(chan error, 1),
	}
	lane := c.lane(exchange)
	select {
	case lane.tasks <- &task:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-task.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *RequestController) lane(exchange string) *requestLane {
	c.mu.Lock()
	defer c.mu.Unlock()
	if lane, ok := c.lanes[exchange]; ok {
		return lane
	}
	lane := newRequestLane(exchange, c, c.queueSize)
	c.lanes[exchange] = lane
	go lane.run()
	return lane
}

func convertControlRules(rules []APILimitRule) []control.Rule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]control.Rule, 0, len(rules))
	for _, rule := range rules {
		scope := normalizeExchangeName(rule.Exchange)
		if scope == "" {
			continue
		}
		out = append(out, control.Rule{
			Scope:       scope,
			Endpoint:    normalizeEndpoint(rule.Endpoint),
			MinInterval: rule.MinInterval,
			MaxRequests: rule.MaxRequests,
			Window:      rule.Window,
		})
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeEndpoint(endpoint string) string {
	return strings.ToLower(strings.TrimSpace(endpoint))
}

func normalizeExchangeIntervals(in map[string]time.Duration) map[string]time.Duration {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]time.Duration, len(in))
	for exchange, interval := range in {
		exchange = normalizeExchangeName(exchange)
		if exchange == "" || interval <= 0 {
			continue
		}
		out[exchange] = interval
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

type requestTask struct {
	ctx    context.Context
	meta   RequestMeta
	handle func(context.Context) error
	done   chan error
}

type requestLane struct {
	exchange string
	ctrl     *RequestController
	tasks    chan *requestTask

	exchangeBackoff backoffState
	endpointBackoff map[string]*backoffState
}

func newRequestLane(exchange string, ctrl *RequestController, queueSize int) *requestLane {
	if queueSize <= 0 {
		queueSize = defaultRequestQueueSize
	}
	return &requestLane{
		exchange:        exchange,
		ctrl:            ctrl,
		tasks:           make(chan *requestTask, queueSize),
		endpointBackoff: make(map[string]*backoffState),
	}
}

func (l *requestLane) run() {
	for task := range l.tasks {
		err := l.handleTask(task)
		task.done <- err
		close(task.done)
	}
}

func (l *requestLane) handleTask(task *requestTask) error {
	if task == nil {
		return nil
	}
	if task.ctx != nil && task.ctx.Err() != nil {
		return task.ctx.Err()
	}
	meta := task.meta
	if meta.Endpoint == "" {
		meta.Endpoint = EndpointOHLCVLatest
	}
	for {
		if err := l.waitForPause(task.ctx, meta); err != nil {
			return err
		}
		if err := l.waitRateLimit(task.ctx, meta); err != nil {
			return err
		}
		err := task.handle(task.ctx)
		err = ClassifyMarketError(meta.Exchange, meta.Endpoint, err)
		if err == nil {
			l.resetBackoff(meta)
			return nil
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if task.ctx != nil && task.ctx.Err() != nil {
			return task.ctx.Err()
		}
		if !IsRetriableMarketError(err) {
			return err
		}
		delay := l.nextBackoffDelay(meta, err)
		if delay <= 0 {
			delay = time.Second
		}
		if err := sleepWithContext(task.ctx, delay); err != nil {
			return err
		}
	}
}

func (l *requestLane) waitForPause(ctx context.Context, meta RequestMeta) error {
	now := time.Now().UTC()
	until := time.Time{}
	if exchangeUntil, ok := ExchangePaused(meta.Exchange, now); ok {
		until = exchangeUntil
	}
	if l.exchangeBackoff.until.After(until) {
		until = l.exchangeBackoff.until
	}
	if endpoint := normalizeEndpoint(meta.Endpoint); endpoint != "" {
		if st := l.endpointBackoff[endpoint]; st != nil && st.until.After(until) {
			until = st.until
		}
	}
	if until.IsZero() || !until.After(now) {
		return nil
	}
	wait := time.Until(until)
	return sleepWithContext(ctx, wait)
}

func (l *requestLane) waitRateLimit(ctx context.Context, meta RequestMeta) error {
	if l.ctrl == nil || l.ctrl.throttle == nil {
		return nil
	}
	endpoint := normalizeEndpoint(meta.Endpoint)
	if endpoint == "" {
		endpoint = EndpointOHLCVLatest
	}
	return l.ctrl.throttle.Do(ctx, control.Meta{
		Scope:    meta.Exchange,
		Endpoint: endpoint,
	}, func(context.Context) error {
		return nil
	})
}

func (l *requestLane) backoffStateFor(scope MarketErrorScope, endpoint string) *backoffState {
	if scope == MarketErrorScopeEndpoint {
		endpoint = normalizeEndpoint(endpoint)
		if endpoint == "" {
			endpoint = EndpointOHLCVLatest
		}
		state := l.endpointBackoff[endpoint]
		if state == nil {
			state = &backoffState{}
			l.endpointBackoff[endpoint] = state
		}
		return state
	}
	return &l.exchangeBackoff
}

func (l *requestLane) resetBackoff(meta RequestMeta) {
	if meta.Endpoint == "" {
		meta.Endpoint = EndpointOHLCVLatest
	}
	if st := l.endpointBackoff[normalizeEndpoint(meta.Endpoint)]; st != nil {
		st.reset()
	}
	l.exchangeBackoff.reset()
}

func (l *requestLane) nextBackoffDelay(meta RequestMeta, err error) time.Duration {
	me, ok := AsMarketError(err)
	if !ok {
		return time.Second
	}
	scope := me.Scope
	if scope == "" {
		scope = MarketErrorScopeExchange
	}
	state := l.backoffStateFor(scope, meta.Endpoint)
	delay := defaultBackoffPolicy().delay(me.Kind, state, me.RetryAfter)
	if delay > 0 {
		state.until = time.Now().UTC().Add(delay)
		if scope == MarketErrorScopeExchange && meta.Exchange != "" {
			PauseExchangeUntil(meta.Exchange, state.until)
			l.ctrl.logger.Warn("market exchange paused",
				zap.String("exchange", meta.Exchange),
				zap.String("endpoint", meta.Endpoint),
				zap.Duration("resume_in", delay),
			)
		}
	}
	return delay
}

type backoffState struct {
	attempts int
	until    time.Time
}

func (b *backoffState) reset() {
	if b == nil {
		return
	}
	b.attempts = 0
	b.until = time.Time{}
}

type backoffPolicy struct {
	rateLimitBase time.Duration
	temporaryBase time.Duration
	ipBanBase     time.Duration
	maxExponent   int
}

func defaultBackoffPolicy() backoffPolicy {
	return backoffPolicy{
		rateLimitBase: time.Second,
		temporaryBase: time.Second,
		ipBanBase:     5 * time.Minute,
		maxExponent:   30,
	}
}

func (p backoffPolicy) delay(kind MarketErrorKind, state *backoffState, retryAfter time.Duration) time.Duration {
	if state == nil {
		return retryAfter
	}
	state.attempts++
	base := p.baseForKind(kind)
	if retryAfter > base {
		base = retryAfter
	}
	if base <= 0 {
		base = time.Second
	}
	exp := state.attempts - 1
	if exp < 0 {
		exp = 0
	}
	if exp > p.maxExponent {
		exp = p.maxExponent
	}
	delay := scaleDuration(base, exp)
	if delay < base {
		delay = base
	}
	return delay
}

func (p backoffPolicy) baseForKind(kind MarketErrorKind) time.Duration {
	switch kind {
	case MarketErrorRateLimit:
		return p.rateLimitBase
	case MarketErrorIPBan:
		return p.ipBanBase
	case MarketErrorTemporary:
		return p.temporaryBase
	default:
		return time.Second
	}
}

func scaleDuration(base time.Duration, exp int) time.Duration {
	if exp <= 0 {
		return base
	}
	const max = time.Duration(int64(^uint64(0) >> 1))
	value := base
	for i := 0; i < exp; i++ {
		if value > max/2 {
			return max
		}
		value *= 2
	}
	return value
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timer := time.NewTimer(delay)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *RequestController) DebugState() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	lanes := len(c.lanes)
	c.mu.Unlock()
	if c.throttle == nil {
		return fmt.Sprintf("request_controller lanes=%d", lanes)
	}
	return fmt.Sprintf("request_controller lanes=%d throttle=%s", lanes, c.throttle.DebugState())
}
