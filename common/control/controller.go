package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const defaultQueueSize = 2048

type Rule struct {
	Scope       string
	Endpoint    string
	MinInterval time.Duration
	MaxRequests int
	Window      time.Duration
}

type Meta struct {
	Scope    string
	Endpoint string
}

type Config struct {
	DefaultInterval time.Duration
	ScopeIntervals  map[string]time.Duration
	Rules           []Rule
	QueueSize       int
}

type Controller struct {
	defaultInterval time.Duration
	scopeIntervals  map[string]time.Duration
	rules           map[ruleKey]Rule
	queueSize       int

	mu    sync.Mutex
	lanes map[string]*lane
}

func NewController(cfg Config) *Controller {
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	return &Controller{
		defaultInterval: cfg.DefaultInterval,
		scopeIntervals:  normalizeScopeIntervals(cfg.ScopeIntervals),
		rules:           normalizeRules(cfg.Rules),
		queueSize:       queueSize,
		lanes:           make(map[string]*lane),
	}
}

func (c *Controller) Do(ctx context.Context, meta Meta, fn func(context.Context) error) error {
	if fn == nil {
		return errors.New("nil request handler")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	scope := normalizeScope(meta.Scope)
	if scope == "" {
		return fn(ctx)
	}
	meta.Scope = scope
	task := task{
		ctx:    ctx,
		meta:   meta,
		handle: fn,
		done:   make(chan error, 1),
	}
	l := c.lane(scope)
	select {
	case l.tasks <- &task:
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

func (c *Controller) DebugState() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("controller lanes=%d", len(c.lanes))
}

func (c *Controller) lane(scope string) *lane {
	c.mu.Lock()
	defer c.mu.Unlock()
	if l, ok := c.lanes[scope]; ok {
		return l
	}
	l := newLane(scope, c, c.queueSize)
	c.lanes[scope] = l
	go l.run()
	return l
}

type ruleKey struct {
	scope    string
	endpoint string
}

func normalizeRules(rules []Rule) map[ruleKey]Rule {
	if len(rules) == 0 {
		return nil
	}
	out := make(map[ruleKey]Rule, len(rules))
	for _, rule := range rules {
		scope := normalizeScope(rule.Scope)
		if scope == "" {
			continue
		}
		endpoint := normalizeEndpoint(rule.Endpoint)
		out[ruleKey{scope: scope, endpoint: endpoint}] = Rule{
			Scope:       scope,
			Endpoint:    endpoint,
			MinInterval: rule.MinInterval,
			MaxRequests: rule.MaxRequests,
			Window:      rule.Window,
		}
	}
	return out
}

func normalizeScopeIntervals(raw map[string]time.Duration) map[string]time.Duration {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]time.Duration, len(raw))
	for scope, interval := range raw {
		scope = normalizeScope(scope)
		if scope == "" || interval <= 0 {
			continue
		}
		out[scope] = interval
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeScope(scope string) string {
	return strings.ToLower(strings.TrimSpace(scope))
}

func normalizeEndpoint(endpoint string) string {
	return strings.ToLower(strings.TrimSpace(endpoint))
}

type task struct {
	ctx    context.Context
	meta   Meta
	handle func(context.Context) error
	done   chan error
}

type lane struct {
	scope    string
	ctrl     *Controller
	tasks    chan *task
	limiters map[string]*limiter
}

func newLane(scope string, ctrl *Controller, queueSize int) *lane {
	if queueSize <= 0 {
		queueSize = defaultQueueSize
	}
	return &lane{
		scope:    scope,
		ctrl:     ctrl,
		tasks:    make(chan *task, queueSize),
		limiters: make(map[string]*limiter),
	}
}

func (l *lane) run() {
	for task := range l.tasks {
		err := l.handleTask(task)
		task.done <- err
		close(task.done)
	}
}

func (l *lane) handleTask(task *task) error {
	if task == nil {
		return nil
	}
	if task.ctx != nil && task.ctx.Err() != nil {
		return task.ctx.Err()
	}
	if err := l.waitRateLimit(task.ctx, task.meta); err != nil {
		return err
	}
	return task.handle(task.ctx)
}

func (l *lane) waitRateLimit(ctx context.Context, meta Meta) error {
	limiter := l.limiterFor(meta)
	if limiter == nil {
		return nil
	}
	return limiter.Wait(ctx)
}

func (l *lane) limiterFor(meta Meta) *limiter {
	endpoint := normalizeEndpoint(meta.Endpoint)
	if limiter, ok := l.limiters[endpoint]; ok {
		return limiter
	}
	rule := l.ctrl.ruleFor(meta.Scope, endpoint)
	limiter := newLimiter(rule)
	l.limiters[endpoint] = limiter
	return limiter
}

func (c *Controller) ruleFor(scope, endpoint string) Rule {
	scope = normalizeScope(scope)
	endpoint = normalizeEndpoint(endpoint)
	if scope == "" {
		return Rule{}
	}
	if rule, ok := c.rules[ruleKey{scope: scope, endpoint: endpoint}]; ok {
		return fillRuleDefaults(rule, c.defaultInterval, c.scopeIntervals[scope])
	}
	if rule, ok := c.rules[ruleKey{scope: scope, endpoint: ""}]; ok {
		return fillRuleDefaults(rule, c.defaultInterval, c.scopeIntervals[scope])
	}
	return fillRuleDefaults(Rule{Scope: scope, Endpoint: endpoint}, c.defaultInterval, c.scopeIntervals[scope])
}

func fillRuleDefaults(rule Rule, defaultInterval, scopeInterval time.Duration) Rule {
	if rule.MinInterval <= 0 {
		if scopeInterval > 0 {
			rule.MinInterval = scopeInterval
		} else if defaultInterval > 0 {
			rule.MinInterval = defaultInterval
		}
	}
	return rule
}

type limiter struct {
	minInterval time.Duration
	maxRequests int
	window      time.Duration
	last        time.Time
	recent      []time.Time
}

func newLimiter(rule Rule) *limiter {
	if rule.MinInterval <= 0 && (rule.MaxRequests <= 0 || rule.Window <= 0) {
		return nil
	}
	return &limiter{
		minInterval: rule.MinInterval,
		maxRequests: rule.MaxRequests,
		window:      rule.Window,
	}
}

func (l *limiter) Wait(ctx context.Context) error {
	if l == nil {
		return nil
	}
	now := time.Now().UTC()
	delay := l.nextDelay(now)
	if delay > 0 {
		if err := sleepWithContext(ctx, delay); err != nil {
			return err
		}
		now = time.Now().UTC()
	}
	l.last = now
	if l.maxRequests > 0 && l.window > 0 {
		l.recent = append(l.recent, now)
	}
	return nil
}

func (l *limiter) nextDelay(now time.Time) time.Duration {
	var delay time.Duration
	if l.minInterval > 0 && !l.last.IsZero() {
		wait := l.minInterval - now.Sub(l.last)
		if wait > delay {
			delay = wait
		}
	}
	if l.maxRequests > 0 && l.window > 0 {
		l.recent = trimWindow(l.recent, now, l.window)
		if len(l.recent) >= l.maxRequests {
			oldest := l.recent[0]
			wait := l.window - now.Sub(oldest)
			if wait > delay {
				delay = wait
			}
		}
	}
	if delay < 0 {
		return 0
	}
	return delay
}

func trimWindow(items []time.Time, now time.Time, window time.Duration) []time.Time {
	if len(items) == 0 {
		return items
	}
	cutoff := now.Add(-window)
	idx := 0
	for idx < len(items) && items[idx].Before(cutoff) {
		idx++
	}
	if idx == 0 {
		return items
	}
	out := items[:0]
	out = append(out, items[idx:]...)
	return out
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
