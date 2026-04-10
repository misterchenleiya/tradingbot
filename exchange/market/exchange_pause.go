package market

import (
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type exchangePauseRegistry struct {
	mu    sync.RWMutex
	until map[string]time.Time
}

func newExchangePauseRegistry() *exchangePauseRegistry {
	return &exchangePauseRegistry{
		until: make(map[string]time.Time),
	}
}

func normalizeExchangeName(exchange string) string {
	return strings.ToLower(strings.TrimSpace(exchange))
}

func (r *exchangePauseRegistry) set(exchange string, until time.Time) bool {
	if r == nil {
		return false
	}
	ex := normalizeExchangeName(exchange)
	if ex == "" {
		return false
	}
	if until.IsZero() {
		return false
	}
	now := time.Now().UTC()
	if !until.After(now) {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.until[ex]
	if ok && !until.After(current) {
		return false
	}
	r.until[ex] = until
	return true
}

func (r *exchangePauseRegistry) get(exchange string, now time.Time) (time.Time, bool) {
	if r == nil {
		return time.Time{}, false
	}
	ex := normalizeExchangeName(exchange)
	if ex == "" {
		return time.Time{}, false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.until[ex]
	if !ok {
		return time.Time{}, false
	}
	if !until.After(now) {
		delete(r.until, ex)
		return time.Time{}, false
	}
	return until, true
}

func (r *exchangePauseRegistry) list(now time.Time) map[string]time.Time {
	if r == nil {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.until) == 0 {
		return nil
	}
	out := make(map[string]time.Time, len(r.until))
	for exchange, until := range r.until {
		if !until.After(now) {
			delete(r.until, exchange)
			continue
		}
		out[exchange] = until
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

var exchangePauses = newExchangePauseRegistry()

const (
	defaultRateLimitPause = time.Minute
	defaultIPBanPause     = 15 * time.Minute
)

func PauseExchangeUntil(exchange string, until time.Time) bool {
	return exchangePauses.set(exchange, until)
}

func ExchangePaused(exchange string, now time.Time) (time.Time, bool) {
	return exchangePauses.get(exchange, now)
}

func ExchangePauses(now time.Time) map[string]time.Time {
	return exchangePauses.list(now)
}

var binanceBanUntilRe = regexp.MustCompile(`banned until\s+(\d+)`)
var rateLimitRe = regexp.MustCompile(`(?i)too many requests|rate limit|request too frequent|status 429|http 429`)

func handleBinanceBan(message string) bool {
	until, ok := parseBinanceBanUntil(message)
	if !ok {
		return false
	}
	return PauseExchangeUntil("binance", until)
}

func parseBinanceBanUntil(message string) (time.Time, bool) {
	if message == "" {
		return time.Time{}, false
	}
	match := binanceBanUntilRe.FindStringSubmatch(strings.ToLower(message))
	if len(match) < 2 {
		return time.Time{}, false
	}
	ts, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || ts <= 0 {
		return time.Time{}, false
	}
	ms := normalizeTimestampMS(ts)
	if ms <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(ms).UTC(), true
}

func PauseExchangeOnError(exchange string, err error) (time.Time, bool) {
	if err == nil {
		return time.Time{}, false
	}
	if me, ok := AsMarketError(err); ok {
		ex := exchange
		if me.Exchange != "" {
			ex = me.Exchange
		}
		if ex == "" {
			return time.Time{}, false
		}
		now := time.Now().UTC()
		switch me.Kind {
		case MarketErrorIPBan:
			delay := me.RetryAfter
			if delay <= 0 {
				delay = defaultIPBanPause
			}
			until := now.Add(delay)
			PauseExchangeUntil(ex, until)
			return until, true
		case MarketErrorRateLimit:
			delay := me.RetryAfter
			if delay <= 0 {
				delay = defaultRateLimitPause
			}
			until := now.Add(delay)
			PauseExchangeUntil(ex, until)
			return until, true
		default:
		}
	}
	now := time.Now().UTC()
	if until, ok := ExchangePaused(exchange, now); ok {
		return until, true
	}
	message := strings.TrimSpace(err.Error())
	if message == "" {
		return time.Time{}, false
	}
	if until, ok := parseBinanceBanUntil(message); ok {
		PauseExchangeUntil(exchange, until)
		return until, true
	}
	if rateLimitRe.MatchString(message) {
		until := now.Add(defaultRateLimitPause)
		PauseExchangeUntil(exchange, until)
		return until, true
	}
	return time.Time{}, false
}
