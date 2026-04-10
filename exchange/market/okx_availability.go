package market

import (
	"strings"
	"sync"
	"time"
)

type okxAvailabilityCache struct {
	mu        sync.Mutex
	notBefore map[string]int64
}

func newOKXAvailabilityCache() *okxAvailabilityCache {
	return &okxAvailabilityCache{notBefore: make(map[string]int64)}
}

func okxAvailabilityKey(symbol, timeframe string) string {
	symbol = strings.ToLower(strings.TrimSpace(symbol))
	timeframe = strings.ToLower(strings.TrimSpace(timeframe))
	if symbol == "" || timeframe == "" {
		return ""
	}
	return symbol + "|" + timeframe
}

func (c *okxAvailabilityCache) get(symbol, timeframe string) (time.Time, bool) {
	if c == nil {
		return time.Time{}, false
	}
	key := okxAvailabilityKey(symbol, timeframe)
	if key == "" {
		return time.Time{}, false
	}
	c.mu.Lock()
	value, ok := c.notBefore[key]
	c.mu.Unlock()
	if !ok || value <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(value).UTC(), true
}

func (c *okxAvailabilityCache) observe(symbol, timeframe string, ts int64) {
	if c == nil || ts <= 0 {
		return
	}
	key := okxAvailabilityKey(symbol, timeframe)
	if key == "" {
		return
	}
	ts = normalizeTimestampMS(ts)
	if ts <= 0 {
		return
	}
	c.mu.Lock()
	current := c.notBefore[key]
	if ts > current {
		c.notBefore[key] = ts
	}
	c.mu.Unlock()
}

var okxAvailability = newOKXAvailabilityCache()
