package market

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
)

var ErrEmptyOHLCV = errors.New("empty ohlcv data")

type MarketErrorKind string

const (
	MarketErrorRateLimit     MarketErrorKind = "rate_limit"
	MarketErrorIPBan         MarketErrorKind = "ip_ban"
	MarketErrorTemporary     MarketErrorKind = "temporary"
	MarketErrorInvalidSymbol MarketErrorKind = "invalid_symbol"
)

type MarketErrorScope string

const (
	MarketErrorScopeExchange MarketErrorScope = "exchange"
	MarketErrorScopeEndpoint MarketErrorScope = "endpoint"
)

type MarketError struct {
	Kind       MarketErrorKind
	Scope      MarketErrorScope
	Exchange   string
	Endpoint   string
	RetryAfter time.Duration
	Message    string
	Cause      error
}

func (e *MarketError) Error() string {
	if e == nil {
		return ""
	}
	message := strings.TrimSpace(e.Message)
	if message == "" && e.Cause != nil {
		message = e.Cause.Error()
	}
	if message == "" {
		message = string(e.Kind)
	}
	if e.Exchange != "" && e.Endpoint != "" {
		return fmt.Sprintf("%s %s/%s: %s", e.Kind, e.Exchange, e.Endpoint, message)
	}
	if e.Exchange != "" {
		return fmt.Sprintf("%s %s: %s", e.Kind, e.Exchange, message)
	}
	return fmt.Sprintf("%s: %s", e.Kind, message)
}

func (e *MarketError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func (e *MarketError) Retriable() bool {
	if e == nil {
		return false
	}
	switch e.Kind {
	case MarketErrorRateLimit, MarketErrorIPBan, MarketErrorTemporary:
		return true
	default:
		return false
	}
}

func AsMarketError(err error) (*MarketError, bool) {
	if err == nil {
		return nil, false
	}
	var target *MarketError
	if errors.As(err, &target) {
		return target, true
	}
	return nil, false
}

func IsRetriableMarketError(err error) bool {
	me, ok := AsMarketError(err)
	if !ok {
		return false
	}
	return me.Retriable()
}

func IsInvalidSymbolError(exchange string, err error) bool {
	if err == nil {
		return false
	}
	if me, ok := AsMarketError(err); ok {
		return me.Kind == MarketErrorInvalidSymbol
	}
	return isDelistedSymbolError(exchange, err)
}

func ClassifyMarketError(exchange, endpoint string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := AsMarketError(err); ok {
		return err
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &MarketError{
			Kind:     MarketErrorTemporary,
			Scope:    MarketErrorScopeExchange,
			Exchange: normalizeExchangeName(exchange),
			Endpoint: endpoint,
			Cause:    err,
		}
	}
	if netErr := asNetError(err); netErr != nil && (netErr.Timeout() || netErr.Temporary()) {
		return &MarketError{
			Kind:     MarketErrorTemporary,
			Scope:    MarketErrorScopeExchange,
			Exchange: normalizeExchangeName(exchange),
			Endpoint: endpoint,
			Cause:    err,
		}
	}

	if isDelistedSymbolError(exchange, err) {
		return &MarketError{
			Kind:     MarketErrorInvalidSymbol,
			Scope:    MarketErrorScopeEndpoint,
			Exchange: normalizeExchangeName(exchange),
			Endpoint: endpoint,
			Cause:    err,
		}
	}

	message := strings.TrimSpace(err.Error())
	if message != "" {
		if until, ok := parseBinanceBanUntil(message); ok {
			retry := time.Until(until)
			if retry < 0 {
				retry = 0
			}
			return &MarketError{
				Kind:       MarketErrorIPBan,
				Scope:      MarketErrorScopeExchange,
				Exchange:   normalizeExchangeName(exchange),
				Endpoint:   endpoint,
				RetryAfter: retry,
				Message:    message,
				Cause:      err,
			}
		}
		if rateLimitRe.MatchString(message) {
			return &MarketError{
				Kind:     MarketErrorRateLimit,
				Scope:    MarketErrorScopeExchange,
				Exchange: normalizeExchangeName(exchange),
				Endpoint: endpoint,
				Message:  message,
				Cause:    err,
			}
		}
		if looksLikeInvalidSymbol(message) {
			return &MarketError{
				Kind:     MarketErrorInvalidSymbol,
				Scope:    MarketErrorScopeEndpoint,
				Exchange: normalizeExchangeName(exchange),
				Endpoint: endpoint,
				Message:  message,
				Cause:    err,
			}
		}
	}
	return err
}

func asNetError(err error) net.Error {
	if err == nil {
		return nil
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr
	}
	return nil
}

func looksLikeInvalidSymbol(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "invalid symbol") ||
		(strings.Contains(msg, "symbol") &&
			(strings.Contains(msg, "not exist") ||
				strings.Contains(msg, "does not exist") ||
				strings.Contains(msg, "doesn't exist") ||
				strings.Contains(msg, "unknown coin") ||
				strings.Contains(msg, "not found")))
}
