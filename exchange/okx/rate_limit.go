package okx

import (
	"net/http"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common/control"
	"github.com/misterchenleiya/tradingbot/exchange"
	"github.com/misterchenleiya/tradingbot/exchange/config"
)

const (
	okxScope                  = "okx"
	defaultRateLimitMS        = 100
	defaultPrivateMinInterval = 100 * time.Millisecond
	defaultPrivateWindow      = 2 * time.Second
	defaultPrivateMaxRequests = 10
	leverageMaxRequests       = 5
)

func newRateController(cfg config.ExchangeConfig) *control.Controller {
	fallback := time.Duration(cfg.RateLimitMS) * time.Millisecond
	if fallback <= 0 {
		fallback = time.Duration(defaultRateLimitMS) * time.Millisecond
	}
	return control.NewController(control.Config{
		ScopeIntervals: map[string]time.Duration{
			okxScope: fallback,
		},
		Rules: okxAPILimitRules(),
	})
}

func okxAPILimitRules() []control.Rule {
	return []control.Rule{
		{
			Scope:       okxScope,
			Endpoint:    exchange.EndpointPublicInstrument,
			MinInterval: 100 * time.Millisecond,
			MaxRequests: 20,
			Window:      2 * time.Second,
		},
		{
			Scope:       okxScope,
			Endpoint:    exchange.EndpointMarketCandles,
			MinInterval: 100 * time.Millisecond,
			MaxRequests: 20,
			Window:      2 * time.Second,
		},
		{
			Scope:       okxScope,
			Endpoint:    exchange.EndpointMarketHistoryCandles,
			MinInterval: 100 * time.Millisecond,
			MaxRequests: 20,
			Window:      2 * time.Second,
		},
		{Scope: okxScope, Endpoint: exchange.EndpointMarketTicker, MinInterval: 50 * time.Millisecond},
		okxPrivateRule(exchange.EndpointAccountConfig, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointSetPositionMode, leverageMaxRequests),
		okxPrivateRule(exchange.EndpointSetLeverage, leverageMaxRequests),
		okxPrivateRule(exchange.EndpointAccountPositions, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointAccountPositionsHistory, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointTradeOrdersAlgoPending, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointTradeCancelAlgos, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointTradeOrderAlgo, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointAccountBalance, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointAssetBalances, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointTradeOrderPlace, defaultPrivateMaxRequests),
		okxPrivateRule(exchange.EndpointTradeOrderGet, defaultPrivateMaxRequests),
	}
}

func okxPrivateRule(endpoint string, maxRequests int) control.Rule {
	if maxRequests <= 0 {
		maxRequests = defaultPrivateMaxRequests
	}
	return control.Rule{
		Scope:       okxScope,
		Endpoint:    endpoint,
		MinInterval: defaultPrivateMinInterval,
		MaxRequests: maxRequests,
		Window:      defaultPrivateWindow,
	}
}

func okxEndpointForPath(method, path string) string {
	path = normalizePath(path)
	switch path {
	case "/api/v5/public/instruments":
		return exchange.EndpointPublicInstrument
	case "/api/v5/market/candles":
		return exchange.EndpointMarketCandles
	case "/api/v5/market/history-candles":
		return exchange.EndpointMarketHistoryCandles
	case "/api/v5/market/ticker":
		return exchange.EndpointMarketTicker
	case "/api/v5/account/config":
		return exchange.EndpointAccountConfig
	case "/api/v5/account/set-position-mode":
		return exchange.EndpointSetPositionMode
	case "/api/v5/account/set-leverage":
		return exchange.EndpointSetLeverage
	case "/api/v5/account/positions":
		return exchange.EndpointAccountPositions
	case "/api/v5/account/positions-history":
		return exchange.EndpointAccountPositionsHistory
	case "/api/v5/trade/orders-algo-pending":
		return exchange.EndpointTradeOrdersAlgoPending
	case "/api/v5/trade/cancel-algos":
		return exchange.EndpointTradeCancelAlgos
	case "/api/v5/trade/order-algo":
		return exchange.EndpointTradeOrderAlgo
	case "/api/v5/account/balance":
		return exchange.EndpointAccountBalance
	case "/api/v5/asset/balances":
		return exchange.EndpointAssetBalances
	case "/api/v5/trade/order":
		if strings.EqualFold(method, http.MethodGet) {
			return exchange.EndpointTradeOrderGet
		}
		if strings.EqualFold(method, http.MethodPost) {
			return exchange.EndpointTradeOrderPlace
		}
		return exchange.EndpointTradeOrderPlace
	default:
		return exchange.EndpointUnknown
	}
}

func normalizePath(path string) string {
	path = strings.TrimSpace(path)
	if idx := strings.Index(path, "?"); idx >= 0 {
		path = path[:idx]
	}
	return path
}
