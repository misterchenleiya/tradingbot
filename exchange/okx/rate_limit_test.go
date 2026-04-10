package okx

import (
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/exchange"
)

func TestOKXPrivateRulesHaveWindowLimits(t *testing.T) {
	rules := okxAPILimitRules()
	index := make(map[string]bool)
	values := make(map[string]struct {
		minInterval time.Duration
		maxRequests int
		window      time.Duration
	})
	for _, rule := range rules {
		index[rule.Endpoint] = true
		values[rule.Endpoint] = struct {
			minInterval time.Duration
			maxRequests int
			window      time.Duration
		}{
			minInterval: rule.MinInterval,
			maxRequests: rule.MaxRequests,
			window:      rule.Window,
		}
	}

	endpoints := []string{
		exchange.EndpointAccountConfig,
		exchange.EndpointSetPositionMode,
		exchange.EndpointSetLeverage,
		exchange.EndpointAccountPositions,
		exchange.EndpointAccountPositionsHistory,
		exchange.EndpointTradeOrdersAlgoPending,
		exchange.EndpointTradeCancelAlgos,
		exchange.EndpointTradeOrderAlgo,
		exchange.EndpointAccountBalance,
		exchange.EndpointAssetBalances,
		exchange.EndpointTradeOrderPlace,
		exchange.EndpointTradeOrderGet,
	}
	for _, endpoint := range endpoints {
		if !index[endpoint] {
			t.Fatalf("missing endpoint rule: %s", endpoint)
		}
		item := values[endpoint]
		if item.minInterval <= 0 {
			t.Fatalf("endpoint %s has no min interval", endpoint)
		}
		if item.maxRequests <= 0 {
			t.Fatalf("endpoint %s has no max requests", endpoint)
		}
		if item.window <= 0 {
			t.Fatalf("endpoint %s has no window", endpoint)
		}
	}

	if got := values[exchange.EndpointSetLeverage].maxRequests; got != leverageMaxRequests {
		t.Fatalf("unexpected set leverage limit: got=%d want=%d", got, leverageMaxRequests)
	}
	if got := values[exchange.EndpointSetPositionMode].maxRequests; got != leverageMaxRequests {
		t.Fatalf("unexpected set position mode limit: got=%d want=%d", got, leverageMaxRequests)
	}
}
