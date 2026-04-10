package market

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDoGetWithControllerAppliesPerRequestWindowLimit(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer server.Close()

	controller := NewRequestController(RequestControllerConfig{
		APIRules: []APILimitRule{
			{
				Exchange:    "binance",
				Endpoint:    EndpointOHLCVRange,
				MaxRequests: 1,
				Window:      60 * time.Millisecond,
			},
		},
	})
	meta := RequestMeta{
		Exchange: "binance",
		Endpoint: EndpointOHLCVRange,
	}

	client := server.Client()
	startedAt := time.Now()

	resp1, err := doGetWithController(context.Background(), client, controller, meta, server.URL)
	if err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	_ = resp1.Body.Close()

	resp2, err := doGetWithController(context.Background(), client, controller, meta, server.URL)
	if err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	_ = resp2.Body.Close()

	elapsed := time.Since(startedAt)
	if elapsed < 45*time.Millisecond {
		t.Fatalf("expected second request to be throttled, elapsed=%s", elapsed)
	}
}
