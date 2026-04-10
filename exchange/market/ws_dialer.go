package market

import (
	"net/http"
	"strings"

	exchangetransport "github.com/misterchenleiya/tradingbot/exchange/transport"
	"nhooyr.io/websocket"
)

func newWSDialOptions(proxy string) (*websocket.DialOptions, error) {
	options := &websocket.DialOptions{
		CompressionMode: websocket.CompressionContextTakeover,
	}
	client, err := newWSHTTPClient(proxy)
	if err != nil {
		return nil, err
	}
	if client != nil {
		options.HTTPClient = client
	}
	return options, nil
}

func newWSHTTPClient(proxy string) (*http.Client, error) {
	proxy = strings.TrimSpace(proxy)
	if proxy == "" {
		return nil, nil
	}
	dialer, err := exchangetransport.NewProxyDialer(proxy)
	if err != nil {
		return nil, err
	}
	if dialer == nil {
		return nil, nil
	}
	return &http.Client{
		Transport: &http.Transport{
			DialContext: dialer.DialContext,
		},
	}, nil
}
