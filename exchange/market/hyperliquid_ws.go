package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

const hyperliquidWSURL = "wss://api.hyperliquid.xyz/ws"

type HyperliquidWSConfig struct {
	URL         string
	Proxy       string
	Logger      *zap.Logger
	EventBuffer int
	ErrorBuffer int
}

type HyperliquidWS struct {
	url    string
	proxy  string
	logger *zap.Logger

	events chan models.MarketData
	errors chan error

	subsMu sync.RWMutex
	subs   map[string]hyperliquidSub

	connMu  sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex

	started atomic.Bool
}

type hyperliquidSub struct {
	symbol    string
	timeframe string
	coin      string
	interval  string
}

func NewHyperliquidWS(cfg HyperliquidWSConfig) *HyperliquidWS {
	url := cfg.URL
	if url == "" {
		url = hyperliquidWSURL
	}
	logger := cfg.Logger
	if logger == nil {
		logger = glog.Nop()
	}
	eventBuffer := cfg.EventBuffer
	if eventBuffer <= 0 {
		eventBuffer = 1024
	}
	errorBuffer := cfg.ErrorBuffer
	if errorBuffer <= 0 {
		errorBuffer = 16
	}
	return &HyperliquidWS{
		url:    url,
		proxy:  strings.TrimSpace(cfg.Proxy),
		logger: logger,
		events: make(chan models.MarketData, eventBuffer),
		errors: make(chan error, errorBuffer),
		subs:   make(map[string]hyperliquidSub),
	}
}

func (h *HyperliquidWS) SupportsExchange(exchange string) bool {
	return strings.EqualFold(exchange, "hyperliquid")
}

func (h *HyperliquidWS) Events() <-chan models.MarketData {
	return h.events
}

func (h *HyperliquidWS) Errors() <-chan error {
	return h.errors
}

func (h *HyperliquidWS) Start(ctx context.Context) error {
	if !h.started.CompareAndSwap(false, true) {
		return errors.New("hyperliquid ws already started")
	}
	go h.run(ctx)
	return nil
}

func (h *HyperliquidWS) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !h.SupportsExchange(exchange) {
		return nil
	}
	interval, err := hyperliquidInterval(timeframe)
	if err != nil {
		return err
	}
	coin, err := hyperliquidCoinFromSymbol(symbol)
	if err != nil {
		return err
	}
	key := hyperliquidStreamKey(coin, interval)
	h.subsMu.Lock()
	h.subs[key] = hyperliquidSub{
		symbol:    symbol,
		timeframe: timeframe,
		coin:      coin,
		interval:  interval,
	}
	h.subsMu.Unlock()
	return h.sendSubscribe(ctx, hyperliquidWSSub{
		Type:     "candle",
		Coin:     coin,
		Interval: interval,
	})
}

func (h *HyperliquidWS) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !h.SupportsExchange(exchange) {
		return nil
	}
	interval, err := hyperliquidInterval(timeframe)
	if err != nil {
		return err
	}
	coin, err := hyperliquidCoinFromSymbol(symbol)
	if err != nil {
		return err
	}
	key := hyperliquidStreamKey(coin, interval)
	h.subsMu.Lock()
	delete(h.subs, key)
	h.subsMu.Unlock()
	return h.sendUnsubscribe(ctx, hyperliquidWSSub{
		Type:     "candle",
		Coin:     coin,
		Interval: interval,
	})
}

func (h *HyperliquidWS) run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := h.connectAndServe(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			h.sendErr(err)
			h.logger.Warn("hyperliquid ws reconnecting", zap.Error(err))
		}
		if ctx.Err() != nil {
			return
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (h *HyperliquidWS) connectAndServe(ctx context.Context) error {
	options, err := newWSDialOptions(h.proxy)
	if err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, h.url, options)
	if err != nil {
		return err
	}
	h.setConn(conn)
	defer h.clearConn(conn)

	if err := h.resubscribe(ctx); err != nil {
		h.sendErr(err)
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		h.handleMessage(data)
	}
}

func (h *HyperliquidWS) setConn(conn *websocket.Conn) {
	h.connMu.Lock()
	h.conn = conn
	h.connMu.Unlock()
}

func (h *HyperliquidWS) clearConn(conn *websocket.Conn) {
	h.connMu.Lock()
	if h.conn == conn {
		h.conn = nil
	}
	h.connMu.Unlock()
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
}

func (h *HyperliquidWS) resubscribe(ctx context.Context) error {
	subs := h.currentSubs()
	if len(subs) == 0 {
		return nil
	}
	for _, sub := range subs {
		if err := h.sendSubscribe(ctx, hyperliquidWSSub{
			Type:     "candle",
			Coin:     sub.coin,
			Interval: sub.interval,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (h *HyperliquidWS) currentSubs() []hyperliquidSub {
	h.subsMu.RLock()
	defer h.subsMu.RUnlock()
	out := make([]hyperliquidSub, 0, len(h.subs))
	for _, sub := range h.subs {
		out = append(out, sub)
	}
	return out
}

func (h *HyperliquidWS) handleMessage(raw []byte) {
	if len(raw) == 0 {
		return
	}
	var msg hyperliquidWSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		h.sendErr(fmt.Errorf("hyperliquid ws decode failed: %w", err))
		return
	}
	switch msg.Channel {
	case "subscriptionResponse":
		return
	case "candle":
		if msg.IsSnapshot {
			return
		}
		candles, err := decodeHyperliquidCandles(msg.Data)
		if err != nil {
			h.sendErr(fmt.Errorf("hyperliquid ws candle decode failed: %w", err))
			return
		}
		for _, candle := range candles {
			coin := candle.Coin
			interval := candle.Interval
			if coin == "" {
				continue
			}
			symbol, timeframe := h.lookupSub(coin, interval)
			if symbol == "" {
				symbol = hyperliquidSymbolFromCoin(coin)
			}
			if timeframe == "" {
				timeframe = interval
			}
			closed := false
			closeTime := candle.CloseTime
			if closeTime == 0 && interval != "" {
				if d, ok := timeframeDuration(interval); ok {
					closeTime = candle.OpenTime + d.Milliseconds()
				}
			}
			if closeTime > 0 {
				closed = time.Now().UTC().UnixMilli() >= closeTime
			}
			h.emit(models.MarketData{
				Exchange:  "hyperliquid",
				Symbol:    symbol,
				Timeframe: timeframe,
				OHLCV: models.OHLCV{
					TS:     candle.OpenTime,
					Open:   candle.Open,
					High:   candle.High,
					Low:    candle.Low,
					Close:  candle.Close,
					Volume: candle.Volume,
				},
				Closed: closed,
				Source: "ws",
			})
		}
		return
	case "error":
		errMsg := parseHyperliquidWSError(msg.Data)
		if errMsg == "" {
			errMsg = "unknown error"
		}
		h.sendErr(fmt.Errorf("hyperliquid ws error: %s", errMsg))
		return
	default:
		return
	}
}

func (h *HyperliquidWS) lookupSub(coin, interval string) (string, string) {
	key := hyperliquidStreamKey(coin, interval)
	h.subsMu.RLock()
	sub, ok := h.subs[key]
	h.subsMu.RUnlock()
	if !ok {
		return "", ""
	}
	return sub.symbol, sub.timeframe
}

func (h *HyperliquidWS) sendSubscribe(ctx context.Context, sub hyperliquidWSSub) error {
	return h.sendCommand(ctx, "subscribe", sub)
}

func (h *HyperliquidWS) sendUnsubscribe(ctx context.Context, sub hyperliquidWSSub) error {
	return h.sendCommand(ctx, "unsubscribe", sub)
}

func (h *HyperliquidWS) sendCommand(ctx context.Context, method string, sub hyperliquidWSSub) error {
	conn := h.getConn()
	if conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload := hyperliquidWSCommand{
		Method:       method,
		Subscription: sub,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	h.writeMu.Lock()
	defer h.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageText, data)
}

func (h *HyperliquidWS) getConn() *websocket.Conn {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	return h.conn
}

func (h *HyperliquidWS) emit(data models.MarketData) {
	select {
	case h.events <- data:
	default:
		h.logger.Warn("hyperliquid ws event dropped",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
		)
	}
}

func (h *HyperliquidWS) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case h.errors <- err:
	default:
		h.logger.Warn("hyperliquid ws error dropped", zap.Error(err))
	}
}

type hyperliquidWSMessage struct {
	Channel    string          `json:"channel"`
	Data       json.RawMessage `json:"data"`
	IsSnapshot bool            `json:"isSnapshot"`
}

type hyperliquidWSCommand struct {
	Method       string           `json:"method"`
	Subscription hyperliquidWSSub `json:"subscription"`
}

type hyperliquidWSSub struct {
	Type     string `json:"type"`
	Coin     string `json:"coin"`
	Interval string `json:"interval"`
}

func parseHyperliquidWSError(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	var text string
	if err := json.Unmarshal(data, &text); err == nil {
		return strings.TrimSpace(text)
	}
	var payload struct {
		Error string `json:"error"`
		Msg   string `json:"msg"`
	}
	if err := json.Unmarshal(data, &payload); err == nil {
		if strings.TrimSpace(payload.Error) != "" {
			return strings.TrimSpace(payload.Error)
		}
		if strings.TrimSpace(payload.Msg) != "" {
			return strings.TrimSpace(payload.Msg)
		}
	}
	return strings.TrimSpace(string(data))
}
