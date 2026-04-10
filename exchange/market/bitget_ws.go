package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

const (
	bitgetWSURL      = "wss://ws.bitget.com/v2/ws/public"
	bitgetInstTypeUS = "USDT-FUTURES"
	bitgetReadLimit  = 1 << 20
)

type BitgetWSConfig struct {
	URL         string
	Proxy       string
	Logger      *zap.Logger
	EventBuffer int
	ErrorBuffer int
}

type BitgetWS struct {
	url    string
	proxy  string
	logger *zap.Logger

	events chan models.MarketData
	errors chan error

	subsMu sync.RWMutex
	subs   map[string]bitgetSub

	connMu  sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex

	started atomic.Bool
}

type bitgetSub struct {
	symbol    string
	timeframe string
	channel   string
	instID    string
}

func NewBitgetWS(cfg BitgetWSConfig) *BitgetWS {
	url := cfg.URL
	if url == "" {
		url = bitgetWSURL
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
	return &BitgetWS{
		url:    url,
		proxy:  strings.TrimSpace(cfg.Proxy),
		logger: logger,
		events: make(chan models.MarketData, eventBuffer),
		errors: make(chan error, errorBuffer),
		subs:   make(map[string]bitgetSub),
	}
}

func (b *BitgetWS) SupportsExchange(exchange string) bool {
	return strings.EqualFold(exchange, "bitget")
}

func (b *BitgetWS) Events() <-chan models.MarketData {
	return b.events
}

func (b *BitgetWS) Errors() <-chan error {
	return b.errors
}

func (b *BitgetWS) Start(ctx context.Context) error {
	if !b.started.CompareAndSwap(false, true) {
		return errors.New("bitget ws already started")
	}
	go b.run(ctx)
	return nil
}

func (b *BitgetWS) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !b.SupportsExchange(exchange) {
		return nil
	}
	channel, err := bitgetWSChannel(timeframe)
	if err != nil {
		return err
	}
	instID := bitgetInstID(symbol)
	key := bitgetStreamKey(channel, instID)
	b.subsMu.Lock()
	b.subs[key] = bitgetSub{
		symbol:    symbol,
		timeframe: timeframe,
		channel:   channel,
		instID:    instID,
	}
	b.subsMu.Unlock()
	return b.sendSubscribe(ctx, []bitgetWSArg{{
		InstType: bitgetInstTypeUS,
		Channel:  channel,
		InstID:   instID,
	}})
}

func (b *BitgetWS) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !b.SupportsExchange(exchange) {
		return nil
	}
	channel, err := bitgetWSChannel(timeframe)
	if err != nil {
		return err
	}
	instID := bitgetInstID(symbol)
	key := bitgetStreamKey(channel, instID)
	b.subsMu.Lock()
	delete(b.subs, key)
	b.subsMu.Unlock()
	return b.sendUnsubscribe(ctx, []bitgetWSArg{{
		InstType: bitgetInstTypeUS,
		Channel:  channel,
		InstID:   instID,
	}})
}

func (b *BitgetWS) run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := b.connectAndServe(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			b.sendErr(err)
			b.logger.Warn("bitget ws reconnecting", zap.Error(err))
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

func (b *BitgetWS) connectAndServe(ctx context.Context) error {
	options, err := newWSDialOptions(b.proxy)
	if err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, b.url, options)
	if err != nil {
		return err
	}
	conn.SetReadLimit(bitgetReadLimit)
	b.setConn(conn)
	defer b.clearConn(conn)

	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go b.pingLoop(pingCtx)

	if err := b.resubscribe(ctx); err != nil {
		b.sendErr(err)
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		b.handleMessage(data)
	}
}

func (b *BitgetWS) setConn(conn *websocket.Conn) {
	b.connMu.Lock()
	b.conn = conn
	b.connMu.Unlock()
}

func (b *BitgetWS) clearConn(conn *websocket.Conn) {
	b.connMu.Lock()
	if b.conn == conn {
		b.conn = nil
	}
	b.connMu.Unlock()
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
}

func (b *BitgetWS) resubscribe(ctx context.Context) error {
	args := b.currentArgs()
	if len(args) == 0 {
		return nil
	}
	return b.sendSubscribe(ctx, args)
}

func (b *BitgetWS) currentArgs() []bitgetWSArg {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	args := make([]bitgetWSArg, 0, len(b.subs))
	for _, sub := range b.subs {
		args = append(args, bitgetWSArg{
			InstType: bitgetInstTypeUS,
			Channel:  sub.channel,
			InstID:   sub.instID,
		})
	}
	return args
}

func (b *BitgetWS) sendSubscribe(ctx context.Context, args []bitgetWSArg) error {
	if len(args) == 0 {
		return nil
	}
	msg := bitgetWSCommand{Op: "subscribe", Args: args}
	return b.send(ctx, msg)
}

func (b *BitgetWS) sendUnsubscribe(ctx context.Context, args []bitgetWSArg) error {
	if len(args) == 0 {
		return nil
	}
	msg := bitgetWSCommand{Op: "unsubscribe", Args: args}
	return b.send(ctx, msg)
}

func (b *BitgetWS) send(ctx context.Context, payload any) error {
	conn := b.getConn()
	if conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageText, data)
}

func (b *BitgetWS) getConn() *websocket.Conn {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	return b.conn
}

func (b *BitgetWS) handleMessage(raw []byte) {
	if string(raw) == "pong" {
		return
	}
	if string(raw) == "ping" {
		_ = b.sendRaw(context.Background(), []byte("pong"))
		return
	}
	var msg bitgetWSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		b.sendErr(fmt.Errorf("bitget ws decode failed: %w", err))
		return
	}
	if msg.Event != "" {
		code := bitgetCodeString(msg.Code)
		if msg.Event == "error" || (code != "" && code != "0") {
			errMsg := msg.Msg
			if errMsg == "" {
				errMsg = code
			}
			if msg.Arg.InstID != "" {
				errMsg = fmt.Sprintf("%s (instId=%s)", errMsg, msg.Arg.InstID)
			}
			b.sendErr(fmt.Errorf("bitget ws error: %s", errMsg))
		}
		return
	}
	if len(msg.Data) == 0 {
		return
	}
	row := msg.Data[0]
	if len(row) < 6 {
		b.sendErr(errors.New("bitget ws invalid kline data"))
		return
	}
	closed := false
	if len(row) >= 9 {
		value := strings.ToLower(row[8])
		closed = value == "1" || value == "true"
	}
	ts, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	open, err := strconv.ParseFloat(row[1], 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	high, err := strconv.ParseFloat(row[2], 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	low, err := strconv.ParseFloat(row[3], 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	closePx, err := strconv.ParseFloat(row[4], 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	vol, err := strconv.ParseFloat(row[5], 64)
	if err != nil {
		b.sendErr(err)
		return
	}

	symbol, timeframe := b.lookupStream(msg.Arg.Channel, msg.Arg.InstID)
	if symbol == "" {
		symbol = bitgetSymbolFromInstID(msg.Arg.InstID)
	}
	if timeframe == "" {
		timeframe = bitgetTimeframeFromChannel(msg.Arg.Channel)
	}
	b.emit(models.MarketData{
		Exchange:  "bitget",
		Symbol:    symbol,
		Timeframe: timeframe,
		OHLCV: models.OHLCV{
			TS:     ts,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePx,
			Volume: vol,
		},
		Closed: closed,
		Source: "ws",
	})
}

func (b *BitgetWS) lookupStream(channel, instID string) (string, string) {
	key := bitgetStreamKey(channel, instID)
	b.subsMu.RLock()
	sub, ok := b.subs[key]
	b.subsMu.RUnlock()
	if !ok {
		return "", ""
	}
	return sub.symbol, sub.timeframe
}

func (b *BitgetWS) emit(data models.MarketData) {
	select {
	case b.events <- data:
	default:
		b.logger.Warn("bitget ws event dropped",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
		)
	}
}

func (b *BitgetWS) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case b.errors <- err:
	default:
		b.logger.Warn("bitget ws error dropped", zap.Error(err))
	}
}

type bitgetWSMessage struct {
	Event  string          `json:"event"`
	Code   json.RawMessage `json:"code"`
	Msg    string          `json:"msg"`
	Action string          `json:"action"`
	Arg    bitgetWSArg     `json:"arg"`
	Data   [][]string      `json:"data"`
}

type bitgetWSCommand struct {
	Op   string        `json:"op"`
	Args []bitgetWSArg `json:"args"`
}

type bitgetWSArg struct {
	InstType string `json:"instType"`
	Channel  string `json:"channel"`
	InstID   string `json:"instId"`
}

func bitgetWSChannel(timeframe string) (string, error) {
	switch timeframe {
	case "1m", "3m", "5m", "15m", "30m":
		return "candle" + timeframe, nil
	case "1h":
		return "candle1H", nil
	case "2h":
		return "candle2H", nil
	case "4h":
		return "candle4H", nil
	case "6h":
		return "candle6H", nil
	case "12h":
		return "candle12H", nil
	case "1d":
		return "candle1D", nil
	default:
		return "", fmt.Errorf("unsupported timeframe for bitget ws: %s", timeframe)
	}
}

func bitgetTimeframeFromChannel(channel string) string {
	if !strings.HasPrefix(channel, "candle") {
		return ""
	}
	return strings.ToLower(strings.TrimPrefix(channel, "candle"))
}

func bitgetInstID(symbol string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(symbol, "/", ""), "-", ""))
}

func bitgetSymbolFromInstID(instID string) string {
	upper := strings.ToUpper(instID)
	if strings.HasSuffix(upper, "USDT") && len(upper) > 4 {
		return upper[:len(upper)-4] + "/USDT"
	}
	return upper
}

func bitgetStreamKey(channel, instID string) string {
	return strings.ToLower(channel) + "|" + strings.ToLower(instID)
}

func (b *BitgetWS) sendRaw(ctx context.Context, payload []byte) error {
	conn := b.getConn()
	if conn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageText, payload)
}

func (b *BitgetWS) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			conn := b.getConn()
			if conn == nil {
				continue
			}
			_ = b.sendRaw(ctx, []byte("ping"))
		}
	}
}

func bitgetCodeString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		return num.String()
	}
	return string(raw)
}
