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
	okxWSURL                     = "wss://ws.okx.com:8443/ws/v5/business"
	okxWSDefaultBatchSize        = 10
	okxWSDefaultSendInterval     = 20 * time.Millisecond
	okxWSDefaultMaxSubscriptions = 240
)

var errOKXWSNoConn = errors.New("okx ws not connected")

type OKXWSConfig struct {
	URL              string
	Proxy            string
	Logger           *zap.Logger
	EventBuffer      int
	ErrorBuffer      int
	BatchSize        int
	SendInterval     time.Duration
	MaxSubscriptions int
}

type OKXWS struct {
	url    string
	proxy  string
	logger *zap.Logger

	events chan models.MarketData
	errors chan error

	subsMu sync.RWMutex
	subs   map[string]okxSub

	connMu  sync.Mutex
	conn    *websocket.Conn
	writeMu sync.Mutex

	pendingMu    sync.Mutex
	pendingSub   map[string]okxWSArg
	pendingUnsub map[string]okxWSArg
	pendingCh    chan struct{}
	connReady    chan struct{}

	batchSize        int
	sendInterval     time.Duration
	maxSubscriptions int
	overLimit        atomic.Bool

	started atomic.Bool
}

type okxSub struct {
	symbol    string
	timeframe string
	channel   string
	instID    string
}

func NewOKXWS(cfg OKXWSConfig) *OKXWS {
	url := cfg.URL
	if url == "" {
		url = okxWSURL
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
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = okxWSDefaultBatchSize
	}
	sendInterval := cfg.SendInterval
	if sendInterval <= 0 {
		sendInterval = okxWSDefaultSendInterval
	}
	maxSubs := cfg.MaxSubscriptions
	if maxSubs <= 0 {
		maxSubs = okxWSDefaultMaxSubscriptions
	}
	return &OKXWS{
		url:              url,
		proxy:            strings.TrimSpace(cfg.Proxy),
		logger:           logger,
		events:           make(chan models.MarketData, eventBuffer),
		errors:           make(chan error, errorBuffer),
		subs:             make(map[string]okxSub),
		pendingSub:       make(map[string]okxWSArg),
		pendingUnsub:     make(map[string]okxWSArg),
		pendingCh:        make(chan struct{}, 1),
		connReady:        make(chan struct{}, 1),
		batchSize:        batchSize,
		sendInterval:     sendInterval,
		maxSubscriptions: maxSubs,
	}
}

func (o *OKXWS) SupportsExchange(exchange string) bool {
	return strings.EqualFold(exchange, "okx")
}

func (o *OKXWS) Events() <-chan models.MarketData {
	return o.events
}

func (o *OKXWS) Errors() <-chan error {
	return o.errors
}

func (o *OKXWS) Start(ctx context.Context) error {
	if !o.started.CompareAndSwap(false, true) {
		return errors.New("okx ws already started")
	}
	go o.runSender(ctx)
	go o.run(ctx)
	return nil
}

func (o *OKXWS) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !o.SupportsExchange(exchange) {
		return nil
	}
	channel, err := okxWSChannel(timeframe)
	if err != nil {
		return err
	}
	instID := okxInstID(symbol)
	key := okxStreamKey(channel, instID)
	added := false
	o.subsMu.Lock()
	if _, ok := o.subs[key]; !ok {
		o.subs[key] = okxSub{
			symbol:    symbol,
			timeframe: timeframe,
			channel:   channel,
			instID:    instID,
		}
		added = true
	}
	count := len(o.subs)
	o.subsMu.Unlock()
	if !added {
		return nil
	}
	o.enqueueSubscribe([]okxWSArg{{Channel: channel, InstID: instID}})
	o.updateSubscriptionLimit(count)
	return nil
}

func (o *OKXWS) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !o.SupportsExchange(exchange) {
		return nil
	}
	channel, err := okxWSChannel(timeframe)
	if err != nil {
		return err
	}
	instID := okxInstID(symbol)
	key := okxStreamKey(channel, instID)
	o.subsMu.Lock()
	if _, ok := o.subs[key]; !ok {
		o.subsMu.Unlock()
		return nil
	}
	delete(o.subs, key)
	count := len(o.subs)
	o.subsMu.Unlock()
	o.enqueueUnsubscribe([]okxWSArg{{Channel: channel, InstID: instID}})
	o.updateSubscriptionLimit(count)
	return nil
}

func (o *OKXWS) run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := o.connectAndServe(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			o.sendErr(err)
			o.logger.Warn("okx ws reconnecting", zap.Error(err))
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

func (o *OKXWS) runSender(ctx context.Context) {
	ticker := time.NewTicker(o.sendInterval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		op, args := o.nextBatch()
		if len(args) == 0 {
			select {
			case <-o.pendingCh:
			case <-o.connReady:
			case <-ctx.Done():
				return
			}
			continue
		}
		if !o.waitForConn(ctx) {
			return
		}
		args = o.filterArgs(op, args)
		if len(args) == 0 {
			continue
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
		if err := o.sendCommand(ctx, op, args); err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errOKXWSNoConn) {
				o.requeue(op, args)
				continue
			}
			o.sendErr(fmt.Errorf("okx ws send failed: %w", err))
			o.requeue(op, args)
		}
	}
}

func (o *OKXWS) connectAndServe(ctx context.Context) error {
	options, err := newWSDialOptions(o.proxy)
	if err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, o.url, options)
	if err != nil {
		return err
	}
	o.setConn(conn)
	defer o.clearConn(conn)

	o.resubscribe()

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		o.handleMessage(data)
	}
}

func (o *OKXWS) setConn(conn *websocket.Conn) {
	o.connMu.Lock()
	o.conn = conn
	o.connMu.Unlock()
	o.notifyConn()
}

func (o *OKXWS) clearConn(conn *websocket.Conn) {
	o.connMu.Lock()
	if o.conn == conn {
		o.conn = nil
	}
	o.connMu.Unlock()
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
}

func (o *OKXWS) resubscribe() {
	args := o.currentArgs()
	if len(args) == 0 {
		return
	}
	o.enqueueSubscribe(args)
}

func (o *OKXWS) currentArgs() []okxWSArg {
	o.subsMu.RLock()
	defer o.subsMu.RUnlock()
	args := make([]okxWSArg, 0, len(o.subs))
	for _, sub := range o.subs {
		args = append(args, okxWSArg{Channel: sub.channel, InstID: sub.instID})
	}
	return args
}

func (o *OKXWS) enqueueSubscribe(args []okxWSArg) {
	if len(args) == 0 {
		return
	}
	o.pendingMu.Lock()
	for _, arg := range args {
		key := okxStreamKey(arg.Channel, arg.InstID)
		delete(o.pendingUnsub, key)
		o.pendingSub[key] = arg
	}
	o.pendingMu.Unlock()
	o.notifyPending()
}

func (o *OKXWS) enqueueUnsubscribe(args []okxWSArg) {
	if len(args) == 0 {
		return
	}
	o.pendingMu.Lock()
	for _, arg := range args {
		key := okxStreamKey(arg.Channel, arg.InstID)
		delete(o.pendingSub, key)
		o.pendingUnsub[key] = arg
	}
	o.pendingMu.Unlock()
	o.notifyPending()
}

func (o *OKXWS) notifyPending() {
	select {
	case o.pendingCh <- struct{}{}:
	default:
	}
}

func (o *OKXWS) notifyConn() {
	select {
	case o.connReady <- struct{}{}:
	default:
	}
}

func (o *OKXWS) nextBatch() (string, []okxWSArg) {
	o.pendingMu.Lock()
	defer o.pendingMu.Unlock()
	if len(o.pendingUnsub) > 0 {
		return "unsubscribe", o.takeBatchLocked(o.pendingUnsub)
	}
	if len(o.pendingSub) > 0 {
		return "subscribe", o.takeBatchLocked(o.pendingSub)
	}
	return "", nil
}

func (o *OKXWS) takeBatchLocked(pending map[string]okxWSArg) []okxWSArg {
	if len(pending) == 0 {
		return nil
	}
	batchSize := o.batchSize
	if batchSize <= 0 || batchSize > len(pending) {
		batchSize = len(pending)
	}
	args := make([]okxWSArg, 0, batchSize)
	for key, arg := range pending {
		args = append(args, arg)
		delete(pending, key)
		if len(args) >= batchSize {
			break
		}
	}
	return args
}

func (o *OKXWS) waitForConn(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		if o.getConn() != nil {
			return true
		}
		select {
		case <-o.connReady:
		case <-ctx.Done():
			return false
		}
	}
}

func (o *OKXWS) filterArgs(op string, args []okxWSArg) []okxWSArg {
	if len(args) == 0 {
		return nil
	}
	o.subsMu.RLock()
	defer o.subsMu.RUnlock()
	out := args[:0]
	for _, arg := range args {
		key := okxStreamKey(arg.Channel, arg.InstID)
		_, ok := o.subs[key]
		if op == "subscribe" {
			if !ok {
				continue
			}
		} else if op == "unsubscribe" && ok {
			continue
		}
		out = append(out, arg)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (o *OKXWS) requeue(op string, args []okxWSArg) {
	if len(args) == 0 {
		return
	}
	args = o.filterArgs(op, args)
	if len(args) == 0 {
		return
	}
	if op == "subscribe" {
		o.enqueueSubscribe(args)
		return
	}
	if op == "unsubscribe" {
		o.enqueueUnsubscribe(args)
	}
}

func (o *OKXWS) sendCommand(ctx context.Context, op string, args []okxWSArg) error {
	if len(args) == 0 {
		return nil
	}
	if op != "subscribe" && op != "unsubscribe" {
		return fmt.Errorf("okx ws invalid op: %s", op)
	}
	msg := okxWSCommand{Op: op, Args: args}
	return o.send(ctx, msg)
}

func (o *OKXWS) updateSubscriptionLimit(count int) {
	if o.maxSubscriptions <= 0 {
		return
	}
	if count > o.maxSubscriptions {
		if !o.overLimit.Load() {
			o.overLimit.Store(true)
			o.logger.Warn("okx ws subscription limit exceeded",
				zap.Int("subscriptions", count),
				zap.Int("max", o.maxSubscriptions),
			)
		}
		return
	}
	if o.overLimit.Load() {
		o.overLimit.Store(false)
	}
}

func (o *OKXWS) sendSubscribe(ctx context.Context, args []okxWSArg) error {
	if len(args) == 0 {
		return nil
	}
	msg := okxWSCommand{Op: "subscribe", Args: args}
	return o.send(ctx, msg)
}

func (o *OKXWS) sendUnsubscribe(ctx context.Context, args []okxWSArg) error {
	if len(args) == 0 {
		return nil
	}
	msg := okxWSCommand{Op: "unsubscribe", Args: args}
	return o.send(ctx, msg)
}

func (o *OKXWS) send(ctx context.Context, payload any) error {
	conn := o.getConn()
	if conn == nil {
		return errOKXWSNoConn
	}
	if ctx == nil {
		ctx = context.Background()
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	o.writeMu.Lock()
	defer o.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageText, data)
}

func (o *OKXWS) getConn() *websocket.Conn {
	o.connMu.Lock()
	defer o.connMu.Unlock()
	return o.conn
}

func (o *OKXWS) handleMessage(raw []byte) {
	if string(raw) == "pong" || string(raw) == "ping" {
		return
	}
	var msg okxWSMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		o.sendErr(fmt.Errorf("okx ws decode failed: %w", err))
		return
	}
	if msg.Event != "" {
		if msg.Event == "error" || (msg.Code != "" && msg.Code != "0") {
			o.sendErr(fmt.Errorf("okx ws error: %s", msg.Msg))
		}
		return
	}
	if len(msg.Data) == 0 {
		return
	}
	row := msg.Data[0]
	if len(row) < 6 {
		o.sendErr(errors.New("okx ws invalid kline data"))
		return
	}
	closed := false
	if len(row) >= 9 {
		closed = row[8] == "1" || strings.EqualFold(row[8], "true")
	}
	ts, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil {
		o.sendErr(err)
		return
	}
	open, err := strconv.ParseFloat(row[1], 64)
	if err != nil {
		o.sendErr(err)
		return
	}
	high, err := strconv.ParseFloat(row[2], 64)
	if err != nil {
		o.sendErr(err)
		return
	}
	low, err := strconv.ParseFloat(row[3], 64)
	if err != nil {
		o.sendErr(err)
		return
	}
	closePx, err := strconv.ParseFloat(row[4], 64)
	if err != nil {
		o.sendErr(err)
		return
	}
	vol, err := strconv.ParseFloat(row[5], 64)
	if err != nil {
		o.sendErr(err)
		return
	}

	symbol, timeframe := o.lookupStream(msg.Arg.Channel, msg.Arg.InstID)
	if symbol == "" {
		symbol = okxSymbolFromInstID(msg.Arg.InstID)
	}
	if timeframe == "" {
		timeframe = okxTimeframeFromChannel(msg.Arg.Channel)
	}
	o.emit(models.MarketData{
		Exchange:  "okx",
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

func (o *OKXWS) lookupStream(channel, instID string) (string, string) {
	key := okxStreamKey(channel, instID)
	o.subsMu.RLock()
	sub, ok := o.subs[key]
	o.subsMu.RUnlock()
	if !ok {
		return "", ""
	}
	return sub.symbol, sub.timeframe
}

func (o *OKXWS) emit(data models.MarketData) {
	select {
	case o.events <- data:
	default:
		o.logger.Warn("okx ws event dropped",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
		)
	}
}

func (o *OKXWS) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case o.errors <- err:
	default:
		o.logger.Warn("okx ws error dropped", zap.Error(err))
	}
}

type okxWSMessage struct {
	Event string     `json:"event"`
	Code  string     `json:"code"`
	Msg   string     `json:"msg"`
	Arg   okxWSArg   `json:"arg"`
	Data  [][]string `json:"data"`
}

type okxWSCommand struct {
	Op   string     `json:"op"`
	Args []okxWSArg `json:"args"`
}

type okxWSArg struct {
	Channel string `json:"channel"`
	InstID  string `json:"instId"`
}

func okxWSChannel(timeframe string) (string, error) {
	bar, err := okxBar(timeframe)
	if err != nil {
		return "", err
	}
	return "candle" + bar, nil
}

func okxTimeframeFromChannel(channel string) string {
	if !strings.HasPrefix(channel, "candle") {
		return ""
	}
	bar := strings.TrimPrefix(channel, "candle")
	lower := strings.ToLower(bar)
	if strings.HasSuffix(lower, "utc") {
		lower = strings.TrimSuffix(lower, "utc")
	}
	return lower
}

func okxSymbolFromInstID(instID string) string {
	parts := strings.Split(instID, "-")
	if len(parts) >= 2 {
		return strings.ToUpper(parts[0]) + "/" + strings.ToUpper(parts[1])
	}
	return strings.ToUpper(instID)
}

func okxStreamKey(channel, instID string) string {
	return strings.ToLower(channel) + "|" + strings.ToLower(instID)
}
