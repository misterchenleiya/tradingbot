package market

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

const binanceWSURL = "wss://fstream.binance.com/stream"

type BinanceWSConfig struct {
	URL         string
	Proxy       string
	Logger      *zap.Logger
	EventBuffer int
	ErrorBuffer int
}

type BinanceWS struct {
	url    string
	proxy  string
	logger *zap.Logger

	events chan models.MarketData
	errors chan error

	subsMu sync.RWMutex
	subs   map[string]binanceSub

	connMu   sync.Mutex
	conn     *websocket.Conn
	updateCh chan struct{}

	started atomic.Bool
}

type binanceSub struct {
	symbol    string
	timeframe string
}

func NewBinanceWS(cfg BinanceWSConfig) *BinanceWS {
	url := cfg.URL
	if url == "" {
		url = binanceWSURL
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
	return &BinanceWS{
		url:      url,
		proxy:    strings.TrimSpace(cfg.Proxy),
		logger:   logger,
		events:   make(chan models.MarketData, eventBuffer),
		errors:   make(chan error, errorBuffer),
		subs:     make(map[string]binanceSub),
		updateCh: make(chan struct{}, 1),
	}
}

func (b *BinanceWS) SupportsExchange(exchange string) bool {
	return strings.EqualFold(exchange, "binance")
}

func (b *BinanceWS) Events() <-chan models.MarketData {
	return b.events
}

func (b *BinanceWS) Errors() <-chan error {
	return b.errors
}

func (b *BinanceWS) Start(ctx context.Context) error {
	if !b.started.CompareAndSwap(false, true) {
		return errors.New("binance ws already started")
	}
	go b.run(ctx)
	return nil
}

func (b *BinanceWS) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !b.SupportsExchange(exchange) {
		return nil
	}
	stream, err := binanceStreamName(symbol, timeframe)
	if err != nil {
		return err
	}
	b.subsMu.Lock()
	b.subs[stream] = binanceSub{symbol: symbol, timeframe: timeframe}
	b.subsMu.Unlock()
	b.signalUpdate()
	return nil
}

func (b *BinanceWS) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if !b.SupportsExchange(exchange) {
		return nil
	}
	stream, err := binanceStreamName(symbol, timeframe)
	if err != nil {
		return err
	}
	b.subsMu.Lock()
	delete(b.subs, stream)
	b.subsMu.Unlock()
	b.signalUpdate()
	return nil
}

func (b *BinanceWS) run(ctx context.Context) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		streams := b.currentStreams()
		if len(streams) == 0 {
			if !b.waitForUpdate(ctx) {
				return
			}
			continue
		}
		url := b.buildStreamURL(streams)
		if err := b.connectAndServe(ctx, url); err != nil {
			if ctx.Err() != nil {
				return
			}
			b.sendErr(err)
			b.logger.Warn("binance ws reconnecting", zap.Error(err))
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
	}
}

func (b *BinanceWS) connectAndServe(ctx context.Context, url string) error {
	options, err := newWSDialOptions(b.proxy)
	if err != nil {
		return err
	}
	conn, _, err := websocket.Dial(ctx, url, options)
	if err != nil {
		return err
	}
	b.setConn(conn)
	defer b.clearConn(conn)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	updateDone := make(chan struct{})
	go func() {
		select {
		case <-connCtx.Done():
		case <-b.updateCh:
			b.drainUpdates()
			cancel()
		}
		close(updateDone)
	}()

	for {
		_, data, err := conn.Read(connCtx)
		if err != nil {
			<-updateDone
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		b.handleMessage(data)
	}
}

func (b *BinanceWS) setConn(conn *websocket.Conn) {
	b.connMu.Lock()
	b.conn = conn
	b.connMu.Unlock()
}

func (b *BinanceWS) clearConn(conn *websocket.Conn) {
	b.connMu.Lock()
	if b.conn == conn {
		b.conn = nil
	}
	b.connMu.Unlock()
	_ = conn.Close(websocket.StatusNormalClosure, "closing")
}

func (b *BinanceWS) currentStreams() []string {
	b.subsMu.RLock()
	defer b.subsMu.RUnlock()
	streams := make([]string, 0, len(b.subs))
	for stream := range b.subs {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	return streams
}

func (b *BinanceWS) handleMessage(raw []byte) {
	if len(raw) == 0 {
		return
	}
	var combined binanceCombinedMessage
	if err := json.Unmarshal(raw, &combined); err == nil && combined.Data.Event != "" {
		if combined.Data.Event != "kline" {
			return
		}
		b.handleKline(combined.Stream, combined.Data.Symbol, combined.Data.Kline)
		return
	}
	var direct binanceEventData
	if err := json.Unmarshal(raw, &direct); err != nil {
		b.sendErr(fmt.Errorf("binance ws decode failed: %w", err))
		return
	}
	if direct.Event != "kline" {
		return
	}
	b.handleKline("", direct.Symbol, direct.Kline)
}

func (b *BinanceWS) handleKline(stream, symbol string, k binanceKline) {
	sym := symbol
	timeframe := k.Interval
	if stream != "" {
		if subSymbol, subTimeframe := b.lookupStream(stream); subSymbol != "" {
			sym = subSymbol
			timeframe = subTimeframe
		}
	}
	if sym == "" {
		sym = binanceNormalizeSymbol(symbol)
	}
	open, err := strconv.ParseFloat(k.Open, 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	high, err := strconv.ParseFloat(k.High, 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	low, err := strconv.ParseFloat(k.Low, 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	closePx, err := strconv.ParseFloat(k.Close, 64)
	if err != nil {
		b.sendErr(err)
		return
	}
	vol, err := strconv.ParseFloat(k.Volume, 64)
	if err != nil {
		b.sendErr(err)
		return
	}

	b.emit(models.MarketData{
		Exchange:  "binance",
		Symbol:    sym,
		Timeframe: timeframe,
		OHLCV: models.OHLCV{
			TS:     k.Start,
			Open:   open,
			High:   high,
			Low:    low,
			Close:  closePx,
			Volume: vol,
		},
		Closed: k.Closed,
		Source: "ws",
	})
}

func (b *BinanceWS) lookupStream(stream string) (string, string) {
	key := strings.ToLower(stream)
	b.subsMu.RLock()
	sub, ok := b.subs[key]
	b.subsMu.RUnlock()
	if !ok {
		return "", ""
	}
	return sub.symbol, sub.timeframe
}

func (b *BinanceWS) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case b.errors <- err:
	default:
	}
}

func (b *BinanceWS) emit(data models.MarketData) {
	select {
	case b.events <- data:
	default:
		b.logger.Warn("binance ws event dropped",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
		)
	}
}

type binanceCombinedMessage struct {
	Stream string           `json:"stream"`
	Data   binanceEventData `json:"data"`
}

type binanceEventData struct {
	Event  string       `json:"e"`
	Symbol string       `json:"s"`
	Kline  binanceKline `json:"k"`
}

type binanceKline struct {
	Start    int64  `json:"t"`
	Interval string `json:"i"`
	Open     string `json:"o"`
	Close    string `json:"c"`
	High     string `json:"h"`
	Low      string `json:"l"`
	Volume   string `json:"v"`
	Closed   bool   `json:"x"`
}

func binanceStreamName(symbol, timeframe string) (string, error) {
	if symbol == "" || timeframe == "" {
		return "", errors.New("binance ws empty symbol/timeframe")
	}
	raw := strings.ToLower(strings.ReplaceAll(symbol, "/", ""))
	return fmt.Sprintf("%s@kline_%s", raw, strings.ToLower(timeframe)), nil
}

func binanceNormalizeSymbol(raw string) string {
	upper := strings.ToUpper(raw)
	if strings.HasSuffix(upper, "USDT") && len(upper) > 4 {
		return upper[:len(upper)-4] + "/USDT"
	}
	return upper
}

func (b *BinanceWS) signalUpdate() {
	select {
	case b.updateCh <- struct{}{}:
	default:
	}
}

func (b *BinanceWS) drainUpdates() {
	for {
		select {
		case <-b.updateCh:
		default:
			return
		}
	}
}

func (b *BinanceWS) waitForUpdate(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-b.updateCh:
		b.drainUpdates()
		return true
	}
}

func (b *BinanceWS) buildStreamURL(streams []string) string {
	return b.url + "?streams=" + strings.Join(streams, "/")
}
