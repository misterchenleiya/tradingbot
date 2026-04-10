package market

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type MultiWS struct {
	logger             *zap.Logger
	streamers          []iface.Streamer
	events             chan models.MarketData
	errors             chan error
	started            atomic.Bool
	allowUnclosedOHLCV atomic.Bool
	wg                 sync.WaitGroup
	ctx                context.Context
	mu                 sync.Mutex
	startedIdx         map[int]bool
}

func NewMultiWS(logger *zap.Logger, streamers ...iface.Streamer) *MultiWS {
	if logger == nil {
		logger = glog.Nop()
	}
	ws := &MultiWS{
		logger:     logger,
		streamers:  streamers,
		events:     make(chan models.MarketData, 2048),
		errors:     make(chan error, 32),
		startedIdx: make(map[int]bool),
	}
	ws.allowUnclosedOHLCV.Store(true)
	return ws
}

func (m *MultiWS) SetAllowUnclosedOHLCV(allow bool) {
	if m == nil {
		return
	}
	m.allowUnclosedOHLCV.Store(allow)
}

func (m *MultiWS) SupportsExchange(exchange string) bool {
	for _, s := range m.streamers {
		if s.SupportsExchange(exchange) {
			return true
		}
	}
	return false
}

func (m *MultiWS) Start(ctx context.Context) error {
	if !m.started.CompareAndSwap(false, true) {
		return errors.New("multi ws already started")
	}
	m.ctx = ctx
	m.logger.Info("market ws started")
	if ctx != nil {
		go func() {
			<-ctx.Done()
			m.logger.Info("market ws stopped")
		}()
	}
	return nil
}

func (m *MultiWS) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	var outErr error
	for idx, s := range m.streamers {
		if !s.SupportsExchange(exchange) {
			continue
		}
		m.ensureStarted(idx, s, ctx)
		if err := s.Subscribe(ctx, exchange, symbol, timeframe); err != nil && outErr == nil {
			outErr = err
		}
	}
	return outErr
}

func (m *MultiWS) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	var outErr error
	for idx, s := range m.streamers {
		if !s.SupportsExchange(exchange) {
			continue
		}
		m.ensureStarted(idx, s, ctx)
		if err := s.Unsubscribe(ctx, exchange, symbol, timeframe); err != nil && outErr == nil {
			outErr = err
		}
	}
	return outErr
}

func (m *MultiWS) Events() <-chan models.MarketData {
	return m.events
}

func (m *MultiWS) Errors() <-chan error {
	return m.errors
}

func (m *MultiWS) forwardEvents(ctx context.Context, ch <-chan models.MarketData) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			m.emit(data)
		}
	}
}

func (m *MultiWS) forwardErrors(ctx context.Context, ch <-chan error) {
	defer m.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case err, ok := <-ch:
			if !ok {
				return
			}
			m.sendErr(err)
		}
	}
}

func (m *MultiWS) emit(data models.MarketData) {
	if !data.Closed && !m.allowUnclosedOHLCV.Load() {
		return
	}
	select {
	case m.events <- data:
	default:
		m.logger.Warn("market ws event dropped",
			zap.String("exchange", data.Exchange),
			zap.String("symbol", data.Symbol),
			zap.String("timeframe", data.Timeframe),
		)
	}
}

func (m *MultiWS) sendErr(err error) {
	if err == nil {
		return
	}
	select {
	case m.errors <- err:
	default:
		m.logger.Warn("market ws error dropped", zap.Error(err))
	}
}

func (m *MultiWS) ensureStarted(idx int, s iface.Streamer, ctx context.Context) {
	m.mu.Lock()
	if m.startedIdx[idx] {
		m.mu.Unlock()
		return
	}
	m.startedIdx[idx] = true
	startCtx := m.ctx
	if startCtx == nil {
		startCtx = ctx
	}
	m.mu.Unlock()

	if startCtx == nil {
		startCtx = context.Background()
	}
	if err := s.Start(startCtx); err != nil {
		m.sendErr(err)
	}
	m.wg.Add(1)
	go m.forwardEvents(startCtx, s.Events())
	m.wg.Add(1)
	go m.forwardErrors(startCtx, s.Errors())
}
