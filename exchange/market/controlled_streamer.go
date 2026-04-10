package market

import (
	"context"

	"github.com/misterchenleiya/tradingbot/iface"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

type ControlledStreamer struct {
	inner      iface.Streamer
	controller *RequestController
	logger     *zap.Logger
}

func NewControlledStreamer(inner iface.Streamer, controller *RequestController, logger *zap.Logger) *ControlledStreamer {
	if logger == nil {
		logger = glog.Nop()
	}
	return &ControlledStreamer{
		inner:      inner,
		controller: controller,
		logger:     logger,
	}
}

func (c *ControlledStreamer) SupportsExchange(exchange string) bool {
	if c == nil || c.inner == nil {
		return false
	}
	return c.inner.SupportsExchange(exchange)
}

func (c *ControlledStreamer) Start(ctx context.Context) error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Start(ctx)
}

func (c *ControlledStreamer) Subscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if c == nil || c.inner == nil {
		return nil
	}
	if c.controller == nil {
		return c.inner.Subscribe(ctx, exchange, symbol, timeframe)
	}
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointWSSubscribe,
		Realtime: true,
	}
	return c.controller.Do(ctx, meta, func(ctx context.Context) error {
		return c.inner.Subscribe(ctx, exchange, symbol, timeframe)
	})
}

func (c *ControlledStreamer) Unsubscribe(ctx context.Context, exchange, symbol, timeframe string) error {
	if c == nil || c.inner == nil {
		return nil
	}
	if c.controller == nil {
		return c.inner.Unsubscribe(ctx, exchange, symbol, timeframe)
	}
	meta := RequestMeta{
		Exchange: exchange,
		Endpoint: EndpointWSUnsubscribe,
		Realtime: true,
	}
	return c.controller.Do(ctx, meta, func(ctx context.Context) error {
		return c.inner.Unsubscribe(ctx, exchange, symbol, timeframe)
	})
}

func (c *ControlledStreamer) Events() <-chan models.MarketData {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Events()
}

func (c *ControlledStreamer) Errors() <-chan error {
	if c == nil || c.inner == nil {
		return nil
	}
	return c.inner.Errors()
}
