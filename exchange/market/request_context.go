package market

import (
	"context"

	"go.uber.org/zap"
)

type requestContextKey int

const (
	requestMaxBarsKey requestContextKey = iota
	requestControllerKey
	requestLoggerKey
)

func withRequestMaxBars(ctx context.Context, maxBars int) context.Context {
	if maxBars <= 0 {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestMaxBarsKey, maxBars)
}

func requestMaxBars(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	if v, ok := ctx.Value(requestMaxBarsKey).(int); ok {
		return v
	}
	return 0
}

func WithRequestController(ctx context.Context, controller *RequestController) context.Context {
	if controller == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestControllerKey, controller)
}

func requestController(ctx context.Context) *RequestController {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(requestControllerKey).(*RequestController); ok {
		return v
	}
	return nil
}

func WithRequestLogger(ctx context.Context, logger *zap.Logger) context.Context {
	if logger == nil {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestLoggerKey, logger)
}

func requestLogger(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return nil
	}
	if v, ok := ctx.Value(requestLoggerKey).(*zap.Logger); ok {
		return v
	}
	return nil
}
