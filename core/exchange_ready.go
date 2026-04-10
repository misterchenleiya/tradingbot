package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
)

const (
	exchangeStateWarming = "warming"
	exchangeStateReady   = "ready"
	exchangeStatePaused  = "paused"
	exchangeStateError   = "error"
)

type exchangeRuntimeState struct {
	State     string    `json:"state"`
	Message   string    `json:"message,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

func (b *Live) listActiveExchanges() ([]string, error) {
	if b == nil || b.ohlcvStore == nil {
		return nil, nil
	}
	exchanges, err := b.ohlcvStore.ListExchanges()
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(exchanges))
	seen := make(map[string]struct{}, len(exchanges))
	for _, ex := range exchanges {
		if !ex.Active {
			continue
		}
		name := strings.TrimSpace(ex.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

func (b *Live) initExchangeReadiness(exchanges []string) {
	if b == nil {
		return
	}
	now := time.Now().UTC()
	states := make(map[string]exchangeRuntimeState, len(exchanges))
	for _, exchange := range exchanges {
		name := strings.TrimSpace(exchange)
		if name == "" {
			continue
		}
		states[name] = exchangeRuntimeState{
			State:     exchangeStateWarming,
			UpdatedAt: now,
		}
	}
	b.exchangeStateMu.Lock()
	b.exchangeStates = states
	b.exchangeStateMu.Unlock()
	b.refreshRuntimeStatus()
}

func (b *Live) ExchangeReady(exchange string) bool {
	if b == nil {
		return true
	}
	state, _ := b.ExchangeStatus(exchange)
	if state == "" {
		return true
	}
	return state == exchangeStateReady
}

func (b *Live) ExchangeStatus(exchange string) (string, string) {
	if b == nil {
		return "", ""
	}
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return "", ""
	}
	b.exchangeStateMu.RLock()
	state, ok := b.exchangeStates[exchange]
	b.exchangeStateMu.RUnlock()
	if !ok {
		return "", ""
	}
	return state.State, state.Message
}

func (b *Live) setExchangeState(exchange, state, message string) {
	if b == nil {
		return
	}
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return
	}
	runtimeState := exchangeRuntimeState{
		State:     strings.TrimSpace(state),
		Message:   strings.TrimSpace(message),
		UpdatedAt: time.Now().UTC(),
	}
	b.exchangeStateMu.Lock()
	if b.exchangeStates == nil {
		b.exchangeStates = make(map[string]exchangeRuntimeState)
	}
	b.exchangeStates[exchange] = runtimeState
	b.exchangeStateMu.Unlock()
	b.refreshRuntimeStatus()
}

func (b *Live) markExchangeReady(exchange string) {
	if b == nil {
		return
	}
	b.setExchangeState(exchange, exchangeStateReady, "")
}

func (b *Live) exchangeStatesSnapshot() map[string]exchangeRuntimeState {
	if b == nil {
		return nil
	}
	b.exchangeStateMu.RLock()
	defer b.exchangeStateMu.RUnlock()
	if len(b.exchangeStates) == 0 {
		return nil
	}
	out := make(map[string]exchangeRuntimeState, len(b.exchangeStates))
	for exchange, state := range b.exchangeStates {
		out[exchange] = state
	}
	return out
}

func (b *Live) exchangePauseDelay(exchange string) (time.Duration, bool) {
	if b == nil {
		return 0, false
	}
	if until, ok := market.ExchangePaused(exchange, time.Now().UTC()); ok {
		delay := time.Until(until)
		if delay < 0 {
			delay = 0
		}
		return delay, true
	}
	return 0, false
}

func (b *Live) waitForExchangeResume(ctx context.Context, exchange string) error {
	for {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		delay, paused := b.exchangePauseDelay(exchange)
		if !paused {
			b.setExchangeState(exchange, exchangeStateWarming, "")
			return nil
		}
		message := fmt.Sprintf("paused, resume in %s", common.FormatDuration(delay))
		b.setExchangeState(exchange, exchangeStatePaused, message)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (b *Live) startWarmup(ctx context.Context) error {
	if b == nil || b.skipWarmup {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	plans, requestCtx, err := b.buildWarmupPlans(ctx)
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		b.refreshRuntimeStatus()
		return nil
	}
	warmupCtx, cancel := context.WithCancel(ctx)
	b.warmupCancel = cancel
	for _, plan := range plans {
		plan := plan
		b.warmupWG.Add(1)
		go func() {
			defer b.warmupWG.Done()
			b.runWarmupExchange(warmupCtx, requestCtx, plan)
		}()
	}
	return nil
}

func (b *Live) runWarmupExchange(ctx, requestCtx context.Context, plan warmupExchangePlan) {
	exchange := plan.exchange
	if strings.TrimSpace(exchange) == "" {
		return
	}
	for {
		if ctx != nil && ctx.Err() != nil {
			return
		}
		if err := b.waitForExchangeResume(ctx, exchange); err != nil {
			return
		}
		paused, err := b.warmUpExchangePlan(requestCtx, plan)
		if err != nil {
			b.setExchangeState(exchange, exchangeStateError, err.Error())
			return
		}
		if paused {
			continue
		}
		b.bootstrapStrategyCombosForExchange(ctx, plan)
		if ctx != nil && ctx.Err() != nil {
			return
		}
		b.markExchangeReady(exchange)
		b.startHistoryExchange(exchange)
		return
	}
}
