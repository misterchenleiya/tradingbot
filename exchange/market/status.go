package market

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/iface"
)

type exchangeStatus struct {
	State            string    `json:"state"`
	Message          string    `json:"message,omitempty"`
	PausedUntil      time.Time `json:"paused_until,omitempty"`
	RemainingSeconds int64     `json:"remaining_seconds,omitempty"`
	RemainingHuman   string    `json:"remaining_human,omitempty"`
}

type marketStatusDetails struct {
	Exchanges map[string]exchangeStatus `json:"exchanges,omitempty"`
}

func (s *RealTimeService) Status() iface.ModuleStatus {
	status := iface.ModuleStatus{Name: "exchange"}
	if s == nil || !s.started.Load() {
		status.State = "stopped"
		status.UpdatedAt = time.Now().UTC()
		return status
	}

	now := time.Now().UTC()
	status.UpdatedAt = now

	pauses := ExchangePauses(now)
	exchanges := s.listActiveExchangeStatus(pauses, now)

	messages := buildPauseMessages(pauses, now)
	if len(messages) > 0 {
		status.State = "paused"
		status.Message = strings.Join(messages, "; ")
	} else {
		status.State = "running"
	}
	if len(exchanges) > 0 {
		status.Details = marketStatusDetails{Exchanges: exchanges}
	}
	return status
}

func (s *RealTimeService) listActiveExchangeStatus(pauses map[string]time.Time, now time.Time) map[string]exchangeStatus {
	exchanges := make(map[string]exchangeStatus)
	if s == nil {
		return exchanges
	}
	names := s.activeExchangeNames()
	for _, exchange := range names {
		if until, ok := pauses[exchange]; ok {
			exchanges[exchange] = pausedExchangeStatus(exchange, until, now)
			continue
		}
		state, message := s.exchangeStatus(exchange)
		state = strings.TrimSpace(state)
		if state == "" || strings.EqualFold(state, "ready") {
			exchanges[exchange] = exchangeStatus{State: "running"}
			continue
		}
		exchanges[exchange] = exchangeStatus{
			State:   state,
			Message: strings.TrimSpace(message),
		}
	}
	for exchange, until := range pauses {
		if _, ok := exchanges[exchange]; ok {
			continue
		}
		exchanges[exchange] = pausedExchangeStatus(exchange, until, now)
	}
	return exchanges
}

func (s *RealTimeService) activeExchangeNames() []string {
	if s == nil {
		return nil
	}
	s.groupsMu.RLock()
	defer s.groupsMu.RUnlock()
	if len(s.groups) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.groups))
	for name := range s.groups {
		normalized := normalizeExchangeName(name)
		if normalized == "" {
			continue
		}
		names = append(names, normalized)
	}
	sort.Strings(names)
	return names
}

func buildPauseMessages(pauses map[string]time.Time, now time.Time) []string {
	if len(pauses) == 0 {
		return nil
	}
	messages := make([]string, 0, len(pauses))
	for exchange, until := range pauses {
		message := pauseMessage(exchange, until, now)
		if message == "" {
			continue
		}
		messages = append(messages, message)
	}
	sort.Strings(messages)
	return messages
}

func pausedExchangeStatus(exchange string, until, now time.Time) exchangeStatus {
	remaining := until.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	remainingSeconds := int64(remaining.Truncate(time.Second).Seconds())
	if remainingSeconds < 0 {
		remainingSeconds = 0
	}
	message := pauseMessage(exchange, until, now)
	return exchangeStatus{
		State:            "paused",
		Message:          message,
		PausedUntil:      until,
		RemainingSeconds: remainingSeconds,
		RemainingHuman:   common.FormatDuration(remaining),
	}
}

func pauseMessage(exchange string, until, now time.Time) string {
	if exchange == "" || until.IsZero() {
		return ""
	}
	remaining := until.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return fmt.Sprintf("%s paused until %s (resume in %s)", exchange, until.Format(time.RFC3339), common.FormatDuration(remaining))
}
