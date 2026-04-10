package common

import (
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
)

const (
	defaultModuleName  = "app"
	defaultModuleState = "running"
	stoppedModuleState = "stopped"
)

type runtimeStatusDetails struct {
	Seconds int64  `json:"seconds"`
	Human   string `json:"human"`
}

type Status struct {
	start  time.Time
	mu     sync.RWMutex
	module iface.ModuleStatus
}

func NewStatus() *Status {
	now := time.Now().UTC()
	return &Status{
		start: now,
		module: iface.ModuleStatus{
			Name:      defaultModuleName,
			State:     defaultModuleState,
			UpdatedAt: now,
		},
	}
}

func (s *Status) Status() iface.ModuleStatus {
	if s == nil {
		return iface.ModuleStatus{Name: defaultModuleName, State: stoppedModuleState}
	}
	s.mu.RLock()
	status := s.module
	s.mu.RUnlock()
	status.Name = strings.TrimSpace(status.Name)
	if status.Name == "" {
		status.Name = defaultModuleName
	}
	status.State = strings.TrimSpace(status.State)
	if status.State == "" {
		status.State = defaultModuleState
	}
	if status.UpdatedAt.IsZero() {
		status.UpdatedAt = s.start
	}
	if status.Details == nil {
		status.Details = runtimeStatusDetails{
			Seconds: runtimeSeconds(s.Runtime()),
			Human:   s.RuntimeString(),
		}
	}
	return status
}

func (s *Status) SetModuleStatus(state, message string, details any) {
	if s == nil {
		return
	}
	now := time.Now().UTC()
	state = strings.TrimSpace(state)
	if state == "" {
		state = defaultModuleState
	}
	message = strings.TrimSpace(message)
	s.mu.Lock()
	s.module.State = state
	s.module.Message = message
	s.module.Details = details
	s.module.UpdatedAt = now
	s.mu.Unlock()
}

func (s *Status) SetModuleName(name string) {
	if s == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = defaultModuleName
	}
	s.mu.Lock()
	s.module.Name = name
	s.mu.Unlock()
}

func (s *Status) Runtime() time.Duration {
	if s == nil {
		return 0
	}
	return time.Since(s.start)
}

func (s *Status) RuntimeString() string {
	return formatDuration(s.Runtime())
}

func runtimeSeconds(d time.Duration) int64 {
	seconds := int64(d.Truncate(time.Second).Seconds())
	if seconds < 0 {
		return 0
	}
	return seconds
}

func FormatDuration(d time.Duration) string {
	return formatDuration(d)
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	totalSeconds := int64(d.Truncate(time.Second).Seconds())
	days := totalSeconds / 86400
	hours := (totalSeconds % 86400) / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	out := make([]byte, 0, 32)
	if days > 0 {
		out = appendInt(out, days)
		out = append(out, 'd')
	}
	if hours > 0 || days > 0 {
		out = appendInt(out, hours)
		out = append(out, 'h')
	}
	if minutes > 0 || hours > 0 || days > 0 {
		out = appendInt(out, minutes)
		out = append(out, 'm')
	}
	out = appendInt(out, seconds)
	out = append(out, 's')
	return string(out)
}

func appendInt(dst []byte, value int64) []byte {
	if value == 0 {
		return append(dst, '0')
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return append(dst, buf[i:]...)
}
