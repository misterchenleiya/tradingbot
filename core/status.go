package core

import (
	"sort"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
)

const (
	coreStateInit    = "init"
	coreStateWarmup  = "warmup"
	coreStateRunning = "running"
	coreStateStopped = "stopped"
	coreStateError   = "error"
)

type coreStatusDetails struct {
	Exchanges map[string]exchangeRuntimeState `json:"exchanges,omitempty"`
}

func (b *Live) Status() iface.ModuleStatus {
	if b == nil {
		return iface.ModuleStatus{Name: "core", State: coreStateStopped}
	}
	b.statusMu.RLock()
	status := b.status
	b.statusMu.RUnlock()
	status.Name = strings.TrimSpace(status.Name)
	if status.Name == "" {
		status.Name = "core"
	}
	if strings.TrimSpace(status.State) == "" {
		if b.started.Load() {
			status.State = coreStateRunning
		} else {
			status.State = coreStateStopped
		}
	}
	if details := b.exchangeStatesSnapshot(); len(details) > 0 {
		status.Details = coreStatusDetails{Exchanges: details}
	}
	return status
}

func (b *Live) setStatus(state, message string) {
	if b == nil {
		return
	}
	status := iface.ModuleStatus{
		Name:      "core",
		State:     strings.TrimSpace(state),
		Message:   strings.TrimSpace(message),
		UpdatedAt: time.Now().UTC(),
	}
	b.statusMu.Lock()
	b.status = status
	b.statusMu.Unlock()
}

func (b *Live) refreshRuntimeStatus() {
	if b == nil {
		return
	}
	states := b.exchangeStatesSnapshot()
	state := coreStateRunning
	message := ""
	if len(states) > 0 {
		var (
			anyReady   bool
			anyWarming bool
			anyPaused  bool
			allError   = true
			warming    []string
			paused     []string
		)
		for exchange, item := range states {
			switch strings.TrimSpace(item.State) {
			case exchangeStateReady:
				anyReady = true
				allError = false
			case exchangeStateWarming:
				anyWarming = true
				allError = false
				warming = append(warming, exchange)
			case exchangeStatePaused:
				anyPaused = true
				allError = false
				paused = append(paused, exchange)
			case exchangeStateError:
			default:
				allError = false
			}
		}
		sort.Strings(warming)
		sort.Strings(paused)
		switch {
		case anyReady:
			state = coreStateRunning
			if len(warming) > 0 || len(paused) > 0 {
				parts := make([]string, 0, 2)
				if len(warming) > 0 {
					parts = append(parts, "warming="+strings.Join(warming, ","))
				}
				if len(paused) > 0 {
					parts = append(parts, "paused="+strings.Join(paused, ","))
				}
				message = strings.Join(parts, "; ")
			}
		case anyWarming || anyPaused:
			state = coreStateWarmup
			parts := make([]string, 0, 2)
			if len(warming) > 0 {
				parts = append(parts, "warming="+strings.Join(warming, ","))
			}
			if len(paused) > 0 {
				parts = append(parts, "paused="+strings.Join(paused, ","))
			}
			message = strings.Join(parts, "; ")
		case allError:
			state = coreStateError
		}
	}
	status := iface.ModuleStatus{
		Name:      "core",
		State:     state,
		Message:   strings.TrimSpace(message),
		UpdatedAt: time.Now().UTC(),
	}
	if len(states) > 0 {
		status.Details = coreStatusDetails{Exchanges: states}
	}
	b.statusMu.Lock()
	b.status = status
	b.statusMu.Unlock()
}
