package execution

import (
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
)

const (
	executionModuleName    = "execution"
	executionStatusRunning = "running"
	executionStatusStopped = "stopped"
)

func (e *Live) Status() iface.ModuleStatus {
	status := iface.ModuleStatus{
		Name:      executionModuleName,
		State:     executionStatusStopped,
		UpdatedAt: time.Now().UTC(),
	}
	if e == nil {
		return status
	}
	if e.started.Load() {
		status.State = executionStatusRunning
	}
	return status
}

func (e *BackTest) Status() iface.ModuleStatus {
	status := iface.ModuleStatus{
		Name:      executionModuleName,
		State:     executionStatusStopped,
		UpdatedAt: time.Now().UTC(),
	}
	if e == nil {
		return status
	}
	if e.started.Load() {
		status.State = executionStatusRunning
	}
	return status
}

var _ iface.StatusProvider = (*Live)(nil)
var _ iface.StatusProvider = (*BackTest)(nil)
