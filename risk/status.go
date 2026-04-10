package risk

import (
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
)

const (
	riskModuleName    = "risk"
	riskStatusRunning = "running"
	riskStatusStopped = "stopped"
)

func (r *Live) Status() iface.ModuleStatus {
	status := iface.ModuleStatus{
		Name:      riskModuleName,
		State:     riskStatusStopped,
		UpdatedAt: time.Now().UTC(),
	}
	if r == nil {
		return status
	}
	if r.started.Load() {
		status.State = riskStatusRunning
	}
	status.Details = map[string]any{
		"trend_guard": r.trendGuard.status(r.currentConfig().TrendGuard),
	}
	return status
}

func (r *BackTest) Status() iface.ModuleStatus {
	status := iface.ModuleStatus{
		Name:      riskModuleName,
		State:     riskStatusStopped,
		UpdatedAt: time.Now().UTC(),
	}
	if r == nil {
		return status
	}
	if r.started.Load() {
		status.State = riskStatusRunning
	}
	details := trendGuardStatusDetails{
		Enabled: r.cfg.TrendGuard.Enabled,
		Mode:    normalizeTrendGuardMode(r.cfg.TrendGuard.Mode),
		Groups:  []trendGuardStatusGroupSummary{},
	}
	if r.trendGuard != nil {
		details = r.trendGuard.status(r.cfg.TrendGuard)
	}
	status.Details = map[string]any{
		"trend_guard": details,
	}
	return status
}

var _ iface.StatusProvider = (*Live)(nil)
var _ iface.StatusProvider = (*BackTest)(nil)
