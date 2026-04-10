package risk

import (
	"sort"
	"strings"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func normalizeStrategyName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func buildActiveStrategySet(names []string) map[string]struct{} {
	if len(names) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(names))
	for _, item := range names {
		name := normalizeStrategyName(item)
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func listActiveStrategies(active map[string]struct{}) []string {
	if len(active) == 0 {
		return nil
	}
	out := make([]string, 0, len(active))
	for name := range active {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func isStrategyAllowed(active map[string]struct{}, name string) bool {
	if len(active) == 0 {
		return true
	}
	_, ok := active[normalizeStrategyName(name)]
	return ok
}

func filterSignalsByActiveStrategies(signals []models.Signal, active map[string]struct{}) []models.Signal {
	if len(signals) == 0 {
		return nil
	}
	if len(active) == 0 {
		return append([]models.Signal(nil), signals...)
	}
	out := make([]models.Signal, 0, len(signals))
	for _, signal := range signals {
		if !isStrategyAllowed(active, signal.Strategy) {
			continue
		}
		out = append(out, signal)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func filterGroupedSignalsByActiveStrategies(grouped map[string]map[string]models.Signal, active map[string]struct{}) map[string]map[string]models.Signal {
	if len(grouped) == 0 {
		return map[string]map[string]models.Signal{}
	}
	if len(active) == 0 {
		return grouped
	}
	out := make(map[string]map[string]models.Signal, len(grouped))
	for outerKey, bucket := range grouped {
		filtered := make(map[string]models.Signal)
		for innerKey, signal := range bucket {
			if !isStrategyAllowed(active, signal.Strategy) {
				continue
			}
			filtered[innerKey] = signal
		}
		if len(filtered) == 0 {
			continue
		}
		out[outerKey] = filtered
	}
	return out
}
