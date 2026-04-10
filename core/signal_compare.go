package core

import "github.com/misterchenleiya/tradingbot/internal/models"

func triggerHistoryEqual(left, right []models.TriggerHistoryRecord) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Action != right[i].Action {
			return false
		}
		if left[i].MidSide != right[i].MidSide {
			return false
		}
		if left[i].TriggerTimestamp != right[i].TriggerTimestamp {
			return false
		}
	}
	return true
}
