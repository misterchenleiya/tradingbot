package core

import (
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestSignalChangedFields_IncludeArmedWhenEnterArmed(t *testing.T) {
	previous := models.Signal{Action: 0}
	next := models.Signal{Action: 4}

	changed := signalChangedFields(previous, next)
	if !containsChangedField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if !containsChangedField(changed, "armed") {
		t.Fatalf("expected changed fields include armed, got %v", changed)
	}
}

func TestSignalChangedFields_IncludeArmedWhenLeaveArmed(t *testing.T) {
	previous := models.Signal{Action: 4}
	next := models.Signal{Action: 8}

	changed := signalChangedFields(previous, next)
	if !containsChangedField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if !containsChangedField(changed, "armed") {
		t.Fatalf("expected changed fields include armed, got %v", changed)
	}
}

func TestSignalChangedFields_NotIncludeArmedForNormalActionChange(t *testing.T) {
	previous := models.Signal{Action: 8}
	next := models.Signal{Action: 16}

	changed := signalChangedFields(previous, next)
	if !containsChangedField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if containsChangedField(changed, "armed") {
		t.Fatalf("expected changed fields not include armed, got %v", changed)
	}
}

func containsChangedField(fields []string, target string) bool {
	for _, field := range fields {
		if field == target {
			return true
		}
	}
	return false
}
