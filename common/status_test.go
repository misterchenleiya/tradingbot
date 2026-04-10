package common

import (
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/iface"
)

func TestStatusImplementsProvider(t *testing.T) {
	var _ iface.StatusProvider = (*Status)(nil)
}

func TestStatusDefaults(t *testing.T) {
	s := NewStatus()
	got := s.Status()
	if got.Name != "app" {
		t.Fatalf("unexpected name: got %q want %q", got.Name, "app")
	}
	if got.State != "running" {
		t.Fatalf("unexpected state: got %q want %q", got.State, "running")
	}
	if got.UpdatedAt.IsZero() {
		t.Fatal("updated_at should not be zero")
	}
	details, ok := got.Details.(runtimeStatusDetails)
	if !ok {
		t.Fatalf("unexpected details type: %T", got.Details)
	}
	if details.Seconds < 0 {
		t.Fatalf("details.seconds must be >= 0, got %d", details.Seconds)
	}
	if details.Human == "" {
		t.Fatal("details.human should not be empty")
	}
}

func TestStatusSetModuleStatus(t *testing.T) {
	s := NewStatus()
	s.SetModuleStatus("paused", "maintenance", map[string]any{"reason": "manual"})

	got := s.Status()
	if got.State != "paused" {
		t.Fatalf("unexpected state: got %q want %q", got.State, "paused")
	}
	if got.Message != "maintenance" {
		t.Fatalf("unexpected message: got %q want %q", got.Message, "maintenance")
	}
	details, ok := got.Details.(map[string]any)
	if !ok {
		t.Fatalf("unexpected details type: %T", got.Details)
	}
	if details["reason"] != "manual" {
		t.Fatalf("unexpected reason: got %#v", details["reason"])
	}
}

func TestStatusSetModuleName(t *testing.T) {
	s := NewStatus()
	s.SetModuleName("runtime")
	got := s.Status()
	if got.Name != "runtime" {
		t.Fatalf("unexpected name: got %q want %q", got.Name, "runtime")
	}
}

func TestNilStatus(t *testing.T) {
	var s *Status
	got := s.Status()
	if got.Name != "app" {
		t.Fatalf("unexpected name: got %q want %q", got.Name, "app")
	}
	if got.State != "stopped" {
		t.Fatalf("unexpected state: got %q want %q", got.State, "stopped")
	}
}

func TestRuntimeSeconds(t *testing.T) {
	if got := runtimeSeconds(-time.Second); got != 0 {
		t.Fatalf("unexpected runtime seconds for negative duration: got %d want 0", got)
	}
	if got := runtimeSeconds(1900 * time.Millisecond); got != 1 {
		t.Fatalf("unexpected runtime seconds for positive duration: got %d want 1", got)
	}
}
