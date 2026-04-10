package strategy

import (
	"strings"
	"testing"
)

func TestBuildStrategies(t *testing.T) {
	items, err := BuildStrategies([]string{"turtle", "turtle", "turtle"})
	if err != nil {
		t.Fatalf("BuildStrategies returned error: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("strategy count = %d, want 3", len(items))
	}
	got := []string{items[0].Name(), items[1].Name(), items[2].Name()}
	want := []string{"turtle", "turtle", "turtle"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("strategy[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestBuildStrategiesAlias(t *testing.T) {
	items, err := BuildStrategies([]string{"turtle"})
	if err != nil {
		t.Fatalf("BuildStrategies returned error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("strategy count = %d, want 1", len(items))
	}
	if items[0].Name() != "turtle" {
		t.Fatalf("strategy name = %q, want %q", items[0].Name(), "turtle")
	}
}

func TestBuildStrategiesUnknown(t *testing.T) {
	_, err := BuildStrategies([]string{"unknown"})
	if err == nil {
		t.Fatalf("BuildStrategies expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unknown strategy") {
		t.Fatalf("error = %v, want contains %q", err, "unknown strategy")
	}
}

func TestBuildStrategiesEmpty(t *testing.T) {
	_, err := BuildStrategies(nil)
	if err == nil {
		t.Fatalf("BuildStrategies expected error, got nil")
	}
}
