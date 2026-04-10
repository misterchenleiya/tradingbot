package main

import (
	"reflect"
	"testing"
)

func TestNormalizeStrategyNameList_AllowEmpty(t *testing.T) {
	got, err := normalizeStrategyNameList([]string{""})
	if err != nil {
		t.Fatalf("normalizeStrategyNameList returned error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("normalized list length = %d, want 0", len(got))
	}
}

func TestNormalizeStrategyNameList_DedupAndTrim(t *testing.T) {
	got, err := normalizeStrategyNameList([]string{" turtle ", "SIMPLE_ELDER", "turtle"})
	if err != nil {
		t.Fatalf("normalizeStrategyNameList returned error: %v", err)
	}
	want := []string{"turtle", "turtle"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized list = %v, want %v", got, want)
	}
}
