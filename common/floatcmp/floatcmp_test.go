package floatcmp

import "testing"

func TestDefaultComparisons(t *testing.T) {
	t.Parallel()

	a := 1.0
	b := 1.0 + DefaultAbsTolerance/2
	if GT(a, b) {
		t.Fatalf("GT should be false within tolerance: a=%v b=%v", a, b)
	}
	if LT(a, b) {
		t.Fatalf("LT should be false within tolerance: a=%v b=%v", a, b)
	}
	if !LE(a, b) {
		t.Fatalf("LE should be true within tolerance: a=%v b=%v", a, b)
	}
	if !GE(a, b) {
		t.Fatalf("GE should be true within tolerance: a=%v b=%v", a, b)
	}
	if !EQ(a, b) {
		t.Fatalf("EQ should be true within tolerance: a=%v b=%v", a, b)
	}
}

func TestCustomConfigComparisons(t *testing.T) {
	t.Parallel()

	cfg := Config{
		AbsTolerance: 0,
		RelTolerance: 0,
	}
	a := 10.0
	b := 10.1
	if !LTWithConfig(a, b, cfg) {
		t.Fatalf("LTWithConfig should be true without tolerance: a=%v b=%v", a, b)
	}
	if !GTWithConfig(b, a, cfg) {
		t.Fatalf("GTWithConfig should be true without tolerance: a=%v b=%v", b, a)
	}
	if LEWithConfig(b, a, cfg) {
		t.Fatalf("LEWithConfig should be false without tolerance: a=%v b=%v", b, a)
	}
	if EQWithConfig(a, b, cfg) {
		t.Fatalf("EQWithConfig should be false without tolerance: a=%v b=%v", a, b)
	}
}

func TestNegativeToleranceClampedToZero(t *testing.T) {
	t.Parallel()

	cfg := Config{
		AbsTolerance: -1,
		RelTolerance: -1,
	}
	a := 5.0
	b := 5.0
	if !EQWithConfig(a, b, cfg) {
		t.Fatalf("EQWithConfig should be true for equal values even with negative tolerance config")
	}
	if GTWithConfig(a, b, cfg) {
		t.Fatalf("GTWithConfig should be false for equal values even with negative tolerance config")
	}
}
