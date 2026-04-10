package risk

import (
	"strings"
	"testing"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func TestSignalLifecycleChangedFields_IncludeArmedWhenEnterArmed(t *testing.T) {
	previous := models.Signal{Action: 0}
	next := models.Signal{Action: 4}

	changed := splitChangedFields(signalChangedFields(previous, next))
	if !containsField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if !containsField(changed, "armed") {
		t.Fatalf("expected changed fields include armed, got %v", changed)
	}
}

func TestSignalLifecycleChangedFields_IncludeArmedWhenLeaveArmed(t *testing.T) {
	previous := models.Signal{Action: 4}
	next := models.Signal{Action: 8}

	changed := splitChangedFields(signalChangedFields(previous, next))
	if !containsField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if !containsField(changed, "armed") {
		t.Fatalf("expected changed fields include armed, got %v", changed)
	}
}

func TestSignalLifecycleChangedFields_NotIncludeArmedForNormalActionChange(t *testing.T) {
	previous := models.Signal{Action: 8}
	next := models.Signal{Action: 16}

	changed := splitChangedFields(signalChangedFields(previous, next))
	if !containsField(changed, "action") {
		t.Fatalf("expected changed fields include action, got %v", changed)
	}
	if containsField(changed, "armed") {
		t.Fatalf("expected changed fields not include armed, got %v", changed)
	}
}

func TestSignalLifecycleChangedFields_IncludeStageEntryUsed(t *testing.T) {
	previous := models.Signal{StageEntryUsed: false}
	next := models.Signal{StageEntryUsed: true}

	changed := splitChangedFields(signalChangedFields(previous, next))
	if !containsField(changed, "stage_entry_used") {
		t.Fatalf("expected changed fields include stage_entry_used, got %v", changed)
	}
}

func TestIsSignalEqual_StageEntryUsedChangeNotEqual(t *testing.T) {
	left := models.Signal{
		Exchange:       "okx",
		Symbol:         "SOL/USDT",
		Timeframe:      "1h",
		Strategy:       "turtle",
		StageEntryUsed: false,
	}
	right := left
	right.StageEntryUsed = true

	if isSignalEqual(left, right) {
		t.Fatalf("expected signals unequal when stage_entry_used changes")
	}
}

func TestSignalLifecycleChangedFields_IncludeProfitProtectFields(t *testing.T) {
	previous := models.Signal{}
	next := models.Signal{
		InitialSL:                       95,
		InitialRiskPct:                  0.05,
		MaxFavorableProfitPct:           0.10,
		ProfitProtectStage:              models.SignalProfitProtectStageBreakEven,
		Plan1LastProfitLockMFER:         1.6,
		Plan1LastProfitLockHighBucketTS: 123456,
		Plan1LastProfitLockStructPrice:  90,
	}

	changed := splitChangedFields(signalChangedFields(previous, next))
	for _, field := range []string{
		"initial_sl",
		"initial_risk_pct",
		"max_favorable_profit_pct",
		"profit_protect_stage",
		"plan1_last_profit_lock_mfer",
		"plan1_last_profit_lock_high_bucket_ts",
		"plan1_last_profit_lock_struct_price",
	} {
		if !containsField(changed, field) {
			t.Fatalf("expected changed fields include %s, got %v", field, changed)
		}
	}
}

func TestIsSignalEqual_ProfitProtectFieldsChangeNotEqual(t *testing.T) {
	left := models.Signal{
		Exchange:                        "okx",
		Symbol:                          "SOL/USDT",
		Timeframe:                       "1h",
		Strategy:                        "turtle",
		InitialSL:                       95,
		InitialRiskPct:                  0.05,
		ProfitProtectStage:              models.SignalProfitProtectStageBreakEven,
		MaxFavorableProfitPct:           0.10,
		Plan1LastProfitLockMFER:         1.6,
		Plan1LastProfitLockHighBucketTS: 123456,
		Plan1LastProfitLockStructPrice:  90,
	}
	right := left
	right.Plan1LastProfitLockStructPrice = 89

	if isSignalEqual(left, right) {
		t.Fatalf("expected signals unequal when profit-protect fields change")
	}
}

func splitChangedFields(changed string) []string {
	if strings.TrimSpace(changed) == "" {
		return nil
	}
	return strings.Split(changed, ",")
}

func containsField(fields []string, target string) bool {
	for _, field := range fields {
		if strings.TrimSpace(field) == target {
			return true
		}
	}
	return false
}
