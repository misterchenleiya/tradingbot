package risk

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

func TestGroupedTrendGuardFirstQualifiedTriggerBecomesLeader(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	first := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	later := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	firstSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}
	laterSeries := []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}
	guard.observeSignal(cfg, first, groupedTrendGuardEvalContext(firstSeries), baseTS)
	guard.observeSignal(cfg, later, groupedTrendGuardEvalContext(laterSeries), baseTS+1)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	firstScore := runtime.candidates["okx|AGLD/USDT"].PriorityScore
	laterScore := runtime.candidates["okx|FIL/USDT"].PriorityScore
	if firstScore < cfg.LeaderMinPriorityScore {
		t.Fatalf("expected first candidate above threshold, got %.2f", firstScore)
	}
	if laterScore <= firstScore {
		t.Fatalf("expected later candidate score %.2f > first candidate score %.2f", laterScore, firstScore)
	}

	firstOpen := first
	firstOpen.Action = 8
	guard.observeSignal(cfg, firstOpen, groupedTrendGuardEvalContext(firstSeries), baseTS+2)

	status := guard.status(cfg)
	if status.GroupsActive != 1 {
		t.Fatalf("expected 1 active group, got %d", status.GroupsActive)
	}
	if len(status.Groups) != 1 {
		t.Fatalf("expected 1 status group, got %d", len(status.Groups))
	}
	if status.Groups[0].SelectedCandidateKey != "okx|AGLD/USDT" {
		t.Fatalf("expected leader okx|AGLD/USDT, got %s", status.Groups[0].SelectedCandidateKey)
	}
	if status.Groups[0].State != trendGroupStateSoftLocked {
		t.Fatalf("expected soft lock state, got %s", status.Groups[0].State)
	}
	if reason, reject := guard.authorizeOpen(cfg, firstOpen); reject {
		t.Fatalf("expected first qualifying trigger allowed, got reject=%v reason=%s", reject, reason)
	}

	laterOpen := later
	laterOpen.Action = 8
	guard.observeSignal(cfg, laterOpen, groupedTrendGuardEvalContext(laterSeries), baseTS+3)
	if reason, reject := guard.authorizeOpen(cfg, laterOpen); !reject || reason == "" {
		t.Fatalf("expected later higher-score candidate rejected after first trigger freeze, got reject=%v reason=%s", reject, reason)
	}
}

func TestGroupedTrendGuardHardLocksAfterFirstOpen(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	leader := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	other := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)

	guard.observeSignal(cfg, leader, groupedTrendGuardEvalContext([]float64{100, 103, 106, 109, 112, 115, 118, 121, 124}), baseTS)
	guard.observeSignal(cfg, other, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS+1)

	leaderOpen := leader
	leaderOpen.Action = 8
	guard.observeSignal(cfg, leaderOpen, groupedTrendGuardEvalContext([]float64{100, 103, 106, 109, 112, 115, 118, 121, 124}), baseTS+2)

	guard.syncPositions(map[string]models.Position{
		"okx|FIL-USDT-SWAP|long|isolated": {
			Exchange:           "okx",
			Symbol:             "FIL/USDT",
			Timeframe:          "30m",
			PositionSide:       positionSideLong,
			Status:             models.PositionStatusOpen,
			EntryQuantity:      1,
			StrategyName:       "turtle",
			StrategyTimeframes: []string{"1m", "5m", "30m"},
			ComboKey:           "1m/5m/30m",
		},
	}, baseTS+3)

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected 1 status group, got %d", len(status.Groups))
	}
	if status.Groups[0].State != trendGroupStateHardLocked {
		t.Fatalf("expected hard lock state, got %s", status.Groups[0].State)
	}
	if status.Groups[0].LockStage != trendGroupLockStageHard {
		t.Fatalf("expected hard lock stage, got %s", status.Groups[0].LockStage)
	}
	otherOpen := other
	otherOpen.Action = 8
	guard.observeSignal(cfg, otherOpen, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS+4)
	if reason, reject := guard.authorizeOpen(cfg, other); !reject || reason == "" {
		t.Fatalf("expected non-leader rejected after hard lock, got reject=%v reason=%s", reject, reason)
	}
}

func TestGroupedTrendGuardMarkSignalGoneFinishesGroup(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	baseTS := int64(1_773_840_000_000)
	signal := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, signal, groupedTrendGuardEvalContext([]float64{100, 102, 104, 105, 107, 109, 111, 113, 115}), baseTS)
	guard.markSignalGone(cfg, signal, baseTS+1)

	status := guard.status(cfg)
	if status.GroupsActive != 0 {
		t.Fatalf("expected no active groups, got %d", status.GroupsActive)
	}
	if len(status.Groups) != 0 {
		t.Fatalf("expected no visible groups, got %d", len(status.Groups))
	}
	if status.GroupsTotal != 0 {
		t.Fatalf("expected no runtime groups after finish, got %d", status.GroupsTotal)
	}
}

func TestGroupedTrendGuardMarkSignalGonePrunesInactiveCandidateFromRuntime(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	baseTS := int64(1_773_840_000_000)
	first := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	second := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, first, groupedTrendGuardEvalContext([]float64{100, 101, 102, 103, 104, 105, 106, 107, 108}), baseTS)
	guard.observeSignal(cfg, second, groupedTrendGuardEvalContext([]float64{100, 102, 104, 106, 108, 110, 112, 114, 116}), baseTS+1)

	guard.markSignalGone(cfg, first, baseTS+2)

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one group, got %d", len(status.Groups))
	}
	if len(status.Groups[0].Candidates) != 1 {
		t.Fatalf("expected stale candidate pruned, got %d candidates", len(status.Groups[0].Candidates))
	}
	if status.Groups[0].Candidates[0].CandidateKey != "okx|FIL/USDT" {
		t.Fatalf("expected remaining candidate okx|FIL/USDT, got %s", status.Groups[0].Candidates[0].CandidateKey)
	}
}

func TestGroupedTrendGuardLookupSignalGroupedIncludesCandidates(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	first := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	later := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	firstSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}
	laterSeries := []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}
	guard.observeSignal(cfg, first, groupedTrendGuardEvalContext(firstSeries), baseTS)
	guard.observeSignal(cfg, later, groupedTrendGuardEvalContext(laterSeries), baseTS+1)
	firstOpen := first
	firstOpen.Action = 8
	guard.observeSignal(cfg, firstOpen, groupedTrendGuardEvalContext(firstSeries), baseTS+2)

	info, ok := guard.lookupSignalGrouped(cfg, first)
	if !ok {
		t.Fatalf("expected grouped info for first candidate")
	}
	if info.GroupID == "" {
		t.Fatalf("expected group id")
	}
	if info.SelectedCandidateKey != "okx|AGLD/USDT" {
		t.Fatalf("unexpected selected candidate key: %s", info.SelectedCandidateKey)
	}
	if info.CandidateKey != "okx|AGLD/USDT" || info.CandidateState != trendCandidateStateSelected {
		t.Fatalf("unexpected current candidate info: %+v", info)
	}
	if len(info.Candidates) != 2 {
		t.Fatalf("unexpected candidates length: %d", len(info.Candidates))
	}
	if info.Candidates[0].CandidateKey != "okx|AGLD/USDT" || info.Candidates[1].CandidateKey != "okx|FIL/USDT" {
		t.Fatalf("unexpected candidate order: %+v", info.Candidates)
	}
	if info.Candidates[0].CandidateState != trendCandidateStateSelected || !info.Candidates[0].IsSelected {
		t.Fatalf("unexpected selected candidate summary: %+v", info.Candidates[0])
	}
}

func TestGroupedTrendGuardActionZeroKeepsGroupActiveAsNoTrade(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	baseTS := int64(1_773_840_000_000)
	signal := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, signal, groupedTrendGuardEvalContext([]float64{100, 101, 102, 103, 104, 105, 106, 107, 108}), baseTS)

	status := guard.status(cfg)
	if status.GroupsTotal != 1 || status.GroupsActive != 1 {
		t.Fatalf("expected one active no-trade group, got total=%d active=%d", status.GroupsTotal, status.GroupsActive)
	}
	if len(status.Groups) != 1 {
		t.Fatalf("expected one visible group, got %d", len(status.Groups))
	}
	if status.Groups[0].State != trendGroupStateNoTrade {
		t.Fatalf("expected no_trade group state, got %s", status.Groups[0].State)
	}
	if len(status.Groups[0].Candidates) != 1 {
		t.Fatalf("expected one candidate, got %d", len(status.Groups[0].Candidates))
	}
	if status.Groups[0].Candidates[0].CandidateState != trendCandidateStateNoTrade {
		t.Fatalf("expected no_trade candidate state, got %s", status.Groups[0].Candidates[0].CandidateState)
	}
}

func TestGroupedTrendGuardFreezeStopsScoreUpdates(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	leader := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, leader, groupedTrendGuardEvalContext([]float64{100, 103, 106, 109, 112, 115, 118, 121, 124}), baseTS)
	leaderOpen := leader
	leaderOpen.Action = 8
	guard.observeSignal(cfg, leaderOpen, groupedTrendGuardEvalContext([]float64{100, 103, 106, 109, 112, 115, 118, 121, 124}), baseTS+1)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	before := runtime.candidates["okx|FIL/USDT"]
	if before == nil {
		t.Fatalf("expected frozen leader candidate")
	}
	beforeScore := before.PriorityScore
	beforeScoreJSON := before.ScoreJSON

	leader.Action = 0
	guard.observeSignal(cfg, leader, groupedTrendGuardEvalContext([]float64{100, 90, 80, 70, 60, 50, 40, 30, 20}), baseTS+2)

	runtime = groupedTrendGuardSingleRuntime(t, guard)
	after := runtime.candidates["okx|FIL/USDT"]
	if after == nil {
		t.Fatalf("expected leader candidate after freeze")
	}
	if after.PriorityScore != beforeScore {
		t.Fatalf("expected frozen score unchanged, before=%f after=%f", beforeScore, after.PriorityScore)
	}
	if after.ScoreJSON != beforeScoreJSON {
		t.Fatalf("expected frozen score_json unchanged")
	}
}

func TestGroupedTrendGuardBelowThresholdCandidateCannotFreezeLeader(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	weak := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)

	guard.observeSignal(cfg, weak, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS)
	runtime := groupedTrendGuardSingleRuntime(t, guard)
	if score := runtime.candidates["okx|AGLD/USDT"].PriorityScore; score >= cfg.LeaderMinPriorityScore {
		t.Fatalf("expected weak candidate below threshold, got %.2f", score)
	}

	weakOpen := weak
	weakOpen.Action = 8
	guard.observeSignal(cfg, weakOpen, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS+1)

	reason, reject := guard.authorizeOpen(cfg, weakOpen)
	if !reject || reason == "" {
		t.Fatalf("expected below-threshold candidate rejected, got reject=%v reason=%s", reject, reason)
	}
	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one group, got %d", len(status.Groups))
	}
	if status.Groups[0].SelectedCandidateKey != "" {
		t.Fatalf("expected no leader frozen for below-threshold candidate, got %s", status.Groups[0].SelectedCandidateKey)
	}
}

func TestGroupedTrendGuardStatusAlwaysIncludesGroupsArray(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}

	payload, err := json.Marshal(guard.status(cfg))
	if err != nil {
		t.Fatalf("marshal status failed: %v", err)
	}
	if string(payload) == "" {
		t.Fatal("expected non-empty payload")
	}
	if !containsJSONKey(payload, "groups") {
		t.Fatalf("expected groups key in payload: %s", string(payload))
	}
}

func TestGroupedTrendGuardRestoreSkipsFinishedGroups(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	guard.restore(
		[]models.RiskTrendGroup{
			{
				ID:                        1,
				Strategy:                  "turtle",
				PrimaryTimeframe:          "30m",
				Side:                      positionSideLong,
				AnchorTrendingTimestampMS: 1_773_840_000_000,
				State:                     trendGroupStateFinished,
				FinishedAtMS:              1_773_840_060_000,
			},
		},
		[]models.RiskTrendGroupCandidate{
			{
				ID:             1,
				GroupID:        1,
				CandidateKey:   "okx|FIL/USDT",
				Exchange:       "okx",
				Symbol:         "FIL/USDT",
				CandidateState: trendCandidateStateInactive,
			},
		},
	)

	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	status := guard.status(cfg)
	if status.GroupsTotal != 0 || status.GroupsActive != 0 || len(status.Groups) != 0 {
		t.Fatalf("expected finished groups skipped during restore, got total=%d active=%d groups=%d", status.GroupsTotal, status.GroupsActive, len(status.Groups))
	}
}

func TestGroupedTrendGuardRestoreSkipsInactiveCandidatesWithoutPosition(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	guard.restore(
		[]models.RiskTrendGroup{
			{
				ID:                        1,
				Strategy:                  "turtle",
				PrimaryTimeframe:          "30m",
				Side:                      positionSideLong,
				AnchorTrendingTimestampMS: 1_773_840_000_000,
				State:                     trendGroupStateNoTrade,
			},
		},
		[]models.RiskTrendGroupCandidate{
			{
				ID:             1,
				GroupID:        1,
				CandidateKey:   "okx|FIL/USDT",
				Exchange:       "okx",
				Symbol:         "FIL/USDT",
				CandidateState: trendCandidateStateInactive,
			},
			{
				ID:             2,
				GroupID:        1,
				CandidateKey:   "okx|AGLD/USDT",
				Exchange:       "okx",
				Symbol:         "AGLD/USDT",
				CandidateState: trendCandidateStateNoTrade,
			},
		},
	)

	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one group, got %d", len(status.Groups))
	}
	if len(status.Groups[0].Candidates) != 1 {
		t.Fatalf("expected inactive candidate skipped during restore, got %d", len(status.Groups[0].Candidates))
	}
	if status.Groups[0].Candidates[0].CandidateKey != "okx|AGLD/USDT" {
		t.Fatalf("unexpected restored candidate %s", status.Groups[0].Candidates[0].CandidateKey)
	}
}

func TestGroupedTrendGuardReconcileCandidatesPrunesStaleRuntimeRows(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	baseTS := int64(1_773_840_000_000)
	guard.restore(
		[]models.RiskTrendGroup{
			{
				ID:                        1,
				Strategy:                  "turtle",
				PrimaryTimeframe:          "30m",
				Side:                      positionSideLong,
				AnchorTrendingTimestampMS: baseTS,
				State:                     trendGroupStateNoTrade,
			},
		},
		[]models.RiskTrendGroupCandidate{
			{
				ID:                  1,
				GroupID:             1,
				CandidateKey:        "okx|AGLD/USDT",
				Exchange:            "okx",
				Symbol:              "AGLD/USDT",
				CandidateState:      trendCandidateStateNoTrade,
				TrendingTimestampMS: baseTS,
			},
			{
				ID:                  2,
				GroupID:             1,
				CandidateKey:        "okx|FIL/USDT",
				Exchange:            "okx",
				Symbol:              "FIL/USDT",
				CandidateState:      trendCandidateStateNoTrade,
				TrendingTimestampMS: baseTS,
			},
		},
	)
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}

	guard.reconcileCandidates(cfg, groupedTrendGuardGroupedSignals(groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)), baseTS+1)

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one group after reconcile, got %d", len(status.Groups))
	}
	if len(status.Groups[0].Candidates) != 1 {
		t.Fatalf("expected stale candidate pruned after reconcile, got %d", len(status.Groups[0].Candidates))
	}
	if status.Groups[0].Candidates[0].CandidateKey != "okx|FIL/USDT" {
		t.Fatalf("expected remaining candidate okx|FIL/USDT, got %s", status.Groups[0].Candidates[0].CandidateKey)
	}
}

func TestGroupedTrendGuardReconcileCandidatesKeepsOpenPositionWithoutSignal(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	baseTS := int64(1_773_840_000_000)
	guard.restore(
		[]models.RiskTrendGroup{
			{
				ID:                        1,
				Strategy:                  "turtle",
				PrimaryTimeframe:          "30m",
				Side:                      positionSideLong,
				AnchorTrendingTimestampMS: baseTS,
				State:                     trendGroupStateNoTrade,
			},
		},
		[]models.RiskTrendGroupCandidate{
			{
				ID:                  1,
				GroupID:             1,
				CandidateKey:        "okx|FIL/USDT",
				Exchange:            "okx",
				Symbol:              "FIL/USDT",
				CandidateState:      trendCandidateStateNoTrade,
				TrendingTimestampMS: baseTS,
				HasOpenPosition:     true,
			},
		},
	)
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}

	guard.reconcileCandidates(cfg, nil, baseTS+1)

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected open-position candidate group retained, got %d groups", len(status.Groups))
	}
	if len(status.Groups[0].Candidates) != 1 {
		t.Fatalf("expected open-position candidate retained, got %d candidates", len(status.Groups[0].Candidates))
	}
	if !status.Groups[0].Candidates[0].HasOpenPosition {
		t.Fatalf("expected retained candidate to still show open position")
	}
}

func TestGroupedTrendGuardHigherScoreLaterTriggerCannotReplaceFrozenLeader(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	first := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	later := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	firstSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}
	laterSeries := []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}

	guard.observeSignal(cfg, first, groupedTrendGuardEvalContext(firstSeries), baseTS)
	guard.observeSignal(cfg, later, groupedTrendGuardEvalContext(laterSeries), baseTS+1)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	firstScore := runtime.candidates["okx|AGLD/USDT"].PriorityScore
	laterScore := runtime.candidates["okx|FIL/USDT"].PriorityScore
	if firstScore < cfg.LeaderMinPriorityScore || laterScore < cfg.LeaderMinPriorityScore || laterScore <= firstScore {
		t.Fatalf("expected both candidates above threshold and later higher, got first=%.2f later=%.2f", firstScore, laterScore)
	}

	firstOpen := first
	firstOpen.Action = 8
	guard.observeSignal(cfg, firstOpen, groupedTrendGuardEvalContext(firstSeries), baseTS+2)
	if reason, reject := guard.authorizeOpen(cfg, firstOpen); reject {
		t.Fatalf("expected first qualifying trigger allowed, got reject=%v reason=%s", reject, reason)
	}

	laterOpen := later
	laterOpen.Action = 8
	guard.observeSignal(cfg, laterOpen, groupedTrendGuardEvalContext(laterSeries), baseTS+3)
	reason, reject := guard.authorizeOpen(cfg, laterOpen)
	if !reject || reason == "" {
		t.Fatalf("expected later higher-score candidate rejected after leader frozen, got reject=%v reason=%s", reject, reason)
	}

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one group, got %d", len(status.Groups))
	}
	if status.Groups[0].SelectedCandidateKey != "okx|AGLD/USDT" {
		t.Fatalf("expected first qualifying trigger to stay leader, got %s", status.Groups[0].SelectedCandidateKey)
	}
}

func TestGroupedTrendGuardRefreshCandidateUpdatesScoreBeforeFreeze(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	baseTS := int64(1_773_840_000_000)
	signal := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, signal, groupedTrendGuardEvalContext([]float64{100, 101, 102, 103, 104, 105, 106, 107, 108}), baseTS)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	before := runtime.candidates["okx|FIL/USDT"]
	if before == nil {
		t.Fatalf("expected candidate runtime row")
	}
	beforeScore := before.PriorityScore

	guard.refreshCandidate(cfg, signal, groupedTrendGuardEvalContext([]float64{100, 103, 106, 109, 112, 115, 118, 121, 124}), baseTS+1)

	runtime = groupedTrendGuardSingleRuntime(t, guard)
	after := runtime.candidates["okx|FIL/USDT"]
	if after == nil {
		t.Fatalf("expected candidate after refresh")
	}
	if after.PriorityScore == beforeScore {
		t.Fatalf("expected refresh to update score, before=%f after=%f", beforeScore, after.PriorityScore)
	}
	if runtime.group.SelectedCandidateKey != "" {
		t.Fatalf("expected refresh not to freeze leader, got %s", runtime.group.SelectedCandidateKey)
	}
}

func TestGroupedTrendGuardRefreshCandidateDoesNotFreezeOpenAction(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:         true,
		Mode:            trendGuardModeGrouped,
		MaxStartLagBars: 12,
	}
	baseTS := int64(1_773_840_000_000)
	weak := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	strong := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)

	guard.observeSignal(cfg, weak, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS)
	guard.observeSignal(cfg, strong, groupedTrendGuardEvalContext([]float64{100, 102, 104, 105, 107, 109, 111, 113, 115}), baseTS+1)

	weakOpen := weak
	weakOpen.Action = 8
	guard.refreshCandidate(cfg, weakOpen, groupedTrendGuardEvalContext([]float64{100, 101, 99, 100, 99, 100, 98, 99, 98}), baseTS+2)

	status := guard.status(cfg)
	if len(status.Groups) != 1 {
		t.Fatalf("expected one active group, got %d", len(status.Groups))
	}
	if status.Groups[0].SelectedCandidateKey != "" {
		t.Fatalf("expected refresh-only path not to freeze leader, got %s", status.Groups[0].SelectedCandidateKey)
	}
	if status.Groups[0].State != trendGroupStateTracking {
		t.Fatalf("expected tracking state after refresh-only open action, got %s", status.Groups[0].State)
	}
}

func TestGroupedTrendGuardLeaderCloseReopensAndAllowsHigherScoreChallenger(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	incumbent := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	challenger := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	incumbentSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}
	challengerSeries := []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}

	guard.observeSignal(cfg, incumbent, groupedTrendGuardEvalContext(incumbentSeries), baseTS)
	incumbentOpen := incumbent
	incumbentOpen.Action = 8
	guard.observeSignal(cfg, incumbentOpen, groupedTrendGuardEvalContext(incumbentSeries), baseTS+1)
	guard.syncPositions(map[string]models.Position{
		"okx|AGLD-USDT-SWAP|long|isolated": {
			Exchange:           "okx",
			Symbol:             "AGLD/USDT",
			Timeframe:          "30m",
			PositionSide:       positionSideLong,
			Status:             models.PositionStatusOpen,
			EntryQuantity:      1,
			StrategyName:       "turtle",
			StrategyTimeframes: []string{"1m", "5m", "30m"},
			ComboKey:           "1m/5m/30m",
		},
	}, baseTS+2)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	incumbentScore := runtime.group.IncumbentLeaderScore
	if incumbentScore < cfg.LeaderMinPriorityScore {
		t.Fatalf("expected incumbent score above threshold, got %.2f", incumbentScore)
	}

	guard.syncPositions(map[string]models.Position{}, baseTS+4)
	runtime = groupedTrendGuardSingleRuntime(t, guard)
	if runtime.group.SelectedCandidateKey != "" {
		t.Fatalf("expected no selected leader after incumbent close, got %s", runtime.group.SelectedCandidateKey)
	}
	if runtime.group.IncumbentLeaderKey != "okx|AGLD/USDT" || runtime.group.IncumbentLeaderClosedAtMS == 0 {
		t.Fatalf("expected incumbent baseline kept after close, got key=%s closed=%d", runtime.group.IncumbentLeaderKey, runtime.group.IncumbentLeaderClosedAtMS)
	}

	guard.observeSignal(cfg, challenger, groupedTrendGuardEvalContext(challengerSeries), baseTS+5)
	runtime = groupedTrendGuardSingleRuntime(t, guard)
	challengerScore := runtime.candidates["okx|FIL/USDT"].PriorityScore
	if challengerScore <= incumbentScore {
		t.Fatalf("expected challenger score %.2f > incumbent score %.2f", challengerScore, incumbentScore)
	}

	challengerOpen := challenger
	challengerOpen.Action = 8
	guard.observeSignal(cfg, challengerOpen, groupedTrendGuardEvalContext(challengerSeries), baseTS+6)
	if reason, reject := guard.authorizeOpen(cfg, challengerOpen); reject {
		t.Fatalf("expected higher-score challenger allowed after incumbent close, reject=%v reason=%s", reject, reason)
	}
	runtime = groupedTrendGuardSingleRuntime(t, guard)
	if runtime.group.SelectedCandidateKey != "okx|FIL/USDT" {
		t.Fatalf("expected challenger to become new leader, got %s", runtime.group.SelectedCandidateKey)
	}
	if runtime.group.IncumbentLeaderKey != "okx|FIL/USDT" || runtime.group.IncumbentLeaderClosedAtMS != 0 {
		t.Fatalf("expected incumbent baseline switched to new leader, got key=%s closed=%d", runtime.group.IncumbentLeaderKey, runtime.group.IncumbentLeaderClosedAtMS)
	}
}

func TestGroupedTrendGuardLeaderCloseRejectsChallengerNotAboveIncumbent(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	incumbent := groupedTrendGuardTestSignal("okx", "FIL/USDT", baseTS)
	challenger := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	incumbentSeries := []float64{100, 103, 106, 109, 112, 115, 118, 121, 124}
	challengerSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}

	guard.observeSignal(cfg, incumbent, groupedTrendGuardEvalContext(incumbentSeries), baseTS)
	incumbentOpen := incumbent
	incumbentOpen.Action = 8
	guard.observeSignal(cfg, incumbentOpen, groupedTrendGuardEvalContext(incumbentSeries), baseTS+1)
	guard.syncPositions(map[string]models.Position{
		"okx|FIL-USDT-SWAP|long|isolated": {
			Exchange:           "okx",
			Symbol:             "FIL/USDT",
			Timeframe:          "30m",
			PositionSide:       positionSideLong,
			Status:             models.PositionStatusOpen,
			EntryQuantity:      1,
			StrategyName:       "turtle",
			StrategyTimeframes: []string{"1m", "5m", "30m"},
			ComboKey:           "1m/5m/30m",
		},
	}, baseTS+2)

	guard.syncPositions(map[string]models.Position{}, baseTS+4)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	incumbentScore := runtime.group.IncumbentLeaderScore
	guard.observeSignal(cfg, challenger, groupedTrendGuardEvalContext(challengerSeries), baseTS+5)
	runtime = groupedTrendGuardSingleRuntime(t, guard)
	challengerScore := runtime.candidates["okx|AGLD/USDT"].PriorityScore
	if challengerScore < cfg.LeaderMinPriorityScore {
		t.Fatalf("expected challenger still above threshold, got %.2f", challengerScore)
	}
	if challengerScore > incumbentScore {
		t.Fatalf("expected challenger score %.2f <= incumbent score %.2f", challengerScore, incumbentScore)
	}

	challengerOpen := challenger
	challengerOpen.Action = 8
	guard.observeSignal(cfg, challengerOpen, groupedTrendGuardEvalContext(challengerSeries), baseTS+6)
	reason, reject := guard.authorizeOpen(cfg, challengerOpen)
	if !reject || reason == "" {
		t.Fatalf("expected challenger not above incumbent rejected, got reject=%v reason=%s", reject, reason)
	}
	runtime = groupedTrendGuardSingleRuntime(t, guard)
	if runtime.group.SelectedCandidateKey != "" {
		t.Fatalf("expected no new leader after incumbent comparison reject, got %s", runtime.group.SelectedCandidateKey)
	}
}

func TestGroupedTrendGuardLeaderCloseAllowsIncumbentRefreeze(t *testing.T) {
	guard := newGroupedTrendGuard(zap.NewNop(), nil, "live")
	cfg := RiskTrendGuardConfig{
		Enabled:                true,
		Mode:                   trendGuardModeGrouped,
		MaxStartLagBars:        12,
		LeaderMinPriorityScore: 50,
	}
	baseTS := int64(1_773_840_000_000)
	leader := groupedTrendGuardTestSignal("okx", "AGLD/USDT", baseTS)
	leaderSeries := []float64{100, 102, 103, 105, 106, 108, 109, 111, 112}

	guard.observeSignal(cfg, leader, groupedTrendGuardEvalContext(leaderSeries), baseTS)
	leaderOpen := leader
	leaderOpen.Action = 8
	guard.observeSignal(cfg, leaderOpen, groupedTrendGuardEvalContext(leaderSeries), baseTS+1)
	guard.syncPositions(map[string]models.Position{
		"okx|AGLD-USDT-SWAP|long|isolated": {
			Exchange:           "okx",
			Symbol:             "AGLD/USDT",
			Timeframe:          "30m",
			PositionSide:       positionSideLong,
			Status:             models.PositionStatusOpen,
			EntryQuantity:      1,
			StrategyName:       "turtle",
			StrategyTimeframes: []string{"1m", "5m", "30m"},
			ComboKey:           "1m/5m/30m",
		},
	}, baseTS+2)
	guard.syncPositions(map[string]models.Position{}, baseTS+3)

	runtime := groupedTrendGuardSingleRuntime(t, guard)
	if runtime.group.SelectedCandidateKey != "" {
		t.Fatalf("expected leader cleared after close, got %s", runtime.group.SelectedCandidateKey)
	}

	leaderReopen := leader
	leaderReopen.Action = 8
	guard.observeSignal(cfg, leaderReopen, groupedTrendGuardEvalContext(leaderSeries), baseTS+4)
	if reason, reject := guard.authorizeOpen(cfg, leaderReopen); reject {
		t.Fatalf("expected incumbent leader allowed to refreeze, reject=%v reason=%s", reject, reason)
	}

	runtime = groupedTrendGuardSingleRuntime(t, guard)
	if runtime.group.SelectedCandidateKey != "okx|AGLD/USDT" {
		t.Fatalf("expected incumbent leader refrozen, got %s", runtime.group.SelectedCandidateKey)
	}
}

func groupedTrendGuardTestSignal(exchange, symbol string, trendingTS int64) models.Signal {
	return models.Signal{
		Exchange:           exchange,
		Symbol:             symbol,
		Timeframe:          "30m",
		Strategy:           "turtle",
		StrategyTimeframes: []string{"1m", "5m", "30m"},
		ComboKey:           "1m/5m/30m",
		Action:             0,
		HighSide:           1,
		TrendingTimestamp:  int(trendingTS),
		LastMidPullbackTS:  int(trendingTS + 30*60*1000),
	}
}

func containsJSONKey(payload []byte, key string) bool {
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return false
	}
	_, ok := decoded[key]
	return ok
}

func groupedTrendGuardSingleRuntime(t *testing.T, guard *groupedTrendGuard) *trendGuardGroupRuntime {
	t.Helper()
	guard.mu.RLock()
	defer guard.mu.RUnlock()
	if len(guard.groups) != 1 {
		t.Fatalf("expected a single runtime group, got %d", len(guard.groups))
	}
	for _, runtime := range guard.groups {
		return runtime
	}
	return nil
}

func groupedTrendGuardEvalContext(midCloses []float64) models.RiskEvalContext {
	const midTimeframe = "5m"
	const primaryTimeframe = "30m"
	bars := make([]models.OHLCV, 0, len(midCloses))
	baseTS := int64(1_773_840_000_000)
	for idx, closePrice := range midCloses {
		barTS := baseTS + int64(idx)*5*60*1000
		bars = append(bars, models.OHLCV{
			TS:     barTS,
			Open:   closePrice - 1,
			High:   closePrice + 1,
			Low:    closePrice - 2,
			Close:  closePrice,
			Volume: 1,
		})
	}
	primaryBars := []models.OHLCV{}
	for idx := 0; idx < len(bars); idx += 6 {
		bar := bars[idx]
		primaryBars = append(primaryBars, models.OHLCV{
			TS:     bar.TS,
			Open:   bar.Open,
			High:   bar.High,
			Low:    bar.Low,
			Close:  bar.Close,
			Volume: bar.Volume,
		})
	}
	if len(primaryBars) == 0 {
		primaryBars = append(primaryBars, models.OHLCV{
			TS:     baseTS,
			Open:   100,
			High:   101,
			Low:    99,
			Close:  100,
			Volume: 1,
		})
	}
	return models.RiskEvalContext{
		MarketData: models.MarketData{
			Exchange:  "okx",
			Symbol:    "FIL/USDT",
			Timeframe: primaryTimeframe,
			OHLCV: models.OHLCV{
				TS:    bars[len(bars)-1].TS,
				Close: bars[len(bars)-1].Close,
			},
			Closed: true,
		},
		Snapshot: &models.MarketSnapshot{
			Exchange:       "okx",
			Symbol:         "FIL/USDT",
			EventTimeframe: primaryTimeframe,
			EventTS:        bars[len(bars)-1].TS,
			EventClosed:    true,
			Series: map[string][]models.OHLCV{
				midTimeframe:     bars,
				primaryTimeframe: primaryBars,
			},
		},
	}
}

func groupedTrendGuardGroupedSignals(signals ...models.Signal) map[string]map[string]models.Signal {
	grouped := make(map[string]map[string]models.Signal)
	for _, signal := range signals {
		if models.IsEmptySignal(signal) {
			continue
		}
		outer := normalizeExchange(signal.Exchange) + "|" + canonicalSymbol(signal.Symbol)
		if outer == "|" {
			continue
		}
		_, _, comboKey := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
		inner := strings.TrimSpace(signal.Strategy) + "|" + comboKey
		bucket := grouped[outer]
		if bucket == nil {
			bucket = make(map[string]models.Signal)
			grouped[outer] = bucket
		}
		bucket[inner] = signal
	}
	return grouped
}
