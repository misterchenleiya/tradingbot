package risk

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/internal/models"
	glog "github.com/misterchenleiya/tradingbot/log"
	"go.uber.org/zap"
)

const (
	trendGuardModeLegacy  = "legacy"
	trendGuardModeGrouped = "grouped"

	trendGroupStateTracking   = "tracking"
	trendGroupStateSoftLocked = "soft_locked"
	trendGroupStateHardLocked = "hard_locked"
	trendGroupStateNoTrade    = "no_trade"
	trendGroupStateFinished   = "finished"

	trendGroupLockStageNone = ""
	trendGroupLockStageSoft = "soft"
	trendGroupLockStageHard = "hard"

	trendCandidateStateTracking = "tracking"
	trendCandidateStateSelected = "selected"
	trendCandidateStateBlocked  = "blocked"
	trendCandidateStateNoTrade  = "no_trade"
	trendCandidateStateInactive = "inactive"
	trendCandidateStateReversed = "reversed"
)

type groupedTrendGuard struct {
	logger *zap.Logger
	store  trendGuardStore
	mode   string

	mu     sync.RWMutex
	groups map[string]*trendGuardGroupRuntime
}

type trendGuardStore interface {
	ListRiskTrendGroups(mode string) ([]models.RiskTrendGroup, error)
	ListRiskTrendGroupCandidates(mode string) ([]models.RiskTrendGroupCandidate, error)
	UpsertRiskTrendGroup(group *models.RiskTrendGroup) error
	UpsertRiskTrendGroupCandidate(candidate *models.RiskTrendGroupCandidate) error
}

type trendGuardGroupRuntime struct {
	group      models.RiskTrendGroup
	candidates map[string]*models.RiskTrendGroupCandidate
	scoreCtx   map[string]*trendGuardCandidateScoreContext
}

type trendGuardGroupedCandidate struct {
	Strategy          string
	PrimaryTimeframe  string
	MidTimeframe      string
	Side              string
	TrendingTS        int64
	LastMidPullbackTS int64
	Exchange          string
	Symbol            string
	CandidateKey      string
	SignalAction      int
	HighSide          int
	MidSide           int
	TrendEntryCount   int
	MidPullbackCount  int
	EntryWatchTS      int64
}

type trendGuardCandidateScoreContext struct {
	Candidate           trendGuardGroupedCandidate
	ObservedAtMS        int64
	MidSeries           []models.OHLCV
	TrendEntryCount     int
	MidPullbackCount    int
	EntryWatchTimestamp int64
}

type trendGuardStatusDetails struct {
	Enabled      bool                           `json:"enabled"`
	Mode         string                         `json:"mode"`
	GroupsTotal  int                            `json:"groups_total"`
	GroupsActive int                            `json:"groups_active"`
	Groups       []trendGuardStatusGroupSummary `json:"groups"`
}

type trendGuardStatusGroupSummary struct {
	GroupID                   string                             `json:"group_id"`
	Strategy                  string                             `json:"strategy"`
	PrimaryTimeframe          string                             `json:"primary_timeframe"`
	Side                      string                             `json:"side"`
	AnchorTrendingTimestampMS int64                              `json:"anchor_trending_timestamp_ms"`
	State                     string                             `json:"state"`
	LockStage                 string                             `json:"lock_stage"`
	SelectedCandidateKey      string                             `json:"selected_candidate_key"`
	EntryCount                int                                `json:"entry_count"`
	Candidates                []trendGuardStatusCandidateSummary `json:"candidates,omitempty"`
}

type trendGuardStatusCandidateSummary struct {
	CandidateKey    string  `json:"candidate_key"`
	CandidateState  string  `json:"candidate_state"`
	IsSelected      bool    `json:"is_selected"`
	PriorityScore   float64 `json:"priority_score"`
	HasOpenPosition bool    `json:"has_open_position"`
}

func newGroupedTrendGuard(logger *zap.Logger, store trendGuardStore, mode string) *groupedTrendGuard {
	if logger == nil {
		logger = glog.Nop()
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = liveMode
	}
	return &groupedTrendGuard{
		logger: logger,
		store:  store,
		mode:   mode,
		groups: make(map[string]*trendGuardGroupRuntime),
	}
}

func normalizeTrendGuardMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", trendGuardModeLegacy:
		return trendGuardModeLegacy
	case trendGuardModeGrouped:
		return trendGuardModeGrouped
	default:
		return trendGuardModeLegacy
	}
}

func trendGuardGroupedEnabled(cfg RiskTrendGuardConfig) bool {
	return cfg.Enabled && normalizeTrendGuardMode(cfg.Mode) == trendGuardModeGrouped
}

func trendGuardLeaderMinPriorityScore(cfg RiskTrendGuardConfig) float64 {
	if cfg.LeaderMinPriorityScore <= 0 {
		return 50
	}
	return cfg.LeaderMinPriorityScore
}

func (g *groupedTrendGuard) restore(groups []models.RiskTrendGroup, candidates []models.RiskTrendGroupCandidate) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.groups = make(map[string]*trendGuardGroupRuntime, len(groups))
	for _, item := range groups {
		item.Strategy = strings.TrimSpace(item.Strategy)
		item.PrimaryTimeframe = normalizeTrendGuardTimeframe(item.PrimaryTimeframe)
		item.Side = normalizePositionSide(item.Side, 0)
		if item.State == trendGroupStateFinished {
			continue
		}
		key := groupedTrendGroupKey(item.Strategy, item.PrimaryTimeframe, item.Side, item.AnchorTrendingTimestampMS)
		if key == "" {
			continue
		}
		copyItem := item
		g.groups[key] = &trendGuardGroupRuntime{
			group:      copyItem,
			candidates: make(map[string]*models.RiskTrendGroupCandidate),
			scoreCtx:   make(map[string]*trendGuardCandidateScoreContext),
		}
	}
	for _, item := range candidates {
		candidateKey := strings.TrimSpace(item.CandidateKey)
		if candidateKey == "" {
			continue
		}
		if !trendCandidateRetainedInRuntime(&item) {
			continue
		}
		for _, runtime := range g.groups {
			if runtime.group.ID != item.GroupID {
				continue
			}
			copyItem := item
			runtime.candidates[candidateKey] = &copyItem
			break
		}
	}
}

func (g *groupedTrendGuard) reconcileCandidates(cfg RiskTrendGuardConfig, signals map[string]map[string]models.Signal, eventTS int64) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	presentByGroup := g.buildPresentCandidatesByGroupLocked(cfg, signals)
	for _, runtime := range g.groups {
		if runtime == nil || runtime.group.State == trendGroupStateFinished {
			continue
		}
		groupKey := groupedTrendGroupKey(runtime.group.Strategy, runtime.group.PrimaryTimeframe, runtime.group.Side, runtime.group.AnchorTrendingTimestampMS)
		present := presentByGroup[groupKey]
		changed := false
		for key, candidate := range runtime.candidates {
			if candidate == nil {
				delete(runtime.candidates, key)
				delete(runtime.scoreCtx, key)
				changed = true
				continue
			}
			if candidate.HasOpenPosition {
				continue
			}
			if _, ok := present[key]; ok {
				continue
			}
			candidate.CandidateState = trendCandidateStateInactive
			candidate.LastSignalAction = 0
			candidate.UpdatedAtMS = eventTS
			changed = true
		}
		if g.pruneInactiveCandidatesLocked(runtime) {
			changed = true
		}
		if !changed {
			continue
		}
		g.recomputeRuntimeLocked(runtime, eventTS)
		g.persistAndPruneRuntimeLocked(runtime)
	}
}

func (g *groupedTrendGuard) observeSignal(cfg RiskTrendGuardConfig, signal models.Signal, evalCtx models.RiskEvalContext, eventTS int64) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return
	}
	candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
	if !ok {
		return
	}
	if eventTS <= 0 {
		eventTS = normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	runtime := g.findOrCreateRuntimeLocked(candidate, cfg, eventTS)
	changedRuntimes := g.deactivateOtherMembershipsLocked(runtime, candidate, cfg, eventTS)
	candidateRow := runtime.candidates[candidate.CandidateKey]
	if candidateRow == nil {
		candidateRow = &models.RiskTrendGroupCandidate{
			GroupID:        runtime.group.ID,
			CandidateKey:   candidate.CandidateKey,
			Exchange:       candidate.Exchange,
			Symbol:         candidate.Symbol,
			FirstSeenAtMS:  eventTS,
			UpdatedAtMS:    eventTS,
			CandidateState: trendCandidateStateTracking,
		}
		runtime.candidates[candidate.CandidateKey] = candidateRow
	}
	candidateRow.GroupID = runtime.group.ID
	candidateRow.Exchange = candidate.Exchange
	candidateRow.Symbol = candidate.Symbol
	candidateRow.LastSignalAction = candidate.SignalAction
	candidateRow.LastHighSide = candidate.HighSide
	candidateRow.LastMidSide = candidate.MidSide
	candidateRow.TrendingTimestampMS = candidate.TrendingTS
	candidateRow.LastSeenAtMS = eventTS
	candidateRow.UpdatedAtMS = eventTS
	if candidateRow.FirstSeenAtMS <= 0 {
		candidateRow.FirstSeenAtMS = eventTS
	}
	leaderFrozen := trendGroupLeaderFrozen(runtime.group)
	if !leaderFrozen {
		scoreCtx, score, scoreJSON := groupedTrendGuardScoreContextFromSignal(candidate, signal, evalCtx.Snapshot, eventTS)
		if scoreCtx != nil {
			runtime.scoreCtx[candidate.CandidateKey] = scoreCtx
		} else {
			delete(runtime.scoreCtx, candidate.CandidateKey)
		}
		candidateRow.PriorityScore = score
		candidateRow.ScoreJSON = scoreJSON
	}
	if candidate.SignalAction == 8 && !leaderFrozen {
		candidateRow.CandidateState = trendCandidateStateTracking
	} else if !candidateRow.HasOpenPosition && !leaderFrozen {
		candidateRow.CandidateState = trendCandidateStateNoTrade
	} else if leaderFrozen {
		if candidate.CandidateKey == runtime.group.SelectedCandidateKey {
			candidateRow.CandidateState = trendCandidateStateSelected
		} else if !candidateRow.HasOpenPosition {
			candidateRow.CandidateState = trendCandidateStateBlocked
		}
	}
	if candidate.SignalAction == 8 && !leaderFrozen {
		if !g.freezeLeaderLocked(runtime, candidate.CandidateKey, cfg, eventTS) {
			g.recomputeRuntimeLocked(runtime, eventTS)
		}
	} else {
		g.recomputeRuntimeLocked(runtime, eventTS)
	}
	g.pruneInactiveCandidatesLocked(runtime)
	g.persistAndPruneRuntimeLocked(runtime)
	for _, changed := range changedRuntimes {
		g.pruneInactiveCandidatesLocked(changed)
		g.recomputeRuntimeLocked(changed, eventTS)
		g.persistAndPruneRuntimeLocked(changed)
	}
}

func (g *groupedTrendGuard) refreshCandidate(cfg RiskTrendGuardConfig, signal models.Signal, evalCtx models.RiskEvalContext, eventTS int64) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return
	}
	candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
	if !ok {
		return
	}
	if eventTS <= 0 {
		eventTS = normalizeTimestampMS(evalCtx.MarketData.OHLCV.TS)
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	runtime := g.findRuntimeLocked(candidate, cfg)
	if runtime == nil || trendGroupLeaderFrozen(runtime.group) {
		return
	}
	candidateRow := runtime.candidates[candidate.CandidateKey]
	if candidateRow == nil {
		return
	}
	if candidateRow.CandidateState == trendCandidateStateInactive || candidateRow.CandidateState == trendCandidateStateReversed {
		return
	}
	candidateRow.GroupID = runtime.group.ID
	candidateRow.Exchange = candidate.Exchange
	candidateRow.Symbol = candidate.Symbol
	candidateRow.LastSignalAction = candidate.SignalAction
	candidateRow.LastHighSide = candidate.HighSide
	candidateRow.LastMidSide = candidate.MidSide
	candidateRow.TrendingTimestampMS = candidate.TrendingTS
	candidateRow.LastSeenAtMS = eventTS
	candidateRow.UpdatedAtMS = eventTS
	if candidateRow.FirstSeenAtMS <= 0 {
		candidateRow.FirstSeenAtMS = eventTS
	}
	scoreCtx, score, scoreJSON := groupedTrendGuardScoreContextFromSignal(candidate, signal, evalCtx.Snapshot, eventTS)
	if scoreCtx != nil {
		runtime.scoreCtx[candidate.CandidateKey] = scoreCtx
	} else {
		delete(runtime.scoreCtx, candidate.CandidateKey)
	}
	candidateRow.PriorityScore = score
	candidateRow.ScoreJSON = scoreJSON
	g.recomputeRuntimeLocked(runtime, eventTS)
	g.persistAndPruneRuntimeLocked(runtime)
}

func (g *groupedTrendGuard) markSignalGone(cfg RiskTrendGuardConfig, signal models.Signal, eventTS int64) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return
	}
	candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
	if !ok {
		return
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	runtime := g.findRuntimeLocked(candidate, cfg)
	if runtime == nil {
		return
	}
	candidateRow := runtime.candidates[candidate.CandidateKey]
	if candidateRow == nil {
		return
	}
	candidateRow.LastSignalAction = 0
	candidateRow.UpdatedAtMS = eventTS
	if candidateRow.HasOpenPosition {
		if candidateRow.IsSelected {
			candidateRow.CandidateState = trendCandidateStateSelected
		} else {
			candidateRow.CandidateState = trendCandidateStateBlocked
		}
	} else {
		candidateRow.CandidateState = trendCandidateStateInactive
		delete(runtime.scoreCtx, candidate.CandidateKey)
	}
	g.pruneInactiveCandidatesLocked(runtime)
	g.recomputeRuntimeLocked(runtime, eventTS)
	g.persistAndPruneRuntimeLocked(runtime)
}

func (g *groupedTrendGuard) authorizeOpen(cfg RiskTrendGuardConfig, signal models.Signal) (string, bool) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return "", false
	}
	candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
	if !ok {
		return "", false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	runtime := g.findRuntimeLocked(candidate, cfg)
	if runtime == nil {
		return "", false
	}
	if runtime.group.SelectedCandidateKey == "" {
		if reason, ok := g.freezeReadyReasonLocked(runtime, candidate.CandidateKey, cfg); !ok {
			return reason, true
		}
		return fmt.Sprintf(
			"trend group leader not frozen candidate=%s strategy=%s timeframe=%s",
			candidate.CandidateKey,
			runtime.group.Strategy,
			runtime.group.PrimaryTimeframe,
		), true
	}
	if runtime.group.SelectedCandidateKey == candidate.CandidateKey {
		return "", false
	}
	return fmt.Sprintf(
		"trend group locked by %s strategy=%s timeframe=%s state=%s",
		runtime.group.SelectedCandidateKey,
		runtime.group.Strategy,
		runtime.group.PrimaryTimeframe,
		runtime.group.State,
	), true
}

func (g *groupedTrendGuard) syncPositions(positions map[string]models.Position, eventTS int64) {
	if g == nil {
		return
	}
	if eventTS <= 0 {
		eventTS = time.Now().UnixMilli()
	}
	type openOwner struct {
		candidateKey     string
		strategy         string
		primaryTimeframe string
		side             string
	}
	openByCandidate := make(map[string][]openOwner)
	for _, pos := range positions {
		if !isPositionOpen(pos) {
			continue
		}
		candidateKey := groupedTrendCandidateKey(pos.Exchange, pos.Symbol)
		if candidateKey == "" {
			continue
		}
		primary, _, _ := common.NormalizeStrategyIdentity(pos.Timeframe, pos.StrategyTimeframes, pos.ComboKey)
		openByCandidate[candidateKey] = append(openByCandidate[candidateKey], openOwner{
			candidateKey:     candidateKey,
			strategy:         strings.TrimSpace(pos.StrategyName),
			primaryTimeframe: normalizeTrendGuardTimeframe(primary),
			side:             normalizePositionSide(pos.PositionSide, 0),
		})
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	for _, runtime := range g.groups {
		changed := false
		for key, candidate := range runtime.candidates {
			isOpen := false
			for _, owner := range openByCandidate[key] {
				if owner.strategy == runtime.group.Strategy && owner.primaryTimeframe == runtime.group.PrimaryTimeframe && owner.side == runtime.group.Side {
					isOpen = true
					break
				}
			}
			if candidate.HasOpenPosition == isOpen {
				continue
			}
			candidate.HasOpenPosition = isOpen
			candidate.UpdatedAtMS = eventTS
			if isOpen {
				candidate.EnteredCount++
				if candidate.FirstEntryAtMS <= 0 {
					candidate.FirstEntryAtMS = eventTS
				}
				candidate.LastEntryAtMS = eventTS
				if runtime.group.FirstEntryAtMS <= 0 {
					runtime.group.FirstEntryAtMS = eventTS
				}
				runtime.group.LastEntryAtMS = eventTS
				runtime.group.EntryCount++
				if runtime.group.SelectedCandidateKey == "" {
					runtime.group.SelectedCandidateKey = candidate.CandidateKey
				}
				runtime.group.IncumbentLeaderKey = candidate.CandidateKey
				runtime.group.IncumbentLeaderScore = candidate.PriorityScore
				runtime.group.IncumbentLeaderClosedAtMS = 0
				if runtime.group.SelectedCandidateKey == candidate.CandidateKey {
					runtime.group.LockStage = trendGroupLockStageHard
					runtime.group.State = trendGroupStateHardLocked
				}
			} else {
				candidate.LastExitAtMS = eventTS
				if runtime.group.LockStage == trendGroupLockStageHard && runtime.group.SelectedCandidateKey == candidate.CandidateKey {
					runtime.group.IncumbentLeaderKey = candidate.CandidateKey
					runtime.group.IncumbentLeaderScore = candidate.PriorityScore
					runtime.group.IncumbentLeaderClosedAtMS = eventTS
					runtime.group.SelectedCandidateKey = ""
					runtime.group.LockStage = trendGroupLockStageNone
				}
				if candidate.LastSignalAction == 0 {
					candidate.CandidateState = trendCandidateStateInactive
					delete(runtime.scoreCtx, key)
				}
			}
			changed = true
		}
		if changed {
			g.pruneInactiveCandidatesLocked(runtime)
			g.recomputeRuntimeLocked(runtime, eventTS)
			g.persistAndPruneRuntimeLocked(runtime)
		}
	}
}

func (g *groupedTrendGuard) lookupSignalGrouped(cfg RiskTrendGuardConfig, signal models.Signal) (models.SignalGroupedInfo, bool) {
	if g == nil || !trendGuardGroupedEnabled(cfg) {
		return models.SignalGroupedInfo{}, false
	}
	candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
	if !ok {
		return models.SignalGroupedInfo{}, false
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	runtime := g.findRuntimeLocked(candidate, cfg)
	if runtime == nil {
		return models.SignalGroupedInfo{}, false
	}
	summary := buildTrendGuardStatusGroupSummary(runtime)
	return buildSignalGroupedInfo(summary, candidate.CandidateKey), true
}

func (g *groupedTrendGuard) status(cfg RiskTrendGuardConfig) trendGuardStatusDetails {
	details := trendGuardStatusDetails{
		Enabled: cfg.Enabled,
		Mode:    normalizeTrendGuardMode(cfg.Mode),
		Groups:  []trendGuardStatusGroupSummary{},
	}
	if g == nil || !cfg.Enabled {
		return details
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	details.GroupsTotal = len(g.groups)
	groups := make([]trendGuardStatusGroupSummary, 0, len(g.groups))
	for _, runtime := range g.groups {
		if runtime.group.State == trendGroupStateFinished {
			continue
		}
		details.GroupsActive++
		groups = append(groups, buildTrendGuardStatusGroupSummary(runtime))
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Strategy != groups[j].Strategy {
			return groups[i].Strategy < groups[j].Strategy
		}
		if groups[i].PrimaryTimeframe != groups[j].PrimaryTimeframe {
			return groups[i].PrimaryTimeframe < groups[j].PrimaryTimeframe
		}
		if groups[i].Side != groups[j].Side {
			return groups[i].Side < groups[j].Side
		}
		return groups[i].AnchorTrendingTimestampMS < groups[j].AnchorTrendingTimestampMS
	})
	details.Groups = groups
	return details
}

func buildTrendGuardStatusGroupSummary(runtime *trendGuardGroupRuntime) trendGuardStatusGroupSummary {
	if runtime == nil {
		return trendGuardStatusGroupSummary{}
	}
	summary := trendGuardStatusGroupSummary{
		GroupID:                   groupedTrendGroupKey(runtime.group.Strategy, runtime.group.PrimaryTimeframe, runtime.group.Side, runtime.group.AnchorTrendingTimestampMS),
		Strategy:                  runtime.group.Strategy,
		PrimaryTimeframe:          runtime.group.PrimaryTimeframe,
		Side:                      runtime.group.Side,
		AnchorTrendingTimestampMS: runtime.group.AnchorTrendingTimestampMS,
		State:                     runtime.group.State,
		LockStage:                 runtime.group.LockStage,
		SelectedCandidateKey:      runtime.group.SelectedCandidateKey,
		EntryCount:                runtime.group.EntryCount,
	}
	candidateKeys := make([]string, 0, len(runtime.candidates))
	for key := range runtime.candidates {
		candidateKeys = append(candidateKeys, key)
	}
	sort.Strings(candidateKeys)
	for _, key := range candidateKeys {
		candidate := runtime.candidates[key]
		if candidate == nil {
			continue
		}
		summary.Candidates = append(summary.Candidates, buildTrendGuardStatusCandidateSummary(candidate))
	}
	return summary
}

func buildTrendGuardStatusCandidateSummary(candidate *models.RiskTrendGroupCandidate) trendGuardStatusCandidateSummary {
	if candidate == nil {
		return trendGuardStatusCandidateSummary{}
	}
	return trendGuardStatusCandidateSummary{
		CandidateKey:    candidate.CandidateKey,
		CandidateState:  candidate.CandidateState,
		IsSelected:      candidate.IsSelected,
		PriorityScore:   candidate.PriorityScore,
		HasOpenPosition: candidate.HasOpenPosition,
	}
}

func buildSignalGroupedInfo(summary trendGuardStatusGroupSummary, candidateKey string) models.SignalGroupedInfo {
	info := models.SignalGroupedInfo{
		GroupID:                   summary.GroupID,
		Strategy:                  summary.Strategy,
		PrimaryTimeframe:          summary.PrimaryTimeframe,
		Side:                      summary.Side,
		AnchorTrendingTimestampMS: summary.AnchorTrendingTimestampMS,
		State:                     summary.State,
		LockStage:                 summary.LockStage,
		SelectedCandidateKey:      summary.SelectedCandidateKey,
		EntryCount:                summary.EntryCount,
		CandidateKey:              candidateKey,
	}
	info.Candidates = make([]models.SignalGroupedCandidate, 0, len(summary.Candidates))
	for _, item := range summary.Candidates {
		candidate := models.SignalGroupedCandidate{
			CandidateKey:    item.CandidateKey,
			CandidateState:  item.CandidateState,
			IsSelected:      item.IsSelected,
			PriorityScore:   item.PriorityScore,
			HasOpenPosition: item.HasOpenPosition,
		}
		info.Candidates = append(info.Candidates, candidate)
		if item.CandidateKey != candidateKey {
			continue
		}
		info.CandidateState = item.CandidateState
		info.IsSelected = item.IsSelected
		info.PriorityScore = item.PriorityScore
		info.HasOpenPosition = item.HasOpenPosition
	}
	if info.CandidateState == "" {
		info.IsSelected = info.SelectedCandidateKey != "" && info.SelectedCandidateKey == candidateKey
	}
	return info
}

func trendGroupIncumbentClosed(group models.RiskTrendGroup) bool {
	return strings.TrimSpace(group.IncumbentLeaderKey) != "" && group.IncumbentLeaderClosedAtMS > 0
}

func (g *groupedTrendGuard) findRuntimeLocked(candidate trendGuardGroupedCandidate, cfg RiskTrendGuardConfig) *trendGuardGroupRuntime {
	var selected *trendGuardGroupRuntime
	bestDiff := int64(math.MaxInt64)
	for _, runtime := range g.groups {
		if runtime.group.State == trendGroupStateFinished {
			continue
		}
		if runtime.group.Strategy != candidate.Strategy || runtime.group.PrimaryTimeframe != candidate.PrimaryTimeframe || runtime.group.Side != candidate.Side {
			continue
		}
		if !trendGuardWithinLag(candidate.TrendingTS, runtime.group.AnchorTrendingTimestampMS, candidate.PrimaryTimeframe, cfg.MaxStartLagBars) {
			continue
		}
		diff := absInt64(candidate.TrendingTS - runtime.group.AnchorTrendingTimestampMS)
		if selected == nil || diff < bestDiff || (diff == bestDiff && runtime.group.AnchorTrendingTimestampMS < selected.group.AnchorTrendingTimestampMS) {
			selected = runtime
			bestDiff = diff
		}
	}
	return selected
}

func (g *groupedTrendGuard) findOrCreateRuntimeLocked(candidate trendGuardGroupedCandidate, cfg RiskTrendGuardConfig, eventTS int64) *trendGuardGroupRuntime {
	if runtime := g.findRuntimeLocked(candidate, cfg); runtime != nil {
		return runtime
	}
	group := models.RiskTrendGroup{
		Mode:                      g.mode,
		Strategy:                  candidate.Strategy,
		PrimaryTimeframe:          candidate.PrimaryTimeframe,
		Side:                      candidate.Side,
		AnchorTrendingTimestampMS: candidate.TrendingTS,
		State:                     trendGroupStateTracking,
		LockStage:                 trendGroupLockStageNone,
		CreatedAtMS:               eventTS,
		UpdatedAtMS:               eventTS,
	}
	runtime := &trendGuardGroupRuntime{
		group:      group,
		candidates: make(map[string]*models.RiskTrendGroupCandidate),
		scoreCtx:   make(map[string]*trendGuardCandidateScoreContext),
	}
	key := groupedTrendGroupKey(group.Strategy, group.PrimaryTimeframe, group.Side, group.AnchorTrendingTimestampMS)
	g.groups[key] = runtime
	g.persistRuntimeLocked(runtime)
	return runtime
}

func (g *groupedTrendGuard) deactivateOtherMembershipsLocked(target *trendGuardGroupRuntime, candidate trendGuardGroupedCandidate, cfg RiskTrendGuardConfig, eventTS int64) []*trendGuardGroupRuntime {
	if g == nil || target == nil {
		return nil
	}
	changed := make([]*trendGuardGroupRuntime, 0)
	for _, runtime := range g.groups {
		if runtime == nil || runtime == target {
			continue
		}
		if runtime.group.State == trendGroupStateFinished {
			continue
		}
		if runtime.group.Strategy != candidate.Strategy || runtime.group.PrimaryTimeframe != candidate.PrimaryTimeframe {
			continue
		}
		candidateRow := runtime.candidates[candidate.CandidateKey]
		if candidateRow == nil {
			continue
		}
		nextState := trendCandidateStateInactive
		if runtime.group.Side != candidate.Side {
			nextState = trendCandidateStateReversed
		} else if trendGuardWithinLag(candidate.TrendingTS, runtime.group.AnchorTrendingTimestampMS, runtime.group.PrimaryTimeframe, cfg.MaxStartLagBars) {
			// Same candidate should belong to at most one active runtime; if we selected another
			// runtime for this signal, retire this membership to avoid duplicated groups.
			nextState = trendCandidateStateInactive
		}
		candidateRow.LastSignalAction = 0
		candidateRow.UpdatedAtMS = eventTS
		if !candidateRow.HasOpenPosition {
			candidateRow.CandidateState = nextState
			delete(runtime.scoreCtx, candidate.CandidateKey)
		}
		changed = append(changed, runtime)
	}
	return changed
}

func (g *groupedTrendGuard) buildPresentCandidatesByGroupLocked(cfg RiskTrendGuardConfig, signals map[string]map[string]models.Signal) map[string]map[string]struct{} {
	present := make(map[string]map[string]struct{})
	for _, groupedSignals := range signals {
		for _, signal := range groupedSignals {
			if models.IsEmptySignal(signal) {
				continue
			}
			candidate, ok := groupedTrendGuardCandidateFromSignal(signal)
			if !ok {
				continue
			}
			runtime := g.findRuntimeLocked(candidate, cfg)
			if runtime == nil {
				continue
			}
			groupKey := groupedTrendGroupKey(runtime.group.Strategy, runtime.group.PrimaryTimeframe, runtime.group.Side, runtime.group.AnchorTrendingTimestampMS)
			if groupKey == "" {
				continue
			}
			bucket := present[groupKey]
			if bucket == nil {
				bucket = make(map[string]struct{})
				present[groupKey] = bucket
			}
			bucket[candidate.CandidateKey] = struct{}{}
		}
	}
	return present
}

func (g *groupedTrendGuard) recomputeRuntimeLocked(runtime *trendGuardGroupRuntime, eventTS int64) {
	if runtime == nil {
		return
	}
	group := &runtime.group
	active := make([]*models.RiskTrendGroupCandidate, 0, len(runtime.candidates))
	for _, candidate := range runtime.candidates {
		if candidate == nil {
			continue
		}
		candidate.IsSelected = false
		if trendCandidateActive(candidate) {
			active = append(active, candidate)
		}
	}
	if len(active) == 0 {
		group.State = trendGroupStateFinished
		group.LockStage = trendGroupLockStageNone
		group.FinishedAtMS = eventTS
		group.UpdatedAtMS = eventTS
		return
	}
	if trendGroupLeaderFrozen(*group) {
		group.State = trendGroupStateSoftLocked
		if group.LockStage == trendGroupLockStageHard {
			group.State = trendGroupStateHardLocked
		}
		if selected := runtime.candidates[group.SelectedCandidateKey]; selected != nil {
			selected.IsSelected = true
		}
		for _, candidate := range active {
			if candidate.CandidateKey == group.SelectedCandidateKey {
				candidate.IsSelected = true
				if candidate.HasOpenPosition || (candidate.CandidateState != trendCandidateStateInactive && candidate.CandidateState != trendCandidateStateReversed) {
					candidate.CandidateState = trendCandidateStateSelected
				}
			} else if candidate.CandidateState != trendCandidateStateInactive && candidate.CandidateState != trendCandidateStateReversed {
				candidate.CandidateState = trendCandidateStateBlocked
			}
			candidate.UpdatedAtMS = eventTS
		}
		group.UpdatedAtMS = eventTS
		return
	}
	hasOpenCandidate := false
	for _, candidate := range active {
		if candidate.HasOpenPosition || candidate.LastSignalAction == 8 {
			hasOpenCandidate = true
			break
		}
	}
	if !hasOpenCandidate {
		group.State = trendGroupStateNoTrade
		group.LockStage = trendGroupLockStageNone
		group.SelectedCandidateKey = ""
		for _, candidate := range active {
			if !candidate.HasOpenPosition {
				candidate.CandidateState = trendCandidateStateNoTrade
			}
			candidate.UpdatedAtMS = eventTS
		}
		group.UpdatedAtMS = eventTS
		return
	}
	for _, candidate := range active {
		switch {
		case candidate.HasOpenPosition || candidate.LastSignalAction == 8:
			candidate.CandidateState = trendCandidateStateTracking
		default:
			candidate.CandidateState = trendCandidateStateNoTrade
		}
		candidate.UpdatedAtMS = eventTS
	}
	group.State = trendGroupStateTracking
	group.LockStage = trendGroupLockStageNone
	group.UpdatedAtMS = eventTS
}

func (g *groupedTrendGuard) freezeLeaderLocked(runtime *trendGuardGroupRuntime, candidateKey string, cfg RiskTrendGuardConfig, eventTS int64) bool {
	if runtime == nil {
		return false
	}
	group := &runtime.group
	active := make([]*models.RiskTrendGroupCandidate, 0, len(runtime.candidates))
	for _, candidate := range runtime.candidates {
		if candidate == nil {
			continue
		}
		candidate.IsSelected = false
		if trendCandidateActive(candidate) {
			active = append(active, candidate)
		}
	}
	if len(active) == 0 {
		group.State = trendGroupStateFinished
		group.LockStage = trendGroupLockStageNone
		group.FinishedAtMS = eventTS
		group.UpdatedAtMS = eventTS
		return false
	}
	if _, ok := g.freezeReadyReasonLocked(runtime, candidateKey, cfg); !ok {
		return false
	}
	leader := runtime.candidates[candidateKey]
	if leader == nil || !trendCandidateActive(leader) {
		return false
	}
	if ctx := runtime.scoreCtx[leader.CandidateKey]; ctx != nil {
		score, scoreJSON := groupedTrendGuardScoreFromContext(ctx)
		leader.PriorityScore = score
		leader.ScoreJSON = scoreJSON
		leader.UpdatedAtMS = maxInt64(leader.UpdatedAtMS, ctx.ObservedAtMS)
	}
	group.IncumbentLeaderKey = leader.CandidateKey
	group.IncumbentLeaderScore = leader.PriorityScore
	group.IncumbentLeaderClosedAtMS = 0
	group.SelectedCandidateKey = leader.CandidateKey
	group.LockStage = trendGroupLockStageSoft
	group.State = trendGroupStateSoftLocked
	for _, candidate := range runtime.candidates {
		if candidate == nil {
			continue
		}
		candidate.IsSelected = candidate.CandidateKey == leader.CandidateKey
		switch {
		case candidate.CandidateKey == leader.CandidateKey:
			candidate.CandidateState = trendCandidateStateSelected
		case trendCandidateActive(candidate):
			candidate.CandidateState = trendCandidateStateBlocked
		}
		candidate.UpdatedAtMS = eventTS
	}
	group.UpdatedAtMS = eventTS
	return true
}

func (g *groupedTrendGuard) freezeReadyReasonLocked(runtime *trendGuardGroupRuntime, candidateKey string, cfg RiskTrendGuardConfig) (string, bool) {
	if runtime == nil {
		return "", true
	}
	if trendGroupLeaderFrozen(runtime.group) || runtime.group.SelectedCandidateKey != "" {
		return "", true
	}
	candidate := runtime.candidates[candidateKey]
	if candidate == nil || !trendCandidateActive(candidate) {
		return "", true
	}
	ctx := runtime.scoreCtx[candidate.CandidateKey]
	if ctx == nil {
		return fmt.Sprintf(
			"trend guard grouped: waiting candidate score refresh candidate=%s strategy=%s timeframe=%s",
			candidate.CandidateKey,
			runtime.group.Strategy,
			runtime.group.PrimaryTimeframe,
		), false
	}
	score, _ := groupedTrendGuardScoreFromContext(ctx)
	threshold := trendGuardLeaderMinPriorityScore(cfg)
	if score < threshold {
		return fmt.Sprintf(
			"trend guard grouped: candidate priority score %.2f below leader threshold %.2f candidate=%s strategy=%s timeframe=%s",
			score,
			threshold,
			candidate.CandidateKey,
			runtime.group.Strategy,
			runtime.group.PrimaryTimeframe,
		), false
	}
	if trendGroupIncumbentClosed(runtime.group) && runtime.group.IncumbentLeaderKey != candidate.CandidateKey && score <= runtime.group.IncumbentLeaderScore {
		return fmt.Sprintf(
			"trend guard grouped: candidate priority score %.2f not above incumbent leader %.2f candidate=%s incumbent=%s strategy=%s timeframe=%s",
			score,
			runtime.group.IncumbentLeaderScore,
			candidate.CandidateKey,
			runtime.group.IncumbentLeaderKey,
			runtime.group.Strategy,
			runtime.group.PrimaryTimeframe,
		), false
	}
	return "", true
}

func (g *groupedTrendGuard) persistRuntimeLocked(runtime *trendGuardGroupRuntime) {
	if g == nil || g.store == nil || runtime == nil {
		return
	}
	group := runtime.group
	group.Mode = g.mode
	if err := g.store.UpsertRiskTrendGroup(&group); err != nil {
		g.logger.Warn("risk trend guard persist group failed", zap.Error(err), zap.String("strategy", runtime.group.Strategy), zap.String("timeframe", runtime.group.PrimaryTimeframe), zap.String("side", runtime.group.Side))
		return
	}
	runtime.group = group
	for _, candidate := range runtime.candidates {
		if candidate == nil {
			continue
		}
		candidate.GroupID = runtime.group.ID
		candidate.Mode = g.mode
		if err := g.store.UpsertRiskTrendGroupCandidate(candidate); err != nil {
			g.logger.Warn("risk trend guard persist candidate failed", zap.Error(err), zap.String("candidate_key", candidate.CandidateKey), zap.Int64("group_id", candidate.GroupID))
		}
	}
}

func (g *groupedTrendGuard) persistCandidateSnapshotLocked(runtime *trendGuardGroupRuntime, candidate *models.RiskTrendGroupCandidate) {
	if g == nil || g.store == nil || runtime == nil || candidate == nil {
		return
	}
	if runtime.group.ID <= 0 {
		g.persistRuntimeLocked(runtime)
	}
	candidate.GroupID = runtime.group.ID
	candidate.Mode = g.mode
	if candidate.GroupID <= 0 {
		return
	}
	if err := g.store.UpsertRiskTrendGroupCandidate(candidate); err != nil {
		g.logger.Warn("risk trend guard persist candidate failed", zap.Error(err), zap.String("candidate_key", candidate.CandidateKey), zap.Int64("group_id", candidate.GroupID))
	}
}

func (g *groupedTrendGuard) pruneInactiveCandidatesLocked(runtime *trendGuardGroupRuntime) bool {
	if runtime == nil {
		return false
	}
	pruned := false
	for key, candidate := range runtime.candidates {
		if trendCandidateRetainedInRuntime(candidate) {
			continue
		}
		g.persistCandidateSnapshotLocked(runtime, candidate)
		delete(runtime.candidates, key)
		delete(runtime.scoreCtx, key)
		pruned = true
	}
	return pruned
}

func (g *groupedTrendGuard) persistAndPruneRuntimeLocked(runtime *trendGuardGroupRuntime) {
	if runtime == nil {
		return
	}
	g.persistRuntimeLocked(runtime)
	if runtime.group.State != trendGroupStateFinished {
		return
	}
	key := groupedTrendGroupKey(runtime.group.Strategy, runtime.group.PrimaryTimeframe, runtime.group.Side, runtime.group.AnchorTrendingTimestampMS)
	if key == "" {
		return
	}
	delete(g.groups, key)
}

func groupedTrendGuardCandidateFromSignal(signal models.Signal) (trendGuardGroupedCandidate, bool) {
	strategy := strings.TrimSpace(signal.Strategy)
	primary, normalizedTimeframes, _ := common.NormalizeStrategyIdentity(signal.Timeframe, signal.StrategyTimeframes, signal.ComboKey)
	primary = normalizeTrendGuardTimeframe(primary)
	mid := normalizeTrendGuardTimeframe(primary)
	if len(normalizedTimeframes) >= 2 {
		mid = normalizeTrendGuardTimeframe(normalizedTimeframes[len(normalizedTimeframes)-2])
	}
	side := normalizePositionSide("", signal.HighSide)
	trendingTS := normalizeTimestampMS(int64(signal.TrendingTimestamp))
	lastMidPullbackTS := normalizeTimestampMS(int64(signal.LastMidPullbackTS))
	exchange := normalizeExchange(signal.Exchange)
	symbol := canonicalSymbol(signal.Symbol)
	candidateKey := groupedTrendCandidateKey(exchange, symbol)
	if strategy == "" || primary == "" || mid == "" || side == "" || trendingTS <= 0 || candidateKey == "" {
		return trendGuardGroupedCandidate{}, false
	}
	return trendGuardGroupedCandidate{
		Strategy:          strategy,
		PrimaryTimeframe:  primary,
		MidTimeframe:      mid,
		Side:              side,
		TrendingTS:        trendingTS,
		LastMidPullbackTS: lastMidPullbackTS,
		Exchange:          exchange,
		Symbol:            symbol,
		CandidateKey:      candidateKey,
		SignalAction:      signal.Action,
		HighSide:          signal.HighSide,
		MidSide:           signal.MidSide,
		TrendEntryCount:   signal.TrendEntryCount,
		MidPullbackCount:  signal.MidPullbackCount,
		EntryWatchTS:      normalizeTimestampMS(int64(signal.EntryWatchTimestamp)),
	}, true
}

func groupedTrendCandidateKey(exchange, symbol string) string {
	exchange = normalizeExchange(exchange)
	symbol = canonicalSymbol(symbol)
	if exchange == "" || symbol == "" {
		return ""
	}
	return exchange + "|" + symbol
}

func groupedTrendGroupKey(strategy, primaryTimeframe, side string, anchorTrendingTimestampMS int64) string {
	strategy = strings.TrimSpace(strategy)
	primaryTimeframe = normalizeTrendGuardTimeframe(primaryTimeframe)
	side = normalizePositionSide(side, 0)
	anchorTrendingTimestampMS = normalizeTimestampMS(anchorTrendingTimestampMS)
	if strategy == "" || primaryTimeframe == "" || side == "" || anchorTrendingTimestampMS <= 0 {
		return ""
	}
	return strategy + "|" + primaryTimeframe + "|" + side + "|" + fmt.Sprintf("%d", anchorTrendingTimestampMS)
}

func trendGuardWithinLag(candidateTS, anchorTS int64, timeframe string, maxStartLagBars int) bool {
	if candidateTS <= 0 || anchorTS <= 0 || maxStartLagBars <= 0 {
		return false
	}
	dur, ok := trendGuardTimeframeDuration(timeframe)
	if !ok {
		return false
	}
	maxLagMS := int64(maxStartLagBars) * dur.Milliseconds()
	if maxLagMS <= 0 {
		return false
	}
	return absInt64(candidateTS-anchorTS) <= maxLagMS
}

func groupedTrendGuardScoreContextFromSignal(candidate trendGuardGroupedCandidate, signal models.Signal, snapshot *models.MarketSnapshot, observedAtMS int64) (*trendGuardCandidateScoreContext, float64, string) {
	ctx := groupedTrendGuardBuildScoreContext(candidate, signal, snapshot, observedAtMS)
	if ctx == nil {
		score, scoreJSON := groupedTrendGuardMissingScore(candidate)
		return nil, score, scoreJSON
	}
	score, scoreJSON := groupedTrendGuardScoreFromContext(ctx)
	return ctx, score, scoreJSON
}

func groupedTrendGuardBuildScoreContext(candidate trendGuardGroupedCandidate, signal models.Signal, snapshot *models.MarketSnapshot, observedAtMS int64) *trendGuardCandidateScoreContext {
	if snapshot == nil {
		return nil
	}
	scoreTimeframe := candidate.MidTimeframe
	if scoreTimeframe == "" {
		scoreTimeframe = candidate.PrimaryTimeframe
	}
	series := snapshot.Series[scoreTimeframe]
	if len(series) == 0 {
		return nil
	}
	bars := make([]models.OHLCV, 0, len(series))
	for _, bar := range series {
		if normalizeTimestampMS(bar.TS) <= 0 {
			continue
		}
		bars = append(bars, bar)
	}
	if len(bars) == 0 {
		return nil
	}
	return &trendGuardCandidateScoreContext{
		Candidate:           candidate,
		ObservedAtMS:        observedAtMS,
		MidSeries:           append([]models.OHLCV(nil), bars...),
		TrendEntryCount:     signal.TrendEntryCount,
		MidPullbackCount:    signal.MidPullbackCount,
		EntryWatchTimestamp: normalizeTimestampMS(int64(signal.EntryWatchTimestamp)),
	}
}

func groupedTrendGuardMissingScore(candidate trendGuardGroupedCandidate) (float64, string) {
	breakdown := models.TrendQualityScoreBreakdown{SnapshotMissing: true}
	scoreTimeframe := candidate.MidTimeframe
	if scoreTimeframe == "" {
		scoreTimeframe = candidate.PrimaryTimeframe
	}
	breakdown.ScoreTimeframe = scoreTimeframe
	payload, _ := json.Marshal(breakdown)
	return 0, string(payload)
}

func groupedTrendGuardScoreFromContext(ctx *trendGuardCandidateScoreContext) (float64, string) {
	if ctx == nil {
		return 0, "{}"
	}
	candidate := ctx.Candidate
	scoreTimeframe := candidate.MidTimeframe
	if scoreTimeframe == "" {
		scoreTimeframe = candidate.PrimaryTimeframe
	}
	score, breakdown := models.ScoreTrendQuality(models.TrendQualityScoreRequest{
		Side:           candidate.Side,
		ScoreTimeframe: scoreTimeframe,
		Series:         ctx.MidSeries,
		StartTS:        candidate.TrendingTS,
		BoundaryTS:     candidate.LastMidPullbackTS,
	})
	payload, _ := json.Marshal(breakdown)
	return score, string(payload)
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func trendCandidateActive(candidate *models.RiskTrendGroupCandidate) bool {
	if candidate == nil {
		return false
	}
	return candidate.HasOpenPosition || (candidate.CandidateState != trendCandidateStateInactive && candidate.CandidateState != trendCandidateStateReversed)
}

func trendCandidateRetainedInRuntime(candidate *models.RiskTrendGroupCandidate) bool {
	return trendCandidateActive(candidate)
}

func trendGroupLeaderFrozen(group models.RiskTrendGroup) bool {
	return strings.TrimSpace(group.SelectedCandidateKey) != "" && (group.LockStage == trendGroupLockStageSoft || group.LockStage == trendGroupLockStageHard)
}
