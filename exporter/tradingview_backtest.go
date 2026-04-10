package exporter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"github.com/misterchenleiya/tradingbot/singleton"
	"github.com/misterchenleiya/tradingbot/storage"
)

const tradingViewBacktestHistoryBarsDefault = 500
const tradingViewBacktestPendingStaleAfter = 15 * time.Second

type tradingViewBacktestCreateRequest struct {
	Exchange           string  `json:"exchange"`
	Symbol             string  `json:"symbol"`
	DisplaySymbol      string  `json:"display_symbol"`
	ChartTimeframe     string  `json:"chart_timeframe"`
	RangeStartMS       int64   `json:"range_start_ms"`
	RangeEndMS         int64   `json:"range_end_ms"`
	PriceLow           float64 `json:"price_low"`
	PriceHigh          float64 `json:"price_high"`
	SelectionDirection string  `json:"selection_direction"`
}

type tradingViewBacktestTasksResponse struct {
	Date        string                `json:"date"`
	Count       int                   `json:"count"`
	HasMoreDays bool                  `json:"has_more_days"`
	Tasks       []models.BacktestTask `json:"tasks"`
}

type tradingViewBacktestTaskResponse struct {
	Task models.BacktestTask `json:"task"`
}

type tradingViewBacktestOverlayResponse struct {
	Task      models.BacktestTask         `json:"task"`
	Count     int                         `json:"count"`
	Total     int                         `json:"total,omitempty"`
	Truncated bool                        `json:"truncated,omitempty"`
	Events    []visualHistoryEventEntry   `json:"events"`
	Positions []visualHistoryPositionItem `json:"positions,omitempty"`
}

type tradingViewBacktestSourcePlan struct {
	Source      string
	HistoryBars int
}

type tradingViewBacktestOrderRow struct {
	ID                 int64
	Action             string
	PositionSide       string
	Size               float64
	LeverageMultiplier float64
	Price              float64
	ResultStatus       string
	RequestJSON        string
	StartedAtMS        int64
	FinishedAtMS       int64
	EventTS            int64
}

type tradingViewBacktestPositionRow struct {
	AvgPx        float64
	UplRatio     float64
	OpenTimeMS   int64
	UpdateTimeMS int64
	PosSide      string
	Lever        float64
}

func (s *Server) handleTradingViewBacktests(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleTradingViewBacktestList(w, r)
	case http.MethodPost:
		s.handleTradingViewBacktestCreate(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTradingViewBacktestSubRoutes(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, tradingViewAPIPrefix+"/backtests/") {
		http.NotFound(w, r)
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, tradingViewAPIPrefix+"/backtests/")
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		http.NotFound(w, r)
		return
	}
	taskID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || taskID <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid backtest task id"})
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleTradingViewBacktestGet(w, r, taskID)
		return
	}
	if len(parts) == 2 && parts[1] == "overlay" {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleTradingViewBacktestOverlay(w, r, taskID)
		return
	}
	if len(parts) == 2 && parts[1] == "retry" {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		s.handleTradingViewBacktestRetry(w, r, taskID)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleTradingViewBacktestCreate(w http.ResponseWriter, r *http.Request) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	var req tradingViewBacktestCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid request body: %v", err)})
		return
	}
	req.Exchange = strings.ToLower(strings.TrimSpace(req.Exchange))
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.DisplaySymbol = strings.TrimSpace(req.DisplaySymbol)
	req.ChartTimeframe = strings.TrimSpace(req.ChartTimeframe)
	req.SelectionDirection = strings.ToLower(strings.TrimSpace(req.SelectionDirection))
	if req.Exchange == "" || req.Symbol == "" || req.ChartTimeframe == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "exchange, symbol and chart_timeframe are required"})
		return
	}
	if req.RangeStartMS <= 0 || req.RangeEndMS <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "range_start_ms and range_end_ms are required"})
		return
	}
	if err := s.reconcileTradingViewBacktestTasks(); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	activeTasks, err := s.cfg.HistoryStore.CountBacktestTasksByStatus(models.BacktestTaskStatusPending, models.BacktestTaskStatusRunning)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if activeTasks > 0 {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "back-test already running"})
		return
	}

	tradeTimeframes, err := s.loadTradingViewTradeEnabledTimeframes()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	displayRangeStartMS, displayRangeEndMS, err := normalizeTradingViewSelectionDisplayRange(req.ChartTimeframe, req.RangeStartMS, req.RangeEndMS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	executionRangeStartMS, executionRangeEndMS, err := normalizeTradingViewBacktestExecutionRange(
		req.ChartTimeframe,
		displayRangeStartMS,
		displayRangeEndMS,
		tradeTimeframes,
	)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	if req.DisplaySymbol == "" {
		metaBySymbol, metaErr := s.loadTradingViewSymbolMeta(req.Exchange)
		if metaErr == nil {
			meta := metaBySymbol[req.Symbol]
			req.DisplaySymbol = tradingViewDisplaySymbol(req.Symbol, meta.Type)
		}
		if req.DisplaySymbol == "" {
			req.DisplaySymbol = req.Symbol
		}
	}
	plan, err := s.resolveTradingViewBacktestSourcePlan(
		req.Exchange,
		req.Symbol,
		tradeTimeframes,
		executionRangeStartMS,
		executionRangeEndMS,
		tradingViewBacktestHistoryBarsDefault,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	task, err := s.cfg.HistoryStore.CreateBacktestTask(storage.BacktestTaskCreateSpec{
		Exchange:           req.Exchange,
		Symbol:             req.Symbol,
		DisplaySymbol:      req.DisplaySymbol,
		ChartTimeframe:     req.ChartTimeframe,
		TradeTimeframes:    tradeTimeframes,
		RangeStartMS:       displayRangeStartMS,
		RangeEndMS:         displayRangeEndMS,
		PriceLow:           req.PriceLow,
		PriceHigh:          req.PriceHigh,
		SelectionDirection: req.SelectionDirection,
		Source:             plan.Source,
		HistoryBars:        plan.HistoryBars,
	})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if err := s.spawnTradingViewBacktestTask(task); err != nil {
		_ = s.cfg.HistoryStore.MarkBacktestTaskFinished(task.ID, models.BacktestTaskStatusFailed, err.Error())
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	created, found, err := s.cfg.HistoryStore.GetBacktestTask(task.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "backtest task disappeared after create"})
		return
	}
	writeJSON(w, http.StatusAccepted, tradingViewBacktestTaskResponse{Task: created})
}

func (s *Server) handleTradingViewBacktestList(w http.ResponseWriter, r *http.Request) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	if err := s.reconcileTradingViewBacktestTasks(); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	query := r.URL.Query()
	dateLoc, err := parseVisualHistoryQueryLocation(query.Get("tz_offset_min"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	dateLabel, startMS, endMS, err := parseHistoryDateWindow(query.Get("date"), dateLoc)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	tasks, err := s.cfg.HistoryStore.ListBacktestTasksByTimeRange(startMS, endMS)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	tasks, err = s.enrichTradingViewBacktestTasks(tasks)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	hasMoreDays, err := queryTradingViewBacktestTasksBefore(s.cfg.HistoryStore.DB, startMS)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tradingViewBacktestTasksResponse{
		Date:        dateLabel,
		Count:       len(tasks),
		HasMoreDays: hasMoreDays,
		Tasks:       tasks,
	})
}

func (s *Server) handleTradingViewBacktestGet(w http.ResponseWriter, r *http.Request, taskID int64) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	if err := s.reconcileTradingViewBacktestTasks(); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	task, found, err := s.cfg.HistoryStore.GetBacktestTask(taskID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "backtest task not found"})
		return
	}
	task, err = s.enrichTradingViewBacktestTask(task)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tradingViewBacktestTaskResponse{Task: task})
}

func (s *Server) handleTradingViewBacktestRetry(w http.ResponseWriter, r *http.Request, taskID int64) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	if err := s.reconcileTradingViewBacktestTasks(); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	task, found, err := s.cfg.HistoryStore.GetBacktestTask(taskID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "backtest task not found"})
		return
	}
	if task.Status == models.BacktestTaskStatusPending || task.Status == models.BacktestTaskStatusRunning {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "back-test already running"})
		return
	}
	activeTasks, err := s.cfg.HistoryStore.CountBacktestTasksByStatus(models.BacktestTaskStatusPending, models.BacktestTaskStatusRunning)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if activeTasks > 0 {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "back-test already running"})
		return
	}
	executionRangeStartMS, executionRangeEndMS, err := normalizeTradingViewBacktestExecutionRange(
		task.ChartTimeframe,
		task.RangeStartMS,
		task.RangeEndMS,
		task.TradeTimeframes,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	plan, err := s.resolveTradingViewBacktestSourcePlan(
		task.Exchange,
		task.Symbol,
		task.TradeTimeframes,
		executionRangeStartMS,
		executionRangeEndMS,
		task.HistoryBars,
	)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if err := s.cfg.HistoryStore.ResetBacktestTaskForRetry(task.ID, plan.Source, plan.HistoryBars); err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	retryTask, found, err := s.cfg.HistoryStore.GetBacktestTask(task.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "backtest task disappeared after retry reset"})
		return
	}
	if err := s.spawnTradingViewBacktestTask(retryTask); err != nil {
		_ = s.cfg.HistoryStore.MarkBacktestTaskFinished(retryTask.ID, models.BacktestTaskStatusFailed, err.Error())
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	updated, found, err := s.cfg.HistoryStore.GetBacktestTask(retryTask.ID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "backtest task disappeared after retry launch"})
		return
	}
	writeJSON(w, http.StatusAccepted, tradingViewBacktestTaskResponse{Task: updated})
}

func (s *Server) handleTradingViewBacktestOverlay(w http.ResponseWriter, r *http.Request, taskID int64) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}
	task, found, err := s.cfg.HistoryStore.GetBacktestTask(taskID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "backtest task not found"})
		return
	}
	positions, err := s.queryTradingViewBacktestHistoryPositions(task)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	task = enrichTradingViewBacktestTaskSummary(task, positions)
	events, err := s.buildTradingViewBacktestOverlayEvents(task, positions)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	items := make([]visualHistoryPositionItem, 0, len(positions))
	for _, row := range positions {
		items = append(items, visualHistoryPositionFromRow(row))
	}
	writeJSON(w, http.StatusOK, tradingViewBacktestOverlayResponse{
		Task:      task,
		Count:     len(events),
		Total:     len(events),
		Events:    events,
		Positions: items,
	})
}

func (s *Server) spawnTradingViewBacktestTask(task models.BacktestTask) error {
	if s.tradingViewBacktestRunner == nil {
		return fmt.Errorf("tradingview back-test runner unavailable")
	}
	return s.tradingViewBacktestRunner.Launch(task)
}

func (s *Server) reconcileTradingViewBacktestTasks() error {
	if s == nil || s.cfg.HistoryStore == nil {
		return fmt.Errorf("history store unavailable")
	}
	tasks, err := s.cfg.HistoryStore.ListBacktestTasksByStatuses(
		models.BacktestTaskStatusPending,
		models.BacktestTaskStatusRunning,
		models.BacktestTaskStatusFailed,
	)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	now := time.Now().UTC()
	nowMS := now.UnixMilli()
	nowSec := now.Unix()
	for _, task := range tasks {
		switch task.Status {
		case models.BacktestTaskStatusPending:
			if task.CreatedAtMS > 0 && nowMS-task.CreatedAtMS >= tradingViewBacktestPendingStaleAfter.Milliseconds() {
				if err := s.cfg.HistoryStore.MarkBacktestTaskFinished(task.ID, models.BacktestTaskStatusFailed, "back-test launch interrupted"); err != nil {
					return err
				}
			}
		case models.BacktestTaskStatusRunning:
			if task.SingletonID <= 0 && strings.TrimSpace(task.SingletonUUID) == "" {
				if err := s.cfg.HistoryStore.MarkBacktestTaskFinished(task.ID, models.BacktestTaskStatusFailed, "back-test interrupted"); err != nil {
					return err
				}
				continue
			}
			nextStatus, nextError, shouldUpdate, err := s.reconcileTradingViewBacktestTaskWithSingleton(task, nowSec)
			if err != nil {
				return err
			}
			if shouldUpdate {
				if err := s.cfg.HistoryStore.MarkBacktestTaskFinished(task.ID, nextStatus, nextError); err != nil {
					return err
				}
			}
		case models.BacktestTaskStatusFailed:
			if strings.TrimSpace(task.ErrorMessage) != "back-test interrupted" {
				continue
			}
			nextStatus, nextError, shouldUpdate, err := s.reconcileTradingViewBacktestTaskWithSingleton(task, nowSec)
			if err != nil {
				return err
			}
			if shouldUpdate {
				if err := s.cfg.HistoryStore.MarkBacktestTaskFinished(task.ID, nextStatus, nextError); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Server) reconcileTradingViewBacktestTaskWithSingleton(task models.BacktestTask, nowSec int64) (string, string, bool, error) {
	if s == nil || s.cfg.HistoryStore == nil {
		return "", "", false, fmt.Errorf("history store unavailable")
	}
	record, found, err := s.cfg.HistoryStore.GetSingleton(task.SingletonID, task.SingletonUUID)
	if err != nil {
		return "", "", false, err
	}
	if !found {
		return models.BacktestTaskStatusFailed, "back-test interrupted", true, nil
	}
	status := strings.ToLower(strings.TrimSpace(record.Status))
	switch status {
	case singleton.StatusCompleted:
		if task.Status == models.BacktestTaskStatusSucceeded && strings.TrimSpace(task.ErrorMessage) == "" {
			return "", "", false, nil
		}
		return models.BacktestTaskStatusSucceeded, "", true, nil
	case singleton.StatusAborted:
		if task.Status == models.BacktestTaskStatusFailed && strings.TrimSpace(task.ErrorMessage) == "back-test interrupted" {
			return "", "", false, nil
		}
		return models.BacktestTaskStatusFailed, "back-test interrupted", true, nil
	case singleton.StatusRunning:
		if isTradingViewBacktestSingletonActive(record, nowSec) {
			return "", "", false, nil
		}
		return models.BacktestTaskStatusFailed, "back-test interrupted", true, nil
	default:
		return models.BacktestTaskStatusFailed, "back-test interrupted", true, nil
	}
}

func isTradingViewBacktestSingletonActive(record models.SingletonRecord, nowSec int64) bool {
	if !strings.EqualFold(strings.TrimSpace(record.Mode), tradingViewBacktestMode) {
		return false
	}
	if record.Closed != nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(record.Status), singleton.StatusRunning) {
		return false
	}
	if record.LeaseExpires != nil && *record.LeaseExpires > 0 && *record.LeaseExpires <= nowSec {
		return false
	}
	return true
}

func (s *Server) resolveTradingViewBacktestSource(
	exchange string,
	symbol string,
	tradeTimeframes []string,
	startMS int64,
	endMS int64,
	historyBars int,
) (string, error) {
	plan, err := s.resolveTradingViewBacktestSourcePlan(exchange, symbol, tradeTimeframes, startMS, endMS, historyBars)
	if err != nil {
		return "", err
	}
	return plan.Source, nil
}

func (s *Server) resolveTradingViewBacktestSourcePlan(
	exchange string,
	symbol string,
	tradeTimeframes []string,
	startMS int64,
	endMS int64,
	historyBars int,
) (tradingViewBacktestSourcePlan, error) {
	if s == nil || s.cfg.HistoryStore == nil {
		return tradingViewBacktestSourcePlan{}, fmt.Errorf("history store unavailable")
	}
	plan, err := buildTradingViewBacktestSourcePlan(s.cfg.HistoryStore, exchange, symbol, tradeTimeframes, startMS, endMS, historyBars)
	if err != nil {
		return tradingViewBacktestSourcePlan{}, err
	}
	return plan, nil
}

func (s *Server) loadTradingViewTradeEnabledTimeframes() ([]string, error) {
	if s.cfg.HistoryStore == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	value, found, err := s.cfg.HistoryStore.GetConfigValue("strategy")
	if err != nil {
		return nil, fmt.Errorf("get config.strategy: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("missing config.strategy")
	}
	var cfg tradingViewStrategyConfig
	if err := json.Unmarshal([]byte(value), &cfg); err != nil {
		return nil, fmt.Errorf("invalid config.strategy: %w", err)
	}
	set := make(map[string]time.Duration)
	for i, combo := range cfg.Combo {
		if !combo.TradeEnabled {
			continue
		}
		_, timeframes, _ := common.NormalizeStrategyIdentity("", combo.Timeframes, "")
		for _, timeframe := range timeframes {
			duration, ok := market.TimeframeDuration(timeframe)
			if !ok {
				return nil, fmt.Errorf("invalid config.strategy.combo[%d] timeframe %q", i, timeframe)
			}
			set[timeframe] = duration
		}
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("config.strategy has no trade_enabled timeframes")
	}
	out := make([]string, 0, len(set))
	for timeframe := range set {
		out = append(out, timeframe)
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, _ := market.TimeframeDuration(out[i])
		right, _ := market.TimeframeDuration(out[j])
		if left != right {
			return left < right
		}
		return out[i] < out[j]
	})
	return out, nil
}

func normalizeTradingViewSelectionDisplayRange(chartTimeframe string, startMS, endMS int64) (int64, int64, error) {
	chartDur, ok := market.TimeframeDuration(strings.TrimSpace(chartTimeframe))
	if !ok || chartDur <= 0 {
		return 0, 0, fmt.Errorf("invalid chart timeframe %q", chartTimeframe)
	}
	if endMS < startMS {
		startMS, endMS = endMS, startMS
	}
	normalizedStart := floorToDuration(startMS, chartDur)
	normalizedEnd := floorToDuration(endMS, chartDur)
	if normalizedEnd < normalizedStart {
		normalizedEnd = normalizedStart
	}
	return normalizedStart, normalizedEnd, nil
}

func normalizeTradingViewBacktestExecutionRange(chartTimeframe string, displayStartMS, displayEndMS int64, tradeTimeframes []string) (int64, int64, error) {
	chartDur, ok := market.TimeframeDuration(strings.TrimSpace(chartTimeframe))
	if !ok || chartDur <= 0 {
		return 0, 0, fmt.Errorf("invalid chart timeframe %q", chartTimeframe)
	}
	if len(tradeTimeframes) == 0 {
		return 0, 0, fmt.Errorf("missing trade-enabled timeframes")
	}
	minTradeTF := strings.TrimSpace(tradeTimeframes[0])
	minTradeDur, ok := market.TimeframeDuration(minTradeTF)
	if !ok || minTradeDur <= 0 {
		return 0, 0, fmt.Errorf("invalid trade timeframe %q", minTradeTF)
	}
	if displayEndMS < displayStartMS {
		displayStartMS, displayEndMS = displayEndMS, displayStartMS
	}
	selectedStart := floorToDuration(displayStartMS, chartDur)
	selectedEndExclusive := floorToDuration(displayEndMS, chartDur) + chartDur.Milliseconds()
	normalizedStart := floorToDuration(selectedStart, minTradeDur)
	normalizedEndExclusive := ceilToDuration(selectedEndExclusive, minTradeDur)
	normalizedEnd := normalizedEndExclusive - minTradeDur.Milliseconds()
	if normalizedEnd < normalizedStart {
		normalizedEnd = normalizedStart
	}
	return normalizedStart, normalizedEnd, nil
}

func buildTradingViewBacktestSource(sourceType, exchange, symbol string, tradeTimeframes []string, startMS, endMS int64) string {
	return fmt.Sprintf(
		"%s:%s:%s:%s:%s-%s",
		strings.ToLower(strings.TrimSpace(sourceType)),
		strings.ToLower(strings.TrimSpace(exchange)),
		strings.TrimSpace(symbol),
		strings.Join(tradeTimeframes, "/"),
		formatTradingViewBacktestSourceTS(startMS),
		formatTradingViewBacktestSourceTS(endMS),
	)
}

func buildTradingViewBacktestSourcePlan(
	store *storage.SQLite,
	exchange string,
	symbol string,
	tradeTimeframes []string,
	startMS int64,
	endMS int64,
	historyBars int,
) (tradingViewBacktestSourcePlan, error) {
	normalizedHistoryBars := normalizeTradingViewBacktestHistoryBars(historyBars)
	exchangeSource := tradingViewBacktestSourcePlan{
		Source:      buildTradingViewBacktestSource("exchange", exchange, symbol, tradeTimeframes, startMS, endMS),
		HistoryBars: normalizedHistoryBars,
	}
	if store == nil || len(tradeTimeframes) == 0 {
		return exchangeSource, nil
	}
	maxLocalWarmup := normalizedHistoryBars
	for _, timeframe := range tradeTimeframes {
		dur, ok := market.TimeframeDuration(strings.TrimSpace(timeframe))
		if !ok || dur <= 0 {
			return tradingViewBacktestSourcePlan{}, fmt.Errorf("invalid backtest timeframe %q", timeframe)
		}
		step := dur.Milliseconds()
		queryStartMS := startMS
		if normalizedHistoryBars > 0 {
			queryStartMS -= int64(normalizedHistoryBars) * step
		}
		startTime := time.UnixMilli(queryStartMS - step).UTC()
		endTime := time.UnixMilli(endMS).UTC()
		data, err := store.ListOHLCVRange(exchange, symbol, timeframe, startTime, endTime)
		if err != nil {
			return tradingViewBacktestSourcePlan{}, err
		}
		replayCovered, availableWarmupBars := measureTradingViewBacktestDBCoverage(data, startMS, endMS, step, normalizedHistoryBars)
		if !replayCovered {
			return exchangeSource, nil
		}
		if availableWarmupBars < maxLocalWarmup {
			maxLocalWarmup = availableWarmupBars
		}
	}
	return tradingViewBacktestSourcePlan{
		Source:      buildTradingViewBacktestSource("db", exchange, symbol, tradeTimeframes, startMS, endMS),
		HistoryBars: maxLocalWarmup,
	}, nil
}

func measureTradingViewBacktestDBCoverage(data []models.OHLCV, startMS, endMS, step int64, requestedWarmupBars int) (bool, int) {
	if len(data) == 0 || step <= 0 || endMS < startMS {
		return false, 0
	}
	present := make(map[int64]struct{}, len(data))
	for _, item := range data {
		ts := normalizeTradingViewBacktestTS(item.TS)
		if ts <= 0 {
			continue
		}
		if ts > endMS {
			continue
		}
		present[ts] = struct{}{}
	}
	firstTS := floorTradingViewBacktestTS(startMS, step)
	lastTS := floorTradingViewBacktestTS(endMS, step)
	for ts := firstTS; ts <= lastTS; ts += step {
		if _, ok := present[ts]; !ok {
			return false, 0
		}
	}
	availableWarmupBars := 0
	if requestedWarmupBars <= 0 {
		return true, 0
	}
	for ts := firstTS - step; availableWarmupBars < requestedWarmupBars; ts -= step {
		if _, ok := present[ts]; !ok {
			break
		}
		availableWarmupBars++
	}
	return true, availableWarmupBars
}

func normalizeTradingViewBacktestHistoryBars(historyBars int) int {
	if historyBars <= 0 {
		return 0
	}
	return historyBars
}

func normalizeTradingViewBacktestTS(ts int64) int64 {
	switch {
	case ts >= 1_000_000_000_000:
		return ts
	case ts > 0:
		return ts * 1000
	default:
		return ts
	}
}

func floorTradingViewBacktestTS(ts, step int64) int64 {
	if step <= 0 {
		return ts
	}
	if ts >= 0 {
		return (ts / step) * step
	}
	return ((ts - step + 1) / step) * step
}

func formatTradingViewBacktestSourceTS(ms int64) string {
	return time.UnixMilli(ms).UTC().Format("20060102_1504Z")
}

func floorToDuration(ts int64, step time.Duration) int64 {
	if step <= 0 {
		return ts
	}
	size := step.Milliseconds()
	if size <= 0 {
		return ts
	}
	return ts - (ts % size)
}

func ceilToDuration(ts int64, step time.Duration) int64 {
	if step <= 0 {
		return ts
	}
	size := step.Milliseconds()
	if size <= 0 {
		return ts
	}
	floor := floorToDuration(ts, step)
	if floor == ts {
		return ts
	}
	return floor + size
}

func queryTradingViewBacktestTasksBefore(db *sql.DB, startMS int64) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("nil db")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM backtest_tasks WHERE created_at_ms < ?;`, startMS).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *Server) enrichTradingViewBacktestTasks(tasks []models.BacktestTask) ([]models.BacktestTask, error) {
	if len(tasks) == 0 {
		return tasks, nil
	}
	out := make([]models.BacktestTask, 0, len(tasks))
	for _, task := range tasks {
		enriched, err := s.enrichTradingViewBacktestTask(task)
		if err != nil {
			return nil, err
		}
		out = append(out, enriched)
	}
	return out, nil
}

func (s *Server) enrichTradingViewBacktestTask(task models.BacktestTask) (models.BacktestTask, error) {
	if task.SingletonID <= 0 {
		return task, nil
	}
	positions, err := s.queryTradingViewBacktestHistoryPositions(task)
	if err != nil {
		return models.BacktestTask{}, err
	}
	task = enrichTradingViewBacktestTaskSummary(task, positions)
	if len(positions) > 0 {
		return task, nil
	}
	return s.enrichTradingViewBacktestTaskSummaryFromRuntime(task)
}

func enrichTradingViewBacktestTaskSummary(
	task models.BacktestTask,
	positions []visualHistoryPositionRow,
) models.BacktestTask {
	if len(positions) == 0 {
		return task
	}
	first := positions[0]
	last := positions[len(positions)-1]
	task.PositionSide = strings.TrimSpace(first.PosSide)
	task.LeverageMultiplier = parseNumericText(first.Lever)
	task.OpenPrice = parseNumericText(first.AvgPx)
	task.ClosePrice = parseNumericText(last.CloseAvgPx)
	task.OpenTimeMS = first.OpenTimeMS
	task.CloseTimeMS = last.CloseTimeMS
	if task.CloseTimeMS <= 0 {
		task.CloseTimeMS = last.UpdatedAtMS
	}
	if task.OpenTimeMS > 0 && task.CloseTimeMS > task.OpenTimeMS {
		task.HoldingDurationMS = task.CloseTimeMS - task.OpenTimeMS
	}
	var realizedProfitRate float64
	for _, row := range positions {
		realizedProfitRate += parseNumericText(row.PnlRatio)
	}
	task.RealizedProfitRate = realizedProfitRate
	return task
}

func (s *Server) enrichTradingViewBacktestTaskSummaryFromRuntime(task models.BacktestTask) (models.BacktestTask, error) {
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return task, fmt.Errorf("history store unavailable")
	}
	if strings.TrimSpace(task.SingletonUUID) == "" {
		return task, nil
	}

	orders, err := s.queryTradingViewBacktestExecutionOrders(task)
	if err != nil {
		return models.BacktestTask{}, err
	}
	position, err := s.queryTradingViewBacktestOpenPosition(task)
	if err != nil {
		return models.BacktestTask{}, err
	}
	return enrichTradingViewBacktestTaskSummaryFromRuntime(task, orders, position), nil
}

func enrichTradingViewBacktestTaskSummaryFromRuntime(
	task models.BacktestTask,
	orders []tradingViewBacktestOrderRow,
	position *tradingViewBacktestPositionRow,
) models.BacktestTask {
	if len(orders) == 0 && position == nil {
		return task
	}

	sort.SliceStable(orders, func(i, j int) bool {
		leftTS := orders[i].EventTS
		if leftTS <= 0 {
			leftTS = orders[i].StartedAtMS
		}
		rightTS := orders[j].EventTS
		if rightTS <= 0 {
			rightTS = orders[j].StartedAtMS
		}
		if leftTS != rightTS {
			return leftTS < rightTS
		}
		return orders[i].ID < orders[j].ID
	})

	var openOrder *tradingViewBacktestOrderRow
	var closeOrder *tradingViewBacktestOrderRow
	var lastEventTS int64
	positionSide := ""

	for i := range orders {
		item := &orders[i]
		if !isTradingViewBacktestOrderSuccessful(item.ResultStatus) {
			continue
		}
		eventTS := item.EventTS
		if eventTS <= 0 {
			eventTS = item.StartedAtMS
		}
		if eventTS > lastEventTS {
			lastEventTS = eventTS
		}
		switch strings.TrimSpace(item.Action) {
		case models.DecisionActionOpenLong, models.DecisionActionOpenShort:
			if openOrder == nil {
				openOrder = item
			}
			if positionSide == "" {
				positionSide = strings.TrimSpace(item.PositionSide)
			}
		case models.DecisionActionClose:
			closeOrder = item
			if positionSide == "" {
				positionSide = strings.TrimSpace(item.PositionSide)
			}
		}
	}

	if position != nil {
		if position.Lever > 0 {
			task.LeverageMultiplier = position.Lever
		}
		if position.AvgPx > 0 {
			task.OpenPrice = position.AvgPx
		}
		if position.OpenTimeMS > 0 {
			task.OpenTimeMS = position.OpenTimeMS
		}
		if positionSide == "" {
			positionSide = strings.TrimSpace(position.PosSide)
		}
	}

	if task.LeverageMultiplier <= 0 && openOrder != nil && openOrder.LeverageMultiplier > 0 {
		task.LeverageMultiplier = openOrder.LeverageMultiplier
	}

	if task.OpenPrice <= 0 && openOrder != nil && openOrder.Price > 0 {
		task.OpenPrice = openOrder.Price
	}
	if task.OpenTimeMS <= 0 && openOrder != nil {
		task.OpenTimeMS = openOrder.EventTS
		if task.OpenTimeMS <= 0 {
			task.OpenTimeMS = openOrder.StartedAtMS
		}
	}

	if closeOrder != nil {
		if closeOrder.Price > 0 {
			task.ClosePrice = closeOrder.Price
		}
		task.CloseTimeMS = closeOrder.EventTS
		if task.CloseTimeMS <= 0 {
			task.CloseTimeMS = closeOrder.StartedAtMS
		}
	}

	if task.OpenTimeMS > 0 {
		switch {
		case task.CloseTimeMS > task.OpenTimeMS:
			task.HoldingDurationMS = task.CloseTimeMS - task.OpenTimeMS
		case position != nil && position.UpdateTimeMS > task.OpenTimeMS:
			task.HoldingDurationMS = position.UpdateTimeMS - task.OpenTimeMS
		case lastEventTS > task.OpenTimeMS:
			task.HoldingDurationMS = lastEventTS - task.OpenTimeMS
		}
	}

	if task.OpenPrice > 0 && task.ClosePrice > 0 {
		if rate, ok := computeTradingViewBacktestProfitRate(positionSide, task.OpenPrice, task.ClosePrice); ok {
			task.RealizedProfitRate = rate
		}
	}
	if task.PositionSide == "" {
		task.PositionSide = strings.TrimSpace(positionSide)
	}

	return task
}

func computeTradingViewBacktestProfitRate(positionSide string, openPrice, closePrice float64) (float64, bool) {
	if openPrice <= 0 || closePrice <= 0 {
		return 0, false
	}
	switch strings.ToLower(strings.TrimSpace(positionSide)) {
	case "short":
		return (openPrice - closePrice) / openPrice, true
	case "long", "":
		return (closePrice - openPrice) / openPrice, true
	default:
		return 0, false
	}
}

func isTradingViewBacktestOrderSuccessful(resultStatus string) bool {
	switch strings.ToLower(strings.TrimSpace(resultStatus)) {
	case "", "success", "partial_failed":
		return true
	default:
		return false
	}
}

func parseTradingViewBacktestOrderEventTS(requestJSON string) int64 {
	payload := strings.TrimSpace(requestJSON)
	if payload == "" {
		return 0
	}
	var decoded struct {
		EventTS int64 `json:"EventTS"`
	}
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return 0
	}
	return decoded.EventTS
}

func (s *Server) queryTradingViewBacktestExecutionOrders(task models.BacktestTask) ([]tradingViewBacktestOrderRow, error) {
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	if strings.TrimSpace(task.SingletonUUID) == "" {
		return nil, nil
	}
	rows, err := s.cfg.HistoryStore.DB.Query(
		`SELECT id, action, position_side, size, leverage_multiplier, price, result_status, request_json, started_at_ms, finished_at_ms
		   FROM orders
		  WHERE mode = 'back-test'
		    AND singleton_uuid = ?
		    AND exchange = ?
		    AND symbol = ?
		  ORDER BY started_at_ms ASC, id ASC;`,
		strings.TrimSpace(task.SingletonUUID),
		task.Exchange,
		task.Symbol,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]tradingViewBacktestOrderRow, 0, 32)
	for rows.Next() {
		var item tradingViewBacktestOrderRow
		if err := rows.Scan(
			&item.ID,
			&item.Action,
			&item.PositionSide,
			&item.Size,
			&item.LeverageMultiplier,
			&item.Price,
			&item.ResultStatus,
			&item.RequestJSON,
			&item.StartedAtMS,
			&item.FinishedAtMS,
		); err != nil {
			return nil, err
		}
		item.EventTS = parseTradingViewBacktestOrderEventTS(item.RequestJSON)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) queryTradingViewBacktestOpenPosition(task models.BacktestTask) (*tradingViewBacktestPositionRow, error) {
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	if task.SingletonID <= 0 {
		return nil, nil
	}
	row := s.cfg.HistoryStore.DB.QueryRow(
		`SELECT avg_px, upl_ratio, open_time_ms, update_time_ms, pos_side, lever
		   FROM positions
		  WHERE mode = 'back-test'
		    AND singleton_id = ?
		    AND exchange = ?
		    AND symbol = ?
		  ORDER BY update_time_ms DESC
		  LIMIT 1;`,
		task.SingletonID,
		task.Exchange,
		task.Symbol,
	)
	var item tradingViewBacktestPositionRow
	if err := row.Scan(
		&item.AvgPx,
		&item.UplRatio,
		&item.OpenTimeMS,
		&item.UpdateTimeMS,
		&item.PosSide,
		&item.Lever,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &item, nil
}

func (s *Server) queryTradingViewBacktestHistoryPositions(task models.BacktestTask) ([]visualHistoryPositionRow, error) {
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	if task.SingletonID <= 0 {
		return nil, nil
	}
	rows, err := s.cfg.HistoryStore.DB.Query(
		`SELECT id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
		        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
		        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
		   FROM history_positions
		  WHERE mode = 'back-test'
		    AND singleton_id = ?
		    AND exchange = ?
		    AND symbol = ?
		    AND open_time_ms <= ?
		    AND (close_time_ms = 0 OR close_time_ms >= ?)
		  ORDER BY open_time_ms ASC, id ASC;`,
		task.SingletonID,
		task.Exchange,
		task.Symbol,
		task.RangeEndMS,
		task.RangeStartMS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]visualHistoryPositionRow, 0, 16)
	for rows.Next() {
		var item visualHistoryPositionRow
		if err := rows.Scan(
			&item.ID,
			&item.Exchange,
			&item.Symbol,
			&item.InstID,
			&item.Pos,
			&item.PosSide,
			&item.MgnMode,
			&item.Margin,
			&item.Lever,
			&item.AvgPx,
			&item.NotionalUSD,
			&item.MarkPx,
			&item.LiqPx,
			&item.TPTriggerPx,
			&item.SLTriggerPx,
			&item.OpenTimeMS,
			&item.OpenUpdateTimeMS,
			&item.MaxFloatingLossAmount,
			&item.MaxFloatingProfitAmount,
			&item.OpenRowJSON,
			&item.CloseAvgPx,
			&item.RealizedPnl,
			&item.PnlRatio,
			&item.Fee,
			&item.FundingFee,
			&item.CloseTimeMS,
			&item.State,
			&item.CloseRowJSON,
			&item.CreatedAtMS,
			&item.UpdatedAtMS,
		); err != nil {
			return nil, err
		}
		item.PositionKey = buildVisualHistoryPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS)
		item.Symbol = normalizeVisualHistorySymbol(item.Symbol, item.InstID)
		item.IsOpen = false
		item.RevisionMS = maxVisualHistoryInt64(item.UpdatedAtMS, item.CloseTimeMS)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Server) buildTradingViewBacktestOverlayEvents(task models.BacktestTask, positions []visualHistoryPositionRow) ([]visualHistoryEventEntry, error) {
	events := make([]visualHistoryEventEntry, 0, len(positions)*4)
	for _, row := range positions {
		events = append(events, buildSyntheticHistoryEvents(row)...)
	}
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil || task.SingletonID <= 0 {
		return sortVisualHistoryEvents(dedupeVisualHistoryEvents(events)), nil
	}
	scope := visualHistoryRunScope{
		RunID:         strings.TrimSpace(task.SingletonUUID),
		SingletonUUID: strings.TrimSpace(task.SingletonUUID),
		SingletonID:   task.SingletonID,
	}
	row := visualHistoryPositionRow{
		Exchange: strings.TrimSpace(task.Exchange),
		Symbol:   strings.TrimSpace(task.Symbol),
	}
	signalEvents, err := querySignalEventsFromStore(
		s.cfg.HistoryStore.DB,
		row,
		models.StrategyContextMeta{},
		task.RangeStartMS,
		task.RangeEndMS,
		scope,
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		return nil, err
	}
	events = append(events, signalEvents...)
	executionEvents, err := queryExecutionEventsFromStore(
		s.cfg.HistoryStore.DB,
		row,
		task.RangeStartMS,
		task.RangeEndMS,
		scope,
	)
	if err != nil {
		return nil, err
	}
	events = append(events, executionEvents...)
	return sortVisualHistoryEvents(dedupeVisualHistoryEvents(events)), nil
}

func dedupeVisualHistoryEvents(items []visualHistoryEventEntry) []visualHistoryEventEntry {
	if len(items) == 0 {
		return nil
	}
	out := make([]visualHistoryEventEntry, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		key := strings.TrimSpace(item.ID)
		if key == "" {
			key = fmt.Sprintf("%s|%d|%s", strings.TrimSpace(item.Source), item.EventAt, strings.TrimSpace(item.Type))
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, item)
	}
	return out
}
