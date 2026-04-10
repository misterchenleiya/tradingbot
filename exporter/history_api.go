package exporter

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/misterchenleiya/tradingbot/common"
	"github.com/misterchenleiya/tradingbot/exchange/market"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

const (
	visualHistoryAPIPrefix         = "/visual-history/api/v1/positions"
	visualHistoryDefaultPageLimit  = 50
	visualHistoryMaxPageLimit      = 200
	visualHistoryDefaultFetchMax   = 300
	visualHistoryDefaultEventLimit = 1200
	visualHistoryMaxEventLimit     = 5000
	historyStateSyncPending        = "sync_pending"
)

var indicatorPeriodRegexp = regexp.MustCompile(`(?i)(\d{1,5})`)

type visualHistoryPositionsResponse struct {
	Date          string                           `json:"date"`
	Count         int                              `json:"count"`
	HasMore       bool                             `json:"has_more"`
	NextBeforeMS  int64                            `json:"next_before_ms,omitempty"`
	FilterOptions visualHistoryPositionFilterGroup `json:"filter_options"`
	Positions     []visualHistoryPositionItem      `json:"positions"`
}

type visualHistoryRunOption struct {
	Value       string `json:"value"`
	Label       string `json:"label"`
	SingletonID int64  `json:"singleton_id,omitempty"`
}

type visualHistoryPositionFilterGroup struct {
	RunIDs     []string                 `json:"run_ids"`
	RunOptions []visualHistoryRunOption `json:"run_options,omitempty"`
	Strategies []string                 `json:"strategies"`
	Versions   []string                 `json:"versions"`
	Exchanges  []string                 `json:"exchanges"`
	Symbols    []string                 `json:"symbols"`
}

type visualHistoryPositionFilters struct {
	RunID    string
	Strategy string
	Version  string
}

type visualHistoryPositionItem struct {
	PositionUID             string              `json:"position_uid"`
	PositionKey             string              `json:"position_key"`
	ID                      int64               `json:"id"`
	IsOpen                  bool                `json:"is_open"`
	DisplayState            string              `json:"display_state"`
	Exchange                string              `json:"exchange"`
	Symbol                  string              `json:"symbol"`
	InstID                  string              `json:"inst_id"`
	PositionSide            string              `json:"position_side"`
	MarginMode              string              `json:"margin_mode"`
	Leverage                float64             `json:"leverage"`
	Margin                  float64             `json:"margin"`
	EntryPrice              float64             `json:"entry_price"`
	ExitPrice               float64             `json:"exit_price"`
	TakeProfitPrice         float64             `json:"take_profit_price"`
	StopLossPrice           float64             `json:"stop_loss_price"`
	Quantity                float64             `json:"quantity"`
	NotionalUSD             float64             `json:"notional_usd"`
	RealizedPnL             float64             `json:"realized_pnl"`
	PnLRatio                float64             `json:"pnl_ratio"`
	Fee                     float64             `json:"fee"`
	FundingFee              float64             `json:"funding_fee"`
	OpenTimeMS              int64               `json:"open_time_ms"`
	CloseTimeMS             int64               `json:"close_time_ms"`
	OpenUpdateTimeMS        int64               `json:"open_update_time_ms"`
	UpdatedAtMS             int64               `json:"updated_at_ms"`
	MaxFloatingLossAmount   float64             `json:"max_floating_loss_amount"`
	MaxFloatingProfitAmount float64             `json:"max_floating_profit_amount"`
	State                   string              `json:"state"`
	SingletonID             int64               `json:"singleton_id,omitempty"`
	RunID                   string              `json:"run_id,omitempty"`
	StrategyName            string              `json:"strategy_name"`
	StrategyVersion         string              `json:"strategy_version"`
	Timeframes              []string            `json:"timeframes"`
	Indicators              map[string][]string `json:"indicators"`
	RevisionMS              int64               `json:"revision_ms"`
	PollIntervalMS          int64               `json:"poll_interval_ms,omitempty"`
}

type visualHistoryPositionEventsResponse struct {
	PositionID int64                     `json:"position_id"`
	Count      int                       `json:"count"`
	Total      int                       `json:"total,omitempty"`
	Truncated  bool                      `json:"truncated,omitempty"`
	Events     []visualHistoryEventEntry `json:"events"`
}

type visualHistoryEventEntry struct {
	ID      string         `json:"id"`
	Source  string         `json:"source"`
	Type    string         `json:"type"`
	Level   string         `json:"level"`
	EventAt int64          `json:"event_at_ms"`
	Title   string         `json:"title"`
	Summary string         `json:"summary"`
	Detail  map[string]any `json:"detail,omitempty"`
}

type visualHistoryLoadCandlesRequest struct {
	Timeframes    []string `json:"timeframes"`
	MaxPerRequest int      `json:"max_per_request"`
	Force         bool     `json:"force"`
}

type visualHistoryLoadCandlesResponse struct {
	PositionID int64                          `json:"position_id"`
	Loaded     []visualHistoryLoadedTimeframe `json:"loaded"`
}

type visualHistoryLoadedTimeframe struct {
	Timeframe         string `json:"timeframe"`
	Bars              int    `json:"bars"`
	LookbackBars      int    `json:"lookback_bars"`
	FetchStartMS      int64  `json:"fetch_start_ms"`
	FetchEndMS        int64  `json:"fetch_end_ms"`
	RecoveredBars     int    `json:"recovered_bars,omitempty"`
	RecoveredFromMain bool   `json:"recovered_from_main,omitempty"`
}

type visualHistoryRecoveredEvents struct {
	Signals int
	Orders  int
}

func (r visualHistoryRecoveredEvents) Total() int {
	return r.Signals + r.Orders
}

type visualHistoryCandlesResponse struct {
	PositionID  int64                                  `json:"position_id"`
	PositionKey string                                 `json:"position_key,omitempty"`
	IsOpen      bool                                   `json:"is_open,omitempty"`
	OpenTimeMS  int64                                  `json:"open_time_ms"`
	CloseTimeMS int64                                  `json:"close_time_ms"`
	Timeframes  map[string]visualHistoryTimeframeBlock `json:"timeframes"`
	Integrity   visualHistoryIntegrityResponse         `json:"integrity"`
}

type visualHistoryTimeframeBlock struct {
	Count           int                   `json:"count"`
	LookbackBars    int                   `json:"lookback_bars"`
	ExpectedStartMS int64                 `json:"expected_start_ms"`
	ExpectedEndMS   int64                 `json:"expected_end_ms"`
	Candles         []visualHistoryCandle `json:"candles"`
}

type visualHistoryCandle struct {
	TS     int64   `json:"ts"`
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Close  float64 `json:"close"`
	Volume float64 `json:"volume"`
}

type visualHistoryIntegrityResponse struct {
	PositionID  int64  `json:"position_id"`
	PositionKey string `json:"position_key,omitempty"`
	IsOpen      bool   `json:"is_open,omitempty"`
	Events      struct {
		MainDBAvailable bool `json:"main_db_available"`
		Signals         int  `json:"signals"`
		Orders          int  `json:"orders"`
	} `json:"events"`
	Candles struct {
		TotalRows  int64                        `json:"total_rows"`
		Timeframes []visualHistoryCandleCacheTF `json:"timeframes"`
	} `json:"candles"`
	Summary struct {
		IncompleteTimeframes int   `json:"incomplete_timeframes"`
		MissingBars          int64 `json:"missing_bars"`
		Discontinuities      int   `json:"discontinuities"`
	} `json:"summary"`
	Check struct {
		HasEvents        bool `json:"has_events"`
		HasCandles       bool `json:"has_candles"`
		HasDiscontinuity bool `json:"has_discontinuity"`
		OK               bool `json:"ok"`
	} `json:"check"`
	Timeframes []visualHistoryIntegrityTimeframe `json:"timeframes"`
}

type visualHistoryCandleCacheTF struct {
	Timeframe string `json:"timeframe"`
	Rows      int64  `json:"rows"`
	FirstTS   int64  `json:"first_ts,omitempty"`
	LastTS    int64  `json:"last_ts,omitempty"`
}

type visualHistoryIntegrityTimeframe struct {
	Timeframe       string                      `json:"timeframe"`
	ExpectedStartMS int64                       `json:"expected_start_ms"`
	ExpectedEndMS   int64                       `json:"expected_end_ms"`
	ExpectedBars    int64                       `json:"expected_bars"`
	ActualBars      int64                       `json:"actual_bars"`
	Complete        bool                        `json:"complete"`
	Continuous      bool                        `json:"continuous"`
	Gaps            []visualHistoryIntegrityGap `json:"gaps,omitempty"`
}

type visualHistoryIntegrityGap struct {
	Kind    string `json:"kind"`
	StartTS int64  `json:"start_ts"`
	EndTS   int64  `json:"end_ts"`
	Bars    int64  `json:"bars"`
}

type visualHistoryPositionRow struct {
	PositionKey             string
	ID                      int64
	IsOpen                  bool
	Exchange                string
	Symbol                  string
	InstID                  string
	Pos                     string
	PosSide                 string
	MgnMode                 string
	Margin                  string
	Lever                   string
	AvgPx                   string
	NotionalUSD             string
	MarkPx                  string
	LiqPx                   string
	TPTriggerPx             string
	SLTriggerPx             string
	OpenTimeMS              int64
	OpenUpdateTimeMS        int64
	MaxFloatingLossAmount   float64
	MaxFloatingProfitAmount float64
	OpenRowJSON             string
	CloseAvgPx              string
	RealizedPnl             string
	PnlRatio                string
	Fee                     string
	FundingFee              string
	CloseTimeMS             int64
	State                   string
	CloseRowJSON            string
	RevisionMS              int64
	LastCandleTSMS          int64
	LastEventAtMS           int64
	CreatedAtMS             int64
	UpdatedAtMS             int64
}

type visualHistoryPositionCandidate struct {
	Row         visualHistoryPositionRow
	PositionKey string
	IsOpen      bool
	RunID       string
	SingletonID int64
	Strategy    string
	Version     string
	SortTime    int64
}

type visualHistoryRunScope struct {
	RunID         string
	SingletonUUID string
	SingletonID   int64
}

type visualHistoryFrameSpec struct {
	Timeframe       string
	LookbackBars    int
	ExpectedStartMS int64
	ExpectedEndMS   int64
	ExpectedBars    int64
}

func (s *Server) handleVisualHistoryPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{Error: "history store unavailable"})
		return
	}

	query := r.URL.Query()
	dateRaw := strings.TrimSpace(query.Get("date"))
	exchange := strings.TrimSpace(query.Get("exchange"))
	symbol := strings.TrimSpace(query.Get("symbol"))
	filters := visualHistoryPositionFilters{
		RunID:    strings.TrimSpace(query.Get("run_id")),
		Strategy: strings.TrimSpace(query.Get("strategy")),
		Version:  strings.TrimSpace(query.Get("version")),
	}
	dateLoc, err := parseVisualHistoryQueryLocation(query.Get("tz_offset_min"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}
	beforeMS, err := parseQueryInt64(query.Get("before"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid before"})
		return
	}
	limit := parsePageLimit(query.Get("limit"))

	dateLabel, startMS, endMS, err := parseHistoryDateWindow(dateRaw, dateLoc)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: err.Error()})
		return
	}

	rows, filterOptions, hasMore, nextBefore, err := s.queryVisualHistoryPositions(startMS, endMS, exchange, symbol, filters, beforeMS, limit)
	if err != nil {
		s.logger.Warn("query visual history positions failed",
			zap.String("date", dateLabel),
			zap.String("exchange", exchange),
			zap.String("symbol", symbol),
			zap.String("run_id", filters.RunID),
			zap.String("strategy", filters.Strategy),
			zap.String("version", filters.Version),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}

	items := make([]visualHistoryPositionItem, 0, len(rows))
	for _, row := range rows {
		items = append(items, visualHistoryPositionFromRow(row))
	}
	writeJSON(w, http.StatusOK, visualHistoryPositionsResponse{
		Date:          dateLabel,
		Count:         len(items),
		HasMore:       hasMore,
		NextBeforeMS:  nextBefore,
		FilterOptions: filterOptions,
		Positions:     items,
	})
}

func (s *Server) handleVisualHistoryPositionSubRoutes(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, visualHistoryAPIPrefix+"/") {
		http.NotFound(w, r)
		return
	}
	tail := strings.TrimPrefix(r.URL.Path, visualHistoryAPIPrefix+"/")
	if strings.TrimSpace(tail) == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(tail, "/"), "/")
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "invalid position id"})
		return
	}
	if len(parts) == 1 {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		row, err := s.loadVisualHistoryPositionByID(id)
		if err != nil {
			writeJSON(w, mapHistoryPositionErrorToStatus(err), errorResponse{Error: err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, visualHistoryPositionFromRow(row))
		return
	}

	switch parts[1] {
	case "events":
		s.handleVisualHistoryPositionEvents(w, r, id)
		return
	case "candles":
		if len(parts) == 2 {
			s.handleVisualHistoryPositionCandles(w, r, id)
			return
		}
		if len(parts) == 3 && parts[2] == "load" {
			s.handleVisualHistoryPositionLoadCandles(w, r, id)
			return
		}
	case "integrity":
		s.handleVisualHistoryPositionIntegrity(w, r, id)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleVisualHistoryPositionEvents(w http.ResponseWriter, r *http.Request, positionID int64) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	query := r.URL.Query()
	eventLimit := parseVisualHistoryEventLimit(query.Get("event_limit"), visualHistoryDefaultEventLimit)

	row, err := s.loadVisualHistoryPositionByID(positionID)
	if err != nil {
		writeJSON(w, mapHistoryPositionErrorToStatus(err), errorResponse{Error: err.Error()})
		return
	}
	events, err := s.buildVisualHistoryEvents(row)
	if err != nil {
		s.logger.Warn("query visual history events failed",
			zap.Int64("position_id", positionID),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	total := len(events)
	events, truncated := trimVisualHistoryEvents(events, eventLimit)
	writeJSON(w, http.StatusOK, visualHistoryPositionEventsResponse{
		PositionID: positionID,
		Count:      len(events),
		Total:      total,
		Truncated:  truncated,
		Events:     events,
	})
}

func (s *Server) handleVisualHistoryPositionLoadCandles(w http.ResponseWriter, r *http.Request, positionID int64) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	row, err := s.loadVisualHistoryPositionByID(positionID)
	if err != nil {
		writeJSON(w, mapHistoryPositionErrorToStatus(err), errorResponse{Error: err.Error()})
		return
	}

	var req visualHistoryLoadCandlesRequest
	if r.Body != nil {
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid request body: %v", err)})
			return
		}
	}

	timeframes := normalizeRequestedTimeframes(req.Timeframes, row)
	if len(timeframes) == 0 {
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: "no valid timeframe"})
		return
	}
	specs, err := buildVisualHistoryFrameSpecs(row, "")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	specByTF := make(map[string]visualHistoryFrameSpec, len(specs))
	for _, spec := range specs {
		specByTF[spec.Timeframe] = spec
	}
	blocks, err := s.loadVisualHistoryCandleCache(positionID, "", row)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	integrity, err := s.buildVisualHistoryIntegrity(row, blocks)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	loaded := make([]visualHistoryLoadedTimeframe, 0, len(timeframes))
	for _, timeframe := range timeframes {
		spec, ok := specByTF[timeframe]
		if !ok {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: fmt.Sprintf("invalid timeframe %s", timeframe)})
			return
		}
		frameIntegrity, ok := findVisualHistoryIntegrityTimeframe(integrity.Timeframes, timeframe)
		if !ok || !frameIntegrity.Complete || !frameIntegrity.Continuous {
			writeJSON(w, http.StatusConflict, errorResponse{Error: fmt.Sprintf("history_candles_incomplete: %s", timeframe)})
			return
		}
		count := len(blocks[timeframe].Candles)
		loaded = append(loaded, visualHistoryLoadedTimeframe{
			Timeframe:    timeframe,
			Bars:         count,
			LookbackBars: spec.LookbackBars,
			FetchStartMS: spec.ExpectedStartMS,
			FetchEndMS:   spec.ExpectedEndMS,
		})
	}
	_ = req.MaxPerRequest
	_ = req.Force
	writeJSON(w, http.StatusOK, visualHistoryLoadCandlesResponse{PositionID: positionID, Loaded: loaded})
}

func (s *Server) handleVisualHistoryPositionCandles(w http.ResponseWriter, r *http.Request, positionID int64) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	row, err := s.loadVisualHistoryPositionByID(positionID)
	if err != nil {
		writeJSON(w, mapHistoryPositionErrorToStatus(err), errorResponse{Error: err.Error()})
		return
	}
	timeframeFilter := strings.TrimSpace(r.URL.Query().Get("timeframe"))
	blocks, err := s.loadVisualHistoryCandleCache(positionID, timeframeFilter, row)
	if err != nil {
		s.logger.Warn("load visual history candles failed",
			zap.Int64("position_id", positionID),
			zap.String("timeframe", timeframeFilter),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	integrity, err := s.buildVisualHistoryIntegrity(row, blocks)
	if err != nil {
		s.logger.Warn("build visual history candles integrity failed",
			zap.Int64("position_id", positionID),
			zap.String("timeframe", timeframeFilter),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	if !integrity.Check.OK {
		writeJSON(w, http.StatusConflict, errorResponse{Error: "history_candles_incomplete: position candles unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, visualHistoryCandlesResponse{
		PositionID:  positionID,
		PositionKey: row.PositionKey,
		IsOpen:      row.IsOpen,
		OpenTimeMS:  row.OpenTimeMS,
		CloseTimeMS: row.CloseTimeMS,
		Timeframes:  blocks,
		Integrity:   integrity,
	})
}

func (s *Server) handleVisualHistoryPositionIntegrity(w http.ResponseWriter, r *http.Request, positionID int64) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	row, err := s.loadVisualHistoryPositionByID(positionID)
	if err != nil {
		writeJSON(w, mapHistoryPositionErrorToStatus(err), errorResponse{Error: err.Error()})
		return
	}
	blocks, err := s.loadVisualHistoryCandleCache(positionID, "", row)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	resp, err := s.buildVisualHistoryIntegrity(row, blocks)
	if err != nil {
		s.logger.Warn("build visual history integrity failed",
			zap.Int64("position_id", positionID),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) queryVisualHistoryPositions(startMS, endMS int64, exchange, symbol string, filters visualHistoryPositionFilters, beforeMS int64, limit int) ([]visualHistoryPositionRow, visualHistoryPositionFilterGroup, bool, int64, error) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, visualHistoryPositionFilterGroup{}, false, 0, fmt.Errorf("history store unavailable")
	}
	if limit <= 0 {
		limit = visualHistoryDefaultPageLimit
	}

	closedRows, err := s.queryVisualHistoryClosedPositionRows(startMS, endMS, exchange, symbol)
	if err != nil {
		return nil, visualHistoryPositionFilterGroup{}, false, 0, err
	}

	candidates := make([]visualHistoryPositionCandidate, 0, len(closedRows))
	appendCandidate := func(row visualHistoryPositionRow) {
		meta := visualHistoryStrategyMetaFromRow(row)
		runtimeMeta := extractVisualHistoryPositionRunMeta(row.OpenRowJSON, row.CloseRowJSON)
		candidates = append(candidates, visualHistoryPositionCandidate{
			Row:         row,
			PositionKey: row.PositionKey,
			IsOpen:      row.IsOpen,
			RunID:       runtimeMeta.RunID,
			SingletonID: runtimeMeta.SingletonID,
			Strategy:    strings.TrimSpace(meta.StrategyName),
			Version:     strings.TrimSpace(meta.StrategyVersion),
			SortTime:    visualHistoryPositionSortTime(row),
		})
	}
	for _, row := range closedRows {
		appendCandidate(row)
	}

	filterOptions := buildVisualHistoryPositionFilterGroup(candidates)
	filtered := filterVisualHistoryPositionCandidates(candidates, filters, beforeMS)
	hasMore := len(filtered) > limit
	nextBefore := int64(0)
	if hasMore {
		filtered = filtered[:limit]
		nextBefore = visualHistoryPositionSortTime(filtered[len(filtered)-1])
	}
	return filtered, filterOptions, hasMore, nextBefore, nil
}

func (s *Server) queryVisualHistoryClosedPositionRows(startMS, endMS int64, exchange, symbol string) ([]visualHistoryPositionRow, error) {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString(`SELECT id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
	        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
	        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
	        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
	        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
	   FROM history_positions
	  WHERE COALESCE(state, '') <> ?
	    AND close_time_ms >= ?
	    AND close_time_ms < ?`)
	args := []any{historyStateSyncPending, startMS, endMS}

	if strings.TrimSpace(exchange) != "" {
		queryBuilder.WriteString(` AND exchange = ?`)
		args = append(args, exchange)
	}
	if strings.TrimSpace(symbol) != "" {
		queryBuilder.WriteString(` AND (UPPER(symbol) = UPPER(?) OR REPLACE(UPPER(symbol), '/', '') = REPLACE(UPPER(?), '/', ''))`)
		args = append(args, symbol, symbol)
	}
	queryBuilder.WriteString(` ORDER BY close_time_ms DESC, id DESC`)

	rows, err := s.cfg.HistoryStore.DB.Query(queryBuilder.String(), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]visualHistoryPositionRow, 0, visualHistoryDefaultPageLimit)
	for rows.Next() {
		var item visualHistoryPositionRow
		if scanErr := rows.Scan(
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
		); scanErr != nil {
			return nil, scanErr
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

func (s *Server) loadVisualHistoryPositionByID(positionID int64) (visualHistoryPositionRow, error) {
	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return visualHistoryPositionRow{}, fmt.Errorf("history store unavailable")
	}
	var item visualHistoryPositionRow
	err := s.cfg.HistoryStore.DB.QueryRow(
		`SELECT id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
		        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
		        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
		   FROM history_positions
		  WHERE id = ? LIMIT 1;`,
		positionID,
	).Scan(
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
	)
	if errors.Is(err, sql.ErrNoRows) {
		return visualHistoryPositionRow{}, fmt.Errorf("history position not found")
	}
	if err != nil {
		return visualHistoryPositionRow{}, err
	}
	if strings.EqualFold(strings.TrimSpace(item.State), historyStateSyncPending) {
		return visualHistoryPositionRow{}, fmt.Errorf("history position pending sync")
	}
	item.PositionKey = buildVisualHistoryPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS)
	item.Symbol = normalizeVisualHistorySymbol(item.Symbol, item.InstID)
	item.IsOpen = false
	item.RevisionMS = maxVisualHistoryInt64(item.UpdatedAtMS, item.CloseTimeMS)
	return item, nil
}

func (s *Server) loadVisualHistoryPosition(positionID int64, positionKey string) (visualHistoryPositionRow, error) {
	if strings.TrimSpace(positionKey) != "" {
		return s.loadVisualHistoryPositionByKey(positionKey)
	}
	if positionID > 0 {
		return s.loadVisualHistoryPositionByID(positionID)
	}
	return visualHistoryPositionRow{}, fmt.Errorf("history position not found")
}

func (s *Server) loadVisualHistoryPositionByKey(positionKey string) (visualHistoryPositionRow, error) {
	positionKey = strings.TrimSpace(positionKey)
	if positionKey == "" {
		return visualHistoryPositionRow{}, fmt.Errorf("history position not found")
	}
	exchange, instID, posSide, mgnMode, openTimeMS, err := parseVisualHistoryPositionKey(positionKey)
	if err != nil {
		return visualHistoryPositionRow{}, err
	}
	var item visualHistoryPositionRow
	err = s.cfg.HistoryStore.DB.QueryRow(
		`SELECT id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
		        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
		        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
		   FROM history_positions
		  WHERE exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ? AND open_time_ms = ? AND COALESCE(state, '') <> ?
		  ORDER BY close_time_ms DESC, id DESC
		  LIMIT 1;`,
		exchange, instID, posSide, mgnMode, openTimeMS, historyStateSyncPending,
	).Scan(
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
	)
	if errors.Is(err, sql.ErrNoRows) {
		return visualHistoryPositionRow{}, fmt.Errorf("history position not found")
	}
	if err != nil {
		return visualHistoryPositionRow{}, err
	}
	item.PositionKey = buildVisualHistoryPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS)
	item.Symbol = normalizeVisualHistorySymbol(item.Symbol, item.InstID)
	item.IsOpen = false
	item.RevisionMS = maxVisualHistoryInt64(item.UpdatedAtMS, item.CloseTimeMS)
	return item, nil
}

func mapHistoryPositionErrorToStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound
	case strings.Contains(msg, "pending sync"):
		return http.StatusConflict
	case strings.Contains(msg, "unavailable"):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func visualHistoryPositionFromRow(row visualHistoryPositionRow) visualHistoryPositionItem {
	meta := visualHistoryStrategyMetaFromRow(row)
	runtimeMeta := extractVisualHistoryPositionRunMeta(row.OpenRowJSON, row.CloseRowJSON)
	timeframes := normalizeMetaTimeframes(meta.StrategyTimeframes)
	indicators := normalizeMetaIndicators(meta.StrategyIndicators)
	if len(timeframes) == 0 {
		timeframes = []string{"15m"}
	}
	qty := math.Abs(parseNumericText(row.Pos))
	entryPrice := parseNumericText(row.AvgPx)
	notional := parseNumericText(row.NotionalUSD)
	if notional <= 0 && qty > 0 && entryPrice > 0 {
		notional = qty * entryPrice
	}
	return visualHistoryPositionItem{
		PositionUID:             buildVisualHistoryPositionUID(row),
		PositionKey:             row.PositionKey,
		ID:                      row.ID,
		IsOpen:                  row.IsOpen,
		DisplayState:            visualHistoryDisplayState(row),
		Exchange:                strings.TrimSpace(row.Exchange),
		Symbol:                  normalizeVisualHistorySymbol(row.Symbol, row.InstID),
		InstID:                  strings.TrimSpace(row.InstID),
		PositionSide:            strings.TrimSpace(row.PosSide),
		MarginMode:              strings.TrimSpace(row.MgnMode),
		Leverage:                parseNumericText(row.Lever),
		Margin:                  parseNumericText(row.Margin),
		EntryPrice:              entryPrice,
		ExitPrice:               parseNumericText(row.CloseAvgPx),
		TakeProfitPrice:         parseNumericText(row.TPTriggerPx),
		StopLossPrice:           parseNumericText(row.SLTriggerPx),
		Quantity:                qty,
		NotionalUSD:             notional,
		RealizedPnL:             parseNumericText(row.RealizedPnl),
		PnLRatio:                parseNumericText(row.PnlRatio),
		Fee:                     parseNumericText(row.Fee),
		FundingFee:              parseNumericText(row.FundingFee),
		OpenTimeMS:              row.OpenTimeMS,
		CloseTimeMS:             row.CloseTimeMS,
		OpenUpdateTimeMS:        row.OpenUpdateTimeMS,
		UpdatedAtMS:             row.UpdatedAtMS,
		MaxFloatingLossAmount:   row.MaxFloatingLossAmount,
		MaxFloatingProfitAmount: row.MaxFloatingProfitAmount,
		State:                   strings.TrimSpace(row.State),
		SingletonID:             runtimeMeta.SingletonID,
		RunID:                   runtimeMeta.RunID,
		StrategyName:            strings.TrimSpace(meta.StrategyName),
		StrategyVersion:         strings.TrimSpace(meta.StrategyVersion),
		Timeframes:              timeframes,
		Indicators:              indicators,
		RevisionMS:              row.RevisionMS,
		PollIntervalMS:          visualHistoryPollIntervalMS(timeframes),
	}
}

func buildVisualHistoryPositionUID(row visualHistoryPositionRow) string {
	if !row.IsOpen && row.ID > 0 {
		return fmt.Sprintf("h:%d", row.ID)
	}
	positionKey := strings.TrimSpace(row.PositionKey)
	if positionKey == "" {
		positionKey = buildVisualHistoryPositionKey(row.Exchange, row.InstID, row.PosSide, row.MgnMode, row.OpenTimeMS)
	}
	if positionKey == "" {
		return ""
	}
	return "o:" + positionKey
}

func (s *Server) buildVisualHistoryEvents(row visualHistoryPositionRow) ([]visualHistoryEventEntry, error) {
	events := make([]visualHistoryEventEntry, 0, 64)
	events = append(events, buildSyntheticHistoryEvents(row)...)
	rangeStart, rangeEnd := positionEventRange(row)
	meta := visualHistoryStrategyMetaFromRow(row)
	scope := s.resolveVisualHistoryRunScope(row)

	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return sortVisualHistoryEvents(events), nil
	}

	signalEvents, err := querySignalEventsFromStore(s.cfg.HistoryStore.DB, row, meta, rangeStart, rangeEnd, scope, visualHistorySignalEventFilter{})
	if err != nil {
		return nil, err
	}
	events = append(events, signalEvents...)

	executionEvents, err := queryExecutionEventsFromStore(s.cfg.HistoryStore.DB, row, rangeStart, rangeEnd, scope)
	if err != nil {
		return nil, err
	}
	events = append(events, executionEvents...)

	return sortVisualHistoryEvents(events), nil
}

func visualHistoryStrategyMetaFromRow(row visualHistoryPositionRow) models.StrategyContextMeta {
	return models.MergeStrategyContextMeta(
		models.ExtractStrategyContextMeta(row.OpenRowJSON),
		models.ExtractStrategyContextMeta(row.CloseRowJSON),
	)
}

func signalEventAnchorTimestamp(trendingTimestamp, triggerTimestamp int64) int64 {
	switch {
	case trendingTimestamp > 0 && triggerTimestamp > 0:
		if trendingTimestamp < triggerTimestamp {
			return trendingTimestamp
		}
		return triggerTimestamp
	case trendingTimestamp > 0:
		return trendingTimestamp
	case triggerTimestamp > 0:
		return triggerTimestamp
	default:
		return 0
	}
}

func buildBubbleSignalEventRange(timeframe string, trendingTimestamp, triggerTimestamp int64, now time.Time) (int64, int64) {
	anchor := signalEventAnchorTimestamp(trendingTimestamp, triggerTimestamp)
	buffer := time.Hour
	if duration, ok := market.TimeframeDuration(strings.TrimSpace(timeframe)); ok && duration > buffer {
		buffer = duration
	}
	if buffer > 24*time.Hour {
		buffer = 24 * time.Hour
	}
	if anchor <= 0 {
		return now.Add(-7 * 24 * time.Hour).UnixMilli(), now.Add(buffer).UnixMilli()
	}
	start := anchor - int64(buffer/time.Millisecond)
	if start < 0 {
		start = 0
	}
	end := now.Add(buffer).UnixMilli()
	if end <= start {
		end = start + int64(time.Hour/time.Millisecond)
	}
	return start, end
}

func splitBubbleSignalEventExchanges(raw string) []string {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, item := range parts {
		normalized := strings.TrimSpace(strings.ToLower(item))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	if len(out) == 0 {
		single := strings.TrimSpace(strings.ToLower(raw))
		if single != "" {
			return []string{single}
		}
	}
	return out
}

func buildBubbleSignalEventFilters(req bubbleSignalEventsRequest) []visualHistorySignalEventFilter {
	filters := make([]visualHistorySignalEventFilter, 0, 4)
	appendFilter := func(version, groupID string) {
		filter := visualHistorySignalEventFilter{
			StrategyVersion: strings.TrimSpace(version),
			GroupID:         strings.TrimSpace(groupID),
		}
		for _, existing := range filters {
			if strings.EqualFold(existing.StrategyVersion, filter.StrategyVersion) &&
				strings.EqualFold(existing.GroupID, filter.GroupID) {
				return
			}
		}
		filters = append(filters, filter)
	}
	appendFilter(req.StrategyVersion, req.GroupID)
	if req.GroupID != "" {
		appendFilter(req.StrategyVersion, "")
	}
	if req.StrategyVersion != "" {
		appendFilter("", req.GroupID)
	}
	appendFilter("", "")
	return filters
}

func (s *Server) buildBubbleSignalEvents(req bubbleSignalEventsRequest) ([]visualHistoryEventEntry, error) {
	req.Exchange = strings.TrimSpace(req.Exchange)
	req.Symbol = strings.TrimSpace(req.Symbol)
	req.Timeframe = strings.TrimSpace(req.Timeframe)
	req.Strategy = strings.TrimSpace(req.Strategy)
	req.StrategyVersion = strings.TrimSpace(req.StrategyVersion)
	req.ComboKey = strings.TrimSpace(req.ComboKey)
	req.GroupID = strings.TrimSpace(req.GroupID)
	if req.Exchange == "" || req.Symbol == "" || req.Timeframe == "" || req.Strategy == "" {
		return nil, fmt.Errorf("signal events query requires exchange, symbol, timeframe, and strategy")
	}

	if s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	startMS, endMS := buildBubbleSignalEventRange(req.Timeframe, req.TrendingTimestamp, req.TriggerTimestamp, time.Now())
	meta := models.NormalizeStrategyContextMeta(models.StrategyContextMeta{
		StrategyName:       req.Strategy,
		StrategyVersion:    req.StrategyVersion,
		TrendingTimestamp:  int(signalEventAnchorTimestamp(req.TrendingTimestamp, req.TriggerTimestamp)),
		StrategyTimeframes: []string{req.Timeframe},
		ComboKey:           req.ComboKey,
		GroupID:            req.GroupID,
	})
	row := visualHistoryPositionRow{
		Exchange: req.Exchange,
		Symbol:   req.Symbol,
	}

	sourceDB := s.cfg.HistoryStore.DB
	exchanges := splitBubbleSignalEventExchanges(req.Exchange)
	filters := buildBubbleSignalEventFilters(req)
	for _, exchange := range exchanges {
		row.Exchange = exchange
		for _, filter := range filters {
			events, queryErr := querySignalEventsFromStore(
				sourceDB,
				row,
				meta,
				startMS,
				endMS,
				visualHistoryRunScope{},
				filter,
			)
			if queryErr != nil {
				return nil, queryErr
			}
			if len(events) > 0 {
				return sortVisualHistoryEvents(events), nil
			}
		}
	}
	return nil, nil
}

func buildVisualHistoryPositionFilterGroup(candidates []visualHistoryPositionCandidate) visualHistoryPositionFilterGroup {
	group := visualHistoryPositionFilterGroup{
		RunIDs:     []string{},
		RunOptions: []visualHistoryRunOption{},
		Strategies: []string{},
		Versions:   []string{},
		Exchanges:  []string{},
		Symbols:    []string{},
	}
	runOptionsByValue := make(map[string]visualHistoryRunOption, len(candidates))
	strategySeen := make(map[string]struct{}, len(candidates))
	versionSeen := make(map[string]struct{}, len(candidates))
	exchangeSeen := make(map[string]struct{}, len(candidates))
	symbolSeen := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		exchange := strings.TrimSpace(item.Row.Exchange)
		if exchange != "" {
			if _, exists := exchangeSeen[exchange]; !exists {
				exchangeSeen[exchange] = struct{}{}
				group.Exchanges = append(group.Exchanges, exchange)
			}
		}
		symbol := strings.TrimSpace(item.Row.Symbol)
		if symbol != "" {
			if _, exists := symbolSeen[symbol]; !exists {
				symbolSeen[symbol] = struct{}{}
				group.Symbols = append(group.Symbols, symbol)
			}
		}
		runID := strings.TrimSpace(item.RunID)
		if runID != "" {
			current := visualHistoryRunOption{
				Value:       runID,
				Label:       formatVisualHistoryRunOptionLabel(runID, item.SingletonID),
				SingletonID: item.SingletonID,
			}
			existing, exists := runOptionsByValue[runID]
			if !exists {
				runOptionsByValue[runID] = current
			} else if existing.SingletonID <= 0 && current.SingletonID > 0 {
				runOptionsByValue[runID] = current
			}
		}
		strategy := strings.TrimSpace(item.Strategy)
		if strategy != "" {
			if _, exists := strategySeen[strategy]; !exists {
				strategySeen[strategy] = struct{}{}
				group.Strategies = append(group.Strategies, strategy)
			}
		}
		version := strings.TrimSpace(item.Version)
		if version != "" {
			if _, exists := versionSeen[version]; !exists {
				versionSeen[version] = struct{}{}
				group.Versions = append(group.Versions, version)
			}
		}
	}
	if len(runOptionsByValue) > 0 {
		group.RunOptions = make([]visualHistoryRunOption, 0, len(runOptionsByValue))
		for _, item := range runOptionsByValue {
			group.RunOptions = append(group.RunOptions, item)
		}
		sort.Slice(group.RunOptions, func(i, j int) bool {
			left := group.RunOptions[i]
			right := group.RunOptions[j]
			leftHasID := left.SingletonID > 0
			rightHasID := right.SingletonID > 0
			if leftHasID != rightHasID {
				return leftHasID
			}
			if leftHasID && left.SingletonID != right.SingletonID {
				return left.SingletonID < right.SingletonID
			}
			return left.Value < right.Value
		})
		group.RunIDs = make([]string, 0, len(group.RunOptions))
		for _, item := range group.RunOptions {
			group.RunIDs = append(group.RunIDs, item.Value)
		}
	}
	return group
}

func filterVisualHistoryPositionCandidates(candidates []visualHistoryPositionCandidate, filters visualHistoryPositionFilters, beforeMS int64) []visualHistoryPositionRow {
	runID := strings.TrimSpace(filters.RunID)
	strategy := strings.TrimSpace(filters.Strategy)
	version := strings.TrimSpace(filters.Version)
	out := make([]visualHistoryPositionRow, 0, len(candidates))
	for _, item := range candidates {
		if item.IsOpen && beforeMS > 0 {
			continue
		}
		if beforeMS > 0 && item.SortTime >= beforeMS {
			continue
		}
		if runID != "" && strings.TrimSpace(item.RunID) != runID {
			continue
		}
		if strategy != "" && strings.TrimSpace(item.Strategy) != strategy {
			continue
		}
		if version != "" && strings.TrimSpace(item.Version) != version {
			continue
		}
		out = append(out, item.Row)
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		if left.IsOpen != right.IsOpen {
			return left.IsOpen
		}
		leftSort := visualHistoryPositionSortTime(left)
		rightSort := visualHistoryPositionSortTime(right)
		if leftSort != rightSort {
			return leftSort > rightSort
		}
		if left.PositionKey != right.PositionKey {
			return left.PositionKey < right.PositionKey
		}
		return left.ID > right.ID
	})
	return out
}

func buildSyntheticHistoryEvents(row visualHistoryPositionRow) []visualHistoryEventEntry {
	out := make([]visualHistoryEventEntry, 0, 2)
	openMS := row.OpenTimeMS
	if openMS <= 0 {
		openMS = row.CreatedAtMS
	}
	closeMS := int64(0)
	if !row.IsOpen {
		closeMS = row.CloseTimeMS
		if closeMS <= 0 {
			closeMS = row.UpdatedAtMS
		}
	}
	if openMS > 0 {
		out = append(out, visualHistoryEventEntry{
			ID:      fmt.Sprintf("position-entry-%d", row.ID),
			Source:  "position",
			Type:    "ENTRY",
			Level:   "info",
			EventAt: openMS,
			Title:   "仓位开仓",
			Summary: "仓位进入持仓状态",
			Detail: map[string]any{
				"entry_price":   parseNumericText(row.AvgPx),
				"quantity":      math.Abs(parseNumericText(row.Pos)),
				"position_side": strings.TrimSpace(row.PosSide),
			},
		})
	}
	if !row.IsOpen && closeMS > 0 {
		level := "success"
		if parseNumericText(row.RealizedPnl) < 0 {
			level = "error"
		}
		out = append(out, visualHistoryEventEntry{
			ID:      fmt.Sprintf("position-exit-%d", row.ID),
			Source:  "position",
			Type:    "EXIT",
			Level:   level,
			EventAt: closeMS,
			Title:   "仓位平仓",
			Summary: "仓位结束",
			Detail: map[string]any{
				"exit_price":   parseNumericText(row.CloseAvgPx),
				"realized_pnl": parseNumericText(row.RealizedPnl),
				"pnl_ratio":    parseNumericText(row.PnlRatio),
			},
		})
	}
	return out
}

type signalEventMeta struct {
	TPPrice             float64
	SLPrice             float64
	EntryPrice          float64
	InitialSL           float64
	InitialRiskPct      float64
	MaxFavorablePct     float64
	MFER                float64
	Action              int
	HasPosition         int
	HighSide            int
	MidSide             int
	OrderType           string
	ProfitProtectStage  int
	EntryWatchTimestamp int64
	TrendingTimestamp   int64
	TriggerTimestamp    int64
	StrategyTimeframes  []string
	ComboKey            string
	GroupID             string
}

type bubbleSignalEventsRequest struct {
	Exchange          string
	Symbol            string
	Timeframe         string
	Strategy          string
	StrategyVersion   string
	ComboKey          string
	GroupID           string
	TriggerTimestamp  int64
	TrendingTimestamp int64
}

type visualHistorySignalEventFilter struct {
	StrategyVersion string
	GroupID         string
}

type visualHistoryStrategyIdentity struct {
	Strategy            string
	PrimaryTimeframe    string
	Timeframes          []string
	ComboKey            string
	ImplicitPrimaryOnly bool
}

func extractSignalEventMeta(signalJSON string) signalEventMeta {
	raw := strings.TrimSpace(signalJSON)
	if raw == "" {
		return signalEventMeta{}
	}
	var signal models.Signal
	if err := json.Unmarshal([]byte(raw), &signal); err == nil {
		meta := signalEventMeta{
			TPPrice:             normalizePositivePrice(signal.TP),
			SLPrice:             normalizePositivePrice(signal.SL),
			EntryPrice:          normalizePositivePrice(signal.Entry),
			InitialSL:           normalizePositivePrice(signal.InitialSL),
			InitialRiskPct:      normalizePositiveNumber(signal.InitialRiskPct),
			MaxFavorablePct:     normalizePositiveNumber(signal.MaxFavorableProfitPct),
			Action:              signal.Action,
			HasPosition:         signal.HasPosition,
			HighSide:            signal.HighSide,
			MidSide:             signal.MidSide,
			OrderType:           strings.TrimSpace(signal.OrderType),
			ProfitProtectStage:  signal.ProfitProtectStage,
			EntryWatchTimestamp: int64(signal.EntryWatchTimestamp),
			TrendingTimestamp:   int64(signal.TrendingTimestamp),
			TriggerTimestamp:    int64(signal.TriggerTimestamp),
			StrategyTimeframes:  normalizeMetaTimeframes(signal.StrategyTimeframes),
			ComboKey:            strings.TrimSpace(signal.ComboKey),
			GroupID:             strings.TrimSpace(signal.GroupID),
		}
		meta.MFER = computeProfitProtectMFER(meta.InitialRiskPct, meta.MaxFavorablePct)
		return meta
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return signalEventMeta{}
	}
	meta := signalEventMeta{
		TPPrice: normalizePositivePrice(readNumericValueByPath(payload, "tp")),
		SLPrice: normalizePositivePrice(readNumericValueByPath(payload, "sl")),
		EntryPrice: firstPositivePrice(
			readNumericValueByPath(payload, "entry"),
			readNumericValueByPath(payload, "entry_price"),
			readNumericValueByPath(payload, "entryPrice"),
		),
		InitialSL: firstPositivePrice(
			readNumericValueByPath(payload, "initial_sl"),
			readNumericValueByPath(payload, "initialSL"),
		),
		InitialRiskPct: normalizePositiveNumber(firstNonZeroValue(
			readNumericValueByPath(payload, "initial_risk_pct"),
			readNumericValueByPath(payload, "initialRiskPct"),
		)),
		MaxFavorablePct: normalizePositiveNumber(firstNonZeroValue(
			readNumericValueByPath(payload, "max_favorable_profit_pct"),
			readNumericValueByPath(payload, "maxFavorableProfitPct"),
		)),
		Action: normalizeIntegerValue(readNumericValueByPath(payload, "action")),
		HasPosition: normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "has_position"),
			readNumericValueByPath(payload, "hasPosition"),
		)),
		HighSide: normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "high_side"),
			readNumericValueByPath(payload, "highSide"),
		)),
		MidSide: normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "mid_side"),
			readNumericValueByPath(payload, "midSide"),
		)),
		OrderType: normalizeStringValue(firstNonEmptyString(
			readStringValueByPath(payload, "order_type"),
			readStringValueByPath(payload, "orderType"),
		)),
		ProfitProtectStage: normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "profit_protect_stage"),
			readNumericValueByPath(payload, "profitProtectStage"),
		)),
		EntryWatchTimestamp: int64(normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "entry_watch_timestamp"),
			readNumericValueByPath(payload, "entryWatchTimestamp"),
			readNumericValueByPath(payload, "entry_watch_ts_ms"),
			readNumericValueByPath(payload, "entryWatchTsMs"),
		))),
		TrendingTimestamp: int64(normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "trending_timestamp"),
			readNumericValueByPath(payload, "trendingTimestamp"),
			readNumericValueByPath(payload, "trending_ts_ms"),
			readNumericValueByPath(payload, "trendingTsMs"),
		))),
		TriggerTimestamp: int64(normalizeIntegerValue(firstNonZeroValue(
			readNumericValueByPath(payload, "trigger_timestamp"),
			readNumericValueByPath(payload, "triggerTimestamp"),
			readNumericValueByPath(payload, "trigger_ts_ms"),
			readNumericValueByPath(payload, "triggerTsMs"),
		))),
		StrategyTimeframes: readTimeframesByPath(payload, "strategy_timeframes"),
		ComboKey: normalizeStringValue(firstNonEmptyString(
			readStringValueByPath(payload, "combo_key"),
			readStringValueByPath(payload, "comboKey"),
		)),
		GroupID: normalizeStringValue(firstNonEmptyString(
			readStringValueByPath(payload, "group_id"),
			readStringValueByPath(payload, "groupId"),
		)),
	}
	meta.MFER = computeProfitProtectMFER(meta.InitialRiskPct, meta.MaxFavorablePct)
	return meta
}

func normalizePositivePrice(value float64) float64 {
	if !isFinitePositive(value) {
		return 0
	}
	return value
}

func normalizePositiveNumber(value float64) float64 {
	if !isFinitePositive(value) {
		return 0
	}
	return value
}

func firstPositivePrice(values ...float64) float64 {
	for _, value := range values {
		if isFinitePositive(value) {
			return value
		}
	}
	return 0
}

func isFinitePositive(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0
}

func firstNonZeroValue(values ...float64) float64 {
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value == 0 {
			continue
		}
		return value
	}
	return 0
}

func normalizeIntegerValue(value float64) int {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return int(math.Round(value))
}

func normalizeStringValue(value string) string {
	return strings.TrimSpace(value)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		normalized := normalizeStringValue(value)
		if normalized != "" {
			return normalized
		}
	}
	return ""
}

func computeProfitProtectMFER(initialRiskPct, maxFavorablePct float64) float64 {
	if !isFinitePositive(initialRiskPct) || !isFinitePositive(maxFavorablePct) {
		return 0
	}
	return maxFavorablePct / initialRiskPct
}

func readNumericValueByPath(input map[string]any, path ...string) float64 {
	if len(path) == 0 || input == nil {
		return 0
	}
	var current any = input
	for _, key := range path {
		mapped, ok := current.(map[string]any)
		if !ok {
			return 0
		}
		next, found := lookupCaseInsensitiveMapValue(mapped, key)
		if !found {
			return 0
		}
		current = next
	}
	if value, ok := castVisualHistoryNumber(current); ok {
		return value
	}
	return 0
}

func readStringValueByPath(input map[string]any, path ...string) string {
	if len(path) == 0 || input == nil {
		return ""
	}
	var current any = input
	for _, key := range path {
		mapped, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		next, found := lookupCaseInsensitiveMapValue(mapped, key)
		if !found {
			return ""
		}
		current = next
	}
	switch value := current.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return strings.TrimSpace(value.String())
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func readTimeframesByPath(input map[string]any, path ...string) []string {
	if len(path) == 0 || input == nil {
		return nil
	}
	var current any = input
	for _, key := range path {
		mapped, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		next, found := lookupCaseInsensitiveMapValue(mapped, key)
		if !found {
			return nil
		}
		current = next
	}
	switch value := current.(type) {
	case []string:
		return normalizeMetaTimeframes(value)
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			switch typed := item.(type) {
			case string:
				out = append(out, typed)
			default:
				text := strings.TrimSpace(fmt.Sprint(typed))
				if text != "" && text != "<nil>" {
					out = append(out, text)
				}
			}
		}
		return normalizeMetaTimeframes(out)
	case string:
		return normalizeMetaTimeframes(strings.Split(strings.TrimSpace(value), "/"))
	default:
		return nil
	}
}

func buildVisualHistoryStrategyIdentity(strategy, primary string, timeframes []string, comboKey string) visualHistoryStrategyIdentity {
	strategy = strings.TrimSpace(strategy)
	rawPrimary := strings.ToLower(strings.TrimSpace(primary))
	rawTimeframes := normalizeMetaTimeframes(timeframes)
	rawCombo := strings.ToLower(strings.TrimSpace(comboKey))
	resolvedPrimary, resolvedTimeframes, resolvedCombo := common.NormalizeStrategyIdentity(rawPrimary, rawTimeframes, rawCombo)
	implicitPrimaryOnly := len(rawTimeframes) == 0 &&
		rawPrimary != "" &&
		(rawCombo == "" || strings.EqualFold(rawCombo, rawPrimary))
	return visualHistoryStrategyIdentity{
		Strategy:            strategy,
		PrimaryTimeframe:    resolvedPrimary,
		Timeframes:          resolvedTimeframes,
		ComboKey:            resolvedCombo,
		ImplicitPrimaryOnly: implicitPrimaryOnly,
	}
}

func visualHistoryIdentityFromMeta(meta models.StrategyContextMeta) visualHistoryStrategyIdentity {
	meta = models.NormalizeStrategyContextMeta(meta)
	return buildVisualHistoryStrategyIdentity(meta.StrategyName, "", meta.StrategyTimeframes, meta.ComboKey)
}

func visualHistoryIdentityFromSignalMeta(strategy, timeframe string, meta signalEventMeta) visualHistoryStrategyIdentity {
	return buildVisualHistoryStrategyIdentity(strategy, timeframe, meta.StrategyTimeframes, meta.ComboKey)
}

func visualHistoryStrategyIdentityMatches(positionMeta models.StrategyContextMeta, event visualHistoryStrategyIdentity) bool {
	position := visualHistoryIdentityFromMeta(positionMeta)
	if position.Strategy != "" && event.Strategy != "" && !strings.EqualFold(position.Strategy, event.Strategy) {
		return false
	}
	if position.ComboKey == "" || event.ComboKey == "" {
		return true
	}
	if strings.EqualFold(position.ComboKey, event.ComboKey) {
		return true
	}
	if position.PrimaryTimeframe != "" &&
		event.PrimaryTimeframe != "" &&
		strings.EqualFold(position.PrimaryTimeframe, event.PrimaryTimeframe) &&
		(position.ImplicitPrimaryOnly || event.ImplicitPrimaryOnly) {
		return true
	}
	return false
}

func parseVisualHistoryDetailJSON(raw string) map[string]any {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil
	}
	var detail map[string]any
	if err := json.Unmarshal([]byte(text), &detail); err != nil {
		return nil
	}
	if len(detail) == 0 {
		return nil
	}
	return detail
}

func lookupCaseInsensitiveMapValue(input map[string]any, key string) (any, bool) {
	if value, ok := input[key]; ok {
		return value, true
	}
	target := strings.ToLower(strings.TrimSpace(key))
	for itemKey, value := range input {
		if strings.ToLower(strings.TrimSpace(itemKey)) == target {
			return value, true
		}
	}
	return nil, false
}

func castVisualHistoryNumber(value any) (float64, bool) {
	switch item := value.(type) {
	case float64:
		if math.IsNaN(item) || math.IsInf(item, 0) {
			return 0, false
		}
		return item, true
	case float32:
		out := float64(item)
		if math.IsNaN(out) || math.IsInf(out, 0) {
			return 0, false
		}
		return out, true
	case int:
		return float64(item), true
	case int8:
		return float64(item), true
	case int16:
		return float64(item), true
	case int32:
		return float64(item), true
	case int64:
		return float64(item), true
	case uint:
		return float64(item), true
	case uint8:
		return float64(item), true
	case uint16:
		return float64(item), true
	case uint32:
		return float64(item), true
	case uint64:
		return float64(item), true
	case json.Number:
		out, err := item.Float64()
		if err != nil || math.IsNaN(out) || math.IsInf(out, 0) {
			return 0, false
		}
		return out, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(item), 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func querySignalEventsFromStore(
	db *sql.DB,
	row visualHistoryPositionRow,
	meta models.StrategyContextMeta,
	startMS, endMS int64,
	scope visualHistoryRunScope,
	filter visualHistorySignalEventFilter,
) ([]visualHistoryEventEntry, error) {
	if db == nil {
		return nil, nil
	}
	query := `SELECT s.id,
	                 COALESCE(s.singleton_id, 0),
	                 COALESCE(s.timeframe, ''),
	                 COALESCE(s.strategy, ''),
	                 COALESCE(s.strategy_version, ''),
	                 COALESCE(s.change_status, 0),
	                 COALESCE(s.changed_fields, ''),
	                 COALESCE(s.signal_json, ''),
	                 COALESCE(s.event_at_ms, 0)
	            FROM signals s
	            JOIN exchanges e ON e.id = s.exchange_id
	            JOIN symbols m ON m.id = s.symbol_id
	           WHERE e.name = ?
	             AND m.symbol = ?
	             AND COALESCE(s.event_at_ms, 0) >= ?
	             AND COALESCE(s.event_at_ms, 0) <= ?
	           ORDER BY s.event_at_ms ASC, s.id ASC;`
	rows, err := db.Query(query, row.Exchange, row.Symbol, startMS, endMS)
	if err != nil {
		if isNoSuchTableErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	strategyName := strings.TrimSpace(meta.StrategyName)
	out := make([]visualHistoryEventEntry, 0)
	for rows.Next() {
		var (
			sourceID      int64
			singletonID   int64
			timeframe     string
			strategy      string
			strategyVer   string
			changeStatus  int
			changedFields string
			signalJSON    string
			eventAtMS     int64
		)
		if scanErr := rows.Scan(
			&sourceID,
			&singletonID,
			&timeframe,
			&strategy,
			&strategyVer,
			&changeStatus,
			&changedFields,
			&signalJSON,
			&eventAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		if scope.SingletonID > 0 && singletonID > 0 && singletonID != scope.SingletonID {
			continue
		}
		if strategyName != "" {
			itemStrategy := strings.TrimSpace(strategy)
			if itemStrategy != "" && itemStrategy != strategyName {
				continue
			}
		}
		if filter.StrategyVersion != "" {
			itemVersion := strings.TrimSpace(strategyVer)
			if itemVersion == "" || !strings.EqualFold(itemVersion, filter.StrategyVersion) {
				continue
			}
		}

		signalMeta := extractSignalEventMeta(signalJSON)
		identity := visualHistoryIdentityFromSignalMeta(strategy, timeframe, signalMeta)
		if !visualHistoryStrategyIdentityMatches(meta, identity) {
			continue
		}
		if filter.GroupID != "" {
			if signalMeta.GroupID == "" || !strings.EqualFold(signalMeta.GroupID, filter.GroupID) {
				continue
			}
		}
		eventType := classifySignalEventType(changeStatus, changedFields, signalMeta)
		title := "策略信号"
		switch eventType {
		case "ARMED":
			title = "Armed"
		case "R_PROTECT_2R":
			title = "2R 保本保护"
		case "R_PROTECT_4R":
			title = "4R 部分平仓保护"
		case "TREND_DETECTED":
			title = "趋势检测"
		case "HIGH_SIDE_CHANGED":
			title = "高周期方向变化"
		case "MID_SIDE_CHANGED":
			title = "中周期状态变化"
		case "TRAILING_STOP":
			title = "移动止损"
		case "TRAILING_TP":
			title = "移动止盈"
		case "TRAILING_TP_SL":
			title = "移动止盈止损"
		}
		detail := map[string]any{
			"timeframe":        strings.TrimSpace(timeframe),
			"strategy":         strings.TrimSpace(strategy),
			"strategy_version": strings.TrimSpace(strategyVer),
			"change_status":    changeStatus,
			"changed_fields":   strings.TrimSpace(changedFields),
		}
		if len(signalMeta.StrategyTimeframes) > 0 {
			detail["strategy_timeframes"] = append([]string(nil), signalMeta.StrategyTimeframes...)
		}
		if signalMeta.ComboKey != "" {
			detail["combo_key"] = signalMeta.ComboKey
		}
		if signalMeta.GroupID != "" {
			detail["group_id"] = signalMeta.GroupID
		}
		if signalMeta.Action != 0 {
			detail["action"] = signalMeta.Action
		}
		if signalMeta.HasPosition != 0 {
			detail["has_position"] = signalMeta.HasPosition
		}
		if signalMeta.EntryPrice > 0 {
			detail["entry_price"] = signalMeta.EntryPrice
		}
		if signalMeta.InitialSL > 0 {
			detail["initial_sl"] = signalMeta.InitialSL
		}
		if signalMeta.InitialRiskPct > 0 {
			detail["initial_risk_pct"] = signalMeta.InitialRiskPct
		}
		if signalMeta.MaxFavorablePct > 0 {
			detail["max_favorable_profit_pct"] = signalMeta.MaxFavorablePct
		}
		if signalMeta.MFER > 0 {
			detail["mfer"] = signalMeta.MFER
		}
		if signalMeta.HighSide != 0 {
			detail["high_side"] = signalMeta.HighSide
		}
		if signalMeta.MidSide != 0 {
			detail["mid_side"] = signalMeta.MidSide
		}
		if signalMeta.OrderType != "" {
			detail["order_type"] = signalMeta.OrderType
		}
		if signalMeta.ProfitProtectStage > 0 {
			detail["profit_protect_stage"] = signalMeta.ProfitProtectStage
		}
		if signalMeta.EntryWatchTimestamp > 0 {
			detail["entry_watch_timestamp"] = signalMeta.EntryWatchTimestamp
		}
		if signalMeta.TrendingTimestamp > 0 {
			detail["trending_timestamp"] = signalMeta.TrendingTimestamp
		}
		if signalMeta.TriggerTimestamp > 0 {
			detail["trigger_timestamp"] = signalMeta.TriggerTimestamp
		}
		if signalMeta.TPPrice > 0 {
			detail["tp_price"] = signalMeta.TPPrice
		}
		if signalMeta.SLPrice > 0 {
			detail["sl_price"] = signalMeta.SLPrice
		}
		out = append(out, visualHistoryEventEntry{
			ID:      fmt.Sprintf("signal-%d", sourceID),
			Source:  "signal",
			Type:    eventType,
			Level:   classifySignalLevel(changeStatus),
			EventAt: eventAtMS,
			Title:   title,
			Summary: strings.TrimSpace(changedFields),
			Detail:  detail,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func queryExecutionEventsFromStore(db *sql.DB, row visualHistoryPositionRow, startMS, endMS int64, scope visualHistoryRunScope) ([]visualHistoryEventEntry, error) {
	if db == nil {
		return nil, nil
	}
	query := `SELECT id, action, order_type, position_side, margin_mode, size, leverage_multiplier, price,
	                 take_profit_price, stop_loss_price, strategy, result_status,
	                 fail_source, fail_stage, fail_reason, client_order_id, started_at_ms, finished_at_ms, duration_ms
	          FROM orders
	          WHERE exchange = ? AND symbol = ? AND started_at_ms >= ? AND started_at_ms <= ?`
	args := []any{row.Exchange, row.Symbol, startMS, endMS}
	if strings.TrimSpace(scope.SingletonUUID) != "" {
		query += ` AND singleton_uuid = ?`
		args = append(args, strings.TrimSpace(scope.SingletonUUID))
	}
	query += ` ORDER BY started_at_ms ASC, id ASC;`
	rows, err := db.Query(query, args...)
	if err != nil {
		if isNoSuchTableErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer rows.Close()

	out := make([]visualHistoryEventEntry, 0)
	for rows.Next() {
		var (
			sourceID      int64
			action        string
			orderType     string
			positionSide  string
			marginMode    string
			size          float64
			leverage      float64
			price         float64
			tpPrice       float64
			slPrice       float64
			strategy      string
			resultStatus  string
			failSource    string
			failStage     string
			failReason    string
			clientOrderID string
			startedAtMS   int64
			finishedAtMS  int64
			durationMS    int64
		)
		if scanErr := rows.Scan(
			&sourceID,
			&action,
			&orderType,
			&positionSide,
			&marginMode,
			&size,
			&leverage,
			&price,
			&tpPrice,
			&slPrice,
			&strategy,
			&resultStatus,
			&failSource,
			&failStage,
			&failReason,
			&clientOrderID,
			&startedAtMS,
			&finishedAtMS,
			&durationMS,
		); scanErr != nil {
			return nil, scanErr
		}
		summary := strings.TrimSpace(resultStatus)
		if summary == "" {
			summary = strings.TrimSpace(action)
		}
		detail := map[string]any{
			"action":              strings.TrimSpace(action),
			"order_type":          strings.TrimSpace(orderType),
			"position_side":       strings.TrimSpace(positionSide),
			"margin_mode":         strings.TrimSpace(marginMode),
			"size":                size,
			"leverage_multiplier": leverage,
			"price":               price,
			"take_profit_price":   tpPrice,
			"stop_loss_price":     slPrice,
			"strategy":            strings.TrimSpace(strategy),
			"result_status":       strings.TrimSpace(resultStatus),
			"fail_source":         strings.TrimSpace(failSource),
			"fail_stage":          strings.TrimSpace(failStage),
			"fail_reason":         strings.TrimSpace(failReason),
			"client_order_id":     strings.TrimSpace(clientOrderID),
			"finished_at_ms":      finishedAtMS,
			"duration_ms":         durationMS,
		}
		if tpPrice > 0 {
			detail["tp_price"] = tpPrice
		}
		if slPrice > 0 {
			detail["sl_price"] = slPrice
		}
		out = append(out, visualHistoryEventEntry{
			ID:      fmt.Sprintf("execution-%d", sourceID),
			Source:  "execution",
			Type:    "EXECUTION",
			Level:   classifyExecutionLevel(resultStatus, failReason),
			EventAt: startedAtMS,
			Title:   "执行记录",
			Summary: summary,
			Detail:  detail,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func queryExecutionOrderItems(db *sql.DB, filter executionOrderFilter, startMS, endMS int64) ([]executionOrderItem, error) {
	if db == nil {
		return nil, nil
	}
	filter, err := normalizeExecutionOrderFilter(filter)
	if err != nil {
		return nil, err
	}
	query := `SELECT id, attempt_id, singleton_uuid, mode, source, exchange, symbol, action,
	                 order_type, position_side, size, leverage_multiplier, price,
	                 take_profit_price, stop_loss_price, strategy, result_status,
	                 fail_source, fail_stage, fail_reason, exchange_code, exchange_message,
	                 exchange_order_id, has_side_effect, started_at_ms, finished_at_ms, duration_ms
	          FROM orders
	          WHERE started_at_ms >= ? AND started_at_ms <= ?`
	args := []any{startMS, endMS}
	if filter.Exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, filter.Exchange)
	}
	if filter.Symbol != "" {
		query += ` AND symbol = ?`
		args = append(args, filter.Symbol)
	}
	if filter.Action != "" {
		query += ` AND action = ?`
		args = append(args, filter.Action)
	}
	if filter.ResultStatus != "" {
		query += ` AND result_status = ?`
		args = append(args, filter.ResultStatus)
	}
	query += ` ORDER BY started_at_ms DESC, id DESC`
	if filter.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, filter.Limit)
	}
	query += `;`
	rows, err := db.Query(query, args...)
	if err != nil {
		if isNoSuchTableErr(err) {
			return []executionOrderItem{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]executionOrderItem, 0)
	for rows.Next() {
		var (
			item          executionOrderItem
			hasSideEffect int
		)
		if scanErr := rows.Scan(
			&item.SourceID,
			&item.AttemptID,
			&item.SingletonUUID,
			&item.Mode,
			&item.Source,
			&item.Exchange,
			&item.Symbol,
			&item.Action,
			&item.OrderType,
			&item.PositionSide,
			&item.Size,
			&item.LeverageMultiplier,
			&item.Price,
			&item.TakeProfitPrice,
			&item.StopLossPrice,
			&item.Strategy,
			&item.ResultStatus,
			&item.FailSource,
			&item.FailStage,
			&item.FailReason,
			&item.ExchangeCode,
			&item.ExchangeMessage,
			&item.ExchangeOrderID,
			&hasSideEffect,
			&item.StartedAtMS,
			&item.FinishedAtMS,
			&item.DurationMS,
		); scanErr != nil {
			return nil, scanErr
		}
		item.HasSideEffect = hasSideEffect != 0
		item.StartedTime = formatLocalTimeMS(item.StartedAtMS)
		item.FinishedTime = formatLocalTimeMS(item.FinishedAtMS)
		if item.DurationMS <= 0 && item.StartedAtMS > 0 && item.FinishedAtMS >= item.StartedAtMS {
			item.DurationMS = item.FinishedAtMS - item.StartedAtMS
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func queryRiskDecisionItems(db *sql.DB, filter riskDecisionFilter, startMS, endMS int64) ([]riskDecisionItem, error) {
	if db == nil {
		return nil, nil
	}
	filter, err := normalizeRiskDecisionFilter(filter)
	if err != nil {
		return nil, err
	}
	query := `SELECT id, singleton_id, singleton_uuid, mode, exchange, symbol, timeframe, strategy,
	                 combo_key, group_id, signal_action, high_side, decision_action, result_status,
	                 reject_reason, event_at_ms, trigger_timestamp_ms, trending_timestamp_ms
	          FROM risk_decisions
	          WHERE event_at_ms >= ? AND event_at_ms <= ?`
	args := []any{startMS, endMS}
	if filter.Exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, filter.Exchange)
	}
	if filter.Symbol != "" {
		query += ` AND symbol = ?`
		args = append(args, filter.Symbol)
	}
	if filter.Strategy != "" {
		query += ` AND strategy = ?`
		args = append(args, filter.Strategy)
	}
	if filter.GroupID != "" {
		query += ` AND group_id = ?`
		args = append(args, filter.GroupID)
	}
	if filter.SignalAction != 0 {
		query += ` AND signal_action = ?`
		args = append(args, filter.SignalAction)
	}
	if filter.DecisionAction != "" {
		query += ` AND decision_action = ?`
		args = append(args, filter.DecisionAction)
	}
	if filter.ResultStatus != "" {
		query += ` AND result_status = ?`
		args = append(args, filter.ResultStatus)
	}
	query += ` ORDER BY event_at_ms DESC, id DESC`
	if filter.Limit > 0 {
		query += ` LIMIT ?`
		args = append(args, filter.Limit)
	}
	query += `;`
	rows, err := db.Query(query, args...)
	if err != nil {
		if isNoSuchTableErr(err) {
			return []riskDecisionItem{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]riskDecisionItem, 0)
	for rows.Next() {
		var item riskDecisionItem
		if scanErr := rows.Scan(
			&item.ID,
			&item.SingletonID,
			&item.SingletonUUID,
			&item.Mode,
			&item.Exchange,
			&item.Symbol,
			&item.Timeframe,
			&item.Strategy,
			&item.ComboKey,
			&item.GroupID,
			&item.SignalAction,
			&item.HighSide,
			&item.DecisionAction,
			&item.ResultStatus,
			&item.RejectReason,
			&item.EventAtMS,
			&item.TriggerTimestampMS,
			&item.TrendingTimestampMS,
		); scanErr != nil {
			return nil, scanErr
		}
		item.EventTime = formatLocalTimeMS(item.EventAtMS)
		item.TriggerTime = formatLocalTimeMS(item.TriggerTimestampMS)
		item.TrendingTime = formatLocalTimeMS(item.TrendingTimestampMS)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Server) resolveVisualHistoryRunScope(row visualHistoryPositionRow) visualHistoryRunScope {
	runtimeMeta := extractVisualHistoryPositionRunMeta(row.OpenRowJSON, row.CloseRowJSON)
	if strings.TrimSpace(runtimeMeta.RunID) == "" {
		return visualHistoryRunScope{}
	}
	scope := visualHistoryRunScope{
		RunID:         strings.TrimSpace(runtimeMeta.RunID),
		SingletonUUID: strings.TrimSpace(runtimeMeta.RunID),
		SingletonID:   runtimeMeta.SingletonID,
	}
	if scope.SingletonID > 0 {
		return scope
	}
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return scope
	}
	var singletonID int64
	err := s.cfg.HistoryStore.DB.QueryRow(
		`SELECT id FROM singleton WHERE uuid = ? LIMIT 1;`,
		scope.SingletonUUID,
	).Scan(&singletonID)
	if err == nil {
		scope.SingletonID = singletonID
	}
	return scope
}

func loadVisualHistorySignalSourceIDs(db *sql.DB, singletonID int64, exchange, symbol string, startMS, endMS int64) (map[int64]struct{}, error) {
	if db == nil || singletonID <= 0 {
		return nil, nil
	}
	rows, err := db.Query(
		`SELECT s.id
		   FROM signals s
		   JOIN exchanges e ON e.id = s.exchange_id
		   JOIN symbols m ON m.id = s.symbol_id
		  WHERE s.singleton_id = ?
		    AND e.name = ?
		    AND m.symbol = ?
		    AND s.event_at_ms >= ?
		    AND s.event_at_ms <= ?;`,
		singletonID,
		exchange,
		symbol,
		startMS,
		endMS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int64]struct{})
	for rows.Next() {
		var sourceID int64
		if scanErr := rows.Scan(&sourceID); scanErr != nil {
			return nil, scanErr
		}
		out[sourceID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type visualHistoryBackTestOpenMeta struct {
	SingletonID int64  `json:"singleton_id,omitempty"`
	RunID       string `json:"run_id"`
}

type visualHistoryBackTestCloseMeta struct {
	SingletonID int64  `json:"singleton_id,omitempty"`
	RunID       string `json:"run_id"`
}

type visualHistoryPositionRunMeta struct {
	RunID       string
	SingletonID int64
}

func extractVisualHistoryPositionRunMeta(openRowJSON, closeRowJSON string) visualHistoryPositionRunMeta {
	if meta := parseVisualHistoryPositionRunMetaFromRaw(openRowJSON, true); meta.RunID != "" || meta.SingletonID > 0 {
		return meta
	}
	return parseVisualHistoryPositionRunMetaFromRaw(closeRowJSON, false)
}

func extractVisualHistoryBackTestRunID(openRowJSON, closeRowJSON string) string {
	return extractVisualHistoryPositionRunMeta(openRowJSON, closeRowJSON).RunID
}

func formatVisualHistoryRunOptionLabel(runID string, singletonID int64) string {
	runID = strings.TrimSpace(runID)
	if singletonID <= 0 {
		return runID
	}
	return fmt.Sprintf("%d:%s", singletonID, runID)
}

func parseVisualHistoryPositionRunMetaFromRaw(raw string, isOpen bool) visualHistoryPositionRunMeta {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return visualHistoryPositionRunMeta{}
	}
	runtimeMeta := models.ExtractPositionRuntimeMeta(raw)
	if runtimeMeta.RunID != "" || runtimeMeta.SingletonID > 0 {
		return visualHistoryPositionRunMeta{
			RunID:       strings.TrimSpace(runtimeMeta.RunID),
			SingletonID: runtimeMeta.SingletonID,
		}
	}
	if isOpen {
		var meta visualHistoryBackTestOpenMeta
		if err := json.Unmarshal([]byte(raw), &meta); err == nil {
			if strings.TrimSpace(meta.RunID) != "" || meta.SingletonID > 0 {
				return visualHistoryPositionRunMeta{
					RunID:       strings.TrimSpace(meta.RunID),
					SingletonID: meta.SingletonID,
				}
			}
		}
	} else {
		var meta visualHistoryBackTestCloseMeta
		if err := json.Unmarshal([]byte(raw), &meta); err == nil {
			if strings.TrimSpace(meta.RunID) != "" || meta.SingletonID > 0 {
				return visualHistoryPositionRunMeta{
					RunID:       strings.TrimSpace(meta.RunID),
					SingletonID: meta.SingletonID,
				}
			}
		}
	}
	env, ok := models.ParsePositionRowEnvelope(raw)
	if !ok || len(env.ExchangeRaw) == 0 {
		return visualHistoryPositionRunMeta{}
	}
	if isOpen {
		var meta visualHistoryBackTestOpenMeta
		if err := json.Unmarshal(env.ExchangeRaw, &meta); err == nil {
			return visualHistoryPositionRunMeta{
				RunID:       strings.TrimSpace(meta.RunID),
				SingletonID: meta.SingletonID,
			}
		}
		return visualHistoryPositionRunMeta{}
	}
	var meta visualHistoryBackTestCloseMeta
	if err := json.Unmarshal(env.ExchangeRaw, &meta); err == nil {
		return visualHistoryPositionRunMeta{
			RunID:       strings.TrimSpace(meta.RunID),
			SingletonID: meta.SingletonID,
		}
	}
	return visualHistoryPositionRunMeta{}
}

func sortVisualHistoryEvents(items []visualHistoryEventEntry) []visualHistoryEventEntry {
	if len(items) == 0 {
		return items
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].EventAt != items[j].EventAt {
			return items[i].EventAt < items[j].EventAt
		}
		return items[i].ID < items[j].ID
	})
	return items
}

func classifySignalEventType(changeStatus int, changedFields string, meta signalEventMeta) string {
	if meta.Action == models.SignalActionOpenTrendGuardRejected {
		return "OPEN_REJECTED_TREND_GUARD"
	}
	if meta.Action == models.SignalActionOpenRiskRejected {
		return "OPEN_REJECTED_RISK"
	}
	if meta.Action == 4 {
		return "ARMED"
	}
	if hasSignalChangedField(changedFields, "profit_protect_stage") || meta.Action == 32 {
		if meta.ProfitProtectStage >= models.SignalProfitProtectStagePartial || meta.Action == 32 {
			return "R_PROTECT_4R"
		}
		if meta.ProfitProtectStage >= models.SignalProfitProtectStageBreakEven {
			return "R_PROTECT_2R"
		}
	}
	hasTP := hasSignalChangedField(changedFields, "tp", "take_profit", "take_profit_price")
	hasSL := hasSignalChangedField(changedFields, "sl", "stop_loss", "stop_loss_price")
	switch {
	case hasTP && hasSL:
		return "TRAILING_TP_SL"
	case hasTP:
		return "TRAILING_TP"
	case hasSL:
		return "TRAILING_STOP"
	case hasSignalChangedField(changedFields, "trending_timestamp"):
		return "TREND_DETECTED"
	case hasSignalChangedField(changedFields, "high_side"):
		return "HIGH_SIDE_CHANGED"
	case hasSignalChangedField(changedFields, "mid_side"):
		return "MID_SIDE_CHANGED"
	case changeStatus < 0:
		return "SIGNAL_CLOSED"
	default:
		return "SIGNAL"
	}
}

func hasSignalChangedField(changedFields string, candidates ...string) bool {
	targets := make(map[string]struct{}, len(candidates))
	for _, item := range candidates {
		key := strings.ToLower(strings.TrimSpace(item))
		if key == "" {
			continue
		}
		targets[key] = struct{}{}
	}
	if len(targets) == 0 {
		return false
	}
	parts := strings.FieldsFunc(changedFields, func(r rune) bool {
		switch r {
		case ',', ';', '|':
			return true
		default:
			return r <= ' '
		}
	})
	for _, part := range parts {
		key := strings.ToLower(strings.TrimSpace(part))
		if key == "" {
			continue
		}
		if _, ok := targets[key]; ok {
			return true
		}
	}
	return false
}

func classifySignalLevel(changeStatus int) string {
	if changeStatus < 0 {
		return "warning"
	}
	return "info"
}

func classifyExecutionLevel(resultStatus, failReason string) string {
	status := strings.ToLower(strings.TrimSpace(resultStatus))
	reason := strings.ToLower(strings.TrimSpace(failReason))
	if strings.Contains(status, "success") || strings.Contains(status, "filled") || strings.Contains(status, "done") {
		return "success"
	}
	if strings.Contains(status, "fail") || strings.Contains(status, "error") || strings.Contains(status, "reject") || strings.Contains(status, "cancel") {
		return "error"
	}
	if reason != "" {
		return "warning"
	}
	return "info"
}

func (s *Server) loadVisualHistoryCandleCache(positionID int64, timeframeFilter string, row visualHistoryPositionRow) (map[string]visualHistoryTimeframeBlock, error) {
	_ = positionID
	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		return nil, fmt.Errorf("history store unavailable")
	}
	db := s.cfg.HistoryStore.DB
	out := make(map[string]visualHistoryTimeframeBlock)
	specs, err := buildVisualHistoryFrameSpecs(row, timeframeFilter)
	if err != nil {
		return nil, err
	}
	for _, spec := range specs {
		symbol := normalizeVisualHistorySymbol(row.Symbol, row.InstID)
		if strings.TrimSpace(symbol) == "" {
			symbol = strings.TrimSpace(row.Symbol)
		}
		rows, err := db.Query(
			`SELECT o.ts, o.open, o.high, o.low, o.close, o.volume
			   FROM ohlcv o
			   JOIN exchanges e ON e.id = o.exchange_id
			   JOIN symbols m ON m.id = o.symbol_id
			  WHERE e.name = ?
			    AND (UPPER(m.symbol) = UPPER(?) OR REPLACE(UPPER(m.symbol), '/', '') = REPLACE(UPPER(?), '/', ''))
			    AND o.timeframe = ? AND o.ts >= ? AND o.ts <= ?
			  ORDER BY o.ts ASC;`,
			row.Exchange,
			symbol,
			symbol,
			spec.Timeframe,
			spec.ExpectedStartMS,
			spec.ExpectedEndMS,
		)
		if err != nil {
			return nil, err
		}
		block := visualHistoryTimeframeBlock{
			LookbackBars:    spec.LookbackBars,
			ExpectedStartMS: spec.ExpectedStartMS,
			ExpectedEndMS:   spec.ExpectedEndMS,
		}
		for rows.Next() {
			var candle visualHistoryCandle
			if scanErr := rows.Scan(&candle.TS, &candle.Open, &candle.High, &candle.Low, &candle.Close, &candle.Volume); scanErr != nil {
				_ = rows.Close()
				return nil, scanErr
			}
			block.Candles = append(block.Candles, candle)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		block.Count = len(block.Candles)
		out[spec.Timeframe] = block
	}
	return out, nil
}

func (s *Server) buildVisualHistoryIntegrity(row visualHistoryPositionRow, blocks map[string]visualHistoryTimeframeBlock) (visualHistoryIntegrityResponse, error) {
	resp := visualHistoryIntegrityResponse{}
	resp.PositionID = row.ID
	resp.PositionKey = row.PositionKey
	resp.IsOpen = row.IsOpen

	if s == nil || s.cfg.HistoryStore == nil || s.cfg.HistoryStore.DB == nil {
		resp.Events.MainDBAvailable = false
		resp.Check.OK = false
		return resp, nil
	}
	db := s.cfg.HistoryStore.DB
	resp.Events.MainDBAvailable = true

	rangeStart, rangeEnd := positionEventRange(row)
	meta := visualHistoryStrategyMetaFromRow(row)
	scope := s.resolveVisualHistoryRunScope(row)
	signalEvents, err := querySignalEventsFromStore(db, row, meta, rangeStart, rangeEnd, scope, visualHistorySignalEventFilter{})
	if err != nil {
		return resp, err
	}
	resp.Events.Signals = len(signalEvents)
	executionEvents, err := queryExecutionEventsFromStore(db, row, rangeStart, rangeEnd, scope)
	if err != nil {
		return resp, err
	}
	resp.Events.Orders = len(executionEvents)
	specs, err := buildVisualHistoryFrameSpecs(row, "")
	if err != nil {
		return resp, err
	}
	for _, spec := range specs {
		block := blocks[spec.Timeframe]
		normalizedCandles := normalizeVisualHistoryCandles(block.Candles, spec.ExpectedStartMS, spec.ExpectedEndMS)
		cacheItem := visualHistoryCandleCacheTF{
			Timeframe: spec.Timeframe,
			Rows:      int64(len(normalizedCandles)),
		}
		if len(normalizedCandles) > 0 {
			cacheItem.FirstTS = normalizedCandles[0].TS
			cacheItem.LastTS = normalizedCandles[len(normalizedCandles)-1].TS
		}
		resp.Candles.Timeframes = append(resp.Candles.Timeframes, cacheItem)
		resp.Candles.TotalRows += cacheItem.Rows
		tfIntegrity := buildVisualHistoryTimeframeIntegrity(spec, block.Candles)
		resp.Timeframes = append(resp.Timeframes, tfIntegrity)
		if !tfIntegrity.Complete || !tfIntegrity.Continuous {
			resp.Summary.IncompleteTimeframes++
		}
		for _, gap := range tfIntegrity.Gaps {
			resp.Summary.MissingBars += gap.Bars
			if gap.Kind == "internal_gap" {
				resp.Summary.Discontinuities++
			}
		}
	}
	resp.Check.HasEvents = resp.Events.Signals+resp.Events.Orders > 0
	resp.Check.HasCandles = resp.Candles.TotalRows > 0
	resp.Check.HasDiscontinuity = resp.Summary.Discontinuities > 0
	resp.Check.OK = resp.Check.HasCandles && resp.Summary.IncompleteTimeframes == 0
	return resp, nil
}

func findVisualHistoryIntegrityTimeframe(items []visualHistoryIntegrityTimeframe, timeframe string) (visualHistoryIntegrityTimeframe, bool) {
	target := strings.TrimSpace(strings.ToLower(timeframe))
	for _, item := range items {
		if strings.TrimSpace(strings.ToLower(item.Timeframe)) == target {
			return item, true
		}
	}
	return visualHistoryIntegrityTimeframe{}, false
}

func buildVisualHistoryFrameSpecs(row visualHistoryPositionRow, timeframeFilter string) ([]visualHistoryFrameSpec, error) {
	requested := []string(nil)
	if strings.TrimSpace(timeframeFilter) != "" {
		requested = []string{timeframeFilter}
	}
	timeframes := normalizeRequestedTimeframes(requested, row)
	if len(timeframes) == 0 {
		return nil, nil
	}
	anchorMS := visualHistoryTrendAnchorMS(row)
	if anchorMS <= 0 {
		return nil, fmt.Errorf("position trend anchor unavailable")
	}
	nowMS := time.Now().UnixMilli()
	endMS := row.CloseTimeMS
	endFromLastCandle := false
	if endMS <= 0 {
		endMS = row.LastCandleTSMS
		endFromLastCandle = endMS > 0
	}
	if endMS <= 0 {
		endMS = row.UpdatedAtMS
	}
	if endMS <= 0 {
		endMS = nowMS
	}
	if endMS < anchorMS {
		endMS = anchorMS
	}
	specs := make([]visualHistoryFrameSpec, 0, len(timeframes))
	for _, timeframe := range timeframes {
		dur, err := timeframeToDuration(timeframe)
		if err != nil {
			return nil, err
		}
		durMS := dur.Milliseconds()
		if durMS <= 0 {
			continue
		}
		lookbackBars := indicatorLookbackBars(row, timeframe)
		if lookbackBars < 1 {
			lookbackBars = 1
		}
		expectedStartMS := alignVisualHistoryBarStart(anchorMS, durMS) - durMS*int64(lookbackBars)
		if expectedStartMS < 0 {
			expectedStartMS = 0
		}
		expectedEndMS := alignVisualHistoryClosedBarStart(endMS, durMS)
		if !row.IsOpen {
			// 已平仓仓位按 close_time 所在K线对齐，避免丢失最后一根触发平仓的K线。
			expectedEndMS = alignVisualHistoryBarStart(endMS, durMS)
		}
		if row.IsOpen && endFromLastCandle {
			// 若未来重新启用持仓视图，last_candle_ts_ms 表示“已收盘K线的起点”，不应再无条件回退一根。
			byLastCandle := alignVisualHistoryBarStart(endMS, durMS)
			byNowClosed := alignVisualHistoryClosedBarStart(nowMS, durMS)
			switch {
			case byLastCandle <= 0:
				expectedEndMS = byNowClosed
			case byNowClosed <= 0:
				expectedEndMS = byLastCandle
			case byLastCandle < byNowClosed:
				expectedEndMS = byLastCandle
			default:
				expectedEndMS = byNowClosed
			}
		}
		if expectedEndMS < expectedStartMS {
			expectedEndMS = expectedStartMS
		}
		expectedBars := int64(0)
		if expectedEndMS >= expectedStartMS {
			expectedBars = ((expectedEndMS - expectedStartMS) / durMS) + 1
		}
		specs = append(specs, visualHistoryFrameSpec{
			Timeframe:       timeframe,
			LookbackBars:    lookbackBars,
			ExpectedStartMS: expectedStartMS,
			ExpectedEndMS:   expectedEndMS,
			ExpectedBars:    expectedBars,
		})
	}
	return specs, nil
}

func buildVisualHistoryTimeframeIntegrity(spec visualHistoryFrameSpec, candles []visualHistoryCandle) visualHistoryIntegrityTimeframe {
	tf := visualHistoryIntegrityTimeframe{
		Timeframe:       spec.Timeframe,
		ExpectedStartMS: spec.ExpectedStartMS,
		ExpectedEndMS:   spec.ExpectedEndMS,
		ExpectedBars:    spec.ExpectedBars,
	}
	normalized := normalizeVisualHistoryCandles(candles, spec.ExpectedStartMS, spec.ExpectedEndMS)
	tf.ActualBars = int64(len(normalized))
	dur, err := timeframeToDuration(spec.Timeframe)
	if err != nil {
		return tf
	}
	durMS := dur.Milliseconds()
	if durMS <= 0 {
		return tf
	}
	if len(normalized) == 0 {
		tf.Complete = false
		tf.Continuous = false
		if spec.ExpectedBars > 0 {
			tf.Gaps = append(tf.Gaps, visualHistoryIntegrityGap{
				Kind:    "coverage_start",
				StartTS: spec.ExpectedStartMS,
				EndTS:   spec.ExpectedEndMS,
				Bars:    spec.ExpectedBars,
			})
		}
		return tf
	}

	first := normalized[0].TS
	if first > spec.ExpectedStartMS {
		tf.Gaps = append(tf.Gaps, buildVisualHistoryGap("coverage_start", spec.ExpectedStartMS, first-durMS, durMS))
	}
	for i := 1; i < len(normalized); i++ {
		prev := normalized[i-1].TS
		curr := normalized[i].TS
		if curr-prev <= durMS {
			continue
		}
		tf.Gaps = append(tf.Gaps, buildVisualHistoryGap("internal_gap", prev+durMS, curr-durMS, durMS))
	}
	last := normalized[len(normalized)-1].TS
	if last < spec.ExpectedEndMS {
		tf.Gaps = append(tf.Gaps, buildVisualHistoryGap("coverage_end", last+durMS, spec.ExpectedEndMS, durMS))
	}
	tf.Continuous = true
	for _, gap := range tf.Gaps {
		if gap.Kind == "internal_gap" {
			tf.Continuous = false
			break
		}
	}
	tf.Complete = len(tf.Gaps) == 0 && tf.ActualBars >= spec.ExpectedBars
	return tf
}

func normalizeVisualHistoryCandles(candles []visualHistoryCandle, startMS, endMS int64) []visualHistoryCandle {
	if len(candles) == 0 {
		return nil
	}
	bucket := make(map[int64]visualHistoryCandle, len(candles))
	for _, candle := range candles {
		if candle.TS < startMS || candle.TS > endMS {
			continue
		}
		bucket[candle.TS] = candle
	}
	if len(bucket) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(bucket))
	for ts := range bucket {
		keys = append(keys, ts)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	out := make([]visualHistoryCandle, 0, len(keys))
	for _, ts := range keys {
		out = append(out, bucket[ts])
	}
	return out
}

func buildVisualHistoryGap(kind string, startTS, endTS, durMS int64) visualHistoryIntegrityGap {
	if endTS < startTS || durMS <= 0 {
		return visualHistoryIntegrityGap{Kind: kind, StartTS: startTS, EndTS: endTS}
	}
	return visualHistoryIntegrityGap{
		Kind:    kind,
		StartTS: startTS,
		EndTS:   endTS,
		Bars:    ((endTS - startTS) / durMS) + 1,
	}
}

func alignVisualHistoryBarStart(ms, durMS int64) int64 {
	if durMS <= 0 || ms <= 0 {
		return 0
	}
	return (ms / durMS) * durMS
}

func alignVisualHistoryClosedBarStart(ms, durMS int64) int64 {
	if durMS <= 0 || ms <= 0 {
		return 0
	}
	currentBarStart := (ms / durMS) * durMS
	if currentBarStart < durMS {
		return 0
	}
	return currentBarStart - durMS
}

func isNoSuchTableErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "no such table")
}

func parseHistoryDateWindow(raw string, loc *time.Location) (string, int64, int64, error) {
	if loc == nil {
		loc = time.Local
	}
	now := time.Now().In(loc)
	nowEndMS := now.UnixMilli() + 1
	var start time.Time
	if strings.TrimSpace(raw) == "" {
		start = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
		startMS := start.UnixMilli()
		if nowEndMS <= startMS {
			nowEndMS = startMS + 1
		}
		return start.Format("2006-01-02"), startMS, nowEndMS, nil
	}
	parsed, err := time.ParseInLocation("2006-01-02", strings.TrimSpace(raw), loc)
	if err != nil {
		return "", 0, 0, fmt.Errorf("invalid date format, expected YYYY-MM-DD")
	}
	start = time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, loc)
	startMS := start.UnixMilli()
	if nowEndMS <= startMS {
		nowEndMS = startMS + 1
	}
	return start.Format("2006-01-02"), startMS, nowEndMS, nil
}

func parseQueryInt64(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func parsePageLimit(raw string) int {
	value := visualHistoryDefaultPageLimit
	if parsed, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
		value = parsed
	}
	if value <= 0 {
		value = visualHistoryDefaultPageLimit
	}
	if value > visualHistoryMaxPageLimit {
		value = visualHistoryMaxPageLimit
	}
	return value
}

func parseVisualHistoryEventLimit(raw string, defaultValue int) int {
	value := defaultValue
	if parsed, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
		value = parsed
	}
	if value <= 0 {
		value = defaultValue
	}
	if value > visualHistoryMaxEventLimit {
		value = visualHistoryMaxEventLimit
	}
	return value
}

func trimVisualHistoryEvents(events []visualHistoryEventEntry, limit int) ([]visualHistoryEventEntry, bool) {
	if limit <= 0 || len(events) <= limit {
		return events, false
	}
	// 保留时间序列末尾的最新事件，避免前端一次渲染过量事件导致卡顿或崩溃。
	start := len(events) - limit
	out := make([]visualHistoryEventEntry, limit)
	copy(out, events[start:])
	return out, true
}

func parseVisualHistoryQueryLocation(rawOffset string) (*time.Location, error) {
	rawOffset = strings.TrimSpace(rawOffset)
	if rawOffset == "" {
		return time.Local, nil
	}
	offsetMinutes, err := strconv.Atoi(rawOffset)
	if err != nil {
		return nil, fmt.Errorf("invalid tz_offset_min")
	}
	if offsetMinutes < -1440 || offsetMinutes > 1440 {
		return nil, fmt.Errorf("invalid tz_offset_min")
	}
	// JavaScript Date#getTimezoneOffset: UTC - Local（分钟）
	offsetSeconds := -offsetMinutes * 60
	return time.FixedZone("", offsetSeconds), nil
}

func normalizeRequestedTimeframes(requested []string, row visualHistoryPositionRow) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(requested)+4)
	appendTF := func(raw string) {
		tf := strings.ToLower(strings.TrimSpace(raw))
		if tf == "" {
			return
		}
		if _, err := timeframeToDuration(tf); err != nil {
			return
		}
		if _, ok := seen[tf]; ok {
			return
		}
		seen[tf] = struct{}{}
		out = append(out, tf)
	}
	for _, tf := range requested {
		appendTF(tf)
	}
	if len(out) == 0 {
		meta := models.ExtractStrategyContextMeta(row.OpenRowJSON)
		for _, tf := range normalizeMetaTimeframes(meta.StrategyTimeframes) {
			appendTF(tf)
		}
	}
	if len(out) == 0 {
		appendTF("15m")
	}
	return out
}

func timeframeToDuration(raw string) (time.Duration, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return 0, fmt.Errorf("empty timeframe")
	}
	if len(raw) < 2 {
		return 0, fmt.Errorf("invalid timeframe")
	}
	numberPart := raw[:len(raw)-1]
	unit := raw[len(raw)-1]
	value, err := strconv.Atoi(numberPart)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid timeframe")
	}
	switch unit {
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	case 'w':
		return time.Duration(value) * 7 * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported timeframe")
	}
}

func indicatorLookbackBars(row visualHistoryPositionRow, timeframe string) int {
	meta := models.ExtractStrategyContextMeta(row.OpenRowJSON)
	indicators := normalizeMetaIndicators(meta.StrategyIndicators)
	if len(indicators) == 0 {
		indicators = meta.StrategyIndicators
	}
	if len(indicators) == 0 {
		return 1
	}
	timeframe = strings.ToLower(strings.TrimSpace(timeframe))
	maxPeriod := 1
	for tf, names := range indicators {
		if strings.ToLower(strings.TrimSpace(tf)) != timeframe {
			continue
		}
		for _, name := range names {
			if period := extractIndicatorPeriod(name); period > maxPeriod {
				maxPeriod = period
			}
		}
	}
	if maxPeriod == 1 {
		for _, names := range indicators {
			for _, name := range names {
				if period := extractIndicatorPeriod(name); period > maxPeriod {
					maxPeriod = period
				}
			}
		}
	}
	return maxPeriod
}

func extractIndicatorPeriod(raw string) int {
	match := indicatorPeriodRegexp.FindStringSubmatch(strings.TrimSpace(raw))
	if len(match) < 2 {
		return 0
	}
	value, err := strconv.Atoi(match[1])
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

func parseNumericText(raw string) float64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0
	}
	return value
}

func normalizeMetaTimeframes(values []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(values))
	for _, item := range values {
		tf := strings.ToLower(strings.TrimSpace(item))
		if tf == "" {
			continue
		}
		if _, err := timeframeToDuration(tf); err != nil {
			continue
		}
		if _, ok := seen[tf]; ok {
			continue
		}
		seen[tf] = struct{}{}
		out = append(out, tf)
	}
	sort.Slice(out, func(i, j int) bool {
		left, _ := timeframeToDuration(out[i])
		right, _ := timeframeToDuration(out[j])
		if left == right {
			return out[i] < out[j]
		}
		return left < right
	})
	return out
}

func normalizeMetaIndicators(input map[string][]string) map[string][]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string][]string)
	for rawTF, rawIndicators := range input {
		tf := strings.ToLower(strings.TrimSpace(rawTF))
		if tf == "" {
			continue
		}
		seen := make(map[string]struct{})
		normalized := make([]string, 0, len(rawIndicators))
		for _, indicator := range rawIndicators {
			name := strings.TrimSpace(indicator)
			if name == "" {
				continue
			}
			key := strings.ToLower(name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			normalized = append(normalized, name)
		}
		if len(normalized) == 0 {
			continue
		}
		out[tf] = normalized
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func positionEventRange(row visualHistoryPositionRow) (int64, int64) {
	start := visualHistoryTrendAnchorMS(row)
	if start <= 0 {
		start = row.OpenTimeMS
	}
	if start <= 0 {
		start = row.CreatedAtMS
	}
	end := row.CloseTimeMS
	if end <= 0 {
		end = row.LastCandleTSMS
	}
	if end <= 0 {
		end = row.UpdatedAtMS
	}
	if end <= 0 {
		end = time.Now().UnixMilli()
	}
	if start <= 0 {
		start = end
	}
	if end < start {
		end = start
	}
	return start, end
}

func visualHistoryTrendAnchorMS(row visualHistoryPositionRow) int64 {
	meta := models.ExtractStrategyContextMeta(row.OpenRowJSON)
	if meta.TrendingTimestamp > 0 {
		return int64(meta.TrendingTimestamp)
	}
	if row.OpenTimeMS > 0 {
		return row.OpenTimeMS
	}
	if row.CreatedAtMS > 0 {
		return row.CreatedAtMS
	}
	return row.CloseTimeMS
}

func normalizeVisualHistorySymbol(symbol, instID string) string {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if out, ok := splitVisualHistoryCompactSymbol(symbol); ok {
		return out
	}
	if strings.Contains(symbol, "/") {
		return symbol
	}
	if strings.Contains(symbol, "-") {
		parts := strings.Split(symbol, "-")
		if len(parts) >= 2 && strings.TrimSpace(parts[0]) != "" && strings.TrimSpace(parts[1]) != "" {
			return strings.TrimSpace(parts[0]) + "/" + strings.TrimSpace(parts[1])
		}
	}
	if fromInst := visualHistorySymbolFromInstID(instID); fromInst != "" {
		return fromInst
	}
	return symbol
}

func visualHistorySymbolFromInstID(instID string) string {
	instID = strings.ToUpper(strings.TrimSpace(instID))
	if instID == "" {
		return ""
	}
	if strings.HasSuffix(instID, "-SWAP") {
		parts := strings.Split(instID, "-")
		if len(parts) >= 3 && parts[0] != "" && parts[1] != "" {
			return parts[0] + "/" + parts[1]
		}
	}
	parts := strings.Split(instID, "-")
	if len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
		return parts[0] + "/" + parts[1]
	}
	if out, ok := splitVisualHistoryCompactSymbol(instID); ok {
		return out
	}
	return strings.ReplaceAll(instID, "-", "/")
}

func splitVisualHistoryCompactSymbol(raw string) (string, bool) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" || strings.Contains(raw, "/") || strings.Contains(raw, "-") {
		return "", false
	}
	for _, quote := range []string{"USDT", "USDC", "BUSD", "USD", "BTC", "ETH"} {
		if !strings.HasSuffix(raw, quote) || len(raw) <= len(quote) {
			continue
		}
		base := strings.TrimSpace(raw[:len(raw)-len(quote)])
		if base == "" {
			continue
		}
		return base + "/" + quote, true
	}
	return "", false
}

func buildVisualHistoryPositionKey(exchange, instID, posSide, mgnMode string, openTimeMS int64) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	instID = strings.ToUpper(strings.TrimSpace(instID))
	posSide = strings.ToLower(strings.TrimSpace(posSide))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || posSide == "" || mgnMode == "" || openTimeMS <= 0 {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%d", exchange, instID, posSide, mgnMode, openTimeMS)
}

func parseVisualHistoryPositionKey(raw string) (string, string, string, string, int64, error) {
	parts := strings.Split(strings.TrimSpace(raw), "|")
	if len(parts) != 5 {
		return "", "", "", "", 0, fmt.Errorf("invalid position key")
	}
	openTimeMS, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil || openTimeMS <= 0 {
		return "", "", "", "", 0, fmt.Errorf("invalid position key")
	}
	return parts[0], parts[1], parts[2], parts[3], openTimeMS, nil
}

func visualHistoryPositionSortTime(row visualHistoryPositionRow) int64 {
	if row.IsOpen {
		return maxVisualHistoryInt64(row.UpdatedAtMS, row.RevisionMS, row.OpenTimeMS)
	}
	return maxVisualHistoryInt64(row.CloseTimeMS, row.UpdatedAtMS, row.OpenTimeMS)
}

func visualHistoryDisplayState(row visualHistoryPositionRow) string {
	if row.IsOpen {
		return "open"
	}
	return "closed"
}

func visualHistoryPollIntervalMS(timeframes []string) int64 {
	best := int64(0)
	for _, timeframe := range timeframes {
		dur, err := timeframeToDuration(timeframe)
		if err != nil {
			continue
		}
		ms := dur.Milliseconds()
		if ms <= 0 {
			continue
		}
		if best == 0 || ms < best {
			best = ms
		}
	}
	return best
}

func maxVisualHistoryInt64(values ...int64) int64 {
	var out int64
	for _, value := range values {
		if value > out {
			out = value
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}
