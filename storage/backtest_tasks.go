package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

type BacktestTaskCreateSpec struct {
	Exchange           string
	Symbol             string
	DisplaySymbol      string
	ChartTimeframe     string
	TradeTimeframes    []string
	RangeStartMS       int64
	RangeEndMS         int64
	PriceLow           float64
	PriceHigh          float64
	SelectionDirection string
	Source             string
	HistoryBars        int
}

func (s *SQLite) CountBacktestTasksByStatus(statuses ...string) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("nil db")
	}
	statuses = compactStatuses(statuses)
	if len(statuses) == 0 {
		return 0, nil
	}
	args := make([]any, 0, len(statuses))
	placeholders := make([]string, 0, len(statuses))
	for _, status := range statuses {
		placeholders = append(placeholders, "?")
		args = append(args, status)
	}
	query := fmt.Sprintf(`SELECT COUNT(*) FROM backtest_tasks WHERE status IN (%s);`, strings.Join(placeholders, ","))
	var count int
	if err := s.DB.QueryRow(query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *SQLite) CreateBacktestTask(spec BacktestTaskCreateSpec) (models.BacktestTask, error) {
	if s == nil || s.DB == nil {
		return models.BacktestTask{}, fmt.Errorf("nil db")
	}
	spec.Exchange = strings.ToLower(strings.TrimSpace(spec.Exchange))
	spec.Symbol = strings.TrimSpace(spec.Symbol)
	spec.DisplaySymbol = strings.TrimSpace(spec.DisplaySymbol)
	spec.ChartTimeframe = strings.TrimSpace(spec.ChartTimeframe)
	spec.SelectionDirection = strings.TrimSpace(strings.ToLower(spec.SelectionDirection))
	spec.Source = strings.TrimSpace(spec.Source)
	if spec.Exchange == "" || spec.Symbol == "" || spec.ChartTimeframe == "" {
		return models.BacktestTask{}, fmt.Errorf("exchange, symbol and chart timeframe are required")
	}
	if spec.RangeStartMS <= 0 || spec.RangeEndMS <= 0 || spec.RangeEndMS <= spec.RangeStartMS {
		return models.BacktestTask{}, fmt.Errorf("invalid backtest task range")
	}
	if spec.HistoryBars <= 0 {
		spec.HistoryBars = 500
	}
	nowMS := time.Now().UnixMilli()
	timeframesJSON, err := json.Marshal(normalizeTaskTimeframes(spec.TradeTimeframes))
	if err != nil {
		return models.BacktestTask{}, err
	}
	result, err := s.DB.Exec(
		`INSERT INTO backtest_tasks (
			status, exchange, symbol, display_symbol, chart_timeframe, trade_timeframes,
			range_start_ms, range_end_ms, price_low, price_high, selection_direction,
			source, history_bars, created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		models.BacktestTaskStatusPending,
		spec.Exchange,
		spec.Symbol,
		spec.DisplaySymbol,
		spec.ChartTimeframe,
		string(timeframesJSON),
		spec.RangeStartMS,
		spec.RangeEndMS,
		spec.PriceLow,
		spec.PriceHigh,
		spec.SelectionDirection,
		spec.Source,
		spec.HistoryBars,
		nowMS,
		nowMS,
	)
	if err != nil {
		return models.BacktestTask{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return models.BacktestTask{}, err
	}
	task, found, err := s.GetBacktestTask(id)
	if err != nil {
		return models.BacktestTask{}, err
	}
	if !found {
		return models.BacktestTask{}, fmt.Errorf("backtest task not found after insert")
	}
	return task, nil
}

func (s *SQLite) GetBacktestTask(id int64) (models.BacktestTask, bool, error) {
	if s == nil || s.DB == nil {
		return models.BacktestTask{}, false, fmt.Errorf("nil db")
	}
	if id <= 0 {
		return models.BacktestTask{}, false, fmt.Errorf("invalid backtest task id")
	}
	row := s.DB.QueryRow(
		`SELECT id, status, exchange, symbol, display_symbol, chart_timeframe, trade_timeframes,
		        range_start_ms, range_end_ms, price_low, price_high, selection_direction, source,
		        history_bars, singleton_id, singleton_uuid, pid, error_message,
		        created_at_ms, started_at_ms, finished_at_ms, updated_at_ms
		   FROM backtest_tasks
		  WHERE id = ?;`,
		id,
	)
	task, found, err := scanBacktestTask(row)
	return task, found, err
}

func (s *SQLite) ListBacktestTasksByTimeRange(startMS, endMS int64) ([]models.BacktestTask, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	if endMS <= startMS {
		return nil, fmt.Errorf("invalid backtest task time range")
	}
	rows, err := s.DB.Query(
		`SELECT id, status, exchange, symbol, display_symbol, chart_timeframe, trade_timeframes,
		        range_start_ms, range_end_ms, price_low, price_high, selection_direction, source,
		        history_bars, singleton_id, singleton_uuid, pid, error_message,
		        created_at_ms, started_at_ms, finished_at_ms, updated_at_ms
		   FROM backtest_tasks
		  WHERE created_at_ms >= ? AND created_at_ms < ?
		  ORDER BY created_at_ms DESC, id DESC;`,
		startMS,
		endMS,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.BacktestTask, 0, 32)
	for rows.Next() {
		task, err := scanBacktestTaskRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListBacktestTasksByStatuses(statuses ...string) ([]models.BacktestTask, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	statuses = compactStatuses(statuses)
	if len(statuses) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(statuses))
	placeholders := make([]string, 0, len(statuses))
	for _, status := range statuses {
		placeholders = append(placeholders, "?")
		args = append(args, status)
	}
	query := fmt.Sprintf(
		`SELECT id, status, exchange, symbol, display_symbol, chart_timeframe, trade_timeframes,
		        range_start_ms, range_end_ms, price_low, price_high, selection_direction, source,
		        history_bars, singleton_id, singleton_uuid, pid, error_message,
		        created_at_ms, started_at_ms, finished_at_ms, updated_at_ms
		   FROM backtest_tasks
		  WHERE status IN (%s)
		  ORDER BY created_at_ms DESC, id DESC;`,
		strings.Join(placeholders, ","),
	)
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.BacktestTask, 0, 16)
	for rows.Next() {
		task, scanErr := scanBacktestTaskRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, task)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) MarkBacktestTaskSpawned(id int64, pid int) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if id <= 0 {
		return fmt.Errorf("invalid backtest task id")
	}
	nowMS := time.Now().UnixMilli()
	_, err := s.DB.Exec(
		`UPDATE backtest_tasks
		    SET pid = ?,
		        updated_at_ms = ?
		  WHERE id = ?;`,
		pid,
		nowMS,
		id,
	)
	return err
}

func (s *SQLite) MarkBacktestTaskRunning(id, singletonID int64, singletonUUID string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if id <= 0 {
		return fmt.Errorf("invalid backtest task id")
	}
	nowMS := time.Now().UnixMilli()
	_, err := s.DB.Exec(
		`UPDATE backtest_tasks
		    SET status = ?,
		        singleton_id = ?,
		        singleton_uuid = ?,
		        started_at_ms = CASE WHEN started_at_ms > 0 THEN started_at_ms ELSE ? END,
		        updated_at_ms = ?
		  WHERE id = ?;`,
		models.BacktestTaskStatusRunning,
		singletonID,
		strings.TrimSpace(singletonUUID),
		nowMS,
		nowMS,
		id,
	)
	return err
}

func (s *SQLite) MarkBacktestTaskFinished(id int64, status, errorMessage string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if id <= 0 {
		return fmt.Errorf("invalid backtest task id")
	}
	status = strings.TrimSpace(status)
	if status == "" {
		status = models.BacktestTaskStatusFailed
	}
	nowMS := time.Now().UnixMilli()
	_, err := s.DB.Exec(
		`UPDATE backtest_tasks
		    SET status = ?,
		        error_message = ?,
		        finished_at_ms = CASE WHEN finished_at_ms > 0 THEN finished_at_ms ELSE ? END,
		        updated_at_ms = ?
		  WHERE id = ?;`,
		status,
		strings.TrimSpace(errorMessage),
		nowMS,
		nowMS,
		id,
	)
	return err
}

func (s *SQLite) ResetBacktestTaskForRetry(id int64, source string, historyBars int) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if id <= 0 {
		return fmt.Errorf("invalid backtest task id")
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return fmt.Errorf("retry source required")
	}
	if historyBars <= 0 {
		historyBars = 500
	}
	nowMS := time.Now().UnixMilli()
	_, err := s.DB.Exec(
		`UPDATE backtest_tasks
		    SET status = ?,
		        source = ?,
		        history_bars = ?,
		        singleton_id = 0,
		        singleton_uuid = '',
		        pid = 0,
		        error_message = '',
		        created_at_ms = ?,
		        started_at_ms = 0,
		        finished_at_ms = 0,
		        updated_at_ms = ?
		  WHERE id = ?;`,
		models.BacktestTaskStatusPending,
		source,
		historyBars,
		nowMS,
		nowMS,
		id,
	)
	return err
}

func compactStatuses(statuses []string) []string {
	if len(statuses) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(statuses))
	out := make([]string, 0, len(statuses))
	for _, item := range statuses {
		status := strings.TrimSpace(item)
		if status == "" {
			continue
		}
		if _, exists := seen[status]; exists {
			continue
		}
		seen[status] = struct{}{}
		out = append(out, status)
	}
	return out
}

func normalizeTaskTimeframes(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, item := range values {
		timeframe := strings.TrimSpace(item)
		if timeframe == "" {
			continue
		}
		if _, exists := seen[timeframe]; exists {
			continue
		}
		seen[timeframe] = struct{}{}
		out = append(out, timeframe)
	}
	return out
}

func scanBacktestTask(row interface {
	Scan(dest ...any) error
}) (models.BacktestTask, bool, error) {
	task, err := scanBacktestTaskWithErr(row)
	if err == sql.ErrNoRows {
		return models.BacktestTask{}, false, nil
	}
	if err != nil {
		return models.BacktestTask{}, false, err
	}
	return task, true, nil
}

func scanBacktestTaskRows(rows *sql.Rows) (models.BacktestTask, error) {
	return scanBacktestTaskWithErr(rows)
}

func scanBacktestTaskWithErr(scanner interface {
	Scan(dest ...any) error
}) (models.BacktestTask, error) {
	var (
		task           models.BacktestTask
		timeframesJSON string
	)
	if err := scanner.Scan(
		&task.ID,
		&task.Status,
		&task.Exchange,
		&task.Symbol,
		&task.DisplaySymbol,
		&task.ChartTimeframe,
		&timeframesJSON,
		&task.RangeStartMS,
		&task.RangeEndMS,
		&task.PriceLow,
		&task.PriceHigh,
		&task.SelectionDirection,
		&task.Source,
		&task.HistoryBars,
		&task.SingletonID,
		&task.SingletonUUID,
		&task.PID,
		&task.ErrorMessage,
		&task.CreatedAtMS,
		&task.StartedAtMS,
		&task.FinishedAtMS,
		&task.UpdatedAtMS,
	); err != nil {
		return models.BacktestTask{}, err
	}
	if strings.TrimSpace(timeframesJSON) != "" {
		var values []string
		if err := json.Unmarshal([]byte(timeframesJSON), &values); err != nil {
			return models.BacktestTask{}, err
		}
		task.TradeTimeframes = normalizeTaskTimeframes(values)
	}
	return task, nil
}
