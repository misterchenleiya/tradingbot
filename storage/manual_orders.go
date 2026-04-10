package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
)

func (s *SQLite) CreateManualOrder(record models.ManualOrder) (models.ManualOrder, error) {
	if s == nil || s.DB == nil {
		return models.ManualOrder{}, fmt.Errorf("nil db")
	}
	record = normalizeManualOrderRecord(record)
	if record.Exchange == "" || record.Symbol == "" {
		return models.ManualOrder{}, fmt.Errorf("manual order requires exchange and symbol")
	}
	if record.OrderType == "" {
		record.OrderType = models.OrderTypeLimit
	}
	if record.Status == "" {
		record.Status = models.ManualOrderStatusPending
	}
	nowMS := time.Now().UnixMilli()
	if record.CreatedAtMS <= 0 {
		record.CreatedAtMS = nowMS
	}
	if record.UpdatedAtMS <= 0 {
		record.UpdatedAtMS = record.CreatedAtMS
	}
	timeframesJSON, err := json.Marshal(normalizeManualOrderTimeframes(record.StrategyTimeframes))
	if err != nil {
		return models.ManualOrder{}, err
	}
	result, err := s.DB.Exec(
		`INSERT INTO manual_orders (
			mode, exchange, symbol, inst_id, timeframe, position_side, margin_mode, order_type, status,
			strategy_name, strategy_version, strategy_timeframes, combo_key, group_id, leverage_multiplier,
			amount, size, price, take_profit_price, stop_loss_price, client_order_id, exchange_order_id,
			exchange_algo_order_id, position_id, entry_price, filled_size, error_message, decision_json,
			metadata_json, created_at_ms, submitted_at_ms, filled_at_ms, last_checked_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		record.Mode,
		record.Exchange,
		record.Symbol,
		record.InstID,
		record.Timeframe,
		record.PositionSide,
		record.MarginMode,
		record.OrderType,
		record.Status,
		record.StrategyName,
		record.StrategyVersion,
		string(timeframesJSON),
		record.ComboKey,
		record.GroupID,
		record.LeverageMultiplier,
		record.Amount,
		record.Size,
		record.Price,
		record.TakeProfitPrice,
		record.StopLossPrice,
		record.ClientOrderID,
		record.ExchangeOrderID,
		record.ExchangeAlgoOrderID,
		record.PositionID,
		record.EntryPrice,
		record.FilledSize,
		record.ErrorMessage,
		record.DecisionJSON,
		record.MetadataJSON,
		record.CreatedAtMS,
		record.SubmittedAtMS,
		record.FilledAtMS,
		record.LastCheckedAtMS,
		record.UpdatedAtMS,
	)
	if err != nil {
		return models.ManualOrder{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return models.ManualOrder{}, err
	}
	item, found, err := s.GetManualOrder(id)
	if err != nil {
		return models.ManualOrder{}, err
	}
	if !found {
		return models.ManualOrder{}, fmt.Errorf("manual order not found after insert")
	}
	return item, nil
}

func (s *SQLite) UpdateManualOrder(record models.ManualOrder) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if record.ID <= 0 {
		return fmt.Errorf("invalid manual order id")
	}
	record = normalizeManualOrderRecord(record)
	if record.UpdatedAtMS <= 0 {
		record.UpdatedAtMS = time.Now().UnixMilli()
	}
	timeframesJSON, err := json.Marshal(normalizeManualOrderTimeframes(record.StrategyTimeframes))
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(
		`UPDATE manual_orders
		    SET mode = ?,
		        exchange = ?,
		        symbol = ?,
		        inst_id = ?,
		        timeframe = ?,
		        position_side = ?,
		        margin_mode = ?,
		        order_type = ?,
		        status = ?,
		        strategy_name = ?,
		        strategy_version = ?,
		        strategy_timeframes = ?,
		        combo_key = ?,
		        group_id = ?,
		        leverage_multiplier = ?,
		        amount = ?,
		        size = ?,
		        price = ?,
		        take_profit_price = ?,
		        stop_loss_price = ?,
		        client_order_id = ?,
		        exchange_order_id = ?,
		        exchange_algo_order_id = ?,
		        position_id = ?,
		        entry_price = ?,
		        filled_size = ?,
		        error_message = ?,
		        decision_json = ?,
		        metadata_json = ?,
		        created_at_ms = ?,
		        submitted_at_ms = ?,
		        filled_at_ms = ?,
		        last_checked_at_ms = ?,
		        updated_at_ms = ?
		  WHERE id = ?;`,
		record.Mode,
		record.Exchange,
		record.Symbol,
		record.InstID,
		record.Timeframe,
		record.PositionSide,
		record.MarginMode,
		record.OrderType,
		record.Status,
		record.StrategyName,
		record.StrategyVersion,
		string(timeframesJSON),
		record.ComboKey,
		record.GroupID,
		record.LeverageMultiplier,
		record.Amount,
		record.Size,
		record.Price,
		record.TakeProfitPrice,
		record.StopLossPrice,
		record.ClientOrderID,
		record.ExchangeOrderID,
		record.ExchangeAlgoOrderID,
		record.PositionID,
		record.EntryPrice,
		record.FilledSize,
		record.ErrorMessage,
		record.DecisionJSON,
		record.MetadataJSON,
		record.CreatedAtMS,
		record.SubmittedAtMS,
		record.FilledAtMS,
		record.LastCheckedAtMS,
		record.UpdatedAtMS,
		record.ID,
	)
	return err
}

func (s *SQLite) GetManualOrder(id int64) (models.ManualOrder, bool, error) {
	if s == nil || s.DB == nil {
		return models.ManualOrder{}, false, fmt.Errorf("nil db")
	}
	if id <= 0 {
		return models.ManualOrder{}, false, fmt.Errorf("invalid manual order id")
	}
	row := s.DB.QueryRow(
		`SELECT id, mode, exchange, symbol, inst_id, timeframe, position_side, margin_mode, order_type, status,
		        strategy_name, strategy_version, strategy_timeframes, combo_key, group_id, leverage_multiplier,
		        amount, size, price, take_profit_price, stop_loss_price, client_order_id, exchange_order_id,
		        exchange_algo_order_id, position_id, entry_price, filled_size, error_message, decision_json,
		        metadata_json, created_at_ms, submitted_at_ms, filled_at_ms, last_checked_at_ms, updated_at_ms
		   FROM manual_orders
		  WHERE id = ?;`,
		id,
	)
	item, found, err := scanManualOrder(row)
	return item, found, err
}

func (s *SQLite) ListManualOrders(mode, exchange string, statuses ...string) ([]models.ManualOrder, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	exchange = strings.TrimSpace(strings.ToLower(exchange))
	statuses = compactStatuses(statuses)
	query := `SELECT id, mode, exchange, symbol, inst_id, timeframe, position_side, margin_mode, order_type, status,
	                 strategy_name, strategy_version, strategy_timeframes, combo_key, group_id, leverage_multiplier,
	                 amount, size, price, take_profit_price, stop_loss_price, client_order_id, exchange_order_id,
	                 exchange_algo_order_id, position_id, entry_price, filled_size, error_message, decision_json,
	                 metadata_json, created_at_ms, submitted_at_ms, filled_at_ms, last_checked_at_ms, updated_at_ms
	            FROM manual_orders`
	args := make([]any, 0, 2+len(statuses))
	clauses := make([]string, 0, 3)
	if mode != "" {
		clauses = append(clauses, "mode = ?")
		args = append(args, mode)
	}
	if exchange != "" {
		clauses = append(clauses, "exchange = ?")
		args = append(args, exchange)
	}
	if len(statuses) > 0 {
		parts := make([]string, 0, len(statuses))
		for _, status := range statuses {
			parts = append(parts, "?")
			args = append(args, status)
		}
		clauses = append(clauses, "status IN ("+strings.Join(parts, ",")+")")
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at_ms DESC, id DESC"
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]models.ManualOrder, 0, 16)
	for rows.Next() {
		item, scanErr := scanManualOrderRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListPendingManualOrders(mode string) ([]models.ManualOrder, error) {
	return s.ListManualOrders(mode, "", models.ManualOrderStatusPending)
}

func (s *SQLite) FindLatestManualOrderByClientOrderID(clientOrderID string) (models.ManualOrder, bool, error) {
	if s == nil || s.DB == nil {
		return models.ManualOrder{}, false, fmt.Errorf("nil db")
	}
	clientOrderID = strings.TrimSpace(clientOrderID)
	if clientOrderID == "" {
		return models.ManualOrder{}, false, fmt.Errorf("empty client order id")
	}
	row := s.DB.QueryRow(
		`SELECT id, mode, exchange, symbol, inst_id, timeframe, position_side, margin_mode, order_type, status,
		        strategy_name, strategy_version, strategy_timeframes, combo_key, group_id, leverage_multiplier,
		        amount, size, price, take_profit_price, stop_loss_price, client_order_id, exchange_order_id,
		        exchange_algo_order_id, position_id, entry_price, filled_size, error_message, decision_json,
		        metadata_json, created_at_ms, submitted_at_ms, filled_at_ms, last_checked_at_ms, updated_at_ms
		   FROM manual_orders
		  WHERE client_order_id = ?
		  ORDER BY created_at_ms DESC, id DESC
		  LIMIT 1;`,
		clientOrderID,
	)
	item, found, err := scanManualOrder(row)
	return item, found, err
}

func (s *SQLite) FindLatestExecutionOrderByClientOrderID(mode, clientOrderID string) (models.ExecutionOrderRecord, bool, error) {
	if s == nil || s.DB == nil {
		return models.ExecutionOrderRecord{}, false, fmt.Errorf("nil db")
	}
	clientOrderID = strings.TrimSpace(clientOrderID)
	mode = strings.TrimSpace(mode)
	if clientOrderID == "" {
		return models.ExecutionOrderRecord{}, false, fmt.Errorf("empty client order id")
	}
	query := `SELECT attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
	                 position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
	                 stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
	                 fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
	                 has_side_effect, step_results_json, request_json, response_json, started_at_ms,
	                 finished_at_ms, duration_ms, created_at_ms, updated_at_ms
	            FROM orders
	           WHERE client_order_id = ?`
	args := []any{clientOrderID}
	if mode != "" {
		query += " AND mode = ?"
		args = append(args, mode)
	}
	query += " ORDER BY created_at_ms DESC, id DESC LIMIT 1"
	row := s.DB.QueryRow(query, args...)
	item, found, err := scanExecutionOrderRecord(row)
	return item, found, err
}

func normalizeManualOrderRecord(record models.ManualOrder) models.ManualOrder {
	record.Mode = strings.TrimSpace(record.Mode)
	if record.Mode == "" {
		record.Mode = "live"
	}
	record.Exchange = strings.ToLower(strings.TrimSpace(record.Exchange))
	record.Symbol = strings.TrimSpace(record.Symbol)
	record.InstID = strings.ToUpper(strings.TrimSpace(record.InstID))
	record.Timeframe = strings.TrimSpace(record.Timeframe)
	record.PositionSide = strings.TrimSpace(strings.ToLower(record.PositionSide))
	record.MarginMode = strings.TrimSpace(strings.ToLower(record.MarginMode))
	if record.MarginMode == "" {
		record.MarginMode = models.MarginModeIsolated
	}
	record.OrderType = strings.TrimSpace(strings.ToLower(record.OrderType))
	record.Status = strings.TrimSpace(strings.ToLower(record.Status))
	record.StrategyName = strings.TrimSpace(record.StrategyName)
	if record.StrategyName == "" {
		record.StrategyName = models.ManualOrderOwnerManual
	}
	record.StrategyVersion = strings.TrimSpace(record.StrategyVersion)
	record.StrategyTimeframes = normalizeManualOrderTimeframes(record.StrategyTimeframes)
	record.ComboKey = strings.TrimSpace(record.ComboKey)
	record.GroupID = strings.TrimSpace(record.GroupID)
	record.ClientOrderID = strings.TrimSpace(record.ClientOrderID)
	record.ExchangeOrderID = strings.TrimSpace(record.ExchangeOrderID)
	record.ExchangeAlgoOrderID = strings.TrimSpace(record.ExchangeAlgoOrderID)
	record.ErrorMessage = strings.TrimSpace(record.ErrorMessage)
	record.DecisionJSON = strings.TrimSpace(record.DecisionJSON)
	record.MetadataJSON = strings.TrimSpace(record.MetadataJSON)
	return record
}

func normalizeManualOrderTimeframes(values []string) []string {
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

func scanManualOrder(row interface{ Scan(dest ...any) error }) (models.ManualOrder, bool, error) {
	item, err := scanManualOrderWithErr(row)
	if err == sql.ErrNoRows {
		return models.ManualOrder{}, false, nil
	}
	if err != nil {
		return models.ManualOrder{}, false, err
	}
	return item, true, nil
}

func scanManualOrderRows(rows *sql.Rows) (models.ManualOrder, error) {
	return scanManualOrderWithErr(rows)
}

func scanManualOrderWithErr(scanner interface{ Scan(dest ...any) error }) (models.ManualOrder, error) {
	var (
		item           models.ManualOrder
		timeframesJSON string
	)
	if err := scanner.Scan(
		&item.ID,
		&item.Mode,
		&item.Exchange,
		&item.Symbol,
		&item.InstID,
		&item.Timeframe,
		&item.PositionSide,
		&item.MarginMode,
		&item.OrderType,
		&item.Status,
		&item.StrategyName,
		&item.StrategyVersion,
		&timeframesJSON,
		&item.ComboKey,
		&item.GroupID,
		&item.LeverageMultiplier,
		&item.Amount,
		&item.Size,
		&item.Price,
		&item.TakeProfitPrice,
		&item.StopLossPrice,
		&item.ClientOrderID,
		&item.ExchangeOrderID,
		&item.ExchangeAlgoOrderID,
		&item.PositionID,
		&item.EntryPrice,
		&item.FilledSize,
		&item.ErrorMessage,
		&item.DecisionJSON,
		&item.MetadataJSON,
		&item.CreatedAtMS,
		&item.SubmittedAtMS,
		&item.FilledAtMS,
		&item.LastCheckedAtMS,
		&item.UpdatedAtMS,
	); err != nil {
		return models.ManualOrder{}, err
	}
	if strings.TrimSpace(timeframesJSON) != "" {
		var values []string
		if err := json.Unmarshal([]byte(timeframesJSON), &values); err != nil {
			return models.ManualOrder{}, err
		}
		item.StrategyTimeframes = normalizeManualOrderTimeframes(values)
	}
	return normalizeManualOrderRecord(item), nil
}

func scanExecutionOrderRecord(row interface{ Scan(dest ...any) error }) (models.ExecutionOrderRecord, bool, error) {
	var (
		item          models.ExecutionOrderRecord
		hasSideEffect int
	)
	if err := row.Scan(
		&item.AttemptID,
		&item.SingletonUUID,
		&item.Mode,
		&item.Source,
		&item.Exchange,
		&item.Symbol,
		&item.InstID,
		&item.Action,
		&item.OrderType,
		&item.PositionSide,
		&item.MarginMode,
		&item.Size,
		&item.LeverageMultiplier,
		&item.Price,
		&item.TakeProfitPrice,
		&item.StopLossPrice,
		&item.ClientOrderID,
		&item.Strategy,
		&item.ResultStatus,
		&item.FailSource,
		&item.FailStage,
		&item.FailReason,
		&item.ExchangeCode,
		&item.ExchangeMessage,
		&item.ExchangeOrderID,
		&item.ExchangeAlgoOrderID,
		&hasSideEffect,
		&item.StepResultsJSON,
		&item.RequestJSON,
		&item.ResponseJSON,
		&item.StartedAtMS,
		&item.FinishedAtMS,
		&item.DurationMS,
		&item.CreatedAtMS,
		&item.UpdatedAtMS,
	); err != nil {
		if err == sql.ErrNoRows {
			return models.ExecutionOrderRecord{}, false, nil
		}
		return models.ExecutionOrderRecord{}, false, err
	}
	item.HasSideEffect = hasSideEffect == 1
	return item, true, nil
}
