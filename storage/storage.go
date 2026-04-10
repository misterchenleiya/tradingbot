package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/misterchenleiya/tradingbot/common/floatcmp"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"go.uber.org/zap"
)

type SQLite struct {
	Path    string
	DB      *sql.DB
	logger  *zap.Logger
	started atomic.Bool
}

type Config struct {
	Path   string
	Logger *zap.Logger
}

type OHLCVTimeframeRange struct {
	Exchange  string
	Symbol    string
	Timeframe string
	Bars      int64
	StartTS   int64
	EndTS     int64
}

type OHLCVBoundRecord struct {
	Exchange            string
	Symbol              string
	EarliestAvailableTS int64
}

const riskHistoryStateSyncPending = "sync_pending"

const (
	sqliteBusyTimeoutMS = 10000
	sqliteSyncMode      = "NORMAL"
	sqliteWALMode       = "WAL"
)

func NewSQLite(cfg Config) *SQLite {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &SQLite{
		Path:   cfg.Path,
		logger: logger,
	}
}

func OpenSQLite(path string) (*SQLite, error) {
	store := NewSQLite(Config{Path: path})
	if err := store.Start(context.Background()); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *SQLite) Start(_ context.Context) (err error) {
	if s == nil {
		return errors.New("nil store")
	}
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	fields := []zap.Field{
		zap.String("db", s.Path),
	}
	logger.Info("storage start", fields...)
	defer func() {
		logger.Info("storage started")
	}()
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("storage already started")
	}
	if strings.TrimSpace(s.Path) == "" {
		s.started.Store(false)
		return errors.New("empty db path")
	}
	db, err := sql.Open("sqlite3", sqliteDSN(s.Path))
	if err != nil {
		s.started.Store(false)
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := configureSQLiteRuntime(db, s.Path); err != nil {
		closeErr := db.Close()
		s.started.Store(false)
		if closeErr != nil {
			return fmt.Errorf("configure sqlite runtime failed: %v; close db failed: %w", err, closeErr)
		}
		return fmt.Errorf("configure sqlite runtime failed: %w", err)
	}
	s.DB = db
	return nil
}

func (s *SQLite) Close() (err error) {
	if s == nil {
		return nil
	}
	logger := s.logger
	if logger == nil {
		logger = zap.NewNop()
	}
	logger.Info("storage close")
	defer func() {
		logger.Info("storage closed")
	}()
	if s.DB == nil {
		s.started.Store(false)
		return nil
	}
	if closeErr := s.DB.Close(); closeErr != nil {
		err = closeErr
	}
	s.DB = nil
	s.started.Store(false)
	return err
}

func (s *SQLite) SetLogger(logger *zap.Logger) {
	if s == nil {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	s.logger = logger
}

func sqliteDSN(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return path
	}
	params := []string{
		"_foreign_keys=on",
		fmt.Sprintf("_busy_timeout=%d", sqliteBusyTimeoutMS),
		fmt.Sprintf("_synchronous=%s", sqliteSyncMode),
	}
	if sqliteSupportsWAL(path) {
		params = append(params, fmt.Sprintf("_journal_mode=%s", sqliteWALMode))
	}
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	return path + sep + strings.Join(params, "&")
}

func sqliteSupportsWAL(path string) bool {
	lower := strings.ToLower(strings.TrimSpace(path))
	if lower == "" {
		return false
	}
	if lower == ":memory:" || strings.HasPrefix(lower, "file::memory:") {
		return false
	}
	return !strings.Contains(lower, "mode=memory")
}

func configureSQLiteRuntime(db *sql.DB, path string) error {
	if db == nil {
		return errors.New("nil sqlite db")
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		return fmt.Errorf("enable foreign keys failed: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d;", sqliteBusyTimeoutMS)); err != nil {
		return fmt.Errorf("set busy_timeout failed: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf("PRAGMA synchronous = %s;", sqliteSyncMode)); err != nil {
		return fmt.Errorf("set synchronous failed: %w", err)
	}
	if !sqliteSupportsWAL(path) {
		return nil
	}
	var journalMode string
	if err := db.QueryRow(fmt.Sprintf("PRAGMA journal_mode = %s;", sqliteWALMode)).Scan(&journalMode); err != nil {
		return fmt.Errorf("set journal_mode failed: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(journalMode), strings.ToLower(sqliteWALMode)) &&
		!strings.EqualFold(strings.TrimSpace(journalMode), sqliteWALMode) {
		return fmt.Errorf("unexpected journal_mode %q", journalMode)
	}
	return nil
}

func (s *SQLite) EnsureSchema() error {
	schema := []string{
		`CREATE TABLE IF NOT EXISTS exchanges (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			api_key TEXT NOT NULL DEFAULT '',
			ohlcv_limit INTEGER NOT NULL DEFAULT 300,
			rate_limit INTEGER NOT NULL DEFAULT 100,
			volume_filter REAL NOT NULL DEFAULT 1,
			market_proxy TEXT NOT NULL DEFAULT '',
			trade_proxy TEXT NOT NULL DEFAULT '',
			timeframes TEXT NOT NULL DEFAULT '["3m","15m","1h"]',
			active INTEGER NOT NULL DEFAULT 1,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS symbols (
			id INTEGER PRIMARY KEY,
			exchange_id INTEGER NOT NULL,
			symbol TEXT NOT NULL,
			base TEXT,
			quote TEXT,
			type TEXT,
			timeframes TEXT NOT NULL DEFAULT '',
			active INTEGER NOT NULL DEFAULT 1,
			dynamic INTEGER NOT NULL DEFAULT 0,
			UNIQUE(exchange_id, symbol),
			FOREIGN KEY(exchange_id) REFERENCES exchanges(id)
		);`,
		`CREATE TABLE IF NOT EXISTS ohlcv (
			exchange_id INTEGER NOT NULL,
			symbol_id INTEGER NOT NULL,
			timeframe TEXT NOT NULL,
			ts INTEGER NOT NULL,
			open REAL NOT NULL,
			high REAL NOT NULL,
			low REAL NOT NULL,
			close REAL NOT NULL,
			volume REAL NOT NULL,
			PRIMARY KEY (exchange_id, symbol_id, timeframe, ts),
			FOREIGN KEY(exchange_id) REFERENCES exchanges(id),
			FOREIGN KEY(symbol_id) REFERENCES symbols(id)
		);`,
		`CREATE TABLE IF NOT EXISTS ohlcv_bounds (
			exchange_id INTEGER NOT NULL,
			symbol_id INTEGER NOT NULL,
			earliest_available_ts INTEGER,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY (exchange_id, symbol_id),
			FOREIGN KEY(exchange_id) REFERENCES exchanges(id),
			FOREIGN KEY(symbol_id) REFERENCES symbols(id)
		);`,
		`CREATE TABLE IF NOT EXISTS positions (
			singleton_id INTEGER NOT NULL DEFAULT 0,
			mode TEXT NOT NULL DEFAULT 'live',
			exchange TEXT NOT NULL,
			symbol TEXT NOT NULL,
			inst_id TEXT NOT NULL,
			pos TEXT NOT NULL DEFAULT '0',
			pos_side TEXT NOT NULL,
			mgn_mode TEXT NOT NULL,
			margin TEXT NOT NULL DEFAULT '0',
			lever TEXT NOT NULL DEFAULT '0',
			avg_px TEXT NOT NULL DEFAULT '0',
			upl TEXT NOT NULL DEFAULT '0',
			upl_ratio TEXT NOT NULL DEFAULT '0',
			notional_usd TEXT NOT NULL DEFAULT '0',
			mark_px TEXT NOT NULL DEFAULT '0',
			liq_px TEXT NOT NULL DEFAULT '0',
			tp_trigger_px TEXT NOT NULL DEFAULT '0',
			sl_trigger_px TEXT NOT NULL DEFAULT '0',
			open_time_ms INTEGER NOT NULL DEFAULT 0,
			update_time_ms INTEGER NOT NULL DEFAULT 0,
			max_floating_loss_amount REAL NOT NULL DEFAULT 0,
			max_floating_profit_amount REAL NOT NULL DEFAULT 0,
			row_json TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (mode, exchange, inst_id, pos_side, mgn_mode)
		);`,
		`CREATE TABLE IF NOT EXISTS history_positions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			singleton_id INTEGER NOT NULL DEFAULT 0,
			mode TEXT NOT NULL DEFAULT 'live',
			exchange TEXT NOT NULL,
			symbol TEXT NOT NULL,
			inst_id TEXT NOT NULL,
			pos TEXT NOT NULL DEFAULT '0',
			pos_side TEXT NOT NULL,
			mgn_mode TEXT NOT NULL,
			margin TEXT NOT NULL DEFAULT '0',
			lever TEXT NOT NULL DEFAULT '0',
			avg_px TEXT NOT NULL DEFAULT '0',
			notional_usd TEXT NOT NULL DEFAULT '0',
			mark_px TEXT NOT NULL DEFAULT '0',
			liq_px TEXT NOT NULL DEFAULT '0',
			tp_trigger_px TEXT NOT NULL DEFAULT '0',
			sl_trigger_px TEXT NOT NULL DEFAULT '0',
			open_time_ms INTEGER NOT NULL DEFAULT 0,
			open_update_time_ms INTEGER NOT NULL DEFAULT 0,
			max_floating_loss_amount REAL NOT NULL DEFAULT 0,
			max_floating_profit_amount REAL NOT NULL DEFAULT 0,
			open_row_json TEXT NOT NULL DEFAULT '',
			close_avg_px TEXT NOT NULL DEFAULT '0',
			realized_pnl TEXT NOT NULL DEFAULT '0',
			pnl_ratio TEXT NOT NULL DEFAULT '0',
			fee TEXT NOT NULL DEFAULT '0',
			funding_fee TEXT NOT NULL DEFAULT '0',
			close_time_ms INTEGER NOT NULL DEFAULT 0,
			state TEXT NOT NULL DEFAULT '',
			close_row_json TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0,
			UNIQUE(mode, exchange, inst_id, pos_side, mgn_mode, open_time_ms, close_time_ms)
		);`,
		`CREATE TABLE IF NOT EXISTS orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			attempt_id TEXT NOT NULL UNIQUE,
			singleton_uuid TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			exchange TEXT NOT NULL,
			symbol TEXT NOT NULL,
			inst_id TEXT NOT NULL DEFAULT '',
			action TEXT NOT NULL,
			order_type TEXT NOT NULL DEFAULT '',
			position_side TEXT NOT NULL DEFAULT '',
			margin_mode TEXT NOT NULL DEFAULT '',
			size REAL NOT NULL DEFAULT 0,
			leverage_multiplier REAL NOT NULL DEFAULT 0,
			price REAL NOT NULL DEFAULT 0,
			take_profit_price REAL NOT NULL DEFAULT 0,
			stop_loss_price REAL NOT NULL DEFAULT 0,
			client_order_id TEXT NOT NULL DEFAULT '',
			strategy TEXT NOT NULL DEFAULT '',
			result_status TEXT NOT NULL,
			fail_source TEXT NOT NULL DEFAULT '',
			fail_stage TEXT NOT NULL DEFAULT '',
			fail_reason TEXT NOT NULL DEFAULT '',
			exchange_code TEXT NOT NULL DEFAULT '',
			exchange_message TEXT NOT NULL DEFAULT '',
			exchange_order_id TEXT NOT NULL DEFAULT '',
			exchange_algo_order_id TEXT NOT NULL DEFAULT '',
			has_side_effect INTEGER NOT NULL DEFAULT 0,
			step_results_json TEXT NOT NULL DEFAULT '',
			request_json TEXT NOT NULL DEFAULT '',
			response_json TEXT NOT NULL DEFAULT '',
			started_at_ms INTEGER NOT NULL DEFAULT 0,
			finished_at_ms INTEGER NOT NULL DEFAULT 0,
			duration_ms INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS manual_orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			mode TEXT NOT NULL DEFAULT 'live',
			exchange TEXT NOT NULL,
			symbol TEXT NOT NULL,
			inst_id TEXT NOT NULL DEFAULT '',
			timeframe TEXT NOT NULL DEFAULT '',
			position_side TEXT NOT NULL DEFAULT '',
			margin_mode TEXT NOT NULL DEFAULT 'isolated',
			order_type TEXT NOT NULL DEFAULT 'limit',
			status TEXT NOT NULL DEFAULT 'pending',
			strategy_name TEXT NOT NULL DEFAULT 'manual',
			strategy_version TEXT NOT NULL DEFAULT '',
			strategy_timeframes TEXT NOT NULL DEFAULT '[]',
			combo_key TEXT NOT NULL DEFAULT '',
			group_id TEXT NOT NULL DEFAULT '',
			leverage_multiplier REAL NOT NULL DEFAULT 0,
			amount REAL NOT NULL DEFAULT 0,
			size REAL NOT NULL DEFAULT 0,
			price REAL NOT NULL DEFAULT 0,
			take_profit_price REAL NOT NULL DEFAULT 0,
			stop_loss_price REAL NOT NULL DEFAULT 0,
			client_order_id TEXT NOT NULL DEFAULT '',
			exchange_order_id TEXT NOT NULL DEFAULT '',
			exchange_algo_order_id TEXT NOT NULL DEFAULT '',
			position_id INTEGER NOT NULL DEFAULT 0,
			entry_price REAL NOT NULL DEFAULT 0,
			filled_size REAL NOT NULL DEFAULT 0,
			error_message TEXT NOT NULL DEFAULT '',
			decision_json TEXT NOT NULL DEFAULT '',
			metadata_json TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			submitted_at_ms INTEGER NOT NULL DEFAULT 0,
			filled_at_ms INTEGER NOT NULL DEFAULT 0,
			last_checked_at_ms INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS risk_decisions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			singleton_id INTEGER NOT NULL DEFAULT 0,
			singleton_uuid TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT '',
			exchange TEXT NOT NULL DEFAULT '',
			symbol TEXT NOT NULL DEFAULT '',
			timeframe TEXT NOT NULL DEFAULT '',
			strategy TEXT NOT NULL DEFAULT '',
			combo_key TEXT NOT NULL DEFAULT '',
			group_id TEXT NOT NULL DEFAULT '',
			signal_action INTEGER NOT NULL DEFAULT 0,
			high_side INTEGER NOT NULL DEFAULT 0,
			decision_action TEXT NOT NULL DEFAULT '',
			result_status TEXT NOT NULL DEFAULT '',
			reject_reason TEXT NOT NULL DEFAULT '',
			event_at_ms INTEGER NOT NULL DEFAULT 0,
			trigger_timestamp_ms INTEGER NOT NULL DEFAULT 0,
			trending_timestamp_ms INTEGER NOT NULL DEFAULT 0,
			signal_json TEXT NOT NULL DEFAULT '{}',
			decision_json TEXT NOT NULL DEFAULT '{}',
			created_at_ms INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS singleton (
			id INTEGER PRIMARY KEY,
			uuid TEXT NOT NULL UNIQUE,
			version TEXT NOT NULL,
			mode TEXT NOT NULL,
			source TEXT,
			status TEXT NOT NULL,
			created INTEGER NOT NULL,
			updated INTEGER NOT NULL,
			closed INTEGER,
			heartbeat INTEGER,
			lease_expires INTEGER,
			start_time TEXT,
			end_time TEXT,
			runtime TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS config (
			name TEXT PRIMARY KEY,
			value TEXT NOT NULL,
			common TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS signals (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			singleton_id INTEGER,
			mode TEXT NOT NULL DEFAULT 'live',
			exchange_id INTEGER NOT NULL,
			symbol_id INTEGER NOT NULL,
			timeframe TEXT NOT NULL,
			strategy TEXT NOT NULL,
			strategy_version TEXT NOT NULL DEFAULT '',
			change_status INTEGER NOT NULL,
			changed_fields TEXT NOT NULL DEFAULT '',
			signal_json TEXT NOT NULL DEFAULT '',
			event_at_ms INTEGER NOT NULL DEFAULT 0,
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			FOREIGN KEY(singleton_id) REFERENCES singleton(id),
			FOREIGN KEY(exchange_id) REFERENCES exchanges(id),
			FOREIGN KEY(symbol_id) REFERENCES symbols(id)
		);`,
		`CREATE TABLE IF NOT EXISTS backtest_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			status TEXT NOT NULL DEFAULT 'pending',
			exchange TEXT NOT NULL,
			symbol TEXT NOT NULL,
			display_symbol TEXT NOT NULL DEFAULT '',
			chart_timeframe TEXT NOT NULL DEFAULT '',
			trade_timeframes TEXT NOT NULL DEFAULT '[]',
			range_start_ms INTEGER NOT NULL DEFAULT 0,
			range_end_ms INTEGER NOT NULL DEFAULT 0,
			price_low REAL NOT NULL DEFAULT 0,
			price_high REAL NOT NULL DEFAULT 0,
			selection_direction TEXT NOT NULL DEFAULT '',
			source TEXT NOT NULL DEFAULT '',
			history_bars INTEGER NOT NULL DEFAULT 500,
			singleton_id INTEGER NOT NULL DEFAULT 0,
			singleton_uuid TEXT NOT NULL DEFAULT '',
			pid INTEGER NOT NULL DEFAULT 0,
			error_message TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL DEFAULT 0,
			started_at_ms INTEGER NOT NULL DEFAULT 0,
			finished_at_ms INTEGER NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0
		);`,
		`CREATE TABLE IF NOT EXISTS risk_account_states (
			mode TEXT NOT NULL DEFAULT 'live',
			exchange TEXT NOT NULL,
			trade_date TEXT NOT NULL,
			total_usdt REAL NOT NULL DEFAULT 0,
			funding_usdt REAL NOT NULL DEFAULT 0,
			trading_usdt REAL NOT NULL DEFAULT 0,
			per_trade_usdt REAL NOT NULL DEFAULT 0,
			daily_loss_limit_usdt REAL NOT NULL DEFAULT 0,
			daily_realized_usdt REAL NOT NULL DEFAULT 0,
			daily_closed_profit_usdt REAL NOT NULL DEFAULT 0,
			updated_at_ms INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (mode, exchange)
		);`,
		`CREATE TABLE IF NOT EXISTS risk_symbol_cooldowns (
				mode TEXT NOT NULL DEFAULT 'live',
				exchange TEXT NOT NULL,
				symbol TEXT NOT NULL,
				consecutive_stop_loss INTEGER NOT NULL DEFAULT 0,
				window_start_at_ms INTEGER NOT NULL DEFAULT 0,
			last_stop_loss_at_ms INTEGER NOT NULL DEFAULT 0,
			cooldown_until_ms INTEGER NOT NULL DEFAULT 0,
				last_processed_close_ms INTEGER NOT NULL DEFAULT 0,
				updated_at_ms INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (mode, exchange, symbol)
			);`,
		`CREATE TABLE IF NOT EXISTS risk_trend_groups (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				mode TEXT NOT NULL DEFAULT 'live',
				strategy TEXT NOT NULL,
				primary_timeframe TEXT NOT NULL,
				side TEXT NOT NULL,
				anchor_trending_timestamp_ms INTEGER NOT NULL,
				state TEXT NOT NULL,
				lock_stage TEXT NOT NULL,
				selected_candidate_key TEXT NOT NULL DEFAULT '',
				incumbent_leader_key TEXT NOT NULL DEFAULT '',
				incumbent_leader_score REAL NOT NULL DEFAULT 0,
				incumbent_leader_closed_at_ms INTEGER NOT NULL DEFAULT 0,
				first_entry_at_ms INTEGER NOT NULL DEFAULT 0,
				last_entry_at_ms INTEGER NOT NULL DEFAULT 0,
				entry_count INTEGER NOT NULL DEFAULT 0,
				finish_reason TEXT NOT NULL DEFAULT '',
				created_at_ms INTEGER NOT NULL,
				updated_at_ms INTEGER NOT NULL,
				finished_at_ms INTEGER NOT NULL DEFAULT 0,
				UNIQUE(mode, strategy, primary_timeframe, side, anchor_trending_timestamp_ms)
			);`,
		`CREATE TABLE IF NOT EXISTS risk_trend_group_candidates (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				mode TEXT NOT NULL DEFAULT 'live',
				group_id INTEGER NOT NULL,
				candidate_key TEXT NOT NULL,
				exchange TEXT NOT NULL,
				symbol TEXT NOT NULL,
				candidate_state TEXT NOT NULL,
				is_selected INTEGER NOT NULL DEFAULT 0,
				priority_score REAL NOT NULL DEFAULT 0,
				score_json TEXT NOT NULL DEFAULT '',
				first_seen_at_ms INTEGER NOT NULL DEFAULT 0,
				last_seen_at_ms INTEGER NOT NULL DEFAULT 0,
				entered_count INTEGER NOT NULL DEFAULT 0,
				first_entry_at_ms INTEGER NOT NULL DEFAULT 0,
				last_entry_at_ms INTEGER NOT NULL DEFAULT 0,
				last_exit_at_ms INTEGER NOT NULL DEFAULT 0,
				has_open_position INTEGER NOT NULL DEFAULT 0,
				last_signal_action INTEGER NOT NULL DEFAULT 0,
				last_high_side INTEGER NOT NULL DEFAULT 0,
				last_mid_side INTEGER NOT NULL DEFAULT 0,
				trending_timestamp_ms INTEGER NOT NULL DEFAULT 0,
				exit_reason TEXT NOT NULL DEFAULT '',
				updated_at_ms INTEGER NOT NULL DEFAULT 0,
				UNIQUE(mode, group_id, candidate_key)
			);`,
	}

	if err := s.cleanupDeprecatedPositionTables(); err != nil {
		return err
	}

	for _, stmt := range schema {
		if _, err := s.DB.Exec(stmt); err != nil {
			return err
		}
	}
	if err := ensureColumn(s.DB, "exchanges", "rate_limit", "INTEGER NOT NULL DEFAULT 100"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "api_key", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "ohlcv_limit", "INTEGER NOT NULL DEFAULT 300"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "volume_filter", "REAL NOT NULL DEFAULT 1"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "market_proxy", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "trade_proxy", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "timeframes", `TEXT NOT NULL DEFAULT '["3m","15m","1h"]'`); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "exchanges", "active", "INTEGER NOT NULL DEFAULT 1"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "symbols", "timeframes", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "symbols", "dynamic", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "positions", "singleton_id", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "history_positions", "singleton_id", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "risk_trend_groups", "incumbent_leader_key", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "risk_trend_groups", "incumbent_leader_score", "REAL NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if err := ensureColumn(s.DB, "risk_trend_groups", "incumbent_leader_closed_at_ms", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_history_positions_mode_exchange_symbol_close_time ON history_positions(mode, exchange, symbol, close_time_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_history_positions_singleton_close_time ON history_positions(singleton_id, close_time_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_positions_mode_singleton_updated_time ON positions(mode, singleton_id, updated_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_signals_mode_pair_time ON signals(mode, exchange_id, symbol_id, id DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_signals_mode_pair_event_time ON signals(mode, exchange_id, symbol_id, event_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_signals_mode_key_time ON signals(mode, exchange_id, symbol_id, timeframe, strategy, id DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_signals_singleton_event_time ON signals(singleton_id, event_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_backtest_tasks_status_created ON backtest_tasks(status, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_backtest_tasks_exchange_symbol_created ON backtest_tasks(exchange, symbol, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_backtest_tasks_singleton ON backtest_tasks(singleton_id, singleton_uuid);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_singleton_mode_lease ON singleton(mode, closed, lease_expires);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_orders_exchange_symbol_time ON orders(exchange, symbol, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_orders_exchange_symbol_started_time ON orders(exchange, symbol, started_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_orders_status_time ON orders(result_status, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_orders_client_order_id ON orders(client_order_id);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_manual_orders_status_created ON manual_orders(mode, status, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_manual_orders_pair_status_created ON manual_orders(mode, exchange, symbol, status, created_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_manual_orders_client_order_id ON manual_orders(client_order_id);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_manual_orders_exchange_order_id ON manual_orders(exchange_order_id);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_risk_decisions_pair_time ON risk_decisions(exchange, symbol, event_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_risk_decisions_status_time ON risk_decisions(result_status, event_at_ms DESC);`); err != nil {
		return err
	}
	if _, err := s.DB.Exec(`CREATE INDEX IF NOT EXISTS idx_risk_decisions_group_time ON risk_decisions(group_id, event_at_ms DESC);`); err != nil {
		return err
	}
	return nil
}

func ensureColumn(db *sql.DB, table, column, columnType string) (err error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close pragma rows: %v; %w", closeErr, err)
			}
		}
	}()

	found := false
	for rows.Next() {
		var (
			cid       int
			name      string
			typ       string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if scanErr := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); scanErr != nil {
			return scanErr
		}
		if name == column {
			found = true
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, columnType))
	return err
}

func (s *SQLite) cleanupDeprecatedPositionTables() error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	legacyExists, err := hasTableByName(s.DB, "legacy_positions")
	if err != nil {
		return err
	}
	if legacyExists {
		if _, err := s.DB.Exec(`DROP TABLE IF EXISTS legacy_positions;`); err != nil {
			return err
		}
	}
	exists, err := hasTableByName(s.DB, "positions")
	if err != nil || !exists {
		return err
	}
	hasPositionID, err := hasTableColumn(s.DB, "positions", "position_id")
	if err != nil {
		return err
	}
	hasInstID, err := hasTableColumn(s.DB, "positions", "inst_id")
	if err != nil {
		return err
	}
	if !(hasPositionID && !hasInstID) {
		return nil
	}
	_, err = s.DB.Exec(`DROP TABLE IF EXISTS positions;`)
	return err
}

func hasTableByName(db *sql.DB, name string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("nil db")
	}
	var found int
	if err := db.QueryRow(`SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1;`, name).Scan(&found); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func hasTableColumn(db *sql.DB, table, column string) (exists bool, err error) {
	if db == nil {
		return false, fmt.Errorf("nil db")
	}
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return false, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()
	for rows.Next() {
		var (
			cid       int
			name      string
			typ       string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if scanErr := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); scanErr != nil {
			return false, scanErr
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func SeedDefaults(db *sql.DB) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("seed begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = fmt.Errorf("seed rollback failed: %w", rbErr)
			} else {
				err = fmt.Errorf("seed rollback failed: %v; %w", rbErr, err)
			}
		}
	}()

	if err := seedDefaults(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("seed commit: %w", err)
	}
	return nil
}

func (s *SQLite) ListActiveSymbols() (out []models.Symbol, err error) {
	return s.listSymbols("WHERE s.active = 1 AND e.active = 1")
}

func (s *SQLite) ListSymbols() (out []models.Symbol, err error) {
	return s.listSymbols("")
}

func (s *SQLite) listSymbols(whereClause string) (out []models.Symbol, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	query := `SELECT e.name, s.symbol, s.base, s.quote, s.type, COALESCE(s.timeframes, ''), s.active, s.dynamic, e.rate_limit
		 FROM symbols s
		 JOIN exchanges e ON s.exchange_id = e.id`
	whereClause = strings.TrimSpace(whereClause)
	if whereClause != "" {
		query += " " + whereClause
	}
	query += " ORDER BY e.name, s.symbol;"
	rows, err := s.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()

	for rows.Next() {
		var item models.Symbol
		var active int
		var dynamic int
		if err := rows.Scan(&item.Exchange, &item.Symbol, &item.Base, &item.Quote, &item.Type, &item.Timeframes, &active, &dynamic, &item.RateLimitMS); err != nil {
			return nil, err
		}
		item.Active = active == 1
		item.Dynamic = dynamic == 1
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListExchanges() (out []models.Exchange, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	rows, err := s.DB.Query(
		`SELECT name, COALESCE(api_key, ''), rate_limit, ohlcv_limit, volume_filter, COALESCE(market_proxy, ''), COALESCE(trade_proxy, ''), timeframes, active
		 FROM exchanges
		 ORDER BY name;`,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()

	for rows.Next() {
		var item models.Exchange
		var active int
		if err := rows.Scan(&item.Name, &item.APIKey, &item.RateLimitMS, &item.OHLCVLimit, &item.VolumeFilter, &item.MarketProxy, &item.TradeProxy, &item.Timeframes, &active); err != nil {
			return nil, err
		}
		item.Active = active == 1
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) UpsertSymbol(sym models.Symbol) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	exchangeID, err := s.lookupExchangeID(sym.Exchange)
	if err != nil {
		return err
	}
	active := 0
	if sym.Active {
		active = 1
	}
	dynamic := 0
	if sym.Dynamic {
		dynamic = 1
	}
	timeframes := strings.TrimSpace(sym.Timeframes)
	_, err = s.DB.Exec(
		`INSERT INTO symbols (exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(exchange_id, symbol) DO UPDATE SET
		   base = excluded.base,
		   quote = excluded.quote,
		   type = excluded.type,
		   timeframes = CASE
		     WHEN excluded.timeframes != '' THEN excluded.timeframes
		     ELSE timeframes
		   END,
		   active = excluded.active,
		   dynamic = excluded.dynamic;`,
		exchangeID, sym.Symbol, sym.Base, sym.Quote, sym.Type, timeframes, active, dynamic,
	)
	return err
}

func (s *SQLite) UpdateSymbolActive(exchange, symbol string, active bool) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	exchangeID, err := s.lookupExchangeID(exchange)
	if err != nil {
		return err
	}
	activeValue := 0
	if active {
		activeValue = 1
	}
	_, err = s.DB.Exec(
		`UPDATE symbols SET active = ? WHERE exchange_id = ? AND symbol = ?;`,
		activeValue, exchangeID, symbol,
	)
	return err
}

func (s *SQLite) GetConfigValue(name string) (value string, found bool, err error) {
	if s == nil || s.DB == nil {
		return "", false, fmt.Errorf("nil db")
	}
	if err := s.DB.QueryRow(`SELECT value FROM config WHERE name = ?;`, name).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return value, true, nil
}

func (s *SQLite) UpsertConfigValue(name, value, common string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return fmt.Errorf("empty config name")
	}
	common = strings.TrimSpace(common)
	if common == "" {
		common = "runtime managed by risk module"
	}
	_, err := s.DB.Exec(
		`INSERT INTO config (name, value, common) VALUES (?, ?, ?)
		 ON CONFLICT(name) DO UPDATE SET
		   value = excluded.value,
		   common = excluded.common;`,
		name, value, common,
	)
	return err
}

func (s *SQLite) GetRiskAccountState(mode, exchange string) (models.RiskAccountState, bool, error) {
	if s == nil || s.DB == nil {
		return models.RiskAccountState{}, false, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return models.RiskAccountState{}, false, fmt.Errorf("empty mode")
	}
	exchange = strings.TrimSpace(exchange)
	if exchange == "" {
		return models.RiskAccountState{}, false, fmt.Errorf("empty exchange")
	}
	var state models.RiskAccountState
	err := s.DB.QueryRow(
		`SELECT mode, exchange, trade_date, total_usdt, funding_usdt, trading_usdt, per_trade_usdt,
		        daily_loss_limit_usdt, daily_realized_usdt, daily_closed_profit_usdt, updated_at_ms
		   FROM risk_account_states
		  WHERE mode = ? AND exchange = ?;`,
		mode, exchange,
	).Scan(
		&state.Mode,
		&state.Exchange,
		&state.TradeDate,
		&state.TotalUSDT,
		&state.FundingUSDT,
		&state.TradingUSDT,
		&state.PerTradeUSDT,
		&state.DailyLossLimitUSDT,
		&state.DailyRealizedUSDT,
		&state.DailyClosedProfitUSDT,
		&state.UpdatedAtMS,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return models.RiskAccountState{}, false, nil
		}
		return models.RiskAccountState{}, false, err
	}
	return state, true, nil
}

func (s *SQLite) UpsertRiskAccountState(state models.RiskAccountState) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	state.Mode = strings.TrimSpace(state.Mode)
	if state.Mode == "" {
		return fmt.Errorf("empty mode")
	}
	state.Exchange = strings.TrimSpace(state.Exchange)
	if state.Exchange == "" {
		return fmt.Errorf("empty exchange")
	}
	state.TradeDate = strings.TrimSpace(state.TradeDate)
	if state.TradeDate == "" {
		state.TradeDate = time.Now().Format("2006-01-02")
	}
	_, err := s.DB.Exec(
		`INSERT INTO risk_account_states (
		     mode, exchange, trade_date, total_usdt, funding_usdt, trading_usdt,
		     per_trade_usdt, daily_loss_limit_usdt, daily_realized_usdt, daily_closed_profit_usdt, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, exchange) DO UPDATE SET
		   trade_date = excluded.trade_date,
		   total_usdt = excluded.total_usdt,
		   funding_usdt = excluded.funding_usdt,
		   trading_usdt = excluded.trading_usdt,
		   per_trade_usdt = excluded.per_trade_usdt,
		   daily_loss_limit_usdt = excluded.daily_loss_limit_usdt,
		   daily_realized_usdt = excluded.daily_realized_usdt,
		   daily_closed_profit_usdt = excluded.daily_closed_profit_usdt,
		   updated_at_ms = excluded.updated_at_ms;`,
		state.Mode,
		state.Exchange,
		state.TradeDate,
		state.TotalUSDT,
		state.FundingUSDT,
		state.TradingUSDT,
		state.PerTradeUSDT,
		state.DailyLossLimitUSDT,
		state.DailyRealizedUSDT,
		state.DailyClosedProfitUSDT,
		state.UpdatedAtMS,
	)
	return err
}

func (s *SQLite) ResetTradeCooldownForTradeDate(tradeDate string) (affected int64, exchanges []string, err error) {
	if s == nil || s.DB == nil {
		return 0, nil, fmt.Errorf("nil db")
	}
	tradeDate = strings.TrimSpace(tradeDate)
	if tradeDate == "" {
		return 0, nil, fmt.Errorf("empty trade_date")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = rbErr
			} else {
				err = fmt.Errorf("rollback failed: %v; %w", rbErr, err)
			}
		}
	}()

	rows, err := tx.Query(
		`SELECT exchange
		   FROM risk_account_states
		  WHERE trade_date = ?
		    AND daily_loss_limit_usdt > 0
		    AND daily_realized_usdt >= daily_loss_limit_usdt
		  ORDER BY exchange;`,
		tradeDate,
	)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if rows == nil {
			return
		}
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()
	for rows.Next() {
		var exchange string
		if scanErr := rows.Scan(&exchange); scanErr != nil {
			return 0, nil, scanErr
		}
		exchanges = append(exchanges, exchange)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	if closeErr := rows.Close(); closeErr != nil {
		return 0, nil, closeErr
	}
	rows = nil

	nowMS := time.Now().UnixMilli()
	result, err := tx.Exec(
		`UPDATE risk_account_states
		    SET daily_realized_usdt = 0,
		        updated_at_ms = ?
		  WHERE trade_date = ?
		    AND daily_loss_limit_usdt > 0
		    AND daily_realized_usdt >= daily_loss_limit_usdt;`,
		nowMS,
		tradeDate,
	)
	if err != nil {
		return 0, nil, err
	}
	affected, err = result.RowsAffected()
	if err != nil {
		return 0, nil, err
	}

	if err := tx.Commit(); err != nil {
		return 0, nil, err
	}
	return affected, exchanges, nil
}

func (s *SQLite) ListRiskSymbolCooldownStates(mode, exchange string) ([]models.RiskSymbolCooldownState, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	exchange = strings.TrimSpace(exchange)
	query := `SELECT mode, exchange, symbol, consecutive_stop_loss, window_start_at_ms, last_stop_loss_at_ms,
	                 cooldown_until_ms, last_processed_close_ms, updated_at_ms
	            FROM risk_symbol_cooldowns
	           WHERE mode = ?`
	args := make([]any, 0, 2)
	args = append(args, mode)
	if exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, exchange)
	}
	query += ` ORDER BY exchange, symbol`
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RiskSymbolCooldownState, 0)
	for rows.Next() {
		var item models.RiskSymbolCooldownState
		if scanErr := rows.Scan(
			&item.Mode,
			&item.Exchange,
			&item.Symbol,
			&item.ConsecutiveStopLoss,
			&item.WindowStartAtMS,
			&item.LastStopLossAtMS,
			&item.CooldownUntilMS,
			&item.LastProcessedCloseMS,
			&item.UpdatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) UpsertRiskSymbolCooldownState(state models.RiskSymbolCooldownState) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	state.Mode = strings.TrimSpace(state.Mode)
	state.Exchange = strings.TrimSpace(state.Exchange)
	state.Symbol = strings.TrimSpace(state.Symbol)
	if state.Mode == "" || state.Exchange == "" || state.Symbol == "" {
		return fmt.Errorf("empty mode/exchange/symbol")
	}
	_, err := s.DB.Exec(
		`INSERT INTO risk_symbol_cooldowns (
		     mode, exchange, symbol, consecutive_stop_loss, window_start_at_ms, last_stop_loss_at_ms,
		     cooldown_until_ms, last_processed_close_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, exchange, symbol) DO UPDATE SET
		   consecutive_stop_loss = excluded.consecutive_stop_loss,
		   window_start_at_ms = excluded.window_start_at_ms,
		   last_stop_loss_at_ms = excluded.last_stop_loss_at_ms,
		   cooldown_until_ms = excluded.cooldown_until_ms,
		   last_processed_close_ms = excluded.last_processed_close_ms,
		   updated_at_ms = excluded.updated_at_ms;`,
		state.Mode,
		state.Exchange,
		state.Symbol,
		state.ConsecutiveStopLoss,
		state.WindowStartAtMS,
		state.LastStopLossAtMS,
		state.CooldownUntilMS,
		state.LastProcessedCloseMS,
		state.UpdatedAtMS,
	)
	return err
}

func (s *SQLite) ListRiskTrendGroups(mode string) ([]models.RiskTrendGroup, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	rows, err := s.DB.Query(
		`SELECT id, mode, strategy, primary_timeframe, side, anchor_trending_timestamp_ms, state, lock_stage,
		        selected_candidate_key, incumbent_leader_key, incumbent_leader_score, incumbent_leader_closed_at_ms,
		        first_entry_at_ms, last_entry_at_ms, entry_count, finish_reason,
		        created_at_ms, updated_at_ms, finished_at_ms
		   FROM risk_trend_groups
		  WHERE mode = ?
		  ORDER BY strategy, primary_timeframe, side, anchor_trending_timestamp_ms, id`,
		mode,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RiskTrendGroup, 0)
	for rows.Next() {
		var item models.RiskTrendGroup
		if scanErr := rows.Scan(
			&item.ID,
			&item.Mode,
			&item.Strategy,
			&item.PrimaryTimeframe,
			&item.Side,
			&item.AnchorTrendingTimestampMS,
			&item.State,
			&item.LockStage,
			&item.SelectedCandidateKey,
			&item.IncumbentLeaderKey,
			&item.IncumbentLeaderScore,
			&item.IncumbentLeaderClosedAtMS,
			&item.FirstEntryAtMS,
			&item.LastEntryAtMS,
			&item.EntryCount,
			&item.FinishReason,
			&item.CreatedAtMS,
			&item.UpdatedAtMS,
			&item.FinishedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListRiskTrendGroupCandidates(mode string) ([]models.RiskTrendGroupCandidate, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	rows, err := s.DB.Query(
		`SELECT id, mode, group_id, candidate_key, exchange, symbol, candidate_state, is_selected, priority_score,
		        score_json, first_seen_at_ms, last_seen_at_ms, entered_count, first_entry_at_ms,
		        last_entry_at_ms, last_exit_at_ms, has_open_position, last_signal_action, last_high_side,
		        last_mid_side, trending_timestamp_ms, exit_reason, updated_at_ms
		   FROM risk_trend_group_candidates
		  WHERE mode = ?
		  ORDER BY group_id, candidate_key, id`,
		mode,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RiskTrendGroupCandidate, 0)
	for rows.Next() {
		var item models.RiskTrendGroupCandidate
		var isSelected int
		var hasOpenPosition int
		if scanErr := rows.Scan(
			&item.ID,
			&item.Mode,
			&item.GroupID,
			&item.CandidateKey,
			&item.Exchange,
			&item.Symbol,
			&item.CandidateState,
			&isSelected,
			&item.PriorityScore,
			&item.ScoreJSON,
			&item.FirstSeenAtMS,
			&item.LastSeenAtMS,
			&item.EnteredCount,
			&item.FirstEntryAtMS,
			&item.LastEntryAtMS,
			&item.LastExitAtMS,
			&hasOpenPosition,
			&item.LastSignalAction,
			&item.LastHighSide,
			&item.LastMidSide,
			&item.TrendingTimestampMS,
			&item.ExitReason,
			&item.UpdatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		item.IsSelected = isSelected != 0
		item.HasOpenPosition = hasOpenPosition != 0
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) UpsertRiskTrendGroup(group *models.RiskTrendGroup) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if group == nil {
		return fmt.Errorf("nil trend group")
	}
	group.Mode = strings.TrimSpace(group.Mode)
	group.Strategy = strings.TrimSpace(group.Strategy)
	group.PrimaryTimeframe = strings.TrimSpace(group.PrimaryTimeframe)
	group.Side = strings.TrimSpace(group.Side)
	group.State = strings.TrimSpace(group.State)
	group.LockStage = strings.TrimSpace(group.LockStage)
	group.SelectedCandidateKey = strings.TrimSpace(group.SelectedCandidateKey)
	group.IncumbentLeaderKey = strings.TrimSpace(group.IncumbentLeaderKey)
	group.FinishReason = strings.TrimSpace(group.FinishReason)
	if group.Mode == "" || group.Strategy == "" || group.PrimaryTimeframe == "" || group.Side == "" || group.AnchorTrendingTimestampMS <= 0 {
		return fmt.Errorf("invalid trend group identity")
	}
	if group.CreatedAtMS <= 0 {
		group.CreatedAtMS = time.Now().UnixMilli()
	}
	if group.UpdatedAtMS <= 0 {
		group.UpdatedAtMS = group.CreatedAtMS
	}
	_, err := s.DB.Exec(
		`INSERT INTO risk_trend_groups (
		     mode, strategy, primary_timeframe, side, anchor_trending_timestamp_ms, state, lock_stage,
		     selected_candidate_key, incumbent_leader_key, incumbent_leader_score, incumbent_leader_closed_at_ms,
		     first_entry_at_ms, last_entry_at_ms, entry_count, finish_reason,
		     created_at_ms, updated_at_ms, finished_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, strategy, primary_timeframe, side, anchor_trending_timestamp_ms) DO UPDATE SET
		   state = excluded.state,
		   lock_stage = excluded.lock_stage,
		   selected_candidate_key = excluded.selected_candidate_key,
		   incumbent_leader_key = excluded.incumbent_leader_key,
		   incumbent_leader_score = excluded.incumbent_leader_score,
		   incumbent_leader_closed_at_ms = excluded.incumbent_leader_closed_at_ms,
		   first_entry_at_ms = excluded.first_entry_at_ms,
		   last_entry_at_ms = excluded.last_entry_at_ms,
		   entry_count = excluded.entry_count,
		   finish_reason = excluded.finish_reason,
		   created_at_ms = CASE
		     WHEN risk_trend_groups.created_at_ms > 0 THEN risk_trend_groups.created_at_ms
		     ELSE excluded.created_at_ms
		   END,
		   updated_at_ms = excluded.updated_at_ms,
		   finished_at_ms = excluded.finished_at_ms;`,
		group.Mode,
		group.Strategy,
		group.PrimaryTimeframe,
		group.Side,
		group.AnchorTrendingTimestampMS,
		group.State,
		group.LockStage,
		group.SelectedCandidateKey,
		group.IncumbentLeaderKey,
		group.IncumbentLeaderScore,
		group.IncumbentLeaderClosedAtMS,
		group.FirstEntryAtMS,
		group.LastEntryAtMS,
		group.EntryCount,
		group.FinishReason,
		group.CreatedAtMS,
		group.UpdatedAtMS,
		group.FinishedAtMS,
	)
	if err != nil {
		return err
	}
	return s.DB.QueryRow(
		`SELECT id, created_at_ms
		   FROM risk_trend_groups
		  WHERE mode = ? AND strategy = ? AND primary_timeframe = ? AND side = ? AND anchor_trending_timestamp_ms = ?`,
		group.Mode,
		group.Strategy,
		group.PrimaryTimeframe,
		group.Side,
		group.AnchorTrendingTimestampMS,
	).Scan(&group.ID, &group.CreatedAtMS)
}

func (s *SQLite) UpsertRiskTrendGroupCandidate(candidate *models.RiskTrendGroupCandidate) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if candidate == nil {
		return fmt.Errorf("nil trend group candidate")
	}
	candidate.Mode = strings.TrimSpace(candidate.Mode)
	candidate.CandidateKey = strings.TrimSpace(candidate.CandidateKey)
	candidate.Exchange = strings.TrimSpace(candidate.Exchange)
	candidate.Symbol = strings.TrimSpace(candidate.Symbol)
	candidate.CandidateState = strings.TrimSpace(candidate.CandidateState)
	candidate.ScoreJSON = strings.TrimSpace(candidate.ScoreJSON)
	candidate.ExitReason = strings.TrimSpace(candidate.ExitReason)
	if candidate.Mode == "" || candidate.GroupID <= 0 || candidate.CandidateKey == "" || candidate.Exchange == "" || candidate.Symbol == "" || candidate.CandidateState == "" {
		return fmt.Errorf("invalid trend group candidate identity")
	}
	if candidate.UpdatedAtMS <= 0 {
		candidate.UpdatedAtMS = time.Now().UnixMilli()
	}
	isSelected := 0
	if candidate.IsSelected {
		isSelected = 1
	}
	hasOpenPosition := 0
	if candidate.HasOpenPosition {
		hasOpenPosition = 1
	}
	_, err := s.DB.Exec(
		`INSERT INTO risk_trend_group_candidates (
		     mode, group_id, candidate_key, exchange, symbol, candidate_state, is_selected, priority_score, score_json,
		     first_seen_at_ms, last_seen_at_ms, entered_count, first_entry_at_ms, last_entry_at_ms, last_exit_at_ms,
		     has_open_position, last_signal_action, last_high_side, last_mid_side, trending_timestamp_ms, exit_reason, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, group_id, candidate_key) DO UPDATE SET
		   exchange = excluded.exchange,
		   symbol = excluded.symbol,
		   candidate_state = excluded.candidate_state,
		   is_selected = excluded.is_selected,
		   priority_score = excluded.priority_score,
		   score_json = excluded.score_json,
		   first_seen_at_ms = CASE
		     WHEN risk_trend_group_candidates.first_seen_at_ms > 0 THEN risk_trend_group_candidates.first_seen_at_ms
		     ELSE excluded.first_seen_at_ms
		   END,
		   last_seen_at_ms = excluded.last_seen_at_ms,
		   entered_count = excluded.entered_count,
		   first_entry_at_ms = CASE
		     WHEN risk_trend_group_candidates.first_entry_at_ms > 0 THEN risk_trend_group_candidates.first_entry_at_ms
		     ELSE excluded.first_entry_at_ms
		   END,
		   last_entry_at_ms = excluded.last_entry_at_ms,
		   last_exit_at_ms = excluded.last_exit_at_ms,
		   has_open_position = excluded.has_open_position,
		   last_signal_action = excluded.last_signal_action,
		   last_high_side = excluded.last_high_side,
		   last_mid_side = excluded.last_mid_side,
		   trending_timestamp_ms = excluded.trending_timestamp_ms,
		   exit_reason = excluded.exit_reason,
		   updated_at_ms = excluded.updated_at_ms;`,
		candidate.Mode,
		candidate.GroupID,
		candidate.CandidateKey,
		candidate.Exchange,
		candidate.Symbol,
		candidate.CandidateState,
		isSelected,
		candidate.PriorityScore,
		candidate.ScoreJSON,
		candidate.FirstSeenAtMS,
		candidate.LastSeenAtMS,
		candidate.EnteredCount,
		candidate.FirstEntryAtMS,
		candidate.LastEntryAtMS,
		candidate.LastExitAtMS,
		hasOpenPosition,
		candidate.LastSignalAction,
		candidate.LastHighSide,
		candidate.LastMidSide,
		candidate.TrendingTimestampMS,
		candidate.ExitReason,
		candidate.UpdatedAtMS,
	)
	if err != nil {
		return err
	}
	return s.DB.QueryRow(
		`SELECT id
		   FROM risk_trend_group_candidates
		  WHERE mode = ? AND group_id = ? AND candidate_key = ?`,
		candidate.Mode,
		candidate.GroupID,
		candidate.CandidateKey,
	).Scan(&candidate.ID)
}

func (s *SQLite) ListRiskOpenPositions(mode, exchange string) ([]models.RiskOpenPosition, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	query := `SELECT singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
	                 upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px,
	                 open_time_ms, update_time_ms, row_json, max_floating_loss_amount,
	                 max_floating_profit_amount, updated_at_ms
	          FROM positions
	         WHERE mode = ?`
	args := make([]any, 0, 2)
	args = append(args, mode)
	if exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, exchange)
	}
	query += ` ORDER BY exchange, symbol, inst_id, pos_side, mgn_mode`
	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RiskOpenPosition, 0)
	for rows.Next() {
		var item models.RiskOpenPosition
		if scanErr := rows.Scan(
			&item.SingletonID,
			&item.Mode,
			&item.Exchange,
			&item.Symbol,
			&item.InstID,
			&item.Pos,
			&item.PosSide,
			&item.MgnMode,
			&item.Margin,
			&item.Lever,
			&item.AvgPx,
			&item.Upl,
			&item.UplRatio,
			&item.NotionalUSD,
			&item.MarkPx,
			&item.LiqPx,
			&item.TPTriggerPx,
			&item.SLTriggerPx,
			&item.OpenTimeMS,
			&item.UpdateTimeMS,
			&item.RowJSON,
			&item.MaxFloatingLossAmount,
			&item.MaxFloatingProfitAmount,
			&item.UpdatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		item.SingletonID = normalizeRiskOpenPositionSingletonID(item.SingletonID, item.RowJSON)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeRiskOpenPositionSingletonID(singletonID int64, rowJSON string) int64 {
	if singletonID > 0 {
		return singletonID
	}
	return models.ExtractPositionRuntimeMeta(rowJSON).SingletonID
}

func (s *SQLite) InsertExecutionOrder(record models.ExecutionOrderRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if strings.TrimSpace(record.AttemptID) == "" {
		return fmt.Errorf("empty attempt id")
	}
	nowMS := time.Now().UnixMilli()
	if record.CreatedAtMS <= 0 {
		record.CreatedAtMS = nowMS
	}
	if record.UpdatedAtMS <= 0 {
		record.UpdatedAtMS = nowMS
	}
	if record.StartedAtMS <= 0 {
		record.StartedAtMS = nowMS
	}
	if record.FinishedAtMS > 0 && record.DurationMS <= 0 {
		record.DurationMS = record.FinishedAtMS - record.StartedAtMS
	}
	if record.ResultStatus == "" {
		record.ResultStatus = "failed"
	}
	hasSideEffect := 0
	if record.HasSideEffect {
		hasSideEffect = 1
	}
	_, err := s.DB.Exec(
		`INSERT INTO orders (
		     attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
		     position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
		     stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
		     fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
		     has_side_effect, step_results_json, request_json, response_json, started_at_ms,
		     finished_at_ms, duration_ms, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		record.AttemptID,
		record.SingletonUUID,
		record.Mode,
		record.Source,
		record.Exchange,
		record.Symbol,
		record.InstID,
		record.Action,
		record.OrderType,
		record.PositionSide,
		record.MarginMode,
		record.Size,
		record.LeverageMultiplier,
		record.Price,
		record.TakeProfitPrice,
		record.StopLossPrice,
		record.ClientOrderID,
		record.Strategy,
		record.ResultStatus,
		record.FailSource,
		record.FailStage,
		record.FailReason,
		record.ExchangeCode,
		record.ExchangeMessage,
		record.ExchangeOrderID,
		record.ExchangeAlgoOrderID,
		hasSideEffect,
		record.StepResultsJSON,
		record.RequestJSON,
		record.ResponseJSON,
		record.StartedAtMS,
		record.FinishedAtMS,
		record.DurationMS,
		record.CreatedAtMS,
		record.UpdatedAtMS,
	)
	if err != nil {
		return err
	}
	return nil
}

func (s *SQLite) InsertRiskDecision(record models.RiskDecisionRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	nowMS := time.Now().UnixMilli()
	if record.CreatedAtMS <= 0 {
		record.CreatedAtMS = nowMS
	}
	if record.EventAtMS <= 0 {
		record.EventAtMS = nowMS
	}
	record.SingletonUUID = strings.TrimSpace(record.SingletonUUID)
	record.Mode = strings.TrimSpace(record.Mode)
	record.Exchange = strings.TrimSpace(record.Exchange)
	record.Symbol = strings.TrimSpace(record.Symbol)
	record.Timeframe = strings.TrimSpace(record.Timeframe)
	record.Strategy = strings.TrimSpace(record.Strategy)
	record.ComboKey = strings.TrimSpace(record.ComboKey)
	record.GroupID = strings.TrimSpace(record.GroupID)
	record.DecisionAction = strings.TrimSpace(record.DecisionAction)
	record.ResultStatus = strings.TrimSpace(record.ResultStatus)
	record.RejectReason = strings.TrimSpace(record.RejectReason)
	record.SignalJSON = strings.TrimSpace(record.SignalJSON)
	record.DecisionJSON = strings.TrimSpace(record.DecisionJSON)
	if record.Exchange == "" || record.Symbol == "" || record.Timeframe == "" {
		return fmt.Errorf("risk decision record missing exchange/symbol/timeframe")
	}
	if record.ResultStatus == "" {
		record.ResultStatus = "ignored"
	}
	if record.SignalJSON == "" {
		record.SignalJSON = "{}"
	}
	if record.DecisionJSON == "" {
		record.DecisionJSON = "{}"
	}
	_, err := s.DB.Exec(
		`INSERT INTO risk_decisions (
			singleton_id, singleton_uuid, mode, exchange, symbol, timeframe, strategy, combo_key,
			group_id, signal_action, high_side, decision_action, result_status, reject_reason,
			event_at_ms, trigger_timestamp_ms, trending_timestamp_ms, signal_json, decision_json, created_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		record.SingletonID,
		record.SingletonUUID,
		record.Mode,
		record.Exchange,
		record.Symbol,
		record.Timeframe,
		record.Strategy,
		record.ComboKey,
		record.GroupID,
		record.SignalAction,
		record.HighSide,
		record.DecisionAction,
		record.ResultStatus,
		record.RejectReason,
		record.EventAtMS,
		record.TriggerTimestampMS,
		record.TrendingTimestampMS,
		record.SignalJSON,
		record.DecisionJSON,
		record.CreatedAtMS,
	)
	return err
}

func (s *SQLite) ListExecutionOrders(mode, singletonUUID string) ([]models.ExecutionOrderRecord, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	singletonUUID = strings.TrimSpace(singletonUUID)

	query := `SELECT attempt_id, singleton_uuid, mode, source, exchange, symbol, inst_id, action, order_type,
	                 position_side, margin_mode, size, leverage_multiplier, price, take_profit_price,
	                 stop_loss_price, client_order_id, strategy, result_status, fail_source, fail_stage,
	                 fail_reason, exchange_code, exchange_message, exchange_order_id, exchange_algo_order_id,
	                 has_side_effect, step_results_json, request_json, response_json, started_at_ms,
	                 finished_at_ms, duration_ms, created_at_ms, updated_at_ms
	          FROM orders`
	args := make([]any, 0, 2)
	clauses := make([]string, 0, 2)
	if mode != "" {
		clauses = append(clauses, "mode = ?")
		args = append(args, mode)
	}
	if singletonUUID != "" {
		clauses = append(clauses, "singleton_uuid = ?")
		args = append(args, singletonUUID)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += " ORDER BY created_at_ms ASC, id ASC"

	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.ExecutionOrderRecord, 0)
	for rows.Next() {
		var (
			item          models.ExecutionOrderRecord
			hasSideEffect int
		)
		if scanErr := rows.Scan(
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
		); scanErr != nil {
			return nil, scanErr
		}
		item.HasSideEffect = hasSideEffect == 1
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

type riskHistoryRecord struct {
	ID                      int64
	SingletonID             int64
	Mode                    string
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
	CreatedAtMS             int64
	UpdatedAtMS             int64
}

func (s *SQLite) ListRiskHistorySnapshots(mode, exchange string) ([]models.RiskHistoryPosition, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	query := `SELECT singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
	                 notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
	                 open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
	                 open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
	                 close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
	          FROM history_positions
	         WHERE mode = ?`
	args := make([]any, 0, 2)
	args = append(args, mode)
	if exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, exchange)
	}
	query += ` ORDER BY close_time_ms DESC, id DESC`

	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.RiskHistoryPosition, 0)
	for rows.Next() {
		var item models.RiskHistoryPosition
		if scanErr := rows.Scan(
			&item.SingletonID,
			&item.Mode,
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
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListRiskHistoryPositions(mode, exchange string) ([]models.Position, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return nil, fmt.Errorf("empty mode")
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	query := `SELECT singleton_id, exchange, symbol, pos_side, mgn_mode, lever, margin, avg_px, pos, notional_usd,
	                 tp_trigger_px, sl_trigger_px, mark_px, open_time_ms, close_avg_px, realized_pnl,
	                 pnl_ratio, fee, funding_fee, max_floating_profit_amount, max_floating_loss_amount,
	                 close_time_ms, state, open_row_json, updated_at_ms
	          FROM history_positions`
	args := make([]any, 0, 3)
	query += ` WHERE mode = ? AND state <> ?`
	args = append(args, mode, riskHistoryStateSyncPending)
	if exchange != "" {
		query += ` AND exchange = ?`
		args = append(args, exchange)
	}
	query += ` ORDER BY exchange, symbol, close_time_ms DESC, id DESC`

	rows, err := s.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.Position, 0)
	for rows.Next() {
		var (
			singletonID  int64
			itemExchange string
			symbol       string
			posSide      string
			mgnMode      string
			lever        string
			margin       string
			avgPx        string
			pos          string
			notionalUSD  string
			tpTriggerPx  string
			slTriggerPx  string
			markPx       string
			openTimeMS   int64
			closeAvgPx   string
			realizedPnl  string
			pnlRatio     string
			fee          string
			fundingFee   string
			maxProfit    float64
			maxLoss      float64
			closeTimeMS  int64
			state        string
			openRowJSON  string
			updatedAtMS  int64
		)
		if scanErr := rows.Scan(
			&singletonID,
			&itemExchange,
			&symbol,
			&posSide,
			&mgnMode,
			&lever,
			&margin,
			&avgPx,
			&pos,
			&notionalUSD,
			&tpTriggerPx,
			&slTriggerPx,
			&markPx,
			&openTimeMS,
			&closeAvgPx,
			&realizedPnl,
			&pnlRatio,
			&fee,
			&fundingFee,
			&maxProfit,
			&maxLoss,
			&closeTimeMS,
			&state,
			&openRowJSON,
			&updatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		openMeta := models.ExtractStrategyContextMeta(openRowJSON)

		entryQty := math.Abs(parseFloatText(pos))
		entryPrice := parseFloatText(avgPx)
		entryValue := parseFloatText(notionalUSD)
		if entryValue <= 0 && entryQty > 0 && entryPrice > 0 {
			entryValue = entryQty * entryPrice
		}
		exitPrice := parseFloatText(closeAvgPx)
		exitValue := 0.0
		if exitPrice > 0 && entryQty > 0 {
			exitValue = exitPrice * entryQty
		}
		realized := parseFloatText(realizedPnl)
		feeValue := parseFloatText(fee)
		fundingValue := parseFloatText(fundingFee)
		profit := realized
		profitRate := parseFloatText(pnlRatio)
		if math.Abs(profitRate) > 1e-12 && math.Abs(realized) > 1e-12 {
			base := realized / profitRate
			if math.Abs(base) > 1e-12 {
				profitRate = profit / base
			}
		}
		status := strings.ToLower(strings.TrimSpace(state))
		if status == "" {
			status = models.PositionStatusClosed
		}
		updatedTime := formatRiskTimestampMS(updatedAtMS)
		if updatedTime == "" {
			updatedTime = formatRiskTimestampMS(closeTimeMS)
		}

		out = append(out, models.Position{
			SingletonID:             singletonID,
			Exchange:                itemExchange,
			Symbol:                  symbol,
			Timeframe:               strategyPrimaryTimeframe(openMeta),
			GroupID:                 strings.TrimSpace(openMeta.GroupID),
			PositionSide:            posSide,
			MarginMode:              mgnMode,
			LeverageMultiplier:      parseFloatText(lever),
			MarginAmount:            parseFloatText(margin),
			EntryPrice:              entryPrice,
			EntryQuantity:           entryQty,
			EntryValue:              entryValue,
			EntryTime:               formatRiskTimestampMS(openTimeMS),
			TakeProfitPrice:         parseFloatText(tpTriggerPx),
			StopLossPrice:           parseFloatText(slTriggerPx),
			CurrentPrice:            parseFloatText(markPx),
			ExitPrice:               exitPrice,
			ExitQuantity:            entryQty,
			ExitValue:               exitValue,
			ExitTime:                formatRiskTimestampMS(closeTimeMS),
			FeeAmount:               feeValue + fundingValue,
			ProfitAmount:            profit,
			ProfitRate:              profitRate,
			MaxFloatingProfitAmount: maxProfit,
			MaxFloatingLossAmount:   maxLoss,
			Status:                  status,
			StrategyName:            strings.TrimSpace(openMeta.StrategyName),
			StrategyVersion:         strings.TrimSpace(openMeta.StrategyVersion),
			StrategyTimeframes:      append([]string(nil), openMeta.StrategyTimeframes...),
			ComboKey:                strings.TrimSpace(openMeta.ComboKey),
			UpdatedTime:             updatedTime,
			UnrealizedProfitAmount:  0,
			UnrealizedProfitRate:    0,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) SyncRiskHistoryPositions(mode, exchange string, closedPositions []models.RiskClosedPosition) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return fmt.Errorf("empty mode")
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" {
		return fmt.Errorf("empty exchange")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			s.logger.Error("sync risk history rollback failed", zap.Error(rbErr))
		}
	}()

	existingRows, err := tx.Query(
		`SELECT id, singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		        notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		        open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount,
		        open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
		        close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
		   FROM history_positions
		  WHERE mode = ? AND exchange = ?;`,
		mode, exchange,
	)
	if err != nil {
		return err
	}
	existing := make(map[string]riskHistoryRecord)
	existingByOpenKey := make(map[string][]riskHistoryRecord)
	existingByOpenKeyNoSide := make(map[string][]riskHistoryRecord)
	for existingRows.Next() {
		var item riskHistoryRecord
		if scanErr := existingRows.Scan(
			&item.ID,
			&item.SingletonID,
			&item.Mode,
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
			_ = existingRows.Close()
			return scanErr
		}
		fullKey := riskHistoryKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS, item.CloseTimeMS)
		if fullKey != "" {
			existing[fullKey] = item
		}
		openKey := riskHistoryOpenKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS)
		if openKey != "" {
			existingByOpenKey[openKey] = append(existingByOpenKey[openKey], item)
		}
		openKeyNoSide := riskHistoryOpenKeyWithoutSide(item.Exchange, item.InstID, item.MgnMode, item.OpenTimeMS)
		if openKeyNoSide != "" {
			existingByOpenKeyNoSide[openKeyNoSide] = append(existingByOpenKeyNoSide[openKeyNoSide], item)
		}
	}
	if err := existingRows.Close(); err != nil {
		return err
	}

	currentOpenKeys, currentOpenKeysNoSide, err := loadRiskOpenHistoryKeySets(tx, mode, exchange)
	if err != nil {
		return err
	}

	latestClosedByOpenKey := make(map[string]models.RiskClosedPosition, len(closedPositions))
	for _, item := range closedPositions {
		itemExchange := strings.ToLower(strings.TrimSpace(item.Exchange))
		if itemExchange == "" {
			itemExchange = exchange
		}
		if itemExchange != exchange {
			continue
		}
		instID := strings.ToUpper(strings.TrimSpace(item.InstID))
		posSide := strings.ToLower(strings.TrimSpace(item.PosSide))
		mgnMode := strings.ToLower(strings.TrimSpace(item.MgnMode))
		closeTimeMS := item.CloseTimeMS
		openTimeMS := item.OpenTimeMS
		if closeTimeMS <= 0 || openTimeMS <= 0 || instID == "" || posSide == "" {
			continue
		}
		if mgnMode == "" {
			mgnMode = models.MarginModeIsolated
		}
		openKey := riskHistoryOpenKey(itemExchange, instID, posSide, mgnMode, openTimeMS)
		if openKey == "" {
			continue
		}
		item.Exchange = itemExchange
		item.Mode = mode
		item.InstID = instID
		item.PosSide = posSide
		item.MgnMode = mgnMode
		if prev, ok := latestClosedByOpenKey[openKey]; ok {
			latestClosedByOpenKey[openKey] = bestRiskClosedPosition(prev, item)
			continue
		}
		latestClosedByOpenKey[openKey] = item
	}

	upsertStmt, err := tx.Prepare(
		`INSERT INTO history_positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount, open_row_json,
		     close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee, close_time_ms, state,
		     close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, exchange, inst_id, pos_side, mgn_mode, open_time_ms, close_time_ms) DO UPDATE SET
		   singleton_id = CASE
		     WHEN history_positions.singleton_id > 0 THEN history_positions.singleton_id
		     ELSE excluded.singleton_id
		   END,
		   symbol = excluded.symbol,
		   close_avg_px = excluded.close_avg_px,
		   realized_pnl = excluded.realized_pnl,
		   pnl_ratio = excluded.pnl_ratio,
		   fee = excluded.fee,
		   funding_fee = excluded.funding_fee,
		   state = excluded.state,
		   close_row_json = excluded.close_row_json,
		   updated_at_ms = excluded.updated_at_ms;`,
	)
	if err != nil {
		return err
	}
	defer upsertStmt.Close()

	deleteByIDStmt, err := tx.Prepare(`DELETE FROM history_positions WHERE id = ?;`)
	if err != nil {
		return err
	}
	defer deleteByIDStmt.Close()

	deletedHistoryIDs := make(map[int64]struct{})
	nowMS := time.Now().UnixMilli()
	for _, item := range latestClosedByOpenKey {
		itemExchange := item.Exchange
		instID := item.InstID
		posSide := item.PosSide
		mgnMode := item.MgnMode
		closeTimeMS := item.CloseTimeMS
		openTimeMS := item.OpenTimeMS
		updatedAtMS := item.UpdatedAtMS
		if updatedAtMS <= 0 {
			updatedAtMS = nowMS
		}
		symbol := strings.TrimSpace(item.Symbol)
		if symbol == "" {
			symbol = symbolFromInstID(instID)
		}
		if symbol == "" {
			continue
		}
		state := strings.TrimSpace(item.State)
		if state == "" {
			state = models.PositionStatusClosed
		}
		record := riskHistoryRecord{
			SingletonID:      0,
			Mode:             mode,
			Exchange:         itemExchange,
			Symbol:           symbol,
			InstID:           instID,
			Pos:              "0",
			PosSide:          posSide,
			MgnMode:          mgnMode,
			Margin:           "0",
			Lever:            nonEmptyNumericText(item.Lever),
			AvgPx:            nonEmptyNumericText(item.OpenAvgPx),
			NotionalUSD:      "0",
			MarkPx:           "0",
			LiqPx:            "0",
			TPTriggerPx:      "0",
			SLTriggerPx:      "0",
			OpenTimeMS:       openTimeMS,
			OpenUpdateTimeMS: openTimeMS,
			OpenRowJSON:      "",
			CloseAvgPx:       nonEmptyNumericText(item.CloseAvgPx),
			RealizedPnl:      nonEmptyNumericText(item.RealizedPnl),
			PnlRatio:         nonEmptyNumericText(item.PnlRatio),
			Fee:              nonEmptyNumericText(item.Fee),
			FundingFee:       nonEmptyNumericText(item.FundingFee),
			CloseTimeMS:      closeTimeMS,
			State:            state,
			CloseRowJSON:     strings.TrimSpace(item.CloseRowJSON),
			UpdatedAtMS:      updatedAtMS,
			CreatedAtMS:      updatedAtMS,
		}
		openKey := riskHistoryOpenKey(record.Exchange, record.InstID, record.PosSide, record.MgnMode, record.OpenTimeMS)
		placeholderIDs := make([]int64, 0, 2)
		placeholderIDSet := make(map[int64]struct{}, 2)
		collectPlaceholder := func(candidate riskHistoryRecord) {
			mergeRiskHistoryOpenFields(&record, candidate)
			if candidate.ID > 0 {
				if _, ok := placeholderIDSet[candidate.ID]; ok {
					return
				}
				placeholderIDSet[candidate.ID] = struct{}{}
				placeholderIDs = append(placeholderIDs, candidate.ID)
			}
		}
		if openKey != "" {
			for _, candidate := range existingByOpenKey[openKey] {
				if candidate.CloseTimeMS == record.CloseTimeMS || !isRiskHistoryPlaceholder(candidate) {
					continue
				}
				collectPlaceholder(candidate)
			}
		}
		if len(placeholderIDs) == 0 && strings.EqualFold(strings.TrimSpace(record.PosSide), "net") {
			openKeyNoSide := riskHistoryOpenKeyWithoutSide(record.Exchange, record.InstID, record.MgnMode, record.OpenTimeMS)
			if openKeyNoSide != "" {
				var bestCandidate *riskHistoryRecord
				bestScore := -1
				for _, candidate := range existingByOpenKeyNoSide[openKeyNoSide] {
					if candidate.CloseTimeMS == record.CloseTimeMS || !isRiskHistoryPlaceholder(candidate) {
						continue
					}
					if strings.TrimSpace(record.Symbol) != "" &&
						strings.TrimSpace(candidate.Symbol) != "" &&
						!strings.EqualFold(strings.TrimSpace(record.Symbol), strings.TrimSpace(candidate.Symbol)) {
						continue
					}
					score := riskHistoryPlaceholderScore(candidate)
					if bestCandidate == nil || score > bestScore || (score == bestScore && candidate.UpdatedAtMS > bestCandidate.UpdatedAtMS) {
						tmp := candidate
						bestCandidate = &tmp
						bestScore = score
					}
				}
				if bestCandidate != nil {
					collectPlaceholder(*bestCandidate)
					if strings.EqualFold(strings.TrimSpace(record.PosSide), "net") {
						candidateSide := strings.ToLower(strings.TrimSpace(bestCandidate.PosSide))
						if candidateSide != "" && candidateSide != "net" {
							record.PosSide = candidateSide
						}
					}
				}
			}
		}
		if openKey == "" {
			continue
		}
		openKeyNoSide := riskHistoryOpenKeyWithoutSide(record.Exchange, record.InstID, record.MgnMode, record.OpenTimeMS)
		_, hasCurrentOpen := currentOpenKeys[openKey]
		if !hasCurrentOpen && openKeyNoSide != "" {
			_, hasCurrentOpen = currentOpenKeysNoSide[openKeyNoSide]
		}
		if hasCurrentOpen {
			if err := deleteRiskHistoryRowsByOpenKey(deleteByIDStmt, deletedHistoryIDs, existingByOpenKey[openKey]); err != nil {
				return err
			}
			if openKeyNoSide != "" && strings.EqualFold(strings.TrimSpace(record.PosSide), "net") {
				if err := deleteRiskHistoryRowsByOpenKey(deleteByIDStmt, deletedHistoryIDs, existingByOpenKeyNoSide[openKeyNoSide]); err != nil {
					return err
				}
			}
			continue
		}
		key := riskHistoryKey(record.Exchange, record.InstID, record.PosSide, record.MgnMode, record.OpenTimeMS, record.CloseTimeMS)
		if key == "" {
			continue
		}
		bestExistingCloseTime := int64(0)
		for _, candidate := range existingByOpenKey[openKey] {
			if isRiskHistoryPlaceholder(candidate) {
				continue
			}
			if candidate.CloseTimeMS > bestExistingCloseTime {
				bestExistingCloseTime = candidate.CloseTimeMS
			}
		}
		if bestExistingCloseTime > 0 && bestExistingCloseTime > record.CloseTimeMS {
			for _, candidate := range existingByOpenKey[openKey] {
				if candidate.CloseTimeMS < bestExistingCloseTime {
					if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, candidate.ID); err != nil {
						return err
					}
				}
			}
			if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, placeholderIDs...); err != nil {
				return err
			}
			continue
		}
		if old, ok := existing[key]; ok {
			mergeRiskHistoryOpenFields(&record, old)
			if riskHistoryCloseEqual(old, record) {
				if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, placeholderIDs...); err != nil {
					return err
				}
				for _, candidate := range existingByOpenKey[openKey] {
					if isRiskHistoryPlaceholder(candidate) || candidate.CloseTimeMS == record.CloseTimeMS {
						continue
					}
					if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, candidate.ID); err != nil {
						return err
					}
				}
				continue
			}
		}

		if _, err := upsertStmt.Exec(
			record.SingletonID,
			mode,
			record.Exchange,
			record.Symbol,
			record.InstID,
			record.Pos,
			record.PosSide,
			record.MgnMode,
			record.Margin,
			record.Lever,
			record.AvgPx,
			record.NotionalUSD,
			record.MarkPx,
			record.LiqPx,
			record.TPTriggerPx,
			record.SLTriggerPx,
			record.OpenTimeMS,
			record.OpenUpdateTimeMS,
			record.MaxFloatingLossAmount,
			record.MaxFloatingProfitAmount,
			record.OpenRowJSON,
			record.CloseAvgPx,
			record.RealizedPnl,
			record.PnlRatio,
			record.Fee,
			record.FundingFee,
			record.CloseTimeMS,
			record.State,
			record.CloseRowJSON,
			record.CreatedAtMS,
			record.UpdatedAtMS,
		); err != nil {
			return err
		}
		if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, placeholderIDs...); err != nil {
			return err
		}
		for _, candidate := range existingByOpenKey[openKey] {
			if isRiskHistoryPlaceholder(candidate) || candidate.CloseTimeMS == record.CloseTimeMS {
				continue
			}
			if err := deleteRiskHistoryRowsByIDs(deleteByIDStmt, deletedHistoryIDs, candidate.ID); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *SQLite) SyncRiskPositions(mode, exchange string, openPositions []models.RiskOpenPosition, closedPositions []models.RiskClosedPosition) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	if mode == "" {
		return fmt.Errorf("empty mode")
	}
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	if exchange == "" {
		return fmt.Errorf("empty exchange")
	}
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			s.logger.Error("sync risk positions rollback failed", zap.Error(rbErr))
		}
	}()

	existingRows, err := tx.Query(
		`SELECT singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
		        upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px,
		        open_time_ms, update_time_ms, row_json, max_floating_loss_amount,
		        max_floating_profit_amount, updated_at_ms
		   FROM positions
		  WHERE mode = ? AND exchange = ?;`,
		mode, exchange,
	)
	if err != nil {
		return err
	}
	existing := make(map[string]models.RiskOpenPosition)
	for existingRows.Next() {
		var item models.RiskOpenPosition
		if scanErr := existingRows.Scan(
			&item.SingletonID,
			&item.Mode,
			&item.Exchange,
			&item.Symbol,
			&item.InstID,
			&item.Pos,
			&item.PosSide,
			&item.MgnMode,
			&item.Margin,
			&item.Lever,
			&item.AvgPx,
			&item.Upl,
			&item.UplRatio,
			&item.NotionalUSD,
			&item.MarkPx,
			&item.LiqPx,
			&item.TPTriggerPx,
			&item.SLTriggerPx,
			&item.OpenTimeMS,
			&item.UpdateTimeMS,
			&item.RowJSON,
			&item.MaxFloatingLossAmount,
			&item.MaxFloatingProfitAmount,
			&item.UpdatedAtMS,
		); scanErr != nil {
			_ = existingRows.Close()
			return scanErr
		}
		item.SingletonID = normalizeRiskOpenPositionSingletonID(item.SingletonID, item.RowJSON)
		existing[riskPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode)] = item
	}
	if err := existingRows.Close(); err != nil {
		return err
	}

	upsertStmt, err := tx.Prepare(
		`INSERT INTO positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
		     upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     update_time_ms, max_floating_loss_amount, max_floating_profit_amount, row_json,
		     created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, exchange, inst_id, pos_side, mgn_mode) DO UPDATE SET
		   singleton_id = CASE
		     WHEN positions.singleton_id > 0 THEN positions.singleton_id
		     ELSE excluded.singleton_id
		   END,
		   symbol = excluded.symbol,
		   pos = excluded.pos,
		   margin = excluded.margin,
		   lever = excluded.lever,
		   avg_px = excluded.avg_px,
		   upl = excluded.upl,
		   upl_ratio = excluded.upl_ratio,
		   notional_usd = excluded.notional_usd,
		   mark_px = excluded.mark_px,
		   liq_px = excluded.liq_px,
		   tp_trigger_px = excluded.tp_trigger_px,
		   sl_trigger_px = excluded.sl_trigger_px,
		   open_time_ms = excluded.open_time_ms,
		   update_time_ms = excluded.update_time_ms,
		   max_floating_loss_amount = excluded.max_floating_loss_amount,
		   max_floating_profit_amount = excluded.max_floating_profit_amount,
		   row_json = excluded.row_json,
		   updated_at_ms = excluded.updated_at_ms;`,
	)
	if err != nil {
		return err
	}
	defer upsertStmt.Close()

	current := make(map[string]bool, len(openPositions))
	currentHistoryKeys := make(map[string]struct{}, len(openPositions))
	nowMS := time.Now().UnixMilli()
	for _, item := range openPositions {
		item.Exchange = strings.TrimSpace(item.Exchange)
		if item.Exchange == "" {
			item.Exchange = exchange
		}
		if item.Exchange != exchange {
			continue
		}
		item.Mode = mode
		item.InstID = strings.ToUpper(strings.TrimSpace(item.InstID))
		item.PosSide = strings.ToLower(strings.TrimSpace(item.PosSide))
		item.MgnMode = strings.ToLower(strings.TrimSpace(item.MgnMode))
		if item.InstID == "" || item.PosSide == "" || item.MgnMode == "" {
			continue
		}
		if item.UpdatedAtMS <= 0 {
			item.UpdatedAtMS = nowMS
		}
		item.SingletonID = normalizeRiskOpenPositionSingletonID(item.SingletonID, item.RowJSON)
		createdAtMS := item.UpdatedAtMS
		if old, ok := existing[riskPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode)]; ok {
			if old.UpdatedAtMS > 0 {
				createdAtMS = old.UpdatedAtMS
			}
			if old.SingletonID > 0 {
				item.SingletonID = old.SingletonID
			}
		}
		_, err := upsertStmt.Exec(
			item.SingletonID,
			mode,
			item.Exchange,
			item.Symbol,
			item.InstID,
			item.Pos,
			item.PosSide,
			item.MgnMode,
			item.Margin,
			item.Lever,
			item.AvgPx,
			item.Upl,
			item.UplRatio,
			item.NotionalUSD,
			item.MarkPx,
			item.LiqPx,
			item.TPTriggerPx,
			item.SLTriggerPx,
			item.OpenTimeMS,
			item.UpdateTimeMS,
			item.MaxFloatingLossAmount,
			item.MaxFloatingProfitAmount,
			item.RowJSON,
			createdAtMS,
			item.UpdatedAtMS,
		)
		if err != nil {
			return err
		}
		current[riskPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode)] = true
		if key := riskHistoryOpenKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode, item.OpenTimeMS); key != "" {
			currentHistoryKeys[key] = struct{}{}
		}
	}

	closeMap := make(map[string]models.RiskClosedPosition, len(closedPositions))
	for _, item := range closedPositions {
		itemExchange := strings.ToLower(strings.TrimSpace(item.Exchange))
		if itemExchange == "" {
			itemExchange = exchange
		}
		item.Exchange = itemExchange
		item.InstID = strings.ToUpper(strings.TrimSpace(item.InstID))
		item.PosSide = strings.ToLower(strings.TrimSpace(item.PosSide))
		item.MgnMode = strings.ToLower(strings.TrimSpace(item.MgnMode))
		key := riskPositionKey(item.Exchange, item.InstID, item.PosSide, item.MgnMode)
		if key == "" || !strings.HasPrefix(key, exchange+"|") {
			continue
		}
		if prev, ok := closeMap[key]; !ok || item.CloseTimeMS > prev.CloseTimeMS {
			closeMap[key] = item
		}
	}

	insertHistoryStmt, err := tx.Prepare(
		`INSERT INTO history_positions (
		     singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
		     notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms,
		     open_update_time_ms, max_floating_loss_amount, max_floating_profit_amount, open_row_json,
		     close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee, close_time_ms, state,
		     close_row_json, created_at_ms, updated_at_ms
		 )
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(mode, exchange, inst_id, pos_side, mgn_mode, open_time_ms, close_time_ms) DO UPDATE SET
		   singleton_id = CASE
		     WHEN history_positions.singleton_id > 0 THEN history_positions.singleton_id
		     ELSE excluded.singleton_id
		   END,
		   symbol = excluded.symbol,
		   pos = excluded.pos,
		   margin = excluded.margin,
		   lever = excluded.lever,
		   avg_px = excluded.avg_px,
		   notional_usd = excluded.notional_usd,
		   mark_px = excluded.mark_px,
		   liq_px = excluded.liq_px,
		   tp_trigger_px = excluded.tp_trigger_px,
		   sl_trigger_px = excluded.sl_trigger_px,
		   open_update_time_ms = excluded.open_update_time_ms,
		   max_floating_loss_amount = excluded.max_floating_loss_amount,
		   max_floating_profit_amount = excluded.max_floating_profit_amount,
		   open_row_json = excluded.open_row_json,
		   close_avg_px = excluded.close_avg_px,
		   realized_pnl = excluded.realized_pnl,
		   pnl_ratio = excluded.pnl_ratio,
		   fee = excluded.fee,
		   funding_fee = excluded.funding_fee,
		   state = excluded.state,
		   close_row_json = excluded.close_row_json,
		   updated_at_ms = excluded.updated_at_ms;`,
	)
	if err != nil {
		return err
	}
	defer insertHistoryStmt.Close()

	deleteStmt, err := tx.Prepare(`DELETE FROM positions WHERE mode = ? AND exchange = ? AND inst_id = ? AND pos_side = ? AND mgn_mode = ?;`)
	if err != nil {
		return err
	}
	defer deleteStmt.Close()

	deleteHistoryFinalizedByOpenStmt, err := tx.Prepare(
		`DELETE FROM history_positions
		  WHERE mode = ?
		    AND exchange = ?
		    AND inst_id = ?
		    AND mgn_mode = ?
		    AND open_time_ms = ?
		    AND COALESCE(state, '') <> ?`,
	)
	if err != nil {
		return err
	}
	defer deleteHistoryFinalizedByOpenStmt.Close()

	historyTouched := false
	for openKey := range currentHistoryKeys {
		parts := strings.Split(openKey, "|")
		if len(parts) != 5 {
			continue
		}
		openTimeMS, err := strconv.ParseInt(parts[4], 10, 64)
		if err != nil || openTimeMS <= 0 {
			continue
		}
		result, err := deleteHistoryFinalizedByOpenStmt.Exec(mode, parts[0], parts[1], parts[3], openTimeMS, riskHistoryStateSyncPending)
		if err != nil {
			return err
		}
		if affected, err := result.RowsAffected(); err == nil && affected > 0 {
			historyTouched = true
		}
	}

	for key, openItem := range existing {
		if current[key] {
			continue
		}
		closeItem, ok := closeMap[key]
		if !ok || closeItem.CloseTimeMS <= 0 {
			placeholderCloseTS := nowMS
			createdAtMS := openItem.UpdatedAtMS
			if createdAtMS <= 0 {
				createdAtMS = placeholderCloseTS
			}
			singletonID := normalizeRiskOpenPositionSingletonID(openItem.SingletonID, openItem.RowJSON)
			if _, err := insertHistoryStmt.Exec(
				singletonID,
				mode,
				openItem.Exchange,
				openItem.Symbol,
				openItem.InstID,
				openItem.Pos,
				openItem.PosSide,
				openItem.MgnMode,
				openItem.Margin,
				openItem.Lever,
				openItem.AvgPx,
				openItem.NotionalUSD,
				openItem.MarkPx,
				openItem.LiqPx,
				openItem.TPTriggerPx,
				openItem.SLTriggerPx,
				openItem.OpenTimeMS,
				openItem.UpdateTimeMS,
				openItem.MaxFloatingLossAmount,
				openItem.MaxFloatingProfitAmount,
				openItem.RowJSON,
				firstNonEmptyText(openItem.MarkPx, openItem.AvgPx, "0"),
				"0",
				"0",
				"0",
				"0",
				placeholderCloseTS,
				riskHistoryStateSyncPending,
				"",
				createdAtMS,
				placeholderCloseTS,
			); err != nil {
				return err
			}
			historyTouched = true
			if _, err := deleteStmt.Exec(mode, openItem.Exchange, openItem.InstID, openItem.PosSide, openItem.MgnMode); err != nil {
				return err
			}
			continue
		}
		updatedAtMS := closeItem.UpdatedAtMS
		if updatedAtMS <= 0 {
			updatedAtMS = nowMS
		}
		createdAtMS := openItem.UpdatedAtMS
		if createdAtMS <= 0 {
			createdAtMS = updatedAtMS
		}
		singletonID := normalizeRiskOpenPositionSingletonID(openItem.SingletonID, openItem.RowJSON)
		if _, err := insertHistoryStmt.Exec(
			singletonID,
			mode,
			openItem.Exchange,
			openItem.Symbol,
			openItem.InstID,
			openItem.Pos,
			openItem.PosSide,
			openItem.MgnMode,
			openItem.Margin,
			openItem.Lever,
			openItem.AvgPx,
			openItem.NotionalUSD,
			openItem.MarkPx,
			openItem.LiqPx,
			openItem.TPTriggerPx,
			openItem.SLTriggerPx,
			openItem.OpenTimeMS,
			openItem.UpdateTimeMS,
			openItem.MaxFloatingLossAmount,
			openItem.MaxFloatingProfitAmount,
			openItem.RowJSON,
			closeItem.CloseAvgPx,
			closeItem.RealizedPnl,
			closeItem.PnlRatio,
			closeItem.Fee,
			closeItem.FundingFee,
			closeItem.CloseTimeMS,
			closeItem.State,
			closeItem.CloseRowJSON,
			createdAtMS,
			updatedAtMS,
		); err != nil {
			return err
		}
		historyTouched = true
		if _, err := deleteStmt.Exec(mode, openItem.Exchange, openItem.InstID, openItem.PosSide, openItem.MgnMode); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	if historyTouched {
	}
	return nil
}

func riskPositionKey(exchange, instID, posSide, mgnMode string) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	instID = strings.ToUpper(strings.TrimSpace(instID))
	posSide = strings.ToLower(strings.TrimSpace(posSide))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || posSide == "" || mgnMode == "" {
		return ""
	}
	return exchange + "|" + instID + "|" + posSide + "|" + mgnMode
}

func riskHistoryKey(exchange, instID, posSide, mgnMode string, openTimeMS, closeTimeMS int64) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	instID = strings.ToUpper(strings.TrimSpace(instID))
	posSide = strings.ToLower(strings.TrimSpace(posSide))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || posSide == "" || mgnMode == "" || openTimeMS <= 0 || closeTimeMS <= 0 {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%d|%d", exchange, instID, posSide, mgnMode, openTimeMS, closeTimeMS)
}

func riskHistoryOpenKey(exchange, instID, posSide, mgnMode string, openTimeMS int64) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	instID = strings.ToUpper(strings.TrimSpace(instID))
	posSide = strings.ToLower(strings.TrimSpace(posSide))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || posSide == "" || mgnMode == "" || openTimeMS <= 0 {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%d", exchange, instID, posSide, mgnMode, openTimeMS)
}

func riskHistoryOpenKeyWithoutSide(exchange, instID, mgnMode string, openTimeMS int64) string {
	exchange = strings.ToLower(strings.TrimSpace(exchange))
	instID = strings.ToUpper(strings.TrimSpace(instID))
	mgnMode = strings.ToLower(strings.TrimSpace(mgnMode))
	if exchange == "" || instID == "" || mgnMode == "" || openTimeMS <= 0 {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%d", exchange, instID, mgnMode, openTimeMS)
}

func riskHistoryPlaceholderScore(item riskHistoryRecord) int {
	score := 0
	side := strings.ToLower(strings.TrimSpace(item.PosSide))
	if side != "" && side != "net" {
		score += 8
	}
	if strings.TrimSpace(item.OpenRowJSON) != "" {
		score += 4
	}
	if floatcmp.GT(math.Abs(parseFloatText(item.TPTriggerPx)), 0) ||
		floatcmp.GT(math.Abs(parseFloatText(item.SLTriggerPx)), 0) {
		score += 2
	}
	if floatcmp.GT(math.Abs(item.MaxFloatingProfitAmount), 0) ||
		floatcmp.GT(math.Abs(item.MaxFloatingLossAmount), 0) {
		score += 1
	}
	return score
}

func isRiskHistoryPlaceholder(item riskHistoryRecord) bool {
	if strings.EqualFold(strings.TrimSpace(item.State), riskHistoryStateSyncPending) {
		return true
	}
	if strings.TrimSpace(item.CloseRowJSON) != "" {
		return false
	}
	return !floatcmp.GT(math.Abs(parseFloatText(item.RealizedPnl)), 0) &&
		!floatcmp.GT(math.Abs(parseFloatText(item.PnlRatio)), 0) &&
		!floatcmp.GT(math.Abs(parseFloatText(item.Fee)), 0) &&
		!floatcmp.GT(math.Abs(parseFloatText(item.FundingFee)), 0)
}

func loadRiskOpenHistoryKeySets(tx *sql.Tx, mode, exchange string) (map[string]struct{}, map[string]struct{}, error) {
	rows, err := tx.Query(
		`SELECT exchange, inst_id, pos_side, mgn_mode, open_time_ms
		   FROM positions
		  WHERE mode = ? AND exchange = ?;`,
		mode, exchange,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	full := make(map[string]struct{})
	noSide := make(map[string]struct{})
	for rows.Next() {
		var (
			itemExchange string
			instID       string
			posSide      string
			mgnMode      string
			openTimeMS   int64
		)
		if err := rows.Scan(&itemExchange, &instID, &posSide, &mgnMode, &openTimeMS); err != nil {
			return nil, nil, err
		}
		if key := riskHistoryOpenKey(itemExchange, instID, posSide, mgnMode, openTimeMS); key != "" {
			full[key] = struct{}{}
		}
		if key := riskHistoryOpenKeyWithoutSide(itemExchange, instID, mgnMode, openTimeMS); key != "" {
			noSide[key] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return full, noSide, nil
}

func deleteRiskHistoryRowsByIDs(stmt *sql.Stmt, deleted map[int64]struct{}, ids ...int64) error {
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := deleted[id]; ok {
			continue
		}
		if _, err := stmt.Exec(id); err != nil {
			return err
		}
		deleted[id] = struct{}{}
	}
	return nil
}

func deleteRiskHistoryRowsByOpenKey(stmt *sql.Stmt, deleted map[int64]struct{}, rows []riskHistoryRecord) error {
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		if row.ID > 0 {
			ids = append(ids, row.ID)
		}
	}
	return deleteRiskHistoryRowsByIDs(stmt, deleted, ids...)
}

func bestRiskClosedPosition(a, b models.RiskClosedPosition) models.RiskClosedPosition {
	if b.CloseTimeMS > a.CloseTimeMS {
		return b
	}
	if b.CloseTimeMS < a.CloseTimeMS {
		return a
	}
	if b.UpdatedAtMS > a.UpdatedAtMS {
		return b
	}
	if b.UpdatedAtMS < a.UpdatedAtMS {
		return a
	}
	if len(strings.TrimSpace(b.CloseRowJSON)) > len(strings.TrimSpace(a.CloseRowJSON)) {
		return b
	}
	return a
}

func mergeRiskHistoryOpenFields(dst *riskHistoryRecord, src riskHistoryRecord) {
	if dst == nil {
		return
	}
	if dst.SingletonID <= 0 && src.SingletonID > 0 {
		dst.SingletonID = src.SingletonID
	}
	dst.Symbol = firstNonEmptyText(dst.Symbol, src.Symbol)
	dst.Pos = mergeNumericTextWhenDstZero(dst.Pos, src.Pos)
	dst.Margin = mergeNumericTextWhenDstZero(dst.Margin, src.Margin)
	dst.Lever = mergeNumericTextWhenDstZero(dst.Lever, src.Lever)
	dst.AvgPx = mergeNumericTextWhenDstZero(dst.AvgPx, src.AvgPx)
	dst.NotionalUSD = mergeNumericTextWhenDstZero(dst.NotionalUSD, src.NotionalUSD)
	dst.MarkPx = mergeNumericTextWhenDstZero(dst.MarkPx, src.MarkPx)
	dst.LiqPx = mergeNumericTextWhenDstZero(dst.LiqPx, src.LiqPx)
	dst.TPTriggerPx = mergeNumericTextWhenDstZero(dst.TPTriggerPx, src.TPTriggerPx)
	dst.SLTriggerPx = mergeNumericTextWhenDstZero(dst.SLTriggerPx, src.SLTriggerPx)
	if dst.OpenUpdateTimeMS <= 0 {
		dst.OpenUpdateTimeMS = src.OpenUpdateTimeMS
	}
	if strings.TrimSpace(dst.OpenRowJSON) == "" && strings.TrimSpace(src.OpenRowJSON) != "" {
		dst.OpenRowJSON = strings.TrimSpace(src.OpenRowJSON)
	}
	if src.MaxFloatingLossAmount > dst.MaxFloatingLossAmount {
		dst.MaxFloatingLossAmount = src.MaxFloatingLossAmount
	}
	if src.MaxFloatingProfitAmount > dst.MaxFloatingProfitAmount {
		dst.MaxFloatingProfitAmount = src.MaxFloatingProfitAmount
	}
	if dst.CreatedAtMS <= 0 {
		dst.CreatedAtMS = src.CreatedAtMS
	}
	if dst.CreatedAtMS <= 0 {
		dst.CreatedAtMS = dst.UpdatedAtMS
	}
}

func mergeNumericTextWhenDstZero(dst, src string) string {
	dst = nonEmptyNumericText(dst)
	src = nonEmptyNumericText(src)
	if floatcmp.GT(math.Abs(parseFloatText(dst)), 0) {
		return dst
	}
	if floatcmp.GT(math.Abs(parseFloatText(src)), 0) {
		return src
	}
	return dst
}

func riskHistoryCloseEqual(old, cur riskHistoryRecord) bool {
	return strings.TrimSpace(old.Symbol) == strings.TrimSpace(cur.Symbol) &&
		strings.TrimSpace(old.CloseAvgPx) == strings.TrimSpace(cur.CloseAvgPx) &&
		strings.TrimSpace(old.RealizedPnl) == strings.TrimSpace(cur.RealizedPnl) &&
		strings.TrimSpace(old.PnlRatio) == strings.TrimSpace(cur.PnlRatio) &&
		strings.TrimSpace(old.Fee) == strings.TrimSpace(cur.Fee) &&
		strings.TrimSpace(old.FundingFee) == strings.TrimSpace(cur.FundingFee) &&
		strings.TrimSpace(old.State) == strings.TrimSpace(cur.State) &&
		strings.TrimSpace(old.CloseRowJSON) == strings.TrimSpace(cur.CloseRowJSON)
}

func parseFloatText(raw string) float64 {
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

func formatRiskTimestampMS(ts int64) string {
	if ts <= 0 {
		return ""
	}
	return time.UnixMilli(ts).Format("2006-01-02 15:04:05")
}

func symbolFromInstID(instID string) string {
	instID = strings.ToUpper(strings.TrimSpace(instID))
	if instID == "" {
		return ""
	}
	parts := strings.Split(instID, "-")
	if len(parts) >= 2 && parts[0] != "" && parts[1] != "" {
		return parts[0] + "/" + parts[1]
	}
	for _, quote := range []string{"USDT", "USDC", "BUSD", "USD", "BTC", "ETH"} {
		if !strings.HasSuffix(instID, quote) || len(instID) <= len(quote) {
			continue
		}
		base := strings.TrimSpace(instID[:len(instID)-len(quote)])
		if base == "" {
			continue
		}
		return base + "/" + quote
	}
	return strings.ReplaceAll(instID, "-", "/")
}

func nonEmptyNumericText(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "0"
	}
	return raw
}

func firstNonEmptyText(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func strategyPrimaryTimeframe(meta models.StrategyContextMeta) string {
	meta = models.NormalizeStrategyContextMeta(meta)
	count := len(meta.StrategyTimeframes)
	if count == 0 {
		return ""
	}
	return meta.StrategyTimeframes[count-1]
}

func (s *SQLite) AppendSignalChange(record models.SignalChangeRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	record.Mode = strings.TrimSpace(record.Mode)
	exchange := strings.TrimSpace(record.Exchange)
	symbol := strings.TrimSpace(record.Symbol)
	timeframe := strings.TrimSpace(record.Timeframe)
	strategy := strings.TrimSpace(record.Strategy)
	if record.Mode == "" || exchange == "" || symbol == "" || timeframe == "" || strategy == "" {
		return fmt.Errorf("signal record missing mode/exchange/symbol/timeframe/strategy")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return err
	}
	nowMS := time.Now().UnixMilli()
	eventAtMS := record.EventAtMS
	if eventAtMS <= 0 {
		eventAtMS = nowMS
	}
	createdAtMS := record.CreatedAtMS
	if createdAtMS <= 0 {
		createdAtMS = nowMS
	}
	changedFields := strings.TrimSpace(record.ChangedFields)
	signalJSON := strings.TrimSpace(record.SignalJSON)
	if signalJSON == "" {
		signalJSON = "{}"
	}

	var singletonID any
	if record.SingletonID > 0 {
		singletonID = record.SingletonID
	}
	_, err = s.DB.Exec(
		`INSERT INTO signals (
			singleton_id, mode, exchange_id, symbol_id, timeframe, strategy, strategy_version,
			change_status, changed_fields, signal_json, event_at_ms, created_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		singletonID,
		record.Mode,
		exchangeID,
		symbolID,
		timeframe,
		strategy,
		strings.TrimSpace(record.StrategyVersion),
		record.ChangeStatus,
		changedFields,
		signalJSON,
		eventAtMS,
		createdAtMS,
	)
	if err != nil {
		return err
	}
	return nil
}

func (s *SQLite) ListSignalChangesByPair(mode, exchange, symbol string) (out []models.SignalChangeRecord, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	mode = strings.TrimSpace(mode)
	exchange = strings.TrimSpace(exchange)
	symbol = strings.TrimSpace(symbol)
	if mode == "" || exchange == "" || symbol == "" {
		return nil, nil
	}
	rows, err := s.DB.Query(
		`SELECT
			s.id,
			COALESCE(s.singleton_id, 0),
			s.mode,
			s.exchange_id,
			s.symbol_id,
			s.timeframe,
			s.strategy,
			COALESCE(s.strategy_version, ''),
			s.change_status,
			COALESCE(s.changed_fields, ''),
			COALESCE(s.signal_json, ''),
			COALESCE(s.event_at_ms, 0),
			COALESCE(s.created_at_ms, 0)
		FROM signals s
		JOIN exchanges e ON e.id = s.exchange_id
		JOIN symbols m ON m.id = s.symbol_id
		WHERE s.mode = ? AND e.name = ? AND m.symbol = ?
		ORDER BY s.id ASC;`,
		mode,
		exchange,
		symbol,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()
	for rows.Next() {
		var item models.SignalChangeRecord
		item.Exchange = exchange
		item.Symbol = symbol
		if scanErr := rows.Scan(
			&item.ID,
			&item.SingletonID,
			&item.Mode,
			&item.ExchangeID,
			&item.SymbolID,
			&item.Timeframe,
			&item.Strategy,
			&item.StrategyVersion,
			&item.ChangeStatus,
			&item.ChangedFields,
			&item.SignalJSON,
			&item.EventAtMS,
			&item.CreatedAtMS,
		); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) GetSingleton(id int64, uuid string) (models.SingletonRecord, bool, error) {
	if s == nil || s.DB == nil {
		return models.SingletonRecord{}, false, fmt.Errorf("nil db")
	}
	uuid = strings.TrimSpace(uuid)
	if id <= 0 && uuid == "" {
		return models.SingletonRecord{}, false, fmt.Errorf("singleton query requires id or uuid")
	}

	query := `SELECT id, uuid, version, mode, source, status, created, updated,
	                 closed, heartbeat, lease_expires, start_time, end_time, runtime
	            FROM singleton
	           WHERE 1 = 1`
	args := make([]any, 0, 2)
	if id > 0 {
		query += ` AND id = ?`
		args = append(args, id)
	}
	if uuid != "" {
		query += ` AND uuid = ?`
		args = append(args, uuid)
	}
	query += ` LIMIT 1`

	var (
		record       models.SingletonRecord
		source       sql.NullString
		closed       sql.NullInt64
		heartbeat    sql.NullInt64
		leaseExpires sql.NullInt64
		startTime    sql.NullString
		endTime      sql.NullString
		runtime      sql.NullString
	)
	err := s.DB.QueryRow(query, args...).Scan(
		&record.ID,
		&record.UUID,
		&record.Version,
		&record.Mode,
		&source,
		&record.Status,
		&record.Created,
		&record.Updated,
		&closed,
		&heartbeat,
		&leaseExpires,
		&startTime,
		&endTime,
		&runtime,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return models.SingletonRecord{}, false, nil
	}
	if err != nil {
		return models.SingletonRecord{}, false, err
	}

	if source.Valid {
		value := source.String
		record.Source = &value
	}
	if closed.Valid {
		value := closed.Int64
		record.Closed = &value
	}
	if heartbeat.Valid {
		value := heartbeat.Int64
		record.Heartbeat = &value
	}
	if leaseExpires.Valid {
		value := leaseExpires.Int64
		record.LeaseExpires = &value
	}
	if startTime.Valid {
		value := startTime.String
		record.StartTime = &value
	}
	if endTime.Valid {
		value := endTime.String
		record.EndTime = &value
	}
	if runtime.Valid {
		value := runtime.String
		record.Runtime = &value
	}
	return record, true, nil
}

func (s *SQLite) SaveOHLCV(data models.MarketData) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(data.Exchange, data.Symbol)
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(
		`INSERT INTO ohlcv (exchange_id, symbol_id, timeframe, ts, open, high, low, close, volume)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(exchange_id, symbol_id, timeframe, ts) DO UPDATE SET
		   open = excluded.open,
		   high = excluded.high,
		   low = excluded.low,
		   close = excluded.close,
		   volume = excluded.volume;`,
		exchangeID, symbolID, data.Timeframe, data.OHLCV.TS, data.OHLCV.Open, data.OHLCV.High,
		data.OHLCV.Low, data.OHLCV.Close, data.OHLCV.Volume,
	)
	return err
}

func (s *SQLite) HasOHLCV(exchange, symbol, timeframe string, ts int64) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("nil db")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return false, err
	}
	var found int
	err = s.DB.QueryRow(
		`SELECT 1 FROM ohlcv
		 WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ? AND ts = ?
		 LIMIT 1;`,
		exchangeID, symbolID, timeframe, ts,
	).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLite) ListRecentOHLCV(exchange, symbol, timeframe string, limit int) (out []models.OHLCV, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("invalid limit")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.Query(
		`SELECT ts, open, high, low, close, volume FROM (
			 SELECT ts, open, high, low, close, volume
			 FROM ohlcv
			 WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ?
			 ORDER BY ts DESC
			 LIMIT ?
		 )
		 ORDER BY ts ASC;`,
		exchangeID, symbolID, timeframe, limit,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()
	for rows.Next() {
		var item models.OHLCV
		if err := rows.Scan(&item.TS, &item.Open, &item.High, &item.Low, &item.Close, &item.Volume); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListOHLCVRange(exchange, symbol, timeframe string, start, end time.Time) (out []models.OHLCV, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	startMS := start.UnixMilli()
	endMS := end.UnixMilli()
	if endMS < startMS {
		return nil, fmt.Errorf("invalid time range")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	rows, err := s.DB.Query(
		`SELECT ts, open, high, low, close, volume
		 FROM ohlcv
		 WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ? AND ts >= ? AND ts <= ?
		 ORDER BY ts ASC;`,
		exchangeID, symbolID, timeframe, startMS, endMS,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()
	for rows.Next() {
		var item models.OHLCV
		if scanErr := rows.Scan(&item.TS, &item.Open, &item.High, &item.Low, &item.Close, &item.Volume); scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) DeleteOHLCVBeforeOrEqual(exchange, symbol, timeframe string, ts int64) (int64, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("nil db")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return 0, err
	}
	res, err := s.DB.Exec(
		`DELETE FROM ohlcv WHERE exchange_id = ? AND symbol_id = ? AND timeframe = ? AND ts <= ?;`,
		exchangeID, symbolID, timeframe, ts,
	)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

func (s *SQLite) GetOHLCVBound(exchange, symbol string) (int64, bool, error) {
	if s == nil || s.DB == nil {
		return 0, false, fmt.Errorf("nil db")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	var earliest sql.NullInt64
	if err := s.DB.QueryRow(
		`SELECT earliest_available_ts
		 FROM ohlcv_bounds
		 WHERE exchange_id = ? AND symbol_id = ?;`,
		exchangeID, symbolID,
	).Scan(&earliest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if !earliest.Valid || earliest.Int64 <= 0 {
		return 0, false, nil
	}
	return earliest.Int64, true, nil
}

func (s *SQLite) ListOHLCVTimeframeRanges() (out []OHLCVTimeframeRange, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	rows, err := s.DB.Query(
		`SELECT e.name, sy.symbol, o.timeframe, COUNT(*), MIN(o.ts), MAX(o.ts)
		 FROM ohlcv o
		 JOIN exchanges e ON o.exchange_id = e.id
		 JOIN symbols sy ON o.symbol_id = sy.id
		 GROUP BY e.name, sy.symbol, o.timeframe
		 ORDER BY e.name, sy.symbol, o.timeframe;`,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()

	for rows.Next() {
		var item OHLCVTimeframeRange
		if err := rows.Scan(&item.Exchange, &item.Symbol, &item.Timeframe, &item.Bars, &item.StartTS, &item.EndTS); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) ListOHLCVBounds() (out []OHLCVBoundRecord, err error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("nil db")
	}
	rows, err := s.DB.Query(
		`SELECT e.name, sy.symbol, b.earliest_available_ts
		 FROM ohlcv_bounds b
		 JOIN exchanges e ON b.exchange_id = e.id
		 JOIN symbols sy ON b.symbol_id = sy.id
		 ORDER BY e.name, sy.symbol;`,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				err = fmt.Errorf("close rows: %v; %w", closeErr, err)
			}
		}
	}()

	for rows.Next() {
		var item OHLCVBoundRecord
		if err := rows.Scan(&item.Exchange, &item.Symbol, &item.EarliestAvailableTS); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *SQLite) UpsertOHLCVBound(exchange, symbol string, earliestAvailableTS int64) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("nil db")
	}
	if earliestAvailableTS <= 0 {
		return fmt.Errorf("invalid earliest_available_ts")
	}
	exchangeID, symbolID, err := s.lookupSymbolIDs(exchange, symbol)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Unix()
	_, err = s.DB.Exec(
		`INSERT INTO ohlcv_bounds (exchange_id, symbol_id, earliest_available_ts, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(exchange_id, symbol_id) DO UPDATE SET
		   earliest_available_ts = CASE
		     WHEN ohlcv_bounds.earliest_available_ts IS NULL THEN excluded.earliest_available_ts
		     WHEN excluded.earliest_available_ts < ohlcv_bounds.earliest_available_ts THEN excluded.earliest_available_ts
		     ELSE ohlcv_bounds.earliest_available_ts
		   END,
		   updated_at = excluded.updated_at;`,
		exchangeID, symbolID, earliestAvailableTS, now,
	)
	return err
}

func (s *SQLite) lookupSymbolIDs(exchange, symbol string) (int64, int64, error) {
	if s == nil || s.DB == nil {
		return 0, 0, fmt.Errorf("nil db")
	}
	var exchangeID, symbolID int64
	if err := s.DB.QueryRow(
		`SELECT e.id, s.id
		 FROM exchanges e
		 JOIN symbols s ON s.exchange_id = e.id
		 WHERE e.name = ? AND s.symbol = ?;`,
		exchange, symbol,
	).Scan(&exchangeID, &symbolID); err != nil {
		return 0, 0, err
	}
	return exchangeID, symbolID, nil
}

func (s *SQLite) lookupExchangeID(exchange string) (int64, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("nil db")
	}
	var exchangeID int64
	if err := s.DB.QueryRow(
		`SELECT id FROM exchanges WHERE name = ?;`,
		exchange,
	).Scan(&exchangeID); err != nil {
		return 0, err
	}
	return exchangeID, nil
}
