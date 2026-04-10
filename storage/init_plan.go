package storage

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type SchemaPlan struct {
	CreateTables []string
	AddColumns   []ColumnPlan
	Rebuild      []RebuildPlan
}

type ColumnPlan struct {
	Table  string
	Column string
	Type   string
}

type RebuildPlan struct {
	Table   string
	Reasons []string
}

type DefaultPlan struct {
	Inserts   []SQLAction
	Patches   []SQLAction
	Overrides []SQLAction
}

type SQLAction struct {
	Summary string
	Query   string
	Args    []any
}

type columnSpec struct {
	Name        string
	Type        string
	CompareType string
}

type tableSpec struct {
	Name       string
	CreateSQL  string
	PrimaryKey []string
	Columns    []columnSpec
}

type defaultConfig struct {
	name   string
	value  string
	common string
}

type defaultExchange struct {
	name         string
	apiKey       string
	rateLimitMS  int
	ohlcvLimit   int
	volumeFilter float64
	marketProxy  string
	tradeProxy   string
	timeframes   string
	active       bool
}

type defaultSymbol struct {
	exchange   string
	symbol     string
	base       string
	quote      string
	typ        string
	timeframes string
	active     bool
	dynamic    bool
}

func expectedSchema() []tableSpec {
	return []tableSpec{
		{
			Name: "exchanges",
			CreateSQL: `CREATE TABLE IF NOT EXISTS exchanges (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY", CompareType: "INTEGER"},
				{Name: "name", Type: "TEXT NOT NULL UNIQUE", CompareType: "TEXT"},
				{Name: "api_key", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "ohlcv_limit", Type: "INTEGER NOT NULL DEFAULT 300", CompareType: "INTEGER"},
				{Name: "rate_limit", Type: "INTEGER NOT NULL DEFAULT 100", CompareType: "INTEGER"},
				{Name: "volume_filter", Type: "REAL NOT NULL DEFAULT 1", CompareType: "REAL"},
				{Name: "market_proxy", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "trade_proxy", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "timeframes", Type: `TEXT NOT NULL DEFAULT '["3m","15m","1h"]'`, CompareType: "TEXT"},
				{Name: "active", Type: "INTEGER NOT NULL DEFAULT 1", CompareType: "INTEGER"},
				{Name: "created_at", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "updated_at", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
			},
		},
		{
			Name: "symbols",
			CreateSQL: `CREATE TABLE IF NOT EXISTS symbols (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY", CompareType: "INTEGER"},
				{Name: "exchange_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "base", Type: "TEXT", CompareType: "TEXT"},
				{Name: "quote", Type: "TEXT", CompareType: "TEXT"},
				{Name: "type", Type: "TEXT", CompareType: "TEXT"},
				{Name: "timeframes", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "active", Type: "INTEGER NOT NULL DEFAULT 1", CompareType: "INTEGER"},
				{Name: "dynamic", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "ohlcv",
			CreateSQL: `CREATE TABLE IF NOT EXISTS ohlcv (
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
			PrimaryKey: []string{"exchange_id", "symbol_id", "timeframe", "ts"},
			Columns: []columnSpec{
				{Name: "exchange_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "symbol_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "timeframe", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "ts", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "open", Type: "REAL NOT NULL", CompareType: "REAL"},
				{Name: "high", Type: "REAL NOT NULL", CompareType: "REAL"},
				{Name: "low", Type: "REAL NOT NULL", CompareType: "REAL"},
				{Name: "close", Type: "REAL NOT NULL", CompareType: "REAL"},
				{Name: "volume", Type: "REAL NOT NULL", CompareType: "REAL"},
			},
		},
		{
			Name: "ohlcv_bounds",
			CreateSQL: `CREATE TABLE IF NOT EXISTS ohlcv_bounds (
				exchange_id INTEGER NOT NULL,
				symbol_id INTEGER NOT NULL,
				earliest_available_ts INTEGER,
				updated_at INTEGER NOT NULL,
				PRIMARY KEY (exchange_id, symbol_id),
				FOREIGN KEY(exchange_id) REFERENCES exchanges(id),
				FOREIGN KEY(symbol_id) REFERENCES symbols(id)
			);`,
			PrimaryKey: []string{"exchange_id", "symbol_id"},
			Columns: []columnSpec{
				{Name: "exchange_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "symbol_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "earliest_available_ts", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "updated_at", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
			},
		},
		{
			Name: "positions",
			CreateSQL: `CREATE TABLE IF NOT EXISTS positions (
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
			PrimaryKey: []string{"mode", "exchange", "inst_id", "pos_side", "mgn_mode"},
			Columns: []columnSpec{
				{Name: "singleton_id", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "inst_id", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "pos", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "pos_side", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "mgn_mode", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "margin", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "lever", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "avg_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "upl", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "upl_ratio", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "notional_usd", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "mark_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "liq_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "tp_trigger_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "sl_trigger_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "open_time_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "update_time_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "max_floating_loss_amount", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "max_floating_profit_amount", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "row_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "history_positions",
			CreateSQL: `CREATE TABLE IF NOT EXISTS history_positions (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "singleton_id", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "inst_id", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "pos", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "pos_side", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "mgn_mode", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "margin", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "lever", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "avg_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "notional_usd", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "mark_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "liq_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "tp_trigger_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "sl_trigger_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "open_time_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "open_update_time_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "max_floating_loss_amount", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "max_floating_profit_amount", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "open_row_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "close_avg_px", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "realized_pnl", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "pnl_ratio", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "fee", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "funding_fee", Type: "TEXT NOT NULL DEFAULT '0'", CompareType: "TEXT"},
				{Name: "close_time_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "state", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "close_row_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "orders",
			CreateSQL: `CREATE TABLE IF NOT EXISTS orders (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "attempt_id", Type: "TEXT NOT NULL UNIQUE", CompareType: "TEXT"},
				{Name: "singleton_uuid", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "source", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "inst_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "action", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "order_type", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "position_side", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "margin_mode", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "size", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "leverage_multiplier", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "take_profit_price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "stop_loss_price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "client_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "strategy", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "result_status", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "fail_source", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "fail_stage", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "fail_reason", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_code", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_message", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_algo_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "has_side_effect", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "step_results_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "request_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "response_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "started_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "finished_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "duration_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "manual_orders",
			CreateSQL: `CREATE TABLE IF NOT EXISTS manual_orders (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				mode TEXT NOT NULL DEFAULT '',
				exchange TEXT NOT NULL DEFAULT '',
				symbol TEXT NOT NULL DEFAULT '',
				inst_id TEXT NOT NULL DEFAULT '',
				timeframe TEXT NOT NULL DEFAULT '',
				position_side TEXT NOT NULL DEFAULT '',
				margin_mode TEXT NOT NULL DEFAULT '',
				order_type TEXT NOT NULL DEFAULT '',
				status TEXT NOT NULL DEFAULT '',
				strategy_name TEXT NOT NULL DEFAULT '',
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "inst_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "timeframe", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "position_side", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "margin_mode", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "order_type", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "status", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "strategy_name", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "strategy_version", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "strategy_timeframes", Type: `TEXT NOT NULL DEFAULT '[]'`, CompareType: "TEXT"},
				{Name: "combo_key", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "group_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "leverage_multiplier", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "amount", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "size", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "take_profit_price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "stop_loss_price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "client_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange_algo_order_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "position_id", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "entry_price", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "filled_size", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "error_message", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "decision_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "metadata_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "submitted_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "filled_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_checked_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "risk_decisions",
			CreateSQL: `CREATE TABLE IF NOT EXISTS risk_decisions (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "singleton_id", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "singleton_uuid", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "timeframe", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "strategy", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "combo_key", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "group_id", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "signal_action", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "high_side", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "decision_action", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "result_status", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "reject_reason", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "event_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "trigger_timestamp_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "trending_timestamp_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "signal_json", Type: `TEXT NOT NULL DEFAULT '{}'`, CompareType: "TEXT"},
				{Name: "decision_json", Type: `TEXT NOT NULL DEFAULT '{}'`, CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "singleton",
			CreateSQL: `CREATE TABLE IF NOT EXISTS singleton (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY", CompareType: "INTEGER"},
				{Name: "uuid", Type: "TEXT NOT NULL UNIQUE", CompareType: "TEXT"},
				{Name: "version", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "mode", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "source", Type: "TEXT", CompareType: "TEXT"},
				{Name: "status", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "created", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "updated", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "closed", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "heartbeat", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "lease_expires", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "start_time", Type: "TEXT", CompareType: "TEXT"},
				{Name: "end_time", Type: "TEXT", CompareType: "TEXT"},
				{Name: "runtime", Type: "TEXT", CompareType: "TEXT"},
			},
		},
		{
			Name: "config",
			CreateSQL: `CREATE TABLE IF NOT EXISTS config (
				name TEXT PRIMARY KEY,
				value TEXT NOT NULL,
				common TEXT NOT NULL
			);`,
			PrimaryKey: []string{"name"},
			Columns: []columnSpec{
				{Name: "name", Type: "TEXT PRIMARY KEY", CompareType: "TEXT"},
				{Name: "value", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "common", Type: "TEXT NOT NULL", CompareType: "TEXT"},
			},
		},
		{
			Name: "signals",
			CreateSQL: `CREATE TABLE IF NOT EXISTS signals (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "singleton_id", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "exchange_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "symbol_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "timeframe", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "strategy", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "strategy_version", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "change_status", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "changed_fields", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "signal_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "event_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "backtest_tasks",
			CreateSQL: `CREATE TABLE IF NOT EXISTS backtest_tasks (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER PRIMARY KEY AUTOINCREMENT", CompareType: "INTEGER"},
				{Name: "status", Type: "TEXT NOT NULL DEFAULT 'pending'", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "display_symbol", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "chart_timeframe", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "trade_timeframes", Type: `TEXT NOT NULL DEFAULT '[]'`, CompareType: "TEXT"},
				{Name: "range_start_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "range_end_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "price_low", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "price_high", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "selection_direction", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "source", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "history_bars", Type: "INTEGER NOT NULL DEFAULT 500", CompareType: "INTEGER"},
				{Name: "singleton_id", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "singleton_uuid", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "pid", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "error_message", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "started_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "finished_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "risk_account_states",
			CreateSQL: `CREATE TABLE IF NOT EXISTS risk_account_states (
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
			PrimaryKey: []string{"mode", "exchange"},
			Columns: []columnSpec{
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "trade_date", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "total_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "funding_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "trading_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "per_trade_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "daily_loss_limit_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "daily_realized_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "daily_closed_profit_usdt", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "risk_symbol_cooldowns",
			CreateSQL: `CREATE TABLE IF NOT EXISTS risk_symbol_cooldowns (
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
			PrimaryKey: []string{"mode", "exchange", "symbol"},
			Columns: []columnSpec{
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "consecutive_stop_loss", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "window_start_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_stop_loss_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "cooldown_until_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_processed_close_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "risk_trend_groups",
			CreateSQL: `CREATE TABLE IF NOT EXISTS risk_trend_groups (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "strategy", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "primary_timeframe", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "side", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "anchor_trending_timestamp_ms", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "state", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "lock_stage", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "selected_candidate_key", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "incumbent_leader_key", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "incumbent_leader_score", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "incumbent_leader_closed_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "first_entry_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_entry_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "entry_count", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "finish_reason", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "created_at_ms", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "finished_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
		{
			Name: "risk_trend_group_candidates",
			CreateSQL: `CREATE TABLE IF NOT EXISTS risk_trend_group_candidates (
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
			PrimaryKey: []string{"id"},
			Columns: []columnSpec{
				{Name: "id", Type: "INTEGER", CompareType: "INTEGER"},
				{Name: "mode", Type: "TEXT NOT NULL DEFAULT 'live'", CompareType: "TEXT"},
				{Name: "group_id", Type: "INTEGER NOT NULL", CompareType: "INTEGER"},
				{Name: "candidate_key", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "exchange", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "symbol", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "candidate_state", Type: "TEXT NOT NULL", CompareType: "TEXT"},
				{Name: "is_selected", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "priority_score", Type: "REAL NOT NULL DEFAULT 0", CompareType: "REAL"},
				{Name: "score_json", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "first_seen_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_seen_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "entered_count", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "first_entry_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_entry_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_exit_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "has_open_position", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_signal_action", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_high_side", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "last_mid_side", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "trending_timestamp_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
				{Name: "exit_reason", Type: "TEXT NOT NULL DEFAULT ''", CompareType: "TEXT"},
				{Name: "updated_at_ms", Type: "INTEGER NOT NULL DEFAULT 0", CompareType: "INTEGER"},
			},
		},
	}
}

func defaultConfigs() []defaultConfig {
	return []defaultConfig{
		{name: "app_name", value: "gobot", common: "Application instance name; used as log file prefix."},
		{name: "log_path", value: "./logs/", common: "Log directory path."},
		{name: "log_level", value: "0", common: "Log level: -1(debug), 0(info), 1(warn), 2(error), 3(panic), 4(fatal)."},
		{name: "with_console", value: "1", common: "Print logs to console: 0=off, 1=on."},
		{name: "with_log_file", value: "1", common: "Write logs to file: 0=off, 1=on."},
		{name: "rotate_time", value: "24", common: "Log rotate interval (hours)."},
		{name: "max_age", value: "7", common: "Log max age (days)."},
		{name: "dynamic_market", value: "1", common: "Enable dynamic market queue: 0=off, 1=on."},
		{name: "exchange", value: `{"fetch_unclosed_ohlcv":false}`, common: "Exchange runtime config in JSON. Currently supports fetch_unclosed_ohlcv only."},
		{name: "exporter_address", value: "http://127.0.0.1:8081", common: "Exporter base address, e.g. http://127.0.0.1:8081."},
		{name: "exporter_ws_origin_patterns", value: "example.com,127.0.0.1:3100,localhost:3100,127.0.0.1:5173,localhost:5173", common: "WebSocket Origin allowlist, comma-separated host[:port], e.g. example.com,localhost:3100."},
		{name: "history_policy", value: defaultHistoryPolicyConfigValue(), common: "OHLCV history retention policy in JSON: max_history_bars with periodic cleanup schedule."},
		{name: "strategy", value: `{"live":["turtle"],"paper":["turtle"],"back-test":["turtle"],"combo":[{"timeframes":["3m","15m","1h"],"trade_enabled":true}]}`, common: "Runtime strategy config in JSON: per-mode strategy list plus combo routing, keys=live/paper/back-test/combo."},
		{name: "risk", value: `{"allow_hedge":false,"allow_scale_in":false,"max_open_positions":5,"trend_guard":{"enabled":true,"mode":"grouped","max_start_lag_bars":12,"leader_min_priority_score":50},"tp":{"mode":"fixed","default_pct":0.2,"only_raise_on_update":true},"sl":{"max_loss_pct":0.05,"only_raise_on_update":true,"require_signal":true},"leverage":{"min":1,"max":50},"account":{"currency":"USDT","baseline_usdt":0},"per_trade":{"ratio":0.1},"symbol_cooldown":{"enabled":true,"consecutive_stop_loss":2,"cooldown":"6h","window":"24h"},"trade_cooldown":{"enabled":true,"loss_ratio_of_per_trade":0.5}}`, common: "Risk config in JSON: hedge/scale-in switches, trend guard mode, TP/SL, leverage, sizing, and cooldown rules."},
	}
}

func defaultExchanges() []defaultExchange {
	return []defaultExchange{
		// {name: "binance", apiKey: "{}", rateLimitMS: 100, ohlcvLimit: 300, volumeFilter: 10, timeframes: `["3m","15m","1h"]`, active: true},
		{name: "okx", apiKey: `{"api_key":"","secret_key":"","passphrase":""}`, rateLimitMS: 100, ohlcvLimit: 300, volumeFilter: 1, timeframes: `["3m","15m","1h"]`, active: true},
		//{name: "bitget", apiKey: "{}", rateLimitMS: 100, ohlcvLimit: 100, volumeFilter: 5, timeframes: `["3m","15m","1h"]`, active: true},
		//{name: "hyperliquid", apiKey: "{}", rateLimitMS: 100, ohlcvLimit: 100, volumeFilter: 5, timeframes: `["3m","15m","1h"]`, active: true},
	}
}

func defaultSymbols() []defaultSymbol {
	return []defaultSymbol{
		// {exchange: "binance", symbol: "BTC/USDT", base: "BTC", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		// {exchange: "binance", symbol: "ETH/USDT", base: "ETH", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		// {exchange: "binance", symbol: "SOL/USDT", base: "SOL", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		{exchange: "okx", symbol: "BTC/USDT", base: "BTC", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		{exchange: "okx", symbol: "ETH/USDT", base: "ETH", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		{exchange: "okx", symbol: "SOL/USDT", base: "SOL", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "bitget", symbol: "BTC/USDT", base: "BTC", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "bitget", symbol: "ETH/USDT", base: "ETH", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "bitget", symbol: "SOL/USDT", base: "SOL", quote: "USDT", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "hyperliquid", symbol: "BTC/USDC", base: "BTC", quote: "USDC", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "hyperliquid", symbol: "ETH/USDC", base: "ETH", quote: "USDC", typ: "swap", timeframes: ``, active: true, dynamic: false},
		//{exchange: "hyperliquid", symbol: "SOL/USDC", base: "SOL", quote: "USDC", typ: "swap", timeframes: ``, active: true, dynamic: false},
	}
}

func PlanSchema(db *sql.DB) (SchemaPlan, error) {
	if db == nil {
		return SchemaPlan{}, fmt.Errorf("nil db")
	}
	existing, err := listTables(db)
	if err != nil {
		return SchemaPlan{}, err
	}

	plan := SchemaPlan{}
	for _, spec := range expectedSchema() {
		if !existing[spec.Name] {
			plan.CreateTables = append(plan.CreateTables, spec.Name)
			continue
		}

		columns, err := loadTableColumns(db, spec.Name)
		if err != nil {
			return SchemaPlan{}, err
		}
		if len(spec.PrimaryKey) > 0 {
			actualPK, err := loadTablePrimaryKey(db, spec.Name)
			if err != nil {
				return SchemaPlan{}, err
			}
			if !samePrimaryKey(spec.PrimaryKey, actualPK) {
				reasons := []string{fmt.Sprintf("primary key mismatch: expected %s got %s", strings.Join(spec.PrimaryKey, ", "), strings.Join(actualPK, ", "))}
				plan.Rebuild = append(plan.Rebuild, RebuildPlan{Table: spec.Name, Reasons: reasons})
				continue
			}
		}
		expected := make(map[string]columnSpec)
		for _, col := range spec.Columns {
			expected[col.Name] = col
		}

		var missing []columnSpec
		var extra []string
		var typeMismatch []string

		for name, col := range expected {
			actualType, ok := columns[name]
			if !ok {
				missing = append(missing, col)
				continue
			}
			if !sameType(col.CompareType, actualType) {
				typeMismatch = append(typeMismatch, fmt.Sprintf("%s(%s!=%s)", name, normalizeType(actualType), col.CompareType))
			}
		}
		for name := range columns {
			if _, ok := expected[name]; !ok {
				extra = append(extra, name)
			}
		}

		if len(extra) > 0 || len(typeMismatch) > 0 {
			reasons := make([]string, 0, len(extra)+len(typeMismatch))
			if len(extra) > 0 {
				reasons = append(reasons, fmt.Sprintf("extra columns: %s", strings.Join(extra, ", ")))
			}
			if len(typeMismatch) > 0 {
				reasons = append(reasons, fmt.Sprintf("type mismatches: %s", strings.Join(typeMismatch, ", ")))
			}
			plan.Rebuild = append(plan.Rebuild, RebuildPlan{Table: spec.Name, Reasons: reasons})
			continue
		}

		for _, col := range missing {
			plan.AddColumns = append(plan.AddColumns, ColumnPlan{Table: spec.Name, Column: col.Name, Type: col.Type})
		}
	}
	return plan, nil
}

func ApplySchemaPlan(db *sql.DB, plan SchemaPlan) (err error) {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !plan.HasChanges() {
		return nil
	}
	if err := ApplySchemaAutoPlan(db, plan); err != nil {
		return err
	}
	if err := ApplySchemaRebuildPlan(db, plan); err != nil {
		return err
	}
	return nil
}

func ApplySchemaAutoPlan(db *sql.DB, plan SchemaPlan) (err error) {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !plan.HasAutoChanges() {
		return nil
	}

	specByName := make(map[string]tableSpec)
	for _, spec := range expectedSchema() {
		specByName[spec.Name] = spec
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = rbErr
			} else {
				err = fmt.Errorf("schema rollback failed: %v; %w", rbErr, err)
			}
		}
	}()

	for _, name := range plan.CreateTables {
		spec, ok := specByName[name]
		if !ok {
			return fmt.Errorf("unknown table: %s", name)
		}
		if _, err := tx.Exec(spec.CreateSQL); err != nil {
			return err
		}
	}

	for _, add := range plan.AddColumns {
		if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", add.Table, add.Column, add.Type)); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func ApplySchemaRebuildPlan(db *sql.DB, plan SchemaPlan) (err error) {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if !plan.HasRebuild() {
		return nil
	}

	specByName := make(map[string]tableSpec)
	for _, spec := range expectedSchema() {
		specByName[spec.Name] = spec
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = rbErr
			} else {
				err = fmt.Errorf("schema rollback failed: %v; %w", rbErr, err)
			}
		}
	}()

	if _, err := tx.Exec("PRAGMA foreign_keys = OFF;"); err != nil {
		return err
	}

	for _, rebuild := range plan.Rebuild {
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", rebuild.Table)); err != nil {
			return err
		}
		spec, ok := specByName[rebuild.Table]
		if !ok {
			return fmt.Errorf("unknown table: %s", rebuild.Table)
		}
		if _, err := tx.Exec(spec.CreateSQL); err != nil {
			return err
		}
	}

	if _, err := tx.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

type runtimeModeTableMigration struct {
	Table     string
	CopySQL   string
	NeedsMode bool
}

func ApplyRuntimeModeIsolationMigrations(db *sql.DB) (err error) {
	if db == nil {
		return fmt.Errorf("nil db")
	}

	specByName := make(map[string]tableSpec)
	for _, spec := range expectedSchema() {
		specByName[spec.Name] = spec
	}

	migrations := []runtimeModeTableMigration{
		{
			Table: "risk_account_states",
			CopySQL: `INSERT INTO risk_account_states__mode_tmp (
				mode, exchange, trade_date, total_usdt, funding_usdt, trading_usdt, per_trade_usdt,
				daily_loss_limit_usdt, daily_realized_usdt, daily_closed_profit_usdt, updated_at_ms
			)
			SELECT 'live', exchange, trade_date, total_usdt, funding_usdt, trading_usdt, per_trade_usdt,
			       daily_loss_limit_usdt, daily_realized_usdt, daily_closed_profit_usdt, updated_at_ms
			  FROM risk_account_states;`,
			NeedsMode: true,
		},
		{
			Table: "risk_symbol_cooldowns",
			CopySQL: `INSERT INTO risk_symbol_cooldowns__mode_tmp (
				mode, exchange, symbol, consecutive_stop_loss, window_start_at_ms, last_stop_loss_at_ms,
				cooldown_until_ms, last_processed_close_ms, updated_at_ms
			)
			SELECT 'live', exchange, symbol, consecutive_stop_loss, window_start_at_ms, last_stop_loss_at_ms,
			       cooldown_until_ms, last_processed_close_ms, updated_at_ms
			  FROM risk_symbol_cooldowns;`,
			NeedsMode: true,
		},
		{
			Table: "risk_trend_groups",
			CopySQL: `INSERT INTO risk_trend_groups__mode_tmp (
				id, mode, strategy, primary_timeframe, side, anchor_trending_timestamp_ms, state, lock_stage,
				selected_candidate_key, incumbent_leader_key, incumbent_leader_score, incumbent_leader_closed_at_ms,
				first_entry_at_ms, last_entry_at_ms, entry_count, finish_reason, created_at_ms, updated_at_ms, finished_at_ms
			)
			SELECT id, 'live', strategy, primary_timeframe, side, anchor_trending_timestamp_ms, state, lock_stage,
			       selected_candidate_key, incumbent_leader_key, incumbent_leader_score, incumbent_leader_closed_at_ms,
			       first_entry_at_ms, last_entry_at_ms, entry_count, finish_reason, created_at_ms, updated_at_ms, finished_at_ms
			  FROM risk_trend_groups;`,
			NeedsMode: true,
		},
		{
			Table: "risk_trend_group_candidates",
			CopySQL: `INSERT INTO risk_trend_group_candidates__mode_tmp (
				id, mode, group_id, candidate_key, exchange, symbol, candidate_state, is_selected, priority_score,
				score_json, first_seen_at_ms, last_seen_at_ms, entered_count, first_entry_at_ms, last_entry_at_ms,
				last_exit_at_ms, has_open_position, last_signal_action, last_high_side, last_mid_side,
				trending_timestamp_ms, exit_reason, updated_at_ms
			)
			SELECT id, 'live', group_id, candidate_key, exchange, symbol, candidate_state, is_selected, priority_score,
			       score_json, first_seen_at_ms, last_seen_at_ms, entered_count, first_entry_at_ms, last_entry_at_ms,
			       last_exit_at_ms, has_open_position, last_signal_action, last_high_side, last_mid_side,
			       trending_timestamp_ms, exit_reason, updated_at_ms
			  FROM risk_trend_group_candidates;`,
			NeedsMode: true,
		},
		{
			Table: "positions",
			CopySQL: `INSERT INTO positions__mode_tmp (
				singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl,
				upl_ratio, notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, update_time_ms,
				max_floating_loss_amount, max_floating_profit_amount, row_json, created_at_ms, updated_at_ms
			)
			SELECT singleton_id,
			       COALESCE((SELECT mode FROM singleton WHERE singleton.id = positions.singleton_id), 'live'),
			       exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, upl, upl_ratio,
			       notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, update_time_ms,
			       max_floating_loss_amount, max_floating_profit_amount, row_json, created_at_ms, updated_at_ms
			  FROM positions;`,
			NeedsMode: true,
		},
		{
			Table: "history_positions",
			CopySQL: `INSERT INTO history_positions__mode_tmp (
				id, singleton_id, mode, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
				notional_usd, mark_px, liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, open_update_time_ms,
				max_floating_loss_amount, max_floating_profit_amount, open_row_json, close_avg_px, realized_pnl,
				pnl_ratio, fee, funding_fee, close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
			)
			SELECT id, singleton_id,
			       COALESCE((SELECT mode FROM singleton WHERE singleton.id = history_positions.singleton_id), 'live'),
			       exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px, notional_usd, mark_px,
			       liq_px, tp_trigger_px, sl_trigger_px, open_time_ms, open_update_time_ms, max_floating_loss_amount,
			       max_floating_profit_amount, open_row_json, close_avg_px, realized_pnl, pnl_ratio, fee, funding_fee,
			       close_time_ms, state, close_row_json, created_at_ms, updated_at_ms
			  FROM history_positions;`,
			NeedsMode: true,
		},
		{
			Table: "signals",
			CopySQL: `INSERT INTO signals__mode_tmp (
				id, singleton_id, mode, exchange_id, symbol_id, timeframe, strategy, strategy_version,
				change_status, changed_fields, signal_json, event_at_ms, created_at_ms
			)
			SELECT id, singleton_id,
			       COALESCE((SELECT mode FROM singleton WHERE singleton.id = signals.singleton_id), 'live'),
			       exchange_id, symbol_id, timeframe, strategy, strategy_version, change_status, changed_fields,
			       signal_json, event_at_ms, created_at_ms
			  FROM signals;`,
			NeedsMode: true,
		},
	}

	pending := make([]runtimeModeTableMigration, 0, len(migrations))
	for _, migration := range migrations {
		ok, err := hasTable(db, migration.Table)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		hasMode, err := hasColumn(db, migration.Table, "mode")
		if err != nil {
			return err
		}
		if migration.NeedsMode && hasMode {
			continue
		}
		pending = append(pending, migration)
	}
	if len(pending) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = rbErr
			} else {
				err = fmt.Errorf("runtime mode isolation rollback failed: %v; %w", rbErr, err)
			}
		}
	}()

	if _, err := tx.Exec("PRAGMA foreign_keys = OFF;"); err != nil {
		return err
	}

	for _, migration := range pending {
		spec, ok := specByName[migration.Table]
		if !ok {
			return fmt.Errorf("unknown runtime mode migration table: %s", migration.Table)
		}
		tempTable := migration.Table + "__mode_tmp"
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tempTable)); err != nil {
			return err
		}
		if _, err := tx.Exec(tempTableCreateSQL(spec, tempTable)); err != nil {
			return err
		}
		if _, err := tx.Exec(migration.CopySQL); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("DROP TABLE %s;", migration.Table)); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s;", tempTable, migration.Table)); err != nil {
			return err
		}
	}

	if _, err := tx.Exec("PRAGMA foreign_keys = ON;"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func PlanDefaults(db *sql.DB, schemaPlan SchemaPlan) (DefaultPlan, error) {
	if db == nil {
		return DefaultPlan{}, fmt.Errorf("nil db")
	}

	rebuild := make(map[string]bool)
	for _, item := range schemaPlan.Rebuild {
		rebuild[item.Table] = true
	}

	plan := DefaultPlan{}
	if ok, err := hasTable(db, "config"); err != nil {
		return DefaultPlan{}, err
	} else if ok && !rebuild["config"] {
		for _, cfg := range defaultConfigs() {
			var value, common string
			err := db.QueryRow(`SELECT value, common FROM config WHERE name = ?;`, cfg.name).Scan(&value, &common)
			if err != nil {
				if err == sql.ErrNoRows {
					plan.Inserts = append(plan.Inserts, SQLAction{
						Summary: fmt.Sprintf("insert default config.%s", cfg.name),
						Query:   `INSERT INTO config (name, value, common) VALUES (?, ?, ?);`,
						Args:    []any{cfg.name, cfg.value, cfg.common},
					})
					continue
				}
				return DefaultPlan{}, err
			}
			if common != cfg.common {
				plan.Patches = append(plan.Patches, SQLAction{
					Summary: fmt.Sprintf("patch config.%s common metadata", cfg.name),
					Query:   `UPDATE config SET common = ? WHERE name = ?;`,
					Args:    []any{cfg.common, cfg.name},
				})
			}
			patches, overrides, err := classifyJSONStringDefault(
				fmt.Sprintf("config.%s value", cfg.name),
				value,
				cfg.value,
				func(next string) SQLAction {
					return SQLAction{
						Summary: fmt.Sprintf("patch config.%s value", cfg.name),
						Query:   `UPDATE config SET value = ? WHERE name = ?;`,
						Args:    []any{next, cfg.name},
					}
				},
				func() SQLAction {
					return SQLAction{
						Summary: fmt.Sprintf("override config.%s value: %s -> %s", cfg.name, fmt.Sprintf("%q", value), fmt.Sprintf("%q", cfg.value)),
						Query:   `UPDATE config SET value = ? WHERE name = ?;`,
						Args:    []any{cfg.value, cfg.name},
					}
				},
			)
			if err != nil {
				return DefaultPlan{}, err
			}
			plan.Patches = append(plan.Patches, patches...)
			plan.Overrides = append(plan.Overrides, overrides...)
		}
	}

	if ok, err := hasTable(db, "exchanges"); err != nil {
		return DefaultPlan{}, err
	} else if ok && !rebuild["exchanges"] {
		columns, err := loadTableColumns(db, "exchanges")
		if err != nil {
			return DefaultPlan{}, err
		}
		hasAPIKey := columns["api_key"] != ""
		hasOHLCVLimit := columns["ohlcv_limit"] != ""
		hasMarketProxy := columns["market_proxy"] != ""
		hasTradeProxy := columns["trade_proxy"] != ""
		hasTimeframes := columns["timeframes"] != ""
		hasActive := columns["active"] != ""
		for _, ex := range defaultExchanges() {
			var apiKey string
			var rateLimit int
			var ohlcvLimit int
			var volumeFilter float64
			var marketProxy string
			var tradeProxy string
			switch {
			case hasAPIKey && hasOHLCVLimit && hasMarketProxy && hasTradeProxy:
				err = db.QueryRow(`SELECT COALESCE(api_key, ''), rate_limit, ohlcv_limit, volume_filter, COALESCE(market_proxy, ''), COALESCE(trade_proxy, '') FROM exchanges WHERE name = ?;`, ex.name).Scan(&apiKey, &rateLimit, &ohlcvLimit, &volumeFilter, &marketProxy, &tradeProxy)
			case hasOHLCVLimit && hasMarketProxy && hasTradeProxy:
				err = db.QueryRow(`SELECT rate_limit, ohlcv_limit, volume_filter, COALESCE(market_proxy, ''), COALESCE(trade_proxy, '') FROM exchanges WHERE name = ?;`, ex.name).Scan(&rateLimit, &ohlcvLimit, &volumeFilter, &marketProxy, &tradeProxy)
			case hasAPIKey && hasOHLCVLimit:
				err = db.QueryRow(`SELECT COALESCE(api_key, ''), rate_limit, ohlcv_limit, volume_filter FROM exchanges WHERE name = ?;`, ex.name).Scan(&apiKey, &rateLimit, &ohlcvLimit, &volumeFilter)
			case hasOHLCVLimit:
				err = db.QueryRow(`SELECT rate_limit, ohlcv_limit, volume_filter FROM exchanges WHERE name = ?;`, ex.name).Scan(&rateLimit, &ohlcvLimit, &volumeFilter)
			case hasAPIKey:
				err = db.QueryRow(`SELECT COALESCE(api_key, ''), rate_limit, volume_filter FROM exchanges WHERE name = ?;`, ex.name).Scan(&apiKey, &rateLimit, &volumeFilter)
			default:
				err = db.QueryRow(`SELECT rate_limit, volume_filter FROM exchanges WHERE name = ?;`, ex.name).Scan(&rateLimit, &volumeFilter)
			}
			if err != nil {
				if err == sql.ErrNoRows {
					plan.Inserts = append(plan.Inserts, SQLAction{
						Summary: fmt.Sprintf("insert default exchanges.%s", ex.name),
						Query: `INSERT INTO exchanges (name, api_key, ohlcv_limit, rate_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at)
							 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
						Args: []any{ex.name, ex.apiKey, ex.ohlcvLimit, ex.rateLimitMS, ex.volumeFilter, ex.marketProxy, ex.tradeProxy, ex.timeframes, boolToInt(ex.active), time.Now().UTC().Unix(), time.Now().UTC().Unix()},
					})
					continue
				}
				return DefaultPlan{}, err
			}
			if rateLimit != ex.rateLimitMS {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override exchanges.%s rate_limit: %d -> %d", ex.name, rateLimit, ex.rateLimitMS),
					Query:   `UPDATE exchanges SET rate_limit = ?, updated_at = ? WHERE name = ?;`,
					Args:    []any{ex.rateLimitMS, time.Now().UTC().Unix(), ex.name},
				})
			}
			if hasAPIKey {
				patches, overrides, err := classifyJSONStringDefault(
					fmt.Sprintf("exchanges.%s api_key", ex.name),
					apiKey,
					ex.apiKey,
					func(next string) SQLAction {
						return SQLAction{
							Summary: fmt.Sprintf("patch exchanges.%s api_key", ex.name),
							Query:   `UPDATE exchanges SET api_key = ?, updated_at = ? WHERE name = ?;`,
							Args:    []any{next, time.Now().UTC().Unix(), ex.name},
						}
					},
					func() SQLAction {
						return SQLAction{
							Summary: fmt.Sprintf("override exchanges.%s api_key: %s -> %s", ex.name, fmt.Sprintf("%q", apiKey), fmt.Sprintf("%q", ex.apiKey)),
							Query:   `UPDATE exchanges SET api_key = ?, updated_at = ? WHERE name = ?;`,
							Args:    []any{ex.apiKey, time.Now().UTC().Unix(), ex.name},
						}
					},
				)
				if err != nil {
					return DefaultPlan{}, err
				}
				plan.Patches = append(plan.Patches, patches...)
				_ = overrides // api_key existing differences are intentionally ignored
			}
			if hasOHLCVLimit && ohlcvLimit != ex.ohlcvLimit {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override exchanges.%s ohlcv_limit: %d -> %d", ex.name, ohlcvLimit, ex.ohlcvLimit),
					Query:   `UPDATE exchanges SET ohlcv_limit = ?, updated_at = ? WHERE name = ?;`,
					Args:    []any{ex.ohlcvLimit, time.Now().UTC().Unix(), ex.name},
				})
			}
			if volumeFilter != ex.volumeFilter {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override exchanges.%s volume_filter: %g -> %g", ex.name, volumeFilter, ex.volumeFilter),
					Query:   `UPDATE exchanges SET volume_filter = ?, updated_at = ? WHERE name = ?;`,
					Args:    []any{ex.volumeFilter, time.Now().UTC().Unix(), ex.name},
				})
			}
			if hasMarketProxy && marketProxy != ex.marketProxy {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override exchanges.%s market_proxy: %s -> %s", ex.name, fmt.Sprintf("%q", marketProxy), fmt.Sprintf("%q", ex.marketProxy)),
					Query:   `UPDATE exchanges SET market_proxy = ?, updated_at = ? WHERE name = ?;`,
					Args:    []any{ex.marketProxy, time.Now().UTC().Unix(), ex.name},
				})
			}
			if hasTradeProxy && tradeProxy != ex.tradeProxy {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override exchanges.%s trade_proxy: %s -> %s", ex.name, fmt.Sprintf("%q", tradeProxy), fmt.Sprintf("%q", ex.tradeProxy)),
					Query:   `UPDATE exchanges SET trade_proxy = ?, updated_at = ? WHERE name = ?;`,
					Args:    []any{ex.tradeProxy, time.Now().UTC().Unix(), ex.name},
				})
			}
			if hasTimeframes {
				var timeframes string
				if err := db.QueryRow(`SELECT timeframes FROM exchanges WHERE name = ?;`, ex.name).Scan(&timeframes); err != nil {
					return DefaultPlan{}, err
				}
				if !equalDefaultValue(timeframes, ex.timeframes) {
					plan.Overrides = append(plan.Overrides, SQLAction{
						Summary: fmt.Sprintf("override exchanges.%s timeframes: %s -> %s", ex.name, fmt.Sprintf("%q", timeframes), fmt.Sprintf("%q", ex.timeframes)),
						Query:   `UPDATE exchanges SET timeframes = ?, updated_at = ? WHERE name = ?;`,
						Args:    []any{ex.timeframes, time.Now().UTC().Unix(), ex.name},
					})
				}
			}
			if hasActive {
				var active int
				if err := db.QueryRow(`SELECT active FROM exchanges WHERE name = ?;`, ex.name).Scan(&active); err != nil {
					return DefaultPlan{}, err
				}
				if (active == 1) != ex.active {
					plan.Overrides = append(plan.Overrides, SQLAction{
						Summary: fmt.Sprintf("override exchanges.%s active: %d -> %d", ex.name, active, boolToInt(ex.active)),
						Query:   `UPDATE exchanges SET active = ?, updated_at = ? WHERE name = ?;`,
						Args:    []any{boolToInt(ex.active), time.Now().UTC().Unix(), ex.name},
					})
				}
			}
		}
	}

	symbolsOK, err := hasTable(db, "symbols")
	if err != nil {
		return DefaultPlan{}, err
	}
	exchangesOK, err := hasTable(db, "exchanges")
	if err != nil {
		return DefaultPlan{}, err
	}
	if symbolsOK && exchangesOK && !rebuild["symbols"] && !rebuild["exchanges"] {
		columns, err := loadTableColumns(db, "symbols")
		if err != nil {
			return DefaultPlan{}, err
		}
		hasTimeframes := columns["timeframes"] != ""
		for _, sym := range defaultSymbols() {
			var base, quote, typ string
			var timeframes string
			var active, dynamic int
			if hasTimeframes {
				err = db.QueryRow(
					`SELECT s.base, s.quote, s.type, COALESCE(s.timeframes, ''), s.active, s.dynamic
					 FROM symbols s
					 JOIN exchanges e ON s.exchange_id = e.id
					 WHERE e.name = ? AND s.symbol = ?;`,
					sym.exchange, sym.symbol,
				).Scan(&base, &quote, &typ, &timeframes, &active, &dynamic)
			} else {
				err = db.QueryRow(
					`SELECT s.base, s.quote, s.type, s.active, s.dynamic
					 FROM symbols s
					 JOIN exchanges e ON s.exchange_id = e.id
					 WHERE e.name = ? AND s.symbol = ?;`,
					sym.exchange, sym.symbol,
				).Scan(&base, &quote, &typ, &active, &dynamic)
			}
			if err != nil {
				if err == sql.ErrNoRows {
					plan.Inserts = append(plan.Inserts, SQLAction{
						Summary: fmt.Sprintf("insert default symbols.%s:%s", sym.exchange, sym.symbol),
						Query: `INSERT INTO symbols (exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
							 SELECT id, ?, ?, ?, ?, ?, ?, ? FROM exchanges WHERE name = ?;`,
						Args: []any{sym.symbol, sym.base, sym.quote, sym.typ, sym.timeframes, boolToInt(sym.active), boolToInt(sym.dynamic), sym.exchange},
					})
					continue
				}
				return DefaultPlan{}, err
			}
			if base != sym.base {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s base: %s -> %s", sym.exchange, sym.symbol, fmt.Sprintf("%q", base), fmt.Sprintf("%q", sym.base)),
					Query:   `UPDATE symbols SET base = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{sym.base, sym.exchange, sym.symbol},
				})
			}
			if quote != sym.quote {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s quote: %s -> %s", sym.exchange, sym.symbol, fmt.Sprintf("%q", quote), fmt.Sprintf("%q", sym.quote)),
					Query:   `UPDATE symbols SET quote = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{sym.quote, sym.exchange, sym.symbol},
				})
			}
			if typ != sym.typ {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s type: %s -> %s", sym.exchange, sym.symbol, fmt.Sprintf("%q", typ), fmt.Sprintf("%q", sym.typ)),
					Query:   `UPDATE symbols SET type = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{sym.typ, sym.exchange, sym.symbol},
				})
			}
			if hasTimeframes && !equalDefaultValue(timeframes, sym.timeframes) {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s timeframes: %s -> %s", sym.exchange, sym.symbol, fmt.Sprintf("%q", timeframes), fmt.Sprintf("%q", sym.timeframes)),
					Query:   `UPDATE symbols SET timeframes = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{sym.timeframes, sym.exchange, sym.symbol},
				})
			}
			if (active == 1) != sym.active {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s active: %d -> %d", sym.exchange, sym.symbol, active, boolToInt(sym.active)),
					Query:   `UPDATE symbols SET active = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{boolToInt(sym.active), sym.exchange, sym.symbol},
				})
			}
			if (dynamic == 1) != sym.dynamic {
				plan.Overrides = append(plan.Overrides, SQLAction{
					Summary: fmt.Sprintf("override symbols.%s:%s dynamic: %d -> %d", sym.exchange, sym.symbol, dynamic, boolToInt(sym.dynamic)),
					Query:   `UPDATE symbols SET dynamic = ? WHERE id = (SELECT s.id FROM symbols s JOIN exchanges e ON s.exchange_id = e.id WHERE e.name = ? AND s.symbol = ?);`,
					Args:    []any{boolToInt(sym.dynamic), sym.exchange, sym.symbol},
				})
			}
		}
	}

	return plan, nil
}

func ApplyDefaultAutoPlan(db *sql.DB, plan DefaultPlan) error {
	actions := make([]SQLAction, 0, len(plan.Inserts)+len(plan.Patches))
	actions = append(actions, plan.Inserts...)
	actions = append(actions, plan.Patches...)
	return applySQLActions(db, actions)
}

func ApplyDefaultOverridePlan(db *sql.DB, plan DefaultPlan) error {
	return applySQLActions(db, plan.Overrides)
}

func applySQLActions(db *sql.DB, actions []SQLAction) (err error) {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	if len(actions) == 0 {
		return nil
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			if err == nil {
				err = rbErr
			} else {
				err = fmt.Errorf("default rollback failed: %v; %w", rbErr, err)
			}
		}
	}()
	for _, action := range actions {
		if _, err := tx.Exec(action.Query, action.Args...); err != nil {
			return fmt.Errorf("%s: %w", action.Summary, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func classifyJSONStringDefault(
	label string,
	current string,
	desired string,
	buildPatch func(string) SQLAction,
	buildOverride func() SQLAction,
) (patches []SQLAction, overrides []SQLAction, err error) {
	if equalDefaultValue(current, desired) {
		return nil, nil, nil
	}
	merged, missing, differing, objectJSON, err := mergeMissingJSONObjectValue(current, desired)
	if err != nil {
		return nil, []SQLAction{{
			Summary: fmt.Sprintf("override %s invalid json -> default", label),
			Query:   buildOverride().Query,
			Args:    buildOverride().Args,
		}}, nil
	}
	if objectJSON {
		if len(missing) > 0 {
			action := buildPatch(merged)
			action.Summary = fmt.Sprintf("%s: add missing keys %s", action.Summary, strings.Join(missing, ", "))
			patches = append(patches, action)
		}
		if len(differing) > 0 {
			action := buildOverride()
			action.Summary = fmt.Sprintf("%s: reset differing keys %s to default", label, strings.Join(differing, ", "))
			overrides = append(overrides, action)
		}
		return patches, overrides, nil
	}
	return nil, []SQLAction{buildOverride()}, nil
}

func equalDefaultValue(current, desired string) bool {
	if current == desired {
		return true
	}
	return jsonSemanticEqual(current, desired)
}

func jsonSemanticEqual(current, desired string) bool {
	var left any
	if err := json.Unmarshal([]byte(strings.TrimSpace(current)), &left); err != nil {
		return false
	}
	var right any
	if err := json.Unmarshal([]byte(strings.TrimSpace(desired)), &right); err != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func mergeMissingJSONObjectValue(currentRaw, defaultRaw string) (merged string, missing []string, differing []string, objectJSON bool, err error) {
	defaultObj, ok, err := parseJSONObjectValue(defaultRaw)
	if err != nil || !ok {
		return "", nil, nil, false, err
	}
	currentObj, ok, err := parseJSONObjectValue(currentRaw)
	if err != nil {
		return "", nil, nil, true, err
	}
	if !ok {
		return "", nil, nil, false, nil
	}
	missing, differing = mergeMissingJSONObjectMap(currentObj, defaultObj, "")
	out, err := json.Marshal(currentObj)
	if err != nil {
		return "", nil, nil, true, err
	}
	return string(out), missing, differing, true, nil
}

func parseJSONObjectValue(raw string) (map[string]any, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false, nil
	}
	var parsed any
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, false, err
	}
	obj, ok := parsed.(map[string]any)
	return obj, ok, nil
}

func mergeMissingJSONObjectMap(current, defaults map[string]any, prefix string) (missing []string, differing []string) {
	keys := make([]string, 0, len(defaults))
	for key := range defaults {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		defaultValue := defaults[key]
		currentValue, ok := current[key]
		if !ok {
			current[key] = cloneJSONValue(defaultValue)
			missing = append(missing, path)
			continue
		}
		defaultObj, defaultIsObj := defaultValue.(map[string]any)
		currentObj, currentIsObj := currentValue.(map[string]any)
		if defaultIsObj && currentIsObj {
			childMissing, childDiffering := mergeMissingJSONObjectMap(currentObj, defaultObj, path)
			missing = append(missing, childMissing...)
			differing = append(differing, childDiffering...)
			continue
		}
		if !reflect.DeepEqual(currentValue, defaultValue) {
			differing = append(differing, path)
		}
	}
	return missing, differing
}

func cloneJSONValue(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}

func (p SchemaPlan) HasChanges() bool {
	return len(p.CreateTables) > 0 || len(p.AddColumns) > 0 || len(p.Rebuild) > 0
}

func (p SchemaPlan) HasRebuild() bool {
	return len(p.Rebuild) > 0
}

func (p SchemaPlan) HasAutoChanges() bool {
	return len(p.CreateTables) > 0 || len(p.AddColumns) > 0
}

func (p DefaultPlan) HasChanges() bool {
	return len(p.Inserts) > 0 || len(p.Patches) > 0 || len(p.Overrides) > 0
}

func (p DefaultPlan) HasAutoChanges() bool {
	return len(p.Inserts) > 0 || len(p.Patches) > 0
}

func (p DefaultPlan) HasOverrides() bool {
	return len(p.Overrides) > 0
}

func listTables(db *sql.DB) (map[string]bool, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type = 'table';`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func hasTable(db *sql.DB, name string) (bool, error) {
	tables, err := listTables(db)
	if err != nil {
		return false, err
	}
	return tables[name], nil
}

func hasColumn(db *sql.DB, table, column string) (bool, error) {
	columns, err := loadTableColumns(db, table)
	if err != nil {
		return false, err
	}
	_, ok := columns[column]
	return ok, nil
}

func loadTableColumns(db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var (
			cid       int
			name      string
			typ       string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func loadTablePrimaryKey(db *sql.DB, table string) ([]string, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type pkColumn struct {
		name string
		pos  int
	}
	var items []pkColumn
	for rows.Next() {
		var (
			cid       int
			name      string
			typ       string
			notnull   int
			dfltValue sql.NullString
			pk        int
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		if pk > 0 {
			items = append(items, pkColumn{name: name, pos: pk})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].pos < items[j].pos
	})
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = item.name
	}
	return out, nil
}

func normalizeType(raw string) string {
	return strings.ToUpper(strings.TrimSpace(raw))
}

func sameType(expected, actual string) bool {
	return normalizeType(actual) == normalizeType(expected)
}

func samePrimaryKey(expected, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := range expected {
		if expected[i] != actual[i] {
			return false
		}
	}
	return true
}

func tempTableCreateSQL(spec tableSpec, tempName string) string {
	prefix := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s", spec.Name)
	replacement := fmt.Sprintf("CREATE TABLE %s", tempName)
	return strings.Replace(spec.CreateSQL, prefix, replacement, 1)
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func seedDefaults(tx *sql.Tx) error {
	now := time.Now().UTC().Unix()

	for _, ex := range defaultExchanges() {
		if _, err := tx.Exec(
			`INSERT INTO exchanges (name, api_key, ohlcv_limit, rate_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(name) DO NOTHING;`,
			ex.name, ex.apiKey, ex.ohlcvLimit, ex.rateLimitMS, ex.volumeFilter, ex.marketProxy, ex.tradeProxy, ex.timeframes, boolToInt(ex.active), now, now,
		); err != nil {
			return fmt.Errorf("seed exchanges: %w", err)
		}
	}

	exchangeIDs := make(map[string]int64)
	for _, ex := range defaultExchanges() {
		var id int64
		if err := tx.QueryRow(`SELECT id FROM exchanges WHERE name = ?;`, ex.name).Scan(&id); err != nil {
			return fmt.Errorf("load exchange id: %w", err)
		}
		exchangeIDs[ex.name] = id
	}

	for _, sym := range defaultSymbols() {
		exchangeID, ok := exchangeIDs[sym.exchange]
		if !ok {
			return fmt.Errorf("missing exchange for symbol: %s", sym.exchange)
		}
		if _, err := tx.Exec(
			`INSERT INTO symbols (exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(exchange_id, symbol) DO NOTHING;`,
			exchangeID, sym.symbol, sym.base, sym.quote, sym.typ, sym.timeframes, boolToInt(sym.active), boolToInt(sym.dynamic),
		); err != nil {
			return fmt.Errorf("seed symbols: %w", err)
		}
	}

	for _, cfg := range defaultConfigs() {
		if _, err := tx.Exec(
			`INSERT INTO config (name, value, common) VALUES (?, ?, ?)
			 ON CONFLICT(name) DO NOTHING;`,
			cfg.name, cfg.value, cfg.common,
		); err != nil {
			return fmt.Errorf("seed config: %w", err)
		}
	}

	return nil
}
