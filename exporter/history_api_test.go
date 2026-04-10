package exporter

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/misterchenleiya/tradingbot/internal/models"
	"github.com/misterchenleiya/tradingbot/storage"
	"go.uber.org/zap"
)

func TestParseVisualHistoryQueryLocation(t *testing.T) {
	t.Run("empty uses local timezone", func(t *testing.T) {
		loc, err := parseVisualHistoryQueryLocation("")
		if err != nil {
			t.Fatalf("parseVisualHistoryQueryLocation empty failed: %v", err)
		}
		if loc == nil {
			t.Fatal("location should not be nil")
		}
	})

	t.Run("js offset -480 means utc+8", func(t *testing.T) {
		loc, err := parseVisualHistoryQueryLocation("-480")
		if err != nil {
			t.Fatalf("parseVisualHistoryQueryLocation -480 failed: %v", err)
		}
		_, offset := time.Now().In(loc).Zone()
		if offset != 8*3600 {
			t.Fatalf("offset=%d, want=%d", offset, 8*3600)
		}
	})

	t.Run("js offset 300 means utc-5", func(t *testing.T) {
		loc, err := parseVisualHistoryQueryLocation("300")
		if err != nil {
			t.Fatalf("parseVisualHistoryQueryLocation 300 failed: %v", err)
		}
		_, offset := time.Now().In(loc).Zone()
		if offset != -5*3600 {
			t.Fatalf("offset=%d, want=%d", offset, -5*3600)
		}
	})

	t.Run("invalid offset", func(t *testing.T) {
		if _, err := parseVisualHistoryQueryLocation("abc"); err == nil {
			t.Fatal("expected invalid tz_offset_min error")
		}
		if _, err := parseVisualHistoryQueryLocation("5000"); err == nil {
			t.Fatal("expected invalid tz_offset_min range error")
		}
	})
}

func TestParseHistoryDateWindowRangeMode(t *testing.T) {
	loc, err := parseVisualHistoryQueryLocation("-480")
	if err != nil {
		t.Fatalf("parseVisualHistoryQueryLocation failed: %v", err)
	}

	t.Run("explicit date uses start-of-day to now range", func(t *testing.T) {
		label, startMS, endMS, err := parseHistoryDateWindow("2026-02-01", loc)
		if err != nil {
			t.Fatalf("parseHistoryDateWindow failed: %v", err)
		}
		if label != "2026-02-01" {
			t.Fatalf("label=%s, want=2026-02-01", label)
		}
		expectedStart := time.Date(2026, 2, 1, 0, 0, 0, 0, loc).UnixMilli()
		if startMS != expectedStart {
			t.Fatalf("startMS=%d, want=%d", startMS, expectedStart)
		}
		if endMS <= startMS {
			t.Fatalf("endMS=%d should be > startMS=%d", endMS, startMS)
		}
	})

	t.Run("empty date uses local today start to now range", func(t *testing.T) {
		label, startMS, endMS, err := parseHistoryDateWindow("", loc)
		if err != nil {
			t.Fatalf("parseHistoryDateWindow empty failed: %v", err)
		}
		now := time.Now().In(loc)
		today := now.Format("2006-01-02")
		if label != today {
			t.Fatalf("label=%s, want=%s", label, today)
		}
		expectedStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc).UnixMilli()
		if startMS != expectedStart {
			t.Fatalf("startMS=%d, want=%d", startMS, expectedStart)
		}
		if endMS <= startMS {
			t.Fatalf("endMS=%d should be > startMS=%d", endMS, startMS)
		}
		if endMS > now.UnixMilli()+1500 {
			t.Fatalf("endMS too far in future: endMS=%d now=%d", endMS, now.UnixMilli())
		}
	})
}

func TestExtractVisualHistoryBackTestRunIDFromEnvelope(t *testing.T) {
	openRowJSON := models.MarshalPositionRowEnvelopeWithRuntime(
		visualHistoryBackTestOpenMeta{SingletonID: 11, RunID: "run-open"},
		models.StrategyContextMeta{},
		models.PositionRuntimeMeta{RunID: "run-open", SingletonID: 11},
	)
	closeRowJSON := models.MarshalPositionRowEnvelopeWithRuntime(
		visualHistoryBackTestCloseMeta{SingletonID: 12, RunID: "run-close"},
		models.StrategyContextMeta{},
		models.PositionRuntimeMeta{RunID: "run-close", SingletonID: 12},
	)
	if got := extractVisualHistoryBackTestRunID(openRowJSON, closeRowJSON); got != "run-open" {
		t.Fatalf("runID=%q, want=%q", got, "run-open")
	}
	if got := extractVisualHistoryBackTestRunID("", closeRowJSON); got != "run-close" {
		t.Fatalf("runID=%q, want=%q", got, "run-close")
	}
	if meta := extractVisualHistoryPositionRunMeta(openRowJSON, closeRowJSON); meta.RunID != "run-open" || meta.SingletonID != 11 {
		t.Fatalf("runtime meta=%+v, want run-open/11", meta)
	}
}

func TestBuildVisualHistoryPositionFilterGroupRunOptions(t *testing.T) {
	group := buildVisualHistoryPositionFilterGroup([]visualHistoryPositionCandidate{
		{RunID: "run-z", SingletonID: 0, Strategy: "turtle", Version: "v1", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "SOL/USDT"}},
		{RunID: "run-b", SingletonID: 12, Strategy: "turtle", Version: "v2", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "BTC/USDT"}},
		{RunID: "run-a", SingletonID: 3, Strategy: "turtle", Version: "v3", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "ETH/USDT"}},
		{RunID: "run-c", SingletonID: 3, Strategy: "turtle", Version: "v4", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "DOGE/USDT"}},
		{RunID: "run-z", SingletonID: 9, Strategy: "turtle", Version: "v5", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "XRP/USDT"}},
		{RunID: "run-d", SingletonID: 0, Strategy: "turtle", Version: "v6", Row: visualHistoryPositionRow{Exchange: "okx", Symbol: "TRX/USDT"}},
	})
	if len(group.RunOptions) != 5 {
		t.Fatalf("run options len=%d, want=5", len(group.RunOptions))
	}
	want := []visualHistoryRunOption{
		{Value: "run-a", Label: "3:run-a", SingletonID: 3},
		{Value: "run-c", Label: "3:run-c", SingletonID: 3},
		{Value: "run-z", Label: "9:run-z", SingletonID: 9},
		{Value: "run-b", Label: "12:run-b", SingletonID: 12},
		{Value: "run-d", Label: "run-d", SingletonID: 0},
	}
	for i := range want {
		if group.RunOptions[i] != want[i] {
			t.Fatalf("run option[%d]=%+v, want=%+v", i, group.RunOptions[i], want[i])
		}
		if group.RunIDs[i] != want[i].Value {
			t.Fatalf("run ids[%d]=%q, want=%q", i, group.RunIDs[i], want[i].Value)
		}
	}
}

func TestQuerySignalEventsFromStoreFiltersBySingletonScope(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(101, 1, 1, 1, '15m', 'turtle', 'v1', 2, 'stop_loss', '{}', 1000, 1000),
		(102, 2, 1, 1, '15m', 'turtle', 'v1', 2, 'stop_loss', '{}', 1000, 1000);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{StrategyName: "turtle"},
		900,
		1100,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].ID != "signal-101" {
		t.Fatalf("event id=%s, want=signal-101", events[0].ID)
	}
}

func TestQuerySignalEventsFromStoreFiltersByComboKey(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "signal-combo-source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(111, 1, 1, 1, '1h', 'turtle', 'v1', 2, 'sl',
		 '{"combo_key":"3m/15m/1h","strategy_timeframes":["3m","15m","1h"],"sl":88.8}', 1000, 1000),
		(112, 1, 1, 1, '30m', 'turtle', 'v1', 2, 'sl',
		 '{"combo_key":"1m/5m/30m","strategy_timeframes":["1m","5m","30m"],"sl":77.7}', 1000, 1000);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{
			StrategyName:       "turtle",
			StrategyTimeframes: []string{"3m", "15m", "1h"},
			ComboKey:           "3m/15m/1h",
		},
		900,
		1100,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].ID != "signal-111" {
		t.Fatalf("event id=%s, want=signal-111", events[0].ID)
	}
	if got := readDetailString(t, events[0].Detail, "combo_key"); got != "3m/15m/1h" {
		t.Fatalf("combo_key=%q, want=%q", got, "3m/15m/1h")
	}
}

func TestBuildVisualHistoryTimeframeIntegrityDetectsCoverageAndInternalGap(t *testing.T) {
	spec := visualHistoryFrameSpec{
		Timeframe:       "15m",
		ExpectedStartMS: 15 * 60 * 1000,
		ExpectedEndMS:   60 * 60 * 1000,
		ExpectedBars:    4,
	}
	candles := []visualHistoryCandle{
		{TS: 15 * 60 * 1000, Open: 1, High: 2, Low: 1, Close: 2, Volume: 10},
		{TS: 45 * 60 * 1000, Open: 2, High: 3, Low: 2, Close: 3, Volume: 12},
	}

	got := buildVisualHistoryTimeframeIntegrity(spec, candles)
	if got.Complete {
		t.Fatal("expected incomplete timeframe")
	}
	if got.Continuous {
		t.Fatal("expected discontinuity due to internal gap")
	}
	if got.ActualBars != 2 {
		t.Fatalf("actual_bars=%d, want=2", got.ActualBars)
	}
	if len(got.Gaps) != 2 {
		t.Fatalf("gap count=%d, want=2", len(got.Gaps))
	}
	if got.Gaps[0].Kind != "internal_gap" || got.Gaps[0].StartTS != 30*60*1000 || got.Gaps[0].EndTS != 30*60*1000 || got.Gaps[0].Bars != 1 {
		t.Fatalf("unexpected internal gap: %+v", got.Gaps[0])
	}
	if got.Gaps[1].Kind != "coverage_end" || got.Gaps[1].StartTS != 60*60*1000 || got.Gaps[1].EndTS != 60*60*1000 || got.Gaps[1].Bars != 1 {
		t.Fatalf("unexpected coverage_end gap: %+v", got.Gaps[1])
	}
}

func TestBuildVisualHistoryFrameSpecsClosedPositionIncludesCloseBar(t *testing.T) {
	row := visualHistoryPositionRow{
		IsOpen:      false,
		OpenTimeMS:  1_800_000, // 00:30
		CloseTimeMS: 2_340_000, // 00:39（3m 边界）
		OpenRowJSON: models.MarshalPositionRowEnvelope(nil, models.StrategyContextMeta{
			StrategyTimeframes: []string{"3m"},
		}),
	}

	specs, err := buildVisualHistoryFrameSpecs(row, "")
	if err != nil {
		t.Fatalf("buildVisualHistoryFrameSpecs failed: %v", err)
	}
	if len(specs) != 1 {
		t.Fatalf("spec count=%d, want=1", len(specs))
	}
	spec := specs[0]
	if spec.Timeframe != "3m" {
		t.Fatalf("timeframe=%s, want=3m", spec.Timeframe)
	}
	if spec.ExpectedEndMS != 2_340_000 {
		t.Fatalf("expected_end_ms=%d, want=2340000", spec.ExpectedEndMS)
	}
}

func TestPositionEventRangeUsesTrendAnchor(t *testing.T) {
	row := visualHistoryPositionRow{
		OpenTimeMS:  2_000,
		CloseTimeMS: 6_000,
		OpenRowJSON: models.MarshalPositionRowEnvelope(nil, models.StrategyContextMeta{
			TrendingTimestamp: 1_000,
		}),
	}
	startMS, endMS := positionEventRange(row)
	if startMS != 1_000 {
		t.Fatalf("start=%d, want=1000", startMS)
	}
	if endMS != 6_000 {
		t.Fatalf("end=%d, want=6000", endMS)
	}
}

func TestNormalizeVisualHistorySymbol(t *testing.T) {
	cases := []struct {
		name   string
		symbol string
		instID string
		want   string
	}{
		{
			name:   "already formatted",
			symbol: "GRASS/USDT",
			instID: "GRASS-USDT-SWAP",
			want:   "GRASS/USDT",
		},
		{
			name:   "compact symbol",
			symbol: "GRASSUSDT",
			instID: "",
			want:   "GRASS/USDT",
		},
		{
			name:   "fallback by inst id",
			symbol: "",
			instID: "GRASS-USDT-SWAP",
			want:   "GRASS/USDT",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := normalizeVisualHistorySymbol(tc.symbol, tc.instID); got != tc.want {
				t.Fatalf("normalizeVisualHistorySymbol(%q,%q)=%q, want=%q", tc.symbol, tc.instID, got, tc.want)
			}
		})
	}
}

func TestQuerySignalEventsFromStoreAddsNormalizedTPSLPrices(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "signal-source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(101, 1, 1, 1, '15m', 'turtle', 'v1', 2, 'stop_loss,tp', '{"tp":91.25,"sl":86.75,"action":16,"high_side":1}', 1000, 1000);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{StrategyName: "turtle"},
		900,
		1100,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].Type != "TRAILING_TP_SL" {
		t.Fatalf("event type=%q, want=%q", events[0].Type, "TRAILING_TP_SL")
	}
	if got := readDetailFloat(t, events[0].Detail, "tp_price"); got != 91.25 {
		t.Fatalf("tp_price=%.8f, want=91.25", got)
	}
	if got := readDetailFloat(t, events[0].Detail, "sl_price"); got != 86.75 {
		t.Fatalf("sl_price=%.8f, want=86.75", got)
	}
	if got := readDetailInt(t, events[0].Detail, "action"); got != 16 {
		t.Fatalf("action=%d, want=16", got)
	}
	if got := readDetailInt(t, events[0].Detail, "high_side"); got != 1 {
		t.Fatalf("high_side=%d, want=1", got)
	}
	if _, exists := events[0].Detail["signal_json"]; exists {
		t.Fatalf("signal_json should not be returned in detail: %+v", events[0].Detail)
	}
}

func TestQuerySignalEventsFromStoreMarksArmedByAction(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "signal-armed-source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(201, 1, 1, 1, '3m', 'turtle', 'v1', 1, 'action,entry_watch_timestamp,has_position,order_type',
		 '{"action":4,"high_side":-1,"entry_watch_timestamp":1710000123456,"has_position":8,"order_type":"market"}', 2000, 2000);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{StrategyName: "turtle"},
		1900,
		2100,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].Type != "ARMED" {
		t.Fatalf("event type=%q, want=%q", events[0].Type, "ARMED")
	}
	if events[0].Title != "Armed" {
		t.Fatalf("event title=%q, want=%q", events[0].Title, "Armed")
	}
	if got := readDetailInt(t, events[0].Detail, "action"); got != 4 {
		t.Fatalf("action=%d, want=4", got)
	}
	if got := readDetailInt(t, events[0].Detail, "high_side"); got != -1 {
		t.Fatalf("high_side=%d, want=-1", got)
	}
	if got := readDetailInt(t, events[0].Detail, "has_position"); got != 8 {
		t.Fatalf("has_position=%d, want=8", got)
	}
	if got := readDetailString(t, events[0].Detail, "order_type"); got != "market" {
		t.Fatalf("order_type=%q, want=%q", got, "market")
	}
	if got := readDetailInt64(t, events[0].Detail, "entry_watch_timestamp"); got != 1710000123456 {
		t.Fatalf("entry_watch_timestamp=%d, want=1710000123456", got)
	}
}

func TestQuerySignalEventsFromStoreAddsTrendHighMidDetails(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "signal-trend-source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(301, 1, 1, 1, '1h', 'turtle', 'v1', 1, 'trending_timestamp,high_side,mid_side',
		 '{"action":0,"high_side":1,"mid_side":255,"trending_timestamp":1710000123456}', 3000, 3000);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{StrategyName: "turtle"},
		2900,
		3100,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].Type != "TREND_DETECTED" {
		t.Fatalf("event type=%q, want=%q", events[0].Type, "TREND_DETECTED")
	}
	if got := readDetailInt(t, events[0].Detail, "high_side"); got != 1 {
		t.Fatalf("high_side=%d, want=1", got)
	}
	if got := readDetailInt(t, events[0].Detail, "mid_side"); got != 255 {
		t.Fatalf("mid_side=%d, want=255", got)
	}
	if got := readDetailInt64(t, events[0].Detail, "trending_timestamp"); got != 1710000123456 {
		t.Fatalf("trending_timestamp=%d, want=1710000123456", got)
	}
}

func TestQuerySignalEventsFromStoreAddsProfitProtectDetails(t *testing.T) {
	sourceDB := openHistoryTestSQLite(t, "signal-r-source.db")
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE exchanges (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE symbols (id INTEGER PRIMARY KEY, symbol TEXT NOT NULL);`)
	mustExecHistoryTestSQL(t, sourceDB, `CREATE TABLE signals (
		id INTEGER PRIMARY KEY,
		singleton_id INTEGER,
		exchange_id INTEGER NOT NULL,
		symbol_id INTEGER NOT NULL,
		timeframe TEXT NOT NULL,
		strategy TEXT NOT NULL,
		strategy_version TEXT NOT NULL DEFAULT '',
		change_status INTEGER NOT NULL,
		changed_fields TEXT NOT NULL DEFAULT '',
		signal_json TEXT NOT NULL DEFAULT '',
		event_at_ms INTEGER NOT NULL DEFAULT 0,
		created_at_ms INTEGER NOT NULL DEFAULT 0
	);`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO exchanges(id, name) VALUES (1, 'okx');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO symbols(id, symbol) VALUES (1, 'solusdtp');`)
	mustExecHistoryTestSQL(t, sourceDB, `INSERT INTO signals(
		id, singleton_id, exchange_id, symbol_id, timeframe, strategy, strategy_version,
		change_status, changed_fields, signal_json, event_at_ms, created_at_ms
	) VALUES
		(401, 1, 1, 1, '15m', 'turtle', 'v1', 1, 'profit_protect_stage,sl,max_favorable_profit_pct',
		 '{"action":16,"entry":100,"sl":100,"initial_sl":99,"initial_risk_pct":0.01,"max_favorable_profit_pct":0.02,"profit_protect_stage":1}', 4000, 4000),
		(402, 1, 1, 1, '15m', 'turtle', 'v1', 1, 'profit_protect_stage,sl,max_favorable_profit_pct,action',
		 '{"action":32,"entry":100,"sl":102,"initial_sl":99,"initial_risk_pct":0.01,"max_favorable_profit_pct":0.04,"profit_protect_stage":2}', 4100, 4100);`)

	events, err := querySignalEventsFromStore(
		sourceDB,
		visualHistoryPositionRow{Exchange: "okx", Symbol: "solusdtp"},
		models.StrategyContextMeta{StrategyName: "turtle"},
		3900,
		4200,
		visualHistoryRunScope{RunID: "run-a", SingletonUUID: "run-a", SingletonID: 1},
		visualHistorySignalEventFilter{},
	)
	if err != nil {
		t.Fatalf("querySignalEventsFromStore failed: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("event count=%d, want=2", len(events))
	}
	if events[0].Type != "R_PROTECT_2R" {
		t.Fatalf("event[0] type=%q, want=%q", events[0].Type, "R_PROTECT_2R")
	}
	if events[0].Title != "2R 保本保护" {
		t.Fatalf("event[0] title=%q, want=%q", events[0].Title, "2R 保本保护")
	}
	if got := readDetailFloat(t, events[0].Detail, "entry_price"); got != 100 {
		t.Fatalf("entry_price=%v, want=100", got)
	}
	if got := readDetailFloat(t, events[0].Detail, "initial_sl"); got != 99 {
		t.Fatalf("initial_sl=%v, want=99", got)
	}
	if got := readDetailFloat(t, events[0].Detail, "initial_risk_pct"); got != 0.01 {
		t.Fatalf("initial_risk_pct=%v, want=0.01", got)
	}
	if got := readDetailFloat(t, events[0].Detail, "max_favorable_profit_pct"); got != 0.02 {
		t.Fatalf("max_favorable_profit_pct=%v, want=0.02", got)
	}
	if got := readDetailFloat(t, events[0].Detail, "mfer"); got != 2 {
		t.Fatalf("mfer=%v, want=2", got)
	}
	if got := readDetailInt(t, events[0].Detail, "profit_protect_stage"); got != 1 {
		t.Fatalf("profit_protect_stage=%d, want=1", got)
	}
	if events[1].Type != "R_PROTECT_4R" {
		t.Fatalf("event[1] type=%q, want=%q", events[1].Type, "R_PROTECT_4R")
	}
	if events[1].Title != "4R 部分平仓保护" {
		t.Fatalf("event[1] title=%q, want=%q", events[1].Title, "4R 部分平仓保护")
	}
	if got := readDetailFloat(t, events[1].Detail, "sl_price"); got != 102 {
		t.Fatalf("sl_price=%v, want=102", got)
	}
	if got := readDetailFloat(t, events[1].Detail, "mfer"); got != 4 {
		t.Fatalf("mfer=%v, want=4", got)
	}
	if got := readDetailInt(t, events[1].Detail, "profit_protect_stage"); got != 2 {
		t.Fatalf("profit_protect_stage=%d, want=2", got)
	}
	if got := readDetailInt(t, events[1].Detail, "action"); got != 32 {
		t.Fatalf("action=%d, want=32", got)
	}
}

func TestBuildBubbleSignalEventsFallsBackWithoutGroupID(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO singleton(id, uuid, version, mode, source, status, created, updated)
		VALUES (1, 'run-a', 'v1', 'back-test', '', 'completed', 1, 1);`)
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO exchanges(id, name, api_key, rate_limit, ohlcv_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at)
		VALUES (1, 'okx', '', 100, 300, 10, '', '', '["1h"]', 1, 1, 1);`)
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO symbols(id, exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
		VALUES (1, 1, 'solusdtp', 'SOL', 'USDT', 'swap', '["1h"]', 1, 0);`)
	if err := store.AppendSignalChange(models.SignalChangeRecord{
		SingletonID:     1,
		Mode:            "back-test",
		Exchange:        "okx",
		Symbol:          "solusdtp",
		Timeframe:       "1h",
		Strategy:        "turtle",
		StrategyVersion: "v1",
		ChangeStatus:    1,
		ChangedFields:   "trending_timestamp,high_side",
		SignalJSON:      `{"combo_key":"1m/5m/1h","strategy_timeframes":["1m","5m","1h"],"high_side":1,"trending_timestamp":1000}`,
		EventAtMS:       1000,
		CreatedAtMS:     1000,
	}); err != nil {
		t.Fatalf("append signal change failed: %v", err)
	}

	server := &Server{cfg: Config{HistoryStore: store}, logger: zap.NewNop()}
	events, err := server.buildBubbleSignalEvents(bubbleSignalEventsRequest{
		Exchange:          "okx",
		Symbol:            "solusdtp",
		Timeframe:         "1h",
		Strategy:          "turtle",
		StrategyVersion:   "v1",
		ComboKey:          "1m/5m/1h",
		GroupID:           "turtle|1h|long|1000",
		TrendingTimestamp: 1000,
	})
	if err != nil {
		t.Fatalf("buildBubbleSignalEvents failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].Type != "TREND_DETECTED" {
		t.Fatalf("event type=%q, want=%q", events[0].Type, "TREND_DETECTED")
	}
}

func TestBuildBubbleSignalEventsFallsBackAcrossMergedExchangeTokens(t *testing.T) {
	store := newSingletonTestStore(t)
	defer func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO singleton(id, uuid, version, mode, source, status, created, updated)
		VALUES (1, 'run-a', 'v1', 'back-test', '', 'completed', 1, 1);`)
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO exchanges(id, name, api_key, rate_limit, ohlcv_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at)
		VALUES (1, 'okx', '', 100, 300, 10, '', '', '["1h"]', 1, 1, 1);`)
	mustExecHistoryTestSQL(t, store.DB, `INSERT INTO symbols(id, exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
		VALUES (1, 1, 'solusdtp', 'SOL', 'USDT', 'swap', '["1h"]', 1, 0);`)
	if err := store.AppendSignalChange(models.SignalChangeRecord{
		SingletonID:     1,
		Mode:            "back-test",
		Exchange:        "okx",
		Symbol:          "solusdtp",
		Timeframe:       "1h",
		Strategy:        "turtle",
		StrategyVersion: "v1",
		ChangeStatus:    1,
		ChangedFields:   "trending_timestamp,high_side",
		SignalJSON:      `{"combo_key":"1m/5m/1h","strategy_timeframes":["1m","5m","1h"],"high_side":1,"trending_timestamp":1000}`,
		EventAtMS:       1000,
		CreatedAtMS:     1000,
	}); err != nil {
		t.Fatalf("append signal change failed: %v", err)
	}

	server := &Server{cfg: Config{HistoryStore: store}, logger: zap.NewNop()}
	events, err := server.buildBubbleSignalEvents(bubbleSignalEventsRequest{
		Exchange:          "binance/okx",
		Symbol:            "solusdtp",
		Timeframe:         "1h",
		Strategy:          "turtle",
		StrategyVersion:   "v1",
		ComboKey:          "1m/5m/1h",
		TrendingTimestamp: 1000,
	})
	if err != nil {
		t.Fatalf("buildBubbleSignalEvents failed: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count=%d, want=1", len(events))
	}
	if events[0].ID == "" {
		t.Fatal("event id should not be empty")
	}
}

func TestQueryVisualHistoryPositionsSupportsFiltersAndOptions(t *testing.T) {
	db := openHistoryTestSQLite(t, "positions.db")
	mustExecHistoryTestSQL(t, db, `CREATE TABLE history_positions (
		id INTEGER PRIMARY KEY,
		exchange TEXT NOT NULL,
		symbol TEXT NOT NULL,
		inst_id TEXT NOT NULL DEFAULT '',
		pos TEXT NOT NULL DEFAULT '0',
		pos_side TEXT NOT NULL DEFAULT '',
		mgn_mode TEXT NOT NULL DEFAULT '',
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
		updated_at_ms INTEGER NOT NULL DEFAULT 0
	);`)

	insertPosition := func(id int64, closeTime int64, runID, strategy, version string) {
		openRowJSON := models.MarshalPositionRowEnvelope(
			visualHistoryBackTestOpenMeta{RunID: runID},
			models.StrategyContextMeta{
				StrategyName:    strategy,
				StrategyVersion: version,
			},
		)
		mustExecHistoryTestSQL(t, db, `
			INSERT INTO history_positions(
				id, exchange, symbol, inst_id, pos, pos_side, mgn_mode, margin, lever, avg_px,
				notional_usd, open_time_ms, open_update_time_ms, open_row_json, close_avg_px,
				realized_pnl, pnl_ratio, close_time_ms, state, created_at_ms, updated_at_ms
			) VALUES (?, 'okx', 'solusdtp', 'SOL-USDT-SWAP', '1', 'long', 'cross', '100', '3', '80',
				'240', ?, ?, ?, '88', '12', '0.15', ?, '', ?, ?);`,
			id,
			closeTime-1800000,
			closeTime-900000,
			openRowJSON,
			closeTime,
			closeTime-1800000,
			closeTime,
		)
	}

	insertPosition(1, 4000, "run-a", "turtle", "v1")
	insertPosition(2, 3500, "run-a", "turtle", "v1")
	insertPosition(3, 3000, "run-b", "turtle", "v2")
	insertPosition(4, 2500, "run-a", "turtle", "v1")

	srv := &Server{cfg: Config{HistoryStore: &storage.SQLite{DB: db}}}

	rows, options, hasMore, nextBefore, err := srv.queryVisualHistoryPositions(
		2000,
		5000,
		"okx",
		"solusdtp",
		visualHistoryPositionFilters{RunID: "run-a", Strategy: "turtle", Version: "v1"},
		0,
		1,
	)
	if err != nil {
		t.Fatalf("queryVisualHistoryPositions failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count=%d, want=1", len(rows))
	}
	if rows[0].ID != 1 {
		t.Fatalf("first row id=%d, want=1", rows[0].ID)
	}
	if !hasMore {
		t.Fatal("hasMore=false, want=true")
	}
	if nextBefore != 4000 {
		t.Fatalf("nextBefore=%d, want=4000", nextBefore)
	}
	assertStringSliceEqual(t, options.RunIDs, []string{"run-a", "run-b"})
	assertStringSliceEqual(t, options.Strategies, []string{"turtle", "turtle"})
	assertStringSliceEqual(t, options.Versions, []string{"v1", "v2"})
	assertStringSliceEqual(t, options.Exchanges, []string{"okx"})
	assertStringSliceEqual(t, options.Symbols, []string{"SOL/USDT"})

	rows, _, hasMore, nextBefore, err = srv.queryVisualHistoryPositions(
		2000,
		5000,
		"okx",
		"solusdtp",
		visualHistoryPositionFilters{RunID: "run-a", Strategy: "turtle", Version: "v1"},
		4000,
		1,
	)
	if err != nil {
		t.Fatalf("queryVisualHistoryPositions page2 failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("row count page2=%d, want=1", len(rows))
	}
	if rows[0].ID != 2 {
		t.Fatalf("second row id=%d, want=2", rows[0].ID)
	}
	if hasMore {
		t.Fatal("hasMore=true on page2, want=false")
	}
	if nextBefore != 0 {
		t.Fatalf("nextBefore page2=%d, want=0", nextBefore)
	}
}

func TestAlignVisualHistoryClosedBarStartUsesLastCompletedBar(t *testing.T) {
	const durMS = int64(15 * 60 * 1000)
	cases := []struct {
		name string
		ts   int64
		want int64
	}{
		{name: "inside first bar", ts: 5 * 60 * 1000, want: 0},
		{name: "inside second bar", ts: 20 * 60 * 1000, want: 0},
		{name: "exact boundary", ts: 30 * 60 * 1000, want: 15 * 60 * 1000},
		{name: "inside third bar", ts: 32 * 60 * 1000, want: 15 * 60 * 1000},
		{name: "inside fourth bar", ts: 46 * 60 * 1000, want: 30 * 60 * 1000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := alignVisualHistoryClosedBarStart(tc.ts, durMS)
			if got != tc.want {
				t.Fatalf("alignVisualHistoryClosedBarStart(%d)=%d want=%d", tc.ts, got, tc.want)
			}
		})
	}
}

func openHistoryTestSQLite(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Fatalf("close sqlite failed: %v", closeErr)
		}
	})
	return db
}

func mustExecHistoryTestSQL(t *testing.T, db *sql.DB, stmt string, args ...any) {
	t.Helper()
	if _, err := db.Exec(stmt, args...); err != nil {
		t.Fatalf("exec sql failed: %v\nsql=%s", err, stmt)
	}
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("slice length=%d, want=%d; got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slice[%d]=%q, want=%q; got=%v want=%v", i, got[i], want[i], got, want)
		}
	}
}

func readDetailFloat(t *testing.T, detail map[string]any, key string) float64 {
	t.Helper()
	raw, ok := detail[key]
	if !ok {
		t.Fatalf("detail missing key %q: %+v", key, detail)
	}
	value, ok := raw.(float64)
	if !ok {
		t.Fatalf("detail[%s] type=%T, want float64", key, raw)
	}
	return value
}

func readDetailInt(t *testing.T, detail map[string]any, key string) int {
	t.Helper()
	raw, ok := detail[key]
	if !ok {
		t.Fatalf("detail missing key %q: %+v", key, detail)
	}
	switch value := raw.(type) {
	case int:
		return value
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		t.Fatalf("detail[%s] type=%T, want int-like", key, raw)
		return 0
	}
}

func readDetailInt64(t *testing.T, detail map[string]any, key string) int64 {
	t.Helper()
	raw, ok := detail[key]
	if !ok {
		t.Fatalf("detail missing key %q: %+v", key, detail)
	}
	switch value := raw.(type) {
	case int:
		return int64(value)
	case int8:
		return int64(value)
	case int16:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	default:
		t.Fatalf("detail[%s] type=%T, want int64-like", key, raw)
		return 0
	}
}

func readDetailString(t *testing.T, detail map[string]any, key string) string {
	t.Helper()
	raw, ok := detail[key]
	if !ok {
		t.Fatalf("detail missing key %q: %+v", key, detail)
	}
	value, ok := raw.(string)
	if !ok {
		t.Fatalf("detail[%s] type=%T, want string", key, raw)
	}
	return value
}
