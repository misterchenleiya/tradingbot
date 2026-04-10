package storage

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func openTestSQLite(t *testing.T) *SQLite {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gobot.db")
	store := NewSQLite(Config{Path: dbPath, Logger: zap.NewNop()})
	if err := store.Start(context.Background()); err != nil {
		t.Fatalf("start sqlite failed: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	})
	if err := store.EnsureSchema(); err != nil {
		t.Fatalf("ensure schema failed: %v", err)
	}
	return store
}

func TestSQLiteStartConfiguresRuntime(t *testing.T) {
	store := openTestSQLite(t)

	var journalMode string
	if err := store.DB.QueryRow(`PRAGMA journal_mode;`).Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode failed: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(journalMode), "wal") {
		t.Fatalf("unexpected journal_mode: got=%q want=%q", journalMode, "wal")
	}

	var busyTimeout int
	if err := store.DB.QueryRow(`PRAGMA busy_timeout;`).Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout failed: %v", err)
	}
	if busyTimeout != sqliteBusyTimeoutMS {
		t.Fatalf("unexpected busy_timeout: got=%d want=%d", busyTimeout, sqliteBusyTimeoutMS)
	}

	var synchronous int
	if err := store.DB.QueryRow(`PRAGMA synchronous;`).Scan(&synchronous); err != nil {
		t.Fatalf("query synchronous failed: %v", err)
	}
	if synchronous != 1 {
		t.Fatalf("unexpected synchronous: got=%d want=%d", synchronous, 1)
	}

	if got := store.DB.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("unexpected max open connections: got=%d want=%d", got, 1)
	}
}

func TestSeedDefaults_InsertOnly(t *testing.T) {
	store := openTestSQLite(t)
	now := time.Now().UTC().Unix()

	if _, err := store.DB.Exec(
		`INSERT INTO exchanges (name, api_key, ohlcv_limit, rate_limit, volume_filter, market_proxy, trade_proxy, timeframes, active, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		"okx",
		`{"api_key":"custom","secret_key":"custom","passphrase":"custom"}`,
		100,
		250,
		9.5,
		"http://market-proxy",
		"http://trade-proxy",
		`["3m","15m","1h"]`,
		0,
		now,
		now,
	); err != nil {
		t.Fatalf("insert custom exchange failed: %v", err)
	}
	if _, err := store.DB.Exec(`INSERT INTO config (name, value, common) VALUES (?, ?, ?);`, "app_name", "custom-gobot", "custom common"); err != nil {
		t.Fatalf("insert custom config failed: %v", err)
	}

	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	var appName, appCommon string
	if err := store.DB.QueryRow(`SELECT value, common FROM config WHERE name = 'app_name';`).Scan(&appName, &appCommon); err != nil {
		t.Fatalf("query app_name failed: %v", err)
	}
	if appName != "custom-gobot" || appCommon != "custom common" {
		t.Fatalf("app_name was overwritten, value=%q common=%q", appName, appCommon)
	}

	var apiKey string
	var timeframes string
	if err := store.DB.QueryRow(`SELECT api_key, timeframes FROM exchanges WHERE name = 'okx';`).Scan(&apiKey, &timeframes); err != nil {
		t.Fatalf("query exchange failed: %v", err)
	}
	if apiKey != `{"api_key":"custom","secret_key":"custom","passphrase":"custom"}` {
		t.Fatalf("api_key was overwritten: %q", apiKey)
	}
	if timeframes != `["3m","15m","1h"]` {
		t.Fatalf("timeframes were overwritten: %q", timeframes)
	}

	var exchangeCfg string
	if err := store.DB.QueryRow(`SELECT value FROM config WHERE name = 'exchange';`).Scan(&exchangeCfg); err != nil {
		t.Fatalf("query config.exchange failed: %v", err)
	}
	if strings.TrimSpace(exchangeCfg) == "" {
		t.Fatalf("expected missing config.exchange to be inserted")
	}

	var symbolCount int
	if err := store.DB.QueryRow(`SELECT COUNT(*) FROM symbols WHERE exchange_id = (SELECT id FROM exchanges WHERE name = 'okx');`).Scan(&symbolCount); err != nil {
		t.Fatalf("count symbols failed: %v", err)
	}
	if symbolCount == 0 {
		t.Fatalf("expected missing default symbols to be inserted")
	}
}

func TestPlanDefaults_JSONPatchAndExcludedOverrides(t *testing.T) {
	store := openTestSQLite(t)
	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	if _, err := store.DB.Exec(`UPDATE config SET value = '{}' WHERE name = 'exchange';`); err != nil {
		t.Fatalf("update config.exchange failed: %v", err)
	}
	if _, err := store.DB.Exec(`UPDATE exchanges SET api_key = ?, rate_limit = 999, timeframes = ? WHERE name = 'okx';`,
		`{"api_key":"custom","secret_key":"secret","passphrase":"pass"}`,
		`["1m","5m","30m"]`,
	); err != nil {
		t.Fatalf("update exchanges.okx failed: %v", err)
	}

	plan, err := PlanDefaults(store.DB, SchemaPlan{})
	if err != nil {
		t.Fatalf("plan defaults failed: %v", err)
	}
	if !plan.HasAutoChanges() {
		t.Fatalf("expected auto changes for config.exchange missing key patch")
	}
	if !plan.HasOverrides() {
		t.Fatalf("expected overrides for non-default exchange fields")
	}

	patchSummaries := make([]string, 0, len(plan.Patches))
	for _, item := range plan.Patches {
		patchSummaries = append(patchSummaries, item.Summary)
	}
	overrideSummaries := make([]string, 0, len(plan.Overrides))
	for _, item := range plan.Overrides {
		overrideSummaries = append(overrideSummaries, item.Summary)
	}

	foundExchangePatch := false
	for _, summary := range patchSummaries {
		if strings.Contains(summary, "config.exchange value") && strings.Contains(summary, "fetch_unclosed_ohlcv") {
			foundExchangePatch = true
			break
		}
	}
	if !foundExchangePatch {
		t.Fatalf("expected config.exchange patch summary, got=%v", patchSummaries)
	}

	for _, summary := range overrideSummaries {
		if strings.Contains(summary, "api_key") {
			t.Fatalf("unexpected api_key override candidate: %s", summary)
		}
		if strings.Contains(summary, "trade_enabled") {
			t.Fatalf("unexpected trade_enabled override candidate: %s", summary)
		}
	}

	foundRateLimit := false
	foundTimeframes := false
	for _, summary := range overrideSummaries {
		if strings.Contains(summary, "rate_limit") {
			foundRateLimit = true
		}
		if strings.Contains(summary, "timeframes") {
			foundTimeframes = true
		}
	}
	if !foundRateLimit {
		t.Fatalf("expected exchanges.okx rate_limit override, got=%v", overrideSummaries)
	}
	if !foundTimeframes {
		t.Fatalf("expected exchanges.okx timeframes override, got=%v", overrideSummaries)
	}
}

func TestSeedDefaults_RiskTrendGuardModeDefaultsToGrouped(t *testing.T) {
	store := openTestSQLite(t)
	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	var raw string
	if err := store.DB.QueryRow(`SELECT value FROM config WHERE name = 'risk';`).Scan(&raw); err != nil {
		t.Fatalf("query config.risk failed: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode config.risk failed: %v", err)
	}
	trendGuard, ok := cfg["trend_guard"].(map[string]any)
	if !ok {
		t.Fatalf("missing trend_guard object: %v", cfg["trend_guard"])
	}
	if got := strings.TrimSpace(toJSONStringValue(trendGuard["mode"])); got != "grouped" {
		t.Fatalf("unexpected trend_guard.mode default: got %q want %q", got, "grouped")
	}
	if got := numberToFloat(trendGuard["leader_min_priority_score"]); got != 50 {
		t.Fatalf("unexpected trend_guard.leader_min_priority_score default: got %.2f want 50", got)
	}
}

func TestSQLiteOHLCVBoundsRoundTrip(t *testing.T) {
	store := openTestSQLite(t)
	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	if _, err := store.DB.Exec(
		`INSERT INTO symbols (exchange_id, symbol, base, quote, type, timeframes, active, dynamic)
		 VALUES ((SELECT id FROM exchanges WHERE name = 'okx'), ?, ?, ?, ?, ?, 1, 1)
		 ON CONFLICT(exchange_id, symbol) DO NOTHING;`,
		"AZTEC/USDT",
		"AZTEC",
		"USDT",
		"swap",
		"",
	); err != nil {
		t.Fatalf("insert symbol failed: %v", err)
	}

	earliest := time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC).UnixMilli()
	if err := store.UpsertOHLCVBound("okx", "AZTEC/USDT", earliest); err != nil {
		t.Fatalf("upsert ohlcv bound failed: %v", err)
	}
	got, ok, err := store.GetOHLCVBound("okx", "AZTEC/USDT")
	if err != nil {
		t.Fatalf("get ohlcv bound failed: %v", err)
	}
	if !ok || got != earliest {
		t.Fatalf("unexpected bound: got=%d ok=%v want=%d", got, ok, earliest)
	}

	later := earliest + int64(time.Hour.Milliseconds())
	if err := store.UpsertOHLCVBound("okx", "AZTEC/USDT", later); err != nil {
		t.Fatalf("upsert later ohlcv bound failed: %v", err)
	}
	got, ok, err = store.GetOHLCVBound("okx", "AZTEC/USDT")
	if err != nil {
		t.Fatalf("get ohlcv bound after later update failed: %v", err)
	}
	if !ok || got != earliest {
		t.Fatalf("expected later update not to overwrite earlier bound, got=%d ok=%v want=%d", got, ok, earliest)
	}
}

func TestPlanDefaults_RiskTrendGuardModePatchPreservesOtherFields(t *testing.T) {
	store := openTestSQLite(t)
	if err := SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	customRisk := `{"allow_hedge":true,"allow_scale_in":false,"max_open_positions":7,"trend_guard":{"enabled":true,"max_start_lag_bars":21},"tp":{"mode":"fixed","default_pct":0.5,"only_raise_on_update":true},"sl":{"max_loss_pct":0.05,"only_raise_on_update":true,"require_signal":true},"leverage":{"min":1,"max":50},"account":{"currency":"USDT","baseline_usdt":0},"per_trade":{"ratio":0.1},"symbol_cooldown":{"enabled":true,"consecutive_stop_loss":2,"cooldown":"6h","window":"24h"},"trade_cooldown":{"enabled":true,"loss_ratio_of_per_trade":0.5}}`
	if _, err := store.DB.Exec(`UPDATE config SET value = ? WHERE name = 'risk';`, customRisk); err != nil {
		t.Fatalf("update config.risk failed: %v", err)
	}

	plan, err := PlanDefaults(store.DB, SchemaPlan{})
	if err != nil {
		t.Fatalf("plan defaults failed: %v", err)
	}

	foundRiskPatch := false
	overrideSummaries := make([]string, 0, len(plan.Overrides))
	for _, item := range plan.Patches {
		if strings.Contains(item.Summary, "config.risk value") && strings.Contains(item.Summary, "trend_guard.mode") {
			foundRiskPatch = true
			break
		}
	}
	for _, item := range plan.Overrides {
		overrideSummaries = append(overrideSummaries, item.Summary)
		if strings.Contains(item.Summary, "trend_guard.mode") {
			t.Fatalf("unexpected override for trend_guard.mode: %s", item.Summary)
		}
	}
	if !foundRiskPatch {
		t.Fatalf("expected config.risk patch for trend_guard.mode, patches=%v", plan.Patches)
	}

	if err := ApplyDefaultAutoPlan(store.DB, plan); err != nil {
		t.Fatalf("apply default auto plan failed: %v", err)
	}

	var raw string
	if err := store.DB.QueryRow(`SELECT value FROM config WHERE name = 'risk';`).Scan(&raw); err != nil {
		t.Fatalf("query config.risk failed: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("decode patched config.risk failed: %v", err)
	}

	if got := numberToInt(cfg["max_open_positions"]); got != 7 {
		t.Fatalf("unexpected max_open_positions after patch: got %d want %d", got, 7)
	}
	if got, _ := cfg["allow_hedge"].(bool); !got {
		t.Fatalf("expected allow_hedge to remain true")
	}
	trendGuard, ok := cfg["trend_guard"].(map[string]any)
	if !ok {
		t.Fatalf("missing trend_guard object after patch: %v", cfg["trend_guard"])
	}
	if got := strings.TrimSpace(toJSONStringValue(trendGuard["mode"])); got != "grouped" {
		t.Fatalf("unexpected trend_guard.mode after patch: got %q want %q", got, "grouped")
	}
	if got := numberToFloat(trendGuard["leader_min_priority_score"]); got != 50 {
		t.Fatalf("unexpected trend_guard.leader_min_priority_score after patch: got %.2f want 50", got)
	}
	if got := numberToInt(trendGuard["max_start_lag_bars"]); got != 21 {
		t.Fatalf("unexpected trend_guard.max_start_lag_bars after patch: got %d want %d", got, 21)
	}
}

func numberToInt(v any) int {
	switch value := v.(type) {
	case float64:
		return int(value)
	case int:
		return value
	case int64:
		return int(value)
	default:
		return 0
	}
}

func numberToFloat(v any) float64 {
	switch value := v.(type) {
	case float64:
		return value
	case float32:
		return float64(value)
	case int:
		return float64(value)
	case int64:
		return float64(value)
	default:
		return 0
	}
}

func toJSONStringValue(v any) string {
	switch value := v.(type) {
	case string:
		return value
	default:
		raw, _ := json.Marshal(value)
		return strings.Trim(string(raw), `"`)
	}
}
