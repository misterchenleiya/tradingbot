package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/misterchenleiya/tradingbot/internal/models"
	"github.com/misterchenleiya/tradingbot/singleton"
	"github.com/misterchenleiya/tradingbot/storage"
	"go.uber.org/zap"
)

func TestLoadExchangeModeConfig(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gobot.db")
	store := storage.NewSQLite(storage.Config{Path: dbPath, Logger: zap.NewNop()})
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
	if err := storage.SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	cfg, err := loadExchangeModeConfig(store)
	if err != nil {
		t.Fatalf("load exchange runtime config failed: %v", err)
	}
	if cfg.FetchUnclosedOHLCV {
		t.Fatalf("expected default fetch_unclosed_ohlcv=false")
	}

	if err := store.UpsertConfigValue("exchange", `{"fetch_unclosed_ohlcv":true}`, "test"); err != nil {
		t.Fatalf("set exchange config failed: %v", err)
	}
	cfg, err = loadExchangeModeConfig(store)
	if err != nil {
		t.Fatalf("reload exchange runtime config failed: %v", err)
	}
	if !cfg.FetchUnclosedOHLCV {
		t.Fatalf("expected fetch_unclosed_ohlcv=true after update")
	}
}

func TestLoadStrategyCombosNormalizesAndKeepsSingleTradeCombo(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gobot.db")
	store := storage.NewSQLite(storage.Config{Path: dbPath, Logger: zap.NewNop()})
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
	if err := storage.SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}
	if err := store.UpsertConfigValue("strategy", `{"live":["turtle"],"paper":["turtle"],"back-test":["turtle"],"combo":[{"timeframes":[" 15m ","3m","1h"],"trade_enabled":true},{"timeframes":["1d","4h","1h"],"trade_enabled":false}]}`, "test"); err != nil {
		t.Fatalf("set strategy config failed: %v", err)
	}

	combos, err := loadStrategyCombos(store)
	if err != nil {
		t.Fatalf("load strategy combos failed: %v", err)
	}
	want := []models.StrategyComboConfig{
		{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true},
		{Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: false},
	}
	if len(combos) != len(want) {
		t.Fatalf("unexpected combo count: got %d want %d", len(combos), len(want))
	}
	for i := range want {
		if len(combos[i].Timeframes) != len(want[i].Timeframes) {
			t.Fatalf("combo[%d] timeframe count mismatch: got %v want %v", i, combos[i].Timeframes, want[i].Timeframes)
		}
		for j := range want[i].Timeframes {
			if combos[i].Timeframes[j] != want[i].Timeframes[j] {
				t.Fatalf("combo[%d].timeframes[%d]=%q want %q", i, j, combos[i].Timeframes[j], want[i].Timeframes[j])
			}
		}
		if combos[i].TradeEnabled != want[i].TradeEnabled {
			t.Fatalf("combo[%d].trade_enabled=%v want %v", i, combos[i].TradeEnabled, want[i].TradeEnabled)
		}
	}
}

func TestNormalizeStrategyComboConfigsRejectsMultipleTradeCombos(t *testing.T) {
	_, err := normalizeStrategyComboConfigs([]models.StrategyComboConfig{
		{Timeframes: []string{"3m", "15m", "1h"}, TradeEnabled: true},
		{Timeframes: []string{"1h", "4h", "1d"}, TradeEnabled: true},
	})
	if err == nil || !strings.Contains(err.Error(), "more than one trade_enabled=true combo") {
		t.Fatalf("expected multiple trade combo error, got %v", err)
	}
}

func TestLoadLogConfigDefaultWithLogFileEnabled(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gobot.db")
	store := storage.NewSQLite(storage.Config{Path: dbPath, Logger: zap.NewNop()})
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
	if err := storage.SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}

	cfg, err := loadLogConfig(store)
	if err != nil {
		t.Fatalf("load log config failed: %v", err)
	}
	if !cfg.File {
		t.Fatalf("expected default with_log_file=1 to enable file logging")
	}

	value, found, err := store.GetConfigValue("with_log_file")
	if err != nil {
		t.Fatalf("get with_log_file failed: %v", err)
	}
	if !found {
		t.Fatalf("expected config.with_log_file to be seeded")
	}
	if value != "1" {
		t.Fatalf("expected config.with_log_file=1, got=%q", value)
	}
}

func TestLoadLogConfigMissingWithLogFileDefaultsToNoFile(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "gobot.db")
	store := storage.NewSQLite(storage.Config{Path: dbPath, Logger: zap.NewNop()})
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
	if err := storage.SeedDefaults(store.DB); err != nil {
		t.Fatalf("seed defaults failed: %v", err)
	}
	if _, err := store.DB.Exec(`DELETE FROM config WHERE name = 'with_log_file';`); err != nil {
		t.Fatalf("delete with_log_file failed: %v", err)
	}

	cfg, err := loadLogConfig(store)
	if err != nil {
		t.Fatalf("load log config failed: %v", err)
	}
	if cfg.File {
		t.Fatalf("expected missing config.with_log_file to disable file logging for legacy db")
	}
}

func TestExtractTimestampFromLogFilePathSupportsOldAndNewFormats(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "new format",
			path: filepath.Join("/tmp", "2026-03-30_153045_gobot.log"),
			want: "20260330_153045",
		},
		{
			name: "old format",
			path: filepath.Join("/tmp", "gobot_20260330_153045.log"),
			want: "20260330_153045",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractTimestampFromLogFilePath(tt.path)
			if err != nil {
				t.Fatalf("extractTimestampFromLogFilePath() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("extractTimestampFromLogFilePath() = %q want %q", got, tt.want)
			}
		})
	}
}

func TestFormatInitConfirmBlock(t *testing.T) {
	block := formatInitConfirmBlock(initConfirmPrompt{
		Stage:     "Schema Risk",
		Header:    "以下变更可能导致数据丢失：",
		Lines:     []string{"rebuild table exchanges (extra columns: legacy)"},
		YesAction: "执行以上 schema 风险变更",
		NoAction:  "跳过以上 schema 风险变更，继续后续检查",
	})

	wantContains := []string{
		"[init confirm] Schema Risk",
		"以下变更可能导致数据丢失：",
		"  - rebuild table exchanges (extra columns: legacy)",
		"说明：",
		"  - 选择 y：执行以上 schema 风险变更",
		"  - 选择 n：跳过以上 schema 风险变更，继续后续检查",
	}
	for _, item := range wantContains {
		if !strings.Contains(block, item) {
			t.Fatalf("confirm block missing %q, block=%q", item, block)
		}
	}
}

func TestSingletonHeartbeatRetriesSQLiteBusyLock(t *testing.T) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "singleton.db")

	managerDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open manager db failed: %v", err)
	}
	t.Cleanup(func() {
		if err := managerDB.Close(); err != nil {
			t.Fatalf("close manager db failed: %v", err)
		}
	})

	lockDB, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open lock db failed: %v", err)
	}
	t.Cleanup(func() {
		if err := lockDB.Close(); err != nil {
			t.Fatalf("close lock db failed: %v", err)
		}
	})

	if err := singleton.EnsureTable(managerDB); err != nil {
		t.Fatalf("ensure singleton table failed: %v", err)
	}

	manager := singleton.NewManager(managerDB, time.Second)
	lock, err := manager.Acquire("test", "live", "")
	if err != nil {
		t.Fatalf("acquire singleton lock failed: %v", err)
	}
	t.Cleanup(func() {
		if err := manager.Release(singleton.StatusCompleted); err != nil && !strings.Contains(err.Error(), "database is locked") {
			t.Fatalf("release singleton lock failed: %v", err)
		}
	})

	ctx := context.Background()
	conn, err := lockDB.Conn(ctx)
	if err != nil {
		t.Fatalf("open lock connection failed: %v", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE;`); err != nil {
		t.Fatalf("begin immediate failed: %v", err)
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK;`)
	}()
	if _, err := conn.ExecContext(ctx, `UPDATE singleton SET updated = updated WHERE id = ?;`, lock.ID); err != nil {
		t.Fatalf("hold singleton write lock failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- manager.Heartbeat()
	}()

	time.Sleep(250 * time.Millisecond)
	if _, err := conn.ExecContext(ctx, `COMMIT;`); err != nil {
		t.Fatalf("commit write lock failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("heartbeat should succeed after transient busy lock, got=%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("heartbeat did not finish after releasing transient busy lock")
	}
}
