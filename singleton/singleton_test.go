package singleton

import (
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func openSingletonTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "singleton.db"))
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := EnsureTable(db); err != nil {
		_ = db.Close()
		t.Fatalf("ensure singleton table failed: %v", err)
	}
	return db
}

func TestAcquireScopesLockByMode(t *testing.T) {
	db := openSingletonTestDB(t)
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close sqlite failed: %v", err)
		}
	}()

	liveA := NewManager(db, 5*time.Second)
	if _, err := liveA.Acquire("v1", "live", "test"); err != nil {
		t.Fatalf("acquire live lock failed: %v", err)
	}
	defer func() {
		if err := liveA.Release(StatusCompleted); err != nil {
			t.Fatalf("release live lock failed: %v", err)
		}
	}()

	liveB := NewManager(db, 5*time.Second)
	if _, err := liveB.Acquire("v1", "live", "test"); !errors.Is(err, ErrLocked) {
		t.Fatalf("second live acquire err=%v, want %v", err, ErrLocked)
	}

	paper := NewManager(db, 5*time.Second)
	if _, err := paper.Acquire("v1", "paper", "test"); err != nil {
		t.Fatalf("acquire paper lock failed: %v", err)
	}
	defer func() {
		if err := paper.Release(StatusCompleted); err != nil {
			t.Fatalf("release paper lock failed: %v", err)
		}
	}()
}
