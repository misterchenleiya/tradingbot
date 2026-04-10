package singleton

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Lock struct {
	ID          int64
	UUID        string
	Version     string
	Mode        string
	Source      string
	Status      string
	Created     int64
	Updated     int64
	Closed      sql.NullInt64
	Heartbeat   int64
	LeaseExpiry int64
	StartTime   string
	EndTime     sql.NullString
	Runtime     sql.NullString
}

type Manager struct {
	db        *sql.DB
	id        int64
	uuid      string
	ttl       time.Duration
	startedAt time.Time
}

func NewManager(db *sql.DB, ttl time.Duration) *Manager {
	return &Manager{db: db, ttl: ttl}
}

func EnsureTable(db *sql.DB) error {
	if db == nil {
		return errors.New("nil db")
	}
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS singleton (
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
	);`)
	if err != nil {
		return err
	}
	if err := ensureColumn(db, "singleton", "start_time", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "singleton", "end_time", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "singleton", "runtime", "TEXT"); err != nil {
		return err
	}
	return nil
}

var ErrLocked = errors.New("another instance is running")
var ErrLockReleased = errors.New("lock released")

var heartbeatRetryDelays = []time.Duration{
	100 * time.Millisecond,
	200 * time.Millisecond,
	500 * time.Millisecond,
	1 * time.Second,
}

const (
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusAborted   = "aborted"
)

func (m *Manager) Acquire(version, mode, source string) (*Lock, error) {
	if m.db == nil {
		return nil, errors.New("nil db")
	}
	if mode == "" {
		return nil, errors.New("mode required")
	}

	ctx := context.Background()
	var out *Lock
	if err := withImmediateTx(ctx, m.db, func(conn *sql.Conn) error {
		nowTime := time.Now().UTC()
		now := nowTime.Unix()
		startTime := formatSingletonTime(nowTime)
		var count int
		if err := conn.QueryRowContext(
			ctx,
			`SELECT COUNT(1) FROM singleton WHERE mode = ? AND closed IS NULL AND lease_expires > ?;`,
			mode, now,
		).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			return ErrLocked
		}
		if _, err := conn.ExecContext(
			ctx,
			`UPDATE singleton SET closed = ?, updated = ?, status = ?
			 WHERE closed IS NULL AND lease_expires <= ?;`,
			now, now, StatusAborted, now,
		); err != nil {
			return err
		}
		id := uuid.NewString()
		leaseExpires := nowTime.Add(m.ttl).Unix()
		res, err := conn.ExecContext(
			ctx,
			`INSERT INTO singleton (
				 uuid, version, mode, source, status, created, updated, closed, heartbeat, lease_expires, start_time
			 )
			 VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?);`,
			id, version, mode, source, StatusRunning, now, now, now, leaseExpires, startTime,
		)
		if err != nil {
			return err
		}
		singletonID, err := res.LastInsertId()
		if err != nil {
			return fmt.Errorf("fetch singleton id: %w", err)
		}
		out = &Lock{
			ID:          singletonID,
			UUID:        id,
			Version:     version,
			Mode:        mode,
			Source:      source,
			Status:      StatusRunning,
			Created:     now,
			Updated:     now,
			Heartbeat:   now,
			LeaseExpiry: leaseExpires,
			StartTime:   startTime,
		}
		return nil
	}); err != nil {
		return nil, err
	}

	m.id = out.ID
	m.uuid = out.UUID
	m.startedAt = time.Unix(out.Created, 0).UTC()
	return out, nil
}

func (m *Manager) Heartbeat() error {
	if m.db == nil || m.id == 0 {
		return errors.New("lock not acquired")
	}
	for attempt := 0; ; attempt++ {
		err := m.heartbeatOnce()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrLockReleased) || !isSQLiteBusyLockError(err) || attempt >= len(heartbeatRetryDelays) {
			return err
		}
		time.Sleep(heartbeatRetryDelays[attempt])
	}
}

func (m *Manager) heartbeatOnce() error {
	nowTime := time.Now().UTC()
	now := nowTime.Unix()
	lease := nowTime.Add(m.ttl).Unix()
	res, err := m.db.Exec(
		`UPDATE singleton SET updated = ?, heartbeat = ?, lease_expires = ? WHERE id = ? AND closed IS NULL;`,
		now, now, lease, m.id,
	)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrLockReleased
	}
	return nil
}

func isSQLiteBusyLockError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "database is locked") ||
		strings.Contains(msg, "database table is locked") ||
		strings.Contains(msg, "database schema is locked") ||
		strings.Contains(msg, "database is busy")
}

func (m *Manager) Release(status string) error {
	if m.db == nil || m.id == 0 {
		return nil
	}
	if status == "" {
		status = StatusCompleted
	}
	endTime := time.Now().UTC()
	now := endTime.Unix()
	endTimeText := formatSingletonTime(endTime)
	runtime := ""
	if !m.startedAt.IsZero() {
		runtime = formatSingletonRuntime(endTime.Sub(m.startedAt))
	}
	_, err := m.db.Exec(
		`UPDATE singleton SET closed = ?, updated = ?, status = ?, end_time = ?, runtime = ? WHERE id = ? AND closed IS NULL;`,
		now, now, status, endTimeText, runtime, m.id,
	)
	return err
}

func formatSingletonTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05")
}

func formatSingletonRuntime(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	totalSeconds := int64(d.Seconds())
	days := totalSeconds / 86400
	hours := (totalSeconds % 86400) / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60
	return fmt.Sprintf("%dd%dh%dm%ds", days, hours, minutes, seconds)
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

func withImmediateTx(ctx context.Context, db *sql.DB, fn func(*sql.Conn) error) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE;"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_, _ = conn.ExecContext(ctx, "ROLLBACK;")
	}()

	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT;"); err != nil {
		return err
	}
	committed = true
	return nil
}
