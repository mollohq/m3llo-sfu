// Package store keeps the media admin's history on the instance's local
// disk: the structured log, the clients' 10 s stats rows, the SFU's own
// 10 s relay rows, the session events, the session summaries and the peer
// minutes per day. One SQLite database (modernc.org/sqlite, no cgo) per SFU
// instance, kept for the retention (30 days by default).
//
// Writes never block the caller: they go into a bounded queue that one
// writer goroutine drains in batches. A full queue drops the write and
// counts it (Info().Dropped). Reads run on their own connections (WAL).
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// SchemaVersion is the version in meta.schema_version.
const SchemaVersion = 2

const (
	queueSize     = 32768
	batchMax      = 2048
	batchInterval = 250 * time.Millisecond
	sweepInterval = time.Hour
	sweepChunk    = 5000
)

// Store is one instance's history database.
type Store struct {
	db        *sql.DB
	path      string
	retention time.Duration
	now       func() time.Time

	queue   chan op
	flushCh chan chan struct{}
	stopCh  chan struct{}
	doneCh  chan struct{}
	stopped sync.Once

	dropped  atomic.Int64
	written  atomic.Int64
	failures atomic.Int64
	lastErr  atomic.Pointer[string]
}

// Options configure Open. Zero values take the defaults.
type Options struct {
	Retention time.Duration    // default 30 days
	Now       func() time.Time // tests
	// AllowUnsafeToolchain opens the store on a runtime with
	// golang/go#77975 anyway (unit tests, which run no media).
	AllowUnsafeToolchain bool
}

const schema = `
CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS logs (
	ms INTEGER NOT NULL, level INTEGER NOT NULL, event TEXT NOT NULL,
	session TEXT NOT NULL DEFAULT '', user TEXT NOT NULL DEFAULT '',
	line TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS logs_ms ON logs (ms);
CREATE INDEX IF NOT EXISTS logs_session ON logs (session, ms);
CREATE INDEX IF NOT EXISTS logs_user ON logs (user, ms);
CREATE INDEX IF NOT EXISTS logs_event ON logs (event, ms);
CREATE TABLE IF NOT EXISTS stats (
	ms INTEGER NOT NULL, session TEXT NOT NULL, user TEXT NOT NULL,
	role TEXT NOT NULL, kind TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS stats_session ON stats (session, ms);
CREATE INDEX IF NOT EXISTS stats_user ON stats (user, ms);
CREATE INDEX IF NOT EXISTS stats_ms ON stats (ms);
CREATE TABLE IF NOT EXISTS relay (
	ms INTEGER NOT NULL, session TEXT NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS relay_session ON relay (session, ms);
CREATE INDEX IF NOT EXISTS relay_ms ON relay (ms);
CREATE TABLE IF NOT EXISTS events (
	ms INTEGER NOT NULL, session TEXT NOT NULL, user TEXT NOT NULL DEFAULT '',
	kind TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '{}');
CREATE INDEX IF NOT EXISTS events_session ON events (session, ms);
CREATE INDEX IF NOT EXISTS events_user ON events (user, ms);
CREATE INDEX IF NOT EXISTS events_ms ON events (ms);
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT NOT NULL, start_ms INTEGER NOT NULL, type TEXT NOT NULL,
	crew_id TEXT NOT NULL DEFAULT '', host_user_id TEXT NOT NULL DEFAULT '',
	host_username TEXT NOT NULL DEFAULT '', synthetic INTEGER NOT NULL DEFAULT 0,
	end_ms INTEGER NOT NULL DEFAULT 0, last_seen_ms INTEGER NOT NULL DEFAULT 0,
	peers INTEGER NOT NULL DEFAULT 0, peer_minutes REAL NOT NULL DEFAULT 0,
	bytes_sent INTEGER NOT NULL DEFAULT 0, bytes_recv INTEGER NOT NULL DEFAULT 0,
	quality TEXT NOT NULL DEFAULT '', trouble TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (id, start_ms));
CREATE INDEX IF NOT EXISTS sessions_start ON sessions (start_ms);
CREATE INDEX IF NOT EXISTS sessions_end ON sessions (end_ms);
CREATE TABLE IF NOT EXISTS session_peers (
	id TEXT NOT NULL, start_ms INTEGER NOT NULL, user TEXT NOT NULL,
	username TEXT NOT NULL DEFAULT '', role TEXT NOT NULL,
	first_ms INTEGER NOT NULL, last_ms INTEGER NOT NULL,
	PRIMARY KEY (id, start_ms, user, role));
CREATE INDEX IF NOT EXISTS session_peers_user ON session_peers (user, first_ms);
CREATE TABLE IF NOT EXISTS minutes (
	day TEXT NOT NULL, type TEXT NOT NULL, role TEXT NOT NULL,
	synthetic INTEGER NOT NULL, minutes REAL NOT NULL,
	PRIMARY KEY (day, type, role, synthetic));
CREATE TABLE IF NOT EXISTS peer_minutes (
	id TEXT NOT NULL, start_ms INTEGER NOT NULL, minute_ms INTEGER NOT NULL,
	user TEXT NOT NULL, role TEXT NOT NULL, type TEXT NOT NULL,
	graded INTEGER NOT NULL, degraded INTEGER NOT NULL,
	reasons TEXT NOT NULL DEFAULT '', leg TEXT NOT NULL DEFAULT '',
	detail TEXT NOT NULL DEFAULT '{}',
	PRIMARY KEY (id, start_ms, minute_ms, user, role));
CREATE INDEX IF NOT EXISTS peer_minutes_minute ON peer_minutes (minute_ms);
CREATE INDEX IF NOT EXISTS peer_minutes_user ON peer_minutes (user, minute_ms);
CREATE TABLE IF NOT EXISTS alerts (
	ms INTEGER NOT NULL, kind TEXT NOT NULL, category TEXT NOT NULL,
	severity TEXT NOT NULL, session TEXT NOT NULL DEFAULT '',
	user TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '{}');
CREATE INDEX IF NOT EXISTS alerts_ms ON alerts (ms);
CREATE TABLE IF NOT EXISTS instance_samples (ms INTEGER NOT NULL, body TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS instance_samples_ms ON instance_samples (ms);
`

// migrations add what a later schema version needs to an older file.
var migrations = []struct {
	table, column, decl string
}{
	{"sessions", "graded_minutes", "INTEGER NOT NULL DEFAULT 0"},
	{"sessions", "degraded_minutes", "INTEGER NOT NULL DEFAULT 0"},
	{"sessions", "worst_minute_ms", "INTEGER NOT NULL DEFAULT 0"},
}

func migrate(db *sql.DB) error {
	for _, m := range migrations {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, m.table, m.column).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := db.Exec(`ALTER TABLE ` + m.table + ` ADD COLUMN ` + m.column + ` ` + m.decl); err != nil {
				return err
			}
		}
	}
	return nil
}

// Open opens (or creates) the database at path, applies the schema,
// closes the sessions a crash left open, and starts the writer.
func Open(path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("store: empty path")
	}
	if unsafeRuntime(runtime.GOOS, runtime.Version()) && !opts.AllowUnsafeToolchain {
		return nil, fmt.Errorf("store: off on %s/windows: golang/go#77975 faults the process "+
			"(build with Go 1.24, as the Dockerfile does, or Go 1.26.2 or later)", runtime.Version())
	}
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
	}
	// auto_vacuum takes effect only on a new file, before the first table:
	// deleted rows then give their pages back on an incremental vacuum.
	dsn := "file:" + filepath.ToSlash(path) +
		"?_pragma=auto_vacuum(INCREMENTAL)&_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: schema: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	if _, err := db.Exec(`INSERT INTO meta (k, v) VALUES ('schema_version', ?)
		ON CONFLICT (k) DO UPDATE SET v = excluded.v`, fmt.Sprint(SchemaVersion)); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: meta: %w", err)
	}
	// A session that was open when the process stopped ends at the last
	// time the recorder saw it.
	if _, err := db.Exec(`UPDATE sessions SET end_ms = last_seen_ms WHERE end_ms = 0`); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: close open sessions: %w", err)
	}
	s := &Store{
		db:        db,
		path:      path,
		retention: opts.Retention,
		now:       opts.Now,
		queue:     make(chan op, queueSize),
		flushCh:   make(chan chan struct{}),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
	if s.retention <= 0 {
		s.retention = 30 * 24 * time.Hour
	}
	if s.now == nil {
		s.now = time.Now
	}
	go s.run()
	return s, nil
}

// Close drains the queue, stops the writer and closes the database.
func (s *Store) Close() error {
	s.stopped.Do(func() { close(s.stopCh) })
	<-s.doneCh
	return s.db.Close()
}

// Flush returns when every write queued before the call is on disk.
func (s *Store) Flush() {
	done := make(chan struct{})
	select {
	case s.flushCh <- done:
		<-done
	case <-s.doneCh:
	}
}

// Path is the database file.
func (s *Store) Path() string { return s.path }

type op func(tx *sql.Tx) error

func (s *Store) put(o op) {
	select {
	case s.queue <- o:
	default:
		s.dropped.Add(1)
	}
}

func (s *Store) run() {
	defer close(s.doneCh)
	ticker := time.NewTicker(batchInterval)
	defer ticker.Stop()
	sweep := time.NewTicker(sweepInterval)
	defer sweep.Stop()
	s.Sweep()

	batch := make([]op, 0, batchMax)
	commit := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.apply(batch); err != nil {
			s.failures.Add(1)
			msg := err.Error()
			s.lastErr.Store(&msg)
		} else {
			s.written.Add(int64(len(batch)))
		}
		batch = batch[:0]
	}
	drain := func() {
		for {
			select {
			case o := <-s.queue:
				batch = append(batch, o)
				if len(batch) >= batchMax {
					commit()
				}
			default:
				commit()
				return
			}
		}
	}
	for {
		select {
		case o := <-s.queue:
			batch = append(batch, o)
			if len(batch) >= batchMax {
				commit()
			}
		case <-ticker.C:
			commit()
		case done := <-s.flushCh:
			drain()
			close(done)
		case <-sweep.C:
			commit()
			s.Sweep()
		case <-s.stopCh:
			drain()
			return
		}
	}
}

func (s *Store) apply(batch []op) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	for _, o := range batch {
		if err := o(tx); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// Info is the store's state for the admin overview.
type Info struct {
	Path      string `json:"path"`
	Bytes     int64  `json:"bytes"`
	Written   int64  `json:"written"`
	Dropped   int64  `json:"dropped"`
	Failures  int64  `json:"failures"`
	LastError string `json:"last_error,omitempty"`
	Retention int    `json:"retention_days"`
}

func (s *Store) Info() Info {
	info := Info{
		Path:      s.path,
		Written:   s.written.Load(),
		Dropped:   s.dropped.Load(),
		Failures:  s.failures.Load(),
		Retention: int(s.retention / (24 * time.Hour)),
	}
	if e := s.lastErr.Load(); e != nil {
		info.LastError = *e
	}
	for _, p := range []string{s.path, s.path + "-wal"} {
		if st, err := os.Stat(p); err == nil {
			info.Bytes += st.Size()
		}
	}
	return info
}

// Sweep deletes what is older than the retention and gives the pages back.
// The writer runs it at start and every hour.
func (s *Store) Sweep() {
	cutoff := s.now().Add(-s.retention).UnixMilli()
	cutoffDay := time.UnixMilli(cutoff).UTC().Format("2006-01-02")
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM logs WHERE rowid IN (SELECT rowid FROM logs WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM stats WHERE rowid IN (SELECT rowid FROM stats WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM relay WHERE rowid IN (SELECT rowid FROM relay WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM events WHERE rowid IN (SELECT rowid FROM events WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM session_peers WHERE rowid IN (SELECT rowid FROM session_peers WHERE last_ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM sessions WHERE rowid IN (SELECT rowid FROM sessions WHERE end_ms > 0 AND end_ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM minutes WHERE rowid IN (SELECT rowid FROM minutes WHERE day < ? LIMIT ?)`, []any{cutoffDay, sweepChunk}},
		{`DELETE FROM peer_minutes WHERE rowid IN (SELECT rowid FROM peer_minutes WHERE minute_ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM alerts WHERE rowid IN (SELECT rowid FROM alerts WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
		{`DELETE FROM instance_samples WHERE rowid IN (SELECT rowid FROM instance_samples WHERE ms < ? LIMIT ?)`, []any{cutoff, sweepChunk}},
	} {
		for {
			res, err := s.db.Exec(q.sql, q.args...)
			if err != nil {
				msg := err.Error()
				s.lastErr.Store(&msg)
				s.failures.Add(1)
				break
			}
			if n, _ := res.RowsAffected(); n < sweepChunk {
				break
			}
		}
	}
	_, _ = s.db.Exec(`PRAGMA incremental_vacuum(2000)`)
}
