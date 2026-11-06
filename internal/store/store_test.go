package store

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func openTest(t *testing.T, now func() time.Time) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "media.db")
	s, err := Open(path, Options{Now: now, AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestSchemaIsCreatedAndReopens(t *testing.T) {
	s, path := openTest(t, nil)
	var version string
	if err := s.db.QueryRow(`SELECT v FROM meta WHERE k = 'schema_version'`).Scan(&version); err != nil {
		t.Fatalf("schema_version: %v", err)
	}
	if version != "2" {
		t.Fatalf("schema_version = %q, want 2", version)
	}
	counts, err := s.Counts()
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	for _, table := range []string{"logs", "stats", "relay", "events", "sessions", "session_peers", "minutes",
		"peer_minutes", "alerts", "instance_samples"} {
		if _, ok := counts[table]; !ok {
			t.Errorf("no table %s", table)
		}
	}
	var vacuum int
	if err := s.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&vacuum); err != nil || vacuum != 2 {
		t.Errorf("auto_vacuum = %d (%v), want 2 (incremental)", vacuum, err)
	}
	s.Close()
	again, err := Open(path, Options{AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	again.Close()
}

func TestLogsKeepTheRingFiltersAndMilliseconds(t *testing.T) {
	s, _ := openTest(t, nil)
	base := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC).UnixMilli()
	line := func(event, session, user string) []byte {
		b, _ := json.Marshal(map[string]string{"ts": "2026-10-25T12:00:00Z", "level": "info", "event": event,
			"session": session, "user": user})
		return b
	}
	s.Log(LogRow{Ms: base + 1, Level: 1, Event: "peer_joined", Session: "a", User: "u1", Line: line("peer_joined", "a", "u1")})
	s.Log(LogRow{Ms: base + 2, Level: 2, Event: "webrtc_failed", Session: "a", User: "u2", Line: line("webrtc_failed", "a", "u2")})
	s.Log(LogRow{Ms: base + 3, Level: 1, Event: "peer_joined", Session: "b", User: "u1", Line: line("peer_joined", "b", "u1")})
	s.Log(LogRow{Ms: base + 4, Level: 0, Event: "pc_state", Session: "a", User: "u1", Line: line("pc_state", "a", "u1")})
	s.Flush()

	cases := []struct {
		name string
		q    LogQuery
		want []int64
	}{
		{"info and up, newest first", LogQuery{Level: 1}, []int64{base + 3, base + 2, base + 1}},
		{"debug", LogQuery{Level: 0}, []int64{base + 4, base + 3, base + 2, base + 1}},
		{"warn", LogQuery{Level: 2}, []int64{base + 2}},
		{"session", LogQuery{Level: 1, Session: "a"}, []int64{base + 2, base + 1}},
		{"user", LogQuery{Level: 1, User: "u1"}, []int64{base + 3, base + 1}},
		{"event", LogQuery{Level: 1, Event: "peer_joined"}, []int64{base + 3, base + 1}},
		{"from inclusive, to exclusive", LogQuery{Level: 0, FromMs: base + 2, ToMs: base + 4}, []int64{base + 3, base + 2}},
		{"limit", LogQuery{Level: 0, Limit: 1}, []int64{base + 4}},
	}
	for _, c := range cases {
		got, err := s.Logs(c.q)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var ms []int64
		for _, raw := range got {
			var v struct {
				Ms    int64  `json:"ms"`
				Event string `json:"event"`
			}
			if err := json.Unmarshal(raw, &v); err != nil {
				t.Fatalf("%s: line %s: %v", c.name, raw, err)
			}
			if v.Event == "" {
				t.Fatalf("%s: line lost its fields: %s", c.name, raw)
			}
			ms = append(ms, v.Ms)
		}
		if len(ms) != len(c.want) {
			t.Fatalf("%s: got %v, want %v", c.name, ms, c.want)
		}
		for i := range ms {
			if ms[i] != c.want[i] {
				t.Fatalf("%s: got %v, want %v", c.name, ms, c.want)
			}
		}
	}
}

func TestRetentionSweepsOnlyOldRows(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	s, _ := openTest(t, func() time.Time { return now })
	old := now.Add(-31 * 24 * time.Hour).UnixMilli()
	fresh := now.Add(-29 * 24 * time.Hour).UnixMilli()
	for _, ms := range []int64{old, fresh} {
		s.Log(LogRow{Ms: ms, Level: 1, Event: "e", Line: []byte(`{"event":"e"}`)})
		s.Stats(StatsRow{Ms: ms, Session: "s", User: "u", Role: "viewer", Kind: "stream", Body: []byte(`{}`)})
		s.Relay(RelayRow{Ms: ms, Session: "s", Body: []byte(`{}`)})
		s.Event(EventRow{Ms: ms, Session: "s", Kind: "join"})
		s.Session(SessionRow{ID: "s", StartMs: ms - 1000, Type: "stream", EndMs: ms, LastSeenMs: ms})
		s.Peer(PeerRow{ID: "s", StartMs: ms - 1000, User: "u", Role: "viewer", FirstMs: ms - 1000, LastMs: ms})
		day := time.UnixMilli(ms).UTC().Format("2006-01-02")
		s.AddMinutes(day, "stream", "viewer", false, 1)
	}
	// A run that is still live is never swept, however old.
	s.Session(SessionRow{ID: "live", StartMs: old, Type: "voice", LastSeenMs: old})
	s.Flush()
	s.Sweep()

	counts, err := s.Counts()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"logs": 1, "stats": 1, "relay": 1, "events": 1, "sessions": 2, "session_peers": 1, "minutes": 1}
	for table, n := range want {
		if counts[table] != n {
			t.Errorf("%s: %d rows after the sweep, want %d", table, counts[table], n)
		}
	}
}

func TestOpenClosesTheRunsACrashLeftOpen(t *testing.T) {
	s, path := openTest(t, nil)
	s.Session(SessionRow{ID: "v", StartMs: 1000, Type: "voice", LastSeenMs: 61_000, Peers: 2})
	s.Flush()
	s.Close()

	again, err := Open(path, Options{AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	run, ok, err := again.SessionRun("v", 0)
	if err != nil || !ok {
		t.Fatalf("run: %v %v", ok, err)
	}
	if run.EndMs != 61_000 {
		t.Fatalf("end_ms = %d, want the last seen 61000", run.EndMs)
	}
}

func TestSessionsSelectByRangeUserAndCrew(t *testing.T) {
	s, _ := openTest(t, nil)
	s.Session(SessionRow{ID: "a", StartMs: 100, EndMs: 200, Type: "stream", CrewID: "c1"})
	s.Session(SessionRow{ID: "b", StartMs: 300, EndMs: 400, Type: "voice", CrewID: "c2"})
	s.Session(SessionRow{ID: "c", StartMs: 500, Type: "stream", CrewID: "c1"}) // live
	s.Peer(PeerRow{ID: "b", StartMs: 300, User: "kattis", Role: "member", FirstMs: 300, LastMs: 400})
	// The same id runs again: a second run, its own row.
	s.Session(SessionRow{ID: "a", StartMs: 600, EndMs: 700, Type: "stream", CrewID: "c1"})
	s.Flush()

	ids := func(q SessionQuery) []string {
		rows, err := s.Sessions(q)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, r := range rows {
			out = append(out, r.ID)
		}
		return out
	}
	check := func(name string, got, want []string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("%s: %v, want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s: %v, want %v", name, got, want)
			}
		}
	}
	check("all, newest first", ids(SessionQuery{}), []string{"a", "c", "b", "a"})
	check("overlap 250..450", ids(SessionQuery{FromMs: 250, ToMs: 450}), []string{"b"})
	check("from 450 keeps the live run", ids(SessionQuery{FromMs: 450}), []string{"a", "c"})
	check("type", ids(SessionQuery{Type: "voice"}), []string{"b"})
	check("crew", ids(SessionQuery{Crew: "c1", ToMs: 550}), []string{"c", "a"})
	check("user", ids(SessionQuery{User: "kattis"}), []string{"b"})

	first, ok, _ := s.SessionRun("a", 100)
	latest, _, _ := s.SessionRun("a", 0)
	if !ok || first.EndMs != 200 || latest.StartMs != 600 {
		t.Fatalf("runs of a: first %+v, latest %+v", first, latest)
	}
}

func TestMinutesAddUp(t *testing.T) {
	s, _ := openTest(t, nil)
	s.AddMinutes("2026-10-25", "stream", "viewer", false, 1.5)
	s.AddMinutes("2026-10-25", "stream", "viewer", false, 2)
	s.AddMinutes("2026-10-25", "stream", "viewer", true, 4)
	s.Flush()
	rows, err := s.Minutes("2026-10-25", "2026-10-25")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].Minutes != 3.5 || rows[1].Minutes != 4 || !rows[1].Synthetic {
		t.Fatalf("minutes = %+v", rows)
	}
}

// On a runtime with golang/go#77975 the store stays off.
func TestOpenRefusesTheUnsafeToolchain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.db")
	s, err := Open(path, Options{})
	if unsafeRuntime(runtime.GOOS, runtime.Version()) {
		if err == nil {
			s.Close()
			t.Fatal("the store opened on an unsafe toolchain")
		}
		return
	}
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	s.Close()
}

// A version 1 file gets the grade columns on open, and keeps its rows.
func TestOpenMigratesAVersion1File(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.db")
	now := time.Now().UnixMilli()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE sessions (
		id TEXT NOT NULL, start_ms INTEGER NOT NULL, type TEXT NOT NULL,
		crew_id TEXT NOT NULL DEFAULT '', host_user_id TEXT NOT NULL DEFAULT '',
		host_username TEXT NOT NULL DEFAULT '', synthetic INTEGER NOT NULL DEFAULT 0,
		end_ms INTEGER NOT NULL DEFAULT 0, last_seen_ms INTEGER NOT NULL DEFAULT 0,
		peers INTEGER NOT NULL DEFAULT 0, peer_minutes REAL NOT NULL DEFAULT 0,
		bytes_sent INTEGER NOT NULL DEFAULT 0, bytes_recv INTEGER NOT NULL DEFAULT 0,
		quality TEXT NOT NULL DEFAULT '', trouble TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (id, start_ms));
		INSERT INTO sessions (id, start_ms, type, end_ms, last_seen_ms) VALUES ('old', ?, 'voice', ?, ?);`,
		now, now+5000, now+5000); err != nil {
		t.Fatal(err)
	}
	db.Close()

	s, err := Open(path, Options{AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	defer s.Close()
	run, ok, err := s.SessionRun("old", 0)
	if err != nil || !ok || run.GradedMinutes != 0 || run.Quality != "" {
		t.Fatalf("v1 run after migration: %+v %v %v", run, ok, err)
	}
	s.SetGrade("old", now, "good", "", 3, 0, 0)
	s.Flush()
	run, _, _ = s.SessionRun("old", 0)
	if run.Quality != "good" || run.GradedMinutes != 3 {
		t.Fatalf("grade not written: %+v", run)
	}
}

// Only Windows with Go 1.26.0 or 1.26.1 has golang/go#77975.
func TestUnsafeRuntimeIsWindowsGo1260And1261Only(t *testing.T) {
	cases := []struct {
		goos, version string
		want          bool
	}{
		{"windows", "go1.26.0", true},
		{"windows", "go1.26.1", true},
		{"windows", "go1.26", true},
		{"windows", "go1.26rc2", true},
		{"windows", "go1.26.1 X:nocoverageredesign", true},
		{"windows", "go1.26.2", false},
		{"windows", "go1.26.10", false},
		{"windows", "go1.27.0", false},
		{"windows", "go1.24.9", false},
		{"windows", "go1.25.3", false},
		{"windows", "devel go1.27-abc", false},
		{"linux", "go1.26.1", false},
		{"darwin", "go1.26.0", false},
	}
	for _, c := range cases {
		if got := unsafeRuntime(c.goos, c.version); got != c.want {
			t.Errorf("unsafeRuntime(%q, %q) = %v, want %v", c.goos, c.version, got, c.want)
		}
	}
}
