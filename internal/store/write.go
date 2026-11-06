package store

import (
	"database/sql"
)

// LogRow is one structured log line. Line is the JSON of the entry, as the
// SFU writes it to stdout; Ms is the time of the line in unix milliseconds.
type LogRow struct {
	Ms      int64
	Level   int
	Event   string
	Session string
	User    string
	Line    []byte
}

// StatsRow is one client stats report (stream_client_stats or
// client_stats): kind "stream" or "voice", the body as the client sent it.
type StatsRow struct {
	Ms      int64
	Session string
	User    string
	Role    string
	Kind    string
	Body    []byte
}

// RelayRow is the SFU's own 10 s row of one session (JSON body).
type RelayRow struct {
	Ms      int64
	Session string
	Body    []byte
}

// EventRow is one session event: a join, a leave, a reconnect, a rung
// change, a keyframe request, an ingress loss episode.
type EventRow struct {
	Ms      int64  `json:"ms"`
	Session string `json:"session"`
	User    string `json:"user,omitempty"`
	Kind    string `json:"kind"`
	Detail  []byte `json:"-"`
}

// SessionRow is the summary of one run of a session: an id can run again
// (a voice channel, a host that streams twice), so a run is (id, start).
type SessionRow struct {
	ID           string  `json:"id"`
	StartMs      int64   `json:"start_ms"`
	Type         string  `json:"type"`
	CrewID       string  `json:"crew_id"`
	HostUserID   string  `json:"host_user_id"`
	HostUsername string  `json:"host_username"`
	Synthetic    bool    `json:"synthetic"`
	EndMs        int64   `json:"end_ms"`
	LastSeenMs   int64   `json:"last_seen_ms"`
	Peers        int     `json:"peers"`
	PeerMinutes  float64 `json:"peer_minutes"`
	BytesSent    int64   `json:"bytes_sent"`
	BytesRecv    int64   `json:"bytes_recv"`
	// The grade from the peer minutes (01-SFU.md section 8.7): quality
	// good, degraded, bad, "no data" or "not graded"; trouble the leg of
	// most degraded minutes.
	Quality         string `json:"quality"`
	Trouble         string `json:"trouble"`
	GradedMinutes   int    `json:"graded_minutes"`
	DegradedMinutes int    `json:"degraded_minutes"`
	WorstMinuteMs   int64  `json:"worst_minute_ms"`
}

// PeerMinuteRow is the grade of one peer minute.
type PeerMinuteRow struct {
	ID       string `json:"-"`
	StartMs  int64  `json:"-"`
	MinuteMs int64  `json:"minute_ms"`
	User     string `json:"user"`
	Role     string `json:"role"`
	Type     string `json:"type"`
	Graded   bool   `json:"graded"`
	Degraded bool   `json:"degraded"`
	Reasons  string `json:"reasons"`
	Leg      string `json:"leg"`
	Detail   []byte `json:"-"`
}

// AlertRow is one alert: an incident from the log, or a rule.
type AlertRow struct {
	Ms       int64  `json:"ms"`
	Kind     string `json:"kind"`
	Category string `json:"category"`
	Severity string `json:"severity"`
	Session  string `json:"session,omitempty"`
	User     string `json:"user,omitempty"`
	Detail   []byte `json:"-"`
}

// PeerRow is one user in one role in a session run.
type PeerRow struct {
	ID       string `json:"-"`
	StartMs  int64  `json:"-"`
	User     string `json:"user"`
	Username string `json:"username"`
	Role     string `json:"role"`
	FirstMs  int64  `json:"first_ms"`
	LastMs   int64  `json:"last_ms"`
}

func (s *Store) Log(r LogRow) {
	line := string(r.Line)
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO logs (ms, level, event, session, user, line) VALUES (?, ?, ?, ?, ?, ?)`,
			r.Ms, r.Level, r.Event, r.Session, r.User, line)
		return err
	})
}

func (s *Store) Stats(r StatsRow) {
	body := string(r.Body)
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO stats (ms, session, user, role, kind, body) VALUES (?, ?, ?, ?, ?, ?)`,
			r.Ms, r.Session, r.User, r.Role, r.Kind, body)
		return err
	})
}

func (s *Store) Relay(r RelayRow) {
	body := string(r.Body)
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO relay (ms, session, body) VALUES (?, ?, ?)`, r.Ms, r.Session, body)
		return err
	})
}

func (s *Store) Event(r EventRow) {
	detail := "{}"
	if len(r.Detail) > 0 {
		detail = string(r.Detail)
	}
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO events (ms, session, user, kind, detail) VALUES (?, ?, ?, ?, ?)`,
			r.Ms, r.Session, r.User, r.Kind, detail)
		return err
	})
}

// Session writes the run's summary (insert or update). EndMs 0 is a run
// that is still live.
func (s *Store) Session(r SessionRow) {
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO sessions (id, start_ms, type, crew_id, host_user_id, host_username,
			synthetic, end_ms, last_seen_ms, peers, peer_minutes, bytes_sent, bytes_recv,
			quality, trouble, graded_minutes, degraded_minutes, worst_minute_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (id, start_ms) DO UPDATE SET
				type = excluded.type, crew_id = excluded.crew_id,
				host_user_id = excluded.host_user_id, host_username = excluded.host_username,
				synthetic = excluded.synthetic, end_ms = excluded.end_ms,
				last_seen_ms = excluded.last_seen_ms, peers = excluded.peers,
				peer_minutes = excluded.peer_minutes, bytes_sent = excluded.bytes_sent,
				bytes_recv = excluded.bytes_recv, quality = excluded.quality,
				trouble = excluded.trouble, graded_minutes = excluded.graded_minutes,
				degraded_minutes = excluded.degraded_minutes, worst_minute_ms = excluded.worst_minute_ms`,
			r.ID, r.StartMs, r.Type, r.CrewID, r.HostUserID, r.HostUsername, boolInt(r.Synthetic),
			r.EndMs, r.LastSeenMs, r.Peers, r.PeerMinutes, r.BytesSent, r.BytesRecv,
			r.Quality, r.Trouble, r.GradedMinutes, r.DegradedMinutes, r.WorstMinuteMs)
		return err
	})
}

// Peer writes one user's presence in a run (insert or widen).
func (s *Store) Peer(r PeerRow) {
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO session_peers (id, start_ms, user, username, role, first_ms, last_ms)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT (id, start_ms, user, role) DO UPDATE SET
				username = excluded.username,
				first_ms = MIN(session_peers.first_ms, excluded.first_ms),
				last_ms = MAX(session_peers.last_ms, excluded.last_ms)`,
			r.ID, r.StartMs, r.User, r.Username, r.Role, r.FirstMs, r.LastMs)
		return err
	})
}

// SetGrade writes a run's grade (the backfill).
func (s *Store) SetGrade(id string, startMs int64, quality, trouble string, graded, degraded int, worstMs int64) {
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE sessions SET quality = ?, trouble = ?, graded_minutes = ?,
			degraded_minutes = ?, worst_minute_ms = ? WHERE id = ? AND start_ms = ?`,
			quality, trouble, graded, degraded, worstMs, id, startMs)
		return err
	})
}

// PeerMinute writes the grade of one peer minute (insert or replace).
func (s *Store) PeerMinute(r PeerMinuteRow) {
	detail := "{}"
	if len(r.Detail) > 0 {
		detail = string(r.Detail)
	}
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT OR REPLACE INTO peer_minutes (id, start_ms, minute_ms, user, role, type,
			graded, degraded, reasons, leg, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			r.ID, r.StartMs, r.MinuteMs, r.User, r.Role, r.Type, boolInt(r.Graded), boolInt(r.Degraded),
			r.Reasons, r.Leg, detail)
		return err
	})
}

// Alert writes one alert.
func (s *Store) Alert(r AlertRow) {
	detail := "{}"
	if len(r.Detail) > 0 {
		detail = string(r.Detail)
	}
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO alerts (ms, kind, category, severity, session, user, detail)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, r.Ms, r.Kind, r.Category, r.Severity, r.Session, r.User, detail)
		return err
	})
}

// InstanceSample writes the instance's 10 s sample (JSON body).
func (s *Store) InstanceSample(ms int64, body []byte) {
	b := string(body)
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO instance_samples (ms, body) VALUES (?, ?)`, ms, b)
		return err
	})
}

// AddMinutes adds peer minutes to a day row.
func (s *Store) AddMinutes(day, typ, role string, synthetic bool, minutes float64) {
	s.put(func(tx *sql.Tx) error {
		_, err := tx.Exec(`INSERT INTO minutes (day, type, role, synthetic, minutes) VALUES (?, ?, ?, ?, ?)
			ON CONFLICT (day, type, role, synthetic) DO UPDATE SET minutes = minutes.minutes + excluded.minutes`,
			day, typ, role, boolInt(synthetic), minutes)
		return err
	})
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
