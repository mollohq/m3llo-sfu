package store

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
)

// LogQuery has the ring's filters (01-SFU.md section 8.2) plus a time
// range in unix milliseconds: FromMs inclusive, ToMs exclusive, 0 = open.
type LogQuery struct {
	Level   int
	Event   string
	Session string
	User    string
	FromMs  int64
	ToMs    int64
	Limit   int
}

// Logs returns the matching lines, newest first. Each line is the stored
// entry with its "ms" first, so a page that ends at a line can ask for
// ToMs = that ms and never split a second.
func (s *Store) Logs(q LogQuery) ([]json.RawMessage, error) {
	where, args := []string{"level >= ?"}, []any{q.Level}
	if q.Event != "" {
		where, args = append(where, "event = ?"), append(args, q.Event)
	}
	if q.Session != "" {
		where, args = append(where, "session = ?"), append(args, q.Session)
	}
	if q.User != "" {
		where, args = append(where, "user = ?"), append(args, q.User)
	}
	if q.FromMs > 0 {
		where, args = append(where, "ms >= ?"), append(args, q.FromMs)
	}
	if q.ToMs > 0 {
		where, args = append(where, "ms < ?"), append(args, q.ToMs)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT ms, line FROM logs WHERE `+strings.Join(where, " AND ")+
		` ORDER BY ms DESC, rowid DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []json.RawMessage{}
	for rows.Next() {
		var ms int64
		var line string
		if err := rows.Scan(&ms, &line); err != nil {
			return nil, err
		}
		out = append(out, withMs(ms, line))
	}
	return out, rows.Err()
}

// withMs puts "ms" first in a JSON object line.
func withMs(ms int64, line string) json.RawMessage {
	rest := strings.TrimPrefix(strings.TrimSpace(line), "{")
	sep := ","
	if strings.HasPrefix(rest, "}") {
		sep = ""
	}
	return json.RawMessage(`{"ms":` + strconv.FormatInt(ms, 10) + sep + rest)
}

// SessionQuery selects runs that overlap [FromMs, ToMs].
type SessionQuery struct {
	FromMs  int64
	ToMs    int64
	Type    string
	User    string
	Crew    string
	Quality string
	Limit   int
}

const sessionCols = `id, start_ms, type, crew_id, host_user_id, host_username, synthetic,
	end_ms, last_seen_ms, peers, peer_minutes, bytes_sent, bytes_recv, quality, trouble,
	graded_minutes, degraded_minutes, worst_minute_ms`

func scanSession(sc interface{ Scan(...any) error }) (SessionRow, error) {
	var r SessionRow
	var synthetic int
	err := sc.Scan(&r.ID, &r.StartMs, &r.Type, &r.CrewID, &r.HostUserID, &r.HostUsername, &synthetic,
		&r.EndMs, &r.LastSeenMs, &r.Peers, &r.PeerMinutes, &r.BytesSent, &r.BytesRecv, &r.Quality, &r.Trouble,
		&r.GradedMinutes, &r.DegradedMinutes, &r.WorstMinuteMs)
	r.Synthetic = synthetic != 0
	return r, err
}

// Sessions returns the runs in the range, newest first.
func (s *Store) Sessions(q SessionQuery) ([]SessionRow, error) {
	where, args := []string{"1 = 1"}, []any{}
	if q.ToMs > 0 {
		where, args = append(where, "start_ms <= ?"), append(args, q.ToMs)
	}
	if q.FromMs > 0 {
		where, args = append(where, "(end_ms = 0 OR end_ms >= ?)"), append(args, q.FromMs)
	}
	if q.Type != "" {
		where, args = append(where, "type = ?"), append(args, q.Type)
	}
	if q.Crew != "" {
		where, args = append(where, "crew_id = ?"), append(args, q.Crew)
	}
	if q.Quality != "" {
		where, args = append(where, "quality = ?"), append(args, q.Quality)
	}
	if q.User != "" {
		where = append(where, `EXISTS (SELECT 1 FROM session_peers p
			WHERE p.id = sessions.id AND p.start_ms = sessions.start_ms AND p.user = ?)`)
		args = append(args, q.User)
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT `+sessionCols+` FROM sessions WHERE `+strings.Join(where, " AND ")+
		` ORDER BY start_ms DESC LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SessionRow{}
	for rows.Next() {
		r, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SessionRun returns one run: the one that started at startMs, or with
// startMs 0 the latest run of the id. ok is false when there is none.
func (s *Store) SessionRun(id string, startMs int64) (SessionRow, bool, error) {
	var row *sql.Row
	if startMs > 0 {
		row = s.db.QueryRow(`SELECT `+sessionCols+` FROM sessions WHERE id = ? AND start_ms = ?`, id, startMs)
	} else {
		row = s.db.QueryRow(`SELECT `+sessionCols+` FROM sessions WHERE id = ? ORDER BY start_ms DESC LIMIT 1`, id)
	}
	r, err := scanSession(row)
	if err == sql.ErrNoRows {
		return SessionRow{}, false, nil
	}
	return r, err == nil, err
}

// Peers returns the users of a run.
func (s *Store) Peers(id string, startMs int64) ([]PeerRow, error) {
	rows, err := s.db.Query(`SELECT user, username, role, first_ms, last_ms FROM session_peers
		WHERE id = ? AND start_ms = ? ORDER BY first_ms`, id, startMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PeerRow{}
	for rows.Next() {
		r := PeerRow{ID: id, StartMs: startMs}
		if err := rows.Scan(&r.User, &r.Username, &r.Role, &r.FirstMs, &r.LastMs); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TimelineRow is one stats or relay row with its body.
type TimelineRow struct {
	Ms   int64           `json:"ms"`
	User string          `json:"user,omitempty"`
	Role string          `json:"role,omitempty"`
	Kind string          `json:"kind,omitempty"`
	Body json.RawMessage `json:"body"`
}

// Timeline is what the session page draws: the clients' rows, the SFU's
// rows and the events of a session between fromMs and toMs (inclusive).
type Timeline struct {
	Stats  []TimelineRow `json:"stats"`
	Relay  []TimelineRow `json:"relay"`
	Events []Event       `json:"events"`
}

// Event is an event row as the API returns it.
type Event struct {
	Ms     int64           `json:"ms"`
	User   string          `json:"user,omitempty"`
	Kind   string          `json:"kind"`
	Detail json.RawMessage `json:"detail"`
}

func (s *Store) Timeline(session string, fromMs, toMs int64) (Timeline, error) {
	t := Timeline{Stats: []TimelineRow{}, Relay: []TimelineRow{}}
	rows, err := s.db.Query(`SELECT ms, user, role, kind, body FROM stats
		WHERE session = ? AND ms >= ? AND ms <= ? ORDER BY ms`, session, fromMs, toMs)
	if err != nil {
		return t, err
	}
	for rows.Next() {
		var r TimelineRow
		var body string
		if err := rows.Scan(&r.Ms, &r.User, &r.Role, &r.Kind, &body); err != nil {
			rows.Close()
			return t, err
		}
		r.Body = json.RawMessage(body)
		t.Stats = append(t.Stats, r)
	}
	rows.Close()
	rows, err = s.db.Query(`SELECT ms, body FROM relay WHERE session = ? AND ms >= ? AND ms <= ? ORDER BY ms`,
		session, fromMs, toMs)
	if err != nil {
		return t, err
	}
	for rows.Next() {
		var r TimelineRow
		var body string
		if err := rows.Scan(&r.Ms, &body); err != nil {
			rows.Close()
			return t, err
		}
		r.Body = json.RawMessage(body)
		t.Relay = append(t.Relay, r)
	}
	rows.Close()
	t.Events, err = s.Events(session, fromMs, toMs)
	return t, err
}

// Events returns the events of a session between fromMs and toMs, oldest
// first.
func (s *Store) Events(session string, fromMs, toMs int64) ([]Event, error) {
	rows, err := s.db.Query(`SELECT ms, user, kind, detail FROM events
		WHERE session = ? AND ms >= ? AND ms <= ? ORDER BY ms, rowid`, session, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Event{}
	for rows.Next() {
		var e Event
		var detail string
		if err := rows.Scan(&e.Ms, &e.User, &e.Kind, &detail); err != nil {
			return nil, err
		}
		e.Detail = json.RawMessage(detail)
		out = append(out, e)
	}
	return out, rows.Err()
}

// MinutesRow is one day row of the peer minutes.
type MinutesRow struct {
	Day       string  `json:"day"`
	Type      string  `json:"type"`
	Role      string  `json:"role"`
	Synthetic bool    `json:"synthetic"`
	Minutes   float64 `json:"minutes"`
}

// Minutes returns the day rows from fromDay to toDay (YYYY-MM-DD, inclusive).
func (s *Store) Minutes(fromDay, toDay string) ([]MinutesRow, error) {
	rows, err := s.db.Query(`SELECT day, type, role, synthetic, minutes FROM minutes
		WHERE day >= ? AND day <= ? ORDER BY day, type, role, synthetic`, fromDay, toDay)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MinutesRow{}
	for rows.Next() {
		var r MinutesRow
		var synthetic int
		if err := rows.Scan(&r.Day, &r.Type, &r.Role, &synthetic, &r.Minutes); err != nil {
			return nil, err
		}
		r.Synthetic = synthetic != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// Counts returns the rows per table, for the growth measurement.
func (s *Store) Counts() (map[string]int64, error) {
	out := map[string]int64{}
	for _, t := range []string{"logs", "stats", "relay", "events", "sessions", "session_peers", "minutes",
		"peer_minutes", "alerts", "instance_samples"} {
		var n int64
		if err := s.db.QueryRow(`SELECT COUNT(*) FROM ` + t).Scan(&n); err != nil {
			return nil, err
		}
		out[t] = n
	}
	return out, nil
}

// PeerMinutes returns the graded minutes of a run, oldest first.
func (s *Store) PeerMinutes(id string, startMs int64) ([]PeerMinuteOut, error) {
	rows, err := s.db.Query(`SELECT minute_ms, user, role, type, graded, degraded, reasons, leg, detail
		FROM peer_minutes WHERE id = ? AND start_ms = ? ORDER BY minute_ms, user`, id, startMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PeerMinuteOut{}
	for rows.Next() {
		var r PeerMinuteOut
		var graded, degraded int
		var detail string
		if err := rows.Scan(&r.MinuteMs, &r.User, &r.Role, &r.Type, &graded, &degraded, &r.Reasons, &r.Leg, &detail); err != nil {
			return nil, err
		}
		r.Graded, r.Degraded, r.Detail = graded != 0, degraded != 0, json.RawMessage(detail)
		out = append(out, r)
	}
	return out, rows.Err()
}

// PeerMinuteOut is a peer minute as the API returns it.
type PeerMinuteOut struct {
	MinuteMs int64           `json:"minute_ms"`
	User     string          `json:"user"`
	Role     string          `json:"role"`
	Type     string          `json:"type"`
	Graded   bool            `json:"graded"`
	Degraded bool            `json:"degraded"`
	Reasons  string          `json:"reasons"`
	Leg      string          `json:"leg"`
	Detail   json.RawMessage `json:"detail"`
}

// UngradedRuns returns the ended runs that have no grade yet.
func (s *Store) UngradedRuns() ([]SessionRow, error) {
	rows, err := s.db.Query(`SELECT ` + sessionCols + ` FROM sessions WHERE end_ms > 0 AND quality = '' ORDER BY start_ms`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SessionRow{}
	for rows.Next() {
		r, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// AlertOut is an alert as the API returns it.
type AlertOut struct {
	Ms       int64           `json:"ms"`
	Kind     string          `json:"kind"`
	Category string          `json:"category"`
	Severity string          `json:"severity"`
	Session  string          `json:"session,omitempty"`
	User     string          `json:"user,omitempty"`
	Detail   json.RawMessage `json:"detail"`
}

// Alerts returns the alerts in [fromMs, toMs), newest first.
func (s *Store) Alerts(fromMs, toMs int64, limit int) ([]AlertOut, error) {
	if toMs <= 0 {
		toMs = 1 << 62
	}
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.Query(`SELECT ms, kind, category, severity, session, user, detail FROM alerts
		WHERE ms >= ? AND ms < ? ORDER BY ms DESC, rowid DESC LIMIT ?`, fromMs, toMs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AlertOut{}
	for rows.Next() {
		var a AlertOut
		var detail string
		if err := rows.Scan(&a.Ms, &a.Kind, &a.Category, &a.Severity, &a.Session, &a.User, &detail); err != nil {
			return nil, err
		}
		a.Detail = json.RawMessage(detail)
		out = append(out, a)
	}
	return out, rows.Err()
}

// Sample is one instance sample.
type Sample struct {
	Ms   int64
	Body string
}

// InstanceSamples returns the instance's 10 s samples in [fromMs, toMs).
func (s *Store) InstanceSamples(fromMs, toMs int64) ([]Sample, error) {
	rows, err := s.db.Query(`SELECT ms, body FROM instance_samples WHERE ms >= ? AND ms < ? ORDER BY ms`, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Sample{}
	for rows.Next() {
		var r Sample
		if err := rows.Scan(&r.Ms, &r.Body); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MinuteBucket is the peer minutes of one time bucket.
type MinuteBucket struct {
	Ms       int64 `json:"t"`
	Graded   int   `json:"graded"`
	Degraded int   `json:"degraded"`
	InBad    int   `json:"in_bad"`
}

// MinuteBuckets sums the graded and degraded peer minutes per bucket, and
// the minutes of runs graded bad.
func (s *Store) MinuteBuckets(fromMs, toMs, bucketMs int64) ([]MinuteBucket, error) {
	rows, err := s.db.Query(`SELECT (pm.minute_ms / ?) * ? AS b, SUM(pm.graded), SUM(pm.degraded),
		SUM(CASE WHEN s.quality = 'bad' AND pm.graded = 1 THEN 1 ELSE 0 END)
		FROM peer_minutes pm LEFT JOIN sessions s ON s.id = pm.id AND s.start_ms = pm.start_ms
		WHERE pm.minute_ms >= ? AND pm.minute_ms < ? GROUP BY b ORDER BY b`,
		bucketMs, bucketMs, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MinuteBucket{}
	for rows.Next() {
		var b MinuteBucket
		if err := rows.Scan(&b.Ms, &b.Graded, &b.Degraded, &b.InBad); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// LegCounts counts the degraded peer minutes per leg in [fromMs, toMs).
func (s *Store) LegCounts(fromMs, toMs int64) (map[string]int, error) {
	rows, err := s.db.Query(`SELECT leg, COUNT(*) FROM peer_minutes WHERE degraded = 1 AND minute_ms >= ? AND minute_ms < ?
		GROUP BY leg`, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var leg string
		var n int
		if err := rows.Scan(&leg, &n); err != nil {
			return nil, err
		}
		out[leg] = n
	}
	return out, rows.Err()
}

// StatsField counts the stream stats rows of a role per value of a field
// in [fromMs, toMs): each row is one 10 s report, so a count is time.
func (s *Store) StatsField(role, field string, fromMs, toMs int64) (map[string]int, error) {
	rows, err := s.db.Query(`SELECT COALESCE(json_extract(body, '$.' || ?), ''), COUNT(*) FROM stats
		WHERE kind = 'stream' AND role = ? AND ms >= ? AND ms < ? GROUP BY 1`, field, role, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var v string
		var n int
		if err := rows.Scan(&v, &n); err != nil {
			return nil, err
		}
		if v != "" {
			out[v] = n
		}
	}
	return out, rows.Err()
}

// EventsOfKinds returns the events of these kinds in [fromMs, toMs), oldest
// first, with their session.
func (s *Store) EventsOfKinds(kinds []string, fromMs, toMs int64) ([]EventRow, error) {
	if len(kinds) == 0 {
		return nil, nil
	}
	args := []any{fromMs, toMs}
	marks := make([]string, len(kinds))
	for i, k := range kinds {
		marks[i] = "?"
		args = append(args, k)
	}
	rows, err := s.db.Query(`SELECT ms, session, user, kind, detail FROM events WHERE ms >= ? AND ms < ?
		AND kind IN (`+strings.Join(marks, ",")+`) ORDER BY ms`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []EventRow{}
	for rows.Next() {
		var e EventRow
		var detail string
		if err := rows.Scan(&e.Ms, &e.Session, &e.User, &e.Kind, &detail); err != nil {
			return nil, err
		}
		e.Detail = []byte(detail)
		out = append(out, e)
	}
	return out, rows.Err()
}

// UserRunGrade is one user's own grade in one run: their peer minutes only.
type UserRunGrade struct {
	ID       string         `json:"id"`
	StartMs  int64          `json:"start_ms"`
	Roles    []string       `json:"roles"`
	Graded   int            `json:"graded"`
	Degraded int            `json:"degraded"`
	Legs     map[string]int `json:"legs"`
	Reasons  map[string]int `json:"reasons"`
}

// UserRunGrades sums a user's peer minutes per run in [fromMs, toMs).
func (s *Store) UserRunGrades(user string, fromMs, toMs int64) (map[string]*UserRunGrade, error) {
	if toMs <= 0 {
		toMs = 1 << 62
	}
	rows, err := s.db.Query(`SELECT id, start_ms, role, graded, degraded, leg, reasons FROM peer_minutes
		WHERE user = ? AND minute_ms >= ? AND minute_ms < ?`, user, fromMs, toMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]*UserRunGrade{}
	for rows.Next() {
		var id, role, leg, reasons string
		var start int64
		var graded, degraded int
		if err := rows.Scan(&id, &start, &role, &graded, &degraded, &leg, &reasons); err != nil {
			return nil, err
		}
		key := id + "\x00" + strconv.FormatInt(start, 10)
		g := out[key]
		if g == nil {
			g = &UserRunGrade{ID: id, StartMs: start, Legs: map[string]int{}, Reasons: map[string]int{}}
			out[key] = g
		}
		found := false
		for _, r := range g.Roles {
			found = found || r == role
		}
		if !found {
			g.Roles = append(g.Roles, role)
		}
		g.Graded += graded
		g.Degraded += degraded
		if degraded != 0 {
			g.Legs[leg]++
			for _, r := range strings.Split(reasons, ",") {
				if r != "" {
					g.Reasons[r]++
				}
			}
		}
	}
	return out, rows.Err()
}

// UserStatRow is one of a user's stats rows.
type UserStatRow struct {
	Ms      int64           `json:"ms"`
	Session string          `json:"session"`
	Role    string          `json:"role"`
	Kind    string          `json:"kind"`
	Body    json.RawMessage `json:"body"`
}

// UserStats returns a user's stats rows in [fromMs, toMs), newest first.
func (s *Store) UserStats(user string, fromMs, toMs int64, limit int) ([]UserStatRow, error) {
	if toMs <= 0 {
		toMs = 1 << 62
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.Query(`SELECT ms, session, role, kind, body FROM stats WHERE user = ? AND ms >= ? AND ms < ?
		ORDER BY ms DESC LIMIT ?`, user, fromMs, toMs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []UserStatRow{}
	for rows.Next() {
		var r UserStatRow
		var body string
		if err := rows.Scan(&r.Ms, &r.Session, &r.Role, &r.Kind, &body); err != nil {
			return nil, err
		}
		r.Body = json.RawMessage(body)
		out = append(out, r)
	}
	return out, rows.Err()
}
