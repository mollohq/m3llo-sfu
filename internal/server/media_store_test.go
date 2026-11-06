package server

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
	"github.com/mollohq/m3llo-sfu/internal/store"
)

func newStoreTestServer(t *testing.T, path string, ringSize int) *Server {
	t.Helper()
	logger := logging.NewWithRing("info", ringSize)
	st, err := store.Open(path, store.Options{AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	s := &Server{
		config:   Config{MaxPeersPerSession: 10},
		Logger:   logger,
		sessions: NewSessionStore(logger),
		metrics:  metrics.New(),
	}
	s.media = newMediaRecorder(st)
	logger.SetSink(s.media.onLog)
	t.Cleanup(func() {
		logger.SetSink(nil)
		st.Close()
	})
	return s
}

func eventKindsOf(events []store.Event) map[string][]store.Event {
	out := map[string][]store.Event{}
	for _, e := range events {
		out[e.Kind] = append(out[e.Kind], e)
	}
	return out
}

// A scripted stream session: the store gets the summary, the users, the
// stats rows, the relay rows and the event rows of the timeline.
func TestRecorderWritesTheEventRowsOfAScriptedSession(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	m := s.media
	start := time.Now().Add(-time.Minute)

	session := NewStreamSession("st-1", "crew-1", s.Logger, streamSnapshotConfig{})
	s.sessions.sessions[session.ID()] = session
	host := &Peer{UserID: "u-host", Username: "host", Role: "host", ConnectedAt: start}
	viewer := &Peer{UserID: "u-viewer", Username: "viewer", Role: "viewer", ConnectedAt: start}
	if err := session.AddPeer(host); err != nil {
		t.Fatal(err)
	}
	if err := session.AddPeer(viewer); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	m.tick(s.sessions.All(), t0)

	m.onStats(session.ID(), host, "stream", map[string]interface{}{
		"role": "host", "w": 1920.0, "h": 1080.0, "fps_cfg": 60.0, "enc_ms_avg": 4.0,
		"eq_drops": 0.0, "br_target": 8000.0, "src": "Secret game window",
	})
	s.Logger.Info("stream_client_stats", logging.D{
		"session": session.ID(), "user": host.UserID, "role": "host",
		"stats": map[string]interface{}{"br_target": 8000, "src": "Secret game window"},
	})
	s.Logger.Info("stream_rung_change", logging.D{
		"session": session.ID(), "user": host.UserID, "w": 1280, "h": 720, "fps": 60,
	})
	// The host repeats its rung for a viewer that joins: not a change.
	s.Logger.Info("stream_rung_change", logging.D{
		"session": session.ID(), "user": host.UserID, "w": 1280, "h": 720, "fps": 60,
	})
	m.onStats(session.ID(), viewer, "stream", map[string]interface{}{"role": "viewer", "rel": 600.0})
	m.onStats(session.ID(), viewer, "stream", map[string]interface{}{"role": "viewer", "rel": 600.0})

	// 10 % ingress loss and two viewer PLIs in the next window.
	session.rtpIngressExpected.Store(1000)
	session.rtpIngressLost.Store(100)
	session.kfReqViewer.Add(2)
	host.BytesReceived.Add(5000)
	viewer.BytesSent.Add(4000)
	m.tick(s.sessions.All(), t0.Add(10*time.Second))
	// A clean window ends the episode.
	session.rtpIngressExpected.Store(2000)
	m.tick(s.sessions.All(), t0.Add(20*time.Second))

	// The session leaves the store: the run ends.
	delete(s.sessions.sessions, session.ID())
	m.tick(s.sessions.All(), t0.Add(30*time.Second))
	m.store.Flush()

	run, ok, err := m.store.SessionRun("st-1", 0)
	if err != nil || !ok {
		t.Fatalf("run: %v %v", ok, err)
	}
	if run.EndMs != t0.Add(20*time.Second).UnixMilli() {
		t.Errorf("end_ms = %d, want the last tick with peers", run.EndMs)
	}
	if run.HostUserID != "u-host" || run.CrewID != "crew-1" || run.Peers != 2 || run.Type != "stream" {
		t.Errorf("summary = %+v", run)
	}
	if run.BytesSent != 4000 || run.BytesRecv != 5000 {
		t.Errorf("bytes = %d down, %d up, want 4000 and 5000", run.BytesSent, run.BytesRecv)
	}
	// Two peers over the two 10 s windows after the first tick.
	if run.PeerMinutes < 0.6 || run.PeerMinutes > 0.8 {
		t.Errorf("peer minutes = %v, want about 0.67", run.PeerMinutes)
	}

	events, err := m.store.Events("st-1", 0, time.Now().Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	kinds := eventKindsOf(events)
	for _, k := range []string{"join", "rung_change", "first_frame", "ingress_loss", "ingress_clean", "keyframe_request"} {
		if len(kinds[k]) == 0 {
			t.Errorf("no %s event; events: %+v", k, events)
		}
	}
	if n := len(kinds["rung_change"]); n != 1 {
		t.Errorf("rung_change events = %d, want 1 (the repeat is no change)", n)
	}
	if n := len(kinds["first_frame"]); n != 1 {
		t.Errorf("first_frame events = %d, want 1", n)
	}
	var rung map[string]interface{}
	if len(kinds["rung_change"]) > 0 {
		_ = json.Unmarshal(kinds["rung_change"][0].Detail, &rung)
	}
	if rung["direction"] != "down" || rung["reason"] != "bitrate" || rung["from"] != "1920x1080@60" {
		t.Errorf("rung change detail = %v", rung)
	}
	var loss map[string]float64
	if len(kinds["ingress_loss"]) > 0 {
		_ = json.Unmarshal(kinds["ingress_loss"][0].Detail, &loss)
	}
	if loss["pct"] != 10 {
		t.Errorf("ingress loss detail = %v, want pct 10", loss)
	}
	var kf map[string]float64
	if len(kinds["keyframe_request"]) > 0 {
		_ = json.Unmarshal(kinds["keyframe_request"][0].Detail, &kf)
	}
	if kf["viewer"] != 2 {
		t.Errorf("keyframe request detail = %v, want viewer 2", kf)
	}

	tl, err := m.store.Timeline("st-1", 0, time.Now().Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if len(tl.Stats) != 3 || len(tl.Relay) != 3 {
		t.Errorf("timeline has %d stats rows and %d relay rows, want 3 and 3", len(tl.Stats), len(tl.Relay))
	}
	for _, r := range tl.Stats {
		if strings.Contains(string(r.Body), "Secret") {
			t.Errorf("a stats row kept the capture source: %s", r.Body)
		}
	}
	lines, err := m.store.Logs(store.LogQuery{Level: 1, Event: "stream_client_stats"})
	if err != nil || len(lines) != 1 {
		t.Fatalf("stats log lines = %d (%v)", len(lines), err)
	}
	if strings.Contains(string(lines[0]), "Secret") {
		t.Errorf("the disk log kept the capture source: %s", lines[0])
	}
	minutes, err := m.store.Minutes("2000-01-01", "2100-01-01")
	if err != nil || len(minutes) == 0 {
		t.Fatalf("minutes = %v (%v)", minutes, err)
	}
}

// The Logs page reads a line the ring no longer holds from the store.
func TestAdminLogsReadPastTheRing(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 5)
	s.Logger.Info("early_line", logging.D{"session": "st-old"})
	for i := 0; i < 20; i++ {
		s.Logger.Info("filler", logging.D{"i": i})
	}
	s.media.store.Flush()

	if ring := s.Logger.Ring().Query(logging.LogFilter{Event: "early_line"}); len(ring.Entries) != 0 {
		t.Fatalf("the ring still holds the early line: %+v", ring.Entries)
	}

	rec := httptest.NewRecorder()
	s.handleAdminLogs(rec, httptest.NewRequest("GET", "/admin/api/logs?from=1&event=early_line", nil))
	var body struct {
		Source  string            `json:"source"`
		Entries []json.RawMessage `json:"entries"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v (%d)", err, rec.Code)
	}
	if body.Source != "store" || len(body.Entries) != 1 {
		t.Fatalf("store answer = %s, %d entries", body.Source, len(body.Entries))
	}
	var line struct {
		Ms      int64  `json:"ms"`
		Event   string `json:"event"`
		Session string `json:"session"`
	}
	_ = json.Unmarshal(body.Entries[0], &line)
	if line.Event != "early_line" || line.Session != "st-old" || line.Ms <= 0 {
		t.Fatalf("line = %+v", line)
	}

	// Without from/to the endpoint is the ring, as before.
	rec = httptest.NewRecorder()
	s.handleAdminLogs(rec, httptest.NewRequest("GET", "/admin/api/logs?event=early_line", nil))
	var ring logging.LogQueryResult
	_ = json.NewDecoder(rec.Body).Decode(&ring)
	if len(ring.Entries) != 0 {
		t.Fatalf("the ring answer has the early line")
	}
}

// Store pages end on a millisecond: the lines of the last millisecond go
// to the next page.
func TestAdminLogsPagesNeverSplitAMillisecond(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	st := s.media.store
	for i, ms := range []int64{1000, 2000, 2000, 3000} {
		line, _ := json.Marshal(map[string]interface{}{"event": "e", "level": "info", "data": map[string]int{"i": i}})
		st.Log(store.LogRow{Ms: ms, Level: 1, Event: "e", Line: line})
	}
	st.Flush()

	get := func(q string) (int, int64) {
		rec := httptest.NewRecorder()
		s.handleAdminLogs(rec, httptest.NewRequest("GET", "/admin/api/logs?"+q, nil))
		var body struct {
			Entries []json.RawMessage `json:"entries"`
			NextTo  int64             `json:"next_to"`
		}
		_ = json.NewDecoder(rec.Body).Decode(&body)
		return len(body.Entries), body.NextTo
	}
	n, next := get("from=1&limit=2")
	if n != 1 || next != 2001 {
		t.Fatalf("first page: %d lines, next_to %d; want 1 and 2001", n, next)
	}
	// A page that is all one millisecond keeps it whole; the next page
	// goes on below it.
	n, next = get("from=1&limit=2&to=2001")
	if n != 2 || next != 2000 {
		t.Fatalf("second page: %d lines, next_to %d; want 2 and 2000", n, next)
	}
	n, next = get("from=1&limit=2&to=2000")
	if n != 1 || next != 0 {
		t.Fatalf("last page: %d lines, next_to %d; want 1 and 0", n, next)
	}
}

// After a restart the ended session and its timeline are still served.
func TestStoreSessionsSurviveARestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.db")
	{
		s := newStoreTestServer(t, path, 0)
		session := NewStreamSession("st-r", "crew-1", s.Logger, streamSnapshotConfig{})
		s.sessions.sessions[session.ID()] = session
		host := &Peer{UserID: "u-host", Username: "host", Role: "host", ConnectedAt: time.Now().Add(-time.Hour)}
		if err := session.AddPeer(host); err != nil {
			t.Fatal(err)
		}
		s.media.tick(s.sessions.All(), time.Now().Add(time.Millisecond))
		// The process stops without ending the run.
		s.media.store.Flush()
		s.Logger.SetSink(nil)
		s.media.store.Close()
	}
	s := newStoreTestServer(t, path, 0)
	rec := httptest.NewRecorder()
	from := time.Now().Add(-2 * time.Hour).UnixMilli()
	s.handleAdminSessions(rec, httptest.NewRequest("GET", "/admin/api/sessions?from="+itoa(from), nil))
	var body struct {
		Source   string             `json:"source"`
		Sessions []store.SessionRow `json:"sessions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if len(body.Sessions) != 1 || body.Sessions[0].EndMs == 0 || body.Sessions[0].HostUserID != "u-host" {
		t.Fatalf("sessions after restart = %+v", body.Sessions)
	}

	rec = httptest.NewRecorder()
	s.handleAdminSessionDetail(rec, httptest.NewRequest("GET", "/admin/api/session/st-r/timeline", nil))
	var tl struct {
		Session store.SessionRow `json:"session"`
		Events  []store.Event    `json:"events"`
		Relay   []json.RawMessage
	}
	if err := json.NewDecoder(rec.Body).Decode(&tl); err != nil {
		t.Fatalf("timeline: %v (%d)", err, rec.Code)
	}
	if tl.Session.ID != "st-r" || len(tl.Events) == 0 {
		t.Fatalf("timeline after restart: %+v", tl)
	}
}

// The leaves and the close come after the run's last tick with a peer;
// the session's events still show them.
func TestStoreEventsKeepTheDrainAfterTheRun(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	st := s.media.store
	st.Session(store.SessionRow{ID: "st-d", StartMs: 1000, EndMs: 60_000, LastSeenMs: 60_000, Type: "stream"})
	st.Event(store.EventRow{Ms: 2000, Session: "st-d", Kind: "join"})
	st.Event(store.EventRow{Ms: 64_000, Session: "st-d", Kind: "leave"})
	st.Event(store.EventRow{Ms: 95_000, Session: "st-d", Kind: "session_closed"})
	st.Event(store.EventRow{Ms: 200_000, Session: "st-d", Kind: "join"}) // a later run
	st.Flush()
	rec := httptest.NewRecorder()
	s.handleAdminSessionDetail(rec, httptest.NewRequest("GET", "/admin/api/session/st-d/events", nil))
	var body struct {
		Events []store.Event `json:"events"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&body)
	if len(body.Events) != 3 || body.Events[2].Kind != "session_closed" {
		t.Fatalf("events = %+v, want join, leave and session_closed", body.Events)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
