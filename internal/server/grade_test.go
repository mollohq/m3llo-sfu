package server

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/store"
)

func hostAt(capFps, encFps, rung, encMs float64) PeerMinuteInput {
	return PeerMinuteInput{User: "h", Role: "host", ViewerLossPct: -1, HasHostStats: true,
		CapFps: capFps, EncFps: encFps, FpsCfg: 60, FpsRung: rung, EncMs: encMs}
}

func viewer(lossPct float64) PeerMinuteInput {
	return PeerMinuteInput{User: "v", Role: "viewer", ViewerLossPct: lossPct}
}

func gradeOf(t *testing.T, in MinuteInput, user string) PeerGrade {
	t.Helper()
	for _, g := range GradeMinute(in) {
		if g.User == user {
			return g
		}
	}
	t.Fatalf("no grade for %s", user)
	return PeerGrade{}
}

func streamMinute(ing float64, peers ...PeerMinuteInput) MinuteInput {
	return MinuteInput{Type: "stream", Seconds: 60, IngWorstPct: ing, IngExp: 1000, Peers: peers}
}

func TestGradeHostUplinkLoss(t *testing.T) {
	g := gradeOf(t, streamMinute(3.2, hostAt(60, 30, 30, 10), viewer(0)), "h")
	if !g.Degraded || g.Leg != LegHostUplink || g.Reasons[0] != "loss" {
		t.Fatalf("host minute with 3.2 %% ingress loss at its rung = %+v", g)
	}
	if v := gradeOf(t, streamMinute(3.2, hostAt(60, 30, 30, 10), viewer(0)), "v"); v.Degraded {
		t.Fatalf("a clean viewer leg is degraded: %+v", v)
	}
}

func TestGradeHostMachineShort(t *testing.T) {
	for name, h := range map[string]PeerMinuteInput{
		"capture under the rung": hostAt(30, 29.5, 60, 8),
		"encode under the rung":  hostAt(60, 40, 60, 8),
		"encode over the budget": hostAt(60, 60, 60, 20),
	} {
		g := gradeOf(t, streamMinute(0, h, viewer(0)), "h")
		if !g.Degraded || g.Leg != LegHostMachine || g.Reasons[0] != "frame_rate" {
			t.Errorf("%s: %+v", name, g)
		}
	}
	idle := hostAt(1, 1, 60, 8)
	idle.CapIdle = true
	if g := gradeOf(t, streamMinute(0, idle), "h"); g.Degraded {
		t.Errorf("an idle capture (a still screen) is degraded: %+v", g)
	}
	// The controller stepped the rung down to 30 fps: 30 is at the rung.
	if g := gradeOf(t, streamMinute(0, hostAt(60, 30, 30, 10)), "h"); g.Degraded {
		t.Errorf("a host at its stepped-down rung is degraded: %+v", g)
	}
}

func TestGradeViewerLeg(t *testing.T) {
	g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), viewer(3.4)), "v")
	if !g.Degraded || g.Leg != LegViewer {
		t.Fatalf("viewer with 3.4 %% loss on its leg = %+v", g)
	}
	if g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), viewer(2.0)), "v"); g.Degraded {
		t.Fatalf("2.0 %% is not over 2 %%: %+v", g)
	}
}

func TestGradeViewerFreezeAndAVOffset(t *testing.T) {
	v := viewer(0)
	v.HasViewerStats, v.FreezeN, v.FreezeMs = true, 1, 400
	g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), v), "v")
	if !g.Degraded || g.Reasons[0] != "freeze" || g.Leg != LegUnknown {
		t.Fatalf("a freeze with no other evidence = %+v", g)
	}
	v.Reconnects = 1
	if g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), v), "v"); g.Leg != LegViewer {
		t.Fatalf("a freeze with a reconnect of the viewer = %+v", g)
	}
	a := viewer(0)
	a.HasViewerStats, a.HasAV, a.AVMaxMs = true, true, 95
	if g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), a), "v"); !g.Degraded || g.Reasons[0] != "av_offset" {
		t.Fatalf("A/V offset 95 ms = %+v", g)
	}
	a.AVMaxMs = 80
	if g := gradeOf(t, streamMinute(0, hostAt(60, 60, 60, 8), a), "v"); g.Degraded {
		t.Fatalf("80 ms is not over 80 ms: %+v", g)
	}
}

func TestGradeSFUComesFirst(t *testing.T) {
	in := streamMinute(5, hostAt(30, 20, 60, 30), viewer(4))
	in.QueueDrop = 3
	for _, g := range GradeMinute(in) {
		if g.Degraded && g.Leg != LegSFU {
			t.Fatalf("queue drops in the minute: %+v", g)
		}
	}
	in.QueueDrop, in.Rejects = 0, 1
	if g := gradeOf(t, in, "v"); g.Leg != LegSFU {
		t.Fatalf("a capacity reject in the minute: %+v", g)
	}
}

func TestGradeOrderUplinkThenMachine(t *testing.T) {
	// Ingress loss with the host short of its rung: not the uplink (the
	// host was not at its preset), not the machine (ingress not clean).
	g := gradeOf(t, streamMinute(5, hostAt(30, 30, 60, 8), viewer(0)), "h")
	if !g.Degraded || g.Leg != LegUnknown {
		t.Fatalf("loss with a short host = %+v", g)
	}
}

func TestGradeVoice(t *testing.T) {
	m := PeerMinuteInput{User: "a", Role: "member", ViewerLossPct: -1, HasVoiceStats: true, LateUnderruns: 400}
	other := PeerMinuteInput{User: "b", Role: "member", ViewerLossPct: -1, HasVoiceStats: true}
	in := MinuteInput{Type: "voice", Seconds: 60, IngWorstPct: -1, Peers: []PeerMinuteInput{m, other}}
	g := gradeOf(t, in, "a")
	// 400 frames of 10 ms in 60 s: 6.67 %.
	if !g.Degraded || g.Reasons[0] != "concealment" || g.Leg != LegReceiver {
		t.Fatalf("voice concealment 6.7 %% = %+v", g)
	}
	in.Peers[1].SendSkips = 12
	if g := gradeOf(t, in, "a"); g.Leg != LegSenderMachine {
		t.Fatalf("a sender that skipped sends = %+v", g)
	}
	in.Peers[0].LateUnderruns = 250 // 4.2 %
	if g := gradeOf(t, in, "a"); g.Degraded {
		t.Fatalf("4.2 %% concealment is degraded: %+v", g)
	}
	if g := gradeOf(t, in, "b"); !g.Graded || g.Degraded {
		t.Fatalf("a clean member = %+v", g)
	}
}

func TestGradeSessionShares(t *testing.T) {
	for _, c := range []struct {
		graded, degraded int
		want             string
	}{
		{0, 0, QualityNoData},
		{100, 0, QualityGood},
		{100, 9, QualityDegraded},
		{100, 10, QualityBad},
		{10, 5, QualityBad},
	} {
		if got := GradeSession(c.graded, c.degraded, map[string]int{"x": c.degraded}).Quality; got != c.want {
			t.Errorf("%d of %d degraded = %s, want %s", c.degraded, c.graded, got, c.want)
		}
	}
	g := GradeSession(10, 3, map[string]int{LegViewer: 1, LegHostUplink: 2})
	if g.Trouble != LegHostUplink {
		t.Errorf("trouble = %s, want the leg of most minutes", g.Trouble)
	}
}

func relayWin(ms int64, body string) gradeWindow {
	var m map[string]interface{}
	_ = json.Unmarshal([]byte(body), &m)
	return gradeWindow{Ms: ms, Relay: m}
}

// The minute's loss is its worst window, so a 10 s burst is not averaged
// away; the host's warm-up report (no frame encoded yet) is left out.
func TestBuildMinuteWorstWindowAndWarmUp(t *testing.T) {
	present := `"present":[["h","host"],["v","viewer"]]`
	wins := []gradeWindow{
		relayWin(10_000, `{"ing_expected":3088,"ing_lost":0,`+present+`}`),
		relayWin(20_000, `{"ing_expected":2562,"ing_lost":81,`+present+`,"viewer_lost":{"v":10},"viewer_acked":{"v":290}}`),
		relayWin(30_000, `{"ing_expected":1753,"ing_lost":0,`+present+`}`),
	}
	wins[0].Stats = []gradeStat{{User: "h", Role: "host", Kind: "stream",
		Body: map[string]interface{}{"cap_fps": 1.4, "enc_fps": 0.0, "fps_cfg": 60.0, "fps_out": 60.0}}}
	wins[1].Stats = []gradeStat{{User: "h", Role: "host", Kind: "stream",
		Body: map[string]interface{}{"cap_fps": 60.0, "enc_fps": 30.0, "fps_cfg": 60.0, "fps_out": 30.0, "enc_ms_avg": 8.4}}}
	in := buildMinute("stream", 0, 60, wins, nil, gradeCarry{})
	if in.IngWorstPct < 3.1 || in.IngWorstPct > 3.2 {
		t.Fatalf("worst window = %.2f %%, want 3.16", in.IngWorstPct)
	}
	var h, v PeerMinuteInput
	for _, p := range in.Peers {
		if p.Role == "host" {
			h = p
		} else {
			v = p
		}
	}
	if !h.HasHostStats || h.EncFps != 30 || h.CapFps != 60 || h.FpsRung != 30 {
		t.Fatalf("host = %+v (the warm-up report must not count)", h)
	}
	if v.ViewerLossPct < 3.3 || v.ViewerLossPct > 3.4 {
		t.Fatalf("viewer loss = %.2f, want 10 of 300", v.ViewerLossPct)
	}
	if g := gradeOf(t, in, "h"); g.Leg != LegHostUplink {
		t.Fatalf("the congested shape = %+v, want host uplink", g)
	}
}

func TestBuildMinuteVoiceTotalsCarry(t *testing.T) {
	carry := gradeCarry{}
	win := func(ms int64, late float64) gradeWindow {
		w := relayWin(ms, `{"present":[["a","member"]]}`)
		w.Stats = []gradeStat{{User: "a", Role: "member", Kind: "voice", Body: map[string]interface{}{"late_underruns": late}}}
		return w
	}
	first := buildMinute("voice", 0, 60, []gradeWindow{win(10_000, 100), win(20_000, 150)}, nil, carry)
	if first.Peers[0].LateUnderruns != 50 {
		t.Fatalf("first minute = %v, want 50 (the first total is the base)", first.Peers[0].LateUnderruns)
	}
	second := buildMinute("voice", 60_000, 60, []gradeWindow{win(70_000, 600)}, nil, carry)
	if second.Peers[0].LateUnderruns != 450 {
		t.Fatalf("second minute = %v, want 450", second.Peers[0].LateUnderruns)
	}
}

// The backfill grades the runs stored before the grades from their rows
// (the viewer loss estimate in place of the counts); a run with no relay
// row is "not graded".
func TestBackfillGradesStoredRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "media.db")
	st, err := store.Open(path, store.Options{AllowUnsafeToolchain: true})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	start := time.Now().Add(-time.Hour).UnixMilli()
	st.Session(store.SessionRow{ID: "wifi", StartMs: start, EndMs: start + 30_000, LastSeenMs: start + 30_000, Type: "stream"})
	st.Peer(store.PeerRow{ID: "wifi", StartMs: start, User: "h", Role: "host", FirstMs: start, LastMs: start + 30_000})
	st.Peer(store.PeerRow{ID: "wifi", StartMs: start, User: "v", Role: "viewer", FirstMs: start, LastMs: start + 30_000})
	for i, pct := range []float64{1.4, 3.4, 1.2} {
		body, _ := json.Marshal(map[string]interface{}{"ing_expected": 3000, "ing_lost": 0,
			"viewer_loss_pct": map[string]float64{"v": pct}})
		st.Relay(store.RelayRow{Ms: start + int64(i+1)*10_000, Session: "wifi", Body: body})
	}
	st.Session(store.SessionRow{ID: "empty", StartMs: start, EndMs: start + 5000, LastSeenMs: start + 5000, Type: "voice"})
	st.Flush()

	backfillGrades(st, func(string, map[string]interface{}) {})

	run, _, _ := st.SessionRun("wifi", start)
	if run.Quality != QualityBad || run.Trouble != LegViewer || run.GradedMinutes != 2 || run.DegradedMinutes != 1 {
		t.Fatalf("backfilled run = %+v", run)
	}
	minutes, _ := st.PeerMinutes("wifi", start)
	if len(minutes) != 2 {
		t.Fatalf("peer minutes = %+v", minutes)
	}
	empty, _, _ := st.SessionRun("empty", start)
	if empty.Quality != QualityNotGraded {
		t.Fatalf("a run with no relay row = %q, want %q", empty.Quality, QualityNotGraded)
	}
	// A second pass grades nothing again.
	backfillGrades(st, func(string, map[string]interface{}) { t.Fatal("graded twice") })
}

// The recorder grades each minute of a live run at the minute's end and
// the last, short minute at the run's end; it alerts on a minute with
// ingress loss over 10 % and on a peer with no media for a window.
func TestRecorderGradesMinutesAndAlerts(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	m := s.media
	orig := peerConnectedFn
	peerConnectedFn = func(*Peer) bool { return true }
	defer func() { peerConnectedFn = orig }()

	session := NewStreamSession("st-g", "crew-1", s.Logger, streamSnapshotConfig{})
	// The run lies in the past, so the overview of the last hour has it.
	session.createdAt = time.Now().Add(-5 * time.Minute)
	s.sessions.sessions[session.ID()] = session
	past := time.Now().Add(-time.Hour)
	host := &Peer{UserID: "h", Username: "host", Role: "host", ConnectedAt: past}
	view := &Peer{UserID: "v", Username: "viewer", Role: "viewer", ConnectedAt: past}
	_ = session.AddPeer(host)
	_ = session.AddPeer(view)
	start := session.CreatedAt()

	expected, lost := int64(0), int64(0)
	var tick = func(at time.Duration, exp, lo int64, viewerLost, viewerAcked int) {
		expected += exp
		lost += lo
		session.rtpIngressExpected.Store(expected)
		session.rtpIngressLost.Store(lost)
		host.PacketsReceived.Add(exp)
		host.BytesReceived.Add(exp * 1000)
		view.BytesSent.Add(exp * 1000)
		if viewerAcked > 0 {
			m.onLog(start.Add(at).UnixMilli(), logEntry("stream_host_remb_tick", session.ID(), map[string]interface{}{
				"viewer_bwe": []map[string]interface{}{{"viewer": "v", "lost": viewerLost, "acked": viewerAcked, "avg_loss": 0.0}},
			}), []byte(`{}`))
		}
		m.tick(s.sessions.All(), start.Add(at))
	}
	// Minute 1: 15 % ingress loss in every window: the host minute is
	// degraded (uplink) and the minute alerts.
	for i := 1; i <= 6; i++ {
		tick(time.Duration(i)*10*time.Second, 1000, 150, 0, 300)
	}
	// Minute 2 (25 s, the last): a clean ingress, 5 % loss on the viewer leg.
	for i := 7; i <= 8; i++ {
		tick(time.Duration(i)*10*time.Second, 1000, 0, 15, 285)
	}
	// The host sends nothing for a window: an alert.
	tick(90*time.Second, 0, 0, 0, 0)
	delete(s.sessions.sessions, session.ID())
	m.tick(s.sessions.All(), start.Add(100*time.Second))
	m.store.Flush()

	run, _, _ := m.store.SessionRun("st-g", 0)
	if run.Quality != QualityBad || run.DegradedMinutes != 2 || run.GradedMinutes != 4 {
		t.Fatalf("run grade = %+v", run)
	}
	minutes, _ := m.store.PeerMinutes("st-g", run.StartMs)
	legs := map[string]string{}
	for _, pm := range minutes {
		if pm.Degraded {
			legs[pm.User] = pm.Leg
		}
	}
	if legs["h"] != LegHostUplink || legs["v"] != LegViewer {
		t.Fatalf("legs = %v (minutes %+v)", legs, minutes)
	}
	alerts, _ := m.store.Alerts(0, 0, 100)
	kinds := map[string]int{}
	for _, a := range alerts {
		kinds[a.Kind]++
	}
	if kinds["ingress_loss_minute"] != 1 || kinds["no_media"] != 1 {
		t.Fatalf("alerts = %v", kinds)
	}

	// The overview of the last hour reads the same minutes and alerts.
	rec := httptest.NewRecorder()
	s.handleAdminOverview(rec, httptest.NewRequest("GET", "/admin/api/overview?range=1h", nil))
	var ov struct {
		Legs  map[string]int `json:"legs"`
		Tiles struct {
			Quality struct {
				Summary map[string]float64 `json:"summary"`
			} `json:"quality"`
		} `json:"tiles"`
		Load []map[string]interface{} `json:"load"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&ov); err != nil {
		t.Fatalf("overview: %v (%d)", err, rec.Code)
	}
	if ov.Legs[LegHostUplink] != 1 || ov.Legs[LegViewer] != 1 || ov.Tiles.Quality.Summary["graded_minutes"] != 4 ||
		ov.Tiles.Quality.Summary["good_pct"] != 50 || len(ov.Load) < 60 {
		t.Fatalf("overview = %+v", ov)
	}
	rec = httptest.NewRecorder()
	s.handleAdminAlerts(rec, httptest.NewRequest("GET", "/admin/api/alerts?from=1", nil))
	var al struct {
		Alerts []store.AlertOut `json:"alerts"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&al)
	if len(al.Alerts) != 2 {
		t.Fatalf("alerts endpoint = %+v", al)
	}
}

func logEntry(event, session string, data map[string]interface{}) logging.Entry {
	return logging.Entry{Timestamp: time.Now().UTC().Format(time.RFC3339), Level: "info", Event: event,
		SessionID: session, Data: data}
}

// A viewer leg's loss is the larger of its NACK share and the gcc share:
// gcc feedback counts the repairs as received and reads low.
func TestBuildMinuteViewerNACKShare(t *testing.T) {
	w := relayWin(10_000, `{"present":[["v","viewer"]],"viewer_lost":{"v":11},"viewer_acked":{"v":989},`+
		`"viewer_sent":{"v":5000},"viewer_nacked":{"v":121}}`)
	in := buildMinute("stream", 0, 60, []gradeWindow{w}, nil, gradeCarry{})
	if got := in.Peers[0].ViewerLossPct; got < 2.41 || got > 2.43 {
		t.Fatalf("viewer loss = %.3f, want the NACK share 2.42", got)
	}
	if g := gradeOf(t, in, "v"); !g.Degraded || g.Leg != LegViewer {
		t.Fatalf("grade = %+v", g)
	}
}
