package server

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/minutes"
	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The media admin's recorder: it feeds the disk store (internal/store) from
// three places, none of them on the media path.
//
//   - The log sink: every log line goes to the store's log table, and the
//     lines that mark a session event (a join, a leave, a reconnect, a rung
//     change, ...) also become event rows.
//   - The client stats handlers: every stream_client_stats and client_stats
//     report becomes a stats row.
//   - A 10 s tick: the session summaries and their users, the SFU's own
//     relay rows, the ingress loss episodes, the keyframe requests and the
//     peer minutes per day.
//
// Every write is a non-blocking enqueue; a full store queue drops it.

const mediaTickInterval = 10 * time.Second

// Ingress loss over this share of a 10 s window starts an episode (the
// brief's degraded-minute threshold); a window at or under it ends one.
const ingressLossEpisodePct = 2.0

// A window with fewer expected packets tells nothing about loss.
const ingressLossMinExpected = 50

type mediaRecorder struct {
	store *store.Store
	daily *minutes.DailyMinutes

	mu sync.Mutex
	// The host's last reported encode size and frame rate per session,
	// for the direction and the inferred reason of a rung change.
	hostRung map[string]hostRungState
	// The last rung each host announced (w, h, fps).
	lastRung map[string][3]int
	// Viewers whose first frames the recorder has seen in the stats.
	firstFrame map[*Peer]struct{}
	// The worst viewer-leg loss (the SFU's gcc estimate) per session and
	// viewer since the last tick, from stream_host_remb_tick.
	viewerLoss map[string]map[string]float64
	// pending holds what came for each session since the last tick: the
	// client reports, the events the grade reads, the viewer-leg counts.
	pending map[string]*pendingSession
	// alertLast dedupes the incident alerts: session and event, last ms.
	alertLast map[string]int64
	// sample reads the instance's numbers for its 10 s sample; nil in tests.
	sample func() map[string]interface{}
	// bundles are swept with the store's retention; nil in tests.
	bundles *bundleManager

	// tickMu serialises tick: the 10 s loop and the last tick at close.
	tickMu sync.Mutex
	runs   map[Session]*mediaRun
}

type hostRungState struct {
	w, h, fps  int
	encMsAvg   float64
	eqDrops    float64
	brTarget   float64
	reportedAt time.Time
}

type peerCounters struct {
	bytesSent, bytesRecv, pktsSent, pktsRecv, dropped int64
}

type mediaRun struct {
	id         string
	start      time.Time
	typ        string
	crew       string
	synthetic  bool
	hostID     string
	hostName   string
	lastActive time.Time
	prevAt     time.Time
	peerMin    float64

	peers map[*Peer]peerCounters
	// Bytes of the peers that left, kept in the run's totals.
	goneSent, goneRecv int64
	users              map[string]*store.PeerRow

	relayPrev    StreamRelaySnapshot
	hasRelayPrev bool
	lossOn       bool
	lossStart    time.Time
	lossPeak     float64

	// The grade: the windows of the current minute and the run's sums.
	grade         *runGrade
	wins          []gradeWindow
	minuteStartMs int64
	// Peers in a no-media episode (one alert per episode).
	noMedia map[*Peer]bool
	// The last sent and NACKed totals of each viewer leg.
	legPrev map[string][2]int64
}

type pendingSession struct {
	stats  []gradeStat
	events []gradeEvent
	// Packets of each viewer leg that gcc reported lost and acknowledged.
	lost, acked map[string]float64
}

// pendingCap bounds what one session can queue between two ticks.
const pendingCap = 4096

// incidentDedupMs: the same incident of a session is one alert a minute.
const incidentDedupMs = 60_000

func newMediaRecorder(st *store.Store) *mediaRecorder {
	return &mediaRecorder{
		store:      st,
		daily:      minutes.NewDailyMinutes(),
		hostRung:   make(map[string]hostRungState),
		lastRung:   make(map[string][3]int),
		firstFrame: make(map[*Peer]struct{}),
		viewerLoss: make(map[string]map[string]float64),
		pending:    make(map[string]*pendingSession),
		alertLast:  make(map[string]int64),
		runs:       make(map[Session]*mediaRun),
	}
}

// eventKinds maps the log events that mark a session event to the event
// kind of the timeline.
var eventKinds = map[string]string{
	"peer_joined":              "join",
	"peer_left":                "leave",
	"host_disconnected":        "host_disconnected",
	"host_reconnected":         "host_reconnected",
	"peer_evicted":             "reconnect",
	"ice_restart_sent":         "ice_restart",
	"peer_timeout_ice_restart": "ice_restart",
	"webrtc_disconnected":      "connection_lost",
	"webrtc_failed":            "connection_failed",
	"peer_force_disconnect":    "kicked",
	"stream_idr_replayed":      "keyframe_to_viewer",
	"stream_first_rtp":         "first_packet",
	"stream_rung_change":       "rung_change",
	"session_reject":           "rejected",
	"renegotiate_failed":       "renegotiation_failed",
	"rtp_blackout":             "audio_blackout",
	"session_closed":           "session_closed",
}

// onLog is the logger's sink. It runs on the logging goroutine: it reads
// entry.Data only here, and only enqueues.
func (m *mediaRecorder) onLog(ms int64, entry logging.Entry, line []byte) {
	if entry.Event == "stream_client_stats" {
		// The host's capture source can be a window title: not on disk.
		line = redactedStatsLine(entry, line)
	}
	m.store.Log(store.LogRow{
		Ms:      ms,
		Level:   int(logging.ParseLevel(entry.Level)),
		Event:   entry.Event,
		Session: entry.SessionID,
		User:    entry.UserID,
		Line:    line,
	})

	if level := logging.ParseLevel(entry.Level); level >= logging.LevelWarn {
		if category := categorizeIncident(entry); category != "" {
			m.incident(ms, entry, category, level)
		}
	}
	if entry.Event == "stream_host_remb_tick" {
		m.noteViewerLoss(entry)
		return
	}
	kind, ok := eventKinds[entry.Event]
	if !ok || entry.SessionID == "" {
		return
	}
	user := entry.UserID
	detail := map[string]interface{}{}
	for k, v := range entry.Data {
		detail[k] = v
	}
	switch entry.Event {
	case "stream_idr_replayed":
		if v, ok := entry.Data["viewer"].(string); ok {
			user = v
			delete(detail, "viewer")
		}
	case "stream_rung_change":
		if !m.rungReason(entry.SessionID, detail) {
			// The host repeats its rung (for a viewer that joins): no change.
			return
		}
	}
	if reconnectKinds[kind] || kind == "rejected" {
		m.mu.Lock()
		ps := m.pend(entry.SessionID)
		if len(ps.events) < pendingCap {
			ps.events = append(ps.events, gradeEvent{Ms: ms, User: user, Kind: kind})
		}
		m.mu.Unlock()
	}
	m.event(ms, entry.SessionID, user, kind, detail)
}

// pend is the pending set of a session; m.mu held.
func (m *mediaRecorder) pend(session string) *pendingSession {
	ps := m.pending[session]
	if ps == nil {
		ps = &pendingSession{lost: map[string]float64{}, acked: map[string]float64{}}
		m.pending[session] = ps
	}
	return ps
}

// incident writes an alert for a warning of the incident timeline
// (capacity, renegotiation, connectivity, media), once a minute per
// session and event.
func (m *mediaRecorder) incident(ms int64, entry logging.Entry, category string, level logging.Level) {
	key := entry.SessionID + "\x00" + entry.Event
	m.mu.Lock()
	last, seen := m.alertLast[key]
	if seen && ms-last < incidentDedupMs {
		m.mu.Unlock()
		return
	}
	m.alertLast[key] = ms
	if len(m.alertLast) > 10_000 {
		m.alertLast = map[string]int64{key: ms}
	}
	m.mu.Unlock()
	detail := map[string]interface{}{}
	for k, v := range entry.Data {
		if k != "remote_ip" {
			detail[k] = v
		}
	}
	body, _ := json.Marshal(detail)
	m.store.Alert(store.AlertRow{
		Ms: ms, Kind: entry.Event, Category: category, Severity: level.String(),
		Session: entry.SessionID, User: entry.UserID, Detail: body,
	})
}

func (m *mediaRecorder) alert(ms int64, kind, category, session, user string, detail map[string]interface{}) {
	body, _ := json.Marshal(detail)
	m.store.Alert(store.AlertRow{
		Ms: ms, Kind: kind, Category: category, Severity: "warn", Session: session, User: user, Detail: body,
	})
}

func (m *mediaRecorder) event(ms int64, session, user, kind string, detail map[string]interface{}) {
	body, _ := json.Marshal(detail)
	m.store.Event(store.EventRow{Ms: ms, Session: session, User: user, Kind: kind, Detail: body})
}

// redactedStatsLine is the stream_client_stats line without the host's
// capture source ("src").
func redactedStatsLine(entry logging.Entry, line []byte) []byte {
	stats, ok := entry.Data["stats"].(map[string]interface{})
	if !ok {
		return line
	}
	if _, has := stats["src"]; !has {
		return line
	}
	copyStats := make(map[string]interface{}, len(stats))
	for k, v := range stats {
		if k != "src" {
			copyStats[k] = v
		}
	}
	data := make(map[string]interface{}, len(entry.Data))
	for k, v := range entry.Data {
		data[k] = v
	}
	data["stats"] = copyStats
	entry.Data = data
	out, err := json.Marshal(entry)
	if err != nil {
		return line
	}
	return out
}

// noteViewerLoss keeps the worst gcc loss estimate per viewer leg until
// the next tick writes it into the relay row.
func (m *mediaRecorder) noteViewerLoss(entry logging.Entry) {
	var items []map[string]interface{}
	switch v := entry.Data["viewer_bwe"].(type) {
	case []map[string]interface{}:
		items = v
	case []interface{}:
		for _, d := range v {
			if mm, ok := d.(map[string]interface{}); ok {
				items = append(items, mm)
			}
		}
	}
	if len(items) == 0 || entry.SessionID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ps := m.pend(entry.SessionID)
	for _, it := range items {
		if viewer, _ := it["viewer"].(string); viewer != "" {
			ps.lost[viewer] += num(it["lost"])
			ps.acked[viewer] += num(it["acked"])
		}
	}
	worst := m.viewerLoss[entry.SessionID]
	if worst == nil {
		worst = make(map[string]float64)
		m.viewerLoss[entry.SessionID] = worst
	}
	for _, it := range items {
		viewer, _ := it["viewer"].(string)
		loss, ok := it["avg_loss"].(float64)
		if viewer == "" || !ok {
			continue
		}
		if loss > worst[viewer] {
			worst[viewer] = loss
		} else if _, seen := worst[viewer]; !seen {
			worst[viewer] = loss
		}
	}
}

// rungReason adds the direction and the reason of a rung change, and
// reports false when the rung is the one the host already had (the host
// sends its rung again for a viewer that joins). The rung message carries
// no reason, so it is inferred from the host's last stats report: encoder
// overload (queue drops, or a mean encode time at or over 80 % of the frame
// interval) or the bitrate target on the way down, a recovery on the way up.
func (m *mediaRecorder) rungReason(session string, detail map[string]interface{}) bool {
	w, _ := detail["w"].(int)
	h, _ := detail["h"].(int)
	fps, _ := detail["fps"].(int)
	next := [3]int{w, h, fps}
	m.mu.Lock()
	stats, haveStats := m.hostRung[session]
	prev, havePrev := m.lastRung[session]
	if !havePrev && haveStats && stats.fps > 0 {
		prev, havePrev = [3]int{stats.w, stats.h, stats.fps}, true
	}
	m.lastRung[session] = next
	m.mu.Unlock()
	if havePrev && prev == next {
		return false
	}
	detail["reason_source"] = "inferred"
	if !havePrev || prev[2] <= 0 {
		detail["reason"] = "start"
		return true
	}
	detail["from"] = fmt.Sprintf("%dx%d@%d", prev[0], prev[1], prev[2])
	if w*h*fps >= prev[0]*prev[1]*prev[2] {
		detail["direction"] = "up"
		detail["reason"] = "recovered"
		return true
	}
	detail["direction"] = "down"
	budgetMs := 1000.0 / float64(prev[2])
	if haveStats && (stats.eqDrops > 0 || stats.encMsAvg >= 0.8*budgetMs) {
		detail["reason"] = "encoder"
	} else {
		detail["reason"] = "bitrate"
	}
	return true
}

// onStats stores one client stats report. kind is "stream" or "voice".
func (m *mediaRecorder) onStats(session string, peer *Peer, kind string, stats map[string]interface{}) {
	now := time.Now()
	body := stats
	if _, has := stats["src"]; has {
		body = make(map[string]interface{}, len(stats))
		for k, v := range stats {
			if k != "src" {
				body[k] = v
			}
		}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return
	}
	m.store.Stats(store.StatsRow{
		Ms: now.UnixMilli(), Session: session, User: peer.UserID, Role: peer.Role, Kind: kind, Body: raw,
	})
	if session != "" {
		m.mu.Lock()
		ps := m.pend(session)
		if len(ps.stats) < pendingCap {
			ps.stats = append(ps.stats, gradeStat{Ms: now.UnixMilli(), User: peer.UserID, Role: peer.Role, Kind: kind, Body: body})
		}
		m.mu.Unlock()
	}
	if session == "" || kind != "stream" {
		return
	}
	switch peer.Role {
	case "host":
		m.mu.Lock()
		st := m.hostRung[session]
		st.w, st.h, st.fps = int(num(stats["w"])), int(num(stats["h"])), int(num(stats["fps_cfg"]))
		st.encMsAvg, st.eqDrops, st.brTarget = num(stats["enc_ms_avg"]), num(stats["eq_drops"]), num(stats["br_target"])
		st.reportedAt = now
		m.hostRung[session] = st
		m.mu.Unlock()
	case "viewer":
		if num(stats["rel"]) <= 0 && num(stats["present_fps"]) <= 0 {
			return
		}
		m.mu.Lock()
		_, seen := m.firstFrame[peer]
		if !seen {
			m.firstFrame[peer] = struct{}{}
		}
		m.mu.Unlock()
		if !seen {
			// The report comes every 10 s: the first frame came at most
			// within_ms after the viewer connected.
			m.event(now.UnixMilli(), session, peer.UserID, "first_frame", map[string]interface{}{
				"by":        "stats",
				"within_ms": now.Sub(peer.ConnectedAt).Milliseconds(),
			})
		}
	}
}

func num(v interface{}) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case uint64:
		return float64(n)
	case uint32:
		return float64(n)
	case float32:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}

func (m *mediaRecorder) run(s *Server, stop <-chan struct{}) {
	ticker := time.NewTicker(mediaTickInterval)
	defer ticker.Stop()
	sweep := time.NewTicker(time.Hour)
	defer sweep.Stop()
	for {
		select {
		case <-sweep.C:
			if m.bundles != nil {
				m.bundles.sweep(time.Now())
			}
		case <-ticker.C:
			m.tick(s.sessions.All(), time.Now())
		case <-stop:
			return
		}
	}
}

// tick writes the summaries, relay rows and minutes of every session, and
// ends the runs whose session left the store.
func (m *mediaRecorder) tick(sessions []Session, now time.Time) {
	m.tickMu.Lock()
	defer m.tickMu.Unlock()
	seen := make(map[Session]struct{}, len(sessions))
	live := make(map[*Peer]struct{})
	m.mu.Lock()
	viewerLoss := m.viewerLoss
	m.viewerLoss = make(map[string]map[string]float64)
	pending := m.pending
	m.pending = make(map[string]*pendingSession)
	m.mu.Unlock()

	for _, sess := range sessions {
		seen[sess] = struct{}{}
		r := m.runs[sess]
		if r == nil {
			r = &mediaRun{
				id:      sess.ID(),
				start:   sess.CreatedAt(),
				typ:     sess.Type(),
				crew:    sess.CrewID(),
				prevAt:  sess.CreatedAt(),
				peers:   make(map[*Peer]peerCounters),
				users:   make(map[string]*store.PeerRow),
				grade:   newRunGrade(),
				noMedia: make(map[*Peer]bool),
				legPrev: make(map[string][2]int64),
			}
			m.runs[sess] = r
		}
		r.synthetic = !sess.HadRealPeer()
		peers := sess.AllPeers()
		var dSent, dRecv, dPktsSent, dPktsRecv, dDropped int64
		current := make(map[*Peer]struct{}, len(peers))
		present := make([][2]string, 0, len(peers))
		droppedBy := map[string]int64{}
		peerSent := make(map[*Peer]int64, len(peers))
		peerRecv := make(map[*Peer]int64, len(peers))
		for _, p := range peers {
			live[p] = struct{}{}
			current[p] = struct{}{}
			from := r.prevAt
			if p.ConnectedAt.After(from) {
				from = p.ConnectedAt
			}
			if now.After(from) {
				m.daily.Add(from, now, r.typ, p.Role, p.Synthetic)
				r.peerMin += now.Sub(from).Minutes()
			}
			c := peerCounters{
				bytesSent: p.BytesSent.Load(), bytesRecv: p.BytesReceived.Load(),
				pktsSent: p.PacketsSent.Load(), pktsRecv: p.PacketsReceived.Load(),
				dropped: p.PacketsDropped.Load(),
			}
			prev := r.peers[p]
			dSent += c.bytesSent - prev.bytesSent
			dRecv += c.bytesRecv - prev.bytesRecv
			dPktsSent += c.pktsSent - prev.pktsSent
			dPktsRecv += c.pktsRecv - prev.pktsRecv
			dDropped += c.dropped - prev.dropped
			if d := c.dropped - prev.dropped; d > 0 {
				droppedBy[p.UserID] += d
			}
			peerSent[p], peerRecv[p] = c.bytesSent-prev.bytesSent, c.pktsRecv-prev.pktsRecv
			present = append(present, [2]string{p.UserID, p.Role})
			r.peers[p] = c
			if p.Role == "host" {
				r.hostID, r.hostName = p.UserID, p.Username
			}
			key := p.UserID + "\x00" + p.Role
			u := r.users[key]
			if u == nil {
				u = &store.PeerRow{ID: r.id, StartMs: r.start.UnixMilli(), User: p.UserID, Role: p.Role,
					FirstMs: p.ConnectedAt.UnixMilli()}
				r.users[key] = u
			}
			u.Username = p.Username
			if ms := p.ConnectedAt.UnixMilli(); ms < u.FirstMs {
				u.FirstMs = ms
			}
			u.LastMs = now.UnixMilli()
			m.store.Peer(*u)
		}
		for p, c := range r.peers {
			if _, ok := current[p]; !ok {
				r.goneSent += c.bytesSent
				r.goneRecv += c.bytesRecv
				delete(r.peers, p)
			}
		}
		if len(peers) > 0 {
			r.lastActive = now
		}

		secs := now.Sub(r.prevAt).Seconds()
		if secs <= 0 {
			secs = mediaTickInterval.Seconds()
		}
		row := map[string]interface{}{
			"members":   len(peers),
			"out_kbps":  round1(float64(dSent) * 8 / secs / 1000),
			"in_kbps":   round1(float64(dRecv) * 8 / secs / 1000),
			"out_pkts":  dPktsSent,
			"in_pkts":   dPktsRecv,
			"dropped":   dDropped,
			"window_ms": int64(secs * 1000),
			"present":   present,
		}
		if len(droppedBy) > 0 {
			row["dropped_by"] = droppedBy
		}
		ps := pending[r.id]
		if ps != nil && len(ps.acked) > 0 {
			row["viewer_lost"], row["viewer_acked"] = ps.lost, ps.acked
		}
		if stream, ok := sess.(*StreamSession); ok {
			m.streamRow(r, stream.RelaySnapshot(), row, now)
			legSent, legNACKed := map[string]int64{}, map[string]int64{}
			for user, c := range stream.viewerLegLoss() {
				prev := r.legPrev[user]
				if c[0] < prev[0] || c[1] < prev[1] {
					prev = [2]int64{} // a new leg
				}
				legSent[user], legNACKed[user] = c[0]-prev[0], c[1]-prev[1]
				r.legPrev[user] = c
			}
			if len(legSent) > 0 {
				row["viewer_sent"], row["viewer_nacked"] = legSent, legNACKed
			}
			if loss := viewerLoss[r.id]; len(loss) > 0 {
				pct := make(map[string]float64, len(loss))
				for v, l := range loss {
					pct[v] = round1(l * 100)
				}
				row["viewer_loss_pct"] = pct
			}
		}
		body, _ := json.Marshal(row)
		m.store.Relay(store.RelayRow{Ms: now.UnixMilli(), Session: r.id, Body: body})
		w := gradeWindow{Ms: now.UnixMilli()}
		_ = json.Unmarshal(body, &w.Relay)
		if ps != nil {
			w.Stats, w.Events = ps.stats, ps.events
		}
		m.addWindow(r, w)
		m.noMediaAlerts(r, sess, peers, peerSent, peerRecv, row, now)
		m.store.Session(m.summary(r, 0, now))
		r.prevAt = now
	}

	for sess, r := range m.runs {
		if _, ok := seen[sess]; ok {
			continue
		}
		end := r.lastActive
		if end.IsZero() {
			end = now
		}
		if r.lossOn {
			m.endLossEpisode(r, end)
		}
		if len(r.wins) > 0 {
			last := r.wins[len(r.wins)-1].Ms
			m.gradeWins(r, math.Max(10, float64(last-r.minuteStartMs)/1000))
		}
		m.store.Session(m.summary(r, end.UnixMilli(), now))
		delete(m.runs, sess)
		m.mu.Lock()
		delete(m.hostRung, r.id)
		delete(m.lastRung, r.id)
		m.mu.Unlock()
	}

	m.mu.Lock()
	for p := range m.firstFrame {
		if _, ok := live[p]; !ok {
			delete(m.firstFrame, p)
		}
	}
	m.mu.Unlock()

	for k, mins := range m.daily.Flush() {
		m.store.AddMinutes(k.Day, k.Type, k.Role, k.Synthetic, mins)
	}
	if m.sample != nil && len(sessions) >= 0 {
		if body, err := json.Marshal(m.sample()); err == nil {
			m.store.InstanceSample(now.UnixMilli(), body)
		}
	}
}

// viewerLegLoss is the packets sent on each viewer's video leg and those
// the viewer NACKed (stream_slow_viewer.go), as running totals.
func (s *StreamSession) viewerLegLoss() map[string][2]int64 {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	out := make(map[string][2]int64, len(s.viewerVideo))
	for user, leg := range s.viewerVideo {
		out[user] = [2]int64{leg.loss.sent.Load(), leg.loss.nacked.Load()}
	}
	return out
}

// liveGrade is the grade so far of a session's current run.
func (m *mediaRecorder) liveGrade(sess Session) (SessionGrade, bool) {
	if m == nil {
		return SessionGrade{}, false
	}
	m.tickMu.Lock()
	defer m.tickMu.Unlock()
	r, ok := m.runs[sess]
	if !ok {
		return SessionGrade{}, false
	}
	g := r.grade.session()
	if g.Quality == QualityNoData {
		g.Quality = ""
	}
	return g, true
}

// addWindow adds a 10 s window to its run and grades the minute before it
// when the window starts a new minute.
func (m *mediaRecorder) addWindow(r *mediaRun, w gradeWindow) {
	mStart := minuteStartOf(r.start.UnixMilli(), w.Ms)
	if len(r.wins) > 0 && mStart != r.minuteStartMs {
		m.gradeWins(r, 60)
	}
	if len(r.wins) == 0 {
		r.minuteStartMs = mStart
	}
	r.wins = append(r.wins, w)
}

// gradeWins grades the run's current minute (secs long) and alerts on an
// ingress loss over 10 % for the minute.
func (m *mediaRecorder) gradeWins(r *mediaRun, secs float64) {
	in := buildMinute(r.typ, r.minuteStartMs, secs, r.wins, nil, r.grade.carry)
	gradeMinuteInto(m.store, r.grade, r.id, r.start.UnixMilli(), in)
	if in.IngExp >= gradeMinPackets {
		if pct := float64(max(in.IngLost, 0)) * 100 / float64(in.IngExp); pct > alertIngressLossPct {
			m.alert(in.MinuteMs+int64(secs*1000), "ingress_loss_minute", "media", r.id, r.hostID, map[string]interface{}{
				"pct": round2(pct), "minute_ms": in.MinuteMs, "seconds": int(secs),
			})
		}
	}
	r.wins = nil
}

// noMediaAlerts: a peer with no media for a 10 s window is an alert, once
// per episode. A stream host that sent nothing; a viewer that got nothing
// while the host sent; a voice member that says it speaks and sent nothing
// (a silent member sends nothing on purpose).
func (m *mediaRecorder) noMediaAlerts(r *mediaRun, sess Session, peers []*Peer, sent, recv map[*Peer]int64, row map[string]interface{}, now time.Time) {
	hostSending := num(row["rtp_in_kbps"]) > 0
	for _, p := range peers {
		if now.Sub(p.ConnectedAt) < mediaTickInterval || !peerConnectedFn(p) {
			continue
		}
		silent := false
		switch {
		case r.typ == "stream" && p.Role == "host":
			silent = recv[p] == 0
		case r.typ == "stream":
			silent = hostSending && sent[p] == 0
		default:
			silent = p.speaking.Load() && recv[p] == 0
		}
		if silent && !r.noMedia[p] {
			m.alert(now.UnixMilli(), "no_media", "media", r.id, p.UserID, map[string]interface{}{
				"role": p.Role, "seconds": int(mediaTickInterval.Seconds()),
			})
		}
		r.noMedia[p] = silent
	}
	for p := range r.noMedia {
		found := false
		for _, q := range peers {
			if q == p {
				found = true
				break
			}
		}
		if !found {
			delete(r.noMedia, p)
		}
	}
}

// peerConnectedFn is peerConnected; tests replace it.
var peerConnectedFn = peerConnected

func peerConnected(p *Peer) bool {
	pc, _ := p.currentPeerConnection()
	return pc != nil && pc.ConnectionState().String() == "connected"
}

// streamRow adds the relay counters of the window to row, and writes the
// ingress loss episodes and the keyframe requests as events.
func (m *mediaRecorder) streamRow(r *mediaRun, cur StreamRelaySnapshot, row map[string]interface{}, now time.Time) {
	prev := r.relayPrev
	if !r.hasRelayPrev {
		prev = StreamRelaySnapshot{}
	}
	r.relayPrev, r.hasRelayPrev = cur, true
	secs := now.Sub(r.prevAt).Seconds()
	if secs <= 0 {
		secs = mediaTickInterval.Seconds()
	}
	expected := cur.RTPIngressExpected - prev.RTPIngressExpected
	lost := cur.RTPIngressLost - prev.RTPIngressLost
	if lost < 0 {
		lost = 0
	}
	lossPct := 0.0
	if expected > 0 {
		lossPct = float64(lost) * 100 / float64(expected)
	}
	row["rtp_in_kbps"] = round1(float64(cur.RTPInBytes-prev.RTPInBytes) * 8 / secs / 1000)
	row["rtp_fwd_kbps"] = round1(float64(cur.RTPForwardedBytes-prev.RTPForwardedBytes) * 8 / secs / 1000)
	row["ing_expected"] = expected
	row["ing_lost"] = lost
	row["ing_loss_pct"] = round2(lossPct)
	row["rtp_dropped"] = cur.RTPDroppedPackets - prev.RTPDroppedPackets
	row["queue_dropped"] = cur.RTPQueueDroppedPackets - prev.RTPQueueDroppedPackets
	row["slow_dropped"] = cur.RTPSlowViewerDroppedPackets - prev.RTPSlowViewerDroppedPackets
	row["nacked"] = cur.RTPViewerNACKedPackets - prev.RTPViewerNACKedPackets
	row["rtx_recovered"] = cur.RTPRTXRecovered - prev.RTPRTXRecovered
	kf := StreamKeyframeRequests{
		Viewer: cur.KeyframeRequests.Viewer - prev.KeyframeRequests.Viewer,
		Join:   cur.KeyframeRequests.Join - prev.KeyframeRequests.Join,
		Slow:   cur.KeyframeRequests.Slow - prev.KeyframeRequests.Slow,
		Rescue: cur.KeyframeRequests.Rescue - prev.KeyframeRequests.Rescue,
	}
	row["kf"] = kf
	if kf.Viewer+kf.Join+kf.Slow+kf.Rescue > 0 {
		m.event(now.UnixMilli(), r.id, "", "keyframe_request", map[string]interface{}{
			"viewer": kf.Viewer, "join": kf.Join, "slow": kf.Slow, "rescue": kf.Rescue,
		})
	}

	if expected < ingressLossMinExpected {
		return
	}
	switch {
	case lossPct > ingressLossEpisodePct && !r.lossOn:
		r.lossOn, r.lossStart, r.lossPeak = true, now, lossPct
		m.event(now.UnixMilli(), r.id, r.hostID, "ingress_loss", map[string]interface{}{
			"pct": round2(lossPct), "window_s": int(math.Round(secs)),
		})
	case lossPct > ingressLossEpisodePct:
		if lossPct > r.lossPeak {
			r.lossPeak = lossPct
		}
	case r.lossOn:
		m.endLossEpisode(r, now)
	}
}

func (m *mediaRecorder) endLossEpisode(r *mediaRun, at time.Time) {
	m.event(at.UnixMilli(), r.id, r.hostID, "ingress_clean", map[string]interface{}{
		"seconds":  int(math.Round(at.Sub(r.lossStart).Seconds())),
		"peak_pct": round2(r.lossPeak),
	})
	r.lossOn = false
}

func (m *mediaRecorder) summary(r *mediaRun, endMs int64, now time.Time) store.SessionRow {
	sent, recv := r.goneSent, r.goneRecv
	for _, c := range r.peers {
		sent += c.bytesSent
		recv += c.bytesRecv
	}
	distinct := make(map[string]struct{}, len(r.users))
	for _, u := range r.users {
		distinct[u.User] = struct{}{}
	}
	sg := r.grade.session()
	quality := sg.Quality
	if quality == QualityNoData && endMs == 0 {
		quality = "" // a live run with no graded minute yet
	}
	return store.SessionRow{
		ID: r.id, StartMs: r.start.UnixMilli(), Type: r.typ, CrewID: r.crew,
		HostUserID: r.hostID, HostUsername: r.hostName, Synthetic: r.synthetic,
		EndMs: endMs, LastSeenMs: now.UnixMilli(), Peers: len(distinct),
		PeerMinutes: round2(r.peerMin), BytesSent: sent, BytesRecv: recv,
		Quality: quality, Trouble: sg.Trouble, GradedMinutes: sg.Graded,
		DegradedMinutes: sg.Degraded, WorstMinuteMs: r.grade.worstMs,
	}
}

// close ends the open runs (a clean stop) and flushes the store.
func (m *mediaRecorder) close() {
	m.tick(nil, time.Now())
	m.store.Flush()
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
