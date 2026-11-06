package server

import (
	"encoding/json"
	"math"
	"runtime"
	"sort"
	"strings"

	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The minutes of a run, built from 10 s windows: the SFU's relay row of the
// window and the client reports and events that came in it. The live
// recorder and the backfill over stored rows build the same windows, so
// they grade the same way.

const minuteMs = int64(60_000)

// Ingress loss over this for a minute is an alert (the brief's rule).
const alertIngressLossPct = 10.0

type gradeStat struct {
	Ms   int64
	User string
	Role string
	Kind string // stream or voice
	Body map[string]interface{}
}

type gradeEvent struct {
	Ms   int64
	User string
	Kind string
}

type gradeWindow struct {
	Ms     int64
	Relay  map[string]interface{}
	Stats  []gradeStat
	Events []gradeEvent
}

// reconnectKinds are the events that count as a peer's reconnect.
var reconnectKinds = map[string]bool{
	"reconnect": true, "ice_restart": true, "connection_lost": true, "host_reconnected": true,
}

type peerKey struct{ user, role string }

// gradeCarry keeps a user's last running totals (voice reports carry
// totals) from one minute to the next.
type gradeCarry map[string]float64

func (c gradeCarry) delta(user, field string, v float64, first *bool) float64 {
	key := user + "\x00" + field
	prev, ok := c[key]
	c[key] = v
	if !ok {
		*first = true
		return 0
	}
	if v < prev { // a new connection starts its totals again
		return v
	}
	return v - prev
}

func mapOf(v interface{}) map[string]interface{} {
	m, _ := v.(map[string]interface{})
	return m
}

// buildMinute is one minute's input from its windows. fallback lists the
// run's users for rows stored without the "present" list.
func buildMinute(typ string, startMs int64, secs float64, wins []gradeWindow, fallback []peerKey, carry gradeCarry) MinuteInput {
	in := MinuteInput{Type: typ, MinuteMs: startMs, Seconds: secs, IngWorstPct: -1}
	peers := map[peerKey]*PeerMinuteInput{}
	order := []peerKey{}
	add := func(k peerKey) *PeerMinuteInput {
		if p, ok := peers[k]; ok {
			return p
		}
		p := &PeerMinuteInput{User: k.user, Role: k.role, ViewerLossPct: -1}
		peers[k] = p
		order = append(order, k)
		return p
	}
	roleOf := map[string]string{}
	sawPresent := false
	for _, w := range wins {
		r := w.Relay
		exp, lost := int64(num(r["ing_expected"])), int64(num(r["ing_lost"]))
		in.IngExp += exp
		in.IngLost += lost
		if exp >= gradeMinPackets {
			if pct := float64(max(lost, 0)) * 100 / float64(exp); pct > in.IngWorstPct {
				in.IngWorstPct = pct
			}
		}
		in.QueueDrop += int64(num(r["queue_dropped"]))
		if list, ok := r["present"].([]interface{}); ok {
			sawPresent = true
			for _, it := range list {
				pair, _ := it.([]interface{})
				if len(pair) == 2 {
					u, _ := pair[0].(string)
					role, _ := pair[1].(string)
					add(peerKey{u, role})
					roleOf[u] = role
				}
			}
		}
		for u, n := range mapOf(r["dropped_by"]) {
			if role, ok := roleOf[u]; ok {
				add(peerKey{u, role}).Dropped += int64(num(n))
			}
		}
		// A viewer leg's loss: the share of its packets the viewer NACKed
		// (the SFU's slow-viewer measure), or that gcc feedback reports
		// lost, whichever is larger.
		lostBy, ackedBy := mapOf(r["viewer_lost"]), mapOf(r["viewer_acked"])
		for u := range ackedBy {
			l, a := num(lostBy[u]), num(ackedBy[u])
			if l+a >= gradeMinPackets {
				p := add(peerKey{u, "viewer"})
				if pct := l * 100 / (l + a); pct > p.ViewerLossPct {
					p.ViewerLossPct = pct
				}
			}
		}
		sentBy, nackedBy := mapOf(r["viewer_sent"]), mapOf(r["viewer_nacked"])
		for u := range sentBy {
			if sent := num(sentBy[u]); sent >= gradeMinPackets {
				p := add(peerKey{u, "viewer"})
				if pct := num(nackedBy[u]) * 100 / sent; pct > p.ViewerLossPct {
					p.ViewerLossPct = pct
				}
			}
		}
		if len(ackedBy) == 0 && len(sentBy) == 0 {
			// Rows stored before the counts: the gcc loss estimate.
			for u, v := range mapOf(r["viewer_loss_pct"]) {
				p := add(peerKey{u, "viewer"})
				if pct := num(v); pct > p.ViewerLossPct {
					p.ViewerLossPct = pct
				}
			}
		}
		for _, e := range w.Events {
			if e.Kind == "rejected" {
				in.Rejects++
			}
		}
	}
	if !sawPresent {
		for _, k := range fallback {
			add(k)
			roleOf[k.user] = k.role
		}
	}

	type hostAcc struct {
		n               int
		cap, enc, encMs float64
		cfg, rung       float64
		idle            bool
	}
	hosts := map[string]*hostAcc{}
	for _, w := range wins {
		for _, st := range w.Stats {
			k := peerKey{st.User, st.Role}
			p := add(k)
			b := st.Body
			switch {
			case st.Kind == "stream" && st.Role == "host":
				p.HasHostStats = true
				h := hosts[st.User]
				if h == nil {
					h = &hostAcc{}
					hosts[st.User] = h
				}
				h.cfg, h.rung = num(b["fps_cfg"]), num(b["fps_out"])
				if num(b["cap_idle_ms"]) > 1000 || b["paused"] == true {
					h.idle = true
				}
				// The first report of a stream comes before the first
				// encoded frame: it says nothing about the machine.
				if num(b["enc_fps"]) <= 0 {
					continue
				}
				h.n++
				h.cap += num(b["cap_fps"])
				h.enc += num(b["enc_fps"])
				h.encMs += num(b["enc_ms_avg"])
			case st.Kind == "stream":
				p.HasViewerStats = true
				p.FreezeN += num(b["freeze_n"])
				p.FreezeMs += num(b["freeze_ms"])
				if av, ok := b["av_ms"]; ok {
					p.HasAV = true
					p.AVMaxMs = math.Max(p.AVMaxMs, math.Abs(num(av)))
				}
				p.JitterMaxMs = math.Max(p.JitterMaxMs, num(b["jit_ms"]))
			default:
				p.HasVoiceStats = true
				first := false
				p.LateUnderruns += carry.delta(st.User, "late_underruns", num(b["late_underruns"]), &first)
				p.SendSkips += carry.delta(st.User, "send_audio_skips", num(b["send_audio_skips"]), &first)
				p.RTTMaxMs = math.Max(p.RTTMaxMs, num(b["rtt_ms"]))
			}
		}
		for _, e := range w.Events {
			if reconnectKinds[e.Kind] && e.User != "" {
				role := roleOf[e.User]
				if role == "" {
					continue
				}
				add(peerKey{e.User, role}).Reconnects++
			}
		}
	}
	for u, h := range hosts {
		p := add(peerKey{u, "host"})
		p.FpsCfg, p.FpsRung, p.CapIdle = h.cfg, h.rung, h.idle
		if h.n > 0 {
			p.CapFps = math.Round(h.cap/float64(h.n)*10) / 10
			p.EncFps = math.Round(h.enc/float64(h.n)*10) / 10
			p.EncMs = math.Round(h.encMs/float64(h.n)*10) / 10
		} else {
			p.HasHostStats = false
		}
	}
	for _, k := range order {
		in.Peers = append(in.Peers, *peers[k])
	}
	return in
}

// runGrade is the grade state of one run.
type runGrade struct {
	graded, degraded int
	legs             map[string]int
	worstMs          int64
	worstScore       float64
	carry            gradeCarry
}

func newRunGrade() *runGrade {
	return &runGrade{legs: map[string]int{}, carry: gradeCarry{}}
}

func (g *runGrade) session() SessionGrade {
	return GradeSession(g.graded, g.degraded, g.legs)
}

// gradeMinuteInto grades one minute of a run, writes its peer minutes and
// returns the grades. It adds them to the run's grade.
func gradeMinuteInto(st *store.Store, g *runGrade, id string, startMs int64, in MinuteInput) []PeerGrade {
	grades := GradeMinute(in)
	score := 0.0
	for _, pg := range grades {
		if pg.Graded {
			g.graded++
		}
		if pg.Degraded {
			g.degraded++
			g.legs[pg.Leg]++
			score += 1 + math.Max(pg.LossPct, 0)/100
		}
		detail, _ := json.Marshal(pg)
		st.PeerMinute(store.PeerMinuteRow{
			ID: id, StartMs: startMs, MinuteMs: in.MinuteMs, User: pg.User, Role: pg.Role, Type: in.Type,
			Graded: pg.Graded, Degraded: pg.Degraded, Reasons: strings.Join(pg.Reasons, ","), Leg: pg.Leg,
			Detail: detail,
		})
	}
	if score > g.worstScore {
		g.worstScore, g.worstMs = score, in.MinuteMs
	}
	return grades
}

// windowsByMinute splits a run's windows into minutes from its start.
// A window belongs to the minute its end falls in.
func windowsByMinute(startMs int64, wins []gradeWindow) [][]gradeWindow {
	var out [][]gradeWindow
	cur, curIdx := []gradeWindow(nil), int64(-1)
	for _, w := range wins {
		idx := (w.Ms - 1 - startMs) / minuteMs
		if idx < 0 {
			idx = 0
		}
		if idx != curIdx && len(cur) > 0 {
			out = append(out, cur)
			cur = nil
		}
		curIdx = idx
		cur = append(cur, w)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

func minuteStartOf(startMs, windowMs int64) int64 {
	idx := (windowMs - 1 - startMs) / minuteMs
	if idx < 0 {
		idx = 0
	}
	return startMs + idx*minuteMs
}

// backfillGrades grades the ended runs of the store that have no grade:
// runs stored before the grades, and runs a stop left open. A run with no
// relay row cannot be graded and is marked "not graded".
func backfillGrades(st *store.Store, logf func(string, map[string]interface{})) {
	runs, err := st.UngradedRuns()
	if err != nil {
		logf("store_backfill_failed", map[string]interface{}{"error": err.Error()})
		return
	}
	graded, notGraded := 0, 0
	for _, run := range runs {
		end := run.EndMs + runEventsTail
		tl, err := st.Timeline(run.ID, run.StartMs, end)
		if err != nil {
			logf("store_backfill_failed", map[string]interface{}{"session": run.ID, "error": err.Error()})
			continue
		}
		if len(tl.Relay) == 0 {
			st.SetGrade(run.ID, run.StartMs, QualityNotGraded, "", 0, 0, 0)
			notGraded++
			continue
		}
		peers, _ := st.Peers(run.ID, run.StartMs)
		var fallback []peerKey
		for _, p := range peers {
			fallback = append(fallback, peerKey{p.User, p.Role})
		}
		wins := windowsFromRows(tl)
		g := newRunGrade()
		groups := windowsByMinute(run.StartMs, wins)
		for i, mw := range groups {
			mStart := minuteStartOf(run.StartMs, mw[0].Ms)
			secs := 60.0
			if i == len(groups)-1 {
				secs = math.Max(10, float64(mw[len(mw)-1].Ms-mStart)/1000)
			}
			gradeMinuteInto(st, g, run.ID, run.StartMs, buildMinute(run.Type, mStart, secs, mw, fallback, g.carry))
		}
		sg := g.session()
		st.SetGrade(run.ID, run.StartMs, sg.Quality, sg.Trouble, sg.Graded, sg.Degraded, g.worstMs)
		graded++
	}
	if len(runs) > 0 {
		st.Flush()
		logf("store_backfill_done", map[string]interface{}{"graded": graded, "not_graded": notGraded})
	}
}

// QualityNotGraded marks a run the backfill could not grade.
const QualityNotGraded = "not graded"

// windowsFromRows rebuilds a run's windows from its stored rows: each
// stats row and event goes to the first relay window that ends at or after
// it.
func windowsFromRows(tl store.Timeline) []gradeWindow {
	wins := make([]gradeWindow, 0, len(tl.Relay))
	for _, r := range tl.Relay {
		var body map[string]interface{}
		_ = json.Unmarshal(r.Body, &body)
		wins = append(wins, gradeWindow{Ms: r.Ms, Relay: body})
	}
	if len(wins) == 0 {
		return wins
	}
	at := func(ms int64) int {
		i := sort.Search(len(wins), func(i int) bool { return wins[i].Ms >= ms })
		if i == len(wins) {
			i = len(wins) - 1
		}
		return i
	}
	for _, s := range tl.Stats {
		var body map[string]interface{}
		_ = json.Unmarshal(s.Body, &body)
		i := at(s.Ms)
		wins[i].Stats = append(wins[i].Stats, gradeStat{Ms: s.Ms, User: s.User, Role: s.Role, Kind: s.Kind, Body: body})
	}
	for _, e := range tl.Events {
		i := at(e.Ms)
		wins[i].Events = append(wins[i].Events, gradeEvent{Ms: e.Ms, User: e.User, Kind: e.Kind})
	}
	return wins
}

// instanceSample is the instance's 10 s sample for the store: the numbers
// of the overview, so the 24 h and 7 d history outlives the 1 h in memory.
func (s *Server) instanceSample() map[string]interface{} {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	bwIn, bwOut, cpu := s.metrics.Rates()
	streams, voice := s.sessions.Count()
	streamsReal, voiceReal := s.sessions.CountReal()
	return map[string]interface{}{
		"cpu":          round2(cpu),
		"mem_mb":       round2(float64(mem.Alloc) / 1_000_000),
		"bw_in":        round2(bwIn),
		"bw_out":       round2(bwOut),
		"peers":        s.metrics.ActivePeers.Load(),
		"streams":      streams,
		"voice":        voice,
		"streams_real": streamsReal,
		"voice_real":   voiceReal,
	}
}
