package server

import (
	"math"
	"sort"
)

// The grade of every peer minute and the leg that caused it (the media
// admin brief, section 2.3; 01-SFU.md section 8.7). Pure functions over
// the minute's inputs, so the live recorder and the backfill over stored
// rows grade the same way.

const (
	// A peer minute is degraded over these.
	gradeLossPct    = 2.0  // loss on the peer's own leg
	gradeAVOffsetMs = 80.0 // |A/V offset| the viewer measured
	gradeConcealPct = 5.0  // voice concealment, share of the minute
	// A session is degraded under this share of degraded peer minutes,
	// bad at or over it.
	gradeBadShare = 0.10
	// The host is at its preset when it captures and encodes at 90 % of
	// the configured frame rate or more.
	gradeFpsAtPreset = 0.9
	// Viewer jitter over this is evidence of the viewer's own leg.
	gradeViewerJitterMs = 30.0
	// A loss share needs this many packets to tell anything.
	gradeMinPackets = 50
	// One voice playout frame (libmello's 10 ms mix): a late underrun is
	// one frame of concealment.
	voiceFrameMs = 10.0
)

// Legs, in the order the rule tries them.
const (
	LegSFU           = "sfu"
	LegHostUplink    = "host_uplink"
	LegHostMachine   = "host_machine"
	LegViewer        = "viewer_leg"
	LegSenderMachine = "sender_machine"
	LegReceiver      = "receiver_leg"
	LegUnknown       = "unknown"
)

// Grades of a session.
const (
	QualityGood     = "good"
	QualityDegraded = "degraded"
	QualityBad      = "bad"
	QualityNoData   = "no data"
)

// PeerMinuteInput is what one peer's minute carries.
type PeerMinuteInput struct {
	User string `json:"user"`
	Role string `json:"role"`

	// The SFU's view of a stream viewer's leg (gcc feedback): the worst
	// 10 s window's loss in the minute, -1 when unknown.
	ViewerLossPct float64 `json:"viewer_loss_pct"`

	// From the viewer's stream reports.
	HasViewerStats bool    `json:"has_viewer_stats,omitempty"`
	FreezeN        float64 `json:"freeze_n,omitempty"`
	FreezeMs       float64 `json:"freeze_ms,omitempty"`
	AVMaxMs        float64 `json:"av_max_ms,omitempty"`
	HasAV          bool    `json:"has_av,omitempty"`
	JitterMaxMs    float64 `json:"jitter_max_ms,omitempty"`

	// From the host's stream reports (means over the minute's reports
	// with frames). FpsRung is the frame rate of the host's current rung
	// (fps_out): the preset unless the controller stepped it down.
	HasHostStats bool    `json:"has_host_stats,omitempty"`
	CapFps       float64 `json:"cap_fps,omitempty"`
	EncFps       float64 `json:"enc_fps,omitempty"`
	FpsCfg       float64 `json:"fps_cfg,omitempty"`
	FpsRung      float64 `json:"fps_rung,omitempty"`
	EncMs        float64 `json:"enc_ms,omitempty"`
	CapIdle      bool    `json:"cap_idle,omitempty"`

	// From a voice member's reports: late underruns (concealed 10 ms
	// frames) and skipped sends in the minute.
	HasVoiceStats bool    `json:"has_voice_stats,omitempty"`
	LateUnderruns float64 `json:"late_underruns,omitempty"`
	SendSkips     float64 `json:"send_skips,omitempty"`
	RTTMaxMs      float64 `json:"rtt_max_ms,omitempty"`

	// From the SFU: reconnects (a replaced connection, an ICE restart, a
	// lost connection) and packets the SFU dropped toward the peer.
	Reconnects int   `json:"reconnects,omitempty"`
	Dropped    int64 `json:"dropped,omitempty"`
}

// MinuteInput is one minute of one session run.
type MinuteInput struct {
	Type     string  `json:"type"` // stream or voice
	MinuteMs int64   `json:"minute_ms"`
	Seconds  float64 `json:"seconds"` // the minute's length (the last can be short)
	IngExp   int64   `json:"ing_expected"`
	IngLost  int64   `json:"ing_lost"`
	// IngWorstPct is the worst 10 s window's host-leg loss, -1 unknown.
	IngWorstPct float64 `json:"ing_worst_pct"`
	QueueDrop   int64   `json:"queue_dropped"`
	Rejects     int     `json:"rejects"`
	Peers       []PeerMinuteInput
}

// PeerGrade is the grade of one peer minute.
type PeerGrade struct {
	User     string   `json:"user"`
	Role     string   `json:"role"`
	Graded   bool     `json:"graded"`
	Degraded bool     `json:"degraded"`
	Reasons  []string `json:"reasons"`
	Leg      string   `json:"leg"`
	// The numbers the grade read, for the verdict sentence.
	LossPct    float64 `json:"loss_pct"` // -1: unknown
	IngressPct float64 `json:"ingress_pct"`
	FreezeMs   float64 `json:"freeze_ms"`
	AVMaxMs    float64 `json:"av_max_ms"`
	ConcealPct float64 `json:"conceal_pct"`
	CapFps     float64 `json:"cap_fps"`
	EncFps     float64 `json:"enc_fps"`
	FpsCfg     float64 `json:"fps_cfg"`
	FpsRung    float64 `json:"fps_rung"`
	EncMs      float64 `json:"enc_ms"`
}

// ingressPct is the minute's host-leg loss: its worst 10 s window, so a
// burst that froze the viewers is not averaged away. -1: unknown.
func ingressPct(in MinuteInput) float64 {
	return in.IngWorstPct
}

func round2f(v float64) float64 { return math.Round(v*100) / 100 }

// GradeMinute grades every peer of the minute and names the leg of each
// degraded one.
func GradeMinute(in MinuteInput) []PeerGrade {
	ing := ingressPct(in)
	var host *PeerMinuteInput
	for i := range in.Peers {
		if in.Peers[i].Role == "host" {
			host = &in.Peers[i]
		}
	}
	secs := in.Seconds
	if secs <= 0 {
		secs = 60
	}
	out := make([]PeerGrade, 0, len(in.Peers))
	for _, p := range in.Peers {
		g := PeerGrade{User: p.User, Role: p.Role, Reasons: []string{}, LossPct: -1, IngressPct: round2f(ing)}
		switch {
		case in.Type == "stream" && p.Role == "host":
			if ing >= 0 {
				g.Graded = true
				g.LossPct = round2f(ing)
				if ing > gradeLossPct {
					g.Reasons = append(g.Reasons, "loss")
				}
			}
			if p.HasHostStats {
				g.Graded = true
				g.CapFps, g.EncFps, g.FpsCfg, g.FpsRung, g.EncMs = p.CapFps, p.EncFps, p.FpsCfg, p.FpsRung, p.EncMs
				// Added to the brief's four: a host that captures or
				// encodes under its rung's frame rate (or over the frame
				// budget) degrades every viewer's picture without a stall.
				if hostMachineShort(&p) {
					g.Reasons = append(g.Reasons, "frame_rate")
				}
			}
		case in.Type == "stream":
			if loss := p.ViewerLossPct; loss >= 0 {
				g.Graded = true
				g.LossPct = round2f(loss)
				if loss > gradeLossPct {
					g.Reasons = append(g.Reasons, "loss")
				}
			}
			if p.HasViewerStats {
				g.Graded = true
				g.FreezeMs = p.FreezeMs
				// The client counts a stall of 150 ms or more as a freeze.
				if p.FreezeN > 0 {
					g.Reasons = append(g.Reasons, "freeze")
				}
				if p.HasAV {
					g.AVMaxMs = p.AVMaxMs
					if p.AVMaxMs > gradeAVOffsetMs {
						g.Reasons = append(g.Reasons, "av_offset")
					}
				}
			}
		default: // voice
			if p.HasVoiceStats {
				g.Graded = true
				g.ConcealPct = round2f(p.LateUnderruns * voiceFrameMs / (secs * 1000) * 100)
				if g.ConcealPct > gradeConcealPct {
					g.Reasons = append(g.Reasons, "concealment")
				}
			}
		}
		g.Degraded = len(g.Reasons) > 0
		if g.Degraded {
			g.Leg = legOf(in, ing, host, p, g)
		}
		out = append(out, g)
	}
	return out
}

// rungFps is the frame rate the host should deliver: its rung's, else
// the preset's.
func rungFps(h *PeerMinuteInput) float64 {
	if h.FpsRung > 0 {
		return h.FpsRung
	}
	return h.FpsCfg
}

func atPreset(h *PeerMinuteInput) bool {
	if h == nil || !h.HasHostStats || rungFps(h) <= 0 {
		return true // nothing says the host is short
	}
	return h.EncFps >= gradeFpsAtPreset*rungFps(h)
}

func hostMachineShort(h *PeerMinuteInput) bool {
	if h == nil || !h.HasHostStats || rungFps(h) <= 0 || h.CapIdle {
		return false
	}
	floor := gradeFpsAtPreset * rungFps(h)
	return h.CapFps < floor || h.EncFps < floor || h.EncMs > 1000/rungFps(h)
}

// legOf is the rule's order: the SFU, the host uplink, the host machine,
// the viewer leg. Voice: the SFU, the sender's machine, the receiver leg.
func legOf(in MinuteInput, ing float64, host *PeerMinuteInput, p PeerMinuteInput, g PeerGrade) string {
	if in.QueueDrop > 0 || in.Rejects > 0 || p.Dropped > 0 {
		return LegSFU
	}
	if in.Type == "voice" {
		for _, o := range in.Peers {
			if o.User != p.User && o.HasVoiceStats && o.SendSkips > 0 {
				return LegSenderMachine
			}
		}
		if p.LateUnderruns > 0 || p.Reconnects > 0 {
			return LegReceiver
		}
		return LegUnknown
	}
	if ing > gradeLossPct && atPreset(host) {
		return LegHostUplink
	}
	if ing <= gradeLossPct && hostMachineShort(host) {
		return LegHostMachine
	}
	if p.Role != "host" {
		if g.LossPct > gradeLossPct || p.JitterMaxMs > gradeViewerJitterMs || p.Reconnects > 0 {
			return LegViewer
		}
	}
	return LegUnknown
}

// SessionGrade sums the graded peer minutes of a run.
type SessionGrade struct {
	Quality  string         `json:"quality"`
	Graded   int            `json:"graded_minutes"`
	Degraded int            `json:"degraded_minutes"`
	Legs     map[string]int `json:"legs"`
	// Trouble is the leg of most degraded minutes ("" when none).
	Trouble string `json:"trouble"`
}

func GradeSession(graded, degraded int, legs map[string]int) SessionGrade {
	g := SessionGrade{Graded: graded, Degraded: degraded, Legs: legs}
	switch {
	case graded == 0:
		g.Quality = QualityNoData
	case degraded == 0:
		g.Quality = QualityGood
	case float64(degraded) < gradeBadShare*float64(graded):
		g.Quality = QualityDegraded
	default:
		g.Quality = QualityBad
	}
	keys := make([]string, 0, len(legs))
	for k := range legs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	best := 0
	for _, k := range keys {
		if legs[k] > best {
			best, g.Trouble = legs[k], k
		}
	}
	return g
}
