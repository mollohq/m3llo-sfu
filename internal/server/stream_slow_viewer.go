package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
)

// Slow stream viewer: lower first, kick last (01-SFU.md §6.3).
//
// A viewer is over budget while its RTP queue stays not empty for
// streamViewerQueueBudget (the writer does not keep up), or while its fresh
// estimate has stayed under streamSlowViewerFloorBps for
// streamSlowViewerFloorHold of consecutive samples (one dip changes
// nothing) and, in the same time, the viewer NACKed at least
// streamSlowViewerLossShare of the packets sent on its leg. The estimate
// alone is not evidence: on the deployed SFU the gcc estimate of a leg
// that carried the stream fell under the floor with no loss (and with 2 to
// 5 % loss) and put the viewer on keyframes only. The estimate is the gcc
// target while transport-cc feedback is fresh, else the REMB: a viewer on
// keyframes only gets little feedback, and its gcc estimate goes stale
// instead of holding it slow. While it is over budget:
//   - its estimate counts as at least the floor in the upstream REMB, so
//     one slow viewer cannot pull the shared encode under the floor (also
//     after an estimate under the floor for streamSlowViewerFloorHold
//     without loss: then the leg keeps all packets);
//   - its queue takes only the packets of IDR access units (keyframes
//     only), and the backlog is dropped once with one PLI for a fresh IDR.
//
// It goes back to all packets after streamSlowViewerRecovery within budget.
// A viewer that stays slow for streamSlowViewerKick gets PEER_SLOW and is
// removed. Before, the slowest viewer pulled the encode down through the
// min-REMB, and a viewer with a stuck queue was never helped or removed.
const (
	streamViewerQueueBudget  = 1 * time.Second
	streamSlowViewerFloorBps = 500_000
	// streamSlowViewerFloorHold: an estimate under the floor counts only
	// when every sample for this long was under the floor.
	streamSlowViewerFloorHold = 3 * time.Second
	streamSlowViewerRecovery  = 3 * time.Second
	streamSlowViewerKick      = 15 * time.Second
	// streamSlowViewerLossShare: an estimate under the floor counts only
	// while the viewer NACKed at least this share of the packets sent on
	// its leg in the last streamSlowViewerFloorHold. A leg that carries
	// the stream loses less (the gate's bad profile: 10 % with bursts); a
	// link under about 80 % of the stream rate loses more.
	streamSlowViewerLossShare = 0.20
	// streamSlowViewerLossMinPackets: with fewer packets sent in the
	// window there is no loss evidence.
	streamSlowViewerLossMinPackets = 50
	// streamLegSeqWindow is the number of sequence numbers a viewer leg
	// keeps to match a NACK to a packet sent on the leg (about 2 s at the
	// 1080p60 preset).
	streamLegSeqWindow = 2048
)

// streamLegLoss counts the packets sent on a viewer video leg and those of
// them that the viewer NACKed, each sequence number once. A NACK for a
// packet that the leg did not send (a gap from the host leg, a delta that a
// slow viewer's queue did not take) does not count: that is no loss on
// this leg.
type streamLegLoss struct {
	sentSeq   [streamLegSeqWindow]atomic.Uint32
	nackedSeq [streamLegSeqWindow]atomic.Uint32
	sent      atomic.Int64
	nacked    atomic.Int64
}

// legSeqMark is the slot value of a sequence number: never 0, so an empty
// slot matches no sequence number.
func legSeqMark(seq uint16) uint32 { return uint32(seq) | 1<<16 }

func (l *streamLegLoss) noteSent(seq uint16) {
	l.sentSeq[seq%streamLegSeqWindow].Store(legSeqMark(seq))
	l.sent.Add(1)
}

// noteNACK counts a NACKed sequence number and reports whether it counted:
// sent on the leg and not NACKed before.
func (l *streamLegLoss) noteNACK(seq uint16) bool {
	slot := seq % streamLegSeqWindow
	mark := legSeqMark(seq)
	if l.sentSeq[slot].Load() != mark {
		return false
	}
	if l.nackedSeq[slot].Swap(mark) == mark {
		return false
	}
	l.nacked.Add(1)
	return true
}

// streamLegLossSample is the leg's counters at one estimate sample.
type streamLegLossSample struct {
	at     time.Time
	sent   int64
	nacked int64
}

// streamViewerBudget is the slow state of one viewer video leg.
type streamViewerBudget struct {
	mu sync.Mutex
	// slowSince is when the leg went over budget, zero when it is not slow.
	slowSince time.Time
	// withinSince is when a slow leg came back within budget.
	withinSince time.Time
	// rescue is set when the leg goes slow: the fan-out drops the backlog
	// and asks for an IDR once.
	rescue bool
	// underSince is the first of the consecutive estimate samples under
	// the floor, zero after a sample at or over it, or with no estimate.
	underSince time.Time
	// lowEstimate: the samples have been under the floor for
	// streamSlowViewerFloorHold. The estimate then counts as the floor in
	// the upstream REMB, but the leg is not slow without loss.
	lowEstimate bool
	// underFloor: lowEstimate, with heavy loss on the leg.
	underFloor bool
	// lossSamples are the leg counters at the samples of the last
	// streamSlowViewerFloorHold, and the last one before it.
	lossSamples []streamLegLossSample
}

// update moves the slow state to now. over reports whether the leg is over
// budget now.
func (b *streamViewerBudget) update(now time.Time, over bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	over = over || b.underFloor
	if over {
		b.withinSince = time.Time{}
		if b.slowSince.IsZero() {
			b.slowSince = now
			b.rescue = true
		}
		return
	}
	if b.slowSince.IsZero() {
		return
	}
	if b.withinSince.IsZero() {
		b.withinSince = now
	}
	if now.Sub(b.withinSince) >= streamSlowViewerRecovery {
		b.slowSince = time.Time{}
		b.withinSince = time.Time{}
		b.rescue = false
	}
}

// noteEstimate records one sample of the viewer's estimate (ok false: no
// fresh estimate, which is no evidence of a slow link) and the leg's
// counters of sent and NACKed packets.
func (b *streamViewerBudget) noteEstimate(now time.Time, estimate float32, ok bool, sent, nacked int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	heavyLoss := b.noteLossLocked(now, sent, nacked)
	if !ok || estimate >= streamSlowViewerFloorBps {
		b.underSince = time.Time{}
		b.lowEstimate = false
		b.underFloor = false
		return
	}
	if b.underSince.IsZero() {
		b.underSince = now
	}
	b.lowEstimate = now.Sub(b.underSince) >= streamSlowViewerFloorHold
	b.underFloor = b.lowEstimate && heavyLoss
}

// floored reports whether the leg's estimate counts as at least the floor
// in the upstream REMB: the leg is slow, or its estimate has been under the
// floor for streamSlowViewerFloorHold.
func (b *streamViewerBudget) floored() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.slowSince.IsZero() || b.lowEstimate
}

// noteLossLocked adds a sample of the leg counters and reports whether the
// viewer NACKed at least streamSlowViewerLossShare of the packets sent in
// the last streamSlowViewerFloorHold.
func (b *streamViewerBudget) noteLossLocked(now time.Time, sent, nacked int64) bool {
	b.lossSamples = append(b.lossSamples, streamLegLossSample{at: now, sent: sent, nacked: nacked})
	edge := now.Add(-streamSlowViewerFloorHold)
	for len(b.lossSamples) > 1 && !b.lossSamples[1].at.After(edge) {
		b.lossSamples = b.lossSamples[1:]
	}
	base := b.lossSamples[0]
	if base.at.After(edge) {
		return false
	}
	window := sent - base.sent
	if window < streamSlowViewerLossMinPackets {
		return false
	}
	return float64(nacked-base.nacked) >= streamSlowViewerLossShare*float64(window)
}

// slow reports whether the leg is slow now (keyframes only, clamped
// estimate).
func (b *streamViewerBudget) slow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.slowSince.IsZero()
}

// slowFor reports how long the leg has been slow, 0 when it is not.
func (b *streamViewerBudget) slowFor(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.slowSince.IsZero() {
		return 0
	}
	return now.Sub(b.slowSince)
}

// takeRescue reports, once per slow episode, that the backlog must go.
func (b *streamViewerBudget) takeRescue() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	r := b.rescue
	b.rescue = false
	return r
}

// queueOverBudget reports whether the queue has not been empty for
// streamViewerQueueBudget.
func (leg *streamViewerVideo) queueOverBudget(now time.Time) bool {
	head := leg.queueHeadAt.Load()
	return head != 0 && now.Sub(time.Unix(0, head)) >= streamViewerQueueBudget
}

// streamKeyframeTracker marks the packets of IDR access units: an access
// unit is a keyframe when one of its packets carries an IDR slice, an SPS
// or a PPS. Guarded by StreamSession.videoMu.
type streamKeyframeTracker struct {
	keyTS   uint32
	haveKey bool
}

func (k *streamKeyframeTracker) isKeyframePacket(pkt *rtp.Packet) bool {
	if rtpPacketStartsKeyframe(pkt.Payload) {
		k.keyTS = pkt.Timestamp
		k.haveKey = true
		return true
	}
	return k.haveKey && pkt.Timestamp == k.keyTS
}

// rtpPacketStartsKeyframe reports whether an H.264 RTP payload carries an
// IDR slice (5), an SPS (7) or a PPS (8): alone, in a FU-A fragment, or in
// a STAP-A.
func rtpPacketStartsKeyframe(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	key := func(nalType byte) bool { return nalType == 5 || nalType == 7 || nalType == 8 }
	switch nalType := payload[0] & 0x1F; nalType {
	case 28, 29:
		return len(payload) >= 2 && key(payload[1]&0x1F)
	case 24:
		for i := 1; i+2 < len(payload); {
			size := int(payload[i])<<8 | int(payload[i+1])
			if size == 0 || i+2+size > len(payload) {
				return false
			}
			if key(payload[i+2] & 0x1F) {
				return true
			}
			i += 2 + size
		}
		return false
	default:
		return key(nalType)
	}
}

// SlowViewers returns the viewers whose leg has been slow for
// streamSlowViewerKick: the slow-peer checker removes them with PEER_SLOW.
func (s *StreamSession) SlowViewers(now time.Time) []*Peer {
	s.videoMu.Lock()
	legs := make([]*streamViewerVideo, 0, len(s.viewerVideo))
	for _, leg := range s.viewerVideo {
		legs = append(legs, leg)
	}
	s.videoMu.Unlock()
	var slow []*Peer
	for _, leg := range legs {
		leg.budget.update(now, leg.queueOverBudget(now))
		if leg.budget.slowFor(now) >= streamSlowViewerKick {
			slow = append(slow, leg.peer)
		}
	}
	return slow
}
