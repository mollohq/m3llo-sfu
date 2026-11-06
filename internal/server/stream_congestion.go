package server

import (
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

const (
	streamVideoRTCPReadBuffer = 8192
	streamQueueRescueDuration = 5 * time.Second
)

// An access unit past these caps is not cached. A real IDR stays far
// below them; without the caps a host that keeps one RTP timestamp could
// grow the cache at its upload rate. A viewer that joins with no cached IDR
// asks the host for one with a PLI.
const (
	idrCacheMaxPackets = 2048
	idrCacheMaxBytes   = 2 << 20
)

type streamIdrPacketCache struct {
	mu        sync.Mutex
	currentTS uint32
	current   []*rtp.Packet
	// currentBytes is the payload size of current.
	currentBytes int
	// overflow drops the rest of the access unit at currentTS.
	overflow bool
	idr      []*rtp.Packet
}

func newStreamIdrPacketCache() *streamIdrPacketCache {
	return &streamIdrPacketCache{}
}

func (c *streamIdrPacketCache) observe(pkt *rtp.Packet) {
	if pkt == nil || pkt.PayloadType != 96 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.currentTS != pkt.Timestamp || (len(c.current) == 0 && !c.overflow) {
		c.currentTS = pkt.Timestamp
		c.current = nil
		c.currentBytes = 0
		c.overflow = false
	}
	if c.overflow {
		return
	}
	if len(c.current) >= idrCacheMaxPackets || c.currentBytes+len(pkt.Payload) > idrCacheMaxBytes {
		c.current = nil
		c.currentBytes = 0
		c.overflow = true
		return
	}
	c.current = append(c.current, pkt.Clone())
	c.currentBytes += len(pkt.Payload)

	if pkt.Marker && idrAccessUnit(c.current) {
		c.idr = append([]*rtp.Packet(nil), c.current...)
		c.current = nil
		c.currentBytes = 0
	}
}

func idrAccessUnit(packets []*rtp.Packet) bool {
	for _, pkt := range packets {
		if rtpPacketContainsH264IDR(pkt.Payload) {
			return true
		}
	}
	return false
}

func (c *streamIdrPacketCache) snapshot() []*rtp.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.idr) == 0 {
		return nil
	}
	out := make([]*rtp.Packet, len(c.idr))
	for i, pkt := range c.idr {
		out[i] = pkt.Clone()
	}
	return out
}

func (c *streamIdrPacketCache) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = nil
	c.currentBytes = 0
	c.overflow = false
	c.idr = nil
}

func rtpPacketContainsH264IDR(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	nalType := payload[0] & 0x1F
	if nalType == 5 {
		return true
	}
	if nalType == 28 || nalType == 29 {
		if len(payload) < 2 {
			return false
		}
		return payload[1]&0x1F == 5
	}
	return false
}

func (s *streamFeedbackState) removeViewer(viewerID string) {
	delete(s.lastPLI, viewerID)
	delete(s.remb, viewerID)
	delete(s.gcc, viewerID)
	delete(s.slow, viewerID)
	delete(s.twcc, viewerID)
	delete(s.constrained, viewerID)
}

// The upstream REMB policy (01-SFU.md §6.1, "Upstream REMB"). The host's
// rate follows its own leg (its goog_cc); the SFU adapts each viewer on
// that viewer's leg (§6.3: keyframes only, then PEER_SLOW). The REMB to the
// host is only a ceiling: the highest of the viewers' contributions.
//
//   - A viewer with a fresh gcc estimate contributes no limit
//     (viewerBWEMaxBitrate). Its estimate is not a ceiling: on the
//     deployed SFU it fell to 0.3 Mbps on legs that carried the stream,
//     and forwarded as the minimum it took the host from 7.9 to 1.6 Mbps
//     on the gate's mobile profile.
//   - A viewer that sends REMB and no transport-cc contributes its REMB
//     (the receiver's own estimate), at least the floor while it is
//     floored (§6.3).
//   - A viewer that went slow (§6.3) is constrained: it contributes its
//     estimate, at least the floor, until its estimate reaches the host
//     rate it could not carry (the demand) while it is not slow. So the
//     ceiling holds the host down only when every viewer is slow or
//     constrained: one viewer that can take the stream lifts it.
const (
	// streamConstrainedStale: a constrained viewer with no fresh estimate
	// for this long stops counting.
	streamConstrainedStale = 10 * time.Second
	// streamIngressRateWindow is the shortest window of the host ingress
	// rate.
	streamIngressRateWindow = 500 * time.Millisecond
)

// streamConstrained is a viewer that went slow.
type streamConstrained struct {
	// demand is the host ingress rate when the viewer went slow.
	demand float32
	// staleSince is when its estimate went stale, zero while fresh.
	staleSince time.Time
}

// noteIngress updates the host ingress rate from the byte counter.
func (s *streamFeedbackState) noteIngress(now time.Time, bytes int64) {
	if s.ingressAt.IsZero() || bytes < s.ingressBytes {
		s.ingressAt, s.ingressBytes = now, bytes
		return
	}
	dt := now.Sub(s.ingressAt)
	if dt < streamIngressRateWindow {
		return
	}
	s.ingressBps = float32(float64(bytes-s.ingressBytes) * 8 / dt.Seconds())
	s.ingressAt, s.ingressBytes = now, bytes
}

// ceilingEstimate is a viewer's estimate for the ceiling: its fresh gcc
// target, else its fresh REMB when it has no transport-cc (a libmello
// viewer's REMB is its receive target, not an estimate).
func (s *streamFeedbackState) ceilingEstimate(viewerID string, now time.Time) (float32, bool) {
	if estimate, ok := s.gcc[viewerID]; ok && now.Sub(estimate.at) <= streamREMBFreshness {
		return estimate.bitrate, true
	}
	if s.twcc[viewerID] {
		return 0, false
	}
	if estimate, ok := s.remb[viewerID]; ok && now.Sub(estimate.at) <= streamREMBFreshness {
		return estimate.bitrate, true
	}
	return 0, false
}

// upstreamCeiling is the REMB to the host now, 0 for none (no viewer has a
// contribution). Stale entries are dropped.
func (s *streamFeedbackState) upstreamCeiling(now time.Time) float32 {
	for id, estimate := range s.gcc {
		if now.Sub(estimate.at) > streamREMBFreshness {
			delete(s.gcc, id)
		}
	}
	for id, estimate := range s.remb {
		if now.Sub(estimate.at) > streamREMBFreshness {
			delete(s.remb, id)
		}
	}
	var ceiling float32
	raise := func(c float32) {
		if c > ceiling {
			ceiling = c
		}
	}
	for id := range s.constrained {
		estimate, _ := s.ceilingEstimate(id, now)
		raise(max(estimate, streamSlowViewerFloorBps))
	}
	for id := range s.gcc {
		if _, ok := s.constrained[id]; !ok {
			raise(viewerBWEMaxBitrate)
		}
	}
	for id := range s.remb {
		if _, ok := s.constrained[id]; ok {
			continue
		}
		if _, ok := s.gcc[id]; ok {
			continue
		}
		if estimate, ok := s.ceilingEstimate(id, now); ok {
			raise(s.clampSlow(id, estimate))
		}
	}
	return ceiling
}

// viewerEstimate is one viewer's contribution to the upstream REMB: its
// fresh gcc target where it has one, else its fresh REMB.
func (s *streamFeedbackState) viewerEstimate(viewerID string, now time.Time) (float32, bool) {
	if estimate, ok := s.gcc[viewerID]; ok && now.Sub(estimate.at) <= streamREMBFreshness {
		return estimate.bitrate, true
	}
	if estimate, ok := s.remb[viewerID]; ok && now.Sub(estimate.at) <= streamREMBFreshness {
		return estimate.bitrate, true
	}
	return 0, false
}

// clampSlow is a slow viewer's contribution: at least the floor.
func (s *streamFeedbackState) clampSlow(viewerID string, estimate float32) float32 {
	if s.slow[viewerID] && estimate < streamSlowViewerFloorBps {
		return streamSlowViewerFloorBps
	}
	return estimate
}

// refreshViewerEstimatesLocked copies the fresh gcc target of each viewer
// leg into the feedback state. Call it with videoMu held, before the
// minimum is taken.
func (s *StreamSession) refreshViewerEstimatesLocked(now time.Time) {
	if s.feedback.gcc == nil {
		s.feedback.gcc = make(map[string]streamREMBEstimate)
	}
	if s.feedback.slow == nil {
		s.feedback.slow = make(map[string]bool)
	}
	if s.feedback.twcc == nil {
		s.feedback.twcc = make(map[string]bool)
	}
	if s.feedback.constrained == nil {
		s.feedback.constrained = make(map[string]*streamConstrained)
	}
	for id, leg := range s.viewerVideo {
		if leg.offersTWCC() {
			s.feedback.twcc[id] = true
		} else {
			delete(s.feedback.twcc, id)
		}
		if estimate, ok := leg.peer.viewerEstimate(now); ok {
			s.feedback.gcc[id] = streamREMBEstimate{bitrate: estimate, at: now}
		} else {
			delete(s.feedback.gcc, id)
		}
		// The gcc target while its feedback is fresh, else the REMB.
		estimate, ok := s.feedback.viewerEstimate(id, now)
		leg.budget.noteEstimate(now, estimate, ok, leg.loss.sent.Load(), leg.loss.nacked.Load())
		if leg.budget.floored() {
			s.feedback.slow[id] = true
		} else {
			delete(s.feedback.slow, id)
		}
		s.updateConstrainedLocked(id, leg.budget.slow(), now)
	}
}

// updateConstrainedLocked moves a viewer into the constrained set when it
// is slow, and out when its estimate reaches the demand while it is not
// slow, or when it has had no estimate for streamConstrainedStale.
func (s *StreamSession) updateConstrainedLocked(id string, slow bool, now time.Time) {
	c := s.feedback.constrained[id]
	if c == nil {
		if !slow {
			return
		}
		c = &streamConstrained{demand: max(s.feedback.ingressBps, streamSlowViewerFloorBps)}
		s.feedback.constrained[id] = c
	}
	estimate, ok := s.feedback.ceilingEstimate(id, now)
	switch {
	case ok:
		c.staleSince = time.Time{}
		if !slow && estimate >= c.demand {
			delete(s.feedback.constrained, id)
		}
	case c.staleSince.IsZero():
		c.staleSince = now
	case now.Sub(c.staleSince) >= streamConstrainedStale:
		delete(s.feedback.constrained, id)
	}
}

// UpstreamCeiling is the bitrate that the SFU forwards to the host as REMB
// now (upstreamCeiling), 0 when there is none.
func (s *StreamSession) UpstreamCeiling(now time.Time) float32 {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	s.refreshViewerEstimatesLocked(now)
	return s.feedback.upstreamCeiling(now)
}

func (leg *streamViewerVideo) noteQueueEnqueue(now time.Time) {
	leg.queueHeadAt.CompareAndSwap(0, now.UnixNano())
}

func (leg *streamViewerVideo) noteQueueDequeue(now time.Time) {
	if len(leg.queue) == 0 {
		leg.queueHeadAt.Store(0)
		return
	}
	leg.queueHeadAt.CompareAndSwap(0, now.UnixNano())
}

func (leg *streamViewerVideo) queueRescueNeeded(now time.Time) bool {
	head := leg.queueHeadAt.Load()
	return head != 0 && now.Sub(time.Unix(0, head)) >= streamQueueRescueDuration
}

func drainViewerQueue(leg *streamViewerVideo) {
	for {
		select {
		case <-leg.queue:
		default:
			leg.queueHeadAt.Store(0)
			return
		}
	}
}

func (s *StreamSession) sendRescuePLI(now time.Time) {
	s.videoMu.Lock()
	if s.videoIngress == nil {
		s.videoMu.Unlock()
		return
	}
	ingress := s.videoIngress
	hostPC := ingress.pc
	hostGeneration := ingress.pcGeneration
	hostSSRC := uint32(ingress.track.SSRC())
	stale := !ingress.peer.isCurrentPeerConnection(hostPC, hostGeneration)
	s.videoMu.Unlock()
	if stale {
		return
	}
	_ = hostPC.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: hostSSRC},
	})
}

// streamSeqRewriter keeps the sequence numbers of the viewer legs going on
// when the host ingress changes (a host rejoin, a new host track). A new
// ingress starts at a random sequence number; a number up to 32768 below
// the last one is old to SRTP replay protection on the viewer, which drops
// the new host's packets. The first ingress keeps the host's numbers
// (offset 0); each later ingress continues after the last forwarded
// number. Guarded by StreamSession.videoMu.
type streamSeqRewriter struct {
	token  uint64
	offset uint16
	last   uint16
	have   bool
}

// rewrite returns pkt, or a copy with the rewritten sequence number. pkt
// can be shared (the snapshot tap), so it is never changed.
func (r *streamSeqRewriter) rewrite(token uint64, pkt *rtp.Packet) *rtp.Packet {
	if token != r.token {
		if r.have {
			r.offset = r.last + 1 - pkt.SequenceNumber
		}
		r.token = token
	}
	seq := pkt.SequenceNumber + r.offset
	if !r.have || int16(seq-r.last) > 0 {
		r.last = seq
		r.have = true
	}
	if r.offset == 0 {
		return pkt
	}
	out := pkt.Clone()
	out.SequenceNumber = seq
	return out
}

// requestViewerPLI sends one PLI to the host for a viewer leg: one that
// just became live, or one that went slow. It counts as that viewer's PLI
// in the 200 ms coalescing, so the viewer's own PLI for the same gap does
// not send a second one.
func (s *StreamSession) requestViewerPLI(leg *streamViewerVideo, now time.Time) {
	s.videoMu.Lock()
	if s.viewerVideo[leg.peer.UserID] != leg || s.videoIngress == nil {
		s.videoMu.Unlock()
		return
	}
	ingress := s.videoIngress
	hostPC := ingress.pc
	hostSSRC := uint32(ingress.track.SSRC())
	if !ingress.peer.isCurrentPeerConnection(hostPC, ingress.pcGeneration) {
		s.videoMu.Unlock()
		return
	}
	if s.feedback.lastPLI == nil {
		s.feedback.lastPLI = make(map[string]time.Time)
	}
	if !intervalElapsed(s.feedback.lastPLI[leg.peer.UserID], now, streamFeedbackMinInterval) {
		s.videoMu.Unlock()
		return
	}
	s.feedback.lastPLI[leg.peer.UserID] = now
	s.videoMu.Unlock()
	_ = hostPC.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: hostSSRC},
	})
}
