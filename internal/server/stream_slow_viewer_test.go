package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

func idrStartPacket(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			SSRC:           0x1234,
		},
		// FU-A, start, IDR slice.
		Payload: []byte{0x7C, 0x85, 0x01, 0x02},
	}
}

// A viewer whose queue stays over budget is not disconnected at once: it
// gets keyframes only, and PEER_SLOW only after streamSlowViewerKick. The
// writer of this leg never runs, as for a viewer whose writes stall. Before,
// such a viewer was never helped (all packets, queue drops) and never
// removed.
func TestSlowViewerGetsKeyframesOnlyThenPEERSLOW(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	session := NewStreamSession("slow", "crew", s.Logger, streamSnapshotConfig{})
	s.sessions.GetOrCreate("slow", func() Session { return session })
	probe := newWSProbe(t)
	viewer := NewPeer("viewer", "viewer", probe.server, nil)
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leg := &streamViewerVideo{
		peer:      viewer,
		ctx:       ctx,
		cancel:    cancel,
		queue:     make(chan *rtp.Packet, streamVideoQueueSize),
		live:      true,
		connected: make(chan struct{}),
	}
	session.videoMu.Lock()
	session.viewerVideo[viewer.UserID] = leg
	session.videoMu.Unlock()

	start := time.Unix(1000, 0)
	clock := start
	session.nowFn = func() time.Time { return clock }
	const frame = 33 * time.Millisecond
	seq := uint16(0)
	step := 0
	// run sends 30 fps, one IDR access unit (two packets) per second.
	run := func(until time.Duration) {
		for ; time.Duration(step)*frame <= until; step++ {
			clock = start.Add(time.Duration(step) * frame)
			ts := uint32(step) * 3000
			if step%30 == 0 {
				session.fanOutVideoRTP(idrStartPacket(seq, ts))
				seq++
				end := deltaTestPacket(seq, ts)
				end.Payload = []byte{0x7C, 0x45, 0x03, 0x04} // FU-A end, IDR
				session.fanOutVideoRTP(end)
			} else {
				session.fanOutVideoRTP(deltaTestPacket(seq, ts))
			}
			seq++
		}
	}
	slowPeerErrors := func() int {
		n := 0
		for _, msg := range probe.messages("error") {
			var data ErrorData
			if json.Unmarshal(msg.Data, &data) == nil && data.Code == "PEER_SLOW" {
				n++
			}
		}
		return n
	}

	run(5 * time.Second)
	s.checkSlowPeers(clock)
	viewer.SendSignalingQueued("test_flush", struct{}{})
	waitFor(t, "flush", func() bool { return probe.count("test_flush") == 1 })
	if n := slowPeerErrors(); n != 0 {
		t.Fatalf("the viewer got PEER_SLOW after 5 s over budget")
	}
	if current, _ := viewer.currentSession(); current != Session(session) {
		t.Fatal("the viewer was removed after 5 s over budget")
	}
	if !leg.budget.slow() {
		t.Fatal("the viewer is not marked slow after 5 s over budget")
	}
	if len(leg.queue) == 0 {
		t.Fatal("the slow viewer's queue is empty: it gets no keyframes")
	}
	for len(leg.queue) > 0 {
		pkt := <-leg.queue
		if pkt.Timestamp%(30*3000) != 0 {
			t.Fatalf("the slow viewer got a packet of a delta frame (ts %d)", pkt.Timestamp)
		}
	}
	if session.RelaySnapshot().RTPSlowViewerDroppedPackets == 0 {
		t.Fatal("no delta packet was held back from the slow viewer")
	}

	run(streamSlowViewerKick + 2*time.Second)
	s.checkSlowPeers(clock)
	waitFor(t, "PEER_SLOW", func() bool { return slowPeerErrors() == 1 })
	if current, _ := viewer.currentSession(); current != nil {
		t.Fatal("the slow viewer was not removed after the kick time")
	}
}

// While a viewer is slow, its estimate counts as at least the floor in the
// upstream REMB: it cannot pull the shared encode under the floor. (A
// REMB viewer: its REMB is its contribution to the ceiling.)
func TestSlowViewerEstimateIsClampedToTheFloor(t *testing.T) {
	var feedback streamFeedbackState
	now := time.Unix(400, 0)
	feedback.Accept("slow", []rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 100_000}}, now, 1)
	if got := feedback.upstreamCeiling(now); got != 100_000 {
		t.Fatalf("ceiling %.0f, want 100000 while the viewer is not slow", got)
	}
	feedback.slow = map[string]bool{"slow": true}
	if got := feedback.upstreamCeiling(now); got != streamSlowViewerFloorBps {
		t.Fatalf("ceiling %.0f with a slow viewer at 100 kbps, want the floor %d", got, streamSlowViewerFloorBps)
	}
}

// The keyframe tracker marks every packet of an IDR access unit, also one
// that starts with a STAP-A of SPS and PPS, and no packet of a delta frame.
func TestKeyframeTrackerMarksIDRAccessUnits(t *testing.T) {
	var k streamKeyframeTracker
	stap := &rtp.Packet{Header: rtp.Header{Timestamp: 10}, Payload: []byte{
		0x18,                   // STAP-A
		0x00, 0x02, 0x67, 0x42, // SPS
		0x00, 0x02, 0x68, 0xCE, // PPS
	}}
	idrMiddle := &rtp.Packet{Header: rtp.Header{Timestamp: 10}, Payload: []byte{0x7C, 0x05, 0x01}}
	delta := &rtp.Packet{Header: rtp.Header{Timestamp: 20}, Payload: []byte{0x41, 0x9A}}
	if !k.isKeyframePacket(stap) || !k.isKeyframePacket(idrMiddle) {
		t.Fatal("a packet of the IDR access unit is not marked")
	}
	if k.isKeyframePacket(delta) {
		t.Fatal("a delta packet is marked as keyframe")
	}
}

// slowFloorSession is a session with one live viewer leg whose writer does
// not run; the queue stays empty because nothing is fanned out.
func slowFloorSession(t *testing.T) (*StreamSession, *streamViewerVideo) {
	t.Helper()
	session := NewStreamSession("floor", "crew", logging.New("error"), streamSnapshotConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	viewer := &Peer{UserID: "viewer", Role: "viewer"}
	leg := &streamViewerVideo{
		peer:      viewer,
		ctx:       ctx,
		cancel:    cancel,
		queue:     make(chan *rtp.Packet, streamVideoQueueSize),
		live:      true,
		connected: make(chan struct{}),
	}
	session.viewerVideo[viewer.UserID] = leg
	return session, leg
}

// legTraffic records sent packets on the leg, and NACKs from the viewer
// for every lossEvery-th of them (0: none), each NACK sent three times as
// a viewer that asks again.
func legTraffic(session *StreamSession, leg *streamViewerVideo, seq *uint16, packets, lossEvery int) {
	var lost []uint16
	for i := 0; i < packets; i++ {
		leg.loss.noteSent(*seq)
		if lossEvery > 0 && i%lossEvery == 0 {
			lost = append(lost, *seq)
		}
		*seq++
	}
	if len(lost) == 0 {
		return
	}
	nack := &rtcp.TransportLayerNack{Nacks: rtcp.NackPairsFromSequenceNumbers(lost)}
	for i := 0; i < 3; i++ {
		session.noteViewerNACKs(leg, []rtcp.Packet{nack})
	}
}

// sample feeds one REMB sample of bps at now and runs the slow check.
func sampleREMB(session *StreamSession, now time.Time, bps float32) {
	session.videoMu.Lock()
	session.feedback.Accept("viewer", []rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: bps}}, now, 1)
	session.refreshViewerEstimatesLocked(now)
	session.videoMu.Unlock()
	session.SlowViewers(now)
}

// An estimate under the floor makes the viewer slow only after it stays
// under for streamSlowViewerFloorHold (3 s) of consecutive samples. A
// 2 s dip changes nothing; a 4 s dip makes the viewer slow. Before, one
// sample under the floor put the viewer on keyframes only. The leg loses
// half of its packets here: without loss no dip makes the viewer slow
// (TestViewerOnALossyLegThatCarriesTheStreamIsNeverSlow).
func TestSlowViewerNeedsASustainedDipUnderTheFloor(t *testing.T) {
	start := time.Unix(2000, 0)
	var seq uint16

	session, leg := slowFloorSession(t)
	for s := 0; s <= 2; s++ {
		legTraffic(session, leg, &seq, 200, 2)
		sampleREMB(session, start.Add(time.Duration(s)*time.Second), 300_000)
		if leg.budget.slow() {
			t.Fatalf("the viewer is slow after a %d s dip", s)
		}
	}
	sampleREMB(session, start.Add(3*time.Second), 2_000_000)
	if leg.budget.slow() {
		t.Fatal("a 2 s dip made the viewer slow")
	}

	session, leg = slowFloorSession(t)
	for s := 0; s <= 4; s++ {
		legTraffic(session, leg, &seq, 200, 2)
		sampleREMB(session, start.Add(time.Duration(s)*time.Second), 300_000)
	}
	if !leg.budget.slow() {
		t.Fatal("a 4 s dip did not make the viewer slow")
	}
	// One sample over the floor ends the dip: the viewer leaves the slow
	// state only after streamSlowViewerRecovery within budget.
	sampleREMB(session, start.Add(5*time.Second), 2_000_000)
	if !leg.budget.slow() {
		t.Fatal("the viewer left the slow state at once")
	}
	sampleREMB(session, start.Add(5*time.Second+streamSlowViewerRecovery), 2_000_000)
	if leg.budget.slow() {
		t.Fatal("the viewer is still slow after the recovery time within budget")
	}
}

// A slow viewer's gcc estimate goes stale when its feedback stops (on
// keyframes only it gets few packets). It then counts with its REMB, as a
// viewer without transport-cc does, so a stale low gcc target cannot keep
// the viewer slow.
func TestStaleGCCEstimateFallsBackToTheREMB(t *testing.T) {
	var feedback streamFeedbackState
	now := time.Unix(3000, 0)
	feedback.gcc = map[string]streamREMBEstimate{
		"viewer": {bitrate: 200_000, at: now.Add(-streamREMBFreshness - time.Millisecond)},
	}
	feedback.remb = map[string]streamREMBEstimate{
		"viewer": {bitrate: 2_000_000, at: now},
	}
	got, ok := feedback.viewerEstimate("viewer", now)
	if !ok || got != 2_000_000 {
		t.Fatalf("estimate %.0f (fresh %v), want the REMB 2000000", got, ok)
	}
}

// A viewer whose leg carries the stream with 2 to 5 % loss (the gate's
// home-wifi and mobile profiles) is never put on keyframes only, however
// low its estimate: on the deployed SFU the gcc estimate of such a leg fell
// to 0.2 Mbps in 12 s and the viewer froze 7 s in 20 s on keyframes only.
// A NACK for a packet that the leg did not send (a host-leg gap) is no loss
// on the leg. With heavy loss on the leg (40 %), the same estimate makes
// the viewer slow after the hold.
func TestViewerOnALossyLegThatCarriesTheStreamIsNeverSlow(t *testing.T) {
	start := time.Unix(5000, 0)
	// No loss (the estimate after a resolution rung, handoff 15.4: 18 to
	// 0.10 Mbps on an unimpaired leg), 2 % and 5 %.
	for _, lossEvery := range []int{0, 50, 20} {
		session, leg := slowFloorSession(t)
		var seq uint16
		for step := 0; step <= 200; step++ { // 20 s, a sample every 100 ms
			legTraffic(session, leg, &seq, 85, lossEvery)
			// The viewer also NACKs a host-leg gap: never sent on the leg.
			gap := &rtcp.TransportLayerNack{Nacks: rtcp.NackPairsFromSequenceNumbers([]uint16{seq + 100, seq + 101})}
			session.noteViewerNACKs(leg, []rtcp.Packet{gap})
			sampleREMB(session, start.Add(time.Duration(step)*100*time.Millisecond), 200_000)
			if leg.budget.slow() {
				t.Fatalf("1/%d loss: the viewer is on keyframes only after %d ms", lossEvery, step*100)
			}
		}
		if got := session.RelaySnapshot().RTPViewerNACKedPackets; lossEvery > 0 && got == 0 {
			t.Fatalf("1/%d loss: no NACKed packet counted", lossEvery)
		}
		// The low estimate still counts as the floor upstream.
		if got := session.UpstreamCeiling(start.Add(20 * time.Second)); got != streamSlowViewerFloorBps {
			t.Fatalf("1/%d loss: upstream ceiling %.0f, want the floor %d", lossEvery, got, streamSlowViewerFloorBps)
		}
	}

	session, leg := slowFloorSession(t)
	var seq uint16
	slowAt := -1
	for step := 0; step <= 100 && slowAt < 0; step++ {
		legTraffic(session, leg, &seq, 85, 2) // 50 %
		sampleREMB(session, start.Add(time.Duration(step)*100*time.Millisecond), 200_000)
		if leg.budget.slow() {
			slowAt = step * 100
		}
	}
	if slowAt < int(streamSlowViewerFloorHold/time.Millisecond) || slowAt > 4000 {
		t.Fatalf("with 50 %% loss and the estimate under the floor the viewer went slow at %d ms, want 3000 to 4000", slowAt)
	}
}

// Each NACKed packet counts once, and only when the leg sent it.
func TestLegLossCountsEachSentPacketOnce(t *testing.T) {
	var loss streamLegLoss
	for seq := uint16(65530); seq != 10; seq++ {
		loss.noteSent(seq)
	}
	for i := 0; i < 3; i++ {
		loss.noteNACK(65534)
		loss.noteNACK(2)
	}
	loss.noteNACK(20)                     // never sent
	loss.noteNACK(2 + streamLegSeqWindow) // same slot, another number
	if got := loss.nacked.Load(); got != 2 {
		t.Fatalf("nacked %d, want 2", got)
	}
	if got := loss.sent.Load(); got != 16 {
		t.Fatalf("sent %d, want 16", got)
	}
}
