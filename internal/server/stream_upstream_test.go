package server

import (
	"context"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

const upstreamTestHostSSRC = 4242

// ceilingTest is a stream session with viewer legs whose gcc estimate the
// test sets, and a host that sends 8 Mbps.
type ceilingTest struct {
	t       *testing.T
	session *StreamSession
	legs    map[string]*streamViewerVideo
	seqs    map[string]*uint16
	now     time.Time
	ingress int64
}

func newCeilingTest(t *testing.T) *ceilingTest {
	return &ceilingTest{
		t:       t,
		session: NewStreamSession("ceiling", "crew", logging.New("error"), streamSnapshotConfig{}),
		legs:    make(map[string]*streamViewerVideo),
		seqs:    make(map[string]*uint16),
		now:     time.Unix(7000, 0),
	}
}

// setEstimate gives the viewer a transport-cc estimator whose target is bps.
func (c *ceilingTest) setEstimate(id string, bps int) {
	c.t.Helper()
	inner, err := gcc.NewSendSideBWE(
		gcc.SendSideBWEInitialBitrate(bps),
		gcc.SendSideBWEMinBitrate(100_000),
		gcc.SendSideBWEMaxBitrate(viewerBWEMaxBitrate),
		gcc.SendSideBWEPacer(gcc.NewNoOpPacer()),
	)
	if err != nil {
		c.t.Fatalf("gcc: %v", err)
	}
	c.t.Cleanup(func() { _ = inner.Close() })
	e := &viewerEstimator{inner: inner}
	leg := c.legs[id]
	if leg == nil {
		ctx, cancel := context.WithCancel(context.Background())
		c.t.Cleanup(cancel)
		leg = &streamViewerVideo{
			peer:      &Peer{UserID: id, Role: "viewer"},
			ctx:       ctx,
			cancel:    cancel,
			queue:     make(chan *rtp.Packet, streamVideoQueueSize),
			live:      true,
			connected: make(chan struct{}),
		}
		leg.twccOffered.Store(2)
		c.legs[id] = leg
		c.seqs[id] = new(uint16)
		c.session.viewerVideo[id] = leg
	}
	leg.peer.estimator = e
}

func (c *ceilingTest) remove(id string) {
	c.session.videoMu.Lock()
	delete(c.session.viewerVideo, id)
	c.session.feedback.removeViewer(id)
	c.session.videoMu.Unlock()
	delete(c.legs, id)
}

// second runs one second: each leg sends 700 packets with the loss of
// lossEvery[id] (0: none) and gets fresh feedback; then one stats tick.
// It returns the REMB of the tick, 0 for none.
func (c *ceilingTest) second(lossEvery map[string]int) float32 {
	c.t.Helper()
	c.now = c.now.Add(time.Second)
	c.ingress += 1_000_000 // 8 Mbps from the host
	for id, leg := range c.legs {
		legTraffic(c.session, leg, c.seqs[id], 700, lossEvery[id])
		leg.peer.estimator.lastFeedback.Store(c.now.UnixNano())
	}
	s := c.session
	s.videoMu.Lock()
	s.feedback.noteIngress(c.now, c.ingress)
	s.refreshViewerEstimatesLocked(c.now)
	packets := s.feedback.Tick(c.now, upstreamTestHostSSRC)
	s.videoMu.Unlock()
	s.SlowViewers(c.now)
	if len(packets) == 0 {
		return 0
	}
	remb, ok := packets[0].(*rtcp.ReceiverEstimatedMaximumBitrate)
	if !ok {
		c.t.Fatalf("upstream packet %T, want a REMB", packets[0])
	}
	if remb.SenderSSRC != upstreamTestHostSSRC || len(remb.SSRCs) != 1 || remb.SSRCs[0] != upstreamTestHostSSRC {
		c.t.Fatalf("REMB sender SSRC %d and SSRCs %v, want the host SSRC %d in both", remb.SenderSSRC, remb.SSRCs, upstreamTestHostSSRC)
	}
	return remb.Bitrate
}

// One impaired viewer among healthy ones does not lower the host: the REMB
// is the no-limit ceiling while the impaired viewer goes to keyframes only
// (the stage 5 rule). It was the minimum viewer estimate: the impaired
// viewer's 0.3 Mbps, then the 0.5 Mbps floor, for every viewer. When the
// healthy viewers leave, the remaining slow viewer holds the host at its
// estimate (at least the floor), and lets it go when its estimate reaches
// the rate it could not carry.
func TestUpstreamREMBUnderOneImpairedViewerAmongHealthyOnes(t *testing.T) {
	c := newCeilingTest(t)
	c.setEstimate("a", 12_000_000)
	c.setEstimate("b", 9_000_000)
	c.setEstimate("bad", 300_000)
	loss := map[string]int{"bad": 2} // 50 % on its leg
	for s := 1; s <= 6; s++ {
		if got := c.second(loss); got != viewerBWEMaxBitrate {
			t.Fatalf("second %d: REMB %.0f, want the no-limit ceiling %d", s, got, viewerBWEMaxBitrate)
		}
	}
	if !c.legs["bad"].budget.slow() {
		t.Fatal("the impaired viewer is not on keyframes only")
	}
	if c.legs["a"].budget.slow() || c.legs["b"].budget.slow() {
		t.Fatal("a healthy viewer is on keyframes only")
	}

	c.remove("a")
	c.remove("b")
	if got := c.second(loss); got != streamSlowViewerFloorBps {
		t.Fatalf("only the slow viewer left: REMB %.0f, want the floor %d", got, streamSlowViewerFloorBps)
	}
	// Its leg carries the lower rate (no loss) and its estimate climbs.
	c.setEstimate("bad", 2_000_000)
	if got := c.second(nil); got != 2_000_000 {
		t.Fatalf("REMB %.0f, want the constrained viewer's estimate 2000000", got)
	}
	c.setEstimate("bad", 8_500_000)
	var got float32
	for s := 0; s < 5; s++ {
		got = c.second(nil)
	}
	if got != viewerBWEMaxBitrate || c.legs["bad"].budget.slow() {
		t.Fatalf("REMB %.0f (slow %v) after the estimate passed the 8 Mbps demand, want the no-limit ceiling", got, c.legs["bad"].budget.slow())
	}
}

// The estimate of an unimpaired viewer fell from 10 to 18 Mbps to 0.10 Mbps
// within seconds of the host's 540p rung (handoff 15.4); forwarded as the
// minimum it took the host to 0.3 Mbps. The REMB now stays the no-limit
// ceiling, and the viewer keeps all packets (no loss on its leg).
func TestUpstreamREMBAfterAViewerSizeChange(t *testing.T) {
	c := newCeilingTest(t)
	c.setEstimate("viewer", 18_000_000)
	for s := 0; s < 5; s++ {
		c.second(nil)
	}
	for _, bps := range []int{5_800_000, 2_300_000, 1_400_000, 360_000, 100_000} {
		c.setEstimate("viewer", bps)
		if got := c.second(nil); got != viewerBWEMaxBitrate {
			t.Fatalf("estimate %d: REMB %.0f, want the no-limit ceiling %d", bps, got, viewerBWEMaxBitrate)
		}
	}
	for s := 0; s < 10; s++ {
		if got := c.second(nil); got != viewerBWEMaxBitrate {
			t.Fatalf("estimate 100000, second %d: REMB %.0f, want the no-limit ceiling", s, got)
		}
	}
	if c.legs["viewer"].budget.slow() {
		t.Fatal("a resolution change put an unimpaired viewer on keyframes only")
	}
}

// A libmello viewer sends its receive target (4 Mbps by default) as REMB at
// connect, before its transport-cc feedback gives a gcc estimate. That REMB
// is no estimate: it must not cap the host's start.
func TestUpstreamREMBIgnoresTheReceiveTargetOfATransportCCViewer(t *testing.T) {
	c := newCeilingTest(t)
	c.setEstimate("viewer", 10_000_000)
	c.legs["viewer"].peer.estimator.lastFeedback.Store(0)
	s := c.session
	s.videoMu.Lock()
	s.refreshViewerEstimatesLocked(c.now)
	packets := s.feedback.Accept("viewer", []rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 4_000_000}}, c.now, upstreamTestHostSSRC)
	s.videoMu.Unlock()
	if len(packets) != 0 {
		t.Fatalf("the receive target of a transport-cc viewer was forwarded: %v", packets)
	}
}
