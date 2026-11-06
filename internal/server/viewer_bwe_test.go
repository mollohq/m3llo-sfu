package server

import (
	"testing"
	"time"

	"github.com/pion/interceptor"
	pionlogging "github.com/pion/logging"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/transport/v5/vnet"
	"github.com/pion/webrtc/v4"
)

// newTWCCClientAPI is a client that sends transport-cc feedback for what
// it receives, as a browser or a fixed libmello viewer does.
func newTWCCClientAPI(t *testing.T, se webrtc.SettingEngine) *webrtc.API {
	t.Helper()
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("register Opus: %v", err)
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: streamVideoCodecCapability(),
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register H.264: %v", err)
	}
	registry := &interceptor.Registry{}
	if err := webrtc.ConfigureTWCCSender(me, registry); err != nil {
		t.Fatalf("TWCC sender: %v", err)
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(registry), webrtc.WithSettingEngine(se))
}

// cappedLink is a virtual network: SFU, host and viewer each on an
// address. Packets to the viewer go through a token bucket at capBps.
type cappedLink struct {
	sfu, host, viewer *vnet.Net
}

func newCappedLink(t *testing.T, capBps int) *cappedLink {
	t.Helper()
	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.8.0.0/24",
		LoggerFactory: pionlogging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("vnet router: %v", err)
	}
	link := &cappedLink{}
	for _, n := range []struct {
		ip  string
		net **vnet.Net
	}{{"10.8.0.1", &link.sfu}, {"10.8.0.2", &link.host}, {"10.8.0.3", &link.viewer}} {
		*n.net, err = vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{n.ip}})
		if err != nil {
			t.Fatalf("vnet net: %v", err)
		}
	}
	if err := router.AddNet(link.sfu); err != nil {
		t.Fatalf("add SFU net: %v", err)
	}
	if err := router.AddNet(link.host); err != nil {
		t.Fatalf("add host net: %v", err)
	}
	// 1.5 KB burst, 60 KB queue: about 480 ms of queue at 1 Mbps.
	shaped, err := vnet.NewQueue(link.viewer, vnet.NewTBFQueue(capBps, 1500, 60_000))
	if err != nil {
		t.Fatalf("token bucket: %v", err)
	}
	if err := router.AddNet(shaped); err != nil {
		t.Fatalf("add viewer net: %v", err)
	}
	if err := router.Start(); err != nil {
		t.Fatalf("start vnet: %v", err)
	}
	t.Cleanup(func() { _ = router.Stop() })
	return link
}

// newBWETestServer is a test server with the production viewer API (a gcc
// estimator per viewer PeerConnection) on se.
func newBWETestServer(t *testing.T, se webrtc.SettingEngine) *Server {
	t.Helper()
	api, err := newSFUAPI(se)
	if err != nil {
		t.Fatalf("SFU API: %v", err)
	}
	s := newStreamTestServer(api)
	s.viewerBWE = newViewerBWERegistry()
	s.viewerAPI, err = newSFUViewerAPI(se, s.viewerBWE)
	if err != nil {
		t.Fatalf("SFU viewer API: %v", err)
	}
	return s
}

// wireHostIngress sends host packets until the SFU wired the ingress.
func wireHostIngress(t *testing.T, host *streamTestHost, session *StreamSession) {
	t.Helper()
	seq := uint16(100)
	waitFor(t, "the host ingress is wired", func() bool {
		_ = host.track.WriteRTP(deltaTestPacket(seq, 9000))
		seq++
		session.videoMu.Lock()
		defer session.videoMu.Unlock()
		return session.videoIngress != nil
	})
}

func drainViewerVideo(v *streamTestViewer) {
	go func() {
		for range v.video {
		}
	}()
}

// bigDeltaPacket is a non-IDR packet with a 1200-byte payload.
func bigDeltaPacket(seq uint16, ts uint32) *rtp.Packet {
	payload := make([]byte, 1200)
	payload[0] = 0x41
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			SSRC:           0x1234,
			Marker:         true,
		},
		Payload: payload,
	}
}

// framePackets returns the packets of one frame of n 1200-byte packets.
// An IDR frame carries FU-A fragments of an IDR slice, so the SFU treats it
// as a keyframe (a slow viewer gets it); a delta frame carries non-IDR
// slices.
func framePackets(seq *uint16, ts uint32, n int, idr bool) []*rtp.Packet {
	out := make([]*rtp.Packet, 0, n)
	for i := 0; i < n; i++ {
		pkt := bigDeltaPacket(*seq, ts)
		pkt.Marker = i == n-1
		if idr {
			fu := byte(0x05)
			if i == 0 {
				fu |= 0x80
			}
			if i == n-1 {
				fu |= 0x40
			}
			pkt.Payload[0], pkt.Payload[1] = 0x7C, fu
		}
		out = append(out, pkt)
		*seq++
	}
	return out
}

// runFollowingSender sends like a host for run: 100 frames per second at
// the viewer's gcc target (2.9 Mbps until it is fresh), with the bit credit
// from the real elapsed time (at most 20 ms of it), an IDR every 2 s and one
// after each PLI the SFU sends. It returns the mean viewer target over the
// last 4 s and whether the estimate was ever fresh. (It followed the
// forwarded REMB while that was the minimum viewer estimate; the REMB is
// now a ceiling that a viewer's gcc estimate does not set, 01-SFU.md
// section 6.1.)
func runFollowingSender(t *testing.T, session *StreamSession, viewer *streamTestViewer, host *streamTestHost, run time.Duration) (float64, bool) {
	t.Helper()
	start := time.Now()
	last := start
	seq := uint16(1000)
	ts := uint32(0)
	credit := 0.0
	nextTick := start
	nextIDR := start
	plis := host.plis.Load()
	var sum float64
	var samples int
	fresh := false
	for time.Since(start) < run {
		now := time.Now()
		rate := 2_900_000.0
		if target, ok := viewer.peer.viewerEstimate(now); ok {
			rate = float64(target)
		}
		credit += rate * now.Sub(last).Seconds()
		// A host's encoder makes frames in real time: after a stall of the
		// test loop (a busy machine) it does not send the missed frames in
		// one burst, which overflowed the link's 60 KB queue and read as
		// loss. At most 20 ms of credit.
		credit = min(credit, rate*0.02)
		last = now
		idr := false
		if now.After(nextIDR) || host.plis.Load() != plis {
			idr = true
			plis = host.plis.Load()
			nextIDR = now.Add(2 * time.Second)
		}
		if n := int(credit / 9600); n > 0 {
			for _, pkt := range framePackets(&seq, ts, n, idr) {
				session.fanOutVideoRTP(pkt)
			}
			credit -= float64(n) * 9600
		} else if idr {
			nextIDR = now // no bits for the IDR yet: send it in the next frame
		}
		ts += 900
		time.Sleep(10 * time.Millisecond)
		now = time.Now()
		if now.After(nextTick) {
			session.tickFeedback(now)
			nextTick = now.Add(200 * time.Millisecond)
		}
		target, ok := viewer.peer.viewerEstimate(now)
		fresh = fresh || ok
		if ok && now.Sub(start) > run-4*time.Second {
			sum += float64(target)
			samples++
		}
	}
	if samples == 0 {
		return 0, fresh
	}
	return sum / float64(samples), fresh
}

// The gcc estimator in the viewer PeerConnection's interceptor chain sees
// what the SFU sends and the viewer's transport-cc feedback. The test
// sends like a host (runFollowingSender). On a link capped at 1 Mbps the
// mean viewer target of the last 4 s is 0.5 to 1.3 times the cap; on a
// 20 Mbps link it is at least 3 times the capped-link mean and at least
// 2.5 Mbps. Before, the viewer leg had no estimator: no target, and the
// host got no REMB for a viewer that sends none.
func TestViewerEstimateFollowsACappedLink(t *testing.T) {
	const run = 10 * time.Second
	follow := func(name string, capBps int) float64 {
		t.Helper()
		link := newCappedLink(t, capBps)
		s := newBWETestServer(t, vnetSettings(link.sfu))
		viewer := offerStreamTestViewer(t, s, "bwe-"+name, newTWCCClientAPI(t, vnetSettings(link.viewer)))
		drainViewerVideo(viewer)
		viewer.connect(t)
		host := connectStreamTestHost(t, s, "bwe-"+name, "host", newTestAPIWithSettings(vnetSettings(link.host)))
		wireHostIngress(t, host, viewer.session)
		mean, fresh := runFollowingSender(t, viewer.session, viewer, host, run)
		if !fresh || mean == 0 {
			t.Fatalf("%s: the viewer estimator got no transport-cc feedback", name)
		}
		t.Logf("%s: link %d bps, mean viewer target %.0f bps over the last 4 s, host REMB %d bps",
			name, capBps, mean, host.remb.Load())
		return mean
	}

	const capBps = 1_000_000
	capped := follow("capped-1mbps", capBps)
	if capped < 0.5*capBps || capped > 1.3*capBps {
		t.Fatalf("mean viewer target %.0f bps on a %d bps link, want %.0f to %.0f",
			capped, capBps, 0.5*capBps, 1.3*capBps)
	}
	fast := follow("fast-20mbps", 20_000_000)
	if fast < 3*capped || fast < 2_500_000 {
		t.Fatalf("mean viewer target %.0f bps on a 20 Mbps link, want at least 3 x %.0f (the capped link) and at least 2500000",
			fast, capped)
	}
}

// A viewer without transport-cc (no header extension in its description,
// no feedback) has no gcc estimate: the SFU forwards its REMB, the
// receiver's own estimate. The client API has no interceptors: Pion's
// defaults (newTestAPI) negotiate transport-cc and send feedback.
func TestViewerWithoutTWCCForwardsItsREMB(t *testing.T) {
	s := newBWETestServer(t, webrtc.SettingEngine{})
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: streamVideoCodecCapability(),
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register H.264: %v", err)
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		PayloadType:        111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatalf("register Opus: %v", err)
	}
	rembOnly := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(&interceptor.Registry{}))
	viewer := offerStreamTestViewer(t, s, "bwe-remb", rembOnly)
	drainViewerVideo(viewer)
	viewer.connect(t)
	host := connectStreamTestHost(t, s, "bwe-remb", "host", newTestAPI())
	session := viewer.session
	wireHostIngress(t, host, session)
	ssrc := uint32(viewer.leg(t).sender.GetParameters().Encodings[0].SSRC)

	const remb = 700_000
	seq := uint16(1000)
	waitFor(t, "the host gets the viewer REMB", func() bool {
		for i := 0; i < 3; i++ {
			session.fanOutVideoRTP(bigDeltaPacket(seq, uint32(seq)*300))
			seq++
		}
		_ = viewer.pc.WriteRTCP([]rtcp.Packet{&rtcp.ReceiverEstimatedMaximumBitrate{
			Bitrate: remb,
			SSRCs:   []uint32{ssrc},
		}})
		time.Sleep(50 * time.Millisecond)
		return host.remb.Load() == remb
	})
	if _, ok := viewer.peer.viewerEstimate(time.Now()); ok {
		t.Fatal("a viewer without transport-cc feedback has a gcc estimate")
	}
	if got := session.UpstreamCeiling(time.Now()); got != remb {
		t.Fatalf("upstream ceiling %.0f, want the REMB %d", got, remb)
	}
}

// The feedback counters: entries, entries reported lost, and entries for
// packets older than Pion's 250-packet feedback history (overrun), across
// the sequence number wrap.
func TestViewerEstimatorCountsFeedbackOverrun(t *testing.T) {
	var e viewerEstimator
	e.sentSeq.Store(uint32(uint16(40)) | 1<<16) // newest sent: 40, after the wrap
	fb := &rtcp.TransportLayerCC{
		BaseSequenceNumber: 65536 - 260 + 41, // 260 packets before the newest
		PacketStatusCount:  20,
		RecvDeltas:         make([]*rtcp.RecvDelta, 15),
	}
	e.noteFeedback(fb)
	d := e.diagSinceLast()
	if d.Feedbacks != 1 || d.Acked != 15 || d.ReportedLost != 5 {
		t.Fatalf("diag %+v, want 1 feedback, 15 acked, 5 lost", d)
	}
	if d.MaxLag != 260 || d.Overrun != 10 {
		t.Fatalf("lag %d overrun %d, want 260 and 10", d.MaxLag, d.Overrun)
	}
	if d := e.diagSinceLast(); d.Feedbacks != 0 || d.MaxLag != 0 {
		t.Fatalf("second diag %+v, want zero", d)
	}
}

// A NACK repair goes out with its original transport-wide number: the
// newest sent number does not move back (it bounds the feedback the
// estimator keeps, viewer_bwe_feedback.go).
func TestViewerEstimatorNewestSentIgnoresRepairs(t *testing.T) {
	var e viewerEstimator
	for _, seq := range []uint16{65530, 65535, 3, 65533, 2} {
		e.noteSent(seq)
	}
	if got := uint16(e.sentSeq.Load()); got != 3 {
		t.Fatalf("newest sent %d, want 3", got)
	}
}
