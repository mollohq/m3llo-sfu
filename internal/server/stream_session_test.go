package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// A gap in host ingress must be reported as loss, so a collapse in rtp_in_pps can
// be attributed to the uplink rather than to the host having stopped encoding.
func TestStreamSequenceWindowReportsIngressLoss(t *testing.T) {
	var window streamSequenceWindow
	for _, seq := range []uint16{100, 101, 102} {
		if !window.Accept(seq) {
			t.Fatalf("sequence %d was rejected", seq)
		}
	}
	if got := window.Lost(); got != 0 {
		t.Fatalf("contiguous run reported %d lost, want 0", got)
	}

	// 103..107 never arrive; 108 does.
	if !window.Accept(108) {
		t.Fatal("sequence 108 was rejected")
	}
	if got, want := window.Expected(), int64(9); got != want {
		t.Fatalf("expected=%d, want %d", got, want)
	}
	if got, want := window.Lost(), int64(5); got != want {
		t.Fatalf("lost=%d, want %d", got, want)
	}

	// A straggler must reclaim its loss rather than leaving it counted forever.
	if !window.Accept(105) {
		t.Fatal("reordered sequence 105 was rejected")
	}
	if got, want := window.Lost(), int64(4); got != want {
		t.Fatalf("after reorder lost=%d, want %d", got, want)
	}

	// Duplicates must not inflate received and drive loss negative.
	if window.Accept(105) {
		t.Fatal("duplicate sequence 105 was accepted")
	}
	if got, want := window.Lost(), int64(4); got != want {
		t.Fatalf("after duplicate lost=%d, want %d", got, want)
	}
}

// Loss accounting must survive the 16-bit sequence rollover, or every wrap would
// register as ~65k lost packets and make the metric useless on long streams.
func TestStreamSequenceWindowLossAcrossWraparound(t *testing.T) {
	var window streamSequenceWindow
	if !window.Accept(65534) || !window.Accept(65535) || !window.Accept(0) || !window.Accept(1) {
		t.Fatal("wraparound sequence was rejected")
	}
	if got, want := window.Expected(), int64(4); got != want {
		t.Fatalf("expected=%d, want %d", got, want)
	}
	if got := window.Lost(); got != 0 {
		t.Fatalf("contiguous wraparound reported %d lost, want 0", got)
	}
}

func TestStreamSequenceWindowForwardsLateRepairOnce(t *testing.T) {
	var window streamSequenceWindow

	if !window.Accept(101) {
		t.Fatal("sequence 101 was rejected")
	}
	if !window.Accept(100) {
		t.Fatal("late repaired sequence 100 was rejected")
	}
	if window.Accept(100) {
		t.Fatal("duplicate repaired sequence 100 was accepted")
	}
	if window.Accept(101) {
		t.Fatal("duplicate sequence 101 was accepted")
	}
}

func TestStreamSequenceWindowWraparound(t *testing.T) {
	var window streamSequenceWindow
	for _, sequence := range []uint16{65534, 65535, 0, 1} {
		if !window.Accept(sequence) {
			t.Fatalf("wraparound sequence %d was rejected", sequence)
		}
	}
	if window.Accept(65535) {
		t.Fatal("duplicate sequence before wrap was accepted")
	}
	if !window.Accept(65533) {
		t.Fatal("late sequence before wrap was rejected")
	}
	if window.Accept(65533) {
		t.Fatal("duplicate late sequence before wrap was accepted")
	}
}

func TestStreamSequenceWindowRejectsPacketsOutsideBoundedHistory(t *testing.T) {
	var window streamSequenceWindow
	if !window.Accept(100) || !window.Accept(100+streamSequenceWindowSize) {
		t.Fatal("failed to advance sequence window")
	}
	if window.Accept(100) {
		t.Fatal("packet outside bounded history was accepted")
	}
}

func TestStreamFeedbackCoalescesPLIAndFIR(t *testing.T) {
	var feedback streamFeedbackState
	now := time.Unix(100, 0)
	hostSSRC := uint32(1234)

	upstream := feedback.Accept("viewer-a", []rtcp.Packet{
		&rtcp.PictureLossIndication{},
		&rtcp.FullIntraRequest{},
		&rtcp.TransportLayerNack{},
	}, now, hostSSRC)
	if len(upstream) != 1 {
		t.Fatalf("first feedback produced %d packets, want one PLI", len(upstream))
	}
	pli, ok := upstream[0].(*rtcp.PictureLossIndication)
	if !ok || pli.MediaSSRC != hostSSRC {
		t.Fatalf("first feedback = %#v, want upstream PLI for SSRC %d", upstream[0], hostSSRC)
	}

	// Per-viewer coalescing: viewer-b's first keyframe request is independent
	// of viewer-a's recent PLI and must not be swallowed by it.
	upstream = feedback.Accept("viewer-b", []rtcp.Packet{
		&rtcp.FullIntraRequest{},
		&rtcp.TransportLayerNack{},
	}, now.Add(streamFeedbackMinInterval-time.Millisecond), hostSSRC)
	if len(upstream) != 1 {
		t.Fatalf("first feedback from a second viewer produced %d packets, want one PLI", len(upstream))
	}

	// The same viewer's repeat inside its own window is coalesced.
	upstream = feedback.Accept("viewer-b", []rtcp.Packet{
		&rtcp.FullIntraRequest{},
	}, now.Add(streamFeedbackMinInterval), hostSSRC)
	if len(upstream) != 0 {
		t.Fatalf("repeat feedback within the viewer's window produced %d packets, want none", len(upstream))
	}

	// Past the window the same viewer's FIR goes through (downgraded to PLI).
	upstream = feedback.Accept("viewer-b", []rtcp.Packet{
		&rtcp.FullIntraRequest{},
	}, now.Add(2*streamFeedbackMinInterval), hostSSRC)
	if len(upstream) != 1 {
		t.Fatalf("feedback after coalescing window produced %d packets, want one PLI", len(upstream))
	}
	if _, ok := upstream[0].(*rtcp.PictureLossIndication); !ok {
		t.Fatalf("FIR was forwarded as %T, want PLI", upstream[0])
	}
}

// REMB viewers (no transport-cc): the ceiling is the highest fresh REMB
// (01-SFU.md section 6.1, upstream REMB), forwarded at most once per
// streamFeedbackMinInterval. It was the minimum.
func TestStreamFeedbackForwardsTheHighestFreshREMB(t *testing.T) {
	var feedback streamFeedbackState
	now := time.Unix(200, 0)
	hostSSRC := uint32(5678)

	assertREMB := func(packets []rtcp.Packet, want float32) {
		t.Helper()
		if len(packets) != 1 {
			t.Fatalf("got %d upstream packets, want one REMB", len(packets))
		}
		remb, ok := packets[0].(*rtcp.ReceiverEstimatedMaximumBitrate)
		if !ok || remb.Bitrate != want {
			t.Fatalf("upstream feedback = %#v, want REMB bitrate %.0f", packets[0], want)
		}
		if len(remb.SSRCs) != 1 || remb.SSRCs[0] != hostSSRC {
			t.Fatalf("upstream REMB SSRCs = %v, want [%d]", remb.SSRCs, hostSSRC)
		}
	}

	assertREMB(feedback.Accept("slow", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 400_000},
	}, now, hostSSRC), 400_000)

	if packets := feedback.Accept("fast", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 900_000},
	}, now.Add(streamFeedbackMinInterval/2), hostSSRC); len(packets) != 0 {
		t.Fatalf("rate-limited REMB produced %d packets, want none", len(packets))
	}

	assertREMB(feedback.Accept("fast", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 900_000},
	}, now.Add(streamFeedbackMinInterval), hostSSRC), 900_000)

	assertREMB(feedback.Accept("slow", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 400_000},
	}, now.Add(streamREMBFreshness+2*streamFeedbackMinInterval), hostSSRC), 400_000)
}

func TestStreamFeedbackTickRecomputesWithoutFreshREMB(t *testing.T) {
	var feedback streamFeedbackState
	now := time.Unix(300, 0)
	hostSSRC := uint32(42)

	assertREMB := func(packets []rtcp.Packet, want float32) {
		t.Helper()
		if len(packets) != 1 {
			t.Fatalf("got %d upstream packets, want one REMB", len(packets))
		}
		remb, ok := packets[0].(*rtcp.ReceiverEstimatedMaximumBitrate)
		if !ok || remb.Bitrate != want {
			t.Fatalf("upstream feedback = %#v, want REMB bitrate %.0f", packets[0], want)
		}
		// libdatachannel routes an incoming RTCP packet to a track by the
		// sender and media source SSRCs of its header. A REMB's media source
		// is 0, so with sender 0 the host's video track never got it.
		if remb.SenderSSRC != hostSSRC {
			t.Fatalf("REMB sender SSRC %d, want the host SSRC %d", remb.SenderSSRC, hostSSRC)
		}
	}

	feedback.Accept("fast", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 800_000},
	}, now, hostSSRC)
	feedback.Accept("slow", []rtcp.Packet{
		&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 300_000},
	}, now.Add(1500*time.Millisecond), hostSSRC)

	// Inside the forward window the tick emits nothing.
	if packets := feedback.Tick(now.Add(1500*time.Millisecond+streamFeedbackMinInterval/2), hostSSRC); len(packets) != 0 {
		t.Fatalf("tick inside forward window produced %d packets, want none", len(packets))
	}

	// Past the window the ceiling is re-forwarded without any fresh REMB.
	assertREMB(feedback.Tick(now.Add(1800*time.Millisecond), hostSSRC), 800_000)

	// The viewer that sets the ceiling goes stale while staying silent: the
	// new ceiling must surface on the tick, not wait for someone to send
	// REMB. (fast is 2.4 s old -> pruned; slow is 0.9 s old -> fresh.)
	assertREMB(feedback.Tick(now.Add(2400*time.Millisecond), hostSSRC), 300_000)
}

func TestStreamVideoFanoutDoesNotBlockHealthyViewer(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	slowContext, cancelSlow := context.WithCancel(context.Background())
	defer cancelSlow()
	healthyContext, cancelHealthy := context.WithCancel(context.Background())
	defer cancelHealthy()

	slow := &streamViewerVideo{
		peer:  &Peer{},
		ctx:   slowContext,
		queue: make(chan *rtp.Packet, 1),
		live:  true,
	}
	healthy := &streamViewerVideo{
		peer:  &Peer{},
		ctx:   healthyContext,
		queue: make(chan *rtp.Packet, 1),
		live:  true,
	}
	slow.queue <- &rtp.Packet{}
	session.viewerVideo["slow"] = slow
	session.viewerVideo["healthy"] = healthy

	packet := &rtp.Packet{Header: rtp.Header{
		Marker:         true,
		SequenceNumber: 44,
		Timestamp:      998877,
	}}
	session.fanOutVideoRTP(packet)

	if got := slow.peer.PacketsDropped.Load(); got != 1 {
		t.Fatalf("slow viewer drops = %d, want 1", got)
	}
	select {
	case forwarded := <-healthy.queue:
		if forwarded == packet ||
			forwarded.SequenceNumber != packet.SequenceNumber ||
			forwarded.Timestamp != packet.Timestamp ||
			forwarded.Marker != packet.Marker {
			t.Fatal("healthy viewer did not receive a preserved packet clone")
		}
	default:
		t.Fatal("healthy viewer was blocked by full slow-viewer queue")
	}
}

func TestStreamViewerVideoCancellationAndReplacement(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})

	first, cleanupFirst := newPeerWithOfferedStreamVideo(t)
	defer cleanupFirst()
	first.UserID = "viewer"
	first.Role = "viewer"
	first.bindSession(session)
	session.viewers.Add(first)
	session.WireViewerVideo(first)

	session.videoMu.Lock()
	firstLeg := session.viewerVideo[first.UserID]
	session.videoMu.Unlock()
	if firstLeg == nil {
		t.Fatal("first viewer video leg was not installed")
	}

	session.stopViewerVideo(first)
	select {
	case <-firstLeg.ctx.Done():
	default:
		t.Fatal("removed viewer video leg was not cancelled")
	}

	second, cleanupSecond := newPeerWithOfferedStreamVideo(t)
	defer cleanupSecond()
	second.UserID = first.UserID
	second.Role = "viewer"
	second.bindSession(session)
	session.viewers.Remove(first.UserID)
	session.viewers.Add(second)
	session.WireViewerVideo(second)

	session.videoMu.Lock()
	secondLeg := session.viewerVideo[second.UserID]
	session.videoMu.Unlock()
	if secondLeg == nil || secondLeg == firstLeg || secondLeg.peer != second {
		t.Fatal("replacement viewer did not own a fresh video leg")
	}
	session.stopViewerVideo(second)
}

func TestStaleHostTeardownDoesNotDrainReplacement(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	oldHost := &Peer{UserID: "host", Role: "host"}
	oldHost.closeOnce.Do(func() {})
	replacement := &Peer{UserID: "host", Role: "host"}
	viewer := &Peer{UserID: "viewer", Role: "viewer"}
	// The viewer gets member_joined for the host, so it needs a WS.
	viewer.WS = newWSProbe(t).server
	viewer.bindSession(session)

	if err := session.AddPeer(oldHost); err != nil {
		t.Fatalf("add old host: %v", err)
	}
	session.viewers.Add(viewer)
	if err := session.AddPeer(replacement); err != nil {
		t.Fatalf("replace host: %v", err)
	}

	session.RemovePeerInstance(oldHost)

	if session.State() != SessionActive {
		t.Fatalf("state = %s, want active", session.State())
	}
	if session.DrainingAt() != (time.Time{}) {
		t.Fatalf("drainingAt = %v, want zero", session.DrainingAt())
	}
	session.videoMu.Lock()
	current := session.host
	session.videoMu.Unlock()
	replacementSession, _ := replacement.currentSession()
	if current != replacement || replacementSession != session {
		t.Fatal("stale host teardown removed the replacement")
	}
}

func TestStreamFinalDrainCancelsViewerVideoLeg(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	viewer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()
	viewer.UserID = "viewer"
	viewer.Role = "viewer"
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	session.WireViewerVideo(viewer)

	session.videoMu.Lock()
	leg := session.viewerVideo[viewer.UserID]
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()
	if leg == nil {
		t.Fatal("viewer video leg was not installed")
	}

	if !session.FinalizeDrain(time.Now(), DrainTimeout) {
		t.Fatal("expired stream drain did not finalize")
	}
	select {
	case <-leg.ctx.Done():
	default:
		t.Fatal("final drain did not cancel viewer RTP leg")
	}
	if sender, lifecycle := viewer.StreamVideoSender(); sender != nil || lifecycle != nil {
		t.Fatal("final drain retained the viewer RTP sender")
	}
	if currentSession, _ := viewer.currentSession(); currentSession != nil {
		t.Fatal("final drain retained viewer session back-reference")
	}
	if session.MemberCount() != 0 {
		t.Fatal("final drain retained viewer membership")
	}
}

func TestStaleViewerWriterCannotUseReplacementTrack(t *testing.T) {
	firstTrack, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "first")
	if err != nil {
		t.Fatalf("create first track: %v", err)
	}
	replacementTrack, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "replacement")
	if err != nil {
		t.Fatalf("create replacement track: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer := &Peer{
		streamVideo: &streamVideoLeg{
			track:  replacementTrack,
			ctx:    ctx,
			cancel: cancel,
		},
	}
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	staleLeg := &streamViewerVideo{peer: peer, track: firstTrack}
	packet := &rtp.Packet{Header: rtp.Header{SequenceNumber: 9}, Payload: []byte{1, 2, 3}}

	if err := session.sendViewerVideoRTP(staleLeg, packet); !errors.Is(err, ErrStaleStreamVideoTrack) {
		t.Fatalf("stale writer error = %v, want %v", err, ErrStaleStreamVideoTrack)
	}
	snapshot := session.RelaySnapshot()
	if snapshot.RTPForwardedPackets != 0 || snapshot.RTPDroppedPackets != 1 {
		t.Fatalf("stale writer counters = forwarded %d dropped %d, want 0/1",
			snapshot.RTPForwardedPackets, snapshot.RTPDroppedPackets)
	}
}

func TestHostReplacementDoesNotInvalidateViewerActivation(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	viewer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()
	viewer.UserID = "viewer"
	viewer.Role = "viewer"
	// The viewer gets member_joined for the host, so it needs a WS.
	viewer.WS = newWSProbe(t).server
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}

	pc, pcGeneration := viewer.currentPeerConnection()
	track, generation, _, err := session.PrepareViewerVideo(viewer, pc, pcGeneration)
	if err != nil {
		t.Fatalf("prepare viewer video: %v", err)
	}

	firstHost := &Peer{UserID: "host", Role: "host"}
	firstHost.closeOnce.Do(func() {})
	if err := session.AddPeer(firstHost); err != nil {
		t.Fatalf("add first host: %v", err)
	}
	replacementHost := &Peer{UserID: "host", Role: "host"}
	replacementHost.closeOnce.Do(func() {})
	if err := session.AddPeer(replacementHost); err != nil {
		t.Fatalf("replace host: %v", err)
	}

	if !session.ActivateViewerVideo(viewer, track, generation) {
		t.Fatal("viewer activation was invalidated by unrelated host replacement")
	}
	session.stopViewerVideo(viewer)
}

func TestFinalDrainClosesOnlyCapturedViewerTrack(t *testing.T) {
	oldTrack, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "old")
	if err != nil {
		t.Fatalf("create old track: %v", err)
	}
	newTrack, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "new")
	if err != nil {
		t.Fatalf("create new track: %v", err)
	}

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	oldCtx, oldCancel := context.WithCancel(context.Background())
	viewer := &Peer{UserID: "viewer", Role: "viewer"}
	viewer.streamVideo = &streamVideoLeg{track: oldTrack, ctx: oldCtx, cancel: oldCancel}
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}

	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()

	teardown, ok := session.PrepareFinalizeDrain(time.Now(), DrainTimeout)
	if !ok {
		t.Fatal("expected finalize drain to be ready")
	}

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	viewer.streamVideo = &streamVideoLeg{track: newTrack, ctx: newCtx, cancel: newCancel}
	viewer.bindSession(NewStreamSession("stream-2", "crew", logging.New("error"), streamSnapshotConfig{}))

	teardown()

	select {
	case <-oldCtx.Done():
	default:
		t.Fatal("final drain did not close captured old track lifecycle")
	}
	if track := viewer.StreamVideoTrack(); track != newTrack {
		t.Fatal("final drain closed a replacement sender negotiated for a new session")
	}
	select {
	case <-newCtx.Done():
		t.Fatal("final drain cancelled replacement track lifecycle")
	default:
	}
}

func TestViewerRTCPIgnoresStaleIngressPeerConnection(t *testing.T) {
	api := newTestAPI()
	hostPC, remoteTrack, cleanupHost := newHostIngressTrackPair(t, api)
	defer cleanupHost()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	host := &Peer{UserID: "host", Role: "host"}
	host.bindSession(session)
	oldGeneration := host.installPeerConnection(hostPC)
	session.host = host
	session.WireVideoTrackFor(host, remoteTrack, hostPC, oldGeneration)

	newPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create replacement host PC: %v", err)
	}
	defer newPC.Close()
	host.installPeerConnection(newPC)

	viewer, cleanupViewer := newPeerWithOfferedStreamVideo(t)
	defer cleanupViewer()
	viewer.UserID = "viewer"
	viewer.Role = "viewer"
	viewer.bindSession(session)
	session.viewers.Add(viewer)
	sender, _ := viewer.StreamVideoSender()
	leg := &streamViewerVideo{
		peer:   viewer,
		track:  viewer.StreamVideoTrack(),
		sender: sender,
	}
	session.viewerVideo[viewer.UserID] = leg

	session.handleViewerVideoRTCP(leg, []rtcp.Packet{
		&rtcp.PictureLossIndication{},
	}, time.Unix(100, 0))
	if len(session.feedback.lastPLI) != 0 {
		t.Fatal("stale ingress RTCP updated feedback state after host PC replacement")
	}
}

func newHostIngressTrackPair(t *testing.T, api *webrtc.API) (*webrtc.PeerConnection, *webrtc.TrackRemote, func()) {
	t.Helper()

	hostPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		hostPC.Close()
		t.Fatalf("create server PC: %v", err)
	}
	hostTrack, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "host")
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create host track: %v", err)
	}
	if _, err := hostPC.AddTransceiverFromTrack(
		hostTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("add host track: %v", err)
	}

	offer, err := hostPC.CreateOffer(nil)
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create host offer: %v", err)
	}
	offerGathered := webrtc.GatheringCompletePromise(hostPC)
	if err := hostPC.SetLocalDescription(offer); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set host local description: %v", err)
	}
	<-offerGathered
	if err := serverPC.SetRemoteDescription(*hostPC.LocalDescription()); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set server remote description: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create server answer: %v", err)
	}
	answerGathered := webrtc.GatheringCompletePromise(serverPC)
	if err := serverPC.SetLocalDescription(answer); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set server local description: %v", err)
	}
	<-answerGathered
	if err := hostPC.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set host remote description: %v", err)
	}

	var remoteTrack *webrtc.TrackRemote
	for _, receiver := range serverPC.GetReceivers() {
		if track := receiver.Track(); track != nil && track.Kind() == webrtc.RTPCodecTypeVideo {
			remoteTrack = track
			break
		}
	}
	if remoteTrack == nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatal("server did not expose a remote host video track")
	}

	return serverPC, remoteTrack, func() {
		hostPC.Close()
		serverPC.Close()
	}
}

func TestRTPRelaySnapshotCounters(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	viewer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow := &streamViewerVideo{
		peer:  &Peer{},
		ctx:   ctx,
		queue: make(chan *rtp.Packet, 1),
		live:  true,
	}
	slow.queue <- &rtp.Packet{}
	session.viewerVideo["slow"] = slow

	packet := &rtp.Packet{Header: rtp.Header{SequenceNumber: 55}, Payload: []byte{1, 2, 3, 4}}
	packetSize := int64(packet.MarshalSize())
	session.rtpInPackets.Add(1)
	session.rtpInBytes.Add(packetSize)
	session.rtpDuplicatePackets.Add(1)
	session.rtpDroppedPackets.Add(1)
	session.rtpDroppedBytes.Add(packetSize)
	session.fanOutVideoRTP(packet)
	if err := session.sendViewerVideoRTP(&streamViewerVideo{
		peer:  viewer,
		track: viewer.StreamVideoTrack(),
	}, packet); err != nil {
		t.Fatalf("record successful RTP forward: %v", err)
	}

	snapshot := session.RelaySnapshot()
	if snapshot.RTPInPackets != 1 ||
		snapshot.RTPInBytes != packetSize ||
		snapshot.RTPFanoutAttempts != 1 ||
		snapshot.RTPDroppedPackets != 2 ||
		snapshot.RTPDroppedBytes != 2*packetSize ||
		snapshot.RTPQueueDroppedPackets != 1 ||
		snapshot.RTPQueueDroppedBytes != packetSize ||
		snapshot.RTPDuplicatePackets != 1 ||
		snapshot.RTPForwardedPackets != 1 ||
		snapshot.RTPForwardedBytes != packetSize {
		t.Fatalf("unexpected RTP relay snapshot: %+v", snapshot)
	}
	if snapshot.MediaInPackets != 0 || snapshot.MediaDroppedPackets != 0 {
		t.Fatal("RTP activity leaked into DataChannel media counters")
	}
}

func TestStreamSequenceWindowLateRepairedSequence(t *testing.T) {
	var window streamSequenceWindow
	if !window.Accept(101) {
		t.Fatal("expected seq 101 accepted")
	}
	if !window.Accept(100) {
		t.Fatal("expected late repaired seq 100 accepted")
	}
	if window.Accept(100) {
		t.Fatal("expected duplicate seq 100 rejected")
	}
}

func TestStreamSequenceWindowDuplicateSuppression(t *testing.T) {
	var window streamSequenceWindow
	if !window.Accept(50) {
		t.Fatal("expected first seq accepted")
	}
	if window.Accept(50) {
		t.Fatal("expected duplicate seq rejected")
	}
}

func TestViewerNegotiationStateRemainsBoundedUnderChurn(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	for i := 0; i < 1000; i++ {
		viewer := &Peer{UserID: fmt.Sprintf("viewer-%d", i), Role: "viewer"}
		viewer.closeOnce.Do(func() {})
		if err := session.AddPeer(viewer); err != nil {
			t.Fatalf("add viewer %d: %v", i, err)
		}
		session.RemovePeerInstance(viewer)
	}

	if got := session.MemberCount(); got != 0 {
		t.Fatalf("members after churn = %d, want 0", got)
	}
	session.videoMu.Lock()
	viewerLegs := len(session.viewerVideo)
	rembEntries := len(session.feedback.remb)
	session.videoMu.Unlock()
	if viewerLegs != 0 || rembEntries != 0 {
		t.Fatalf("retained viewer state after churn: legs=%d remb=%d", viewerLegs, rembEntries)
	}

	oldViewer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()
	oldViewer.UserID = "reused"
	oldViewer.Role = "viewer"
	oldViewer.closeOnce.Do(func() {})
	if err := session.AddPeer(oldViewer); err != nil {
		t.Fatalf("add old viewer: %v", err)
	}
	pc, pcGeneration := oldViewer.currentPeerConnection()
	track, token, _, err := session.PrepareViewerVideo(oldViewer, pc, pcGeneration)
	if err != nil {
		t.Fatalf("prepare old viewer: %v", err)
	}
	session.RemovePeerInstance(oldViewer)

	replacement := &Peer{UserID: oldViewer.UserID, Role: "viewer"}
	replacement.closeOnce.Do(func() {})
	if err := session.AddPeer(replacement); err != nil {
		t.Fatalf("add replacement viewer: %v", err)
	}
	if session.ActivateViewerVideo(oldViewer, track, token) {
		t.Fatal("stale peer-scoped negotiation token activated after viewer-ID reuse")
	}
}

func newHostIngressAudioTrackPair(
	t *testing.T,
	api *webrtc.API,
) (*webrtc.PeerConnection, *webrtc.TrackRemote, func()) {
	t.Helper()

	hostPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		hostPC.Close()
		t.Fatalf("create server PC: %v", err)
	}
	hostTrack, err := webrtc.NewTrackLocalStaticRTP(
		streamAudioCodecCapability(),
		"audio",
		"host",
	)
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create host audio track: %v", err)
	}
	if _, err := hostPC.AddTransceiverFromTrack(
		hostTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("add host audio track: %v", err)
	}

	offer, err := hostPC.CreateOffer(nil)
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create host offer: %v", err)
	}
	offerGathered := webrtc.GatheringCompletePromise(hostPC)
	if err := hostPC.SetLocalDescription(offer); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set host local description: %v", err)
	}
	<-offerGathered
	if err := serverPC.SetRemoteDescription(*hostPC.LocalDescription()); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set server remote description: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("create server answer: %v", err)
	}
	answerGathered := webrtc.GatheringCompletePromise(serverPC)
	if err := serverPC.SetLocalDescription(answer); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set server local description: %v", err)
	}
	<-answerGathered
	if err := hostPC.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("set host remote description: %v", err)
	}

	connected := make(chan struct{})
	hostPC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}
	})
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		hostPC.Close()
		serverPC.Close()
		t.Fatal("host/server peer connection did not connect")
	}

	if err := hostTrack.WriteRTP(&rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    111,
			SequenceNumber: 1,
			Timestamp:      960,
		},
		Payload: []byte{0xf8, 0xff, 0xfe},
	}); err != nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatalf("write host audio RTP: %v", err)
	}

	var remoteTrack *webrtc.TrackRemote
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, receiver := range serverPC.GetReceivers() {
			if track := receiver.Track(); track != nil &&
				track.Kind() == webrtc.RTPCodecTypeAudio &&
				strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
				remoteTrack = track
				break
			}
		}
		if remoteTrack != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if remoteTrack == nil {
		hostPC.Close()
		serverPC.Close()
		t.Fatal("server did not expose a remote host audio track")
	}

	return serverPC, remoteTrack, func() {
		hostPC.Close()
		serverPC.Close()
	}
}

// streamJoinView is what one stream peer learned: the user ids in its
// joined.members, how many member_joined and member_left it got for each
// user and with which role, and how many session_ended it got.
type streamJoinView struct {
	listed           map[string]bool
	memberJoined     map[string]int
	joinedRole       map[string]string
	memberLeft       map[string]int
	leftRole         map[string]string
	sessionEnded     int
	hostReconnecting int
}

// takeStreamJoinView waits until every message sent to peer before this call
// has reached probe, reads what peer learned and empties probe.
func takeStreamJoinView(t *testing.T, peer *Peer, probe *wsProbe) streamJoinView {
	t.Helper()
	// It goes out after every write queued for peer before it, and returns
	// when it is written: the writer's earlier writes happen before what
	// the test does next (a later round reuses the probe's connection with
	// a new Peer).
	peer.SendSignaling("test_flush", struct{}{})
	deadline := time.Now().Add(2 * time.Second)
	for len(probe.messages("test_flush")) == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: flush message never arrived", peer.UserID)
		}
		time.Sleep(time.Millisecond)
	}
	view := streamJoinView{
		listed:       map[string]bool{},
		memberJoined: map[string]int{},
		joinedRole:   map[string]string{},
		memberLeft:   map[string]int{},
		leftRole:     map[string]string{},
	}
	for _, msg := range probe.messages("joined") {
		var data JoinedData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.Fatalf("joined data: %v", err)
		}
		for _, m := range data.Members {
			view.listed[m.UserID] = true
		}
	}
	for _, msg := range probe.messages("member_joined") {
		var data MemberEventData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.Fatalf("member_joined data: %v", err)
		}
		view.memberJoined[data.UserID]++
		view.joinedRole[data.UserID] = data.Role
	}
	for _, msg := range probe.messages("member_left") {
		var data MemberEventData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.Fatalf("member_left data: %v", err)
		}
		view.memberLeft[data.UserID]++
		view.leftRole[data.UserID] = data.Role
	}
	view.sessionEnded = len(probe.messages("session_ended"))
	view.hostReconnecting = len(probe.messages("host_reconnecting"))
	probe.mu.Lock()
	probe.types, probe.msgs = nil, nil
	probe.mu.Unlock()
	return view
}

// streamJoinRace joins users[i] with roles[i] to a new stream session per
// round, all at the same moment, and returns what each peer learned.
type streamJoinRace struct {
	s      *Server
	users  []string
	roles  []string
	probes []*wsProbe
}

func newStreamJoinRace(t *testing.T, users, roles []string) *streamJoinRace {
	logger := logging.New("error")
	r := &streamJoinRace{
		s: &Server{
			Logger:   logger,
			metrics:  metrics.New(),
			sessions: NewSessionStore(logger),
		},
		users: users,
		roles: roles,
	}
	for range users {
		r.probes = append(r.probes, newWSProbe(t))
	}
	return r
}

func (r *streamJoinRace) round(t *testing.T, sessionID string) []streamJoinView {
	t.Helper()
	join := make([][]byte, len(r.users))
	peers := make([]*Peer, len(r.users))
	for i, user := range r.users {
		join[i], _ = json.Marshal(JoinStreamData{SessionID: sessionID, Role: r.roles[i]})
		peers[i] = NewPeer(user, r.roles[i], r.probes[i].server, r.s.Logger)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range r.users {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r.s.handleJoinStream(peers[i], &auth.Claims{UserID: r.users[i], Type: "stream"},
				SignalingMessage{Type: "join_stream", Data: join[i]})
		}(i)
	}
	close(start)
	wg.Wait()
	views := make([]streamJoinView, len(r.users))
	for i := range r.users {
		views[i] = takeStreamJoinView(t, peers[i], r.probes[i])
	}
	return views
}

// Of two viewers that join at the same moment, the one that joins second
// lists the first in joined.members, and the first gets member_joined with
// role viewer for the second: each learns of the other exactly once
// (01-SFU.md §4.1).
func TestSimultaneousStreamViewerJoinsSeeEachOther(t *testing.T) {
	var users, roles []string
	for i := 0; i < 8; i++ {
		users = append(users, fmt.Sprintf("v%d", i))
		roles = append(roles, "viewer")
	}
	race := newStreamJoinRace(t, users, roles)
	const rounds = 200
	missed := 0
	for round := 0; round < rounds; round++ {
		views := race.round(t, fmt.Sprintf("viewers-%d", round))
		for i := range users {
			for j := i + 1; j < len(users); j++ {
				listed := 0
				if views[i].listed[users[j]] {
					listed++
				}
				if views[j].listed[users[i]] {
					listed++
				}
				if listed != 1 {
					missed++
					t.Errorf("round %d: %d of %s and %s list the other, want 1",
						round, listed, users[i], users[j])
				}
				for _, pair := range [][2]int{{i, j}, {j, i}} {
					a, b := pair[0], pair[1]
					if !learnedOnce(views[a], users[b], "viewer") {
						missed++
						t.Errorf("round %d: %s learned of %s %d times (listed %v, member_joined role %q), want once",
							round, users[a], users[b], timesLearned(views[a], users[b]),
							views[a].listed[users[b]], views[a].joinedRole[users[b]])
					}
				}
			}
		}
	}
	if missed > 0 {
		t.Fatalf("%d viewer pairs did not learn of each other exactly once", missed)
	}
}

// timesLearned counts how often view learned of user: in joined.members and
// by member_joined.
func timesLearned(view streamJoinView, user string) int {
	n := view.memberJoined[user]
	if view.listed[user] {
		n++
	}
	return n
}

// learnedOnce reports whether view learned of user exactly once, with role
// when it was by member_joined.
func learnedOnce(view streamJoinView, user, role string) bool {
	if timesLearned(view, user) != 1 {
		return false
	}
	return view.memberJoined[user] == 0 || view.joinedRole[user] == role
}

// A host and viewers that join at the same moment: every member learns of
// every other member exactly once (joined.members, or member_joined with the
// role), and of the host and a viewer, the one that joins second lists the
// other in joined.members.
func TestSimultaneousStreamHostAndViewerJoinsSeeEachOther(t *testing.T) {
	users, roles := []string{"host"}, []string{"host"}
	for i := 0; i < 7; i++ {
		users = append(users, fmt.Sprintf("v%d", i))
		roles = append(roles, "viewer")
	}
	race := newStreamJoinRace(t, users, roles)
	const rounds = 200
	missed := 0
	for round := 0; round < rounds; round++ {
		views := race.round(t, fmt.Sprintf("host-viewers-%d", round))
		host := views[0]
		for i := 1; i < len(users); i++ {
			viewer := users[i]
			hostLearned := host.memberJoined[viewer]
			if host.listed[viewer] {
				hostLearned++
			}
			if hostLearned != 1 || host.listed[viewer] == views[i].listed["host"] {
				missed++
				t.Errorf("round %d: host learned of %s %d times (listed %v), %s lists host %v",
					round, viewer, hostLearned, host.listed[viewer], viewer, views[i].listed["host"])
			}
			if !learnedOnce(host, viewer, "viewer") {
				missed++
				t.Errorf("round %d: host member_joined role for %s is %q, want viewer",
					round, viewer, host.joinedRole[viewer])
			}
			if !learnedOnce(views[i], "host", "host") {
				missed++
				t.Errorf("round %d: %s learned of host %d times (listed %v, member_joined role %q), want once",
					round, viewer, timesLearned(views[i], "host"), views[i].listed["host"], views[i].joinedRole["host"])
			}
			for j := 1; j < len(users); j++ {
				if j != i && !learnedOnce(views[i], users[j], "viewer") {
					missed++
					t.Errorf("round %d: %s learned of %s %d times, want once",
						round, viewer, users[j], timesLearned(views[i], users[j]))
				}
			}
		}
	}
	if missed > 0 {
		t.Fatalf("%d host-viewer pairs did not see each other", missed)
	}
}

// joinStreamInOrder joins users[i] with roles[i] to sessionID one after the
// other and returns the peers and their probes.
func joinStreamInOrder(t *testing.T, s *Server, sessionID string, users, roles []string) ([]*Peer, []*wsProbe) {
	t.Helper()
	peers := make([]*Peer, len(users))
	probes := make([]*wsProbe, len(users))
	for i, user := range users {
		probes[i] = newWSProbe(t)
		peers[i] = NewPeer(user, roles[i], probes[i].server, s.Logger)
		join, _ := json.Marshal(JoinStreamData{SessionID: sessionID, Role: roles[i]})
		s.handleJoinStream(peers[i], &auth.Claims{UserID: user, Type: "stream"},
			SignalingMessage{Type: "join_stream", Data: join})
		if len(probes[i].messages("error")) > 0 {
			t.Fatalf("%s did not join %s", user, sessionID)
		}
	}
	return peers, probes
}

// A viewer that joins before the host learns of the host by member_joined
// with role host, and of a later viewer with role viewer.
func TestStreamViewerJoinedFirstLearnsOfLaterMembers(t *testing.T) {
	race := newStreamJoinRace(t, nil, nil)
	users := []string{"v0", "v1", "host"}
	peers, probes := joinStreamInOrder(t, race.s, "viewer-first", users, []string{"viewer", "viewer", "host"})

	v0 := takeStreamJoinView(t, peers[0], probes[0])
	if v0.memberJoined["v1"] != 1 || v0.joinedRole["v1"] != "viewer" {
		t.Errorf("v0: member_joined for v1 %d times, role %q; want once, viewer",
			v0.memberJoined["v1"], v0.joinedRole["v1"])
	}
	if v0.memberJoined["host"] != 1 || v0.joinedRole["host"] != "host" {
		t.Errorf("v0: member_joined for host %d times, role %q; want once, host",
			v0.memberJoined["host"], v0.joinedRole["host"])
	}
	v1 := takeStreamJoinView(t, peers[1], probes[1])
	if !v1.listed["v0"] || v1.memberJoined["v0"] != 0 {
		t.Errorf("v1: want v0 in joined.members only, got listed %v, member_joined %d",
			v1.listed["v0"], v1.memberJoined["v0"])
	}
	if v1.memberJoined["host"] != 1 || v1.joinedRole["host"] != "host" {
		t.Errorf("v1: member_joined for host %d times, role %q; want once, host",
			v1.memberJoined["host"], v1.joinedRole["host"])
	}
	host := takeStreamJoinView(t, peers[2], probes[2])
	for _, viewer := range []string{"v0", "v1"} {
		if !host.listed[viewer] || host.memberJoined[viewer] != 0 {
			t.Errorf("host: want %s in joined.members only, got listed %v, member_joined %d",
				viewer, host.listed[viewer], host.memberJoined[viewer])
		}
	}
}

// A viewer that leaves is announced with member_left (role viewer) to the
// host and the other viewers. A host that leaves sends host_reconnecting
// and no member_left; the viewers get session_ended when the drain closes
// the session without a host (01-SFU.md §4.1).
func TestStreamLeaveEvents(t *testing.T) {
	race := newStreamJoinRace(t, nil, nil)
	users := []string{"host", "v0", "v1"}
	peers, probes := joinStreamInOrder(t, race.s, "leave-events", users, []string{"host", "viewer", "viewer"})
	sess, ok := race.s.sessions.Get("leave-events")
	if !ok {
		t.Fatal("session missing")
	}
	stream := sess.(*StreamSession)
	for i := range peers {
		takeStreamJoinView(t, peers[i], probes[i])
	}

	stream.RemovePeerInstance(peers[2])
	for _, i := range []int{0, 1} {
		view := takeStreamJoinView(t, peers[i], probes[i])
		if view.memberLeft["v1"] != 1 || view.leftRole["v1"] != "viewer" {
			t.Errorf("%s: member_left for v1 %d times, role %q; want once, viewer",
				users[i], view.memberLeft["v1"], view.leftRole["v1"])
		}
	}

	stream.RemovePeerInstance(peers[0])
	v0 := takeStreamJoinView(t, peers[1], probes[1])
	if v0.sessionEnded != 0 {
		t.Errorf("v0: session_ended %d times during the hold, want none", v0.sessionEnded)
	}
	if v0.hostReconnecting != 1 {
		t.Errorf("v0: host_reconnecting %d times, want once", v0.hostReconnecting)
	}
	if v0.memberLeft["host"] != 0 {
		t.Errorf("v0: member_left for host %d times, want none", v0.memberLeft["host"])
	}
	if !stream.FinalizeDrain(time.Now().Add(DrainTimeout+time.Second), DrainTimeout) {
		t.Fatal("the drain did not close the held session")
	}
	v0 = takeStreamJoinView(t, peers[1], probes[1])
	if v0.sessionEnded != 1 {
		t.Errorf("v0: session_ended %d times after the hold, want once", v0.sessionEnded)
	}
}

// A host's audio track can be wired before its video track: the order of
// OnTrack events is not fixed. Wiring the video track must not cancel the
// audio ingress (01-SFU.md Â§6.1).
func TestStreamAudioIngressSurvivesLaterVideoWiring(t *testing.T) {
	api := newTestAPI()
	pair := newHostIngressAVTrackPair(t, api)
	defer pair.cleanup()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	host := &Peer{UserID: "host", Role: "host"}
	host.bindSession(session)
	generation := host.installPeerConnection(pair.serverPC)
	session.host = host

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	viewerAudio := &streamViewerAudio{
		peer:   &Peer{UserID: "viewer", Role: "viewer"},
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan *rtp.Packet, streamAudioQueueSize),
	}
	session.videoMu.Lock()
	session.viewerAudio["viewer"] = viewerAudio
	session.videoMu.Unlock()

	var seq uint16
	sendAudioUntilViewerGetsIt := func(stage string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			seq++
			if err := pair.hostAudio.WriteRTP(&rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					PayloadType:    111,
					SequenceNumber: seq,
					Timestamp:      uint32(seq) * 960,
				},
				Payload: []byte{0xfc, 0xff, 0xfe},
			}); err != nil {
				t.Fatalf("%s: write host audio RTP: %v", stage, err)
			}
			select {
			case <-viewerAudio.queue:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
		t.Fatalf("%s: viewer got no host audio", stage)
	}

	session.WireAudioTrackFor(host, pair.audio, pair.serverPC, generation)
	sendAudioUntilViewerGetsIt("audio wired")

	session.WireVideoTrackFor(host, pair.video, pair.serverPC, generation)
	session.videoMu.Lock()
	audioIngress := session.audioIngress
	videoIngress := session.videoIngress
	session.videoMu.Unlock()
	if videoIngress == nil || videoIngress.track != pair.video {
		t.Fatal("video ingress was not wired")
	}
	if audioIngress == nil || audioIngress.track != pair.audio {
		t.Fatal("wiring the video track cancelled the audio ingress")
	}

	// Drain packets that were queued before the video track was wired.
	for len(viewerAudio.queue) > 0 {
		<-viewerAudio.queue
	}
	sendAudioUntilViewerGetsIt("video wired after audio")
}

type hostIngressAVTrackPair struct {
	serverPC  *webrtc.PeerConnection
	video     *webrtc.TrackRemote
	audio     *webrtc.TrackRemote
	hostAudio *webrtc.TrackLocalStaticRTP
	cleanup   func()
}

// newHostIngressAVTrackPair connects a host PC that sends H.264 and Opus to a
// server PC and returns the server's remote video and audio tracks.
func newHostIngressAVTrackPair(t *testing.T, api *webrtc.API) hostIngressAVTrackPair {
	t.Helper()

	hostPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		hostPC.Close()
		t.Fatalf("create server PC: %v", err)
	}
	cleanup := func() {
		hostPC.Close()
		serverPC.Close()
	}
	fail := func(format string, args ...any) {
		t.Helper()
		cleanup()
		t.Fatalf(format, args...)
	}

	hostVideo, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "host")
	if err != nil {
		fail("create host video track: %v", err)
	}
	hostAudio, err := webrtc.NewTrackLocalStaticRTP(streamAudioCodecCapability(), "audio", "host")
	if err != nil {
		fail("create host audio track: %v", err)
	}
	for _, track := range []*webrtc.TrackLocalStaticRTP{hostVideo, hostAudio} {
		if _, err := hostPC.AddTransceiverFromTrack(
			track,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
		); err != nil {
			fail("add host %s track: %v", track.Kind(), err)
		}
	}

	offer, err := hostPC.CreateOffer(nil)
	if err != nil {
		fail("create host offer: %v", err)
	}
	offerGathered := webrtc.GatheringCompletePromise(hostPC)
	if err := hostPC.SetLocalDescription(offer); err != nil {
		fail("set host local description: %v", err)
	}
	<-offerGathered
	if err := serverPC.SetRemoteDescription(*hostPC.LocalDescription()); err != nil {
		fail("set server remote description: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		fail("create server answer: %v", err)
	}
	answerGathered := webrtc.GatheringCompletePromise(serverPC)
	if err := serverPC.SetLocalDescription(answer); err != nil {
		fail("set server local description: %v", err)
	}
	<-answerGathered

	connected := make(chan struct{})
	var connectedOnce sync.Once
	hostPC.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})
	if err := hostPC.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
		fail("set host remote description: %v", err)
	}
	select {
	case <-connected:
	case <-time.After(5 * time.Second):
		fail("host/server peer connection did not connect")
	}

	pair := hostIngressAVTrackPair{
		serverPC:  serverPC,
		hostAudio: hostAudio,
		cleanup:   cleanup,
	}
	// A remote track has no codec until its first packet arrives, and the
	// wiring functions check the codec. Send until both codecs are known.
	deadline := time.Now().Add(2 * time.Second)
	for seq := uint16(1); time.Now().Before(deadline); seq++ {
		_ = hostVideo.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 3000},
			Payload: []byte{0x65, 0x88, 0x84, 0x00},
		})
		_ = hostAudio.WriteRTP(&rtp.Packet{
			Header:  rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960},
			Payload: []byte{0xfc, 0xff, 0xfe},
		})
		pair.video, pair.audio = nil, nil
		for _, receiver := range serverPC.GetReceivers() {
			track := receiver.Track()
			if track == nil || track.Codec().MimeType == "" {
				continue
			}
			switch track.Kind() {
			case webrtc.RTPCodecTypeVideo:
				pair.video = track
			case webrtc.RTPCodecTypeAudio:
				pair.audio = track
			}
		}
		if pair.video != nil && pair.audio != nil {
			return pair
		}
		time.Sleep(10 * time.Millisecond)
	}
	fail("server did not expose both host tracks (video=%v audio=%v)", pair.video != nil, pair.audio != nil)
	return pair
}

// The fan-out and the viewer writer goroutine both update the queue head
// time of a leg. Run them at the same time: go test -race reported a data
// race on the old time.Time field (01-SFU.md §6.1, queue rescue).
func TestViewerQueueHeadUpdatesDoNotRace(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	viewer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()
	viewer.UserID = "viewer"
	viewer.Role = "viewer"
	viewer.bindSession(session)
	session.viewers.Add(viewer)
	if !session.WireViewerVideo(viewer) {
		t.Fatal("viewer video leg was not wired")
	}
	session.videoMu.Lock()
	leg := session.viewerVideo[viewer.UserID]
	session.markViewerVideoLiveLocked(leg)
	session.videoMu.Unlock()

	for i := 0; i < 5000; i++ {
		session.fanOutVideoRTP(deltaTestPacket(uint16(i), uint32(i)*3000))
		_ = leg.queueRescueNeeded(time.Now())
	}
	waitFor(t, "the writer empties the queue", func() bool {
		return len(leg.queue) == 0 && leg.queueHeadAt.Load() == 0
	})
	session.stopViewerVideo(viewer)
}

// One failed write on the viewer audio leg drops that packet only. The
// writer used to exit on the first write error, so the viewer had no game
// audio for the rest of the session.
func TestViewerAudioWriterSurvivesAWriteError(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	written := make(chan uint16, 8)
	calls := 0
	leg := &streamViewerAudio{
		peer:   &Peer{},
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan *rtp.Packet, streamAudioQueueSize),
		write: func(pkt *rtp.Packet) error {
			calls++
			if calls == 1 {
				return errors.New("write: no buffer space available")
			}
			written <- pkt.SequenceNumber
			return nil
		},
	}
	done := make(chan struct{})
	go func() {
		session.writeViewerAudioRTP(leg)
		close(done)
	}()
	leg.queue <- &rtp.Packet{Header: rtp.Header{SequenceNumber: 1}}
	leg.queue <- &rtp.Packet{Header: rtp.Header{SequenceNumber: 2}}
	select {
	case seq := <-written:
		if seq != 2 {
			t.Fatalf("written seq %d, want 2", seq)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the audio writer stopped after one write error")
	}
	if got := leg.peer.PacketsDropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the audio writer did not end with its leg")
	}
}

// A closed audio leg still ends the writer.
func TestViewerAudioWriterEndsOnClosedTrack(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leg := &streamViewerAudio{
		peer:   &Peer{},
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan *rtp.Packet, streamAudioQueueSize),
		write:  func(*rtp.Packet) error { return ErrNoStreamAudioTrack },
	}
	done := make(chan struct{})
	go func() {
		session.writeViewerAudioRTP(leg)
		close(done)
	}()
	leg.queue <- &rtp.Packet{}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the audio writer did not end on a closed track")
	}
}

// typesOf returns the message types that probe received, in order, from
// the types in want only.
func typesOf(probe *wsProbe, want ...string) []string {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	var out []string
	for _, typ := range probe.types {
		for _, w := range want {
			if typ == w {
				out = append(out, typ)
			}
		}
	}
	return out
}

// A viewer whose socket is stuck must not hold up the host. The host
// disconnect delivery used to send session_ended to each viewer while it
// held the mutex that host Join takes, so a stuck viewer write blocked the
// disconnect cleanup and every host rejoin. The events are now queued
// under videoMu, in session order, and sent per viewer off the caller.
func TestHostJoinDoesNotWaitForAStuckViewer(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	probe := newWSProbe(t)
	viewer := NewPeer("viewer", "viewer", probe.server, nil)
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	first := &Peer{UserID: "host-1", Role: "host"}
	if err := session.AddPeer(first); err != nil {
		t.Fatalf("add first host: %v", err)
	}
	takeStreamJoinView(t, viewer, probe)

	// A write to the viewer that does not complete holds wsMu.
	viewer.wsMu.Lock()
	stuck := true
	defer func() {
		if stuck {
			viewer.wsMu.Unlock()
		}
	}()

	removed := make(chan struct{})
	go func() {
		session.RemovePeerInstance(first)
		close(removed)
	}()
	select {
	case <-removed:
	case <-time.After(time.Second):
		t.Fatal("the host disconnect waited for the stuck viewer")
	}

	second := &Peer{UserID: "host-2", Role: "host"}
	joined := make(chan error, 1)
	go func() { joined <- session.AddPeer(second) }()
	select {
	case err := <-joined:
		if err != nil {
			t.Fatalf("second host join: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the host join waited for the stuck viewer")
	}
	lockFree := make(chan struct{})
	go func() {
		session.videoMu.Lock()
		session.videoMu.Unlock()
		close(lockFree)
	}()
	select {
	case <-lockFree:
	case <-time.After(time.Second):
		t.Fatal("videoMu is held while the viewer is stuck")
	}

	// The viewer recovers and gets both events, in session order.
	viewer.wsMu.Unlock()
	stuck = false
	viewer.SendSignalingQueued("test_flush", struct{}{})
	waitFor(t, "the queued events reach the viewer", func() bool {
		return probe.count("test_flush") == 1
	})
	got := typesOf(probe, "host_reconnecting", "session_ended", "member_joined", "host_reconnected")
	want := []string{"host_reconnecting", "member_joined", "host_reconnected"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("viewer events %v, want %v", got, want)
	}
}
