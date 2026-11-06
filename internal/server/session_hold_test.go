package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// streamTestHost is a host client PeerConnection that negotiated with the
// SFU through handleOffer and sends H.264 RTP on one track.
type streamTestHost struct {
	peer       *Peer
	probe      *wsProbe
	pc         *webrtc.PeerConnection
	track      *webrtc.TrackLocalStaticRTP
	connected  chan struct{}
	candidates int
	// plis counts the PLIs the host client received from the SFU.
	plis atomic.Int64
	// remb is the last REMB bitrate the host client received.
	remb atomic.Int64
}

// connectStreamTestHost joins user as the host of sessionID, offers one
// sendonly H.264 track and connects.
func connectStreamTestHost(t *testing.T, s *Server, sessionID, user string, clientAPI *webrtc.API) *streamTestHost {
	t.Helper()
	peers, probes := joinStreamInOrder(t, s, sessionID, []string{user}, []string{"host"})
	h := &streamTestHost{peer: peers[0], probe: probes[0], connected: make(chan struct{})}
	pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host client PC: %v", err)
	}
	h.pc = pc
	t.Cleanup(func() { _ = pc.Close() })
	h.track, err = webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "host")
	if err != nil {
		t.Fatalf("host track: %v", err)
	}
	transceiver, err := pc.AddTransceiverFromTrack(h.track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		t.Fatalf("add host track: %v", err)
	}
	go func() {
		for {
			packets, _, err := transceiver.Sender().ReadRTCP()
			if err != nil {
				return
			}
			for _, packet := range packets {
				switch p := packet.(type) {
				case *rtcp.PictureLossIndication:
					h.plis.Add(1)
				case *rtcp.ReceiverEstimatedMaximumBitrate:
					h.remb.Store(int64(p.Bitrate))
				}
			}
		}
	}()
	var once sync.Once
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			once.Do(func() { close(h.connected) })
		}
	})
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("host offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set host offer: %v", err)
	}
	<-gathered
	data, _ := json.Marshal(SDPData{SDP: pc.LocalDescription().SDP})
	s.handleOffer(h.peer, &auth.Claims{UserID: user, Type: "stream"},
		SignalingMessage{Type: "offer", Data: data})

	deadline := time.Now().Add(5 * time.Second)
	var answers []SignalingMessage
	for answers = h.probe.messages("answer"); len(answers) == 0; answers = h.probe.messages("answer") {
		if time.Now().After(deadline) {
			t.Fatal("SFU sent no host answer")
		}
		time.Sleep(time.Millisecond)
	}
	var answer SDPData
	_ = json.Unmarshal(answers[0].Data, &answer)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		t.Fatalf("apply host answer: %v", err)
	}
	for {
		candidates := h.probe.messages("ice_candidate")
		for ; h.candidates < len(candidates); h.candidates++ {
			var c ICECandidateData
			_ = json.Unmarshal(candidates[h.candidates].Data, &c)
			index := uint16(c.SDPMLineIndex)
			mid := c.SDPMid
			_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &index})
		}
		select {
		case <-h.connected:
			return h
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("host did not connect to the SFU")
		}
	}
}

// sendUntilViewerGets writes delta packets from firstSeq with timestamp ts
// until the viewer receives a packet with timestamp ts. Pion fires the
// SFU's OnTrack on the first packet, so the first packets can be lost.
func sendUntilViewerGets(t *testing.T, h *streamTestHost, v *streamTestViewer, firstSeq uint16, ts uint32) *rtp.Packet {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	seq := firstSeq
	for time.Now().Before(deadline) {
		if err := h.track.WriteRTP(deltaTestPacket(seq, ts)); err != nil {
			t.Fatalf("host write: %v", err)
		}
		seq++
		select {
		case pkt := <-v.video:
			if pkt.Timestamp == ts {
				return pkt
			}
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("the viewer received no packet with timestamp %d", ts)
	return nil
}

// A host that leaves and joins again inside the hold keeps the viewers:
// they get host_reconnecting, keep their PeerConnection and video leg,
// get host_reconnected, and receive the new host's video. The new host
// starts at a sequence number below the old one; the SFU continues the
// viewer leg's numbers, or SRTP replay protection on the viewer drops the
// new packets. A host that does not come back ends the session with
// session_ended when the hold (DrainTimeout) runs out (01-SFU.md §4.1).
func TestHostRejoinInsideTheHoldKeepsTheViewerLeg(t *testing.T) {
	api := newTestAPI()
	s := newStreamTestServer(api)
	viewer := offerStreamTestViewer(t, s, "hold", api)
	viewer.connect(t)
	session := viewer.session
	legBefore := viewer.leg(t)
	viewerPC, _ := viewer.peer.currentPeerConnection()

	first := connectStreamTestHost(t, s, "hold", "host", api)
	sendUntilViewerGets(t, first, viewer, 40000, 1_000_000)

	s.removePeerMembership(first.peer)
	waitFor(t, "host_reconnecting reaches the viewer", func() bool {
		return viewer.probe.count("host_reconnecting") == 1
	})
	if got := viewer.probe.count("session_ended"); got != 0 {
		t.Fatalf("the viewer got session_ended %d times when the host left", got)
	}
	if !viewer.isMember() {
		t.Fatal("the viewer left the session when the host left")
	}

	second := connectStreamTestHost(t, s, "hold", "host", api)
	got := sendUntilViewerGets(t, second, viewer, 20000, 2_000_000)
	if int16(got.SequenceNumber-40000) <= 0 {
		t.Fatalf("the new host's packet has viewer seq %d, want after the old host's (40000 and up)", got.SequenceNumber)
	}
	waitFor(t, "host_reconnected reaches the viewer", func() bool {
		return viewer.probe.count("host_reconnected") == 1
	})
	if viewer.leg(t) != legBefore {
		t.Fatal("the viewer video leg was replaced")
	}
	if pc, _ := viewer.peer.currentPeerConnection(); pc != viewerPC {
		t.Fatal("the viewer PeerConnection was replaced")
	}
	if got := viewer.probe.count("session_ended"); got != 0 {
		t.Fatalf("the viewer got session_ended %d times", got)
	}

	// The host leaves again and does not come back.
	s.removePeerMembership(second.peer)
	if session.FinalizeDrain(time.Now().Add(DrainTimeout-time.Second), DrainTimeout) {
		t.Fatal("the session closed inside the hold")
	}
	if !session.FinalizeDrain(time.Now().Add(DrainTimeout+time.Second), DrainTimeout) {
		t.Fatal("the session did not close after the hold")
	}
	waitFor(t, "session_ended reaches the viewer", func() bool {
		return viewer.probe.count("session_ended") == 1
	})
	if got := viewer.probe.count("host_reconnecting"); got != 2 {
		t.Fatalf("host_reconnecting %d times, want 2", got)
	}
}

// A stream join that loses the race with the drain finalize joins a new
// session under the same id, as a voice join does (01-SFU.md §4.3). Here
// the closed session is still in the store when the join looks it up.
func TestStreamJoinRetriesAfterTheDrainClosedTheSession(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	stale := NewStreamSession("retry", "crew", s.Logger, streamSnapshotConfig{})
	stale.videoMu.Lock()
	stale.state = SessionClosed
	stale.videoMu.Unlock()
	s.sessions.GetOrCreate("retry", func() Session { return stale })

	peers, probes := joinStreamInOrder(t, s, "retry", []string{"viewer"}, []string{"viewer"})
	waitFor(t, "the join reply", func() bool {
		return probes[0].count("joined") == 1
	})
	sess, _ := s.sessions.Get("retry")
	if sess == Session(stale) {
		t.Fatal("the store still holds the closed session")
	}
	if current, _ := peers[0].currentSession(); current != sess {
		t.Fatal("the viewer is not in the new session")
	}
}

// A viewer that connects makes the SFU send one PLI to the host right after
// the cached IDR replay: the replayed IDR is old, and the viewer's own PLI
// comes only when it finds the gap. The viewer's PLI for the same gap,
// inside 200 ms, is coalesced with it: one PLI in all (01-SFU.md §6.1).
func TestViewerConnectSendsOneUpstreamPLI(t *testing.T) {
	api := newTestAPI()
	s := newStreamTestServer(api)
	viewer := offerStreamTestViewer(t, s, "connect-pli", api)
	host := connectStreamTestHost(t, s, "connect-pli", "host", api)
	// The SFU wires the host ingress on the first packet.
	waitFor(t, "the host ingress is wired", func() bool {
		_ = host.track.WriteRTP(deltaTestPacket(100, 9000))
		viewer.session.videoMu.Lock()
		defer viewer.session.videoMu.Unlock()
		return viewer.session.videoIngress != nil
	})
	before := host.plis.Load()

	viewer.connect(t)
	waitFor(t, "a PLI reaches the host after the viewer connects", func() bool {
		return host.plis.Load() > before
	})
	// The viewer's own PLI for the same gap.
	ssrc := uint32(viewer.leg(t).sender.GetParameters().Encodings[0].SSRC)
	if err := viewer.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: ssrc}}); err != nil {
		t.Fatalf("viewer PLI: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := host.plis.Load() - before; got != 1 {
		t.Fatalf("the host got %d PLIs for one viewer connect, want 1", got)
	}
}

// A host that ends the stream on purpose (a leave message) ends it for the
// viewers at once with session_ended, as before the hold. Only an unclean
// disconnect holds (01-SFU.md §4.1). The drain sends no second
// session_ended.
func TestCleanHostLeaveEndsTheSessionAtOnce(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	peers, probes := joinStreamInOrder(t, s, "clean-leave", []string{"host", "viewer"}, []string{"host", "viewer"})
	host, viewer, viewerProbe := peers[0], peers[1], probes[1]
	sess, _ := s.sessions.Get("clean-leave")
	session := sess.(*StreamSession)

	s.handleLeave(host, &auth.Claims{UserID: "host", Type: "stream"}, SignalingMessage{Type: "leave"})
	s.removePeerMembership(host)
	viewer.SendSignalingQueued("test_flush", struct{}{})
	waitFor(t, "flush", func() bool { return viewerProbe.count("test_flush") == 1 })
	if got := viewerProbe.count("session_ended"); got != 1 {
		t.Fatalf("session_ended %d times after a clean host leave, want 1", got)
	}
	if got := viewerProbe.count("host_reconnecting"); got != 0 {
		t.Fatalf("host_reconnecting %d times after a clean host leave, want 0", got)
	}
	if !session.FinalizeDrain(time.Now().Add(DrainTimeout+time.Second), DrainTimeout) {
		t.Fatal("the drain did not close the session")
	}
	viewer.SendSignalingQueued("test_flush", struct{}{})
	waitFor(t, "flush", func() bool { return viewerProbe.count("test_flush") == 2 })
	if got := viewerProbe.count("session_ended"); got != 1 {
		t.Fatalf("session_ended %d times after the drain, want 1", got)
	}
}

// A WebSocket close with code 1000 from the host is a clean leave too; the
// signaling loop sees it and the viewers get session_ended at once.
func TestNormalWebSocketCloseIsACleanLeave(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	serverConn := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- c
	}))
	defer ts.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	host := NewPeer("host", "", <-serverConn, s.Logger)
	claims := &auth.Claims{UserID: "host", Type: "stream"}
	loopDone := make(chan struct{})
	go func() {
		s.handleSignalingLoop(host, claims)
		close(loopDone)
	}()
	join, _ := json.Marshal(JoinStreamData{SessionID: "normal-close", Role: "host"})
	raw, _ := json.Marshal(SignalingMessage{Type: "join_stream", Data: join})
	if err := client.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("send join: %v", err)
	}
	waitFor(t, "the host joined", func() bool {
		sess, ok := s.sessions.Get("normal-close")
		return ok && sess.MemberCount() == 1
	})
	_, probes := joinStreamInOrder(t, s, "normal-close", []string{"viewer"}, []string{"viewer"})

	if err := client.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "stop"),
		time.Now().Add(time.Second)); err != nil {
		t.Fatalf("send close: %v", err)
	}
	select {
	case <-loopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the signaling loop did not end on the close")
	}
	waitFor(t, "session_ended", func() bool { return probes[0].count("session_ended") == 1 })
	if got := probes[0].count("host_reconnecting"); got != 0 {
		t.Fatalf("host_reconnecting %d times after a normal close, want 0", got)
	}
}
