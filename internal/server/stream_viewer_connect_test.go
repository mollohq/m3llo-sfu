package server

import (
	"encoding/binary"
	"encoding/json"
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

// streamTestViewer is a viewer client PeerConnection that negotiated with the
// SFU through handleOffer, so the production connection-state callback runs.
type streamTestViewer struct {
	s         *Server
	session   *StreamSession
	peer      *Peer
	probe     *wsProbe
	pc        *webrtc.PeerConnection
	video     chan *rtp.Packet
	connected chan struct{}
	// candidates is how many SFU ice_candidate messages were applied.
	candidates int
}

func newStreamTestServer(api *webrtc.API) *Server {
	logger := logging.New("error")
	return &Server{
		Logger:    logger,
		metrics:   metrics.New(),
		sessions:  NewSessionStore(logger),
		webrtcAPI: api,
	}
}

// offerStreamTestViewer joins a viewer to sessionID and applies its offer.
// The viewer does not apply the answer yet, so its PeerConnection cannot
// connect before connect is called.
func offerStreamTestViewer(t *testing.T, s *Server, sessionID string, clientAPI *webrtc.API) *streamTestViewer {
	t.Helper()
	peers, probes := joinStreamInOrder(t, s, sessionID, []string{"viewer"}, []string{"viewer"})
	sess, ok := s.sessions.Get(sessionID)
	if !ok {
		t.Fatal("stream session was not created")
	}
	v := &streamTestViewer{
		s:         s,
		session:   sess.(*StreamSession),
		peer:      peers[0],
		probe:     probes[0],
		video:     make(chan *rtp.Packet, 1024),
		connected: make(chan struct{}),
	}

	pc, err := clientAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer client PC: %v", err)
	}
	v.pc = pc
	t.Cleanup(func() { _ = pc.Close() })
	for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
		if _, err := pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		}); err != nil {
			t.Fatalf("add viewer %s transceiver: %v", kind, err)
		}
	}
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if track.Kind() != webrtc.RTPCodecTypeVideo {
			return
		}
		for {
			pkt, _, err := track.ReadRTP()
			if err != nil {
				return
			}
			v.video <- pkt
		}
	})
	var connectedOnce sync.Once
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(v.connected) })
		}
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create viewer offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set viewer offer: %v", err)
	}
	<-gathered
	data, _ := json.Marshal(SDPData{SDP: pc.LocalDescription().SDP})
	s.handleOffer(v.peer, &auth.Claims{UserID: "viewer", Type: "stream"},
		SignalingMessage{Type: "offer", Data: data})
	if errs := v.probe.messages("error"); len(errs) > 0 {
		t.Fatalf("SFU rejected the viewer offer: %s", errs[0].Data)
	}
	return v
}

// connect applies the SFU answer and the trickled SFU candidates, and waits
// until the viewer client is connected.
func (v *streamTestViewer) connect(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var answers []SignalingMessage
	for answers = v.probe.messages("answer"); len(answers) == 0; answers = v.probe.messages("answer") {
		if time.Now().After(deadline) {
			t.Fatal("SFU sent no answer")
		}
		time.Sleep(time.Millisecond)
	}
	var answer SDPData
	if err := json.Unmarshal(answers[0].Data, &answer); err != nil {
		t.Fatalf("answer data: %v", err)
	}
	if err := v.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer.SDP,
	}); err != nil {
		t.Fatalf("apply SFU answer: %v", err)
	}
	for {
		candidates := v.probe.messages("ice_candidate")
		for ; v.candidates < len(candidates); v.candidates++ {
			var c ICECandidateData
			if err := json.Unmarshal(candidates[v.candidates].Data, &c); err != nil {
				t.Fatalf("candidate data: %v", err)
			}
			index := uint16(c.SDPMLineIndex)
			mid := c.SDPMid
			if err := v.pc.AddICECandidate(webrtc.ICECandidateInit{
				Candidate:     c.Candidate,
				SDPMid:        &mid,
				SDPMLineIndex: &index,
			}); err != nil {
				t.Fatalf("add SFU candidate: %v", err)
			}
		}
		select {
		case <-v.connected:
			// The SFU side reaches connected on its own: wait until the
			// viewer leg is live.
			select {
			case <-v.leg(t).connected:
				return
			case <-time.After(5 * time.Second):
				t.Fatal("SFU viewer leg did not go live")
			}
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("viewer did not connect to the SFU")
		}
	}
}

// nextVideo returns the next video packet the viewer client received.
func (v *streamTestViewer) nextVideo(t *testing.T, what string) *rtp.Packet {
	t.Helper()
	select {
	case pkt := <-v.video:
		return pkt
	case <-time.After(5 * time.Second):
		t.Fatalf("viewer received no video packet: %s", what)
		return nil
	}
}

func (v *streamTestViewer) leg(t *testing.T) *streamViewerVideo {
	t.Helper()
	v.session.videoMu.Lock()
	defer v.session.videoMu.Unlock()
	leg := v.session.viewerVideo[v.peer.UserID]
	if leg == nil {
		t.Fatal("viewer video leg was not wired")
	}
	return leg
}

// idrTestAccessUnit is one IDR access unit in FU-A fragments, with more
// packets than the 64-packet viewer queue.
func idrTestAccessUnit(firstSeq uint16, timestamp uint32, packets int) []*rtp.Packet {
	out := make([]*rtp.Packet, 0, packets)
	for i := 0; i < packets; i++ {
		fuHeader := byte(0x05) // IDR slice
		if i == 0 {
			fuHeader |= 0x80 // start
		}
		if i == packets-1 {
			fuHeader |= 0x40 // end
		}
		out = append(out, &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    96,
				SequenceNumber: firstSeq + uint16(i),
				Timestamp:      timestamp,
				SSRC:           0x1234,
				Marker:         i == packets-1,
			},
			Payload: []byte{0x7C, fuHeader, byte(i), 0xAB, 0xCD},
		})
	}
	return out
}

func deltaTestPacket(seq uint16, timestamp uint32) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      timestamp,
			SSRC:           0x1234,
			Marker:         true,
		},
		Payload: []byte{0x41, 0x9A, 0x01, 0x02},
	}
}

// A viewer that connects after the IDR cache filled receives the whole
// cached IDR, in order, and nothing before its PeerConnection is connected.
// The replay used to go into the queue at answer time, before DTLS, where
// Pion's SRTP writer dropped it, and the 64-packet queue cut it.
func TestLateViewerReceivesCachedIDRAfterConnected(t *testing.T) {
	api := newTestAPI()
	s := newStreamTestServer(api)
	viewer := offerStreamTestViewer(t, s, "idr-replay", api)
	session := viewer.session

	idr := idrTestAccessUnit(1000, 90_000, 100)
	for _, pkt := range idr {
		session.fanOutVideoRTP(pkt)
	}

	leg := viewer.leg(t)
	session.fanOutVideoRTP(deltaTestPacket(2000, 93_000))
	session.videoMu.Lock()
	live := leg.live
	session.videoMu.Unlock()
	if live {
		t.Fatal("viewer leg is live before its PeerConnection connected")
	}
	if got := len(leg.queue); got != 0 {
		t.Fatalf("fan-out queued %d packets before connect, want 0", got)
	}
	if got := viewer.peer.PacketsSent.Load(); got != 0 {
		t.Fatalf("SFU wrote %d packets to the viewer before connect, want 0", got)
	}

	viewer.connect(t)
	for i, want := range idr {
		got := viewer.nextVideo(t, "cached IDR")
		if got.SequenceNumber != want.SequenceNumber || got.Timestamp != want.Timestamp {
			t.Fatalf("replay packet %d: seq %d ts %d, want seq %d ts %d",
				i, got.SequenceNumber, got.Timestamp, want.SequenceNumber, want.Timestamp)
		}
	}
	if !idr[len(idr)-1].Marker {
		t.Fatal("test IDR has no marker")
	}

	// The next packet is the first live packet after connect: the packet
	// fanned out before connect never reaches the viewer.
	session.fanOutVideoRTP(deltaTestPacket(2001, 96_000))
	if got := viewer.nextVideo(t, "live packet after replay"); got.SequenceNumber != 2001 {
		t.Fatalf("first packet after the replay has seq %d, want 2001", got.SequenceNumber)
	}
}

// nackTestViewer connects a viewer client to an SFU that runs its production
// interceptor chain. The client runs no interceptor, so every repeat comes
// from the SFU, and it has no SRTP replay protection, so it keeps a repeat of
// a packet that it received. A real viewer NACKs only a packet it lost.
func nackTestViewer(t *testing.T, sessionID string) *streamTestViewer {
	t.Helper()
	sfuAPI, err := newSFUAPI(webrtc.SettingEngine{})
	if err != nil {
		t.Fatalf("SFU API: %v", err)
	}
	var clientSettings webrtc.SettingEngine
	clientSettings.DisableSRTPReplayProtection(true)
	viewer := offerStreamTestViewer(t, newStreamTestServer(sfuAPI), sessionID,
		newTestAPIWithSettings(clientSettings))
	viewer.connect(t)
	return viewer
}

// sendAndNACK forwards seq 3000 to 3004 to the viewer, sends extra RTCP, then
// a NACK for seq 3002, and returns the next packet the viewer gets.
func sendAndNACK(t *testing.T, viewer *streamTestViewer, extra func(mediaSSRC uint32) []rtcp.Packet) *rtp.Packet {
	t.Helper()
	for seq := uint16(3000); seq < 3005; seq++ {
		viewer.session.fanOutVideoRTP(deltaTestPacket(seq, uint32(seq)*3000))
	}
	var mediaSSRC uint32
	for want := uint16(3000); want < 3005; want++ {
		got := viewer.nextVideo(t, "live packet")
		if got.SequenceNumber != want {
			t.Fatalf("live packet seq %d, want %d", got.SequenceNumber, want)
		}
		mediaSSRC = got.SSRC
	}
	var extraPackets []rtcp.Packet
	if extra != nil {
		extraPackets = extra(mediaSSRC)
	}
	for _, pkt := range extraPackets {
		if err := viewer.pc.WriteRTCP([]rtcp.Packet{pkt}); err != nil {
			t.Fatalf("send extra RTCP: %v", err)
		}
	}
	if err := viewer.pc.WriteRTCP([]rtcp.Packet{&rtcp.TransportLayerNack{
		SenderSSRC: 1,
		MediaSSRC:  mediaSSRC,
		Nacks:      rtcp.NackPairsFromSequenceNumbers([]uint16{3002}),
	}}); err != nil {
		t.Fatalf("send NACK: %v", err)
	}
	got := viewer.nextVideo(t, "retransmit of seq 3002")
	if got.SSRC != mediaSSRC {
		t.Fatalf("retransmit on ssrc %d, want the viewer leg ssrc %d", got.SSRC, mediaSSRC)
	}
	return got
}

// A NACK from a viewer makes the SFU send the packet again on the viewer leg
// (Pion's NACK responder, 01-SFU.md §6.1).
func TestViewerNACKRetransmitsOnViewerLeg(t *testing.T) {
	viewer := nackTestViewer(t, "nack-repair")
	if got := sendAndNACK(t, viewer, nil); got.SequenceNumber != 3002 {
		t.Fatalf("after the NACK got seq %d, want a repeat of seq 3002", got.SequenceNumber)
	}
}

// One viewer RTCP packet that does not parse must not stop NACK repair. The
// interceptor chain parses RTCP inside RTPSender.Read and returns the parse
// error; the RTCP loop of the viewer leg used to end on it, and from then on
// no NACK of that viewer got an answer, and no PLI or REMB went upstream.
func TestViewerNACKRepairSurvivesInvalidRTCP(t *testing.T) {
	viewer := nackTestViewer(t, "nack-after-invalid-rtcp")
	// A transport-cc feedback for the viewer leg that declares 10 packet
	// statuses and carries no status chunk.
	truncatedTWCC := func(mediaSSRC uint32) []rtcp.Packet {
		raw := rtcp.RawPacket{
			0x8F, 205, 0x00, 0x04,
			0x00, 0x00, 0x00, 0x01,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x0A,
			0x00, 0x00, 0x00, 0x00,
		}
		binary.BigEndian.PutUint32(raw[8:], mediaSSRC)
		return []rtcp.Packet{&raw}
	}
	if got := sendAndNACK(t, viewer, truncatedTWCC); got.SequenceNumber != 3002 {
		t.Fatalf("after the NACK got seq %d, want a repeat of seq 3002", got.SequenceNumber)
	}
	if got := viewer.session.RelaySnapshot().RTPViewerRTCPInvalid; got != 1 {
		t.Fatalf("invalid viewer RTCP count = %d, want 1", got)
	}
}
