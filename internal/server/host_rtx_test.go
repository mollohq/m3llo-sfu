package server

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// rtxTestTrack is a host video track that hands its bound context to the
// test, so the test can write on the media SSRC and on the RTX SSRC.
type rtxTestTrack struct {
	bound chan webrtc.TrackLocalContext
}

func (t *rtxTestTrack) Bind(ctx webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	for _, codec := range ctx.CodecParameters() {
		if strings.EqualFold(codec.MimeType, webrtc.MimeTypeH264) {
			t.bound <- ctx
			return codec, nil
		}
	}
	return webrtc.RTPCodecParameters{}, webrtc.ErrUnsupportedCodec
}

func (t *rtxTestTrack) Unbind(webrtc.TrackLocalContext) error { return nil }
func (t *rtxTestTrack) ID() string                            { return "video" }
func (t *rtxTestTrack) RID() string                           { return "" }
func (t *rtxTestTrack) StreamID() string                      { return "host" }
func (t *rtxTestTrack) Kind() webrtc.RTPCodecType             { return webrtc.RTPCodecTypeVideo }

// rtxTestHostAPI is a host like libmello's: H.264 at PT 96 and RFC 4588 RTX
// at PT 97 (apt=96), with no interceptors.
func rtxTestHostAPI(t *testing.T) *webrtc.API {
	t.Helper()
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: streamVideoCodecCapability(),
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register host H.264: %v", err)
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeRTX, ClockRate: 90000, SDPFmtpLine: "apt=96",
		},
		PayloadType: 97,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register host RTX: %v", err)
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(me))
}

// negotiate runs one offer/answer between an offerer and an answerer with
// full ICE gathering, and returns the answer.
func negotiate(t *testing.T, offerer, answerer *webrtc.PeerConnection) string {
	t.Helper()
	offer, err := offerer.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(offerer)
	if err := offerer.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local offer: %v", err)
	}
	<-gathered
	if err := answerer.SetRemoteDescription(*offerer.LocalDescription()); err != nil {
		t.Fatalf("apply offer: %v", err)
	}
	answer, err := answerer.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("create answer: %v", err)
	}
	gathered = webrtc.GatheringCompletePromise(answerer)
	if err := answerer.SetLocalDescription(answer); err != nil {
		t.Fatalf("set local answer: %v", err)
	}
	<-gathered
	if err := offerer.SetRemoteDescription(*answerer.LocalDescription()); err != nil {
		t.Fatalf("apply answer: %v", err)
	}
	return answerer.LocalDescription().SDP
}

// A host repair on its RFC 4588 RTX SSRC reaches the host track that the SFU
// forwards, as the packet it repairs (original sequence number, payload type
// and payload). Before mello streaming-quality stage 4 the SFU registered no
// RTX codec: the answer had no RTX, the repair came on an SSRC that no m-line
// declared, Pion dropped it, and the viewers repaired nothing.
func TestHostRTXRepairReachesTheHostTrack(t *testing.T) {
	sfuAPI, err := newSFUAPI(webrtc.SettingEngine{})
	if err != nil {
		t.Fatalf("SFU API: %v", err)
	}
	server, err := sfuAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create SFU PC: %v", err)
	}
	defer server.Close()
	host, err := rtxTestHostAPI(t).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	defer host.Close()

	track := &rtxTestTrack{bound: make(chan webrtc.TrackLocalContext, 1)}
	if _, err := host.AddTrack(track); err != nil {
		t.Fatalf("add host track: %v", err)
	}
	received := make(chan *rtp.Packet, 256)
	server.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		for {
			pkt, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			select {
			case received <- pkt:
			default:
			}
		}
	})
	answer := negotiate(t, host, server)
	if !strings.Contains(answer, "rtx/90000") {
		t.Fatalf("the SFU answer to a host has no RTX:\n%s", answer)
	}

	var ctx webrtc.TrackLocalContext
	select {
	case ctx = <-track.bound:
	case <-time.After(10 * time.Second):
		t.Fatal("the host track was never bound")
	}
	if ctx.SSRCRetransmission() == 0 {
		t.Fatal("the host sender has no RTX SSRC")
	}
	payload := func(seq uint16) []byte { return []byte{0x41, 0x9a, byte(seq >> 8), byte(seq)} }
	writeMedia := func(seq uint16) {
		if _, err := ctx.WriteStream().WriteRTP(&rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: seq,
			Timestamp: uint32(seq) * 3000, SSRC: uint32(ctx.SSRC()),
		}, payload(seq)); err != nil {
			t.Fatalf("write media %d: %v", seq, err)
		}
	}

	// Media until the SFU reads it (DTLS and SRTP come up first), then two
	// more packets around one that is never sent: the lost packet.
	seq := uint16(1000)
	deadline := time.Now().Add(10 * time.Second)
	for started := false; !started; {
		if time.Now().After(deadline) {
			t.Fatal("the SFU never read host media")
		}
		writeMedia(seq)
		select {
		case <-received:
			started = true
		case <-time.After(50 * time.Millisecond):
			seq++
		}
	}
	lost := seq + 2
	writeMedia(seq + 1)
	writeMedia(seq + 3)

	// The repair: RFC 4588 on the RTX SSRC, the original sequence number in
	// the first two payload bytes.
	rtxPayload := append([]byte{byte(lost >> 8), byte(lost)}, payload(lost)...)
	if _, err := ctx.WriteStream().WriteRTP(&rtp.Header{
		Version: 2, PayloadType: 97, SequenceNumber: 1,
		Timestamp: uint32(lost) * 3000, SSRC: uint32(ctx.SSRCRetransmission()),
	}, rtxPayload); err != nil {
		t.Fatalf("write RTX: %v", err)
	}

	// Pion hands a repair to the reader before the next media packet, so
	// live media keeps flowing while the test waits.
	timeout := time.After(5 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	next := seq + 4
	for {
		select {
		case <-tick.C:
			writeMedia(next)
			next++
		case pkt := <-received:
			if pkt.SequenceNumber != lost {
				continue
			}
			if pkt.PayloadType != 96 {
				t.Fatalf("repair has payload type %d, want 96", pkt.PayloadType)
			}
			if pkt.SSRC != uint32(ctx.SSRC()) {
				t.Fatalf("repair has SSRC %d, want the media SSRC %d", pkt.SSRC, ctx.SSRC())
			}
			if !bytes.Equal(pkt.Payload, payload(lost)) {
				t.Fatalf("repair payload %x, want %x", pkt.Payload, payload(lost))
			}
			return
		case <-timeout:
			t.Fatalf("the RTX repair of sequence %d never reached the host track", lost)
		}
	}
}

// The viewer leg stays as it is: the viewer API registers no RTX, so a
// viewer offer that carries RTX gets an answer without it, and the SFU
// repairs viewers from its NACK cache on the viewer SSRC.
func TestViewerAnswerHasNoRTX(t *testing.T) {
	viewerAPI, err := newSFUViewerAPI(webrtc.SettingEngine{}, nil)
	if err != nil {
		t.Fatalf("viewer API: %v", err)
	}
	server, err := viewerAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create SFU PC: %v", err)
	}
	defer server.Close()
	viewer, err := rtxTestHostAPI(t).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewer.Close()
	if _, err := viewer.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatalf("add viewer video: %v", err)
	}
	answer := negotiate(t, viewer, server)
	if strings.Contains(answer, "rtx/90000") {
		t.Fatalf("the SFU answer to a viewer negotiates RTX:\n%s", answer)
	}
}

// The relay counts the host repairs that Pion unwrapped from RTX: forwarded
// (the original was lost) and dropped (the original had arrived). The gate
// reads them as the RTX end-to-end numbers (rtp_rtx_recovered_packets,
// rtp_rtx_duplicate_packets in stream_relay_stats).
func TestIngressCountsRTXRepairs(t *testing.T) {
	s := &StreamSession{}
	var window streamSequenceWindow
	for _, seq := range []uint16{10, 11, 13} {
		if !s.acceptIngressVideo(&window, seq, 1200, false) {
			t.Fatalf("media %d dropped", seq)
		}
	}
	if !s.acceptIngressVideo(&window, 12, 1200, true) {
		t.Fatal("the repair of the lost sequence 12 was dropped")
	}
	if s.acceptIngressVideo(&window, 11, 1200, true) {
		t.Fatal("a repair of sequence 11, which arrived, was forwarded")
	}
	if got := s.rtpRTXRecoveredPackets.Load(); got != 1 {
		t.Fatalf("recovered %d, want 1", got)
	}
	if got := s.rtpRTXDuplicatePackets.Load(); got != 1 {
		t.Fatalf("RTX duplicates %d, want 1", got)
	}
	if got := s.rtpDuplicatePackets.Load(); got != 1 {
		t.Fatalf("duplicates %d, want 1", got)
	}
}

// The SFU registers H.264 Main at level 5.1 (4d0033), the level mello's
// encoders run at since streaming-quality stage 4 (it said 4d002a, level
// 4.2); its own offers (renegotiation) say it. Pion matches H.264 on the
// profile bytes and answers with the offer's parameters, so a host that
// offers either level gets H.264 at its level (level-asymmetry-allowed=1).
func TestSFUAnswersLevel42AndLevel51HostOffers(t *testing.T) {
	for _, level := range []string{"4d002a", "4d0033"} {
		me := &webrtc.MediaEngine{}
		capability := streamVideoCodecCapability()
		capability.SDPFmtpLine = "profile-level-id=" + level + ";packetization-mode=1;level-asymmetry-allowed=1"
		if err := me.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: capability,
			PayloadType:        96,
		}, webrtc.RTPCodecTypeVideo); err != nil {
			t.Fatalf("register host H.264: %v", err)
		}
		host, err := webrtc.NewAPI(webrtc.WithMediaEngine(me)).NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create host PC: %v", err)
		}
		if _, err := host.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
			t.Fatalf("add host video: %v", err)
		}
		sfuAPI, err := newSFUAPI(webrtc.SettingEngine{})
		if err != nil {
			t.Fatalf("SFU API: %v", err)
		}
		server, err := sfuAPI.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create SFU PC: %v", err)
		}
		answer := negotiate(t, host, server)
		if !strings.Contains(answer, "H264/90000") || !strings.Contains(answer, "profile-level-id="+level) {
			t.Fatalf("offer %s: the answer has no H.264 at that level:\n%s", level, answer)
		}
		_ = server.Close()
		_ = host.Close()
	}
}
