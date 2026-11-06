package server

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func newTestAPI() *webrtc.API {
	return newTestAPIWithSettings(webrtc.SettingEngine{})
}

func newTestAPIWithSettings(se webrtc.SettingEngine) *webrtc.API {
	me := &webrtc.MediaEngine{}
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio)
	_ = me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: streamVideoCodecCapability(),
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo)
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))
}

func TestStaleICETimerDoesNotFireAfterPeerConnectionReplacement(t *testing.T) {
	api := newTestAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create first PC: %v", err)
	}
	defer pc1.Close()
	pc2, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create second PC: %v", err)
	}
	defer pc2.Close()

	peer := &Peer{}
	gen1 := peer.installPeerConnection(pc1)
	fired := make(chan struct{}, 1)
	peer.StartICEFailTimerFor(pc1, gen1, 20*time.Millisecond, func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	})

	gen2 := peer.installPeerConnection(pc2)
	peer.StartICEFailTimerFor(pc2, gen2, time.Second, func() {
		select {
		case fired <- struct{}{}:
		default:
		}
	})

	time.Sleep(50 * time.Millisecond)
	select {
	case <-fired:
		t.Fatal("stale ICE timer callback fired after PeerConnection replacement")
	default:
	}
	peer.CancelICEFailTimer()
}

func TestSameGenerationICETimerReplacementInvalidatesFiringCallback(t *testing.T) {
	api := newTestAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	peer := &Peer{}
	generation := peer.installPeerConnection(pc)
	oldFired := false
	oldEpoch := peer.StartICEFailTimerFor(pc, generation, time.Hour, func() {
		oldFired = true
	})
	newEpoch := peer.StartICEFailTimerFor(pc, generation, time.Hour, func() {})
	if oldEpoch == 0 || newEpoch == 0 || oldEpoch == newEpoch {
		t.Fatalf("timer epochs = old %d new %d, want distinct non-zero epochs", oldEpoch, newEpoch)
	}

	if peer.expireICEFailTimerFor(pc, generation, oldEpoch, func() {
		oldFired = true
	}) {
		t.Fatal("replaced same-generation timer callback remained active")
	}
	if oldFired {
		t.Fatal("replaced same-generation timer callback fired")
	}
	peer.CancelICEFailTimer()
}

func TestSameGenerationICETimerReplacementSerializesWithActiveCallback(t *testing.T) {
	api := newTestAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	peer := &Peer{}
	generation := peer.installPeerConnection(pc)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	peer.StartICEFailTimerFor(pc, generation, 0, func() {
		close(callbackStarted)
		<-releaseCallback
	})

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("ICE timer callback did not start")
	}

	replacementDone := make(chan uint64, 1)
	go func() {
		replacementDone <- peer.StartICEFailTimerFor(pc, generation, time.Hour, func() {})
	}()
	select {
	case <-replacementDone:
		t.Fatal("same-generation timer replacement completed during old callback")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCallback)
	select {
	case epoch := <-replacementDone:
		if epoch == 0 {
			t.Fatal("replacement timer was rejected")
		}
	case <-time.After(time.Second):
		t.Fatal("replacement timer did not resume after old callback")
	}
	peer.CancelICEFailTimer()
}

func TestAddStreamVideoTrackReusesOfferedTransceiver(t *testing.T) {
	api := newTestAPI()
	viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewerPC.Close()

	if _, err := viewerPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		t.Fatalf("add viewer video transceiver: %v", err)
	}
	offer, err := viewerPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create viewer offer: %v", err)
	}
	if err := viewerPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("set viewer local description: %v", err)
	}

	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create server PC: %v", err)
	}
	defer serverPC.Close()
	if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
		t.Fatalf("set server remote description: %v", err)
	}

	peer := &Peer{PC: serverPC}
	transceiverCount := len(serverPC.GetTransceivers())
	track, err := peer.AddStreamVideoTrack()
	if err != nil {
		t.Fatalf("add stream video track: %v", err)
	}
	// The viewer offers one video m-line, and the SFU answers on it only. A
	// sender on a transceiver the viewer did not offer is not in the answer,
	// and Pion writes its packets to no binding (01-SFU.md §6.1).
	if got := len(serverPC.GetTransceivers()); got != transceiverCount {
		t.Fatalf("stream video added a transceiver the viewer did not offer: before=%d after=%d",
			transceiverCount, got)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("create server answer: %v", err)
	}
	if got := strings.Count(answer.SDP, "m=video"); got != 1 {
		t.Fatalf("answer has %d video m-lines, want 1:\n%s", got, answer.SDP)
	}

	codec := track.Codec()
	if codec.MimeType != webrtc.MimeTypeH264 ||
		codec.ClockRate != 90000 ||
		codec.SDPFmtpLine != "profile-level-id=4d0033;packetization-mode=1;level-asymmetry-allowed=1" {
		t.Fatalf("unexpected stream video codec: %+v", codec)
	}
	expectedFeedback := []webrtc.RTCPFeedback{
		{Type: "nack"},
		{Type: "nack", Parameter: "pli"},
		{Type: "goog-remb"},
	}
	if !reflect.DeepEqual(codec.RTCPFeedback, expectedFeedback) {
		t.Fatalf("unexpected RTCP feedback: %+v", codec.RTCPFeedback)
	}
	sender, lifecycle := peer.StreamVideoSender()
	if sender == nil || lifecycle == nil {
		t.Fatal("stream video sender lifecycle was not exposed")
	}
	if sender.Track() != track {
		t.Fatal("stream video sender does not own the created track")
	}
	select {
	case <-lifecycle.Done():
		t.Fatal("stream video lifecycle was cancelled before cleanup")
	default:
	}

	if _, err := peer.AddStreamVideoTrack(); !errors.Is(err, ErrStreamVideoTrackExists) {
		t.Fatalf("second add error = %v, want %v", err, ErrStreamVideoTrackExists)
	}

	peer.closeStreamVideo()
	select {
	case <-lifecycle.Done():
	default:
		t.Fatal("stream video lifecycle was not cancelled during cleanup")
	}
	if sender, lifecycle := peer.StreamVideoSender(); sender != nil || lifecycle != nil {
		t.Fatal("stream video state remained visible after cleanup")
	}
}

func TestAddStreamVideoTrackRequiresAppliedVideoOffer(t *testing.T) {
	peer := &Peer{}
	if _, err := peer.AddStreamVideoTrack(); !errors.Is(err, ErrNoPeerConnection) {
		t.Fatalf("nil PC error = %v, want %v", err, ErrNoPeerConnection)
	}

	api := newTestAPI()
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create server PC: %v", err)
	}
	defer serverPC.Close()
	peer.PC = serverPC

	if _, err := peer.AddStreamVideoTrack(); !errors.Is(err, ErrNoRemoteDescription) {
		t.Fatalf("missing remote description error = %v, want %v", err, ErrNoRemoteDescription)
	}

	viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewerPC.Close()
	if _, err := viewerPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		t.Fatalf("add viewer audio transceiver: %v", err)
	}
	offer, err := viewerPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create viewer offer: %v", err)
	}
	if err := viewerPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("set viewer local description: %v", err)
	}
	if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
		t.Fatalf("set server remote description: %v", err)
	}

	if _, err := peer.AddStreamVideoTrack(); !errors.Is(err, ErrNoStreamVideoTransceiver) {
		t.Fatalf("missing video transceiver error = %v, want %v", err, ErrNoStreamVideoTransceiver)
	}
}

func TestSendStreamVideoRTPPreservesSourcePacketAndUpdatesStats(t *testing.T) {
	peer, cleanup := newPeerWithOfferedStreamVideo(t)
	defer cleanup()

	peer.rtpErrStreak.Store(7)
	peer.rtpErrSinceNs.Store(123)
	packet := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         true,
			PayloadType:    112,
			SequenceNumber: 32123,
			Timestamp:      0x12345678,
			SSRC:           0x87654321,
		},
		Payload: []byte{0x65, 0x01, 0x02, 0x03},
	}
	before := packet.Clone()

	if err := peer.SendStreamVideoRTP(packet); err != nil {
		t.Fatalf("send stream video RTP: %v", err)
	}
	if !reflect.DeepEqual(packet, before) {
		t.Fatalf("source packet mutated:\n got: %+v\nwant: %+v", packet, before)
	}
	if got := peer.PacketsSent.Load(); got != 1 {
		t.Fatalf("PacketsSent = %d, want 1", got)
	}
	if got := peer.BytesSent.Load(); got != int64(packet.MarshalSize()) {
		t.Fatalf("BytesSent = %d, want %d", got, packet.MarshalSize())
	}
	if got := peer.rtpErrStreak.Load(); got != 0 {
		t.Fatalf("RTP error streak = %d, want 0", got)
	}
	if got := peer.rtpErrSinceNs.Load(); got != 0 {
		t.Fatalf("RTP error start = %d, want 0", got)
	}
}

func TestSendStreamVideoRTPErrorPaths(t *testing.T) {
	peer := &Peer{}
	if err := peer.SendStreamVideoRTP(nil); !errors.Is(err, ErrNilRTPPacket) {
		t.Fatalf("nil packet error = %v, want %v", err, ErrNilRTPPacket)
	}
	if err := peer.SendStreamVideoRTP(&rtp.Packet{}); !errors.Is(err, ErrNoStreamVideoTrack) {
		t.Fatalf("missing track error = %v, want %v", err, ErrNoStreamVideoTrack)
	}
	if peer.PacketsSent.Load() != 0 ||
		peer.PacketsDropped.Load() != 0 ||
		peer.RTPWriteErrors.Load() != 0 {
		t.Fatal("pre-write validation errors changed RTP statistics")
	}
}

func newPeerWithOfferedStreamVideo(t *testing.T) (*Peer, func()) {
	t.Helper()

	api := newTestAPI()
	viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	if _, err := viewerPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		viewerPC.Close()
		t.Fatalf("add viewer video transceiver: %v", err)
	}
	offer, err := viewerPC.CreateOffer(nil)
	if err != nil {
		viewerPC.Close()
		t.Fatalf("create viewer offer: %v", err)
	}
	if err := viewerPC.SetLocalDescription(offer); err != nil {
		viewerPC.Close()
		t.Fatalf("set viewer local description: %v", err)
	}

	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		viewerPC.Close()
		t.Fatalf("create server PC: %v", err)
	}
	if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
		serverPC.Close()
		viewerPC.Close()
		t.Fatalf("set server remote description: %v", err)
	}

	peer := &Peer{PC: serverPC}
	if _, err := peer.AddStreamVideoTrack(); err != nil {
		serverPC.Close()
		viewerPC.Close()
		t.Fatalf("add stream video track: %v", err)
	}

	return peer, func() {
		peer.closeStreamVideo()
		serverPC.Close()
		viewerPC.Close()
	}
}

// TestStopPhantomTransceivers asserts that StopPhantomTransceivers stops
// only the sendrecv "phantom" transceivers Pion's undeclared-SSRC handler
// auto-creates, while preserving:
//   - the outgoing sendonly track we explicitly added
//   - the recvonly transceiver carrying the client's incoming audio
//
// Insertion order intentionally puts the phantoms BEFORE the recvonly:
// the previous implementation relied on iteration order via a
// "firstAudioSeen" fallback and would have killed the recvonly here.
func TestStopPhantomTransceivers(t *testing.T) {
	api := newTestAPI()

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	// 1) Two phantoms first (sendrecv, no associated track)
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv}); err != nil {
		t.Fatalf("add phantom1: %v", err)
	}
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv}); err != nil {
		t.Fatalf("add phantom2: %v", err)
	}

	// 2) An explicit outgoing sendonly track (should be kept)
	outTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "sender-abc",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pc.AddTransceiverFromTrack(outTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
		t.Fatalf("add sendonly: %v", err)
	}

	// 3) The legit recvonly LAST (would be killed by the old order-dependent code)
	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatalf("add recvonly: %v", err)
	}

	peer := &Peer{
		UserID: "test-user",
		PC:     pc,
		logger: logging.New("debug"),
		OutgoingTracks: map[string]*outgoingTrack{
			"sender-abc": {track: outTrack, ssrc: 12345},
		},
	}

	if before := countActiveAudio(pc); before != 4 {
		t.Fatalf("expected 4 active audio transceivers before, got %d", before)
	}

	peer.StopPhantomTransceivers()

	if after := countActiveAudio(pc); after != 2 {
		t.Errorf("expected 2 active audio transceivers after, got %d", after)
	}

	// Survivors must be exactly recvonly + sendonly.
	var dirs []string
	for _, tr := range pc.GetTransceivers() {
		if tr.Kind() != webrtc.RTPCodecTypeAudio {
			continue
		}
		if tr.Direction() == webrtc.RTPTransceiverDirectionInactive {
			continue
		}
		dirs = append(dirs, tr.Direction().String())
	}
	sort.Strings(dirs)
	expected := []string{"recvonly", "sendonly"}
	if !reflect.DeepEqual(dirs, expected) {
		t.Errorf("expected surviving directions %v, got %v", expected, dirs)
	}
}

// TestStopPhantomTransceivers_RepeatedCalls protects against transceiver
// accumulation across multiple renegotiations. Each call should leave the
// recvonly + sendonly intact and stop any newly-added sendrecv phantoms.
func TestStopPhantomTransceivers_RepeatedCalls(t *testing.T) {
	api := newTestAPI()

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
		t.Fatalf("add recvonly: %v", err)
	}
	outTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "sender-abc",
	)
	if err != nil {
		t.Fatalf("create track: %v", err)
	}
	if _, err := pc.AddTransceiverFromTrack(outTrack,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly}); err != nil {
		t.Fatalf("add sendonly: %v", err)
	}

	peer := &Peer{
		UserID: "test-user",
		PC:     pc,
		logger: logging.New("debug"),
		OutgoingTracks: map[string]*outgoingTrack{
			"sender-abc": {track: outTrack, ssrc: 12345},
		},
	}

	for round := 0; round < 3; round++ {
		// Each renegotiation cycle imagines Pion auto-creating one new phantom.
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv}); err != nil {
			t.Fatalf("round %d add phantom: %v", round, err)
		}
		peer.StopPhantomTransceivers()
		if got := countActiveAudio(pc); got != 2 {
			t.Errorf("round %d: expected 2 active audio transceivers, got %d", round, got)
		}
	}
}

// A client whose offer omits `a=ssrc` on its audio m-line gets its microphone
// routed through Pion's undeclared-SSRC handler, which creates a *sendrecv*
// transceiver with a Pion-generated send track. That is the same shape as a
// phantom but it is the only thing carrying the member's voice, so stopping it
// silenced that member on every join/leave.
func TestUndeclaredSSRCMicTransceiverIsNotAPhantom(t *testing.T) {
	ourOutgoing, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "sender-abc",
	)
	if err != nil {
		t.Fatalf("create outgoing track: %v", err)
	}
	// Pion generates its own send track for the transceiver it auto-creates.
	pionGenerated, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"SZUTBMKfphCWunpU", "SZUTBMKfphCWunpU",
	)
	if err != nil {
		t.Fatalf("create pion track: %v", err)
	}
	outgoing := []*webrtc.TrackLocalStaticRTP{ourOutgoing}

	liveMic := &webrtc.TrackRemote{}
	staleMic := &webrtc.TrackRemote{}

	cases := []struct {
		name        string
		dir         webrtc.RTPTransceiverDirection
		senderTrack webrtc.TrackLocal
		receiver    *webrtc.TrackRemote
		liveInbound *webrtc.TrackRemote
		isSlot      bool
		wantPhantom bool
	}{
		{
			name:        "undeclared-ssrc mic transceiver is preserved",
			dir:         webrtc.RTPTransceiverDirectionSendrecv,
			senderTrack: pionGenerated,
			receiver:    liveMic,
			liveInbound: liveMic,
			wantPhantom: false,
		},
		{
			name:        "superseded duplicate is still stopped",
			dir:         webrtc.RTPTransceiverDirectionSendrecv,
			senderTrack: pionGenerated,
			receiver:    staleMic,
			liveInbound: liveMic,
			wantPhantom: true,
		},
		{
			name:        "true phantom with no inbound audio yet",
			dir:         webrtc.RTPTransceiverDirectionSendrecv,
			senderTrack: pionGenerated,
			receiver:    nil,
			liveInbound: nil,
			wantPhantom: true,
		},
		{
			name:        "our own outgoing track answered sendrecv is preserved",
			dir:         webrtc.RTPTransceiverDirectionSendrecv,
			senderTrack: ourOutgoing,
			receiver:    nil,
			liveInbound: liveMic,
			wantPhantom: false,
		},
		{
			name:        "recvonly client m-line is never a phantom",
			dir:         webrtc.RTPTransceiverDirectionRecvonly,
			senderTrack: nil,
			receiver:    liveMic,
			liveInbound: liveMic,
			wantPhantom: false,
		},
		{
			name:        "sendonly outgoing leg is never a phantom",
			dir:         webrtc.RTPTransceiverDirectionSendonly,
			senderTrack: ourOutgoing,
			receiver:    nil,
			liveInbound: liveMic,
			wantPhantom: false,
		},
		{
			name:        "receive slot answered sendrecv is preserved",
			dir:         webrtc.RTPTransceiverDirectionSendrecv,
			senderTrack: pionGenerated,
			receiver:    nil,
			liveInbound: liveMic,
			isSlot:      true,
			wantPhantom: false,
		},
		{
			name:        "already stopped transceiver is left alone",
			dir:         webrtc.RTPTransceiverDirectionInactive,
			senderTrack: pionGenerated,
			receiver:    staleMic,
			liveInbound: liveMic,
			wantPhantom: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isPhantomAudioTransceiver(
				tc.dir, tc.senderTrack, tc.receiver, outgoing, tc.liveInbound, tc.isSlot,
			)
			if got != tc.wantPhantom {
				t.Errorf("isPhantomAudioTransceiver = %v, want %v", got, tc.wantPhantom)
			}
		})
	}
}

// Pion re-fires OnTrack with a new TrackRemote that reuses the sender's SSRC.
// Keying the "already wired" check on SSRC made the SFU skip that live track
// while the dying reader for the old one was still counting errors, leaving the
// member with no reader at all.
func TestAudioWiringIdentityIsTrackNotSSRC(t *testing.T) {
	current := &webrtc.TrackRemote{}
	replacement := &webrtc.TrackRemote{}

	if current.SSRC() != replacement.SSRC() {
		t.Fatalf("test needs both tracks to share an SSRC, got %d and %d",
			current.SSRC(), replacement.SSRC())
	}

	peer := &Peer{
		UserID:          "test-user",
		logger:          logging.New("debug"),
		wiredAudioTrack: current,
		audioCancel:     func() {},
		audioReadActive: true,
	}

	if !peer.audioWiringIsCurrentLocked(current) {
		t.Error("the track already being read must be recognised as current")
	}
	if peer.audioWiringIsCurrentLocked(replacement) {
		t.Error("a replacement track sharing the SSRC must not be treated as current")
	}

	// Once the reader has exited, even the original track must rewire.
	peer.audioReadActive = false
	if peer.audioWiringIsCurrentLocked(current) {
		t.Error("a track whose reader has exited must not be treated as current")
	}
}

func countActiveAudio(pc *webrtc.PeerConnection) int {
	n := 0
	for _, t := range pc.GetTransceivers() {
		if t.Kind() == webrtc.RTPCodecTypeAudio && t.Direction() != webrtc.RTPTransceiverDirectionInactive {
			n++
		}
	}
	return n
}
