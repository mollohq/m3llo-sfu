package server

import (
	"reflect"
	"strings"
	"testing"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

func TestDelayedOldPeerConnectionRemovalCannotRemoveReplacement(t *testing.T) {
	api := newTestAPI()
	oldPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create old PC: %v", err)
	}
	defer oldPC.Close()
	replacementPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create replacement PC: %v", err)
	}
	defer replacementPC.Close()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	peer := &Peer{UserID: "host", Role: "host"}
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add host: %v", err)
	}
	oldGeneration := peer.installPeerConnection(oldPC)
	replacementGeneration := peer.installPeerConnection(replacementPC)

	server := &Server{}
	server.removePeerFromSessionForPC(peer, oldPC, oldGeneration)

	if !peer.isCurrentPeerConnection(replacementPC, replacementGeneration) {
		t.Fatal("old callback changed the replacement PeerConnection")
	}
	session.videoMu.Lock()
	currentHost := session.host
	session.videoMu.Unlock()
	if currentHost != peer || session.State() != SessionActive {
		t.Fatal("old callback removed the current stream membership")
	}
}

func TestSlowPeerEvictionUsesExactStreamMembership(t *testing.T) {
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	oldViewer := &Peer{UserID: "viewer", Role: "viewer"}
	oldViewer.closeOnce.Do(func() {})
	replacement := &Peer{UserID: "viewer", Role: "viewer"}
	if err := session.AddPeer(oldViewer); err != nil {
		t.Fatalf("add old viewer: %v", err)
	}
	if err := session.AddPeer(replacement); err != nil {
		t.Fatalf("replace viewer: %v", err)
	}

	removeSlowPeer(session, oldViewer)

	members := session.AllPeers()
	if len(members) != 1 || members[0] != replacement {
		t.Fatalf("slow eviction removed replacement membership: %v", members)
	}
}

func TestStreamRTPTelemetryIsSeparateAndAnomalous(t *testing.T) {
	previous := StreamRelaySnapshot{
		RTPInPackets:        10,
		RTPFanoutAttempts:   20,
		RTPForwardedPackets: 19,
	}
	current := previous
	current.RTPInPackets += 8
	current.RTPInBytes += 8_000
	current.RTPFanoutAttempts += 12
	current.RTPForwardedPackets += 9
	current.RTPForwardedBytes += 9_000
	current.RTPDroppedPackets += 3
	current.RTPDroppedBytes += 3_000
	current.RTPQueueDroppedPackets += 2
	current.RTPQueueDroppedBytes += 2_000
	current.RTPDuplicatePackets += 1

	fields, reasons := streamRTPTelemetry(current, previous, 2)

	if fields["rtp_in_pps"] != float64(4) ||
		fields["rtp_forwarded_pps"] != float64(4.5) ||
		fields["rtp_queue_drop_pps"] != float64(1) {
		t.Fatalf("unexpected RTP telemetry rates: %+v", fields)
	}
	if _, exists := fields["media_in_mps"]; exists {
		t.Fatal("RTP telemetry reused DataChannel field names")
	}
	if !reflect.DeepEqual(reasons, []string{"rtp_drop", "rtp_queue_drop"}) {
		t.Fatalf("RTP anomaly reasons = %v", reasons)
	}
	totals, ok := fields["rtp_totals"].(logging.D)
	if !ok || totals["dropped_packets"] != current.RTPDroppedPackets {
		t.Fatalf("unexpected RTP totals: %#v", fields["rtp_totals"])
	}
}

// A host that offers ULPFEC gets an answer without it, so it sends no parity.
// mello streaming-quality stage 4 removed the send-side FEC (01-SFU.md §6.1).
func TestSFUAnswerToHostOfferHasNoULPFEC(t *testing.T) {
	hostEngine := &webrtc.MediaEngine{}
	if err := hostEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: streamVideoCodecCapability(),
		PayloadType:        96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register host H.264: %v", err)
	}
	if err := hostEngine.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeUlpFEC, ClockRate: 90000},
		PayloadType:        127,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		t.Fatalf("register host ULPFEC: %v", err)
	}
	hostPC, err := webrtc.NewAPI(webrtc.WithMediaEngine(hostEngine)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	defer hostPC.Close()
	if _, err := hostPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly},
	); err != nil {
		t.Fatalf("add host video: %v", err)
	}
	offer, err := hostPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create host offer: %v", err)
	}
	if !strings.Contains(offer.SDP, "ulpfec/90000") {
		t.Fatalf("test host offer carries no ULPFEC:\n%s", offer.SDP)
	}

	sfuEngine, err := newSFUMediaEngine(true)
	if err != nil {
		t.Fatalf("SFU media engine: %v", err)
	}
	serverPC, err := webrtc.NewAPI(webrtc.WithMediaEngine(sfuEngine)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create server PC: %v", err)
	}
	defer serverPC.Close()
	if err := serverPC.SetRemoteDescription(offer); err != nil {
		t.Fatalf("apply host offer: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("create answer: %v", err)
	}
	if !strings.Contains(answer.SDP, "H264/90000") {
		t.Fatalf("answer lost H.264:\n%s", answer.SDP)
	}
	if strings.Contains(strings.ToLower(answer.SDP), "ulpfec") {
		t.Fatalf("answer negotiates ULPFEC:\n%s", answer.SDP)
	}
}
