package server

import (
	"errors"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

// answerOffer applies the SFU's offer on the test client and returns the
// client's answer, as the mello client does on a renegotiation offer.
func (c *voiceTestClient) answerOffer(t *testing.T, offer webrtc.SessionDescription) string {
	t.Helper()
	if err := c.pc.SetRemoteDescription(offer); err != nil {
		t.Fatalf("client remote offer: %v", err)
	}
	answer, err := c.pc.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("client answer: %v", err)
	}
	if err := c.pc.SetLocalDescription(answer); err != nil {
		t.Fatalf("client local answer: %v", err)
	}
	return answer.SDP
}

func pendingOffer(t *testing.T, pc *webrtc.PeerConnection) webrtc.SessionDescription {
	t.Helper()
	if pc.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("signaling state %s, want have-local-offer", pc.SignalingState())
	}
	return *pc.PendingLocalDescription()
}

// Past the receive slots, two renegotiations on one peer can collide: the
// second starts while the client has not answered the first. The second must
// be queued and run when the answer applies. Before the fix it failed with
// have-local-offer and was never retried, and Pion still started the new
// sender: its packets reached the client on an SSRC no m-line declared.
func TestRenegotiationQueuedWhileOfferPendingLandsAfterAnswer(t *testing.T) {
	api := newTestAPI()
	logger := logging.New("error")
	client := newVoiceTestClient(t, api, "rx", 0) // no slots: own m-lines only
	probe := newWSProbe(t)
	peer := NewPeer("rx", "member", probe.server, logger)
	peer.controlTextHook = (&controlRecorder{}).hook
	serverPC := connectVoicePeer(t, api, client, peer)
	s := &Server{Logger: logger}
	claims := &auth.Claims{UserID: "rx", Type: "voice"}
	answer := func(sdp string) {
		s.handleAnswer(peer, claims, SignalingMessage{Type: "answer", Data: mustMarshal(SDPData{SDP: sdp})})
	}

	if _, err := peer.AddOutgoingTrack("alice"); err != nil {
		t.Fatalf("add alice: %v", err)
	}
	if err := peer.Renegotiate(); err != nil {
		t.Fatalf("first renegotiation: %v", err)
	}
	offer1 := pendingOffer(t, serverPC)

	// bob joins before the client answered the offer for alice.
	if _, err := peer.AddOutgoingTrack("bob"); err != nil {
		t.Fatalf("add bob: %v", err)
	}
	if err := peer.Renegotiate(); err != nil {
		t.Errorf("renegotiation while an offer is pending = %v, want it queued", err)
	}
	if got := probe.count("offer"); got > 1 {
		t.Fatalf("sent %d offers while one was pending", got)
	}

	answer(client.answerOffer(t, offer1))
	// Packets the SFU writes between the two answers.
	sendVoicePackets(peer, "alice", 100, 10, 0xA)
	sendVoicePackets(peer, "bob", 100, 10, 0xB)

	waitFor(t, "the queued offer", func() bool {
		return serverPC.SignalingState() == webrtc.SignalingStateHaveLocalOffer
	})
	answer(client.answerOffer(t, pendingOffer(t, serverPC)))
	if serverPC.SignalingState() != webrtc.SignalingStateStable {
		t.Fatalf("signaling state %s after the second answer", serverPC.SignalingState())
	}

	peer.tracksMu.RLock()
	bobMid := peer.OutgoingTracks["bob"].mid
	aliceMid := peer.OutgoingTracks["alice"].mid
	peer.tracksMu.RUnlock()
	if bobMid == "" || aliceMid == "" || bobMid == aliceMid {
		t.Fatalf("own m-lines not announced: alice %q bob %q", aliceMid, bobMid)
	}

	sendVoicePackets(peer, "bob", 110, 20, 0xB)
	waitFor(t, "bob's packets on his m-line", func() bool {
		return len(client.packets(bobMid)) >= 20
	})
	for _, pkt := range client.packets(bobMid) {
		if pkt.Payload[0] != 0xB {
			t.Fatalf("a packet on bob's m-line is not bob's")
		}
	}
	if n := len(client.packets("")); n != 0 {
		t.Fatalf("%d packets arrived on an SSRC no m-line declares", n)
	}
	if got := peer.RenegotiationCount.Load(); got != 2 {
		t.Fatalf("renegotiation count %d, want 2", got)
	}
}

// A renegotiation queued behind an offer whose answer never comes runs
// again when the answer window times out.
func TestQueuedRenegotiationRunsOnAnswerTimeout(t *testing.T) {
	api := newTestAPI()
	client := newVoiceTestClient(t, api, "rx", 0)
	probe := newWSProbe(t)
	peer := NewPeer("rx", "member", probe.server, logging.New("error"))
	peer.controlTextHook = (&controlRecorder{}).hook
	serverPC := connectVoicePeer(t, api, client, peer)

	if _, err := peer.AddOutgoingTrack("alice"); err != nil {
		t.Fatalf("add alice: %v", err)
	}
	// The state a lost answer leaves behind once the PeerConnection is
	// stable again: a queued renegotiation and a running answer window.
	peer.renegMu.Lock()
	peer.renegQueued = serverPC
	peer.renegMu.Unlock()
	peer.beginRenegotiation(20 * time.Millisecond)

	waitFor(t, "the retried offer", func() bool { return probe.count("offer") == 1 })
	if serverPC.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("signaling state %s, want have-local-offer", serverPC.SignalingState())
	}
}

// An ICE restart while an offer waits for its answer fails before
// CreateOffer, which would restart ICE and mark new senders negotiated.
func TestICERestartWhileOfferPendingFailsWithoutAnOffer(t *testing.T) {
	api := newTestAPI()
	client := newVoiceTestClient(t, api, "rx", 0)
	probe := newWSProbe(t)
	peer := NewPeer("rx", "member", probe.server, logging.New("error"))
	peer.controlTextHook = (&controlRecorder{}).hook
	serverPC := connectVoicePeer(t, api, client, peer)

	if _, err := peer.AddOutgoingTrack("alice"); err != nil {
		t.Fatalf("add alice: %v", err)
	}
	if err := peer.Renegotiate(); err != nil {
		t.Fatalf("renegotiate: %v", err)
	}
	pending := pendingOffer(t, serverPC)
	if err := peer.RestartICE(); !errors.Is(err, ErrOfferPending) {
		t.Fatalf("ICE restart with an offer pending = %v, want ErrOfferPending", err)
	}
	if got := pendingOffer(t, serverPC); got.SDP != pending.SDP {
		t.Fatal("the pending offer changed")
	}
	if peer.ICERestartCount.Load() != 0 {
		t.Fatal("an ICE restart was counted")
	}
}
