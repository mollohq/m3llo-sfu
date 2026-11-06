package server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// closeTestMember is a voice member whose client sends RTP on its mic, with
// the SFU reading it in WireAudioTrack.
func closeTestMember(t *testing.T, api *webrtc.API, session *VoiceSession, name string, slots int) (*Peer, *voiceTestClient, *webrtc.PeerConnection) {
	t.Helper()
	probe := newWSProbe(t)
	peer := NewPeer(name, "member", probe.server, logging.New("error"))
	peer.controlTextHook = (&controlRecorder{}).hook
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	peer.bindSession(session)
	client := newVoiceTestClient(t, api, name, slots)
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("server PC: %v", err)
	}
	t.Cleanup(func() { _ = serverPC.Close() })
	serverPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		session.WireAudioTrack(peer, track)
	})
	// connectVoicePeer installs a PeerConnection of its own: use its
	// negotiation steps on serverPC instead.
	peer.installPeerConnection(serverPC)
	offer, err := client.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(client.pc)
	if err := client.pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("client local: %v", err)
	}
	<-gathered
	if err := serverPC.SetRemoteDescription(*client.pc.LocalDescription()); err != nil {
		t.Fatalf("server remote: %v", err)
	}
	if _, err := peer.BindVoiceSlots(api, serverPC); err != nil {
		t.Fatalf("bind slots: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	gathered = webrtc.GatheringCompletePromise(serverPC)
	if err := serverPC.SetLocalDescription(answer); err != nil {
		t.Fatalf("server local: %v", err)
	}
	<-gathered
	if err := client.pc.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
		t.Fatalf("client remote: %v", err)
	}
	waitFor(t, name+" connected", func() bool {
		return serverPC.ConnectionState() == webrtc.PeerConnectionStateConnected
	})
	return peer, client, serverPC
}

func startMicRTP(t *testing.T, client *voiceTestClient) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	t.Cleanup(func() { close(stop); <-done })
	go func() {
		defer close(done)
		seq := uint16(0)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				_ = client.mic.WriteRTP(&rtp.Packet{
					Header:  rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: uint32(seq) * 960},
					Payload: []byte{0xfc, 0xff, 0xfe},
				})
				seq++
			}
		}
	}()
}

// Peer.Close stops the audio reader before it closes the PeerConnection, so
// no reader runs on a receiver that pc.Close is stopping. Close returns with
// the reader gone.
func TestPeerCloseStopsAudioReaderBeforePeerConnectionClose(t *testing.T) {
	api := newTestAPI()
	session := NewVoiceSession("voice:c:ch", "c", "ch", logging.New("error"))
	for i := 0; i < 5; i++ {
		peer, client, serverPC := closeTestMember(t, api, session, fmt.Sprintf("m%d", i), 2)
		startMicRTP(t, client)
		waitFor(t, "audio reader", func() bool {
			peer.tracksMu.RLock()
			defer peer.tracksMu.RUnlock()
			return peer.audioReadActive && peer.PacketsReceived.Load() > 5
		})

		peer.Close()

		peer.tracksMu.RLock()
		active := peer.audioReadActive
		peer.tracksMu.RUnlock()
		if active {
			t.Fatalf("round %d: the audio reader still runs after Close", i)
		}
		if serverPC.ConnectionState() != webrtc.PeerConnectionStateClosed {
			t.Fatalf("round %d: PeerConnection state %s after Close", i, serverPC.ConnectionState())
		}
	}
}

// After Close no slot change touches the closed PeerConnection: a slot
// assign finds no slot, a bind finds no PeerConnection.
func TestNoSlotChangeAfterPeerClose(t *testing.T) {
	api := newTestAPI()
	session := NewVoiceSession("voice:c:ch", "c", "ch", logging.New("error"))
	peer, _, serverPC := closeTestMember(t, api, session, "rx", 3)
	if err := peer.AssignVoiceSlot("alice"); err != nil {
		t.Fatalf("assign before close: %v", err)
	}

	peer.Close()

	if err := peer.AssignVoiceSlot("bob"); err != ErrNoVoiceSlot {
		t.Fatalf("assign after close = %v, want ErrNoVoiceSlot", err)
	}
	if peer.RemoveOutgoingTrack("alice") {
		t.Fatal("a release after close asked for a renegotiation")
	}
	if _, err := peer.BindVoiceSlots(api, serverPC); err != ErrNoPeerConnection {
		t.Fatalf("bind after close = %v, want ErrNoPeerConnection", err)
	}
}

// Slot changes from other members' joins and leaves run while the receiver
// closes. Run with -race.
func TestPeerCloseRacesSlotChanges(t *testing.T) {
	api := newTestAPI()
	session := NewVoiceSession("voice:c:ch", "c", "ch", logging.New("error"))
	for round := 0; round < 5; round++ {
		peer, client, _ := closeTestMember(t, api, session, fmt.Sprintf("rx%d", round), 4)
		startMicRTP(t, client)
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 50; i++ {
					sender := fmt.Sprintf("s%d-%d", g, i%3)
					_ = peer.AssignVoiceSlot(sender)
					sendVoicePackets(peer, sender, uint16(i), 1, 0x1)
					peer.RemoveOutgoingTrack(sender)
				}
			}(g)
		}
		time.Sleep(time.Duration(round) * 5 * time.Millisecond)
		peer.Close()
		wg.Wait()
		session.RemovePeer(peer.UserID)
	}
}
