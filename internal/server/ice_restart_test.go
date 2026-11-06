package server

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/pion/ice/v4"
	pionlogging "github.com/pion/logging"
	"github.com/pion/transport/v5/vnet"
	"github.com/pion/webrtc/v4"
)

// vnetLink is a virtual network with the SFU on one address and a client on
// another. block drops every packet between them.
type vnetLink struct {
	router  *vnet.Router
	sfuNet  *vnet.Net
	cliNet  *vnet.Net
	blocked atomic.Bool
}

func newVNetLink(t *testing.T) *vnetLink {
	t.Helper()
	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.9.0.0/24",
		LoggerFactory: pionlogging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatalf("vnet router: %v", err)
	}
	link := &vnetLink{router: router}
	link.sfuNet, err = vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.9.0.1"}})
	if err != nil {
		t.Fatalf("vnet SFU net: %v", err)
	}
	link.cliNet, err = vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.9.0.2"}})
	if err != nil {
		t.Fatalf("vnet client net: %v", err)
	}
	if err := router.AddNet(link.sfuNet); err != nil {
		t.Fatalf("add SFU net: %v", err)
	}
	if err := router.AddNet(link.cliNet); err != nil {
		t.Fatalf("add client net: %v", err)
	}
	router.AddChunkFilter(func(vnet.Chunk) bool { return !link.blocked.Load() })
	if err := router.Start(); err != nil {
		t.Fatalf("start vnet: %v", err)
	}
	t.Cleanup(func() { _ = router.Stop() })
	return link
}

func vnetSettings(n *vnet.Net) webrtc.SettingEngine {
	var se webrtc.SettingEngine
	se.SetNet(n)
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	se.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	return se
}

// applyNewSFUCandidates applies the SFU ice_candidate messages that the
// viewer has not applied yet.
func (v *streamTestViewer) applyNewSFUCandidates(t *testing.T) {
	t.Helper()
	candidates := v.probe.messages("ice_candidate")
	for ; v.candidates < len(candidates); v.candidates++ {
		var c ICECandidateData
		if err := json.Unmarshal(candidates[v.candidates].Data, &c); err != nil {
			t.Fatalf("candidate data: %v", err)
		}
		index := uint16(c.SDPMLineIndex)
		mid := c.SDPMid
		_ = v.pc.AddICECandidate(webrtc.ICECandidateInit{
			Candidate:     c.Candidate,
			SDPMid:        &mid,
			SDPMLineIndex: &index,
		})
	}
}

func (v *streamTestViewer) isMember() bool {
	v.session.videoMu.Lock()
	defer v.session.videoMu.Unlock()
	for _, viewer := range v.session.viewers.Snapshot() {
		if viewer == v.peer {
			return true
		}
	}
	return false
}

// When the selected candidate pair stops carrying packets, Pion reports
// Disconnected and then Failed, and the SFU sends an ICE restart offer on
// Failed. The peer stays a member through the restart, and the restart
// reconnects it (01-SFU.md §9.2). Before, the 5 s Disconnected timer removed
// the peer at least 20 s before Pion reported Failed (Pion's default
// timeouts), so the restart never ran.
func TestICERestartOnFailedKeepsThePeer(t *testing.T) {
	link := newVNetLink(t)
	sfuAPI, err := newSFUAPI(vnetSettings(link.sfuNet))
	if err != nil {
		t.Fatalf("SFU API: %v", err)
	}
	s := newStreamTestServer(sfuAPI)
	viewer := offerStreamTestViewer(t, s, "ice-restart", newTestAPIWithSettings(vnetSettings(link.cliNet)))
	viewer.connect(t)

	link.blocked.Store(true)
	blockedAt := time.Now()
	deadline := blockedAt.Add(iceDisconnectedTimeout + iceFailedAfterDisconnected + 6*time.Second)
	for viewer.probe.count("offer") == 0 {
		if !viewer.isMember() {
			t.Fatalf("the SFU removed the peer %v after the link went down, before an ICE restart",
				time.Since(blockedAt).Round(time.Millisecond))
		}
		if time.Now().After(deadline) {
			t.Fatal("the SFU sent no ICE restart offer")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !viewer.isMember() {
		t.Fatal("the SFU removed the peer when it sent the ICE restart offer")
	}

	link.blocked.Store(false)
	var offer SDPData
	if err := json.Unmarshal(viewer.probe.messages("offer")[0].Data, &offer); err != nil {
		t.Fatalf("offer data: %v", err)
	}
	if err := viewer.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offer.SDP,
	}); err != nil {
		t.Fatalf("apply restart offer: %v", err)
	}
	answer, err := viewer.pc.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("restart answer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(viewer.pc)
	if err := viewer.pc.SetLocalDescription(answer); err != nil {
		t.Fatalf("set restart answer: %v", err)
	}
	<-gathered
	data, _ := json.Marshal(SDPData{SDP: viewer.pc.LocalDescription().SDP})
	s.handleAnswer(viewer.peer, &auth.Claims{UserID: "viewer", Type: "stream"},
		SignalingMessage{Type: "answer", Data: data})

	sfuPC, _ := viewer.peer.currentPeerConnection()
	if sfuPC == nil {
		t.Fatal("the SFU dropped the viewer PeerConnection")
	}
	reconnect := time.Now().Add(iceRestartTimeout)
	for sfuPC.ConnectionState() != webrtc.PeerConnectionStateConnected {
		viewer.applyNewSFUCandidates(t)
		if time.Now().After(reconnect) {
			t.Fatalf("the ICE restart did not reconnect: SFU state %s", sfuPC.ConnectionState())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !viewer.isMember() {
		t.Fatal("the peer is not a member after the ICE restart")
	}
	if got := viewer.peer.ICERestartCount.Load(); got != 1 {
		t.Fatalf("ICE restarts = %d, want 1", got)
	}
}
