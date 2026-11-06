package server

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

// iceTestHostIP returns a non-loopback IPv4 address of this host: Pion
// clients do not use loopback candidates, so the SFU must advertise one.
func iceTestHostIP(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Skipf("interface addresses: %v", err)
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String()
		}
	}
	t.Skip("no non-loopback IPv4 address")
	return ""
}

// newICETestServerAPI returns an API with the production ICE settings
// (ICE-lite, NAT 1:1, UDP mux and passive ICE-TCP mux on free ports).
func newICETestServerAPI(t *testing.T, publicIP string) *webrtc.API {
	t.Helper()
	se := webrtc.SettingEngine{}
	se.SetHandleUndeclaredSSRCWithoutAnswer(true)
	closeMux, err := configureICE(&se, publicIP, "0.0.0.0:0", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("configure ICE: %v", err)
	}
	t.Cleanup(closeMux)
	return newTestAPIWithSettings(se)
}

func newICETestClientAPI(networkTypes ...webrtc.NetworkType) *webrtc.API {
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes(networkTypes)
	return newTestAPIWithSettings(se)
}

func selectedPair(t *testing.T, pc *webrtc.PeerConnection) *webrtc.ICECandidatePair {
	t.Helper()
	pair, err := pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil || pair == nil {
		t.Fatalf("no selected candidate pair: %v", err)
	}
	return pair
}

// A client that can use only TCP connects to the SFU over passive ICE-TCP
// and receives voice RTP on a receive slot.
func TestVoiceOverPassiveICETCP(t *testing.T) {
	publicIP := iceTestHostIP(t)
	serverAPI := newICETestServerAPI(t, publicIP)
	client := newVoiceTestClient(t, newICETestClientAPI(webrtc.NetworkTypeTCP4), "rx", 1)
	peer := &Peer{UserID: "rx", logger: logging.New("error")}
	serverPC := connectVoicePeer(t, serverAPI, client, peer)

	answer := serverPC.LocalDescription().SDP
	if !strings.Contains(answer, publicIP) || !strings.Contains(answer, "tcptype passive") {
		t.Fatalf("answer has no passive TCP host candidate on %s:\n%s", publicIP, answer)
	}
	for _, line := range strings.Split(answer, "\n") {
		if strings.HasPrefix(line, "a=candidate") {
			t.Logf("SFU candidate: %s", strings.TrimSpace(line))
		}
	}
	if pair := selectedPair(t, serverPC); pair.Local.Protocol != webrtc.ICEProtocolTCP {
		t.Fatalf("selected pair is %s, want TCP", pair)
	} else {
		t.Logf("selected pair: %s", pair)
	}

	if err := peer.AssignVoiceSlot("alice"); err != nil {
		t.Fatalf("assign alice: %v", err)
	}
	var mid string
	for slotMid := range peer.voiceSlots {
		mid = slotMid // one slot
	}
	deadline := time.Now().Add(5 * time.Second)
	for seq := uint16(1); len(client.packets(mid)) < 10; seq += 5 {
		if time.Now().After(deadline) {
			t.Fatalf("client got %d voice packets over TCP on mid %s, want 10", len(client.packets(mid)), mid)
		}
		sendVoicePackets(peer, "alice", seq, 5, 0xa1)
		time.Sleep(20 * time.Millisecond)
	}
	if got := client.packets(mid)[0].Payload[0]; got != 0xa1 {
		t.Fatalf("payload marker %#x, want 0xa1", got)
	}
}

// maxCandidatePriority returns the highest priority of the SDP's candidates
// for the transport protocol ("udp" or "tcp"), or 0 when there is none.
func maxCandidatePriority(t *testing.T, sdp, protocol string) uint64 {
	t.Helper()
	var best uint64
	for _, line := range strings.Split(sdp, "\n") {
		if !strings.HasPrefix(line, "a=candidate:") {
			continue
		}
		// a=candidate:<foundation> <component> <protocol> <priority> ...
		fields := strings.Fields(line)
		if len(fields) < 4 || !strings.EqualFold(fields[2], protocol) {
			continue
		}
		priority, err := strconv.ParseUint(fields[3], 10, 32)
		if err != nil {
			t.Fatalf("candidate priority in %q: %v", line, err)
		}
		best = max(best, priority)
	}
	return best
}

// A client that can use UDP and TCP selects a UDP pair: ICE-TCP is the
// fallback, not the preferred path.
//
// The client is the controlling agent against the ICE-lite SFU, so its
// nomination decides the pair. The real client nominates by candidate
// priority, and UDP candidates have the higher priority on both sides. By
// default Pion nominates the first pair that becomes valid, and on a busy
// machine that can be the TCP pair. The test client waits one second before
// it nominates a host pair, so all pairs are checked and the valid pair with
// the highest priority wins, as with the real client.
func TestICEPrefersUDPWhenOpen(t *testing.T) {
	publicIP := iceTestHostIP(t)
	serverAPI := newICETestServerAPI(t, publicIP)
	clientSettings := webrtc.SettingEngine{}
	clientSettings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})
	clientSettings.SetHostAcceptanceMinWait(time.Second)
	client := newVoiceTestClient(t, newTestAPIWithSettings(clientSettings), "rx", 1)
	peer := &Peer{UserID: "rx", logger: logging.New("error")}
	serverPC := connectVoicePeer(t, serverAPI, client, peer)

	answer := serverPC.LocalDescription().SDP
	if !strings.Contains(answer, "udp") || !strings.Contains(answer, "tcptype passive") {
		t.Fatalf("answer lacks a UDP or a passive TCP candidate:\n%s", answer)
	}
	offer := client.pc.LocalDescription().SDP
	for side, sdp := range map[string]string{"SFU": answer, "client": offer} {
		udp, tcp := maxCandidatePriority(t, sdp, "udp"), maxCandidatePriority(t, sdp, "tcp")
		if udp == 0 || tcp == 0 || udp <= tcp {
			t.Fatalf("%s candidate priority: udp %d, tcp %d; want udp > tcp > 0:\n%s", side, udp, tcp, sdp)
		}
	}
	if pair := selectedPair(t, serverPC); pair.Local.Protocol != webrtc.ICEProtocolUDP {
		t.Fatalf("server selected pair is %s, want UDP", pair)
	}
	if pair := selectedPair(t, client.pc); pair.Local.Protocol != webrtc.ICEProtocolUDP {
		t.Fatalf("client selected pair is %s, want UDP", pair)
	}
}

// The passive TCP candidate carries SFU_PUBLIC_IP, not the local address.
func TestICETCPCandidateUsesPublicIP(t *testing.T) {
	const publicIP = "203.0.113.7"
	serverAPI := newICETestServerAPI(t, publicIP)
	client := newVoiceTestClient(t, newICETestClientAPI(webrtc.NetworkTypeTCP4), "rx", 1)
	serverPC, err := serverAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("server PC: %v", err)
	}
	t.Cleanup(func() { _ = serverPC.Close() })
	offer, err := client.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if err := client.pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("client local: %v", err)
	}
	if err := serverPC.SetRemoteDescription(offer); err != nil {
		t.Fatalf("server remote: %v", err)
	}
	answer, err := serverPC.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("answer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(serverPC)
	if err := serverPC.SetLocalDescription(answer); err != nil {
		t.Fatalf("server local: %v", err)
	}
	<-gathered
	var tcp []string
	for _, line := range strings.Split(serverPC.LocalDescription().SDP, "\n") {
		if strings.HasPrefix(line, "a=candidate") && strings.Contains(line, " tcp ") {
			tcp = append(tcp, strings.TrimSpace(line))
		}
	}
	if len(tcp) == 0 {
		t.Fatal("answer has no TCP candidate")
	}
	for _, line := range tcp {
		if !strings.Contains(line, " "+publicIP+" ") || !strings.Contains(line, "typ host tcptype passive") {
			t.Fatalf("TCP candidate %q is not a passive host candidate on %s", line, publicIP)
		}
	}
}
