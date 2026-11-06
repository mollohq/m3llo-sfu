package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func TestSlotAllocatorAssignAndRelease(t *testing.T) {
	a := newSlotAllocator([]string{"s0", "s1", "s2"})

	if mid, ok := a.Assign("alice"); !ok || mid != "s0" {
		t.Fatalf("first assign = %q %v, want s0", mid, ok)
	}
	if mid, ok := a.Assign("bob"); !ok || mid != "s1" {
		t.Fatalf("second assign = %q %v, want s1", mid, ok)
	}
	if got := a.Owner("s1"); got != "bob" {
		t.Fatalf("owner of s1 = %q, want bob", got)
	}
	if mid, ok := a.Release("alice"); !ok || mid != "s0" {
		t.Fatalf("release = %q %v, want s0", mid, ok)
	}
	if got := a.Owner("s0"); got != "" {
		t.Fatalf("released slot still owned by %q", got)
	}
	if _, ok := a.Release("alice"); ok {
		t.Fatal("a second release of the same user must report no slot")
	}
	if a.Free() != 2 {
		t.Fatalf("free = %d, want 2", a.Free())
	}
}

func TestSlotAllocatorPrefersUnusedThenOldestReleased(t *testing.T) {
	a := newSlotAllocator([]string{"s0", "s1", "s2"})
	a.Assign("u0")
	a.Assign("u1")
	a.Release("u0") // s0 released first

	// A never-used slot comes before a released one.
	if mid, _ := a.Assign("u2"); mid != "s2" {
		t.Fatalf("assign = %q, want the unused s2", mid)
	}
	a.Release("u1") // s1 released second
	// Reassign: the slot released longest ago, so a late packet of the old
	// owner is least likely to land on the new one.
	if mid, _ := a.Assign("u3"); mid != "s0" {
		t.Fatalf("assign = %q, want s0 (released first)", mid)
	}
	if mid, _ := a.Assign("u4"); mid != "s1" {
		t.Fatalf("assign = %q, want s1", mid)
	}
}

func TestSlotAllocatorExhaustion(t *testing.T) {
	mids := make([]string, voiceRecvSlots)
	for i := range mids {
		mids[i] = fmt.Sprintf("s%d", i)
	}
	a := newSlotAllocator(mids)
	for i := 0; i < voiceRecvSlots; i++ {
		if _, ok := a.Assign(fmt.Sprintf("u%d", i)); !ok {
			t.Fatalf("assign %d failed below N", i)
		}
	}
	if _, ok := a.Assign("one-too-many"); ok {
		t.Fatal("assign past N must fail so the caller renegotiates")
	}
	if a.Free() != 0 {
		t.Fatalf("free = %d, want 0", a.Free())
	}
	a.Release("u5")
	if mid, ok := a.Assign("one-too-many"); !ok || mid != "s5" {
		t.Fatalf("assign after a release = %q %v, want s5", mid, ok)
	}
}

func TestSlotAllocatorReconnectOfTheSameUser(t *testing.T) {
	a := newSlotAllocator([]string{"s0", "s1"})
	first, _ := a.Assign("alice")

	// A reconnect whose setup runs before its own release keeps the slot.
	if again, ok := a.Assign("alice"); !ok || again != first {
		t.Fatalf("repeat assign = %q %v, want %q", again, ok, first)
	}
	if a.Free() != 1 {
		t.Fatalf("repeat assign took a second slot (free %d)", a.Free())
	}

	// The usual order: release on eviction, assign on the new connection.
	a.Release("alice")
	if mid, _ := a.Assign("alice"); mid != "s1" {
		t.Fatalf("reconnect = %q, want the unused s1", mid)
	}
	a.Release("alice")
	a.Assign("bob") // takes s0, released longest ago
	if mid, _ := a.Assign("alice"); mid != "s1" {
		t.Fatalf("reconnect = %q, want s1", mid)
	}
}

func TestSlotSeqStaysContinuousAcrossOwners(t *testing.T) {
	var s slotSeq
	for i, in := range []uint16{1000, 1001, 1002} {
		if out := s.next(in); out != in {
			t.Fatalf("first owner packet %d: %d -> %d, want pass-through", i, in, out)
		}
	}

	s.newOwner()
	if out := s.next(60000); out != 1003 {
		t.Fatalf("new owner first packet = %d, want 1003", out)
	}
	if out := s.next(60002); out != 1005 {
		t.Fatalf("gap inside an owner = %d, want 1005", out)
	}
	if out := s.next(60001); out != 1004 {
		t.Fatalf("reordered packet = %d, want 1004", out)
	}

	// The next owner reuses the first owner's numbers: still continuous.
	s.newOwner()
	if out := s.next(1000); out != 1006 {
		t.Fatalf("third owner first packet = %d, want 1006", out)
	}
}

func TestSlotSeqWraps(t *testing.T) {
	var s slotSeq
	s.next(65534)
	s.next(65535)
	s.newOwner()
	if out := s.next(7); out != 0 {
		t.Fatalf("continuation across the wrap = %d, want 0", out)
	}
	if out := s.next(8); out != 1 {
		t.Fatalf("next = %d, want 1", out)
	}
}

func TestParseSpeakingControl(t *testing.T) {
	cases := []struct {
		in     string
		on, ok bool
	}{
		{`{"type":"speaking","on":true}`, true, true},
		{`{"type":"speaking","on":false}`, false, true},
		{`{"type":"speaking"}`, false, false},
		{`{"type":"ping","ts":1}`, false, false},
		{`not json`, false, false},
	}
	for _, tc := range cases {
		on, ok := parseSpeakingControl([]byte(tc.in))
		if on != tc.on || ok != tc.ok {
			t.Errorf("parse(%s) = %v %v, want %v %v", tc.in, on, ok, tc.on, tc.ok)
		}
	}
}

func TestEncodeSlotMessage(t *testing.T) {
	if got := encodeSlotMessage("3", "u-1"); got != `{"type":"slot","mid":"3","user":"u-1"}` {
		t.Errorf("assign message = %s", got)
	}
	if got := encodeSlotMessage("3", ""); got != `{"type":"slot","mid":"3","user":null}` {
		t.Errorf("release message = %s", got)
	}
}

// controlRecorder collects the control-channel messages a peer sends.
type controlRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (r *controlRecorder) hook(msg string) bool {
	r.mu.Lock()
	r.msgs = append(r.msgs, msg)
	r.mu.Unlock()
	return true
}

func (r *controlRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.msgs...)
}

// voiceTestClient is a Pion PeerConnection shaped like the mello voice
// client: one sendrecv mic m-line with a declared SSRC and `slots` recvonly
// receive slots.
type voiceTestClient struct {
	pc  *webrtc.PeerConnection
	mic *webrtc.TrackLocalStaticRTP

	mu       sync.Mutex
	received map[string][]*rtp.Packet // mid -> packets
}

func newVoiceTestClient(t *testing.T, api *webrtc.API, name string, slots int) *voiceTestClient {
	t.Helper()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client PC: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	mic, err := webrtc.NewTrackLocalStaticRTP(voiceOpusCapability(), "audio", name)
	if err != nil {
		t.Fatalf("mic track: %v", err)
	}
	if _, err := pc.AddTransceiverFromTrack(mic, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendrecv,
	}); err != nil {
		t.Fatalf("mic transceiver: %v", err)
	}
	for i := 0; i < slots; i++ {
		if _, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionRecvonly,
		}); err != nil {
			t.Fatalf("slot transceiver: %v", err)
		}
	}
	c := &voiceTestClient{pc: pc, mic: mic, received: map[string][]*rtp.Packet{}}
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		mid := ""
		for _, tr := range pc.GetTransceivers() {
			if tr.Receiver() == receiver {
				mid = tr.Mid()
			}
		}
		go func() {
			for {
				pkt, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				c.mu.Lock()
				c.received[mid] = append(c.received[mid], pkt)
				c.mu.Unlock()
			}
		}()
	})
	return c
}

func (c *voiceTestClient) packets(mid string) []*rtp.Packet {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*rtp.Packet(nil), c.received[mid]...)
}

// connectVoicePeer negotiates the client's offer on serverPC the way
// applyOfferAndAnswerFor does for a voice session (slots bound before the
// answer) and waits until both sides are connected.
func connectVoicePeer(t *testing.T, api *webrtc.API, c *voiceTestClient, peer *Peer) *webrtc.PeerConnection {
	t.Helper()
	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("server PC: %v", err)
	}
	t.Cleanup(func() { _ = serverPC.Close() })
	peer.installPeerConnection(serverPC)

	offer, err := c.pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(c.pc)
	if err := c.pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("client local: %v", err)
	}
	<-gathered
	if err := serverPC.SetRemoteDescription(*c.pc.LocalDescription()); err != nil {
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
	if err := c.pc.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
		t.Fatalf("client remote: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for serverPC.ConnectionState() != webrtc.PeerConnectionStateConnected ||
		c.pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
		if time.Now().After(deadline) {
			t.Fatalf("no connection: server %s client %s",
				serverPC.ConnectionState(), c.pc.ConnectionState())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return serverPC
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sendVoicePackets(peer *Peer, sender string, firstSeq uint16, n int, marker byte) {
	for i := 0; i < n; i++ {
		peer.SendRTP(sender, &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    111,
				SequenceNumber: firstSeq + uint16(i),
				Timestamp:      uint32(i) * 960,
				SSRC:           0xdeadbeef,
			},
			Payload: []byte{marker, 0xff, 0xfe},
		})
		time.Sleep(2 * time.Millisecond)
	}
}

// The Pion spike as a regression test: a slot bound at answer time swaps
// owners with ReplaceTrack, with no renegotiation and a fixed SSRC, and the
// receiver hears a new owner that reuses the old owner's sequence numbers.
func TestVoiceSlotsSwapOwnersWithoutRenegotiation(t *testing.T) {
	api := newTestAPI()
	const slots = 3
	client := newVoiceTestClient(t, api, "rx", slots)
	rec := &controlRecorder{}
	peer := &Peer{UserID: "rx", logger: logging.New("error"), controlTextHook: rec.hook}
	serverPC := connectVoicePeer(t, api, client, peer)

	var negotiationNeeded atomic.Int32
	serverPC.OnNegotiationNeeded(func() { negotiationNeeded.Add(1) })

	if got := peer.VoiceSlotCount(); got != slots {
		t.Fatalf("bound %d slots, want %d", got, slots)
	}
	for _, tr := range serverPC.GetTransceivers() {
		if tr.Mid() == "0" && tr.Sender() != nil && tr.Sender().Track() != nil {
			t.Fatal("the mic m-line must not carry a slot sender")
		}
	}

	if err := peer.AssignVoiceSlot("alice"); err != nil {
		t.Fatalf("assign alice: %v", err)
	}
	peer.tracksMu.RLock()
	aliceSlot := peer.OutgoingTracks["alice"].slot
	peer.tracksMu.RUnlock()
	slotMid, slotSSRC := aliceSlot.mid, aliceSlot.ssrc

	sendVoicePackets(peer, "alice", 100, 20, 0xA)
	waitFor(t, "alice's packets", func() bool { return len(client.packets(slotMid)) >= 20 })
	for _, pkt := range client.packets(slotMid) {
		if pkt.SSRC != slotSSRC {
			t.Fatalf("packet SSRC %d, want the slot SSRC %d", pkt.SSRC, slotSSRC)
		}
	}

	if peer.RemoveOutgoingTrack("alice") {
		t.Fatal("releasing a slot must not ask for a renegotiation")
	}
	sendVoicePackets(peer, "alice", 120, 10, 0xA) // dropped: alice has no track
	if err := peer.AssignVoiceSlot("bob"); err != nil {
		t.Fatalf("assign bob: %v", err)
	}
	if err := peer.AssignVoiceSlot("carol"); err != nil {
		t.Fatalf("assign carol: %v", err)
	}
	// dave gets the slot alice released: the only free one.
	if err := peer.AssignVoiceSlot("dave"); err != nil {
		t.Fatalf("assign dave: %v", err)
	}
	peer.tracksMu.RLock()
	daveSlot := peer.OutgoingTracks["dave"].slot
	peer.tracksMu.RUnlock()
	if daveSlot != aliceSlot {
		t.Fatalf("dave got slot %s, want alice's %s", daveSlot.mid, slotMid)
	}
	if err := peer.AssignVoiceSlot("erin"); err != ErrNoVoiceSlot {
		t.Fatalf("assign past the slots = %v, want ErrNoVoiceSlot", err)
	}

	// dave reuses alice's sequence numbers. Without the slot rewrite, SRTP
	// replay protection on the receiver drops all of them.
	sendVoicePackets(peer, "dave", 100, 20, 0xD)
	waitFor(t, "dave's packets", func() bool { return len(client.packets(slotMid)) >= 40 })

	got := client.packets(slotMid)
	for i, pkt := range got[:20] {
		if pkt.Payload[0] != 0xA {
			t.Fatalf("packet %d before the swap is not alice's", i)
		}
	}
	for i, pkt := range got[20:] {
		if pkt.Payload[0] != 0xD {
			t.Fatalf("packet %d after the swap is not dave's (alice leaked after release?)", i)
		}
		if pkt.SSRC != slotSSRC {
			t.Fatalf("slot SSRC changed across owners: %d -> %d", slotSSRC, pkt.SSRC)
		}
		if want := got[19].SequenceNumber + uint16(i+1); pkt.SequenceNumber != want {
			t.Fatalf("slot sequence not continuous: got %d want %d", pkt.SequenceNumber, want)
		}
	}

	if n := negotiationNeeded.Load(); n != 0 {
		t.Fatalf("slot swaps raised negotiationneeded %d times", n)
	}
	if serverPC.SignalingState() != webrtc.SignalingStateStable {
		t.Fatalf("signaling state %s, want stable", serverPC.SignalingState())
	}
	if peer.RenegotiationCount.Load() != 0 {
		t.Fatalf("renegotiated %d times", peer.RenegotiationCount.Load())
	}

	wantMsgs := []string{
		encodeSlotMessage(slotMid, "alice"),
		encodeSlotMessage(slotMid, ""),
	}
	msgs := rec.all()
	for i, want := range wantMsgs {
		if i >= len(msgs) || msgs[i] != want {
			t.Fatalf("control message %d = %v, want %s", i, msgs, want)
		}
	}
	if last := msgs[len(msgs)-1]; last != encodeSlotMessage(slotMid, "dave") {
		t.Fatalf("last control message = %s, want dave on %s", last, slotMid)
	}
}

// wsProbe is the client end of a peer's signaling socket: it records
// every message the SFU sends.
type wsProbe struct {
	server *websocket.Conn
	mu     sync.Mutex
	types  []string
	msgs   []SignalingMessage
}

func newWSProbe(t *testing.T) *wsProbe {
	t.Helper()
	serverConn := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- c
	}))
	t.Cleanup(ts.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial probe: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	p := &wsProbe{server: <-serverConn}
	go func() {
		for {
			_, data, err := client.ReadMessage()
			if err != nil {
				return
			}
			var msg SignalingMessage
			if json.Unmarshal(data, &msg) == nil {
				p.mu.Lock()
				p.types = append(p.types, msg.Type)
				p.msgs = append(p.msgs, msg)
				p.mu.Unlock()
			}
		}
	}()
	return p
}

func (p *wsProbe) messages(msgType string) []SignalingMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []SignalingMessage
	for _, m := range p.msgs {
		if m.Type == msgType {
			out = append(out, m)
		}
	}
	return out
}

func (p *wsProbe) count(msgType string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, t := range p.types {
		if t == msgType {
			n++
		}
	}
	return n
}

type voiceTestMember struct {
	peer  *Peer
	probe *wsProbe
	rec   *controlRecorder
}

func joinVoiceTestMember(
	t *testing.T,
	api *webrtc.API,
	session *VoiceSession,
	name string,
	slots int,
) *voiceTestMember {
	t.Helper()
	probe := newWSProbe(t)
	rec := &controlRecorder{}
	peer := NewPeer(name, "member", probe.server, logging.New("error"))
	peer.controlTextHook = rec.hook
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	peer.bindSession(session)
	client := newVoiceTestClient(t, api, name, slots)
	connectVoicePeer(t, api, client, peer)
	if _, _, needsReneg := session.SetupOutgoingTracks(peer); needsReneg {
		if err := peer.Renegotiate(); err != nil {
			t.Fatalf("renegotiate %s: %v", name, err)
		}
	}
	return &voiceTestMember{peer: peer, probe: probe, rec: rec}
}

// Joins and leaves within the slots send no offer to anyone. Past the slots
// the SFU falls back to a new m-line and a renegotiation.
func TestVoiceJoinWithinSlotsSendsNoOffer(t *testing.T) {
	api := newTestAPI()
	session := NewVoiceSession("voice:c:ch", "c", "ch", logging.New("error"))
	const slots = 1

	alice := joinVoiceTestMember(t, api, session, "alice", slots)
	bob := joinVoiceTestMember(t, api, session, "bob", slots)

	// Each holds the other in its only slot.
	time.Sleep(100 * time.Millisecond)
	if n := alice.probe.count("offer") + bob.probe.count("offer"); n != 0 {
		t.Fatalf("a join within the slots sent %d offers", n)
	}
	if alice.peer.RenegotiationCount.Load()+bob.peer.RenegotiationCount.Load() != 0 {
		t.Fatal("a join within the slots renegotiated")
	}

	// carol is past alice's and bob's single slot: both renegotiate once.
	carol := joinVoiceTestMember(t, api, session, "carol", slots)
	waitFor(t, "offers past the slots", func() bool {
		return alice.probe.count("offer") == 1 && bob.probe.count("offer") == 1
	})
	// carol has one slot for two senders: one renegotiation for the second.
	waitFor(t, "carol's offer", func() bool { return carol.probe.count("offer") == 1 })

	// The overflow m-line is announced with its mid once the offer exists.
	announced := func(rec *controlRecorder, user string) bool {
		for _, m := range rec.all() {
			if strings.Contains(m, `"user":"`+user+`"`) {
				return true
			}
		}
		return false
	}
	if !announced(alice.rec, "carol") || !announced(bob.rec, "carol") {
		t.Fatalf("overflow mid not announced: alice %v bob %v", alice.rec.all(), bob.rec.all())
	}

	// bob leaves: alice held bob in her slot, so she gets no offer.
	offersBefore := alice.probe.count("offer")
	session.RemovePeer("bob")
	time.Sleep(100 * time.Millisecond)
	if got := alice.probe.count("offer"); got != offersBefore {
		t.Fatalf("a slot release sent alice %d new offers", got-offersBefore)
	}
	msgs := alice.rec.all()
	if msgs[len(msgs)-1] != encodeSlotMessage("1", "") {
		t.Fatalf("alice's last control message = %s, want the release of slot 1", msgs[len(msgs)-1])
	}
}

func TestSetSpeakingFansOutToOtherMembers(t *testing.T) {
	session := NewVoiceSession("voice:c:ch", "c", "ch", logging.New("error"))
	members := map[string]*controlRecorder{}
	var alice *Peer
	for _, name := range []string{"alice", "bob", "carol"} {
		rec := &controlRecorder{}
		peer := NewPeer(name, "member", newWSProbe(t).server, logging.New("error"))
		peer.controlTextHook = rec.hook
		if err := session.AddPeer(peer); err != nil {
			t.Fatalf("add %s: %v", name, err)
		}
		members[name] = rec
		if name == "alice" {
			alice = peer
		}
	}

	session.SetSpeaking(alice, true)
	session.SetSpeaking(alice, true) // no change, no message

	want := encodeSpeakingMessage("alice", true)
	for _, name := range []string{"bob", "carol"} {
		if got := members[name].all(); len(got) != 1 || got[0] != want {
			t.Fatalf("%s got %v, want one %s", name, got, want)
		}
	}
	if got := members["alice"].all(); len(got) != 0 {
		t.Fatalf("the speaker got her own state back: %v", got)
	}
	if !alice.speaking.Load() {
		t.Fatal("the speaking flag for the blackout check is not set")
	}

	// A member whose control channel opens late gets the current state.
	late := &controlRecorder{}
	dave := NewPeer("dave", "member", newWSProbe(t).server, logging.New("error"))
	dave.controlTextHook = late.hook
	if err := session.AddPeer(dave); err != nil {
		t.Fatalf("add dave: %v", err)
	}
	session.SendControlSnapshot(dave)
	if got := late.all(); len(got) != 1 || got[0] != want {
		t.Fatalf("snapshot = %v, want %s", got, want)
	}

	session.SetSpeaking(alice, false)
	if got := members["bob"].all(); got[len(got)-1] != encodeSpeakingMessage("alice", false) {
		t.Fatalf("bob's last message = %s", got[len(got)-1])
	}
}

// The slot message of a new owner goes out before the SFU can forward the
// owner's first packet on the slot: at the time of the message the track is
// not in OutgoingTracks (SendRTP drops) and the slot still carries its
// placeholder.
func TestVoiceSlotMessagePrecedesForwarding(t *testing.T) {
	api := newTestAPI()
	client := newVoiceTestClient(t, api, "rx", 2)
	peer := &Peer{UserID: "rx", logger: logging.New("error")}
	var checked atomic.Int32
	peer.controlTextHook = func(msg string) bool {
		if !strings.Contains(msg, `"user":"alice"`) {
			return true
		}
		checked.Add(1)
		peer.tracksMu.RLock()
		ot := peer.OutgoingTracks["alice"]
		var placeholder bool
		for _, slot := range peer.voiceSlots {
			if strings.Contains(msg, `"mid":"`+slot.mid+`"`) {
				placeholder = slot.sender.Track() == webrtc.TrackLocal(slot.placeholder)
			}
		}
		peer.tracksMu.RUnlock()
		if ot != nil {
			t.Errorf("slot message sent after alice's track was live for forwarding")
		}
		if !placeholder {
			t.Errorf("slot message sent after the slot swapped to alice's track")
		}
		return true
	}
	connectVoicePeer(t, api, client, peer)

	if err := peer.AssignVoiceSlot("alice"); err != nil {
		t.Fatalf("assign alice: %v", err)
	}
	if checked.Load() != 1 {
		t.Fatalf("alice's slot message sent %d times, want 1", checked.Load())
	}
	peer.tracksMu.RLock()
	defer peer.tracksMu.RUnlock()
	if peer.OutgoingTracks["alice"] == nil {
		t.Fatal("alice is not forwarded after the assign")
	}
}
