package server

import (
	"strings"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// nackTestReader returns the packets written to it, one per Read.
type nackTestReader struct{ next []byte }

func (r *nackTestReader) Read(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
	return copy(b, r.next), a, nil
}

func nackSeqs(t *testing.T, pkts []rtcp.Packet) []uint16 {
	t.Helper()
	var out []uint16
	for _, pkt := range pkts {
		nack, ok := pkt.(*rtcp.TransportLayerNack)
		if !ok {
			t.Fatalf("not a NACK: %T", pkt)
		}
		for _, pair := range nack.Nacks {
			out = append(out, pair.PacketList()...)
		}
	}
	return out
}

// The generator asks for a lost packet at once, not again while its repair
// can still be on the way (1.5 round trips, measured from the first NACK to
// the repair), never again once the RTX repair came, and at most
// nackMaxPerPacket times. Pion's generator asked every 100 ms and never saw
// the RTX repairs (TestSFUAsksTheHostOnceForARepairedPacket).
func TestNACKGeneratorWaitsForTheRoundTripAndSeesRTXRepairs(t *testing.T) {
	clock := time.Unix(100, 0)
	g := newNACKGenerator("", func() time.Time { return clock })
	media, repair := &nackTestReader{}, &nackTestReader{}
	mediaRead := g.BindRemoteStream(&interceptor.StreamInfo{
		SSRC: 1, SSRCRetransmission: 2, MimeType: webrtc.MimeTypeH264,
		RTCPFeedback: []interceptor.RTCPFeedback{{Type: "nack"}},
	}, media)
	rtxRead := g.BindRemoteStream(&interceptor.StreamInfo{SSRC: 2, MimeType: webrtc.MimeTypeRTX}, repair)
	buf := make([]byte, 1500)
	sendMedia := func(seq uint16) {
		media.next, _ = (&rtp.Packet{Header: rtp.Header{Version: 2, SSRC: 1, SequenceNumber: seq}, Payload: []byte{1}}).Marshal()
		if _, _, err := mediaRead.Read(buf, nil); err != nil {
			t.Fatalf("read media: %v", err)
		}
	}
	sendRTX := func(osn uint16) {
		repair.next, _ = (&rtp.Packet{Header: rtp.Header{Version: 2, SSRC: 2, SequenceNumber: 7}, Payload: []byte{byte(osn >> 8), byte(osn), 1}}).Marshal()
		if _, _, err := rtxRead.Read(buf, nil); err != nil {
			t.Fatalf("read RTX: %v", err)
		}
	}
	due := func(after time.Duration) []uint16 {
		clock = clock.Add(after)
		return nackSeqs(t, g.due(clock))
	}

	for _, seq := range []uint16{10, 11, 13} {
		sendMedia(seq)
	}
	if got := due(0); len(got) != 1 || got[0] != 12 {
		t.Fatalf("first NACK %v, want [12]", got)
	}
	if got := due(100 * time.Millisecond); len(got) != 0 {
		t.Fatalf("asked again after 100 ms with no round trip known: %v", got)
	}
	clock = clock.Add(-20 * time.Millisecond) // the repair at 80 ms
	sendRTX(12)
	if got := due(time.Second); len(got) != 0 {
		t.Fatalf("asked again after the RTX repair: %v", got)
	}
	if st := g.stats(); st.Repaired != 1 || st.RTTMs != 80 {
		t.Fatalf("stats %+v, want 1 repaired and an 80 ms round trip", st)
	}

	// A packet that is never repaired: asked again after 1.5 x 80 ms.
	sendMedia(15)
	if got := due(0); len(got) != 1 || got[0] != 14 {
		t.Fatalf("first NACK %v, want [14]", got)
	}
	if got := due(110 * time.Millisecond); len(got) != 0 {
		t.Fatalf("asked again after 110 ms with an 80 ms round trip: %v", got)
	}
	if got := due(15 * time.Millisecond); len(got) != 1 {
		t.Fatalf("not asked again after 125 ms: %v", got)
	}
	for i := 2; i < nackMaxPerPacket; i++ {
		if got := due(200 * time.Millisecond); len(got) != 1 {
			t.Fatalf("request %d: %v", i+1, got)
		}
	}
	if got := due(200 * time.Millisecond); len(got) != 0 {
		t.Fatalf("asked more than %d times: %v", nackMaxPerPacket, got)
	}
	if st := g.stats(); st.First != 2 || st.Retries != nackMaxPerPacket-1 || st.GaveUp != 1 {
		t.Fatalf("stats %+v", st)
	}

	// A late original ends the requests too.
	sendMedia(18)
	if got := due(0); len(got) != 2 {
		t.Fatalf("first NACKs %v, want 16 and 17", got)
	}
	sendMedia(16)
	if got := due(time.Second); len(got) != 1 || got[0] != 17 {
		t.Fatalf("NACKs %v after 16 came late, want [17]", got)
	}
	if st := g.stats(); st.LateOriginals != 1 {
		t.Fatalf("late originals %d, want 1", st.LateOriginals)
	}
}

// End to end: a host like libmello (RTX on its own SSRC) loses one packet
// and repairs it on RTX at the SFU's first NACK. The SFU must not ask for
// it again. Pion's generator did, every 100 ms for as long as the packet
// stayed in its log, because Pion delivers the RTX repair outside the media
// stream's interceptors: under the gate's congested cap that was 607
// duplicate repairs for 94 recovered packets.
func TestSFUAsksTheHostOnceForARepairedPacket(t *testing.T) {
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
	sender, err := host.AddTrack(track)
	if err != nil {
		t.Fatalf("add host track: %v", err)
	}
	nacks := make(chan uint16, 1024)
	go func() {
		buf := make([]byte, 1500)
		for {
			n, _, err := sender.Read(buf)
			if err != nil {
				return
			}
			pkts, err := rtcp.Unmarshal(buf[:n])
			if err != nil {
				continue
			}
			for _, pkt := range pkts {
				if nack, ok := pkt.(*rtcp.TransportLayerNack); ok {
					for _, pair := range nack.Nacks {
						for _, seq := range pair.PacketList() {
							nacks <- seq
						}
					}
				}
			}
		}
	}()
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
	if answer := negotiate(t, host, server); !strings.Contains(answer, "rtx/90000") {
		t.Fatal("the SFU answer to a host has no RTX")
	}
	var ctx webrtc.TrackLocalContext
	select {
	case ctx = <-track.bound:
	case <-time.After(10 * time.Second):
		t.Fatal("the host track was never bound")
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
	next := seq + 4

	// Media every 20 ms; the repair at the first NACK of the lost packet.
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	first := time.After(5 * time.Second)
	for asked := false; !asked; {
		select {
		case <-tick.C:
			writeMedia(next)
			next++
		case got := <-nacks:
			asked = got == lost
		case <-first:
			t.Fatal("the SFU never asked for the lost packet")
		}
	}
	rtxPayload := append([]byte{byte(lost >> 8), byte(lost)}, payload(lost)...)
	if _, err := ctx.WriteStream().WriteRTP(&rtp.Header{
		Version: 2, PayloadType: 97, SequenceNumber: 1,
		Timestamp: uint32(lost) * 3000, SSRC: uint32(ctx.SSRCRetransmission()),
	}, rtxPayload); err != nil {
		t.Fatalf("write RTX: %v", err)
	}
	// One request may already be on its way back; then none for 1 s.
	settle := time.Now().Add(50 * time.Millisecond)
	end := time.After(time.Second + 50*time.Millisecond)
	again := 0
	for done := false; !done; {
		select {
		case <-tick.C:
			writeMedia(next)
			next++
		case got := <-nacks:
			if got != lost {
				continue
			}
			if time.Now().After(settle) {
				again++
			}
		case <-end:
			done = true
		}
	}
	if again != 0 {
		t.Fatalf("the SFU asked %d more times for packet %d after its RTX repair", again, lost)
	}
}
