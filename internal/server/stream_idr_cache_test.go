package server

import (
	"testing"

	"github.com/pion/rtp"
)

func idrTestPacket(seq uint16, ts uint32, marker bool, payload []byte) *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{PayloadType: 96, SequenceNumber: seq, Timestamp: ts, Marker: marker},
		Payload: payload,
	}
}

// A host that never changes the RTP timestamp cannot grow the IDR cache
// past its caps. The next real IDR is cached as before.
func TestIdrCacheIsCappedPerAccessUnit(t *testing.T) {
	c := newStreamIdrPacketCache()
	filler := make([]byte, 1000)
	for i := 0; i < 3*idrCacheMaxPackets; i++ {
		c.observe(idrTestPacket(uint16(i), 1000, false, filler))
		if len(c.current) > idrCacheMaxPackets || c.currentBytes > idrCacheMaxBytes {
			t.Fatalf("packet %d: cache holds %d packets, %d bytes", i, len(c.current), c.currentBytes)
		}
	}
	idr := []byte{0x65, 0x88, 0x84} // NAL type 5: IDR slice
	c.observe(idrTestPacket(1, 1000, true, idr))
	if c.snapshot() != nil {
		t.Fatal("cached the IDR of an access unit that went over the cap")
	}

	c.observe(idrTestPacket(2, 4000, true, idr))
	if got := c.snapshot(); len(got) != 1 {
		t.Fatalf("the next IDR: cached %d packets, want 1", len(got))
	}
}
