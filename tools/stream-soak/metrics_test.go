package main

import (
	"testing"
	"time"
)

func TestPlanFramePacketsLoopsTheFixtureFromItsIDR(t *testing.T) {
	if got := len(h264FixtureFrames); got != h264FixtureAccessUnits {
		t.Fatalf("fixture access units = %d, want %d", got, h264FixtureAccessUnits)
	}
	first := planFramePackets(0)
	if !first.keyframe {
		t.Fatal("frame 0 must be the IDR")
	}
	types := map[byte]bool{}
	for _, p := range first.packets {
		types[p.payload[0]&0x1f] = true
	}
	if !types[7] || !types[8] {
		t.Fatalf("the IDR access unit carries no SPS or PPS: NAL types %v", types)
	}
	again := planFramePackets(h264FixtureAccessUnits)
	if !again.keyframe || len(again.packets) != len(first.packets) {
		t.Fatal("the fixture loops from its IDR")
	}
	for i := 0; i < h264FixtureAccessUnits; i++ {
		f := planFramePackets(i)
		for j, p := range f.packets {
			if len(p.payload) > rtpMaxPayload {
				t.Fatalf("frame %d packet %d: %d bytes > %d", i, j, len(p.payload), rtpMaxPayload)
			}
			if p.marker != (j == len(f.packets)-1) {
				t.Fatalf("frame %d packet %d: marker only on the last packet", i, j)
			}
		}
		if i > 0 && f.keyframe {
			t.Fatalf("frame %d: the fixture has one IDR, at frame 0", i)
		}
	}
}

func TestTheFixtureIsTheFrozenFile(t *testing.T) {
	if got := fixtureFNV1a(h264FixtureData); got != h264FixtureFNV1a {
		t.Fatalf("fixture fnv1a = %s, want %s", got, h264FixtureFNV1a)
	}
}

func TestFUAFragmentsRebuildTheNAL(t *testing.T) {
	nal := make([]byte, 3000)
	nal[0] = 0x65 // IDR, nal_ref_idc 3
	for i := 1; i < len(nal); i++ {
		nal[i] = byte(i * 7)
	}
	frags := packetizeNAL(nal, 1100)
	if len(frags) != 3 {
		t.Fatalf("fragments = %d, want 3", len(frags))
	}
	rebuilt := []byte{(frags[0][0] & 0xe0) | (frags[0][1] & 0x1f)}
	for i, f := range frags {
		if f[0]&0x1f != nalTypeFUA {
			t.Fatalf("fragment %d is not FU-A", i)
		}
		if (f[1]&0x80 != 0) != (i == 0) || (f[1]&0x40 != 0) != (i == len(frags)-1) {
			t.Fatalf("fragment %d: wrong start or end bit", i)
		}
		rebuilt = append(rebuilt, f[2:]...)
	}
	if string(rebuilt) != string(nal) {
		t.Fatal("FU-A fragments do not rebuild the NAL")
	}
	if got := packetizeNAL(nal[:500], 1100); len(got) != 1 || got[0][0] != 0x65 {
		t.Fatal("a small NAL goes as a single NAL unit packet")
	}
}

func TestSimImpairmentIsSeededAndNearItsRate(t *testing.T) {
	a := newSimImpairment(3, 8, 7)
	b := newSimImpairment(3, 8, 7)
	drops := 0
	for i := 0; i < 20000; i++ {
		da, db := a.drop(), b.drop()
		if da != db {
			t.Fatal("same seed, different drops")
		}
		if da {
			drops++
		}
		if a.frameDelay() != b.frameDelay() {
			t.Fatal("same seed, different delays")
		}
	}
	if drops < 500 || drops > 700 {
		t.Fatalf("drops = %d of 20000, want about 600", drops)
	}
	if newSimImpairment(0, 0, 1).drop() || newSimImpairment(0, 0, 1).frameDelay() != 0 {
		t.Fatal("zero settings must not impair")
	}
}

func TestViewerMetricsSequenceGapsAndDuplicates(t *testing.T) {
	m := &viewerMetrics{id: "v1"}
	base := time.Unix(0, 0)

	m.onPacket(10, 100, base)
	m.onPacket(11, 100, base.Add(10*time.Millisecond))
	m.onPacket(11, 100, base.Add(20*time.Millisecond))
	m.onPacket(14, 100, base.Add(30*time.Millisecond))

	snap := m.snapshot()
	if snap.packets != 4 {
		t.Fatalf("packets = %d, want 4", snap.packets)
	}
	if snap.duplicates != 1 {
		t.Fatalf("duplicates = %d, want 1", snap.duplicates)
	}
	if snap.seqGaps != 2 {
		t.Fatalf("seqGaps = %d, want 2 (missing 12,13)", snap.seqGaps)
	}
	if len(snap.interArrival) != 2 {
		t.Fatalf("interArrival samples = %d, want 2", len(snap.interArrival))
	}
}

func TestP95Millis(t *testing.T) {
	values := make([]int64, 20)
	for i := range values {
		values[i] = int64(i+1) * int64(time.Millisecond)
	}
	got := p95Millis(values)
	want := 19.0
	if got != want {
		t.Fatalf("p95 = %.1f, want %.1f", got, want)
	}
}

func TestReconcileRelay(t *testing.T) {
	admin := adminRelayStats{
		RTPInPackets:           100,
		RTPFanoutAttempts:      800,
		RTPForwardedPackets:    790,
		RTPQueueDroppedPackets: 0,
		RTPDuplicatePackets:    0,
	}
	res := reconcileRelay(admin, 100, 8, 0, 0)
	if !res.inMatchesHost || !res.forwardMatches || !res.queueDropsOK || !res.duplicatesOK {
		t.Fatalf("expected passing reconcile, got %+v", res)
	}

	bad := reconcileRelay(adminRelayStats{RTPQueueDroppedPackets: 3}, 100, 8, 0, 0)
	if bad.queueDropsOK {
		t.Fatal("expected queue drop failure")
	}
}

func TestBuildRTPPacketUsesPT96(t *testing.T) {
	pkt := buildRTPPacket(7, 9000, true, planFramePackets(0).packets[0].payload)
	if pkt.PayloadType != h264PayloadType {
		t.Fatalf("payload type = %d, want %d", pkt.PayloadType, h264PayloadType)
	}
	if !pkt.Marker {
		t.Fatal("expected marker bit")
	}
}
