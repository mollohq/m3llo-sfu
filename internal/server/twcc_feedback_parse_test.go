package server

import (
	"strings"
	"testing"

	"github.com/pion/rtcp"
)

// libmelloTWCCReport is one transport-cc feedback packet as libmello's
// TwccFeedbackGenerator::build_feedback writes it (mello
// libmello/tests/test_twcc.cpp, kLayoutReport; keep the two equal).
// Sequences 100 to 115 arrive 1 ms apart from 64.003 s, 116 is lost, 117
// arrives 70 ms after 115.
//
// Before mello streaming-quality stage 4 the generator wrote the run length
// chunk with a one-bit symbol and a 14-bit run. Pion reads a two-bit symbol
// and a 13-bit run, saw a run of large deltas, ran past the end of the packet
// and returned "rtcp: packet too short" (about 18 per second per viewer).
var libmelloTWCCReport = []byte{
	0x8f, 0xcd, 0x00, 0x0a, // V=2, FMT=15, PT=205, length 10 words
	0x11, 0x11, 0x11, 0x11, // sender SSRC
	0x22, 0x22, 0x22, 0x22, // media SSRC
	0x00, 0x64, 0x00, 0x12, // base sequence 100, status count 18
	0x00, 0x03, 0xe8, 0x00, // reference time 1000 x 64 ms, feedback count 0
	0x20, 0x10, // run length chunk: small delta, run 16
	0xc8, 0x00, // two-bit status vector: not received, large delta
	0x0c,                                           // 3 ms
	0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, // 1 ms each
	0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
	0x01, 0x18, // 70 ms, large delta
	0x00, 0x00, // padding
}

func TestPionParsesLibmelloTWCCFeedback(t *testing.T) {
	packets, err := rtcp.Unmarshal(libmelloTWCCReport)
	if err != nil {
		t.Fatalf("rtcp.Unmarshal: %v", err)
	}
	if len(packets) != 1 {
		t.Fatalf("got %d packets, want 1", len(packets))
	}
	cc, ok := packets[0].(*rtcp.TransportLayerCC)
	if !ok {
		t.Fatalf("got %T, want *rtcp.TransportLayerCC", packets[0])
	}
	if cc.SenderSSRC != 0x11111111 || cc.MediaSSRC != 0x22222222 {
		t.Errorf("SSRCs %#x %#x", cc.SenderSSRC, cc.MediaSSRC)
	}
	if cc.BaseSequenceNumber != 100 || cc.PacketStatusCount != 18 {
		t.Errorf("base %d count %d, want 100 18", cc.BaseSequenceNumber, cc.PacketStatusCount)
	}
	if cc.ReferenceTime != 1000 || cc.FbPktCount != 0 {
		t.Errorf("reference %d feedback count %d, want 1000 0", cc.ReferenceTime, cc.FbPktCount)
	}
	if len(cc.PacketChunks) != 2 {
		t.Fatalf("got %d chunks, want 2", len(cc.PacketChunks))
	}
	run, ok := cc.PacketChunks[0].(*rtcp.RunLengthChunk)
	if !ok || run.PacketStatusSymbol != rtcp.TypeTCCPacketReceivedSmallDelta || run.RunLength != 16 {
		t.Errorf("first chunk %+v, want a run of 16 small deltas", cc.PacketChunks[0])
	}
	vector, ok := cc.PacketChunks[1].(*rtcp.StatusVectorChunk)
	if !ok || vector.SymbolSize != rtcp.TypeTCCSymbolSizeTwoBit ||
		vector.SymbolList[0] != rtcp.TypeTCCPacketNotReceived ||
		vector.SymbolList[1] != rtcp.TypeTCCPacketReceivedLargeDelta {
		t.Errorf("second chunk %+v, want a two-bit vector: not received, large delta", cc.PacketChunks[1])
	}

	// Deltas in microseconds: 3 ms from the reference, 15 x 1 ms, 70 ms.
	want := []int64{3000}
	for i := 0; i < 15; i++ {
		want = append(want, 1000)
	}
	want = append(want, 70000)
	if len(cc.RecvDeltas) != len(want) {
		t.Fatalf("got %d deltas, want %d", len(cc.RecvDeltas), len(want))
	}
	for i, delta := range cc.RecvDeltas {
		if delta.Delta != want[i] {
			t.Errorf("delta %d = %d us, want %d", i, delta.Delta, want[i])
		}
	}
	if cc.RecvDeltas[16].Type != rtcp.TypeTCCPacketReceivedLargeDelta {
		t.Errorf("last delta type %d, want large", cc.RecvDeltas[16].Type)
	}
}

// libmelloTWCCReportBeforeStage4 is the same arrivals as the old generator
// wrote them: run length chunk 0x4010 (one-bit symbol 1, run 16) and deltas
// clamped to a signed byte. Pion reads 0x4010 as 16 large deltas.
var libmelloTWCCReportBeforeStage4 = []byte{
	0x8f, 0xcd, 0x00, 0x0a,
	0x11, 0x11, 0x11, 0x11,
	0x22, 0x22, 0x22, 0x22,
	0x00, 0x64, 0x00, 0x12,
	0x00, 0x03, 0xe8, 0x00,
	0x40, 0x10, // the old run length chunk
	0x90, 0x00, // one-bit vector: not received, received
	0x00,
	0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
	0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
	0x7f,             // 70 ms clamped to 31.75 ms
	0x00, 0x00, 0x00, // padding
}

func TestPionRejectsTheOldLibmelloTWCCFeedback(t *testing.T) {
	_, err := rtcp.Unmarshal(libmelloTWCCReportBeforeStage4)
	if err == nil || !strings.Contains(err.Error(), "packet too short") {
		t.Fatalf("got %v, want the field error \"rtcp: packet too short\"", err)
	}
}
