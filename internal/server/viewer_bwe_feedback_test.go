package server

import (
	"math/rand"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

// libmelloFeedback is a feedback packet as libmello's viewer builds it
// (TwccFeedbackGenerator, libmello/src/transport/twcc.cpp
// encode_status_chunks) and as rtcp.Unmarshal reads it back: a run of at
// least 14 equal symbols, or of at least 7 that ends the list, is a run
// length chunk; else a one-bit vector of 14 when the next 14 symbols have
// no large delta; else a two-bit vector of 7. A vector chunk is always
// full: its symbols past the list are padding ("not received").
// arrivalsUs are the arrival times of the packets (of the received ones).
func libmelloFeedback(base uint16, symbols []uint16, arrivalsUs []int64) *rtcp.TransportLayerCC {
	fb := &rtcp.TransportLayerCC{
		BaseSequenceNumber: base,
		PacketStatusCount:  uint16(len(symbols)),
	}
	// The reference time: the first arrival rounded down to 64 ms.
	clock := int64(-1)
	for i, s := range symbols {
		if s != rtcp.TypeTCCPacketNotReceived {
			fb.ReferenceTime = uint32(arrivalsUs[i] / 64_000)
			clock = int64(fb.ReferenceTime) * 64_000
			break
		}
	}
	n := len(symbols)
	for i := 0; i < n; {
		run := 1
		for i+run < n && symbols[i+run] == symbols[i] {
			run++
		}
		if run >= 14 || (i+run == n && run >= 7) {
			fb.PacketChunks = append(fb.PacketChunks, &rtcp.RunLengthChunk{
				Type: rtcp.TypeTCCRunLengthChunk, PacketStatusSymbol: symbols[i], RunLength: uint16(run),
			})
			i += run
			continue
		}
		oneBit := min(14, n-i)
		fits := true
		for _, s := range symbols[i : i+oneBit] {
			fits = fits && s != rtcp.TypeTCCPacketReceivedLargeDelta
		}
		size, width := uint16(rtcp.TypeTCCSymbolSizeOneBit), 14
		if !fits {
			size, width = rtcp.TypeTCCSymbolSizeTwoBit, 7
		}
		list := make([]uint16, width)
		used := copy(list, symbols[i:min(i+width, n)])
		fb.PacketChunks = append(fb.PacketChunks, &rtcp.StatusVectorChunk{
			Type: rtcp.TypeTCCStatusVectorChunk, SymbolSize: size, SymbolList: list,
		})
		i += used
	}
	for i, s := range symbols {
		if s != rtcp.TypeTCCPacketNotReceived {
			fb.RecvDeltas = append(fb.RecvDeltas, &rtcp.RecvDelta{Type: s, Delta: arrivalsUs[i] - clock})
			clock = arrivalsUs[i]
		}
	}
	return fb
}

// A feedback that ends in a vector chunk: the padding symbols go, the
// arrival deltas stay. Entries older than the history window go too, and
// the first kept delta carries theirs.
func TestSanitizeFeedbackDropsPaddingAndOldEntries(t *testing.T) {
	R, L := rtcp.TypeTCCPacketReceivedSmallDelta, rtcp.TypeTCCPacketNotReceived
	symbols := []uint16{R, R, L, R, R}                         // one vector chunk of 14: 9 padding
	arrivals := []int64{641_000, 643_000, 0, 646_000, 650_000} // reference 640 ms
	fb := libmelloFeedback(65534, symbols, arrivals)
	out, padding, old := sanitizeFeedback(fb, 0, false)
	if padding != 9 || old != 0 || out.PacketStatusCount != 5 || out.BaseSequenceNumber != 65534 {
		t.Fatalf("padding %d old %d count %d base %d", padding, old, out.PacketStatusCount, out.BaseSequenceNumber)
	}
	if got := len(out.PacketChunks[0].(*rtcp.StatusVectorChunk).SymbolList); got != 5 {
		t.Fatalf("%d symbols, want 5", got)
	}
	// Keep from 65535+1 = 0 (after the wrap): two entries go.
	out, _, old = sanitizeFeedback(fb, 0, true)
	if old != 2 || out.BaseSequenceNumber != 0 || out.PacketStatusCount != 3 {
		t.Fatalf("old %d base %d count %d, want 2, 0, 3", old, out.BaseSequenceNumber, out.PacketStatusCount)
	}
	if len(out.RecvDeltas) != 2 || out.RecvDeltas[0].Delta != 1000+2000+3000 || out.RecvDeltas[1].Delta != 4000 {
		t.Fatalf("deltas %v, want the first to carry the dropped ones", out.RecvDeltas)
	}
	// A run length chunk and no old entries: unchanged.
	full := libmelloFeedback(10, []uint16{R, R, R, R, R, R, R, R}, []int64{1, 2, 3, 4, 5, 6, 7, 8})
	if out, _, _ := sanitizeFeedback(full, 0, false); out != full {
		t.Fatal("a feedback without padding was copied")
	}
}

// bweFeedbackRun feeds a viewer estimator (a real gcc behind it) for run:
// every 50 ms perFeedback packets are sent and the previous ones are acked
// in a libmello feedback with lossPct random loss. It returns the target.
func bweFeedbackRun(t *testing.T, perFeedback int, lossPct float64, run time.Duration) int {
	t.Helper()
	const start = 8_000_000
	inner, err := gcc.NewSendSideBWE(
		gcc.SendSideBWEInitialBitrate(start),
		gcc.SendSideBWEMinBitrate(viewerBWEMinBitrate),
		gcc.SendSideBWEMaxBitrate(viewerBWEMaxBitrate),
		gcc.SendSideBWEPacer(gcc.NewNoOpPacer()),
	)
	if err != nil {
		t.Fatalf("gcc: %v", err)
	}
	defer inner.Close()
	e := &viewerEstimator{inner: inner}
	const extID = 5
	writer := e.AddStream(&interceptor.StreamInfo{
		SSRC:                1,
		RTPHeaderExtensions: []interceptor.RTPHeaderExtension{{URI: transportCCExtensionURI, ID: extID}},
	}, interceptor.RTPWriterFunc(func(*rtp.Header, []byte, interceptor.Attributes) (int, error) { return 0, nil }))
	rng := rand.New(rand.NewSource(7))
	var seq uint16
	send := func() uint16 {
		first := seq
		for i := 0; i < perFeedback; i++ {
			h := &rtp.Header{Version: 2, SSRC: 1, SequenceNumber: seq}
			if err := h.SetExtension(extID, []byte{byte(seq >> 8), byte(seq)}); err != nil {
				t.Fatalf("extension: %v", err)
			}
			if _, err := writer.Write(h, make([]byte, 1100), nil); err != nil {
				t.Fatalf("write: %v", err)
			}
			seq++
		}
		return first
	}
	// Arrival: 10 ms after the send, the batch spread over 20 ms (a link
	// faster than the stream), on the viewer's clock.
	gap := int64(20_000 / perFeedback)
	begin := time.Now()
	prevAt := time.Since(begin).Microseconds()
	prev := send()
	for time.Since(begin) < run {
		time.Sleep(50 * time.Millisecond)
		at := time.Since(begin).Microseconds()
		next := send()
		symbols := make([]uint16, perFeedback)
		arrivals := make([]int64, perFeedback)
		for i := range symbols {
			symbols[i] = rtcp.TypeTCCPacketReceivedSmallDelta
			arrivals[i] = 1_000_000 + prevAt + 10_000 + int64(i)*gap
			if rng.Float64()*100 < lossPct {
				symbols[i] = rtcp.TypeTCCPacketNotReceived
			}
		}
		prevAt = at
		if err := e.WriteRTCP([]rtcp.Packet{libmelloFeedback(prev, symbols, arrivals)}, nil); err != nil {
			t.Fatalf("feedback: %v", err)
		}
		prev = next
	}
	return e.GetTargetBitrate()
}

// The viewer estimate on a leg that carries the stream, with a libmello
// viewer's feedback. Pion counted the padding of the feedback's last vector
// chunk as lost packets (the newest ones, in flight): with 4 packets per
// feedback (a low rate, as after the host's 540p rung) that is 10 phantom
// losses per 4 packets, and the target fell from 8 Mbps to the 0.3 Mbps
// floor in about 2 s on a leg with no loss; with 5 % loss (the gate's
// mobile profile) it fell too. Now the target holds within 30 %.
func TestViewerEstimateHoldsWithLibmelloFeedback(t *testing.T) {
	for _, c := range []struct {
		name        string
		perFeedback int
		lossPct     float64
	}{
		{"low rate, no loss", 4, 0},
		{"preset rate, 5 % loss", 40, 5},
	} {
		got := bweFeedbackRun(t, c.perFeedback, c.lossPct, 3*time.Second)
		t.Logf("%s: target %d bps", c.name, got)
		if got < 5_600_000 {
			t.Errorf("%s: target %d bps after 3 s, want at least 70 %% of 8 Mbps", c.name, got)
		}
	}
}
