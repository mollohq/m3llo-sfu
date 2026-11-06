package snapshot

import (
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
)

func buildRTP(seq uint16, ts uint32, marker bool, payload []byte) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: seq,
			Timestamp:      ts,
			Marker:         marker,
		},
		Payload: append([]byte(nil), payload...),
	}
}

func feedIDR(a *rtpAUAssembler, seq uint16, ts uint32, marker bool, payload []byte) auFeedResult {
	return a.feed(buildRTP(seq, ts, marker, payload))
}

func TestRTPAUAssemblerSingleNALIDR(t *testing.T) {
	a := newRTPAUAssembler()
	sps := []byte{0x67, 0x42, 0x00, 0x0a}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	idr := []byte{0x65, 0x88, 0x84, 0x00}

	if result := feedIDR(a, 1, 9000, false, sps); result.complete {
		t.Fatal("sps alone should not complete AU")
	}
	if result := feedIDR(a, 2, 9000, false, pps); result.complete {
		t.Fatal("pps alone should not complete AU")
	}
	result := feedIDR(a, 3, 9000, true, idr)
	if !result.complete || !result.isIDR || !result.decodable || len(result.annexB) == 0 {
		t.Fatalf("idr marker should complete decodable AU: %+v len=%d", result, len(result.annexB))
	}
}

func TestRTPAUAssemblerSTAPA(t *testing.T) {
	a := newRTPAUAssembler()
	stap := []byte{0x18, 0x00, 0x04, 0x67, 0x42, 0x00, 0x0a, 0x00, 0x04, 0x68, 0xce, 0x38, 0x80}
	idr := []byte{0x65, 0x01, 0x02, 0x03}

	if result := feedIDR(a, 1, 100, false, stap); result.complete {
		t.Fatal("stap without marker should not complete")
	}
	result := feedIDR(a, 2, 100, true, idr)
	if !result.complete || !result.isIDR || !result.decodable || len(result.annexB) == 0 {
		t.Fatalf("stap+idr should complete: %+v len=%d", result, len(result.annexB))
	}
}

func TestRTPAUAssemblerFUA(t *testing.T) {
	a := newRTPAUAssembler()
	sps := []byte{0x67, 0x42, 0x00, 0x0a}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	start := []byte{0x7c, 0x85, 0x88, 0x84, 0x00}
	middle := []byte{0x7c, 0x05, 0x10, 0x20}
	end := []byte{0x7c, 0x45, 0x30}

	feedIDR(a, 1, 200, false, sps)
	feedIDR(a, 2, 200, false, pps)
	if result := feedIDR(a, 3, 200, false, start); result.complete {
		t.Fatal("fu-a start should not complete")
	}
	if result := feedIDR(a, 4, 200, false, middle); result.complete {
		t.Fatal("fu-a middle should not complete")
	}
	result := feedIDR(a, 5, 200, true, end)
	if !result.complete || !result.isIDR || !result.decodable || len(result.annexB) == 0 {
		t.Fatalf("fu-a end should complete decodable idr AU: %+v len=%d", result, len(result.annexB))
	}
}

func TestRTPAUAssemblerNonIDRMarkerDoesNotComplete(t *testing.T) {
	a := newRTPAUAssembler()
	p := []byte{0x41, 0x01, 0x02}
	result := feedIDR(a, 1, 300, true, p)
	if result.complete || result.isIDR {
		t.Fatal("non-idr marker should not produce snapshot candidate")
	}
}

func TestRTPAUAssemblerIDRWithoutParamsNotDecodable(t *testing.T) {
	a := newRTPAUAssembler()
	idr := []byte{0x65, 0x88, 0x84, 0x00}

	result := feedIDR(a, 1, 350, true, idr)
	if !result.complete || !result.isIDR {
		t.Fatalf("idr should complete as candidate: %+v", result)
	}
	if result.decodable || len(result.annexB) > 0 {
		t.Fatalf("idr without SPS/PPS should not be decodable: %+v", result)
	}
}

func TestRTPAUAssemblerMissingFragmentExpires(t *testing.T) {
	a := newRTPAUAssembler()
	sps := []byte{0x67, 0x42, 0x00, 0x0a}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	start := []byte{0x7c, 0x85, 0x01, 0x02}

	feedIDR(a, 1, 400, false, sps)
	feedIDR(a, 2, 400, false, pps)
	if result := feedIDR(a, 3, 400, false, start); result.complete {
		t.Fatal("incomplete fu-a should not complete")
	}
	a.cleanupExpired()
	result := feedIDR(a, 4, 401, true, []byte{0x65, 0x03})
	if !result.complete || !result.decodable {
		t.Fatalf("idr on new timestamp with cached params should be decodable: %+v", result)
	}
}

func TestRTPAUAssemblerInjectsCachedParameterSets(t *testing.T) {
	a := newRTPAUAssembler()
	sps := []byte{0x67, 0x42, 0x00, 0x0a}
	pps := []byte{0x68, 0xce, 0x38, 0x80}
	idrOnly := []byte{0x65, 0x99, 0x88}

	feedIDR(a, 1, 500, false, sps)
	feedIDR(a, 2, 500, false, pps)
	result := feedIDR(a, 3, 500, true, idrOnly)
	if !result.complete || !result.isIDR || !result.decodable {
		t.Fatalf("idr completion failed: %+v", result)
	}
	if len(result.annexB) < len(sps)+len(pps)+len(idrOnly)+12 {
		t.Fatalf("expected injected SPS/PPS prefix, au len=%d", len(result.annexB))
	}
}

func TestPipelineIDRBurstWithSTAPA(t *testing.T) {
	a := newRTPAUAssembler()
	stap := []byte{0x18, 0x00, 0x04, 0x67, 0x42, 0x00, 0x0a, 0x00, 0x04, 0x68, 0xce, 0x38, 0x80}
	start := []byte{0x7c, 0x85, 0x88, 0x84, 0x00}
	middle := []byte{0x7c, 0x05, 0x10, 0x20}
	end := []byte{0x7c, 0x45, 0x30}

	packets := []*rtp.Packet{
		buildRTP(1, 600, false, stap),
		buildRTP(2, 600, false, start),
		buildRTP(3, 600, false, middle),
		buildRTP(4, 600, true, end),
	}
	for _, pkt := range packets {
		if result := a.feed(pkt); result.complete {
			if !result.decodable || len(result.annexB) == 0 {
				t.Fatalf("burst should produce decodable AU: %+v", result)
			}
			return
		}
	}
	t.Fatal("expected decodable AU after STAP-A + FU-A IDR burst")
}

// Overflow has to be true by construction. This test used to Start() the
// pipeline and then sleep, which raced the ingest loop: the consumer normally
// drained tapCh faster than the loop below could fill it, so no drop was ever
// recorded and the test failed intermittently.
//
// Enabling the tap without starting the ingest loop leaves nothing consuming
// tapCh, so the buffer fills at exactly rtpTapBufferSize and every packet after
// that must be dropped. No sleep, no goroutines, no timing assumption.
func TestPipelineFeedRTPNonblockingOverflow(t *testing.T) {
	p := NewPipeline(PipelineConfig{
		SessionID: "sess",
		CrewID:    "crew",
		Interval:  time.Millisecond,
		Logger:    logging.New("error"),
		R2:        nil,
		OnUpload:  nil,
	})

	// FeedRTP is a no-op until the tap is enabled; Start() would also spawn the
	// consumer, which is precisely what must not run here.
	p.started.Store(true)

	const overflow = 8
	for i := 0; i < rtpTapBufferSize+overflow; i++ {
		p.FeedRTP(buildRTP(uint16(i), 1000, true, []byte{0x65, 0x01}))
	}

	if got := len(p.tapCh); got != rtpTapBufferSize {
		t.Errorf("buffered packets = %d, want %d", got, rtpTapBufferSize)
	}
	if got := p.tapDropped.Load(); got != overflow {
		t.Errorf("tap drops = %d, want %d", got, overflow)
	}
}

func TestPipelineUploadCallback(t *testing.T) {
	uploads := make(chan string, 1)
	onUpload := func(key string) {
		select {
		case uploads <- key:
		default:
		}
	}
	onUpload("snapshots/crew/sess/1.jpg")
	select {
	case key := <-uploads:
		if key == "" {
			t.Fatal("expected upload key")
		}
	case <-time.After(time.Second):
		t.Fatal("upload callback not invoked")
	}
}

func TestPipelineDecodeSkippedWithoutParams(t *testing.T) {
	p := NewPipeline(PipelineConfig{
		SessionID: "sess",
		CrewID:    "crew",
		Interval:  time.Millisecond,
		Logger:    logging.New("error"),
		R2:        nil,
		OnUpload:  nil,
	})
	p.Start()
	defer p.Stop()

	p.FeedRTP(buildRTP(1, 700, true, []byte{0x65, 0x01, 0x02}))
	time.Sleep(20 * time.Millisecond)

	if p.decodeSkipped.Load() == 0 {
		t.Fatal("expected decode skip counter for IDR without parameter sets")
	}
}
