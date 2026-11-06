package main

import (
	"math/rand"
	"strings"
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// viewerDrop is the viewer-leg loss of --viewer-drop-pct: one viewer drops
// a share of the video packets that reach it from the SFU, before its NACK
// generator sees them. The generator then NACKs them, and the SFU's NACK
// responder sends them again on the viewer leg. A dropped sequence number
// that arrives later is repaired. Each sequence number is dropped at most
// once, so a repair is never dropped.
//
// The drop runs after SRTP decryption, inside the interceptor chain. The
// viewer therefore runs with SRTP replay protection off: the repair has the
// sequence number of a packet that SRTP already took.
type viewerDrop struct {
	pct float64

	mu       sync.Mutex
	rng      *rand.Rand
	pending  map[uint16]struct{}
	dropped  int64
	repaired int64
}

func newViewerDrop(pct float64, seed int64) *viewerDrop {
	return &viewerDrop{
		pct:     pct,
		rng:     rand.New(rand.NewSource(seed)),
		pending: make(map[uint16]struct{}),
	}
}

// keep reports whether the packet with seq goes on to the NACK generator.
func (d *viewerDrop) keep(seq uint16) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.pending[seq]; ok {
		delete(d.pending, seq)
		d.repaired++
		return true
	}
	if d.rng.Float64()*100 < d.pct {
		d.pending[seq] = struct{}{}
		d.dropped++
		return false
	}
	return true
}

// counts returns the dropped and the repaired packets.
func (d *viewerDrop) counts() (dropped, repaired int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dropped, d.repaired
}

type viewerDropFactory struct{ drop *viewerDrop }

func (f viewerDropFactory) NewInterceptor(string) (interceptor.Interceptor, error) {
	return &viewerDropInterceptor{drop: f.drop}, nil
}

type viewerDropInterceptor struct {
	interceptor.NoOp
	drop *viewerDrop
}

// BindRemoteStream drops video packets of the stream before the
// interceptors registered after it (the NACK generator) read them.
func (i *viewerDropInterceptor) BindRemoteStream(
	info *interceptor.StreamInfo, reader interceptor.RTPReader,
) interceptor.RTPReader {
	if !strings.HasPrefix(strings.ToLower(info.MimeType), "video/") {
		return reader
	}
	return interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		for {
			n, attr, err := reader.Read(b, a)
			if err != nil {
				return n, attr, err
			}
			var header rtp.Header
			if _, err := header.Unmarshal(b[:n]); err != nil || i.drop.keep(header.SequenceNumber) {
				return n, attr, nil
			}
		}
	})
}

// newStreamAPIWithDrop is newStreamAPI with drop first in the interceptor
// chain and SRTP replay protection off.
func newStreamAPIWithDrop(drop *viewerDrop) (*webrtc.API, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: h264CodecCapability(),
		PayloadType:        h264PayloadType,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, err
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: opusCodecCapability(),
		PayloadType:        opusPayloadType,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}
	registry := &interceptor.Registry{}
	registry.Add(viewerDropFactory{drop: drop})
	if err := webrtc.RegisterDefaultInterceptors(me, registry); err != nil {
		return nil, err
	}
	var se webrtc.SettingEngine
	se.DisableSRTPReplayProtection(true)
	return webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(registry),
		webrtc.WithSettingEngine(se),
	), nil
}

// repairGateOK reports whether the viewer-leg repair passes: some packets
// dropped, and at least (1 - tolerance) of them repaired.
func repairGateOK(dropped, repaired int64, tolerance float64) bool {
	return dropped > 0 && float64(repaired) >= float64(dropped)*(1-tolerance)
}
