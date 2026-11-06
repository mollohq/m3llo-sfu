package main

import (
	_ "embed"
	"errors"
	"fmt"
	"hash/fnv"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	h264PayloadType = 96
	h264ClockRate   = 90000
	// rtpMaxPayload is the largest RTP payload the soak host sends: the
	// fragment size of the libmello RTP sender (rtp_video_sender.cpp).
	rtpMaxPayload = 1100
	nalTypeFUA    = 28
)

// h264FixtureData is a real H.264 stream: the first 2 s (120 access units)
// of the streaming quality gate's 720p60 reference clip, encoded once with
// the OpenH264 test-lane encoder of libmello at 2500 kbps (one slice, IDR
// at frame 0, constant SPS/PPS ids). Made with
// `stream-gate --write-fixture` in the mello repository (TESTING.md,
// "Streaming quality gate"). Annex-B, access units back to back.
//
//go:embed testdata/stream-gate-720p60-2s.h264
var h264FixtureData []byte

// h264FixtureFNV1a is the FNV-1a 64 of the fixture, as stream-gate prints it.
// A test checks it, so a changed file is a deliberate change.
const h264FixtureFNV1a = "17c08039c987fc87"

// h264FixtureAccessUnits is the frame count of the fixture.
const h264FixtureAccessUnits = 120

func h264CodecCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeH264,
		ClockRate:   h264ClockRate,
		SDPFmtpLine: "profile-level-id=4d0033;packetization-mode=1;level-asymmetry-allowed=1",
		RTCPFeedback: []webrtc.RTCPFeedback{
			{Type: "nack"},
			{Type: "nack", Parameter: "pli"},
			{Type: "goog-remb"},
		},
	}
}

type plannedRTPPacket struct {
	payload []byte
	marker  bool
}

// fixtureFrame is one access unit, packetized.
type fixtureFrame struct {
	keyframe bool
	packets  []plannedRTPPacket
}

// splitAnnexB returns the NAL units of an Annex-B stream, without start codes.
func splitAnnexB(b []byte) [][]byte {
	var nals [][]byte
	start := -1
	i := 0
	for i+3 <= len(b) {
		sc := 0
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			sc = 3
		} else if i+4 <= len(b) && b[i] == 0 && b[i+1] == 0 && b[i+2] == 0 && b[i+3] == 1 {
			sc = 4
		}
		if sc == 0 {
			i++
			continue
		}
		if start >= 0 && i > start {
			nals = append(nals, b[start:i])
		}
		i += sc
		start = i
	}
	if start >= 0 && start < len(b) {
		nals = append(nals, b[start:])
	}
	return nals
}

// groupAccessUnits groups NAL units into access units. The fixture has one
// slice per frame, so each VCL NAL (type 1 or 5) closes an access unit; the
// parameter sets and SEI before it belong to it.
func groupAccessUnits(nals [][]byte) [][][]byte {
	var aus [][][]byte
	var cur [][]byte
	for _, n := range nals {
		if len(n) == 0 {
			continue
		}
		cur = append(cur, n)
		t := n[0] & 0x1f
		if t == 1 || t == 5 {
			aus = append(aus, cur)
			cur = nil
		}
	}
	return aus
}

// packetizeNAL packetizes one NAL unit as RFC 6184 packetization mode 1:
// a single NAL unit packet when it fits, else FU-A fragments.
func packetizeNAL(nal []byte, maxPayload int) [][]byte {
	if len(nal) <= maxPayload {
		return [][]byte{append([]byte(nil), nal...)}
	}
	header := nal[0]
	indicator := (header & 0xe0) | nalTypeFUA
	nalType := header & 0x1f
	body := nal[1:]
	var out [][]byte
	for off := 0; off < len(body); {
		n := len(body) - off
		if n > maxPayload-2 {
			n = maxPayload - 2
		}
		fu := nalType
		if off == 0 {
			fu |= 0x80 // start
		}
		if off+n == len(body) {
			fu |= 0x40 // end
		}
		p := make([]byte, 0, n+2)
		p = append(p, indicator, fu)
		p = append(p, body[off:off+n]...)
		out = append(out, p)
		off += n
	}
	return out
}

// loadFixture parses and packetizes an Annex-B stream.
func loadFixture(data []byte, maxPayload int) ([]fixtureFrame, error) {
	aus := groupAccessUnits(splitAnnexB(data))
	if len(aus) == 0 {
		return nil, errors.New("no access unit in the H.264 fixture")
	}
	frames := make([]fixtureFrame, 0, len(aus))
	for _, au := range aus {
		var f fixtureFrame
		for _, nal := range au {
			if nal[0]&0x1f == 5 {
				f.keyframe = true
			}
			for _, p := range packetizeNAL(nal, maxPayload) {
				f.packets = append(f.packets, plannedRTPPacket{payload: p})
			}
		}
		f.packets[len(f.packets)-1].marker = true
		frames = append(frames, f)
	}
	return frames, nil
}

func fixtureFNV1a(data []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(data)
	return fmt.Sprintf("%016x", h.Sum64())
}

var h264FixtureFrames = func() []fixtureFrame {
	frames, err := loadFixture(h264FixtureData, rtpMaxPayload)
	if err != nil {
		panic(err)
	}
	return frames
}()

// planFramePackets returns the packets of frame `frameIndex` of the host
// stream: the fixture's access units in order, looped. Each pass starts with
// the fixture's IDR, so the stream has a keyframe every 120 frames.
func planFramePackets(frameIndex int) fixtureFrame {
	return h264FixtureFrames[frameIndex%len(h264FixtureFrames)]
}

func buildRTPPacket(seq uint16, ts uint32, marker bool, payload []byte) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    h264PayloadType,
			SequenceNumber: seq,
			Timestamp:      ts,
			Marker:         marker,
		},
		Payload: append([]byte(nil), payload...),
	}
}

// newStreamAPI builds a Pion API with only the codecs the client offers for a
// stream (01-SFU.md §6.1): H.264 at PT 96 and Opus at PT 111. Pion's default
// codec set puts H.264 4d001f at PT 127, which the SFU reserves for ULPFEC, so
// the SFU would route the soak host's video as FEC parity.
func newStreamAPI() (*webrtc.API, error) {
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
	if err := webrtc.RegisterDefaultInterceptors(me, registry); err != nil {
		return nil, err
	}
	return webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(registry)), nil
}
