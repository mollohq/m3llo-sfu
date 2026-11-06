package main

import (
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	opusPayloadType = 111
	opusClockRate   = 48000
	// 20 ms frames, as the client sends game audio (01-SFU.md §6.1).
	opusFrameMs       = 20
	opusFramesPerSec  = 1000 / opusFrameMs
	opusTimestampStep = opusClockRate / opusFramesPerSec
)

// opusSilentFrame is a 20 ms CELT fullband stereo silence frame. The SFU
// forwards it without decode.
var opusSilentFrame = []byte{0xfc, 0xff, 0xfe}

// opusCodecCapability matches the Opus codec the client offers for stream
// game audio: PT 111, 48 kHz, 2 channels (01-SFU.md §6.1).
func opusCodecCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeOpus,
		ClockRate:   opusClockRate,
		Channels:    2,
		SDPFmtpLine: "minptime=10;useinbandfec=1",
	}
}

func buildOpusRTPPacket(seq uint16, ts uint32) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    opusPayloadType,
			SequenceNumber: seq,
			Timestamp:      ts,
		},
		Payload: append([]byte(nil), opusSilentFrame...),
	}
}
