package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// avSyncExtensionID is the RTP header extension that mello's stream host puts
// on each game-audio packet (libmello rtp_av_sync_extension.hpp,
// streaming-quality stage 7): the video RTP timestamp of the instant the
// audio was captured, 4 bytes, one-byte form. The SFU does not negotiate its
// URI; the viewer reads it at this fixed ID.
const avSyncExtensionID = 5

// The game audio reaches the viewer with the host's A/V sync header extension
// unchanged: the ingress reads it with the packet, the sequence rewrite keeps
// it, and the viewer leg's Opus track writes it. Through the production
// interceptor chains on both SFU legs (newSFUAPI, newSFUViewerAPI). Without
// it the viewer cannot map its audio clock to the video clock.
func TestStreamAudioForwardsTheAvSyncExtension(t *testing.T) {
	// Host leg: a host PC that sends Opus, and the SFU's PC.
	sfuHostAPI, err := newSFUAPI(webrtc.SettingEngine{})
	if err != nil {
		t.Fatalf("SFU host API: %v", err)
	}
	hostPC, err := newTestAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create host PC: %v", err)
	}
	defer hostPC.Close()
	serverHostPC, err := sfuHostAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create SFU host PC: %v", err)
	}
	defer serverHostPC.Close()
	hostAudio, err := webrtc.NewTrackLocalStaticRTP(streamAudioCodecCapability(), "audio", "host")
	if err != nil {
		t.Fatalf("host audio track: %v", err)
	}
	if _, err := hostPC.AddTransceiverFromTrack(hostAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		t.Fatalf("add host audio: %v", err)
	}
	ingress := make(chan *webrtc.TrackRemote, 1)
	serverHostPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		ingress <- track
	})
	negotiate(t, hostPC, serverHostPC)

	// Viewer leg: the SFU's PC with the viewer's Opus track, and a viewer PC.
	sfuViewerAPI, err := newSFUViewerAPI(webrtc.SettingEngine{}, newViewerBWERegistry())
	if err != nil {
		t.Fatalf("SFU viewer API: %v", err)
	}
	serverViewerPC, err := sfuViewerAPI.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create SFU viewer PC: %v", err)
	}
	defer serverViewerPC.Close()
	viewerPC, err := newTestAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewerPC.Close()
	viewerTrack, err := webrtc.NewTrackLocalStaticRTP(streamAudioCodecCapability(), "audio", "stream_host")
	if err != nil {
		t.Fatalf("viewer audio track: %v", err)
	}
	if _, err := serverViewerPC.AddTransceiverFromTrack(viewerTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		t.Fatalf("add viewer audio: %v", err)
	}
	received := make(chan *rtp.Packet, 64)
	viewerPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			for {
				pkt, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				select {
				case received <- pkt:
				default:
				}
			}
		}()
	})
	negotiate(t, serverViewerPC, viewerPC)

	// The session: the host's audio ingress and one viewer audio leg.
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	host := &Peer{UserID: "host", Role: "host"}
	host.bindSession(session)
	generation := host.installPeerConnection(serverHostPC)
	session.host = host
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	leg := &streamViewerAudio{
		peer:   &Peer{UserID: "viewer", Role: "viewer"},
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan *rtp.Packet, streamAudioQueueSize),
		write:  viewerTrack.WriteRTP,
	}
	session.videoMu.Lock()
	session.viewerAudio["viewer"] = leg
	session.videoMu.Unlock()
	go session.writeViewerAudioRTP(leg)

	value := []byte{0x12, 0x34, 0x56, 0x78}
	send := func(seq uint16) {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    111,
				SequenceNumber: seq,
				Timestamp:      uint32(seq) * 960,
			},
			Payload: []byte{0xfc, 0xff, 0xfe},
		}
		if err := pkt.Header.SetExtension(avSyncExtensionID, value); err != nil {
			t.Fatalf("set extension: %v", err)
		}
		if err := hostAudio.WriteRTP(pkt); err != nil {
			t.Fatalf("write host audio: %v", err)
		}
	}

	// The remote track has no codec before its first packet.
	var track *webrtc.TrackRemote
	deadline := time.Now().Add(5 * time.Second)
	seq := uint16(1)
	for track == nil && time.Now().Before(deadline) {
		send(seq)
		seq++
		select {
		case track = <-ingress:
		case <-time.After(20 * time.Millisecond):
		}
	}
	if track == nil {
		t.Fatal("no host audio track at the SFU")
	}
	session.WireAudioTrackFor(host, track, serverHostPC, generation)

	for time.Now().Before(deadline) {
		send(seq)
		seq++
		select {
		case pkt := <-received:
			got := pkt.Header.GetExtension(avSyncExtensionID)
			if got == nil {
				t.Fatalf("the viewer's packet (seq %d) lost the A/V sync extension (profile %#x, ids %v)",
					pkt.SequenceNumber, pkt.Header.ExtensionProfile, pkt.Header.GetExtensionIDs())
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("A/V sync extension changed: got %x, want %x", got, value)
			}
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatal("the viewer got no game audio")
}
