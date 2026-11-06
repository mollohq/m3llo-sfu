package main

import (
	"strings"
	"testing"

	"github.com/pion/webrtc/v4"
)

type offeredMedia struct {
	kind      string
	direction string
	opus111   bool
	h264PT96  bool
	pt127     bool
}

func newStreamTestPC(t *testing.T) *webrtc.PeerConnection {
	t.Helper()
	api, err := newStreamAPI()
	if err != nil {
		t.Fatal(err)
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	return pc
}

func streamRoleOffer(t *testing.T, role string) (string, []offeredMedia) {
	t.Helper()
	pc := newStreamTestPC(t)
	if _, err := pc.CreateDataChannel("control", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := addStreamMedia(pc, role, "offer-shape"); err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := offer.Unmarshal()
	if err != nil {
		t.Fatal(err)
	}

	var media []offeredMedia
	for _, md := range parsed.MediaDescriptions {
		m := offeredMedia{kind: md.MediaName.Media}
		for _, dir := range []string{"sendrecv", "sendonly", "recvonly", "inactive"} {
			if _, ok := md.Attribute(dir); ok {
				m.direction = dir
			}
		}
		for _, attr := range md.Attributes {
			if attr.Key != "rtpmap" {
				continue
			}
			switch {
			case strings.EqualFold(attr.Value, "111 opus/48000/2"):
				m.opus111 = true
			case strings.EqualFold(attr.Value, "96 H264/90000"):
				m.h264PT96 = true
			case strings.HasPrefix(attr.Value, "127 "):
				m.pt127 = true
			}
		}
		media = append(media, m)
	}
	return offer.SDP, media
}

func requireMedia(t *testing.T, sdp string, media []offeredMedia, kind, direction string) offeredMedia {
	t.Helper()
	var found []offeredMedia
	for _, m := range media {
		if m.kind == kind {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want one %s m-line, got %d:\n%s", kind, len(found), sdp)
	}
	if found[0].direction != direction {
		t.Fatalf("%s m-line direction = %q, want %q:\n%s", kind, found[0].direction, direction, sdp)
	}
	return found[0]
}

// The SFU reserves PT 127 for ULPFEC and routes the host's PT 127 packets as
// FEC parity (01-SFU.md §6.1). Pion's default codecs put H.264 at PT 127.
func requireH264AtPT96(t *testing.T, sdp string, video offeredMedia) {
	t.Helper()
	if !video.h264PT96 {
		t.Fatalf("video m-line does not offer H.264 at PT 96:\n%s", sdp)
	}
	if video.pt127 {
		t.Fatalf("video m-line offers a codec at PT 127, which the SFU reserves for ULPFEC:\n%s", sdp)
	}
}

// The SFU rejects a stream viewer offer without a recvonly audio m-line
// (01-SFU.md §6.1, "no offered stream audio transceiver").
func TestViewerOfferCarriesRecvonlyOpusAudio(t *testing.T) {
	sdp, media := streamRoleOffer(t, "viewer")
	requireH264AtPT96(t, sdp, requireMedia(t, sdp, media, "video", "recvonly"))
	audio := requireMedia(t, sdp, media, "audio", "recvonly")
	if !audio.opus111 {
		t.Fatalf("viewer audio m-line does not offer Opus PT 111 at 48000/2:\n%s", sdp)
	}
	if !strings.Contains(sdp, "m=application ") {
		t.Fatalf("viewer offer has no control DataChannel m-line:\n%s", sdp)
	}
}

func TestHostOfferCarriesSendonlyVideoAndOpusAudio(t *testing.T) {
	sdp, media := streamRoleOffer(t, "host")
	requireH264AtPT96(t, sdp, requireMedia(t, sdp, media, "video", "sendonly"))
	audio := requireMedia(t, sdp, media, "audio", "sendonly")
	if !audio.opus111 {
		t.Fatalf("host audio m-line does not offer Opus PT 111 at 48000/2:\n%s", sdp)
	}
}

// The SFU attaches viewer audio to an offered audio transceiver that has no
// sender yet (Peer.addStreamAudioTrackFor). Check that a Pion answerer sees
// one for the viewer offer.
func TestViewerOfferGivesAnswererAnAudioTransceiverWithoutSender(t *testing.T) {
	viewer := newStreamTestPC(t)
	if _, err := viewer.CreateDataChannel("control", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := addStreamMedia(viewer, "viewer", "offer-shape"); err != nil {
		t.Fatal(err)
	}
	offer, err := viewer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}

	answerer, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer answerer.Close()
	if err := answerer.SetRemoteDescription(offer); err != nil {
		t.Fatal(err)
	}
	for _, tr := range answerer.GetTransceivers() {
		if tr.Kind() == webrtc.RTPCodecTypeAudio && tr.Sender() == nil {
			return
		}
	}
	t.Fatalf("answerer has no audio transceiver without a sender:\n%s", offer.SDP)
}

func TestAddStreamMediaRejectsUnknownRole(t *testing.T) {
	pc := newStreamTestPC(t)
	if _, _, err := addStreamMedia(pc, "spectator", "offer-shape"); err == nil {
		t.Fatal("want error for unknown role")
	}
}
