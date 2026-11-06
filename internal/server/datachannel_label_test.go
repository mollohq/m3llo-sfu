package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/pion/webrtc/v4"
)

// dataChannelTestClient joins a session as user (join_voice or
// join_stream), opens a "control" and an unreliable "audio" DataChannel and
// connects through handleOffer. It also opens a channel with the label
// "media". It returns the three channels.
func dataChannelTestClient(t *testing.T, s *Server, voice bool) (control, audio, media *webrtc.DataChannel) {
	t.Helper()
	return dataChannelTestClientAs(t, s, voice, "viewer")
}

// dataChannelTestClientAs is dataChannelTestClient for a stream member with
// role ("viewer" or "host"; a host sends video).
func dataChannelTestClientAs(t *testing.T, s *Server, voice bool, role string) (control, audio, media *webrtc.DataChannel) {
	t.Helper()
	api := newTestAPI()
	probe := newWSProbe(t)
	var peer *Peer
	var claims *auth.Claims
	if voice {
		peer = NewPeer("member", "member", probe.server, s.Logger)
		claims = &auth.Claims{UserID: "member", Type: "voice", CrewID: "crew"}
		join, _ := json.Marshal(JoinVoiceData{CrewID: "crew", ChannelID: "ch"})
		s.handleJoinVoice(peer, claims, SignalingMessage{Type: "join_voice", Data: join})
	} else {
		peer = NewPeer(role, role, probe.server, s.Logger)
		claims = &auth.Claims{UserID: role, Type: "stream"}
		join, _ := json.Marshal(JoinStreamData{SessionID: "dc-labels", Role: role})
		s.handleJoinStream(peer, claims, SignalingMessage{Type: "join_stream", Data: join})
	}
	waitFor(t, "joined", func() bool { return probe.count("joined") == 1 })

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("client PC: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if voice {
		mic, err := webrtc.NewTrackLocalStaticRTP(streamAudioCodecCapability(), "audio", "member")
		if err != nil {
			t.Fatalf("mic track: %v", err)
		}
		if _, err := pc.AddTrack(mic); err != nil {
			t.Fatalf("add mic: %v", err)
		}
	} else if role == "host" {
		video, err := webrtc.NewTrackLocalStaticRTP(streamVideoCodecCapability(), "video", "host")
		if err != nil {
			t.Fatalf("host video track: %v", err)
		}
		if _, err := pc.AddTrack(video); err != nil {
			t.Fatalf("add host video: %v", err)
		}
	} else {
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
			if _, err := pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{
				Direction: webrtc.RTPTransceiverDirectionRecvonly,
			}); err != nil {
				t.Fatalf("add %s: %v", kind, err)
			}
		}
	}
	control, err = pc.CreateDataChannel("control", nil)
	if err != nil {
		t.Fatalf("control channel: %v", err)
	}
	unordered := false
	retransmits := uint16(0)
	audio, err = pc.CreateDataChannel("audio", &webrtc.DataChannelInit{
		Ordered:        &unordered,
		MaxRetransmits: &retransmits,
	})
	if err != nil {
		t.Fatalf("audio channel: %v", err)
	}
	media, err = pc.CreateDataChannel("media", nil)
	if err != nil {
		t.Fatalf("media channel: %v", err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	gathered := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("set offer: %v", err)
	}
	<-gathered
	data, _ := json.Marshal(SDPData{SDP: pc.LocalDescription().SDP})
	s.handleOffer(peer, claims, SignalingMessage{Type: "offer", Data: data})
	waitFor(t, "answer", func() bool { return probe.count("answer") == 1 })
	var answer SDPData
	_ = json.Unmarshal(probe.messages("answer")[0].Data, &answer)
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		t.Fatalf("apply answer: %v", err)
	}
	applied := 0
	waitFor(t, "client connected", func() bool {
		candidates := probe.messages("ice_candidate")
		for ; applied < len(candidates); applied++ {
			var c ICECandidateData
			_ = json.Unmarshal(candidates[applied].Data, &c)
			index := uint16(c.SDPMLineIndex)
			mid := c.SDPMid
			_ = pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: c.Candidate, SDPMid: &mid, SDPMLineIndex: &index})
		}
		return pc.ConnectionState() == webrtc.PeerConnectionStateConnected
	})
	return control, audio, media
}

// The SFU keeps "control" and closes every other label. A legacy "audio"
// channel (a client before the stream-5 change waits for it) stays open
// with no data path, unless SFU_REJECT_LEGACY_AUDIO_DC is set: then it is
// closed too (01-SFU.md §5.3).
func TestSFUDataChannelLabels(t *testing.T) {
	for _, tc := range []struct {
		name        string
		voice       bool
		rejectAudio bool
	}{
		{"voice", true, false},
		{"stream", false, false},
		{"voice-reject-legacy-audio", true, true},
		{"stream-reject-legacy-audio", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newStreamTestServer(newTestAPI())
			s.config.RejectLegacyAudioDC = tc.rejectAudio
			control, audio, media := dataChannelTestClient(t, s, tc.voice)
			waitFor(t, "the SFU closes the media channel", func() bool {
				return media.ReadyState() == webrtc.DataChannelStateClosed
			})
			waitFor(t, "the control channel opens", func() bool {
				return control.ReadyState() == webrtc.DataChannelStateOpen
			})
			if tc.rejectAudio {
				waitFor(t, "the SFU closes the audio channel", func() bool {
					return audio.ReadyState() == webrtc.DataChannelStateClosed
				})
			} else {
				waitFor(t, "the audio channel opens", func() bool {
					return audio.ReadyState() == webrtc.DataChannelStateOpen
				})
			}
			time.Sleep(200 * time.Millisecond)
			if control.ReadyState() != webrtc.DataChannelStateOpen {
				t.Fatalf("control channel state %s, want open", control.ReadyState())
			}
			if !tc.rejectAudio && audio.ReadyState() != webrtc.DataChannelStateOpen {
				t.Fatalf("legacy audio channel state %s, want open", audio.ReadyState())
			}
		})
	}
}

func TestRejectLegacyAudioDCFromEnv(t *testing.T) {
	t.Setenv("SFU_REJECT_LEGACY_AUDIO_DC", "")
	if ConfigFromEnv().RejectLegacyAudioDC {
		t.Fatal("the legacy audio channel is rejected by default")
	}
	t.Setenv("SFU_REJECT_LEGACY_AUDIO_DC", "true")
	if !ConfigFromEnv().RejectLegacyAudioDC {
		t.Fatal("SFU_REJECT_LEGACY_AUDIO_DC=true does not reject the legacy audio channel")
	}
}

// The SFU answers a control-channel ping on a stream leg, host and viewer,
// with the same text as "pong" (the client's link health reads the pong
// age), and does not forward a viewer's ping to the host. Before, a
// viewer's ping went to the host and a host's was ignored: stream rtt_ms
// was always 0.
func TestStreamControlChannelAnswersPing(t *testing.T) {
	for _, role := range []string{"viewer", "host"} {
		t.Run(role, func(t *testing.T) {
			s := newStreamTestServer(newTestAPI())
			control, _, _ := dataChannelTestClientAs(t, s, false, role)
			pongs := make(chan string, 4)
			control.OnMessage(func(msg webrtc.DataChannelMessage) {
				if msg.IsString {
					pongs <- string(msg.Data)
				}
			})
			waitFor(t, "the control channel opens", func() bool {
				return control.ReadyState() == webrtc.DataChannelStateOpen
			})
			const ping = `{"type":"ping","ts":1234567}`
			var got string
			waitFor(t, "a pong", func() bool {
				_ = control.SendText(ping)
				select {
				case got = <-pongs:
					return true
				case <-time.After(100 * time.Millisecond):
					return false
				}
			})
			if got != `{"type":"pong","ts":1234567}` {
				t.Fatalf("pong %q", got)
			}
			sess, _ := s.sessions.Get("dc-labels")
			if in := sess.(*StreamSession).RelaySnapshot().ControlInPackets; in != 0 {
				t.Fatalf("%d control messages counted for forwarding, want 0: a ping is answered, not forwarded", in)
			}
		})
	}
}
