package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const streamAudioQueueSize = 32

type streamAudioIngress struct {
	peer              *Peer
	track             *webrtc.TrackRemote
	pc                *webrtc.PeerConnection
	pcGeneration      uint64
	sessionGeneration uint64
	token             uint64
	cancel            func()
}

type streamViewerAudio struct {
	peer   *Peer
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *rtp.Packet
	// write sends one packet on the leg. It is the viewer's audio track;
	// tests replace it.
	write func(*rtp.Packet) error
}

func streamAudioCodecCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: 48000,
		Channels:  2,
	}
}

// WireAudioTrack owns the stream host's single Opus ingress loop.
func (s *StreamSession) WireAudioTrack(peer *Peer, track *webrtc.TrackRemote) {
	pc, generation := peer.currentPeerConnection()
	s.WireAudioTrackFor(peer, track, pc, generation)
}

func (s *StreamSession) WireAudioTrackFor(
	peer *Peer,
	track *webrtc.TrackRemote,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) {
	if peer == nil || track == nil ||
		peer.Role != "host" ||
		!peer.isCurrentPeerConnection(pc, pcGeneration) ||
		!strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
		return
	}

	session, sessionGeneration := peer.currentSession()
	s.videoMu.Lock()
	if s.host != peer || session != s ||
		!peer.isCurrentSession(s, sessionGeneration) {
		s.videoMu.Unlock()
		return
	}
	if current := s.audioIngress; current != nil && current.peer == peer && current.track == track {
		s.videoMu.Unlock()
		return
	}
	s.cancelAudioIngressLocked(peer)

	ctx, cancelContext := context.WithCancel(context.Background())
	cancel := func() {
		cancelContext()
		_ = track.SetReadDeadline(time.Now())
	}
	s.audioIngressToken++
	token := s.audioIngressToken
	s.audioIngress = &streamAudioIngress{
		peer:              peer,
		track:             track,
		pc:                pc,
		pcGeneration:      pcGeneration,
		sessionGeneration: sessionGeneration,
		token:             token,
		cancel:            cancel,
	}
	s.videoMu.Unlock()

	peer.TrackWireCount.Add(1)
	s.logger.Info("stream_audio_track_wired", logging.D{
		"session":    s.id,
		"user":       peer.UserID,
		"codec":      track.Codec().MimeType,
		"ssrc":       track.SSRC(),
		"wire_count": peer.TrackWireCount.Load(),
	})

	go s.readHostAudioRTP(ctx, cancel, peer, track, token)
}

func (s *StreamSession) cancelAudioIngressLocked(peer *Peer) {
	if s.audioIngress != nil && (peer == nil || s.audioIngress.peer == peer) {
		s.logIngressCancelledLocked("audio", s.audioIngress.peer, s.audioIngress.track)
		s.audioIngress.cancel()
		s.audioIngress = nil
	}
}

func (s *StreamSession) readHostAudioRTP(
	ctx context.Context,
	cancel func(),
	peer *Peer,
	track *webrtc.TrackRemote,
	token uint64,
) {
	defer func() {
		s.videoMu.Lock()
		if s.audioIngress != nil && s.audioIngress.token == token {
			s.audioIngress = nil
		}
		s.videoMu.Unlock()
		cancel()
	}()

	for {
		pkt, _, err := track.ReadRTP()
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Info("stream_audio_track_ended", logging.D{
					"session": s.id,
					"user":    peer.UserID,
					"ssrc":    track.SSRC(),
					"error":   err.Error(),
				})
			}
			return
		}

		s.videoMu.Lock()
		active := s.audioIngress != nil &&
			s.audioIngress.token == token &&
			s.host == peer &&
			peer.isCurrentConnectionSession(
				s.audioIngress.pc,
				s.audioIngress.pcGeneration,
				s,
				s.audioIngress.sessionGeneration,
			)
		s.videoMu.Unlock()
		if !active {
			return
		}

		packetSize := int64(pkt.MarshalSize())
		peer.PacketsReceived.Add(1)
		peer.BytesReceived.Add(packetSize)
		s.fanOutAudioRTPFrom(token, pkt)
	}
}

func (s *StreamSession) fanOutAudioRTP(pkt *rtp.Packet) {
	s.fanOutAudioRTPFrom(0, pkt)
}

func (s *StreamSession) fanOutAudioRTPFrom(token uint64, pkt *rtp.Packet) {
	s.videoMu.Lock()
	pkt = s.audioSeq.rewrite(token, pkt)
	legs := make([]*streamViewerAudio, 0, len(s.viewerAudio))
	for _, leg := range s.viewerAudio {
		legs = append(legs, leg)
	}
	s.videoMu.Unlock()

	packetSize := int64(pkt.MarshalSize())
	for _, leg := range legs {
		select {
		case <-leg.ctx.Done():
		case leg.queue <- pkt.Clone():
		default:
			leg.peer.PacketsDropped.Add(1)
		}
		_ = packetSize
	}
}

func (s *StreamSession) PrepareViewerAudio(
	peer *Peer,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) (*webrtc.TrackLocalStaticRTP, uint64, bool, error) {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	if s.state == SessionClosed || peer == nil || peer.Role != "viewer" {
		return nil, 0, false, ErrSessionClosed
	}
	session, generation := peer.currentSession()
	if session != s {
		return nil, 0, false, ErrSessionClosed
	}
	isCurrentViewer := false
	for _, viewer := range s.viewers.Snapshot() {
		if viewer == peer {
			isCurrentViewer = true
			break
		}
	}
	if !isCurrentViewer {
		return nil, 0, false, ErrSessionClosed
	}
	track, sender, _ := peer.streamAudioLeg()
	if sender != nil {
		return track, generation, false, nil
	}
	track, err := peer.addStreamAudioTrackFor(pc, pcGeneration)
	if err != nil {
		return nil, 0, false, err
	}
	return track, generation, true, nil
}

func (s *StreamSession) ActivateViewerAudio(
	peer *Peer,
	expectedTrack *webrtc.TrackLocalStaticRTP,
	generation uint64,
) bool {
	if peer == nil || expectedTrack == nil {
		return false
	}
	s.videoMu.Lock()
	if s.state == SessionClosed ||
		!peer.isCurrentSession(s, generation) {
		s.videoMu.Unlock()
		return false
	}
	isCurrentViewer := false
	for _, viewer := range s.viewers.Snapshot() {
		if viewer == peer {
			isCurrentViewer = true
			break
		}
	}
	if !isCurrentViewer {
		s.videoMu.Unlock()
		return false
	}
	track, sender, lifecycle := peer.streamAudioLeg()
	if sender == nil || lifecycle == nil || track != expectedTrack {
		s.videoMu.Unlock()
		return false
	}
	if current := s.viewerAudio[peer.UserID]; current != nil &&
		current.peer == peer && current.sender == sender && current.track == track {
		s.videoMu.Unlock()
		return true
	}
	var replaced *streamViewerAudio
	if current := s.viewerAudio[peer.UserID]; current != nil {
		s.cancelViewerAudioLocked(current)
		replaced = current
	}
	ctx, cancel := context.WithCancel(lifecycle)
	leg := &streamViewerAudio{
		peer:   peer,
		track:  track,
		sender: sender,
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan *rtp.Packet, streamAudioQueueSize),
		write: func(pkt *rtp.Packet) error {
			return peer.SendStreamAudioRTPFor(track, pkt)
		},
	}
	s.viewerAudio[peer.UserID] = leg
	s.videoMu.Unlock()
	s.closeViewerAudioLeg(replaced)
	go s.writeViewerAudioRTP(leg)
	return true
}

func (s *StreamSession) cancelViewerAudioLocked(leg *streamViewerAudio) {
	if leg == nil {
		return
	}
	leg.cancel()
}

func (s *StreamSession) closeViewerAudioLeg(leg *streamViewerAudio) {
	if leg == nil {
		return
	}
	leg.cancel()
}

func (s *StreamSession) writeViewerAudioRTP(leg *streamViewerAudio) {
	for {
		select {
		case <-leg.ctx.Done():
			return
		case pkt := <-leg.queue:
			if leg.ctx.Err() != nil {
				return
			}
			if err := leg.write(pkt); err != nil {
				// Only a closed or replaced leg ends the writer. Any other
				// error is about this packet (a full socket buffer, a
				// transport in an ICE restart): the writer used to exit
				// on it, and the viewer had no game audio after that.
				if leg.ctx.Err() != nil ||
					errors.Is(err, ErrNoStreamAudioTrack) ||
					errors.Is(err, ErrStaleStreamAudioTrack) {
					return
				}
				leg.peer.PacketsDropped.Add(1)
				continue
			}
			leg.peer.PacketsSent.Add(1)
			leg.peer.BytesSent.Add(int64(pkt.MarshalSize()))
		}
	}
}

func (s *StreamSession) teardownViewerAudioLocked(peer *Peer) {
	if peer == nil {
		return
	}
	if leg := s.viewerAudio[peer.UserID]; leg != nil {
		s.cancelViewerAudioLocked(leg)
		delete(s.viewerAudio, peer.UserID)
		peer.closeStreamAudioTrack(leg.track)
	}
}
