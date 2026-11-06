package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

type VoiceSession struct {
	id        string
	crewID    string
	channelID string
	members   *PeerSet
	createdAt time.Time
	logger    *logging.Logger

	// mu guards state and drainingAt, and makes each membership change one
	// step: a join takes its member snapshot, sends member_joined and adds
	// the peer under mu, so two joins at the same time see each other.
	mu         sync.Mutex
	state      SessionState
	drainingAt time.Time

	hadRealPeer atomic.Bool
}

func (s *VoiceSession) HadRealPeer() bool { return s.hadRealPeer.Load() }

func NewVoiceSession(id, crewID, channelID string, logger *logging.Logger) *VoiceSession {
	return &VoiceSession{
		id:        id,
		crewID:    crewID,
		channelID: channelID,
		state:     SessionActive,
		members:   NewPeerSet(),
		createdAt: time.Now(),
		logger:    logger,
	}
}

func (s *VoiceSession) ID() string           { return s.id }
func (s *VoiceSession) Type() string         { return "voice" }
func (s *VoiceSession) CrewID() string       { return s.crewID }
func (s *VoiceSession) CreatedAt() time.Time { return s.createdAt }
func (s *VoiceSession) MemberCount() int     { return s.members.Len() }

func (s *VoiceSession) State() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *VoiceSession) DrainingAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drainingAt
}

// finalizeDrain closes the session when it has drained for longer than
// timeout. A join that holds a stale pointer to a closed session gets
// ErrSessionClosed and does not bring it back to life outside the store.
func (s *VoiceSession) finalizeDrain(now time.Time, timeout time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != SessionDraining || s.drainingAt.IsZero() ||
		now.Sub(s.drainingAt) <= timeout {
		return false
	}
	s.state = SessionClosed
	return true
}

func (s *VoiceSession) AllPeers() []*Peer {
	return s.members.Snapshot()
}

func (s *VoiceSession) Members() []MemberInfo {
	var members []MemberInfo
	s.members.ForEach(func(p *Peer) {
		members = append(members, MemberInfo{UserID: p.UserID, Role: "member"})
	})
	return members
}

func (s *VoiceSession) AddPeer(peer *Peer) error {
	_, err := s.Join(peer)
	return err
}

// Join adds peer and returns the other members at the moment it joined.
// Every member not in that snapshot gets member_joined for peer, so two
// joins at the same time each learn about the other in exactly one of
// joined.members and member_joined (01-SFU.md §4.2).
func (s *VoiceSession) Join(peer *Peer) ([]MemberInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SessionClosed {
		return nil, ErrSessionClosed
	}
	if s.state == SessionDraining {
		s.state = SessionActive
		s.drainingAt = time.Time{}
	}

	if old := s.members.Remove(peer.UserID); old != nil {
		old.Session = nil
		old.Close()

		// Reconnects must clean up stale forwarding state on remaining peers
		// exactly like a regular leave path to avoid one-way audio.
		s.members.ForEach(func(existing *Peer) {
			needsReneg := existing.RemoveOutgoingTrack(peer.UserID)
			existing.SendSignaling("member_left", MemberEventData{
				UserID: peer.UserID,
				Reason: "reconnect",
			})
			if !needsReneg {
				// A receive slot was released: no m-line changed.
				return
			}
			if err := existing.Renegotiate(); err != nil {
				s.logger.Warn("renegotiate_on_reconnect_failed", logging.D{
					"session": s.id,
					"user":    existing.UserID,
					"left":    peer.UserID,
					"error":   err.Error(),
				})
			}
		})

		s.logger.Info("peer_evicted", logging.D{
			"session":      s.id,
			"user":         peer.UserID,
			"reason":       "reconnect",
			"pkts_sent":    old.PacketsSent.Load(),
			"pkts_recv":    old.PacketsReceived.Load(),
			"pkts_dropped": old.PacketsDropped.Load(),
			"duration_sec": int(time.Since(old.ConnectedAt).Seconds()),
		})
	}

	var members []MemberInfo
	s.members.ForEach(func(existing *Peer) {
		members = append(members, MemberInfo{UserID: existing.UserID, Role: "member"})
		existing.SendSignaling("member_joined", MemberEventData{
			UserID: peer.UserID,
			Role:   "member",
		})
	})

	s.members.Add(peer)
	peer.Session = s
	if !peer.Synthetic {
		s.hadRealPeer.Store(true)
	}

	s.logger.Info("peer_joined", logging.D{
		"session": s.id,
		"user":    peer.UserID,
		"role":    "member",
		"members": s.members.Len(),
	})
	return members, nil
}

// SetupOutgoingTracks forwards audio between newPeer and every
// already-connected peer. Peers whose ICE hasn't connected yet are skipped;
// their own OnConnectionStateChange will call this for them.
//
// Each direction first takes a free receive slot of the receiver
// (AssignVoiceSlot): no offer, no answer. Only a receiver with no free slot
// gets a new m-line (AddOutgoingTrack) and a renegotiation.
// Returns (added tracks for newPeer, connected peers considered, whether
// newPeer needs a renegotiation).
func (s *VoiceSession) SetupOutgoingTracks(newPeer *Peer) (int, int, bool) {
	added := 0
	connectedPeers := 0
	newPeerNeedsReneg := false
	s.members.ForEach(func(existing *Peer) {
		if existing.UserID == newPeer.UserID {
			return
		}
		if pc, _ := existing.currentPeerConnection(); pc == nil ||
			pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
			return
		}
		connectedPeers++

		if err := newPeer.AssignVoiceSlot(existing.UserID); err == nil {
			added++
		} else if _, err := newPeer.AddOutgoingTrack(existing.UserID); err != nil {
			s.logger.Warn("track_add_failed", logging.D{
				"session":  s.id,
				"receiver": newPeer.UserID,
				"sender":   existing.UserID,
				"error":    err.Error(),
			})
		} else {
			added++
			newPeerNeedsReneg = true
		}

		if err := existing.AssignVoiceSlot(newPeer.UserID); err == nil {
			return
		}
		if _, err := existing.AddOutgoingTrack(newPeer.UserID); err != nil {
			s.logger.Warn("track_add_failed", logging.D{
				"session":  s.id,
				"receiver": existing.UserID,
				"sender":   newPeer.UserID,
				"error":    err.Error(),
			})
		} else {
			if err := existing.Renegotiate(); err != nil {
				s.logger.Warn("renegotiate_failed", logging.D{
					"session": s.id,
					"user":    existing.UserID,
					"error":   err.Error(),
				})
			}
		}
	})
	return added, connectedPeers, newPeerNeedsReneg
}

// WireAudioTrack starts a goroutine that reads RTP from the peer's incoming
// audio track and forwards it to all other peers' outgoing tracks.
// Skips re-wire only when this exact track is already being read.
func (s *VoiceSession) WireAudioTrack(peer *Peer, track *webrtc.TrackRemote) {
	senderID := peer.UserID
	trackSSRC := track.SSRC()

	peer.tracksMu.Lock()
	if peer.audioWiringIsCurrentLocked(track) {
		peer.tracksMu.Unlock()
		s.logger.Debug("audio_track_wire_skipped", logging.D{
			"session": s.id,
			"user":    senderID,
			"ssrc":    trackSSRC,
			"reason":  "same track already wired",
		})
		return
	}
	if peer.audioCancel != nil {
		peer.audioCancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	peer.audioReadToken++
	readToken := peer.audioReadToken
	peer.audioCancel = cancel
	peer.wiredAudioTrack = track
	peer.audioReadActive = true
	peer.audioReadDone = done
	peer.tracksMu.Unlock()

	peer.TrackWireCount.Add(1)

	s.logger.Info("audio_track_wired", logging.D{
		"session":    s.id,
		"user":       senderID,
		"codec":      track.Codec().MimeType,
		"ssrc":       trackSSRC,
		"wire_count": peer.TrackWireCount.Load(),
	})

	go func() {
		defer func() {
			peer.tracksMu.Lock()
			// Only clear if this goroutine still owns the active wiring.
			if peer.audioReadToken == readToken {
				peer.audioCancel = nil
				peer.wiredAudioTrack = nil
				peer.audioReadActive = false
				peer.audioReadDone = nil
			}
			peer.tracksMu.Unlock()
			cancel()
			close(done)
		}()
		consecutiveErrors := 0
		for {
			select {
			case <-ctx.Done():
				s.logger.Debug("audio_track_cancelled", logging.D{
					"session": s.id,
					"user":    senderID,
					"ssrc":    trackSSRC,
				})
				return
			default:
			}
			pkt, _, err := track.ReadRTP()
			if err != nil {
				if ctx.Err() != nil {
					// Cancelled (Peer.Close or a newer wiring); the read
					// deadline or the closed track ended the read.
					s.logger.Debug("audio_track_cancelled", logging.D{
						"session": s.id,
						"user":    senderID,
						"ssrc":    trackSSRC,
					})
					return
				}
				consecutiveErrors++
				s.logger.Warn("audio_track_read_error", logging.D{
					"session":     s.id,
					"user":        senderID,
					"ssrc":        trackSSRC,
					"error":       err.Error(),
					"consecutive": consecutiveErrors,
				})
				// Transient errors during renegotiation are common (the inbound
				// track is mid-swap). While the sender is actively
				// renegotiating, tolerate far more before giving up; a high
				// backstop still prevents a permanently-stuck reader from
				// leaking if an answer never arrives.
				limit := 50
				if peer.renegotiating.Load() {
					limit = 500
				}
				if consecutiveErrors > limit {
					s.logger.Error("audio_track_ended", logging.D{
						"session":     s.id,
						"user":        senderID,
						"ssrc":        trackSSRC,
						"reason":      "too many consecutive errors",
						"limit":       limit,
						"renegotiate": peer.renegotiating.Load(),
					})
					return
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			consecutiveErrors = 0
			peer.PacketsReceived.Add(1)
			peer.BytesReceived.Add(int64(pkt.MarshalSize()))

			s.members.ForEach(func(other *Peer) {
				if other.UserID != senderID {
					other.SendRTP(senderID, pkt)
				}
			})
		}
	}()
}

// SetSpeaking records peer's in-band speaking state and sends a change to
// every other member's control channel (01-SFU.md §5.4).
func (s *VoiceSession) SetSpeaking(peer *Peer, on bool) {
	if peer.speaking.Swap(on) == on {
		return
	}
	msg := encodeSpeakingMessage(peer.UserID, on)
	s.members.ForEach(func(other *Peer) {
		if other.UserID != peer.UserID {
			other.SendControlText(msg)
		}
	})
	s.logger.Debug("voice_speaking", logging.D{
		"session": s.id,
		"user":    peer.UserID,
		"on":      on,
	})
}

// SendControlSnapshot sends peer the state it missed before its control
// channel opened: its whole slot map and every member that speaks now.
func (s *VoiceSession) SendControlSnapshot(peer *Peer) {
	peer.SendSlotMap()
	s.members.ForEach(func(other *Peer) {
		if other.UserID != peer.UserID && other.speaking.Load() {
			peer.SendControlText(encodeSpeakingMessage(other.UserID, true))
		}
	})
}

// isMemberLocked reports whether peer is the current member for its user
// id. Call it with mu held: every membership write holds mu.
func (s *VoiceSession) isMemberLocked(peer *Peer) bool {
	for _, m := range s.members.Snapshot() {
		if m == peer {
			return true
		}
	}
	return false
}

// WireDataChannels is kept for the control channel in voice sessions.
// Media forwarding is handled via RTP tracks (WireAudioTrack).
func (s *VoiceSession) WireDataChannels(_ *Peer) {}

// RemovePeer removes the member with userID, whichever peer instance it is.
func (s *VoiceSession) RemovePeer(userID string) {
	s.removeMember(userID, nil)
}

// RemovePeerInstance removes peer only while it is the member for its user
// id. A late cleanup of a peer that a reconnect evicted (its signaling loop
// defer, an ICE timer, the slow-peer check) then cannot remove the new peer
// with the same user id.
func (s *VoiceSession) RemovePeerInstance(peer *Peer) {
	if peer == nil {
		return
	}
	s.removeMember(peer.UserID, peer)
}

// removeMember removes the member with userID. A non-nil instance must be
// that member, or nothing is removed and only instance is closed.
func (s *VoiceSession) removeMember(userID string, instance *Peer) {
	s.mu.Lock()
	if instance != nil && !s.isMemberLocked(instance) {
		s.mu.Unlock()
		instance.Close()
		return
	}
	removed := s.members.Remove(userID)
	if removed != nil && s.members.Len() == 0 {
		s.state = SessionDraining
		s.drainingAt = time.Now()
	}
	s.mu.Unlock()
	if removed == nil {
		return
	}
	removed.Close()

	s.members.ForEach(func(m *Peer) {
		needsReneg := m.RemoveOutgoingTrack(userID)
		m.SendSignaling("member_left", MemberEventData{
			UserID: userID,
			Reason: "disconnect",
		})
		if !needsReneg {
			// A receive slot was released: no m-line changed.
			return
		}
		if err := m.Renegotiate(); err != nil {
			s.logger.Warn("renegotiate_on_leave_failed", logging.D{
				"session": s.id,
				"user":    m.UserID,
				"left":    userID,
				"error":   err.Error(),
			})
		}
	})

	s.logger.Info("peer_left", logging.D{
		"session":      s.id,
		"user":         userID,
		"members":      s.members.Len(),
		"pkts_sent":    removed.PacketsSent.Load(),
		"pkts_recv":    removed.PacketsReceived.Load(),
		"pkts_dropped": removed.PacketsDropped.Load(),
		"bytes_sent":   removed.BytesSent.Load(),
		"bytes_recv":   removed.BytesReceived.Load(),
		"peak_buf_pct": removed.BufferUsage.Load(),
		"duration_sec": int(time.Since(removed.ConnectedAt).Seconds()),
	})
}
