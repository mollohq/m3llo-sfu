package server

import (
	"sync"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

type SessionState string

const (
	SessionActive   SessionState = "active"
	SessionDraining SessionState = "draining"
	SessionClosed   SessionState = "closed"
)

const DrainTimeout = 30 * time.Second

type Session interface {
	ID() string
	Type() string // "stream" or "voice"
	State() SessionState
	CrewID() string
	MemberCount() int
	CreatedAt() time.Time
	DrainingAt() time.Time
	AddPeer(peer *Peer) error
	// RemovePeer removes the member with userID, whichever instance it is.
	RemovePeer(userID string)
	// RemovePeerInstance removes peer only while it is the member for its
	// user id, so a late cleanup of a reconnect-evicted peer is a no-op.
	RemovePeerInstance(peer *Peer)
	Members() []MemberInfo
	WireDataChannels(peer *Peer)
	WireAudioTrack(peer *Peer, track *webrtc.TrackRemote)
	AllPeers() []*Peer
	// HadRealPeer reports whether a non-synthetic peer has ever joined. Sessions
	// that only ever held test-mode peers are excluded from /stats.
	HadRealPeer() bool
}

type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]Session
	logger   *logging.Logger
}

func NewSessionStore(logger *logging.Logger) *SessionStore {
	return &SessionStore{
		sessions: make(map[string]Session),
		logger:   logger,
	}
}

func (s *SessionStore) Get(id string) (Session, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	return sess, ok
}

func (s *SessionStore) GetOrCreate(id string, factory func() Session) Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[id]; ok {
		return sess
	}
	sess := factory()
	s.sessions[id] = sess
	return sess
}

func (s *SessionStore) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, id)
}

// RemoveIf removes id only while it maps to sess.
func (s *SessionStore) RemoveIf(id string, sess Session) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions[id] == sess {
		delete(s.sessions, id)
	}
}

func (s *SessionStore) All() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		result = append(result, sess)
	}
	return result
}

// Count returns all sessions, including those only ever used by synthetic
// (test-mode) peers. Used by /health, the peer mesh, and the admin views.
func (s *SessionStore) Count() (stream, voice int) {
	return s.count(false)
}

// CountReal excludes sessions that only ever held synthetic peers (canary,
// /sfu-test, soak tools), so /stats reflects real users.
func (s *SessionStore) CountReal() (stream, voice int) {
	return s.count(true)
}

func (s *SessionStore) count(realOnly bool) (stream, voice int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sess := range s.sessions {
		if realOnly && !sess.HadRealPeer() {
			continue
		}
		switch sess.Type() {
		case "stream":
			stream++
		case "voice":
			voice++
		}
	}
	return
}

// RunDrainLoop garbage-collects sessions that have been draining for > DrainTimeout.
func (s *SessionStore) RunDrainLoop(stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.cleanDraining()
		case <-stop:
			return
		}
	}
}

func (s *SessionStore) cleanDraining() {
	type finalizedSession struct {
		id       string
		session  Session
		teardown func()
	}
	var finalized []finalizedSession

	s.mu.Lock()
	now := time.Now()
	for id, sess := range s.sessions {
		if stream, ok := sess.(*StreamSession); ok {
			teardown, ready := stream.PrepareFinalizeDrain(now, DrainTimeout)
			if !ready || s.sessions[id] != sess {
				continue
			}
			delete(s.sessions, id)
			finalized = append(finalized, finalizedSession{id: id, session: sess, teardown: teardown})
			continue
		}
		if voice, ok := sess.(*VoiceSession); ok {
			// Close under the session lock, so a join that still holds
			// this session cannot reactivate it after it leaves the store.
			if voice.finalizeDrain(now, DrainTimeout) {
				delete(s.sessions, id)
				finalized = append(finalized, finalizedSession{id: id, session: sess})
			}
			continue
		}
		if sess.State() != SessionDraining {
			continue
		}
		da := sess.DrainingAt()
		if da.IsZero() {
			continue
		}
		if now.Sub(da) > DrainTimeout {
			delete(s.sessions, id)
			finalized = append(finalized, finalizedSession{id: id, session: sess})
		}
	}
	s.mu.Unlock()

	for _, item := range finalized {
		if item.teardown != nil {
			item.teardown()
		}
		s.logger.Info("session_closed", logging.D{
			"session":  item.id,
			"type":     item.session.Type(),
			"duration": int(now.Sub(item.session.CreatedAt()).Seconds()),
		})
	}
}
