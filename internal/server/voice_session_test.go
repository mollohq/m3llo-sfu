package server

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
	"github.com/pion/webrtc/v4"
)

// RemovePeer sets the session state and drainingAt while the drain loop
// reads them, and two RemovePeer calls can run at the same time. Run with
// -race: the race detector is the assertion.
func TestVoiceSessionStateDoesNotRaceDrainLoop(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewVoiceSession("voice:c:ch", "c", "ch", logger)
	store.sessions[session.ID()] = session

	const peers = 8
	probes := make([]*wsProbe, peers)
	for i := range probes {
		probes[i] = newWSProbe(t)
	}
	for round := 0; round < 20; round++ {
		for i := 0; i < peers; i++ {
			peer := NewPeer(fmt.Sprintf("u%d", i), "member", probes[i].server, logger)
			if err := session.AddPeer(peer); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		var wg sync.WaitGroup
		for i := 0; i < peers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				p := session.members.Snapshot()
				for _, peer := range p {
					if peer.UserID == fmt.Sprintf("u%d", i) {
						peer.closeOnce.Do(func() {}) // no WS, no PC
					}
				}
				session.RemovePeer(fmt.Sprintf("u%d", i))
			}(i)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				store.cleanDraining()
				_ = session.State()
				_ = session.DrainingAt()
			}
		}()
		wg.Wait()
		if session.State() != SessionDraining || session.DrainingAt().IsZero() {
			t.Fatalf("round %d: empty session state %s drainingAt %v",
				round, session.State(), session.DrainingAt())
		}
		if time.Since(session.DrainingAt()) > time.Second {
			t.Fatalf("round %d: drainingAt not set by the last leave", round)
		}
	}
}

// knownMembers is what a joiner learns about the other members: the
// joined.members snapshot plus every member_joined after it.
func knownMembers(t *testing.T, probe *wsProbe) map[string]bool {
	t.Helper()
	known := map[string]bool{}
	for _, msg := range probe.messages("joined") {
		var data JoinedData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.Fatalf("joined data: %v", err)
		}
		for _, m := range data.Members {
			known[m.UserID] = true
		}
	}
	for _, msg := range probe.messages("member_joined") {
		var data MemberEventData
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			t.Fatalf("member_joined data: %v", err)
		}
		known[data.UserID] = true
	}
	return known
}

// Two members that join a voice channel at the same moment must each learn
// about the other: in joined.members or in member_joined (01-SFU.md §4.2).
func TestSimultaneousVoiceJoinsSeeEachOther(t *testing.T) {
	logger := logging.New("error")
	s := &Server{
		Logger:   logger,
		metrics:  metrics.New(),
		sessions: NewSessionStore(logger),
	}
	const rounds = 500
	missed := 0
	for round := 0; round < rounds; round++ {
		channel := fmt.Sprintf("ch-%d", round)
		join, _ := json.Marshal(JoinVoiceData{CrewID: "crew", ChannelID: channel})
		names := []string{"alice", "bob"}
		probes := make([]*wsProbe, len(names))
		peers := make([]*Peer, len(names))
		for i, name := range names {
			probes[i] = newWSProbe(t)
			peers[i] = NewPeer(name, "member", probes[i].server, logger)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range names {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				s.handleJoinVoice(peers[i], &auth.Claims{UserID: names[i], Type: "voice"},
					SignalingMessage{Type: "join_voice", Data: join})
			}(i)
		}
		close(start)
		wg.Wait()

		for i, probe := range probes {
			other := names[1-i]
			deadline := time.Now().Add(time.Second)
			for !knownMembers(t, probe)[other] && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if !knownMembers(t, probe)[other] {
				missed++
				t.Errorf("round %d: %s never learned about %s", round, names[i], other)
			}
		}
		for _, probe := range probes {
			_ = probe.server.Close()
		}
	}
	if missed > 0 {
		t.Fatalf("%d of %d joins missed the other member", missed, 2*rounds)
	}
}

// The drain loop can close a voice session between a join's store lookup
// and the join itself. The join must not bring the closed session back to
// life outside the store: it joins a new session under the same id.
func TestJoinAfterVoiceDrainFinalizeUsesANewSession(t *testing.T) {
	logger := logging.New("error")
	s := &Server{
		Logger:   logger,
		metrics:  metrics.New(),
		sessions: NewSessionStore(logger),
	}
	stale := NewVoiceSession("voice:crew:ch", "crew", "ch", logger)
	s.sessions.sessions[stale.ID()] = stale
	stale.mu.Lock()
	stale.state = SessionDraining
	stale.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	stale.mu.Unlock()

	s.sessions.cleanDraining()
	if _, ok := s.sessions.Get(stale.ID()); ok {
		t.Fatal("expired voice session remained in the store")
	}
	if _, err := stale.Join(&Peer{UserID: "late", Role: "member"}); err != ErrSessionClosed {
		t.Fatalf("join of a closed session = %v, want ErrSessionClosed", err)
	}
	if stale.MemberCount() != 0 || stale.State() != SessionClosed {
		t.Fatalf("closed session changed: members %d state %s", stale.MemberCount(), stale.State())
	}

	probe := newWSProbe(t)
	peer := NewPeer("alice", "member", probe.server, logger)
	join, _ := json.Marshal(JoinVoiceData{CrewID: "crew", ChannelID: "ch"})
	s.handleJoinVoice(peer, &auth.Claims{UserID: "alice", Type: "voice"},
		SignalingMessage{Type: "join_voice", Data: join})
	got, ok := s.sessions.Get(stale.ID())
	if !ok || got == Session(stale) {
		t.Fatal("the join did not create a new session")
	}
	if cur, _ := peer.currentSession(); cur != got {
		t.Fatal("the peer is not bound to the session in the store")
	}
}

// SetupOutgoingTracks and the admin peer-state counts read other members'
// PeerConnections while those members close. Run with -race; without the
// lock a member that closed between the nil check and the use also crashed
// the SFU with a nil pointer.
func TestPeerConnectionReadsDoNotRaceClose(t *testing.T) {
	api := newTestAPI()
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewVoiceSession("voice:c:ch", "c", "ch", logger)
	store.sessions[session.ID()] = session
	s := &Server{sessions: store}

	joiner := NewPeer("joiner", "member", newWSProbe(t).server, logger)
	for round := 0; round < 20; round++ {
		peers := make([]*Peer, 4)
		for i := range peers {
			peers[i] = NewPeer(fmt.Sprintf("m%d-%d", round, i), "member", newWSProbe(t).server, logger)
			pc, err := api.NewPeerConnection(webrtc.Configuration{})
			if err != nil {
				t.Fatalf("pc: %v", err)
			}
			peers[i].installPeerConnection(pc)
			if err := session.AddPeer(peers[i]); err != nil {
				t.Fatalf("add: %v", err)
			}
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				session.SetupOutgoingTracks(joiner)
				s.collectPeerStateCounts()
			}
		}()
		go func() {
			defer wg.Done()
			for _, p := range peers {
				p.Close()
			}
		}()
		wg.Wait()
		for _, p := range peers {
			session.RemovePeer(p.UserID)
		}
	}
}

// A reconnect evicts the old peer. A late cleanup of that old peer (its
// signaling loop defer, an ICE timer, the slow-peer check) must not remove
// the new peer that has the same user id.
func TestLateCleanupOfEvictedVoicePeerKeepsReplacement(t *testing.T) {
	cleanups := []struct {
		name string
		run  func(s *Server, sess Session, old *Peer)
	}{
		{"removePeerMembership", func(s *Server, _ Session, old *Peer) { s.removePeerMembership(old) }},
		{"removeSlowPeer", func(_ *Server, sess Session, old *Peer) { removeSlowPeer(sess, old) }},
	}
	for _, cleanup := range cleanups {
		t.Run(cleanup.name, func(t *testing.T) {
			logger := logging.New("error")
			s := &Server{
				Logger:   logger,
				metrics:  metrics.New(),
				sessions: NewSessionStore(logger),
			}
			join, _ := json.Marshal(JoinVoiceData{CrewID: "crew", ChannelID: "ch"})
			joinAs := func(user string) *Peer {
				probe := newWSProbe(t)
				peer := NewPeer(user, "member", probe.server, logger)
				s.handleJoinVoice(peer, &auth.Claims{UserID: user, Type: "voice"},
					SignalingMessage{Type: "join_voice", Data: join})
				return peer
			}
			bob := joinAs("bob")
			oldAlice := joinAs("alice")
			newAlice := joinAs("alice")

			sess, ok := s.sessions.Get("voice:crew:ch")
			if !ok {
				t.Fatal("voice session missing")
			}
			cleanup.run(s, sess, oldAlice)

			members := sess.AllPeers()
			if len(members) != 2 {
				t.Fatalf("late cleanup of the evicted peer left %d members, want bob and the new alice", len(members))
			}
			for _, m := range members {
				if m != bob && m != newAlice {
					t.Fatalf("unexpected member %s (%p)", m.UserID, m)
				}
			}
			if current, _ := newAlice.currentSession(); current != sess {
				t.Fatal("late cleanup of the evicted peer unbound the new peer")
			}
		})
	}
}
