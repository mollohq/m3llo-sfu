package server

import (
	"errors"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

func TestStreamReactivationPreventsDrainCleanup(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewStreamSession("stream", "crew", logger, streamSnapshotConfig{})
	store.sessions[session.ID()] = session

	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()

	host := &Peer{UserID: "host", Role: "host"}
	if err := session.AddPeer(host); err != nil {
		t.Fatalf("reactivate stream: %v", err)
	}
	store.cleanDraining()

	if got, ok := store.Get(session.ID()); !ok || got != session {
		t.Fatal("drain cleanup deleted a reactivated stream session")
	}
	if session.State() != SessionActive || !session.DrainingAt().IsZero() {
		t.Fatalf("reactivated lifecycle = state %s drainingAt %v",
			session.State(), session.DrainingAt())
	}
}

func TestFinalizedStreamRejectsStaleReactivation(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewStreamSession("stream", "crew", logger, streamSnapshotConfig{})
	store.sessions[session.ID()] = session

	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()
	store.cleanDraining()

	if _, ok := store.Get(session.ID()); ok {
		t.Fatal("expired stream session remained in store")
	}
	if err := session.AddPeer(&Peer{UserID: "late-host", Role: "host"}); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("stale reactivation error = %v, want %v", err, ErrSessionClosed)
	}
}

func TestStreamDrainTeardownDoesNotHoldStoreLock(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewStreamSession("stream", "crew", logger, streamSnapshotConfig{})
	store.sessions[session.ID()] = session

	teardownStarted := make(chan struct{})
	releaseTeardown := make(chan struct{})
	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.finalizeTeardownHook = func() {
		close(teardownStarted)
		<-releaseTeardown
	}
	session.videoMu.Unlock()

	cleanupDone := make(chan struct{})
	go func() {
		store.cleanDraining()
		close(cleanupDone)
	}()
	select {
	case <-teardownStarted:
	case <-time.After(time.Second):
		t.Fatal("stream teardown did not start")
	}

	getDone := make(chan bool, 1)
	go func() {
		_, ok := store.Get(session.ID())
		getDone <- ok
	}()
	select {
	case exists := <-getDone:
		if exists {
			t.Fatal("finalized session remained visible during teardown")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("SessionStore.Get blocked behind stream teardown")
	}

	close(releaseTeardown)
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("stream cleanup did not finish")
	}
}
