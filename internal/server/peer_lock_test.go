package server

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

// Renegotiate reads the session ID while it holds pcMu for read. A second
// read lock in the same goroutine deadlocks when Close asks for the write
// lock between the two: Go blocks new readers behind a waiting writer, and
// the writer waits for the first read lock. The test runs Renegotiate
// against the detach that Close does. It fails if the two do not stop in
// time; the suite does not hang.
func TestRenegotiateDoesNotDeadlockWithPeerConnectionDetach(t *testing.T) {
	api := newTestAPI()
	logger := logging.New("error")
	session := NewVoiceSession("voice:c:ch", "c", "ch", logger)
	peer := NewPeer("rx", "member", nil, logger)
	peer.bindSession(session)

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("server PC: %v", err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	if _, err := pc.CreateDataChannel("control", nil); err != nil {
		t.Fatalf("data channel: %v", err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatalf("offer: %v", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatalf("local offer: %v", err)
	}
	// The offer waits for its answer, so each Renegotiate only queues and
	// sends no signaling message.
	peer.installPeerConnection(pc)

	const runFor = time.Second
	const stopWithin = 5 * time.Second
	stop := make(chan struct{})
	var renegotiations, detaches atomic.Int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := peer.Renegotiate(); err != nil && !errors.Is(err, ErrNoPeerConnection) {
				t.Errorf("renegotiate: %v", err)
				return
			}
			renegotiations.Add(1)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			// The same write lock as Close, then a new PeerConnection.
			peer.detachCurrentPeerConnection()
			peer.installPeerConnection(pc)
			detaches.Add(1)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	time.Sleep(runFor)
	close(stop)
	select {
	case <-done:
	case <-time.After(stopWithin):
		t.Fatalf("Renegotiate and detach did not stop in %s: deadlock on pcMu (%d renegotiations, %d detaches)",
			stopWithin, renegotiations.Load(), detaches.Load())
	}
	if renegotiations.Load() == 0 || detaches.Load() == 0 {
		t.Fatalf("no concurrent work: %d renegotiations, %d detaches",
			renegotiations.Load(), detaches.Load())
	}
}
