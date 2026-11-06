package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// A signaling write to a peer whose client reads nothing fails after
// signalingWriteTimeout and closes the socket. Without the deadline the
// write blocked for good once the socket buffers were full, and so did
// every later sender to that peer (they wait for wsMu).
func TestSendSignalingToAStuckClientEndsAtTheWriteDeadline(t *testing.T) {
	old := signalingWriteTimeout
	signalingWriteTimeout = 200 * time.Millisecond
	defer func() { signalingWriteTimeout = old }()

	serverConn := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- c
	}))
	defer ts.Close()
	// The client dials and then never reads.
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()
	peer := NewPeer("stuck", "viewer", <-serverConn, nil)

	payload := strings.Repeat("x", 1<<20)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Up to 256 MiB: far more than the socket buffers hold.
		for i := 0; i < 256; i++ {
			peer.SendSignaling("test_payload", map[string]string{"data": payload})
		}
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("signaling writes to a client that reads nothing did not end")
	}
	// The failed write closed the socket: a read on it now fails at once.
	_ = peer.WS.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := peer.WS.ReadMessage(); err == nil {
		t.Fatal("the socket is still open after the failed write")
	}
}

// Every write of a peer goes through one ordered writer: a direct
// SendSignaling goes out after the messages queued before it, and returns
// only when it is written. Before, the direct write and the queue writer
// were two goroutines that wrote the same connection (go test -race
// reported it), and a direct write could pass queued messages.
func TestSignalingWritesKeepOneOrderAcrossQueuedAndDirectSends(t *testing.T) {
	probe := newWSProbe(t)
	peer := NewPeer("viewer", "viewer", probe.server, nil)
	const queued = 500
	for i := 0; i < queued; i++ {
		peer.SendSignalingQueued("queued", map[string]int{"i": i})
	}
	peer.SendSignaling("direct", struct{}{})
	waitFor(t, "all messages", func() bool { return probe.count("direct") == 1 && probe.count("queued") == queued })
	probe.mu.Lock()
	types := append([]string(nil), probe.types...)
	probe.mu.Unlock()
	if types[len(types)-1] != "direct" {
		for i, typ := range types {
			if typ == "direct" {
				t.Fatalf("the direct message went out as message %d of %d, before queued ones", i+1, len(types))
			}
		}
	}
}
