package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mollohq/m3llo-sfu/internal/auth"
)

// dialSignalingLoop runs the signaling loop for claims on a real socket and
// returns the client end.
func dialSignalingLoop(t *testing.T, s *Server, claims *auth.Claims) *websocket.Conn {
	t.Helper()
	serverConn := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- c
	}))
	t.Cleanup(ts.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	peer := NewPeer(claims.UserID, claims.Role, <-serverConn, s.Logger)
	go s.handleSignalingLoop(peer, claims)
	return client
}

func sendSignaling(t *testing.T, c *websocket.Conn, msgType string, data any) {
	t.Helper()
	d, _ := json.Marshal(data)
	raw, _ := json.Marshal(SignalingMessage{Type: msgType, Data: d})
	if err := c.WriteMessage(websocket.TextMessage, raw); err != nil {
		t.Fatalf("send %s: %v", msgType, err)
	}
}

// nextReply returns the next joined or error message.
func nextReply(t *testing.T, c *websocket.Conn) SignalingMessage {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, raw, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var msg SignalingMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if msg.Type == "joined" || msg.Type == "error" {
			return msg
		}
	}
}

func errorCode(t *testing.T, msg SignalingMessage) string {
	t.Helper()
	if msg.Type != "error" {
		t.Fatalf("got %s, want an error", msg.Type)
	}
	var e ErrorData
	_ = json.Unmarshal(msg.Data, &e)
	return e.Code
}

// A socket joins one session. A second join_voice is refused: it used to
// leave the peer in the first session for good, so that session never
// drained and one socket could fill every session slot.
func TestSecondJoinOnOneSocketIsRefused(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	claims := &auth.Claims{UserID: "u1", Type: "voice", Role: "member", CrewID: "crew"}
	client := dialSignalingLoop(t, s, claims)

	sendSignaling(t, client, "join_voice", JoinVoiceData{CrewID: "crew", ChannelID: "ch-1"})
	if msg := nextReply(t, client); msg.Type != "joined" {
		t.Fatalf("first join: got %s", msg.Type)
	}
	sendSignaling(t, client, "join_voice", JoinVoiceData{CrewID: "crew", ChannelID: "ch-2"})
	if code := errorCode(t, nextReply(t, client)); code != "INVALID_REQUEST" {
		t.Fatalf("second join: got %s, want INVALID_REQUEST", code)
	}
	if _, ok := s.sessions.Get("voice:crew:ch-2"); ok {
		t.Fatal("the second join created a session")
	}
}

// A voice token names its channel. A join for another channel of the same
// crew is refused.
func TestVoiceJoinMustMatchTheTokenChannel(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	claims := &auth.Claims{UserID: "u1", Type: "voice", Role: "member", CrewID: "crew", ChannelID: "ch-1"}
	client := dialSignalingLoop(t, s, claims)

	sendSignaling(t, client, "join_voice", JoinVoiceData{CrewID: "crew", ChannelID: "ch-2"})
	if code := errorCode(t, nextReply(t, client)); code != "INVALID_TOKEN" {
		t.Fatalf("got %s, want INVALID_TOKEN", code)
	}
	sendSignaling(t, client, "join_voice", JoinVoiceData{CrewID: "crew", ChannelID: "ch-1"})
	if msg := nextReply(t, client); msg.Type != "joined" {
		t.Fatalf("join for the token's channel: got %s", msg.Type)
	}
}

// The SFU closes a socket that sends a message over the cap instead of
// buffering it.
func TestOversizedSignalingMessageClosesTheSocket(t *testing.T) {
	s := newStreamTestServer(newTestAPI())
	s.config.TestPassword = "tp"
	ts := httptest.NewServer(http.HandlerFunc(s.handleWebSocket))
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws?test_password=tp&test_type=voice"
	client, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	big := `{"type":"` + strings.Repeat("x", maxSignalingMessage) + `"}`
	_ = client.WriteMessage(websocket.TextMessage, []byte(big))
	_ = client.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, _, err := client.ReadMessage()
		if err == nil {
			continue
		}
		if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
			t.Fatalf("got %v, want close 1009 (message too big)", err)
		}
		return
	}
}
