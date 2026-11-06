// Command sfu-canary is a single-shot synthetic prober for the SFU.
//
// It performs a REAL WebRTC voice join against a running SFU (connect WS, join,
// exchange offer/answer, run ICE + DTLS) and reports whether media could be
// established. This is the check that catches "green but broken": the /health
// endpoint can report ok while every PeerConnection fails (e.g. the expired DTLS
// cert outage of 2026-09-09). A join canary exercises NewPeerConnection with the
// shared cert, exactly the call that breaks.
//
// It uses the test-mode WebSocket path (?test_password=...), so it needs the
// SFU test password (SFU_TEST_PASSWORD) but not a backend-signed JWT. Run one
// per SFU.
//
// Exit codes: 0 = media established (healthy), 1 = probe failed (alert on this).
//
// Example (cron every minute, alert when it exits non-zero):
//
//	SFU_TEST_PASSWORD=… sfu-canary \
//	  -endpoint wss://sfu.example.com:8443/ws -region eu-west || notify "SFU eu-west canary FAILED"
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

type signalMsg struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Data json.RawMessage `json:"data"`
}

func main() {
	var (
		endpoint = flag.String("endpoint", envOr("SFU_CANARY_ENDPOINT", "ws://127.0.0.1:8443/ws"), "SFU websocket endpoint")
		password = flag.String("test-password", os.Getenv("SFU_TEST_PASSWORD"), "SFU test-mode password (or SFU_TEST_PASSWORD env)")
		crewID   = flag.String("crew", "canary", "voice crew id to join")
		channel  = flag.String("channel", "canary", "voice channel id to join")
		region   = flag.String("region", "", "region label for log output only")
		timeout  = flag.Duration("timeout", 20*time.Second, "overall probe timeout")
	)
	flag.Parse()

	if *password == "" {
		fail(*region, "config", errors.New("no test password (set -test-password or SFU_TEST_PASSWORD)"))
	}

	start := time.Now()
	if err := probe(*endpoint, *password, *crewID, *channel, *timeout); err != nil {
		fail(*region, err.(*stageErr).stage, err.(*stageErr).err)
	}
	fmt.Printf("sfu_canary result=ok region=%q elapsed_ms=%d\n", *region, time.Since(start).Milliseconds())
}

// stageErr tags an error with the probe stage it happened in, so alerts can say
// where the join broke (dial / join / webrtc).
type stageErr struct {
	stage string
	err   error
}

func (e *stageErr) Error() string { return fmt.Sprintf("%s: %v", e.stage, e.err) }

func at(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &stageErr{stage: stage, err: err}
}

func fail(region, stage string, err error) {
	fmt.Fprintf(os.Stderr, "sfu_canary result=fail region=%q stage=%q error=%q\n", region, stage, err.Error())
	os.Exit(1)
}

func probe(endpoint, password, crewID, channel string, timeout time.Duration) error {
	wsURL, err := endpointWithTestParams(endpoint, password, crewID)
	if err != nil {
		return at("config", err)
	}

	overall := time.Now().Add(timeout)

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return at("dial", err)
	}
	defer conn.Close()
	reader := newWSReader(conn)
	sendMu := &sync.Mutex{}

	if msg, err := reader.recv(5 * time.Second); err != nil || msg.Type != "welcome" {
		return at("welcome", firstNonNil(err, fmt.Errorf("expected welcome, got %q", typeOf(msg))))
	}

	if err := send(conn, sendMu, map[string]any{
		"type": "join_voice", "seq": 1,
		"data": map[string]any{"crew_id": crewID, "channel_id": channel},
	}); err != nil {
		return at("join", err)
	}
	if err := awaitJoined(reader, 5*time.Second); err != nil {
		return at("join", err)
	}

	return at("webrtc", negotiate(conn, reader, sendMu, overall))
}

// negotiate creates a PeerConnection, sends an offer, applies the answer + ICE,
// and waits until the connection reaches Connected (DTLS handshake succeeded) or
// fails/times out. Reaching Connected is the proof the shared DTLS cert is valid.
func negotiate(conn *websocket.Conn, reader *wsReader, sendMu *sync.Mutex, overall time.Time) error {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return err
	}
	defer pc.Close()

	if _, err := pc.AddTransceiverFromKind(
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv},
	); err != nil {
		return err
	}

	var connected, failed sync.Once
	connCh := make(chan struct{})
	failCh := make(chan struct{})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			connected.Do(func() { close(connCh) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			failed.Do(func() { close(failCh) })
		}
	})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		mid, mline := "", 0
		if init.SDPMid != nil {
			mid = *init.SDPMid
		}
		if init.SDPMLineIndex != nil {
			mline = int(*init.SDPMLineIndex)
		}
		_ = send(conn, sendMu, map[string]any{
			"type": "ice_candidate", "seq": 0,
			"data": map[string]any{"candidate": init.Candidate, "sdp_mid": mid, "sdp_mline_index": mline},
		})
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	if err := send(conn, sendMu, map[string]any{
		"type": "offer", "seq": 0, "data": map[string]any{"sdp": offer.SDP},
	}); err != nil {
		return err
	}

	answerApplied := false
	for time.Now().Before(overall) {
		select {
		case <-connCh:
			return nil
		case <-failCh:
			return errors.New("peer connection failed (no media; DTLS/ICE did not establish)")
		default:
		}

		msg, err := reader.recv(250 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				continue
			}
			return fmt.Errorf("signaling read: %w", err)
		}
		switch msg.Type {
		case "answer":
			if answerApplied {
				continue
			}
			var d struct {
				SDP string `json:"sdp"`
			}
			if err := json.Unmarshal(msg.Data, &d); err != nil {
				return err
			}
			if d.SDP == "" {
				return errors.New("answer missing sdp")
			}
			if err := pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: d.SDP}); err != nil {
				return err
			}
			answerApplied = true
		case "ice_candidate":
			var d struct {
				Candidate     string `json:"candidate"`
				SDPMid        string `json:"sdp_mid"`
				SDPMLineIndex int    `json:"sdp_mline_index"`
			}
			if err := json.Unmarshal(msg.Data, &d); err != nil || d.Candidate == "" {
				continue
			}
			mid := d.SDPMid
			mline := uint16(d.SDPMLineIndex)
			_ = pc.AddICECandidate(webrtc.ICECandidateInit{
				Candidate:     strings.TrimPrefix(d.Candidate, "a="),
				SDPMid:        &mid,
				SDPMLineIndex: &mline,
			})
		case "error":
			return fmt.Errorf("server error during negotiation: %s", string(msg.Data))
		}
	}

	// Final chance in case the state flipped as the loop deadline passed.
	select {
	case <-connCh:
		return nil
	default:
		return errors.New("timed out before peer connection reached Connected")
	}
}

func awaitJoined(reader *wsReader, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg, err := reader.recv(500 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				continue
			}
			return err
		}
		switch msg.Type {
		case "joined":
			return nil
		case "error":
			return fmt.Errorf("join rejected: %s", string(msg.Data))
		}
	}
	return errors.New("timed out waiting for join ack")
}

// ── small helpers ──

var errRecvTimeout = errors.New("recv timeout")

type wsReader struct {
	conn *websocket.Conn
	msgs chan *signalMsg
	err  error
}

func newWSReader(conn *websocket.Conn) *wsReader {
	r := &wsReader{conn: conn, msgs: make(chan *signalMsg, 16)}
	go func() {
		for {
			var m signalMsg
			if err := conn.ReadJSON(&m); err != nil {
				r.err = err
				close(r.msgs)
				return
			}
			r.msgs <- &m
		}
	}()
	return r
}

func (r *wsReader) recv(timeout time.Duration) (*signalMsg, error) {
	select {
	case m, ok := <-r.msgs:
		if !ok {
			if r.err != nil {
				return nil, r.err
			}
			return nil, errors.New("connection closed")
		}
		return m, nil
	case <-time.After(timeout):
		return nil, errRecvTimeout
	}
}

func send(conn *websocket.Conn, mu *sync.Mutex, payload any) error {
	mu.Lock()
	defer mu.Unlock()
	return conn.WriteJSON(payload)
}

func endpointWithTestParams(endpoint, password, crewID string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("test_password", password)
	q.Set("test_type", "voice")
	q.Set("test_crew_id", crewID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func firstNonNil(err error, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}

func typeOf(m *signalMsg) string {
	if m == nil {
		return ""
	}
	return m.Type
}
