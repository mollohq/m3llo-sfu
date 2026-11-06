package server

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

func newAdminTestServer(t *testing.T) (*Server, *StreamSession) {
	t.Helper()
	logger := logging.New("error")
	store := NewSessionStore(logger)
	session := NewStreamSession("stream-admin", "crew-1", logger, streamSnapshotConfig{})
	store.sessions[session.ID()] = session

	host := &Peer{UserID: "u-host", Username: "hostname", Role: "host"}
	viewer := &Peer{UserID: "u-viewer", Username: "viewername", Role: "viewer", Synthetic: true}
	if err := session.AddPeer(host); err != nil {
		t.Fatalf("add host: %v", err)
	}
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	host.BytesReceived.Add(1000)
	host.BytesSent.Add(10)
	viewer.BytesSent.Add(900)
	viewer.BytesReceived.Add(20)

	return &Server{
		config:   Config{MaxPeersPerSession: 10},
		Logger:   logger,
		sessions: store,
	}, session
}

// The media admin's Sessions page reads host, peers and bytes from the
// session list without a detail call per session.
func TestAdminSessionsCarriesHostPeersAndBytes(t *testing.T) {
	s, _ := newAdminTestServer(t)

	rec := httptest.NewRecorder()
	s.handleAdminSessions(rec, httptest.NewRequest("GET", "/admin/api/sessions", nil))

	var body struct {
		Sessions []map[string]interface{} `json:"sessions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Sessions) != 1 {
		t.Fatalf("sessions = %d, want 1", len(body.Sessions))
	}
	row := body.Sessions[0]
	want := map[string]interface{}{
		"host_user_id":    "u-host",
		"host_username":   "hostname",
		"members":         float64(2),
		"peers_connected": float64(0), // no PeerConnection in the test
		"synthetic":       false,      // the host is a real peer
		"bytes_sent":      float64(910),
		"bytes_recv":      float64(1020),
	}
	for k, v := range want {
		if row[k] != v {
			t.Errorf("%s = %v, want %v", k, row[k], v)
		}
	}
}

// The session page's relay counters show the host-leg ingress loss and the
// NACK repairs (01-SFU.md section 8.3).
func TestAdminSessionDetailCarriesIngressLoss(t *testing.T) {
	s, session := newAdminTestServer(t)
	session.rtpIngressExpected.Store(2000)
	session.rtpIngressLost.Store(25)

	rec := httptest.NewRecorder()
	s.handleAdminSessionDetail(rec, httptest.NewRequest("GET", "/admin/api/session/"+session.ID(), nil))

	var body struct {
		StreamRelay map[string]interface{} `json:"stream_relay"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	relay := body.StreamRelay
	if relay == nil {
		t.Fatal("no stream_relay in the stream session detail")
	}
	for k, v := range map[string]float64{
		"rtp_ingress_expected": 2000,
		"rtp_ingress_lost":     25,
		"rtp_ingress_loss_pct": 1.25,
	} {
		if relay[k] != v {
			t.Errorf("%s = %v, want %v", k, relay[k], v)
		}
	}
	for _, k := range []string{"rtp_rtx_recovered", "rtp_viewer_nacked_packets", "rtp_slow_viewer_dropped"} {
		if _, ok := relay[k]; !ok {
			t.Errorf("stream_relay has no %s", k)
		}
	}
}
