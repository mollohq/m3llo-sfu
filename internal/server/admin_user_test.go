package server

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The person's profile reads the machine from the latest reports and the
// link from the last session; their sessions carry their own grade.
func TestUserProfileAndSessions(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	st := s.media.store
	start := time.Now().Add(-time.Hour).UnixMilli()
	st.Session(store.SessionRow{ID: "st-k", StartMs: start, EndMs: start + 120_000, LastSeenMs: start + 120_000,
		Type: "stream", HostUserID: "h", Quality: "degraded", Trouble: LegViewer})
	st.Peer(store.PeerRow{ID: "st-k", StartMs: start, User: "kattis", Role: "viewer", FirstMs: start, LastMs: start + 120_000})
	st.PeerMinute(store.PeerMinuteRow{ID: "st-k", StartMs: start, MinuteMs: start, User: "kattis", Role: "viewer",
		Type: "stream", Graded: true, Degraded: true, Reasons: "loss,freeze", Leg: LegViewer})
	st.PeerMinute(store.PeerMinuteRow{ID: "st-k", StartMs: start, MinuteMs: start + 60_000, User: "kattis", Role: "viewer",
		Type: "stream", Graded: true})
	st.Stats(store.StatsRow{Ms: start + 10_000, Session: "st-k", User: "kattis", Role: "viewer", Kind: "stream",
		Body: []byte(`{"dec":"AMF","jit_ms":6.5,"rtt_ms":41}`)})
	st.Stats(store.StatsRow{Ms: start - 86_400_000, Session: "st-old", User: "kattis", Role: "host", Kind: "stream",
		Body: []byte(`{"gpu":"Radeon RX 7800 XT","enc":"AMF","cap_backend":"hook"}`)})
	st.Relay(store.RelayRow{Ms: start + 10_000, Session: "st-k",
		Body: []byte(`{"viewer_sent":{"kattis":1000},"viewer_nacked":{"kattis":41}}`)})
	st.Flush()

	rec := httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("GET", "/admin/api/user/kattis/profile", nil))
	var p struct {
		Machine map[string]interface{} `json:"machine"`
		Link    []linkPoint            `json:"link"`
		Last    store.SessionRow       `json:"last_session"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&p); err != nil {
		t.Fatalf("profile: %v (%d)", err, rec.Code)
	}
	if p.Machine["gpu"] != "Radeon RX 7800 XT" || p.Machine["decoder"] != "AMF" || p.Machine["capture_backend"] != "hook" {
		t.Fatalf("machine = %+v", p.Machine)
	}
	if p.Last.ID != "st-k" || len(p.Link) != 2 {
		t.Fatalf("last session %s, link %+v", p.Last.ID, p.Link)
	}
	if p.Link[0].LossPct == nil || *p.Link[0].LossPct != 4.1 {
		t.Fatalf("link loss = %+v", p.Link)
	}

	rec = httptest.NewRecorder()
	s.handleAdminUser(rec, httptest.NewRequest("GET", "/admin/api/user/kattis/sessions?from=1", nil))
	var us struct {
		Sessions []struct {
			ID   string              `json:"id"`
			User *store.UserRunGrade `json:"user_grade"`
		} `json:"sessions"`
	}
	_ = json.NewDecoder(rec.Body).Decode(&us)
	if len(us.Sessions) != 1 || us.Sessions[0].User == nil {
		t.Fatalf("sessions = %+v", us)
	}
	g := us.Sessions[0].User
	if g.Graded != 2 || g.Degraded != 1 || g.Legs[LegViewer] != 1 || g.Reasons["freeze"] != 1 || g.Roles[0] != "viewer" {
		t.Fatalf("own grade = %+v", g)
	}
}
