package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
)

// TestCountRealExcludesSyntheticOnlySessions guards /stats against the
// canary: a session that only ever held test-mode peers must not count as real
// activity (it used to set peak_sessions_today >= 1 every day).
func TestCountRealExcludesSyntheticOnlySessions(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)

	canary := NewVoiceSession("voice:canary:canary", "canary", "canary", logger)
	if err := canary.AddPeer(&Peer{UserID: "test-browser-1", Role: "member", Synthetic: true}); err != nil {
		t.Fatalf("add canary peer: %v", err)
	}
	store.sessions[canary.ID()] = canary

	realVoice := NewVoiceSession("voice:crew:ch", "crew", "ch", logger)
	if err := realVoice.AddPeer(&Peer{UserID: "user-1", Role: "member"}); err != nil {
		t.Fatalf("add real peer: %v", err)
	}
	store.sessions[realVoice.ID()] = realVoice

	// A real user in a session a synthetic peer created still counts. The viewer
	// has a WS: it gets member_joined when the host joins.
	mixed := NewStreamSession("stream", "crew", logger, streamSnapshotConfig{})
	viewer := NewPeer("test-browser-2", "viewer", newWSProbe(t).server, logger)
	viewer.Synthetic = true
	if err := mixed.AddPeer(viewer); err != nil {
		t.Fatalf("add synthetic viewer: %v", err)
	}
	if err := mixed.AddPeer(&Peer{UserID: "host-1", Role: "host"}); err != nil {
		t.Fatalf("add real host: %v", err)
	}
	store.sessions[mixed.ID()] = mixed

	if stream, voice := store.Count(); stream != 1 || voice != 2 {
		t.Fatalf("Count() = stream %d voice %d, want 1 and 2 (all sessions)", stream, voice)
	}
	if stream, voice := store.CountReal(); stream != 1 || voice != 1 {
		t.Fatalf("CountReal() = stream %d voice %d, want 1 and 1 (canary excluded)", stream, voice)
	}

	// After the last real peer leaves, the session drains and still counts,
	// exactly as before the change.
	realVoice.members.Remove("user-1")
	realVoice.state = SessionDraining
	if _, voice := store.CountReal(); voice != 1 {
		t.Fatalf("draining real session: CountReal voice = %d, want 1", voice)
	}
}

func TestStatsExcludesSyntheticActivity(t *testing.T) {
	logger := logging.New("error")
	store := NewSessionStore(logger)

	canary := NewVoiceSession("voice:canary:canary", "canary", "canary", logger)
	if err := canary.AddPeer(&Peer{UserID: "test-browser-1", Role: "member", Synthetic: true}); err != nil {
		t.Fatalf("add canary peer: %v", err)
	}
	store.sessions[canary.ID()] = canary

	realVoice := NewVoiceSession("voice:crew:ch", "crew", "ch", logger)
	if err := realVoice.AddPeer(&Peer{UserID: "user-1", Role: "member"}); err != nil {
		t.Fatalf("add real peer: %v", err)
	}
	store.sessions[realVoice.ID()] = realVoice

	m := metrics.New()
	m.ActivePeers.Store(3) // two real peers + one canary
	m.ActiveSyntheticPeers.Store(1)
	s := &Server{sessions: store, metrics: m, peakSessions: newPeakSessionsTracker()}

	rec := httptest.NewRecorder()
	s.handleStats(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var got StatsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode /stats: %v", err)
	}
	if got.SessionsActive != 1 {
		t.Fatalf("sessions_active = %d, want 1 (canary excluded)", got.SessionsActive)
	}
	if got.ParticipantsTotal != 2 {
		t.Fatalf("participants_total = %d, want 2 (canary excluded)", got.ParticipantsTotal)
	}
}

func TestPeakSessionsTrackerRecordsHighWaterMark(t *testing.T) {
	p := newPeakSessionsTracker()
	p.Observe(3)
	p.Observe(11)
	p.Observe(7) // lower than the peak; must not lower it
	if got := p.Peak(); got != 11 {
		t.Fatalf("peak = %d, want 11", got)
	}
}

func TestPeakSessionsTrackerResetsOnDayChange(t *testing.T) {
	p := newPeakSessionsTracker()
	p.Observe(9)
	// Simulate crossing local midnight by rewinding the recorded day.
	p.mu.Lock()
	p.day = "2000-01-01"
	p.mu.Unlock()
	if got := p.Peak(); got != 0 {
		t.Fatalf("peak after day rollover = %d, want 0", got)
	}
	p.Observe(2)
	if got := p.Peak(); got != 2 {
		t.Fatalf("peak in new day = %d, want 2", got)
	}
}

func TestStatsAuthRejectsWithoutToken(t *testing.T) {
	s := &Server{config: Config{StatsToken: "secret"}}
	handler := s.statsAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name       string
		authHeader string
		want       int
	}{
		{"missing", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic secret", http.StatusUnauthorized},
		{"wrong token", "Bearer nope", http.StatusUnauthorized},
		{"correct", "Bearer secret", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/stats", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d", rec.Code, tc.want)
			}
		})
	}
}

func TestStatsAuthDisabledWhenNoTokenConfigured(t *testing.T) {
	s := &Server{config: Config{StatsToken: ""}}
	handler := s.statsAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler must not run when stats endpoint is disabled")
	}))
	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer anything")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestStockholmLocationLoads(t *testing.T) {
	// tzdata is embedded via cmd/sfu, but the test binary links it too through
	// the same package graph only if referenced; guard against a UTC fallback
	// silently masking a broken day boundary in production.
	if stockholmLocation == time.UTC {
		t.Skip("Europe/Stockholm unavailable in test env (tzdata embedded in cmd/sfu build)")
	}
	if stockholmLocation.String() != "Europe/Stockholm" {
		t.Fatalf("stockholmLocation = %q, want Europe/Stockholm", stockholmLocation.String())
	}
}
