package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The SFU serves no web UI (01-SFU.md section 8.2): /admin answers 404
// with one line that points at the API. Every admin API endpoint still
// answers with JSON behind the admin auth, and /health, /stats and pprof
// stay where they were.
func TestAdminRoutesWithoutAWebUI(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	s.config.AdminPassword = "pw"
	s.config.StatsToken = "st"
	s.peers = NewPeerPoller(nil, "sfu-test", "test")
	s.peakSessions = newPeakSessionsTracker()
	s.bundles = newBundleManager(filepath.Join(t.TempDir(), "bundles"), time.Hour, s.Logger)
	mux := s.adminMux()
	start := time.Now().Add(-time.Hour).UnixMilli()
	s.media.store.Session(store.SessionRow{ID: "st-r", StartMs: start, EndMs: start + 60_000, LastSeenMs: start + 60_000, Type: "stream"})
	s.media.store.Flush()

	get := func(path string, auth bool) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if auth {
			req.SetBasicAuth("admin", "pw")
		}
		mux.ServeHTTP(rec, req)
		return rec
	}

	for _, path := range []string{"/admin", "/admin/", "/"} {
		for _, auth := range []bool{false, true} {
			rec := get(path, auth)
			if rec.Code != http.StatusNotFound || rec.Body.String() != noUIText {
				t.Errorf("GET %s (auth %v) = %d %q, want 404 with the one line", path, auth, rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "<html") {
				t.Errorf("GET %s still serves HTML", path)
			}
		}
	}

	// The API list of 01-SFU.md section 8.2, as the admin's Worker calls it.
	api := []string{
		"/admin/api/overview",
		"/admin/api/overview?range=24h",
		"/admin/api/sessions",
		"/admin/api/sessions?from=1",
		"/admin/api/session/st-r/timeline?from=1",
		"/admin/api/session/st-r/events",
		"/admin/api/logs",
		"/admin/api/logs?from=1",
		"/admin/api/peers",
		"/admin/api/troubleshoot?window_minutes=120",
		"/admin/api/alerts",
		"/admin/api/minutes",
		"/admin/api/minutes?days=7",
		"/admin/api/user/u-1/sessions?from=1",
		"/admin/api/user/u-1/profile",
		"/admin/api/user/u-1/bundles",
	}
	for _, path := range api {
		if rec := get(path, false); rec.Code != http.StatusUnauthorized {
			t.Errorf("GET %s without auth = %d, want 401", path, rec.Code)
		}
		rec := get(path, true)
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d %s, want 200", path, rec.Code, strings.TrimSpace(rec.Body.String()))
			continue
		}
		var body map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Errorf("GET %s is not a JSON object: %v", path, err)
		}
	}
	// A session that is not live is still a 404 from the API, not from the
	// retired page.
	if rec := get("/admin/api/session/none", true); rec.Code != http.StatusNotFound || rec.Body.String() == noUIText {
		t.Errorf("GET /admin/api/session/none = %d %q", rec.Code, rec.Body.String())
	}
	// POST request-logs answers too.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/admin/api/user/u-1/request-logs", nil)
	req.SetBasicAuth("admin", "pw")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "not_connected") {
		t.Errorf("POST request-logs = %d %s", rec.Code, rec.Body.String())
	}

	// /health needs no auth; this bare test server is "unhealthy" (503),
	// which /health answers with its JSON body.
	if rec := get("/health", false); (rec.Code != http.StatusOK && rec.Code != http.StatusServiceUnavailable) ||
		!strings.Contains(rec.Body.String(), `"status"`) {
		t.Errorf("GET /health = %d %s", rec.Code, rec.Body.String())
	}
	if rec := get("/stats", false); rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /stats without the token = %d, want 401", rec.Code)
	}
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer st")
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("GET /stats with the token = %d, want 200", rec.Code)
	}
	if rec := get("/debug/pprof/", false); rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /debug/pprof/ without auth = %d, want 401", rec.Code)
	}
	if rec := get("/debug/pprof/goroutine?debug=1", true); rec.Code != http.StatusOK {
		t.Errorf("GET /debug/pprof/goroutine = %d, want 200", rec.Code)
	}
}
