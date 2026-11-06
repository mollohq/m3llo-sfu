package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

// Test mode takes its own secret, SFU_TEST_PASSWORD, and is off without it
// (01-SFU.md section 5.1). The admin password does not open it.
func TestTestModeUsesItsOwnPassword(t *testing.T) {
	cases := []struct {
		name     string
		testPwd  string
		adminPwd string
		given    string
		wantAuth bool
	}{
		{"off without a test password", "", "ap", "ap", false},
		{"off without a test password, empty given", "", "", "x", false},
		{"admin password does not open it", "tp", "ap", "ap", false},
		{"wrong password", "tp", "ap", "nope", false},
		{"test password opens it", "tp", "ap", "tp", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &Server{
				config: Config{TestPassword: c.testPwd, AdminPassword: c.adminPwd},
				Logger: logging.New("error"),
			}
			rec := httptest.NewRecorder()
			// A plain GET: past the password check the WebSocket upgrade fails
			// with 400, which is enough to tell the check passed.
			req := httptest.NewRequest(http.MethodGet, "/ws?test_password="+c.given+"&test_type=voice", nil)
			s.handleWebSocket(rec, req)
			gotAuth := rec.Code != http.StatusUnauthorized
			if gotAuth != c.wantAuth {
				t.Errorf("status %d, passed auth %v, want %v", rec.Code, gotAuth, c.wantAuth)
			}
		})
	}
}

// The admin routes fail closed: no password outside dev mode turns them off
// (503); in dev mode they stay open; with a password they ask for it.
func TestAdminAuthFailsClosedWithoutAPassword(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	cases := []struct {
		name     string
		password string
		devOpen  bool
		basic    string
		want     int
	}{
		{"no password, not dev: off", "", false, "", http.StatusServiceUnavailable},
		{"no password, not dev, any basic auth: off", "", false, "x", http.StatusServiceUnavailable},
		{"no password, dev: open", "", true, "", http.StatusOK},
		{"password, none given", "pw", false, "", http.StatusUnauthorized},
		{"password, wrong", "pw", true, "nope", http.StatusUnauthorized},
		{"password, right", "pw", false, "pw", http.StatusOK},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := NewAdminAuth(c.password, nil, c.devOpen, clientIPResolver{}, nil).Wrap(ok)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/admin/api/overview", nil)
			if c.basic != "" {
				req.SetBasicAuth("admin", c.basic)
			}
			h.ServeHTTP(rec, req)
			if rec.Code != c.want {
				t.Errorf("status %d, want %d", rec.Code, c.want)
			}
		})
	}
}

// New refuses a config that can authenticate no client, and starts with a
// key, a test password or dev mode.
func TestNewVerifierFailsClosed(t *testing.T) {
	log := logging.New("error")
	if _, err := newVerifier(Config{}, log); err == nil {
		t.Fatal("no key, no test password, no dev mode: want an error")
	}
	v, err := newVerifier(Config{TestPassword: "tp"}, log)
	if err != nil {
		t.Fatalf("test password only: %v", err)
	}
	if _, err := v.Verify("dev"); err == nil {
		t.Error("test password only: a token was accepted without a key")
	}
	v, err = newVerifier(Config{DevMode: true}, log)
	if err != nil {
		t.Fatalf("dev mode: %v", err)
	}
	if _, err := v.Verify("dev"); err != nil {
		t.Errorf("dev mode: token refused: %v", err)
	}
}
