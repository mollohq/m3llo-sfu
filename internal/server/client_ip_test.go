package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
)

// X-Forwarded-For names the client only behind a trusted proxy: a direct
// client that sends the header cannot pass SFU_ADMIN_IPS with it.
func TestClientIPTrustsForwardedHeadersOnlyFromTrustedProxies(t *testing.T) {
	cases := []struct {
		name    string
		trusted []string
		remote  string
		xff     string
		xri     string
		want    string
	}{
		{"no proxies: header ignored", nil, "203.0.113.9:4000", "10.0.0.1", "", "203.0.113.9"},
		{"untrusted peer: header ignored", []string{"192.0.2.0/24"}, "203.0.113.9:4000", "10.0.0.1", "", "203.0.113.9"},
		{"trusted peer: last untrusted hop", []string{"192.0.2.0/24"}, "192.0.2.5:4000", "10.0.0.1, 198.51.100.7", "", "198.51.100.7"},
		{"spoofed left entry skipped", []string{"192.0.2.0/24"}, "192.0.2.5:4000", "10.0.0.1, 198.51.100.7, 192.0.2.6", "", "198.51.100.7"},
		{"trusted peer, X-Real-IP", []string{"192.0.2.5"}, "192.0.2.5:4000", "", "198.51.100.8", "198.51.100.8"},
		{"trusted peer, no headers", []string{"192.0.2.5"}, "192.0.2.5:4000", "", "", "192.0.2.5"},
		{"trusted peer, garbage hop", []string{"192.0.2.5"}, "192.0.2.5:4000", "not-an-ip", "", "192.0.2.5"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/admin/api/overview", nil)
			r.RemoteAddr = c.remote
			if c.xff != "" {
				r.Header.Set("X-Forwarded-For", c.xff)
			}
			if c.xri != "" {
				r.Header.Set("X-Real-IP", c.xri)
			}
			if got := newClientIPResolver(c.trusted).Of(r); got != c.want {
				t.Errorf("Of = %q, want %q", got, c.want)
			}
		})
	}
}

func TestAdminAllowlistIgnoresSpoofedForwardedFor(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := NewAdminAuth("pw", []string{"198.51.100.7"}, false, newClientIPResolver(nil), nil).Wrap(ok)
	r := httptest.NewRequest(http.MethodGet, "/admin/api/overview", nil)
	r.RemoteAddr = "203.0.113.9:4000"
	r.Header.Set("X-Forwarded-For", "198.51.100.7")
	r.SetBasicAuth("admin", "pw")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403", rec.Code)
	}
}

// After authFailMax wrong passwords in the window an address gets 429, the
// right password included, until the window passes. Requests without
// credentials do not count.
func TestFailedPasswordsAreLimitedPerAddress(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	fails := newFailLimiter(3, time.Minute)
	fails.now = func() time.Time { return now }
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	h := NewAdminAuth("pw", nil, false, newClientIPResolver(nil), fails).Wrap(ok)
	call := func(remote, pw string) int {
		r := httptest.NewRequest(http.MethodGet, "/admin/api/overview", nil)
		r.RemoteAddr = remote
		if pw != "" {
			r.SetBasicAuth("admin", pw)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec.Code
	}
	for i := 0; i < 5; i++ {
		if got := call("203.0.113.9:1", ""); got != http.StatusUnauthorized {
			t.Fatalf("no credentials #%d: %d, want 401", i, got)
		}
	}
	for i := 0; i < 3; i++ {
		if got := call("203.0.113.9:1", "nope"); got != http.StatusUnauthorized {
			t.Fatalf("wrong password #%d: %d, want 401", i, got)
		}
	}
	if got := call("203.0.113.9:1", "pw"); got != http.StatusTooManyRequests {
		t.Fatalf("after the limit: %d, want 429", got)
	}
	if got := call("198.51.100.1:1", "pw"); got != http.StatusOK {
		t.Fatalf("another address: %d, want 200", got)
	}
	now = now.Add(61 * time.Second)
	if got := call("203.0.113.9:1", "pw"); got != http.StatusOK {
		t.Fatalf("after the window: %d, want 200", got)
	}
}

// The test-mode join counts failures too.
func TestTestModeFailuresAreLimited(t *testing.T) {
	s := &Server{config: Config{TestPassword: "tp"}, authFails: newFailLimiter(2, time.Minute)}
	s.Logger = logging.New("error")
	call := func(pw string) int {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/ws?test_password="+pw+"&test_type=voice", nil)
		r.RemoteAddr = "203.0.113.9:1"
		s.handleWebSocket(rec, r)
		return rec.Code
	}
	call("a")
	call("b")
	if got := call("tp"); got != http.StatusTooManyRequests {
		t.Fatalf("after the limit: %d, want 429", got)
	}
}
