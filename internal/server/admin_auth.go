package server

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

type AdminAuth struct {
	password   string
	allowedIPs []string // raw strings, can be IPs or CIDRs
	// devOpen leaves the admin routes open when no password is set
	// (SFU_DEV_MODE). Without it, no password turns the admin routes off.
	devOpen bool
	ips     clientIPResolver
	fails   *failLimiter
}

// adminOffText is the answer of every admin route when no admin password is
// set outside dev mode.
const adminOffText = "The admin API is off: set SFU_ADMIN_PASSWORD to turn it on.\n"

// NewAdminAuth wraps the admin routes. ips finds the client address for the
// allowlist and the failure limit; fails may be nil (no limit).
func NewAdminAuth(password string, allowedIPs []string, devOpen bool, ips clientIPResolver, fails *failLimiter) *AdminAuth {
	return &AdminAuth{
		password:   password,
		allowedIPs: allowedIPs,
		devOpen:    devOpen,
		ips:        ips,
		fails:      fails,
	}
}

func (a *AdminAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No password: open in dev mode, else off. The admin API serves
		// user data (sessions, logs, client log bundles), so it fails closed.
		if a.password == "" {
			if a.devOpen {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, strings.TrimSuffix(adminOffText, "\n"), http.StatusServiceUnavailable)
			return
		}

		clientIP := a.ips.Of(r)

		// 1. IP allowlist check (if configured)
		if len(a.allowedIPs) > 0 && !ipInList(clientIP, a.allowedIPs) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		// 2. Too many failed passwords from this address
		if a.fails.Blocked(clientIP) {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}

		// 3. Basic Auth password check
		_, password, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(password), []byte(a.password)) != 1 {
			if ok {
				// A wrong password is a guess; no credentials is not.
				a.fails.Fail(clientIP)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="m3llo-sfu"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		a.fails.Succeed(clientIP)

		next.ServeHTTP(w, r)
	})
}
