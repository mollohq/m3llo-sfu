package server

import (
	"net"
	"net/http"
	"strings"
)

// clientIPResolver finds the address of the client of a request. It reads
// X-Forwarded-For and X-Real-IP only when the direct peer is a trusted proxy
// (SFU_TRUSTED_PROXIES). Anyone can send those headers, so without that rule
// a client could name an allowlisted address and pass SFU_ADMIN_IPS.
type clientIPResolver struct {
	trusted []string // IPs or CIDRs
}

func newClientIPResolver(trusted []string) clientIPResolver {
	return clientIPResolver{trusted: trusted}
}

// Of returns the client address of r.
func (c clientIPResolver) Of(r *http.Request) string {
	peer := remoteHost(r)
	if len(c.trusted) == 0 || !ipInList(peer, c.trusted) {
		return peer
	}
	// The peer is a trusted proxy. X-Forwarded-For lists the hops from the
	// client to the proxy, each proxy appending the one it heard from. Walk
	// from the right and stop at the first hop that is not a trusted proxy:
	// entries left of it come from the client and can be anything.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		hops := strings.Split(xff, ",")
		for i := len(hops) - 1; i >= 0; i-- {
			hop := strings.TrimSpace(hops[i])
			if net.ParseIP(hop) == nil {
				break
			}
			if !ipInList(hop, c.trusted) {
				return hop
			}
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-IP")); net.ParseIP(xri) != nil {
		return xri
	}
	return peer
}

func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ipInList reports whether ip matches one of the IPs or CIDRs in list.
func ipInList(ip string, list []string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, entry := range list {
		if strings.Contains(entry, "/") {
			if _, network, err := net.ParseCIDR(entry); err == nil && network.Contains(parsed) {
				return true
			}
			continue
		}
		if other := net.ParseIP(entry); other != nil && other.Equal(parsed) {
			return true
		}
	}
	return false
}
