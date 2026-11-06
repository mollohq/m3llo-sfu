package server

import (
	"crypto/subtle"
	"encoding/json"
	"math"
	"net/http"
	"strings"
)

// StatsResponse is the compact /stats summary for a status dashboard
// (01-SFU.md section 8.4).
type StatsResponse struct {
	SessionsActive    int     `json:"sessions_active"`    // active voice sessions
	ParticipantsTotal int     `json:"participants_total"` // connected peers across all sessions
	StreamsActive     int     `json:"streams_active"`     // active stream sessions
	PeakSessionsToday int     `json:"peak_sessions_today"`
	CPUPct            float64 `json:"cpu_pct"`
	MemPct            float64 `json:"mem_pct"`
	UptimeS           int64   `json:"uptime_s"`
	Version           string  `json:"version"`
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	// Mission control shows real-user activity: exclude synthetic (test-mode)
	// sessions and peers. /health and the admin dashboard still count them.
	streamCount, voiceCount := s.sessions.CountReal()
	participants := s.metrics.ActivePeers.Load() - s.metrics.ActiveSyntheticPeers.Load()
	_, _, cpuPercent := s.metrics.Rates()
	if participants < 0 {
		participants = 0
	}

	resp := StatsResponse{
		SessionsActive:    voiceCount,
		ParticipantsTotal: int(participants),
		StreamsActive:     streamCount,
		PeakSessionsToday: s.peakSessions.Peak(),
		CPUPct:            round1(cpuPercent),
		MemPct:            round1(s.metrics.HostMemoryPercent()),
		UptimeS:           s.metrics.UptimeSeconds(),
		Version:           Version,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// statsAuth guards /stats with a bearer token from SFU_STATS_TOKEN. It is
// independent of the admin Basic-auth wrapper so a dashboard can poll stats
// without holding the admin password. When no token is configured the endpoint
// is disabled rather than open.
func (s *Server) statsAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := s.config.StatsToken
		if token == "" {
			http.Error(w, "stats endpoint disabled: SFU_STATS_TOKEN not set", http.StatusServiceUnavailable)
			return
		}
		const prefix = "Bearer "
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, prefix) ||
			subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, prefix)), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="m3llo-sfu-stats"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}
