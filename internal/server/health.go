package server

import (
	"encoding/json"
	"net/http"
	"runtime"
)

type HealthResponse struct {
	Status    string        `json:"status"` // "ok", "degraded", or "unhealthy"
	ServerID  string        `json:"server_id"`
	Region    string        `json:"region"`
	Version   string        `json:"version"`
	Uptime    int64         `json:"uptime_seconds"`
	Sessions  SessionCount  `json:"sessions"`
	Peers     PeerCount     `json:"peers"`
	BW        BWStats       `json:"bandwidth_mbps"`
	Snapshots SnapshotStats `json:"snapshots"`
	System    SystemStats   `json:"system"`
	WebRTC    WebRTCStats   `json:"webrtc"`
}

type WebRTCStats struct {
	// DTLSCertExpiresIn is the remaining validity of the shared DTLS cert, in
	// seconds. Negative means the cert has expired (every new PeerConnection
	// will fail). Alert on this well before it reaches zero.
	DTLSCertExpiresIn int64 `json:"dtls_cert_expires_in_seconds"`
	Connections       int64 `json:"connections_total"`
	Failures          int64 `json:"failures_total"`
}

type SessionCount struct {
	StreamActive   int `json:"stream_active"`
	StreamDraining int `json:"stream_draining"`
	VoiceActive    int `json:"voice_active"`
	VoiceDraining  int `json:"voice_draining"`
}

type PeerCount struct {
	Total   int `json:"total"`
	Hosts   int `json:"hosts"`
	Viewers int `json:"viewers"`
	Voice   int `json:"voice_members"`
}

type BWStats struct {
	Inbound  float64 `json:"inbound"`
	Outbound float64 `json:"outbound"`
}

type SnapshotStats struct {
	TotalUploaded int64 `json:"total_uploaded"`
}

type SystemStats struct {
	CPUPercent float64 `json:"cpu_percent"`
	MemoryMB   float64 `json:"memory_mb"`
	Goroutines int     `json:"goroutines"`
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	streamCount, voiceCount := s.sessions.Count()
	bwIn, bwOut, _ := s.metrics.Rates()

	// Health reflects whether the SFU can actually establish media, not just
	// whether the process is alive. The shared DTLS cert is the single dependency
	// that silently breaks every PeerConnection when it expires.
	status := "ok"
	certExpiresIn := int64(0)
	if remaining, ok := s.dtlsCertExpiresIn(); ok {
		certExpiresIn = int64(remaining.Seconds())
		switch {
		case remaining <= 0:
			// Cert expired: every new PeerConnection fails. Fail the check so
			// load balancers / uptime probes take this instance out of rotation.
			status = "unhealthy"
		case remaining < dtlsCertMinRemaining:
			// Near expiry but still serving. Keep serving (HTTP 200); the
			// expires_in field drives the early-warning alert.
			status = "degraded"
		}
	} else {
		// No cert at all: cannot serve DTLS.
		status = "unhealthy"
	}

	resp := HealthResponse{
		Status:   status,
		ServerID: s.config.ServerID,
		Region:   s.config.Region,
		Version:  Version,
		Uptime:   s.metrics.UptimeSeconds(),
		Sessions: SessionCount{
			StreamActive: streamCount,
			VoiceActive:  voiceCount,
		},
		Peers: PeerCount{
			Total: int(s.metrics.ActivePeers.Load()),
		},
		BW: BWStats{
			Inbound:  bwIn,
			Outbound: bwOut,
		},
		Snapshots: SnapshotStats{
			TotalUploaded: s.metrics.SnapshotsUploaded.Load(),
		},
		System: SystemStats{
			MemoryMB:   float64(memStats.Alloc) / 1_000_000,
			Goroutines: runtime.NumGoroutine(),
		},
		WebRTC: WebRTCStats{
			DTLSCertExpiresIn: certExpiresIn,
			Connections:       s.metrics.WebRTCConnections.Load(),
			Failures:          s.metrics.WebRTCFailures.Load(),
		},
	}

	w.Header().Set("Content-Type", "application/json")
	// "unhealthy" means the instance cannot establish media; fail the check so
	// probes and the peer mesh take it out of rotation. "degraded" still serves.
	if status == "unhealthy" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	json.NewEncoder(w).Encode(resp)
}
