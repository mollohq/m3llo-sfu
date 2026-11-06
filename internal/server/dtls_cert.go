package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

// DTLS certificate lifecycle.
//
// The SFU shares a single self-signed DTLS-SRTP certificate across every
// PeerConnection to avoid per-connection ECDSA key generation. pion's
// webrtc.GenerateCertificate hardcodes a ~30-day lifetime and never rotates, so
// a process running longer than that starts rejecting every new PeerConnection
// with "InvalidAccessError: x509Cert expired" (SEV-1 outage 2026-09-09).
//
// We therefore issue our own certificate with an explicit validity window and
// rotate it well inside that window (see runDTLSCertRotation). WebRTC peers are
// authenticated by the certificate fingerprint carried in the JWT-authenticated
// SDP, not by a CA chain, so the certificate lifetime is a liveness concern, not
// a security one.
const (
	// dtlsCertValidity is how long each generated certificate is valid.
	dtlsCertValidity = 90 * 24 * time.Hour
	// dtlsCertRotateEvery is how often the shared cert is regenerated. It must
	// stay comfortably below dtlsCertValidity so rotation always runs ahead of
	// expiry, even if a rotation cycle is missed.
	dtlsCertRotateEvery = 14 * 24 * time.Hour
	// dtlsCertMinRemaining is the safety window. If the active cert has less than
	// this left, the server reports itself degraded (see handleHealth) and a
	// fresh cert should already have been rotated in.
	dtlsCertMinRemaining = 7 * 24 * time.Hour
)

// generateDTLSCert creates a self-signed ECDSA P-256 certificate for DTLS-SRTP
// with a validity window we control (unlike webrtc.GenerateCertificate).
func generateDTLSCert() (*webrtc.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate DTLS key: %w", err)
	}
	// 130-bit random serial, matching pion's own GenerateCertificate.
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 130))
	if err != nil {
		return nil, fmt.Errorf("generate DTLS serial: %w", err)
	}
	now := time.Now()
	cert, err := webrtc.NewCertificate(key, x509.Certificate{
		SerialNumber: serial,
		Version:      2,
		Subject:      pkix.Name{CommonName: "m3llo-sfu"},
		Issuer:       pkix.Name{CommonName: "m3llo-sfu"},
		NotBefore:    now.AddDate(0, 0, -1),
		NotAfter:     now.Add(dtlsCertValidity),
	})
	if err != nil {
		return nil, fmt.Errorf("generate DTLS cert: %w", err)
	}
	return cert, nil
}

// currentDTLSCert returns the active shared certificate, or nil if none is set.
func (s *Server) currentDTLSCert() *webrtc.Certificate {
	return s.dtlsCert.Load()
}

// dtlsCertExpiresIn returns the remaining validity of the active certificate.
// The bool is false when no certificate is set.
func (s *Server) dtlsCertExpiresIn() (time.Duration, bool) {
	c := s.dtlsCert.Load()
	if c == nil {
		return 0, false
	}
	return time.Until(c.Expires()), true
}

// runDTLSCertRotation regenerates the shared DTLS certificate on a timer, well
// before it expires. New PeerConnections pick up the current cert atomically;
// in-flight connections keep the cert they already negotiated with.
func (s *Server) runDTLSCertRotation(stop <-chan struct{}) {
	ticker := time.NewTicker(dtlsCertRotateEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cert, err := generateDTLSCert()
			if err != nil {
				// Keep serving the existing cert; it is still valid for weeks.
				// The expiry early-warning (via /health) is the backstop.
				s.Logger.Error("dtls_cert_rotate_failed", logging.D{"error": err.Error()})
				continue
			}
			s.dtlsCert.Store(cert)
			s.Logger.Info("dtls_cert_rotated", logging.D{
				"expires_at": cert.Expires().UTC().Format(time.RFC3339),
			})
		case <-stop:
			return
		}
	}
}
