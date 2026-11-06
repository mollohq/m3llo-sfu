package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

// TestGenerateDTLSCertValidity guards against the 2026-09-09 outage: the shared
// DTLS cert must be valid well beyond the safety window, not the ~30 days pion's
// GenerateCertificate hands out.
func TestGenerateDTLSCertValidity(t *testing.T) {
	cert, err := generateDTLSCert()
	if err != nil {
		t.Fatalf("generateDTLSCert: %v", err)
	}
	remaining := time.Until(cert.Expires())
	if remaining < dtlsCertValidity-24*time.Hour || remaining > dtlsCertValidity+time.Minute {
		t.Fatalf("cert validity = %s, want ~%s", remaining, dtlsCertValidity)
	}
	if remaining <= dtlsCertMinRemaining {
		t.Fatalf("fresh cert already inside the degraded window (%s <= %s)", remaining, dtlsCertMinRemaining)
	}
}

// TestGeneratedCertAcceptedByPeerConnection is the direct regression: a fresh
// cert must be accepted by NewPeerConnection (an expired one is rejected, see
// TestExpiredCertRejectedByPeerConnection).
func TestGeneratedCertAcceptedByPeerConnection(t *testing.T) {
	cert, err := generateDTLSCert()
	if err != nil {
		t.Fatalf("generateDTLSCert: %v", err)
	}
	api := newTestAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{
		Certificates: []webrtc.Certificate{*cert},
	})
	if err != nil {
		t.Fatalf("NewPeerConnection with fresh cert: %v", err)
	}
	pc.Close()
}

// TestExpiredCertRejectedByPeerConnection pins the exact failure mode from the
// outage, so the regression above cannot silently stop testing anything.
func TestExpiredCertRejectedByPeerConnection(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 130))
	expired, err := webrtc.NewCertificate(key, x509.Certificate{
		SerialNumber: serial,
		Version:      2,
		Subject:      pkix.Name{CommonName: "m3llo-sfu"},
		Issuer:       pkix.Name{CommonName: "m3llo-sfu"},
		NotBefore:    time.Now().AddDate(0, 0, -2),
		NotAfter:     time.Now().AddDate(0, 0, -1), // expired yesterday
	})
	if err != nil {
		t.Fatalf("build expired cert: %v", err)
	}
	api := newTestAPI()
	_, err = api.NewPeerConnection(webrtc.Configuration{
		Certificates: []webrtc.Certificate{*expired},
	})
	if err == nil {
		t.Fatal("expected NewPeerConnection to reject an expired cert, got nil")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected an expiry error, got: %v", err)
	}
}
