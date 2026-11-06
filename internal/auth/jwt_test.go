package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyDevModeParsesNakamaJWTUnverified(t *testing.T) {
	t.Parallel()

	token := signTestToken(t, map[string]interface{}{
		"uid":  "user-1",
		"sid":  "stream_abc_123",
		"type": "stream",
		"role": "host",
		"exp":  time.Now().Add(5 * time.Minute).Unix(),
	})

	v := NewDevJWTVerifier()
	claims, err := v.Verify(token)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if claims.SessionID != "stream_abc_123" {
		t.Fatalf("SessionID = %q, want stream_abc_123", claims.SessionID)
	}
	if claims.Role != "host" {
		t.Fatalf("Role = %q, want host", claims.Role)
	}
	if claims.UserID != "user-1" {
		t.Fatalf("UserID = %q, want user-1", claims.UserID)
	}
}

func TestVerifyDevModeNonJWTUsesEmptySessionRole(t *testing.T) {
	t.Parallel()

	v := NewDevJWTVerifier()
	claims, err := v.Verify("dev")
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if claims.SessionID != "" {
		t.Fatalf("SessionID = %q, want empty", claims.SessionID)
	}
	if claims.Role != "" {
		t.Fatalf("Role = %q, want empty", claims.Role)
	}
}

func signTestToken(t *testing.T, payload map[string]interface{}) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims(payload))
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	return signed
}

func TestVerifyWithPublicKeyRejectsBadSignature(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey other: %v", err)
	}

	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
	tmp := t.TempDir() + "/pub.pem"
	if err := os.WriteFile(tmp, pubPEM, 0o600); err != nil {
		t.Fatalf("write pub: %v", err)
	}

	v, err := NewJWTVerifier(tmp)
	if err != nil {
		t.Fatalf("NewJWTVerifier: %v", err)
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"uid":  "user-1",
		"sid":  "stream_abc",
		"type": "stream",
		"role": "host",
		"exp":  time.Now().Add(5 * time.Minute).Unix(),
	})
	signed, err := token.SignedString(other)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}

	if _, err := v.Verify(signed); err == nil {
		t.Fatal("expected signature verification failure")
	}
}

// verifierFor writes key's public half and returns a verifier for it, with
// SFU_REGION set to region.
func verifierFor(t *testing.T, key *rsa.PrivateKey, region string) *JWTVerifier {
	t.Helper()
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	tmp := t.TempDir() + "/pub.pem"
	if err := os.WriteFile(tmp, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o600); err != nil {
		t.Fatalf("write pub: %v", err)
	}
	t.Setenv("SFU_REGION", region)
	v, err := NewJWTVerifier(tmp)
	if err != nil {
		t.Fatalf("NewJWTVerifier: %v", err)
	}
	return v
}

func signedWithRegion(t *testing.T, key *rsa.PrivateKey, region string) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss":  Issuer,
		"uid":  "user-1",
		"sid":  "stream_abc",
		"type": "stream",
		"role": "viewer",
		"exp":  time.Now().Add(5 * time.Minute).Unix(),
	}
	if region != "" {
		claims["region"] = region
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(key)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}
	return signed
}

// The region claim is checked (streaming-quality stage 6): a token the
// backend signed for the SFU of another region is refused, so it cannot
// open a second, empty session of the same id here. A token without the
// claim, and an SFU without a region, pass.
func TestVerifyChecksTheRegionClaim(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	v := verifierFor(t, key, "eu-west")
	if _, err := v.Verify(signedWithRegion(t, key, "eu-west")); err != nil {
		t.Fatalf("a token for this region: %v", err)
	}
	if _, err := v.Verify(signedWithRegion(t, key, "us-east")); !errors.Is(err, ErrWrongRegion) {
		t.Fatalf("a token for another region: got %v, want ErrWrongRegion", err)
	}
	if _, err := v.Verify(signedWithRegion(t, key, "")); err != nil {
		t.Fatalf("a token without the claim: %v", err)
	}

	unset := verifierFor(t, key, "")
	if _, err := unset.Verify(signedWithRegion(t, key, "us-east")); err != nil {
		t.Fatalf("an SFU without a region checks nothing: %v", err)
	}
}

// Without a key and outside dev mode the verifier refuses every token, a
// well-formed unsigned JWT and a plain string alike.
func TestVerifyWithoutKeyRefusesEveryToken(t *testing.T) {
	t.Parallel()

	v, err := NewJWTVerifier("")
	if err != nil {
		t.Fatalf("NewJWTVerifier: %v", err)
	}
	unsigned := signTestToken(t, map[string]interface{}{
		"uid":  "user-1",
		"type": "voice",
		"exp":  time.Now().Add(5 * time.Minute).Unix(),
	})
	for _, tok := range []string{unsigned, "dev", ""} {
		if _, err := v.Verify(tok); !errors.Is(err, ErrNoKey) {
			t.Errorf("Verify(%q) error = %v, want ErrNoKey", tok, err)
		}
	}
}

// A verified token must come from the backend (iss), expire (exp) and name
// what it binds: a stream token its session and role, a voice token its
// crew and channel. The join handlers read an empty claim as "any".
func TestVerifyRequiresIssuerExpiryAndBindingClaims(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	v := verifierFor(t, key, "")
	exp := time.Now().Add(5 * time.Minute).Unix()
	voice := func() jwt.MapClaims {
		return jwt.MapClaims{"iss": Issuer, "uid": "user-1", "type": "voice", "role": "member",
			"sid": "voice:crew-1:ch-1", "crew_id": "crew-1", "ch_id": "ch-1", "exp": exp}
	}
	stream := func() jwt.MapClaims {
		return jwt.MapClaims{"iss": Issuer, "uid": "user-1", "type": "stream", "role": "viewer",
			"sid": "stream_abc", "crew_id": "crew-1", "exp": exp}
	}
	without := func(c jwt.MapClaims, key string) jwt.MapClaims { delete(c, key); return c }
	with := func(c jwt.MapClaims, key string, v any) jwt.MapClaims { c[key] = v; return c }

	cases := []struct {
		name   string
		claims jwt.MapClaims
		ok     bool
	}{
		{"voice", voice(), true},
		{"stream", stream(), true},
		{"no iss", without(voice(), "iss"), false},
		{"other iss", with(voice(), "iss", "someone-else"), false},
		{"no exp", without(voice(), "exp"), false},
		{"voice without ch_id", without(voice(), "ch_id"), false},
		{"voice without crew_id", without(voice(), "crew_id"), false},
		{"stream without sid", without(stream(), "sid"), false},
		{"stream without role", without(stream(), "role"), false},
		{"other type", with(voice(), "type", "admin"), false},
	}
	for _, tc := range cases {
		signed, err := jwt.NewWithClaims(jwt.SigningMethodRS256, tc.claims).SignedString(key)
		if err != nil {
			t.Fatalf("SignedString: %v", err)
		}
		_, err = v.Verify(signed)
		if tc.ok && err != nil {
			t.Errorf("%s: refused: %v", tc.name, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
}
