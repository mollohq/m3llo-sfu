package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserID    string `json:"uid"`
	Username  string `json:"username"`
	SessionID string `json:"sid"`
	Type      string `json:"type"` // "stream" or "voice"
	Role      string `json:"role"` // "host", "viewer", "member"
	CrewID    string `json:"crew_id"`
	ChannelID string `json:"ch_id"`
	Region    string `json:"region"`
	ExpiresAt int64  `json:"exp"`
}

type JWTVerifier struct {
	publicKey *rsa.PublicKey
	// dev accepts any token without a key (SFU_DEV_MODE). Without a key and
	// without dev, every token is refused.
	dev bool
	// region is this SFU's region (SFU_REGION). A verified token whose
	// region claim names another region is refused: the backend signed it
	// for the SFU of that region, where the session lives. Empty disables
	// the check.
	region string
}

// regionEnv is the variable config.go reads for the SFU's region. The
// verifier reads it too, so the check needs no change outside this file.
const regionEnv = "SFU_REGION"

// ErrNoKey is returned for every token by a verifier with no public key
// outside dev mode.
var ErrNoKey = errors.New("no JWT public key configured (SFU_JWT_PUB_KEY)")

// NewDevJWTVerifier accepts any token and trusts its claims without a
// signature check. Local development only (SFU_DEV_MODE).
func NewDevJWTVerifier() *JWTVerifier {
	return &JWTVerifier{dev: true}
}

// NewJWTVerifier verifies RS256 tokens with the public key at pubKeyPath.
// With an empty path it refuses every token.
func NewJWTVerifier(pubKeyPath string) (*JWTVerifier, error) {
	if pubKeyPath == "" {
		return &JWTVerifier{}, nil
	}

	data, err := os.ReadFile(pubKeyPath)
	if err != nil {
		return nil, fmt.Errorf("read public key: %w", err)
	}

	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse public key: %w", err)
	}

	rsaPub, ok := pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("public key is not RSA")
	}

	return &JWTVerifier{publicKey: rsaPub, region: strings.TrimSpace(os.Getenv(regionEnv))}, nil
}

// ErrWrongRegion is returned for a token signed for the SFU of another
// region.
var ErrWrongRegion = errors.New("token is for another SFU region")

// checkRegion refuses a token whose region claim names another region than
// this SFU's. A token without the claim (an older backend) and an SFU
// without a region pass: the check only catches a token that reached the
// wrong SFU, which would otherwise start a second, empty session of the
// same id there.
func (v *JWTVerifier) checkRegion(claims *Claims) error {
	if v.region == "" || claims.Region == "" || claims.Region == v.region {
		return nil
	}
	return fmt.Errorf("%w: claim %q, this SFU %q", ErrWrongRegion, claims.Region, v.region)
}

func (v *JWTVerifier) Verify(tokenStr string) (*Claims, error) {
	if v.publicKey == nil {
		if !v.dev {
			return nil, ErrNoKey
		}
		if claims, err := parseUnverifiedClaims(tokenStr); err == nil {
			return claims, nil
		}
		// Non-JWT tokens (e.g. "dev"): accept with empty sid/role so join_stream
		// picks session/role from the client message (same as test_password mode).
		return devFallbackClaims(), nil
	}

	token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return v.publicKey, nil
	}, jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(Issuer),
		jwt.WithExpirationRequired())
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, errors.New("invalid token claims")
	}

	claims, err := mapClaimsToClaims(mapClaims)
	if err != nil {
		return nil, err
	}
	if err := requireClaims(claims); err != nil {
		return nil, err
	}
	if err := v.checkRegion(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

// Issuer is the iss claim of every token the backend signs. A token
// without it is refused, so a token the same key signed for another use
// cannot join.
const Issuer = "mello-nakama"

// requireClaims refuses a verified token that leaves a binding claim empty.
// The join handlers treat an empty claim as "any", which only test and dev
// mode may rely on: a stream token names its session and role, a voice
// token its crew and channel.
func requireClaims(c *Claims) error {
	switch c.Type {
	case "stream":
		if c.SessionID == "" || c.Role == "" {
			return errors.New("stream token missing sid or role claim")
		}
	case "voice":
		if c.CrewID == "" || c.ChannelID == "" {
			return errors.New("voice token missing crew_id or ch_id claim")
		}
	default:
		return fmt.Errorf("token type %q is not stream or voice", truncate(c.Type, 32))
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func devFallbackClaims() *Claims {
	return &Claims{
		UserID:    "dev-user",
		Type:      "stream",
		ExpiresAt: time.Now().Add(1 * time.Hour).Unix(),
	}
}

func parseUnverifiedClaims(tokenStr string) (*Claims, error) {
	parser := jwt.NewParser()
	token, _, err := parser.ParseUnverified(tokenStr, jwt.MapClaims{})
	if err != nil {
		return nil, err
	}
	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid token claims")
	}
	return mapClaimsToClaims(mapClaims)
}

func mapClaimsToClaims(mapClaims jwt.MapClaims) (*Claims, error) {
	claims := &Claims{}
	if v, ok := mapClaims["uid"].(string); ok {
		claims.UserID = v
	}
	if v, ok := mapClaims["sid"].(string); ok {
		claims.SessionID = v
	}
	if v, ok := mapClaims["type"].(string); ok {
		claims.Type = v
	}
	if v, ok := mapClaims["role"].(string); ok {
		claims.Role = v
	}
	if v, ok := mapClaims["crew_id"].(string); ok {
		claims.CrewID = v
	}
	if v, ok := mapClaims["ch_id"].(string); ok {
		claims.ChannelID = v
	}
	if v, ok := mapClaims["region"].(string); ok {
		claims.Region = v
	}
	if v, ok := mapClaims["username"].(string); ok {
		claims.Username = v
	}
	if v, ok := mapClaims["exp"].(float64); ok {
		claims.ExpiresAt = int64(v)
		if time.Now().Unix() > claims.ExpiresAt {
			return nil, errors.New("token expired")
		}
	}

	if claims.UserID == "" {
		return nil, errors.New("token missing uid claim")
	}

	return claims, nil
}
