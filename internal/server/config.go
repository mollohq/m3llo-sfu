package server

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	// Server
	Addr      string // :8443
	AdminAddr string // :8080
	PublicIP  string // Static external IP
	Region    string // eu-west, us-east
	ServerID  string // sfu-eu-01

	// Passive ICE-TCP port next to the UDP mux (0 disables). Only with PublicIP.
	ICETCPPort int

	// TLS
	CertFile string
	KeyFile  string

	// Auth
	JWTPubKeyPath string
	// TestPassword enables the test-mode join (?test_password=, 01-SFU.md
	// section 5.1) for soaks, canaries and the QA harness. Empty: off.
	TestPassword string
	// DevMode accepts any token without a key and leaves the admin API open
	// without a password. Local development only.
	DevMode bool

	// Admin dashboard
	AdminPassword string
	AdminIPs      []string // IP allowlist
	// TrustedProxies are the proxies (IPs or CIDRs) whose X-Forwarded-For
	// and X-Real-IP name the client. Empty: the headers are ignored.
	TrustedProxies []string

	// Mission control stats endpoint (GET /stats bearer token)
	StatsToken string

	// Peer discovery (sibling SFU instances)
	Peers []string // ["sfu-a.example.com:8080", "sfu-b.example.com:8080"]

	// Limits
	MaxSessions        int
	MaxPeersPerSession int

	// Stream snapshots: any S3-compatible store (Cloudflare R2, MinIO, AWS
	// S3). The env names say R2 for history.
	R2Endpoint          string
	R2Bucket            string
	R2AccessKeyID       string
	R2SecretAccessKey   string
	R2PublicDomain      string
	SnapshotIntervalSec int

	// Logging
	LogLevel string // debug, info, warn, error

	// RejectLegacyAudioDC closes the unreliable "audio" DataChannel that
	// clients before the stream-5 change open (SFU_REJECT_LEGACY_AUDIO_DC).
	// Off by default: the SFU keeps it open and forwards nothing, so older
	// clients still become ready. Turn it on when no such client is left.
	RejectLegacyAudioDC bool

	// Media admin store (internal/store). StorePath empty: no store, the
	// SFU writes nothing to disk (SFU_STORE_PATH). StoreRetentionDays is
	// SFU_STORE_RETENTION_DAYS (default 30). LogRingSize is the in-memory
	// log ring (SFU_LOG_RING_SIZE, default 10 000).
	StorePath          string
	StoreRetentionDays int
	LogRingSize        int
}

func ConfigFromEnv() Config {
	cfg := Config{
		Addr:                envOr("SFU_ADDR", ":8443"),
		AdminAddr:           envOr("SFU_ADMIN_ADDR", ":8080"),
		PublicIP:            os.Getenv("SFU_PUBLIC_IP"),
		ICETCPPort:          envOrInt("SFU_ICE_TCP_PORT", 10000),
		Region:              envOr("SFU_REGION", "eu-west"),
		ServerID:            envOr("SFU_SERVER_ID", "sfu-01"),
		CertFile:            os.Getenv("SFU_CERT_FILE"),
		KeyFile:             os.Getenv("SFU_KEY_FILE"),
		JWTPubKeyPath:       os.Getenv("SFU_JWT_PUB_KEY"),
		AdminPassword:       os.Getenv("SFU_ADMIN_PASSWORD"),
		TestPassword:        os.Getenv("SFU_TEST_PASSWORD"),
		DevMode:             envBool("SFU_DEV_MODE"),
		StatsToken:          os.Getenv("SFU_STATS_TOKEN"),
		R2Endpoint:          os.Getenv("SFU_R2_ENDPOINT"),
		R2Bucket:            os.Getenv("SFU_R2_BUCKET"),
		R2AccessKeyID:       os.Getenv("SFU_R2_ACCESS_KEY_ID"),
		R2SecretAccessKey:   os.Getenv("SFU_R2_SECRET_ACCESS_KEY"),
		R2PublicDomain:      os.Getenv("SFU_R2_PUBLIC_DOMAIN"),
		SnapshotIntervalSec: envOrInt("SFU_SNAPSHOT_INTERVAL_S", 10),
		MaxSessions:         envOrInt("SFU_MAX_SESSIONS", 500),
		MaxPeersPerSession:  envOrInt("SFU_MAX_PEERS_PER_SESSION", 100),
		LogLevel:            envOr("SFU_LOG_LEVEL", "info"),
		StorePath:           os.Getenv("SFU_STORE_PATH"),
		StoreRetentionDays:  envOrInt("SFU_STORE_RETENTION_DAYS", 30),
		LogRingSize:         envOrInt("SFU_LOG_RING_SIZE", 10_000),
	}

	cfg.TrustedProxies = splitList(os.Getenv("SFU_TRUSTED_PROXIES"))
	cfg.AdminIPs = splitList(os.Getenv("SFU_ADMIN_IPS"))

	if peers := os.Getenv("SFU_PEERS"); peers != "" {
		cfg.Peers = strings.Split(peers, ",")
		for i := range cfg.Peers {
			cfg.Peers[i] = strings.TrimSpace(cfg.Peers[i])
		}
	}

	cfg.RejectLegacyAudioDC = envBool("SFU_REJECT_LEGACY_AUDIO_DC")
	return cfg
}

// splitList splits a comma-separated list and drops blank entries.
func splitList(v string) []string {
	var out []string
	for _, item := range strings.Split(v, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// envBool is true for "1", "true", "yes" or "on" (any case).
func envBool(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envOrInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
