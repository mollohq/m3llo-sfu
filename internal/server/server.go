package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
	"github.com/mollohq/m3llo-sfu/internal/snapshot"
	"github.com/mollohq/m3llo-sfu/internal/store"
	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

var Version = "0.1.0"

type Server struct {
	config    Config
	Logger    *logging.Logger
	metrics   *metrics.Metrics
	sessions  *SessionStore
	auth      *auth.JWTVerifier
	peers     *PeerPoller
	webrtcAPI *webrtc.API
	// viewerAPI creates stream viewer PeerConnections: it runs a gcc
	// estimator per PeerConnection, handed over through viewerBWE. nil in
	// tests that set only webrtcAPI.
	viewerAPI *webrtc.API
	viewerBWE *viewerBWERegistry
	// dtlsCert is the shared DTLS-SRTP certificate, rotated in place by
	// runDTLSCertRotation. Read atomically per PeerConnection in signaling.go.
	dtlsCert atomic.Pointer[webrtc.Certificate]

	peakSessions *peakSessionsTracker

	// clientIPs finds a request's client address (SFU_TRUSTED_PROXIES);
	// authFails limits failed admin and test-mode passwords per address.
	// A nil authFails limits nothing.
	clientIPs clientIPResolver
	authFails *failLimiter
	// authFailLogs samples the auth_failed log of /ws: anyone can send a
	// bad token, so the entries are capped per minute.
	authFailLogs *logSampler

	snapshotEnabled  bool
	snapshotInterval time.Duration
	snapshotR2       *snapshot.R2Client

	signalingServer *http.Server // :8443 WSS
	adminServer     *http.Server // :8080 health + admin

	// media feeds the media admin's disk store; nil without SFU_STORE_PATH.
	media *mediaRecorder
	// bundles keeps the clients' log bundles on the store's volume.
	bundles *bundleManager

	stopCh chan struct{}
}

type relayCheckpoint struct {
	at       time.Time
	snapshot StreamRelaySnapshot
}

func streamRTPTelemetry(
	current StreamRelaySnapshot,
	previous StreamRelaySnapshot,
	elapsedSec float64,
) (logging.D, []string) {
	if elapsedSec <= 0 {
		elapsedSec = 1
	}
	dIn := current.RTPInPackets - previous.RTPInPackets
	dInBytes := current.RTPInBytes - previous.RTPInBytes
	dAttempts := current.RTPFanoutAttempts - previous.RTPFanoutAttempts
	dForwarded := current.RTPForwardedPackets - previous.RTPForwardedPackets
	dForwardedBytes := current.RTPForwardedBytes - previous.RTPForwardedBytes
	dDropped := current.RTPDroppedPackets - previous.RTPDroppedPackets
	dDroppedBytes := current.RTPDroppedBytes - previous.RTPDroppedBytes
	dQueueDropped := current.RTPQueueDroppedPackets - previous.RTPQueueDroppedPackets
	dQueueDroppedBytes := current.RTPQueueDroppedBytes - previous.RTPQueueDroppedBytes
	dDuplicates := current.RTPDuplicatePackets - previous.RTPDuplicatePackets

	var fanoutDropPct float64
	fanoutDropped := dDropped - dDuplicates
	if dAttempts > 0 && fanoutDropped > 0 {
		fanoutDropPct = float64(fanoutDropped) * 100 / float64(dAttempts)
	}
	fields := logging.D{
		"rtp_in_pps":             float64(dIn) / elapsedSec,
		"rtp_in_mbps":            float64(dInBytes) * 8 / elapsedSec / 1_000_000,
		"rtp_fanout_attempt_pps": float64(dAttempts) / elapsedSec,
		"rtp_forwarded_pps":      float64(dForwarded) / elapsedSec,
		"rtp_forwarded_mbps":     float64(dForwardedBytes) * 8 / elapsedSec / 1_000_000,
		"rtp_drop_pps":           float64(dDropped) / elapsedSec,
		"rtp_drop_mbps":          float64(dDroppedBytes) * 8 / elapsedSec / 1_000_000,
		"rtp_fanout_drop_pct":    fanoutDropPct,
		"rtp_queue_drop_pps":     float64(dQueueDropped) / elapsedSec,
		"rtp_queue_drop_mbps":    float64(dQueueDroppedBytes) * 8 / elapsedSec / 1_000_000,
		"rtp_duplicate_pps":      float64(dDuplicates) / elapsedSec,
		"rtp_totals": logging.D{
			"in_packets":            current.RTPInPackets,
			"fanout_attempts":       current.RTPFanoutAttempts,
			"forwarded_packets":     current.RTPForwardedPackets,
			"forwarded_bytes":       current.RTPForwardedBytes,
			"dropped_packets":       current.RTPDroppedPackets,
			"dropped_bytes":         current.RTPDroppedBytes,
			"queue_dropped_packets": current.RTPQueueDroppedPackets,
			"queue_dropped_bytes":   current.RTPQueueDroppedBytes,
			"duplicate_packets":     current.RTPDuplicatePackets,
			"rtx_recovered_packets": current.RTPRTXRecovered,
			"rtx_duplicate_packets": current.RTPRTXDuplicate,
		},
	}
	var reasons []string
	if dDropped > 0 {
		reasons = append(reasons, "rtp_drop")
	}
	if dQueueDropped > 0 {
		reasons = append(reasons, "rtp_queue_drop")
	}
	return fields, reasons
}

// configureICE sets ICE-lite, the public host address and the shared media
// listeners: a UDP mux on udpAddr and, when tcpAddr is not empty, a passive
// ICE-TCP mux on tcpAddr for clients on networks that block UDP. Both
// advertise publicIP. The returned func closes the listeners.
func configureICE(se *webrtc.SettingEngine, publicIP, udpAddr, tcpAddr string) (func(), error) {
	se.SetLite(true)
	se.SetNAT1To1IPs([]string{publicIP}, webrtc.ICECandidateTypeHost)

	udpConn, err := net.ListenPacket("udp4", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen UDP for ICE mux: %w", err)
	}
	udpMux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: udpConn})
	se.SetICEUDPMux(udpMux)
	if tcpAddr == "" {
		se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
		return func() { _ = udpMux.Close() }, nil
	}

	tcpListener, err := net.Listen("tcp4", tcpAddr)
	if err != nil {
		_ = udpMux.Close()
		return nil, fmt.Errorf("listen TCP for ICE mux: %w", err)
	}
	tcpMux := ice.NewTCPMuxDefault(ice.TCPMuxParams{
		Listener:       tcpListener,
		ReadBufferSize: 8,
		// A full write buffer drops packets instead of blocking the
		// forwarding loop on a slow TCP receiver.
		WriteBufferSize: 4 * 1024 * 1024,
	})
	se.SetICETCPMux(tcpMux)
	// UDP host candidates keep the higher priority, so a client with UDP
	// selects a UDP pair; TCP is the fallback for UDP-blocked networks.
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeTCP4})

	return func() {
		_ = udpMux.Close()
		_ = tcpMux.Close()
	}, nil
}

// rtpRxCheckpoint tracks whether a session is receiving any inbound RTP.
// Used to detect a silent media blackout: the SFU stops receiving RTP from
// every peer while >=2 members are still connected and signaling looks healthy
// (no read errors, no state change). Observed in the field as clients that keep
// encoding into a black hole until an unrelated reconnect forces a re-wire.
type rtpRxCheckpoint struct {
	lastPktsRecv int64
	flatSec      int
	warned       bool
}

func New(cfg Config) (*Server, error) {
	logger := logging.NewWithRing(cfg.LogLevel, cfg.LogRingSize)

	jwtVerifier, err := newVerifier(cfg, logger)
	if err != nil {
		return nil, err
	}

	se := webrtc.SettingEngine{}
	se.SetHandleUndeclaredSSRCWithoutAnswer(true)
	if cfg.PublicIP != "" {
		tcpAddr := ""
		if cfg.ICETCPPort > 0 {
			tcpAddr = fmt.Sprintf("0.0.0.0:%d", cfg.ICETCPPort)
		}
		if _, err := configureICE(&se, cfg.PublicIP, "0.0.0.0:10000", tcpAddr); err != nil {
			return nil, err
		}
		if tcpAddr != "" {
			logger.Info("ice_tcp_listening", logging.D{"addr": tcpAddr, "public_ip": cfg.PublicIP})
		}
	}
	api, err := newSFUAPI(se)
	if err != nil {
		return nil, err
	}
	viewerBWE := newViewerBWERegistry()
	viewerAPI, err := newSFUViewerAPI(se, viewerBWE)
	if err != nil {
		return nil, err
	}

	// Pre-generate a single DTLS certificate shared by all PeerConnections
	// (avoids per-connection ECDSA key generation). It is rotated in place by
	// runDTLSCertRotation so a long-running process never serves an expired cert.
	dtlsCert, err := generateDTLSCert()
	if err != nil {
		return nil, err
	}

	m := metrics.New()
	snapshotInterval := time.Duration(cfg.SnapshotIntervalSec) * time.Second
	if snapshotInterval <= 0 {
		snapshotInterval = 10 * time.Second
	}

	var snapshotR2 *snapshot.R2Client
	snapshotEnabled := false
	if cfg.R2Endpoint != "" {
		snapshotR2, err = snapshot.NewR2Client(snapshot.R2Config{
			Endpoint:        cfg.R2Endpoint,
			Bucket:          cfg.R2Bucket,
			AccessKeyID:     cfg.R2AccessKeyID,
			SecretAccessKey: cfg.R2SecretAccessKey,
			PublicDomain:    cfg.R2PublicDomain,
		})
		if err != nil {
			return nil, fmt.Errorf("init snapshot R2 client: %w", err)
		}
		if !snapshot.DecoderAvailable() {
			return nil, fmt.Errorf("SFU_R2_ENDPOINT is set but this build has no snapshot decoder: build with OpenH264 (docker build --build-arg OPENH264=1, see SELF-HOSTING.md) or leave SFU_R2_ENDPOINT empty")
		}
		snapshotEnabled = true
		logger.Info("snapshot_pipeline_enabled", logging.D{
			"interval_s": int(snapshotInterval.Seconds()),
			"bucket":     cfg.R2Bucket,
		})
	}

	s := &Server{
		config:           cfg,
		Logger:           logger,
		metrics:          m,
		sessions:         NewSessionStore(logger),
		auth:             jwtVerifier,
		peers:            NewPeerPoller(cfg.Peers, cfg.ServerID, cfg.Region),
		webrtcAPI:        api,
		viewerAPI:        viewerAPI,
		viewerBWE:        viewerBWE,
		peakSessions:     newPeakSessionsTracker(),
		clientIPs:        newClientIPResolver(cfg.TrustedProxies),
		authFails:        newFailLimiter(authFailMax, authFailWindow),
		authFailLogs:     newLogSampler(authFailLogMax, time.Minute),
		snapshotEnabled:  snapshotEnabled,
		snapshotInterval: snapshotInterval,
		snapshotR2:       snapshotR2,
		stopCh:           make(chan struct{}),
	}
	s.dtlsCert.Store(dtlsCert)

	if cfg.StorePath != "" {
		st, err := store.Open(cfg.StorePath, store.Options{
			Retention: time.Duration(cfg.StoreRetentionDays) * 24 * time.Hour,
		})
		if err != nil {
			// The store is history, not media: the SFU runs without it.
			logger.Error("store_open_failed", logging.D{"path": cfg.StorePath, "error": err.Error()})
		} else {
			s.media = newMediaRecorder(st)
			s.media.sample = s.instanceSample
			retention := time.Duration(cfg.StoreRetentionDays) * 24 * time.Hour
			if retention <= 0 {
				retention = 30 * 24 * time.Hour
			}
			s.bundles = newBundleManager(filepath.Join(filepath.Dir(cfg.StorePath), "bundles"), retention, logger)
			s.media.bundles = s.bundles
			logger.SetSink(s.media.onLog)
			logger.Info("store_enabled", logging.D{
				"path":           cfg.StorePath,
				"retention_days": cfg.StoreRetentionDays,
			})
		}
	}

	s.peers.SetLocalStatus(func() *PeerInstanceStatus {
		streamCount, voiceCount := s.sessions.Count()
		_, bwOut, _ := m.Rates()
		return &PeerInstanceStatus{
			ServerID:     cfg.ServerID,
			Region:       cfg.Region,
			Status:       "ok",
			Sessions:     streamCount + voiceCount,
			Peers:        int(m.ActivePeers.Load()),
			BandwidthOut: bwOut,
			Uptime:       m.UptimeSeconds(),
			Self:         true,
			LastSeen:     time.Now(),
		}
	})

	return s, nil
}

func (s *Server) Run() {
	s.Logger.Info("server_started", logging.D{
		"region":     s.config.Region,
		"server_id":  s.config.ServerID,
		"version":    Version,
		"public_ip":  s.config.PublicIP,
		"addr":       s.config.Addr,
		"admin_addr": s.config.AdminAddr,
	})

	if c := s.currentDTLSCert(); c != nil {
		s.Logger.Info("dtls_cert_active", logging.D{
			"expires_at": c.Expires().UTC().Format(time.RFC3339),
		})
	}

	// Start background collectors
	go s.metrics.RunCollector(s.stopCh)
	go s.sessions.RunDrainLoop(s.stopCh)
	go s.peers.Run(s.stopCh)
	go s.runDTLSCertRotation(s.stopCh)
	go s.runSlowPeerChecker(s.stopCh)
	go s.runSessionStatsLoop(s.stopCh)
	if s.media != nil {
		go backfillGrades(s.media.store, func(event string, data map[string]interface{}) {
			s.Logger.Info(event, data)
		})
		go s.media.run(s, s.stopCh)
	}

	// Admin + health server (:8080)
	adminMux := s.adminMux()

	s.adminServer = &http.Server{
		Addr:              s.config.AdminAddr,
		Handler:           adminMux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	go func() {
		s.Logger.Info("admin_listening", logging.D{"addr": s.config.AdminAddr, "admin_api": adminAPIState(s.config)})
		if s.config.CertFile != "" && s.config.KeyFile != "" {
			if err := s.adminServer.ListenAndServeTLS(s.config.CertFile, s.config.KeyFile); err != http.ErrServerClosed {
				s.Logger.Error("admin_server_error", logging.D{"error": err.Error()})
			}
		} else {
			if err := s.adminServer.ListenAndServe(); err != http.ErrServerClosed {
				s.Logger.Error("admin_server_error", logging.D{"error": err.Error()})
			}
		}
	}()

	// Signaling server (:8443)
	sigMux := http.NewServeMux()
	sigMux.HandleFunc("/ws", s.handleWebSocket)

	s.signalingServer = &http.Server{
		Addr:              s.config.Addr,
		Handler:           sigMux,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	s.Logger.Info("signaling_listening", logging.D{"addr": s.config.Addr})
	if s.config.CertFile != "" && s.config.KeyFile != "" {
		if err := s.signalingServer.ListenAndServeTLS(s.config.CertFile, s.config.KeyFile); err != http.ErrServerClosed {
			s.Logger.Error("signaling_server_error", logging.D{"error": err.Error()})
		}
	} else {
		s.Logger.Warn("signaling_no_tls", logging.D{"msg": "Running without TLS. Use a reverse proxy for production."})
		if err := s.signalingServer.ListenAndServe(); err != http.ErrServerClosed {
			s.Logger.Error("signaling_server_error", logging.D{"error": err.Error()})
		}
	}
}

func (s *Server) runSlowPeerChecker(stop <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.checkSlowPeers(time.Now())
		case <-stop:
			return
		}
	}
}

// checkSlowPeers removes the peers that are slow now. A slow stream viewer
// is first served keyframes only, with its estimate clamped, and removed
// with PEER_SLOW only after streamSlowViewerKick (stream_slow_viewer.go).
func (s *Server) checkSlowPeers(now time.Time) {
	for _, sess := range s.sessions.All() {
		if stream, ok := sess.(*StreamSession); ok {
			for _, viewer := range stream.SlowViewers(now) {
				s.Logger.Warn("peer_force_disconnect", logging.D{
					"session":      sess.ID(),
					"user":         viewer.UserID,
					"reason":       "PEER_SLOW",
					"slow_sec":     int(streamSlowViewerKick.Seconds()),
					"pkts_sent":    viewer.PacketsSent.Load(),
					"pkts_dropped": viewer.PacketsDropped.Load(),
					"duration_sec": int(now.Sub(viewer.ConnectedAt).Seconds()),
				})
				s.metrics.PeersForceDisconnected.Add(1)
				viewer.SendSignaling("error", ErrorData{
					Code:    "PEER_SLOW",
					Message: "Viewer too slow for the stream for >15 seconds",
				})
				removeSlowPeer(sess, viewer)
			}
		}
		for _, peer := range sess.AllPeers() {
			slowDataChannel := peer.IsSlowPeer()
			slowRTP := peer.IsRTPStalled()
			if slowDataChannel || slowRTP {
				reasonCode := "PEER_SLOW"
				reasonMessage := "Send buffer overflow for >5 seconds"
				if slowRTP {
					reasonCode = "PEER_STALLED_RTP"
					reasonMessage = "RTP forwarding stalled for >5 seconds"
				}
				s.Logger.Warn("peer_force_disconnect", logging.D{
					"session":      sess.ID(),
					"user":         peer.UserID,
					"reason":       reasonCode,
					"pkts_sent":    peer.PacketsSent.Load(),
					"pkts_recv":    peer.PacketsReceived.Load(),
					"pkts_dropped": peer.PacketsDropped.Load(),
					"bytes_sent":   peer.BytesSent.Load(),
					"bytes_recv":   peer.BytesReceived.Load(),
					"peak_buf_pct": peer.BufferUsage.Load(),
					"rtp_errs":     peer.RTPWriteErrors.Load(),
					"rtp_streak":   peer.rtpErrStreak.Load(),
					"duration_sec": int(now.Sub(peer.ConnectedAt).Seconds()),
				})
				s.metrics.PeersForceDisconnected.Add(1)
				peer.SendSignaling("error", ErrorData{
					Code:    reasonCode,
					Message: reasonMessage,
				})
				removeSlowPeer(sess, peer)
			}
		}
	}
}

func removeSlowPeer(sess Session, peer *Peer) {
	sess.RemovePeerInstance(peer)
}

func (s *Server) runSessionStatsLoop(stop <-chan struct{}) {
	const (
		relayStatsSampleInterval  = 1 * time.Second
		relayStatsPeriodicEvery   = 10
		relayAnomalyBufferTrigger = uint64(32 * 1024)
	)
	const rtpBlackoutWarnSec = 15 // warn after this many seconds of zero inbound RTP
	ticker := time.NewTicker(relayStatsSampleInterval)
	defer ticker.Stop()
	relayState := make(map[string]relayCheckpoint)
	rtpRxState := make(map[string]rtpRxCheckpoint)
	tickCount := 0
	for {
		select {
		case <-ticker.C:
			tickCount++
			emitPeriodic := tickCount%relayStatsPeriodicEvery == 0
			now := time.Now()
			// Track the daily peak of concurrent sessions (stream + voice) for
			// /stats. Sampled every second so brief spikes between polls count.
			// Real users only: a synthetic canary session must not set the peak.
			streamCount, voiceCount := s.sessions.CountReal()
			s.peakSessions.Observe(streamCount + voiceCount)
			seen := make(map[string]struct{})
			for _, sess := range s.sessions.All() {
				seen[sess.ID()] = struct{}{}
				peers := sess.AllPeers()
				if len(peers) == 0 {
					continue
				}
				var pktsSent, pktsRecv, pktsDropped int64
				var bytesSent, bytesRecv int64
				var maxBuf int32
				for _, p := range peers {
					pktsSent += p.PacketsSent.Load()
					pktsRecv += p.PacketsReceived.Load()
					pktsDropped += p.PacketsDropped.Load()
					bytesSent += p.BytesSent.Load()
					bytesRecv += p.BytesReceived.Load()
					if buf := p.BufferUsage.Load(); buf > maxBuf {
						maxBuf = buf
					}
				}
				if emitPeriodic {
					s.Logger.Debug("stats", logging.D{
						"session":        sess.ID(),
						"wall_ms":        now.UnixMilli(),
						"session_age_ms": now.Sub(sess.CreatedAt()).Milliseconds(),
						"type":           sess.Type(),
						"members":        len(peers),
						"pkts_sent":      pktsSent,
						"pkts_recv":      pktsRecv,
						"pkts_dropped":   pktsDropped,
						"bytes_sent":     bytesSent,
						"bytes_recv":     bytesRecv,
						"max_buf_pct":    int(maxBuf),
					})
				}

				// Silent RTP-blackout detection (voice only): if the SFU receives
				// no inbound RTP from ANY peer for an extended window while >=2
				// members are connected, audio is dead even though signaling looks
				// fine. To avoid false alarms during normal VAD-gated silence
				// (nobody talking -> no RTP, which is expected), only warn when a
				// peer's in-band speaking flag is on -- i.e. it believes it is
				// transmitting but the SFU receives nothing. Emit once per episode
				// (and re-warn periodically) so the blackout shows up without spam.
				if sess.Type() == "voice" {
					anyRecentSpeaking := false
					for _, p := range peers {
						if p.speaking.Load() {
							anyRecentSpeaking = true
							break
						}
					}
					rx := rtpRxState[sess.ID()]
					if len(peers) >= 2 && pktsRecv == rx.lastPktsRecv {
						rx.flatSec++
						if rx.flatSec >= rtpBlackoutWarnSec &&
							(rx.flatSec-rtpBlackoutWarnSec)%rtpBlackoutWarnSec == 0 &&
							anyRecentSpeaking {
							s.Logger.Warn("rtp_blackout", logging.D{
								"session":   sess.ID(),
								"members":   len(peers),
								"flat_sec":  rx.flatSec,
								"pkts_recv": pktsRecv,
							})
							rx.warned = true
						}
					} else {
						if rx.warned {
							s.Logger.Info("rtp_blackout_cleared", logging.D{
								"session":   sess.ID(),
								"flat_sec":  rx.flatSec,
								"pkts_recv": pktsRecv,
							})
						}
						rx.flatSec = 0
						rx.warned = false
					}
					rx.lastPktsRecv = pktsRecv
					rtpRxState[sess.ID()] = rx
				}

				streamSess, ok := sess.(*StreamSession)
				if !ok {
					continue
				}
				// Re-evaluate aggregated REMB/PLI state even when no fresh viewer
				// feedback arrived this tick (lost RTCP, quiet viewer).
				streamSess.tickFeedback(now)
				current := streamSess.RelaySnapshot()
				prev, hadPrev := relayState[sess.ID()]
				relayState[sess.ID()] = relayCheckpoint{
					at:       now,
					snapshot: current,
				}
				if !hadPrev {
					continue
				}

				elapsedSec := now.Sub(prev.at).Seconds()
				if elapsedSec <= 0 {
					elapsedSec = relayStatsSampleInterval.Seconds()
				}

				dMediaInPkts := current.MediaInPackets - prev.snapshot.MediaInPackets
				dMediaInBytes := current.MediaInBytes - prev.snapshot.MediaInBytes
				dMediaFwdPkts := current.MediaForwardedPackets - prev.snapshot.MediaForwardedPackets
				dMediaFwdBytes := current.MediaForwardedBytes - prev.snapshot.MediaForwardedBytes
				dMediaDropPkts := current.MediaDroppedPackets - prev.snapshot.MediaDroppedPackets
				dMediaDropBytes := current.MediaDroppedBytes - prev.snapshot.MediaDroppedBytes
				dMediaAttempts := current.MediaForwardAttempts - prev.snapshot.MediaForwardAttempts
				dControlIn := current.ControlInPackets - prev.snapshot.ControlInPackets
				dControlFwd := current.ControlForwarded - prev.snapshot.ControlForwarded
				dControlDrop := current.ControlDropped - prev.snapshot.ControlDropped
				dKeyframeLike := current.KeyframeLikeMessages - prev.snapshot.KeyframeLikeMessages

				var dropPct float64
				if dMediaAttempts > 0 {
					dropPct = float64(dMediaDropPkts) * 100 / float64(dMediaAttempts)
				}
				rtpFields, rtpAnomalyReasons := streamRTPTelemetry(current, prev.snapshot, elapsedSec)

				var maxBufferedBytes uint64
				slowPeerCount := 0
				peerBuffers := make([]logging.D, 0, len(peers))
				for _, p := range peers {
					var bufferedBytes uint64
					if p.MediaChannel != nil {
						bufferedBytes = p.MediaChannel.BufferedAmount()
					}
					if bufferedBytes > maxBufferedBytes {
						maxBufferedBytes = bufferedBytes
					}
					bufferPct := int(p.BufferUsage.Load())
					if bufferPct >= 80 {
						slowPeerCount++
					}
					if bufferPct == 0 && bufferedBytes == 0 {
						continue
					}
					peerBuffers = append(peerBuffers, logging.D{
						"user":         p.UserID,
						"role":         p.Role,
						"buffer_pct":   bufferPct,
						"buffered":     bufferedBytes,
						"pkts_dropped": p.PacketsDropped.Load(),
					})
				}

				commonFields := logging.D{
					"session":           sess.ID(),
					"wall_ms":           now.UnixMilli(),
					"session_age_ms":    now.Sub(sess.CreatedAt()).Milliseconds(),
					"members":           len(peers),
					"media_in_mps":      float64(dMediaInPkts) / elapsedSec,
					"media_in_mbps":     float64(dMediaInBytes) * 8 / elapsedSec / 1_000_000,
					"media_fwd_mps":     float64(dMediaFwdPkts) / elapsedSec,
					"media_fwd_mbps":    float64(dMediaFwdBytes) * 8 / elapsedSec / 1_000_000,
					"media_drop_mps":    float64(dMediaDropPkts) / elapsedSec,
					"media_drop_mbps":   float64(dMediaDropBytes) * 8 / elapsedSec / 1_000_000,
					"media_drop_pct":    dropPct,
					"control_in_mps":    float64(dControlIn) / elapsedSec,
					"control_fwd_mps":   float64(dControlFwd) / elapsedSec,
					"control_drop_mps":  float64(dControlDrop) / elapsedSec,
					"keyframe_like_mps": float64(dKeyframeLike) / elapsedSec,
					"max_msg_bytes":     current.MaxMediaMessageBytes,
					"size_bucket_delta": logging.D{
						"le_1200":  current.SizeBuckets.LE1200 - prev.snapshot.SizeBuckets.LE1200,
						"le_8000":  current.SizeBuckets.LE8000 - prev.snapshot.SizeBuckets.LE8000,
						"le_20000": current.SizeBuckets.LE20000 - prev.snapshot.SizeBuckets.LE20000,
						"le_40000": current.SizeBuckets.LE40000 - prev.snapshot.SizeBuckets.LE40000,
						"gt_40000": current.SizeBuckets.GT40000 - prev.snapshot.SizeBuckets.GT40000,
					},
					"relay_totals": logging.D{
						"media_in_packets":        current.MediaInPackets,
						"media_forward_attempts":  current.MediaForwardAttempts,
						"media_forwarded_packets": current.MediaForwardedPackets,
						"media_dropped_packets":   current.MediaDroppedPackets,
						"media_forwarded_bytes":   current.MediaForwardedBytes,
						"media_dropped_bytes":     current.MediaDroppedBytes,
						"control_in_packets":      current.ControlInPackets,
						"control_forwarded":       current.ControlForwarded,
						"control_dropped":         current.ControlDropped,
						"keyframe_like_messages":  current.KeyframeLikeMessages,
					},
					"peers_over_80_pct":  slowPeerCount,
					"max_buffered_bytes": maxBufferedBytes,
				}
				for key, value := range rtpFields {
					commonFields[key] = value
				}

				if emitPeriodic {
					s.Logger.Info("stream_relay_stats", commonFields)
				}

				isAnomaly := dMediaDropPkts > 0 ||
					dControlDrop > 0 ||
					len(rtpAnomalyReasons) > 0 ||
					slowPeerCount > 0 ||
					maxBufferedBytes >= relayAnomalyBufferTrigger
				if isAnomaly {
					reasons := append([]string(nil), rtpAnomalyReasons...)
					if dMediaDropPkts > 0 {
						reasons = append(reasons, "media_drop")
					}
					if dControlDrop > 0 {
						reasons = append(reasons, "control_drop")
					}
					if slowPeerCount > 0 {
						reasons = append(reasons, "slow_peer")
					}
					if maxBufferedBytes >= relayAnomalyBufferTrigger {
						reasons = append(reasons, "buffer_pressure")
					}
					s.Logger.Warn("stream_relay_anomaly", logging.D{
						"session":                  sess.ID(),
						"wall_ms":                  now.UnixMilli(),
						"session_age_ms":           now.Sub(sess.CreatedAt()).Milliseconds(),
						"members":                  len(peers),
						"anomaly_reasons":          strings.Join(reasons, ","),
						"media_in_mps":             commonFields["media_in_mps"],
						"media_fwd_mps":            commonFields["media_fwd_mps"],
						"media_drop_mps":           commonFields["media_drop_mps"],
						"media_drop_pct":           commonFields["media_drop_pct"],
						"control_in_mps":           commonFields["control_in_mps"],
						"control_fwd_mps":          commonFields["control_fwd_mps"],
						"control_drop_mps":         commonFields["control_drop_mps"],
						"keyframe_like_mps":        commonFields["keyframe_like_mps"],
						"max_msg_bytes":            commonFields["max_msg_bytes"],
						"max_buffered_bytes":       maxBufferedBytes,
						"buffer_trigger_bytes":     relayAnomalyBufferTrigger,
						"peers_over_80_pct":        slowPeerCount,
						"peer_backpressure":        peerBuffers,
						"size_bucket_delta":        commonFields["size_bucket_delta"],
						"relay_totals":             commonFields["relay_totals"],
						"rtp_in_pps":               commonFields["rtp_in_pps"],
						"rtp_in_mbps":              commonFields["rtp_in_mbps"],
						"rtp_fanout_attempt_pps":   commonFields["rtp_fanout_attempt_pps"],
						"rtp_forwarded_pps":        commonFields["rtp_forwarded_pps"],
						"rtp_forwarded_mbps":       commonFields["rtp_forwarded_mbps"],
						"rtp_drop_pps":             commonFields["rtp_drop_pps"],
						"rtp_drop_mbps":            commonFields["rtp_drop_mbps"],
						"rtp_fanout_drop_pct":      commonFields["rtp_fanout_drop_pct"],
						"rtp_queue_drop_pps":       commonFields["rtp_queue_drop_pps"],
						"rtp_queue_drop_mbps":      commonFields["rtp_queue_drop_mbps"],
						"rtp_duplicate_pps":        commonFields["rtp_duplicate_pps"],
						"rtp_totals":               commonFields["rtp_totals"],
						"sample_interval_seconds":  elapsedSec,
						"sample_tick_interval_sec": relayStatsSampleInterval.Seconds(),
					})
				}
			}
			for sessionID := range relayState {
				if _, ok := seen[sessionID]; !ok {
					delete(relayState, sessionID)
				}
			}
			for sessionID := range rtpRxState {
				if _, ok := seen[sessionID]; !ok {
					delete(rtpRxState, sessionID)
				}
			}
		case <-stop:
			return
		}
	}
}

func (s *Server) Shutdown() {
	close(s.stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if s.signalingServer != nil {
		s.signalingServer.Shutdown(ctx)
	}
	if s.adminServer != nil {
		s.adminServer.Shutdown(ctx)
	}

	s.Logger.Info("server_stopped", logging.D{
		"uptime_seconds":  s.metrics.UptimeSeconds(),
		"sessions_served": s.metrics.SessionsCreated.Load(),
		"peers_served":    s.metrics.PeersConnected.Load(),
	})

	if s.media != nil {
		s.media.close()
		s.Logger.SetSink(nil)
		s.media.store.Close()
	}
}

// newSFUMediaEngine registers the codecs that the SFU negotiates: Opus at PT
// 111 and H.264 at PT 96. It does not register ULPFEC: mello streaming-quality
// stage 4 removed the send-side FEC, so without ULPFEC in the answer a host
// sends no parity (01-SFU.md §6.1).
//
// With ingressRTX it also registers RFC 4588 RTX at PT 97 for PT 96 (the
// host and voice PeerConnections). A host that offers RTX then sends its
// repairs and goog_cc padding on its own RTX SSRC; Pion unwraps a repair
// into the original packet on the host track and drops padding-only RTX
// packets. The viewer PeerConnections do not register it: the SFU repairs
// a viewer from its own NACK cache on the viewer SSRC (§6.1).
func newSFUMediaEngine(ingressRTX bool) (*webrtc.MediaEngine, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register Opus codec: %w", err)
	}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			ClockRate:   90000,
			SDPFmtpLine: "profile-level-id=4d0033;packetization-mode=1;level-asymmetry-allowed=1",
			RTCPFeedback: []webrtc.RTCPFeedback{
				{Type: "nack"},
				{Type: "nack", Parameter: "pli"},
				{Type: "goog-remb"},
				{Type: "transport-cc"},
			},
		},
		PayloadType: 96,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("register H.264 codec: %w", err)
	}
	if ingressRTX {
		if err := me.RegisterCodec(webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeRTX,
				ClockRate:   90000,
				SDPFmtpLine: "apt=96",
			},
			PayloadType: 97,
		}, webrtc.RTPCodecTypeVideo); err != nil {
			return nil, fmt.Errorf("register RTX codec: %w", err)
		}
	}
	return me, nil
}

// ICE timeouts of every SFU PeerConnection (01-SFU.md §9.2). Pion reports
// Disconnected 3 s after the last packet on the selected pair and Failed
// 5 s after that (8 s after the last packet). With Pion's defaults (5 s,
// then 25 s) the 5 s Disconnected removal timer removed the peer long
// before Failed, so the ICE restart on Failed never ran.
const (
	iceDisconnectedTimeout     = 3 * time.Second
	iceFailedAfterDisconnected = 5 * time.Second
	iceKeepaliveInterval       = 2 * time.Second // Pion's default
	// iceRestartTimeout is how long an ICE restart has to reach Connected.
	iceRestartTimeout = 10 * time.Second
	// iceDisconnectedRemoveAfter removes a Disconnected peer only if Pion
	// never reports Failed or Connected: Failed comes first and starts the
	// restart.
	iceDisconnectedRemoveAfter = iceFailedAfterDisconnected + iceRestartTimeout
)

// newSFUAPI builds the WebRTC API of the SFU: its codecs, its ICE timeouts
// and its interceptor chain (NACK, TWCC, RTCP reports, stats).
func newSFUAPI(se webrtc.SettingEngine) (*webrtc.API, error) {
	return buildSFUAPI(se, nil, true)
}

// newSFUViewerAPI is newSFUAPI with a per-viewer gcc estimator in the
// interceptor chain (configureViewerBWE). Stream viewer PeerConnections use
// it; host and voice PeerConnections do not, so their sends stay as they
// are.
func newSFUViewerAPI(se webrtc.SettingEngine, estimators *viewerBWERegistry) (*webrtc.API, error) {
	return buildSFUAPI(se, estimators, false)
}

func buildSFUAPI(se webrtc.SettingEngine, estimators *viewerBWERegistry, ingressRTX bool) (*webrtc.API, error) {
	se.SetICETimeouts(iceDisconnectedTimeout, iceFailedAfterDisconnected, iceKeepaliveInterval)
	me, err := newSFUMediaEngine(ingressRTX)
	if err != nil {
		return nil, err
	}

	// TWCC header extension + feedback generation for hop-terminated congestion
	// control (host ingress TWCC, viewer egress TWCC).
	interceptorRegistry := &interceptor.Registry{}
	if estimators != nil {
		// First: the TWCC header extension interceptor below wraps it.
		if err := configureViewerBWE(interceptorRegistry, estimators); err != nil {
			return nil, fmt.Errorf("configure viewer estimator: %w", err)
		}
	}
	if err := configureNACK(me, interceptorRegistry); err != nil {
		return nil, fmt.Errorf("configure NACK interceptors: %w", err)
	}
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(me, interceptorRegistry); err != nil {
		return nil, fmt.Errorf("configure TWCC header extension: %w", err)
	}
	if err := webrtc.ConfigureTWCCSender(me, interceptorRegistry); err != nil {
		return nil, fmt.Errorf("configure TWCC sender: %w", err)
	}
	if err := webrtc.ConfigureRTCPReports(interceptorRegistry); err != nil {
		return nil, fmt.Errorf("configure RTCP report interceptors: %w", err)
	}
	if err := configureStatsInterceptor(interceptorRegistry); err != nil {
		return nil, fmt.Errorf("configure stats interceptor: %w", err)
	}

	return webrtc.NewAPI(
		webrtc.WithSettingEngine(se),
		webrtc.WithMediaEngine(me),
		webrtc.WithInterceptorRegistry(interceptorRegistry),
	), nil
}

// noUIText answers /admin and /: the SFU serves no web UI, only the JSON
// admin API (01-SFU.md section 8.2).
const noUIText = "This SFU serves no web UI. The admin API is at /admin/api/ (JSON, HTTP Basic Auth).\n"

// adminMux is the admin server's routes: /health, /stats, the admin API and
// pprof. Every route but /health and /stats is behind the admin auth.
func (s *Server) adminMux() *http.ServeMux {
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/health", s.handleHealth)
	// Mission control stats: bearer-token auth (SFU_STATS_TOKEN), independent of
	// the admin Basic-auth wrapper below.
	adminMux.Handle("/stats", s.statsAuth(http.HandlerFunc(s.handleStats)))
	adminMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/", "/admin", "/admin/":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(noUIText))
		default:
			http.NotFound(w, r)
		}
	})

	adminAuth := NewAdminAuth(s.config.AdminPassword, s.config.AdminIPs, s.config.DevMode, s.clientIPs, s.authFails)
	adminMux.Handle("/admin/api/overview", adminAuth.Wrap(http.HandlerFunc(s.handleAdminOverview)))
	adminMux.Handle("/admin/api/sessions", adminAuth.Wrap(http.HandlerFunc(s.handleAdminSessions)))
	adminMux.Handle("/admin/api/session/", adminAuth.Wrap(http.HandlerFunc(s.handleAdminSessionDetail)))
	adminMux.Handle("/admin/api/logs", adminAuth.Wrap(http.HandlerFunc(s.handleAdminLogs)))
	adminMux.Handle("/admin/api/peers", adminAuth.Wrap(http.HandlerFunc(s.handleAdminPeers)))
	adminMux.Handle("/admin/api/troubleshoot", adminAuth.Wrap(http.HandlerFunc(s.handleAdminTroubleshoot)))
	adminMux.Handle("/admin/api/user/", adminAuth.Wrap(http.HandlerFunc(s.handleAdminUser)))
	adminMux.Handle("/admin/api/alerts", adminAuth.Wrap(http.HandlerFunc(s.handleAdminAlerts)))
	adminMux.Handle("/admin/api/minutes", adminAuth.Wrap(http.HandlerFunc(s.handleAdminMinutes)))

	adminMux.Handle("/debug/pprof/", adminAuth.Wrap(http.HandlerFunc(pprof.Index)))
	adminMux.Handle("/debug/pprof/cmdline", adminAuth.Wrap(http.HandlerFunc(pprof.Cmdline)))
	adminMux.Handle("/debug/pprof/profile", adminAuth.Wrap(http.HandlerFunc(pprof.Profile)))
	adminMux.Handle("/debug/pprof/symbol", adminAuth.Wrap(http.HandlerFunc(pprof.Symbol)))
	adminMux.Handle("/debug/pprof/trace", adminAuth.Wrap(http.HandlerFunc(pprof.Trace)))
	return adminMux
}

// Both HTTP servers face the internet. A connection that does not finish
// its TLS handshake and headers in httpReadHeaderTimeout is closed, so slow
// clients cannot hold connections open. No read or write timeout: a
// WebSocket lives long after its upgrade, and a pprof profile takes 30 s.
const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 2 * time.Minute
)

// errNoAuth is returned by New for a config that can authenticate no client.
var errNoAuth = errors.New("no client authentication configured: set SFU_JWT_PUB_KEY " +
	"(the backend's public key), SFU_TEST_PASSWORD (test-mode joins only) " +
	"or SFU_DEV_MODE=true (local development: accepts any token)")

// newVerifier picks the client authentication (01-SFU.md section 7.1). The
// SFU fails closed: without a key it accepts no JWT unless SFU_DEV_MODE is
// set, and with nothing to authenticate anyone it does not start.
func newVerifier(cfg Config, logger *logging.Logger) (*auth.JWTVerifier, error) {
	switch {
	case cfg.JWTPubKeyPath != "":
		v, err := auth.NewJWTVerifier(cfg.JWTPubKeyPath)
		if err != nil {
			return nil, fmt.Errorf("init JWT verifier: %w", err)
		}
		if cfg.DevMode {
			logger.Warn("dev_mode_ignored_for_jwt", logging.D{"reason": "SFU_JWT_PUB_KEY is set, tokens are verified"})
		}
		return v, nil
	case cfg.DevMode:
		logger.Warn("dev_mode_auth_open", logging.D{"reason": "SFU_DEV_MODE without SFU_JWT_PUB_KEY: any token is accepted. Local development only."})
		return auth.NewDevJWTVerifier(), nil
	case cfg.TestPassword != "":
		logger.Warn("jwt_auth_off", logging.D{"reason": "no SFU_JWT_PUB_KEY: only test-mode joins are accepted"})
		return auth.NewJWTVerifier("")
	default:
		return nil, errNoAuth
	}
}

// adminAPIState names how the admin routes answer, for the start log.
func adminAPIState(cfg Config) string {
	switch {
	case cfg.AdminPassword != "":
		return "password"
	case cfg.DevMode:
		return "open (SFU_DEV_MODE)"
	default:
		return "off (no SFU_ADMIN_PASSWORD)"
	}
}
