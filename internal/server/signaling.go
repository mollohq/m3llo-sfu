package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mollohq/m3llo-sfu/internal/auth"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

var testPeerCounter atomic.Int64

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// maxSignalingMessage caps one inbound signaling message. The largest real
// one is a logs_bundle chunk: 192 KiB of log, base64 in JSON. Without the
// cap a client could make the SFU buffer a message of any size.
const maxSignalingMessage = 512 << 10

// maxLoggedClientString caps a client-supplied string in a log entry.
const maxLoggedClientString = 64

type SignalingMessage struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Data json.RawMessage `json:"data"`
}

// Client -> Server
type JoinStreamData struct {
	SessionID string `json:"session_id"`
	Role      string `json:"role"`
}

type JoinVoiceData struct {
	CrewID    string `json:"crew_id"`
	ChannelID string `json:"channel_id"`
}

type SDPData struct {
	SDP string `json:"sdp"`
}

type ICECandidateData struct {
	Candidate     string `json:"candidate"`
	SDPMid        string `json:"sdp_mid"`
	SDPMLineIndex int    `json:"sdp_mline_index"`
}

// Server -> Client
type WelcomeData struct {
	ServerID string `json:"server_id"`
	Region   string `json:"region"`
}

type JoinedData struct {
	SessionType string       `json:"session_type"`
	SessionID   string       `json:"session_id"`
	Members     []MemberInfo `json:"members"`
}

type MemberInfo struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"`
}

type MemberEventData struct {
	UserID string `json:"user_id"`
	Role   string `json:"role,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// HostReconnectingData is the data of host_reconnecting: the host left and
// the session waits HoldMS for a host to join again (01-SFU.md §4.1).
type HostReconnectingData struct {
	UserID string `json:"user_id"`
	Reason string `json:"reason"`
	HoldMS int64  `json:"hold_ms"`
}

type ErrorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type LeaveAckData struct {
	Reason string `json:"reason"`
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Test mode: ?test_password=<SFU_TEST_PASSWORD> bypasses JWT, uses synthetic claims
	if testPwd := r.URL.Query().Get("test_password"); testPwd != "" {
		s.handleTestWebSocket(w, r)
		return
	}

	token := r.URL.Query().Get("token")
	claims, err := s.auth.Verify(token)
	if err != nil {
		// Anyone can fail auth, so the log entries are sampled: a flood of
		// bad tokens must not fill the disk or push real entries out.
		if suppressed, ok := s.authFailLogs.allow(time.Now()); ok {
			s.Logger.Warn("auth_failed", logging.D{
				"remote_ip":  r.RemoteAddr,
				"error":      truncate(err.Error(), 200),
				"suppressed": suppressed,
			})
		}
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Logger.Error("ws_upgrade_failed", logging.D{"error": err.Error()})
		return
	}
	conn.SetReadLimit(maxSignalingMessage)

	peer := NewPeer(claims.UserID, claims.Role, conn, s.Logger)
	peer.Username = claims.Username

	s.Logger.Info("ws_connected", logging.D{
		"user":      claims.UserID,
		"username":  claims.Username,
		"remote_ip": r.RemoteAddr,
	})
	s.metrics.PeersConnected.Add(1)
	s.metrics.ActivePeers.Add(1)

	peer.sendMessage(SignalingMessage{
		Type: "welcome",
		Seq:  1,
		Data: mustMarshal(WelcomeData{
			ServerID: s.config.ServerID,
			Region:   s.config.Region,
		}),
	})

	go s.handleSignalingLoop(peer, claims)
}

func (s *Server) handleTestWebSocket(w http.ResponseWriter, r *http.Request) {
	password := r.URL.Query().Get("test_password")
	if s.config.TestPassword == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	clientIP := s.clientIPs.Of(r)
	if s.authFails.Blocked(clientIP) {
		http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
		return
	}
	if subtle.ConstantTimeCompare([]byte(password), []byte(s.config.TestPassword)) != 1 {
		s.authFails.Fail(clientIP)
		s.Logger.Warn("test_auth_failed", logging.D{"remote_ip": clientIP})
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	s.authFails.Succeed(clientIP)

	testType := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("test_type")))
	if testType == "" {
		testType = "voice"
	}
	if testType != "voice" && testType != "stream" {
		http.Error(w, "Invalid test_type", http.StatusBadRequest)
		return
	}
	testCrewID := strings.TrimSpace(r.URL.Query().Get("test_crew_id"))
	testRole := "member"
	if testType == "stream" {
		// Let join_stream choose host/viewer role explicitly.
		testRole = ""
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.Logger.Error("ws_test_upgrade_failed", logging.D{"error": err.Error()})
		return
	}
	conn.SetReadLimit(maxSignalingMessage)

	n := testPeerCounter.Add(1)
	// Optional stable identity overrides so harnesses can exercise the
	// same-user reconnect / eviction path (otherwise every WS is a fresh user).
	testUserID := strings.TrimSpace(r.URL.Query().Get("test_user_id"))
	if testUserID == "" {
		testUserID = fmt.Sprintf("test-browser-%d", n)
	}
	testUsername := strings.TrimSpace(r.URL.Query().Get("test_username"))
	if testUsername == "" {
		testUsername = fmt.Sprintf("Browser Test %d", n)
	}
	claims := &auth.Claims{
		UserID:   testUserID,
		Username: testUsername,
		Type:     testType,
		Role:     testRole,
		CrewID:   testCrewID,
	}

	peer := NewPeer(claims.UserID, claims.Role, conn, s.Logger)
	peer.Username = claims.Username
	peer.Synthetic = true

	s.Logger.Info("ws_connected", logging.D{
		"user":      claims.UserID,
		"username":  claims.Username,
		"remote_ip": r.RemoteAddr,
		"test_mode": true,
		"test_type": testType,
	})
	s.metrics.PeersConnected.Add(1)
	s.metrics.ActivePeers.Add(1)
	s.metrics.ActiveSyntheticPeers.Add(1)

	peer.sendMessage(SignalingMessage{
		Type: "welcome",
		Seq:  1,
		Data: mustMarshal(WelcomeData{
			ServerID: s.config.ServerID,
			Region:   s.config.Region,
		}),
	})

	go s.handleSignalingLoop(peer, claims)
}

func (s *Server) handleSignalingLoop(peer *Peer, claims *auth.Claims) {
	defer func() {
		s.cleanupPeer(peer)
		s.metrics.ActivePeers.Add(-1)
		if peer.Synthetic {
			s.metrics.ActiveSyntheticPeers.Add(-1)
		}
	}()

	for {
		_, raw, err := peer.WS.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				peer.cleanLeave.Store(true)
			}
			s.Logger.Info("ws_disconnected", logging.D{
				"user":  claims.UserID,
				"error": err.Error(),
			})
			return
		}

		var msg SignalingMessage
		if err := json.Unmarshal(raw, &msg); err != nil {
			s.Logger.Warn("ws_invalid_message", logging.D{
				"user":  claims.UserID,
				"error": err.Error(),
			})
			continue
		}

		switch msg.Type {
		case "join_stream", "join_voice":
			// One session per socket. A second join would leave the peer
			// in the first session, which then never drains.
			if sess, _ := peer.currentSession(); sess != nil {
				peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Already joined a session"})
				continue
			}
			if msg.Type == "join_stream" {
				s.handleJoinStream(peer, claims, msg)
			} else {
				s.handleJoinVoice(peer, claims, msg)
			}
		case "offer":
			s.handleOffer(peer, claims, msg)
		case "answer":
			s.handleAnswer(peer, claims, msg)
		case "ice_candidate":
			s.handleICECandidate(peer, claims, msg)
		case "client_stats":
			s.handleClientStats(peer, claims, msg)
		case "stream_client_stats":
			s.handleStreamClientStats(peer, claims, msg)
		case "logs_bundle":
			// The client's answer to request_logs (01-SFU.md section 8.8).
			s.handleLogsBundle(peer, claims.UserID, msg.Data)
		case "leave":
			s.handleLeave(peer, claims, msg)
			return
		default:
			s.Logger.Warn("ws_unknown_message", logging.D{
				"user": claims.UserID,
				"type": truncate(msg.Type, maxLoggedClientString),
			})
		}
	}
}

func (s *Server) handleJoinStream(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	var data JoinStreamData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Invalid join_stream data"})
		return
	}

	if claims.Type != "stream" {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_TOKEN", Message: "Token type mismatch"})
		return
	}
	if claims.SessionID != "" && claims.SessionID != data.SessionID {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_TOKEN", Message: "Session ID mismatch"})
		return
	}
	if claims.Role != "" && claims.Role != data.Role {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_ROLE", Message: "Token role mismatch"})
		return
	}

	peer.Role = data.Role

	sess, exists := s.sessions.Get(data.SessionID)
	if !exists {
		if !s.canCreateSession() {
			s.Logger.Warn("session_reject", logging.D{
				"session": data.SessionID,
				"user":    claims.UserID,
				"code":    "SERVER_AT_CAPACITY",
				"type":    "stream",
			})
			peer.SendSignaling("error", ErrorData{
				Code:    "SERVER_AT_CAPACITY",
				Message: "Server session capacity reached",
			})
			return
		}
		sess = s.sessions.GetOrCreate(data.SessionID, func() Session {
			s.metrics.SessionsCreated.Add(1)
			s.metrics.ActiveSessions.Add(1)
			return NewStreamSession(data.SessionID, claims.CrewID, s.Logger, streamSnapshotConfig{
				enabled:  s.snapshotEnabled,
				interval: s.snapshotInterval,
				r2:       s.snapshotR2,
				metrics:  s.metrics,
			})
		})
	}
	if s.sessionAtCapacity(sess, claims.UserID) {
		s.Logger.Warn("session_reject", logging.D{
			"session": data.SessionID,
			"user":    claims.UserID,
			"code":    "SESSION_FULL",
			"type":    "stream",
		})
		peer.SendSignaling("error", ErrorData{
			Code:    "SESSION_FULL",
			Message: "Session at capacity",
		})
		return
	}

	streamSess, ok := sess.(*StreamSession)
	if !ok {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Not a stream session"})
		return
	}
	// The member snapshot and the join are one step (01-SFU.md §4.1).
	members, err := streamSess.Join(peer)
	if errors.Is(err, ErrSessionClosed) {
		// The drain loop closed this session after we looked it up. It
		// is gone from the store: join a new one under the same id, as
		// handleJoinVoice does. The drain removes a session from the store
		// when it closes it; remove this one if it is still there.
		s.sessions.RemoveIf(data.SessionID, sess)
		sess = s.sessions.GetOrCreate(data.SessionID, func() Session {
			s.metrics.SessionsCreated.Add(1)
			s.metrics.ActiveSessions.Add(1)
			return NewStreamSession(data.SessionID, claims.CrewID, s.Logger, streamSnapshotConfig{
				enabled:  s.snapshotEnabled,
				interval: s.snapshotInterval,
				r2:       s.snapshotR2,
				metrics:  s.metrics,
			})
		})
		if streamSess, ok = sess.(*StreamSession); ok {
			members, err = streamSess.Join(peer)
		}
	}
	if err != nil {
		code := streamJoinErrorCode(err)
		s.Logger.Warn("session_reject", logging.D{
			"session": data.SessionID,
			"user":    claims.UserID,
			"code":    code,
			"type":    "stream",
			"error":   err.Error(),
		})
		peer.SendSignaling("error", ErrorData{Code: code, Message: err.Error()})
		return
	}

	peer.SendSignaling("joined", JoinedData{
		SessionType: "stream",
		SessionID:   data.SessionID,
		Members:     members,
	})

	s.Logger.Info("join_stream", logging.D{
		"session": data.SessionID,
		"user":    claims.UserID,
		"role":    data.Role,
	})
}

func (s *Server) handleJoinVoice(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	var data JoinVoiceData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Invalid join_voice data"})
		return
	}

	if claims.Type != "voice" {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_TOKEN", Message: "Token type mismatch"})
		return
	}
	if claims.CrewID != "" && claims.CrewID != data.CrewID {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_TOKEN", Message: "Crew ID mismatch"})
		return
	}
	if claims.ChannelID != "" && claims.ChannelID != data.ChannelID {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_TOKEN", Message: "Channel ID mismatch"})
		return
	}

	peer.Role = "member"
	sessionID := fmt.Sprintf("voice:%s:%s", data.CrewID, data.ChannelID)

	sess, exists := s.sessions.Get(sessionID)
	if !exists {
		if !s.canCreateSession() {
			s.Logger.Warn("session_reject", logging.D{
				"session": sessionID,
				"user":    claims.UserID,
				"code":    "SERVER_AT_CAPACITY",
				"type":    "voice",
			})
			peer.SendSignaling("error", ErrorData{
				Code:    "SERVER_AT_CAPACITY",
				Message: "Server session capacity reached",
			})
			return
		}
		sess = s.sessions.GetOrCreate(sessionID, func() Session {
			s.metrics.SessionsCreated.Add(1)
			s.metrics.ActiveSessions.Add(1)
			return NewVoiceSession(sessionID, data.CrewID, data.ChannelID, s.Logger)
		})
	}
	if s.sessionAtCapacity(sess, claims.UserID) {
		s.Logger.Warn("session_reject", logging.D{
			"session": sessionID,
			"user":    claims.UserID,
			"code":    "SESSION_FULL",
			"type":    "voice",
		})
		peer.SendSignaling("error", ErrorData{
			Code:    "SESSION_FULL",
			Message: "Session at capacity",
		})
		return
	}

	voiceSess, ok := sess.(*VoiceSession)
	if !ok {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Not a voice session"})
		return
	}
	// The member snapshot and the join are one step (01-SFU.md §4.2).
	members, err := voiceSess.Join(peer)
	if errors.Is(err, ErrSessionClosed) {
		// The drain loop closed this session after we looked it up. It
		// is gone from the store: join a new one under the same id.
		sess = s.sessions.GetOrCreate(sessionID, func() Session {
			s.metrics.SessionsCreated.Add(1)
			s.metrics.ActiveSessions.Add(1)
			return NewVoiceSession(sessionID, data.CrewID, data.ChannelID, s.Logger)
		})
		if voiceSess, ok = sess.(*VoiceSession); ok {
			members, err = voiceSess.Join(peer)
		}
	}
	if err != nil {
		s.Logger.Warn("session_reject", logging.D{
			"session": sessionID,
			"user":    claims.UserID,
			"code":    "SESSION_FULL",
			"type":    "voice",
			"error":   err.Error(),
		})
		peer.SendSignaling("error", ErrorData{Code: "SESSION_FULL", Message: err.Error()})
		return
	}
	peer.bindSession(sess)

	peer.SendSignaling("joined", JoinedData{
		SessionType: "voice",
		SessionID:   sessionID,
		Members:     members,
	})

	s.Logger.Info("join_voice", logging.D{
		"session": sessionID,
		"user":    claims.UserID,
	})
}

func (s *Server) handleOffer(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	var data SDPData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Invalid offer data"})
		return
	}

	session, sessionGeneration := peer.currentSession()
	if session == nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Must join a session before sending offer"})
		return
	}

	// Existing PeerConnection: treat as client-initiated renegotiation/ICE restart.
	if existingPC, generation := peer.currentPeerConnection(); existingPC != nil {
		if existingPC.ConnectionState() == webrtc.PeerConnectionStateClosed {
			peer.discardPeerConnection(existingPC, generation)
		} else {
			if err := s.applyOfferAndAnswerFor(peer, claims, data.SDP, existingPC, generation); err != nil {
				peer.discardPeerConnection(existingPC, generation)
				s.Logger.Error("sdp_remote_offer_failed", logging.D{
					"user":  claims.UserID,
					"error": err.Error(),
				})
				peer.SendSignaling("error", ErrorData{Code: negotiationErrorCode(err), Message: "Failed to apply offer"})
			}
			return
		}
	}

	pcConfig := webrtc.Configuration{}
	if cert := s.currentDTLSCert(); cert != nil {
		pcConfig.Certificates = []webrtc.Certificate{*cert}
	}
	api := s.webrtcAPI
	viewerBWE := session.Type() == "stream" && peer.Role == "viewer" && s.viewerAPI != nil
	if viewerBWE {
		api = s.viewerAPI
	}
	pc, err := api.NewPeerConnection(pcConfig)
	if err != nil {
		s.metrics.WebRTCFailures.Add(1)
		s.Logger.Error("webrtc_failed", logging.D{
			"user":  claims.UserID,
			"error": err.Error(),
		})
		peer.SendSignaling("error", ErrorData{Code: "WEBRTC_ERROR", Message: "Failed to create peer connection"})
		return
	}
	s.metrics.WebRTCConnections.Add(1)
	var estimator *viewerEstimator
	if viewerBWE {
		estimator = s.viewerBWE.take(pc.ID())
	}
	generation := peer.installPeerConnectionWithEstimator(pc, estimator)
	if !peer.isCurrentSession(session, sessionGeneration) {
		peer.discardPeerConnection(pc, generation)
		return
	}

	isVoice := session.Type() == "voice"

	// RTP track handler for voice sessions
	if isVoice {
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
				return
			}
			s.Logger.Info("on_track", logging.D{
				"session": session.ID(),
				"user":    claims.UserID,
				"codec":   track.Codec().MimeType,
				"ssrc":    track.SSRC(),
			})
			if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
				return
			}
			session.WireAudioTrack(peer, track)
		})
	} else if peer.Role == "host" {
		streamSession, ok := session.(*StreamSession)
		if ok {
			pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
				if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
					return
				}
				routeStreamHostTrack(
					s.Logger,
					streamSession,
					peer,
					track,
					pc,
					generation,
				)
			})
		}
	}

	// DataChannel handler -- voice only uses "control"; stream uses both
	if isVoice {
		voiceSession, _ := session.(*VoiceSession)
		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
				return
			}
			if dc.Label() != "control" {
				s.rejectDataChannel(peer, dc)
				return
			}
			if dc.Label() == "control" {
				dc.OnOpen(func() {
					if !peer.setDataChannelForSession(
						pc, generation, session, sessionGeneration, dc, false,
					) {
						return
					}
					// Slot and speaking messages sent before the channel
					// opened were dropped: send the current state.
					if voiceSession != nil {
						voiceSession.SendControlSnapshot(peer)
					}
				})
				dc.OnMessage(func(msg webrtc.DataChannelMessage) {
					if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
						return
					}
					if voiceSession != nil && len(msg.Data) < 256 {
						if on, ok := parseSpeakingControl(msg.Data); ok {
							voiceSession.SetSpeaking(peer, on)
							return
						}
					}
					if msg.IsString && len(msg.Data) < 256 {
						if len(msg.Data) > 14 && string(msg.Data[:14]) == `{"type":"ping"` {
							pong := make([]byte, len(msg.Data))
							copy(pong, msg.Data)
							pong[10] = 'o' // "ping" -> "pong" (index 10 is the 'i')
							// Keep pong on the TEXT path: the client RTT parser consumes
							// string messages and treats []byte as binary.
							if err := dc.SendText(string(pong)); err != nil {
								s.Logger.Warn("voice_pong_send_failed", logging.D{
									"session": peer.sessionID(),
									"user":    claims.UserID,
									"error":   err.Error(),
								})
							}
						}
					}
				})
			}
		})
	} else {
		streamSession, _ := session.(*StreamSession)
		wireControlChannel := func() {
			if streamSession == nil {
				return
			}
			peer.wireControlChannelForSession(
				pc,
				generation,
				session,
				sessionGeneration,
				func(control *webrtc.DataChannel) {
					streamSession.wireControlChannelFor(
						peer,
						pc,
						generation,
						sessionGeneration,
						control,
					)
				},
			)
		}
		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
				return
			}
			switch dc.Label() {
			case "control":
				dc.OnOpen(func() {
					if !peer.setDataChannelForSession(
						pc, generation, session, sessionGeneration, dc, false,
					) {
						return
					}
					wireControlChannel()
				})
			default:
				// Stream video and audio use RTP: a legacy "audio"
				// channel is ignored, any other label is closed.
				s.rejectDataChannel(peer, dc)
			}
		})
	}

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil ||
			!peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
			return
		}
		init := c.ToJSON()
		var sdpMid string
		if init.SDPMid != nil {
			sdpMid = *init.SDPMid
		}
		var mLineIndex int
		if init.SDPMLineIndex != nil {
			mLineIndex = int(*init.SDPMLineIndex)
		}
		if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
			return
		}
		peer.SendSignaling("ice_candidate", ICECandidateData{
			Candidate:     init.Candidate,
			SDPMid:        sdpMid,
			SDPMLineIndex: mLineIndex,
		})
	})

	// Voice sessions: defer track setup until ICE connects so we don't
	// renegotiate before the DTLS-SRTP session is established.
	var voiceSetupOnce sync.Once
	var vs *VoiceSession
	if isVoice {
		vs, _ = session.(*VoiceSession)
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
			return
		}
		sid := peer.sessionID()
		s.Logger.Debug("pc_state", logging.D{
			"session": sid,
			"user":    claims.UserID,
			"state":   state.String(),
		})

		switch state {
		case webrtc.PeerConnectionStateDisconnected:
			s.Logger.Warn("webrtc_disconnected", logging.D{
				"session": sid,
				"user":    claims.UserID,
			})
			// A backstop only: Pion reports Failed first (newSFUAPI), and
			// Failed starts the ICE restart below.
			peer.StartICEFailTimerFor(pc, generation, iceDisconnectedRemoveAfter, func() {
				if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
					return
				}
				s.Logger.Warn("peer_timeout_disconnected", logging.D{
					"session": peer.sessionID(),
					"user":    claims.UserID,
				})
				s.removePeerFromSessionForPC(peer, pc, generation)
			})

		case webrtc.PeerConnectionStateFailed:
			s.Logger.Warn("webrtc_failed", logging.D{
				"session": sid,
				"user":    claims.UserID,
			})
			if err := peer.restartICEFor(pc, generation); err != nil {
				s.Logger.Error("ice_restart_failed", logging.D{
					"session": sid,
					"user":    claims.UserID,
					"error":   err.Error(),
				})
				s.removePeerFromSessionForPC(peer, pc, generation)
				return
			}
			peer.StartICEFailTimerFor(pc, generation, iceRestartTimeout, func() {
				if !peer.isCurrentConnectionSession(pc, generation, session, sessionGeneration) {
					return
				}
				s.Logger.Warn("peer_timeout_ice_restart", logging.D{
					"session":       peer.sessionID(),
					"user":          claims.UserID,
					"restart_count": peer.ICERestartCount.Load(),
				})
				s.removePeerFromSessionForPC(peer, pc, generation)
			})

		case webrtc.PeerConnectionStateClosed:
			s.Logger.Info("pc_closed", logging.D{
				"session": sid,
				"user":    claims.UserID,
			})
			s.removePeerFromSessionForPC(peer, pc, generation)

		case webrtc.PeerConnectionStateConnected:
			peer.CancelICEFailTimerFor(pc, generation)
			if streamSession, ok := session.(*StreamSession); ok && peer.Role == "viewer" {
				streamSession.ViewerVideoConnected(peer)
			}
			if vs != nil {
				voiceSetupOnce.Do(func() {
					added, connectedPeers, needsReneg := vs.SetupOutgoingTracks(peer)
					// Receive slots need no renegotiation. Renegotiate only
					// when a sender got its own m-line (past the slots).
					if connectedPeers > 0 && needsReneg {
						if err := peer.Renegotiate(); err != nil {
							s.Logger.Warn("renegotiate_new_peer_failed", logging.D{
								"session":         sid,
								"user":            claims.UserID,
								"error":           err.Error(),
								"added_tracks":    added,
								"connected_peers": connectedPeers,
							})
						}
					}
				})
			}
		}
	})

	if err := s.applyOfferAndAnswerFor(peer, claims, data.SDP, pc, generation); err != nil {
		peer.discardPeerConnection(pc, generation)
		s.Logger.Error("sdp_local_failed", logging.D{
			"user":  claims.UserID,
			"error": err.Error(),
		})
		peer.SendSignaling("error", ErrorData{Code: negotiationErrorCode(err), Message: "Failed to negotiate"})
		return
	}
}

// handleAnswer processes an SDP answer from the client in response to a
// server-initiated renegotiation offer.
func (s *Server) handleAnswer(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	var data SDPData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Invalid answer data"})
		return
	}

	pc, generation := peer.currentPeerConnection()
	if pc == nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "No peer connection established"})
		return
	}
	if s.applyRenegotiationAnswer(peer, claims, pc, generation, data.SDP) {
		// A renegotiation collided with the offer this answer closes.
		peer.runQueuedRenegotiation("answer_applied")
	}
}

// applyRenegotiationAnswer applies the client's answer under renegMu and
// reports whether a queued renegotiation must run now.
func (s *Server) applyRenegotiationAnswer(
	peer *Peer,
	claims *auth.Claims,
	pc *webrtc.PeerConnection,
	generation uint64,
	sdp string,
) bool {
	peer.renegMu.Lock()
	defer peer.renegMu.Unlock()
	if !peer.isCurrentPeerConnection(pc, generation) {
		return false
	}

	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	}

	sid := ""
	if session, _ := peer.currentSession(); session != nil {
		sid = session.ID()
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		s.Logger.Error("sdp_answer_apply_failed", logging.D{
			"session": sid,
			"user":    claims.UserID,
			"error":   err.Error(),
		})
		return false
	}
	// Only clear after the answer is successfully applied.
	peer.finishRenegotiation()
	s.Logger.Debug("renegotiation_answer_applied", logging.D{
		"session": sid,
		"user":    claims.UserID,
	})
	return peer.takeQueuedRenegotiationLocked(pc)
}

func (s *Server) handleICECandidate(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	var data ICECandidateData
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "Invalid ICE candidate data"})
		return
	}

	pc, generation := peer.currentPeerConnection()
	if pc == nil {
		peer.SendSignaling("error", ErrorData{Code: "INVALID_REQUEST", Message: "No peer connection established"})
		return
	}

	candidateStr := strings.TrimPrefix(data.Candidate, "a=")
	mLineIndex := uint16(data.SDPMLineIndex)
	candidate := webrtc.ICECandidateInit{
		Candidate:     candidateStr,
		SDPMid:        &data.SDPMid,
		SDPMLineIndex: &mLineIndex,
	}

	if !peer.isCurrentPeerConnection(pc, generation) {
		return
	}
	if err := pc.AddICECandidate(candidate); err != nil {
		s.Logger.Warn("ice_add_failed", logging.D{
			"user":  claims.UserID,
			"error": err.Error(),
		})
	}
}

func (s *Server) handleLeave(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	// A stream host that leaves ends the stream: no session hold.
	peer.cleanLeave.Store(true)
	s.Logger.Info("peer_leave", logging.D{
		"session": peer.sessionID(),
		"user":    claims.UserID,
	})
	peer.SendSignaling("left", LeaveAckData{Reason: "client_leave"})
	peer.SendClose(websocket.CloseNormalClosure, "client_leave")
}

func (s *Server) cleanupPeer(peer *Peer) {
	s.removePeerMembership(peer)
}

// removePeerFromSession is safe to call from timer goroutines and ICE callbacks.
// Peer.Close is idempotent so double-calls from signaling loop defer are harmless.
func (s *Server) removePeerFromSession(peer *Peer) {
	s.removePeerMembership(peer)
}

func (s *Server) removePeerFromSessionForPC(
	peer *Peer,
	pc *webrtc.PeerConnection,
	generation uint64,
) {
	if peer.isCurrentPeerConnection(pc, generation) {
		s.removePeerMembership(peer)
	}
}

func (s *Server) removePeerMembership(peer *Peer) {
	sess, _ := peer.currentSession()
	if sess == nil {
		peer.Close()
		return
	}
	peer.clearSession(sess)
	sess.RemovePeerInstance(peer)
}

// Helpers

func (s *Server) canCreateSession() bool {
	if s.config.MaxSessions <= 0 {
		return true
	}
	stream, voice := s.sessions.Count()
	return stream+voice < s.config.MaxSessions
}

func sessionContainsUser(sess Session, userID string) bool {
	for _, m := range sess.Members() {
		if m.UserID == userID {
			return true
		}
	}
	return false
}

// rejectDataChannel handles a DataChannel that the client opened with a
// label other than "control". Media goes on RTP tracks, voice and stream:
// no other channel has a data path (01-SFU.md §5.3).
//
// A client before the stream-5 change opens an unreliable "audio" channel
// and waits until it is open. Unless SFU_REJECT_LEGACY_AUDIO_DC is set, the
// SFU keeps that channel open and forwards nothing on it (log
// datachannel_ignored, once per peer), so the SFU can deploy before the
// client. Any other label is closed (log datachannel_rejected).
func (s *Server) rejectDataChannel(peer *Peer, dc *webrtc.DataChannel) {
	if dc.Label() == "audio" && !s.config.RejectLegacyAudioDC {
		if peer.legacyAudioDCLogged.CompareAndSwap(false, true) {
			s.Logger.Info("datachannel_ignored", logging.D{
				"session": peer.sessionID(),
				"user":    peer.UserID,
				"label":   dc.Label(),
			})
		}
		return
	}
	s.Logger.Info("datachannel_rejected", logging.D{
		"session": peer.sessionID(),
		"user":    peer.UserID,
		"label":   dc.Label(),
	})
	_ = dc.Close()
}

func streamJoinErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrHostAlreadyConnected):
		return "HOST_ALREADY_CONNECTED"
	case errors.Is(err, ErrSessionClosed):
		return "SESSION_CLOSED"
	default:
		return "SESSION_FULL"
	}
}

func negotiationErrorCode(err error) string {
	if errors.Is(err, ErrSessionClosed) {
		return "SESSION_CLOSED"
	}
	return "WEBRTC_ERROR"
}

func routeStreamHostTrack(
	logger *logging.Logger,
	session *StreamSession,
	peer *Peer,
	track *webrtc.TrackRemote,
	pc *webrtc.PeerConnection,
	generation uint64,
) {
	logger.Info("on_track", logging.D{
		"session": session.ID(),
		"user":    peer.UserID,
		"codec":   track.Codec().MimeType,
		"ssrc":    track.SSRC(),
		"kind":    track.Kind().String(),
	})
	switch track.Kind() {
	case webrtc.RTPCodecTypeVideo:
		switch {
		case strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeH264):
			session.WireVideoTrackFor(peer, track, pc, generation)
		default:
			logger.Warn("stream_video_track_rejected", logging.D{
				"session": session.ID(),
				"user":    peer.UserID,
				"codec":   track.Codec().MimeType,
			})
		}
	case webrtc.RTPCodecTypeAudio:
		if strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeOpus) {
			session.WireAudioTrackFor(peer, track, pc, generation)
		} else {
			logger.Warn("stream_audio_track_rejected", logging.D{
				"session": session.ID(),
				"user":    peer.UserID,
				"codec":   track.Codec().MimeType,
			})
		}
	default:
		logger.Warn("stream_track_rejected", logging.D{
			"session": session.ID(),
			"user":    peer.UserID,
			"codec":   track.Codec().MimeType,
			"kind":    track.Kind().String(),
		})
	}
}

func (s *Server) sessionAtCapacity(sess Session, userID string) bool {
	if s.config.MaxPeersPerSession <= 0 {
		return false
	}
	if sessionContainsUser(sess, userID) {
		return false
	}
	return sess.MemberCount() >= s.config.MaxPeersPerSession
}

func (s *Server) applyOfferAndAnswer(peer *Peer, claims *auth.Claims, remoteSDP string) error {
	pc, generation := peer.currentPeerConnection()
	return s.applyOfferAndAnswerFor(peer, claims, remoteSDP, pc, generation)
}

func (s *Server) applyOfferAndAnswerFor(
	peer *Peer,
	claims *auth.Claims,
	remoteSDP string,
	pc *webrtc.PeerConnection,
	generation uint64,
) error {
	peer.renegMu.Lock()
	defer peer.renegMu.Unlock()

	if pc == nil || !peer.isCurrentPeerConnection(pc, generation) {
		return ErrNoPeerConnection
	}
	// Client-initiated renegotiation / ICE-restart path should get the same
	// reader-protection window as server-initiated renegotiation.
	peer.beginRenegotiation(renegotiationAnswerTimeout)
	defer peer.finishRenegotiation()

	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  remoteSDP,
	}

	if err := pc.SetRemoteDescription(offer); err != nil {
		return err
	}
	if session, _ := peer.currentSession(); session != nil && session.Type() == "voice" {
		// Bind the client's recvonly receive slots before the answer, so
		// the answer declares each slot's SSRC (01-SFU.md §6.2).
		if _, err := peer.BindVoiceSlots(s.webrtcAPI, pc); err != nil {
			return err
		}
	}
	preparation, err := prepareStreamViewerVideo(peer, pc, generation)
	if err != nil {
		return err
	}
	audioPreparation, err := prepareStreamViewerAudio(peer, pc, generation)
	if err != nil {
		if preparation != nil {
			preparation.Rollback()
		}
		return err
	}
	activated := false
	audioActivated := false
	defer func() {
		if preparation != nil && !activated {
			preparation.Rollback()
		}
		if audioPreparation != nil && !audioActivated {
			audioPreparation.Rollback()
		}
	}()

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return err
	}

	// Send answer BEFORE SetLocalDescription: with ice-lite, setting the
	// local description triggers OnICECandidate immediately, and clients
	// expect the answer to arrive before any ice_candidate messages.
	peer.SendSignaling("answer", SDPData{SDP: answer.SDP})
	sid := ""
	if session, _ := peer.currentSession(); session != nil {
		sid = session.ID()
	}
	s.Logger.Debug("sdp_answer_sent", logging.D{
		"session": sid,
		"user":    claims.UserID,
	})

	if err := pc.SetLocalDescription(answer); err != nil {
		return err
	}
	if preparation != nil {
		if !preparation.Activate() {
			return ErrSessionClosed
		}
		activated = true
	}
	if audioPreparation != nil {
		if !audioPreparation.Activate() {
			return ErrSessionClosed
		}
		audioActivated = true
	}
	return nil
}

type streamViewerVideoPreparation struct {
	peer              *Peer
	session           *StreamSession
	track             *webrtc.TrackLocalStaticRTP
	sessionGeneration uint64
	created           bool
}

func prepareStreamViewerVideo(
	peer *Peer,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) (*streamViewerVideoPreparation, error) {
	if peer == nil || peer.Role != "viewer" {
		return nil, nil
	}
	session, _ := peer.currentSession()
	streamSession, ok := session.(*StreamSession)
	if !ok {
		return nil, nil
	}

	preparation := &streamViewerVideoPreparation{
		peer:    peer,
		session: streamSession,
	}
	track, generation, created, err := streamSession.PrepareViewerVideo(peer, pc, pcGeneration)
	if err != nil {
		return nil, fmt.Errorf("attach stream viewer video: %w", err)
	}
	preparation.track = track
	preparation.sessionGeneration = generation
	preparation.created = created
	return preparation, nil
}

func (p *streamViewerVideoPreparation) Activate() bool {
	return p.session.ActivateViewerVideo(p.peer, p.track, p.sessionGeneration)
}

func (p *streamViewerVideoPreparation) Rollback() {
	if p.created {
		p.peer.closeStreamVideoTrack(p.track)
	}
}

type streamViewerAudioPreparation struct {
	peer              *Peer
	session           *StreamSession
	track             *webrtc.TrackLocalStaticRTP
	sessionGeneration uint64
	created           bool
}

func prepareStreamViewerAudio(
	peer *Peer,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) (*streamViewerAudioPreparation, error) {
	if peer == nil || peer.Role != "viewer" {
		return nil, nil
	}
	session, _ := peer.currentSession()
	streamSession, ok := session.(*StreamSession)
	if !ok {
		return nil, nil
	}

	preparation := &streamViewerAudioPreparation{
		peer:    peer,
		session: streamSession,
	}
	track, generation, created, err := streamSession.PrepareViewerAudio(peer, pc, pcGeneration)
	if err != nil {
		return nil, fmt.Errorf("attach stream viewer audio: %w", err)
	}
	preparation.track = track
	preparation.sessionGeneration = generation
	preparation.created = created
	return preparation, nil
}

func (p *streamViewerAudioPreparation) Activate() bool {
	return p.session.ActivateViewerAudio(p.peer, p.track, p.sessionGeneration)
}

func (p *streamViewerAudioPreparation) Rollback() {
	if p.created {
		p.peer.closeStreamAudioTrack(p.track)
	}
}

const clientStatsMinInterval = 5 * time.Second

// Raised from 1024 to fit the stream host/viewer payloads, which carry encoder,
// capture, decode-cadence and freeze fields on top of the voice set.
const clientStatsMaxBytes = 2048

func (s *Server) handleClientStats(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	now := time.Now()
	if now.Sub(peer.lastClientStats) < clientStatsMinInterval {
		return
	}
	peer.lastClientStats = now

	if len(msg.Data) > clientStatsMaxBytes {
		return
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(msg.Data, &stats); err != nil {
		return
	}

	sid := ""
	if session, _ := peer.currentSession(); session != nil {
		sid = session.ID()
	}
	s.Logger.Info("client_stats", logging.D{
		"session": sid,
		"user":    claims.UserID,
		"stats":   stats,
	})
	if s.media != nil {
		s.media.onStats(sid, peer, "voice", stats)
	}
}

// handleStreamClientStats records host/viewer telemetry for stream sessions.
//
// Kept separate from client_stats rather than folded into it: the payloads share
// no fields, and stream sessions are the ones we cannot debug from the user's
// machine. A distinct event name keeps them greppable in log exports.
func (s *Server) handleStreamClientStats(peer *Peer, claims *auth.Claims, msg SignalingMessage) {
	now := time.Now()
	if now.Sub(peer.lastClientStats) < clientStatsMinInterval {
		return
	}
	peer.lastClientStats = now

	if len(msg.Data) > clientStatsMaxBytes {
		s.Logger.Warn("stream_client_stats_oversized", logging.D{
			"user":  claims.UserID,
			"bytes": len(msg.Data),
		})
		return
	}

	var stats map[string]interface{}
	if err := json.Unmarshal(msg.Data, &stats); err != nil {
		return
	}

	sid := ""
	if session, _ := peer.currentSession(); session != nil {
		sid = session.ID()
	}
	s.Logger.Info("stream_client_stats", logging.D{
		"session": sid,
		"user":    claims.UserID,
		"role":    peer.Role,
		"stats":   stats,
	})
	if s.media != nil {
		s.media.onStats(sid, peer, "stream", stats)
	}
}

func mustMarshal(v interface{}) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}
