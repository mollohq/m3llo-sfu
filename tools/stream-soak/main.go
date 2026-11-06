package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

type signalMsg struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Data json.RawMessage `json:"data"`
}

type joinStreamData struct {
	SessionID string `json:"session_id"`
	Role      string `json:"role"`
}

var errRecvTimeout = errors.New("recv_timeout")

type wsReadResult struct {
	msg *signalMsg
	err error
}

type wsReader struct {
	ch chan wsReadResult
}

func newWSReader(conn *websocket.Conn) *wsReader {
	r := &wsReader{ch: make(chan wsReadResult, 1)}
	go func() {
		defer close(r.ch)
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				r.ch <- wsReadResult{err: err}
				return
			}
			var msg signalMsg
			if err := json.Unmarshal(b, &msg); err != nil {
				r.ch <- wsReadResult{err: err}
				continue
			}
			r.ch <- wsReadResult{msg: &msg}
		}
	}()
	return r
}

func (r *wsReader) recv(timeout time.Duration) (*signalMsg, error) {
	if timeout <= 0 {
		res, ok := <-r.ch
		if !ok {
			return nil, io.EOF
		}
		return res.msg, res.err
	}
	select {
	case res, ok := <-r.ch:
		if !ok {
			return nil, io.EOF
		}
		return res.msg, res.err
	case <-time.After(timeout):
		return nil, errRecvTimeout
	}
}

func endpointWithPassword(endpoint, password string) (string, error) {
	return endpointWithTestIdentity(endpoint, password, "", "")
}

func endpointWithTestIdentity(endpoint, password, userID, username string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if password != "" {
		q.Set("test_password", password)
		q.Set("test_type", "stream")
	}
	if userID != "" {
		q.Set("test_user_id", userID)
	}
	if username != "" {
		q.Set("test_username", username)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func mustSend(conn *websocket.Conn, mu *sync.Mutex, payload any) error {
	mu.Lock()
	defer mu.Unlock()
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}

type hostTxMetrics struct {
	frames    int64
	keyframes int64
	attempted int64
	sent      int64
	errors    int64
	bytes     int64
	// simDropped counts packets --sim-drop-pct kept off the wire. They use
	// a sequence number, so the SFU sees ingress loss.
	simDropped int64
}

type streamClient struct {
	role string

	conn   *websocket.Conn
	reader *wsReader
	sendMu sync.Mutex

	pc         *webrtc.PeerConnection
	controlDC  *webrtc.DataChannel
	videoTrack *webrtc.TrackLocalStaticRTP
	audioTrack *webrtc.TrackLocalStaticRTP

	closed     atomic.Bool
	connected  atomic.Bool
	failed     atomic.Bool
	answerSeen atomic.Bool

	viewer *viewerMetrics

	loopDone chan struct{}
}

func (c *streamClient) handleSignaling(msg *signalMsg) error {
	switch msg.Type {
	case "answer":
		if c.answerSeen.Load() {
			return nil
		}
		var data struct {
			SDP string `json:"sdp"`
		}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		if data.SDP == "" {
			return fmt.Errorf("answer missing sdp")
		}
		if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeAnswer,
			SDP:  data.SDP,
		}); err != nil {
			return err
		}
		c.answerSeen.Store(true)
		return nil
	case "ice_candidate":
		var data struct {
			Candidate     string `json:"candidate"`
			SDPMid        string `json:"sdp_mid"`
			SDPMLineIndex int    `json:"sdp_mline_index"`
		}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		if data.Candidate == "" {
			return nil
		}
		candidate := strings.TrimPrefix(data.Candidate, "a=")
		mid := data.SDPMid
		mline := uint16(data.SDPMLineIndex)
		init := webrtc.ICECandidateInit{
			Candidate:     candidate,
			SDPMid:        &mid,
			SDPMLineIndex: &mline,
		}
		return c.pc.AddICECandidate(init)
	case "offer":
		var data struct {
			SDP string `json:"sdp"`
		}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		if data.SDP == "" {
			return fmt.Errorf("renegotiation offer missing sdp")
		}
		if err := c.pc.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  data.SDP,
		}); err != nil {
			return err
		}
		answer, err := c.pc.CreateAnswer(nil)
		if err != nil {
			return err
		}
		if err := c.pc.SetLocalDescription(answer); err != nil {
			return err
		}
		return mustSend(c.conn, &c.sendMu, map[string]any{
			"type": "answer",
			"seq":  0,
			"data": map[string]any{"sdp": answer.SDP},
		})
	case "error":
		var data struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(msg.Data, &data)
		if data.Code == "" {
			data.Code = "UNKNOWN"
		}
		return fmt.Errorf("sfu_error:%s:%s", data.Code, data.Message)
	case "member_joined", "member_left", "left", "session_ended":
		return nil
	default:
		return nil
	}
}

func (c *streamClient) startSignalingLoop() {
	c.loopDone = make(chan struct{})
	go func() {
		defer close(c.loopDone)
		for !c.closed.Load() {
			msg, err := c.reader.recv(500 * time.Millisecond)
			if err != nil {
				if errors.Is(err, errRecvTimeout) {
					continue
				}
				return
			}
			if err := c.handleSignaling(msg); err != nil {
				return
			}
		}
	}()
}

func (c *streamClient) close() {
	if c == nil {
		return
	}
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	_ = mustSend(c.conn, &c.sendMu, map[string]any{
		"type": "leave",
		"seq":  0,
		"data": map[string]any{},
	})
	_ = c.conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "client_leave"),
		time.Now().Add(500*time.Millisecond),
	)
	_ = c.conn.Close()
	if c.pc != nil {
		_ = c.pc.Close()
	}
	if c.loopDone != nil {
		<-c.loopDone
	}
}

func connectStreamClient(
	wsURL string,
	sessionID string,
	role string,
	userID string,
	connectTimeout time.Duration,
	viewer *viewerMetrics,
	drop *viewerDrop,
) (*streamClient, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial failed: %w", err)
	}
	reader := newWSReader(conn)
	sendMu := sync.Mutex{}

	welcome, err := reader.recv(5 * time.Second)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("welcome read failed: %w", err)
	}
	if welcome.Type != "welcome" {
		_ = conn.Close()
		return nil, fmt.Errorf("expected welcome, got %s", welcome.Type)
	}

	joinReq := map[string]any{
		"type": "join_stream",
		"seq":  1,
		"data": joinStreamData{
			SessionID: sessionID,
			Role:      role,
		},
	}
	if err := mustSend(conn, &sendMu, joinReq); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("join_stream send failed: %w", err)
	}

	joined := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := reader.recv(750 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				continue
			}
			_ = conn.Close()
			return nil, fmt.Errorf("join read failed: %w", err)
		}
		switch msg.Type {
		case "joined":
			joined = true
		case "error":
			var e struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			_ = json.Unmarshal(msg.Data, &e)
			if e.Code == "" {
				e.Code = "UNKNOWN"
			}
			_ = conn.Close()
			return nil, fmt.Errorf("join rejected: %s (%s)", e.Code, e.Message)
		}
		if joined {
			break
		}
	}
	if !joined {
		_ = conn.Close()
		return nil, errors.New("join timeout")
	}

	var api *webrtc.API
	if drop != nil {
		api, err = newStreamAPIWithDrop(drop)
	} else {
		api, err = newStreamAPI()
	}
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	c := &streamClient{
		role:   role,
		conn:   conn,
		reader: reader,
		pc:     pc,
		viewer: viewer,
	}

	controlDC, err := pc.CreateDataChannel("control", nil)
	if err != nil {
		c.close()
		return nil, err
	}
	c.controlDC = controlDC

	videoTrack, audioTrack, err := addStreamMedia(pc, role, sessionID)
	if err != nil {
		c.close()
		return nil, err
	}
	c.videoTrack = videoTrack
	c.audioTrack = audioTrack
	if role == "viewer" && viewer != nil {
		pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
			kind := track.Kind()
			if kind != webrtc.RTPCodecTypeVideo && kind != webrtc.RTPCodecTypeAudio {
				return
			}
			go func() {
				for {
					pkt, _, readErr := track.ReadRTP()
					if readErr != nil {
						return
					}
					if kind == webrtc.RTPCodecTypeAudio {
						viewer.onAudioPacket()
						continue
					}
					viewer.onPacket(pkt.SequenceNumber, len(pkt.Payload), time.Now())
				}
			}()
		})
	}

	dcReady := make(chan struct{})
	controlDC.OnOpen(func() { close(dcReady) })

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			c.connected.Store(true)
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			c.failed.Store(true)
		}
	})

	pc.OnICECandidate(func(cand *webrtc.ICECandidate) {
		if cand == nil || c.closed.Load() {
			return
		}
		init := cand.ToJSON()
		mid := ""
		if init.SDPMid != nil {
			mid = *init.SDPMid
		}
		mline := 0
		if init.SDPMLineIndex != nil {
			mline = int(*init.SDPMLineIndex)
		}
		_ = mustSend(conn, &sendMu, map[string]any{
			"type": "ice_candidate",
			"seq":  0,
			"data": map[string]any{
				"candidate":       init.Candidate,
				"sdp_mid":         mid,
				"sdp_mline_index": mline,
			},
		})
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		c.close()
		return nil, err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		c.close()
		return nil, err
	}
	if err := mustSend(conn, &sendMu, map[string]any{
		"type": "offer",
		"seq":  0,
		"data": map[string]any{"sdp": offer.SDP},
	}); err != nil {
		c.close()
		return nil, err
	}

	connectDeadline := time.Now().Add(connectTimeout)
	for time.Now().Before(connectDeadline) {
		msg, err := reader.recv(250 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				if c.failed.Load() {
					c.close()
					return nil, errors.New("peer connection failed")
				}
				select {
				case <-dcReady:
					if c.connected.Load() {
						c.startSignalingLoop()
						return c, nil
					}
				default:
				}
				continue
			}
			c.close()
			return nil, fmt.Errorf("connect recv failed: %w", err)
		}
		if err := c.handleSignaling(msg); err != nil {
			c.close()
			return nil, err
		}
		select {
		case <-dcReady:
			if c.connected.Load() {
				c.startSignalingLoop()
				return c, nil
			}
		default:
		}
		if c.failed.Load() {
			c.close()
			return nil, errors.New("peer connection failed")
		}
	}

	c.close()
	return nil, errors.New("peer connection timeout")
}

// addStreamMedia adds the media m-lines the client offers for its stream role
// (01-SFU.md §6.1): the host sends H.264 and Opus, a viewer receives both. The
// SFU rejects a viewer offer without a recvonly audio m-line.
func addStreamMedia(
	pc *webrtc.PeerConnection,
	role string,
	sessionID string,
) (*webrtc.TrackLocalStaticRTP, *webrtc.TrackLocalStaticRTP, error) {
	switch role {
	case "host":
		streamID := fmt.Sprintf("stream-host-%s", sessionID)
		videoTrack, err := webrtc.NewTrackLocalStaticRTP(h264CodecCapability(), "video", streamID)
		if err != nil {
			return nil, nil, err
		}
		audioTrack, err := webrtc.NewTrackLocalStaticRTP(opusCodecCapability(), "audio", streamID)
		if err != nil {
			return nil, nil, err
		}
		for _, track := range []*webrtc.TrackLocalStaticRTP{videoTrack, audioTrack} {
			if _, err := pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
				Direction: webrtc.RTPTransceiverDirectionSendonly,
			}); err != nil {
				return nil, nil, err
			}
		}
		return videoTrack, audioTrack, nil
	case "viewer":
		for _, kind := range []webrtc.RTPCodecType{webrtc.RTPCodecTypeVideo, webrtc.RTPCodecTypeAudio} {
			if _, err := pc.AddTransceiverFromKind(
				kind,
				webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
			); err != nil {
				return nil, nil, err
			}
		}
		return nil, nil, nil
	default:
		return nil, nil, fmt.Errorf("unsupported role %q", role)
	}
}

type pumpConfig struct {
	fps  int
	hold time.Duration
	// hostUser and viewerUser, when set, are the SFU identities of the host
	// and of viewer 1, so runs share one person (the admin's Troubleshoot
	// page). Empty: <session>-host and <session>-viewer-1.
	hostUser   string
	viewerUser string
	// simDropPct is the percentage of video packets the host does not send
	// (seeded by simSeed). simJitterMs delays each frame by a half-normal
	// draw with this sigma, frames kept in order.
	simDropPct  float64
	simJitterMs float64
	simSeed     int64
	// audioLead starts the host audio pump this long before the video pump,
	// so the SFU wires the host audio track first.
	audioLead time.Duration
	// viewerDropPct is the share of video packets that viewer 1 drops on
	// the SFU-to-viewer leg before its NACK generator (viewer_drop.go).
	viewerDropPct  float64
	viewerDropSeed int64
}

func runHostRTPPump(track *webrtc.TrackLocalStaticRTP, cfg pumpConfig, tx *hostTxMetrics) int64 {
	if track == nil || cfg.fps <= 0 {
		return 0
	}

	frameDur := time.Second / time.Duration(cfg.fps)
	tsStep := uint32(h264ClockRate / uint32(cfg.fps))
	end := time.Now().Add(cfg.hold)
	nextFrame := time.Now()
	sim := newSimImpairment(cfg.simDropPct, cfg.simJitterMs, cfg.simSeed)
	var lastSend time.Time

	var seq uint16
	var ts uint32
	frameIndex := 0
	pumpStart := time.Now().UnixNano()

	for time.Now().Before(end) {
		now := time.Now()
		if now.Before(nextFrame) {
			time.Sleep(nextFrame.Sub(now))
		}
		sendAt := nextFrame.Add(sim.frameDelay())
		if sendAt.Before(lastSend) {
			sendAt = lastSend
		}
		if d := time.Until(sendAt); d > 0 {
			time.Sleep(d)
		}
		lastSend = sendAt
		nextFrame = nextFrame.Add(frameDur)

		frame := planFramePackets(frameIndex)
		tx.frames++
		if frame.keyframe {
			tx.keyframes++
		}
		for _, planned := range frame.packets {
			tx.attempted++
			if sim.drop() {
				tx.simDropped++
				seq++
				continue
			}
			pkt := buildRTPPacket(seq, ts, planned.marker, planned.payload)
			if err := track.WriteRTP(pkt); err != nil {
				tx.errors++
			} else {
				tx.sent++
				tx.bytes += int64(len(planned.payload))
			}
			seq++
		}
		ts += tsStep
		frameIndex++
	}

	return pumpStart
}

type hostAudioTxMetrics struct {
	sent   int64
	errors int64
}

// runHostAudioPump sends one silent Opus frame every 20 ms for the hold time.
func runHostAudioPump(track *webrtc.TrackLocalStaticRTP, hold time.Duration, tx *hostAudioTxMetrics) {
	if track == nil {
		return
	}
	frameDur := time.Duration(opusFrameMs) * time.Millisecond
	end := time.Now().Add(hold)
	nextFrame := time.Now()

	var seq uint16
	var ts uint32
	for time.Now().Before(end) {
		now := time.Now()
		if now.Before(nextFrame) {
			time.Sleep(nextFrame.Sub(now))
		}
		nextFrame = nextFrame.Add(frameDur)

		if err := track.WriteRTP(buildOpusRTPPacket(seq, ts)); err != nil {
			tx.errors++
		} else {
			tx.sent++
		}
		seq++
		ts += opusTimestampStep
	}
}

type soakResult struct {
	sessionID          string
	viewers            int
	connectFailures    int
	hostTx             hostTxMetrics
	hostAudioTx        hostAudioTxMetrics
	totalAudioRx       int64
	expectedAudio      int64
	audioRxRatio       float64
	totalRxPackets     int64
	totalSeqGaps       int64
	totalDuplicates    int64
	totalOutOfOrder    int64
	expectedPackets    int64
	rxRatio            float64
	p95InterArrivalMs  float64
	maxFirstPacketMs   float64
	adminRelay         adminRelayStats
	reconcile          relayReconcileResult
	minRxRatio         float64
	maxP95Ms           float64
	maxQueueDrops      int64
	maxDuplicates      int64
	skipAdminReconcile bool
	// Viewer-leg repair (--viewer-drop-pct): packets viewer 1 dropped and
	// the ones that arrived later (the SFU's NACK responder sent them).
	viewerDropPct  float64
	viewerDropped  int64
	viewerRepaired int64
	passed         bool
	failReasons    []string
}

// viewerRepairTolerance: the repaired count must reach the dropped count
// within 1 %.
const viewerRepairTolerance = 0.01

func runSoak(
	wsURL string,
	adminBase string,
	adminPassword string,
	testPassword string,
	sessionID string,
	viewers int,
	hold time.Duration,
	cfg pumpConfig,
	gates struct {
		minRxRatio         float64
		maxP95Ms           float64
		maxQueueDrops      int64
		maxDuplicates      int64
		skipAdminReconcile bool
	},
) (soakResult, error) {
	result := soakResult{
		sessionID:          sessionID,
		viewers:            viewers,
		minRxRatio:         gates.minRxRatio,
		maxP95Ms:           gates.maxP95Ms,
		maxQueueDrops:      gates.maxQueueDrops,
		maxDuplicates:      gates.maxDuplicates,
		skipAdminReconcile: gates.skipAdminReconcile,
	}

	clients := make([]*streamClient, 0, viewers+1)
	cleanup := func() {
		for i := len(clients) - 1; i >= 0; i-- {
			clients[i].close()
		}
	}
	defer cleanup()

	baseWSURL := wsURL
	hostID := fmt.Sprintf("%s-host", sessionID)
	if cfg.hostUser != "" {
		hostID = cfg.hostUser
	}
	hostURL, err := endpointWithTestIdentity(baseWSURL, testPassword, hostID, hostID)
	if err != nil {
		return result, fmt.Errorf("host endpoint: %w", err)
	}

	host, err := connectStreamClient(hostURL, sessionID, "host", hostID, 15*time.Second, nil, nil)
	if err != nil {
		return result, fmt.Errorf("host connect failed: %w", err)
	}
	clients = append(clients, host)

	var drop *viewerDrop
	if cfg.viewerDropPct > 0 {
		drop = newViewerDrop(cfg.viewerDropPct, cfg.viewerDropSeed)
		result.viewerDropPct = cfg.viewerDropPct
	}
	viewerStats := make([]*viewerMetrics, 0, viewers)
	for i := 0; i < viewers; i++ {
		viewerID := fmt.Sprintf("%s-viewer-%d", sessionID, i+1)
		if i == 0 && cfg.viewerUser != "" {
			viewerID = cfg.viewerUser
		}
		vs := &viewerMetrics{id: viewerID}
		viewerURL, err := endpointWithTestIdentity(baseWSURL, testPassword, viewerID, viewerID)
		if err != nil {
			return result, fmt.Errorf("viewer endpoint: %w", err)
		}
		var viewerDropper *viewerDrop
		if i == 0 {
			viewerDropper = drop
		}
		v, err := connectStreamClient(viewerURL, sessionID, "viewer", viewerID, 15*time.Second, vs, viewerDropper)
		if err != nil {
			result.connectFailures++
			continue
		}
		viewerStats = append(viewerStats, vs)
		clients = append(clients, v)
		time.Sleep(100 * time.Millisecond)
	}
	if len(viewerStats) == 0 {
		return result, errors.New("no viewers connected")
	}

	readyDeadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(readyDeadline) {
		allReady := host.connected.Load()
		for _, client := range clients[1:] {
			allReady = allReady && client.connected.Load()
		}
		if allReady {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	cfg.hold = hold
	tx := &hostTxMetrics{}
	audioTx := &hostAudioTxMetrics{}
	audioDone := make(chan struct{})
	go func() {
		defer close(audioDone)
		runHostAudioPump(host.audioTrack, hold+cfg.audioLead, audioTx)
	}()
	time.Sleep(cfg.audioLead)
	pumpStart := runHostRTPPump(host.videoTrack, cfg, tx)
	<-audioDone
	result.hostTx = *tx
	result.hostAudioTx = *audioTx

	time.Sleep(700 * time.Millisecond)

	var allInterArrival []int64
	maxFirstPacket := 0.0
	for _, vs := range viewerStats {
		snap := vs.snapshot()
		result.totalRxPackets += snap.packets
		result.totalSeqGaps += snap.seqGaps
		result.totalDuplicates += snap.duplicates
		result.totalOutOfOrder += snap.outOfOrder
		result.totalAudioRx += snap.audioPackets
		allInterArrival = append(allInterArrival, snap.interArrival...)
		if lat := firstPacketLatencyMs(snap.firstPacketNs, pumpStart); lat > maxFirstPacket {
			maxFirstPacket = lat
		}
	}
	result.maxFirstPacketMs = maxFirstPacket
	result.expectedPackets = result.hostTx.sent * int64(len(viewerStats))
	if result.expectedPackets > 0 {
		result.rxRatio = float64(result.totalRxPackets) / float64(result.expectedPackets)
	}
	result.expectedAudio = result.hostAudioTx.sent * int64(len(viewerStats))
	if result.expectedAudio > 0 {
		result.audioRxRatio = float64(result.totalAudioRx) / float64(result.expectedAudio)
	}
	result.p95InterArrivalMs = p95Millis(allInterArrival)
	if drop != nil {
		result.viewerDropped, result.viewerRepaired = drop.counts()
	}

	adminStats, adminErr := fetchAdminRelayStats(adminBase, sessionID, adminPassword)
	if adminErr != nil {
		if gates.skipAdminReconcile {
			fmt.Fprintf(os.Stderr, "stream-soak warning: skipping admin reconcile: %v\n", adminErr)
		} else {
			return result, fmt.Errorf("admin relay fetch failed: %w", adminErr)
		}
	} else {
		result.adminRelay = adminStats
		result.reconcile = reconcileRelay(adminStats, result.hostTx.sent, len(viewerStats), gates.maxQueueDrops, gates.maxDuplicates)
	}

	result.failReasons = evaluateGates(result)
	result.passed = len(result.failReasons) == 0
	return result, nil
}

func evaluateGates(r soakResult) []string {
	var reasons []string
	if r.rxRatio < r.minRxRatio {
		reasons = append(reasons, fmt.Sprintf("rx_ratio %.4f < %.4f", r.rxRatio, r.minRxRatio))
	}
	if r.audioRxRatio < r.minRxRatio {
		reasons = append(reasons, fmt.Sprintf("audio_rx_ratio %.4f < %.4f", r.audioRxRatio, r.minRxRatio))
	}
	if r.p95InterArrivalMs > r.maxP95Ms {
		reasons = append(reasons, fmt.Sprintf("p95_inter_arrival_ms %.1f > %.1f", r.p95InterArrivalMs, r.maxP95Ms))
	}
	if r.totalDuplicates > r.maxDuplicates {
		reasons = append(reasons, fmt.Sprintf("viewer_duplicates %d > %d", r.totalDuplicates, r.maxDuplicates))
	}
	if r.viewerDropPct > 0 && !repairGateOK(r.viewerDropped, r.viewerRepaired, viewerRepairTolerance) {
		reasons = append(reasons, fmt.Sprintf("viewer_leg_repaired %d < %.0f%% of dropped %d",
			r.viewerRepaired, (1-viewerRepairTolerance)*100, r.viewerDropped))
	}
	if r.skipAdminReconcile {
		return reasons
	}
	if !r.reconcile.queueDropsOK {
		reasons = append(reasons, fmt.Sprintf("admin_queue_drops %d > %d", r.reconcile.adminQueueDrops, r.maxQueueDrops))
	}
	if !r.reconcile.duplicatesOK {
		reasons = append(reasons, fmt.Sprintf("admin_duplicates %d > %d", r.reconcile.adminDuplicates, r.maxDuplicates))
	}
	if !r.reconcile.inMatchesHost {
		reasons = append(reasons, fmt.Sprintf("admin_rtp_in %d != host_sent %d", r.reconcile.adminIn, r.reconcile.hostSent))
	}
	if !r.reconcile.forwardMatches {
		reasons = append(reasons, fmt.Sprintf("admin_rtp_forwarded %d < expected ~%d", r.reconcile.adminForwarded, r.reconcile.expectedForward))
	}
	return reasons
}

func printSoakResult(r soakResult) {
	fmt.Printf("\n=== stream-soak session=%s transport=rtp ===\n", r.sessionID)
	fmt.Printf("viewers_connected:       %d (connect_failures=%d)\n", r.viewers-r.connectFailures, r.connectFailures)
	fmt.Printf("host_frames:             %d (keyframes=%d)\n", r.hostTx.frames, r.hostTx.keyframes)
	fmt.Printf("host_sim_dropped:        %d\n", r.hostTx.simDropped)
	fmt.Printf("host_tx_attempted:       %d\n", r.hostTx.attempted)
	fmt.Printf("host_tx_sent:            %d\n", r.hostTx.sent)
	fmt.Printf("host_tx_errors:          %d\n", r.hostTx.errors)
	fmt.Printf("host_tx_bytes:           %d\n", r.hostTx.bytes)
	fmt.Printf("expected_rx_packets:     %d\n", r.expectedPackets)
	fmt.Printf("aggregate_rx_packets:    %d\n", r.totalRxPackets)
	fmt.Printf("aggregate_seq_gaps:      %d\n", r.totalSeqGaps)
	fmt.Printf("aggregate_duplicates:    %d\n", r.totalDuplicates)
	fmt.Printf("aggregate_out_of_order:  %d\n", r.totalOutOfOrder)
	fmt.Printf("rx_ratio:                %.4f (gate >= %.4f)\n", r.rxRatio, r.minRxRatio)
	fmt.Printf("p95_inter_arrival_ms:    %.1f (gate <= %.1f)\n", r.p95InterArrivalMs, r.maxP95Ms)
	fmt.Printf("max_first_packet_ms:     %.1f\n", r.maxFirstPacketMs)
	fmt.Printf("host_audio_tx_sent:      %d\n", r.hostAudioTx.sent)
	fmt.Printf("host_audio_tx_errors:    %d\n", r.hostAudioTx.errors)
	fmt.Printf("expected_audio_rx:       %d\n", r.expectedAudio)
	fmt.Printf("rtp_audio_rx:            %d\n", r.totalAudioRx)
	fmt.Printf("audio_rx_ratio:          %.4f (gate >= %.4f)\n", r.audioRxRatio, r.minRxRatio)
	fmt.Printf("admin_rtp_in:            %d\n", r.adminRelay.RTPInPackets)
	fmt.Printf("admin_rtp_fanout:        %d\n", r.adminRelay.RTPFanoutAttempts)
	fmt.Printf("admin_rtp_forwarded:     %d\n", r.adminRelay.RTPForwardedPackets)
	fmt.Printf("admin_rtp_queue_drops:   %d (gate <= %d)\n", r.adminRelay.RTPQueueDroppedPackets, r.maxQueueDrops)
	fmt.Printf("admin_rtp_duplicates:    %d (gate <= %d)\n", r.adminRelay.RTPDuplicatePackets, r.maxDuplicates)
	if r.viewerDropPct > 0 {
		fmt.Printf("viewer_leg_drop_pct:     %.2f (viewer 1)\n", r.viewerDropPct)
		fmt.Printf("viewer_leg_dropped:      %d\n", r.viewerDropped)
		fmt.Printf("viewer_leg_repaired:     %d (gate >= %.0f%% of dropped)\n", r.viewerRepaired, (1-viewerRepairTolerance)*100)
	}
	if len(r.failReasons) > 0 {
		fmt.Println("fail_reasons:")
		for _, reason := range r.failReasons {
			fmt.Printf("  - %s\n", reason)
		}
	}
	fmt.Printf("pass:                    %v\n", r.passed)
}

func main() {
	endpoint := flag.String("endpoint", "ws://127.0.0.1:8443/ws", "SFU WebSocket endpoint")
	adminEndpoint := flag.String("admin-endpoint", "", "SFU admin HTTP base URL (default: derived from --endpoint host on :8080)")
	adminPassword := flag.String("admin-password", "", "SFU admin API password, for the counter reconciliation (or SFU_ADMIN_PASSWORD)")
	testPassword := flag.String("test-password", "", "SFU test-mode password (or SFU_TEST_PASSWORD; default: the admin password)")
	sessionPrefix := flag.String("session-prefix", "stream-soak", "session id prefix")
	hostUser := flag.String("host-user", "", "SFU user id of the host (default <session>-host); the same id across runs makes one person")
	viewerUser := flag.String("viewer-user", "", "SFU user id of viewer 1 (default <session>-viewer-1)")
	transport := flag.String("transport", "rtp", "media transport (only rtp is supported)")
	viewers := flag.Int("viewers", 8, "number of viewers to connect (plus one host)")
	holdMs := flag.Int("hold-ms", 15000, "test hold duration in milliseconds")
	fps := flag.Int("fps", 30, "host video frame rate (the H.264 fixture plays at this rate)")
	simDropPct := flag.Float64("sim-drop-pct", 0, "percentage of host video packets not sent (ingress loss at the SFU), 0-100")
	simJitterMs := flag.Float64("sim-jitter-ms", 0, "sigma of a half-normal extra delay per host frame, in ms")
	simSeed := flag.Int64("sim-seed", 1, "seed of --sim-drop-pct and --sim-jitter-ms")
	audioLeadMs := flag.Int("audio-lead-ms", 0, "start host audio this many ms before host video, so the SFU wires the audio track first")
	viewerDropPct := flag.Float64("viewer-drop-pct", 0, "percentage of video packets viewer 1 drops on the SFU-to-viewer leg before NACK; gates that the SFU repairs them, 0-100")
	viewerDropSeed := flag.Int64("viewer-drop-seed", 1, "seed of --viewer-drop-pct")
	minRxRatio := flag.Float64("min-rx-ratio", 0.95, "minimum aggregate RX ratio")
	maxP95Ms := flag.Float64("max-p95-ms", 160, "maximum p95 inter-arrival delay (ms)")
	maxQueueDrops := flag.Int64("max-queue-drops", 0, "maximum allowed admin rtp_queue_dropped_packets")
	maxDuplicates := flag.Int64("max-duplicates", 0, "maximum allowed duplicate RTP packets (viewer + admin)")
	skipAdminReconcile := flag.Bool("skip-admin-reconcile", false, "skip admin RTP counter reconciliation when admin auth is unavailable")
	flag.Parse()

	adminPwd := strings.TrimSpace(*adminPassword)
	if adminPwd == "" {
		adminPwd = strings.TrimSpace(os.Getenv("SFU_ADMIN_PASSWORD"))
	}
	testPwd := firstNonEmpty(*testPassword, os.Getenv("SFU_TEST_PASSWORD"), adminPwd)

	if strings.TrimSpace(*transport) != "rtp" {
		fmt.Fprintf(os.Stderr, "error: unsupported transport %q (only rtp)\n", *transport)
		os.Exit(2)
	}
	if *viewers <= 0 {
		fmt.Fprintln(os.Stderr, "error: --viewers must be > 0")
		os.Exit(2)
	}
	if *fps <= 0 {
		fmt.Fprintln(os.Stderr, "error: --fps must be > 0")
		os.Exit(2)
	}
	if *simDropPct < 0 || *simDropPct >= 100 {
		fmt.Fprintln(os.Stderr, "error: --sim-drop-pct must be in [0, 100)")
		os.Exit(2)
	}
	if *simJitterMs < 0 {
		fmt.Fprintln(os.Stderr, "error: --sim-jitter-ms must be >= 0")
		os.Exit(2)
	}
	if *viewerDropPct < 0 || *viewerDropPct >= 100 {
		fmt.Fprintln(os.Stderr, "error: --viewer-drop-pct must be in [0, 100)")
		os.Exit(2)
	}
	if *audioLeadMs < 0 {
		fmt.Fprintln(os.Stderr, "error: --audio-lead-ms must be >= 0")
		os.Exit(2)
	}

	wsURL, err := endpointWithPassword(*endpoint, testPwd)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid endpoint: %v\n", err)
		os.Exit(2)
	}

	adminBase := strings.TrimSpace(*adminEndpoint)
	if adminBase == "" {
		adminBase, err = adminBaseFromWSEndpoint(*endpoint)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: derive admin endpoint: %v\n", err)
			os.Exit(2)
		}
	}

	sessionID := fmt.Sprintf("%s-%d", *sessionPrefix, time.Now().UnixNano())
	hold := time.Duration(*holdMs) * time.Millisecond

	fmt.Printf(
		"stream-soak start endpoint=%s admin=%s transport=rtp viewers=%d hold=%s fps=%d fixture_aus=%d sim_drop_pct=%.2f sim_jitter_ms=%.1f audio_lead_ms=%d viewer_drop_pct=%.2f\n",
		*endpoint,
		adminBase,
		*viewers,
		hold.String(),
		*fps,
		len(h264FixtureFrames),
		*simDropPct,
		*simJitterMs,
		*audioLeadMs,
		*viewerDropPct,
	)

	res, err := runSoak(
		wsURL,
		adminBase,
		adminPwd,
		testPwd,
		sessionID,
		*viewers,
		hold,
		pumpConfig{
			fps:            *fps,
			simDropPct:     *simDropPct,
			simJitterMs:    *simJitterMs,
			simSeed:        *simSeed,
			audioLead:      time.Duration(*audioLeadMs) * time.Millisecond,
			viewerDropPct:  *viewerDropPct,
			viewerDropSeed: *viewerDropSeed,
			hostUser:       strings.TrimSpace(*hostUser),
			viewerUser:     strings.TrimSpace(*viewerUser),
		},
		struct {
			minRxRatio         float64
			maxP95Ms           float64
			maxQueueDrops      int64
			maxDuplicates      int64
			skipAdminReconcile bool
		}{
			minRxRatio:         *minRxRatio,
			maxP95Ms:           *maxP95Ms,
			maxQueueDrops:      *maxQueueDrops,
			maxDuplicates:      *maxDuplicates,
			skipAdminReconcile: *skipAdminReconcile,
		},
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "stream-soak failed: %v\n", err)
		os.Exit(1)
	}
	printSoakResult(res)
	if !res.passed {
		os.Exit(1)
	}
}

// firstNonEmpty returns the first value that is not blank, trimmed.
func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}
