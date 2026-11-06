package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type signalMsg struct {
	Type string          `json:"type"`
	Seq  int             `json:"seq"`
	Data json.RawMessage `json:"data"`
}

type joinVoiceData struct {
	CrewID    string `json:"crew_id"`
	ChannelID string `json:"channel_id"`
}

type soakStats struct {
	connectOK        atomic.Int64
	connectFail      atomic.Int64
	welcomeOK        atomic.Int64
	joinOK           atomic.Int64
	joinFail         atomic.Int64
	joinTimeout      atomic.Int64
	wsReadErr        atomic.Int64
	memberJoined     atomic.Int64
	memberLeft       atomic.Int64
	totalJoinNanos   atomic.Int64
	webrtcAttempts   atomic.Int64
	webrtcOK         atomic.Int64
	webrtcFail       atomic.Int64
	webrtcTimeout    atomic.Int64
	webrtcOfferTx    atomic.Int64
	webrtcAnswerRx   atomic.Int64
	renegOfferRx     atomic.Int64
	renegAnswerTx    atomic.Int64
	iceCandTx        atomic.Int64
	iceCandRx        atomic.Int64
	rtpSendRounds    atomic.Int64
	rtpTxPackets     atomic.Int64
	rtpTxBytes       atomic.Int64
	rtpTxErrors      atomic.Int64
	rtpRxTracks      atomic.Int64
	rtpRxPackets     atomic.Int64
	rtpRxBytes       atomic.Int64
	halfOpenFrozen   atomic.Int64
	idleResumeRounds atomic.Int64

	// Receive slots (01-SFU.md §5.5): an offer to a client that never saw
	// more members than its slots is a defect.
	offersWithinSlots atomic.Int64
	offersPastSlots   atomic.Int64
	slotMsgRx         atomic.Int64
	speakingMsgRx     atomic.Int64
	speakingMsgTx     atomic.Int64
	rtpRxMapped       atomic.Int64
	rtpRxUnmapped     atomic.Int64 // on a receive slot with no owner in the slot map
	rtpRxUnmappedOwn  atomic.Int64 // on an own m-line (past the slots) with no owner in the slot map
	rtpRxUndeclared   atomic.Int64 // on an SSRC no negotiated m-line declares

	// Transport of the selected ICE pair when the PeerConnection connects.
	pcPairUDP   atomic.Int64
	pcPairTCP   atomic.Int64
	pcPairOther atomic.Int64

	// The unmapped packets split by the next slot message for their mid:
	// early = before the new owner's assign, late = up to lateWindow after
	// the old owner's release (or never settled).
	rtpRxEarlySlot atomic.Int64
	rtpRxLateSlot  atomic.Int64
	rtpRxEarlyOwn  atomic.Int64
	rtpRxLateOwn   atomic.Int64
	// rtpRxEarlyLeadMax is the longest time an early packet arrived before
	// its slot message: the buffer a client needs to keep every one.
	rtpRxEarlyLeadMax atomic.Int64 // nanoseconds

	mu             sync.Mutex
	joinErrorCodes map[string]int64
	webrtcErrors   map[string]int64
	maxJoinNanos   int64
}

type soakOptions struct {
	endpoint            string
	adminPassword       string
	crewID              string
	channelID           string
	clients             int
	rounds              int
	hold                time.Duration
	stagger             time.Duration
	negotiateWebRTC     bool
	pcConnectTimeout    time.Duration
	gracefulLeave       bool
	minJoinSuccessRate  float64
	minPCConnectRate    float64
	seed                int64
	enableRTPAudio      bool
	rtpPacketsPerSecond int
	rtpPayloadBytes     int
	sweepClientCounts   []int

	// Robustness scenarios (server-resilience focused).
	reuseUserID  bool          // reuse a stable SFU identity per client across rounds (same-user reconnect/eviction)
	halfOpen     bool          // after join, freeze the client (stop reading WS, keep PC alive) to simulate a sleep/wake zombie
	halfOpenHold time.Duration // how long to stay frozen before dropping the connection
	idleResume   time.Duration // idle (no RTP) for this long after connect, then resume sending RTP

	// Voice slots and media realism.
	recvSlots            int      // recvonly receive slots in each offer (mello client: 16)
	rtpSource            string   // "speech" (embedded Opus clip) or "synthetic"
	vadGaps              bool     // talk spurts with silences, and in-band speaking messages
	maxOffersWithinSlots int      // gate: offers to clients that never exceeded their slots (-1 = off)
	iceTCPOnly           bool     // gather only TCP4 candidates: a client on a UDP-blocked network
	speechPackets        [][]byte // loaded once from the embedded clip
}

type soakResult struct {
	totalAttempts  int64
	joinSuccess    int64
	joinSuccessPct float64
	pcAttempts     int64
	pcSuccess      int64
	pcSuccessPct   float64
	passedJoinGate bool
	passedPCGate   bool
	passedSlotGate bool
	passedAllGates bool
}

func (s *soakStats) recordJoinError(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinErrorCodes[code]++
}

func (s *soakStats) recordJoinLatency(d time.Duration) {
	ns := d.Nanoseconds()
	s.totalJoinNanos.Add(ns)
	s.mu.Lock()
	if ns > s.maxJoinNanos {
		s.maxJoinNanos = ns
	}
	s.mu.Unlock()
}

func (s *soakStats) recordWebrtcError(code string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.webrtcErrors[code]++
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

func endpointWithPassword(endpoint, password string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("test_password", password)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// withTestUser appends a stable SFU identity so successive rounds reconnect as
// the same user, exercising the same-user eviction path in voice_session.go.
func withTestUser(wsURL, userID string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("test_user_id", userID)
	q.Set("test_username", userID)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func parseCSVInts(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v == "" {
			continue
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("invalid client count %q", v)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("empty client sweep")
	}
	return out, nil
}

func makeOpusPayload(size int) []byte {
	if size <= 0 {
		size = 3
	}
	payload := make([]byte, size)
	// Minimal Opus comfort-noise style payload prefix.
	payload[0] = 0xF8
	if size > 1 {
		payload[1] = 0xFF
	}
	if size > 2 {
		payload[2] = 0xFE
	}
	return payload
}

// newSoakPeerConnection uses Pion's default codecs and interceptors. With
// --ice-tcp-only the client gathers only TCP4 candidates and connects to the
// SFU's passive ICE-TCP candidate as an active TCP client.
func newSoakPeerConnection(opts *soakOptions) (*webrtc.PeerConnection, error) {
	if !opts.iceTCPOnly {
		return webrtc.NewPeerConnection(webrtc.Configuration{})
	}
	se := webrtc.SettingEngine{}
	se.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeTCP4})
	return webrtc.NewAPI(webrtc.WithSettingEngine(se)).NewPeerConnection(webrtc.Configuration{})
}

// countSelectedPair counts the transport of the ICE pair pc selected.
func countSelectedPair(pc *webrtc.PeerConnection, stats *soakStats) {
	pair, err := pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	switch {
	case err != nil || pair == nil:
		stats.pcPairOther.Add(1)
	case pair.Local.Protocol == webrtc.ICEProtocolTCP:
		stats.pcPairTCP.Add(1)
	case pair.Local.Protocol == webrtc.ICEProtocolUDP:
		stats.pcPairUDP.Add(1)
	default:
		stats.pcPairOther.Add(1)
	}
}

var (
	errRecvTimeout      = errors.New("recv_timeout")
	errPCConnectTimeout = errors.New("pc_connect_timeout")
	errPCConnectFailed  = errors.New("pc_connect_failed")
)

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

func handleWebRTCSignaling(
	pc *webrtc.PeerConnection,
	msg *signalMsg,
	initialAnswerApplied *bool,
	sendMu *sync.Mutex,
	conn *websocket.Conn,
	stats *soakStats,
	slots *clientSlots,
) error {
	switch msg.Type {
	case "answer":
		if *initialAnswerApplied {
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
		if err := pc.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeAnswer,
			SDP:  data.SDP,
		}); err != nil {
			return err
		}
		*initialAnswerApplied = true
		stats.webrtcAnswerRx.Add(1)
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
		if err := pc.AddICECandidate(init); err != nil {
			return err
		}
		stats.iceCandRx.Add(1)
	case "offer":
		stats.renegOfferRx.Add(1)
		var data struct {
			SDP string `json:"sdp"`
		}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		slots.recordOffer(data.SDP, stats)
		if data.SDP == "" {
			return fmt.Errorf("renegotiation offer missing sdp")
		}
		if err := pc.SetRemoteDescription(webrtc.SessionDescription{
			Type: webrtc.SDPTypeOffer,
			SDP:  data.SDP,
		}); err != nil {
			return err
		}
		answer, err := pc.CreateAnswer(nil)
		if err != nil {
			return err
		}
		if err := pc.SetLocalDescription(answer); err != nil {
			return err
		}
		if err := mustSend(conn, sendMu, map[string]any{
			"type": "answer",
			"seq":  0,
			"data": map[string]any{"sdp": answer.SDP},
		}); err != nil {
			return err
		}
		stats.renegAnswerTx.Add(1)
	case "member_joined":
		stats.memberJoined.Add(1)
	case "member_left":
		stats.memberLeft.Add(1)
	case "error":
		var data struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(msg.Data, &data)
		if data.Code == "" {
			data.Code = "UNKNOWN"
		}
		phase := "connect"
		if *initialAnswerApplied {
			phase = "after_answer"
		}
		stats.recordWebrtcError(fmt.Sprintf("%s (%s, %s)", data.Code, data.Message, phase))
		return fmt.Errorf("sfu_error:%s", data.Code)
	}
	return nil
}

func startRTPPump(
	track *webrtc.TrackLocalStaticRTP,
	packetsPerSecond int,
	payloadBytes int,
	stop <-chan struct{},
	stats *soakStats,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := time.Second / time.Duration(packetsPerSecond)
		if interval <= 0 {
			interval = 20 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		payload := makeOpusPayload(payloadBytes)
		samplesPerPacket := uint32(48000 / packetsPerSecond)
		if samplesPerPacket == 0 {
			samplesPerPacket = 960
		}

		var seq uint16
		var ts uint32
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				pkt := &rtp.Packet{
					Header: rtp.Header{
						Version:        2,
						PayloadType:    111,
						SequenceNumber: seq,
						Timestamp:      ts,
					},
					Payload: payload,
				}
				if err := track.WriteRTP(pkt); err != nil {
					stats.rtpTxErrors.Add(1)
				} else {
					stats.rtpTxPackets.Add(1)
					stats.rtpTxBytes.Add(int64(len(payload)))
				}
				seq++
				ts += samplesPerPacket
			}
		}
	}()
	return done
}

func runNegotiatedSession(
	conn *websocket.Conn,
	reader *wsReader,
	sendMu *sync.Mutex,
	opts *soakOptions,
	stats *soakStats,
	seed int64,
) error {
	pc, err := newSoakPeerConnection(opts)
	if err != nil {
		return err
	}
	defer pc.Close()

	slots := newClientSlots(opts.recvSlots)
	defer slots.flush(stats)
	control, err := pc.CreateDataChannel("control", nil)
	if err != nil {
		return err
	}
	control.OnMessage(func(msg webrtc.DataChannelMessage) {
		slots.handleControl(msg.Data, stats)
	})

	var localAudioTrack *webrtc.TrackLocalStaticRTP
	if opts.enableRTPAudio {
		localAudioTrack, err = webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{
				MimeType:    webrtc.MimeTypeOpus,
				ClockRate:   48000,
				Channels:    2,
				SDPFmtpLine: "minptime=10;useinbandfec=1",
			},
			"audio",
			fmt.Sprintf("soak-audio-%d", time.Now().UnixNano()),
		)
		if err != nil {
			return err
		}
		if _, err := pc.AddTrack(localAudioTrack); err != nil {
			return err
		}
		stats.rtpSendRounds.Add(1)
	} else {
		_, err = pc.AddTransceiverFromKind(
			webrtc.RTPCodecTypeAudio,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv},
		)
		if err != nil {
			return err
		}
	}

	// Receive slots go after the mic m-line, as in the mello client.
	if err := addRecvSlots(pc, opts.recvSlots); err != nil {
		return err
	}

	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		stats.rtpRxTracks.Add(1)
		// The transceiver of an m-line the SFU offered can get its mid
		// after OnTrack fires: look it up until it is known.
		midOf := func() string {
			for _, tr := range pc.GetTransceivers() {
				if tr.Receiver() == receiver {
					return tr.Mid()
				}
			}
			return ""
		}
		mid := midOf()
		go func() {
			for {
				pkt, _, readErr := track.ReadRTP()
				if readErr != nil {
					return
				}
				if mid == "" {
					mid = midOf()
				}
				stats.rtpRxPackets.Add(1)
				stats.rtpRxBytes.Add(int64(len(pkt.Payload)))
				switch {
				case mid == "":
					// An SSRC that no negotiated m-line declares (Pion made
					// a transceiver without a mid for it).
					stats.rtpRxUndeclared.Add(1)
				case slots.owner(mid) != "":
					stats.rtpRxMapped.Add(1)
				case slots.isSlot(mid):
					stats.rtpRxUnmapped.Add(1)
					slots.recordUnmapped(mid)
				default:
					stats.rtpRxUnmappedOwn.Add(1)
					slots.recordUnmapped(mid)
				}
			}
		}()
	})

	var connected atomic.Bool
	var failed atomic.Bool
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			connected.Store(true)
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			failed.Store(true)
		}
	})
	// Trickle candidates only after the offer: gathering starts in
	// SetLocalDescription and can beat the offer onto the socket, and the
	// SFU answers a candidate without a PeerConnection with an error.
	offerSent := make(chan struct{})
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		<-offerSent
		init := c.ToJSON()
		mid := ""
		if init.SDPMid != nil {
			mid = *init.SDPMid
		}
		mline := 0
		if init.SDPMLineIndex != nil {
			mline = int(*init.SDPMLineIndex)
		}
		_ = mustSend(conn, sendMu, map[string]any{
			"type": "ice_candidate",
			"seq":  0,
			"data": map[string]any{
				"candidate":       init.Candidate,
				"sdp_mid":         mid,
				"sdp_mline_index": mline,
			},
		})
		stats.iceCandTx.Add(1)
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	slots.markSlotMids(pc)
	err = mustSend(conn, sendMu, map[string]any{
		"type": "offer",
		"seq":  0,
		"data": map[string]any{"sdp": offer.SDP},
	})
	close(offerSent)
	if err != nil {
		return err
	}
	stats.webrtcOfferTx.Add(1)

	initialAnswerApplied := false
	deadline := time.Now().Add(opts.pcConnectTimeout)
	for time.Now().Before(deadline) {
		msg, err := reader.recv(250 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				if connected.Load() {
					break
				}
				if failed.Load() {
					return errPCConnectFailed
				}
				continue
			}
			if errors.Is(err, io.EOF) || websocket.IsCloseError(err) {
				return errPCConnectFailed
			}
			return err
		}
		if err := handleWebRTCSignaling(pc, msg, &initialAnswerApplied, sendMu, conn, stats, slots); err != nil {
			return err
		}
		if connected.Load() {
			break
		}
		if failed.Load() {
			return errPCConnectFailed
		}
	}
	if !connected.Load() {
		if failed.Load() {
			return errPCConnectFailed
		}
		return errPCConnectTimeout
	}
	countSelectedPair(pc, stats)

	// Half-open zombie: the PeerConnection stays alive (deferred Close) but we
	// stop consuming the WS and never send RTP, simulating a sleep/wake freeze.
	// The server must detect the dead peer via ping/read deadlines rather than
	// leaving a ghost member behind.
	if opts.halfOpen {
		stats.halfOpenFrozen.Add(1)
		time.Sleep(opts.halfOpenHold)
		return nil
	}

	// Idle-resume: stay connected but silent for a while (reading WS to remain
	// live), then resume RTP. Exercises the SFU's audio-track EOF tolerance over
	// idle gaps and a late RTP resume.
	if opts.idleResume > 0 {
		stats.idleResumeRounds.Add(1)
		idleEnd := time.Now().Add(opts.idleResume)
		for time.Now().Before(idleEnd) {
			msg, err := reader.recv(250 * time.Millisecond)
			if err != nil {
				if errors.Is(err, errRecvTimeout) {
					continue
				}
				if errors.Is(err, io.EOF) || websocket.IsCloseError(err) {
					return nil
				}
				return err
			}
			if err := handleWebRTCSignaling(pc, msg, &initialAnswerApplied, sendMu, conn, stats, slots); err != nil {
				return err
			}
		}
	}

	var pumpStop chan struct{}
	var pumpDone <-chan struct{}
	if opts.enableRTPAudio && localAudioTrack != nil {
		pumpStop = make(chan struct{})
		if opts.rtpSource == "speech" {
			pumpDone = startSpeechPump(
				localAudioTrack,
				control,
				opts.speechPackets,
				opts.vadGaps,
				seed,
				pumpStop,
				stats,
			)
		} else {
			pumpDone = startRTPPump(
				localAudioTrack,
				opts.rtpPacketsPerSecond,
				opts.rtpPayloadBytes,
				pumpStop,
				stats,
			)
		}
		defer func() {
			close(pumpStop)
			<-pumpDone
		}()
	}

	endHold := time.Now().Add(opts.hold)
	for time.Now().Before(endHold) {
		msg, err := reader.recv(250 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				continue
			}
			if errors.Is(err, io.EOF) || websocket.IsCloseError(err) {
				break
			}
			return err
		}
		if err := handleWebRTCSignaling(pc, msg, &initialAnswerApplied, sendMu, conn, stats, slots); err != nil {
			return err
		}
	}
	return nil
}

func holdJoinedSession(reader *wsReader, hold time.Duration, stats *soakStats, opts *soakOptions) {
	// Half-open without WebRTC: freeze the WS consumer but keep the socket open.
	if opts.halfOpen {
		stats.halfOpenFrozen.Add(1)
		time.Sleep(opts.halfOpenHold)
		return
	}
	endHold := time.Now().Add(hold)
	for time.Now().Before(endHold) {
		msg, err := reader.recv(250 * time.Millisecond)
		if err != nil {
			if errors.Is(err, errRecvTimeout) {
				continue
			}
			break
		}
		switch msg.Type {
		case "member_joined":
			stats.memberJoined.Add(1)
		case "member_left":
			stats.memberLeft.Add(1)
		}
	}
}

func runClient(
	id int,
	rng *rand.Rand,
	wsURL string,
	opts *soakOptions,
	stats *soakStats,
) {
	if opts.reuseUserID {
		if u, err := withTestUser(wsURL, fmt.Sprintf("soak-user-%d", id)); err == nil {
			wsURL = u
		}
	}
	for r := 0; r < opts.rounds; r++ {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			stats.connectFail.Add(1)
			time.Sleep(opts.stagger)
			continue
		}
		stats.connectOK.Add(1)
		reader := newWSReader(conn)

		msg, err := reader.recv(5 * time.Second)
		if err != nil || msg.Type != "welcome" {
			stats.wsReadErr.Add(1)
			_ = conn.Close()
			time.Sleep(opts.stagger)
			continue
		}
		stats.welcomeOK.Add(1)
		sendMu := &sync.Mutex{}

		joinReq := map[string]any{
			"type": "join_voice",
			"seq":  1,
			"data": joinVoiceData{
				CrewID:    opts.crewID,
				ChannelID: opts.channelID,
			},
		}
		start := time.Now()
		if err := mustSend(conn, sendMu, joinReq); err != nil {
			stats.joinFail.Add(1)
			_ = conn.Close()
			time.Sleep(opts.stagger)
			continue
		}

		joined := false
		joinRejected := false
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			msg, err := reader.recv(750 * time.Millisecond)
			if err != nil {
				if errors.Is(err, errRecvTimeout) {
					continue
				}
				stats.wsReadErr.Add(1)
				joinRejected = true
				stats.joinFail.Add(1)
				break
			}
			switch msg.Type {
			case "joined":
				joined = true
				stats.joinOK.Add(1)
				stats.recordJoinLatency(time.Since(start))
				break
			case "error":
				stats.joinFail.Add(1)
				var e struct {
					Code string `json:"code"`
				}
				_ = json.Unmarshal(msg.Data, &e)
				if e.Code == "" {
					e.Code = "UNKNOWN"
				}
				stats.recordJoinError(e.Code)
				joinRejected = true
				break
			case "member_joined":
				stats.memberJoined.Add(1)
			case "member_left":
				stats.memberLeft.Add(1)
			}
			if joined || joinRejected {
				break
			}
		}
		if !joined && !joinRejected {
			stats.joinTimeout.Add(1)
		}
		if joined {
			if opts.negotiateWebRTC {
				stats.webrtcAttempts.Add(1)
				err := runNegotiatedSession(conn, reader, sendMu, opts, stats, rng.Int63())
				if err != nil {
					if errors.Is(err, errPCConnectTimeout) {
						stats.webrtcTimeout.Add(1)
					} else {
						stats.webrtcFail.Add(1)
					}
				} else {
					stats.webrtcOK.Add(1)
				}
			} else {
				holdJoinedSession(reader, opts.hold, stats, opts)
			}

			if opts.gracefulLeave && !opts.halfOpen {
				_ = mustSend(conn, sendMu, map[string]any{
					"type": "leave",
					"seq":  2,
					"data": map[string]any{},
				})
			}
		}
		_ = conn.Close()
		// per-client jitter so reconnect storm is realistic rather than lockstep
		j := time.Duration(rng.Int63n(int64(opts.stagger + 1)))
		time.Sleep(opts.stagger + j/2)
	}

	_ = id
}

func runSoakOnce(opts *soakOptions) (*soakResult, error) {
	wsURL, err := endpointWithPassword(opts.endpoint, opts.adminPassword)
	if err != nil {
		return nil, fmt.Errorf("invalid endpoint: %w", err)
	}

	fmt.Printf("voice-soak: endpoint=%s clients=%d rounds=%d hold=%dms stagger=%dms seed=%d\n",
		opts.endpoint,
		opts.clients,
		opts.rounds,
		opts.hold/time.Millisecond,
		opts.stagger/time.Millisecond,
		opts.seed,
	)
	fmt.Printf("negotiate_webrtc: %v rtp_audio: %v\n", opts.negotiateWebRTC, opts.enableRTPAudio)

	stats := &soakStats{
		joinErrorCodes: make(map[string]int64),
		webrtcErrors:   make(map[string]int64),
	}
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < opts.clients; i++ {
		wg.Add(1)
		rng := rand.New(rand.NewSource(opts.seed + int64(i)*7919))
		go func(clientID int) {
			defer wg.Done()
			runClient(clientID, rng, wsURL, opts, stats)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	totalAttempts := int64(opts.clients * opts.rounds)
	success := stats.joinOK.Load()
	failures := stats.joinFail.Load() + stats.joinTimeout.Load()
	successRate := 0.0
	if totalAttempts > 0 {
		successRate = float64(success) / float64(totalAttempts)
	}

	avgJoinMs := 0.0
	if success > 0 {
		avgJoinMs = float64(stats.totalJoinNanos.Load()) / float64(success) / float64(time.Millisecond)
	}
	maxJoinMs := float64(stats.maxJoinNanos) / float64(time.Millisecond)

	fmt.Println("---- voice-soak summary ----")
	fmt.Printf("duration:            %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("attempts:            %d\n", totalAttempts)
	fmt.Printf("connect_ok:          %d\n", stats.connectOK.Load())
	fmt.Printf("connect_fail:        %d\n", stats.connectFail.Load())
	fmt.Printf("welcome_ok:          %d\n", stats.welcomeOK.Load())
	fmt.Printf("join_ok:             %d\n", success)
	fmt.Printf("join_fail:           %d\n", stats.joinFail.Load())
	fmt.Printf("join_timeout:        %d\n", stats.joinTimeout.Load())
	fmt.Printf("ws_read_err:         %d\n", stats.wsReadErr.Load())
	fmt.Printf("member_joined_ev:    %d\n", stats.memberJoined.Load())
	fmt.Printf("member_left_ev:      %d\n", stats.memberLeft.Load())
	if opts.halfOpen {
		fmt.Printf("half_open_frozen:    %d\n", stats.halfOpenFrozen.Load())
	}
	if opts.idleResume > 0 {
		fmt.Printf("idle_resume_rounds:  %d\n", stats.idleResumeRounds.Load())
	}
	fmt.Printf("join_success_rate:   %.2f%%\n", successRate*100.0)
	fmt.Printf("join_latency_avg_ms: %.2f\n", avgJoinMs)
	fmt.Printf("join_latency_max_ms: %.2f\n", maxJoinMs)
	fmt.Printf("join_not_ok_total:   %d\n", failures)

	if len(stats.joinErrorCodes) > 0 {
		fmt.Println("join_error_codes:")
		for code, n := range stats.joinErrorCodes {
			fmt.Printf("  %s: %d\n", code, n)
		}
	}

	pcAttempts := stats.webrtcAttempts.Load()
	pcOK := stats.webrtcOK.Load()
	pcRate := 0.0
	if pcAttempts > 0 {
		pcRate = float64(pcOK) / float64(pcAttempts)
	}
	if opts.negotiateWebRTC {
		fmt.Println("---- webrtc summary ----")
		fmt.Printf("webrtc_attempts:      %d\n", pcAttempts)
		fmt.Printf("webrtc_ok:            %d\n", pcOK)
		fmt.Printf("webrtc_fail:          %d\n", stats.webrtcFail.Load())
		fmt.Printf("webrtc_timeout:       %d\n", stats.webrtcTimeout.Load())
		fmt.Printf("pc_success_rate:      %.2f%%\n", pcRate*100.0)
		fmt.Printf("offer_tx:             %d\n", stats.webrtcOfferTx.Load())
		fmt.Printf("answer_rx:            %d\n", stats.webrtcAnswerRx.Load())
		fmt.Printf("reneg_offer_rx:       %d\n", stats.renegOfferRx.Load())
		fmt.Printf("reneg_answer_tx:      %d\n", stats.renegAnswerTx.Load())
		fmt.Printf("ice_cand_tx:          %d\n", stats.iceCandTx.Load())
		fmt.Printf("ice_cand_rx:          %d\n", stats.iceCandRx.Load())
		fmt.Printf("ice_tcp_only:         %v\n", opts.iceTCPOnly)
		fmt.Printf("pc_pair_udp:          %d\n", stats.pcPairUDP.Load())
		fmt.Printf("pc_pair_tcp:          %d\n", stats.pcPairTCP.Load())
		fmt.Printf("pc_pair_other:        %d\n", stats.pcPairOther.Load())
		fmt.Printf("recv_slots:           %d\n", opts.recvSlots)
		fmt.Printf("offers_within_slots:  %d\n", stats.offersWithinSlots.Load())
		fmt.Printf("offers_past_slots:    %d\n", stats.offersPastSlots.Load())
		fmt.Printf("slot_msg_rx:          %d\n", stats.slotMsgRx.Load())
		fmt.Printf("speaking_msg_tx:      %d\n", stats.speakingMsgTx.Load())
		fmt.Printf("speaking_msg_rx:      %d\n", stats.speakingMsgRx.Load())
		if opts.enableRTPAudio {
			elapsedSec := elapsed.Seconds()
			txPps := 0.0
			rxPps := 0.0
			if elapsedSec > 0 {
				txPps = float64(stats.rtpTxPackets.Load()) / elapsedSec
				rxPps = float64(stats.rtpRxPackets.Load()) / elapsedSec
			}
			fmt.Println("---- rtp summary ----")
			fmt.Printf("rtp_send_rounds:      %d\n", stats.rtpSendRounds.Load())
			fmt.Printf("rtp_tx_packets:       %d\n", stats.rtpTxPackets.Load())
			fmt.Printf("rtp_tx_bytes:         %d\n", stats.rtpTxBytes.Load())
			fmt.Printf("rtp_tx_errors:        %d\n", stats.rtpTxErrors.Load())
			fmt.Printf("rtp_tx_pps_eff:       %.2f\n", txPps)
			fmt.Printf("rtp_rx_tracks:        %d\n", stats.rtpRxTracks.Load())
			fmt.Printf("rtp_rx_packets:       %d\n", stats.rtpRxPackets.Load())
			fmt.Printf("rtp_rx_bytes:         %d\n", stats.rtpRxBytes.Load())
			fmt.Printf("rtp_rx_pps_eff:       %.2f\n", rxPps)
			fmt.Printf("rtp_rx_slot_mapped:   %d\n", stats.rtpRxMapped.Load())
			fmt.Printf("rtp_rx_unmapped_slot: %d\n", stats.rtpRxUnmapped.Load())
			fmt.Printf("rtp_rx_unmapped_own:  %d\n", stats.rtpRxUnmappedOwn.Load())
			fmt.Printf("rtp_rx_early_slot:    %d\n", stats.rtpRxEarlySlot.Load())
			fmt.Printf("rtp_rx_late_slot:     %d\n", stats.rtpRxLateSlot.Load())
			fmt.Printf("rtp_rx_early_own:     %d\n", stats.rtpRxEarlyOwn.Load())
			fmt.Printf("rtp_rx_late_own:      %d\n", stats.rtpRxLateOwn.Load())
			fmt.Printf("rtp_rx_early_lead_max_ms: %.1f\n", float64(stats.rtpRxEarlyLeadMax.Load())/float64(time.Millisecond))
			fmt.Printf("rtp_rx_undeclared:    %d\n", stats.rtpRxUndeclared.Load())
			fmt.Printf("rtp_source:           %s (vad_gaps=%v)\n", opts.rtpSource, opts.vadGaps)
		}
		if len(stats.webrtcErrors) > 0 {
			fmt.Println("webrtc_error_codes:")
			for code, n := range stats.webrtcErrors {
				fmt.Printf("  %s: %d\n", code, n)
			}
		}
	}

	passedJoinGate := successRate >= opts.minJoinSuccessRate
	passedPCGate := true
	if opts.negotiateWebRTC && pcAttempts > 0 {
		passedPCGate = pcRate >= opts.minPCConnectRate
	}
	passedSlotGate := true
	if opts.maxOffersWithinSlots >= 0 &&
		stats.offersWithinSlots.Load() > int64(opts.maxOffersWithinSlots) {
		passedSlotGate = false
		fmt.Printf("slot gate: %d offers within slots > %d\n",
			stats.offersWithinSlots.Load(), opts.maxOffersWithinSlots)
	}
	passedAll := passedJoinGate && passedPCGate && passedSlotGate
	if passedAll {
		fmt.Println("PASS")
	} else {
		fmt.Println("FAIL")
	}

	return &soakResult{
		totalAttempts:  totalAttempts,
		joinSuccess:    success,
		joinSuccessPct: successRate,
		pcAttempts:     pcAttempts,
		pcSuccess:      pcOK,
		pcSuccessPct:   pcRate,
		passedJoinGate: passedJoinGate,
		passedPCGate:   passedPCGate,
		passedSlotGate: passedSlotGate,
		passedAllGates: passedAll,
	}, nil
}

func runTier(base *soakOptions, clients int, seed int64, tierLabel string) (*soakResult, error) {
	runOpts := *base
	runOpts.clients = clients
	runOpts.seed = seed
	fmt.Printf("\n== %s: clients=%d ==\n", tierLabel, clients)
	result, err := runSoakOnce(&runOpts)
	if err != nil {
		fmt.Printf("%s_error: %v\n", tierLabel, err)
		return nil, err
	}
	fmt.Printf(
		"%s_result: clients=%d join=%.2f%% pc=%.2f%% pass=%v\n",
		tierLabel,
		clients,
		result.joinSuccessPct*100.0,
		result.pcSuccessPct*100.0,
		result.passedAllGates,
	)
	return result, nil
}

func main() {
	var (
		endpoint        = flag.String("endpoint", "ws://127.0.0.1:8443/ws", "SFU websocket endpoint")
		adminPassword   = flag.String("admin-password", "", "older name of --test-password")
		testPassword    = flag.String("test-password", "", "SFU test-mode password (or SFU_TEST_PASSWORD, then --admin-password, then SFU_ADMIN_PASSWORD)")
		crewID          = flag.String("crew", "soak-crew", "crew id")
		channelID       = flag.String("channel", "soak-voice", "voice channel id")
		clients         = flag.Int("clients", 20, "concurrent clients")
		rounds          = flag.Int("rounds", 25, "connect/join/leave cycles per client")
		holdMs          = flag.Int("hold-ms", 500, "time to stay joined per round")
		staggerMs       = flag.Int("stagger-ms", 150, "base jitter between rounds per client")
		negotiateWRTC   = flag.Bool("negotiate-webrtc", false, "perform full WebRTC offer/answer + ICE + renegotiation handling")
		pcTimeoutMs     = flag.Int("pc-connect-timeout-ms", 7000, "WebRTC connect timeout per round when --negotiate-webrtc is enabled")
		gracefulLeave   = flag.Bool("graceful-leave", true, "send leave message before disconnect")
		minSuccess      = flag.Float64("min-success-rate", 0.95, "minimum join success rate required")
		minPCRate       = flag.Float64("min-pc-connect-rate", 0.85, "minimum WebRTC connect success rate required in negotiate mode")
		enableRTPAudio  = flag.Bool("rtp-audio", false, "send synthetic Opus RTP during negotiated hold window")
		rtpPPS          = flag.Int("rtp-pps", 50, "RTP packets per second when --rtp-audio is enabled")
		rtpPayloadBytes = flag.Int("rtp-payload-bytes", 80, "RTP payload size in bytes when --rtp-audio is enabled")
		reuseUserID     = flag.Bool("reuse-user-id", false, "reuse a stable SFU identity per client across rounds (exercises same-user reconnect/eviction)")
		halfOpen        = flag.Bool("half-open", false, "after join, freeze the client (stop reading WS, keep PeerConnection alive) to simulate a sleep/wake zombie")
		halfOpenHoldMs  = flag.Int("half-open-hold-ms", 30000, "how long to stay frozen per round when --half-open is set")
		idleResumeMs    = flag.Int("idle-resume-ms", 0, "idle (no RTP) for this long after connect, then resume RTP (requires --negotiate-webrtc)")
		recvSlots       = flag.Int("recv-slots", 16, "recvonly receive slots per offer, as the mello client (0 = every sender on its own m-line)")
		rtpSource       = flag.String("rtp-source", "speech", "RTP payloads: speech (real Opus, real timestamps) or synthetic (--rtp-pps/--rtp-payload-bytes)")
		vadGaps         = flag.Bool("vad-gaps", false, "speech in talk spurts with silences, plus in-band speaking messages")
		iceTCPOnly      = flag.Bool("ice-tcp-only", false, "gather only TCP4 ICE candidates, as a client on a UDP-blocked network (needs the SFU passive ICE-TCP port)")
		maxOffersInSlot = flag.Int("max-offers-within-slots", -1, "fail when more offers than this reach clients that never exceeded their slots (-1 = no gate)")
		sweepClients    = flag.String("sweep-clients", "", "comma-separated client counts for sequential capacity sweep (example: 25,50,75,100)")
		autoSearch      = flag.Bool("auto-capacity-search", false, "auto-search max passing client tier using exponential ramp + binary refinement")
		searchStart     = flag.Int("search-start-clients", 10, "auto-search starting client count")
		searchMax       = flag.Int("search-max-clients", 200, "auto-search maximum client count to probe")
		searchGrowth    = flag.Float64("search-growth-factor", 1.6, "auto-search exponential growth factor (>1.0)")
		searchMaxRuns   = flag.Int("search-max-runs", 12, "maximum soak runs allowed during auto search")
		searchSafety    = flag.Float64("search-safety-factor", 0.85, "recommended safe tier multiplier applied to max passing tier (0,1]")
		seed            = flag.Int64("seed", time.Now().UnixNano(), "random seed")
	)
	flag.Parse()

	password := ""
	for _, v := range []string{*testPassword, os.Getenv("SFU_TEST_PASSWORD"), *adminPassword, os.Getenv("SFU_ADMIN_PASSWORD")} {
		if v = strings.TrimSpace(v); v != "" {
			password = v
			break
		}
	}
	if password == "" {
		fmt.Fprintln(os.Stderr, "missing --test-password (or SFU_TEST_PASSWORD)")
		os.Exit(2)
	}

	if *enableRTPAudio && !*negotiateWRTC {
		fmt.Fprintln(os.Stderr, "--rtp-audio requires --negotiate-webrtc")
		os.Exit(2)
	}
	if *idleResumeMs > 0 && !*negotiateWRTC {
		fmt.Fprintln(os.Stderr, "--idle-resume-ms requires --negotiate-webrtc")
		os.Exit(2)
	}
	if *halfOpenHoldMs <= 0 {
		fmt.Fprintln(os.Stderr, "--half-open-hold-ms must be > 0")
		os.Exit(2)
	}
	if *rtpPPS <= 0 {
		fmt.Fprintln(os.Stderr, "--rtp-pps must be > 0")
		os.Exit(2)
	}
	if *rtpPayloadBytes < 3 {
		fmt.Fprintln(os.Stderr, "--rtp-payload-bytes must be >= 3")
		os.Exit(2)
	}
	if *recvSlots < 0 || *recvSlots > 64 {
		fmt.Fprintln(os.Stderr, "--recv-slots must be in 0..64")
		os.Exit(2)
	}
	if *rtpSource != "speech" && *rtpSource != "synthetic" {
		fmt.Fprintln(os.Stderr, "--rtp-source must be speech or synthetic")
		os.Exit(2)
	}
	var speechPackets [][]byte
	if *rtpSource == "speech" {
		var err error
		if speechPackets, err = loadSpeechPackets(); err != nil {
			fmt.Fprintf(os.Stderr, "speech clip: %v\n", err)
			os.Exit(2)
		}
	}
	if *clients <= 0 || *rounds <= 0 {
		fmt.Fprintln(os.Stderr, "--clients and --rounds must be > 0")
		os.Exit(2)
	}
	if *minSuccess < 0 || *minSuccess > 1 || *minPCRate < 0 || *minPCRate > 1 {
		fmt.Fprintln(os.Stderr, "--min-success-rate and --min-pc-connect-rate must be between 0 and 1")
		os.Exit(2)
	}
	if *autoSearch && strings.TrimSpace(*sweepClients) != "" {
		fmt.Fprintln(os.Stderr, "--auto-capacity-search cannot be combined with --sweep-clients")
		os.Exit(2)
	}
	if *autoSearch {
		if *searchStart <= 0 {
			fmt.Fprintln(os.Stderr, "--search-start-clients must be > 0")
			os.Exit(2)
		}
		if *searchMax < *searchStart {
			fmt.Fprintln(os.Stderr, "--search-max-clients must be >= --search-start-clients")
			os.Exit(2)
		}
		if *searchGrowth <= 1.0 {
			fmt.Fprintln(os.Stderr, "--search-growth-factor must be > 1.0")
			os.Exit(2)
		}
		if *searchMaxRuns <= 0 {
			fmt.Fprintln(os.Stderr, "--search-max-runs must be > 0")
			os.Exit(2)
		}
		if *searchSafety <= 0.0 || *searchSafety > 1.0 {
			fmt.Fprintln(os.Stderr, "--search-safety-factor must be in (0,1]")
			os.Exit(2)
		}
	}

	opts := &soakOptions{
		endpoint:             *endpoint,
		adminPassword:        password,
		crewID:               *crewID,
		channelID:            *channelID,
		clients:              *clients,
		rounds:               *rounds,
		hold:                 time.Duration(*holdMs) * time.Millisecond,
		stagger:              time.Duration(*staggerMs) * time.Millisecond,
		negotiateWebRTC:      *negotiateWRTC,
		pcConnectTimeout:     time.Duration(*pcTimeoutMs) * time.Millisecond,
		gracefulLeave:        *gracefulLeave,
		minJoinSuccessRate:   *minSuccess,
		minPCConnectRate:     *minPCRate,
		seed:                 *seed,
		enableRTPAudio:       *enableRTPAudio,
		rtpPacketsPerSecond:  *rtpPPS,
		rtpPayloadBytes:      *rtpPayloadBytes,
		reuseUserID:          *reuseUserID,
		halfOpen:             *halfOpen,
		halfOpenHold:         time.Duration(*halfOpenHoldMs) * time.Millisecond,
		idleResume:           time.Duration(*idleResumeMs) * time.Millisecond,
		recvSlots:            *recvSlots,
		rtpSource:            *rtpSource,
		vadGaps:              *vadGaps,
		maxOffersWithinSlots: *maxOffersInSlot,
		iceTCPOnly:           *iceTCPOnly,
		speechPackets:        speechPackets,
	}

	if !*autoSearch && strings.TrimSpace(*sweepClients) == "" {
		result, err := runSoakOnce(opts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "run failed: %v\n", err)
			os.Exit(1)
		}
		if !result.passedJoinGate {
			fmt.Fprintf(
				os.Stderr,
				"FAIL: join success %.2f%% < %.2f%%\n",
				result.joinSuccessPct*100.0,
				opts.minJoinSuccessRate*100.0,
			)
			os.Exit(1)
		}
		if opts.negotiateWebRTC && result.pcAttempts > 0 && !result.passedPCGate {
			fmt.Fprintf(
				os.Stderr,
				"FAIL: pc success %.2f%% < %.2f%%\n",
				result.pcSuccessPct*100.0,
				opts.minPCConnectRate*100.0,
			)
			os.Exit(1)
		}
		if !result.passedSlotGate {
			fmt.Fprintln(os.Stderr, "FAIL: offers reached clients within their receive slots")
			os.Exit(1)
		}
		return
	}

	if *autoSearch {
		fmt.Println("==== auto capacity search ====")
		fmt.Printf(
			"start=%d max=%d growth=%.2f max_runs=%d safety=%.2f\n",
			*searchStart,
			*searchMax,
			*searchGrowth,
			*searchMaxRuns,
			*searchSafety,
		)
		fmt.Printf(
			"join gate: %.2f%% | pc gate: %.2f%%\n",
			opts.minJoinSuccessRate*100.0,
			opts.minPCConnectRate*100.0,
		)

		runCount := 0
		hadRunError := false
		lastPassing := 0
		firstFailing := 0
		current := *searchStart

		for runCount < *searchMaxRuns {
			result, runErr := runTier(
				opts,
				current,
				opts.seed+int64(runCount)*100003,
				"search-ramp",
			)
			runCount++
			if runErr != nil {
				hadRunError = true
				firstFailing = current
				break
			}
			if result.passedAllGates {
				lastPassing = current
				if current >= *searchMax {
					break
				}
				next := int(math.Ceil(float64(current) * *searchGrowth))
				if next <= current {
					next = current + 1
				}
				if next > *searchMax {
					next = *searchMax
				}
				current = next
				continue
			}

			firstFailing = current
			break
		}

		if lastPassing == 0 {
			fmt.Println("\n==== auto capacity search result ====")
			fmt.Printf("runs_executed: %d\n", runCount)
			fmt.Printf("max_passing_clients: %d\n", 0)
			fmt.Printf("recommended_safe_clients: %d\n", 0)
			fmt.Fprintln(os.Stderr, "FAIL: no passing capacity tier found")
			os.Exit(1)
		}

		if firstFailing != 0 && runCount < *searchMaxRuns {
			lo := lastPassing
			hi := firstFailing
			for lo+1 < hi && runCount < *searchMaxRuns {
				mid := lo + (hi-lo)/2
				result, runErr := runTier(
					opts,
					mid,
					opts.seed+int64(runCount)*100003,
					"search-binary",
				)
				runCount++
				if runErr != nil {
					hadRunError = true
					hi = mid
					continue
				}
				if result.passedAllGates {
					lo = mid
				} else {
					hi = mid
				}
			}
			lastPassing = lo
		}

		recommended := int(math.Floor(float64(lastPassing) * *searchSafety))
		if recommended < 1 {
			recommended = 1
		}
		fmt.Println("\n==== auto capacity search result ====")
		fmt.Printf("runs_executed: %d\n", runCount)
		fmt.Printf("max_passing_clients: %d\n", lastPassing)
		fmt.Printf("recommended_safe_clients: %d\n", recommended)
		if firstFailing != 0 {
			fmt.Printf("first_failing_clients: %d\n", firstFailing)
		} else {
			fmt.Printf("first_failing_clients: %s\n", "not-found-within-search-budget")
		}
		if hadRunError {
			fmt.Fprintln(os.Stderr, "FAIL: search encountered run errors")
			os.Exit(1)
		}
		return
	}

	sweepCounts, err := parseCSVInts(*sweepClients)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --sweep-clients: %v\n", err)
		os.Exit(2)
	}
	opts.sweepClientCounts = sweepCounts

	fmt.Println("==== capacity sweep ====")
	fmt.Printf("clients ladder: %v\n", opts.sweepClientCounts)
	fmt.Printf("join gate: %.2f%% | pc gate: %.2f%%\n", opts.minJoinSuccessRate*100.0, opts.minPCConnectRate*100.0)

	highestPassing := 0
	hadRunError := false
	for idx, c := range opts.sweepClientCounts {
		result, runErr := runTier(
			opts,
			c,
			opts.seed+int64(idx)*100003,
			"sweep-run",
		)
		if runErr != nil {
			hadRunError = true
			continue
		}
		if result.passedAllGates {
			highestPassing = c
		}
	}

	fmt.Println("\n==== capacity sweep result ====")
	fmt.Printf("highest_passing_clients: %d\n", highestPassing)
	if highestPassing == 0 || hadRunError {
		fmt.Fprintln(os.Stderr, "FAIL: no successful capacity tier met all gates")
		os.Exit(1)
	}
}
