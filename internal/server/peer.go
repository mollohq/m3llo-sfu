package server

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var (
	ErrNoPeerConnection         = errors.New("no PeerConnection")
	ErrNoRemoteDescription      = errors.New("remote description not set")
	ErrNoStreamVideoTransceiver = errors.New("no offered stream video transceiver")
	ErrStreamVideoTrackExists   = errors.New("stream video track already attached")
	ErrNoStreamVideoTrack       = errors.New("no stream video track")
	ErrStaleStreamVideoTrack    = errors.New("stale stream video track")
	ErrNoStreamAudioTransceiver = errors.New("no offered stream audio transceiver")
	ErrStreamAudioTrackExists   = errors.New("stream audio track already attached")
	ErrNoStreamAudioTrack       = errors.New("no stream audio track")
	ErrStaleStreamAudioTrack    = errors.New("stale stream audio track")
	ErrNilRTPPacket             = errors.New("nil RTP packet")
	ErrOfferPending             = errors.New("an offer is waiting for its answer")
)

// closeCallerStack returns a compact "->"-joined list of server-package call
// sites that led to Peer.Close, so logs reveal exactly which path tore the
// peer down (read-loop defer, ICE timer, eviction, pc-closed, etc.).
func closeCallerStack() string {
	var pcs [16]uintptr
	n := runtime.Callers(2, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])
	var out []string
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "m3llo-sfu/internal/server") {
			fn := frame.Function
			if i := strings.LastIndex(fn, "."); i >= 0 {
				fn = fn[i+1:]
			}
			out = append(out, fn)
		}
		if !more || len(out) >= 6 {
			break
		}
	}
	return strings.Join(out, "->")
}

type outgoingTrack struct {
	track *webrtc.TrackLocalStaticRTP
	ssrc  uint32
	// slot is the receive slot that carries this sender, nil when the
	// sender has its own m-line (past voiceRecvSlots).
	slot *voiceSlot
	// mid of the m-line that carries this sender; "" until known.
	mid string
}

type streamVideoLeg struct {
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	ctx    context.Context
	cancel context.CancelFunc
}

type streamAudioLeg struct {
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	ctx    context.Context
	cancel context.CancelFunc
}

const maxSendBufferBytes = 2 * 1024 * 1024 // 2 MB per peer
const bufferTimeoutSec = 5
const missingTrackWarnInterval = 5 * time.Second
const renegotiationAnswerTimeout = 12 * time.Second

// audioReaderStopTimeout bounds how long Close waits for the audio reader to
// return before it closes the PeerConnection anyway.
const audioReaderStopTimeout = 500 * time.Millisecond

type Peer struct {
	UserID      string
	Username    string // display name from JWT, may be empty
	Role        string // "host", "viewer", "member"
	WS          *websocket.Conn
	ConnectedAt time.Time
	// Synthetic marks test-mode connections (?test_password: the canary,
	// /sfu-test, soak tools). They are served normally but excluded from the
	// real-user numbers in /stats. Set once before the signaling loop starts.
	Synthetic bool

	PC                *webrtc.PeerConnection
	MediaChannel      *webrtc.DataChannel
	ControlChannel    *webrtc.DataChannel
	Session           Session // back-reference for cleanup
	pcMu              sync.RWMutex
	pcGeneration      uint64
	session           Session
	sessionGeneration uint64
	// estimator is the gcc estimator of PC (stream viewers only), nil
	// otherwise. Guarded by pcMu.
	estimator *viewerEstimator

	// RTP audio tracks (voice sessions)
	OutgoingTracks  map[string]*outgoingTrack // sender user_id -> outgoing track
	wiredAudioTrack *webrtc.TrackRemote       // actively-read incoming track, set by WireAudioTrack
	audioCancel     context.CancelFunc        // cancels the current ReadRTP goroutine
	audioReadActive bool                      // true while current ReadRTP loop is running
	audioReadToken  uint64                    // increments per wiring to identify active loop owner
	audioReadDone   chan struct{}             // closed when the current ReadRTP loop has returned
	tracksMu        sync.RWMutex

	// Voice receive slots (voice_slots.go). voiceSlots, slotAlloc and
	// slotsPC are guarded by tracksMu. slotMu serializes each slot change
	// with its control message, so the client sees them in order.
	voiceSlots map[string]*voiceSlot
	slotAlloc  *slotAllocator
	slotsPC    *webrtc.PeerConnection
	slotMu     sync.Mutex
	// controlTextHook replaces the control channel in tests.
	controlTextHook func(string) bool

	// One outgoing H.264 RTP leg for a stream viewer. This is intentionally
	// independent of the per-sender audio tracks used by voice sessions.
	streamVideoMu sync.RWMutex
	streamVideo   *streamVideoLeg
	streamAudioMu sync.RWMutex
	streamAudio   *streamAudioLeg

	missingTrackWarnMu sync.Mutex
	missingTrackWarnAt map[string]time.Time

	renegMu sync.Mutex // serializes Renegotiate / RestartICE
	// renegQueued is the PeerConnection on which a Renegotiate found an
	// offer still waiting for its answer. The renegotiation runs again when
	// that answer applies or the answer window times out. Guarded by renegMu.
	renegQueued *webrtc.PeerConnection
	wsMu        sync.Mutex // held by the one writer during a WS write

	// Every WebSocket write of the peer, in order; one goroutine at a time
	// writes them (drainSignalingQueue). Guarded by sigQueueMu.
	sigQueueMu      sync.Mutex
	sigQueue        []signalingWrite
	sigQueueRunning bool

	// ICE failure recovery
	iceTimerCallbackMu     sync.Mutex
	iceTimerMu             sync.Mutex
	iceFailTimer           *time.Timer
	iceFailTimerGeneration uint64
	iceFailTimerEpoch      uint64
	renegTimerMu           sync.Mutex
	renegTimer             *time.Timer

	logger    *logging.Logger
	closeOnce sync.Once

	// Stats
	PacketsSent     atomic.Int64
	PacketsReceived atomic.Int64
	BytesSent       atomic.Int64
	BytesReceived   atomic.Int64
	PacketsDropped  atomic.Int64
	BufferUsage     atomic.Int32 // percentage 0-100
	RTPWriteErrors  atomic.Int64

	// Observability counters
	RenegotiationCount atomic.Int32
	ICERestartCount    atomic.Int32
	TrackWireCount     atomic.Int32

	// True between sending a renegotiation offer and applying the client's
	// answer. While set, transient inbound-track read errors are tolerated far
	// longer (tracks are mid-swap) instead of tearing the audio reader down.
	renegotiating atomic.Bool

	// Slow-peer tracking
	bufferFullSince atomic.Pointer[time.Time]
	rtpErrSinceNs   atomic.Int64
	rtpErrStreak    atomic.Int32

	// Client telemetry rate-limit
	lastClientStats time.Time

	// cleanLeave is set when the client ends its session on purpose: a
	// leave message, or a WebSocket close with code 1000. A stream host
	// that leaves cleanly ends the stream at once; any other disconnect
	// holds the session (01-SFU.md §4.1).
	cleanLeave atomic.Bool

	// legacyAudioDCLogged is set once the SFU logged that it ignores this
	// peer's legacy "audio" DataChannel.
	legacyAudioDCLogged atomic.Bool

	// In-band speaking state from the voice control channel. Used to tell a
	// real RTP blackout (peer is speaking but the SFU receives nothing) apart
	// from normal VAD-gated silence (peer simply isn't transmitting).
	speaking atomic.Bool
}

func NewPeer(userID, role string, ws *websocket.Conn, logger *logging.Logger) *Peer {
	return &Peer{
		UserID:      userID,
		Role:        role,
		WS:          ws,
		ConnectedAt: time.Now(),
		logger:      logger,
	}
}

func (p *Peer) installPeerConnection(pc *webrtc.PeerConnection) uint64 {
	return p.installPeerConnectionWithEstimator(pc, nil)
}

// installPeerConnectionWithEstimator installs pc and the gcc estimator of
// its interceptor chain (nil when it has none).
func (p *Peer) installPeerConnectionWithEstimator(pc *webrtc.PeerConnection, estimator *viewerEstimator) uint64 {
	p.iceTimerCallbackMu.Lock()
	defer p.iceTimerCallbackMu.Unlock()
	p.iceTimerMu.Lock()
	p.pcMu.Lock()
	p.pcGeneration++
	p.PC = pc
	p.estimator = estimator
	generation := p.pcGeneration
	p.invalidateICEFailTimerLocked()
	p.pcMu.Unlock()
	p.iceTimerMu.Unlock()
	return generation
}

// viewerEstimate is the gcc target of the current PeerConnection, when its
// transport-cc feedback is fresh (01-SFU.md §6.1).
func (p *Peer) viewerEstimate(now time.Time) (float32, bool) {
	p.pcMu.RLock()
	estimator := p.estimator
	p.pcMu.RUnlock()
	if estimator == nil {
		return 0, false
	}
	return estimator.estimateAt(now)
}

func (p *Peer) currentPeerConnection() (*webrtc.PeerConnection, uint64) {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.PC, p.pcGeneration
}

func (p *Peer) isCurrentPeerConnection(pc *webrtc.PeerConnection, generation uint64) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.PC == pc && p.pcGeneration == generation
}

func (p *Peer) bindSession(session Session) uint64 {
	p.pcMu.Lock()
	defer p.pcMu.Unlock()
	p.sessionGeneration++
	p.session = session
	p.Session = session
	return p.sessionGeneration
}

func (p *Peer) clearSession(session Session) bool {
	p.pcMu.Lock()
	defer p.pcMu.Unlock()
	if p.session != session {
		return false
	}
	p.sessionGeneration++
	p.session = nil
	p.Session = nil
	return true
}

func (p *Peer) currentSession() (Session, uint64) {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.session, p.sessionGeneration
}

func (p *Peer) isCurrentSession(session Session, generation uint64) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.session == session && p.sessionGeneration == generation
}

func (p *Peer) isCurrentConnectionSession(
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
	session Session,
	sessionGeneration uint64,
) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.PC == pc &&
		p.pcGeneration == pcGeneration &&
		p.session == session &&
		p.sessionGeneration == sessionGeneration
}

func (p *Peer) setDataChannelForSession(
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
	session Session,
	sessionGeneration uint64,
	dc *webrtc.DataChannel,
	media bool,
) bool {
	p.pcMu.Lock()
	defer p.pcMu.Unlock()
	if p.PC != pc ||
		p.pcGeneration != pcGeneration ||
		p.session != session ||
		p.sessionGeneration != sessionGeneration {
		return false
	}
	if media {
		p.MediaChannel = dc
	} else {
		p.ControlChannel = dc
	}
	return true
}

func (p *Peer) wireDataChannelsForSession(
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
	session Session,
	sessionGeneration uint64,
	wire func(media, control *webrtc.DataChannel),
) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	if p.PC != pc ||
		p.pcGeneration != pcGeneration ||
		p.session != session ||
		p.sessionGeneration != sessionGeneration {
		return false
	}
	wire(p.MediaChannel, p.ControlChannel)
	return true
}

func (p *Peer) wireControlChannelForSession(
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
	session Session,
	sessionGeneration uint64,
	wire func(control *webrtc.DataChannel),
) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	if p.PC != pc ||
		p.pcGeneration != pcGeneration ||
		p.session != session ||
		p.sessionGeneration != sessionGeneration {
		return false
	}
	wire(p.ControlChannel)
	return true
}

func (p *Peer) discardPeerConnection(pc *webrtc.PeerConnection, generation uint64) bool {
	p.iceTimerMu.Lock()
	p.pcMu.Lock()
	if p.PC != pc || p.pcGeneration != generation {
		p.pcMu.Unlock()
		p.iceTimerMu.Unlock()
		return false
	}
	p.PC = nil
	p.pcGeneration++
	p.MediaChannel = nil
	p.ControlChannel = nil
	p.estimator = nil
	p.invalidateICEFailTimerLocked()
	p.pcMu.Unlock()
	p.iceTimerMu.Unlock()

	p.closeStreamVideo()
	p.closeStreamAudio()
	_ = pc.Close()
	return true
}

func (p *Peer) detachCurrentPeerConnection() *webrtc.PeerConnection {
	p.iceTimerMu.Lock()
	p.pcMu.Lock()
	pc := p.PC
	p.PC = nil
	p.pcGeneration++
	p.MediaChannel = nil
	p.ControlChannel = nil
	p.estimator = nil
	p.invalidateICEFailTimerLocked()
	p.pcMu.Unlock()
	p.iceTimerMu.Unlock()
	return pc
}

// SendMedia sends a media packet via the unreliable DataChannel.
// Non-blocking: drops the packet if the channel isn't open or buffer is full.
// Returns true only when the packet is handed to Pion for delivery.
func (p *Peer) SendMedia(data []byte) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	dc := p.MediaChannel
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return false
	}

	buffered := dc.BufferedAmount()
	if buffered+uint64(len(data)) > maxSendBufferBytes {
		p.PacketsDropped.Add(1)
		p.updateBufferPressure(buffered, true)
		return false
	}

	if err := dc.Send(data); err != nil {
		p.PacketsDropped.Add(1)
		return false
	}
	p.PacketsSent.Add(1)
	p.BytesSent.Add(int64(len(data)))
	p.updateBufferPressure(dc.BufferedAmount(), false)
	return true
}

// SendControl sends a control packet via the reliable DataChannel.
// Returns true only when the packet is handed to Pion for delivery.
func (p *Peer) SendControl(data []byte) bool {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	dc := p.ControlChannel
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return false
	}
	if err := dc.Send(data); err != nil {
		return false
	}
	p.PacketsSent.Add(1)
	p.BytesSent.Add(int64(len(data)))
	return true
}

// signalingWriteTimeout bounds one signaling write. A peer whose socket
// takes no data for this long is gone: without a deadline the write, and
// every write queued after it, blocked for good.
var signalingWriteTimeout = 5 * time.Second

// One writer per peer. gorilla/websocket allows one concurrent writer per
// connection. Every write of a peer (SendSignaling, SendSignalingQueued,
// SendClose, the welcome) goes into one ordered queue, and one goroutine
// at a time writes the queue in order. The order of the messages of a peer
// is the order in which they were queued, whichever call queued them.

// signalingWrite is one queued write: a message, or a close frame when msg
// is nil. done, when not nil, is closed after the write.
type signalingWrite struct {
	msg         *SignalingMessage
	closeCode   int
	closeReason string
	done        chan struct{}
}

// SendSignaling sends a signaling message via the WebSocket and returns
// when it is written (or failed). It is queued after every write that was
// queued before it. A write that does not complete in
// signalingWriteTimeout fails and closes the socket: the read loop then
// ends and cleans the peer up.
func (p *Peer) SendSignaling(msgType string, data interface{}) {
	p.writeAndWait(signalingWrite{msg: &SignalingMessage{Type: msgType, Data: mustMarshal(data)}})
}

// sendMessage is SendSignaling for a whole message (the welcome).
func (p *Peer) sendMessage(msg SignalingMessage) {
	p.writeAndWait(signalingWrite{msg: &msg})
}

// SendSignalingQueued queues a signaling message and returns at once. A
// caller can queue under a session lock: the per-peer order is then the
// order of the session changes, and a slow socket holds up only this
// peer's writes, not the caller.
func (p *Peer) SendSignalingQueued(msgType string, data interface{}) {
	p.enqueueWrite(signalingWrite{msg: &SignalingMessage{Type: msgType, Data: mustMarshal(data)}})
}

// SendClose sends a WebSocket close frame after every queued write, and
// returns when it is written.
func (p *Peer) SendClose(code int, reason string) {
	p.writeAndWait(signalingWrite{closeCode: code, closeReason: reason})
}

func (p *Peer) writeAndWait(w signalingWrite) {
	w.done = make(chan struct{})
	p.enqueueWrite(w)
	<-w.done
}

func (p *Peer) enqueueWrite(w signalingWrite) {
	p.sigQueueMu.Lock()
	p.sigQueue = append(p.sigQueue, w)
	if p.sigQueueRunning {
		p.sigQueueMu.Unlock()
		return
	}
	p.sigQueueRunning = true
	p.sigQueueMu.Unlock()
	go p.drainSignalingQueue()
}

// drainSignalingQueue is the one writer of the peer while the queue is not
// empty.
func (p *Peer) drainSignalingQueue() {
	for {
		p.sigQueueMu.Lock()
		if len(p.sigQueue) == 0 {
			p.sigQueueRunning = false
			p.sigQueueMu.Unlock()
			return
		}
		next := p.sigQueue[0]
		p.sigQueue[0] = signalingWrite{}
		p.sigQueue = p.sigQueue[1:]
		p.sigQueueMu.Unlock()
		p.writeSignaling(next)
		if next.done != nil {
			close(next.done)
		}
	}
}

// writeSignaling does one write. Only drainSignalingQueue calls it. wsMu
// marks the write in progress (tests hold it to stall a write).
func (p *Peer) writeSignaling(w signalingWrite) {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	if p.WS == nil {
		return
	}
	if w.msg == nil {
		_ = p.WS.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(w.closeCode, w.closeReason),
			time.Now().Add(1*time.Second),
		)
		return
	}
	_ = p.WS.SetWriteDeadline(time.Now().Add(signalingWriteTimeout))
	if err := p.WS.WriteJSON(w.msg); err != nil {
		if p.logger != nil {
			p.logger.Warn("signaling_write_failed", logging.D{
				"user":  p.UserID,
				"type":  w.msg.Type,
				"error": err.Error(),
			})
		}
		_ = p.WS.Close()
	}
}

// AddOutgoingTrack creates a TrackLocalStaticRTP for the given sender
// and adds it to this peer's PeerConnection.
func (p *Peer) AddOutgoingTrack(senderID string) (*webrtc.TrackLocalStaticRTP, error) {
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		"audio",
		senderID,
	)
	if err != nil {
		return nil, err
	}

	p.pcMu.RLock()
	pc := p.PC
	if pc == nil {
		p.pcMu.RUnlock()
		return nil, ErrNoPeerConnection
	}
	defer p.pcMu.RUnlock()
	// Use AddTransceiverFromTrack with SendOnly to force a NEW m-line.
	// AddTrack reuses the existing audio transceiver (changing recvonly→sendrecv),
	// which means onTrack never fires on the client for the reused m-line.
	t, err := pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		return nil, err
	}

	var ssrc uint32
	params := t.Sender().GetParameters()
	if len(params.Encodings) > 0 {
		ssrc = uint32(params.Encodings[0].SSRC)
	}

	p.tracksMu.Lock()
	if p.OutgoingTracks == nil {
		p.OutgoingTracks = make(map[string]*outgoingTrack)
	}
	p.OutgoingTracks[senderID] = &outgoingTrack{track: track, ssrc: ssrc}
	p.tracksMu.Unlock()

	p.missingTrackWarnMu.Lock()
	if p.missingTrackWarnAt != nil {
		delete(p.missingTrackWarnAt, senderID)
	}
	p.missingTrackWarnMu.Unlock()

	return track, nil
}

// RemoveOutgoingTrack removes the outgoing track for the given sender. A
// receive slot swaps back to its placeholder and needs no renegotiation. A
// sender on its own m-line has its transceiver stopped so the m-line goes
// inactive; the return value is true in that case only, and the caller must
// renegotiate.
func (p *Peer) RemoveOutgoingTrack(senderID string) bool {
	p.slotMu.Lock()
	defer p.slotMu.Unlock()

	p.tracksMu.Lock()
	ot := p.OutgoingTracks[senderID]
	delete(p.OutgoingTracks, senderID)
	p.tracksMu.Unlock()
	if ot == nil {
		return false
	}
	if ot.slot != nil {
		p.releaseVoiceSlot(senderID, ot)
		return false
	}

	pc, _ := p.currentPeerConnection()
	if pc != nil {
		for _, t := range pc.GetTransceivers() {
			sender := t.Sender()
			if sender != nil && sender.Track() == ot.track {
				t.Stop()
				break
			}
		}
	}
	if ot.mid != "" {
		p.SendControlText(encodeSlotMessage(ot.mid, ""))
	}
	return true
}

// AddStreamVideoTrack attaches one outgoing H.264 track to the video
// transceiver from the viewer's already-applied recvonly offer.
func (p *Peer) AddStreamVideoTrack() (*webrtc.TrackLocalStaticRTP, error) {
	pc, generation := p.currentPeerConnection()
	return p.addStreamVideoTrackFor(pc, generation)
}

func (p *Peer) addStreamVideoTrackFor(
	pc *webrtc.PeerConnection,
	generation uint64,
) (*webrtc.TrackLocalStaticRTP, error) {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	if pc == nil || p.PC != pc || p.pcGeneration != generation {
		return nil, ErrNoPeerConnection
	}
	if pc.RemoteDescription() == nil {
		return nil, ErrNoRemoteDescription
	}

	p.streamVideoMu.Lock()
	defer p.streamVideoMu.Unlock()

	if p.streamVideo != nil {
		return nil, ErrStreamVideoTrackExists
	}

	hasOfferedVideoTransceiver := false
	for _, transceiver := range pc.GetTransceivers() {
		if transceiver.Kind() == webrtc.RTPCodecTypeVideo && transceiver.Sender() == nil {
			hasOfferedVideoTransceiver = true
			break
		}
	}
	if !hasOfferedVideoTransceiver {
		return nil, ErrNoStreamVideoTransceiver
	}

	track, err := webrtc.NewTrackLocalStaticRTP(
		streamVideoCodecCapability(),
		"video",
		p.sessionIDLocked(),
	)
	if err != nil {
		return nil, err
	}

	// AddTrack reuses the transceiver created from the viewer's recvonly
	// offer instead of adding another video m-line.
	sender, err := pc.AddTrack(track)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.streamVideo = &streamVideoLeg{
		track:  track,
		sender: sender,
		ctx:    ctx,
		cancel: cancel,
	}
	return track, nil
}

// StreamVideoSender returns the sender and lifecycle context for the current
// stream-video leg. The context is cancelled before the sender is stopped.
func (p *Peer) StreamVideoSender() (*webrtc.RTPSender, context.Context) {
	p.streamVideoMu.RLock()
	defer p.streamVideoMu.RUnlock()
	if p.streamVideo == nil {
		return nil, nil
	}
	return p.streamVideo.sender, p.streamVideo.ctx
}

func (p *Peer) StreamVideoTrack() *webrtc.TrackLocalStaticRTP {
	p.streamVideoMu.RLock()
	defer p.streamVideoMu.RUnlock()
	if p.streamVideo == nil {
		return nil
	}
	return p.streamVideo.track
}

func (p *Peer) streamVideoLeg() (
	*webrtc.TrackLocalStaticRTP,
	*webrtc.RTPSender,
	context.Context,
) {
	p.streamVideoMu.RLock()
	defer p.streamVideoMu.RUnlock()
	if p.streamVideo == nil {
		return nil, nil, nil
	}
	return p.streamVideo.track,
		p.streamVideo.sender,
		p.streamVideo.ctx
}

// SendStreamVideoRTP forwards one source packet to this viewer. The source
// packet is cloned because TrackLocalStaticRTP may rewrite SSRC and payload
// type for the negotiated outgoing leg.
func (p *Peer) SendStreamVideoRTP(pkt *rtp.Packet) error {
	return p.SendStreamVideoRTPFor(nil, pkt)
}

// SendStreamVideoRTPFor writes only if expectedTrack still owns the viewer's
// active leg. Holding the read lock through WriteRTP prevents replacement from
// switching tracks mid-write.
func (p *Peer) SendStreamVideoRTPFor(expectedTrack *webrtc.TrackLocalStaticRTP, pkt *rtp.Packet) error {
	if pkt == nil {
		return ErrNilRTPPacket
	}

	p.streamVideoMu.RLock()
	defer p.streamVideoMu.RUnlock()
	if p.streamVideo == nil {
		return ErrNoStreamVideoTrack
	}
	track := p.streamVideo.track
	if expectedTrack != nil && track != expectedTrack {
		return ErrStaleStreamVideoTrack
	}

	outgoing := pkt.Clone()
	if err := track.WriteRTP(outgoing); err != nil {
		p.PacketsDropped.Add(1)
		p.RTPWriteErrors.Add(1)
		if p.rtpErrStreak.Add(1) == 1 {
			p.rtpErrSinceNs.Store(time.Now().UnixNano())
		}
		return err
	}

	p.rtpErrStreak.Store(0)
	p.rtpErrSinceNs.Store(0)
	p.PacketsSent.Add(1)
	p.BytesSent.Add(int64(pkt.MarshalSize()))
	return nil
}

func streamVideoCodecCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeH264,
		ClockRate:   90000,
		SDPFmtpLine: "profile-level-id=4d0033;packetization-mode=1;level-asymmetry-allowed=1",
		RTCPFeedback: []webrtc.RTCPFeedback{
			{Type: "nack"},
			{Type: "nack", Parameter: "pli"},
			{Type: "goog-remb"},
		},
	}
}

func (p *Peer) closeStreamVideo() {
	p.closeStreamVideoTrack(nil)
}

func (p *Peer) closeStreamVideoTrack(expectedTrack *webrtc.TrackLocalStaticRTP) bool {
	p.streamVideoMu.Lock()
	leg := p.streamVideo
	if leg == nil || (expectedTrack != nil && leg.track != expectedTrack) {
		p.streamVideoMu.Unlock()
		return false
	}
	p.streamVideo = nil
	p.streamVideoMu.Unlock()

	leg.cancel()
	if leg.sender != nil {
		_ = leg.sender.Stop()
	}
	return true
}

func (p *Peer) addStreamAudioTrackFor(
	pc *webrtc.PeerConnection,
	generation uint64,
) (*webrtc.TrackLocalStaticRTP, error) {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	if pc == nil || p.PC != pc || p.pcGeneration != generation {
		return nil, ErrNoPeerConnection
	}
	if pc.RemoteDescription() == nil {
		return nil, ErrNoRemoteDescription
	}

	p.streamAudioMu.Lock()
	defer p.streamAudioMu.Unlock()

	if p.streamAudio != nil {
		return nil, ErrStreamAudioTrackExists
	}

	hasOfferedAudioTransceiver := false
	for _, transceiver := range pc.GetTransceivers() {
		if transceiver.Kind() == webrtc.RTPCodecTypeAudio && transceiver.Sender() == nil {
			hasOfferedAudioTransceiver = true
			break
		}
	}
	if !hasOfferedAudioTransceiver {
		return nil, ErrNoStreamAudioTransceiver
	}

	track, err := webrtc.NewTrackLocalStaticRTP(
		streamAudioCodecCapability(),
		"audio",
		p.sessionIDLocked(),
	)
	if err != nil {
		return nil, err
	}

	sender, err := pc.AddTrack(track)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.streamAudio = &streamAudioLeg{
		track:  track,
		sender: sender,
		ctx:    ctx,
		cancel: cancel,
	}
	return track, nil
}

func (p *Peer) streamAudioLeg() (
	*webrtc.TrackLocalStaticRTP,
	*webrtc.RTPSender,
	context.Context,
) {
	p.streamAudioMu.RLock()
	defer p.streamAudioMu.RUnlock()
	if p.streamAudio == nil {
		return nil, nil, nil
	}
	return p.streamAudio.track, p.streamAudio.sender, p.streamAudio.ctx
}

func (p *Peer) SendStreamAudioRTPFor(
	expectedTrack *webrtc.TrackLocalStaticRTP,
	pkt *rtp.Packet,
) error {
	if pkt == nil {
		return ErrNilRTPPacket
	}
	p.streamAudioMu.RLock()
	leg := p.streamAudio
	p.streamAudioMu.RUnlock()
	if leg == nil || leg.track == nil {
		return ErrNoStreamAudioTrack
	}
	if expectedTrack != nil && leg.track != expectedTrack {
		return ErrStaleStreamAudioTrack
	}
	return leg.track.WriteRTP(pkt)
}

func (p *Peer) closeStreamAudio() {
	p.closeStreamAudioTrack(nil)
}

func (p *Peer) closeStreamAudioTrack(expectedTrack *webrtc.TrackLocalStaticRTP) bool {
	p.streamAudioMu.Lock()
	leg := p.streamAudio
	if leg == nil || (expectedTrack != nil && leg.track != expectedTrack) {
		p.streamAudioMu.Unlock()
		return false
	}
	p.streamAudio = nil
	p.streamAudioMu.Unlock()

	leg.cancel()
	if leg.sender != nil {
		_ = leg.sender.Stop()
	}
	return true
}

// SendRTP writes an RTP packet to the outgoing track for the given sender.
// Rewrites the SSRC to match the negotiated value so the receiver can demux.
func (p *Peer) SendRTP(senderID string, pkt *rtp.Packet) {
	p.tracksMu.RLock()
	ot := p.OutgoingTracks[senderID]
	outgoingTrackCount := len(p.OutgoingTracks)
	p.tracksMu.RUnlock()
	if ot == nil {
		if p.shouldLogMissingTrack(senderID) {
			sessionID := ""
			sessionType := ""
			if session, _ := p.currentSession(); session != nil {
				sessionID = session.ID()
				sessionType = session.Type()
			}
			p.logger.Warn("rtp_drop_missing_track", logging.D{
				"session":         sessionID,
				"session_type":    sessionType,
				"receiver":        p.UserID,
				"sender":          senderID,
				"outgoing_tracks": outgoingTrackCount,
			})
		}
		return
	}
	pkt.Header.SSRC = ot.ssrc
	out := pkt
	if ot.slot != nil {
		// pkt is shared by every receiver of this sender: rewrite a copy.
		slotPkt := *pkt
		slotPkt.Header.SequenceNumber = ot.slot.seq.next(pkt.Header.SequenceNumber)
		out = &slotPkt
	}
	if err := ot.track.WriteRTP(out); err != nil {
		p.PacketsDropped.Add(1)
		p.RTPWriteErrors.Add(1)
		if p.rtpErrStreak.Add(1) == 1 {
			p.rtpErrSinceNs.Store(time.Now().UnixNano())
		}
		return
	}
	p.rtpErrStreak.Store(0)
	p.rtpErrSinceNs.Store(0)
	p.PacketsSent.Add(1)
	p.BytesSent.Add(int64(pkt.MarshalSize()))
}

func (p *Peer) shouldLogMissingTrack(senderID string) bool {
	now := time.Now()

	p.missingTrackWarnMu.Lock()
	defer p.missingTrackWarnMu.Unlock()

	if p.missingTrackWarnAt == nil {
		p.missingTrackWarnAt = make(map[string]time.Time)
	}
	if last, ok := p.missingTrackWarnAt[senderID]; ok && now.Sub(last) < missingTrackWarnInterval {
		return false
	}
	p.missingTrackWarnAt[senderID] = now
	return true
}

// audioWiringIsCurrentLocked reports whether a track handed to us by OnTrack is
// already the one being read. Callers must hold tracksMu.
//
// Identity is the *TrackRemote itself, deliberately not its SSRC. When Pion
// replaces a receiver it fires OnTrack again with a new TrackRemote carrying
// the same SSRC, while the reader for the old, now-dead track is still burning
// down its error budget. Comparing SSRCs classified that live track as a
// duplicate and skipped it; moments later the dying reader cleared the wiring,
// leaving the peer with no reader at all and its microphone silent to everyone
// for the rest of the session.
func (p *Peer) audioWiringIsCurrentLocked(track *webrtc.TrackRemote) bool {
	return p.wiredAudioTrack == track &&
		p.audioCancel != nil &&
		p.audioReadActive
}

// Renegotiate creates a new SDP offer and sends it to the client.
// Serialized: only one renegotiation in flight per peer at a time.
func (p *Peer) Renegotiate() error {
	p.renegMu.Lock()
	defer p.renegMu.Unlock()

	// Announce new own m-lines after pcMu is released: slot messages take
	// slotMu, then pcMu, in the same order as every other slot change.
	var offered *webrtc.PeerConnection
	defer func() {
		if offered != nil {
			p.announceOverflowMids(offered)
		}
	}()

	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	pc := p.PC
	if pc == nil {
		return ErrNoPeerConnection
	}

	sessionID := p.sessionIDLocked()
	if state := pc.SignalingState(); state != webrtc.SignalingStateStable {
		// Pion cannot set a second local offer before the answer, and
		// CreateOffer alone marks new senders negotiated: the pending
		// answer would then start them on SSRCs that no m-line of the
		// client declares. Run this renegotiation after the answer.
		p.renegQueued = pc
		p.logger.Info("renegotiate_queued", logging.D{
			"session": sessionID,
			"user":    p.UserID,
			"state":   state.String(),
		})
		return nil
	}
	// This offer carries every change made so far, a queued one too.
	p.renegQueued = nil
	for _, t := range pc.GetTransceivers() {
		if t.Kind() != webrtc.RTPCodecTypeAudio {
			continue
		}
		senderTrackID := ""
		if sender := t.Sender(); sender != nil {
			if senderTrack := sender.Track(); senderTrack != nil {
				senderTrackID = senderTrack.ID()
			}
		}
		p.logger.Debug("transceiver_inventory", logging.D{
			"session":      sessionID,
			"user":         p.UserID,
			"mid":          t.Mid(),
			"direction":    t.Direction().String(),
			"sender_track": senderTrackID,
		})
	}

	// Open the reader-protection window before touching transceivers. Anything
	// we disturb below surfaces as read errors on the inbound audio track, and
	// those must be charged against the tolerant renegotiation budget rather
	// than the 50-error teardown limit.
	p.beginRenegotiation(renegotiationAnswerTimeout)
	p.stopPhantomTransceivers(pc)
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		p.finishRenegotiation()
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		p.finishRenegotiation()
		return err
	}
	p.RenegotiationCount.Add(1)
	offered = pc
	p.logger.Debug("renegotiate_offer", logging.D{
		"session":     sessionID,
		"user":        p.UserID,
		"sdp_len":     len(offer.SDP),
		"reneg_count": p.RenegotiationCount.Load(),
	})
	p.SendSignaling("offer", SDPData{SDP: offer.SDP})
	return nil
}

// RestartICE sends an ICE restart offer to attempt recovery without
// tearing down the entire PeerConnection.
func (p *Peer) RestartICE() error {
	pc, generation := p.currentPeerConnection()
	return p.restartICEFor(pc, generation)
}

func (p *Peer) restartICEFor(pc *webrtc.PeerConnection, generation uint64) error {
	p.renegMu.Lock()
	defer p.renegMu.Unlock()

	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	if pc == nil || p.PC != pc || p.pcGeneration != generation {
		return ErrNoPeerConnection
	}
	if pc.SignalingState() != webrtc.SignalingStateStable {
		// SetLocalDescription would fail; CreateOffer would still restart
		// ICE and mark new senders negotiated (see Renegotiate).
		return ErrOfferPending
	}

	offer, err := pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	if err != nil {
		return err
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return err
	}
	p.beginRenegotiation(renegotiationAnswerTimeout)
	p.ICERestartCount.Add(1)
	p.logger.Info("ice_restart_sent", logging.D{
		"user": p.UserID, "restart_count": p.ICERestartCount.Load(),
	})
	p.SendSignaling("offer", SDPData{SDP: offer.SDP})
	return nil
}

// StartICEFailTimerFor starts a generation-scoped timer for the given PC.
// Stale callbacks cannot create, replace, or fire against a newer PC.
func (p *Peer) StartICEFailTimerFor(
	pc *webrtc.PeerConnection,
	generation uint64,
	d time.Duration,
	onExpire func(),
) uint64 {
	if !p.isCurrentPeerConnection(pc, generation) {
		return 0
	}
	p.iceTimerCallbackMu.Lock()
	defer p.iceTimerCallbackMu.Unlock()
	p.iceTimerMu.Lock()
	defer p.iceTimerMu.Unlock()
	p.pcMu.RLock()
	current := p.PC == pc && p.pcGeneration == generation
	p.pcMu.RUnlock()
	if !current {
		return 0
	}
	p.invalidateICEFailTimerLocked()
	p.iceFailTimerGeneration = generation
	timerEpoch := p.iceFailTimerEpoch
	timerGen := generation
	p.iceFailTimer = time.AfterFunc(d, func() {
		p.expireICEFailTimerFor(pc, timerGen, timerEpoch, onExpire)
	})
	return timerEpoch
}

func (p *Peer) expireICEFailTimerFor(
	pc *webrtc.PeerConnection,
	generation uint64,
	epoch uint64,
	onExpire func(),
) bool {
	p.iceTimerCallbackMu.Lock()
	defer p.iceTimerCallbackMu.Unlock()
	p.iceTimerMu.Lock()
	p.pcMu.RLock()
	active := p.PC == pc &&
		p.pcGeneration == generation &&
		p.iceFailTimerGeneration == generation &&
		p.iceFailTimerEpoch == epoch
	p.pcMu.RUnlock()
	if !active {
		p.iceTimerMu.Unlock()
		return false
	}
	p.iceFailTimer = nil
	p.iceFailTimerEpoch++
	p.iceTimerMu.Unlock()
	onExpire()
	return true
}

// CancelICEFailTimerFor cancels the timer only when it still belongs to pc/generation.
func (p *Peer) CancelICEFailTimerFor(pc *webrtc.PeerConnection, generation uint64) {
	p.iceTimerCallbackMu.Lock()
	defer p.iceTimerCallbackMu.Unlock()
	p.iceTimerMu.Lock()
	defer p.iceTimerMu.Unlock()
	if p.iceFailTimer == nil || p.iceFailTimerGeneration != generation {
		return
	}
	p.pcMu.RLock()
	current := p.PC == pc && p.pcGeneration == generation
	p.pcMu.RUnlock()
	if !current {
		return
	}
	p.iceFailTimer.Stop()
	p.iceFailTimer = nil
	p.iceFailTimerEpoch++
}

// CancelICEFailTimer cancels any pending ICE failure timer.
func (p *Peer) CancelICEFailTimer() {
	p.iceTimerMu.Lock()
	defer p.iceTimerMu.Unlock()
	p.invalidateICEFailTimerLocked()
}

func (p *Peer) invalidateICEFailTimerLocked() {
	if p.iceFailTimer != nil {
		p.iceFailTimer.Stop()
		p.iceFailTimer = nil
	}
	p.iceFailTimerEpoch++
}

// StopPhantomTransceivers stops audio transceivers that were implicitly
// created by Pion's undeclared-SSRC handler. These "phantom" transceivers
// always appear as sendrecv m-lines (Pion never auto-creates recvonly).
// Without cleanup they accumulate on each renegotiation and cause ICE flapping.
//
// Legitimate audio transceivers we must preserve:
//   - every receive slot (BindVoiceSlots), whatever its direction or track
//   - sendonly: every outgoing track we added via AddTransceiverFromTrack
//   - recvonly: the original m-line from the client's offer (carries
//     the client's mic into the SFU)
//
// We therefore stop only sendrecv audio transceivers whose sender track
// is not one of ours. We previously matched the recvonly transceiver by
// Receiver().Track() pointer identity against peer.wiredAudioTrack, but
// that pointer can change across renegotiations and the strict mismatch
// caused the legit recvonly to be stopped, killing the peer's incoming
// audio for the rest of the session.
//
// Not every sendrecv audio transceiver is a phantom, though. A client whose
// offer omits `a=ssrc` for its audio m-line cannot have its mic bound to the
// negotiated m-line, so Pion routes it through handleUndeclaredSSRC, which
// creates a *sendrecv* transceiver with a Pion-generated send track and fires
// OnTrack on it. That transceiver is indistinguishable from a phantom by
// shape, yet it is the only thing carrying the member's microphone. Stopping
// it killed the audio reader on every join/leave. We therefore additionally
// exclude whichever transceiver is feeding the peer's currently wired inbound
// track. Stale duplicates are still stopped, so phantoms cannot accumulate.
func (p *Peer) StopPhantomTransceivers() {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	pc := p.PC
	if pc == nil {
		return
	}
	p.stopPhantomTransceivers(pc)
}

// stopPhantomTransceivers is StopPhantomTransceivers for pc. Callers hold
// pcMu.
func (p *Peer) stopPhantomTransceivers(pc *webrtc.PeerConnection) {
	p.tracksMu.RLock()
	outgoingTracks := make([]*webrtc.TrackLocalStaticRTP, 0, len(p.OutgoingTracks))
	for _, ot := range p.OutgoingTracks {
		if ot != nil && ot.track != nil {
			outgoingTracks = append(outgoingTracks, ot.track)
		}
	}
	slotSenders := make(map[*webrtc.RTPSender]struct{}, len(p.voiceSlots))
	for _, slot := range p.voiceSlots {
		slotSenders[slot.sender] = struct{}{}
	}
	liveInbound := p.wiredAudioTrack
	p.tracksMu.RUnlock()

	for _, t := range pc.GetTransceivers() {
		if t.Kind() != webrtc.RTPCodecTypeAudio {
			continue
		}
		var senderTrack webrtc.TrackLocal
		isSlot := false
		if sender := t.Sender(); sender != nil {
			senderTrack = sender.Track()
			_, isSlot = slotSenders[sender]
		}
		var receiverTrack *webrtc.TrackRemote
		if receiver := t.Receiver(); receiver != nil {
			receiverTrack = receiver.Track()
		}
		dir := t.Direction()
		if !isPhantomAudioTransceiver(
			dir, senderTrack, receiverTrack, outgoingTracks, liveInbound, isSlot,
		) {
			continue
		}

		p.logger.Info("phantom_stopped", logging.D{
			"session":   p.sessionIDLocked(),
			"user":      p.UserID,
			"mid":       t.Mid(),
			"direction": dir.String(),
		})
		_ = t.Stop()
	}
}

// isPhantomAudioTransceiver decides whether one audio transceiver is a
// leftover of Pion's undeclared-SSRC handler that we can safely stop.
//
// Kept separate from the PeerConnection walk so the rule can be exercised
// directly; the shapes it distinguishes are subtle and getting one wrong
// silently kills a member's audio for the rest of the call.
func isPhantomAudioTransceiver(
	dir webrtc.RTPTransceiverDirection,
	senderTrack webrtc.TrackLocal,
	receiverTrack *webrtc.TrackRemote,
	outgoingTracks []*webrtc.TrackLocalStaticRTP,
	liveInbound *webrtc.TrackRemote,
	isSlot bool,
) bool {
	if dir == webrtc.RTPTransceiverDirectionInactive {
		return false
	}
	// A receive slot is load-bearing for its whole life, whichever track it
	// carries now (01-SFU.md §6.2).
	if isSlot {
		return false
	}
	// sendonly = an outgoing track we added; recvonly = the client's original
	// audio m-line. Only sendrecv can be a phantom.
	if dir != webrtc.RTPTransceiverDirectionSendrecv {
		return false
	}
	// Whatever is currently delivering the peer's mic is load-bearing, even
	// though Pion gave it the same sendrecv shape as a phantom.
	if liveInbound != nil && receiverTrack == liveInbound {
		return false
	}
	// One of our own outgoing tracks that has been answered as sendrecv.
	if senderTrack != nil {
		for _, outTrack := range outgoingTracks {
			if webrtc.TrackLocal(outTrack) == senderTrack {
				return false
			}
		}
	}
	return true
}

// sessionID returns the current session ID for log correlation, "" if none.
// It takes pcMu for read. A caller that holds pcMu must use sessionIDLocked.
func (p *Peer) sessionID() string {
	session, _ := p.currentSession()
	if session == nil {
		return ""
	}
	return session.ID()
}

// sessionIDLocked is sessionID for a caller that holds pcMu. Do not take
// pcMu for read two times in one goroutine: if Close waits for the write
// lock between the two, Go blocks the second read lock behind it, and the
// two goroutines wait forever.
func (p *Peer) sessionIDLocked() string {
	if p.session == nil {
		return ""
	}
	return p.session.ID()
}

func (p *Peer) Close() {
	p.closeOnce.Do(func() {
		if p.logger != nil {
			sid := ""
			if session, _ := p.currentSession(); session != nil {
				sid = session.ID()
			}
			p.logger.Info("peer_close_called", logging.D{
				"session": sid,
				"user":    p.UserID,
				"caller":  closeCallerStack(),
			})
		}
		p.CancelICEFailTimer()
		p.finishRenegotiation()
		p.closeStreamVideo()
		p.closeStreamAudio()

		// Detach under slotMu: a slot change in progress finishes first,
		// and no slot ReplaceTrack or transceiver Stop starts on this
		// PeerConnection after it, so none can overlap pc.Close.
		p.slotMu.Lock()
		pc := p.detachCurrentPeerConnection()
		p.tracksMu.Lock()
		p.resetVoiceSlotsLocked()
		p.slotsPC = nil
		cancel, track, done := p.audioCancel, p.wiredAudioTrack, p.audioReadDone
		p.tracksMu.Unlock()
		p.slotMu.Unlock()

		// The audio reader returns before pc.Close stops its receiver.
		p.stopAudioReader(cancel, track, done)
		if pc != nil {
			pc.Close()
		}
		p.SendClose(websocket.CloseNormalClosure, "server_close")
		if p.WS != nil {
			p.WS.Close()
		}
	})
}

// stopAudioReader cancels the audio read loop, unblocks its ReadRTP with a
// read deadline and waits until the loop returns, up to
// audioReaderStopTimeout.
func (p *Peer) stopAudioReader(cancel context.CancelFunc, track *webrtc.TrackRemote, done <-chan struct{}) {
	if cancel == nil {
		return
	}
	cancel()
	if track != nil {
		_ = track.SetReadDeadline(time.Now())
	}
	if done == nil {
		return
	}
	select {
	case <-done:
	case <-time.After(audioReaderStopTimeout):
		if p.logger != nil {
			p.logger.Warn("audio_reader_stop_timeout", logging.D{
				"session": p.sessionID(),
				"user":    p.UserID,
			})
		}
	}
}

func (p *Peer) beginRenegotiation(timeout time.Duration) {
	p.renegotiating.Store(true)
	p.renegTimerMu.Lock()
	defer p.renegTimerMu.Unlock()
	if p.renegTimer != nil {
		p.renegTimer.Stop()
	}
	p.renegTimer = time.AfterFunc(timeout, func() {
		if !p.renegotiating.Load() {
			return
		}
		p.renegotiating.Store(false)
		if p.logger != nil {
			p.logger.Warn("renegotiation_timeout_clear", logging.D{
				"session": p.sessionID(),
				"user":    p.UserID,
				"timeout": timeout.String(),
			})
		}
		pc, _ := p.currentPeerConnection()
		p.renegMu.Lock()
		queued := p.takeQueuedRenegotiationLocked(pc)
		p.renegMu.Unlock()
		if queued {
			p.runQueuedRenegotiation("answer_timeout")
		}
	})
}

// takeQueuedRenegotiationLocked reports whether a renegotiation is queued
// for pc, and clears it. Callers hold renegMu.
func (p *Peer) takeQueuedRenegotiationLocked(pc *webrtc.PeerConnection) bool {
	if pc == nil || p.renegQueued != pc {
		return false
	}
	p.renegQueued = nil
	return true
}

// runQueuedRenegotiation runs a renegotiation that collided with a pending
// offer. If the offer is still pending, Renegotiate queues it again.
func (p *Peer) runQueuedRenegotiation(reason string) {
	err := p.Renegotiate()
	if p.logger == nil {
		return
	}
	if err != nil {
		p.logger.Warn("renegotiate_retry_failed", logging.D{
			"session": p.sessionID(),
			"user":    p.UserID,
			"reason":  reason,
			"error":   err.Error(),
		})
		return
	}
	p.logger.Info("renegotiate_retry", logging.D{
		"session": p.sessionID(),
		"user":    p.UserID,
		"reason":  reason,
	})
}

func (p *Peer) finishRenegotiation() {
	p.renegotiating.Store(false)
	p.renegTimerMu.Lock()
	defer p.renegTimerMu.Unlock()
	if p.renegTimer != nil {
		p.renegTimer.Stop()
		p.renegTimer = nil
	}
}

func (p *Peer) updateBufferPressure(buffered uint64, overLimit bool) {
	pct := int32(float64(buffered) / float64(maxSendBufferBytes) * 100)
	if pct > 100 {
		pct = 100
	}
	p.BufferUsage.Store(pct)

	if overLimit {
		if p.bufferFullSince.Load() == nil {
			now := time.Now()
			p.bufferFullSince.Store(&now)
		}
	} else {
		p.bufferFullSince.Store(nil)
	}
}

// IsSlowPeer returns true if the send buffer has been over limit for > bufferTimeoutSec.
func (p *Peer) IsSlowPeer() bool {
	since := p.bufferFullSince.Load()
	if since == nil {
		return false
	}
	return time.Since(*since) > time.Duration(bufferTimeoutSec)*time.Second
}

// IsRTPStalled returns true if RTP track writes have continuously failed
// for longer than the slow-peer timeout window.
func (p *Peer) IsRTPStalled() bool {
	sinceNs := p.rtpErrSinceNs.Load()
	if sinceNs == 0 {
		return false
	}
	// Ignore short bursts; sustained write failures indicate a broken leg.
	if p.rtpErrStreak.Load() < 50 {
		return false
	}
	return time.Since(time.Unix(0, sinceNs)) > time.Duration(bufferTimeoutSec)*time.Second
}

// DisplayName returns username if available, otherwise the raw user ID.
func (p *Peer) DisplayName() string {
	if p.Username != "" {
		return p.Username
	}
	return p.UserID
}

// Stats returns a snapshot of this peer's statistics.
func (p *Peer) Stats() PeerStats {
	stats := PeerStats{
		UserID:             p.UserID,
		Username:           p.Username,
		Role:               p.Role,
		ConnectedAt:        p.ConnectedAt,
		PacketsSent:        p.PacketsSent.Load(),
		PacketsReceived:    p.PacketsReceived.Load(),
		BytesSent:          p.BytesSent.Load(),
		BytesReceived:      p.BytesReceived.Load(),
		PacketsDropped:     p.PacketsDropped.Load(),
		BufferPercent:      int(p.BufferUsage.Load()),
		ConnectedDuration:  int(time.Since(p.ConnectedAt).Seconds()),
		RenegotiationCount: int(p.RenegotiationCount.Load()),
		ICERestartCount:    int(p.ICERestartCount.Load()),
		TrackWireCount:     int(p.TrackWireCount.Load()),
		RTPWriteErrors:     p.RTPWriteErrors.Load(),
		RTPErrStreak:       int(p.rtpErrStreak.Load()),
	}
	if pc, _ := p.currentPeerConnection(); pc != nil {
		stats.ConnectionState = pc.ConnectionState().String()
		stats.ICEState = pc.ICEConnectionState().String()
		stats.TransceiverCount = len(pc.GetTransceivers())
	}
	p.tracksMu.RLock()
	stats.OutgoingTrackCount = len(p.OutgoingTracks)
	if p.wiredAudioTrack != nil {
		stats.AudioTrackSSRC = uint32(p.wiredAudioTrack.SSRC())
	}
	p.tracksMu.RUnlock()
	return stats
}

type PeerStats struct {
	UserID             string    `json:"user_id"`
	Username           string    `json:"username"`
	Role               string    `json:"role"`
	ConnectedAt        time.Time `json:"connected_at"`
	PacketsSent        int64     `json:"packets_sent"`
	PacketsReceived    int64     `json:"packets_received"`
	BytesSent          int64     `json:"bytes_sent"`
	BytesReceived      int64     `json:"bytes_received"`
	PacketsDropped     int64     `json:"packets_dropped"`
	BufferPercent      int       `json:"buffer_usage_percent"`
	ConnectionState    string    `json:"connection_state"`
	ICEState           string    `json:"ice_state"`
	TransceiverCount   int       `json:"transceiver_count"`
	OutgoingTrackCount int       `json:"outgoing_track_count"`
	AudioTrackSSRC     uint32    `json:"audio_track_ssrc"`
	RenegotiationCount int       `json:"renegotiation_count"`
	ICERestartCount    int       `json:"ice_restart_count"`
	TrackWireCount     int       `json:"track_wire_count"`
	RTPWriteErrors     int64     `json:"rtp_write_errors"`
	RTPErrStreak       int       `json:"rtp_error_streak"`
	ConnectedDuration  int       `json:"connected_duration_sec"`
}
