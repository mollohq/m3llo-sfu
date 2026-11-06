package server

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
	"github.com/mollohq/m3llo-sfu/internal/snapshot"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

var (
	ErrHostAlreadyConnected = errors.New("host already connected")
	ErrSessionFull          = errors.New("session at capacity")
	ErrInvalidRole          = errors.New("invalid role")
	ErrSessionClosed        = errors.New("session closed")
)

const (
	streamVideoQueueSize      = 64
	streamSequenceWindowSize  = 1024
	streamFeedbackMinInterval = 200 * time.Millisecond
	streamREMBFreshness       = 2 * time.Second
)

type streamVideoIngress struct {
	peer              *Peer
	track             *webrtc.TrackRemote
	pc                *webrtc.PeerConnection
	pcGeneration      uint64
	sessionGeneration uint64
	token             uint64
	cancel            func()
}

type streamViewerVideo struct {
	peer   *Peer
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *rtp.Packet
	// queueHeadAt is the time (Unix nanoseconds) when the queue last became
	// not empty, 0 while it is empty. The fan-out and the writer goroutine
	// both write it, so it is atomic.
	queueHeadAt atomic.Int64

	// live is false until the viewer's PeerConnection is connected. Before
	// that, Pion's SRTP writer drops every packet, so the fan-out skips the
	// leg. Guarded by StreamSession.videoMu.
	live bool
	// connected is closed when live becomes true. replay is the cached IDR
	// access unit at that moment. The writer sends it before the queue, as
	// one burst outside the queue cap.
	connected chan struct{}
	replay    []*rtp.Packet

	// budget is the slow state of the leg (stream_slow_viewer.go).
	budget streamViewerBudget
	// loss counts the packets sent on the leg and those the viewer NACKed
	// (stream_slow_viewer.go).
	loss streamLegLoss
	// twccOffered: 0 unknown, 1 the viewer's description has no
	// transport-cc, 2 it has (offersTWCC).
	twccOffered atomic.Int32
}

// offersTWCC reports whether the viewer's description has the transport-cc
// header extension. Such a viewer's REMB is no estimate for the upstream
// ceiling: libmello sends its receive target (4 Mbps by default) as REMB at
// connect, before its first transport-cc feedback.
func (leg *streamViewerVideo) offersTWCC() bool {
	switch leg.twccOffered.Load() {
	case 1:
		return false
	case 2:
		return true
	}
	pc, _ := leg.peer.currentPeerConnection()
	if pc == nil {
		return false
	}
	desc := pc.RemoteDescription()
	if desc == nil {
		return false
	}
	offered := int32(1)
	if strings.Contains(desc.SDP, transportCCExtensionURI) {
		offered = 2
	}
	leg.twccOffered.Store(offered)
	return offered == 2
}

type viewerDrainCleanup struct {
	viewer *Peer
	track  *webrtc.TrackLocalStaticRTP
	cancel context.CancelFunc
}

type streamREMBEstimate struct {
	bitrate float32
	at      time.Time
}

type streamFeedbackState struct {
	// PLI coalescing is per viewer: a second viewer's genuine keyframe need
	// must not be swallowed by an unrelated viewer's recent PLI.
	lastPLI         map[string]time.Time
	lastREMBForward time.Time
	remb            map[string]streamREMBEstimate
	// gcc holds the fresh gcc target of each viewer whose transport-cc
	// feedback reaches the SFU's estimator (viewer_bwe.go).
	gcc map[string]streamREMBEstimate
	// slow marks the viewers whose estimate counts as at least
	// streamSlowViewerFloorBps: the slow viewers, and the viewers whose
	// estimate has been under the floor for streamSlowViewerFloorHold
	// (stream_slow_viewer.go).
	slow map[string]bool
	// twcc marks the viewers whose transport-cc is negotiated: their REMB
	// is no estimate for the ceiling (stream_congestion.go).
	twcc map[string]bool
	// constrained holds the viewers that went slow, until their estimate
	// reaches the host rate they could not carry (stream_congestion.go).
	constrained map[string]*streamConstrained
	// The host ingress rate, from the byte counter at each stats tick.
	ingressBps   float32
	ingressBytes int64
	ingressAt    time.Time
}

// streamSequenceWindow rejects duplicate/too-old RTP sequence numbers and, as a
// side effect, tracks enough state to report ingress loss RFC 3550-style:
// expected = (cycles * 65536 + highest) - base + 1, lost = expected - received.
//
// Loss is derived rather than counted per gap because reordering would otherwise
// register as loss and then never be reclaimed when the straggler arrives.
type streamSequenceWindow struct {
	initialized bool
	highest     uint16
	base        uint16
	cycles      uint32
	received    int64
	seen        [streamSequenceWindowSize / 64]uint64
}

// Expected reports how many packets should have arrived between the first and
// highest sequence numbers observed, inclusive.
func (w *streamSequenceWindow) Expected() int64 {
	if !w.initialized {
		return 0
	}
	return int64(uint64(w.cycles)*65536 + uint64(w.highest) - uint64(w.base) + 1)
}

// Lost reports expected-minus-received. Never negative: duplicates are rejected
// by Accept before they reach the received tally.
func (w *streamSequenceWindow) Lost() int64 {
	lost := w.Expected() - w.received
	if lost < 0 {
		return 0
	}
	return lost
}

type StreamRelaySizeBuckets struct {
	LE1200  int64
	LE8000  int64
	LE20000 int64
	LE40000 int64
	GT40000 int64
}

type StreamRelaySnapshot struct {
	MediaInPackets         int64
	MediaInBytes           int64
	MediaForwardAttempts   int64
	MediaForwardedPackets  int64
	MediaForwardedBytes    int64
	MediaDroppedPackets    int64
	MediaDroppedBytes      int64
	ControlInPackets       int64
	ControlInBytes         int64
	ControlForwarded       int64
	ControlDropped         int64
	KeyframeLikeMessages   int64
	MaxMediaMessageBytes   int64
	SizeBuckets            StreamRelaySizeBuckets
	RTPInPackets           int64
	RTPInBytes             int64
	RTPFanoutAttempts      int64
	RTPForwardedPackets    int64
	RTPForwardedBytes      int64
	RTPDroppedPackets      int64
	RTPDroppedBytes        int64
	RTPQueueDroppedPackets int64
	RTPQueueDroppedBytes   int64
	RTPDuplicatePackets    int64
	RTPRTXRecovered        int64
	RTPRTXDuplicate        int64
	RTPIngressExpected     int64
	RTPIngressLost         int64
	RTPViewerRTCPInvalid   int64
	// RTPSlowViewerDroppedPackets counts packets that a slow viewer's
	// queue did not take (keyframes only).
	RTPSlowViewerDroppedPackets int64
	// RTPViewerNACKedPackets counts the packets sent on viewer legs that
	// the viewers NACKed, each sequence number of a leg once.
	RTPViewerNACKedPackets int64
	// KeyframeRequests counts the PLIs to the host by cause.
	KeyframeRequests StreamKeyframeRequests
}

// StreamKeyframeRequests counts keyframe requests to the host by cause.
type StreamKeyframeRequests struct {
	Viewer int64 `json:"viewer"`
	Join   int64 `json:"join"`
	Slow   int64 `json:"slow"`
	Rescue int64 `json:"rescue"`
}

type StreamSession struct {
	id         string
	crewID     string
	state      SessionState
	host       *Peer
	viewers    *PeerSet
	createdAt  time.Time
	drainingAt time.Time
	logger     *logging.Logger

	hadRealPeer atomic.Bool

	mediaInPackets        atomic.Int64
	mediaInBytes          atomic.Int64
	mediaForwardAttempts  atomic.Int64
	mediaForwardedPackets atomic.Int64
	mediaForwardedBytes   atomic.Int64
	mediaDroppedPackets   atomic.Int64
	mediaDroppedBytes     atomic.Int64

	controlInPackets atomic.Int64
	controlInBytes   atomic.Int64
	controlForwarded atomic.Int64
	controlDropped   atomic.Int64

	keyframeLikeMessages atomic.Int64
	maxMediaMessageBytes atomic.Int64

	sizeLE1200  atomic.Int64
	sizeLE8000  atomic.Int64
	sizeLE20000 atomic.Int64
	sizeLE40000 atomic.Int64
	sizeGT40000 atomic.Int64

	rtpInPackets           atomic.Int64
	rtpInBytes             atomic.Int64
	rtpFanoutAttempts      atomic.Int64
	rtpForwardedPackets    atomic.Int64
	rtpForwardedBytes      atomic.Int64
	rtpDroppedPackets      atomic.Int64
	rtpDroppedBytes        atomic.Int64
	rtpQueueDroppedPackets atomic.Int64
	rtpQueueDroppedBytes   atomic.Int64
	rtpDuplicatePackets    atomic.Int64
	// Host RTX repairs (RFC 4588, unwrapped by Pion): forwarded, and dropped
	// because the original had arrived.
	rtpRTXRecoveredPackets atomic.Int64
	rtpRTXDuplicatePackets atomic.Int64
	// Packets not queued to a slow viewer (not part of an IDR access unit).
	rtpSlowViewerDroppedPackets atomic.Int64
	// Viewer RTCP packets that the interceptor chain could not parse.
	rtpViewerRTCPInvalid atomic.Int64
	// Packets sent on a viewer leg that the viewer NACKed, each once.
	rtpViewerNACKedPackets atomic.Int64

	// Host->SFU ingress loss on the media video stream. Without these, a
	// collapse in rtp_in_packets is ambiguous: the host may have sent little,
	// or the network may have eaten it.
	rtpIngressExpected atomic.Int64
	rtpIngressLost     atomic.Int64

	// Keyframe requests to the host by cause (the session timeline): a
	// viewer's own PLI or FIR, a viewer leg that went live, a slow viewer's
	// rescue, a queue rescue.
	kfReqViewer atomic.Int64
	kfReqJoin   atomic.Int64
	kfReqSlow   atomic.Int64
	kfReqRescue atomic.Int64

	// videoMu is the single lifecycle lock for host identity, session state,
	// drain timing, ingress ownership, viewer RTP legs, and feedback state.
	videoMu             sync.Mutex
	lifecycleGeneration uint64
	videoIngress        *streamVideoIngress
	videoIngressToken   uint64
	viewerVideo         map[string]*streamViewerVideo
	audioIngress        *streamAudioIngress
	audioIngressToken   uint64
	viewerAudio         map[string]*streamViewerAudio
	feedback            streamFeedbackState
	idrCache            *streamIdrPacketCache
	// hostHeld is true from a host disconnect until a host joins or the
	// drain closes the session: the viewers got host_reconnecting.
	hostHeld bool
	// videoSeq and audioSeq keep the viewer-leg sequence numbers going on
	// across host ingress changes (a host rejoin).
	videoSeq streamSeqRewriter
	audioSeq streamSeqRewriter
	// keyframes marks the packets of IDR access units for the legs of slow
	// viewers.
	keyframes streamKeyframeTracker
	// nowFn replaces time.Now in the fan-out (tests).
	nowFn func() time.Time
	// hostNACK is the NACK generator of the last wired host
	// PeerConnection, for the relay stats (nack_generator.go).
	hostNACK atomic.Pointer[nackGenerator]

	snapshotPipeline     *snapshot.Pipeline
	snapshotCount        atomic.Int64
	finalizeTeardownHook func()
}

type streamSnapshotConfig struct {
	enabled  bool
	interval time.Duration
	r2       *snapshot.R2Client
	metrics  *metrics.Metrics
}

func NewStreamSession(id, crewID string, logger *logging.Logger, snapshotCfg streamSnapshotConfig) *StreamSession {
	s := &StreamSession{
		id:                  id,
		crewID:              crewID,
		state:               SessionActive,
		lifecycleGeneration: 1,
		viewers:             NewPeerSet(),
		createdAt:           time.Now(),
		logger:              logger,
		viewerVideo:         make(map[string]*streamViewerVideo),
		viewerAudio:         make(map[string]*streamViewerAudio),
		feedback: streamFeedbackState{
			remb: make(map[string]streamREMBEstimate),
		},
		idrCache: newStreamIdrPacketCache(),
	}

	if snapshotCfg.enabled && snapshotCfg.r2 != nil {
		pipeline := snapshot.NewPipeline(snapshot.PipelineConfig{
			SessionID: id,
			CrewID:    crewID,
			Interval:  snapshotCfg.interval,
			Logger:    logger,
			R2:        snapshotCfg.r2,
			OnUpload: func(_ string) {
				s.snapshotCount.Add(1)
				if snapshotCfg.metrics != nil {
					snapshotCfg.metrics.SnapshotsUploaded.Add(1)
				}
			},
		})
		pipeline.Start()
		s.snapshotPipeline = pipeline
	}

	return s
}

func (s *StreamSession) ID() string { return s.id }

func (s *StreamSession) Type() string         { return "stream" }
func (s *StreamSession) CrewID() string       { return s.crewID }
func (s *StreamSession) CreatedAt() time.Time { return s.createdAt }

func (s *StreamSession) State() SessionState {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	return s.state
}

func (s *StreamSession) DrainingAt() time.Time {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	return s.drainingAt
}

func (s *StreamSession) AllPeers() []*Peer {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	var peers []*Peer
	if s.host != nil {
		peers = append(peers, s.host)
	}
	peers = append(peers, s.viewers.Snapshot()...)
	return peers
}

func (s *StreamSession) MemberCount() int {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	count := s.viewers.Len()
	if s.host != nil {
		count++
	}
	return count
}

func (s *StreamSession) Members() []MemberInfo {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	return s.membersLocked(nil)
}

// peersLocked returns the host and the viewers, except skip: the members
// that get member_joined and member_left for skip. Call it with videoMu held.
func (s *StreamSession) peersLocked(skip *Peer) []*Peer {
	var peers []*Peer
	if s.host != nil && s.host != skip {
		peers = append(peers, s.host)
	}
	s.viewers.ForEach(func(p *Peer) {
		if p != skip {
			peers = append(peers, p)
		}
	})
	return peers
}

// sendMemberEvent sends one membership event to each peer in to.
func sendMemberEvent(to []*Peer, msgType string, data MemberEventData) {
	for _, p := range to {
		p.SendSignaling(msgType, data)
	}
}

// queueMemberEvent queues one event to each peer in to (SendSignalingQueued).
// Call it under videoMu: the per-peer order of the queued events is then the
// order of the session changes, and no WebSocket write runs under the lock.
func queueMemberEvent(to []*Peer, msgType string, data interface{}) {
	for _, p := range to {
		p.SendSignalingQueued(msgType, data)
	}
}

// membersLocked lists the host and the viewers, except skip. Call it with
// videoMu held.
func (s *StreamSession) membersLocked(skip *Peer) []MemberInfo {
	var members []MemberInfo
	if s.host != nil && s.host != skip {
		members = append(members, MemberInfo{UserID: s.host.UserID, Role: "host"})
	}
	s.viewers.ForEach(func(p *Peer) {
		if p != skip {
			members = append(members, MemberInfo{UserID: p.UserID, Role: "viewer"})
		}
	})
	return members
}

func (s *StreamSession) HadRealPeer() bool { return s.hadRealPeer.Load() }

func (s *StreamSession) AddPeer(peer *Peer) error {
	_, err := s.Join(peer)
	return err
}

// Join adds peer and returns the other members at the moment it joined.
// The snapshot, the choice of the members that get member_joined and the add
// are one step under videoMu, so of two peers that join at the same time,
// the second lists the first in joined.members and the first gets
// member_joined for the second: every member learns of every other member
// exactly once (01-SFU.md §4.1).
func (s *StreamSession) Join(peer *Peer) ([]MemberInfo, error) {
	var members []MemberInfo
	switch peer.Role {
	case "host":
		s.videoMu.Lock()
		if s.state == SessionClosed {
			s.videoMu.Unlock()
			return nil, ErrSessionClosed
		}
		old := s.host
		if old == nil || old.UserID == peer.UserID {
			members = s.membersLocked(old)
		}
		// member_joined for the host is queued under videoMu, in the same
		// per-peer order as the session_ended of a host disconnect, so the
		// two cannot cross. Join does not wait for the writes.
		announceHostLocked := func(notify []*Peer) {
			queueMemberEvent(notify, "member_joined", MemberEventData{
				UserID: peer.UserID,
				Role:   "host",
			})
		}
		if old != nil && old.UserID == peer.UserID {
			s.cancelVideoIngressLocked(old)
			s.feedback = streamFeedbackState{remb: make(map[string]streamREMBEstimate)}
			s.host = peer
			s.lifecycleGeneration++
			peer.bindSession(s)
			s.state = SessionActive
			s.drainingAt = time.Time{}
			old.clearSession(s)
			announceHostLocked(s.peersLocked(peer))
			s.videoMu.Unlock()
			old.Close()
			s.logger.Info("peer_evicted", logging.D{
				"session": s.id,
				"user":    peer.UserID,
				"reason":  "reconnect",
				"role":    "host",
			})
		} else if old != nil {
			s.videoMu.Unlock()
			return nil, ErrHostAlreadyConnected
		} else {
			held := s.hostHeld
			s.hostHeld = false
			s.host = peer
			s.lifecycleGeneration++
			peer.bindSession(s)
			s.state = SessionActive
			s.drainingAt = time.Time{}
			announceHostLocked(s.peersLocked(peer))
			if held {
				// The viewers kept their legs through the hold: the
				// new ingress feeds them again (01-SFU.md §4.1).
				queueMemberEvent(s.viewers.Snapshot(), "host_reconnected", MemberEventData{
					UserID: peer.UserID,
					Role:   "host",
				})
			}
			s.videoMu.Unlock()
			if held {
				s.logger.Info("host_reconnected", logging.D{
					"session": s.id,
					"user":    peer.UserID,
					"viewers": s.viewers.Len(),
				})
			}
		}

		if !peer.Synthetic {
			s.hadRealPeer.Store(true)
		}
		s.logger.Info("peer_joined", logging.D{
			"session": s.id,
			"user":    peer.UserID,
			"role":    "host",
		})
		return members, nil

	case "viewer":
		s.videoMu.Lock()
		if s.state == SessionClosed {
			s.videoMu.Unlock()
			return nil, ErrSessionClosed
		}
		old := s.viewers.Remove(peer.UserID)
		members = s.membersLocked(nil)
		if old != nil {
			leg := s.detachViewerVideoLocked(old)
			old.clearSession(s)
			s.viewers.Add(peer)
			peer.bindSession(s)
			notify := s.peersLocked(peer)
			s.videoMu.Unlock()
			s.closeViewerVideoLeg(leg)
			old.Close()
			s.logger.Info("peer_evicted", logging.D{
				"session": s.id,
				"user":    peer.UserID,
				"reason":  "reconnect",
				"role":    "viewer",
			})
			sendMemberEvent(notify, "member_joined", MemberEventData{
				UserID: peer.UserID,
				Role:   "viewer",
			})
		} else {
			s.viewers.Add(peer)
			peer.bindSession(s)
			notify := s.peersLocked(peer)
			s.videoMu.Unlock()
			sendMemberEvent(notify, "member_joined", MemberEventData{
				UserID: peer.UserID,
				Role:   "viewer",
			})
		}

		if !peer.Synthetic {
			s.hadRealPeer.Store(true)
		}
		s.logger.Info("peer_joined", logging.D{
			"session": s.id,
			"user":    peer.UserID,
			"role":    "viewer",
			"viewers": s.viewers.Len(),
		})
		return members, nil

	default:
		return nil, ErrInvalidRole
	}
}

// WireAudioTrack is implemented in stream_audio_session.go.

// WireVideoTrack owns the stream host's single H.264 ingress loop.
func (s *StreamSession) WireVideoTrack(peer *Peer, track *webrtc.TrackRemote) {
	pc, generation := peer.currentPeerConnection()
	s.WireVideoTrackFor(peer, track, pc, generation)
}

func (s *StreamSession) WireVideoTrackFor(
	peer *Peer,
	track *webrtc.TrackRemote,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) {
	if peer == nil || track == nil ||
		peer.Role != "host" ||
		!peer.isCurrentPeerConnection(pc, pcGeneration) ||
		!strings.EqualFold(track.Codec().MimeType, webrtc.MimeTypeH264) {
		return
	}

	session, sessionGeneration := peer.currentSession()
	s.videoMu.Lock()
	if s.host != peer || session != s ||
		!peer.isCurrentSession(s, sessionGeneration) {
		s.videoMu.Unlock()
		return
	}
	if current := s.videoIngress; current != nil && current.peer == peer && current.track == track {
		s.videoMu.Unlock()
		return
	}
	s.cancelH264IngressLocked(nil)

	ctx, cancelContext := context.WithCancel(context.Background())
	cancel := func() {
		cancelContext()
		_ = track.SetReadDeadline(time.Now())
	}
	s.videoIngressToken++
	token := s.videoIngressToken
	s.videoIngress = &streamVideoIngress{
		peer:              peer,
		track:             track,
		pc:                pc,
		pcGeneration:      pcGeneration,
		sessionGeneration: sessionGeneration,
		token:             token,
		cancel:            cancel,
	}
	s.videoMu.Unlock()
	if g := nackGeneratorFor(pc); g != nil {
		s.hostNACK.Store(g)
	}

	peer.TrackWireCount.Add(1)
	s.logger.Info("stream_video_track_wired", logging.D{
		"session":    s.id,
		"user":       peer.UserID,
		"codec":      track.Codec().MimeType,
		"ssrc":       track.SSRC(),
		"wire_count": peer.TrackWireCount.Load(),
	})

	go s.readHostVideoRTP(ctx, cancel, peer, track, token)
}

func (s *StreamSession) readHostVideoRTP(
	ctx context.Context,
	cancel func(),
	peer *Peer,
	track *webrtc.TrackRemote,
	token uint64,
) {
	defer func() {
		s.videoMu.Lock()
		if s.videoIngress != nil && s.videoIngress.token == token {
			s.videoIngress = nil
		}
		s.videoMu.Unlock()
		cancel()
	}()

	var duplicates streamSequenceWindow
	first := true
	for {
		pkt, attributes, err := track.ReadRTP()
		if err != nil {
			if ctx.Err() == nil {
				s.logger.Info("stream_video_track_ended", logging.D{
					"session": s.id,
					"user":    peer.UserID,
					"ssrc":    track.SSRC(),
					"error":   err.Error(),
				})
			}
			return
		}

		s.videoMu.Lock()
		active := s.videoIngress != nil &&
			s.videoIngress.token == token &&
			s.host == peer &&
			peer.isCurrentConnectionSession(
				s.videoIngress.pc,
				s.videoIngress.pcGeneration,
				s,
				s.videoIngress.sessionGeneration,
			)
		s.videoMu.Unlock()
		if !active {
			return
		}
		packetSize := int64(pkt.MarshalSize())
		if !s.acceptIngressVideo(&duplicates, pkt.SequenceNumber, packetSize,
			attributes != nil && attributes.Get(webrtc.AttributeRtxSsrc) != nil) {
			continue
		}
		// Single reader goroutine owns this window, so Store (not Add) keeps the
		// published totals consistent with the window's own view.
		s.rtpIngressExpected.Store(duplicates.Expected())
		s.rtpIngressLost.Store(duplicates.Lost())

		peer.PacketsReceived.Add(1)
		peer.BytesReceived.Add(packetSize)
		if first {
			// The session timeline's "first packet" event (media admin).
			first = false
			s.logger.Info("stream_first_rtp", logging.D{
				"session": s.id,
				"user":    peer.UserID,
				"ssrc":    track.SSRC(),
			})
		}
		s.tapSnapshotRTP(pkt)
		s.fanOutVideoRTPFrom(token, pkt)
	}
}

func (s *StreamSession) tapSnapshotRTP(pkt *rtp.Packet) {
	pipeline := s.snapshotPipeline
	if pipeline == nil || pkt == nil {
		return
	}
	pipeline.FeedRTP(pkt)
}

// fanOutVideoRTP fans out a packet of no ingress (tests): the sequence
// numbers stay as they are.
func (s *StreamSession) fanOutVideoRTP(pkt *rtp.Packet) {
	s.fanOutVideoRTPFrom(0, pkt)
}

// fanOutVideoRTPFrom fans out a packet of the host ingress with token.
func (s *StreamSession) fanOutVideoRTPFrom(token uint64, pkt *rtp.Packet) {
	s.videoMu.Lock()
	pkt = s.videoSeq.rewrite(token, pkt)
	// The IDR cache and the live flags change under one lock: a packet is
	// either in the replay of a leg that becomes live, or queued to that leg
	// after the replay, never both.
	if s.idrCache != nil {
		s.idrCache.observe(pkt)
	}
	keyframe := s.keyframes.isKeyframePacket(pkt)
	legs := make([]*streamViewerVideo, 0, len(s.viewerVideo))
	for _, leg := range s.viewerVideo {
		if leg.live {
			legs = append(legs, leg)
		}
	}
	s.videoMu.Unlock()

	packetSize := int64(pkt.MarshalSize())
	now := s.clock()
	rescuePLI := false
	var slowRescues []*streamViewerVideo
	for _, leg := range legs {
		if leg.queueRescueNeeded(now) {
			drainViewerQueue(leg)
			rescuePLI = true
		}
		leg.budget.update(now, leg.queueOverBudget(now))
		if leg.budget.takeRescue() {
			// The backlog is late: drop it, and ask for an IDR for
			// this viewer.
			drainViewerQueue(leg)
			slowRescues = append(slowRescues, leg)
		}
		s.rtpFanoutAttempts.Add(1)
		if !keyframe && leg.budget.slow() {
			// Keyframes only while the viewer is slow.
			s.rtpSlowViewerDroppedPackets.Add(1)
			s.rtpDroppedPackets.Add(1)
			s.rtpDroppedBytes.Add(packetSize)
			continue
		}
		// Mark the head before the send: the writer can take the packet
		// and find the queue empty before a mark after the send, and a
		// mark on an empty queue stays until a false rescue.
		leg.noteQueueEnqueue(now)
		select {
		case <-leg.ctx.Done():
			s.rtpDroppedPackets.Add(1)
			s.rtpDroppedBytes.Add(packetSize)
		case leg.queue <- pkt.Clone():
		default:
			leg.peer.PacketsDropped.Add(1)
			s.rtpQueueDroppedPackets.Add(1)
			s.rtpQueueDroppedBytes.Add(packetSize)
			s.rtpDroppedPackets.Add(1)
			s.rtpDroppedBytes.Add(packetSize)
		}
	}
	if rescuePLI {
		s.kfReqRescue.Add(1)
		s.sendRescuePLI(now)
	}
	for _, leg := range slowRescues {
		s.kfReqSlow.Add(1)
		s.requestViewerPLI(leg, now)
	}
}

func (s *StreamSession) clock() time.Time {
	if s.nowFn != nil {
		return s.nowFn()
	}
	return time.Now()
}

// WireViewerVideo starts the independent RTP send and RTCP feedback loops for
// a viewer after AddStreamVideoTrack has attached its negotiated sender.
func (s *StreamSession) WireViewerVideo(peer *Peer) bool {
	if peer == nil {
		return false
	}
	session, generation := peer.currentSession()
	if session != s {
		return false
	}
	track := peer.StreamVideoTrack()
	return s.ActivateViewerVideo(peer, track, generation)
}

func (s *StreamSession) PrepareViewerVideo(
	peer *Peer,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
) (*webrtc.TrackLocalStaticRTP, uint64, bool, error) {
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	if s.state == SessionClosed || peer == nil || peer.Role != "viewer" {
		return nil, 0, false, ErrSessionClosed
	}
	session, generation := peer.currentSession()
	if session != s {
		return nil, 0, false, ErrSessionClosed
	}
	isCurrentViewer := false
	for _, viewer := range s.viewers.Snapshot() {
		if viewer == peer {
			isCurrentViewer = true
			break
		}
	}
	if !isCurrentViewer {
		return nil, 0, false, ErrSessionClosed
	}
	track, sender, _ := peer.streamVideoLeg()
	if sender != nil {
		return track, generation, false, nil
	}
	track, err := peer.addStreamVideoTrackFor(pc, pcGeneration)
	if err != nil {
		return nil, 0, false, err
	}
	return track, generation, true, nil
}

func (s *StreamSession) ActivateViewerVideo(
	peer *Peer,
	expectedTrack *webrtc.TrackLocalStaticRTP,
	generation uint64,
) bool {
	if peer == nil || expectedTrack == nil {
		return false
	}
	s.videoMu.Lock()
	if s.state == SessionClosed ||
		!peer.isCurrentSession(s, generation) {
		s.videoMu.Unlock()
		return false
	}
	isCurrentViewer := false
	for _, viewer := range s.viewers.Snapshot() {
		if viewer == peer {
			isCurrentViewer = true
			break
		}
	}
	if !isCurrentViewer {
		s.videoMu.Unlock()
		return false
	}
	track, sender, lifecycle := peer.streamVideoLeg()
	if sender == nil || lifecycle == nil || track != expectedTrack {
		s.videoMu.Unlock()
		return false
	}
	if current := s.viewerVideo[peer.UserID]; current != nil &&
		current.peer == peer && current.sender == sender && current.track == track {
		s.videoMu.Unlock()
		return true
	}
	var replaced *streamViewerVideo
	if current := s.viewerVideo[peer.UserID]; current != nil {
		s.cancelViewerVideoLocked(current)
		replaced = current
	}
	ctx, cancel := context.WithCancel(lifecycle)
	leg := &streamViewerVideo{
		peer:      peer,
		track:     track,
		sender:    sender,
		ctx:       ctx,
		cancel:    cancel,
		queue:     make(chan *rtp.Packet, streamVideoQueueSize),
		connected: make(chan struct{}),
	}
	s.viewerVideo[peer.UserID] = leg
	// The connected callback can run before the leg exists (a fast ICE-lite
	// connect), and a renegotiation can wire a leg on a connected
	// PeerConnection. Pion stores the state before it calls the callback.
	if pc, _ := peer.currentPeerConnection(); pc != nil &&
		pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
		s.markViewerVideoLiveLocked(leg)
	}
	s.videoMu.Unlock()
	s.closeViewerVideoLeg(replaced)

	go s.writeViewerVideoRTP(leg)
	go s.readViewerVideoRTCP(leg)
	return true
}

// ViewerVideoConnected starts the viewer's video when its PeerConnection
// reaches Connected: the cached IDR first, then the live packets. Before
// DTLS is up, Pion's SRTP writer drops every packet, so an earlier replay is
// lost.
func (s *StreamSession) ViewerVideoConnected(peer *Peer) {
	if peer == nil {
		return
	}
	s.videoMu.Lock()
	defer s.videoMu.Unlock()
	leg := s.viewerVideo[peer.UserID]
	if leg == nil || leg.peer != peer {
		return
	}
	s.markViewerVideoLiveLocked(leg)
}

func (s *StreamSession) markViewerVideoLiveLocked(leg *streamViewerVideo) {
	if leg.live {
		return
	}
	if s.idrCache != nil {
		leg.replay = s.idrCache.snapshot()
	}
	leg.live = true
	close(leg.connected)
}

func (s *StreamSession) writeViewerVideoRTP(leg *streamViewerVideo) {
	select {
	case <-leg.ctx.Done():
		return
	case <-leg.connected:
	}
	// One burst outside the 64-packet queue: an IDR is often 60 to 200
	// packets.
	for _, pkt := range leg.replay {
		if err := s.sendViewerVideoRTP(leg, pkt); err != nil &&
			(leg.ctx.Err() != nil || errors.Is(err, ErrStaleStreamVideoTrack)) {
			return
		}
	}
	if len(leg.replay) > 0 && s.logger != nil {
		s.logger.Info("stream_idr_replayed", logging.D{
			"session": s.id,
			"viewer":  leg.peer.UserID,
			"packets": len(leg.replay),
		})
	}
	leg.replay = nil
	// The replayed IDR is old: the live packets after it do not chain to
	// it, and the viewer sends a PLI once it finds the gap, about 120 ms
	// later. Ask the host for a fresh IDR now (01-SFU.md §6.1).
	s.kfReqJoin.Add(1)
	s.requestViewerPLI(leg, time.Now())
	for {
		select {
		case <-leg.ctx.Done():
			return
		case pkt := <-leg.queue:
			if leg.ctx.Err() != nil {
				return
			}
			now := time.Now()
			if err := s.sendViewerVideoRTP(leg, pkt); err != nil {
				if leg.ctx.Err() != nil || errors.Is(err, ErrStaleStreamVideoTrack) {
					return
				}
				continue
			}
			leg.noteQueueDequeue(now)
		}
	}
}

func (s *StreamSession) sendViewerVideoRTP(leg *streamViewerVideo, pkt *rtp.Packet) error {
	packetSize := int64(pkt.MarshalSize())
	if err := leg.peer.SendStreamVideoRTPFor(leg.track, pkt); err != nil {
		s.rtpDroppedPackets.Add(1)
		s.rtpDroppedBytes.Add(packetSize)
		return err
	}
	s.rtpForwardedPackets.Add(1)
	s.rtpForwardedBytes.Add(packetSize)
	leg.loss.noteSent(pkt.SequenceNumber)
	return nil
}

func (s *StreamSession) readViewerVideoRTCP(leg *streamViewerVideo) {
	buffer := make([]byte, streamVideoRTCPReadBuffer)
	var invalid int64
	for {
		// The interceptor chain parses each packet in Read: the NACK
		// responder answers a viewer NACK only while this loop reads. A
		// packet that does not parse returns an error for that packet only,
		// so the loop continues. Only a closed stream ends it.
		n, _, err := leg.sender.Read(buffer)
		if err != nil {
			if leg.ctx.Err() != nil || viewerRTCPReadClosed(err) {
				return
			}
			invalid++
			s.rtpViewerRTCPInvalid.Add(1)
			if (invalid == 1 || invalid%500 == 0) && s.logger != nil {
				s.logger.Warn("stream_viewer_rtcp_invalid", logging.D{
					"session": s.id,
					"viewer":  leg.peer.UserID,
					"count":   invalid,
					"error":   err.Error(),
				})
			}
			continue
		}
		packets, err := rtcp.Unmarshal(buffer[:n])
		if err != nil {
			continue
		}
		if leg.ctx.Err() != nil {
			return
		}
		s.handleViewerVideoRTCP(leg, packets, time.Now())
	}
}

// viewerRTCPReadClosed reports whether a viewer RTCP read error means that
// the stream is closed. Any other error is about one packet.
func viewerRTCPReadClosed(err error) bool {
	var netErr net.Error
	return errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) ||
		errors.As(err, &netErr)
}

func (s *StreamSession) handleViewerVideoRTCP(leg *streamViewerVideo, packets []rtcp.Packet, now time.Time) {
	s.noteViewerNACKs(leg, packets)
	s.videoMu.Lock()
	if s.viewerVideo[leg.peer.UserID] != leg || s.videoIngress == nil {
		s.videoMu.Unlock()
		return
	}
	ingress := s.videoIngress
	hostPC := ingress.pc
	hostGeneration := ingress.pcGeneration
	hostSSRC := uint32(ingress.track.SSRC())
	stale := !ingress.peer.isCurrentPeerConnection(hostPC, hostGeneration)
	var upstream []rtcp.Packet
	if !stale {
		s.refreshViewerEstimatesLocked(now)
		upstream = s.feedback.Accept(leg.peer.UserID, packets, now, hostSSRC)
	}
	s.videoMu.Unlock()
	for _, pkt := range upstream {
		if _, ok := pkt.(*rtcp.PictureLossIndication); ok {
			s.kfReqViewer.Add(1)
			break
		}
	}

	if stale || len(upstream) == 0 {
		return
	}
	if err := hostPC.WriteRTCP(upstream); err != nil {
		s.logger.Debug("stream_video_feedback_write_failed", logging.D{
			"session": s.id,
			"viewer":  leg.peer.UserID,
			"error":   err.Error(),
		})
	}
}

// noteViewerNACKs counts the packets of the leg that the viewer NACKed: the
// loss evidence of the slow-viewer rule (stream_slow_viewer.go).
func (s *StreamSession) noteViewerNACKs(leg *streamViewerVideo, packets []rtcp.Packet) {
	for _, packet := range packets {
		nack, ok := packet.(*rtcp.TransportLayerNack)
		if !ok {
			continue
		}
		for _, pair := range nack.Nacks {
			for _, seq := range pair.PacketList() {
				if leg.loss.noteNACK(seq) {
					s.rtpViewerNACKedPackets.Add(1)
				}
			}
		}
	}
}

// tickFeedback re-evaluates aggregated congestion feedback once per server
// stats tick (~1s), forwarding an updated minimum REMB upstream when viewer
// estimates changed or expired without any fresh REMB arriving. See
// streamFeedbackState.Tick.
func (s *StreamSession) tickFeedback(now time.Time) {
	s.videoMu.Lock()
	if s.videoIngress == nil {
		s.videoMu.Unlock()
		return
	}
	ingress := s.videoIngress
	hostPC := ingress.pc
	hostGeneration := ingress.pcGeneration
	hostSSRC := uint32(ingress.track.SSRC())
	stale := !ingress.peer.isCurrentPeerConnection(hostPC, hostGeneration)
	var upstream []rtcp.Packet
	viewers, gccFresh, rembFresh, constrained := 0, 0, 0, 0
	var ingressBps float32
	var bwe []logging.D
	if !stale {
		s.feedback.noteIngress(now, s.rtpInBytes.Load())
		s.refreshViewerEstimatesLocked(now)
		upstream = s.feedback.Tick(now, hostSSRC)
		viewers = len(s.viewerVideo)
		gccFresh = len(s.feedback.gcc)
		rembFresh = len(s.feedback.remb)
		constrained = len(s.feedback.constrained)
		ingressBps = s.feedback.ingressBps
		for id, leg := range s.viewerVideo {
			if len(bwe) == viewerBWELogLimit {
				break
			}
			if e := leg.peer.currentEstimator(); e != nil {
				fields := viewerBWELogFields(id, e)
				fields["slow"] = leg.budget.slow()
				fields["sent"] = leg.loss.sent.Load()
				fields["nacked"] = leg.loss.nacked.Load()
				bwe = append(bwe, fields)
			}
		}
	}
	s.videoMu.Unlock()

	if stale {
		return
	}
	// One line per tick (about 1 s): the upstream REMB of 01-SFU.md §6.1,
	// or why there is none (no fresh gcc estimate and no fresh REMB).
	fields := logging.D{
		"session":    s.id,
		"viewers":    viewers,
		"gcc_fresh":  gccFresh,
		"remb_fresh": rembFresh,
		"sent":       len(upstream) > 0,
		// Viewers that hold the ceiling down, and the host rate.
		"constrained": constrained,
		"ingress_bps": ingressBps,
	}
	if len(bwe) > 0 {
		// Up to viewerBWELogLimit viewers: the gcc state and the feedback
		// counters since the last tick (viewer_bwe.go).
		fields["viewer_bwe"] = bwe
	}
	for _, pkt := range upstream {
		if remb, ok := pkt.(*rtcp.ReceiverEstimatedMaximumBitrate); ok {
			fields["bitrate_bps"] = remb.Bitrate
		}
	}
	if len(upstream) > 0 {
		if err := hostPC.WriteRTCP(upstream); err != nil {
			fields["error"] = err.Error()
		}
	}
	s.logger.Info("stream_host_remb_tick", fields)
}

func (s *StreamSession) stopViewerVideo(peer *Peer) {
	if peer == nil {
		return
	}
	s.videoMu.Lock()
	leg := s.detachViewerVideoLocked(peer)
	s.videoMu.Unlock()
	s.closeViewerVideoLeg(leg)
}

func (s *StreamSession) detachViewerVideoLocked(peer *Peer) *streamViewerVideo {
	leg := s.viewerVideo[peer.UserID]
	if leg == nil || leg.peer != peer {
		return nil
	}
	delete(s.viewerVideo, peer.UserID)
	s.teardownViewerAudioLocked(peer)
	s.feedback.removeViewer(peer.UserID)
	s.cancelViewerVideoLocked(leg)
	return leg
}

func (s *StreamSession) cancelViewerVideoLocked(leg *streamViewerVideo) {
	leg.cancel()
}

func (s *StreamSession) closeViewerVideoLeg(leg *streamViewerVideo) {
	if leg != nil {
		leg.peer.closeStreamVideoTrack(leg.track)
	}
}

// cancelVideoIngressLocked cancels all host ingress: H.264 and audio. It is
// for host leave and host replacement.
func (s *StreamSession) cancelVideoIngressLocked(peer *Peer) {
	s.cancelH264IngressLocked(peer)
	s.cancelAudioIngressLocked(peer)
}

// cancelH264IngressLocked cancels only the H.264 ingress. A new host video
// track replaces only this one: the audio ingress keeps its own lifetime.
func (s *StreamSession) cancelH264IngressLocked(peer *Peer) {
	if s.videoIngress != nil && (peer == nil || s.videoIngress.peer == peer) {
		s.logIngressCancelledLocked("video", s.videoIngress.peer, s.videoIngress.track)
		s.videoIngress.cancel()
		s.videoIngress = nil
		if s.idrCache != nil {
			s.idrCache.reset()
		}
	}
}

func (s *StreamSession) logIngressCancelledLocked(kind string, peer *Peer, track *webrtc.TrackRemote) {
	if s.logger == nil {
		return
	}
	data := logging.D{"session": s.id, "kind": kind}
	if peer != nil {
		data["user"] = peer.UserID
	}
	if track != nil {
		data["ssrc"] = track.SSRC()
	}
	s.logger.Info("stream_ingress_cancelled", data)
}

func (s *streamFeedbackState) Accept(
	viewerID string,
	packets []rtcp.Packet,
	now time.Time,
	hostSSRC uint32,
) []rtcp.Packet {
	if s.remb == nil {
		s.remb = make(map[string]streamREMBEstimate)
	}
	if s.lastPLI == nil {
		s.lastPLI = make(map[string]time.Time)
	}
	requestKeyframe := false
	gotREMB := false
	for _, packet := range packets {
		switch feedback := packet.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			requestKeyframe = true
		case *rtcp.ReceiverEstimatedMaximumBitrate:
			if feedback.Bitrate > 0 {
				s.remb[viewerID] = streamREMBEstimate{bitrate: feedback.Bitrate, at: now}
				gotREMB = true
			}
		}
	}

	var upstream []rtcp.Packet
	if requestKeyframe && intervalElapsed(s.lastPLI[viewerID], now, streamFeedbackMinInterval) {
		s.lastPLI[viewerID] = now
		upstream = append(upstream, &rtcp.PictureLossIndication{MediaSSRC: hostSSRC})
	}
	if gotREMB && intervalElapsed(s.lastREMBForward, now, streamFeedbackMinInterval) {
		if ceiling := s.upstreamCeiling(now); ceiling > 0 {
			s.lastREMBForward = now
			upstream = append(upstream, upstreamREMB(ceiling, hostSSRC))
		}
	}
	return upstream
}

// upstreamREMB is the REMB to the host. Its sender SSRC is the host's
// video SSRC: libdatachannel, the host's stack, routes an incoming RTCP
// packet to a track by the sender and media source SSRCs of its header,
// and a REMB's media source is 0. With sender 0 the host's video track
// never got it (mello stage 4 gate: tx_remb_reports 0 on every profile).
func upstreamREMB(bitrate float32, hostSSRC uint32) rtcp.Packet {
	return &rtcp.ReceiverEstimatedMaximumBitrate{
		Bitrate:    bitrate,
		SenderSSRC: hostSSRC,
		SSRCs:      []uint32{hostSSRC},
	}
}

// Tick re-forwards the upstream ceiling even when no fresh REMB just
// arrived: viewer estimates change and expire without one, and a viewer
// whose RTCP stopped must not pin the host at its stale value. Called once
// per server stats tick (~1s).
func (s *streamFeedbackState) Tick(now time.Time, hostSSRC uint32) []rtcp.Packet {
	if !intervalElapsed(s.lastREMBForward, now, streamFeedbackMinInterval) {
		return nil
	}
	ceiling := s.upstreamCeiling(now)
	if ceiling <= 0 {
		return nil
	}
	s.lastREMBForward = now
	return []rtcp.Packet{upstreamREMB(ceiling, hostSSRC)}
}

func intervalElapsed(previous, now time.Time, interval time.Duration) bool {
	return previous.IsZero() || now.Sub(previous) >= interval
}

func (w *streamSequenceWindow) Accept(sequence uint16) bool {
	if !w.initialized {
		w.initialized = true
		w.highest = sequence
		w.base = sequence
		w.received = 1
		w.seen[0] = 1
		return true
	}

	ahead := int16(sequence - w.highest)
	if ahead > 0 {
		// Wrapped past 65535 if the new sequence is numerically smaller while
		// still being ahead in modular terms.
		if sequence < w.highest {
			w.cycles++
		}
		w.advance(uint16(ahead))
		w.highest = sequence
		w.seen[0] |= 1
		w.received++
		return true
	}

	behind := uint16(w.highest - sequence)
	if behind >= streamSequenceWindowSize {
		return false
	}
	word := behind / 64
	bit := uint(behind % 64)
	mask := uint64(1) << bit
	if w.seen[word]&mask != 0 {
		return false
	}
	w.seen[word] |= mask
	w.received++
	return true
}

func (w *streamSequenceWindow) advance(count uint16) {
	if count >= streamSequenceWindowSize {
		clear(w.seen[:])
		return
	}

	words := int(count / 64)
	bits := uint(count % 64)
	if words > 0 {
		for i := len(w.seen) - 1; i >= words; i-- {
			w.seen[i] = w.seen[i-words]
		}
		clear(w.seen[:words])
	}
	if bits == 0 {
		return
	}
	for i := len(w.seen) - 1; i > 0; i-- {
		w.seen[i] = (w.seen[i] << bits) | (w.seen[i-1] >> (64 - bits))
	}
	w.seen[0] <<= bits
}

// WireDataChannels wires the stream control DataChannel once it is open.
func (s *StreamSession) WireDataChannels(peer *Peer) {
	pc, pcGeneration := peer.currentPeerConnection()
	session, sessionGeneration := peer.currentSession()
	if session != s {
		return
	}
	peer.wireControlChannelForSession(
		pc,
		pcGeneration,
		s,
		sessionGeneration,
		func(control *webrtc.DataChannel) {
			s.wireControlChannelFor(peer, pc, pcGeneration, sessionGeneration, control)
		},
	)
}

func (s *StreamSession) wireControlChannelFor(
	peer *Peer,
	pc *webrtc.PeerConnection,
	pcGeneration uint64,
	sessionGeneration uint64,
	control *webrtc.DataChannel,
) {
	if control == nil {
		return
	}
	control.OnMessage(func(msg webrtc.DataChannelMessage) {
		if !peer.isCurrentConnectionSession(pc, pcGeneration, s, sessionGeneration) {
			return
		}
		// The SFU answers a ping on the leg it came on (host and viewers),
		// as on a voice control channel: the client's link health reads
		// the pong age (01-SFU.md §5.3). Before, a viewer's ping went to
		// the host and a host's was ignored, so stream rtt_ms was 0.
		if pong, ok := controlPong(msg); ok {
			if err := control.SendText(pong); err != nil && s.logger != nil {
				s.logger.Warn("stream_pong_send_failed", logging.D{
					"session": s.id,
					"user":    peer.UserID,
					"error":   err.Error(),
				})
			}
			return
		}
		if peer.Role != "viewer" {
			// The SFU forwards nothing from the host's control channel.
			// The stage 4 rung message is logged for the session timeline.
			if w, h, fps, ok := parseRungMessage(msg); ok && s.logger != nil {
				s.logger.Info("stream_rung_change", logging.D{
					"session": s.id,
					"user":    peer.UserID,
					"w":       w,
					"h":       h,
					"fps":     fps,
				})
			}
			return
		}
		msgSize := len(msg.Data)
		peer.PacketsReceived.Add(1)
		peer.BytesReceived.Add(int64(msgSize))
		s.recordControlIngress(msgSize)
		s.videoMu.Lock()
		host := s.host
		s.videoMu.Unlock()
		if host != nil {
			if host.SendControl(msg.Data) {
				s.recordControlForward(true)
				return
			}
			s.recordControlForward(false)
		}
	})
}

func (s *StreamSession) RelaySnapshot() StreamRelaySnapshot {
	return StreamRelaySnapshot{
		MediaInPackets:        s.mediaInPackets.Load(),
		MediaInBytes:          s.mediaInBytes.Load(),
		MediaForwardAttempts:  s.mediaForwardAttempts.Load(),
		MediaForwardedPackets: s.mediaForwardedPackets.Load(),
		MediaForwardedBytes:   s.mediaForwardedBytes.Load(),
		MediaDroppedPackets:   s.mediaDroppedPackets.Load(),
		MediaDroppedBytes:     s.mediaDroppedBytes.Load(),
		ControlInPackets:      s.controlInPackets.Load(),
		ControlInBytes:        s.controlInBytes.Load(),
		ControlForwarded:      s.controlForwarded.Load(),
		ControlDropped:        s.controlDropped.Load(),
		KeyframeLikeMessages:  s.keyframeLikeMessages.Load(),
		MaxMediaMessageBytes:  s.maxMediaMessageBytes.Load(),
		SizeBuckets: StreamRelaySizeBuckets{
			LE1200:  s.sizeLE1200.Load(),
			LE8000:  s.sizeLE8000.Load(),
			LE20000: s.sizeLE20000.Load(),
			LE40000: s.sizeLE40000.Load(),
			GT40000: s.sizeGT40000.Load(),
		},
		RTPInPackets:                s.rtpInPackets.Load(),
		RTPInBytes:                  s.rtpInBytes.Load(),
		RTPFanoutAttempts:           s.rtpFanoutAttempts.Load(),
		RTPForwardedPackets:         s.rtpForwardedPackets.Load(),
		RTPForwardedBytes:           s.rtpForwardedBytes.Load(),
		RTPDroppedPackets:           s.rtpDroppedPackets.Load(),
		RTPDroppedBytes:             s.rtpDroppedBytes.Load(),
		RTPIngressExpected:          s.rtpIngressExpected.Load(),
		RTPIngressLost:              s.rtpIngressLost.Load(),
		RTPViewerRTCPInvalid:        s.rtpViewerRTCPInvalid.Load(),
		RTPSlowViewerDroppedPackets: s.rtpSlowViewerDroppedPackets.Load(),
		RTPViewerNACKedPackets:      s.rtpViewerNACKedPackets.Load(),
		RTPQueueDroppedPackets:      s.rtpQueueDroppedPackets.Load(),
		RTPQueueDroppedBytes:        s.rtpQueueDroppedBytes.Load(),
		RTPDuplicatePackets:         s.rtpDuplicatePackets.Load(),
		RTPRTXRecovered:             s.rtpRTXRecoveredPackets.Load(),
		RTPRTXDuplicate:             s.rtpRTXDuplicatePackets.Load(),
		KeyframeRequests: StreamKeyframeRequests{
			Viewer: s.kfReqViewer.Load(),
			Join:   s.kfReqJoin.Load(),
			Slow:   s.kfReqSlow.Load(),
			Rescue: s.kfReqRescue.Load(),
		},
	}
}

// acceptIngressVideo counts one host video packet and runs it through the
// duplicate window. A repair is a packet that Pion unwrapped from the host's
// RTX stream: forwarded, it counts as recovered; dropped (the original had
// arrived), as an RTX duplicate. Returns false for a duplicate.
func (s *StreamSession) acceptIngressVideo(window *streamSequenceWindow, sequence uint16, size int64, repair bool) bool {
	s.rtpInPackets.Add(1)
	s.rtpInBytes.Add(size)
	if !window.Accept(sequence) {
		s.rtpDuplicatePackets.Add(1)
		s.rtpDroppedPackets.Add(1)
		s.rtpDroppedBytes.Add(size)
		if repair {
			s.rtpRTXDuplicatePackets.Add(1)
		}
		return false
	}
	if repair {
		s.rtpRTXRecoveredPackets.Add(1)
	}
	return true
}

func (s *StreamSession) SnapshotCount() int64 {
	return s.snapshotCount.Load()
}

func (s *StreamSession) recordControlIngress(msgSize int) {
	s.controlInPackets.Add(1)
	s.controlInBytes.Add(int64(msgSize))
}

func (s *StreamSession) recordControlForward(sent bool) {
	if sent {
		s.controlForwarded.Add(1)
		return
	}
	s.controlDropped.Add(1)
}

func (s *StreamSession) RemovePeer(userID string) {
	s.videoMu.Lock()
	var peer *Peer
	if s.host != nil && s.host.UserID == userID {
		peer = s.host
	} else {
		for _, viewer := range s.viewers.Snapshot() {
			if viewer.UserID == userID {
				peer = viewer
				break
			}
		}
	}
	s.videoMu.Unlock()
	if peer != nil {
		s.RemovePeerInstance(peer)
	}
}

// RemovePeerInstance removes only the exact membership generation. A stale
// reconnect teardown therefore cannot remove a replacement with the same ID.
func (s *StreamSession) RemovePeerInstance(peer *Peer) {
	if peer == nil {
		return
	}

	s.videoMu.Lock()
	if s.host == peer {
		s.cancelVideoIngressLocked(peer)
		s.host = nil
		s.lifecycleGeneration++
		peer.clearSession(s)
		if s.state != SessionClosed {
			s.state = SessionDraining
			s.drainingAt = time.Now()
		}
		s.feedback = streamFeedbackState{remb: make(map[string]streamREMBEstimate)}
		// Hold the session for a host rejoin: the viewers keep their legs
		// and get session_ended only when the drain closes the session.
		// Queued under videoMu, not sent: a viewer whose socket is slow
		// must not hold up a host Join.
		// A host that sent leave ended the stream: session_ended at once,
		// as before the hold. Only an unclean disconnect (socket closed
		// without leave, ICE failure, slow-peer removal) holds.
		if s.state == SessionDraining && peer.cleanLeave.Load() {
			queueMemberEvent(s.viewers.Snapshot(), "session_ended", MemberEventData{Reason: "host_disconnected"})
		} else if s.state == SessionDraining {
			s.hostHeld = true
			queueMemberEvent(s.viewers.Snapshot(), "host_reconnecting", HostReconnectingData{
				UserID: peer.UserID,
				Reason: "host_disconnected",
				HoldMS: DrainTimeout.Milliseconds(),
			})
		}
		s.videoMu.Unlock()

		peer.Close()

		fields := s.rtpLogFields()
		fields["session"] = s.id
		fields["user"] = peer.UserID
		fields["pkts_sent"] = peer.PacketsSent.Load()
		fields["pkts_recv"] = peer.PacketsReceived.Load()
		fields["pkts_dropped"] = peer.PacketsDropped.Load()
		fields["bytes_sent"] = peer.BytesSent.Load()
		fields["bytes_recv"] = peer.BytesReceived.Load()
		fields["peak_buf_pct"] = peer.BufferUsage.Load()
		fields["duration_sec"] = int(time.Since(peer.ConnectedAt).Seconds())
		s.logger.Info("host_disconnected", fields)
		return
	}

	var removed *Peer
	for _, viewer := range s.viewers.Snapshot() {
		if viewer == peer {
			removed = s.viewers.Remove(peer.UserID)
			break
		}
	}
	if removed != nil {
		leg := s.detachViewerVideoLocked(removed)
		removed.clearSession(s)
		if s.host == nil && s.viewers.Len() == 0 && s.state != SessionClosed {
			s.state = SessionDraining
			s.drainingAt = time.Now()
		}
		viewerCount := s.viewers.Len()
		notify := s.peersLocked(removed)
		s.videoMu.Unlock()

		s.closeViewerVideoLeg(leg)
		removed.Close()
		sendMemberEvent(notify, "member_left", MemberEventData{
			UserID: removed.UserID,
			Role:   "viewer",
			Reason: "disconnect",
		})
		s.logger.Info("peer_left", logging.D{
			"session":      s.id,
			"user":         removed.UserID,
			"viewers":      viewerCount,
			"pkts_sent":    removed.PacketsSent.Load(),
			"pkts_recv":    removed.PacketsReceived.Load(),
			"pkts_dropped": removed.PacketsDropped.Load(),
			"bytes_sent":   removed.BytesSent.Load(),
			"bytes_recv":   removed.BytesReceived.Load(),
			"peak_buf_pct": removed.BufferUsage.Load(),
			"duration_sec": int(time.Since(removed.ConnectedAt).Seconds()),
		})
		return
	}
	s.videoMu.Unlock()
	peer.Close()
}

// FinalizeDrain atomically closes an expired drain generation. Once marked
// closed, a concurrent AddPeer rejects the orphaned session even if it already
// obtained a pointer from SessionStore.
func (s *StreamSession) PrepareFinalizeDrain(
	now time.Time,
	timeout time.Duration,
) (func(), bool) {
	s.videoMu.Lock()
	if s.state != SessionDraining ||
		s.drainingAt.IsZero() ||
		now.Sub(s.drainingAt) <= timeout {
		s.videoMu.Unlock()
		return nil, false
	}
	s.state = SessionClosed
	s.lifecycleGeneration++
	ingress := s.videoIngress
	s.videoIngress = nil

	legs := make([]*streamViewerVideo, 0, len(s.viewerVideo))
	viewerCleanups := make([]viewerDrainCleanup, 0, len(s.viewerVideo))
	viewers := s.viewers.Snapshot()
	for _, viewer := range viewers {
		if leg := s.detachViewerVideoLocked(viewer); leg != nil {
			legs = append(legs, leg)
		}
		track, _, _ := viewer.streamVideoLeg()
		var cancel context.CancelFunc
		viewer.streamVideoMu.RLock()
		if leg := viewer.streamVideo; leg != nil && leg.track == track {
			cancel = leg.cancel
		}
		viewer.streamVideoMu.RUnlock()
		viewerCleanups = append(viewerCleanups, viewerDrainCleanup{
			viewer: viewer,
			track:  track,
			cancel: cancel,
		})
		s.viewers.Remove(viewer.UserID)
		viewer.clearSession(s)
	}
	pipeline := s.snapshotPipeline
	s.snapshotPipeline = nil
	s.feedback = streamFeedbackState{remb: make(map[string]streamREMBEstimate)}
	// The hold ends without a host: the viewers get session_ended now.
	// After a clean host leave they got it already.
	if s.hostHeld {
		queueMemberEvent(viewers, "session_ended", MemberEventData{Reason: "host_disconnected"})
	}
	s.hostHeld = false
	hook := s.finalizeTeardownHook
	s.videoMu.Unlock()

	teardown := func() {
		if hook != nil {
			hook()
		}
		if ingress != nil {
			ingress.cancel()
		}
		for _, leg := range legs {
			s.closeViewerVideoLeg(leg)
		}
		for _, cleanup := range viewerCleanups {
			if cleanup.cancel != nil {
				cleanup.cancel()
			}
			if cleanup.track != nil {
				cleanup.viewer.closeStreamVideoTrack(cleanup.track)
			}
		}
		if pipeline != nil {
			pipeline.Stop()
		}
		fields := s.rtpLogFields()
		fields["session"] = s.id
		s.logger.Info("stream_rtp_relay_final", fields)
	}
	return teardown, true
}

func (s *StreamSession) FinalizeDrain(now time.Time, timeout time.Duration) bool {
	teardown, ok := s.PrepareFinalizeDrain(now, timeout)
	if !ok {
		return false
	}
	teardown()
	return true
}

func (s *StreamSession) rtpLogFields() logging.D {
	snapshot := s.RelaySnapshot()
	fields := logging.D{
		"rtp_in_packets":                  snapshot.RTPInPackets,
		"rtp_in_bytes":                    snapshot.RTPInBytes,
		"rtp_fanout_attempts":             snapshot.RTPFanoutAttempts,
		"rtp_forwarded_packets":           snapshot.RTPForwardedPackets,
		"rtp_forwarded_bytes":             snapshot.RTPForwardedBytes,
		"rtp_dropped_packets":             snapshot.RTPDroppedPackets,
		"rtp_dropped_bytes":               snapshot.RTPDroppedBytes,
		"rtp_queue_dropped_packets":       snapshot.RTPQueueDroppedPackets,
		"rtp_queue_dropped_bytes":         snapshot.RTPQueueDroppedBytes,
		"rtp_duplicate_packets":           snapshot.RTPDuplicatePackets,
		"rtp_rtx_recovered_packets":       snapshot.RTPRTXRecovered,
		"rtp_rtx_duplicate_packets":       snapshot.RTPRTXDuplicate,
		"rtp_ingress_expected":            snapshot.RTPIngressExpected,
		"rtp_ingress_lost":                snapshot.RTPIngressLost,
		"rtp_ingress_loss_pct":            ingressLossPct(snapshot),
		"rtp_viewer_rtcp_invalid":         snapshot.RTPViewerRTCPInvalid,
		"rtp_slow_viewer_dropped_packets": snapshot.RTPSlowViewerDroppedPackets,
		"rtp_viewer_nacked_packets":       snapshot.RTPViewerNACKedPackets,
	}
	if g := s.hostNACK.Load(); g != nil {
		// The SFU's NACKs to the host (nack_generator.go): packets asked
		// for, further requests, RTX repairs of them, late originals,
		// given up, and the measured NACK-to-repair round trip.
		st := g.stats()
		fields["host_nack_first"] = st.First
		fields["host_nack_retries"] = st.Retries
		fields["host_nack_repaired"] = st.Repaired
		fields["host_nack_late_originals"] = st.LateOriginals
		fields["host_nack_gave_up"] = st.GaveUp
		fields["host_nack_rtt_ms"] = st.RTTMs
	}
	return fields
}

// ingressLossPct is the host->SFU media loss rate. A collapse in rtp_in_pps with
// loss near zero means the host stopped producing; with loss high it means the
// uplink dropped it. Distinguishing those is the whole point of the counters.
func ingressLossPct(snapshot StreamRelaySnapshot) float64 {
	if snapshot.RTPIngressExpected <= 0 {
		return 0
	}
	return float64(snapshot.RTPIngressLost) / float64(snapshot.RTPIngressExpected) * 100
}

// controlPong is the answer to a control-channel ping: the same text with
// "ping" turned into "pong" (`{"type":"ping","ts":N}`, libmello
// PeerConnectionImpl::send_ping), as the voice control channel answers.
func controlPong(msg webrtc.DataChannelMessage) (string, bool) {
	const prefix = `{"type":"ping"`
	if !msg.IsString || len(msg.Data) >= 256 || len(msg.Data) <= len(prefix) || string(msg.Data[:len(prefix)]) != prefix {
		return "", false
	}
	pong := []byte(string(msg.Data))
	pong[10] = 'o' // "ping" -> "pong" (index 10 is the 'i')
	return string(pong), true
}

// parseRungMessage reads the host's stage 4 rung message (mello-core
// stream/controller.rs): [0x04, 0x05, width u16 BE, height u16 BE, fps].
func parseRungMessage(msg webrtc.DataChannelMessage) (w, h, fps int, ok bool) {
	d := msg.Data
	if msg.IsString || len(d) != 7 || d[0] != 0x04 || d[1] != 0x05 {
		return 0, 0, 0, false
	}
	return int(d[2])<<8 | int(d[3]), int(d[4])<<8 | int(d[5]), int(d[6]), true
}
