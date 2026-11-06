package server

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
)

// Per-viewer send-side estimate (01-SFU.md §6.1). Pion's gcc estimator runs
// in the interceptor chain of each stream viewer PeerConnection: that is
// the only place where it sees the packets the SFU sends and the viewer's
// transport-cc feedback. A standalone estimator (removed in stage 2) saw
// neither and never produced a target.
const (
	// viewerBWEInitialBitrate is above the stream encode rate, so a viewer
	// whose feedback just started does not pull the host down: GCC only
	// lowers its target on overuse or loss.
	viewerBWEInitialBitrate = 10_000_000
	viewerBWEMinBitrate     = 300_000
	viewerBWEMaxBitrate     = 25_000_000
	// pionFeedbackHistory is the number of sent packets that Pion's
	// transport-cc feedback adapter keeps (internal/cc, fixed). A feedback
	// entry for an older packet finds no history.
	pionFeedbackHistory = 250
	// transportCCExtensionURI is the transport-wide sequence number header
	// extension.
	transportCCExtensionURI = "http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01"
)

// viewerEstimator wraps Pion's gcc estimator of one viewer PeerConnection.
// It records when transport-cc feedback last reached the estimator: with
// no fresh feedback there is no estimate (a viewer whose feedback does not
// parse, or that sends none, has only its REMB).
type viewerEstimator struct {
	inner        *gcc.SendSideBWE
	lastFeedback atomic.Int64 // Unix nanoseconds, 0 = never
	feedbackErrs atomic.Int64
	id           string
	registry     *viewerBWERegistry

	// Feedback counters (viewerBWEDiag).
	sentSeq      atomic.Uint32 // newest transport-wide sequence sent, +1<<16 once set
	feedbacks    atomic.Int64
	acked        atomic.Int64
	reportedLost atomic.Int64
	// overrun counts feedback entries for packets older than Pion's
	// history: Pion counts them lost and does not consume their deltas.
	overrun atomic.Int64
	// padding counts the status symbols past a feedback's packet count
	// that sanitize removed (Pion counted each as a lost packet).
	padding atomic.Int64
	maxLag  atomic.Int64
	last    viewerBWEDiag
}

// viewerBWEDiag is a snapshot of the estimator for the tick log.
type viewerBWEDiag struct {
	Feedbacks    int64
	Acked        int64
	ReportedLost int64
	Overrun      int64
	Padding      int64
	MaxLag       int64
}

func (e *viewerEstimator) AddStream(info *interceptor.StreamInfo, writer interceptor.RTPWriter) interceptor.RTPWriter {
	var extID uint8
	for _, ext := range info.RTPHeaderExtensions {
		if ext.URI == transportCCExtensionURI {
			extID = uint8(ext.ID) //nolint:gosec // an extension ID is 1 to 14
		}
	}
	if extID == 0 {
		return e.inner.AddStream(info, writer)
	}
	return e.inner.AddStream(info, interceptor.RTPWriterFunc(
		func(header *rtp.Header, payload []byte, attributes interceptor.Attributes) (int, error) {
			if ext := header.GetExtension(extID); len(ext) >= 2 {
				e.noteSent(uint16(ext[0])<<8 | uint16(ext[1]))
			}
			return writer.Write(header, payload, attributes)
		}))
}

// noteSent records the newest transport-wide sequence number sent. A NACK
// repair goes out again with its original number (the responder keeps the
// stamped packet), so an older number does not move it back.
func (e *viewerEstimator) noteSent(seq uint16) {
	for {
		cur := e.sentSeq.Load()
		if cur != 0 && int16(seq-uint16(cur)) <= 0 {
			return
		}
		if e.sentSeq.CompareAndSwap(cur, uint32(seq)|1<<16) {
			return
		}
	}
}

// noteFeedback counts one transport-cc feedback packet: its entries, the
// ones reported lost, and the ones older than Pion's history.
func (e *viewerEstimator) noteFeedback(fb *rtcp.TransportLayerCC) {
	e.feedbacks.Add(1)
	count := int64(fb.PacketStatusCount)
	received := int64(len(fb.RecvDeltas))
	e.acked.Add(received)
	if count > received {
		e.reportedLost.Add(count - received)
	}
	sent := e.sentSeq.Load()
	if sent == 0 {
		return
	}
	lag := int64(uint16(uint16(sent)-fb.BaseSequenceNumber)) + 1
	for {
		old := e.maxLag.Load()
		if lag <= old || e.maxLag.CompareAndSwap(old, lag) {
			break
		}
	}
	if over := lag - pionFeedbackHistory; over > 0 {
		e.overrun.Add(min(over, count))
	}
}

// diagSinceLast returns the counters since the last call.
func (e *viewerEstimator) diagSinceLast() viewerBWEDiag {
	now := viewerBWEDiag{
		Feedbacks:    e.feedbacks.Load(),
		Acked:        e.acked.Load(),
		ReportedLost: e.reportedLost.Load(),
		Overrun:      e.overrun.Load(),
		Padding:      e.padding.Load(),
		MaxLag:       e.maxLag.Swap(0),
	}
	d := viewerBWEDiag{
		Feedbacks:    now.Feedbacks - e.last.Feedbacks,
		Acked:        now.Acked - e.last.Acked,
		ReportedLost: now.ReportedLost - e.last.ReportedLost,
		Overrun:      now.Overrun - e.last.Overrun,
		Padding:      now.Padding - e.last.Padding,
		MaxLag:       now.MaxLag,
	}
	e.last = now
	return d
}

// WriteRTCP passes transport-cc feedback to gcc. It never returns an
// error: the cc interceptor returns it from the RTCP read, and the viewer
// RTCP loop would count a good packet as invalid.
func (e *viewerEstimator) WriteRTCP(pkts []rtcp.Packet, attrs interceptor.Attributes) error {
	feedback := false
	var clean []rtcp.Packet
	for _, pkt := range pkts {
		switch fb := pkt.(type) {
		case *rtcp.TransportLayerCC:
			feedback = true
			e.noteFeedback(fb)
			if fixed := e.sanitize(fb); fixed != nil {
				clean = append(clean, fixed)
			}
		case *rtcp.CCFeedbackReport:
			feedback = true
			clean = append(clean, fb)
		}
	}
	if !feedback {
		return nil
	}
	if err := e.inner.WriteRTCP(clean, attrs); err != nil {
		e.feedbackErrs.Add(1)
		return nil
	}
	e.lastFeedback.Store(time.Now().UnixNano())
	return nil
}

func (e *viewerEstimator) GetTargetBitrate() int             { return e.inner.GetTargetBitrate() }
func (e *viewerEstimator) OnTargetBitrateChange(f func(int)) { e.inner.OnTargetBitrateChange(f) }
func (e *viewerEstimator) GetStats() map[string]any          { return e.inner.GetStats() }
func (e *viewerEstimator) estimateAt(now time.Time) (float32, bool) {
	last := e.lastFeedback.Load()
	if last == 0 || now.Sub(time.Unix(0, last)) > streamREMBFreshness {
		return 0, false
	}
	return float32(e.inner.GetTargetBitrate()), true
}

func (e *viewerEstimator) Close() error {
	if e.registry != nil {
		e.registry.remove(e.id, e)
	}
	return e.inner.Close()
}

// viewerBWERegistry hands the estimator of a new PeerConnection (created
// in the interceptor chain, keyed by the PeerConnection ID) to the code
// that created the PeerConnection.
type viewerBWERegistry struct {
	mu         sync.Mutex
	estimators map[string]*viewerEstimator
}

func newViewerBWERegistry() *viewerBWERegistry {
	return &viewerBWERegistry{estimators: make(map[string]*viewerEstimator)}
}

func (r *viewerBWERegistry) add(id string, e *viewerEstimator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.estimators[id] = e
}

// take returns the estimator of the PeerConnection id and forgets it.
func (r *viewerBWERegistry) take(id string) *viewerEstimator {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.estimators[id]
	delete(r.estimators, id)
	return e
}

func (r *viewerBWERegistry) remove(id string, e *viewerEstimator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.estimators[id] == e {
		delete(r.estimators, id)
	}
}

// configureViewerBWE adds Pion's cc interceptor with a gcc estimator per
// PeerConnection to registry. Call it first: the transport-cc header
// extension interceptor must wrap it, so a packet has its transport-wide
// sequence number when the estimator records it. The pacer is a no-op: the
// SFU forwards at once, as before.
func configureViewerBWE(registry *interceptor.Registry, estimators *viewerBWERegistry) error {
	factory, err := cc.NewInterceptor(func() (cc.BandwidthEstimator, error) {
		inner, err := gcc.NewSendSideBWE(
			gcc.SendSideBWEInitialBitrate(viewerBWEInitialBitrate),
			gcc.SendSideBWEMinBitrate(viewerBWEMinBitrate),
			gcc.SendSideBWEMaxBitrate(viewerBWEMaxBitrate),
			gcc.SendSideBWEPacer(gcc.NewNoOpPacer()),
		)
		if err != nil {
			return nil, err
		}
		return &viewerEstimator{inner: inner, registry: estimators}, nil
	})
	if err != nil {
		return err
	}
	factory.OnNewPeerConnection(func(id string, estimator cc.BandwidthEstimator) {
		if e, ok := estimator.(*viewerEstimator); ok {
			e.id = id
			estimators.add(id, e)
		}
	})
	registry.Add(factory)
	return nil
}

// currentEstimator is the gcc estimator of the peer's current
// PeerConnection, nil when it has none.
func (p *Peer) currentEstimator() *viewerEstimator {
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	return p.estimator
}

// viewerBWELogLimit bounds the viewers in one stream_host_remb_tick line.
const viewerBWELogLimit = 4

// viewerBWELogFields is one viewer's estimator state for the tick log:
// Pion's targets and loss, and the feedback counters since the last tick.
func viewerBWELogFields(viewer string, e *viewerEstimator) logging.D {
	d := e.diagSinceLast()
	fields := logging.D{
		"viewer":  viewer,
		"fb":      d.Feedbacks,
		"acked":   d.Acked,
		"lost":    d.ReportedLost,
		"overrun": d.Overrun,
		"padding": d.Padding,
		"max_lag": d.MaxLag,
		"target":  e.inner.GetTargetBitrate(),
	}
	stats := e.inner.GetStats()
	for key, name := range map[string]string{
		"delayTargetBitrate": "delay_target",
		"lossTargetBitrate":  "loss_target",
		"usage":              "usage",
		"state":              "state",
	} {
		fields[name] = stats[key]
	}
	if loss, ok := stats["averageLoss"].(float64); ok {
		fields["avg_loss"] = math.Round(loss*1000) / 1000
	}
	return fields
}
