package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	stdDraw "image/draw"
	"image/jpeg"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/rtp"
	xdraw "golang.org/x/image/draw"
)

const (
	targetSnapshotWidth  = 1280
	targetSnapshotHeight = 720
)

type PipelineConfig struct {
	SessionID string
	CrewID    string
	Interval  time.Duration
	Logger    *logging.Logger
	R2        *R2Client
	OnUpload  func(key string)
}

type idrCandidate struct {
	annexB []byte
	meta   auMeta
}

type Pipeline struct {
	sessionID string
	crewID    string
	logger    *logging.Logger
	r2        *R2Client
	onUpload  func(key string)
	rateGate  *snapshotRateGate
	assembler *rtpAUAssembler

	tapCh    chan *rtp.Packet
	decodeCh chan idrCandidate
	stopCh   chan struct{}
	doneCh   chan struct{}

	tapDropped    atomic.Uint64
	decodeSkipped atomic.Uint64
	decodeFailed  atomic.Uint64

	startOnce sync.Once
	stopOnce  sync.Once
	wg        sync.WaitGroup
	started   atomic.Bool
}

func NewPipeline(cfg PipelineConfig) *Pipeline {
	interval := cfg.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	return &Pipeline{
		sessionID: cfg.SessionID,
		crewID:    cfg.CrewID,
		logger:    cfg.Logger,
		r2:        cfg.R2,
		onUpload:  cfg.OnUpload,
		rateGate:  newSnapshotRateGate(interval),
		assembler: newRTPAUAssembler(),
		tapCh:     make(chan *rtp.Packet, rtpTapBufferSize),
		decodeCh:  make(chan idrCandidate, 1),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// FeedRTP taps one host RTP packet into the snapshot pipeline without blocking.
func (p *Pipeline) FeedRTP(pkt *rtp.Packet) {
	if pkt == nil || !p.started.Load() {
		return
	}
	select {
	case p.tapCh <- pkt.Clone():
	default:
		p.tapDropped.Add(1)
	}
}

func (p *Pipeline) Start() {
	p.startOnce.Do(func() {
		p.started.Store(true)
		p.logger.Info("snapshot_pipeline_started", logging.D{
			"session": p.sessionID,
			"crew":    p.crewID,
		})
		p.wg.Add(2)
		go func() {
			defer p.wg.Done()
			p.ingestLoop()
		}()
		go func() {
			defer p.wg.Done()
			p.decodeLoop()
		}()
	})
}

func (p *Pipeline) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		if p.started.Load() {
			p.wg.Wait()
			close(p.doneCh)
		}
	})
}

func (p *Pipeline) ingestLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		case pkt := <-p.tapCh:
			p.processRTP(pkt)
		}
	}
}

func (p *Pipeline) decodeLoop() {
	for {
		select {
		case <-p.stopCh:
			return
		case candidate := <-p.decodeCh:
			p.decodeIDR(candidate)
		}
	}
}

func (p *Pipeline) processRTP(pkt *rtp.Packet) {
	result := p.assembler.feed(pkt)
	if !result.complete || !result.isIDR {
		return
	}
	if !result.decodable {
		p.decodeSkipped.Add(1)
		p.logger.Debug("snapshot_decode_skipped_no_params", logging.D{
			"session":    p.sessionID,
			"nal_count":  result.meta.nalCount,
			"has_sps":    result.meta.hasSPS,
			"has_pps":    result.meta.hasPPS,
			"cached_sps": result.meta.cachedSPS,
			"cached_pps": result.meta.cachedPPS,
		})
		return
	}

	select {
	case p.decodeCh <- idrCandidate{annexB: result.annexB, meta: result.meta}:
	case <-p.stopCh:
	default:
		// Decode worker busy; drop this IDR candidate (rate-gated capture).
	}
}

func (p *Pipeline) decodeIDR(candidate idrCandidate) {
	if !p.rateGate.Allow() {
		return
	}

	p.logger.Debug("snapshot_captured", logging.D{
		"session": p.sessionID,
		"ts":      time.Now().Unix(),
	})

	frame, err := DecodeIDRFrame(candidate.annexB)
	if err != nil {
		if err.Error() == "decoder did not emit a frame" {
			p.logger.Debug("snapshot_decode_no_frame", logging.D{
				"session":   p.sessionID,
				"au_bytes":  len(candidate.annexB),
				"nal_count": candidate.meta.nalCount,
			})
			return
		}
		p.decodeFailed.Add(1)
		p.assembler.clearParameterSets()
		p.logger.Warn("snapshot_decode_failed", logging.D{
			"session":     p.sessionID,
			"error":       err.Error(),
			"au_bytes":    len(candidate.annexB),
			"nal_count":   candidate.meta.nalCount,
			"has_sps":     candidate.meta.hasSPS,
			"has_pps":     candidate.meta.hasPPS,
			"cached_sps":  candidate.meta.cachedSPS,
			"cached_pps":  candidate.meta.cachedPPS,
			"tap_dropped": p.tapDropped.Load(),
		})
		return
	}

	rendered, err := renderSnapshotFrame(frame)
	if err != nil {
		p.logger.Warn("snapshot_render_failed", logging.D{
			"session": p.sessionID,
			"error":   err.Error(),
		})
		return
	}

	var jpegBuf bytes.Buffer
	if err := jpeg.Encode(&jpegBuf, rendered, &jpeg.Options{Quality: 75}); err != nil {
		p.logger.Warn("snapshot_encode_failed", logging.D{
			"session": p.sessionID,
			"error":   err.Error(),
		})
		return
	}

	tsMillis := time.Now().UnixMilli()
	key := fmt.Sprintf("snapshots/%s/%s/%d.jpg", p.crewID, p.sessionID, tsMillis)
	jpegBytes := append([]byte(nil), jpegBuf.Bytes()...)
	go p.upload(key, jpegBytes)
}

func (p *Pipeline) upload(key string, data []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := p.r2.PutObject(ctx, key, data); err != nil {
		p.logger.Warn("snapshot_upload_failed", logging.D{
			"session": p.sessionID,
			"key":     key,
			"error":   err.Error(),
		})
		return
	}

	p.logger.Info("snapshot_uploaded", logging.D{
		"session": p.sessionID,
		"key":     key,
		"size_kb": len(data) / 1024,
	})
	if p.onUpload != nil {
		p.onUpload(key)
	}
}

func annexBWrap(nal []byte) []byte {
	buf := make([]byte, 4+len(nal))
	copy(buf, annexBPrefix)
	copy(buf[4:], nal)
	return buf
}

func renderSnapshotFrame(src *image.YCbCr) (*image.RGBA, error) {
	if src == nil {
		return nil, fmt.Errorf("nil decoded frame")
	}
	srcBounds := src.Bounds()
	srcW, srcH := srcBounds.Dx(), srcBounds.Dy()
	if srcW <= 0 || srcH <= 0 {
		return nil, fmt.Errorf("invalid decoded dimensions: %dx%d", srcW, srcH)
	}

	targetW := srcW
	targetH := srcH
	if srcW > targetSnapshotWidth || srcH > targetSnapshotHeight {
		scaleW := float64(targetSnapshotWidth) / float64(srcW)
		scaleH := float64(targetSnapshotHeight) / float64(srcH)
		scale := scaleW
		if scaleH < scale {
			scale = scaleH
		}
		targetW = int(float64(srcW) * scale)
		targetH = int(float64(srcH) * scale)
		if targetW < 1 {
			targetW = 1
		}
		if targetH < 1 {
			targetH = 1
		}
	}

	dst := image.NewRGBA(image.Rect(0, 0, targetSnapshotWidth, targetSnapshotHeight))
	stdDraw.Draw(dst, dst.Bounds(), &image.Uniform{C: color.Black}, image.Point{}, stdDraw.Src)

	dstX := (targetSnapshotWidth - targetW) / 2
	dstY := (targetSnapshotHeight - targetH) / 2
	dstRect := image.Rect(dstX, dstY, dstX+targetW, dstY+targetH)

	if targetW == srcW && targetH == srcH {
		stdDraw.Draw(dst, dstRect, src, srcBounds.Min, stdDraw.Src)
		return dst, nil
	}

	xdraw.BiLinear.Scale(dst, dstRect, src, srcBounds, stdDraw.Over, nil)
	return dst, nil
}
