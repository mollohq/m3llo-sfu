package server

import (
	"math/rand"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/nack"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// The SFU's NACK generator (01-SFU.md §6.1, host RTX). It replaces Pion's
// generator, which has two faults on a host leg with RTX:
//
//   - Pion hands an RTX repair to the track reader outside the media
//     stream's interceptor chain, so its generator never learns that the
//     packet arrived and NACKs it again every 100 ms until it leaves its
//     512-packet log. Under the gate's congested cap (RTT about 170 ms)
//     that was 607 duplicate repairs for 94 recovered packets.
//   - It NACKs a gap again every 100 ms whatever the round trip: on a leg
//     whose RTT is over 100 ms the second NACK leaves before the first
//     repair can arrive.
//
// This generator reads the original sequence number of each RTX repair (the
// RTX stream is bound through the same chain), so a repaired packet is never
// asked for again. It sends the first NACK of a gap at the next tick, and
// asks again only after nackRetryRTTs round trips, measured from the first
// NACK of a packet to its repair. A packet is asked for at most
// nackMaxPerPacket times.
const (
	nackLogSize      = 1024 // sequence numbers per stream; a power of two
	nackTick         = 20 * time.Millisecond
	nackInitialRTT   = 100 * time.Millisecond
	nackMinRetry     = 50 * time.Millisecond
	nackMaxRTT       = time.Second
	nackRetryRTTs    = 1.5
	nackMaxPerPacket = 10
)

// nackGeneratorStats are the counters of one generator (one PeerConnection).
type nackGeneratorStats struct {
	// First is the number of packets asked for at least once; Retries the
	// further requests; Repaired the RTX repairs of a packet asked for;
	// LateOriginals the packets asked for that came on the media stream
	// (late, out of order); GaveUp the packets asked for nackMaxPerPacket
	// times without an answer.
	First, Retries, Repaired, LateOriginals, GaveUp int64
	// RTTMs is the measured NACK-to-repair round trip.
	RTTMs int64
}

// nackGenerators maps a PeerConnection ID to its generator, for the relay
// stats of a stream host (stream_session.go).
var nackGenerators sync.Map

func nackGeneratorFor(pc *webrtc.PeerConnection) *nackGenerator {
	if pc == nil {
		return nil
	}
	if g, ok := nackGenerators.Load(pc.ID()); ok {
		return g.(*nackGenerator)
	}
	return nil
}

type nackGeneratorFactory struct{}

func (nackGeneratorFactory) NewInterceptor(id string) (interceptor.Interceptor, error) {
	g := newNACKGenerator(id, time.Now)
	if id != "" {
		nackGenerators.Store(id, g)
	}
	return g, nil
}

// configureNACK is webrtc.ConfigureNack with the SFU's generator: the nack
// and nack pli feedback on video, Pion's NACK responder (the viewer legs'
// repair cache) and nackGenerator.
func configureNACK(me *webrtc.MediaEngine, registry *interceptor.Registry) error {
	responder, err := nack.NewResponderInterceptor()
	if err != nil {
		return err
	}
	me.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack"}, webrtc.RTPCodecTypeVideo)
	me.RegisterFeedback(webrtc.RTCPFeedback{Type: "nack", Parameter: "pli"}, webrtc.RTPCodecTypeVideo)
	registry.Add(responder)
	registry.Add(nackGeneratorFactory{})
	return nil
}

type nackMissing struct {
	seen  time.Time // when the gap was found
	last  time.Time // the last NACK, zero before the first
	count int
}

// nackStream is the receive log of one media stream.
type nackStream struct {
	received [nackLogSize / 64]uint64
	end      uint16
	started  bool
	missing  map[uint16]*nackMissing
}

func (s *nackStream) set(seq uint16) {
	pos := seq % nackLogSize
	s.received[pos/64] |= 1 << (pos % 64)
}

func (s *nackStream) clear(seq uint16) {
	pos := seq % nackLogSize
	s.received[pos/64] &^= 1 << (pos % 64)
}

type nackGenerator struct {
	interceptor.NoOp
	id  string
	now func() time.Time

	mu      sync.Mutex
	streams map[uint32]*nackStream
	// rtxOf maps an RTX SSRC to its media SSRC.
	rtxOf map[uint32]uint32
	rtt   time.Duration

	first, retries, repaired, lateOriginals, gaveUp atomic.Int64

	senderSSRC uint32
	closeOnce  sync.Once
	closed     chan struct{}
	wg         sync.WaitGroup
}

func newNACKGenerator(id string, now func() time.Time) *nackGenerator {
	return &nackGenerator{
		id:         id,
		now:        now,
		streams:    make(map[uint32]*nackStream),
		rtxOf:      make(map[uint32]uint32),
		senderSSRC: rand.Uint32(), // #nosec G404 -- an RTCP sender SSRC, not a secret
		closed:     make(chan struct{}),
	}
}

func streamWantsNACK(info *interceptor.StreamInfo) bool {
	for _, fb := range info.RTCPFeedback {
		if fb.Type == "nack" && fb.Parameter == "" {
			return true
		}
	}
	return false
}

func (g *nackGenerator) BindRTCPWriter(writer interceptor.RTCPWriter) interceptor.RTCPWriter {
	g.wg.Add(1)
	go g.loop(writer)
	return writer
}

func (g *nackGenerator) BindRemoteStream(info *interceptor.StreamInfo, reader interceptor.RTPReader) interceptor.RTPReader {
	if strings.EqualFold(info.MimeType, webrtc.MimeTypeRTX) {
		ssrc := info.SSRC
		return interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			n, attr, err := reader.Read(b, a)
			if err == nil {
				g.onRTX(ssrc, b[:n])
			}
			return n, attr, err
		})
	}
	if !streamWantsNACK(info) {
		return reader
	}
	g.mu.Lock()
	g.streams[info.SSRC] = &nackStream{missing: make(map[uint16]*nackMissing)}
	if info.SSRCRetransmission != 0 {
		g.rtxOf[info.SSRCRetransmission] = info.SSRC
	}
	g.mu.Unlock()
	ssrc := info.SSRC
	return interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
		n, attr, err := reader.Read(b, a)
		if err != nil {
			return n, attr, err
		}
		if attr == nil {
			attr = make(interceptor.Attributes)
		}
		header, herr := attr.GetRTPHeader(b[:n])
		if herr != nil {
			return n, attr, herr
		}
		g.onMedia(ssrc, header.SequenceNumber)
		return n, attr, nil
	})
}

func (g *nackGenerator) UnbindRemoteStream(info *interceptor.StreamInfo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.streams, info.SSRC)
	for rtx, media := range g.rtxOf {
		if media == info.SSRC || rtx == info.SSRC {
			delete(g.rtxOf, rtx)
		}
	}
}

func (g *nackGenerator) Close() error {
	g.closeOnce.Do(func() { close(g.closed) })
	g.wg.Wait()
	if g.id != "" {
		nackGenerators.CompareAndDelete(g.id, g)
	}
	return nil
}

// onRTX reads the original sequence number of an RTX repair (RFC 4588: the
// first two payload bytes). Padding-only packets carry none.
func (g *nackGenerator) onRTX(rtxSSRC uint32, raw []byte) {
	var header rtp.Header
	hlen, err := header.Unmarshal(raw)
	if err != nil {
		return
	}
	end := len(raw)
	if header.Padding && end > hlen {
		end -= int(raw[end-1])
	}
	if end-hlen < 2 {
		return
	}
	osn := uint16(raw[hlen])<<8 | uint16(raw[hlen+1])
	g.mu.Lock()
	defer g.mu.Unlock()
	media, ok := g.rtxOf[rtxSSRC]
	if !ok && len(g.streams) == 1 {
		// No ssrc-group in the offer: the one media stream.
		for ssrc := range g.streams {
			media = ssrc
		}
		ok = true
	}
	if !ok {
		return
	}
	g.receivedLocked(media, osn, true)
}

func (g *nackGenerator) onMedia(ssrc uint32, seq uint16) {
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.streams[ssrc]
	if s == nil {
		return
	}
	if !s.started {
		s.started = true
		s.end = seq
		s.set(seq)
		return
	}
	diff := seq - s.end
	switch {
	case diff == 0:
		return
	case diff < 0x8000:
		if diff > nackLogSize/2 {
			// A jump (a new host, a restart): no gap to repair.
			s.received = [nackLogSize / 64]uint64{}
			clear(s.missing)
		} else {
			now := g.now()
			for i := s.end + 1; i != seq; i++ {
				s.clear(i)
				s.missing[i] = &nackMissing{seen: now}
			}
		}
		s.end = seq
		s.set(seq)
		g.pruneLocked(s)
	default:
		g.receivedLocked(ssrc, seq, false)
	}
}

// receivedLocked marks a packet behind the newest one: a repair or a late
// original.
func (g *nackGenerator) receivedLocked(ssrc uint32, seq uint16, repair bool) {
	s := g.streams[ssrc]
	if s == nil || !s.started || uint16(s.end-seq) >= nackLogSize {
		return
	}
	s.set(seq)
	m, ok := s.missing[seq]
	if !ok {
		return
	}
	delete(s.missing, seq)
	if m.count == 0 {
		return
	}
	if !repair {
		g.lateOriginals.Add(1)
		return
	}
	g.repaired.Add(1)
	if m.count == 1 {
		// One NACK: the round trip is unambiguous.
		g.noteRTTLocked(g.now().Sub(m.last))
	}
}

func (g *nackGenerator) noteRTTLocked(sample time.Duration) {
	if sample <= 0 {
		return
	}
	sample = min(sample, nackMaxRTT)
	if g.rtt == 0 {
		g.rtt = sample
		return
	}
	g.rtt += (sample - g.rtt) / 8
}

// retryAfterLocked is the wait before a packet is asked for again.
func (g *nackGenerator) retryAfterLocked() time.Duration {
	rtt := g.rtt
	if rtt == 0 {
		rtt = nackInitialRTT
	}
	return max(nackMinRetry, time.Duration(float64(rtt)*nackRetryRTTs))
}

func (g *nackGenerator) pruneLocked(s *nackStream) {
	for seq := range s.missing {
		if uint16(s.end-seq) >= nackLogSize {
			delete(s.missing, seq)
		}
	}
}

// due returns the NACKs to send now, one per stream.
func (g *nackGenerator) due(now time.Time) []rtcp.Packet {
	g.mu.Lock()
	defer g.mu.Unlock()
	retry := g.retryAfterLocked()
	var out []rtcp.Packet
	for ssrc, s := range g.streams {
		var seqs []uint16
		for seq, m := range s.missing {
			switch {
			case m.count == 0:
				g.first.Add(1)
			case now.Sub(m.last) < retry:
				continue
			case m.count >= nackMaxPerPacket:
				delete(s.missing, seq)
				g.gaveUp.Add(1)
				continue
			default:
				g.retries.Add(1)
			}
			m.count++
			m.last = now
			seqs = append(seqs, seq)
		}
		if len(seqs) == 0 {
			continue
		}
		// Oldest first (wrap-aware: the farthest behind the newest).
		slices.SortFunc(seqs, func(a, b uint16) int { return int(uint16(s.end-b)) - int(uint16(s.end-a)) })
		out = append(out, &rtcp.TransportLayerNack{
			SenderSSRC: g.senderSSRC,
			MediaSSRC:  ssrc,
			Nacks:      rtcp.NackPairsFromSequenceNumbers(seqs),
		})
	}
	return out
}

func (g *nackGenerator) loop(writer interceptor.RTCPWriter) {
	defer g.wg.Done()
	ticker := time.NewTicker(nackTick)
	defer ticker.Stop()
	for {
		select {
		case <-g.closed:
			return
		case <-ticker.C:
			for _, pkt := range g.due(g.now()) {
				_, _ = writer.Write([]rtcp.Packet{pkt}, interceptor.Attributes{})
			}
		}
	}
}

// stats returns the generator's counters.
func (g *nackGenerator) stats() nackGeneratorStats {
	g.mu.Lock()
	rtt := g.rtt
	g.mu.Unlock()
	return nackGeneratorStats{
		First:         g.first.Load(),
		Retries:       g.retries.Load(),
		Repaired:      g.repaired.Load(),
		LateOriginals: g.lateOriginals.Load(),
		GaveUp:        g.gaveUp.Load(),
		RTTMs:         rtt.Milliseconds(),
	}
}
