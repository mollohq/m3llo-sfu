package snapshot

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pion/rtp"
)

const (
	rtpTapBufferSize   = 256
	auMaxPackets       = 256
	auMaxBytes         = 512 * 1024
	auAssemblyDeadline = 120 * time.Millisecond
	fuFragmentExpiry   = 120 * time.Millisecond
	fuMaxInFlight      = 32
)

type auFeedResult struct {
	annexB    []byte
	isIDR     bool
	complete  bool
	decodable bool
	meta      auMeta
}

type auMeta struct {
	nalCount  int
	hasSPS    bool
	hasPPS    bool
	cachedSPS bool
	cachedPPS bool
}

type rtpAUAssembler struct {
	lastSPS []byte
	lastPPS []byte

	curTimestamp uint32
	curNALs      [][]byte
	curPackets   int
	curBytes     int
	curStarted   time.Time
	hasCurrent   bool

	fuFragments map[fuKey]*fuAssembly
}

type fuKey struct {
	timestamp uint32
	nalType   uint8
}

type fuAssembly struct {
	nalHeader byte
	parts     [][]byte
	bytes     int
	started   time.Time
}

func newRTPAUAssembler() *rtpAUAssembler {
	return &rtpAUAssembler{
		fuFragments: make(map[fuKey]*fuAssembly),
	}
}

func (a *rtpAUAssembler) feed(pkt *rtp.Packet) auFeedResult {
	if pkt == nil || len(pkt.Payload) == 0 {
		return auFeedResult{}
	}
	a.cleanupExpired()

	nals, err := a.depacketize(pkt)
	if err != nil || len(nals) == 0 {
		return auFeedResult{}
	}

	if a.hasCurrent && pkt.Timestamp != a.curTimestamp {
		a.resetCurrent()
	}
	if !a.hasCurrent {
		a.curTimestamp = pkt.Timestamp
		a.curStarted = time.Now()
		a.hasCurrent = true
	}

	for _, nal := range nals {
		a.cacheParameterSets(nal)
		if len(nal) == 0 {
			continue
		}
		a.curPackets++
		a.curBytes += len(nal)
		if a.curPackets > auMaxPackets || a.curBytes > auMaxBytes {
			a.resetCurrent()
			return auFeedResult{}
		}
		a.curNALs = append(a.curNALs, append([]byte(nil), nal...))
	}

	if time.Since(a.curStarted) > auAssemblyDeadline {
		a.resetCurrent()
		return auFeedResult{}
	}

	if !pkt.Marker {
		return auFeedResult{}
	}

	annexB, isIDR, decodable, meta := a.finalizeCurrentAU()
	a.resetCurrent()
	if !isIDR {
		return auFeedResult{}
	}
	if !decodable {
		return auFeedResult{
			isIDR:     true,
			complete:  true,
			decodable: false,
			meta:      meta,
		}
	}
	if len(annexB) == 0 {
		return auFeedResult{}
	}
	return auFeedResult{
		annexB:    annexB,
		isIDR:     true,
		complete:  true,
		decodable: true,
		meta:      meta,
	}
}

func (a *rtpAUAssembler) depacketize(pkt *rtp.Packet) ([][]byte, error) {
	payload := pkt.Payload
	if len(payload) < 1 {
		return nil, fmt.Errorf("empty rtp payload")
	}
	nalType := payload[0] & 0x1F
	switch nalType {
	case 1, 5, 6, 7, 8, 9:
		return [][]byte{append([]byte(nil), payload...)}, nil
	case 24:
		return a.parseSTAPA(payload)
	case 28:
		nal, ready, err := a.parseFUA(pkt.Timestamp, payload)
		if err != nil {
			return nil, err
		}
		if !ready {
			return nil, nil
		}
		return [][]byte{nal}, nil
	default:
		return nil, fmt.Errorf("unsupported h264 nal type %d", nalType)
	}
}

func (a *rtpAUAssembler) parseSTAPA(payload []byte) ([][]byte, error) {
	if len(payload) < 3 {
		return nil, fmt.Errorf("stap-a too short")
	}
	var nals [][]byte
	offset := 1
	for offset+2 <= len(payload) {
		size := int(payload[offset])<<8 | int(payload[offset+1])
		offset += 2
		if size <= 0 || offset+size > len(payload) {
			return nil, fmt.Errorf("invalid stap-a size")
		}
		nals = append(nals, append([]byte(nil), payload[offset:offset+size]...))
		offset += size
	}
	return nals, nil
}

func (a *rtpAUAssembler) parseFUA(timestamp uint32, payload []byte) ([]byte, bool, error) {
	if len(payload) < 2 {
		return nil, false, fmt.Errorf("fu-a too short")
	}
	fuIndicator := payload[0]
	fuHeader := payload[1]
	start := fuHeader&0x80 != 0
	end := fuHeader&0x40 != 0
	nalType := fuHeader & 0x1F
	fragment := payload[2:]
	key := fuKey{timestamp: timestamp, nalType: nalType}

	if start {
		if len(a.fuFragments) >= fuMaxInFlight {
			a.dropOldestFU()
		}
		a.fuFragments[key] = &fuAssembly{
			nalHeader: (fuIndicator & 0xE0) | nalType,
			parts:     [][]byte{append([]byte(nil), fragment...)},
			bytes:     len(fragment),
			started:   time.Now(),
		}
		if end {
			nal := a.completeFU(key)
			return nal, true, nil
		}
		return nil, false, nil
	}

	assembly, ok := a.fuFragments[key]
	if !ok {
		return nil, false, nil
	}
	assembly.parts = append(assembly.parts, append([]byte(nil), fragment...))
	assembly.bytes += len(fragment)
	if assembly.bytes > auMaxBytes {
		delete(a.fuFragments, key)
		return nil, false, fmt.Errorf("fu-a overflow")
	}
	if !end {
		return nil, false, nil
	}
	nal := a.completeFU(key)
	return nal, true, nil
}

func (a *rtpAUAssembler) completeFU(key fuKey) []byte {
	assembly := a.fuFragments[key]
	delete(a.fuFragments, key)
	if assembly == nil {
		return nil
	}
	var buf bytes.Buffer
	buf.WriteByte(assembly.nalHeader)
	for _, part := range assembly.parts {
		buf.Write(part)
	}
	return buf.Bytes()
}

func (a *rtpAUAssembler) finalizeCurrentAU() ([]byte, bool, bool, auMeta) {
	meta := auMeta{nalCount: len(a.curNALs)}
	if len(a.curNALs) == 0 {
		return nil, false, false, meta
	}

	isIDR := false
	hasSPS, hasPPS := false, false
	for _, nal := range a.curNALs {
		if len(nal) == 0 {
			continue
		}
		switch nal[0] & 0x1F {
		case 5:
			isIDR = true
		case 7:
			hasSPS = true
		case 8:
			hasPPS = true
		}
	}
	meta.hasSPS = hasSPS
	meta.hasPPS = hasPPS
	meta.cachedSPS = len(a.lastSPS) > 0
	meta.cachedPPS = len(a.lastPPS) > 0

	if !isIDR {
		return nil, false, false, meta
	}

	injectSPS := !hasSPS && meta.cachedSPS
	injectPPS := !hasPPS && meta.cachedPPS
	if (!hasSPS && !injectSPS) || (!hasPPS && !injectPPS) {
		return nil, true, false, meta
	}

	var out bytes.Buffer
	if injectSPS {
		out.Write(a.lastSPS)
	}
	if injectPPS {
		out.Write(a.lastPPS)
	}
	for _, nal := range a.curNALs {
		out.Write(annexBPrefix)
		out.Write(nal)
	}
	return out.Bytes(), true, true, meta
}

func (a *rtpAUAssembler) cacheParameterSets(nal []byte) {
	if len(nal) == 0 {
		return
	}
	switch nal[0] & 0x1F {
	case 7:
		a.lastSPS = annexBWrap(nal)
	case 8:
		a.lastPPS = annexBWrap(nal)
	}
}

func (a *rtpAUAssembler) clearParameterSets() {
	a.lastSPS = nil
	a.lastPPS = nil
}

func (a *rtpAUAssembler) resetCurrent() {
	a.curNALs = a.curNALs[:0]
	a.curPackets = 0
	a.curBytes = 0
	a.hasCurrent = false
}

func (a *rtpAUAssembler) cleanupExpired() {
	now := time.Now()
	for key, assembly := range a.fuFragments {
		if now.Sub(assembly.started) > fuFragmentExpiry {
			delete(a.fuFragments, key)
		}
	}
	if a.hasCurrent && now.Sub(a.curStarted) > auAssemblyDeadline {
		a.resetCurrent()
	}
}

func (a *rtpAUAssembler) dropOldestFU() {
	var oldestKey fuKey
	var oldestAt time.Time
	found := false
	for key, assembly := range a.fuFragments {
		if !found || assembly.started.Before(oldestAt) {
			oldestKey = key
			oldestAt = assembly.started
			found = true
		}
	}
	if found {
		delete(a.fuFragments, oldestKey)
	}
}

var annexBPrefix = []byte{0x00, 0x00, 0x00, 0x01}
