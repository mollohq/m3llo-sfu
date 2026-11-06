package main

import (
	"sort"
	"sync"
	"time"
)

type viewerMetrics struct {
	id string

	mu sync.Mutex

	packets       int64
	bytes         int64
	seqGaps       int64
	duplicates    int64
	outOfOrder    int64
	haveSeq       bool
	havePrevTime  bool
	lastSeq       uint16
	prevPacketNs  int64
	firstPacketNs int64
	interArrival  []int64

	audioPackets int64
}

func (m *viewerMetrics) onAudioPacket() {
	m.mu.Lock()
	m.audioPackets++
	m.mu.Unlock()
}

func (m *viewerMetrics) onPacket(seq uint16, payloadLen int, now time.Time) {
	nowNs := now.UnixNano()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.packets == 0 {
		m.firstPacketNs = nowNs
	}
	m.packets++
	m.bytes += int64(payloadLen)

	if !m.haveSeq {
		m.haveSeq = true
		m.lastSeq = seq
		m.prevPacketNs = nowNs
		m.havePrevTime = true
		return
	}

	if seq == m.lastSeq {
		m.duplicates++
		return
	}

	if seqLess(seq, m.lastSeq) {
		m.outOfOrder++
		return
	}

	if gap := seqDiff(m.lastSeq, seq); gap > 1 {
		m.seqGaps += int64(gap - 1)
	}
	if m.havePrevTime {
		m.interArrival = append(m.interArrival, nowNs-m.prevPacketNs)
	}
	m.prevPacketNs = nowNs
	m.havePrevTime = true
	m.lastSeq = seq
}

type viewerSnapshot struct {
	id            string
	packets       int64
	bytes         int64
	seqGaps       int64
	duplicates    int64
	outOfOrder    int64
	firstPacketNs int64
	interArrival  []int64
	audioPackets  int64
}

func (m *viewerMetrics) snapshot() viewerSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	ia := make([]int64, len(m.interArrival))
	copy(ia, m.interArrival)
	return viewerSnapshot{
		id:            m.id,
		packets:       m.packets,
		bytes:         m.bytes,
		seqGaps:       m.seqGaps,
		duplicates:    m.duplicates,
		outOfOrder:    m.outOfOrder,
		firstPacketNs: m.firstPacketNs,
		interArrival:  ia,
		audioPackets:  m.audioPackets,
	}
}

func seqDiff(prev, next uint16) uint16 {
	return next - prev
}

func seqLess(a, b uint16) bool {
	return int16(a-b) < 0
}

func p95Millis(valuesNs []int64) float64 {
	if len(valuesNs) == 0 {
		return 0
	}
	sorted := make([]int64, len(valuesNs))
	copy(sorted, valuesNs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * 0.95)
	return float64(sorted[idx]) / float64(time.Millisecond)
}

func firstPacketLatencyMs(firstPacketNs, pumpStartNs int64) float64 {
	if firstPacketNs <= 0 || pumpStartNs <= 0 || firstPacketNs <= pumpStartNs {
		return 0
	}
	return float64(firstPacketNs-pumpStartNs) / float64(time.Millisecond)
}
