package metrics

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const HistorySize = 360 // 10s interval = 1 hour

type Metrics struct {
	// Counters (monotonic)
	SessionsCreated        atomic.Int64
	PeersConnected         atomic.Int64
	PeersForceDisconnected atomic.Int64
	PacketsForwarded       atomic.Int64
	BytesForwarded         atomic.Int64
	SnapshotsUploaded      atomic.Int64
	// WebRTC PeerConnection setup outcomes. A sustained WebRTCFailures rate with
	// no WebRTCConnections is the signature of an expired/broken DTLS cert.
	WebRTCConnections atomic.Int64
	WebRTCFailures    atomic.Int64

	// Gauges (current)
	ActiveSessions atomic.Int32
	ActivePeers    atomic.Int32
	// ActiveSyntheticPeers is the subset of ActivePeers on test-mode
	// connections (canary, /sfu-test, soak tools); /stats subtracts it.
	ActiveSyntheticPeers atomic.Int32

	// Bandwidth tracking
	bytesInCounter  atomic.Int64
	bytesOutCounter atomic.Int64
	lastBytesIn     int64
	lastBytesOut    int64
	lastSampleTime  time.Time
	// BandwidthInMbps, BandwidthOutMbps and LastCPUPercent are written by
	// TakeSnapshot under mu. Read them with Rates.
	BandwidthInMbps  float64
	BandwidthOutMbps float64
	LastCPUPercent   float64

	// CPU tracking via process CPU time deltas
	lastCPUSeconds float64

	// History for sparklines
	mu      sync.RWMutex
	history [HistorySize]Snapshot
	head    int
	count   int

	startTime time.Time
}

type Snapshot struct {
	Timestamp    time.Time `json:"ts"`
	Peers        int       `json:"peers"`
	Sessions     int       `json:"sessions"`
	BandwidthOut float64   `json:"bw_out"`
	CPU          float64   `json:"cpu"`
	MemoryMB     float64   `json:"mem"`
}

func New() *Metrics {
	initialCPU, _ := processCPUSeconds()
	return &Metrics{
		startTime:      time.Now(),
		lastSampleTime: time.Now(),
		lastCPUSeconds: initialCPU,
	}
}

func (m *Metrics) StartTime() time.Time {
	return m.startTime
}

func (m *Metrics) UptimeSeconds() int64 {
	return int64(time.Since(m.startTime).Seconds())
}

// HostMemoryPercent returns used physical memory as a percent of total (0-100),
// or 0 when the platform has no supported implementation.
func (m *Metrics) HostMemoryPercent() float64 {
	pct, _ := hostMemoryPercent()
	return pct
}

func (m *Metrics) RecordBytesIn(n int) {
	m.bytesInCounter.Add(int64(n))
	m.BytesForwarded.Add(int64(n))
}

func (m *Metrics) RecordBytesOut(n int) {
	m.bytesOutCounter.Add(int64(n))
}

func (m *Metrics) RecordPacketForwarded() {
	m.PacketsForwarded.Add(1)
}

// TakeSnapshot is called every 10 seconds by a background goroutine.
func (m *Metrics) TakeSnapshot() {
	now := time.Now()
	elapsed := now.Sub(m.lastSampleTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}

	currentIn := m.bytesInCounter.Load()
	currentOut := m.bytesOutCounter.Load()

	bwIn := float64(currentIn-m.lastBytesIn) * 8 / elapsed / 1_000_000
	bwOut := float64(currentOut-m.lastBytesOut) * 8 / elapsed / 1_000_000

	m.lastBytesIn = currentIn
	m.lastBytesOut = currentOut
	m.lastSampleTime = now

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.mu.RLock()
	cpuPercent := m.LastCPUPercent
	m.mu.RUnlock()
	if currentCPU, ok := processCPUSeconds(); ok {
		deltaCPU := currentCPU - m.lastCPUSeconds
		if deltaCPU < 0 {
			deltaCPU = 0
		}
		numCPU := float64(runtime.NumCPU())
		if numCPU <= 0 {
			numCPU = 1
		}
		// Report process CPU as host-relative percent (0-100 for full host saturation).
		cpuPercent = (deltaCPU / elapsed) * (100.0 / numCPU)
		if cpuPercent < 0 {
			cpuPercent = 0
		}
		if cpuPercent > 100 {
			cpuPercent = 100
		}
		m.lastCPUSeconds = currentCPU
	}

	snap := Snapshot{
		Timestamp:    now,
		Peers:        int(m.ActivePeers.Load()),
		Sessions:     int(m.ActiveSessions.Load()),
		BandwidthOut: bwOut,
		CPU:          cpuPercent,
		MemoryMB:     float64(memStats.Alloc) / 1_000_000,
	}

	m.mu.Lock()
	m.BandwidthInMbps = bwIn
	m.BandwidthOutMbps = bwOut
	m.LastCPUPercent = cpuPercent
	m.history[m.head] = snap
	m.head = (m.head + 1) % HistorySize
	if m.count < HistorySize {
		m.count++
	}
	m.mu.Unlock()
}

// Rates returns the bandwidth and CPU of the last snapshot. Safe to call
// while TakeSnapshot runs.
func (m *Metrics) Rates() (inMbps, outMbps, cpuPercent float64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.BandwidthInMbps, m.BandwidthOutMbps, m.LastCPUPercent
}

func (m *Metrics) History() []Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Snapshot, m.count)
	for i := 0; i < m.count; i++ {
		idx := (m.head - m.count + i + HistorySize) % HistorySize
		result[i] = m.history[idx]
	}
	return result
}

// RunCollector starts the background snapshot loop.
func (m *Metrics) RunCollector(stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.TakeSnapshot()
		case <-stop:
			return
		}
	}
}
