package snapshot

import (
	"sync"
	"time"
)

type snapshotRateGate struct {
	interval      time.Duration
	lastCaptureAt time.Time
	mu            sync.Mutex
}

func newSnapshotRateGate(interval time.Duration) *snapshotRateGate {
	return &snapshotRateGate{interval: interval}
}

func (g *snapshotRateGate) Allow() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	if time.Since(g.lastCaptureAt) < g.interval {
		return false
	}
	g.lastCaptureAt = time.Now()
	return true
}
