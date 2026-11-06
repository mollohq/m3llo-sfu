package server

import (
	"sync"
	"time"
)

// logSampler lets at most limit entries of one log event through per
// window. It is global, not per address, so it holds no state an attacker
// can grow. The first entry of a new window reports how many the last
// windows dropped.
type logSampler struct {
	mu         sync.Mutex
	limit      int
	window     time.Duration
	start      time.Time
	count      int
	suppressed int
}

func newLogSampler(limit int, window time.Duration) *logSampler {
	return &logSampler{limit: limit, window: window}
}

// allow reports whether to log one more entry, and how many entries were
// dropped since the last one that was logged. A nil sampler allows all.
func (l *logSampler) allow(now time.Time) (suppressed int, ok bool) {
	if l == nil {
		return 0, true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.start) >= l.window {
		l.start, l.count = now, 0
	}
	if l.count >= l.limit {
		l.suppressed++
		return 0, false
	}
	l.count++
	suppressed, l.suppressed = l.suppressed, 0
	return suppressed, true
}
