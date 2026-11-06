package server

import (
	"sync"
	"time"
)

// Failed password attempts (admin Basic Auth, test-mode join) per client
// address: after authFailMax failures within authFailWindow, the address gets
// 429 until its oldest failure leaves the window.
const (
	authFailMax    = 10
	authFailWindow = time.Minute
	// authFailMaxAddrs bounds the map: past it, every address with no
	// failure in the window is dropped.
	authFailMaxAddrs = 4096
	// authFailLogMax caps auth_failed log entries of /ws per minute.
	authFailLogMax = 30
)

type failLimiter struct {
	mu     sync.Mutex
	max    int
	window time.Duration
	now    func() time.Time
	fails  map[string][]time.Time
}

func newFailLimiter(max int, window time.Duration) *failLimiter {
	return &failLimiter{max: max, window: window, now: time.Now, fails: make(map[string][]time.Time)}
}

// Blocked reports whether addr has used up its failures in the window. A nil
// limiter blocks nothing.
func (l *failLimiter) Blocked(addr string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.recentLocked(addr)) >= l.max
}

// Fail records a failed attempt from addr.
func (l *failLimiter) Fail(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fails[addr] = append(l.recentLocked(addr), l.now())
	if len(l.fails) > authFailMaxAddrs {
		for a := range l.fails {
			if len(l.recentLocked(a)) == 0 {
				delete(l.fails, a)
			}
		}
	}
}

// Succeed clears the failures of addr.
func (l *failLimiter) Succeed(addr string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.fails, addr)
}

// recentLocked drops the failures of addr older than the window and returns
// the rest.
func (l *failLimiter) recentLocked(addr string) []time.Time {
	times := l.fails[addr]
	cut := l.now().Add(-l.window)
	i := 0
	for i < len(times) && !times[i].After(cut) {
		i++
	}
	if i == len(times) {
		delete(l.fails, addr)
		return nil
	}
	times = times[i:]
	l.fails[addr] = times
	return times
}
