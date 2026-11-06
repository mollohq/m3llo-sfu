package server

import (
	"testing"
	"time"
)

// A flood of one event logs limit entries per window. The first entry of
// the next window carries the count that was dropped.
func TestLogSamplerCapsEachWindow(t *testing.T) {
	l := newLogSampler(3, time.Minute)
	now := time.Unix(1000, 0)
	logged := 0
	for i := 0; i < 100; i++ {
		if _, ok := l.allow(now); ok {
			logged++
		}
	}
	if logged != 3 {
		t.Fatalf("logged %d in one window, want 3", logged)
	}
	suppressed, ok := l.allow(now.Add(time.Minute))
	if !ok || suppressed != 97 {
		t.Fatalf("next window: ok=%v suppressed=%d, want true 97", ok, suppressed)
	}
	if suppressed, _ := l.allow(now.Add(time.Minute)); suppressed != 0 {
		t.Fatalf("suppressed count not reset: %d", suppressed)
	}
}
