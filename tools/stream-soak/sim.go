package main

import (
	"math"
	"math/rand"
	"time"
)

// simImpairment is the host-side impairment of --sim-drop-pct and
// --sim-jitter-ms: it keeps a share of the video packets off the wire (the
// SFU sees ingress loss and NACKs the host) and delays whole frames (the SFU
// sees jittered ingress). Seeded, so a run repeats.
type simImpairment struct {
	dropPct  float64
	jitterMs float64
	rng      *rand.Rand
}

func newSimImpairment(dropPct, jitterMs float64, seed int64) *simImpairment {
	return &simImpairment{dropPct: dropPct, jitterMs: jitterMs, rng: rand.New(rand.NewSource(seed))}
}

// drop reports whether the next packet stays off the wire.
func (s *simImpairment) drop() bool {
	return s.dropPct > 0 && s.rng.Float64()*100 < s.dropPct
}

// frameDelay is the extra delay of the next frame: a half-normal draw with
// sigma jitterMs.
func (s *simImpairment) frameDelay() time.Duration {
	if s.jitterMs <= 0 {
		return 0
	}
	d := math.Abs(s.rng.NormFloat64()) * s.jitterMs
	return time.Duration(d * float64(time.Millisecond))
}
