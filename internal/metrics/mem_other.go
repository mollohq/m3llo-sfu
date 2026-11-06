//go:build !linux && !windows

package metrics

// hostMemoryPercent has no portable implementation on platforms other than
// Linux (production) and Windows (dev). Callers treat the false return as
// "unavailable" and report 0.
func hostMemoryPercent() (float64, bool) {
	return 0, false
}
