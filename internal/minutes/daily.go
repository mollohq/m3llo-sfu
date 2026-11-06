package minutes

import (
	"sync"
	"time"
)

// DayKey is one row of the per-day accrual: a UTC day, the session type
// (voice, stream), the peer role (member, host, viewer) and whether the
// peer was synthetic (test mode: canary, soak tools).
type DayKey struct {
	Day       string // YYYY-MM-DD, UTC
	Type      string
	Role      string
	Synthetic bool
}

// DailyMinutes accrues peer minutes per day, type and role for the media
// store (/admin/api/minutes). It counts every peer, with the synthetic ones
// on their own rows.
type DailyMinutes struct {
	mu      sync.Mutex
	accrued map[DayKey]float64
}

func NewDailyMinutes() *DailyMinutes {
	return &DailyMinutes{accrued: make(map[DayKey]float64)}
}

// Add accrues the time from..to of one peer. A span over UTC midnight is
// split between the two days.
func (d *DailyMinutes) Add(from, to time.Time, typ, role string, synthetic bool) {
	if !to.After(from) {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	from, to = from.UTC(), to.UTC()
	for from.Before(to) {
		midnight := time.Date(from.Year(), from.Month(), from.Day()+1, 0, 0, 0, 0, time.UTC)
		end := to
		if midnight.Before(end) {
			end = midnight
		}
		key := DayKey{Day: from.Format("2006-01-02"), Type: typ, Role: role, Synthetic: synthetic}
		d.accrued[key] += end.Sub(from).Minutes()
		from = end
	}
}

// Flush returns the minutes accrued since the last flush and resets them.
func (d *DailyMinutes) Flush() map[DayKey]float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.accrued) == 0 {
		return nil
	}
	out := d.accrued
	d.accrued = make(map[DayKey]float64)
	return out
}
