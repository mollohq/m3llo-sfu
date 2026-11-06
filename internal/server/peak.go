package server

import (
	"sync"
	"time"
)

// stockholmLocation is Europe/Stockholm, the day boundary used for
// peak_sessions_today. tzdata is embedded (see cmd/sfu/main.go), so this loads
// even on the tzdata-less alpine runtime image; UTC is a defensive fallback.
var stockholmLocation = func() *time.Location {
	loc, err := time.LoadLocation("Europe/Stockholm")
	if err != nil {
		return time.UTC
	}
	return loc
}()

func todayStockholm() string {
	return time.Now().In(stockholmLocation).Format("2006-01-02")
}

// peakSessionsTracker records the high-water mark of concurrent sessions for the
// current calendar day in Europe/Stockholm, resetting at local midnight.
type peakSessionsTracker struct {
	mu   sync.Mutex
	day  string // YYYY-MM-DD in Stockholm
	peak int
}

func newPeakSessionsTracker() *peakSessionsTracker {
	return &peakSessionsTracker{day: todayStockholm()}
}

// rollover resets the peak when the Stockholm day has changed. Caller holds mu.
func (p *peakSessionsTracker) rollover() {
	if today := todayStockholm(); today != p.day {
		p.day = today
		p.peak = 0
	}
}

// Observe records the current concurrent session count, updating the daily peak.
func (p *peakSessionsTracker) Observe(current int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	if current > p.peak {
		p.peak = current
	}
}

// Peak returns today's peak. It rolls over first so a quiet SFU after midnight
// reports 0 rather than yesterday's value even if Observe has not run yet.
func (p *peakSessionsTracker) Peak() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rollover()
	return p.peak
}
