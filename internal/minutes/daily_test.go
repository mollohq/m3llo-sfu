package minutes

import (
	"math"
	"testing"
	"time"
)

func TestDailyMinutesSplitsAtUTCMidnight(t *testing.T) {
	d := NewDailyMinutes()
	from := time.Date(2026, 10, 25, 23, 50, 0, 0, time.UTC)
	d.Add(from, from.Add(30*time.Minute), "stream", "viewer", false)
	d.Add(from, from.Add(time.Minute), "stream", "viewer", true)

	got := d.Flush()
	want := map[DayKey]float64{
		{Day: "2026-10-25", Type: "stream", Role: "viewer"}:                  10,
		{Day: "2026-10-26", Type: "stream", Role: "viewer"}:                  20,
		{Day: "2026-10-25", Type: "stream", Role: "viewer", Synthetic: true}: 1,
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	for k, v := range want {
		if math.Abs(got[k]-v) > 1e-9 {
			t.Errorf("%v = %v, want %v", k, got[k], v)
		}
	}
	if d.Flush() != nil {
		t.Fatal("a second flush returned minutes")
	}
}

func TestDailyMinutesIgnoresAnEmptySpan(t *testing.T) {
	d := NewDailyMinutes()
	now := time.Now()
	d.Add(now, now, "voice", "member", false)
	d.Add(now, now.Add(-time.Second), "voice", "member", false)
	if d.Flush() != nil {
		t.Fatal("an empty span accrued minutes")
	}
}
