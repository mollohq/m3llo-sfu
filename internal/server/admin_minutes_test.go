package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Scripted day rows: the endpoint sums voice and stream per UTC day for
// real users, keeps synthetic peers apart, fills the days with no row, and
// leaves out the days before the range.
func TestAdminMinutesPerDay(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	st := s.media.store
	now := time.Now().UTC()
	day := func(back int) string { return now.AddDate(0, 0, -back).Format("2006-01-02") }

	// Today: a voice call (3 members) and a stream (host and viewers).
	st.AddMinutes(day(0), "voice", "member", false, 30)
	st.AddMinutes(day(0), "stream", "host", false, 10)
	st.AddMinutes(day(0), "stream", "viewer", false, 20.25)
	st.AddMinutes(day(0), "stream", "viewer", true, 50) // a soak's viewers
	// Two days ago: voice only, and a synthetic voice row.
	st.AddMinutes(day(2), "voice", "member", false, 12.5)
	st.AddMinutes(day(2), "voice", "member", true, 4)
	// Six days ago: the first day of a 7-day range.
	st.AddMinutes(day(6), "stream", "host", false, 7)
	// Seven days ago: outside a 7-day range, inside 14.
	st.AddMinutes(day(7), "voice", "member", false, 100)
	// The same row twice adds up.
	st.AddMinutes(day(2), "voice", "member", false, 0.5)
	st.Flush()

	get := func(q string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		s.handleAdminMinutes(rec, httptest.NewRequest(http.MethodGet, "/admin/api/minutes"+q, nil))
		return rec
	}
	decode := func(rec *httptest.ResponseRecorder) minutesResponse {
		t.Helper()
		if rec.Code != http.StatusOK {
			t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
		}
		var out minutesResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}

	week := decode(get("?days=7"))
	if week.Days != 7 || len(week.PerDay) != 7 || week.FromDay != day(6) || week.ToDay != day(0) {
		t.Fatalf("range = %d days, %d rows, %s to %s", week.Days, len(week.PerDay), week.FromDay, week.ToDay)
	}
	if week.Unit != "peer_minutes" || !strings.Contains(week.UnitNote, "one peer connected for one minute") {
		t.Fatalf("unit = %q %q", week.Unit, week.UnitNote)
	}
	for i, d := range week.PerDay {
		if d.Day != day(6-i) {
			t.Fatalf("per_day[%d] = %s, want %s (oldest first, no gap)", i, d.Day, day(6-i))
		}
	}
	if got := week.PerDay[6]; got.Voice != 30 || got.Stream != 30.3 || got.Synthetic.Stream != 50 || got.Synthetic.Voice != 0 {
		t.Fatalf("today = %+v", got)
	}
	if got := week.PerDay[4]; got.Voice != 13 || got.Stream != 0 || got.Synthetic.Voice != 4 {
		t.Fatalf("two days ago = %+v", got)
	}
	if got := week.PerDay[0]; got.Stream != 7 || got.Voice != 0 {
		t.Fatalf("six days ago = %+v", got)
	}
	if got := week.PerDay[5]; got.Voice != 0 || got.Stream != 0 || got.Synthetic.Voice != 0 || got.Synthetic.Stream != 0 {
		t.Fatalf("a day with no row = %+v", got)
	}
	if week.Voice != 43 || week.Stream != 37.3 || week.Synthetic.Voice != 4 || week.Synthetic.Stream != 50 {
		t.Fatalf("7-day totals = voice %v stream %v synthetic %+v", week.Voice, week.Stream, week.Synthetic)
	}

	// The default is 14 days and takes in the row of seven days ago.
	fortnight := decode(get(""))
	if fortnight.Days != 14 || len(fortnight.PerDay) != 14 || fortnight.Voice != 143 {
		t.Fatalf("default = %d days, %d rows, voice %v", fortnight.Days, len(fortnight.PerDay), fortnight.Voice)
	}
	if one := decode(get("?days=1")); len(one.PerDay) != 1 || one.Voice != 30 || one.FromDay != day(0) {
		t.Fatalf("days=1 = %+v", one)
	}
	if max := decode(get("?days=90")); len(max.PerDay) != 90 {
		t.Fatalf("days=90 gave %d rows", len(max.PerDay))
	}

	for _, q := range []string{"?days=0", "?days=91", "?days=-3", "?days=x", "?days=7.5"} {
		if rec := get(q); rec.Code != http.StatusBadRequest {
			t.Errorf("GET minutes%s = %d, want 400", q, rec.Code)
		}
	}
}

// A day row over UTC midnight lands on its own UTC day, also when the
// clock of "now" is not UTC.
func TestAdminMinutesUsesUTCDays(t *testing.T) {
	s := newStoreTestServer(t, filepath.Join(t.TempDir(), "media.db"), 0)
	st := s.media.store
	st.AddMinutes("2026-03-01", "voice", "member", false, 5)
	st.AddMinutes("2026-03-02", "voice", "member", false, 7)
	st.Flush()
	// 2026-03-02 01:30 in UTC is 2026-03-01 in New York.
	now := time.Date(2026, 3, 2, 1, 30, 0, 0, time.UTC).In(time.FixedZone("EST", -5*3600))
	out, err := minutesByDay(st, 2, now)
	if err != nil {
		t.Fatal(err)
	}
	if out.FromDay != "2026-03-01" || out.ToDay != "2026-03-02" || out.PerDay[1].Voice != 7 || out.Voice != 12 {
		t.Fatalf("minutes = %+v", out)
	}
}

// Without the store the endpoint answers 503, as the other store endpoints.
func TestAdminMinutesWithoutTheStore(t *testing.T) {
	s := &Server{}
	rec := httptest.NewRecorder()
	s.handleAdminMinutes(rec, httptest.NewRequest(http.MethodGet, "/admin/api/minutes?days=7", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "store disabled") {
		t.Fatalf("without the store = %d %q", rec.Code, rec.Body.String())
	}
}
