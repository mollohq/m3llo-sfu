package server

import (
	"net/http"
	"strconv"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/store"
)

// GET /admin/api/minutes?days=N: the peer minutes per UTC day, for an admin
// UI's usage tiles (01-SFU.md section 8.2).

const (
	minutesDaysDefault = 14
	minutesDaysMax     = 90
)

// minutesUnitNote is in every answer, so a reader of "voice minutes" knows
// they are participant minutes, not call minutes.
const minutesUnitNote = "A peer minute is one peer connected for one minute. " +
	"A voice call of 3 people for 10 minutes is 30 voice minutes; " +
	"a stream's host and each viewer count."

type minutesPair struct {
	Voice  float64 `json:"voice_minutes"`
	Stream float64 `json:"stream_minutes"`
}

func (p *minutesPair) add(typ string, v float64) {
	switch typ {
	case "voice":
		p.Voice += v
	case "stream":
		p.Stream += v
	}
}

func (p minutesPair) rounded() minutesPair {
	return minutesPair{Voice: round1(p.Voice), Stream: round1(p.Stream)}
}

type minutesDay struct {
	Day string `json:"day"`
	minutesPair
	Synthetic minutesPair `json:"synthetic"`
}

type minutesResponse struct {
	Days     int    `json:"days"`
	FromDay  string `json:"from_day"`
	ToDay    string `json:"to_day"`
	Unit     string `json:"unit"`
	UnitNote string `json:"unit_note"`
	minutesPair
	Synthetic minutesPair  `json:"synthetic"`
	PerDay    []minutesDay `json:"per_day"`
}

func (s *Server) handleAdminMinutes(w http.ResponseWriter, r *http.Request) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	days := minutesDaysDefault
	if v := r.URL.Query().Get("days"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > minutesDaysMax {
			http.Error(w, "days is 1 to 90", http.StatusBadRequest)
			return
		}
		days = n
	}
	out, err := minutesByDay(st, days, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, out)
}

// minutesByDay returns the days UTC days that end with today (today is the
// last one and is not complete), oldest first, each day present. Real users
// and synthetic peers (test mode: canary, soak tools) are apart, as in the
// overview's "minutes today". The minutes are the store's day rows, which
// the media tick accrues every 10 s from each peer's connected time.
func minutesByDay(st *store.Store, days int, now time.Time) (minutesResponse, error) {
	today := now.UTC()
	first := time.Date(today.Year(), today.Month(), today.Day()-(days-1), 0, 0, 0, 0, time.UTC)
	out := minutesResponse{
		Days:     days,
		FromDay:  first.Format("2006-01-02"),
		ToDay:    today.Format("2006-01-02"),
		Unit:     "peer_minutes",
		UnitNote: minutesUnitNote,
		PerDay:   make([]minutesDay, 0, days),
	}
	rows, err := st.Minutes(out.FromDay, out.ToDay)
	if err != nil {
		return out, err
	}
	index := make(map[string]int, days)
	for i := 0; i < days; i++ {
		day := first.AddDate(0, 0, i).Format("2006-01-02")
		index[day] = i
		out.PerDay = append(out.PerDay, minutesDay{Day: day})
	}
	var users, synthetic minutesPair
	for _, row := range rows {
		i, ok := index[row.Day]
		if !ok {
			continue
		}
		if row.Synthetic {
			out.PerDay[i].Synthetic.add(row.Type, row.Minutes)
			synthetic.add(row.Type, row.Minutes)
		} else {
			out.PerDay[i].minutesPair.add(row.Type, row.Minutes)
			users.add(row.Type, row.Minutes)
		}
	}
	for i := range out.PerDay {
		out.PerDay[i].minutesPair = out.PerDay[i].minutesPair.rounded()
		out.PerDay[i].Synthetic = out.PerDay[i].Synthetic.rounded()
	}
	out.minutesPair = users.rounded()
	out.Synthetic = synthetic.rounded()
	return out, nil
}
