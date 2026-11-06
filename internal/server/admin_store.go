package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The admin API endpoints that read the disk store (01-SFU.md section 8.2).
// Without SFU_STORE_PATH they answer 503 "store disabled".

// runEventsTail is how long after a run's end its events still count:
// the drain (DrainTimeout) and a tick.
const runEventsTail = int64((DrainTimeout + mediaTickInterval) / time.Millisecond)

const storeDisabled = "store disabled (SFU_STORE_PATH is not set)"

// parseTimeMs reads a time parameter: unix milliseconds or RFC 3339.
// ok is false for a value that is neither.
func parseTimeMs(v string) (int64, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, true
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, n >= 0
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t.UnixMilli(), true
	}
	return 0, false
}

func (s *Server) storeOrError(w http.ResponseWriter) *store.Store {
	if s.media == nil {
		http.Error(w, storeDisabled, http.StatusServiceUnavailable)
		return nil
	}
	return s.media.store
}

// timeRange reads from and to; it writes the error and returns ok false
// for a bad value.
func timeRange(w http.ResponseWriter, r *http.Request) (from, to int64, ok bool) {
	q := r.URL.Query()
	from, okFrom := parseTimeMs(q.Get("from"))
	to, okTo := parseTimeMs(q.Get("to"))
	if !okFrom || !okTo {
		http.Error(w, "from and to take unix milliseconds or RFC 3339", http.StatusBadRequest)
		return 0, 0, false
	}
	return from, to, true
}

func queryLimit(r *http.Request, fallback, max int) int {
	n, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || n <= 0 {
		return fallback
	}
	if n > max {
		return max
	}
	return n
}

// handleStoreSessions answers GET /admin/api/sessions with from or to:
// the session runs from the store, live and ended, newest first.
func (s *Server) handleStoreSessions(w http.ResponseWriter, r *http.Request) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	from, to, ok := timeRange(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	rows, err := st.Sessions(store.SessionQuery{
		FromMs: from, ToMs: to,
		Type: q.Get("type"), User: q.Get("user"), Crew: q.Get("crew"), Quality: q.Get("quality"),
		Limit: queryLimit(r, 500, 5000),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"source": "store", "sessions": rows})
}

// handleStoreSessionSub answers /admin/api/session/{id}/timeline and
// /admin/api/session/{id}/events. start picks the run (default: the
// latest run of the id).
func (s *Server) handleStoreSessionSub(w http.ResponseWriter, r *http.Request, id, sub string) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	start, ok := parseTimeMs(r.URL.Query().Get("start"))
	if !ok {
		http.Error(w, "start takes unix milliseconds or RFC 3339", http.StatusBadRequest)
		return
	}
	run, found, err := st.SessionRun(id, start)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "session not in the store", http.StatusNotFound)
		return
	}
	runEnd := run.EndMs
	if runEnd == 0 {
		runEnd = time.Now().UnixMilli()
	}
	// The end of a run is the last 10 s tick with a peer; the leaves and the
	// close come after it (the drain), so the events go a minute further.
	eventsEnd := runEnd
	if run.EndMs > 0 {
		eventsEnd = run.EndMs + runEventsTail
	}
	events, err := st.Events(id, run.StartMs, eventsEnd)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if sub == "events" {
		writeJSON(w, map[string]interface{}{"session": run, "events": events})
		return
	}
	from, to, ok := timeRange(w, r)
	if !ok {
		return
	}
	if from == 0 || from < run.StartMs {
		from = run.StartMs
	}
	if to == 0 || to > runEnd {
		to = runEnd
	}
	// window (ms) without from: the last window of the range, so a page that
	// follows a live run asks with the same URL every time.
	if w, err := strconv.ParseInt(r.URL.Query().Get("window"), 10, 64); err == nil && w > 0 &&
		r.URL.Query().Get("from") == "" && to-w > from {
		from = to - w
	}
	tl, err := st.Timeline(id, from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	peers, err := st.Peers(id, run.StartMs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	minutes, err := st.PeerMinutes(id, run.StartMs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"session": run,
		"minutes": minutes,
		"peers":   peers,
		"from_ms": from,
		"to_ms":   to,
		"stats":   tl.Stats,
		"relay":   tl.Relay,
		"events":  events,
	})
}

// handleStoreLogs answers GET /admin/api/logs with from or to: the lines
// from the disk store with the ring's filters. Each entry carries "ms";
// a next page asks for to = the last entry's ms.
func (s *Server) handleStoreLogs(w http.ResponseWriter, r *http.Request) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	from, to, ok := timeRange(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit := 100
	if l, err := strconv.Atoi(q.Get("limit")); err == nil && l > 0 {
		limit = l
	}
	entries, err := st.Logs(store.LogQuery{
		Level:   int(logging.ParseLevel(q.Get("level"))),
		Event:   q.Get("event"),
		Session: q.Get("session"),
		User:    q.Get("user"),
		FromMs:  from,
		ToMs:    to,
		Limit:   limit,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// A full page can end inside one millisecond. The lines of that last
	// millisecond go to the next page (to = that ms + 1), so a page never
	// splits one: unless the whole page is one millisecond.
	var next int64
	if len(entries) == limit {
		msOf := func(e json.RawMessage) int64 {
			var v struct {
				Ms int64 `json:"ms"`
			}
			_ = json.Unmarshal(e, &v)
			return v.Ms
		}
		lastMs := msOf(entries[len(entries)-1])
		cut := len(entries)
		for cut > 0 && msOf(entries[cut-1]) == lastMs {
			cut--
		}
		if cut > 0 {
			entries = entries[:cut]
			next = lastMs + 1
		} else {
			next = lastMs
		}
	}
	writeJSON(w, map[string]interface{}{
		"source":   "store",
		"entries":  entries,
		"filtered": len(entries),
		"next_to":  next,
	})
}
