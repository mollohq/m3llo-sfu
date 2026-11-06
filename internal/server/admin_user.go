package server

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/store"
)

// The person endpoints of the admin API (01-SFU.md section 8.8):
//
//	GET  /admin/api/user/{id}/sessions?from&to   runs with the user's own grade
//	GET  /admin/api/user/{id}/profile            machine, link, last session
//	POST /admin/api/user/{id}/request-logs       ask the client for its log
//	GET  /admin/api/user/{id}/bundles            the stored bundles, requests
//	GET  /admin/api/user/{id}/bundles/{name}?q&limit   lines of a bundle
//	GET  /admin/api/user/{id}/bundles/{name}/download  the gzip file
func (s *Server) handleAdminUser(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/admin/api/user/")
	id, sub, found := strings.Cut(rest, "/")
	if !found || id == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(sub, "/")
	switch {
	case sub == "sessions" && r.Method == http.MethodGet:
		s.handleUserSessions(w, r, id)
	case sub == "profile" && r.Method == http.MethodGet:
		s.handleUserProfile(w, r, id)
	case sub == "request-logs" && r.Method == http.MethodPost:
		s.handleUserRequestLogs(w, r, id)
	case sub == "bundles" && r.Method == http.MethodGet:
		s.handleUserBundles(w, r, id)
	case len(parts) == 2 && parts[0] == "bundles" && r.Method == http.MethodGet:
		s.handleUserBundleView(w, r, id, parts[1])
	case len(parts) == 3 && parts[0] == "bundles" && parts[2] == "download" && r.Method == http.MethodGet:
		s.handleUserBundleDownload(w, r, id, parts[1])
	default:
		http.NotFound(w, r)
	}
}

// userSessionRow is a run with the user's own peer minutes.
type userSessionRow struct {
	store.SessionRow
	User *store.UserRunGrade `json:"user_grade,omitempty"`
}

func (s *Server) handleUserSessions(w http.ResponseWriter, r *http.Request, id string) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	from, to, ok := timeRange(w, r)
	if !ok {
		return
	}
	rows, err := s.userSessions(st, id, from, to, queryLimit(r, 500, 5000))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"user": id, "sessions": rows})
}

func (s *Server) userSessions(st *store.Store, id string, from, to int64, limit int) ([]userSessionRow, error) {
	runs, err := st.Sessions(store.SessionQuery{FromMs: from, ToMs: to, User: id, Limit: limit})
	if err != nil {
		return nil, err
	}
	gradeFrom := from
	if len(runs) > 0 {
		gradeFrom = runs[len(runs)-1].StartMs
	}
	grades, err := st.UserRunGrades(id, gradeFrom, 0)
	if err != nil {
		return nil, err
	}
	out := make([]userSessionRow, 0, len(runs))
	for _, run := range runs {
		row := userSessionRow{SessionRow: run}
		if g, ok := grades[run.ID+"\x00"+itoa64(run.StartMs)]; ok {
			row.User = g
		}
		out = append(out, row)
	}
	return out, nil
}

func itoa64(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}

// linkPoint is one point of the user's link in a session.
type linkPoint struct {
	Ms       int64    `json:"ms"`
	LossPct  *float64 `json:"loss_pct,omitempty"`
	JitterMs *float64 `json:"jitter_ms,omitempty"`
	RTTMs    *float64 `json:"rtt_ms,omitempty"`
}

func f64(v float64) *float64 { return &v }

func (s *Server) handleUserProfile(w http.ResponseWriter, r *http.Request, id string) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	now := time.Now()
	stats, err := st.UserStats(id, now.Add(-30*24*time.Hour).UnixMilli(), 0, 400)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The latest report of each kind and role.
	latest := map[string]map[string]interface{}{}
	latestMs := map[string]int64{}
	for _, row := range stats {
		key := row.Kind + ":" + row.Role
		if _, ok := latest[key]; ok {
			continue
		}
		var b map[string]interface{}
		if json.Unmarshal(row.Body, &b) == nil {
			latest[key], latestMs[key] = b, row.Ms
		}
	}
	machine := map[string]interface{}{}
	pick := func(key, field, as string) {
		if b := latest[key]; b != nil {
			if v, ok := b[field]; ok && v != "" && v != nil {
				machine[as] = v
				machine[as+"_ms"] = latestMs[key]
			}
		}
	}
	pick("stream:host", "gpu", "gpu")
	pick("stream:host", "enc", "encoder")
	pick("stream:host", "cap_backend", "capture_backend")
	pick("stream:host", "enc_tier", "encoder_cost_tier")
	pick("stream:viewer", "dec", "decoder")
	pick("voice:member", "rtt_ms", "voice_rtt_ms")
	pick("voice:member", "pipeline_delay_ms", "voice_pipeline_delay_ms")
	pick("stream:viewer", "rtt_ms", "stream_rtt_ms")

	runs, err := st.Sessions(store.SessionQuery{User: id, Limit: 1})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := map[string]interface{}{
		"user":    id,
		"machine": machine,
		// Not in the client's reports today (a request to mello-core).
		"missing":   []string{"client version", "platform", "audio device"},
		"region":    s.config.Region,
		"server_id": s.config.ServerID,
		"connected": len(s.userPeers(id)) > 0,
	}
	if len(runs) == 1 {
		run := runs[0]
		end := run.EndMs
		if end == 0 {
			end = now.UnixMilli()
		}
		resp["last_session"] = run
		resp["last_seen_ms"] = run.LastSeenMs
		resp["link"] = s.userLink(st, id, run, end)
	}
	writeJSON(w, resp)
}

// userLink is the user's loss, jitter and RTT over a run: the loss from
// the SFU's rows (the host leg for a host, the viewer leg's NACK share for
// a viewer), jitter and RTT from the user's own reports.
func (s *Server) userLink(st *store.Store, id string, run store.SessionRow, end int64) []linkPoint {
	points := []linkPoint{}
	tl, err := st.Timeline(run.ID, run.StartMs, end)
	if err != nil {
		return points
	}
	host := run.HostUserID == id
	for _, rr := range tl.Relay {
		var b map[string]interface{}
		if json.Unmarshal(rr.Body, &b) != nil {
			continue
		}
		p := linkPoint{Ms: rr.Ms}
		if host {
			if num(b["ing_expected"]) >= gradeMinPackets {
				p.LossPct = f64(round2(num(b["ing_loss_pct"])))
			}
		} else if sent := num(mapOf(b["viewer_sent"])[id]); sent >= gradeMinPackets {
			p.LossPct = f64(round2(num(mapOf(b["viewer_nacked"])[id]) * 100 / sent))
		}
		if p.LossPct != nil {
			points = append(points, p)
		}
	}
	for _, sr := range tl.Stats {
		if sr.User != id {
			continue
		}
		var b map[string]interface{}
		if json.Unmarshal(sr.Body, &b) != nil {
			continue
		}
		p := linkPoint{Ms: sr.Ms}
		if v, ok := b["jit_ms"]; ok {
			p.JitterMs = f64(num(v))
		} else if v, ok := b["rx_jitter"]; ok {
			p.JitterMs = f64(num(v))
		}
		if v, ok := b["rtt_ms"]; ok {
			p.RTTMs = f64(num(v))
		}
		if p.JitterMs != nil || p.RTTMs != nil {
			points = append(points, p)
		}
	}
	sort.Slice(points, func(i, j int) bool { return points[i].Ms < points[j].Ms })
	return points
}

// userPeers are the user's peers in the sessions of this instance.
func (s *Server) userPeers(id string) []*Peer {
	var out []*Peer
	seen := map[*Peer]bool{}
	for _, sess := range s.sessions.All() {
		for _, p := range sess.AllPeers() {
			if p.UserID == id && !seen[p] {
				seen[p] = true
				out = append(out, p)
			}
		}
	}
	return out
}

func (s *Server) bundlesOrError(w http.ResponseWriter) *bundleManager {
	if s.bundles == nil {
		http.Error(w, storeDisabled, http.StatusServiceUnavailable)
		return nil
	}
	return s.bundles
}

// requestLogsData is the `request_logs` message to a client.
type requestLogsData struct {
	RequestID  string `json:"request_id"`
	SinceHours int    `json:"since_hours"`
	MaxBytes   int    `json:"max_bytes"`
	ChunkBytes int    `json:"chunk_bytes"`
}

// handleUserRequestLogs sends `request_logs` to every socket of the user on
// this instance. A user with no socket here is "not_connected" (200: the
// admin asks every instance, and only one has the user).
func (s *Server) handleUserRequestLogs(w http.ResponseWriter, r *http.Request, id string) {
	b := s.bundlesOrError(w)
	if b == nil {
		return
	}
	peers := s.userPeers(id)
	if len(peers) == 0 {
		writeJSON(w, map[string]interface{}{"status": "not_connected", "user": id})
		return
	}
	req := b.start(id, len(peers), time.Now())
	for _, p := range peers {
		p.SendSignaling("request_logs", requestLogsData{
			RequestID: req.ID, SinceHours: bundleSinceHours, MaxBytes: bundleMaxBytes, ChunkBytes: bundleChunkBytes,
		})
	}
	s.Logger.Info("logs_requested", logging.D{"user": id, "request_id": req.ID, "peers": len(peers)})
	writeJSON(w, map[string]interface{}{"status": "sent", "user": id, "request_id": req.ID, "peers": len(peers)})
}

func (s *Server) handleUserBundles(w http.ResponseWriter, r *http.Request, id string) {
	b := s.bundlesOrError(w)
	if b == nil {
		return
	}
	files, reqs := b.list(id, time.Now())
	writeJSON(w, map[string]interface{}{
		"user": id, "bundles": files, "requests": reqs, "connected": len(s.userPeers(id)) > 0,
	})
}

func (s *Server) handleUserBundleView(w http.ResponseWriter, r *http.Request, id, name string) {
	b := s.bundlesOrError(w)
	if b == nil {
		return
	}
	path, ok := b.path(id, name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query().Get("q")
	lines, matched, total, err := b.view(path, q, queryLimit(r, 500, bundleViewMaxLines))
	if err != nil {
		http.Error(w, "cannot read the bundle: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{
		"name": name, "q": q, "lines": lines, "matched": matched, "total_lines": total,
		"shown": len(lines),
	})
}

func (s *Server) handleUserBundleDownload(w http.ResponseWriter, r *http.Request, id, name string) {
	b := s.bundlesOrError(w)
	if b == nil {
		return
	}
	path, ok := b.path(id, name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+name+`"`)
	http.ServeFile(w, r, path)
}

// handleLogsBundle takes one `logs_bundle` chunk from a client's socket.
func (s *Server) handleLogsBundle(peer *Peer, userID string, data json.RawMessage) {
	if s.bundles == nil {
		return
	}
	var head struct {
		RequestID string `json:"request_id"`
		Seq       int    `json:"seq"`
		Last      bool   `json:"last"`
	}
	_ = json.Unmarshal(data, &head)
	if err := s.bundles.chunk(userID, data, time.Now()); err != nil {
		s.Logger.Warn("logs_bundle_refused", logging.D{
			"user": userID, "request_id": truncate(head.RequestID, maxLoggedClientString), "seq": head.Seq, "error": err.Error(),
		})
		return
	}
	if head.Last {
		s.Logger.Info("logs_bundle_stored", logging.D{"user": userID, "request_id": head.RequestID})
	}
}
