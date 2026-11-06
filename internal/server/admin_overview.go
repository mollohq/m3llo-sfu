package server

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/store"
)

// overviewRanges are the ranges of GET /admin/api/overview?range= and the
// width of one point of their series.
var overviewRanges = map[string]struct{ span, bucket time.Duration }{
	"1h":  {time.Hour, time.Minute},
	"24h": {24 * time.Hour, 15 * time.Minute},
	"7d":  {7 * 24 * time.Hour, 2 * time.Hour},
}

type seriesPoint map[string]interface{}

func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Ceil(p*float64(len(s)))) - 1
	if i < 0 {
		i = 0
	}
	return s[i]
}

type overviewAcc struct {
	n                                  int
	cpu, mem, bwIn, bwOut              float64
	peers, streams, voice, peerSeconds float64
}

// handleStoreOverview answers GET /admin/api/overview?range=1h|24h|7d: the
// tiles and series of the Overview page from the store (01-SFU.md
// section 8.7).
func (s *Server) handleStoreOverview(w http.ResponseWriter, r *http.Request) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	name := r.URL.Query().Get("range")
	rg, ok := overviewRanges[name]
	if !ok {
		http.Error(w, "range is 1h, 24h or 7d", http.StatusBadRequest)
		return
	}
	now := time.Now()
	to := now.UnixMilli()
	from := now.Add(-rg.span).UnixMilli()
	bucket := rg.bucket.Milliseconds()
	bucketOf := func(ms int64) int64 { return (ms / bucket) * bucket }
	fail := func(err error) { http.Error(w, err.Error(), http.StatusInternalServerError) }

	// The instance: load and resources per bucket.
	samples, err := st.InstanceSamples(from, to)
	if err != nil {
		fail(err)
		return
	}
	byBucket := map[int64]*overviewAcc{}
	for _, sm := range samples {
		var b map[string]interface{}
		if json.Unmarshal([]byte(sm.Body), &b) != nil {
			continue
		}
		k := bucketOf(sm.Ms)
		a := byBucket[k]
		if a == nil {
			a = &overviewAcc{}
			byBucket[k] = a
		}
		a.n++
		a.cpu += num(b["cpu"])
		a.mem += num(b["mem_mb"])
		a.bwIn += num(b["bw_in"])
		a.bwOut += num(b["bw_out"])
		a.peers = math.Max(a.peers, num(b["peers"]))
		a.streams = math.Max(a.streams, num(b["streams"]))
		a.voice = math.Max(a.voice, num(b["voice"]))
		a.peerSeconds += num(b["peers"]) * mediaTickInterval.Seconds()
	}
	minutes, err := st.MinuteBuckets(from, to, bucket)
	if err != nil {
		fail(err)
		return
	}
	minByBucket := map[int64]store.MinuteBucket{}
	totGraded, totDegraded, totBad := 0, 0, 0
	for _, mb := range minutes {
		minByBucket[mb.Ms] = mb
		totGraded += mb.Graded
		totDegraded += mb.Degraded
		totBad += mb.InBad
	}

	// The time from a viewer's join to the first keyframe the SFU sends it
	// (the SFU's share of the time to first frame), and the client's first
	// frame report (an upper bound: the reports come every 10 s).
	evs, err := st.EventsOfKinds([]string{"join", "keyframe_to_viewer", "first_frame"}, from, to)
	if err != nil {
		fail(err)
		return
	}
	joined := map[string]int64{}
	var keyframeMs, firstFrameMs []float64
	kfByBucket := map[int64][]float64{}
	for _, e := range evs {
		key := e.Session + "\x00" + e.User
		var d map[string]interface{}
		_ = json.Unmarshal(e.Detail, &d)
		switch e.Kind {
		case "join":
			if d["role"] == "viewer" {
				joined[key] = e.Ms
			}
		case "keyframe_to_viewer":
			if j, ok := joined[key]; ok {
				v := float64(e.Ms - j)
				keyframeMs = append(keyframeMs, v)
				kfByBucket[bucketOf(e.Ms)] = append(kfByBucket[bucketOf(e.Ms)], v)
				delete(joined, key)
			}
		case "first_frame":
			if v := num(d["within_ms"]); v > 0 {
				firstFrameMs = append(firstFrameMs, v)
			}
		}
	}

	var load, instance []seriesPoint
	peersSeries, minutesSeries, qualitySeries, kfSeries, cpuSeries :=
		[]seriesPoint{}, []seriesPoint{}, []seriesPoint{}, []seriesPoint{}, []seriesPoint{}
	for t := bucketOf(from); t <= to; t += bucket {
		a := byBucket[t]
		mb := minByBucket[t]
		p := seriesPoint{"t": t}
		inst := seriesPoint{"t": t}
		if a != nil && a.n > 0 {
			p["peers"], p["streams"], p["voice"] = a.peers, a.streams, a.voice
			inst["cpu"] = round1(a.cpu / float64(a.n))
			inst["mem_mb"] = round1(a.mem / float64(a.n))
			inst["bw_in"] = round1(a.bwIn / float64(a.n))
			inst["bw_out"] = round1(a.bwOut / float64(a.n))
			inst["peers"] = a.peers
			peersSeries = append(peersSeries, seriesPoint{"t": t, "v": a.peers})
			minutesSeries = append(minutesSeries, seriesPoint{"t": t, "v": round1(a.peerSeconds / 60)})
			cpuSeries = append(cpuSeries, seriesPoint{"t": t, "v": round1(a.cpu / float64(a.n))})
		}
		if mb.Graded > 0 {
			deg := float64(mb.Degraded) * 100 / float64(mb.Graded)
			p["degraded_pct"] = round1(deg)
			p["bad_pct"] = round1(float64(mb.InBad) * 100 / float64(mb.Graded))
			qualitySeries = append(qualitySeries, seriesPoint{"t": t, "v": round1(100 - deg)})
		}
		if ks := kfByBucket[t]; len(ks) > 0 {
			kfSeries = append(kfSeries, seriesPoint{"t": t, "v": percentile(ks, 0.5), "p95": percentile(ks, 0.95)})
		}
		load = append(load, p)
		instance = append(instance, inst)
	}

	// Peer minutes per day; real users and synthetic peers apart.
	today := now.UTC().Format("2006-01-02")
	days, err := st.Minutes(now.Add(-7*24*time.Hour).UTC().Format("2006-01-02"), today)
	if err != nil {
		fail(err)
		return
	}
	minutesToday := map[string]float64{}
	perDay := map[string]float64{}
	for _, d := range days {
		key := d.Type
		if d.Synthetic {
			key += "_synthetic"
		}
		if d.Day == today {
			minutesToday[key] = round1(minutesToday[key] + d.Minutes)
		}
		if !d.Synthetic && d.Day != today {
			perDay[d.Day] += d.Minutes
		}
	}
	avg7 := 0.0
	for _, v := range perDay {
		avg7 += v / 7
	}

	legs, err := st.LegCounts(from, to)
	if err != nil {
		fail(err)
		return
	}
	fleet := map[string]map[string]int{}
	for key, q := range map[string][2]string{
		"encoders": {"host", "enc"}, "capture": {"host", "cap_backend"}, "decoders": {"viewer", "dec"},
	} {
		m, err := st.StatsField(q[0], q[1], from, to)
		if err != nil {
			fail(err)
			return
		}
		fleet[key] = m
	}

	streamCount, voiceCount := s.sessions.Count()
	bwIn, bwOut, cpu := s.metrics.Rates()
	quality := map[string]interface{}{
		"graded_minutes": totGraded, "degraded_minutes": totDegraded, "in_bad_minutes": totBad,
	}
	if totGraded > 0 {
		quality["good_pct"] = round1(float64(totGraded-totDegraded) * 100 / float64(totGraded))
		quality["degraded_pct"] = round1(float64(totDegraded) * 100 / float64(totGraded))
		quality["bad_pct"] = round1(float64(totBad) * 100 / float64(totGraded))
	}
	writeJSON(w, map[string]interface{}{
		"range": name, "from_ms": from, "to_ms": to, "bucket_ms": bucket,
		"tiles": map[string]interface{}{
			"peers": map[string]interface{}{
				"now": s.metrics.ActivePeers.Load(), "streams": streamCount, "voice": voiceCount, "series": peersSeries,
			},
			"minutes": map[string]interface{}{"today": minutesToday, "avg_7d": round1(avg7), "series": minutesSeries},
			"quality": map[string]interface{}{"summary": quality, "series": qualitySeries},
			"first_keyframe": map[string]interface{}{
				"median_ms": percentile(keyframeMs, 0.5), "p95_ms": percentile(keyframeMs, 0.95),
				"count": len(keyframeMs), "first_frame_report_median_ms": percentile(firstFrameMs, 0.5),
				"first_frame_reports": len(firstFrameMs), "series": kfSeries,
			},
			"cpu": map[string]interface{}{
				"now": round1(cpu), "bw_in_mbps": round1(bwIn), "bw_out_mbps": round1(bwOut),
				"uptime": s.metrics.UptimeSeconds(), "series": cpuSeries,
			},
		},
		"load":     load,
		"instance": instance,
		"legs":     legs,
		"fleet":    fleet,
		"now":      s.liveGrades(),
	})
}

// liveGrades are the sessions that run now with their grade so far.
func (s *Server) liveGrades() []map[string]interface{} {
	out := []map[string]interface{}{}
	for _, sess := range s.sessions.All() {
		row := map[string]interface{}{
			"id": sess.ID(), "type": sess.Type(), "crew_id": sess.CrewID(), "members": sess.MemberCount(),
			"synthetic": !sess.HadRealPeer(), "created": sess.CreatedAt().UnixMilli(),
		}
		for _, p := range sess.AllPeers() {
			if p.Role == "host" {
				row["host_user_id"], row["host_username"] = p.UserID, p.Username
			}
		}
		if g, ok := s.media.liveGrade(sess); ok {
			row["quality"], row["graded_minutes"], row["degraded_minutes"], row["trouble"] =
				g.Quality, g.Graded, g.Degraded, g.Trouble
		}
		out = append(out, row)
	}
	return out
}

// handleAdminAlerts answers GET /admin/api/alerts?from&to&limit: the
// stored alerts, newest first, the last 24 h by default.
func (s *Server) handleAdminAlerts(w http.ResponseWriter, r *http.Request) {
	st := s.storeOrError(w)
	if st == nil {
		return
	}
	from, to, ok := timeRange(w, r)
	if !ok {
		return
	}
	if from == 0 {
		from = time.Now().Add(-24 * time.Hour).UnixMilli()
	}
	alerts, err := st.Alerts(from, to, queryLimit(r, 200, 2000))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]interface{}{"alerts": alerts})
}
