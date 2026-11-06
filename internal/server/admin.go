package server

import (
	"encoding/json"
	"math"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

type sessionTroubleMetrics struct {
	forcedDisconnects int
	rtpStalled        int
	renegFailures     int
	sessionRejects    int
	sessionFull       int
	serverAtCapacity  int
}

type incidentTimelineBucket struct {
	TimestampISO  string `json:"ts"`
	Capacity      int    `json:"capacity"`
	Renegotiation int    `json:"renegotiation"`
	Connectivity  int    `json:"connectivity"`
	Media         int    `json:"media"`
}

func (s *Server) handleAdminOverview(w http.ResponseWriter, r *http.Request) {
	// With a range the overview comes from the store (01-SFU.md section 8.7).
	if r.URL.Query().Get("range") != "" {
		s.handleStoreOverview(w, r)
		return
	}
	streamCount, voiceCount := s.sessions.Count()
	pcStates, iceStates, totalPeers := s.collectPeerStateCounts()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	bwIn, bwOut, cpuPercent := s.metrics.Rates()

	resp := map[string]interface{}{
		"server": map[string]interface{}{
			"id":      s.config.ServerID,
			"region":  s.config.Region,
			"version": Version,
			"uptime":  s.metrics.UptimeSeconds(),
			"started": s.metrics.StartTime().Format(time.RFC3339),
		},
		"now": map[string]interface{}{
			"sessions":    streamCount + voiceCount,
			"peers":       s.metrics.ActivePeers.Load(),
			"bw_in_mbps":  bwIn,
			"bw_out_mbps": bwOut,
			"cpu_percent": cpuPercent,
			"memory_mb":   float64(memStats.Alloc) / 1_000_000,
		},
		"limits": map[string]interface{}{
			"max_sessions":          s.config.MaxSessions,
			"max_peers_per_session": s.config.MaxPeersPerSession,
		},
		"peer_states": map[string]interface{}{
			"total": totalPeers,
			"pc":    pcStates,
			"ice":   iceStates,
		},
		"history": s.metrics.History(),
		"store":   s.storeInfo(),
		"totals": map[string]interface{}{
			"sessions_created":  s.metrics.SessionsCreated.Load(),
			"peers_connected":   s.metrics.PeersConnected.Load(),
			"peers_force_dc":    s.metrics.PeersForceDisconnected.Load(),
			"bytes_forwarded":   s.metrics.BytesForwarded.Load(),
			"packets_forwarded": s.metrics.PacketsForwarded.Load(),
		},
	}

	writeJSON(w, resp)
}

func (s *Server) handleAdminSessions(w http.ResponseWriter, r *http.Request) {
	// With a time range the list comes from the store: live and ended runs.
	if q := r.URL.Query(); q.Get("from") != "" || q.Get("to") != "" {
		s.handleStoreSessions(w, r)
		return
	}
	windowMinutes := adminWindowMinutes(r, 15, 120)
	warnLogs := s.queryLogs(logging.LevelWarn, windowMinutes, 5000)
	aggBySession, _ := analyzeTroubleLogs(warnLogs, windowMinutes)

	sessions := s.sessions.All()
	result := make([]map[string]interface{}, 0, len(sessions))

	for _, sess := range sessions {
		var pktsDropped, bytesSent, bytesRecv int64
		var maxBuf int32
		peersConnected := 0
		hostUserID, hostUsername := "", ""
		for _, p := range sess.AllPeers() {
			pktsDropped += p.PacketsDropped.Load()
			bytesSent += p.BytesSent.Load()
			bytesRecv += p.BytesReceived.Load()
			if buf := p.BufferUsage.Load(); buf > maxBuf {
				maxBuf = buf
			}
			if pc, _ := p.currentPeerConnection(); pc != nil &&
				pc.ConnectionState() == webrtc.PeerConnectionStateConnected {
				peersConnected++
			}
			if p.Role == "host" {
				hostUserID, hostUsername = p.UserID, p.Username
			}
		}
		agg := aggBySession[sess.ID()]
		members := sess.MemberCount()
		saturationPct := 0
		if s.config.MaxPeersPerSession > 0 {
			saturationPct = int(math.Round(float64(members) * 100.0 / float64(s.config.MaxPeersPerSession)))
		}
		forcedDC := 0
		rtpStalled := 0
		renegFails := 0
		sessionRejects := 0
		sessionFull := 0
		serverAtCapacity := 0
		if agg != nil {
			forcedDC = agg.forcedDisconnects
			rtpStalled = agg.rtpStalled
			renegFails = agg.renegFailures
			sessionRejects = agg.sessionRejects
			sessionFull = agg.sessionFull
			serverAtCapacity = agg.serverAtCapacity
		}
		troubleScore := int(pktsDropped/100) + (int(maxBuf) / 10) + forcedDC*5 + rtpStalled*6 + renegFails*4 + sessionRejects*3
		entry := map[string]interface{}{
			"id":                 sess.ID(),
			"type":               sess.Type(),
			"state":              sess.State(),
			"crew_id":            sess.CrewID(),
			"members":            members,
			"peers_connected":    peersConnected,
			"host_user_id":       hostUserID,
			"host_username":      hostUsername,
			"synthetic":          !sess.HadRealPeer(),
			"bytes_sent":         bytesSent,
			"bytes_recv":         bytesRecv,
			"created":            sess.CreatedAt().Format(time.RFC3339),
			"duration":           int(time.Since(sess.CreatedAt()).Seconds()),
			"pkts_dropped":       pktsDropped,
			"max_buf_pct":        int(maxBuf),
			"saturation_pct":     saturationPct,
			"forced_dc":          forcedDC,
			"rtp_stalled":        rtpStalled,
			"reneg_failures":     renegFails,
			"session_rejects":    sessionRejects,
			"session_full":       sessionFull,
			"server_at_capacity": serverAtCapacity,
			"trouble_score":      troubleScore,
		}
		if g, ok := s.media.liveGrade(sess); ok {
			entry["quality"], entry["graded_minutes"], entry["degraded_minutes"], entry["trouble"] =
				g.Quality, g.Graded, g.Degraded, g.Trouble
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool {
		left, lok := result[i]["trouble_score"].(int)
		right, rok := result[j]["trouble_score"].(int)
		if lok && rok && left != right {
			return left > right
		}
		return result[i]["id"].(string) < result[j]["id"].(string)
	})

	writeJSON(w, map[string]interface{}{"sessions": result})
}

func (s *Server) handleAdminSessionDetail(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 5 {
		http.Error(w, "session ID required", http.StatusBadRequest)
		return
	}
	sessionID := strings.Join(parts[4:], "/")
	for _, sub := range []string{"timeline", "events"} {
		if id, found := strings.CutSuffix(sessionID, "/"+sub); found && id != "" {
			s.handleStoreSessionSub(w, r, id, sub)
			return
		}
	}

	sess, ok := s.sessions.Get(sessionID)
	if !ok {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}

	allPeers := sess.AllPeers()
	var peerStats []PeerStats
	var totalPktsSent, totalPktsRecv, totalPktsDropped int64
	var totalBytesSent, totalBytesRecv int64
	var maxBufPct int32
	for _, p := range allPeers {
		peerStats = append(peerStats, p.Stats())
		totalPktsSent += p.PacketsSent.Load()
		totalPktsRecv += p.PacketsReceived.Load()
		totalPktsDropped += p.PacketsDropped.Load()
		totalBytesSent += p.BytesSent.Load()
		totalBytesRecv += p.BytesReceived.Load()
		if buf := p.BufferUsage.Load(); buf > maxBufPct {
			maxBufPct = buf
		}
	}
	if peerStats == nil {
		peerStats = []PeerStats{}
	}

	var streamRelay map[string]interface{}
	var snapshotCount int64
	if streamSess, ok := sess.(*StreamSession); ok {
		relay := streamSess.RelaySnapshot()
		snapshotCount = streamSess.SnapshotCount()
		streamRelay = map[string]interface{}{
			"media_in_packets":          relay.MediaInPackets,
			"media_in_bytes":            relay.MediaInBytes,
			"media_forward_attempts":    relay.MediaForwardAttempts,
			"media_forwarded_packets":   relay.MediaForwardedPackets,
			"media_forwarded_bytes":     relay.MediaForwardedBytes,
			"media_dropped_packets":     relay.MediaDroppedPackets,
			"media_dropped_bytes":       relay.MediaDroppedBytes,
			"control_in_packets":        relay.ControlInPackets,
			"control_in_bytes":          relay.ControlInBytes,
			"control_forwarded":         relay.ControlForwarded,
			"control_dropped":           relay.ControlDropped,
			"keyframe_like_messages":    relay.KeyframeLikeMessages,
			"max_media_msg_bytes":       relay.MaxMediaMessageBytes,
			"rtp_in_packets":            relay.RTPInPackets,
			"rtp_in_bytes":              relay.RTPInBytes,
			"rtp_fanout_attempts":       relay.RTPFanoutAttempts,
			"rtp_forwarded_packets":     relay.RTPForwardedPackets,
			"rtp_forwarded_bytes":       relay.RTPForwardedBytes,
			"rtp_dropped_packets":       relay.RTPDroppedPackets,
			"rtp_dropped_bytes":         relay.RTPDroppedBytes,
			"rtp_queue_dropped_packets": relay.RTPQueueDroppedPackets,
			"rtp_queue_dropped_bytes":   relay.RTPQueueDroppedBytes,
			"rtp_duplicate_packets":     relay.RTPDuplicatePackets,
			"rtp_rtx_recovered":         relay.RTPRTXRecovered,
			"rtp_rtx_duplicate":         relay.RTPRTXDuplicate,
			"rtp_ingress_expected":      relay.RTPIngressExpected,
			"rtp_ingress_lost":          relay.RTPIngressLost,
			"rtp_ingress_loss_pct":      ingressLossPct(relay),
			"rtp_viewer_nacked_packets": relay.RTPViewerNACKedPackets,
			"rtp_slow_viewer_dropped":   relay.RTPSlowViewerDroppedPackets,
			"rtp_viewer_rtcp_invalid":   relay.RTPViewerRTCPInvalid,
			"size_buckets": map[string]int64{
				"le_1200":  relay.SizeBuckets.LE1200,
				"le_8000":  relay.SizeBuckets.LE8000,
				"le_20000": relay.SizeBuckets.LE20000,
				"le_40000": relay.SizeBuckets.LE40000,
				"gt_40000": relay.SizeBuckets.GT40000,
			},
		}
	}

	resp := map[string]interface{}{
		"id":       sess.ID(),
		"type":     sess.Type(),
		"state":    sess.State(),
		"crew_id":  sess.CrewID(),
		"members":  sess.MemberCount(),
		"created":  sess.CreatedAt().Format(time.RFC3339),
		"duration": int(time.Since(sess.CreatedAt()).Seconds()),
		"peers":    peerStats,
		"totals": map[string]interface{}{
			"pkts_sent":       totalPktsSent,
			"pkts_recv":       totalPktsRecv,
			"pkts_dropped":    totalPktsDropped,
			"bytes_sent":      totalBytesSent,
			"bytes_recv":      totalBytesRecv,
			"max_buf_pct":     int(maxBufPct),
			"total_peers":     len(peerStats),
			"total_bandwidth": totalBytesSent + totalBytesRecv,
		},
	}
	if streamRelay != nil {
		resp["stream_relay"] = streamRelay
		resp["snapshot_count"] = snapshotCount
	}

	writeJSON(w, resp)
}

func (s *Server) handleAdminLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// from and to (unix ms or RFC 3339) read the disk store, past the ring.
	if q.Get("from") != "" || q.Get("to") != "" {
		s.handleStoreLogs(w, r)
		return
	}

	limit := 100
	if l := q.Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	after := q.Get("after")
	if after == "" {
		if days := q.Get("days"); days != "" {
			if d, err := strconv.Atoi(days); err == nil && d > 0 {
				after = time.Now().UTC().AddDate(0, 0, -d).Format(time.RFC3339)
			}
		}
	}

	filter := logging.LogFilter{
		Level:   logging.ParseLevel(q.Get("level")),
		Session: q.Get("session"),
		User:    q.Get("user"),
		Event:   q.Get("event"),
		Limit:   limit,
		Before:  q.Get("before"),
		After:   after,
	}

	result := s.Logger.Ring().Query(filter)
	writeJSON(w, result)
}

// storeInfo is the store's state for the overview; nil without a store.
func (s *Server) storeInfo() interface{} {
	if s.media == nil {
		return nil
	}
	return s.media.store.Info()
}

func (s *Server) handleAdminPeers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.peers.Status())
}

func (s *Server) handleAdminTroubleshoot(w http.ResponseWriter, r *http.Request) {
	windowMinutes := adminWindowMinutes(r, 15, 120)
	warnLogs := s.queryLogs(logging.LevelWarn, windowMinutes, 5000)
	aggBySession, incidents := analyzeTroubleLogs(warnLogs, windowMinutes)

	sessionRows := make([]map[string]interface{}, 0)
	maxMembers := 0
	totalRenegCount := 0
	for _, sess := range s.sessions.All() {
		row := buildSessionTroubleRow(sess, s.config.MaxPeersPerSession, aggBySession[sess.ID()])
		sessionRows = append(sessionRows, row)
		if m, ok := row["members"].(int); ok && m > maxMembers {
			maxMembers = m
		}
		if n, ok := row["reneg_count"].(int); ok {
			totalRenegCount += n
		}
	}
	sort.Slice(sessionRows, func(i, j int) bool {
		return sessionRows[i]["trouble_score"].(int) > sessionRows[j]["trouble_score"].(int)
	})

	pcStates, iceStates, totalPeers := s.collectPeerStateCounts()
	renegFailures := incidents["renegotiation"]
	renegSuccess := totalRenegCount - renegFailures
	if renegSuccess < 0 {
		renegSuccess = 0
	}
	renegCompletionRate := 1.0
	if totalRenegCount > 0 {
		renegCompletionRate = float64(renegSuccess) / float64(totalRenegCount)
	}

	recommendedPct := 0.85
	penalty := float64(incidents["capacity"])*0.01 +
		float64(incidents["renegotiation"])*0.004 +
		float64(incidents["connectivity"])*0.003 +
		float64(incidents["media"])*0.003
	if penalty > 0.30 {
		penalty = 0.30
	}
	recommendedPct -= penalty
	if recommendedPct < 0.55 {
		recommendedPct = 0.55
	}
	if recommendedPct > 0.90 {
		recommendedPct = 0.90
	}

	recommendedSafe := 0
	if s.config.MaxPeersPerSession > 0 {
		recommendedSafe = int(math.Floor(float64(s.config.MaxPeersPerSession) * recommendedPct))
		if recommendedSafe < 2 {
			recommendedSafe = 2
		}
	}

	resp := map[string]interface{}{
		"window_minutes": windowMinutes,
		"limits": map[string]interface{}{
			"max_sessions":          s.config.MaxSessions,
			"max_peers_per_session": s.config.MaxPeersPerSession,
		},
		"incidents": map[string]interface{}{
			"counts":   incidents,
			"timeline": buildIncidentTimeline(warnLogs, windowMinutes),
		},
		"states": map[string]interface{}{
			"total_peers": totalPeers,
			"pc":          pcStates,
			"ice":         iceStates,
		},
		"renegotiation_funnel": map[string]interface{}{
			"offers_sent":      totalRenegCount,
			"failures":         renegFailures,
			"successful":       renegSuccess,
			"completion_ratio": renegCompletionRate,
		},
		"capacity_envelope": map[string]interface{}{
			"max_members_observed": maxMembers,
			"recommended_safe":     recommendedSafe,
			"recommended_pct":      recommendedPct,
		},
		"sessions": sessionRows,
	}

	writeJSON(w, resp)
}

func buildSessionTroubleRow(sess Session, maxPeersPerSession int, agg *sessionTroubleMetrics) map[string]interface{} {
	var pktsDropped int64
	var maxBuf int32
	var renegCount int
	var rtpWriteErrors int64
	rtpErrStreakMax := 0
	for _, p := range sess.AllPeers() {
		pktsDropped += p.PacketsDropped.Load()
		if buf := p.BufferUsage.Load(); buf > maxBuf {
			maxBuf = buf
		}
		renegCount += int(p.RenegotiationCount.Load())
		rtpWriteErrors += p.RTPWriteErrors.Load()
		if streak := int(p.rtpErrStreak.Load()); streak > rtpErrStreakMax {
			rtpErrStreakMax = streak
		}
	}

	members := sess.MemberCount()
	saturationPct := 0
	if maxPeersPerSession > 0 {
		saturationPct = int(math.Round(float64(members) * 100.0 / float64(maxPeersPerSession)))
	}

	forcedDC := 0
	rtpStalled := 0
	renegFails := 0
	sessionRejects := 0
	sessionFull := 0
	serverAtCapacity := 0
	if agg != nil {
		forcedDC = agg.forcedDisconnects
		rtpStalled = agg.rtpStalled
		renegFails = agg.renegFailures
		sessionRejects = agg.sessionRejects
		sessionFull = agg.sessionFull
		serverAtCapacity = agg.serverAtCapacity
	}

	troubleScore := int(pktsDropped/100) +
		(int(maxBuf) / 10) +
		(forcedDC * 5) +
		(rtpStalled * 6) +
		(renegFails * 4) +
		(sessionRejects * 3) +
		(int(rtpWriteErrors) / 50)

	renegFailRate := 0.0
	if renegCount > 0 {
		renegFailRate = float64(renegFails) / float64(renegCount)
	}

	return map[string]interface{}{
		"id":                 sess.ID(),
		"type":               sess.Type(),
		"state":              sess.State(),
		"crew_id":            sess.CrewID(),
		"members":            members,
		"duration":           int(time.Since(sess.CreatedAt()).Seconds()),
		"saturation_pct":     saturationPct,
		"pkts_dropped":       pktsDropped,
		"max_buf_pct":        int(maxBuf),
		"reneg_count":        renegCount,
		"reneg_failures":     renegFails,
		"reneg_fail_rate":    renegFailRate,
		"forced_dc":          forcedDC,
		"rtp_stalled":        rtpStalled,
		"rtp_write_errors":   rtpWriteErrors,
		"rtp_err_streak_max": rtpErrStreakMax,
		"session_rejects":    sessionRejects,
		"session_full":       sessionFull,
		"server_at_capacity": serverAtCapacity,
		"trouble_score":      troubleScore,
	}
}

func analyzeTroubleLogs(entries []logging.Entry, windowMinutes int) (map[string]*sessionTroubleMetrics, map[string]int) {
	aggBySession := make(map[string]*sessionTroubleMetrics)
	incidents := map[string]int{
		"capacity":      0,
		"renegotiation": 0,
		"connectivity":  0,
		"media":         0,
		"total":         0,
	}

	for _, e := range entries {
		category := categorizeIncident(e)
		if category == "" {
			continue
		}
		incidents[category]++
		incidents["total"]++

		if e.SessionID == "" {
			continue
		}
		if _, ok := aggBySession[e.SessionID]; !ok {
			aggBySession[e.SessionID] = &sessionTroubleMetrics{}
		}
		agg := aggBySession[e.SessionID]
		switch e.Event {
		case "peer_force_disconnect":
			agg.forcedDisconnects++
			if reason, _ := e.Data["reason"].(string); reason == "PEER_STALLED_RTP" {
				agg.rtpStalled++
			}
		case "session_reject":
			agg.sessionRejects++
			if code, _ := e.Data["code"].(string); code == "SESSION_FULL" {
				agg.sessionFull++
			}
			if code, _ := e.Data["code"].(string); code == "SERVER_AT_CAPACITY" {
				agg.serverAtCapacity++
			}
		default:
			if category == "renegotiation" {
				agg.renegFailures++
			}
		}
	}
	return aggBySession, incidents
}

func buildIncidentTimeline(entries []logging.Entry, windowMinutes int) []incidentTimelineBucket {
	if windowMinutes <= 0 {
		windowMinutes = 15
	}
	now := time.Now().UTC()
	start := now.Add(-time.Duration(windowMinutes-1) * time.Minute).Truncate(time.Minute)
	timeline := make([]incidentTimelineBucket, windowMinutes)
	for i := 0; i < windowMinutes; i++ {
		ts := start.Add(time.Duration(i) * time.Minute)
		timeline[i] = incidentTimelineBucket{TimestampISO: ts.Format(time.RFC3339)}
	}

	for _, e := range entries {
		ts, err := time.Parse(time.RFC3339, e.Timestamp)
		if err != nil {
			continue
		}
		idx := int(ts.UTC().Truncate(time.Minute).Sub(start) / time.Minute)
		if idx < 0 || idx >= len(timeline) {
			continue
		}
		switch categorizeIncident(e) {
		case "capacity":
			timeline[idx].Capacity++
		case "renegotiation":
			timeline[idx].Renegotiation++
		case "connectivity":
			timeline[idx].Connectivity++
		case "media":
			timeline[idx].Media++
		}
	}
	return timeline
}

func (s *Server) collectPeerStateCounts() (map[string]int, map[string]int, int) {
	pcStates := map[string]int{}
	iceStates := map[string]int{}
	total := 0
	for _, sess := range s.sessions.All() {
		for _, p := range sess.AllPeers() {
			total++
			pcState := "unknown"
			iceState := "unknown"
			if pc, _ := p.currentPeerConnection(); pc != nil {
				pcState = strings.ToLower(pc.ConnectionState().String())
				iceState = strings.ToLower(pc.ICEConnectionState().String())
			}
			pcStates[pcState]++
			iceStates[iceState]++
		}
	}
	return pcStates, iceStates, total
}

func (s *Server) queryLogs(level logging.Level, windowMinutes int, limit int) []logging.Entry {
	after := time.Now().UTC().Add(-time.Duration(windowMinutes) * time.Minute).Format(time.RFC3339)
	result := s.Logger.Ring().Query(logging.LogFilter{
		Level: level,
		After: after,
		Limit: limit,
	})
	return result.Entries
}

func categorizeIncident(e logging.Entry) string {
	switch e.Event {
	case "session_reject":
		return "capacity"
	case "peer_force_disconnect", "audio_track_read_error":
		return "media"
	case "renegotiate_failed", "renegotiate_on_leave_failed", "renegotiate_new_peer_failed", "sdp_remote_offer_failed", "sdp_local_failed":
		return "renegotiation"
	case "webrtc_failed", "webrtc_disconnected", "peer_timeout_disconnected", "peer_timeout_ice_restart", "ice_restart_failed", "sdp_answer_apply_failed":
		return "connectivity"
	default:
		return ""
	}
}

func adminWindowMinutes(r *http.Request, fallback, max int) int {
	minutes := fallback
	if raw := r.URL.Query().Get("window_minutes"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			minutes = parsed
		}
	}
	if minutes < 1 {
		minutes = fallback
	}
	if max > 0 && minutes > max {
		minutes = max
	}
	return minutes
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
