package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type adminRelayStats struct {
	RTPInPackets           int64 `json:"rtp_in_packets"`
	RTPFanoutAttempts      int64 `json:"rtp_fanout_attempts"`
	RTPForwardedPackets    int64 `json:"rtp_forwarded_packets"`
	RTPDroppedPackets      int64 `json:"rtp_dropped_packets"`
	RTPQueueDroppedPackets int64 `json:"rtp_queue_dropped_packets"`
	RTPDuplicatePackets    int64 `json:"rtp_duplicate_packets"`
}

func adminBaseFromWSEndpoint(wsEndpoint string) (string, error) {
	u, err := url.Parse(wsEndpoint)
	if err != nil {
		return "", err
	}
	scheme := "http"
	if u.Scheme == "wss" {
		scheme = "https"
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("missing host in endpoint")
	}
	return fmt.Sprintf("%s://%s:8080", scheme, host), nil
}

func fetchAdminRelayStats(adminBase, sessionID, password string) (adminRelayStats, error) {
	base := strings.TrimRight(adminBase, "/")
	reqURL := fmt.Sprintf("%s/admin/api/session/%s", base, url.PathEscape(sessionID))

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return adminRelayStats{}, err
	}
	if password != "" {
		req.SetBasicAuth("admin", password)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return adminRelayStats{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return adminRelayStats{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return adminRelayStats{}, fmt.Errorf("admin session detail: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var detail struct {
		StreamRelay adminRelayStats `json:"stream_relay"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		return adminRelayStats{}, err
	}
	return detail.StreamRelay, nil
}

type relayReconcileResult struct {
	hostSent        int64
	viewerCount     int
	expectedForward int64
	adminIn         int64
	adminForwarded  int64
	adminFanout     int64
	adminQueueDrops int64
	adminDuplicates int64
	inMatchesHost   bool
	forwardMatches  bool
	queueDropsOK    bool
	duplicatesOK    bool
}

func reconcileRelay(admin adminRelayStats, hostSent int64, viewerCount int, maxQueueDrops, maxDuplicates int64) relayReconcileResult {
	expectedForward := hostSent * int64(viewerCount)
	inTolerance := int64(5)
	if hostSent/100 > inTolerance {
		inTolerance = hostSent / 100
	}

	inMatches := admin.RTPInPackets >= hostSent-inTolerance && admin.RTPInPackets <= hostSent+inTolerance
	forwardTolerance := expectedForward / 50
	if forwardTolerance < 10 {
		forwardTolerance = 10
	}
	forwardMatches := admin.RTPForwardedPackets >= expectedForward-forwardTolerance

	return relayReconcileResult{
		hostSent:        hostSent,
		viewerCount:     viewerCount,
		expectedForward: expectedForward,
		adminIn:         admin.RTPInPackets,
		adminForwarded:  admin.RTPForwardedPackets,
		adminFanout:     admin.RTPFanoutAttempts,
		adminQueueDrops: admin.RTPQueueDroppedPackets,
		adminDuplicates: admin.RTPDuplicatePackets,
		inMatchesHost:   inMatches,
		forwardMatches:  forwardMatches,
		queueDropsOK:    admin.RTPQueueDroppedPackets <= maxQueueDrops,
		duplicatesOK:    admin.RTPDuplicatePackets <= maxDuplicates,
	}
}
