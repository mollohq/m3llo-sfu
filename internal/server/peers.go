package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type LocalStatusFunc func() *PeerInstanceStatus

type PeerPoller struct {
	peers      []string // from SFU_PEERS env
	selfID     string
	selfRegion string

	mu          sync.RWMutex
	statuses    map[string]*PeerInstanceStatus
	client      *http.Client
	localStatus LocalStatusFunc
}

type PeerInstanceStatus struct {
	ServerID     string    `json:"server_id"`
	Region       string    `json:"region"`
	Status       string    `json:"status"` // "ok" or "unreachable"
	URL          string    `json:"url"`
	Sessions     int       `json:"sessions"`
	Peers        int       `json:"peers"`
	BandwidthOut float64   `json:"bandwidth_out_mbps"`
	Uptime       int64     `json:"uptime_seconds"`
	Latency      int64     `json:"latency_ms"`
	Self         bool      `json:"self"`
	LastSeen     time.Time `json:"last_seen"`
	missCount    int
}

type AggregateStatus struct {
	Instances []*PeerInstanceStatus `json:"instances"`
	Totals    AggregateTotals       `json:"totals"`
}

type AggregateTotals struct {
	InstanceCount int     `json:"instances"`
	Sessions      int     `json:"sessions"`
	Peers         int     `json:"peers"`
	BandwidthOut  float64 `json:"bandwidth_out_mbps"`
}

func NewPeerPoller(peers []string, selfID, selfRegion string) *PeerPoller {
	return &PeerPoller{
		peers:      peers,
		selfID:     selfID,
		selfRegion: selfRegion,
		statuses:   make(map[string]*PeerInstanceStatus),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (pp *PeerPoller) SetLocalStatus(fn LocalStatusFunc) {
	pp.localStatus = fn
}

func (pp *PeerPoller) Run(stop <-chan struct{}) {
	// Initial poll
	pp.pollAll()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pp.pollAll()
		case <-stop:
			return
		}
	}
}

func (pp *PeerPoller) pollAll() {
	for _, peerAddr := range pp.peers {
		go pp.pollOne(peerAddr)
	}
}

func (pp *PeerPoller) pollOne(addr string) {
	url := fmt.Sprintf("http://%s/health", addr)
	start := time.Now()

	resp, err := pp.client.Get(url)
	latency := time.Since(start).Milliseconds()

	pp.mu.Lock()
	defer pp.mu.Unlock()

	existing, ok := pp.statuses[addr]

	if err != nil || resp.StatusCode != 200 {
		if ok {
			existing.missCount++
			if existing.missCount >= 3 {
				existing.Status = "unreachable"
			}
		} else {
			pp.statuses[addr] = &PeerInstanceStatus{
				URL:       addr,
				Status:    "unreachable",
				missCount: 1,
			}
		}
		if resp != nil {
			resp.Body.Close()
		}
		return
	}
	defer resp.Body.Close()

	var health HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return
	}

	pp.statuses[addr] = &PeerInstanceStatus{
		ServerID:     health.ServerID,
		Region:       health.Region,
		Status:       "ok",
		URL:          addr,
		Sessions:     health.Sessions.StreamActive + health.Sessions.VoiceActive,
		Peers:        health.Peers.Total,
		BandwidthOut: health.BW.Outbound,
		Uptime:       health.Uptime,
		Latency:      latency,
		Self:         health.ServerID == pp.selfID,
		LastSeen:     time.Now(),
	}
}

func (pp *PeerPoller) Status() AggregateStatus {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	var instances []*PeerInstanceStatus
	var totals AggregateTotals

	if pp.localStatus != nil {
		if local := pp.localStatus(); local != nil {
			instances = append(instances, local)
			totals.Sessions += local.Sessions
			totals.Peers += local.Peers
			totals.BandwidthOut += local.BandwidthOut
		}
	}

	for _, s := range pp.statuses {
		instances = append(instances, s)
		if s.Status == "ok" {
			totals.Sessions += s.Sessions
			totals.Peers += s.Peers
			totals.BandwidthOut += s.BandwidthOut
		}
	}

	totals.InstanceCount = len(instances)

	return AggregateStatus{
		Instances: instances,
		Totals:    totals,
	}
}
