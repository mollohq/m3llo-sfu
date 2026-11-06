package main

import (
	"strings"
	"testing"
)

func TestReconcileRelayHealthyMultiViewer(t *testing.T) {
	admin := adminRelayStats{
		RTPInPackets:        1000,
		RTPFanoutAttempts:   5000,
		RTPForwardedPackets: 5000,
	}
	result := reconcileRelay(admin, 1000, 5, 0, 0)
	if !result.inMatchesHost {
		t.Fatalf("ingress mismatch: admin=%d host=%d", result.adminIn, result.hostSent)
	}
	if !result.forwardMatches {
		t.Fatalf("forward mismatch: admin=%d expected~%d", result.adminForwarded, result.expectedForward)
	}
}

func TestReconcileRelayDetectsQueueDrops(t *testing.T) {
	admin := adminRelayStats{RTPQueueDroppedPackets: 3}
	result := reconcileRelay(admin, 100, 2, 0, 0)
	if result.queueDropsOK {
		t.Fatal("expected queue drop failure")
	}
}

func TestReconcileRelayAllowsConfiguredDuplicates(t *testing.T) {
	admin := adminRelayStats{RTPDuplicatePackets: 2}
	result := reconcileRelay(admin, 100, 1, 0, 5)
	if !result.duplicatesOK {
		t.Fatal("expected duplicates within gate")
	}
}

func TestEvaluateGatesFailsWithoutStreamAudio(t *testing.T) {
	r := soakResult{
		rxRatio:            1,
		audioRxRatio:       0,
		minRxRatio:         0.99,
		maxP95Ms:           90,
		skipAdminReconcile: true,
	}
	reasons := evaluateGates(r)
	if len(reasons) != 1 || !strings.HasPrefix(reasons[0], "audio_rx_ratio") {
		t.Fatalf("want one audio_rx_ratio failure, got %v", reasons)
	}
	r.audioRxRatio = 1
	if reasons := evaluateGates(r); len(reasons) != 0 {
		t.Fatalf("want no failures, got %v", reasons)
	}
}

// The viewer-leg drop keeps a share of the video packets from the NACK
// generator once each, and counts a dropped sequence number that comes
// again (the SFU's repair) as repaired.
func TestViewerDropCountsRepairs(t *testing.T) {
	drop := newViewerDrop(2, 1)
	var droppedSeqs []uint16
	for seq := uint16(0); seq < 5000; seq++ {
		if !drop.keep(seq) {
			droppedSeqs = append(droppedSeqs, seq)
		}
	}
	dropped, repaired := drop.counts()
	if dropped == 0 || dropped != int64(len(droppedSeqs)) || repaired != 0 {
		t.Fatalf("dropped %d (%d seen), repaired %d", dropped, len(droppedSeqs), repaired)
	}
	if dropped < 50 || dropped > 150 {
		t.Fatalf("dropped %d of 5000 at 2 %%", dropped)
	}
	// The repairs arrive: none is dropped again.
	for _, seq := range droppedSeqs {
		if !drop.keep(seq) {
			t.Fatalf("the repair of seq %d was dropped", seq)
		}
	}
	if _, repaired := drop.counts(); repaired != dropped {
		t.Fatalf("repaired %d, want %d", repaired, dropped)
	}
}

// The repair gate fails when the SFU repairs less than 99 % of the
// dropped packets, and when nothing was dropped.
func TestEvaluateGatesChecksViewerLegRepair(t *testing.T) {
	r := soakResult{
		rxRatio:            1,
		audioRxRatio:       1,
		minRxRatio:         0.99,
		maxP95Ms:           90,
		skipAdminReconcile: true,
		viewerDropPct:      2,
		viewerDropped:      1000,
		viewerRepaired:     989,
	}
	if reasons := evaluateGates(r); len(reasons) != 1 || !strings.HasPrefix(reasons[0], "viewer_leg_repaired") {
		t.Fatalf("want one viewer_leg_repaired failure, got %v", reasons)
	}
	r.viewerRepaired = 990
	if reasons := evaluateGates(r); len(reasons) != 0 {
		t.Fatalf("want no failures, got %v", reasons)
	}
	r.viewerDropped, r.viewerRepaired = 0, 0
	if reasons := evaluateGates(r); len(reasons) != 1 {
		t.Fatalf("want a failure when nothing was dropped, got %v", reasons)
	}
}
