package server

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/mollohq/m3llo-sfu/internal/metrics"
)

// The metrics collector writes the bandwidth and CPU fields while /health,
// /stats and /admin read them. Run with -race: the race detector is the
// assertion.
func TestMetricsSnapshotDoesNotRaceHealthAndStats(t *testing.T) {
	logger := logging.New("error")
	m := metrics.New()
	s := &Server{
		sessions:     NewSessionStore(logger),
		metrics:      m,
		peakSessions: newPeakSessionsTracker(),
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				m.RecordBytesIn(100)
				m.RecordBytesOut(100)
				m.TakeSnapshot()
			}
		}
	}()
	for i := 0; i < 200; i++ {
		s.handleHealth(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
		s.handleStats(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/stats", nil))
	}
	close(stop)
	wg.Wait()
}
