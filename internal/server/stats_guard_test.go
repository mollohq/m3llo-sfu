package server

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/stats"
	"github.com/pion/rtp"
)

func newGuardedStatsForTest(t *testing.T) (*closeGuardFactory, func(id string) stats.Getter) {
	t.Helper()
	factory, err := stats.NewInterceptor()
	if err != nil {
		t.Fatalf("stats.NewInterceptor: %v", err)
	}
	var mu sync.Mutex
	getters := map[string]stats.Getter{}
	factory.OnNewPeerConnection(func(id string, getter stats.Getter) {
		mu.Lock()
		getters[id] = getter
		mu.Unlock()
	})

	return &closeGuardFactory{inner: factory}, func(id string) stats.Getter {
		mu.Lock()
		defer mu.Unlock()

		return getters[id]
	}
}

func nopRTPWriter() interceptor.RTPWriter {
	return interceptor.RTPWriterFunc(func(*rtp.Header, []byte, interceptor.Attributes) (int, error) {
		return 0, nil
	})
}

// A stream bound after Close must not start a stats recorder: Close already
// waited for the recorders it stopped.
func TestStatsGuardBindAfterCloseStartsNoRecorder(t *testing.T) {
	factory, getter := newGuardedStatsForTest(t)
	chain, err := factory.NewInterceptor("pc-1")
	if err != nil {
		t.Fatalf("NewInterceptor: %v", err)
	}
	chain.BindLocalStream(&interceptor.StreamInfo{SSRC: 1, ClockRate: 48000}, nopRTPWriter())
	if getter("pc-1").Get(1) == nil {
		t.Fatal("bind before Close started no recorder")
	}
	if err := chain.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	chain.BindLocalStream(&interceptor.StreamInfo{SSRC: 2, ClockRate: 48000}, nopRTPWriter())
	chain.BindRemoteStream(&interceptor.StreamInfo{SSRC: 3, ClockRate: 48000},
		interceptor.RTPReaderFunc(func(b []byte, a interceptor.Attributes) (int, interceptor.Attributes, error) {
			return 0, a, nil
		}))
	if getter("pc-1").Get(2) != nil {
		t.Fatal("local stream bound after Close started a recorder")
	}
	if getter("pc-1").Get(3) != nil {
		t.Fatal("remote stream bound after Close started a recorder")
	}
}

// Close against concurrent binds, the order Pion produces when it closes a
// PeerConnection after a DTLS failure while SetRemoteDescription starts
// senders. Under -race the unguarded stats interceptor reports WaitGroup Add
// during Wait.
func TestStatsGuardCloseDuringBind(t *testing.T) {
	factory, _ := newGuardedStatsForTest(t)
	for round := 0; round < 200; round++ {
		chain, err := factory.NewInterceptor(fmt.Sprintf("pc-%d", round))
		if err != nil {
			t.Fatalf("NewInterceptor: %v", err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for ssrc := uint32(1); ssrc <= 20; ssrc++ {
				chain.BindLocalStream(&interceptor.StreamInfo{SSRC: ssrc, ClockRate: 48000}, nopRTPWriter())
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			_ = chain.Close()
		}()
		close(start)
		wg.Wait()
	}
}
