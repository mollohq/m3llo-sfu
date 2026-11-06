package server

import (
	"sync"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
)

// configureStatsInterceptor registers Pion's stats interceptor behind a close
// guard.
//
// pion/interceptor (v0.1.49 and main as of 2026-10-08) starts a stats recorder
// with WaitGroup.Add in BindLocalStream/BindRemoteStream and waits on the
// same WaitGroup in Close, without ordering the two. Pion can close a
// PeerConnection on its own (DTLS failure) while SetRemoteDescription binds a
// new sender stream, so Add can run during Wait: the race detector reports it
// in the churn soak, and a recorder bound after Close is never stopped.
// The guard orders every bind before Close and passes binds after Close
// through without a recorder.
func configureStatsInterceptor(registry *interceptor.Registry) error {
	statsRegistry := &interceptor.Registry{}
	// webrtc.ConfigureStatsInterceptor keeps the PeerConnection.GetStats wiring.
	if err := webrtc.ConfigureStatsInterceptor(statsRegistry); err != nil {
		return err
	}
	registry.Add(&closeGuardFactory{inner: interceptorFactoryFunc(statsRegistry.Build)})

	return nil
}

type interceptorFactoryFunc func(id string) (interceptor.Interceptor, error)

func (f interceptorFactoryFunc) NewInterceptor(id string) (interceptor.Interceptor, error) {
	return f(id)
}

type closeGuardFactory struct {
	inner interceptor.Factory
}

func (f *closeGuardFactory) NewInterceptor(id string) (interceptor.Interceptor, error) {
	inner, err := f.inner.NewInterceptor(id)
	if err != nil {
		return nil, err
	}

	return &closeGuardInterceptor{Interceptor: inner}, nil
}

// closeGuardInterceptor forwards to the wrapped interceptor. Stream binds and
// Close are ordered by mu; a stream bind after Close is not forwarded.
type closeGuardInterceptor struct {
	interceptor.Interceptor

	mu     sync.RWMutex
	closed bool
}

func (g *closeGuardInterceptor) BindLocalStream(
	info *interceptor.StreamInfo, writer interceptor.RTPWriter,
) interceptor.RTPWriter {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return writer
	}

	return g.Interceptor.BindLocalStream(info, writer)
}

func (g *closeGuardInterceptor) BindRemoteStream(
	info *interceptor.StreamInfo, reader interceptor.RTPReader,
) interceptor.RTPReader {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.closed {
		return reader
	}

	return g.Interceptor.BindRemoteStream(info, reader)
}

func (g *closeGuardInterceptor) Close() error {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()

	return g.Interceptor.Close()
}
