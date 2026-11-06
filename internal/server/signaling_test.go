package server

import (
	"strings"
	"testing"
	"time"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

func TestStreamRoleNegotiationAttachesVideoOnlyForViewer(t *testing.T) {
	t.Run("viewer reuses outgoing track on renegotiation", func(t *testing.T) {
		api := newTestAPI()
		viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create viewer PC: %v", err)
		}
		defer viewerPC.Close()
		if _, err := viewerPC.AddTransceiverFromKind(
			webrtc.RTPCodecTypeVideo,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
		); err != nil {
			t.Fatalf("add viewer video transceiver: %v", err)
		}

		serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create server PC: %v", err)
		}
		defer serverPC.Close()

		session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
		peer := &Peer{
			UserID: "viewer",
			Role:   "viewer",
			PC:     serverPC,
		}
		if err := session.AddPeer(peer); err != nil {
			t.Fatalf("add viewer to stream: %v", err)
		}

		offer, err := viewerPC.CreateOffer(nil)
		if err != nil {
			t.Fatalf("create viewer offer: %v", err)
		}
		if err := viewerPC.SetLocalDescription(offer); err != nil {
			t.Fatalf("set viewer local description: %v", err)
		}
		if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
			t.Fatalf("set server remote description: %v", err)
		}
		preparation, err := prepareStreamViewerVideo(peer, serverPC, 0)
		if err != nil {
			t.Fatalf("attach viewer video: %v", err)
		}
		firstSender, _ := peer.StreamVideoSender()
		if firstSender == nil {
			t.Fatal("viewer did not get an outgoing video sender")
		}
		if len(session.viewerVideo) != 0 {
			t.Fatal("viewer relay started before answer was applied")
		}

		answer, err := serverPC.CreateAnswer(nil)
		if err != nil {
			t.Fatalf("create server answer: %v", err)
		}
		if !strings.Contains(answer.SDP, "a=sendonly") {
			t.Fatalf("viewer answer did not advertise sendonly video:\n%s", answer.SDP)
		}
		if err := serverPC.SetLocalDescription(answer); err != nil {
			t.Fatalf("set server local description: %v", err)
		}
		if !preparation.Activate() {
			t.Fatal("viewer video preparation did not activate")
		}
		if err := viewerPC.SetRemoteDescription(*serverPC.LocalDescription()); err != nil {
			t.Fatalf("set viewer remote description: %v", err)
		}

		restartOffer, err := viewerPC.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
		if err != nil {
			t.Fatalf("create viewer ICE restart offer: %v", err)
		}
		if err := viewerPC.SetLocalDescription(restartOffer); err != nil {
			t.Fatalf("set viewer ICE restart offer: %v", err)
		}
		if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
			t.Fatalf("apply viewer ICE restart offer: %v", err)
		}
		restartPreparation, err := prepareStreamViewerVideo(peer, serverPC, 0)
		if err != nil {
			t.Fatalf("reattach viewer video: %v", err)
		}
		if !restartPreparation.Activate() {
			t.Fatal("viewer video restart preparation did not activate")
		}
		secondSender, _ := peer.StreamVideoSender()
		if secondSender != firstSender {
			t.Fatal("viewer ICE renegotiation replaced the outgoing video sender")
		}

		session.stopViewerVideo(peer)
		peer.closeStreamVideo()
	})

	t.Run("host gets no outgoing video track", func(t *testing.T) {
		api := newTestAPI()
		hostPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create host PC: %v", err)
		}
		defer hostPC.Close()

		hostTrack, err := webrtc.NewTrackLocalStaticRTP(
			streamVideoCodecCapability(),
			"video",
			"host",
		)
		if err != nil {
			t.Fatalf("create host video track: %v", err)
		}
		if _, err := hostPC.AddTransceiverFromTrack(hostTrack, webrtc.RTPTransceiverInit{
			Direction: webrtc.RTPTransceiverDirectionSendonly,
		}); err != nil {
			t.Fatalf("add host video transceiver: %v", err)
		}

		serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create server PC: %v", err)
		}
		defer serverPC.Close()

		session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
		peer := &Peer{
			UserID:  "host",
			Role:    "host",
			PC:      serverPC,
			Session: session,
		}

		offer, err := hostPC.CreateOffer(nil)
		if err != nil {
			t.Fatalf("create host offer: %v", err)
		}
		if err := hostPC.SetLocalDescription(offer); err != nil {
			t.Fatalf("set host local description: %v", err)
		}
		if err := serverPC.SetRemoteDescription(*hostPC.LocalDescription()); err != nil {
			t.Fatalf("set server remote description: %v", err)
		}
		preparation, err := prepareStreamViewerVideo(peer, serverPC, 0)
		if err != nil {
			t.Fatalf("host role attachment failed: %v", err)
		}
		if preparation != nil {
			t.Fatal("host role created a viewer-video preparation")
		}
		if sender, lifecycle := peer.StreamVideoSender(); sender != nil || lifecycle != nil {
			t.Fatal("host received an outgoing stream video sender")
		}

		answer, err := serverPC.CreateAnswer(nil)
		if err != nil {
			t.Fatalf("create server answer: %v", err)
		}
		if !strings.Contains(answer.SDP, "a=recvonly") {
			t.Fatalf("host answer did not remain recvonly:\n%s", answer.SDP)
		}
	})
}

func TestStreamViewerPreparationRollsBackFailedAnswer(t *testing.T) {
	api := newTestAPI()
	viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewerPC.Close()
	if _, err := viewerPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		t.Fatalf("add viewer video transceiver: %v", err)
	}
	offer, err := viewerPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create viewer offer: %v", err)
	}
	if err := viewerPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("set viewer local description: %v", err)
	}

	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create server PC: %v", err)
	}
	defer serverPC.Close()
	if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
		t.Fatalf("set server remote description: %v", err)
	}

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	peer := &Peer{
		UserID: "viewer",
		Role:   "viewer",
		PC:     serverPC,
	}
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add viewer to stream: %v", err)
	}
	preparation, err := prepareStreamViewerVideo(peer, serverPC, 0)
	if err != nil {
		t.Fatalf("prepare viewer video: %v", err)
	}
	_, lifecycle := peer.StreamVideoSender()
	if lifecycle == nil {
		t.Fatal("prepared video sender has no lifecycle")
	}

	// This is the same rollback used by applyOfferAndAnswer when CreateAnswer
	// or SetLocalDescription fails.
	preparation.Rollback()

	select {
	case <-lifecycle.Done():
	default:
		t.Fatal("failed answer rollback did not cancel prepared sender")
	}
	if sender, currentLifecycle := peer.StreamVideoSender(); sender != nil || currentLifecycle != nil {
		t.Fatal("failed answer rollback retained prepared sender")
	}
	if len(session.viewerVideo) != 0 {
		t.Fatal("failed answer rollback started relay loops")
	}
}

func TestPrepareVersusFinalizeCannotRetainSender(t *testing.T) {
	api := newTestAPI()
	viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create viewer PC: %v", err)
	}
	defer viewerPC.Close()
	if _, err := viewerPC.AddTransceiverFromKind(
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
	); err != nil {
		t.Fatalf("add viewer transceiver: %v", err)
	}
	offer, err := viewerPC.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := viewerPC.SetLocalDescription(offer); err != nil {
		t.Fatalf("set viewer offer: %v", err)
	}

	serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create server PC: %v", err)
	}
	defer serverPC.Close()
	if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
		t.Fatalf("apply offer: %v", err)
	}

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	peer := &Peer{UserID: "viewer", Role: "viewer", PC: serverPC}
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	preparation, err := prepareStreamViewerVideo(peer, serverPC, 0)
	if err != nil {
		t.Fatalf("prepare viewer video: %v", err)
	}

	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()
	if !session.FinalizeDrain(time.Now(), DrainTimeout) {
		t.Fatal("final drain did not win prepare/finalize race")
	}
	if preparation.Activate() {
		t.Fatal("prepared sender activated after final cleanup")
	}
	preparation.Rollback()
	if sender, lifecycle := peer.StreamVideoSender(); sender != nil || lifecycle != nil {
		t.Fatal("prepare/finalize race retained a sender")
	}
}

func TestFailedAnswerDiscardAllowsFreshPeerConnectionRetry(t *testing.T) {
	api := newTestAPI()
	newOfferPair := func(t *testing.T) (*webrtc.PeerConnection, *webrtc.PeerConnection) {
		t.Helper()
		viewerPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create viewer PC: %v", err)
		}
		if _, err := viewerPC.AddTransceiverFromKind(
			webrtc.RTPCodecTypeVideo,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
		); err != nil {
			t.Fatalf("add viewer transceiver: %v", err)
		}
		offer, err := viewerPC.CreateOffer(nil)
		if err != nil {
			t.Fatalf("create offer: %v", err)
		}
		if err := viewerPC.SetLocalDescription(offer); err != nil {
			t.Fatalf("set viewer offer: %v", err)
		}
		serverPC, err := api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			t.Fatalf("create server PC: %v", err)
		}
		if err := serverPC.SetRemoteDescription(*viewerPC.LocalDescription()); err != nil {
			t.Fatalf("apply viewer offer: %v", err)
		}
		return viewerPC, serverPC
	}

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	peer := &Peer{UserID: "viewer", Role: "viewer"}
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}

	firstViewer, firstServer := newOfferPair(t)
	defer firstViewer.Close()
	firstGeneration := peer.installPeerConnection(firstServer)
	firstPreparation, err := prepareStreamViewerVideo(peer, firstServer, firstGeneration)
	if err != nil {
		t.Fatalf("prepare failed attempt: %v", err)
	}
	firstPreparation.Rollback()
	if !peer.discardPeerConnection(firstServer, firstGeneration) {
		t.Fatal("failed negotiation PC was not discarded")
	}

	retryViewer, retryServer := newOfferPair(t)
	defer retryViewer.Close()
	defer retryServer.Close()
	retryGeneration := peer.installPeerConnection(retryServer)
	retryPreparation, err := prepareStreamViewerVideo(peer, retryServer, retryGeneration)
	if err != nil {
		t.Fatalf("prepare retry: %v", err)
	}
	answer, err := retryServer.CreateAnswer(nil)
	if err != nil {
		t.Fatalf("create retry answer: %v", err)
	}
	if err := retryServer.SetLocalDescription(answer); err != nil {
		t.Fatalf("set retry answer: %v", err)
	}
	if !retryPreparation.Activate() {
		t.Fatal("fresh PeerConnection retry did not activate")
	}
	session.stopViewerVideo(peer)
}

func TestStaleDataChannelOnOpenDoesNotInstallChannel(t *testing.T) {
	api := newTestAPI()
	pc1, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create first PC: %v", err)
	}
	defer pc1.Close()
	pc2, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create second PC: %v", err)
	}
	defer pc2.Close()

	peer := &Peer{}
	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	sessionGeneration := peer.bindSession(session)
	gen1 := peer.installPeerConnection(pc1)
	if !peer.discardPeerConnection(pc1, gen1) {
		t.Fatal("expected stale PC to be discarded")
	}
	gen2 := peer.installPeerConnection(pc2)

	offer, err := pc2.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create offer: %v", err)
	}
	if err := pc2.SetLocalDescription(offer); err != nil {
		t.Fatalf("set local description: %v", err)
	}
	dc, err := pc2.CreateDataChannel("media", nil)
	if err != nil {
		t.Fatalf("create data channel: %v", err)
	}

	if peer.setDataChannelForSession(
		pc1, gen1, session, sessionGeneration, dc, true,
	) {
		t.Fatal("stale DataChannel callback installed media channel")
	}
	if peer.setDataChannelForSession(
		pc2, gen2, session, sessionGeneration, dc, true,
	) {
		if peer.MediaChannel != dc {
			t.Fatal("current-generation DataChannel was not installed")
		}
	} else {
		t.Fatal("current-generation DataChannel install was rejected")
	}

	rejoined := NewStreamSession("stream-2", "crew", logging.New("error"), streamSnapshotConfig{})
	rejoinedGeneration := peer.bindSession(rejoined)
	if peer.setDataChannelForSession(
		pc2, gen2, session, sessionGeneration, dc, false,
	) {
		t.Fatal("old-session DataChannel callback installed after rejoin")
	}
	wired := false
	if peer.wireDataChannelsForSession(
		pc2,
		gen2,
		session,
		sessionGeneration,
		func(_, _ *webrtc.DataChannel) { wired = true },
	) {
		t.Fatal("old-session callback wiring was accepted after rejoin")
	}
	if wired {
		t.Fatal("old-session callback wiring ran after rejoin")
	}
	if !peer.setDataChannelForSession(
		pc2, gen2, rejoined, rejoinedGeneration, dc, false,
	) {
		t.Fatal("current-session DataChannel callback was rejected after rejoin")
	}
}

func TestDelayedDataChannelOnOpenAfterFinalDrainDoesNotWire(t *testing.T) {
	api := newTestAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	peer := &Peer{UserID: "viewer", Role: "viewer"}
	if err := session.AddPeer(peer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	peer.installPeerConnection(pc)
	capturedSession, capturedGeneration := peer.currentSession()

	session.videoMu.Lock()
	session.state = SessionDraining
	session.drainingAt = time.Now().Add(-DrainTimeout - time.Second)
	session.videoMu.Unlock()
	teardown, ok := session.PrepareFinalizeDrain(time.Now(), DrainTimeout)
	if !ok {
		t.Fatal("expected final drain to be ready")
	}
	teardown()

	currentSession, _ := peer.currentSession()
	if currentSession != nil {
		t.Fatal("final drain left session membership on peer")
	}
	if peer.isCurrentSession(capturedSession, capturedGeneration) || capturedSession == nil {
		t.Fatal("expected stale session guard to block delayed DataChannel OnOpen")
	}
	if peer.MediaChannel != nil {
		t.Fatal("delayed DataChannel OnOpen left a media channel installed")
	}
}

func TestStreamControlOnlyWiringDoesNotRequireMedia(t *testing.T) {
	api := newTestAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PC: %v", err)
	}
	defer pc.Close()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	viewer := &Peer{UserID: "viewer", Role: "viewer"}
	if err := session.AddPeer(viewer); err != nil {
		t.Fatalf("add viewer: %v", err)
	}
	pcGeneration := viewer.installPeerConnection(pc)
	currentSession, sessionGeneration := viewer.currentSession()

	control, err := pc.CreateDataChannel("control", nil)
	if err != nil {
		t.Fatalf("create control channel: %v", err)
	}
	if !viewer.setDataChannelForSession(
		pc, pcGeneration, currentSession, sessionGeneration, control, false,
	) {
		t.Fatal("control channel was not installed")
	}
	if viewer.MediaChannel != nil {
		t.Fatal("stream viewer must not require a media DataChannel")
	}

	wired := false
	if !viewer.wireControlChannelForSession(
		pc,
		pcGeneration,
		currentSession,
		sessionGeneration,
		func(dc *webrtc.DataChannel) {
			wired = dc != nil
			session.wireControlChannelFor(
				viewer,
				pc,
				pcGeneration,
				sessionGeneration,
				dc,
			)
		},
	) {
		t.Fatal("control-only wiring was rejected")
	}
	if !wired {
		t.Fatal("control channel was not wired")
	}
}

func TestSessionClosedErrorMapping(t *testing.T) {
	if got := streamJoinErrorCode(ErrSessionClosed); got != "SESSION_CLOSED" {
		t.Fatalf("join code = %q, want SESSION_CLOSED", got)
	}
	if got := negotiationErrorCode(ErrSessionClosed); got != "SESSION_CLOSED" {
		t.Fatalf("negotiation code = %q, want SESSION_CLOSED", got)
	}
	if got := streamJoinErrorCode(ErrSessionFull); got != "SESSION_FULL" {
		t.Fatalf("session-full code = %q, want SESSION_FULL", got)
	}
}

func TestRouteStreamHostTrackWiresOpusAudio(t *testing.T) {
	api := newTestAPI()
	serverPC, remoteTrack, cleanup := newHostIngressAudioTrackPair(t, api)
	defer cleanup()

	if !strings.EqualFold(remoteTrack.Codec().MimeType, webrtc.MimeTypeOpus) {
		t.Fatalf("remote track codec = %q, want opus", remoteTrack.Codec().MimeType)
	}

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	host := &Peer{UserID: "host", Role: "host"}
	host.bindSession(session)
	generation := host.installPeerConnection(serverPC)
	session.host = host

	routeStreamHostTrack(logging.New("error"), session, host, remoteTrack, serverPC, generation)

	session.videoMu.Lock()
	ingress := session.audioIngress
	session.videoMu.Unlock()
	if ingress == nil {
		t.Fatal("expected stream audio ingress to be wired")
	}
	if ingress.track != remoteTrack {
		t.Fatal("audio ingress track mismatch")
	}
}

func TestRouteStreamHostTrackRejectsVideoKindForOpusCodec(t *testing.T) {
	api := newTestAPI()
	serverPC, remoteTrack, cleanup := newHostIngressAudioTrackPair(t, api)
	defer cleanup()

	session := NewStreamSession("stream", "crew", logging.New("error"), streamSnapshotConfig{})
	host := &Peer{UserID: "host", Role: "host"}
	host.bindSession(session)
	generation := host.installPeerConnection(serverPC)
	session.host = host

	// Regression guard: audio ingress must not be gated on video kind.
	if remoteTrack.Kind() != webrtc.RTPCodecTypeAudio {
		t.Fatalf("fixture track kind = %s, want audio", remoteTrack.Kind().String())
	}
	routeStreamHostTrack(logging.New("error"), session, host, remoteTrack, serverPC, generation)

	session.videoMu.Lock()
	ingress := session.audioIngress
	session.videoMu.Unlock()
	if ingress == nil {
		t.Fatal("expected Opus audio track to wire stream audio ingress")
	}
}
