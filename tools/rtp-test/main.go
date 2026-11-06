// rtp-test: verifies the SFU's renegotiation + RTP forwarding flow locally
// using two Pion PeerConnections (one "SFU", one "client").
//
// Tests:
// 1. Client sends an audio track to SFU
// 2. SFU adds an outgoing track via AddTransceiverFromTrack(SendOnly)
// 3. SFU renegotiates (sends offer, client answers)
// 4. SFU forwards RTP with rewritten SSRC
// 5. Client receives the RTP on the new track
package main

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func main() {
	fmt.Println("=== RTP Renegotiation Test ===")

	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		fatal("register codec: %v", err)
	}

	se := webrtc.SettingEngine{}
	se.SetHandleUndeclaredSSRCWithoutAnswer(true)
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))

	// --- Create "client" PC ---
	clientPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		fatal("client PC: %v", err)
	}
	defer clientPC.Close()

	// Client creates an outgoing audio track
	clientTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "client-stream",
	)
	if err != nil {
		fatal("client track: %v", err)
	}
	if _, err := clientPC.AddTrack(clientTrack); err != nil {
		fatal("client AddTrack: %v", err)
	}

	// --- Create "SFU" PC ---
	sfuPC, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		fatal("SFU PC: %v", err)
	}
	defer sfuPC.Close()

	// SFU: wire OnTrack
	var sfuRecvCount atomic.Int64
	sfuPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Printf("[SFU] OnTrack: codec=%s ssrc=%d\n", track.Codec().MimeType, track.SSRC())
		go func() {
			for {
				_, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				sfuRecvCount.Add(1)
			}
		}()
	})

	// Client: wire OnTrack for renegotiated tracks
	var clientRecvCount atomic.Int64
	var clientOnTrackStreamID string
	var clientOnTrackMu sync.Mutex
	clientPC.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		clientOnTrackMu.Lock()
		clientOnTrackStreamID = track.StreamID()
		clientOnTrackMu.Unlock()
		fmt.Printf("[Client] OnTrack: codec=%s ssrc=%d streamID=%s mid=%s\n",
			track.Codec().MimeType, track.SSRC(), track.StreamID(), track.ID())
		go func() {
			for {
				_, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				clientRecvCount.Add(1)
			}
		}()
	})

	// === Initial SDP exchange ===
	fmt.Println("[1] Initial SDP exchange...")
	exchangeSDP(clientPC, sfuPC)
	fmt.Println("[1] Connected!")

	// Client sends test audio
	go func() {
		seq := uint16(0)
		for {
			seq++
			pkt := &rtp.Packet{
				Header: rtp.Header{
					Version: 2, PayloadType: 111,
					SequenceNumber: seq, Timestamp: uint32(seq) * 960,
					SSRC: 12345,
				},
				Payload: make([]byte, 40),
			}
			if err := clientTrack.WriteRTP(pkt); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	time.Sleep(300 * time.Millisecond)
	fmt.Printf("[SFU] Received %d packets from client\n\n", sfuRecvCount.Load())

	// === Test A: AddTrack (old behavior) ===
	fmt.Println("[2a] Testing AddTrack (may reuse m-line)...")
	testTrackA, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "sender-uuid-A",
	)
	senderA, err := sfuPC.AddTrack(testTrackA)
	if err != nil {
		fatal("AddTrack: %v", err)
	}
	paramsA := senderA.GetParameters()
	ssrcA := uint32(paramsA.Encodings[0].SSRC)
	fmt.Printf("  sender SSRC: %d\n", ssrcA)

	offerA := gatherOffer(sfuPC)
	fmt.Printf("  SDP: %d audio m-lines\n", countAudioMLines(offerA.SDP))
	printMsidLines(offerA.SDP)
	applyOfferAnswer(sfuPC, clientPC, offerA)

	time.Sleep(100 * time.Millisecond)

	sendTestPackets(testTrackA, ssrcA, 50)
	time.Sleep(300 * time.Millisecond)

	recvA := clientRecvCount.Load()
	clientOnTrackMu.Lock()
	streamA := clientOnTrackStreamID
	clientOnTrackMu.Unlock()
	fmt.Printf("  Client: recv=%d  onTrack_streamID=%q\n", recvA, streamA)

	// Remove for test B
	sfuPC.RemoveTrack(senderA)
	clientRecvCount.Store(0)
	clientOnTrackMu.Lock()
	clientOnTrackStreamID = ""
	clientOnTrackMu.Unlock()

	// === Test B: AddTransceiverFromTrack (new behavior) ===
	fmt.Println("\n[2b] Testing AddTransceiverFromTrack(SendOnly)...")
	testTrackB, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "sender-uuid-B",
	)
	tB, err := sfuPC.AddTransceiverFromTrack(testTrackB, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		fatal("AddTransceiverFromTrack: %v", err)
	}
	paramsB := tB.Sender().GetParameters()
	ssrcB := uint32(paramsB.Encodings[0].SSRC)
	fmt.Printf("  sender SSRC: %d\n", ssrcB)

	offerB := gatherOffer(sfuPC)
	fmt.Printf("  SDP: %d audio m-lines\n", countAudioMLines(offerB.SDP))
	printMsidLines(offerB.SDP)
	applyOfferAnswer(sfuPC, clientPC, offerB)

	time.Sleep(100 * time.Millisecond)

	sendTestPackets(testTrackB, ssrcB, 50)
	time.Sleep(300 * time.Millisecond)

	recvB := clientRecvCount.Load()
	clientOnTrackMu.Lock()
	streamB := clientOnTrackStreamID
	clientOnTrackMu.Unlock()
	fmt.Printf("  Client: recv=%d  onTrack_streamID=%q\n", recvB, streamB)

	// === Results ===
	fmt.Println("\n=== Results ===")
	fmt.Printf("AddTrack:                 recv=%-5d onTrack_streamID=%q\n", recvA, streamA)
	fmt.Printf("AddTransceiverFromTrack:  recv=%-5d onTrack_streamID=%q\n", recvB, streamB)

	if recvA == 0 && recvB > 0 {
		fmt.Println("\nConfirmed: AddTrack reuses m-line (no onTrack), AddTransceiverFromTrack creates new m-line (onTrack fires).")
		fmt.Println("✓ SFU-side logic is correct.")
	} else if recvA > 0 && recvB > 0 {
		fmt.Println("\nBoth methods work (Pion-to-Pion doesn't exhibit the m-line reuse issue).")
		fmt.Println("The issue may be libdatachannel-specific.")
	} else {
		fmt.Println("\n✗ Something unexpected happened.")
		os.Exit(1)
	}
}

func exchangeSDP(offerer, answerer *webrtc.PeerConnection) {
	offer := gatherOffer(offerer)
	if err := answerer.SetRemoteDescription(offer); err != nil {
		fatal("answerer SetRemote: %v", err)
	}
	answer := gatherAnswer(answerer)
	if err := offerer.SetRemoteDescription(answer); err != nil {
		fatal("offerer SetRemote: %v", err)
	}
}

func applyOfferAnswer(offerer, answerer *webrtc.PeerConnection, offer webrtc.SessionDescription) {
	if err := answerer.SetRemoteDescription(offer); err != nil {
		fatal("answerer SetRemote: %v", err)
	}
	answer := gatherAnswer(answerer)
	if err := offerer.SetRemoteDescription(answer); err != nil {
		fatal("offerer SetRemote: %v", err)
	}
}

func gatherOffer(pc *webrtc.PeerConnection) webrtc.SessionDescription {
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		fatal("CreateOffer: %v", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(offer); err != nil {
		fatal("SetLocal: %v", err)
	}
	<-gatherComplete
	return *pc.LocalDescription()
}

func gatherAnswer(pc *webrtc.PeerConnection) webrtc.SessionDescription {
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		fatal("CreateAnswer: %v", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		fatal("SetLocal: %v", err)
	}
	<-gatherComplete
	return *pc.LocalDescription()
}

func sendTestPackets(track *webrtc.TrackLocalStaticRTP, ssrc uint32, count int) {
	for i := 0; i < count; i++ {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version: 2, PayloadType: 111,
				SequenceNumber: uint16(i), Timestamp: uint32(i) * 960,
				SSRC: ssrc,
			},
			Payload: make([]byte, 40),
		}
		track.WriteRTP(pkt)
		time.Sleep(5 * time.Millisecond)
	}
}

func waitConnected(pc *webrtc.PeerConnection, label string) {
	done := make(chan struct{})
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		fatal("%s: connection timeout", label)
	}
}

func countAudioMLines(sdp string) int {
	count := 0
	for _, line := range strings.Split(sdp, "\n") {
		if strings.HasPrefix(line, "m=audio") {
			count++
		}
	}
	return count
}

func printMsidLines(sdp string) {
	for _, line := range strings.Split(sdp, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=msid:") || strings.HasPrefix(line, "a=mid:") {
			fmt.Printf("  %s\n", line)
		}
	}
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
