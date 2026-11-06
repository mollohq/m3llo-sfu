// rtp-sfu-mini: minimal SFU that accepts a single client via TCP signaling.
//
// Protocol (newline-delimited JSON over TCP):
//
//	{"type":"offer","sdp":"..."}   client → SFU  (initial offer)
//	{"type":"answer","sdp":"..."}  SFU → client  (initial answer)
//	{"type":"offer","sdp":"..."}   SFU → client  (renegotiation: outgoing track added)
//	{"type":"answer","sdp":"..."}  client → SFU  (renegotiation answer)
//
// After renegotiation, SFU sends 50 pps of test RTP and prints stats.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

type sigMsg struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

func main() {
	addr := ":9999"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		fatal("listen: %v", err)
	}
	fmt.Printf("[sfu-mini] Listening on %s ...\n", addr)

	conn, err := ln.Accept()
	if err != nil {
		fatal("accept: %v", err)
	}
	fmt.Printf("[sfu-mini] Client connected from %s\n", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	send := func(msg sigMsg) {
		data, _ := json.Marshal(msg)
		writer.Write(data)
		writer.WriteByte('\n')
		writer.Flush()
	}
	recv := func() sigMsg {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			fatal("recv: %v", err)
		}
		var msg sigMsg
		if err := json.Unmarshal(line, &msg); err != nil {
			fatal("unmarshal: %v", err)
		}
		return msg
	}

	// Set up Pion
	me := &webrtc.MediaEngine{}
	me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio)

	se := webrtc.SettingEngine{}
	se.SetHandleUndeclaredSSRCWithoutAnswer(true)
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithSettingEngine(se))

	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		fatal("new PC: %v", err)
	}
	defer pc.Close()

	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		fmt.Printf("[sfu-mini] OnTrack: codec=%s ssrc=%d streamID=%s\n",
			track.Codec().MimeType, track.SSRC(), track.StreamID())
		go func() {
			count := 0
			for {
				_, _, err := track.ReadRTP()
				if err != nil {
					fmt.Printf("[sfu-mini] Track read ended: %v (after %d pkts)\n", err, count)
					return
				}
				count++
				if count%100 == 0 {
					fmt.Printf("[sfu-mini] Received %d packets from client\n", count)
				}
			}
		}()
	})

	connected := make(chan struct{}, 1)
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		fmt.Printf("[sfu-mini] Connection state: %s\n", s)
		if s == webrtc.PeerConnectionStateConnected {
			select {
			case connected <- struct{}{}:
			default:
			}
		}
	})

	// Step 1: receive initial offer
	fmt.Println("\n[step 1] Waiting for client offer...")
	offerMsg := recv()
	if offerMsg.Type != "offer" {
		fatal("expected offer, got %s", offerMsg.Type)
	}
	fmt.Printf("[sfu-mini] Got offer (%d bytes)\n", len(offerMsg.SDP))

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerMsg.SDP,
	}); err != nil {
		fatal("SetRemoteDescription: %v", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		fatal("CreateAnswer: %v", err)
	}
	gatherDone := webrtc.GatheringCompletePromise(pc)
	pc.SetLocalDescription(answer)
	<-gatherDone

	localDesc := pc.LocalDescription()
	send(sigMsg{Type: "answer", SDP: localDesc.SDP})
	fmt.Printf("[sfu-mini] Sent answer (%d bytes)\n", len(localDesc.SDP))

	// Wait for connection
	fmt.Println("\n[step 2] Waiting for connection...")
	select {
	case <-connected:
		fmt.Println("[sfu-mini] Connected!")
	case <-time.After(10 * time.Second):
		fatal("connection timeout")
	}

	time.Sleep(500 * time.Millisecond) // let some client audio flow

	// Step 3: add outgoing track (renegotiation)
	fmt.Println("\n[step 3] Adding outgoing audio track + renegotiating...")

	outTrack, _ := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		"audio", "test-sender-uuid",
	)

	t, err := pc.AddTransceiverFromTrack(outTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		fatal("AddTransceiverFromTrack: %v", err)
	}
	params := t.Sender().GetParameters()
	ssrc := uint32(params.Encodings[0].SSRC)
	fmt.Printf("[sfu-mini] Outgoing track SSRC: %d\n", ssrc)

	reOffer, _ := pc.CreateOffer(nil)
	gatherDone2 := webrtc.GatheringCompletePromise(pc)
	pc.SetLocalDescription(reOffer)
	<-gatherDone2

	reOfferDesc := pc.LocalDescription()
	fmt.Printf("[sfu-mini] Sending renegotiation offer (%d bytes)\n", len(reOfferDesc.SDP))
	send(sigMsg{Type: "offer", SDP: reOfferDesc.SDP})

	// Wait for renegotiation answer
	fmt.Println("[sfu-mini] Waiting for renegotiation answer...")
	answerMsg := recv()
	if answerMsg.Type != "answer" {
		fatal("expected answer, got %s", answerMsg.Type)
	}
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answerMsg.SDP,
	}); err != nil {
		fatal("SetRemoteDescription(answer): %v", err)
	}
	fmt.Printf("[sfu-mini] Renegotiation answer applied (%d bytes)\n", len(answerMsg.SDP))

	// Step 4: send test RTP
	fmt.Println("\n[step 4] Sending 200 test RTP packets (50pps, 4 seconds)...")
	for i := 0; i < 200; i++ {
		pkt := &rtp.Packet{
			Header: rtp.Header{
				Version: 2, PayloadType: 111,
				SequenceNumber: uint16(i), Timestamp: uint32(i) * 960,
				SSRC: ssrc,
			},
			Payload: make([]byte, 80), // 80 bytes of silence opus
		}
		if err := outTrack.WriteRTP(pkt); err != nil {
			fmt.Printf("[sfu-mini] WriteRTP error at pkt %d: %v\n", i, err)
		}
		if (i+1)%50 == 0 {
			fmt.Printf("[sfu-mini] Sent %d packets\n", i+1)
		}
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Println("\n[sfu-mini] Done sending. Waiting 2s for client to report...")
	time.Sleep(2 * time.Second)
	fmt.Println("[sfu-mini] Test complete.")
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
