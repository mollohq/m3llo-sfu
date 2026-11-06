package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media/oggreader"
)

// speechOgg is 11.5 s of read speech as Ogg Opus, one 20 ms packet per page:
// m1_davison.wav from the mello voice gate corpus (LibriVox, public domain;
// mello/libmello/tests/fixtures/voice/LICENSE), encoded with
// `ffmpeg -c:a libopus -b:a 48k -frame_duration 20 -application voip
// -page_duration 20000 -ac 1`.
//
//go:embed testdata/speech.ogg
var speechOgg []byte

const opusFrameSamples = 960 // 20 ms at 48 kHz

// loadSpeechPackets returns the Opus packets of the embedded speech clip.
func loadSpeechPackets() ([][]byte, error) {
	reader, _, err := oggreader.NewWith(bytes.NewReader(speechOgg))
	if err != nil {
		return nil, err
	}
	var packets [][]byte
	for {
		payload, _, err := reader.ParseNextPage()
		if err != nil {
			break
		}
		// Skip the OpusTags page.
		if len(payload) >= 8 && string(payload[:8]) == "OpusTags" {
			continue
		}
		packets = append(packets, payload)
	}
	if len(packets) == 0 {
		return nil, fmt.Errorf("no opus packets in the speech clip")
	}
	return packets, nil
}

// clientSlots is one soak client's view of its voice session: which mid
// carries which member (from the SFU's slot messages), and the offers it
// received.
type clientSlots struct {
	recvSlots int

	mu       sync.Mutex
	mids     map[string]string
	slotMids map[string]bool
	// released is when each mid last lost its owner. pending holds the
	// arrival times of RTP on a mid with no owner, until the next slot
	// message for the mid tells whether they were late (the old owner's,
	// after the release) or early (the new owner's, before the assign).
	released map[string]time.Time
	pending  map[string][]time.Time

	// RTP received per mid, counted once a slot message names the mid.
	rxMapped   atomic.Int64
	rxUnmapped atomic.Int64
}

func newClientSlots(recvSlots int) *clientSlots {
	return &clientSlots{
		recvSlots: recvSlots,
		mids:      make(map[string]string),
		slotMids:  make(map[string]bool),
		released:  make(map[string]time.Time),
		pending:   make(map[string][]time.Time),
	}
}

// lateWindow: RTP on a mid with no owner up to this long after its release
// is counted as the old owner's late packet, not the new owner's early one.
const lateWindow = 100 * time.Millisecond

// recordUnmapped notes one RTP packet on a mid that has no owner now.
func (c *clientSlots) recordUnmapped(mid string) {
	c.mu.Lock()
	c.pending[mid] = append(c.pending[mid], time.Now())
	c.mu.Unlock()
}

// settleLocked counts the pending packets of mid as early or late. assign
// is true when the slot message that settles them names a new owner.
// Callers hold mu.
func (c *clientSlots) settleLocked(mid string, assign bool, stats *soakStats) {
	early, late := &stats.rtpRxEarlySlot, &stats.rtpRxLateSlot
	if !c.slotMids[mid] {
		early, late = &stats.rtpRxEarlyOwn, &stats.rtpRxLateOwn
	}
	releasedAt, hadOwner := c.released[mid]
	now := time.Now()
	for _, at := range c.pending[mid] {
		if assign && (!hadOwner || at.Sub(releasedAt) > lateWindow) {
			early.Add(1)
			lead := int64(now.Sub(at))
			for {
				cur := stats.rtpRxEarlyLeadMax.Load()
				if lead <= cur || stats.rtpRxEarlyLeadMax.CompareAndSwap(cur, lead) {
					break
				}
			}
		} else {
			late.Add(1)
		}
	}
	delete(c.pending, mid)
}

// flush counts the packets still pending when the session ends as late.
func (c *clientSlots) flush(stats *soakStats) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for mid := range c.pending {
		c.settleLocked(mid, false, stats)
	}
}

// markSlotMids records the mids of this client's own recvonly m-lines (the
// receive slots), once the offer assigned them.
func (c *clientSlots) markSlotMids(pc *webrtc.PeerConnection) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, tr := range pc.GetTransceivers() {
		if tr.Kind() == webrtc.RTPCodecTypeAudio &&
			tr.Direction() == webrtc.RTPTransceiverDirectionRecvonly &&
			tr.Mid() != "" {
			c.slotMids[tr.Mid()] = true
		}
	}
}

func (c *clientSlots) isSlot(mid string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.slotMids[mid]
}

func (c *clientSlots) owner(mid string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mids[mid]
}

// recordOffer classifies a server offer. An offer with no audio m-line
// beyond the mic and the receive slots changes nothing a slot could not
// carry: within the slots it is a defect (01-SFU.md §5.5). An offer with
// more audio m-lines adds (or keeps) an own m-line for a sender past the
// slots. The SDP is the judge, not the member count: the SFU's member
// events can miss joins that run at the same time.
func (c *clientSlots) recordOffer(sdp string, stats *soakStats) {
	if strings.Count(sdp, "m=audio ") > 1+c.recvSlots {
		stats.offersPastSlots.Add(1)
	} else {
		stats.offersWithinSlots.Add(1)
	}
}

// handleControl handles one SFU message on the voice control channel.
func (c *clientSlots) handleControl(data []byte, stats *soakStats) {
	var msg struct {
		Type string  `json:"type"`
		Mid  string  `json:"mid"`
		User *string `json:"user"`
		On   *bool   `json:"on"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}
	switch msg.Type {
	case "slot":
		stats.slotMsgRx.Add(1)
		c.mu.Lock()
		c.settleLocked(msg.Mid, msg.User != nil, stats)
		if msg.User != nil {
			c.mids[msg.Mid] = *msg.User
		} else {
			delete(c.mids, msg.Mid)
			c.released[msg.Mid] = time.Now()
		}
		c.mu.Unlock()
	case "speaking":
		stats.speakingMsgRx.Add(1)
	}
}

// addRecvSlots adds the receive slots of the mello voice client: recvonly
// audio m-lines after the mic m-line.
func addRecvSlots(pc *webrtc.PeerConnection, n int) error {
	for i := 0; i < n; i++ {
		if _, err := pc.AddTransceiverFromKind(
			webrtc.RTPCodecTypeAudio,
			webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly},
		); err != nil {
			return err
		}
	}
	return nil
}

// speechPump sends the speech clip as a mello client does: a random start
// sequence and timestamp, the RTP timestamp on the 48 kHz media clock from
// the wall clock, and with vadGaps, talk spurts with silences in which no
// packet goes out but the timestamp still advances. With vadGaps it also
// sends the in-band speaking state on the control channel.
func startSpeechPump(
	track *webrtc.TrackLocalStaticRTP,
	control *webrtc.DataChannel,
	packets [][]byte,
	vadGaps bool,
	seed int64,
	stop <-chan struct{},
	stats *soakStats,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		rng := rand.New(rand.NewSource(seed))
		seq := uint16(rng.Intn(1 << 16))
		tsBase := rng.Uint32()
		start := time.Now()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()

		talking := true
		phaseEnd := time.Now().Add(spurtLength(rng, true))
		sendSpeaking := func(on bool) {
			if control == nil || control.ReadyState() != webrtc.DataChannelStateOpen {
				return
			}
			msg := `{"type":"speaking","on":false}`
			if on {
				msg = `{"type":"speaking","on":true}`
			}
			if control.SendText(msg) == nil {
				stats.speakingMsgTx.Add(1)
			}
		}
		if vadGaps {
			sendSpeaking(true)
		}

		next := rng.Intn(len(packets))
		for {
			select {
			case <-stop:
				return
			case now := <-ticker.C:
				if vadGaps && now.After(phaseEnd) {
					talking = !talking
					phaseEnd = now.Add(spurtLength(rng, talking))
					sendSpeaking(talking)
				}
				if !talking {
					continue
				}
				frame := uint32(now.Sub(start) / (20 * time.Millisecond))
				payload := packets[next]
				next = (next + 1) % len(packets)
				pkt := &rtp.Packet{
					Header: rtp.Header{
						Version:        2,
						PayloadType:    111,
						SequenceNumber: seq,
						Timestamp:      tsBase + frame*opusFrameSamples,
					},
					Payload: payload,
				}
				if err := track.WriteRTP(pkt); err != nil {
					stats.rtpTxErrors.Add(1)
				} else {
					stats.rtpTxPackets.Add(1)
					stats.rtpTxBytes.Add(int64(len(payload)))
				}
				seq++
			}
		}
	}()
	return done
}

// spurtLength: speech 1 to 3 s, silence 0.5 to 2 s.
func spurtLength(rng *rand.Rand, talking bool) time.Duration {
	if talking {
		return time.Second + time.Duration(rng.Int63n(int64(2*time.Second)))
	}
	return 500*time.Millisecond + time.Duration(rng.Int63n(int64(1500*time.Millisecond)))
}
