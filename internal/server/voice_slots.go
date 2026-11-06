package server

import (
	"encoding/json"
	"errors"
	"sync"

	"github.com/mollohq/m3llo-sfu/internal/logging"
	"github.com/pion/webrtc/v4"
)

// voiceRecvSlots is the most receive slots the SFU binds per voice peer
// (01-SFU.md §6.2). A client offers one recvonly audio m-line per slot. A
// member past the slots gets its own m-line through renegotiation.
const voiceRecvSlots = 16

var ErrNoVoiceSlot = errors.New("no free voice receive slot")

// slotAllocator maps sender user ids to the receive slots of one receiver.
// It is pure bookkeeping: the Peer applies each decision to its RTP senders.
//
// A free slot is chosen by age: a slot never used comes first, in offer
// order, then the slot released longest ago. A reassigned slot keeps its
// SSRC, so reusing the most recently freed slot would maximise the chance
// that the receiver sees the new owner's packets before the slot message
// (the two travel on different transports).
type slotAllocator struct {
	mids     []string
	owner    map[string]string // mid -> user id
	byUser   map[string]string // user id -> mid
	released map[string]uint64 // mid -> release order; absent = never used
	clock    uint64
}

func newSlotAllocator(mids []string) *slotAllocator {
	return &slotAllocator{
		mids:     append([]string(nil), mids...),
		owner:    make(map[string]string, len(mids)),
		byUser:   make(map[string]string, len(mids)),
		released: make(map[string]uint64, len(mids)),
	}
}

// Assign gives user a slot. A user that holds a slot keeps it (a reconnect
// that races its own release). ok is false when every slot is taken.
func (a *slotAllocator) Assign(user string) (mid string, ok bool) {
	if mid, held := a.byUser[user]; held {
		return mid, true
	}
	best := ""
	var bestAge uint64
	for _, m := range a.mids {
		if _, taken := a.owner[m]; taken {
			continue
		}
		age, used := a.released[m]
		if !used {
			best = m
			break
		}
		if best == "" || age < bestAge {
			best, bestAge = m, age
		}
	}
	if best == "" {
		return "", false
	}
	a.owner[best] = user
	a.byUser[user] = best
	return best, true
}

// Release frees the slot that user holds. ok is false when it holds none.
func (a *slotAllocator) Release(user string) (mid string, ok bool) {
	mid, ok = a.byUser[user]
	if !ok {
		return "", false
	}
	delete(a.byUser, user)
	delete(a.owner, mid)
	a.clock++
	a.released[mid] = a.clock
	return mid, true
}

// Owner returns the user that holds mid, or "".
func (a *slotAllocator) Owner(mid string) string { return a.owner[mid] }

// Free returns the count of free slots.
func (a *slotAllocator) Free() int { return len(a.mids) - len(a.owner) }

// slotSeq keeps the outgoing RTP sequence of one slot continuous across
// owners. A slot keeps its SSRC when it changes owner, so the receiver sees
// one RTP stream. Without the rewrite the new owner's sequence numbers would
// collide with the old owner's in the receiver's SRTP replay window, and
// SRTP drops them (Pion spike, 01-SFU.md §6.2).
type slotSeq struct {
	mu      sync.Mutex
	started bool
	resync  bool
	offset  uint16
	highest uint16
}

// newOwner makes the next packet continue the slot's sequence.
func (s *slotSeq) newOwner() {
	s.mu.Lock()
	s.resync = true
	s.mu.Unlock()
}

// next maps an owner's sequence number to the slot's sequence number.
// Gaps and reordering inside one owner's stream are kept.
func (s *slotSeq) next(in uint16) uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case !s.started:
		s.started = true
		s.resync = false
		s.offset = 0
		s.highest = in
		return in
	case s.resync:
		s.resync = false
		s.offset = s.highest + 1 - in
	}
	out := in + s.offset
	if int16(out-s.highest) > 0 {
		s.highest = out
	}
	return out
}

// voiceSlot is one receive slot: the sender on the transceiver of one
// recvonly m-line of the client's offer.
type voiceSlot struct {
	mid         string
	sender      *webrtc.RTPSender
	placeholder *webrtc.TrackLocalStaticRTP
	ssrc        uint32
	seq         slotSeq
}

// slotMessage is the control-channel message for one slot (01-SFU.md §5.4).
type slotMessage struct {
	Type string  `json:"type"`
	Mid  string  `json:"mid"`
	User *string `json:"user"`
}

// speakingMessage is the control-channel speaking state of one member.
type speakingMessage struct {
	Type string `json:"type"`
	User string `json:"user,omitempty"`
	On   bool   `json:"on"`
}

func encodeSlotMessage(mid, user string) string {
	msg := slotMessage{Type: "slot", Mid: mid}
	if user != "" {
		msg.User = &user
	}
	data, _ := json.Marshal(msg)
	return string(data)
}

func encodeSpeakingMessage(user string, on bool) string {
	data, _ := json.Marshal(speakingMessage{Type: "speaking", User: user, On: on})
	return string(data)
}

func voiceOpusCapability() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:    webrtc.MimeTypeOpus,
		ClockRate:   48000,
		Channels:    2,
		SDPFmtpLine: "minptime=10;useinbandfec=1",
	}
}

// BindVoiceSlots gives each recvonly audio m-line of the client's offer a
// sender with a silent placeholder track, up to voiceRecvSlots. Call it after
// SetRemoteDescription and before CreateAnswer, so the answer declares each
// slot's SSRC. It returns the count of slots bound by this call; a later
// offer on the same PeerConnection binds nothing new.
//
// The transceiver of a recvonly m-line has no sender yet. pc.AddTrack must
// not be used here: it binds the first transceiver whose current direction
// allows a send, which is the client's sendrecv mic m-line.
func (p *Peer) BindVoiceSlots(api *webrtc.API, pc *webrtc.PeerConnection) (int, error) {
	if api == nil || pc == nil {
		return 0, ErrNoPeerConnection
	}
	p.slotMu.Lock()
	defer p.slotMu.Unlock()
	// Close detaches the PeerConnection under slotMu: never bind slots on
	// one that is closing.
	if cur, _ := p.currentPeerConnection(); cur != pc {
		return 0, ErrNoPeerConnection
	}

	p.tracksMu.Lock()
	if p.slotsPC != pc {
		// A new PeerConnection: slots of the old one are gone with it.
		p.resetVoiceSlotsLocked()
		p.slotsPC = pc
	}
	have := len(p.voiceSlots)
	p.tracksMu.Unlock()

	var bound []*voiceSlot
	for _, t := range pc.GetTransceivers() {
		if have+len(bound) >= voiceRecvSlots {
			break
		}
		if t.Kind() != webrtc.RTPCodecTypeAudio ||
			t.Sender() != nil ||
			t.Direction() != webrtc.RTPTransceiverDirectionSendonly ||
			t.Mid() == "" {
			continue
		}
		placeholder, err := webrtc.NewTrackLocalStaticRTP(
			voiceOpusCapability(), "audio", "slot-"+t.Mid(),
		)
		if err != nil {
			return len(bound), err
		}
		sender, err := api.NewRTPSender(placeholder, pc.SCTP().Transport())
		if err != nil {
			return len(bound), err
		}
		if err := t.SetSender(sender, placeholder); err != nil {
			_ = sender.Stop()
			return len(bound), err
		}
		var ssrc uint32
		if params := sender.GetParameters(); len(params.Encodings) > 0 {
			ssrc = uint32(params.Encodings[0].SSRC)
		}
		bound = append(bound, &voiceSlot{
			mid:         t.Mid(),
			sender:      sender,
			placeholder: placeholder,
			ssrc:        ssrc,
		})
	}
	if len(bound) == 0 {
		return 0, nil
	}

	p.tracksMu.Lock()
	if p.voiceSlots == nil {
		p.voiceSlots = make(map[string]*voiceSlot, len(bound))
	}
	mids := make([]string, 0, have+len(bound))
	if p.slotAlloc != nil {
		mids = append(mids, p.slotAlloc.mids...)
	}
	for _, s := range bound {
		p.voiceSlots[s.mid] = s
		mids = append(mids, s.mid)
	}
	old := p.slotAlloc
	p.slotAlloc = newSlotAllocator(mids)
	if old != nil {
		for user, mid := range old.byUser {
			p.slotAlloc.owner[mid] = user
			p.slotAlloc.byUser[user] = mid
		}
	}
	p.tracksMu.Unlock()

	p.logger.Info("voice_slots_bound", logging.D{
		"session": p.sessionID(),
		"user":    p.UserID,
		"slots":   have + len(bound),
	})
	return len(bound), nil
}

// resetVoiceSlotsLocked drops the slot state and every slot-backed outgoing
// track. Callers hold tracksMu.
func (p *Peer) resetVoiceSlotsLocked() {
	for sender, ot := range p.OutgoingTracks {
		if ot != nil && ot.slot != nil {
			delete(p.OutgoingTracks, sender)
		}
	}
	p.voiceSlots = nil
	p.slotAlloc = nil
}

// VoiceSlotCount returns the count of bound receive slots.
func (p *Peer) VoiceSlotCount() int {
	p.tracksMu.RLock()
	defer p.tracksMu.RUnlock()
	return len(p.voiceSlots)
}

// AssignVoiceSlot forwards senderID's audio to this peer on a free receive
// slot: the slot's sender swaps its placeholder for a forwarding track with
// ReplaceTrack. No renegotiation. ErrNoVoiceSlot means every slot is taken
// and the caller must fall back to AddOutgoingTrack and Renegotiate.
func (p *Peer) AssignVoiceSlot(senderID string) error {
	p.slotMu.Lock()
	defer p.slotMu.Unlock()

	p.tracksMu.Lock()
	if ot := p.OutgoingTracks[senderID]; ot != nil {
		// Already forwarded (slot or own m-line).
		p.tracksMu.Unlock()
		return nil
	}
	if p.slotAlloc == nil {
		p.tracksMu.Unlock()
		return ErrNoVoiceSlot
	}
	mid, ok := p.slotAlloc.Assign(senderID)
	if !ok {
		p.tracksMu.Unlock()
		return ErrNoVoiceSlot
	}
	slot := p.voiceSlots[mid]
	p.tracksMu.Unlock()

	track, err := webrtc.NewTrackLocalStaticRTP(voiceOpusCapability(), "audio", senderID)
	if err != nil {
		p.tracksMu.Lock()
		p.slotAlloc.Release(senderID)
		p.tracksMu.Unlock()
		return err
	}
	// The slot message goes out before the first packet of the new owner
	// can: SendRTP writes only once the track is in OutgoingTracks below.
	// RTP and the control channel are separate transports, so this narrows
	// the window but cannot close it (01-SFU.md §5.4).
	p.SendControlText(encodeSlotMessage(mid, senderID))
	if err := slot.sender.ReplaceTrack(track); err != nil {
		p.tracksMu.Lock()
		p.slotAlloc.Release(senderID)
		p.tracksMu.Unlock()
		p.SendControlText(encodeSlotMessage(mid, ""))
		return err
	}
	slot.seq.newOwner()

	p.tracksMu.Lock()
	if p.OutgoingTracks == nil {
		p.OutgoingTracks = make(map[string]*outgoingTrack)
	}
	p.OutgoingTracks[senderID] = &outgoingTrack{
		track: track,
		ssrc:  slot.ssrc,
		slot:  slot,
		mid:   mid,
	}
	p.tracksMu.Unlock()

	p.missingTrackWarnMu.Lock()
	if p.missingTrackWarnAt != nil {
		delete(p.missingTrackWarnAt, senderID)
	}
	p.missingTrackWarnMu.Unlock()

	p.logger.Info("voice_slot_assigned", logging.D{
		"session": p.sessionID(),
		"user":    p.UserID,
		"sender":  senderID,
		"mid":     mid,
		"ssrc":    slot.ssrc,
	})
	return nil
}

// releaseVoiceSlot swaps the slot back to its silent placeholder. Callers
// hold slotMu and have removed ot from OutgoingTracks.
func (p *Peer) releaseVoiceSlot(senderID string, ot *outgoingTrack) {
	if err := ot.slot.sender.ReplaceTrack(ot.slot.placeholder); err != nil {
		p.logger.Warn("voice_slot_release_failed", logging.D{
			"session": p.sessionID(),
			"user":    p.UserID,
			"sender":  senderID,
			"mid":     ot.mid,
			"error":   err.Error(),
		})
	}
	p.tracksMu.Lock()
	if p.slotAlloc != nil {
		p.slotAlloc.Release(senderID)
	}
	p.tracksMu.Unlock()
	p.SendControlText(encodeSlotMessage(ot.mid, ""))
	p.logger.Info("voice_slot_released", logging.D{
		"session": p.sessionID(),
		"user":    p.UserID,
		"sender":  senderID,
		"mid":     ot.mid,
	})
}

// announceOverflowMids sends the slot message for every outgoing track on
// its own m-line once its mid is known (after the renegotiation offer).
func (p *Peer) announceOverflowMids(pc *webrtc.PeerConnection) {
	p.slotMu.Lock()
	defer p.slotMu.Unlock()

	type pending struct {
		sender string
		ot     *outgoingTrack
	}
	var todo []pending
	p.tracksMu.RLock()
	for sender, ot := range p.OutgoingTracks {
		if ot != nil && ot.slot == nil && ot.mid == "" {
			todo = append(todo, pending{sender, ot})
		}
	}
	p.tracksMu.RUnlock()
	if len(todo) == 0 {
		return
	}
	for _, t := range pc.GetTransceivers() {
		s := t.Sender()
		if s == nil || t.Mid() == "" {
			continue
		}
		for _, item := range todo {
			if s.Track() != webrtc.TrackLocal(item.ot.track) {
				continue
			}
			p.tracksMu.Lock()
			item.ot.mid = t.Mid()
			p.tracksMu.Unlock()
			p.SendControlText(encodeSlotMessage(t.Mid(), item.sender))
		}
	}
}

// SendSlotMap sends the slot message for every receive slot and every
// outgoing m-line: the whole map. The SFU sends it when the control channel
// opens; messages sent before that were dropped.
func (p *Peer) SendSlotMap() {
	p.slotMu.Lock()
	defer p.slotMu.Unlock()

	var msgs []string
	p.tracksMu.RLock()
	if p.slotAlloc != nil {
		for _, mid := range p.slotAlloc.mids {
			msgs = append(msgs, encodeSlotMessage(mid, p.slotAlloc.Owner(mid)))
		}
	}
	for sender, ot := range p.OutgoingTracks {
		if ot != nil && ot.slot == nil && ot.mid != "" {
			msgs = append(msgs, encodeSlotMessage(ot.mid, sender))
		}
	}
	p.tracksMu.RUnlock()
	for _, m := range msgs {
		p.SendControlText(m)
	}
}

// SendControlText sends one text message on the voice control channel.
// Returns true only when the message is handed to Pion for delivery.
func (p *Peer) SendControlText(msg string) bool {
	if hook := p.controlTextHook; hook != nil {
		return hook(msg)
	}
	p.pcMu.RLock()
	defer p.pcMu.RUnlock()
	dc := p.ControlChannel
	if dc == nil || dc.ReadyState() != webrtc.DataChannelStateOpen {
		return false
	}
	return dc.SendText(msg) == nil
}

// parseSpeakingControl reads a client's `{"type":"speaking","on":bool}`.
func parseSpeakingControl(data []byte) (on bool, ok bool) {
	var msg struct {
		Type string `json:"type"`
		On   *bool  `json:"on"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return false, false
	}
	if msg.Type != "speaking" || msg.On == nil {
		return false, false
	}
	return *msg.On, true
}
