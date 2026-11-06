package server

import "github.com/pion/rtcp"

// Pion's transport-cc feedback adapter (interceptor internal/cc, v0.1.49)
// reads a feedback packet wrong in two ways, and both make the per-viewer
// gcc estimate fall on a leg that carries the stream (01-SFU.md §6.1):
//
//   - It takes every symbol of every status chunk as a packet, also the
//     padding symbols of the last status vector chunk after the packet
//     status count. rtcp keeps them as "not received". They name the
//     newest packets, which are in flight and in the adapter's history, so
//     each one counts as a lost packet. A libmello viewer ends a feedback
//     with a vector chunk whenever a loss or a large delta breaks the last
//     run: up to 13 phantom losses per feedback of 13 to 40 packets, and
//     Pion's loss controller cuts the target while the loss is over 10 %.
//   - Its history keeps the last 250 packets sent. An entry for an older
//     packet finds no history: it counts as lost and its arrival delta is
//     not consumed, so every later arrival time in that feedback shifts.
//
// sanitize gives the adapter only what it reads right: the packet status
// count entries, none older than the last viewerBWEHistoryKeep packets
// sent. The reference time stays; the first kept arrival delta carries
// the deltas of the entries it drops.
const viewerBWEHistoryKeep = pionFeedbackHistory - 10

// sanitize returns fb, a corrected copy, or nil when no entry is left.
func (e *viewerEstimator) sanitize(fb *rtcp.TransportLayerCC) *rtcp.TransportLayerCC {
	sent := e.sentSeq.Load()
	keepFrom, haveKeep := uint16(0), sent != 0
	if haveKeep {
		keepFrom = uint16(sent) - viewerBWEHistoryKeep + 1
	}
	out, padding, _ := sanitizeFeedback(fb, keepFrom, haveKeep)
	e.padding.Add(int64(padding))
	return out
}

// sanitizeFeedback is sanitize for a given oldest sequence number to keep
// (haveKeep false: keep all). It also returns the number of padding
// symbols and of old entries it removed.
func sanitizeFeedback(fb *rtcp.TransportLayerCC, keepFrom uint16, haveKeep bool) (*rtcp.TransportLayerCC, int, int) {
	count := int(fb.PacketStatusCount)
	symbols := make([]uint16, 0, count)
	total := 0
	for _, chunk := range fb.PacketChunks {
		switch c := chunk.(type) {
		case *rtcp.RunLengthChunk:
			for i := 0; i < int(c.RunLength); i++ {
				if len(symbols) < count {
					symbols = append(symbols, c.PacketStatusSymbol)
				}
				total++
			}
		case *rtcp.StatusVectorChunk:
			for _, s := range c.SymbolList {
				if len(symbols) < count {
					symbols = append(symbols, s)
				}
				total++
			}
		default:
			return fb, 0, 0
		}
	}
	if len(symbols) < count {
		return fb, 0, 0 // malformed: let Pion judge it
	}
	padding := total - count

	received := func(s uint16) bool {
		return s == rtcp.TypeTCCPacketReceivedSmallDelta || s == rtcp.TypeTCCPacketReceivedLargeDelta
	}
	drop := 0
	if haveKeep {
		for drop < count && int16(fb.BaseSequenceNumber+uint16(drop)-keepFrom) < 0 {
			drop++
		}
	}
	if padding == 0 && drop == 0 {
		return fb, 0, 0
	}
	if drop == count {
		return nil, padding, drop
	}

	deltas := fb.RecvDeltas
	var carry int64
	for _, s := range symbols[:drop] {
		if received(s) {
			if len(deltas) == 0 {
				return fb, 0, 0
			}
			carry += deltas[0].Delta
			deltas = deltas[1:]
		}
	}
	kept := symbols[drop:]
	out := &rtcp.TransportLayerCC{
		Header:             fb.Header,
		SenderSSRC:         fb.SenderSSRC,
		MediaSSRC:          fb.MediaSSRC,
		BaseSequenceNumber: fb.BaseSequenceNumber + uint16(drop),
		PacketStatusCount:  uint16(len(kept)),
		ReferenceTime:      fb.ReferenceTime,
		FbPktCount:         fb.FbPktCount,
		PacketChunks: []rtcp.PacketStatusChunk{&rtcp.StatusVectorChunk{
			Type:       rtcp.TypeTCCStatusVectorChunk,
			SymbolSize: rtcp.TypeTCCSymbolSizeTwoBit,
			SymbolList: kept,
		}},
	}
	for _, s := range kept {
		if !received(s) {
			continue
		}
		if len(deltas) == 0 {
			return fb, 0, 0
		}
		out.RecvDeltas = append(out.RecvDeltas, &rtcp.RecvDelta{Type: s, Delta: deltas[0].Delta + carry})
		carry = 0
		deltas = deltas[1:]
	}
	return out, padding, drop
}
