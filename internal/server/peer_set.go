package server

import (
	"sync/atomic"
)

// PeerSet is a copy-on-write set optimised for frequent reads (forwarding loop)
// and infrequent writes (member join/leave).
type PeerSet struct {
	peers atomic.Pointer[[]*Peer]
}

func NewPeerSet() *PeerSet {
	ps := &PeerSet{}
	empty := make([]*Peer, 0)
	ps.peers.Store(&empty)
	return ps
}

func (ps *PeerSet) Snapshot() []*Peer {
	p := ps.peers.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (ps *PeerSet) Len() int {
	return len(ps.Snapshot())
}

func (ps *PeerSet) ForEach(fn func(*Peer)) {
	for _, p := range ps.Snapshot() {
		fn(p)
	}
}

func (ps *PeerSet) Add(peer *Peer) {
	for {
		old := ps.peers.Load()
		var newSlice []*Peer
		if old != nil {
			newSlice = make([]*Peer, len(*old)+1)
			copy(newSlice, *old)
			newSlice[len(*old)] = peer
		} else {
			newSlice = []*Peer{peer}
		}
		if ps.peers.CompareAndSwap(old, &newSlice) {
			return
		}
	}
}

func (ps *PeerSet) Remove(userID string) *Peer {
	for {
		old := ps.peers.Load()
		if old == nil {
			return nil
		}

		var removed *Peer
		newSlice := make([]*Peer, 0, len(*old))
		for _, p := range *old {
			if p.UserID == userID {
				removed = p
			} else {
				newSlice = append(newSlice, p)
			}
		}

		if ps.peers.CompareAndSwap(old, &newSlice) {
			return removed
		}
	}
}
