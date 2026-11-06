package logging

import (
	"sync"
)

const DefaultRingSize = 10_000

type Ring struct {
	mu      sync.RWMutex
	entries []Entry
	head    int
	count   int
	size    int
}

func NewRing(size int) *Ring {
	return &Ring{
		entries: make([]Entry, size),
		size:    size,
	}
}

func (r *Ring) Push(entry Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries[r.head] = entry
	r.head = (r.head + 1) % r.size
	if r.count < r.size {
		r.count++
	}
}

type LogFilter struct {
	Level   Level
	Session string
	User    string
	Event   string
	Limit   int
	Before  string // ISO timestamp: exclude entries at or after this time
	After   string // ISO timestamp: exclude entries before this time
}

type LogQueryResult struct {
	Entries  []Entry `json:"entries"`
	Total    int     `json:"total"`
	Filtered int     `json:"filtered"`
}

func (r *Ring) Query(filter LogFilter) LogQueryResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if filter.Limit <= 0 {
		filter.Limit = 100
	}

	var results []Entry
	minLevel := filter.Level

	// Iterate backwards from head (newest first)
	for i := 0; i < r.count && len(results) < filter.Limit; i++ {
		idx := (r.head - 1 - i + r.size) % r.size
		entry := r.entries[idx]

		// Level filter
		if ParseLevel(entry.Level) < minLevel {
			continue
		}

		// Session filter
		if filter.Session != "" && entry.SessionID != filter.Session {
			continue
		}

		// User filter
		if filter.User != "" && entry.UserID != filter.User {
			continue
		}

		// Event filter
		if filter.Event != "" && entry.Event != filter.Event {
			continue
		}

		// Before filter
		if filter.Before != "" && entry.Timestamp >= filter.Before {
			continue
		}

		// After filter
		if filter.After != "" && entry.Timestamp < filter.After {
			continue
		}

		results = append(results, entry)
	}

	return LogQueryResult{
		Entries:  results,
		Total:    r.count,
		Filtered: len(results),
	}
}
