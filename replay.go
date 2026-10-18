package gosse

import (
	"fmt"
	"sync"
)

// DefaultReplayCapacity is the number of events kept by the replay store a
// [Broker] creates when [WithReplay] is not used.
const DefaultReplayCapacity = 1000

// StoredEvent is an [Event] together with the ID the broker assigned to it.
type StoredEvent struct {
	ID uint64
	Event
}

// ReplayStore keeps recently published events so that reconnecting clients
// can receive the events they missed.
//
// The broker calls Append for every published event, in increasing ID order
// and without gaps (each ID is one greater than the previous). Implementations
// must be safe for concurrent use.
type ReplayStore interface {
	// Append records ev, published with the given id to the given topics.
	// The store may retain the topics slice.
	Append(id uint64, ev Event, topics []string) error

	// Since returns, in increasing ID order, the stored events with an ID
	// greater than lastID that were published to at least one of topics.
	//
	// found reports whether the store can guarantee that no matching event
	// after lastID is missing, i.e. lastID is known and has not been evicted.
	// When found is false the broker sends no replayed events.
	Since(lastID uint64, topics []string) (events []StoredEvent, found bool, err error)
}

// MemoryReplayStore is a bounded, in-memory [ReplayStore]. It keeps the most
// recently appended events, discarding the oldest when full.
type MemoryReplayStore struct {
	mu    sync.Mutex
	buf   []memEntry // ring buffer
	start int        // index of the oldest entry
	n     int        // number of entries
	last  uint64     // ID of the most recently appended entry
}

type memEntry struct {
	ev     StoredEvent
	topics []string
}

// NewMemoryReplayStore returns a [MemoryReplayStore] holding up to capacity
// events. It panics if capacity is less than 1.
func NewMemoryReplayStore(capacity int) *MemoryReplayStore {
	if capacity < 1 {
		panic("gosse: NewMemoryReplayStore capacity must be at least 1")
	}
	return &MemoryReplayStore{buf: make([]memEntry, capacity)}
}

// Append implements [ReplayStore]. IDs must be strictly increasing.
func (s *MemoryReplayStore) Append(id uint64, ev Event, topics []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n > 0 && id <= s.last {
		return fmt.Errorf("gosse: replay id %d is not greater than previous id %d", id, s.last)
	}
	e := memEntry{ev: StoredEvent{ID: id, Event: ev}, topics: topics}
	if s.n < len(s.buf) {
		s.buf[(s.start+s.n)%len(s.buf)] = e
		s.n++
	} else {
		s.buf[s.start] = e
		s.start = (s.start + 1) % len(s.buf)
	}
	s.last = id
	return nil
}

// Since implements [ReplayStore].
func (s *MemoryReplayStore) Since(lastID uint64, topics []string) ([]StoredEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == 0 || lastID > s.last {
		return nil, false, nil
	}
	if oldest := s.buf[s.start].ev.ID; lastID < oldest-1 {
		return nil, false, nil // the event after lastID has been evicted
	}
	want := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		want[t] = struct{}{}
	}
	var out []StoredEvent
	for i := 0; i < s.n; i++ {
		e := s.buf[(s.start+i)%len(s.buf)]
		if e.ev.ID <= lastID {
			continue
		}
		for _, t := range e.topics {
			if _, ok := want[t]; ok {
				out = append(out, e.ev)
				break
			}
		}
	}
	return out, true, nil
}

// Len returns the number of events currently stored.
func (s *MemoryReplayStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}
