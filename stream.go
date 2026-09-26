package main

import (
	"sync"
	"time"
)

// subscriberBuffer is how many frames a subscriber can fall behind before
// it is considered stuck and dropped rather than blocking the producer.
const subscriberBuffer = 32

type frameKind int

const (
	frameChunk frameKind = iota
	frameDone
)

type frame struct {
	kind frameKind
	id   uint64
	data string
}

type subscriber struct {
	ch chan frame
	// lagged is only ever written under the owning stream's mutex, in the
	// same goroutine that then closes ch. A receiver observing ch closed
	// is guaranteed by Go's memory model to see that write, so it is read
	// lock-free after the channel is drained.
	lagged bool
}

// Stream is one producer's append-only chunk sequence plus its live
// subscribers. All fields are guarded by mu.
type Stream struct {
	mu          sync.Mutex
	id          string
	ring        *ring
	nextID      uint64
	done        bool
	createdAt   time.Time
	updatedAt   time.Time
	subscribers map[*subscriber]struct{}
}

func newStream(id string, bufferSize int) *Stream {
	now := time.Now()
	return &Stream{
		id:          id,
		ring:        newRing(bufferSize),
		createdAt:   now,
		updatedAt:   now,
		subscribers: make(map[*subscriber]struct{}),
	}
}

// Publish appends data as one chunk and fans it out to every live
// subscriber. It reports false without publishing if the stream already
// finished.
func (s *Stream) Publish(data string) (id uint64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return 0, false
	}
	s.nextID++
	id = s.nextID
	s.ring.push(chunk{id: id, data: data})
	s.updatedAt = time.Now()
	s.broadcast(frame{kind: frameChunk, id: id, data: data})
	return id, true
}

// Finish marks the stream done and delivers a done frame to every live
// subscriber, closing their channels. It reports false if the stream was
// already finished.
func (s *Stream) Finish() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return false
	}
	s.done = true
	s.updatedAt = time.Now()
	s.broadcast(frame{kind: frameDone})
	for sub := range s.subscribers {
		close(sub.ch)
	}
	s.subscribers = make(map[*subscriber]struct{})
	return true
}

// broadcast must be called with mu held. A subscriber that cannot take the
// frame immediately is dropped instead of blocking the producer.
func (s *Stream) broadcast(f frame) {
	for sub := range s.subscribers {
		select {
		case sub.ch <- f:
		default:
			sub.lagged = true
			close(sub.ch)
			delete(s.subscribers, sub)
		}
	}
}

// subscribeResult carries everything a new SSE connection needs: the
// backlog it missed, whether that backlog has a genuine gap in it, and a
// live subscription if the stream has not finished yet.
type subscribeResult struct {
	replay []frame
	gap    bool
	done   bool
	sub    *subscriber
}

// Subscribe replays whatever is still buffered after lastID (0 means
// replay everything held) and, if the stream is still live, registers a
// new subscription for the tail.
func (s *Stream) Subscribe(lastID uint64) subscribeResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res subscribeResult
	if lastID > 0 {
		if oldest, _, ok := s.ring.bounds(); ok && lastID < oldest-1 {
			res.gap = true
		}
	}
	for _, c := range s.ring.since(lastID) {
		res.replay = append(res.replay, frame{kind: frameChunk, id: c.id, data: c.data})
	}
	if s.done {
		res.replay = append(res.replay, frame{kind: frameDone})
		res.done = true
		return res
	}
	sub := &subscriber{ch: make(chan frame, subscriberBuffer)}
	s.subscribers[sub] = struct{}{}
	res.sub = sub
	return res
}

// Unsubscribe detaches a subscriber that is walking away on its own (the
// client disconnected). It is a no-op if the subscriber was already
// removed, whether by lagging or by Finish.
func (s *Stream) Unsubscribe(sub *subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscribers[sub]; ok {
		delete(s.subscribers, sub)
		close(sub.ch)
	}
}

// Stats is the JSON-serialisable snapshot returned by the stats endpoints.
type Stats struct {
	ID          string    `json:"id"`
	Events      uint64    `json:"events"`
	Buffered    int       `json:"buffered"`
	Subscribers int       `json:"subscribers"`
	Done        bool      `json:"done"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (s *Stream) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{
		ID:          s.id,
		Events:      s.nextID,
		Buffered:    s.ring.count,
		Subscribers: len(s.subscribers),
		Done:        s.done,
		CreatedAt:   s.createdAt,
		UpdatedAt:   s.updatedAt,
	}
}
