package main

import "sync"

// Relay owns every active stream, keyed by id.
type Relay struct {
	mu         sync.Mutex
	streams    map[string]*Stream
	bufferSize int
}

func NewRelay(bufferSize int) *Relay {
	return &Relay{streams: make(map[string]*Stream), bufferSize: bufferSize}
}

func (r *Relay) getOrCreate(id string) *Stream {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	if !ok {
		s = newStream(id, r.bufferSize)
		r.streams[id] = s
	}
	return s
}

func (r *Relay) get(id string) (*Stream, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.streams[id]
	return s, ok
}

// delete removes id from the relay entirely, ending it first if it was
// still live so any open subscribers see event: done.
func (r *Relay) delete(id string) bool {
	r.mu.Lock()
	s, ok := r.streams[id]
	delete(r.streams, id)
	r.mu.Unlock()
	if ok {
		s.Finish()
	}
	return ok
}

func (r *Relay) list() []*Stream {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Stream, 0, len(r.streams))
	for _, s := range r.streams {
		out = append(out, s)
	}
	return out
}

// finishAll ends every stream so their subscribers see event: done. Used
// during graceful shutdown, before the HTTP server itself is drained.
func (r *Relay) finishAll() {
	r.mu.Lock()
	streams := make([]*Stream, 0, len(r.streams))
	for _, s := range r.streams {
		streams = append(streams, s)
	}
	r.mu.Unlock()
	for _, s := range streams {
		s.Finish()
	}
}
