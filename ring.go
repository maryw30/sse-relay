package main

type chunk struct {
	id   uint64
	data string
}

// ring is a fixed-capacity circular buffer of chunks. Once it fills up,
// pushing a new chunk overwrites the oldest one still held.
type ring struct {
	buf   []chunk
	start int
	count int
}

func newRing(capacity int) *ring {
	if capacity < 1 {
		capacity = 1
	}
	return &ring{buf: make([]chunk, capacity)}
}

func (r *ring) push(c chunk) {
	capacity := len(r.buf)
	if r.count < capacity {
		r.buf[(r.start+r.count)%capacity] = c
		r.count++
		return
	}
	r.buf[r.start] = c
	r.start = (r.start + 1) % capacity
}

// bounds reports the oldest and newest ids currently held. ok is false if
// nothing has been buffered yet.
func (r *ring) bounds() (oldest, newest uint64, ok bool) {
	if r.count == 0 {
		return 0, 0, false
	}
	oldest = r.buf[r.start].id
	newest = r.buf[(r.start+r.count-1)%len(r.buf)].id
	return oldest, newest, true
}

// since returns every buffered chunk with an id greater than lastID, in
// order. Passing 0 returns everything still buffered.
func (r *ring) since(lastID uint64) []chunk {
	if r.count == 0 {
		return nil
	}
	out := make([]chunk, 0, r.count)
	for i := 0; i < r.count; i++ {
		c := r.buf[(r.start+i)%len(r.buf)]
		if c.id > lastID {
			out = append(out, c)
		}
	}
	return out
}
