package main

import "testing"

func TestStreamPublishAndReplay(t *testing.T) {
	s := newStream("s1", 4)
	for i := 0; i < 3; i++ {
		if _, ok := s.Publish("chunk"); !ok {
			t.Fatalf("publish %d: unexpectedly done", i)
		}
	}

	res := s.Subscribe(0)
	if res.gap {
		t.Fatal("unexpected gap on fresh subscribe")
	}
	if len(res.replay) != 3 {
		t.Fatalf("expected 3 replayed frames, got %d", len(res.replay))
	}
	for i, f := range res.replay {
		if f.id != uint64(i+1) {
			t.Fatalf("frame %d: got id %d", i, f.id)
		}
	}
	if res.sub == nil {
		t.Fatal("expected a live subscription for an unfinished stream")
	}
}

func TestStreamResumeFromLastEventID(t *testing.T) {
	s := newStream("s1", 4)
	for i := 0; i < 3; i++ {
		s.Publish("chunk")
	}

	res := s.Subscribe(2)
	if res.gap {
		t.Fatal("unexpected gap: id 2 is still buffered")
	}
	if len(res.replay) != 1 || res.replay[0].id != 3 {
		t.Fatalf("expected only chunk 3 replayed, got %+v", res.replay)
	}
}

func TestStreamGapWhenBufferWrapped(t *testing.T) {
	s := newStream("s1", 2) // small buffer so it wraps quickly
	for i := 0; i < 5; i++ {
		s.Publish("chunk")
	}
	// ids 1..3 have been evicted by now, only 4 and 5 remain.

	res := s.Subscribe(1)
	if !res.gap {
		t.Fatal("expected a gap: id 1 was evicted long ago")
	}
	if len(res.replay) != 2 || res.replay[0].id != 4 || res.replay[1].id != 5 {
		t.Fatalf("expected replay of remaining chunks 4 and 5, got %+v", res.replay)
	}
}

func TestStreamNoGapWhenCaughtUp(t *testing.T) {
	s := newStream("s1", 2)
	for i := 0; i < 5; i++ {
		s.Publish("chunk")
	}

	res := s.Subscribe(5) // already has everything
	if res.gap {
		t.Fatal("unexpected gap: subscriber is fully caught up")
	}
	if len(res.replay) != 0 {
		t.Fatalf("expected no replay, got %+v", res.replay)
	}
}

func TestStreamPublishAfterDoneRejected(t *testing.T) {
	s := newStream("s1", 4)
	s.Publish("chunk")
	s.Finish()

	if _, ok := s.Publish("late"); ok {
		t.Fatal("expected publish after done to be rejected")
	}
}

func TestStreamSubscribeAfterDoneReplaysAndCloses(t *testing.T) {
	s := newStream("s1", 4)
	s.Publish("chunk")
	s.Finish()

	res := s.Subscribe(0)
	if !res.done {
		t.Fatal("expected subscribe to report the stream as finished")
	}
	if res.sub != nil {
		t.Fatal("a finished stream should not hand back a live subscription")
	}
	if len(res.replay) != 2 || res.replay[0].kind != frameChunk || res.replay[1].kind != frameDone {
		t.Fatalf("expected chunk then done frame, got %+v", res.replay)
	}
}

func TestStreamFinishIsIdempotent(t *testing.T) {
	s := newStream("s1", 4)
	if !s.Finish() {
		t.Fatal("first Finish should report success")
	}
	if s.Finish() {
		t.Fatal("second Finish should be a no-op")
	}
}

func TestStreamLaggedSubscriberIsDropped(t *testing.T) {
	s := newStream("s1", subscriberBuffer*2)
	res := s.Subscribe(0)
	sub := res.sub

	// Publish past the subscriber's channel capacity without ever
	// draining it, forcing an overflow.
	for i := 0; i < subscriberBuffer+1; i++ {
		if _, ok := s.Publish("chunk"); !ok {
			t.Fatal("publish failed")
		}
	}

	closed := false
	for i := 0; i < subscriberBuffer+2; i++ {
		if _, open := <-sub.ch; !open {
			closed = true
			break
		}
	}
	if !closed {
		t.Fatal("expected subscriber channel to be closed after overflow")
	}
	if !sub.lagged {
		t.Fatal("expected subscriber to be marked lagged")
	}
}

func TestStreamUnsubscribeIsSafeAfterFinish(t *testing.T) {
	s := newStream("s1", 4)
	res := s.Subscribe(0)
	s.Finish()

	// Must not panic on a double close.
	s.Unsubscribe(res.sub)
}
