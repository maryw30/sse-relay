package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandlePublishStatsAndDelete(t *testing.T) {
	relay := NewRelay(16)
	srv := NewServer(relay, time.Hour, 2*time.Second, "")
	handler := srv.Routes()

	post := func(body, contentType string) int {
		req := httptest.NewRequest(http.MethodPost, "/streams/chat-1", strings.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post("hello", ""); code != http.StatusAccepted {
		t.Fatalf("expected 202 for raw chunk, got %d", code)
	}

	req := httptest.NewRequest(http.MethodGet, "/streams/chat-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for stats, got %d", rec.Code)
	}
	var stats Stats
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Events != 1 || stats.Done {
		t.Fatalf("unexpected stats after one chunk: %+v", stats)
	}

	if code := post(`{"data":"bye","done":true}`, "application/json"); code != http.StatusAccepted {
		t.Fatalf("expected 202 for json done chunk, got %d", code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/streams/chat-1", nil))
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.Events != 2 || !stats.Done {
		t.Fatalf("expected stream marked done with 2 events, got %+v", stats)
	}

	if code := post("late", ""); code != http.StatusConflict {
		t.Fatalf("expected 409 publishing to a finished stream, got %d", code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/streams/chat-1", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on delete, got %d", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/streams/chat-1", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete, got %d", rec.Code)
	}
}

func TestHandlePublishInvalidID(t *testing.T) {
	relay := NewRelay(16)
	srv := NewServer(relay, time.Hour, 2*time.Second, "")
	handler := srv.Routes()

	req := httptest.NewRequest(http.MethodPost, "/streams/bad!id", strings.NewReader("x"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid id, got %d", rec.Code)
	}
}

func TestHandleSubscribeGapAndDoneReplay(t *testing.T) {
	relay := NewRelay(2)
	srv := NewServer(relay, time.Hour, 2*time.Second, "")
	handler := srv.Routes()

	stream := relay.getOrCreate("chat-1")
	for i := 0; i < 5; i++ {
		stream.Publish("x")
	}
	stream.Finish()

	req := httptest.NewRequest(http.MethodGet, "/streams/chat-1/events", nil)
	req.Header.Set("Last-Event-ID", "1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "event: gap") || !strings.Contains(body, "data: 1") {
		t.Fatalf("expected a gap event referencing id 1, got:\n%s", body)
	}
	if !strings.Contains(body, "id: 4") || !strings.Contains(body, "id: 5") {
		t.Fatalf("expected replay of remaining chunks 4 and 5, got:\n%s", body)
	}
	if !strings.Contains(body, "event: done") {
		t.Fatalf("expected a done event since the stream already finished, got:\n%s", body)
	}
}

func TestHandleSubscribeLiveClosesOnDisconnect(t *testing.T) {
	relay := NewRelay(16)
	srv := NewServer(relay, time.Hour, 2*time.Second, "")
	handler := srv.Routes()
	relay.getOrCreate("chat-1") // live stream, never finished

	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodGet, "/streams/chat-1/events", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after client disconnect")
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestAuthGatesWritesNotReads(t *testing.T) {
	relay := NewRelay(16)
	srv := NewServer(relay, time.Hour, 2*time.Second, "secret")
	handler := srv.Routes()

	post := func(token string) int {
		req := httptest.NewRequest(http.MethodPost, "/streams/chat-1", strings.NewReader("x"))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	if code := post(""); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with no token, got %d", code)
	}
	if code := post("wrong"); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", code)
	}
	if code := post("secret"); code != http.StatusAccepted {
		t.Fatalf("expected 202 with correct token, got %d", code)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/streams", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected reads to stay public, got %d", rec.Code)
	}
}
