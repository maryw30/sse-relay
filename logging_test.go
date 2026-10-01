package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithLoggingRecordsRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "json")

	h := withLogging(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write([]byte("short"))
	}))

	req := httptest.NewRequest(http.MethodPost, "/streams/chat-1", nil)
	req.Header.Set("Authorization", "Bearer hunter2")
	h.ServeHTTP(httptest.NewRecorder(), req)

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("log line is not json: %v\n%s", err, buf.String())
	}
	if line["method"] != "POST" || line["path"] != "/streams/chat-1" {
		t.Fatalf("unexpected method/path: %v", line)
	}
	if line["status"] != float64(http.StatusTeapot) || line["bytes"] != float64(5) {
		t.Fatalf("unexpected status/bytes: %v", line)
	}
	if strings.Contains(buf.String(), "hunter2") {
		t.Fatalf("authorization header leaked into log: %s", buf.String())
	}
}

func TestWithLoggingDefaultsToOK(t *testing.T) {
	var buf bytes.Buffer
	h := withLogging(newLogger(&buf, "text"), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if !strings.Contains(buf.String(), "status=200") {
		t.Fatalf("expected status=200 in %q", buf.String())
	}
}

func TestWithLoggingKeepsFlusher(t *testing.T) {
	var buf bytes.Buffer
	var flushable bool
	h := withLogging(newLogger(&buf, "text"), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, flushable = w.(http.Flusher)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/streams/x/events", nil))
	if !flushable {
		t.Fatal("wrapped writer must still implement http.Flusher for SSE")
	}
}
