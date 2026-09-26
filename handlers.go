package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// maxBodyBytes bounds a single chunk POST so a misbehaving producer can't
// exhaust memory through one request.
const maxBodyBytes = 1 << 20

var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func validID(id string) bool { return idPattern.MatchString(id) }

// Server wires the relay to HTTP.
type Server struct {
	relay     *Relay
	heartbeat time.Duration
	retry     time.Duration
	token     string // empty disables auth
}

func NewServer(relay *Relay, heartbeat, retry time.Duration, token string) *Server {
	return &Server{relay: relay, heartbeat: heartbeat, retry: retry, token: token}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /streams", s.handleList)
	mux.HandleFunc("POST /streams/{id}", s.auth(s.handlePublish))
	mux.HandleFunc("POST /streams/{id}/done", s.auth(s.handleDone))
	mux.HandleFunc("DELETE /streams/{id}", s.auth(s.handleDelete))
	mux.HandleFunc("GET /streams/{id}/events", s.handleSubscribe)
	mux.HandleFunc("GET /streams/{id}", s.handleStats)
	return mux
}

// auth requires a bearer token on writes when RELAY_TOKEN is set. Reads
// are never gated.
func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	if s.token == "" {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		got := r.Header.Get("Authorization")
		if !strings.HasPrefix(got, prefix) ||
			subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(got, prefix)), []byte(s.token)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	streams := s.relay.list()
	stats := make([]Stats, 0, len(streams))
	for _, st := range streams {
		stats = append(stats, st.Stats())
	}
	sort.Slice(stats, func(i, j int) bool { return stats[i].ID < stats[j].ID })
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "invalid stream id", http.StatusBadRequest)
		return
	}
	stream, ok := s.relay.get(id)
	if !ok {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, stream.Stats())
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "invalid stream id", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	if len(body) > maxBodyBytes {
		http.Error(w, "chunk too large", http.StatusRequestEntityTooLarge)
		return
	}

	var data string
	var done bool
	var publish bool
	if isJSONContentType(r.Header.Get("Content-Type")) {
		var payload struct {
			Data string `json:"data"`
			Done bool   `json:"done"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}
		data, done = payload.Data, payload.Done
		publish = data != ""
	} else {
		data = string(body)
		publish = true
	}

	stream := s.relay.getOrCreate(id)
	if publish {
		if _, ok := stream.Publish(data); !ok {
			http.Error(w, "stream is done", http.StatusConflict)
			return
		}
	}
	if done {
		stream.Finish()
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleDone(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "invalid stream id", http.StatusBadRequest)
		return
	}
	_, _ = io.Copy(io.Discard, r.Body)
	stream, ok := s.relay.get(id)
	if !ok {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}
	stream.Finish()
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "invalid stream id", http.StatusBadRequest)
		return
	}
	if !s.relay.delete(id) {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(id) {
		http.Error(w, "invalid stream id", http.StatusBadRequest)
		return
	}
	stream, ok := s.relay.get(id)
	if !ok {
		http.Error(w, "stream not found", http.StatusNotFound)
		return
	}

	var lastID uint64
	if h := r.Header.Get("Last-Event-ID"); h != "" {
		v, err := strconv.ParseUint(h, 10, 64)
		if err != nil {
			http.Error(w, "invalid Last-Event-ID", http.StatusBadRequest)
			return
		}
		lastID = v
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	result := stream.Subscribe(lastID)

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "retry: %d\n\n", s.retry.Milliseconds())

	if result.gap {
		fmt.Fprintf(w, "event: gap\ndata: %d\n\n", lastID)
	}
	for _, f := range result.replay {
		writeFrame(w, f)
	}
	flusher.Flush()

	if result.done {
		return
	}

	sub := result.sub
	defer stream.Unsubscribe(sub)

	ticker := time.NewTicker(s.heartbeat)
	defer ticker.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil {
				return
			}
			flusher.Flush()
		case f, open := <-sub.ch:
			if !open {
				if sub.lagged {
					fmt.Fprint(w, "event: lagged\ndata: {}\n\n")
					flusher.Flush()
				}
				return
			}
			writeFrame(w, f)
			flusher.Flush()
			if f.kind == frameDone {
				return
			}
		}
	}
}

func writeFrame(w io.Writer, f frame) {
	switch f.kind {
	case frameChunk:
		fmt.Fprintf(w, "id: %d\n", f.id)
		for _, line := range strings.Split(f.data, "\n") {
			fmt.Fprintf(w, "data: %s\n", line)
		}
		fmt.Fprint(w, "\n")
	case frameDone:
		fmt.Fprint(w, "event: done\ndata: {}\n\n")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func isJSONContentType(ct string) bool {
	if ct == "" {
		return false
	}
	mt, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return false
	}
	return mt == "application/json"
}
