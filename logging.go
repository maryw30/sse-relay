package main

import (
	"io"
	"log/slog"
	"net/http"
	"time"
)

// statusRecorder remembers what the handler wrote so the log line can report
// it. It forwards Flush because the subscribe handler type-asserts for
// http.Flusher, and Unwrap so http.ResponseController still reaches the
// underlying writer.
type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *statusRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// withLogging writes one line per request, after it finishes. For SSE
// subscriptions that means the line appears when the client disconnects, and
// duration is how long the subscription lasted. The Authorization header is
// deliberately never logged.
func withLogging(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		}
		logger.LogAttrs(r.Context(), level, "request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Int64("bytes", rec.bytes),
			slog.Duration("duration", time.Since(start)),
			slog.String("remote", r.RemoteAddr),
		)
	})
}

// newLogger builds the process logger. "json" suits log shippers, anything
// else falls back to key=value text.
func newLogger(w io.Writer, format string) *slog.Logger {
	if format == "json" {
		return slog.New(slog.NewJSONHandler(w, nil))
	}
	return slog.New(slog.NewTextHandler(w, nil))
}
