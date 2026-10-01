package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	bufferSize := flag.Int("buffer", 1024, "events kept per stream for replay")
	heartbeat := flag.Duration("heartbeat", 15*time.Second, "delay between : ping frames")
	retry := flag.Duration("retry", 2*time.Second, "reconnect delay advertised in the retry: field")
	shutdownTimeout := flag.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests")
	logFormat := flag.String("log-format", "text", "request log format: text or json")
	flag.Parse()

	relay := NewRelay(*bufferSize)
	srv := NewServer(relay, *heartbeat, *retry, os.Getenv("RELAY_TOKEN"))

	logger := newLogger(os.Stderr, *logFormat)
	httpServer := &http.Server{
		Addr:    *addr,
		Handler: withLogging(logger, srv.Routes()),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("sse-relay listening on %s", *addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		log.Fatalf("sse-relay: %v", err)
	}

	log.Print("sse-relay shutting down")
	// End every stream first so open subscribers get event: done, then
	// give the HTTP server a chance to drain those responses.
	relay.finishAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("sse-relay: shutdown error: %v", err)
	}
}
