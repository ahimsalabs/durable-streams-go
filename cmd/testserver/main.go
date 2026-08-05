// Command testserver runs a Durable Streams server for testing.
//
// Usage:
//
//	go run ./cmd/testserver
//	go run ./cmd/testserver -port 8080
//	go run ./cmd/testserver -storage badger
//	go run ./cmd/testserver -storage seglog
//	go run ./cmd/testserver -storage badger -data-dir /var/lib/ds
//
// Default port is 4437 per PROTOCOL.md Section 13.1.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/badgerstore"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/memorystorage"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/seglog"
)

// Server timeouts. WriteTimeout is deliberately unset: SSE and long-poll
// responses are long-lived and a write deadline would sever them.
const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 60 * time.Second
	idleTimeout       = 120 * time.Second
	shutdownTimeout   = 10 * time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	port := flag.Int("port", 4437, "port to listen on")
	storageKind := flag.String("storage", "memory", "storage backend: memory, badger, or seglog")
	dataDir := flag.String("data-dir", "", "data directory for disk-backed storage (default: a temp dir removed on exit)")
	flag.Parse()

	storage, cleanup, err := newStorage(*storageKind, *dataDir)
	if err != nil {
		return err
	}
	defer cleanup()

	// The conformance server is intentionally accessible to browser-based test
	// clients. Production deployments should choose their own origin policy.
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{EnableCORS: true})

	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Printf("Durable Streams test server listening on http://%s", addr)
		log.Printf("Stream URLs: http://%s/v1/stream/{stream-id}", addr)
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("listen: %v", err)
		}
		return nil
	case <-ctx.Done():
	}

	log.Println("Shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("Graceful shutdown failed, forcing close: %v", err)
		if err := server.Close(); err != nil {
			log.Printf("Error closing server: %v", err)
		}
	}
	return nil
}

// newStorage builds the requested storage backend. The returned cleanup
// function closes the storage and removes any temporary data directory.
func newStorage(kind, dataDir string) (durablestream.Storage, func(), error) {
	switch kind {
	case "memory":
		log.Println("Using memory storage")
		s := memorystorage.New()
		return s, func() {
			if err := s.Close(); err != nil {
				log.Printf("Error closing storage: %v", err)
			}
		}, nil

	case "badger":
		dir := dataDir
		removeDir := false
		if dir == "" {
			var err error
			dir, err = os.MkdirTemp("", "ds-testserver-*")
			if err != nil {
				return nil, nil, fmt.Errorf("create badger data dir: %v", err)
			}
			removeDir = true
		}
		s, err := badgerstore.New(badgerstore.Options{Dir: dir})
		if err != nil {
			if removeDir {
				if rmErr := os.RemoveAll(dir); rmErr != nil {
					log.Printf("Error removing data dir %s: %v", dir, rmErr)
				}
			}
			return nil, nil, fmt.Errorf("open badger storage: %v", err)
		}
		log.Printf("Using badger storage at %s", dir)
		return s, func() {
			if err := s.Close(); err != nil {
				log.Printf("Error closing storage: %v", err)
			}
			if removeDir {
				if err := os.RemoveAll(dir); err != nil {
					log.Printf("Error removing data dir %s: %v", dir, err)
				}
			}
		}, nil

	case "seglog":
		dir := dataDir
		removeDir := false
		if dir == "" {
			var err error
			dir, err = os.MkdirTemp("", "ds-testserver-*")
			if err != nil {
				return nil, nil, fmt.Errorf("create seglog data dir: %v", err)
			}
			removeDir = true
		}
		s, err := seglog.New(seglog.Options{Dir: dir})
		if err != nil {
			if removeDir {
				if rmErr := os.RemoveAll(dir); rmErr != nil {
					log.Printf("Error removing data dir %s: %v", dir, rmErr)
				}
			}
			return nil, nil, fmt.Errorf("open seglog storage: %v", err)
		}
		log.Printf("Using seglog storage at %s", dir)
		return s, func() {
			if err := s.Close(); err != nil {
				log.Printf("Error closing storage: %v", err)
			}
			if removeDir {
				if err := os.RemoveAll(dir); err != nil {
					log.Printf("Error removing data dir %s: %v", dir, err)
				}
			}
		}, nil

	default:
		return nil, nil, fmt.Errorf("unknown -storage %q: want memory, badger, or seglog", kind)
	}
}
