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
	"strconv"
	"strings"
	"sync"
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
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		return err
	}

	storage, cleanup, err := newStorage(cfg.storageKind, cfg.dataDir, cfg.seglog)
	if err != nil {
		return err
	}
	defer cleanup()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	var workers sync.WaitGroup
	defer func() {
		stop()
		workers.Wait()
	}()
	if cfg.debugStatsInterval > 0 {
		if seglogStorage, ok := storage.(*seglog.Storage); ok {
			workers.Go(func() { runSeglogStats(ctx, seglogStorage, cfg.debugStatsInterval) })
		}
	}

	// The conformance server is intentionally accessible to browser-based test
	// clients. Production deployments should choose their own origin policy.
	handler := durablestream.NewHandler(storage, &durablestream.HandlerConfig{EnableCORS: true})

	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))

	addr := fmt.Sprintf("%s:%d", cfg.host, cfg.port)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
	}

	errCh := make(chan error, 1)
	workers.Go(func() {
		log.Printf("Durable Streams test server listening on http://%s", addr)
		log.Printf("Stream URLs: http://%s/v1/stream/{stream-id}", addr)
		errCh <- server.ListenAndServe()
	})

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

type serverConfig struct {
	host               string
	port               int
	storageKind        string
	dataDir            string
	seglog             seglog.Options
	debugStatsInterval time.Duration
}

func parseFlags(args []string) (serverConfig, error) {
	cfg := serverConfig{seglog: seglog.Options{
		Partitions:          seglog.DefaultPartitions,
		WALSegmentBytes:     seglog.DefaultWALSegmentBytes,
		StreamSegmentBytes:  seglog.DefaultStreamSegmentBytes,
		MaterializeInterval: seglog.DefaultMaterializeInterval,
		CheckpointInterval:  seglog.DefaultCheckpointInterval,
	}}
	fs := flag.NewFlagSet("testserver", flag.ContinueOnError)
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "host to listen on")
	fs.IntVar(&cfg.port, "port", 4437, "port to listen on")
	fs.StringVar(&cfg.storageKind, "storage", "memory", "storage backend: memory, badger, or seglog")
	fs.StringVar(&cfg.dataDir, "data-dir", "", "data directory for disk-backed storage (default: a temp dir removed on exit)")
	fs.IntVar(&cfg.seglog.Partitions, "seglog-partitions", cfg.seglog.Partitions, "seglog WAL partitions")
	fs.Var(byteSizeValue{target: &cfg.seglog.WALSegmentBytes}, "seglog-wal-segment-bytes", "seglog logical WAL segment size (default 256MiB)")
	fs.Var(byteSizeValue{target: &cfg.seglog.StreamSegmentBytes}, "seglog-stream-segment-bytes", "seglog stream segment size (default 128MiB)")
	fs.DurationVar(&cfg.seglog.MaterializeInterval, "seglog-materialize-interval", cfg.seglog.MaterializeInterval, "seglog materialization interval")
	fs.DurationVar(&cfg.seglog.CheckpointInterval, "seglog-checkpoint-interval", cfg.seglog.CheckpointInterval, "seglog checkpoint interval")
	fs.DurationVar(&cfg.debugStatsInterval, "debug-stats-interval", 0, "interval for seglog delta statistics (default off)")
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}
	if cfg.debugStatsInterval < 0 {
		return serverConfig{}, fmt.Errorf("-debug-stats-interval must not be negative")
	}
	return cfg, nil
}

func runSeglogStats(ctx context.Context, storage *seglog.Storage, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	previous := storage.Stats()
	lastTick := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			current := storage.Stats()
			logSeglogStats(current, previous, now.Sub(lastTick))
			previous = current
			lastTick = now
		}
	}
}

func logSeglogStats(current, previous seglog.Stats, elapsed time.Duration) {
	delta := current.PartitionStats
	commitWaves := current.CommitWaves - previous.CommitWaves
	delta.GroupsCommitted -= previous.GroupsCommitted
	delta.OpsCommitted -= previous.OpsCommitted
	delta.WALBytesWritten -= previous.WALBytesWritten
	delta.CommitFdatasyncNanos -= previous.CommitFdatasyncNanos
	delta.CommitterIdleNanos -= previous.CommitterIdleNanos
	delta.MaterializerSyncs -= previous.MaterializerSyncs
	delta.SyncfsCalls -= previous.SyncfsCalls
	delta.CheckpointRounds -= previous.CheckpointRounds
	for i := range delta.GroupSizeHist {
		delta.GroupSizeHist[i] -= previous.GroupSizeHist[i]
	}
	seconds := elapsed.Seconds()
	var opsPerGroup, opsPerWave, fsyncMeanMillis, idlePercent float64
	if delta.GroupsCommitted > 0 {
		opsPerGroup = float64(delta.OpsCommitted) / float64(delta.GroupsCommitted)
		fsyncMeanMillis = float64(delta.CommitFdatasyncNanos) / float64(delta.GroupsCommitted) / float64(time.Millisecond)
	}
	if commitWaves > 0 {
		opsPerWave = float64(delta.OpsCommitted) / float64(commitWaves)
	}
	if partitions := len(current.PerPartition); partitions > 0 {
		idlePercent = float64(delta.CommitterIdleNanos) / float64(elapsed.Nanoseconds()*int64(partitions)) * 100
		// A receive that spans ticks is charged when it completes, so its raw
		// delta can briefly exceed one interval.
		idlePercent = min(idlePercent, 100)
	}
	log.Printf("seglog-stats: ops=%.0f/s groups=%.0f/s waves=%.0f/s ops/group=%.1f ops/wave=%.1f wal_bytes=%.0f/s fsync_mean=%.1fms idle=%.0f%% hist=%v mat_syncs=%.0f/s syncfs=%.0f/s checkpoints=%.0f/s",
		float64(delta.OpsCommitted)/seconds, float64(delta.GroupsCommitted)/seconds, float64(commitWaves)/seconds, opsPerGroup, opsPerWave,
		float64(delta.WALBytesWritten)/seconds, fsyncMeanMillis, idlePercent, delta.GroupSizeHist,
		float64(delta.MaterializerSyncs)/seconds, float64(delta.SyncfsCalls)/seconds, float64(delta.CheckpointRounds)/seconds)
}

type byteSizeValue struct{ target *int64 }

func (v byteSizeValue) String() string { return formatByteSize(*v.target) }

func (v byteSizeValue) Set(raw string) error {
	n, err := parseByteSize(raw)
	if err != nil {
		return err
	}
	*v.target = n
	return nil
}

func parseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	multiplier := int64(1)
	for _, suffix := range []struct {
		name string
		mult int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1_000_000_000}, {"MB", 1_000_000}, {"KB", 1_000}} {
		if strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix.name)) {
			multiplier = suffix.mult
			s = strings.TrimSpace(s[:len(s)-len(suffix.name)])
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || (multiplier != 0 && n > int64(^uint64(0)>>1)/multiplier) {
		return 0, fmt.Errorf("invalid byte size %q", raw)
	}
	return n * multiplier, nil
}

func formatByteSize(n int64) string {
	for _, unit := range []struct {
		suffix string
		bytes  int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}} {
		if n%unit.bytes == 0 && n >= unit.bytes {
			return fmt.Sprintf("%d%s", n/unit.bytes, unit.suffix)
		}
	}
	return strconv.FormatInt(n, 10)
}

// newStorage builds the requested storage backend. The returned cleanup
// function closes the storage and removes any temporary data directory.
func newStorage(kind, dataDir string, seglogOpts seglog.Options) (durablestream.Storage, func(), error) {
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
		seglogOpts.Dir = dir
		s, err := seglog.New(seglogOpts)
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
