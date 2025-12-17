package badgerstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// cleanupBatchSize is the number of streams to delete before checking for shutdown.
// This ensures cleanup can be interrupted in a reasonable time.
const cleanupBatchSize = 100

// runGCLoop runs Badger's value log garbage collection periodically.
func (s *Storage) runGCLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case <-ticker.C:
			s.runGC()
		}
	}
}

// runGC performs one round of garbage collection with proper shutdown checks.
func (s *Storage) runGC() {
	// Run GC multiple times until no more garbage to collect.
	// Per Badger docs: one call only removes at most one log file.
	// Under heavy load, we need to loop to keep up with write rate.
	// Limit iterations to prevent blocking too long.
	const maxGCIterations = 10
	for i := 0; i < maxGCIterations; i++ {
		// Check for shutdown between iterations
		select {
		case <-s.shutdownCtx.Done():
			return
		default:
		}

		err := s.db.RunValueLogGC(0.5)
		if err == badger.ErrNoRewrite {
			// No more garbage to collect - this is expected
			return
		}
		if err != nil {
			// Unexpected error - log it and stop this round
			s.logger.Warn("badgerstore: GC error", "error", err)
			return
		}
		// nil error = successfully GC'd a file, try again
	}
}

// runCleanupLoop scans for and deletes expired streams periodically.
func (s *Storage) runCleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case <-ticker.C:
			s.cleanupExpiredStreams(s.shutdownCtx)
		}
	}
}

// cleanupExpiredStreams finds and deletes all expired streams.
// It respects the provided context for cancellation during shutdown.
// Called synchronously from runCleanupLoop - only one cleanup runs at a time.
func (s *Storage) cleanupExpiredStreams(ctx context.Context) {
	// Check for cancellation before starting
	if ctx.Err() != nil {
		return
	}

	var expiredStreams []string

	// Find expired streams
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefixConfig)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek([]byte(prefixConfig)); it.ValidForPrefix([]byte(prefixConfig)); it.Next() {
			// Check for cancellation periodically during iteration
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			streamID := string(item.Key()[len(prefixConfig):])

			err := item.Value(func(val []byte) error {
				var cfg durablestream.StreamConfig
				if err := json.Unmarshal(val, &cfg); err != nil {
					s.logger.Warn("badgerstore: failed to unmarshal stream config during cleanup",
						"streamID", streamID, "error", err)
					return nil // Skip malformed, continue iteration
				}
				if isExpired(cfg) {
					expiredStreams = append(expiredStreams, streamID)
				}
				return nil
			})
			if err != nil {
				s.logger.Warn("badgerstore: failed to read stream config during cleanup",
					"streamID", streamID, "error", err)
			}
		}
		return nil
	})
	if err != nil && err != context.Canceled {
		s.logger.Warn("badgerstore: cleanup scan failed", "error", err)
		return
	}

	// Delete expired streams in batches, checking for cancellation between batches
	deleted := 0
	for _, streamID := range expiredStreams {
		// Check for cancellation every batch
		if deleted > 0 && deleted%cleanupBatchSize == 0 {
			select {
			case <-ctx.Done():
				s.logger.Info("badgerstore: cleanup interrupted by shutdown",
					"deleted", deleted, "remaining", len(expiredStreams)-deleted)
				return
			default:
			}
		}

		if err := s.Delete(ctx, streamID); err != nil {
			if err == context.Canceled || err == ErrClosed {
				s.logger.Info("badgerstore: cleanup interrupted by shutdown",
					"deleted", deleted, "remaining", len(expiredStreams)-deleted)
				return
			}
			s.logger.Warn("badgerstore: failed to delete expired stream",
				"streamID", streamID, "error", err)
			// Continue with other streams
		} else {
			deleted++
		}
	}

	if deleted > 0 {
		s.logger.Debug("badgerstore: cleanup completed", "deleted", deleted)
	}
}
