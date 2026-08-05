package badgerstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// cleanupScanBatchSize bounds both the config records held by one read
// transaction and the expired candidates retained in memory before deletion.
const cleanupScanBatchSize = 256

// expiredGeneration identifies the exact stream incarnation observed as
// expired during a cleanup scan. Cleanup must not act on the stream ID alone:
// the expired incarnation can be replaced before the deletion transaction.
type expiredGeneration struct {
	streamID string
	gen      generation
}

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

		err := s.withDB(func() error { return s.db.RunValueLogGC(0.5) })
		if errors.Is(err, durablestream.ErrClosed) {
			return
		}
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
	seek := []byte(prefixConfig)
	deleted := 0

	for {
		expiredStreams, next, err := s.scanExpiredStreams(ctx, seek)
		if err != nil {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, durablestream.ErrClosed) {
				s.logger.Warn("badgerstore: cleanup scan failed", "error", err)
			}
			return
		}

		for i, expired := range expiredStreams {
			removed, err := s.deleteExpiredGeneration(ctx, expired)
			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, durablestream.ErrClosed) {
					s.logger.Info("badgerstore: cleanup interrupted by shutdown",
						"deleted", deleted, "remainingInBatch", len(expiredStreams)-i)
					return
				}
				s.logger.Warn("badgerstore: failed to delete expired stream",
					"streamID", expired.streamID, "error", err)
				// Continue with other streams.
			} else if removed {
				deleted++
			}
		}

		if next == nil {
			break
		}
		seek = next
	}

	if deleted > 0 {
		s.logger.Debug("badgerstore: cleanup completed", "deleted", deleted)
	}
}

// scanExpiredStreams examines one bounded page of stream configuration records.
// next is the first unexamined key, or nil after the end of the config keyspace.
// A generation is revalidated inside its deletion transaction, so it is safe for
// Touch, Delete, or Create to change a candidate after this scan returns.
func (s *Storage) scanExpiredStreams(ctx context.Context, seek []byte) ([]expiredGeneration, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var expiredStreams []expiredGeneration
	var next []byte
	prefix := []byte(prefixConfig)

	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = true
		opts.PrefetchSize = cleanupScanBatchSize

		it := txn.NewIterator(opts)
		defer it.Close()

		examined := 0
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			if examined == cleanupScanBatchSize {
				next = it.Item().KeyCopy(nil)
				break
			}
			examined++

			if err := ctx.Err(); err != nil {
				return err
			}

			item := it.Item()
			streamID := string(item.Key()[len(prefixConfig):])

			err := item.Value(func(val []byte) error {
				var rec streamRecord
				if err := json.Unmarshal(val, &rec); err != nil {
					s.logger.Warn("badgerstore: failed to unmarshal stream config during cleanup",
						"streamID", streamID, "error", err)
					return nil // Skip malformed, continue iteration
				}
				// New rejects legacy records before starting this loop. If one is
				// nevertheless observed (for example after external corruption), leave
				// it intact rather than turning a format problem into data loss.
				if rec.isLegacy() {
					s.logger.Warn("badgerstore: refusing to clean up legacy stream record",
						"streamID", streamID, "error", ErrLegacyFormat)
					return nil
				}
				if rec.IsExpired() && !rec.SoftDeleted {
					expiredStreams = append(expiredStreams, expiredGeneration{
						streamID: streamID,
						gen:      rec.Gen,
					})
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
	return expiredStreams, next, err
}

// deleteExpiredGeneration removes candidate only if the same generation is
// still present and expired when the write transaction commits. A concurrent
// Touch may renew the generation after the scan, and Create may replace it;
// neither operation may be undone by delayed cleanup.
func (s *Storage) deleteExpiredGeneration(ctx context.Context, candidate expiredGeneration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	var (
		removed bool
		changes topologyChanges
	)
	err := s.updateWithRetry(func(txn *badger.Txn) error {
		removed, changes = false, topologyChanges{}
		if err := ctx.Err(); err != nil {
			return err
		}

		rec, found, err := getRecord(txn, candidate.streamID)
		if err != nil {
			return err
		}
		if !found || rec.isLegacy() || rec.Gen != candidate.gen || !rec.IsExpired() || rec.SoftDeleted {
			return nil
		}
		if rec.RefCount > 0 {
			rec.SoftDeleted = true
			if err := setRecord(txn, candidate.streamID, rec); err != nil {
				return err
			}
			if err := txn.Delete(lastSeqKey(candidate.streamID)); err != nil {
				return err
			}
			changes.softened = append(changes.softened, streamGeneration{streamID: candidate.streamID, gen: rec.Gen})
		} else {
			var err error
			changes, err = removeRecordCascade(txn, candidate.streamID, rec)
			if err != nil {
				return err
			}
		}
		removed = true
		return nil
	})
	if err != nil || !removed {
		return removed, err
	}

	s.publishTopologyChanges(changes)
	return true, nil
}
