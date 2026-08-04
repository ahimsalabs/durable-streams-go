package badgerstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	// deleteBatchSize is the maximum number of keys to delete in a single
	// transaction. Badger transactions have size limits; batching prevents
	// failures on large streams.
	deleteBatchSize = 10000

	// scanBatchSize bounds how many keys one orphan-sweep scan examines before
	// committing its deletions, keeping the candidate list bounded.
	scanBatchSize = 10000

	// tombstoneBatchSize bounds how many tombstones are collected per scan.
	tombstoneBatchSize = 256
)

// liveGen is a stream's currently claimed generation. known is false when the
// stream's record could not be read, in which case its data must be left alone
// rather than assumed orphaned.
type liveGen struct {
	gen   generation
	known bool
}

// liveGeneration returns the generation currently claiming a stream ID.
// A missing record yields the empty generation, which no live stream uses.
func liveGeneration(txn *badger.Txn, streamID string) (gen generation, known bool) {
	rec, found, err := getRecord(txn, streamID)
	if err != nil {
		return "", false
	}
	if !found {
		return "", true
	}
	return rec.Gen, true
}

// signalReaper asks the reaper to purge tombstoned data now. The send is
// non-blocking: the reaper drains every outstanding tombstone on each run, so a
// coalesced signal loses no work.
func (s *Storage) signalReaper() {
	select {
	case s.reapCh <- struct{}{}:
	default:
	}
}

// runReaperLoop purges data belonging to deleted stream generations.
//
// Purging is deliberately decoupled from Delete: Delete only has to commit the
// stream's removal, and this loop finishes the work, resuming after a crash or
// an interrupted shutdown.
func (s *Storage) runReaperLoop(interval time.Duration) {
	// Sweep first: a previous process may have died mid-purge, leaving
	// orphaned message data behind.
	s.reap(s.shutdownCtx, true)
	close(s.initialReapDone)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdownCtx.Done():
			return
		case <-s.reapCh:
			s.reap(s.shutdownCtx, false)
		case <-ticker.C:
			s.reap(s.shutdownCtx, true)
		}
	}
}

// reap purges every tombstoned generation. When full is set it additionally
// sweeps orphaned data: keys whose stream no longer exists, or belongs to a
// generation that has been replaced, without a tombstone to point at them.
func (s *Storage) reap(ctx context.Context, full bool) {
	if err := s.reapTombstones(ctx); err != nil {
		s.logReapError("purge tombstoned generations", err)
		return
	}
	if !full {
		return
	}
	if err := s.reapOrphans(ctx); err != nil {
		s.logReapError("sweep orphaned data", err)
	}
}

func (s *Storage) logReapError(op string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, badger.ErrDBClosed) {
		return // Shutdown, not a failure
	}
	s.logger.Warn("badgerstore: reaper failed", "op", op, "error", err)
}

// reapTombstones purges the data of every generation with a tombstone, then
// removes the tombstone. Each generation is purged independently: an
// interruption or a single failing stream leaves the remaining tombstones for
// the next run rather than stalling the sweep.
func (s *Storage) reapTombstones(ctx context.Context) error {
	type pending struct {
		streamID string
		gen      generation
		key      []byte
	}

	prefix := []byte(prefixTombstone)
	seek := prefix
	var firstErr error

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var batch []pending
		err := s.view(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(seek); it.ValidForPrefix(prefix) && len(batch) < tombstoneBatchSize; it.Next() {
				key := it.Item().KeyCopy(nil)
				// A malformed tombstone identifies no data to purge; it is
				// dropped with streamID empty.
				streamID, gen, _ := splitScopedKey(prefixTombstone, key, 2)
				batch = append(batch, pending{streamID: streamID, gen: gen, key: key})
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("badgerstore: scan tombstones: %w", err)
		}
		if len(batch) == 0 {
			return firstErr
		}

		for _, p := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if p.streamID != "" {
				if err := s.purgeGeneration(ctx, p.streamID, p.gen); err != nil {
					if ctx.Err() != nil {
						return err
					}
					// Keep the tombstone so the purge is retried, and move on.
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
			}
			if err := s.deleteKeys(ctx, [][]byte{p.key}); err != nil {
				return fmt.Errorf("badgerstore: delete tombstone: %w", err)
			}
		}

		// Advance past the batch: removals do not disturb the cursor, and
		// tombstones left behind by a failed purge are skipped this round.
		seek = append(batch[len(batch)-1].key, 0)
	}
}

// purgeGeneration deletes all messages and the offset sequence belonging to one
// generation of a stream. It is safe to run concurrently with any operation on
// the same stream ID: a later incarnation has a different generation and
// therefore disjoint keys.
func (s *Storage) purgeGeneration(ctx context.Context, streamID string, gen generation) error {
	// Defensive: never purge the live generation, and never purge a stream
	// whose record cannot be read (its state is unknown).
	var live, known bool
	if err := s.view(func(txn *badger.Txn) error {
		liveGen, ok := liveGeneration(txn, streamID)
		live, known = liveGen == gen, ok
		return nil
	}); err != nil {
		return fmt.Errorf("badgerstore: check generation before purge: %w", err)
	}
	if !known {
		return fmt.Errorf("badgerstore: cannot read config for %q; skipping purge", streamID)
	}
	if live {
		s.logger.Warn("badgerstore: refusing to purge live generation",
			"streamID", streamID)
		return nil
	}

	if err := s.deletePrefix(ctx, messagePrefix(streamID, gen)); err != nil {
		return fmt.Errorf("badgerstore: purge messages for %q: %w", streamID, err)
	}
	if err := s.deleteKeys(ctx, [][]byte{seqKey(streamID, gen)}); err != nil {
		return fmt.Errorf("badgerstore: purge sequence for %q: %w", streamID, err)
	}
	return nil
}

// reapOrphans deletes generation-scoped data that no live stream refers to.
// This covers purges interrupted by a crash and data written by versions that
// predate generation scoping.
//
// Deletion is safe against concurrent stream creation because a generation's
// configuration is committed before any of its data is written, and every
// deletion transaction re-reads the configuration before removing a key.
func (s *Storage) reapOrphans(ctx context.Context) error {
	if err := s.sweepPrefix(ctx, prefixMessage, 3); err != nil {
		return err
	}
	return s.sweepPrefix(ctx, prefixSeq, 2)
}

// sweepPrefix scans one generation-scoped key space and deletes entries that no
// live stream generation claims. segments is the number of ':'-separated
// segments following the prefix (see splitScopedKey).
func (s *Storage) sweepPrefix(ctx context.Context, prefix string, segments int) error {
	seek := []byte(prefix)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		var candidates [][]byte
		var next []byte
		// Generations observed in this bounded scan batch. Keeping this cache per
		// batch is important: a full startup sweep may span millions of streams,
		// while the authoritative deletion transaction re-reads every candidate
		// and does not rely on cache entries from an earlier batch.
		seen := make(map[string]liveGen)
		err := s.view(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = []byte(prefix)
			opts.PrefetchValues = false

			it := txn.NewIterator(opts)
			defer it.Close()

			examined := 0
			for it.Seek(seek); it.ValidForPrefix(opts.Prefix); it.Next() {
				if examined == scanBatchSize {
					// Resume from here on the next round.
					next = it.Item().KeyCopy(nil)
					return nil
				}
				examined++

				key := it.Item().KeyCopy(nil)
				streamID, gen, ok := splitScopedKey(prefix, key, segments)
				if !ok {
					candidates = append(candidates, key)
					continue
				}
				cur, cached := seen[streamID]
				if !cached {
					cur.gen, cur.known = liveGeneration(txn, streamID)
					seen[streamID] = cur
				}
				if cur.known && cur.gen != gen {
					candidates = append(candidates, key)
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("badgerstore: scan %q: %w", prefix, err)
		}

		if err := s.deleteOrphanKeys(ctx, prefix, segments, candidates); err != nil {
			return err
		}
		if next == nil {
			return nil
		}
		seek = next
	}
}

// deleteOrphanKeys deletes candidate keys after re-checking, inside the write
// transaction, that no live stream claims their generation. The re-check closes
// the race with a stream created after the scan observed its keys.
func (s *Storage) deleteOrphanKeys(ctx context.Context, prefix string, segments int, keys [][]byte) error {
	for start := 0; start < len(keys); start += deleteBatchSize {
		end := min(start+deleteBatchSize, len(keys))
		batch := keys[start:end]

		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.update(func(txn *badger.Txn) error {
			verified := make(map[string]liveGen)
			for _, key := range batch {
				streamID, gen, ok := splitScopedKey(prefix, key, segments)
				if ok {
					cur, cached := verified[streamID]
					if !cached {
						cur.gen, cur.known = liveGeneration(txn, streamID)
						verified[streamID] = cur
					}
					if !cur.known || cur.gen == gen {
						continue // Unreadable, or became live between scan and delete
					}
				}
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("badgerstore: delete orphaned keys: %w", err)
		}
	}
	return nil
}

// deletePrefix deletes every key with the given prefix, in bounded batches.
func (s *Storage) deletePrefix(ctx context.Context, prefix []byte) error {
	for {
		deleted, err := s.deleteKeyBatch(ctx, prefix, deleteBatchSize)
		if err != nil {
			return err
		}
		if deleted == 0 {
			return nil
		}
	}
}

// deleteKeyBatch deletes up to limit keys with the given prefix.
// Returns the number of keys deleted.
func (s *Storage) deleteKeyBatch(ctx context.Context, prefix []byte, limit int) (int, error) {
	var keysToDelete [][]byte

	// Collect keys in a read transaction
	err := s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keysToDelete) < limit; it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			keysToDelete = append(keysToDelete, it.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	if err := s.deleteKeys(ctx, keysToDelete); err != nil {
		return 0, err
	}
	return len(keysToDelete), nil
}

// deleteKeys removes the given keys in a single transaction.
func (s *Storage) deleteKeys(ctx context.Context, keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return s.update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}
