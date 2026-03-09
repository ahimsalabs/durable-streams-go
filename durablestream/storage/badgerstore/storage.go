// Package badgerstore provides a Badger-backed implementation of durablestream.Storage.
//
// This implementation is optimized for development and testing workloads.
// For production use, consider the following limitations:
//
//   - Expired streams are cleaned up by a background goroutine (configurable interval)
//   - Badger's value log GC is not run automatically; call RunGC() periodically for
//     long-running processes or set GCInterval in Options
//   - Message size is limited by MaxMessageSize (default 10MB)
//   - Single-process only: Badger uses file locking, but no additional fencing is performed
//
// # Stream-Seq Ordering
//
// The Stream-Seq header (for deduplication) uses byte-wise lexicographic ordering
// per the protocol spec. Clients MUST ensure their sequence values sort correctly:
//
//   - Zero-padded numbers: "0001", "0002", "0010" (correct)
//   - Unpadded numbers: "1", "2", "10" (WRONG: "10" < "2" lexicographically)
//   - ULIDs/UUIDs: naturally lexicographically sortable
//   - Timestamps: ISO8601 format sorts correctly
//
// Badger is an LSM-tree based key-value store optimized for append-only workloads,
// making it well-suited for durable streams.
package badgerstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
	"github.com/go4org/hashtriemap"
)

// Key prefixes for different data types within a stream.
const (
	prefixConfig  = "c:" // c:{streamID} -> JSON-encoded StreamConfig
	prefixLastSeq = "q:" // q:{streamID} -> last sequence number (for dedup)
	prefixMessage = "m:" // m:{streamID}:{offset} -> message data
	prefixSeq     = "s:" // s:{streamID} -> Badger sequence for atomic offset generation
)

// streamState holds in-memory notification state for a stream.
// Uses close-and-replace channel pattern to wake all waiters atomically.
type streamState struct {
	mu       sync.RWMutex
	notifyCh chan struct{} // Closed on append/delete, replaced with new channel
	deleted  bool          // Set to true when Delete() closes the channel
}

// ErrClosed is returned when operations are attempted on a closed storage.
var ErrClosed = errors.New("badgerstore: storage closed")

// Storage is a Badger-backed implementation of durablestream.Storage.
type Storage struct {
	db *badger.DB

	// In-memory notification tracking (ephemeral, not persisted).
	// Uses hashtriemap for lock-free lookups with per-stream locks for mutations.
	streams hashtriemap.HashTrieMap[string, *streamState]

	// Per-stream sequences for atomic offset generation (lock-free)
	seqs hashtriemap.HashTrieMap[string, *badger.Sequence]

	// Configuration
	maxMessageSize  int
	shutdownTimeout time.Duration
	logger          *slog.Logger

	// Background goroutine control
	wg             sync.WaitGroup
	shutdownCtx    context.Context    // Cancelled on Close(), signals all background work to stop
	shutdownCancel context.CancelFunc // Called during Close()

	// Close protection - prevents double-close panic and rejects operations after close
	closeOnce sync.Once
	closed    atomic.Bool

	// Sequence creation mutex - prevents race condition in getOrCreateSequence
	seqCreateMu sync.Mutex

	// Temp directory to clean up on Close() (only set when in-memory mode
	// was requested but disk storage was needed for large message support)
	tempDir string
}

// New creates a new Badger-backed storage.
func New(opts Options) (*Storage, error) {
	maxMsgSize := opts.MaxMessageSize
	if maxMsgSize <= 0 {
		maxMsgSize = DefaultMaxMessageSize
	}

	// Badger's in-memory mode has a hard 1MB value limit (ValueThreshold max).
	// For larger messages, we must use disk storage even if InMemory was requested.
	const badgerMaxValueThreshold = 1 << 20 // 1MB - Badger's hard limit
	useInMemory := opts.InMemory || opts.Dir == ""
	actualDir := opts.Dir

	var tempDir string
	if useInMemory && maxMsgSize > badgerMaxValueThreshold {
		// Need disk storage for large messages - use temp directory
		var err error
		tempDir, err = os.MkdirTemp("", "badgerstore-*")
		if err != nil {
			return nil, fmt.Errorf("badgerstore: create temp dir: %w", err)
		}
		actualDir = tempDir
		useInMemory = false
	}

	badgerOpts := badger.DefaultOptions(actualDir)
	if useInMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	} else {
		// Disk mode: values are checked against ValueLogFileSize (default 1GB).
		// Ensure it's at least 2x maxMsgSize for headroom.
		minVLogSize := int64(maxMsgSize) * 2
		if badgerOpts.ValueLogFileSize < minVLogSize {
			badgerOpts = badgerOpts.WithValueLogFileSize(minVLogSize)
		}
	}
	if opts.Logger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.Logger)
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		// Clean up temp dir if we created one and open failed
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
		return nil, fmt.Errorf("badgerstore: open: %w", err)
	}

	shutdownTimeout := opts.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = DefaultShutdownTimeout
	}

	logger := opts.SLogger
	if logger == nil {
		logger = slog.Default()
	}

	// Create shutdown context - cancelled during Close() to interrupt background work
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())

	s := &Storage{
		db:              db,
		maxMessageSize:  maxMsgSize,
		shutdownTimeout: shutdownTimeout,
		logger:          logger,
		shutdownCtx:     shutdownCtx,
		shutdownCancel:  shutdownCancel,
		tempDir:         tempDir,
	}

	// Start background GC if enabled
	gcInterval := opts.GCInterval
	if gcInterval == 0 {
		gcInterval = DefaultGCInterval
	}
	if gcInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runGCLoop(gcInterval)
		}()
	}

	// Start background cleanup if enabled
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = DefaultCleanupInterval
	}
	if cleanupInterval > 0 {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.runCleanupLoop(cleanupInterval)
		}()
	}

	return s, nil
}

// Close closes the Badger database and stops background goroutines.
// Waits up to ShutdownTimeout for background goroutines to finish gracefully.
// If the timeout is exceeded, Close logs a warning and returns anyway to prevent indefinite hangs.
// Close is safe to call multiple times - subsequent calls are no-ops.
func (s *Storage) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		// Mark as closed to reject new operations
		s.closed.Store(true)

		// Cancel shutdown context to signal all background goroutines and interrupt blocking operations
		s.shutdownCancel()

		// Wait for background goroutines with timeout
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		cleanShutdown := false
		select {
		case <-done:
			// Goroutines finished within timeout
			cleanShutdown = true
		case <-time.After(s.shutdownTimeout):
			// Timeout exceeded - log warning but proceed to prevent indefinite hangs
			s.logger.Warn("badgerstore: shutdown timeout exceeded, proceeding with close",
				"timeout", s.shutdownTimeout)
		}

		// Only release sequences on clean shutdown - avoids race with goroutines still using them
		if cleanShutdown {
			s.seqs.Range(func(streamID string, seq *badger.Sequence) bool {
				if err := seq.Release(); err != nil {
					s.logger.Warn("badgerstore: failed to release sequence",
						"streamID", streamID, "error", err)
				}
				s.seqs.Delete(streamID)
				return true
			})
		} else {
			s.logger.Warn("badgerstore: skipping sequence release due to unclean shutdown")
		}

		closeErr = s.db.Close()

		// Clean up temp directory if we created one
		if s.tempDir != "" {
			if err := os.RemoveAll(s.tempDir); err != nil {
				s.logger.Warn("badgerstore: failed to remove temp directory",
					"dir", s.tempDir, "error", err)
			}
		}
	})

	return closeErr
}

// checkClosed returns ErrClosed if the storage has been closed.
func (s *Storage) checkClosed() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}

// RunGC runs Badger's value log garbage collection.
// Call this periodically for long-running processes.
func (s *Storage) RunGC() error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	return s.db.RunValueLogGC(0.5)
}

// validateStreamID checks if a streamID is valid.
// StreamIDs must be non-empty and not contain ':' (used as key separator).
func validateStreamID(streamID string) error {
	if streamID == "" {
		return fmt.Errorf("badgerstore: streamID cannot be empty: %w", durablestream.ErrBadRequest)
	}
	if strings.Contains(streamID, ":") {
		return fmt.Errorf("badgerstore: streamID cannot contain ':': %w", durablestream.ErrBadRequest)
	}
	return nil
}

// Create creates a new stream (Section 5.1).
func (s *Storage) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	if err := s.checkClosed(); err != nil {
		return false, err
	}
	if err := validateStreamID(streamID); err != nil {
		return false, err
	}

	configKey := []byte(prefixConfig + streamID)
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return false, fmt.Errorf("badgerstore: marshal config: %w", err)
	}

	var created bool
	var needsCleanup bool // Track if we need to clean up an expired stream's data
	err = s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(configKey)
		if err == nil {
			// Stream exists - check if it's expired first
			var existing durablestream.StreamConfig
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &existing)
			}); err != nil {
				return fmt.Errorf("badgerstore: unmarshal existing config: %w", err)
			}

			if existing.IsExpired() {
				// Expired stream can be replaced (Section 5.1)
				// Delete old config and metadata in this transaction
				if err := txn.Delete(configKey); err != nil {
					return fmt.Errorf("badgerstore: delete expired config: %w", err)
				}
				// Delete sequence key
				seqNumKey := []byte(prefixSeq + streamID)
				_ = txn.Delete(seqNumKey)
				// Delete last dedup seq
				seqKey := []byte(prefixLastSeq + streamID)
				_ = txn.Delete(seqKey)

				needsCleanup = true // Clean up messages after transaction
			} else {
				// Not expired - check if config matches for idempotency
				if existing.Matches(cfg) {
					created = false
					return nil
				}
				return fmt.Errorf("badgerstore: stream exists with different config: %w", durablestream.ErrConflict)
			}
		} else if err != badger.ErrKeyNotFound {
			return fmt.Errorf("badgerstore: check existing: %w", err)
		}

		// Create new stream
		if err := txn.Set(configKey, encoded); err != nil {
			return fmt.Errorf("badgerstore: set config: %w", err)
		}

		created = true
		return nil
	})
	if err != nil {
		return false, err
	}

	// Clean up expired stream's messages in background (best effort)
	if needsCleanup {
		// Release old sequence if exists
		if seq, ok := s.seqs.LoadAndDelete(streamID); ok {
			_ = seq.Release()
		}
		// Close old notification channel if any
		if state, ok := s.streams.LoadAndDelete(streamID); ok {
			state.mu.Lock()
			state.deleted = true
			close(state.notifyCh)
			state.mu.Unlock()
		}
		// Delete old messages in batches (best effort, don't fail create)
		prefix := []byte(prefixMessage + streamID + ":")
		for {
			deleted, err := s.deleteMessageBatch(ctx, prefix, deleteBatchSize)
			if err != nil || deleted == 0 {
				break
			}
		}
	}

	// Initialize in-memory notification state
	if created {
		s.streams.Store(streamID, &streamState{
			notifyCh: make(chan struct{}),
		})
	}

	return created, nil
}

// Append writes data to a stream (Section 5.2).
func (s *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("badgerstore: empty append: %w", durablestream.ErrBadRequest)
	}
	if len(data) > s.maxMessageSize {
		return "", fmt.Errorf("badgerstore: message too large (%d > %d): %w",
			len(data), s.maxMessageSize, durablestream.ErrPayloadTooLarge)
	}

	// Validate stream exists and not expired first
	err := s.db.View(func(txn *badger.Txn) error {
		cfg, err := s.getConfig(txn, streamID)
		if err != nil {
			return err
		}
		if cfg.IsExpired() {
			return durablestream.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	// Get atomic sequence for offset (lock-free, no transaction conflicts)
	sequence, err := s.getOrCreateSequence(streamID)
	if err != nil {
		return "", fmt.Errorf("badgerstore: get sequence: %w", err)
	}
	nextNum, err := sequence.Next()
	if err != nil {
		return "", fmt.Errorf("badgerstore: next sequence: %w", err)
	}
	newOffset := storage.FormatSimpleOffset(int64(nextNum + 1)) // sequences start at 0, offsets at 1

	// Write the message (and sequence validation if provided)
	err = s.db.Update(func(txn *badger.Txn) error {
		// Re-validate in case stream was deleted between View and Update
		cfg, err := s.getConfig(txn, streamID)
		if err != nil {
			return err
		}
		if cfg.IsExpired() {
			return durablestream.ErrNotFound
		}

		// Validate dedup sequence number if provided
		if seq != "" {
			lastSeq, err := s.getLastSeq(txn, streamID)
			if err != nil && err != badger.ErrKeyNotFound {
				return fmt.Errorf("badgerstore: get last seq: %w", err)
			}
			if lastSeq != "" && seq <= lastSeq {
				return fmt.Errorf("badgerstore: sequence regression: %w", durablestream.ErrConflict)
			}
			seqKey := []byte(prefixLastSeq + streamID)
			if err := txn.Set(seqKey, []byte(seq)); err != nil {
				return fmt.Errorf("badgerstore: set last seq: %w", err)
			}
		}

		// Write the message
		msgKey := []byte(prefixMessage + streamID + ":" + newOffset.String())
		if err := txn.Set(msgKey, data); err != nil {
			return fmt.Errorf("badgerstore: set message: %w", err)
		}

		return nil
	})
	if err != nil {
		// Map Badger's transaction conflict to durablestream conflict.
		// This can happen when concurrent writers race to update the lastSeq key.
		// The client should retry the operation.
		if errors.Is(err, badger.ErrConflict) {
			return "", fmt.Errorf("badgerstore: concurrent write conflict: %w", durablestream.ErrConflict)
		}
		return "", err
	}

	// Notify waiters: close current channel to wake all, then replace it
	state, _ := s.streams.LoadOrStore(streamID, &streamState{
		notifyCh: make(chan struct{}),
	})
	state.mu.Lock()
	if !state.deleted {
		close(state.notifyCh)
		state.notifyCh = make(chan struct{})
	}
	state.mu.Unlock()

	return newOffset, nil
}

// getOrCreateSequence returns the Badger sequence for a stream, creating it if needed.
// Uses lock-free hashtriemap for O(1) lookups, with mutex protection for creation
// to prevent race conditions that could cause duplicate offsets.
func (s *Storage) getOrCreateSequence(streamID string) (*badger.Sequence, error) {
	// Fast path: sequence already exists (lock-free)
	if seq, ok := s.seqs.Load(streamID); ok {
		return seq, nil
	}

	// Slow path: acquire mutex for creation to prevent race condition
	// where multiple goroutines create sequences and some sequence numbers
	// are lost when the "losing" sequences are released.
	s.seqCreateMu.Lock()
	defer s.seqCreateMu.Unlock()

	// Double-check after acquiring lock - another goroutine may have created it
	if seq, ok := s.seqs.Load(streamID); ok {
		return seq, nil
	}

	// Create new sequence - now safe from races
	seqKey := []byte(prefixSeq + streamID)
	seq, err := s.db.GetSequence(seqKey, 100)
	if err != nil {
		return nil, err
	}

	s.seqs.Store(streamID, seq)
	return seq, nil
}

// AppendFrom streams data from an io.Reader to a stream.
func (s *Storage) AppendFrom(ctx context.Context, streamID string, r io.Reader, seq string) (durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return "", err
	}

	// Limit reader to prevent OOM
	limited := io.LimitReader(r, int64(s.maxMessageSize)+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return "", fmt.Errorf("badgerstore: read body: %w", err)
	}
	if len(data) > s.maxMessageSize {
		return "", fmt.Errorf("badgerstore: message too large: %w", durablestream.ErrPayloadTooLarge)
	}
	return s.Append(ctx, streamID, data, seq)
}

// Read returns messages from offset (Section 5.5).
func (s *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("badgerstore: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	var result *durablestream.ReadResult

	err := s.db.View(func(txn *badger.Txn) error {
		// Check for context cancellation
		if err := ctx.Err(); err != nil {
			return err
		}

		cfg, err := s.getConfig(txn, streamID)
		if err != nil {
			return err
		}
		if cfg.IsExpired() {
			return durablestream.ErrNotFound
		}

		// Find the tail offset (highest message offset)
		tailOffset, err := s.getTailOffset(txn, streamID)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}

		// Collect messages starting after the requested offset
		// Gaps are handled naturally: we seek to the start position and iterate
		// forward, returning whatever messages exist.
		var messages []durablestream.StoredMessage
		totalBytes := 0

		prefix := []byte(prefixMessage + streamID + ":")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Parse offset to calculate start position
		_, byteOffset, err := storage.ParseOffset(offset)
		if err != nil {
			return err
		}
		startOffset := storage.FormatSimpleOffset(byteOffset + 1)
		startKey := []byte(prefixMessage + streamID + ":" + string(startOffset))
		it.Seek(startKey)

		for it.Valid() {
			// Check for context cancellation
			if err := ctx.Err(); err != nil {
				return err
			}

			item := it.Item()
			keyStr := string(item.Key())
			offsetStr := keyStr[len(prefix):]
			msgOffset := durablestream.Offset(offsetStr)

			var data []byte
			if err := item.Value(func(val []byte) error {
				data = make([]byte, len(val))
				copy(data, val)
				return nil
			}); err != nil {
				return fmt.Errorf("badgerstore: read message: %w", err)
			}

			if limit > 0 && totalBytes+len(data) > limit && len(messages) > 0 {
				break
			}

			messages = append(messages, durablestream.StoredMessage{
				Data:   data,
				Offset: msgOffset,
			})
			totalBytes += len(data)

			it.Next()
		}

		// Determine next offset for subsequent reads
		var nextOffset durablestream.Offset
		if len(messages) > 0 {
			nextOffset = messages[len(messages)-1].Offset
		} else if offset.IsZero() || offset == "-1" {
			// Empty/zero offset or -1 sentinel with no messages: return formatted zero offset
			nextOffset = storage.FormatSimpleOffset(0)
		} else if offset.Compare(tailOffset) >= 0 {
			// At or past the tail: return requested offset to avoid going backward
			// This prevents duplicate message delivery on retry
			nextOffset = offset
		} else {
			// Before tail but no messages (gap): return requested offset
			nextOffset = offset
		}

		result = &durablestream.ReadResult{
			Messages:   messages,
			NextOffset: nextOffset,
			TailOffset: tailOffset,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Head returns stream metadata (Section 5.4).
func (s *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}

	var info *durablestream.StreamInfo

	err := s.db.View(func(txn *badger.Txn) error {
		cfg, err := s.getConfig(txn, streamID)
		if err != nil {
			return err
		}
		if cfg.IsExpired() {
			return durablestream.ErrNotFound
		}

		tailOffset, err := s.getTailOffset(txn, streamID)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}

		info = &durablestream.StreamInfo{
			ContentType: cfg.ContentType,
			NextOffset:  tailOffset,
			TTL:         cfg.TTL,
			ExpiresAt:   cfg.ExpiresAt,
			IsPrivate:   cfg.IsPrivate,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

// deleteBatchSize is the maximum number of keys to delete in a single transaction.
// Badger transactions have size limits; batching prevents failures on large streams.
const deleteBatchSize = 10000

// Delete removes a stream (Section 5.3).
func (s *Storage) Delete(ctx context.Context, streamID string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}

	// Mark deleted and wake any WaitForData callers
	if state, ok := s.streams.LoadAndDelete(streamID); ok {
		state.mu.Lock()
		state.deleted = true
		close(state.notifyCh)
		state.mu.Unlock()
	}

	// Release sequence if exists (lock-free)
	if seq, ok := s.seqs.LoadAndDelete(streamID); ok {
		_ = seq.Release()
	}

	// First transaction: delete config and metadata, check existence
	var found bool
	err := s.db.Update(func(txn *badger.Txn) error {
		configKey := []byte(prefixConfig + streamID)
		_, err := txn.Get(configKey)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return fmt.Errorf("badgerstore: check stream: %w", err)
		}
		found = true

		// Delete config
		if err := txn.Delete(configKey); err != nil {
			return fmt.Errorf("badgerstore: delete config: %w", err)
		}

		// Delete sequence key
		seqNumKey := []byte(prefixSeq + streamID)
		_ = txn.Delete(seqNumKey)

		// Delete last dedup seq
		seqKey := []byte(prefixLastSeq + streamID)
		_ = txn.Delete(seqKey)

		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return durablestream.ErrNotFound
	}

	// Delete messages in batches to avoid transaction size limits
	prefix := []byte(prefixMessage + streamID + ":")
	for {
		// Check for context cancellation before each batch
		if err := ctx.Err(); err != nil {
			return err
		}
		deleted, err := s.deleteMessageBatch(ctx, prefix, deleteBatchSize)
		if err != nil {
			return fmt.Errorf("badgerstore: delete messages: %w", err)
		}
		if deleted == 0 {
			break // No more messages
		}
	}

	return nil
}

// deleteMessageBatch deletes up to limit messages with the given prefix.
// Returns the number of messages deleted.
func (s *Storage) deleteMessageBatch(ctx context.Context, prefix []byte, limit int) (int, error) {
	var keysToDelete [][]byte

	// Collect keys in a read transaction
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix) && len(keysToDelete) < limit; it.Next() {
			// Check for context cancellation
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

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	// Delete in a write transaction
	err = s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keysToDelete {
			// Check for context cancellation
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	return len(keysToDelete), nil
}

// WaitForData blocks until data is available at offset, then returns it.
// Returns immediately if data already exists at offset.
// Returns ctx.Err() on timeout/cancellation.
// Returns ErrNotFound if stream doesn't exist or is deleted while waiting.
func (s *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}

	for {
		// Get or create in-memory notification state
		state, _ := s.streams.LoadOrStore(streamID, &streamState{
			notifyCh: make(chan struct{}),
		})

		state.mu.RLock()
		if state.deleted {
			state.mu.RUnlock()
			return nil, durablestream.ErrNotFound
		}
		notifyCh := state.notifyCh
		state.mu.RUnlock()

		// Try to read data
		result, err := s.Read(ctx, streamID, offset, limit)
		if err != nil {
			return nil, err
		}
		if len(result.Messages) > 0 {
			return result, nil
		}

		// No data available, wait for notification or context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notifyCh:
			// New data or deletion — loop to re-check
		}
	}
}

// Helper methods

func (s *Storage) getConfig(txn *badger.Txn, streamID string) (durablestream.StreamConfig, error) {
	configKey := []byte(prefixConfig + streamID)
	item, err := txn.Get(configKey)
	if err == badger.ErrKeyNotFound {
		return durablestream.StreamConfig{}, durablestream.ErrNotFound
	}
	if err != nil {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: get config: %w", err)
	}

	var cfg durablestream.StreamConfig
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &cfg)
	})
	if err != nil {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: unmarshal config: %w", err)
	}
	return cfg, nil
}

func (s *Storage) getLastSeq(txn *badger.Txn, streamID string) (string, error) {
	seqKey := []byte(prefixLastSeq + streamID)
	item, err := txn.Get(seqKey)
	if err != nil {
		return "", err
	}

	var seq string
	err = item.Value(func(val []byte) error {
		seq = string(val)
		return nil
	})
	return seq, err
}

// getTailOffset returns the highest message offset for a stream.
// Uses reverse iteration to find the last key efficiently.
// Returns formatted zero offset for empty streams.
func (s *Storage) getTailOffset(txn *badger.Txn, streamID string) (durablestream.Offset, error) {
	prefix := []byte(prefixMessage + streamID + ":")

	// Use reverse iteration to find the last message efficiently
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	opts.Reverse = true

	it := txn.NewIterator(opts)
	defer it.Close()

	// Seek to the end of the prefix range
	// For reverse iteration, we seek to the prefix + \xff to start at the end
	seekKey := make([]byte, len(prefix)+1)
	copy(seekKey, prefix)
	seekKey[len(prefix)] = 0xff
	it.Seek(seekKey)

	if it.ValidForPrefix(prefix) {
		key := it.Item().Key()
		offsetStr := string(key[len(prefix):])
		return durablestream.Offset(offsetStr), nil
	}

	// No messages: return zero offset
	return storage.FormatSimpleOffset(0), nil
}
