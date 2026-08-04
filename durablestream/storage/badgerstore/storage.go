// Package badgerstore provides a Badger-backed implementation of durablestream.Storage.
//
// This implementation is optimized for development and testing workloads.
// For production use, consider the following limitations:
//
//   - Expired streams are cleaned up by a background goroutine (configurable interval)
//   - Badger's value-log GC runs every Options.GCInterval (5 minutes by
//     default); set it to -1 to disable the loop, or call RunGC on demand
//   - Message size is limited by MaxMessageSize (default 10 MiB on disk and
//     1 MiB minus one byte in memory)
//   - Single-process only: Badger uses file locking, but no additional fencing is performed
//
// # Durability
//
// In disk mode, writes are fsynced before an operation is acknowledged (see
// [Options.SyncWrites]), so a successful Append survives process death and
// machine crash. Disabling SyncWrites trades that guarantee for throughput:
// acknowledged appends can then be lost if the process is killed. In-memory
// mode has no durability at all.
//
// # Deletion
//
// Delete removes a stream's configuration atomically and records a tombstone;
// the stream's message data is purged afterwards by a background reaper. Each
// stream incarnation is assigned a unique generation that scopes its message
// keys, so a purge that is slow, interrupted, or resumed after a crash can
// never observe or destroy data belonging to a stream created later with the
// same ID. The reaper also sweeps orphaned data at startup.
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
//
// # On-disk format
//
// Generation scoping changed the message key layout. A directory containing
// streams written by an earlier version is rejected with [ErrLegacyFormat]
// before any background reaper starts. The old bytes are left intact so an
// operator can migrate them explicitly instead of losing durable data merely by
// opening the directory with a newer binary.
package badgerstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
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

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
)

// streamState holds in-memory notification state for a stream.
// Uses close-and-replace channel pattern to wake all waiters atomically.
type streamState struct {
	appendMu sync.Mutex // Serializes offset reservation and commit for this generation
	mu       sync.RWMutex
	notifyCh chan struct{} // Closed on append/delete/close, replaced with new channel
	deleted  bool          // Set to true when the stream is deleted or replaced
	closed   bool          // Set to true when Close() wakes waiters
	gen      generation    // Stream incarnation this notification state belongs to
}

// streamStateKey identifies notification state for one stream incarnation.
// Stream IDs cannot contain ':', and generations are fixed-width hex strings,
// so this encoding is unambiguous.
func streamStateKey(streamID string, gen generation) string {
	return streamID + ":" + string(gen)
}

// wake closes the current notification channel and installs a fresh one,
// releasing every waiter. It is a no-op once the stream is deleted or the
// storage is closed, since the channel is left closed in those states.
func (st *streamState) wake() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted || st.closed {
		return
	}
	close(st.notifyCh)
	st.notifyCh = make(chan struct{})
}

// markDeleted permanently closes a generation's notification channel. Waiters
// keep a pointer to this state, so they observe the deletion even if a later
// stream incarnation reuses the same stream ID.
func (st *streamState) markDeleted() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted || st.closed {
		return
	}
	st.deleted = true
	close(st.notifyCh)
}

// ErrClosed is returned when operations are attempted on a closed storage.
// It wraps durablestream.ErrClosed so callers (and HTTP error mapping) can
// classify it with errors.Is.
var ErrClosed = fmt.Errorf("badgerstore: storage closed: %w", durablestream.ErrClosed)

// ErrLegacyFormat is returned by New when a Badger directory contains stream
// records written before generation-scoped keys were introduced. New closes
// the database and leaves every legacy key intact; callers must migrate or
// explicitly discard that directory before reopening it.
var ErrLegacyFormat = errors.New("badgerstore: legacy on-disk format")

// errGenerationChanged is an internal sentinel: the stream was deleted and
// recreated between reading its generation and committing a write.
var errGenerationChanged = errors.New("badgerstore: stream generation changed")

// stopTimer releases a deadline timer when another wakeup wins the select.
// The non-blocking drain handles the race where the timer fired concurrently.
func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// appendAttempts bounds how many times Append retries after losing a race with
// a concurrent Delete + Create of the same stream ID.
const appendAttempts = 3

// txnAttempts bounds how many times a mutation retries a Badger transaction
// conflict caused by a concurrent mutation of the same stream ID.
const txnAttempts = 10

// streamRecord is the persisted form of a stream's configuration. The
// configuration is embedded so the JSON encoding stays compatible with the
// fields written by earlier versions.
type streamRecord struct {
	durablestream.StreamConfig
	Gen generation `json:"gen"`
}

// isLegacy reports whether the record predates generation scoping. New rejects
// databases containing such records so their old-format message data remains
// intact for an explicit migration.
func (r streamRecord) isLegacy() bool { return r.Gen == "" }

// isUsable reports whether the record represents a live, readable stream.
func (r streamRecord) isUsable() bool { return !r.isLegacy() && !r.IsExpired() }

// Storage is a Badger-backed implementation of durablestream.Storage.
type Storage struct {
	db *badger.DB

	// In-memory notification tracking (ephemeral, not persisted). Entries are
	// keyed by stream ID plus generation, so deleting an old generation can
	// never remove or wake the state of a replacement generation.
	streams hashtriemap.HashTrieMap[string, *streamState]

	// Per-generation sequences for atomic offset generation (lock-free).
	// Keyed by the stream's sequence key, so a recreated stream never shares a
	// sequence with the generation it replaced.
	seqs hashtriemap.HashTrieMap[string, *badger.Sequence]

	// Configuration
	maxMessageSize  int
	shutdownTimeout time.Duration
	logger          *slog.Logger

	// Background goroutine control
	wg             sync.WaitGroup
	shutdownCtx    context.Context    // Cancelled on Close(), signals all background work to stop
	shutdownCancel context.CancelFunc // Called during Close()

	// reapCh signals the reaper that a tombstone is waiting. Capacity 1 with a
	// non-blocking send: the reaper always sweeps every outstanding tombstone,
	// so a coalesced signal loses no work.
	reapCh chan struct{}

	// initialReapDone is closed once the startup sweep has finished.
	initialReapDone chan struct{}

	// Close protection - prevents double-close panic and rejects operations after close
	closeOnce sync.Once
	closed    atomic.Bool

	// dbMu keeps the database from being closed while an operation is using
	// it. Every database call runs under a read lock and re-checks closed
	// under that same lock; Close takes the write lock before db.Close().
	// Badger panics rather than returning an error when a closed database is
	// used, so the atomic flag alone is not enough: it is a check that an
	// in-flight operation can pass moments before Close runs.
	dbMu sync.RWMutex

	// Sequence creation mutex - prevents race condition in getOrCreateSequence
	seqCreateMu sync.Mutex

	// Temp directory to clean up on Close. It is set when no persistent Dir was
	// provided and the caller did not request strictly in-memory storage.
	tempDir string
}

// New creates a new Badger-backed storage.
func New(opts Options) (*Storage, error) {
	if opts.InMemory && opts.Dir != "" {
		return nil, fmt.Errorf("badgerstore: InMemory and Dir are mutually exclusive")
	}

	maxMsgSize := opts.MaxMessageSize
	if maxMsgSize <= 0 {
		if opts.InMemory {
			maxMsgSize = DefaultInMemoryMaxMessageSize
		} else {
			maxMsgSize = DefaultMaxMessageSize
		}
	}
	if opts.InMemory && maxMsgSize > DefaultInMemoryMaxMessageSize {
		return nil, fmt.Errorf(
			"badgerstore: MaxMessageSize %d exceeds in-memory limit %d",
			maxMsgSize, DefaultInMemoryMaxMessageSize,
		)
	}

	useInMemory := opts.InMemory
	actualDir := opts.Dir

	var tempDir string
	if !useInMemory && actualDir == "" {
		// The zero-value configuration remains useful and supports the normal
		// 10 MiB message limit, but it is explicitly ephemeral disk storage rather
		// than silently pretending to be memory-only.
		var err error
		tempDir, err = os.MkdirTemp("", "badgerstore-*")
		if err != nil {
			return nil, fmt.Errorf("badgerstore: create temp dir: %w", err)
		}
		actualDir = tempDir
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
	// Badger defaults SyncWrites to false, which loses acknowledged writes on
	// process death. Default to fsync in disk mode. Badger forces it off in
	// in-memory mode, where there is nothing to sync.
	syncWrites, err := opts.SyncWrites.enabled(!useInMemory)
	if err != nil {
		if tempDir != "" {
			_ = os.RemoveAll(tempDir)
		}
		return nil, err
	}
	badgerOpts = badgerOpts.WithSyncWrites(syncWrites)
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
	if err := rejectLegacyFormat(db); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("badgerstore: close after format check: %w", closeErr))
		}
		if tempDir != "" {
			if removeErr := os.RemoveAll(tempDir); removeErr != nil {
				err = errors.Join(err, fmt.Errorf("badgerstore: remove temporary directory after format check: %w", removeErr))
			}
		}
		return nil, err
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
		reapCh:          make(chan struct{}, 1),
		initialReapDone: make(chan struct{}),
		tempDir:         tempDir,
	}

	// Start background GC if enabled
	gcInterval := opts.GCInterval
	if gcInterval == 0 {
		gcInterval = DefaultGCInterval
	}
	if gcInterval > 0 {
		s.wg.Go(func() { s.runGCLoop(gcInterval) })
	}

	// Start background cleanup if enabled
	cleanupInterval := opts.CleanupInterval
	if cleanupInterval == 0 {
		cleanupInterval = DefaultCleanupInterval
	}
	if cleanupInterval > 0 {
		s.wg.Go(func() { s.runCleanupLoop(cleanupInterval) })
	}

	// The reaper always runs: deleted streams rely on it to purge their data.
	reapInterval := opts.ReapInterval
	if reapInterval <= 0 {
		reapInterval = DefaultReapInterval
	}
	s.wg.Go(func() { s.runReaperLoop(reapInterval) })

	return s, nil
}

// rejectLegacyFormat checks the persisted stream records before Storage starts
// any background work. Older records have no generation, and therefore refer
// to message keys whose layout this version cannot safely operate on. Failing
// the open preserves those bytes for an explicit migration.
func rejectLegacyFormat(db *badger.DB) error {
	return db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefixConfig)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			streamID := string(item.Key()[len(prefixConfig):])
			var rec streamRecord
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rec)
			}); err != nil {
				return fmt.Errorf("badgerstore: inspect stream record %q before open: %w", streamID, err)
			}
			if rec.isLegacy() {
				return fmt.Errorf("badgerstore: stream %q predates generation-scoped storage: %w", streamID, ErrLegacyFormat)
			}
		}
		return nil
	})
}

// Close closes the Badger database and stops background goroutines.
// Waits up to ShutdownTimeout for background goroutines to finish gracefully.
// If the timeout is exceeded, Close returns an error rather than hanging, and
// the database is closed in the background once the stragglers finish.
// Close is safe to call multiple times - subsequent calls are no-ops.
func (s *Storage) Close() error {
	var closeErr error

	s.closeOnce.Do(func() {
		// Mark as closed to reject new operations
		s.closed.Store(true)

		// Cancel shutdown context to signal all background goroutines and interrupt blocking operations
		s.shutdownCancel()

		// Release WaitForData waiters. They also select on shutdownCtx, but
		// closing their channels wakes them without relying on that alone.
		s.streams.Range(func(_ string, state *streamState) bool {
			state.mu.Lock()
			if !state.deleted && !state.closed {
				state.closed = true
				close(state.notifyCh)
			}
			state.mu.Unlock()
			return true
		})

		// Wait for background goroutines with timeout.
		// This goroutine is owned by Close: it terminates as soon as the
		// background loops observe shutdownCtx, which is already cancelled.
		done := make(chan struct{})
		go func() {
			s.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			closeErr = s.shutdownDB()
		case <-time.After(s.shutdownTimeout):
			// Background goroutines are still running and may be using the
			// database; closing it underneath them risks a crash. Defer the
			// close to a goroutine that waits for them to drain, and report
			// the timeout instead of blocking the caller.
			s.logger.Warn("badgerstore: shutdown timeout exceeded, deferring database close",
				"timeout", s.shutdownTimeout)
			go func() {
				<-done
				if err := s.shutdownDB(); err != nil {
					s.logger.Warn("badgerstore: deferred database close failed", "error", err)
				}
			}()
			closeErr = fmt.Errorf("badgerstore: background goroutines did not finish within %s; database close deferred", s.shutdownTimeout)
		}
	})

	return closeErr
}

// shutdownDB releases sequences, closes the database, and removes the temp
// directory. It runs exactly once, after all background goroutines have exited.
//
// The write lock makes the close exclusive with every in-flight operation:
// callers that already passed the closed check finish first, and later ones
// see the closed flag under the read lock and get ErrClosed.
func (s *Storage) shutdownDB() error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

	s.seqs.Range(func(key string, seq *badger.Sequence) bool {
		if err := seq.Release(); err != nil {
			s.logger.Warn("badgerstore: failed to release sequence",
				"key", key, "error", err)
		}
		s.seqs.Delete(key)
		return true
	})

	err := s.db.Close()

	if s.tempDir != "" {
		if rmErr := os.RemoveAll(s.tempDir); rmErr != nil {
			s.logger.Warn("badgerstore: failed to remove temp directory",
				"dir", s.tempDir, "error", rmErr)
		}
	}
	return err
}

// withDB runs fn while holding Close off, so the database cannot be closed
// underneath it. Returns ErrClosed if the storage is already closed.
//
// fn must not block: it delays Close for as long as it runs, and it must not
// call withDB again (the read lock is not reentrant).
func (s *Storage) withDB(fn func() error) error {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.closed.Load() {
		return ErrClosed
	}
	return fn()
}

// view runs a read-only transaction, guarded against a concurrent Close.
func (s *Storage) view(fn func(txn *badger.Txn) error) error {
	return s.withDB(func() error { return s.db.View(fn) })
}

// update runs a read-write transaction, guarded against a concurrent Close.
func (s *Storage) update(fn func(txn *badger.Txn) error) error {
	return s.withDB(func() error { return s.db.Update(fn) })
}

// updateWithRetry runs a read-write transaction, retrying when Badger's
// snapshot isolation aborts it because a concurrent transaction wrote a key it
// read. Create and Delete both read and write a stream's config key, so racing
// callers collide there; retrying converges immediately, because the retry
// observes the winner's committed record and takes a branch that either writes
// nothing or writes over a record it has now seen.
//
// fn must be safe to run more than once and must reset anything it assigns to
// variables outside itself: only the committed run's effects are real.
func (s *Storage) updateWithRetry(fn func(txn *badger.Txn) error) error {
	var err error
	for range txnAttempts {
		err = s.update(fn)
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
	}
	// Unreachable in practice: once any caller commits, the contending
	// transactions stop writing the same key.
	return fmt.Errorf("badgerstore: transaction still contended after %d attempts: %w", txnAttempts, err)
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
	return s.withDB(func() error { return s.db.RunValueLogGC(0.5) })
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
	created, _, err := s.CreateWithMessages(ctx, streamID, cfg, nil)
	return created, err
}

// CreateWithMessages creates a stream and its initial messages in one Badger
// transaction. The generation-scoped sequence key is initialized to the number
// of committed messages so the next Append cannot reuse an initial offset.
func (s *Storage) CreateWithMessages(ctx context.Context, streamID string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return false, "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return false, "", err
	}
	if err := validateMessageBatch(messages, true, s.maxMessageSize); err != nil {
		return false, "", err
	}
	// Storage callers may specify the sliding window without precomputing its
	// first deadline. Initialize it once here; idempotent replays compare TTL but
	// deliberately ignore the newly derived ExpiresAt.
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}

	gen, err := newGeneration()
	if err != nil {
		return false, "", err
	}
	encoded, err := json.Marshal(streamRecord{StreamConfig: cfg, Gen: gen})
	if err != nil {
		return false, "", fmt.Errorf("badgerstore: marshal config: %w", err)
	}
	newTail := storage.FormatSimpleOffset(int64(len(messages)))
	var sequenceValue [8]byte
	binary.BigEndian.PutUint64(sequenceValue[:], uint64(len(messages)))

	var created bool
	var nextOffset durablestream.Offset
	var replaced generation // Non-empty when an unusable stream was displaced
	commit := func(txn *badger.Txn) error {
		created, nextOffset, replaced = false, "", ""
		existing, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if found {
			if existing.isLegacy() {
				return fmt.Errorf("badgerstore: stream %q predates generation-scoped storage: %w", streamID, ErrLegacyFormat)
			}
			if existing.isUsable() {
				// Not expired - check if config matches for idempotency
				if existing.Matches(cfg) {
					created = false
					nextOffset, err = getTailOffset(txn, streamID, existing.Gen)
					return err
				}
				return fmt.Errorf("badgerstore: stream exists with different config: %w", durablestream.ErrConflict)
			}

			// Expired streams may be replaced (Section 5.1).
			// Tombstone the displaced generation so the reaper purges its
			// messages; the new generation writes under a different prefix and
			// is unaffected by that purge.
			if err := txn.Set(tombstoneKey(streamID, existing.Gen), nil); err != nil {
				return fmt.Errorf("badgerstore: set tombstone: %w", err)
			}
			if err := txn.Delete(lastSeqKey(streamID)); err != nil {
				return fmt.Errorf("badgerstore: delete last seq: %w", err)
			}
			replaced = existing.Gen
		}

		// Create new stream
		if err := txn.Set(configKey(streamID), encoded); err != nil {
			return fmt.Errorf("badgerstore: set config: %w", err)
		}
		for i, message := range messages {
			offset := storage.FormatSimpleOffset(int64(i + 1))
			if err := txn.Set(messageKey(streamID, gen, offset), message); err != nil {
				return fmt.Errorf("badgerstore: set initial message %d: %w", i, err)
			}
		}
		if len(messages) > 0 {
			// Badger sequences persist the next number to lease. Initial messages
			// occupy offsets 1..N, corresponding to sequence values 0..N-1, so
			// storing N makes the first later Next return N (offset N+1).
			if err := txn.Set(seqKey(streamID, gen), sequenceValue[:]); err != nil {
				return fmt.Errorf("badgerstore: initialize sequence: %w", err)
			}
		}

		created = true
		nextOffset = newTail
		return nil
	}

	// Racing creates of the same stream ID all write the config key, so all
	// but one hits a Badger transaction conflict. Create is idempotent, so the
	// conflict is retried rather than pushed onto the caller: the retry sees
	// the winner's record and reports created=false. A create losing this race
	// is a benign race, not the ErrConflict of an incompatible config.
	if err := s.updateWithRetry(commit); err != nil {
		return false, "", mapTransactionSizeError(err)
	}

	// Past this point the commit is durable; drop in-memory state for the
	// displaced generation and let the reaper purge its data.
	if replaced != "" {
		s.forgetStream(streamID, replaced)
		s.signalReaper()
	}

	return created, nextOffset, nil
}

// Append writes data to a stream (Section 5.2).
func (s *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	return s.AppendBatch(ctx, streamID, [][]byte{data}, seq)
}

// AppendBatch appends messages in one Badger transaction. Offset reservation is
// serialized per generation with ordinary Append calls, so no other append can
// interleave inside the batch. Reservations happen before the transaction and
// may therefore leave gaps when validation loses a lifecycle race or commit
// fails; no message in a failed batch becomes visible.
func (s *Storage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return "", err
	}
	if err := validateMessageBatch(messages, false, s.maxMessageSize); err != nil {
		return "", err
	}

	// The generation can change if the stream is deleted and recreated between
	// resolving it and committing the write; the transaction detects that and
	// we retry against the replacement.
	for attempt := 0; ; attempt++ {
		offset, err := s.appendBatchOnce(streamID, messages, seq)
		if errors.Is(err, errGenerationChanged) && attempt < appendAttempts-1 {
			continue
		}
		if errors.Is(err, errGenerationChanged) {
			return "", durablestream.ErrNotFound
		}
		return offset, err
	}
}

// appendBatchOnce performs one attempt at appending, against the stream's currently
// known generation. It returns errGenerationChanged if the stream was replaced
// before the write committed.
func (s *Storage) appendBatchOnce(streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	gen, err := s.generationFor(streamID)
	if err != nil {
		return "", err
	}
	state := s.notificationState(streamID, gen)
	state.appendMu.Lock()
	defer state.appendMu.Unlock()

	// Get atomic sequence for offset (lock-free, no transaction conflicts)
	sequence, err := s.getOrCreateSequence(streamID, gen)
	if err != nil {
		return "", fmt.Errorf("badgerstore: get sequence: %w", err)
	}
	// Next may extend the lease, which writes to the database. Reserve the whole
	// block while holding appendMu so ordinary Appends cannot take an offset from
	// the middle of this batch.
	offsets := make([]durablestream.Offset, len(messages))
	if err := s.withDB(func() error {
		for i := range messages {
			nextNum, err := sequence.Next()
			if err != nil {
				return err
			}
			if nextNum >= math.MaxInt64 {
				return fmt.Errorf("badgerstore: offset space exhausted")
			}
			offsets[i] = storage.FormatSimpleOffset(int64(nextNum + 1))
		}
		return nil
	}); err != nil {
		if errors.Is(err, durablestream.ErrClosed) {
			return "", err
		}
		return "", fmt.Errorf("badgerstore: next sequence: %w", err)
	}

	// Write every message (and sequence validation if provided) atomically.
	err = s.update(func(txn *badger.Txn) error {
		// Re-validate: the stream may have been deleted, replaced, or expired
		// since the generation was resolved.
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() {
			return durablestream.ErrNotFound
		}
		if rec.Gen != gen {
			return errGenerationChanged
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
			if err := txn.Set(lastSeqKey(streamID), []byte(seq)); err != nil {
				return fmt.Errorf("badgerstore: set last seq: %w", err)
			}
		}

		for i, message := range messages {
			if err := txn.Set(messageKey(streamID, gen, offsets[i]), message); err != nil {
				return fmt.Errorf("badgerstore: set message %d: %w", i, err)
			}
		}

		return nil
	})
	if err != nil {
		err = mapTransactionSizeError(err)
		if errors.Is(err, errGenerationChanged) {
			return "", err
		}
		// Map Badger's transaction conflict to durablestream conflict.
		// This can happen when concurrent writers race to update the lastSeq key.
		// The client should retry the operation.
		if errors.Is(err, badger.ErrConflict) {
			return "", fmt.Errorf("badgerstore: concurrent write conflict: %w", durablestream.ErrConflict)
		}
		return "", err
	}

	// Notify waiters: close current channel to wake all, then replace it
	state.wake()

	return offsets[len(offsets)-1], nil
}

// mapTransactionSizeError translates Badger's backend-specific atomic-write
// limit into the storage API's payload classification. A JSON request can fit
// within the handler's byte limit yet contain enough tiny messages to exceed
// Badger's per-transaction entry limit; that is a request-size rejection, not
// an internal server failure. Badger aborts the whole transaction, so no part
// of the batch is visible when this error is returned.
func mapTransactionSizeError(err error) error {
	if errors.Is(err, badger.ErrTxnTooBig) {
		return fmt.Errorf("badgerstore: atomic batch exceeds transaction capacity: %w", durablestream.ErrPayloadTooLarge)
	}
	return err
}

func validateMessageBatch(messages [][]byte, allowEmptyBatch bool, maxMessageSize int) error {
	if len(messages) == 0 && !allowEmptyBatch {
		return fmt.Errorf("badgerstore: empty append batch: %w", durablestream.ErrBadRequest)
	}
	for i, message := range messages {
		if len(message) == 0 {
			return fmt.Errorf("badgerstore: empty message at batch index %d: %w", i, durablestream.ErrBadRequest)
		}
		if len(message) > maxMessageSize {
			return fmt.Errorf("badgerstore: message %d too large (%d > %d): %w",
				i, len(message), maxMessageSize, durablestream.ErrPayloadTooLarge)
		}
	}
	return nil
}

// generationFor returns the current persisted generation of a stream. It does
// not infer liveness from notification state: that state is generation-scoped
// and may legitimately outlive the config transaction briefly while deletion
// waiters drain.
func (s *Storage) generationFor(streamID string) (generation, error) {
	var gen generation
	err := s.view(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() {
			return durablestream.ErrNotFound
		}
		gen = rec.Gen
		return nil
	})
	if err != nil {
		return "", err
	}
	return gen, nil
}

// expiryForGeneration returns the current deadline for one exact incarnation.
// WaitForData uses it after an empty read to arm an expiry wakeup. Touch wakes
// the same generation's notification state when it moves this deadline.
func (s *Storage) expiryForGeneration(streamID string, gen generation) (time.Time, error) {
	var expiresAt time.Time
	err := s.view(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() || rec.Gen != gen {
			return durablestream.ErrNotFound
		}
		expiresAt = rec.ExpiresAt
		return nil
	})
	return expiresAt, err
}

// notificationState returns the shared waiter state for one exact stream
// generation. Different incarnations of a reused stream ID never share a
// channel or replace one another in the map.
func (s *Storage) notificationState(streamID string, gen generation) *streamState {
	state, _ := s.streams.LoadOrStore(streamStateKey(streamID, gen), &streamState{
		notifyCh: make(chan struct{}),
		gen:      gen,
	})
	return state
}

// dropNotificationState removes state only if the map still contains the exact
// pointer the caller observed. A concurrent waiter may have installed a fresh
// state for the same generation after an earlier removal; in that case the
// caller must not close the fresh channel underneath it.
func (s *Storage) dropNotificationState(streamID string, gen generation, state *streamState) {
	if s.streams.CompareAndDelete(streamStateKey(streamID, gen), state) {
		state.markDeleted()
	}
}

// forgetStream drops all in-memory state for a dead generation of a stream and
// wakes any waiters so they observe the deletion.
//
// The generation's Badger sequence is dropped without Release: the generation
// is gone and the reaper deletes its sequence key, so releasing would only
// resurrect that key.
func (s *Storage) forgetStream(streamID string, gen generation) {
	// Serialize cache deletion with sequence creation. If creation is already in
	// progress, this waits and removes the pointer it installs; if deletion won
	// first, getOrCreateSequence's persisted-generation check refuses to install
	// a sequence for the dead generation.
	s.seqCreateMu.Lock()
	s.seqs.Delete(string(seqKey(streamID, gen)))
	s.seqCreateMu.Unlock()

	if state, ok := s.streams.Load(streamStateKey(streamID, gen)); ok {
		s.dropNotificationState(streamID, gen, state)
	}
}

// getOrCreateSequence returns the Badger sequence for one generation of a
// stream, creating it if needed. Uses lock-free hashtriemap for O(1) lookups,
// with mutex protection for creation to prevent race conditions that could
// cause duplicate offsets.
func (s *Storage) getOrCreateSequence(streamID string, gen generation) (*badger.Sequence, error) {
	key := seqKey(streamID, gen)
	cacheKey := string(key)

	// Fast path: sequence already exists (lock-free)
	if seq, ok := s.seqs.Load(cacheKey); ok {
		return seq, nil
	}

	// Slow path: acquire mutex for creation to prevent race condition
	// where multiple goroutines create sequences and some sequence numbers
	// are lost when the "losing" sequences are released.
	s.seqCreateMu.Lock()
	defer s.seqCreateMu.Unlock()

	// Double-check after acquiring lock - another goroutine may have created it
	if seq, ok := s.seqs.Load(cacheKey); ok {
		return seq, nil
	}

	// The stream may have been deleted while this append waited for sequence
	// creation. Validate under seqCreateMu so forgetStream either observes and
	// removes the sequence we install below, or completes first and makes this
	// check fail. This prevents dead generations from accumulating in the cache.
	if err := s.view(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() || rec.Gen != gen {
			return errGenerationChanged
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Create new sequence - now safe from races
	var seq *badger.Sequence
	if err := s.withDB(func() error {
		var err error
		seq, err = s.db.GetSequence(key, 100)
		return err
	}); err != nil {
		return nil, err
	}

	s.seqs.Store(cacheKey, seq)
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

// Read returns messages from offset (Section 5.6).
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
	return s.readGeneration(ctx, streamID, "", offset, limit)
}

// readGeneration performs a read and, when wantGen is non-empty, refuses to
// cross into a different incarnation of the same stream ID. WaitForData uses
// this to remain bound to the generation it originally observed.
func (s *Storage) readGeneration(ctx context.Context, streamID string, wantGen generation, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	var result *durablestream.ReadResult

	err := s.view(func(txn *badger.Txn) error {
		// Check for context cancellation
		if err := ctx.Err(); err != nil {
			return err
		}

		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() {
			return durablestream.ErrNotFound
		}
		if wantGen != "" && rec.Gen != wantGen {
			return durablestream.ErrNotFound
		}

		// Find the tail offset (highest message offset)
		tailOffset, err := getTailOffset(txn, streamID, rec.Gen)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}

		// Collect messages starting after the requested offset
		// Gaps are handled naturally: we seek to the start position and iterate
		// forward, returning whatever messages exist.
		var messages []durablestream.StoredMessage
		totalBytes := 0

		prefix := messagePrefix(streamID, rec.Gen)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix

		it := txn.NewIterator(opts)
		defer it.Close()

		// Parse offset to calculate start position
		_, byteOffset, err := storage.ParseOffset(offset)
		if err != nil {
			return err
		}
		// MaxInt64 has no representable successor. It is necessarily at or
		// beyond every offset this implementation can generate, so leave the
		// iterator unpositioned and return an empty page instead of overflowing
		// to a negative seek key and replaying the stream from the beginning.
		if byteOffset != math.MaxInt64 {
			startOffset := storage.FormatSimpleOffset(byteOffset + 1)
			it.Seek(messageKey(streamID, rec.Gen, startOffset))
		}

		for byteOffset != math.MaxInt64 && it.Valid() {
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
			Messages:      messages,
			NextOffset:    nextOffset,
			TailOffset:    tailOffset,
			IncarnationID: string(rec.Gen),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Head returns stream metadata (Section 5.5).
func (s *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}

	var info *durablestream.StreamInfo

	err := s.view(func(txn *badger.Txn) error {
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found || !rec.isUsable() {
			return durablestream.ErrNotFound
		}

		tailOffset, err := getTailOffset(txn, streamID, rec.Gen)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}

		info = &durablestream.StreamInfo{
			ContentType:   rec.ContentType,
			NextOffset:    tailOffset,
			TTL:           rec.TTL,
			ExpiresAt:     rec.ExpiresAt,
			IsPrivate:     rec.IsPrivate,
			IncarnationID: string(rec.Gen),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return info, nil
}

// Touch restarts the stream's sliding TTL window (Section 5.1).
//
// The window lives in the persisted config record, so extending it is a durable
// write. The protocol requires each activity to reset expiry to exactly
// now+TTL; batching those writes by adding deadline slack would let an idle
// stream outlive its configured TTL.
func (s *Storage) Touch(ctx context.Context, streamID string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}

	// Create and Delete write the same config key, so this transaction can lose
	// a conflict to either; retrying settles it against whichever record is
	// committed by then, and a Touch that then finds the stream gone reports it.
	var gen generation
	var moved bool
	err := s.updateWithRetry(func(txn *badger.Txn) error {
		gen, moved = "", false
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		// An already-expired stream stays expired: sliding the window here would
		// resurrect a stream that Read, Append and Head have been reporting as
		// absent, and that Create is entitled to replace.
		if !found || !rec.isUsable() {
			return durablestream.ErrNotFound
		}

		cfg, didMove := rec.SlideExpiry(time.Now())
		if !didMove {
			return nil
		}
		gen = rec.Gen
		// Assign the outer result only on the branch that persists a renewal.
		// updateWithRetry resets it before every attempt.
		moved = true
		rec.StreamConfig = cfg

		encoded, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("badgerstore: marshal config: %w", err)
		}
		if err := txn.Set(configKey(streamID), encoded); err != nil {
			return fmt.Errorf("badgerstore: set config: %w", err)
		}
		return nil
	})
	if err != nil || !moved {
		return err
	}
	// A waiter may have armed a timer for the previous deadline. Do not create
	// notification state solely for Touch, but wake existing waiters so they
	// reload the renewed deadline.
	if state, ok := s.streams.Load(streamStateKey(streamID, gen)); ok {
		state.wake()
	}
	return nil
}

// Delete removes a stream (Section 5.4).
//
// The stream's configuration and metadata are removed in a single transaction
// that also records a tombstone; the message data is purged asynchronously by
// the reaper. Once that transaction commits the stream is gone as far as
// callers are concerned, so Delete reports no error after that point.
func (s *Storage) Delete(ctx context.Context, streamID string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}

	// Delete and a concurrent Create of the same stream ID both write the
	// config key, so this transaction can lose a conflict; retrying settles it
	// against whichever record is committed by then.
	var found bool
	var gen generation
	err := s.updateWithRetry(func(txn *badger.Txn) error {
		found, gen = false, ""
		rec, ok, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		found = true
		gen = rec.Gen

		if err := txn.Delete(configKey(streamID)); err != nil {
			return fmt.Errorf("badgerstore: delete config: %w", err)
		}
		if err := txn.Delete(lastSeqKey(streamID)); err != nil {
			return fmt.Errorf("badgerstore: delete last seq: %w", err)
		}
		// Tombstone drives the asynchronous purge of messages and the
		// sequence key for this generation.
		if err := txn.Set(tombstoneKey(streamID, gen), nil); err != nil {
			return fmt.Errorf("badgerstore: set tombstone: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return durablestream.ErrNotFound
	}

	// Wake WaitForData callers AFTER the Badger commit, so Read() returns
	// ErrNotFound when they re-check. If we closed notifyCh before the
	// transaction, waiters could wake, find the config still exists,
	// and block again on a new channel nobody will close.
	s.forgetStream(streamID, gen)
	s.signalReaper()

	return nil
}

// WaitForData blocks until data is available at offset, then returns it.
// Returns immediately if data already exists at offset.
// Returns ctx.Err() on timeout/cancellation.
// Returns ErrNotFound if stream doesn't exist or is deleted while waiting.
// Returns ErrClosed if the storage is closed while waiting.
func (s *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("badgerstore: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	gen, err := s.generationFor(streamID)
	if err != nil {
		return nil, err
	}
	state := s.notificationState(streamID, gen)
	return s.waitForGeneration(ctx, streamID, gen, state, offset, limit)
}

// waitForGeneration waits on one captured stream incarnation for the lifetime
// of the call. A delete followed by a fast recreate therefore releases the old
// waiter with ErrNotFound instead of letting it consume the replacement's data.
func (s *Storage) waitForGeneration(ctx context.Context, streamID string, gen generation, state *streamState, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	for {
		state.mu.RLock()
		deleted := state.deleted
		closed := state.closed
		notifyCh := state.notifyCh
		state.mu.RUnlock()
		if deleted {
			return nil, durablestream.ErrNotFound
		}
		if closed {
			return nil, ErrClosed
		}

		// Try to read data
		result, err := s.readGeneration(ctx, streamID, gen, offset, limit)
		if err != nil {
			if errors.Is(err, durablestream.ErrNotFound) {
				// Delete may have committed before this waiter installed its state,
				// in which case forgetStream could not remove it. Clean up the exact
				// pointer without disturbing a newer incarnation.
				s.dropNotificationState(streamID, gen, state)
			}
			return nil, err
		}
		if len(result.Messages) > 0 {
			return result, nil
		}
		expiresAt, err := s.expiryForGeneration(streamID, gen)
		if err != nil {
			if errors.Is(err, durablestream.ErrNotFound) {
				s.dropNotificationState(streamID, gen, state)
			}
			return nil, err
		}

		// No data available: expiry makes the incarnation absent and must release
		// a waiter even when no append or delete occurs.
		var expiryTimer *time.Timer
		var expiryCh <-chan time.Time
		if !expiresAt.IsZero() {
			expiryTimer = time.NewTimer(time.Until(expiresAt))
			expiryCh = expiryTimer.C
		}
		select {
		case <-ctx.Done():
			stopTimer(expiryTimer)
			return nil, ctx.Err()
		case <-s.shutdownCtx.Done():
			stopTimer(expiryTimer)
			return nil, ErrClosed
		case <-notifyCh:
			stopTimer(expiryTimer)
			// New data, deletion, or close — loop to re-check
		case <-expiryCh:
			// Loop so the persisted record is the authoritative expiry check.
		}
	}
}

// Helper methods

// getRecord loads a stream's persisted record. It reports found=false when no
// record exists; callers must additionally check isUsable for expiry.
func getRecord(txn *badger.Txn, streamID string) (streamRecord, bool, error) {
	item, err := txn.Get(configKey(streamID))
	if err == badger.ErrKeyNotFound {
		return streamRecord{}, false, nil
	}
	if err != nil {
		return streamRecord{}, false, fmt.Errorf("badgerstore: get config: %w", err)
	}

	var rec streamRecord
	if err := item.Value(func(val []byte) error {
		return json.Unmarshal(val, &rec)
	}); err != nil {
		return streamRecord{}, false, fmt.Errorf("badgerstore: unmarshal config: %w", err)
	}
	return rec, true, nil
}

func (s *Storage) getLastSeq(txn *badger.Txn, streamID string) (string, error) {
	item, err := txn.Get(lastSeqKey(streamID))
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

// getTailOffset returns the highest message offset for a stream generation.
// Uses reverse iteration to find the last key efficiently.
// Returns formatted zero offset for empty streams.
func getTailOffset(txn *badger.Txn, streamID string, gen generation) (durablestream.Offset, error) {
	prefix := messagePrefix(streamID, gen)

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
