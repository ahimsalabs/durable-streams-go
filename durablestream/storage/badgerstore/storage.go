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
// machine crash. Concurrent independent appends share bounded synchronous
// Badger transactions, amortizing one WAL sync without making any mutation
// visible before that shared commit succeeds. Disabling SyncWrites trades the
// durability guarantee for throughput: acknowledged appends can then be lost
// if the process is killed. In-memory mode has no durability at all.
//
// # Forks and deletion
//
// Forks store a generation-fenced pointer to their immediate parent and read
// the inherited prefix in place. Append-batch boundaries are persisted beside
// messages so JSON sub-offsets retain their meaning after restart.
//
// Delete normally removes a stream's configuration atomically and records a
// tombstone; message and batch data are purged afterwards by a background
// reaper. A stream that still has child forks is instead soft-deleted and kept
// internally readable until its final descendant reference is released. An
// expired referenced stream is retained the same way, so its path cannot be
// reused until the descendant chain is removed. Each incarnation has a unique
// generation, so a slow or crash-resumed purge can never destroy data belonging
// to a later stream. The reaper also sweeps orphaned data at startup.
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
// Record format version 1 includes generation-scoped messages, durable fork
// lineage and reference counts, and atomic append-batch boundaries. A directory
// containing any older stream record is rejected with [ErrLegacyFormat] before
// background cleanup starts. In particular, generation-scoped records written
// before batch metadata cannot be upgraded in place: the grouping needed for a
// JSON fork sub-offset cannot be reconstructed from individual messages.
//
// New also rejects malformed generation fences, reference counts, or cyclic
// lineage. Every rejected open closes Badger and leaves all bytes intact so the
// operator can inspect, drain, or discard them; this package never guesses at
// durable history and ships no migration tool (see [ErrLegacyFormat]).
package badgerstore

import (
	"context"
	"encoding/binary"
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

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
	_ durablestream.AtomicCloseStorage = (*Storage)(nil)
	_ durablestream.ForkStorage        = (*Storage)(nil)
	_ durablestream.TouchHeadStorage   = (*Storage)(nil)
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
// records that do not use this package's complete versioned lineage and batch
// format. New closes the database and leaves every incompatible key intact.
// No in-place migration exists: legacy records lack the batch-boundary
// metadata this format requires, so callers must either discard the directory
// or drain its streams through the protocol using the binary that wrote it.
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

const currentRecordFormatVersion = 1

// parentReference is the immutable edge from a fork to the exact source
// incarnation it inherited. Offset is the last source position visible to the
// fork; any materialized sub-offset prefix belongs to the fork's own
// generation at later offsets.
type parentReference struct {
	StreamID           string               `json:"streamId"`
	Gen                generation           `json:"gen"`
	Offset             durablestream.Offset `json:"offset"`
	OffsetSet          bool                 `json:"offsetSet"`
	SubOffset          uint64               `json:"subOffset,omitempty"`
	ContentTypeSet     bool                 `json:"contentTypeSet,omitempty"`
	TTLSet             bool                 `json:"ttlSet,omitempty"`
	ExpiresAtSet       bool                 `json:"expiresAtSet,omitempty"`
	RequestedTTL       time.Duration        `json:"requestedTtl,omitempty"`
	RequestedExpiresAt time.Time            `json:"requestedExpiresAt,omitempty"`
}

// streamRecord is the persisted stream metadata. Parent and RefCount form a
// durable, generation-fenced lineage graph: every live or retained fork owns
// exactly one reference to its immediate parent until the fork is physically
// removed.
type streamRecord struct {
	durablestream.StreamConfig
	FormatVersion int              `json:"formatVersion"`
	Gen           generation       `json:"gen"`
	Parent        *parentReference `json:"parent,omitempty"`
	RefCount      uint64           `json:"refCount,omitempty"`
	SoftDeleted   bool             `json:"softDeleted,omitempty"`
}

// isLegacy reports whether the record predates the complete lineage and batch
// metadata required by this version. New rejects such databases before any
// background reaper can mistake their bytes for orphaned data.
func (r streamRecord) isLegacy() bool {
	return r.FormatVersion != currentRecordFormatVersion || !validPersistedGeneration(r.Gen)
}

func newStreamRecord(cfg durablestream.StreamConfig, gen generation) streamRecord {
	return streamRecord{
		StreamConfig:  cfg,
		FormatVersion: currentRecordFormatVersion,
		Gen:           gen,
	}
}

// Storage is a Badger-backed implementation of durablestream.Storage.
type Storage struct {
	db *badger.DB

	// appendCommits combines independent append mutations into bounded Badger
	// transactions. It is enabled only for durable SyncWrites mode, where one
	// transaction lets every request in the group share a single WAL sync.
	// Unsynced and in-memory stores commit directly to avoid needless queuing.
	appendCommits *appendCommitter

	// In-memory notification tracking (ephemeral, not persisted). Entries are
	// keyed by stream ID plus generation, so deleting an old generation can
	// never remove or wake the state of a replacement generation.
	streams hashtriemap.HashTrieMap[string, *streamState]

	// gens caches each stream's current generation so the append hot path can
	// skip a read transaction and JSON record decode per request. Entries may
	// be stale: appends re-validate the generation inside their transaction and
	// retry on mismatch, so a stale hit costs one retry, never correctness.
	// Only the append path consumes this cache; reads stay authoritative.
	gens hashtriemap.HashTrieMap[string, generation]

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
	// Badger's expvar counters cost measurable atomic traffic on the append
	// hot path (~5% CPU at saturation) and nothing here consumes them.
	badgerOpts = badgerOpts.WithMetricsEnabled(false)
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
	if err := validatePersistedFormat(db); err != nil {
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
	if syncWrites && !useInMemory {
		commitCfg := defaultAppendCommitConfig()
		if opts.AppendCommitMaxInFlight > 0 {
			commitCfg.maxInFlight = opts.AppendCommitMaxInFlight
		}
		s.appendCommits = newAppendCommitter(s, commitCfg)
		go s.appendCommits.run()
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
		if s.appendCommits != nil {
			// Closing admission before the database prevents a sender from
			// enqueueing onto a coordinator that has already exited. Accepted
			// requests either finish an in-progress transaction or receive
			// ErrClosed while the coordinator drains.
			s.appendCommits.close()
		}

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
			if s.appendCommits != nil {
				<-s.appendCommits.done
			}
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

// shutdownDB closes the database and removes the temp directory. It runs
// exactly once, after all background goroutines and the append committer exit.
//
// The write lock makes the close exclusive with every in-flight operation:
// callers that already passed the closed check finish first, and later ones
// see the closed flag under the read lock and get ErrClosed.
func (s *Storage) shutdownDB() error {
	s.dbMu.Lock()
	defer s.dbMu.Unlock()

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
	newTail := storage.FormatSimpleOffset(int64(len(messages)))
	var sequenceValue [8]byte
	binary.BigEndian.PutUint64(sequenceValue[:], uint64(len(messages)))

	var (
		created    bool
		nextOffset durablestream.Offset
		changes    topologyChanges
		committed  error
	)
	commit := func(txn *badger.Txn) error {
		created, nextOffset, changes, committed = false, "", topologyChanges{}, nil
		existing, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if found {
			if existing.isLegacy() {
				return fmt.Errorf("badgerstore: stream %q uses an incompatible format: %w", streamID, ErrLegacyFormat)
			}
			if existing.SoftDeleted {
				return fmt.Errorf("badgerstore: stream %q is soft-deleted: %w", streamID, durablestream.ErrConflict)
			}
			if !existing.IsExpired() {
				// A regular PUT cannot silently confirm a fork: lineage is part of
				// the target's creation configuration.
				if existing.Parent == nil && existing.Matches(cfg) {
					created = false
					nextOffset, err = getTailOffset(txn, streamID, existing.Gen)
					return err
				}
				return fmt.Errorf("badgerstore: stream exists with different config: %w", durablestream.ErrConflict)
			}

			// An expired source with children cannot be replaced at the same
			// path. Retain it internally and make the blocked path explicit.
			if existing.RefCount > 0 {
				existing.SoftDeleted = true
				if err := setRecord(txn, streamID, existing); err != nil {
					return err
				}
				changes.softened = append(changes.softened, streamGeneration{streamID: streamID, gen: existing.Gen})
				committed = fmt.Errorf("badgerstore: expired stream %q is retained by forks: %w", streamID, durablestream.ErrConflict)
				return nil
			}
			changes, err = removeRecordCascade(txn, streamID, existing)
			if err != nil {
				return err
			}
		}

		// Create new stream
		if err := setRecord(txn, streamID, newStreamRecord(cfg, gen)); err != nil {
			return err
		}
		for i, message := range messages {
			offset := storage.FormatSimpleOffset(int64(i + 1))
			if err := txn.Set(messageKey(streamID, gen, offset), message); err != nil {
				return fmt.Errorf("badgerstore: set initial message %d: %w", i, err)
			}
		}
		if err := setBatchBoundary(txn, streamID, gen, offsetsForContiguousRange(1, len(messages))); err != nil {
			return fmt.Errorf("badgerstore: set initial batch boundary: %w", err)
		}
		if len(messages) > 0 {
			// The high-water stores the last allocated position. Initial messages
			// occupy offsets 1..N, so storing N makes the next append start at N+1.
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
	s.publishTopologyChanges(changes)
	if committed != nil {
		return false, "", committed
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
		offset, err := s.appendBatchOnce(streamID, messages, seq, false)
		if errors.Is(err, errGenerationChanged) && attempt < appendAttempts-1 {
			continue
		}
		if errors.Is(err, errGenerationChanged) {
			return "", durablestream.ErrNotFound
		}
		return offset, err
	}
}

// CloseStream atomically appends an optional final batch and marks the stream
// closed. An empty final batch makes this operation idempotent; once closed,
// every append with data is rejected with durablestream.ErrStreamClosed.
func (s *Storage) CloseStream(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return "", err
	}
	if err := validateMessageBatch(messages, true, s.maxMessageSize); err != nil {
		return "", err
	}

	for attempt := 0; ; attempt++ {
		offset, err := s.appendBatchOnce(streamID, messages, seq, true)
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
func (s *Storage) appendBatchOnce(streamID string, messages [][]byte, seq string, closeStream bool) (durablestream.Offset, error) {
	gen, err := s.cachedGenerationFor(streamID)
	if err != nil {
		return "", err
	}
	state := s.notificationState(streamID, gen)
	state.appendMu.Lock()
	defer state.appendMu.Unlock()

	request := newAppendCommitRequest(streamID, gen, messages, seq, closeStream)
	var result appendCommitResult
	if s.appendCommits != nil {
		result = s.appendCommits.submit(request)
	} else {
		results, commitErr := s.commitAppendRequests([]*appendCommitRequest{request})
		if commitErr != nil {
			result.err = commitErr
		} else {
			result = results[0]
		}
	}
	err = result.err
	if err != nil {
		// Any failure may mean the cached generation is dead (deleted,
		// replaced, or expired). Dropping it is cheap and makes the next
		// attempt re-read the authoritative record.
		s.gens.CompareAndDelete(streamID, gen)
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

	// Wake after the entire transaction commits. For append-and-close this makes
	// the final messages visible before waiters can observe the closed state.
	state.wake()

	return result.offset, nil
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
		if !found {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
		}
		gen = rec.Gen
		return nil
	})
	if err != nil {
		return "", err
	}
	return gen, nil
}

// cachedGenerationFor returns the stream's generation from the in-memory
// cache, falling back to the persisted record on a miss. Callers must
// re-validate the generation transactionally before trusting it: the cache is
// only dropped on append failure and stream removal, so it can briefly serve a
// generation that a concurrent delete or recreate has already replaced.
func (s *Storage) cachedGenerationFor(streamID string) (generation, error) {
	if gen, ok := s.gens.Load(streamID); ok {
		return gen, nil
	}
	gen, err := s.generationFor(streamID)
	if err != nil {
		return "", err
	}
	s.gens.Store(streamID, gen)
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
		if !found || rec.Gen != gen {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
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
func (s *Storage) forgetStream(streamID string, gen generation) {
	// Only the exact dead generation is evicted; a replacement incarnation may
	// already have cached its own generation under the same stream ID.
	s.gens.CompareAndDelete(streamID, gen)
	if state, ok := s.streams.Load(streamStateKey(streamID, gen)); ok {
		s.dropNotificationState(streamID, gen, state)
	}
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
		if !found {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
		}
		if wantGen != "" && rec.Gen != wantGen {
			return durablestream.ErrNotFound
		}

		result, err = readLogicalStream(ctx, txn, streamID, rec, offset, limit)
		return err
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
		if !found {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
		}

		tailOffset, err := getTailOffset(txn, streamID, rec.Gen)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}
		lastSeq, err := s.getLastSeq(txn, streamID)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("badgerstore: get last seq: %w", err)
		}

		info = &durablestream.StreamInfo{
			ContentType:   rec.ContentType,
			NextOffset:    tailOffset,
			LastSeq:       lastSeq,
			TTL:           rec.TTL,
			ExpiresAt:     rec.ExpiresAt,
			IsPrivate:     rec.IsPrivate,
			Closed:        rec.Closed,
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
		if !found {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
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

// TouchHead snapshots stream metadata and restarts the sliding TTL window in
// one transaction. It is Head and Touch fused: handlers call the pair on every
// origin-reaching request, and separately each call costs a transaction and a
// record decode. The returned ExpiresAt is the pre-renewal deadline, matching
// what a separate Head would have reported.
func (s *Storage) TouchHead(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}

	// Create and Delete write the same config key, so this transaction can lose
	// a conflict to either; retrying settles it against whichever record is
	// committed by then. See Touch for the sliding-window rules preserved here.
	var info *durablestream.StreamInfo
	var gen generation
	var moved bool
	err := s.updateWithRetry(func(txn *badger.Txn) error {
		info, gen, moved = nil, "", false
		rec, found, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !found {
			return durablestream.ErrNotFound
		}
		if err := directRecordError(rec); err != nil {
			return err
		}

		tailOffset, err := getTailOffset(txn, streamID, rec.Gen)
		if err != nil {
			return fmt.Errorf("badgerstore: get tail offset: %w", err)
		}
		lastSeq, err := s.getLastSeq(txn, streamID)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("badgerstore: get last seq: %w", err)
		}
		info = &durablestream.StreamInfo{
			ContentType:   rec.ContentType,
			NextOffset:    tailOffset,
			LastSeq:       lastSeq,
			TTL:           rec.TTL,
			ExpiresAt:     rec.ExpiresAt,
			IsPrivate:     rec.IsPrivate,
			Closed:        rec.Closed,
			IncarnationID: string(rec.Gen),
		}

		cfg, didMove := rec.SlideExpiry(time.Now())
		if !didMove {
			// TTL-less (or absolute-expiry) streams write nothing: the commit of
			// a read-only update transaction is free, so the fused call costs
			// the same as Head alone.
			return nil
		}
		gen = rec.Gen
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
	if err != nil {
		return nil, err
	}
	if moved {
		// Wake existing waiters armed against the previous deadline, exactly as
		// Touch does. No notification state is created solely for a renewal.
		if state, ok := s.streams.Load(streamStateKey(streamID, gen)); ok {
			state.wake()
		}
	}
	return info, nil
}

// Delete removes a stream (Section 5.4).
//
// A stream with children is retained as soft-deleted; otherwise its record is
// removed, its parent reference is released, and any now-unreferenced retained
// ancestors are removed in the same transaction. Message bytes are reaped
// asynchronously from generation-scoped tombstones.
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
	var (
		found   bool
		changes topologyChanges
	)
	err := s.updateWithRetry(func(txn *badger.Txn) error {
		found, changes = false, topologyChanges{}
		rec, ok, err := getRecord(txn, streamID)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		found = true
		if rec.isLegacy() {
			return ErrLegacyFormat
		}
		if rec.SoftDeleted {
			return durablestream.ErrSoftDeleted
		}
		if rec.RefCount > 0 {
			rec.SoftDeleted = true
			if err := setRecord(txn, streamID, rec); err != nil {
				return err
			}
			if err := txn.Delete(lastSeqKey(streamID)); err != nil {
				return fmt.Errorf("badgerstore: delete last sequence for soft-deleted stream: %w", err)
			}
			changes.softened = append(changes.softened, streamGeneration{streamID: streamID, gen: rec.Gen})
			return nil
		}
		changes, err = removeRecordCascade(txn, streamID, rec)
		return err
	})
	if err != nil {
		return err
	}
	if !found {
		return durablestream.ErrNotFound
	}

	// Publish only after the topology transaction commits, so waiters re-read
	// the authoritative soft-deleted or absent state.
	s.publishTopologyChanges(changes)

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
		if len(result.Messages) > 0 || result.Closed {
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

// getTailOffset returns the logical tail of the requested current generation,
// including an immutable inherited prefix for forked streams.
func getTailOffset(txn *badger.Txn, streamID string, gen generation) (durablestream.Offset, error) {
	rec, found, err := getRecord(txn, streamID)
	if err != nil {
		return "", err
	}
	if !found || rec.Gen != gen {
		return "", durablestream.ErrNotFound
	}
	return streamTailOffset(txn, streamID, rec)
}
