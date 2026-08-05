package seglog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go4org/hashtriemap"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// ErrClosed is returned for operations on a closed storage. It wraps
// durablestream.ErrClosed so callers can classify it with errors.Is.
var ErrClosed = fmt.Errorf("seglog: storage closed: %w", durablestream.ErrClosed)

// Storage is a partitioned WAL-backed implementation of
// durablestream.Storage. See the package documentation for the design.
type Storage struct {
	opts        Options
	dir         string
	ephemeral   bool
	releaseLock func() error

	streams hashtriemap.HashTrieMap[string, *streamState]
	parts   []*partition

	// shutdownCh releases WaitForData callers when the storage closes.
	shutdownCh chan struct{}
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
	workers    sync.WaitGroup
}

// Compile-time capability assertions.
var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
	_ durablestream.AtomicCloseStorage = (*Storage)(nil)
)

// New opens (or initializes) a seglog storage rooted at opts.Dir, recovering
// any existing WAL before serving requests. On recovery failure the directory
// is left byte-for-byte intact.
func New(opts Options) (_ *Storage, retErr error) {
	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	dir := opts.Dir
	ephemeral := false
	if dir == "" {
		tmp, err := os.MkdirTemp("", "seglog-*")
		if err != nil {
			return nil, fmt.Errorf("seglog: create ephemeral dir: %w", err)
		}
		dir = tmp
		ephemeral = true
	} else if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("seglog: create dir: %w", err)
	}
	defer func() {
		if retErr != nil && ephemeral {
			_ = os.RemoveAll(dir)
		}
	}()

	release, err := lockDir(dir)
	if err != nil {
		return nil, fmt.Errorf("seglog: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = release()
		}
	}()

	syncWrites, err := opts.SyncWrites.enabled()
	if err != nil {
		return nil, err
	}
	if err := checkFormat(dir, opts.Partitions); err != nil {
		return nil, err
	}

	s := &Storage{
		opts:        opts,
		dir:         dir,
		ephemeral:   ephemeral,
		releaseLock: release,
		shutdownCh:  make(chan struct{}),
	}
	s.parts = make([]*partition, opts.Partitions)
	for i := range s.parts {
		walDir := filepath.Join(dir, "wal", fmt.Sprintf("p%04d", i))
		w := newWALWriter(walDir, uint32(i), opts.WALSegmentBytes, syncWrites)
		s.parts[i] = newPartition(uint32(i), s, w)
	}
	defer func() {
		if retErr != nil {
			for _, p := range s.parts {
				_ = p.wal.close()
			}
		}
	}()

	if err := s.recoverAll(); err != nil {
		return nil, err
	}
	for _, p := range s.parts {
		s.workers.Go(p.run)
		if opts.MaterializeInterval != -1 {
			s.workers.Go(func() { s.runMaterializer(p) })
		}
	}
	return s, nil
}

const formatVersionLine = "seglog-format-v1"

// checkFormat validates or initializes the root FORMAT file. The persisted
// partition count is authoritative (invariant I4): opening with a different
// Options.Partitions fails rather than silently rehashing streams.
func checkFormat(dir string, partitions int) error {
	path := filepath.Join(dir, "FORMAT")
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		content := fmt.Sprintf("%s\npartitions=%d\n", formatVersionLine, partitions)
		if err := atomicWrite(path, []byte(content), 0o644); err != nil {
			return fmt.Errorf("seglog: initialize FORMAT: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("seglog: read FORMAT: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 2 || lines[0] != formatVersionLine {
		return fmt.Errorf("seglog: unsupported format %q in %s", strings.TrimSpace(string(raw)), path)
	}
	value, ok := strings.CutPrefix(lines[1], "partitions=")
	if !ok {
		return fmt.Errorf("seglog: malformed FORMAT line %q in %s", lines[1], path)
	}
	persisted, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("seglog: malformed partition count in %s: %w", path, err)
	}
	if persisted != partitions {
		return fmt.Errorf("seglog: directory was created with %d partitions, cannot open with %d", persisted, partitions)
	}
	return nil
}

func (s *Storage) checkClosed() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (s *Storage) partitionFor(streamID string) *partition {
	return s.parts[streamHash(streamID)%uint64(len(s.parts))]
}

func validateStreamID(streamID string) error {
	if streamID == "" {
		return fmt.Errorf("seglog: stream ID cannot be empty: %w", durablestream.ErrBadRequest)
	}
	if len(streamID) > maxStreamIDLen {
		return fmt.Errorf("seglog: stream ID exceeds %d bytes: %w", maxStreamIDLen, durablestream.ErrBadRequest)
	}
	return nil
}

func notFoundErr(streamID string) error {
	return fmt.Errorf("seglog: stream %q not found: %w", streamID, durablestream.ErrNotFound)
}

// validateBatch checks every message against per-message and aggregate
// limits. The aggregate bound is the WAL segment capacity: one logical
// mutation is one frame and a frame never spans segments. metaBound must be
// the same overestimate group admission uses (estimateFrameBytes), so any
// admitted request is guaranteed to encode within one segment.
func (s *Storage) validateBatch(streamID string, messages [][]byte, allowEmptyBatch bool, metaBound int) error {
	if len(messages) == 0 {
		if allowEmptyBatch {
			return nil
		}
		return fmt.Errorf("seglog: batch cannot be empty: %w", durablestream.ErrBadRequest)
	}
	for i, msg := range messages {
		if len(msg) == 0 {
			return fmt.Errorf("seglog: message %d is empty: %w", i, durablestream.ErrBadRequest)
		}
		if len(msg) > s.opts.MaxMessageSize {
			return fmt.Errorf("seglog: message %d of %d bytes exceeds limit %d: %w",
				i, len(msg), s.opts.MaxMessageSize, durablestream.ErrPayloadTooLarge)
		}
	}
	capacity := s.opts.WALSegmentBytes - walSegmentHeaderSize
	if size := encodedFrameSize(len(streamID), metaBound, messages); size > capacity {
		return fmt.Errorf("seglog: batch of %d bytes exceeds the %d-byte transaction capacity: %w",
			size, capacity, durablestream.ErrPayloadTooLarge)
	}
	return nil
}

// Create implements durablestream.Storage.
func (s *Storage) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	created, _, err := s.CreateWithMessages(ctx, streamID, cfg, nil)
	return created, err
}

// CreateWithMessages implements durablestream.AtomicBatchStorage.
func (s *Storage) CreateWithMessages(ctx context.Context, streamID string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return false, "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return false, "", err
	}
	if err := s.validateBatch(streamID, messages, true, metaBoundForCreate(cfg.ContentType)); err != nil {
		return false, "", err
	}
	res := s.partitionFor(streamID).submit(&request{
		op:       opCreate,
		streamID: streamID,
		cfg:      cfg,
		messages: messages,
		done:     make(chan result, 1),
	})
	return res.created, res.offset, res.err
}

// Append implements durablestream.Storage.
func (s *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("seglog: empty data: %w", durablestream.ErrBadRequest)
	}
	return s.append(streamID, [][]byte{data}, seq, false, false)
}

// AppendBatch implements durablestream.AtomicBatchStorage.
func (s *Storage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	return s.append(streamID, messages, seq, false, false)
}

// CloseStream implements durablestream.AtomicCloseStorage.
func (s *Storage) CloseStream(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	return s.append(streamID, messages, seq, true, true)
}

func (s *Storage) append(streamID string, messages [][]byte, seq string, closeAfter, allowEmptyBatch bool) (durablestream.Offset, error) {
	if err := s.checkClosed(); err != nil {
		return "", err
	}
	if err := validateStreamID(streamID); err != nil {
		return "", err
	}
	if err := s.validateBatch(streamID, messages, allowEmptyBatch, len(seq)); err != nil {
		return "", err
	}
	res := s.partitionFor(streamID).submit(&request{
		op:       opAppend,
		streamID: streamID,
		messages: messages,
		seq:      seq,
		hasSeq:   seq != "",
		close:    closeAfter,
		done:     make(chan result, 1),
	})
	return res.offset, res.err
}

// Delete implements durablestream.Storage.
func (s *Storage) Delete(ctx context.Context, streamID string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}
	res := s.partitionFor(streamID).submit(&request{
		op:       opDelete,
		streamID: streamID,
		done:     make(chan result, 1),
	})
	return res.err
}

// Touch implements durablestream.Storage.
func (s *Storage) Touch(ctx context.Context, streamID string) error {
	if err := s.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}
	res := s.partitionFor(streamID).submit(&request{
		op:       opTouch,
		streamID: streamID,
		done:     make(chan result, 1),
	})
	return res.err
}

// Head implements durablestream.Storage.
func (s *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	state, ok := s.streams.Load(streamID)
	if !ok {
		return nil, notFoundErr(streamID)
	}
	snap := state.snapshot()
	if snap.deleted || snap.cfg.IsExpired() {
		return nil, notFoundErr(streamID)
	}
	return &durablestream.StreamInfo{
		ContentType:   snap.cfg.ContentType,
		NextOffset:    storage.FormatSimpleOffset(snap.tail),
		TTL:           snap.cfg.TTL,
		ExpiresAt:     snap.cfg.ExpiresAt,
		IsPrivate:     snap.cfg.IsPrivate,
		Closed:        snap.closed,
		IncarnationID: snap.inc.String(),
	}, nil
}

// Close implements durablestream.Storage. It stops admission, drains the
// partition workers, releases every WaitForData caller with ErrClosed, and
// closes all files. Later calls are no-ops returning the first result.
func (s *Storage) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		for _, p := range s.parts {
			p.closeAdmission()
		}
		close(s.shutdownCh)

		workersDone := make(chan struct{})
		go func() {
			s.workers.Wait()
			close(workersDone)
		}()
		select {
		case <-workersDone:
			s.closeErr = s.releaseResources()
		case <-time.After(s.opts.ShutdownTimeout):
			// Leave files open rather than close them under a live worker;
			// the deferred goroutine finishes teardown when workers exit.
			s.closeErr = fmt.Errorf("seglog: shutdown timed out after %v", s.opts.ShutdownTimeout)
			go func() {
				<-workersDone
				_ = s.releaseResources()
			}()
		}
	})
	return s.closeErr
}

func (s *Storage) releaseResources() error {
	var firstErr error
	for _, p := range s.parts {
		if err := p.wal.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.streams.Range(func(_ string, st *streamState) bool {
		st.closeSegments()
		return true
	})
	if err := s.releaseLock(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("seglog: release lock: %w", err)
	}
	if s.ephemeral {
		if err := os.RemoveAll(s.dir); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("seglog: remove ephemeral dir: %w", err)
		}
	}
	return firstErr
}
