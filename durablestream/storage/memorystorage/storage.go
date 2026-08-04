// Package memorystorage provides an in-memory implementation of durablestream.Storage.
//
// This implementation uses the reference offset format from the official Node.js
// implementation: "<readSeq>_<byteOffset>" with 16-digit zero-padded integers.
package memorystorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/go4org/hashtriemap"
)

// memoryStream represents a single stream in memory.
type memoryStream struct {
	mu       sync.RWMutex // Per-stream lock for mutations
	config   durablestream.StreamConfig
	messages []durablestream.StoredMessage // All messages in order
	lastSeq  string                        // Last sequence number seen (lexicographic)
	notifyCh chan struct{}                 // Closed on append, replaced with new channel
	deleted  bool                          // True if stream has been deleted
	incID    string                        // Opaque identity of this exact incarnation
}

// markDeleted marks the stream deleted and wakes any waiters. Safe to call more
// than once and from multiple goroutines: only the first call closes notifyCh.
func (s *memoryStream) markDeleted() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.deleted {
		s.deleted = true
		close(s.notifyCh)
	}
}

// Storage is an in-memory implementation of durablestream.Storage.
// Uses hashtriemap for lock-free stream lookups with per-stream locks for mutations.
//
// Offsets use the reference implementation format: "0000000000000000_0000000000000042"
// where the first 16 digits are the read sequence (always 0 for this implementation)
// and the second 16 digits are the cumulative byte offset.
type Storage struct {
	streams hashtriemap.HashTrieMap[string, *memoryStream]

	initOnce  sync.Once
	closeOnce sync.Once
	closedCh  chan struct{} // Closed by Close to release WaitForData callers
}

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
)

// errClosed reports that the storage was closed while a caller was waiting.
var errClosed = fmt.Errorf("memorystorage: storage closed: %w", durablestream.ErrClosed)

func newIncarnationID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("memorystorage: generate incarnation ID: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}

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

// New creates a new in-memory storage instance.
func New() *Storage {
	return &Storage{}
}

// closed returns the channel that Close closes. It initializes the channel on
// first use so the zero value of Storage is usable.
func (m *Storage) closed() <-chan struct{} {
	m.initOnce.Do(func() { m.closedCh = make(chan struct{}) })
	return m.closedCh
}

// Create creates a new stream (Section 5.1).
func (m *Storage) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	created, _, err := m.CreateWithMessages(ctx, streamID, cfg, nil)
	return created, err
}

// CreateWithMessages creates a stream and its initial messages atomically.
func (m *Storage) CreateWithMessages(ctx context.Context, streamID string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	cloned, err := cloneBatch(messages, true)
	if err != nil {
		return false, "", err
	}
	// Storage callers may specify the sliding window without precomputing its
	// first deadline. Initialize it once here; idempotent replays compare TTL but
	// deliberately ignore the newly derived ExpiresAt.
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	incID, err := newIncarnationID()
	if err != nil {
		return false, "", err
	}

	stream := &memoryStream{
		config:   cfg,
		messages: make([]durablestream.StoredMessage, len(cloned)),
		lastSeq:  "",
		notifyCh: make(chan struct{}),
		incID:    incID,
	}
	for i, data := range cloned {
		stream.messages[i] = durablestream.StoredMessage{
			Data:   data,
			Offset: storage.FormatSimpleOffset(int64(i + 1)),
		}
	}
	initialTail := tailOffset(stream.messages)

	for {
		existing, loaded := m.streams.LoadOrStore(streamID, stream)
		if !loaded {
			return true, initialTail, nil // Newly created
		}

		// Stream already exists - check if it is live while holding the same
		// lock Append uses, so the returned tail has a valid linearization point.
		existing.mu.RLock()
		deleted := existing.deleted
		existingCfg := existing.config
		existingTail := tailOffset(existing.messages)
		existing.mu.RUnlock()
		if deleted {
			// A deleted entry should already have been removed from the map, but
			// discard it defensively if a custom lifecycle race exposed one.
			m.streams.CompareAndDelete(streamID, existing)
			continue
		}
		if !existingCfg.IsExpired() {
			// Not expired - check if config matches for idempotency
			if existingCfg.Matches(cfg) {
				return false, existingTail, nil // Initial messages are not replayed.
			}
			return false, "", fmt.Errorf("stream exists with different config: %w", durablestream.ErrConflict)
		}

		// Expired stream can be replaced (Section 5.1). Swap atomically so that
		// exactly one concurrent Create claims the replacement, and so a
		// concurrent Delete of the expired stream is not silently undone.
		if !m.streams.CompareAndSwap(streamID, existing, stream) {
			// Another Create replaced it, or a Delete removed it. Re-evaluate.
			continue
		}

		// We own the old stream now: wake any waiters on it.
		existing.markDeleted()
		return true, initialTail, nil
	}
}

// Append writes data to a stream (Section 5.2).
func (m *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	return m.AppendBatch(ctx, streamID, [][]byte{data}, seq)
}

// AppendBatch appends messages as one ordered, atomic mutation.
func (m *Storage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cloned, err := cloneBatch(messages, false)
	if err != nil {
		return "", err
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return "", durablestream.ErrNotFound
	}
	return appendBatchToStream(stream, cloned, seq)
}

// appendBatchToStream appends to the exact stream incarnation the caller loaded.
// Delete removes an incarnation from the map before marking it deleted, so a
// concurrent caller can still hold its pointer after Delete returns. Checking
// deleted under the same lock that markDeleted uses prevents that stale caller
// from closing an already-closed notification channel or acknowledging data
// that is no longer reachable.
func appendBatchToStream(stream *memoryStream, messages [][]byte, seq string) (durablestream.Offset, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.deleted {
		return "", durablestream.ErrNotFound
	}

	// Check expiry
	if stream.config.IsExpired() {
		return "", durablestream.ErrNotFound
	}

	// Validate sequence number if provided (Section 5.2)
	if seq != "" {
		if stream.lastSeq != "" && seq <= stream.lastSeq {
			return "", fmt.Errorf("sequence regression detected: %w", durablestream.ErrConflict)
		}
		stream.lastSeq = seq
	}

	// The caller's slices were copied before the lock was acquired. Install the
	// complete batch while readers are excluded, assigning one offset per item.
	for _, data := range messages {
		offset := storage.FormatSimpleOffset(int64(len(stream.messages) + 1))
		stream.messages = append(stream.messages, durablestream.StoredMessage{
			Data:   data,
			Offset: offset,
		})
	}

	// Notify waiters once after the entire batch is visible.
	close(stream.notifyCh)
	stream.notifyCh = make(chan struct{})

	return stream.messages[len(stream.messages)-1].Offset, nil
}

// cloneBatch validates and copies a borrowed message batch. Empty batches are
// valid only for CreateWithMessages; individual empty messages are never valid
// Storage messages.
func cloneBatch(messages [][]byte, allowEmptyBatch bool) ([][]byte, error) {
	if len(messages) == 0 && !allowEmptyBatch {
		return nil, fmt.Errorf("empty append batch not allowed: %w", durablestream.ErrBadRequest)
	}
	cloned := make([][]byte, len(messages))
	for i, message := range messages {
		if len(message) == 0 {
			return nil, fmt.Errorf("empty message at batch index %d: %w", i, durablestream.ErrBadRequest)
		}
		cloned[i] = bytes.Clone(message)
	}
	return cloned, nil
}

func tailOffset(messages []durablestream.StoredMessage) durablestream.Offset {
	if len(messages) == 0 {
		return storage.FormatSimpleOffset(0)
	}
	return messages[len(messages)-1].Offset
}

// Read returns messages from offset (Section 5.6).
func (m *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}
	return readStream(ctx, stream, offset, limit)
}

// readStream reads from one captured stream incarnation. Keeping the pointer in
// the call chain is important for WaitForData: a waiter on a deleted stream must
// not silently switch to a new stream created with the same ID.
func readStream(ctx context.Context, stream *memoryStream, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

	if stream.deleted {
		return nil, durablestream.ErrNotFound
	}

	// Check expiry
	if stream.config.IsExpired() {
		return nil, durablestream.ErrNotFound
	}

	// Parse offset to message index
	_, byteOffset, err := storage.ParseOffset(offset)
	if err != nil {
		return nil, err
	}
	offsetIdx := byteOffset

	// Offset 0 means "start", which maps to first message (index 0).
	// Offset N means "after message N", so we start reading from index N.
	// An offset at or past the tail is not an error: the loop below yields no
	// messages and the caller is told to retry at the same offset. ErrGone is
	// reserved for retention/compaction, which this implementation never does.

	// Collect messages starting from offsetIdx, respecting byte limit.
	// Message data is copied: callers own what they receive, and must not be
	// able to mutate the stored log.
	var messages []durablestream.StoredMessage
	totalBytes := 0
	// Clamping before the int conversion keeps a far-past-the-tail offset from
	// truncating into a valid index on 32-bit platforms.
	start := len(stream.messages)
	if offsetIdx < int64(start) {
		start = int(offsetIdx)
	}
	for i := start; i < len(stream.messages); i++ {
		msg := stream.messages[i]
		if limit > 0 && totalBytes+len(msg.Data) > limit && len(messages) > 0 {
			// Would exceed limit and we have at least one message
			break
		}
		messages = append(messages, durablestream.StoredMessage{
			Data:   bytes.Clone(msg.Data),
			Offset: msg.Offset,
		})
		totalBytes += len(msg.Data)
	}

	// Calculate next offset
	var nextOffset durablestream.Offset
	if len(messages) > 0 {
		// Next offset is the offset of the last message we returned
		// (which points to "after that message")
		nextOffset = messages[len(messages)-1].Offset
	} else {
		// No messages returned, stay at current offset
		nextOffset = offset
		if nextOffset == "" || nextOffset == "-1" {
			nextOffset = storage.FormatSimpleOffset(0)
		}
	}

	// Tail offset is the offset of the last message (or 0 if empty)
	var tailOffset durablestream.Offset
	if len(stream.messages) > 0 {
		tailOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		tailOffset = storage.FormatSimpleOffset(0)
	}

	return &durablestream.ReadResult{
		Messages:      messages,
		NextOffset:    nextOffset,
		TailOffset:    tailOffset,
		IncarnationID: stream.incID,
	}, nil
}

// Head returns stream metadata (Section 5.5).
func (m *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

	if stream.deleted {
		return nil, durablestream.ErrNotFound
	}

	// Check expiry
	if stream.config.IsExpired() {
		return nil, durablestream.ErrNotFound
	}

	// NextOffset is the offset of the last message, or 0 if empty
	var nextOffset durablestream.Offset
	if len(stream.messages) > 0 {
		nextOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		nextOffset = storage.FormatSimpleOffset(0)
	}

	return &durablestream.StreamInfo{
		ContentType:   stream.config.ContentType,
		NextOffset:    nextOffset,
		TTL:           stream.config.TTL,
		ExpiresAt:     stream.config.ExpiresAt,
		IsPrivate:     stream.config.IsPrivate,
		IncarnationID: stream.incID,
	}, nil
}

// Touch restarts the stream's sliding TTL window (Section 5.1).
func (m *Storage) Touch(ctx context.Context, streamID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return durablestream.ErrNotFound
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

	if stream.deleted {
		return durablestream.ErrNotFound
	}

	// An already-expired stream stays expired: sliding the window here would
	// resurrect a stream that Read, Append and Head have been reporting as
	// absent, and that Create is entitled to replace.
	if stream.config.IsExpired() {
		return durablestream.ErrNotFound
	}

	cfg, moved := stream.config.SlideExpiry(time.Now())
	if moved {
		stream.config = cfg
		// WaitForData also waits on the current expiry deadline. Wake waiters so
		// they replace a timer based on the old deadline with the renewed one.
		close(stream.notifyCh)
		stream.notifyCh = make(chan struct{})
	}
	return nil
}

// Delete removes a stream (Section 5.4).
func (m *Storage) Delete(ctx context.Context, streamID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	stream, ok := m.streams.LoadAndDelete(streamID)
	if !ok {
		return durablestream.ErrNotFound
	}

	// Mark as deleted and close notification channel to wake any waiters.
	// A concurrent Create replacing an expired stream may have already done
	// this, so markDeleted guards against a double close.
	stream.markDeleted()

	return nil
}

// WaitForData blocks until data is available at offset, then returns it.
// Returns immediately if data already exists at offset.
// Returns ctx.Err() on timeout/cancellation.
// Returns ErrNotFound if stream doesn't exist or is deleted while waiting.
// Returns an error matching durablestream.ErrClosed if the storage is closed
// while waiting.
func (m *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}
	return m.waitForStream(ctx, stream, offset, limit)
}

// waitForStream waits on one stream incarnation for the lifetime of the call.
// In particular, waking because that incarnation was deleted cannot make the
// waiter attach to a replacement that happens to reuse the same stream ID.
func (m *Storage) waitForStream(ctx context.Context, stream *memoryStream, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	for {
		stream.mu.RLock()

		// Check if stream is deleted
		if stream.deleted {
			stream.mu.RUnlock()
			return nil, durablestream.ErrNotFound
		}

		// Check expiry
		if stream.config.IsExpired() {
			stream.mu.RUnlock()
			return nil, durablestream.ErrNotFound
		}

		// Get notification channel before reading (to avoid race)
		notifyCh := stream.notifyCh
		expiresAt := stream.config.ExpiresAt

		stream.mu.RUnlock()

		// Try to read data
		result, err := readStream(ctx, stream, offset, limit)
		if err != nil {
			return nil, err
		}

		// If we have data, return it
		if len(result.Messages) > 0 {
			return result, nil
		}

		// No data available: expiry makes this incarnation absent just as surely
		// as Delete does. A Touch wakes notifyCh so a renewed deadline is picked
		// up before this timer can incorrectly expire the stream.
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
		case <-m.closed():
			stopTimer(expiryTimer)
			return nil, errClosed
		case <-notifyCh:
			stopTimer(expiryTimer)
			// New data or deletion - loop to re-check
		case <-expiryCh:
			// Loop so IsExpired performs the authoritative deadline check.
		}
	}
}

// Close releases resources. Safe to call multiple times.
//
// There is nothing to flush for in-memory storage, but Close still releases
// every blocked WaitForData caller with an error matching
// durablestream.ErrClosed. Stream data stays in memory and other operations
// keep working after Close.
func (m *Storage) Close() error {
	m.closeOnce.Do(func() {
		m.initOnce.Do(func() { m.closedCh = make(chan struct{}) })
		close(m.closedCh)
	})
	return nil
}
