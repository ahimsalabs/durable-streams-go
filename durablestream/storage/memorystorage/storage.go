// Package memorystore provides an in-memory implementation of durablestream.Storage.
//
// This implementation uses the reference offset format from the official Node.js
// implementation: "<readSeq>_<byteOffset>" with 16-digit zero-padded integers.
package memorystorage

import (
	"context"
	"fmt"
	"sync"

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
}

// Storage is an in-memory implementation of durablestream.Storage.
// Uses hashtriemap for lock-free stream lookups with per-stream locks for mutations.
//
// Offsets use the reference implementation format: "0000000000000000_0000000000000042"
// where the first 16 digits are the read sequence (always 0 for this implementation)
// and the second 16 digits are the cumulative byte offset.
type Storage struct {
	streams hashtriemap.HashTrieMap[string, *memoryStream]
}

// New creates a new in-memory storage instance.
func New() *Storage {
	return &Storage{}
}

// Create creates a new stream (Section 5.1).
func (m *Storage) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	stream := &memoryStream{
		config:   cfg,
		messages: make([]durablestream.StoredMessage, 0),
		lastSeq:  "",
		notifyCh: make(chan struct{}),
	}

	existing, loaded := m.streams.LoadOrStore(streamID, stream)
	if loaded {
		// Stream already exists - check if it's expired
		if existing.config.IsExpired() {
			// Wake any waiters on the old stream before replacing
			existing.mu.Lock()
			if !existing.deleted {
				existing.deleted = true
				close(existing.notifyCh)
			}
			existing.mu.Unlock()

			// Expired stream can be replaced (Section 5.1)
			m.streams.Store(streamID, stream)
			return true, nil
		}
		// Not expired - check if config matches for idempotency
		if existing.config.Matches(cfg) {
			return false, nil // Not newly created, but config matches
		}
		return false, fmt.Errorf("stream exists with different config: %w", durablestream.ErrConflict)
	}

	return true, nil // Newly created
}

// Append writes data to a stream (Section 5.2).
func (m *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("empty append not allowed: %w", durablestream.ErrBadRequest)
	}

	stream, ok := m.streams.Load(streamID)
	if !ok {
		return "", durablestream.ErrNotFound
	}

	stream.mu.Lock()
	defer stream.mu.Unlock()

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

	// Create new message with offset
	// Copy data to ensure durability - caller may reuse/mutate the slice
	b := make([]byte, len(data))
	copy(b, data)

	// Use message index as offset (matching reference implementation's byte offset concept)
	offset := storage.FormatSimpleOffset(int64(len(stream.messages) + 1))
	msg := durablestream.StoredMessage{
		Data:   b,
		Offset: offset,
	}
	stream.messages = append(stream.messages, msg)

	// Notify waiters: close current channel to wake all waiters, then replace it
	close(stream.notifyCh)
	stream.notifyCh = make(chan struct{})

	return offset, nil
}

// Read returns messages from offset (Section 5.5).
func (m *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

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

	// Offset 0 means "start", which maps to first message (index 0)
	// Offset N means "after message N", so we start reading from index N
	// If offsetIdx > len(messages), client is ahead of stream (gone)
	if offsetIdx > int64(len(stream.messages)) {
		return nil, durablestream.ErrGone
	}

	// Collect messages starting from offsetIdx, respecting byte limit
	var messages []durablestream.StoredMessage
	totalBytes := 0
	for i := int(offsetIdx); i < len(stream.messages); i++ {
		msg := stream.messages[i]
		if limit > 0 && totalBytes+len(msg.Data) > limit && len(messages) > 0 {
			// Would exceed limit and we have at least one message
			break
		}
		messages = append(messages, msg)
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
		Messages:   messages,
		NextOffset: nextOffset,
		TailOffset: tailOffset,
	}, nil
}

// Head returns stream metadata (Section 5.4).
func (m *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

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
		ContentType: stream.config.ContentType,
		NextOffset:  nextOffset,
		TTL:         stream.config.TTL,
		ExpiresAt:   stream.config.ExpiresAt,
		IsPrivate:   stream.config.IsPrivate,
	}, nil
}

// Delete removes a stream (Section 5.3).
func (m *Storage) Delete(ctx context.Context, streamID string) error {
	stream, ok := m.streams.LoadAndDelete(streamID)
	if !ok {
		return durablestream.ErrNotFound
	}

	// Mark as deleted and close notification channel to wake any waiters
	stream.mu.Lock()
	stream.deleted = true
	close(stream.notifyCh)
	stream.mu.Unlock()

	return nil
}

// WaitForData blocks until data is available at offset, then returns it.
// Returns immediately if data already exists at offset.
// Returns ctx.Err() on timeout/cancellation.
// Returns ErrNotFound if stream doesn't exist or is deleted while waiting.
func (m *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	for {
		stream, ok := m.streams.Load(streamID)
		if !ok {
			return nil, durablestream.ErrNotFound
		}

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

		stream.mu.RUnlock()

		// Try to read data
		result, err := m.Read(ctx, streamID, offset, limit)
		if err != nil {
			return nil, err
		}

		// If we have data, return it
		if len(result.Messages) > 0 {
			return result, nil
		}

		// No data available, wait for notification or context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notifyCh:
			// New data or deletion - loop to re-check
		}
	}
}

// Close releases resources. Safe to call multiple times.
// For in-memory storage, this is a no-op.
func (m *Storage) Close() error {
	return nil
}
