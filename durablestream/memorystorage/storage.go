// Package memorystorage provides an in-memory implementation of durablestream.Storage.
package memorystorage

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/go4org/hashtriemap"
)

// memoryStream represents a single stream in memory.
type memoryStream struct {
	mu          sync.RWMutex // Per-stream lock for mutations
	config      durablestream.StreamConfig
	createdAt   time.Time                     // When the stream was created
	messages    []durablestream.StoredMessage // All messages in order
	lastSeq     string                        // Last sequence number seen (lexicographic)
	subscribers []chan durablestream.Offset
}

// Storage is an in-memory implementation of durablestream.Storage.
// Uses hashtriemap for lock-free stream lookups with per-stream locks for mutations.
// Offsets are zero-padded sequential strings (e.g., "0000000001", "0000000002").
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
		config:      cfg,
		createdAt:   time.Now(),
		messages:    make([]durablestream.StoredMessage, 0),
		lastSeq:     "",
		subscribers: make([]chan durablestream.Offset, 0),
	}

	existing, loaded := m.streams.LoadOrStore(streamID, stream)
	if loaded {
		// Stream already exists - check expiry first
		if isExpired(existing.config, existing.createdAt) {
			// Expired stream - delete it and create new one
			m.streams.Delete(streamID)
			_, loaded = m.streams.LoadOrStore(streamID, stream)
			if loaded {
				// Race condition - another request recreated it first
				return false, fmt.Errorf("stream exists with different config: %w", durablestream.ErrConflict)
			}
			return true, nil
		}
		// Stream exists and not expired - check if config matches for idempotency
		if configsMatch(existing.config, cfg) {
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
	if isExpired(stream.config, stream.createdAt) {
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
	offset := formatOffset(len(stream.messages) + 1)
	msg := durablestream.StoredMessage{
		Data:   data,
		Offset: offset,
	}
	stream.messages = append(stream.messages, msg)

	// Notify subscribers (non-blocking)
	for _, ch := range stream.subscribers {
		select {
		case ch <- offset:
		default:
		}
	}

	return offset, nil
}

// AppendFrom streams data from an io.Reader to a stream.
// For Storage, this reads all data into memory then appends.
// A production storage would stream directly to disk/network.
func (m *Storage) AppendFrom(ctx context.Context, streamID string, r io.Reader, seq string) (durablestream.Offset, error) {
	// For memory storage, we still need to read into memory
	// A real implementation would stream to disk
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("failed to read request body: %w", durablestream.ErrBadRequest)
	}
	return m.Append(ctx, streamID, data, seq)
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
	if isExpired(stream.config, stream.createdAt) {
		return nil, durablestream.ErrNotFound
	}

	// Parse offset to message index
	offsetIdx, err := parseOffset(offset)
	if err != nil {
		return nil, err
	}

	// Offset 0 means "start", which maps to first message (index 0)
	// Offset N means "after message N", so we start reading from index N
	// If offsetIdx > len(messages), client is ahead of stream (gone)
	if offsetIdx < 0 || offsetIdx > len(stream.messages) {
		return nil, durablestream.ErrGone
	}

	// Collect messages starting from offsetIdx, respecting byte limit
	var messages []durablestream.StoredMessage
	totalBytes := 0
	for i := offsetIdx; i < len(stream.messages); i++ {
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
			nextOffset = formatOffset(0)
		}
	}

	// Tail offset is the offset of the last message (or 0 if empty)
	var tailOffset durablestream.Offset
	if len(stream.messages) > 0 {
		tailOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		tailOffset = formatOffset(0)
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
	if isExpired(stream.config, stream.createdAt) {
		return nil, durablestream.ErrNotFound
	}

	// NextOffset is the offset of the last message, or 0 if empty
	var nextOffset durablestream.Offset
	if len(stream.messages) > 0 {
		nextOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		nextOffset = formatOffset(0)
	}

	return &durablestream.StreamInfo{
		ContentType: stream.config.ContentType,
		NextOffset:  nextOffset,
		TTL:         stream.config.TTL,
		ExpiresAt:   stream.config.ExpiresAt,
	}, nil
}

// Delete removes a stream (Section 5.3).
func (m *Storage) Delete(ctx context.Context, streamID string) error {
	stream, ok := m.streams.LoadAndDelete(streamID)
	if !ok {
		return durablestream.ErrNotFound
	}

	// Close all subscriber channels
	stream.mu.Lock()
	for _, ch := range stream.subscribers {
		close(ch)
	}
	stream.mu.Unlock()

	return nil
}

// Subscribe returns a channel notified when new data arrives.
func (m *Storage) Subscribe(ctx context.Context, streamID string, offset durablestream.Offset) (<-chan durablestream.Offset, error) {
	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}

	stream.mu.Lock()

	// Check expiry
	if isExpired(stream.config, stream.createdAt) {
		stream.mu.Unlock()
		return nil, durablestream.ErrNotFound
	}

	// Create buffered channel to avoid blocking appends
	ch := make(chan durablestream.Offset, 10)
	stream.subscribers = append(stream.subscribers, ch)
	stream.mu.Unlock()

	// Handle context cancellation
	go func() {
		<-ctx.Done()

		// Remove subscriber - stream may have been deleted, so re-check
		s, ok := m.streams.Load(streamID)
		if ok {
			s.mu.Lock()
			for i, sub := range s.subscribers {
				if sub == ch {
					s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
					break
				}
			}
			s.mu.Unlock()
		}
		close(ch)
	}()

	return ch, nil
}

// formatOffset formats an offset index as a zero-padded string.
// Uses 10 digits to support up to 9,999,999,999 offsets.
func formatOffset(idx int) durablestream.Offset {
	return durablestream.Offset(fmt.Sprintf("%010d", idx))
}

// parseOffset parses an offset string back to an index.
// Special value "-1" is treated as 0 (read from start).
func parseOffset(offset durablestream.Offset) (int, error) {
	if offset == "" || offset == "-1" {
		return 0, nil
	}
	var idx int
	_, err := fmt.Sscanf(string(offset), "%d", &idx)
	if err != nil {
		return 0, fmt.Errorf("invalid offset %q: %w", offset, durablestream.ErrBadRequest)
	}
	return idx, nil
}

// configsMatch checks if two StreamConfigs are equivalent for idempotent create.
func configsMatch(a, b durablestream.StreamConfig) bool {
	// Content-Type media types are case-insensitive per RFC 2045
	if !contentTypesMatch(a.ContentType, b.ContentType) {
		return false
	}
	if a.TTL != b.TTL {
		return false
	}
	if !a.ExpiresAt.Equal(b.ExpiresAt) {
		return false
	}
	return true
}

// isExpired checks if a stream has expired based on its config.
func isExpired(cfg durablestream.StreamConfig, createdAt time.Time) bool {
	if !cfg.ExpiresAt.IsZero() && time.Now().After(cfg.ExpiresAt) {
		return true
	}
	// TTL is relative to creation time
	if cfg.TTL > 0 && time.Now().After(createdAt.Add(cfg.TTL)) {
		return true
	}
	return false
}

// contentTypesMatch compares two content types case-insensitively for the base media type.
func contentTypesMatch(a, b string) bool {
	partsA := strings.Split(a, ";")
	partsB := strings.Split(b, ";")
	mediaTypeA := strings.TrimSpace(partsA[0])
	mediaTypeB := strings.TrimSpace(partsB[0])
	return strings.EqualFold(mediaTypeA, mediaTypeB)
}
