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
	data        []byte   // All stream data concatenated
	messages    [][]byte // Individual messages for JSON mode (nil for non-JSON)
	offsets     []int    // Byte position for each offset (offsets[i] is start of offset i)
	lastSeq     string   // Last sequence number seen (lexicographic)
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
		data:        make([]byte, 0),
		offsets:     []int{0}, // offset 0 starts at byte 0
		lastSeq:     "",
		subscribers: make([]chan durablestream.Offset, 0),
	}

	// Initialize messages slice for JSON mode
	if isJSONContentType(cfg.ContentType) {
		stream.messages = make([][]byte, 0)
	}

	existing, loaded := m.streams.LoadOrStore(streamID, stream)
	if loaded {
		// Stream already exists - check if config matches for idempotency
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
	if isExpired(stream.config) {
		return "", durablestream.ErrNotFound
	}

	// Validate sequence number if provided (Section 5.2)
	if seq != "" {
		if stream.lastSeq != "" && seq <= stream.lastSeq {
			return "", fmt.Errorf("sequence regression detected: %w", durablestream.ErrConflict)
		}
		stream.lastSeq = seq
	}

	// Append data
	stream.data = append(stream.data, data...)

	// Track message for JSON mode
	if stream.messages != nil {
		stream.messages = append(stream.messages, data)
	}

	newOffset := len(stream.offsets)
	stream.offsets = append(stream.offsets, len(stream.data))

	offset := formatOffset(newOffset)

	// Notify subscribers (non-blocking)
	for _, ch := range stream.subscribers {
		select {
		case ch <- offset:
		default:
		}
	}

	return offset, nil
}

// AppendReader streams data from an io.Reader to a stream.
// For Storage, this reads all data into memory then appends.
// A production storage would stream directly to disk/network.
func (m *Storage) AppendReader(ctx context.Context, streamID string, r io.Reader, seq string) (durablestream.Offset, error) {
	// For memory storage, we still need to read into memory
	// A real implementation would stream to disk
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("failed to read request body: %w", durablestream.ErrBadRequest)
	}
	return m.Append(ctx, streamID, data, seq)
}

// Read returns data from offset (Section 5.5).
func (m *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	stream, ok := m.streams.Load(streamID)
	if !ok {
		return nil, durablestream.ErrNotFound
	}

	stream.mu.RLock()
	defer stream.mu.RUnlock()

	// Check expiry
	if isExpired(stream.config) {
		return nil, durablestream.ErrNotFound
	}

	// Parse offset
	offsetIdx, err := parseOffset(offset)
	if err != nil {
		return nil, err
	}

	// Validate offset is within bounds
	if offsetIdx < 0 || offsetIdx >= len(stream.offsets) {
		return nil, durablestream.ErrGone
	}

	// Calculate byte range to read
	startPos := stream.offsets[offsetIdx]
	endPos := len(stream.data)

	// Apply limit
	if limit > 0 && endPos-startPos > limit {
		endPos = startPos + limit
	}

	// Find next offset: the offset that starts at or after endPos
	nextOffsetIdx := offsetIdx
	for i := offsetIdx + 1; i < len(stream.offsets); i++ {
		if stream.offsets[i] >= endPos {
			nextOffsetIdx = i
			break
		}
		nextOffsetIdx = i
	}
	// If we read to the end, next offset is tail
	if endPos >= len(stream.data) {
		nextOffsetIdx = len(stream.offsets) - 1
	}

	data := make([]byte, endPos-startPos)
	copy(data, stream.data[startPos:endPos])

	result := &durablestream.ReadResult{
		Data:       data,
		Messages:   nil,
		NextOffset: formatOffset(nextOffsetIdx),
		TailOffset: formatOffset(len(stream.offsets) - 1),
	}

	// For JSON mode, return individual messages
	// Each offset corresponds to a message boundary
	if stream.messages != nil && offsetIdx < len(stream.messages) {
		// Determine how many messages to return based on the next offset
		endMsgIdx := nextOffsetIdx
		if endMsgIdx > len(stream.messages) {
			endMsgIdx = len(stream.messages)
		}

		// Return messages from offsetIdx to endMsgIdx
		result.Messages = make([][]byte, 0, endMsgIdx-offsetIdx)
		for i := offsetIdx; i < endMsgIdx && i < len(stream.messages); i++ {
			result.Messages = append(result.Messages, stream.messages[i])
		}
	}

	return result, nil
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
	if isExpired(stream.config) {
		return nil, durablestream.ErrNotFound
	}

	info := &durablestream.StreamInfo{
		ContentType: stream.config.ContentType,
		NextOffset:  formatOffset(len(stream.offsets) - 1),
		TTL:         stream.config.TTL,
		ExpiresAt:   stream.config.ExpiresAt,
	}

	return info, nil
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
	if isExpired(stream.config) {
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
	if !durationEqual(a.TTL, b.TTL) {
		return false
	}
	if !timeEqual(a.ExpiresAt, b.ExpiresAt) {
		return false
	}
	return true
}

// durationEqual compares two *time.Duration for equality.
func durationEqual(a, b *time.Duration) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// timeEqual compares two *time.Time for equality.
func timeEqual(a, b *time.Time) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Equal(*b)
}

// isExpired checks if a stream has expired based on its config.
func isExpired(cfg durablestream.StreamConfig) bool {
	now := time.Now()
	if cfg.ExpiresAt != nil && now.After(*cfg.ExpiresAt) {
		return true
	}
	// TTL is checked at creation time, not on every access in this simple implementation
	return false
}

// isJSONContentType returns true if the content type is application/json.
func isJSONContentType(contentType string) bool {
	parts := strings.Split(contentType, ";")
	mediaType := strings.TrimSpace(parts[0])
	return strings.EqualFold(mediaType, "application/json")
}

// contentTypesMatch compares two content types case-insensitively for the base media type.
func contentTypesMatch(a, b string) bool {
	partsA := strings.Split(a, ";")
	partsB := strings.Split(b, ";")
	mediaTypeA := strings.TrimSpace(partsA[0])
	mediaTypeB := strings.TrimSpace(partsB[0])
	return strings.EqualFold(mediaTypeA, mediaTypeB)
}
