package durablestream

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

// memoryStream represents a single stream in memory.
type memoryStream struct {
	config      StreamConfig
	data        []byte   // All stream data concatenated
	messages    [][]byte // Individual messages for JSON mode (nil for non-JSON)
	offsets     []int    // Byte position for each offset (offsets[i] is start of offset i)
	lastSeq     string   // Last sequence number seen (lexicographic)
	subscribers []chan Offset
}

// MemoryStorage is an in-memory implementation of Storage.
// It uses zero-padded sequential offsets (e.g., "0000000001", "0000000002").
type MemoryStorage struct {
	mu      sync.RWMutex
	streams map[string]*memoryStream
}

// NewMemoryStorage creates a new in-memory storage instance.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		streams: make(map[string]*memoryStream),
	}
}

// Create creates a new stream (Section 5.1).
func (m *MemoryStorage) Create(ctx context.Context, streamID string, cfg StreamConfig) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.streams[streamID]; ok {
		// Idempotent if config matches
		if configsMatch(existing.config, cfg) {
			return false, nil // Not newly created
		}
		return false, newError(codeConflict, "stream exists with different config")
	}

	stream := &memoryStream{
		config:      cfg,
		data:        make([]byte, 0),
		offsets:     []int{0}, // offset 0 starts at byte 0
		lastSeq:     "",
		subscribers: make([]chan Offset, 0),
	}

	// Initialize messages slice for JSON mode
	if protocol.IsJSONContentType(cfg.ContentType) {
		stream.messages = make([][]byte, 0)
	}

	m.streams[streamID] = stream

	return true, nil // Newly created
}

// Append writes data to a stream (Section 5.2).
func (m *MemoryStorage) Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error) {
	if len(data) == 0 {
		return "", newError(codeBadRequest, "empty append not allowed")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[streamID]
	if !ok {
		return "", newError(codeNotFound, "stream not found")
	}

	// Check expiry
	if isExpired(stream.config) {
		return "", newError(codeNotFound, "stream not found")
	}

	// Validate sequence number if provided (Section 5.2)
	if seq != "" {
		if stream.lastSeq != "" && seq <= stream.lastSeq {
			return "", newError(codeConflict, "sequence regression detected")
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

	// Notify subscribers
	m.notifySubscribers(stream, offset)

	return offset, nil
}

// AppendReader streams data from an io.Reader to a stream.
// For MemoryStorage, this reads all data into memory then appends.
// A production storage would stream directly to disk/network.
func (m *MemoryStorage) AppendReader(ctx context.Context, streamID string, r io.Reader, seq string) (Offset, error) {
	// For memory storage, we still need to read into memory
	// A real implementation would stream to disk
	data, err := io.ReadAll(r)
	if err != nil {
		return "", newError(codeBadRequest, "failed to read request body")
	}
	return m.Append(ctx, streamID, data, seq)
}

// Read returns data from offset (Section 5.5).
func (m *MemoryStorage) Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stream, ok := m.streams[streamID]
	if !ok {
		return nil, newError(codeNotFound, "stream not found")
	}

	// Check expiry
	if isExpired(stream.config) {
		return nil, newError(codeNotFound, "stream not found")
	}

	// Parse offset
	offsetIdx, err := parseOffset(offset)
	if err != nil {
		return nil, err
	}

	// Validate offset is within bounds
	if offsetIdx < 0 || offsetIdx >= len(stream.offsets) {
		return nil, newError(codeGone, "offset before earliest retained position")
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

	result := &ReadResult{
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
func (m *MemoryStorage) Head(ctx context.Context, streamID string) (*StreamInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stream, ok := m.streams[streamID]
	if !ok {
		return nil, newError(codeNotFound, "stream not found")
	}

	// Check expiry
	if isExpired(stream.config) {
		return nil, newError(codeNotFound, "stream not found")
	}

	info := &StreamInfo{
		ContentType: stream.config.ContentType,
		NextOffset:  formatOffset(len(stream.offsets) - 1),
		TTL:         stream.config.TTL,
		ExpiresAt:   stream.config.ExpiresAt,
	}

	return info, nil
}

// Delete removes a stream (Section 5.3).
func (m *MemoryStorage) Delete(ctx context.Context, streamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[streamID]
	if !ok {
		return newError(codeNotFound, "stream not found")
	}

	// Close all subscriber channels
	for _, ch := range stream.subscribers {
		close(ch)
	}

	delete(m.streams, streamID)
	return nil
}

// Subscribe returns a channel notified when new data arrives.
func (m *MemoryStorage) Subscribe(ctx context.Context, streamID string, offset Offset) (<-chan Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stream, ok := m.streams[streamID]
	if !ok {
		return nil, newError(codeNotFound, "stream not found")
	}

	// Check expiry
	if isExpired(stream.config) {
		return nil, newError(codeNotFound, "stream not found")
	}

	// Create buffered channel to avoid blocking appends
	ch := make(chan Offset, 10)
	stream.subscribers = append(stream.subscribers, ch)

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		m.mu.Lock()
		defer m.mu.Unlock()

		// Remove subscriber
		if s, ok := m.streams[streamID]; ok {
			for i, sub := range s.subscribers {
				if sub == ch {
					s.subscribers = append(s.subscribers[:i], s.subscribers[i+1:]...)
					break
				}
			}
		}
		close(ch)
	}()

	return ch, nil
}

// notifySubscribers sends the new offset to all subscribers.
// Must be called with lock held.
func (m *MemoryStorage) notifySubscribers(stream *memoryStream, offset Offset) {
	for _, ch := range stream.subscribers {
		select {
		case ch <- offset:
		default:
			// Skip if channel is full (non-blocking)
		}
	}
}

// formatOffset formats an offset index as a zero-padded string.
// Uses 10 digits to support up to 9,999,999,999 offsets.
func formatOffset(idx int) Offset {
	return Offset(fmt.Sprintf("%010d", idx))
}

// parseOffset parses an offset string back to an index.
// Special value "-1" is treated as 0 (read from start).
func parseOffset(offset Offset) (int, error) {
	if offset == "" || offset == "-1" {
		return 0, nil
	}
	var idx int
	_, err := fmt.Sscanf(string(offset), "%d", &idx)
	if err != nil {
		return 0, fmt.Errorf("invalid offset: %w", err)
	}
	return idx, nil
}

// configsMatch checks if two StreamConfigs are equivalent for idempotent create.
func configsMatch(a, b StreamConfig) bool {
	// Content-Type media types are case-insensitive per RFC 2045
	if !protocol.ContentTypesMatch(a.ContentType, b.ContentType) {
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
func isExpired(cfg StreamConfig) bool {
	now := time.Now()
	if cfg.ExpiresAt != nil && now.After(*cfg.ExpiresAt) {
		return true
	}
	// TTL is checked at creation time, not on every access in this simple implementation
	return false
}
