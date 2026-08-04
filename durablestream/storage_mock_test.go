package durablestream

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// formatTestOffset formats an offset for testing using the reference implementation format.
// This is a local helper to avoid importing the storage package (which would create a cycle).
func formatTestOffset(idx int64) Offset {
	return Offset(fmt.Sprintf("0000000000000000_%016d", idx))
}

// parseTestOffset parses a test offset to get the byte offset.
func parseTestOffset(offset Offset) int64 {
	s := string(offset)
	if s == "" || s == "-1" {
		return 0
	}
	parts := strings.SplitN(s, "_", 2)
	if len(parts) != 2 {
		return 0
	}
	// A malformed suffix leaves idx at zero, which is the same "start of stream"
	// answer the sentinel cases above return, so the scan error carries nothing
	// a caller could act on.
	var idx int64
	_, _ = fmt.Sscanf(parts[1], "%d", &idx)
	return idx
}

// testStorage is a minimal storage implementation for internal tests.
// It provides basic functionality without importing external packages.
type testStorage struct {
	mu              sync.RWMutex
	streams         map[string]*testStream
	nextIncarnation uint64
}

type testStream struct {
	config   StreamConfig
	messages []StoredMessage
	lastSeq  string
	notifyCh chan struct{} // Closed on append, replaced with new channel
	deleted  bool
	incID    string
}

func newTestStorage() *testStorage {
	return &testStorage{
		streams: make(map[string]*testStream),
	}
}

func (s *testStorage) Create(ctx context.Context, streamID string, cfg StreamConfig) (bool, error) {
	created, _, err := s.CreateWithMessages(ctx, streamID, cfg, nil)
	return created, err
}

func (s *testStorage) CreateWithMessages(ctx context.Context, streamID string, cfg StreamConfig, messages [][]byte) (bool, Offset, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	cloned := make([][]byte, len(messages))
	for i, data := range messages {
		if len(data) == 0 {
			return false, "", ErrBadRequest
		}
		cloned[i] = bytes.Clone(data)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if stream, ok := s.streams[streamID]; ok {
		if !stream.config.IsExpired() {
			if stream.config.Matches(cfg) {
				return false, testStreamTail(stream), nil
			}
			return false, "", ErrConflict
		}
		stream.deleted = true
		close(stream.notifyCh)
	}

	s.nextIncarnation++
	stream := &testStream{
		config:   cfg,
		messages: make([]StoredMessage, len(cloned)),
		notifyCh: make(chan struct{}),
		incID:    fmt.Sprintf("test-incarnation-%d", s.nextIncarnation),
	}
	for i, data := range cloned {
		stream.messages[i] = StoredMessage{
			Data:   data,
			Offset: formatTestOffset(int64(i + 1)),
		}
	}

	s.streams[streamID] = stream
	return true, testStreamTail(stream), nil
}

func (s *testStorage) Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error) {
	return s.AppendBatch(ctx, streamID, [][]byte{data}, seq)
}

func (s *testStorage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(messages) == 0 {
		return "", ErrBadRequest
	}
	cloned := make([][]byte, len(messages))
	for i, data := range messages {
		if len(data) == 0 {
			return "", ErrBadRequest
		}
		cloned[i] = bytes.Clone(data)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok || stream.config.IsExpired() {
		return "", ErrNotFound
	}
	if seq != "" && stream.lastSeq != "" && seq <= stream.lastSeq {
		return "", ErrConflict
	}

	var offset Offset
	for _, data := range cloned {
		offset = formatTestOffset(int64(len(stream.messages) + 1))
		stream.messages = append(stream.messages, StoredMessage{
			Data:   data,
			Offset: offset,
		})
	}
	if seq != "" {
		stream.lastSeq = seq
	}

	// Notify waiters: close current channel to wake all waiters, then replace it
	close(stream.notifyCh)
	stream.notifyCh = make(chan struct{})

	return offset, nil
}

func testStreamTail(stream *testStream) Offset {
	if len(stream.messages) == 0 {
		return formatTestOffset(0)
	}
	return stream.messages[len(stream.messages)-1].Offset
}

func (s *testStorage) Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, ErrNotFound
	}

	offsetIdx := int(parseTestOffset(offset))

	if offsetIdx > len(stream.messages) {
		return nil, ErrGone
	}

	// Collect messages starting from offsetIdx, respecting byte limit
	var messages []StoredMessage
	totalBytes := 0
	for i := offsetIdx; i < len(stream.messages); i++ {
		msg := stream.messages[i]
		if limit > 0 && totalBytes+len(msg.Data) > limit && len(messages) > 0 {
			break
		}
		messages = append(messages, msg)
		totalBytes += len(msg.Data)
	}

	// Calculate next offset
	var nextOffset Offset
	if len(messages) > 0 {
		nextOffset = messages[len(messages)-1].Offset
	} else {
		nextOffset = offset
		if nextOffset == "" || nextOffset == "-1" {
			nextOffset = formatTestOffset(0)
		}
	}

	// Tail offset
	var tailOffset Offset
	if len(stream.messages) > 0 {
		tailOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		tailOffset = formatTestOffset(0)
	}

	return &ReadResult{
		Messages:      messages,
		NextOffset:    nextOffset,
		TailOffset:    tailOffset,
		IncarnationID: stream.incID,
	}, nil
}

func (s *testStorage) Head(ctx context.Context, streamID string) (*StreamInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, ErrNotFound
	}

	var nextOffset Offset
	if len(stream.messages) > 0 {
		nextOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		nextOffset = formatTestOffset(0)
	}

	return &StreamInfo{
		ContentType:   stream.config.ContentType,
		NextOffset:    nextOffset,
		TTL:           stream.config.TTL,
		ExpiresAt:     stream.config.ExpiresAt,
		IncarnationID: stream.incID,
	}, nil
}

func (s *testStorage) Touch(ctx context.Context, streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return ErrNotFound
	}
	if cfg, moved := stream.config.SlideExpiry(time.Now()); moved {
		stream.config = cfg
	}
	return nil
}

func (s *testStorage) Delete(ctx context.Context, streamID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return ErrNotFound
	}

	// Mark as deleted and close notification channel to wake any waiters
	stream.deleted = true
	close(stream.notifyCh)
	delete(s.streams, streamID)
	return nil
}

// WaitForData blocks until data is available at offset, then returns it.
func (s *testStorage) WaitForData(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	for {
		s.mu.RLock()
		stream, ok := s.streams[streamID]
		if !ok {
			s.mu.RUnlock()
			return nil, ErrNotFound
		}

		if stream.deleted {
			s.mu.RUnlock()
			return nil, ErrNotFound
		}

		notifyCh := stream.notifyCh
		s.mu.RUnlock()

		// Try to read data
		result, err := s.Read(ctx, streamID, offset, limit)
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

func (s *testStorage) Close() error {
	return nil
}

// setupInternalTestServer creates a test HTTP server with testStorage for internal tests.
// Uses short timeouts (100ms) to avoid slow tests when long-polling is triggered.
func setupInternalTestServer() (*httptest.Server, *testStorage, *Client) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{
		LongPollTimeout: 100 * time.Millisecond,
	})
	server := httptest.NewServer(handler)
	client := NewClient(server.URL, nil)
	return server, storage, client
}
