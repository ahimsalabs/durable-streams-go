package durablestream

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"time"
)

// testStorage is a minimal storage implementation for internal tests.
// It provides basic functionality without importing external packages.
type testStorage struct {
	mu      sync.RWMutex
	streams map[string]*testStream
}

type testStream struct {
	config   StreamConfig
	messages []StoredMessage
	notifyCh chan struct{} // Closed on append, replaced with new channel
	deleted  bool
}

func newTestStorage() *testStorage {
	return &testStorage{
		streams: make(map[string]*testStream),
	}
}

func (s *testStorage) Create(ctx context.Context, streamID string, cfg StreamConfig) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.streams[streamID]; ok {
		return false, nil
	}

	stream := &testStream{
		config:   cfg,
		messages: make([]StoredMessage, 0),
		notifyCh: make(chan struct{}),
	}

	s.streams[streamID] = stream
	return true, nil
}

func (s *testStorage) Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return "", ErrNotFound
	}

	// Copy data - caller may reuse the slice (per Storage interface contract)
	b := make([]byte, len(data))
	copy(b, data)

	offset := FormatOffset(int64(len(stream.messages) + 1))
	msg := StoredMessage{
		Data:   b,
		Offset: offset,
	}
	stream.messages = append(stream.messages, msg)

	// Notify waiters: close current channel to wake all waiters, then replace it
	close(stream.notifyCh)
	stream.notifyCh = make(chan struct{})

	return offset, nil
}

func (s *testStorage) Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, ErrNotFound
	}

	offsetIdx := 0
	if offset != "" && offset != "-1" {
		_, _ = fmt.Sscanf(string(offset), "%d", &offsetIdx)
	}

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
			nextOffset = FormatOffset(0)
		}
	}

	// Tail offset
	var tailOffset Offset
	if len(stream.messages) > 0 {
		tailOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		tailOffset = FormatOffset(0)
	}

	return &ReadResult{
		Messages:   messages,
		NextOffset: nextOffset,
		TailOffset: tailOffset,
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
		nextOffset = FormatOffset(0)
	}

	return &StreamInfo{
		ContentType: stream.config.ContentType,
		NextOffset:  nextOffset,
		TTL:         stream.config.TTL,
		ExpiresAt:   stream.config.ExpiresAt,
	}, nil
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
