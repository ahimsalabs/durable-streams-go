package durablestream

import (
	"context"
	"fmt"
	"io"
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
	config      StreamConfig
	messages    []StoredMessage
	subscribers []chan Offset
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

	offset := Offset(fmt.Sprintf("%010d", len(stream.messages)+1))
	msg := StoredMessage{
		Data:   data,
		Offset: offset,
	}
	stream.messages = append(stream.messages, msg)

	// Notify subscribers
	for _, ch := range stream.subscribers {
		select {
		case ch <- offset:
		default:
		}
	}

	return offset, nil
}

func (s *testStorage) AppendFrom(ctx context.Context, streamID string, r io.Reader, seq string) (Offset, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return s.Append(ctx, streamID, data, seq)
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
			nextOffset = Offset(fmt.Sprintf("%010d", 0))
		}
	}

	// Tail offset
	var tailOffset Offset
	if len(stream.messages) > 0 {
		tailOffset = stream.messages[len(stream.messages)-1].Offset
	} else {
		tailOffset = Offset(fmt.Sprintf("%010d", 0))
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
		nextOffset = Offset(fmt.Sprintf("%010d", 0))
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

	for _, ch := range stream.subscribers {
		close(ch)
	}
	delete(s.streams, streamID)
	return nil
}

func (s *testStorage) Subscribe(ctx context.Context, streamID string, offset Offset) (<-chan Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, ErrNotFound
	}

	ch := make(chan Offset, 10)
	stream.subscribers = append(stream.subscribers, ch)

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		defer s.mu.Unlock()
		if st, ok := s.streams[streamID]; ok {
			for i, sub := range st.subscribers {
				if sub == ch {
					st.subscribers = append(st.subscribers[:i], st.subscribers[i+1:]...)
					break
				}
			}
		}
		close(ch)
	}()

	return ch, nil
}

// setupInternalTestServer creates a test HTTP server with testStorage for internal tests.
// Uses short timeouts (100ms) to avoid slow tests when long-polling is triggered.
func setupInternalTestServer() (*httptest.Server, *testStorage, *Client) {
	storage := newTestStorage()
	handler := NewHandler(storage, &HandlerConfig{
		LongPollTimeout: 100 * time.Millisecond,
	})
	server := httptest.NewServer(handler)
	client := NewClient(server.URL, &ClientConfig{LongPollTimeout: 100 * time.Millisecond})
	return server, storage, client
}
