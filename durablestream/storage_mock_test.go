package durablestream

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
)

// testStorage is a minimal storage implementation for internal tests.
// It provides basic functionality without importing external packages.
type testStorage struct {
	mu      sync.RWMutex
	streams map[string]*testStream
}

type testStream struct {
	config      StreamConfig
	data        []byte
	messages    [][]byte // Individual messages for JSON mode
	offsets     []int
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
		config:  cfg,
		data:    make([]byte, 0),
		offsets: []int{0},
	}

	// Initialize messages slice for JSON mode
	if isJSONContentType(cfg.ContentType) {
		stream.messages = make([][]byte, 0)
	}

	s.streams[streamID] = stream
	return true, nil
}

// isJSONContentType returns true if the content type is application/json.
func isJSONContentType(contentType string) bool {
	parts := strings.Split(contentType, ";")
	mediaType := strings.TrimSpace(parts[0])
	return strings.EqualFold(mediaType, "application/json")
}

func (s *testStorage) Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return "", ErrNotFound
	}

	stream.data = append(stream.data, data...)

	// Track message for JSON mode
	if stream.messages != nil {
		stream.messages = append(stream.messages, data)
	}

	newOffset := len(stream.offsets)
	stream.offsets = append(stream.offsets, len(stream.data))
	offset := Offset(fmt.Sprintf("%010d", newOffset))

	// Notify subscribers
	for _, ch := range stream.subscribers {
		select {
		case ch <- offset:
		default:
		}
	}

	return offset, nil
}

func (s *testStorage) AppendReader(ctx context.Context, streamID string, r io.Reader, seq string) (Offset, error) {
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
		fmt.Sscanf(string(offset), "%d", &offsetIdx)
	}

	if offsetIdx >= len(stream.offsets) {
		return nil, ErrGone
	}

	startPos := stream.offsets[offsetIdx]
	endPos := len(stream.data)
	if limit > 0 && endPos-startPos > limit {
		endPos = startPos + limit
	}

	nextOffsetIdx := len(stream.offsets) - 1
	for i := offsetIdx + 1; i < len(stream.offsets); i++ {
		if stream.offsets[i] >= endPos {
			nextOffsetIdx = i
			break
		}
	}

	data := make([]byte, endPos-startPos)
	copy(data, stream.data[startPos:endPos])

	result := &ReadResult{
		Data:       data,
		NextOffset: Offset(fmt.Sprintf("%010d", nextOffsetIdx)),
		TailOffset: Offset(fmt.Sprintf("%010d", len(stream.offsets)-1)),
	}

	// For JSON mode, return individual messages
	if stream.messages != nil && offsetIdx < len(stream.messages) {
		endMsgIdx := nextOffsetIdx
		if endMsgIdx > len(stream.messages) {
			endMsgIdx = len(stream.messages)
		}

		result.Messages = make([][]byte, 0, endMsgIdx-offsetIdx)
		for i := offsetIdx; i < endMsgIdx && i < len(stream.messages); i++ {
			result.Messages = append(result.Messages, stream.messages[i])
		}
	}

	return result, nil
}

func (s *testStorage) Head(ctx context.Context, streamID string) (*StreamInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, ok := s.streams[streamID]
	if !ok {
		return nil, ErrNotFound
	}

	return &StreamInfo{
		ContentType: stream.config.ContentType,
		NextOffset:  Offset(fmt.Sprintf("%010d", len(stream.offsets)-1)),
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
func setupInternalTestServer() (*httptest.Server, *testStorage, *Client) {
	storage := newTestStorage()
	handler := NewHandler(storage)
	server := httptest.NewServer(handler)
	client := NewClient().BaseURL(server.URL)
	return server, storage, client
}
