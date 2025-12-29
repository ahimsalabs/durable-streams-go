package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// BatchedWriter wraps a StreamWriter to provide automatic batching for appends.
// Multiple concurrent Send calls are automatically combined into a single
// HTTP request, significantly improving throughput for high-frequency writes.
type BatchedWriter struct {
	client      *Client
	path        string
	contentType string
	offset      Offset

	mu       sync.Mutex
	buffer   []*pendingAppend
	inflight bool
	closed   bool

	// Condition variable for signaling batch completion
	cond *sync.Cond
}

// pendingAppend represents a buffered append waiting to be sent.
type pendingAppend struct {
	ctx  context.Context
	data []byte
	seq  string

	// Channel to signal completion
	done chan error
}

// NewBatchedWriter creates a BatchedWriter for the given stream path.
// Always call Close() when done to release resources.
func (c *Client) BatchedWriter(ctx context.Context, path string) (*BatchedWriter, error) {
	info, err := c.Head(ctx, path)
	if err != nil {
		return nil, err
	}

	bw := &BatchedWriter{
		client:      c,
		path:        path,
		contentType: info.ContentType,
		offset:      info.NextOffset,
		buffer:      make([]*pendingAppend, 0),
	}
	bw.cond = sync.NewCond(&bw.mu)
	return bw, nil
}

// Send appends raw bytes to the stream with automatic batching.
// Multiple concurrent Send calls may be combined into a single HTTP request.
// Returns when the data has been successfully written to the server.
func (bw *BatchedWriter) Send(data []byte, opts *SendOptions) error {
	if len(data) == 0 {
		return ErrBadRequest
	}

	var seq string
	if opts != nil {
		seq = opts.Seq
	}

	// Create pending append
	pending := &pendingAppend{
		ctx:  context.Background(),
		data: data,
		seq:  seq,
		done: make(chan error, 1),
	}

	bw.mu.Lock()
	if bw.closed {
		bw.mu.Unlock()
		return ErrClosed
	}

	// Add to buffer
	bw.buffer = append(bw.buffer, pending)

	// If no request in flight, start one
	if !bw.inflight {
		bw.inflight = true
		batch := bw.buffer
		bw.buffer = make([]*pendingAppend, 0)
		bw.mu.Unlock()

		// Process batch in goroutine
		go bw.processBatch(batch)
	} else {
		bw.mu.Unlock()
	}

	// Wait for completion
	err := <-pending.done
	return err
}

// SendJSON marshals v as JSON and appends to the stream with batching.
func (bw *BatchedWriter) SendJSON(v any, opts *SendOptions) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return bw.Send(data, opts)
}

// processBatch sends a batch of pending appends as a single HTTP request.
func (bw *BatchedWriter) processBatch(batch []*pendingAppend) {
	err := bw.sendBatch(batch)

	// Notify all pending appends
	for _, p := range batch {
		p.done <- err
	}

	// Check for more buffered appends
	bw.mu.Lock()
	if len(bw.buffer) > 0 {
		// More appends came in while we were sending - process them
		nextBatch := bw.buffer
		bw.buffer = make([]*pendingAppend, 0)
		bw.mu.Unlock()
		go bw.processBatch(nextBatch)
	} else {
		bw.inflight = false
		bw.cond.Broadcast()
		bw.mu.Unlock()
	}
}

// sendBatch sends a batch of appends as a single HTTP request with retry logic.
func (bw *BatchedWriter) sendBatch(batch []*pendingAppend) error {
	if len(batch) == 0 {
		return nil
	}

	// Use first context (all should be similar)
	ctx := batch[0].ctx

	// Check if any context is already cancelled
	for _, p := range batch {
		if p.ctx.Err() != nil {
			return p.ctx.Err()
		}
	}

	// Find highest seq number (last non-empty seq)
	var highestSeq string
	for i := len(batch) - 1; i >= 0; i-- {
		if batch[i].seq != "" {
			highestSeq = batch[i].seq
			break
		}
	}

	// Build request body based on content type and batch size
	var body []byte
	isJSON := isJSONContentType(bw.contentType)

	if len(batch) == 1 {
		// Single item: send as-is (no array wrapping for JSON)
		body = batch[0].data
	} else if isJSON {
		// Multiple items in JSON mode: wrap all items in an array
		items := make([]json.RawMessage, len(batch))
		for i, p := range batch {
			items[i] = json.RawMessage(p.data)
		}
		var err error
		body, err = json.Marshal(items)
		if err != nil {
			return err
		}
	} else {
		// Multiple items in byte mode: concatenate all data
		totalSize := 0
		for _, p := range batch {
			totalSize += len(p.data)
		}
		body = make([]byte, 0, totalSize)
		for _, p := range batch {
			body = append(body, p.data...)
		}
	}

	// Send the batch with retry logic for transient failures
	maxRetries := 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		resp, err := bw.client.transport.Append(ctx, transport.AppendRequest{
			Path:        bw.path,
			Data:        body,
			ContentType: bw.contentType,
			Seq:         highestSeq,
		})
		if err != nil {
			// Check if this is a retryable error
			var tErr *transport.Error
			if errors.As(err, &tErr) {
				if tErr.StatusCode == 500 || tErr.StatusCode == 503 || tErr.StatusCode == 429 {
					lastErr = err
					time.Sleep(10 * time.Millisecond)
					continue
				}
			}
			return convertTransportError(err)
		}

		bw.offset = Offset(resp.NextOffset)
		return nil
	}
	return convertTransportError(lastErr)
}

// Offset returns the current tail offset after the last successful append.
func (bw *BatchedWriter) Offset() Offset {
	return bw.offset
}

// Close stops accepting new appends and waits for pending appends to complete.
func (bw *BatchedWriter) Close() error {
	bw.mu.Lock()
	bw.closed = true

	// Wait for inflight batch to complete
	for bw.inflight {
		bw.cond.Wait()
	}
	bw.mu.Unlock()

	return nil
}

// isJSONContentType checks if the content type is JSON.
func isJSONContentType(ct string) bool {
	return strings.HasPrefix(ct, "application/json")
}
