package durablestream

import (
	"context"
	"time"
)

// StreamConfig contains creation-time configuration.
type StreamConfig struct {
	ContentType string
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry
	IsPrivate   bool          // If true, use Cache-Control: private (Section 8.1)
}

// StreamInfo contains metadata about a stream.
type StreamInfo struct {
	ContentType string
	NextOffset  Offset
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry
	IsPrivate   bool          // If true, use Cache-Control: private (Section 8.1)
}

// StoredMessage represents a single message in a stream.
// Each append operation creates one StoredMessage (or multiple if JSON array is flattened).
type StoredMessage struct {
	Data   []byte // Raw bytes of this message
	Offset Offset // Offset after this message
}

// ReadResult contains messages from a storage read.
type ReadResult struct {
	Messages   []StoredMessage // Individual messages in offset order
	NextOffset Offset          // Offset to use for next read
	TailOffset Offset          // Current tail (for up-to-date detection)
}

// Storage defines the interface for stream persistence.
// Implementations must be goroutine-safe.
type Storage interface {
	// Create creates a new stream. Returns (true, nil) if newly created.
	// Returns (false, nil) if stream exists with matching config (idempotent).
	// Returns (false, error) if stream exists with different config.
	Create(ctx context.Context, streamID string, cfg StreamConfig) (created bool, err error)

	// Append writes data to a stream. Returns the new tail offset.
	// seq is optional sequence number for coordination (Section 5.2).
	//
	// The data slice is only valid for the duration of the call; the caller
	// may reuse or modify it after Append returns. Implementations that need
	// to retain the data (e.g., in-memory storage) must copy it.
	Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error)

	// Read returns messages from offset. limit is max total bytes to return.
	// Returns messages and the next offset to read from (Section 5.5).
	Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Head returns stream metadata without reading data (Section 5.4).
	// Returns ErrNotFound if stream doesn't exist (use for existence checks).
	Head(ctx context.Context, streamID string) (*StreamInfo, error)

	// Delete removes a stream (Section 5.3).
	Delete(ctx context.Context, streamID string) error

	// WaitForData blocks until data is available at offset, then returns it.
	// Returns immediately if data already exists at offset.
	// Returns ctx.Err() on timeout/cancellation.
	// Returns ErrNotFound if stream doesn't exist or is deleted while waiting.
	WaitForData(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Close releases resources. Safe to call multiple times.
	Close() error
}
