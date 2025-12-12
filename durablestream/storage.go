package durablestream

import (
	"context"
	"io"
	"time"
)

// StreamConfig contains creation-time configuration.
type StreamConfig struct {
	ContentType string
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry
}

// StreamInfo contains metadata about a stream.
type StreamInfo struct {
	ContentType string
	NextOffset  Offset
	TTL         time.Duration // Zero means no TTL
	ExpiresAt   time.Time     // Zero means no expiry
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
	Append(ctx context.Context, streamID string, data []byte, seq string) (Offset, error)

	// AppendFrom streams data from an io.Reader to a stream.
	// This avoids buffering the entire request body in memory.
	// The reader is read until EOF or error. Returns the new tail offset.
	// Implementations MUST ensure atomic writes - either all data is persisted
	// or none (per protocol atomicity requirements).
	AppendFrom(ctx context.Context, streamID string, r io.Reader, seq string) (Offset, error)

	// Read returns messages from offset. limit is max total bytes to return.
	// Returns messages and the next offset to read from (Section 5.5).
	Read(ctx context.Context, streamID string, offset Offset, limit int) (*ReadResult, error)

	// Head returns stream metadata without reading data (Section 5.4).
	Head(ctx context.Context, streamID string) (*StreamInfo, error)

	// Delete removes a stream (Section 5.3).
	Delete(ctx context.Context, streamID string) error

	// Subscribe returns a channel notified when new data arrives after offset.
	// The channel receives the new tail offset. Closing the context cancels the subscription.
	Subscribe(ctx context.Context, streamID string, offset Offset) (<-chan Offset, error)
}
