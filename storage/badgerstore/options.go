package badgerstore

import (
	"log/slog"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// Default configuration values.
const (
	DefaultMaxMessageSize  = 10 * 1024 * 1024 // 10MB
	DefaultGCInterval      = 5 * time.Minute  // Run value log GC every 5 minutes
	DefaultCleanupInterval = 1 * time.Minute  // Check for expired streams every minute
	DefaultShutdownTimeout = 30 * time.Second // Max wait for graceful shutdown
)

// Options configures the Badger storage.
type Options struct {
	// Dir is the directory for Badger data files.
	// If empty, uses in-memory mode (for testing).
	Dir string

	// InMemory runs Badger in memory-only mode.
	InMemory bool

	// Logger for Badger. If nil, uses default (logs to stderr).
	Logger badger.Logger

	// SLogger is a structured logger for badgerstore operations.
	// If nil, uses slog.Default().
	SLogger *slog.Logger

	// MaxMessageSize limits the size of individual messages.
	// Default: 10MB. Set to 0 to use default.
	MaxMessageSize int

	// GCInterval is how often to run Badger's value log GC.
	// Default: 5 minutes. Set to -1 to disable.
	GCInterval time.Duration

	// CleanupInterval is how often to scan for and delete expired streams.
	// Default: 1 minute. Set to -1 to disable.
	CleanupInterval time.Duration

	// ShutdownTimeout is the maximum time to wait for background goroutines
	// to finish during Close(). If goroutines don't finish within this time,
	// Close() returns anyway to prevent indefinite hangs (e.g., in Kubernetes).
	// Default: 30 seconds. Set to 0 to use default.
	ShutdownTimeout time.Duration
}

// configsMatch checks if two StreamConfigs are equivalent for idempotent create.
func configsMatch(a, b durablestream.StreamConfig) bool {
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
func isExpired(cfg durablestream.StreamConfig) bool {
	return !cfg.ExpiresAt.IsZero() && time.Now().After(cfg.ExpiresAt)
}

// contentTypesMatch compares two content types case-insensitively for the base media type.
func contentTypesMatch(a, b string) bool {
	// Extract base media type (before semicolon)
	if i := strings.IndexByte(a, ';'); i >= 0 {
		a = a[:i]
	}
	if i := strings.IndexByte(b, ';'); i >= 0 {
		b = b[:i]
	}
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}
