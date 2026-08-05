package badgerstore

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Default configuration values.
const (
	DefaultMaxMessageSize = 10 * 1024 * 1024 // 10MB for disk-backed storage
	// DefaultInMemoryMaxMessageSize stays one byte below Badger's 1 MiB
	// threshold. Badger v4 rejects values above that threshold but mishandles a
	// value exactly equal to it in memory mode, so equality is not safe either.
	DefaultInMemoryMaxMessageSize = 1*1024*1024 - 1
	DefaultGCInterval             = 5 * time.Minute  // Run value log GC every 5 minutes
	DefaultCleanupInterval        = 1 * time.Minute  // Check for expired streams every minute
	DefaultShutdownTimeout        = 30 * time.Second // Max wait for graceful shutdown
	DefaultReapInterval           = 1 * time.Minute  // Sweep orphaned data every minute
)

// SyncWrites selects whether Badger fsyncs a write before acknowledging it.
// Durable appends use bounded group commit, so concurrent independent calls
// can share one synchronous Badger transaction and WAL flush.
type SyncWrites int

const (
	// SyncWritesDefault fsyncs in disk mode and is ignored in memory mode.
	// This is the zero value: acknowledged appends survive process death.
	SyncWritesDefault SyncWrites = iota

	// SyncWritesEnabled forces fsync on every write. Badger ignores this in
	// memory mode, where there is nothing to sync.
	SyncWritesEnabled

	// SyncWritesDisabled acknowledges writes before they reach stable storage.
	// This raises append throughput substantially (often by an order of
	// magnitude) at the cost of durability: appends acknowledged in the last
	// moments before a crash or SIGKILL can be lost. Only choose this when the
	// stream data can be reconstructed or discarded.
	SyncWritesDisabled
)

// enabled resolves the setting against the storage mode.
func (m SyncWrites) enabled(onDisk bool) (bool, error) {
	switch m {
	case SyncWritesDefault:
		return onDisk, nil
	case SyncWritesEnabled:
		return true, nil
	case SyncWritesDisabled:
		return false, nil
	default:
		return false, fmt.Errorf("badgerstore: invalid SyncWrites value %d", int(m))
	}
}

// Options configures the Badger storage.
type Options struct {
	// Dir is the directory for Badger data files.
	// If empty and InMemory is false, New creates an ephemeral disk directory
	// and removes it on Close. Set InMemory for a strictly memory-only store.
	Dir string

	// InMemory runs Badger in strictly memory-only mode. It is mutually exclusive
	// with Dir. Badger requires in-memory values to remain below 1 MiB, so
	// MaxMessageSize must not exceed DefaultInMemoryMaxMessageSize.
	InMemory bool

	// Logger for Badger. If nil, uses default (logs to stderr).
	Logger badger.Logger

	// SLogger is a structured logger for badgerstore operations.
	// If nil, uses slog.Default().
	SLogger *slog.Logger

	// MaxMessageSize limits the size of individual messages.
	// Set to 0 to use 10 MiB for disk-backed storage or 1 MiB minus one byte
	// for InMemory.
	MaxMessageSize int

	// GCInterval is how often to run Badger's value log GC.
	// Default: 5 minutes. Set to -1 to disable.
	GCInterval time.Duration

	// CleanupInterval is how often to scan for and delete expired streams. An
	// expired stream retained by child forks is soft-deleted instead; its bytes
	// remain available only through descendants until their references release.
	// Default: 1 minute. Set to -1 to disable.
	CleanupInterval time.Duration

	// ReapInterval is how often to sweep for orphaned data left behind by an
	// interrupted purge (for example, one cut short by a crash). Data from
	// streams deleted by this process is purged as soon as Delete commits,
	// regardless of this interval.
	// Default: 1 minute. Reaping cannot be disabled.
	ReapInterval time.Duration

	// SyncWrites controls whether writes are fsynced before being
	// acknowledged. The zero value fsyncs disk-backed storage; see the
	// SyncWrites constants for the durability/throughput tradeoff.
	SyncWrites SyncWrites

	// ShutdownTimeout is the maximum time to wait for background goroutines
	// to finish during Close(). If goroutines don't finish within this time,
	// Close() returns an error and defers closing the database until they exit,
	// rather than hanging indefinitely or closing storage they may still use.
	// Default: 30 seconds. Set to 0 to use default.
	ShutdownTimeout time.Duration

	// AppendCommitMaxInFlight bounds how many grouped append transactions may
	// commit concurrently in durable SyncWrites mode. The default (0 → 1)
	// serializes commits, which batches adaptively and minimizes fsyncs; it is
	// the right choice for ordinary disks, where concurrent commits fragment
	// groups and multiply fsyncs. Raise it only on storage with very cheap
	// fsync, and only after measuring. Ignored when the group committer is
	// disabled (unsynced or in-memory stores).
	AppendCommitMaxInFlight int
}
