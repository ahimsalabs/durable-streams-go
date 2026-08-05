// Package seglog is a file-backed Storage engine built around a partitioned
// write-ahead log with group commit, modeled on Redpanda's storage layer and
// adapted to many small independent streams.
//
// # Architecture
//
// Stream IDs hash (XXH64, stable across restarts) to one of N logical
// partitions. A single worker goroutine owns each partition: it is the only
// writer of the partition's WAL and the only mutator of the in-memory state of
// the partition's streams, which gives per-stream ordering, deduplication, and
// offset assignment without a global write lock. Independent requests are
// group-committed: their frames share one write and one fdatasync, but each
// frame commits or fails on its own — requests are never atomically coupled.
//
// The WAL is the sole durable commit point. Every mutation — create, append,
// close, delete, touch, fork, retention, trim — is one transaction frame in
// exactly one partition's WAL. Per-stream immutable segments and manifests are
// derived state written by a background materializer; recovery is always
// "load manifests, replay the WAL suffix past the checkpoint".
//
// # Invariants
//
//	I1: A request is acknowledged only after its frame's group fdatasync.
//	    Recovery keeps the longest valid frame prefix; anything discarded was
//	    never acknowledged.
//	I2: Frame txnIDs are strictly monotonic per partition. A txnID gap during
//	    replay is corruption and open fails, leaving all bytes intact.
//	I3: In-memory state and reader wakeups are published only after the frame
//	    is durable.
//	I4: A stream's partition assignment never changes: the hash is stable and
//	    the partition count is persisted in the FORMAT file. Open refuses a
//	    conflicting Options.Partitions.
//	I5: Only the owning partition worker mutates a stream's state; readers
//	    take consistent snapshots under RLock and never do file I/O while
//	    holding it.
//	I6: All mutations of one stream flow through its single partition worker,
//	    so per-stream operations are totally ordered without cross-partition
//	    coordination.
//
// # On-disk layout
//
//	<dir>/seglog.lock                lock file (single-process fencing)
//	<dir>/FORMAT                     format version + partition count
//	<dir>/wal/p<NN>/wal-<seq>.log    preallocated WAL segments
//
// WAL segments begin with a 4KiB header block and contain a sequence of
// self-checking transaction frames (see walrecord.go for the byte layout).
// Segment files are preallocated so the zeroed tail never parses as a frame.
package seglog
