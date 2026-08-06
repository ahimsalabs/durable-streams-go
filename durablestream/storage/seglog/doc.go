// Package seglog is a file-backed Storage engine built around a partitioned
// write-ahead log with group commit, modeled on Redpanda's storage layer and
// adapted to many small independent streams.
//
// # Architecture
//
// Stream IDs hash (XXH64, stable across restarts) to one of N logical
// partitions. Each partition has a bounded three-stage worker: one stager owns
// validation, offset and transaction allocation, encoding, and immediate WAL
// writes; one committer snapshots the contiguous-written pending watermark and
// establishes durability through the storage-wide gate; and one FIFO publisher
// updates catalog state and acknowledges requests. Writes continue during
// fdatasync, but those later records remain pending for the next wave.
// Independent requests can share one fdatasync, but each frame remains
// independently replayable — requests are never atomically coupled.
//
// The WAL is the sole durable commit point. Every mutation — create, append,
// close, delete, touch, fork, retention, trim — is one transaction frame in
// exactly one partition's WAL. Per-stream immutable segments are derived state
// written by a background materializer. A cumulative schema-v2 partition
// checkpoint owns the replay position, next transaction ID, and every live
// stream's complete derived state; recovery loads it and replays the WAL suffix.
//
// # Invariants
//
//	I1: A request is acknowledged only after it is in the pending watermark
//	    snapshot taken before a commit wave's fdatasync and its segment's
//	    fdatasync returns successfully. Bytes written during fdatasync are not
//	    covered by that call. Recovery keeps the longest valid frame prefix;
//	    anything discarded was never acknowledged.
//	I2: Frame txnIDs are strictly monotonic per partition. The WAL format has no
//	    group boundaries, so recovery still scans independent frames and needs no
//	    pipeline-specific handling. A txnID gap is corruption and open fails.
//	I3: In-memory state and reader wakeups are published only after the frame
//	    is durable.
//	I4: A stream's partition assignment never changes: the hash is stable and
//	    the partition count is persisted in the FORMAT file. Open refuses a
//	    conflicting Options.Partitions.
//	I5: Per partition, the stager owns validation, encoding, transaction and
//	    offset allocation, and WAL writes; the committer owns durability; and a
//	    single publisher alone publishes logical stream state and completions in
//	    FIFO order. The pending list is bounded and their handoffs preserve order
//	    and backpressure. The stager carries uncommitted logical end-state in its
//	    private overlay and takes stream RLock when seeding from published state.
//	    Other readers also
//	    take consistent snapshots under RLock and never do file I/O while holding
//	    it.
//	I6: All mutations of one stream flow through its single partition worker,
//	    so per-stream operations are totally ordered without cross-partition
//	    coordination.
//	I7: Every segment prefix, active dense-index prefix, and seal footer is
//	    fsync'd before a checkpoint references it. Ordinary materialization
//	    rounds publish unsynced derived prefixes while the WAL covers recovery;
//	    checkpoint cadence establishes a coalesced storage-wide filesystem
//	    durability barrier (Linux syncfs, or a portable per-file/directory
//	    fallback), then writes the partition checkpoint. A full reclaimable WAL
//	    segment or retention/removal forces that sequence immediately.
//	I8: A checkpoint's replay position and cumulative stream map are one
//	    atomic image of the same barrier frontier. Published derived state may
//	    run ahead between checkpoint cadences, but the WAL remains authoritative
//	    for that gap; replay keeps the unknown-stream, stale-incarnation, and
//	    materialized-prefix tolerances.
//	I9: Retention commits opTrim to the WAL, checkpoints the new floor without
//	    dropped segments, and only then publishes, closes, unlinks, and syncs
//	    the stream directory. Pin acquisition shares the physical trim gate.
//
// # On-disk layout
//
//	<dir>/seglog.lock                lock file (single-process fencing)
//	<dir>/FORMAT                     format version + partition count
//	<dir>/wal/p<NN>/wal-<seq>.log    preallocated WAL segments
//	<dir>/wal/p<NN>/checkpoint.json  cumulative partition state (schema v2)
//	<dir>/streams/<shard>/<stream>/seg-<first>.seg  derived stream data
//	<dir>/streams/<shard>/<stream>/seg-<first>.idx  active dense index sidecar
//
// WAL segments begin with a 4KiB header block and contain a sequence of
// self-checking transaction frames (see walrecord.go for the byte layout).
// WAL segment files are preallocated so the zeroed tail never parses as a frame.
package seglog
