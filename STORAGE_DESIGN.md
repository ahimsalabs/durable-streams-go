# Storage backend prototypes

This document records the deliberately small design target for alternative
backends. The existing Badger backend remains the default until an alternative
passes the complete storage conformance suite and the high-cardinality
benchmarks.

## Adopted design: seglog

`durablestream/storage/seglog` implements the design below at full scope and
passes the complete storage conformance suite (all four capabilities,
including forks). Its shape follows Redpanda's storage layer adapted to many
independent streams: stream IDs hash to fixed partitions (count persisted in
the FORMAT file); each partition worker group-commits every mutation as one
transaction frame in a partition WAL (one fdatasync per group, requests never
atomically coupled); a per-partition materializer copies committed records
into per-stream sealed segments with manifests, checkpoints, and reclaims
fully-reflected WAL segments. Retention keeps a WAL-durable monotonic floor
(ErrGone below it), deletes only sealed unpinned segments, and orders floor →
manifest → unlink. Forks are one frame in the target's partition: the child's
durable parent reference is the pin record, and refcounts/pins are derived at
recovery. See `durablestream/storage/seglog/doc.go` for invariants I1–I6.

## File-backed target

The closest match to the Rust server is not another key/value database. It is
an append-only file store:

* one active data segment per stream (with sealed segments rotated by size),
* a compact per-stream index/sidecar containing message boundaries, tail,
  incarnation, metadata, and the retention floor,
* a sharded WAL for crash recovery and group-commit `fdatasync`, and
* a bounded in-memory tail/notification state for live readers.

Sealed segments make retention cheap: complete segments can be unlinked without
rewriting the active stream. Reads use `ReaderAt`/section readers; the HTTP
layer can later add an optional range-reader capability to use the kernel's
file-to-socket fast path. The ordinary `Storage.Read` API still returns owned
message slices and remains the compatibility path.

## Content-aware batching

Batching must not change message or append atomicity. The scheduler should:

1. preserve per-stream order with the existing stream lock;
2. group independent requests by storage/WAL shard;
3. cap each commit by entries, estimated bytes, and wait time; and
4. isolate oversized records instead of allowing one large request to create
   head-of-line delay for small records.

Content type is a scheduling hint, not a reason to combine unrelated logical
messages. JSON-array flattening remains one atomic append; binary and JSON
payloads should not be concatenated into a single logical record. A practical
first implementation is size classes (small/medium/large) plus the existing
byte/entry/time bounds.

## History retention

Retention is distinct from stream expiry. A stream can remain live while its
oldest records are discarded. A policy can contain:

* `max_age`: discard records older than this duration;
* `max_bytes`: discard oldest complete records until the logical history is
   within this budget; and
* zero values meaning unlimited.

Retention is a soft upper bound because only complete records/segments are
removed. The newest record is retained even when it is larger than
`max_bytes`. Roll segments independently with `segment_bytes` and
`segment_age`; the active segment is never deleted. Once a segment is sealed,
either history limit can make it eligible for deletion (the earlier of the two
limits wins), just as in Redpanda. A background sweeper should run per shard
(not one timer per stream), and append can opportunistically enqueue a stream
for trimming.

When a read offset is below the retained floor, return `ErrGone` (HTTP 410),
never silently skip data. Persist the floor and expose it through an optional
`EarliestOffset` field so clients can recover without probing. `WaitForData`
must return the same error for a stale offset.

Forks require an explicit policy: either pin source segments through every
fork boundary, or materialize/copy the inherited prefix before allowing source
retention. Silent deletion of bytes still visible through a child is unsafe.

## Backend guidance

* A file backend should own payload bytes and its recovery metadata. Using one
  database for metadata and another for payloads creates a cross-store atomicity
  problem.
* A small WAL library can be reused as an implementation detail, but its sync
  and recovery contract must be wrapped and tested against the storage
  semantics.
* bbolt/LMDB/Pebble are useful experiments for metadata or as Badger
  alternatives; none alone provides the direct stream-file/range-read design.
