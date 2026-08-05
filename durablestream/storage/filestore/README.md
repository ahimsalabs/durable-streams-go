# `filestore` prototype

This package is an intentionally small file-backed `Storage` implementation
for evaluating a Rust-style storage layout. Each stream gets a directory with
`meta.json` and an append-only `data.log`. A record has a fixed binary header
(magic, logical message index, timestamp, payload length and CRC32) followed by
the payload. Startup scans and validates the log and truncates a torn final
record. Reads use `ReadAt`; payloads are not retained in a Go heap log and can
be served from the operating-system page cache.

`Options.MaxBytes` and `Options.MaxAge` implement a simple per-stream history
floor. When a cap is crossed, the oldest records are dropped and the live file
is compacted. Reads before the retained floor return `ErrGone`. A single
oversized message is retained so a stream never becomes unreadable.

`AppendBatch` writes a batch in one contiguous write and one metadata update,
which is the hook for a future content-aware/group committer. It is not yet a
cross-request batcher, and `SyncWrites` currently fsyncs each call. The package
does not implement forks, zero-copy HTTP range responses, sealed segments, or
remote tiering. Those are deliberate follow-ups rather than hidden behavior.

The production Rust implementation extends this basic shape with immutable
fixed-size sealed segments and a manifest. A segment boundary is chosen at a
complete JSON value, then the old prefix can be deleted (or offloaded) once the
manifest watermark is durable. A production Go backend should adopt that
manifest/watermark scheme instead of rewriting the entire live file on every
retention eviction.

The package implements `durablestream.Storage`, `AtomicBatchStorage`, and
`AtomicCloseStorage`, so it can be used by conformance tests after the usual
fork-specific cases are excluded. It is a prototype, not the default backend.
