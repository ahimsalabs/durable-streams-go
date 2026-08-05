# Pebble log prototype

This directory is a small, intentionally incomplete prototype for comparing a
Pebble-backed append log with the current Badger backend. It is **not** wired
into the server and does not claim `durablestream.Storage` conformance. The
prototype focuses on the two primitives that a future backend needs:

* one ordered key range per stream, with metadata and message records committed
  in one Pebble batch; and
* bounded retention by message age and/or bytes, with an explicit `ErrGone`
  result for offsets before the retained head.

The dependency remains internal until a complete backend exists. The key
layout uses a hex-encoded stream ID and an 8-byte big-endian message position,
so arbitrary IDs cannot collide with metadata or another stream's range.

## Retention model

`Options{MaxAge, MaxBytes}` describes a policy; `Retain` applies it for one
stream. A deployment can call `Retain` from a periodic sweeper, or trigger it
after a commit when a stream crosses a size threshold. The operation deletes
whole messages in offset order and atomically advances the persisted
`Earliest` watermark. Reads before that watermark return `ErrGone`; reads from
the watermark onward remain normal. This is the same contract a file/segment
backend should expose when it drops old segments.

Retention is deliberately explicit rather than hidden in `Read`: a slow reader
must receive `ErrGone`, not silently skip messages, and pruning can be metered
and rate-limited. A production implementation also needs a tombstone or
generation fence so a concurrent stream recreation cannot race an old purge.

## Content-aware batching

`PlanBatches` bounds groups by request count, bytes, and JSON-vs-binary content
type. It **never concatenates request bodies**. JSON values must remain grouped
by their originating append for fork sub-offsets and atomic replay; a backend
may still put all records in one Pebble transaction. The planner is a policy
seam, not a replacement for protocol validation (`ProcessJSONAppend`) or the
per-message limits enforced by the real storage backend.

## What remains before production

An adapter would need the complete lifecycle and fork semantics from
`durablestream.Storage`, crash-safe retention/recovery, close and deduplication
state, waiters, and a conformance test run. Pebble's `Batch.Commit` with
`WriteOptions.Sync=true` gives the same basic fsync tradeoff as Badger, but
does not by itself provide per-stream files or zero-copy range reads; a
file/segment backend remains the better fit for the Rust-style read path.
