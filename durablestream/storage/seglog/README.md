# `seglog` storage backend

`seglog` is a file-backed `durablestream.Storage` for many independent
streams. It favors explicit crash ordering and predictable write ownership
over a minimal on-disk layout.

## Design in one screen

Stream IDs hash to a fixed partition count persisted in `FORMAT`. Each
partition has one worker goroutine, one request queue, and one segmented WAL.
The worker is the only WAL writer and the only owner of logical stream state
for that partition. It validates queued operations against a staging overlay,
encodes independent transaction frames into a commit group, writes the group,
calls `fdatasync`, then publishes catalog changes and wakes readers.

A background materializer copies committed payloads from the partition WAL
into per-stream segment files. Full segments become immutable; a stream
manifest names the sealed segments, active segment prefix, metadata, and
materialized frontier. After all dirty streams are durable, a partition
checkpoint advances and WAL segments older than the checkpoint can be
unlinked. Recovery loads manifests and replays the valid WAL suffix from each
checkpoint. Manifests may be ahead of a checkpoint, so replay is idempotent.

Fork reference counts and physical-trim pins are derived in-memory state, not
independent durable truth. Recovery reconstructs them from surviving,
generation-fenced parent links. Readers take short metadata snapshots and do
file I/O without holding stream locks.

## Durability and failure behavior

With the default `SyncWritesEnabled` behavior:

1. A successful mutation has passed the commit group's `fdatasync`. Recovery
   retains the longest valid WAL-frame prefix.
2. Transaction IDs increase without gaps within a partition. A gap or damage
   outside the final WAL tail is corruption, and `New` fails without trying to
   guess or rewrite that history.
3. Catalog mutations and waiter notifications happen only after the WAL commit
   is durable.
4. Stream routing is stable: FNV-1a hashing is fixed and reopening with a
   different partition count is rejected.
5. One partition worker owns logical mutations; readers consume consistent
   snapshots and never perform file I/O while holding the state lock.
6. Every mutation of one stream passes through the same worker, establishing a
   total per-stream order without a global writer lock.

Commit-group frames are independently committed; a group is an fsync-sharing
unit, not a cross-request transaction. A write or sync error fail-stops that
partition because the durable state of its tail may be ambiguous. The
triggering mutation and all later mutations on that partition return the
latched error until reopen. Other partitions and already-committed reads keep
working. `SyncWritesDisabled` is provided only for throughput experiments: an
acknowledged mutation may then be lost after a machine crash.

## Retention and forks

Retention advances a logical floor at whole-segment boundaries. Reads below
that floor return `durablestream.ErrGone`; byte and age limits are therefore
soft bounds, and the newest record is preserved. The floor is committed to the
WAL before a manifest can stop naming old segment files. The manifest is made
durable before those files are unlinked.

A fork pins the physical source history it can still read. Retention may
advance the source's public logical floor while such pins exist, but physical
segment removal pauses until the final descendant releases its pin. Pin
acquisition and unlink share a per-stream gate, preventing a check-then-unlink
race. Recovery derives pins from fork topology and completes interrupted trims
from the durable floor and manifest.

## Capabilities and current limitations

The package implements `durablestream.Storage`, `AtomicBatchStorage`,
`AtomicCloseStorage`, `ForkStorage`, and `TouchHeadStorage`.

Current limitations are explicit:

- File-descriptor use grows with open WAL and stream segments; there is no
  descriptor LRU yet.
- Each retention sweep is O(streams) within a partition.
- Reads copy payloads; there is no zero-copy HTTP or `sendfile` path.
- Directory fencing uses a single-process advisory `flock`. Non-Unix builds
  cannot enforce this fence, and multi-host shared-directory access is not
  supported.
- Linux uses `fdatasync` and `fallocate`; portable builds fall back to full
  `fsync` and sparse `truncate`.
- There is no remote tiering or offload path.

## Benchmarks

The following headline results were measured on Linux 7.0 with an AMD Ryzen 9
7950X (16 cores / 32 threads) and the local NVMe-backed, encrypted Btrfs
`/home` filesystem. Each benchmark used `-benchtime=2s`; durable append numbers
include real `fdatasync`. The cold-segment read reopens the store, but the
operating-system page cache may still be warm.

| Benchmark | Result | Additional metric |
| --- | ---: | ---: |
| Durable append, sequential, 256 B | 4.59 µs/op (218k ops/s) | p50 4.74 µs; p99 8.68 µs |
| Durable append, parallel group commit | 448 ns/op (2.23M ops/s) | p99 71.8 µs |
| Append without sync, 256 B | 3.58 µs/op (280k ops/s) | ceiling only; not crash durable |
| Many streams, append once each | 3.69 µs/stream (1k) | 3.75 µs/stream at 10k and 100k |
| Hot WAL reads, 64 KiB pages | 1.20 GB/s | 54.8 µs/page |
| Reopened segment reads, 64 KiB pages | 770 MB/s | 85.1 µs/page |
| Writes during active retention | 4.99 µs/op (200k ops/s) | 1 KiB payloads |
| Clean checkpoint recovery | 1.87 ms/open+close | 4,096-record fixture |
| Full WAL suffix recovery | 19.1 ms/open+close | 16,384-record fixture |

Run the storage conformance suite and benchmarks with:

```sh
go test ./durablestream/storage/seglog/ -run '^TestConformance$' -count=1
go test ./durablestream/storage/seglog/ -race -count=1
go test ./durablestream/storage/seglog/ -run '^$' -bench=. -benchtime=2s
go test ./durablestream/storage/seglog/ -run '^$' -fuzz=FuzzDecodeFrame -fuzztime=15s
```
