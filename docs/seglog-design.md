# seglog Storage Engine Design

**Status:** Implemented design for root format `seglog-format-v4`.

seglog is a single-node storage engine for named, append-only streams. It is
the storage engine for the Go Durable Streams server. This document defines
its current data model, durability protocol, file formats, and operating
limits.

## 1. Scope and non-goals

### 1.1 Scope

seglog provides these functions:

- It stores independent streams on a local filesystem.
- It appends records in stream order.
- It reads records from a stream offset.
- It stores stream metadata in the same write-ahead log as stream data.
- It supports stream deletion, retention, and forks.
- It recovers committed state after a process or machine failure.

The default durability mode acknowledges a mutation only after `fdatasync`
succeeds for each WAL segment that contains the mutation. This guarantee
depends on correct filesystem and storage-device behavior.

The unsafe mode skips synchronous WAL flushes. This mode can lose
acknowledged mutations after a process, kernel, machine, or power failure.

### 1.2 Non-goals

seglog does not provide these functions:

- Replication across disks or nodes.
- Protection from permanent loss of the local storage device.
- A transaction across streams or partitions.
- A hard limit on total disk use.
- Online migration from an older root, checkpoint, WAL, or stream format.
- A global order across partitions.

## 2. Terminology

**Active stream segment:** A mutable stream segment with a payload file and an
index sidecar.

**Barrier:** A position in the partition publisher's FIFO order. A
materialization batch captures the replay position and dirty-stream snapshots
at this position.

**Checkpoint:** A durable per-partition description of stream state and the
WAL replay position.

**Commit:** The operation that gives admitted WAL frames their final durability
result and then publishes their results in FIFO order.

**Dirty stream:** A stream with published state that a completed
materialization barrier does not yet cover.

**Fork:** A stream that reads a retained prefix from a source stream and then
continues with its own records.

**Frame:** One checksummed WAL encoding of one mutation.

**Logical byte:** A byte in a file's defined address range. Logical size does
not imply physical block allocation.

**Materialization:** The copy of committed WAL state into per-stream files. A
later checkpoint can make this derived state authoritative for recovery.

**Partition:** One independently ordered WAL and its worker pipeline.

**Published:** Applied to in-memory stream state with a final commit result.

**Replay position:** The WAL segment and byte offset where recovery starts.

**Sealed stream segment:** An immutable stream segment with its dense index and
footer in one `.seg` file.

**Stream segment policy:** The immutable payload target and maximum open age
selected when a stream is created.

**WAL segment:** One file in a partition's write-ahead log sequence.

## 3. Architecture

The store has a configurable number of partitions. The default is one.

The store routes a stream name with XXH64. The partition count and routing
algorithm are persisted in `FORMAT`. Routing is stable across restart.

Each partition has these ordered components:

```text
caller
  │
  ▼
bounded admission queue
  │
  ▼
stager ── write frame at arrival ──▶ partition WAL
  │                                  │
  │ add to pending FIFO              │ fdatasync covering snapshot
  ▼                                  ▼
committer ───────────────────────▶ FIFO publisher
                                      │
                                      ├─ result to caller
                                      ├─ in-memory stream state
                                      └─ dirty-stream set
```

The stager, committer, and publisher are separate pipeline stages. The queue
and the pending list are bounded by admission and pipeline backpressure.

The store has two persistent representations of record payloads:

```text
Partition WAL order

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ WAL segment 17   │  │ WAL segment 18   │  │ WAL segment 19   │
│ streams A, B, C  │  │ streams A, C, D  │  │ streams B, D     │
│ sealed           │  │ sealed           │  │ active           │
└──────────────────┘  └──────────────────┘  └──────────────────┘

Per-stream record order

stream A: ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
          │ sealed       │  │ sealed       │  │ active       │
          └──────────────┘  └──────────────┘  └──────────────┘

stream B: ┌──────────────┐  ┌──────────────┐
          │ sealed       │  │ active       │
          └──────────────┘  └──────────────┘
```

The WAL supplies commit durability and recovery history. Stream segments
supply stream-local payload layout and lifecycle boundaries. Reads compose
sealed segments, the active segment, and committed WAL-tail state.

## 4. Invariants

1. **Stable routing.** A stream maps to one persisted partition for the life of
   the store format.
2. **Partition order.** WAL transaction numbers increase by one without gaps
   within a partition.
3. **Frame containment.** One frame or `ProducerGroup` cannot cross a WAL
   segment boundary.
4. **Write-before-commit.** The stager writes an admitted frame before the
   committer can include it in a pending snapshot.
5. **FIFO publication.** The publisher applies final results in partition WAL
   order.
6. **Synchronous acknowledgement.** In the default mode, a successful result
   follows a successful covering WAL `fdatasync`.
7. **Immutable stream policy.** Recovery uses the persisted stream segment
   policy. It does not resolve the policy again.
8. **Record containment.** Materialization never splits one record across
   stream segments.
9. **Immutable seal.** A sealed stream segment never changes.
10. **Checkpoint barrier.** A checkpoint replay position and its owned
    dirty-stream snapshots come from the same FIFO barrier.
11. **Post-barrier separation.** A mutation after a barrier remains dirty and
    remains after that barrier's replay position.
12. **Frozen retry.** A failed checkpoint retry uses the same captured batch.
    It does not merge later mutations into that batch.
13. **Reclaim after checkpoint.** The engine removes required WAL or stream
    files only after a durable checkpoint no longer depends on them.
14. **Logical retention.** The retained floor does not decrease and survives
    restart. Physical file removal can occur later.
15. **Read pin safety.** The engine does not unlink a file while a read or fork
    pin requires it.

## 5. Configuration and defaults

| Option | Default | Meaning |
|---|---:|---|
| `Partitions` | 1 | Number of WAL partitions |
| `MaxMessageSize` | 10 MiB | Maximum message payload size |
| `WALSegmentBytes` | 256 MiB | Logical WAL segment size, including header |
| `WALExtentBytes` | 16 MiB | Logical WAL growth extent |
| `QueueDepth` | 256 | Bounded queue capacity per partition |
| `SyncWrites` | true | Require a covering WAL flush before success |
| `ShutdownTimeout` | 30 s | Maximum synchronous shutdown wait |
| `MaterializeBytes` | 4 MiB | Unmaterialized WAL pressure threshold |
| `MaterializeMaxAge` | 250 ms | Oldest unmaterialized-record age threshold |
| `CheckpointBytes` | 32 MiB | Materialized WAL-frame byte threshold |
| `CheckpointMaxAge` | 3 s | Age threshold from first successful uncheckpointed materialization |
| `DefaultSegmentPolicy.TargetBytes` | 128 MiB | Stream segment payload target |
| `DefaultSegmentPolicy.MaxOpenAge` | 1 h | Maximum non-empty active segment open age |
| `RetentionInterval` | 30 s | Background retention sweep interval |
| `FDCacheSize` | 384 | Open-file cache capacity |

`Dir` selects the root directory. An empty `Dir` creates temporary storage and
removes it on close.

A selector can set a stream segment policy at stream creation. The engine
persists the selected policy. Later default or selector changes do not change
an existing stream.

A zero segment maximum open age disables age sealing. A retention cadence of
`-1` disables the background retention loop. A materialization maximum age of
`-1` disables process-level byte and age triggers. It does not disable
persisted stream age-seal deadlines. A checkpoint maximum age of `-1` forces a
checkpoint in every materialization round.

## 6. On-disk layout and formats

The root layout has this form:

```text
root/
├── FORMAT
├── wal/
│   ├── p0000/
│   │   ├── checkpoint.json
│   │   ├── wal-0000000000000011.log
│   │   └── wal-0000000000000012.log
│   └── p0001/
│       └── ...
└── streams/
    └── <shards>/<stream-identity>/
        ├── seg-0000000000000000.seg
        ├── seg-0000000000000001.seg
        └── seg-0000000000000001.idx
```

The stream path uses sharding and an internal stream identity. A recreated
stream does not reuse the deleted incarnation's files.

The format versions are:

| Object | Version |
|---|---|
| Root `FORMAT` | `seglog-format-v4` |
| Checkpoint schema | 3 |
| WAL | 1 |
| Stream segment | 2 |

The engine refuses older formats. It does not migrate them.

`FORMAT` also stores the partition count and the XXH64 routing identifier.

A WAL segment starts with a 4 KiB header. WAL frames contain operation type,
transaction number, stream identity, metadata, payload lengths, and payloads.
CRC32C values protect the frame header, body, each payload, and complete frame.
A closing magic value terminates the frame.

An active stream segment has this form:

```text
.seg: [64-byte header][payload A][payload B][payload C]...
.idx: [16-byte entry A][16-byte entry B][16-byte entry C]...
```

Each index entry stores the payload end offset, payload CRC32C, and batch
boundary. A sealed segment has this form:

```text
.seg: [header][unchanged payload bytes][dense index][checksummed footer]
```

The checkpoint stores stream metadata, segment geometry, durable active
prefixes, materialized record frontiers, fork metadata, and the replay
position. It does not store record payloads.

## 7. Write and commit pipeline

Admission uses one bounded queue per partition. A caller waits when the queue
is full. Context cancellation can stop a mutation only before admission.

After admission, the caller waits for the final result. The engine borrows the
caller's payload memory until that result is returned. The caller must not
modify or release the payload during this interval.

The stager validates and encodes the mutation. It writes the frame at arrival.
It then adds the frame and result handle to the pending FIFO list.

The committer snapshots the current pending prefix. The snapshot can contain
frames in two WAL segments after rollover. The committer calls `fdatasync` on
each distinct WAL segment in write order. A frame written after the snapshot
remains pending for the next commit.

The FIFO publisher gives each frame its final result in order. It updates
in-memory stream state only after the commit outcome is known. Publisher
backpressure can delay the committer.

Committers across partitions use a shared commit-wave gate. Workers in one
wave flush their WAL files concurrently. Workers that arrive during an active
wave wait for the next wave. After a contended wave, the gate uses a short
boarding window. The window is one eighth of the preceding wave duration. It
has a minimum of 200 microseconds and a maximum of 2 ms. An idle gate releases
a single worker without a fixed periodic timer.

If `SyncWritesDisabled` is selected, the pipeline does not wait for a WAL
durability barrier. A successful result in this mode is not crash-durable.

## 8. `ProducerGroup`

A `ProducerGroup` collects multiple append mutations for one partition. It is
single-use. All stream names in the group must route to the same partition.

The complete group uses one queue item. The stager encodes independent WAL
frames and writes them with one contiguous write. One covering sync applies to
the group. The complete encoded group must fit in one WAL segment.

A `ProducerGroup` is not an atomic transaction. Each frame has independent
validation, offset, sequence result, and final result. Recovery processes each
valid frame independently. A torn tail can contain a valid frame prefix.

The group borrows all caller payload memory until `Commit` returns. Queue depth
does not bound the total caller memory referenced by one admitted group. The
caller must bound group size as well as satisfy the WAL segment limit.

Cancellation can stop `Commit` only before queue admission. After admission,
`Commit` waits for every final result.

A WAL write or sync error can leave frame durability unknown. Affected results
include `ErrDurabilityUnknown` and preserve the underlying system error. The
partition then enters fail-stop state for writes.

## 9. Materialization

Materialization copies published WAL payloads to active stream segments. It
also applies metadata and lifecycle changes to owned stream snapshots.

The default triggers are 4 MiB of unmaterialized WAL-frame bytes or an oldest
age of 250 ms. Pressure or age starts work. The implementation does not scan
all streams on a fixed polling interval.

A capacity-one wake signal coalesces notifications. Each active partition
retains a 1 MiB payload buffer and a 64 KiB index buffer. Consecutive payload
and index writes are coalesced into these buffers. A payload larger than the
payload buffer bypasses that retained buffer.

The ordinary sequence is:

```text
WAL frame
   │ verify payload checksum
   ▼
active .seg payload prefix
   │
   ├─ append 16-byte index entry to active .idx
   └─ publish readable materialized prefix
```

An ordinary materialized prefix can become readable before checkpoint
durability. The WAL remains the recovery source for state after the last
checkpoint.

## 10. Stream segment rollover

The default stream segment target is 128 MiB of payload. The header, index, and
footer do not count toward this target.

The engine never splits a record. It writes the record that crosses the target
to the current segment. It seals that segment before the next record. A single
large record can therefore make a segment larger than the target.

Size sealing copies the index to the end of the `.seg` file and writes a
footer. It does not rewrite payload bytes. The engine removes the obsolete
`.idx` sidecar only after a durable checkpoint names the sealed form.

The default maximum open age is one hour. Open age starts at the creation time
in the segment header. Writes do not reset it. Restart preserves it. The age
rule applies only to a non-empty active segment.

Age-seal work is deadline-driven. If due work remains after a failure or
incomplete round, the scheduler retries after a fixed 100 ms delay. It does
not use exponential backoff.

## 11. Checkpoint protocol and barrier invariant

Checkpoint byte pressure counts materialized WAL-frame bytes. The default byte
threshold is 32 MiB.

Checkpoint age starts with the first successful materialization that no
durable checkpoint covers. The default age threshold is three seconds. Idle
time before that materialization does not count.

A checkpoint is forced in these conditions:

- Checkpoint byte or age pressure is due.
- The materialized replay position advances to another WAL segment.
- The batch contains removals or victim files.
- `CheckpointMaxAge` is `-1`, which forces every materialization round.
- Final shutdown materialization runs.

The partition publisher creates one FIFO barrier. At this barrier it captures
the WAL replay position and owned snapshots of all dirty streams. It replaces
the dirty set before later publications enter it.

```text
publisher FIFO

pre-barrier records ──▶ [ barrier B ] ──▶ post-barrier records
                              │
                              ├─ capture replay position B
                              ├─ capture owned dirty-stream snapshots
                              └─ install a new dirty set

checkpoint B: state at or before B
next batch:   mutations after B
```

This gives the barrier invariant:

> A checkpoint replay position and all stream snapshots in that checkpoint
> come from the same publisher FIFO barrier. Every post-barrier mutation stays
> dirty and has WAL position after the captured replay position.

The durability sequence is:

1. Materialize the frozen stream snapshots.
2. Sync all payload files, index sidecars, and directories that the checkpoint
   will require.
3. Write a temporary checkpoint in the partition directory.
4. Sync and close the temporary checkpoint.
5. Rename it atomically to `checkpoint.json`.
6. Sync the checkpoint directory.
7. Remove obsolete sidecars, victim stream files, and eligible whole WAL
   segments.

On Linux, required stream-file durability uses a coalesced `syncfs` service.
One `syncfs` call serves each request epoch. Requests that arrive during an
active call join the next epoch. The portable path syncs the required files
and directories individually.

If checkpoint preparation or installation fails, the engine retains the
frozen batch. A retry uses the same snapshots and replay position. Newer
mutations remain in the next dirty set.

## 12. Recovery

Recovery validates `FORMAT`, partition routing, checkpoint schema, WAL headers,
WAL sequence, and stream segment metadata. A version mismatch fails startup.

Each partition loads its checkpoint and starts WAL replay at that checkpoint's
replay position. Replay requires consecutive transaction numbers and WAL
segment numbers.

An invalid non-zero frame in the final WAL segment is a possible torn tail.
Recovery keeps the valid prefix. It truncates and clears the invalid suffix
before it resumes writes.

An invalid frame in an older WAL segment is corruption. A transaction-number
gap or WAL-segment gap is also corruption. These conditions fail startup.

A valid frame can survive even if its caller did not receive success. The
durability contract does not guarantee absence of unacknowledged frames.

Recovery reconstructs stream metadata from the checkpoint and WAL tail. It
uses persisted stream policies. It derives fork pin counts from live fork
references.

## 13. Reads and direct spans

A read can use three state sources:

1. Immutable sealed stream segments.
2. The durable or readable prefix of an active stream segment.
3. Committed WAL-tail records that are not yet materialized.

The read path composes these sources in stream order. It verifies CRC32C for
ordinary reads from WAL, active segments, and sealed segments.

A direct span is available only for a non-fork stream when the full requested
tail consists of immutable sealed data. A direct span pins the source files for
the read duration. The direct path does not verify each payload checksum again.

The HTTP transport can use a zero-copy kernel path for a direct span. The
design does not guarantee use of `sendfile`, `splice`, or another specific
system call. TLS, JSON encoding, forks, active data, or WAL-tail data require a
copied path.

## 14. Retention and forks

Retention can use `MaxBytes`, maximum record age, or both. Zero disables the
corresponding limit.

`MaxBytes` is a physical segment-payload target. For compressed v3 segments it
counts compressed frame bytes (not indexes, footers, or sidecars). It is not a
hard disk quota. The engine
advances retention at whole sealed-segment granularity and preserves records
that must remain readable.

Compressed materialization accumulates WAL-backed records until the partition
reaches `MaterializeBytes` or the oldest record reaches
`CompressionMaxBlockAge` (10 seconds by default). This lets low-rate streams
approach the 1 MiB compression target without retaining encoder state. Reads
remain visible from the WAL during accumulation. Each materialization visit
closes its final partial frame, so compression state does not survive the visit
or a restart.

Retention advances a logical floor. Reads below the floor return a gone
result. The floor survives restart. Record indexes do not change.

Physical deletion uses whole sealed stream segments. The active segment is not
partially removed. Read pins and fork pins can delay unlink after the logical
floor advances.

A fork can retain source segments below the source stream's logical floor. The
engine unlinks those segments only after the last fork and read pin releases
them. Deleting a source does not invalidate an existing fork's pinned history.

The default background sweep interval is 30 seconds. A value of `-1` disables
the background sweep. Request-time validity checks and the persisted logical
floor do not depend on the sweep.

## 15. Scheduling and retries

Write commits are completion-driven. The shared gate coordinates concurrent
partition flushes. It does not impose a fixed commit interval.

Materialization is pressure-driven and deadline-driven. A capacity-one wake
prevents unbounded wake accumulation. The scheduler tracks the nearest age
deadline instead of polling every stream.

Checkpoint sync requests use epochs. A request that arrives during an active
Linux `syncfs` call waits for the next call.

Transient materialization and checkpoint failures retain due work for retry.
The retry retains the frozen barrier batch when one exists. Age-seal due work
uses a fixed 100 ms retry delay.

A WAL write or WAL sync error is different. The partition latches its first WAL
failure and refuses later writes. It does not retry writes on that partition.
Other partitions can remain available.

## 16. Resource bounds, admission, and disk-full behavior

The queue depth bounds admitted queue items per partition. The publisher and
committer apply backpressure through their bounded pipeline. Retained payload
and index coalescing buffers have fixed sizes per active partition.

A queue item can reference caller-owned payload memory. One `ProducerGroup`
can contain many frames. The caller must impose an application-level bound on
group memory.

A WAL segment has a default logical limit of 256 MiB, including its 4 KiB
header. The file grows lazily in 16 MiB logical extents. On Linux, the engine
uses `fallocate`. It falls back to sparse truncate when the platform or
filesystem does not support allocation. Physical allocation therefore depends
on the filesystem and platform.

The store has no global hard disk budget. It has no free-space admission
control. `DiskUsage` is observational. Retention `MaxBytes` does not constrain
WAL, metadata, indexes, checkpoints, filesystem overhead, or pinned files.

WAL use has no hard bound when materialization, checkpointing, or reclamation
lags or fails. Whole-segment reclamation also retains the segment that contains
the replay position.

`ENOSPC` can occur during WAL allocation, WAL writes, materialization,
checkpoint installation, or file metadata operations. A WAL allocation, write,
or sync failure puts that partition in fail-stop state. An attempted frame can
return `ErrDurabilityUnknown`. Background materialization or checkpoint errors
retain work and retry, but free space must become available for progress.

## 17. Metrics

`Stats` contains aggregate `PartitionStats`, `CommitWaves`, and
`PerPartition`. Counters and histograms reset when the process starts. They are
not persisted across restart.

The complete `PartitionStats` fields are:

| Field | Meaning |
|---|---|
| `GroupsCommitted` | Partition pending-list snapshots processed in commit waves |
| `OpsCommitted` | Operations processed in those snapshots |
| `WALBytesWritten` | WAL frame bytes written |
| `CommitFdatasyncNanos` | Time spent in WAL durability calls |
| `GroupSizeHist` | Ten-bucket histogram of operations per partition snapshot |
| `CommitterIdleNanos` | Time the committer had no commit work |
| `PublishNanos` | Time used to publish final results |
| `MaterializerSyncs` | Per-file sync calls attempted by the portable checkpoint fallback |
| `SyncfsCalls` | Actual Linux `syncfs` calls |
| `CheckpointRounds` | Checkpoint rounds attempted, including failed attempts |
| `PendingWALBytes` | Written frame bytes without a final publish result |
| `UnmaterializedWALBytes` | Published frame bytes not covered by a completed materialization barrier |
| `OldestUnmaterializedAge` | Age of the oldest frame before that barrier |
| `MaterializedNotCheckpointedBytes` | Materialized frame bytes not covered by a durable checkpoint |
| `UnreclaimedWALBytes` | Logical bytes in WAL segments that still exist |
| `CurrentWALSegmentBytes` | Logical bytes used in the active WAL segment |
| `CurrentWALSegmentCapacityBytes` | Current logical capacity of the active WAL segment |
| `CurrentWALSegmentUtilization` | Used bytes divided by logical capacity |
| `CheckpointReplayPosition` | Segment and byte offset where this partition replays |

Aggregate byte and counter fields are sums across partitions.
`OldestUnmaterializedAge` is the maximum age across partitions.
`CheckpointReplayPosition` is not aggregated because partition WAL positions
are not comparable. Use `PerPartition` for replay positions.

`CommitWaves` counts process-wide commit-gate waves. `DiskUsage` reports
observed physical allocation through its separate API. It falls back to
logical file size on platforms that do not expose allocated block counts.
Neither metric defines an admission limit.

## 18. Shutdown and platform behavior

`Close` stops new admission and drains the write pipeline. It requests final
materialization and a final checkpoint for every partition. It then releases
resources and removes temporary storage when applicable.

The default shutdown timeout is 30 seconds. If shutdown exceeds the timeout,
`Close` returns an error. It leaves files open and defers final teardown so
active workers do not use closed files. A caller must treat timeout as an
incomplete graceful shutdown.

Linux uses `fdatasync` for WAL durability and `syncfs` for coalesced
materialization durability. Linux retries interrupted sync operations. Linux
uses `fallocate` for WAL extents when supported.

The portable fallback uses full file sync where `fdatasync` is unavailable. It
syncs required files and directories instead of using `syncfs`. Filesystem
allocation and directory-sync guarantees vary by platform.

Supported Unix platforms lock the store directory against a second process.
Platforms without that lock implementation do not detect a second opener.
Opening one store from multiple processes is unsupported on all platforms.

The durability guarantee requires the operating system, filesystem, storage
controller, and device to honor the selected sync operation. seglog cannot
detect false flush completion from lower layers.

## 19. Design trade-offs

The partitioned WAL limits flush ownership and permits parallel staging. It
also removes global transaction order and requires persisted routing.

Per-stream segments provide contiguous stream payloads and independent
retention boundaries. They add a second payload write, a dense index, and a
checkpoint consistency protocol.

Payload-only segment targets keep record data limits independent of index and
footer size. Whole-record and whole-segment rules can exceed logical targets.

Write-at-arrival reduces time before a frame can join a commit snapshot. It
requires a pending FIFO and borrowed caller memory until publication.

Shared commit waves and coalesced filesystem syncs coordinate costs that apply
across partitions. They can couple latency across otherwise independent
partitions.

Frozen checkpoint batches preserve a simple recovery invariant across retries.
They retain memory and delay reclamation while checkpoint work cannot complete.

Whole-segment reclamation simplifies crash safety. It can retain substantial
logical and physical space after the logical frontier advances.

Direct spans avoid payload copying only for fully immutable requested tails.
This restriction preserves stable file geometry and pin safety.

## 20. Benchmark requirements

Current throughput, write amplification, and tail-latency effects require
measurement. This design makes no current claim that recent changes improved
tail latency.

A benchmark report must define hardware, kernel, filesystem, mount options,
storage cache policy, process settings, workload, stream cardinality, message
size distribution, client concurrency, test duration, warm-up, and durability
mode. It must distinguish process-failure tests from power-loss tests.

Each reported workload must include:

- Throughput.
- Latency at p50, p95, p99, and p99.9.
- `PendingWALBytes` and its age or duration distribution.
- `UnmaterializedWALBytes` and `OldestUnmaterializedAge`.
- `MaterializedNotCheckpointedBytes`.
- `UnreclaimedWALBytes` and per-partition replay positions.
- Commit-wave, WAL sync, materializer sync, `syncfs`, and checkpoint counts.
- Logical file bytes and physical allocated bytes.
- Relevant write, sync, allocation, rename, and zero-copy syscall counts.
- Block-device bytes written and resulting write amplification.

The report must include steady-state and recovery behavior. It must run long
enough to include materialization, checkpoint, rollover, retention, and file
reclamation. Comparative results require equivalent durability and workload
settings.

The repository includes `BenchmarkAppendBackgroundScheduling`. It compares
foreground-only work, the default background policy, and high checkpoint
pressure. Run it on the storage device under test:

```sh
TMPDIR=/path/on/test-device GOMAXPROCS=16 \
  go test ./durablestream/storage/seglog \
  -run '^$' \
  -bench '^BenchmarkAppendBackgroundScheduling$' \
  -benchmem -benchtime=5s -count=10
```

Five seconds is the minimum duration for the default three-second checkpoint
age to occur. Longer runs are necessary for rollover and reclamation tests.
