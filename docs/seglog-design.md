# A storage engine built around the disk's flush budget

seglog is the storage engine behind our Go implementation of the Durable
Streams protocol: append-only named streams over HTTP, readable from any
offset. With synchronous writes enabled, the default, seglog confirms an
append only after `fdatasync` succeeds for the WAL segment that contains it.
Three decisions carry most of the design's weight:

1. Per-stream read segments store message payload bytes verbatim.
2. Writes reach the write-ahead log at arrival; a pending watermark separates
   written records from records confirmed durable.
3. Group commit runs across partitions. Flush completion sets its main pace,
   with a short adaptive boarding window between contended waves.

None of the pieces are novel. The log is a conventional WAL with per-record
checksums and torn-tail truncation. The watermark follows the WAL of the
Rust implementation of the same protocol. Group commit is as old as
databases; the only unusual choice is the level where it operates. Most of
this post is about matching the design to two measured constants of one
NVMe device - and what it costs, in write amplification and commit latency,
when a design ignores them.

This is a single-node, local-filesystem durability guarantee. It protects
against process and machine crashes when the filesystem and device honor
`fdatasync`. It does not protect against loss of the disk or the node. The
engine also has an explicit unsafe mode that skips `fdatasync` for throughput
experiments; confirmed writes can be lost in that mode.

## Two copies of the data

The engine writes each message twice, to two different files, for two
different jobs.

The first copy goes to a **log file**. The log is for durability and
recovery. The engine has a fixed number of partitions (32 by default). A hash
of the stream name assigns each stream to one partition, and each partition
owns one sequence of log files. All writes to one partition go to the end of
its current log file, in order.

The second copy goes to a **stream file**. Each stream has its own files.
A background task copies message data out of the log and into the stream
files. This copy is for reads.

This second representation is a deliberate write and space cost. A
Kafka-style partition log can use one file sequence for durability and reads.
That works well when the API exposes partition order and record-batch framing.
seglog has a different boundary: many independent streams share 32 WAL
partitions, but reads, retention, and forks operate on one stream at a time.
The shared WALs bound the number of flush owners. The per-stream files provide
contiguous payload ranges and independent lifecycle boundaries.

The cost is an extra payload write, a dense index, and a checkpoint invariant
between the two representations. An LSM tree also writes a WAL and a derived
read representation, then rewrites that representation during compaction.
seglog writes each stream payload once during materialization. Sealing appends
the index and footer without rewriting the payload.

## Segments, materialization, and checkpoints

The words *segment*, *materialization*, and *checkpoint* describe three
different boundaries. They are easy to confuse:

- A **segment** is a file rollover unit.
- **Materialization** copies committed payloads from the log into stream
  files.
- A **checkpoint** records which materialized bytes are durable and therefore
  no longer need an older log segment for recovery.

Reaching a segment size does not create a checkpoint. A checkpoint does not
wait for a segment to reach its target size.

### There are two kinds of segment

Each partition has a sequence of **WAL segments**. A WAL segment has a default
logical size of 256 MiB, including a 4 KiB header. A transaction frame must fit
in one WAL segment. If the next frame does not fit in the active segment, the
writer creates the next segment and puts the complete frame there. It never
splits a frame between two WAL segments.

The 256 MiB value is a rollover limit, not an immediate allocation. A WAL file
grows in 16 MiB extents as the writer needs space. A lightly used partition
therefore does not consume 256 MiB only because it has an active WAL segment.
The engine creates WAL segments lazily, so an unused partition has no active
segment.

Each stream has a sequence of **stream segments**. Its persisted segment policy
has a payload target and a maximum open age. The default payload target is 128
MiB. Header, index, and footer bytes do not count toward this target. The
materializer tests the payload size before it copies the next message. It never
splits a message. Thus, the message that crosses the target stays in the old
segment, and at most that one message can take the payload area above the
target. A message larger than the target gets a segment of its own when a later
message arrives.

The engine resolves the policy once when it creates a stream and writes the
resolved value to the create WAL frame. A selector can choose different values
for different stream names or configurations. For example, an application can
use 128 MiB and one hour for RAW streams, but 8 MiB and five minutes for META
streams. A fork target is a new stream: the selector resolves its policy from
the target name and configuration, not from its parent. A retry is idempotent
only when its newly resolved policy equals the stored policy.

Checkpoints contain the same policy for every live stream. Recovery requires a
valid policy in either the checkpoint or the create or fork WAL metadata. It
does not use the current default or call the current selector. Changing process
options therefore affects only later creations. This schema change uses root
format `seglog-format-v4` and checkpoint format 3. WAL and stream-segment
envelopes keep their versions because their binary layouts did not change.

These values are configuration options. They are defaults, not protocol
limits:

| File type | Owner | Default rollover target | Contents |
|---|---|---:|---|
| WAL segment | One partition | 256 MiB | Changes for many streams |
| Stream segment | One stream | 128 MiB | Contiguous payloads for that stream |

The two file sequences have different axes:

```text
Partition WAL, ordered by transaction number

┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ WAL segment 17   │  │ WAL segment 18   │  │ WAL segment 19   │
│ streams A, B, C  │  │ streams A, C, D  │  │ streams B, D     │
│ up to 256 MiB    │  │ up to 256 MiB    │  │ active           │
└──────────────────┘  └──────────────────┘  └──────────────────┘

Per-stream files, ordered by message index

stream A: ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
          │ sealed       │  │ sealed       │  │ active       │
          │ about 128 MiB│  │ about 128 MiB│  │ 23 MiB       │
          └──────────────┘  └──────────────┘  └──────────────┘

stream B: ┌──────────────┐  ┌──────────────┐
          │ sealed       │  │ active       │
          │ about 128 MiB│  │ 4 MiB        │
          └──────────────┘  └──────────────┘
```

### Materialization is an incremental copy between files

The engine does not hold 128 MiB in memory and then write a stream segment.
By default, a materializer runs every 25 milliseconds. It reads each newly
committed payload from the partition WAL and appends that payload to the
active file for its stream. It also appends one 16-byte entry to the active
index sidecar. The copy uses a message-sized memory buffer. The active stream
segment grows a little at a time.

For one message, the normal path is:

```text
client
  │
  │ append
  ▼
partition WAL
  │  1. write the transaction frame
  │  2. flush the WAL
  │  3. confirm the client
  │
  │ later: materializer copies the payload
  ▼
active stream segment and index sidecar
```

This is a real second copy on disk. The WAL copy is in transaction order and
is the recovery source. The stream copy is in message order and is the read
source. Both copies remain live until a checkpoint and a WAL rollover make an
old WAL segment eligible for removal. The current WAL segment cannot be partly
removed. On a low-volume partition, it can remain for a long time even when a
checkpoint points near its end.

The live WAL can make the payload part of disk use approach twice the incoming
data size for the history that it still contains. Record headers, indexes,
checkpoints, and filesystem allocation add more space. Retention controls how
long stream history remains. When materialization keeps up, checkpoints and
whole-segment WAL reclamation limit duplication to the unreclaimed WAL suffix,
including the segment that contains the replay position. There is no hard bound
on that suffix if materialization falls behind.

Ordinary materialization can publish the new stream-file prefix to readers
before it is durable. This is safe because the durable WAL remains the recovery
source. If the machine fails at this point, recovery ignores stream-file state
that the last checkpoint does not name and rebuilds it from the WAL.

### Sealing does not rewrite the payload

While a stream segment is active, it has two files:

```text
active .seg:  [64-byte header][payload A][payload B][payload C]...
active .idx:  [16-byte entry A][16-byte entry B][16-byte entry C]...
```

Each index entry records the payload end offset, the payload CRC32C checksum,
and the batch boundary. The separate sidecar lets the payload bytes remain
contiguous while the segment grows.

When the active segment has reached the 128 MiB target, the materializer seals
it before it writes the next message. Sealing does not read and rewrite the
payload area. It copies only the small index to the end of the existing `.seg`
file and appends a checksummed footer:

```text
sealed .seg:  [header][unchanged payload bytes][dense index][footer]
new .seg:     [header][new payloads...]
```

The sealed file is immutable. A reader can send a payload range from it without
copying the payload through user-space memory. The materializer removes the old
`.idx` sidecar only after a durable checkpoint names the sealed `.seg` file.

Size is not the only seal condition. By default, the engine also seals a
non-empty active segment one hour after the creation time in its segment
header. This is wall-clock open age, not idle age: continuous writes do not
restart it, and restart preserves it. The adaptive materializer schedules the
nearest deadline, so this works without retention and without polling every
stream at a fixed interval. A zero maximum open age disables age rollover.

### A checkpoint is a durable recovery statement

A checkpoint is per partition. By default, the engine materializes every 25
milliseconds and writes an ordinary checkpoint no more often than every 250
milliseconds. A WAL roll, retention work, or shutdown can require an earlier
checkpoint.

The checkpoint describes a point in two views of the same history:

- the WAL segment, byte offset, and next transaction number from which replay
  must continue;
- each live stream's metadata, sealed segment list, durable active-segment
  prefix, and materialized message frontier.

The checkpoint can name any durable active prefix. A stream segment does not
need to be full or sealed. One checkpoint can name a 128 MiB sealed segment for
stream A, a 4 MiB active segment for stream B, and a 20 KiB active segment for
stream C.

The durability order matters:

1. Copy committed WAL payloads into the stream files.
2. Flush every stream payload file, index sidecar, and required directory that
   the new checkpoint will name. On Linux, one shared `syncfs` barrier does
   this work for the filesystem.
3. Atomically replace the partition checkpoint with the new durable image.
4. Remove index sidecars that the checkpoint no longer needs.
5. Remove whole WAL segments that are older than the checkpoint replay
   segment.

```text
                 materialize                 checkpoint

WAL 17 ─┐                                  ┌─ durable stream A prefix
WAL 18 ─┼─ copy payloads into stream files ├─ durable stream B prefix
WAL 19 ─┘                                  └─ replay starts in WAL 19
                                                │
                                                ▼
                                  WAL 17 and WAL 18 can be removed
```

The engine never removes a WAL segment only because one stream reached 128
MiB. A WAL segment usually contains records for many streams. Every record
before the checkpoint frontier must have a durable representation in the
checkpointed stream state. Reclamation also works only on whole WAL segments.
The WAL segment that contains the replay position remains, even if the
checkpoint points near its end.

This ordering handles the important failures:

- **Failure before stream-file flush:** the confirmed WAL record remains, so
  recovery copies it again.
- **Failure after stream-file flush but before checkpoint replacement:** the
  old checkpoint remains authoritative, and recovery replays the WAL. Extra
  stream-file bytes are not treated as committed checkpoint state.
- **Failure after checkpoint replacement but before WAL removal:** both copies
  remain, and recovery starts at the newer replay position. The extra WAL file
  can be removed later.
- **Failure after WAL removal:** the checkpoint already names durable stream
  files, so recovery does not need the removed WAL history.

For example, a 1 GiB stream eventually has about eight 128 MiB stream segments,
plus index and footer overhead. During ingestion, some of the same payload also
exists in one or more WAL segments. The amount of duplicate data depends on the
materialization rate, checkpoint rate, WAL rollover points, and write load. It
is not a fixed extra 256 MiB or a fixed factor of two.

## The log format

The log holds one record for each change: create a stream, append messages,
delete a stream, and so on. A record has:

- a header with a magic number, the operation type, lengths, a timestamp,
  and a transaction number,
- the stream name,
- operation metadata,
- the message payloads,
- a trailer with a closing magic number.

The frame header, the body containing stream identity and operation metadata,
each payload, and the complete frame have CRC32C checksums. The transaction
number increases by exactly one for each record in a partition, with no gaps.

Recovery starts at the checkpoint replay position and requires consecutive
transaction numbers. An invalid non-zero frame in the final WAL segment is a
torn tail. Recovery keeps the preceding valid prefix, truncates and re-zeroes
the remainder, and resumes writing there. An invalid frame in an earlier
segment, a transaction-number gap, or a WAL-segment gap fails startup as
corruption.

The valid prefix can contain a complete frame whose caller did not receive a
confirmation. Recovery can keep such a frame. The guarantee is one-way: every
confirmed frame survives a crash covered by the durability model, but an
unconfirmed frame can also survive.

## Metadata takes the same path as messages

A stream has more state than its messages: a content type, an expiry time,
a closed flag that marks permanent end of stream, retention rules, the last
producer sequence value, and - for a fork - a reference to its source
stream. This post calls all of this metadata.

The engine has one rule for it: **every metadata change is one log record**,
written and flushed exactly like an append. Create carries the full stream
configuration. Delete, close, expiry renewal, and retention changes each
carry their complete new value. There is no separate configuration file, no
metadata database, and no second commit path.

This rule buys three things.

First, metadata gets the same durability as data, at almost no extra cost. With
synchronous writes enabled, a create or delete is confirmed only after a flush
covers its record. An expiry renewal is one small record that rides the same
flush round as the appends around it. The gate makes no distinction: a round
can carry appends from one partition and a delete from another in the same
device flush wave.

Second, recovery has one source of truth. There is no moment where a
configuration file says one thing and the log says another. Replay applies
records in transaction order. Recovery enforces that order with consecutive
transaction numbers. Each metadata record carries an absolute new value, not
a difference, so applying the same record twice is idempotent. Applying
different records out of order would not be equivalent.

Third, the state that can be derived is not stored. A fork record in the
log carries the full reference to its source stream. The engine does not
store a count of how many forks use a source. After a crash, it derives the
counts by scanning the streams that exist. A stored count could disagree
with reality; a derived count cannot.

The current metadata for every stream lives in one in-memory table, rebuilt
at startup from the checkpoint plus the log tail. Appends and reads check
it at the moment of use: does the content type match, is the stream closed,
has the expiry time passed. An expired stream is treated as gone the moment
a request touches it. No background timer has to fire at the right moment
for correctness; background work only reclaims the space later.

Two details follow from production use:

- The last accepted producer sequence is part of the durable metadata. When
  an append supplies a sequence, its WAL frame stores the value and the
  checkpoint later stores the last accepted value and offset. An append
  without a sequence does not change this state. After restart, a producer
  that sends a non-advancing value is still rejected, and the head of the
  stream reports where the producer can continue.
- Delete does not always remove data. If a fork still reads from the
  deleted stream's history, the files stay until the last such fork is
  gone. Clients see the stream as deleted at once - the delete record is
  durable - but the bytes wait. A stream created again with the same name
  gets its own identity and its own files; the old and new files never mix.

The checkpoint completes the picture. Its per-stream image holds the
configuration, the closed flag, the last sequence value, the message count,
the retention floor, the fork reference, and the file list. Recovery loads
this image and replays only the log records after it. Payload bytes stay out
of the checkpoint. The checkpoint stores sealed file names and geometry and
the durable prefix of any active segment. The stream files contain a small
header, payload bytes, index entries, and, when sealed, a footer. Stream
configuration never enters these files, which keeps the payload area
byte-identical to the wire.

## Stream segments store payload bytes verbatim

A read request asks for a range of messages from one stream. In the stream
files, message payloads are stored next to each other, with no headers
between them, in stream order. A separate small index stores one 16-byte
entry per message: where the payload ends, its checksum, and its batch
boundary.

The current direct path applies to non-JSON reads of non-fork streams whose
entire current tail is materialized in immutable sealed segments. The server
passes pinned file ranges to the response writer. A plaintext `net/http`
connection can promote this transfer to `sendfile` or `splice`. JSON responses,
TLS connections, every forked stream, and any stream with an active or
WAL-resident tail use a user-space copied path.

One local test with 1,000 clients reading the same 64 MiB stream observed 5.5
GB/s with the server at 183 MiB of memory. This is a result from that machine
and test setup, not a general throughput guarantee.

Materialization verifies each WAL payload before copying it. The ordinary
`Read` API verifies payload CRC32C again for WAL, active-segment, and sealed-
segment reads. Only the direct file-range path for immutable sealed segments
skips per-read payload verification.

## How a write travels

An append request follows this path:

1. The HTTP handler hashes the stream name and finds the partition.
2. The request goes into that partition's queue. The queue has a fixed
   size. When it is full, callers wait. This is the backpressure boundary.
3. One worker per partition takes requests from the queue, one at a time.
   It validates the request, builds the record, and writes the record to
   the log file **immediately**.
4. The worker adds the record to the partition's pending list and moves to
   the next request. It does not wait for the disk.

At step 3, the record has entered the operating system's page cache but has
not crossed seglog's durability barrier. A machine or power failure can lose
it. That is acceptable because the client has not received a confirmation.
With synchronous writes enabled, seglog confirms the request only after the
covering WAL `fdatasync` succeeds.

The earlier version of this engine held requests back and formed them into
groups before writing. Measurement showed this was a mistake: each request
waited for its group, then the group waited for the disk, and every wait
multiplied. Writing at arrival removes the first wait completely. The very
next flush covers the record, whichever flush that is.

## The watermark

The pending list is the watermark. It holds every record that has been
written to the log but not yet confirmed safe.

A flush call (`fdatasync`) has a useful property: it covers every byte
written to the file *before* the call started. It makes no promise about
bytes written *during* the call.

The flush worker for a partition uses this property with one strict rule:

1. Take the whole pending list. Call it the snapshot.
2. Call `fdatasync` on each distinct WAL segment represented in the snapshot,
   in write order. A snapshot can cross a segment rollover.
3. When every required flush returns without error, confirm the snapshot in
   order.

A record written while step 2 runs is not in the snapshot. It stays in the
pending list and is covered by the next flush. Order is preserved: records
enter the list in log order and leave in log order.

This is the same shape as the Rust server's WAL: reserve, write, and let a
moving mark divide what is safe from what is not.

## The gate

One fact from measurement drives the last part of the design. On our test
disk, a flush that carries data takes 5 to 8 milliseconds, and the disk
completes roughly 120 to 180 real cache-flush commands per second, no
matter how many programs ask. When many flush calls run at the same time,
the kernel merges them into few device commands - but each caller still
waits in line.

This creates a trap for a partitioned engine, and we walked straight into
it. Thirty-two partitions, each flush-clocked on its own schedule, made up
to 3,400 fdatasync calls per second. Each call returned in about a
millisecond - the merged cost, not the real one - so every partition kept
committing two-record groups against a device that could honor perhaps 150
real flushes per second. The result was one quarter of the possible
throughput, and the write amplification described in the next section.

The fix is group commit at the level where the cost actually lives: the
device flush, shared by all partitions, not the partition's own file.
The gate is one shared object per store:

- A flush worker arrives at the gate and waits.
- The gate releases every waiting worker **together**. Each flushes its own
  log file concurrently; the kernel merges the batch into about one device
  command.
- Workers arriving while that cohort runs wait for the next release.
- When the cohort completes, the gate holds the next release open briefly -
  an eighth of the measured cohort duration, at most two milliseconds - so
  workers that just finished can rejoin. Without this window, a worker
  always misses the next release by microseconds and idles a full round,
  which structurally doubles commit latency for every client.
- Then the next cohort releases, and the cycle repeats.

No fixed periodic commit timer sets the overall cadence. Completion of one
flush wave opens the next cycle. After a contended wave, an adaptive boarding
timer waits for one eighth of the measured wave duration, clamped between 200
microseconds and 2 milliseconds. A fast disk therefore produces shorter
windows than a slow disk. When one worker arrives and nothing else is running,
the gate releases it at once.

Each partition also has a publisher. After a flush, it confirms the snapshot's
requests to the HTTP handlers in order. A two-wave buffer normally lets
publication overlap the next flush. If the publisher falls behind, its
backpressure can eventually delay the committer.

Checkpoints follow the same principle in one more place. On Linux, the
engine uses `syncfs`, which flushes the whole filesystem. That call is
global, so issuing it once per partition was waste - 70 filesystem-wide
flushes per second at the default settings. Now all partitions share one
call per round.

## Write amplification

Write amplification is the ratio of bytes the device writes to bytes the
application handed the engine. Our starting point was 244x: 354 MB/s of
device writes carrying about 1.4 MB/s of payload. That number is worth
decomposing, because its terms are independent, they respond to different
fixes, and only some of them are fundamental.

1. **Log framing.** A 256-byte append becomes a record of roughly 340
   bytes: header, stream name, metadata, checksums, trailer. About 1.3x at
   this payload size, asymptotically 1x for large payloads.
2. **Flush granularity.** On the measured filesystem, each `fdatasync` of a
   small change caused page and journal writes much larger than the changed
   record. At two records per commit group, about 700 logical bytes caused
   16-20 KiB of device writes, 3,400 times per second. This was the dominant
   measured term. Group commit is usually presented as a latency-for-
   throughput trade; on this system it also controlled write amplification.
3. **The second copy.** Materialization rewrites every payload into its
   stream file plus a 16-byte index entry. This adds one planned payload write
   and one index entry. Each payload is materialized once, but the amount of
   unreclaimed WAL is not bounded if materialization falls behind.
4. **Page granularity on stream files.** A 256-byte append to one of
   10,000 streams dirties a 4 KiB payload page and an index-sidecar page,
   and writeback usually flushes both before neighboring appends arrive to
   share them. This was the observed cost of small messages, high stream
   cardinality, buffered I/O, and the measured writeback schedule. A different
   filesystem, direct-I/O strategy, or workload can produce a different cost.
5. **Amplification by schedule.** The per-partition checkpoint barrier
   forced the entire filesystem's dirty pages out 70 times per second,
   before any page could accumulate a second write. Same bytes, worse
   timing. Coalescing the barrier removed this measured source of extra writes.

The ladder as the fixes landed, measured at 256 clients:

| Change | Device writes | Amplification |
|---|---:|---:|
| Baseline (terms 2 + 5 dominant) | 354 MB/s | ~244x |
| One shared checkpoint barrier | 178 MB/s | ~120x |
| Real commit groups | 57 MB/s | ~30x |
| Final watermark pipeline, at 2.2x the throughput | 100 MB/s | ~24x |

The remaining ~24x reflects terms 1, 3, and 4 under this workload. Messages of
256 bytes scattered across 10,000 streams are an unfavorable case for page
reuse. For larger messages, arithmetic predicts that the page-granularity
term will move toward the two-representation cost, but we have not measured
that point. For calibration, the LSM engine in the same test wrote about 25x
during the same window. That short window did not include all later
compaction work.

The main avoidable amplification in this test came from *when* bytes were
forced to the device, not only from their format. Two of the five measured
terms were scheduling defects. Layout inspection alone did not expose them;
the block-layer counters in `/sys/block/*/stat` did.

## What it adds up to

All numbers: one machine, NVMe with an honest cache flush, 10,000 streams,
256-byte appends, measured on the same afternoon. seglog runs its default
configuration (32 partitions). These are observations from one local 15-second
benchmark window. They are not general product comparisons, and this
repository does not yet contain a reproducible report with the raw results.

| Server | 256 clients | 1,024 clients | Peak memory |
|---|---|---|---|
| seglog | 15,000/s, p50 16 ms, p99 37 ms | 47,600/s, p50 19 ms, p99 60 ms | ~190 MB |
| BadgerDB engine | 15,800/s, p50 16 ms, p99 26 ms | 21,600/s, p50 48 ms, p99 66 ms | - |
| Rust server | 9,500/s, p50 24 ms, p99 152 ms | 31,800/s, p50 25 ms, p99 202 ms | 3.4 GB |

The one number the LSM engine still wins is the 256-client tail. seglog
carries background materialization and checkpoint barriers in the same
window; the LSM defers its reorganization to compaction, which this
15-second window never charges. The 1,024-client column shows the same
machinery winning on every metric once the commit pipeline is the
bottleneck.

At the start of this work, seglog reached 5,900 appends per second on the
256-client test. The gap to the other engines was not one defect. It was
three: a filesystem barrier issued 32 times too often, flush calls made at
32 times the useful rate, and a write path that made each request wait
twice before its bytes existed anywhere. Each fix came from a measurement,
and one of the fixes came from reading the competitor we were losing to.

The durability rule held in the process-kill test used during this work:
append, send `SIGKILL` to the server, restart it, and count survivors. Every
confirmed message was present after restart. This test exercises process-crash
recovery. It does not simulate power loss because `SIGKILL` does not discard
the kernel page cache.
