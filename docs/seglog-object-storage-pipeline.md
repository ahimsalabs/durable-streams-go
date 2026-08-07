# A durable event agent for the edge

## Pitch

seglog is not a distributed event broker. It is a lightweight, durable local
handoff between event producers and shared systems.

An edge agent accepts NetFlow records, logs, metrics, or audit events. It writes
each accepted event to local disk before it confirms the write. A remote outage
then creates a disk backlog instead of an in-memory backlog or an application
failure. The agent can replay the backlog when the connection returns. This
guarantee requires synchronous seglog writes.

The agent has four jobs:

1. Accept low-latency local writes.
2. Buffer accepted events on local disk.
3. Provide recent replay and a direct live stream.
4. Deliver events to Redpanda, object storage, or another shared system.

This design keeps the edge data plane small. It leaves replication, consumer
groups, and distributed ownership to systems that already provide them.

## Recommended deployment

The strongest general architecture uses seglog at the edge and Redpanda at the
shared boundary:

```text
producers ──▶ edge agent and seglog ──▶ Redpanda
                         │                  │
                         │                  ├──▶ live consumers
                         │                  ├──▶ ClickHouse
                         │                  └──▶ Parquet ──▶ object storage
                         │
                         └──▶ direct recent or live access
```

The systems have separate responsibilities:

- seglog protects events from a producer process failure and a temporary
  network outage.
- Redpanda provides a shared, replicated log and coordinates consumers.
- Object storage holds large, immutable archive files.
- ClickHouse provides replicated, low-latency queries.

Redpanda lets the Parquet builder wait for an efficient batch size without
depending on one edge disk. It also lets the builder move between nodes and
lets many consumers read the same event stream.

This architecture adds one local storage layer. That layer is useful when edge
links fail, collectors restart, or an in-memory producer buffer is not an
acceptable durability boundary.

## Small deployments can omit Redpanda

A smaller deployment can ship directly from seglog:

```text
producer ──▶ seglog ──▶ Parquet and manifest ──▶ object storage
                 │                                      │
                 └──▶ direct live access                ▼
                                                    ClickHouse
```

This form is suitable when one main pipeline consumes the data and permanent
loss of the edge node is an accepted risk. Object storage is the authoritative
shared copy. ClickHouse is a derived query view that can be rebuilt from the
archive.

The shipper can run in the edge agent, in a local sidecar, or in a central
service that reads from a saved seglog cursor. A deployment assigns one active
archive owner to each stream.

## Parquet building is continuous but object commits are periodic

The Parquet builder reads records as soon as seglog commits them. It does not
wait for a seglog segment to roll, seal, or materialize. Before materialization,
the read path serves the record from the write-ahead log.

The builder writes records to an open local Parquet file. It closes the file
when it reaches a configured byte, row, or time limit. It then uploads one new
immutable object. It does not append to an object that is already committed.

```text
seglog ── continuous read ──▶ open local Parquet file
                                      │ close on a limit
                                      ▼
                              new immutable object
                                      │
                                      ▼
                              manifest commit
```

The normal target is a large object, not one small object every few seconds.
For example, a deployment can test targets such as 64 to 256 MiB and a time
limit of one to five minutes. These values are starting points, not defaults.
The correct values depend on traffic, recovery targets, and object-store cost.

Direct seglog streaming or Redpanda supply the live path while the Parquet
file remains open. If data must reach shared durable storage more quickly than
an efficient Parquet batch permits, use Redpanda or upload small record batches
and compact them into Parquet later.

## Object commits must be recoverable

The shipper processes one ordered range at a time:

1. Read records after the last committed cursor.
2. Build and close a local file.
3. Upload it with a deterministic, create-only object key.
4. Verify its size and digest.
5. Add it to a manifest with a conditional update.
6. Advance the committed cursor from the new manifest head.

The manifest, not an object listing or a local file, defines committed archive
data. A retry can repeat work, but it must not omit or overwrite a committed
range. ClickHouse ingestion is also at least once, so each row needs a stable
source identity for duplicate removal.

## The durability limit is explicit

seglog is node-local storage. It protects confirmed writes when a process or
host restarts and the disk remains intact. It does not protect unshipped events
from permanent loss of the node and its disk.

Use Redpanda or another synchronous replicated copy when an acknowledgment must
survive immediate node loss. A recoverable cloud disk reduces operational risk,
but it does not provide the same guarantee as broker replication.

Current seglog retention cannot protect a consumer cursor. A strict delivery
pipeline must therefore disable automatic retention for an active stream. The
agent can delete only a closed stream incarnation that is fully shipped. It
must stop or reject writes before the disk is full. A future cursor-based trim
API can release data through the durable downstream cursor.

Operators must monitor producer silence and shipping lag as separate failures.
They must also monitor backlog age, backlog bytes, free disk, and the last
shared committed cursor.

## Product boundary

The value of seglog is not a smaller Kafka or Redpanda. Its value is a durable
edge handoff that isolates producers from process restarts and unreliable
networks.

Distribution starts after the edge node. Redpanda remains the better tool for
replication, shared replay, consumer groups, and automatic ownership transfer.
seglog makes delivery to that distributed system safer and simpler.
