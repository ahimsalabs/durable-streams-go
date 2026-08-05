# bboltstore prototype

`bboltstore` is an intentionally small alternative backend for evaluating a
page/mmap design. bbolt maps one database file and lets the kernel fault message
pages in on demand. A stream is one nested bucket containing JSON metadata and
length-prefixed (timestamp + payload) message values. An append updates the
metadata, message, and retention window in one transaction.

```go
s, err := bboltstore.New("streams.db", bboltstore.Options{
    DefaultRetention: bboltstore.Retention{
        MaxBytes: 256 << 20,
        MaxAge:   7 * 24 * time.Hour,
    },
})
defer s.Close()
```

`SetRetention` changes the policy for an existing stream. Limits are enforced
at append boundaries. `MaxBytes` removes oldest messages until the retained
payload is within the cap; `MaxAge` removes messages older than the append
time. An offset before the retained window returns `durablestream.ErrGone`.
There is no background reaper in this prototype, so an idle stream is not
trimmed until its next append or an explicit `SetRetention`.

## Why this is not yet the production backend

* bbolt permits only **one write transaction at a time**. Independent streams
  therefore queue behind one writer; batching helps amortize fsync but cannot
  create Badger-style write parallelism.
* Values are copied from the mmap for the `Storage` API. A future range-reader
  interface could avoid that copy for catch-up responses, but the current
  protocol interface returns owned byte slices.
* The file grows when old buckets/values are deleted; reclaim requires bbolt's
  offline `Compact` operation. Retention limits logical history, not necessarily
  the database file size.
* ForkStorage, cross-process fencing, and a content-aware JSON batch index are
  deliberately absent. This package implements the core `Storage`, atomic
  append/close operations, and retention semantics to make benchmark and design
  comparisons concrete.
* `Options.NoSync` is an explicit unsafe benchmark mode. With the default
  setting, bbolt syncs each write transaction before it returns.

The prototype is useful as a metadata/index or low-write-rate backend and as a
baseline for a custom per-stream file store. It should not replace the current
Badger backend without conformance, crash-recovery, and 10k/100k-stream tests.
