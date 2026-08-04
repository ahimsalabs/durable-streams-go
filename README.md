# durable-streams-go

[![Go Reference](https://pkg.go.dev/badge/github.com/ahimsalabs/durable-streams-go.svg)](https://pkg.go.dev/github.com/ahimsalabs/durable-streams-go/durablestream)

Go implementation of [Durable Streams](https://github.com/durable-streams/durable-streams).

## Conformance

| Suite | Version | Tests | Passed | Failed | Skipped |
|-------|---------|-------|--------|--------|---------|
| Server (memorystorage) | v0.3.6 | 338 | 243 | 89 | 6 |
| Server (badgerstore) | v0.3.6 | 338 | 243 | 89 | 6 |
| Client | v0.2.12 | 269 | 224 | 28 | 17 |

Client features: `batching`, `sse`, `longPoll`, `streaming`, `dynamicHeaders`

Known server gaps account for all 89 failures: 31 closure-dependent cases and 58 stream-fork/soft-delete cases. The 6 skipped server cases cover subscriptions. Closure, forks, soft deletion, and subscriptions are newer protocol areas that are not implemented yet.

Of the 28 client failures, 27 require stream closure. The remaining resilience case expects an automatic retry of a bare POST append; this client deliberately does not retry a non-idempotent append because the first attempt may already have committed. The 17 skips cover unadvertised auto-mode and retry-option features, batch validation options, and two suite-disabled cross-implementation SSE cases.

## Known limitations

- Stream closure, forks/soft deletion, and subscriptions are not implemented yet, as reflected in the conformance results above.
- Idempotent-producer state and Handler lifecycle coordination are process-local. Use one Handler per Storage; producer deduplication state does not survive a process restart.

## Offset Format

This implementation uses the same offset format as the reference Node.js implementation:
```
<readSeq>_<byteOffset>
```
Both components are 16-digit zero-padded integers, e.g., `0000000000000000_0000000000000042`.
This ensures lexicographic sortability as required by PROTOCOL.md Section 8.

## Test Coverage

These figures come from a representative `task test` run
(`go test -race -cover ./...`). Coverage in the storage backends can vary by a
few tenths of a percentage point because their concurrency tests exercise
different branches depending on scheduling.

| Package | Coverage |
|---------|----------|
| durablestream | 90.8% |
| durablestream/transport | 91.9% |
| durablestream/storage | 100.0% |
| durablestream/storage/memorystorage | about 96% |
| durablestream/internal/protocol | 98.5% |
| durablestream/storage/badgerstore | about 88% |

Badgerstore also has [7 fuzz tests](durablestream/storage/badgerstore/fuzz_test.go) covering stream ID validation, stream operations, sequence ordering, and concurrent operations. Run them with Go's built-in fuzzing support, for example `go test -fuzz=Fuzz -fuzztime=30s ./durablestream/storage/badgerstore`.

## Upgrade notes

- The `Storage` interface now includes `Touch`, which custom backends must implement to support sliding TTL renewal. New backends should run the reusable `durablestream/storage/storagetest` suite.
- `AtomicBatchStorage` is an optional storage capability used for all-or-nothing initial content and multi-message JSON appends. A Handler backed by a custom implementation without this capability returns `501 Not Implemented` for those requests rather than risk a partial commit; ordinary empty creates and single-message appends still work.
- `StreamInfo` and `ReadResult` can expose an opaque incarnation ID. Custom backends should populate it to enable safe ETag validation and cross-incarnation read detection.
- The generation-scoped Badger key layout is not automatically compatible with databases written by earlier revisions. Opening a legacy directory returns an error matching `badgerstore.ErrLegacyFormat` **without modifying its data**; migrate the directory explicitly or discard it before reopening.
- `badgerstore.Options{InMemory: true}` is now strictly memory-only, defaults to a message limit one byte below Badger's 1 MiB threshold, and rejects a larger configured limit instead of silently using disk. The zero-value options retain the 10 MiB limit by using an ephemeral disk directory that `Close` removes.
- Disk-backed Badger storage now fsyncs acknowledged writes by default. Set `badgerstore.Options.SyncWrites` to `badgerstore.SyncWritesDisabled` only when higher throughput is worth possible crash-time data loss.
- The Handler does not enable cross-origin browser access by default. Set `HandlerConfig.EnableCORS` only for a deliberate any-origin, credential-free policy; otherwise configure trusted origins in outer middleware.
- Client operations now default to a 30-second non-streaming timeout, 64 MiB response limit, and 16 MiB SSE-event limit. All are configurable through `ClientConfig`. Existing `Send` and `SendJSON` calls remain supported; use `SendContext` and `SendJSONContext` for per-call cancellation.

## Usage

### Server

<!-- [snippet:server] -->
```go title="example_test.go"
func ExampleHandler() {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))

	log.Println("Listening on :4437")
	log.Fatal(http.ListenAndServe(":4437", mux))
}

```
<!-- [/snippet:server] -->

### Client

<!-- [snippet:client] -->
```go title="example_test.go"
func ExampleClient() {
	ctx := context.Background()

	client := durablestream.NewClient("http://localhost:4437/v1/stream", nil)

	_, err := client.Create(ctx, "events", &durablestream.CreateOptions{
		ContentType: "application/json",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Write using Writer
	writer, err := client.Writer(ctx, "events")
	if err != nil {
		log.Fatal(err)
	}

	event := map[string]any{"type": "user.created", "id": 123}
	if err := writer.SendJSONContext(ctx, event, nil); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Appended at offset:", writer.Offset())

	// Read using Reader
	reader := client.Reader("events", durablestream.ZeroOffset)
	defer reader.Close()

	result, err := reader.Read(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Got data:", len(result.Data) > 0)
	fmt.Println("Next offset:", result.NextOffset)
}

```
<!-- [/snippet:client] -->

### Streaming Reader

<!-- [snippet:reader] -->
```go title="example_test.go"
func ExampleReader() {
	ctx := context.Background()

	client := durablestream.NewClient("http://localhost:4437/v1/stream", nil)

	// Create a reader starting from offset 0
	reader := client.Reader("events", durablestream.ZeroOffset)
	defer reader.Close()

	for msg, err := range reader.Messages(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		// Use msg.String() for text, msg.Bytes() for raw bytes,
		// or msg.Decode(&v) for JSON
		fmt.Println("Received:", msg.String())
	}
}

```
<!-- [/snippet:reader] -->

## Development

Run the complete Go validation suite with:

```sh
task check
```

The conformance runners install the exact dependencies in `conformance/package-lock.json` before running:

```sh
task conformance:client
task conformance:go
task conformance:go:badger
```

`task api` regenerates the ignored local `API.md`; published package documentation comes from Go doc comments through pkg.go.dev.

## License

MIT - see [LICENSE](LICENSE)
