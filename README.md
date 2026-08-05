# durable-streams-go

[![Go Reference](https://pkg.go.dev/badge/github.com/ahimsalabs/durable-streams-go.svg)](https://pkg.go.dev/github.com/ahimsalabs/durable-streams-go/durablestream)

Go implementation of [Durable Streams](https://github.com/durable-streams/durable-streams).

## Conformance

| Suite | Version | Tests | Passed | Failed | Skipped |
|-------|---------|-------|--------|--------|---------|
| Server (memorystorage) | v0.3.6 | 338 | 332 | 0 | 6 |
| Server (badgerstore) | v0.3.6 | 338 | 332 | 0 | 6 |
| Client | v0.2.12 | 269 | 257 | 1 | 11 |

Client features: `batching`, `sse`, `longPoll`, `streaming`, `dynamicHeaders`, `auto`

Both server backends pass every enabled conformance case; the 6 skips cover subscriptions.

The sole client failure expects an automatic retry of a bare POST append. This client intentionally does not retry that unsafe, non-idempotent request because the first attempt may already have committed. The 11 skips comprise six retry-option cases, one strict-zero validation case, two batch-items cases, and two suite-disabled Swift SSE cases.

## Known limitations

- Subscriptions are not implemented yet, accounting for the 6 skipped server cases above.
- Idempotent-producer state and Handler lifecycle coordination are process-local. Use one Handler per Storage; producer deduplication state does not survive a process restart.
- Fork creation is currently exposed by the Handler's HTTP protocol surface and storage APIs, but not by the typed `Client`/`transport.CreateRequest` API. Typed-client callers that need to create forks must issue the fork protocol headers through an HTTP request for now.

## Offset Format

Offsets are opaque tokens. Clients must retain and echo server-issued values
without parsing, modifying, or constructing them, and must not assume that a
token encodes a byte position. Tokens are ordered only within one stream
incarnation. Forks are the exception to cross-stream reuse: they share their
source's offset space through the inherited range and continue in that ordered
space after the fork boundary.

## Test Coverage

These figures come from a representative `task test` run
(`go test -race -cover ./...`). Coverage in the storage backends can vary by a
few tenths of a percentage point because their concurrency tests exercise
different branches depending on scheduling.

| Package | Coverage |
|---------|----------|
| durablestream | 85.6% |
| durablestream/transport | 92.4% |
| durablestream/storage | 100.0% |
| durablestream/storage/memorystorage | 85.6% |
| durablestream/internal/protocol | 98.5% |
| durablestream/storage/badgerstore | 83.5% |

Badgerstore also has [7 fuzz tests](durablestream/storage/badgerstore/fuzz_test.go) covering stream ID validation, stream operations, sequence ordering, and concurrent operations. Run them with Go's built-in fuzzing support, for example `go test -fuzz=Fuzz -fuzztime=30s ./durablestream/storage/badgerstore`.

## Upgrade notes

This module is pre-v1 and unreleased (no tags): exported APIs and durable formats may change between revisions without a compatibility path, announced in this section. Interface additions like `Touch` stay in the core `Storage` interface when they implement normative protocol behavior, since a backend without them cannot serve a conforming server anyway.

- The `Storage` interface now includes `Touch`, which custom backends must implement to support sliding TTL renewal. New backends should run the reusable `durablestream/storage/storagetest` suite.
- `AtomicBatchStorage` is an optional storage capability used for all-or-nothing initial content and multi-message JSON appends. A Handler backed by a custom implementation without this capability returns `501 Not Implemented` for those requests rather than risk a partial commit; ordinary empty creates and single-message appends still work.
- `StreamConfig`, `StreamInfo`, and `ReadResult` now carry `Closed`. Custom backends must persist and report permanent EOF, reject later appends with `ErrStreamClosed` (distinct from storage shutdown's `ErrClosed`), and may implement `AtomicCloseStorage` for atomic final appends and closure. The Handler returns `501 Not Implemented` for POST closure when that capability is absent. Client append conflicts can be inspected as `StreamClosedError` to obtain the server's final offset.
- Custom `transport.Transport` implementations must honor `CreateRequest.Closed` and `AppendRequest.Close`, and populate the `Closed` field on create, append, read, event, and HEAD results. The client now rejects a successful create/append that requested closure but did not confirm it. These public struct field additions also require updates to external unkeyed literals; keyed literals are recommended.
- Fork-aware backends opt into `ForkStorage` and receive a `ForkRequest` describing the source, boundary, and target configuration. Direct storage access to a deleted source retained for descendants reports `ErrSoftDeleted`; the Handler maps it to HTTP 410 and the built-in Client reports `ErrGone`. The Handler returns `501 Not Implemented` for fork creation when the backend lacks this capability.
- `HandlerConfig.ForkPathExtractor` maps the same-server path in `Stream-Forked-From` to a storage ID when custom routing cannot be inferred. The default supports direct mounting and common `http.StripPrefix` layouts; source paths are validated before a custom extractor receives them.
- `StreamInfo` and `ReadResult` can expose an opaque incarnation ID. Custom backends should populate it to enable safe ETag validation and cross-incarnation read detection.
- Badger's durable format now records closure and fork lineage. It is not compatible with databases written by earlier revisions: opening a legacy directory returns an error matching `badgerstore.ErrLegacyFormat` **without modifying its data**. There is no in-place migration and none is planned pre-release — the old format was never in a tagged release, and the batch-boundary metadata a migration would need (for JSON fork sub-offsets) was never recorded. Discard and recreate the directory, or drain its streams through the protocol using the binary that wrote it, before reopening with this version.
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
