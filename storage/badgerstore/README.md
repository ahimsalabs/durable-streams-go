# badgerstore

Persistent storage backend for durable streams using [Badger](https://github.com/dgraph-io/badger), an embedded key-value store optimized for SSDs and append-heavy workloads.

## When to Use

- **Dev/staging servers** that need persistence across restarts
- **Single-node deployments** where external dependencies (Redis, Postgres) aren't justified
- **Append-heavy workloads** - Badger's LSM-tree architecture excels here

For production multi-node deployments, consider a distributed storage backend instead.

## Installation

<!-- snippet-ignore -->
```bash
go get github.com/ahimsalabs/durable-streams-go/storage/badgerstore
```

## Quick Start

<!-- [snippet:storage/badgerstore:quickstart] -->
```go title="example_test.go"
func Example() {
	// Create persistent storage
	storage, err := badgerstore.New(badgerstore.Options{
		Dir: "./data/streams",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Create a stream
	_, err = storage.Create(ctx, "events", durablestream.StreamConfig{
		ContentType: "application/json",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Append data
	offset, err := storage.Append(ctx, "events", []byte(`{"type":"user.created"}`), "")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Appended at offset: %s", offset)

	// Read from start
	result, err := storage.Read(ctx, "events", "", 0)
	if err != nil {
		log.Fatal(err)
	}
	for _, msg := range result.Messages {
		log.Printf("Offset %s: %s", msg.Offset, msg.Data)
	}
}

```
<!-- [/snippet:storage/badgerstore:quickstart] -->

## Configuration

<!-- [snippet:storage/badgerstore:options] -->
```go title="example_test.go"
func Example_options() {
	storage, err := badgerstore.New(badgerstore.Options{
		// Required for persistence (omit for in-memory)
		Dir: "./data/streams",

		// In-memory mode for tests (no persistence)
		InMemory: false,

		// Maximum message size (default: 10MB)
		MaxMessageSize: 10 * 1024 * 1024,

		// Background value log GC interval (default: 5min, 0 to disable)
		GCInterval: 5 * time.Minute,

		// Expired stream cleanup interval (default: 1min, 0 to disable)
		CleanupInterval: 1 * time.Minute,

		// Custom Badger logger (nil uses default)
		Logger: nil,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()
}

```
<!-- [/snippet:storage/badgerstore:options] -->

## Usage with HTTP Server

<!-- [snippet:storage/badgerstore:http] -->
```go title="example_test.go"
func Example_httpServer() {
	storage, _ := badgerstore.New(badgerstore.Options{Dir: "./data"})
	defer storage.Close()

	handler := durablestream.NewHandler(storage, nil)

	http.Handle("/streams/", http.StripPrefix("/streams", handler))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

```
<!-- [/snippet:storage/badgerstore:http] -->

## Deduplication with Stream-Seq

The `seq` parameter enables exactly-once append semantics:

<!-- [snippet:storage/badgerstore:dedup] -->
```go title="example_test.go"
func Example_deduplication() {
	storage, _ := badgerstore.New(badgerstore.Options{InMemory: true})
	defer storage.Close()

	ctx := context.Background()
	storage.Create(ctx, "stream", durablestream.StreamConfig{ContentType: "text/plain"})

	// First append succeeds
	_, err := storage.Append(ctx, "stream", []byte("data"), "0001")
	fmt.Println("First append:", err)

	// Duplicate with same seq returns ErrConflict
	_, err = storage.Append(ctx, "stream", []byte("data"), "0001")
	fmt.Println("Duplicate append:", err != nil)

	// Higher seq succeeds
	_, err = storage.Append(ctx, "stream", []byte("data"), "0002")
	fmt.Println("Next append:", err)

	// Output:
	// First append: <nil>
	// Duplicate append: true
	// Next append: <nil>
}

```
<!-- [/snippet:storage/badgerstore:dedup] -->

**Important**: Sequence values use lexicographic (string) ordering:
- Use zero-padded numbers: `"0001"`, `"0002"`, `"0010"`
- Or ULIDs/UUIDs which sort naturally
- **Not** `"1"`, `"2"`, `"10"` (lexicographically `"10" < "2"`)

## Stream Expiration

<!-- [snippet:storage/badgerstore:expiry] -->
```go title="example_test.go"
func Example_expiry() {
	storage, _ := badgerstore.New(badgerstore.Options{InMemory: true})
	defer storage.Close()

	ctx := context.Background()

	// Expire after duration (TTL in seconds)
	storage.Create(ctx, "temp-ttl", durablestream.StreamConfig{
		ContentType: "text/plain",
		TTL:         3600, // 1 hour
	})

	// Expire at specific time
	storage.Create(ctx, "temp-expires", durablestream.StreamConfig{
		ContentType: "text/plain",
		ExpiresAt:   time.Now().Add(24 * time.Hour),
	})
}

```
<!-- [/snippet:storage/badgerstore:expiry] -->

Expired streams return `ErrNotFound` on access and are cleaned up by a background goroutine.

## Subscribing to Updates

<!-- [snippet:storage/badgerstore:subscribe] -->
```go title="example_test.go"
func Example_subscribe() {
	storage, _ := badgerstore.New(badgerstore.Options{InMemory: true})
	defer storage.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage.Create(ctx, "events", durablestream.StreamConfig{ContentType: "text/plain"})

	ch, err := storage.Subscribe(ctx, "events", "")
	if err != nil {
		log.Fatal(err)
	}

	// In a real app, this would be in a goroutine
	go func() {
		for offset := range ch {
			// New data available - fetch it
			result, _ := storage.Read(ctx, "events", offset, 0)
			for _, msg := range result.Messages {
				fmt.Println("Got:", string(msg.Data))
			}
		}
	}()

	// Append triggers notification
	storage.Append(ctx, "events", []byte("hello"), "")
	time.Sleep(10 * time.Millisecond) // Let goroutine process
}

```
<!-- [/snippet:storage/badgerstore:subscribe] -->

The channel receives notifications (not data) when new messages arrive. Cancel the context to unsubscribe.

## Limitations

| Limitation | Details |
|------------|---------|
| **Single process** | Badger uses file locking. Don't run multiple instances on the same directory. |
| **Offset gaps** | If an append fails after allocating an offset, that offset is skipped. Reads handle this correctly. |
| **Subscribers are ephemeral** | Subscriptions don't survive restarts. Clients must track their offset and reconnect. |
| **No retention policies** | Old data isn't automatically pruned (except via TTL expiration). |

## Performance Notes

- **Writes**: O(1) amortized - Badger sequences pre-allocate offset numbers in batches
- **Reads**: O(messages returned) - sequential scan from offset
- **Head/Metadata**: O(1) - reverse iteration finds tail offset directly
- **Delete**: O(messages) - batched in transactions of 10,000 keys

For long-running processes, Badger's value log grows. The background GC reclaims space, or call `storage.RunGC()` manually.

## Testing

Use in-memory mode for fast, isolated tests:

<!-- [snippet:storage/badgerstore:testing] -->
```go title="example_test.go"
func Example_testing() {
	// Use in-memory mode for fast, isolated tests
	storage, err := badgerstore.New(badgerstore.Options{InMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()

	// ... test code using storage
}

```
<!-- [/snippet:storage/badgerstore:testing] -->

## See Also

- [Durable Streams Protocol](../../PROTOCOL.md) - Full protocol specification
- [durablestream package](../../durablestream/) - Core types and HTTP handler
