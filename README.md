# durable-streams-go

[![Go Reference](https://pkg.go.dev/badge/github.com/ahimsalabs/durable-streams-go.svg)](https://pkg.go.dev/github.com/ahimsalabs/durable-streams-go/durablestream)

Go implementation of [Durable Streams](https://github.com/durable-streams/durable-streams).

Passes the durable-streams conformance suite.

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

	client := durablestream.NewClient("http://localhost:4437/streams", nil)

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
	if err := writer.SendJSON(event, nil); err != nil {
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

	client := durablestream.NewClient("http://localhost:4437/streams", nil)

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

## License

MIT - see [LICENSE](LICENSE)
