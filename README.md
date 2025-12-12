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
	handler := durablestream.NewHandler(storage)

	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

```
<!-- [/snippet:server] -->

### Client

<!-- [snippet:client] -->
```go title="example_test.go"
func ExampleClient() {
	ctx := context.Background()

	client := durablestream.NewClient().BaseURL("http://localhost:8080/streams")

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

	msg := json.RawMessage(`{"type":"user.created","id":123}`)
	if err := writer.Send(msg); err != nil {
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

	fmt.Println("Messages:", len(result.Messages))
	fmt.Println("Next offset:", result.NextOffset)
}

```
<!-- [/snippet:client] -->

### Streaming Reader

<!-- [snippet:reader] -->
```go title="example_test.go"
func ExampleReader() {
	ctx := context.Background()

	client := durablestream.NewClient().BaseURL("http://localhost:8080/streams")

	// Create a reader starting from offset 0
	reader := client.Reader("events", durablestream.ZeroOffset)
	defer reader.Close()

	for msg, err := range reader.Messages(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Received:", string(msg))
	}
}

```
<!-- [/snippet:reader] -->

## License

MIT - see [LICENSE](LICENSE)
