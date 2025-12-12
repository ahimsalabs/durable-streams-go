package durablestream_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/memorystorage"
)

// [snippet:server]
func ExampleHandler() {
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage)

	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", handler))

	log.Println("Listening on :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

// [/snippet:server]

// [snippet:client]
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

	msg := []byte(`{"type":"user.created","id":123}`)
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

	fmt.Println("Got data:", len(result.Data) > 0)
	fmt.Println("Next offset:", result.NextOffset)
}

// [/snippet:client]

// [snippet:reader]
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
		// Use msg.String() for text, msg.Bytes() for raw bytes,
		// or msg.Decode(&v) for JSON
		fmt.Println("Received:", msg.String())
	}
}

// [/snippet:reader]

func Example_fullDemo() {
	// [snippet:demo]
	// Start test server
	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage)
	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	client := durablestream.NewClient().BaseURL(server.URL)

	_, _ = client.Create(ctx, "/mystream", &durablestream.CreateOptions{
		ContentType: "application/json",
	})

	// Write using Writer
	writer, _ := client.Writer(ctx, "/mystream")
	_ = writer.Send([]byte(`{"hello":"world"}`))

	// Read using Reader with Messages iterator
	reader := client.Reader("/mystream", durablestream.ZeroOffset)
	defer reader.Close()

	for msg, err := range reader.Messages(ctx) {
		if err != nil {
			break
		}
		fmt.Println(msg.String())
		break // Just read first message for demo
	}
	// [/snippet:demo]

	// Output: {"hello":"world"}
}
