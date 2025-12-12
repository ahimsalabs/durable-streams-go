package durablestream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

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

	msg := json.RawMessage(`{"type":"user.created","id":123}`)
	offset, err := client.Append(ctx, "events", msg, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Appended at offset:", offset)

	// Read from start
	result, err := client.Read(ctx, "events", nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Messages:", len(result.Messages))
	fmt.Println("Next offset:", result.NextOffset)
}

// [/snippet:client]

// [snippet:reader]
func ExampleReader() {
	ctx := context.Background()

	client := durablestream.NewClient().BaseURL("http://localhost:8080/streams")

	// Create a reader starting from offset 0
	reader := client.Reader("events", "0")
	defer reader.Close()

	for msg, err := range reader.Messages(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Received:", string(msg))
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

	ctx := context.Background()
	client := durablestream.NewClient().BaseURL(server.URL)

	client.Create(ctx, "/mystream", &durablestream.CreateOptions{
		ContentType: "application/json",
	})

	// Append data and read it back.
	client.Append(ctx, "/mystream", []byte(`{"hello":"world"}`), nil)

	result, _ := client.Read(ctx, "/mystream", nil)
	fmt.Println(string(result.Messages[0]))
	// [/snippet:demo]

	// Output: {"hello":"world"}
}
