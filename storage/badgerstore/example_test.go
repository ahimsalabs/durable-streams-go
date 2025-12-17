package badgerstore_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/storage/badgerstore"
)

// [snippet:quickstart]
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

// [/snippet:quickstart]

// [snippet:options]
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

// [/snippet:options]

// [snippet:http]
func Example_httpServer() {
	storage, _ := badgerstore.New(badgerstore.Options{Dir: "./data"})
	defer storage.Close()

	handler := durablestream.NewHandler(storage, nil)

	http.Handle("/streams/", http.StripPrefix("/streams", handler))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// [/snippet:http]

// [snippet:dedup]
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

// [/snippet:dedup]

// [snippet:expiry]
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

// [/snippet:expiry]

// [snippet:subscribe]
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

// [/snippet:subscribe]

// [snippet:testing]
func Example_testing() {
	// Use in-memory mode for fast, isolated tests
	storage, err := badgerstore.New(badgerstore.Options{InMemory: true})
	if err != nil {
		log.Fatal(err)
	}
	defer storage.Close()

	// ... test code using storage
}

// [/snippet:testing]

func Example_fullDemo() {
	// [snippet:demo]
	storage, _ := badgerstore.New(badgerstore.Options{InMemory: true})
	defer storage.Close()

	handler := durablestream.NewHandler(storage, nil)
	server := httptest.NewServer(handler)
	defer server.Close()

	ctx := context.Background()
	client := durablestream.NewClient(server.URL, nil)

	// Create stream
	client.Create(ctx, "/mystream", &durablestream.CreateOptions{
		ContentType: "application/json",
	})

	// Write data
	writer, _ := client.Writer(ctx, "/mystream")
	writer.SendJSON(map[string]string{"hello": "world"}, nil)

	// Read back
	reader := client.Reader("/mystream", durablestream.ZeroOffset)
	defer reader.Close()

	for msg, err := range reader.Messages(ctx) {
		if err != nil {
			break
		}
		fmt.Println(msg.String())
		break
	}
	// [/snippet:demo]

	// Output: {"hello":"world"}
}
