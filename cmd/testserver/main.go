package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/memorystorage"
)

func main() {
	port := flag.Int("port", 4437, "port to listen on")
	flag.Parse()

	storage := memorystorage.New()
	handler := durablestream.NewHandler(storage, nil)

	mux := http.NewServeMux()
	mux.Handle("/", handler)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("Durable Streams test server listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
