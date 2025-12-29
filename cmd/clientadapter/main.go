package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

// Command represents a test command from the runner
type Command struct {
	Type        string      `json:"type"`
	ServerURL   string      `json:"serverUrl,omitempty"`
	Path        string      `json:"path,omitempty"`
	ContentType string      `json:"contentType,omitempty"`
	Data        string      `json:"data,omitempty"`
	Binary      bool        `json:"binary,omitempty"`
	Seq         interface{} `json:"seq,omitempty"` // can be string or int
	Offset      string      `json:"offset,omitempty"`
	Live        interface{} `json:"live,omitempty"` // can be string "long-poll", "sse" or bool false
	TimeoutMs   int         `json:"timeoutMs,omitempty"`
	TTL         int         `json:"ttl,omitempty"`
}

// getLiveMode returns the live mode as a string
func (c *Command) getLiveMode() string {
	switch v := c.Live.(type) {
	case string:
		return v
	case bool:
		return "" // false means no live mode
	default:
		return ""
	}
}

// getSeq returns the seq as a string
func (c *Command) getSeq() string {
	switch v := c.Seq.(type) {
	case string:
		return v
	case float64:
		return fmt.Sprintf("%.0f", v)
	case int:
		return fmt.Sprintf("%d", v)
	default:
		return ""
	}
}

// Result represents a test result sent back to the runner
type Result struct {
	Type          string    `json:"type"`
	Success       bool      `json:"success"`
	CommandType   string    `json:"commandType,omitempty"`
	Status        int       `json:"status,omitempty"`
	Offset        string    `json:"offset,omitempty"`
	Chunks        []Chunk   `json:"chunks"`
	UpToDate      bool      `json:"upToDate,omitempty"`
	ContentType   string    `json:"contentType,omitempty"`
	ErrorCode     string    `json:"errorCode,omitempty"`
	Message       string    `json:"message,omitempty"`
	ClientName    string    `json:"clientName,omitempty"`
	ClientVersion string    `json:"clientVersion,omitempty"`
	Features      *Features `json:"features,omitempty"`
}

type Chunk struct {
	Data   string `json:"data"`
	Offset string `json:"offset"`
}

type Features struct {
	Batching bool `json:"batching"`
	SSE      bool `json:"sse"`
	LongPoll bool `json:"longPoll"`
}

var globalServerURL string
var globalClient *durablestream.Client

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	// Increase buffer size for large test data
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var cmd Command
		if err := json.Unmarshal([]byte(line), &cmd); err != nil {
			sendError("parse", "PARSE_ERROR", err.Error())
			continue
		}

		// Handle init specially to set up globals
		if cmd.Type == "init" {
			globalServerURL = cmd.ServerURL
			globalClient = durablestream.NewClient(globalServerURL, nil)
		}

		result := handleCommand(cmd)

		output, _ := json.Marshal(result)
		fmt.Println(string(output))

		if cmd.Type == "shutdown" {
			break
		}
	}
}

func handleCommand(cmd Command) Result {
	ctx := context.Background()
	if cmd.TimeoutMs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(cmd.TimeoutMs)*time.Millisecond)
		defer cancel()
	}

	switch cmd.Type {
	case "init":
		return Result{
			Type:          "init",
			Success:       true,
			ClientName:    "durable-streams-go",
			ClientVersion: "0.1.0",
			Chunks:        []Chunk{},
			Features: &Features{
				Batching: false,
				SSE:      true,
				LongPoll: true,
			},
		}

	case "create":
		if globalClient == nil {
			return errorResult("create", "INTERNAL_ERROR", "client not initialized")
		}

		// Check if stream exists for idempotent create (returns 200 vs 201)
		_, existsErr := globalClient.Head(ctx, cmd.Path)
		streamExists := existsErr == nil

		opts := &durablestream.CreateOptions{}
		if cmd.ContentType != "" {
			opts.ContentType = cmd.ContentType
		}
		if cmd.TTL > 0 {
			opts.TTL = time.Duration(cmd.TTL) * time.Second
		}
		if cmd.Data != "" {
			opts.InitialData = []byte(cmd.Data)
		}

		info, err := globalClient.Create(ctx, cmd.Path, opts)
		if err != nil {
			return mapError("create", err)
		}

		// Return 200 for idempotent re-creation, 201 for new stream
		status := 201
		if streamExists {
			status = 200
		}
		return Result{
			Type:    "create",
			Success: true,
			Status:  status,
			Offset:  string(info.NextOffset),
			Chunks:  []Chunk{},
		}

	case "append":
		if globalClient == nil {
			return errorResult("append", "INTERNAL_ERROR", "client not initialized")
		}
		writer, err := globalClient.Writer(ctx, cmd.Path)
		if err != nil {
			return mapError("append", err)
		}

		var opts *durablestream.SendOptions
		seq := cmd.getSeq()
		if seq != "" {
			opts = &durablestream.SendOptions{Seq: seq}
		}

		// Decode base64 if binary flag is set
		var data []byte
		if cmd.Binary {
			data, err = base64.StdEncoding.DecodeString(cmd.Data)
			if err != nil {
				return errorResult("append", "BAD_REQUEST", "invalid base64 data: "+err.Error())
			}
		} else {
			data = []byte(cmd.Data)
		}

		if err := writer.Send(data, opts); err != nil {
			// Empty append returns a specific error with status 400
			if strings.Contains(err.Error(), "empty") {
				return Result{
					Type:        "error",
					Success:     false,
					CommandType: "append",
					Status:      400,
					ErrorCode:   "BAD_REQUEST",
					Message:     err.Error(),
					Chunks:      []Chunk{},
				}
			}
			return mapError("append", err)
		}
		return Result{
			Type:    "append",
			Success: true,
			Status:  200,
			Offset:  string(writer.Offset()),
			Chunks:  []Chunk{},
		}

	case "read":
		if globalClient == nil {
			return errorResult("read", "INTERNAL_ERROR", "client not initialized")
		}

		// Determine read mode
		var readMode durablestream.ReadMode
		liveMode := cmd.getLiveMode()
		switch liveMode {
		case "long-poll":
			readMode = durablestream.ReadModeLongPoll
		case "sse":
			readMode = durablestream.ReadModeSSE
		default:
			readMode = durablestream.ReadModeAuto
		}

		// Create client with specific read mode for this request
		readClient := durablestream.NewClient(globalServerURL, &durablestream.ClientConfig{
			ReadMode: readMode,
		})

		reader := readClient.Reader(cmd.Path, durablestream.Offset(cmd.Offset))
		defer reader.Close()

		result, err := reader.Read(ctx)
		if err != nil {
			return mapError("read", err)
		}

		chunks := []Chunk{}
		if len(result.Data) > 0 {
			chunks = append(chunks, Chunk{
				Data:   string(result.Data),
				Offset: string(result.NextOffset),
			})
		}

		return Result{
			Type:     "read",
			Success:  true,
			Status:   200,
			Chunks:   chunks,
			Offset:   string(result.NextOffset),
			UpToDate: result.UpToDate,
		}

	case "head":
		if globalClient == nil {
			return errorResult("head", "INTERNAL_ERROR", "client not initialized")
		}
		info, err := globalClient.Head(ctx, cmd.Path)
		if err != nil {
			return mapError("head", err)
		}
		return Result{
			Type:        "head",
			Success:     true,
			Status:      200,
			Offset:      string(info.NextOffset),
			ContentType: info.ContentType,
			Chunks:      []Chunk{},
		}

	case "delete":
		if globalClient == nil {
			return errorResult("delete", "INTERNAL_ERROR", "client not initialized")
		}
		if err := globalClient.Delete(ctx, cmd.Path); err != nil {
			return mapError("delete", err)
		}
		return Result{
			Type:    "delete",
			Success: true,
			Status:  200,
			Chunks:  []Chunk{},
		}

	case "connect":
		// Connect tests if a stream exists (like HEAD but for the client abstraction)
		if globalClient == nil {
			return errorResult("connect", "INTERNAL_ERROR", "client not initialized")
		}
		_, err := globalClient.Head(ctx, cmd.Path)
		if err != nil {
			return mapError("connect", err)
		}
		return Result{
			Type:    "connect",
			Success: true,
			Status:  200,
			Chunks:  []Chunk{},
		}

	case "shutdown":
		return Result{
			Type:    "shutdown",
			Success: true,
			Chunks:  []Chunk{},
		}

	default:
		return errorResult(cmd.Type, "NOT_SUPPORTED", "unknown command type: "+cmd.Type)
	}
}

func errorResult(cmdType, code, message string) Result {
	return Result{
		Type:        "error",
		Success:     false,
		CommandType: cmdType,
		ErrorCode:   code,
		Message:     message,
		Chunks:      []Chunk{},
	}
}

func mapError(cmdType string, err error) Result {
	code := "INTERNAL_ERROR"
	status := 500
	errMsg := err.Error()

	// Check if it's a transport error first (has more specific codes)
	var tErr *transport.Error
	if errors.As(err, &tErr) {
		status = tErr.StatusCode
		code = mapTransportCode(tErr.Code, tErr.StatusCode)
		errMsg = tErr.Message
	} else {
		// Check for sentinel errors
		switch {
		case errors.Is(err, durablestream.ErrNotFound):
			code = "NOT_FOUND"
			status = 404
		case errors.Is(err, durablestream.ErrConflict):
			code = "CONFLICT"
			status = 409
		case errors.Is(err, durablestream.ErrGone):
			code = "GONE"
			status = 410
		case errors.Is(err, durablestream.ErrBadRequest):
			code = "BAD_REQUEST"
			status = 400
		}
	}

	// Refine based on command type and status codes when we don't have specific info
	if status == 409 && cmdType == "append" {
		code = "SEQUENCE_CONFLICT" // 409 during append is sequence conflict
	}
	if status == 400 && cmdType == "read" {
		code = "INVALID_OFFSET" // 400 during read is usually invalid offset
	}

	return Result{
		Type:        "error",
		Success:     false,
		CommandType: cmdType,
		Status:      status,
		ErrorCode:   code,
		Message:     errMsg,
		Chunks:      []Chunk{},
	}
}

func mapTransportCode(code string, status int) string {
	// Map transport codes to conformance test codes
	switch strings.ToUpper(code) {
	case "NOT_FOUND":
		return "NOT_FOUND"
	case "CONFLICT":
		return "CONFLICT"
	case "GONE":
		return "GONE"
	case "BAD_REQUEST":
		return "BAD_REQUEST"
	case "RATE_LIMITED", "TOO_MANY_REQUESTS":
		return "RATE_LIMITED"
	default:
		// Fall back to HTTP status mapping
		switch status {
		case 404:
			return "NOT_FOUND"
		case 409:
			return "CONFLICT"
		case 410:
			return "GONE"
		case 400:
			return "BAD_REQUEST"
		case 429:
			return "RATE_LIMITED"
		default:
			return "INTERNAL_ERROR"
		}
	}
}

func sendError(cmdType, code, message string) {
	result := errorResult(cmdType, code, message)
	output, _ := json.Marshal(result)
	fmt.Println(string(output))
}
