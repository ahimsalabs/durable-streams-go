package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// Command represents a test command from the runner
type Command struct {
	Type        string `json:"type"`
	ServerURL   string `json:"serverUrl,omitempty"`
	Path        string `json:"path,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Data        string `json:"data,omitempty"`
	Seq         string `json:"seq,omitempty"`
	Offset      string `json:"offset,omitempty"`
	Live        string `json:"live,omitempty"`
	TimeoutMs   int    `json:"timeoutMs,omitempty"`
	TTL         int    `json:"ttl,omitempty"`
}

// Result represents a test result sent back to the runner
type Result struct {
	Type          string   `json:"type"`
	Success       bool     `json:"success"`
	CommandType   string   `json:"commandType,omitempty"`
	Status        int      `json:"status,omitempty"`
	Offset        string   `json:"offset,omitempty"`
	Chunks        []Chunk  `json:"chunks,omitempty"`
	UpToDate      bool     `json:"upToDate,omitempty"`
	ContentType   string   `json:"contentType,omitempty"`
	ErrorCode     string   `json:"errorCode,omitempty"`
	Message       string   `json:"message,omitempty"`
	ClientName    string   `json:"clientName,omitempty"`
	ClientVersion string   `json:"clientVersion,omitempty"`
	Features      Features `json:"features,omitempty"`
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

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	// Increase buffer size for large test data
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	var client *durablestream.Client
	var serverURL string

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

		result := handleCommand(cmd, client, serverURL)

		if cmd.Type == "init" && result.Success {
			serverURL = cmd.ServerURL
			client = durablestream.NewClient(serverURL, nil)
		}

		output, _ := json.Marshal(result)
		fmt.Println(string(output))

		if cmd.Type == "shutdown" {
			break
		}
	}
}

func handleCommand(cmd Command, client *durablestream.Client, serverURL string) Result {
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
			Features: Features{
				Batching: false,
				SSE:      true,
				LongPoll: true,
			},
		}

	case "create":
		if client == nil {
			return errorResult("create", "INTERNAL_ERROR", "client not initialized")
		}
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

		info, err := client.Create(ctx, cmd.Path, opts)
		if err != nil {
			return mapError("create", err)
		}
		return Result{
			Type:    "create",
			Success: true,
			Status:  201,
			Offset:  string(info.NextOffset),
		}

	case "append":
		if client == nil {
			return errorResult("append", "INTERNAL_ERROR", "client not initialized")
		}
		writer, err := client.Writer(ctx, cmd.Path)
		if err != nil {
			return mapError("append", err)
		}

		var opts *durablestream.SendOptions
		if cmd.Seq != "" {
			opts = &durablestream.SendOptions{Seq: cmd.Seq}
		}

		if err := writer.Send([]byte(cmd.Data), opts); err != nil {
			return mapError("append", err)
		}
		return Result{
			Type:    "append",
			Success: true,
			Status:  200,
			Offset:  string(writer.Offset()),
		}

	case "read":
		if client == nil {
			return errorResult("read", "INTERNAL_ERROR", "client not initialized")
		}
		reader := client.Reader(cmd.Path, durablestream.Offset(cmd.Offset))
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
		if client == nil {
			return errorResult("head", "INTERNAL_ERROR", "client not initialized")
		}
		info, err := client.Head(ctx, cmd.Path)
		if err != nil {
			return mapError("head", err)
		}
		return Result{
			Type:        "head",
			Success:     true,
			Status:      200,
			Offset:      string(info.NextOffset),
			ContentType: info.ContentType,
		}

	case "delete":
		if client == nil {
			return errorResult("delete", "INTERNAL_ERROR", "client not initialized")
		}
		if err := client.Delete(ctx, cmd.Path); err != nil {
			return mapError("delete", err)
		}
		return Result{
			Type:    "delete",
			Success: true,
			Status:  200,
		}

	case "shutdown":
		return Result{
			Type:    "shutdown",
			Success: true,
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
	}
}

func mapError(cmdType string, err error) Result {
	code := "INTERNAL_ERROR"
	status := 500

	switch err {
	case durablestream.ErrNotFound:
		code = "NOT_FOUND"
		status = 404
	case durablestream.ErrConflict:
		code = "CONFLICT"
		status = 409
	case durablestream.ErrGone:
		code = "GONE"
		status = 410
	case durablestream.ErrBadRequest:
		code = "BAD_REQUEST"
		status = 400
	}

	return Result{
		Type:        "error",
		Success:     false,
		CommandType: cmdType,
		Status:      status,
		ErrorCode:   code,
		Message:     err.Error(),
	}
}

func sendError(cmdType, code, message string) {
	result := errorResult(cmdType, code, message)
	output, _ := json.Marshal(result)
	fmt.Println(string(output))
}
