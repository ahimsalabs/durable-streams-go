// Package main implements a conformance test adapter for the local durablestream client.
//
// This adapter communicates with the test runner via stdin/stdout using
// a JSON-line protocol. It bridges the conformance test protocol to the
// durablestream package in this repository.
//
// Run with:
//
//	go run ./conformance/adapter
//
// Or build and run:
//
//	go build -o adapter ./conformance/adapter
//	./adapter
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/transport"
)

const clientVersion = "0.1.0"

// Command types from the test runner.
type Command struct {
	Type      string `json:"type"`
	ServerURL string `json:"serverUrl,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
	Path      string `json:"path,omitempty"`
	// Create fields
	ContentType string `json:"contentType,omitempty"`
	TTLSeconds  int    `json:"ttlSeconds,omitempty"`
	ExpiresAt   string `json:"expiresAt,omitempty"`
	// Append fields
	Data   string `json:"data,omitempty"`
	Binary bool   `json:"binary,omitempty"`
	Seq    int    `json:"seq,omitempty"`
	// IdempotentProducer fields
	ProducerID  string   `json:"producerId,omitempty"`
	Epoch       int      `json:"epoch,omitempty"`
	AutoClaim   bool     `json:"autoClaim,omitempty"`
	MaxInFlight int      `json:"maxInFlight,omitempty"`
	Items       []string `json:"items,omitempty"` // Test runner extracts data field as strings
	// Read fields
	Offset          string `json:"offset,omitempty"`
	Live            any    `json:"live,omitempty"` // false | "long-poll" | "sse"
	MaxChunks       int    `json:"maxChunks,omitempty"`
	WaitForUpToDate bool   `json:"waitForUpToDate,omitempty"`
	// Headers
	Headers map[string]string `json:"headers,omitempty"`
	// Dynamic header/param fields
	Name         string `json:"name,omitempty"`
	ValueType    string `json:"valueType,omitempty"`    // "counter" | "timestamp" | "token"
	InitialValue string `json:"initialValue,omitempty"` // For token type
	// Validation fields
	Target *ValidationTarget `json:"target,omitempty"`
}

// ValidationTarget specifies what to validate and with what parameters.
type ValidationTarget struct {
	Target        string `json:"target"` // "idempotent-producer", "retry-options", etc.
	ProducerID    string `json:"producerId,omitempty"`
	Epoch         *int   `json:"epoch,omitempty"`         // Pointer to distinguish 0 from absent
	MaxBatchBytes *int   `json:"maxBatchBytes,omitempty"` // Pointer to distinguish 0 from absent
}

// Result types sent back to test runner.
type Result struct {
	Type          string      `json:"type"`
	Success       bool        `json:"success"`
	ClientName    string      `json:"clientName,omitempty"`
	ClientVersion string      `json:"clientVersion,omitempty"`
	Features      *Features   `json:"features,omitempty"`
	Status        int         `json:"status,omitempty"`
	Offset        string      `json:"offset,omitempty"`
	ContentType   string      `json:"contentType,omitempty"`
	Chunks        []ReadChunk `json:"chunks"`
	UpToDate      bool        `json:"upToDate"`
	Cursor        string      `json:"cursor,omitempty"`
	CommandType   string      `json:"commandType,omitempty"`
	ErrorCode     string      `json:"errorCode,omitempty"`
	Message       string      `json:"message,omitempty"`
	// IdempotentProducer fields
	Duplicate bool `json:"duplicate,omitempty"`
	// Dynamic header/param values (for get-dynamic-values)
	HeaderValues map[string]string `json:"headerValues,omitempty"`
	ParamValues  map[string]string `json:"paramValues,omitempty"`
	// Headers/params actually sent in request (for dynamic header testing)
	HeadersSent map[string]string `json:"headersSent,omitempty"`
	ParamsSent  map[string]string `json:"paramsSent,omitempty"`
}

// MarshalJSON ensures Chunks is [] not null for read results.
func (r Result) MarshalJSON() ([]byte, error) {
	type Alias Result
	alias := Alias(r)
	if alias.Type == "read" && alias.Chunks == nil {
		alias.Chunks = []ReadChunk{}
	}
	return json.Marshal(alias)
}

// Features reports client capabilities to the test runner.
type Features struct {
	Batching       bool `json:"batching"`
	SSE            bool `json:"sse"`
	LongPoll       bool `json:"longPoll"`
	Streaming      bool `json:"streaming"`
	DynamicHeaders bool `json:"dynamicHeaders"`
}

// ReadChunk represents a single chunk of data read from the stream.
type ReadChunk struct {
	Data   string `json:"data"`
	Binary bool   `json:"binary,omitempty"`
	Offset string `json:"offset,omitempty"`
}

var (
	serverURL string
	client    *durablestream.Client
	// Cache content types per stream path for append operations
	streamContentTypes = make(map[string]string)
)

// dynamicValue holds state for a dynamic header or param.
type dynamicValue struct {
	valueType  string // "counter" | "timestamp" | "token"
	counter    int
	tokenValue string
}

// resolve returns the current value and increments counter if applicable.
func (d *dynamicValue) resolve() string {
	switch d.valueType {
	case "counter":
		d.counter++
		return strconv.Itoa(d.counter)
	case "timestamp":
		return strconv.FormatInt(time.Now().UnixMilli(), 10)
	case "token":
		return d.tokenValue
	default:
		return ""
	}
}

// peek returns the current value without incrementing.
func (d *dynamicValue) peek() string {
	switch d.valueType {
	case "counter":
		return strconv.Itoa(d.counter + 1) // What the next resolve() would return
	case "timestamp":
		return strconv.FormatInt(time.Now().UnixMilli(), 10)
	case "token":
		return d.tokenValue
	default:
		return ""
	}
}

var (
	dynamicHeaders = make(map[string]*dynamicValue)
	dynamicParams  = make(map[string]*dynamicValue)
	// Track what was sent in the last request
	lastSentHeaders = make(map[string]string)
	lastSentParams  = make(map[string]string)
)

// Context key for resolved dynamic headers
type ctxKey string

const resolvedHeadersKey ctxKey = "resolvedHeaders"

// withResolvedHeaders resolves dynamic headers/params once and stores them in context.
// Returns the new context. Also updates lastSentHeaders/lastSentParams for reporting.
func withResolvedHeaders(ctx context.Context) context.Context {
	resolved := make(map[string]string)
	for name, dv := range dynamicHeaders {
		resolved[name] = dv.resolve()
	}
	lastSentHeaders = resolved

	resolvedParams := make(map[string]string)
	for name, dv := range dynamicParams {
		resolvedParams[name] = dv.resolve()
	}
	lastSentParams = resolvedParams

	return context.WithValue(ctx, resolvedHeadersKey, resolved)
}

// dynamicHeaderProvider reads resolved headers from context.
func dynamicHeaderProvider(ctx context.Context) (http.Header, error) {
	h := make(http.Header)
	if resolved, ok := ctx.Value(resolvedHeadersKey).(map[string]string); ok {
		for name, val := range resolved {
			h.Set(name, val)
		}
	}
	return h, nil
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	// Increase buffer size for large messages
	scanner.Buffer(make([]byte, 1024*1024), 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var cmd Command
		if err := json.Unmarshal([]byte(line), &cmd); err != nil {
			errResult := sendError("unknown", "PARSE_ERROR", fmt.Sprintf("failed to parse command: %v", err))
			output, _ := json.Marshal(errResult)
			fmt.Println(string(output))
			continue
		}

		result := handleCommand(cmd)
		output, _ := json.Marshal(result)
		fmt.Println(string(output))

		if cmd.Type == "shutdown" {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanner error: %v\n", err)
		os.Exit(1)
	}
}

func handleCommand(cmd Command) Result {
	switch cmd.Type {
	case "init":
		return handleInit(cmd)
	case "create":
		return handleCreate(cmd)
	case "connect":
		return handleConnect(cmd)
	case "append":
		return handleAppend(cmd)
	case "idempotent-append":
		return handleIdempotentAppend(cmd)
	case "idempotent-append-batch":
		return handleIdempotentAppendBatch(cmd)
	case "read":
		return handleRead(cmd)
	case "head":
		return handleHead(cmd)
	case "delete":
		return handleDelete(cmd)
	case "set-dynamic-header":
		return handleSetDynamicHeader(cmd)
	case "set-dynamic-param":
		return handleSetDynamicParam(cmd)
	case "clear-dynamic":
		return handleClearDynamic(cmd)
	case "get-dynamic-values":
		return handleGetDynamicValues(cmd)
	case "validate":
		return handleValidate(cmd)
	case "shutdown":
		return Result{Type: "shutdown", Success: true}
	default:
		return sendError(cmd.Type, "NOT_SUPPORTED", fmt.Sprintf("command not supported: %s", cmd.Type))
	}
}

func handleInit(cmd Command) Result {
	serverURL = cmd.ServerURL
	streamContentTypes = make(map[string]string)
	dynamicHeaders = make(map[string]*dynamicValue)
	dynamicParams = make(map[string]*dynamicValue)
	lastSentHeaders = make(map[string]string)
	lastSentParams = make(map[string]string)

	client = durablestream.NewClient(serverURL, &durablestream.ClientConfig{
		Timeout:  30 * time.Second,
		ReadMode: durablestream.ReadModeAuto,
		Headers:  dynamicHeaderProvider,
	})

	return Result{
		Type:          "init",
		Success:       true,
		ClientName:    "durable-streams-go-local",
		ClientVersion: clientVersion,
		Features: &Features{
			Batching:       true, // Idempotent producer support via transport layer
			SSE:            true,
			LongPoll:       true,
			Streaming:      true,
			DynamicHeaders: true,
		},
	}
}

func handleCreate(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	contentType := cmd.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Check if stream already exists for idempotent create detection
	alreadyExists := false
	if _, err := client.Head(ctx, cmd.Path); err == nil {
		alreadyExists = true
	}

	opts := &durablestream.CreateOptions{
		ContentType: contentType,
	}

	if cmd.TTLSeconds > 0 {
		opts.TTL = time.Duration(cmd.TTLSeconds) * time.Second
	}
	if cmd.ExpiresAt != "" {
		if t, err := time.Parse(time.RFC3339, cmd.ExpiresAt); err == nil {
			opts.ExpiresAt = t
		}
	}

	info, err := client.Create(ctx, cmd.Path, opts)
	if err != nil {
		return errorResult("create", err)
	}

	// Cache content type for append operations
	streamContentTypes[cmd.Path] = contentType

	// Return 200 if stream already existed (idempotent), 201 if newly created
	status := 201
	if alreadyExists {
		status = 200
	}

	return Result{
		Type:    "create",
		Success: true,
		Status:  status,
		Offset:  info.NextOffset.String(),
	}
}

func handleConnect(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := client.Head(ctx, cmd.Path)
	if err != nil {
		return errorResult("connect", err)
	}

	// Cache content type
	if info.ContentType != "" {
		streamContentTypes[cmd.Path] = info.ContentType
	}

	return Result{
		Type:    "connect",
		Success: true,
		Status:  200,
		Offset:  info.NextOffset.String(),
	}
}

func handleAppend(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Resolve dynamic headers once for this operation via context
	ctx = withResolvedHeaders(ctx)

	// Get data
	var data []byte
	if cmd.Binary {
		var err error
		data, err = base64.StdEncoding.DecodeString(cmd.Data)
		if err != nil {
			return sendError("append", "PARSE_ERROR", fmt.Sprintf("failed to decode base64: %v", err))
		}
	} else {
		data = []byte(cmd.Data)
	}

	// Create a writer for this path
	writer, err := client.Writer(ctx, cmd.Path)
	if err != nil {
		return errorResult("append", err)
	}

	// Build send options
	var opts *durablestream.SendOptions
	if cmd.Seq > 0 {
		opts = &durablestream.SendOptions{
			Seq: strconv.Itoa(cmd.Seq),
		}
	}

	if err := writer.Send(data, opts); err != nil {
		return errorResult("append", err)
	}

	hdrsSent, paramsSent := getSentDynamic()
	return Result{
		Type:        "append",
		Success:     true,
		Status:      200,
		Offset:      writer.Offset().String(),
		HeadersSent: hdrsSent,
		ParamsSent:  paramsSent,
	}
}

// idempotentAppendTransport creates a transport that bypasses the durablestream.Client
// to directly support idempotent producer headers.
func idempotentAppendTransport(ctx context.Context, path string, data []byte, contentType string, producerID string, epoch, seq int, autoClaim bool) (*transport.AppendResponse, error) {
	// Get transport directly - we need to bypass Client.Writer since it doesn't support producer headers
	httpTransport := transport.NewHTTPTransport(serverURL, nil)
	retryTransport := transport.WithRetry(transport.DefaultRetryOptions())(httpTransport)

	req := transport.AppendRequest{
		Path:               path,
		Data:               data,
		ContentType:        contentType,
		ProducerID:         producerID,
		ProducerEpoch:      epoch,
		ProducerSeq:        seq,
		HasProducerHeaders: true,
	}

	resp, err := retryTransport.Append(ctx, req)

	// Handle autoClaim: if we get 403 (stale epoch), bump epoch and retry
	if autoClaim && err != nil {
		if tErr, ok := err.(*transport.Error); ok && tErr.StatusCode == 403 {
			// Get the current epoch from error response headers
			// Server returns Producer-Epoch header with current epoch on 403
			newEpoch := tErr.ProducerEpoch + 1
			if newEpoch <= epoch {
				// Fallback if epoch not in error (shouldn't happen per protocol)
				newEpoch = epoch + 1
			}
			// Retry with new epoch and seq=0
			req.ProducerEpoch = newEpoch
			req.ProducerSeq = 0
			return retryTransport.Append(ctx, req)
		}
	}

	return resp, err
}

func handleIdempotentAppend(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get data
	var data []byte
	if cmd.Binary {
		var err error
		data, err = base64.StdEncoding.DecodeString(cmd.Data)
		if err != nil {
			return sendError("idempotent-append", "PARSE_ERROR", fmt.Sprintf("failed to decode base64: %v", err))
		}
	} else {
		data = []byte(cmd.Data)
	}

	// Get content type for this stream
	contentType := streamContentTypes[cmd.Path]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// For JSON streams, wrap the data in an array (server flattens one level)
	isJSON := normalizeContentType(contentType) == "application/json"
	if isJSON {
		// Wrap single item in array for JSON mode
		data = []byte("[" + string(data) + "]")
	}

	resp, err := idempotentAppendTransport(ctx, cmd.Path, data, contentType, cmd.ProducerID, cmd.Epoch, 0, cmd.AutoClaim)
	if err != nil {
		return errorResult("idempotent-append", err)
	}

	status := resp.StatusCode
	if status == 0 {
		status = 200
	}

	return Result{
		Type:      "idempotent-append",
		Success:   true,
		Status:    status,
		Offset:    resp.NextOffset,
		Duplicate: resp.Duplicate,
	}
}

func handleIdempotentAppendBatch(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get content type for this stream
	contentType := streamContentTypes[cmd.Path]
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	isJSON := normalizeContentType(contentType) == "application/json"

	// Collect all items data
	// Items are already strings (test runner extracts data field)
	var allData []byte
	if isJSON {
		// For JSON mode, build a JSON array of all items
		var jsonItems []json.RawMessage
		for _, item := range cmd.Items {
			jsonItems = append(jsonItems, json.RawMessage(item))
		}
		var err error
		allData, err = json.Marshal(jsonItems)
		if err != nil {
			return sendError("idempotent-append-batch", "PARSE_ERROR", fmt.Sprintf("failed to marshal JSON batch: %v", err))
		}
	} else {
		// For byte mode, concatenate all items
		var buf bytes.Buffer
		for _, item := range cmd.Items {
			buf.WriteString(item)
		}
		allData = buf.Bytes()
	}

	resp, err := idempotentAppendTransport(ctx, cmd.Path, allData, contentType, cmd.ProducerID, cmd.Epoch, 0, cmd.AutoClaim)
	if err != nil {
		return errorResult("idempotent-append-batch", err)
	}

	status := resp.StatusCode
	if status == 0 {
		status = 200
	}

	return Result{
		Type:      "idempotent-append-batch",
		Success:   true,
		Status:    status,
		Offset:    resp.NextOffset,
		Duplicate: resp.Duplicate,
	}
}

func handleRead(cmd Command) Result {
	timeoutMs := cmd.TimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 5000
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	// Determine starting offset
	var offset durablestream.Offset
	if cmd.Offset != "" {
		offset = durablestream.Offset(cmd.Offset)
	}

	// Determine live mode from command
	var readMode durablestream.ReadMode
	switch v := cmd.Live.(type) {
	case string:
		switch v {
		case "long-poll":
			readMode = durablestream.ReadModeLongPoll
		case "sse":
			readMode = durablestream.ReadModeSSE
		}
	case bool:
		if !v {
			readMode = durablestream.ReadModeAuto // No live mode, just catch-up
		}
	}

	// Resolve dynamic headers once for this operation via context
	ctx = withResolvedHeaders(ctx)

	// Create a client with the specified read mode for this request
	readClient := durablestream.NewClient(serverURL, &durablestream.ClientConfig{
		Timeout:  time.Duration(timeoutMs) * time.Millisecond,
		ReadMode: readMode,
		Headers:  dynamicHeaderProvider,
	})

	reader := readClient.Reader(cmd.Path, offset)
	defer reader.Close()

	chunks := make([]ReadChunk, 0)
	maxChunks := cmd.MaxChunks
	if maxChunks == 0 {
		maxChunks = 100
	}

	var finalOffset string
	upToDate := false
	status := 200

	for len(chunks) < maxChunks {
		result, err := reader.Read(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				// Timeout - we've caught up (no new data within timeout)
				upToDate = true
				finalOffset = reader.Offset().String()
				status = 204
				break
			}
			// Include reader's offset so test runner knows current position
			errRes := errorResult("read", err)
			errRes.Offset = reader.Offset().String()
			return errRes
		}

		if len(result.Data) > 0 {
			// Return raw data without parsing - conformance tests expect exact server response
			chunks = append(chunks, ReadChunk{
				Data:   string(result.Data),
				Offset: result.NextOffset.String(),
			})
		}

		finalOffset = result.NextOffset.String()
		upToDate = result.UpToDate

		// For waitForUpToDate, stop when we've reached up-to-date
		if cmd.WaitForUpToDate && result.UpToDate {
			break
		}

		// In non-live mode, if we got upToDate, we're done
		if cmd.Live == false && result.UpToDate {
			break
		}
	}

	// If no offset was set, use the initial one
	if finalOffset == "" {
		if cmd.Offset != "" {
			finalOffset = cmd.Offset
		} else {
			finalOffset = "0"
		}
	}

	hdrsSent, paramsSent := getSentDynamic()
	return Result{
		Type:        "read",
		Success:     true,
		Status:      status,
		Chunks:      chunks,
		Offset:      finalOffset,
		UpToDate:    upToDate,
		HeadersSent: hdrsSent,
		ParamsSent:  paramsSent,
	}
}

func handleHead(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := client.Head(ctx, cmd.Path)
	if err != nil {
		return errorResult("head", err)
	}

	return Result{
		Type:        "head",
		Success:     true,
		Status:      200,
		Offset:      info.NextOffset.String(),
		ContentType: info.ContentType,
	}
}

func handleDelete(cmd Command) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := client.Delete(ctx, cmd.Path)
	if err != nil {
		return errorResult("delete", err)
	}

	// Remove from cache
	delete(streamContentTypes, cmd.Path)

	return Result{
		Type:    "delete",
		Success: true,
		Status:  200,
	}
}

// getSentDynamic returns copies of the last sent headers/params, or nil if empty.
func getSentDynamic() (map[string]string, map[string]string) {
	var hdrs, params map[string]string
	if len(lastSentHeaders) > 0 {
		hdrs = make(map[string]string)
		for k, v := range lastSentHeaders {
			hdrs[k] = v
		}
	}
	if len(lastSentParams) > 0 {
		params = make(map[string]string)
		for k, v := range lastSentParams {
			params[k] = v
		}
	}
	return hdrs, params
}

func handleSetDynamicHeader(cmd Command) Result {
	dynamicHeaders[cmd.Name] = &dynamicValue{
		valueType:  cmd.ValueType,
		counter:    0,
		tokenValue: cmd.InitialValue,
	}
	return Result{
		Type:    "set-dynamic-header",
		Success: true,
	}
}

func handleSetDynamicParam(cmd Command) Result {
	dynamicParams[cmd.Name] = &dynamicValue{
		valueType:  cmd.ValueType,
		counter:    0,
		tokenValue: cmd.InitialValue,
	}
	return Result{
		Type:    "set-dynamic-param",
		Success: true,
	}
}

func handleClearDynamic(cmd Command) Result {
	dynamicHeaders = make(map[string]*dynamicValue)
	dynamicParams = make(map[string]*dynamicValue)
	return Result{
		Type:    "clear-dynamic",
		Success: true,
	}
}

func handleGetDynamicValues(cmd Command) Result {
	headerValues := make(map[string]string)
	for name, dv := range dynamicHeaders {
		headerValues[name] = dv.peek()
	}
	paramValues := make(map[string]string)
	for name, dv := range dynamicParams {
		paramValues[name] = dv.peek()
	}
	return Result{
		Type:         "get-dynamic-values",
		Success:      true,
		HeaderValues: headerValues,
		ParamValues:  paramValues,
	}
}

func handleValidate(cmd Command) Result {
	if cmd.Target == nil {
		return sendError("validate", "INVALID_ARGUMENT", "target is required")
	}

	switch cmd.Target.Target {
	case "idempotent-producer":
		return validateIdempotentProducer(cmd.Target)
	case "retry-options":
		// retry-options validation requires the retryOptions feature
		// We don't support this feature, so skip (return NOT_SUPPORTED)
		return sendError("validate", "NOT_SUPPORTED", "retry-options validation not supported")
	default:
		return sendError("validate", "NOT_SUPPORTED", fmt.Sprintf("unknown validation target: %s", cmd.Target.Target))
	}
}

func validateIdempotentProducer(target *ValidationTarget) Result {
	// Validate epoch if provided
	if target.Epoch != nil {
		if *target.Epoch < 0 {
			return Result{
				Type:        "error",
				Success:     false,
				CommandType: "validate",
				ErrorCode:   "INVALID_ARGUMENT",
				Message:     "epoch must be non-negative",
			}
		}
	}

	// Validate maxBatchBytes if provided
	if target.MaxBatchBytes != nil {
		if *target.MaxBatchBytes < 0 {
			return Result{
				Type:        "error",
				Success:     false,
				CommandType: "validate",
				ErrorCode:   "INVALID_ARGUMENT",
				Message:     "maxBatchBytes must be non-negative",
			}
		}
		// Note: zero is allowed in Go (treated as default), so we don't reject it here
		// unless the strictZeroValidation feature is required
	}

	// All validations passed
	return Result{
		Type:    "validate",
		Success: true,
	}
}

func errorResult(cmdType string, err error) Result {
	// Check for transport errors
	var tErr *transport.Error
	if errors.As(err, &tErr) {
		code := mapTransportErrorCode(tErr)
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      tErr.StatusCode,
			ErrorCode:   code,
			Message:     err.Error(),
		}
	}

	// Check for sentinel errors
	if errors.Is(err, durablestream.ErrNotFound) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      404,
			ErrorCode:   "NOT_FOUND",
			Message:     err.Error(),
		}
	}
	if errors.Is(err, durablestream.ErrSequenceConflict) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      409,
			ErrorCode:   "SEQUENCE_CONFLICT",
			Message:     err.Error(),
		}
	}
	if errors.Is(err, durablestream.ErrConflict) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      409,
			ErrorCode:   "CONFLICT",
			Message:     err.Error(),
		}
	}
	if errors.Is(err, durablestream.ErrGone) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      410,
			ErrorCode:   "INVALID_OFFSET",
			Message:     err.Error(),
		}
	}
	if errors.Is(err, durablestream.ErrBadRequest) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      400,
			ErrorCode:   "INVALID_OFFSET",
			Message:     err.Error(),
		}
	}
	if errors.Is(err, durablestream.ErrParseError) {
		return Result{
			Type:        "error",
			Success:     false,
			CommandType: cmdType,
			Status:      0, // Client-side error, no HTTP status
			ErrorCode:   "PARSE_ERROR",
			Message:     err.Error(),
		}
	}

	// Default to internal error
	return Result{
		Type:        "error",
		Success:     false,
		CommandType: cmdType,
		ErrorCode:   "INTERNAL_ERROR",
		Message:     err.Error(),
	}
}

func sendError(cmdType, code, message string) Result {
	return Result{
		Type:        "error",
		Success:     false,
		CommandType: cmdType,
		ErrorCode:   code,
		Message:     message,
	}
}

func mapTransportErrorCode(err *transport.Error) string {
	switch err.Code {
	case "NOT_FOUND", "not_found":
		return "NOT_FOUND"
	case "SEQUENCE_CONFLICT", "sequence_conflict":
		return "SEQUENCE_CONFLICT"
	case "CONFLICT", "conflict":
		return "CONFLICT"
	case "GONE", "gone":
		return "INVALID_OFFSET"
	case "BAD_REQUEST", "bad_request":
		return "INVALID_OFFSET"
	case "PARSE_ERROR":
		return "PARSE_ERROR"
	default:
		return "UNEXPECTED_STATUS"
	}
}

// normalizeContentType extracts the media type before semicolon and lowercases.
func normalizeContentType(contentType string) string {
	if contentType == "" {
		return ""
	}
	idx := strings.Index(contentType, ";")
	if idx >= 0 {
		contentType = contentType[:idx]
	}
	return strings.TrimSpace(strings.ToLower(contentType))
}
