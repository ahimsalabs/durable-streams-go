package durablestream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

const (
	defaultMaxAppendSize   = 10 * 1024 * 1024 // 10MB
	defaultChunkSize       = 1 * 1024 * 1024  // 1MB
	defaultLongPollTimeout = 30 * time.Second
	defaultSSECloseAfter   = 60 * time.Second
)

// HandlerConfig configures a Handler.
type HandlerConfig struct {
	// PathExtractor extracts the stream ID from the request.
	// Default: uses r.URL.Path.
	PathExtractor func(*http.Request) string

	// LongPollTimeout is the maximum wait time for long-poll requests. Default: 30s.
	LongPollTimeout time.Duration

	// SSECloseAfter is the duration after which SSE connections are closed. Default: 60s.
	SSECloseAfter time.Duration

	// MaxAppendSize is the maximum allowed size for append operations. Default: 10MB.
	MaxAppendSize int64

	// ChunkSize is the maximum response size (in bytes) for read operations.
	// When a read would return more data than this limit, results are paginated.
	// Default: 1MB.
	ChunkSize int
}

// Handler implements http.Handler for serving durable streams.
// Per spec Section 5: routes requests based on HTTP method.
type Handler struct {
	storage         Storage
	pathExtractor   func(*http.Request) string
	longPollTimeout time.Duration
	sseCloseAfter   time.Duration
	maxAppendSize   int64
	chunkSize       int
}

// NewHandler creates a new stream handler with the given storage.
// Pass nil for cfg to use defaults.
func NewHandler(storage Storage, cfg *HandlerConfig) *Handler {
	h := &Handler{
		storage:         storage,
		pathExtractor:   func(r *http.Request) string { return r.URL.Path },
		longPollTimeout: defaultLongPollTimeout,
		sseCloseAfter:   defaultSSECloseAfter,
		maxAppendSize:   defaultMaxAppendSize,
		chunkSize:       defaultChunkSize,
	}

	if cfg != nil {
		if cfg.PathExtractor != nil {
			h.pathExtractor = cfg.PathExtractor
		}
		if cfg.LongPollTimeout > 0 {
			h.longPollTimeout = cfg.LongPollTimeout
		}
		if cfg.SSECloseAfter > 0 {
			h.sseCloseAfter = cfg.SSECloseAfter
		}
		if cfg.MaxAppendSize > 0 {
			h.maxAppendSize = cfg.MaxAppendSize
		}
		if cfg.ChunkSize > 0 {
			h.chunkSize = cfg.ChunkSize
		}
	}

	return h
}

// ServeHTTP routes to appropriate handler based on method.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	streamID := h.pathExtractor(r)

	switch r.Method {
	case http.MethodPut:
		h.handleCreate(w, r, streamID)
	case http.MethodPost:
		h.handleAppend(w, r, streamID)
	case http.MethodGet:
		h.handleRead(w, r, streamID)
	case http.MethodHead:
		h.handleHead(w, r, streamID)
	case http.MethodDelete:
		h.handleDelete(w, r, streamID)
	default:
		writeError(w, newError(codeBadRequest, "method not allowed"))
	}
}

// handleCreate implements PUT (Create Stream) - Section 5.1
func (h *Handler) handleCreate(w http.ResponseWriter, r *http.Request, streamID string) {
	// Parse Content-Type header (default: application/octet-stream)
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// Parse TTL and Expires-At headers
	cfg := StreamConfig{
		ContentType: contentType,
	}

	hasTTL := r.Header.Get(protocol.HeaderStreamTTL) != ""
	hasExpiresAt := r.Header.Get(protocol.HeaderStreamExpiresAt) != ""

	// Reject if both are present (Section 5.1)
	if hasTTL && hasExpiresAt {
		writeError(w, newError(codeBadRequest, "cannot specify both Stream-TTL and Stream-Expires-At"))
		return
	}

	if hasTTL {
		ttlStr := r.Header.Get(protocol.HeaderStreamTTL)
		// Reject leading zeros (except "0" itself) and plus sign
		if len(ttlStr) == 0 || ttlStr[0] == '+' || (len(ttlStr) > 1 && ttlStr[0] == '0') {
			writeError(w, newError(codeBadRequest, "invalid Stream-TTL header"))
			return
		}
		ttlSec, err := strconv.ParseInt(ttlStr, 10, 64)
		if err != nil || ttlSec < 0 {
			writeError(w, newError(codeBadRequest, "invalid Stream-TTL header"))
			return
		}
		cfg.TTL = time.Duration(ttlSec) * time.Second
	}

	if hasExpiresAt {
		expiresAt, err := time.Parse(time.RFC3339, r.Header.Get(protocol.HeaderStreamExpiresAt))
		if err != nil {
			writeError(w, newError(codeBadRequest, "invalid Stream-Expires-At header (must be RFC3339)"))
			return
		}
		cfg.ExpiresAt = expiresAt
	}

	// Create stream
	created, err := h.storage.Create(r.Context(), streamID, cfg)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Get the tail offset (which will be 0 for a new empty stream)
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	nextOffset := info.NextOffset

	// Handle initial body content if provided
	if r.ContentLength > 0 || r.TransferEncoding != nil {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, newError(codeBadRequest, "failed to read request body"))
			return
		}

		if len(body) > 0 {
			// For JSON mode, process the body
			if protocol.IsJSONContentType(contentType) {
				messages, err := protocol.ProcessJSONAppend(body)
				if err != nil {
					// For PUT, empty arrays are allowed (creates empty stream)
					if !errors.Is(err, protocol.ErrEmptyArray) {
						writeError(w, newError(codeBadRequest, err.Error()))
						return
					}
					// Empty array is OK for PUT, just don't append anything
				}
				// Append each message
				for _, msg := range messages {
					nextOffset, err = h.storage.Append(r.Context(), streamID, msg, "")
					if err != nil {
						writeStorageError(w, err)
						return
					}
				}
			} else {
				// Non-JSON: append as-is
				nextOffset, err = h.storage.Append(r.Context(), streamID, body, "")
				if err != nil {
					writeStorageError(w, err)
					return
				}
			}
		}
	}

	// Return success headers
	// Location must be absolute URL per RFC 7231
	scheme := "http"
	if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
		scheme = "https"
	}
	w.Header().Set("Location", scheme+"://"+r.Host+r.URL.Path)
	w.Header().Set("Content-Type", contentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())

	// 201 Created for new streams, 200 OK for idempotent match
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

// handleAppend implements POST (Append) - Section 5.2
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, streamID string) {
	// Get stream info to validate content type
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Validate Content-Type is present and matches stream
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		writeError(w, newError(codeBadRequest, "Content-Type header required"))
		return
	}
	if !protocol.ContentTypesMatch(contentType, info.ContentType) {
		writeError(w, newError(codeConflict, "content type mismatch"))
		return
	}

	// Check Content-Length if provided (known size)
	if r.ContentLength > h.maxAppendSize {
		writeError(w, newError(codePayloadTooLarge, fmt.Sprintf("request body exceeds maximum size of %d bytes", h.maxAppendSize)))
		return
	}

	// Get sequence number if provided
	seq := r.Header.Get(protocol.HeaderStreamSeq)

	var nextOffset Offset

	// For JSON mode, we must buffer to parse/flatten arrays (Section 7.1)
	if protocol.IsJSONContentType(contentType) {
		// Buffer body for JSON parsing
		body, err := io.ReadAll(io.LimitReader(r.Body, h.maxAppendSize+1))
		if err != nil {
			writeError(w, newError(codeBadRequest, "failed to read request body"))
			return
		}

		// Reject empty body (Section 5.2)
		if len(body) == 0 {
			writeError(w, newError(codeBadRequest, "empty body not allowed"))
			return
		}

		// Check size after reading (for chunked transfers without Content-Length)
		if int64(len(body)) > h.maxAppendSize {
			writeError(w, newError(codePayloadTooLarge, fmt.Sprintf("request body exceeds maximum size of %d bytes", h.maxAppendSize)))
			return
		}

		messages, err := protocol.ProcessJSONAppend(body)
		if err != nil {
			writeError(w, newError(codeBadRequest, err.Error()))
			return
		}

		// Append each message
		for _, msg := range messages {
			nextOffset, err = h.storage.Append(r.Context(), streamID, msg, seq)
			if err != nil {
				writeStorageError(w, err)
				return
			}
			// Only use seq for first message to avoid multiple seq validations
			seq = ""
		}
	} else {
		// Non-JSON mode: stream directly to storage without buffering entire body.
		// This is critical for large uploads - avoids memory exhaustion.
		// Use a counting reader to detect empty bodies and enforce size limits.
		limitedReader := &limitedCountingReader{
			r:     r.Body,
			limit: h.maxAppendSize,
		}

		nextOffset, err = h.storage.AppendFrom(r.Context(), streamID, limitedReader, seq)
		if err != nil {
			writeStorageError(w, err)
			return
		}

		// Check if body was empty (after streaming)
		if limitedReader.n == 0 {
			writeError(w, newError(codeBadRequest, "empty body not allowed"))
			return
		}

		// Check if size limit was exceeded
		if limitedReader.exceeded {
			writeError(w, newError(codePayloadTooLarge, fmt.Sprintf("request body exceeds maximum size of %d bytes", h.maxAppendSize)))
			return
		}
	}

	// Return success
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())
	w.WriteHeader(http.StatusNoContent)
}

// handleRead implements GET (Read) - Sections 5.5, 5.6, 5.7
func (h *Handler) handleRead(w http.ResponseWriter, r *http.Request, streamID string) {
	// Reject duplicate query parameters
	query := r.URL.Query()
	if len(query[protocol.QueryOffset]) > 1 {
		writeError(w, newError(codeBadRequest, "duplicate offset parameter"))
		return
	}
	if len(query[protocol.QueryLive]) > 1 {
		writeError(w, newError(codeBadRequest, "duplicate live parameter"))
		return
	}

	// Parse and validate offset query parameter
	offsetStr := query.Get(protocol.QueryOffset)
	// Reject explicitly empty offset (?offset=) vs omitted offset
	if len(query[protocol.QueryOffset]) > 0 && offsetStr == "" {
		writeError(w, newError(codeBadRequest, "offset cannot be empty"))
		return
	}
	// Reject offsets containing invalid characters
	// Offsets should only contain alphanumeric characters, hyphens, underscores, and periods
	if !isValidOffset(offsetStr) {
		writeError(w, newError(codeBadRequest, "invalid offset format"))
		return
	}
	offset := Offset(offsetStr)

	// Route based on live query parameter
	liveMode := query.Get(protocol.QueryLive)

	switch liveMode {
	case "":
		// Catch-up read (Section 5.5)
		h.handleCatchupRead(w, r, streamID, offset)
	case protocol.LiveModeLongPoll:
		// Long-poll read (Section 5.6)
		h.handleLongPoll(w, r, streamID, offset)
	case protocol.LiveModeSSE:
		// SSE streaming (Section 5.7)
		h.handleSSE(w, r, streamID, offset)
	default:
		writeError(w, newError(codeBadRequest, "invalid live parameter"))
	}
}

// handleCatchupRead implements catch-up reads (Section 5.5)
func (h *Handler) handleCatchupRead(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Get stream info for content type
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Read data
	result, err := h.storage.Read(r.Context(), streamID, offset, h.chunkSize)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Set ETag (Section 5.5)
	etag := fmt.Sprintf("\"%s:%s:%s\"", streamID, offset, result.NextOffset)

	// Check If-None-Match for 304 Not Modified
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// Set headers
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())

	// Set Cache-Control (Section 8)
	w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

	w.Header().Set("ETag", etag)

	// Set Stream-Up-To-Date if at tail (Section 5.5)
	if result.NextOffset.Compare(result.TailOffset) == 0 {
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
	}

	// Format response based on content type
	responseBody := formatResponseBody(result.Messages, info.ContentType)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

// handleLongPoll implements long-poll reads (Section 5.6)
func (h *Handler) handleLongPoll(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Offset is required for long-poll
	if offset.IsZero() {
		writeError(w, newError(codeBadRequest, "offset required for long-poll"))
		return
	}

	// Generate cursor for CDN collapsing optimization
	// Cursor is based on time interval; if client sends same cursor, we advance it
	cursor := generateCursor(r.URL.Query().Get(protocol.QueryCursor))

	// Try immediate read first
	result, err := h.storage.Read(r.Context(), streamID, offset, h.chunkSize)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// If messages available, return immediately
	if len(result.Messages) > 0 {
		info, err := h.storage.Head(r.Context(), streamID)
		if err != nil {
			writeStorageError(w, err)
			return
		}

		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
		w.Header().Set(protocol.HeaderStreamCursor, cursor)
		w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

		responseBody := formatResponseBody(result.Messages, info.ContentType)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBody)
		return
	}

	// No data available, subscribe and wait
	// Use the shorter of the request context deadline or longPollTimeout
	waitCtx := r.Context()
	deadline, hasDeadline := r.Context().Deadline()
	timeout := h.longPollTimeout

	if hasDeadline {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(r.Context(), timeout)
		defer cancel()
	}

	notifyCh, err := h.storage.Subscribe(waitCtx, streamID, offset)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Wait for data or timeout
	select {
	case _, ok := <-notifyCh:
		if !ok {
			// Channel closed (stream deleted or error)
			writeError(w, newError(codeNotFound, "stream not found"))
			return
		}

		// Data arrived, read and return
		result, err := h.storage.Read(waitCtx, streamID, offset, h.chunkSize)
		if err != nil {
			writeStorageError(w, err)
			return
		}

		info, err := h.storage.Head(waitCtx, streamID)
		if err != nil {
			writeStorageError(w, err)
			return
		}

		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
		w.Header().Set(protocol.HeaderStreamCursor, cursor)
		w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

		responseBody := formatResponseBody(result.Messages, info.ContentType)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBody)

	case <-waitCtx.Done():
		// Timeout - return 204 No Content
		info, err := h.storage.Head(r.Context(), streamID)
		if err == nil {
			w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String())
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
		}
		w.Header().Set(protocol.HeaderStreamCursor, cursor)
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleSSE implements SSE streaming (Section 5.7)
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Offset is required for SSE
	if offset.IsZero() {
		writeError(w, newError(codeBadRequest, "offset required for SSE"))
		return
	}

	// Get cursor parameter for echo in control events
	cursor := r.URL.Query().Get(protocol.QueryCursor)

	// Get stream info
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Validate content type supports SSE (Section 5.7)
	if !protocol.IsSSECompatible(info.ContentType) {
		writeError(w, newError(codeBadRequest, "content type not compatible with SSE (must be text/* or application/json)"))
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Get flusher for streaming
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, newError(codeInternal, "streaming not supported"))
		return
	}

	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Set close timer
	closeTimer := time.NewTimer(h.sseCloseAfter)
	defer closeTimer.Stop()

	currentOffset := offset

	// Stream loop
	for {
		// Try to read data
		result, err := h.storage.Read(r.Context(), streamID, currentOffset, h.chunkSize)
		if err != nil {
			// Connection likely already established, just close
			return
		}

		// Generate cursor for this response
		sseCursor := generateCursor(cursor)

		// If messages available, send them
		if len(result.Messages) > 0 {
			// Send data event
			fmt.Fprintf(w, "event: data\n")

			if protocol.IsJSONContentType(info.ContentType) {
				// For JSON, format as single-line array
				// (SSE joins data: lines with \n which would create invalid JSON)
				jsonArray := formatResponseBody(result.Messages, info.ContentType)
				fmt.Fprintf(w, "data: %s\n", string(jsonArray))
			} else {
				// For text/*, send concatenated data split by lines
				data := concatenateMessages(result.Messages)
				lines := strings.Split(string(data), "\n")
				for _, line := range lines {
					fmt.Fprintf(w, "data: %s\n", line)
				}
			}
			fmt.Fprintf(w, "\n")

			// Send control event with cursor and upToDate status
			fmt.Fprintf(w, "event: control\n")
			isUpToDate := result.NextOffset.Compare(result.TailOffset) == 0
			if isUpToDate {
				fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"streamCursor\":\"%s\",\"upToDate\":true}\n\n", result.NextOffset, sseCursor)
			} else {
				fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"streamCursor\":\"%s\"}\n\n", result.NextOffset, sseCursor)
			}

			flusher.Flush()

			currentOffset = result.NextOffset
		} else {
			// No messages, but we still need to send control event with upToDate
			fmt.Fprintf(w, "event: control\n")
			fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"streamCursor\":\"%s\",\"upToDate\":true}\n\n", result.NextOffset, sseCursor)
			flusher.Flush()
		}

		// Check if we should close
		select {
		case <-closeTimer.C:
			// Close after timeout (Section 5.7)
			return
		case <-r.Context().Done():
			// Client disconnected
			return
		default:
			// Continue, but wait for new data
			if len(result.Messages) == 0 {
				// Subscribe and wait for new data
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				notifyCh, err := h.storage.Subscribe(ctx, streamID, currentOffset)
				if err != nil {
					cancel()
					return
				}

				select {
				case <-notifyCh:
					// New data available, loop will read it
				case <-closeTimer.C:
					cancel()
					return
				case <-r.Context().Done():
					cancel()
					return
				case <-ctx.Done():
					// Short timeout, loop again
				}
				cancel()
			}
		}
	}
}

// handleHead implements HEAD (Metadata) - Section 5.4
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, streamID string) {
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Set headers
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String())

	// Set TTL/Expires-At if present
	if info.TTL > 0 {
		w.Header().Set(protocol.HeaderStreamTTL, strconv.FormatInt(int64(info.TTL.Seconds()), 10))
	}
	if !info.ExpiresAt.IsZero() {
		w.Header().Set(protocol.HeaderStreamExpiresAt, info.ExpiresAt.Format(time.RFC3339))
	}

	// Set Cache-Control (Section 5.4)
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(http.StatusOK)
}

// handleDelete implements DELETE (Delete) - Section 5.3
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, streamID string) {
	err := h.storage.Delete(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// formatResponseBody formats messages for HTTP response based on content type.
// For JSON, wraps messages in a JSON array. For other types, concatenates raw bytes.
func formatResponseBody(messages []StoredMessage, contentType string) []byte {
	if len(messages) == 0 {
		if protocol.IsJSONContentType(contentType) {
			return []byte("[]")
		}
		return nil
	}

	if protocol.IsJSONContentType(contentType) {
		// Extract raw message data for JSON formatting
		rawMessages := make([][]byte, len(messages))
		for i, msg := range messages {
			rawMessages[i] = msg.Data
		}
		return protocol.FormatJSONResponse(rawMessages)
	}

	// Non-JSON: concatenate all message data
	return concatenateMessages(messages)
}

// concatenateMessages concatenates all message data into a single byte slice.
func concatenateMessages(messages []StoredMessage) []byte {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) == 1 {
		return messages[0].Data
	}

	// Calculate total size
	total := 0
	for _, msg := range messages {
		total += len(msg.Data)
	}

	// Concatenate
	result := make([]byte, 0, total)
	for _, msg := range messages {
		result = append(result, msg.Data...)
	}
	return result
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, err *protoError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(err.Code.httpStatus())
	_ = json.NewEncoder(w).Encode(err)
}

// writeStorageError converts a storage error to an HTTP error response.
// Handles both protoError (from internal use) and sentinel errors (from storage).
func writeStorageError(w http.ResponseWriter, err error) {
	if protoErr, ok := err.(*protoError); ok {
		writeError(w, protoErr)
		return
	}

	// Map sentinel errors to protocol errors
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, newError(codeNotFound, err.Error()))
	case errors.Is(err, ErrGone):
		writeError(w, newError(codeGone, err.Error()))
	case errors.Is(err, ErrConflict):
		writeError(w, newError(codeConflict, err.Error()))
	case errors.Is(err, ErrBadRequest):
		writeError(w, newError(codeBadRequest, err.Error()))
	default:
		writeError(w, newError(codeInternal, err.Error()))
	}
}

// limitedCountingReader wraps an io.Reader to count bytes read and enforce a size limit.
// Unlike io.LimitReader, it tracks whether the limit was exceeded rather than just stopping.
type limitedCountingReader struct {
	r        io.Reader
	limit    int64
	n        int64 // bytes read so far
	exceeded bool  // true if limit was exceeded
}

func (l *limitedCountingReader) Read(p []byte) (n int, err error) {
	n, err = l.r.Read(p)
	l.n += int64(n)
	if l.n > l.limit {
		l.exceeded = true
	}
	return n, err
}

// generateCursor generates a cursor for CDN collapsing optimization.
// The cursor is based on Unix milliseconds. If clientCursor is provided and equals
// the current time bucket, we advance to the next one to prevent cache cycles.
func generateCursor(clientCursor string) string {
	now := time.Now().UnixMilli()
	cursor := strconv.FormatInt(now, 10)

	// If client sent a cursor, ensure we return a strictly greater one
	if clientCursor != "" {
		clientVal, err := strconv.ParseInt(clientCursor, 10, 64)
		if err == nil && clientVal >= now {
			// Client cursor is >= current time, advance by 1
			cursor = strconv.FormatInt(clientVal+1, 10)
		}
	}

	return cursor
}

// isValidOffset checks if an offset string contains only valid characters.
// Valid offsets contain only alphanumeric characters, hyphens, and underscores.
// Control characters, whitespace, and special URL characters are rejected.
func isValidOffset(s string) bool {
	if s == "" {
		return true // Empty offset is handled separately
	}
	for _, r := range s {
		// Allow alphanumeric, hyphen, underscore, and period
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}
