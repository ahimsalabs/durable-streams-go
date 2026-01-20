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
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
)

const (
	defaultMaxAppendSize   = 10 * 1024 * 1024 // 10MB
	defaultChunkSize       = 1 * 1024 * 1024  // 1MB
	defaultLongPollTimeout = 30 * time.Second
	defaultSSECloseAfter   = 60 * time.Second

	// Security headers per protocol Section 10.7
	headerXContentTypeOptions       = "X-Content-Type-Options"
	headerCrossOriginResourcePolicy = "Cross-Origin-Resource-Policy"
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

// producerState tracks the state for a single producer on a single stream.
// Per PROTOCOL.md Section 5.2.1.
type producerState struct {
	epoch   int64 // Current epoch for this producer
	lastSeq int64 // Highest accepted sequence number in current epoch
}

// producerKey uniquely identifies a producer on a stream.
type producerKey struct {
	streamID   string
	producerID string
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

	// Producer state tracking (PROTOCOL.md Section 5.2.1)
	producersMu sync.Mutex
	producers   map[producerKey]*producerState
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
		producers:       make(map[producerKey]*producerState),
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
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
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
				// Use ProcessJSONCreate for PUT - allows empty arrays per Section 7.1
				messages, err := protocol.ProcessJSONCreate(body)
				if err != nil {
					writeError(w, newError(codeBadRequest, err.Error()))
					return
				}
				// Append each message (may be empty for [] body)
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
	setSecurityHeaders(w)

	// Set Location header on 201 Created (Section 5.1)
	// Use r.RequestURI which preserves the original path (before StripPrefix etc.)
	// Location must be absolute URL per RFC 7231
	if created {
		scheme := "http"
		if r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https" {
			scheme = "https"
		}
		// r.RequestURI is the unmodified request-target, preserving the original path
		// We need to strip any query string for the Location header
		path := r.RequestURI
		if idx := strings.Index(path, "?"); idx != -1 {
			path = path[:idx]
		}
		w.Header().Set("Location", scheme+"://"+r.Host+path)
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())

	// 201 Created for new streams, 200 OK for idempotent match
	if created {
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

// producerResult represents the outcome of producer validation.
type producerResult struct {
	isDuplicate bool  // True if this is a duplicate request (return 204)
	isNew       bool  // True if this is a new request (return 200)
	epoch       int64 // Epoch to echo in response
	highestSeq  int64 // Highest seq to echo in response
}

// handleAppend implements POST (Append) - Section 5.2
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, streamID string) {
	// Get stream info to validate content type
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Validate Content-Type is present and matches stream (Section 5.2)
	// Per spec: "MUST match the stream's existing content type"
	// Per spec: "MUST return 409 Conflict when the content type is valid but does not match"
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

	// Parse and validate producer headers (Section 5.2.1)
	producerID, producerEpoch, producerSeq, hasProducer, err := h.parseProducerHeaders(r)
	if err != nil {
		writeError(w, newError(codeBadRequest, err.Error()))
		return
	}

	// Validate producer state BEFORE reading body (for deduplication efficiency)
	var producerRes *producerResult
	if hasProducer {
		producerRes, err = h.validateProducer(w, streamID, producerID, producerEpoch, producerSeq)
		if err != nil {
			// Error already written to response
			return
		}
		// If duplicate, return 204 immediately without reading body
		if producerRes.isDuplicate {
			setSecurityHeaders(w)
			w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String())
			w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(producerRes.epoch, 10))
			w.Header().Set(protocol.HeaderProducerSeq, strconv.FormatInt(producerRes.highestSeq, 10))
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	// Get Stream-Seq if provided (separate from producer seq)
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
		// Per spec: "Servers MUST reject POST requests with an empty body...with 400 Bad Request"
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

		// Check if body was empty (after streaming) - Section 5.2
		// Per spec: "Servers MUST reject POST requests with an empty body...with 400 Bad Request"
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

	// Commit producer state after successful append
	if hasProducer {
		h.commitProducerState(streamID, producerID, producerEpoch, producerSeq)
	}

	// Return success
	setSecurityHeaders(w)
	w.Header().Set(protocol.HeaderStreamNextOffset, nextOffset.String())

	if hasProducer {
		// With producer headers: return 200 OK for new data
		w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(producerEpoch, 10))
		w.Header().Set(protocol.HeaderProducerSeq, strconv.FormatInt(producerSeq, 10))
		w.WriteHeader(http.StatusOK)
	} else {
		// Without producer headers: return 204 No Content
		w.WriteHeader(http.StatusNoContent)
	}
}

// parseProducerHeaders extracts and validates producer headers.
// Per Section 5.2.1: all three headers MUST be provided together or none at all.
func (h *Handler) parseProducerHeaders(r *http.Request) (producerID string, epoch, seq int64, hasProducer bool, err error) {
	idStr := r.Header.Get(protocol.HeaderProducerID)
	epochStr := r.Header.Get(protocol.HeaderProducerEpoch)
	seqStr := r.Header.Get(protocol.HeaderProducerSeq)

	// Count how many producer headers are present
	hasID := idStr != ""
	hasEpoch := epochStr != ""
	hasSeq := seqStr != ""

	// All or none
	count := 0
	if hasID {
		count++
	}
	if hasEpoch {
		count++
	}
	if hasSeq {
		count++
	}

	if count == 0 {
		return "", 0, 0, false, nil
	}
	if count != 3 {
		return "", 0, 0, false, errors.New("all producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together")
	}

	// Validate Producer-Id is non-empty (already checked above with hasID)
	producerID = idStr

	// Parse and validate epoch (must be non-negative integer ≤ 2^53-1)
	epoch, err = parseProducerInt(epochStr, "Producer-Epoch")
	if err != nil {
		return "", 0, 0, false, err
	}

	// Parse and validate seq (must be non-negative integer ≤ 2^53-1)
	seq, err = parseProducerInt(seqStr, "Producer-Seq")
	if err != nil {
		return "", 0, 0, false, err
	}

	return producerID, epoch, seq, true, nil
}

// parseProducerInt parses a string as a non-negative integer with strict validation.
// Rejects leading zeros (except "0"), plus signs, and values > 2^53-1.
func parseProducerInt(s string, headerName string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("invalid %s: empty value", headerName)
	}

	// Reject leading zeros (except "0" itself)
	if len(s) > 1 && s[0] == '0' {
		return 0, fmt.Errorf("invalid %s: leading zeros not allowed", headerName)
	}

	// Reject plus sign
	if s[0] == '+' {
		return 0, fmt.Errorf("invalid %s: plus sign not allowed", headerName)
	}

	// Reject negative numbers
	if s[0] == '-' {
		return 0, fmt.Errorf("invalid %s: negative values not allowed", headerName)
	}

	// Parse as integer
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: must be a valid integer", headerName)
	}

	// Check max value for JavaScript interoperability (2^53-1)
	const maxSafeInt = 9007199254740991
	if val > maxSafeInt {
		return 0, fmt.Errorf("invalid %s: exceeds maximum safe integer", headerName)
	}

	return val, nil
}

// validateProducer implements the validation logic from Section 5.2.1.
// Returns a producerResult or writes an error response and returns an error.
func (h *Handler) validateProducer(w http.ResponseWriter, streamID, producerID string, epoch, seq int64) (*producerResult, error) {
	h.producersMu.Lock()
	defer h.producersMu.Unlock()

	key := producerKey{streamID: streamID, producerID: producerID}
	state := h.producers[key]

	// New producer - validate seq starts at 0
	if state == nil {
		if seq != 0 {
			setSecurityHeaders(w)
			w.Header().Set(protocol.HeaderProducerExpectedSeq, "0")
			w.Header().Set(protocol.HeaderProducerReceivedSeq, strconv.FormatInt(seq, 10))
			writeError(w, newError(codeConflict, "sequence gap: expected 0"))
			return nil, errors.New("sequence gap")
		}
		// Will be committed after successful append
		return &producerResult{isNew: true, epoch: epoch, highestSeq: seq}, nil
	}

	// Epoch validation (client-declared, server-validated)
	if epoch < state.epoch {
		// Stale epoch - zombie fencing (403 Forbidden)
		setSecurityHeaders(w)
		w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(state.epoch, 10))
		writeError(w, newError(codeForbidden, "stale producer epoch"))
		return nil, errors.New("stale epoch")
	}

	if epoch > state.epoch {
		// New epoch - must start at seq=0
		if seq != 0 {
			writeError(w, newError(codeBadRequest, "new epoch must start at seq=0"))
			return nil, errors.New("new epoch must start at seq=0")
		}
		// Will be committed after successful append
		return &producerResult{isNew: true, epoch: epoch, highestSeq: seq}, nil
	}

	// Same epoch - sequence validation
	if seq <= state.lastSeq {
		// Duplicate (idempotent success)
		return &producerResult{isDuplicate: true, epoch: state.epoch, highestSeq: state.lastSeq}, nil
	}

	if seq == state.lastSeq+1 {
		// Valid next sequence - will be committed after successful append
		return &producerResult{isNew: true, epoch: epoch, highestSeq: seq}, nil
	}

	// Sequence gap (seq > lastSeq + 1)
	setSecurityHeaders(w)
	w.Header().Set(protocol.HeaderProducerExpectedSeq, strconv.FormatInt(state.lastSeq+1, 10))
	w.Header().Set(protocol.HeaderProducerReceivedSeq, strconv.FormatInt(seq, 10))
	writeError(w, newError(codeConflict, "sequence gap"))
	return nil, errors.New("sequence gap")
}

// commitProducerState updates the producer state after a successful append.
func (h *Handler) commitProducerState(streamID, producerID string, epoch, seq int64) {
	h.producersMu.Lock()
	defer h.producersMu.Unlock()

	key := producerKey{streamID: streamID, producerID: producerID}
	h.producers[key] = &producerState{
		epoch:   epoch,
		lastSeq: seq,
	}
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
	// Reject invalid offsets per spec Sections 6 and 10.2
	if err := validateOffset(offsetStr); err != nil {
		writeError(w, newError(codeBadRequest, err.Error()))
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

	// Get cursor parameter for CDN collapsing (Section 8.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)

	// Handle offset=now sentinel (Section 6)
	// Returns empty response with current tail offset
	if offset == Offset(protocol.OffsetNow) {
		setSecurityHeaders(w)
		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String())
		w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
		// Per spec: SHOULD return Cache-Control: no-store for offset=now
		w.Header().Set("Cache-Control", "no-store")

		w.WriteHeader(http.StatusOK)
		// Return empty body appropriate to content type
		if protocol.IsJSONContentType(info.ContentType) {
			_, _ = w.Write([]byte("[]"))
		}
		return
	}

	// Read data
	result, err := h.storage.Read(r.Context(), streamID, offset, h.chunkSize)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Compute ETag (Section 5.5)
	// Format: "{streamID}:{start_offset}:{end_offset}"
	// ETag must be quoted per HTTP spec (RFC 7232)
	// Sanitize streamID to ensure no control characters in header value
	etag := fmt.Sprintf(`"%s:%s:%s"`, SanitizeForETag(streamID), offset, result.NextOffset)

	// Check If-None-Match for 304 Not Modified (Section 8.1)
	// Per spec: "When a client provides a valid If-None-Match header that matches
	// the current ETag, servers MUST respond with 304 Not Modified"
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		if etagMatches(ifNoneMatch, etag) {
			setSecurityHeaders(w)
			w.Header().Set("ETag", etag)
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
			w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Set headers
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
	w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))

	// Set Cache-Control (Section 8)
	w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

	// Set ETag
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

	// Get cursor parameter for CDN collapsing (Section 8.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)

	// Handle offset=now sentinel (Section 6)
	// For long-poll, immediately begin waiting (no initial empty response)
	isOffsetNow := offset == Offset(protocol.OffsetNow)
	if isOffsetNow {
		// Get the actual tail offset to wait from
		info, err := h.storage.Head(r.Context(), streamID)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		// Replace "now" with actual tail offset for subscription
		offset = info.NextOffset
	}

	// Try immediate read first (skip for offset=now per spec)
	if !isOffsetNow {
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

			setSecurityHeaders(w)
			w.Header().Set("Content-Type", info.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
			w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

			// Set Stream-Up-To-Date if at tail (per spec Section 5.6)
			if result.NextOffset.Compare(result.TailOffset) == 0 {
				w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			}

			responseBody := formatResponseBody(result.Messages, info.ContentType)

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(responseBody)
			return
		}
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

		setSecurityHeaders(w)
		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
		w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
		w.Header().Set("Cache-Control", "public, max-age=60, stale-while-revalidate=300")

		// Set Stream-Up-To-Date if at tail (per spec Section 5.6)
		if result.NextOffset.Compare(result.TailOffset) == 0 {
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
		}

		responseBody := formatResponseBody(result.Messages, info.ContentType)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(responseBody)

	case <-waitCtx.Done():
		// Timeout - return 204 No Content (per spec Section 5.6)
		setSecurityHeaders(w)
		info, err := h.storage.Head(r.Context(), streamID)
		if err == nil {
			w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String())
		}
		w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
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

	// Get cursor parameter for CDN collapsing (Section 8.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)

	// Get stream info
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Validate content type supports SSE (Section 5.7, Section 7)
	// Per spec: "ONLY valid for streams with content-type: text/* or application/json"
	if !protocol.IsSSECompatible(info.ContentType) {
		writeError(w, newError(codeBadRequest, "content type not compatible with SSE (must be text/* or application/json)"))
		return
	}

	// Handle offset=now sentinel (Section 6)
	// Start from tail position, sending initial control event with upToDate
	isOffsetNow := offset == Offset(protocol.OffsetNow)
	if isOffsetNow {
		// Replace "now" with actual tail offset
		offset = info.NextOffset
	}

	// Set SSE headers
	setSecurityHeaders(w)
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

	// For offset=now, send initial control event with upToDate: true (per spec Section 6)
	if isOffsetNow {
		fmt.Fprintf(w, "event: control\n")
		fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"upToDate\":true,\"streamCursor\":\"%s\"}\n\n", currentOffset, protocol.GenerateCursor(clientCursor))
		flusher.Flush()
	}

	// Track whether we've sent the initial control event for non-offset=now cases.
	// Per spec, when client is caught up (at tail), we should send a control event
	// with upToDate: true even if there's no data.
	sentInitialControl := isOffsetNow

	// Stream loop
	for {
		// Try to read data
		result, err := h.storage.Read(r.Context(), streamID, currentOffset, h.chunkSize)
		if err != nil {
			// Connection likely already established, just close
			return
		}

		// If messages available, send them
		if len(result.Messages) > 0 {
			sentInitialControl = true // We're sending events now

			// Send data event
			fmt.Fprintf(w, "event: data\n")

			if protocol.IsJSONContentType(info.ContentType) {
				// For JSON, format as single-line array
				// (SSE joins data: lines with \n which would create invalid JSON)
				jsonArray := formatResponseBody(result.Messages, info.ContentType)
				fmt.Fprintf(w, "data: %s\n", string(jsonArray))
			} else {
				// For text/*, send concatenated data split by lines.
				// Per SSE spec, lines can be terminated by CR, LF, or CRLF.
				// We must split by all terminators to prevent CRLF injection attacks
				// where embedded CR characters could be interpreted as line terminators.
				data := concatenateMessages(result.Messages)
				lines := splitBySSELineTerminators(string(data))
				for _, line := range lines {
					fmt.Fprintf(w, "data: %s\n", line)
				}
			}
			fmt.Fprintf(w, "\n")

			// Send control event with generated cursor (Section 8.1)
			// Include upToDate: true if we're at the tail
			fmt.Fprintf(w, "event: control\n")
			if result.NextOffset.Compare(result.TailOffset) == 0 {
				fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"upToDate\":true,\"streamCursor\":\"%s\"}\n\n", result.NextOffset, protocol.GenerateCursor(clientCursor))
			} else {
				fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"streamCursor\":\"%s\"}\n\n", result.NextOffset, protocol.GenerateCursor(clientCursor))
			}

			flusher.Flush()

			currentOffset = result.NextOffset
		} else if !sentInitialControl {
			// No messages and haven't sent initial control yet.
			// Check if we're at tail (caught up) and send control with upToDate: true.
			// This handles the case of SSE on an empty stream or starting from tail.
			if currentOffset.Compare(result.TailOffset) == 0 || result.NextOffset.Compare(result.TailOffset) == 0 {
				sentInitialControl = true
				fmt.Fprintf(w, "event: control\n")
				fmt.Fprintf(w, "data: {\"streamNextOffset\":\"%s\",\"upToDate\":true,\"streamCursor\":\"%s\"}\n\n", result.NextOffset, protocol.GenerateCursor(clientCursor))
				flusher.Flush()
			}
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
// Response codes: 200 OK, 404 Not Found, 429 Too Many Requests (Section 5.4)
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, streamID string) {
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Set response headers per Section 5.4
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", info.ContentType)                          // Per spec: stream's content type
	w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String()) // Per spec: current tail offset

	// Set TTL/Expires-At if present (Section 5.4)
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
// Response codes: 204 No Content (success), 404 Not Found, 429 Too Many Requests (Section 5.3)
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, streamID string) {
	err := h.storage.Delete(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	setSecurityHeaders(w)
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

// splitBySSELineTerminators splits a string by SSE line terminators (CR, LF, or CRLF).
// Per the SSE spec, all three are valid line terminators. This function is used to
// safely encode text data for SSE transmission, preventing CRLF injection attacks
// where embedded CR or LF characters could be interpreted as event boundaries.
func splitBySSELineTerminators(s string) []string {
	if s == "" {
		return []string{""}
	}

	var lines []string
	var current strings.Builder

	i := 0
	for i < len(s) {
		c := s[i]
		if c == '\r' {
			// CR - ends current line; if followed by LF, consume both as one terminator
			lines = append(lines, current.String())
			current.Reset()
			if i+1 < len(s) && s[i+1] == '\n' {
				i++ // Skip the LF in CRLF
			}
		} else if c == '\n' {
			// LF - ends current line
			lines = append(lines, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
		i++
	}

	// Append final segment (may be empty if string ended with terminator)
	lines = append(lines, current.String())

	return lines
}

// setSecurityHeaders adds browser security headers to the response.
// Per protocol Section 10.7, these headers prevent MIME-sniffing attacks
// and cross-origin embedding exploits.
func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	w.Header().Set(headerCrossOriginResourcePolicy, "cross-origin")
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, err *protoError) {
	setSecurityHeaders(w)
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

// etagMatches checks if the If-None-Match header value matches the given ETag.
// Per RFC 7232, If-None-Match can contain multiple ETags (comma-separated) or "*".
// ETags are compared as opaque quoted strings.
func etagMatches(ifNoneMatch, etag string) bool {
	// Handle wildcard
	if strings.TrimSpace(ifNoneMatch) == "*" {
		return true
	}

	// Split by comma and check each ETag
	// If-None-Match: "etag1", "etag2", ...
	for _, candidate := range strings.Split(ifNoneMatch, ",") {
		candidate = strings.TrimSpace(candidate)
		// Exact match (ETags include quotes)
		if candidate == etag {
			return true
		}
		// Handle weak ETags: W/"etag" matches "etag" for cache validation
		// Per RFC 7232, weak comparison is used for If-None-Match
		if strings.HasPrefix(candidate, "W/") && candidate[2:] == etag {
			return true
		}
	}

	return false
}

// SanitizeForETag encodes characters that are invalid in HTTP header values.
// Per RFC 7230, header values must not contain NUL, CR, or LF characters.
// We URL-encode any byte < 0x20 (control characters) or > 0x7E to ensure valid HTTP headers.
func SanitizeForETag(s string) string {
	var needsEncoding bool
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 || s[i] > 0x7E {
			needsEncoding = true
			break
		}
	}
	if !needsEncoding {
		return s
	}

	var buf strings.Builder
	buf.Grow(len(s) * 3) // worst case: all bytes need encoding
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b < 0x20 || b > 0x7E {
			buf.WriteString(fmt.Sprintf("%%%02X", b))
		} else {
			buf.WriteByte(b)
		}
	}
	return buf.String()
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

// validateOffset validates an offset string per protocol Section 6 and 10.2.
// Returns an error if the offset contains invalid characters or patterns.
//
// Per Section 6: Offsets MUST NOT contain commas, ampersands, equals signs,
// or question marks (to avoid conflict with URL query parameter syntax).
//
// Per Section 10.2: Servers SHOULD validate and sanitize to prevent path
// traversal attacks (patterns like "..").
func validateOffset(offset string) error {
	// Empty offset is valid (equivalent to stream start)
	if offset == "" {
		return nil
	}

	// Per Section 6: MUST NOT contain these URL query parameter conflict characters
	if strings.ContainsAny(offset, ",&=?") {
		return errors.New("invalid offset format")
	}

	// Reject whitespace and control characters (common validation)
	if strings.ContainsAny(offset, " \t\n\r") {
		return errors.New("invalid offset format")
	}

	// Per Section 10.2: prevent path traversal attacks
	// Check for ".." anywhere in the offset
	if strings.Contains(offset, "..") {
		return errors.New("invalid offset format")
	}

	return nil
}
