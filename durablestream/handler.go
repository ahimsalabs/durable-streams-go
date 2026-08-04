package durablestream

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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

	// Security headers per protocol Section 12.7
	headerXContentTypeOptions       = "X-Content-Type-Options"
	headerCrossOriginResourcePolicy = "Cross-Origin-Resource-Policy"

	headerAccessControlAllowOrigin   = "Access-Control-Allow-Origin"
	headerAccessControlAllowMethods  = "Access-Control-Allow-Methods"
	headerAccessControlAllowHeaders  = "Access-Control-Allow-Headers"
	headerAccessControlExposeHeaders = "Access-Control-Expose-Headers"
	headerAccessControlMaxAge        = "Access-Control-Max-Age"

	// corsMaxAgeSeconds caps how long a browser may reuse a preflight result.
	// One day is the practical ceiling browsers honor, and the policy below is
	// static, so there is nothing to invalidate sooner.
	corsMaxAgeSeconds = "86400"
)

// CORS policy for browser clients. The protocol defines no CORS requirements of
// its own, so these lists are derived from what the protocol actually puts on
// the wire: every method this handler routes, every request header a client may
// send, and every response header a client must be able to read.
var (
	corsAllowMethods = strings.Join([]string{
		http.MethodGet, http.MethodHead, http.MethodPost,
		http.MethodPut, http.MethodDelete, http.MethodOptions,
	}, ", ")

	corsAllowHeaders = strings.Join([]string{
		"Authorization",
		"Content-Type",
		"Cache-Control",
		// Conditional read of a catch-up response (Section 10.1).
		"If-None-Match",
		protocol.HeaderStreamTTL,
		protocol.HeaderStreamExpiresAt,
		protocol.HeaderStreamSeq,
		protocol.HeaderStreamPrivate,
		protocol.HeaderProducerID,
		protocol.HeaderProducerEpoch,
		protocol.HeaderProducerSeq,
	}, ", ")

	corsExposeHeaders = strings.Join([]string{
		"ETag",
		"Location",
		protocol.HeaderStreamNextOffset,
		protocol.HeaderStreamCursor,
		protocol.HeaderStreamUpToDate,
		protocol.HeaderStreamTTL,
		protocol.HeaderStreamExpiresAt,
		protocol.HeaderStreamSSEDataEncoding,
		protocol.HeaderProducerEpoch,
		protocol.HeaderProducerSeq,
		protocol.HeaderProducerExpectedSeq,
		protocol.HeaderProducerReceivedSeq,
	}, ", ")
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

	// MaxProducers bounds committed (stream, producer) idempotency states plus
	// first appends currently in flight. At capacity, a new producer receives
	// 429; existing non-expired producers remain available. Producer state is
	// process-local: it is not persisted by Storage and does not survive a Handler
	// restart. Default: 10000.
	MaxProducers int

	// ProducerStateTTL is how long an idle process-local idempotent-producer state
	// is retained. Once it expires, an old retry can be accepted as new data and a
	// stale epoch is no longer fenced. Values <= 0 use the protocol-recommended
	// in-memory default of seven days.
	ProducerStateTTL time.Duration

	// EnableCORS installs a permissive, credential-free browser policy that
	// allows every stream method and protocol header from any origin. Leave it
	// false when an outer authentication/CORS layer owns browser access.
	// Default: false.
	EnableCORS bool
}

// Handler implements http.Handler for serving durable streams.
// Per spec Section 5: routes requests based on HTTP method.
//
// Producer state and per-stream lifecycle locks are process-local. Use one
// Handler instance for all HTTP mutations of a Storage when relying on
// idempotent-producer guarantees; mutating the same Storage directly or through
// another Handler bypasses those safeguards. The built-in Storage
// implementations additionally expose incarnation identity so in-flight reads
// can reject a cross-Handler replacement; a custom Storage that leaves
// IncarnationID empty cannot provide that cross-Handler protection. Initial
// content and multi-message JSON appends require [AtomicBatchStorage]; a custom
// backend without that optional capability receives 501 for those requests
// rather than exposing a partial commit.
type Handler struct {
	storage         Storage
	pathExtractor   func(*http.Request) string
	longPollTimeout time.Duration
	sseCloseAfter   time.Duration
	maxAppendSize   int64
	chunkSize       int
	enableCORS      bool

	// mutations binds create, append, delete, and producer-state changes to one
	// stream incarnation. It is process-local, like the producer registry; use a
	// single Handler for all mutations of a Storage when relying on idempotent
	// producer guarantees.
	mutations keyedMutex

	// Producer state tracking (PROTOCOL.md Section 5.2.1)
	producers *producerRegistry
}

// NewHandler creates a new stream handler with the given storage.
// Pass nil for cfg to use defaults.
func NewHandler(storage Storage, cfg *HandlerConfig) *Handler {
	maxProducers := defaultMaxProducers
	producerStateTTL := defaultProducerStateTTL
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
		if cfg.MaxProducers > 0 {
			maxProducers = cfg.MaxProducers
		}
		if cfg.ProducerStateTTL > 0 {
			producerStateTTL = cfg.ProducerStateTTL
		}
		h.enableCORS = cfg.EnableCORS
	}
	h.producers = newProducerRegistry(maxProducers, producerStateTTL)

	return h
}

// ServeHTTP routes to appropriate handler based on method.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	streamID := h.pathExtractor(r)

	// Browser clients read protocol headers off every response and send
	// If-None-Match and the Stream-*/Producer-* request headers, none of which
	// are CORS-safelisted. Advertise them on every response, errors included, so
	// a cross-origin fetch behaves the same as a same-origin one.
	h.setCORSHeaders(w)

	// The __ds prefix is reserved for Durable Streams control APIs (spec Section 6),
	// which this handler does not implement. Reject it before any stream operation so
	// application streams cannot squat on the namespace: allowing them would make
	// adding subscription support later a breaking change. 404 is used because
	// nothing is served at these paths — for any method, including PUT.
	if hasReservedSegment(streamID) {
		writeError(w, newError(codeNotFound, "path segment \"__ds\" is reserved"))
		return
	}

	if r.Method == http.MethodOptions {
		// Preflight is answered without consulting storage: the browser asks
		// about the request it is allowed to make, not about the resource, and
		// the stream need not exist yet (a PUT may be what follows).
		w.WriteHeader(http.StatusNoContent)
		return
	}

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

// touch restarts a stream's sliding TTL window because this request counts as
// activity on it (Section 5.1).
//
// Which requests count is a decision only the handler can make, so every reset
// in this file is one of these calls: reads and writes that reach the origin
// reset the countdown, HEAD does not, and for the live modes the reset happens
// when the server begins processing rather than when data is delivered. Callers
// therefore touch before doing their work, not after — a long-poll that waits
// and returns nothing has still kept its stream alive.
//
// A failed renewal is a failed request, not best-effort bookkeeping. Returning
// success while the durable expiry was not moved tells a client its activity
// kept the stream alive when it did not. Pre-response callers map the error to an
// HTTP response; an SSE stream whose response is already committed terminates.
func (h *Handler) touch(r *http.Request, streamID string) error {
	return h.storage.Touch(r.Context(), streamID)
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
		if err != nil || ttlSec < 0 || ttlSec > math.MaxInt64/int64(time.Second) {
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

	// Parse Stream-Private header (Section 10.1)
	if privateStr := r.Header.Get(protocol.HeaderStreamPrivate); privateStr != "" {
		if privateStr == "true" {
			cfg.IsPrivate = true
		} else if privateStr != "false" {
			writeError(w, newError(codeBadRequest, "invalid Stream-Private header (must be 'true' or 'false')"))
			return
		}
	}

	// Read the initial body before creating anything, so an oversized body is
	// rejected without leaving an empty stream behind.
	body, protoErr := h.readBoundedBody(r)
	if protoErr != nil {
		writeError(w, protoErr)
		return
	}

	// Validate and split JSON before creating anything. A malformed PUT must not
	// return 400 after leaving an empty stream behind.
	var initialMessages [][]byte
	if len(body) > 0 {
		if protocol.IsJSONContentType(contentType) {
			messages, err := protocol.ProcessJSONCreate(body)
			if err != nil {
				writeError(w, newError(codeBadRequest, err.Error()))
				return
			}
			initialMessages = messages
		} else {
			initialMessages = [][]byte{body}
		}
	}

	// Creating a stream with initial data must be one storage mutation: if an
	// append failed after Create, an idempotent retry would see the empty stream
	// and skip its initial body. Custom Storage implementations can continue to
	// serve empty PUTs, but must opt into AtomicBatchStorage for non-empty ones.
	batchStorage, hasAtomicBatch := h.storage.(AtomicBatchStorage)
	if len(initialMessages) > 0 && !hasAtomicBatch {
		writeError(w, newError(codeNotImplemented, "storage does not support atomic stream creation with initial content"))
		return
	}
	// From the Create commit through producer-state reset and initial appends,
	// no POST or DELETE handled by this Handler may cross stream incarnations.
	unlock := h.mutations.lock(streamID)
	defer unlock()
	// Start the relative window at the actual serialized create attempt, not
	// before a potentially slow request body read or wait for another mutation.
	if hasTTL {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}

	var (
		created    bool
		nextOffset Offset
		err        error
	)
	if len(initialMessages) > 0 {
		created, nextOffset, err = batchStorage.CreateWithMessages(r.Context(), streamID, cfg, initialMessages)
	} else {
		created, err = h.storage.Create(r.Context(), streamID, cfg)
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}

	if created {
		// Invalidate live reads of the absent/expired predecessor before any
		// initial content becomes visible in this new incarnation.
		h.mutations.bump(streamID)
		// A newly created stream shares no history with any stream that previously
		// used this ID. Stale producer state would make a restarted producer's first
		// append look like a duplicate and silently drop its data (Section 5.2.1).
		h.producers.forget(streamID)
	} else {
		// Idempotent replay against a live stream: the window it was created with
		// is unchanged, but this PUT is still a write that reached the origin.
		if err := h.touch(r, streamID); err != nil {
			writeStorageError(w, err)
			return
		}
	}

	// CreateWithMessages already returned the exact tail. Empty creates use the
	// base Storage API, so read their (or an idempotent predecessor's) tail now.
	if len(initialMessages) == 0 {
		info, err := h.storage.Head(r.Context(), streamID)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		nextOffset = info.NextOffset
	}

	// Return success headers
	setSecurityHeaders(w)

	// Set Location header on 201 Created (Section 5.1)
	// Use r.RequestURI which preserves the original path (before StripPrefix etc.)
	// Location must be absolute URL per RFC 7231
	//
	// TRUST ASSUMPTION: the authority and scheme come from the client-supplied Host
	// header and X-Forwarded-Proto. That is correct behind a reverse proxy that
	// rewrites both, and forgeable by any client when the handler is exposed
	// directly. Deployments that treat Location as trusted must run behind a proxy
	// that overwrites these headers.
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

// readBoundedBody reads the request body, refusing to buffer more than
// maxAppendSize bytes. Both the declared Content-Length and the bytes actually
// delivered are checked: a chunked request declares no length, and a lying
// Content-Length must not be trusted either.
func (h *Handler) readBoundedBody(r *http.Request) ([]byte, *protoError) {
	tooLarge := newError(codePayloadTooLarge, fmt.Sprintf("request body exceeds maximum size of %d bytes", h.maxAppendSize))

	if r.ContentLength > h.maxAppendSize {
		return nil, tooLarge
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxAppendSize+1))
	if err != nil {
		return nil, newError(codeBadRequest, "failed to read request body")
	}
	if int64(len(body)) > h.maxAppendSize {
		return nil, tooLarge
	}
	return body, nil
}

// writeProducerRejection writes the response for a producer decision that is not
// producerAccept, per the response codes in Section 5.2.1. tailOffset is the stream's
// current tail, echoed on the idempotent-duplicate path.
func writeProducerRejection(w http.ResponseWriter, decision producerDecision, tailOffset Offset) {
	setSecurityHeaders(w)

	switch decision.outcome {
	case producerDuplicate:
		// Already accepted: idempotent success, no data written.
		w.Header().Set(protocol.HeaderStreamNextOffset, tailOffset.String())
		w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(decision.epoch, 10))
		w.Header().Set(protocol.HeaderProducerSeq, strconv.FormatInt(decision.seq, 10))
		w.WriteHeader(http.StatusNoContent)
	case producerStaleEpoch:
		// Zombie fencing: report the epoch that fenced this request.
		w.Header().Set(protocol.HeaderProducerEpoch, strconv.FormatInt(decision.epoch, 10))
		writeError(w, newError(codeForbidden, "stale producer epoch"))
	case producerEpochRestart:
		writeError(w, newError(codeBadRequest, "new epoch must start at seq=0"))
	case producerGap:
		w.Header().Set(protocol.HeaderProducerExpectedSeq, strconv.FormatInt(decision.expectedSeq, 10))
		w.Header().Set(protocol.HeaderProducerReceivedSeq, strconv.FormatInt(decision.receivedSeq, 10))
		writeError(w, newError(codeConflict, fmt.Sprintf("sequence gap: expected %d", decision.expectedSeq)))
	case producerAccept:
		// Not a rejection; callers must not reach this.
		writeError(w, newError(codeInternal, "internal error"))
	}
}

// handleAppend implements POST (Append) - Section 5.2
func (h *Handler) handleAppend(w http.ResponseWriter, r *http.Request, streamID string) {
	// Validate request-local metadata and buffer the bounded body before taking
	// the per-stream mutation lock. A client that uploads slowly must not block
	// DELETE, PUT, or another POST for the stream. Once the body is ready, the
	// metadata snapshot, producer decision, and append are serialized together.
	contentType := r.Header.Get("Content-Type")

	// Parse and validate producer headers (Section 5.2.1)
	producerID, producerEpoch, producerSeq, hasProducer, producerErr := h.parseProducerHeaders(r)
	producerKeyTooLarge := producerErr == nil && hasProducer && len(streamID) > maxProducerKeyBytes-len(producerID)

	// Get Stream-Seq if provided (separate from producer seq).
	seq := r.Header.Get(protocol.HeaderStreamSeq)

	body, protoErr := h.readBoundedBody(r)

	// Parse and flatten JSON before entering the mutation critical section so a
	// large request does not monopolize the stream lock. Defer reporting the
	// error until after Head/Touch below: an origin write still resets sliding
	// TTL even when the handler ultimately rejects its payload.
	var messages [][]byte
	var jsonErr error
	if protoErr == nil && len(body) > 0 {
		if protocol.IsJSONContentType(contentType) {
			messages, jsonErr = protocol.ProcessJSONAppend(body)
		} else {
			messages = [][]byte{body}
		}
	}

	// Keep the metadata snapshot, producer decision, and append bound to the
	// same stream incarnation. Storage methods are keyed only by stream ID, so
	// DELETE+PUT must not interleave this sequence.
	unlock := h.mutations.lock(streamID)
	defer unlock()

	// Get stream info to validate content type.
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// The stream exists and a write request has reached the origin, which is what
	// resets the window — including for an append this handler goes on to reject,
	// and for a POST that closes the stream without appending anything.
	if err := h.touch(r, streamID); err != nil {
		writeStorageError(w, err)
		return
	}

	if contentType == "" {
		writeError(w, newError(codeBadRequest, "Content-Type header required"))
		return
	}

	// Per Section 5.2, Content-Type must match the existing stream. A valid but
	// mismatched type receives 409 Conflict.
	if !protocol.ContentTypesMatch(contentType, info.ContentType) {
		writeError(w, newError(codeConflict, "content type mismatch"))
		return
	}
	if producerErr != nil {
		writeError(w, newError(codeBadRequest, producerErr.Error()))
		return
	}
	if producerKeyTooLarge {
		writeError(w, newError(codeBadRequest, "stream and producer IDs exceed maximum combined size"))
		return
	}
	if protoErr != nil {
		writeError(w, protoErr)
		return
	}
	if len(body) == 0 {
		writeError(w, newError(codeBadRequest, "empty body not allowed"))
		return
	}
	if jsonErr != nil {
		writeError(w, newError(codeBadRequest, jsonErr.Error()))
		return
	}

	batchStorage, hasAtomicBatch := h.storage.(AtomicBatchStorage)
	if len(messages) > 1 && !hasAtomicBatch {
		writeError(w, newError(codeNotImplemented, "storage does not support atomic multi-message appends"))
		return
	}

	// Per Section 5.2.1 "Concurrency Requirements", validation, append, and the state
	// update MUST be serialized per (stream, producerId). The entry lock acquired here
	// is therefore held for the remainder of the request: releasing it between
	// validation and commit lets a pipelined seq=N+1 validate against state that
	// seq=N has not yet committed, which reports a spurious sequence gap and wedges
	// the producer for good.
	var entry *producerEntry
	if hasProducer {
		entry = h.producers.acquire(producerKey{streamID: streamID, producerID: producerID})
		if entry == nil {
			writeError(w, newError(codeTooManyRequests, "producer state capacity reached"))
			return
		}
		defer h.producers.release(entry)

		decision := entry.validate(producerEpoch, producerSeq)
		if decision.outcome != producerAccept {
			tailOffset := info.NextOffset
			if decision.outcome == producerDuplicate {
				// The initial Head happened before waiting for this producer's
				// in-flight append. Refresh it so an idempotent retry cannot report
				// the pre-append tail.
				current, err := h.storage.Head(r.Context(), streamID)
				if err != nil {
					writeStorageError(w, err)
					return
				}
				tailOffset = current.NextOffset
			}
			writeProducerRejection(w, decision, tailOffset)
			return
		}
	}

	var nextOffset Offset

	if len(messages) > 1 {
		nextOffset, err = batchStorage.AppendBatch(r.Context(), streamID, messages, seq)
	} else {
		nextOffset, err = h.storage.Append(r.Context(), streamID, messages[0], seq)
	}
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Commit producer state after successful append
	if hasProducer {
		entry.commit(producerEpoch, producerSeq)
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
	if len(producerID) > maxProducerIDBytes {
		return "", 0, 0, false, fmt.Errorf("invalid Producer-Id: exceeds maximum size of %d bytes", maxProducerIDBytes)
	}

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

// handleRead implements GET (Read) - Sections 5.6, 5.7, 5.8
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
	if len(query[protocol.QueryCursor]) > 1 {
		writeError(w, newError(codeBadRequest, "duplicate cursor parameter"))
		return
	}

	// Parse and validate offset query parameter
	offsetStr := query.Get(protocol.QueryOffset)
	// Reject explicitly empty offset (?offset=) vs omitted offset
	if len(query[protocol.QueryOffset]) > 0 && offsetStr == "" {
		writeError(w, newError(codeBadRequest, "offset cannot be empty"))
		return
	}
	// Reject invalid offsets per protocol Section 8.
	if err := validateOffset(offsetStr); err != nil {
		writeError(w, newError(codeBadRequest, err.Error()))
		return
	}
	offset := Offset(offsetStr)

	// Cursors generated by this server are bounded decimal interval numbers.
	// Silently ignoring an oversized echoed cursor could return a smaller value
	// and violate the monotonicity guarantee in Section 10.1, so reject it.
	if cursor := query.Get(protocol.QueryCursor); cursor != "" && !protocol.ValidCursor(cursor) {
		writeError(w, newError(codeBadRequest, "invalid cursor"))
		return
	}

	// Route based on live query parameter
	liveMode := query.Get(protocol.QueryLive)

	switch liveMode {
	case "":
		// Catch-up read (Section 5.6)
		h.handleCatchupRead(w, r, streamID, offset)
	case protocol.LiveModeLongPoll:
		// Long-poll read (Section 5.7)
		h.handleLongPoll(w, r, streamID, offset)
	case protocol.LiveModeSSE:
		// SSE streaming (Section 5.8)
		h.handleSSE(w, r, streamID, offset)
	default:
		writeError(w, newError(codeBadRequest, "invalid live parameter"))
	}
}

// handleCatchupRead implements catch-up reads (Section 5.6)
func (h *Handler) handleCatchupRead(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Bind the metadata (especially Content-Type), TTL renewal, and bytes to one
	// incarnation. This is a short catch-up read, so holding the mutation lock does
	// not prevent a writer needed to complete the request.
	unlock := h.mutations.lock(streamID)
	locked := true
	releaseMutation := func() {
		if locked {
			unlock()
			locked = false
		}
	}
	defer releaseMutation()

	// Get stream info for content type
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// A catch-up read that reaches the origin resets the window; one served from
	// a CDN never gets here, and per Section 5.1 must not reset it.
	if err := h.touch(r, streamID); err != nil {
		writeStorageError(w, err)
		return
	}
	// Get cursor parameter for CDN collapsing (Section 10.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)

	// Handle offset=now sentinel (Section 8)
	// Returns empty response with current tail offset
	if offset == Offset(protocol.OffsetNow) {
		releaseMutation()
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
	if !incarnationMatches(info.IncarnationID, result.IncarnationID) {
		writeStorageError(w, ErrNotFound)
		return
	}
	releaseMutation()

	// Incarnation identity keeps a validator for a deleted stream from matching a
	// replacement that happens to have the same offsets. Custom Storage
	// implementations may omit identity; in that case no safe validator exists,
	// so omit ETag and ignore If-None-Match rather than risk a false 304.
	etag := makeETag(info.IncarnationID, offset, result.NextOffset)

	// Check If-None-Match for 304 Not Modified (Section 10.1)
	// Per spec: "When a client provides a valid If-None-Match header that matches
	// the current ETag, servers MUST respond with 304 Not Modified"
	if ifNoneMatch := r.Header.Get("If-None-Match"); etag != "" && ifNoneMatch != "" {
		if etagMatches(ifNoneMatch, etag) {
			setSecurityHeaders(w)
			w.Header().Set("ETag", etag)
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
			w.Header().Set("Cache-Control", cacheControlHeader(info.IsPrivate))
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}

	// Set headers
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
	w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))

	// Set Cache-Control (Section 10.1)
	w.Header().Set("Cache-Control", cacheControlHeader(info.IsPrivate))

	if etag != "" {
		w.Header().Set("ETag", etag)
	}

	// Set Stream-Up-To-Date if at tail (Section 5.6)
	if result.NextOffset.Compare(result.TailOffset) == 0 {
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
	}

	// Format response based on content type
	responseBody := formatResponseBody(result.Messages, info.ContentType)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

// handleLongPoll implements long-poll reads (Section 5.7)
func (h *Handler) handleLongPoll(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Offset is required for long-poll
	if offset.IsZero() {
		writeError(w, newError(codeBadRequest, "offset required for long-poll"))
		return
	}

	// Get cursor parameter for CDN collapsing (Section 10.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)
	// Snapshot metadata and perform the initial read under the lifecycle lock.
	// Before blocking, pin that incarnation and release the lock so POST can
	// append the data this request is waiting for.
	unlock := h.mutations.lock(streamID)
	locked := true
	releaseMutation := func() {
		if locked {
			unlock()
			locked = false
		}
	}
	defer releaseMutation()

	// Fetch stream info once at the start - needed for offset=now and ContentType/IsPrivate
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Reset the window now that processing has begun, so a poll that waits and
	// returns no data still counts as activity (Section 5.1).
	if err := h.touch(r, streamID); err != nil {
		writeStorageError(w, err)
		return
	}
	timeoutOffset := info.NextOffset

	// Handle offset=now sentinel (Section 8)
	// For long-poll, immediately begin waiting (no initial empty response)
	isOffsetNow := offset == Offset(protocol.OffsetNow)
	if isOffsetNow {
		offset = info.NextOffset
	}

	// Try immediate read first (skip for offset=now per spec)
	if !isOffsetNow {
		result, err := h.storage.Read(r.Context(), streamID, offset, h.chunkSize)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		if !incarnationMatches(info.IncarnationID, result.IncarnationID) {
			writeStorageError(w, ErrNotFound)
			return
		}
		timeoutOffset = result.TailOffset

		// If messages available, return immediately
		if len(result.Messages) > 0 {
			releaseMutation()
			setSecurityHeaders(w)
			w.Header().Set("Content-Type", info.ContentType)
			w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
			w.Header().Set("Cache-Control", cacheControlHeader(info.IsPrivate))

			// Set Stream-Up-To-Date if at tail (per spec Section 5.7)
			if result.NextOffset.Compare(result.TailOffset) == 0 {
				w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			}

			responseBody := formatResponseBody(result.Messages, info.ContentType)

			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(responseBody)
			return
		}
	}

	epoch := h.mutations.pin(streamID)
	releaseMutation()
	defer epoch.release()
	liveCtx, cancelLive := epoch.context(r.Context())
	defer cancelLive()

	// No data available, wait for data to arrive
	// Use the shorter of the request context deadline, longPollTimeout, or half
	// the sliding TTL. A live reader must not remain blocked until Storage expires
	// the stream underneath it. Returning 204 early lets the client reconnect and
	// renew the next window while leaving ample scheduling margin.
	waitCtx := liveCtx
	deadline, hasDeadline := liveCtx.Deadline()
	timeout := h.longPollTimeout
	if info.TTL > 0 {
		ttlWake := info.TTL / 2
		if ttlWake <= 0 {
			ttlWake = info.TTL
		}
		if ttlWake < timeout {
			timeout = ttlWake
		}
	}

	if hasDeadline {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}

	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(liveCtx, timeout)
		defer cancel()
	}

	// Wait for data atomically
	result, err := h.storage.WaitForData(waitCtx, streamID, offset, h.chunkSize)
	if err != nil {
		if epoch.invalidated() {
			writeStorageError(w, ErrNotFound)
			return
		}
		// Timeout - return 204 No Content (per spec Section 5.7)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// A client-constructed future offset can remain ahead of newly appended
			// data, so WaitForData may wake and continue waiting while the tail moves.
			// Refresh after our own timeout to report the actual current tail. When
			// the request context itself is canceled, no further storage operation is
			// possible (and the client cannot receive the response), so retain the
			// last known tail.
			if r.Context().Err() == nil {
				refreshUnlock := h.mutations.lock(streamID)
				if epoch.invalidated() {
					refreshUnlock()
					writeStorageError(w, ErrNotFound)
					return
				}
				current, headErr := h.storage.Head(r.Context(), streamID)
				refreshUnlock()
				if headErr != nil {
					writeStorageError(w, headErr)
					return
				}
				if !incarnationMatches(info.IncarnationID, current.IncarnationID) {
					writeStorageError(w, ErrNotFound)
					return
				}
				timeoutOffset = current.NextOffset
			}
			setSecurityHeaders(w)
			// A timeout reports the current tail, never the request's -1/now
			// sentinel or a client-constructed future offset (Sections 5.7 and 8).
			w.Header().Set(protocol.HeaderStreamNextOffset, timeoutOffset.String())
			w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
			w.Header().Set(protocol.HeaderStreamUpToDate, "true")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Other error (e.g., stream deleted/not found)
		writeStorageError(w, err)
		return
	}
	if epoch.invalidated() {
		writeStorageError(w, ErrNotFound)
		return
	}
	if !incarnationMatches(info.IncarnationID, result.IncarnationID) {
		writeStorageError(w, ErrNotFound)
		return
	}

	setSecurityHeaders(w)
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set(protocol.HeaderStreamNextOffset, result.NextOffset.String())
	w.Header().Set(protocol.HeaderStreamCursor, protocol.GenerateCursor(clientCursor))
	w.Header().Set("Cache-Control", cacheControlHeader(info.IsPrivate))

	// Set Stream-Up-To-Date if at tail (per spec Section 5.7)
	if result.NextOffset.Compare(result.TailOffset) == 0 {
		w.Header().Set(protocol.HeaderStreamUpToDate, "true")
	}

	responseBody := formatResponseBody(result.Messages, info.ContentType)

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(responseBody)
}

// handleSSE implements SSE streaming (Section 5.8)
func (h *Handler) handleSSE(w http.ResponseWriter, r *http.Request, streamID string, offset Offset) {
	// Offset is required for SSE
	if offset.IsZero() {
		writeError(w, newError(codeBadRequest, "offset required for SSE"))
		return
	}

	// Get cursor parameter for CDN collapsing (Section 10.1)
	clientCursor := r.URL.Query().Get(protocol.QueryCursor)
	// Capture metadata and validate the initial offset against one incarnation.
	// The epoch token keeps that identity after the mutation lock is released,
	// allowing appends while ensuring a DELETE+PUT replacement terminates rather
	// than being decoded with the predecessor's Content-Type.
	unlock := h.mutations.lock(streamID)
	locked := true
	releaseMutation := func() {
		if locked {
			unlock()
			locked = false
		}
	}
	defer releaseMutation()

	// Get stream info
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// SSE serves every content type (Section 5.8). text/* and application/json are
	// carried as UTF-8 text; anything else is base64-encoded and announced with the
	// Stream-SSE-Data-Encoding response header so clients know to decode it.
	base64Data := protocol.SSERequiresBase64(info.ContentType)

	// Renew before committing the streaming response. Once the headers have been
	// flushed, a Touch failure can only be represented by terminating the stream.
	if err := h.touch(r, streamID); err != nil {
		writeStorageError(w, err)
		return
	}

	// Handle offset=now sentinel (Section 8)
	// Start from tail position, sending initial control event with upToDate
	isOffsetNow := offset == Offset(protocol.OffsetNow)
	if isOffsetNow {
		// Replace "now" with actual tail offset
		offset = info.NextOffset
	}

	// Let Storage validate its opaque offset before the 200 response is flushed.
	// Generic URL validation cannot know a backend's offset syntax; without this
	// read, values such as "bogus" produced a successful but immediately closed
	// SSE stream instead of a 400 response.
	var pendingResult *ReadResult
	if !isOffsetNow {
		pendingResult, err = h.storage.Read(r.Context(), streamID, offset, h.chunkSize)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		if !incarnationMatches(info.IncarnationID, pendingResult.IncarnationID) {
			writeStorageError(w, ErrNotFound)
			return
		}
	}
	epoch := h.mutations.pin(streamID)
	releaseMutation()
	defer epoch.release()
	liveCtx, cancelLive := epoch.context(r.Context())
	defer cancelLive()
	if epoch.invalidated() {
		writeStorageError(w, ErrNotFound)
		return
	}

	// Set SSE headers
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if base64Data {
		w.Header().Set(protocol.HeaderStreamSSEDataEncoding, protocol.SSEDataEncodingBase64)
	}

	// Get flusher for streaming
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, newError(codeInternal, "streaming not supported"))
		return
	}

	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Set close deadline and timer
	closeDeadline := time.Now().Add(h.sseCloseAfter)
	closeTimer := time.NewTimer(h.sseCloseAfter)
	defer closeTimer.Stop()

	currentOffset := offset

	// For offset=now, send initial control event with upToDate: true (per spec Section 8)
	if isOffsetNow {
		writeSSEControlEvent(w, currentOffset.String(), true, protocol.GenerateCursor(clientCursor))
		flusher.Flush()
	}

	// Track whether we've sent the initial control event for non-offset=now cases.
	// Per spec, when client is caught up (at tail), we should send a control event
	// with upToDate: true even if there's no data.
	sentInitialControl := isOffsetNow

	// Keepalive interval for sending control events to prevent proxy timeouts
	const keepaliveInterval = 30 * time.Second

	// Stream loop
	for {
		// Check for close/disconnect before blocking
		select {
		case <-closeTimer.C:
			return
		case <-liveCtx.Done():
			return
		default:
		}

		var result *ReadResult
		if pendingResult != nil {
			result = pendingResult
			pendingResult = nil
		} else {
			// An SSE connection can outlive several TTL windows, so it resets the
			// countdown on every pass rather than only when processing began: a
			// stream with a live reader attached must not expire underneath it
			// (Section 5.1). The response is already committed, so failure closes it.
			if err := h.storage.Touch(liveCtx, streamID); err != nil {
				return
			}

			// Try to read data.
			result, err = h.storage.Read(liveCtx, streamID, currentOffset, h.chunkSize)
			if err != nil {
				return
			}
		}
		if epoch.invalidated() {
			return
		}
		if !incarnationMatches(info.IncarnationID, result.IncarnationID) {
			return
		}

		// If no messages, wait for data or send keepalive
		if len(result.Messages) == 0 {
			// Send initial control if not yet sent (empty stream or starting at tail)
			if !sentInitialControl {
				if currentOffset.Compare(result.TailOffset) == 0 || result.NextOffset.Compare(result.TailOffset) == 0 {
					sentInitialControl = true
					writeSSEControlEvent(w, result.NextOffset.String(), true, protocol.GenerateCursor(clientCursor))
					flusher.Flush()
				}
			}

			// Calculate wait timeout: min of keepalive interval and remaining close time
			waitTimeout := keepaliveInterval
			// A stream whose TTL is shorter than the keepalive interval would
			// expire mid-wait, ending a connection the spec says must keep the
			// stream alive. Wake early enough to touch it again; the cost is a
			// keepalive comment per window, which SSE clients ignore.
			if info.TTL > 0 && info.TTL/2 < waitTimeout {
				waitTimeout = info.TTL / 2
			}
			if remaining := time.Until(closeDeadline); remaining < waitTimeout {
				waitTimeout = remaining
				if waitTimeout <= 0 {
					// Close deadline reached
					return
				}
			}

			// Wait for new data - storage handles efficient blocking
			ctx, cancel := context.WithTimeout(liveCtx, waitTimeout)
			waitResult, err := h.storage.WaitForData(ctx, streamID, currentOffset, h.chunkSize)
			cancel()

			if err != nil {
				if epoch.invalidated() {
					return
				}
				// Check if parent context is done (client disconnect)
				if r.Context().Err() != nil {
					return
				}
				if errors.Is(err, context.DeadlineExceeded) {
					// Our wait timeout expired - send SSE comment as keepalive to prevent proxy timeout
					// Per SSE spec, lines starting with ":" are comments and ignored by clients
					fmt.Fprintf(w, ": keepalive\n\n")
					flusher.Flush()
					continue
				}
				if errors.Is(err, context.Canceled) {
					return
				}
				// Stream deleted or other error
				return
			}
			if epoch.invalidated() {
				return
			}
			if !incarnationMatches(info.IncarnationID, waitResult.IncarnationID) {
				return
			}

			// Reuse waitResult directly instead of re-reading
			result = waitResult
		}

		// If we have messages, send them
		if len(result.Messages) > 0 {
			sentInitialControl = true

			// Send data event
			fmt.Fprintf(w, "event: data\n")

			switch {
			case base64Data:
				// Binary stream: standard base64 (RFC 4648) on a single data line.
				// Section 5.8 permits splitting across lines; one line keeps the
				// framing simple and is what clients must handle either way.
				encoded := base64.StdEncoding.EncodeToString(concatenateMessages(result.Messages))
				writeSSEDataLine(w, encoded)
			case protocol.IsJSONContentType(info.ContentType):
				jsonArray := formatResponseBody(result.Messages, info.ContentType)
				// JSON permits insignificant CR/LF between tokens. Every physical
				// SSE line still needs its own data: prefix or a conforming client
				// drops the unprefixed fragments and receives invalid JSON.
				for _, line := range splitBySSELineTerminators(string(jsonArray)) {
					writeSSEDataLine(w, line)
				}
			default:
				// For text/*, send concatenated data split by lines.
				// Per SSE spec, lines can be terminated by CR, LF, or CRLF.
				// We must split by all terminators to prevent CRLF injection attacks.
				data := concatenateMessages(result.Messages)
				lines := splitBySSELineTerminators(string(data))
				for _, line := range lines {
					writeSSEDataLine(w, line)
				}
			}
			fmt.Fprintf(w, "\n")

			// Send control event with cursor (Section 5.8: streamCursor is required
			// while the stream is open).
			upToDate := result.NextOffset.Compare(result.TailOffset) == 0
			writeSSEControlEvent(w, result.NextOffset.String(), upToDate, protocol.GenerateCursor(clientCursor))

			flusher.Flush()
			currentOffset = result.NextOffset
		}
	}
}

// handleHead implements HEAD (Metadata) - Section 5.5
// Response codes: 200 OK, 404 Not Found, 429 Too Many Requests (Section 5.5)
func (h *Handler) handleHead(w http.ResponseWriter, r *http.Request, streamID string) {
	info, err := h.storage.Head(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}

	// Set response headers per Section 5.5
	setSecurityHeaders(w)
	w.Header().Set("Content-Type", info.ContentType)                          // Per spec: stream's content type
	w.Header().Set(protocol.HeaderStreamNextOffset, info.NextOffset.String()) // Per spec: current tail offset

	// Set TTL/Expires-At if present (Section 5.5)
	if info.TTL > 0 {
		w.Header().Set(protocol.HeaderStreamTTL, strconv.FormatInt(int64(info.TTL.Seconds()), 10))
	}
	if !info.ExpiresAt.IsZero() {
		w.Header().Set(protocol.HeaderStreamExpiresAt, info.ExpiresAt.Format(time.RFC3339Nano))
	}

	// Set Cache-Control (Section 5.5)
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(http.StatusOK)
}

// handleDelete implements DELETE (Delete) - Section 5.4
// Response codes: 204 No Content (success), 404 Not Found, 429 Too Many Requests (Section 5.4)
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, streamID string) {
	unlock := h.mutations.lock(streamID)
	defer unlock()

	err := h.storage.Delete(r.Context(), streamID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	// Wake live reads of the deleted incarnation before a later PUT can reuse the
	// same stream ID.
	h.mutations.bump(streamID)

	// Producer state describes data that no longer exists. Keeping it would make the
	// first append of a producer restarted against a recreated stream look like a
	// duplicate (204) and silently discard the data (Section 5.2.1).
	h.producers.forget(streamID)

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

// sseControlEvent is the payload of an SSE control event (Section 5.8). Field
// names are camelCase and the order below is the order they are serialized in.
type sseControlEvent struct {
	StreamNextOffset string `json:"streamNextOffset"`
	UpToDate         bool   `json:"upToDate,omitempty"`
	// StreamCursor is required while the stream is open and may only be omitted
	// once the stream is closed, so it is empty only in that case.
	StreamCursor string `json:"streamCursor,omitempty"`
}

// writeSSEControlEvent writes a complete SSE control event, including the
// blank line that terminates it. Callers flush afterwards.
func writeSSEControlEvent(w io.Writer, nextOffset string, upToDate bool, cursor string) {
	payload, err := json.Marshal(sseControlEvent{
		StreamNextOffset: nextOffset,
		UpToDate:         upToDate,
		StreamCursor:     cursor,
	})
	if err != nil {
		// The struct holds only strings and a bool, so marshaling cannot fail;
		// dropping the event is better than emitting a malformed one.
		return
	}
	_, _ = fmt.Fprint(w, "event: control\n")
	writeSSEDataLine(w, string(payload))
	_, _ = fmt.Fprint(w, "\n")
}

// writeSSEDataLine writes a single SSE "data:" field holding value, which must
// not contain a line terminator (callers split payloads with
// splitBySSELineTerminators first).
//
// The value follows the colon with no separating space. The SSE parsing rules
// strip at most one leading U+0020 from a data field, so a space is only
// written when value itself starts with one — that way the byte a conforming
// parser strips is always framing, never payload, and parsers that do not strip
// still see the exact payload.
func writeSSEDataLine(w io.Writer, value string) {
	if strings.HasPrefix(value, " ") {
		_, _ = fmt.Fprintf(w, "data: %s\n", value)
		return
	}
	_, _ = fmt.Fprintf(w, "data:%s\n", value)
}

// setSecurityHeaders adds browser security headers to the response.
// Per protocol Section 12.7, these headers prevent MIME-sniffing attacks
// and cross-origin embedding exploits.
func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set(headerXContentTypeOptions, "nosniff")
	w.Header().Set(headerCrossOriginResourcePolicy, "cross-origin")
}

// setCORSHeaders makes the stream API usable from a browser on any origin.
//
// The allow-origin value is the wildcard rather than an echo of the request's
// Origin: streams are served with cacheable Cache-Control values, and echoing
// would make the response vary by origin in a way shared caches key on. The
// wildcard also means credentials (cookies) are never sent — this handler
// expects bearer credentials in the Authorization header instead.
//
// CORS is opt-in because allowing mutation methods from any website is unsafe
// for unauthenticated or network-local deployments. Deployments that need a
// narrower policy should leave HandlerConfig.EnableCORS false and wrap the
// handler with middleware that owns the complete CORS policy.
func (h *Handler) setCORSHeaders(w http.ResponseWriter) {
	if !h.enableCORS {
		return
	}
	headers := w.Header()
	headers.Set(headerAccessControlAllowOrigin, "*")
	headers.Set(headerAccessControlAllowMethods, corsAllowMethods)
	headers.Set(headerAccessControlAllowHeaders, corsAllowHeaders)
	headers.Set(headerAccessControlExposeHeaders, corsExposeHeaders)
	headers.Set(headerAccessControlMaxAge, corsMaxAgeSeconds)
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
	// errors.As, not a bare type assertion: storage backends wrap the protocol error
	// they return, and a wrapped one must still map to its own status.
	var protoErr *protoError
	if errors.As(err, &protoErr) {
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
	case errors.Is(err, ErrPayloadTooLarge):
		writeError(w, newError(codePayloadTooLarge, err.Error()))
	case errors.Is(err, ErrClosed):
		// The storage backend is shutting down or already closed: the request may
		// succeed against another instance or after a restart, so it is retryable.
		writeError(w, newError(codeServiceUnavailable, "storage unavailable"))
	default:
		// Unclassified errors carry internal detail (file paths, driver messages)
		// that must not reach a client, so the body is generic. There is no logger
		// on Handler to record the detail; adding one is a separate change.
		writeError(w, newError(codeInternal, "internal error"))
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

// cacheControlHeader returns the appropriate Cache-Control header value based on stream privacy.
// Per PROTOCOL.md Section 10.1:
//   - Shared, non-user-specific streams: "public, max-age=60, stale-while-revalidate=300"
//   - User-specific or confidential streams: "private, max-age=60, stale-while-revalidate=300"
func cacheControlHeader(isPrivate bool) string {
	if isPrivate {
		return "private, max-age=60, stale-while-revalidate=300"
	}
	return "public, max-age=60, stale-while-revalidate=300"
}

// incarnationMatches binds a Read result to the metadata snapshot used to
// interpret it. Exact equality deliberately treats a Storage that populates
// only one side as inconsistent; two empty values preserve compatibility with
// implementations that do not expose optional incarnation identity.
func incarnationMatches(infoID, resultID string) bool {
	return infoID == resultID
}

// makeETag returns an opaque, header-safe validator for one immutable range of
// one stream incarnation. Length prefixes avoid ambiguity even though
// incarnation IDs are arbitrary bytes. An empty incarnation ID cannot safely
// distinguish delete/recreate cycles, so it produces no validator.
func makeETag(incarnationID string, start, end Offset) string {
	if incarnationID == "" {
		return ""
	}

	digest := sha256.New()
	var size [8]byte
	writePart := func(part string) {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = digest.Write(size[:])
		_, _ = io.WriteString(digest, part)
	}
	writePart(incarnationID)
	writePart(start.String())
	writePart(end.String())

	return fmt.Sprintf(`"%x"`, digest.Sum(nil))
}

// reservedPathSegment is the path prefix reserved for Durable Streams control APIs
// (spec Section 6: subscriptions live at {stream-url}/__ds/subscriptions/:id).
const reservedPathSegment = "__ds"

// hasReservedSegment reports whether any segment of a stream path is the reserved
// "__ds" segment. The spec reserves the first stream-root-relative segment, but the
// stream root is defined by the (configurable) PathExtractor and is not visible here,
// so every segment is checked. This is conservative: it rejects a few paths the spec
// would allow, and never accepts one it reserves.
func hasReservedSegment(streamID string) bool {
	for _, segment := range strings.Split(streamID, "/") {
		if segment == reservedPathSegment {
			return true
		}
	}
	return false
}

// validateOffset validates an offset string per protocol Sections 8 and 12.2.
// Returns an error if the offset contains invalid characters or patterns.
//
// Per Section 8: Offsets MUST NOT contain commas, ampersands, equals signs,
// question marks, or slashes (to avoid conflict with URL query parameter and
// path syntax).
//
// Per Section 12.2: Servers SHOULD validate and sanitize to prevent path
// traversal attacks (patterns like "..").
func validateOffset(offset string) error {
	// Empty offset is valid (equivalent to stream start)
	if offset == "" {
		return nil
	}

	// Per Section 8: MUST NOT contain these URL query parameter conflict characters
	if strings.ContainsAny(offset, ",&=?/") {
		return errors.New("invalid offset format")
	}

	// Reject whitespace and control characters (common validation)
	if strings.ContainsAny(offset, " \t\n\r") {
		return errors.New("invalid offset format")
	}

	// Per Section 12.2: prevent path traversal attacks
	// Check for ".." anywhere in the offset
	if strings.Contains(offset, "..") {
		return errors.New("invalid offset format")
	}

	return nil
}
