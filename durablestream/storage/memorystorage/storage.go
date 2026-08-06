// Package memorystorage provides an in-memory implementation of
// durablestream.Storage.
//
// Offsets use the reference implementation's lexicographically sortable
// "<readSeq>_<position>" representation. This backend uses readSeq zero and a
// logical message position for the second component.
package memorystorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/internal/protocol"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/go4org/hashtriemap"
)

// memoryMessage is data owned by exactly one stream. Forks normally retain a
// parent pointer rather than copying these bytes. batch identifies the atomic
// append that produced a group of flattened JSON messages.
type memoryMessage struct {
	data   []byte
	offset durablestream.Offset
	index  int64
	batch  uint64
}

// forkMetadata is immutable after creation and records the request shape used
// for idempotency separately from the resolved, internal fork boundary.
type forkMetadata struct {
	sourceID            string
	sourceIncarnationID string
	requestedOffset     durablestream.Offset
	offsetSet           bool
	subOffset           uint64
	contentTypeSet      bool
	ttlSet              bool
	expiresAtSet        bool
	requestedTTL        time.Duration
	requestedExpiresAt  time.Time
	isPrivate           bool
	closed              bool
}

// memoryStream represents one stream incarnation. parent and refCount are
// protected by Storage.topologyMu; every other mutable field is protected by
// mu. A deleted stream can remain reachable through parent pointers after
// expiry, while a softDeleted stream remains in the path map to block reuse.
type memoryStream struct {
	mu sync.RWMutex

	config        durablestream.StreamConfig
	messages      []memoryMessage // target-owned suffix only
	tailIndex     int64
	lastSeq       string
	lastSeqOffset durablestream.Offset
	nextBatch     uint64
	notifyCh      chan struct{}

	deleted     bool
	softDeleted bool
	collected   bool // protected by topologyMu
	streamID    string
	incID       string

	parent              *memoryStream
	parentBoundaryIndex int64
	fork                *forkMetadata
	refCount            uint64 // direct children; protected by topologyMu
}

// Storage is an in-memory implementation of durablestream.Storage. The
// topology lock makes path replacement, fork reference counts, and cascading
// reclamation one atomic graph operation. Per-stream locks still serialize
// reads and appends without holding the topology exclusively.
type Storage struct {
	streams hashtriemap.HashTrieMap[string, *memoryStream]

	topologyMu sync.RWMutex
	initOnce   sync.Once
	closeOnce  sync.Once
	closedCh   chan struct{}
}

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
	_ durablestream.AtomicCloseStorage = (*Storage)(nil)
	_ durablestream.ForkStorage        = (*Storage)(nil)
)

// errClosed reports that the storage was closed while a caller was waiting.
var errClosed = fmt.Errorf("memorystorage: storage closed: %w", durablestream.ErrClosed)

func newIncarnationID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("memorystorage: generate incarnation ID: %w", err)
	}
	return hex.EncodeToString(id[:]), nil
}

// stopTimer releases a deadline timer when another wakeup wins the select.
func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// New creates a new in-memory storage instance.
func New() *Storage { return &Storage{} }

// closed returns the channel that Close closes. Lazy initialization keeps the
// zero value of Storage usable.
func (m *Storage) closed() <-chan struct{} {
	m.initOnce.Do(func() { m.closedCh = make(chan struct{}) })
	return m.closedCh
}

func wakeLocked(stream *memoryStream) {
	close(stream.notifyCh)
	stream.notifyCh = make(chan struct{})
}

func tailOffset(stream *memoryStream) durablestream.Offset {
	return storage.FormatSimpleOffset(stream.tailIndex)
}

func streamInfoLocked(stream *memoryStream) *durablestream.StreamInfo {
	return &durablestream.StreamInfo{
		ContentType:   stream.config.ContentType,
		NextOffset:    tailOffset(stream),
		LastSeq:       stream.lastSeq,
		TTL:           stream.config.TTL,
		ExpiresAt:     stream.config.ExpiresAt,
		IsPrivate:     stream.config.IsPrivate,
		Closed:        stream.config.Closed,
		IncarnationID: stream.incID,
	}
}

func directStateErrorLocked(stream *memoryStream) error {
	if stream.softDeleted {
		return durablestream.ErrSoftDeleted
	}
	if stream.deleted || stream.config.IsExpired() {
		return durablestream.ErrNotFound
	}
	return nil
}

// collectLocked physically reclaims a node whose last direct child is gone and
// recursively releases its parent. topologyMu must be held for writing.
func (m *Storage) collectLocked(stream *memoryStream) {
	if stream == nil || stream.collected || stream.refCount != 0 {
		return
	}

	stream.collected = true
	m.streams.CompareAndDelete(stream.streamID, stream)
	stream.mu.Lock()
	if !stream.deleted {
		stream.deleted = true
		wakeLocked(stream)
	}
	stream.mu.Unlock()

	parent := stream.parent
	stream.parent = nil
	if parent == nil {
		return
	}
	if parent.refCount > 0 {
		parent.refCount--
	}
	parent.mu.RLock()
	parentRetired := parent.deleted || parent.softDeleted
	parent.mu.RUnlock()
	if parent.refCount == 0 && parentRetired {
		m.collectLocked(parent)
	}
}

// retireExpiredLocked removes an expired incarnation from its public path.
// Referenced data remains reachable by child pointers until cascading GC can
// reclaim it. topologyMu must be held for writing.
func (m *Storage) retireExpiredLocked(streamID string, stream *memoryStream) bool {
	current, ok := m.streams.Load(streamID)
	if !ok || current != stream {
		return false
	}

	stream.mu.Lock()
	if stream.softDeleted || stream.deleted || !stream.config.IsExpired() {
		stream.mu.Unlock()
		return false
	}
	m.streams.CompareAndDelete(streamID, stream)
	stream.deleted = true
	wakeLocked(stream)
	stream.mu.Unlock()

	if stream.refCount == 0 {
		m.collectLocked(stream)
	}
	return true
}

// cloneBatch validates and copies borrowed input. Empty batches are valid for
// create and close operations, but never for AppendBatch.
func cloneBatch(messages [][]byte, allowEmptyBatch bool) ([][]byte, error) {
	if len(messages) == 0 && !allowEmptyBatch {
		return nil, fmt.Errorf("empty append batch not allowed: %w", durablestream.ErrBadRequest)
	}
	cloned := make([][]byte, len(messages))
	for i, message := range messages {
		if len(message) == 0 {
			return nil, fmt.Errorf("empty message at batch index %d: %w", i, durablestream.ErrBadRequest)
		}
		cloned[i] = bytes.Clone(message)
	}
	return cloned, nil
}

func newStream(streamID string, cfg durablestream.StreamConfig, messages [][]byte, incID string) *memoryStream {
	stream := &memoryStream{
		config:    cfg,
		messages:  make([]memoryMessage, 0, len(messages)),
		notifyCh:  make(chan struct{}),
		streamID:  streamID,
		incID:     incID,
		nextBatch: 1,
	}
	if len(messages) != 0 {
		batch := stream.nextBatch
		stream.nextBatch++
		for _, data := range messages {
			stream.tailIndex++
			stream.messages = append(stream.messages, memoryMessage{
				data:   data,
				offset: storage.FormatSimpleOffset(stream.tailIndex),
				index:  stream.tailIndex,
				batch:  batch,
			})
		}
	}
	return stream
}

// Create creates a new regular stream.
func (m *Storage) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	created, _, err := m.CreateWithMessages(ctx, streamID, cfg, nil)
	return created, err
}

// CreateWithMessages creates a regular stream and its initial batch atomically.
func (m *Storage) CreateWithMessages(ctx context.Context, streamID string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	cloned, err := cloneBatch(messages, true)
	if err != nil {
		return false, "", err
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	incID, err := newIncarnationID()
	if err != nil {
		return false, "", err
	}
	candidate := newStream(streamID, cfg, cloned, incID)

	m.topologyMu.Lock()
	defer m.topologyMu.Unlock()
	for {
		existing, ok := m.streams.Load(streamID)
		if !ok {
			m.streams.Store(streamID, candidate)
			return true, tailOffset(candidate), nil
		}

		existing.mu.RLock()
		softDeleted := existing.softDeleted
		deleted := existing.deleted
		expired := existing.config.IsExpired()
		isFork := existing.fork != nil
		existingCfg := existing.config
		existingTail := tailOffset(existing)
		existing.mu.RUnlock()

		if softDeleted {
			return false, "", fmt.Errorf("stream path is retained by active forks: %w", durablestream.ErrConflict)
		}
		if deleted {
			m.streams.CompareAndDelete(streamID, existing)
			if existing.refCount == 0 {
				m.collectLocked(existing)
			}
			continue
		}
		if expired {
			m.retireExpiredLocked(streamID, existing)
			continue
		}
		if isFork || !existingCfg.Matches(cfg) {
			return false, "", fmt.Errorf("stream exists with different config: %w", durablestream.ErrConflict)
		}
		return false, existingTail, nil
	}
}

func contentTypesMatch(a, b string) bool {
	return (durablestream.StreamConfig{ContentType: a}).Matches(durablestream.StreamConfig{ContentType: b})
}

func normalizeForkOffset(offset durablestream.Offset) (durablestream.Offset, int64, error) {
	readSeq, index, err := storage.ParseOffset(offset)
	if err != nil {
		return "", 0, err
	}
	if readSeq != 0 {
		return "", 0, fmt.Errorf("memorystorage: fork offset belongs to an unsupported offset sequence: %w", durablestream.ErrBadRequest)
	}
	return storage.FormatSimpleOffset(index), index, nil
}

func resolveForkConfig(req durablestream.ForkRequest, source durablestream.StreamConfig, now time.Time) (durablestream.StreamConfig, error) {
	if req.TTLSet && req.ExpiresAtSet {
		return durablestream.StreamConfig{}, fmt.Errorf("fork TTL and absolute expiry are mutually exclusive: %w", durablestream.ErrBadRequest)
	}
	if req.ContentTypeSet && req.Config.ContentType == "" {
		return durablestream.StreamConfig{}, fmt.Errorf("explicit fork content type is empty: %w", durablestream.ErrBadRequest)
	}
	if req.Config.ContentType != "" && !contentTypesMatch(req.Config.ContentType, source.ContentType) {
		return durablestream.StreamConfig{}, fmt.Errorf("fork content type does not match source: %w", durablestream.ErrConflict)
	}

	cfg := durablestream.StreamConfig{
		ContentType: source.ContentType,
		IsPrivate:   req.Config.IsPrivate,
		Closed:      req.Config.Closed,
	}
	switch {
	case req.TTLSet:
		if req.Config.TTL < 0 {
			return durablestream.StreamConfig{}, fmt.Errorf("fork TTL cannot be negative: %w", durablestream.ErrBadRequest)
		}
		cfg.TTL = req.Config.TTL
		if cfg.TTL > 0 {
			cfg.ExpiresAt = now.Add(cfg.TTL)
		}
	case req.ExpiresAtSet:
		if req.Config.ExpiresAt.IsZero() {
			return durablestream.StreamConfig{}, fmt.Errorf("explicit fork expiry is empty: %w", durablestream.ErrBadRequest)
		}
		cfg.ExpiresAt = req.Config.ExpiresAt
	case source.TTL > 0:
		cfg.TTL = source.TTL
		cfg.ExpiresAt = now.Add(source.TTL)
	default:
		cfg.ExpiresAt = source.ExpiresAt
	}
	return cfg, nil
}

func forkRequestMatches(stream *memoryStream, req durablestream.ForkRequest) bool {
	meta := stream.fork
	if meta == nil || meta.sourceID != req.SourceStreamID || meta.offsetSet != req.OffsetSet ||
		meta.subOffset != req.SubOffset || meta.contentTypeSet != req.ContentTypeSet ||
		meta.ttlSet != req.TTLSet || meta.expiresAtSet != req.ExpiresAtSet ||
		meta.isPrivate != req.Config.IsPrivate || meta.closed != req.Config.Closed {
		return false
	}
	if req.SourceIncarnationID != "" && req.SourceIncarnationID != meta.sourceIncarnationID {
		return false
	}
	if req.Config.ContentType != "" && !contentTypesMatch(req.Config.ContentType, stream.config.ContentType) {
		return false
	}
	if req.OffsetSet {
		normalized, _, err := normalizeForkOffset(req.Offset)
		if err != nil || normalized != meta.requestedOffset {
			return false
		}
	}
	if req.TTLSet && req.Config.TTL != meta.requestedTTL {
		return false
	}
	if req.ExpiresAtSet && !req.Config.ExpiresAt.Equal(meta.requestedExpiresAt) {
		return false
	}
	return true
}

type logicalMessageRef struct {
	owner *memoryStream
	msg   memoryMessage
}

// collectRefsLocked traverses one immutable fork prefix and appends message
// descriptors in logical order. stream.mu must already be held for reading.
func collectRefsLocked(stream *memoryStream, after, through int64, refs *[]logicalMessageRef) {
	if stream.parent != nil && after < stream.parentBoundaryIndex {
		parentThrough := through
		if parentThrough > stream.parentBoundaryIndex {
			parentThrough = stream.parentBoundaryIndex
		}
		stream.parent.mu.RLock()
		collectRefsLocked(stream.parent, after, parentThrough, refs)
		stream.parent.mu.RUnlock()
	}
	for _, msg := range stream.messages {
		if msg.index > after && msg.index <= through {
			*refs = append(*refs, logicalMessageRef{owner: stream, msg: msg})
		}
	}
}

func resolveForkBoundaryLocked(source *memoryStream, anchor int64, subOffset uint64) (boundary int64, prefix []byte, err error) {
	if subOffset == 0 {
		return anchor, nil, nil
	}

	refs := make([]logicalMessageRef, 0)
	collectRefsLocked(source, anchor, source.tailIndex, &refs)
	if len(refs) == 0 {
		return 0, nil, fmt.Errorf("fork sub-offset is past the source tail: %w", durablestream.ErrBadRequest)
	}

	if protocol.IsJSONContentType(source.config.ContentType) {
		first := refs[0]
		var available uint64
		for _, ref := range refs {
			if ref.owner != first.owner || ref.msg.batch != first.msg.batch {
				break
			}
			available++
		}
		if subOffset > available {
			return 0, nil, fmt.Errorf("JSON fork sub-offset exceeds the next append batch: %w", durablestream.ErrBadRequest)
		}
		return refs[subOffset-1].msg.index, nil, nil
	}

	first := refs[0].msg
	if subOffset > uint64(len(first.data)) {
		return 0, nil, fmt.Errorf("binary fork sub-offset exceeds the next message: %w", durablestream.ErrBadRequest)
	}
	if subOffset == uint64(len(first.data)) {
		return first.index, nil, nil
	}
	return anchor, bytes.Clone(first.data[:subOffset]), nil
}

func newForkStream(streamID string, cfg durablestream.StreamConfig, incID string, source *memoryStream, boundary int64, prefix []byte, initial [][]byte, meta *forkMetadata) *memoryStream {
	stream := &memoryStream{
		config:              cfg,
		tailIndex:           boundary,
		notifyCh:            make(chan struct{}),
		streamID:            streamID,
		incID:               incID,
		nextBatch:           1,
		parent:              source,
		parentBoundaryIndex: boundary,
		fork:                meta,
	}
	if len(prefix) != 0 {
		stream.tailIndex++
		// A strict binary prefix occupies the source message's original offset,
		// which is necessarily the first logical position after boundary.
		stream.messages = append(stream.messages, memoryMessage{
			data:   prefix,
			offset: storage.FormatSimpleOffset(stream.tailIndex),
			index:  stream.tailIndex,
			batch:  stream.nextBatch,
		})
		stream.nextBatch++
	}
	if len(initial) != 0 {
		batch := stream.nextBatch
		stream.nextBatch++
		for _, data := range initial {
			stream.tailIndex++
			stream.messages = append(stream.messages, memoryMessage{
				data:   data,
				offset: storage.FormatSimpleOffset(stream.tailIndex),
				index:  stream.tailIndex,
				batch:  batch,
			})
		}
	}
	return stream
}

// CreateFork atomically creates a pointer-stitched fork and its initial batch.
func (m *Storage) CreateFork(ctx context.Context, targetStreamID string, req durablestream.ForkRequest, messages [][]byte) (bool, *durablestream.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	cloned, err := cloneBatch(messages, true)
	if err != nil {
		return false, nil, err
	}
	if req.SourceStreamID == "" {
		return false, nil, fmt.Errorf("fork source stream ID is empty: %w", durablestream.ErrBadRequest)
	}
	if req.TTLSet && req.ExpiresAtSet {
		return false, nil, fmt.Errorf("fork TTL and expiry are mutually exclusive: %w", durablestream.ErrBadRequest)
	}

	m.topologyMu.Lock()
	defer m.topologyMu.Unlock()

	// Resolve an existing target before consulting the source. This preserves
	// retry idempotency even if the source has since grown or been soft-deleted.
	for {
		existing, ok := m.streams.Load(targetStreamID)
		if !ok {
			break
		}
		existing.mu.RLock()
		softDeleted := existing.softDeleted
		deleted := existing.deleted
		expired := existing.config.IsExpired()
		matches := !softDeleted && !deleted && !expired && forkRequestMatches(existing, req)
		var info *durablestream.StreamInfo
		if matches {
			info = streamInfoLocked(existing)
		}
		existing.mu.RUnlock()
		if softDeleted {
			return false, nil, fmt.Errorf("fork target path is soft-deleted: %w", durablestream.ErrConflict)
		}
		if deleted {
			m.streams.CompareAndDelete(targetStreamID, existing)
			if existing.refCount == 0 {
				m.collectLocked(existing)
			}
			continue
		}
		if expired {
			m.retireExpiredLocked(targetStreamID, existing)
			continue
		}
		if matches {
			return false, info, nil
		}
		return false, nil, fmt.Errorf("fork target exists with different configuration: %w", durablestream.ErrConflict)
	}

	source, ok := m.streams.Load(req.SourceStreamID)
	if !ok {
		return false, nil, durablestream.ErrNotFound
	}
	source.mu.RLock()
	if source.softDeleted {
		source.mu.RUnlock()
		return false, nil, fmt.Errorf("fork source is soft-deleted: %w", durablestream.ErrConflict)
	}
	if source.deleted {
		source.mu.RUnlock()
		return false, nil, durablestream.ErrNotFound
	}
	if source.config.IsExpired() {
		source.mu.RUnlock()
		return false, nil, durablestream.ErrNotFound
	}
	if req.SourceIncarnationID != "" && req.SourceIncarnationID != source.incID {
		source.mu.RUnlock()
		return false, nil, fmt.Errorf("fork source incarnation changed: %w", durablestream.ErrConflict)
	}

	cfg, err := resolveForkConfig(req, source.config, time.Now())
	if err != nil {
		source.mu.RUnlock()
		return false, nil, err
	}

	var requestedOffset durablestream.Offset
	var anchor int64
	if req.OffsetSet {
		requestedOffset, anchor, err = normalizeForkOffset(req.Offset)
		if err != nil {
			source.mu.RUnlock()
			return false, nil, err
		}
	} else {
		anchor = source.tailIndex
		requestedOffset = storage.FormatSimpleOffset(anchor)
	}
	if anchor > source.tailIndex {
		source.mu.RUnlock()
		return false, nil, fmt.Errorf("fork offset is past the source tail: %w", durablestream.ErrBadRequest)
	}

	boundary, prefix, err := resolveForkBoundaryLocked(source, anchor, req.SubOffset)
	sourceIncID := source.incID
	source.mu.RUnlock()
	if err != nil {
		return false, nil, err
	}

	incID, err := newIncarnationID()
	if err != nil {
		return false, nil, err
	}
	meta := &forkMetadata{
		sourceID:            req.SourceStreamID,
		sourceIncarnationID: sourceIncID,
		requestedOffset:     requestedOffset,
		offsetSet:           req.OffsetSet,
		subOffset:           req.SubOffset,
		contentTypeSet:      req.ContentTypeSet,
		ttlSet:              req.TTLSet,
		expiresAtSet:        req.ExpiresAtSet,
		requestedTTL:        req.Config.TTL,
		requestedExpiresAt:  req.Config.ExpiresAt,
		isPrivate:           req.Config.IsPrivate,
		closed:              req.Config.Closed,
	}
	target := newForkStream(targetStreamID, cfg, incID, source, boundary, prefix, cloned, meta)

	// No operation below can fail: publish the edge and target together while
	// topologyMu excludes readers and lifecycle mutations.
	source.refCount++
	m.streams.Store(targetStreamID, target)
	target.mu.RLock()
	info := streamInfoLocked(target)
	target.mu.RUnlock()
	return true, info, nil
}

// Append writes one message to a stream.
func (m *Storage) Append(ctx context.Context, streamID string, data []byte, seq string) (durablestream.Offset, error) {
	return m.AppendBatch(ctx, streamID, [][]byte{data}, seq)
}

// appendLocked installs an already-cloned atomic batch. stream.mu must be held
// for writing and the batch must be non-empty.
func appendLocked(stream *memoryStream, messages [][]byte, seq string) durablestream.Offset {
	batch := stream.nextBatch
	stream.nextBatch++
	for _, data := range messages {
		stream.tailIndex++
		stream.messages = append(stream.messages, memoryMessage{
			data:   data,
			offset: storage.FormatSimpleOffset(stream.tailIndex),
			index:  stream.tailIndex,
			batch:  batch,
		})
	}
	if seq != "" {
		stream.lastSeq = seq
		stream.lastSeqOffset = tailOffset(stream)
	}
	wakeLocked(stream)
	return tailOffset(stream)
}

// AppendBatch appends an ordered batch atomically.
func (m *Storage) AppendBatch(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cloned, err := cloneBatch(messages, false)
	if err != nil {
		return "", err
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		m.topologyMu.RUnlock()
		return "", durablestream.ErrNotFound
	}
	stream.mu.Lock()
	if err := directStateErrorLocked(stream); err != nil {
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		return "", err
	}
	if stream.config.Closed {
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		return "", durablestream.ErrStreamClosed
	}
	if seq != "" && stream.lastSeq != "" && seq <= stream.lastSeq {
		conflict := &durablestream.SequenceConflictError{LastSeq: stream.lastSeq, LastOffset: stream.lastSeqOffset}
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		return "", fmt.Errorf("sequence regression detected: %w", conflict)
	}
	offset := appendLocked(stream, cloned, seq)
	stream.mu.Unlock()
	m.topologyMu.RUnlock()
	return offset, nil
}

// CloseStream atomically appends an optional final batch and marks the target
// closed. Source closure state is never consulted for a fork.
func (m *Storage) CloseStream(ctx context.Context, streamID string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	cloned, err := cloneBatch(messages, true)
	if err != nil {
		return "", err
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		m.topologyMu.RUnlock()
		return "", durablestream.ErrNotFound
	}
	stream.mu.Lock()
	if err := directStateErrorLocked(stream); err != nil {
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		return "", err
	}
	if stream.config.Closed {
		offset := tailOffset(stream)
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		if len(cloned) == 0 {
			return offset, nil
		}
		return "", durablestream.ErrStreamClosed
	}
	if seq != "" && stream.lastSeq != "" && seq <= stream.lastSeq {
		conflict := &durablestream.SequenceConflictError{LastSeq: stream.lastSeq, LastOffset: stream.lastSeqOffset}
		stream.mu.Unlock()
		m.topologyMu.RUnlock()
		return "", fmt.Errorf("sequence regression detected: %w", conflict)
	}

	offset := tailOffset(stream)
	if len(cloned) != 0 {
		offset = appendLocked(stream, cloned, seq)
	} else {
		if seq != "" {
			stream.lastSeq = seq
			stream.lastSeqOffset = offset
		}
		wakeLocked(stream)
	}
	stream.config.Closed = true
	stream.mu.Unlock()
	m.topologyMu.RUnlock()
	return offset, nil
}

type collectState struct {
	messages []durablestream.StoredMessage
	total    int
	limit    int
	stopped  bool
}

// collectMessagesLocked traverses the visible prefix and suffix in order.
// stream.mu must already be held for reading.
func collectMessagesLocked(stream *memoryStream, after, through int64, state *collectState) {
	if state.stopped {
		return
	}
	if stream.parent != nil && after < stream.parentBoundaryIndex {
		parentThrough := through
		if parentThrough > stream.parentBoundaryIndex {
			parentThrough = stream.parentBoundaryIndex
		}
		stream.parent.mu.RLock()
		collectMessagesLocked(stream.parent, after, parentThrough, state)
		stream.parent.mu.RUnlock()
	}
	for _, msg := range stream.messages {
		if state.stopped || msg.index <= after || msg.index > through {
			continue
		}
		if state.limit > 0 && len(state.messages) > 0 && len(msg.data) > state.limit-state.total {
			state.stopped = true
			return
		}
		state.messages = append(state.messages, durablestream.StoredMessage{
			Data:   bytes.Clone(msg.data),
			Offset: msg.offset,
		})
		state.total += len(msg.data)
		if state.limit > 0 && state.total >= state.limit {
			state.stopped = true
			return
		}
	}
}

// readLocked returns one target snapshot while stream.mu is held for reading.
// topologyMu must also be held for reading so its ancestor chain cannot be
// reclaimed during traversal.
func readLocked(stream *memoryStream, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := directStateErrorLocked(stream); err != nil {
		return nil, err
	}
	_, after, err := storage.ParseOffset(offset)
	if err != nil {
		return nil, err
	}
	state := collectState{limit: limit}
	collectMessagesLocked(stream, after, stream.tailIndex, &state)

	next := offset
	if len(state.messages) != 0 {
		next = state.messages[len(state.messages)-1].Offset
	} else if next == "" || next == "-1" {
		next = storage.FormatSimpleOffset(0)
	}
	return &durablestream.ReadResult{
		Messages:      state.messages,
		NextOffset:    next,
		TailOffset:    tailOffset(stream),
		IncarnationID: stream.incID,
		Closed:        stream.config.Closed,
	}, nil
}

// Read returns messages strictly after offset, transparently stitching fork
// ancestry through the target's immutable boundary.
func (m *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		m.topologyMu.RUnlock()
		return nil, durablestream.ErrNotFound
	}
	stream.mu.RLock()
	result, err := readLocked(stream, offset, limit)
	stream.mu.RUnlock()
	m.topologyMu.RUnlock()
	return result, err
}

// Head returns target-owned metadata. Ancestor closure and expiry are not
// exposed through a live fork.
func (m *Storage) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		m.topologyMu.RUnlock()
		return nil, durablestream.ErrNotFound
	}
	stream.mu.RLock()
	err := directStateErrorLocked(stream)
	var info *durablestream.StreamInfo
	if err == nil {
		info = streamInfoLocked(stream)
	}
	stream.mu.RUnlock()
	m.topologyMu.RUnlock()
	return info, err
}

// Touch restarts only the target's independent sliding TTL window.
func (m *Storage) Touch(ctx context.Context, streamID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		m.topologyMu.RUnlock()
		return durablestream.ErrNotFound
	}
	stream.mu.Lock()
	err := directStateErrorLocked(stream)
	if err == nil {
		if cfg, moved := stream.config.SlideExpiry(time.Now()); moved {
			stream.config = cfg
			wakeLocked(stream)
		}
	}
	stream.mu.Unlock()
	m.topologyMu.RUnlock()
	return err
}

// Delete either removes a stream immediately or soft-deletes it while direct
// children retain its history.
func (m *Storage) Delete(ctx context.Context, streamID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.topologyMu.Lock()
	defer m.topologyMu.Unlock()
	stream, ok := m.streams.Load(streamID)
	if !ok {
		return durablestream.ErrNotFound
	}
	stream.mu.Lock()
	if stream.softDeleted {
		stream.mu.Unlock()
		return durablestream.ErrSoftDeleted
	}
	if stream.deleted {
		stream.mu.Unlock()
		m.streams.CompareAndDelete(streamID, stream)
		return durablestream.ErrNotFound
	}
	expired := stream.config.IsExpired()
	if stream.refCount != 0 && !expired {
		stream.softDeleted = true
		wakeLocked(stream)
		stream.mu.Unlock()
		return nil
	}

	m.streams.CompareAndDelete(streamID, stream)
	stream.deleted = true
	wakeLocked(stream)
	stream.mu.Unlock()
	if stream.refCount == 0 {
		m.collectLocked(stream)
	}
	return nil
}

// WaitForData waits on one exact target incarnation. Ancestor appends never
// wake it because the target's fork boundary is immutable.
func (m *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}

	m.topologyMu.RLock()
	stream, ok := m.streams.Load(streamID)
	m.topologyMu.RUnlock()
	if !ok {
		return nil, durablestream.ErrNotFound
	}
	return m.waitForStream(ctx, stream, offset, limit)
}

// waitForStream is the captured-incarnation half of WaitForData. Keeping it
// separate makes the no-generation-switch guarantee explicit and supports the
// lifecycle regression tests that intentionally retain an old pointer.
func (m *Storage) waitForStream(ctx context.Context, stream *memoryStream, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}
	for {
		m.topologyMu.RLock()
		stream.mu.RLock()
		result, err := readLocked(stream, offset, limit)
		notifyCh := stream.notifyCh
		expiresAt := stream.config.ExpiresAt
		stream.mu.RUnlock()
		m.topologyMu.RUnlock()
		if err != nil {
			return nil, err
		}
		if len(result.Messages) != 0 || result.Closed {
			return result, nil
		}

		var expiryTimer *time.Timer
		var expiryCh <-chan time.Time
		if !expiresAt.IsZero() {
			expiryTimer = time.NewTimer(time.Until(expiresAt))
			expiryCh = expiryTimer.C
		}
		select {
		case <-ctx.Done():
			stopTimer(expiryTimer)
			return nil, ctx.Err()
		case <-m.closed():
			stopTimer(expiryTimer)
			return nil, errClosed
		case <-notifyCh:
			stopTimer(expiryTimer)
		case <-expiryCh:
			// The next iteration performs the authoritative expiry transition.
		}
	}
}

// appendBatchToStream and readStream exercise an already captured incarnation.
// Public operations additionally hold topologyMu; these narrow helpers remain
// for regression tests of stale pointers after Delete.
func appendBatchToStream(stream *memoryStream, messages [][]byte, seq string) (durablestream.Offset, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if err := directStateErrorLocked(stream); err != nil {
		return "", err
	}
	if stream.config.Closed {
		return "", durablestream.ErrStreamClosed
	}
	if seq != "" && stream.lastSeq != "" && seq <= stream.lastSeq {
		return "", fmt.Errorf("sequence regression detected: %w", &durablestream.SequenceConflictError{LastSeq: stream.lastSeq, LastOffset: stream.lastSeqOffset})
	}
	return appendLocked(stream, messages, seq), nil
}

func readStream(ctx context.Context, stream *memoryStream, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("memorystorage: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}
	stream.mu.RLock()
	defer stream.mu.RUnlock()
	return readLocked(stream, offset, limit)
}

// Close releases blocked waiters. In-memory data remains available, matching
// this backend's historical post-Close behavior.
func (m *Storage) Close() error {
	m.closeOnce.Do(func() {
		m.initOnce.Do(func() { m.closedCh = make(chan struct{}) })
		close(m.closedCh)
	})
	return nil
}
