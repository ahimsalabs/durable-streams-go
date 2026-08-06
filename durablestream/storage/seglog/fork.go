package seglog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

const maxLineageDepth = 1024

// forkRequestMeta is the complete request echo used for idempotency. It is
// deliberately separate from the resolved durable link below.
type forkRequestMeta struct {
	SourceStreamID      string                     `json:"sourceStreamId"`
	SourceIncarnationID string                     `json:"sourceIncarnationId,omitempty"`
	Offset              durablestream.Offset       `json:"offset"`
	OffsetSet           bool                       `json:"offsetSet"`
	SubOffset           uint64                     `json:"subOffset,omitempty"`
	Config              durablestream.StreamConfig `json:"config"`
	ContentTypeSet      bool                       `json:"contentTypeSet,omitempty"`
	TTLSet              bool                       `json:"ttlSet,omitempty"`
	ExpiresAtSet        bool                       `json:"expiresAtSet,omitempty"`
}

// forkMeta stores both the request identity and the resolved immutable link.
// Boundary is always the request anchor; child-local prefix records begin at
// Boundary+1 and are followed by the caller's initial batch.
type forkMeta struct {
	Request             forkRequestMeta `json:"request"`
	SourceID            string          `json:"sourceId"`
	SourceIncarnationID string          `json:"sourceIncarnationId"`
	Boundary            int64           `json:"boundary"`
	PrefixCount         int64           `json:"prefixCount,omitempty"`
}

type forkFrameMeta struct {
	Create createMeta `json:"create"`
	Fork   forkMeta   `json:"fork"`
}

func contentTypesMatch(a, b string) bool {
	baseA, _, _ := strings.Cut(a, ";")
	baseB, _, _ := strings.Cut(b, ";")
	return strings.EqualFold(strings.TrimSpace(baseA), strings.TrimSpace(baseB))
}

func resolveForkConfig(req durablestream.ForkRequest, source durablestream.StreamConfig, now time.Time) (durablestream.StreamConfig, error) {
	if req.TTLSet && req.ExpiresAtSet {
		return durablestream.StreamConfig{}, fmt.Errorf("seglog: fork TTL and expiry are mutually exclusive: %w", durablestream.ErrBadRequest)
	}
	if req.ContentTypeSet && req.Config.ContentType == "" {
		return durablestream.StreamConfig{}, fmt.Errorf("seglog: explicit fork content type is empty: %w", durablestream.ErrBadRequest)
	}
	if req.Config.ContentType != "" && !contentTypesMatch(req.Config.ContentType, source.ContentType) {
		return durablestream.StreamConfig{}, fmt.Errorf("seglog: fork content type does not match source: %w", durablestream.ErrConflict)
	}
	cfg := durablestream.StreamConfig{ContentType: source.ContentType, IsPrivate: req.Config.IsPrivate, Closed: req.Config.Closed}
	switch {
	case req.TTLSet:
		if req.Config.TTL < 0 {
			return durablestream.StreamConfig{}, fmt.Errorf("seglog: fork TTL is negative: %w", durablestream.ErrBadRequest)
		}
		cfg.TTL = req.Config.TTL
		if cfg.TTL > 0 {
			cfg.ExpiresAt = now.Add(cfg.TTL)
		}
	case req.ExpiresAtSet:
		if req.Config.ExpiresAt.IsZero() {
			return durablestream.StreamConfig{}, fmt.Errorf("seglog: explicit fork expiry is empty: %w", durablestream.ErrBadRequest)
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

func forkMatches(st *streamState, req durablestream.ForkRequest) bool {
	m := st.fork
	return m != nil && m.Request == requestForkMeta(req)
}

func requestForkMeta(req durablestream.ForkRequest) forkRequestMeta {
	return forkRequestMeta{SourceStreamID: req.SourceStreamID, SourceIncarnationID: req.SourceIncarnationID, Offset: req.Offset, OffsetSet: req.OffsetSet, SubOffset: req.SubOffset, Config: req.Config, ContentTypeSet: req.ContentTypeSet, TTLSet: req.TTLSet, ExpiresAtSet: req.ExpiresAtSet}
}

// CreateFork commits exactly one opFork frame on the target partition. The
// source refCount is provisionally pinned before queue submission, preventing
// deletion while the worker durably creates and publishes the target edge.
func (s *Storage) CreateFork(ctx context.Context, targetID string, req durablestream.ForkRequest, messages [][]byte) (bool, *durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return false, nil, err
	}
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	if err := validateStreamID(targetID); err != nil {
		return false, nil, err
	}
	if err := validateStreamID(req.SourceStreamID); err != nil {
		return false, nil, err
	}
	if targetID == req.SourceStreamID {
		return false, nil, fmt.Errorf("seglog: stream cannot fork from itself: %w", durablestream.ErrConflict)
	}
	if req.SubOffset > 0 && !req.OffsetSet {
		return false, nil, fmt.Errorf("seglog: sub-offset requires explicit offset: %w", durablestream.ErrBadRequest)
	}
	// Payload validity precedes target idempotency by contract. Exact aggregate
	// sizing follows resolution because prefixes and metadata are then final.
	if err := s.validatePayloads(messages, true); err != nil {
		return false, nil, err
	}

	s.topologyMu.Lock()
	defer s.topologyMu.Unlock()
	if existing, ok := s.streams.Load(targetID); ok {
		snap := existing.snapshot()
		if snap.softDeleted {
			return false, nil, fmt.Errorf("seglog: fork target is soft-deleted: %w", durablestream.ErrConflict)
		}
		if !snap.deleted && !snap.cfg.IsExpired() && forkMatches(existing, req) {
			return false, infoFromSnapshot(snap), nil
		}
		return false, nil, fmt.Errorf("seglog: fork target exists: %w", durablestream.ErrConflict)
	}
	source, ok := s.streams.Load(req.SourceStreamID)
	if !ok {
		return false, nil, notFoundErr(req.SourceStreamID)
	}
	// Pin before taking the source snapshot or validating it. physicalMu is
	// the acquisition side of the physical-trim exclusion protocol.
	source.physicalMu.Lock()
	source.refCount.Add(1)
	source.physicalMu.Unlock()
	keepPin := false
	defer func() {
		if !keepPin {
			s.rollbackSourcePin(source)
		}
	}()
	ss := source.snapshot()
	if ss.softDeleted || (ss.cfg.IsExpired() && source.refCount.Load() > 1) {
		return false, nil, fmt.Errorf("seglog: fork source is soft-deleted: %w", durablestream.ErrConflict)
	}
	if ss.deleted || ss.cfg.IsExpired() {
		return false, nil, notFoundErr(req.SourceStreamID)
	}
	if req.SourceIncarnationID != "" && req.SourceIncarnationID != ss.inc.String() {
		return false, nil, fmt.Errorf("seglog: source incarnation changed: %w", durablestream.ErrConflict)
	}
	cfg, err := resolveForkConfig(req, ss.cfg, time.Now())
	if err != nil {
		return false, nil, err
	}
	anchor := ss.tail
	if req.OffsetSet {
		readSeq, pos, err := storage.ParseOffset(req.Offset)
		if err != nil || readSeq != 0 {
			return false, nil, fmt.Errorf("seglog: invalid fork offset: %w", durablestream.ErrBadRequest)
		}
		anchor = pos
	}
	if anchor < ss.floor || anchor > ss.tail {
		return false, nil, fmt.Errorf("seglog: fork offset is outside source history: %w", durablestream.ErrBadRequest)
	}
	boundary, prefixes, err := s.resolveSubOffset(ctx, source, anchor, req.SubOffset)
	if err != nil {
		return false, nil, err
	}
	owned := append(prefixes[:len(prefixes):len(prefixes)], messages...)
	meta := &forkMeta{Request: requestForkMeta(req), SourceID: req.SourceStreamID, SourceIncarnationID: ss.inc.String(), Boundary: boundary, PrefixCount: int64(len(prefixes))}
	metaRaw, err := json.Marshal(forkFrameMeta{Create: createMeta{ContentType: cfg.ContentType, TTLNanos: int64(cfg.TTL), ExpiresAt: cfg.ExpiresAt, IsPrivate: cfg.IsPrivate, Closed: cfg.Closed, Retention: &retentionMeta{MaxBytes: s.opts.DefaultRetention.MaxBytes, MaxAgeNanos: int64(s.opts.DefaultRetention.MaxAge)}}, Fork: *meta})
	if err != nil {
		return false, nil, fmt.Errorf("seglog: encode fork meta: %w", err)
	}
	if err := s.validateBatch(targetID, owned, true, len(metaRaw)); err != nil {
		return false, nil, err
	}
	res := s.partitionFor(targetID).submit(&request{op: opFork, streamID: targetID, cfg: cfg, messages: owned, forkSource: source, forkMeta: meta, forkMetaRaw: metaRaw, forkBoundary: boundary, prefixCount: int64(len(prefixes)), done: make(chan result, 1)})
	if res.err != nil {
		keepPin = res.ambiguous
		return false, nil, res.err
	}
	st, ok := s.streams.Load(targetID)
	if !ok {
		return false, nil, fmt.Errorf("seglog: committed fork was not published")
	}
	keepPin = true
	return true, infoFromSnapshot(st.snapshot()), nil
}

func infoFromSnapshot(s readSnapshot) *durablestream.StreamInfo {
	return &durablestream.StreamInfo{ContentType: s.cfg.ContentType, NextOffset: storage.FormatSimpleOffset(s.tail), TTL: s.cfg.TTL, ExpiresAt: s.cfg.ExpiresAt, IsPrivate: s.cfg.IsPrivate, Closed: s.closed, IncarnationID: s.inc.String()}
}

func (s *Storage) resolveSubOffset(ctx context.Context, source *streamState, anchor int64, sub uint64) (int64, [][]byte, error) {
	if sub == 0 {
		return anchor, nil, nil
	}
	res, err := s.readLogical(ctx, source, anchor, 0, 0)
	if err != nil || len(res.Messages) == 0 {
		return 0, nil, fmt.Errorf("seglog: fork sub-offset has no next message: %w", durablestream.ErrBadRequest)
	}
	if !strings.EqualFold(strings.TrimSpace(strings.Split(source.snapshot().cfg.ContentType, ";")[0]), "application/json") {
		if sub > uint64(len(res.Messages[0].Data)) {
			return 0, nil, fmt.Errorf("seglog: binary sub-offset exceeds message: %w", durablestream.ErrBadRequest)
		}
		return anchor, [][]byte{bytes.Clone(res.Messages[0].Data[:sub])}, nil
	}
	firstBatch, err := s.messageBatch(source, anchor+1, 0)
	if err != nil {
		return 0, nil, err
	}
	if sub > uint64(len(res.Messages)) {
		return 0, nil, fmt.Errorf("seglog: JSON sub-offset exceeds batch: %w", durablestream.ErrBadRequest)
	}
	for i := uint64(0); i < sub; i++ {
		_, pos, _ := storage.ParseOffset(res.Messages[i].Offset)
		batch, err := s.messageBatch(source, pos, 0)
		if err != nil || batch != firstBatch {
			return 0, nil, fmt.Errorf("seglog: JSON sub-offset crosses batch: %w", durablestream.ErrBadRequest)
		}
	}
	prefixes := make([][]byte, sub)
	for i := range prefixes {
		prefixes[i] = bytes.Clone(res.Messages[i].Data)
	}
	return anchor, prefixes, nil
}

func (p *partition) stageFork(op *stagedOp, req *request, ps *pendingStream, now time.Time, ts int64) frameSpec {
	if ps.exists {
		op.res = result{err: fmt.Errorf("seglog: fork target exists: %w", durablestream.ErrConflict)}
		return frameSpec{}
	}
	inc, err := newIncarnation()
	if err != nil {
		op.res = result{err: err}
		return frameSpec{}
	}
	st := newStreamState(req.streamID, inc, p.id, req.cfg)
	st.closed, st.retention, st.parent, st.parentBoundary, st.fork = req.cfg.Closed, p.st.opts.DefaultRetention, req.forkSource, req.forkBoundary, req.forkMeta
	st.floor, st.materializedThrough, st.firstLive = 0, req.forkBoundary, req.forkBoundary+1
	st.nextIndex = req.forkBoundary + 1 + int64(len(req.messages))
	op.applyFork = &createApply{newState: st}
	op.res = result{created: true, offset: storage.FormatSimpleOffset(st.nextIndex - 1)}
	ps.state, ps.exists, ps.cfg, ps.closed, ps.softDeleted, ps.nextIndex = st, true, st.cfg, st.closed, false, st.nextIndex
	return frameSpec{op: opFork, streamID: req.streamID, inc: inc, meta: req.forkMetaRaw, firstIndex: req.forkBoundary + 1, ts: ts, payloads: req.messages}
}

func (s *Storage) rollbackSourcePin(source *streamState) {
	if source.refCount.Add(-1) < 0 {
		source.refCount.Add(1)
		return
	}
	if source.refCount.Load() == 0 {
		ss := source.snapshot()
		if ss.softDeleted || ss.cfg.IsExpired() {
			res := s.partitionFor(source.id).submit(&request{op: opDelete, streamID: source.id, hardCascade: true, done: make(chan result, 1)})
			if res.err == nil {
				s.releaseParentCascade(source)
			}
		}
	}
}

// releaseParentCascade runs with topologyMu held after a child hard delete.
func (s *Storage) releaseParentCascade(child *streamState) {
	for depth := 0; child.parent != nil && depth < maxLineageDepth; depth++ {
		parent := child.parent
		if parent.refCount.Add(-1) != 0 {
			return
		}
		ps := parent.snapshot()
		if !ps.softDeleted && !ps.cfg.IsExpired() {
			return
		}
		res := s.partitionFor(parent.id).submit(&request{op: opDelete, streamID: parent.id, hardCascade: true, done: make(chan result, 1)})
		if res.err != nil {
			return
		}
		child = parent
	}
}
