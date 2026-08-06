package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// Read implements durablestream.Storage.
func (s *Storage) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	pos, err := parseReadOffset(offset, limit)
	if err != nil {
		return nil, err
	}
	state, ok := s.streams.Load(streamID)
	if !ok {
		return nil, notFoundErr(streamID)
	}
	snap := state.snapshot()
	if snap.softDeleted || (snap.cfg.IsExpired() && state.refCount.Load() != 0) {
		return nil, softDeletedErr(streamID)
	}
	if snap.deleted || snap.cfg.IsExpired() {
		return nil, notFoundErr(streamID)
	}
	if pos < snap.floor {
		return nil, fmt.Errorf("seglog: stream %q retained floor is %d: %w", streamID, snap.floor, durablestream.ErrGone)
	}
	return s.readLogical(ctx, state, pos, limit, 0)
}

// parseReadOffset validates the shared (offset, limit) read arguments and
// resolves the position, normalizing the ""/"-1" start sentinels to zero.
func parseReadOffset(offset durablestream.Offset, limit int) (int64, error) {
	if limit < 0 {
		return 0, fmt.Errorf("seglog: limit cannot be negative: %w", durablestream.ErrBadRequest)
	}
	_, pos, err := storage.ParseOffset(offset)
	if err != nil {
		return 0, err // wraps ErrBadRequest
	}
	return pos, nil
}

// readState reads messages strictly after pos from one pinned incarnation.
// It snapshots under RLock and performs all file I/O lock-free (invariant
// I5); returned buffers are fresh and owned by the caller.
//
// A read can race WAL reclamation: the snapshot references a WAL location
// whose segment the materializer just removed. The data is then available
// from stream segments, so the read re-snapshots and retries.
func (s *Storage) readState(ctx context.Context, state *streamState, pos int64, limit int) (*durablestream.ReadResult, error) {
	snap := state.snapshot()
	if snap.softDeleted || (snap.cfg.IsExpired() && state.refCount.Load() != 0) {
		return nil, softDeletedErr(state.id)
	}
	if snap.deleted || snap.cfg.IsExpired() {
		return nil, notFoundErr(state.id)
	}
	if pos < snap.floor {
		return nil, fmt.Errorf("seglog: stream %q retained floor is %d: %w", state.id, snap.floor, durablestream.ErrGone)
	}
	return s.readLogical(ctx, state, pos, limit, 0)
}

// readLogical pointer-stitches immutable ancestor prefixes. Ancestors are
// read internally, so their public soft-delete/expiry state cannot sever a
// live descendant.
func (s *Storage) readLogical(ctx context.Context, state *streamState, pos int64, limit, depth int) (*durablestream.ReadResult, error) {
	budget := &readBudget{limit: limit}
	seen := make(map[*streamState]struct{})
	return s.readLineage(ctx, state, pos, depth, budget, seen)
}

type readBudget struct {
	limit int
	used  int
	has   bool
}

func (b *readBudget) remaining() int {
	if b.limit == 0 {
		return 0
	}
	return max(0, b.limit-b.used)
}

func (s *Storage) readLineage(ctx context.Context, state *streamState, pos int64, depth int, budget *readBudget, seen map[*streamState]struct{}) (*durablestream.ReadResult, error) {
	if depth > maxLineageDepth {
		return nil, fmt.Errorf("seglog: fork lineage exceeds %d", maxLineageDepth)
	}
	if _, ok := seen[state]; ok {
		return nil, fmt.Errorf("seglog: cycle in fork lineage at %q", state.id)
	}
	seen[state] = struct{}{}
	defer delete(seen, state)
	snap := state.snapshot()
	if snap.parent == nil {
		return s.readLocalBudget(ctx, state, pos, budget)
	}
	res := &durablestream.ReadResult{NextOffset: storage.FormatSimpleOffset(pos), TailOffset: storage.FormatSimpleOffset(snap.tail), IncarnationID: snap.inc.String(), Closed: snap.closed}
	if pos < snap.parentBoundary {
		usedBefore, hadBefore := budget.used, budget.has
		parent, err := s.readLineage(ctx, snap.parent, pos, depth+1, budget, seen)
		if err != nil {
			return nil, err
		}
		// The parent can have appended beyond this child's immutable boundary.
		// Those records are invisible here and must not consume the child's
		// shared byte budget or suppress its local suffix.
		budget.used, budget.has = usedBefore, hadBefore
		for _, msg := range parent.Messages {
			_, idx, _ := storage.ParseOffset(msg.Offset)
			if idx > snap.parentBoundary {
				break
			}
			res.Messages = append(res.Messages, msg)
			budget.used += len(msg.Data)
			budget.has = true
		}
	}
	if budget.limit > 0 && budget.has && budget.used >= budget.limit {
		if len(res.Messages) > 0 {
			res.NextOffset = res.Messages[len(res.Messages)-1].Offset
		}
		return res, nil
	}
	localPos := max(pos, snap.parentBoundary)
	local, err := s.readLocalBudget(ctx, state, localPos, budget)
	if err != nil {
		return nil, err
	}
	res.Messages = append(res.Messages, local.Messages...)
	if len(res.Messages) > 0 {
		res.NextOffset = res.Messages[len(res.Messages)-1].Offset
	}
	return res, nil
}

func (s *Storage) readLocalBudget(ctx context.Context, state *streamState, pos int64, budget *readBudget) (*durablestream.ReadResult, error) {
	res, err := s.readLocal(ctx, state, pos, budget.remaining(), false)
	if err != nil {
		return nil, err
	}
	if budget.limit > 0 && budget.has && len(res.Messages) > 0 && len(res.Messages[0].Data) > budget.remaining() {
		// readLocal permits its first message to exceed its local limit. That is
		// correct only for the first message in the whole stitched result.
		res.Messages = nil
		res.NextOffset = storage.FormatSimpleOffset(pos)
		return res, nil
	}
	for _, msg := range res.Messages {
		budget.used += len(msg.Data)
		budget.has = true
	}
	return res, nil
}

func (s *Storage) readLocal(ctx context.Context, state *streamState, pos int64, limit int, rejectInvisible bool) (*durablestream.ReadResult, error) {
	const attempts = 3
	var err error
	for range attempts {
		var res *durablestream.ReadResult
		res, err = s.readSnapshotted(ctx, state, pos, limit, rejectInvisible, rejectInvisible)
		if err == nil || (!errors.Is(err, errWALSegmentGone) && !errors.Is(err, os.ErrClosed)) {
			return res, err
		}
	}
	return nil, err
}

// errStopRead is the internal sentinel that ends a segment scan when the
// byte budget is exhausted.
var errStopRead = errors.New("seglog: read budget reached")

func (s *Storage) readSnapshotted(ctx context.Context, state *streamState, pos int64, limit int, rejectInvisible, enforceFloor bool) (*durablestream.ReadResult, error) {
	snap := state.snapshot()
	if rejectInvisible && snap.deleted {
		return nil, notFoundErr(state.id)
	}
	if rejectInvisible && snap.cfg.IsExpired() {
		return nil, notFoundErr(state.id)
	}
	if enforceFloor && pos < snap.floor {
		return nil, fmt.Errorf("seglog: stream %q retained floor is %d: %w", state.id, snap.floor, durablestream.ErrGone)
	}

	res := &durablestream.ReadResult{
		NextOffset:    storage.FormatSimpleOffset(pos),
		TailOffset:    storage.FormatSimpleOffset(snap.tail),
		IncarnationID: snap.inc.String(),
		Closed:        snap.closed,
	}
	if pos >= snap.tail {
		// At or past the tail is not an error; a poller retries at the same
		// (normalized) offset.
		return res, nil
	}

	total := 0
	// accept reports whether the message fits the byte budget: the first
	// message is always returned whole, later ones must fit.
	accept := func(length int32) bool {
		if limit > 0 && len(res.Messages) > 0 && total+int(length) > limit {
			return false
		}
		return true
	}
	appendMsg := func(idx int64, payload []byte) bool {
		res.Messages = append(res.Messages, durablestream.StoredMessage{
			Data:   payload,
			Offset: storage.FormatSimpleOffset(idx),
		})
		total += len(payload)
		return !(limit > 0 && total >= limit)
	}

	// Materialized part: indices in (pos, min(tail, through)] from segments.
	from := pos + 1
	if from <= snap.through {
		segThrough := min(snap.tail, snap.through)
		views := make([]segmentView, 0, len(snap.sealed)+1)
		for _, sf := range snap.sealed {
			views = append(views, sf.view(s.fdCache))
		}
		views = append(views, snap.activeView)
		for _, v := range views {
			if v.path == "" || v.lastIndex < from || v.firstIndex > segThrough {
				continue
			}
			err := v.readRecords(max(from, v.firstIndex), segThrough, func(rec segmentRecord, payloadOff int64) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !accept(rec.length) {
					return errStopRead
				}
				payload, err := v.readPayloadAt(rec, payloadOff)
				if err != nil {
					return err
				}
				if !appendMsg(rec.index, payload) {
					return errStopRead
				}
				return nil
			})
			if errors.Is(err, errStopRead) {
				res.NextOffset = res.Messages[len(res.Messages)-1].Offset
				return res, nil
			}
			if err != nil {
				return nil, err
			}
		}
		from = segThrough + 1
	}

	// WAL-resident part: indices in [max(from, firstLive), tail].
	wal := s.parts[state.partition].wal
	for idx := max(from, snap.firstLive); idx <= snap.tail; idx++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loc := snap.walTail[idx-snap.firstLive]
		if !accept(loc.length) {
			break
		}
		payload, err := wal.readPayload(loc)
		if err != nil {
			return nil, err
		}
		if !appendMsg(idx, payload) {
			break
		}
	}
	if len(res.Messages) > 0 {
		res.NextOffset = res.Messages[len(res.Messages)-1].Offset
	}
	return res, nil
}

// messageBatch returns the first index of the atomic batch owning index.
func (s *Storage) messageBatch(state *streamState, index int64, depth int) (int64, error) {
	if depth > maxLineageDepth {
		return 0, fmt.Errorf("seglog: fork lineage exceeds %d", maxLineageDepth)
	}
	snap := state.snapshot()
	if snap.parent != nil && index <= snap.parentBoundary {
		return s.messageBatch(snap.parent, index, depth+1)
	}
	if index >= snap.firstLive && index < snap.firstLive+int64(len(snap.walTail)) {
		return snap.walTail[index-snap.firstLive].batchFirst, nil
	}
	views := make([]segmentView, 0, len(snap.sealed)+1)
	for _, sf := range snap.sealed {
		views = append(views, sf.view(s.fdCache))
	}
	views = append(views, snap.activeView)
	for _, v := range views {
		if index < v.firstIndex || index > v.lastIndex {
			continue
		}
		var batch int64
		err := v.readRecords(index, index, func(rec segmentRecord, _ int64) error { batch = rec.batchFirst; return nil })
		return batch, err
	}
	return 0, fmt.Errorf("seglog: message %d is unavailable", index)
}
