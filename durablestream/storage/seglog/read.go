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
	return s.readState(ctx, state, pos, limit)
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
	const attempts = 3
	var err error
	for range attempts {
		var res *durablestream.ReadResult
		res, err = s.readSnapshotted(ctx, state, pos, limit)
		if err == nil || (!errors.Is(err, errWALSegmentGone) && !errors.Is(err, os.ErrClosed)) {
			return res, err
		}
	}
	return nil, err
}

// errStopRead is the internal sentinel that ends a segment scan when the
// byte budget is exhausted.
var errStopRead = errors.New("seglog: read budget reached")

func (s *Storage) readSnapshotted(ctx context.Context, state *streamState, pos int64, limit int) (*durablestream.ReadResult, error) {
	snap := state.snapshot()
	if snap.deleted {
		return nil, notFoundErr(state.id)
	}
	if snap.cfg.IsExpired() {
		return nil, notFoundErr(state.id)
	}
	if pos < snap.floor {
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
			views = append(views, sf.view())
		}
		views = append(views, snap.activeView)
		for _, v := range views {
			if v.f == nil || v.lastIndex < from || v.firstIndex > segThrough {
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
