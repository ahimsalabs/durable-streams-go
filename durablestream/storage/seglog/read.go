package seglog

import (
	"context"
	"fmt"

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
func (s *Storage) readState(ctx context.Context, state *streamState, pos int64, limit int) (*durablestream.ReadResult, error) {
	snap := state.snapshot()
	if snap.deleted {
		return nil, notFoundErr(state.id)
	}
	if snap.cfg.IsExpired() {
		return nil, notFoundErr(state.id)
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

	wal := s.parts[state.partition].wal
	total := 0
	for idx := pos + 1; idx <= snap.tail; idx++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		loc := snap.walTail[idx-snap.firstLive]
		if limit > 0 && len(res.Messages) > 0 && total+int(loc.length) > limit {
			break
		}
		payload, err := wal.readPayload(loc)
		if err != nil {
			return nil, err
		}
		res.Messages = append(res.Messages, durablestream.StoredMessage{
			Data:   payload,
			Offset: storage.FormatSimpleOffset(idx),
		})
		total += int(loc.length)
		if limit > 0 && total >= limit {
			break
		}
	}
	if len(res.Messages) > 0 {
		res.NextOffset = res.Messages[len(res.Messages)-1].Offset
	}
	return res, nil
}
