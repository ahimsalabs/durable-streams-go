package seglog

import (
	"context"
	"errors"
	"fmt"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// ErrDurabilityUnknown classifies a producer mutation whose WAL write was
// attempted but whose durability cannot be determined. The partition enters
// fail-stop mode after this error.
var ErrDurabilityUnknown = errors.New("seglog: durability unknown")

// ProducerResult is the ordered outcome of one mutation in a ProducerGroup.
type ProducerResult struct {
	Offset durablestream.Offset
	Err    error
}

// ProducerGroup collects append mutations for one WAL partition. It is
// caller-owned, single-use, and not safe for concurrent use. Payload bytes are
// borrowed until Commit returns; AppendBatch copies only the outer descriptor
// slice. A group gives one contiguous WAL write and one covering sync, not
// cross-stream atomicity: each mutation remains an independent WAL frame and
// can have an independent validation result.
type ProducerGroup struct {
	storage   *Storage
	partition *partition
	requests  []*request
	size      int64
	committed bool
}

// NewProducerGroup returns an empty producer durability group owned by the
// caller. The first added mutation selects its partition.
func (s *Storage) NewProducerGroup() *ProducerGroup {
	return &ProducerGroup{storage: s}
}

// Append adds one append mutation. It borrows data until Commit returns.
func (g *ProducerGroup) Append(streamID string, data []byte, seq string) error {
	if len(data) == 0 {
		return fmt.Errorf("seglog: empty data: %w", durablestream.ErrBadRequest)
	}
	return g.add(streamID, [][]byte{data}, seq)
}

// AppendBatch adds one atomic batch mutation. It copies the outer messages
// descriptor but borrows every payload byte slice until Commit returns.
func (g *ProducerGroup) AppendBatch(streamID string, messages [][]byte, seq string) error {
	return g.add(streamID, append([][]byte(nil), messages...), seq)
}

func (g *ProducerGroup) add(streamID string, messages [][]byte, seq string) error {
	if g == nil || g.storage == nil || g.committed {
		return fmt.Errorf("seglog: producer group is not active: %w", durablestream.ErrBadRequest)
	}
	if err := g.storage.checkClosed(); err != nil {
		return err
	}
	if err := validateStreamID(streamID); err != nil {
		return err
	}
	if err := g.storage.validateBatch(streamID, messages, false, len(seq)); err != nil {
		return err
	}
	part := g.storage.partitionFor(streamID)
	if g.partition != nil && part != g.partition {
		return fmt.Errorf("seglog: producer group stream %q is in another partition: %w", streamID, durablestream.ErrBadRequest)
	}
	frameSize := encodedFrameSize(len(streamID), len(seq), messages)
	if g.size+frameSize > part.wal.capacity() {
		return fmt.Errorf("seglog: producer group of %d bytes exceeds the %d-byte transaction capacity: %w",
			g.size+frameSize, part.wal.capacity(), durablestream.ErrPayloadTooLarge)
	}
	g.partition = part
	g.size += frameSize
	g.requests = append(g.requests, &request{
		op: opAppend, streamID: streamID, messages: messages, seq: seq, hasSeq: seq != "", done: make(chan result, 1),
	})
	return nil
}

// Commit admits the complete group as one bounded partition queue item and
// returns one result per added mutation in add order. Context cancellation is
// observed only before admission. After admission Commit waits through the
// covering durability result so callers can safely reclaim borrowed payloads.
func (g *ProducerGroup) Commit(ctx context.Context) ([]ProducerResult, error) {
	if g == nil || g.storage == nil || g.committed || len(g.requests) == 0 {
		return nil, fmt.Errorf("seglog: producer group is empty or already committed: %w", durablestream.ErrBadRequest)
	}
	g.committed = true
	if ctx == nil {
		ctx = context.Background()
	}
	g.storage.topologyMu.RLock()
	defer g.storage.topologyMu.RUnlock()
	groupDone := make(chan result, 1)
	admission := g.partition.submitContext(ctx, &request{group: g.requests, done: groupDone})
	if admission.err != nil {
		return nil, admission.err
	}
	results := make([]ProducerResult, len(g.requests))
	for i, req := range g.requests {
		res := <-req.done
		err := res.err
		if res.ambiguous {
			err = fmt.Errorf("%w: %w", ErrDurabilityUnknown, err)
		}
		results[i] = ProducerResult{Offset: res.offset, Err: err}
	}
	return results, nil
}
