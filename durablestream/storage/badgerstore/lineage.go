package badgerstore

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

// maxLineageDepth bounds work on corrupt or adversarial lineage graphs. A
// valid fork graph is acyclic because a target is created after its parent and
// records generation-fenced parent pointers, but persisted state is still
// treated as untrusted input on reopen.
const maxLineageDepth = 1024

func directRecordError(rec streamRecord) error {
	switch {
	case rec.isLegacy():
		return ErrLegacyFormat
	case rec.SoftDeleted:
		return durablestream.ErrSoftDeleted
	case rec.RefCount > 0 && rec.IsExpired():
		// Expiry cannot retire a path while descendants still require its data.
		// Treat it like a soft deletion even before the reaper persists that state.
		return durablestream.ErrSoftDeleted
	case rec.IsExpired():
		return durablestream.ErrNotFound
	default:
		return nil
	}
}

func offsetsForContiguousRange(first int64, count int) []durablestream.Offset {
	if count == 0 {
		return nil
	}
	offsets := make([]durablestream.Offset, count)
	for i := range count {
		offsets[i] = storage.FormatSimpleOffset(first + int64(i))
	}
	return offsets
}

func parsePersistedOffset(offset durablestream.Offset) (int64, error) {
	readSeq, position, err := storage.ParseOffset(offset)
	if err != nil || readSeq != 0 || offset != storage.FormatSimpleOffset(position) {
		return 0, fmt.Errorf("badgerstore: invalid persisted offset %q", offset)
	}
	return position, nil
}

// setBatchBoundary records one successful atomic append. The individual
// messages remain independently addressable; this compact start->end index is
// used only to interpret JSON sub-offsets in terms of flattened append batches.
func setBatchBoundary(txn *badger.Txn, streamID string, gen generation, offsets []durablestream.Offset) error {
	if len(offsets) == 0 {
		return nil
	}
	return txn.Set(batchKey(streamID, gen, offsets[0]), []byte(offsets[len(offsets)-1]))
}

// batchBoundsForMessage finds the atomic batch containing messageOffset.
func batchBoundsForMessage(txn *badger.Txn, streamID string, gen generation, messageOffset durablestream.Offset) (durablestream.Offset, durablestream.Offset, error) {
	prefix := batchPrefix(streamID, gen)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = true
	opts.Reverse = true

	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(batchKey(streamID, gen, messageOffset))
	if !it.ValidForPrefix(prefix) {
		return "", "", fmt.Errorf("badgerstore: missing batch metadata for %q at %q: %w", streamID, messageOffset, ErrLegacyFormat)
	}

	item := it.Item()
	start := durablestream.Offset(string(item.Key()[len(prefix):]))
	var end durablestream.Offset
	if err := item.Value(func(value []byte) error {
		end = durablestream.Offset(string(value))
		return nil
	}); err != nil {
		return "", "", fmt.Errorf("badgerstore: read batch metadata: %w", err)
	}
	if _, err := parsePersistedOffset(start); err != nil {
		return "", "", err
	}
	if _, err := parsePersistedOffset(end); err != nil {
		return "", "", err
	}
	if start.Compare(messageOffset) > 0 || end.Compare(messageOffset) < 0 {
		return "", "", fmt.Errorf("badgerstore: invalid batch metadata %q..%q for message %q", start, end, messageOffset)
	}
	if _, err := txn.Get(messageKey(streamID, gen, start)); err != nil {
		return "", "", fmt.Errorf("badgerstore: batch start %q has no message: %w", start, err)
	}
	if _, err := txn.Get(messageKey(streamID, gen, end)); err != nil {
		return "", "", fmt.Errorf("badgerstore: batch end %q has no message: %w", end, err)
	}

	// No later batch may begin inside this range. Without this check malformed
	// metadata could let one JSON sub-offset spill into a separate append.
	forwardOpts := badger.DefaultIteratorOptions
	forwardOpts.Prefix = prefix
	forwardOpts.PrefetchValues = false
	forward := txn.NewIterator(forwardOpts)
	defer forward.Close()
	forward.Seek(batchKey(streamID, gen, start))
	if forward.ValidForPrefix(prefix) {
		forward.Next()
	}
	if forward.ValidForPrefix(prefix) {
		nextStart := durablestream.Offset(string(forward.Item().Key()[len(prefix):]))
		if _, err := parsePersistedOffset(nextStart); err != nil {
			return "", "", err
		}
		if nextStart.Compare(end) <= 0 {
			return "", "", fmt.Errorf("badgerstore: overlapping batch metadata %q..%q and next start %q", start, end, nextStart)
		}
	}
	return start, end, nil
}

// getOwnTailOffset returns the last message physically owned by one generation.
func getOwnTailOffset(txn *badger.Txn, streamID string, gen generation) (durablestream.Offset, error) {
	prefix := messagePrefix(streamID, gen)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	opts.PrefetchValues = false
	opts.Reverse = true

	it := txn.NewIterator(opts)
	defer it.Close()
	seekKey := make([]byte, len(prefix)+1)
	copy(seekKey, prefix)
	seekKey[len(prefix)] = 0xff
	it.Seek(seekKey)
	if it.ValidForPrefix(prefix) {
		offset := durablestream.Offset(string(it.Item().Key()[len(prefix):]))
		if _, err := parsePersistedOffset(offset); err != nil {
			return "", err
		}
		return offset, nil
	}
	return storage.FormatSimpleOffset(0), nil
}

// streamTailOffset includes inherited data. A fork with no local messages has
// its immutable parent cutoff as its tail; local offsets always continue after
// that cutoff.
func streamTailOffset(txn *badger.Txn, streamID string, rec streamRecord) (durablestream.Offset, error) {
	own, err := getOwnTailOffset(txn, streamID, rec.Gen)
	if err != nil {
		return "", err
	}
	if rec.Parent != nil && own.Compare(rec.Parent.Offset) < 0 {
		return rec.Parent.Offset, nil
	}
	return own, nil
}

type visibleMessage struct {
	durablestream.StoredMessage
	ownerID  string
	ownerGen generation
}

// walkVisibleMessages visits one logical stream in offset order. The requested
// record itself must already have passed direct-access checks; ancestors are
// intentionally loaded with internal semantics so soft deletion and expiry do
// not break descendants.
func walkVisibleMessages(
	ctx context.Context,
	txn *badger.Txn,
	streamID string,
	rec streamRecord,
	after, upper durablestream.Offset,
	visited map[string]struct{},
	depth int,
	visit func(visibleMessage) (bool, error),
) (bool, error) {
	if depth > maxLineageDepth {
		return false, fmt.Errorf("badgerstore: fork lineage exceeds %d generations", maxLineageDepth)
	}
	key := streamID + "\x00" + string(rec.Gen)
	if _, seen := visited[key]; seen {
		return false, fmt.Errorf("badgerstore: cycle in fork lineage at %q", streamID)
	}
	visited[key] = struct{}{}
	defer delete(visited, key)

	if rec.Parent != nil && after.Compare(rec.Parent.Offset) < 0 {
		parent, found, err := getRecord(txn, rec.Parent.StreamID)
		if err != nil {
			return false, err
		}
		if !found || parent.isLegacy() || parent.Gen != rec.Parent.Gen {
			return false, fmt.Errorf("badgerstore: fork %q references unavailable source incarnation %q/%s", streamID, rec.Parent.StreamID, rec.Parent.Gen)
		}
		parentUpper := rec.Parent.Offset
		if upper.Compare(parentUpper) < 0 {
			parentUpper = upper
		}
		stopped, err := walkVisibleMessages(ctx, txn, rec.Parent.StreamID, parent, after, parentUpper, visited, depth+1, visit)
		if err != nil || stopped {
			return stopped, err
		}
	}

	localLower := after
	if rec.Parent != nil && localLower.Compare(rec.Parent.Offset) < 0 {
		localLower = rec.Parent.Offset
	}
	if localLower.Compare(upper) >= 0 {
		return false, nil
	}
	return walkOwnedMessages(ctx, txn, streamID, rec.Gen, localLower, upper, visit)
}

func walkOwnedMessages(
	ctx context.Context,
	txn *badger.Txn,
	streamID string,
	gen generation,
	after, upper durablestream.Offset,
	visit func(visibleMessage) (bool, error),
) (bool, error) {
	_, numericAfter, err := storage.ParseOffset(after)
	if err != nil {
		return false, err
	}
	if numericAfter == math.MaxInt64 {
		return false, nil
	}

	prefix := messagePrefix(streamID, gen)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	it.Seek(messageKey(streamID, gen, storage.FormatSimpleOffset(numericAfter+1)))

	for ; it.ValidForPrefix(prefix); it.Next() {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		item := it.Item()
		offset := durablestream.Offset(string(item.Key()[len(prefix):]))
		if _, err := parsePersistedOffset(offset); err != nil {
			return false, err
		}
		if offset.Compare(upper) > 0 {
			break
		}
		var data []byte
		if err := item.Value(func(value []byte) error {
			data = append([]byte(nil), value...)
			return nil
		}); err != nil {
			return false, fmt.Errorf("badgerstore: read message: %w", err)
		}
		stop, err := visit(visibleMessage{
			StoredMessage: durablestream.StoredMessage{Data: data, Offset: offset},
			ownerID:       streamID,
			ownerGen:      gen,
		})
		if err != nil || stop {
			return stop, err
		}
	}
	return false, nil
}

func readLogicalStream(ctx context.Context, txn *badger.Txn, streamID string, rec streamRecord, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if _, _, err := storage.ParseOffset(offset); err != nil {
		return nil, err
	}
	tail, err := streamTailOffset(txn, streamID, rec)
	if err != nil {
		return nil, fmt.Errorf("badgerstore: get logical tail offset: %w", err)
	}

	var messages []durablestream.StoredMessage
	totalBytes := 0
	_, err = walkVisibleMessages(ctx, txn, streamID, rec, offset, tail, make(map[string]struct{}), 0, func(message visibleMessage) (bool, error) {
		if limit > 0 && totalBytes+len(message.Data) > limit && len(messages) > 0 {
			return true, nil
		}
		messages = append(messages, message.StoredMessage)
		totalBytes += len(message.Data)
		return false, nil
	})
	if err != nil {
		return nil, err
	}

	next := offset
	if len(messages) > 0 {
		next = messages[len(messages)-1].Offset
	} else if next.IsZero() || next == "-1" {
		next = storage.FormatSimpleOffset(0)
	}
	return &durablestream.ReadResult{
		Messages:      messages,
		NextOffset:    next,
		TailOffset:    tail,
		Closed:        rec.Closed,
		IncarnationID: string(rec.Gen),
	}, nil
}

func isJSONContentType(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), "application/json")
}

// materializeSubOffset resolves the requested sub-position from the next
// visible source append. Binary sources copy a byte prefix of one message;
// JSON sources copy a prefix of the flattened messages belonging to one atomic
// append batch.
func materializeSubOffset(ctx context.Context, txn *badger.Txn, sourceID string, source streamRecord, anchor durablestream.Offset, subOffset uint64) ([][]byte, error) {
	if subOffset == 0 {
		return nil, nil
	}
	tail, err := streamTailOffset(txn, sourceID, source)
	if err != nil {
		return nil, err
	}
	if anchor.Compare(tail) >= 0 {
		return nil, fmt.Errorf("badgerstore: fork sub-offset has no data after anchor %q: %w", anchor, durablestream.ErrBadRequest)
	}

	var first *visibleMessage
	_, err = walkVisibleMessages(ctx, txn, sourceID, source, anchor, tail, make(map[string]struct{}), 0, func(message visibleMessage) (bool, error) {
		copy := message
		first = &copy
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if first == nil {
		return nil, fmt.Errorf("badgerstore: fork sub-offset has no visible data after anchor %q: %w", anchor, durablestream.ErrBadRequest)
	}

	if !isJSONContentType(source.ContentType) {
		if subOffset > uint64(len(first.Data)) {
			return nil, fmt.Errorf("badgerstore: binary fork sub-offset %d exceeds next message length %d: %w", subOffset, len(first.Data), durablestream.ErrBadRequest)
		}
		return [][]byte{append([]byte(nil), first.Data[:int(subOffset)]...)}, nil
	}

	_, batchEnd, err := batchBoundsForMessage(txn, first.ownerID, first.ownerGen, first.Offset)
	if err != nil {
		return nil, err
	}
	var available [][]byte
	_, err = walkVisibleMessages(ctx, txn, sourceID, source, anchor, tail, make(map[string]struct{}), 0, func(message visibleMessage) (bool, error) {
		if message.ownerID != first.ownerID || message.ownerGen != first.ownerGen || message.Offset.Compare(batchEnd) > 0 {
			return true, nil
		}
		available = append(available, message.Data)
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	if subOffset > uint64(len(available)) {
		return nil, fmt.Errorf("badgerstore: JSON fork sub-offset %d exceeds remaining batch length %d: %w", subOffset, len(available), durablestream.ErrBadRequest)
	}
	materialized := make([][]byte, int(subOffset))
	for i := range materialized {
		materialized[i] = append([]byte(nil), available[i]...)
	}
	return materialized, nil
}
