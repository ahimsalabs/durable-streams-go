package badgerstore

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

func contentTypeMatches(a, b string) bool {
	baseA, _, _ := strings.Cut(a, ";")
	baseB, _, _ := strings.Cut(b, ";")
	return strings.EqualFold(strings.TrimSpace(baseA), strings.TrimSpace(baseB))
}

func normalizeForkOffset(offset durablestream.Offset) (durablestream.Offset, int64, error) {
	readSeq, position, err := storage.ParseOffset(offset)
	if err != nil {
		return "", 0, err
	}
	if readSeq != 0 {
		return "", 0, fmt.Errorf("badgerstore: fork offset uses unsupported read sequence %d: %w", readSeq, durablestream.ErrBadRequest)
	}
	return storage.FormatOffset(readSeq, position), position, nil
}

func forkIdentityMatches(rec streamRecord, req durablestream.ForkRequest, normalizedOffset durablestream.Offset) bool {
	if rec.Parent == nil || rec.Parent.StreamID != req.SourceStreamID ||
		rec.Parent.OffsetSet != req.OffsetSet || rec.Parent.SubOffset != req.SubOffset ||
		rec.Parent.ContentTypeSet != req.ContentTypeSet ||
		rec.Parent.TTLSet != req.TTLSet || rec.Parent.ExpiresAtSet != req.ExpiresAtSet {
		return false
	}
	if req.SourceIncarnationID != "" && string(rec.Parent.Gen) != req.SourceIncarnationID {
		return false
	}
	if req.OffsetSet && rec.Parent.Offset != normalizedOffset {
		return false
	}
	if req.TTLSet && rec.Parent.RequestedTTL != req.Config.TTL {
		return false
	}
	if req.ExpiresAtSet && !rec.Parent.RequestedExpiresAt.Equal(req.Config.ExpiresAt) {
		return false
	}
	return true
}

func forkTargetConfigMatches(rec streamRecord, req durablestream.ForkRequest) bool {
	candidate := req.Config
	if candidate.ContentType != "" && !contentTypeMatches(candidate.ContentType, rec.ContentType) {
		return false
	}
	candidate.ContentType = rec.ContentType
	switch {
	case req.TTLSet:
		candidate.ExpiresAt = rec.ExpiresAt // Matches ignores it when TTL is set.
	case req.ExpiresAtSet:
		candidate.TTL = 0
	default:
		candidate.TTL = rec.TTL
		candidate.ExpiresAt = rec.ExpiresAt
	}
	return rec.StreamConfig.Matches(candidate)
}

func resolveForkConfig(req durablestream.ForkRequest, source streamRecord, now time.Time) (durablestream.StreamConfig, error) {
	if req.TTLSet && req.ExpiresAtSet {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: fork TTL and ExpiresAt are mutually exclusive: %w", durablestream.ErrBadRequest)
	}
	if req.Config.ContentType != "" && !contentTypeMatches(req.Config.ContentType, source.ContentType) {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: fork content type %q does not match source %q: %w", req.Config.ContentType, source.ContentType, durablestream.ErrConflict)
	}
	if req.ContentTypeSet && req.Config.ContentType == "" {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: explicit empty fork content type: %w", durablestream.ErrBadRequest)
	}
	if req.TTLSet && req.Config.TTL < 0 {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: fork TTL cannot be negative: %w", durablestream.ErrBadRequest)
	}
	if req.ExpiresAtSet && req.Config.ExpiresAt.IsZero() {
		return durablestream.StreamConfig{}, fmt.Errorf("badgerstore: explicit fork expiry is empty: %w", durablestream.ErrBadRequest)
	}

	cfg := req.Config
	cfg.ContentType = source.ContentType
	switch {
	case req.TTLSet:
		cfg.ExpiresAt = time.Time{}
		if cfg.TTL > 0 {
			cfg.ExpiresAt = now.Add(cfg.TTL)
		}
	case req.ExpiresAtSet:
		cfg.TTL = 0
	default:
		cfg.TTL = source.TTL
		if source.TTL > 0 {
			cfg.ExpiresAt = now.Add(source.TTL)
		} else {
			cfg.ExpiresAt = source.ExpiresAt
		}
	}
	return cfg, nil
}

func visibleOffsetExists(ctx context.Context, txn *badger.Txn, streamID string, rec streamRecord, offset, tail durablestream.Offset) (bool, error) {
	if offset == storage.FormatSimpleOffset(0) || offset == tail {
		return true, nil
	}
	found := false
	_, err := walkVisibleMessages(ctx, txn, streamID, rec, storage.FormatSimpleOffset(0), offset, make(map[string]struct{}), 0, func(message visibleMessage) (bool, error) {
		if message.Offset == offset {
			found = true
			return true, nil
		}
		return false, nil
	})
	return found, err
}

func writeLocalBatch(txn *badger.Txn, streamID string, gen generation, firstPosition int64, messages [][]byte) ([]durablestream.Offset, error) {
	offsets := offsetsForContiguousRange(firstPosition, len(messages))
	for i, message := range messages {
		if err := txn.Set(messageKey(streamID, gen, offsets[i]), message); err != nil {
			return nil, err
		}
	}
	if err := setBatchBoundary(txn, streamID, gen, offsets); err != nil {
		return nil, err
	}
	return offsets, nil
}

func (s *Storage) streamInfoForRecord(txn *badger.Txn, streamID string, rec streamRecord) (*durablestream.StreamInfo, error) {
	tail, err := streamTailOffset(txn, streamID, rec)
	if err != nil {
		return nil, err
	}
	lastSeq, err := s.getLastSeq(txn, streamID)
	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, fmt.Errorf("badgerstore: get last seq: %w", err)
	}
	return &durablestream.StreamInfo{
		ContentType:   rec.ContentType,
		NextOffset:    tail,
		LastSeq:       lastSeq,
		TTL:           rec.TTL,
		ExpiresAt:     rec.ExpiresAt,
		IsPrivate:     rec.IsPrivate,
		Closed:        rec.Closed,
		IncarnationID: string(rec.Gen),
	}, nil
}

// CreateFork atomically installs a generation-fenced parent reference, any
// materialized sub-offset prefix, and the target's initial batch.
func (s *Storage) CreateFork(ctx context.Context, targetStreamID string, req durablestream.ForkRequest, messages [][]byte) (bool, *durablestream.StreamInfo, error) {
	if err := s.checkClosed(); err != nil {
		return false, nil, err
	}
	if err := ctx.Err(); err != nil {
		return false, nil, err
	}
	if err := validateStreamID(targetStreamID); err != nil {
		return false, nil, err
	}
	if err := validateStreamID(req.SourceStreamID); err != nil {
		return false, nil, err
	}
	if targetStreamID == req.SourceStreamID {
		return false, nil, fmt.Errorf("badgerstore: a stream cannot fork from itself: %w", durablestream.ErrConflict)
	}
	if req.SubOffset > 0 && !req.OffsetSet {
		return false, nil, fmt.Errorf("badgerstore: a positive fork sub-offset requires an explicit offset: %w", durablestream.ErrBadRequest)
	}
	if req.TTLSet && req.ExpiresAtSet {
		return false, nil, fmt.Errorf("badgerstore: fork TTL and ExpiresAt are mutually exclusive: %w", durablestream.ErrBadRequest)
	}
	if req.ContentTypeSet && req.Config.ContentType == "" {
		return false, nil, fmt.Errorf("badgerstore: explicit empty fork content type: %w", durablestream.ErrBadRequest)
	}
	if req.TTLSet && req.Config.TTL < 0 {
		return false, nil, fmt.Errorf("badgerstore: fork TTL cannot be negative: %w", durablestream.ErrBadRequest)
	}
	if req.ExpiresAtSet && req.Config.ExpiresAt.IsZero() {
		return false, nil, fmt.Errorf("badgerstore: explicit fork expiry is empty: %w", durablestream.ErrBadRequest)
	}
	if err := validateMessageBatch(messages, true, s.maxMessageSize); err != nil {
		return false, nil, err
	}

	normalizedOffset := storage.FormatSimpleOffset(0)
	if req.OffsetSet {
		var err error
		normalizedOffset, _, err = normalizeForkOffset(req.Offset)
		if err != nil {
			return false, nil, err
		}
	}
	gen, err := newGeneration()
	if err != nil {
		return false, nil, err
	}
	now := time.Now()

	var (
		created   bool
		info      *durablestream.StreamInfo
		changes   topologyChanges
		committed error
	)
	commit := func(txn *badger.Txn) error {
		created, info, changes, committed = false, nil, topologyChanges{}, nil
		if err := ctx.Err(); err != nil {
			return err
		}

		existing, targetFound, err := getRecord(txn, targetStreamID)
		if err != nil {
			return err
		}
		if targetFound {
			if existing.isLegacy() {
				return ErrLegacyFormat
			}
			if existing.SoftDeleted {
				return fmt.Errorf("badgerstore: fork target %q is soft-deleted: %w", targetStreamID, durablestream.ErrConflict)
			}
			if !existing.IsExpired() {
				if !forkIdentityMatches(existing, req, normalizedOffset) || !forkTargetConfigMatches(existing, req) {
					return fmt.Errorf("badgerstore: fork target %q exists with different configuration: %w", targetStreamID, durablestream.ErrConflict)
				}
				// Once the equivalent target exists, its generation-fenced parent
				// edge is the durable proof that this request already committed. Do
				// not re-resolve current source visibility here: deleting or expiring
				// the source may retain it only for this target, and must not turn an
				// otherwise safe retry into a conflict or not-found response.
				info, err = s.streamInfoForRecord(txn, targetStreamID, existing)
				return err
			}

			if existing.RefCount > 0 {
				existing.SoftDeleted = true
				if err := setRecord(txn, targetStreamID, existing); err != nil {
					return err
				}
				changes.softened = append(changes.softened, streamGeneration{streamID: targetStreamID, gen: existing.Gen})
				committed = fmt.Errorf("badgerstore: expired fork target %q is retained by children: %w", targetStreamID, durablestream.ErrConflict)
				return nil
			}
			changes, err = removeRecordCascade(txn, targetStreamID, existing)
			if err != nil {
				return err
			}
		}

		source, found, err := getRecord(txn, req.SourceStreamID)
		if err != nil {
			return err
		}
		if !found {
			return durablestream.ErrNotFound
		}
		if source.isLegacy() {
			return ErrLegacyFormat
		}
		if source.SoftDeleted || (source.RefCount > 0 && source.IsExpired()) {
			return fmt.Errorf("badgerstore: fork source %q is soft-deleted: %w", req.SourceStreamID, durablestream.ErrConflict)
		}
		if source.IsExpired() {
			return durablestream.ErrNotFound
		}
		if req.SourceIncarnationID != "" && string(source.Gen) != req.SourceIncarnationID {
			return fmt.Errorf("badgerstore: fork source incarnation changed: %w", durablestream.ErrConflict)
		}
		cfg, err := resolveForkConfig(req, source, now)
		if err != nil {
			return err
		}

		sourceTail, err := streamTailOffset(txn, req.SourceStreamID, source)
		if err != nil {
			return err
		}
		anchor := normalizedOffset
		if !req.OffsetSet {
			anchor = sourceTail
		}
		if anchor.Compare(sourceTail) > 0 {
			return fmt.Errorf("badgerstore: fork offset %q is beyond source tail %q: %w", anchor, sourceTail, durablestream.ErrBadRequest)
		}
		exists, err := visibleOffsetExists(ctx, txn, req.SourceStreamID, source, anchor, sourceTail)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("badgerstore: fork offset %q is not a source boundary: %w", anchor, durablestream.ErrBadRequest)
		}
		materialized, err := materializeSubOffset(ctx, txn, req.SourceStreamID, source, anchor, req.SubOffset)
		if err != nil {
			return err
		}
		_, anchorPosition, err := storage.ParseOffset(anchor)
		if err != nil {
			return err
		}
		localCount := len(materialized) + len(messages)
		if int64(localCount) > math.MaxInt64-anchorPosition {
			return fmt.Errorf("badgerstore: fork offset space exhausted: %w", durablestream.ErrBadRequest)
		}

		if source.RefCount == ^uint64(0) {
			return fmt.Errorf("badgerstore: source fork reference count overflow")
		}
		source.RefCount++
		if err := setRecord(txn, req.SourceStreamID, source); err != nil {
			return err
		}

		target := newStreamRecord(cfg, gen)
		target.Parent = &parentReference{
			StreamID:           req.SourceStreamID,
			Gen:                source.Gen,
			Offset:             anchor,
			OffsetSet:          req.OffsetSet,
			SubOffset:          req.SubOffset,
			ContentTypeSet:     req.ContentTypeSet,
			TTLSet:             req.TTLSet,
			ExpiresAtSet:       req.ExpiresAtSet,
			RequestedTTL:       req.Config.TTL,
			RequestedExpiresAt: req.Config.ExpiresAt,
		}
		if err := setRecord(txn, targetStreamID, target); err != nil {
			return err
		}
		nextPosition := anchorPosition + 1
		if _, err := writeLocalBatch(txn, targetStreamID, gen, nextPosition, materialized); err != nil {
			return fmt.Errorf("badgerstore: write materialized fork prefix: %w", err)
		}
		nextPosition += int64(len(materialized))
		if _, err := writeLocalBatch(txn, targetStreamID, gen, nextPosition, messages); err != nil {
			return fmt.Errorf("badgerstore: write initial fork batch: %w", err)
		}
		finalPosition := anchorPosition + int64(localCount)
		var sequenceValue [8]byte
		binary.BigEndian.PutUint64(sequenceValue[:], uint64(finalPosition))
		if err := txn.Set(seqKey(targetStreamID, gen), sequenceValue[:]); err != nil {
			return fmt.Errorf("badgerstore: initialize fork offset sequence: %w", err)
		}

		created = true
		info = &durablestream.StreamInfo{
			ContentType:   cfg.ContentType,
			NextOffset:    storage.FormatSimpleOffset(finalPosition),
			TTL:           cfg.TTL,
			ExpiresAt:     cfg.ExpiresAt,
			IsPrivate:     cfg.IsPrivate,
			Closed:        cfg.Closed,
			IncarnationID: string(gen),
		}
		return nil
	}

	if err := s.updateWithRetry(commit); err != nil {
		return false, nil, mapTransactionSizeError(err)
	}
	s.publishTopologyChanges(changes)
	if committed != nil {
		return false, nil, committed
	}
	return created, info, nil
}
