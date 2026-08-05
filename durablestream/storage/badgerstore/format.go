package badgerstore

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

// validatePersistedFormat validates persisted metadata before Storage starts any
// background work. In addition to refusing records from older versions, it
// rejects broken generation-fenced topology rather than letting cleanup turn
// durable corruption into data loss. New closes the database on every error,
// leaving all keys available to an explicit repair or migration tool.
func validatePersistedFormat(db *badger.DB) error {
	return db.View(func(txn *badger.Txn) error {
		records := make(map[string]streamRecord)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefixConfig)
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
			item := it.Item()
			streamID := string(item.Key()[len(prefixConfig):])
			var rec streamRecord
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &rec)
			}); err != nil {
				it.Close()
				return fmt.Errorf("badgerstore: inspect stream record %q before open: %w", streamID, err)
			}
			if rec.isLegacy() {
				it.Close()
				return fmt.Errorf(
					"badgerstore: stream %q uses record format %d; require %d with lineage and batch metadata: %w",
					streamID, rec.FormatVersion, currentRecordFormatVersion, ErrLegacyFormat,
				)
			}
			records[streamID] = rec
		}
		it.Close()

		if err := validateRecordTopology(records); err != nil {
			return fmt.Errorf("badgerstore: invalid persisted stream topology: %w", err)
		}

		// An edge beyond its exact parent's logical tail could expose gaps or
		// cause later local messages to occupy an invalid offset space. Reverse
		// prefix seeks make this check proportional to the number of forks, not
		// to the number of stored messages.
		for streamID, rec := range records {
			if rec.Parent == nil {
				continue
			}
			parent := records[rec.Parent.StreamID]
			tail, err := streamTailOffset(txn, rec.Parent.StreamID, parent)
			if err != nil {
				return fmt.Errorf("fork %q: read parent tail: %w", streamID, err)
			}
			if rec.Parent.Offset.Compare(tail) > 0 {
				return fmt.Errorf("fork %q has parent offset %q beyond %q tail %q", streamID, rec.Parent.Offset, rec.Parent.StreamID, tail)
			}
		}
		return nil
	})
}

func validateRecordTopology(records map[string]streamRecord) error {
	incoming := make(map[string]uint64, len(records))
	for streamID, rec := range records {
		if streamID == "" || strings.Contains(streamID, ":") {
			return fmt.Errorf("invalid stream ID %q in config key", streamID)
		}
		if !validPersistedGeneration(rec.Gen) {
			return fmt.Errorf("stream %q has invalid generation %q", streamID, rec.Gen)
		}
		if rec.SoftDeleted && rec.RefCount == 0 {
			return fmt.Errorf("soft-deleted stream %q has no child references", streamID)
		}
		parent := rec.Parent
		if parent == nil {
			continue
		}
		if parent.StreamID == "" || strings.Contains(parent.StreamID, ":") || parent.StreamID == streamID {
			return fmt.Errorf("fork %q has invalid parent ID %q", streamID, parent.StreamID)
		}
		if !validPersistedGeneration(parent.Gen) {
			return fmt.Errorf("fork %q has invalid parent generation %q", streamID, parent.Gen)
		}
		readSeq, position, err := storage.ParseOffset(parent.Offset)
		if err != nil || readSeq != 0 || parent.Offset != storage.FormatSimpleOffset(position) {
			return fmt.Errorf("fork %q has invalid parent offset %q", streamID, parent.Offset)
		}
		if parent.SubOffset > 0 && !parent.OffsetSet {
			return fmt.Errorf("fork %q has a sub-offset without an explicit offset", streamID)
		}
		if parent.TTLSet && parent.ExpiresAtSet {
			return fmt.Errorf("fork %q records both TTL and absolute expiry", streamID)
		}
		if parent.ContentTypeSet && rec.ContentType == "" {
			return fmt.Errorf("fork %q records an explicit empty content type", streamID)
		}
		if parent.TTLSet && parent.RequestedTTL < 0 {
			return fmt.Errorf("fork %q records a negative TTL", streamID)
		}
		if parent.ExpiresAtSet && parent.RequestedExpiresAt.IsZero() {
			return fmt.Errorf("fork %q records an empty explicit expiry", streamID)
		}

		persistedParent, ok := records[parent.StreamID]
		if !ok {
			return fmt.Errorf("fork %q references missing parent %q", streamID, parent.StreamID)
		}
		if persistedParent.Gen != parent.Gen {
			return fmt.Errorf("fork %q references stale parent generation %q/%s", streamID, parent.StreamID, parent.Gen)
		}
		incoming[parent.StreamID]++
	}

	for streamID, rec := range records {
		if rec.RefCount != incoming[streamID] {
			return fmt.Errorf("stream %q reference count is %d; topology has %d direct children", streamID, rec.RefCount, incoming[streamID])
		}
	}

	// Every record has at most one parent, so a three-color DFS detects both
	// cycles and pathological depth without materializing adjacency lists.
	const (
		unvisited = iota
		visiting
		visited
	)
	colors := make(map[string]int, len(records))
	var visit func(string, int) error
	visit = func(streamID string, depth int) error {
		if depth > maxLineageDepth {
			return fmt.Errorf("fork lineage at %q exceeds %d generations", streamID, maxLineageDepth)
		}
		switch colors[streamID] {
		case visiting:
			return fmt.Errorf("fork lineage contains a cycle at %q", streamID)
		case visited:
			return nil
		}
		colors[streamID] = visiting
		if parent := records[streamID].Parent; parent != nil {
			if err := visit(parent.StreamID, depth+1); err != nil {
				return err
			}
		}
		colors[streamID] = visited
		return nil
	}
	for streamID := range records {
		if colors[streamID] == unvisited {
			if err := visit(streamID, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func validPersistedGeneration(gen generation) bool {
	if len(gen) != 32 {
		return false
	}
	_, err := hex.DecodeString(string(gen))
	return err == nil
}
