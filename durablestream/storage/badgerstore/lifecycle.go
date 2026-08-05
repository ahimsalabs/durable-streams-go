package badgerstore

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

type streamGeneration struct {
	streamID string
	gen      generation
}

type topologyChanges struct {
	removed  []streamGeneration
	softened []streamGeneration
}

func setRecord(txn *badger.Txn, streamID string, rec streamRecord) error {
	encoded, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("badgerstore: marshal stream record: %w", err)
	}
	if err := txn.Set(configKey(streamID), encoded); err != nil {
		return fmt.Errorf("badgerstore: set stream record: %w", err)
	}
	return nil
}

// removeRecordCascade physically removes rec, releases its immediate parent
// reference, and recursively removes retained ancestors whose final reference
// disappears. All topology updates and tombstones commit in one transaction,
// so a crash cannot leave an uncounted child or prematurely reclaimed parent.
func removeRecordCascade(txn *badger.Txn, streamID string, rec streamRecord) (topologyChanges, error) {
	changes := topologyChanges{}
	seen := make(map[string]struct{})
	for depth := 0; ; depth++ {
		if depth > maxLineageDepth {
			return topologyChanges{}, fmt.Errorf("badgerstore: removal cascade exceeds %d generations", maxLineageDepth)
		}
		key := streamID + "\x00" + string(rec.Gen)
		if _, duplicate := seen[key]; duplicate {
			return topologyChanges{}, fmt.Errorf("badgerstore: cycle in removal cascade at %q", streamID)
		}
		seen[key] = struct{}{}

		if err := txn.Delete(configKey(streamID)); err != nil {
			return topologyChanges{}, fmt.Errorf("badgerstore: delete config for %q: %w", streamID, err)
		}
		if err := txn.Delete(lastSeqKey(streamID)); err != nil {
			return topologyChanges{}, fmt.Errorf("badgerstore: delete last sequence for %q: %w", streamID, err)
		}
		if err := txn.Set(tombstoneKey(streamID, rec.Gen), nil); err != nil {
			return topologyChanges{}, fmt.Errorf("badgerstore: tombstone %q: %w", streamID, err)
		}
		changes.removed = append(changes.removed, streamGeneration{streamID: streamID, gen: rec.Gen})

		if rec.Parent == nil {
			return changes, nil
		}
		parentID, parentGen := rec.Parent.StreamID, rec.Parent.Gen
		parent, found, err := getRecord(txn, parentID)
		if err != nil {
			return topologyChanges{}, err
		}
		if !found || parent.isLegacy() || parent.Gen != parentGen {
			return topologyChanges{}, fmt.Errorf("badgerstore: %q references missing parent incarnation %q/%s", streamID, parentID, parentGen)
		}
		if parent.RefCount == 0 {
			return topologyChanges{}, fmt.Errorf("badgerstore: parent %q has zero references while child %q exists", parentID, streamID)
		}
		parent.RefCount--
		if parent.RefCount == 0 && (parent.SoftDeleted || parent.IsExpired()) {
			streamID, rec = parentID, parent
			continue
		}
		if err := setRecord(txn, parentID, parent); err != nil {
			return topologyChanges{}, err
		}
		return changes, nil
	}
}

func (s *Storage) publishTopologyChanges(changes topologyChanges) {
	for _, stream := range changes.removed {
		s.forgetStream(stream.streamID, stream.gen)
	}
	for _, stream := range changes.softened {
		if state, ok := s.streams.Load(streamStateKey(stream.streamID, stream.gen)); ok {
			state.wake()
		}
	}
	if len(changes.removed) > 0 {
		s.signalReaper()
	}
}
