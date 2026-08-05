package badgerstore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// Key prefixes for different data types within a stream.
//
// Message and sequence keys are scoped by generation so that data belonging to
// a deleted stream can never be confused with (or destroyed by) data belonging
// to a stream later created with the same ID. See [generation].
const (
	prefixConfig    = "c:" // c:{streamID} -> JSON-encoded streamRecord
	prefixLastSeq   = "q:" // q:{streamID} -> last sequence number (for dedup)
	prefixMessage   = "m:" // m:{streamID}:{generation}:{offset} -> message data
	prefixBatch     = "b:" // b:{streamID}:{generation}:{startOffset} -> end offset
	prefixSeq       = "s:" // s:{streamID}:{generation} -> Badger sequence for offset generation
	prefixTombstone = "t:" // t:{streamID}:{generation} -> generation awaiting purge (empty value)
)

// generation identifies one incarnation of a stream ID. A fresh generation is
// assigned every time a stream is created, so message keys written by one
// incarnation are disjoint from those of every other incarnation. This makes
// the asynchronous purge of a deleted stream safe: a purge is scoped to a
// single generation and can never touch data written after the delete.
//
// The empty generation identifies streams written by versions of this package
// that predate generation scoping. New rejects a directory containing one and
// preserves its bytes for explicit migration.
type generation string

// newGeneration returns a fresh, unique generation identifier.
func newGeneration() (generation, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("badgerstore: generate stream generation: %w", err)
	}
	return generation(hex.EncodeToString(buf[:])), nil
}

func configKey(streamID string) []byte {
	return []byte(prefixConfig + streamID)
}

func lastSeqKey(streamID string) []byte {
	return []byte(prefixLastSeq + streamID)
}

// messagePrefix returns the key prefix covering every message of one generation.
func messagePrefix(streamID string, gen generation) []byte {
	return []byte(prefixMessage + streamID + ":" + string(gen) + ":")
}

func messageKey(streamID string, gen generation, offset durablestream.Offset) []byte {
	return []byte(prefixMessage + streamID + ":" + string(gen) + ":" + offset.String())
}

// batchPrefix covers the append-batch boundaries for one generation. Boundary
// metadata is required to interpret JSON fork sub-offsets: flattened messages
// remain individually readable, while the batch key records which messages
// came from one atomic append.
func batchPrefix(streamID string, gen generation) []byte {
	return []byte(prefixBatch + streamID + ":" + string(gen) + ":")
}

func batchKey(streamID string, gen generation, start durablestream.Offset) []byte {
	return []byte(prefixBatch + streamID + ":" + string(gen) + ":" + start.String())
}

func seqKey(streamID string, gen generation) []byte {
	return []byte(prefixSeq + streamID + ":" + string(gen))
}

func tombstoneKey(streamID string, gen generation) []byte {
	return []byte(prefixTombstone + streamID + ":" + string(gen))
}

// splitScopedKey splits a generation-scoped key into its stream ID and
// generation. It reports false for keys that do not have the expected shape,
// including keys written by versions that predate generation scoping.
//
// segments is the number of ':'-separated segments after the prefix: 2 for
// sequence and tombstone keys ({streamID}:{generation}), 3 for message and
// batch-boundary keys ({streamID}:{generation}:{offset}). Stream IDs may not
// contain ':' (see validateStreamID), so the split is unambiguous.
func splitScopedKey(prefix string, key []byte, segments int) (streamID string, gen generation, ok bool) {
	rest, found := strings.CutPrefix(string(key), prefix)
	if !found {
		return "", "", false
	}
	parts := strings.Split(rest, ":")
	if len(parts) != segments {
		return "", "", false
	}
	if parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], generation(parts[1]), true
}
