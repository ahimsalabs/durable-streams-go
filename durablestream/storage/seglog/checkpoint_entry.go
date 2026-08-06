package seglog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// streamCheckpointEntry is the durable per-stream state embedded in a
// partition checkpoint. The containing Streams map supplies the stream ID.
type streamCheckpointEntry struct {
	IncarnationID string `json:"incarnationId"`

	ContentType string    `json:"contentType,omitempty"`
	TTLNanos    int64     `json:"ttlNanos,omitempty"`
	ExpiresAt   time.Time `json:"expiresAt,omitzero"`
	IsPrivate   bool      `json:"isPrivate,omitempty"`

	Closed      bool                `json:"closed,omitempty"`
	LastSeq     string              `json:"lastSeq,omitempty"`
	Retention   checkpointRetention `json:"retention,omitzero"`
	FloorIndex  int64               `json:"floorIndex,omitempty"`
	SoftDeleted bool                `json:"softDeleted,omitempty"`
	Parent      *checkpointParent   `json:"parent,omitempty"`

	MaterializedThrough int64               `json:"materializedThrough"`
	Sealed              []checkpointSegment `json:"sealed,omitempty"`
	Active              *checkpointActive   `json:"active,omitempty"`
}

type checkpointParent struct {
	StreamID      string   `json:"streamId"`
	IncarnationID string   `json:"incarnationId"`
	Fork          forkMeta `json:"fork"`
}

type checkpointRetention struct {
	MaxBytes    int64 `json:"maxBytes,omitempty"`
	MaxAgeNanos int64 `json:"maxAgeNanos,omitempty"`
}

func retentionCheckpointEntry(r Retention) checkpointRetention {
	return checkpointRetention{MaxBytes: r.MaxBytes, MaxAgeNanos: int64(r.MaxAge)}
}

func (r checkpointRetention) retention() Retention {
	return Retention{MaxBytes: r.MaxBytes, MaxAge: time.Duration(r.MaxAgeNanos)}
}

type checkpointSegment struct {
	File       string `json:"file"`
	FirstIndex int64  `json:"firstIndex"`
	LastIndex  int64  `json:"lastIndex"`
	PayloadEnd int64  `json:"payloadEnd"`
	Count      int64  `json:"count"`
	MaxTS      int64  `json:"maxTsUnixNano,omitempty"`
}

type checkpointActive struct {
	File       string `json:"file"`
	FirstIndex int64  `json:"firstIndex"`
	PayloadEnd int64  `json:"payloadEnd"`
	Count      int64  `json:"count"`
	MaxTS      int64  `json:"maxTsUnixNano,omitempty"`
}

func (e streamCheckpointEntry) config() durablestream.StreamConfig {
	return durablestream.StreamConfig{
		ContentType: e.ContentType,
		TTL:         time.Duration(e.TTLNanos),
		ExpiresAt:   e.ExpiresAt,
		IsPrivate:   e.IsPrivate,
		Closed:      e.Closed,
	}
}

const maxEncodedIDLen = 180

func encodeStreamDirName(streamID string, inc incarnation) string {
	enc := base64.RawURLEncoding.EncodeToString([]byte(streamID))
	if len(enc) > maxEncodedIDLen {
		sum := sha256.Sum256([]byte(streamID))
		enc = "h-" + hex.EncodeToString(sum[:])
	}
	return enc + "-" + hex.EncodeToString(inc[:8])
}

func streamShard(streamID string) string {
	return fmt.Sprintf("%02x", byte(streamHash(streamID)>>56))
}

func streamDir(root, streamID string, inc incarnation) string {
	return filepath.Join(root, "streams", streamShard(streamID), encodeStreamDirName(streamID, inc))
}

func parseIncarnationID(value string) (incarnation, error) {
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != incarnationSize {
		return incarnation{}, fmt.Errorf("seglog: invalid incarnation ID %q", value)
	}
	var inc incarnation
	copy(inc[:], raw)
	return inc, nil
}
