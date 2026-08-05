package seglog

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// manifest is the durable per-stream record written only by the materializer
// (and read by recovery). It describes the materialized prefix and the
// stream's metadata as of its last flush; the WAL suffix past the partition
// checkpoint overlays everything newer. A manifest may run ahead of the
// checkpoint but never behind it.
type manifest struct {
	FormatVersion int    `json:"formatVersion"`
	StreamID      string `json:"streamId"`
	IncarnationID string `json:"incarnationId"`

	ContentType string    `json:"contentType,omitempty"`
	TTLNanos    int64     `json:"ttlNanos,omitempty"`
	ExpiresAt   time.Time `json:"expiresAt,omitzero"`
	IsPrivate   bool      `json:"isPrivate,omitempty"`

	Closed     bool              `json:"closed,omitempty"`
	LastSeq    string            `json:"lastSeq,omitempty"`
	Retention  manifestRetention `json:"retention,omitzero"`
	FloorIndex int64             `json:"floorIndex,omitempty"`

	// MaterializedThrough is the highest logical index present in segments.
	MaterializedThrough int64 `json:"materializedThrough"`

	Sealed []manifestSegment `json:"sealed,omitempty"`
	Active *manifestActive   `json:"active,omitempty"`
}

type manifestRetention struct {
	MaxBytes    int64 `json:"maxBytes,omitempty"`
	MaxAgeNanos int64 `json:"maxAgeNanos,omitempty"`
}

func retentionManifest(r Retention) manifestRetention {
	return manifestRetention{MaxBytes: r.MaxBytes, MaxAgeNanos: int64(r.MaxAge)}
}

func (r manifestRetention) retention() Retention {
	return Retention{MaxBytes: r.MaxBytes, MaxAge: time.Duration(r.MaxAgeNanos)}
}

type manifestSegment struct {
	File       string `json:"file"`
	FirstIndex int64  `json:"firstIndex"`
	LastIndex  int64  `json:"lastIndex"`
	Bytes      int64  `json:"bytes"` // absolute end of the record area
	MaxTS      int64  `json:"maxTsUnixNano,omitempty"`
}

type manifestActive struct {
	File       string `json:"file"`
	FirstIndex int64  `json:"firstIndex"`
	Bytes      int64  `json:"bytes"` // absolute end of the record area
}

const manifestFormatVersion = 1

func (m manifest) config() durablestream.StreamConfig {
	return durablestream.StreamConfig{
		ContentType: m.ContentType,
		TTL:         time.Duration(m.TTLNanos),
		ExpiresAt:   m.ExpiresAt,
		IsPrivate:   m.IsPrivate,
		Closed:      m.Closed,
	}
}

// maxEncodedIDLen bounds the base64 form of a stream ID used directly as a
// directory name; longer IDs switch to a hash form, with the authoritative ID
// kept in the manifest.
const maxEncodedIDLen = 180

// encodeStreamDirName returns the directory base name for a stream
// incarnation: an encoding of the ID plus a 16-hex-char incarnation suffix,
// so successive incarnations never share a path.
func encodeStreamDirName(streamID string, inc incarnation) string {
	enc := base64.RawURLEncoding.EncodeToString([]byte(streamID))
	if len(enc) > maxEncodedIDLen {
		sum := sha256.Sum256([]byte(streamID))
		enc = "h-" + hex.EncodeToString(sum[:])
	}
	return enc + "-" + hex.EncodeToString(inc[:8])
}

// streamShard returns the two-hex-character shard directory for a stream.
func streamShard(streamID string) string {
	return fmt.Sprintf("%02x", byte(streamHash(streamID)>>56))
}

// streamDir returns the directory of one stream incarnation under root.
func streamDir(root, streamID string, inc incarnation) string {
	return filepath.Join(root, "streams", streamShard(streamID), encodeStreamDirName(streamID, inc))
}

const manifestFileName = "manifest.json"

// writeManifest atomically persists m into dir (which must exist).
func writeManifest(dir string, m manifest) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("seglog: encode manifest: %w", err)
	}
	if err := atomicWrite(filepath.Join(dir, manifestFileName), data, 0o644); err != nil {
		return fmt.Errorf("seglog: write manifest: %w", err)
	}
	return nil
}

// loadManifest reads and validates a stream directory's manifest.
func loadManifest(dir string) (manifest, error) {
	raw, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return manifest{}, fmt.Errorf("seglog: decode manifest in %s: %w", dir, err)
	}
	if m.FormatVersion != manifestFormatVersion {
		return manifest{}, fmt.Errorf("seglog: manifest in %s has unsupported version %d", dir, m.FormatVersion)
	}
	if m.StreamID == "" || len(m.IncarnationID) != 2*incarnationSize {
		return manifest{}, fmt.Errorf("seglog: manifest in %s is missing identity", dir)
	}
	return m, nil
}

// parseIncarnationID decodes the hex incarnation identity from a manifest.
func parseIncarnationID(s string) (incarnation, error) {
	raw, err := hex.DecodeString(s)
	if err != nil || len(raw) != incarnationSize {
		return incarnation{}, fmt.Errorf("seglog: invalid incarnation ID %q", s)
	}
	var inc incarnation
	copy(inc[:], raw)
	return inc, nil
}
