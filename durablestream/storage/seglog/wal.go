package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// walWriter owns one partition's WAL segment files. Only the partition worker
// calls appendGroup and roll; concurrent readers resolve payloads through
// readPayload, which takes the segments lock only to look up a descriptor.
type walWriter struct {
	dir          string
	partition    uint32
	segmentBytes int64
	sync         bool

	// mu guards segments. Segment files are append-only and their descriptors
	// stay open while any walLoc references them (never removed in phase 1).
	mu       sync.RWMutex
	segments map[uint64]*os.File

	active    *os.File // nil until the first append; also present in segments
	activeSeq uint64
	writePos  int64
}

func newWALWriter(dir string, partition uint32, segmentBytes int64, syncWrites bool) *walWriter {
	return &walWriter{
		dir:          dir,
		partition:    partition,
		segmentBytes: segmentBytes,
		sync:         syncWrites,
		segments:     make(map[uint64]*os.File),
	}
}

func walSegmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%016x.log", seq))
}

// adopt installs a recovered segment as part of the writer's set; the last
// adopted segment becomes the active one at writePos.
func (w *walWriter) adopt(seq uint64, f *os.File, writePos int64) {
	w.segments[seq] = f
	w.active = f
	w.activeSeq = seq
	w.writePos = writePos
}

// capacity reports the payload capacity of one segment.
func (w *walWriter) capacity() int64 { return w.segmentBytes - walSegmentHeaderSize }

// appendGroup writes one encoded commit group contiguously and, when sync is
// enabled, fdatasyncs it. It returns the segment sequence and file offset at
// which buf begins. Rolling to a new segment happens first when the group
// does not fit the active one; a group never spans segments.
func (w *walWriter) appendGroup(buf []byte) (seq uint64, base int64, err error) {
	if int64(len(buf)) > w.capacity() {
		// Callers bound frames to one segment; a group that outgrew it is a
		// programmer error upstream, reported rather than split.
		return 0, 0, fmt.Errorf("seglog: commit group of %d bytes exceeds segment capacity %d", len(buf), w.capacity())
	}
	if w.active == nil || w.writePos+int64(len(buf)) > w.segmentBytes {
		if err := w.roll(); err != nil {
			return 0, 0, err
		}
	}
	base = w.writePos
	if _, err := w.active.WriteAt(buf, base); err != nil {
		return 0, 0, fmt.Errorf("seglog: write WAL group: %w", err)
	}
	if w.sync {
		if err := fdatasync(w.active); err != nil {
			return 0, 0, fmt.Errorf("seglog: sync WAL group: %w", err)
		}
	}
	w.writePos = base + int64(len(buf))
	return w.activeSeq, base, nil
}

// roll creates the next segment file: header written and fsync'd, file
// preallocated, directory entry made durable before any frame lands in it.
func (w *walWriter) roll() error {
	seq := w.activeSeq + 1
	if err := os.MkdirAll(w.dir, 0o755); err != nil {
		return fmt.Errorf("seglog: create WAL dir: %w", err)
	}
	f, err := os.OpenFile(walSegmentPath(w.dir, seq), os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("seglog: create WAL segment: %w", err)
	}
	if err := w.initSegment(f, seq); err != nil {
		_ = f.Close()
		return err
	}
	w.mu.Lock()
	w.segments[seq] = f
	w.mu.Unlock()
	w.active = f
	w.activeSeq = seq
	w.writePos = walSegmentHeaderSize
	return nil
}

// initSegment preallocates f and writes its durable header.
func (w *walWriter) initSegment(f *os.File, seq uint64) error {
	if err := preallocate(f, w.segmentBytes); err != nil {
		return fmt.Errorf("seglog: preallocate WAL segment: %w", err)
	}
	hdr := encodeWALSegmentHeader(w.partition, seq, time.Now().UnixNano())
	if _, err := f.WriteAt(hdr, 0); err != nil {
		return fmt.Errorf("seglog: write WAL segment header: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("seglog: sync WAL segment header: %w", err)
	}
	if err := syncDir(w.dir); err != nil {
		return fmt.Errorf("seglog: sync WAL dir: %w", err)
	}
	return nil
}

// errWALSegmentGone reports a read of a WAL segment that was reclaimed after
// materialization. Readers holding a stale snapshot re-snapshot and retry:
// the data is now served from stream segments.
var errWALSegmentGone = errors.New("seglog: WAL segment reclaimed")

// readPayload copies one committed payload into a fresh buffer, verifying its
// checksum. It is safe concurrently with appends: committed bytes are never
// rewritten.
func (w *walWriter) readPayload(loc walLoc) ([]byte, error) {
	w.mu.RLock()
	f := w.segments[loc.segmentSeq]
	w.mu.RUnlock()
	if f == nil {
		return nil, errWALSegmentGone
	}
	buf := make([]byte, int(loc.length)+4)
	if _, err := f.ReadAt(buf, loc.off); err != nil {
		if errors.Is(err, os.ErrClosed) {
			return nil, errWALSegmentGone
		}
		return nil, fmt.Errorf("seglog: read WAL payload: %w", err)
	}
	payload := buf[:loc.length]
	if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(buf[loc.length:]) {
		return nil, fmt.Errorf("seglog: WAL payload checksum mismatch in segment %d at %d", loc.segmentSeq, loc.off)
	}
	return payload, nil
}

// removeBefore closes and unlinks every segment with a sequence below keep.
// The caller guarantees every frame in those segments is reflected in durable
// manifests and the checkpoint; concurrent readers with stale snapshots get
// errWALSegmentGone and retry against stream segments.
func (w *walWriter) removeBefore(keep uint64) error {
	w.mu.Lock()
	var victims []uint64
	for seq := range w.segments {
		if seq < keep {
			victims = append(victims, seq)
		}
	}
	for _, seq := range victims {
		_ = w.segments[seq].Close()
		delete(w.segments, seq)
	}
	w.mu.Unlock()

	var firstErr error
	for _, seq := range victims {
		if err := os.Remove(walSegmentPath(w.dir, seq)); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("seglog: unlink WAL segment %d: %w", seq, err)
		}
	}
	if len(victims) > 0 && firstErr == nil {
		firstErr = syncDir(w.dir)
	}
	return firstErr
}

// close closes every segment descriptor.
func (w *walWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var firstErr error
	for seq, f := range w.segments {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("seglog: close WAL segment %d: %w", seq, err)
		}
		delete(w.segments, seq)
	}
	w.active = nil
	return firstErr
}
