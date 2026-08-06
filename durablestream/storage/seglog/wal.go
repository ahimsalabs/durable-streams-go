package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const walExtentBytes int64 = DefaultWALExtentBytes

// walWriter owns one partition's WAL segment files. Only the partition stager
// calls writeGroup and roll; the committer syncs the exact returned file while
// concurrent readers resolve payloads through readPayload.
type walWriter struct {
	dir          string
	partition    uint32
	segmentBytes int64
	extentBytes  int64
	sync         bool

	// mu guards segments. Segment files are append-only and their descriptors
	// stay open while any walLoc references them (never removed in phase 1).
	mu            sync.RWMutex
	segments      map[uint64]*os.File
	logicalBytes  map[uint64]int64
	retainedBytes atomic.Int64
	activeBytes   atomic.Int64

	active    *os.File // nil until the first append; also present in segments
	activeSeq uint64
	writePos  int64
	extentEnd int64

	// writeHook and syncHook are one-shot test seams for a failure or blocked
	// operation. Production code never installs either hook; taking one consumes
	// exactly one operation and is safe even if a test races the worker.
	failMu    sync.Mutex
	writeHook func() error
	syncHook  func() error
}

func newWALWriter(dir string, partition uint32, segmentBytes, extentBytes int64, syncWrites bool) *walWriter {
	return &walWriter{
		dir:          dir,
		partition:    partition,
		segmentBytes: segmentBytes,
		extentBytes:  extentBytes,
		sync:         syncWrites,
		segments:     make(map[uint64]*os.File),
		logicalBytes: make(map[uint64]int64),
	}
}

func walSegmentPath(dir string, seq uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal-%016x.log", seq))
}

// adopt installs a recovered segment as part of the writer's set; the last
// adopted segment becomes the active one at writePos.
func (w *walWriter) adopt(seq uint64, f *os.File, writePos int64) {
	w.installRecoveredSegment(seq, f, writePos)
	w.active = f
	w.activeSeq = seq
	w.writePos = writePos
	w.activeBytes.Store(writePos)
	if info, err := f.Stat(); err == nil {
		w.extentEnd = info.Size()
	}
}

// installRecoveredSegment records one recovered segment's valid logical end.
// Recovery calls it before workers start, but the lock keeps Stats snapshots
// consistent with later reclamation.
func (w *walWriter) installRecoveredSegment(seq uint64, f *os.File, writePos int64) {
	w.mu.Lock()
	w.segments[seq] = f
	w.logicalBytes[seq] = writePos
	w.mu.Unlock()
	w.retainedBytes.Add(writePos)
}

// capacity reports the payload capacity of one segment.
func (w *walWriter) capacity() int64 { return w.segmentBytes - walSegmentHeaderSize }

// failNextWrite causes the next writeGroup call to fail before touching the
// active segment. It exists solely for deterministic fail-stop tests.
func (w *walWriter) failNextWrite(err error) {
	w.failMu.Lock()
	w.writeHook = func() error { return err }
	w.failMu.Unlock()
}

// blockNextWrite holds the next writeGroup before it touches the file. It
// exists solely to prove staging overlap in deterministic pipeline tests.
func (w *walWriter) blockNextWrite(started chan<- struct{}, release <-chan struct{}) {
	w.failMu.Lock()
	w.writeHook = func() error {
		close(started)
		<-release
		return nil
	}
	w.failMu.Unlock()
}

func (w *walWriter) takeWriteHook() func() error {
	w.failMu.Lock()
	defer w.failMu.Unlock()
	hook := w.writeHook
	w.writeHook = nil
	return hook
}

// failNextSync causes the next syncSegment call to fail before fdatasync. It
// exists solely for deterministic pipeline fail-stop tests.
func (w *walWriter) failNextSync(err error) {
	w.failMu.Lock()
	w.syncHook = func() error { return err }
	w.failMu.Unlock()
}

// blockNextSync holds the next syncSegment call after reporting that it
// started. It exists solely for deterministic pipeline ordering tests.
func (w *walWriter) blockNextSync(started chan<- struct{}, release <-chan struct{}) {
	w.failMu.Lock()
	w.syncHook = func() error {
		close(started)
		<-release
		return nil
	}
	w.failMu.Unlock()
}

func (w *walWriter) takeSyncHook() func() error {
	w.failMu.Lock()
	defer w.failMu.Unlock()
	hook := w.syncHook
	w.syncHook = nil
	return hook
}

// writeGroup writes one encoded commit group contiguously without syncing it.
// It returns the segment sequence, file offset, and exact file containing the
// group. Rolling happens first when the group does not fit; a group never spans
// segments. The partition committer later syncs the returned file.
func (w *walWriter) writeGroup(buf []byte) (seq uint64, base int64, file *os.File, err error) {
	if hook := w.takeWriteHook(); hook != nil {
		if err := hook(); err != nil {
			return 0, 0, nil, fmt.Errorf("seglog: injected WAL write failure: %w", err)
		}
	}
	if int64(len(buf)) > w.capacity() {
		// Callers bound frames to one segment; a group that outgrew it is a
		// programmer error upstream, reported rather than split.
		return 0, 0, nil, fmt.Errorf("seglog: commit group of %d bytes exceeds segment capacity %d", len(buf), w.capacity())
	}
	if w.active == nil || w.writePos+int64(len(buf)) > w.segmentBytes {
		if err := w.roll(); err != nil {
			return 0, 0, nil, err
		}
	}
	base = w.writePos
	if err := w.grow(base + int64(len(buf))); err != nil {
		return 0, 0, nil, err
	}
	if _, err := w.active.WriteAt(buf, base); err != nil {
		return 0, 0, nil, fmt.Errorf("seglog: write WAL group: %w", err)
	}
	written := int64(len(buf))
	w.writePos = base + written
	w.mu.Lock()
	w.logicalBytes[w.activeSeq] += written
	w.mu.Unlock()
	w.retainedBytes.Add(written)
	w.activeBytes.Add(written)
	return w.activeSeq, base, w.active, nil
}

// syncSegment establishes the commit point for one group. The file is the
// exact descriptor returned by writeGroup, so a concurrent stager roll cannot
// redirect an older group's flush to the new active segment.
func (w *walWriter) syncSegment(file *os.File) error {
	if !w.sync || file == nil {
		return nil
	}
	if hook := w.takeSyncHook(); hook != nil {
		if err := hook(); err != nil {
			return fmt.Errorf("seglog: injected WAL sync failure: %w", err)
		}
	}
	if err := fdatasync(file); err != nil {
		return fmt.Errorf("seglog: sync WAL group: %w", err)
	}
	return nil
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
	w.logicalBytes[seq] = walSegmentHeaderSize
	w.mu.Unlock()
	w.retainedBytes.Add(walSegmentHeaderSize)
	w.active = f
	w.activeSeq = seq
	w.writePos = walSegmentHeaderSize
	w.activeBytes.Store(walSegmentHeaderSize)
	w.extentEnd = walExtentEnd(w.writePos, w.segmentBytes, w.extentBytes)
	return nil
}

// initSegment preallocates the first extent and writes its durable header.
func (w *walWriter) initSegment(f *os.File, seq uint64) error {
	if err := preallocate(f, walExtentEnd(walSegmentHeaderSize, w.segmentBytes, w.extentBytes)); err != nil {
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

func walExtentEnd(pos, segmentBytes, extentBytes int64) int64 {
	if pos >= segmentBytes {
		return segmentBytes
	}
	remainder := pos % extentBytes
	if remainder == 0 {
		return pos
	}
	delta := extentBytes - remainder
	if delta > segmentBytes-pos {
		return segmentBytes
	}
	return pos + delta
}

// grow reserves all bytes that the next write can touch. The logical segment
// cap remains segmentBytes even though physical allocation happens in extents.
func (w *walWriter) grow(requiredEnd int64) error {
	if requiredEnd <= w.extentEnd {
		return nil
	}
	end := walExtentEnd(requiredEnd, w.segmentBytes, w.extentBytes)
	if err := preallocate(w.active, end); err != nil {
		return fmt.Errorf("seglog: grow WAL segment to %d bytes: %w", end, err)
	}
	w.extentEnd = end
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
// the checkpoint; concurrent readers with stale snapshots get
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
	}
	w.mu.Unlock()

	var firstErr error
	removed := 0
	for _, seq := range victims {
		err := os.Remove(walSegmentPath(w.dir, seq))
		if err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = fmt.Errorf("seglog: unlink WAL segment %d: %w", seq, err)
			}
			continue
		}
		w.mu.Lock()
		removedBytes := w.logicalBytes[seq]
		delete(w.segments, seq)
		delete(w.logicalBytes, seq)
		w.mu.Unlock()
		w.retainedBytes.Add(-removedBytes)
		removed++
	}
	if removed > 0 {
		if err := syncDir(w.dir); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

type walUsage struct {
	retainedBytes        int64
	activeBytes          int64
	segmentCapacityBytes int64
}

func (w *walWriter) usageSnapshot() walUsage {
	active := w.activeBytes.Load()
	capacity := int64(0)
	if active > 0 {
		capacity = w.segmentBytes
	}
	return walUsage{
		retainedBytes:        w.retainedBytes.Load(),
		activeBytes:          active,
		segmentCapacityBytes: capacity,
	}
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
