package seglog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// errCorrupt marks unrecoverable damage. New fails and leaves every byte
// intact rather than guess at durable history.
var errCorrupt = errors.New("seglog: WAL corruption")

// identityKey names one exact stream incarnation.
type identityKey struct {
	id  string
	inc incarnation
}

// recoveredDir is one stream directory with a valid manifest.
type recoveredDir struct {
	key identityKey
	dir string
	st  *streamState
}

// recoveryScan is the result of the manifest pass.
type recoveryScan struct {
	dirs       []recoveredDir
	byIdentity map[identityKey]*streamState
}

// recoverAll rebuilds the in-memory catalog and every partition's writer
// position: load manifests, replay each partition's WAL suffix past its
// checkpoint, then sweep stream directories that are no longer live. It runs
// before any worker starts, so state mutation needs no locks.
func (s *Storage) recoverAll() error {
	scan, err := s.scanStreamDirs()
	if err != nil {
		return err
	}
	// Seed the catalog. When a crash left two incarnation directories for
	// one stream, either may seed; the surviving create frame (necessarily
	// past the checkpoint in that situation) corrects the choice during
	// replay, and the sweep removes the loser's directory.
	for _, d := range scan.dirs {
		s.streams.Store(d.key.id, d.st)
	}
	for _, p := range s.parts {
		if err := s.recoverPartition(p, scan); err != nil {
			return err
		}
	}
	s.sweepStreamDirs(scan)
	// Replay may have advanced logical state beyond its manifest. Mark every
	// survivor dirty before workers start so the first checkpoint cannot move
	// past replayed mutations until their derived state is durable.
	s.streams.Range(func(_ string, st *streamState) bool {
		s.parts[st.partition].markDirty(st)
		return true
	})
	return nil
}

// scanStreamDirs loads every stream directory's manifest. A directory without
// a manifest is a partial materializer artifact (the checkpoint never
// advances past an incomplete manifest write) and is removed; an undecodable
// manifest is corruption and fails open.
func (s *Storage) scanStreamDirs() (*recoveryScan, error) {
	scan := &recoveryScan{byIdentity: make(map[identityKey]*streamState)}
	root := filepath.Join(s.dir, "streams")
	shards, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return scan, nil
	}
	if err != nil {
		return nil, fmt.Errorf("seglog: read streams dir: %w", err)
	}
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		shardPath := filepath.Join(root, shard.Name())
		entries, err := os.ReadDir(shardPath)
		if err != nil {
			return nil, fmt.Errorf("seglog: read shard dir: %w", err)
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			dir := filepath.Join(shardPath, e.Name())
			m, err := loadManifest(dir)
			if os.IsNotExist(err) {
				s.opts.SLogger.Warn("seglog: removing stream directory without manifest", "dir", dir)
				if err := os.RemoveAll(dir); err != nil {
					return nil, fmt.Errorf("seglog: remove partial stream dir: %w", err)
				}
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("%w: %v", errCorrupt, err)
			}
			if err := removeOrphanSegments(dir, m); err != nil {
				return nil, err
			}
			st, err := s.stateFromManifest(dir, m)
			if err != nil {
				return nil, err
			}
			key := identityKey{id: st.id, inc: st.inc}
			scan.dirs = append(scan.dirs, recoveredDir{key: key, dir: dir, st: st})
			scan.byIdentity[key] = st
		}
	}
	return scan, nil
}

// removeOrphanSegments finishes a trim interrupted after its manifest rewrite
// but before unlink. The manifest is authoritative derived state, so an
// unreferenced segment cannot contain live records.
func removeOrphanSegments(dir string, m manifest) error {
	referenced := make(map[string]struct{}, len(m.Sealed)+1)
	for _, seg := range m.Sealed {
		referenced[seg.File] = struct{}{}
	}
	if m.Active != nil {
		referenced[m.Active.File] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("seglog: read stream dir for orphan cleanup: %w", err)
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "seg-") || !strings.HasSuffix(name, ".seg") {
			continue
		}
		if _, ok := referenced[name]; ok {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("seglog: remove orphan segment %s: %w", name, err)
		}
		removed = true
	}
	if removed {
		return syncDir(dir)
	}
	return nil
}

// stateFromManifest opens a manifest's segments and rebuilds the stream's
// materialized state.
func (s *Storage) stateFromManifest(dir string, m manifest) (*streamState, error) {
	inc, err := parseIncarnationID(m.IncarnationID)
	if err != nil {
		return nil, fmt.Errorf("%w: manifest in %s: %v", errCorrupt, dir, err)
	}
	part := uint32(streamHash(m.StreamID) % uint64(len(s.parts)))
	st := newStreamState(m.StreamID, inc, part, m.config())
	st.closed = m.Closed
	st.lastSeq = m.LastSeq
	st.retention = m.Retention.retention()
	st.floor = m.FloorIndex
	st.nextIndex = m.MaterializedThrough + 1
	st.firstLive = m.MaterializedThrough + 1
	st.materializedThrough = m.MaterializedThrough

	if st.retention.MaxBytes < 0 || st.retention.MaxAge < 0 || st.floor < 0 || st.floor > m.MaterializedThrough {
		return nil, fmt.Errorf("%w: manifest in %s has invalid retention state", errCorrupt, dir)
	}
	prevLast := st.floor
	for _, ms := range m.Sealed {
		sf, err := openSealedSegment(filepath.Join(dir, ms.File), ms.File, inc)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCorrupt, err)
		}
		if sf.firstIndex != ms.FirstIndex || sf.lastIndex != ms.LastIndex || sf.bytes != ms.Bytes ||
			sf.firstIndex != prevLast+1 {
			return nil, fmt.Errorf("%w: sealed segment %s disagrees with manifest in %s", errCorrupt, ms.File, dir)
		}
		prevLast = sf.lastIndex
		st.sealed = append(st.sealed, sf)
	}
	if m.Active != nil {
		sf, err := openActiveSegment(filepath.Join(dir, m.Active.File), m.Active.File, inc, m.Active.Bytes, s.opts.SparseIndexBytes)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCorrupt, err)
		}
		if sf.firstIndex != m.Active.FirstIndex || sf.firstIndex != prevLast+1 ||
			sf.lastIndex != m.MaterializedThrough {
			return nil, fmt.Errorf("%w: active segment %s disagrees with manifest in %s", errCorrupt, m.Active.File, dir)
		}
		st.activeSeg = sf
		st.activeView = sf.view()
	} else if len(m.Sealed) > 0 && prevLast != m.MaterializedThrough {
		return nil, fmt.Errorf("%w: manifest in %s: segments end at %d, materializedThrough %d",
			errCorrupt, dir, prevLast, m.MaterializedThrough)
	} else if len(m.Sealed) == 0 && st.floor != m.MaterializedThrough {
		return nil, fmt.Errorf("%w: manifest in %s: no segments at floor %d, materializedThrough %d",
			errCorrupt, dir, st.floor, m.MaterializedThrough)
	}
	return st, nil
}

// sweepStreamDirs removes every scanned directory whose incarnation did not
// survive replay: displaced incarnations, reflected deletions, and the losing
// candidate of a crash that left two directories for one stream.
func (s *Storage) sweepStreamDirs(scan *recoveryScan) {
	for _, d := range scan.dirs {
		if live, ok := s.streams.Load(d.key.id); ok && live == d.st {
			continue
		}
		d.st.closeSegments()
		if err := os.RemoveAll(d.dir); err != nil {
			s.opts.SLogger.Warn("seglog: sweeping dead stream directory failed", "dir", d.dir, "error", err)
			continue
		}
		if err := syncDir(filepath.Dir(d.dir)); err != nil {
			s.opts.SLogger.Warn("seglog: syncing shard directory failed", "dir", d.dir, "error", err)
		}
	}
}

// recoverPartition replays one partition's WAL suffix past its checkpoint,
// applying every valid frame (invariant I1: the longest valid prefix is
// exactly the set of possibly-acknowledged mutations plus unacknowledged tail
// writes, which the contract permits to have taken effect). The final
// segment's torn tail is truncated and re-zeroed; damage anywhere else fails
// open (I2).
func (s *Storage) recoverPartition(p *partition, scan *recoveryScan) error {
	ckpt, hasCkpt, err := loadCheckpoint(p.wal.dir)
	if err != nil {
		return err
	}
	if hasCkpt && ckpt.Replay.SegmentSeq > 0 {
		p.ckptSeq = ckpt.Replay.SegmentSeq
		p.ckptOff = ckpt.Replay.Offset
		p.nextTxnID = ckpt.NextTxnID
	}

	seqs, err := listWALSegments(p.wal.dir)
	if err != nil {
		return err
	}
	// Finish any reclaim the crash interrupted: segments before the
	// checkpoint are fully reflected.
	kept := seqs[:0]
	removed := false
	for _, seq := range seqs {
		if seq < p.ckptSeq {
			if err := os.Remove(walSegmentPath(p.wal.dir, seq)); err != nil {
				return fmt.Errorf("seglog: remove reclaimed WAL segment: %w", err)
			}
			removed = true
			continue
		}
		kept = append(kept, seq)
	}
	if removed {
		if err := syncDir(p.wal.dir); err != nil {
			return err
		}
	}
	seqs = kept

	if len(seqs) == 0 {
		if p.ckptSeq > 0 {
			return fmt.Errorf("%w: partition %d checkpoint names segment %d but no segments exist",
				errCorrupt, p.id, p.ckptSeq)
		}
		return nil // fresh partition; first append rolls segment 1
	}
	if p.ckptSeq > 0 && seqs[0] != p.ckptSeq {
		return fmt.Errorf("%w: partition %d checkpoint segment %d is missing", errCorrupt, p.id, p.ckptSeq)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return fmt.Errorf("%w: partition %d segment sequence gap between %d and %d",
				errCorrupt, p.id, seqs[i-1], seqs[i])
		}
	}

	for i, seq := range seqs {
		startOff := int64(walSegmentHeaderSize)
		if seq == p.ckptSeq && p.ckptOff > startOff {
			startOff = p.ckptOff
		}
		if err := s.recoverSegment(p, scan, seq, i == len(seqs)-1, startOff); err != nil {
			return err
		}
	}
	return nil
}

func listWALSegments(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("seglog: read WAL dir: %w", err)
	}
	var seqs []uint64
	for _, e := range entries {
		name := e.Name()
		rest, ok := strings.CutPrefix(name, "wal-")
		if !ok {
			continue
		}
		rest, ok = strings.CutSuffix(rest, ".log")
		if !ok {
			continue
		}
		seq, err := strconv.ParseUint(rest, 16, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: unparseable WAL segment name %q", errCorrupt, name)
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func (s *Storage) recoverSegment(p *partition, scan *recoveryScan, seq uint64, last bool, startOff int64) error {
	path := walSegmentPath(p.wal.dir, seq)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("seglog: open WAL segment: %w", err)
	}
	keepOpen := false
	defer func() {
		if !keepOpen {
			_ = f.Close()
		}
	}()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("seglog: stat WAL segment: %w", err)
	}
	size := info.Size()

	hdrBuf := make([]byte, walSegmentHeaderSize)
	n, err := f.ReadAt(hdrBuf, 0)
	if err != nil && n < walSegmentHeaderSize {
		hdrBuf = hdrBuf[:n] // short file: decoded below as invalid
	}
	hdr, hdrErr := decodeWALSegmentHeader(hdrBuf)
	if hdrErr != nil || hdr.partition != p.id || hdr.segmentSeq != seq {
		// A crash during roll can leave the newest segment with a partial
		// header and no frames; that is only legitimate when no checkpointed
		// frame lives in this segment.
		if !last || startOff > walSegmentHeaderSize {
			return fmt.Errorf("%w: partition %d segment %d has an invalid header", errCorrupt, p.id, seq)
		}
		if err := p.wal.initSegment(f, seq); err != nil {
			return err
		}
		p.wal.adopt(seq, f, walSegmentHeaderSize)
		keepOpen = true
		return nil
	}

	scanner := newFrameScanner(f, size)
	scanner.off = startOff
	for {
		frame, err := scanner.next()
		if err == nil {
			if frame.txnID != p.nextTxnID {
				return fmt.Errorf("%w: partition %d expected txnID %d, found %d in segment %d",
					errCorrupt, p.id, p.nextTxnID, frame.txnID, seq)
			}
			p.nextTxnID++
			p.lastTS = max(p.lastTS, frame.ts)
			if err := s.applyRecovered(p, scan, seq, frame); err != nil {
				return err
			}
			continue
		}
		switch {
		case errors.Is(err, errFrameClean):
			// End of frames.
		case errors.Is(err, errFrameTorn):
			if !last {
				return fmt.Errorf("%w: partition %d segment %d has invalid frames before the final segment",
					errCorrupt, p.id, seq)
			}
			if err := rezeroTail(f, scanner.off, s.opts.WALSegmentBytes); err != nil {
				return err
			}
		default:
			return err
		}
		break
	}

	if last {
		p.wal.adopt(seq, f, scanner.off)
	} else {
		p.wal.segments[seq] = f
	}
	keepOpen = true
	return nil
}

// rezeroTail discards a torn tail: truncate to the valid end, re-extend to
// the full preallocated size (extended bytes read as zeros), and make it
// durable so stale bytes cannot resurface after a second crash.
func rezeroTail(f *os.File, validEnd, segmentBytes int64) error {
	if err := f.Truncate(validEnd); err != nil {
		return fmt.Errorf("seglog: truncate torn WAL tail: %w", err)
	}
	if err := preallocate(f, segmentBytes); err != nil {
		return fmt.Errorf("seglog: re-preallocate WAL segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("seglog: sync truncated WAL segment: %w", err)
	}
	return nil
}

// applyRecovered replays one durable frame into the in-memory catalog,
// mirroring partition.publish without locks or wakeups.
//
// Manifests may be ahead of the checkpoint, so replay tolerates frames whose
// effects are already reflected: frames for unknown streams or superseded
// incarnations are skipped, appends at or below materializedThrough add no
// entries, and metadata applies idempotently by value.
func (s *Storage) applyRecovered(p *partition, scan *recoveryScan, segSeq uint64, frame walFrame) error {
	switch frame.op {
	case opCreate:
		key := identityKey{id: frame.streamID, inc: frame.inc}
		if st, ok := scan.byIdentity[key]; ok {
			// Already materialized. Ensure this candidate owns the catalog
			// path: it may have lost the arbitrary seeding choice when a
			// crash left two directories for the stream.
			s.streams.Store(frame.streamID, st)
			return nil
		}
		var m createMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable create meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		st := newStreamState(frame.streamID, frame.inc, p.id, durablestream.StreamConfig{
			ContentType: m.ContentType,
			TTL:         time.Duration(m.TTLNanos),
			ExpiresAt:   m.ExpiresAt,
			IsPrivate:   m.IsPrivate,
			Closed:      m.Closed,
		})
		st.retention = s.opts.DefaultRetention
		if m.Retention != nil {
			if m.Retention.MaxBytes < 0 || m.Retention.MaxAgeNanos < 0 {
				return fmt.Errorf("%w: negative create retention for stream %q", errCorrupt, frame.streamID)
			}
			st.retention = Retention{MaxBytes: m.Retention.MaxBytes, MaxAge: time.Duration(m.Retention.MaxAgeNanos)}
		}
		st.closed = m.Closed
		st.nextIndex = 1 + int64(len(frame.payloads))
		for _, pl := range frame.payloads {
			st.walTail = append(st.walTail, walLoc{
				segmentSeq: segSeq,
				off:        pl.off,
				length:     pl.length,
				batchFirst: 1,
				ts:         frame.ts,
			})
		}
		s.streams.Store(frame.streamID, st)

	case opAppend:
		st, ok := s.recoveredState(frame)
		if !ok {
			return nil
		}
		if count := int64(len(frame.payloads)); count > 0 {
			switch {
			case frame.firstIndex+count-1 <= st.materializedThrough:
				// Already in segments; the manifest ran ahead.
			case frame.firstIndex != st.nextIndex:
				return fmt.Errorf("%w: stream %q expected index %d, frame assigns %d",
					errCorrupt, frame.streamID, st.nextIndex, frame.firstIndex)
			default:
				for _, pl := range frame.payloads {
					st.walTail = append(st.walTail, walLoc{
						segmentSeq: segSeq,
						off:        pl.off,
						length:     pl.length,
						batchFirst: frame.firstIndex,
						ts:         frame.ts,
					})
				}
				st.nextIndex = frame.firstIndex + count
			}
		}
		if frame.flags&flagHasSeq != 0 {
			if seq := string(frame.meta); seq > st.lastSeq {
				st.lastSeq = seq
			}
		}
		if frame.flags&flagClose != 0 {
			st.closed = true
		}

	case opDelete:
		if st, ok := s.recoveredState(frame); ok {
			s.streams.CompareAndDelete(frame.streamID, st)
		}

	case opTouch:
		st, ok := s.recoveredState(frame)
		if !ok {
			return nil
		}
		var m touchMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable touch meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		st.cfg.ExpiresAt = m.ExpiresAt

	case opRetention:
		st, ok := s.recoveredState(frame)
		if !ok {
			return nil
		}
		var m retentionMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable retention meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		if m.MaxBytes < 0 || m.MaxAgeNanos < 0 {
			return fmt.Errorf("%w: negative retention meta for stream %q", errCorrupt, frame.streamID)
		}
		st.retention = Retention{MaxBytes: m.MaxBytes, MaxAge: time.Duration(m.MaxAgeNanos)}

	case opTrim:
		st, ok := s.recoveredState(frame)
		if !ok {
			return nil
		}
		var m trimMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable trim meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		if m.FloorIndex < 0 || m.FloorIndex > st.materializedThrough {
			return fmt.Errorf("%w: invalid trim floor %d for stream %q", errCorrupt, m.FloorIndex, frame.streamID)
		}
		st.floor = max(st.floor, m.FloorIndex)

	default:
		return fmt.Errorf("%w: unknown frame op %d (written by a newer version?)", errCorrupt, frame.op)
	}
	return nil
}

// recoveredState resolves a frame's stream. A missing stream or another
// incarnation means the frame's effects were already reflected (the manifest
// or a later frame superseded it) and the frame is skipped.
func (s *Storage) recoveredState(frame walFrame) (*streamState, bool) {
	st, ok := s.streams.Load(frame.streamID)
	if !ok || st.inc != frame.inc {
		return nil, false
	}
	return st, true
}
