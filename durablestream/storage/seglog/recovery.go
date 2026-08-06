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
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// errCorrupt marks unrecoverable damage. New fails and leaves every byte
// intact rather than guess at durable history.
var errCorrupt = errors.New("seglog: WAL corruption")

// identityKey names one exact stream incarnation.
type identityKey struct {
	id  string
	inc incarnation
}

// recoveredDir is one stream directory referenced by a checkpoint.
type recoveredDir struct {
	key   identityKey
	dir   string
	st    *streamState
	entry streamCheckpointEntry
}

// recoveryScan is the result of loading partition checkpoints.
type recoveryScan struct {
	dirs        []recoveredDir
	byIdentity  map[identityKey]*streamState
	checkpoints map[uint32]checkpoint
}

// recoverAll rebuilds the in-memory catalog and every partition's writer
// position: load checkpoints, replay each partition's WAL suffix past its
// checkpoint, then sweep stream directories that are no longer live. It runs
// before any worker starts, so state mutation needs no locks.
func (s *Storage) recoverAll() (retErr error) {
	scan, err := s.scanStreamDirs()
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			for _, d := range scan.dirs {
				d.st.closeSegments()
			}
		}
	}()
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
	if err := s.rebuildTopology(scan); err != nil {
		return err
	}
	if err := s.sweepStreamDirs(scan); err != nil {
		return err
	}
	// Replay may have advanced logical state beyond its checkpoint entry. Mark every
	// survivor dirty before workers start so the first checkpoint cannot move
	// past replayed mutations until their derived state is durable.
	s.streams.Range(func(_ string, st *streamState) bool {
		s.parts[st.partition].markDirty(st)
		return true
	})
	return nil
}

// scanStreamDirs loads every stream from its owning partition's cumulative
// checkpoint. Stream directories contain segment files only; checkpoint.json
// is the sole authority for identity, metadata, and referenced prefixes.
func (s *Storage) scanStreamDirs() (*recoveryScan, error) {
	scan := &recoveryScan{byIdentity: make(map[identityKey]*streamState), checkpoints: make(map[uint32]checkpoint)}
	succeeded := false
	defer func() {
		if !succeeded {
			for _, d := range scan.dirs {
				d.st.closeSegments()
			}
		}
	}()
	for _, p := range s.parts {
		c, ok, err := loadCheckpoint(p.wal.dir)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		scan.checkpoints[p.id] = c
		if c.Partition != p.id || c.NextTxnID == 0 {
			return nil, fmt.Errorf("%w: invalid checkpoint identity for partition %d", errCorrupt, p.id)
		}
		for streamID, entry := range c.Streams {
			if uint32(streamHash(streamID)%uint64(len(s.parts))) != p.id {
				return nil, fmt.Errorf("%w: checkpoint stream %q is in the wrong partition", errCorrupt, streamID)
			}
			inc, err := parseIncarnationID(entry.IncarnationID)
			if err != nil {
				return nil, fmt.Errorf("%w: checkpoint stream %q: %v", errCorrupt, streamID, err)
			}
			dir := streamDir(s.dir, streamID, inc)
			st, err := s.stateFromCheckpointEntry(streamID, dir, entry)
			if err != nil {
				return nil, err
			}
			key := identityKey{id: st.id, inc: st.inc}
			scan.dirs = append(scan.dirs, recoveredDir{key: key, dir: dir, st: st, entry: entry})
			scan.byIdentity[key] = st
		}
	}
	succeeded = true
	return scan, nil
}

// removeUnreferencedSegments finishes a trim interrupted after its checkpoint
// but before unlink. The checkpoint is authoritative derived state, so an
// unreferenced segment cannot contain live records.
func removeUnreferencedSegments(dir string, m streamCheckpointEntry) error {
	referenced := make(map[string]struct{}, len(m.Sealed)+1)
	for _, seg := range m.Sealed {
		referenced[seg.File] = struct{}{}
	}
	if m.Active != nil {
		referenced[m.Active.File] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) && len(referenced) == 0 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("seglog: read stream dir for orphan cleanup: %w", err)
	}
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "seg-") || (!strings.HasSuffix(name, ".seg") && !strings.HasSuffix(name, ".idx")) {
			continue
		}
		if strings.HasSuffix(name, ".idx") && m.Active != nil && name == strings.TrimSuffix(m.Active.File, ".seg")+".idx" {
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

// stateFromCheckpointEntry opens an entry's segments and rebuilds the stream's
// materialized state.
func (s *Storage) stateFromCheckpointEntry(streamID, dir string, m streamCheckpointEntry) (*streamState, error) {
	inc, err := parseIncarnationID(m.IncarnationID)
	if err != nil {
		return nil, fmt.Errorf("%w: checkpoint entry for %s: %v", errCorrupt, streamID, err)
	}
	part := uint32(streamHash(streamID) % uint64(len(s.parts)))
	st := newStreamState(streamID, inc, part, m.config())
	succeeded := false
	defer func() {
		if !succeeded {
			st.closeSegments()
		}
	}()
	st.closed = m.Closed
	st.lastSeq = m.LastSeq
	st.lastSeqOffset = m.LastSeqOffset
	st.retention = m.Retention.retention()
	st.floor = m.FloorIndex
	st.softDeleted = m.SoftDeleted
	if m.Parent != nil {
		fork := m.Parent.Fork
		if m.Parent.StreamID == "" || m.Parent.IncarnationID == "" ||
			m.Parent.StreamID != fork.SourceID || m.Parent.IncarnationID != fork.SourceIncarnationID ||
			fork.Request.SourceStreamID != fork.SourceID || fork.Boundary < 0 || fork.PrefixCount < 0 {
			return nil, fmt.Errorf("%w: checkpoint entry for %s has inconsistent fork parent identity", errCorrupt, streamID)
		}
		st.fork = &fork
		st.parentBoundary = fork.Boundary
	}
	st.nextIndex = m.MaterializedThrough + 1
	st.firstLive = m.MaterializedThrough + 1
	st.materializedThrough = m.MaterializedThrough

	if st.retention.MaxBytes < 0 || st.retention.MaxAge < 0 || st.floor < 0 || st.floor > m.MaterializedThrough ||
		(st.fork != nil && m.MaterializedThrough < st.parentBoundary) {
		return nil, fmt.Errorf("%w: checkpoint entry for %s has invalid retention state", errCorrupt, streamID)
	}
	prevLast := int64(0)
	requiredStart := max(m.FloorIndex, st.parentBoundary) + 1
	for i, ms := range m.Sealed {
		sf, err := openSealedSegment(filepath.Join(dir, ms.File), ms.File, inc)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCorrupt, err)
		}
		badStart := i == 0 && (sf.firstIndex > requiredStart || sf.firstIndex < st.parentBoundary+1)
		if sf.firstIndex != ms.FirstIndex || sf.lastIndex != ms.LastIndex || sf.payloadEnd != ms.PayloadEnd || sf.count != ms.Count ||
			badStart || (i > 0 && sf.firstIndex != prevLast+1) {
			return nil, fmt.Errorf("%w: sealed segment %s disagrees with checkpoint entry for %s", errCorrupt, ms.File, streamID)
		}
		sf.minTS = ms.MinTS
		prevLast = sf.lastIndex
		st.sealed = append(st.sealed, sf)
	}
	if m.Active != nil {
		sf, err := openActiveSegment(filepath.Join(dir, m.Active.File), m.Active.File, inc, m.Active.PayloadEnd, m.Active.Count, m.Active.MinTS, m.Active.MaxTS)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errCorrupt, err)
		}
		badStart := len(m.Sealed) == 0 && (sf.firstIndex > requiredStart || sf.firstIndex < st.parentBoundary+1)
		if sf.firstIndex != m.Active.FirstIndex || badStart || (len(m.Sealed) > 0 && sf.firstIndex != prevLast+1) ||
			sf.lastIndex != m.MaterializedThrough {
			return nil, fmt.Errorf("%w: active segment %s disagrees with checkpoint entry for %s", errCorrupt, m.Active.File, streamID)
		}
		st.activeSeg = sf
		st.activeView = sf.view(s.fdCache)
	} else if len(m.Sealed) > 0 && prevLast != m.MaterializedThrough {
		return nil, fmt.Errorf("%w: checkpoint entry for %s: segments end at %d, materializedThrough %d",
			errCorrupt, streamID, prevLast, m.MaterializedThrough)
	} else if len(m.Sealed) == 0 && st.floor != m.MaterializedThrough &&
		(st.fork == nil || m.MaterializedThrough != st.parentBoundary) {
		return nil, fmt.Errorf("%w: checkpoint entry for %s: no segments at floor %d, materializedThrough %d",
			errCorrupt, streamID, st.floor, m.MaterializedThrough)
	}
	succeeded = true
	return st, nil
}

// rebuildTopology resolves generation-fenced parent identities after all WAL
// partitions have replayed, derives direct-child pins, and rejects malformed
// or over-deep persisted graphs before workers or retention start.
func (s *Storage) rebuildTopology(scan *recoveryScan) error {
	s.streams.Range(func(_ string, st *streamState) bool { st.refCount.Store(0); st.parent = nil; return true })
	var firstErr error
	s.streams.Range(func(_ string, st *streamState) bool {
		if st.fork == nil {
			return true
		}
		inc, err := parseIncarnationID(st.fork.SourceIncarnationID)
		if err != nil {
			firstErr = fmt.Errorf("%w: fork %q has invalid parent identity", errCorrupt, st.id)
			return false
		}
		// Resolve only through the surviving catalog incarnation. A stale scan
		// directory must never resurrect a displaced parent.
		parent, ok := s.streams.Load(st.fork.SourceID)
		if !ok || parent.inc != inc || st.parentBoundary < 0 || st.parentBoundary > parent.snapshot().tail {
			firstErr = fmt.Errorf("%w: fork %q references unavailable parent boundary", errCorrupt, st.id)
			return false
		}
		st.parent = parent
		parent.refCount.Add(1)
		return true
	})
	if firstErr != nil {
		return firstErr
	}
	s.streams.Range(func(_ string, st *streamState) bool {
		seen := make(map[*streamState]struct{})
		for cur, depth := st, 0; cur != nil; cur, depth = cur.parent, depth+1 {
			if depth > maxLineageDepth {
				firstErr = fmt.Errorf("%w: fork lineage exceeds %d", errCorrupt, maxLineageDepth)
				return false
			}
			if _, ok := seen[cur]; ok {
				firstErr = fmt.Errorf("%w: cycle in fork lineage at %q", errCorrupt, cur.id)
				return false
			}
			seen[cur] = struct{}{}
		}
		return true
	})
	if firstErr != nil {
		return firstErr
	}
	// Finish a cascade interrupted after the child's hard-delete frame but
	// before a retained ancestor's delete frame. Refcounts are derived from
	// surviving edges, so zero-pinned soft nodes are now unambiguously dead.
	for depth := 0; depth <= maxLineageDepth; depth++ {
		removed := false
		s.streams.Range(func(id string, st *streamState) bool {
			if (!st.softDeleted && !st.cfg.IsExpired()) || st.refCount.Load() != 0 {
				return true
			}
			s.streams.CompareAndDelete(id, st)
			st.deleted = true
			s.parts[st.partition].markRemoval(st)
			if st.parent != nil {
				if st.parent.refCount.Add(-1) < 0 {
					return false
				}
			}
			removed = true
			return true
		})
		if !removed {
			return nil
		}
	}
	return fmt.Errorf("%w: recovery removal cascade exceeds %d", errCorrupt, maxLineageDepth)
}

// sweepStreamDirs removes every scanned directory whose incarnation did not
// survive replay: displaced incarnations, reflected deletions, and the losing
// candidate of a crash that left two directories for one stream.
func (s *Storage) sweepStreamDirs(scan *recoveryScan) error {
	checkpointEntries := make(map[identityKey]streamCheckpointEntry, len(scan.dirs))
	for _, d := range scan.dirs {
		if live, ok := s.streams.Load(d.key.id); ok && live == d.st {
			checkpointEntries[d.key] = d.entry
			continue
		}
		d.st.closeSegments()
	}
	livePaths := make(map[string]streamCheckpointEntry)
	s.streams.Range(func(id string, st *streamState) bool {
		livePaths[streamDir(s.dir, id, st.inc)] = checkpointEntries[identityKey{id: id, inc: st.inc}]
		return true
	})
	streamsRoot := filepath.Join(s.dir, "streams")
	shards, err := os.ReadDir(streamsRoot)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("seglog: scan stream shards: %w", err)
	}
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		shardDir := filepath.Join(streamsRoot, shard.Name())
		dirs, err := os.ReadDir(shardDir)
		if err != nil {
			return fmt.Errorf("seglog: scan stream shard %s: %w", shardDir, err)
		}
		for _, entry := range dirs {
			if !entry.IsDir() {
				continue
			}
			path := filepath.Join(shardDir, entry.Name())
			checkpointEntry, live := livePaths[path]
			if live {
				if err := removeUnreferencedSegments(path, checkpointEntry); err != nil {
					return err
				}
				continue
			}
			if err := os.RemoveAll(path); err != nil {
				return fmt.Errorf("seglog: remove dead stream directory %s: %w", path, err)
			}
			if err := syncDir(shardDir); err != nil {
				return err
			}
		}
	}
	return nil
}

// recoverPartition replays one partition's WAL suffix past its checkpoint,
// applying every valid frame (invariant I1: the longest valid prefix is
// exactly the set of possibly-acknowledged mutations plus unacknowledged tail
// writes, which the contract permits to have taken effect). The final
// segment's torn tail is truncated and re-zeroed; damage anywhere else fails
// open (I2).
func (s *Storage) recoverPartition(p *partition, scan *recoveryScan) error {
	ckpt, hasCkpt := scan.checkpoints[p.id]
	if hasCkpt && ckpt.Replay.SegmentSeq > 0 {
		p.ckptSeq = ckpt.Replay.SegmentSeq
		p.ckptOff = ckpt.Replay.Offset
		p.nextTxnID = ckpt.NextTxnID
		p.ckptState, _ = json.Marshal(ckpt.Streams)
		p.materializedEntries = cloneCheckpointEntries(ckpt.Streams)
		p.stats.initializeCheckpoint(WALPosition{
			SegmentSeq: ckpt.Replay.SegmentSeq,
			Offset:     ckpt.Replay.Offset,
		})
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
			p.stats.recoverWALFrame(frame.end-frame.start, frame.ts)
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
			if err := rezeroTail(f, scanner.off, s.opts.WALSegmentBytes, s.opts.WALExtentBytes); err != nil {
				return err
			}
		default:
			return err
		}
		break
	}

	if last {
		// A clean EOF can come from a previously short extent. Reserve the
		// current boundary before this segment becomes writable again.
		if err := preallocate(f, walExtentEnd(scanner.off, s.opts.WALSegmentBytes, s.opts.WALExtentBytes)); err != nil {
			return fmt.Errorf("seglog: re-extend WAL segment: %w", err)
		}
		p.wal.adopt(seq, f, scanner.off)
	} else {
		p.wal.installRecoveredSegment(seq, f, scanner.off)
	}
	keepOpen = true
	return nil
}

// rezeroTail discards a torn tail: truncate to the valid end, re-extend to
// its current allocation extent (extended bytes read as zeros), and make it
// durable so stale bytes cannot resurface after a second crash.
func rezeroTail(f *os.File, validEnd, segmentBytes, extentBytes int64) error {
	if err := f.Truncate(validEnd); err != nil {
		return fmt.Errorf("seglog: truncate torn WAL tail: %w", err)
	}
	if err := preallocate(f, walExtentEnd(validEnd, segmentBytes, extentBytes)); err != nil {
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
// Replay tolerates frames whose
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

	case opFork:
		key := identityKey{id: frame.streamID, inc: frame.inc}
		if st, ok := scan.byIdentity[key]; ok {
			s.streams.Store(frame.streamID, st)
			return nil
		}
		var m forkFrameMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable fork meta for %q: %v", errCorrupt, frame.streamID, err)
		}
		if err := validateRecoveredFork(frame, m); err != nil {
			return err
		}
		cfg := durablestream.StreamConfig{ContentType: m.Create.ContentType, TTL: time.Duration(m.Create.TTLNanos), ExpiresAt: m.Create.ExpiresAt, IsPrivate: m.Create.IsPrivate, Closed: m.Create.Closed}
		st := newStreamState(frame.streamID, frame.inc, p.id, cfg)
		st.fork = &m.Fork
		st.parentBoundary, st.floor, st.materializedThrough, st.firstLive = m.Fork.Boundary, 0, m.Fork.Boundary, m.Fork.Boundary+1
		st.nextIndex = frame.firstIndex + int64(len(frame.payloads))
		st.closed = cfg.Closed
		st.retention = s.opts.DefaultRetention
		if m.Create.Retention != nil {
			st.retention = Retention{MaxBytes: m.Create.Retention.MaxBytes, MaxAge: time.Duration(m.Create.Retention.MaxAgeNanos)}
		}
		for i, pl := range frame.payloads {
			batchFirst := m.Fork.Boundary + 1
			if int64(i) >= m.Fork.PrefixCount {
				batchFirst += m.Fork.PrefixCount
			}
			st.walTail = append(st.walTail, walLoc{segmentSeq: segSeq, off: pl.off, length: pl.length, batchFirst: batchFirst, ts: frame.ts})
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
				// Already in checkpointed segments.
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
				st.lastSeqOffset = storage.FormatSimpleOffset(st.nextIndex - 1)
			}
		}
		if frame.flags&flagClose != 0 {
			st.closed = true
		}

	case opDelete:
		if st, ok := s.recoveredState(frame); ok {
			if frame.flags&flagSoftDelete != 0 {
				st.softDeleted = true
			} else {
				s.streams.CompareAndDelete(frame.streamID, st)
			}
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

func validateRecoveredFork(frame walFrame, m forkFrameMeta) error {
	if m.Fork.SourceID == "" || m.Fork.SourceIncarnationID == "" || m.Fork.Boundary < 0 ||
		m.Fork.PrefixCount < 0 || m.Fork.PrefixCount > int64(len(frame.payloads)) ||
		frame.firstIndex != m.Fork.Boundary+1 || m.Create.Retention == nil ||
		m.Create.Retention.MaxBytes < 0 || m.Create.Retention.MaxAgeNanos < 0 ||
		m.Fork.Request.SourceStreamID != m.Fork.SourceID {
		return fmt.Errorf("%w: invalid fork geometry or metadata for %q", errCorrupt, frame.streamID)
	}
	return nil
}

// recoveredState resolves a frame's stream. A missing stream or another
// incarnation means the frame's effects were already reflected (the checkpoint
// or a later frame superseded it) and the frame is skipped.
func (s *Storage) recoveredState(frame walFrame) (*streamState, bool) {
	st, ok := s.streams.Load(frame.streamID)
	if !ok || st.inc != frame.inc {
		return nil, false
	}
	return st, true
}
