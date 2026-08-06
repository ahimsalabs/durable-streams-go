package seglog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
)

type checkpoint struct {
	FormatVersion int    `json:"formatVersion"`
	Partition     uint32 `json:"partition"`
	Replay        struct {
		SegmentSeq uint64 `json:"segmentSeq"`
		Offset     int64  `json:"offset"`
	} `json:"replay"`
	NextTxnID uint64                           `json:"nextTxnID"`
	Streams   map[string]streamCheckpointEntry `json:"streams"`
}

const (
	checkpointFormatVersion = 2
	checkpointFileName      = "checkpoint.json"
)

func loadCheckpoint(dir string) (checkpoint, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, checkpointFileName))
	if os.IsNotExist(err) {
		return checkpoint{}, false, nil
	}
	if err != nil {
		return checkpoint{}, false, fmt.Errorf("seglog: read checkpoint: %w", err)
	}
	var c checkpoint
	if err := json.Unmarshal(raw, &c); err != nil {
		return checkpoint{}, false, fmt.Errorf("%w: undecodable checkpoint in %s: %v", errCorrupt, dir, err)
	}
	if c.FormatVersion != checkpointFormatVersion || c.Streams == nil {
		return checkpoint{}, false, fmt.Errorf("%w: checkpoint in %s has unsupported or incomplete schema", errCorrupt, dir)
	}
	return c, true, nil
}

type preparedStream struct {
	st             *streamState
	snap           readSnapshot
	sealed         []*segmentFile
	active         *segmentFile
	through        int64
	touched        map[string]struct{}
	touchedDirs    map[string]struct{}
	touchedParents map[string]struct{}
	victims        []*segmentFile
	created        []*segmentFile
	pins           []*fdPin
	payloadPin     *fdPin
	indexPin       *fdPin
	sealedSidecars []string
	locked         bool
}

// materializationBatch is retained across transient sync or checkpoint
// failures. Reusing the prepared files avoids colliding with their O_EXCL
// names and, more importantly, avoids deleting a file after an ambiguous
// atomicWrite failure may already have made a checkpoint reference it.
type materializationBatch struct {
	barrier  result
	prepared map[*streamState]*preparedStream
	removals []*streamState
	entries  map[string]streamCheckpointEntry
	synced   bool
}

func (s *Storage) runMaterializer(p *partition) {
	ticker := time.NewTicker(s.opts.MaterializeInterval)
	defer ticker.Stop()
	lastSweep := time.Now()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			s.materializeRound(p)
			if s.opts.RetentionInterval != -1 && time.Since(lastSweep) >= s.opts.RetentionInterval {
				s.retentionSweep(p)
				lastSweep = time.Now()
			}
		}
	}
}

func (s *Storage) materializeRound(p *partition) {
	if err := s.retryCleanup(p); err != nil {
		s.opts.SLogger.Error("seglog: deferred segment cleanup failed", "partition", p.id, "error", err)
		return
	}
	if p.pending != nil {
		if err := s.finishMaterialization(p, p.pending, true); err != nil {
			s.opts.SLogger.Error("seglog: pending materialization failed", "partition", p.id, "error", err)
		}
		return
	}
	barrier := p.submit(&request{op: opBarrier, captureDirty: true, done: make(chan result, 1)})
	if barrier.err != nil {
		return
	}
	dirty, removals := barrier.dirty, barrier.removals
	fail := func() {
		for st := range dirty {
			p.markDirty(st)
		}
		for _, st := range removals {
			p.markRemoval(st)
		}
	}

	prepared := make(map[*streamState]*preparedStream, len(dirty))
	for st := range dirty {
		draft, err := s.materializeStream(p, st)
		s.releaseDraftPins(draft)
		if err != nil {
			s.opts.SLogger.Error("seglog: materialization prepare failed", "stream", st.id, "error", err)
			if draft != nil {
				prepared[st] = draft
			}
			s.abortPrepared(p, prepared)
			fail()
			return
		}
		prepared[st] = draft
	}
	if barrier.walSeq == 0 {
		s.abortPrepared(p, prepared)
		return
	}
	forceCheckpoint := s.opts.CheckpointInterval == -1 || time.Since(p.lastCheckpoint) >= s.opts.CheckpointInterval ||
		barrier.walSeq > p.ckptSeq || len(removals) > 0 || batchHasVictims(prepared)
	if len(prepared) == 0 && len(removals) == 0 && !forceCheckpoint {
		return
	}
	batch := &materializationBatch{barrier: barrier, prepared: prepared, removals: removals}
	if len(prepared) == 0 && len(removals) == 0 {
		batch.entries = p.materializedEntries
	} else {
		batch.entries = s.checkpointEntries(p.materializedEntries, prepared, removals)
	}
	p.pending = batch
	if err := s.finishMaterialization(p, batch, forceCheckpoint); err != nil {
		s.opts.SLogger.Error("seglog: materialization commit failed", "partition", p.id, "error", err)
	}
}

// finalMaterialize runs after admission is closed and the partition worker has
// drained. With no publisher remaining, it can capture the final WAL frontier
// and dirty sets directly instead of submitting a barrier to a stopped worker.
func (s *Storage) finalMaterialize(p *partition) error {
	// An untouched partition has not needed to create its lazy WAL directory,
	// but Close still gives every partition a durable final checkpoint.
	if err := os.MkdirAll(p.wal.dir, 0o755); err != nil {
		return fmt.Errorf("create final checkpoint directory: %w", err)
	}
	if err := s.retryCleanup(p); err != nil {
		return fmt.Errorf("final cleanup: %w", err)
	}
	if p.pending != nil {
		if err := s.finishMaterialization(p, p.pending, true); err != nil {
			return fmt.Errorf("finish pending materialization: %w", err)
		}
	}
	dirty, removals := p.swapDirty()
	prepared := make(map[*streamState]*preparedStream, len(dirty))
	for st := range dirty {
		draft, err := s.materializeStream(p, st)
		s.releaseDraftPins(draft)
		if err != nil {
			if draft != nil {
				prepared[st] = draft
			}
			s.abortPrepared(p, prepared)
			return fmt.Errorf("prepare final stream %q: %w", st.id, err)
		}
		prepared[st] = draft
	}
	barrier := result{walSeq: p.publishedSeq, walOff: p.publishedOff, nextTxn: p.publishedNextTx}
	batch := &materializationBatch{
		barrier:  barrier,
		prepared: prepared,
		removals: removals,
		entries:  s.checkpointEntries(p.materializedEntries, prepared, removals),
	}
	p.pending = batch
	if err := s.finishMaterialization(p, batch, true); err != nil {
		return fmt.Errorf("commit final materialization: %w", err)
	}
	return nil
}

func (s *Storage) finishMaterialization(p *partition, batch *materializationBatch, checkpoint bool) error {
	if checkpoint {
		p.stats.checkpointRounds.Add(1)
		if !batch.synced {
			if err := s.syncCheckpointFiles(p, batch); err != nil {
				return err
			}
			batch.synced = true
		}
		if err := s.advanceCheckpoint(p, batch.barrier, batch.entries); err != nil {
			return err
		}
		clear(p.unsyncedFiles)
		clear(p.unsyncedDirs)
		clear(p.unsyncedParents)
		p.lastCheckpoint = time.Now()
		for _, draft := range batch.prepared {
			for _, sidecar := range draft.sealedSidecars {
				p.sealedSidecars[sidecar] = struct{}{}
			}
		}
	}
	for _, draft := range batch.prepared {
		s.publishPrepared(draft)
	}
	p.materializedEntries = batch.entries
	if !checkpoint {
		for _, draft := range batch.prepared {
			for path := range draft.touched {
				p.unsyncedFiles[path] = struct{}{}
			}
			for dir := range draft.touchedDirs {
				p.unsyncedDirs[dir] = struct{}{}
			}
			for dir := range draft.touchedParents {
				p.unsyncedParents[dir] = struct{}{}
			}
			for _, sidecar := range draft.sealedSidecars {
				p.sealedSidecars[sidecar] = struct{}{}
			}
		}
		s.releasePrepared(batch.prepared)
		p.pending = nil
		return nil
	}
	for _, draft := range batch.prepared {
		if err := s.unlinkPrepared(p, draft); err != nil {
			s.opts.SLogger.Error("seglog: deferred trim unlink failed", "stream", draft.st.id, "error", err)
		}
	}
	for _, st := range batch.removals {
		if live, ok := s.streams.Load(st.id); ok && live == st {
			p.markRemoval(st)
			continue
		}
		if err := s.removeStreamDir(st); err != nil {
			s.opts.SLogger.Error("seglog: stream directory removal failed", "stream", st.id, "error", err)
			p.markRemoval(st)
		}
	}
	for sidecar := range p.sealedSidecars {
		if err := s.fdCache.unlink(sidecar); err != nil {
			s.opts.SLogger.Error("seglog: sealed sidecar removal failed", "path", sidecar, "error", err)
			p.cleanupPaths[sidecar] = struct{}{}
			continue
		}
		delete(p.sealedSidecars, sidecar)
		p.cleanupDirs[filepath.Dir(sidecar)] = struct{}{}
	}
	if err := p.wal.removeBefore(batch.barrier.walSeq); err != nil {
		s.opts.SLogger.Error("seglog: WAL reclaim failed", "partition", p.id, "error", err)
	}
	s.releasePrepared(batch.prepared)
	p.pending = nil
	return nil
}

// syncCheckpointFiles establishes durability for every derived prefix that
// the next checkpoint will reference. Ordinary materialization rounds publish
// unsynced prefixes immediately; their WAL records remain the recovery source
// until this checkpoint-time batch completes.
func (s *Storage) syncCheckpointFiles(p *partition, batch *materializationBatch) error {
	paths := make(map[string]struct{}, len(p.unsyncedFiles))
	for path := range p.unsyncedFiles {
		paths[path] = struct{}{}
	}
	for _, draft := range batch.prepared {
		for path := range draft.touched {
			paths[path] = struct{}{}
		}
	}
	if len(paths) > 0 || len(p.unsyncedDirs) > 0 || len(p.unsyncedParents) > 0 {
		supported, performed, syncErr := s.checkpointBarrier.run(func() (bool, error) {
			root, err := os.Open(s.dir)
			if err != nil {
				return false, fmt.Errorf("open storage root for checkpoint sync: %w", err)
			}
			supported, syncErr := syncFilesystem(root)
			return supported, errors.Join(syncErr, root.Close())
		})
		if performed && supported {
			p.stats.syncfsCalls.Add(1)
		}
		if syncErr != nil {
			return fmt.Errorf("sync checkpoint filesystem: %w", syncErr)
		}
		if supported {
			return nil
		}
	}
	if err := s.syncFileBatch(p, paths); err != nil {
		return err
	}
	dirs := make(map[string]struct{}, len(p.unsyncedDirs))
	for dir := range p.unsyncedDirs {
		dirs[dir] = struct{}{}
	}
	for _, draft := range batch.prepared {
		for dir := range draft.touchedDirs {
			dirs[dir] = struct{}{}
		}
	}
	if err := syncDirectoryBatch(dirs); err != nil {
		return err
	}
	parents := make(map[string]struct{}, len(p.unsyncedParents))
	for dir := range p.unsyncedParents {
		parents[dir] = struct{}{}
	}
	for _, draft := range batch.prepared {
		for dir := range draft.touchedParents {
			parents[dir] = struct{}{}
		}
	}
	if err := syncDirectoryBatch(parents); err != nil {
		return err
	}
	if len(parents) > 0 {
		return syncDir(filepath.Join(s.dir, "streams"))
	}
	return nil
}

func (s *Storage) syncFileBatch(p *partition, paths map[string]struct{}) error {
	// A checkpoint deliberately issues a broad flush wave so independent files
	// reach the block layer together instead of serializing one device cache
	// flush per stream. Across the default 32 partitions this bounds checkpoint
	// pins at 1024, independent of stream cardinality.
	const syncWorkers = 32
	work := make(chan string)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	worker := func() {
		defer wg.Done()
		for path := range work {
			pin, err := s.fdCache.pin(path, true)
			if err == nil {
				p.stats.materializerSyncs.Add(1)
				err = fdatasync(pin.file())
				err = errors.Join(err, pin.release())
			}
			if err != nil {
				errOnce.Do(func() { firstErr = fmt.Errorf("sync checkpoint segment %s: %w", path, err) })
			}
		}
	}
	workers := min(syncWorkers, len(paths))
	wg.Add(workers)
	for range workers {
		go worker()
	}
	for path := range paths {
		work <- path
	}
	close(work)
	wg.Wait()
	return firstErr
}

func syncDirectoryBatch(dirs map[string]struct{}) error {
	if len(dirs) == 0 {
		return nil
	}
	const workersLimit = 32
	work := make(chan string)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	workers := min(workersLimit, len(dirs))
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for dir := range work {
				if err := syncDir(dir); err != nil {
					errOnce.Do(func() { firstErr = err })
				}
			}
		}()
	}
	for dir := range dirs {
		work <- dir
	}
	close(work)
	wg.Wait()
	return firstErr
}

func batchHasVictims(prepared map[*streamState]*preparedStream) bool {
	for _, draft := range prepared {
		if len(draft.victims) > 0 {
			return true
		}
	}
	return false
}

func (s *Storage) releasePrepared(prepared map[*streamState]*preparedStream) {
	for _, draft := range prepared {
		s.releaseDraftPins(draft)
		if draft.locked {
			draft.st.physicalMu.Unlock()
			draft.locked = false
		}
	}
}

func (s *Storage) releaseDraftPins(draft *preparedStream) {
	if draft == nil {
		return
	}
	for _, pin := range draft.pins {
		_ = pin.release()
	}
	draft.pins = nil
	draft.payloadPin = nil
	draft.indexPin = nil
}

func (s *Storage) abortPrepared(p *partition, prepared map[*streamState]*preparedStream) {
	s.releasePrepared(prepared)
	for _, draft := range prepared {
		for _, sf := range draft.created {
			path := sf.path
			if err := s.fdCache.unlink(path); err != nil {
				p.cleanupPaths[path] = struct{}{}
			}
			if err := s.fdCache.unlink(sf.indexPath); err != nil {
				p.cleanupPaths[sf.indexPath] = struct{}{}
			}
			p.cleanupDirs[filepath.Dir(path)] = struct{}{}
		}
	}
}

func (s *Storage) retryCleanup(p *partition) error {
	var firstErr error
	for path := range p.cleanupPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			if firstErr == nil {
				firstErr = fmt.Errorf("remove %s: %w", path, err)
			}
			continue
		}
		delete(p.cleanupPaths, path)
		p.cleanupDirs[filepath.Dir(path)] = struct{}{}
	}
	for dir := range p.cleanupDirs {
		if err := syncDir(dir); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		delete(p.cleanupDirs, dir)
	}
	return firstErr
}

func (s *Storage) advanceCheckpoint(p *partition, barrier result, entries map[string]streamCheckpointEntry) error {
	state, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("encode checkpoint streams: %w", err)
	}
	if p.ckptSeq == barrier.walSeq && p.ckptOff == barrier.walOff && bytes.Equal(p.ckptState, state) {
		return nil
	}
	c := checkpoint{FormatVersion: checkpointFormatVersion, Partition: p.id, NextTxnID: barrier.nextTxn, Streams: entries}
	c.Replay.SegmentSeq, c.Replay.Offset = barrier.walSeq, barrier.walOff
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := atomicWrite(filepath.Join(p.wal.dir, checkpointFileName), data, 0o644); err != nil {
		return err
	}
	p.ckptSeq, p.ckptOff, p.ckptState = barrier.walSeq, barrier.walOff, state
	return nil
}

func cloneCheckpointEntries(src map[string]streamCheckpointEntry) map[string]streamCheckpointEntry {
	entries := make(map[string]streamCheckpointEntry, len(src))
	for id, entry := range src {
		entries[id] = entry
	}
	return entries
}

// checkpointEntries advances the previous durable image only with the dirty
// and removal sets captured at the barrier. It must not range the live catalog:
// that could include a create or fork published after the replay frontier.
func (s *Storage) checkpointEntries(base map[string]streamCheckpointEntry, prepared map[*streamState]*preparedStream, removals []*streamState) map[string]streamCheckpointEntry {
	entries := cloneCheckpointEntries(base)
	for st, draft := range prepared {
		live, ok := s.streams.Load(st.id)
		if draft.snap.deleted || !ok || live != st {
			delete(entries, st.id)
			continue
		}
		entries[st.id] = buildCheckpointEntry(st, draft.snap, draft.sealed, draft.active, draft.through)
	}
	for _, st := range removals {
		entry, ok := entries[st.id]
		if ok && entry.IncarnationID == st.inc.String() {
			delete(entries, st.id)
		}
	}
	return entries
}

func (s *Storage) removeStreamDir(st *streamState) error {
	st.closeSegments()
	dir := streamDir(s.dir, st.id, st.inc)
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if err := s.fdCache.unlink(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	if err := os.Remove(dir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func (st *streamState) closeSegments() {
	st.mu.Lock()
	st.sealed, st.activeSeg, st.activeView = nil, nil, segmentView{}
	st.mu.Unlock()
}

// materializeStream prepares derived state without publishing it. Its caller
// batch-syncs every touched file and durably checkpoints the complete
// partition image before publish.
func (s *Storage) materializeStream(p *partition, st *streamState) (*preparedStream, error) {
	snap := st.snapshot()
	draft := &preparedStream{st: st, snap: snap, sealed: slices.Clone(snap.sealed), through: snap.through, touched: make(map[string]struct{}), touchedDirs: make(map[string]struct{}), touchedParents: make(map[string]struct{})}
	if snap.deleted {
		return draft, nil
	}
	if st.activeSeg != nil {
		copy := *st.activeSeg
		draft.active = &copy
	}
	dir := streamDir(s.dir, st.id, st.inc)
	if snap.floor > 0 {
		st.physicalMu.Lock()
		draft.locked = true
		if st.refCount.Load() == 0 && len(snap.walTail) == 0 {
			drop := 0
			for drop < len(draft.sealed) && draft.sealed[drop].lastIndex <= snap.floor {
				drop++
			}
			draft.victims = slices.Clone(draft.sealed[:drop])
			draft.sealed = slices.Clone(draft.sealed[drop:])
		}
	}
	for i, loc := range snap.walTail {
		idx := snap.firstLive + int64(i)
		if idx > snap.tail {
			break
		}
		payload, err := p.wal.readPayload(loc)
		if err != nil {
			return draft, fmt.Errorf("read WAL payload for %s index %d: %w", st.id, idx, err)
		}
		if draft.active != nil && draft.active.payloadEnd >= s.opts.StreamSegmentBytes {
			payloadPin, indexPin, err := s.pinDraftActive(draft)
			if err != nil {
				return draft, err
			}
			if err := draft.active.seal(payloadPin.file(), indexPin.file()); err != nil {
				return draft, err
			}
			draft.touched[draft.active.path] = struct{}{}
			draft.touched[draft.active.indexPath] = struct{}{}
			draft.sealedSidecars = append(draft.sealedSidecars, draft.active.indexPath)
			draft.sealed = append(draft.sealed, draft.active)
			draft.active = nil
		}
		if draft.active == nil {
			_, statErr := os.Stat(dir)
			newDir := os.IsNotExist(statErr)
			if statErr != nil && !newDir {
				return draft, fmt.Errorf("stat stream dir: %w", statErr)
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return draft, fmt.Errorf("create stream dir: %w", err)
			}
			if newDir {
				draft.touchedParents[filepath.Dir(dir)] = struct{}{}
			}
			draft.active, err = s.createDraftActiveSegment(dir, st.inc, idx)
			if err != nil {
				return draft, err
			}
			draft.created = append(draft.created, draft.active)
			draft.touchedDirs[dir] = struct{}{}
		}
		rec := segmentRecord{index: idx, batchFirst: loc.batchFirst, ts: loc.ts, length: loc.length}
		payloadPin, indexPin, err := s.pinDraftActive(draft)
		if err != nil {
			return draft, err
		}
		if err := draft.active.appendRecord(payloadPin.file(), indexPin.file(), rec, payload); err != nil {
			return draft, err
		}
		draft.touched[draft.active.path] = struct{}{}
		draft.touched[draft.active.indexPath] = struct{}{}
	}
	if st.forceSeal && draft.active != nil && draft.active.lastIndex >= draft.active.firstIndex {
		payloadPin, indexPin, err := s.pinDraftActive(draft)
		if err != nil {
			return draft, err
		}
		if err := draft.active.seal(payloadPin.file(), indexPin.file()); err != nil {
			return draft, err
		}
		draft.touched[draft.active.path] = struct{}{}
		draft.touched[draft.active.indexPath] = struct{}{}
		draft.sealedSidecars = append(draft.sealedSidecars, draft.active.indexPath)
		draft.sealed = append(draft.sealed, draft.active)
		draft.active = nil
	}
	draft.through = max(snap.through, snap.firstLive+int64(len(snap.walTail))-1)
	return draft, nil
}

func (s *Storage) pinDraftActive(draft *preparedStream) (*fdPin, *fdPin, error) {
	if draft.payloadPin != nil && draft.indexPin != nil &&
		draft.payloadPin.e.path == draft.active.path && draft.indexPin.e.path == draft.active.indexPath {
		return draft.payloadPin, draft.indexPin, nil
	}
	payload, err := s.fdCache.pin(draft.active.path, true)
	if err != nil {
		return nil, nil, err
	}
	index, err := s.fdCache.pin(draft.active.indexPath, true)
	if err != nil {
		_ = payload.release()
		return nil, nil, err
	}
	draft.pins = append(draft.pins, payload, index)
	draft.payloadPin, draft.indexPin = payload, index
	return payload, index, nil
}

func (s *Storage) createDraftActiveSegment(dir string, inc incarnation, firstIndex int64) (*segmentFile, error) {
	active, err := createActiveSegment(dir, inc, firstIndex, time.Now().UnixNano())
	if !errors.Is(err, os.ErrExist) {
		return active, err
	}
	// No published descriptor can own this next-index name. It is an orphan
	// from an aborted pre-checkpoint prepare; remove it and retry once.
	path := filepath.Join(dir, segmentFileName(firstIndex))
	if err := s.fdCache.unlink(path); err != nil {
		return nil, fmt.Errorf("remove abandoned segment %s: %w", path, err)
	}
	if err := s.fdCache.unlink(filepath.Join(dir, segmentIndexName(firstIndex))); err != nil {
		return nil, fmt.Errorf("remove abandoned segment sidecar: %w", err)
	}
	if err := syncDir(dir); err != nil {
		return nil, err
	}
	return createActiveSegment(dir, inc, firstIndex, time.Now().UnixNano())
}

func (s *Storage) publishPrepared(draft *preparedStream) {
	st := draft.st
	st.mu.Lock()
	st.forceSeal = false
	st.sealed, st.activeSeg, st.materializedThrough = draft.sealed, draft.active, draft.through
	if draft.active != nil {
		st.activeView = draft.active.view(s.fdCache)
	} else {
		st.activeView = segmentView{}
	}
	if k := draft.through - st.firstLive + 1; k > 0 {
		st.walTail, st.firstLive = st.walTail[k:], draft.through+1
	}
	st.mu.Unlock()
	if draft.snap.floor > 0 && len(draft.snap.walTail) > 0 {
		s.parts[st.partition].markDirty(st)
	}
}

func (s *Storage) unlinkPrepared(p *partition, draft *preparedStream) error {
	if len(draft.victims) == 0 {
		return nil
	}
	dir := streamDir(s.dir, draft.st.id, draft.st.inc)
	var first error
	for _, sf := range draft.victims {
		if err := s.fdCache.unlink(filepath.Join(dir, sf.name)); err != nil {
			path := filepath.Join(dir, sf.name)
			p.cleanupPaths[path] = struct{}{}
			if first == nil {
				first = err
			}
		}
	}
	// Sealed sidecars are partition-level checkpoint cleanup. finishMaterialization
	// removes the cumulative set only after the checkpoint is durable.
	if err := syncDir(dir); err != nil {
		p.cleanupDirs[dir] = struct{}{}
		if first == nil {
			first = err
		}
	}
	return first
}

func buildCheckpointEntry(st *streamState, snap readSnapshot, sealed []*segmentFile, active *segmentFile, through int64) streamCheckpointEntry {
	e := streamCheckpointEntry{IncarnationID: snap.inc.String(), ContentType: snap.cfg.ContentType, TTLNanos: int64(snap.cfg.TTL), ExpiresAt: snap.cfg.ExpiresAt, IsPrivate: snap.cfg.IsPrivate, Closed: snap.closed, LastSeq: snap.lastSeq, LastSeqOffset: snap.lastSeqOffset, Retention: retentionCheckpointEntry(snap.retention), FloorIndex: snap.floor, SoftDeleted: snap.softDeleted, MaterializedThrough: through}
	if snap.parent != nil {
		e.Parent = &checkpointParent{StreamID: snap.parent.id, IncarnationID: snap.parent.inc.String(), Fork: *st.fork}
	}
	for _, sf := range sealed {
		e.Sealed = append(e.Sealed, checkpointSegment{File: sf.name, FirstIndex: sf.firstIndex, LastIndex: sf.lastIndex, PayloadEnd: sf.payloadEnd, Count: sf.count, MaxTS: sf.maxTS})
	}
	if active != nil {
		e.Active = &checkpointActive{File: active.name, FirstIndex: active.firstIndex, PayloadEnd: active.payloadEnd, Count: active.count, MaxTS: active.maxTS}
	}
	return e
}

func (s *Storage) retentionSweep(p *partition) {
	now := time.Now()
	s.streams.Range(func(_ string, st *streamState) bool {
		if st.partition != p.id {
			return true
		}
		if err := s.sweepStreamRetention(p, st, now); err != nil {
			s.opts.SLogger.Error("seglog: retention sweep failed", "stream", st.id, "error", err)
		}
		return true
	})
}

func (s *Storage) sweepStreamRetention(p *partition, st *streamState, now time.Time) error {
	snap := st.snapshot()
	if snap.deleted || snap.cfg.IsExpired() {
		return nil
	}
	if s.opts.StreamSegmentAge != -1 && st.activeSeg != nil && st.activeSeg.lastIndex == snap.through && st.activeSeg.maxTS > 0 && now.Sub(time.Unix(0, st.activeSeg.maxTS)) > s.opts.StreamSegmentAge {
		st.forceSeal = true
		p.markDirty(st)
		s.materializeRound(p)
		snap = st.snapshot()
	}
	sealed, maxDrop := snap.sealed, len(snap.sealed)
	if snap.activeView.path == "" && len(snap.walTail) == 0 && maxDrop > 0 {
		maxDrop--
	}
	drop, floor := 0, snap.floor
	for drop < maxDrop && sealed[drop].lastIndex <= floor {
		drop++
	}
	if snap.retention.MaxAge > 0 {
		for i := drop; i < maxDrop && now.Sub(time.Unix(0, sealed[i].maxTS)) > snap.retention.MaxAge; i++ {
			floor, drop = sealed[i].lastIndex, i+1
		}
	}
	if snap.retention.MaxBytes > 0 {
		var retained int64
		for _, sf := range sealed {
			retained += segmentPayloadBytes(sf)
		}
		if st.activeSeg != nil {
			retained += segmentPayloadBytes(st.activeSeg)
		}
		for _, loc := range snap.walTail {
			retained += int64(loc.length)
		}
		for i := range drop {
			retained -= segmentPayloadBytes(sealed[i])
		}
		for drop < maxDrop && retained > snap.retention.MaxBytes {
			retained -= segmentPayloadBytes(sealed[drop])
			floor, drop = max(floor, sealed[drop].lastIndex), drop+1
		}
	}
	if drop == 0 {
		return nil
	}
	res := p.submit(&request{op: opTrim, streamID: st.id, floor: floor, done: make(chan result, 1)})
	if res.err != nil {
		return res.err
	}
	p.markDirty(st)
	s.materializeRound(p)
	return nil
}

func segmentPayloadBytes(sf *segmentFile) int64 {
	return sf.payloadEnd - segmentHeaderSize
}
