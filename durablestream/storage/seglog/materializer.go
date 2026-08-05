package seglog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// checkpoint is the per-partition WAL replay point, written atomically to
// wal/pNNNN/checkpoint.json. Every frame before replay is fully reflected in
// durable manifests (and stream-directory removals); manifests may run ahead
// of it, never behind.
type checkpoint struct {
	FormatVersion int    `json:"formatVersion"`
	Partition     uint32 `json:"partition"`
	Replay        struct {
		SegmentSeq uint64 `json:"segmentSeq"`
		Offset     int64  `json:"offset"`
	} `json:"replay"`
	NextTxnID uint64 `json:"nextTxnID"`
}

const checkpointFileName = "checkpoint.json"

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
	return c, true, nil
}

// runMaterializer periodically materializes one partition. It owns the
// partition's stream segments and manifests; the loop exits on shutdown.
func (s *Storage) runMaterializer(p *partition) {
	ticker := time.NewTicker(s.opts.MaterializeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-ticker.C:
			s.materializeRound(p)
		}
	}
}

// materializeRound flushes every dirty stream and pending removal, then
// advances the checkpoint to the barrier position P and reclaims WAL
// segments before it.
//
// Ordering argument: every frame committed before P was published — and its
// stream dirty-marked or queued for removal — before the barrier completed,
// so the post-barrier swap contains it, and this round's flushes (which read
// current state) reflect it. Flushes may also include effects of frames
// after P; manifests running ahead of the checkpoint is safe because replay
// is idempotent and appends skip via materializedThrough.
func (s *Storage) materializeRound(p *partition) {
	res := p.submit(&request{op: opBarrier, done: make(chan result, 1)})
	if res.err != nil {
		return // storage closing or partition fail-stopped
	}
	dirty, removals := p.swapDirty()

	ok := true
	for _, st := range removals {
		if err := s.removeStreamDir(st); err != nil {
			s.opts.SLogger.Error("seglog: stream directory removal failed", "stream", st.id, "error", err)
			ok = false
		}
	}
	for st := range dirty {
		if err := s.materializeStream(p, st); err != nil {
			s.opts.SLogger.Error("seglog: materialization failed", "stream", st.id, "error", err)
			ok = false
		}
	}
	if !ok {
		// Something is unflushed; advancing the checkpoint could orphan it.
		// The streams stay dirty-marked... but swapDirty already consumed the
		// set, so re-mark to retry next round.
		for st := range dirty {
			p.markDirty(st)
		}
		for _, st := range removals {
			p.markRemoval(st)
		}
		return
	}

	if res.walSeq == 0 {
		return // no WAL yet: nothing to checkpoint or reclaim
	}
	if err := s.advanceCheckpoint(p, res); err != nil {
		s.opts.SLogger.Error("seglog: checkpoint advance failed", "partition", p.id, "error", err)
		return
	}
	if err := p.wal.removeBefore(res.walSeq); err != nil {
		s.opts.SLogger.Error("seglog: WAL reclaim failed", "partition", p.id, "error", err)
	}
}

// advanceCheckpoint durably records the new replay point, skipping the write
// when nothing moved.
func (s *Storage) advanceCheckpoint(p *partition, res result) error {
	if p.ckptSeq == res.walSeq && p.ckptOff == res.walOff {
		return nil
	}
	var c checkpoint
	c.FormatVersion = 1
	c.Partition = p.id
	c.Replay.SegmentSeq = res.walSeq
	c.Replay.Offset = res.walOff
	c.NextTxnID = res.nextTxn
	data, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	if err := atomicWrite(filepath.Join(p.wal.dir, checkpointFileName), data, 0o644); err != nil {
		return err
	}
	p.ckptSeq, p.ckptOff = res.walSeq, res.walOff
	return nil
}

// removeStreamDir closes a dead incarnation's segment descriptors and removes
// its directory. Missing directories (never materialized) are a no-op.
func (s *Storage) removeStreamDir(st *streamState) error {
	st.closeSegments()
	dir := streamDir(s.dir, st.id, st.inc)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil
	}
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

// closeSegments closes the incarnation's segment descriptors. Only called
// once the incarnation is dead (deleted or displaced), when the materializer
// no longer writes to it; a concurrent reader gets a read error and maps it
// through its ErrNotFound path.
func (st *streamState) closeSegments() {
	st.mu.Lock()
	sealed := st.sealed
	active := st.activeSeg
	st.sealed = nil
	st.activeSeg = nil
	st.activeView = segmentView{}
	st.mu.Unlock()
	for _, sf := range sealed {
		_ = sf.f.Close()
	}
	if active != nil {
		_ = active.f.Close()
	}
}

// materializeStream copies the stream's unmaterialized WAL records into its
// segments, seals full segments, flushes the manifest, and prunes the WAL
// tail. Single-threaded per partition.
func (s *Storage) materializeStream(p *partition, st *streamState) error {
	snap := st.snapshot()
	if snap.deleted {
		return nil // the removals path owns dead incarnations
	}

	dir := streamDir(s.dir, st.id, st.inc)
	if st.activeSeg == nil && len(st.sealed) == 0 {
		// First materialization of this incarnation: create its directory
		// with durable entries up the chain.
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create stream dir: %w", err)
		}
		if err := syncDir(filepath.Dir(dir)); err != nil {
			return err
		}
		if err := syncDir(filepath.Join(s.dir, "streams")); err != nil {
			return err
		}
	}

	sealed := st.sealed
	active := st.activeSeg
	activeDirty := false
	for i, loc := range snap.walTail {
		idx := snap.firstLive + int64(i)
		if idx > snap.tail {
			break
		}
		payload, err := p.wal.readPayload(loc)
		if err != nil {
			return fmt.Errorf("read WAL payload for %s index %d: %w", st.id, idx, err)
		}
		if active != nil && active.bytes >= s.opts.StreamSegmentBytes {
			if err := active.seal(); err != nil {
				return err
			}
			sealed = append(sealed[:len(sealed):len(sealed)], active) // copy-on-write for readers
			active = nil
		}
		if active == nil {
			active, err = createActiveSegment(dir, st.inc, idx, time.Now().UnixNano())
			if err != nil {
				return err
			}
			if err := syncDir(dir); err != nil {
				return err
			}
		}
		rec := segmentRecord{index: idx, batchFirst: loc.batchFirst, ts: loc.ts, length: loc.length}
		if err := active.appendRecord(rec, payload, s.opts.SparseIndexBytes); err != nil {
			return err
		}
		activeDirty = true
	}
	if activeDirty {
		if err := active.f.Sync(); err != nil {
			return fmt.Errorf("sync active segment: %w", err)
		}
	}

	newThrough := max(snap.through, snap.firstLive+int64(len(snap.walTail))-1)
	m := manifest{
		FormatVersion:       manifestFormatVersion,
		StreamID:            st.id,
		IncarnationID:       snap.inc.String(),
		ContentType:         snap.cfg.ContentType,
		TTLNanos:            int64(snap.cfg.TTL),
		ExpiresAt:           snap.cfg.ExpiresAt,
		IsPrivate:           snap.cfg.IsPrivate,
		Closed:              snap.closed,
		LastSeq:             snap.lastSeq,
		MaterializedThrough: newThrough,
	}
	for _, sf := range sealed {
		m.Sealed = append(m.Sealed, manifestSegment{
			File:       sf.name,
			FirstIndex: sf.firstIndex,
			LastIndex:  sf.lastIndex,
			Bytes:      sf.bytes,
			MaxTS:      sf.maxTS,
		})
	}
	if active != nil {
		m.Active = &manifestActive{File: active.name, FirstIndex: active.firstIndex, Bytes: active.bytes}
	}
	if err := writeManifest(dir, m); err != nil {
		return err
	}

	// Publish the new materialized frontier and prune the WAL tail.
	st.mu.Lock()
	st.sealed = sealed
	st.activeSeg = active
	if active != nil {
		st.activeView = active.view()
	}
	st.materializedThrough = newThrough
	if k := newThrough - st.firstLive + 1; k > 0 {
		st.walTail = st.walTail[k:]
		st.firstLive = newThrough + 1
	}
	st.mu.Unlock()
	return nil
}
