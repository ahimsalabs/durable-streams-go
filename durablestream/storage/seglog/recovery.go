package seglog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// errCorrupt marks unrecoverable WAL damage. New fails and leaves every byte
// intact rather than guess at durable history.
var errCorrupt = errors.New("seglog: WAL corruption")

// recoverAll rebuilds the in-memory catalog and every partition's writer
// position by replaying the WALs. It runs before any worker starts, so state
// mutation needs no locks.
func (s *Storage) recoverAll() error {
	for _, p := range s.parts {
		if err := s.recoverPartition(p); err != nil {
			return err
		}
	}
	return nil
}

// recoverPartition replays one partition's segments in order, applying every
// valid frame (invariant I1: the longest valid prefix is exactly the set of
// possibly-acknowledged mutations plus unacknowledged tail writes, which the
// contract permits to have taken effect). The final segment's torn tail is
// truncated and re-zeroed; damage anywhere else fails open (I2).
func (s *Storage) recoverPartition(p *partition) error {
	seqs, err := listWALSegments(p.wal.dir)
	if err != nil {
		return err
	}
	if len(seqs) == 0 {
		return nil // fresh partition; first append rolls segment 1
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return fmt.Errorf("%w: partition %d segment sequence gap between %d and %d",
				errCorrupt, p.id, seqs[i-1], seqs[i])
		}
	}

	for i, seq := range seqs {
		last := i == len(seqs)-1
		if err := s.recoverSegment(p, seq, last); err != nil {
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

func (s *Storage) recoverSegment(p *partition, seq uint64, last bool) error {
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
		if !last {
			return fmt.Errorf("%w: partition %d segment %d has an invalid header", errCorrupt, p.id, seq)
		}
		// A crash during roll can leave the newest segment with a partial
		// header and no frames. Re-initialize it in place.
		if err := p.wal.initSegment(f, seq); err != nil {
			return err
		}
		p.wal.adopt(seq, f, walSegmentHeaderSize)
		keepOpen = true
		return nil
	}

	scanner := newFrameScanner(f, size)
	for {
		frame, err := scanner.next()
		if err == nil {
			if frame.txnID != p.nextTxnID {
				return fmt.Errorf("%w: partition %d expected txnID %d, found %d in segment %d",
					errCorrupt, p.id, p.nextTxnID, frame.txnID, seq)
			}
			p.nextTxnID++
			p.lastTS = max(p.lastTS, frame.ts)
			if err := s.applyRecovered(p, seq, frame); err != nil {
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
func (s *Storage) applyRecovered(p *partition, segSeq uint64, frame walFrame) error {
	switch frame.op {
	case opCreate:
		var m createMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable create meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		cfg := durablestream.StreamConfig{
			ContentType: m.ContentType,
			TTL:         time.Duration(m.TTLNanos),
			ExpiresAt:   m.ExpiresAt,
			IsPrivate:   m.IsPrivate,
			Closed:      m.Closed,
		}
		st := newStreamState(frame.streamID, frame.inc, p.id, cfg)
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
		// A displaced expired incarnation is simply replaced; nothing waits
		// on it during recovery.
		s.streams.Store(frame.streamID, st)

	case opAppend:
		st, err := s.recoveredState(frame)
		if err != nil {
			return err
		}
		if len(frame.payloads) > 0 && frame.firstIndex != st.nextIndex {
			return fmt.Errorf("%w: stream %q expected index %d, frame assigns %d",
				errCorrupt, frame.streamID, st.nextIndex, frame.firstIndex)
		}
		for _, pl := range frame.payloads {
			st.walTail = append(st.walTail, walLoc{
				segmentSeq: segSeq,
				off:        pl.off,
				length:     pl.length,
				batchFirst: frame.firstIndex,
				ts:         frame.ts,
			})
		}
		if len(frame.payloads) > 0 {
			st.nextIndex = frame.firstIndex + int64(len(frame.payloads))
		}
		if frame.flags&flagHasSeq != 0 {
			st.lastSeq = string(frame.meta)
		}
		if frame.flags&flagClose != 0 {
			st.closed = true
		}

	case opDelete:
		st, err := s.recoveredState(frame)
		if err != nil {
			return err
		}
		s.streams.CompareAndDelete(frame.streamID, st)

	case opTouch:
		st, err := s.recoveredState(frame)
		if err != nil {
			return err
		}
		var m touchMeta
		if err := json.Unmarshal(frame.meta, &m); err != nil {
			return fmt.Errorf("%w: undecodable touch meta for stream %q: %v", errCorrupt, frame.streamID, err)
		}
		st.cfg.ExpiresAt = m.ExpiresAt

	default:
		return fmt.Errorf("%w: unknown frame op %d (written by a newer version?)", errCorrupt, frame.op)
	}
	return nil
}

// recoveredState resolves the frame's stream and incarnation; a mismatch
// breaks replay-order invariants and fails open.
func (s *Storage) recoveredState(frame walFrame) (*streamState, error) {
	st, ok := s.streams.Load(frame.streamID)
	if !ok {
		return nil, fmt.Errorf("%w: frame for unknown stream %q", errCorrupt, frame.streamID)
	}
	if st.inc != frame.inc {
		return nil, fmt.Errorf("%w: frame for stream %q references a different incarnation", errCorrupt, frame.streamID)
	}
	return st, nil
}
