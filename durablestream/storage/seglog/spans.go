package seglog

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	storagepkg "github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

const spanWriteChunk = 32 << 10

// ReadSpans implements durablestream.SpanReadStorage. A run of records in one
// immutable sealed segment is returned as one contiguous pinned file range.
// Active, WAL-resident, and fork-stitched reads transparently retain Read's
// owned-copy behavior. Sealed bytes are trusted because payload CRCs were
// verified while materializing the immutable segment; Read remains the path
// for callers that require verification on every access.
func (s *Storage) ReadSpans(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.SpanReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	pos, err := parseReadOffset(offset, limit)
	if err != nil {
		return nil, err
	}
	state, ok := s.streams.Load(streamID)
	if !ok {
		return nil, notFoundErr(streamID)
	}
	snap := state.snapshot()
	if snap.softDeleted || (snap.cfg.IsExpired() && state.refCount.Load() != 0) {
		return nil, softDeletedErr(streamID)
	}
	if snap.deleted || snap.cfg.IsExpired() {
		return nil, notFoundErr(streamID)
	}
	if pos < snap.floor {
		return nil, fmt.Errorf("seglog: stream %q retained floor is %d: %w", streamID, snap.floor, durablestream.ErrGone)
	}
	result := &durablestream.SpanReadResult{
		NextOffset:    storagepkg.FormatSimpleOffset(pos),
		TailOffset:    storagepkg.FormatSimpleOffset(snap.tail),
		IncarnationID: snap.inc.String(),
		Closed:        snap.closed,
	}
	if pos >= snap.tail {
		return result, nil
	}

	// A span must not cross a lineage stitch, mutable active segment, or WAL
	// boundary. Those cases use the verifying copied path for the whole range.
	through := snap.tail
	if snap.parent != nil || through > lastSealedIndex(snap.sealed) {
		return s.copiedSpanRead(ctx, streamID, offset, limit)
	}

	next := pos + 1
	total := int64(0)
	for _, sf := range snap.sealed {
		if sf.lastIndex < next || sf.firstIndex > through {
			continue
		}
		span, last, bytes, spanErr := s.sealedRange(ctx, sf, max(next, sf.firstIndex), through, int64(limit), total)
		if spanErr != nil {
			closeSpans(result.Spans)
			return s.copiedSpanRead(ctx, streamID, offset, limit)
		}
		if span == nil {
			break
		}
		result.Spans = append(result.Spans, span)
		result.NextOffset = storagepkg.FormatSimpleOffset(last)
		total += bytes
		next = last + 1
		if last < min(through, sf.lastIndex) {
			break
		}
	}
	return result, nil
}

func lastSealedIndex(sealed []*segmentFile) int64 {
	if len(sealed) == 0 {
		return -1
	}
	return sealed[len(sealed)-1].lastIndex
}

func (s *Storage) copiedSpanRead(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.SpanReadResult, error) {
	read, err := s.Read(ctx, streamID, offset, limit)
	if err != nil {
		return nil, err
	}
	result := &durablestream.SpanReadResult{NextOffset: read.NextOffset, TailOffset: read.TailOffset, IncarnationID: read.IncarnationID, Closed: read.Closed}
	return ownedSpanResult(ctx, result, read.Messages), nil
}

func ownedSpanResult(ctx context.Context, result *durablestream.SpanReadResult, messages []durablestream.StoredMessage) *durablestream.SpanReadResult {
	for _, msg := range messages {
		result.Spans = append(result.Spans, &ownedReadSpan{ctx: ctx, data: msg.Data})
	}
	return result
}

func (s *Storage) sealedRange(ctx context.Context, sf *segmentFile, from, through int64, limit, used int64) (durablestream.ReadSpan, int64, int64, error) {
	pin, err := s.fdCache.pin(sf.path, false)
	if err != nil {
		return nil, 0, 0, err
	}
	f := pin.file()
	startOrdinal := from - sf.firstIndex
	endOrdinal := min(through, sf.lastIndex) - sf.firstIndex
	start := int64(segmentHeaderSize)
	if startOrdinal > 0 {
		var prev [8]byte
		if _, err := f.ReadAt(prev[:], sf.payloadEnd+(startOrdinal-1)*denseEntrySize); err != nil {
			_ = pin.release()
			return nil, 0, 0, err
		}
		start = int64(binary.LittleEndian.Uint64(prev[:]))
	}
	end, last := start, from-1
	for ordinal := startOrdinal; ordinal <= endOrdinal; ordinal++ {
		var entry [8]byte
		if _, err := f.ReadAt(entry[:], sf.payloadEnd+ordinal*denseEntrySize); err != nil {
			_ = pin.release()
			return nil, 0, 0, err
		}
		candidate := int64(binary.LittleEndian.Uint64(entry[:]))
		if candidate < end || candidate > sf.payloadEnd {
			_ = pin.release()
			return nil, 0, 0, errBadSegment
		}
		if limit > 0 && (used > 0 || last >= from) && used+candidate-start > limit {
			break
		}
		end = candidate
		last = sf.firstIndex + ordinal
	}
	if last < from {
		_ = pin.release()
		return nil, 0, 0, nil
	}
	return &fileReadSpan{ctx: ctx, pin: pin, off: start, length: end - start}, last, end - start, nil
}

func closeSpans(spans []durablestream.ReadSpan) {
	for _, span := range spans {
		_ = span.Close()
	}
}

type ownedReadSpan struct {
	mu     sync.Mutex
	ctx    context.Context
	data   []byte
	closed bool
	used   bool
}

func (s *ownedReadSpan) WriteTo(w io.Writer) (int64, error) {
	s.mu.Lock()
	if s.closed || s.used {
		s.mu.Unlock()
		return 0, osClosedError()
	}
	s.used = true
	data := s.data
	s.mu.Unlock()
	return writeChunks(s.ctx, w, func(p []byte, off int64) (int, error) { return copy(p, data[off:]), nil }, int64(len(data)))
}

func (s *ownedReadSpan) Close() error {
	s.mu.Lock()
	s.closed, s.data = true, nil
	s.mu.Unlock()
	return nil
}

type fileReadSpan struct {
	mu     sync.Mutex
	ctx    context.Context
	pin    *fdPin
	off    int64
	length int64
	closed bool
	used   bool
}

func (s *fileReadSpan) WriteTo(w io.Writer) (int64, error) {
	s.mu.Lock()
	if s.closed || s.used {
		s.mu.Unlock()
		return 0, osClosedError()
	}
	s.used = true
	f, off, length := s.pin.file(), s.off, s.length
	s.mu.Unlock()
	var written int64
	for written < length {
		if err := s.ctx.Err(); err != nil {
			return written, err
		}
		chunk := min(int64(spanWriteChunk), length-written)
		// Keep the source as a file section so destinations with a ReaderFrom
		// fast path (notably plaintext net/http connections) can promote the
		// transfer to sendfile/splice. Chunking retains a cancellation point
		// during long spans; TLS naturally falls back to userspace copying.
		n, err := io.Copy(w, io.NewSectionReader(f, off+written, chunk))
		written += n
		if err != nil {
			return written, err
		}
		if n != chunk {
			return written, io.ErrUnexpectedEOF
		}
	}
	return written, nil
}

func (s *fileReadSpan) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	pin := s.pin
	s.pin = nil
	s.mu.Unlock()
	return pin.release()
}

func writeChunks(ctx context.Context, w io.Writer, read func([]byte, int64) (int, error), length int64) (int64, error) {
	buf := make([]byte, min(int64(spanWriteChunk), length))
	var written int64
	for written < length {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		want := min(int64(len(buf)), length-written)
		n, err := read(buf[:want], written)
		if err != nil && !errors.Is(err, io.EOF) {
			return written, err
		}
		wn, writeErr := w.Write(buf[:n])
		written += int64(wn)
		if writeErr != nil {
			return written, writeErr
		}
		if wn != n {
			return written, io.ErrShortWrite
		}
		if n == 0 {
			return written, io.ErrUnexpectedEOF
		}
	}
	return written, nil
}

func osClosedError() error {
	return fmt.Errorf("seglog: span closed or already written: %w", errors.ErrUnsupported)
}
