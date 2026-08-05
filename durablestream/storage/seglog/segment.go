package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
)

// Stream segment byte layout (all integers little-endian).
//
// Header (64 bytes): magic "DSSG", version u16, flags u16, incarnation
// [16]byte, firstIndex u64, createdUnixNano i64, headerCRC u32, zero pad.
//
// Record (32-byte header + payload):
//
//	u64 index | u64 batchFirst | i64 tsUnixNano | u32 len | u32 crc32c(payload)
//
// Footer, present only in sealed segments, written at seal time: sparse index
// entries (u64 index, u64 fileOffset each, offsets pointing at record
// headers), then a 48-byte trailer:
//
//	u32 magic "DSIX" | u32 entryCount | u64 indexStart | u64 firstIndex |
//	u64 lastIndex | i64 maxTsUnixNano | u32 footerCRC (over the entries and
//	the trailer up to this field) | u32 reserved
//
// The active segment has no footer; its sparse index lives in memory and is
// rebuilt on recovery by scanning to the manifest-recorded byte length.
const (
	segmentMagic            uint32 = 0x44535347 // "DSSG"
	segmentIndexMagic       uint32 = 0x44534958 // "DSIX"
	segmentVersion          uint16 = 1
	segmentHeaderSize              = 64
	segmentRecordHeaderSize        = 32
	segmentFooterSize              = 48
	sparseEntrySize                = 16
)

func encodeSegmentHeader(inc incarnation, firstIndex, createdUnixNano int64) []byte {
	hdr := make([]byte, segmentHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], segmentVersion)
	copy(hdr[8:24], inc[:])
	binary.LittleEndian.PutUint64(hdr[24:32], uint64(firstIndex))
	binary.LittleEndian.PutUint64(hdr[32:40], uint64(createdUnixNano))
	binary.LittleEndian.PutUint32(hdr[40:44], crc32.Checksum(hdr[0:40], crcTable))
	return hdr
}

var errBadSegment = errors.New("seglog: invalid stream segment")

func decodeSegmentHeader(b []byte) (inc incarnation, firstIndex int64, err error) {
	if len(b) < segmentHeaderSize ||
		binary.LittleEndian.Uint32(b[0:4]) != segmentMagic ||
		binary.LittleEndian.Uint16(b[4:6]) != segmentVersion ||
		binary.LittleEndian.Uint32(b[40:44]) != crc32.Checksum(b[0:40], crcTable) {
		return incarnation{}, 0, errBadSegment
	}
	copy(inc[:], b[8:24])
	return inc, int64(binary.LittleEndian.Uint64(b[24:32])), nil
}

// sparseEntry maps a logical index to the file offset of its record header.
type sparseEntry struct {
	index int64
	off   int64
}

// segmentRecord is one decoded record header.
type segmentRecord struct {
	index      int64
	batchFirst int64
	ts         int64
	length     int32
	crc        uint32
}

func decodeSegmentRecordHeader(b []byte) segmentRecord {
	return segmentRecord{
		index:      int64(binary.LittleEndian.Uint64(b[0:8])),
		batchFirst: int64(binary.LittleEndian.Uint64(b[8:16])),
		ts:         int64(binary.LittleEndian.Uint64(b[16:24])),
		length:     int32(binary.LittleEndian.Uint32(b[24:28])),
		crc:        binary.LittleEndian.Uint32(b[28:32]),
	}
}

// segmentFile is one stream segment (sealed or active) and the metadata
// needed to read from it. The materializer mutates it; readers use only
// immutable fields plus ReadAt on the descriptor. Sparse offsets are
// absolute file offsets of record headers; bytes is the absolute end of the
// record area (records start at segmentHeaderSize).
type segmentFile struct {
	f          *os.File
	name       string // file name within the stream directory
	firstIndex int64
	lastIndex  int64 // last record present (firstIndex-1 when empty)
	bytes      int64 // absolute end of the record area
	maxTS      int64
	sparse     []sparseEntry
	sealed     bool

	// Writer-side state for the active segment.
	sinceIndex int64 // bytes written since the last sparse entry
}

// segmentFileName returns the on-disk name for a segment starting at
// firstIndex.
func segmentFileName(firstIndex int64) string {
	return fmt.Sprintf("seg-%016x.seg", firstIndex)
}

// createActiveSegment creates a new active segment file with a durable
// header. Records land behind it; the file is fsync'd by the materializer
// before any manifest references its contents.
func createActiveSegment(dir string, inc incarnation, firstIndex, createdUnixNano int64) (*segmentFile, error) {
	name := segmentFileName(firstIndex)
	f, err := os.OpenFile(dir+"/"+name, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("seglog: create stream segment: %w", err)
	}
	if _, err := f.WriteAt(encodeSegmentHeader(inc, firstIndex, createdUnixNano), 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seglog: write stream segment header: %w", err)
	}
	return &segmentFile{
		f:          f,
		name:       name,
		firstIndex: firstIndex,
		lastIndex:  firstIndex - 1,
		bytes:      segmentHeaderSize,
	}, nil
}

// openActiveSegment reopens the manifest's active segment: truncate to the
// recorded record-area end (dropping any partially-materialized suffix, which
// the WAL re-materializes), then scan to rebuild the in-memory sparse index.
func openActiveSegment(path, name string, inc incarnation, recordEnd int64, sparseEvery int) (*segmentFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("seglog: open active segment: %w", err)
	}
	sf, err := loadActiveSegment(f, name, inc, recordEnd, sparseEvery)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

func loadActiveSegment(f *os.File, name string, inc incarnation, recordEnd int64, sparseEvery int) (*segmentFile, error) {
	hdrBuf := make([]byte, segmentHeaderSize)
	if _, err := f.ReadAt(hdrBuf, 0); err != nil {
		return nil, fmt.Errorf("seglog: read active segment header: %w", err)
	}
	segInc, firstIndex, err := decodeSegmentHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("%w: active segment %s header", errBadSegment, name)
	}
	if segInc != inc {
		return nil, fmt.Errorf("%w: active segment %s belongs to another incarnation", errBadSegment, name)
	}
	if recordEnd < segmentHeaderSize {
		return nil, fmt.Errorf("%w: active segment %s record end %d", errBadSegment, name, recordEnd)
	}
	if err := f.Truncate(recordEnd); err != nil {
		return nil, fmt.Errorf("seglog: truncate active segment: %w", err)
	}
	if err := f.Sync(); err != nil {
		return nil, fmt.Errorf("seglog: sync truncated active segment: %w", err)
	}

	sf := &segmentFile{
		f:          f,
		name:       name,
		firstIndex: firstIndex,
		lastIndex:  firstIndex - 1,
		bytes:      segmentHeaderSize,
	}
	var hdr [segmentRecordHeaderSize]byte
	for sf.bytes < recordEnd {
		if _, err := f.ReadAt(hdr[:], sf.bytes); err != nil {
			return nil, fmt.Errorf("seglog: scan active segment at %d: %w", sf.bytes, err)
		}
		rec := decodeSegmentRecordHeader(hdr[:])
		next := sf.bytes + segmentRecordHeaderSize + int64(rec.length)
		if rec.length < 0 || next > recordEnd || rec.index != sf.lastIndex+1 {
			return nil, fmt.Errorf("%w: active segment %s record at %d", errBadSegment, name, sf.bytes)
		}
		if sf.sinceIndex == 0 || sf.sinceIndex >= int64(sparseEvery) {
			sf.sparse = append(sf.sparse, sparseEntry{index: rec.index, off: sf.bytes})
			sf.sinceIndex = 0
		}
		written := next - sf.bytes
		sf.sinceIndex += written
		sf.bytes = next
		sf.lastIndex = rec.index
		sf.maxTS = max(sf.maxTS, rec.ts)
	}
	if sf.bytes != recordEnd {
		return nil, fmt.Errorf("%w: active segment %s records end at %d, manifest says %d",
			errBadSegment, name, sf.bytes, recordEnd)
	}
	return sf, nil
}

// appendRecord writes one record at the current end of the segment. The
// caller (materializer) is single-threaded per stream.
func (sf *segmentFile) appendRecord(rec segmentRecord, payload []byte, sparseEvery int) error {
	var hdr [segmentRecordHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(rec.index))
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(rec.batchFirst))
	binary.LittleEndian.PutUint64(hdr[16:24], uint64(rec.ts))
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(payload, crcTable))

	if sf.sinceIndex == 0 || sf.sinceIndex >= int64(sparseEvery) {
		sf.sparse = append(sf.sparse, sparseEntry{index: rec.index, off: sf.bytes})
		sf.sinceIndex = 0
	}
	if _, err := sf.f.WriteAt(hdr[:], sf.bytes); err != nil {
		return fmt.Errorf("seglog: write segment record header: %w", err)
	}
	if _, err := sf.f.WriteAt(payload, sf.bytes+segmentRecordHeaderSize); err != nil {
		return fmt.Errorf("seglog: write segment record payload: %w", err)
	}
	written := int64(segmentRecordHeaderSize + len(payload))
	sf.bytes += written
	sf.sinceIndex += written
	sf.lastIndex = rec.index
	sf.maxTS = max(sf.maxTS, rec.ts)
	return nil
}

// seal writes the footer and makes the segment immutable and durable. The
// footer CRC covers the sparse entries and every trailer field before it.
func (sf *segmentFile) seal() error {
	buf := make([]byte, 0, len(sf.sparse)*sparseEntrySize+segmentFooterSize)
	for _, e := range sf.sparse {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.index))
		buf = binary.LittleEndian.AppendUint64(buf, uint64(e.off))
	}
	buf = binary.LittleEndian.AppendUint32(buf, segmentIndexMagic)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(sf.sparse)))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sf.bytes))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sf.firstIndex))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sf.lastIndex))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(sf.maxTS))
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, crcTable))
	buf = binary.LittleEndian.AppendUint32(buf, 0)

	if _, err := sf.f.WriteAt(buf, sf.bytes); err != nil {
		return fmt.Errorf("seglog: write segment footer: %w", err)
	}
	if err := sf.f.Sync(); err != nil {
		return fmt.Errorf("seglog: sync sealed segment: %w", err)
	}
	sf.sealed = true
	return nil
}

// openSealedSegment opens a sealed segment file and loads its sparse index
// from the footer.
func openSealedSegment(path, name string, inc incarnation) (*segmentFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("seglog: open sealed segment: %w", err)
	}
	sf, err := loadSealedSegment(f, name, inc)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return sf, nil
}

func loadSealedSegment(f *os.File, name string, inc incarnation) (*segmentFile, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("seglog: stat sealed segment: %w", err)
	}
	size := info.Size()
	if size < segmentHeaderSize+segmentFooterSize {
		return nil, fmt.Errorf("%w: sealed segment %s is too short", errBadSegment, name)
	}

	hdrBuf := make([]byte, segmentHeaderSize)
	if _, err := f.ReadAt(hdrBuf, 0); err != nil {
		return nil, fmt.Errorf("seglog: read segment header: %w", err)
	}
	segInc, firstIndex, err := decodeSegmentHeader(hdrBuf)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errBadSegment, name)
	}
	if segInc != inc {
		return nil, fmt.Errorf("%w: segment %s belongs to another incarnation", errBadSegment, name)
	}

	trailer := make([]byte, segmentFooterSize)
	if _, err := f.ReadAt(trailer, size-segmentFooterSize); err != nil {
		return nil, fmt.Errorf("seglog: read segment footer: %w", err)
	}
	if binary.LittleEndian.Uint32(trailer[0:4]) != segmentIndexMagic {
		return nil, fmt.Errorf("%w: segment %s has no footer", errBadSegment, name)
	}
	entryCount := int64(binary.LittleEndian.Uint32(trailer[4:8]))
	indexStart := int64(binary.LittleEndian.Uint64(trailer[8:16]))
	first := int64(binary.LittleEndian.Uint64(trailer[16:24]))
	last := int64(binary.LittleEndian.Uint64(trailer[24:32]))
	maxTS := int64(binary.LittleEndian.Uint64(trailer[32:40]))
	if first != firstIndex || indexStart < segmentHeaderSize ||
		indexStart+entryCount*sparseEntrySize+segmentFooterSize != size {
		return nil, fmt.Errorf("%w: segment %s footer geometry", errBadSegment, name)
	}

	covered := make([]byte, entryCount*sparseEntrySize+segmentFooterSize-8)
	if _, err := f.ReadAt(covered, indexStart); err != nil {
		return nil, fmt.Errorf("seglog: read segment index: %w", err)
	}
	wantCRC := binary.LittleEndian.Uint32(trailer[40:44])
	if crc32.Checksum(covered, crcTable) != wantCRC {
		return nil, fmt.Errorf("%w: segment %s footer checksum", errBadSegment, name)
	}
	entriesBuf := covered[:entryCount*sparseEntrySize]

	sparse := make([]sparseEntry, entryCount)
	for i := range sparse {
		base := i * sparseEntrySize
		sparse[i] = sparseEntry{
			index: int64(binary.LittleEndian.Uint64(entriesBuf[base : base+8])),
			off:   int64(binary.LittleEndian.Uint64(entriesBuf[base+8 : base+16])),
		}
	}
	return &segmentFile{
		f:          f,
		name:       name,
		firstIndex: firstIndex,
		lastIndex:  last,
		bytes:      indexStart,
		maxTS:      maxTS,
		sparse:     sparse,
		sealed:     true,
	}, nil
}

// readRecords calls visit for every record with index in [from, through],
// locating the start with the sparse index and scanning forward. visit
// receives the record and its payload's file offset. Reading a view is safe
// concurrently with the materializer: records below v.end are never
// rewritten.
func (v segmentView) readRecords(from, through int64, visit func(rec segmentRecord, payloadOff int64) error) error {
	if v.f == nil || through < from || v.lastIndex < from {
		return nil
	}
	off := int64(segmentHeaderSize)
	for _, e := range v.sparse {
		if e.index > from {
			break
		}
		off = e.off
	}

	var hdr [segmentRecordHeaderSize]byte
	for off < v.end {
		if _, err := v.f.ReadAt(hdr[:], off); err != nil {
			return fmt.Errorf("seglog: read segment record at %d: %w", off, err)
		}
		rec := decodeSegmentRecordHeader(hdr[:])
		next := off + segmentRecordHeaderSize + int64(rec.length)
		if rec.length < 0 || next > v.end {
			return fmt.Errorf("%w: segment %s record at %d overruns data area", errBadSegment, v.name, off)
		}
		if rec.index > through {
			return nil
		}
		if rec.index >= from {
			if err := visit(rec, off+segmentRecordHeaderSize); err != nil {
				return err
			}
		}
		off = next
	}
	return nil
}

// readPayloadAt copies and verifies one record payload.
func (v segmentView) readPayloadAt(rec segmentRecord, payloadOff int64) ([]byte, error) {
	payload := make([]byte, rec.length)
	if _, err := v.f.ReadAt(payload, payloadOff); err != nil {
		return nil, fmt.Errorf("seglog: read segment payload: %w", err)
	}
	if crc32.Checksum(payload, crcTable) != rec.crc {
		return nil, fmt.Errorf("%w: segment %s payload checksum at %d", errBadSegment, v.name, payloadOff)
	}
	return payload, nil
}
