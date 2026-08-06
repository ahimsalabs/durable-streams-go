package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// Segment v2 stores a 64-byte header followed by contiguous payload bytes.
// While active, one fixed-width dense entry per payload is appended to .idx:
// absolute payload end u64, payload CRC32C u32, and batch delta u32. Sealing
// copies the dense index behind the payload and appends the footer; the .idx
// sidecar is removed only after the checkpoint naming the sealed file.
const (
	segmentMagic      uint32 = 0x44535347 // DSSG
	segmentIndexMagic uint32 = 0x44534958 // DSIX
	segmentVersion    uint16 = 2
	segmentHeaderSize        = 64
	denseEntrySize           = 16
	segmentFooterSize        = 56

	materializerPayloadBufferBytes = 1 << 20
	materializerIndexBufferBytes   = 64 << 10
)

var errBadSegment = errors.New("seglog: invalid stream segment")

func encodeSegmentHeader(inc incarnation, firstIndex, createdUnixNano int64) []byte {
	h := make([]byte, segmentHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(h[4:6], segmentVersion)
	copy(h[8:24], inc[:])
	binary.LittleEndian.PutUint64(h[24:32], uint64(firstIndex))
	binary.LittleEndian.PutUint64(h[32:40], uint64(createdUnixNano))
	binary.LittleEndian.PutUint32(h[40:44], crc32.Checksum(h[:40], crcTable))
	return h
}

func decodeSegmentHeader(b []byte) (inc incarnation, firstIndex, createdUnixNano int64, err error) {
	if len(b) < segmentHeaderSize || binary.LittleEndian.Uint32(b[:4]) != segmentMagic ||
		binary.LittleEndian.Uint16(b[4:6]) != segmentVersion || binary.LittleEndian.Uint32(b[40:44]) != crc32.Checksum(b[:40], crcTable) {
		return inc, 0, 0, errBadSegment
	}
	copy(inc[:], b[8:24])
	return inc, int64(binary.LittleEndian.Uint64(b[24:32])), int64(binary.LittleEndian.Uint64(b[32:40])), nil
}

type segmentRecord struct {
	index      int64
	batchFirst int64
	ts         int64
	length     int32
	crc        uint32
}

type segmentFile struct {
	name            string
	path            string
	indexPath       string
	firstIndex      int64
	lastIndex       int64
	payloadEnd      int64
	count           int64
	minTS           int64
	maxTS           int64
	createdUnixNano int64
	sealed          bool
}

func segmentFileName(firstIndex int64) string  { return fmt.Sprintf("seg-%016x.seg", firstIndex) }
func segmentIndexName(firstIndex int64) string { return fmt.Sprintf("seg-%016x.idx", firstIndex) }

func createActiveSegment(dir string, inc incarnation, firstIndex, createdUnixNano int64) (*segmentFile, error) {
	name := segmentFileName(firstIndex)
	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return nil, fmt.Errorf("seglog: create stream segment: %w", err)
	}
	idxPath := filepath.Join(dir, segmentIndexName(firstIndex))
	idx, err := os.OpenFile(idxPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("seglog: create segment sidecar: %w", err)
	}
	if _, err = f.Write(encodeSegmentHeader(inc, firstIndex, createdUnixNano)); err != nil {
		_ = f.Close()
		_ = idx.Close()
		_ = os.Remove(path)
		_ = os.Remove(idxPath)
		return nil, err
	}
	_ = f.Close()
	_ = idx.Close()
	return &segmentFile{name: name, path: path, indexPath: idxPath, firstIndex: firstIndex, lastIndex: firstIndex - 1, payloadEnd: segmentHeaderSize, createdUnixNano: createdUnixNano}, nil
}

func openActiveSegment(path, name string, inc incarnation, payloadEnd, count, minTS, maxTS int64) (*segmentFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("seglog: open active segment: %w", err)
	}
	defer f.Close()
	h := make([]byte, segmentHeaderSize)
	if _, err = f.ReadAt(h, 0); err != nil {
		return nil, err
	}
	gotInc, first, created, err := decodeSegmentHeader(h)
	if err != nil || gotInc != inc || created <= 0 || payloadEnd < segmentHeaderSize || count < 0 {
		return nil, fmt.Errorf("%w: active segment %s header", errBadSegment, name)
	}
	idxPath := filepath.Join(filepath.Dir(path), segmentIndexName(first))
	idx, err := os.OpenFile(idxPath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("seglog: open active sidecar: %w", err)
	}
	defer idx.Close()
	payloadInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if payloadInfo.Size() != payloadEnd {
		if err = f.Truncate(payloadEnd); err != nil {
			return nil, err
		}
		if err = f.Sync(); err != nil {
			return nil, err
		}
	}
	indexEnd := count * denseEntrySize
	indexInfo, err := idx.Stat()
	if err != nil {
		return nil, err
	}
	if indexInfo.Size() != indexEnd {
		if err = idx.Truncate(indexEnd); err != nil {
			return nil, err
		}
		if err = idx.Sync(); err != nil {
			return nil, err
		}
	}
	return &segmentFile{name: name, path: path, indexPath: idxPath, firstIndex: first, lastIndex: first + count - 1, payloadEnd: payloadEnd, count: count, minTS: minTS, maxTS: maxTS, createdUnixNano: created}, nil
}

type writerAt interface {
	WriteAt([]byte, int64) (int, error)
}

func writeAtFull(w writerAt, data []byte, offset int64) error {
	if len(data) == 0 {
		return nil
	}
	n, err := w.WriteAt(data, offset)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func (sf *segmentFile) appendRecord(payloadFile, indexFile writerAt, rec segmentRecord, payload []byte) error {
	if rec.index != sf.firstIndex+sf.count || rec.batchFirst > rec.index || rec.index-rec.batchFirst > int64(^uint32(0)) {
		return fmt.Errorf("%w: invalid dense entry geometry", errBadSegment)
	}
	if err := writeAtFull(payloadFile, payload, sf.payloadEnd); err != nil {
		return err
	}
	end := sf.payloadEnd + int64(len(payload))
	var e [denseEntrySize]byte
	binary.LittleEndian.PutUint64(e[:8], uint64(end))
	binary.LittleEndian.PutUint32(e[8:12], crc32.Checksum(payload, crcTable))
	binary.LittleEndian.PutUint32(e[12:16], uint32(rec.index-rec.batchFirst))
	if err := writeAtFull(indexFile, e[:], sf.count*denseEntrySize); err != nil {
		return err
	}
	sf.advance(rec, end)
	return nil
}

func (sf *segmentFile) advance(rec segmentRecord, end int64) {
	sf.payloadEnd, sf.count, sf.lastIndex = end, sf.count+1, rec.index
	if sf.count == 1 && sf.minTS == 0 {
		sf.minTS = rec.ts
	}
	sf.maxTS = max(sf.maxTS, rec.ts)
}

type segmentWriteBuffer struct {
	payload       []byte
	index         []byte
	payloadOffset int64
	indexOffset   int64
}

func (b *segmentWriteBuffer) reset() {
	if cap(b.payload) != materializerPayloadBufferBytes {
		b.payload = make([]byte, 0, materializerPayloadBufferBytes)
	} else {
		b.payload = b.payload[:0]
	}
	if cap(b.index) != materializerIndexBufferBytes {
		b.index = make([]byte, 0, materializerIndexBufferBytes)
	} else {
		b.index = b.index[:0]
	}
	b.payloadOffset = 0
	b.indexOffset = 0
}

func (b *segmentWriteBuffer) wouldExceed(payloadBytes int) bool {
	return len(b.payload)+payloadBytes > materializerPayloadBufferBytes ||
		len(b.index)+denseEntrySize > materializerIndexBufferBytes
}

func (b *segmentWriteBuffer) append(sf *segmentFile, rec segmentRecord, payload []byte) error {
	if rec.index != sf.firstIndex+sf.count || rec.batchFirst > rec.index || rec.index-rec.batchFirst > int64(^uint32(0)) {
		return fmt.Errorf("%w: invalid dense entry geometry", errBadSegment)
	}
	if len(b.index) == 0 {
		b.payloadOffset = sf.payloadEnd
		b.indexOffset = sf.count * denseEntrySize
	}
	end := sf.payloadEnd + int64(len(payload))
	b.payload = append(b.payload, payload...)
	b.index = binary.LittleEndian.AppendUint64(b.index, uint64(end))
	b.index = binary.LittleEndian.AppendUint32(b.index, crc32.Checksum(payload, crcTable))
	b.index = binary.LittleEndian.AppendUint32(b.index, uint32(rec.index-rec.batchFirst))
	sf.advance(rec, end)
	return nil
}

func (b *segmentWriteBuffer) flush(payloadFile, indexFile writerAt) error {
	if len(b.index) == 0 {
		return nil
	}
	if err := writeAtFull(payloadFile, b.payload, b.payloadOffset); err != nil {
		return fmt.Errorf("write coalesced segment payload: %w", err)
	}
	if err := writeAtFull(indexFile, b.index, b.indexOffset); err != nil {
		return fmt.Errorf("write coalesced segment index: %w", err)
	}
	b.payload = b.payload[:0]
	b.index = b.index[:0]
	return nil
}

func (sf *segmentFile) seal(payloadFile, indexFile *os.File) error {
	indexBytes := sf.count * denseEntrySize
	index := make([]byte, indexBytes)
	if indexBytes > 0 {
		if _, err := indexFile.ReadAt(index, 0); err != nil {
			return err
		}
	}
	if _, err := payloadFile.WriteAt(index, sf.payloadEnd); err != nil {
		return err
	}
	footer := make([]byte, segmentFooterSize)
	binary.LittleEndian.PutUint32(footer[:4], segmentIndexMagic)
	binary.LittleEndian.PutUint16(footer[4:6], segmentVersion)
	binary.LittleEndian.PutUint64(footer[8:16], uint64(sf.payloadEnd))
	binary.LittleEndian.PutUint64(footer[16:24], uint64(sf.count))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(sf.firstIndex))
	binary.LittleEndian.PutUint64(footer[32:40], uint64(sf.lastIndex))
	binary.LittleEndian.PutUint64(footer[40:48], uint64(sf.maxTS))
	binary.LittleEndian.PutUint32(footer[48:52], crc32.Checksum(index, crcTable))
	binary.LittleEndian.PutUint32(footer[52:56], crc32.Checksum(footer[:52], crcTable))
	if _, err := payloadFile.WriteAt(footer, sf.payloadEnd+indexBytes); err != nil {
		return err
	}
	sf.sealed = true
	return nil
}

func openSealedSegment(path, name string, inc incarnation) (*segmentFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < segmentHeaderSize+segmentFooterSize {
		return nil, errBadSegment
	}
	h := make([]byte, segmentHeaderSize)
	if _, err = f.ReadAt(h, 0); err != nil {
		return nil, err
	}
	gotInc, first, created, err := decodeSegmentHeader(h)
	if err != nil || gotInc != inc {
		return nil, errBadSegment
	}
	footer := make([]byte, segmentFooterSize)
	if _, err = f.ReadAt(footer, info.Size()-segmentFooterSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[:4]) != segmentIndexMagic || binary.LittleEndian.Uint16(footer[4:6]) != segmentVersion || binary.LittleEndian.Uint32(footer[52:]) != crc32.Checksum(footer[:52], crcTable) {
		return nil, errBadSegment
	}
	payloadEnd := int64(binary.LittleEndian.Uint64(footer[8:16]))
	count := int64(binary.LittleEndian.Uint64(footer[16:24]))
	last := int64(binary.LittleEndian.Uint64(footer[32:40]))
	if int64(binary.LittleEndian.Uint64(footer[24:32])) != first || last != first+count-1 || payloadEnd < segmentHeaderSize || payloadEnd+count*denseEntrySize+segmentFooterSize != info.Size() {
		return nil, errBadSegment
	}
	index := make([]byte, count*denseEntrySize)
	if len(index) > 0 {
		if _, err = f.ReadAt(index, payloadEnd); err != nil {
			return nil, err
		}
	}
	if binary.LittleEndian.Uint32(footer[48:52]) != crc32.Checksum(index, crcTable) {
		return nil, errBadSegment
	}
	return &segmentFile{name: name, path: path, firstIndex: first, lastIndex: last, payloadEnd: payloadEnd, count: count, maxTS: int64(binary.LittleEndian.Uint64(footer[40:48])), createdUnixNano: created, sealed: true}, nil
}

type segmentView struct {
	cache                                    *fdCache
	path, indexPath, name                    string
	firstIndex, lastIndex, payloadEnd, count int64
	sealed                                   bool
}

func (sf *segmentFile) view(cache *fdCache) segmentView {
	return segmentView{cache: cache, path: sf.path, indexPath: sf.indexPath, name: sf.name, firstIndex: sf.firstIndex, lastIndex: sf.lastIndex, payloadEnd: sf.payloadEnd, count: sf.count, sealed: sf.sealed}
}

func (v segmentView) readRecords(from, through int64, visit func(segmentRecord, int64) error) error {
	if v.path == "" || through < from || v.lastIndex < from {
		return nil
	}
	p, err := v.cache.pin(v.path, false)
	if err != nil {
		return err
	}
	defer func() { _ = p.release() }() //nolint:errcheck // read pin; nothing actionable
	indexFile := p.file()
	var ip *fdPin
	indexStart := v.payloadEnd
	if !v.sealed {
		ip, err = v.cache.pin(v.indexPath, false)
		if err != nil {
			return err
		}
		defer func() { _ = ip.release() }() //nolint:errcheck // read pin; nothing actionable
		indexFile = ip.file()
		indexStart = 0
	}
	startOrdinal := max(from, v.firstIndex) - v.firstIndex
	for ordinal := startOrdinal; ordinal < v.count; ordinal++ {
		idx := v.firstIndex + ordinal
		if idx > through {
			break
		}
		var e [denseEntrySize]byte
		if _, err := indexFile.ReadAt(e[:], indexStart+ordinal*denseEntrySize); err != nil {
			return err
		}
		end := int64(binary.LittleEndian.Uint64(e[:8]))
		start := int64(segmentHeaderSize)
		if ordinal > 0 {
			var prev [8]byte
			if _, err := indexFile.ReadAt(prev[:], indexStart+(ordinal-1)*denseEntrySize); err != nil {
				return err
			}
			start = int64(binary.LittleEndian.Uint64(prev[:]))
		}
		if end < start || end > v.payloadEnd || end-start > int64(^uint32(0)>>1) {
			return errBadSegment
		}
		delta := int64(binary.LittleEndian.Uint32(e[12:16]))
		if delta > idx {
			return errBadSegment
		}
		rec := segmentRecord{index: idx, batchFirst: idx - delta, length: int32(end - start), crc: binary.LittleEndian.Uint32(e[8:12])}
		if err := visit(rec, start); err != nil {
			return err
		}
	}
	return nil
}

func (v segmentView) readPayloadAt(rec segmentRecord, off int64) ([]byte, error) {
	p, err := v.cache.pin(v.path, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.release() }() //nolint:errcheck // read pin; nothing actionable
	b := make([]byte, rec.length)
	if _, err = p.file().ReadAt(b, off); err != nil && err != io.EOF {
		return nil, err
	}
	if crc32.Checksum(b, crcTable) != rec.crc {
		return nil, errBadSegment
	}
	return b, nil
}
