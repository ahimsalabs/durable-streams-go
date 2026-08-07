package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/klauspost/compress/zstd"
)

// Segment v2 stores a 64-byte header followed by contiguous payload bytes.
// While active, one fixed-width dense entry per payload is appended to .idx:
// absolute payload end u64, payload CRC32C u32, and batch delta u32. Sealing
// copies the dense index behind the payload and appends the footer; the .idx
// sidecar is removed only after the checkpoint naming the sealed file.
const (
	segmentMagic         uint32 = 0x44535347 // DSSG
	segmentIndexMagic    uint32 = 0x44534958 // DSIX
	segmentVersionV2     uint16 = 2
	segmentVersionV3     uint16 = 3
	segmentVersion              = segmentVersionV2 // legacy test fixtures
	segmentHeaderSize           = 64
	denseEntrySize              = 16
	segmentFooterSize           = 56
	compressedFooterSize        = 72
	blockEntrySize              = 32

	materializerPayloadBufferBytes = 1 << 20
	materializerIndexBufferBytes   = 64 << 10
)

var errBadSegment = errors.New("seglog: invalid stream segment")

func checkedMul(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt64/a) {
		return 0, false
	}
	return a * b, true
}

func checkedAdd(a, b int64) (int64, bool) {
	if a < 0 || b < 0 || a > math.MaxInt64-b {
		return 0, false
	}
	return a + b, true
}

func checkedInt(n int64) (int, bool) {
	if n < 0 || uint64(n) > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(n), true
}

func encodeSegmentHeader(inc incarnation, firstIndex, createdUnixNano int64, versions ...uint16) []byte {
	version := uint16(segmentVersionV2)
	if len(versions) > 0 {
		version = versions[0]
	}
	h := make([]byte, segmentHeaderSize)
	binary.LittleEndian.PutUint32(h[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(h[4:6], version)
	copy(h[8:24], inc[:])
	binary.LittleEndian.PutUint64(h[24:32], uint64(firstIndex))
	binary.LittleEndian.PutUint64(h[32:40], uint64(createdUnixNano))
	binary.LittleEndian.PutUint32(h[40:44], crc32.Checksum(h[:40], crcTable))
	return h
}

func decodeSegmentHeader(b []byte) (inc incarnation, firstIndex, createdUnixNano int64, err error) {
	inc, firstIndex, createdUnixNano, _, err = decodeSegmentHeaderVersion(b)
	return
}

func decodeSegmentHeaderVersion(b []byte) (inc incarnation, firstIndex, createdUnixNano int64, version uint16, err error) {
	if len(b) < segmentHeaderSize || binary.LittleEndian.Uint32(b[:4]) != segmentMagic ||
		binary.LittleEndian.Uint32(b[40:44]) != crc32.Checksum(b[:40], crcTable) {
		return inc, 0, 0, 0, errBadSegment
	}
	version = binary.LittleEndian.Uint16(b[4:6])
	if version != segmentVersionV2 && version != segmentVersionV3 {
		return inc, 0, 0, 0, errBadSegment
	}
	copy(inc[:], b[8:24])
	return inc, int64(binary.LittleEndian.Uint64(b[24:32])), int64(binary.LittleEndian.Uint64(b[32:40])), version, nil
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
	blockPath       string
	version         uint16
	firstIndex      int64
	lastIndex       int64
	payloadEnd      int64
	count           int64
	logicalEnd      int64
	blockCount      int64
	minTS           int64
	maxTS           int64
	createdUnixNano int64
	sealed          bool
}

func segmentFileName(firstIndex int64) string  { return fmt.Sprintf("seg-%016x.seg", firstIndex) }
func segmentIndexName(firstIndex int64) string { return fmt.Sprintf("seg-%016x.idx", firstIndex) }
func segmentBlockName(firstIndex int64) string { return fmt.Sprintf("seg-%016x.bix", firstIndex) }

func createActiveSegment(dir string, inc incarnation, firstIndex, createdUnixNano int64, compressed bool) (*segmentFile, error) {
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
	version := segmentVersionV2
	blockPath := ""
	if compressed {
		version = segmentVersionV3
		blockPath = filepath.Join(dir, segmentBlockName(firstIndex))
		block, blockErr := os.OpenFile(blockPath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
		if blockErr != nil {
			_ = f.Close()
			_ = idx.Close()
			_ = os.Remove(path)
			_ = os.Remove(idxPath)
			return nil, blockErr
		}
		_ = block.Close()
	}
	if _, err = f.Write(encodeSegmentHeader(inc, firstIndex, createdUnixNano, version)); err != nil {
		_ = f.Close()
		_ = idx.Close()
		_ = os.Remove(path)
		_ = os.Remove(idxPath)
		_ = os.Remove(blockPath)
		return nil, err
	}
	_ = f.Close()
	_ = idx.Close()
	return &segmentFile{name: name, path: path, indexPath: idxPath, blockPath: blockPath, version: version, firstIndex: firstIndex, lastIndex: firstIndex - 1, payloadEnd: segmentHeaderSize, createdUnixNano: createdUnixNano}, nil
}

func openActiveSegment(path, name string, inc incarnation, payloadEnd, logicalEnd, count, blockCount, minTS, maxTS int64) (*segmentFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("seglog: open active segment: %w", err)
	}
	defer f.Close()
	h := make([]byte, segmentHeaderSize)
	if _, err = f.ReadAt(h, 0); err != nil {
		return nil, err
	}
	gotInc, first, created, version, err := decodeSegmentHeaderVersion(h)
	indexEnd, indexOK := checkedMul(count, denseEntrySize)
	blockEnd, blockOK := checkedMul(blockCount, blockEntrySize)
	if err != nil || gotInc != inc || created <= 0 || payloadEnd < segmentHeaderSize || !indexOK || !blockOK {
		return nil, fmt.Errorf("%w: active segment %s header", errBadSegment, name)
	}
	idxPath := filepath.Join(filepath.Dir(path), segmentIndexName(first))
	idx, err := os.OpenFile(idxPath, os.O_RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("seglog: open active sidecar: %w", err)
	}
	defer idx.Close()
	indexInfo, err := idx.Stat()
	if err != nil {
		return nil, err
	}
	if indexInfo.Size() < indexEnd {
		return nil, errBadSegment
	}
	blockPath := ""
	var block *os.File
	if version == segmentVersionV3 {
		blockPath = filepath.Join(filepath.Dir(path), segmentBlockName(first))
		var blockErr error
		block, blockErr = os.OpenFile(blockPath, os.O_RDWR, 0)
		if blockErr != nil {
			return nil, blockErr
		}
		defer block.Close()
		blockInfo, statErr := block.Stat()
		if statErr != nil {
			return nil, statErr
		}
		if blockInfo.Size() < blockEnd {
			return nil, errBadSegment
		}
		index, readErr := readBoundedAt(idx, indexEnd, 0, indexEnd)
		if readErr != nil {
			return nil, errBadSegment
		}
		blocks, readErr := readBoundedAt(block, blockEnd, 0, blockEnd)
		if readErr != nil {
			return nil, errBadSegment
		}
		if validateV3Metadata(index, blocks, count, blockCount, payloadEnd, logicalEnd) != nil {
			return nil, errBadSegment
		}
	} else if logicalEnd != 0 || blockCount != 0 {
		return nil, errBadSegment
	}
	payloadInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if payloadInfo.Size() < payloadEnd {
		return nil, errBadSegment
	}
	last, ok := checkedAdd(first, count-1)
	if count == 0 {
		last, ok = first-1, first > math.MinInt64
	}
	if !ok {
		return nil, errBadSegment
	}
	// Validate the complete retained prefix before any destructive truncation.
	for _, target := range []struct {
		file *os.File
		size int64
	}{
		{f, payloadEnd},
		{idx, indexEnd},
		{block, blockEnd},
	} {
		if target.file == nil {
			continue
		}
		info, statErr := target.file.Stat()
		if statErr != nil {
			return nil, statErr
		}
		if info.Size() != target.size {
			if err = target.file.Truncate(target.size); err != nil {
				return nil, err
			}
			if err = target.file.Sync(); err != nil {
				return nil, err
			}
		}
	}
	return &segmentFile{name: name, path: path, indexPath: idxPath, blockPath: blockPath, version: version, firstIndex: first, lastIndex: last, payloadEnd: payloadEnd, logicalEnd: logicalEnd, count: count, blockCount: blockCount, minTS: minTS, maxTS: maxTS, createdUnixNano: created}, nil
}

func readBoundedAt(r io.ReaderAt, length, offset, limit int64) ([]byte, error) {
	if length > limit || offset < 0 {
		return nil, errBadSegment
	}
	n, ok := checkedInt(length)
	if !ok {
		return nil, errBadSegment
	}
	b := make([]byte, n)
	if n > 0 {
		if _, err := r.ReadAt(b, offset); err != nil {
			return nil, err
		}
	}
	return b, nil
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
	if sf.version == segmentVersionV3 {
		end = sf.logicalEnd + int64(len(payload))
	}
	b.payload = append(b.payload, payload...)
	b.index = binary.LittleEndian.AppendUint64(b.index, uint64(end))
	b.index = binary.LittleEndian.AppendUint32(b.index, crc32.Checksum(payload, crcTable))
	b.index = binary.LittleEndian.AppendUint32(b.index, uint32(rec.index-rec.batchFirst))
	if sf.version == segmentVersionV3 {
		sf.logicalEnd, sf.count, sf.lastIndex = end, sf.count+1, rec.index
		if sf.count == 1 && sf.minTS == 0 {
			sf.minTS = rec.ts
		}
		sf.maxTS = max(sf.maxTS, rec.ts)
	} else {
		sf.advance(rec, end)
	}
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

// flushCompressed closes one independent frame. Its encoder belongs to the
// current materialization visit, so no compression state crosses visits.
func (b *segmentWriteBuffer) flushCompressed(sf *segmentFile, payloadFile, indexFile, blockFile writerAt, enc *zstd.Encoder) error {
	if len(b.index) == 0 {
		return nil
	}
	frame := enc.EncodeAll(b.payload, nil)
	if err := writeAtFull(payloadFile, frame, sf.payloadEnd); err != nil {
		return err
	}
	if err := writeAtFull(indexFile, b.index, b.indexOffset); err != nil {
		return err
	}
	var block [blockEntrySize]byte
	firstOrdinal := b.indexOffset / denseEntrySize
	binary.LittleEndian.PutUint64(block[0:8], uint64(firstOrdinal))
	binary.LittleEndian.PutUint64(block[8:16], uint64(sf.payloadEnd))
	binary.LittleEndian.PutUint64(block[16:24], uint64(len(frame)))
	binary.LittleEndian.PutUint64(block[24:32], uint64(len(b.payload)))
	if err := writeAtFull(blockFile, block[:], sf.blockCount*blockEntrySize); err != nil {
		return err
	}
	sf.payloadEnd += int64(len(frame))
	sf.blockCount++
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
	if sf.version == segmentVersionV3 {
		blockFile, err := os.Open(sf.blockPath)
		if err != nil {
			return err
		}
		blocks := make([]byte, sf.blockCount*blockEntrySize)
		if len(blocks) > 0 {
			_, err = blockFile.ReadAt(blocks, 0)
		}
		closeErr := blockFile.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if _, err = payloadFile.WriteAt(blocks, sf.payloadEnd+indexBytes); err != nil {
			return err
		}
		footer := make([]byte, compressedFooterSize)
		binary.LittleEndian.PutUint32(footer[:4], segmentIndexMagic)
		binary.LittleEndian.PutUint16(footer[4:6], segmentVersionV3)
		binary.LittleEndian.PutUint64(footer[8:16], uint64(sf.payloadEnd))
		binary.LittleEndian.PutUint64(footer[16:24], uint64(sf.count))
		binary.LittleEndian.PutUint64(footer[24:32], uint64(sf.firstIndex))
		binary.LittleEndian.PutUint64(footer[32:40], uint64(sf.lastIndex))
		binary.LittleEndian.PutUint64(footer[40:48], uint64(sf.maxTS))
		binary.LittleEndian.PutUint64(footer[48:56], uint64(sf.logicalEnd))
		binary.LittleEndian.PutUint64(footer[56:64], uint64(sf.blockCount))
		binary.LittleEndian.PutUint32(footer[64:68], crc32.Checksum(append(index, blocks...), crcTable))
		binary.LittleEndian.PutUint32(footer[68:72], crc32.Checksum(footer[:68], crcTable))
		if _, err = payloadFile.WriteAt(footer, sf.payloadEnd+indexBytes+int64(len(blocks))); err != nil {
			return err
		}
		sf.sealed = true
		return nil
	}
	footer := make([]byte, segmentFooterSize)
	binary.LittleEndian.PutUint32(footer[:4], segmentIndexMagic)
	binary.LittleEndian.PutUint16(footer[4:6], segmentVersionV2)
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
	gotInc, first, created, version, err := decodeSegmentHeaderVersion(h)
	if err != nil || gotInc != inc {
		return nil, errBadSegment
	}
	if version == segmentVersionV3 {
		if info.Size() < segmentHeaderSize+compressedFooterSize {
			return nil, errBadSegment
		}
		footer := make([]byte, compressedFooterSize)
		if _, err = f.ReadAt(footer, info.Size()-compressedFooterSize); err != nil {
			return nil, err
		}
		if binary.LittleEndian.Uint32(footer[:4]) != segmentIndexMagic || binary.LittleEndian.Uint16(footer[4:6]) != version || binary.LittleEndian.Uint32(footer[68:72]) != crc32.Checksum(footer[:68], crcTable) {
			return nil, errBadSegment
		}
		payloadEnd := int64(binary.LittleEndian.Uint64(footer[8:16]))
		count := int64(binary.LittleEndian.Uint64(footer[16:24]))
		last := int64(binary.LittleEndian.Uint64(footer[32:40]))
		logicalEnd := int64(binary.LittleEndian.Uint64(footer[48:56]))
		blockCount := int64(binary.LittleEndian.Uint64(footer[56:64]))
		indexLen, indexOK := checkedMul(count, denseEntrySize)
		blockLen, blockOK := checkedMul(blockCount, blockEntrySize)
		metaLen, metaOK := checkedAdd(indexLen, blockLen)
		fileEnd, fileOK := checkedAdd(payloadEnd, metaLen)
		fileEnd, footerOK := checkedAdd(fileEnd, compressedFooterSize)
		expectedLast, lastOK := checkedAdd(first, count-1)
		if payloadEnd < segmentHeaderSize || !indexOK || !blockOK || !metaOK || !fileOK || !footerOK || !lastOK || count == 0 || last != expectedLast || fileEnd != info.Size() {
			return nil, errBadSegment
		}
		meta, err := readBoundedAt(f, metaLen, payloadEnd, info.Size()-payloadEnd-compressedFooterSize)
		if err != nil {
			return nil, errBadSegment
		}
		if binary.LittleEndian.Uint32(footer[64:68]) != crc32.Checksum(meta, crcTable) {
			return nil, errBadSegment
		}
		if err := validateV3Metadata(meta[:indexLen], meta[indexLen:], count, blockCount, payloadEnd, logicalEnd); err != nil {
			return nil, err
		}
		return &segmentFile{name: name, path: path, version: version, firstIndex: first, lastIndex: last, payloadEnd: payloadEnd, logicalEnd: logicalEnd, count: count, blockCount: blockCount, maxTS: int64(binary.LittleEndian.Uint64(footer[40:48])), createdUnixNano: created, sealed: true}, nil
	}
	footer := make([]byte, segmentFooterSize)
	if _, err = f.ReadAt(footer, info.Size()-segmentFooterSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[:4]) != segmentIndexMagic || binary.LittleEndian.Uint16(footer[4:6]) != segmentVersionV2 || binary.LittleEndian.Uint32(footer[52:]) != crc32.Checksum(footer[:52], crcTable) {
		return nil, errBadSegment
	}
	payloadEnd := int64(binary.LittleEndian.Uint64(footer[8:16]))
	count := int64(binary.LittleEndian.Uint64(footer[16:24]))
	last := int64(binary.LittleEndian.Uint64(footer[32:40]))
	indexLen, indexOK := checkedMul(count, denseEntrySize)
	end, endOK := checkedAdd(payloadEnd, indexLen)
	end, footerOK := checkedAdd(end, segmentFooterSize)
	expectedLast, lastOK := checkedAdd(first, count-1)
	if int64(binary.LittleEndian.Uint64(footer[24:32])) != first || !indexOK || !endOK || !footerOK || !lastOK || count == 0 || last != expectedLast || payloadEnd < segmentHeaderSize || end != info.Size() {
		return nil, errBadSegment
	}
	index, err := readBoundedAt(f, indexLen, payloadEnd, info.Size()-payloadEnd-segmentFooterSize)
	if err != nil {
		return nil, errBadSegment
	}
	if binary.LittleEndian.Uint32(footer[48:52]) != crc32.Checksum(index, crcTable) {
		return nil, errBadSegment
	}
	return &segmentFile{name: name, path: path, version: version, firstIndex: first, lastIndex: last, payloadEnd: payloadEnd, count: count, maxTS: int64(binary.LittleEndian.Uint64(footer[40:48])), createdUnixNano: created, sealed: true}, nil
}

func validateV3Metadata(index, blocks []byte, count, blockCount, payloadEnd, logicalEnd int64) error {
	indexLen, iok := checkedMul(count, denseEntrySize)
	blockLen, bok := checkedMul(blockCount, blockEntrySize)
	if !iok || !bok || int64(len(index)) != indexLen || int64(len(blocks)) != blockLen || count <= 0 || blockCount <= 0 || logicalEnd < 0 {
		return errBadSegment
	}
	denseEnds := make([]int64, len(index)/denseEntrySize)
	var previousEnd int64
	for i := range denseEnds {
		end := int64(binary.LittleEndian.Uint64(index[i*denseEntrySize:]))
		if end < previousEnd || end > logicalEnd || end-previousEnd > int64(math.MaxInt32) {
			return errBadSegment
		}
		denseEnds[i] = end
		previousEnd = end
	}
	if previousEnd != logicalEnd {
		return errBadSegment
	}
	nextPhysical := int64(segmentHeaderSize)
	for i := int64(0); i < blockCount; i++ {
		off := i * blockEntrySize
		first := int64(binary.LittleEndian.Uint64(blocks[off:]))
		physical := int64(binary.LittleEndian.Uint64(blocks[off+8:]))
		compressed := int64(binary.LittleEndian.Uint64(blocks[off+16:]))
		plain := int64(binary.LittleEndian.Uint64(blocks[off+24:]))
		if (i == 0 && first != 0) || first < 0 || first >= count || physical != nextPhysical || compressed <= 0 || plain < 0 {
			return errBadSegment
		}
		var nextFirst int64 = count
		if i+1 < blockCount {
			nextFirst = int64(binary.LittleEndian.Uint64(blocks[off+blockEntrySize:]))
		}
		if nextFirst <= first || nextFirst > count {
			return errBadSegment
		}
		logicalStart := int64(0)
		if first > 0 {
			logicalStart = denseEnds[first-1]
		}
		logicalBlockEnd := denseEnds[nextFirst-1]
		physicalEnd, ok := checkedAdd(physical, compressed)
		if !ok || physicalEnd > payloadEnd || logicalBlockEnd-logicalStart != plain {
			return errBadSegment
		}
		nextPhysical = physicalEnd
	}
	if nextPhysical != payloadEnd {
		return errBadSegment
	}
	return nil
}

type segmentView struct {
	cache                                    *fdCache
	path, indexPath, name                    string
	firstIndex, lastIndex, payloadEnd, count int64
	sealed                                   bool
	version                                  uint16
	logicalEnd, blockCount                   int64
}

func (sf *segmentFile) view(cache *fdCache) segmentView {
	return segmentView{cache: cache, path: sf.path, indexPath: sf.indexPath, name: sf.name, firstIndex: sf.firstIndex, lastIndex: sf.lastIndex, payloadEnd: sf.payloadEnd, count: sf.count, sealed: sf.sealed, version: sf.version, logicalEnd: sf.logicalEnd, blockCount: sf.blockCount}
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
		if v.version == segmentVersionV3 {
			start = 0
		}
		if ordinal > 0 {
			var prev [8]byte
			if _, err := indexFile.ReadAt(prev[:], indexStart+(ordinal-1)*denseEntrySize); err != nil {
				return err
			}
			start = int64(binary.LittleEndian.Uint64(prev[:]))
		}
		maxEnd := v.payloadEnd
		if v.version == segmentVersionV3 {
			maxEnd = v.logicalEnd
		}
		if end < start || end > maxEnd || end-start > int64(^uint32(0)>>1) {
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

type decodedBlock struct {
	ordinal       int
	plain         []byte
	blocks        []byte
	firstOrdinals []int64
	logicalStarts []int64
}

func (v segmentView) readPayloadAtCached(rec segmentRecord, off int64, cached *decodedBlock) ([]byte, error) {
	p, err := v.cache.pin(v.path, false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = p.release() }() //nolint:errcheck // read pin; nothing actionable
	if v.version == segmentVersionV3 {
		b, err := v.readCompressedPayload(p.file(), rec, off, cached)
		if err != nil {
			return nil, err
		}
		if crc32.Checksum(b, crcTable) != rec.crc {
			return nil, errBadSegment
		}
		return b, nil
	}
	b := make([]byte, rec.length)
	if _, err = p.file().ReadAt(b, off); err != nil && err != io.EOF {
		return nil, err
	}
	if crc32.Checksum(b, crcTable) != rec.crc {
		return nil, errBadSegment
	}
	return b, nil
}

func (v segmentView) readCompressedPayload(payloadFile *os.File, rec segmentRecord, logicalOff int64, cached *decodedBlock) ([]byte, error) {
	if cached == nil {
		return nil, errBadSegment
	}
	if cached.blocks == nil {
		if err := v.loadBlockMetadata(payloadFile, cached); err != nil {
			return nil, err
		}
	}
	recordOrdinal := rec.index - v.firstIndex
	block := sort.Search(len(cached.firstOrdinals), func(i int) bool { return cached.firstOrdinals[i] > recordOrdinal }) - 1
	if block < 0 {
		return nil, errBadSegment
	}
	e := cached.blocks[block*blockEntrySize:]
	physical := int64(binary.LittleEndian.Uint64(e[8:16]))
	compressed := int64(binary.LittleEndian.Uint64(e[16:24]))
	plainLen := int64(binary.LittleEndian.Uint64(e[24:32]))
	var plain []byte
	var err error
	if cached.ordinal == block {
		plain = cached.plain
	} else {
		frame, readErr := readBoundedAt(payloadFile, compressed, physical, v.payloadEnd-physical)
		plainCap, capOK := checkedInt(plainLen)
		if readErr != nil || !capOK {
			return nil, errBadSegment
		}
		memoryLimit := uint64(max(plainLen, 1<<20))
		dec, decErr := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecodeAllCapLimit(true), zstd.WithDecoderMaxMemory(memoryLimit), zstd.WithDecoderMaxWindow(memoryLimit))
		if decErr != nil {
			return nil, decErr
		}
		plain, err = dec.DecodeAll(frame, make([]byte, 0, plainCap))
		dec.Close()
		if err == nil && int64(len(plain)) == plainLen {
			cached.ordinal, cached.plain = block, plain
		}
	}
	start := logicalOff - cached.logicalStarts[block]
	end, ok := checkedAdd(start, int64(rec.length))
	if err != nil || int64(len(plain)) != plainLen || start < 0 || !ok || end > int64(len(plain)) {
		return nil, errBadSegment
	}
	return slices.Clone(plain[start:end]), nil
}

func (v segmentView) loadBlockMetadata(payloadFile *os.File, cached *decodedBlock) error {
	var indexFile *os.File = payloadFile
	indexLen, indexOK := checkedMul(v.count, denseEntrySize)
	blockLen, blockOK := checkedMul(v.blockCount, blockEntrySize)
	indexStart, startOK := checkedAdd(v.payloadEnd, indexLen)
	if !indexOK || !blockOK || !startOK {
		return errBadSegment
	}
	var pin *fdPin
	if !v.sealed {
		var err error
		pin, err = v.cache.pin(filepath.Join(filepath.Dir(v.path), segmentBlockName(v.firstIndex)), false)
		if err != nil {
			return err
		}
		defer func() { _ = pin.release() }()
		indexFile, indexStart = pin.file(), 0
	}
	blocks, err := readBoundedAt(indexFile, blockLen, indexStart, blockLen)
	if err != nil {
		return errBadSegment
	}
	logicalStarts := make([]int64, len(blocks)/blockEntrySize)
	firstOrdinals := make([]int64, len(blocks)/blockEntrySize)
	var logical int64
	for i := range logicalStarts {
		logicalStarts[i] = logical
		e := blocks[i*blockEntrySize:]
		firstOrdinals[i] = int64(binary.LittleEndian.Uint64(e[0:8]))
		physical := int64(binary.LittleEndian.Uint64(e[8:16]))
		compressed := int64(binary.LittleEndian.Uint64(e[16:24]))
		plain := int64(binary.LittleEndian.Uint64(e[24:32]))
		physicalEnd, physicalOK := checkedAdd(physical, compressed)
		var ok bool
		logical, ok = checkedAdd(logical, plain)
		if !ok || !physicalOK || plain < 0 || compressed <= 0 || physical < segmentHeaderSize || physicalEnd > v.payloadEnd || logical > v.logicalEnd {
			return errBadSegment
		}
	}
	if logical != v.logicalEnd {
		return errBadSegment
	}
	cached.blocks, cached.firstOrdinals, cached.logicalStarts = blocks, firstOrdinals, logicalStarts
	return nil
}
