package seglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// WAL byte layout (all integers little-endian).
//
// Segment header, one 4KiB block, fsync'd before the first frame:
//
//	0  u32 magic "DSWL"
//	4  u16 version
//	6  u16 flags (0)
//	8  u32 partition
//	12 u64 segmentSeq
//	20 i64 createdUnixNano
//	28 u32 reserved (0)
//	32 u32 crc32c of bytes [0,32)
//	36 .. 4096 zero padding
//
// Transaction frame, one per logical mutation:
//
//	header (48 bytes):
//	  0  u32 magic "DSTX"
//	  4  u32 hdrCRC  = crc32c(header bytes [8,48))
//	  8  u64 txnID   — per-partition, strictly monotonic
//	  16 u8  op
//	  17 u8  flags
//	  18 u16 streamIDLen
//	  20 u32 metaLen
//	  24 u32 payloadCount
//	  28 u64 firstIndex — first logical index assigned by this frame (0 if none)
//	  36 i64 tsUnixNano
//	  44 u32 totalLen   — whole frame including trailer
//	body:
//	  streamID bytes, incarnation [16]byte, meta bytes,
//	  u32 metaCRC = crc32c(streamID ‖ incarnation ‖ meta)
//	payloads × payloadCount:
//	  u32 len ‖ payload ‖ u32 crc32c(payload)
//	trailer (16 bytes):
//	  u32 magic "DSCM" ‖ u64 txnID ‖ u32 frameCRC = crc32c(frame[0 : len-4])
//
// A frame is valid iff every magic and CRC checks out, totalLen fits the
// segment, and the trailer txnID matches the header. Preallocated segments
// read as zeros past the last frame, so a zero magic marks the clean end.

const (
	walSegmentMagic  uint32 = 0x4453574C // "DSWL"
	frameMagic       uint32 = 0x44535458 // "DSTX"
	frameTrailer     uint32 = 0x4453434D // "DSCM"
	walFormatVersion uint16 = 1

	walSegmentHeaderSize = 4096
	frameHeaderSize      = 48
	frameTrailerSize     = 16
	incarnationSize      = 16

	// minFrameSize is a frame with an empty stream ID, no meta and no
	// payloads: header + incarnation + metaCRC + trailer.
	minFrameSize = frameHeaderSize + incarnationSize + 4 + frameTrailerSize

	maxStreamIDLen = 1<<16 - 1
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// opKind identifies the logical mutation a WAL frame carries.
type opKind uint8

const (
	opCreate    opKind = 1
	opAppend    opKind = 2
	opDelete    opKind = 3
	opTouch     opKind = 4
	opFork      opKind = 5
	opRetention opKind = 6
	opTrim      opKind = 7
)

// Frame flags.
const (
	flagClose          = 1 << 0 // append group ends with permanent EOF
	flagHasSeq         = 1 << 1 // meta carries a dedup sequence string
	flagClosedAtCreate = 1 << 2 // stream created already at permanent EOF
	flagSoftDelete     = 1 << 3
)

// incarnation is the raw 16-byte identity of one stream incarnation; its hex
// form is exposed as StreamInfo.IncarnationID.
type incarnation [16]byte

// frameSpec is the writer-side description of one frame.
type frameSpec struct {
	txnID      uint64
	op         opKind
	flags      uint8
	streamID   string
	inc        incarnation
	meta       []byte
	firstIndex int64
	ts         int64
	payloads   [][]byte // borrowed for the duration of encoding
}

// encodedFrameSize returns the exact encoded size of the frame.
func encodedFrameSize(streamIDLen, metaLen int, payloads [][]byte) int64 {
	size := int64(frameHeaderSize + streamIDLen + incarnationSize + metaLen + 4 + frameTrailerSize)
	for _, p := range payloads {
		size += 8 + int64(len(p))
	}
	return size
}

// appendFrame appends the encoded frame to buf and returns the new buffer
// together with each payload's offset relative to the start of buf.
func appendFrame(buf []byte, spec frameSpec) (out []byte, payloadOffs []int64) {
	frameStart := len(buf)
	total := encodedFrameSize(len(spec.streamID), len(spec.meta), spec.payloads)

	var hdr [frameHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], frameMagic)
	binary.LittleEndian.PutUint64(hdr[8:16], spec.txnID)
	hdr[16] = byte(spec.op)
	hdr[17] = spec.flags
	binary.LittleEndian.PutUint16(hdr[18:20], uint16(len(spec.streamID)))
	binary.LittleEndian.PutUint32(hdr[20:24], uint32(len(spec.meta)))
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(spec.payloads)))
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(spec.firstIndex))
	binary.LittleEndian.PutUint64(hdr[36:44], uint64(spec.ts))
	binary.LittleEndian.PutUint32(hdr[44:48], uint32(total))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(hdr[8:48], crcTable))
	buf = append(buf, hdr[:]...)

	bodyStart := len(buf)
	buf = append(buf, spec.streamID...)
	buf = append(buf, spec.inc[:]...)
	buf = append(buf, spec.meta...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf[bodyStart:], crcTable))

	if len(spec.payloads) > 0 {
		payloadOffs = make([]int64, len(spec.payloads))
	}
	for i, p := range spec.payloads {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(p)))
		payloadOffs[i] = int64(len(buf))
		buf = append(buf, p...)
		buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(p, crcTable))
	}

	buf = binary.LittleEndian.AppendUint32(buf, frameTrailer)
	buf = binary.LittleEndian.AppendUint64(buf, spec.txnID)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf[frameStart:], crcTable))
	return buf, payloadOffs
}

// payloadRef locates one payload's bytes within a WAL segment file.
type payloadRef struct {
	off    int64
	length int32
}

// walFrame is a decoded transaction frame. Payload bytes are not retained;
// payloads reference their location in the segment file.
type walFrame struct {
	txnID      uint64
	op         opKind
	flags      uint8
	streamID   string
	inc        incarnation
	meta       []byte
	firstIndex int64
	ts         int64
	payloads   []payloadRef
	start      int64 // frame's file offset
	end        int64 // file offset just after the frame
}

// Scanner outcomes distinguishing a clean end from a torn or corrupt tail.
var (
	// errFrameClean reports the zeroed tail of a preallocated segment: the
	// previous frame was the last one written.
	errFrameClean = errors.New("seglog: clean end of frames")
	// errFrameTorn reports bytes that are neither a valid frame nor zeros:
	// a write that never completed its group fdatasync (or corruption; the
	// caller decides based on segment position).
	errFrameTorn = errors.New("seglog: torn or invalid frame")
)

// frameScanner sequentially decodes frames from one WAL segment.
type frameScanner struct {
	r    io.ReaderAt
	off  int64 // next read position
	size int64 // segment file size
	buf  []byte
}

func newFrameScanner(r io.ReaderAt, size int64) *frameScanner {
	return &frameScanner{r: r, off: walSegmentHeaderSize, size: size}
}

// next decodes the frame at the current position. It returns errFrameClean at
// the zeroed tail, errFrameTorn for invalid non-zero bytes, or another error
// for I/O failures. On success the scanner advances past the frame.
func (s *frameScanner) next() (walFrame, error) {
	if s.off+minFrameSize > s.size {
		return walFrame{}, s.classifyTail()
	}
	var hdr [frameHeaderSize]byte
	if _, err := s.r.ReadAt(hdr[:], s.off); err != nil {
		return walFrame{}, fmt.Errorf("read frame header at %d: %w", s.off, err)
	}
	magic := binary.LittleEndian.Uint32(hdr[0:4])
	if magic != frameMagic {
		if magic == 0 && allZero(hdr[:]) {
			return walFrame{}, s.classifyTail()
		}
		return walFrame{}, errFrameTorn
	}
	if binary.LittleEndian.Uint32(hdr[4:8]) != crc32.Checksum(hdr[8:48], crcTable) {
		return walFrame{}, errFrameTorn
	}

	total := int64(binary.LittleEndian.Uint32(hdr[44:48]))
	idLen := int(binary.LittleEndian.Uint16(hdr[18:20]))
	metaLen := int64(binary.LittleEndian.Uint32(hdr[20:24]))
	payloadCount := int64(binary.LittleEndian.Uint32(hdr[24:28]))
	if total < minFrameSize || s.off+total > s.size {
		return walFrame{}, errFrameTorn
	}
	// The declared sections must fit the declared total even before payload
	// lengths are known: each payload occupies at least its 8 framing bytes.
	fixed := int64(frameHeaderSize+idLen+incarnationSize+4+frameTrailerSize) + metaLen
	if fixed+8*payloadCount > total {
		return walFrame{}, errFrameTorn
	}

	if int64(cap(s.buf)) < total {
		s.buf = make([]byte, total)
	}
	body := s.buf[:total]
	if _, err := s.r.ReadAt(body, s.off); err != nil {
		return walFrame{}, fmt.Errorf("read frame at %d: %w", s.off, err)
	}

	// Trailer first: it covers the whole frame, so most torn writes fail here.
	tr := body[total-frameTrailerSize:]
	txnID := binary.LittleEndian.Uint64(hdr[8:16])
	if binary.LittleEndian.Uint32(tr[0:4]) != frameTrailer ||
		binary.LittleEndian.Uint64(tr[4:12]) != txnID ||
		binary.LittleEndian.Uint32(tr[12:16]) != crc32.Checksum(body[:total-4], crcTable) {
		return walFrame{}, errFrameTorn
	}

	pos := int64(frameHeaderSize)
	streamID := string(body[pos : pos+int64(idLen)])
	pos += int64(idLen)
	var inc incarnation
	copy(inc[:], body[pos:pos+incarnationSize])
	pos += incarnationSize
	meta := make([]byte, metaLen)
	copy(meta, body[pos:pos+metaLen])
	pos += metaLen
	bodyCRC := binary.LittleEndian.Uint32(body[pos : pos+4])
	if bodyCRC != crc32.Checksum(body[frameHeaderSize:pos], crcTable) {
		return walFrame{}, errFrameTorn
	}
	pos += 4

	var payloads []payloadRef
	if payloadCount > 0 {
		payloads = make([]payloadRef, payloadCount)
	}
	for i := range payloads {
		if pos+8 > total-frameTrailerSize {
			return walFrame{}, errFrameTorn
		}
		plen := int64(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+plen+4 > total-frameTrailerSize {
			return walFrame{}, errFrameTorn
		}
		if binary.LittleEndian.Uint32(body[pos+plen:pos+plen+4]) != crc32.Checksum(body[pos:pos+plen], crcTable) {
			return walFrame{}, errFrameTorn
		}
		payloads[i] = payloadRef{off: s.off + pos, length: int32(plen)}
		pos += plen + 4
	}
	if pos != total-frameTrailerSize {
		return walFrame{}, errFrameTorn
	}

	frame := walFrame{
		txnID:      txnID,
		op:         opKind(hdr[16]),
		flags:      hdr[17],
		streamID:   streamID,
		inc:        inc,
		meta:       meta,
		firstIndex: int64(binary.LittleEndian.Uint64(hdr[28:36])),
		ts:         int64(binary.LittleEndian.Uint64(hdr[36:44])),
		payloads:   payloads,
		start:      s.off,
		end:        s.off + total,
	}
	s.off = frame.end
	return frame, nil
}

// classifyTail decides whether the remaining bytes are a clean zeroed tail or
// torn residue too short to be a frame.
func (s *frameScanner) classifyTail() error {
	remaining := s.size - s.off
	if remaining <= 0 {
		return errFrameClean
	}
	if remaining > minFrameSize {
		remaining = minFrameSize
	}
	probe := make([]byte, remaining)
	if _, err := s.r.ReadAt(probe, s.off); err != nil {
		return fmt.Errorf("read segment tail at %d: %w", s.off, err)
	}
	if allZero(probe) {
		return errFrameClean
	}
	return errFrameTorn
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// encodeWALSegmentHeader fills the 4KiB header block for a new segment.
func encodeWALSegmentHeader(partition uint32, segmentSeq uint64, createdUnixNano int64) []byte {
	hdr := make([]byte, walSegmentHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], walSegmentMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], walFormatVersion)
	binary.LittleEndian.PutUint32(hdr[8:12], partition)
	binary.LittleEndian.PutUint64(hdr[12:20], segmentSeq)
	binary.LittleEndian.PutUint64(hdr[20:28], uint64(createdUnixNano))
	binary.LittleEndian.PutUint32(hdr[32:36], crc32.Checksum(hdr[0:32], crcTable))
	return hdr
}

// walSegmentHeader is the decoded segment header.
type walSegmentHeader struct {
	partition  uint32
	segmentSeq uint64
	createdAt  int64
}

var errBadSegmentHeader = errors.New("seglog: invalid WAL segment header")

func decodeWALSegmentHeader(b []byte) (walSegmentHeader, error) {
	if len(b) < walSegmentHeaderSize {
		return walSegmentHeader{}, errBadSegmentHeader
	}
	if binary.LittleEndian.Uint32(b[0:4]) != walSegmentMagic ||
		binary.LittleEndian.Uint16(b[4:6]) != walFormatVersion ||
		binary.LittleEndian.Uint32(b[32:36]) != crc32.Checksum(b[0:32], crcTable) {
		return walSegmentHeader{}, errBadSegmentHeader
	}
	return walSegmentHeader{
		partition:  binary.LittleEndian.Uint32(b[8:12]),
		segmentSeq: binary.LittleEndian.Uint64(b[12:20]),
		createdAt:  int64(binary.LittleEndian.Uint64(b[20:28])),
	}, nil
}
