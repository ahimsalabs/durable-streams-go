package seglog

import (
	"bytes"
	"errors"
	"testing"
)

func testFrameSpecs() []frameSpec {
	return []frameSpec{
		{
			txnID:      1,
			op:         opCreate,
			flags:      flagClosedAtCreate,
			streamID:   "stream-a",
			inc:        incarnation{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
			meta:       []byte(`{"contentType":"text/plain"}`),
			firstIndex: 1,
			ts:         1234567890,
			payloads:   [][]byte{[]byte("hello"), []byte("world!")},
		},
		{
			txnID:    2,
			op:       opDelete,
			streamID: "s",
			inc:      incarnation{0xff},
			ts:       1234567891,
		},
		{
			txnID:      3,
			op:         opAppend,
			flags:      flagHasSeq | flagClose,
			streamID:   "stream-a",
			meta:       []byte("seq-0042"),
			firstIndex: 7,
			ts:         1234567892,
			payloads:   [][]byte{bytes.Repeat([]byte("x"), 4096)},
		},
	}
}

// encodeSegment builds an in-memory segment: header plus the given frames,
// zero-padded to size.
func encodeSegment(t *testing.T, size int64, specs ...frameSpec) []byte {
	t.Helper()
	buf := make([]byte, walSegmentHeaderSize, size)
	copy(buf, encodeWALSegmentHeader(0, 1, 42))
	for _, spec := range specs {
		buf, _ = appendFrame(buf, spec)
	}
	if int64(len(buf)) > size {
		t.Fatalf("frames of %d bytes overflow segment size %d", len(buf), size)
	}
	return append(buf, make([]byte, size-int64(len(buf)))...)
}

func TestFrameRoundTrip(t *testing.T) {
	specs := testFrameSpecs()
	seg := encodeSegment(t, 64<<10, specs...)
	scanner := newFrameScanner(bytes.NewReader(seg), int64(len(seg)))

	for i, spec := range specs {
		frame, err := scanner.next()
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if frame.txnID != spec.txnID || frame.op != spec.op || frame.flags != spec.flags ||
			frame.streamID != spec.streamID || frame.inc != spec.inc ||
			!bytes.Equal(frame.meta, spec.meta) ||
			frame.firstIndex != spec.firstIndex || frame.ts != spec.ts {
			t.Fatalf("frame %d mismatch: got %+v, want %+v", i, frame, spec)
		}
		if len(frame.payloads) != len(spec.payloads) {
			t.Fatalf("frame %d: got %d payloads, want %d", i, len(frame.payloads), len(spec.payloads))
		}
		for j, ref := range frame.payloads {
			got := seg[ref.off : ref.off+int64(ref.length)]
			if !bytes.Equal(got, spec.payloads[j]) {
				t.Fatalf("frame %d payload %d: got %q, want %q", i, j, got, spec.payloads[j])
			}
		}
	}
	if _, err := scanner.next(); !errors.Is(err, errFrameClean) {
		t.Fatalf("after last frame: got %v, want errFrameClean", err)
	}
}

func TestFrameScannerDetectsCorruption(t *testing.T) {
	specs := testFrameSpecs()
	pristine := encodeSegment(t, 64<<10, specs...)

	// Find the second frame's start so mutations target it precisely.
	scanner := newFrameScanner(bytes.NewReader(pristine), int64(len(pristine)))
	first, err := scanner.next()
	if err != nil {
		t.Fatal(err)
	}
	second, err := scanner.next()
	if err != nil {
		t.Fatal(err)
	}

	mutations := map[string]int64{
		"header magic":  second.start,
		"header crc":    second.start + 9, // inside txnID, covered by hdrCRC
		"op byte":       second.start + 16,
		"body byte":     second.start + frameHeaderSize,
		"trailer magic": second.end - frameTrailerSize,
		"trailer txnid": second.end - frameTrailerSize + 5,
		"frame crc":     second.end - 2,
	}
	for name, pos := range mutations {
		t.Run(name, func(t *testing.T) {
			seg := bytes.Clone(pristine)
			seg[pos] ^= 0xa5
			s := newFrameScanner(bytes.NewReader(seg), int64(len(seg)))
			got, err := s.next()
			if err != nil {
				t.Fatalf("first frame should stay valid, got %v", err)
			}
			if got.txnID != first.txnID {
				t.Fatalf("first frame txnID = %d, want %d", got.txnID, first.txnID)
			}
			if _, err := s.next(); !errors.Is(err, errFrameTorn) {
				t.Fatalf("mutated frame: got %v, want errFrameTorn", err)
			}
		})
	}
}

func TestFrameScannerShortTail(t *testing.T) {
	specs := testFrameSpecs()
	full := encodeSegment(t, 64<<10, specs...)
	scanner := newFrameScanner(bytes.NewReader(full), int64(len(full)))
	if _, err := scanner.next(); err != nil {
		t.Fatal(err)
	}
	second, err := scanner.next()
	if err != nil {
		t.Fatal(err)
	}

	// Truncating anywhere inside the final frame (simulated by zeroing its
	// suffix, as a preallocated file would read) must yield errFrameTorn
	// while keeping every earlier frame.
	for _, cut := range []int64{1, frameTrailerSize, second.end - second.start - 1} {
		seg := bytes.Clone(full)
		for i := second.end - cut; i < second.end; i++ {
			seg[i] = 0
		}
		s := newFrameScanner(bytes.NewReader(seg), int64(len(seg)))
		if _, err := s.next(); err != nil {
			t.Fatalf("cut %d: first frame: %v", cut, err)
		}
		if _, err := s.next(); !errors.Is(err, errFrameTorn) {
			t.Fatalf("cut %d: got %v, want errFrameTorn", cut, err)
		}
	}
}

func TestSegmentHeaderRoundTrip(t *testing.T) {
	hdr := encodeWALSegmentHeader(7, 42, 987654321)
	decoded, err := decodeWALSegmentHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.partition != 7 || decoded.segmentSeq != 42 || decoded.createdAt != 987654321 {
		t.Fatalf("decoded %+v", decoded)
	}

	corrupt := bytes.Clone(hdr)
	corrupt[13] ^= 1
	if _, err := decodeWALSegmentHeader(corrupt); !errors.Is(err, errBadSegmentHeader) {
		t.Fatalf("corrupt header: got %v, want errBadSegmentHeader", err)
	}
}
