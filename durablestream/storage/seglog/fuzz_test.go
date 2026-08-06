package seglog

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func FuzzDecodeFrame(f *testing.F) {
	var inc incarnation
	copy(inc[:], "fuzz-incarnation")
	specs := []frameSpec{
		{txnID: 1, op: opCreate, streamID: "alpha", inc: inc, meta: []byte("text/plain"), firstIndex: 1, ts: 10},
		{txnID: 2, op: opAppend, flags: flagHasSeq, streamID: "alpha", inc: inc, meta: []byte("sequence-2"), firstIndex: 1, ts: 11, payloads: [][]byte{[]byte("one"), []byte("two")}},
		{txnID: 3, op: opAppend, flags: flagClose, streamID: "alpha", inc: inc, firstIndex: 3, ts: 12, payloads: [][]byte{bytes.Repeat([]byte{0xa5}, 257)}},
	}

	valid := encodeWALSegmentHeader(3, 7, 12345)
	starts := make([]int, 0, len(specs))
	ends := make([]int, 0, len(specs))
	for _, spec := range specs {
		starts = append(starts, len(valid))
		valid, _ = appendFrame(valid, spec)
		ends = append(ends, len(valid))
	}
	f.Add(valid)

	// Mirror every corruption location covered by TestFrameScannerDetectsCorruption.
	secondStart, secondEnd := starts[1], ends[1]
	for _, pos := range []int{
		secondStart,
		secondStart + 9,
		secondStart + 16,
		secondStart + frameHeaderSize,
		secondEnd - frameTrailerSize,
		secondEnd - frameTrailerSize + 5,
		secondEnd - 2,
	} {
		mutated := bytes.Clone(valid)
		mutated[pos] ^= 0xa5
		f.Add(mutated)
	}

	for _, cut := range []int{1, frameTrailerSize, secondEnd - secondStart - 1, len(valid) - walSegmentHeaderSize} {
		f.Add(bytes.Clone(valid[:len(valid)-cut]))
	}
	f.Add([]byte{})
	f.Add(make([]byte, walSegmentHeaderSize))

	// Exercise the sealed-segment footer path with a valid, empty segment too.
	segmentSeed := encodeSegmentHeader(inc, 1, 12345)
	footer := make([]byte, segmentFooterSize)
	binary.LittleEndian.PutUint32(footer[0:4], segmentIndexMagic)
	binary.LittleEndian.PutUint16(footer[4:6], segmentVersion)
	binary.LittleEndian.PutUint64(footer[8:16], segmentHeaderSize)
	binary.LittleEndian.PutUint64(footer[24:32], 1)
	binary.LittleEndian.PutUint64(footer[32:40], 0)
	binary.LittleEndian.PutUint32(footer[52:56], crc32.Checksum(footer[:52], crcTable))
	f.Add(append(segmentSeed, footer...))

	tempDir := f.TempDir()
	f.Fuzz(func(t *testing.T, data []byte) {
		// These decoders must reject short or malformed input rather than panic.
		_, _ = decodeWALSegmentHeader(data)
		_, _, _ = decodeSegmentHeader(data)

		scanner := newFrameScanner(bytes.NewReader(data), int64(len(data)))
		maxFrames := 0
		if len(data) > walSegmentHeaderSize {
			maxFrames = (len(data)-walSegmentHeaderSize)/minFrameSize + 1
		}
		accepted := 0
		for {
			frame, err := scanner.next()
			if err != nil {
				break
			}
			accepted++
			if accepted > maxFrames {
				t.Fatalf("accepted %d frames, exceeds size-derived bound %d", accepted, maxFrames)
			}
			if frame.end-frame.start < minFrameSize {
				t.Fatalf("frame advanced %d bytes, want at least %d", frame.end-frame.start, minFrameSize)
			}
			if frame.start < 0 || frame.end < frame.start || frame.end > int64(len(data)) {
				t.Fatalf("accepted frame range [%d,%d) outside input of %d bytes", frame.start, frame.end, len(data))
			}

			payloads := make([][]byte, len(frame.payloads))
			for i, ref := range frame.payloads {
				end := ref.off + int64(ref.length)
				if ref.length < 0 || ref.off < frame.start || end < ref.off || end > frame.end || end > int64(len(data)) {
					t.Fatalf("payload %d range [%d,%d) outside accepted frame [%d,%d)", i, ref.off, end, frame.start, frame.end)
				}
				payloads[i] = data[ref.off:end]
			}
			reencoded, _ := appendFrame(nil, frameSpec{
				txnID: frame.txnID, op: frame.op, flags: frame.flags, streamID: frame.streamID,
				inc: frame.inc, meta: frame.meta, firstIndex: frame.firstIndex, ts: frame.ts, payloads: payloads,
			})
			if !bytes.Equal(reencoded, data[frame.start:frame.end]) {
				t.Fatalf("accepted frame [%d,%d) did not encode identically", frame.start, frame.end)
			}
		}

		// Keep filesystem work and loader allocations bounded independently of
		// the scanner, which always receives the complete fuzz input above.
		const maxSegmentProbe = 1 << 20
		fileData := data
		if len(fileData) > maxSegmentProbe {
			fileData = fileData[:maxSegmentProbe]
		}
		probe, err := os.CreateTemp(tempDir, "segment-*.seg")
		if err != nil {
			t.Fatal(err)
		}
		path := probe.Name()
		if _, err := probe.Write(fileData); err != nil {
			_ = probe.Close()
			t.Fatal(err)
		}
		if err := probe.Close(); err != nil {
			t.Fatal(err)
		}
		_, _ = openSealedSegment(path, filepath.Base(path), inc)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	})
}
