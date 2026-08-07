package seglog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

func TestCompressionZstd_ReadsActiveSealedAndReopenedSegments(t *testing.T) {
	dir := t.TempDir()
	opts := singlePartitionOptions(dir)
	opts.Compression = CompressionZstd
	opts.CompressionBlockBytes = 1024
	opts.DefaultSegmentPolicy = SegmentPolicy{TargetBytes: 1 << 20}
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "compressed", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	payloads := [][]byte{bytes.Repeat([]byte("a"), 700), bytes.Repeat([]byte("b"), 700), bytes.Repeat([]byte("c"), 2048), bytes.Repeat([]byte("d"), 200)}
	for _, payload := range payloads {
		if _, err := s.Append(t.Context(), "compressed", payload, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.materializeRoundResult(s.parts[0]); err != nil {
		t.Fatal(err)
	}
	assertCompressedMessages(t, s, payloads)
	state, _ := s.streams.Load("compressed")
	state.mu.Lock()
	state.forceSeal = true
	state.mu.Unlock()
	s.parts[0].markDirty(state)
	if err := s.materializeRoundResult(s.parts[0]); err != nil {
		t.Fatal(err)
	}
	assertCompressedMessages(t, s, payloads)
	spans, err := s.ReadSpans(t.Context(), "compressed", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer closeSpans(spans.Spans)
	if len(spans.Spans) == 0 {
		t.Fatal("no spans")
	}
	if _, ok := spans.Spans[0].(*ownedReadSpan); !ok {
		t.Fatalf("span type = %T, want owned", spans.Spans[0])
	}
	sf := state.snapshot().sealed[0]
	info, err := os.Stat(sf.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= int64(segmentHeaderSize)+sf.logicalEnd {
		t.Fatalf("compressed file size %d did not reduce %d bytes", info.Size(), sf.logicalEnd)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r := openTest(t, opts)
	assertCompressedMessages(t, r, payloads)
}

func assertCompressedMessages(t *testing.T, s *Storage, want [][]byte) {
	t.Helper()
	res, err := s.Read(t.Context(), "compressed", "", 1400)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("budgeted messages = %d, want 2", len(res.Messages))
	}
	res, err = s.Read(t.Context(), "compressed", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Messages) != len(want) {
		t.Fatalf("messages = %d, want %d", len(res.Messages), len(want))
	}
	for i := range want {
		if !bytes.Equal(res.Messages[i].Data, want[i]) {
			t.Errorf("message %d mismatch", i)
		}
	}
}

func TestCompressionOptions_RejectInvalidValues(t *testing.T) {
	tests := []Options{
		{Compression: Compression(99)},
		{CompressionBlockBytes: 1},
		{CompressionMaxBlockAge: time.Second},
		{Compression: CompressionZstd, CompressionBlockBytes: -1},
		{Compression: CompressionZstd, CompressionMaxBlockAge: -1},
	}
	for _, opts := range tests {
		opts = opts.withDefaults()
		if err := opts.validate(); err == nil {
			t.Errorf("options %+v accepted", opts)
		}
	}
	got := (Options{Compression: CompressionZstd}).withDefaults().CompressionBlockBytes
	if got != DefaultCompressionBlockBytes {
		t.Errorf("default block bytes = %d", got)
	}
	age := (Options{Compression: CompressionZstd}).withDefaults().CompressionMaxBlockAge
	if age != DefaultCompressionMaxBlockAge {
		t.Errorf("default block age = %v", age)
	}
	if _, err := New(Options{Compression: Compression(99)}); err == nil {
		t.Error("New accepted invalid compression")
	}
}

func TestCompressionZstd_AccumulatesUntilCompressionAge(t *testing.T) {
	opts := singlePartitionOptions(t.TempDir())
	opts.Compression = CompressionZstd
	opts.CompressionBlockBytes = DefaultCompressionBlockBytes
	opts.CompressionMaxBlockAge = 100 * time.Millisecond
	opts.MaterializeBytes = 1 << 20
	opts.MaterializeMaxAge = time.Millisecond
	s := openTest(t, opts)
	if _, err := s.Create(t.Context(), "accumulate", durablestream.StreamConfig{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), "accumulate", bytes.Repeat([]byte("x"), 1024), ""); err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	if got := materializedThrough(s, "accumulate"); got != 0 {
		t.Fatalf("materialized through = %d before compression age, want 0", got)
	}
	waitFor(t, "compression block age", func() bool { return materializedThrough(s, "accumulate") == 1 })
}

func TestValidateV3Metadata_RejectsCorruptGeometryWithoutPanic(t *testing.T) {
	validIndex := make([]byte, 2*denseEntrySize)
	binary.LittleEndian.PutUint64(validIndex[0:8], 3)
	binary.LittleEndian.PutUint64(validIndex[denseEntrySize:denseEntrySize+8], 7)
	validBlocks := make([]byte, 2*blockEntrySize)
	putBlock := func(dst []byte, first, physical, compressed, plain uint64) {
		binary.LittleEndian.PutUint64(dst[0:8], first)
		binary.LittleEndian.PutUint64(dst[8:16], physical)
		binary.LittleEndian.PutUint64(dst[16:24], compressed)
		binary.LittleEndian.PutUint64(dst[24:32], plain)
	}
	putBlock(validBlocks, 0, segmentHeaderSize, 5, 3)
	putBlock(validBlocks[blockEntrySize:], 1, segmentHeaderSize+5, 6, 4)

	tests := []struct {
		name       string
		mutate     func(index, blocks []byte)
		count      int64
		blockCount int64
		payloadEnd int64
		logicalEnd int64
	}{
		{name: "first block ordinal is not zero", mutate: func(_ []byte, blocks []byte) { binary.LittleEndian.PutUint64(blocks[:8], 1) }, count: 2, blockCount: 2, payloadEnd: segmentHeaderSize + 11, logicalEnd: 7},
		{name: "physical blocks have a gap", mutate: func(_ []byte, blocks []byte) {
			binary.LittleEndian.PutUint64(blocks[blockEntrySize+8:], segmentHeaderSize+6)
		}, count: 2, blockCount: 2, payloadEnd: segmentHeaderSize + 11, logicalEnd: 7},
		{name: "block ends inside record", mutate: func(_ []byte, blocks []byte) { binary.LittleEndian.PutUint64(blocks[24:32], 2) }, count: 2, blockCount: 2, payloadEnd: segmentHeaderSize + 11, logicalEnd: 7},
		{name: "dense logical ends are not monotonic", mutate: func(index, _ []byte) { binary.LittleEndian.PutUint64(index[denseEntrySize:], 2) }, count: 2, blockCount: 2, payloadEnd: segmentHeaderSize + 11, logicalEnd: 7},
		{name: "metadata multiplication overflows", count: math.MaxInt64, blockCount: 2, payloadEnd: segmentHeaderSize + 11, logicalEnd: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, blocks := bytes.Clone(validIndex), bytes.Clone(validBlocks)
			if tt.mutate != nil {
				tt.mutate(index, blocks)
			}
			if err := validateV3Metadata(index, blocks, tt.count, tt.blockCount, tt.payloadEnd, tt.logicalEnd); !errors.Is(err, errBadSegment) {
				t.Errorf("error = %v, want %v", err, errBadSegment)
			}
		})
	}
}
