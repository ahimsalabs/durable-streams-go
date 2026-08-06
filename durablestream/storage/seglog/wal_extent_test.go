package seglog

import (
	"os"
	"testing"
)

func TestWALAllocation_GrowsByExtentBeforeWriteAndKeepsRollCap(t *testing.T) {
	dir := t.TempDir()
	segmentBytes := 2*walExtentBytes + 4096
	w := newWALWriter(dir, 0, segmentBytes, false)
	t.Cleanup(func() { _ = w.close() })

	if _, _, err := w.appendGroup(make([]byte, walExtentBytes-walSegmentHeaderSize)); err != nil {
		t.Fatalf("fill first extent: %v", err)
	}
	assertFileSize(t, w.active, walExtentBytes)

	if _, _, err := w.appendGroup([]byte{1}); err != nil {
		t.Fatalf("cross first extent: %v", err)
	}
	assertFileSize(t, w.active, 2*walExtentBytes)

	first := w.active
	if _, _, err := w.appendGroup(make([]byte, walExtentBytes)); err != nil {
		t.Fatalf("fill second extent: %v", err)
	}
	assertFileSize(t, first, segmentBytes)
	if _, _, err := w.appendGroup(make([]byte, 4096)); err != nil {
		t.Fatalf("roll at logical cap: %v", err)
	}
	if w.activeSeq != 2 {
		t.Errorf("active sequence = %d, want 2", w.activeSeq)
	}
	assertFileSize(t, w.active, walExtentBytes)
}

func TestRezeroTail_ReextendsOnlyCurrentExtentAndClearsTornBytes(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	validEnd := walExtentBytes + 123
	if err := preallocate(f, 3*walExtentBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("torn"), validEnd); err != nil {
		t.Fatal(err)
	}
	if err := rezeroTail(f, validEnd, 4*walExtentBytes); err != nil {
		t.Fatal(err)
	}
	assertFileSize(t, f, 2*walExtentBytes)
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, validEnd); err != nil {
		t.Fatalf("read zeroed tail: %v", err)
	}
	if string(buf) != "\x00\x00\x00\x00" {
		t.Errorf("tail = %q, want zeros", buf)
	}
}

func assertFileSize(t *testing.T, f *os.File, want int64) {
	t.Helper()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != want {
		t.Errorf("file size = %d, want %d", info.Size(), want)
	}
}
