//go:build !linux

package seglog

import "os"

// fdatasync falls back to a full fsync where fdatasync is unavailable.
func fdatasync(f *os.File) error { return f.Sync() }

// syncFilesystem reports that this platform needs the exact per-file and
// per-directory fallback in materializer.go.
func syncFilesystem(_ *os.File) (bool, error) { return false, nil }

// preallocate falls back to a sparse truncate: the size is reserved in
// metadata (zero reads are guaranteed) without allocating blocks.
func preallocate(f *os.File, size int64) error { return f.Truncate(size) }
