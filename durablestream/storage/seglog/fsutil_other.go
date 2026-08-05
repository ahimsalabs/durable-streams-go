//go:build !linux

package seglog

import "os"

// fdatasync falls back to a full fsync where fdatasync is unavailable.
func fdatasync(f *os.File) error { return f.Sync() }

// preallocate falls back to a sparse truncate: the size is reserved in
// metadata (zero reads are guaranteed) without allocating blocks.
func preallocate(f *os.File, size int64) error { return f.Truncate(size) }
