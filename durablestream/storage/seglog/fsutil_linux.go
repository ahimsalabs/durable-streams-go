//go:build linux

package seglog

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// fdatasync flushes file data (and the size metadata needed to read it back)
// without forcing unrelated metadata updates, which is cheaper than fsync on
// preallocated WAL segments.
func fdatasync(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if err == nil {
			return nil
		}
		if !errors.Is(err, unix.EINTR) {
			return &os.PathError{Op: "fdatasync", Path: f.Name(), Err: err}
		}
	}
}

// syncFilesystem establishes one durability barrier for every pending file
// and directory update on f's filesystem. Checkpoint cadence uses this on
// Linux to avoid issuing one device-cache flush per dirty stream file.
func syncFilesystem(f *os.File) (bool, error) {
	for {
		err := unix.Syncfs(int(f.Fd()))
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, unix.EINTR) {
			return true, &os.PathError{Op: "syncfs", Path: f.Name(), Err: err}
		}
	}
}

// preallocate reserves size bytes for f so later appends cannot fail with
// ENOSPC mid-group and fdatasync does not need to journal size changes.
// Filesystems without fallocate support fall back to a sparse truncate.
func preallocate(f *os.File, size int64) error {
	for {
		err := unix.Fallocate(int(f.Fd()), 0, 0, size)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.EOPNOTSUPP), errors.Is(err, unix.ENOSYS):
			return f.Truncate(size)
		default:
			return &os.PathError{Op: "fallocate", Path: f.Name(), Err: err}
		}
	}
}
