package seglog

import (
	"fmt"
	"os"
	"path/filepath"
)

// syncDir fsyncs a directory so entry creations, renames, and unlinks inside
// it are durable (required on Linux/ext4; harmless elsewhere).
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir for sync: %w", err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", dir, err)
	}
	return nil
}

// atomicWrite writes data to path with the full crash-safety protocol:
// temp file in the same directory, write, fsync, close, rename, fsync dir.
// A crash at any point leaves either the old file or the new file intact.
func atomicWrite(path string, data []byte, perm os.FileMode) (retErr error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if retErr != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(perm); err != nil {
		return fmt.Errorf("chmod temp file: %w", err)
	}
	n, err := tmp.Write(data)
	if err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("short write: %d of %d bytes", n, len(data))
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename temp to final: %w", err)
	}
	return syncDir(dir)
}
