//go:build !unix

package seglog

import "os"

func allocatedFileBytes(info os.FileInfo) int64 { return info.Size() }

// lockDir is a no-op on platforms without flock. Single-process fencing is
// only enforced on unix; opening one directory from two processes on other
// platforms is unsupported and undetected.
func lockDir(dir string) (release func() error, err error) {
	return func() error { return nil }, nil
}
