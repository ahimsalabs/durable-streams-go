//go:build unix

package seglog

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// lockDir takes an exclusive advisory lock on dir's lock file, fencing the
// storage against a second process opening the same directory. The returned
// release func closes the descriptor (dropping the lock); the file itself is
// left in place because its presence carries no meaning.
func lockDir(dir string) (release func() error, err error) {
	path := dir + "/seglog.lock"
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("lock %s (already open in another process?): %w", path, err)
	}
	return f.Close, nil
}
