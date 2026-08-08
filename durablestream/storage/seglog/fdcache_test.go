package seglog

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFDCache_ConcurrentPinsOfColdPathShareDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := newFDCache(1)
	t.Cleanup(func() { _ = cache.close() })

	const workers = 32
	pins := make([]*fdPin, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			<-start
			pins[i], errs[i] = cache.pin(path)
		})
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("pin %d: %v", i, err)
		}
		if pins[i].file() != pins[0].file() {
			t.Errorf("pin %d used a different descriptor", i)
		}
	}
	for i, pin := range pins {
		if err := pin.release(); err != nil {
			t.Errorf("release pin %d: %v", i, err)
		}
	}
}

func TestFDCache_EvictionDoesNotClosePinnedEntry(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first")
	secondPath := filepath.Join(dir, "second")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cache := newFDCache(1)
	t.Cleanup(func() { _ = cache.close() })

	first, err := cache.pin(firstPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.pin(secondPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.release(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.file().Stat(); err != nil {
		t.Errorf("pinned descriptor was closed during eviction: %v", err)
	}
	if err := first.release(); err != nil {
		t.Fatal(err)
	}
}

func TestFDCache_PinReleaseAfterCloseSucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cache := newFDCache(1)
	pin, err := cache.pin(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.close(); err != nil {
		t.Fatal(err)
	}
	if err := pin.release(); err != nil {
		t.Errorf("release after cache close: %v", err)
	}
}

func TestFDCache_ConcurrentChurnBeyondCapacity(t *testing.T) {
	dir := t.TempDir()
	const pathCount = 16
	paths := make([]string, pathCount)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("segment-%02d", i))
		if err := os.WriteFile(paths[i], []byte{byte(i)}, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	cache := newFDCache(4)
	t.Cleanup(func() { _ = cache.close() })

	const workers = 16
	const rounds = 50
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			<-start
			for round := range rounds {
				pathIndex := (worker + round) % len(paths)
				pin, err := cache.pin(paths[pathIndex])
				if err != nil {
					errs <- fmt.Errorf("pin %s: %w", paths[pathIndex], err)
					return
				}
				var buf [1]byte
				if _, err := pin.file().ReadAt(buf[:], 0); err != nil {
					_ = pin.release()
					errs <- fmt.Errorf("read %s: %w", paths[pathIndex], err)
					return
				}
				if buf[0] != byte(pathIndex) {
					_ = pin.release()
					errs <- fmt.Errorf("read %s: got %d, want %d", paths[pathIndex], buf[0], pathIndex)
					return
				}
				if err := pin.release(); err != nil {
					errs <- fmt.Errorf("release %s: %w", paths[pathIndex], err)
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
