package seglog

import (
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/storagetest"
)

// conformanceMaxMessageSize keeps the suite's payload-size subtests cheap
// while still exercising the limit.
const conformanceMaxMessageSize = 8192

// conformanceStore carries the directory backing a storage so the conformance
// suite's Reopen hook can restart against the same data.
type conformanceStore struct {
	*Storage
	dir string
}

// conformanceOptions keeps the suite light: few partitions, small preallocated
// segments, and small groups so many fresh stores per run stay cheap.
func conformanceOptions(dir string) Options {
	return Options{
		Dir:             dir,
		Partitions:      4,
		MaxMessageSize:  conformanceMaxMessageSize,
		WALSegmentBytes: 4 << 20,
	}
}

func openConformanceStore(t *testing.T, dir string) durablestream.Storage {
	t.Helper()
	s, err := New(conformanceOptions(dir))
	if err != nil {
		t.Fatalf("opening storage in %s: %v", dir, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("closing storage: %v", err)
		}
	})
	return &conformanceStore{Storage: s, dir: dir}
}

// TestConformance runs the shared storage conformance suite, including the
// durability subtests via the Reopen hook. Forks are phase 4.
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Config{
		New: func(t *testing.T) durablestream.Storage {
			return openConformanceStore(t, t.TempDir())
		},
		Reopen: func(t *testing.T, s durablestream.Storage) durablestream.Storage {
			t.Helper()
			cs, ok := s.(*conformanceStore)
			if !ok {
				t.Fatalf("Reopen got a %T, want the *conformanceStore that New returned", s)
			}
			if err := cs.Close(); err != nil {
				t.Fatalf("closing before reopen: %v", err)
			}
			return openConformanceStore(t, cs.dir)
		},
		MaxMessageSize: conformanceMaxMessageSize,
		FutureOffset:   "0000000000000000_0000000000009999",
		MalformedOffsets: []durablestream.Offset{
			"0000000000000000",
			"abc_def",
			"not-an-offset",
			"-2",
			"0000000000000000_-5",
		},
		RequireIncarnationID: true,
		RequireAtomicBatches: true,
		RequireAtomicClose:   true,
		RequireForks:         true,
	})
}
