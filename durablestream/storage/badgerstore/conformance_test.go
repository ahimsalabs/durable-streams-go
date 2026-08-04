package badgerstore

import (
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage/storagetest"
)

// conformanceMaxMessageSize keeps the suite's payload-size subtests cheap while
// still exercising the limit.
const conformanceMaxMessageSize = 8192

// conformanceStore carries the directory backing a storage so the conformance
// suite's Reopen hook can restart against the same data.
type conformanceStore struct {
	*Storage
	dir string
}

// openConformanceStore opens disk-backed storage in dir. Disk mode is what
// makes the durability subtests meaningful: a reopen has to recover from files.
func openConformanceStore(t *testing.T, dir string) durablestream.Storage {
	t.Helper()
	s, err := New(Options{
		Dir:             dir,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		MaxMessageSize:  conformanceMaxMessageSize,
		GCInterval:      -1, // Value log GC is irrelevant to the contract.
		CleanupInterval: -1, // Expiry is enforced on read; the sweeper only reclaims space.
	})
	if err != nil {
		t.Fatalf("opening storage in %s: %v", dir, err)
	}
	// Let the startup sweep finish so a reopened store is not still reclaiming
	// the previous incarnation's data underneath the subtest.
	<-s.initialReapDone
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("closing storage: %v", err)
		}
	})
	return &conformanceStore{Storage: s, dir: dir}
}

// TestConformance runs the shared storage conformance suite, including the
// durability subtests.
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
	})
}
