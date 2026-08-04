package badgerstore

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/dgraph-io/badger/v4"
)

// checkNoRawConflict fails the test if err is Badger's transaction conflict
// leaking through the storage API. Snapshot-isolation aborts are an internal
// concern: callers only ever see the sentinels the Storage contract documents.
func checkNoRawConflict(t *testing.T, op string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if errors.Is(err, badger.ErrConflict) || strings.Contains(err.Error(), "Transaction Conflict") {
		t.Errorf("%s leaked a Badger transaction conflict: %v", op, err)
	}
}

// TestConcurrentCreateIsIdempotent checks the contract for racing creates of
// one stream ID with an identical config: exactly one reports creation and
// nobody sees an error. Without an internal retry, Badger aborts all but one
// transaction and the losers get a raw conflict.
func TestConcurrentCreateIsIdempotent(t *testing.T) {
	const rounds = 20

	for range rounds {
		s := newTestStorage(t)
		const creators = 8

		created := make([]bool, creators)
		errs := make([]error, creators)
		var wg sync.WaitGroup
		for i := range creators {
			wg.Go(func() {
				created[i], errs[i] = s.Create(context.Background(), "contended", durablestream.StreamConfig{
					ContentType: "text/plain",
				})
			})
		}
		wg.Wait()

		winners := 0
		for i := range creators {
			checkNoRawConflict(t, "Create", errs[i])
			if errs[i] != nil {
				t.Errorf("creator %d: %v; concurrent identical creates must all succeed", i, errs[i])
			}
			if created[i] {
				winners++
			}
		}
		if winners != 1 {
			t.Fatalf("%d of %d concurrent creates reported created=true, want exactly 1", winners, creators)
		}

		// The survivor must be a working stream.
		mustAppend(t, s, "contended", "data")
		if got := readAll(t, s, "contended"); len(got) != 1 || got[0] != "data" {
			t.Fatalf("contended stream contents = %v, want [data]", got)
		}
	}
}

// TestConcurrentCreateDeleteNeverLeakConflict races Create against Delete on
// one stream ID. Both write the config key, so both can lose a Badger
// conflict; whichever order they settle in, callers must see only the
// contract's sentinels.
func TestConcurrentCreateDeleteNeverLeakConflict(t *testing.T) {
	const (
		rounds  = 20
		workers = 8
	)

	for range rounds {
		s := newTestStorage(t)
		ctx := context.Background()
		mustCreate(t, s, "contended")

		var wg sync.WaitGroup
		for i := range workers {
			wg.Go(func() {
				if i%2 == 0 {
					_, err := s.Create(ctx, "contended", durablestream.StreamConfig{ContentType: "text/plain"})
					checkNoRawConflict(t, "Create", err)
					if err != nil {
						t.Errorf("Create racing Delete = %v, want success", err)
					}
					return
				}
				err := s.Delete(ctx, "contended")
				checkNoRawConflict(t, "Delete", err)
				if err != nil && !errors.Is(err, durablestream.ErrNotFound) {
					t.Errorf("Delete racing Create = %v, want nil or ErrNotFound", err)
				}
			})
		}
		wg.Wait()

		// Whatever order they settled in, the stream is either absent or
		// usable — never a half-deleted record.
		if _, err := s.Head(ctx, "contended"); err != nil {
			if !errors.Is(err, durablestream.ErrNotFound) {
				t.Fatalf("Head after the race = %v, want nil or ErrNotFound", err)
			}
			continue
		}
		mustAppend(t, s, "contended", "data")
		if got := readAll(t, s, "contended"); len(got) != 1 || got[0] != "data" {
			t.Fatalf("surviving stream contents = %v, want [data]", got)
		}
	}
}
