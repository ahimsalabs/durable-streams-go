package badgerstore

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

const (
	testGenerationA generation = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testGenerationB generation = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testGenerationC generation = "cccccccccccccccccccccccccccccccc"
)

func seedRawBadger(t *testing.T, dir string, records map[string]streamRecord, extra map[string][]byte) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(&quietLogger{}).WithSyncWrites(true))
	if err != nil {
		t.Fatalf("open raw Badger database: %v", err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		for streamID, rec := range records {
			encoded, err := json.Marshal(rec)
			if err != nil {
				return err
			}
			if err := txn.Set(configKey(streamID), encoded); err != nil {
				return err
			}
		}
		for key, value := range extra {
			if err := txn.Set([]byte(key), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		t.Fatalf("seed raw Badger database: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw Badger database: %v", err)
	}
}

func assertRawKeysRemain(t *testing.T, dir string, keys ...string) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(&quietLogger{}))
	if err != nil {
		t.Fatalf("reopen raw Badger database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close raw verification database: %v", err)
		}
	}()
	if err := db.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			if _, err := txn.Get([]byte(key)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("persisted key missing after rejected open: %v", err)
	}
}

func TestNewRejectsPreForkRecordFormatWithoutBatchMetadata(t *testing.T) {
	dir := t.TempDir()
	rec := streamRecord{
		StreamConfig: durablestream.StreamConfig{ContentType: "application/json"},
		// Generation-scoped storage existed before fork lineage and atomic-batch
		// boundaries. Its omitted formatVersion decodes as zero.
		FormatVersion: 0,
		Gen:           testGenerationA,
	}
	message := string(messageKey("json", testGenerationA, storage.FormatSimpleOffset(1)))
	sequence := string(seqKey("json", testGenerationA))
	seedRawBadger(t, dir, map[string]streamRecord{"json": rec}, map[string][]byte{
		message:  []byte("1"),
		sequence: make([]byte, 8),
	})

	s, err := New(Options{
		Dir:             dir,
		Logger:          &quietLogger{},
		SLogger:         quietSLog(),
		GCInterval:      -1,
		CleanupInterval: -1,
		ReapInterval:    time.Nanosecond,
	})
	if s != nil {
		_ = s.Close()
		t.Fatal("New returned storage for a pre-fork record format")
	}
	if !errors.Is(err, ErrLegacyFormat) {
		t.Fatalf("New pre-fork record error = %v, want ErrLegacyFormat", err)
	}
	assertRawKeysRemain(t, dir, string(configKey("json")), message, sequence)
}

func TestNewRejectsMalformedForkTopologyWithoutDeletingData(t *testing.T) {
	base := func(gen generation) streamRecord {
		return streamRecord{
			StreamConfig:  durablestream.StreamConfig{ContentType: "text/plain"},
			FormatVersion: currentRecordFormatVersion,
			Gen:           gen,
		}
	}
	parent := func(streamID string, gen generation) *parentReference {
		return &parentReference{
			StreamID:  streamID,
			Gen:       gen,
			Offset:    storage.FormatSimpleOffset(0),
			OffsetSet: true,
		}
	}

	tests := []struct {
		name    string
		records func() map[string]streamRecord
		want    string
	}{
		{
			name: "cycle",
			records: func() map[string]streamRecord {
				a := base(testGenerationA)
				a.Parent = parent("b", testGenerationB)
				a.RefCount = 1
				b := base(testGenerationB)
				b.Parent = parent("a", testGenerationA)
				b.RefCount = 1
				return map[string]streamRecord{"a": a, "b": b}
			},
			want: "cycle",
		},
		{
			name: "stale parent generation",
			records: func() map[string]streamRecord {
				a := base(testGenerationA)
				a.RefCount = 1
				b := base(testGenerationB)
				b.Parent = parent("a", testGenerationC)
				return map[string]streamRecord{"a": a, "b": b}
			},
			want: "stale parent generation",
		},
		{
			name: "reference count mismatch",
			records: func() map[string]streamRecord {
				a := base(testGenerationA)
				b := base(testGenerationB)
				b.Parent = parent("a", testGenerationA)
				return map[string]streamRecord{"a": a, "b": b}
			},
			want: "reference count",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			orphan := string(messageKey("unrelated", testGenerationC, storage.FormatSimpleOffset(1)))
			records := tt.records()
			seedRawBadger(t, dir, records, map[string][]byte{orphan: []byte("preserve")})

			s, err := New(Options{
				Dir:             dir,
				Logger:          &quietLogger{},
				SLogger:         quietSLog(),
				GCInterval:      -1,
				CleanupInterval: -1,
				ReapInterval:    time.Nanosecond,
			})
			if s != nil {
				_ = s.Close()
				t.Fatal("New returned storage for malformed fork topology")
			}
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New malformed topology error = %v, want text %q", err, tt.want)
			}

			keys := []string{orphan}
			for streamID := range records {
				keys = append(keys, string(configKey(streamID)))
			}
			assertRawKeysRemain(t, dir, keys...)
		})
	}
}
