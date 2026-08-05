// Package bboltstore is a small page/mmap-backed durable-stream prototype.
//
// bbolt maps its database file into the process and serves reads directly from
// those pages (the values are copied before they leave this package).  Stream
// metadata and messages live in one nested bucket, so an append and its index
// update commit atomically.  This is intentionally a prototype rather than a
// replacement for badgerstore: bbolt has a single writer transaction and does
// not provide a value-log or background compaction.
package bboltstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"go.etcd.io/bbolt"
)

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
	_ durablestream.AtomicCloseStorage = (*Storage)(nil)
)

var (
	rootBucket = []byte("streams")
	metaKey    = []byte("_meta")
	errClosed  = fmt.Errorf("bboltstore: storage closed: %w", durablestream.ErrClosed)
)

// Retention limits the amount of history retained for one stream. A zero value
// means unlimited. The age is measured from append time, not stream creation.
// Retention is best-effort at append boundaries: an idle stream is not scanned
// by a background reaper in this prototype.
type Retention struct {
	MaxBytes int64
	MaxAge   time.Duration
}

// Options controls the prototype backend.
type Options struct {
	// NoSync skips bbolt's fdatasync after write transactions. It is useful for
	// throughput experiments but acknowledged writes can be lost on a crash.
	NoSync bool
	// DefaultRetention is copied to each newly created stream.
	DefaultRetention Retention
	// Timeout bounds the wait for another process holding bbolt's writer lock.
	// Zero uses bbolt's default (one second).
	Timeout time.Duration
}

// Storage is a bbolt-backed stream store. The zero value is not usable; use
// New to open a database.
type Storage struct {
	db       *bbolt.DB
	defaults Retention

	mu      sync.RWMutex
	closed  bool
	closedC chan struct{}
	waiters map[string]chan struct{}
}

// streamMeta is JSON to keep the prototype's on-disk format inspectable. seq
// is the next message position (the first message is position one); tail is
// the position of the last message and is equal to seq-1. earliest is the
// oldest retained message position, or seq when history is empty.
type streamMeta struct {
	Config    durablestream.StreamConfig `json:"config"`
	IncID     string                     `json:"incarnationId"`
	Seq       int64                      `json:"seq"`
	Earliest  int64                      `json:"earliest"`
	TotalSize int64                      `json:"totalBytes"`
	Retention Retention                  `json:"retention"`
	LastSeq   string                     `json:"lastSeq,omitempty"`
}

// message stores append time in the key's value. The timestamp lets retention
// be enforced without a second index. Values are timestamp nanos followed by
// the caller's bytes.
const timestampBytes = 8

func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("bboltstore: empty stream id: %w", durablestream.ErrBadRequest)
	}
	return nil
}

func newIncarnation() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	}
	return hex.EncodeToString(b[:])
}

func encodeMeta(m *streamMeta) ([]byte, error) { return json.Marshal(m) }
func decodeMeta(v []byte) (streamMeta, error) {
	var m streamMeta
	if err := json.Unmarshal(v, &m); err != nil {
		return m, fmt.Errorf("bboltstore: decode metadata: %w", err)
	}
	return m, nil
}

func seqKey(seq int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(seq))
	return key
}

func encodeMessage(now time.Time, data []byte) []byte {
	v := make([]byte, timestampBytes+len(data))
	binary.BigEndian.PutUint64(v, uint64(now.UnixNano()))
	copy(v[timestampBytes:], data)
	return v
}

func decodeMessage(v []byte) (time.Time, []byte, error) {
	if len(v) < timestampBytes {
		return time.Time{}, nil, errors.New("bboltstore: malformed message")
	}
	ts := int64(binary.BigEndian.Uint64(v[:timestampBytes]))
	return time.Unix(0, ts), bytes.Clone(v[timestampBytes:]), nil
}

// New opens (or creates) a bbolt database at path.
func New(path string, opts Options) (*Storage, error) {
	if path == "" {
		return nil, fmt.Errorf("bboltstore: empty database path: %w", durablestream.ErrBadRequest)
	}
	if opts.Timeout == 0 {
		opts.Timeout = time.Second
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{NoSync: opts.NoSync, Timeout: opts.Timeout})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: open %s: %w", path, err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error { _, err := tx.CreateBucketIfNotExists(rootBucket); return err }); err != nil {
		db.Close()
		return nil, fmt.Errorf("bboltstore: initialize: %w", err)
	}
	s := &Storage{db: db, closedC: make(chan struct{}), waiters: make(map[string]chan struct{})}
	s.defaults = opts.DefaultRetention
	return s, nil
}

func (s *Storage) checkOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.db == nil {
		return errClosed
	}
	return nil
}

func (s *Storage) wake(streamID string) {
	s.mu.Lock()
	if ch := s.waiters[streamID]; ch != nil {
		close(ch)
		s.waiters[streamID] = make(chan struct{})
	}
	s.mu.Unlock()
}

func (s *Storage) waiter(streamID string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.waiters[streamID]
	if ch == nil {
		ch = make(chan struct{})
		s.waiters[streamID] = ch
	}
	return ch
}

func readMeta(b *bbolt.Bucket) (streamMeta, error) {
	if b == nil {
		return streamMeta{}, durablestream.ErrNotFound
	}
	v := b.Get(metaKey)
	if v == nil {
		return streamMeta{}, fmt.Errorf("bboltstore: missing metadata")
	}
	return decodeMeta(v)
}

func writeMeta(b *bbolt.Bucket, m *streamMeta) error {
	v, err := encodeMeta(m)
	if err != nil {
		return err
	}
	return b.Put(metaKey, v)
}

func expired(m streamMeta, now time.Time) bool {
	return !m.Config.ExpiresAt.IsZero() && now.After(m.Config.ExpiresAt)
}

func ensureLive(root *bbolt.Bucket, id string, now time.Time) (*bbolt.Bucket, streamMeta, error) {
	b := root.Bucket([]byte(id))
	m, err := readMeta(b)
	if err != nil {
		return nil, m, err
	}
	if expired(m, now) {
		return nil, m, durablestream.ErrNotFound
	}
	return b, m, nil
}

// Create creates a stream. An expired stream is replaced in the same write
// transaction, ensuring readers never observe a hybrid incarnation.
func (s *Storage) Create(ctx context.Context, id string, cfg durablestream.StreamConfig) (bool, error) {
	if err := validateID(id); err != nil {
		return false, err
	}
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	var created bool
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(rootBucket)
		if b := root.Bucket([]byte(id)); b != nil {
			m, err := readMeta(b)
			if err != nil {
				return err
			}
			if !expired(m, time.Now()) {
				if !m.Config.Matches(cfg) {
					return fmt.Errorf("bboltstore: stream %q already exists: %w", id, durablestream.ErrConflict)
				}
				return nil
			}
			if err := root.DeleteBucket([]byte(id)); err != nil {
				return err
			}
		}
		b, err := root.CreateBucket([]byte(id))
		if err != nil {
			return err
		}
		m := streamMeta{Config: cfg, IncID: newIncarnation(), Seq: 1, Earliest: 1, Retention: s.defaults}
		if err := writeMeta(b, &m); err != nil {
			return err
		}
		created = true
		return nil
	})
	if err == nil && created {
		s.wake(id)
	}
	return created, err
}

// CreateWithMessages creates a stream and initial messages atomically.
func (s *Storage) CreateWithMessages(ctx context.Context, id string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	for _, msg := range messages {
		if len(msg) == 0 {
			return false, "", fmt.Errorf("empty message: %w", durablestream.ErrBadRequest)
		}
	}
	if err := validateID(id); err != nil {
		return false, "", err
	}
	if err := s.checkOpen(); err != nil {
		return false, "", err
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	var created bool
	var tail int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(rootBucket)
		if old := root.Bucket([]byte(id)); old != nil {
			m, err := readMeta(old)
			if err != nil {
				return err
			}
			if !expired(m, time.Now()) {
				if !m.Config.Matches(cfg) {
					return fmt.Errorf("stream exists: %w", durablestream.ErrConflict)
				}
				tail = m.Seq - 1
				return nil
			}
			if err := root.DeleteBucket([]byte(id)); err != nil {
				return err
			}
		}
		b, err := root.CreateBucket([]byte(id))
		if err != nil {
			return err
		}
		m := streamMeta{Config: cfg, IncID: newIncarnation(), Seq: 1, Earliest: 1, Retention: s.defaults}
		// A closed stream may be created with an atomic initial batch. Append
		// while the metadata is open, then publish the requested EOF state.
		closed := m.Config.Closed
		m.Config.Closed = false
		if err := appendInBucket(b, &m, messages, "", false, time.Now()); err != nil {
			return err
		}
		m.Config.Closed = closed
		if err := writeMeta(b, &m); err != nil {
			return err
		}
		tail, created = m.Seq-1, true
		return nil
	})
	if err == nil && created {
		s.wake(id)
	}
	return created, storage.FormatSimpleOffset(tail), err
}

func appendInBucket(b *bbolt.Bucket, m *streamMeta, messages [][]byte, seq string, closeStream bool, now time.Time) error {
	if m.Config.Closed && len(messages) > 0 {
		return durablestream.ErrStreamClosed
	}
	if seq != "" && m.LastSeq != "" && seq <= m.LastSeq {
		return fmt.Errorf("sequence %q does not follow %q: %w", seq, m.LastSeq, durablestream.ErrConflict)
	}
	for _, data := range messages {
		if len(data) == 0 {
			return fmt.Errorf("empty message: %w", durablestream.ErrBadRequest)
		}
		if err := b.Put(seqKey(m.Seq), encodeMessage(now, data)); err != nil {
			return err
		}
		m.TotalSize += int64(len(data))
		m.Seq++
	}
	if seq != "" {
		m.LastSeq = seq
	}
	if closeStream {
		m.Config.Closed = true
	}
	pruneBucket(b, m, now)
	return nil
}

func pruneBucket(b *bbolt.Bucket, m *streamMeta, now time.Time) {
	for m.Earliest < m.Seq {
		if m.Retention.MaxBytes <= 0 && m.Retention.MaxAge <= 0 {
			return
		}
		v := b.Get(seqKey(m.Earliest))
		if v == nil {
			m.Earliest++
			continue
		}
		ts, _, err := decodeMessage(v)
		if err != nil {
			return
		}
		tooOld := m.Retention.MaxAge > 0 && now.Sub(ts) >= m.Retention.MaxAge
		tooLarge := m.Retention.MaxBytes > 0 && m.TotalSize > m.Retention.MaxBytes
		if !tooOld && !tooLarge {
			return
		}
		_, data, _ := decodeMessage(v)
		m.TotalSize -= int64(len(data))
		if m.TotalSize < 0 {
			m.TotalSize = 0
		}
		_ = b.Delete(seqKey(m.Earliest))
		m.Earliest++
	}
}

// Append appends one message and returns its durable position.
func (s *Storage) Append(ctx context.Context, id string, data []byte, seq string) (durablestream.Offset, error) {
	return s.AppendBatch(ctx, id, [][]byte{data}, seq)
}

// AppendBatch commits all messages in one bbolt write transaction.
func (s *Storage) AppendBatch(ctx context.Context, id string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	if len(messages) == 0 {
		return "", fmt.Errorf("empty append batch: %w", durablestream.ErrBadRequest)
	}
	if err := s.checkOpen(); err != nil {
		return "", err
	}
	var tail int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		if m.Config.Closed {
			return durablestream.ErrStreamClosed
		}
		if err := appendInBucket(b, &m, messages, seq, false, time.Now()); err != nil {
			return err
		}
		if err := writeMeta(b, &m); err != nil {
			return err
		}
		tail = m.Seq - 1
		return nil
	})
	if err == nil {
		s.wake(id)
	}
	return storage.FormatSimpleOffset(tail), err
}

// CloseStream atomically appends an optional final batch and marks EOF.
func (s *Storage) CloseStream(ctx context.Context, id string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := validateID(id); err != nil {
		return "", err
	}
	if err := s.checkOpen(); err != nil {
		return "", err
	}
	var tail int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		if m.Config.Closed {
			if len(messages) > 0 {
				return durablestream.ErrStreamClosed
			}
			tail = m.Seq - 1
			return nil
		}
		if err := appendInBucket(b, &m, messages, seq, true, time.Now()); err != nil {
			return err
		}
		if err := writeMeta(b, &m); err != nil {
			return err
		}
		tail = m.Seq - 1
		return nil
	})
	if err == nil {
		s.wake(id)
	}
	return storage.FormatSimpleOffset(tail), err
}

func infoFromMeta(m streamMeta) *durablestream.StreamInfo {
	return &durablestream.StreamInfo{ContentType: m.Config.ContentType, NextOffset: storage.FormatSimpleOffset(m.Seq - 1), TTL: m.Config.TTL, ExpiresAt: m.Config.ExpiresAt, IsPrivate: m.Config.IsPrivate, Closed: m.Config.Closed, IncarnationID: m.IncID}
}

// Head returns metadata without loading messages.
func (s *Storage) Head(ctx context.Context, id string) (*durablestream.StreamInfo, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	var info *durablestream.StreamInfo
	err := s.db.View(func(tx *bbolt.Tx) error {
		_, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		info = infoFromMeta(m)
		return nil
	})
	return info, err
}

// Read returns messages strictly after offset. Values are copied out of the
// mmap before the read transaction closes.
func (s *Storage) Read(ctx context.Context, id string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := validateID(id); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("negative limit: %w", durablestream.ErrBadRequest)
	}
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	_, pos, err := storage.ParseOffset(offset)
	if err != nil {
		return nil, err
	}
	result := &durablestream.ReadResult{NextOffset: storage.FormatSimpleOffset(pos)}
	err = s.db.View(func(tx *bbolt.Tx) error {
		b, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		result.TailOffset = storage.FormatSimpleOffset(m.Seq - 1)
		result.IncarnationID = m.IncID
		result.Closed = m.Config.Closed
		// An offset strictly before the retained window is gone. An empty
		// retained window has no earliest message and therefore returns empty.
		if m.Earliest < m.Seq && pos+1 < m.Earliest {
			return durablestream.ErrGone
		}
		cur := b.Cursor()
		key, value := cur.Seek(seqKey(pos + 1))
		total := 0
		for key != nil {
			if bytes.Equal(key, metaKey) {
				key, value = cur.Next()
				continue
			}
			seq := int64(binary.BigEndian.Uint64(key))
			if seq >= m.Seq {
				break
			}
			ts, data, err := decodeMessage(value)
			if err != nil {
				return err
			}
			_ = ts
			if len(result.Messages) > 0 && limit > 0 && total+len(data) > limit {
				break
			}
			result.Messages = append(result.Messages, durablestream.StoredMessage{Data: data, Offset: storage.FormatSimpleOffset(seq)})
			total += len(data)
			result.NextOffset = storage.FormatSimpleOffset(seq)
			if limit > 0 && total >= limit {
				break
			}
			key, value = cur.Next()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	return result, nil
}

// Touch restarts a sliding TTL window, but never moves an absolute expiry.
func (s *Storage) Touch(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		updated, changed := m.Config.SlideExpiry(time.Now())
		if !changed {
			return nil
		}
		m.Config = updated
		return writeMeta(b, &m)
	})
	return err
}

// SetRetention changes limits for an existing stream. It immediately prunes
// by bytes, while age is enforced on subsequent appends.
func (s *Storage) SetRetention(ctx context.Context, id string, retention Retention) error {
	if retention.MaxBytes < 0 || retention.MaxAge < 0 {
		return fmt.Errorf("negative retention: %w", durablestream.ErrBadRequest)
	}
	if err := validateID(id); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b, m, err := ensureLive(tx.Bucket(rootBucket), id, time.Now())
		if err != nil {
			return err
		}
		m.Retention = retention
		pruneBucket(b, &m, time.Now())
		return writeMeta(b, &m)
	})
	if err == nil {
		s.wake(id)
	}
	return err
}

// Delete removes a stream and all its messages atomically.
func (s *Storage) Delete(ctx context.Context, id string) error {
	if err := validateID(id); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket(rootBucket)
		b := root.Bucket([]byte(id))
		if b == nil {
			return durablestream.ErrNotFound
		}
		return root.DeleteBucket([]byte(id))
	})
	if err == nil {
		s.wake(id)
	}
	return err
}

// WaitForData waits without polling until a write, close, delete, or storage
// shutdown wakes the caller.
func (s *Storage) WaitForData(ctx context.Context, id string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	for {
		result, err := s.Read(ctx, id, offset, limit)
		if err != nil {
			return nil, err
		}
		if len(result.Messages) > 0 || result.Closed {
			return result, nil
		}
		ch := s.waiter(id)
		// Close/read again closes the race with an append between Read and waiter.
		result, err = s.Read(ctx, id, offset, limit)
		if err != nil {
			return nil, err
		}
		if len(result.Messages) > 0 || result.Closed {
			return result, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.closedC:
			return nil, errClosed
		case <-ch:
		}
	}
}

// Close closes the mmap and wakes every waiter. It is idempotent.
func (s *Storage) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closedC)
	for id, ch := range s.waiters {
		close(ch)
		delete(s.waiters, id)
	}
	db := s.db
	s.db = nil
	s.mu.Unlock()
	if db == nil {
		return nil
	}
	return db.Close()
}
