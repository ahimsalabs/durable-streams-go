// Package pebblelog is an intentionally small storage prototype.
//
// It is not a drop-in durablestream.Storage implementation yet: forks and
// protocol-level JSON handling are deliberately left to a later adapter.  The
// prototype exists to measure the useful primitive underneath such an adapter:
// one Pebble key-range per stream, ordered message records, and explicit
// retention.  Keeping this package under internal/prototypes prevents an
// accidental production dependency on an incomplete backend.
package pebblelog

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	storagefmt "github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/cockroachdb/pebble"
)

var (
	ErrClosed   = errors.New("pebblelog: closed")
	ErrNotFound = errors.New("pebblelog: stream not found")
	ErrGone     = errors.New("pebblelog: offset expired")
)

// Options controls the prototype. MaxAge and MaxBytes are enforced by Retain;
// no background goroutine is started, so callers can choose their own sweep
// cadence and account for the work in their maintenance budget.
type Options struct {
	// Sync makes each commit wait for Pebble's WAL to reach stable storage.
	// Group callers should use AppendBatch with Sync enabled to amortise fsync.
	Sync bool
	// MaxAge drops records older than now-MaxAge. Zero disables age retention.
	MaxAge time.Duration
	// MaxBytes keeps at most this many message bytes. Zero disables the byte cap.
	MaxBytes int64
}

type persistedStream struct {
	Config    durablestream.StreamConfig `json:"config"`
	Next      uint64                     `json:"next"`
	Earliest  uint64                     `json:"earliest"`
	Bytes     int64                      `json:"bytes"`
	Incarnate string                     `json:"incarnation"`
}

// streamState contains process-local locking and the durable stream metadata.
// The metadata itself is rewritten in the same Pebble batch as each mutation.
type streamState struct {
	mu     sync.RWMutex
	meta   persistedStream
	notify chan struct{}
}

// Log is a primitive append log backed by Pebble. Message offsets are simple
// monotonically increasing positions (the same textual representation used by
// memorystorage), while the physical keys use fixed-width big-endian uint64s.
type Log struct {
	db   *pebble.DB
	opts Options

	mu      sync.Mutex // protects streams and closed
	streams map[string]*streamState
	closed  bool
}

// Open opens (or creates) a Pebble log at dir.
func Open(dir string, opts Options) (*Log, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebblelog: open: %w", err)
	}
	return &Log{db: db, opts: opts, streams: make(map[string]*streamState)}, nil
}

func (l *Log) checkOpen() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return nil
}

// Close closes the Pebble database. It is idempotent.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	for _, s := range l.streams {
		s.mu.Lock()
		close(s.notify)
		s.notify = make(chan struct{})
		s.mu.Unlock()
	}
	l.mu.Unlock()
	if err := l.db.Close(); err != nil {
		return fmt.Errorf("pebblelog: close: %w", err)
	}
	return nil
}

// Create creates a stream. A repeated call for the same stream is idempotent
// only when the creation config is equal.
func (l *Log) Create(ctx context.Context, streamID string, cfg durablestream.StreamConfig) (bool, error) {
	if err := l.checkOpen(); err != nil {
		return false, err
	}
	if err := validateID(streamID); err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return false, ErrClosed
	}
	if old, ok := l.streams[streamID]; ok {
		old.mu.Lock()
		if !expired(old.meta.Config) {
			defer old.mu.Unlock()
			if old.meta.Config.Matches(cfg) {
				return false, nil
			}
			return false, fmt.Errorf("pebblelog: stream exists with different config: %w", durablestream.ErrConflict)
		}
		old.mu.Unlock()
		delete(l.streams, streamID)
	}
	key := metaKey(streamID)
	if val, closer, err := l.db.Get(key); err == nil {
		var old persistedStream
		copyVal := append([]byte(nil), val...)
		_ = closer.Close()
		if err := json.Unmarshal(copyVal, &old); err != nil {
			return false, fmt.Errorf("pebblelog: decode metadata: %w", err)
		}
		if old.Config.Matches(cfg) && !expired(old.Config) {
			l.streams[streamID] = newState(old)
			return false, nil
		}
		// An expired record is replaced atomically. This prototype eagerly
		// removes its old messages; production should do this as a generation
		// scoped background purge.
		if !expired(old.Config) {
			return false, fmt.Errorf("pebblelog: stream exists with different config: %w", durablestream.ErrConflict)
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return false, fmt.Errorf("pebblelog: read metadata: %w", err)
	}
	inc := fmt.Sprintf("%x", time.Now().UnixNano())
	meta := persistedStream{Config: cfg, Incarnate: inc}
	b := l.db.NewBatch()
	defer b.Close()
	// Replacing an expired stream must not leave its old message range behind.
	// The key prefix is generation-independent in this prototype, so deleting
	// it in the replacement batch is the generation fence.
	if err := b.DeleteRange(messagePrefix(streamID), prefixEnd(messagePrefix(streamID)), nil); err != nil {
		return false, fmt.Errorf("pebblelog: purge expired stream: %w", err)
	}
	if err := b.Set(key, encodeMeta(meta), nil); err != nil {
		return false, fmt.Errorf("pebblelog: create metadata: %w", err)
	}
	if err := b.Commit(l.writeOptions()); err != nil {
		return false, fmt.Errorf("pebblelog: create commit: %w", err)
	}
	s := newState(meta)
	l.streams[streamID] = s
	return true, nil
}

// Append appends one message and returns the offset after it. For throughput,
// callers that can collect requests should prefer AppendBatch.
func (l *Log) Append(ctx context.Context, streamID string, data []byte) (durablestream.Offset, error) {
	return l.AppendBatch(ctx, streamID, [][]byte{data})
}

// AppendBatch appends all messages atomically in one Pebble batch. A failed
// batch exposes no messages. Message boundaries are retained; callers must not
// concatenate JSON payloads here because each HTTP append is an atomic unit.
func (l *Log) AppendBatch(ctx context.Context, streamID string, messages [][]byte) (durablestream.Offset, error) {
	if err := l.checkOpen(); err != nil {
		return "", err
	}
	if err := validateID(streamID); err != nil {
		return "", err
	}
	if len(messages) == 0 {
		return "", fmt.Errorf("pebblelog: empty batch: %w", durablestream.ErrBadRequest)
	}
	for _, msg := range messages {
		if len(msg) == 0 {
			return "", fmt.Errorf("pebblelog: empty message: %w", durablestream.ErrBadRequest)
		}
	}
	s, err := l.state(streamID)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if expired(s.meta.Config) {
		return "", ErrNotFound
	}
	if s.meta.Config.Closed {
		return "", durablestream.ErrStreamClosed
	}
	b := l.db.NewBatch()
	defer b.Close()
	next := s.meta.Next
	bytes := s.meta.Bytes
	now := time.Now().UnixNano()
	for _, msg := range messages {
		next++
		bytes += int64(len(msg))
		if err := b.Set(messageKey(streamID, next), encodeMessage(now, msg), nil); err != nil {
			return "", fmt.Errorf("pebblelog: append: %w", err)
		}
	}
	s.meta.Next, s.meta.Bytes = next, bytes
	if err := b.Set(metaKey(streamID), encodeMeta(s.meta), nil); err != nil {
		return "", fmt.Errorf("pebblelog: append metadata: %w", err)
	}
	if err := b.Commit(l.writeOptions()); err != nil {
		return "", fmt.Errorf("pebblelog: append commit: %w", err)
	}
	close(s.notify)
	s.notify = make(chan struct{})
	return storagefmt.FormatSimpleOffset(int64(next)), nil
}

// Read returns messages strictly after offset. If retention removed the
// requested position, ErrGone is returned.
func (l *Log) Read(ctx context.Context, streamID string, offset durablestream.Offset, limit int) ([]durablestream.StoredMessage, durablestream.Offset, error) {
	if err := l.checkOpen(); err != nil {
		return nil, "", err
	}
	if err := validateID(streamID); err != nil {
		return nil, "", err
	}
	if limit < 0 {
		return nil, "", fmt.Errorf("pebblelog: negative limit: %w", durablestream.ErrBadRequest)
	}
	_, pos, err := storagefmt.ParseOffset(offset)
	if err != nil {
		return nil, "", err
	}
	s, err := l.state(streamID)
	if err != nil {
		return nil, "", err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if expired(s.meta.Config) {
		return nil, "", ErrNotFound
	}
	if uint64(pos) < s.meta.Earliest {
		return nil, "", ErrGone
	}
	if uint64(pos) >= s.meta.Next {
		return nil, storagefmt.FormatSimpleOffset(pos), nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	prefix := messagePrefix(streamID)
	start := messageKey(streamID, uint64(pos)+1)
	it, err := l.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, "", fmt.Errorf("pebblelog: iterator: %w", err)
	}
	defer it.Close()
	if !it.SeekGE(start) {
		if err := it.Error(); err != nil {
			return nil, "", fmt.Errorf("pebblelog: seek: %w", err)
		}
		return nil, storagefmt.FormatSimpleOffset(pos), nil
	}
	result := make([]durablestream.StoredMessage, 0, 16)
	last := uint64(pos)
	total := 0
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		rec, err := decodeMessage(it.Value())
		if err != nil {
			return nil, "", err
		}
		n := total + len(rec.data)
		if limit > 0 && total > 0 && n > limit {
			break
		}
		if limit > 0 && total == 0 && n > limit {
			// A single message is never split.
			n = len(rec.data)
		}
		result = append(result, durablestream.StoredMessage{Data: append([]byte(nil), rec.data...), Offset: storagefmt.FormatSimpleOffset(int64(last + 1))})
		total = n
		last++
		if limit > 0 && total >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, "", fmt.Errorf("pebblelog: iterate: %w", err)
	}
	return result, storagefmt.FormatSimpleOffset(int64(last)), nil
}

// Wait blocks until a read has data, the stream is closed, or ctx is done.
func (l *Log) Wait(ctx context.Context, streamID string, offset durablestream.Offset, limit int) ([]durablestream.StoredMessage, durablestream.Offset, error) {
	for {
		messages, next, err := l.Read(ctx, streamID, offset, limit)
		if err != nil || len(messages) > 0 {
			return messages, next, err
		}
		s, err := l.state(streamID)
		if err != nil {
			return nil, "", err
		}
		s.mu.RLock()
		ch := s.notify
		s.mu.RUnlock()
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-ch:
		}
	}
}

// Retain applies MaxAge and MaxBytes and returns the number of deleted
// messages. It is safe to run from a periodic maintenance goroutine.
func (l *Log) Retain(ctx context.Context, streamID string, now time.Time) (int, error) {
	if err := l.checkOpen(); err != nil {
		return 0, err
	}
	s, err := l.state(streamID)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expired(s.meta.Config) {
		return 0, ErrNotFound
	}
	if l.opts.MaxAge <= 0 && l.opts.MaxBytes <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-l.opts.MaxAge).UnixNano()
	prefix := messagePrefix(streamID)
	it, err := l.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return 0, fmt.Errorf("pebblelog: iterator: %w", err)
	}
	defer it.Close()
	type doomed struct {
		key   []byte
		pos   uint64
		bytes int
	}
	var remove []doomed
	remaining := s.meta.Bytes
	for it.First(); it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rec, err := decodeMessage(it.Value())
		if err != nil {
			return 0, err
		}
		if (l.opts.MaxAge > 0 && rec.at < cutoff) || (l.opts.MaxBytes > 0 && remaining > l.opts.MaxBytes) {
			pos := binary.BigEndian.Uint64(it.Key()[len(prefix):])
			remove = append(remove, doomed{append([]byte(nil), it.Key()...), pos, len(rec.data)})
			remaining -= int64(len(rec.data))
		}
	}
	if len(remove) == 0 {
		return 0, nil
	}
	b := l.db.NewBatch()
	defer b.Close()
	for _, d := range remove {
		if err := b.Delete(d.key, nil); err != nil {
			return 0, fmt.Errorf("pebblelog: retain delete: %w", err)
		}
	}
	s.meta.Earliest = remove[len(remove)-1].pos
	s.meta.Bytes = remaining
	if err := b.Set(metaKey(streamID), encodeMeta(s.meta), nil); err != nil {
		return 0, fmt.Errorf("pebblelog: retain metadata: %w", err)
	}
	if err := b.Commit(l.writeOptions()); err != nil {
		return 0, fmt.Errorf("pebblelog: retain commit: %w", err)
	}
	return len(remove), nil
}

// Head returns a snapshot of stream metadata.
func (l *Log) Head(ctx context.Context, streamID string) (*durablestream.StreamInfo, error) {
	if err := l.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateID(streamID); err != nil {
		return nil, err
	}
	s, err := l.state(streamID)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if expired(s.meta.Config) {
		return nil, ErrNotFound
	}
	return &durablestream.StreamInfo{ContentType: s.meta.Config.ContentType, NextOffset: storagefmt.FormatSimpleOffset(int64(s.meta.Next)), TTL: s.meta.Config.TTL, ExpiresAt: s.meta.Config.ExpiresAt, IsPrivate: s.meta.Config.IsPrivate, Closed: s.meta.Config.Closed, IncarnationID: s.meta.Incarnate}, nil
}

// Touch restarts a sliding TTL window. Absolute expiry remains unchanged.
func (l *Log) Touch(ctx context.Context, streamID string) error {
	if err := l.checkOpen(); err != nil {
		return err
	}
	s, err := l.state(streamID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if expired(s.meta.Config) {
		return ErrNotFound
	}
	if s.meta.Config.TTL == 0 {
		return nil
	}
	s.meta.Config.ExpiresAt = time.Now().Add(s.meta.Config.TTL)
	b := l.db.NewBatch()
	defer b.Close()
	if err := b.Set(metaKey(streamID), encodeMeta(s.meta), nil); err != nil {
		return err
	}
	return b.Commit(l.writeOptions())
}

func (l *Log) state(id string) (*streamState, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrClosed
	}
	if s := l.streams[id]; s != nil {
		return s, nil
	}
	val, closer, err := l.db.Get(metaKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("pebblelog: read metadata: %w", err)
	}
	copyVal := append([]byte(nil), val...)
	_ = closer.Close()
	var meta persistedStream
	if err := json.Unmarshal(copyVal, &meta); err != nil {
		return nil, fmt.Errorf("pebblelog: decode metadata: %w", err)
	}
	s := newState(meta)
	l.streams[id] = s
	return s, nil
}

func newState(meta persistedStream) *streamState {
	return &streamState{meta: meta, notify: make(chan struct{})}
}

func (l *Log) writeOptions() *pebble.WriteOptions { return &pebble.WriteOptions{Sync: l.opts.Sync} }

func validateID(id string) error {
	if id == "" {
		return fmt.Errorf("pebblelog: empty stream ID: %w", durablestream.ErrBadRequest)
	}
	return nil
}

func expired(cfg durablestream.StreamConfig) bool {
	return !cfg.ExpiresAt.IsZero() && !time.Now().Before(cfg.ExpiresAt)
}

func idPart(id string) string        { return hex.EncodeToString([]byte(id)) }
func metaKey(id string) []byte       { return []byte("m/" + idPart(id)) }
func messagePrefix(id string) []byte { return []byte("d/" + idPart(id) + "/") }
func messageKey(id string, pos uint64) []byte {
	k := append([]byte(nil), messagePrefix(id)...)
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], pos)
	return append(k, b[:]...)
}

func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func encodeMeta(m persistedStream) []byte {
	b, _ := json.Marshal(m)
	return b
}

func encodeMessage(at int64, data []byte) []byte {
	b := make([]byte, 12+len(data))
	binary.BigEndian.PutUint64(b[:8], uint64(at))
	binary.BigEndian.PutUint32(b[8:12], uint32(len(data)))
	copy(b[12:], data)
	return b
}

type decodedMessage struct {
	at   int64
	data []byte
}

func decodeMessage(b []byte) (decodedMessage, error) {
	if len(b) < 12 {
		return decodedMessage{}, fmt.Errorf("pebblelog: corrupt message record")
	}
	n := int(binary.BigEndian.Uint32(b[8:12]))
	if n < 0 || len(b) != n+12 {
		return decodedMessage{}, fmt.Errorf("pebblelog: corrupt message length")
	}
	return decodedMessage{at: int64(binary.BigEndian.Uint64(b[:8])), data: b[12:]}, nil
}
