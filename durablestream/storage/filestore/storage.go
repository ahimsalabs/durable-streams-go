// Package filestore is a small file-backed Storage prototype.
//
// Each stream has one append-only data file. Records contain an offset,
// timestamp, length and checksum, followed by the payload. The process keeps
// only a compact record index in memory; payloads are read with ReadAt, so the
// operating system page cache (rather than a Go heap log) is the hot read
// tier. The implementation intentionally does not attempt forks or a
// cross-stream commit protocol yet; it exists to make the file/range-read
// design measurable before integrating it with the production handler.
package filestore

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

const (
	recordMagic  uint32 = 0x44534631 // DSF1
	recordHeader        = 4 + 8 + 8 + 4 + 4
)

// Options controls a file store. Retention is process-wide for this
// prototype; production integration should make it part of stream policy.
type Options struct {
	// Dir is the directory containing stream subdirectories. It is required.
	Dir string
	// SyncWrites calls fsync after every successful Append/AppendBatch. Set
	// false to measure the page-cache/no-fsync ceiling.
	SyncWrites bool
	// MaxBytes retains at most this many payload bytes per stream. Zero means
	// unlimited. A single message larger than MaxBytes is retained by itself.
	MaxBytes int64
	// MaxAge drops records older than this duration. Zero means unlimited.
	MaxAge time.Duration
}

// Storage is a prototype implementation of durablestream.Storage.
type Storage struct {
	mu      sync.RWMutex
	streams map[string]*stream
	opts    Options
	closed  chan struct{}
	once    sync.Once
}

type stream struct {
	mu sync.RWMutex

	id            string
	incarnationID string
	config        durablestream.StreamConfig
	file          *os.File
	records       []record
	nextIndex     int64
	lastSeq       string
	notify        chan struct{}
}

type record struct {
	index int64
	ts    time.Time
	pos   int64
	len   int
}

type diskMeta struct {
	StreamID      string                     `json:"stream_id"`
	IncarnationID string                     `json:"incarnation_id"`
	Config        durablestream.StreamConfig `json:"config"`
	NextIndex     int64                      `json:"next_index"`
	LastSeq       string                     `json:"last_seq,omitempty"`
}

var (
	_ durablestream.Storage            = (*Storage)(nil)
	_ durablestream.AtomicBatchStorage = (*Storage)(nil)
	_ durablestream.AtomicCloseStorage = (*Storage)(nil)
)

// New opens (or creates) a file store. Existing stream directories are
// scanned and their records indexed; a torn final record is truncated during
// recovery. New requires a non-empty Options.Dir.
func New(opts Options) (*Storage, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("filestore: Dir is required")
	}
	if opts.MaxBytes < 0 || opts.MaxAge < 0 {
		return nil, fmt.Errorf("filestore: retention limits cannot be negative")
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: create directory: %w", err)
	}
	f := &Storage{opts: opts, streams: make(map[string]*stream), closed: make(chan struct{})}
	entries, err := os.ReadDir(opts.Dir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		s, err := openStream(filepath.Join(opts.Dir, entry.Name()))
		if err != nil {
			f.Close()
			return nil, err
		}
		if _, exists := f.streams[s.id]; exists {
			s.close()
			f.Close()
			return nil, fmt.Errorf("filestore: duplicate stream %q", s.id)
		}
		f.streams[s.id] = s
	}
	return f, nil
}

func streamDir(root, id string) string {
	// IDs accepted by the protocol are arbitrary non-empty strings. Hex
	// encoding gives a portable, traversal-safe directory name.
	return filepath.Join(root, hex.EncodeToString([]byte(id)))
}

func newIncarnation() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func openStream(dir string) (*stream, error) {
	metaBytes, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("filestore: read %s: %w", filepath.Join(dir, "meta.json"), err)
	}
	var meta diskMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("filestore: decode metadata: %w", err)
	}
	if meta.StreamID == "" {
		return nil, fmt.Errorf("filestore: metadata has empty stream id")
	}
	file, err := os.OpenFile(filepath.Join(dir, "data.log"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &stream{id: meta.StreamID, incarnationID: meta.IncarnationID, config: meta.Config, file: file, notify: make(chan struct{}), nextIndex: meta.NextIndex, lastSeq: meta.LastSeq}
	if s.incarnationID == "" {
		s.incarnationID, err = newIncarnation()
		if err != nil {
			file.Close()
			return nil, err
		}
	}
	if err := s.scan(); err != nil {
		file.Close()
		return nil, err
	}
	if s.nextIndex <= 0 {
		s.nextIndex = 1
	}
	return s, nil
}

func (s *stream) scan() error {
	stat, err := s.file.Stat()
	if err != nil {
		return err
	}
	var pos int64
	for pos < stat.Size() {
		header := make([]byte, recordHeader)
		if _, err := s.file.ReadAt(header, pos); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return s.file.Truncate(pos)
			}
			return err
		}
		magic := binary.LittleEndian.Uint32(header[0:4])
		idx := int64(binary.LittleEndian.Uint64(header[4:12]))
		ts := int64(binary.LittleEndian.Uint64(header[12:20]))
		n := int(binary.LittleEndian.Uint32(header[20:24]))
		wantCRC := binary.LittleEndian.Uint32(header[24:28])
		if magic != recordMagic || idx <= 0 || n < 1 || int64(n) > stat.Size()-pos-int64(recordHeader) {
			return s.file.Truncate(pos)
		}
		payload := make([]byte, n)
		if _, err := s.file.ReadAt(payload, pos+recordHeader); err != nil {
			return s.file.Truncate(pos)
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return s.file.Truncate(pos)
		}
		s.records = append(s.records, record{index: idx, ts: time.Unix(0, ts), pos: pos + recordHeader, len: n})
		if idx >= s.nextIndex {
			s.nextIndex = idx + 1
		}
		pos += int64(recordHeader + n)
	}
	return nil
}

func (s *stream) meta() diskMeta {
	return diskMeta{StreamID: s.id, IncarnationID: s.incarnationID, Config: s.config, NextIndex: s.nextIndex, LastSeq: s.lastSeq}
}

func (s *stream) persistMeta() error {
	b, err := json.Marshal(s.meta())
	if err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(s.file.Name()), "meta.json.tmp")
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(filepath.Dir(s.file.Name()), "meta.json")); err != nil {
		return err
	}
	return nil
}

func (s *stream) close() error { return s.file.Close() }

func (f *Storage) get(id string) (*stream, error) {
	f.mu.RLock()
	s := f.streams[id]
	closed := isClosed(f.closed)
	f.mu.RUnlock()
	if closed {
		return nil, durablestream.ErrClosed
	}
	if s == nil {
		return nil, durablestream.ErrNotFound
	}
	return s, nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (f *Storage) expired(s *stream) bool {
	return !s.config.ExpiresAt.IsZero() && !time.Now().Before(s.config.ExpiresAt)
}

func validID(id string) error {
	if id == "" {
		return fmt.Errorf("filestore: empty stream id: %w", durablestream.ErrBadRequest)
	}
	return nil
}

func (f *Storage) Create(ctx context.Context, id string, cfg durablestream.StreamConfig) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := validID(id); err != nil {
		return false, err
	}
	if cfg.TTL < 0 {
		return false, fmt.Errorf("filestore: negative TTL: %w", durablestream.ErrBadRequest)
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	if err := f.ensureOpen(); err != nil {
		return false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if old := f.streams[id]; old != nil {
		old.mu.RLock()
		expired := f.expired(old)
		match := !expired && old.config.Matches(cfg)
		old.mu.RUnlock()
		if expired {
			delete(f.streams, id)
			_ = old.close()
			_ = os.RemoveAll(filepath.Dir(old.file.Name()))
		} else if match {
			return false, nil
		} else {
			return false, fmt.Errorf("filestore: stream exists with different config: %w", durablestream.ErrConflict)
		}
	}
	inc, err := newIncarnation()
	if err != nil {
		return false, err
	}
	dir := streamDir(f.opts.Dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, err
	}
	file, err := os.OpenFile(filepath.Join(dir, "data.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return false, err
	}
	s := &stream{id: id, incarnationID: inc, config: cfg, file: file, notify: make(chan struct{}), nextIndex: 1}
	if err := s.persistMeta(); err != nil {
		file.Close()
		return false, err
	}
	f.streams[id] = s
	return true, nil
}

func (f *Storage) ensureOpen() error {
	if isClosed(f.closed) {
		return durablestream.ErrClosed
	}
	return nil
}

func (f *Storage) appendLocked(s *stream, messages [][]byte, seq string, closeAfter bool) (durablestream.Offset, error) {
	if f.expired(s) {
		return "", durablestream.ErrNotFound
	}
	if s.config.Closed && !(closeAfter && len(messages) == 0) {
		return "", durablestream.ErrStreamClosed
	}
	if seq != "" && s.lastSeq != "" && seq <= s.lastSeq {
		return "", fmt.Errorf("filestore: sequence regression: %w", durablestream.ErrConflict)
	}
	for i, m := range messages {
		if len(m) == 0 {
			return "", fmt.Errorf("filestore: empty message %d: %w", i, durablestream.ErrBadRequest)
		}
	}
	if len(messages) == 0 && !closeAfter {
		return "", fmt.Errorf("filestore: empty append batch: %w", durablestream.ErrBadRequest)
	}
	startPos, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return "", err
	}
	buf := bytes.NewBuffer(nil)
	recs := make([]record, 0, len(messages))
	now := time.Now()
	pos := startPos
	for _, m := range messages {
		idx := s.nextIndex
		h := make([]byte, recordHeader)
		binary.LittleEndian.PutUint32(h[0:4], recordMagic)
		binary.LittleEndian.PutUint64(h[4:12], uint64(idx))
		binary.LittleEndian.PutUint64(h[12:20], uint64(now.UnixNano()))
		binary.LittleEndian.PutUint32(h[20:24], uint32(len(m)))
		binary.LittleEndian.PutUint32(h[24:28], crc32.ChecksumIEEE(m))
		buf.Write(h)
		buf.Write(m)
		recs = append(recs, record{index: idx, ts: now, pos: pos + int64(recordHeader), len: len(m)})
		pos += int64(recordHeader + len(m))
		s.nextIndex++
	}
	if _, err := s.file.Write(buf.Bytes()); err != nil {
		_ = s.file.Truncate(startPos)
		return "", err
	}
	if f.opts.SyncWrites {
		if err := s.file.Sync(); err != nil {
			_ = s.file.Truncate(startPos)
			return "", err
		}
	}
	s.records = append(s.records, recs...)
	if seq != "" {
		s.lastSeq = seq
	}
	if closeAfter {
		s.config.Closed = true
	}
	if err := f.applyRetentionLocked(s); err != nil {
		return "", err
	}
	if err := s.persistMeta(); err != nil {
		return "", err
	}
	close(s.notify)
	s.notify = make(chan struct{})
	if len(recs) == 0 {
		return storage.FormatSimpleOffset(s.nextIndex - 1), nil
	}
	return storage.FormatSimpleOffset(recs[len(recs)-1].index), nil
}

func (f *Storage) applyRetentionLocked(s *stream) error {
	if len(s.records) == 0 || (f.opts.MaxBytes == 0 && f.opts.MaxAge == 0) {
		return nil
	}
	cutoff := time.Time{}
	if f.opts.MaxAge > 0 {
		cutoff = time.Now().Add(-f.opts.MaxAge)
	}
	total := int64(0)
	for _, r := range s.records {
		total += int64(r.len)
	}
	keep := 0
	for keep < len(s.records) {
		r := s.records[keep]
		tooOld := !cutoff.IsZero() && r.ts.Before(cutoff)
		tooBig := f.opts.MaxBytes > 0 && total > f.opts.MaxBytes && total-int64(r.len) >= 0
		if !tooOld && !tooBig {
			break
		}
		if len(s.records)-keep == 1 {
			break // retain one oversized message so the stream remains readable
		}
		total -= int64(r.len)
		keep++
	}
	if keep == 0 {
		return nil
	}
	retained := append([]record(nil), s.records[keep:]...)
	return s.rewrite(retained)
}

func (s *stream) rewrite(retained []record) error {
	tmpPath := filepath.Join(filepath.Dir(s.file.Name()), "data.log.tmp")
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	newRecs := make([]record, 0, len(retained))
	var pos int64
	for _, old := range retained {
		h := make([]byte, recordHeader)
		if _, err := s.file.ReadAt(h, old.pos-int64(recordHeader)); err != nil {
			tmp.Close()
			return err
		}
		payload := make([]byte, old.len)
		if _, err := s.file.ReadAt(payload, old.pos); err != nil {
			tmp.Close()
			return err
		}
		if _, err := tmp.Write(h); err != nil {
			tmp.Close()
			return err
		}
		if _, err := tmp.Write(payload); err != nil {
			tmp.Close()
			return err
		}
		newRecs = append(newRecs, record{index: old.index, ts: old.ts, pos: pos + recordHeader, len: old.len})
		pos += int64(recordHeader + old.len)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.file.Name()); err != nil {
		return err
	}
	s.file, err = os.OpenFile(s.file.Name(), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.records = newRecs
	return nil
}

func (f *Storage) Append(ctx context.Context, id string, data []byte, seq string) (durablestream.Offset, error) {
	return f.AppendBatch(ctx, id, [][]byte{data}, seq)
}

func (f *Storage) AppendBatch(ctx context.Context, id string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := f.ensureOpen(); err != nil {
		return "", err
	}
	s, err := f.get(id)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return f.appendLocked(s, messages, seq, false)
}

func (f *Storage) CreateWithMessages(ctx context.Context, id string, cfg durablestream.StreamConfig, messages [][]byte) (bool, durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return false, "", err
	}
	if err := validID(id); err != nil {
		return false, "", err
	}
	if cfg.TTL < 0 {
		return false, "", fmt.Errorf("filestore: negative TTL: %w", durablestream.ErrBadRequest)
	}
	for i, m := range messages {
		if len(m) == 0 {
			return false, "", fmt.Errorf("filestore: empty message %d: %w", i, durablestream.ErrBadRequest)
		}
	}
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = time.Now().Add(cfg.TTL)
	}
	if err := f.ensureOpen(); err != nil {
		return false, "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if old := f.streams[id]; old != nil {
		old.mu.RLock()
		expired := f.expired(old)
		match := !expired && old.config.Matches(cfg)
		tail := storage.FormatSimpleOffset(old.nextIndex - 1)
		old.mu.RUnlock()
		if expired {
			delete(f.streams, id)
			_ = old.close()
			_ = os.RemoveAll(filepath.Dir(old.file.Name()))
		} else if match {
			return false, tail, nil
		} else {
			return false, "", fmt.Errorf("filestore: stream exists with different config: %w", durablestream.ErrConflict)
		}
	}
	inc, err := newIncarnation()
	if err != nil {
		return false, "", err
	}
	dir := streamDir(f.opts.Dir, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return false, "", err
	}
	file, err := os.OpenFile(filepath.Join(dir, "data.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return false, "", err
	}
	s := &stream{id: id, incarnationID: inc, config: cfg, file: file, notify: make(chan struct{}), nextIndex: 1}
	// Keep the new stream private until its metadata and initial batch are
	// complete. This gives CreateWithMessages its advertised all-or-nothing
	// visibility even if a write or fsync fails.
	var off durablestream.Offset
	var appendErr error
	if len(messages) != 0 {
		s.mu.Lock()
		off, appendErr = f.appendLocked(s, messages, "", false)
		s.mu.Unlock()
	}
	if appendErr != nil {
		_ = file.Close()
		_ = os.RemoveAll(dir)
		return false, "", appendErr
	}
	if len(messages) == 0 {
		if err := s.persistMeta(); err != nil {
			_ = file.Close()
			_ = os.RemoveAll(dir)
			return false, "", err
		}
		off = storage.FormatSimpleOffset(0)
	}
	f.streams[id] = s
	return true, off, nil
}

func (f *Storage) CloseStream(ctx context.Context, id string, messages [][]byte, seq string) (durablestream.Offset, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s, err := f.get(id)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.config.Closed && len(messages) != 0 {
		return "", durablestream.ErrStreamClosed
	}
	return f.appendLocked(s, messages, seq, true)
}

func (f *Storage) Read(ctx context.Context, id string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("filestore: negative limit: %w", durablestream.ErrBadRequest)
	}
	s, err := f.get(id)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if f.expired(s) {
		return nil, durablestream.ErrNotFound
	}
	_, pos, err := storage.ParseOffset(offset)
	if err != nil {
		return nil, err
	}
	if len(s.records) > 0 && pos < s.records[0].index-1 {
		return nil, fmt.Errorf("filestore: offset %s was dropped by retention: %w", offset, durablestream.ErrGone)
	}
	result := &durablestream.ReadResult{NextOffset: storage.FormatSimpleOffset(pos), TailOffset: storage.FormatSimpleOffset(s.nextIndex - 1), IncarnationID: s.incarnationID, Closed: s.config.Closed}
	for _, r := range s.records {
		if r.index <= pos {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		payload := make([]byte, r.len)
		if _, err := s.file.ReadAt(payload, r.pos); err != nil {
			return nil, err
		}
		if len(result.Messages) > 0 && limit > 0 && resultBytes(result) > 0 && resultBytes(result)+len(payload) > limit {
			break
		}
		result.Messages = append(result.Messages, durablestream.StoredMessage{Data: payload, Offset: storage.FormatSimpleOffset(r.index)})
		result.NextOffset = storage.FormatSimpleOffset(r.index)
		if limit > 0 && resultBytes(result) >= limit {
			break
		}
	}
	return result, nil
}

func resultBytes(r *durablestream.ReadResult) int {
	n := 0
	for _, m := range r.Messages {
		n += len(m.Data)
	}
	return n
}

func (f *Storage) Head(ctx context.Context, id string) (*durablestream.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s, err := f.get(id)
	if err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if f.expired(s) {
		return nil, durablestream.ErrNotFound
	}
	return &durablestream.StreamInfo{ContentType: s.config.ContentType, NextOffset: storage.FormatSimpleOffset(s.nextIndex - 1), TTL: s.config.TTL, ExpiresAt: s.config.ExpiresAt, IsPrivate: s.config.IsPrivate, Closed: s.config.Closed, IncarnationID: s.incarnationID}, nil
}

func (f *Storage) Touch(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := f.get(id)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.expired(s) {
		return durablestream.ErrNotFound
	}
	if s.config.TTL > 0 {
		s.config.ExpiresAt = time.Now().Add(s.config.TTL)
		if err := s.persistMeta(); err != nil {
			return err
		}
	}
	return nil
}

func (f *Storage) Delete(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	s := f.streams[id]
	if s == nil {
		f.mu.Unlock()
		return durablestream.ErrNotFound
	}
	delete(f.streams, id)
	f.mu.Unlock()
	s.mu.Lock()
	close(s.notify)
	_ = s.close()
	dir := filepath.Dir(s.file.Name())
	s.mu.Unlock()
	return os.RemoveAll(dir)
}

func (f *Storage) WaitForData(ctx context.Context, id string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	for {
		result, err := f.Read(ctx, id, offset, limit)
		if err != nil {
			return nil, err
		}
		if len(result.Messages) != 0 || result.Closed {
			return result, nil
		}
		s, err := f.get(id)
		if err != nil {
			return nil, err
		}
		s.mu.RLock()
		notify := s.notify
		s.mu.RUnlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-f.closed:
			return nil, durablestream.ErrClosed
		case <-notify:
		}
	}
}

// Close closes all open stream files and wakes WaitForData callers.
func (f *Storage) Close() error {
	f.once.Do(func() {
		close(f.closed)
		f.mu.Lock()
		for id, s := range f.streams {
			delete(f.streams, id)
			s.mu.Lock()
			close(s.notify)
			_ = s.close()
			s.mu.Unlock()
		}
		f.mu.Unlock()
	})
	return nil
}
