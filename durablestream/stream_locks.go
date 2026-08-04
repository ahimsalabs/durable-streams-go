package durablestream

import (
	"context"
	"strings"
	"sync"
)

// keyedMutex serializes mutations of one stream without retaining a lock for
// every stream ID ever observed. Entries live only while a holder or waiter
// references them.
//
// The Handler needs this lifecycle boundary because Storage mutations are
// addressed by stream ID: without it, a POST can validate the old incarnation,
// race with DELETE+PUT, and append into the replacement. The same race can let
// PUT/DELETE forget producer state that a POST just committed for the new
// incarnation.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyedMutexEntry
}

type keyedMutexEntry struct {
	mu      sync.Mutex
	refs    int
	changed chan struct{}
}

// keyedMutexToken pins one stream's lifecycle entry after its mutation lock is
// released. A successful delete or new-incarnation create closes changed,
// allowing a live read to discard any result it may have obtained from a
// replacement stream. Tokens must be released exactly once.
type keyedMutexToken struct {
	owner   *keyedMutex
	key     string
	entry   *keyedMutexEntry
	changed <-chan struct{}
}

// lock acquires key and returns a non-idempotent unlock function. Callers must
// invoke the function exactly once, normally with defer.
func (m *keyedMutex) lock(key string) func() {
	m.mu.Lock()
	if m.locks == nil {
		m.locks = make(map[string]*keyedMutexEntry)
	}
	entry := m.locks[key]
	if entry == nil {
		entry = &keyedMutexEntry{changed: make(chan struct{})}
		// Request paths may be substrings of a larger parse buffer. Retain only
		// the actual stream ID while an operation is active.
		m.locks[strings.Clone(key)] = entry
	}
	entry.refs++
	m.mu.Unlock()

	entry.mu.Lock()
	return func() {
		// Release the per-key lock before dropping our reference. A new acquirer
		// either increments refs on this entry first, or observes refs==0 only
		// after it has become safe to create a replacement entry.
		entry.mu.Unlock()

		m.release(key, entry)
	}
}

// pin returns a token for the current stream incarnation. The caller must hold
// key's mutation lock, which guarantees that the map entry cannot be replaced
// between the storage snapshot and this pin.
func (m *keyedMutex) pin(key string) *keyedMutexToken {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.locks[key]
	if entry == nil {
		panic("durablestream: pin called without holding stream mutation lock")
	}
	entry.refs++
	return &keyedMutexToken{owner: m, key: key, entry: entry, changed: entry.changed}
}

// bump invalidates tokens for the previous stream incarnation. The caller must
// hold key's mutation lock and call bump only after a Create(new) or Delete has
// committed successfully.
func (m *keyedMutex) bump(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := m.locks[key]
	if entry == nil {
		panic("durablestream: bump called without holding stream mutation lock")
	}
	close(entry.changed)
	entry.changed = make(chan struct{})
}

func (m *keyedMutex) release(key string, entry *keyedMutexEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry.refs--
	if entry.refs == 0 {
		delete(m.locks, key)
	}
}

// invalidated reports whether a successful lifecycle mutation superseded the
// incarnation captured by this token.
func (t *keyedMutexToken) invalidated() bool {
	select {
	case <-t.changed:
		return true
	default:
		return false
	}
}

// context returns a child context canceled when either parent finishes or the
// token's stream incarnation is superseded. The caller must invoke cancel.
func (t *keyedMutexToken) context(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-t.changed:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (t *keyedMutexToken) release() {
	t.owner.release(t.key, t.entry)
}
