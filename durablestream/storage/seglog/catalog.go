package seglog

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"os"
	"slices"
	"sync"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// newIncarnation returns a fresh random incarnation identity.
func newIncarnation() (incarnation, error) {
	var inc incarnation
	if _, err := rand.Read(inc[:]); err != nil {
		return incarnation{}, fmt.Errorf("seglog: generate incarnation: %w", err)
	}
	return inc, nil
}

func (i incarnation) String() string { return hex.EncodeToString(i[:]) }

// streamHash is the stable hash used for partition routing and stream
// directory sharding. It must never change (invariant I4): a stream's frames
// live in one partition's WAL for the lifetime of the directory.
func streamHash(streamID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(streamID)) // never fails
	return h.Sum64()
}

// walLoc locates one committed message inside a partition's WAL.
type walLoc struct {
	segmentSeq uint64
	off        int64 // file offset of the payload bytes
	length     int32
	batchFirst int64 // first logical index of the atomic append that wrote it
	ts         int64 // commit wall clock, unix nanos
}

// streamState is the in-memory state of one stream incarnation.
//
// Ownership: two goroutines mutate disjoint field groups, both always under
// mu, so each may read its own fields lock-free while readers take mu.RLock
// for consistent snapshots (invariant I5). The partition worker owns the
// logical state: cfg, retention, floor, closed, deleted, lastSeq, nextIndex, and walTail
// appends. The partition's materializer owns the materialized state: sealed,
// activeView, materializedThrough, and walTail pruning (firstLive). A new
// incarnation of the same stream ID is a new *streamState; waiters pin the
// pointer they started with, so a delete+recreate can never feed them the
// successor's data.
type streamState struct {
	mu sync.RWMutex

	id        string
	inc       incarnation
	partition uint32

	cfg       durablestream.StreamConfig
	retention Retention
	floor     int64 // highest trimmed index; messages at or below it are gone
	closed    bool  // permanent EOF (protocol closure, not Storage.Close)
	deleted   bool  // this incarnation was deleted or displaced

	lastSeq   string
	nextIndex int64 // next logical index to assign; the first message gets 1

	// walTail holds the location of every message not yet materialized.
	// walTail[i] is the message at logical index firstLive+i, and firstLive
	// is always materializedThrough+1.
	firstLive int64
	walTail   []walLoc

	// Materialized state: indices <= materializedThrough are served from
	// sealed segments (immutable) and the published activeView (a stable
	// prefix of the active segment). activeSeg is the materializer-private
	// mutable handle behind activeView and is never touched under mu.
	sealed              []*segmentFile
	activeView          segmentView
	materializedThrough int64
	activeSeg           *segmentFile

	// notifyCh releases WaitForData callers. It is closed and replaced on
	// every wake, and closed permanently when the incarnation dies.
	notifyCh chan struct{}
}

// segmentView is a read-only view of a segment prefix that is safe to read
// concurrently with the materializer appending behind it: records below end
// are never rewritten.
type segmentView struct {
	f          *os.File
	name       string
	firstIndex int64
	lastIndex  int64
	end        int64
	sparse     []sparseEntry
}

// view publishes the segment's current committed prefix. The sparse slice is
// cloned because the materializer keeps appending to its own copy.
func (sf *segmentFile) view() segmentView {
	return segmentView{
		f:          sf.f,
		name:       sf.name,
		firstIndex: sf.firstIndex,
		lastIndex:  sf.lastIndex,
		end:        sf.bytes,
		sparse:     slices.Clone(sf.sparse),
	}
}

func newStreamState(id string, inc incarnation, partition uint32, cfg durablestream.StreamConfig) *streamState {
	return &streamState{
		id:        id,
		inc:       inc,
		partition: partition,
		cfg:       cfg,
		firstLive: 1,
		nextIndex: 1,
		notifyCh:  make(chan struct{}),
	}
}

// wake releases every current waiter and installs a fresh channel. It is a
// no-op once the incarnation is deleted (the channel stays closed).
func (st *streamState) wake() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return
	}
	close(st.notifyCh)
	st.notifyCh = make(chan struct{})
}

// markDeleted permanently closes the notification channel. Waiters holding
// this pointer observe the deletion even if a later incarnation reuses the
// stream ID.
func (st *streamState) markDeleted() {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.deleted {
		return
	}
	st.deleted = true
	close(st.notifyCh)
}

// readSnapshot is a consistent view of the fields Read/Head/WaitForData need,
// taken under RLock so file I/O can happen without holding any lock.
type readSnapshot struct {
	inc       incarnation
	cfg       durablestream.StreamConfig
	closed    bool
	deleted   bool
	lastSeq   string
	retention Retention
	floor     int64
	tail      int64
	firstLive int64
	walTail   []walLoc // shared read-only prefix; never mutated in place

	sealed     []*segmentFile // immutable once sealed
	activeView segmentView
	through    int64 // materializedThrough
}

func (st *streamState) snapshot() readSnapshot {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return readSnapshot{
		inc:        st.inc,
		cfg:        st.cfg,
		closed:     st.closed,
		deleted:    st.deleted,
		lastSeq:    st.lastSeq,
		retention:  st.retention,
		floor:      st.floor,
		tail:       st.nextIndex - 1,
		firstLive:  st.firstLive,
		walTail:    st.walTail,
		sealed:     st.sealed,
		activeView: st.activeView,
		through:    st.materializedThrough,
	}
}
