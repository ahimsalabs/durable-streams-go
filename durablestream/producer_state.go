package durablestream

import (
	"strings"
	"sync"
	"time"
)

const (
	// defaultMaxProducers bounds committed producer states plus first appends in
	// flight. Non-expired committed state is never evicted just to make room:
	// doing so would make an already-accepted retry append a duplicate and defeat
	// fencing.
	defaultMaxProducers = 10000
	// defaultProducerStateTTL bounds how long an idle in-memory producer state is
	// retained. PROTOCOL.md Section 5.2.1 recommends seven days for in-memory
	// implementations. Expiry trades deduplication of very old retries for bounded
	// memory; callers that need stronger guarantees must persist producer state with
	// the append itself.
	defaultProducerStateTTL = 7 * 24 * time.Hour

	// Producer IDs and stream IDs are client-controlled strings retained by the
	// registry. Bounding each key keeps MaxProducers a meaningful memory bound,
	// rather than allowing a few thousand maximum-size HTTP headers or URLs to
	// retain gigabytes. The limits are in bytes, matching their memory cost.
	maxProducerIDBytes  = 256
	maxProducerKeyBytes = 4096
)

// producerState tracks the state for a single producer on a single stream.
// Per PROTOCOL.md Section 5.2.1.
type producerState struct {
	epoch   int64 // Current epoch for this producer
	lastSeq int64 // Highest accepted sequence number in current epoch
	known   bool  // False until the first append for this producer commits
}

// producerKey uniquely identifies a producer on a stream.
type producerKey struct {
	streamID   string
	producerID string
}

// producerEntry holds the state for one (stream, producer) pair together with the
// mutex that serializes validate → append → commit for it.
//
// Section 5.2.1 requires servers to serialize validation and append per
// (stream, producerId): pipelined requests can arrive out of order, and without
// serialization seq=N+1 can validate against state that seq=N has not yet
// committed, producing a spurious 409 that wedges the producer permanently.
//
// The lock is per producer rather than global. Handler additionally serializes
// mutations of one stream so validation cannot cross a delete/recreate boundary;
// unrelated streams still proceed concurrently.
type producerEntry struct {
	mu sync.Mutex // held across validate → append → commit
	// key is immutable. New entries clone both strings before retaining them, so
	// a short path/header cannot keep a much larger HTTP parse buffer alive.
	key producerKey

	// state is guarded by mu.
	state producerState

	// pins is guarded by the owning registry's mu.
	pins int

	// lastAccess is guarded by the owning registry's mu. Both acquisition and
	// release refresh it, so a request that runs longer than the TTL is considered
	// active until it completes rather than immediately expiring afterwards.
	lastAccess time.Time
}

// validate applies the Section 5.2.1 validation logic. Callers must hold e.mu.
func (e *producerEntry) validate(epoch, seq int64) producerDecision {
	if !e.state.known {
		// Unknown producer: first sequence of any epoch must be 0.
		if seq != 0 {
			return producerDecision{outcome: producerGap, expectedSeq: 0, receivedSeq: seq, epoch: epoch}
		}
		return producerDecision{outcome: producerAccept, epoch: epoch, seq: seq}
	}

	if epoch < e.state.epoch {
		// Stale epoch: zombie fencing.
		return producerDecision{outcome: producerStaleEpoch, epoch: e.state.epoch}
	}

	if epoch > e.state.epoch {
		if seq != 0 {
			return producerDecision{outcome: producerEpochRestart, epoch: epoch}
		}
		return producerDecision{outcome: producerAccept, epoch: epoch, seq: seq}
	}

	// Same epoch: sequence validation.
	switch {
	case seq <= e.state.lastSeq:
		return producerDecision{outcome: producerDuplicate, epoch: e.state.epoch, seq: e.state.lastSeq}
	case seq == e.state.lastSeq+1:
		return producerDecision{outcome: producerAccept, epoch: epoch, seq: seq}
	default:
		return producerDecision{outcome: producerGap, expectedSeq: e.state.lastSeq + 1, receivedSeq: seq, epoch: e.state.epoch}
	}
}

// commit records a successful append. Callers must hold e.mu.
//
// lastSeq never moves backwards within an epoch: a lower sequence for the current
// epoch has already been superseded, and rewinding it would let an already-accepted
// sequence be accepted a second time.
func (e *producerEntry) commit(epoch, seq int64) {
	switch {
	case !e.state.known || epoch > e.state.epoch:
		e.state = producerState{epoch: epoch, lastSeq: seq, known: true}
	case epoch == e.state.epoch && seq > e.state.lastSeq:
		e.state.lastSeq = seq
	}
}

// producerOutcome enumerates the results of producer validation.
type producerOutcome int

const (
	// producerAccept means the append may proceed and then be committed.
	producerAccept producerOutcome = iota
	// producerDuplicate means the append was already accepted (204, idempotent success).
	producerDuplicate
	// producerStaleEpoch means the producer was fenced by a newer epoch (403).
	producerStaleEpoch
	// producerEpochRestart means a new epoch was declared without seq=0 (400).
	producerEpochRestart
	// producerGap means a sequence number was skipped (409).
	producerGap
)

// producerDecision is the result of validating producer headers against stored state.
type producerDecision struct {
	outcome     producerOutcome
	epoch       int64 // Epoch to echo in the response
	seq         int64 // Highest accepted sequence (duplicate/accept)
	expectedSeq int64 // Expected sequence, on producerGap
	receivedSeq int64 // Sequence the client sent, on producerGap
}

// producerRegistry stores process-local producer state for all streams. A first
// request is kept in pending until its append commits. Committed state is never
// evicted merely for capacity: at capacity, a brand-new producer is rejected
// while non-expired producers keep their deduplication and fencing guarantees.
// Idle entries expire after ttl, as permitted by PROTOCOL.md Section 5.2.1, and
// Delete/recreate explicitly forgets all state belonging to the old stream
// incarnation.
//
// acquire never waits for an entry lock while holding registry.mu. release takes
// registry.mu while it still holds the entry lock so state cannot change between
// deciding whether to promote a pending entry and updating the maps. No path
// takes registry.mu and then waits for an entry lock, so this order cannot form a
// cycle.
type producerRegistry struct {
	mu      sync.Mutex
	max     int
	ttl     time.Duration
	now     func() time.Time
	entries map[producerKey]*producerEntry // committed entries only
	pending map[producerKey]*producerEntry // first append not yet committed
}

func newProducerRegistry(max int, ttl time.Duration) *producerRegistry {
	return newProducerRegistryWithClock(max, ttl, time.Now)
}

// newProducerRegistryWithClock exists to make expiry behavior deterministic in
// tests. Production callers use newProducerRegistry, whose clock is time.Now.
func newProducerRegistryWithClock(max int, ttl time.Duration, now func() time.Time) *producerRegistry {
	if max <= 0 {
		max = defaultMaxProducers
	}
	if ttl <= 0 {
		ttl = defaultProducerStateTTL
	}
	if now == nil {
		now = time.Now
	}
	return &producerRegistry{
		max:     max,
		ttl:     ttl,
		now:     now,
		entries: make(map[producerKey]*producerEntry),
		pending: make(map[producerKey]*producerEntry),
	}
}

// acquire returns the entry for key with its mutex held, creating it if needed.
// The caller must call release exactly once. It returns nil when the hard bound
// on concurrent first appends is full; existing non-expired producers remain
// available.
func (r *producerRegistry) acquire(key producerKey) *producerEntry {
	r.mu.Lock()
	now := r.now()
	entry, ok := r.entries[key]
	if ok && entry.pins == 0 && r.expired(entry, now) {
		// An idle producer whose state TTL elapsed is unknown again. Removing it
		// before lookup is important: otherwise its first retry after expiry would
		// refresh and retain stale deduplication state forever.
		delete(r.entries, key)
		entry, ok = nil, false
	}
	if !ok {
		entry, ok = r.pending[key]
		if !ok {
			if len(r.entries)+len(r.pending) >= r.max {
				r.pruneExpired(now)
			}
			if len(r.entries)+len(r.pending) >= r.max {
				r.mu.Unlock()
				return nil
			}
			// Clone before retention: strings from URL/header parsing may be
			// slices of substantially larger request buffers.
			key = producerKey{
				streamID:   strings.Clone(key.streamID),
				producerID: strings.Clone(key.producerID),
			}
			entry = &producerEntry{key: key, lastAccess: now}
			r.pending[key] = entry
		}
	}
	entry.pins++
	entry.lastAccess = now
	r.mu.Unlock()

	entry.mu.Lock()
	return entry
}

// release unpins an entry acquired by acquire and unlocks it.
func (r *producerRegistry) release(entry *producerEntry) {
	// Keep entry.mu held until the registry reflects whether this request
	// committed. Otherwise a queued request could commit between the state check
	// and removal of what appeared to be an unused pending entry.
	r.mu.Lock()
	entry.pins--
	entry.lastAccess = r.now()

	if pending, ok := r.pending[entry.key]; ok && pending == entry {
		switch {
		case entry.state.known:
			// Promotion keeps the total entry count unchanged (pending ->
			// committed), so it cannot exceed the admission bound.
			delete(r.pending, entry.key)
			r.entries[entry.key] = entry
		case entry.pins == 0:
			// Every request for this new producer was rejected before commit.
			delete(r.pending, entry.key)
		}
	}
	r.mu.Unlock()
	entry.mu.Unlock()
}

// expired reports whether entry has been idle for at least the configured TTL.
// Callers must hold r.mu. A backwards-moving test or wall clock never expires an
// entry early.
func (r *producerRegistry) expired(entry *producerEntry, now time.Time) bool {
	if now.Before(entry.lastAccess) {
		return false
	}
	return now.Sub(entry.lastAccess) >= r.ttl
}

// pruneExpired removes only idle, unpinned committed state. Pending first
// appends and pinned committed entries represent active requests and are never
// reclaimed. Callers must hold r.mu.
func (r *producerRegistry) pruneExpired(now time.Time) {
	for key, entry := range r.entries {
		if entry.pins == 0 && r.expired(entry, now) {
			delete(r.entries, key)
		}
	}
}

// forget drops all producer state for a stream. Entries that are currently pinned
// are detached from the registry rather than mutated: the in-flight request keeps
// writing to its own (now orphaned) entry, and the next request for that producer
// starts from a clean slate.
func (r *producerRegistry) forget(streamID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Scanning every entry is O(len(entries)), bounded by r.max. Stream deletes are
	// rare compared to appends, so this avoids maintaining a second index.
	for key := range r.entries {
		if key.streamID == streamID {
			delete(r.entries, key)
		}
	}
	for key := range r.pending {
		if key.streamID == streamID {
			// A pinned request retains the entry itself, but it is orphaned: its
			// eventual release must not promote state for the deleted stream.
			delete(r.pending, key)
		}
	}
}

// len returns the number of tracked producers. For tests and diagnostics.
func (r *producerRegistry) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}
