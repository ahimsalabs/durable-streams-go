package seglog

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// request is one logical mutation submitted to a partition worker. Message
// slices are borrowed: submit blocks until completion, so the worker may
// encode them without copying.
type request struct {
	op        opKind
	streamID  string
	cfg       durablestream.StreamConfig // opCreate
	messages  [][]byte
	seq       string
	hasSeq    bool
	close     bool      // opAppend: stream reaches permanent EOF after messages
	retention Retention // opRetention
	floor     int64     // opTrim
	done      chan result
}

type result struct {
	created bool
	offset  durablestream.Offset
	err     error

	// Barrier results: the WAL position and txnID with every prior frame
	// committed and published.
	walSeq  uint64
	walOff  int64
	nextTxn uint64
}

// opBarrier is a queue-only operation (never a WAL frame): its result carries
// the WAL position at which every earlier submission has been committed and
// published. The materializer uses it to bound checkpoint advancement.
const opBarrier opKind = 0xff

// partition is one WAL partition and its worker. The worker goroutine is the
// only writer of the partition's WAL and the only mutator of its streams'
// state (invariants I5/I6).
type partition struct {
	id  uint32
	st  *Storage
	wal *walWriter

	queue chan *request

	// Admission close protocol (see badgerstore's appendCommitter): senders
	// register under admissionMu before the potentially-blocking queue send,
	// so closeAdmission can Wait for every in-flight sender before closing
	// queue without racing a send.
	admissionMu sync.Mutex
	accepting   bool
	stop        chan struct{}
	senders     sync.WaitGroup

	// Worker-owned state, touched only by run() (and recovery, before run
	// starts).
	nextTxnID uint64
	lastTS    int64
	encodeBuf []byte
	broken    error // latched fail-stop error after a WAL I/O failure

	// Materializer coordination. The worker adds under dirtyMu at publish
	// time; the materializer swaps the containers each round.
	dirtyMu  sync.Mutex
	dirty    map[*streamState]struct{}
	removals []*streamState // dead incarnations awaiting directory removal

	// Materializer-owned: the last durably written checkpoint position.
	ckptSeq uint64
	ckptOff int64
}

func newPartition(id uint32, st *Storage, wal *walWriter) *partition {
	return &partition{
		id:        id,
		st:        st,
		wal:       wal,
		queue:     make(chan *request, st.opts.QueueDepth),
		accepting: true,
		stop:      make(chan struct{}),
		nextTxnID: 1,
		dirty:     make(map[*streamState]struct{}),
	}
}

// markDirty records that a stream's state changed since the materializer's
// last round.
func (p *partition) markDirty(st *streamState) {
	p.dirtyMu.Lock()
	p.dirty[st] = struct{}{}
	p.dirtyMu.Unlock()
}

// markRemoval queues a dead incarnation's directory for removal.
func (p *partition) markRemoval(st *streamState) {
	p.dirtyMu.Lock()
	p.removals = append(p.removals, st)
	p.dirtyMu.Unlock()
}

// swapDirty hands the current dirty set and removal list to the materializer.
func (p *partition) swapDirty() (map[*streamState]struct{}, []*streamState) {
	p.dirtyMu.Lock()
	dirty, removals := p.dirty, p.removals
	p.dirty = make(map[*streamState]struct{})
	p.removals = nil
	p.dirtyMu.Unlock()
	return dirty, removals
}

// submit hands a request to the worker and blocks until it completes. It is
// deliberately not context-cancellable after admission: the worker borrows
// the request's message slices, so returning early would let the caller
// mutate them mid-encode. Group linger and fdatasync latency bound the wait.
func (p *partition) submit(req *request) result {
	p.admissionMu.Lock()
	if !p.accepting {
		p.admissionMu.Unlock()
		return result{err: ErrClosed}
	}
	p.senders.Add(1)
	p.admissionMu.Unlock()

	select {
	case p.queue <- req:
		p.senders.Done()
	case <-p.stop:
		p.senders.Done()
		return result{err: ErrClosed}
	}
	return <-req.done
}

// closeAdmission stops new submissions and closes the queue once every
// in-flight sender has either enqueued or observed stop. The worker drains
// remaining queued requests (they complete durably) before exiting.
func (p *partition) closeAdmission() {
	p.admissionMu.Lock()
	if !p.accepting {
		p.admissionMu.Unlock()
		return
	}
	p.accepting = false
	close(p.stop)
	p.admissionMu.Unlock()

	p.senders.Wait()
	close(p.queue)
}

// run is the worker loop: collect a group, commit it, apply it, repeat until
// the queue drains closed.
func (p *partition) run() {
	var carry *request
	for {
		first := carry
		if first == nil {
			var ok bool
			first, ok = <-p.queue
			if !ok {
				return
			}
		}
		var queueClosed bool
		var group []*request
		group, queueClosed, carry = p.collect(first)
		p.processGroup(group)
		if queueClosed {
			return
		}
	}
}

// Guaranteed upper bounds on encoded JSON meta documents. createMetaBound
// covers every fixed field (timestamps in RFC 3339 with nanoseconds); the
// content type is bounded separately at 6x for worst-case \uXXXX escaping.
const (
	createMetaBound = 288
	touchMetaBound  = 96
)

// metaBoundForCreate bounds an opCreate frame's meta document.
func metaBoundForCreate(contentType string) int {
	return createMetaBound + 6*len(contentType)
}

// estimateFrameBytes returns a guaranteed overestimate of a request's encoded
// frame size. Group admission relies on it never undercounting: as long as
// the estimated group total fits the segment capacity, the encoded group does
// too, so a commit group never has to span segments.
func estimateFrameBytes(req *request) int64 {
	var metaLen int
	switch req.op {
	case opAppend:
		metaLen = len(req.seq)
	case opCreate:
		metaLen = metaBoundForCreate(req.cfg.ContentType)
	default:
		metaLen = touchMetaBound
	}
	return encodedFrameSize(len(req.streamID), metaLen, req.messages)
}

// collect gathers requests for one commit group, waiting at most GroupLinger
// from the first request and stopping at GroupMaxBytes. A request that would
// cross the byte bound is carried into the next group rather than split.
func (p *partition) collect(first *request) (group []*request, queueClosed bool, carry *request) {
	group = []*request{first}
	groupBytes := estimateFrameBytes(first)
	// The segment capacity is a hard bound (a group never spans segments);
	// GroupMaxBytes is the tuning bound within it.
	maxBytes := min(p.st.opts.GroupMaxBytes, p.wal.capacity())
	if groupBytes >= maxBytes {
		return group, false, nil
	}

	var timerC <-chan time.Time
	if linger := p.st.opts.GroupLinger; linger > 0 {
		timer := time.NewTimer(linger)
		timerC = timer.C
		defer stopTimer(timer)
	}

	for {
		var req *request
		var ok bool
		if timerC == nil {
			select {
			case req, ok = <-p.queue:
			default:
				return group, false, nil
			}
		} else {
			select {
			case req, ok = <-p.queue:
			case <-timerC:
				return group, false, nil
			}
		}
		if !ok {
			return group, true, nil
		}
		size := estimateFrameBytes(req)
		if groupBytes > maxBytes-size {
			return group, false, req
		}
		groupBytes += size
		group = append(group, req)
	}
}

// stopTimer releases a timer when another event wins the select, draining the
// fired-timer race.
func stopTimer(timer *time.Timer) {
	if timer == nil || timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

// stagedOp is one request's validated outcome. Exactly one of the apply*
// fields is set when the request encoded a frame; all nil means the request
// completes without touching the WAL (validation failure or idempotent no-op)
// and res is already final.
type stagedOp struct {
	req *request
	res result

	barrier bool

	applyCreate    *createApply
	applyAppend    *appendApply
	applyDelete    *deleteApply
	applyTouch     *touchApply
	applyRetention *retentionApply
	applyTrim      *trimApply

	// payloadOffs are each payload's offset relative to the group buffer,
	// filled after encoding.
	payloadOffs []int64
	payloadLens []int32
	ts          int64
}

type createApply struct {
	newState  *streamState
	displaced *streamState // expired incarnation being replaced, may be nil
}

type appendApply struct {
	state      *streamState
	firstIndex int64
	count      int64
	seq        string
	hasSeq     bool
	close      bool
}

type deleteApply struct{ state *streamState }

type touchApply struct {
	state     *streamState
	expiresAt time.Time
}

type retentionApply struct {
	state     *streamState
	retention Retention
}

type trimApply struct {
	state *streamState
	floor int64
}

func (op *stagedOp) hasFrame() bool {
	return op.applyCreate != nil || op.applyAppend != nil || op.applyDelete != nil || op.applyTouch != nil ||
		op.applyRetention != nil || op.applyTrim != nil
}

// pendingStream is the staging overlay for one stream within a group: later
// requests in the same group observe the effects of earlier staged ones.
type pendingStream struct {
	state  *streamState // live state, or the staged new state for creations
	exists bool

	cfg       durablestream.StreamConfig
	closed    bool
	nextIndex int64
	lastSeq   string
	retention Retention
	floor     int64
}

// processGroup validates and encodes every request, commits the encoded
// frames with one write and one fdatasync, then publishes and completes them
// in order. Requests are never atomically coupled: each frame stands alone,
// and only a WAL I/O failure — which fail-stops the partition — is group-wide.
func (p *partition) processGroup(group []*request) {
	if p.broken != nil {
		for _, req := range group {
			req.done <- result{err: p.broken}
		}
		return
	}

	buf := p.encodeBuf[:0]
	pending := make(map[string]*pendingStream)
	staged := make([]*stagedOp, 0, len(group))
	now := time.Now()
	ts := max(now.UnixNano(), p.lastTS) // clamp against wall-clock regression
	p.lastTS = ts

	for _, req := range group {
		op, frame := p.stage(req, pending, now, ts)
		if op.hasFrame() {
			frame.txnID = p.nextTxnID
			p.nextTxnID++
			buf, op.payloadOffs = appendFrame(buf, frame)
			op.payloadLens = make([]int32, len(frame.payloads))
			for i, pl := range frame.payloads {
				op.payloadLens[i] = int32(len(pl))
			}
			op.ts = ts
		}
		staged = append(staged, op)
	}

	var segSeq uint64
	var base int64
	if len(buf) > 0 {
		var err error
		segSeq, base, err = p.wal.appendGroup(buf)
		if err != nil {
			// Fail-stop: after a failed write or sync the durable state of
			// the WAL tail is unknowable, so the partition refuses further
			// work rather than risk acknowledging after a failed sync.
			p.broken = fmt.Errorf("seglog: partition %d WAL failure: %w", p.id, err)
			p.st.opts.SLogger.Error("seglog partition failed, refusing further writes",
				"partition", p.id, "error", err)
			for _, op := range staged {
				if op.hasFrame() {
					op.res = result{err: p.broken}
				}
				op.req.done <- op.res
			}
			return
		}
	}
	if cap(buf) <= maxRetainedEncodeBuf {
		p.encodeBuf = buf[:0]
	} else {
		p.encodeBuf = nil
	}

	// Publish everything before delivering any result: a barrier's result
	// must not reach the materializer while frames later in this group are
	// still unpublished, or a checkpoint could advance past them.
	for _, op := range staged {
		p.publish(op, segSeq, base)
	}
	for _, op := range staged {
		op.req.done <- op.res
	}
}

// maxRetainedEncodeBuf caps the group buffer kept across groups so one huge
// append does not pin its allocation forever.
const maxRetainedEncodeBuf = 8 << 20

// publish applies one durable frame to the in-memory catalog and wakes
// waiters (invariant I3: only after the group fdatasync).
func (p *partition) publish(op *stagedOp, segSeq uint64, base int64) {
	switch {
	case op.barrier:
		// Everything submitted before this barrier is committed and
		// published; report the WAL frontier for checkpointing.
		op.res.walSeq = p.wal.activeSeq
		op.res.walOff = p.wal.writePos
		op.res.nextTxn = p.nextTxnID
	case op.applyCreate != nil:
		a := op.applyCreate
		st := a.newState
		for i, rel := range op.payloadOffs {
			st.walTail = append(st.walTail, walLoc{
				segmentSeq: segSeq,
				off:        base + rel,
				length:     op.payloadLens[i],
				batchFirst: 1,
				ts:         op.ts,
			})
		}
		p.st.streams.Store(st.id, st)
		if a.displaced != nil {
			a.displaced.markDeleted()
			p.markRemoval(a.displaced)
		}
		p.markDirty(st)
	case op.applyAppend != nil:
		a := op.applyAppend
		st := a.state
		st.mu.Lock()
		for i, rel := range op.payloadOffs {
			st.walTail = append(st.walTail, walLoc{
				segmentSeq: segSeq,
				off:        base + rel,
				length:     op.payloadLens[i],
				batchFirst: a.firstIndex,
				ts:         op.ts,
			})
		}
		if a.count > 0 {
			st.nextIndex = a.firstIndex + a.count
		}
		if a.hasSeq {
			st.lastSeq = a.seq
		}
		if a.close {
			st.closed = true
		}
		st.mu.Unlock()
		st.wake()
		p.markDirty(st)
	case op.applyDelete != nil:
		a := op.applyDelete
		a.state.markDeleted()
		p.st.streams.CompareAndDelete(a.state.id, a.state)
		p.markRemoval(a.state)
	case op.applyTouch != nil:
		a := op.applyTouch
		a.state.mu.Lock()
		a.state.cfg.ExpiresAt = a.expiresAt
		a.state.mu.Unlock()
		a.state.wake()
		p.markDirty(a.state)
	case op.applyRetention != nil:
		a := op.applyRetention
		a.state.mu.Lock()
		a.state.retention = a.retention
		a.state.mu.Unlock()
		p.markDirty(a.state)
	case op.applyTrim != nil:
		a := op.applyTrim
		a.state.mu.Lock()
		a.state.floor = max(a.state.floor, a.floor)
		a.state.mu.Unlock()
		a.state.wake()
		p.markDirty(a.state)
	}
}

// loadPending resolves the staging overlay for a stream, seeding it from the
// catalog on first use. Worker-owned fields are read lock-free (I5).
func (p *partition) loadPending(pending map[string]*pendingStream, streamID string) *pendingStream {
	if ps, ok := pending[streamID]; ok {
		return ps
	}
	ps := &pendingStream{}
	if state, ok := p.st.streams.Load(streamID); ok && !state.deleted {
		ps.state = state
		ps.exists = true
		ps.cfg = state.cfg
		ps.closed = state.closed
		ps.nextIndex = state.nextIndex
		ps.lastSeq = state.lastSeq
		ps.retention = state.retention
		ps.floor = state.floor
	}
	pending[streamID] = ps
	return ps
}

// stage validates one request against the overlay and, when it mutates the
// stream, prepares its frame spec and apply context.
func (p *partition) stage(req *request, pending map[string]*pendingStream, now time.Time, ts int64) (*stagedOp, frameSpec) {
	op := &stagedOp{req: req}
	if req.op == opBarrier {
		op.barrier = true // resolved in publish, after the group commits
		return op, frameSpec{}
	}
	ps := p.loadPending(pending, req.streamID)
	expired := ps.exists && !ps.cfg.ExpiresAt.IsZero() && now.After(ps.cfg.ExpiresAt)

	switch req.op {
	case opCreate:
		return op, p.stageCreate(op, req, ps, expired, now, ts)
	case opAppend:
		return op, p.stageAppend(op, req, ps, expired, ts)
	case opDelete:
		return op, p.stageDelete(op, req, ps, ts)
	case opTouch:
		return op, p.stageTouch(op, req, ps, expired, now, ts)
	case opRetention:
		return op, p.stageRetention(op, req, ps, expired, ts)
	case opTrim:
		return op, p.stageTrim(op, req, ps, expired, ts)
	default:
		op.res = result{err: fmt.Errorf("seglog: unknown operation %d: %w", req.op, durablestream.ErrBadRequest)}
		return op, frameSpec{}
	}
}

// createMeta is the JSON meta document of an opCreate frame.
type createMeta struct {
	ContentType string         `json:"contentType,omitempty"`
	TTLNanos    int64          `json:"ttlNanos,omitempty"`
	ExpiresAt   time.Time      `json:"expiresAt,omitzero"`
	IsPrivate   bool           `json:"isPrivate,omitempty"`
	Closed      bool           `json:"closed,omitempty"`
	Retention   *retentionMeta `json:"retention,omitempty"`
}

// touchMeta is the JSON meta document of an opTouch frame.
type touchMeta struct {
	ExpiresAt time.Time `json:"expiresAt"`
}

type retentionMeta struct {
	MaxBytes    int64 `json:"maxBytes"`
	MaxAgeNanos int64 `json:"maxAgeNanos"`
}

type trimMeta struct {
	FloorIndex int64 `json:"floorIndex"`
}

func (p *partition) stageCreate(op *stagedOp, req *request, ps *pendingStream, expired bool, now time.Time, ts int64) frameSpec {
	if ps.exists && !expired {
		if ps.cfg.Matches(req.cfg) {
			op.res = result{created: false, offset: storage.FormatSimpleOffset(ps.nextIndex - 1)}
		} else {
			op.res = result{err: fmt.Errorf("seglog: stream %q exists with different config: %w", req.streamID, durablestream.ErrConflict)}
		}
		return frameSpec{}
	}

	inc, err := newIncarnation()
	if err != nil {
		op.res = result{err: err}
		return frameSpec{}
	}
	cfg := req.cfg
	if cfg.TTL > 0 && cfg.ExpiresAt.IsZero() {
		cfg.ExpiresAt = now.Add(cfg.TTL)
	}
	defaultRetention := p.st.opts.DefaultRetention
	meta, err := json.Marshal(createMeta{
		ContentType: cfg.ContentType,
		TTLNanos:    int64(cfg.TTL),
		ExpiresAt:   cfg.ExpiresAt,
		IsPrivate:   cfg.IsPrivate,
		Closed:      cfg.Closed,
		Retention: &retentionMeta{
			MaxBytes:    defaultRetention.MaxBytes,
			MaxAgeNanos: int64(defaultRetention.MaxAge),
		},
	})
	if err != nil {
		op.res = result{err: fmt.Errorf("seglog: encode create meta: %w", err)}
		return frameSpec{}
	}

	var flags uint8
	if cfg.Closed {
		flags |= flagClosedAtCreate
	}
	firstIndex := int64(0)
	if len(req.messages) > 0 {
		firstIndex = 1
	}

	newState := newStreamState(req.streamID, inc, p.id, cfg)
	newState.retention = defaultRetention
	newState.closed = cfg.Closed
	newState.nextIndex = 1 + int64(len(req.messages))

	op.applyCreate = &createApply{newState: newState, displaced: ps.state}
	op.res = result{created: true, offset: storage.FormatSimpleOffset(newState.nextIndex - 1)}

	ps.state = newState
	ps.exists = true
	ps.cfg = cfg
	ps.closed = cfg.Closed
	ps.nextIndex = newState.nextIndex
	ps.lastSeq = ""
	ps.retention = newState.retention
	ps.floor = 0

	return frameSpec{
		op:         opCreate,
		flags:      flags,
		streamID:   req.streamID,
		inc:        inc,
		meta:       meta,
		firstIndex: firstIndex,
		ts:         ts,
		payloads:   req.messages,
	}
}

func (p *partition) stageAppend(op *stagedOp, req *request, ps *pendingStream, expired bool, ts int64) frameSpec {
	if !ps.exists || expired {
		op.res = result{err: fmt.Errorf("seglog: stream %q not found: %w", req.streamID, durablestream.ErrNotFound)}
		return frameSpec{}
	}
	if ps.closed {
		if req.close && len(req.messages) == 0 {
			// Repeating a close-only mutation is idempotent.
			op.res = result{offset: storage.FormatSimpleOffset(ps.nextIndex - 1)}
		} else {
			op.res = result{err: fmt.Errorf("seglog: stream %q is closed: %w", req.streamID, durablestream.ErrStreamClosed)}
		}
		return frameSpec{}
	}
	if req.hasSeq && req.seq <= ps.lastSeq {
		op.res = result{err: fmt.Errorf("seglog: sequence %q does not advance past %q: %w", req.seq, ps.lastSeq, durablestream.ErrConflict)}
		return frameSpec{}
	}

	var flags uint8
	var meta []byte
	if req.hasSeq {
		flags |= flagHasSeq
		meta = []byte(req.seq)
	}
	if req.close {
		flags |= flagClose
	}
	firstIndex := int64(0)
	if len(req.messages) > 0 {
		firstIndex = ps.nextIndex
	}

	op.applyAppend = &appendApply{
		state:      ps.state,
		firstIndex: firstIndex,
		count:      int64(len(req.messages)),
		seq:        req.seq,
		hasSeq:     req.hasSeq,
		close:      req.close,
	}

	ps.nextIndex += int64(len(req.messages))
	if req.hasSeq {
		ps.lastSeq = req.seq
	}
	if req.close {
		ps.closed = true
	}
	op.res = result{offset: storage.FormatSimpleOffset(ps.nextIndex - 1)}

	return frameSpec{
		op:         opAppend,
		flags:      flags,
		streamID:   req.streamID,
		inc:        op.applyAppend.state.inc,
		meta:       meta,
		firstIndex: firstIndex,
		ts:         ts,
		payloads:   req.messages,
	}
}

func (p *partition) stageDelete(op *stagedOp, req *request, ps *pendingStream, ts int64) frameSpec {
	// An expired-but-present record is still deleted successfully: expiry
	// hides it from readers, but its record is there to reclaim.
	if !ps.exists {
		op.res = result{err: fmt.Errorf("seglog: stream %q not found: %w", req.streamID, durablestream.ErrNotFound)}
		return frameSpec{}
	}
	state := ps.state
	ps.exists = false
	ps.state = nil

	op.applyDelete = &deleteApply{state: state}
	return frameSpec{
		op:       opDelete,
		streamID: req.streamID,
		inc:      state.inc,
		ts:       ts,
	}
}

func (p *partition) stageTouch(op *stagedOp, req *request, ps *pendingStream, expired bool, now time.Time, ts int64) frameSpec {
	if !ps.exists || expired {
		op.res = result{err: fmt.Errorf("seglog: stream %q not found: %w", req.streamID, durablestream.ErrNotFound)}
		return frameSpec{}
	}
	slid, moved := ps.cfg.SlideExpiry(now)
	if !moved {
		op.res = result{}
		return frameSpec{}
	}
	meta, err := json.Marshal(touchMeta{ExpiresAt: slid.ExpiresAt})
	if err != nil {
		op.res = result{err: fmt.Errorf("seglog: encode touch meta: %w", err)}
		return frameSpec{}
	}

	op.applyTouch = &touchApply{state: ps.state, expiresAt: slid.ExpiresAt}
	ps.cfg = slid
	return frameSpec{
		op:       opTouch,
		streamID: req.streamID,
		inc:      op.applyTouch.state.inc,
		meta:     meta,
		ts:       ts,
	}
}

func (p *partition) stageRetention(op *stagedOp, req *request, ps *pendingStream, expired bool, ts int64) frameSpec {
	if !ps.exists || expired {
		op.res = result{err: notFoundErr(req.streamID)}
		return frameSpec{}
	}
	meta, err := json.Marshal(retentionMeta{MaxBytes: req.retention.MaxBytes, MaxAgeNanos: int64(req.retention.MaxAge)})
	if err != nil {
		op.res = result{err: fmt.Errorf("seglog: encode retention meta: %w", err)}
		return frameSpec{}
	}
	op.applyRetention = &retentionApply{state: ps.state, retention: req.retention}
	ps.retention = req.retention
	return frameSpec{op: opRetention, streamID: req.streamID, inc: ps.state.inc, meta: meta, ts: ts}
}

func (p *partition) stageTrim(op *stagedOp, req *request, ps *pendingStream, expired bool, ts int64) frameSpec {
	if !ps.exists || expired {
		op.res = result{err: notFoundErr(req.streamID)}
		return frameSpec{}
	}
	if req.floor <= ps.floor {
		return frameSpec{}
	}
	meta, err := json.Marshal(trimMeta{FloorIndex: req.floor})
	if err != nil {
		op.res = result{err: fmt.Errorf("seglog: encode trim meta: %w", err)}
		return frameSpec{}
	}
	op.applyTrim = &trimApply{state: ps.state, floor: req.floor}
	ps.floor = req.floor
	return frameSpec{op: opTrim, streamID: req.streamID, inc: ps.state.inc, meta: meta, ts: ts}
}
