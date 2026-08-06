package seglog

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
)

// request is one logical mutation submitted to a partition worker. Message
// slices are borrowed: submit blocks until completion, so the worker may
// encode them without copying.
type request struct {
	op           opKind
	streamID     string
	returnInfo   bool                       // opTouch: return the pre-renewal Head result
	cfg          durablestream.StreamConfig // opCreate
	messages     [][]byte
	seq          string
	hasSeq       bool
	close        bool      // opAppend: stream reaches permanent EOF after messages
	retention    Retention // opRetention
	floor        int64     // opTrim
	softDelete   bool
	hardCascade  bool
	forkSource   *streamState
	forkMeta     *forkMeta
	forkMetaRaw  []byte
	forkBoundary int64
	prefixCount  int64
	captureDirty bool // opBarrier: atomically hand the materializer its frontier
	done         chan result
}

type result struct {
	created bool
	offset  durablestream.Offset
	info    *durablestream.StreamInfo
	err     error
	// ambiguous is set when a WAL write/sync failed after bytes may have
	// reached durable media. Callers must conservatively retain topology pins.
	ambiguous bool

	// Barrier results: the WAL position and txnID with every prior frame
	// committed and published.
	walSeq   uint64
	walOff   int64
	nextTxn  uint64
	dirty    map[*streamState]struct{}
	removals []*streamState
}

// opBarrier is a queue-only operation (never a WAL frame): its result carries
// the WAL position at which every earlier submission has been committed and
// published. The materializer uses it to bound checkpoint advancement.
const opBarrier opKind = 0xff

// partition is one WAL partition and its bounded stager/committer worker
// (invariants I5/I6).
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

	// Stager-owned state, touched only by run() (and recovery, before run
	// starts). These allocation cursors may run ahead of publication.
	nextTxnID uint64
	lastTS    int64
	encodeBuf []byte

	// broken is latched by the committer after the first FIFO WAL failure and
	// read by the stager to stop further encoding. published* is committer-owned
	// while run is active and read after run exits by finalMaterialize.
	brokenMu        sync.Mutex
	broken          error
	publishedSeq    uint64
	publishedOff    int64
	publishedNextTx uint64

	// Materializer coordination. The worker adds under dirtyMu at publish
	// time; the materializer swaps the containers each round.
	dirtyMu  sync.Mutex
	dirty    map[*streamState]struct{}
	removals []*streamState // dead incarnations awaiting directory removal

	// Materializer-owned: the last durably written checkpoint position.
	ckptSeq   uint64
	ckptOff   int64
	ckptState []byte
	// materializedEntries is the cumulative image including materialization
	// rounds published since the checkpoint became durable. Materializer-owned.
	materializedEntries map[string]streamCheckpointEntry
	lastCheckpoint      time.Time
	pending             *materializationBatch
	// unsyncedFiles are segment payloads and sidecars published since the last
	// checkpoint. The WAL covers them until checkpoint-time batch sync makes
	// their current prefixes durable.
	unsyncedFiles   map[string]struct{}
	unsyncedDirs    map[string]struct{}
	unsyncedParents map[string]struct{}
	// sealedSidecars become removable only after a checkpoint durably names
	// their payload files as sealed. Delayed-checkpoint rounds accumulate them
	// here until that checkpoint commits.
	sealedSidecars map[string]struct{}
	cleanupPaths   map[string]struct{}
	cleanupDirs    map[string]struct{}
}

func newPartition(id uint32, st *Storage, wal *walWriter) *partition {
	return &partition{
		id:                  id,
		st:                  st,
		wal:                 wal,
		queue:               make(chan *request, st.opts.QueueDepth),
		accepting:           true,
		stop:                make(chan struct{}),
		nextTxnID:           1,
		dirty:               make(map[*streamState]struct{}),
		materializedEntries: make(map[string]streamCheckpointEntry),
		unsyncedFiles:       make(map[string]struct{}),
		unsyncedDirs:        make(map[string]struct{}),
		unsyncedParents:     make(map[string]struct{}),
		sealedSidecars:      make(map[string]struct{}),
		cleanupPaths:        make(map[string]struct{}),
		cleanupDirs:         make(map[string]struct{}),
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

// stagedGroup is the ownership boundary between the partition's stager and
// committer. The stager never reads or mutates a group after handing it off.
type stagedGroup struct {
	id uint64

	ops []*stagedOp

	segment *os.File
	segSeq  uint64
	base    int64

	endSeq     uint64
	endOff     int64
	endNextTxn uint64

	writeAttempted bool
	writeErr       error
}

type retiredGroup struct {
	id        uint64
	published bool
}

// partitionStager owns the in-flight validation overlay and the bounded
// channels connecting it to the committer. A capacity-one handoff permits one
// group to wait behind the flushing group; GroupMaxBytes and QueueDepth bound
// the forming group and admission queue.
type partitionStager struct {
	p        *partition
	handoff  chan<- *stagedGroup
	retired  <-chan retiredGroup
	inflight map[string]*pendingStream
	last     map[string]uint64

	nextGroupID  uint64
	outstanding  int
	writeStopped bool
}

// run owns both pipeline stages. It starts exactly one committer, drains the
// admission queue, closes the FIFO handoff, consumes every retirement, and
// joins the committer before returning.
func (p *partition) run() {
	p.publishedSeq = p.wal.activeSeq
	p.publishedOff = p.wal.writePos
	p.publishedNextTx = p.nextTxnID
	handoff := make(chan *stagedGroup, 1)
	retired := make(chan retiredGroup, 1)
	committerDone := make(chan struct{})
	// run owns and joins this goroutine. It terminates after handoff closes and
	// every staged group has been published or failed.
	go func() {
		p.commitGroups(handoff, retired)
		close(committerDone)
	}()
	stager := partitionStager{
		p: p, handoff: handoff, retired: retired,
		inflight: make(map[string]*pendingStream), last: make(map[string]uint64),
		nextGroupID: 1,
	}

	var carry *request
	for {
		first := carry
		if first == nil {
			var ok bool
			first, ok = stager.nextRequest()
			if !ok {
				break
			}
		}
		var queueClosed bool
		var group []*request
		group, queueClosed, carry = stager.collect(first)
		stager.send(p.stageGroup(stager.nextGroupID, group, stager.inflight, stager.last, stager.writeStopped))
		if p.brokenErr() != nil {
			stager.writeStopped = true
		}
		stager.nextGroupID++
		if queueClosed {
			break
		}
	}
	close(handoff)
	for stager.outstanding > 0 {
		stager.retire(<-retired)
	}
	<-committerDone
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
	case opFork:
		metaLen = len(req.forkMetaRaw)
	default:
		metaLen = touchMetaBound
	}
	return encodedFrameSize(len(req.streamID), metaLen, req.messages)
}

func (s *partitionStager) nextRequest() (*request, bool) {
	for s.outstanding > 0 {
		select {
		case req, ok := <-s.p.queue:
			return req, ok
		case ack := <-s.retired:
			s.retire(ack)
		}
	}
	req, ok := <-s.p.queue
	return req, ok
}

// collect gathers one bounded commit group. A barrier is a strict queue cut
// and always forms a standalone marker. The first group behind the committer
// drains what is already queued and fills the handoff; while both pipeline
// slots are occupied, the next group remains open until retirement, making
// fdatasync the adaptive clock. Only a fully idle pipeline uses GroupLinger.
// A request crossing GroupMaxBytes is carried, never split.
func (s *partitionStager) collect(first *request) (group []*request, queueClosed bool, carry *request) {
	group = []*request{first}
	if first.op == opBarrier {
		return group, false, nil
	}
	groupBytes := estimateFrameBytes(first)
	// The segment capacity is a hard bound (a group never spans segments);
	// GroupMaxBytes is the tuning bound within it.
	maxBytes := min(s.p.st.opts.GroupMaxBytes, s.p.wal.capacity())
	if groupBytes >= maxBytes {
		return group, false, nil
	}

	flushClocked := s.outstanding >= 2
	var timerC <-chan time.Time
	if linger := s.p.st.opts.GroupLinger; s.outstanding == 0 && linger > 0 && !s.writeStopped {
		timer := time.NewTimer(linger)
		timerC = timer.C
		defer stopTimer(timer)
	}

	for {
		var req *request
		var ok bool
		switch {
		case flushClocked:
			select {
			case req, ok = <-s.p.queue:
			case ack := <-s.retired:
				s.retire(ack)
				return group, false, nil
			}
		case timerC == nil:
			select {
			case req, ok = <-s.p.queue:
			default:
				return group, false, nil
			}
		default:
			select {
			case req, ok = <-s.p.queue:
			case <-timerC:
				return group, false, nil
			}
		}
		if !ok {
			return group, true, nil
		}
		if req.op == opBarrier {
			return group, false, req
		}
		size := estimateFrameBytes(req)
		if groupBytes > maxBytes-size {
			return group, false, req
		}
		groupBytes += size
		group = append(group, req)
	}
}

func (s *partitionStager) send(group *stagedGroup) {
	for {
		select {
		case s.handoff <- group:
			s.outstanding++
			if group.writeErr != nil {
				s.writeStopped = true
			}
			return
		case ack := <-s.retired:
			s.retire(ack)
		}
	}
}

func (s *partitionStager) retire(ack retiredGroup) {
	if s.outstanding > 0 {
		s.outstanding--
	}
	if !ack.published {
		s.writeStopped = true
		return
	}
	for streamID, groupID := range s.last {
		if groupID == ack.id {
			delete(s.last, streamID)
			delete(s.inflight, streamID)
		}
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
	applyFork      *createApply

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

type deleteApply struct {
	state *streamState
	soft  bool
}

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
		op.applyRetention != nil || op.applyTrim != nil || op.applyFork != nil
}

// pendingStream is the stager-owned logical overlay for one stream. It remains
// live across groups until the committer confirms that the last group touching
// the stream published.
type pendingStream struct {
	state  *streamState // live state, or the staged new state for creations
	exists bool

	cfg         durablestream.StreamConfig
	closed      bool
	softDeleted bool
	nextIndex   int64
	lastSeq     string
	retention   Retention
	floor       int64
}

// stageGroup validates and encodes a request group against the cross-group
// overlay, then writes it without syncing. It never publishes catalog state,
// wakes readers, or completes requests; those are committer-only operations.
func (p *partition) stageGroup(groupID uint64, group []*request, inflight map[string]*pendingStream, last map[string]uint64, writeStopped bool) *stagedGroup {
	batch := &stagedGroup{id: groupID, endSeq: p.wal.activeSeq, endOff: p.wal.writePos, endNextTxn: p.nextTxnID}
	if writeStopped || p.brokenErr() != nil {
		batch.ops = make([]*stagedOp, len(group))
		for i, req := range group {
			batch.ops[i] = &stagedOp{req: req}
		}
		return batch
	}
	buf := p.encodeBuf[:0]
	pending := make(map[string]*pendingStream)
	staged := make([]*stagedOp, 0, len(group))
	now := time.Now()
	ts := max(now.UnixNano(), p.lastTS) // clamp against wall-clock regression
	p.lastTS = ts

	for _, req := range group {
		op, frame := p.stage(req, pending, inflight, now, ts)
		if op.hasFrame() {
			frame.txnID = p.nextTxnID
			p.nextTxnID++
			buf, op.payloadOffs = appendFrame(buf, frame)
			op.payloadLens = make([]int32, len(frame.payloads))
			for i, pl := range frame.payloads {
				op.payloadLens[i] = int32(len(pl))
			}
			op.ts = ts
			inflight[req.streamID] = pending[req.streamID]
			last[req.streamID] = groupID
		} else if op.barrier {
			op.res.walSeq = p.wal.activeSeq
			op.res.walOff = p.wal.writePos
			op.res.nextTxn = p.nextTxnID
		}
		staged = append(staged, op)
	}

	batch.ops = staged
	if len(buf) > 0 {
		batch.writeAttempted = true
		batch.segSeq, batch.base, batch.segment, batch.writeErr = p.wal.writeGroup(buf)
	}
	batch.endSeq = p.wal.activeSeq
	batch.endOff = p.wal.writePos
	batch.endNextTxn = p.nextTxnID
	if cap(buf) <= maxRetainedEncodeBuf {
		p.encodeBuf = buf[:0]
	} else {
		p.encodeBuf = nil
	}
	return batch
}

// commitGroups serially establishes durability, publishes in FIFO order, and
// completes requests. One committer per partition is invariant I1/I3's owner.
func (p *partition) commitGroups(handoff <-chan *stagedGroup, retired chan<- retiredGroup) {
	for batch := range handoff {
		published := p.commitGroup(batch)
		retired <- retiredGroup{id: batch.id, published: published}
	}
}

func (p *partition) commitGroup(batch *stagedGroup) bool {
	err := batch.writeErr
	if err == nil {
		err = p.brokenErr()
	}
	if err == nil {
		err = p.wal.syncSegment(batch.segment)
	}
	if err != nil {
		broken := p.latchBroken(err)
		for _, op := range batch.ops {
			op.req.done <- result{err: broken, ambiguous: batch.writeAttempted && op.hasFrame()}
		}
		return false
	}

	for _, op := range batch.ops {
		p.publish(op, batch.segSeq, batch.base)
	}
	// Barriers are standalone queue cuts. Swap the exact dirty/removal
	// frontier only after every preceding FIFO group has published.
	for _, op := range batch.ops {
		if op.barrier && op.req.captureDirty {
			op.res.dirty, op.res.removals = p.swapDirty()
		}
	}
	p.publishedSeq = batch.endSeq
	p.publishedOff = batch.endOff
	p.publishedNextTx = batch.endNextTxn
	for _, op := range batch.ops {
		op.req.done <- op.res
	}
	return true
}

func (p *partition) brokenErr() error {
	p.brokenMu.Lock()
	defer p.brokenMu.Unlock()
	return p.broken
}

func (p *partition) latchBroken(err error) error {
	p.brokenMu.Lock()
	defer p.brokenMu.Unlock()
	if p.broken == nil {
		p.broken = fmt.Errorf("seglog: partition %d WAL failure: %w", p.id, err)
		p.st.opts.SLogger.Error("seglog partition failed, refusing further writes",
			"partition", p.id, "error", err)
	}
	return p.broken
}

// maxRetainedEncodeBuf caps the group buffer kept across groups so one huge
// append does not pin its allocation forever.
const maxRetainedEncodeBuf = 8 << 20

// publish applies one durable frame to the in-memory catalog and wakes
// waiters (invariant I3: only after the group fdatasync).
func (p *partition) publish(op *stagedOp, segSeq uint64, base int64) {
	switch {
	case op.barrier:
		// The stager captured the marker's frontier after every preceding
		// group write. FIFO arrival here proves those groups are durable and
		// published without reading staging cursors from this goroutine.
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
	case op.applyFork != nil:
		st := op.applyFork.newState
		for i, rel := range op.payloadOffs {
			batchFirst := st.parentBoundary + 1
			if int64(i) >= op.req.prefixCount {
				batchFirst += op.req.prefixCount
			}
			st.walTail = append(st.walTail, walLoc{
				segmentSeq: segSeq, off: base + rel, length: op.payloadLens[i],
				batchFirst: batchFirst, ts: op.ts,
			})
		}
		p.st.streams.Store(st.id, st)
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
		if a.soft {
			a.state.mu.Lock()
			a.state.softDeleted = true
			close(a.state.notifyCh)
			a.state.notifyCh = make(chan struct{})
			a.state.mu.Unlock()
			p.markDirty(a.state)
			break
		}
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

// loadPending resolves the current group's view from the cross-group overlay
// first. Only after the last touching group retires does it seed from the live
// catalog, taking RLock because the committer now owns logical publication.
func (p *partition) loadPending(pending, inflight map[string]*pendingStream, streamID string) *pendingStream {
	if ps, ok := pending[streamID]; ok {
		return ps
	}
	if ps, ok := inflight[streamID]; ok {
		pending[streamID] = ps
		return ps
	}
	ps := &pendingStream{}
	if state, ok := p.st.streams.Load(streamID); ok {
		state.mu.RLock()
		if !state.deleted {
			ps.state = state
			ps.exists = true
			ps.cfg = state.cfg
			ps.closed = state.closed
			ps.softDeleted = state.softDeleted
			ps.nextIndex = state.nextIndex
			ps.lastSeq = state.lastSeq
			ps.retention = state.retention
			ps.floor = state.floor
		}
		state.mu.RUnlock()
	}
	pending[streamID] = ps
	return ps
}

// stage validates one request against the overlay and, when it mutates the
// stream, prepares its frame spec and apply context.
func (p *partition) stage(req *request, pending, inflight map[string]*pendingStream, now time.Time, ts int64) (*stagedOp, frameSpec) {
	op := &stagedOp{req: req}
	if req.op == opBarrier {
		op.barrier = true // resolved in publish, after the group commits
		return op, frameSpec{}
	}
	ps := p.loadPending(pending, inflight, req.streamID)
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
	case opFork:
		return op, p.stageFork(op, req, ps, now, ts)
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
	ps.softDeleted = false
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
	if ps.softDeleted {
		op.res = result{err: softDeletedErr(req.streamID)}
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
	if ps.softDeleted && !req.hardCascade {
		op.res = result{err: softDeletedErr(req.streamID)}
		return frameSpec{}
	}
	if req.softDelete {
		op.applyDelete = &deleteApply{state: state, soft: true}
		ps.softDeleted = true
		return frameSpec{op: opDelete, flags: flagSoftDelete, streamID: req.streamID, inc: state.inc, ts: ts}
	}
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
	if ps.softDeleted {
		op.res = result{err: softDeletedErr(req.streamID)}
		return frameSpec{}
	}
	if req.returnInfo {
		op.res.info = &durablestream.StreamInfo{
			ContentType:   ps.cfg.ContentType,
			NextOffset:    storage.FormatSimpleOffset(ps.nextIndex - 1),
			TTL:           ps.cfg.TTL,
			ExpiresAt:     ps.cfg.ExpiresAt,
			IsPrivate:     ps.cfg.IsPrivate,
			Closed:        ps.closed,
			IncarnationID: ps.state.inc.String(),
		}
	}
	slid, moved := ps.cfg.SlideExpiry(now)
	if !moved {
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
	if ps.softDeleted {
		op.res = result{err: softDeletedErr(req.streamID)}
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
