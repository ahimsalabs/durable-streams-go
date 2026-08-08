package seglog

import (
	"container/heap"
	"context"
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
	// group contains the append mutations in one explicit producer durability
	// group. A group request does not use the mutation fields below directly.
	group        []*request
	op           opKind
	streamID     string
	returnInfo   bool                       // opTouch: return the pre-renewal Head result
	cfg          durablestream.StreamConfig // opCreate
	policy       SegmentPolicy
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
	walSeq         uint64
	walOff         int64
	nextTxn        uint64
	dirty          map[*streamState]struct{}
	dirtySnapshots map[*streamState]readSnapshot
	removals       []*streamState
	frontier       statsFrontier
}

// opBarrier is a queue-only operation (never a WAL frame): its result carries
// the WAL position at which every earlier submission has been committed and
// published. The materializer uses it to bound checkpoint advancement.
const opBarrier opKind = 0xff

// partition is one WAL partition and its bounded stager/committer worker
// (invariants I5/I6).
type partition struct {
	id    uint32
	st    *Storage
	wal   *walWriter
	stats partitionStats

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

	// walPending is the contiguous-written watermark. The stager appends only
	// after writeGroup returns; the committer swaps the complete prefix before
	// fdatasync. pendingSpace coalesces capacity notifications.
	walPendingMu sync.Mutex
	walPending   []*stagedRecord
	pendingSpace chan struct{}

	// broken is latched after the first FIFO WAL failure and read by the stager
	// to stop further encoding. published* is publisher-owned while run is
	// active and read after run exits by finalMaterialize.
	brokenMu        sync.Mutex
	broken          error
	publishedSeq    uint64
	publishedOff    int64
	publishedNextTx uint64

	// Materializer coordination. The worker adds under dirtyMu at publish
	// time; the materializer swaps the containers each round.
	dirtyMu        sync.Mutex
	materializerMu sync.Mutex
	dirty          map[*streamState]struct{}
	removals       []*streamState // dead incarnations awaiting directory removal
	// materializeWake is a coalescing doorbell. Publication never blocks when
	// the materializer is already awake or a signal is pending.
	materializeWake chan struct{}
	// ageMu protects the scheduler's deadline heap. Publication registers a
	// generation after it releases the stream lock. ageEntries keeps at most
	// one heap entry for each stream and removes replaced generations eagerly.
	ageMu        sync.Mutex
	ageDeadlines activeDeadlineHeap
	ageEntries   map[*streamState]*activeDeadline

	// Materializer-owned: the last durably written checkpoint position.
	ckptSeq       uint64
	ckptOff       int64
	ckptState     []byte
	segmentWrites segmentWriteBuffer
	// materializedEntries is the cumulative image including materialization
	// rounds published since the checkpoint became durable. Materializer-owned.
	materializedEntries map[string]streamCheckpointEntry
	// uncheckpointedSince is the time of the first successful materialization
	// not covered by a checkpoint. Additional rounds and failures preserve it.
	uncheckpointedSince time.Time
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
	sealedSidecars             map[string]struct{}
	cleanupPaths               map[string]struct{}
	cleanupDirs                map[string]struct{}
	walReclaimBefore           uint64
	checkpointWriteHook        func() error // one-shot test seam; guarded by checkpointHookMu
	checkpointHookMu           sync.Mutex
	materializationBarrierHook func() // one-shot test seam; guarded by barrierHookMu
	barrierHookMu              sync.Mutex
}

func newPartition(id uint32, st *Storage, wal *walWriter) *partition {
	return &partition{
		id:                  id,
		st:                  st,
		wal:                 wal,
		queue:               make(chan *request, st.opts.QueueDepth),
		accepting:           true,
		stop:                make(chan struct{}),
		pendingSpace:        make(chan struct{}, 1),
		materializeWake:     make(chan struct{}, 1),
		ageEntries:          make(map[*streamState]*activeDeadline),
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

type activeDeadline struct {
	st              *streamState
	createdUnixNano int64
	deadline        time.Time
	index           int
}

type activeDeadlineHeap []*activeDeadline

func (h activeDeadlineHeap) Len() int           { return len(h) }
func (h activeDeadlineHeap) Less(i, j int) bool { return h[i].deadline.Before(h[j].deadline) }
func (h activeDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *activeDeadlineHeap) Push(x any) {
	entry := x.(*activeDeadline)
	entry.index = len(*h)
	*h = append(*h, entry)
}

func (h *activeDeadlineHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	x.index = -1
	*h = old[:n-1]
	return x
}

func (p *partition) registerActiveDeadline(st *streamState, created int64, maxAge time.Duration, nonempty bool) {
	p.ageMu.Lock()
	current := p.ageEntries[st]
	if created == 0 || maxAge == 0 || !nonempty {
		if current != nil {
			heap.Remove(&p.ageDeadlines, current.index)
			delete(p.ageEntries, st)
		}
		p.ageMu.Unlock()
		return
	}
	deadline := time.Unix(0, created).Add(maxAge)
	if current == nil || current.createdUnixNano != created || current.deadline != deadline {
		if current != nil {
			heap.Remove(&p.ageDeadlines, current.index)
		}
		entry := &activeDeadline{st: st, createdUnixNano: created, deadline: deadline}
		p.ageEntries[st] = entry
		heap.Push(&p.ageDeadlines, entry)
	}
	p.ageMu.Unlock()
	p.wakeMaterializer()
}

func (p *partition) unregisterActiveDeadline(st *streamState) {
	p.ageMu.Lock()
	if current := p.ageEntries[st]; current != nil {
		heap.Remove(&p.ageDeadlines, current.index)
		delete(p.ageEntries, st)
	}
	p.ageMu.Unlock()
}

// markDirty records that a stream's state changed since the materializer's
// last round.
func (p *partition) markDirty(st *streamState) {
	p.dirtyMu.Lock()
	p.dirty[st] = struct{}{}
	p.dirtyMu.Unlock()
	p.wakeMaterializer()
}

// markRemoval queues a dead incarnation's directory for removal.
func (p *partition) markRemoval(st *streamState) {
	p.unregisterActiveDeadline(st)
	p.dirtyMu.Lock()
	p.removals = append(p.removals, st)
	p.dirtyMu.Unlock()
	p.wakeMaterializer()
}

func (p *partition) wakeMaterializer() {
	select {
	case p.materializeWake <- struct{}{}:
	default:
	}
}

func (p *partition) hasRemovals() bool {
	p.dirtyMu.Lock()
	defer p.dirtyMu.Unlock()
	return len(p.removals) > 0
}

func (p *partition) takeCheckpointWriteHook() func() error {
	p.checkpointHookMu.Lock()
	defer p.checkpointHookMu.Unlock()
	hook := p.checkpointWriteHook
	p.checkpointWriteHook = nil
	return hook
}

func (p *partition) takeMaterializationBarrierHook() func() {
	p.barrierHookMu.Lock()
	defer p.barrierHookMu.Unlock()
	hook := p.materializationBarrierHook
	p.materializationBarrierHook = nil
	return hook
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
// mutate them mid-encode. WAL write and fdatasync latency bound the wait.
func (p *partition) submit(req *request) result {
	return p.submitContext(context.Background(), req)
}

// submitContext applies cancellation only while waiting for queue admission.
// After admission it waits for completion because the request borrows payloads.
func (p *partition) submitContext(ctx context.Context, req *request) result {
	if err := ctx.Err(); err != nil {
		return result{err: err}
	}
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
	case <-ctx.Done():
		p.senders.Done()
		return result{err: ctx.Err()}
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

// stagedRecord is one request's write-at-arrival result. The stager does not
// mutate it after adding it to the pending watermark.
type stagedRecord struct {
	id uint64
	op *stagedOp
	// ops has one outcome for each explicit producer-group mutation. Ordinary
	// requests continue to use op.
	ops []*stagedOp
	// groupDone completes the wrapper queue item after all per-operation
	// results have been delivered.
	groupDone chan result

	segment *os.File
	segSeq  uint64
	base    int64

	endSeq     uint64
	endOff     int64
	endNextTxn uint64

	writeAttempted bool
	writeErr       error
}

func (r *stagedRecord) stagedOps() []*stagedOp {
	if r.ops != nil {
		return r.ops
	}
	return []*stagedOp{r.op}
}

func (r *stagedRecord) hasFrame() bool {
	for _, op := range r.stagedOps() {
		if op.hasFrame() {
			return true
		}
	}
	return false
}

func (r *stagedRecord) firstFrameTS() int64 {
	for _, op := range r.stagedOps() {
		if op.hasFrame() {
			return op.ts
		}
	}
	return 0
}

type retiredRecord struct {
	id        uint64
	published bool
}

// partitionStager owns the in-flight validation overlay and serial WAL writer.
type partitionStager struct {
	p        *partition
	retired  <-chan retiredRecord
	inflight map[string]*pendingStream
	last     map[string]uint64

	nextRecordID uint64
	outstanding  int
	writeStopped bool
}

// run owns all three pipeline stages. It starts exactly one committer and one
// FIFO publisher, drains the admission queue, consumes every retirement, and
// joins both goroutines before returning.
func (p *partition) run() {
	p.publishedSeq = p.wal.activeSeq
	p.publishedOff = p.wal.writePos
	p.publishedNextTx = p.nextTxnID
	retired := make(chan retiredRecord, 1)
	// wakeup is a coalescing doorbell: the stager rings it after appending to
	// the pending list; the committer collects everything pending per snapshot.
	wakeup := make(chan struct{}, 1)
	// Capacity two is the bounded overlap contract: the committer may run one
	// snapshot ahead of acknowledgements, then its blocking send applies
	// backpressure instead of accumulating durable-but-unpublished work.
	publish := make(chan publishSet, 2)
	committerDone := make(chan struct{})
	publisherDone := make(chan struct{})
	// run owns and joins both goroutines. The committer terminates after
	// wakeup closes and the pending list drains, then closes publish; the
	// publisher terminates after every synced snapshot has been published or
	// failed.
	go func() {
		p.commitSnapshots(wakeup, publish)
		close(publish)
		close(committerDone)
	}()
	go func() {
		p.publishSets(publish, retired)
		close(publisherDone)
	}()
	stager := partitionStager{
		p: p, retired: retired,
		inflight: make(map[string]*pendingStream), last: make(map[string]uint64),
		nextRecordID: 1,
	}

	// Write-at-arrival: every request's frame reaches the WAL immediately, so
	// the very next partition sync covers it. There is no group formation wait;
	// batching is whatever accumulates in the pending list while a sync is in
	// flight (the watermark pipeline, after the rust server's WAL shards).
	for {
		req, ok := stager.nextRequest()
		if !ok {
			break
		}
		stager.waitForPendingSpace()
		_, dependsOnInflight := stager.inflight[req.streamID]
		if len(req.group) > 0 {
			for _, grouped := range req.group {
				if _, ok := stager.inflight[grouped.streamID]; ok {
					dependsOnInflight = true
					break
				}
			}
		}
		record := p.stageRecord(stager.nextRecordID, req, stager.inflight, stager.last, stager.writeStopped)
		stager.nextRecordID++
		if !record.hasFrame() && record.op != nil && !record.op.barrier && record.writeErr == nil && !dependsOnInflight {
			// Independent validation failures and no-ops complete inline. A
			// no-op validated against speculative overlay state must remain a
			// FIFO marker so a preceding durability failure reaches it.
			record.op.req.done <- record.op.res
			continue
		}
		if record.writeErr != nil || p.brokenErr() != nil {
			stager.writeStopped = true
		}
		p.appendPending(record)
		stager.outstanding++
		select {
		case wakeup <- struct{}{}:
		default:
		}
	}
	close(wakeup)
	for stager.outstanding > 0 {
		stager.retire(<-retired)
	}
	<-committerDone
	<-publisherDone
}

// appendPending queues one written (or failed, or barrier-only) staged record
// for the next partition sync. waitForPendingSpace enforces an explicit 4*QueueDepth
// bound; concurrent submitters can otherwise refill the admission queue while
// the stager drains it.
func (p *partition) appendPending(record *stagedRecord) {
	if record.segment != nil && record.writeErr == nil {
		p.stats.addPendingWALBytes(record.endOff - record.base)
	}
	p.walPendingMu.Lock()
	p.walPending = append(p.walPending, record)
	p.walPendingMu.Unlock()
}

// swapPending takes the current pending list. Called by the committer at snapshot
// release: every swapped record's bytes were written before this instant, so
// the fdatasync that follows covers all of them (the watermark snapshot).
func (p *partition) swapPending() []*stagedRecord {
	p.walPendingMu.Lock()
	batches := p.walPending
	p.walPending = nil
	p.walPendingMu.Unlock()
	if len(batches) > 0 {
		select {
		case p.pendingSpace <- struct{}{}:
		default:
		}
	}
	return batches
}

// takeImmediatePending atomically takes a snapshot only when it has no WAL
// bytes that need syncing. A frame appended after the unlock remains pending
// for its own wakeup and cannot be published by this call.
func (p *partition) takeImmediatePending() []*stagedRecord {
	p.walPendingMu.Lock()
	for _, record := range p.walPending {
		if record.segment != nil && record.writeErr == nil && p.wal.sync {
			p.walPendingMu.Unlock()
			return nil
		}
	}
	batches := p.walPending
	p.walPending = nil
	p.walPendingMu.Unlock()
	if len(batches) > 0 {
		select {
		case p.pendingSpace <- struct{}{}:
		default:
		}
	}
	return batches
}

func (p *partition) hasPending() bool {
	p.walPendingMu.Lock()
	hasPending := len(p.walPending) > 0
	p.walPendingMu.Unlock()
	return hasPending
}

// Guaranteed upper bounds on encoded JSON meta documents. createMetaBound
// covers every fixed field (timestamps in RFC 3339 with nanoseconds); the
// content type is bounded separately at 6x for worst-case \uXXXX escaping.
const (
	segmentPolicyMetaJSONBound = `,"segmentPolicy":{"targetBytes":-9223372036854775808,"maxOpenAgeNanos":9223372036854775807}`
	segmentPolicyMetaBound     = len(segmentPolicyMetaJSONBound)
	createMetaBound            = 288 + segmentPolicyMetaBound
)

// metaBoundForCreate bounds an opCreate frame's meta document.
func metaBoundForCreate(contentType string) int {
	return createMetaBound + 6*len(contentType)
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

func (s *partitionStager) waitForPendingSpace() {
	limit := 4 * s.p.st.opts.QueueDepth
	for {
		s.p.walPendingMu.Lock()
		hasSpace := len(s.p.walPending) < limit
		s.p.walPendingMu.Unlock()
		if hasSpace {
			return
		}
		select {
		case ack := <-s.retired:
			s.retire(ack)
		case <-s.p.pendingSpace:
		}
	}
}

func (s *partitionStager) retire(ack retiredRecord) {
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
	seqOffset  durablestream.Offset
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

	cfg           durablestream.StreamConfig
	closed        bool
	softDeleted   bool
	nextIndex     int64
	lastSeq       string
	lastSeqOffset durablestream.Offset
	retention     Retention
	floor         int64
}

// stageRecord validates and encodes one request against the in-flight overlay,
// then writes its frame without syncing. Publication remains publisher-only.
func (p *partition) stageRecord(recordID uint64, req *request, inflight map[string]*pendingStream, last map[string]uint64, writeStopped bool) *stagedRecord {
	record := &stagedRecord{id: recordID, endSeq: p.wal.activeSeq, endOff: p.wal.writePos, endNextTxn: p.nextTxnID}
	if len(req.group) > 0 {
		record.groupDone = req.done
	}
	if writeStopped || p.brokenErr() != nil {
		record.writeErr = p.brokenErr()
		if len(req.group) > 0 {
			record.ops = make([]*stagedOp, len(req.group))
			for i, mutation := range req.group {
				record.ops[i] = &stagedOp{req: mutation}
			}
		} else {
			record.op = &stagedOp{req: req}
		}
		return record
	}
	buf := p.encodeBuf[:0]
	pending := make(map[string]*pendingStream)
	now := time.Now()
	requests := []*request{req}
	if len(req.group) > 0 {
		requests = req.group
		record.ops = make([]*stagedOp, 0, len(requests))
	}
	for _, mutation := range requests {
		ts := max(now.UnixNano(), p.lastTS) // clamp against wall-clock regression
		p.lastTS = ts
		op, frame := p.stage(mutation, pending, inflight, now, ts)
		if op.hasFrame() {
			frame.txnID = p.nextTxnID
			p.nextTxnID++
			buf, op.payloadOffs = appendFrame(buf, frame)
			op.payloadLens = make([]int32, len(frame.payloads))
			for i, pl := range frame.payloads {
				op.payloadLens[i] = int32(len(pl))
			}
			op.ts = ts
			inflight[mutation.streamID] = pending[mutation.streamID]
			last[mutation.streamID] = recordID
		} else if op.barrier {
			op.res.walSeq = p.wal.activeSeq
			op.res.walOff = p.wal.writePos
			op.res.nextTxn = p.nextTxnID
		}
		if record.ops != nil {
			record.ops = append(record.ops, op)
		} else {
			record.op = op
		}
	}
	if len(buf) > 0 {
		record.writeAttempted = true
		record.segSeq, record.base, record.segment, record.writeErr = p.wal.writeGroup(buf)
		if record.writeErr != nil {
			record.writeErr = p.latchBroken(record.writeErr)
		}
	}
	record.endSeq = p.wal.activeSeq
	record.endOff = p.wal.writePos
	record.endNextTxn = p.nextTxnID
	if cap(buf) <= maxRetainedEncodeBuf {
		p.encodeBuf = buf[:0]
	} else {
		p.encodeBuf = nil
	}
	return record
}

// publishSet is one partition snapshot's records with its shared sync outcome,
// handed from the committer to the publisher.
type publishSet struct {
	batches []*stagedRecord
	syncErr error
}

// commitSnapshots establishes durability for the pending list, one partition
// snapshot at a time. It parks on the doorbell, acquires a storage-wide sync
// slot, and swaps the pending list — the watermark snapshot: every swapped
// record's bytes reached the WAL before this instant, so the fdatasync that
// follows covers all of them. Records written during the sync remain for the
// next snapshot. The committer owns I1's durability boundary; the publisher
// owns I3's state transition.
func (p *partition) commitSnapshots(wakeup <-chan struct{}, publish chan<- publishSet) {
	for {
		idleStart := time.Now()
		_, open := <-wakeup
		p.stats.committerIdleNanos.Add(time.Since(idleStart).Nanoseconds())
		if !open {
			// Shutdown: the stager appended everything before closing the
			// doorbell; establish one final snapshot for what remains.
			if batches := p.takeImmediatePending(); len(batches) > 0 {
				publish <- publishSet{batches: batches}
				return
			}
			if batches := p.swapPending(); len(batches) > 0 {
				admission, err := p.st.syncLimiter.admit()
				if err == nil {
					err = p.syncPending(batches)
					admission.complete()
				}
				publish <- publishSet{batches: batches, syncErr: err}
			}
			return
		}
		if batches := p.takeImmediatePending(); len(batches) > 0 {
			// Sync-off and marker-only snapshots publish without limiter
			// admission. Failed writes carry their own error to the publisher.
			publish <- publishSet{batches: batches}
			continue
		}
		if !p.hasPending() {
			// A coalesced token can describe records already captured by the
			// preceding snapshot. Do not acquire a slot for an empty snapshot.
			continue
		}
		admission, err := p.st.syncLimiter.admit()
		if err != nil {
			if batches := p.swapPending(); len(batches) > 0 {
				publish <- publishSet{batches: batches, syncErr: err}
			}
			continue
		}
		batches := p.swapPending()
		syncErr := p.syncPending(batches)
		// Completion is unconditional: a failed fdatasync must release its
		// storage-wide concurrency slot.
		admission.complete()
		if syncErr != nil {
			// Latch before handoff so the stager observes the failed
			// durability boundary even before publication runs.
			_ = p.latchBroken(syncErr)
		}
		if len(batches) > 0 {
			publish <- publishSet{batches: batches, syncErr: syncErr}
		}
	}
}

// publishSets is the partition's single publisher. It fails or publishes each
// snapshot's records in FIFO order and retires them to the stager, all strictly
// after that snapshot's fdatasync outcome (invariant I1 rides the syncErr, not
// goroutine identity).
func (p *partition) publishSets(publish <-chan publishSet, retired chan<- retiredRecord) {
	var priorErr error
	for set := range publish {
		start := time.Now()
		var snapshotOps int64
		setErr := set.syncErr
		if setErr == nil {
			setErr = priorErr
		}
		for _, record := range set.batches {
			published := p.commitRecord(record, setErr)
			if published && record.hasFrame() {
				for _, op := range record.stagedOps() {
					if op.hasFrame() {
						snapshotOps++
					}
				}
				p.stats.walBytesWritten.Add(record.endOff - record.base)
			}
			if !published && priorErr == nil {
				// Carry failures forward in publisher order. Do not reread the
				// global broken state for successful earlier sets: a later
				// committer failure may already have latched it.
				priorErr = p.brokenErr()
				setErr = priorErr
			}
			retired <- retiredRecord{id: record.id, published: published}
		}
		if snapshotOps > 0 {
			p.stats.opsCommitted.Add(snapshotOps)
			p.stats.groupSizeHist[groupSizeBucket(snapshotOps)].Add(1)
		}
		p.stats.publishNanos.Add(time.Since(start).Nanoseconds())
	}
}

// syncPending fdatasyncs each distinct WAL segment among the snapshot in
// write order. Barrier-only and failed-write records carry no durable bytes
// and are skipped.
func (p *partition) syncPending(batches []*stagedRecord) error {
	if !p.wal.sync {
		return nil
	}
	if err := p.brokenErr(); err != nil {
		// A prior sync failure is latched; do not issue more fdatasyncs for a
		// partition that refuses writes.
		return err
	}
	syncStart := time.Now()
	var synced *os.File
	var err error
	for _, b := range batches {
		if b.segment == nil || b.writeErr != nil || b.segment == synced {
			continue
		}
		if err = p.wal.syncSegment(b.segment); err != nil {
			break
		}
		synced = b.segment
	}
	p.stats.commitFdatasyncNanos.Add(time.Since(syncStart).Nanoseconds())
	return err
}

func (p *partition) commitRecord(record *stagedRecord, syncErr error) bool {
	frameBytes := int64(0)
	if record.segment != nil && record.writeErr == nil {
		frameBytes = record.endOff - record.base
	}
	err := record.writeErr
	if err == nil {
		err = syncErr
	}
	if err != nil {
		p.stats.discardPendingWALBytes(frameBytes)
		broken := p.latchBroken(err)
		for _, op := range record.stagedOps() {
			res := result{err: broken}
			if record.ops != nil && record.writeAttempted && !op.hasFrame() {
				// Preserve definitive state-dependent validation outcomes for
				// frame-less siblings in an attempted explicit group write.
				res = op.res
			} else if op.hasFrame() {
				res = result{err: broken, ambiguous: record.writeAttempted}
			}
			op.req.done <- res
		}
		if record.groupDone != nil {
			record.groupDone <- result{}
		}
		return false
	}
	for _, op := range record.stagedOps() {
		p.publish(op, record.segSeq, record.base)
	}
	if frameBytes > 0 {
		p.stats.publishWALFrame(frameBytes, record.firstFrameTS())
		// publish can ring the dirty doorbell before the frontier counters are
		// advanced. Ring it again after both become visible so a fast consumer
		// cannot lose the transition that crosses a pressure threshold.
		p.wakeMaterializer()
	}
	// A barrier is a queue marker. Swap the exact dirty/removal frontier only
	// after every preceding FIFO record has published.
	if record.op != nil && record.op.barrier && record.op.req.captureDirty {
		record.op.res.frontier = p.stats.captureMaterializationFrontier()
		record.op.res.dirty, record.op.res.removals = p.swapDirty()
		record.op.res.dirtySnapshots = make(map[*streamState]readSnapshot, len(record.op.res.dirty))
		for st := range record.op.res.dirty {
			record.op.res.dirtySnapshots[st] = st.materializationSnapshot()
		}
	}
	p.publishedSeq = record.endSeq
	p.publishedOff = record.endOff
	p.publishedNextTx = record.endNextTxn
	for _, op := range record.stagedOps() {
		op.req.done <- op.res
	}
	if record.groupDone != nil {
		record.groupDone <- result{}
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

// maxRetainedEncodeBuf caps the frame buffer kept across records so one huge
// append does not pin its allocation forever.
const maxRetainedEncodeBuf = 8 << 20

// publish applies one durable frame to the in-memory catalog and wakes
// waiters (invariant I3: only after the covering fdatasync).
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
			st.lastSeqOffset = a.seqOffset
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
			ps.lastSeqOffset = state.lastSeqOffset
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
	ContentType string             `json:"contentType,omitempty"`
	TTLNanos    int64              `json:"ttlNanos,omitempty"`
	ExpiresAt   time.Time          `json:"expiresAt,omitzero"`
	IsPrivate   bool               `json:"isPrivate,omitempty"`
	Closed      bool               `json:"closed,omitempty"`
	Retention   *retentionMeta     `json:"retention,omitempty"`
	Policy      *segmentPolicyMeta `json:"segmentPolicy"`
}

type segmentPolicyMeta struct {
	TargetBytes     int64 `json:"targetBytes"`
	MaxOpenAgeNanos int64 `json:"maxOpenAgeNanos"`
}

func policyMeta(p SegmentPolicy) *segmentPolicyMeta {
	return &segmentPolicyMeta{TargetBytes: p.TargetBytes, MaxOpenAgeNanos: int64(p.MaxOpenAge)}
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
		if ps.cfg.Matches(req.cfg) && ps.state.policy == req.policy {
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
		Policy: policyMeta(req.policy),
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
	newState.policy = req.policy
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
	ps.lastSeqOffset = ""
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
	if !ps.exists {
		op.res = result{err: fmt.Errorf("seglog: stream %q not found: %w", req.streamID, durablestream.ErrNotFound)}
		return frameSpec{}
	}
	if ps.softDeleted || (expired && ps.state.refCount.Load() != 0) {
		op.res = result{err: softDeletedErr(req.streamID)}
		return frameSpec{}
	}
	if expired {
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
		conflict := &durablestream.SequenceConflictError{LastSeq: ps.lastSeq, LastOffset: ps.lastSeqOffset}
		op.res = result{err: fmt.Errorf("seglog: sequence %q does not advance past %q: %w", req.seq, ps.lastSeq, conflict)}
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

	seqOffset := storage.FormatSimpleOffset(ps.nextIndex + int64(len(req.messages)) - 1)
	op.applyAppend = &appendApply{
		state:      ps.state,
		firstIndex: firstIndex,
		count:      int64(len(req.messages)),
		seq:        req.seq,
		seqOffset:  seqOffset,
		hasSeq:     req.hasSeq,
		close:      req.close,
	}

	ps.nextIndex += int64(len(req.messages))
	if req.hasSeq {
		ps.lastSeq = req.seq
		ps.lastSeqOffset = seqOffset
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
			LastSeq:       ps.lastSeq,
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
