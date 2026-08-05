package badgerstore

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
	"github.com/ahimsalabs/durable-streams-go/durablestream/storage"
	"github.com/dgraph-io/badger/v4"
)

// The defaults keep both admission memory and each attempted Badger
// transaction bounded. A single logical AppendBatch may exceed these grouping
// bounds; it is tried alone so Badger can either commit it atomically or return
// ErrTxnTooBig, which the storage API maps to ErrPayloadTooLarge.
const (
	defaultAppendCommitMaxRequests = 256
	defaultAppendCommitMaxEntries  = 16 * 1024
	defaultAppendCommitMaxBytes    = 4 * 1024 * 1024
	defaultAppendCommitMaxWait     = 200 * time.Microsecond
)

type appendCommitConfig struct {
	maxRequests int
	maxEntries  int
	maxBytes    int
	maxWait     time.Duration
}

func defaultAppendCommitConfig() appendCommitConfig {
	return appendCommitConfig{
		maxRequests: defaultAppendCommitMaxRequests,
		maxEntries:  defaultAppendCommitMaxEntries,
		maxBytes:    defaultAppendCommitMaxBytes,
		maxWait:     defaultAppendCommitMaxWait,
	}
}

type appendCommitRequest struct {
	streamID   string
	gen        generation
	messages   [][]byte
	seq        string
	close      bool
	entries    int
	bytes      int
	completion chan appendCommitResult
}

type appendCommitResult struct {
	offset durablestream.Offset
	err    error
}

func newAppendCommitRequest(
	streamID string,
	gen generation,
	messages [][]byte,
	seq string,
	closeStream bool,
) *appendCommitRequest {
	// Each message creates one data entry, and every non-empty logical batch
	// creates one boundary entry. Sequence and closure add at most one each.
	entries := len(messages)
	if len(messages) > 0 {
		entries += 2 // Batch boundary and offset high-water.
	}
	if seq != "" {
		entries++
	}
	if closeStream {
		entries++
	}

	// This need not reproduce Badger's internal accounting exactly: it is an
	// admission bound, while ErrTxnTooBig remains the authoritative safeguard.
	// Include key overhead so many long stream IDs cannot evade the byte bound.
	bytes := len(seq) + len(streamID) + len(gen) + 128
	for _, message := range messages {
		bytes += len(message) + len(streamID) + len(gen) + 96
	}

	return &appendCommitRequest{
		streamID:   streamID,
		gen:        gen,
		messages:   messages,
		seq:        seq,
		close:      closeStream,
		entries:    entries,
		bytes:      bytes,
		completion: make(chan appendCommitResult, 1),
	}
}

// appendCommitter owns the durable append queue. Admission is synchronized
// with close so no request can be sent after the worker exits. The queue and
// every physical transaction are both bounded; callers provide the remaining
// backpressure by blocking in submit while retaining their own request data.
type appendCommitter struct {
	storage *Storage
	config  appendCommitConfig
	queue   chan *appendCommitRequest

	admissionMu sync.Mutex
	accepting   bool
	stop        chan struct{}
	senders     sync.WaitGroup
	done        chan struct{}

	// White-box counters make the batching property testable without exposing
	// backend tuning or metrics as public API.
	transactionAttempts atomic.Uint64
	requestAttempts     atomic.Uint64
}

func newAppendCommitter(storage *Storage, config appendCommitConfig) *appendCommitter {
	if config.maxRequests <= 0 {
		config.maxRequests = 1
	}
	if config.maxEntries <= 0 {
		config.maxEntries = 1
	}
	if config.maxBytes <= 0 {
		config.maxBytes = 1
	}
	if config.maxWait < 0 {
		config.maxWait = 0
	}
	return &appendCommitter{
		storage:   storage,
		config:    config,
		queue:     make(chan *appendCommitRequest, config.maxRequests),
		accepting: true,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

func (c *appendCommitter) submit(request *appendCommitRequest) appendCommitResult {
	// Register the sender while admission is locked so close cannot begin its
	// Wait until every possible Add has finished. The potentially blocking send
	// happens after unlocking and is interruptible by stop.
	c.admissionMu.Lock()
	if !c.accepting {
		c.admissionMu.Unlock()
		return appendCommitResult{err: ErrClosed}
	}
	c.senders.Add(1)
	c.admissionMu.Unlock()

	select {
	case c.queue <- request:
		c.senders.Done()
	case <-c.stop:
		c.senders.Done()
		return appendCommitResult{err: ErrClosed}
	}

	// Once admitted, the call remains live until its transaction has either
	// committed or failed. Returning early on context cancellation would make
	// it unsafe for the caller to reuse the borrowed message slices.
	return <-request.completion
}

func (c *appendCommitter) close() {
	c.admissionMu.Lock()
	if !c.accepting {
		c.admissionMu.Unlock()
		return
	}
	c.accepting = false
	close(c.stop)
	c.admissionMu.Unlock()

	// No sender can call Add after accepting becomes false. Once existing
	// senders have either queued or observed stop, closing queue is safe.
	c.senders.Wait()
	close(c.queue)
}

func (c *appendCommitter) run() {
	defer close(c.done)
	var carry *appendCommitRequest
	for {
		first := carry
		if first == nil {
			var ok bool
			first, ok = <-c.queue
			if !ok {
				return
			}
		}

		batch, queueClosed, next := c.collect(first)
		c.commit(batch)
		if queueClosed {
			return
		}
		carry = next
	}
}

// collect waits at most maxWait from the first request and reports whether it
// drained a closed queue. If a received request would cross a bound, collect
// returns that one request as the next group's first member; it never splits a
// logical AppendBatch.
func (c *appendCommitter) collect(first *appendCommitRequest) (
	batch []*appendCommitRequest,
	queueClosed bool,
	carry *appendCommitRequest,
) {
	batch = []*appendCommitRequest{first}
	entries, bytes := first.entries, first.bytes
	if len(batch) >= c.config.maxRequests ||
		entries >= c.config.maxEntries || bytes >= c.config.maxBytes {
		return batch, false, nil
	}

	var timer *time.Timer
	var timerC <-chan time.Time
	if c.config.maxWait > 0 {
		timer = time.NewTimer(c.config.maxWait)
		timerC = timer.C
		defer stopTimer(timer)
	}

	for len(batch) < c.config.maxRequests {
		var (
			request *appendCommitRequest
			ok      bool
		)
		if timerC == nil {
			select {
			case request, ok = <-c.queue:
			default:
				return batch, false, nil
			}
		} else {
			select {
			case request, ok = <-c.queue:
			case <-timerC:
				return batch, false, nil
			}
		}
		if !ok {
			return batch, true, nil
		}
		if entries > c.config.maxEntries-request.entries ||
			bytes > c.config.maxBytes-request.bytes {
			return batch, false, request
		}

		batch = append(batch, request)
		entries += request.entries
		bytes += request.bytes
		if entries >= c.config.maxEntries || bytes >= c.config.maxBytes {
			return batch, false, nil
		}
	}
	return batch, false, nil
}

func (c *appendCommitter) commit(requests []*appendCommitRequest) {
	if len(requests) == 0 {
		return
	}
	c.transactionAttempts.Add(1)
	c.requestAttempts.Add(uint64(len(requests)))

	results, err := c.storage.commitAppendRequests(requests)
	if err != nil {
		// A transaction-wide size or conflict error says nothing about which
		// independent request caused it. Bisecting preserves logical request
		// atomicity, lets unrelated streams proceed, and isolates the error at
		// a single request without guessing.
		if len(requests) > 1 && (errors.Is(err, badger.ErrTxnTooBig) || errors.Is(err, badger.ErrConflict)) {
			middle := len(requests) / 2
			c.commit(requests[:middle])
			c.commit(requests[middle:])
			return
		}
		if errors.Is(err, badger.ErrConflict) {
			err = fmt.Errorf("badgerstore: concurrent write conflict: %w", durablestream.ErrConflict)
		}
		for _, request := range requests {
			request.completion <- appendCommitResult{err: err}
		}
		return
	}

	for i, request := range requests {
		request.completion <- results[i]
	}
}

type appendEntry struct {
	key   []byte
	value []byte
}

type preparedAppend struct {
	entries []appendEntry
	offset  durablestream.Offset
}

// commitAppendRequests validates every independent request against one Badger
// snapshot, then writes all valid mutations in one transaction. Semantic
// failure of one stream is recorded for that request and does not poison the
// others. No Set occurs until all validation and encoding has completed, so a
// per-request error can never leave half of that request in the shared txn.
func (s *Storage) commitAppendRequests(requests []*appendCommitRequest) ([]appendCommitResult, error) {
	results := make([]appendCommitResult, len(requests))
	err := s.update(func(txn *badger.Txn) error {
		prepared := make([]preparedAppend, len(requests))
		for i, request := range requests {
			mutation, err := s.prepareAppend(txn, request)
			if err != nil {
				results[i].err = err
				continue
			}
			prepared[i] = mutation
			results[i].offset = mutation.offset
		}

		for i := range prepared {
			if results[i].err != nil {
				continue
			}
			for _, entry := range prepared[i].entries {
				if err := txn.Set(entry.key, entry.value); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Storage) prepareAppend(txn *badger.Txn, request *appendCommitRequest) (preparedAppend, error) {
	rec, found, err := getRecord(txn, request.streamID)
	if err != nil {
		return preparedAppend{}, err
	}
	if !found {
		return preparedAppend{}, durablestream.ErrNotFound
	}
	if rec.Gen != request.gen {
		return preparedAppend{}, errGenerationChanged
	}
	if err := directRecordError(rec); err != nil {
		return preparedAppend{}, err
	}
	if rec.Closed {
		if request.close && len(request.messages) == 0 {
			offset, err := getTailOffset(txn, request.streamID, rec.Gen)
			if err != nil {
				return preparedAppend{}, fmt.Errorf("badgerstore: get tail offset: %w", err)
			}
			return preparedAppend{offset: offset}, nil
		}
		return preparedAppend{}, durablestream.ErrStreamClosed
	}

	entries := make([]appendEntry, 0, request.entries)
	if request.seq != "" {
		lastSeq, err := s.getLastSeq(txn, request.streamID)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return preparedAppend{}, fmt.Errorf("badgerstore: get last seq: %w", err)
		}
		if lastSeq != "" && request.seq <= lastSeq {
			return preparedAppend{}, fmt.Errorf("badgerstore: sequence regression: %w", durablestream.ErrConflict)
		}
		entries = append(entries, appendEntry{key: lastSeqKey(request.streamID), value: []byte(request.seq)})
	}

	offsets, highWater, err := nextAppendOffsets(txn, request.streamID, request.gen, len(request.messages))
	if err != nil {
		return preparedAppend{}, err
	}
	for i, message := range request.messages {
		entries = append(entries, appendEntry{
			key:   messageKey(request.streamID, request.gen, offsets[i]),
			value: message,
		})
	}
	if len(offsets) > 0 {
		entries = append(entries, appendEntry{
			key:   batchKey(request.streamID, request.gen, offsets[0]),
			value: []byte(offsets[len(offsets)-1]),
		})
		var encodedHighWater [8]byte
		binary.BigEndian.PutUint64(encodedHighWater[:], uint64(highWater))
		entries = append(entries, appendEntry{
			key:   seqKey(request.streamID, request.gen),
			value: encodedHighWater[:],
		})
	}

	if request.close {
		rec.Closed = true
		encoded, err := json.Marshal(rec)
		if err != nil {
			return preparedAppend{}, fmt.Errorf("badgerstore: marshal config: %w", err)
		}
		entries = append(entries, appendEntry{key: configKey(request.streamID), value: encoded})
	}

	if len(offsets) > 0 {
		return preparedAppend{entries: entries, offset: offsets[len(offsets)-1]}, nil
	}
	offset, err := getTailOffset(txn, request.streamID, rec.Gen)
	if err != nil {
		return preparedAppend{}, fmt.Errorf("badgerstore: get tail offset: %w", err)
	}
	return preparedAppend{entries: entries, offset: offset}, nil
}

// nextAppendOffsets treats the generation-scoped sequence key as a persisted
// high-water mark. Values written by the previous Badger Sequence allocator
// may be ahead of the visible tail because of leasing; starting after that
// value preserves the permitted gap and can never overwrite an old message.
// New values are advanced atomically with their messages in the grouped txn.
func nextAppendOffsets(
	txn *badger.Txn,
	streamID string,
	gen generation,
	count int,
) ([]durablestream.Offset, int64, error) {
	if count == 0 {
		return nil, 0, nil
	}

	var highWater uint64
	item, err := txn.Get(seqKey(streamID, gen))
	if errors.Is(err, badger.ErrKeyNotFound) {
		// Empty regular streams historically omitted the sequence key. Forks and
		// streams created with initial content initialize it, but deriving the
		// tail here also makes a missing key safe to repair.
		tail, tailErr := getTailOffset(txn, streamID, gen)
		if tailErr != nil {
			return nil, 0, fmt.Errorf("badgerstore: get tail for offset allocation: %w", tailErr)
		}
		position, parseErr := parsePersistedOffset(tail)
		if parseErr != nil {
			return nil, 0, parseErr
		}
		highWater = uint64(position)
	} else if err != nil {
		return nil, 0, fmt.Errorf("badgerstore: get offset high-water: %w", err)
	} else if err := item.Value(func(value []byte) error {
		if len(value) != 8 {
			return fmt.Errorf("badgerstore: invalid offset high-water length %d", len(value))
		}
		highWater = binary.BigEndian.Uint64(value)
		return nil
	}); err != nil {
		return nil, 0, fmt.Errorf("badgerstore: read offset high-water: %w", err)
	}

	if highWater > math.MaxInt64 || uint64(count) > uint64(math.MaxInt64)-highWater {
		return nil, 0, fmt.Errorf("badgerstore: offset space exhausted")
	}
	first := int64(highWater) + 1
	offsets := make([]durablestream.Offset, count)
	for i := range offsets {
		offsets[i] = storage.FormatSimpleOffset(first + int64(i))
	}
	return offsets, first + int64(count) - 1, nil
}
