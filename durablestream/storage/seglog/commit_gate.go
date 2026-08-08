package seglog

import (
	"sync"
	"sync/atomic"
)

// syncLimiter bounds concurrent per-partition WAL fdatasyncs across a Storage.
// Some devices pipeline concurrent syncs and lose throughput when committers
// are aligned into device-wide waves. Flush-serializing devices can set the
// limit to one, while pipelining devices can keep several syncs in flight.
//
// A condition variable keeps close atomic with admission: close rejects new
// admissions and wakes blocked callers without waiting for current holders.
type syncLimiter struct {
	mu   sync.Mutex
	cond *sync.Cond

	limit    int
	inFlight int
	closed   bool

	completed atomic.Int64
}

type syncAdmission struct {
	limiter *syncLimiter
}

func newSyncLimiter(limit int) *syncLimiter {
	limiter := &syncLimiter{limit: limit}
	limiter.cond = sync.NewCond(&limiter.mu)
	return limiter
}

func (l *syncLimiter) admit() (syncAdmission, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for l.inFlight >= l.limit && !l.closed {
		l.cond.Wait()
	}
	if l.closed {
		return syncAdmission{}, ErrClosed
	}
	l.inFlight++
	return syncAdmission{limiter: l}, nil
}

func (a syncAdmission) complete() {
	l := a.limiter
	l.mu.Lock()
	l.inFlight--
	l.completed.Add(1)
	l.cond.Signal()
	l.mu.Unlock()
}

// close rejects future admissions and wakes blocked callers. It does not wait
// for holders to complete.
func (l *syncLimiter) close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}
