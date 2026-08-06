package seglog

import "sync"

type checkpointBarrier struct {
	mu        sync.Mutex
	nextEpoch uint64
	tail      *checkpointFlight
}

type checkpointFlight struct {
	epoch       uint64
	predecessor *checkpointFlight
	once        sync.Once
	done        chan struct{}
	started     bool // guarded by checkpointBarrier.mu
	waiters     int  // guarded by checkpointBarrier.mu
	supported   bool
	err         error
}

// run coalesces filesystem barriers by request epoch. Requests made before an
// epoch starts share it. A request made while syncfs is already running joins
// the next epoch, because the active call may have passed that request's
// filesystem updates. sync.Once elects the epoch leader without a goroutine.
func (b *checkpointBarrier) run(syncFn func() (bool, error)) (supported, performed bool, err error) {
	b.mu.Lock()
	if b.tail == nil || b.tail.started {
		b.nextEpoch++
		b.tail = &checkpointFlight{
			epoch:       b.nextEpoch,
			predecessor: b.tail,
			done:        make(chan struct{}),
		}
	}
	flight := b.tail
	flight.waiters++
	b.mu.Unlock()

	flight.once.Do(func() {
		performed = true
		if predecessor := flight.predecessor; predecessor != nil {
			<-predecessor.done
			flight.predecessor = nil
		}
		b.mu.Lock()
		flight.started = true
		b.mu.Unlock()

		flight.supported, flight.err = syncFn()
		close(flight.done)
	})
	return flight.supported, performed, flight.err
}
