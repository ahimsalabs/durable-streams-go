package seglog

import (
	"sync"
	"sync/atomic"
	"time"
)

// commitGate aligns partition committers into device-wide flush waves. A
// mutex/condition gate is used instead of a gate goroutine: release decisions
// happen on the completing member's stack, with no worker lifecycle on the
// commit path.
//
// After a contended wave completes, the gate holds the next release open for a
// short boarding window (an eighth of the measured wave duration). Without it,
// a partition that just published its wave cannot re-arm and admit before the
// next wave releases, so every partition structurally rides every other wave
// and doubles its commit latency. An uncontended wave (solo, nobody queued)
// skips the window, so sequential appends keep their minimum latency.
type commitGate struct {
	mu   sync.Mutex
	cond *sync.Cond

	inFlight  bool
	boarding  bool
	wave      uint64
	remaining int
	size      int
	queued    int
	waveStart time.Time
	closed    bool

	completed atomic.Int64
}

type commitAdmission struct {
	gate *commitGate
	wave uint64
}

const (
	minBoardingPause = 200 * time.Microsecond
	maxBoardingPause = 2 * time.Millisecond
)

func newCommitGate() *commitGate {
	gate := &commitGate{}
	gate.cond = sync.NewCond(&gate.mu)
	return gate
}

func (g *commitGate) admit() (commitAdmission, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return commitAdmission{}, ErrClosed
	}
	if !g.inFlight && !g.boarding {
		g.inFlight = true
		g.wave++
		g.remaining = 1
		g.size = 1
		g.waveStart = time.Now()
		return commitAdmission{gate: g, wave: g.wave}, nil
	}

	target := g.wave + 1
	g.queued++
	for g.wave < target {
		g.cond.Wait()
	}
	return commitAdmission{gate: g, wave: target}, nil
}

func (a commitAdmission) complete() {
	g := a.gate
	g.mu.Lock()
	g.remaining--
	if g.remaining == 0 {
		g.completed.Add(1)
		g.inFlight = false
		if g.size == 1 && g.queued == 0 {
			// Uncontended wave: stay closed-and-idle, the next admit releases
			// itself immediately.
			g.mu.Unlock()
			return
		}
		pause := min(max(time.Since(g.waveStart)/8, minBoardingPause), maxBoardingPause)
		g.boarding = true
		time.AfterFunc(pause, g.releaseBoarding)
	}
	g.mu.Unlock()
}

// releaseBoarding closes the boarding window and releases everything queued as
// the next wave. With nothing queued the gate simply returns to idle.
func (g *commitGate) releaseBoarding() {
	g.mu.Lock()
	g.boarding = false
	if g.queued > 0 {
		g.inFlight = true
		g.wave++
		g.remaining = g.queued
		g.size = g.queued
		g.queued = 0
		g.waveStart = time.Now()
		g.cond.Broadcast()
	}
	g.mu.Unlock()
}

// close rejects future admissions. Storage calls it only after partition
// workers drain, so every admitted cohort has already completed and a stray
// boarding timer finds nothing queued.
func (g *commitGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}
