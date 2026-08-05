package seglog

import (
	"context"
	"time"

	"github.com/ahimsalabs/durable-streams-go/durablestream"
)

// WaitForData implements durablestream.Storage. The waiter pins the stream
// incarnation it starts with: a delete followed by a fast recreate releases
// it with ErrNotFound instead of feeding it the replacement's data.
func (s *Storage) WaitForData(ctx context.Context, streamID string, offset durablestream.Offset, limit int) (*durablestream.ReadResult, error) {
	if err := s.checkClosed(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStreamID(streamID); err != nil {
		return nil, err
	}
	pos, err := parseReadOffset(offset, limit)
	if err != nil {
		return nil, err
	}
	state, ok := s.streams.Load(streamID)
	if !ok {
		return nil, notFoundErr(streamID)
	}

	for {
		// Capture the notification channel before reading: any commit that
		// lands after the read closes this exact channel, so no wakeup is
		// lost between the read and the select below.
		state.mu.RLock()
		deleted := state.deleted
		softDeleted := state.softDeleted || (state.cfg.IsExpired() && state.refCount.Load() != 0)
		notifyCh := state.notifyCh
		state.mu.RUnlock()
		if deleted {
			return nil, notFoundErr(streamID)
		}
		if softDeleted {
			return nil, softDeletedErr(streamID)
		}

		res, err := s.readState(ctx, state, pos, limit)
		if err != nil {
			return nil, err
		}
		if len(res.Messages) > 0 || res.Closed {
			return res, nil
		}

		// Expiry must release a waiter even when no append or delete ever
		// arrives. The loop re-checks, so the stored deadline stays
		// authoritative; Touch wakes waiters to re-arm the timer.
		state.mu.RLock()
		expiresAt := state.cfg.ExpiresAt
		state.mu.RUnlock()
		var timer *time.Timer
		var expiryCh <-chan time.Time
		if !expiresAt.IsZero() {
			timer = time.NewTimer(time.Until(expiresAt))
			expiryCh = timer.C
		}

		select {
		case <-ctx.Done():
			stopTimer(timer)
			return nil, ctx.Err()
		case <-s.shutdownCh:
			stopTimer(timer)
			return nil, ErrClosed
		case <-notifyCh:
			stopTimer(timer)
		case <-expiryCh:
		}
	}
}
