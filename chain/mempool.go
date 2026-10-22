package chain

import (
	"sort"

	"supplychain/ledger"
)

func eventIDs(events []ledger.Event) map[string]bool {
	ids := make(map[string]bool, len(events))
	for _, e := range events {
		ids[e.ID] = true
	}
	return ids
}

// Submit checks an event against canonical state plus every accepted pending
// predecessor. Pending events are not persisted and must be resubmitted on restart.
func (c *Chain) Submit(e ledger.Event) error {
	c.gate <- struct{}{}
	defer func() { <-c.gate }()
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ErrClosed
	}
	if c.confirmed[e.ID] || c.pendingIDs[e.ID] {
		c.mu.RUnlock()
		return ErrKnownEvent
	}
	if len(c.pending) >= MaxPending {
		c.mu.RUnlock()
		return ErrMempoolFull
	}
	state := c.pendingState.Clone()
	cfg := c.cfg
	c.mu.RUnlock()
	if err := state.Apply(cfg, e); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, e)
	c.pendingIDs[e.ID] = true
	c.pendingState = state
	return nil
}

// Pending returns independent values in valid application order; nil after Close.
func (c *Chain) Pending() []ledger.Event {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil
	}
	return append([]ledger.Event{}, c.pending...)
}

// revalidate deterministically retries dependencies until no progress remains.
// Each successful application is atomic; invalid/conflicting candidates simply
// fail to enter the pool. This also recovers detached events after a reorg.
func revalidate(cfg ledger.Config, base *ledger.State, confirmed map[string]bool, candidates []ledger.Event) ([]ledger.Event, *ledger.State) {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Sequence != candidates[j].Sequence {
			return candidates[i].Sequence < candidates[j].Sequence
		}
		return candidates[i].ID < candidates[j].ID
	})
	state := base.Clone()
	pool := []ledger.Event{}
	seen := map[string]bool{}
	remaining := []ledger.Event{}
	for _, e := range candidates {
		if !confirmed[e.ID] && !seen[e.ID] {
			seen[e.ID] = true
			remaining = append(remaining, e)
		}
	}
	for len(remaining) > 0 && len(pool) < MaxPending {
		deferred := []ledger.Event{}
		progress := false
		for _, e := range remaining {
			if len(pool) == MaxPending {
				break
			}
			if err := state.Apply(cfg, e); err != nil {
				deferred = append(deferred, e)
				continue
			}
			pool = append(pool, e)
			progress = true
		}
		if !progress {
			break
		}
		remaining = deferred
	}
	return pool, state
}
