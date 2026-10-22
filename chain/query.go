package chain

import (
	"errors"

	"supplychain/ledger"
)

// Status describes the canonical tip. Work is an arbitrary-precision decimal
// cumulative work value; genesis contributes zero and each block contributes 2^d.
type Status struct {
	ChainID     string `json:"chain_id"`
	GenesisHash string `json:"genesis_hash"`
	Height      uint64 `json:"height"`
	Hash        string `json:"hash"`
	Work        string `json:"work"`
	Pending     int    `json:"pending"`
}

// TraceEntry identifies one canonical event and its depth, counting its own block.
type TraceEntry struct {
	Event         ledger.Event `json:"event"`
	BlockHash     string       `json:"block_hash"`
	Height        uint64       `json:"height"`
	Confirmations uint64       `json:"confirmations"`
}

// Config returns normalized configuration with an independent participant slice.
func (c *Chain) Config() ledger.Config {
	c.mu.RLock()
	defer c.mu.RUnlock()
	cfg := c.cfg
	cfg.Participants = append([]ledger.Participant{}, cfg.Participants...)
	return cfg
}

// Genesis returns the immutable configuration-derived anchor as a copied value.
func (c *Chain) Genesis() ledger.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.genesis.Clone()
}

// Tip returns the last committed canonical block as a copied value.
func (c *Chain) Tip() ledger.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tip.block.Clone()
}
func (c *Chain) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Status{ChainID: c.cfg.ChainID, GenesisHash: c.genesis.Hash, Height: c.tip.block.Height, Hash: c.tip.block.Hash, Work: c.tip.work.String(), Pending: len(c.pending)}
}

// State returns a mutable copy of canonical state, excluding pending events.
func (c *Chain) State() *ledger.State {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.Clone()
}
func (c *Chain) Batch(id string) (ledger.Batch, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.GetBatch(id)
}
func (c *Chain) Batches() []ledger.Batch {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.state.AllBatches()
}

// GetBlock retrieves either a canonical or a validated side-branch block.
func (c *Chain) GetBlock(hash string) (ledger.Block, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return ledger.Block{}, false
	}
	n, ok := c.nodes[hash]
	if !ok {
		return ledger.Block{}, false
	}
	return n.block.Clone(), true
}
func (c *Chain) HasBlock(hash string) bool { _, ok := c.GetBlock(hash); return ok }

// CanonicalBlocks returns a genesis-first snapshot, including the anchor.
func (c *Chain) CanonicalBlocks() []ledger.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	blocks := make([]ledger.Block, 0, len(c.path))
	for _, h := range c.path {
		blocks = append(blocks, c.nodes[h].block.Clone())
	}
	return blocks
}

// Trace returns canonical batch history in block and event order.
func (c *Chain) Trace(batchID string) []TraceEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	trace := []TraceEntry{}
	for _, h := range c.path {
		b := c.nodes[h].block
		for _, e := range b.Events {
			if e.BatchID == batchID {
				trace = append(trace, TraceEntry{Event: e, BlockHash: b.Hash, Height: b.Height, Confirmations: c.tip.block.Height - b.Height + 1})
			}
		}
	}
	return trace
}

// Locators returns tip-first canonical hashes with exponential backoff and a
// final genesis hash. The caller may prepend a fetched cursor for continuation.
func (c *Chain) Locators() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	loc := []string{}
	index := len(c.path) - 1
	step := 1
	for index > 0 {
		loc = append(loc, c.path[index])
		if len(loc) > 10 {
			step *= 2
		}
		if step > index {
			index = 0
		} else {
			index -= step
		}
	}
	loc = append(loc, c.path[0])
	return loc, nil
}

// BlocksAfter finds the first supplied locator on this node's canonical branch
// and returns successors in parent-first order. Limits above 32 are capped.
func (c *Chain) BlocksAfter(locators []string, limit int) ([]ledger.Block, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	if limit <= 0 {
		return nil, errors.New("block limit must be positive")
	}
	if limit > MaxSyncBlocks {
		limit = MaxSyncBlocks
	}
	positions := make(map[string]int, len(c.path))
	for i, h := range c.path {
		positions[h] = i
	}
	for _, h := range locators {
		if index, ok := positions[h]; ok {
			end := index + 1 + limit
			if end > len(c.path) {
				end = len(c.path)
			}
			blocks := make([]ledger.Block, 0, end-index-1)
			for _, hash := range c.path[index+1 : end] {
				blocks = append(blocks, c.nodes[hash].block.Clone())
			}
			return blocks, nil
		}
	}
	return nil, ErrNoCommonAncestor
}
