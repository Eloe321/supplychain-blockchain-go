// Package chain maintains a validated, persistent block graph and its canonical
// supply-chain state. Consensus rules are implemented by package ledger.
package chain

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"supplychain/ledger"
)

var (
	ErrClosed           = errors.New("chain is closed")
	ErrKnownBlock       = errors.New("block is already known")
	ErrUnknownParent    = errors.New("block parent is unknown")
	ErrKnownEvent       = errors.New("event is already pending or confirmed")
	ErrMempoolFull      = errors.New("mempool is full")
	ErrNoCommonAncestor = errors.New("no locator belongs to the canonical chain")
)

const MaxPending = 4096
const MaxSyncBlocks = 32

type node struct {
	block ledger.Block
	state *ledger.State
	work  *big.Int
}

// Chain serializes writers with a gate while its one mutex protects snapshots.
// Mining and database transactions never hold the snapshot mutex.
type Chain struct {
	mu           sync.RWMutex
	gate         chan struct{}
	db           *bolt.DB
	cfg          ledger.Config
	genesis      ledger.Block
	nodes        map[string]*node
	tip          *node
	path         []string
	state        *ledger.State
	confirmed    map[string]bool
	pending      []ledger.Event
	pendingIDs   map[string]bool
	pendingState *ledger.State
	closed       bool
}

// Close is idempotent and waits for any committing writer before closing storage.
// Snapshot accessors retain their last committed values after Close. Pending
// returns nil and block lookups return false; operations returning errors return
// ErrClosed. Mining already in progress rechecks closure at insertion.
func (c *Chain) Close() error {
	c.gate <- struct{}{}
	defer func() { <-c.gate }()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	return c.db.Close()
}

func blockWork(d uint8) *big.Int { return new(big.Int).Lsh(big.NewInt(1), uint(d)) }
func better(a, b *node) bool {
	cmp := a.work.Cmp(b.work)
	return cmp > 0 || (cmp == 0 && a.block.Hash < b.block.Hash)
}

func canonical(nodes map[string]*node, tip *node) ([]string, map[string]bool) {
	reversed := []string{}
	confirmed := map[string]bool{}
	for n := tip; ; n = nodes[n.block.ParentHash] {
		reversed = append(reversed, n.block.Hash)
		for _, e := range n.block.Events {
			confirmed[e.ID] = true
		}
		if n.block.Height == 0 {
			break
		}
	}
	path := make([]string, len(reversed))
	for i, h := range reversed {
		path[len(path)-1-i] = h
	}
	return path, confirmed
}

// AddBlock validates against the candidate's own parent state, stores side
// branches, and atomically changes the canonical tip if the fork wins.
func (c *Chain) AddBlock(block ledger.Block) (bool, error) {
	c.gate <- struct{}{}
	defer func() { <-c.gate }()
	block = block.Clone()
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return false, ErrClosed
	}
	if _, ok := c.nodes[block.Hash]; ok {
		c.mu.RUnlock()
		return false, ErrKnownBlock
	}
	parent, ok := c.nodes[block.ParentHash]
	if !ok {
		c.mu.RUnlock()
		return false, ErrUnknownParent
	}
	cfg := c.cfg
	oldTip := c.tip
	pending := append([]ledger.Event(nil), c.pending...)
	c.mu.RUnlock()
	state, err := ledger.ValidateBlock(cfg, parent.block, block, parent.state)
	if err != nil {
		return false, err
	}
	candidate := &node{block: block, state: state, work: new(big.Int).Add(parent.work, blockWork(cfg.Difficulty))}
	changed := better(candidate, oldTip)
	var path []string
	var confirmed map[string]bool
	var pool []ledger.Event
	var poolState *ledger.State
	if changed {
		// The candidate need not be published: its ancestors are already in the
		// stable graph, and canonical starts from the supplied candidate node.
		path, confirmed = canonical(c.nodes, candidate)
		for _, h := range c.path {
			for _, e := range c.nodes[h].block.Events {
				if !confirmed[e.ID] {
					pending = append(pending, e)
				}
			}
		}
		pool, poolState = revalidate(cfg, state, confirmed, pending)
	}
	tipHash := oldTip.block.Hash
	if changed {
		tipHash = block.Hash
	}
	if err := c.persistBlock(block, tipHash); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[block.Hash] = candidate
	if changed {
		c.tip = candidate
		c.state = state
		c.path = path
		c.confirmed = confirmed
		c.pending = pool
		c.pendingState = poolState
		c.pendingIDs = eventIDs(pool)
	}
	return changed, nil
}

// Mine takes a bounded pending snapshot and performs proof of work outside the
// mutex and writer gate. AddBlock revalidates and stores the result afterwards.
func (c *Chain) Mine(ctx context.Context) (ledger.Block, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return ledger.Block{}, ErrClosed
	}
	cfg := c.cfg
	parent := c.tip.block.Clone()
	state := c.state.Clone()
	count := len(c.pending)
	if count > ledger.MaxBlockEvents {
		count = ledger.MaxBlockEvents
	}
	events := append([]ledger.Event{}, c.pending[:count]...)
	c.mu.RUnlock()
	timestamp := time.Now().Unix()
	if timestamp < parent.Timestamp {
		timestamp = parent.Timestamp
	}
	events, err := miningPrefix(cfg, parent, events, timestamp)
	if err != nil {
		return ledger.Block{}, err
	}
	block, err := ledger.Mine(ctx, cfg, parent, state, events, timestamp)
	if err != nil {
		return ledger.Block{}, err
	}
	if _, err = c.AddBlock(block); err != nil {
		return ledger.Block{}, err
	}
	return block.Clone(), nil
}

// miningPrefix keeps dependency order while reserving the exact encoded header
// and worst-case nonce width. JSON escaping counts toward the consensus limit.
func miningPrefix(cfg ledger.Config, parent ledger.Block, events []ledger.Event, timestamp int64) ([]ledger.Event, error) {
	placeholder := strings.Repeat("0", 64)
	header := ledger.Block{
		Version: ledger.BlockVersion, ChainID: cfg.ChainID, Height: parent.Height + 1,
		ParentHash: parent.Hash, Timestamp: timestamp, Difficulty: cfg.Difficulty,
		Nonce: math.MaxUint64, MerkleRoot: placeholder, Hash: placeholder,
		Events: []ledger.Event{},
	}
	encoded, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	size := len(encoded)
	count := 0
	for _, event := range events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		added := len(encoded)
		if count > 0 {
			added++ // Separator between successive array elements.
		}
		if size+added > ledger.MaxBlockBytes {
			break
		}
		size += added
		count++
	}
	return events[:count], nil
}
