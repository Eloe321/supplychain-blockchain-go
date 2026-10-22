package chain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"

	"supplychain/ledger"
)

var blocksBucket = []byte("blocks")
var metaBucket = []byte("meta")
var configKey = []byte("config")
var tipKey = []byte("tip")

// Open creates or verifies a database. Every stored block and branch is replayed
// from the exact genesis before the persisted canonical tip is trusted.
func Open(path string, cfg ledger.Config) (*Chain, error) {
	normalized, err := ledger.NormalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	genesis, err := ledger.Genesis(normalized)
	if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Chain, error) { _ = db.Close(); return nil, err }
	if err := os.Chmod(path, 0600); err != nil {
		return fail(err)
	}
	c := &Chain{
		db: db, cfg: normalized, genesis: genesis,
		gate: make(chan struct{}, 1), nodes: map[string]*node{},
		pending: []ledger.Event{}, pendingIDs: map[string]bool{},
	}
	configBytes, _ := json.Marshal(normalized)
	err = db.Update(func(tx *bolt.Tx) error {
		blocks := tx.Bucket(blocksBucket)
		meta := tx.Bucket(metaBucket)
		if blocks == nil && meta == nil {
			var err error
			blocks, err = tx.CreateBucket(blocksBucket)
			if err != nil {
				return err
			}
			meta, err = tx.CreateBucket(metaBucket)
			if err != nil {
				return err
			}
			encoded, _ := json.Marshal(genesis)
			if err := blocks.Put([]byte(genesis.Hash), encoded); err != nil {
				return err
			}
			if err := meta.Put(configKey, configBytes); err != nil {
				return err
			}
			return meta.Put(tipKey, []byte(genesis.Hash))
		}
		if blocks == nil || meta == nil {
			return errors.New("incomplete chain database")
		}
		var stored ledger.Config
		if err := json.Unmarshal(meta.Get(configKey), &stored); err != nil {
			return fmt.Errorf("stored config: %w", err)
		}
		canonicalConfig, err := ledger.NormalizeConfig(stored)
		if err != nil {
			return err
		}
		encoded, _ := json.Marshal(canonicalConfig)
		if !bytes.Equal(encoded, configBytes) || !bytes.Equal(meta.Get(configKey), encoded) {
			return errors.New("stored configuration does not match normalized configuration")
		}
		return nil
	})
	if err != nil {
		return fail(err)
	}
	var storedTip string
	var blocks []ledger.Block
	err = db.View(func(tx *bolt.Tx) error {
		storedTip = string(tx.Bucket(metaBucket).Get(tipKey))
		return tx.Bucket(blocksBucket).ForEach(func(key, value []byte) error {
			var b ledger.Block
			if err := json.Unmarshal(value, &b); err != nil {
				return fmt.Errorf("decode block: %w", err)
			}
			if string(key) != b.Hash {
				return errors.New("stored block key/hash mismatch")
			}
			blocks = append(blocks, b)
			return nil
		})
	})
	if err != nil {
		return fail(err)
	}
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Height != blocks[j].Height {
			return blocks[i].Height < blocks[j].Height
		}
		return blocks[i].Hash < blocks[j].Hash
	})
	for _, b := range blocks {
		var n *node
		if b.Height == 0 {
			if err := ledger.ValidateGenesis(normalized, b); err != nil {
				return fail(err)
			}
			if len(c.nodes) > 0 {
				return fail(errors.New("multiple genesis blocks"))
			}
			n = &node{block: b.Clone(), state: ledger.NewState(), work: new(big.Int)}
		} else {
			parent, ok := c.nodes[b.ParentHash]
			if !ok {
				return fail(errors.New("stored graph has an unknown parent"))
			}
			state, err := ledger.ValidateBlock(normalized, parent.block, b, parent.state)
			if err != nil {
				return fail(fmt.Errorf("stored block %s: %w", b.Hash, err))
			}
			n = &node{block: b.Clone(), state: state, work: new(big.Int).Add(parent.work, blockWork(normalized.Difficulty))}
		}
		c.nodes[b.Hash] = n
		if c.tip == nil || better(n, c.tip) {
			c.tip = n
		}
	}
	if _, ok := c.nodes[genesis.Hash]; !ok {
		return fail(errors.New("stored genesis is missing"))
	}
	if c.tip == nil || c.tip.block.Hash != storedTip {
		return fail(errors.New("stored canonical tip violates fork choice"))
	}
	c.state = c.tip.state
	c.path, c.confirmed = canonical(c.nodes, c.tip)
	c.pendingState = c.state.Clone()
	return c, nil
}

func (c *Chain) persistBlock(block ledger.Block, tip string) error {
	encoded, err := json.Marshal(block)
	if err != nil {
		return err
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(blocksBucket).Put([]byte(block.Hash), encoded); err != nil {
			return err
		}
		return tx.Bucket(metaBucket).Put(tipKey, []byte(tip))
	})
}
