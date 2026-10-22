package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"
)

const (
	BlockVersion     = 1
	GenesisTimestamp = int64(1_700_000_000)
	MaxBlockEvents   = 256
	MaxBlockBytes    = 1024 * 1024
)

// Block commits an ordered list of signed events through a Merkle root. Its
// header commits every scalar field except Hash itself. Events are immutable
// values; Clone copies the sole mutable reference, the Events slice.
type Block struct {
	Version    int     `json:"version"`
	ChainID    string  `json:"chain_id"`
	Height     uint64  `json:"height"`
	ParentHash string  `json:"parent_hash"`
	Timestamp  int64   `json:"timestamp"`
	Difficulty uint8   `json:"difficulty"`
	Nonce      uint64  `json:"nonce"`
	MerkleRoot string  `json:"merkle_root"`
	Events     []Event `json:"events"`
	Hash       string  `json:"hash"`
}

type blockHeader struct {
	Domain     string `json:"domain"`
	Version    int    `json:"version"`
	ChainID    string `json:"chain_id"`
	Height     uint64 `json:"height"`
	ParentHash string `json:"parent_hash"`
	Timestamp  int64  `json:"timestamp"`
	Difficulty uint8  `json:"difficulty"`
	Nonce      uint64 `json:"nonce"`
	MerkleRoot string `json:"merkle_root"`
}

// Clone returns a block whose Events slice is independent of the original.
func (b Block) Clone() Block {
	copy := b
	copy.Events = append(make([]Event, 0, len(b.Events)), b.Events...)
	return copy
}

// BlockHash returns SHA-256 of the domain-separated fixed-field JSON header.
// It does not validate the proof, Merkle root, or state transitions.
func BlockHash(b Block) (string, error) {
	if !utf8.ValidString(b.ChainID) || !utf8.ValidString(b.ParentHash) || !utf8.ValidString(b.MerkleRoot) {
		return "", errors.New("block header contains invalid UTF-8")
	}
	encoded, err := json.Marshal(blockHeader{
		Domain: "supplychain/block/v1", Version: b.Version, ChainID: b.ChainID,
		Height: b.Height, ParentHash: b.ParentHash, Timestamp: b.Timestamp,
		Difficulty: b.Difficulty, Nonce: b.Nonce, MerkleRoot: b.MerkleRoot,
	})
	if err != nil {
		return "", err
	}
	return hashHex(encoded), nil
}

// MerkleRoot hashes full fixed-field event JSON: leaves use prefix 0x00 and
// internal nodes use prefix 0x01 followed by two raw 32-byte hashes. Odd nodes
// are duplicated at every level. The empty tree is SHA-256 of byte 0x02.
func MerkleRoot(events []Event) (string, error) {
	if len(events) > MaxBlockEvents {
		return "", fmt.Errorf("block exceeds %d events", MaxBlockEvents)
	}
	if len(events) == 0 {
		return hashHex([]byte{2}), nil
	}
	hashes := make([][sha256.Size]byte, len(events))
	for i, event := range events {
		if _, err := EventPayload(event); err != nil {
			return "", err
		}
		if !utf8.ValidString(event.ID) || !utf8.ValidString(event.Signature) {
			return "", errors.New("event identity or signature contains invalid UTF-8")
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return "", err
		}
		if len(encoded) > MaxEventBytes {
			return "", fmt.Errorf("event exceeds %d bytes", MaxEventBytes)
		}
		hashes[i] = sha256.Sum256(append([]byte{0}, encoded...))
	}
	for len(hashes) > 1 {
		level := make([][sha256.Size]byte, (len(hashes)+1)/2)
		for i := 0; i < len(hashes); i += 2 {
			j := i + 1
			if j == len(hashes) {
				j = i
			}
			var input [1 + 2*sha256.Size]byte
			input[0] = 1
			copy(input[1:], hashes[i][:])
			copy(input[1+sha256.Size:], hashes[j][:])
			level[i/2] = sha256.Sum256(input[:])
		}
		hashes = level
	}
	return hex.EncodeToString(hashes[0][:]), nil
}

// Genesis derives the deterministic, unmined chain anchor. ParentHash is the
// normalized configuration hash, committing the actor registry and difficulty.
// Nonce is always zero; only blocks of height >= 1 require proof of work.
func Genesis(cfg Config) (Block, error) {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return Block{}, err
	}
	configHash, err := ConfigHash(normalized)
	if err != nil {
		return Block{}, err
	}
	root, _ := MerkleRoot(nil)
	genesis := Block{
		Version: BlockVersion, ChainID: normalized.ChainID, Height: 0,
		ParentHash: configHash, Timestamp: GenesisTimestamp,
		Difficulty: normalized.Difficulty, Nonce: 0, MerkleRoot: root,
		Events: []Event{},
	}
	genesis.Hash, err = BlockHash(genesis)
	return genesis, err
}

// ValidateGenesis requires exact equality with the configuration-derived anchor
// (including an explicit empty event array). Its hash need not meet PoW.
func ValidateGenesis(cfg Config, candidate Block) error {
	want, err := Genesis(cfg)
	if err != nil {
		return err
	}
	if candidate.Version != want.Version || candidate.ChainID != want.ChainID || candidate.Height != 0 ||
		candidate.ParentHash != want.ParentHash || candidate.Timestamp != want.Timestamp || candidate.Difficulty != want.Difficulty ||
		candidate.Nonce != 0 || candidate.MerkleRoot != want.MerkleRoot || candidate.Hash != want.Hash ||
		candidate.Events == nil || len(candidate.Events) != 0 {
		return errors.New("genesis does not equal the configuration-derived anchor")
	}
	return nil
}

// MeetsDifficulty checks the configured number of leading zero bits, rather
// than hexadecimal zero characters. Invalid hashes or difficulties return false.
func MeetsDifficulty(hash string, difficulty uint8) bool {
	if difficulty < MinDifficulty || difficulty > MaxDifficulty || !canonicalHex(hash, sha256.Size) {
		return false
	}
	decoded, _ := hex.DecodeString(hash)
	whole := int(difficulty / 8)
	for _, b := range decoded[:whole] {
		if b != 0 {
			return false
		}
	}
	remaining := difficulty % 8
	return remaining == 0 || decoded[whole]>>(8-remaining) == 0
}

// Mine validates an ordered event snapshot and searches the next block's nonce
// without mutating or locking external state. timestamp must be at least the
// parent timestamp; height and parent links define order. Context cancellation
// is checked before work and on every nonce.
// The parent and parentState must come from the same already validated branch.
func Mine(ctx context.Context, cfg Config, parent Block, parentState *State, events []Event, timestamp int64) (Block, error) {
	if ctx == nil {
		return Block{}, errors.New("nil mining context")
	}
	if err := ctx.Err(); err != nil {
		return Block{}, err
	}
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return Block{}, err
	}
	if err := validateParent(normalized, parent, parentState); err != nil {
		return Block{}, err
	}
	if timestamp < parent.Timestamp {
		return Block{}, errors.New("block timestamp must not decrease")
	}
	if len(events) > MaxBlockEvents {
		return Block{}, fmt.Errorf("block exceeds %d events", MaxBlockEvents)
	}
	root, err := MerkleRoot(events)
	if err != nil {
		return Block{}, err
	}
	block := Block{
		Version: BlockVersion, ChainID: normalized.ChainID, Height: parent.Height + 1,
		ParentHash: parent.Hash, Timestamp: timestamp, Difficulty: normalized.Difficulty,
		MerkleRoot: root, Events: append(make([]Event, 0, len(events)), events...),
		Hash: parent.Hash, // Canonical 64-character placeholder for size accounting.
	}
	// A 20-digit nonce gives an upper bound on the final encoded size.
	block.Nonce = math.MaxUint64
	if err := blockSize(block); err != nil {
		return Block{}, err
	}
	state := parentState.Clone()
	for i, event := range block.Events {
		if err := ctx.Err(); err != nil {
			return Block{}, err
		}
		if err := state.Apply(normalized, event); err != nil {
			return Block{}, fmt.Errorf("event %d: %w", i, err)
		}
	}
	for block.Nonce = 0; ; block.Nonce++ {
		if err := ctx.Err(); err != nil {
			return Block{}, err
		}
		block.Hash, err = BlockHash(block)
		if err != nil {
			return Block{}, err
		}
		if MeetsDifficulty(block.Hash, block.Difficulty) {
			return block, nil
		}
		if block.Nonce == math.MaxUint64 {
			return Block{}, errors.New("nonce space exhausted")
		}
	}
}

// ValidateBlock checks a non-genesis child and returns a newly allocated state.
// It validates proof, all headers, Merkle commitment and each sequential action;
// errors never change parentState. The parent must already be a validated member
// of the branch represented by parentState.
func ValidateBlock(cfg Config, parent Block, candidate Block, parentState *State) (*State, error) {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	if err := validateParent(normalized, parent, parentState); err != nil {
		return nil, err
	}
	if candidate.Version != BlockVersion || candidate.ChainID != normalized.ChainID || candidate.Height != parent.Height+1 || candidate.ParentHash != parent.Hash {
		return nil, errors.New("block version, chain, height, or parent does not match")
	}
	if candidate.Timestamp < parent.Timestamp || candidate.Difficulty != normalized.Difficulty {
		return nil, errors.New("block timestamp or fixed difficulty is invalid")
	}
	if !canonicalHex(candidate.Hash, sha256.Size) || !canonicalHex(candidate.MerkleRoot, sha256.Size) {
		return nil, errors.New("block hash and Merkle root must be canonical lowercase hexadecimal")
	}
	if len(candidate.Events) > MaxBlockEvents {
		return nil, fmt.Errorf("block exceeds %d events", MaxBlockEvents)
	}
	if err := blockSize(candidate); err != nil {
		return nil, err
	}
	wantHash, err := BlockHash(candidate)
	if err != nil {
		return nil, err
	}
	if candidate.Hash != wantHash || !MeetsDifficulty(candidate.Hash, normalized.Difficulty) {
		return nil, errors.New("block header hash or proof of work is invalid")
	}
	root, err := MerkleRoot(candidate.Events)
	if err != nil {
		return nil, err
	}
	if candidate.MerkleRoot != root {
		return nil, errors.New("Merkle root does not match ordered events")
	}
	state := parentState.Clone()
	for i, event := range candidate.Events {
		if err := state.Apply(normalized, event); err != nil {
			return nil, fmt.Errorf("event %d: %w", i, err)
		}
	}
	return state, nil
}

func validateParent(cfg Config, parent Block, state *State) error {
	if state == nil {
		return errors.New("nil parent state")
	}
	if parent.Height == math.MaxUint64 {
		return errors.New("parent height cannot be incremented")
	}
	if parent.Height == 0 {
		if err := ValidateGenesis(cfg, parent); err != nil {
			return err
		}
		if len(state.Batches) != 0 {
			return errors.New("genesis state must be empty")
		}
		return nil
	}
	if parent.Version != BlockVersion || parent.ChainID != cfg.ChainID || parent.Difficulty != cfg.Difficulty || parent.Timestamp <= 0 ||
		!canonicalHex(parent.ParentHash, sha256.Size) || !canonicalHex(parent.MerkleRoot, sha256.Size) {
		return errors.New("invalid parent header")
	}
	hash, err := BlockHash(parent)
	if err != nil || hash != parent.Hash || !MeetsDifficulty(parent.Hash, cfg.Difficulty) {
		return errors.New("invalid parent hash or proof")
	}
	return nil
}

func blockSize(block Block) error {
	encoded, err := json.Marshal(block)
	if err != nil {
		return err
	}
	if len(encoded) > MaxBlockBytes {
		return fmt.Errorf("block exceeds %d bytes", MaxBlockBytes)
	}
	return nil
}
