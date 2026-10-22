package ledger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGenesisIsDeterministicAndCommitsConfiguration(t *testing.T) {
	cfg, _ := testConfig(t)
	first, err := Genesis(cfg)
	if err != nil {
		t.Fatal(err)
	}
	configHash, _ := ConfigHash(cfg)
	if first.Height != 0 || first.Timestamp != GenesisTimestamp || first.ParentHash != configHash || first.Nonce != 0 || len(first.Events) != 0 || first.Difficulty != cfg.Difficulty {
		t.Fatalf("incorrect genesis anchor: %+v", first)
	}
	for i, j := 0, len(cfg.Participants)-1; i < j; i, j = i+1, j-1 {
		cfg.Participants[i], cfg.Participants[j] = cfg.Participants[j], cfg.Participants[i]
	}
	second, err := Genesis(cfg)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("participant ordering changed genesis: %v", err)
	}
	if err := ValidateGenesis(cfg, first); err != nil {
		t.Fatal(err)
	}
	changed := first
	changed.Timestamp++
	if err := ValidateGenesis(cfg, changed); err == nil {
		t.Fatal("accepted a modified genesis")
	}
	cfg.Participants[0].Name += " changed"
	third, _ := Genesis(cfg)
	if first.Hash == third.Hash {
		t.Fatal("genesis failed to commit registry metadata")
	}
	cfg.Difficulty = MaxDifficulty
	highDifficulty, err := Genesis(cfg)
	if err != nil || highDifficulty.Nonce != 0 {
		t.Fatalf("genesis should be an unmined fixed anchor: %v", err)
	}
}

func TestMerkleRootDuplicatesOddNodesAtEveryLevel(t *testing.T) {
	events := make([]Event, 5)
	for i := range events {
		events[i].BatchID = strconv.Itoa(i)
	}
	for _, count := range []int{0, 1, 2, 3, 5} {
		leaves := make([][]byte, count)
		for i := 0; i < count; i++ {
			encoded, err := json.Marshal(events[i])
			if err != nil {
				t.Fatal(err)
			}
			hash := sha256.Sum256(append([]byte{0}, encoded...))
			leaves[i] = hash[:]
		}
		if count == 0 {
			hash := sha256.Sum256([]byte{2})
			leaves = [][]byte{hash[:]}
		}
		for len(leaves) > 1 {
			var level [][]byte
			for i := 0; i < len(leaves); i += 2 {
				j := i + 1
				if j == len(leaves) {
					j = i
				}
				input := append([]byte{1}, leaves[i]...)
				input = append(input, leaves[j]...)
				hash := sha256.Sum256(input)
				level = append(level, hash[:])
			}
			leaves = level
		}
		want := hex.EncodeToString(leaves[0])
		got, err := MerkleRoot(events[:count])
		if err != nil || got != want {
			t.Fatalf("Merkle root for %d events = %s, want %s: %v", count, got, want, err)
		}
	}
	root, _ := MerkleRoot(events)
	events[0], events[1] = events[1], events[0]
	changed, _ := MerkleRoot(events)
	if root == changed {
		t.Fatal("Merkle root did not commit event order")
	}
	events[0].Signature = string([]byte{0xff})
	if _, err := MerkleRoot(events); err == nil {
		t.Fatal("Merkle leaf silently replaced invalid UTF-8")
	}
}

func TestMineValidatesSequentialEventsAndCopiesInputs(t *testing.T) {
	cfg, keys := testConfig(t)
	genesis, err := Genesis(cfg)
	if err != nil {
		t.Fatal(err)
	}
	state := NewState()
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	offer := nextEvent(created, KindOffer, created.Actor)
	offer.Recipient = cfg.Participants[2].ID
	offer = signTestEvent(t, offer, keys["supplier"])
	accept := signTestEvent(t, nextEvent(offer, KindAccept, offer.Recipient), keys["carrier"])
	events := []Event{created, offer, accept}
	block, err := Mine(context.Background(), cfg, genesis, state, events, GenesisTimestamp+1)
	if err != nil {
		t.Fatal(err)
	}
	if block.Height != 1 || block.ParentHash != genesis.Hash || !MeetsDifficulty(block.Hash, cfg.Difficulty) {
		t.Fatalf("incorrect mined block: %+v", block)
	}
	nextState, err := ValidateBlock(cfg, genesis, block, state)
	if err != nil {
		t.Fatal(err)
	}
	batch, _ := nextState.GetBatch(created.BatchID)
	if batch.Owner != offer.Recipient || batch.Sequence != 3 {
		t.Fatalf("events did not apply in order: %+v", batch)
	}
	if len(state.Batches) != 0 {
		t.Fatal("mining or validation mutated parent state")
	}
	events[0].Name = "mutated"
	if block.Events[0].Name != created.Name {
		t.Fatal("mined events alias caller slice")
	}
	cloned := block.Clone()
	cloned.Events[0].Name = "mutated"
	if block.Events[0].Name != created.Name {
		t.Fatal("block clone aliases source events")
	}
	empty, err := Mine(context.Background(), cfg, block, nextState, nil, block.Timestamp+1)
	if err != nil {
		t.Fatal(err)
	}
	afterEmpty, err := ValidateBlock(cfg, block, empty, nextState)
	if err != nil || !reflect.DeepEqual(nextState, afterEmpty) {
		t.Fatalf("empty block changed state: %v", err)
	}
}

func TestValidateBlockRejectsHeaderAndEventTampering(t *testing.T) {
	cfg, keys := testConfig(t)
	genesis, _ := Genesis(cfg)
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	block, err := Mine(context.Background(), cfg, genesis, NewState(), []Event{created}, GenesisTimestamp+1)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Block){
		"version":        func(b *Block) { b.Version++ },
		"chain":          func(b *Block) { b.ChainID += "other" },
		"height":         func(b *Block) { b.Height++ },
		"parent":         func(b *Block) { b.ParentHash = strings.Repeat("a", 64) },
		"timestamp":      func(b *Block) { b.Timestamp = genesis.Timestamp - 1 },
		"difficulty":     func(b *Block) { b.Difficulty++ },
		"nonce":          func(b *Block) { b.Nonce++ },
		"root":           func(b *Block) { b.MerkleRoot = strings.Repeat("a", 64) },
		"hash":           func(b *Block) { b.Hash = strings.Repeat("0", 64) },
		"uppercase hash": func(b *Block) { b.Hash = strings.ToUpper(b.Hash) },
		"event":          func(b *Block) { b.Events[0].Name += "other" },
		"removed event":  func(b *Block) { b.Events = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := block.Clone()
			mutate(&changed)
			parentState := NewState()
			if _, err := ValidateBlock(cfg, genesis, changed, parentState); err == nil {
				t.Fatal("accepted modified block")
			}
			if len(parentState.Batches) != 0 {
				t.Fatal("rejected block changed parent state")
			}
		})
	}
	wrongDifficulty := block.Clone()
	wrongDifficulty.Difficulty = 1
	remineTestBlock(t, &wrongDifficulty)
	if _, err := ValidateBlock(cfg, genesis, wrongDifficulty, NewState()); err == nil {
		t.Fatal("accepted valid proof with unexpected difficulty")
	}
	noProof := block.Clone()
	for {
		noProof.Nonce++
		noProof.Hash, _ = BlockHash(noProof)
		if !MeetsDifficulty(noProof.Hash, cfg.Difficulty) {
			break
		}
	}
	if _, err := ValidateBlock(cfg, genesis, noProof, NewState()); err == nil {
		t.Fatal("accepted hash that does not meet proof target")
	}
}

func remineTestBlock(t *testing.T, block *Block) {
	t.Helper()
	var err error
	block.MerkleRoot, err = MerkleRoot(block.Events)
	if err != nil {
		t.Fatal(err)
	}
	for block.Nonce = 0; ; block.Nonce++ {
		block.Hash, err = BlockHash(*block)
		if err != nil {
			t.Fatal(err)
		}
		if MeetsDifficulty(block.Hash, block.Difficulty) {
			return
		}
	}
}

func TestValidProofCannotBypassStateValidation(t *testing.T) {
	cfg, keys := testConfig(t)
	genesis, _ := Genesis(cfg)
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	block, err := Mine(context.Background(), cfg, genesis, NewState(), []Event{created}, GenesisTimestamp+1)
	if err != nil {
		t.Fatal(err)
	}
	state, err := ValidateBlock(cfg, genesis, block, NewState())
	if err != nil {
		t.Fatal(err)
	}
	stale := nextEvent(created, KindCheckpoint, created.Actor)
	stale.Sequence = created.Sequence
	stale = signTestEvent(t, stale, keys["supplier"])
	forged := Block{Version: BlockVersion, ChainID: cfg.ChainID, Height: 2, ParentHash: block.Hash, Timestamp: block.Timestamp + 1, Difficulty: cfg.Difficulty, Events: []Event{stale}}
	remineTestBlock(t, &forged)
	before := state.Clone()
	if _, err := ValidateBlock(cfg, block, forged, state); err == nil {
		t.Fatal("accepted stale event despite valid Merkle root and proof")
	}
	if !reflect.DeepEqual(before, state) {
		t.Fatal("failed block validation mutated ancestor state")
	}
	if _, err := Mine(context.Background(), cfg, block, state, []Event{stale}, block.Timestamp+1); err == nil {
		t.Fatal("mined invalid state transition")
	}
}

func TestMiningCancellationAndConsensusBounds(t *testing.T) {
	cfg, keys := testConfig(t)
	genesis, _ := Genesis(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Mine(ctx, cfg, genesis, NewState(), nil, genesis.Timestamp+1); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected mining cancellation, got %v", err)
	}
	if _, err := Mine(context.Background(), cfg, genesis, NewState(), nil, genesis.Timestamp-1); err == nil {
		t.Fatal("mined decreasing timestamp")
	}
	if _, err := Mine(context.Background(), cfg, genesis, NewState(), make([]Event, MaxBlockEvents+1), genesis.Timestamp+1); err == nil {
		t.Fatal("mined oversized event count")
	}
	cfg.ChainID = strings.Repeat("\x01", MaxChainIDBytes)
	genesis, _ = Genesis(cfg)
	events := make([]Event, MaxBlockEvents)
	for i := range events {
		e := createEvent(cfg, keys)
		e.BatchID = strconv.Itoa(i) + strings.Repeat("\x01", MaxBatchIDBytes-3)
		e.Name = strings.Repeat("\x01", MaxNameBytes)
		e.Unit = strings.Repeat("\x01", MaxUnitBytes)
		e.Location = strings.Repeat("\x01", MaxLocationBytes)
		e.Status = strings.Repeat("\x01", MaxStatusBytes)
		events[i] = signTestEvent(t, e, keys["supplier"])
	}
	if _, err := Mine(context.Background(), cfg, genesis, NewState(), events, genesis.Timestamp+1); err == nil {
		t.Fatal("mined block larger than one MiB")
	}
	cfg.ChainID = "cancellation-test"
	cfg.Difficulty = MaxDifficulty
	genesis, _ = Genesis(cfg)
	ctx, cancel = context.WithCancel(context.Background())
	timer := time.AfterFunc(time.Millisecond, cancel)
	defer timer.Stop()
	_, err := Mine(ctx, cfg, genesis, NewState(), nil, genesis.Timestamp+1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("active mining did not stop on cancellation: %v", err)
	}
}

func TestMiningRejectsHeightOverflow(t *testing.T) {
	cfg, _ := testConfig(t)
	parent := Block{Version: BlockVersion, ChainID: cfg.ChainID, Height: math.MaxUint64, ParentHash: strings.Repeat("a", 64), Timestamp: GenesisTimestamp + 1, Difficulty: cfg.Difficulty, Events: []Event{}}
	remineTestBlock(t, &parent)
	if _, err := Mine(context.Background(), cfg, parent, NewState(), nil, GenesisTimestamp+2); err == nil {
		t.Fatal("accepted a parent whose next height would overflow")
	}
	if _, err := ValidateBlock(cfg, parent, Block{}, NewState()); err == nil {
		t.Fatal("validated a child of an exhausted parent")
	}
}

func TestEqualBlockTimestampsRemainExtendableAtMaxInt64(t *testing.T) {
	cfg, _ := testConfig(t)
	parent, err := Genesis(cfg)
	if err != nil {
		t.Fatal(err)
	}
	state := NewState()
	// Equal timestamps are valid even at genesis; height defines block order.
	for _, timestamp := range []int64{GenesisTimestamp, math.MaxInt64, math.MaxInt64, math.MaxInt64} {
		block, err := Mine(context.Background(), cfg, parent, state, nil, timestamp)
		if err != nil {
			t.Fatalf("could not extend height %d with timestamp %d: %v", parent.Height, timestamp, err)
		}
		nextState, err := ValidateBlock(cfg, parent, block, state)
		if err != nil {
			t.Fatalf("rejected nondecreasing timestamp %d: %v", timestamp, err)
		}
		parent, state = block, nextState
	}
	if _, err := Mine(context.Background(), cfg, parent, state, nil, math.MaxInt64-1); err == nil {
		t.Fatal("mined a decreasing timestamp after the maximum timestamp")
	}
	decreasing := Block{Version: BlockVersion, ChainID: cfg.ChainID, Height: parent.Height + 1, ParentHash: parent.Hash, Timestamp: math.MaxInt64 - 1, Difficulty: cfg.Difficulty, Events: []Event{}}
	remineTestBlock(t, &decreasing)
	if _, err := ValidateBlock(cfg, parent, decreasing, state); err == nil {
		t.Fatal("accepted a decreasing timestamp with a valid proof")
	}
}

func TestDifficultyCountsLeadingBits(t *testing.T) {
	for _, test := range []struct {
		hash string
		bits uint8
		want bool
	}{
		{strings.Repeat("0", 64), 24, true},
		{"0f" + strings.Repeat("f", 62), 4, true},
		{"1f" + strings.Repeat("f", 62), 4, false},
		{"07" + strings.Repeat("f", 62), 5, true},
		{"08" + strings.Repeat("f", 62), 5, false},
		{"000001" + strings.Repeat("f", 58), 23, true},
		{"000001" + strings.Repeat("f", 58), 24, false},
		{strings.Repeat("0", 64), 0, false},
		{strings.Repeat("0", 64), 25, false},
		{"invalid", 4, false},
	} {
		if got := MeetsDifficulty(test.hash, test.bits); got != test.want {
			t.Fatalf("MeetsDifficulty(%s, %d) = %t", test.hash, test.bits, got)
		}
	}
}
