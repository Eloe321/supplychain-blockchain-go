package chain

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"supplychain/ledger"
)

func fixture(t *testing.T) (ledger.Config, map[string]ed25519.PrivateKey) {
	t.Helper()
	keys := map[string]ed25519.PrivateKey{}
	cfg := ledger.Config{ChainID: "chain-test", Difficulty: 1}
	for i, role := range []string{ledger.RoleSupplier, ledger.RoleCarrier} {
		seed := make([]byte, 32)
		seed[0] = byte(i + 1)
		key := ed25519.NewKeyFromSeed(seed)
		keys[role] = key
		p, err := ledger.NewParticipant(role, role, key.Public().(ed25519.PublicKey))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Participants = append(cfg.Participants, p)
	}
	return cfg, keys
}
func signed(t *testing.T, cfg ledger.Config, keys map[string]ed25519.PrivateKey, role string, e ledger.Event) ledger.Event {
	t.Helper()
	e.Version = ledger.EventVersion
	e.ChainID = cfg.ChainID
	e.Actor = ledger.PublicKeyID(keys[role].Public().(ed25519.PublicKey))
	e.Timestamp = 1
	if err := ledger.SignEvent(&e, keys[role]); err != nil {
		t.Fatal(err)
	}
	return e
}
func create(t *testing.T, cfg ledger.Config, keys map[string]ed25519.PrivateKey, id string) ledger.Event {
	return signed(t, cfg, keys, ledger.RoleSupplier, ledger.Event{Kind: ledger.KindCreate, BatchID: id, Name: "coffee", Quantity: 1, Unit: "kg", Sequence: 1})
}
func next(t *testing.T, cfg ledger.Config, keys map[string]ed25519.PrivateKey, role string, prev ledger.Event, kind string) ledger.Event {
	return signed(t, cfg, keys, role, ledger.Event{Kind: kind, BatchID: prev.BatchID, Sequence: prev.Sequence + 1, PreviousEvent: prev.ID})
}
func mine(t *testing.T, cfg ledger.Config, parent ledger.Block, state *ledger.State, events ...ledger.Event) (ledger.Block, *ledger.State) {
	t.Helper()
	b, err := ledger.Mine(context.Background(), cfg, parent, state, events, parent.Timestamp+1)
	if err != nil {
		t.Fatal(err)
	}
	s, err := ledger.ValidateBlock(cfg, parent, b, state)
	if err != nil {
		t.Fatal(err)
	}
	return b, s
}
func openTest(t *testing.T) (*Chain, ledger.Config, map[string]ed25519.PrivateKey, string) {
	t.Helper()
	cfg, keys := fixture(t)
	path := filepath.Join(t.TempDir(), "chain.db")
	c, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, cfg, keys, path
}

func TestOpenRestartAndConfiguration(t *testing.T) {
	c, cfg, _, path := openTest(t)
	genesis, _ := ledger.Genesis(cfg)
	if !reflect.DeepEqual(c.Genesis(), genesis) || c.Tip().Hash != genesis.Hash || c.Status().Work != "0" {
		t.Fatal("wrong initial anchor")
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatalf("permissions: %v %v", stat, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_ = reopened.Close()
	cfg.ChainID = "different"
	if other, err := Open(path, cfg); err == nil {
		_ = other.Close()
		t.Fatal("opened mismatched config")
	}
}

func TestValidatedGraphPersistenceForkChoiceAndQueries(t *testing.T) {
	c, cfg, keys, path := openTest(t)
	g := c.Genesis()
	e := create(t, cfg, keys, "A")
	a, as := mine(t, cfg, g, ledger.NewState(), e)
	if changed, err := c.AddBlock(a); err != nil || !changed {
		t.Fatalf("add: %v %v", changed, err)
	}
	invalid := a.Clone()
	invalid.Hash = "bad"
	if _, err := c.AddBlock(invalid); err == nil {
		t.Fatal("accepted invalid block")
	}
	if c.HasBlock("bad") || c.Tip().Hash != a.Hash {
		t.Fatal("invalid insertion changed graph")
	}
	if _, err := c.AddBlock(a); !errors.Is(err, ErrKnownBlock) {
		t.Fatal(err)
	}
	unknown := a.Clone()
	unknown.ParentHash = "unknown"
	unknown.Hash = "unknown"
	if _, err := c.AddBlock(unknown); !errors.Is(err, ErrUnknownParent) {
		t.Fatal(err)
	}
	// This branch independently creates the same batch and must validate from genesis.
	other := create(t, cfg, keys, "A")
	other.Location = "elsewhere"
	other = signed(t, cfg, keys, ledger.RoleSupplier, other)
	b, bs := mine(t, cfg, g, ledger.NewState(), other)
	if _, err := c.AddBlock(b); err != nil {
		t.Fatal(err)
	}
	want := a.Hash
	if b.Hash < want {
		want = b.Hash
	}
	if c.Tip().Hash != want {
		t.Fatal("tie was not deterministic")
	}
	child, _ := mine(t, cfg, b, bs)
	if changed, err := c.AddBlock(child); err != nil || !changed {
		t.Fatalf("long fork: %v %v", changed, err)
	}
	batch, ok := c.Batch("A")
	if !ok || batch.Location != "elsewhere" {
		t.Fatal("state did not follow branch")
	}
	if c.Status().Work != "4" || c.Status().Height != 2 {
		t.Fatalf("wrong work %+v", c.Status())
	}
	trace := c.Trace("A")
	if len(trace) != 1 || trace[0].Event.ID != other.ID || trace[0].Confirmations != 2 {
		t.Fatalf("trace: %+v", trace)
	}
	if len(c.Batches()) != 1 || len(c.CanonicalBlocks()) != 3 {
		t.Fatal("canonical queries")
	}
	got, ok := c.GetBlock(a.Hash)
	if !ok {
		t.Fatal("lost side branch")
	}
	got.Events[0].Name = "mutated"
	got, _ = c.GetBlock(a.Hash)
	if got.Events[0].Name == "mutated" {
		t.Fatal("block aliases")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if c2.Tip().Hash != child.Hash || !c2.HasBlock(a.Hash) {
		t.Fatal("restart lost graph")
	}
	_ = as
}

func TestMempoolDependenciesMiningAndDuplicates(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	e := create(t, cfg, keys, "A")
	offer := next(t, cfg, keys, ledger.RoleSupplier, e, ledger.KindOffer)
	offer.Recipient = ledger.PublicKeyID(keys[ledger.RoleCarrier].Public().(ed25519.PublicKey))
	offer = signed(t, cfg, keys, ledger.RoleSupplier, offer)
	accept := next(t, cfg, keys, ledger.RoleCarrier, offer, ledger.KindAccept)
	if err := c.Submit(offer); err == nil {
		t.Fatal("accepted missing dependency")
	}
	for _, event := range []ledger.Event{e, offer, accept} {
		if err := c.Submit(event); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Submit(e); !errors.Is(err, ErrKnownEvent) {
		t.Fatal(err)
	}
	conflict := e
	conflict.Name = "conflict"
	conflict = signed(t, cfg, keys, ledger.RoleSupplier, conflict)
	if err := c.Submit(conflict); err == nil {
		t.Fatal("accepted conflicting create")
	}
	if c.Status().Pending != 3 {
		t.Fatal("pending count")
	}
	pending := c.Pending()
	pending[0].Name = "changed"
	if c.Pending()[0].Name == "changed" {
		t.Fatal("pending aliases")
	}
	b, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Events) != 3 || len(c.Pending()) != 0 {
		t.Fatal("mining snapshot/pool")
	}
	if err := c.Submit(e); !errors.Is(err, ErrKnownEvent) {
		t.Fatal(err)
	}
	batch, _ := c.Batch("A")
	if batch.Owner != accept.Actor {
		t.Fatal("pending transfer not mined")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Mine(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReorganizationRestoresDetachedDependenciesAndDropsConflicts(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	g := c.Genesis()
	a := create(t, cfg, keys, "A")
	offer := next(t, cfg, keys, ledger.RoleSupplier, a, ledger.KindOffer)
	offer.Recipient = ledger.PublicKeyID(keys[ledger.RoleCarrier].Public().(ed25519.PublicKey))
	offer = signed(t, cfg, keys, ledger.RoleSupplier, offer)
	accept := next(t, cfg, keys, ledger.RoleCarrier, offer, ledger.KindAccept)
	b1, s1 := mine(t, cfg, g, ledger.NewState(), a, offer, accept)
	if _, err := c.AddBlock(b1); err != nil {
		t.Fatal(err)
	}
	checkpoint := next(t, cfg, keys, ledger.RoleCarrier, accept, ledger.KindCheckpoint)
	if err := c.Submit(checkpoint); err != nil {
		t.Fatal(err)
	}
	// An unrelated longer branch detaches all A events; the full valid dependency chain returns.
	x := create(t, cfg, keys, "X")
	f1, fs := mine(t, cfg, g, ledger.NewState(), x)
	_, _ = c.AddBlock(f1)
	f2, _ := mine(t, cfg, f1, fs)
	if _, err := c.AddBlock(f2); err != nil {
		t.Fatal(err)
	}
	if len(c.Pending()) != 4 {
		t.Fatalf("detached chain missing: %d", len(c.Pending()))
	}
	for i, e := range c.Pending() {
		if e.Sequence != uint64(i+1) {
			t.Fatal("dependency order")
		}
	}
	// Now extend original branch with a valid conflicting checkpoint of the accepted owner.
	conflict := checkpoint
	conflict.Location = "canonical"
	conflict = signed(t, cfg, keys, ledger.RoleCarrier, conflict)
	a2, s2 := mine(t, cfg, b1, s1, conflict)
	_, _ = c.AddBlock(a2)
	a3, _ := mine(t, cfg, a2, s2)
	if _, err := c.AddBlock(a3); err != nil {
		t.Fatal(err)
	}
	for _, e := range c.Pending() {
		if e.BatchID == "A" {
			t.Fatal("retained conflicting or confirmed event")
		}
	}
	if len(c.Pending()) != 1 || c.Pending()[0].ID != x.ID {
		t.Fatal("detached independent event lost")
	}
}

func TestLocatorsBoundedBatchesAndClosedOperations(t *testing.T) {
	c, cfg, _, _ := openTest(t)
	g := c.Genesis()
	parent := g
	state := ledger.NewState()
	for i := 0; i < 40; i++ {
		b, s := mine(t, cfg, parent, state)
		if _, err := c.AddBlock(b); err != nil {
			t.Fatal(err)
		}
		parent, state = b, s
	}
	loc, err := c.Locators()
	if err != nil || loc[0] != c.Tip().Hash || loc[len(loc)-1] != g.Hash {
		t.Fatal("locators", err)
	}
	blocks, err := c.BlocksAfter([]string{g.Hash}, 1<<30)
	if err != nil || len(blocks) != 32 {
		t.Fatal("bound", err, len(blocks))
	}
	second, err := c.BlocksAfter([]string{blocks[31].Hash}, 32)
	if err != nil || len(second) != 8 || second[0].ParentHash != blocks[31].Hash {
		t.Fatal("batch cursor", err)
	}
	if _, err := c.BlocksAfter([]string{"wrong-genesis"}, 32); !errors.Is(err, ErrNoCommonAncestor) {
		t.Fatal(err)
	}
	if _, err := c.BlocksAfter([]string{g.Hash}, 0); err == nil {
		t.Fatal("zero limit")
	}
	cfgCopy := c.Config()
	cfgCopy.Participants[0].Name = "changed"
	if c.Config().Participants[0].Name == "changed" {
		t.Fatal("config aliases")
	}
	stateCopy := c.State()
	stateCopy.Batches["bad"] = ledger.Batch{}
	if len(c.State().Batches) != 0 {
		t.Fatal("state aliases")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddBlock(parent); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := c.Submit(ledger.Event{}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := c.Mine(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := c.Locators(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := c.BlocksAfter([]string{g.Hash}, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if c.Pending() != nil || c.HasBlock(g.Hash) {
		t.Fatal("closed lookup")
	}
}

func TestConcurrentCopiesAndMutations(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			e := create(t, cfg, keys, fmt.Sprint(i))
			if err := c.Submit(e); err != nil {
				t.Error(err)
			}
			_ = c.Config()
			_ = c.Tip()
			_ = c.Status()
			_ = c.State()
			_ = c.Pending()
			_ = c.CanonicalBlocks()
		}(i)
	}
	wg.Wait()
	if len(c.Pending()) != 10 {
		t.Fatal("lost concurrent submit")
	}
}

func TestStartupRejectsCorruptGraphAndStoredTip(t *testing.T) {
	for _, kind := range []string{"genesis", "invalid side branch", "missing parent", "wrong tip", "config", "key mismatch"} {
		t.Run(kind, func(t *testing.T) {
			c, cfg, _, path := openTest(t)
			g := c.Genesis()
			a, as := mine(t, cfg, g, ledger.NewState())
			_, _ = c.AddBlock(a)
			a2, _ := mine(t, cfg, a, as)
			_, _ = c.AddBlock(a2)
			side, _ := ledger.Mine(context.Background(), cfg, g, ledger.NewState(), nil, g.Timestamp+2)
			_, _ = c.AddBlock(side)
			_ = c.Close()
			db, err := bolt.Open(path, 0600, nil)
			if err != nil {
				t.Fatal(err)
			}
			err = db.Update(func(tx *bolt.Tx) error {
				blocks := tx.Bucket(blocksBucket)
				meta := tx.Bucket(metaBucket)
				switch kind {
				case "genesis":
					return blocks.Delete([]byte(g.Hash))
				case "invalid side branch":
					side.Nonce++
					raw, _ := json.Marshal(side)
					return blocks.Put([]byte(side.Hash), raw)
				case "missing parent":
					return blocks.Delete([]byte(a.Hash))
				case "wrong tip":
					return meta.Put(tipKey, []byte(g.Hash))
				case "config":
					return meta.Put(configKey, []byte(`{"chain_id":"broken"}`))
				case "key mismatch":
					raw, _ := json.Marshal(a2)
					return blocks.Put([]byte("wrong"), raw)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
			if reopened, err := Open(path, cfg); err == nil {
				_ = reopened.Close()
				t.Fatal("trusted corrupt store")
			}
		})
	}
}

func TestWriteFailureLeavesMemoryAndDatabaseUnchanged(t *testing.T) {
	c, cfg, keys, path := openTest(t)
	e := create(t, cfg, keys, "A")
	if err := c.Submit(e); err != nil {
		t.Fatal(err)
	}
	g := c.Genesis()
	b, _ := mine(t, cfg, g, ledger.NewState(), e)
	before := c.Status()
	if err := c.db.Close(); err != nil {
		t.Fatal(err)
	}
	if changed, err := c.AddBlock(b); err == nil || changed {
		t.Fatal("accepted closed storage write")
	}
	if c.Status() != before || c.HasBlock(b.Hash) || len(c.Pending()) != 1 {
		t.Fatal("failed write mutated memory")
	}
	reopened, err := Open(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Tip().Hash != g.Hash || reopened.HasBlock(b.Hash) {
		t.Fatal("failed write persisted")
	}
}

func TestMempoolCapacityMiningCapAndTimestamp(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	for i := 0; i < MaxPending; i++ {
		if err := c.Submit(create(t, cfg, keys, fmt.Sprintf("B-%04d", i))); err != nil {
			t.Fatal(i, err)
		}
	}
	extra := create(t, cfg, keys, "overflow")
	if err := c.Submit(extra); !errors.Is(err, ErrMempoolFull) {
		t.Fatal(err)
	}
	if len(c.Pending()) != MaxPending {
		t.Fatal("capacity changed")
	}
	b, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Events) != ledger.MaxBlockEvents || len(c.Pending()) != MaxPending-ledger.MaxBlockEvents {
		t.Fatal("mining did not cap snapshot")
	}
	if b.Timestamp < c.Genesis().Timestamp {
		t.Fatal("decreasing timestamp")
	}
	// Equal timestamps must remain mineable even when no wall clock can exceed parent.
	parent := c.Tip()
	s := c.State()
	far, err := ledger.Mine(context.Background(), cfg, parent, s, nil, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddBlock(far); err != nil {
		t.Fatal(err)
	}
	equal, err := c.Mine(context.Background())
	if err != nil || equal.Timestamp != math.MaxInt64 {
		t.Fatal("max parent timestamp", err)
	}
}

func TestLongerForkRollsBackConflictingOwnership(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	g := c.Genesis()
	e := create(t, cfg, keys, "A")
	base, baseState := mine(t, cfg, g, ledger.NewState(), e)
	_, _ = c.AddBlock(base)
	offer := next(t, cfg, keys, ledger.RoleSupplier, e, ledger.KindOffer)
	offer.Recipient = ledger.PublicKeyID(keys[ledger.RoleCarrier].Public().(ed25519.PublicKey))
	offer = signed(t, cfg, keys, ledger.RoleSupplier, offer)
	accept := next(t, cfg, keys, ledger.RoleCarrier, offer, ledger.KindAccept)
	transferred, _ := mine(t, cfg, base, baseState, offer, accept)
	_, _ = c.AddBlock(transferred)
	batch, _ := c.Batch("A")
	if batch.Owner != accept.Actor {
		t.Fatal("transfer not reflected")
	}
	checkpoint := next(t, cfg, keys, ledger.RoleSupplier, e, ledger.KindCheckpoint)
	alternative, altState := mine(t, cfg, base, baseState, checkpoint)
	if _, err := c.AddBlock(alternative); err != nil {
		t.Fatal("validated side branch using canonical custody", err)
	}
	longer, _ := mine(t, cfg, alternative, altState)
	if _, err := c.AddBlock(longer); err != nil {
		t.Fatal(err)
	}
	batch, _ = c.Batch("A")
	if batch.Owner != e.Actor || batch.Sequence != 2 || batch.LastEvent != checkpoint.ID {
		t.Fatal("custody rollback failed", batch)
	}
	if len(c.Pending()) != 0 {
		t.Fatal("retained detached transfer that conflicts with winning predecessor")
	}
	if trace := c.Trace("A"); len(trace) != 2 || trace[0].Confirmations != 3 || trace[1].Confirmations != 2 {
		t.Fatal("trace retained detached custody", trace)
	}
}

func TestCopiedBlockInputsAndSnapshots(t *testing.T) {
	c, cfg, keys, _ := openTest(t)
	e := create(t, cfg, keys, "A")
	b, _ := mine(t, cfg, c.Genesis(), ledger.NewState(), e)
	_, _ = c.AddBlock(b)
	b.Events[0].Name = "input mutated"
	tip := c.Tip()
	if tip.Events[0].Name != e.Name {
		t.Fatal("input block retained alias")
	}
	tip.Events[0].Name = "tip mutated"
	all := c.CanonicalBlocks()
	all[1].Events[0].Name = "list mutated"
	batches := c.Batches()
	batches[0].Owner = "batch mutated"
	trace := c.Trace("A")
	trace[0].Event.Name = "trace mutated"
	if c.Tip().Events[0].Name != e.Name || c.Trace("A")[0].Event.Name != e.Name {
		t.Fatal("snapshot aliases")
	}
	batch, _ := c.Batch("A")
	if batch.Owner != e.Actor {
		t.Fatal("batch aliases")
	}
}

func TestMiningCapsEscapedBlockBytesAndRetainsRemainder(t *testing.T) {
	cfg, keys := fixture(t)
	cfg.ChainID = strings.Repeat("<", ledger.MaxChainIDBytes)
	c, err := Open(filepath.Join(t.TempDir(), "escaped.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for i := 0; i < ledger.MaxBlockEvents; i++ {
		event := signed(t, cfg, keys, ledger.RoleSupplier, ledger.Event{
			Kind: ledger.KindCreate, BatchID: fmt.Sprintf("%03d", i) + strings.Repeat("<", ledger.MaxBatchIDBytes-3),
			Name: strings.Repeat("<", ledger.MaxNameBytes), Unit: strings.Repeat("<", ledger.MaxUnitBytes),
			Location: strings.Repeat("<", ledger.MaxLocationBytes), Status: strings.Repeat("<", ledger.MaxStatusBytes),
			Quantity: 1, Sequence: 1,
		})
		if err := c.Submit(event); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) == 0 || len(first.Events) >= ledger.MaxBlockEvents {
		t.Fatalf("expected byte-bounded prefix, got %d events", len(first.Events))
	}
	raw, err := json.Marshal(first)
	if err != nil || len(raw) > ledger.MaxBlockBytes {
		t.Fatalf("oversized block: %d, %v", len(raw), err)
	}
	remainder := c.Pending()
	if len(remainder) != ledger.MaxBlockEvents-len(first.Events) {
		t.Fatal("unmined remainder lost")
	}
	second, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != len(remainder) || len(c.Pending()) != 0 {
		t.Fatal("remainder did not progress")
	}
	for i, event := range second.Events {
		if event.ID != remainder[i].ID {
			t.Fatal("mining changed prefix order")
		}
	}
}
