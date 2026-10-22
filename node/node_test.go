package node

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"os"
	"path/filepath"
	"supplychain/chain"
	"supplychain/ledger"
	"testing"
	"time"
)

func fixture(t *testing.T) (ledger.Config, ed25519.PrivateKey) {
	t.Helper()
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	p, err := ledger.NewParticipant("supplier", ledger.RoleSupplier, key.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	return ledger.Config{ChainID: "network-test", Difficulty: 1, Participants: []ledger.Participant{p}}, key
}
func openChain(t *testing.T, cfg ledger.Config) *chain.Chain {
	t.Helper()
	c, err := chain.Open(filepath.Join(t.TempDir(), "ledger.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func start(t *testing.T, c *chain.Chain, cfg Config) *Node {
	t.Helper()
	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	if cfg.ListenAddrs == nil {
		cfg.ListenAddrs = []string{"/ip4/127.0.0.1/tcp/0"}
	}
	if cfg.SyncInterval == 0 {
		cfg.SyncInterval = 100 * time.Millisecond
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	n, err := New(context.Background(), c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}
func event(t *testing.T, cfg ledger.Config, key ed25519.PrivateKey, id string) ledger.Event {
	t.Helper()
	e := ledger.Event{Version: 1, ChainID: cfg.ChainID, Kind: ledger.KindCreate, BatchID: id, Actor: ledger.PublicKeyID(key.Public().(ed25519.PublicKey)), Name: "coffee", Unit: "kg", Quantity: 1, Sequence: 1, Timestamp: 1}
	if err := ledger.SignEvent(&e, key); err != nil {
		t.Fatal(err)
	}
	return e
}
func await(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if f() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("condition did not converge")
		case <-ticker.C:
		}
	}
}
func connect(t *testing.T, a, b *Node) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.Connect(ctx, b.Addrs()[0]); err != nil {
		t.Fatal(err)
	}
}
func TestRealHostsPropagationCatchupAndBootstrapLoss(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb, cc := openChain(t, cfg), openChain(t, cfg), openChain(t, cfg)
	a, b, c := start(t, ca, Config{}), start(t, cb, Config{}), start(t, cc, Config{})
	connect(t, a, b)
	connect(t, b, c)
	e := event(t, cfg, key, "A")
	if err := a.Submit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(cc.Pending()) == 1 })
	block, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return ca.Tip().Hash == block.Hash && cb.Tip().Hash == block.Hash })
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 70; i++ {
		if _, err := b.Mine(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	cd := openChain(t, cfg)
	d := start(t, cd, Config{BootstrapPeers: b.Addrs()})
	await(t, func() bool { return cd.Tip().Hash == cb.Tip().Hash && cc.Tip().Hash == cb.Tip().Hash })
	if len(d.Peers()) == 0 {
		t.Fatal("no approved peers")
	}
}
func TestIdentityRestartOfflineCatchupAndWrongNetwork(t *testing.T) {
	cfg, _ := fixture(t)
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	dir := t.TempDir()
	a := start(t, ca, Config{DataDir: dir})
	b := start(t, cb, Config{})
	connect(t, a, b)
	id := a.ID()
	a.Close()
	info, err := os.Stat(filepath.Join(dir, "identity.key"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	for i := 0; i < 35; i++ {
		if _, err := b.Mine(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	a2 := start(t, ca, Config{DataDir: dir})
	if a2.ID() != id {
		t.Fatal("identity changed")
	}
	await(t, func() bool { return ca.Tip().Hash == cb.Tip().Hash })
	other := cfg
	other.Difficulty = 2
	wrong := start(t, openChain(t, other), Config{})
	if err := wrong.Connect(context.Background(), b.Addrs()[0]); err == nil {
		t.Fatal("approved different genesis")
	}
	if len(wrong.Peers()) != 0 {
		t.Fatal("wrong network peer approved")
	}
}
func TestEqualHeightForksConvergeAndRetainBothBranches(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	a, b := start(t, ca, Config{}), start(t, cb, Config{})
	ea, eb := event(t, cfg, key, "A"), event(t, cfg, key, "A")
	eb.Location = "other"
	if err := ledger.SignEvent(&eb, key); err != nil {
		t.Fatal(err)
	}
	if err := a.Submit(context.Background(), ea); err != nil {
		t.Fatal(err)
	}
	ba, err := a.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Submit(context.Background(), eb); err != nil {
		t.Fatal(err)
	}
	bb, err := b.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ba.Hash == bb.Hash {
		t.Fatal("identical branches")
	}
	connect(t, a, b)
	await(t, func() bool { return ca.Tip().Hash == cb.Tip().Hash && ca.HasBlock(bb.Hash) && cb.HasBlock(ba.Hash) })
	for i := 0; i < 40; i++ {
		if _, err := b.Mine(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	await(t, func() bool { return ca.Tip().Hash == cb.Tip().Hash })
	aa, _ := ca.Batch("A")
	ab, _ := cb.Batch("A")
	if aa != ab {
		t.Fatal("state mismatch")
	}
}
func TestConfigBounds(t *testing.T) {
	cfg, _ := fixture(t)
	c := openChain(t, cfg)
	for _, bad := range []Config{{DataDir: t.TempDir(), MaxPeers: 65}, {DataDir: t.TempDir(), ProtocolVersion: "99"}, {DataDir: t.TempDir(), RequestTimeout: -time.Second}, {DataDir: t.TempDir(), SyncInterval: -time.Second}, {DataDir: t.TempDir(), ListenAddrs: []string{"invalid"}}} {
		if n, err := New(context.Background(), c, bad); err == nil {
			n.Close()
			t.Fatal(fmt.Sprintf("accepted %+v", bad))
		}
	}
}

func TestGossipRejectsTamperedKnownEvent(t *testing.T) {
	cfg, key := fixture(t)
	c := openChain(t, cfg)
	n := start(t, c, Config{})
	e := event(t, cfg, key, "known")
	if err := n.Submit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	e.Signature = fmt.Sprintf("%0128d", 0)
	if err := n.publish(context.Background(), n.events, e); err == nil {
		t.Fatal("relayed invalid signature sharing known ID")
	}
}
