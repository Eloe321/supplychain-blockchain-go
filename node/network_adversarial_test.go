package node

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"supplychain/chain"
	"supplychain/ledger"
)

func bareHost(t *testing.T) host.Host {
	t.Helper()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.Close() })
	return h
}
func dialHost(t *testing.T, h host.Host, n *Node) {
	t.Helper()
	info, err := parsePeer(n.Addrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Connect(context.Background(), *info); err != nil {
		t.Fatal(err)
	}
}
func TestHandshakeAndMalformedStreams(t *testing.T) {
	cfg, _ := fixture(t)
	c := openChain(t, cfg)
	n := start(t, c, Config{RequestTimeout: 150 * time.Millisecond})
	h := bareHost(t)
	dialHost(t, h, n)
	for _, change := range []func(*hello){func(v *hello) { v.Version = "99" }, func(v *hello) { v.ChainID = "other" }, func(v *hello) { v.GenesisHash = strings.Repeat("a", 64) }, func(v *hello) { v.ConfigHash = strings.Repeat("b", 64) }} {
		s, err := h.NewStream(context.Background(), n.host.ID(), n.protocol)
		if err != nil {
			t.Fatal(err)
		}
		v := n.hello
		change(&v)
		if err = writeFrame(s, request{Hello: v, Op: "status"}); err != nil {
			t.Fatal(err)
		}
		var r response
		if err = readFrame(s, &r); err != nil || r.Error == "" {
			t.Fatal(r, err)
		}
		s.Close()
		if n.isApproved(h.ID()) {
			t.Fatal("approved bad handshake")
		}
	}
	for _, raw := range [][]byte{binary.BigEndian.AppendUint32(nil, MaxFrameBytes+1), framed([]byte(`{"hello":{},"op":"status","surprise":1}`)), append(binary.BigEndian.AppendUint32(nil, 100), []byte(`{}`)...)} {
		s, err := h.NewStream(context.Background(), n.host.ID(), n.protocol)
		if err != nil {
			t.Fatal(err)
		}
		s.SetDeadline(time.Now().Add(time.Second))
		s.Write(raw)
		s.CloseWrite()
		if _, err = io.ReadAll(s); err == nil {
			t.Fatal("malformed request not reset")
		}
		s.Close()
	}
	stalled, err := h.NewStream(context.Background(), n.host.ID(), n.protocol)
	if err != nil {
		t.Fatal(err)
	}
	stalled.SetDeadline(time.Now().Add(time.Second))
	if _, err = io.ReadAll(stalled); err == nil {
		t.Fatal("stalled request not reset")
	}
	stalled.Close()
	good := start(t, openChain(t, cfg), Config{})
	connect(t, good, n)
	if len(good.Peers()) == 0 {
		t.Fatal("malformed peers prevented normal handshake")
	}
}
func TestSyncRetriesAfterStalledPeer(t *testing.T) {
	cfg, _ := fixture(t)
	c1, c2 := openChain(t, cfg), openChain(t, cfg)
	a, b := start(t, c1, Config{RequestTimeout: 150 * time.Millisecond}), start(t, c2, Config{})
	for i := 0; i < 3; i++ {
		if _, err := b.Mine(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	bad := bareHost(t)
	done := make(chan struct{})
	defer close(done)
	bad.SetStreamHandler(a.protocol, func(s network.Stream) { defer s.Close(); <-done })
	if err := a.host.Connect(context.Background(), peer.AddrInfo{ID: bad.ID(), Addrs: bad.Addrs()}); err != nil {
		t.Fatal(err)
	}
	connect(t, a, b)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = a.Sync(ctx)
	await(t, func() bool { return c1.Tip().Hash == c2.Tip().Hash })
}
func TestGossipOnlyMultihop(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb, cc := openChain(t, cfg), openChain(t, cfg), openChain(t, cfg)
	a, b, c := start(t, ca, Config{SyncInterval: time.Hour}), start(t, cb, Config{SyncInterval: time.Hour}), start(t, cc, Config{SyncInterval: time.Hour})
	connect(t, a, b)
	connect(t, b, c)
	await(t, func() bool {
		return len(a.events.ListPeers()) == 1 && len(b.events.ListPeers()) == 2 && len(c.events.ListPeers()) == 1
	})
	await(t, func() bool {
		return b.host.ConnManager().IsProtected(a.host.ID(), "pubsub:"+b.events.String()) && b.host.ConnManager().IsProtected(c.host.ID(), "pubsub:"+b.events.String()) && b.host.ConnManager().IsProtected(a.host.ID(), "pubsub:"+b.blocks.String()) && b.host.ConnManager().IsProtected(c.host.ID(), "pubsub:"+b.blocks.String())
	})
	if a.host.Network().Connectedness(c.host.ID()) == network.Connected {
		t.Fatal("topology not multihop")
	}
	e := event(t, cfg, key, "gossip")
	if err := a.Submit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(cc.Pending()) == 1 })
	block, err := c.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return ca.Tip().Hash == block.Hash })
	if a.host.Network().Connectedness(c.host.ID()) == network.Connected {
		t.Fatal("gossip bypassed middle node")
	}
}
func TestPendingRecoveryHasNoKnownPrefixStarvation(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	a, b := start(t, ca, Config{}), start(t, cb, Config{})
	e := event(t, cfg, key, "pending")
	for i := 0; i < 100; i++ {
		if i > 0 {
			prev := e
			e = ledger.Event{Version: 1, ChainID: cfg.ChainID, Kind: ledger.KindCheckpoint, BatchID: prev.BatchID, Actor: prev.Actor, Sequence: prev.Sequence + 1, PreviousEvent: prev.ID, Timestamp: 1}
			if err := ledger.SignEvent(&e, key); err != nil {
				t.Fatal(err)
			}
		}
		if err := ca.Submit(e); err != nil {
			t.Fatal(err)
		}
		if i < 60 {
			if err := cb.Submit(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	connect(t, a, b)
	await(t, func() bool { return len(cb.Pending()) == 100 })
	for i, e := range cb.Pending() {
		if e.Sequence != uint64(i+1) {
			t.Fatal("dependency order lost")
		}
	}
}
func TestLowerWorkCursorAcrossBatches(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	a, b := start(t, ca, Config{}), start(t, cb, Config{})
	for _, v := range []struct {
		n     *Node
		id    string
		count int
	}{{a, "local", 50}, {b, "remote", 80}} {
		if err := v.n.Submit(context.Background(), event(t, cfg, key, v.id)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < v.count; i++ {
			if _, err := v.n.Mine(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	connect(t, a, b)
	await(t, func() bool { return ca.Tip().Hash == cb.Tip().Hash && ca.Tip().Height == 80 })
}
func TestParentCancellationClosesHost(t *testing.T) {
	cfg, _ := fixture(t)
	c := openChain(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	n, err := New(ctx, c, Config{DataDir: t.TempDir(), ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	cancel()
	await(t, func() bool { return len(n.host.Network().ListenAddresses()) == 0 })
	if c.Genesis().Hash == "" {
		t.Fatal("node closed caller chain")
	}
}
func TestStaticRelayCircuitSync(t *testing.T) {
	cfg, key := fixture(t)
	relayNode := start(t, openChain(t, cfg), Config{EnableRelayService: true, ForcePublic: true})
	privateChain := openChain(t, cfg)
	private := start(t, privateChain, Config{RelayPeers: relayNode.Addrs(), ForcePrivate: true, ListenAddrs: []string{}})
	circuit := relayNode.Addrs()[0] + "/p2p-circuit/p2p/" + private.ID()
	callerChain := openChain(t, cfg)
	caller := start(t, callerChain, Config{})
	if _, err := private.Mine(context.Background()); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return caller.Connect(context.Background(), circuit) == nil })
	await(t, func() bool { return callerChain.Tip().Hash == privateChain.Tip().Hash })
	conns := caller.host.Network().ConnsToPeer(private.host.ID())
	if len(conns) == 0 || !strings.Contains(conns[0].RemoteMultiaddr().String(), "/p2p-circuit") {
		t.Fatal("sync did not traverse relay circuit")
	}
	e := event(t, cfg, key, "relay-pending")
	if err := private.Submit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	await(t, func() bool { return len(callerChain.Pending()) == 1 })
}
func TestBlockResponseByteLimit(t *testing.T) {
	cfg, key := fixture(t)
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	a, b := start(t, ca, Config{}), start(t, cb, Config{})
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 256; i++ {
			e := event(t, cfg, key, fmtID(batch, i))
			e.Name = strings.Repeat("<", 128)
			e.Unit = strings.Repeat("<", 32)
			e.Location = strings.Repeat("<", 256)
			e.Status = strings.Repeat("<", 64)
			if err := ledger.SignEvent(&e, key); err != nil {
				t.Fatal(err)
			}
			if err := ca.Submit(e); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := ca.Mine(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	info, _ := parsePeer(a.Addrs()[0])
	if err := b.host.Connect(context.Background(), *info); err != nil {
		t.Fatal(err)
	}
	r, err := b.call(context.Background(), a.host.ID(), request{Op: "blocks", Locators: []string{ca.Genesis().Hash}})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(r)
	if len(raw) > MaxFrameBytes || len(r.Blocks) == 0 || len(r.Blocks) >= 3 {
		t.Fatalf("unbounded response: %d bytes %d blocks", len(raw), len(r.Blocks))
	}
}
func fmtID(batch, i int) string {
	return strings.Repeat("<", 120) + string(rune('A'+batch)) + string(rune('a'+i/26)) + string(rune('a'+i%26))
}

func TestApplicationScopedDHT(t *testing.T) {
	cfg, _ := fixture(t)
	a := start(t, openChain(t, cfg), Config{EnableDHT: true})
	b := start(t, openChain(t, cfg), Config{EnableDHT: true, BootstrapPeers: a.Addrs()})
	await(t, func() bool {
		return len(a.Peers()) > 0 && len(b.Peers()) > 0 && a.dht.RoutingTable().Size() > 0 && b.dht.RoutingTable().Size() > 0
	})
	for _, p := range a.host.Mux().Protocols() {
		if strings.HasPrefix(string(p), "/ipfs/kad/") {
			t.Fatal("public IPFS protocol enabled")
		}
	}
}

func TestPersistedCacheAndBootstrapsShareOneBound(t *testing.T) {
	cfg, _ := fixture(t)
	reference := start(t, openChain(t, cfg), Config{})
	dir := t.TempDir()
	cached, bootstrap := []string{}, []string{}
	for i := 0; i < maxCache; i++ {
		cached = append(cached, "/ip4/127.0.0.1/tcp/"+strconv.Itoa(1000+i)+"/p2p/"+reference.ID())
		bootstrap = append(bootstrap, "/ip4/127.0.0.1/tcp/"+strconv.Itoa(2000+i)+"/p2p/"+reference.ID())
	}
	raw, _ := json.Marshal(cached)
	if err := os.WriteFile(filepath.Join(dir, "peers.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	n := start(t, openChain(t, cfg), Config{DataDir: dir, BootstrapPeers: bootstrap})
	n.mu.Lock()
	count := len(n.cache)
	n.mu.Unlock()
	if count > maxCache {
		t.Fatalf("cache expanded to %d", count)
	}
}

func TestPartitionReconnectConflictingCustody(t *testing.T) {
	cfg, supplierKey := fixture(t)
	recipients := make([]ed25519.PrivateKey, 2)
	for i := range recipients {
		seed := make([]byte, 32)
		seed[0] = byte(i + 1)
		recipients[i] = ed25519.NewKeyFromSeed(seed)
		p, err := ledger.NewParticipant("carrier-"+strconv.Itoa(i), ledger.RoleCarrier, recipients[i].Public().(ed25519.PublicKey))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Participants = append(cfg.Participants, p)
	}
	ca, cb := openChain(t, cfg), openChain(t, cfg)
	created := event(t, cfg, supplierKey, "custody")
	if err := ca.Submit(created); err != nil {
		t.Fatal(err)
	}
	base, err := ca.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cb.AddBlock(base); err != nil {
		t.Fatal(err)
	}
	a, b := start(t, ca, Config{}), start(t, cb, Config{})
	sign := func(key ed25519.PrivateKey, e ledger.Event) ledger.Event {
		e.Version = 1
		e.ChainID = cfg.ChainID
		e.Actor = ledger.PublicKeyID(key.Public().(ed25519.PublicKey))
		e.Timestamp = 1
		if err := ledger.SignEvent(&e, key); err != nil {
			t.Fatal(err)
		}
		return e
	}
	var branches [2]ledger.Block
	for i, n := range []*Node{a, b} {
		offer := sign(supplierKey, ledger.Event{Kind: ledger.KindOffer, BatchID: created.BatchID, Recipient: ledger.PublicKeyID(recipients[i].Public().(ed25519.PublicKey)), Sequence: 2, PreviousEvent: created.ID})
		accept := sign(recipients[i], ledger.Event{Kind: ledger.KindAccept, BatchID: created.BatchID, Sequence: 3, PreviousEvent: offer.ID})
		for _, e := range []ledger.Event{offer, accept} {
			if err := n.Submit(context.Background(), e); err != nil {
				t.Fatal(err)
			}
		}
		branches[i], err = n.Mine(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		checkpoint := sign(recipients[i], ledger.Event{Kind: ledger.KindCheckpoint, BatchID: created.BatchID, Sequence: 4, PreviousEvent: accept.ID, Location: "recipient-" + strconv.Itoa(i)})
		if err := n.Submit(context.Background(), checkpoint); err != nil {
			t.Fatal(err)
		}
	}
	// B's additional verified work resolves the disconnected custody dispute.
	winning, err := b.Mine(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	connect(t, a, b)
	await(t, func() bool { return ca.Tip().Hash == winning.Hash && cb.Tip().Hash == winning.Hash })
	wantOwner := ledger.PublicKeyID(recipients[1].Public().(ed25519.PublicKey))
	for _, c := range []*chain.Chain{ca, cb} {
		batch, ok := c.Batch(created.BatchID)
		if !ok || batch.Owner != wantOwner || batch.Location != "recipient-1" {
			t.Fatal("wrong canonical custody", batch)
		}
		trace := c.Trace(created.BatchID)
		if len(trace) != 4 || trace[1].Event.Kind != ledger.KindOffer || trace[2].Event.Actor != wantOwner {
			t.Fatal("trace contains detached custody", trace)
		}
		for _, e := range c.Pending() {
			if e.BatchID == created.BatchID {
				t.Fatal("kept conflicting detached custody", e)
			}
		}
	}
	if !ca.HasBlock(branches[0].Hash) {
		t.Fatal("discarded validated competing branch")
	}
}

func TestInvalidProofFromRealPeerAndDuplicateGossip(t *testing.T) {
	cfg, key := fixture(t)
	c := openChain(t, cfg)
	n := start(t, c, Config{})
	e := event(t, cfg, key, "proof")
	b, err := ledger.Mine(context.Background(), cfg, c.Genesis(), ledger.NewState(), []ledger.Event{e}, c.Genesis().Timestamp)
	if err != nil {
		t.Fatal(err)
	}
	for {
		b.Nonce++
		b.Hash, err = ledger.BlockHash(b)
		if err != nil {
			t.Fatal(err)
		}
		if !ledger.MeetsDifficulty(b.Hash, cfg.Difficulty) {
			break
		}
	}
	bad := bareHost(t)
	bad.SetStreamHandler(n.protocol, func(s network.Stream) {
		defer s.Close()
		s.SetDeadline(time.Now().Add(time.Second))
		var req request
		if readFrame(s, &req) != nil {
			return
		}
		_ = writeFrame(s, response{Hello: n.hello, Blocks: []ledger.Block{b}})
	})
	if err := n.host.Connect(context.Background(), peer.AddrInfo{ID: bad.ID(), Addrs: bad.Addrs()}); err != nil {
		t.Fatal(err)
	}
	if err := n.receiveBlock(context.Background(), bad.ID(), b.Hash); err == nil {
		t.Fatal("accepted invalid proof from network")
	}
	if c.HasBlock(b.Hash) || c.Tip().Height != 0 || len(c.Pending()) != 0 {
		t.Fatal("invalid network block changed ledger")
	}
	if err := n.Submit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := n.publish(context.Background(), n.events, e); err != nil {
			t.Fatal(err)
		}
	}
	conflict := e
	conflict.Location = "conflicting duplicate batch"
	if err := ledger.SignEvent(&conflict, key); err != nil {
		t.Fatal(err)
	}
	if err := n.publish(context.Background(), n.events, conflict); err == nil {
		t.Fatal("relayed invalid state transition")
	}
	if len(c.Pending()) != 1 || c.Pending()[0].ID != e.ID {
		t.Fatal("duplicate/invalid gossip changed pending state")
	}
}
