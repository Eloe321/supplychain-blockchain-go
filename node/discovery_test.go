package node

import (
	"context"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"strconv"
	"testing"
	"time"
)

func TestAdvertisementsPreserveBackoffAndVerifiedConnectResetsIt(t *testing.T) {
	cfg, _ := fixture(t)
	n := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	other := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	unavailable := "/ip4/127.0.0.1/tcp/1/p2p/" + other.ID()
	advanced := retryState{delay: 32 * time.Second, next: time.Now().Add(time.Minute)}
	n.remember(unavailable)
	n.mu.Lock()
	n.cache[unavailable] = advanced
	n.mu.Unlock()
	for i := 0; i < 10; i++ {
		n.remember(unavailable)
	}
	n.mu.Lock()
	got := n.cache[unavailable]
	n.mu.Unlock()
	if got != advanced {
		t.Fatalf("advertisements erased failed-dial backoff: got %+v, want %+v", got, advanced)
	}
	raw := other.Addrs()[0]
	n.remember(raw)
	n.mu.Lock()
	n.cache[raw] = advanced
	n.mu.Unlock()
	if err := n.Connect(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	got = n.cache[raw]
	unavailableState := n.cache[unavailable]
	n.mu.Unlock()
	if got != (retryState{}) {
		t.Fatalf("verified connection did not reset retry state: %+v", got)
	}
	if unavailableState != advanced {
		t.Fatal("connection reset another address's failed-dial backoff")
	}
}

func TestDisconnectedOutboundOnlyAccountingIsPruned(t *testing.T) {
	cfg, _ := fixture(t)
	n := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	for i := 0; i < 8; i++ {
		remote := bareHost(t)
		remote.SetStreamHandler(n.protocol, func(s network.Stream) {
			defer s.Close()
			var req request
			if readFrame(s, &req) != nil {
				return
			}
			status := n.chain.Status()
			_ = writeFrame(s, response{Hello: n.hello, Status: &status})
		})
		if err := n.host.Connect(context.Background(), peer.AddrInfo{ID: remote.ID(), Addrs: remote.Addrs()}); err != nil {
			t.Fatal(err)
		}
		if _, err := n.call(context.Background(), remote.ID(), request{Op: "status"}); err != nil {
			t.Fatal(err)
		}
		n.mu.Lock()
		_, hasLimiter := n.sendLimits[remote.ID()]
		_, hasInboundRate := n.rates[remote.ID()]
		n.cursors[remote.ID()] = n.chain.Genesis().Hash
		n.mu.Unlock()
		if !hasLimiter || hasInboundRate {
			t.Fatal("fixture was not outbound-only accounting")
		}
		if err := n.host.Network().ClosePeer(remote.ID()); err != nil {
			t.Fatal(err)
		}
		remote.Close()
		await(t, func() bool { return !connected(n.host.Network().Connectedness(remote.ID())) })
		n.maintenance()
		n.mu.Lock()
		_, hasLimiter = n.sendLimits[remote.ID()]
		_, hasCursor := n.cursors[remote.ID()]
		limiterCount, cursorCount := len(n.sendLimits), len(n.cursors)
		n.mu.Unlock()
		if hasLimiter || hasCursor || limiterCount != 0 || cursorCount != 0 {
			t.Fatalf("peer churn leaked accounting: limiters=%d cursors=%d", limiterCount, cursorCount)
		}
	}
}

func TestDiscoveryTriesAlternativeAddresses(t *testing.T) {
	cfg, _ := fixture(t)
	n := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour, RequestTimeout: 500 * time.Millisecond})
	other := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	info, err := parsePeer(other.Addrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	refused, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	if err != nil {
		t.Fatal(err)
	}
	info.Addrs = append([]ma.Multiaddr{refused}, info.Addrs...)
	n.HandlePeerFound(*info)
	await(t, func() bool {
		if !n.isApproved(other.host.ID()) {
			return false
		}
		n.mu.Lock()
		state, exists := n.cache[other.Addrs()[0]]
		n.mu.Unlock()
		return exists && state == (retryState{})
	})
}

func TestRepeatedDiscoveryHonorsFutureRetryAndExplicitConnectResets(t *testing.T) {
	cfg, _ := fixture(t)
	n := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour, RequestTimeout: 500 * time.Millisecond})
	other := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	raw := other.Addrs()[0]
	info, err := parsePeer(raw)
	if err != nil {
		t.Fatal(err)
	}
	advanced := retryState{delay: 32 * time.Second, next: time.Now().Add(time.Minute)}
	n.mu.Lock()
	n.cache[raw] = advanced
	n.mu.Unlock()
	for i := 0; i < 10; i++ {
		n.HandlePeerFound(*info)
	}
	// Queue independent reachable candidates as worker barriers instead of sleeping.
	barrier := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
	barrierInfo, err := parsePeer(barrier.Addrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	n.HandlePeerFound(*barrierInfo)
	await(t, func() bool { return n.isApproved(barrier.host.ID()) && len(n.candidates) == 0 })
	if connected(n.host.Network().Connectedness(other.host.ID())) {
		t.Fatal("discovery bypassed future retry deadline")
	}
	n.mu.Lock()
	got := n.cache[raw]
	n.mu.Unlock()
	if got != advanced {
		t.Fatalf("discovery changed protected backoff: %+v", got)
	}
	if err := n.Connect(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	got = n.cache[raw]
	n.mu.Unlock()
	if got != (retryState{}) {
		t.Fatal("explicit verified connection did not reset retry state")
	}
}

func TestFullStaleCacheAdmitsFreshDiscoveryAndProtectsConnectedRoute(t *testing.T) {
	for _, keepConnected := range []bool{false, true} {
		t.Run(strconv.FormatBool(keepConnected), func(t *testing.T) {
			cfg, _ := fixture(t)
			n := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
			live := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
			fresh := start(t, openChain(t, cfg), Config{SyncInterval: time.Hour})
			stale := bareHost(t)
			if keepConnected {
				connect(t, n, live)
			}
			liveRaw := live.Addrs()[0]
			n.mu.Lock()
			n.cache = map[string]retryState{}
			staleCount := maxCache
			if keepConnected {
				n.cache[liveRaw] = retryState{}
				staleCount--
			}
			n.cfg.BootstrapPeers = nil
			for i := 0; i < staleCount; i++ {
				raw := "/ip4/127.0.0.1/tcp/" + strconv.Itoa(1000+i) + "/p2p/" + stale.ID().String()
				n.cache[raw] = retryState{delay: time.Minute, next: time.Now().Add(time.Hour)}
				// Every disconnected route is configured: none may permanently block recovery.
				n.cfg.BootstrapPeers = append(n.cfg.BootstrapPeers, raw)
			}
			n.mu.Unlock()
			stale.Close()
			info, err := parsePeer(fresh.Addrs()[0])
			if err != nil {
				t.Fatal(err)
			}
			n.HandlePeerFound(*info)
			await(t, func() bool { return n.isApproved(fresh.host.ID()) })
			n.mu.Lock()
			count := len(n.cache)
			_, keptLive := n.cache[liveRaw]
			_, keptFresh := n.cache[fresh.Addrs()[0]]
			n.mu.Unlock()
			if count > maxCache || (keepConnected && !keptLive) || !keptFresh {
				t.Fatalf("cache admission violated bounds/protection: count=%d live=%v fresh=%v", count, keptLive, keptFresh)
			}
			await(t, func() bool { n.mu.Lock(); defer n.mu.Unlock(); return n.cache[fresh.Addrs()[0]] == (retryState{}) })
		})
	}
}
