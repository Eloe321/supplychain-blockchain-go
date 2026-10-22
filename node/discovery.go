package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	routing "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	ma "github.com/multiformats/go-multiaddr"
)

const maxCache = 256

func peerAddresses(info peer.AddrInfo) []string {
	addrs, err := peer.AddrInfoToP2pAddrs(&info)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, a := range addrs {
		out = append(out, a.String())
	}
	return out
}
func (n *Node) loadCache() {
	raw, err := readBounded(filepath.Join(n.cfg.DataDir, "peers.json"), 600*1024)
	if err != nil {
		return
	}
	var peers []string
	if decodeStrict(raw, &peers) != nil || len(peers) > maxCache {
		return
	}
	for _, raw := range peers {
		if p, err := parsePeer(raw); err == nil && p.ID != n.host.ID() && len(n.cache) < maxCache {
			n.cache[raw] = retryState{}
		}
	}
}
func (n *Node) remember(raw string) {
	info, err := parsePeer(raw)
	if err != nil || info.ID == n.host.ID() {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.cache[raw]; exists {
		// Advertisements are not evidence of reachability. Preserve failed-dial
		// backoff even when the same address is repeatedly exchanged.
		return
	}
	if !n.admitCacheLocked(raw, nil) {
		return
	}
	n.saveCacheLocked()
}

// admitCacheLocked keeps one shared bound for routes and retry state. Live
// normal/limited peers are protected. Discovery may replace a disconnected
// configured route when stale bootstraps otherwise occupy the entire cache.
// protected prevents addresses in one current candidate from evicting each other.
func (n *Node) admitCacheLocked(raw string, protected map[string]bool) bool {
	if _, exists := n.cache[raw]; exists {
		return true
	}
	if len(n.cache) >= maxCache {
		configured := map[string]bool{}
		for _, address := range n.cfg.BootstrapPeers {
			configured[address] = true
		}
		victim := ""
		var chosen retryState
		chosenConfigured := false
		for address, state := range n.cache {
			if protected[address] {
				continue
			}
			info, err := parsePeer(address)
			if err == nil && connected(n.host.Network().Connectedness(info.ID)) {
				continue
			}
			isConfigured := configured[address]
			prefer := victim == "" || (!isConfigured && chosenConfigured)
			if isConfigured == chosenConfigured {
				prefer = prefer || state.delay > chosen.delay || (state.delay == chosen.delay && (state.next.Before(chosen.next) || (state.next.Equal(chosen.next) && address < victim)))
			}
			if prefer {
				victim = address
				chosen = state
				chosenConfigured = isConfigured
			}
		}
		if victim == "" {
			return false
		}
		delete(n.cache, victim)
	}
	n.cache[raw] = retryState{}
	return true
}

// saveCacheLocked is called before publishing the host or while holding mu.
func (n *Node) saveCacheLocked() {
	peers := make([]string, 0, len(n.cache))
	for a := range n.cache {
		peers = append(peers, a)
	}
	sort.Strings(peers)
	encoded, _ := json.Marshal(peers)
	path := filepath.Join(n.cfg.DataDir, "peers.json")
	f, err := os.CreateTemp(n.cfg.DataDir, ".peers-*")
	if err != nil {
		return
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(encoded); err != nil {
		f.Close()
		return
	}
	if f.Close() != nil {
		return
	}
	_ = os.Rename(f.Name(), path)
}
func (n *Node) peerExchange() []string {
	out := n.Addrs()
	n.mu.Lock()
	defer n.mu.Unlock()
	for raw := range n.cache {
		if len(out) >= maxAddresses {
			break
		}
		out = append(out, raw)
	}
	if len(out) > maxAddresses {
		out = out[:maxAddresses]
	}
	return out
}
func (n *Node) maintain() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.SyncInterval)
	defer ticker.Stop()
	n.maintenance()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.maintenance()
		}
	}
}
func (n *Node) maintenance() {
	for _, p := range n.host.Network().Peers() {
		n.trigger(p)
		if n.isApproved(p) {
			info := peer.AddrInfo{ID: p, Addrs: n.host.Peerstore().Addrs(p)}
			for _, raw := range peerAddresses(info) {
				n.remember(raw)
			}
		}
	}
	n.mu.Lock()
	now := time.Now()
	attempts := []peer.AddrInfo{}
	positions := map[peer.ID]int{}
	for raw, r := range n.cache {
		info, err := parsePeer(raw)
		if err != nil || connected(n.host.Network().Connectedness(info.ID)) || now.Before(r.next) {
			continue
		}
		if index, ok := positions[info.ID]; ok {
			if len(attempts[index].Addrs) < maxAddresses {
				attempts[index].Addrs = append(attempts[index].Addrs, info.Addrs...)
			}
			continue
		}
		if len(attempts) >= 8 {
			continue
		}
		positions[info.ID] = len(attempts)
		attempts = append(attempts, *info)
	}
	// Remove disconnected peer accounting so a long-lived node has bounded maps.
	for p := range n.approved {
		if !connected(n.host.Network().Connectedness(p)) {
			delete(n.approved, p)
		}
	}
	for p := range n.rates {
		if !connected(n.host.Network().Connectedness(p)) {
			delete(n.rates, p)
		}
	}
	// Outbound-only peers have no inbound rate entry, and the disconnect
	// notifier may already have removed one. Prune each map independently.
	for p := range n.sendLimits {
		if !connected(n.host.Network().Connectedness(p)) {
			delete(n.sendLimits, p)
		}
	}
	for p := range n.cursors {
		if !connected(n.host.Network().Connectedness(p)) {
			delete(n.cursors, p)
		}
	}
	n.mu.Unlock()
	for _, info := range attempts {
		n.HandlePeerFound(info)
	}
}

// HandlePeerFound only queues candidates. The worker claims retry deadlines for
// discovery and maintenance alike, so repeated callbacks cannot bypass backoff.
func (n *Node) HandlePeerFound(info peer.AddrInfo) {
	if len(info.Addrs) > maxAddresses {
		info.Addrs = info.Addrs[:maxAddresses]
	}
	info.Addrs = append([]ma.Multiaddr{}, info.Addrs...)
	select {
	case n.candidates <- info:
	default:
	}
}

func (n *Node) connectCandidate(info peer.AddrInfo) {
	if info.ID == n.host.ID() || connected(n.host.Network().Connectedness(info.ID)) {
		return
	}
	eligible := peer.AddrInfo{ID: info.ID}
	candidateAddresses := peerAddresses(info)
	protected := make(map[string]bool, len(candidateAddresses))
	for _, raw := range candidateAddresses {
		protected[raw] = true
	}
	added := false
	n.mu.Lock()
	now := time.Now()
	for _, raw := range candidateAddresses {
		parsed, err := parsePeer(raw)
		if err != nil {
			continue
		}
		r, exists := n.cache[raw]
		if !exists {
			if !n.admitCacheLocked(raw, protected) {
				continue
			}
			added = true
		}
		if now.Before(r.next) {
			continue
		}
		if r.delay == 0 {
			r.delay = time.Second
		} else {
			r.delay = min(2*r.delay, time.Minute)
		}
		r.next = now.Add(r.delay)
		n.cache[raw] = r
		eligible.Addrs = append(eligible.Addrs, parsed.Addrs...)
	}
	if added {
		n.saveCacheLocked()
	}
	n.mu.Unlock()
	if len(eligible.Addrs) == 0 {
		return
	}
	// The dialer races/falls back across all due addresses under one operation
	// budget, rather than granting every bad address a fresh timeout.
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RequestTimeout)
	defer cancel()
	if n.connectPeer(ctx, eligible) != nil {
		return
	}
	for _, conn := range n.host.Network().ConnsToPeer(info.ID) {
		for _, raw := range peerAddresses(peer.AddrInfo{ID: info.ID, Addrs: []ma.Multiaddr{conn.RemoteMultiaddr()}}) {
			n.connectionSucceeded(raw)
		}
	}
	// DNS addresses can resolve to a different transport address. A single
	// supplied address is still proven reachable by the verified connection.
	if len(eligible.Addrs) == 1 {
		for _, raw := range peerAddresses(eligible) {
			n.connectionSucceeded(raw)
		}
	}
}

func (n *Node) setupDiscovery() error {
	if n.cfg.EnableMDNS {
		n.mdns = mdns.NewMdnsService(n.host, "_sc"+n.hello.ConfigHash[:10]+"._udp", n)
		if err := n.mdns.Start(); err != nil {
			return err
		}
	}
	if n.cfg.EnableDHT {
		var err error
		n.dht, err = dht.New(n.host, dht.ProtocolPrefix(protocol.ID(n.scope+"/kad")), dht.Mode(dht.ModeServer), dht.BootstrapPeers())
		if err != nil {
			return err
		}
		if err = n.dht.Bootstrap(n.ctx); err != nil {
			return err
		}
		n.wg.Add(1)
		go n.discoverDHT()
	}
	return nil
}
func (n *Node) discoverDHT() {
	defer n.wg.Done()
	rd := routing.NewRoutingDiscovery(n.dht)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RequestTimeout)
		_, _ = rd.Advertise(ctx, n.scope)
		peers, err := rd.FindPeers(ctx, n.scope, discovery.Limit(maxAddresses))
		if err == nil {
			for info := range peers {
				if info.ID != n.host.ID() {
					n.HandlePeerFound(info)
				}
			}
		}
		cancel()
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
