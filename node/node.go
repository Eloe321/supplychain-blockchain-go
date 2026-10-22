package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
	"supplychain/chain"
	"supplychain/ledger"
)

var ErrClosed = errors.New("node closed")
var ErrWrongNetwork = errors.New("incompatible network handshake")

type rateWindow struct {
	start time.Time
	count int
}
type retryState struct {
	next  time.Time
	delay time.Duration
}

// Node owns network resources, never the caller's Chain. New starts networking.
type Node struct {
	cfg                Config
	chain              *chain.Chain
	host               host.Host
	ctx                context.Context
	cancel             context.CancelFunc
	hello              hello
	protocol           protocol.ID
	scope              string
	pubsub             *pubsub.PubSub
	events, blocks     *pubsub.Topic
	eventSub, blockSub *pubsub.Subscription
	dht                *dht.IpfsDHT
	mdns               mdns.Service
	wg                 sync.WaitGroup
	handlers           sync.WaitGroup
	once               sync.Once
	closeErr           error
	mu                 sync.Mutex
	approved           map[peer.ID]bool
	rates              map[peer.ID]rateWindow
	syncing            map[peer.ID]bool
	cursors            map[peer.ID]string
	sendLimits         map[peer.ID]*rate.Limiter
	cache              map[string]retryState
	requests           chan struct{}
	incoming           chan struct{}
	syncSlots          chan struct{}
	mining             chan struct{}
	triggers           chan peer.ID
	candidates         chan peer.AddrInfo
}

func New(ctx context.Context, c *chain.Chain, cfg Config) (*Node, error) {
	if c == nil {
		return nil, errors.New("nil chain")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	key, err := loadIdentity(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	hash, err := ledger.ConfigHash(c.Config())
	if err != nil {
		return nil, err
	}
	manager, err := connmgr.NewConnManager(max(1, cfg.MaxPeers/2), cfg.MaxPeers, connmgr.WithGracePeriod(time.Second))
	if err != nil {
		return nil, err
	}
	limits := rcmgr.PartialLimitConfig{System: rcmgr.ResourceLimits{Conns: rcmgr.LimitVal(cfg.MaxPeers), ConnsInbound: rcmgr.LimitVal(cfg.MaxPeers), ConnsOutbound: rcmgr.LimitVal(cfg.MaxPeers), Streams: 256, StreamsInbound: 128, StreamsOutbound: 128, Memory: rcmgr.LimitVal64(64 << 20)}, PeerDefault: rcmgr.ResourceLimits{Conns: 2, Streams: 32, StreamsInbound: 16, StreamsOutbound: 16}}
	rm, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Build(rcmgr.DefaultLimits.AutoScale())))
	if err != nil {
		manager.Close()
		return nil, err
	}
	opts := []libp2p.Option{libp2p.Identity(key), libp2p.NoTransports, libp2p.Transport(tcp.NewTCPTransport), libp2p.Security(noise.ID, noise.New), libp2p.Muxer(yamux.ID, yamux.DefaultTransport), libp2p.ListenAddrStrings(cfg.ListenAddrs...), libp2p.ConnectionManager(manager), libp2p.ResourceManager(rm), libp2p.EnableRelay(), libp2p.NATPortMap(), libp2p.WithDialTimeout(cfg.RequestTimeout)}
	if len(cfg.ListenAddrs) == 0 {
		opts = append(opts, libp2p.NoListenAddrs)
	}
	if len(cfg.AnnounceAddrs) > 0 {
		announced := make([]ma.Multiaddr, 0, len(cfg.AnnounceAddrs))
		for _, raw := range cfg.AnnounceAddrs {
			a, _ := ma.NewMultiaddr(raw)
			announced = append(announced, a)
		}
		opts = append(opts, libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr { return append([]ma.Multiaddr{}, announced...) }))
	}
	if cfg.ForcePublic {
		opts = append(opts, libp2p.ForceReachabilityPublic())
	}
	if cfg.ForcePrivate {
		opts = append(opts, libp2p.ForceReachabilityPrivate())
	}
	if cfg.EnableRelayService {
		resources := relay.DefaultResources()
		resources.MaxReservations = 32
		resources.MaxCircuits = 8
		resources.Limit = &relay.RelayLimit{Duration: 2 * time.Minute, Data: 16 << 20}
		opts = append(opts, libp2p.EnableRelayService(relay.WithResources(resources)))
	}
	if len(cfg.RelayPeers) > 0 {
		infos := []peer.AddrInfo{}
		for _, raw := range cfg.RelayPeers {
			p, _ := parsePeer(raw)
			infos = append(infos, *p)
		}
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(infos))
	}
	if cfg.EnableHolePunching {
		opts = append(opts, libp2p.EnableHolePunching())
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		rm.Close()
		manager.Close()
		return nil, err
	}
	run, cancel := context.WithCancel(ctx)
	n := &Node{cfg: cfg, chain: c, host: h, ctx: run, cancel: cancel, hello: hello{Version: ProtocolVersion, ChainID: c.Config().ChainID, ConfigHash: hash, GenesisHash: c.Genesis().Hash}, scope: "/supplychain/" + hash, approved: map[peer.ID]bool{}, rates: map[peer.ID]rateWindow{}, syncing: map[peer.ID]bool{}, cursors: map[peer.ID]string{}, sendLimits: map[peer.ID]*rate.Limiter{}, cache: map[string]retryState{}, requests: make(chan struct{}, 8), incoming: make(chan struct{}, 8), syncSlots: make(chan struct{}, 2), mining: make(chan struct{}, 1), triggers: make(chan peer.ID, 64), candidates: make(chan peer.AddrInfo, 64)}
	n.protocol = protocol.ID(n.scope + "/sync/1")
	for _, raw := range cfg.BootstrapPeers {
		n.cache[raw] = retryState{}
	}
	n.loadCache()
	n.saveCacheLocked()
	h.SetStreamHandler(n.protocol, n.handleStream)
	if err = n.setupGossip(); err != nil {
		n.cancel()
		h.Close()
		return nil, err
	}
	if err = n.setupDiscovery(); err != nil {
		n.Close()
		return nil, err
	}
	h.Network().Notify(&network.NotifyBundle{ConnectedF: func(_ network.Network, c network.Conn) { n.trigger(c.RemotePeer()) }, DisconnectedF: func(_ network.Network, c network.Conn) {
		n.mu.Lock()
		delete(n.approved, c.RemotePeer())
		delete(n.rates, c.RemotePeer())
		n.mu.Unlock()
	}})
	n.wg.Add(5)
	go n.drain(n.eventSub)
	go n.drain(n.blockSub)
	go n.worker()
	go n.worker()
	go n.maintain()
	go func() { <-n.ctx.Done(); _ = n.Close() }()
	return n, nil
}
func Start(ctx context.Context, c *chain.Chain, cfg Config) (*Node, error) { return New(ctx, c, cfg) }
func (n *Node) ID() string                                                 { return n.host.ID().String() }
func (n *Node) Addrs() []string {
	suffix, _ := ma.NewMultiaddr("/p2p/" + n.ID())
	out := []string{}
	for _, a := range n.host.Addrs() {
		out = append(out, a.Encapsulate(suffix).String())
	}
	sort.Strings(out)
	return out
}
func (n *Node) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := []string{}
	for p, ok := range n.approved {
		if ok && connected(n.host.Network().Connectedness(p)) {
			out = append(out, p.String())
		}
	}
	sort.Strings(out)
	return out
}
func (n *Node) Connect(ctx context.Context, raw string) error {
	if n.ctx.Err() != nil {
		return ErrClosed
	}
	info, err := parsePeer(raw)
	if err != nil {
		return err
	}
	if err := n.connectPeer(ctx, *info); err != nil {
		return err
	}
	n.connectionSucceeded(raw)
	return nil
}

// connectPeer performs one transport dial plus the network handshake. Automatic
// callers supply all due addresses and one timeout for the entire operation.
func (n *Node) connectPeer(ctx context.Context, info peer.AddrInfo) error {
	if n.ctx.Err() != nil {
		return ErrClosed
	}
	if info.ID == n.host.ID() {
		return nil
	}
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	if err := n.host.Connect(ctx, info); err != nil {
		return err
	}
	if _, err := n.call(ctx, info.ID, request{Op: "status"}); err != nil {
		n.host.Network().ClosePeer(info.ID)
		return err
	}
	n.trigger(info.ID)
	return nil
}

func (n *Node) connectionSucceeded(raw string) {
	n.remember(raw)
	n.mu.Lock()
	if _, exists := n.cache[raw]; exists {
		n.cache[raw] = retryState{}
	}
	n.mu.Unlock()
}

func (n *Node) Submit(ctx context.Context, e ledger.Event) error {
	if n.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.chain.Submit(e); err != nil {
		return err
	}
	return n.publish(ctx, n.events, e)
}
func (n *Node) Mine(ctx context.Context) (ledger.Block, error) {
	if n.ctx.Err() != nil {
		return ledger.Block{}, ErrClosed
	}
	select {
	case n.mining <- struct{}{}:
		defer func() { <-n.mining }()
	case <-ctx.Done():
		return ledger.Block{}, ctx.Err()
	case <-n.ctx.Done():
		return ledger.Block{}, ErrClosed
	}
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	b, err := n.chain.Mine(ctx)
	if err != nil {
		return b, err
	}
	return b, n.publish(ctx, n.blocks, announcement{Hash: b.Hash})
}
func (n *Node) Close() error {
	n.once.Do(func() {
		n.cancel()
		if n.mdns != nil {
			n.mdns.Close()
		}
		if n.dht != nil {
			n.dht.Close()
		}
		if n.eventSub != nil {
			n.eventSub.Cancel()
		}
		if n.blockSub != nil {
			n.blockSub.Cancel()
		}
		if n.events != nil {
			n.events.Close()
		}
		if n.blocks != nil {
			n.blocks.Close()
		}
		n.closeErr = n.host.Close()
		n.mu.Lock()
		n.mu.Unlock()
		n.handlers.Wait()
		n.wg.Wait()
	})
	return n.closeErr
}
func (n *Node) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	op, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(n.ctx, cancel)
	return op, func() { stop(); cancel() }
}
func (n *Node) trigger(p peer.ID) {
	select {
	case n.triggers <- p:
	default:
	}
}
func (n *Node) worker() {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case p := <-n.triggers:
			if connected(n.host.Network().Connectedness(p)) {
				_ = n.syncPeer(n.ctx, p)
			}
		case info := <-n.candidates:
			n.connectCandidate(info)
		}
	}
}
func (n *Node) drain(sub *pubsub.Subscription) {
	defer n.wg.Done()
	for {
		if _, err := sub.Next(n.ctx); err != nil {
			return
		}
	}
}
func (n *Node) String() string { return fmt.Sprintf("node %s", n.ID()) }

func connected(c network.Connectedness) bool { return c == network.Connected || c == network.Limited }
