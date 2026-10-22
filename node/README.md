# P2P node

`New(ctx, chain, Config)` starts a node and returns `(*Node, error)`; `Start` is an alias. The caller owns the chain. Canceling the parent context closes network resources automatically. `Close() error` is idempotent and waits for node workers and incoming request handlers. Close the node before closing its chain.

The public methods are:

```go
ID() string                         // libp2p PeerID, separate from organization keys
Addrs() []string                    // complete /p2p multiaddresses
Peers() []string                    // approved connected PeerIDs, including limited circuits
Connect(context.Context, string) error
Submit(context.Context, ledger.Event) error
Mine(context.Context) (ledger.Block, error)
Sync(context.Context) error
Close() error
```

`Submit` validates and stores a signed event before publishing. `Mine` allows one concurrent mining operation per node; it can mine an empty block and announces the resulting validated block. There is no automatic mining loop. Sync retries connected peers and uses locally validated fork choice, never remote height or work. A publish error can occur after local submission or mining succeeded; callers can inspect their chain before retrying.

`Config` contains:

| Field | Default / meaning |
| --- | --- |
| `DataDir string` | Required; stores `identity.key` (0600) and `peers.json` (0600). |
| `ListenAddrs []string` | Nil: `/ip4/0.0.0.0/tcp/0`; empty slice: no listeners. |
| `BootstrapPeers []string` | Complete `/p2p` addresses; up to 256, no public defaults. |
| `AnnounceAddrs []string` | Explicit reachable addresses; up to 16, replaces detected announcement addresses. |
| `RelayPeers []string` | Up to 16 complete addresses of reachable static Circuit Relay v2 services. Enables client AutoRelay. |
| `EnableMDNS bool` | Opt-in LAN discovery, with a chain-specific DNS-SD service name. |
| `EnableDHT bool` | Opt-in Kademlia routing and rendezvous advertisement/search under a configuration-specific protocol prefix. Only configured/cached/discovered peers seed routing. |
| `EnableRelayService bool` | Opt-in relay service; 32 reservations, eight circuits per peer, two-minute/16 MiB limits per relayed connection. |
| `EnableHolePunching bool` | Opt-in coordinated hole punching; default off. |
| `ForcePublic bool` | Explicitly declare public reachability, e.g. a reachable static relay service. |
| `ForcePrivate bool` | Explicitly declare private reachability and request static relay reservations even on a local network. Mutually exclusive with `ForcePublic`. |
| `SyncInterval time.Duration` | 3 seconds; allowed 50 milliseconds through one hour. |
| `RequestTimeout time.Duration` | 5 seconds; allowed 50 milliseconds through one minute. |
| `MaxPeers int` | 64; allowed 1–64; this is also a hard total transport connection limit. |
| `ProtocolVersion string` | Empty means `"1"`; other versions are rejected. |

TCP, Noise and yamux provide authenticated encrypted transport. libp2p's AutoNAT reachability detection and NAT port mapping remain enabled. Organization authorization comes exclusively from the genesis participant registry. A successful transport connection alone does not approve ledger exchange: every request carries a strict protocol/configuration/genesis/chain handshake. Gossip validators require the immediate forwarding peer to have completed that handshake and verify event signatures even for known event IDs.

Frames are four-byte big-endian lengths followed by strict, single-value JSON. The receiver rejects sizes above 2 MiB before allocating payload storage. Requests have read/write deadlines, eight concurrent incoming and eight outgoing slots, per-peer inbound throttling, and paced outgoing requests. Sync has two concurrent slots, a 30-second session budget and at most 32 batches per pass. Each block batch contains at most 32 blocks and is trimmed to the 2 MiB response cap. Per-peer validated cursors let a lower-work branch continue across passes. Pending inventory includes all up to 4096 IDs; missing events are fetched in dependency order in batches of 32, so a known prefix does not starve later events. The peer cache has one shared 256-address bound across configured and persisted entries. Fresh discovery may evict a disconnected route, preferring failed nonconfigured routes while protecting normal/limited connected peers; stale configured routes can be replaced when necessary to restore connectivity. Peer exchange returns at most 64 addresses, and reconnect delay grows from one second to one minute. Automatic discovery and reconnect callbacks share the same retry claims; a repeated advertisement cannot bypass a future retry deadline. Each candidate supplies all due addresses to the libp2p dialer under one request-timeout budget. Explicit `Connect` may intentionally bypass backoff, and a verified successful connection resets the address retry state. Gossip validator, stream, worker, subscription and outgoing peer queues are bounded.

For a WAN node, listen on an appropriate local interface and forward its TCP port, then set `AnnounceAddrs` to the reachable public multiaddress. Supply several independent `BootstrapPeers`. For a relay deployment, run a reachable service with `EnableRelayService` and an accurate public announcement; use `ForcePublic` only when reachability is known. Private clients set `RelayPeers`, optionally `ForcePrivate`, and may opt into hole punching. AutoRelay reserves and refreshes circuits. libp2p deliberately excludes loopback relay addresses from its public announcements; the local integration test constructs its loopback circuit path explicitly.

GossipSub v0.18 excludes resource-limited relay connections from its mesh. Such relay-only nodes exchange verified blocks and pending events through the same periodic request/sync protocol over Circuit Relay v2. Direct connections established by normal dialing or optional hole punching carry gossip. NAT traversal, address reachability and successful hole punching depend on the deployment.

Tests use real localhost libp2p hosts and cover multi-hop gossip independently of periodic sync, bootstrap loss, restart identity/offline catch-up, more than 32 blocks, equal-height and longer competing forks, pending dependencies, strict malformed/oversized/stalled traffic, byte-bounded block responses, incompatible handshakes, local scoped DHT routing, parent-context cleanup, and a real limited relay circuit with block and pending-event sync. Public WAN routing and NAT/hole punching across physical hosts have not been verified. A separate WAN check should start nodes on independent networks, confirm their announced addresses and approved peers, submit and mine a signed event, disconnect/reconnect one node, and compare validated tips and batch histories after catch-up.
