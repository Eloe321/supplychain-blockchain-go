# Protocol v1

This document describes this repository's custom educational protocol. Wire interoperability requires the same normalized ledger configuration and these exact consensus encodings. It is not a Bitcoin or Ethereum implementation.

## Consensus and signed actions

Configuration JSON has `chain_id`, integer `difficulty` (1–24), and participant objects with `id`, `name`, `role`, `public_key`. Normalization validates and sorts participants by ID. Config hash is SHA-256 over `supplychain/config/v1` followed by a zero byte and Go's fixed-struct JSON encoding. Genesis derives from that normalized configuration and a fixed timestamp of 1700000000; every honest node builds the same anchor.

Actor IDs are SHA-256 of raw 32-byte Ed25519 public keys; public keys/IDs/signatures use lowercase hex. `ledger.EventPayload` is the authoritative event signing encoder: a fixed-field JSON struct with domain `supplychain/event/v1` and every public event field except `id` and `signature`. Event ID is SHA-256 of those bytes; signature is Ed25519 over the same bytes. Unknown actors, changed payloads, invalid UTF-8, invalid action fields and mismatched event versions/chain IDs are rejected. The explicit JSON fields and object shapes are listed in [README](../README.md#http-api).

A create event has sequence 1, no predecessor, positive fixed quantity/name/unit and an enrolled supplier/manufacturer actor. Other actions require exact previous event ID and sequence increment. Checkpoint belongs to the owner. Offer belongs to the owner and names an enrolled distinct recipient; the owner remains owner until recipient accept. During an offer, recipient accept or owner cancel is required before other actions. Transfer events cannot carry checkpoint metadata, and only create carries name/unit/quantity. Human batch IDs can contain any valid UTF-8 up to 128 bytes; actor-provided document hashes reference off-chain contents.

`ledger/block.go` is the authoritative fixed-field header, Merkle and proof encoder. Header domain is `supplychain/block/v1`; its hash is SHA-256 and must have at least configured difficulty leading zero bits. Parent, height, chain, version, fixed difficulty, event bounds, event validity and Merkle root are verified before insertion. Each parent has its own state snapshot. Timestamps cannot decrease along a branch; actor event timestamps do not order custody. Genesis adds zero work; later blocks add `2^difficulty`. Highest cumulative work wins; equal work selects the smaller hex tip hash. Only a validated branch can become canonical.

Canonical traces count an event's own block as one confirmation. Reorganization changes trace/state atomically and revalidates detached/pending candidates. Pending admission checks the canonical state plus ordered pending predecessors. Mining includes a valid prefix bounded by both block event count and encoded byte size; leftover pending events stay queued. Pending events are not persisted.

## Transport and handshake

Each node has a durable libp2p transport key independent from all organization signing keys. TCP + Noise + yamux supplies authenticated encrypted peer streams. Knowing a PeerID authenticates a transport endpoint; event authorization comes from the static actor registry.

Let `H` be normalized configuration hash. The request protocol is `/supplychain/H/sync/1`; GossipSub topics are `/supplychain/H/events/1` and `/supplychain/H/blocks/1`. Scoped Kademlia uses `/supplychain/H/kad`; rendezvous and mDNS are likewise configuration-scoped. Protocol version is string `"1"`.

Every request and response contains `hello`:

```json
{"version":"1","chain_id":"coffee-network","config_hash":"64 lowercase hex","genesis_hash":"64 lowercase hex"}
```

All four values must match locally derived values. Peers that pass this application handshake enter the approved set; a transport connection alone is insufficient. Gossip validators require the immediate forwarding peer to be approved. They verify event signatures even for already-known IDs, preventing a forged duplicate from bypassing validation.

Frames have a **four-byte unsigned big-endian byte length**, then exactly that many UTF-8 JSON bytes. Length zero or above 2 MiB is rejected before payload allocation. Strict decoders reject unknown fields and additional JSON values. `request` fields are `hello`, `op`, optional `locators` and optional `ids`. Responses contain `hello`, optional `error` and the operation's optional `status`, `blocks`, `ids`, `events` or `peers`.

| Operation | Request | Response |
| --- | --- | --- |
| `status` | No payload fields | Canonical status; height/work are hints only |
| `blocks` | Tip-first `locators` | Up to 32 canonical successor blocks, parent first, trimmed to frame cap |
| `block` | One block hash in `ids` | One locally known validated block, including side branches |
| `pending` | No payload fields | Up to 4096 ordered pending event IDs |
| `events` | Up to 32 requested event IDs | Known pending signed events in pending/dependency order |
| `peers` | No payload fields | Up to 64 complete peer multiaddresses |

Unknown operations, incompatible hello and invalid request bounds fail. Read/write stream deadlines default to five seconds. Incoming streams, outgoing requests and sync sessions have separate bounded concurrency, with per-peer throttling and paced requests. Context cancellation resets in-flight streams and closes node workers. Parent cancellation closes network resources automatically; callers still own and close their chain.

## Propagation and convergence

A local `Submit` validates/adopts its signed event before publishing it. A local `Mine` finds/inserts a verified block before publishing its hash announcement. Publication can fail after local success; callers inspect local status before retrying. A received event is independently validated before pool insertion. A block announcement triggers bounded verified fetch/sync; remote work and height never override validation or local fork choice.

Periodic anti-entropy defaults to three seconds and also follows approved connection/gossip triggers. A sync pass has a 30-second context and at most 32 batches. Exponential-backoff locators find shared canonical ancestry. Locally validated cursors permit a lower-work branch to continue across passes and eventually compete. Unknown parent handling triggers fresh ancestry discovery, rather than retaining an unbounded orphan pool. Pending sync inventories the entire bounded pool and fetches missing IDs in dependency order, 32 at a time, so a known prefix cannot hide later items.

Direct peers exchange GossipSub traffic. Official GossipSub v0.18 excludes limited relay circuits from its mesh: relay-only connections use the same periodic verified request/sync protocol for block and pending-event propagation. Optional successful hole punching establishes a direct gossip-capable link.

Peer exchange supplies candidate routes, never application trust. Reconnect backoff grows from one second to one minute. Configured and persisted peer addresses share a 256-entry cache. Fresh discovery can evict a disconnected route, preferring failed nonconfigured routes while protecting connected direct/limited peers; stale configured routes may be replaced to restore connectivity. Automatic discovery and reconnect share retry claims; repeated advertisements cannot bypass an existing deadline. One candidate's due addresses go to the libp2p dialer under one request-timeout budget. Explicit Connect may bypass backoff; success resets retry state.

mDNS is opt-in local multicast. DHT is opt-in under a private configuration scope and needs reachable seeds from this same overlay. Static AutoRelay is opt-in via relay candidates; relay service and coordinated hole punching are separate flags. Reachable announcement/forwarding configuration is a deployment responsibility; see the [WAN/relay examples](../README.md#discovery-wan-and-relay-deployment).

## Resource limits

| Resource | Bound/default |
| --- | --- |
| Signed event encoded bytes | 16 KiB |
| Events per block / encoded block | 256 / 1 MiB |
| Pending events / inventory IDs | 4096 |
| Wire frame | 2 MiB |
| Blocks or requested events per response batch | 32, further trimmed by encoded bytes |
| Incoming / outgoing request slots | 8 / 8 |
| Concurrent sync sessions | 2 |
| Sync pass | 30 seconds, at most 32 batches |
| Request timeout | 5s default; 50ms–1m configurable |
| Sync interval | 3s default; 50ms–1h configurable |
| Total transport connections | 64 default/hard maximum; configurable 1–64 |
| System streams / per-peer streams | 256 / 32 |
| System libp2p resource-manager memory budget | 64 MiB (network accounting, not total process RSS) |
| Peer-address cache / exchanged addresses | 256 / 64 |
| Configured bootstrap / relay peers | 256 / 16 |
| Configured listen / announce addresses | 16 / 16 |
| Relay reservations / circuits per peer | 32 / 8 |
| Each relayed connection | 2 minutes / 16 MiB |

Queues for gossip validation, streams, workers, subscriptions and peer requests are bounded. Ledger storage and retained branch snapshots are deliberately small-network designs rather than bounded total-history storage. Full-batch list/trace HTTP reads are not paginated.

## Compatibility, persistence and verified scope

Configuration changes create a different network/genesis. On startup bbolt storage is checked against configuration and blocks are replay-validated. Shutdown closes network handlers before caller-owned storage. Actor signing keys, libp2p identity keys, and block storage have distinct responsibilities and backup needs.

The tests exercise actual localhost transport, multi-hop gossip independently of periodic sync, offline catch-up/restart, bootstrap loss, large multi-pass sync, competing forks, strict/adversarial framing, parent-context closure, scoped DHT, and limited Circuit Relay v2 traversal carrying blocks and pending events. The three-process application demo verifies API/CLI state agreement after bootstrap loss. Public WAN routing, physical NAT/hole punching and multicast mDNS remain unverified; deployment-specific reachability tests are necessary before making those claims.
