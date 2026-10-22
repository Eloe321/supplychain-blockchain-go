# Supply-chain blockchain in Go

An original educational implementation of a custom proof-of-work blockchain for signed, whole-batch custody. It includes persistent storage, independent libp2p peers, local signing, a CLI, and a JSON HTTP API. It adapts the learning sequence in [Jeiwan's seven-part Go blockchain series](docs/tutorial-map.md).

## Run the working demonstration

Install **Go 1.26 or later** and run:

```sh
make demo
```

The first build downloads the pinned Go modules. The demo needs Go and `make`; it does not require Docker, curl, Python, jq, a public bootstrap server, or any paid service. `./scripts/demo.sh` is an alternative using a POSIX shell and Go. It builds the binary when absent; use `make demo` to always rebuild after source changes.

The demonstration generates disposable supplier/carrier signing keys and a shared configuration, then launches **three independent `supplychain` executable processes**, each with its own database and peer identity. It uses dynamic loopback TCP/API ports and atomic readiness files, polls verified state with deadlines, creates and mines a coffee batch, records an offer and recipient acceptance, and checks matching tips, custody and traces on all nodes. It then stops the original bootstrap process and confirms a new checkpoint on the two survivors. A successful run ends with `PASS` at height 4. Cleanup signals and waits for only the processes it started and removes only its own temporary directory; failures print all node logs. To retain files:

```sh
make build
go run ./cmd/demo --binary bin/supplychain --keep
```

## Manual network

Build and create shared actor configuration once:

```sh
make build
mkdir -p work/manual
bin/supplychain keygen --name Farm --role supplier \
  --out work/manual/farm.key.json --public work/manual/farm.json
bin/supplychain keygen --name Carrier --role carrier \
  --out work/manual/carrier.key.json --public work/manual/carrier.json
bin/supplychain init --chain-id coffee-network --difficulty 12 \
  --participant work/manual/farm.json --participant work/manual/carrier.json \
  --out work/manual/network.json
```

`keygen` prints the public participant JSON. Save the carrier's `id` as a shell variable named `CARRIER_ID` for the transfer below. Share `network.json` with every peer. Keep each `.key.json` with its organization: it contains an unencrypted Ed25519 private key, is created with mode `0600`, and is never sent to a node. Key, participant, configuration and signed-event file creation refuses to overwrite existing files. The shared configuration is normalized by participant ID, so file order does not change genesis.

Start these in separate terminals. In terminal 1:

```sh
bin/supplychain run --config work/manual/network.json --data work/manual/node1 \
  --listen /ip4/127.0.0.1/tcp/4101 --api-bind 127.0.0.1:8101 \
  --ready-file work/manual/node1.ready.json
```

The startup JSON contains `api_url`, `id`, `addresses` (full `/p2p` addresses) and `data_dir`. Copy a complete address into `BOOTSTRAP` in the next terminals. For example, assign the exact string shown by node 1, including its generated PeerID. Then:

```sh
bin/supplychain run --config work/manual/network.json --data work/manual/node2 \
  --listen /ip4/127.0.0.1/tcp/4102 --api-bind 127.0.0.1:8102 \
  --bootstrap "$BOOTSTRAP"
```

```sh
bin/supplychain run --config work/manual/network.json --data work/manual/node3 \
  --listen /ip4/127.0.0.1/tcp/4103 --api-bind 127.0.0.1:8103 \
  --bootstrap "$BOOTSTRAP"
```

Bootstrap may be repeated for independent entry points. Approved peers exchange addresses and reconnect; the bootstrap has no special consensus authority. Confirm discovery with `bin/supplychain peers --api http://127.0.0.1:8102`. Every node can submit events, mine, validate, and serve history. One running process owns each data directory. SIGINT/SIGTERM shuts down HTTP and P2P resources and closes its database.

## Sign, submit, mine, trace

Run these from a fourth terminal. They deliberately use different nodes:

```sh
bin/supplychain event --kind create --key work/manual/farm.key.json \
  --config work/manual/network.json --batch-id 'Coffee / harvest 2026' \
  --name 'Arabica coffee' --quantity 120 --unit kg --location Farm \
  --out work/manual/create.json
bin/supplychain submit --api http://127.0.0.1:8101 --file work/manual/create.json
bin/supplychain mine --api http://127.0.0.1:8101
bin/supplychain batch --api http://127.0.0.1:8102 --id 'Coffee / harvest 2026'
```

Wait until node 2 shows sequence 1, then sign its confirmed predecessor:

```sh
bin/supplychain event --kind offer --key work/manual/farm.key.json \
  --config work/manual/network.json --batch-id 'Coffee / harvest 2026' \
  --recipient "$CARRIER_ID" --api http://127.0.0.1:8102 \
  --out work/manual/offer.json --submit
bin/supplychain mine --api http://127.0.0.1:8102
```

An offer reserves transfer to a registered recipient; custody remains with the supplier. Wait until node 3's batch query shows sequence 2 and that pending recipient. The carrier must explicitly accept:

```sh
bin/supplychain event --kind accept --key work/manual/carrier.key.json \
  --config work/manual/network.json --batch-id 'Coffee / harvest 2026' \
  --api http://127.0.0.1:8103 --out work/manual/accept.json --submit
bin/supplychain mine --api http://127.0.0.1:8103
bin/supplychain trace --api http://127.0.0.1:8101 --id 'Coffee / harvest 2026'
```

The owner can sign `checkpoint` with `--location`, `--status` and optional `--document-hash` (64 lowercase SHA-256 hex characters). During a pending offer only recipient `accept` or owner `cancel` is valid. Accept/cancel have no `--recipient` and transfer actions cannot attach checkpoint metadata. Quantity, unit and product name are set only on creation; suppliers and manufacturers may create batches.

`event` signs locally, validates enrollment and signature, and prints only the signed event JSON. `--out` additionally saves it; `--submit` additionally submits to `--api`. For dependent events, `--api` reads **confirmed** state: mine and wait for that node before signing the next action. To sign an update offline, supply **both** `--sequence` and `--previous`; these can refer to a known signed pending predecessor, but submission still validates the complete ordered transition. `--timestamp` defaults to current Unix seconds. Event timestamps are actor assertions, not trusted real-world evidence. Use `COMMAND --help` for every flag.

Manual `mine` can create an empty confirmation block. Optional `run --automine 2s` only mines when events are pending. Mining is cancellable, has a 30-second API budget and is serialized per node. At a high difficulty a request can time out before finding a proof; inspect status before retrying. A gossip publication failure can follow local acceptance/mining, so inspect pending/history/tip after an error before resubmitting.

## HTTP API

The default binding is `127.0.0.1:8080`. `--api-bind 127.0.0.1:0` chooses a dynamic port; stdout/readiness metadata reports the actual URL. External binding requires an explicit `--api-bind`. The API has no authentication: keep it private or place an authenticated proxy/firewall in front of a deliberate remote deployment. It exposes no private-key endpoints or permissive CORS policy.

| Method and path | Success body |
| --- | --- |
| `GET /v1/status` | Chain status with nested `node` identity, addresses and approved peers |
| `GET /v1/peers` | `{ "id": PeerID, "addresses": [full multiaddress], "peers": [approved PeerID] }` |
| `GET /v1/batches` | Array of confirmed `Batch` objects, sorted by ID |
| `GET /v1/batch?id=...` | One confirmed `Batch` |
| `GET /v1/trace?id=...` | Canonical `TraceEntry` array in event order |
| `GET /v1/pending` | Signed `Event` array in valid pending order |
| `POST /v1/events` | HTTP 202, `{ "id": event ID, "status": "pending" }` |
| `POST /v1/mine` with `{}` | Mined `Block` object |

All responses, including errors and unknown routes, are JSON. Errors have exactly `{ "error": "message" }`. Mutations require `Content-Type: application/json` and a single JSON object with no unknown fields; a submitted event body is limited to **16 KiB**, mining body to 1 KiB. Missing/invalid fields, signatures and JSON return 400; unknown batch/path 404; incorrect method 405 with `Allow`; duplicate event or invalid custody transition 409; excessive body 413; missing/wrong media type 415; canceled/closed node or full pending pool 503; operation deadline 504. HTTP header/read/write/idle timeouts are configured. Query `id` exactly once; URL encoding preserves arbitrary human IDs including slash and space.

Status shape:

```json
{"chain_id":"coffee-network","genesis_hash":"64 hex","height":3,"hash":"64 hex","work":"12288","pending":0,"node":{"id":"PeerID","addresses":["/ip4/127.0.0.1/tcp/4101/p2p/PeerID"],"peers":["PeerID"]}}
```

Exact object fields, all present (empty strings and zeros are included):

- `Batch`: `id`, `name`, `quantity` (integer), `unit`, `owner`, `pending_recipient`, `location`, `status`, `sequence` (integer), `last_event`.
- `Event`: `version` (1), `chain_id`, `kind`, `batch_id`, `actor`, `recipient`, `name`, `unit`, `location`, `status`, `document_hash`, `previous_event`, `quantity` (integer), `sequence` (integer), `timestamp` (Unix seconds integer), `id`, `signature`.
- `TraceEntry`: `event` (`Event`), `block_hash`, `height`, `confirmations` (integers). Confirmations count the containing block itself as 1.
- `Block`: `version` (1), `chain_id`, `height`, `parent_hash`, `timestamp`, `difficulty`, `nonce`, `merkle_root`, `events` (`Event` array), `hash`.
- Public participant: `id`, `name`, `role`, `public_key`. Configuration: `chain_id`, `difficulty`, `participants` (participant array). Local private key file: `participant`, `private_key` (128 lowercase hex characters).

Examples using optional curl:

```sh
curl http://127.0.0.1:8101/v1/status
curl --get --data-urlencode 'id=Coffee / harvest 2026' http://127.0.0.1:8101/v1/trace
curl -H 'Content-Type: application/json' --data-binary @work/manual/create.json \
  http://127.0.0.1:8101/v1/events
curl -H 'Content-Type: application/json' -d '{}' http://127.0.0.1:8101/v1/mine
```

## Architecture and consensus

`ledger/` holds deterministic configuration, Ed25519 signatures, event rules, batch state, Merkle trees and SHA-256 proof of work. `chain/` validates a persistent block graph, stores it in bbolt, maintains branch-specific state, chooses a canonical tip, handles reorganizations and keeps a dependency-ordered pending pool. `node/` adds encrypted TCP libp2p connections, strict network handshakes, GossipSub, sync, peer exchange, reconnects, scoped DHT/mDNS and optional Circuit Relay v2. `api/` and `cmd/supplychain/` expose the local application; `cmd/demo/` verifies a real multiprocess flow.

Genesis deterministically commits the normalized actor registry, chain ID and fixed difficulty. Organization actor IDs are SHA-256 of Ed25519 public keys; peer IDs identify libp2p transport keys stored separately per node. A peer key grants no supply-chain authority. Actors can sign offline and submit through any node. A static actor registry controls valid business events; it does not restrict who can run a peer or mine.

Each block commits its predecessor, ordered event Merkle root, height, timestamp, difficulty and nonce. Proof requires the configured number of leading zero bits (1–24; CLI default 12). Genesis contributes zero cumulative work; each subsequent valid block contributes `2^difficulty`. Higher cumulative work wins. Equal work chooses the lexicographically smaller block hash, making ties deterministic. Block timestamps must be nondecreasing relative to the parent; mining uses the greater of local wall-clock seconds and the parent's timestamp. Event sequence and predecessor IDs determine custody order.

Every incoming branch is independently validated, including signatures, ownership, sequence, proof and root. Valid side branches are retained. Reorganization replaces canonical batch state and revalidates pending and detached events against the new branch. Confirmations are **probabilistic**: a later higher-work branch can replace confirmed history. No fixed confirmation count supplies finality at this low educational difficulty.

`DATA/chain.db` persists blocks, canonical tip and network identity; opening replays and validates storage. `DATA/identity.key` (0600) persists the libp2p identity; `DATA/peers.json` (0600) caches bounded reconnect addresses. Pending events are memory-only: they may be recovered from online peers, or resubmitted from signed files after restart. Stop a node before copying its database for a simple backup; keep actor keys independently. Use the same config to restart and a fresh directory for another network.

## Discovery, WAN and relay deployment

Discovery is opt-in. No hardcoded public bootstrap exists. Repeat `--bootstrap` with reachable full `/p2p` addresses. `--mdns` enables chain-scoped multicast LAN discovery where available. `--dht` enables configuration-scoped Kademlia routing/rendezvous; seed it with reachable peers using the same configuration, since this is a private overlay rather than the public IPFS DHT.

A directly reachable WAN peer can use:

```sh
bin/supplychain run --config network.json --data node-public \
  --listen /ip4/0.0.0.0/tcp/4101 \
  --announce /ip4/203.0.113.10/tcp/4101 --dht
```

Replace the documentation IP with the real reachable address. Forward/open TCP 4101 at the router/firewall. The default HTTP API stays on loopback. `--announce` is repeatable and replaces automatically detected announcement addresses. Share the resulting full `/p2p` address with bootstrap clients. Check actual peer approval and tip/trace convergence, not just a successful TCP connection.

To provide a static relay at a reachable public address:

```sh
bin/supplychain run --config network.json --data relay \
  --listen /ip4/0.0.0.0/tcp/4101 \
  --announce /ip4/203.0.113.10/tcp/4101 \
  --relay-service --force-public
```

Use `--force-public` only when reachability is known. Set `RELAY` to that service's full advertised `/p2p` address. Private clients can run:

```sh
bin/supplychain run --config network.json --data private-node \
  --listen /ip4/0.0.0.0/tcp/0 --bootstrap "$RELAY" \
  --relay-peer "$RELAY" --force-private --hole-punch
```

`--relay-peer` is repeatable; AutoRelay reserves and refreshes circuits. `--force-private` requests relay reservations even on a local network and cannot be combined with `--force-public`. `--hole-punch` opts into coordinated hole punching; AutoNAT and NAT port mapping remain enabled. Relay circuit addresses include `/p2p/RELAY_ID/p2p-circuit/p2p/DESTINATION_ID`; distribute an actually reachable client circuit address to another peer when needed. libp2p does not advertise loopback relay addresses as public reachability.

GossipSub excludes resource-limited relay circuits from its mesh. Relay-only nodes still exchange verified blocks/pending events through periodic request/sync anti-entropy over Circuit Relay v2. Direct or successfully hole-punched links carry gossip. Timing knobs are `--sync-interval` (default 3s), `--request-timeout` (default 5s) and `--max-peers` (default/hard cap 64). [Protocol details](docs/protocol.md) describe the bounds.

Verified on this host: real local TCP peers, gossip, sync, fork convergence, bootstrap loss/restart, scoped DHT routing, and actual limited Circuit Relay v2 traversal for block and pending-event sync. **Public WAN operation, physical NAT/hole punching and multicast mDNS have not been verified.** For those deployments, run peers on independent networks, check announcement/reachability and approved peers, submit/mine signed events, disconnect/reconnect a node and compare tips, custody and traces after catch-up.

## Tests and limits

```sh
make test
make race
make vet
make build
make demo
```

Tests cover deterministic consensus vectors, tampering and signatures, state authorization, storage/restart/reorgs, bounded dependency pools and mining, real peer gossip/sync/forks/discovery/relay, hostile/oversized/stalled streams, strict HTTP handling, CLI key/config/signing/requests and graceful readiness/cancellation. `make demo` is the application acceptance check.

This is a learning network with deliberately low, fixed proof difficulty, no production attack economics, rewards or adjustable difficulty. A well-resourced miner can cheaply replace history. Custody signatures demonstrate authorization by registered keys, not physical possession or truth. It tracks indivisible whole batches with fixed quantities; no splits, merging, manufacturing transformations, money or UTXOs are modeled. Registry changes require another configuration/network. Documents stay off-chain; only their optional hash is recorded. Branch snapshots grow memory with blocks and batches, retained forks grow storage, and the HTTP list/history endpoints are intended for small educational datasets. There is no production identity recovery, confidential ledger, access-controlled API, cross-organization governance or audited deployment guarantee.
