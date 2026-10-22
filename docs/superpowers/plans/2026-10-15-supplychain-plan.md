# Supply chain blockchain implementation plan

> **For agentic workers:** Use `subagent-driven-development` task by task, with specification review followed by code quality review for each task. Work in this new isolated output repository on `feat/supplychain-capstone`. Use meaningful test-first red/green cycles and commit each reviewed task.

**Goal:** Deliver a runnable custom Go blockchain for signed whole-batch custody and provenance over a decentralized libp2p network.

**Architecture:** Pure ledger rules feed a bbolt-backed block graph and mempool. A libp2p node gossips events and blocks, performs bounded validated synchronization and optional mining, and exposes a loopback HTTP API and CLI.

**Tech stack:** Go, standard library Ed25519/SHA256/JSON/HTTP, bbolt, Go libp2p, GossipSub and Kademlia DHT. Resolve compatible current library versions, then pin them in go.mod/go.sum. No JavaScript toolchain or external database server.

## Task 1 Pure ledger and supply chain rules

Files: `go.mod`, `ledger/config.go`, `ledger/identity.go`, `ledger/event.go`, `ledger/state.go`, `ledger/block.go`, and focused `ledger/*_test.go`.

- [x] Write tests for generated actor keys and IDs, canonical config hashing, valid creation, offer and recipient acceptance, cancellation, stale sequence rejection, unauthorized signing, tampering, malformed fields, and mutation safety after rejection. Run `go test ./ledger` and observe failure for missing behavior.
- [x] Implement the structs and validation rules from the approved design, with explicit canonical JSON signing payloads and strict action field checks. Export practical documented constructors/helpers; communicate exact APIs to the controller before the next task.
- [x] Write tests for deterministic genesis, proof mining with cancellation, header/hash validation, empty blocks, odd-width Merkle levels and changed events. Run the focused tests and observe the missing behavior.
- [x] Implement Merkle hashing, deterministic genesis, context-aware mining and block validation against a parent and state. Keep consensus independent of storage and networking. Run `go test ./ledger` and `go vet ./ledger` until successful, then commit.

Contract: `ledger.Config`, `Participant`, `Event`, `Batch`, `State`, and `Block` are JSON-serializable fixed-field types. Functions support config normalization/hash, key generation/signing, event validation/state application, state cloning, genesis generation, block mining, and validation. Exact Go signatures are finalized in this task and documented in its report; downstream agents must consume those APIs rather than redefine consensus.

## Task 2 Persistent chain fork choice and mempool

Files: `chain/store.go`, `chain/chain.go`, `chain/mempool.go`, `chain/query.go`, focused `chain/*_test.go`, and dependency pins.

- [x] Write real temporary-database tests for initialization, restart, mismatched config, invalid block rejection without writes, sibling branches, canonical tie choice, longer branch reorganization, custody rollback and mempool recovery/conflicts. Verify failures before implementation.
- [x] Implement `Open(path string, cfg ledger.Config) (*Chain, error)` and `Close() error`, then a coherent copied-value API for adding blocks, submitting events, mining a pending snapshot, querying tip/state/trace, canonical block locators and bounded branch retrieval. Use bbolt transactions and synchronization. Publish exact API for the node task.
- [x] Validate candidates against their own ancestor state, choose by verified work and deterministic hash tie, and rebuild canonical state plus pending events after reorganization. Recheck mined blocks if the current tip changes. Bound pool sizes; never hold locks during PoW or network operations.
- [x] Run `go test -race ./ledger ./chain` and `go vet ./ledger ./chain`, inspect failures, fix causes and commit.

## Task 3 Full P2P node

Files: `node/config.go`, `node/identity.go`, `node/node.go`, `node/protocol.go`, `node/gossip.go`, `node/sync.go`, `node/discovery.go`, `node/*_test.go`, and dependency pins.

- [x] Write failing tests using real local libp2p hosts for signed event gossip, mining/block gossip, multi-hop propagation, a late joiner, bootstrap shutdown, incompatible network rejection and restart catch-up. Tests must poll with deadlines instead of relying on fixed sleeps.
- [x] Implement persistent node keys; encrypted TCP hosts with resource/connection bounds; chain-scoped protocol and topic IDs; configurable bootstraps, peer cache, peer exchange, reconnect, optional mDNS and application-scoped DHT discovery; explicit public-address/relay/hole-punching options.
- [x] Implement strict bounded length-prefixed JSON stream messages with deadlines and per-peer throttling. Handshake config and genesis compatibility. Exchange status, common-ancestor locators, batches of at most 32 parent-first blocks, and bounded mempool inventories. Check every incoming event/block through ledger/chain validation; never trust remote work assertions. Gossip deduplicated events and block announcements, repairing missed announcements with periodic sync and peer fallback.
- [x] Add failing partition tests with conflicting custody branches and equal-height tips, then implement verified fork convergence, pending-event revalidation and graceful cancellation as needed. Add malformed frame, oversized frame, stalled peer and duplicate-message cases. Confirm continued propagation after the bootstrap host stops.
- [x] Run `go test -race ./node` and all previous tests, inspect source against every P2P acceptance criterion, and commit. Report local verification separately from untested real WAN behavior.

## Task 4 CLI HTTP API demo and documentation

Files: `api/server.go`, `api/server_test.go`, `cmd/supplychain/main.go`, focused CLI tests, `scripts/demo.sh`, `Makefile`, `README.md`, `docs/protocol.md`, `docs/tutorial-map.md`, and packaging files if needed.

- [x] Write failing HTTP tests for status, batches, trace, peers, bounded signed event submission, mine, invalid JSON, method/status handling, and body limits. Implement handlers against the existing node and chain APIs with loopback defaults, no private-key endpoints, timeouts, JSON responses and clear errors.
- [x] Write CLI smoke tests for key generation, shared config construction, signed event file generation and submission, and invalid flags. Implement command help and commands for key/config creation, node run, status/peers, batch queries, event signing/submission and mining. Key files use 0600; signing happens locally. Use ordinary Go flags and HTTP client APIs.
- [x] Add a multi-process demo with isolated node databases, disposable actors, shared configuration, at least three nodes, a created batch and two-party transfer. Poll confirmed state with deadlines; compare tips/histories; trap exit and stop only owned processes.
- [x] Document exact quickstart commands and API routes, data/state transitions, module architecture, fixed-difficulty work and probabilistic confirmations, P2P protocols/resource bounds, LAN/WAN and relay deployment, tutorial adaptations, tests and scoped limitations. Make targets must expose build/test/race/vet/demo.
- [x] Run `go test ./...`, `go test -race ./...`, `go vet ./...`, `go build -o bin/supplychain ./cmd/supplychain`, and `make demo`. Fix failures with regression tests, commit, then request an independent end-to-end specification review followed by code quality review.

## Review and delivery

Each implementer reports DONE, DONE_WITH_CONCERNS, NEEDS_CONTEXT or BLOCKED, test red/green evidence, files and commit hash. A fresh specification reviewer reads the actual code against that task, and a fresh quality reviewer inspects correctness, errors, concurrency and tests only after specification approval. Fix and re-review all important findings before starting the next task. A final independent review checks the integrated implementation. Final delivery links the output project and README and states test and demo results plus material verification limits.
