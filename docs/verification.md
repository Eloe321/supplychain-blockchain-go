# Verification report

Verified on 15 October 2026 with Go 1.27.2 on macOS arm64. Application source is commit `38ed434`; subsequent delivery changes only update documentation and plan completion.

## Independent final checks

| Command | Result |
| --- | --- |
| `go test -count=1 ./...` | Passed all six packages |
| `go test -race -count=1 ./...` | Passed all six packages |
| `go vet ./...` | Passed |
| `go build -o bin/supplychain ./cmd/supplychain` | Passed |
| `gofmt -l api chain cmd ledger node` | No unformatted files |
| `git diff --check` | Passed |
| `make demo` | Passed with CLI default difficulty 12 |

The final demonstration launched three independent executable processes with separate databases and peer identities. All three agreed after batch creation and signed recipient acceptance. It stopped the original bootstrap process, then the surviving peers recorded and mined a checkpoint and agreed on the complete four-event trace at height 4. The final observed tip was `000c786b6fd2114f5f3c35ccd3e489a86491b1c383a5c94937a0643584b10312`. Demo keys, ports and proof nonces vary between runs, so future successful tips will differ.

## Coverage of behavior

The automated suite covers fixed consensus encodings, signature and payload tampering, role and custody authorization, pending transfers, stale predecessors, Merkle commitments, proof validation, mining cancellation, storage restart and corruption, branch choice and reorganization, pending dependencies and conflicts, encoded block size limits, and copied concurrent state queries.

Real local libp2p tests cover multi-hop gossip, bounded catch-up across more than one block batch, bootstrap loss, identity restart, competing and lower-work branches, conflicting custody across a partition and reconnection, incompatible handshakes, hostile and stalled streams, duplicate and invalid messages, relay circuit block and pending-event synchronization, application-scoped DHT routing, resource accounting, reconnect backoff, alternate routes, full-cache recovery, and context shutdown. API/CLI tests cover signed custody, strict requests and errors, local signing, key permissions, query encoding, readiness and cancellation. The executable demo exercises the integrated application through its CLI and HTTP API.

Each implementation task received an independent specification review followed by code quality review. Reported issues were reproduced and corrected with regression tests; a fresh final integrated review found no remaining material issue. Its documentation correction, describing Merkle leaves as full signed events, is included.

## Connectivity limits

Public WAN operation, physical-network NAT traversal, hole punching and multicast mDNS discovery were not verified. The code provides explicit configuration and deployment instructions for these conditions. Local Circuit Relay v2 traversal was verified; relay-only peers use validated periodic anti-entropy because GossipSub excludes resource-limited circuits. See [the README](../README.md) for the learning-network scope and deployment instructions.
