// Package node provides bounded, encrypted libp2p networking for a verified chain.
package node

import (
	"errors"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"time"
)

const ProtocolVersion = "1"

// Config controls transport and discovery only; consensus lives in chain.Config.
// Discovery is opt-in. RelayPeers are reachable static Circuit Relay v2 services.
// ForcePublic declares this host publicly reachable (useful for a relay service).
// ForcePrivate makes static AutoRelay reserve circuits even on a local network.
// Hole punching is opt-in and normally used with static relay candidates.
type Config struct {
	DataDir            string
	ListenAddrs        []string
	BootstrapPeers     []string
	AnnounceAddrs      []string
	RelayPeers         []string
	EnableMDNS         bool
	EnableDHT          bool
	EnableRelayService bool
	EnableHolePunching bool
	ForcePublic        bool
	ForcePrivate       bool
	SyncInterval       time.Duration
	RequestTimeout     time.Duration
	MaxPeers           int
	ProtocolVersion    string
}

func normalizeConfig(c Config) (Config, error) {
	if c.DataDir == "" {
		return c, errors.New("node DataDir is required")
	}
	if c.ProtocolVersion == "" {
		c.ProtocolVersion = ProtocolVersion
	}
	if c.ProtocolVersion != ProtocolVersion {
		return c, errors.New("unsupported protocol version")
	}
	if c.MaxPeers == 0 {
		c.MaxPeers = 64
	}
	if c.MaxPeers < 1 || c.MaxPeers > 64 {
		return c, errors.New("MaxPeers must be 1..64")
	}
	if c.SyncInterval == 0 {
		c.SyncInterval = 3 * time.Second
	}
	if c.SyncInterval < 50*time.Millisecond || c.SyncInterval > time.Hour {
		return c, errors.New("SyncInterval must be 50ms..1h")
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 5 * time.Second
	}
	if c.RequestTimeout < 50*time.Millisecond || c.RequestTimeout > time.Minute {
		return c, errors.New("RequestTimeout must be 50ms..1m")
	}
	if c.ForcePublic && c.ForcePrivate {
		return c, errors.New("cannot force public and private reachability")
	}
	if len(c.BootstrapPeers) > 256 || len(c.RelayPeers) > 16 || len(c.AnnounceAddrs) > 16 || len(c.ListenAddrs) > 16 {
		return c, errors.New("too many configured addresses")
	}
	if c.ListenAddrs == nil {
		c.ListenAddrs = []string{"/ip4/0.0.0.0/tcp/0"}
	}
	for _, raw := range append(append([]string{}, c.ListenAddrs...), c.AnnounceAddrs...) {
		if _, err := ma.NewMultiaddr(raw); err != nil {
			return c, err
		}
	}
	for _, raw := range append(append([]string{}, c.BootstrapPeers...), c.RelayPeers...) {
		if _, err := parsePeer(raw); err != nil {
			return c, err
		}
	}
	c.ListenAddrs = append([]string{}, c.ListenAddrs...)
	c.BootstrapPeers = append([]string{}, c.BootstrapPeers...)
	c.RelayPeers = append([]string{}, c.RelayPeers...)
	c.AnnounceAddrs = append([]string{}, c.AnnounceAddrs...)
	return c, nil
}
func parsePeer(raw string) (*peer.AddrInfo, error) {
	if len(raw) > 2048 {
		return nil, errors.New("peer address too long")
	}
	a, err := ma.NewMultiaddr(raw)
	if err != nil {
		return nil, err
	}
	return peer.AddrInfoFromP2pAddr(a)
}
