package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/time/rate"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"supplychain/chain"
	"supplychain/ledger"
)

func (n *Node) compatible(h hello) bool   { return h == n.hello }
func (n *Node) approve(p peer.ID)         { n.mu.Lock(); n.approved[p] = true; n.mu.Unlock() }
func (n *Node) isApproved(p peer.ID) bool { n.mu.Lock(); defer n.mu.Unlock(); return n.approved[p] }
func (n *Node) allow(p peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	r := n.rates[p]
	now := time.Now()
	if now.Sub(r.start) >= time.Second {
		r = rateWindow{start: now}
	}
	r.count++
	n.rates[p] = r
	return r.count <= 64
}
func (n *Node) handleStream(s network.Stream) {
	n.mu.Lock()
	if n.ctx.Err() != nil {
		n.mu.Unlock()
		s.Reset()
		return
	}
	n.handlers.Add(1)
	n.mu.Unlock()
	defer n.handlers.Done()
	select {
	case n.incoming <- struct{}{}:
		defer func() { <-n.incoming }()
	default:
		s.Reset()
		return
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(n.cfg.RequestTimeout))
	if !n.allow(s.Conn().RemotePeer()) {
		s.Reset()
		return
	}
	var req request
	if err := readFrame(s, &req); err != nil {
		s.Reset()
		return
	}
	res := response{Hello: n.hello}
	if !n.compatible(req.Hello) {
		res.Error = ErrWrongNetwork.Error()
		_ = writeFrame(s, res)
		return
	}
	if len(req.Locators) > 128 || len(req.IDs) > 32 {
		res.Error = "request limit exceeded"
		_ = writeFrame(s, res)
		return
	}
	for _, id := range append(append([]string{}, req.Locators...), req.IDs...) {
		if !validHash(id) {
			res.Error = "invalid hash"
			_ = writeFrame(s, res)
			return
		}
	}
	n.approve(s.Conn().RemotePeer())
	switch req.Op {
	case "status":
		status := n.chain.Status()
		res.Status = &status
	case "blocks":
		blocks, err := n.chain.BlocksAfter(req.Locators, 32)
		if err != nil {
			res.Error = err.Error()
			break
		}
		for _, b := range blocks {
			res.Blocks = append(res.Blocks, b)
			raw, _ := json.Marshal(res)
			if len(raw) > MaxFrameBytes {
				res.Blocks = res.Blocks[:len(res.Blocks)-1]
				break
			}
		}
	case "block":
		if len(req.IDs) != 1 {
			res.Error = "one block ID required"
			break
		}
		if b, ok := n.chain.GetBlock(req.IDs[0]); ok {
			res.Blocks = []ledger.Block{b}
		}
	case "pending":
		for _, e := range n.chain.Pending() {
			res.IDs = append(res.IDs, e.ID)
		}
	case "events":
		wanted := map[string]bool{}
		for _, id := range req.IDs {
			wanted[id] = true
		}
		for _, e := range n.chain.Pending() {
			if wanted[e.ID] {
				res.Events = append(res.Events, e)
			}
		}
	case "peers":
		res.Peers = n.peerExchange()
	default:
		res.Error = "unsupported operation"
	}
	_ = writeFrame(s, res)
}
func (n *Node) call(ctx context.Context, p peer.ID, req request) (response, error) {
	var res response
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer timeout()
	select {
	case n.requests <- struct{}{}:
		defer func() { <-n.requests }()
	case <-ctx.Done():
		return res, ctx.Err()
	}
	n.mu.Lock()
	limiter := n.sendLimits[p]
	if limiter == nil {
		limiter = rate.NewLimiter(24, 8)
		n.sendLimits[p] = limiter
	}
	n.mu.Unlock()
	if err := limiter.Wait(ctx); err != nil {
		return res, err
	}
	ctx = network.WithAllowLimitedConn(ctx, "supplychain verified synchronization")
	ctx = network.WithNoDial(ctx, "supplychain requests require an established connection")
	s, err := n.host.NewStream(ctx, p, n.protocol)
	if err != nil {
		return res, err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { s.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = s.SetDeadline(deadline)
	req.Hello = n.hello
	if err = writeFrame(s, req); err != nil {
		s.Reset()
		return res, err
	}
	if err = readFrame(s, &res); err != nil {
		s.Reset()
		return res, err
	}
	if !n.compatible(res.Hello) {
		return res, ErrWrongNetwork
	}
	if res.Error != "" {
		return res, errors.New(res.Error)
	}
	if len(res.Blocks) > 32 || len(res.Events) > 32 || len(res.IDs) > maxInventory || len(res.Peers) > maxAddresses {
		return res, errors.New("response limit exceeded")
	}
	n.approve(p)
	return res, nil
}

// Sync tries all connected peers so one stalled or invalid peer cannot prevent
// another peer from repairing the chain. Advertised height/work never select a tip.
func (n *Node) Sync(ctx context.Context) error {
	if n.ctx.Err() != nil {
		return ErrClosed
	}
	var errs []error
	for _, p := range n.host.Network().Peers() {
		if err := n.syncPeer(ctx, p); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (n *Node) syncPeer(ctx context.Context, p peer.ID) error {
	n.mu.Lock()
	if n.syncing[p] {
		n.mu.Unlock()
		return nil
	}
	n.syncing[p] = true
	n.mu.Unlock()
	defer func() { n.mu.Lock(); delete(n.syncing, p); n.mu.Unlock() }()
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	ctx, deadline := context.WithTimeout(ctx, 30*time.Second)
	defer deadline()
	select {
	case n.syncSlots <- struct{}{}:
		defer func() { <-n.syncSlots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	status, err := n.call(ctx, p, request{Op: "status"})
	if err != nil {
		return err
	}
	if status.Status == nil || !validHash(status.Status.Hash) {
		return errors.New("invalid peer status")
	}
	target := status.Status.Hash
	if !n.chain.HasBlock(target) {
		locators, err := n.chain.Locators()
		if err != nil {
			return err
		}
		n.mu.Lock()
		cursor := n.cursors[p]
		n.mu.Unlock()
		if cursor != "" && n.chain.HasBlock(cursor) {
			locators = append([]string{cursor}, locators...)
		}
		for rounds := 0; rounds < 32; rounds++ {
			res, err := n.call(ctx, p, request{Op: "blocks", Locators: locators})
			if err != nil {
				return err
			}
			if len(res.Blocks) == 0 {
				break
			}
			for i, b := range res.Blocks {
				if i > 0 && b.ParentHash != res.Blocks[i-1].Hash {
					return errors.New("noncontiguous block response")
				}
				if _, err := n.chain.AddBlock(b); err != nil && !errors.Is(err, chain.ErrKnownBlock) {
					return err
				}
			}
			// Keep the remote branch cursor even while it has less work than our tip.
			locators = []string{res.Blocks[len(res.Blocks)-1].Hash}
			n.mu.Lock()
			n.cursors[p] = locators[0]
			n.mu.Unlock()
			if locators[0] == target {
				break
			}
		}
		// The remote canonical tip may have changed while the above stream progressed.
		// Preserve the originally advertised valid side branch too; parent walks are bounded.
		if !n.chain.HasBlock(target) {
			if err := n.fetchAncestry(ctx, p, target); err != nil {
				return err
			}
		}
	}
	if err := n.syncPending(ctx, p); err != nil {
		return err
	}
	if res, err := n.call(ctx, p, request{Op: "peers"}); err == nil {
		for _, raw := range res.Peers {
			n.remember(raw)
		}
	}
	return nil
}
func (n *Node) fetchAncestry(ctx context.Context, p peer.ID, hash string) error {
	reverse := make([]ledger.Block, 0, 32)
	// A bounded fallback handles a tip changing mid-sync; longer walks retry using
	// fresh locators next anti-entropy tick instead of retaining unbounded orphans.
	for !n.chain.HasBlock(hash) && len(reverse) < 32 {
		res, err := n.call(ctx, p, request{Op: "block", IDs: []string{hash}})
		if err != nil {
			return err
		}
		if len(res.Blocks) != 1 || res.Blocks[0].Hash != hash {
			return errors.New("requested block missing")
		}
		b := res.Blocks[0]
		reverse = append(reverse, b)
		hash = b.ParentHash
	}
	if !n.chain.HasBlock(hash) {
		return chain.ErrUnknownParent
	}
	for i := len(reverse) - 1; i >= 0; i-- {
		if _, err := n.chain.AddBlock(reverse[i]); err != nil && !errors.Is(err, chain.ErrKnownBlock) {
			return err
		}
	}
	return nil
}
func (n *Node) syncPending(ctx context.Context, p peer.ID) error {
	res, err := n.call(ctx, p, request{Op: "pending"})
	if err != nil {
		return err
	}
	known := map[string]bool{}
	for _, e := range n.chain.Pending() {
		known[e.ID] = true
	}
	missing := []string{}
	for _, id := range res.IDs {
		if !validHash(id) {
			return errors.New("invalid event inventory")
		}
		if !known[id] {
			missing = append(missing, id)
		}
	}
	for start := 0; start < len(missing); start += 32 {
		end := min(start+32, len(missing))
		r, err := n.call(ctx, p, request{Op: "events", IDs: missing[start:end]})
		if err != nil {
			return err
		}
		wanted := map[string]bool{}
		for _, id := range missing[start:end] {
			wanted[id] = true
		}
		for _, e := range r.Events {
			if !wanted[e.ID] {
				return errors.New("unsolicited event")
			}
			if err := n.chain.Submit(e); err == nil {
				_ = n.publish(ctx, n.events, e)
			} else if errors.Is(err, chain.ErrMempoolFull) {
				return err
			}
		}
	}
	return nil
}
func (n *Node) receiveBlock(ctx context.Context, p peer.ID, hash string) error {
	if n.chain.HasBlock(hash) {
		return nil
	}
	res, err := n.call(ctx, p, request{Op: "block", IDs: []string{hash}})
	if err != nil {
		return err
	}
	if len(res.Blocks) != 1 || res.Blocks[0].Hash != hash {
		return fmt.Errorf("missing announced block")
	}
	_, err = n.chain.AddBlock(res.Blocks[0])
	if errors.Is(err, chain.ErrKnownBlock) {
		return nil
	}
	if errors.Is(err, chain.ErrUnknownParent) {
		n.trigger(p)
	}
	return err
}
