package node

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"supplychain/chain"
	"supplychain/ledger"
)

type announcement struct {
	Hash string `json:"hash"`
}

func (n *Node) setupGossip() error {
	ps, err := pubsub.NewGossipSub(n.ctx, n.host, pubsub.WithMaxMessageSize(32*1024), pubsub.WithMessageIdFn(func(m *pb.Message) string {
		h := sha256.Sum256(append([]byte(m.GetTopic()+"\x00"), m.Data...))
		return hex.EncodeToString(h[:])
	}), pubsub.WithValidateQueueSize(64), pubsub.WithValidateWorkers(4), pubsub.WithValidateThrottle(8), pubsub.WithPeerOutboundQueueSize(64), pubsub.WithFloodPublish(true))
	if err != nil {
		return err
	}
	n.pubsub = ps
	eventTopic, blockTopic := n.scope+"/events/1", n.scope+"/blocks/1"
	if err = ps.RegisterTopicValidator(eventTopic, func(ctx context.Context, p peer.ID, m *pubsub.Message) pubsub.ValidationResult {
		if p != n.host.ID() && !n.isApproved(p) {
			n.trigger(p)
			return pubsub.ValidationIgnore
		}
		if len(m.Data) > ledger.MaxEventBytes {
			return pubsub.ValidationReject
		}
		var e ledger.Event
		if decodeStrict(m.Data, &e) != nil {
			return pubsub.ValidationReject
		}
		if ledger.VerifyEvent(n.chain.Config(), e) != nil {
			return pubsub.ValidationReject
		}
		err := n.chain.Submit(e)
		if err == nil || errors.Is(err, chain.ErrKnownEvent) {
			return pubsub.ValidationAccept
		}
		return pubsub.ValidationIgnore
	}, pubsub.WithValidatorConcurrency(4), pubsub.WithValidatorTimeout(n.cfg.RequestTimeout)); err != nil {
		return err
	}
	if err = ps.RegisterTopicValidator(blockTopic, func(ctx context.Context, p peer.ID, m *pubsub.Message) pubsub.ValidationResult {
		if p != n.host.ID() && !n.isApproved(p) {
			n.trigger(p)
			return pubsub.ValidationIgnore
		}
		if len(m.Data) > 128 {
			return pubsub.ValidationReject
		}
		var a announcement
		if decodeStrict(m.Data, &a) != nil || !validHash(a.Hash) {
			return pubsub.ValidationReject
		}
		if err := n.receiveBlock(ctx, p, a.Hash); err != nil {
			return pubsub.ValidationIgnore
		}
		return pubsub.ValidationAccept
	}, pubsub.WithValidatorConcurrency(4), pubsub.WithValidatorTimeout(n.cfg.RequestTimeout)); err != nil {
		return err
	}
	n.events, err = ps.Join(eventTopic)
	if err != nil {
		return err
	}
	n.blocks, err = ps.Join(blockTopic)
	if err != nil {
		return err
	}
	n.eventSub, err = n.events.Subscribe(pubsub.WithBufferSize(64))
	if err != nil {
		return err
	}
	n.blockSub, err = n.blocks.Subscribe(pubsub.WithBufferSize(64))
	return err
}
func (n *Node) publish(ctx context.Context, t *pubsub.Topic, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := n.operationContext(ctx)
	defer cancel()
	return t.Publish(ctx, raw)
}
