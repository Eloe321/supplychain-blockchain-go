package ledger

import (
	"crypto/ed25519"
	"testing"
)

// These frozen values protect consensus encoding from accidental field-order,
// domain, escaping, or omission changes. The all-zero seed is public test data.
func TestFrozenConsensusEncodingVector(t *testing.T) {
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	participant, err := NewParticipant("Supplier", RoleSupplier, key.Public().(ed25519.PublicKey))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{ChainID: "vector-chain", Difficulty: 4, Participants: []Participant{participant}}
	event := Event{Version: 1, ChainID: cfg.ChainID, Kind: KindCreate, BatchID: "LOT-001", Actor: participant.ID, Name: "Coffee", Unit: "kg", Quantity: 10, Sequence: 1, Timestamp: 1}
	if err := SignEvent(&event, key); err != nil {
		t.Fatal(err)
	}
	payload, err := EventPayload(event)
	if err != nil {
		t.Fatal(err)
	}
	const wantPayload = `{"domain":"supplychain/event/v1","version":1,"chain_id":"vector-chain","kind":"create","batch_id":"LOT-001","actor":"139e3940e64b5491722088d9a0d741628fc826e09475d341a780acde3c4b8070","recipient":"","name":"Coffee","unit":"kg","location":"","status":"","document_hash":"","previous_event":"","quantity":10,"sequence":1,"timestamp":1}`
	if string(payload) != wantPayload {
		t.Fatalf("signing payload changed:\n%s", payload)
	}
	if event.ID != "45875bd1b2ca3dff221c2e99ca51ac4b325ec71c9ad6ef03a22736fb19a1a690" {
		t.Fatalf("event ID changed: %s", event.ID)
	}
	if event.Signature != "eb70b6f62278fa76809d60861016a90467fee6189b4c0dcfe0661b06bf0748e6d5c6e4b82dbb7b649cb358c49559e4f7e7bdcbd80507529b7c5cb5c3ff86ff04" {
		t.Fatalf("signature changed: %s", event.Signature)
	}
	configHash, err := ConfigHash(cfg)
	if err != nil || configHash != "47aba09aa420f91350d8936a48eaa4ebbd456e60e793ed7990212de34f4773ea" {
		t.Fatalf("config hash changed: %s (%v)", configHash, err)
	}
	genesis, err := Genesis(cfg)
	if err != nil || genesis.Hash != "8000502ca752ff50abefbe869a2b774db8e7d9ff70c778c7020869cb5c2f23fe" {
		t.Fatalf("genesis hash changed: %s (%v)", genesis.Hash, err)
	}
}
