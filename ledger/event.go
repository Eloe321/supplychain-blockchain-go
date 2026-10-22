package ledger

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

const (
	EventVersion   = 1
	KindCreate     = "create"
	KindCheckpoint = "checkpoint"
	KindOffer      = "offer"
	KindAccept     = "accept"
	KindCancel     = "cancel"
)

// Event is a fixed-field, signed supply-chain action. No consensus JSON field
// may be omitted. Timestamp is positive Unix seconds asserted by the actor;
// custody ordering is determined exclusively by Sequence and PreviousEvent.
type Event struct {
	Version       int    `json:"version"`
	ChainID       string `json:"chain_id"`
	Kind          string `json:"kind"`
	BatchID       string `json:"batch_id"`
	Actor         string `json:"actor"`
	Recipient     string `json:"recipient"`
	Name          string `json:"name"`
	Unit          string `json:"unit"`
	Location      string `json:"location"`
	Status        string `json:"status"`
	DocumentHash  string `json:"document_hash"`
	PreviousEvent string `json:"previous_event"`
	Quantity      int64  `json:"quantity"`
	Sequence      uint64 `json:"sequence"`
	Timestamp     int64  `json:"timestamp"`
	ID            string `json:"id"`
	Signature     string `json:"signature"`
}

// eventPayload fixes signing field names and ordering and contains no maps,
// floating point numbers, event ID, or signature. Domain prevents signatures
// from being interpreted as another protocol's JSON payload.
type eventPayload struct {
	Domain        string `json:"domain"`
	Version       int    `json:"version"`
	ChainID       string `json:"chain_id"`
	Kind          string `json:"kind"`
	BatchID       string `json:"batch_id"`
	Actor         string `json:"actor"`
	Recipient     string `json:"recipient"`
	Name          string `json:"name"`
	Unit          string `json:"unit"`
	Location      string `json:"location"`
	Status        string `json:"status"`
	DocumentHash  string `json:"document_hash"`
	PreviousEvent string `json:"previous_event"`
	Quantity      int64  `json:"quantity"`
	Sequence      uint64 `json:"sequence"`
	Timestamp     int64  `json:"timestamp"`
}

// EventPayload returns the exact canonical bytes used for the event ID and
// Ed25519 signature. It rejects invalid UTF-8 rather than silently replacing it.
func EventPayload(e Event) ([]byte, error) {
	for _, value := range []string{e.ChainID, e.Kind, e.BatchID, e.Actor, e.Recipient, e.Name, e.Unit, e.Location, e.Status, e.DocumentHash, e.PreviousEvent} {
		if !utf8.ValidString(value) {
			return nil, errors.New("event contains invalid UTF-8")
		}
	}
	return json.Marshal(eventPayload{
		Domain: "supplychain/event/v1", Version: e.Version, ChainID: e.ChainID,
		Kind: e.Kind, BatchID: e.BatchID, Actor: e.Actor, Recipient: e.Recipient,
		Name: e.Name, Unit: e.Unit, Location: e.Location, Status: e.Status,
		DocumentHash: e.DocumentHash, PreviousEvent: e.PreviousEvent,
		Quantity: e.Quantity, Sequence: e.Sequence, Timestamp: e.Timestamp,
	})
}

// SignEvent sets ID and Signature only after successful signing. Actor must
// already identify private; other fields must be supplied by the caller.
// Signature validity alone does not imply enrollment or a valid state change.
func SignEvent(e *Event, private ed25519.PrivateKey) error {
	if e == nil {
		return errors.New("nil event")
	}
	if err := validatePrivateKey(private); err != nil {
		return err
	}
	if e.Actor != PublicKeyID(private.Public().(ed25519.PublicKey)) {
		return errors.New("event actor does not identify signing key")
	}
	payload, err := EventPayload(*e)
	if err != nil {
		return err
	}
	signed := *e
	signed.ID = hashHex(payload)
	signed.Signature = hex.EncodeToString(ed25519.Sign(private, payload))
	encoded, err := json.Marshal(signed)
	if err != nil {
		return err
	}
	if len(encoded) > MaxEventBytes {
		return fmt.Errorf("event exceeds %d bytes", MaxEventBytes)
	}
	*e = signed
	return nil
}

// VerifyEvent checks fixed fields, action-specific fields, enrollment, event ID,
// and signature. State-dependent ownership and sequence checks are in Apply.
func VerifyEvent(cfg Config, e Event) error {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return err
	}
	return verifyEvent(normalized, e)
}

func verifyEvent(cfg Config, e Event) error {
	if e.Version != EventVersion || e.ChainID != cfg.ChainID {
		return errors.New("event version or chain ID does not match")
	}
	if err := requiredText("batch_id", e.BatchID, MaxBatchIDBytes); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value string
		limit int
	}{
		{"name", e.Name, MaxNameBytes}, {"unit", e.Unit, MaxUnitBytes},
		{"location", e.Location, MaxLocationBytes}, {"status", e.Status, MaxStatusBytes},
	} {
		if err := boundedText(field.name, field.value, field.limit); err != nil {
			return err
		}
	}
	if e.Timestamp <= 0 || e.Sequence == 0 {
		return errors.New("timestamp and sequence must be positive")
	}
	if !canonicalHex(e.Actor, sha256.Size) || !canonicalHex(e.ID, sha256.Size) || !canonicalHex(e.Signature, ed25519.SignatureSize) {
		return errors.New("actor, event ID, and signature must be canonical lowercase hexadecimal")
	}
	for _, value := range []string{e.Recipient, e.DocumentHash, e.PreviousEvent} {
		if value != "" && !canonicalHex(value, sha256.Size) {
			return errors.New("recipient, document hash, and predecessor must be empty or 64 lowercase hexadecimal characters")
		}
	}
	if err := actionFields(e); err != nil {
		return err
	}
	actor, err := participantByID(cfg, e.Actor)
	if err != nil {
		return err
	}
	payload, err := EventPayload(e)
	if err != nil {
		return err
	}
	if e.ID != hashHex(payload) {
		return errors.New("event ID does not match signed payload")
	}
	public, _ := ParsePublicKey(actor.PublicKey) // Already validated by NormalizeConfig.
	signature, _ := hex.DecodeString(e.Signature)
	if !ed25519.Verify(public, payload, signature) {
		return errors.New("invalid event signature")
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(encoded) > MaxEventBytes {
		return fmt.Errorf("event exceeds %d bytes", MaxEventBytes)
	}
	return nil
}

func actionFields(e Event) error {
	if e.Kind == KindCreate {
		if e.Sequence != 1 || e.PreviousEvent != "" || e.Recipient != "" || e.Quantity <= 0 {
			return errors.New("create requires sequence 1, no predecessor or recipient, and positive quantity")
		}
		if err := requiredText("name", e.Name, MaxNameBytes); err != nil {
			return err
		}
		return requiredText("unit", e.Unit, MaxUnitBytes)
	}
	if e.Name != "" || e.Unit != "" || e.Quantity != 0 {
		return errors.New("only create may specify name, unit, or quantity")
	}
	if e.PreviousEvent == "" {
		return errors.New("update requires predecessor")
	}
	switch e.Kind {
	case KindCheckpoint:
		if e.Recipient != "" {
			return errors.New("checkpoint may not specify recipient")
		}
	case KindOffer, KindAccept, KindCancel:
		if e.Location != "" || e.Status != "" || e.DocumentHash != "" {
			return errors.New("transfer actions may not specify checkpoint metadata")
		}
		if e.Kind == KindOffer {
			if e.Recipient == "" {
				return errors.New("offer requires recipient")
			}
		} else if e.Recipient != "" {
			return errors.New("accept and cancel may not specify recipient")
		}
	default:
		return errors.New("unknown event kind")
	}
	return nil
}
