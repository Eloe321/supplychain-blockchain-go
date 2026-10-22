package ledger

import (
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func createEvent(cfg Config, keys map[string]ed25519.PrivateKey) Event {
	return Event{Version: EventVersion, ChainID: cfg.ChainID, Kind: "create", BatchID: "COFFEE-001", Actor: PublicKeyID(keys["supplier"].Public().(ed25519.PublicKey)), Name: "Coffee beans", Unit: "kg", Quantity: 100, Sequence: 1, Timestamp: 1, Location: "Farm", Status: "harvested"}
}

func signTestEvent(t *testing.T, e Event, key ed25519.PrivateKey) Event {
	t.Helper()
	if err := SignEvent(&e, key); err != nil {
		t.Fatal(err)
	}
	return e
}

func nextEvent(previous Event, kind string, actor string) Event {
	return Event{Version: EventVersion, ChainID: previous.ChainID, Kind: kind, BatchID: previous.BatchID, Actor: actor, Sequence: previous.Sequence + 1, PreviousEvent: previous.ID, Timestamp: previous.Timestamp + 1}
}

func TestEventSignatureCommitsEveryFieldAndChain(t *testing.T) {
	cfg, keys := testConfig(t)
	e := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	if err := VerifyEvent(cfg, e); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(*Event){
		"version":     func(e *Event) { e.Version++ },
		"chain":       func(e *Event) { e.ChainID += "other" },
		"kind":        func(e *Event) { e.Kind = "checkpoint" },
		"batch":       func(e *Event) { e.BatchID += "other" },
		"actor":       func(e *Event) { e.Actor = cfg.Participants[1].ID },
		"recipient":   func(e *Event) { e.Recipient = cfg.Participants[1].ID },
		"name":        func(e *Event) { e.Name += "other" },
		"unit":        func(e *Event) { e.Unit = "tonnes" },
		"location":    func(e *Event) { e.Location = "Depot" },
		"status":      func(e *Event) { e.Status = "delivered" },
		"document":    func(e *Event) { e.DocumentHash = strings.Repeat("a", 64) },
		"predecessor": func(e *Event) { e.PreviousEvent = strings.Repeat("a", 64) },
		"quantity":    func(e *Event) { e.Quantity++ },
		"sequence":    func(e *Event) { e.Sequence++ },
		"timestamp":   func(e *Event) { e.Timestamp++ },
		"id":          func(e *Event) { e.ID = strings.Repeat("a", 64) },
		"signature":   func(e *Event) { e.Signature = strings.Repeat("a", 128) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			changed := e
			mutate(&changed)
			if err := VerifyEvent(cfg, changed); err == nil {
				t.Fatal("accepted tampered event")
			}
		})
	}
	encoded, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"version", "chain_id", "kind", "batch_id", "actor", "recipient", "name", "unit", "location", "status", "document_hash", "previous_event", "quantity", "sequence", "timestamp", "id", "signature"} {
		if _, ok := object[field]; !ok {
			t.Fatalf("missing explicit field %s", field)
		}
	}
	if len(object) != 17 {
		t.Fatalf("unexpected event fields: %s", encoded)
	}
}

func TestSigningRejectsWrongKeyWithoutMutation(t *testing.T) {
	cfg, keys := testConfig(t)
	e := createEvent(cfg, keys)
	before := e
	if err := SignEvent(&e, keys["retailer"]); err == nil {
		t.Fatal("signed event for a different actor")
	}
	if !reflect.DeepEqual(before, e) {
		t.Fatal("failed signing mutated event")
	}
	if err := SignEvent(&e, ed25519.PrivateKey{1}); err == nil {
		t.Fatal("accepted malformed private key")
	}
}

func TestTwoPartyCustodyAndCloneSafety(t *testing.T) {
	cfg, keys := testConfig(t)
	state := NewState()
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	if err := state.Apply(cfg, created); err != nil {
		t.Fatal(err)
	}
	carrier := PublicKeyID(keys["carrier"].Public().(ed25519.PublicKey))
	offer := nextEvent(created, "offer", created.Actor)
	offer.Recipient = carrier
	offer = signTestEvent(t, offer, keys["supplier"])
	if err := state.Apply(cfg, offer); err != nil {
		t.Fatal(err)
	}
	batch, ok := state.GetBatch(created.BatchID)
	if !ok || batch.Owner != created.Actor || batch.PendingRecipient != carrier {
		t.Fatalf("offer changed custody or failed to record pending recipient: %+v", batch)
	}
	accepted := signTestEvent(t, nextEvent(offer, "accept", carrier), keys["carrier"])
	if err := state.Apply(cfg, accepted); err != nil {
		t.Fatal(err)
	}
	batch, _ = state.GetBatch(created.BatchID)
	if batch.Owner != carrier || batch.PendingRecipient != "" || batch.Sequence != 3 || batch.LastEvent != accepted.ID || batch.Quantity != 100 {
		t.Fatalf("incorrect accepted custody: %+v", batch)
	}
	checkpoint := nextEvent(accepted, "checkpoint", carrier)
	checkpoint.Location = "Warehouse"
	checkpoint.Status = "stored"
	checkpoint.DocumentHash = strings.Repeat("a", 64)
	checkpoint = signTestEvent(t, checkpoint, keys["carrier"])
	if err := state.Apply(cfg, checkpoint); err != nil {
		t.Fatal(err)
	}
	batch, _ = state.GetBatch(created.BatchID)
	if batch.Location != "Warehouse" || batch.Status != "stored" {
		t.Fatalf("checkpoint did not update batch: %+v", batch)
	}
	clone := state.Clone()
	delete(clone.Batches, created.BatchID)
	if _, ok := state.GetBatch(created.BatchID); !ok {
		t.Fatal("cloned map aliases original")
	}
	list := state.AllBatches()
	list[0].Owner = "changed"
	batch, _ = state.GetBatch(created.BatchID)
	if batch.Owner != carrier {
		t.Fatal("query aliases original")
	}
}

func TestCancelAndPendingTransferRestrictions(t *testing.T) {
	cfg, keys := testConfig(t)
	state := NewState()
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	if err := state.Apply(cfg, created); err != nil {
		t.Fatal(err)
	}
	offer := nextEvent(created, "offer", created.Actor)
	offer.Recipient = cfg.Participants[2].ID
	offer = signTestEvent(t, offer, keys["supplier"])
	if err := state.Apply(cfg, offer); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"offer", "checkpoint"} {
		attempt := nextEvent(offer, kind, created.Actor)
		if kind == "offer" {
			attempt.Recipient = cfg.Participants[3].ID
		}
		attempt = signTestEvent(t, attempt, keys["supplier"])
		before := state.Clone()
		if err := state.Apply(cfg, attempt); err == nil {
			t.Fatalf("accepted %s while transfer pending", kind)
		}
		if !reflect.DeepEqual(before, state) {
			t.Fatal("rejected action mutated state")
		}
	}
	cancel := signTestEvent(t, nextEvent(offer, "cancel", created.Actor), keys["supplier"])
	if err := state.Apply(cfg, cancel); err != nil {
		t.Fatal(err)
	}
	batch, _ := state.GetBatch(created.BatchID)
	if batch.PendingRecipient != "" || batch.Owner != created.Actor {
		t.Fatalf("incorrect cancellation: %+v", batch)
	}
	replay := signTestEvent(t, nextEvent(cancel, "accept", offer.Recipient), keys["carrier"])
	if err := state.Apply(cfg, replay); err == nil {
		t.Fatal("accepted canceled transfer")
	}
}

func TestInvalidEventsDoNotMutateState(t *testing.T) {
	tests := map[string]func(Event, Config) Event{
		"duplicate":      func(e Event, _ Config) Event { return e },
		"stale sequence": func(e Event, _ Config) Event { n := nextEvent(e, "checkpoint", e.Actor); n.Sequence = 1; return n },
		"wrong predecessor": func(e Event, _ Config) Event {
			n := nextEvent(e, "checkpoint", e.Actor)
			n.PreviousEvent = strings.Repeat("a", 64)
			return n
		},
		"missing batch": func(e Event, _ Config) Event {
			n := nextEvent(e, "checkpoint", e.Actor)
			n.BatchID = "missing"
			return n
		},
		"no transfer to accept": func(e Event, _ Config) Event { return nextEvent(e, "accept", e.Actor) },
		"no transfer to cancel": func(e Event, _ Config) Event { return nextEvent(e, "cancel", e.Actor) },
		"self offer":            func(e Event, _ Config) Event { n := nextEvent(e, "offer", e.Actor); n.Recipient = e.Actor; return n },
		"auditor recipient": func(e Event, c Config) Event {
			n := nextEvent(e, "offer", e.Actor)
			n.Recipient = c.Participants[5].ID
			return n
		},
		"unknown recipient": func(e Event, _ Config) Event {
			n := nextEvent(e, "offer", e.Actor)
			n.Recipient = strings.Repeat("a", 64)
			return n
		},
		"quantity change": func(e Event, _ Config) Event { n := nextEvent(e, "checkpoint", e.Actor); n.Quantity = 1; return n },
		"checkpoint recipient": func(e Event, c Config) Event {
			n := nextEvent(e, "checkpoint", e.Actor)
			n.Recipient = c.Participants[2].ID
			return n
		},
		"offer metadata": func(e Event, c Config) Event {
			n := nextEvent(e, "offer", e.Actor)
			n.Recipient = c.Participants[2].ID
			n.Location = "ambiguous"
			return n
		},
		"create predecessor": func(e Event, _ Config) Event {
			e.BatchID = "another"
			e.PreviousEvent = strings.Repeat("a", 64)
			return e
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, keys := testConfig(t)
			state := NewState()
			created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
			if err := state.Apply(cfg, created); err != nil {
				t.Fatal(err)
			}
			before := state.Clone()
			invalid := signTestEvent(t, mutate(created, cfg), keys["supplier"])
			if err := state.Apply(cfg, invalid); err == nil {
				t.Fatal("accepted invalid event")
			}
			if !reflect.DeepEqual(before, state) {
				t.Fatal("invalid event changed state")
			}
		})
	}
}

func TestRoleAndOwnershipEnforcement(t *testing.T) {
	cfg, keys := testConfig(t)
	for _, participant := range cfg.Participants {
		e := createEvent(cfg, keys)
		e.Actor = participant.ID
		e = signTestEvent(t, e, keys[participant.Role])
		err := NewState().Apply(cfg, e)
		allowed := participant.Role == "supplier" || participant.Role == "manufacturer"
		if (err == nil) != allowed {
			t.Fatalf("create role %s: %v", participant.Role, err)
		}
	}
	state := NewState()
	created := signTestEvent(t, createEvent(cfg, keys), keys["supplier"])
	if err := state.Apply(cfg, created); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"manufacturer", "carrier", "retailer", "auditor"} {
		actor := PublicKeyID(keys[role].Public().(ed25519.PublicKey))
		e := signTestEvent(t, nextEvent(created, "checkpoint", actor), keys[role])
		if err := state.Apply(cfg, e); err == nil {
			t.Fatalf("nonowner %s changed batch", role)
		}
	}
	unknown := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	e := createEvent(cfg, keys)
	e.Actor = PublicKeyID(unknown.Public().(ed25519.PublicKey))
	e = signTestEvent(t, e, unknown)
	if err := VerifyEvent(cfg, e); err == nil {
		t.Fatal("accepted unenrolled signer")
	}
}

func TestEventRejectsMalformedFields(t *testing.T) {
	tests := map[string]func(*Event){
		"version":            func(e *Event) { e.Version = 0 },
		"chain":              func(e *Event) { e.ChainID = "other" },
		"kind":               func(e *Event) { e.Kind = "mint" },
		"batch empty":        func(e *Event) { e.BatchID = "" },
		"batch length":       func(e *Event) { e.BatchID = strings.Repeat("x", 129) },
		"name empty":         func(e *Event) { e.Name = "" },
		"name length":        func(e *Event) { e.Name = strings.Repeat("x", 129) },
		"unit empty":         func(e *Event) { e.Unit = "" },
		"unit length":        func(e *Event) { e.Unit = strings.Repeat("x", 33) },
		"location length":    func(e *Event) { e.Location = strings.Repeat("x", 257) },
		"status length":      func(e *Event) { e.Status = strings.Repeat("x", 65) },
		"quantity zero":      func(e *Event) { e.Quantity = 0 },
		"quantity negative":  func(e *Event) { e.Quantity = -1 },
		"sequence zero":      func(e *Event) { e.Sequence = 0 },
		"timestamp zero":     func(e *Event) { e.Timestamp = 0 },
		"timestamp negative": func(e *Event) { e.Timestamp = -1 },
		"bad document":       func(e *Event) { e.DocumentHash = strings.Repeat("g", 64) },
		"uppercase document": func(e *Event) { e.DocumentHash = strings.Repeat("A", 64) },
		"invalid UTF8":       func(e *Event) { e.Name = string([]byte{0xff}) },
		"create recipient":   func(e *Event) { e.Recipient = strings.Repeat("a", 64) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, keys := testConfig(t)
			e := createEvent(cfg, keys)
			mutate(&e)
			// Signing may reject invalid encoding before verification does.
			if err := SignEvent(&e, keys["supplier"]); err == nil {
				if err := VerifyEvent(cfg, e); err == nil {
					t.Fatal("accepted malformed event")
				}
			}
		})
	}
}

func TestEventTimestampsAreNotCustodyOrder(t *testing.T) {
	cfg, keys := testConfig(t)
	state := NewState()
	created := createEvent(cfg, keys)
	created.Timestamp = 10_000_000_000
	created = signTestEvent(t, created, keys["supplier"])
	if err := state.Apply(cfg, created); err != nil {
		t.Fatal(err)
	}
	checkpoint := nextEvent(created, "checkpoint", created.Actor)
	checkpoint.Timestamp = 1
	checkpoint = signTestEvent(t, checkpoint, keys["supplier"])
	if err := state.Apply(cfg, checkpoint); err != nil {
		t.Fatalf("valid sequence rejected due to wall clock: %v", err)
	}
}
