package ledger

import (
	"errors"
	"math"
	"sort"
)

// Batch is the current state of one indivisible batch. Document hashes remain
// in its signed event history; quantities and product identity never change.
type Batch struct {
	ID               string `json:"id"`
	Name             string `json:"name"`
	Quantity         int64  `json:"quantity"`
	Unit             string `json:"unit"`
	Owner            string `json:"owner"`
	PendingRecipient string `json:"pending_recipient"`
	Location         string `json:"location"`
	Status           string `json:"status"`
	Sequence         uint64 `json:"sequence"`
	LastEvent        string `json:"last_event"`
}

// State is a branch-specific snapshot. Call Clone before sharing a mutable
// snapshot with another goroutine. Batch values contain no mutable references.
type State struct {
	Batches map[string]Batch `json:"batches"`
}

// NewState returns an empty state ready to apply the first event.
func NewState() *State {
	return &State{Batches: make(map[string]Batch)}
}

// Clone returns a separately allocated snapshot. A nil state clones as empty.
func (s *State) Clone() *State {
	copy := NewState()
	if s != nil {
		for id, batch := range s.Batches {
			copy.Batches[id] = batch
		}
	}
	return copy
}

// GetBatch returns a copied batch value.
func (s *State) GetBatch(id string) (Batch, bool) {
	if s == nil {
		return Batch{}, false
	}
	batch, ok := s.Batches[id]
	return batch, ok
}

// AllBatches returns copied values sorted by batch ID.
func (s *State) AllBatches() []Batch {
	batches := make([]Batch, 0)
	if s != nil {
		for _, batch := range s.Batches {
			batches = append(batches, batch)
		}
	}
	sort.Slice(batches, func(i, j int) bool { return batches[i].ID < batches[j].ID })
	return batches
}

// Apply validates and atomically applies one signed action. A rejected event
// never changes any batch or allocates a map in the receiver.
func (s *State) Apply(cfg Config, e Event) error {
	if s == nil {
		return errors.New("nil state")
	}
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return err
	}
	if err := verifyEvent(normalized, e); err != nil {
		return err
	}
	batch, exists := s.Batches[e.BatchID]
	if e.Kind == KindCreate {
		if exists {
			return errors.New("batch already exists")
		}
		actor, _ := participantByID(normalized, e.Actor)
		if actor.Role != RoleSupplier && actor.Role != RoleManufacturer {
			return errors.New("only a supplier or manufacturer may create batches")
		}
		batch = Batch{ID: e.BatchID, Name: e.Name, Quantity: e.Quantity, Unit: e.Unit, Owner: e.Actor, Location: e.Location, Status: e.Status}
	} else {
		if !exists {
			return errors.New("batch does not exist")
		}
		if batch.Sequence == math.MaxUint64 || e.Sequence != batch.Sequence+1 || e.PreviousEvent != batch.LastEvent {
			return errors.New("event does not extend the batch's sequence and predecessor")
		}
		if batch.PendingRecipient != "" && e.Kind != KindAccept && e.Kind != KindCancel {
			return errors.New("only accept or cancel is allowed while transfer is pending")
		}
		switch e.Kind {
		case KindAccept:
			if batch.PendingRecipient == "" || e.Actor != batch.PendingRecipient {
				return errors.New("only the pending recipient may accept custody")
			}
			batch.Owner = e.Actor
			batch.PendingRecipient = ""
		case KindCheckpoint, KindOffer, KindCancel:
			if e.Actor != batch.Owner {
				return errors.New("only the owner may checkpoint, offer, or cancel")
			}
			switch e.Kind {
			case KindCheckpoint:
				batch.Location, batch.Status = e.Location, e.Status
			case KindOffer:
				recipient, err := participantByID(normalized, e.Recipient)
				if err != nil {
					return err
				}
				if recipient.Role == RoleAuditor || e.Recipient == e.Actor {
					return errors.New("recipient must be another enrolled nonauditor actor")
				}
				batch.PendingRecipient = e.Recipient
			case KindCancel:
				if batch.PendingRecipient == "" {
					return errors.New("no pending transfer to cancel")
				}
				batch.PendingRecipient = ""
			}
		}
	}
	batch.Sequence, batch.LastEvent = e.Sequence, e.ID
	if s.Batches == nil {
		s.Batches = make(map[string]Batch)
	}
	s.Batches[e.BatchID] = batch
	return nil
}
