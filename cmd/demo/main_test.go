package main

import (
	"context"
	"errors"
	"supplychain/chain"
	"supplychain/ledger"
	"testing"
	"time"
)

func TestPollDeadlineAndSuccess(t *testing.T) {
	n := 0
	if err := poll(context.Background(), time.Second, func() error {
		n++
		if n < 2 {
			return errors.New("waiting")
		}
		return nil
	}); err != nil || n != 2 {
		t.Fatal(n, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := poll(ctx, time.Second, func() error { return errors.New("waiting") }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestAgreementChecksTipCustodyAndTrace(t *testing.T) {
	a := agreementFixture()
	b := a
	if err := agreement([]snapshot{a, b}, "carrier", 3); err != nil {
		t.Fatal(err)
	}
	b.Status.Hash = "other"
	if err := agreement([]snapshot{a, b}, "carrier", 3); err == nil {
		t.Fatal("different tip accepted")
	}
	b = a
	b.Batch.Owner = "supplier"
	if err := agreement([]snapshot{a, b}, "carrier", 3); err == nil {
		t.Fatal("different owner accepted")
	}
	b = a
	b.Trace = b.Trace[:2]
	if err := agreement([]snapshot{a, b}, "carrier", 3); err == nil {
		t.Fatal("short trace accepted")
	}
}

func agreementFixture() snapshot {
	return snapshot{Status: chain.Status{Hash: "block3", Height: 3}, Batch: ledger.Batch{ID: "lot", Owner: "carrier", Sequence: 3, LastEvent: "event3"}, Trace: []chain.TraceEntry{
		{Event: ledger.Event{ID: "event1", BatchID: "lot", Kind: "create", Sequence: 1}, BlockHash: "block1", Height: 1, Confirmations: 3},
		{Event: ledger.Event{ID: "event2", BatchID: "lot", Kind: "offer", Sequence: 2, PreviousEvent: "event1"}, BlockHash: "block2", Height: 2, Confirmations: 2},
		{Event: ledger.Event{ID: "event3", BatchID: "lot", Kind: "accept", Sequence: 3, PreviousEvent: "event2"}, BlockHash: "block3", Height: 3, Confirmations: 1},
	}}
}
func TestAgreementRejectsDifferentEqualLengthHistories(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*chain.TraceEntry)
	}{
		{"event ID", func(e *chain.TraceEntry) { e.Event.ID = "forged" }},
		{"event payload", func(e *chain.TraceEntry) { e.Event.Location = "elsewhere" }},
		{"block hash", func(e *chain.TraceEntry) { e.BlockHash = "other" }},
		{"height", func(e *chain.TraceEntry) { e.Height = 2 }},
		{"confirmations", func(e *chain.TraceEntry) { e.Confirmations = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, b := agreementFixture(), agreementFixture()
			tt.change(&b.Trace[0])
			if err := agreement([]snapshot{a, b}, "carrier", 3); err == nil {
				t.Fatal("different equal-length histories accepted")
			}
		})
	}
}
func TestAgreementRejectsInternallyInconsistentHistory(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*snapshot)
	}{
		{"sequence", func(s *snapshot) { s.Trace[1].Event.Sequence = 7 }},
		{"predecessor", func(s *snapshot) { s.Trace[1].Event.PreviousEvent = "other" }},
		{"tip", func(s *snapshot) { s.Trace[2].BlockHash = "other" }},
		{"batch last event", func(s *snapshot) { s.Batch.LastEvent = "other" }},
		{"batch ID", func(s *snapshot) { s.Trace[1].Event.BatchID = "other" }},
		{"confirmation depth", func(s *snapshot) { s.Trace[1].Confirmations = 1 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := agreementFixture()
			tt.change(&a)
			if err := agreement([]snapshot{a, a}, "carrier", 3); err == nil {
				t.Fatal("inconsistent trace accepted")
			}
		})
	}
}
