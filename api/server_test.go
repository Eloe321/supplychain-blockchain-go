package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"supplychain/chain"
	"supplychain/ledger"
	"supplychain/node"
	"testing"
	"time"
)

func fixture(t *testing.T) (*chain.Chain, *node.Node, []ed25519.PrivateKey) {
	t.Helper()
	cfg := ledger.Config{ChainID: "api-test", Difficulty: 1}
	keys := []ed25519.PrivateKey{}
	for _, role := range []string{"supplier", "carrier"} {
		pub, key, _ := ledger.GenerateKey()
		p, _ := ledger.NewParticipant(role, role, pub)
		cfg.Participants = append(cfg.Participants, p)
		keys = append(keys, key)
	}
	c, err := chain.Open(filepath.Join(t.TempDir(), "chain.db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	n, err := node.New(context.Background(), c, node.Config{DataDir: t.TempDir(), ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(); c.Close() })
	return c, n, keys
}
func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestCustodyAPI(t *testing.T) {
	c, n, keys := fixture(t)
	h := New(c, n)
	id := "lot / 2026 A"
	e := ledger.Event{Version: 1, ChainID: c.Config().ChainID, Kind: "create", BatchID: id, Actor: ledger.PublicKeyID(keys[0].Public().(ed25519.PublicKey)), Name: "Coffee", Quantity: 50, Unit: "kg", Sequence: 1, Timestamp: time.Now().Unix()}
	for i, kind := range []string{"create", "offer", "accept"} {
		if i > 0 {
			e = ledger.Event{Version: 1, ChainID: c.Config().ChainID, Kind: kind, BatchID: id, Actor: ledger.PublicKeyID(keys[0].Public().(ed25519.PublicKey)), Sequence: uint64(i + 1), PreviousEvent: e.ID, Timestamp: time.Now().Unix()}
			if kind == "offer" {
				e.Recipient = ledger.PublicKeyID(keys[1].Public().(ed25519.PublicKey))
			} else {
				e.Actor = ledger.PublicKeyID(keys[1].Public().(ed25519.PublicKey))
			}
		}
		key := keys[0]
		if kind == "accept" {
			key = keys[1]
		}
		if err := ledger.SignEvent(&e, key); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(e)
		if w := request(h, "POST", "/v1/events", string(body)); w.Code != 202 {
			t.Fatalf("submit %s: %d %s", kind, w.Code, w.Body)
		}
		if w := request(h, "POST", "/v1/events", string(body)); w.Code != 409 {
			t.Fatalf("duplicate: %d %s", w.Code, w.Body)
		}
		if w := request(h, "POST", "/v1/mine", "{}"); w.Code != 200 {
			t.Fatalf("mine: %d %s", w.Code, w.Body)
		}
	}
	for _, path := range []string{"/v1/status", "/v1/peers", "/v1/batches", "/v1/pending", "/v1/batch?id=" + url.QueryEscape(id), "/v1/trace?id=" + url.QueryEscape(id)} {
		w := request(h, "GET", path, "")
		if w.Code != 200 || !json.Valid(w.Body.Bytes()) {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body)
		}
	}
	w := request(h, "GET", "/v1/trace?id="+url.QueryEscape(id), "")
	var trace []chain.TraceEntry
	json.Unmarshal(w.Body.Bytes(), &trace)
	if len(trace) != 3 || trace[0].Confirmations != 3 || trace[2].Confirmations != 1 {
		t.Fatalf("trace %s", w.Body)
	}
	b, _ := c.Batch(id)
	if b.Owner != ledger.PublicKeyID(keys[1].Public().(ed25519.PublicKey)) || b.PendingRecipient != "" {
		t.Fatal(b)
	}
}
func TestStrictHTTP(t *testing.T) {
	c, n, _ := fixture(t)
	h := New(c, n)
	for _, tt := range []struct {
		method, path, body string
		want               int
	}{
		{"POST", "/v1/events", "{", 400}, {"POST", "/v1/events", "{} {}", 400}, {"POST", "/v1/events", `{"surprise":1}`, 400}, {"POST", "/v1/events", "{}", 400},
		{"POST", "/v1/events", strings.Repeat(" ", ledger.MaxEventBytes+1), 413}, {"POST", "/v1/mine", `{"surprise":1}`, 400},
		{"GET", "/v1/batch?id=missing", "", 404}, {"GET", "/v1/trace?id=missing", "", 404}, {"GET", "/v1/batch", "", 400},
		{"POST", "/v1/status", "{}", 405}, {"GET", "/v1/mine", "", 405}, {"GET", "/absent", "", 404},
	} {
		w := request(h, tt.method, tt.path, tt.body)
		if w.Code != tt.want || !json.Valid(w.Body.Bytes()) {
			t.Errorf("%s %s want %d got %d %s", tt.method, tt.path, tt.want, w.Code, w.Body)
		}
	}
	r := httptest.NewRequest("POST", "/v1/mine", bytes.NewBufferString("{}"))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 415 {
		t.Fatal(w.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r = httptest.NewRequest("POST", "/v1/mine", strings.NewReader("{}")).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("canceled %d", w.Code)
	}
	c.Close()
	if w := request(h, "GET", "/v1/status", ""); w.Code != 503 {
		t.Fatalf("closed %d", w.Code)
	}
}
func TestInvalidSignatureAndTransition(t *testing.T) {
	c, n, keys := fixture(t)
	h := New(c, n)
	e := ledger.Event{Version: 1, ChainID: c.Config().ChainID, Kind: "create", BatchID: "x", Actor: ledger.PublicKeyID(keys[0].Public().(ed25519.PublicKey)), Name: "x", Unit: "kg", Quantity: 1, Sequence: 1, Timestamp: 1}
	ledger.SignEvent(&e, keys[0])
	e.Signature = strings.Repeat("0", 128)
	b, _ := json.Marshal(e)
	if w := request(h, "POST", "/v1/events", string(b)); w.Code != 400 {
		t.Fatal(w.Code)
	}
	e.Kind = "checkpoint"
	e.Name = ""
	e.Unit = ""
	e.Quantity = 0
	e.Sequence = 2
	e.PreviousEvent = strings.Repeat("1", 64)
	ledger.SignEvent(&e, keys[0])
	b, _ = json.Marshal(e)
	if w := request(h, "POST", "/v1/events", string(b)); w.Code != 409 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

type deadlineNode struct{ PeerNode }

func (n deadlineNode) Mine(ctx context.Context) (ledger.Block, error) {
	<-ctx.Done()
	return ledger.Block{}, ctx.Err()
}
func TestMiningDeadlineAndNodeClosure(t *testing.T) {
	c, n, keys := fixture(t)
	h := New(c, deadlineNode{n})
	h.MineTimeout = time.Millisecond
	if w := request(h, "POST", "/v1/mine", "{}"); w.Code != 504 {
		t.Fatalf("deadline %d %s", w.Code, w.Body)
	}
	n.Close()
	h = New(c, n)
	if w := request(h, "POST", "/v1/mine", "{}"); w.Code != 503 {
		t.Fatalf("closed node %d %s", w.Code, w.Body)
	}
	e := ledger.Event{Version: 1, ChainID: c.Config().ChainID, Kind: "create", BatchID: "closed", Actor: ledger.PublicKeyID(keys[0].Public().(ed25519.PublicKey)), Name: "x", Unit: "kg", Quantity: 1, Sequence: 1, Timestamp: 1}
	ledger.SignEvent(&e, keys[0])
	b, _ := json.Marshal(e)
	if w := request(h, "POST", "/v1/events", string(b)); w.Code != 503 {
		t.Fatalf("closed submit %d %s", w.Code, w.Body)
	}
	for _, body := range []string{"null", "[]", "{} {}"} {
		if w := request(h, "POST", "/v1/mine", body); w.Code != 400 {
			t.Fatalf("body %q: %d", body, w.Code)
		}
	}
	if w := request(h, "POST", "/v1/mine", strings.Repeat(" ", 1025)); w.Code != 413 {
		t.Fatal(w.Code)
	}
}
