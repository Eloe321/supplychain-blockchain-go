package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"supplychain/api"
	"supplychain/ledger"
	"testing"
	"time"
)

func invoke(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out, errOut bytes.Buffer
	err := run(context.Background(), args, &out, &errOut)
	return out.String(), err
}
func setupCLI(t *testing.T) (string, string, ledger.Participant) {
	t.Helper()
	dir := t.TempDir()
	key := filepath.Join(dir, "actor.key.json")
	pub := filepath.Join(dir, "actor.json")
	cfg := filepath.Join(dir, "network.json")
	if _, err := invoke(t, "keygen", "--name", "Farm", "--role", "supplier", "--out", key, "--public", pub); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(pub)
	var p ledger.Participant
	json.Unmarshal(b, &p)
	if _, err := invoke(t, "init", "--chain-id", "cli-test", "--difficulty", "1", "--participant", pub, "--out", cfg); err != nil {
		t.Fatal(err)
	}
	return key, cfg, p
}
func TestKeyConfigRoundTripAndSigning(t *testing.T) {
	key, cfg, p := setupCLI(t)
	info, _ := os.Stat(key)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	if _, err := invoke(t, "keygen", "--name", "Farm", "--role", "supplier", "--out", key, "--public", key+".pub"); err == nil {
		t.Fatal("overwrote key")
	}
	body, err := invoke(t, "event", "--kind", "create", "--key", key, "--config", cfg, "--batch-id", "lot / A", "--name", "Coffee", "--quantity", "5", "--unit", "kg")
	if err != nil {
		t.Fatal(err)
	}
	var event ledger.Event
	if err = json.Unmarshal([]byte(body), &event); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(cfg)
	var network ledger.Config
	json.Unmarshal(b, &network)
	if event.Actor != p.ID || event.Sequence != 1 || event.BatchID != "lot / A" {
		t.Fatal(event)
	}
	if err = ledger.VerifyEvent(network, event); err != nil {
		t.Fatal(err)
	}
	_, err = invoke(t, "event", "--kind", "checkpoint", "--key", key, "--config", cfg, "--batch-id", "lot / A")
	if err == nil {
		t.Fatal("missing predecessor allowed")
	}
}
func TestClientRoutesAndErrors(t *testing.T) {
	seen := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/trace" {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"error":"batch not found"}`)
			return
		}
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	for _, command := range []string{"status", "peers", "batches", "pending", "mine"} {
		if _, err := invoke(t, command, "--api", server.URL); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := invoke(t, "batch", "--api", server.URL, "--id", "lot / A"); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke(t, "trace", "--api", server.URL, "--id", "lot / A"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatal(err)
	}
	joined := strings.Join(seen, "\n")
	if !strings.Contains(joined, "POST /v1/mine") || !strings.Contains(joined, "GET /v1/batch?id=lot+%2F+A") {
		t.Fatal(joined)
	}
}
func TestHelpAndInvalidFlags(t *testing.T) {
	for _, args := range [][]string{{}, {"help"}, {"--help"}, {"event", "--help"}, {"run", "--help"}} {
		out, err := invoke(t, args...)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if len(args) < 2 && out == "" {
			t.Fatal("empty help")
		}
	}
	for _, args := range [][]string{{"wat"}, {"status", "--unknown"}, {"status", "extra"}, {"keygen"}, {"init", "--difficulty", "256"}, {"run", "--config", "missing"}, {"event", "--kind", "bogus"}} {
		if _, err := invoke(t, args...); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
func TestRunReadyAndCancellation(t *testing.T) {
	_, cfg, _ := setupCLI(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := filepath.Join(t.TempDir(), "ready.json")
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"run", "--config", cfg, "--data", t.TempDir(), "--api-bind", "127.0.0.1:0", "--listen", "/ip4/127.0.0.1/tcp/0", "--ready-file", ready}, &bytes.Buffer{}, &bytes.Buffer{})
	}()
	deadline := time.Now().Add(10 * time.Second)
	var meta Ready
	for time.Now().Before(deadline) {
		b, err := os.ReadFile(ready)
		if err == nil && json.Unmarshal(b, &meta) == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("startup: %v", err)
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if meta.APIURL == "" || len(meta.Addresses) == 0 {
		t.Fatal("not ready")
	}
	r, err := http.Get(meta.APIURL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	var status api.Status
	if err = json.NewDecoder(r.Body).Decode(&status); err != nil || status.ChainID != "cli-test" {
		t.Fatal(status, err)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop")
	}
}
func TestAutofillAndOfflineEvents(t *testing.T) {
	key, cfg, p := setupCLI(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("id") != "lot / A" {
			t.Error(r.URL)
		}
		json.NewEncoder(w).Encode(ledger.Batch{ID: "lot / A", Owner: p.ID, Sequence: 4, LastEvent: strings.Repeat("a", 64)})
	}))
	defer server.Close()
	body, err := invoke(t, "event", "--kind", "checkpoint", "--key", key, "--config", cfg, "--batch-id", "lot / A", "--api", server.URL, "--status", "arrived")
	if err != nil {
		t.Fatal(err)
	}
	var e ledger.Event
	json.Unmarshal([]byte(body), &e)
	if e.Sequence != 5 || e.PreviousEvent != strings.Repeat("a", 64) {
		t.Fatal(e)
	}
	body, err = invoke(t, "event", "--kind", "cancel", "--key", key, "--config", cfg, "--batch-id", "lot / A", "--sequence", "9", "--previous", strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal([]byte(body), &e)
	if e.Sequence != 9 {
		t.Fatal(e)
	}
}

func TestSignedTransferFilesAndSubmit(t *testing.T) {
	key, cfg, p := setupCLI(t)
	seen := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/events" || r.Header.Get("Content-Type") != "application/json" {
			t.Error(r.Method, r.URL)
		}
		var e ledger.Event
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			t.Error(err)
		}
		if e.Signature == "" || e.Actor != p.ID {
			t.Error(e)
		}
		seen = true
		w.WriteHeader(202)
		fmt.Fprint(w, `{"status":"pending"}`)
	}))
	defer server.Close()
	for _, kind := range []string{"offer", "accept", "cancel"} {
		args := []string{"event", "--kind", kind, "--key", key, "--config", cfg, "--batch-id", "x", "--sequence", "2", "--previous", strings.Repeat("a", 64)}
		if kind == "offer" {
			args = append(args, "--recipient", strings.Repeat("b", 64))
		}
		body, err := invoke(t, args...)
		if err != nil {
			t.Fatal(err)
		}
		var e ledger.Event
		json.Unmarshal([]byte(body), &e)
		if e.Kind != kind || e.Signature == "" {
			t.Fatal(e)
		}
		file := filepath.Join(t.TempDir(), kind+".json")
		os.WriteFile(file, []byte(body), 0644)
		if _, err = invoke(t, "submit", "--api", server.URL, "--file", file); err != nil {
			t.Fatal(err)
		}
	}
	if !seen {
		t.Fatal("no submit")
	}
}
