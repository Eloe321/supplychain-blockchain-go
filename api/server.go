// Package api exposes a local JSON interface. It never handles private keys.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"supplychain/chain"
	"supplychain/ledger"
	"supplychain/node"
	"time"
)

type PeerNode interface {
	ID() string
	Addrs() []string
	Peers() []string
	Submit(context.Context, ledger.Event) error
	Mine(context.Context) (ledger.Block, error)
}
type NodeStatus struct {
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
	Peers     []string `json:"peers"`
}
type Status struct {
	chain.Status
	Node NodeStatus `json:"node"`
}
type Server struct {
	chain       *chain.Chain
	node        PeerNode
	MineTimeout time.Duration
}

// New creates a handler. The caller owns the chain and node lifecycle.
func New(c *chain.Chain, n PeerNode) *Server {
	return &Server{chain: c, node: n, MineTimeout: 30 * time.Second}
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, err string) {
	write(w, status, struct {
		Error string `json:"error"`
	}{err})
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	routes := map[string]string{"/v1/status": "GET", "/v1/peers": "GET", "/v1/batches": "GET", "/v1/batch": "GET", "/v1/trace": "GET", "/v1/pending": "GET", "/v1/events": "POST", "/v1/mine": "POST"}
	method, ok := routes[r.URL.Path]
	if !ok {
		fail(w, 404, "route not found")
		return
	}
	if method != r.Method {
		w.Header().Set("Allow", method)
		fail(w, 405, "method must be "+method)
		return
	}
	if err := r.Context().Err(); err != nil {
		s.failure(w, err)
		return
	}
	if _, err := s.chain.Locators(); err != nil {
		s.failure(w, err)
		return
	}
	if method == "POST" {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			fail(w, 415, "Content-Type must be application/json")
			return
		}
	}
	switch r.URL.Path {
	case "/v1/status":
		write(w, 200, Status{s.chain.Status(), NodeStatus{s.node.ID(), s.node.Addrs(), s.node.Peers()}})
	case "/v1/peers":
		write(w, 200, NodeStatus{s.node.ID(), s.node.Addrs(), s.node.Peers()})
	case "/v1/batches":
		write(w, 200, s.chain.Batches())
	case "/v1/pending":
		write(w, 200, s.chain.Pending())
	case "/v1/batch", "/v1/trace":
		values := r.URL.Query()["id"]
		if len(values) != 1 || values[0] == "" {
			fail(w, 400, "exactly one id query parameter is required")
			return
		}
		id := values[0]
		batch, ok := s.chain.Batch(id)
		if !ok {
			fail(w, 404, "batch not found")
			return
		}
		if r.URL.Path == "/v1/batch" {
			write(w, 200, batch)
		} else {
			write(w, 200, s.chain.Trace(id))
		}
	case "/v1/events":
		var e ledger.Event
		if !decode(w, r, &e, ledger.MaxEventBytes) {
			return
		}
		if err := ledger.VerifyEvent(s.chain.Config(), e); err != nil {
			fail(w, 400, err.Error())
			return
		}
		if err := s.node.Submit(r.Context(), e); err != nil {
			s.failure(w, err)
			return
		}
		write(w, 202, struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		}{e.ID, "pending"})
	case "/v1/mine":
		var empty struct{}
		if !decode(w, r, &empty, 1024) {
			return
		}
		timeout := s.MineTimeout
		if timeout <= 0 || timeout > time.Minute {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		block, err := s.node.Mine(ctx)
		if err != nil {
			s.failure(w, err)
			return
		}
		write(w, 200, block)
	}
}
func decode(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			fail(w, 413, "request body too large")
		} else {
			fail(w, 400, "cannot read request body")
		}
		return false
	}
	// Reject JSON null as well as unknown fields and additional values.
	if len(data) == 0 {
		fail(w, 400, "JSON object required")
		return false
	}
	first := 0
	for first < len(data) && (data[first] == ' ' || data[first] == '\n' || data[first] == '\r' || data[first] == '\t') {
		first++
	}
	if first == len(data) || data[first] != '{' {
		fail(w, 400, "JSON object required")
		return false
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		fail(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		fail(w, 400, "request must contain one JSON value")
		return false
	}
	return true
}
func (s *Server) failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		fail(w, 504, "operation deadline exceeded")
	case errors.Is(err, context.Canceled), errors.Is(err, node.ErrClosed), errors.Is(err, chain.ErrClosed):
		fail(w, 503, "node unavailable")
	case errors.Is(err, chain.ErrMempoolFull):
		fail(w, 503, err.Error())
	default:
		fail(w, 409, err.Error())
	}
}
