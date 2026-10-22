// supplychain runs an educational custody blockchain, signs events locally,
// and talks to its JSON API. Only signed public events leave this process.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"supplychain/api"
	"supplychain/chain"
	"supplychain/ledger"
	"supplychain/node"
	"syscall"
	"time"
)

const help = `Usage: supplychain COMMAND [flags]

Commands:
  keygen   Generate an organization key and public participant file
  init     Create shared network configuration from participant files
  run      Run a persistent P2P node and local HTTP API
  event    Sign create/checkpoint/offer/accept/cancel locally
  submit   Submit a signed event file (--file)
  mine     Mine one block (including an empty confirmation block)
  status   Show chain and node status
  peers    Show node identity, addresses and approved peers
  batches  List confirmed batches
  batch    Get confirmed batch (--id)
  trace    Get confirmed history and confirmations (--id)
  pending  List pending events

Use COMMAND --help for flags. Query commands use --api http://127.0.0.1:8080.
Private organization keys are never sent to a node.
`

type keyFile struct {
	Participant ledger.Participant `json:"participant"`
	PrivateKey  string             `json:"private_key"`
}
type Ready struct {
	APIURL    string   `json:"api_url"`
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
	DataDir   string   `json:"data_dir"`
}
type stringsFlag []string

func (s *stringsFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringsFlag) Set(v string) error { *s = append(*s, v); return nil }
func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
func flags(name string, stderr io.Writer) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(stderr)
	return f
}
func parse(f *flag.FlagSet, args []string) error {
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(f.Args(), " "))
	}
	return nil
}
func run(ctx context.Context, args []string, out, stderr io.Writer) (err error) {
	defer func() {
		if errors.Is(err, flag.ErrHelp) {
			err = nil
		}
	}()
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		_, err = io.WriteString(out, help)
		return err
	}
	switch args[0] {
	case "keygen":
		return generate(args[1:], out, stderr)
	case "init":
		return initialize(args[1:], out, stderr)
	case "run":
		return serve(ctx, args[1:], out, stderr)
	case "event":
		return eventCommand(ctx, args[1:], out, stderr)
	case "status", "peers", "batches", "batch", "trace", "pending", "mine", "submit":
		return query(ctx, args[0], args[1:], out, stderr)
	default:
		return fmt.Errorf("unknown command %q; run supplychain help", args[0])
	}
}
func encode(out io.Writer, v any) error {
	e := json.NewEncoder(out)
	e.SetIndent("", "  ")
	return e.Encode(v)
}

// writeNew uses exclusive creation so keys/config/events are never overwritten.
func writeNew(path string, v any, mode os.FileMode) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(append(b, '\n'))
	}
	closeErr := f.Close()
	if err != nil {
		os.Remove(path)
		return err
	}
	return closeErr
}
func readJSON(path string, dst any, limit int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > limit {
		return fmt.Errorf("%s exceeds %d bytes", path, limit)
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err = d.Decode(dst); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	var extra any
	if err = d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("%s must contain one JSON value", path)
	}
	return nil
}
func loadConfig(path string) (ledger.Config, error) {
	var c ledger.Config
	if err := readJSON(path, &c, 1<<20); err != nil {
		return c, err
	}
	return ledger.NormalizeConfig(c)
}
func generate(args []string, out, stderr io.Writer) error {
	f := flags("keygen", stderr)
	name := f.String("name", "", "organization name (required)")
	role := f.String("role", "", "supplier/manufacturer/carrier/distributor/retailer/auditor")
	keyPath := f.String("out", "", "new private key JSON path (0600, required)")
	publicPath := f.String("public", "", "new public participant JSON path (required)")
	if err := parse(f, args); err != nil {
		return err
	}
	if *keyPath == "" || *publicPath == "" || *keyPath == *publicPath {
		return errors.New("distinct --out and --public paths are required")
	}
	pub, key, err := ledger.GenerateKey()
	if err != nil {
		return err
	}
	p, err := ledger.NewParticipant(*name, *role, pub)
	if err != nil {
		return err
	}
	if err = writeNew(*keyPath, keyFile{p, hex.EncodeToString(key)}, 0600); err != nil {
		return err
	}
	if err = writeNew(*publicPath, p, 0644); err != nil {
		os.Remove(*keyPath)
		return err
	}
	return encode(out, p)
}
func initialize(args []string, out, stderr io.Writer) error {
	f := flags("init", stderr)
	id := f.String("chain-id", "", "shared chain identifier (required)")
	difficulty := f.Uint("difficulty", 12, "fixed leading-zero proof bits (1..24)")
	path := f.String("out", "", "new network JSON path (required)")
	var files stringsFlag
	f.Var(&files, "participant", "public participant JSON (repeat)")
	if err := parse(f, args); err != nil {
		return err
	}
	if *path == "" || len(files) == 0 {
		return errors.New("--out and at least one --participant are required")
	}
	if *difficulty > 24 || *difficulty < 1 {
		return errors.New("difficulty must be 1..24")
	}
	c := ledger.Config{ChainID: *id, Difficulty: uint8(*difficulty)}
	for _, file := range files {
		var p ledger.Participant
		if err := readJSON(file, &p, 4096); err != nil {
			return err
		}
		c.Participants = append(c.Participants, p)
	}
	c, err := ledger.NormalizeConfig(c)
	if err != nil {
		return err
	}
	if err = writeNew(*path, c, 0644); err != nil {
		return err
	}
	return encode(out, c)
}
func serve(parent context.Context, args []string, out, stderr io.Writer) error {
	f := flags("run", stderr)
	configPath := f.String("config", "", "shared network JSON (required)")
	data := f.String("data", "", "node data directory (required, one process per directory)")
	bind := f.String("api-bind", "127.0.0.1:8080", "HTTP listen address; external exposure requires explicit bind")
	readyPath := f.String("ready-file", "", "write startup metadata atomically to this path")
	var listens, boots, announces, relays stringsFlag
	f.Var(&listens, "listen", "listen multiaddress (repeat; default /ip4/0.0.0.0/tcp/0)")
	f.Var(&boots, "bootstrap", "full peer /p2p multiaddress (repeat)")
	f.Var(&announces, "announce", "reachable public multiaddress (repeat)")
	f.Var(&relays, "relay-peer", "static relay full /p2p multiaddress (repeat)")
	nc := node.Config{}
	f.BoolVar(&nc.EnableMDNS, "mdns", false, "enable LAN multicast discovery")
	f.BoolVar(&nc.EnableDHT, "dht", false, "enable scoped Kademlia discovery")
	f.BoolVar(&nc.EnableRelayService, "relay-service", false, "serve Circuit Relay v2")
	f.BoolVar(&nc.EnableHolePunching, "hole-punch", false, "enable coordinated hole punching")
	f.BoolVar(&nc.ForcePublic, "force-public", false, "declare known public reachability")
	f.BoolVar(&nc.ForcePrivate, "force-private", false, "declare private reachability for AutoRelay")
	f.IntVar(&nc.MaxPeers, "max-peers", 64, "hard peer limit (1..64)")
	f.DurationVar(&nc.SyncInterval, "sync-interval", 3*time.Second, "periodic verified anti-entropy interval")
	f.DurationVar(&nc.RequestTimeout, "request-timeout", 5*time.Second, "peer request deadline")
	automine := f.Duration("automine", 0, "optional mining poll interval, only mines pending events (0 disables)")
	if err := parse(f, args); err != nil {
		return err
	}
	if *data == "" || *configPath == "" {
		return errors.New("--config and --data are required")
	}
	if *automine < 0 || (*automine > 0 && *automine < 50*time.Millisecond) {
		return errors.New("--automine must be zero or at least 50ms")
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(*data, 0700); err != nil {
		return err
	}
	c, err := chain.Open(filepath.Join(*data, "chain.db"), cfg)
	if err != nil {
		return err
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	nc.DataDir = *data
	nc.ListenAddrs = listens
	nc.BootstrapPeers = boots
	nc.AnnounceAddrs = announces
	nc.RelayPeers = relays
	n, err := node.New(ctx, c, nc)
	if err != nil {
		return err
	}
	defer n.Close()
	listener, err := net.Listen("tcp", *bind)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: api.New(c, n), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		server.Shutdown(shutdown)
		server.Close()
	}()
	address := listener.Addr().String()
	host, port, _ := net.SplitHostPort(address)
	if host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	meta := Ready{APIURL: "http://" + net.JoinHostPort(host, port), ID: n.ID(), Addresses: n.Addrs(), DataDir: *data}
	if *readyPath != "" {
		encoded, _ := json.Marshal(meta)
		temp, err := os.CreateTemp(filepath.Dir(*readyPath), ".ready-*")
		if err != nil {
			return err
		}
		tmp := temp.Name()
		defer os.Remove(tmp)
		if _, err = temp.Write(append(encoded, '\n')); err != nil {
			temp.Close()
			return err
		}
		if err = temp.Close(); err != nil {
			return err
		}
		if err = os.Rename(tmp, *readyPath); err != nil {
			return err
		}
	}
	if err = encode(out, meta); err != nil {
		return err
	}
	var tick <-chan time.Time
	var timer *time.Ticker
	if *automine > 0 {
		timer = time.NewTicker(*automine)
		tick = timer.C
		defer timer.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-done:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-tick:
			if len(c.Pending()) > 0 {
				mineCtx, stop := context.WithTimeout(ctx, 30*time.Second)
				_, err := n.Mine(mineCtx)
				stop()
				if err != nil && ctx.Err() == nil {
					fmt.Fprintln(stderr, "automine:", err)
				}
			}
		}
	}
}
func call(ctx context.Context, base, method, path string, value any) ([]byte, error) {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return nil, errors.New("--api must be an http(s) origin such as http://127.0.0.1:8080")
	}
	var body io.Reader
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return nil, err
	}
	if value != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 40 * time.Second}
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20+1))
	if err != nil {
		return nil, err
	}
	if len(b) > 8<<20 {
		return nil, errors.New("API response exceeds 8 MiB")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("API %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	if !json.Valid(b) {
		return nil, errors.New("API returned invalid JSON")
	}
	return b, nil
}
func query(ctx context.Context, command string, args []string, out, stderr io.Writer) error {
	f := flags(command, stderr)
	base := f.String("api", "http://127.0.0.1:8080", "node API origin")
	var id, file *string
	if command == "batch" || command == "trace" {
		id = f.String("id", "", "human batch ID (required)")
	}
	if command == "submit" {
		file = f.String("file", "", "signed event JSON path (required)")
	}
	if err := parse(f, args); err != nil {
		return err
	}
	path := "/v1/" + command
	method := "GET"
	var body any
	if id != nil {
		if *id == "" {
			return errors.New("--id is required")
		}
		path += "?id=" + url.QueryEscape(*id)
	}
	if command == "mine" {
		method = "POST"
		body = struct{}{}
	}
	if command == "submit" {
		if *file == "" {
			return errors.New("--file is required")
		}
		var e ledger.Event
		if err := readJSON(*file, &e, ledger.MaxEventBytes); err != nil {
			return err
		}
		body = e
		path = "/v1/events"
		method = "POST"
	}
	b, err := call(ctx, *base, method, path, body)
	if err != nil {
		return err
	}
	_, err = out.Write(b)
	return err
}
func eventCommand(ctx context.Context, args []string, out, stderr io.Writer) error {
	f := flags("event", stderr)
	kind := f.String("kind", "", "create/checkpoint/offer/accept/cancel (required)")
	keyPath := f.String("key", "", "local organization private key JSON (required)")
	configPath := f.String("config", "", "shared network config JSON (required)")
	id := f.String("batch-id", "", "human batch ID (required)")
	base := f.String("api", "", "API origin to fill confirmed predecessor for updates")
	path := f.String("out", "", "new signed event file; default stdout")
	submit := f.Bool("submit", false, "submit signed event after signing (--api required)")
	e := ledger.Event{}
	f.StringVar(&e.Recipient, "recipient", "", "enrolled recipient ID (offer only)")
	f.StringVar(&e.Name, "name", "", "product name (create only)")
	f.StringVar(&e.Unit, "unit", "", "quantity unit (create only)")
	f.Int64Var(&e.Quantity, "quantity", 0, "positive fixed quantity (create only)")
	f.StringVar(&e.Location, "location", "", "checkpoint location (create/checkpoint)")
	f.StringVar(&e.Status, "status", "", "checkpoint status (create/checkpoint)")
	f.StringVar(&e.DocumentHash, "document-hash", "", "SHA-256 of external document (create/checkpoint)")
	f.Uint64Var(&e.Sequence, "sequence", 0, "explicit sequence for offline signing")
	f.StringVar(&e.PreviousEvent, "previous", "", "explicit predecessor event ID for offline signing")
	f.Int64Var(&e.Timestamp, "timestamp", time.Now().Unix(), "actor timestamp in Unix seconds")
	if err := parse(f, args); err != nil {
		return err
	}
	if *kind == "" || *keyPath == "" || *configPath == "" || *id == "" {
		return errors.New("--kind, --key, --config and --batch-id are required")
	}
	if *submit && *base == "" {
		return errors.New("--submit requires --api")
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}
	var k keyFile
	if err = readJSON(*keyPath, &k, 4096); err != nil {
		return err
	}
	key, err := ledger.ParsePrivateKey(k.PrivateKey)
	if err != nil {
		return err
	}
	actor := ledger.PublicKeyID(key.Public().(ed25519.PublicKey))
	if actor != k.Participant.ID || k.Participant.PublicKey != hex.EncodeToString(key.Public().(ed25519.PublicKey)) {
		return errors.New("private key does not match participant")
	}
	e.Version = ledger.EventVersion
	e.ChainID = cfg.ChainID
	e.Kind = *kind
	e.BatchID = *id
	e.Actor = actor
	if e.Kind == ledger.KindCreate {
		if e.Sequence == 0 {
			e.Sequence = 1
		}
	} else if e.Sequence == 0 || e.PreviousEvent == "" {
		if e.Sequence != 0 || e.PreviousEvent != "" {
			return errors.New("offline updates require both --sequence and --previous")
		}
		if *base == "" {
			return errors.New("updates require --api or both --sequence and --previous")
		}
		b, err := call(ctx, *base, "GET", "/v1/batch?id="+url.QueryEscape(*id), nil)
		if err != nil {
			return err
		}
		var batch ledger.Batch
		if err = json.Unmarshal(b, &batch); err != nil {
			return err
		}
		if batch.ID != *id || batch.Sequence == ^uint64(0) {
			return errors.New("invalid batch state from API")
		}
		e.Sequence = batch.Sequence + 1
		e.PreviousEvent = batch.LastEvent
	}
	if err = ledger.SignEvent(&e, key); err != nil {
		return err
	}
	if err = ledger.VerifyEvent(cfg, e); err != nil {
		return err
	}
	if *path != "" {
		if err = writeNew(*path, e, 0644); err != nil {
			return err
		}
	}
	if *submit {
		if _, err = call(ctx, *base, "POST", "/v1/events", e); err != nil {
			return err
		}
	}
	return encode(out, e)
}
