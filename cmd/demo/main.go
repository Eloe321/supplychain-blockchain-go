// demo starts three independent supplychain executables with isolated storage.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"supplychain/api"
	"supplychain/chain"
	"supplychain/ledger"
	"syscall"
	"time"
)

type ready struct {
	APIURL    string   `json:"api_url"`
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
}
type process struct {
	cmd     *exec.Cmd
	done    chan error
	log     *os.File
	path    string
	meta    ready
	stopped bool
}
type snapshot struct {
	Status chain.Status
	Batch  ledger.Batch
	Trace  []chain.TraceEntry
}

func main() {
	binary := flag.String("binary", "bin/supplychain", "path to built supplychain executable")
	keep := flag.Bool("keep", false, "keep self-created demo directory for inspection")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, stop := context.WithTimeout(ctx, 2*time.Minute)
	defer stop()
	if err := demo(ctx, *binary, *keep); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}
func poll(ctx context.Context, timeout time.Duration, check func() error) error {
	deadline, stop := context.WithTimeout(ctx, timeout)
	defer stop()
	var last error
	for {
		if err := deadline.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("%w (last observation: %v)", err, last)
			}
			return err
		}
		last = check()
		if last == nil {
			return nil
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-deadline.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}
func agreement(states []snapshot, owner string, events int) error {
	if len(states) == 0 {
		return errors.New("no nodes")
	}
	for i, s := range states {
		if s.Status.Hash != states[0].Status.Hash || s.Status.Height != uint64(events) || s.Status.Pending != 0 {
			return fmt.Errorf("node %d tip/pending differs: %+v", i, s.Status)
		}
		if s.Batch.Owner != owner || s.Batch.PendingRecipient != "" || s.Batch.Sequence != uint64(events) || len(s.Trace) != events {
			return fmt.Errorf("node %d custody/history differs", i)
		}
		previous := ""
		for j, entry := range s.Trace {
			if entry != states[0].Trace[j] {
				return fmt.Errorf("node %d trace entry %d differs", i, j)
			}
			// The demo mines exactly one event per block, so each sequence,
			// block height and confirmation depth has a known expected value.
			if entry.Event.ID == "" || entry.Event.BatchID != s.Batch.ID || entry.Event.Sequence != uint64(j+1) || entry.Event.PreviousEvent != previous || entry.Height != uint64(j+1) || entry.Confirmations != uint64(events-j) {
				return fmt.Errorf("node %d trace entry %d is inconsistent", i, j)
			}
			previous = entry.Event.ID
		}
		if events == 0 || s.Trace[events-1].BlockHash != s.Status.Hash || s.Batch.LastEvent != previous {
			return fmt.Errorf("node %d trace does not match its tip/batch", i)
		}
	}
	return nil
}
func readAPI(ctx context.Context, base, path string, dst any) error {
	r, err := http.NewRequestWithContext(ctx, "GET", base+path, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 2<<20)).Decode(dst)
}
func stopProcess(p *process) {
	if p.stopped {
		return
	}
	p.stopped = true
	_ = p.cmd.Process.Signal(os.Interrupt)
	select {
	case <-p.done:
	case <-time.After(8 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
	}
	p.log.Close()
}
func demo(ctx context.Context, binary string, keep bool) (result error) {
	binary, err := filepath.Abs(binary)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "supplychain-demo-*")
	if err != nil {
		return err
	}
	var processes []*process
	defer func() {
		for _, p := range processes {
			stopProcess(p)
		}
		if result != nil {
			for _, p := range processes {
				b, _ := os.ReadFile(p.path)
				fmt.Fprintf(os.Stderr, "\n--- %s ---\n%s", filepath.Base(p.path), b)
			}
		}
		if keep {
			fmt.Println("Demo files:", dir)
		} else {
			os.RemoveAll(dir)
		}
	}()
	command := func(args ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, binary, args...)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("supplychain %v: %w: %s", args, err, stderr.String())
		}
		return stdout.Bytes(), nil
	}
	supplierKey := filepath.Join(dir, "supplier.key.json")
	carrierKey := filepath.Join(dir, "carrier.key.json")
	supplierPub := filepath.Join(dir, "supplier.json")
	carrierPub := filepath.Join(dir, "carrier.json")
	config := filepath.Join(dir, "network.json")
	for _, k := range []struct{ role, key, pub string }{{"supplier", supplierKey, supplierPub}, {"carrier", carrierKey, carrierPub}} {
		if _, err = command("keygen", "--name", k.role, "--role", k.role, "--out", k.key, "--public", k.pub); err != nil {
			return err
		}
	}
	b, err := command("init", "--chain-id", "coffee-demo", "--participant", supplierPub, "--participant", carrierPub, "--out", config)
	if err != nil {
		return err
	}
	var cfg ledger.Config
	if err = json.Unmarshal(b, &cfg); err != nil {
		return err
	}
	ownerIDs := map[string]string{}
	for _, p := range cfg.Participants {
		ownerIDs[p.Role] = p.ID
	}
	start := func(index int, bootstrap []string) (*process, error) {
		readyFile := filepath.Join(dir, fmt.Sprintf("node%d.ready.json", index))
		logPath := filepath.Join(dir, fmt.Sprintf("node%d.log", index))
		log, err := os.Create(logPath)
		if err != nil {
			return nil, err
		}
		args := []string{"run", "--config", config, "--data", filepath.Join(dir, fmt.Sprintf("node%d", index)), "--listen", "/ip4/127.0.0.1/tcp/0", "--api-bind", "127.0.0.1:0", "--ready-file", readyFile, "--sync-interval", "100ms"}
		for _, address := range bootstrap {
			args = append(args, "--bootstrap", address)
		}
		cmd := exec.Command(binary, args...)
		cmd.Stdout = log
		cmd.Stderr = log
		if err = cmd.Start(); err != nil {
			log.Close()
			return nil, err
		}
		p := &process{cmd: cmd, done: make(chan error, 1), log: log, path: logPath}
		processes = append(processes, p)
		go func() { p.done <- cmd.Wait() }()
		err = poll(ctx, 20*time.Second, func() error {
			select {
			case err := <-p.done:
				p.done <- err
				return fmt.Errorf("node exited: %v", err)
			default:
			}
			b, err := os.ReadFile(readyFile)
			if err != nil {
				return err
			}
			if err = json.Unmarshal(b, &p.meta); err != nil {
				return err
			}
			if p.meta.APIURL == "" || len(p.meta.Addresses) == 0 {
				return errors.New("incomplete readiness")
			}
			return readAPI(ctx, p.meta.APIURL, "/v1/status", &api.Status{})
		})
		return p, err
	}
	p1, err := start(1, nil)
	if err != nil {
		return err
	}
	p2, err := start(2, p1.meta.Addresses)
	if err != nil {
		return err
	}
	p3, err := start(3, append(append([]string{}, p1.meta.Addresses...), p2.meta.Addresses...))
	if err != nil {
		return err
	}
	fmt.Println("Started three processes with independent databases, identities, and dynamic ports.")
	if err = poll(ctx, 20*time.Second, func() error {
		for _, p := range processes {
			var status api.Status
			if err := readAPI(ctx, p.meta.APIURL, "/v1/status", &status); err != nil {
				return err
			}
			if len(status.Node.Peers) < 2 {
				return errors.New("waiting for approved peer mesh")
			}
		}
		return nil
	}); err != nil {
		return err
	}
	batchID := "Coffee / harvest 2026"
	step := func(kind, key string, p *process, extras ...string) error {
		eventPath := filepath.Join(dir, kind+".event.json")
		args := []string{"event", "--kind", kind, "--key", key, "--config", config, "--batch-id", batchID, "--api", p.meta.APIURL, "--out", eventPath}
		args = append(args, extras...)
		if _, err := command(args...); err != nil {
			return err
		}
		if _, err := command("submit", "--api", p.meta.APIURL, "--file", eventPath); err != nil {
			return err
		}
		_, err := command("mine", "--api", p.meta.APIURL)
		return err
	}
	waitAgreement := func(nodes []*process, owner string, count int) error {
		return poll(ctx, 20*time.Second, func() error {
			states := make([]snapshot, len(nodes))
			for i, p := range nodes {
				if err := readAPI(ctx, p.meta.APIURL, "/v1/status", &states[i].Status); err != nil {
					return err
				}
				if err := readAPI(ctx, p.meta.APIURL, "/v1/batch?id="+url.QueryEscape(batchID), &states[i].Batch); err != nil {
					return err
				}
				if err := readAPI(ctx, p.meta.APIURL, "/v1/trace?id="+url.QueryEscape(batchID), &states[i].Trace); err != nil {
					return err
				}
			}
			return agreement(states, owner, count)
		})
	}
	if err = step("create", supplierKey, p1, "--name", "Arabica coffee", "--quantity", "120", "--unit", "kg", "--location", "Farm"); err != nil {
		return err
	}
	if err = waitAgreement(processes, ownerIDs["supplier"], 1); err != nil {
		return err
	}
	fmt.Println("Create mined: all three nodes agree on tip, owner, and history.")
	if err = step("offer", supplierKey, p2, "--recipient", ownerIDs["carrier"]); err != nil {
		return err
	}
	if err = poll(ctx, 20*time.Second, func() error {
		var batch ledger.Batch
		if err := readAPI(ctx, p3.meta.APIURL, "/v1/batch?id="+url.QueryEscape(batchID), &batch); err != nil {
			return err
		}
		if batch.Sequence != 2 || batch.PendingRecipient != ownerIDs["carrier"] {
			return errors.New("waiting for transfer offer")
		}
		return nil
	}); err != nil {
		return err
	}
	if err = step("accept", carrierKey, p3); err != nil {
		return err
	}
	if err = waitAgreement(processes, ownerIDs["carrier"], 3); err != nil {
		return err
	}
	fmt.Println("Recipient accepted: all three nodes agree on carrier custody and three-event trace.")
	stopProcess(p1)
	fmt.Println("Stopped original bootstrap process.")
	if err = step("checkpoint", carrierKey, p2, "--location", "Distribution hub", "--status", "arrived"); err != nil {
		return err
	}
	if err = waitAgreement([]*process{p2, p3}, ownerIDs["carrier"], 4); err != nil {
		return err
	}
	var status chain.Status
	if err = readAPI(ctx, p2.meta.APIURL, "/v1/status", &status); err != nil {
		return err
	}
	fmt.Printf("PASS: survivors agree at height %d, tip %s, carrier owner, four signed events.\n", status.Height, status.Hash)
	return nil
}
