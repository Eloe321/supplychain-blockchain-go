package node

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"supplychain/chain"
	"supplychain/ledger"
)

const MaxFrameBytes = 2 * 1024 * 1024
const maxInventory = 4096
const maxAddresses = 64

type hello struct {
	Version     string `json:"version"`
	ChainID     string `json:"chain_id"`
	ConfigHash  string `json:"config_hash"`
	GenesisHash string `json:"genesis_hash"`
}
type request struct {
	Hello    hello    `json:"hello"`
	Op       string   `json:"op"`
	Locators []string `json:"locators,omitempty"`
	IDs      []string `json:"ids,omitempty"`
}
type response struct {
	Hello  hello          `json:"hello"`
	Error  string         `json:"error,omitempty"`
	Status *chain.Status  `json:"status,omitempty"`
	Blocks []ledger.Block `json:"blocks,omitempty"`
	IDs    []string       `json:"ids,omitempty"`
	Events []ledger.Event `json:"events,omitempty"`
	Peers  []string       `json:"peers,omitempty"`
}

func decodeStrict(raw []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
func readFrame(r io.Reader, dst any) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxFrameBytes {
		return errors.New("invalid frame length")
	}
	raw := make([]byte, int(size))
	if _, err := io.ReadFull(r, raw); err != nil {
		return err
	}
	return decodeStrict(raw, dst)
}
func writeFrame(w io.Writer, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(raw) > MaxFrameBytes {
		return fmt.Errorf("frame exceeds %d bytes", MaxFrameBytes)
	}
	header := binary.BigEndian.AppendUint32(nil, uint32(len(raw)))
	if err := writeAll(w, header); err != nil {
		return err
	}
	return writeAll(w, raw)
}
func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
	}
	return nil
}
func validHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'f' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}
