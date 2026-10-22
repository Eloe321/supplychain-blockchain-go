package node

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestFramesRejectUnboundedAndNonStrictJSON(t *testing.T) {
	for name, raw := range map[string][]byte{
		"oversize":  binary.BigEndian.AppendUint32(nil, MaxFrameBytes+1),
		"truncated": append(binary.BigEndian.AppendUint32(nil, 20), []byte(`{}`)...),
		"unknown":   framed([]byte(`{"unexpected":1}`)),
		"multiple":  framed([]byte(`{} {}`)),
		"zero":      make([]byte, 4),
	} {
		t.Run(name, func(t *testing.T) {
			var v hello
			if err := readFrame(bytes.NewReader(raw), &v); err == nil {
				t.Fatal("accepted invalid frame")
			}
		})
	}
	var b bytes.Buffer
	want := hello{Version: "1", ChainID: "test", ConfigHash: strings.Repeat("a", 64), GenesisHash: strings.Repeat("b", 64)}
	if err := writeFrame(&b, want); err != nil {
		t.Fatal(err)
	}
	var got hello
	if err := readFrame(&b, &got); err != nil || got != want {
		t.Fatal(got, err)
	}
	if err := writeFrame(&b, strings.Repeat("x", MaxFrameBytes)); err == nil {
		t.Fatal("wrote oversized frame")
	}
}
func framed(raw []byte) []byte {
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(raw))), raw...)
}
