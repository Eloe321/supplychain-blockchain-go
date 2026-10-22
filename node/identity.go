package node

import (
	"crypto/rand"
	"errors"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	"io"
	"os"
	"path/filepath"
)

func loadIdentity(dir string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "identity.key")
	raw, err := readBounded(path, 4096)
	if err == nil {
		if err := os.Chmod(path, 0600); err != nil {
			return nil, err
		}
		return crypto.UnmarshalPrivateKey(raw)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	raw, err = crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(dir, ".identity-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(raw)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	// Link publishes a complete key without replacing another process's identity.
	if err = os.Link(f.Name(), path); errors.Is(err, os.ErrExist) {
		raw, err = readBounded(path, 4096)
		if err != nil {
			return nil, err
		}
		return crypto.UnmarshalPrivateKey(raw)
	}
	if err != nil {
		return nil, err
	}
	return key, nil
}
func readBounded(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > max {
		return nil, errors.New("file exceeds size limit")
	}
	return raw, nil
}
