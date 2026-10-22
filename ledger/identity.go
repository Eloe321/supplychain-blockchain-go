package ledger

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// GenerateKey creates an actor key pair using the operating system RNG.
func GenerateKey() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(rand.Reader)
}

// PublicKeyID returns the lowercase SHA-256 identity of a public key.
// Callers accepting arbitrary key bytes should validate their size first.
func PublicKeyID(public ed25519.PublicKey) string {
	return hashHex(public)
}

// ParsePublicKey accepts only a 32-byte, lowercase hexadecimal Ed25519 key.
func ParsePublicKey(encoded string) (ed25519.PublicKey, error) {
	if !canonicalHex(encoded, ed25519.PublicKeySize) {
		return nil, errors.New("public key must be 64 lowercase hexadecimal characters")
	}
	decoded, _ := hex.DecodeString(encoded)
	return ed25519.PublicKey(decoded), nil
}

// ParsePrivateKey accepts Go's 64-byte Ed25519 private key representation and
// verifies that its embedded public key agrees with the private seed.
func ParsePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	if !canonicalHex(encoded, ed25519.PrivateKeySize) {
		return nil, errors.New("private key must be 128 lowercase hexadecimal characters")
	}
	decoded, _ := hex.DecodeString(encoded)
	private := ed25519.PrivateKey(decoded)
	if err := validatePrivateKey(private); err != nil {
		return nil, err
	}
	return private, nil
}

func validatePrivateKey(private ed25519.PrivateKey) error {
	if len(private) != ed25519.PrivateKeySize {
		return errors.New("invalid Ed25519 private key length")
	}
	derived := ed25519.NewKeyFromSeed(private[:ed25519.SeedSize])
	if !bytes.Equal(private, derived) {
		return errors.New("private key public half does not match its seed")
	}
	return nil
}

// NewParticipant constructs and validates a registry entry for an actor.
func NewParticipant(name, role string, public ed25519.PublicKey) (Participant, error) {
	if len(public) != ed25519.PublicKeySize {
		return Participant{}, errors.New("invalid Ed25519 public key length")
	}
	if err := requiredText("participant name", name, MaxNameBytes); err != nil {
		return Participant{}, err
	}
	if !validRole(role) {
		return Participant{}, errors.New("unknown participant role")
	}
	return Participant{ID: PublicKeyID(public), Name: name, Role: role, PublicKey: hex.EncodeToString(public)}, nil
}
