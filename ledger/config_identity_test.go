package ledger

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

func testConfig(t *testing.T) (Config, map[string]ed25519.PrivateKey) {
	t.Helper()
	roles := []string{"supplier", "manufacturer", "carrier", "distributor", "retailer", "auditor"}
	cfg := Config{ChainID: "test-supplychain", Difficulty: 4}
	keys := make(map[string]ed25519.PrivateKey)
	for i, role := range roles {
		private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))
		participant, err := NewParticipant(role, role, private.Public().(ed25519.PublicKey))
		if err != nil {
			t.Fatal(err)
		}
		cfg.Participants = append(cfg.Participants, participant)
		keys[role] = private
	}
	return cfg, keys
}

func TestIdentityKeysAndCanonicalIDs(t *testing.T) {
	pub, private, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(pub) != ed25519.PublicKeySize || len(private) != ed25519.PrivateKeySize {
		t.Fatal("wrong Ed25519 key lengths")
	}
	wantID := sha256.Sum256(pub)
	if got := PublicKeyID(pub); got != hex.EncodeToString(wantID[:]) {
		t.Fatalf("ID %q does not identify public key", got)
	}
	parsedPub, err := ParsePublicKey(hex.EncodeToString(pub))
	if err != nil || !bytes.Equal(parsedPub, pub) {
		t.Fatalf("public key round trip: %v", err)
	}
	parsedPrivate, err := ParsePrivateKey(hex.EncodeToString(private))
	if err != nil || !bytes.Equal(parsedPrivate, private) {
		t.Fatalf("private key round trip: %v", err)
	}
	for _, key := range []string{"", "aa", strings.ToUpper(hex.EncodeToString(pub)), strings.Repeat("g", 64)} {
		if _, err := ParsePublicKey(key); err == nil {
			t.Fatalf("accepted invalid public key %q", key)
		}
	}
	private[63] ^= 1
	if _, err := ParsePrivateKey(hex.EncodeToString(private)); err == nil {
		t.Fatal("accepted private key whose public half differs from its seed")
	}
}

func TestNormalizeConfigIsDeterministicAndDoesNotAlias(t *testing.T) {
	cfg, _ := testConfig(t)
	original := append([]Participant(nil), cfg.Participants...)
	normal, err := NormalizeConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(normal.Participants); i++ {
		if normal.Participants[i-1].ID >= normal.Participants[i].ID {
			t.Fatal("participants must be sorted by ID")
		}
	}
	if !reflect.DeepEqual(original, cfg.Participants) {
		t.Fatal("normalization mutated input")
	}
	firstHash, err := ConfigHash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i, j := 0, len(cfg.Participants)-1; i < j; i, j = i+1, j-1 {
		cfg.Participants[i], cfg.Participants[j] = cfg.Participants[j], cfg.Participants[i]
	}
	secondHash, err := ConfigHash(cfg)
	if err != nil || firstHash != secondHash {
		t.Fatalf("participant ordering changed config hash: %v", err)
	}
	normal.Participants[0].Name = "changed"
	if reflect.DeepEqual(normal.Participants, cfg.Participants) {
		t.Fatal("normalized config must have its own participant slice")
	}
	cfg.Participants[0].Name += " changed"
	thirdHash, _ := ConfigHash(cfg)
	if thirdHash == firstHash {
		t.Fatal("configuration hash failed to commit participant metadata")
	}
}

func TestConfigRejectsInvalidConsensusParameters(t *testing.T) {
	tests := map[string]func(*Config){
		"empty chain":           func(c *Config) { c.ChainID = "" },
		"blank chain":           func(c *Config) { c.ChainID = " \t" },
		"difficulty zero":       func(c *Config) { c.Difficulty = 0 },
		"difficulty too high":   func(c *Config) { c.Difficulty = 25 },
		"duplicate participant": func(c *Config) { c.Participants = append(c.Participants, c.Participants[0]) },
		"unknown role":          func(c *Config) { c.Participants[0].Role = "admin" },
		"wrong ID":              func(c *Config) { c.Participants[0].ID = strings.Repeat("0", 64) },
		"uppercase public key":  func(c *Config) { c.Participants[0].PublicKey = strings.ToUpper(c.Participants[0].PublicKey) },
		"malformed public key":  func(c *Config) { c.Participants[0].PublicKey = "abcd" },
		"oversize name":         func(c *Config) { c.Participants[0].Name = strings.Repeat("x", 129) },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, _ := testConfig(t)
			mutate(&cfg)
			if _, err := NormalizeConfig(cfg); err == nil {
				t.Fatal("accepted invalid configuration")
			}
			if _, err := ConfigHash(cfg); err == nil {
				t.Fatal("hashed invalid configuration")
			}
		})
	}
}
