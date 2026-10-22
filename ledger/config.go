// Package ledger defines deterministic consensus and whole-batch custody rules.
// It has no storage, networking, or wall-clock dependencies.
package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	RoleSupplier     = "supplier"
	RoleManufacturer = "manufacturer"
	RoleCarrier      = "carrier"
	RoleDistributor  = "distributor"
	RoleRetailer     = "retailer"
	RoleAuditor      = "auditor"

	MaxChainIDBytes  = 128
	MaxBatchIDBytes  = 128
	MaxNameBytes     = 128
	MaxUnitBytes     = 32
	MaxLocationBytes = 256
	MaxStatusBytes   = 64
	MaxEventBytes    = 16 * 1024
	MinDifficulty    = 1
	MaxDifficulty    = 24
)

// Participant enrolls one Ed25519 actor in the shared chain configuration.
// ID is SHA-256(PublicKey bytes); keys and IDs use lowercase hexadecimal.
type Participant struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	PublicKey string `json:"public_key"`
}

// Config is immutable consensus configuration shared by every node on a chain.
type Config struct {
	ChainID      string        `json:"chain_id"`
	Difficulty   uint8         `json:"difficulty"`
	Participants []Participant `json:"participants"`
}

// NormalizeConfig validates configuration and returns a separately allocated
// participant slice sorted by ID. It never mutates the caller's configuration.
func NormalizeConfig(cfg Config) (Config, error) {
	if err := requiredText("chain_id", cfg.ChainID, MaxChainIDBytes); err != nil {
		return Config{}, err
	}
	if cfg.Difficulty < MinDifficulty || cfg.Difficulty > MaxDifficulty {
		return Config{}, fmt.Errorf("difficulty must be between %d and %d", MinDifficulty, MaxDifficulty)
	}
	normalized := cfg
	normalized.Participants = make([]Participant, len(cfg.Participants))
	copy(normalized.Participants, cfg.Participants)
	seen := make(map[string]bool, len(cfg.Participants))
	for _, p := range normalized.Participants {
		if err := requiredText("participant name", p.Name, MaxNameBytes); err != nil {
			return Config{}, err
		}
		if !validRole(p.Role) {
			return Config{}, fmt.Errorf("participant %q has unknown role %q", p.Name, p.Role)
		}
		public, err := ParsePublicKey(p.PublicKey)
		if err != nil {
			return Config{}, fmt.Errorf("participant %q: %w", p.Name, err)
		}
		if !canonicalHex(p.ID, sha256.Size) || p.ID != PublicKeyID(public) {
			return Config{}, fmt.Errorf("participant %q ID does not identify its public key", p.Name)
		}
		if seen[p.ID] {
			return Config{}, fmt.Errorf("duplicate participant %s", p.ID)
		}
		seen[p.ID] = true
	}
	sort.Slice(normalized.Participants, func(i, j int) bool {
		return normalized.Participants[i].ID < normalized.Participants[j].ID
	})
	return normalized, nil
}

// ConfigHash commits the complete normalized configuration, including names.
func ConfigHash(cfg Config) (string, error) {
	normalized, err := NormalizeConfig(cfg)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return hashHex(append([]byte("supplychain/config/v1\x00"), encoded...)), nil
}

func validRole(role string) bool {
	switch role {
	case RoleSupplier, RoleManufacturer, RoleCarrier, RoleDistributor, RoleRetailer, RoleAuditor:
		return true
	default:
		return false
	}
}

func requiredText(field, value string, limit int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	return boundedText(field, value, limit)
}

func boundedText(field, value string, limit int) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%s is not valid UTF-8", field)
	}
	if len(value) > limit {
		return fmt.Errorf("%s exceeds %d bytes", field, limit)
	}
	return nil
}

func canonicalHex(value string, size int) bool {
	if len(value) != size*2 {
		return false
	}
	for _, ch := range value {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func hashHex(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func participantByID(cfg Config, id string) (Participant, error) {
	for _, p := range cfg.Participants {
		if p.ID == id {
			return p, nil
		}
	}
	return Participant{}, errors.New("actor or recipient is not enrolled")
}
