// Package continuity defines immutable byte and manifest identities. It does not
// establish catalog membership, authorization, source stability, or publication.
package continuity

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

type LibraryID string
type AssetID string
type RevisionID string

func (id LibraryID) Validate() error {
	_, err := decodeID(string(id), "lib_", 16, true)
	return err
}

func (id AssetID) Validate() error {
	_, err := decodeID(string(id), "asset_", 16, true)
	return err
}

func (id RevisionID) Validate() error {
	_, err := decodeID(string(id), "rev1_", 32, false)
	return err
}

// NewLibraryID allocates a nonzero random catalog identity. Restoring a catalog
// must preserve its existing identity instead of allocating another one.
func NewLibraryID() (LibraryID, error) {
	id, err := randomID("lib_")
	return LibraryID(id), err
}

// NewAssetID allocates a nonzero random logical identity, independent of bytes.
func NewAssetID() (AssetID, error) {
	id, err := randomID("asset_")
	return AssetID(id), err
}

func randomID(prefix string) (string, error) {
	var raw [16]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return "", fmt.Errorf("continuity identity: %w", err)
		}
		if raw != [16]byte{} {
			return prefix + hex.EncodeToString(raw[:]), nil
		}
	}
}

func decodeID(id, prefix string, bytes int, nonzero bool) ([]byte, error) {
	if !strings.HasPrefix(id, prefix) {
		return nil, fmt.Errorf("continuity identity: expected %s prefix", prefix)
	}
	raw, err := decodeHex(id[len(prefix):], bytes)
	if err != nil {
		return nil, fmt.Errorf("continuity identity: %w", err)
	}
	if nonzero {
		for _, b := range raw {
			if b != 0 {
				return raw, nil
			}
		}
		return nil, fmt.Errorf("continuity identity: zero %s identity", prefix)
	}
	return raw, nil
}

func decodeHex(value string, bytes int) ([]byte, error) {
	if len(value) != bytes*2 {
		return nil, fmt.Errorf("expected %d lowercase hex characters", bytes*2)
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return nil, fmt.Errorf("expected lowercase hex characters")
		}
	}
	return hex.DecodeString(value)
}
