package continuity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
)

const ManifestVersion = 1
const MaxParents = 64

// Manifest declares bytes and lineage for one asset. Canonical and RevisionID
// validate syntax only; catalog admission must verify the declared relationships.
type Manifest struct {
	Version   int
	LibraryID LibraryID
	AssetID   AssetID
	Content   Content
	Parents   []RevisionID
}

// Canonical returns the complete v1 binary encoding without mutating Parents.
func (m Manifest) Canonical() ([]byte, error) {
	if m.Version != ManifestVersion {
		return nil, fmt.Errorf("continuity manifest: unsupported version %d", m.Version)
	}
	library, err := decodeID(string(m.LibraryID), "lib_", 16, true)
	if err != nil {
		return nil, err
	}
	asset, err := decodeID(string(m.AssetID), "asset_", 16, true)
	if err != nil {
		return nil, err
	}
	if err := m.Content.Validate(); err != nil {
		return nil, err
	}
	if len(m.Parents) > MaxParents {
		return nil, fmt.Errorf("continuity manifest: more than %d parents", MaxParents)
	}
	parents := append([]RevisionID(nil), m.Parents...)
	sort.Slice(parents, func(i, j int) bool { return parents[i] < parents[j] })
	parentBytes := make([]byte, 0, len(parents)*sha256.Size)
	for i, parent := range parents {
		raw, err := decodeID(string(parent), "rev1_", sha256.Size, false)
		if err != nil {
			return nil, err
		}
		if i > 0 && parent == parents[i-1] {
			return nil, fmt.Errorf("continuity manifest: duplicate parent")
		}
		parentBytes = append(parentBytes, raw...)
	}
	digest, _ := decodeHex(m.Content.SHA256, sha256.Size) // Validated above.
	encoded := make([]byte, 0, 95+len(parentBytes))
	encoded = append(encoded, "juicemount.revision\x00\x01"...)
	encoded = append(encoded, library...)
	encoded = append(encoded, asset...)
	encoded = binary.BigEndian.AppendUint64(encoded, uint64(m.Content.Size))
	encoded = append(encoded, digest...)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(parents)))
	return append(encoded, parentBytes...), nil
}

// RevisionID hashes the complete canonical encoding exactly once.
func (m Manifest) RevisionID() (RevisionID, error) {
	encoded, err := m.Canonical()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return RevisionID("rev1_" + hex.EncodeToString(digest[:])), nil
}
