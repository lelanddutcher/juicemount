package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

// These structs freeze the version 1 field order independently of C01's Go types.
type contentJSON struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type manifestJSON struct {
	Version   int                     `json:"version"`
	LibraryID continuity.LibraryID    `json:"library_id"`
	AssetID   continuity.AssetID      `json:"asset_id"`
	Content   contentJSON             `json:"content"`
	Parents   []continuity.RevisionID `json:"parents"`
}

type changeJSON struct {
	CreateAsset        *CreateAsset        `json:"create_asset,omitempty"`
	RenameAsset        *RenameAsset        `json:"rename_asset,omitempty"`
	RecordRevision     *manifestJSON       `json:"record_revision,omitempty"`
	AdvanceHead        *AdvanceHead        `json:"advance_head,omitempty"`
	PutRelationship    *PutRelationship    `json:"put_relationship,omitempty"`
	RemoveRelationship *RemoveRelationship `json:"remove_relationship,omitempty"`
	PutLocation        *PutLocation        `json:"put_location,omitempty"`
	RemoveLocation     *RemoveLocation     `json:"remove_location,omitempty"`
}

type operationJSON struct {
	Version   int                  `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	ID        string               `json:"operation_id"`
	Actor     string               `json:"actor"`
	Changes   []changeJSON         `json:"changes"`
}

func manifestWire(m continuity.Manifest) manifestJSON {
	return manifestJSON{m.Version, m.LibraryID, m.AssetID,
		contentJSON{m.Content.SHA256, m.Content.Size}, m.Parents}
}

func (m manifestJSON) manifest() continuity.Manifest {
	return continuity.Manifest{Version: m.Version, LibraryID: m.LibraryID, AssetID: m.AssetID,
		Content: continuity.Content{SHA256: m.Content.SHA256, Size: m.Content.Size}, Parents: m.Parents}
}

func validText(value string, max int, nonempty bool) bool {
	return len(value) <= max && (!nonempty || value != "") && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validOperationID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func invalid(reason string) error { return fmt.Errorf("%w: %s", ErrInvalid, reason) }
func corrupt(reason string) error { return fmt.Errorf("%w: %s", ErrFormat, reason) }

// CanonicalRequest returns detached canonical request bytes and their v1
// domain-separated SHA-256. It validates and copies every mutable input slice.
func CanonicalRequest(op Operation) ([]byte, string, error) {
	_, data, digest, err := freezeOperation(op)
	return data, digest, err
}

func freezeOperation(op Operation) (Operation, []byte, string, error) {
	if op.Version != OperationVersion || op.LibraryID.Validate() != nil || !validOperationID(op.ID) || !validText(op.Actor, 256, true) {
		return Operation{}, nil, "", invalid("operation envelope")
	}
	if len(op.Changes) == 0 || len(op.Changes) > MaxChanges {
		return Operation{}, nil, "", invalid("change count")
	}
	frozen := op
	frozen.Changes = make([]Change, len(op.Changes))
	wire := operationJSON{op.Version, op.LibraryID, op.ID, op.Actor, make([]changeJSON, len(op.Changes))}
	for i, change := range op.Changes {
		count := 0
		if change.CreateAsset != nil {
			count++
		}
		if change.RenameAsset != nil {
			count++
		}
		if change.RecordRevision != nil {
			count++
		}
		if change.AdvanceHead != nil {
			count++
		}
		if change.PutRelationship != nil {
			count++
		}
		if change.RemoveRelationship != nil {
			count++
		}
		if change.PutLocation != nil {
			count++
		}
		if change.RemoveLocation != nil {
			count++
		}
		if count != 1 {
			return Operation{}, nil, "", invalid("change requires exactly one variant")
		}
		switch {
		case change.CreateAsset != nil:
			c := *change.CreateAsset
			if c.AssetID.Validate() != nil || !validText(c.Label, 4096, false) {
				return Operation{}, nil, "", invalid("create asset")
			}
			frozen.Changes[i].CreateAsset, wire.Changes[i].CreateAsset = &c, &c
		case change.RenameAsset != nil:
			c := *change.RenameAsset
			if c.AssetID.Validate() != nil || !validText(c.ExpectedLabel, 4096, false) || !validText(c.Label, 4096, false) {
				return Operation{}, nil, "", invalid("rename asset")
			}
			frozen.Changes[i].RenameAsset, wire.Changes[i].RenameAsset = &c, &c
		case change.RecordRevision != nil:
			m := *change.RecordRevision
			if m.LibraryID != op.LibraryID {
				return Operation{}, nil, "", invalid("manifest library mismatch")
			}
			if _, err := m.Canonical(); err != nil {
				return Operation{}, nil, "", fmt.Errorf("%w: manifest: %v", ErrInvalid, err)
			}
			m.Parents = append(make([]continuity.RevisionID, 0, len(m.Parents)), m.Parents...)
			sort.Slice(m.Parents, func(i, j int) bool { return m.Parents[i] < m.Parents[j] })
			frozen.Changes[i].RecordRevision = &m
			encoded := manifestWire(m)
			wire.Changes[i].RecordRevision = &encoded
		case change.AdvanceHead != nil:
			c := *change.AdvanceHead
			if c.AssetID.Validate() != nil || c.Revision.Validate() != nil || (c.ExpectedRevision != "" && c.ExpectedRevision.Validate() != nil) {
				return Operation{}, nil, "", invalid("advance head")
			}
			frozen.Changes[i].AdvanceHead, wire.Changes[i].AdvanceHead = &c, &c
		case change.PutRelationship != nil:
			c := *change.PutRelationship
			if !validOperationID(c.ID) || !c.Value.valid() {
				return Operation{}, nil, "", invalid("put relationship")
			}
			var err error
			c.Expected, err = freezeRelationshipExpected(c.Expected)
			if err != nil {
				return Operation{}, nil, "", err
			}
			frozen.Changes[i].PutRelationship, wire.Changes[i].PutRelationship = &c, &c
		case change.RemoveRelationship != nil:
			c := *change.RemoveRelationship
			if !validOperationID(c.ID) || c.Expected == nil {
				return Operation{}, nil, "", invalid("remove relationship")
			}
			var err error
			c.Expected, err = freezeRelationshipExpected(c.Expected)
			if err != nil {
				return Operation{}, nil, "", err
			}
			frozen.Changes[i].RemoveRelationship, wire.Changes[i].RemoveRelationship = &c, &c
		case change.PutLocation != nil:
			c := *change.PutLocation
			if !validOperationID(c.ID) || !c.Value.valid() {
				return Operation{}, nil, "", invalid("put location")
			}
			var err error
			c.Expected, err = freezeLocationExpected(c.Expected)
			if err != nil {
				return Operation{}, nil, "", err
			}
			frozen.Changes[i].PutLocation, wire.Changes[i].PutLocation = &c, &c
		case change.RemoveLocation != nil:
			c := *change.RemoveLocation
			if !validOperationID(c.ID) || c.Expected == nil {
				return Operation{}, nil, "", invalid("remove location")
			}
			var err error
			c.Expected, err = freezeLocationExpected(c.Expected)
			if err != nil {
				return Operation{}, nil, "", err
			}
			frozen.Changes[i].RemoveLocation, wire.Changes[i].RemoveLocation = &c, &c
		}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return Operation{}, nil, "", fmt.Errorf("%w: request encoding: %v", ErrInvalid, err)
	}
	if len(data) > MaxRequestBytes {
		return Operation{}, nil, "", invalid("canonical request exceeds byte limit")
	}
	return frozen, data, payloadDigest(data), nil
}

func payloadDigest(data []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte("juicemount.catalog.operation\x00\x01"))
	_, _ = h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func strictJSON(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("trailing JSON value: %v", err)
	}
	return nil
}

func decodeOperation(data []byte) (Operation, error) {
	if len(data) == 0 || len(data) > MaxRequestBytes {
		return Operation{}, corrupt("request length")
	}
	var wire operationJSON
	if err := strictJSON(data, &wire); err != nil {
		return Operation{}, fmt.Errorf("%w: request JSON: %v", ErrFormat, err)
	}
	op := Operation{Version: wire.Version, LibraryID: wire.LibraryID, ID: wire.ID, Actor: wire.Actor, Changes: make([]Change, len(wire.Changes))}
	for i, c := range wire.Changes {
		op.Changes[i] = Change{CreateAsset: c.CreateAsset, RenameAsset: c.RenameAsset, AdvanceHead: c.AdvanceHead}
		op.Changes[i].PutRelationship, op.Changes[i].RemoveRelationship = c.PutRelationship, c.RemoveRelationship
		op.Changes[i].PutLocation, op.Changes[i].RemoveLocation = c.PutLocation, c.RemoveLocation
		if c.RecordRevision != nil {
			m := c.RecordRevision.manifest()
			op.Changes[i].RecordRevision = &m
		}
	}
	frozen, canonical, _, err := freezeOperation(op)
	if err != nil || !bytes.Equal(data, canonical) {
		return Operation{}, corrupt("noncanonical or invalid request")
	}
	return frozen, nil
}

func decodeManifest(data []byte) (continuity.Manifest, error) {
	if len(data) == 0 || len(data) > 8192 {
		return continuity.Manifest{}, corrupt("manifest JSON length")
	}
	var wire manifestJSON
	if err := strictJSON(data, &wire); err != nil {
		return continuity.Manifest{}, fmt.Errorf("%w: manifest JSON: %v", ErrFormat, err)
	}
	m := wire.manifest()
	if _, err := m.Canonical(); err != nil {
		return continuity.Manifest{}, fmt.Errorf("%w: manifest: %v", ErrFormat, err)
	}
	parents := append(make([]continuity.RevisionID, 0, len(m.Parents)), m.Parents...)
	sort.Slice(parents, func(i, j int) bool { return parents[i] < parents[j] })
	m.Parents = parents
	canonical, _ := json.Marshal(manifestWire(m))
	if !bytes.Equal(data, canonical) {
		return continuity.Manifest{}, corrupt("noncanonical manifest JSON")
	}
	return m, nil
}
