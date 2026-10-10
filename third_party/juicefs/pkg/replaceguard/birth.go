package replaceguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

const birthStagePrefix = ".juicemount-streaming-drain-replacement-"

// Birth is immutable evidence of a fresh stage create. AttrSHA256 hashes the
// exact raw SET value from creation (including flags, IDs, all timestamps and
// nanoseconds, rdev and ACL fields), never reconstructed selected attributes.
// Proxy and Epoch describe historical authority, not later write permission.
type Birth struct {
	Version     uint32   `json:"version"`
	OperationID string   `json:"operation_id"`
	Owner       string   `json:"owner"`
	Proxy       string   `json:"proxy"`
	Database    int64    `json:"database"`
	BackendUUID string   `json:"backend_uuid"`
	Project     uint64   `json:"project"`
	Epoch       uint64   `json:"epoch"`
	Parents     []Parent `json:"parents"`
	Stage       File     `json:"stage"`
	Type        uint8    `json:"type"`
	Nlink       uint32   `json:"nlink"`
	Mode        uint16   `json:"mode"`
	AttrSHA256  string   `json:"attr_sha256"`
}

// BirthStageName returns empty for an invalid operation ID.
func BirthStageName(operationID string) string {
	if !hexOK(operationID, 32) {
		return ""
	}
	return birthStagePrefix + operationID
}

// ParseBirthStageName recognizes only the exact reserved name grammar.
func ParseBirthStageName(name string) (string, bool) {
	if len(name) != len(birthStagePrefix)+32 || name[:len(birthStagePrefix)] != birthStagePrefix {
		return "", false
	}
	op := name[len(birthStagePrefix):]
	return op, hexOK(op, 32)
}

// BirthOperationKey and BirthInodeKey return empty for invalid input.
func BirthOperationKey(operationID string) string {
	if !hexOK(operationID, 32) {
		return ""
	}
	return "jmlease:v1:birth:op:" + operationID
}

func BirthInodeKey(inode uint64) string {
	if inode <= 1 || inode > math.MaxInt64 {
		return ""
	}
	return "jmlease:v1:birth:inode:" + strconv.FormatUint(inode, 10)
}

func canonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if s[i] != '-' {
				return false
			}
		} else if s[i] < '0' || s[i] > '9' && s[i] < 'a' || s[i] > 'f' {
			return false
		}
	}
	return true
}

func (b Birth) Validate() error {
	if b.Version != 1 || !hexOK(b.OperationID, 32) || !bounded(b.Owner) || !bounded(b.Proxy) ||
		b.Database < 0 || b.Database > math.MaxInt32 || !canonicalUUID(b.BackendUUID) ||
		b.Project == 0 || b.Project > math.MaxInt64 || b.Epoch == 0 || b.Epoch > math.MaxInt64 ||
		len(b.Parents) == 0 || len(b.Parents) > 256 || b.Parents[0] != (Parent{Inode: 1}) ||
		b.Stage.Name != BirthStageName(b.OperationID) || b.Stage.Inode <= 1 || b.Stage.Inode > math.MaxInt64 ||
		b.Stage.Length != 0 || b.Stage.MtimeNS < 0 || b.Stage.CtimeNS < 0 ||
		b.Type != 1 || b.Nlink != 1 || b.Mode > 07777 || !hexOK(b.AttrSHA256, 64) {
		return ErrInvalid
	}
	seen := make(map[uint64]bool, len(b.Parents))
	project := false
	for i, p := range b.Parents {
		if p.Inode == 0 || p.Inode > math.MaxInt64 || seen[p.Inode] || i > 0 && !nameOK(p.Name) {
			return ErrInvalid
		}
		seen[p.Inode] = true
		project = project || p.Inode == b.Project
	}
	if !project || b.Stage.Parent != b.Parents[len(b.Parents)-1].Inode || seen[b.Stage.Inode] {
		return ErrInvalid
	}
	raw, err := json.Marshal(b)
	if err != nil || len(raw) > MaxBytes {
		return ErrInvalid
	}
	return nil
}

func EncodeBirth(b Birth) ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(b)
	if err != nil || len(raw) > MaxBytes {
		return nil, ErrInvalid
	}
	return raw, nil
}

// DecodeBirth accepts only the exact canonical encoding; byte comparison also
// rejects duplicate/unknown fields, reordered keys and noncanonical numbers.
func DecodeBirth(raw []byte) (Birth, error) {
	if len(raw) == 0 || len(raw) > MaxBytes {
		return Birth{}, ErrInvalid
	}
	var b Birth
	if err := json.Unmarshal(raw, &b); err != nil {
		return Birth{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	canonical, err := EncodeBirth(b)
	if err != nil || !bytes.Equal(raw, canonical) {
		return Birth{}, ErrInvalid
	}
	return b, nil
}

// BirthRequiredKeys is the minimum WATCH set for a candidate create, not
// authority proof. The proxy must separately validate fresh creation, raw
// attributes, exact postconditions and guard all mutations before activation.
// ACL-dependent doMknod shapes can touch additional keys before TxPipelined;
// integration must WATCH and qualify them explicitly or refuse those shapes.
func BirthRequiredKeys(b Birth) []string {
	if b.Validate() != nil {
		return nil
	}
	keys := []string{BirthOperationKey(b.OperationID), BirthInodeKey(b.Stage.Inode),
		Key(b.Stage.Inode), "setting", "jmlease:v1:proxy",
		"jmlease:v1:p:" + strconv.FormatUint(b.Project, 10),
		"jmlease:v1:epoch:" + strconv.FormatUint(b.Project, 10),
		"usedSpace", "totalInodes"}
	for _, p := range b.Parents {
		id := strconv.FormatUint(p.Inode, 10)
		keys = append(keys, "i"+id, "d"+id)
	}
	id := strconv.FormatUint(b.Stage.Inode, 10)
	return append(keys, "i"+id, "d"+id)
}

// MatchesReplacement compares immutable custody identity. Publication binds a
// later image and may use a new lease/proxy incarnation; those fields are not
// historical birth identity.
func (b Birth) MatchesReplacement(r Record) bool {
	if b.Validate() != nil || r.Validate() != nil || b.Owner != r.Owner || b.OperationID != r.OperationID || b.BackendUUID != r.BackendUUID || b.Project != r.Project || b.Stage.Inode != r.Stage.Inode || b.Stage.Parent != r.Stage.Parent || b.Stage.Name != r.Stage.Name || len(b.Parents) != len(r.Parents) {
		return false
	}
	for i, p := range b.Parents {
		if p != r.Parents[i] {
			return false
		}
	}
	return true
}
