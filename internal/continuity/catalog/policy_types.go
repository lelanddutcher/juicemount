package catalog

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

var ErrUnauthenticated = errors.New("catalog unauthenticated")

type SubjectID string

func (id SubjectID) Validate() error {
	if !validRandomID(string(id), "sub_") {
		return invalid("subject identity")
	}
	return nil
}

func validRandomID(id, prefix string) bool {
	if len(id) != len(prefix)+32 || !strings.HasPrefix(id, prefix) {
		return false
	}
	s := id[len(prefix):]
	if s == strings.Repeat("0", 32) || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

type SubjectValue struct {
	Kind    string `json:"kind"`
	Label   string `json:"label"`
	Enabled bool   `json:"enabled"`
}

type Subject struct {
	LibraryID               continuity.LibraryID `json:"library_id"`
	ID                      SubjectID            `json:"subject_id"`
	Value                   SubjectValue         `json:"value"`
	AuthorizationGeneration int64                `json:"authorization_generation"`
	CreatedSequence         int64                `json:"created_sequence"`
	UpdatedSequence         int64                `json:"updated_sequence"`
}

type ExpectedSubject struct {
	Value                   SubjectValue `json:"value"`
	AuthorizationGeneration int64        `json:"authorization_generation"`
	UpdatedSequence         int64        `json:"updated_sequence"`
}

func (s Subject) Expected() *ExpectedSubject {
	return &ExpectedSubject{s.Value, s.AuthorizationGeneration, s.UpdatedSequence}
}

type Scope struct {
	Kind    string             `json:"kind"`
	AssetID continuity.AssetID `json:"asset_id"`
}

func (s Scope) valid() bool {
	return s.Kind == "library" && s.AssetID == "" || s.Kind == "asset" && s.AssetID.Validate() == nil
}
func (s Scope) matches(id continuity.AssetID) bool {
	return s.Kind == "library" || s.Kind == "asset" && s.AssetID == id
}

type GrantValue struct {
	SubjectID SubjectID `json:"subject_id"`
	Scope     Scope     `json:"scope"`
	Actions   []string  `json:"actions"`
}

type Grant struct {
	LibraryID       continuity.LibraryID `json:"library_id"`
	ID              string               `json:"id"`
	Value           GrantValue           `json:"value"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
	Removed         bool                 `json:"removed"`
}

type ExpectedGrant struct {
	Value           GrantValue `json:"value"`
	UpdatedSequence int64      `json:"updated_sequence"`
	Removed         bool       `json:"removed"`
}

func (g Grant) Expected() *ExpectedGrant {
	v := g.Value
	v.Actions = append([]string(nil), v.Actions...)
	return &ExpectedGrant{v, g.UpdatedSequence, g.Removed}
}

type CreateSubject struct {
	SubjectID SubjectID    `json:"subject_id"`
	Value     SubjectValue `json:"value"`
}
type UpdateSubject struct {
	SubjectID SubjectID        `json:"subject_id"`
	Expected  *ExpectedSubject `json:"expected"`
	Value     SubjectValue     `json:"value"`
}
type PutGrant struct {
	ID       string         `json:"id"`
	Expected *ExpectedGrant `json:"expected"`
	Value    GrantValue     `json:"value"`
}
type RemoveGrant struct {
	ID       string         `json:"id"`
	Expected *ExpectedGrant `json:"expected"`
}

// PolicyChange's value is a typed canonical JSON object selected by Kind.
// NewPolicyChange is a convenient constructor; admission still validates it.
type PolicyChange struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

func NewPolicyChange(kind string, value any) PolicyChange {
	b, _ := json.Marshal(value)
	return PolicyChange{kind, b}
}

type PolicyOperation struct {
	Version           int                  `json:"version"`
	LibraryID         continuity.LibraryID `json:"library_id"`
	ID                string               `json:"operation_id"`
	Administration    string               `json:"administration"`
	OperatorSubjectID SubjectID            `json:"operator_subject_id"`
	Changes           []PolicyChange       `json:"changes"`
}

type AdminRequest struct {
	Version             int                  `json:"version"`
	LibraryID           continuity.LibraryID `json:"library_id"`
	ID                  string               `json:"operation_id"`
	ExpectedIncarnation string               `json:"expected_incarnation"`
	Kind                string               `json:"kind"`
	Value               json.RawMessage      `json:"value"`
}

type BootstrapRequest struct {
	OwnerSubjectID SubjectID `json:"owner_subject_id"`
	CreateLabel    *string   `json:"create_label"`
}
type RecoverRequest struct {
	OwnerSubjectID SubjectID `json:"owner_subject_id"`
}
type ApplyPolicyRequest struct {
	Operation PolicyOperation `json:"operation"`
}
type IssueCredentialRequest struct {
	SubjectID   SubjectID `json:"subject_id"`
	Scope       Scope     `json:"scope"`
	Actions     []string  `json:"actions"`
	ExpiresUnix int64     `json:"expires_unix"`
}
type RevokeCredentialRequest struct {
	Selector           string `json:"selector"`
	ExpectedGeneration int64  `json:"expected_generation"`
}

type AdminResult struct {
	Incarnation    string    `json:"incarnation"`
	OwnerSubjectID SubjectID `json:"owner_subject_id"`
	SubjectID      SubjectID `json:"subject_id"`
	Selector       string    `json:"selector"`
	PolicySequence int64     `json:"policy_sequence"`
}
type AdminReceipt struct {
	Version          int                  `json:"version"`
	LibraryID        continuity.LibraryID `json:"library_id"`
	OperationID      string               `json:"operation_id"`
	Sequence         int64                `json:"sequence"`
	PayloadSHA256    string               `json:"payload_sha256"`
	RecordedUnixNano int64                `json:"recorded_unix_nano"`
	Result           AdminResult          `json:"result"`
}
type IssuanceResult struct {
	Receipt AdminReceipt `json:"receipt"`
	Status  string       `json:"status"`
	Bearer  []byte       `json:"-"`
}
type AuthorityStatus struct {
	Incarnation    string    `json:"incarnation"`
	OwnerSubjectID SubjectID `json:"owner_subject_id"`
}
type AccessRequest struct {
	LibraryID continuity.LibraryID
	Action    string
	AssetID   continuity.AssetID
}
type AccessDecision struct{ Allowed bool }
type Identity struct {
	SubjectID   SubjectID `json:"subject_id"`
	Kind        string    `json:"kind"`
	Selector    string    `json:"selector"`
	ExpiresUnix int64     `json:"expires_unix"`
}
