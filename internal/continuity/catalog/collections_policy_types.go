package catalog

import (
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const (
	MaxCollectionPolicyRecordBytes          = 36864
	MaxCollectionSecurityRequestBytes       = 2048
	CollectionRemovalReserveBytes     int64 = 3072
)

// These scopes never reinterpret the v1 Scope.AssetID field.
type CollectionGrantScope struct {
	Kind         string       `json:"kind"`
	CollectionID CollectionID `json:"collection_id"`
}
type CollectionGrantValue struct {
	SubjectID SubjectID            `json:"subject_id"`
	Scope     CollectionGrantScope `json:"scope"`
	Actions   []string             `json:"actions"`
}
type ExpectedCollectionGrant struct {
	Value   CollectionGrantValue   `json:"value"`
	Version CollectionGrantVersion `json:"version"`
	Removed bool                   `json:"removed"`
}
type CollectionGrantChange struct {
	Kind     string                   `json:"kind"`
	ID       CollectionGrantID        `json:"grant_id"`
	Expected *ExpectedCollectionGrant `json:"expected"`
	Value    *CollectionGrantValue    `json:"value"`
}
type CollectionPolicyRequest struct {
	Version              int                         `json:"version"`
	LibraryID            continuity.LibraryID        `json:"library_id"`
	ID                   CollectionPolicyOperationID `json:"operation_id"`
	Category             string                      `json:"category"`
	CollectionID         CollectionID                `json:"collection_id"`
	BindingOperationID   CollectionOperationID       `json:"binding_operation_id"`
	BindingPayloadSHA256 string                      `json:"binding_payload_sha256"`
	Changes              []CollectionGrantChange     `json:"changes"`
}
type CollectionGrantResult struct {
	ID      CollectionGrantID      `json:"grant_id"`
	Version CollectionGrantVersion `json:"version"`
	Removed bool                   `json:"removed"`
}
type CollectionPolicyRecord struct {
	Version           int                     `json:"version"`
	Request           CollectionPolicyRequest `json:"request"`
	Administration    string                  `json:"administration"`
	OperatorSubjectID SubjectID               `json:"operator_subject_id"`
	Results           []CollectionGrantResult `json:"results"`
	RecordedUnixNano  int64                   `json:"recorded_unix_nano"`
}

// CollectionPolicyReceipt is an internal/owner receipt, not a public history DTO.
type CollectionPolicyReceipt struct {
	Version          int                         `json:"version"`
	LibraryID        continuity.LibraryID        `json:"library_id"`
	OperationID      CollectionPolicyOperationID `json:"operation_id"`
	PayloadSHA256    string                      `json:"payload_sha256"`
	RecordedUnixNano int64                       `json:"recorded_unix_nano"`
}

// Namespace contains no serialized event sequence. Local-owner operations have
// an empty namespace subject, while generated operations are actor-specific.
type CollectionPolicyNamespace struct {
	LibraryID      continuity.LibraryID
	Administration string
	SubjectID      SubjectID
	OperationID    CollectionPolicyOperationID
}

// AdminRequestV2 is issuance-request syntax only; it neither issues credentials
// nor changes any existing v1 validator or bearer format.
type AdminRequestV2 struct {
	Version             int                  `json:"version"`
	LibraryID           continuity.LibraryID `json:"library_id"`
	ID                  string               `json:"operation_id"`
	ExpectedIncarnation string               `json:"expected_incarnation"`
	Kind                string               `json:"kind"`
	Value               json.RawMessage      `json:"value"`
}
type IssueCredentialRequestV2 struct {
	SubjectID   SubjectID `json:"subject_id"`
	Scope       Scope     `json:"scope"`
	Actions     []string  `json:"actions"`
	ExpiresUnix int64     `json:"expires_unix"`
}
