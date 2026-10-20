package catalog

import (
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type AdminReceiptV2 AdminReceipt
type IssuanceResultV2 struct {
	Receipt AdminReceiptV2 `json:"receipt"`
	Status  string         `json:"status"`
	Bearer  []byte         `json:"-"`
}

func (r IssuanceResultV2) String() string   { return "catalog issuance v2 result (secret redacted)" }
func (r IssuanceResultV2) GoString() string { return r.String() }

type CollectionOwnerRequest struct {
	Version             int                  `json:"version"`
	LibraryID           continuity.LibraryID `json:"library_id"`
	ID                  string               `json:"operation_id"`
	ExpectedIncarnation string               `json:"expected_incarnation"`
	Kind                string               `json:"kind"`
	Value               json.RawMessage      `json:"value"`
}
type ApplyCollectionPolicyRequest struct {
	Operation CollectionPolicyRequest `json:"operation"`
}
type CollectionOwnerResult struct {
	Incarnation       string                      `json:"incarnation"`
	OwnerSubjectID    SubjectID                   `json:"owner_subject_id"`
	PolicyOperationID CollectionPolicyOperationID `json:"policy_operation_id"`
}
type CollectionOwnerReceipt struct {
	Version          int                   `json:"version"`
	LibraryID        continuity.LibraryID  `json:"library_id"`
	OperationID      string                `json:"operation_id"`
	Sequence         int64                 `json:"sequence"`
	PayloadSHA256    string                `json:"payload_sha256"`
	RecordedUnixNano int64                 `json:"recorded_unix_nano"`
	Result           CollectionOwnerResult `json:"result"`
}
