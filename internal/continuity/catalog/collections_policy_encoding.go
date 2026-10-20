package catalog

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
)

func validCollectionDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validCollectionGrantValue(v CollectionGrantValue) bool {
	if v.SubjectID.Validate() != nil || len(v.Actions) == 0 {
		return false
	}
	if v.Scope.Kind == "library_create" {
		return v.Scope.CollectionID == "" && len(v.Actions) == 1 && v.Actions[0] == "collections.create"
	}
	if v.Scope.Kind != "collection" || v.Scope.CollectionID.Validate() != nil || len(v.Actions) > 3 {
		return false
	}
	for i, action := range v.Actions {
		if action != "collections.admin" && action != "collections.edit" && action != "collections.read" || i > 0 && v.Actions[i-1] >= action {
			return false
		}
	}
	return true
}

func equalCollectionGrantValue(a, b CollectionGrantValue) bool {
	if a.SubjectID != b.SubjectID || a.Scope != b.Scope || len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Actions {
		if a.Actions[i] != b.Actions[i] {
			return false
		}
	}
	return true
}

func validCollectionGrantChanges(changes []CollectionGrantChange, scopeKind string, collection CollectionID) bool {
	if len(changes) == 0 || len(changes) > 16 {
		return false
	}
	for i, change := range changes {
		if change.ID.Validate() != nil || i > 0 && changes[i-1].ID >= change.ID {
			return false
		}
		if change.Expected != nil && (!validCollectionGrantValue(change.Expected.Value) || change.Expected.Version.Validate() != nil) {
			return false
		}
		var value CollectionGrantValue
		switch change.Kind {
		case "put":
			if change.Value == nil || !validCollectionGrantValue(*change.Value) {
				return false
			}
			value = *change.Value
			if e := change.Expected; e != nil {
				if e.Value.SubjectID != value.SubjectID || e.Value.Scope != value.Scope || !e.Removed && equalCollectionGrantValue(e.Value, value) {
					return false
				}
			}
		case "remove":
			if change.Value != nil || change.Expected == nil || change.Expected.Removed {
				return false
			}
			value = change.Expected.Value
		default:
			return false
		}
		if value.Scope.Kind != scopeKind || value.Scope.CollectionID != collection {
			return false
		}
	}
	return true
}

func CanonicalCollectionPolicyRequest(op CollectionPolicyRequest) ([]byte, string, error) {
	if op.Version != 2 || op.LibraryID.Validate() != nil || op.ID.Validate() != nil {
		return nil, "", invalid("collection policy envelope")
	}
	bound := op.BindingOperationID != "" || op.BindingPayloadSHA256 != ""
	if bound && (op.Category != "collection" || op.BindingOperationID.Validate() != nil || !validCollectionDigest(op.BindingPayloadSHA256) || op.ID != generatedCollectionPolicyID(op.BindingOperationID)) {
		return nil, "", invalid("collection policy binding")
	}
	scope := "collection"
	limit := MaxCollectionRequestBytes
	switch op.Category {
	case "library_create":
		scope = "library_create"
		if op.CollectionID != "" || bound {
			return nil, "", invalid("collection policy scope")
		}
	case "collection":
		if op.CollectionID.Validate() != nil {
			return nil, "", invalid("collection policy scope")
		}
	case "security_remove":
		limit = MaxCollectionSecurityRequestBytes
		if bound || len(op.Changes) != 1 || op.Changes[0].Kind != "remove" {
			return nil, "", invalid("collection security removal")
		}
		if op.CollectionID == "" {
			scope = "library_create"
		} else if op.CollectionID.Validate() != nil {
			return nil, "", invalid("collection policy scope")
		}
	default:
		return nil, "", invalid("collection policy category")
	}
	if !validCollectionGrantChanges(op.Changes, scope, op.CollectionID) {
		return nil, "", invalid("collection policy changes")
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > limit {
		return nil, "", invalid("collection policy request bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.policy.operation\x00\x02", b), nil
}

func DecodeCollectionPolicyRequest(data []byte) (CollectionPolicyRequest, error) {
	var op CollectionPolicyRequest
	if len(data) == 0 || len(data) > MaxCollectionRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return CollectionPolicyRequest{}, invalid("collection policy request JSON")
	}
	b, _, err := CanonicalCollectionPolicyRequest(op)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionPolicyRequest{}, invalid("collection policy request")
	}
	return op, nil
}

func CanonicalCollectionPolicyRecord(record CollectionPolicyRecord) ([]byte, string, error) {
	if record.Version != 2 || record.OperatorSubjectID.Validate() != nil || record.RecordedUnixNano <= 0 {
		return nil, "", invalid("collection policy record")
	}
	if _, _, err := CanonicalCollectionPolicyRequest(record.Request); err != nil {
		return nil, "", err
	}
	bound := record.Request.BindingOperationID != ""
	if bound && record.Administration != "collection_service" || !bound && record.Administration != "local_owner" {
		return nil, "", invalid("collection policy administration")
	}
	if len(record.Results) != len(record.Request.Changes) {
		return nil, "", invalid("collection policy results")
	}
	for i, result := range record.Results {
		change := record.Request.Changes[i]
		if result.ID != change.ID || result.Version.Validate() != nil || result.Removed != (change.Kind == "remove") || change.Expected != nil && result.Version == change.Expected.Version {
			return nil, "", invalid("collection policy result")
		}
	}
	b, err := json.Marshal(record)
	if err != nil || len(b) > MaxCollectionPolicyRecordBytes {
		return nil, "", invalid("collection policy record bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.policy.record\x00\x02", b), nil
}

func DecodeCollectionPolicyRecord(data []byte) (CollectionPolicyRecord, error) {
	var record CollectionPolicyRecord
	if len(data) == 0 || len(data) > MaxCollectionPolicyRecordBytes || decodeCanonicalValue(data, &record) != nil {
		return CollectionPolicyRecord{}, invalid("collection policy record JSON")
	}
	b, _, err := CanonicalCollectionPolicyRecord(record)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionPolicyRecord{}, invalid("collection policy record")
	}
	return record, nil
}

// Namespace validates shape; it does not establish live owner authority or prove
// a generated record's actor matches its content record. Replay must bind both.
func (record CollectionPolicyRecord) Namespace() (CollectionPolicyNamespace, error) {
	if _, _, err := CanonicalCollectionPolicyRecord(record); err != nil {
		return CollectionPolicyNamespace{}, err
	}
	ns := CollectionPolicyNamespace{LibraryID: record.Request.LibraryID, Administration: record.Administration, OperationID: record.Request.ID}
	if record.Administration == "collection_service" {
		ns.SubjectID = record.OperatorSubjectID
	}
	return ns, nil
}

func CanonicalCollectionPolicyReceipt(receipt CollectionPolicyReceipt) ([]byte, error) {
	if receipt.Version != 2 || receipt.LibraryID.Validate() != nil || receipt.OperationID.Validate() != nil || !validCollectionDigest(receipt.PayloadSHA256) || receipt.RecordedUnixNano <= 0 {
		return nil, invalid("collection policy receipt")
	}
	b, err := json.Marshal(receipt)
	if err != nil || len(b) > MaxCollectionReceiptBytes {
		return nil, invalid("collection policy receipt bytes")
	}
	return b, nil
}

func DecodeCollectionPolicyReceipt(data []byte) (CollectionPolicyReceipt, error) {
	var receipt CollectionPolicyReceipt
	if len(data) == 0 || len(data) > MaxCollectionReceiptBytes || decodeCanonicalValue(data, &receipt) != nil {
		return CollectionPolicyReceipt{}, invalid("collection policy receipt JSON")
	}
	b, err := CanonicalCollectionPolicyReceipt(receipt)
	if err != nil || !bytes.Equal(b, data) {
		return CollectionPolicyReceipt{}, invalid("collection policy receipt")
	}
	return receipt, nil
}

func validCredentialActionsV2(actions []string) bool {
	if len(actions) == 0 || len(actions) > 9 {
		return false
	}
	for i, action := range actions {
		collection := action == "collections.create" || action == "collections.read" || action == "collections.edit" || action == "collections.admin"
		if !validAction(action) && !collection || i > 0 && actions[i-1] >= action {
			return false
		}
	}
	return true
}

func CanonicalAdminRequestV2(op AdminRequestV2) ([]byte, string, error) {
	if op.Version != 2 || op.LibraryID.Validate() != nil || !validOperationID(op.ID) || !validRandomID(op.ExpectedIncarnation, "auth_") || op.Kind != "issue_credential" {
		return nil, "", invalid("authority v2 envelope")
	}
	var value IssueCredentialRequestV2
	if decodeCanonicalValue(op.Value, &value) != nil || value.SubjectID.Validate() != nil || value.Scope.Kind != "library" || value.Scope.AssetID != "" || !validCredentialActionsV2(value.Actions) || value.ExpiresUnix <= 0 {
		return nil, "", invalid("authority v2 issuance")
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxRequestBytes {
		return nil, "", invalid("authority v2 request bytes")
	}
	return b, domainDigest("juicemount.catalog.authority.operation\x00\x02", b), nil
}

func DecodeAdminRequestV2(data []byte) (AdminRequestV2, error) {
	var op AdminRequestV2
	if len(data) == 0 || len(data) > MaxRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return AdminRequestV2{}, invalid("authority v2 request JSON")
	}
	b, _, err := CanonicalAdminRequestV2(op)
	if err != nil || !bytes.Equal(b, data) {
		return AdminRequestV2{}, invalid("authority v2 request")
	}
	return op, nil
}
