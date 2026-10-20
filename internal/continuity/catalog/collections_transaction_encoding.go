package catalog

import (
	"bytes"
	"encoding/json"
)

func CanonicalCollectionOwnerRequest(op CollectionOwnerRequest) ([]byte, string, error) {
	if op.Version != 1 || op.LibraryID.Validate() != nil || !validOperationID(op.ID) || !validRandomID(op.ExpectedIncarnation, "auth_") || op.Kind != "apply_collection_policy" {
		return nil, "", invalid("collection owner envelope")
	}
	var value ApplyCollectionPolicyRequest
	if decodeCanonicalValue(op.Value, &value) != nil {
		return nil, "", invalid("collection owner value")
	}
	request := value.Operation
	if _, _, err := CanonicalCollectionPolicyRequest(request); err != nil {
		return nil, "", err
	}
	if request.LibraryID != op.LibraryID || string(request.ID) != op.ID || request.BindingOperationID != "" || request.BindingPayloadSHA256 != "" {
		return nil, "", invalid("collection owner binding")
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxRequestBytes {
		return nil, "", invalid("collection owner bytes")
	}
	return b, domainDigest("juicemount.catalog.collection.authority.operation\x00\x01", b), nil
}
func DecodeCollectionOwnerRequest(b []byte) (CollectionOwnerRequest, error) {
	var op CollectionOwnerRequest
	if len(b) == 0 || len(b) > MaxRequestBytes || decodeCanonicalValue(b, &op) != nil {
		return CollectionOwnerRequest{}, invalid("collection owner JSON")
	}
	encoded, _, err := CanonicalCollectionOwnerRequest(op)
	if err != nil || !bytes.Equal(encoded, b) {
		return CollectionOwnerRequest{}, invalid("collection owner request")
	}
	return op, nil
}
func decodeAdminReceiptV2(b []byte, request AdminRequestV2, data []byte, digest string, sequence, timestamp int64) (AdminReceiptV2, error) {
	var r AdminReceiptV2
	var value IssueCredentialRequestV2
	if decodeCanonicalValue(request.Value, &value) != nil {
		return AdminReceiptV2{}, corrupt("authority v2 issuance request")
	}
	if decodeCanonicalValue(b, &r) != nil || r.Version != 2 || r.LibraryID != request.LibraryID || r.OperationID != request.ID || r.Sequence != sequence || r.RecordedUnixNano != timestamp || timestamp <= 0 || r.PayloadSHA256 != digest || digest != domainDigest("juicemount.catalog.authority.operation\x00\x02", data) || !validRandomID(r.Result.Incarnation, "auth_") || r.Result.OwnerSubjectID.Validate() != nil || r.Result.SubjectID != value.SubjectID || r.Result.Incarnation != request.ExpectedIncarnation || !validRandomID(r.Result.Selector, "cred_") || r.Result.PolicySequence != 0 {
		return AdminReceiptV2{}, corrupt("authority v2 receipt")
	}
	return r, nil
}
