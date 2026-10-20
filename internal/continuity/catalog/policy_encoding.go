package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func validAction(a string) bool {
	switch a {
	case "activity.record", "assets.discover", "locations.read", "relationships.read", "revisions.read":
		return true
	}
	return false
}
func validActions(actions []string) bool {
	if len(actions) == 0 || len(actions) > 5 {
		return false
	}
	for i, a := range actions {
		if !validAction(a) || i > 0 && actions[i-1] >= a {
			return false
		}
	}
	return true
}
func containsAction(actions []string, action string) bool {
	for _, a := range actions {
		if a == action {
			return true
		}
	}
	return false
}
func validSubject(v SubjectValue) bool {
	return (v.Kind == "person" || v.Kind == "service") && validText(v.Label, 256, false)
}
func validGrant(v GrantValue) bool {
	return v.SubjectID.Validate() == nil && v.Scope.valid() && validActions(v.Actions)
}
func validExpectedSubject(v *ExpectedSubject) bool {
	return v != nil && validSubject(v.Value) && v.AuthorizationGeneration > 0 && v.UpdatedSequence > 0
}
func validExpectedGrant(v *ExpectedGrant) bool {
	return v == nil || validGrant(v.Value) && v.UpdatedSequence > 0
}

// All new decoders use fixed diagnostics: JSON decoder errors may echo secrets.
func decodeCanonicalValue(data []byte, value any) error {
	if strictJSON(data, value) != nil {
		return invalid("request JSON fields")
	}
	b, err := json.Marshal(value)
	if err != nil || !bytes.Equal(b, data) {
		return invalid("noncanonical request JSON")
	}
	return nil
}
func domainDigest(domain string, data []byte) string {
	h := sha256.New()
	h.Write([]byte(domain))
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func policyChangeValue(c PolicyChange) (any, error) {
	var value any
	switch c.Kind {
	case "create_subject":
		value = &CreateSubject{}
	case "update_subject":
		value = &UpdateSubject{}
	case "put_grant":
		value = &PutGrant{}
	case "remove_grant":
		value = &RemoveGrant{}
	default:
		return nil, invalid("policy change kind")
	}
	if err := decodeCanonicalValue(c.Value, value); err != nil {
		return nil, err
	}
	valid := false
	switch v := value.(type) {
	case *CreateSubject:
		valid = v.SubjectID.Validate() == nil && validSubject(v.Value)
	case *UpdateSubject:
		valid = v.SubjectID.Validate() == nil && validExpectedSubject(v.Expected) && validSubject(v.Value) && v.Expected.Value.Kind == v.Value.Kind
	case *PutGrant:
		valid = validOperationID(v.ID) && validExpectedGrant(v.Expected) && validGrant(v.Value) && (v.Expected == nil || v.Expected.Value.SubjectID == v.Value.SubjectID)
	case *RemoveGrant:
		valid = validOperationID(v.ID) && v.Expected != nil && validExpectedGrant(v.Expected) && !v.Expected.Removed
	}
	if !valid {
		return nil, invalid("policy change value")
	}
	return value, nil
}

func CanonicalPolicyRequest(op PolicyOperation) ([]byte, string, error) {
	if op.Version != 1 || op.LibraryID.Validate() != nil || !validOperationID(op.ID) || op.Administration != "local_owner" || op.OperatorSubjectID.Validate() != nil || len(op.Changes) == 0 || len(op.Changes) > MaxChanges {
		return nil, "", invalid("policy envelope")
	}
	for _, c := range op.Changes {
		if _, err := policyChangeValue(c); err != nil {
			return nil, "", err
		}
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxRequestBytes {
		return nil, "", invalid("policy request bytes")
	}
	return b, domainDigest("juicemount.catalog.policy.operation\x00\x01", b), nil
}

func DecodePolicyRequest(data []byte) (PolicyOperation, error) {
	var op PolicyOperation
	if len(data) == 0 || len(data) > MaxRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return op, invalid("policy request JSON")
	}
	b, _, err := CanonicalPolicyRequest(op)
	if err != nil || !bytes.Equal(b, data) {
		return PolicyOperation{}, invalid("policy request")
	}
	return op, nil
}

func adminValue(op AdminRequest) (any, error) {
	var value any
	switch op.Kind {
	case "bootstrap":
		value = &BootstrapRequest{}
	case "recover":
		value = &RecoverRequest{}
	case "apply_policy":
		value = &ApplyPolicyRequest{}
	case "issue_credential":
		value = &IssueCredentialRequest{}
	case "revoke_credential":
		value = &RevokeCredentialRequest{}
	default:
		return nil, invalid("admin kind")
	}
	if err := decodeCanonicalValue(op.Value, value); err != nil {
		return nil, err
	}
	valid := false
	switch v := value.(type) {
	case *BootstrapRequest:
		valid = op.ExpectedIncarnation == "" && v.OwnerSubjectID.Validate() == nil && (v.CreateLabel == nil || validText(*v.CreateLabel, 256, false))
	case *RecoverRequest:
		valid = v.OwnerSubjectID.Validate() == nil
	case *ApplyPolicyRequest:
		_, _, err := CanonicalPolicyRequest(v.Operation)
		valid = err == nil && v.Operation.ID == op.ID && v.Operation.LibraryID == op.LibraryID
	case *IssueCredentialRequest:
		valid = v.SubjectID.Validate() == nil && v.Scope.valid() && validActions(v.Actions) && v.ExpiresUnix > 0
	case *RevokeCredentialRequest:
		valid = validRandomID(v.Selector, "cred_") && v.ExpectedGeneration > 0
	}
	if !valid {
		return nil, invalid("admin value")
	}
	return value, nil
}

func CanonicalAdminRequest(op AdminRequest) ([]byte, string, error) {
	if op.Version != 1 || op.LibraryID.Validate() != nil || !validOperationID(op.ID) || (op.Kind != "bootstrap" && !validRandomID(op.ExpectedIncarnation, "auth_")) {
		return nil, "", invalid("admin envelope")
	}
	if _, err := adminValue(op); err != nil {
		return nil, "", err
	}
	b, err := json.Marshal(op)
	if err != nil || len(b) > MaxRequestBytes {
		return nil, "", invalid("admin request bytes")
	}
	return b, domainDigest("juicemount.catalog.authority.operation\x00\x01", b), nil
}
func DecodeAdminRequest(data []byte) (AdminRequest, error) {
	var op AdminRequest
	if len(data) == 0 || len(data) > MaxRequestBytes || decodeCanonicalValue(data, &op) != nil {
		return op, invalid("admin request JSON")
	}
	b, _, err := CanonicalAdminRequest(op)
	if err != nil || !bytes.Equal(b, data) {
		return AdminRequest{}, invalid("admin request")
	}
	return op, nil
}
