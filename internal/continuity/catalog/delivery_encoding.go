package catalog

import (
    "bytes"
    "encoding/json"

    "github.com/lelanddutcher/juicemount/internal/continuity"
)

type AdminRequestV3 struct {
    Version int `json:"version"`
    LibraryID continuity.LibraryID `json:"library_id"`
    ID string `json:"operation_id"`
    ExpectedIncarnation string `json:"expected_incarnation"`
    Kind string `json:"kind"`
    Value json.RawMessage `json:"value"`
}
type IssueCredentialRequestV3 struct {
    SubjectID SubjectID `json:"subject_id"`
    Scope Scope `json:"scope"`
    Actions []string `json:"actions"`
    ExpiresUnix int64 `json:"expires_unix"`
}
type AdminReceiptV3 AdminReceipt
type IssuanceResultV3 struct {
    Receipt AdminReceiptV3 `json:"receipt"`
    Status string `json:"status"`
    Bearer []byte `json:"-"`
}
func (r IssuanceResultV3) String() string { return "catalog issuance v3 result (secret redacted)" }
func (r IssuanceResultV3) GoString() string { return r.String() }

func validCredentialActionsV3(scope Scope, actions []string) bool {
    if !scope.valid() || len(actions)==0 { return false }
    limit:=10
    if scope.Kind=="asset" { limit=6 }
    if len(actions)>limit { return false }
    for i,a:=range actions {
        allowed:=validAction(a) || a=="bytes.read"
        if scope.Kind=="library" { allowed=allowed || a=="collections.create" || a=="collections.read" || a=="collections.edit" || a=="collections.admin" }
        if !allowed || i>0 && actions[i-1]>=a { return false }
    }
    return true
}
func CanonicalAdminRequestV3(op AdminRequestV3) ([]byte,string,error) {
    if op.Version!=3 || op.LibraryID.Validate()!=nil || !validOperationID(op.ID) || !validRandomID(op.ExpectedIncarnation,"auth_") || op.Kind!="issue_credential" { return nil,"",invalid("authority v3 envelope") }
    var value IssueCredentialRequestV3
    if decodeCanonicalValue(op.Value,&value)!=nil || value.SubjectID.Validate()!=nil || !validCredentialActionsV3(value.Scope,value.Actions) || value.ExpiresUnix<=0 { return nil,"",invalid("authority v3 issuance") }
    b,err:=json.Marshal(op)
    if err!=nil || len(b)>MaxRequestBytes { return nil,"",invalid("authority v3 request bytes") }
    return b,domainDigest("juicemount.catalog.authority.operation\x00\x03",b),nil
}
func DecodeAdminRequestV3(data []byte) (AdminRequestV3,error) {
    var op AdminRequestV3
    if len(data)==0 || len(data)>MaxRequestBytes || decodeCanonicalValue(data,&op)!=nil { return AdminRequestV3{},invalid("authority v3 request JSON") }
    b,_,err:=CanonicalAdminRequestV3(op)
    if err!=nil || !bytes.Equal(b,data) { return AdminRequestV3{},invalid("authority v3 request") }
    return op,nil
}
func decodeAdminReceiptV3(b []byte, request AdminRequestV3, data []byte, digest string, sequence,timestamp int64) (AdminReceiptV3,error) {
    var r AdminReceiptV3
    var value IssueCredentialRequestV3
    if decodeCanonicalValue(request.Value,&value)!=nil { return r,corrupt("authority v3 issuance request") }
    if decodeCanonicalValue(b,&r)!=nil || r.Version!=3 || r.LibraryID!=request.LibraryID || r.OperationID!=request.ID || r.Sequence!=sequence || r.RecordedUnixNano!=timestamp || timestamp<=0 || r.PayloadSHA256!=digest || digest!=domainDigest("juicemount.catalog.authority.operation\x00\x03",data) || !validRandomID(r.Result.Incarnation,"auth_") || r.Result.OwnerSubjectID.Validate()!=nil || r.Result.SubjectID!=value.SubjectID || r.Result.Incarnation!=request.ExpectedIncarnation || !validRandomID(r.Result.Selector,"cred_") || r.Result.PolicySequence!=0 { return AdminReceiptV3{},corrupt("authority v3 receipt") }
    return r,nil
}
