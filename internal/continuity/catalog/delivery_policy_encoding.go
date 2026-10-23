package catalog

import (
    "bytes"
    "encoding/json"

    "github.com/lelanddutcher/juicemount/internal/continuity"
)

// Byte policy is independent of both legacy metadata and collection grants.
// Its only action is bytes.read; the scope is an exact asset or library.
type ByteGrantValue struct {
    SubjectID SubjectID `json:"subject_id"`
    Scope Scope `json:"scope"`
    Action string `json:"action"`
}
type ExpectedByteGrant struct {
    Value ByteGrantValue `json:"value"`
    Version string `json:"version"`
    Removed bool `json:"removed"`
}
type ByteGrantChange struct {
    Kind string `json:"kind"`
    ID string `json:"grant_id"`
    Expected *ExpectedByteGrant `json:"expected,omitempty"`
    Value *ByteGrantValue `json:"value,omitempty"`
}
type BytePolicyRequest struct {
    Version int `json:"version"`
    LibraryID continuity.LibraryID `json:"library_id"`
    ID string `json:"operation_id"`
    ExpectedIncarnation string `json:"expected_incarnation"`
    Changes []ByteGrantChange `json:"changes"`
}
type ByteGrantResult struct {
    ID string `json:"grant_id"`
    Version string `json:"version"`
    Removed bool `json:"removed"`
}
type BytePolicyReceipt struct {
    Version int `json:"version"`
    LibraryID continuity.LibraryID `json:"library_id"`
    OperationID string `json:"operation_id"`
    Sequence int64 `json:"sequence"`
    PayloadSHA256 string `json:"payload_sha256"`
    RecordedUnixNano int64 `json:"recorded_unix_nano"`
    Results []ByteGrantResult `json:"results"`
}
type BytePolicyRecord struct {
    Version int `json:"version"`
    OperatorSubjectID SubjectID `json:"operator_subject_id"`
    Request BytePolicyRequest `json:"request"`
    Receipt BytePolicyReceipt `json:"receipt"`
}
func validByteGrantValue(v ByteGrantValue) bool { return v.SubjectID.Validate()==nil && v.Scope.valid() && v.Action=="bytes.read" }
func CanonicalBytePolicyRequest(op BytePolicyRequest) ([]byte,string,error) {
    if op.Version!=1 || op.LibraryID.Validate()!=nil || !validOperationID(op.ID) || !validRandomID(op.ExpectedIncarnation,"auth_") || len(op.Changes)==0 || len(op.Changes)>16 { return nil,"",invalid("byte policy envelope") }
    for i,c:=range op.Changes {
        if !validRandomID(c.ID,"bgr_") || i>0 && op.Changes[i-1].ID>=c.ID { return nil,"",invalid("byte policy order") }
        if c.Expected!=nil && (!validByteGrantValue(c.Expected.Value) || !validRandomID(c.Expected.Version,"bgv_")) { return nil,"",invalid("byte policy expected") }
        switch c.Kind {
        case "put":
            if c.Value==nil || !validByteGrantValue(*c.Value) || c.Expected!=nil && (c.Expected.Value.SubjectID!=c.Value.SubjectID || c.Expected.Value.Scope!=c.Value.Scope || !c.Expected.Removed && c.Expected.Value==*c.Value) { return nil,"",invalid("byte policy put") }
        case "remove":
            if c.Expected==nil || c.Expected.Removed || c.Value!=nil { return nil,"",invalid("byte policy remove") }
        default: return nil,"",invalid("byte policy kind")
        }
    }
    b,err:=json.Marshal(op)
    if err!=nil || len(b)>32768 { return nil,"",invalid("byte policy request bytes") }
    return b,domainDigest("juicemount.catalog.byte-policy.operation\x00\x01",b),nil
}
func DecodeBytePolicyRequest(b []byte) (BytePolicyRequest,error) {
    var op BytePolicyRequest
    if len(b)==0 || len(b)>32768 || decodeCanonicalValue(b,&op)!=nil { return BytePolicyRequest{},invalid("byte policy request JSON") }
    encoded,_,err:=CanonicalBytePolicyRequest(op)
    if err!=nil || !bytes.Equal(encoded,b) { return BytePolicyRequest{},invalid("byte policy request") }
    return op,nil
}
func CanonicalBytePolicyReceipt(r BytePolicyReceipt) ([]byte,error) {
    if r.Version!=1 || r.LibraryID.Validate()!=nil || !validOperationID(r.OperationID) || !validCollectionDigest(r.PayloadSHA256) || r.Sequence<=0 || r.RecordedUnixNano<=0 || r.Results==nil || len(r.Results)>16 { return nil,invalid("byte policy receipt") }
    for i,result:=range r.Results { if !validRandomID(result.ID,"bgr_") || !validRandomID(result.Version,"bgv_") || i>0 && r.Results[i-1].ID>=result.ID { return nil,invalid("byte policy result") } }
    b,err:=json.Marshal(r)
    if err!=nil || len(b)>2048 { return nil,invalid("byte policy receipt bytes") }
    return b,nil
}
func DecodeBytePolicyReceipt(b []byte) (BytePolicyReceipt,error) {
    var r BytePolicyReceipt
    if len(b)==0 || len(b)>2048 || decodeCanonicalValue(b,&r)!=nil { return BytePolicyReceipt{},invalid("byte policy receipt JSON") }
    encoded,err:=CanonicalBytePolicyReceipt(r)
    if err!=nil || !bytes.Equal(encoded,b) { return BytePolicyReceipt{},invalid("byte policy receipt") }
    return r,nil
}
func CanonicalBytePolicyRecord(r BytePolicyRecord) ([]byte,string,error) {
    request,_,err:=CanonicalBytePolicyRequest(r.Request)
    if r.Version!=1 || r.OperatorSubjectID.Validate()!=nil || err!=nil || r.Receipt.LibraryID!=r.Request.LibraryID || r.Receipt.OperationID!=r.Request.ID || r.Receipt.PayloadSHA256!=domainDigest("juicemount.catalog.byte-policy.operation\x00\x01",request) || len(r.Receipt.Results)!=len(r.Request.Changes) { return nil,"",invalid("byte policy record") }
    if _,err=CanonicalBytePolicyReceipt(r.Receipt);err!=nil{return nil,"",err}
    for i,result:=range r.Receipt.Results { if result.ID!=r.Request.Changes[i].ID || result.Removed!=(r.Request.Changes[i].Kind=="remove") || r.Request.Changes[i].Expected!=nil && result.Version==r.Request.Changes[i].Expected.Version { return nil,"",invalid("byte policy recorded result") } }
    b,err:=json.Marshal(r)
    if err!=nil || len(b)>36864 {return nil,"",invalid("byte policy record bytes")}
    return b,domainDigest("juicemount.catalog.byte-policy.record\x00\x01",b),nil
}
func DecodeBytePolicyRecord(b []byte) (BytePolicyRecord,error) {
    var r BytePolicyRecord
    if len(b)==0 || len(b)>36864 || decodeCanonicalValue(b,&r)!=nil { return BytePolicyRecord{},invalid("byte policy record JSON") }
    encoded,_,err:=CanonicalBytePolicyRecord(r)
    if err!=nil || !bytes.Equal(encoded,b) { return BytePolicyRecord{},invalid("byte policy record") }
    return r,nil
}
