package catalog

import (
	"bytes"
	"encoding/json"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

type SourceStatusRequest struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	SourceID string `json:"source_id"`
	SourceVersion int64 `json:"source_version"`
}
type SourceDisableRequest struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	ID string `json:"operation_id"`
	ExpectedIncarnation string `json:"expected_incarnation"`
	SourceID string `json:"source_id"`
	ExpectedSourceVersion int64 `json:"expected_source_version"`
}
type DeliveryInspectRequest struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	SelectionID string `json:"selection_id"`
}
type DeliveryLookupRequest struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	SelectionOperationID string `json:"selection_operation_id"`
}
type DeliveryCancelRequest struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	ID string `json:"operation_id"`
	SelectionID string `json:"selection_id"`
}
type TerminalReceipt struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	OperationID string `json:"operation_id"`
	Kind string `json:"kind"`
	PayloadSHA256 string `json:"payload_sha256"`
	ActorSubjectID SubjectID `json:"actor_subject_id"`
	AuthorityIncarnation string `json:"authority_incarnation"`
	RecordedUnixNano int64 `json:"recorded_unix_nano"`
	SourceID string `json:"source_id,omitempty"`
	SourceVersion int64 `json:"source_version,omitempty"`
	SelectionID string `json:"selection_id,omitempty"`
}
type SourceStatus struct {
	Version int `json:"version"`
	LibraryID continuity.LibraryID `json:"library_id"`
	SourceID string `json:"source_id"`
	SourceVersion int64 `json:"source_version"`
	CurrentSourceVersion int64 `json:"current_source_version"`
	AssetID continuity.AssetID `json:"asset_id"`
	RevisionID continuity.RevisionID `json:"revision_id"`
	Content continuity.Content `json:"content"`
	Enabled bool `json:"enabled"`
	Eligibility string `json:"eligibility"`
	Terminal string `json:"terminal"`
}
type SourceBindResult struct {
	OriginalReceipt SourceReceipt `json:"original_receipt"`
	CurrentStatus SourceStatus `json:"current_status"`
}
type SourceDisableResult struct {
	OriginalReceipt TerminalReceipt `json:"original_receipt"`
	CurrentStatus SourceStatus `json:"current_status"`
}
type DeliveryRecordState struct {
	Canceled bool `json:"canceled"`
	Expired bool `json:"expired"`
	Terminal string `json:"terminal"`
}
type DeliveryEligibility struct {
	Eligible bool `json:"eligible"`
	Reason string `json:"reason"`
}
type DeliveryInspection struct {
	OriginalSelection DeliverySelection `json:"original_selection"`
	RecordState DeliveryRecordState `json:"record_state"`
	RequesterDeliveryEligibility DeliveryEligibility `json:"requester_delivery_eligibility"`
}
type DeliveryCancelResult struct {
	OriginalReceipt TerminalReceipt `json:"original_receipt"`
	CurrentStatus DeliveryInspection `json:"current_status"`
}

func lifecycleCanonical(value any, valid bool, max int) ([]byte, error) {
	if !valid { return nil, invalid("delivery lifecycle fields") }
	b, err := json.Marshal(value)
	if err != nil || len(b) == 0 || len(b) > max { return nil, invalid("delivery lifecycle bytes") }
	return b, nil
}
func CanonicalSourceStatusRequest(r SourceStatusRequest) ([]byte, error) {
	return lifecycleCanonical(r, r.Version == 1 && r.LibraryID.Validate() == nil && validRandomID(r.SourceID,"dsrc_") && r.SourceVersion > 0, MaxTerminalRequestBytes)
}
func DecodeSourceStatusRequest(b []byte) (SourceStatusRequest, error) {
	var r SourceStatusRequest
	if len(b)==0 || len(b)>MaxTerminalRequestBytes || decodeCanonicalValue(b,&r)!=nil { return r, invalid("source status JSON") }
	a, err := CanonicalSourceStatusRequest(r); if err != nil || !bytes.Equal(a,b) { return r, invalid("source status request") }; return r,nil
}
func CanonicalSourceDisableRequest(r SourceDisableRequest) ([]byte,string,error) {
	b, err := lifecycleCanonical(r, r.Version==1 && r.LibraryID.Validate()==nil && validOperationID(r.ID) && validRandomID(r.ExpectedIncarnation,"auth_") && validRandomID(r.SourceID,"dsrc_") && r.ExpectedSourceVersion>0, MaxTerminalRequestBytes)
	return b, domainDigest("juicemount.catalog.delivery.source-disable\x00\x01",b), err
}
func DecodeSourceDisableRequest(b []byte) (SourceDisableRequest,error) {
	var r SourceDisableRequest
	if len(b)==0 || len(b)>MaxTerminalRequestBytes || decodeCanonicalValue(b,&r)!=nil { return r,invalid("source disable JSON") }
	a,_,err:=CanonicalSourceDisableRequest(r); if err!=nil || !bytes.Equal(a,b) { return r,invalid("source disable request") }; return r,nil
}
func CanonicalDeliveryInspectRequest(r DeliveryInspectRequest) ([]byte,error) {
	return lifecycleCanonical(r,r.Version==1 && r.LibraryID.Validate()==nil && validRandomID(r.SelectionID,"dsel_"),MaxTerminalRequestBytes)
}
func DecodeDeliveryInspectRequest(b []byte) (DeliveryInspectRequest,error) {
	var r DeliveryInspectRequest
	if len(b)==0 || len(b)>MaxTerminalRequestBytes || decodeCanonicalValue(b,&r)!=nil { return r,invalid("delivery inspect JSON") }
	a,err:=CanonicalDeliveryInspectRequest(r); if err!=nil || !bytes.Equal(a,b) { return r,invalid("delivery inspect request") }; return r,nil
}
func CanonicalDeliveryLookupRequest(r DeliveryLookupRequest) ([]byte,error) {
	return lifecycleCanonical(r,r.Version==1 && r.LibraryID.Validate()==nil && validOperationID(r.SelectionOperationID),MaxTerminalRequestBytes)
}
func DecodeDeliveryLookupRequest(b []byte) (DeliveryLookupRequest,error) {
	var r DeliveryLookupRequest
	if len(b)==0 || len(b)>MaxTerminalRequestBytes || decodeCanonicalValue(b,&r)!=nil { return r,invalid("delivery lookup JSON") }
	a,err:=CanonicalDeliveryLookupRequest(r); if err!=nil || !bytes.Equal(a,b) { return r,invalid("delivery lookup request") }; return r,nil
}
func CanonicalDeliveryCancelRequest(r DeliveryCancelRequest) ([]byte,string,error) {
	b,err:=lifecycleCanonical(r,r.Version==1 && r.LibraryID.Validate()==nil && validOperationID(r.ID) && validRandomID(r.SelectionID,"dsel_"),MaxTerminalRequestBytes)
	return b,domainDigest("juicemount.catalog.delivery.selection-cancel\x00\x01",b),err
}
func DecodeDeliveryCancelRequest(b []byte) (DeliveryCancelRequest,error) {
	var r DeliveryCancelRequest
	if len(b)==0 || len(b)>MaxTerminalRequestBytes || decodeCanonicalValue(b,&r)!=nil { return r,invalid("delivery cancel JSON") }
	a,_,err:=CanonicalDeliveryCancelRequest(r); if err!=nil || !bytes.Equal(a,b) { return r,invalid("delivery cancel request") }; return r,nil
}
func CanonicalTerminalReceipt(r TerminalReceipt) ([]byte,error) {
	target:=r.Kind=="source_disable" && validRandomID(r.SourceID,"dsrc_") && r.SourceVersion>0 && r.SelectionID=="" || r.Kind=="selection_cancel" && validRandomID(r.SelectionID,"dsel_") && r.SourceID=="" && r.SourceVersion==0
	return lifecycleCanonical(r,r.Version==1 && r.LibraryID.Validate()==nil && validOperationID(r.OperationID) && validCollectionDigest(r.PayloadSHA256) && r.ActorSubjectID.Validate()==nil && validRandomID(r.AuthorityIncarnation,"auth_") && r.RecordedUnixNano>0 && target,MaxTerminalReceiptBytes)
}
func DecodeTerminalReceipt(b []byte) (TerminalReceipt,error) {
	var r TerminalReceipt
	if len(b)==0 || len(b)>MaxTerminalReceiptBytes || decodeCanonicalValue(b,&r)!=nil { return r,invalid("terminal receipt JSON") }
	a,err:=CanonicalTerminalReceipt(r); if err!=nil || !bytes.Equal(a,b) { return r,invalid("terminal receipt") }; return r,nil
}
