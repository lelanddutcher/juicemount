package catalog

import (
	"context"
	"database/sql"
	"errors"
)

// Inspection checks current disclosure rights and same-subject ownership, not
// original-selector usability. Eligibility is explicitly for THIS requester.
func (s *Store) inspectDeliverySnapshot(ctx context.Context,f *deliveryClockTxn,bearer []byte,id string) (DeliveryInspection,credential,error) {
	var result DeliveryInspection
	now,err:=f.observe(ctx);if err!=nil { return result,credential{},err }
	_,c,sub,err:=s.authenticateSnapshotAt(ctx,f.tx,s.library,bearer,now);if err!=nil { return result,c,err }
	selection,_,selector,incarnation,credentialGeneration,subjectGeneration,budget,err:=loadSelection(ctx,f.tx,s,id)
	if err!=nil { return result,c,err }
	var owner SubjectID
	if err:=f.tx.QueryRowContext(ctx,`SELECT subject_id FROM delivery_selections WHERE selection_id=? AND library_id=?`,id,s.library).Scan(&owner);err!=nil { return result,c,err }
	if owner!=c.subject { return result,c,ErrNotFound }
	if err:=deliveryRights(ctx,f.tx,s,c,selection.AssetID);err!=nil { return result,c,err }
	result.OriginalSelection=selection
	result.OriginalSelection.Canceled=false
	result.RecordState=DeliveryRecordState{Canceled:selection.Canceled,Expired:now>=selection.ExpiresUnix,Terminal:"none"}
	if supportsDeliveryLifecycle(s.version) {
		slot,err:=readSelectionTerminal(ctx,f.tx,s.library,id,selection.Canceled);if err!=nil { return DeliveryInspection{},c,err }
		if slot.legacy { result.RecordState.Terminal="legacy" } else if slot.bit { result.RecordState.Terminal="committed" }
	} else if selection.Canceled { result.RecordState.Terminal="legacy" }
	eligibility:=DeliveryEligibility{Eligible:false,Reason:"ineligible"}
	switch {
	case selection.Canceled:eligibility.Reason="canceled"
	case now>=selection.ExpiresUnix:eligibility.Reason="expired"
	case c.selector!=selector:eligibility.Reason="selector_mismatch"
	case c.generation!=credentialGeneration || c.incarnation!=incarnation || sub.AuthorizationGeneration!=subjectGeneration:eligibility.Reason="pin_changed"
	case selection.Content.Size>budget:eligibility.Reason="budget"
	default:
		// This strict fence validates source, immutable manifest and all original
		// generations. No network or final byte authority is implied by status.
		_,err:=s.selectionFence(ctx,f,bearer,id)
		if err==nil { eligibility=DeliveryEligibility{true,"eligible"} } else if errors.Is(err,ErrNotFound) { eligibility.Reason="source_or_pin_changed" } else { return DeliveryInspection{},c,err }
	}
	result.RequesterDeliveryEligibility=eligibility
	return result,c,nil
}
func (s *Store) InspectDelivery(ctx context.Context,bearer []byte,op DeliveryInspectRequest) (result DeliveryInspection,resultErr error) {
	if err:=s.enter();err!=nil { return result,err };defer s.leave()
	if !supportsDelivery(s.version) { return result,ErrUpgradeRequired }
	if _,err:=CanonicalDeliveryInspectRequest(op);err!=nil { return result,err };if op.LibraryID!=s.library { return result,ErrNotFound }
	f,err:=s.beginDeliveryClockTxn(ctx);if err!=nil { return result,err };defer f.finish(&resultErr)
	if err:=s.checkVersion(ctx,f.tx);err!=nil { return result,err }
	result,_,err=s.inspectDeliverySnapshot(ctx,f,bearer,op.SelectionID);if err!=nil { return DeliveryInspection{},err }
	if err:=ctx.Err();err!=nil { return DeliveryInspection{},err };if err:=s.checkPaths();err!=nil { return DeliveryInspection{},err }
	if err:=f.commit();err!=nil { return DeliveryInspection{},terminalCommitError("delivery inspect",err) };return result,nil
}
func (s *Store) LookupDelivery(ctx context.Context,bearer []byte,op DeliveryLookupRequest) (result DeliveryInspection,resultErr error) {
	if err:=s.enter();err!=nil { return result,err };defer s.leave()
	if !supportsDelivery(s.version) { return result,ErrUpgradeRequired }
	if _,err:=CanonicalDeliveryLookupRequest(op);err!=nil { return result,err };if op.LibraryID!=s.library { return result,ErrNotFound }
	f,err:=s.beginDeliveryClockTxn(ctx);if err!=nil { return result,err };defer f.finish(&resultErr)
	if err:=s.checkVersion(ctx,f.tx);err!=nil { return result,err }
	now,err:=f.observe(ctx);if err!=nil { return result,err }
	_,c,_,err:=s.authenticateSnapshotAt(ctx,f.tx,s.library,bearer,now);if err!=nil { return result,err }
	var id string
	err=f.tx.QueryRowContext(ctx,`SELECT selection_id FROM delivery_selections WHERE library_id=? AND subject_id=? AND operation_id=?`,s.library,c.subject,op.SelectionOperationID).Scan(&id)
	if errors.Is(err,sql.ErrNoRows) { return result,ErrNotFound };if err!=nil { return result,err }
	result,_,err=s.inspectDeliverySnapshot(ctx,f,bearer,id);if err!=nil { return DeliveryInspection{},err }
	if err:=ctx.Err();err!=nil { return DeliveryInspection{},err };if err:=s.checkPaths();err!=nil { return DeliveryInspection{},err }
	if err:=f.commit();err!=nil { return DeliveryInspection{},terminalCommitError("delivery lookup",err) };return result,nil
}
func (s *Store) CancelDelivery(ctx context.Context,bearer []byte,op DeliveryCancelRequest) (result DeliveryCancelResult,resultErr error) {
	if err:=s.enter();err!=nil { return result,err };defer s.leave()
	if !supportsDeliveryLifecycle(s.version) { return result,ErrUpgradeRequired }
	request,digest,err:=CanonicalDeliveryCancelRequest(op);if err!=nil { return result,err };if op.LibraryID!=s.library { return result,ErrNotFound }
	f,err:=s.beginDeliveryClockTxn(ctx);if err!=nil { return result,err };defer f.finish(&resultErr)
	if err:=s.checkVersion(ctx,f.tx);err!=nil { return result,err }
	status,c,err:=s.inspectDeliverySnapshot(ctx,f,bearer,op.SelectionID);if err!=nil { return result,err }
	prior,found,err:=terminalPrior(ctx,f.tx,s.library,"selection_cancel",op.ID,c.subject,request);if err!=nil { return result,err }
	if found { result=DeliveryCancelResult{prior,status} } else {
		slot,err:=readSelectionTerminal(ctx,f.tx,s.library,op.SelectionID,status.RecordState.Canceled);if err!=nil { return result,err }
		if slot.bit { return result,ErrConflict }
		r:=TerminalReceipt{Version:1,LibraryID:s.library,OperationID:op.ID,Kind:"selection_cancel",PayloadSHA256:digest,ActorSubjectID:c.subject,AuthorityIncarnation:c.incarnation,RecordedUnixNano:s.now().UnixNano(),SelectionID:op.SelectionID}
		if err:=fillTerminal(ctx,f.tx,r,request);err!=nil { return result,err }
		if err:=changedOne(f.tx.ExecContext(ctx,`UPDATE delivery_selections SET canceled=1 WHERE selection_id=? AND subject_id=? AND canceled=0`,op.SelectionID,c.subject));err!=nil { return result,err }
		status,_,err=s.inspectDeliverySnapshot(ctx,f,bearer,op.SelectionID);if err!=nil { return result,err }
		result=DeliveryCancelResult{r,status}
	}
	if err:=ctx.Err();err!=nil { return DeliveryCancelResult{},err };if err:=s.checkPaths();err!=nil { return DeliveryCancelResult{},err }
	if err:=f.commit();err!=nil { return DeliveryCancelResult{},terminalCommitError("delivery cancel",err) };return result,nil
}
