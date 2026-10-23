package catalog

import (
    "bytes"
    "context"
    "database/sql"
    "errors"
    "fmt"
    "math"

    "github.com/lelanddutcher/juicemount/internal/continuity"
)

type byteGrantRow struct { expected ExpectedByteGrant; library string }
func readByteGrant(ctx context.Context,q queryer,library string,id string) (byteGrantRow,error) {
    var r byteGrantRow
    var asset string
    var kind,action string
    err:=q.QueryRowContext(ctx,`SELECT library_id,subject_id,scope_kind,COALESCE(asset_id,''),action,grant_version,removed FROM byte_grants WHERE grant_id=?`,id).Scan(&r.library,&r.expected.Value.SubjectID,&kind,&asset,&action,&r.expected.Version,&r.expected.Removed)
    if errors.Is(err,sql.ErrNoRows) { return r,ErrNotFound }
    if err!=nil {return r,corrupt("byte grant fields")}
    r.expected.Value.Scope.Kind=kind
    r.expected.Value.Scope.AssetID=continuityAssetID(asset)
    r.expected.Value.Action=action
    if r.library!=library || !validByteGrantValue(r.expected.Value) || !validRandomID(r.expected.Version,"bgv_") { return byteGrantRow{},corrupt("byte grant fields") }
    return r,nil
}
// A local alias keeps SQL's nullable asset conversion explicit.
func continuityAssetID(v string) continuity.AssetID { return continuity.AssetID(v) }

func bytePolicyPrior(ctx context.Context,q queryer,op BytePolicyRequest,data []byte) (BytePolicyReceipt,bool,error) {
    var existing,receipt []byte
    var savedDigest string
    var sequence,timestamp int64
    err:=q.QueryRowContext(ctx,`SELECT request,payload_sha256,receipt,sequence,recorded_unix_nano FROM byte_policy_events WHERE operation_id=?`,op.ID).Scan(&existing,&savedDigest,&receipt,&sequence,&timestamp)
    if errors.Is(err,sql.ErrNoRows) {return BytePolicyReceipt{},false,nil}
    if err!=nil {return BytePolicyReceipt{},false,err}
    if !bytes.Equal(existing,data) {return BytePolicyReceipt{},false,ErrChangedRequest}
    r,err:=DecodeBytePolicyReceipt(receipt)
    if err!=nil || r.OperationID!=op.ID || r.LibraryID!=op.LibraryID || r.Sequence!=sequence || r.RecordedUnixNano!=timestamp || r.PayloadSHA256!=savedDigest || savedDigest!=domainDigest("juicemount.catalog.byte-policy.operation\x00\x01",data) {return BytePolicyReceipt{},false,corrupt("byte policy prior receipt")}
    return r,true,nil
}

// applyBytePolicyStructural is the one chronology-preserving projection step
// used by live owner administration and by format-5 replay.
func (s *Store) applyBytePolicyStructural(ctx context.Context,tx *sql.Tx,op BytePolicyRequest,operator SubjectID,timestamp int64, versions []string) (BytePolicyReceipt,error) {
    request,digest,err:=CanonicalBytePolicyRequest(op)
    if err!=nil{return BytePolicyReceipt{},err}
    if op.LibraryID!=s.library || len(versions)!=len(op.Changes) || timestamp<=0 {return BytePolicyReceipt{},invalid("byte policy attribution")}
    sequence,err:=nextSequence(ctx,tx,"byte_policy_events")
    if err!=nil{return BytePolicyReceipt{},err}
    results:=make([]ByteGrantResult,0,len(op.Changes))
    touched:=map[SubjectID]bool{}
    var added,deltaReserve int64
    for i,c:=range op.Changes {
        prior,readErr:=readByteGrant(ctx,tx,string(s.library),c.ID)
        missing:=errors.Is(readErr,ErrNotFound)
        if readErr!=nil && !missing{return BytePolicyReceipt{},readErr}
        if missing && c.Expected!=nil || !missing && (c.Expected==nil || prior.expected!=*c.Expected) {return BytePolicyReceipt{},ErrConflict}
        value:=ByteGrantValue{}
        removed:=c.Kind=="remove"
        if removed {
            if missing || prior.expected.Removed {return BytePolicyReceipt{},ErrConflict}
            value=prior.expected.Value
            deltaReserve--
        } else {
            value=*c.Value
            subject,err:=readSubject(ctx,tx,s.library,value.SubjectID)
            if err!=nil{return BytePolicyReceipt{},err}
            if !subject.Value.Enabled {return BytePolicyReceipt{},ErrConflict}
            if value.Scope.Kind=="asset" {if _,err=readAsset(ctx,tx,s.library,value.Scope.AssetID);err!=nil{return BytePolicyReceipt{},err}}
            if missing {added++}
            if missing || prior.expected.Removed {deltaReserve++}
        }
        if !validRandomID(versions[i],"bgv_") {return BytePolicyReceipt{},invalid("byte grant version")}
        var used bool
        if err:=tx.QueryRowContext(ctx,`SELECT EXISTS(SELECT 1 FROM byte_policy_results WHERE grant_id=? AND result_version=?)`,c.ID,versions[i]).Scan(&used);err!=nil{return BytePolicyReceipt{},err}
        if used{return BytePolicyReceipt{},ErrConflict}
        if missing {
            _,err=tx.ExecContext(ctx,`INSERT INTO byte_grants(grant_id,library_id,subject_id,scope_kind,asset_id,action,grant_version,removed,created_sequence,updated_sequence) VALUES(?,?,?,?,?,?,?,0,?,?)`,c.ID,s.library,value.SubjectID,value.Scope.Kind,nullableAsset(value.Scope),value.Action,versions[i],sequence,sequence)
        } else {
            err=changedOne(tx.ExecContext(ctx,`UPDATE byte_grants SET grant_version=?,removed=?,updated_sequence=? WHERE grant_id=? AND grant_version=? AND removed=?`,versions[i],removed,sequence,c.ID,prior.expected.Version,prior.expected.Removed))
        }
        if err!=nil{return BytePolicyReceipt{},err}
        touched[value.SubjectID]=true
        results=append(results,ByteGrantResult{c.ID,versions[i],removed})
    }
    for subjectID:=range touched {
        subject,err:=readSubject(ctx,tx,s.library,subjectID)
        if err!=nil{return BytePolicyReceipt{},err}
        if subject.AuthorizationGeneration==math.MaxInt64{return BytePolicyReceipt{},ErrConflict}
        if err:=changedOne(tx.ExecContext(ctx,`UPDATE policy_subjects SET authorization_generation=authorization_generation+1 WHERE subject_id=? AND authorization_generation=?`,subjectID,subject.AuthorizationGeneration));err!=nil{return BytePolicyReceipt{},err}
    }
    receipt:=BytePolicyReceipt{1,s.library,op.ID,sequence,digest,timestamp,results}
    receiptBytes,err:=CanonicalBytePolicyReceipt(receipt)
    if err!=nil{return BytePolicyReceipt{},err}
    var grants,events,canonical,reserved int64
    if err:=tx.QueryRowContext(ctx,`SELECT lifetime_grants,events,canonical_bytes,reserved_removals FROM byte_policy_quota WHERE singleton=1 AND library_id=?`,s.library).Scan(&grants,&events,&canonical,&reserved);err!=nil{return BytePolicyReceipt{},corrupt("byte policy quota")}
    nextReserve:=reserved+deltaReserve
    nextCanonical:=canonical+int64(len(request)+len(receiptBytes))
    if grants+added>16384 || events+1+nextReserve>131072 || nextReserve<0 || nextCanonical+3072*nextReserve>64<<20 {return BytePolicyReceipt{},ErrCollectionCapacity}
    if _,err:=tx.ExecContext(ctx,`UPDATE byte_policy_quota SET lifetime_grants=?,events=?,canonical_bytes=?,reserved_removals=? WHERE singleton=1`,grants+added,events+1,nextCanonical,nextReserve);err!=nil{return BytePolicyReceipt{},err}
    if _,err:=tx.ExecContext(ctx,`INSERT INTO byte_policy_events(sequence,library_id,operator_subject_id,operation_id,request,payload_sha256,receipt,recorded_unix_nano) VALUES(?,?,?,?,?,?,?,?)`,sequence,s.library,operator,op.ID,request,digest,receiptBytes,timestamp);err!=nil{return BytePolicyReceipt{},err}
    for _,r:=range results {if _,err:=tx.ExecContext(ctx,`INSERT INTO byte_policy_results(policy_sequence,grant_id,result_version,removed) VALUES(?,?,?,?)`,sequence,r.ID,r.Version,r.Removed);err!=nil{return BytePolicyReceipt{},err}}
    if err:=appendCollectionOrder(ctx,tx,"byte_policy_sequence",sequence);err!=nil{return BytePolicyReceipt{},err}
    return receipt,nil
}

func (o *LocalOwnerService) ApplyBytePolicy(ctx context.Context,input BytePolicyRequest) (BytePolicyReceipt,error) {
    if err:=o.enter();err!=nil{return BytePolicyReceipt{},err}
    defer o.store.leave()
    s:=o.store
    if !supportsDelivery(s.version){return BytePolicyReceipt{},ErrUpgradeRequired}
    data,_,err:=CanonicalBytePolicyRequest(input)
    if err!=nil{return BytePolicyReceipt{},err}
    op,err:=DecodeBytePolicyRequest(data)
    if err!=nil{return BytePolicyReceipt{},err}
    if op.LibraryID!=o.library{return BytePolicyReceipt{},invalid("byte policy library")}
    tx,err:=s.db.BeginTx(ctx,nil)
    if err!=nil{return BytePolicyReceipt{},err}
    defer tx.Rollback()
    if err:=s.checkVersion(ctx,tx);err!=nil{return BytePolicyReceipt{},err}
    prior,found,err:=bytePolicyPrior(ctx,tx,op,data)
    if err!=nil || found{return prior,err}
    authority,err:=readAuthority(ctx,tx,s.library)
    if err!=nil{return BytePolicyReceipt{},err}
    if authority.Incarnation!=op.ExpectedIncarnation || authority.Incarnation=="" {return BytePolicyReceipt{},ErrConflict}
    owner,err:=readSubject(ctx,tx,s.library,authority.OwnerSubjectID)
    if err!=nil{return BytePolicyReceipt{},err}
    if !owner.Value.Enabled || owner.Value.Kind!="person" {return BytePolicyReceipt{},ErrConflict}
    versions:=make([]string,len(op.Changes))
    for i:=range versions {versions[i],err=randomIdentity("bgv_");if err!=nil{return BytePolicyReceipt{},err}}
    receipt,err:=s.applyBytePolicyStructural(ctx,tx,op,authority.OwnerSubjectID,s.now().UnixNano(),versions)
    if err!=nil{return BytePolicyReceipt{},err}
    if err:=ctx.Err();err!=nil{return BytePolicyReceipt{},err}
    if err:=s.checkPaths();err!=nil{return BytePolicyReceipt{},err}
    if err:=tx.Commit();err!=nil{return BytePolicyReceipt{},fmt.Errorf("byte policy commit uncertain (operation %s): %w",op.ID,err)}
    return receipt,nil
}
func (o *LocalOwnerService) BytePolicyStatus(ctx context.Context,id string) (result BytePolicyReceipt,resultErr error) {
    if err:=o.enter();err!=nil{return result,err}
    defer o.store.leave()
    if !supportsDelivery(o.store.version){return result,ErrUpgradeRequired}
    if !validOperationID(id){return result,invalid("byte policy operation")}
    tx,done,err:=o.store.readSnapshot(ctx)
    if err!=nil{return result,err}
    defer func(){resultErr=errors.Join(resultErr,done())}()
    var request []byte
    if err:=tx.QueryRowContext(ctx,`SELECT request FROM byte_policy_events WHERE operation_id=?`,id).Scan(&request);errors.Is(err,sql.ErrNoRows){return result,ErrNotFound}else if err!=nil{return result,err}
    op,err:=DecodeBytePolicyRequest(request)
    if err!=nil{return result,corrupt("byte policy request")}
    result,_,err=bytePolicyPrior(ctx,tx,op,request)
    return result,err
}
