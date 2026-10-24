package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

const lifecycleSourceID = "dsrc_33333333333333333333333333333333"

// Reuse the qualified catalog admission fixture. Its source is explicitly
// synthetic, not evidence of image verification. All subjects, grants,
// credentials, selections and terminal operations below use production APIs.
func lifecycleUpgradeTest(t *testing.T, f deliveryClockFixture) deliveryClockFixture {
	t.Helper()
	if err := f.store.Close(); err != nil { t.Fatal(err) }
	if err := UpgradeDeliveryLifecycle(context.Background(), f.path, testLibrary); err != nil { t.Fatal(err) }
	s, err := Open(f.path)
	if err != nil { t.Fatal(err) }
	s.clock = func() time.Time { return time.Unix(f.now.Load(),0) }
	t.Cleanup(func(){ if err := s.Close(); err != nil { t.Error(err) } })
	f.store = s
	return f
}
func lifecycleFixture(t *testing.T) (deliveryClockFixture, *LocalOwnerService, AuthorityStatus, DeliverySelection) {
	t.Helper()
	f := newDeliveryClockFixture(t,3600)
	_, selected := f.selectLatest(t,"lifecycle-initial",deliveryClockBase+600)
	f = lifecycleUpgradeTest(t,f)
	o, err := f.store.LocalOwner(testLibrary); if err != nil { t.Fatal(err) }
	a, err := o.Status(context.Background()); if err != nil { t.Fatal(err) }
	return f,o,a,selected
}
func lifecycleIssue(t *testing.T,o *LocalOwnerService,a AuthorityStatus,id string,subject SubjectID) []byte {
	t.Helper()
	r, err := o.IssueCredentialV3(context.Background(),AdminRequestV3{3,testLibrary,id,a.Incarnation,"issue_credential",collectionTxRaw(t,IssueCredentialRequestV3{subject,Scope{"asset",testAsset},[]string{"assets.discover","bytes.read","revisions.read"},o.store.now().Unix()+3600})})
	if err != nil || len(r.Bearer)!=102 { t.Fatalf("fresh lifecycle credential: %v",err) }
	return r.Bearer
}
func lifecycleCancel(id, selection string) DeliveryCancelRequest { return DeliveryCancelRequest{1,testLibrary,id,selection} }
func lifecycleDisable(id string,a AuthorityStatus,version int64) SourceDisableRequest { return SourceDisableRequest{1,testLibrary,id,a.Incarnation,lifecycleSourceID,version} }
func lifecycleQuota(t *testing.T,s *Store) (int64,int64,int64) {
	t.Helper()
	var source,selection,used int64
	if err := s.db.QueryRow(`SELECT source_slots,selection_slots,used_bytes FROM delivery_terminal_quota WHERE singleton=1`).Scan(&source,&selection,&used); err != nil { t.Fatal(err) }
	return source,selection,used
}

// Complete typed SQLite rows, including blob/text distinctions. Version7
// snapshots omit only the new ledger entry and new tables, so adoption can be
// compared exactly without deriving an expected state through the migration.
func lifecycleRows(t *testing.T,s *Store,version int) []byte {
	t.Helper()
	var tables []string
	for _, o := range expectedSchemaFor(version) { if o.Type=="table" { tables=append(tables,o.Name) } }
	sort.Strings(tables)
	var all []any
	for _, table := range tables {
		query := "SELECT * FROM "+table
		if table=="schema_migrations" { query+=fmt.Sprintf(" WHERE version<=%d",version) }
		rows,err:=s.db.Query(query); if err!=nil { t.Fatal(err) }
		columns,err:=rows.Columns(); if err!=nil { rows.Close();t.Fatal(err) }
		values:=[]string{}
		for rows.Next() {
			cells:=make([]any,len(columns));dest:=make([]any,len(cells));for i:=range cells { dest[i]=&cells[i] }
			if err:=rows.Scan(dest...);err!=nil { rows.Close();t.Fatal(err) }
			typed:=make([]string,len(cells))
			for i,v:=range cells { switch x:=v.(type) { case nil:typed[i]="null";case []byte:typed[i]="blob:"+hex.EncodeToString(x);case string:typed[i]="text:"+hex.EncodeToString([]byte(x));case int64:typed[i]=fmt.Sprintf("integer:%d",x);default:rows.Close();t.Fatalf("unexpected SQLite type %T",v) } }
			b,err:=json.Marshal(typed);if err!=nil { rows.Close();t.Fatal(err) };values=append(values,string(b))
		}
		if err:=errors.Join(rows.Err(),rows.Close());err!=nil { t.Fatal(err) };sort.Strings(values)
		all=append(all,table,columns,values)
	}
	b,err:=json.Marshal(all);if err!=nil { t.Fatal(err) };return b
}
func lifecycleExport(t *testing.T,s *Store) []byte {
	t.Helper();var out bytes.Buffer
	if err:=s.Export(context.Background(),testLibrary,&out,collectionWireLimits());err!=nil { t.Fatal(err) }
	return out.Bytes()
}

func TestDeliveryLifecycleExplicitAdoptionPreservesAllSevenRows(t *testing.T) {
	ctx:=context.Background()
	f:=newDeliveryClockFixture(t,3600)
	_,selected:=f.selectLatest(t,"lifecycle-adopt",deliveryClockBase+600)
	before:=lifecycleRows(t,f.store,7)
	wire:=lifecycleExport(t,f.store)
	if err:=f.store.Close();err!=nil { t.Fatal(err) }
	old,err:=Open(f.path);if err!=nil { t.Fatal(err) };if old.version!=7 { t.Fatal("Open promoted schema7") };old.Close()
	if err:=UpgradeDeliveryLifecycle(ctx,f.path,testLibrary);err!=nil { t.Fatal(err) }
	if err:=UpgradeDeliveryLifecycle(ctx,f.path,testLibrary);err!=nil { t.Fatalf("exact8 retry: %v",err) }
	s:=deliveryOpenTest(t,f.path)
	if s.version!=8 || !bytes.Equal(before,lifecycleRows(t,s,7)) { t.Fatal("adoption changed preexisting rows/receipt bytes/storage types/ledger") }
	if !bytes.Equal(wire,lifecycleExport(t,s)) { t.Fatal("operational adoption changed format5") }
	sources,selections,used:=lifecycleQuota(t,s)
	if sources!=1 || selections!=1 || used!=0 { t.Fatalf("adoption reserves: %d/%d/%d",sources,selections,used) }
	if _,err:=readSourceTerminal(ctx,s.db,testLibrary,lifecycleSourceID,1);err!=nil { t.Fatal(err) }
	if _,err:=readSelectionTerminal(ctx,s.db,testLibrary,selected.SelectionID,false);err!=nil { t.Fatal(err) }
	if err:=UpgradeDelivery(ctx,f.path,testLibrary);!errors.Is(err,ErrUpgradeRequired) { t.Fatalf("target7 silently adopted8: %v",err) }
	fresh,err:=CreateDeliveryLifecycle(ctx,filepath.Join(t.TempDir(),"new8.sqlite"),testLibrary);if err!=nil { t.Fatal(err) };defer fresh.Close()
	if fresh.version!=8 { t.Fatal("explicit creation did not produce8") }
	if err:=checkDeliveryLifecycleState(ctx,fresh.db,testLibrary);err!=nil { t.Fatal(err) }
}

func TestDeliveryLifecycleAdoptionLateFailureAndBadCounters(t *testing.T) {
	for _, name:=range []string{"late_ledger_fault","normal_count_mismatch"} {
		t.Run(name,func(t *testing.T){
			f:=newDeliveryClockFixture(t,3600)
			f.selectLatest(t,"lifecycle-migration-fault",deliveryClockBase+600)
			if name=="normal_count_mismatch" { if _,err:=f.store.db.Exec(`UPDATE delivery_quota SET source_versions=2 WHERE singleton=1`);err!=nil { t.Fatal(err) } }
			before:=lifecycleRows(t,f.store,7)
			if name=="late_ledger_fault" { if _,err:=f.store.db.Exec(`CREATE TEMP TRIGGER lifecycle_adoption_fault BEFORE INSERT ON main.schema_migrations WHEN NEW.version=8 BEGIN SELECT RAISE(ABORT,'lifecycle ledger fault'); END`);err!=nil { t.Fatal(err) } }
			tx,err:=f.store.db.BeginTx(context.Background(),nil);if err!=nil { t.Fatal(err) }
			err=initializeDeliveryLifecycle(context.Background(),tx,testLibrary)
			if e:=tx.Rollback();e!=nil { t.Fatal(e) }
			if name=="late_ledger_fault" && (err==nil || !strings.Contains(err.Error(),"lifecycle ledger fault")) { t.Fatalf("did not reach late fault: %v",err) }
			if name=="normal_count_mismatch" && !errors.Is(err,ErrFormat) { t.Fatalf("invalid counters adopted: %v",err) }
			if !bytes.Equal(before,lifecycleRows(t,f.store,7)) { t.Fatal("failed adoption changed existing state") }
			var version,tables,ledger int
			if err:=f.store.db.QueryRow(`PRAGMA user_version`).Scan(&version);err!=nil { t.Fatal(err) }
			if err:=f.store.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name IN('delivery_terminal_slots','delivery_terminal_quota')`).Scan(&tables);err!=nil { t.Fatal(err) }
			if err:=f.store.db.QueryRow(`SELECT count(*) FROM schema_migrations WHERE version=8`).Scan(&ledger);err!=nil { t.Fatal(err) }
			if version!=7 || tables!=0 || ledger!=0 { t.Fatalf("partial adoption retained: %d/%d/%d",version,tables,ledger) }
		})
	}
}

// Synthetic capacity setup only: clone the fixture's immutable source and
// selection rows in ONE transaction. Each cloned select request/digest is
// canonical and independently addressed. It does not measure physical capacity
// or claim4096 real image bindings. Public terminal paths are then exercised.
func lifecycleFillSevenCaps(t *testing.T,f deliveryClockFixture,selected DeliverySelection) {
	t.Helper();ctx:=context.Background()
	tx,err:=f.store.db.BeginTx(ctx,nil);if err!=nil { t.Fatal(err) };defer tx.Rollback()
	for version:=int64(2);version<=1024;version++ {
		if _,err:=tx.Exec(`INSERT INTO delivery_sources SELECT source_id,?,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,enabled,created_unix_nano FROM delivery_sources WHERE source_id=? AND source_version=1`,version,lifecycleSourceID);err!=nil { t.Fatal(err) }
	}
	for i:=1;i<4096;i++ {
		id:=fmt.Sprintf("dsel_%032x",i+10000);operation:=fmt.Sprintf("lifecycle-cap-%d",i)
		request,digest,err:=CanonicalDeliverySelectRequest(DeliverySelectRequest{1,testLibrary,operation,testAsset,"latest","",deliveryClockBase+600,3});if err!=nil { t.Fatal(err) }
		if _,err:=tx.Exec(`INSERT INTO delivery_selections SELECT ?,library_id,subject_id,?,?,?,asset_id,revision_id,source_id,source_version,credential_selector,credential_generation,authority_incarnation,subject_generation,content_size,content_sha256,expires_unix,byte_budget,canceled,created_unix_nano FROM delivery_selections WHERE selection_id=?`,id,operation,request,digest,selected.SelectionID);err!=nil { t.Fatal(err) }
	}
	if _,err:=tx.Exec(`UPDATE delivery_quota SET source_versions=1024,selections=4096,canonical_bytes=67108864 WHERE singleton=1`);err!=nil { t.Fatal(err) }
	if err:=tx.Commit();err!=nil { t.Fatal(err) }
}
func TestDeliveryLifecycleFullCapsStillPermitTerminalOperations(t *testing.T) {
	ctx:=context.Background()
	f:=newDeliveryClockFixture(t,3600);_,selected:=f.selectLatest(t,"lifecycle-full-selected",deliveryClockBase+600)
	lifecycleFillSevenCaps(t,f,selected)
	f=lifecycleUpgradeTest(t,f)
	o,err:=f.store.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) };a,err:=o.Status(ctx);if err!=nil { t.Fatal(err) }
	sources,selections,used:=lifecycleQuota(t,f.store)
	if sources!=1024 || selections!=4096 || used!=0 || (sources+selections)*TerminalReservationBytes!=15728640 { t.Fatal("full-cap reservations not guaranteed") }
	if _,err:=f.store.SelectDelivery(ctx,f.bearer,DeliverySelectRequest{1,testLibrary,"lifecycle-full-refusal",testAsset,"latest","",deliveryClockBase+600,3});!errors.Is(err,ErrLimit) { t.Fatalf("normal admission did not reach cap: %v",err) }
	cancel:=lifecycleCancel("lifecycle-full-cancel",selected.SelectionID)
	first,err:=f.store.CancelDelivery(ctx,f.bearer,cancel);if err!=nil { t.Fatal(err) }
	retry,err:=f.store.CancelDelivery(ctx,f.bearer,cancel);if err!=nil || retry.OriginalReceipt!=first.OriginalReceipt { t.Fatalf("full-cap exact cancel retry: %v",err) }
	disabled,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-full-disable",a,1));if err!=nil { t.Fatal(err) }
	if disabled.CurrentStatus.Enabled || disabled.CurrentStatus.Terminal!="committed" { t.Fatal("full-cap source tombstone not visible") }
	afterSources,afterSelections,afterUsed:=lifecycleQuota(t,f.store)
	if afterSources!=sources || afterSelections!=selections || afterUsed<=0 || afterUsed>2*TerminalReservationBytes { t.Fatalf("terminal admission recharged slots: %d/%d/%d",afterSources,afterSelections,afterUsed) }
	var normalBytes int64;if err:=f.store.db.QueryRow(`SELECT canonical_bytes FROM delivery_quota WHERE singleton=1`).Scan(&normalBytes);err!=nil || normalBytes!=64<<20 { t.Fatal("terminal operation charged normal quota") }
	if err:=checkDeliveryLifecycleState(ctx,f.store.db,testLibrary);err!=nil { t.Fatal(err) }
}

func TestDeliveryLifecycleFreshCredentialInspectionAndReceiptCustody(t *testing.T) {
	ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
	fresh:=lifecycleIssue(t,o,a,"lifecycle-fresh-reader",readerSubject)
	inspect:=DeliveryInspectRequest{1,testLibrary,selected.SelectionID}
	status,err:=f.store.InspectDelivery(ctx,fresh,inspect)
	if err!=nil || status.OriginalSelection!=selected || status.RecordState.Canceled || status.RecordState.Expired || status.RecordState.Terminal!="none" || status.RequesterDeliveryEligibility.Eligible || status.RequesterDeliveryEligibility.Reason!="selector_mismatch" { t.Fatalf("fresh selector confused durable pin with eligibility: %+v %v",status,err) }
	if _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);err!=nil { t.Fatalf("inspection made original pin inactive: %v",err) }
	if _,err:=f.store.AdmitDelivery(ctx,fresh,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("fresh selector acquired old byte pin: %v",err) }
	lookup,err:=f.store.LookupDelivery(ctx,fresh,DeliveryLookupRequest{1,testLibrary,"lifecycle-initial"})
	if err!=nil || lookup!=status { t.Fatalf("same-subject lookup: %+v %v",lookup,err) }
	_,second:=f.selectLatest(t,"lifecycle-second",deliveryClockBase+600)
	op:=lifecycleCancel("lifecycle-custody",selected.SelectionID)
	first,err:=f.store.CancelDelivery(ctx,fresh,op);if err!=nil { t.Fatal(err) }
	request,digest,err:=CanonicalDeliveryCancelRequest(op);if err!=nil { t.Fatal(err) }
	want:=TerminalReceipt{1,testLibrary,op.ID,"selection_cancel",digest,readerSubject,a.Incarnation,deliveryClockBase*1e9,"",0,selected.SelectionID}
	if first.OriginalReceipt!=want || first.CurrentStatus.OriginalSelection!=selected || !first.CurrentStatus.RecordState.Canceled || first.CurrentStatus.RecordState.Terminal!="committed" || first.CurrentStatus.RequesterDeliveryEligibility.Reason!="canceled" { t.Fatalf("cancel lost immutable pin/attribution: %+v",first) }
	receipt,_:=CanonicalTerminalReceipt(want)
	var storedRequest,storedReceipt []byte
	if err:=f.store.db.QueryRow(`SELECT request,receipt FROM delivery_terminal_slots WHERE selection_id=?`,selected.SelectionID).Scan(&storedRequest,&storedReceipt);err!=nil { t.Fatal(err) }
	if !bytes.Equal(storedRequest,request) || !bytes.Equal(storedReceipt,receipt) { t.Fatal("committed request/receipt bytes differ from original operation") }
	before:=lifecycleRows(t,f.store,8)
	retry,err:=f.store.CancelDelivery(ctx,f.bearer,op)
	if err!=nil || retry.OriginalReceipt!=want || !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatalf("exact retry changed durable custody/quota: %+v %v",retry,err) }
	if _,err:=f.store.CancelDelivery(ctx,fresh,lifecycleCancel("lifecycle-another-id",selected.SelectionID));!errors.Is(err,ErrConflict) { t.Fatalf("already terminal invented a new event: %v",err) }
	if _,err:=f.store.CancelDelivery(ctx,fresh,lifecycleCancel(op.ID,second.SelectionID));!errors.Is(err,ErrChangedRequest) { t.Fatalf("changed request reused actor namespace: %v",err) }
	if _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("canceled original pin admitted: %v",err) }
	if _,err:=f.store.AdmitDelivery(ctx,f.bearer,second.SelectionID);err!=nil { t.Fatalf("cancel escaped target: %v",err) }
	if !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatal("refused terminal retries mutated durable state") }
}

func TestDeliveryLifecycleHistoricalDisclosureRequiresCurrentRights(t *testing.T) {
	ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
	op:=lifecycleCancel("lifecycle-rights-cancel",selected.SelectionID)
	first,err:=f.store.CancelDelivery(ctx,f.bearer,op);if err!=nil { t.Fatal(err) }
	// Another currently authorized subject has the same exact asset rights,
	// but cannot disclose or cancel the reader's historical pin.
	const other SubjectID="sub_44444444444444444444444444444444"
	policyTest(t,o,a,"lifecycle-other-subject",NewPolicyChange("create_subject",CreateSubject{other,SubjectValue{"person","other reader",true}}),NewPolicyChange("put_grant",PutGrant{"lifecycle-other-discovery",nil,GrantValue{other,Scope{"asset",testAsset},[]string{"assets.discover","revisions.read"}}}))
	value:=ByteGrantValue{other,Scope{"asset",testAsset},"bytes.read"}
	if _,err:=o.ApplyBytePolicy(ctx,BytePolicyRequest{1,testLibrary,"lifecycle-other-bytes",a.Incarnation,[]ByteGrantChange{{"put","bgr_44444444444444444444444444444444",nil,&value}}});err!=nil { t.Fatal(err) }
	foreign:=lifecycleIssue(t,o,a,"lifecycle-other-credential",other)
	if _,err:=f.store.InspectDelivery(ctx,foreign,DeliveryInspectRequest{1,testLibrary,selected.SelectionID});!errors.Is(err,ErrNotFound) { t.Fatalf("foreign historical inspection: %v",err) }
	if _,err:=f.store.CancelDelivery(ctx,foreign,op);!errors.Is(err,ErrNotFound) { t.Fatalf("foreign historical cancel: %v",err) }
	if _,err:=f.store.LookupDelivery(ctx,foreign,DeliveryLookupRequest{1,testLibrary,"lifecycle-initial"});!errors.Is(err,ErrNotFound) { t.Fatalf("lookup escaped subject namespace: %v",err) }
	foreignSelection,err:=f.store.SelectDelivery(ctx,foreign,DeliverySelectRequest{1,testLibrary,"lifecycle-foreign-select",testAsset,"latest","",deliveryClockBase+600,3});if err!=nil { t.Fatal(err) }
	foreignCancel,err:=f.store.CancelDelivery(ctx,foreign,lifecycleCancel(op.ID,foreignSelection.SelectionID));if err!=nil || foreignCancel.OriginalReceipt.ActorSubjectID!=other { t.Fatalf("terminal operation ID escaped actor namespace: %+v %v",foreignCancel,err) }
	grant,err:=readByteGrant(ctx,f.store.db,string(testLibrary),"bgr_11111111111111111111111111111111");if err!=nil { t.Fatal(err) }
	if _,err:=o.ApplyBytePolicy(ctx,BytePolicyRequest{1,testLibrary,"lifecycle-drop-current-bytes",a.Incarnation,[]ByteGrantChange{{"remove","bgr_11111111111111111111111111111111",&grant.expected,nil}}});err!=nil { t.Fatal(err) }
	if _,err:=f.store.CancelDelivery(ctx,f.bearer,op);!errors.Is(err,ErrNotFound) { t.Fatalf("prior receipt bypassed current byte grant: %v",err) }
	if _,err:=f.store.InspectDelivery(ctx,f.bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID});!errors.Is(err,ErrNotFound) { t.Fatalf("inspection bypassed current byte grant: %v",err) }
	grant,err=readByteGrant(ctx,f.store.db,string(testLibrary),"bgr_11111111111111111111111111111111");if err!=nil { t.Fatal(err) }
	if _,err:=o.ApplyBytePolicy(ctx,BytePolicyRequest{1,testLibrary,"lifecycle-restore-current-bytes",a.Incarnation,[]ByteGrantChange{{"put","bgr_11111111111111111111111111111111",&grant.expected,&grant.expected.Value}}});err!=nil { t.Fatal(err) }
	fresh:=lifecycleIssue(t,o,a,"lifecycle-after-regrant",readerSubject)
	retry,err:=f.store.CancelDelivery(ctx,fresh,op)
	if err!=nil || retry.OriginalReceipt!=first.OriginalReceipt || !retry.CurrentStatus.RecordState.Canceled { t.Fatalf("current fresh credential could not inspect durable original cancellation: %+v %v",retry,err) }
	if _,err:=f.store.AdmitDelivery(ctx,fresh,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("regrant resurrected old byte pin: %v",err) }
	selector:="cred_"+string(fresh[5:37])
	if _,err:=o.RevokeCredential(ctx,adminTestRequest("lifecycle-revoke-fresh",a.Incarnation,"revoke_credential",RevokeCredentialRequest{selector,1}));err!=nil { t.Fatal(err) }
	if _,err:=f.store.CancelDelivery(ctx,fresh,op);!errors.Is(err,ErrUnauthenticated) { t.Fatalf("revoked exact retry disclosed receipt: %v",err) }
}

func TestDeliveryLifecycleSourceReceiptSurvivesRecoveryWithoutRecharging(t *testing.T) {
	ctx:=context.Background();f,o,a,_:=lifecycleFixture(t)
	var originalImage []byte;var originalEnabled bool
	if err:=f.store.db.QueryRow(`SELECT image_view,enabled FROM delivery_sources WHERE source_id=? AND source_version=1`,lifecycleSourceID).Scan(&originalImage,&originalEnabled);err!=nil { t.Fatal(err) }
	op:=lifecycleDisable("lifecycle-source-custody",a,1)
	first,err:=o.DisableDeliverySource(ctx,op);if err!=nil { t.Fatal(err) }
	_,digest,_:=CanonicalSourceDisableRequest(op)
	want:=TerminalReceipt{1,testLibrary,op.ID,"source_disable",digest,ownerSubject,a.Incarnation,deliveryClockBase*1e9,lifecycleSourceID,1,""}
	if first.OriginalReceipt!=want || first.CurrentStatus.Enabled || first.CurrentStatus.Eligibility!="disabled" || first.CurrentStatus.Terminal!="committed" { t.Fatalf("source terminal custody: %+v",first) }
	recovery,err:=o.Recover(ctx,adminTestRequest("lifecycle-source-recover",a.Incarnation,"recover",RecoverRequest{ownerSubject}));if err!=nil { t.Fatal(err) }
	before:=lifecycleRows(t,f.store,8)
	retry,err:=o.DisableDeliverySource(ctx,op)
	if err!=nil || retry.OriginalReceipt!=want || retry.CurrentStatus.Enabled || !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatalf("historical source receipt/retry changed: %+v %v",retry,err) }
	changed:=op;changed.ExpectedIncarnation=recovery.Result.Incarnation
	if _,err:=o.DisableDeliverySource(ctx,changed);!errors.Is(err,ErrChangedRequest) { t.Fatalf("same ID changed source request: %v",err) }
	changed.ID="lifecycle-source-second-operation"
	if _,err:=o.DisableDeliverySource(ctx,changed);!errors.Is(err,ErrConflict) { t.Fatalf("terminal source invented event: %v",err) }
	var image []byte;var enabled bool
	if err:=f.store.db.QueryRow(`SELECT image_view,enabled FROM delivery_sources WHERE source_id=? AND source_version=1`,lifecycleSourceID).Scan(&image,&enabled);err!=nil { t.Fatal(err) }
	if !enabled || enabled!=originalEnabled || !bytes.Equal(image,originalImage) { t.Fatal("source disable rewrote immutable source version") }
	// Controlled authority corruption is solely a fail-closed oracle: both a
	// current inspect and an exact historical terminal retry must revalidate.
	if _,err:=f.store.db.Exec(`UPDATE policy_subjects SET enabled=0 WHERE subject_id=?`,ownerSubject);err!=nil { t.Fatal(err) }
	if _,err:=o.DisableDeliverySource(ctx,op);!errors.Is(err,ErrFormat) || !strings.Contains(err.Error(),"authority owner") { t.Fatalf("disabled owner disclosed historical receipt: %v",err) }
	if _,err:=o.DeliverySourceStatus(ctx,SourceStatusRequest{1,testLibrary,lifecycleSourceID,1});!errors.Is(err,ErrFormat) || !strings.Contains(err.Error(),"authority owner") { t.Fatalf("disabled owner disclosed source status: %v",err) }
}

func TestDeliveryLifecycleLegacyFlagsHaveNoInventedCustody(t *testing.T) {
	ctx:=context.Background();f:=newDeliveryClockFixture(t,3600);_,selected:=f.selectLatest(t,"lifecycle-legacy-select",deliveryClockBase+600)
	if _,err:=f.store.db.Exec(`UPDATE delivery_selections SET canceled=1 WHERE selection_id=?`,selected.SelectionID);err!=nil { t.Fatal(err) }
	if _,err:=f.store.db.Exec(`INSERT INTO delivery_sources SELECT source_id,2,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,0,created_unix_nano FROM delivery_sources WHERE source_id=? AND source_version=1`,lifecycleSourceID);err!=nil { t.Fatal(err) }
	if _,err:=f.store.db.Exec(`UPDATE delivery_source_heads SET source_version=2 WHERE source_id=?`,lifecycleSourceID);err!=nil { t.Fatal(err) }
	if _,err:=f.store.db.Exec(`UPDATE delivery_quota SET source_versions=2 WHERE singleton=1`);err!=nil { t.Fatal(err) }
	f=lifecycleUpgradeTest(t,f);o,err:=f.store.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) };a,err:=o.Status(ctx);if err!=nil { t.Fatal(err) }
	status,err:=o.DeliverySourceStatus(ctx,SourceStatusRequest{1,testLibrary,lifecycleSourceID,2})
	if err!=nil || status.Enabled || status.Terminal!="legacy" || status.Eligibility!="disabled" { t.Fatalf("legacy source status: %+v %v",status,err) }
	inspection,err:=f.store.InspectDelivery(ctx,f.bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID})
	if err!=nil || !inspection.RecordState.Canceled || inspection.RecordState.Terminal!="legacy" || inspection.OriginalSelection!=selected { t.Fatalf("legacy selection status: %+v %v",inspection,err) }
	before:=lifecycleRows(t,f.store,8)
	if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-no-fake-source-event",a,2));!errors.Is(err,ErrConflict) { t.Fatalf("legacy source fabricated custody: %v",err) }
	if _,err:=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-no-fake-cancel-event",selected.SelectionID));!errors.Is(err,ErrConflict) { t.Fatalf("legacy canceled fabricated custody: %v",err) }
	if !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatal("legacy terminal refusals mutated state") }
	var attributed int
	if err:=f.store.db.QueryRow(`SELECT count(*) FROM delivery_terminal_slots WHERE legacy_terminal=1 AND (operation_id IS NOT NULL OR actor_subject_id IS NOT NULL OR recorded_unix_nano IS NOT NULL OR request IS NOT NULL OR receipt IS NOT NULL)`).Scan(&attributed);err!=nil || attributed!=0 { t.Fatal("adoption invented legacy historical attribution") }
	if _,_,used:=lifecycleQuota(t,f.store);used!=0 { t.Fatal("legacy adoption charged nonexistent receipts") }
}

func TestDeliveryLifecycleAtomicReservationsAndTerminalLateFailures(t *testing.T) {
	for _,kind:=range []string{"selection_reservation","source_reservation","terminal_quota","canceled_bit","source_terminal_quota"} {
		t.Run(kind,func(t *testing.T){
			ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
			before:=lifecycleRows(t,f.store,8)
			var fault string
			switch kind {
			case "selection_reservation":fault=`CREATE TEMP TRIGGER lifecycle_late_fault BEFORE UPDATE ON main.delivery_quota WHEN NEW.selections>OLD.selections BEGIN SELECT RAISE(ABORT,'lifecycle late normal quota'); END`
			case "source_reservation":fault=`CREATE TEMP TRIGGER lifecycle_late_fault BEFORE UPDATE ON main.delivery_quota WHEN NEW.source_versions>OLD.source_versions BEGIN SELECT RAISE(ABORT,'lifecycle late normal quota'); END`
			case "canceled_bit":fault=`CREATE TEMP TRIGGER lifecycle_late_fault BEFORE UPDATE ON main.delivery_selections WHEN NEW.canceled=1 BEGIN SELECT RAISE(ABORT,'lifecycle late canceled bit'); END`
			default:fault=`CREATE TEMP TRIGGER lifecycle_late_fault BEFORE UPDATE ON main.delivery_terminal_quota WHEN NEW.used_bytes>OLD.used_bytes BEGIN SELECT RAISE(ABORT,'lifecycle late terminal quota'); END`
			}
			if _,err:=f.store.db.Exec(fault);err!=nil { t.Fatal(err) }
			var err error
			switch kind {
			case "selection_reservation":_,err=f.store.SelectDelivery(ctx,f.bearer,DeliverySelectRequest{1,testLibrary,"lifecycle-reservation-retry",testAsset,"latest","",deliveryClockBase+600,3})
			case "source_reservation":
				// This is the source-insert structural boundary; real bind I/O is
				// covered by the service/executable fixture, not simulated here.
				tx,e:=f.store.db.BeginTx(ctx,nil);if e!=nil { t.Fatal(e) }
				_,err=tx.Exec(`INSERT INTO delivery_sources SELECT source_id,2,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,enabled,created_unix_nano FROM delivery_sources WHERE source_id=? AND source_version=1`,lifecycleSourceID)
				if err==nil { _,err=tx.Exec(`UPDATE delivery_quota SET source_versions=source_versions+1 WHERE singleton=1`) }
				if e:=tx.Rollback();e!=nil { t.Fatal(e) }
			case "source_terminal_quota":_,err=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-terminal-retry",a,1))
			default:_,err=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-terminal-retry",selected.SelectionID))
			}
			if err==nil || !strings.Contains(err.Error(),"lifecycle late ") { t.Fatalf("intended late failure not reached: %v",err) }
			if !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatal("late failure retained a parent, reservation, event, canceled bit or quota") }
			if _,err:=f.store.db.Exec(`DROP TRIGGER lifecycle_late_fault`);err!=nil { t.Fatal(err) }
			switch kind {
			case "selection_reservation":
				_,created:=f.selectLatest(t,"lifecycle-reservation-retry",deliveryClockBase+600)
				if _,err:=readSelectionTerminal(ctx,f.store.db,testLibrary,created.SelectionID,false);err!=nil { t.Fatal(err) }
				sources,selections,used:=lifecycleQuota(t,f.store);if sources!=1 || selections!=2 || used!=0 { t.Fatal("public admission failed to reserve exactly one slot") }
			case "source_reservation":
				tx,err:=f.store.db.BeginTx(ctx,nil);if err!=nil { t.Fatal(err) };defer tx.Rollback()
				if _,err:=tx.Exec(`INSERT INTO delivery_sources SELECT source_id,2,library_id,asset_id,revision_id,content_size,content_sha256,authority_incarnation,route_id,route_digest,reader_leaf_sha256,image_view,enabled,created_unix_nano FROM delivery_sources WHERE source_id=? AND source_version=1`,lifecycleSourceID);err!=nil { t.Fatal(err) }
				if _,err:=tx.Exec(`UPDATE delivery_quota SET source_versions=source_versions+1 WHERE singleton=1`);err!=nil { t.Fatal(err) }
				if err:=tx.Commit();err!=nil { t.Fatal(err) }
				if _,err:=readSourceTerminal(ctx,f.store.db,testLibrary,lifecycleSourceID,2);err!=nil { t.Fatal(err) }
				sources,selections,used:=lifecycleQuota(t,f.store);if sources!=2 || selections!=1 || used!=0 { t.Fatal("source insertion failed to reserve exactly one slot") }
			case "source_terminal_quota":if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-terminal-retry",a,1));err!=nil { t.Fatalf("same source operation could not retry after rollback: %v",err) }
			default:if _,err:=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-terminal-retry",selected.SelectionID));err!=nil { t.Fatalf("same cancel operation could not retry after rollback: %v",err) }
			}
			if err:=checkDeliveryLifecycleState(ctx,f.store.db,testLibrary);err!=nil { t.Fatal(err) }
		})
	}
}

func lifecycleRefuseTxn(t *testing.T,s *Store,fragment string,change func(*sql.Tx)error) {
	t.Helper();before:=lifecycleRows(t,s,8)
	tx,err:=s.db.BeginTx(context.Background(),nil);if err!=nil { t.Fatal(err) }
	err=change(tx)
	if err==nil { err=tx.Commit() } else if e:=tx.Rollback();e!=nil { t.Fatal(e) }
	if err==nil || !strings.Contains(err.Error(),fragment) { t.Fatalf("intended %q refusal not reached: %v",fragment,err) }
	if !bytes.Equal(before,lifecycleRows(t,s,8)) { t.Fatal("refused mutation retained incomplete terminal state") }
}
func TestDeliveryLifecycleTerminalCouplingAndImmutableCustody(t *testing.T) {
	ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
	op:=lifecycleCancel("lifecycle-pair",selected.SelectionID);request,digest,_:=CanonicalDeliveryCancelRequest(op)
	r:=TerminalReceipt{1,testLibrary,op.ID,"selection_cancel",digest,readerSubject,a.Incarnation,deliveryClockBase*1e9,"",0,selected.SelectionID}
	lifecycleRefuseTxn(t,f.store,"FOREIGN KEY constraint failed",func(tx *sql.Tx)error{ _,err:=tx.Exec(`UPDATE delivery_selections SET canceled=1 WHERE selection_id=?`,selected.SelectionID);return err })
	lifecycleRefuseTxn(t,f.store,"FOREIGN KEY constraint failed",func(tx *sql.Tx)error{ return fillTerminal(ctx,tx,r,request) })
	wrong:=r;wrong.ActorSubjectID=ownerSubject
	lifecycleRefuseTxn(t,f.store,"immutable terminal custody",func(tx *sql.Tx)error{ return fillTerminal(ctx,tx,wrong,request) })
	// NULL is not a present timestamp. Explicit SQL checks the DDL, rather
	// than relying on the typed receipt encoder to reject it first.
	receipt,_:=CanonicalTerminalReceipt(r)
	lifecycleRefuseTxn(t,f.store,"CHECK constraint failed",func(tx *sql.Tx)error{
		_,err:=tx.Exec(`UPDATE delivery_terminal_slots SET terminal_bit=1,operation_id=?,actor_subject_id=?,authority_incarnation=?,recorded_unix_nano=NULL,request=?,payload_sha256=?,receipt=? WHERE selection_id=?`,r.OperationID,r.ActorSubjectID,r.AuthorityIncarnation,request,r.PayloadSHA256,receipt,r.SelectionID);return err
	})
	if _,err:=f.store.CancelDelivery(ctx,f.bearer,op);err!=nil { t.Fatal(err) }
	for _,statement:=range []string{
		`UPDATE delivery_terminal_slots SET receipt=x'7b7d' WHERE selection_id='`+selected.SelectionID+`'`,
		`UPDATE delivery_terminal_slots SET terminal_bit=0 WHERE selection_id='`+selected.SelectionID+`'`,
		`DELETE FROM delivery_terminal_slots WHERE selection_id='`+selected.SelectionID+`'`,
		`UPDATE delivery_sources SET enabled=0 WHERE source_id='`+lifecycleSourceID+`'`,
	} {
		fragment:="immutable terminal custody";if strings.HasPrefix(statement,"DELETE") { fragment="terminal custody retained" };if strings.HasPrefix(statement,"UPDATE delivery_sources") { fragment="immutable source version" }
		lifecycleRefuseTxn(t,f.store,fragment,func(tx *sql.Tx)error{ _,err:=tx.Exec(statement);return err })
	}
	if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-pair-source",a,1));err!=nil { t.Fatal(err) }
	if err:=checkDeliveryLifecycleState(ctx,f.store.db,testLibrary);err!=nil { t.Fatal(err) }
}

func lifecycleReinstallTrigger(t *testing.T,s *Store,name string) {
	t.Helper();for _,object:=range deliveryLifecycleSchema() { if object.Type=="trigger" && object.Name==name { if _,err:=s.db.Exec(object.SQL);err!=nil { t.Fatal(err) };return } };t.Fatalf("unknown lifecycle trigger %s",name)
}
func TestDeliveryLifecycleCorruptTerminalStateNeverAdmits(t *testing.T) {
	for _,kind:=range []string{"missing_slot","unpaired_canceled","unpaired_event","receipt_binding","foreign_actor"} {
		t.Run(kind,func(t *testing.T){
			ctx:=context.Background();f,_,a,selected:=lifecycleFixture(t)
			op:=lifecycleCancel("lifecycle-corruption",selected.SelectionID);request,digest,_:=CanonicalDeliveryCancelRequest(op)
			r:=TerminalReceipt{1,testLibrary,op.ID,"selection_cancel",digest,readerSubject,a.Incarnation,deliveryClockBase*1e9,"",0,selected.SelectionID}
			fragment:=""
			switch kind {
			case "missing_slot":
				if _,err:=f.store.db.Exec(`DROP TRIGGER delivery_terminal_slots_no_delete`);err!=nil { t.Fatal(err) }
				if _,err:=f.store.db.Exec(`DELETE FROM delivery_terminal_slots WHERE selection_id=?`,selected.SelectionID);err!=nil { t.Fatal(err) };lifecycleReinstallTrigger(t,f.store,"delivery_terminal_slots_no_delete");fragment="missing terminal reservation"
			case "unpaired_canceled","unpaired_event":
				// Deliberately corrupt this disposable database outside FK
				// enforcement; production commits of each half were refused above.
				if _,err:=f.store.db.Exec(`PRAGMA foreign_keys=OFF`);err!=nil { t.Fatal(err) }
				if kind=="unpaired_canceled" { if _,err:=f.store.db.Exec(`UPDATE delivery_selections SET canceled=1 WHERE selection_id=?`,selected.SelectionID);err!=nil { t.Fatal(err) } } else {
					tx,err:=f.store.db.BeginTx(ctx,nil);if err!=nil { t.Fatal(err) };if err:=fillTerminal(ctx,tx,r,request);err!=nil { tx.Rollback();t.Fatal(err) };if err:=tx.Commit();err!=nil { t.Fatal(err) }
				}
				if _,err:=f.store.db.Exec(`PRAGMA foreign_keys=ON`);err!=nil { t.Fatal(err) };fragment="selection terminal coupling"
			case "receipt_binding","foreign_actor":
				if _,err:=f.store.CancelDelivery(ctx,f.bearer,op);err!=nil { t.Fatal(err) }
				if _,err:=f.store.db.Exec(`DROP TRIGGER delivery_terminal_slots_fill`);err!=nil { t.Fatal(err) }
				if kind=="receipt_binding" { if _,err:=f.store.db.Exec(`UPDATE delivery_terminal_slots SET receipt=? WHERE selection_id=?`,[]byte(`{}`),selected.SelectionID);err!=nil { t.Fatal(err) };fragment="terminal receipt binding" } else {
					r.ActorSubjectID=ownerSubject;receipt,_:=CanonicalTerminalReceipt(r)
					if _,err:=f.store.db.Exec(`UPDATE delivery_terminal_slots SET actor_subject_id=?,receipt=? WHERE selection_id=?`,ownerSubject,receipt,selected.SelectionID);err!=nil { t.Fatal(err) };fragment="terminal selection actor"
				}
				lifecycleReinstallTrigger(t,f.store,"delivery_terminal_slots_fill")
			}
			before:=lifecycleRows(t,f.store,8)
			for _,attempt:=range []func()error{
				func()error{ _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);return err },
				func()error{ _,err:=f.store.InspectDelivery(ctx,f.bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID});return err },
				func()error{ _,err:=f.store.CancelDelivery(ctx,f.bearer,op);return err },
			} { if err:=attempt();!errors.Is(err,ErrFormat) || !strings.Contains(err.Error(),fragment) { t.Fatalf("corrupt target did not refuse at %q: %v",fragment,err) } }
			if !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatal("corruption refusal changed committed prefix") }
			if err:=checkDeliveryLifecycleState(ctx,f.store.db,testLibrary);!errors.Is(err,ErrFormat) { t.Fatalf("corrupt full state accepted: %v",err) }
			if err:=f.store.Close();err!=nil { t.Fatal(err) };opened,err:=Open(f.path);if opened!=nil { opened.Close() };if !errors.Is(err,ErrFormat) { t.Fatalf("Open accepted corruption: %v",err) }
		})
	}
}

func TestDeliveryLifecycleCommittedCutPrecedesSeparateStoreAdmission(t *testing.T) {
	for _,kind:=range []string{"selection_cancel","source_disable"} {
		t.Run(kind,func(t *testing.T){
			ctx:=context.Background();f,_,_,selected:=lifecycleFixture(t)
			prepared,ready,attempt:=clockPrepared(t,f,selected.SelectionID)
			other:=deliveryOpenTest(t,f.path);other.clock=func()time.Time{return time.Unix(f.now.Load(),0)}
			if kind=="selection_cancel" { if _,err:=other.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-other-store-cut",selected.SelectionID));err!=nil { t.Fatal(err) } } else {
				o,err:=other.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) };a,err:=o.Status(ctx);if err!=nil { t.Fatal(err) }
				if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-other-store-cut",a,1));err!=nil { t.Fatal(err) }
			}
			if _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("committed %s bypassed byte admission: %v",kind,err) }
			if _,err:=f.store.AdmitDeliveryChunk(ctx,f.bearer,selected.SelectionID,0);!errors.Is(err,ErrNotFound) { t.Fatalf("committed %s bypassed chunk fence: %v",kind,err) }
			if _,err:=f.store.PublishMaterialization(ctx,f.bearer,prepared);!errors.Is(err,ErrNotFound) { t.Fatalf("committed %s bypassed final exposure fence: %v",kind,err) }
			if _,err:=os.Lstat(ready);!os.IsNotExist(err) { t.Fatalf("cut exposed ready output: %v",err) }
			if err:=prepared.Revalidate();err!=nil { t.Fatalf("denial discarded owned prepared payload: %v",err) }
			var state string;if err:=f.store.db.QueryRow(`SELECT state FROM delivery_materializations WHERE attempt_id=?`,attempt.ID).Scan(&state);err!=nil || state!="PREPARED" { t.Fatalf("cut changed retained reconciliation state: %s %v",state,err) }
		})
	}
}
func TestDeliveryLifecycleTerminalAfterPublicationPreservesCompletedOutput(t *testing.T) {
	for _,kind:=range []string{"selection_cancel","source_disable"} {
		t.Run(kind,func(t *testing.T){
			ctx:=context.Background();f,_,_,selected:=lifecycleFixture(t)
			prepared,ready,_:=clockPrepared(t,f,selected.SelectionID)
			published,err:=f.store.PublishMaterialization(ctx,f.bearer,prepared);if err!=nil || published.State!="COMPLETED" { t.Fatalf("publication positive control: %+v %v",published,err) }
			other:=deliveryOpenTest(t,f.path);other.clock=func()time.Time{return time.Unix(f.now.Load(),0)}
			if kind=="selection_cancel" { if _,err:=other.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-after-publication",selected.SelectionID));err!=nil { t.Fatal(err) } } else {
				o,err:=other.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) };a,err:=o.Status(ctx);if err!=nil { t.Fatal(err) }
				if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-after-publication",a,1));err!=nil { t.Fatal(err) }
			}
			data,err:=os.ReadFile(ready);if err!=nil || string(data)!="abc" { t.Fatalf("terminal operation discarded/replaced prior completed output: %q %v",data,err) }
			var state string;if err:=f.store.db.QueryRow(`SELECT state FROM delivery_materializations WHERE attempt_id=?`,published.ID).Scan(&state);err!=nil || state!="COMPLETED" { t.Fatalf("terminal operation rewrote prior completion: %s %v",state,err) }
			if _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("completed output revived byte pin: %v",err) }
		})
	}
}

func TestDeliveryLifecycleExportKeepsExactFormatFiveAndRestoresInactiveSeven(t *testing.T) {
	ctx:=context.Background();f:=newDeliveryClockFixture(t,3600);_,selected:=f.selectLatest(t,"lifecycle-portable-select",deliveryClockBase+600)
	wire:=lifecycleExport(t,f.store)
	if !bytes.HasPrefix(wire,[]byte(streamMagic5)) { t.Fatal("schema7 baseline is not format5") }
	f=lifecycleUpgradeTest(t,f);o,err:=f.store.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) };a,err:=o.Status(ctx);if err!=nil { t.Fatal(err) }
	if _,err:=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-portable-cancel",selected.SelectionID));err!=nil { t.Fatal(err) }
	if _,err:=o.DisableDeliverySource(ctx,lifecycleDisable("lifecycle-portable-disable",a,1));err!=nil { t.Fatal(err) }
	if !bytes.Equal(wire,lifecycleExport(t,f.store)) { t.Fatal("physical8 terminal custody or operational state changed portable format5 bytes") }
	frames:=savedQueryPortableFrames(t,wire,len(streamMagic5));var header deliveryStreamHeader
	if err:=json.Unmarshal(frames[0].payload,&header);err!=nil || header.SchemaVersion!=7 || header.Version!=5 { t.Fatalf("physical8 leaked into portable header: %+v %v",header,err) }
	path:=filepath.Join(t.TempDir(),"inactive7.sqlite")
	if err:=Restore(ctx,path,testLibrary,bytes.NewReader(wire),collectionWireLimits());err!=nil { t.Fatal(err) }
	restored:=deliveryOpenTest(t,path);restored.clock=func()time.Time{return time.Unix(f.now.Load(),0)}
	if restored.version!=7 || !bytes.Equal(wire,lifecycleExport(t,restored)) { t.Fatal("portable format5 changed bytes or physical restore target") }
	if err:=validateInactiveDeliveryImport(ctx,restored.db);err!=nil { t.Fatal(err) }
	var terminalTables int;if err:=restored.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name IN('delivery_terminal_slots','delivery_terminal_quota')`).Scan(&terminalTables);err!=nil || terminalTables!=0 { t.Fatal("restore retained operational terminal custody") }
	if _,err:=restored.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-restored-no-cancel",selected.SelectionID));!errors.Is(err,ErrUpgradeRequired) { t.Fatalf("inactive7 gained8 terminal authority: %v",err) }
	if _,err:=restored.Authenticate(ctx,testLibrary,f.bearer);!errors.Is(err,ErrUnauthenticated) { t.Fatalf("restore revived original credential: %v",err) }
	rOwner,err:=restored.LocalOwner(testLibrary);if err!=nil { t.Fatal(err) }
	boot,err:=rOwner.Bootstrap(ctx,adminTestRequest("lifecycle-restored-rebind","","bootstrap",BootstrapRequest{ownerSubject,nil}));if err!=nil { t.Fatal(err) }
	fresh:=lifecycleIssue(t,rOwner,AuthorityStatus{boot.Result.Incarnation,ownerSubject},"lifecycle-restored-fresh",readerSubject)
	if _,err:=restored.SelectDelivery(ctx,fresh,DeliverySelectRequest{1,testLibrary,"lifecycle-restored-no-source",testAsset,"latest","",deliveryClockBase+600,3});!errors.Is(err,ErrNotFound) { t.Fatalf("restored policy manufactured operational byte rights: %v",err) }
	if _,err:=restored.LookupDelivery(ctx,fresh,DeliveryLookupRequest{1,testLibrary,selected.OperationID});!errors.Is(err,ErrNotFound) { t.Fatalf("restore retained operational selections: %v",err) }
}

func TestDeliveryLifecycleExplicitUpgradeRefusesEarlierTargets(t *testing.T) {
	ctx:=context.Background()
	for version:=2;version<=6;version++ {
		t.Run(fmt.Sprintf("schema%d",version),func(t *testing.T){
			path:=filepath.Join(t.TempDir(),"earlier.sqlite")
			s,err:=openStoreVersionContext(ctx,path,testLibrary,true,true,version);if err!=nil { t.Fatal(err) }
			before:=lifecycleRows(t,s,version);if err:=s.Close();err!=nil { t.Fatal(err) }
			if err:=UpgradeDeliveryLifecycle(ctx,path,testLibrary);!errors.Is(err,ErrUpgradeRequired) { t.Fatalf("lifecycle silently skipped explicit earlier upgrades: %v",err) }
			s=deliveryOpenTest(t,path)
			if s.version!=version || !bytes.Equal(before,lifecycleRows(t,s,version)) { t.Fatal("refused lifecycle upgrade changed older storage") }
		})
	}
}

func TestDeliveryLifecycleStrictCanonicalRequests(t *testing.T) {
	// Frozen literal wire forms are independent of the production marshaler.
	valid:=[]byte(`{"version":1,"library_id":"lib_11111111111111111111111111111111","operation_id":"cancel-wire","selection_id":"dsel_22222222222222222222222222222222"}`)
	decoded,err:=DecodeDeliveryCancelRequest(valid)
	if err!=nil || decoded.ID!="cancel-wire" { t.Fatalf("canonical cancellation literal: %+v %v",decoded,err) }
	for _,bad:=range [][]byte{
		bytes.Replace(valid,[]byte(`"version":1`),[]byte(`"version":1,"version":1`),1),
		bytes.Replace(valid,[]byte(`"version":1`),[]byte(`"version":null`),1),
		append(append([]byte{},valid...),[]byte(" {}")...),
		append([]byte(" "),valid...),
		bytes.Replace(valid,[]byte(`"selection_id":`),[]byte(`"unknown":0,"selection_id":`),1),
		bytes.Repeat([]byte("x"),MaxTerminalRequestBytes+1),
	} { if _,err:=DecodeDeliveryCancelRequest(bad);!errors.Is(err,ErrInvalid) { t.Fatalf("noncanonical request accepted: %v",err) } }
	if _,err:=DecodeTerminalReceipt(bytes.Repeat([]byte("x"),MaxTerminalReceiptBytes+1));!errors.Is(err,ErrInvalid) { t.Fatalf("unbounded receipt accepted: %v",err) }
}

func TestDeliveryLifecycleHistoricalRetryChecksMetadataSubjectAndCeiling(t *testing.T) {
	for _,kind:=range []string{"metadata_removed","subject_disabled","scope_ceiling","action_ceiling"} {
		t.Run(kind,func(t *testing.T){
			ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
			op:=lifecycleCancel("lifecycle-historical-rights",selected.SelectionID)
			if _,err:=f.store.CancelDelivery(ctx,f.bearer,op);err!=nil { t.Fatal(err) }
			bearer:=f.bearer;want:=ErrNotFound
			switch kind {
			case "metadata_removed":
				grant,err:=readGrant(ctx,f.store.db,testLibrary,"clock-discovery");if err!=nil { t.Fatal(err) }
				policyTest(t,o,a,"lifecycle-remove-metadata",NewPolicyChange("remove_grant",RemoveGrant{grant.ID,grant.Expected()}))
			case "subject_disabled":
				sub,err:=readSubject(ctx,f.store.db,testLibrary,readerSubject);if err!=nil { t.Fatal(err) };disabled:=sub.Value;disabled.Enabled=false
				policyTest(t,o,a,"lifecycle-disable-reader",NewPolicyChange("update_subject",UpdateSubject{readerSubject,sub.Expected(),disabled}));want=ErrUnauthenticated
			case "scope_ceiling","action_ceiling":
				request:=IssueCredentialRequestV3{readerSubject,Scope{"asset",testAsset},[]string{"assets.discover","bytes.read","revisions.read"},deliveryClockBase+3600}
				if kind=="scope_ceiling" { applyTest(t,f.store,testOperation("lifecycle-other-asset",Change{CreateAsset:&CreateAsset{otherAsset,"other"}}));request.Scope=Scope{"asset",otherAsset} } else { request.Actions=[]string{"assets.discover","revisions.read"} }
				issued,err:=o.IssueCredentialV3(ctx,AdminRequestV3{3,testLibrary,"lifecycle-limited-credential",a.Incarnation,"issue_credential",collectionTxRaw(t,request)});if err!=nil { t.Fatal(err) };bearer=issued.Bearer
			}
			before:=lifecycleRows(t,f.store,8)
			for _,attempt:=range []func()error{
				func()error{ _,err:=f.store.CancelDelivery(ctx,bearer,op);return err },
				func()error{ _,err:=f.store.InspectDelivery(ctx,bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID});return err },
				func()error{ _,err:=f.store.LookupDelivery(ctx,bearer,DeliveryLookupRequest{1,testLibrary,selected.OperationID});return err },
			} { if err:=attempt();!errors.Is(err,want) { t.Fatalf("historical receipt bypassed current %s: %v",kind,err) } }
			if !bytes.Equal(before,lifecycleRows(t,f.store,8)) { t.Fatal("unauthorized historical request mutated state") }
		})
	}
}

func TestDeliveryLifecycleIneligibleAndExpiredOwnPinRemainsCancelable(t *testing.T) {
	ctx:=context.Background();f,o,a,selected:=lifecycleFixture(t)
	sub,err:=readSubject(ctx,f.store.db,testLibrary,readerSubject);if err!=nil { t.Fatal(err) };renamed:=sub.Value;renamed.Label="reader after selection"
	policyTest(t,o,a,"lifecycle-reader-generation",NewPolicyChange("update_subject",UpdateSubject{readerSubject,sub.Expected(),renamed}))
	status,err:=f.store.InspectDelivery(ctx,f.bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID})
	if err!=nil || status.RecordState.Canceled || status.RecordState.Expired || status.RequesterDeliveryEligibility.Reason!="pin_changed" { t.Fatalf("generation change confused historical inspection: %+v %v",status,err) }
	if _,err:=f.store.AdmitDelivery(ctx,f.bearer,selected.SelectionID);!errors.Is(err,ErrNotFound) { t.Fatalf("generation change did not close byte pin: %v",err) }
	f.now.Store(selected.ExpiresUnix)
	status,err=f.store.InspectDelivery(ctx,f.bearer,DeliveryInspectRequest{1,testLibrary,selected.SelectionID})
	if err!=nil || !status.RecordState.Expired || status.RequesterDeliveryEligibility.Reason!="expired" { t.Fatalf("expired own pin not inspectable: %+v %v",status,err) }
	cancel,err:=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-expired-cancel",selected.SelectionID))
	if err!=nil || !cancel.CurrentStatus.RecordState.Expired || !cancel.CurrentStatus.RecordState.Canceled || cancel.OriginalReceipt.RecordedUnixNano!=selected.ExpiresUnix*1e9 { t.Fatalf("expired own pin could not be durably canceled: %+v %v",cancel,err) }
	f.now.Store(deliveryClockBase)
	retry,err:=f.store.CancelDelivery(ctx,f.bearer,lifecycleCancel("lifecycle-expired-cancel",selected.SelectionID))
	if err!=nil || retry.OriginalReceipt!=cancel.OriginalReceipt || !retry.CurrentStatus.RecordState.Expired || !retry.CurrentStatus.RecordState.Canceled { t.Fatalf("clock rollback lost terminal receipt/expiry: %+v %v",retry,err) }
}
