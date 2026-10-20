package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

func preferencesTestStore(t *testing.T) (*Store, *LocalOwnerService, AuthorityStatus, *DiscoveryService, []byte, *atomic.Int64) {
	t.Helper()
	s, err := CreatePreferences(context.Background(), filepath.Join(t.TempDir(), "preferences.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	now := new(atomic.Int64)
	now.Store(1800000000)
	s.clock = func() time.Time { return time.Unix(now.Load(), 0) }
	o, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "owner"
	receipt, err := o.Bootstrap(context.Background(), adminTestRequest("bootstrap", "", "bootstrap", BootstrapRequest{ownerSubject, &label}))
	if err != nil {
		t.Fatal(err)
	}
	a := AuthorityStatus{receipt.Result.Incarnation, ownerSubject}
	applyTest(t, s, initialOperation())
	applyTest(t, s, testOperation("other", Change{CreateAsset: &CreateAsset{otherAsset, "B"}}))
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	now.Add(100)
	d := discoveryServiceTest(t, s)
	return s, o, a, d, bearer, now
}
func preferenceRequestTest(id, kind string, asset continuity.AssetID, token *string, now int64) PreferenceRequest {
	return PreferenceRequest{2, testLibrary, id, kind, asset, token, now + 600}
}
func recordPreferenceTest(t *testing.T, d *DiscoveryService, bearer []byte, r PreferenceRequest) preferenceReceipt {
	t.Helper()
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, r, &out)
	if err != nil || !result.Started {
		t.Fatalf("preference %s: %+v %v", r.Kind, result, err)
	}
	v, err := decodePreferenceReceipt(out.Bytes(), r)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
func preferenceStatusTest(t *testing.T, d *DiscoveryService, bearer []byte, asset continuity.AssetID) *string {
	t.Helper()
	var out bytes.Buffer
	result, err := d.PreferenceState(context.Background(), bearer, PreferenceStateRequest{2, testLibrary, asset}, &out)
	if err != nil || !result.Started {
		t.Fatalf("preference status: %+v %v", result, err)
	}
	var v preferenceState
	if err := json.Unmarshal(out.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != 2 || v.AssetID != asset {
		t.Fatalf("status DTO: %s", out.String())
	}
	return v.Token
}
func v2SearchTest(t *testing.T, d *DiscoveryService, bearer []byte, r DiscoveryRequest) discoveryCollection {
	t.Helper()
	r.Version = 2
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, r, &out)
	if err != nil || !result.Started {
		t.Fatalf("v2 query: %+v %v", result, err)
	}
	var c discoveryCollection
	if err := json.Unmarshal(out.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	if c.Version != 2 || c.Sort != "discovery2" {
		t.Fatalf("v2 envelope: %s", out.String())
	}
	discoveryNoFieldsTest(t, out.Bytes())
	return c
}

func TestPreferenceCanonicalForms(t *testing.T) {
	r := PreferenceRequest{2, testLibrary, "select_test_1", "select", testAsset, nil, 1800000700}
	_, b, err := canonicalPreferenceRequest(r)
	const expected = `{"version":2,"library_id":"lib_000102030405060708090a0b0c0d0e0f","operation_id":"select_test_1","kind":"select","asset_id":"asset_101112131415161718191a1b1c1d1e1f","expected_token":null,"expires_unix":1800000700}`
	if err != nil || string(b) != expected || len(b) != 212 || payloadDigest(b) != "f6510fbc2556a439f0ce8c918af7fefd343752831019a7a28d8ca692181a8323" {
		t.Fatalf("canonical request: %s %v", b, err)
	}
	token := "sel1_" + strings.Repeat("b", 64)
	receipt := preferenceReceipt{2, r.OperationID, "select", &token, "operation_result"}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"version":2,"operation_id":"select_test_1","kind":"select","selection_token":"sel1_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","receipt_scope":"operation_result"}` {
		t.Fatalf("receipt shape: %s", encoded)
	}
	if sum := sha256.Sum256(encoded); fmt.Sprintf("%x", sum) != "d987265cb15a7ebda19415cc4a42152c919c002fb2fafa33c88b65995ab78701" {
		t.Fatal("independent select receipt hash")
	}
	old := "sel1_" + strings.Repeat("a", 64)
	clear := PreferenceRequest{2, testLibrary, "clear_test_1", "clear", testAsset, &old, 1800000700}
	_, clearBytes, err := canonicalPreferenceRequest(clear)
	if err != nil || len(clearBytes) != 277 || payloadDigest(clearBytes) != "f3e5c651c9e974490bc78c9c5e25d69c83f49d33bfb13307eae340de73ef35f3" {
		t.Fatalf("independent clear request: %s %v", clearBytes, err)
	}
	clearReceipt, err := json.Marshal(preferenceReceipt{2, clear.OperationID, "clear", nil, "operation_result"})
	if err != nil {
		t.Fatal(err)
	}
	if sum := sha256.Sum256(clearReceipt); fmt.Sprintf("%x", sum) != "08c4b76b134ddcab1c3336c6741b775a40c00c0547939d0340dad319931cae22" {
		t.Fatal("independent clear receipt hash")
	}
	for _, bad := range [][]byte{append(append([]byte(nil), b...), byte(' ')), bytes.Replace(b, []byte(`"expected_token":null,`), nil, 1), bytes.Replace(b, []byte(`"kind":"select"`), []byte(`"kind":"select","kind":"select"`), 1)} {
		if _, err := decodePreferenceRequest(bad); err == nil {
			t.Fatalf("noncanonical request admitted: %s", bad)
		}
	}
}

func TestPreferenceRetryClearCASAndRestart(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	r := preferenceRequestTest("select", "select", testAsset, nil, now.Load())
	first := recordPreferenceTest(t, d, bearer, r)
	if first.Token == nil || preferenceStatusTest(t, d, bearer, testAsset) == nil {
		t.Fatal("selection token unavailable")
	}
	before, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	retry := recordPreferenceTest(t, d, bearer, r)
	after, _ := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if *retry.Token != *first.Token || before != after {
		t.Fatal("retry changed recency/token/counters")
	}
	changed := r
	changed.AssetID = otherAsset
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, changed, &out)
	if DiscoveryErrorCode(err) != "conflict" || result.Started {
		t.Fatalf("changed retry: %v", err)
	}
	second := recordPreferenceTest(t, d, bearer, preferenceRequestTest("again", "select", testAsset, nil, now.Load()))
	if *second.Token == *first.Token {
		t.Fatal("replacement reused token")
	}
	stale := preferenceRequestTest("stale-clear", "clear", testAsset, first.Token, now.Load())
	result, err = d.RecordPreference(context.Background(), bearer, stale, &out)
	if DiscoveryErrorCode(err) != "conflict" || result.Started {
		t.Fatalf("stale clear accepted: %v", err)
	}
	clear := preferenceRequestTest("clear", "clear", testAsset, second.Token, now.Load())
	recordPreferenceTest(t, d, bearer, clear)
	if preferenceStatusTest(t, d, bearer, testAsset) != nil {
		t.Fatal("clear retained active association")
	}
	recordPreferenceTest(t, d, bearer, clear)
	third := recordPreferenceTest(t, d, bearer, preferenceRequestTest("third", "select", testAsset, nil, now.Load()))
	historical := recordPreferenceTest(t, d, bearer, r)
	if *historical.Token != *first.Token || *preferenceStatusTest(t, d, bearer, testAsset) != *third.Token {
		t.Fatal("historical retry asserted new/current state")
	}
	path := s.path
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.clock = func() time.Time { return time.Unix(now.Load(), 0) }
	next := discoveryServiceTest(t, reopened)
	if got := preferenceStatusTest(t, next, bearer, testAsset); got == nil || *got != *third.Token {
		t.Fatal("restart lost token recovery")
	}
	_ = o
	_ = a
}

func TestPreferenceOwnCandidatesRankingAndVersionOwnership(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	discoveryGrantTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	other := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	var changes []Change
	for i := 1; i <= 1001; i++ {
		id := continuity.AssetID(fmt.Sprintf("asset_%032x", i))
		changes = append(changes, Change{CreateAsset: &CreateAsset{id, "match"}})
		if len(changes) == 250 || i == 1001 {
			applyTest(t, s, testOperation(fmt.Sprintf("many-%d", i), changes...))
			changes = nil
		}
	}
	r := discoveryRequestTest("search", "")
	r.PageSize = 1
	baseline := v2SearchTest(t, d, other, r)
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("select-high", "select", testAsset, nil, now.Load()))
	mine := v2SearchTest(t, d, bearer, r)
	if !bytes.Contains(mine.Items[0], []byte(testAsset)) || !bytes.Contains(mine.Items[0], []byte("declared_selection")) {
		t.Fatalf("own high-ID selection omitted: %s", mine.Items[0])
	}
	again := v2SearchTest(t, d, other, r)
	if !bytes.Equal(baseline.Items[0], again.Items[0]) || bytes.Contains(again.Items[0], []byte("declared_selection")) {
		t.Fatal("another person's activity affected ordering")
	}
	r.ContextAssetIDs = []continuity.AssetID{otherAsset}
	contextual := v2SearchTest(t, d, bearer, r)
	if !bytes.Contains(contextual.Items[0], []byte(otherAsset)) {
		t.Fatal("recency outranked explicit context")
	}
	if mine.Next == nil {
		t.Fatal("needs frozen pages")
	}
	token := *mine.Next
	wrong := discoveryPageRequest(token)
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, wrong, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started {
		t.Fatalf("v2 token accepted onv1: %v", err)
	}
	wrong.Version = 2
	_ = v2SearchTest(t, d, bearer, wrong)
	legacyRequest := discoveryRequestTest("search", "")
	legacyRequest.PageSize = 1
	legacy := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, legacyRequest))
	if bytes.Contains(legacy.Items[0], []byte("declared_selection")) {
		t.Fatal("v1 started using preferences")
	}
	wrong = discoveryPageRequest(*legacy.Next)
	wrong.Version = 2
	result, err = d.Query(context.Background(), bearer, wrong, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started {
		t.Fatalf("v1 token accepted onv2: %v", err)
	}
	_ = discoveryQueryTest(t, d, bearer, discoveryPageRequest(*legacy.Next))
}

func TestPreferenceFiniteRetryFloorAndExpiryPruning(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	r := preferenceRequestTest("once", "select", testAsset, nil, now.Load())
	recordPreferenceTest(t, d, bearer, r)
	var out bytes.Buffer
	now.Store(r.ExpiresUnix)
	result, err := d.RecordPreference(context.Background(), bearer, r, &out)
	if DiscoveryErrorCode(err) != "expired_request" || result.Started {
		t.Fatalf("exact expiry replay accepted: %v", err)
	}
	if err := o.PrunePreferences(context.Background()); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := s.db.QueryRow(`SELECT count(*) FROM preference_receipts`).Scan(&retained); err != nil || retained != 0 {
		t.Fatalf("expired receipt retained: %d %v", retained, err)
	}
	result, err = d.RecordPreference(context.Background(), bearer, r, &out)
	if DiscoveryErrorCode(err) != "expired_request" || result.Started {
		t.Fatalf("pruned request became new selection: %v", err)
	}
	now.Add(-1)
	fresh := preferenceRequestTest("clock-back", "select", otherAsset, nil, now.Load())
	result, err = d.RecordPreference(context.Background(), bearer, fresh, &out)
	if !errors.Is(err, errPreferenceClock) || result.Started {
		t.Fatalf("durable floor rollback accepted: %v", err)
	}
	path := s.path
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.clock = func() time.Time { return time.Unix(now.Load(), 0) }
	next := discoveryServiceTest(t, reopened)
	result, err = next.RecordPreference(context.Background(), bearer, fresh, &out)
	if !errors.Is(err, errPreferenceClock) || result.Started {
		t.Fatalf("restart forgot floor: %v", err)
	}
	now.Store(1800000100 + preferenceAge)
	owner, err := reopened.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	renewed := discoveryIssueTest(t, owner, a, "renewed", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	if preferenceStatusTest(t, next, renewed, testAsset) != nil {
		t.Fatal("expired preference revived")
	}
	if err := owner.PrunePreferences(context.Background()); err != nil {
		t.Fatal(err)
	}
	var entries, subjects int
	if err := reopened.db.QueryRow(`SELECT (SELECT count(*) FROM preferences),(SELECT count(*) FROM preference_subjects)`).Scan(&entries, &subjects); err != nil || entries != 0 || subjects != 1 {
		t.Fatalf("expiry erased generation or retained entry: %d %d %v", entries, subjects, err)
	}
}

func TestPreferenceCapacityCleanupAndPermanentGuards(t *testing.T) {
	s, o, _, d, bearer, now := preferencesTestStore(t)
	for i := 0; i < 128; i++ {
		recordPreferenceTest(t, d, bearer, preferenceRequestTest(fmt.Sprintf("op-%d", i), "select", testAsset, nil, now.Load()))
	}
	var out bytes.Buffer
	r := preferenceRequestTest("at-capacity", "select", otherAsset, nil, now.Load())
	result, err := d.RecordPreference(context.Background(), bearer, r, &out)
	if DiscoveryErrorCode(err) != "busy" || result.Started {
		t.Fatalf("unexpired receipt evicted: %v", err)
	}
	control, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || control.receipts != 128 {
		t.Fatalf("capacity counters: %+v %v", control, err)
	}
	for _, query := range []string{`DELETE FROM preference_subjects`, `DELETE FROM preference_control`, `UPDATE preference_subjects SET generation=1`, `UPDATE preference_control SET clock_floor_unix=0`, `DELETE FROM preference_receipts`} {
		if _, err := s.db.Exec(query); err == nil {
			t.Fatalf("operational guard missing: %s", query)
		}
	}
	now.Add(600)
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("after-expiry", "select", otherAsset, nil, now.Load()))
	control, err = readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || control.receipts != 1 || control.subjects != 1 || control.entries != 2 {
		t.Fatalf("refused admission stranded cleanup: %+v %v", control, err)
	}
	if err := o.PrunePreferences(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := checkPreferences(context.Background(), s.db, testLibrary); err != nil {
		t.Fatal(err)
	}
}

func TestPreferenceMetadataExportOmitsPrivateState(t *testing.T) {
	s, _, _, d, bearer, now := preferencesTestStore(t)
	limits := Limits{MaxBytes: 1 << 20, MaxEvents: 100, MaxProjectionRows: 100}
	var before, after bytes.Buffer
	if err := s.Export(context.Background(), testLibrary, &before, limits); err != nil {
		t.Fatal(err)
	}
	receipt := recordPreferenceTest(t, d, bearer, preferenceRequestTest("private-selection", "select", testAsset, nil, now.Load()))
	if err := s.Export(context.Background(), testLibrary, &after, limits); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.Bytes(), after.Bytes()) || bytes.Contains(after.Bytes(), []byte(*receipt.Token)) || bytes.Contains(after.Bytes(), []byte("private-selection")) {
		t.Fatal("preferences changed or entered portable2")
	}
	path := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(after.Bytes()), limits); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if restored.version != 3 {
		t.Fatalf("portable2 restored physical%d", restored.version)
	}
	owner, err := restored.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(context.Background())
	if err != nil || status.Incarnation != "" {
		t.Fatalf("import activated authority: %+v %v", status, err)
	}
	if _, err := restored.Authenticate(context.Background(), testLibrary, bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("imported credential active: %v", err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	restored, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	control, err := readPreferenceControl(context.Background(), restored.db, testLibrary)
	if err != nil || control.subjects != 0 || control.entries != 0 || control.receipts != 0 {
		t.Fatalf("imported private state: %+v %v", control, err)
	}
}

func TestPreferenceSchema3RefusesV2WithoutMutation(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	bearer := discoveryIssueTest(t, o, a, "reader", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	var out bytes.Buffer
	_, err := d.RecordPreference(context.Background(), bearer, preferenceRequestTest("select", "select", testAsset, nil, time.Now().Unix()), &out)
	if !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema3 mutation accepted: %v", err)
	}
	r := discoveryRequestTest("search", "")
	r.Version = 2
	_, err = d.Query(context.Background(), bearer, r, &out)
	if !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema3v2 query accepted: %v", err)
	}
	path := s.path
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	if err := UpgradePreferences(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	mode, err := os.Stat(path)
	if err != nil || mode.Mode().Perm()&0077 != 0 {
		t.Fatalf("upgrade private mode: %v %v", mode, err)
	}
}

func TestPreferencePermanentPersonAdmissionCap(t *testing.T) {
	s, o, a, d, _, now := preferencesTestStore(t)
	var changes []PolicyChange
	for i := 1; i <= 1025; i++ {
		subject := SubjectID(fmt.Sprintf("sub_%032x", i+100))
		changes = append(changes, NewPolicyChange("create_subject", CreateSubject{subject, SubjectValue{"person", "bounded fixture", true}}))
		if len(changes) == 250 || i == 1025 {
			policyTest(t, o, a, fmt.Sprintf("people-%d", i), changes...)
			changes = nil
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 1; i <= 1024; i++ {
		subject := SubjectID(fmt.Sprintf("sub_%032x", i+100))
		if _, err := tx.Exec(`INSERT INTO preference_subjects(subject_id,library_id,generation,entry_count,receipt_count) VALUES(?,?,1,0,0)`, subject, testLibrary); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	last := SubjectID(fmt.Sprintf("sub_%032x", 1125))
	discoveryGrantTest(t, o, a, "last", last, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "last", last, Scope{"library", ""}, discoveryAllActions)
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, preferenceRequestTest("full", "select", testAsset, nil, now.Load()), &out)
	if DiscoveryErrorCode(err) != "busy" || result.Started {
		t.Fatalf("permanent person cap ignored: %+v %v", result, err)
	}
	if preferenceStatusTest(t, d, bearer, testAsset) != nil {
		t.Fatal("refusal created selection")
	}
	c, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || c.subjects != 1024 || c.entries != 0 || c.receipts != 0 {
		t.Fatalf("read/refusal changed capped identities: %+v %v", c, err)
	}
	if err := checkPreferences(context.Background(), s.db, testLibrary); err != nil {
		t.Fatal(err)
	}
}

func TestPreferenceEntryCapAndConditionalClearFreesCapacity(t *testing.T) {
	s, _, _, d, bearer, now := preferencesTestStore(t)
	first := recordPreferenceTest(t, d, bearer, preferenceRequestTest("first", "select", testAsset, nil, now.Load()))
	var changes []Change
	for i := 1; i <= 256; i++ {
		changes = append(changes, Change{CreateAsset: &CreateAsset{continuity.AssetID(fmt.Sprintf("asset_%032x", i+100)), "bounded fixture"}})
	}
	applyTest(t, s, testOperation("capacity-assets", changes...))
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 1; i <= 255; i++ {
		if _, err := tx.Exec(`UPDATE preference_subjects SET generation=generation+1 WHERE subject_id=?`, ownerSubject); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`INSERT INTO preferences(subject_id,asset_id,selection_token,person_order,selected_unix,expires_unix) VALUES(?,?,?,?,?,?)`, ownerSubject, continuity.AssetID(fmt.Sprintf("asset_%032x", i+100)), fmt.Sprintf("sel1_%064x", i), i+1, now.Load(), now.Load()+preferenceAge); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	asset := continuity.AssetID(fmt.Sprintf("asset_%032x", 356))
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, preferenceRequestTest("full", "select", asset, nil, now.Load()), &out)
	if DiscoveryErrorCode(err) != "busy" || result.Started {
		t.Fatalf("person entry cap ignored: %+v %v", result, err)
	}
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("free-one", "clear", testAsset, first.Token, now.Load()))
	recordPreferenceTest(t, d, bearer, preferenceRequestTest("now-fits", "select", asset, nil, now.Load()))
	c, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || c.entries != 256 {
		t.Fatalf("clear capacity counters: %+v %v", c, err)
	}
}

func TestPreferenceCleanupCommitsBeforeRefusedAdmission(t *testing.T) {
	s, o, a, d, bearer, now := preferencesTestStore(t)
	for i := 0; i < 128; i++ {
		recordPreferenceTest(t, d, bearer, preferenceRequestTest(fmt.Sprintf("live-%d", i), "select", testAsset, nil, now.Load()))
	}
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	discoveryGrantTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	reader := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	expiring := preferenceRequestTest("short-retry", "select", otherAsset, nil, now.Load())
	expiring.ExpiresUnix = now.Load() + 60
	recordPreferenceTest(t, d, reader, expiring)
	before, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if err != nil {
		t.Fatal(err)
	}
	now.Add(60)
	var out bytes.Buffer
	result, err := d.RecordPreference(context.Background(), bearer, preferenceRequestTest("refused", "select", otherAsset, nil, now.Load()), &out)
	if DiscoveryErrorCode(err) != "busy" || result.Started || out.Len() != 0 {
		t.Fatalf("expected final live-receipt capacity refusal: %+v %v", result, err)
	}
	after, err := readPreferenceSubject(context.Background(), s.db, testLibrary, ownerSubject)
	if err != nil || before != after {
		t.Fatalf("refused operation changed caller generation/state: %+v %+v %v", before, after, err)
	}
	other, err := readPreferenceSubject(context.Background(), s.db, testLibrary, readerSubject)
	if err != nil || other.receipts != 0 || other.generation != 1 {
		t.Fatalf("committed cleanup lost after admission refusal: %+v %v", other, err)
	}
	c, err := readPreferenceControl(context.Background(), s.db, testLibrary)
	if err != nil || c.receipts != 128 || c.entries != 2 {
		t.Fatalf("refusal rolled cleanup counters back: %+v %v", c, err)
	}
	if _, err := readPreference(context.Background(), s.db, ownerSubject, otherAsset); !errors.Is(err, ErrNotFound) {
		t.Fatalf("refused operation created preference: %v", err)
	}
}
