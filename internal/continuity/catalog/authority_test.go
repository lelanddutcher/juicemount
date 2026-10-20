package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
)

const ownerSubject SubjectID = "sub_11111111111111111111111111111111"
const readerSubject SubjectID = "sub_22222222222222222222222222222222"

func adminTestRequest(id, inc, kind string, value any) AdminRequest {
	b, _ := json.Marshal(value)
	return AdminRequest{1, testLibrary, id, inc, kind, b}
}
func authorityTestStore(t *testing.T) (*Store, *LocalOwnerService, AuthorityStatus, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "authority.sqlite")
	s, err := CreateAuthority(context.Background(), path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	o, err := s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	label := "owner"
	r, err := o.Bootstrap(context.Background(), adminTestRequest("bootstrap", "", "bootstrap", BootstrapRequest{ownerSubject, &label}))
	if err != nil {
		t.Fatal(err)
	}
	applyTest(t, s, initialOperation())
	applyTest(t, s, testOperation("asset-b", Change{CreateAsset: &CreateAsset{otherAsset, "B"}}))
	return s, o, AuthorityStatus{r.Result.Incarnation, ownerSubject}, path
}
func policyTest(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id string, changes ...PolicyChange) AdminReceipt {
	t.Helper()
	p := PolicyOperation{1, testLibrary, id, "local_owner", a.OwnerSubjectID, changes}
	r, err := o.ApplyPolicy(context.Background(), adminTestRequest(id, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func issueTest(t *testing.T, o *LocalOwnerService, a AuthorityStatus, id string, scope Scope, subject SubjectID) (IssuanceResult, AdminRequest) {
	t.Helper()
	op := adminTestRequest(id, a.Incarnation, "issue_credential", IssueCredentialRequest{subject, scope, []string{"assets.discover"}, o.store.now().Unix() + 3600})
	r, err := o.IssueCredential(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Bearer) != 102 {
		t.Fatal("first issue did not return exactly one bearer")
	}
	return r, op
}
func grantForTest(subject SubjectID, scope Scope) GrantValue {
	return GrantValue{subject, scope, []string{"assets.discover"}}
}
func checkTest(t *testing.T, s *Store, h *AuthHandle, asset string, want bool) {
	t.Helper()
	r, err := s.CheckAccess(context.Background(), h, AccessRequest{testLibrary, "assets.discover", continuity.AssetID(asset)})
	if err != nil || r.Allowed != want {
		t.Fatalf("current-grant access allowed=%v want=%v: %v", r.Allowed, want, err)
	}
}

func TestAuthorityCredentialScopeCeilingAndCurrentGrant(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}), NewPolicyChange("put_grant", PutGrant{"reader-grant", nil, grantForTest(readerSubject, Scope{"library", ""})}))
	r, _ := issueTest(t, o, a, "issue-a", Scope{"asset", testAsset}, readerSubject)
	h, err := s.Authenticate(context.Background(), testLibrary, r.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	checkTest(t, s, h, string(testAsset), true)
	checkTest(t, s, h, string(otherAsset), false)
	decision, err := s.CheckAccess(context.Background(), h, AccessRequest{testLibrary, "locations.read", testAsset})
	if err != nil || decision.Allowed {
		t.Fatalf("action ceiling widened: %v", err)
	}
	g, err := readGrant(context.Background(), s.db, testLibrary, "reader-grant")
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "remove-grant", NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()}))
	checkTest(t, s, h, string(testAsset), false) // compiled negative control targets this predicate.
	checkTest(t, s, h, "asset_33333333333333333333333333333333", false)
}

func TestAuthorityIssueRetryRecoveryAndRestart(t *testing.T) {
	s, o, a, path := authorityTestStore(t)
	r, op := issueTest(t, o, a, "issue", Scope{"library", ""}, ownerSubject)
	retry, err := o.IssueCredential(context.Background(), op)
	if err != nil || len(retry.Bearer) != 0 || retry.Receipt != r.Receipt || retry.Status != "active" {
		t.Fatalf("issuance retry: secret=%d status=%s err=%v", len(retry.Bearer), retry.Status, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenContext(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o, err = s.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := o.Status(context.Background())
	if err != nil || status != a {
		t.Fatalf("restart changed person/binding: %v", err)
	}
	h, err := s.Authenticate(context.Background(), testLibrary, r.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	recoverOp := adminTestRequest("recover", a.Incarnation, "recover", RecoverRequest{ownerSubject})
	recovery, err := o.Recover(context.Background(), recoverOp)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Result.Incarnation == a.Incarnation || recovery.Result.OwnerSubjectID != ownerSubject {
		t.Fatal("recovery identity")
	}
	if again, err := o.Recover(context.Background(), recoverOp); err != nil || again != recovery {
		t.Fatalf("recovery retry rotated twice: %v", err)
	}
	if _, err := s.CheckAccess(context.Background(), h, AccessRequest{testLibrary, "assets.discover", testAsset}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("recovery retained handle: %v", err)
	}
	retry, found, err := o.LookupIssuance(context.Background(), op)
	if err != nil || !found || retry.Status != "invalidated" || len(retry.Bearer) != 0 {
		t.Fatalf("historical issue status: %s %v", retry.Status, err)
	}
	next := adminTestRequest("recover-again", recovery.Result.Incarnation, "recover", RecoverRequest{ownerSubject})
	if _, err := o.Recover(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Recover(context.Background(), recoverOp); !errors.Is(err, ErrConflict) {
		t.Fatalf("ancient recovery claimed current authority: %v", err)
	}
}

func TestAuthorityExpectedFullValue(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	policyTest(t, o, a, "subject-grant", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}), NewPolicyChange("put_grant", PutGrant{"g", nil, grantForTest(readerSubject, Scope{"library", ""})}))
	sub, _ := readSubject(context.Background(), s.db, testLibrary, readerSubject)
	g, _ := readGrant(context.Background(), s.db, testLibrary, "g")
	for _, kind := range []string{"subject", "grant"} {
		t.Run(kind, func(t *testing.T) {
			var c PolicyChange
			if kind == "subject" {
				expected := sub.Expected()
				expected.Value.Label = "forged"
				c = NewPolicyChange("update_subject", UpdateSubject{readerSubject, expected, sub.Value})
			} else {
				expected := g.Expected()
				expected.Value.Scope = Scope{"asset", testAsset}
				c = NewPolicyChange("put_grant", PutGrant{g.ID, expected, g.Value})
			}
			p := PolicyOperation{1, testLibrary, "forged-" + kind, "local_owner", ownerSubject, []PolicyChange{c}}
			_, err := o.ApplyPolicy(context.Background(), adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("full-value CAS: %v", err)
			}
		})
	}
}

func TestAuthorityGrantABAAndLifetimeGuards(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	policyTest(t, o, a, "grant", NewPolicyChange("put_grant", PutGrant{"g", nil, grantForTest(ownerSubject, Scope{"library", ""})}))
	g, _ := readGrant(context.Background(), s.db, testLibrary, "g")
	policyTest(t, o, a, "remove", NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()}))
	removed, _ := readGrant(context.Background(), s.db, testLibrary, "g")
	policyTest(t, o, a, "reactivate", NewPolicyChange("put_grant", PutGrant{g.ID, removed.Expected(), g.Value}))
	p := PolicyOperation{1, testLibrary, "stale", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()})}}
	if _, err := o.ApplyPolicy(context.Background(), adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p})); !errors.Is(err, ErrConflict) {
		t.Fatalf("ABA accepted: %v", err)
	}
	for _, sqlText := range []string{`DELETE FROM policy_subjects`, `DELETE FROM policy_grants`, `UPDATE policy_subjects SET kind='service'`, `UPDATE policy_grants SET grant_id='replacement'`, `UPDATE policy_grants SET subject_id='sub_22222222222222222222222222222222'`} {
		if _, err := s.db.Exec(sqlText); err == nil {
			t.Fatalf("lifetime guard missing: %s", sqlText)
		}
	}
}

func TestAuthorityExpiryAndHandleBinding(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	now := time.Unix(1_800_000_000, 0)
	s.clock = func() time.Time { return now }
	r, op := issueTest(t, o, a, "clock", Scope{"library", ""}, ownerSubject)
	h, err := s.Authenticate(context.Background(), testLibrary, r.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(3600 * time.Second)
	if _, err := s.WhoAmI(context.Background(), h); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("exact expiry accepted: %v", err)
	}
	retry, err := o.IssueCredential(context.Background(), op)
	if err != nil || retry.Status != "expired" || len(retry.Bearer) > 0 {
		t.Fatalf("expiry retry: %v %s", err, retry.Status)
	}
	other, _, _, _ := authorityTestStore(t)
	for _, handle := range []*AuthHandle{nil, {}, h} {
		if _, err := other.CheckAccess(context.Background(), handle, AccessRequest{testLibrary, "assets.discover", testAsset}); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("unbound handle: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Authenticate(context.Background(), testLibrary, r.Bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("closed auth: %v", err)
	}
}

func TestAuthorityConcurrentIssuanceOneSecret(t *testing.T) {
	_, o, a, _ := authorityTestStore(t)
	op := adminTestRequest("same-issue", a.Incarnation, "issue_credential", IssueCredentialRequest{ownerSubject, Scope{"library", ""}, []string{"assets.discover"}, time.Now().Unix() + 3600})
	start := make(chan struct{})
	results := make(chan IssuanceResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r, e := o.IssueCredential(context.Background(), op)
			results <- r
			errs <- e
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	var receipts []AdminReceipt
	for r := range results {
		if len(r.Bearer) > 0 {
			count++
		}
		receipts = append(receipts, r.Receipt)
	}
	if count != 1 || len(receipts) != 2 || receipts[0] != receipts[1] {
		t.Fatal("concurrent retry did not return one secret and identical receipts")
	}
}

func TestAuthorityPortableRestoreInactiveAndSecretAbsent(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	issued, _ := issueTest(t, o, a, "issue", Scope{"library", ""}, ownerSubject)
	var stream bytes.Buffer
	if err := s.Export(context.Background(), testLibrary, &stream, portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	credential, _ := readCredential(context.Background(), s.db, testLibrary, issued.Receipt.Result.Selector)
	if bytes.Contains(stream.Bytes(), issued.Bearer) || bytes.Contains(stream.Bytes(), credential.verifier) || bytes.Contains(stream.Bytes(), []byte(issued.Receipt.Result.Selector)) {
		t.Fatal("operational credentials leaked into portable metadata")
	}
	path := filepath.Join(t.TempDir(), "restored.sqlite")
	if err := Restore(context.Background(), path, testLibrary, bytes.NewReader(stream.Bytes()), portabilityLimits()); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	owner, err := restored.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := owner.Status(context.Background())
	if err != nil || status != (AuthorityStatus{}) {
		t.Fatalf("import activated authority: %+v %v", status, err)
	}
	for _, table := range []string{"catalog_authority", "catalog_credentials", "catalog_admin_events", "credential_issuance_receipts"} {
		var count int
		if err := restored.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("imported operational table %s: %d %v", table, count, err)
		}
	}
	if _, err := restored.Authenticate(context.Background(), testLibrary, issued.Bearer); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("imported bearer activated")
	}
	if _, err := owner.Bootstrap(context.Background(), adminTestRequest("bind-imported", "", "bootstrap", BootstrapRequest{ownerSubject, nil})); err != nil {
		t.Fatal(err)
	}
}

func TestAuthorityPermissionAndSchemaOptIn(t *testing.T) {
	s, path := newTestStore(t)
	if _, err := s.LocalOwner(testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema2 authority: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeAuthority(context.Background(), path, testLibrary); err == nil {
		t.Fatal("unsafe mode admitted")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("unsafe upgrade mutated DB")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := UpgradeAuthority(context.Background(), path, testLibrary); err != nil {
		t.Fatal(err)
	}
	authority, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.Close()
	o, err := authority.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	status, err := o.Status(context.Background())
	if err != nil || status != (AuthorityStatus{}) {
		t.Fatal("upgrade bootstrapped")
	}
}

func TestAuthorityCanonicalRefusalRedactsInput(t *testing.T) {
	op := adminTestRequest("issue", "auth_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "issue_credential", IssueCredentialRequest{ownerSubject, Scope{"library", ""}, []string{"assets.discover"}, 1_800_000_000})
	b, _, err := CanonicalAdminRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{bytes.Replace(b, []byte(`"expires_unix":1800000000`), []byte(`"secret-unknown":"DO-NOT-ECHO"`), 1), append(b, []byte(`{}`)...), bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)} {
		_, err := DecodeAdminRequest(bad)
		if err == nil || strings.Contains(err.Error(), "DO-NOT-ECHO") {
			t.Fatalf("canonical refusal leaked input: %v", err)
		}
	}
	if strings.Contains(fmt.Sprintf("%+v", IssuanceResult{Bearer: []byte("DO-NOT-ECHO")}), "DO-NOT-ECHO") {
		t.Fatal("formatting leaked one-time secret")
	}
}
