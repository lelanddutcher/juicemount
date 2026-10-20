package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The private clock is reached after currentHandle's first credential SELECT,
// so this gate proves an admitted, pinned read snapshot without production hooks.
func authorityClockGate(s *Store, ctx context.Context) (<-chan struct{}, func()) {
	reached, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	s.clock = func() time.Time {
		if first.CompareAndSwap(false, true) {
			close(reached)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		return time.Now()
	}
	return reached, func() { once.Do(func() { close(release) }) }
}

func TestAuthoritySnapshotRevokeAndRecoveryInterleavings(t *testing.T) {
	for _, kind := range []string{"revoke", "recover"} {
		t.Run(kind, func(t *testing.T) {
			s, o, a, _ := authorityTestStore(t)
			policyTest(t, o, a, "allow", NewPolicyChange("put_grant", PutGrant{"owner-grant", nil, grantForTest(ownerSubject, Scope{"library", ""})}))
			issued, issue := issueTest(t, o, a, "issued", Scope{"library", ""}, ownerSubject)
			handle, err := s.Authenticate(context.Background(), testLibrary, issued.Bearer)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			reached, release := authorityClockGate(s, ctx)
			defer release()
			type accessResult struct {
				decision AccessDecision
				err      error
			}
			done := make(chan accessResult, 1)
			go func() {
				d, e := s.CheckAccess(ctx, handle, AccessRequest{testLibrary, "assets.discover", testAsset})
				done <- accessResult{d, e}
			}()
			select {
			case <-reached:
			case <-ctx.Done():
				release()
				<-done
				t.Fatal("access never reached its read snapshot")
			}
			if kind == "revoke" {
				_, err = o.RevokeCredential(ctx, adminTestRequest("revoke", a.Incarnation, "revoke_credential", RevokeCredentialRequest{issued.Receipt.Result.Selector, 1}))
			} else {
				_, err = o.Recover(ctx, adminTestRequest("recover", a.Incarnation, "recover", RecoverRequest{ownerSubject}))
			}
			release()
			old := <-done
			if err != nil || old.err != nil || !old.decision.Allowed {
				t.Fatalf("snapshot mutation=%v old-access=%v/%v", err, old.decision.Allowed, old.err)
			}
			if _, err := s.CheckAccess(ctx, handle, AccessRequest{testLibrary, "assets.discover", testAsset}); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("fresh access retained authority after %s: %v", kind, err)
			}
			retry, err := o.IssueCredential(ctx, issue)
			want := "revoked"
			if kind == "recover" {
				want = "invalidated"
			}
			if err != nil || retry.Status != want || len(retry.Bearer) != 0 {
				t.Fatalf("historical retry status=%s secret=%d: %v", retry.Status, len(retry.Bearer), err)
			}
		})
	}
}

func TestAuthorityCloseDrainsAdmittedAccess(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	policyTest(t, o, a, "grant", NewPolicyChange("put_grant", PutGrant{"g", nil, grantForTest(ownerSubject, Scope{"library", ""})}))
	issued, _ := issueTest(t, o, a, "issue", Scope{"library", ""}, ownerSubject)
	h, err := s.Authenticate(context.Background(), testLibrary, issued.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reached, release := authorityClockGate(s, ctx)
	defer release()
	accessDone := make(chan error, 1)
	go func() {
		d, e := s.CheckAccess(ctx, h, AccessRequest{testLibrary, "assets.discover", testAsset})
		if e == nil && !d.Allowed {
			e = errors.New("admitted access denied")
		}
		accessDone <- e
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		release()
		<-accessDone
		t.Fatal("access not admitted")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- s.Close() }()
	pending, early := false, false
	var closeErr error
	for !pending && !early && ctx.Err() == nil {
		if !s.mu.TryRLock() {
			pending = true
			break
		}
		s.mu.RUnlock()
		select {
		case closeErr = <-closeDone:
			early = true
		default:
			runtime.Gosched()
		}
	}
	release()
	accessErr := <-accessDone
	if !early {
		closeErr = <-closeDone
	}
	if !pending || early || accessErr != nil || closeErr != nil {
		t.Fatalf("drain pending=%v early=%v access=%v close=%v", pending, early, accessErr, closeErr)
	}
	if _, err := s.CheckAccess(ctx, h, AccessRequest{testLibrary, "assets.discover", testAsset}); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("closed handle: %v", err)
	}
	if _, err := o.Status(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed owner capability: %v", err)
	}
}

func TestAuthorityCompetingBootstrapOneBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	first, err := CreateAuthority(ctx, path, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	one, err := first.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	two, err := second.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		receipt AdminReceipt
		err     error
		subject SubjectID
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, owner := range []*LocalOwnerService{one, two} {
		id := ownerSubject
		if i == 1 {
			id = readerSubject
		}
		wg.Add(1)
		go func(owner *LocalOwnerService, subject SubjectID) {
			defer wg.Done()
			<-start
			label := "owner"
			r, e := owner.Bootstrap(ctx, adminTestRequest("bootstrap-"+string(subject), "", "bootstrap", BootstrapRequest{subject, &label}))
			results <- outcome{r, e, subject}
		}(owner, id)
	}
	close(start)
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	var winner SubjectID
	for r := range results {
		if r.err == nil {
			wins++
			winner = r.subject
		} else if errors.Is(r.err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(r.err)
		}
	}
	status, err := one.Status(ctx)
	if err != nil || wins != 1 || conflicts != 1 || status.OwnerSubjectID != winner {
		t.Fatalf("bootstrap winners=%d conflicts=%d status=%+v: %v", wins, conflicts, status, err)
	}
	for _, table := range []string{"policy_subjects", "policy_events", "catalog_admin_events", "catalog_authority"} {
		var count int
		if err := first.db.QueryRowContext(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("losing bootstrap left %s rows=%d: %v", table, count, err)
		}
	}
}

func TestAuthorityCurrentOwnerSubjectServiceAndRevocationGuards(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	ctx := context.Background()
	service := SubjectID("sub_33333333333333333333333333333333")
	policyTest(t, o, a, "subjects", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}), NewPolicyChange("create_subject", CreateSubject{service, SubjectValue{"service", "tool", true}}))
	owner, _ := readSubject(ctx, s.db, testLibrary, ownerSubject)
	for name, change := range map[string]PolicyChange{
		"disable-owner":    NewPolicyChange("update_subject", UpdateSubject{ownerSubject, owner.Expected(), SubjectValue{"person", "owner", false}}),
		"service-activity": NewPolicyChange("put_grant", PutGrant{"service-interest", nil, GrantValue{service, Scope{"library", ""}, []string{"activity.record"}}}),
	} {
		p := PolicyOperation{1, testLibrary, name, "local_owner", ownerSubject, []PolicyChange{change}}
		if _, err := o.ApplyPolicy(ctx, adminTestRequest(name, a.Incarnation, "apply_policy", ApplyPolicyRequest{p})); !errors.Is(err, ErrConflict) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	forged := PolicyOperation{1, testLibrary, "forged-operator", "local_owner", readerSubject, []PolicyChange{NewPolicyChange("put_grant", PutGrant{"forged", nil, grantForTest(readerSubject, Scope{"library", ""})})}}
	if _, err := o.ApplyPolicy(ctx, adminTestRequest(forged.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{forged})); !errors.Is(err, ErrConflict) {
		t.Fatalf("caller operator admitted: %v", err)
	}
	badIssue := adminTestRequest("service-issue", a.Incarnation, "issue_credential", IssueCredentialRequest{service, Scope{"library", ""}, []string{"activity.record"}, time.Now().Unix() + 3600})
	if _, err := o.IssueCredential(ctx, badIssue); !errors.Is(err, ErrConflict) {
		t.Fatalf("service activity ceiling: %v", err)
	}
	issued, issue := issueTest(t, o, a, "reader-issue", Scope{"library", ""}, readerSubject)
	h, err := s.Authenticate(ctx, testLibrary, issued.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	sub, _ := readSubject(ctx, s.db, testLibrary, readerSubject)
	policyTest(t, o, a, "disable-reader", NewPolicyChange("update_subject", UpdateSubject{readerSubject, sub.Expected(), SubjectValue{"person", "reader", false}}))
	if _, err := s.WhoAmI(ctx, h); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("disabled subject handle: %v", err)
	}
	retry, err := o.IssueCredential(ctx, issue)
	if err != nil || retry.Status != "subject_disabled" || len(retry.Bearer) > 0 {
		t.Fatalf("disabled retry: %s %v", retry.Status, err)
	}
	revoke := adminTestRequest("revoke-reader", a.Incarnation, "revoke_credential", RevokeCredentialRequest{issued.Receipt.Result.Selector, 1})
	r, err := o.RevokeCredential(ctx, revoke)
	if err != nil {
		t.Fatal(err)
	}
	again, err := o.RevokeCredential(ctx, revoke)
	if err != nil || again != r {
		t.Fatalf("revoke retry: %v", err)
	}
	retry, err = o.IssueCredential(ctx, issue)
	if err != nil || retry.Status != "revoked" {
		t.Fatalf("revoked retry status: %s %v", retry.Status, err)
	}
	if _, err := s.LocalOwner(otherLibrary); !errors.Is(err, ErrInvalid) {
		t.Fatalf("owner library fence: %v", err)
	}
}

func TestAuthorityCommitErrorReturnsNoSecretAndAtomicInspection(t *testing.T) {
	s, o, a, path := authorityTestStore(t)
	ctx := context.Background()
	// A connection-local deferred FK fault reaches the real commit error path
	// without changing persistent DDL, adding hooks, or exposing a secret.
	for _, statement := range []string{`CREATE TEMP TABLE authority_fault_parent(id INTEGER PRIMARY KEY)`, `CREATE TEMP TABLE authority_fault_child(id INTEGER REFERENCES authority_fault_parent(id) DEFERRABLE INITIALLY DEFERRED)`, `CREATE TEMP TRIGGER authority_fault AFTER INSERT ON main.catalog_admin_events BEGIN INSERT INTO authority_fault_child(id) VALUES(1); END`} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	op := adminTestRequest("uncertain-issue", a.Incarnation, "issue_credential", IssueCredentialRequest{ownerSubject, Scope{"library", ""}, []string{"assets.discover"}, time.Now().Unix() + 3600})
	r, err := o.IssueCredential(ctx, op)
	if err == nil || !strings.Contains(err.Error(), "commit outcome uncertain") || len(r.Bearer) != 0 {
		t.Fatalf("commit error returned a secret or wrong status: bytes=%d err=%v", len(r.Bearer), err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenContext(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var credentials, mappings, receipts int
	for _, query := range []struct {
		sql   string
		value *int
	}{{`SELECT count(*) FROM catalog_credentials`, &credentials}, {`SELECT count(*) FROM credential_issuance_receipts`, &mappings}, {`SELECT count(*) FROM catalog_admin_events WHERE operation_id='uncertain-issue'`, &receipts}} {
		if err := reopened.db.QueryRowContext(ctx, query.sql).Scan(query.value); err != nil {
			t.Fatal(err)
		}
	}
	if credentials != mappings || mappings != receipts || credentials < 0 || credentials > 1 {
		t.Fatalf("uncertain issuance partial state: %d/%d/%d", credentials, mappings, receipts)
	}
	owner, err := reopened.LocalOwner(testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	inspected, err := owner.IssuanceStatus(ctx, op.ID)
	if receipts == 0 && !errors.Is(err, ErrNotFound) || receipts == 1 && (err != nil || len(inspected.Bearer) > 0) {
		t.Fatalf("uncertain inspection receipts=%d secret=%d err=%v", receipts, len(inspected.Bearer), err)
	}
}
