package catalog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type savedQueryLifecycleAnswer struct {
	result DisclosureResult
	err    error
	body   []byte
}

// Inspect the actual durable parent after a publication failure. A failure is
// not evidence of rollback; this helper also binds the retained minimal receipt.
func savedQueryLifecycleCommitted(t *testing.T, s *Store, op SavedQueryRequest, prior CollectionVersion, name string) []byte {
	t.Helper()
	ctx := context.Background()
	def, err := readSavedQueryDefinition(ctx, s.db, op.CollectionID)
	if err != nil || def.name != name || def.version == prior {
		t.Fatalf("postcommit definition missing: %+v %v", def, err)
	}
	var sequence, count int64
	if err := s.db.QueryRowContext(ctx, `SELECT sequence FROM collection_events WHERE library_id=? AND actor_subject_id=? AND operation_id=?`, testLibrary, ownerSubject, op.ID).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM collection_events WHERE collection_id=?`, op.CollectionID).Scan(&count); err != nil || count != 2 {
		t.Fatalf("expected exactly create and committed rename: %d %v", count, err)
	}
	record, receipt, child, err := readSavedQueryContentRecord(ctx, s.db, testLibrary, sequence)
	if err != nil || child != 0 || record.ActorSubjectID != ownerSubject || record.ResultVersion != def.version {
		t.Fatalf("retained parent/receipt mismatch: %+v child=%d %v", record, child, err)
	}
	want, _, err := CanonicalSavedQueryRequest(op)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := CanonicalSavedQueryRequest(record.Request)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("committed request changed: %v", err)
	}
	return append([]byte(nil), receipt...)
}

func savedQueryLifecycleRetry(t *testing.T, d *DiscoveryService, bearer []byte, op SavedQueryRequest, retained []byte) {
	t.Helper()
	before := savedQueryTestSnapshot(t, d.store, true)
	var out bytes.Buffer
	result, err := d.MutateSavedQuery(context.Background(), bearer, op, &out)
	if err != nil || !result.Started || !bytes.Equal(out.Bytes(), retained) {
		t.Fatalf("exact postcommit retry: %+v %v %s", result, err, out.Bytes())
	}
	if !bytes.Equal(before, savedQueryTestSnapshot(t, d.store, true)) {
		t.Fatal("postcommit retry changed durable state")
	}
}

func savedQueryLifecycleIdle(t *testing.T, d *DiscoveryService) {
	t.Helper()
	d.mu.Lock()
	active, charge := len(d.active), d.bytes
	windows, tokens, building := len(d.windows), len(d.tokens), len(d.building)
	d.mu.Unlock()
	if active != 0 || charge != discoveryAdmissionBytes || len(d.sql) != 0 || windows != 0 || tokens != 0 || building != 0 {
		t.Fatalf("mutation owner retained after return: active=%d bytes=%d sql=%d windows=%d tokens=%d building=%d", active, charge, len(d.sql), windows, tokens, building)
	}
}

func TestSavedQueryPostcommitCredentialAndGenerationFences(t *testing.T) {
	for _, scenario := range []string{"expired", "revoked", "benign_generation"} {
		t.Run(scenario, func(t *testing.T) {
			s, o, a, d, bearer, _ := savedQueryTestStore(t)
			ctx := context.Background()
			var now atomic.Int64
			now.Store(s.now().Unix())
			s.clock = func() time.Time { return time.Unix(now.Load(), 0) }
			create := savedQueryTestCreate(t, 300)
			created := savedQueryTestCall(t, d, bearer, create)
			credential, err := readCredentialVersion(ctx, s.db, testLibrary, "cred_"+string(bearer[5:37]), s.version)
			if err != nil {
				t.Fatal(err)
			}
			beforeSubject, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
			if err != nil {
				t.Fatal(err)
			}
			beforeDefinition, err := readSavedQueryDefinition(ctx, s.db, create.CollectionID)
			if err != nil {
				t.Fatal(err)
			}
			beforeGrant, err := readCollectionGrant(ctx, s.db, "cgr_0000000000000000000000000000012c")
			if err != nil {
				t.Fatal(err)
			}
			updatedSubject := beforeSubject.Value
			updatedSubject.Label = "Owner renamed without rights change"
			policy := PolicyOperation{1, testLibrary, "saved-benign-generation", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("update_subject", UpdateSubject{ownerSubject, beforeSubject.Expected(), updatedSubject})}}
			policyRequest := adminTestRequest(policy.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{policy})
			revokeRequest := adminTestRequest("saved-revoke-at-fence", a.Incarnation, "revoke_credential", RevokeCredentialRequest{credential.selector, credential.generation})
			calls := 0
			var hookErr error
			d.beforeFence = func(context.Context) error {
				calls++
				switch scenario {
				case "expired":
					now.Store(credential.expires)
				case "revoked":
					_, hookErr = o.RevokeCredential(ctx, revokeRequest)
				case "benign_generation":
					_, hookErr = o.ApplyPolicy(ctx, policyRequest)
				}
				return hookErr
			}
			op := savedQueryTestEdit(t, 301, create.CollectionID, created.ResultVersion, "rename", RenameCollection{"Committed before authority fence"})
			var out bytes.Buffer
			result, mutationErr := d.MutateSavedQuery(ctx, bearer, op, &out)
			d.beforeFence = nil
			if hookErr != nil || calls != 1 {
				t.Fatalf("fence stimulus failed: calls=%d %v", calls, hookErr)
			}
			want := ErrUnauthenticated
			if scenario == "benign_generation" {
				want = errDiscoveryRestart
			}
			if !errors.Is(mutationErr, want) || result.Started || out.Len() != 0 {
				t.Fatalf("postcommit %s disclosed output: %+v %v %s", scenario, result, mutationErr, out.Bytes())
			}
			retained := savedQueryLifecycleCommitted(t, s, op, created.ResultVersion, "Committed before authority fence")
			savedQueryLifecycleIdle(t, d)
			if scenario == "benign_generation" {
				afterSubject, err := readSubject(ctx, s.db, testLibrary, ownerSubject)
				if err != nil || afterSubject.AuthorizationGeneration != beforeSubject.AuthorizationGeneration+1 || afterSubject.Value.Kind != beforeSubject.Value.Kind || afterSubject.Value.Enabled != beforeSubject.Value.Enabled {
					t.Fatalf("benign generation stimulus: %+v %v", afterSubject, err)
				}
				afterGrant, err := readCollectionGrant(ctx, s.db, beforeGrant.id)
				if err != nil || afterGrant.expected.Version != beforeGrant.expected.Version || afterGrant.expected.Removed != beforeGrant.expected.Removed || !equalCollectionGrantValue(afterGrant.expected.Value, beforeGrant.expected.Value) {
					t.Fatalf("benign stimulus changed collection grant: %+v %v", afterGrant, err)
				}
				afterDefinition, err := readSavedQueryDefinition(ctx, s.db, create.CollectionID)
				if err != nil || afterDefinition.generation != beforeDefinition.generation {
					t.Fatalf("benign stimulus changed collection policy generation: %v", err)
				}
			} else {
				// The original expired/revoked credential remains denied. Recovery
				// uses a fresh credential for the SAME actor and exact operation.
				savedQueryTestDenied(t, d, bearer, op, ErrUnauthenticated)
				fresh, _ := collectionTxIssue(t, o, a, "saved-lifecycle-fresh", ownerSubject, collectionTxActions)
				bearer = fresh.Bearer
			}
			savedQueryLifecycleRetry(t, d, bearer, op, retained)
			savedQueryLifecycleIdle(t, d)
		})
	}
}

func TestSavedQueryCancellationAfterCommitJoinsOriginalMutation(t *testing.T) {
	s, _, _, d, bearer, _ := savedQueryTestStore(t)
	create := savedQueryTestCreate(t, 310)
	created := savedQueryTestCall(t, d, bearer, create)
	op := savedQueryTestEdit(t, 311, create.CollectionID, created.ResultVersion, "rename", RenameCollection{"Committed before cancellation"})
	ctx, cancel := context.WithCancel(context.Background())
	reached := make(chan struct{})
	d.beforeFence = func(call context.Context) error {
		close(reached)
		<-call.Done()
		return call.Err()
	}
	done := make(chan savedQueryLifecycleAnswer, 1)
	var original sync.WaitGroup
	original.Add(1)
	// Registered before launch: every failure path cancels and joins the same
	// original goroutine, before fixture cleanup can close the service/store.
	defer func() { cancel(); original.Wait(); d.beforeFence = nil }()
	go func() {
		defer original.Done()
		var out bytes.Buffer
		result, err := d.MutateSavedQuery(ctx, bearer, op, &out)
		done <- savedQueryLifecycleAnswer{result, err, append([]byte(nil), out.Bytes()...)}
	}()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	select {
	case <-reached:
	case got := <-done:
		t.Fatalf("mutation returned before cancellation gate: %+v", got)
	case <-guard.C:
		t.Fatal("mutation did not reach its postcommit gate")
	}
	retained := savedQueryLifecycleCommitted(t, s, op, created.ResultVersion, "Committed before cancellation")
	cancel()
	original.Wait()
	got := <-done
	d.beforeFence = nil
	if !errors.Is(got.err, context.Canceled) || got.result.Started || len(got.body) != 0 {
		t.Fatalf("postcommit cancellation result: %+v", got)
	}
	savedQueryLifecycleIdle(t, d)
	savedQueryLifecycleRetry(t, d, bearer, op, retained)
}

func TestSavedQueryCloseDrainsOriginalHeldOutput(t *testing.T) {
	s, _, _, d, bearer, _ := savedQueryTestStore(t)
	create := savedQueryTestCreate(t, 320)
	created := savedQueryTestCall(t, d, bearer, create)
	op := savedQueryTestEdit(t, 321, create.CollectionID, created.ResultVersion, "rename", RenameCollection{"Committed before held output"})
	writer := collectionHeldWriter{make(chan struct{}), make(chan struct{})}
	var release sync.Once
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan savedQueryLifecycleAnswer, 1)
	var original sync.WaitGroup
	original.Add(1)
	defer func() { release.Do(func() { close(writer.release) }); cancel(); original.Wait() }()
	go func() {
		defer original.Done()
		var out bytes.Buffer
		// The existing held writer owns the block; the buffer is inspected only
		// after joining. No test goroutine reads it concurrently with Write.
		result, err := d.MutateSavedQuery(ctx, bearer, op, io.MultiWriter(writer, &out))
		done <- savedQueryLifecycleAnswer{result, err, append([]byte(nil), out.Bytes()...)}
	}()
	guard := time.NewTimer(5 * time.Second)
	defer guard.Stop()
	select {
	case <-writer.entered:
	case got := <-done:
		t.Fatalf("mutation returned before held output: %+v", got)
	case <-guard.C:
		t.Fatal("mutation did not enter held output")
	}
	retained := savedQueryLifecycleCommitted(t, s, op, created.ResultVersion, "Committed before held output")
	d.mu.Lock()
	var originalID uint64
	for id := range d.active {
		originalID = id
	}
	active, charge := len(d.active), d.bytes
	d.mu.Unlock()
	if active != 1 || charge != discoveryAdmissionBytes+discoveryWorkBytes+discoveryCopyBytes || len(d.sql) != 1 {
		t.Fatalf("missing original admitted writer lease: active=%d bytes=%d sql=%d", active, charge, len(d.sql))
	}
	closed, stop := context.WithCancel(context.Background())
	stop()
	if err := d.Close(closed); !errors.Is(err, ErrDiscoveryDraining) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Close falsely joined held output: %v", err)
	}
	select {
	case <-d.drained:
		t.Fatal("Close signaled drained while original Write was held")
	default:
	}
	var denied bytes.Buffer
	r, err := d.MutateSavedQuery(context.Background(), bearer, op, &denied)
	if !errors.Is(err, ErrClosed) || r.Started || denied.Len() != 0 {
		t.Fatalf("closing service admitted a new call: %+v %v", r, err)
	}
	d.mu.Lock()
	_, ownsOriginal := d.active[originalID]
	active, charge = len(d.active), d.bytes
	d.mu.Unlock()
	if !ownsOriginal || active != 1 || charge != discoveryAdmissionBytes+discoveryWorkBytes+discoveryCopyBytes || len(d.sql) != 1 {
		t.Fatalf("Close/new-call refusal released original ownership: original=%t active=%d bytes=%d sql=%d", ownsOriginal, active, charge, len(d.sql))
	}
	if s.mu.TryLock() {
		s.mu.Unlock()
		t.Fatal("held mutation lost its Store read lease")
	}
	release.Do(func() { close(writer.release) })
	original.Wait()
	got := <-done
	// Once an arbitrary writer has started, Close cannot undo its successful
	// write. Cancellation is not claimed to interrupt caller-owned I/O.
	if got.err != nil || !got.result.Started || !bytes.Equal(got.body, retained) {
		t.Fatalf("original held output settlement: %+v", got)
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	savedQueryLifecycleIdle(t, d)
	select {
	case <-d.drained:
	default:
		t.Fatal("joined original mutation did not signal drained")
	}
	fresh, err := NewDiscoveryService(context.Background(), s, testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fresh.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	savedQueryLifecycleRetry(t, fresh, bearer, op, retained)
}
