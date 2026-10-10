package mutationauthority

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type backendFunc func(context.Context, Request) (DurabilityProof, error)

func (f backendFunc) Apply(ctx context.Context, r Request) (DurabilityProof, error) {
	return f(ctx, r)
}

func testJournal(t *testing.T) (*Journal, string) {
	t.Helper()
	private := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(private, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(private, "authority.db")
	j, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j, path
}

func testRequest(l Lease, id string) Request {
	return Request{Lease: l, ID: id, Digest: DigestBytes([]byte("request:" + id)), ExpectedRevision: "parent@7"}
}

func TestTwoContendersAndStaleEpoch(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, session := range []string{"mac-A", "farm-B"} {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, err := j.Claim(ctx, "media", s, time.Minute)
			results <- err
		}(session)
	}
	wg.Wait()
	close(results)
	success, active := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, ErrOwnerActive):
			active++
		default:
			t.Fatalf("unexpected contender result: %v", err)
		}
	}
	if success != 1 || active != 1 {
		t.Fatalf("contenders: successful=%d denied=%d", success, active)
	}
	owner, err := j.Claim(ctx, "media", "mac-A", time.Minute)
	if errors.Is(err, ErrOwnerActive) {
		owner, err = j.Claim(ctx, "media", "farm-B", time.Minute)
	}
	if err != nil {
		t.Fatal(err)
	}
	clock := time.Now()
	j.now = func() time.Time { return clock }
	clock = time.Unix(0, owner.ExpiresAtNS).Add(time.Nanosecond)
	next := "mac-A"
	if owner.Session == next {
		next = "farm-B"
	}
	successor, err := j.Claim(ctx, "media", next, time.Minute)
	if err != nil || successor.Epoch != owner.Epoch+1 {
		t.Fatalf("successor: %+v %v", successor, err)
	}
	calls := 0
	_, err = j.Execute(ctx, testRequest(owner, "old-op"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "8", Evidence: "durable"}, nil
	}))
	if !errors.Is(err, ErrStaleEpoch) || calls != 0 {
		t.Fatalf("old epoch admission: calls=%d err=%v", calls, err)
	}
}

func TestInFlightAndCancellationBlockSuccession(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	clock := time.Now()
	j.now = func() time.Time { return clock }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	callbackCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := j.Execute(callbackCtx, testRequest(owner, "append"), backendFunc(func(ctx context.Context, _ Request) (DurabilityProof, error) {
			close(started)
			<-ctx.Done()
			return DurabilityProof{}, ctx.Err()
		}))
		done <- err
	}()
	<-started
	if err := j.Close(); !errors.Is(err, ErrInFlight) {
		t.Fatalf("close during callback: %v", err)
	}
	if err := j.ResolveNoEffect(ctx, testRequest(owner, "append"), "external proof"); !errors.Is(err, ErrInFlight) {
		t.Fatalf("settled active callback: %v", err)
	}
	clock = clock.Add(2 * time.Second)
	if _, err := j.Claim(ctx, "media", "farm-B", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("successor during callback: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, ErrUncertain) {
		t.Fatalf("cancelled effect: %v", err)
	}
	if _, err := j.Claim(ctx, "media", "farm-B", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("cancellation granted successor: %v", err)
	}
}

func TestCrashReopenPreparedRequiresReconciliation(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	clock := time.Now()
	j.now = func() time.Time { return clock }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r := testRequest(owner, "publish")
	calls := 0
	_, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{}, errors.New("backend reply lost after possible effect")
	}))
	if !errors.Is(err, ErrUncertain) || calls != 1 {
		t.Fatalf("ambiguous effect: calls=%d err=%v", calls, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.now = func() time.Time { return clock }
	o, err := reopened.Inspect(ctx, "media", "publish")
	if err != nil || o.State != Prepared {
		t.Fatalf("reopened prepared: %+v %v", o, err)
	}
	_, err = reopened.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "8", Evidence: "unexpected replay"}, nil
	}))
	if !errors.Is(err, ErrUncertain) || calls != 1 {
		t.Fatalf("replayed prepared: calls=%d err=%v", calls, err)
	}
	clock = clock.Add(2 * time.Second)
	if _, err := reopened.Claim(ctx, "media", "farm-B", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("successor across restart: %v", err)
	}
	proof := DurabilityProof{Revision: "8", Evidence: "backend-txn-42"}
	receipt, err := reopened.ResolveCommitted(ctx, r, proof)
	if err != nil {
		t.Fatal(err)
	}
	// Expiry stops new effects, but an exact committed retry may still read
	// the receipt until a successor epoch is actually granted.
	again, err := reopened.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{}, nil
	}))
	if err != nil || !reflect.DeepEqual(receipt, again) || calls != 1 {
		t.Fatalf("receipt retry: receipt=%+v again=%+v calls=%d err=%v", receipt, again, calls, err)
	}
	restored, err := reopened.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil || restored.Epoch != owner.Epoch+1 {
		t.Fatalf("expired owner must advance epoch: %+v %v", restored, err)
	}
	if _, err := reopened.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{}, nil
	})); !errors.Is(err, ErrStaleEpoch) || calls != 1 {
		t.Fatalf("old epoch after same-session succession: calls=%d err=%v", calls, err)
	}
}

func TestExpiredSameSessionCannotResurrectPreparedEpoch(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	clock := time.Now()
	j.now = func() time.Time { return clock }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.Execute(ctx, testRequest(owner, "write"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		return DurabilityProof{}, errors.New("ambiguous")
	}))
	if !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	clock = clock.Add(2 * time.Second)
	if _, err := j.Claim(ctx, "media", "mac-A", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("expired same-session claim resurrected prepared epoch: %v", err)
	}
	if _, err := j.Execute(ctx, testRequest(owner, "new"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		t.Fatal("expired old epoch reached backend")
		return DurabilityProof{}, nil
	})); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expired old epoch admitted new effect: %v", err)
	}
}

func TestCommittedReceiptAndChangedRequestAfterReopen(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	owner, err := j.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r := testRequest(owner, "rename")
	calls := 0
	apply := backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "inode@8", Evidence: "object-and-metadata-durable-9"}, nil
	})
	first, err := j.Execute(ctx, r, apply)
	if err != nil || calls != 1 {
		t.Fatalf("first mutation: %+v calls=%d err=%v", first, calls, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second, err := reopened.Execute(ctx, r, apply)
	if err != nil || !reflect.DeepEqual(first, second) || calls != 1 {
		t.Fatalf("reply-loss retry: first=%+v second=%+v calls=%d err=%v", first, second, calls, err)
	}
	changed := r
	changed.Digest = DigestBytes([]byte("different mutation"))
	if _, err := reopened.Execute(ctx, changed, apply); !errors.Is(err, ErrChangedRequest) || calls != 1 {
		t.Fatalf("changed retry: calls=%d err=%v", calls, err)
	}
}

func TestSQLFailureBeforeCallbackHasNoEffect(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	owner, err := j.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec(`CREATE TRIGGER deny_prepared BEFORE INSERT ON operations BEGIN SELECT RAISE(ABORT, 'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	calls := 0
	_, err = j.Execute(ctx, testRequest(owner, "write"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "8", Evidence: "effect"}, nil
	}))
	if err == nil || calls != 0 {
		t.Fatalf("SQL failure crossed callback boundary: calls=%d err=%v", calls, err)
	}
	if _, err := j.Inspect(ctx, "media", "write"); !errors.Is(err, ErrNoSuchOperation) {
		t.Fatalf("prepared row after failed insert: %v", err)
	}
}

func TestEmptyDurabilityProofCannotCommit(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	owner, err := j.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	r := testRequest(owner, "write")
	_, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		return DurabilityProof{}, nil
	}))
	if !errors.Is(err, ErrUncertain) {
		t.Fatalf("empty proof: %v", err)
	}
	if _, err := j.ResolveCommitted(ctx, r, DurabilityProof{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty reconciliation proof: %v", err)
	}
	o, err := j.Inspect(ctx, "media", "write")
	if err != nil || o.State != Prepared {
		t.Fatalf("empty proof changed state: %+v %v", o, err)
	}
}

func TestConfirmedNoEffectAllowsSuccessor(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	clock := time.Now()
	j.now = func() time.Time { return clock }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	r := testRequest(owner, "unlink")
	_, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		return DurabilityProof{}, errors.New("ambiguous reply")
	}))
	if !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	if err := j.ResolveNoEffect(ctx, r, "backend transaction absent and old effect fenced"); err != nil {
		t.Fatal(err)
	}
	if err := j.ResolveNoEffect(ctx, r, "different proof"); !errors.Is(err, ErrChangedRequest) {
		t.Fatalf("changed no-effect proof: %v", err)
	}
	clock = clock.Add(2 * time.Second)
	successor, err := j.Claim(ctx, "media", "farm-B", time.Minute)
	if err != nil || successor.Epoch != owner.Epoch+1 {
		t.Fatalf("successor after proven non-effect: %+v %v", successor, err)
	}
}

func TestReceiptWriteFailureRetainsPrepared(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	owner, err := j.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec(`CREATE TRIGGER deny_receipt BEFORE UPDATE ON operations
		WHEN NEW.state='COMMITTED' BEGIN SELECT RAISE(ABORT, 'receipt write failure'); END`); err != nil {
		t.Fatal(err)
	}
	r := testRequest(owner, "append")
	calls := 0
	_, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "inode@8", Evidence: "backend durable 47"}, nil
	}))
	if !errors.Is(err, ErrUncertain) || calls != 1 {
		t.Fatalf("receipt failure: calls=%d err=%v", calls, err)
	}
	o, err := j.Inspect(ctx, "media", "append")
	if err != nil || o.State != Prepared {
		t.Fatalf("receipt failure state: %+v %v", o, err)
	}
	_, err = j.Execute(ctx, r, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{}, nil
	}))
	if !errors.Is(err, ErrUncertain) || calls != 1 {
		t.Fatalf("receipt failure replayed effect: calls=%d err=%v", calls, err)
	}
}

func TestProcessLockHelper(t *testing.T) {
	path := os.Getenv("MUTATION_AUTHORITY_LOCK_TEST_PATH")
	if path == "" {
		return
	}
	j, err := Open(path)
	if j != nil {
		_ = j.Close()
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("second process opened journal: %v", err)
	}
}

func TestSecondProcessRefused(t *testing.T) {
	_, path := testJournal(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestProcessLockHelper$")
	cmd.Env = append(os.Environ(), "MUTATION_AUTHORITY_LOCK_TEST_PATH="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("lock helper: %v\n%s", err, out)
	}
}

func TestAliasAndHardlinkDoNotBypassLock(t *testing.T) {
	j, path := testJournal(t)
	parent := filepath.Dir(path)
	aliasRoot := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(aliasRoot, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(aliasRoot, "alias")
	if err := os.Symlink(parent, alias); err != nil {
		t.Fatal(err)
	}
	other, err := Open(filepath.Join(alias, filepath.Base(path)))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrLocked) {
		t.Fatalf("symlinked parent bypassed canonical lock: %v", err)
	}
	linked := filepath.Join(aliasRoot, "copy.db")
	if err := os.Link(path, linked); err != nil {
		t.Fatal(err)
	}
	other, err = Open(linked)
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("hardlinked DB bypassed lock: %v", err)
	}
	if err := os.Remove(linked); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	// A database path symlink is rejected even without a competing owner.
	symDB := filepath.Join(aliasRoot, "linked.db")
	if err := os.Symlink(path, symDB); err != nil {
		t.Fatal(err)
	}
	other, err = Open(symDB)
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("symlinked DB accepted: %v", err)
	}
}

func TestActiveCallbacksWithNULNamesNeverAlias(t *testing.T) {
	j, _ := testJournal(t)
	ctx := context.Background()
	// Concatenation with a NUL separator maps both pairs to "a\0b\0c".
	// The database accepts both names, so the in-memory active index must
	// preserve the tuple boundary independently of their string contents.
	a, err := j.Claim(ctx, "a", "first", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	b, err := j.Claim(ctx, "a\x00b", "second", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	rA := testRequest(a, "b\x00c")
	rB := testRequest(b, "c")
	startA, startB := make(chan struct{}), make(chan struct{})
	releaseA, releaseB := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-releaseA:
		default:
			close(releaseA)
		}
		select {
		case <-releaseB:
		default:
			close(releaseB)
		}
	}()
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := j.Execute(ctx, rA, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
			close(startA)
			<-releaseA
			return DurabilityProof{}, errors.New("first callback ambiguous")
		}))
		doneA <- err
	}()
	go func() {
		_, err := j.Execute(ctx, rB, backendFunc(func(context.Context, Request) (DurabilityProof, error) {
			close(startB)
			<-releaseB
			return DurabilityProof{}, errors.New("second callback ambiguous")
		}))
		doneB <- err
	}()
	for _, started := range []<-chan struct{}{startA, startB} {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("callback did not start")
		}
	}
	close(releaseA)
	if err := <-doneA; !errors.Is(err, ErrUncertain) {
		t.Fatalf("first callback: %v", err)
	}
	if err := j.Close(); !errors.Is(err, ErrInFlight) {
		t.Fatalf("first completion unblocked Close while second active: %v", err)
	}
	if _, err := j.ResolveCommitted(ctx, rB, DurabilityProof{Revision: "8", Evidence: "proof"}); !errors.Is(err, ErrInFlight) {
		t.Fatalf("first completion unblocked committed resolver: %v", err)
	}
	if err := j.ResolveNoEffect(ctx, rB, "no-effect proof"); !errors.Is(err, ErrInFlight) {
		t.Fatalf("first completion unblocked no-effect resolver: %v", err)
	}
	close(releaseB)
	if err := <-doneB; !errors.Is(err, ErrUncertain) {
		t.Fatalf("second callback: %v", err)
	}
}

func TestObservedExpiryCannotReopenAfterClockRollback(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	mono := time.Now()
	wall := time.Now()
	j.now = func() time.Time { return mono }
	j.wallNow = func() time.Time { return wall }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	mono = mono.Add(2 * time.Second)
	wall = wall.Add(-time.Hour)
	calls := 0
	apply := backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "8", Evidence: "durable"}, nil
	})
	if _, err := j.Execute(ctx, testRequest(owner, "late"), apply); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("observed expiry: %v", err)
	}
	// Even an invalid backwards movement of the injected monotonic clock
	// cannot reverse the persisted closure once expiry has been observed.
	mono = mono.Add(-2 * time.Second)
	if _, err := j.Execute(ctx, testRequest(owner, "resurrected"), apply); !errors.Is(err, ErrLeaseExpired) || calls != 0 {
		t.Fatalf("rollback resurrected admission: calls=%d err=%v", calls, err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Execute(ctx, testRequest(owner, "after-restart"), apply); !errors.Is(err, ErrLeaseExpired) || calls != 0 {
		t.Fatalf("restart resurrected admission: calls=%d err=%v", calls, err)
	}
	next, err := reopened.Claim(ctx, "media", "mac-A", time.Minute)
	if err != nil || next.Epoch != owner.Epoch+1 {
		t.Fatalf("same-session successor after closure: %+v %v", next, err)
	}
}

func TestDeniedSuccessorPersistsExpiryClosure(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	mono := time.Now()
	j.now = func() time.Time { return mono }
	owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = j.Execute(ctx, testRequest(owner, "uncertain"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		return DurabilityProof{}, errors.New("unknown effect")
	}))
	if !errors.Is(err, ErrUncertain) {
		t.Fatal(err)
	}
	mono = mono.Add(2 * time.Second)
	if _, err := j.Claim(ctx, "media", "farm-B", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("denied successor: %v", err)
	}
	var closed int
	if err := j.db.QueryRow(`SELECT admission_closed FROM owners WHERE volume='media'`).Scan(&closed); err != nil || closed != 1 {
		t.Fatalf("denied claim rolled back closure: closed=%d err=%v", closed, err)
	}
	mono = mono.Add(-2 * time.Second)
	if _, err := j.Execute(ctx, testRequest(owner, "new-effect"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		t.Fatal("closed epoch reached backend")
		return DurabilityProof{}, nil
	})); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("closed epoch after rollback: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Claim(ctx, "media", "farm-B", time.Minute); !errors.Is(err, ErrUnsettled) {
		t.Fatalf("reopen admitted successor over prepared effect: %v", err)
	}
}

func TestRestartFreezesEvenLongRemainingLease(t *testing.T) {
	j, path := testJournal(t)
	ctx := context.Background()
	owner, err := j.Claim(ctx, "media", "mac-A", 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	calls := 0
	_, err = reopened.Execute(ctx, testRequest(owner, "write"), backendFunc(func(context.Context, Request) (DurabilityProof, error) {
		calls++
		return DurabilityProof{Revision: "8", Evidence: "durable"}, nil
	}))
	if !errors.Is(err, ErrLeaseExpired) || calls != 0 {
		t.Fatalf("restart reused old remaining time: calls=%d err=%v", calls, err)
	}
	next, err := reopened.Claim(ctx, "media", "mac-A", time.Hour)
	if err != nil || next.Epoch != owner.Epoch+1 {
		t.Fatalf("restart successor: %+v %v", next, err)
	}
}

func TestOpenRequiresProvisionedDirectory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "not-provisioned")
	path := filepath.Join(missing, "authority.db")
	j, err := Open(path)
	if j != nil {
		_ = j.Close()
	}
	if err == nil {
		t.Fatal("opened journal in absent directory")
	}
	if _, statErr := os.Lstat(missing); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("Open created missing parent: %v", statErr)
	}
}

func TestExpiryLatchWriteFailureCannotReviveAdmission(t *testing.T) {
	for _, triggerBy := range []string{"execute", "claim"} {
		t.Run(triggerBy, func(t *testing.T) {
			j, _ := testJournal(t)
			ctx := context.Background()
			mono := time.Now()
			j.now = func() time.Time { return mono }
			owner, err := j.Claim(ctx, "media", "mac-A", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec(`CREATE TRIGGER deny_expiry BEFORE UPDATE ON owners
				WHEN NEW.admission_closed=1 BEGIN SELECT RAISE(ABORT, 'latch write failed'); END`); err != nil {
				t.Fatal(err)
			}
			mono = mono.Add(2 * time.Second)
			calls := 0
			apply := backendFunc(func(context.Context, Request) (DurabilityProof, error) {
				calls++
				return DurabilityProof{Revision: "8", Evidence: "durable"}, nil
			})
			if triggerBy == "execute" {
				_, err = j.Execute(ctx, testRequest(owner, "late"), apply)
			} else {
				_, err = j.Claim(ctx, "media", "farm-B", time.Minute)
			}
			if err == nil || calls != 0 {
				t.Fatalf("expiry latch write failure: calls=%d err=%v", calls, err)
			}
			if _, err := j.db.Exec(`DROP TRIGGER deny_expiry`); err != nil {
				t.Fatal(err)
			}
			mono = mono.Add(-2 * time.Second)
			if _, err := j.Execute(ctx, testRequest(owner, "resurrected"), apply); !errors.Is(err, ErrLeaseExpired) || calls != 0 {
				t.Fatalf("SQL failure plus rollback revived admission: calls=%d err=%v", calls, err)
			}
		})
	}
}
