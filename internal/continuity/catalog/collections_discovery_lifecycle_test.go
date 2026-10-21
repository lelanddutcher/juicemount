package catalog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

func TestCollectionReadAfterEncodingFreshFences(t *testing.T) {
	for _, kind := range []string{"target-policy", "asset", "caller-policy", "revoke", "disable", "recover"} {
		t.Run(kind, func(t *testing.T) {
			s, o, a, d, owner, reader := collectionTxStore(t)
			create := collectionTxCreate(t, 1301)
			collectionTxCall(t, d, owner, create)
			grant := collectionReadGrant(t, o, a, 1302, create.CollectionID, readerSubject, []string{"collections.read"})
			bearer := reader
			if kind == "target-policy" || kind == "asset" {
				bearer = owner
			}
			reached, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			d.beforeFence = func(ctx context.Context) error {
				close(reached)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			type answer struct {
				result DisclosureResult
				err    error
				body   []byte
			}
			done := make(chan answer, 1)
			go func() {
				var out bytes.Buffer
				r, e := d.QueryCollections(ctx, bearer, collectionReadRequest("members", create.CollectionID), &out)
				done <- answer{r, e, out.Bytes()}
			}()
			select {
			case <-reached:
			case <-ctx.Done():
				close(release)
				<-done
				t.Fatal("collection read never reached encoded fence")
			}
			var mutationErr error
			switch kind {
			case "target-policy":
				current, e := readCollectionGrant(ctx, s.db, grant)
				mutationErr = e
				if e == nil {
					value := current.expected.Value
					value.Actions = []string{"collections.edit", "collections.read"}
					p := collectionTxPolicy(1303, create.CollectionID, "collection", CollectionGrantChange{"put", grant, &current.expected, &value})
					_, mutationErr = o.ApplyCollectionPolicy(ctx, collectionTxOwner(t, a, p))
				}
			case "asset":
				_, mutationErr = s.Apply(ctx, testOperation("encoded-label", Change{RenameAsset: &RenameAsset{AssetID: testAsset, ExpectedLabel: "original", Label: "changed after encoding"}}))
			case "caller-policy":
				g, e := readGrant(ctx, s.db, testLibrary, "tx-reader-assets")
				mutationErr = e
				if e == nil {
					p := PolicyOperation{1, testLibrary, "encoded-policy", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()})}}
					_, mutationErr = o.ApplyPolicy(ctx, adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
				}
			case "revoke":
				_, mutationErr = o.RevokeCredential(ctx, adminTestRequest("encoded-revoke", a.Incarnation, "revoke_credential", RevokeCredentialRequest{"cred_" + string(reader[5:37]), 1}))
			case "disable":
				subject, e := readSubject(ctx, s.db, testLibrary, readerSubject)
				mutationErr = e
				if e == nil {
					value := subject.Value
					value.Enabled = false
					p := PolicyOperation{1, testLibrary, "encoded-disable", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("update_subject", UpdateSubject{readerSubject, subject.Expected(), value})}}
					_, mutationErr = o.ApplyPolicy(ctx, adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
				}
			case "recover":
				_, mutationErr = o.Recover(ctx, adminTestRequest("encoded-recover", a.Incarnation, "recover", RecoverRequest{ownerSubject}))
			}
			close(release)
			got := <-done
			if mutationErr != nil {
				t.Fatal(mutationErr)
			}
			want := "restart_query"
			if kind == "revoke" || kind == "disable" || kind == "recover" {
				want = "unauthenticated"
			}
			if DiscoveryErrorCode(got.err) != want || got.result.Started || len(got.body) != 0 {
				t.Fatalf("fresh %s fence: %+v %v %s", kind, got.result, got.err, got.body)
			}
			d.mu.Lock()
			if len(d.windows) != 0 || len(d.building) != 0 || d.bytes != discoveryAdmissionBytes {
				t.Errorf("failed fence leaked cache/reservation: windows=%d building=%d bytes=%d", len(d.windows), len(d.building), d.bytes)
			}
			d.mu.Unlock()
		})
	}
}

func TestCollectionReadInvisibleMutationDoesNotInvalidateDirectory(t *testing.T) {
	_, o, a, d, owner, reader := collectionTxStore(t)
	visible := collectionTxCreate(t, 1401)
	collectionTxCall(t, d, owner, visible)
	collectionReadGrant(t, o, a, 1402, visible.CollectionID, readerSubject, []string{"collections.read"})
	hidden := collectionTxCreate(t, 1403)
	hidden.Value = collectionTxRaw(t, CreateCollection{"Hidden", "cgr_00000000000000000000000000001403", []NewCollectionEntry{}})
	created := collectionTxCall(t, d, owner, hidden)
	d.beforeFence = func(ctx context.Context) error {
		op := CollectionRequest{1, testLibrary, "cop_00000000000000000000000000001404", hidden.CollectionID, created.ResultVersion, "rename", collectionTxRaw(t, RenameCollection{"Still hidden"})}
		var out bytes.Buffer
		_, err := d.MutateCollection(ctx, owner, op, &out)
		return err
	}
	// The callback also runs on MutateCollection: disable it for this one nested
	// call by replacing it before querying; no concurrent calls access the seam.
	hook := d.beforeFence
	d.beforeFence = func(ctx context.Context) error { d.beforeFence = nil; return hook(ctx) }
	result := collectionReadEnvelope(t, collectionReadCall(t, d, reader, collectionReadRequest("directory", "")))
	if result.MatchedCount != 1 || len(result.Items) != 1 || bytes.Contains(result.Items[0], []byte(hidden.CollectionID)) {
		t.Fatal("invisible mutation affected authorized directory")
	}
}

func TestCollectionReadExpiryAfterFreshOwnerCleanup(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 1501)
	collectionTxCall(t, d, owner, create)
	base := s.now()
	var calls atomic.Int64
	d.beforeFence = func(context.Context) error {
		s.clock = func() time.Time {
			if calls.Add(1) >= 2 {
				return base.Add(2 * time.Hour)
			}
			return base
		}
		return nil
	}
	collectionReadDenied(t, d, owner, collectionReadRequest("open", create.CollectionID), "unauthenticated")
	if calls.Load() < 2 {
		t.Fatal("expiry did not reach post-cleanup check")
	}
}

type collectionHeldWriter struct {
	entered chan struct{}
	release chan struct{}
}

func (w collectionHeldWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}
func TestCollectionReadCloseDrainsOriginalWriter(t *testing.T) {
	_, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 1601)
	collectionTxCall(t, d, owner, create)
	writer := collectionHeldWriter{make(chan struct{}), make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		_, err := d.QueryCollections(context.Background(), owner, collectionReadRequest("members", create.CollectionID), writer)
		done <- err
	}()
	select {
	case <-writer.entered:
	case <-time.After(5 * time.Second):
		close(writer.release)
		<-done
		t.Fatal("writer was not entered")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Close(ctx); !errors.Is(err, ErrDiscoveryDraining) {
		close(writer.release)
		<-done
		t.Fatalf("Close falsely joined blocked writer: %v", err)
	}
	d.mu.Lock()
	if len(d.active) != 1 || d.bytes <= discoveryAdmissionBytes {
		t.Error("Close freed an admitted writer lease")
	}
	d.mu.Unlock()
	close(writer.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.active) != 0 || len(d.windows) != 0 || len(d.tokens) != 0 || len(d.building) != 0 || d.bytes != discoveryAdmissionBytes {
		t.Fatal("drained collection owners leaked")
	}
}

type collectionErrorWriter struct {
	body  []byte
	err   error
	short bool
}

func (w *collectionErrorWriter) Write(p []byte) (int, error) {
	w.body = append(w.body, p...)
	if w.short {
		return 0, nil
	}
	return len(p), w.err
}
func TestCollectionReadPublishedRetryAfterOutputError(t *testing.T) {
	_, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 1701)
	collectionTxCall(t, d, owner, create)
	r := collectionReadRequest("members", create.CollectionID)
	r.PageSize = 1
	first := collectionReadEnvelope(t, collectionReadCall(t, d, owner, r))
	if first.Next == nil {
		t.Fatal("missing continuation")
	}
	r = collectionReadRequest("page", "")
	r.Token = *first.Next
	sentinel := errors.New("owned output failure")
	writer := &collectionErrorWriter{err: sentinel}
	result, err := d.QueryCollections(context.Background(), owner, r, writer)
	if !errors.Is(err, sentinel) || !result.Started || len(writer.body) == 0 {
		t.Fatalf("uncertain output: %+v %v", result, err)
	}
	replay := collectionReadCall(t, d, owner, r)
	if !bytes.Equal(replay, writer.body) {
		t.Fatal("published page changed after body+error")
	}
	short := &collectionErrorWriter{short: true}
	result, err = d.QueryCollections(context.Background(), owner, r, short)
	if !errors.Is(err, io.ErrShortWrite) || !result.Started {
		t.Fatal("short write not reported")
	}
}

func TestCollectionReadCancellationBeforeFenceDrainsConstructor(t *testing.T) {
	_, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 2401)
	collectionTxCall(t, d, owner, create)
	parent, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	reached := make(chan struct{})
	d.beforeFence = func(ctx context.Context) error { close(reached); <-ctx.Done(); return ctx.Err() }
	type answer struct {
		result DisclosureResult
		err    error
		body   []byte
	}
	done := make(chan answer, 1)
	go func() {
		var out bytes.Buffer
		r, e := d.QueryCollections(ctx, owner, collectionReadRequest("members", create.CollectionID), &out)
		done <- answer{r, e, out.Bytes()}
	}()
	select {
	case <-reached:
	case <-parent.Done():
		cancel()
		<-done
		t.Fatal("constructor did not reach its owned pre-fence boundary")
	}
	// The original call still owns its work/copy reservation and writer/window
	// leases, but its construction snapshot and SQL slot have been released.
	d.mu.Lock()
	owned := len(d.active) == 1 && len(d.windows) == 1
	for _, w := range d.windows {
		owned = owned && w.leases == 1 && d.bytes-w.charge-discoveryAdmissionBytes == discoveryWorkBytes+discoveryCopyBytes
	}
	d.mu.Unlock()
	sqlReleased := len(d.sql) == 0
	cancel()
	got := <-done // Join the ORIGINAL query before any failure exits the test.
	if !owned || !sqlReleased {
		t.Fatal("constructor reservation/owner boundary was not actually held")
	}
	if !errors.Is(got.err, context.Canceled) || got.result.Started || len(got.body) != 0 {
		t.Fatalf("cancelled constructor disclosed: %+v %v", got.result, got.err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.active) != 0 || len(d.windows) != 0 || len(d.tokens) != 0 || len(d.building) != 0 || d.bytes != discoveryAdmissionBytes {
		t.Fatal("cancelled constructor left work/cache owners")
	}
}

func TestCollectionReadSchemaDriftAfterEncodingRefusesWrite(t *testing.T) {
	s, _, _, d, owner, _ := collectionTxStore(t)
	create := collectionTxCreate(t, 2501)
	collectionTxCall(t, d, owner, create)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reached, release := make(chan struct{}), make(chan struct{})
	d.beforeFence = func(ctx context.Context) error {
		close(reached)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	type answer struct {
		result DisclosureResult
		err    error
		body   []byte
	}
	done := make(chan answer, 1)
	go func() {
		var out bytes.Buffer
		r, e := d.QueryCollections(ctx, owner, collectionReadRequest("members", create.CollectionID), &out)
		done <- answer{r, e, out.Bytes()}
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		close(release)
		<-done
		t.Fatal("query did not encode before schema-drift boundary")
	}
	// Deliberately inconsistent user_version is an adversarial stale-handle
	// fixture, not a supported downgrade or a claim of a valid schema4 database.
	_, changeErr := s.db.ExecContext(ctx, `PRAGMA user_version=4`)
	close(release)
	got := <-done // Restore the fixture only AFTER the original fresh read terminates.
	_, restoreErr := s.db.ExecContext(context.Background(), `PRAGMA user_version=5`)
	if changeErr != nil || restoreErr != nil {
		t.Fatalf("schema drift fixture/restore: %v %v", changeErr, restoreErr)
	}
	if !errors.Is(got.err, ErrUpgradeRequired) || got.result.Started || len(got.body) != 0 {
		t.Fatalf("post-encoding schema drift disclosed: %+v %v", got.result, got.err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.active) != 0 || len(d.windows) != 0 || len(d.tokens) != 0 || len(d.building) != 0 || d.bytes != discoveryAdmissionBytes {
		t.Fatal("schema drift left construction/cache owners")
	}
}
