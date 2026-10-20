package catalog

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDiscoveryCurrentGrantDenial(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "reader", ownerSubject, Scope{"library", ""}, []string{"assets.discover"})
	bearer := discoveryIssueTest(t, o, a, "reader", ownerSubject, Scope{"library", ""}, []string{"assets.discover"})
	_ = discoveryQueryTest(t, d, bearer, discoveryRequestTest("asset", testAsset))
	g, err := readGrant(context.Background(), s.db, testLibrary, "reader")
	if err != nil {
		t.Fatal(err)
	}
	policyTest(t, o, a, "remove", NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()}))
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), &out)
	if DiscoveryErrorCode(err) != "not_found" || result.Started || out.Len() != 0 {
		t.Fatalf("removed current grant disclosed asset: %v %s", err, out.String())
	}
}

func TestDiscoveryFreshFenceRejectsSnapshotAuthorityLoss(t *testing.T) {
	for _, kind := range []string{"revoke", "disable", "recover", "policy"} {
		t.Run(kind, func(t *testing.T) {
			s, o, a, _ := authorityTestStore(t)
			d := discoveryServiceTest(t, s)
			policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
			discoveryGrantTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
			bearer := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
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
				body   string
			}
			done := make(chan answer, 1)
			go func() {
				var b bytes.Buffer
				r, e := d.Query(ctx, bearer, discoveryRequestTest("history", testAsset), &b)
				done <- answer{r, e, b.String()}
			}()
			select {
			case <-reached:
			case <-ctx.Done():
				close(release)
				<-done
				t.Fatal("query never completed encoding before its fresh fence")
			}
			var mutationErr error
			switch kind {
			case "revoke":
				_, mutationErr = o.RevokeCredential(ctx, adminTestRequest("revoke", a.Incarnation, "revoke_credential", RevokeCredentialRequest{"cred_" + string(bearer[5:37]), 1}))
			case "recover":
				_, mutationErr = o.Recover(ctx, adminTestRequest("recover", a.Incarnation, "recover", RecoverRequest{ownerSubject}))
			case "disable":
				sub, e := readSubject(ctx, s.db, testLibrary, readerSubject)
				if e != nil {
					mutationErr = e
					break
				}
				value := sub.Value
				value.Enabled = false
				p := PolicyOperation{1, testLibrary, "disable", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("update_subject", UpdateSubject{readerSubject, sub.Expected(), value})}}
				_, mutationErr = o.ApplyPolicy(ctx, adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
			case "policy":
				g, e := readGrant(ctx, s.db, testLibrary, "reader")
				if e != nil {
					mutationErr = e
					break
				}
				p := PolicyOperation{1, testLibrary, "remove", "local_owner", ownerSubject, []PolicyChange{NewPolicyChange("remove_grant", RemoveGrant{g.ID, g.Expected()})}}
				_, mutationErr = o.ApplyPolicy(ctx, adminTestRequest(p.ID, a.Incarnation, "apply_policy", ApplyPolicyRequest{p}))
			}
			close(release)
			got := <-done
			if mutationErr != nil {
				t.Fatal(mutationErr)
			}
			want := "unauthenticated"
			if kind == "policy" {
				want = "restart_query"
			}
			if DiscoveryErrorCode(got.err) != want || got.result.Started || got.body != "" {
				t.Fatalf("fresh fence retained %s after encoding: %+v %v %s", kind, got.result, got.err, got.body)
			}
		})
	}
}

func TestDiscoveryExpiryAfterFinalSnapshotCleanup(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	base := time.Unix(1800000000, 0)
	s.clock = func() time.Time { return base }
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	var calls atomic.Int64
	s.clock = func() time.Time {
		if calls.Add(1) >= 3 {
			return base.Add(time.Hour)
		}
		return base
	}
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), &out)
	if calls.Load() != 3 || !errors.Is(err, ErrUnauthenticated) || result.Started || out.Len() != 0 {
		t.Fatalf("post-cleanup expiry: calls=%d started=%v bytes=%d err=%v", calls.Load(), result.Started, out.Len(), err)
	}
}

func TestDiscoveryCloseRetainsStoreUntilOutputDrains(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	reached, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := d.Query(context.Background(), bearer, discoveryRequestTest("search", ""), discoveryWriterFunc(func(b []byte) (int, error) { close(reached); <-release; return len(b), nil }))
		done <- err
	}()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		close(release)
		<-done
		t.Fatal("writer not reached")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := d.Close(ctx)
	if !errors.Is(err, ErrDiscoveryDraining) {
		close(release)
		<-done
		t.Fatalf("claimed drained with live writer: %v", err)
	}
	if _, err := s.GetAsset(context.Background(), testLibrary, testAsset); err != nil {
		close(release)
		<-done
		t.Fatalf("borrowed store closed early: %v", err)
	}
	d.mu.Lock()
	charged := d.bytes
	leased := 0
	for _, w := range d.windows {
		leased += w.leases
		if !w.retired {
			t.Error("close retained a live token")
		}
	}
	d.mu.Unlock()
	if charged <= discoveryAdmissionBytes || leased != 1 {
		t.Errorf("lease charge disappeared: bytes=%d leases=%d", charged, leased)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := d.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	left := d.bytes
	windows, tokens := len(d.windows), len(d.tokens)
	d.mu.Unlock()
	if left != discoveryAdmissionBytes || windows != 0 || tokens != 0 {
		t.Fatalf("drain accounting: %d %d %d", left, windows, tokens)
	}
	var out bytes.Buffer
	if _, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), &out); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed admitted: %v", err)
	}
}

func TestDiscoveryConstructorAndAdmissionBounds(t *testing.T) {
	schema2, _ := newTestStore(t)
	if _, err := NewDiscoveryService(context.Background(), schema2, testLibrary); !errors.Is(err, ErrUpgradeRequired) {
		t.Fatalf("schema2 accepted: %v", err)
	}
	inactive, err := CreateAuthority(context.Background(), filepath.Join(t.TempDir(), "inactive.sqlite"), testLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer inactive.Close()
	if _, err := NewDiscoveryService(context.Background(), inactive, testLibrary); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("inactive authority accepted: %v", err)
	}
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	var leaves []func()
	for i := 0; i < 16; i++ {
		_, leave, err := d.admit(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, leave)
	}
	_, _, err = d.admit(context.Background())
	for _, leave := range leaves {
		leave()
	}
	if DiscoveryErrorCode(err) != "busy" {
		t.Fatalf("admission count unbounded: %v", err)
	}
	for i := 0; i < 4; i++ {
		d.sql <- struct{}{}
	}
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", testAsset), &out)
	for i := 0; i < 4; i++ {
		<-d.sql
	}
	if DiscoveryErrorCode(err) != "busy" || result.Started {
		t.Fatalf("SQL queued rather than refused: %v", err)
	}
	for _, token := range []string{"page1_" + strings.Repeat("1", 64), "page1_" + strings.Repeat("2", 64)} {
		var b bytes.Buffer
		_, err := d.Query(context.Background(), bearer, DiscoveryRequest{Version: 1, Kind: "page", LibraryID: testLibrary, Token: token}, &b)
		if DiscoveryErrorCode(err) != "restart_query" || b.Len() != 0 {
			t.Fatalf("unknown token distinction: %v", err)
		}
	}
}
