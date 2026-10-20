package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func discoveryPageRequest(token string) DiscoveryRequest {
	return DiscoveryRequest{Version: 1, Kind: "page", LibraryID: testLibrary, Token: token}
}
func discoveryFirstNext(t *testing.T, d *DiscoveryService, bearer []byte) string {
	t.Helper()
	r := discoveryRequestTest("search", "")
	r.PageSize = 1
	c := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, r))
	if c.Next == nil {
		t.Fatal("fixture needs two pages")
	}
	return *c.Next
}

func TestDiscoveryPageReplayOwnershipExpiryAndEviction(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	owner := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	policyTest(t, o, a, "reader", NewPolicyChange("create_subject", CreateSubject{readerSubject, SubjectValue{"person", "reader", true}}))
	discoveryGrantTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	reader := discoveryIssueTest(t, o, a, "reader", readerSubject, Scope{"library", ""}, discoveryAllActions)
	token := discoveryFirstNext(t, d, owner)
	first := discoveryQueryTest(t, d, owner, discoveryPageRequest(token))
	applyTest(t, s, testOperation("rename-after-freeze", Change{RenameAsset: &RenameAsset{otherAsset, "B", "later"}}))
	if second := discoveryQueryTest(t, d, owner, discoveryPageRequest(token)); !bytes.Equal(first, second) {
		t.Fatalf("replay advanced or reread mutable metadata: %s / %s", first, second)
	}
	// Invalid ownership must not sweep even an expired victim or evict under
	// quota pressure. Manipulating the logical counter creates pressure without
	// allocating an irrelevant64MiB heap fixture.
	d.mu.Lock()
	w := d.tokens[token].window
	w.created = time.Now().Add(-6 * time.Minute)
	saved := d.bytes
	d.bytes = discoveryQuota
	d.mu.Unlock()
	var b bytes.Buffer
	result, err := d.Query(context.Background(), reader, discoveryPageRequest(token), &b)
	d.mu.Lock()
	survived := d.tokens[token] != nil && !w.retired && d.bytes == discoveryQuota
	d.bytes = saved
	d.mu.Unlock()
	if DiscoveryErrorCode(err) != "restart_query" || result.Started || b.Len() != 0 || !survived {
		t.Fatalf("foreign token mutated victim: survived=%v err=%v", survived, err)
	}
	_, err = d.Query(context.Background(), owner, discoveryPageRequest(token), &b)
	if DiscoveryErrorCode(err) != "restart_query" {
		t.Fatalf("expired owner accepted: %v", err)
	}
	d.mu.Lock()
	removed := d.tokens[token] == nil
	d.mu.Unlock()
	if !removed {
		t.Fatal("expired owned window retained")
	}
	oldest := discoveryFirstNext(t, d, owner)
	for i := 0; i < 16; i++ {
		_ = discoveryFirstNext(t, d, owner)
	}
	_, err = d.Query(context.Background(), owner, discoveryPageRequest(oldest), &b)
	if DiscoveryErrorCode(err) != "restart_query" {
		t.Fatalf("oldest subject query not evicted: %v", err)
	}
	d.mu.Lock()
	windows, building := 0, d.building[ownerSubject]
	for _, w := range d.windows {
		if !w.retired && w.binding.handle.subject == ownerSubject {
			windows++
		}
	}
	charged := d.bytes
	d.mu.Unlock()
	if windows != 16 || building != 0 || charged > discoveryQuota {
		t.Fatalf("subject/accounting bounds: windows=%d building=%d bytes=%d", windows, building, charged)
	}
}

func TestDiscoveryConcurrentPageLeasesStayCharged(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	token := discoveryFirstNext(t, d, bearer)
	reached, release := make(chan []byte, 2), make(chan struct{})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := d.Query(context.Background(), bearer, discoveryPageRequest(token), discoveryWriterFunc(func(b []byte) (int, error) { reached <- append([]byte(nil), b...); <-release; return len(b), nil }))
			done <- err
		}()
	}
	var bodies [][]byte
	for i := 0; i < 2; i++ {
		select {
		case b := <-reached:
			bodies = append(bodies, b)
		case <-time.After(5 * time.Second):
			close(release)
			for j := 0; j < 2; j++ {
				<-done
			}
			t.Fatal("both page writers were not admitted")
		}
	}
	d.mu.Lock()
	w := d.tokens[token].window
	before := d.bytes
	leases := w.leases
	binding := w.binding
	d.bytes = discoveryQuota
	d.mu.Unlock()
	err := d.reserveNew(binding)
	d.mu.Lock()
	notEvicted := d.tokens[token] != nil && !w.retired
	d.bytes = before
	d.mu.Unlock()
	if !errors.Is(err, errDiscoveryBusy) || !notEvicted || leases != 2 {
		t.Errorf("leased window evicted or uncharged: err=%v retained=%v leases=%d", err, notEvicted, leases)
	}
	if !bytes.Equal(bodies[0], bodies[1]) {
		t.Error("concurrent replay bodies differ")
	}
	d.mu.Lock()
	d.retireLocked(w)
	held := d.bytes
	stillPresent := d.windows[w.ordinal] != nil
	d.mu.Unlock()
	if held != before || !stillPresent {
		t.Error("retirement released active leases")
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	d.mu.Lock()
	left := d.bytes
	remaining := len(d.windows)
	d.mu.Unlock()
	if left != discoveryAdmissionBytes || remaining != 0 {
		t.Fatalf("lease owners did not drain: bytes=%d windows=%d", left, remaining)
	}
}

func TestDiscoveryConstructionBoundsBeforePublication(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	h, c, subject, err := func() (*AuthHandle, credential, Subject, error) {
		tx, done, e := s.readSnapshot(context.Background())
		if e != nil {
			return nil, credential{}, Subject{}, e
		}
		h, c, sub, e := s.authenticateSnapshot(context.Background(), tx, testLibrary, bearer)
		return h, c, sub, errors.Join(e, done())
	}()
	if err != nil {
		t.Fatal(err)
	}
	binding := discoveryBinding{h, subject.AuthorizationGeneration, c.issued, c.expires, nil}
	r := discoveryRequestTest("search", "")
	// Distinct live raw buffers and their encoded page copies share the same
	// construction reservation; counting only retained JSON would accept this.
	items := make([]json.RawMessage, 160)
	for i := range items {
		items[i] = json.RawMessage(`"` + strings.Repeat("x", 32<<10) + `"`)
	}
	if _, err := d.makeWindow(r, binding, items, true); !errors.Is(err, errDiscoveryWork) {
		t.Fatalf("overlapping construction buffers admitted: %v", err)
	}
	d.mu.Lock()
	published, charged := len(d.tokens), d.bytes
	d.mu.Unlock()
	if published != 0 || charged != discoveryAdmissionBytes {
		t.Fatalf("failed construction published/charged: %d %d", published, charged)
	}
	reader := discoveryReader{}
	var out []json.RawMessage
	if _, err := reader.add(&out, strings.Repeat("z", 64<<10)); !errors.Is(err, errDiscoveryWork) {
		t.Fatalf("oversized DTO accepted: %v", err)
	}
	if _, err := encodeDiscoveryValue(strings.Repeat("x", 1<<20), 1<<20); !errors.Is(err, errDiscoveryWork) {
		t.Fatalf("page bytes exceeded: %v", err)
	}
}

func TestDiscoveryGrantAndHistoryWorkBudgets(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, []string{"assets.discover"})
	var changes []PolicyChange
	for i := 0; i < 4097; i++ {
		changes = append(changes, NewPolicyChange("put_grant", PutGrant{fmt.Sprintf("g-%04d", i), nil, grantForTest(ownerSubject, Scope{"library", ""})}))
		if len(changes) == 256 || i == 4096 {
			policyTest(t, o, a, fmt.Sprintf("grants-%d", i), changes...)
			changes = nil
		}
	}
	var out bytes.Buffer
	result, err := d.Query(context.Background(), bearer, discoveryRequestTest("search", ""), &out)
	if DiscoveryErrorCode(err) != "work_budget" || result.Started || out.Len() != 0 {
		t.Fatalf("4097 grants were silently truncated: %v %s", err, out.String())
	}
	// Length/event counters are checked before fetching/parsing an event. These
	// boundary fixtures use genuine stored requests with a nearly-spent budget.
	tx, done, err := s.readSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := done(); err != nil {
			t.Error(err)
		}
	}()
	for _, budget := range []discoveryBudget{{events: 8192}, {raw: (32 << 20) - 1}} {
		r := discoveryReader{ctx: context.Background(), q: tx, library: testLibrary, budget: budget}
		calls := 0
		complete, err := r.history(testAsset, func(any) (bool, error) { calls++; return false, nil })
		if !errors.Is(err, errDiscoveryWork) || complete || calls != 0 {
			t.Fatalf("history exhaustion became complete: %+v %v calls=%d", budget, err, calls)
		}
	}
}

func TestDiscoveryHistoryThousandItemWindow(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	discoveryGrantTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	bearer := discoveryIssueTest(t, o, a, "owner", ownerSubject, Scope{"library", ""}, discoveryAllActions)
	label := "original"
	var changes []Change
	for i := 0; i < 997; i++ {
		next := fmt.Sprintf("label-%d", i)
		changes = append(changes, Change{RenameAsset: &RenameAsset{testAsset, label, next}})
		label = next
		if len(changes) == 250 || i == 996 {
			applyTest(t, s, testOperation(fmt.Sprintf("history-%d", i), changes...))
			changes = nil
		}
	}
	r := discoveryRequestTest("history", testAsset)
	for _, extra := range []bool{false, true} {
		if extra {
			applyTest(t, s, testOperation("history-extra", Change{RenameAsset: &RenameAsset{testAsset, label, "extra"}}))
		}
		c := discoveryItemsTest(t, discoveryQueryTest(t, d, bearer, r))
		if c.MatchedCount != 1000 || c.Complete || c.Next == nil || len(c.Items) != 100 {
			t.Fatalf("thousand window extra=%v: %+v", extra, c)
		}
	}
}

func TestDiscoveryBuildingReservationsReleaseSubjectKeys(t *testing.T) {
	s, o, a, _ := authorityTestStore(t)
	d := discoveryServiceTest(t, s)
	for i := 1; i <= 4; i++ {
		subject := SubjectID(fmt.Sprintf("sub_%032x", i+100))
		id := fmt.Sprintf("builder-%d", i)
		policyTest(t, o, a, "create-"+id, NewPolicyChange("create_subject", CreateSubject{subject, SubjectValue{"person", id, true}}))
		discoveryGrantTest(t, o, a, id, subject, Scope{"library", ""}, discoveryAllActions)
		bearer := discoveryIssueTest(t, o, a, id, subject, Scope{"library", ""}, discoveryAllActions)
		_ = discoveryQueryTest(t, d, bearer, discoveryRequestTest("search", ""))
		d.mu.Lock()
		successKeys := len(d.building)
		d.mu.Unlock()
		if successKeys != 0 {
			t.Fatalf("published window retained building subject: keys=%d", successKeys)
		}
		var out bytes.Buffer
		result, err := d.Query(context.Background(), bearer, discoveryRequestTest("asset", "asset_99999999999999999999999999999999"), &out)
		d.mu.Lock()
		failedKeys := len(d.building)
		d.mu.Unlock()
		if DiscoveryErrorCode(err) != "not_found" || result.Started || failedKeys != 0 {
			t.Fatalf("failed construction retained building subject: keys=%d started=%v err=%v", failedKeys, result.Started, err)
		}
		// Abandon a fully constructed window before its release fence.
		d.beforeFence = func(context.Context) error { return context.Canceled }
		result, err = d.Query(context.Background(), bearer, discoveryRequestTest("search", ""), &out)
		d.beforeFence = nil
		d.mu.Lock()
		abandonedKeys := len(d.building)
		for _, w := range d.windows {
			d.retireLocked(w)
		}
		remaining := d.bytes
		d.mu.Unlock()
		if !errors.Is(err, context.Canceled) || result.Started || abandonedKeys != 0 || remaining != discoveryAdmissionBytes {
			t.Fatalf("abandoned construction retained reservation: keys=%d bytes=%d err=%v", abandonedKeys, remaining, err)
		}
	}
	// Two simultaneously held reservations for one subject must preserve the
	// positive count until the final builder releases it. Every worker is joined.
	binding := discoveryBinding{handle: &AuthHandle{store: s, subject: ownerSubject}}
	reserved := make(chan error, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	done := []chan struct{}{make(chan struct{}), make(chan struct{})}
	for i := 0; i < 2; i++ {
		go func(i int) {
			err := d.reserveNew(binding)
			reserved <- err
			<-release[i]
			if err == nil {
				d.mu.Lock()
				d.releaseBuildingLocked(ownerSubject)
				d.bytes -= discoveryNewBytes
				d.mu.Unlock()
			}
			close(done[i])
		}(i)
	}
	err := errors.Join(<-reserved, <-reserved)
	d.mu.Lock()
	both := d.building[ownerSubject]
	d.mu.Unlock()
	close(release[0])
	<-done[0]
	d.mu.Lock()
	one, exists := d.building[ownerSubject]
	d.mu.Unlock()
	close(release[1])
	<-done[1]
	d.mu.Lock()
	keys, remaining := len(d.building), d.bytes
	d.mu.Unlock()
	if err != nil || both != 2 || one != 1 || !exists || keys != 0 || remaining != discoveryAdmissionBytes {
		t.Fatalf("concurrent building release: both=%d one=%d exists=%v keys=%d bytes=%d err=%v", both, one, exists, keys, remaining, err)
	}
}
