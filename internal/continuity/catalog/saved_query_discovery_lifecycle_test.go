package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSavedQueryReadHeldPublicationFences(t *testing.T) {
	for _, change := range []string{"queryedit", "archive", "rightloss", "subject", "credential", "metadata", "ownRemember", "expiry"} {
		t.Run(change, func(t *testing.T) {
			f := newSavedReadFixture(t)
			reached, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			f.d.beforeFence = func(ctx context.Context) error {
				close(reached)
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			type answer struct {
				r    DisclosureResult
				err  error
				body []byte
			}
			done := make(chan answer, 1)
			go func() {
				var out bytes.Buffer
				r, err := f.d.QuerySavedQueries(context.Background(), f.reader, f.request("execute"), &out)
				done <- answer{r, err, out.Bytes()}
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				unblock()
				<-done
				t.Fatal("never reached prepublication hold")
			}
			// The held call has already loaded its hook. Subsequent mutation calls do
			// not inherit that hold; original snapshot/SQL owners have been released.
			f.d.beforeFence = nil
			want := "restart_query"
			switch change {
			case "queryedit":
				f.version = f.mutate(t, 50, f.version, "rename", RenameCollection{"changed"})
			case "archive":
				f.version = f.mutate(t, 50, f.version, "archive", SetCollectionArchived{true})
			case "rightloss":
				g, err := readCollectionGrant(context.Background(), f.s.db, "cgr_44444444444444444444444444444444")
				if err != nil {
					t.Fatal(err)
				}
				f.collectionPolicy(t, 50, readOracleQuery, "security_remove", CollectionGrantChange{"remove", g.id, &g.expected, nil})
				want = "not_found"
			case "subject":
				sub, err := readSubject(context.Background(), f.s.db, readOracleLibrary, readOracleB)
				if err != nil {
					t.Fatal(err)
				}
				v := sub.Value
				v.Enabled = false
				f.policy(t, "disable-B", NewPolicyChange("update_subject", UpdateSubject{readOracleB, sub.Expected(), v}))
				want = "unauthenticated"
			case "credential":
				_, err := f.o.RevokeCredential(context.Background(), AdminRequest{1, readOracleLibrary, "revoke-B", f.incarnation, "revoke_credential", collectionTxRaw(t, RevokeCredentialRequest{"cred_" + string(f.reader[5:37]), 1})})
				if err != nil {
					t.Fatal(err)
				}
				want = "unauthenticated"
			case "metadata":
				f.apply(t, "held-rename", Change{RenameAsset: &RenameAsset{readOracleAssets[0], "Alpha", "different"}})
			case "ownRemember":
				f.remember(t, f.reader, "held-remember", readOracleAssets[0])
			case "expiry":
				f.now.Store(1700003500)
				want = "unauthenticated"
			}
			unblock()
			got := <-done
			if DiscoveryErrorCode(got.err) != want || got.r.Started || len(got.body) != 0 {
				t.Fatalf("stale %s disclosed: %+v %v %s", change, got.r, got.err, got.body)
			}
		})
	}
}

func TestSavedQueryReadTypedCursorIsolationAndValidation(t *testing.T) {
	f := newSavedReadFixture(t)
	var first SavedQueryExecuteResponse
	json.Unmarshal(savedReadCall(t, f.d, f.owner, f.request("execute")), &first)
	if first.NextPageToken == nil {
		t.Fatal("missing cursor")
	}
	r := SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: readOracleLibrary, CollectionID: readOracleQuery, QueryKind: "execute", Token: *first.NextPageToken}
	f.d.mu.Lock()
	w := f.d.tokens[r.Token].window
	charge, windows, tokens := f.d.bytes, len(f.d.windows), len(f.d.tokens)
	f.d.mu.Unlock()
	wrong := r
	wrong.QueryKind = "history"
	savedReadDenied(t, f.d, f.owner, wrong, "restart_query")
	wrong = r
	wrong.CollectionID = "col_77777777777777777777777777777777"
	savedReadDenied(t, f.d, f.owner, wrong, "restart_query")
	savedReadDenied(t, f.d, f.reader, r, "restart_query")
	var out bytes.Buffer
	result, err := f.d.QueryRecents(context.Background(), f.owner, RecentsReadRequest{2, "page", readOracleLibrary, 0, r.Token}, &out)
	if DiscoveryErrorCode(err) != "restart_query" || result.Started {
		t.Fatal("cross-family cursor")
	}
	f.d.mu.Lock()
	if f.d.bytes != charge || len(f.d.windows) != windows || len(f.d.tokens) != tokens || w.retired {
		t.Error("wrong typed/owner cursor mutated window")
	}
	f.d.mu.Unlock()
	_ = savedReadCall(t, f.d, f.owner, r)
	for _, bad := range []SavedQueryReadRequest{{Version: 1, Kind: "open", LibraryID: readOracleLibrary, CollectionID: readOracleQuery}, {Version: 2, Kind: "execute", LibraryID: readOracleLibrary, CollectionID: readOracleQuery, PageSize: 101}, {Version: 2, Kind: "page", LibraryID: readOracleLibrary, QueryKind: "open", CollectionID: readOracleQuery, Token: r.Token}} {
		if _, err := ValidateSavedQueryReadRequest(bad, readOracleLibrary); err == nil {
			t.Fatal("bad request admitted")
		}
	}
	good := []byte(`{"version":2,"kind":"execute","library_id":"lib_11111111111111111111111111111111","collection_id":"col_22222222222222222222222222222222","page_size":1}`)
	if _, err := DecodeSavedQueryCLIReadRequest(good, readOracleLibrary); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{append(append([]byte(nil), good...), byte(' ')), bytes.Replace(good, []byte(`"page_size":1`), []byte(`"page_size":1,"extra":0`), 1), []byte(`{"version":2,"kind":"page","library_id":"lib_11111111111111111111111111111111","token":"` + r.Token + `"}`)} {
		if _, err := DecodeSavedQueryCLIReadRequest(bad, readOracleLibrary); err == nil {
			t.Fatal("noncanonical/transport page admitted")
		}
	}
	// A service's otherwise valid collection ceiling cannot enter either family.
	service := SubjectID("sub_99999999999999999999999999999999")
	f.policy(t, "service", NewPolicyChange("create_subject", CreateSubject{service, SubjectValue{"service", "service", true}}))
	credential := f.issue(t, "service-credential", service, readOracleActions[1:])
	savedReadDenied(t, f.d, credential, f.request("open"), "not_found")
	result, err = f.d.QueryRecents(context.Background(), credential, RecentsReadRequest{2, "list", readOracleLibrary, 100, ""}, &out)
	if DiscoveryErrorCode(err) != "not_found" || result.Started {
		t.Fatalf("service recents admitted: %v", err)
	}
	// Freeze family at a known manual ID, even when its grants are valid.
	manual := CollectionRequest{1, readOracleLibrary, "cop_77777777777777777777777777777777", "col_77777777777777777777777777777777", "", "create", collectionTxRaw(t, CreateCollection{"Manual", "cgr_77777777777777777777777777777777", []NewCollectionEntry{}})}
	if _, err := f.d.MutateCollection(context.Background(), f.owner, manual, &out); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"open", "definition", "execute", "history", "access"} {
		wrong = f.request(kind)
		wrong.CollectionID = manual.CollectionID
		savedReadDenied(t, f.d, f.owner, wrong, "not_found")
	}
}

func TestSavedQueryReadWriterFailureCloseAndCancellationOwnership(t *testing.T) {
	for _, family := range []string{"query", "recents"} {
		t.Run(family, func(t *testing.T) {
			f := newSavedReadFixture(t)
			f.remember(t, f.owner, "for-recents", readOracleAssets[0])
			call := func(ctx context.Context, w io.Writer) (DisclosureResult, error) {
				if family == "query" {
					return f.d.QuerySavedQueries(ctx, f.owner, f.request("execute"), w)
				}
				return f.d.QueryRecents(ctx, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""}, w)
			}
			for _, writer := range []io.Writer{savedQueryFailWriter{}, discoveryWriterFunc(func(b []byte) (int, error) { return len(b) - 1, nil })} {
				result, err := call(context.Background(), writer)
				if err == nil || !result.Started {
					t.Fatalf("writer failure hidden: %+v %v", result, err)
				}
			}
			reached, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				_, err := call(ctx, discoveryWriterFunc(func(b []byte) (int, error) { close(reached); <-release; return len(b), nil }))
				done <- err
			}()
			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				close(release)
				<-done
				t.Fatal("writer not reached")
			}
			cancel()
			closedCtx, stop := context.WithCancel(context.Background())
			stop()
			if err := f.d.Close(closedCtx); !errors.Is(err, ErrDiscoveryDraining) {
				close(release)
				<-done
				t.Fatalf("live writer falsely drained: %v", err)
			}
			f.d.mu.Lock()
			bytesHeld := f.d.bytes
			leases := 0
			for _, w := range f.d.windows {
				leases += w.leases
			}
			f.d.mu.Unlock()
			if bytesHeld <= discoveryAdmissionBytes || leases != 1 {
				t.Errorf("original writer ownership lost: %d/%d", bytesHeld, leases)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if err := f.d.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.d.mu.Lock()
			defer f.d.mu.Unlock()
			if f.d.bytes != discoveryAdmissionBytes || len(f.d.windows) != 0 || len(f.d.tokens) != 0 || len(f.d.active) != 0 || len(f.d.building) != 0 {
				t.Fatalf("final drain retained owners: %d", f.d.bytes)
			}
		})
	}
}

func TestSavedQueryReadContinuationRetainsExactReservationUntilWriterReturns(t *testing.T) {
	for _, tc := range []struct {
		family string
		active int64
	}{{"query", 13 << 20}, {"recents", 14 << 20}} {
		t.Run(tc.family, func(t *testing.T) {
			f := newSavedReadFixture(t)
			var token string
			var query SavedQueryReadRequest
			var recent RecentsReadRequest
			if tc.family == "query" {
				var first SavedQueryExecuteResponse
				if err := json.Unmarshal(savedReadCall(t, f.d, f.owner, f.request("execute")), &first); err != nil {
					t.Fatal(err)
				}
				if first.NextPageToken == nil {
					t.Fatal("missing saved-query continuation")
				}
				token = *first.NextPageToken
				query = SavedQueryReadRequest{Version: 2, Kind: "page", LibraryID: readOracleLibrary, CollectionID: readOracleQuery, QueryKind: "execute", Token: token}
			} else {
				f.remember(t, f.owner, "held-recent-first", readOracleAssets[0])
				f.remember(t, f.owner, "held-recent-second", readOracleAssets[1])
				first := readRecentsCall(t, f.d, f.owner, RecentsReadRequest{2, "list", readOracleLibrary, 1, ""})
				if first.NextPageToken == nil {
					t.Fatal("missing Recents continuation")
				}
				token = *first.NextPageToken
				recent = RecentsReadRequest{2, "page", readOracleLibrary, 0, token}
			}
			f.d.mu.Lock()
			w := f.d.tokens[token].window
			retained := w.charge
			initialBytes, initialLeases, initialWindows := f.d.bytes, w.leases, len(f.d.windows)
			f.d.mu.Unlock()
			if initialBytes != discoveryAdmissionBytes+retained || initialLeases != 0 || initialWindows != 1 {
				t.Fatalf("initial retained ownership: bytes%d retained%d leases%d windows%d", initialBytes, retained, initialLeases, initialWindows)
			}
			reached, release := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			joined := false
			defer func() {
				unblock()
				if !joined {
					<-done
				}
			}()
			go func() {
				writer := discoveryWriterFunc(func(body []byte) (int, error) { close(reached); <-release; return len(body), nil })
				var err error
				if tc.family == "query" {
					_, err = f.d.QuerySavedQueries(ctx, f.owner, query, writer)
				} else {
					_, err = f.d.QueryRecents(ctx, f.owner, recent, writer)
				}
				done <- err
			}()
			select {
			case <-reached:
			case err := <-done:
				joined = true
				t.Fatalf("continuation returned before writer: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("continuation writer not reached")
			}
			// Literal13MiB (query) /14MiB (Recents), plus the one retained window.
			// This must reject a compiling query reservation reversion to2MiB.
			f.d.mu.Lock()
			heldBytes, heldLeases, heldActive := f.d.bytes, w.leases, len(f.d.active)
			f.d.mu.Unlock()
			if heldBytes != discoveryAdmissionBytes+retained+tc.active || heldLeases != 1 || heldActive != 1 {
				t.Errorf("continuation reservation before cancel: bytes%d want%d leases%d active%d", heldBytes, discoveryAdmissionBytes+retained+tc.active, heldLeases, heldActive)
			}
			cancel()
			closedCtx, stop := context.WithCancel(context.Background())
			stop()
			if err := f.d.Close(closedCtx); !errors.Is(err, ErrDiscoveryDraining) {
				t.Errorf("live continuation falsely drained: %v", err)
			}
			f.d.mu.Lock()
			heldBytes, heldLeases, heldActive = f.d.bytes, w.leases, len(f.d.active)
			retired, tokenCount := w.retired, len(f.d.tokens)
			f.d.mu.Unlock()
			if heldBytes != discoveryAdmissionBytes+retained+tc.active || heldLeases != 1 || heldActive != 1 || !retired || tokenCount != 0 {
				t.Errorf("cancel/Close released original continuation: bytes%d want%d leases%d active%d retired%v tokens%d", heldBytes, discoveryAdmissionBytes+retained+tc.active, heldLeases, heldActive, retired, tokenCount)
			}
			unblock()
			err := <-done
			joined = true
			if err != nil {
				t.Fatal(err)
			}
			if err := f.d.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.d.mu.Lock()
			finalBytes, windows, tokens, active, building := f.d.bytes, len(f.d.windows), len(f.d.tokens), len(f.d.active), len(f.d.building)
			finalLeases := w.leases
			f.d.mu.Unlock()
			if finalBytes != discoveryAdmissionBytes || windows != 0 || tokens != 0 || active != 0 || building != 0 || finalLeases != 0 {
				t.Fatalf("joined continuation final ownership: bytes%d windows%d tokens%d active%d building%d leases%d", finalBytes, windows, tokens, active, building, finalLeases)
			}
		})
	}
}

func TestSavedQueryHistoryDuplicateSourceEventsAndCountCutoff(t *testing.T) {
	f := newSavedReadFixture(t)
	for i, name := range []string{"X", "Y", "X"} {
		f.version = f.mutate(t, 100+i, f.version, "rename", RenameCollection{name})
	}
	// Grants are canonical scanned source records, but never public history.
	g, err := readCollectionGrant(context.Background(), f.s.db, "cgr_44444444444444444444444444444444")
	if err != nil {
		t.Fatal(err)
	}
	v := g.expected.Value
	v.Actions = []string{"collections.read"}
	f.version = f.mutate(t, 110, f.version, "grants", EditCollectionGrants{[]CollectionGrantChange{{"put", g.id, &g.expected, &v}}})
	var summary SavedQueryOpenResponse
	if err := json.Unmarshal(savedReadCall(t, f.d, f.reader, f.request("open")), &summary); err != nil {
		t.Fatal(err)
	}
	if !summary.Collection.Permissions.Read || summary.Collection.Permissions.Edit || summary.Collection.Permissions.Admin {
		t.Fatal("reader role permissions")
	}
	savedReadDenied(t, f.d, f.reader, f.request("access"), "not_found")
	r := f.request("history")
	r.PageSize = 100
	var p SavedQueryListResponse[CollectionHistoryItem]
	if err := json.Unmarshal(savedReadCall(t, f.d, f.owner, r), &p); err != nil {
		t.Fatal(err)
	}
	if p.MatchedCount != 4 || len(p.Items) != 4 || !p.WindowComplete || p.Items[0].Kind != "rename" || !bytes.Equal(p.Items[0].Value, p.Items[2].Value) || p.Items[3].Kind != "create" {
		t.Fatalf("duplicate history source events were lost: %+v", p)
	}
	for i := 0; i < 996; i++ {
		f.version = f.mutate(t, 200+i, f.version, "rename", RenameCollection{strings.Repeat("R", 1) + string(rune('a'+i%26))})
	}
	if err := json.Unmarshal(savedReadCall(t, f.d, f.owner, r), &p); err != nil {
		t.Fatal(err)
	}
	if p.MatchedCount != 1000 || p.WindowComplete {
		t.Fatalf("1000 output cutoff claimed EOF: %+v", p)
	}
}
