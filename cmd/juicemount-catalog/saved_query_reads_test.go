package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func cliSavedReadBody(t *testing.T, bearer []byte, request any) []byte {
	t.Helper()
	return cliCollectionRaw(t, savedQueryMutationEnvelope{Version: 2, Bearer: string(bearer), Request: cliCollectionRaw(t, request)})
}

func cliSavedReadLines(t *testing.T, path string, bearer []byte, request any, command string) [][]byte {
	t.Helper()
	code, out, diagnostic := invokeCLI(path, nil, cliSavedReadBody(t, bearer, request), command, "read")
	if code != 0 || len(diagnostic) != 0 || len(out) == 0 || out[len(out)-1] != '\n' {
		t.Fatalf("read failed: %d", code)
	}
	return bytes.Split(out[:len(out)-1], []byte{'\n'})
}

func TestCLISavedQueryReadProductionTraversal(t *testing.T) {
	path, authority, bearer := cliSavedQuerySetup(t)
	create := cliSavedQueryCreate(t, 800)
	create.Value = cliCollectionRaw(t, catalog.CreateSavedQuery{Name: "All", CreatorGrantID: "cgr_88888888888888888888888888888888", Definition: catalog.SavedQueryDefinition{Version: 1, Kind: "ranked_assets", Text: "", ContextAssetIDs: []continuity.AssetID{}}})
	created := cliSavedQueryMutate(t, path, bearer, create)
	cliSavedQueryMutate(t, path, bearer, cliSavedQueryCreate(t, 801))
	value := catalog.CollectionGrantValue{SubjectID: cliReaderA, Scope: catalog.CollectionGrantScope{Kind: "collection", CollectionID: create.CollectionID}, Actions: []string{"collections.read"}}
	current := cliSavedQueryMutate(t, path, bearer, cliSavedQueryEdit(t, 802, create.CollectionID, created.ResultVersion, "grants", catalog.EditCollectionGrants{Changes: []catalog.CollectionGrantChange{{Kind: "put", ID: "cgr_99999999999999999999999999999999", Value: &value}}}))
	cliSavedQueryMutate(t, path, bearer, cliSavedQueryEdit(t, 803, create.CollectionID, current.ResultVersion, "rename", catalog.RenameCollection{Name: "Renamed"}))
	for _, tc := range []struct {
		kind    string
		request any
		pages   int
	}{
		{"directory", catalog.CollectionCLIDirectoryRequest{Version: 2, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1}, 2},
		{"open", catalog.CollectionCLIOpenRequest{Version: 2, Kind: "open", LibraryID: cliLibrary, CollectionID: create.CollectionID}, 1},
		{"definition", catalog.CollectionCLIOpenRequest{Version: 2, Kind: "definition", LibraryID: cliLibrary, CollectionID: create.CollectionID}, 1},
		{"execute", catalog.CollectionCLIListRequest{Version: 2, Kind: "execute", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}, 2},
		{"history", catalog.CollectionCLIListRequest{Version: 2, Kind: "history", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}, 2},
		{"access", catalog.CollectionCLIListRequest{Version: 2, Kind: "access", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}, 2},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			lines := cliSavedReadLines(t, path, bearer, tc.request, "query")
			if len(lines) != tc.pages {
				t.Fatal("lost or duplicated same-service pages")
			}
			if tc.kind == "execute" {
				for i, body := range lines {
					var page catalog.SavedQueryExecuteResponse
					if json.Unmarshal(body, &page) != nil || len(page.Items) != 1 || page.Items[0].ResultID.CollectionID != create.CollectionID || page.Items[0].AssetID != []continuity.AssetID{cliAssetA, cliAssetB}[i] {
						t.Fatal("typed result order")
					}
				}
			}
		})
	}
	tool := cliCollectionIssue(t, path, authority, "read-query-service", cliCollectionService, cliCollectionActions)
	for _, token := range [][]byte{bearer, tool} {
		id := create.CollectionID
		if bytes.Equal(token, bearer) {
			id = "col_000000000000000000000000000002bc"
		} // existing manual ID
		request := catalog.CollectionCLIOpenRequest{Version: 2, Kind: "open", LibraryID: cliLibrary, CollectionID: id}
		if code, out, _ := invokeCLI(path, nil, cliSavedReadBody(t, token, request), "query", "read"); code != 3 || len(out) != 0 {
			t.Fatal("family or service bypass")
		}
	}
}

func TestCLIRecentsReadProductionTraversal(t *testing.T) {
	path, authority, _ := cliSavedQuerySetup(t)
	policy := catalog.PolicyOperation{Version: 1, LibraryID: cliLibrary, ID: "recent-owner-policy", Administration: "local_owner", OperatorSubjectID: cliOwner, Changes: []catalog.PolicyChange{catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "recent-owner", Value: catalog.GrantValue{SubjectID: cliOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"activity.record", "assets.discover"}}})}}
	if code, _, _ := invokeCLI(path, nil, cliRequest(t, policy.ID, authority.Incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy}), "policy", "apply"); code != 0 {
		t.Fatal("activity policy")
	}
	bearer := cliCollectionIssue(t, path, authority, "recent-owner-ceiling", cliOwner, []string{"activity.record", "assets.discover"})
	store, err := catalog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	service, err := catalog.NewDiscoveryService(context.Background(), store, cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	for i, asset := range []continuity.AssetID{cliAssetA, cliAssetB} {
		var out bytes.Buffer
		if _, err := service.RecordPreference(context.Background(), bearer, catalog.PreferenceRequest{Version: 2, LibraryID: cliLibrary, OperationID: []string{"recent-a", "recent-b"}[i], Kind: "select", AssetID: asset, ExpiresUnix: time.Now().Add(10 * time.Minute).Unix()}, &out); err != nil {
			t.Fatal(err)
		}
	}
	if err := errors.Join(service.Close(context.Background()), store.Close()); err != nil {
		t.Fatal(err)
	}
	lines := cliSavedReadLines(t, path, bearer, catalog.RecentsCLIListRequest{Version: 2, Kind: "list", LibraryID: cliLibrary, PageSize: 1}, "recents")
	if len(lines) != 2 {
		t.Fatal("recents pagination")
	}
	for i, body := range lines {
		var page catalog.RecentsResponse
		if json.Unmarshal(body, &page) != nil || page.MatchedCount != 2 || len(page.Items) != 1 || page.Items[0].AssetID != []continuity.AssetID{cliAssetB, cliAssetA}[i] || bytes.Contains(body, []byte("selection_token")) {
			t.Fatal("personal recent order/privacy")
		}
	}
}

func TestCLISavedReadClosedInputAndOwnership(t *testing.T) {
	path, _, bearer := cliSavedQuerySetup(t)
	create := cliSavedQueryCreate(t, 810)
	cliSavedQueryMutate(t, path, bearer, create)
	request := catalog.CollectionCLIOpenRequest{Version: 2, Kind: "open", LibraryID: cliLibrary, CollectionID: create.CollectionID}
	good := cliSavedReadBody(t, bearer, request)
	for name, body := range map[string][]byte{
		"unknown":            bytes.Replace(good, []byte(`"version":2`), []byte(`"extra":0,"version":2`), 1),
		"duplicate":          bytes.Replace(good, []byte(`"version":2`), []byte(`"version":2,"version":2`), 1),
		"wrong-version":      bytes.Replace(good, []byte(`"version":2`), []byte(`"version":1`), 1),
		"noncanonical-inner": bytes.Replace(good, []byte(`"kind":"open"`), []byte(`"kind": "open"`), 1),
		"page":               bytes.Replace(good, []byte(`"kind":"open"`), []byte(`"kind":"page"`), 1),
		"oversize":           bytes.Repeat([]byte{' '}, 32769),
		"unicode-trim":       append([]byte("\u00a0"), good...),
	} {
		t.Run(name, func(t *testing.T) {
			if code, out, diagnostic := invokeCLI(path, nil, body, "query", "read"); code != 3 || len(out) != 0 || bytes.Contains(diagnostic, bearer) {
				t.Fatal("input accepted or credential leaked")
			}
		})
	}
	name := filepath.Join(t.TempDir(), "input.json")
	if err := os.WriteFile(name, good, 0600); err != nil {
		t.Fatal(err)
	}
	files := defaultFiles()
	var input *faultOutput
	files.input = func(path string) (fileResource, error) {
		f, err := openInput(path)
		if err != nil {
			return nil, err
		}
		input = &faultOutput{File: f, closeErr: errors.New("DO-NOT-ECHO-close")}
		return input, nil
	}
	var out, diagnostic bytes.Buffer
	if code := runConfigured(context.Background(), cliArgs(path, []string{"--input", name}, "query", "read"), bytes.NewReader(nil), &out, &diagnostic, false, files); code != 3 || out.Len() != 0 || input == nil || input.closes != 1 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
		t.Fatal("read before owned input close")
	}
	fault := &cliCollectionCaptureFault{}
	diagnostic.Reset()
	if code := run(context.Background(), cliArgs(path, nil, "query", "read"), bytes.NewReader(good), fault, &diagnostic); code != 3 || !bytes.Contains(diagnostic.Bytes(), []byte(`"partial_output":true`)) || bytes.Contains(diagnostic.Bytes(), bearer) {
		t.Fatal("partial read missing or leaked")
	}
	if code, _, _ := invokeCLI(path, nil, append([]byte(" \t\r\n"), good...), "query", "read"); code != 0 {
		t.Fatal("ASCII trim")
	}
}

func TestSavedReadStreamGuardControlsAndIdenticalHistory(t *testing.T) {
	id := catalog.CollectionID("col_22222222222222222222222222222222")
	summary := catalog.SavedQuerySummary{CollectionID: id, Kind: "saved_query", Name: "Work", DefinitionVersion: "cv_55555555555555555555555555555555", Creator: "you", Permissions: catalog.CollectionPermissions{Read: true, Edit: true, Admin: true}}
	token := "page1_" + strings.Repeat("a", 64)
	initial := catalog.SavedQueryReadRequest{Version: 2, Kind: "history", LibraryID: cliLibrary, CollectionID: id, PageSize: 1}
	row := catalog.CollectionHistoryItem{Kind: "rename", RecordedUnixNano: "42", Actor: "you", Value: json.RawMessage(`{"name":"X"}`)}
	page := catalog.SavedQueryListResponse[catalog.CollectionHistoryItem]{Version: 2, Kind: "history", Collection: summary, Items: []catalog.CollectionHistoryItem{row}, MatchedCount: 2, CountScope: "frozen_window", WindowComplete: true, NextPageToken: &token, SortVersion: "saved_queries2", SnapshotScope: "frozen_snapshot"}
	for _, mode := range []string{"identical-history", "count-drift", "summary-drift", "cycle", "unknown-field", "missing-field", "short-write", "stream-budget", "oversize-page"} {
		t.Run(mode, func(t *testing.T) {
			var out bytes.Buffer
			var writer io.Writer = &out
			if mode == "short-write" {
				writer = &cliCollectionCaptureFault{short: true}
			}
			output := &collectionReadOutput{writer: writer, remaining: savedReadStreamLimit}
			if mode == "stream-budget" {
				output.remaining = 10
			}
			calls := 0
			err := streamSavedRead(context.Background(), func(_ context.Context, cursor string, w io.Writer) (catalog.DisclosureResult, error) {
				p := page
				if mode == "cycle" {
					p.MatchedCount = 3
				}
				if calls > 0 {
					if cursor != token {
						t.Fatal("cursor changed")
					}
					p.NextPageToken = nil
					if mode == "count-drift" {
						p.MatchedCount++
					}
					if mode == "summary-drift" {
						p.Collection.Name = "Changed"
					}
					if mode == "cycle" && calls == 1 {
						p.NextPageToken = &token
					}
				}
				body := cliCollectionRaw(t, p)
				if mode == "unknown-field" {
					body = bytes.Replace(body, []byte(`"version":2`), []byte(`"version":2,"extra":0`), 1)
				}
				if mode == "missing-field" {
					body = bytes.Replace(body, []byte(`"window_complete":true,`), nil, 1)
				}
				if mode == "oversize-page" {
					body = bytes.Repeat([]byte{' '}, (1<<20)+1)
				}
				calls++
				_, err := w.Write(body)
				if mode == "identical-history" && out.Len() == 0 {
					t.Fatal("delayed forwarding")
				}
				return catalog.DisclosureResult{}, err
			}, func(body []byte) (savedReadPageInfo, error) { return savedQueryReadPage(body, initial) }, output)
			if mode == "cycle" && calls != 2 {
				t.Fatal("cycle guard did not stop at repeated token; third valid terminal page was reachable")
			}
			if mode == "identical-history" {
				if err != nil || calls != 2 || bytes.Count(out.Bytes(), []byte(`"name":"X"`)) != 2 {
					t.Fatal("history incorrectly deduplicated")
				}
			} else if err == nil {
				t.Fatal("guard accepted invalid traversal")
			}
		})
	}
}

func cliSavedReadSummaryFixture() catalog.SavedQuerySummary {
	return catalog.SavedQuerySummary{CollectionID: "col_22222222222222222222222222222222", Kind: "saved_query", Name: "Work", DefinitionVersion: "cv_55555555555555555555555555555555", Creator: "you", Permissions: catalog.CollectionPermissions{Read: true, Edit: true, Admin: true}}
}

func cliSavedReadAccessFixture(summary catalog.SavedQuerySummary) catalog.CollectionAccessItem {
	return catalog.CollectionAccessItem{GrantID: "cgr_11111111111111111111111111111111", Expected: catalog.ExpectedCollectionGrant{Value: catalog.CollectionGrantValue{SubjectID: "sub_33333333333333333333333333333333", Scope: catalog.CollectionGrantScope{Kind: "collection", CollectionID: summary.CollectionID}, Actions: []string{"collections.read"}}, Version: "cgv_44444444444444444444444444444444", Removed: false}}
}

func TestSavedReadAccessActionGrammar(t *testing.T) {
	summary := cliSavedReadSummaryFixture()
	initial := catalog.SavedQueryReadRequest{Version: 2, Kind: "access", LibraryID: cliLibrary, CollectionID: summary.CollectionID, PageSize: 1}
	for _, tc := range []struct {
		name           string
		actions        []string
		removed, valid bool
	}{
		{"null", nil, false, false},
		{"empty", []string{}, false, false},
		{"unsupported", []string{"assets.discover"}, false, false},
		{"duplicate", []string{"collections.read", "collections.read"}, false, false},
		{"unsorted", []string{"collections.read", "collections.edit"}, false, false},
		{"over-cap", []string{"collections.admin", "collections.edit", "collections.read", "collections.read"}, false, false},
		{"one-right", []string{"collections.read"}, false, true},
		{"all-rights", []string{"collections.admin", "collections.edit", "collections.read"}, false, true},
		{"removed-one-right", []string{"collections.edit"}, true, true},
		{"removed-all-rights", []string{"collections.admin", "collections.edit", "collections.read"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := cliSavedReadAccessFixture(summary)
			row.Expected.Value.Actions, row.Expected.Removed = tc.actions, tc.removed
			page := catalog.SavedQueryListResponse[catalog.CollectionAccessItem]{Version: 2, Kind: "access", Collection: summary, Items: []catalog.CollectionAccessItem{row}, MatchedCount: 1, CountScope: "frozen_window", WindowComplete: true, NextPageToken: nil, SortVersion: "saved_queries2", SnapshotScope: "frozen_snapshot"}
			calls := 0
			var out bytes.Buffer
			err := streamSavedRead(context.Background(), func(_ context.Context, cursor string, writer io.Writer) (catalog.DisclosureResult, error) {
				calls++
				if cursor != "" || calls != 1 {
					t.Fatal("unexpected continuation")
				}
				_, err := writer.Write(cliCollectionRaw(t, page))
				return catalog.DisclosureResult{}, err
			}, func(body []byte) (savedReadPageInfo, error) { return savedQueryReadPage(body, initial) }, &collectionReadOutput{writer: &out, remaining: savedReadStreamLimit})
			if calls != 1 || (err == nil) != tc.valid {
				t.Fatal("access action grammar")
			}
		})
	}
}

func TestSavedReadIdentityUniquenessControls(t *testing.T) {
	summary := cliSavedReadSummaryFixture()
	token := "page1_" + strings.Repeat("b", 64)
	for _, family := range []string{"directory", "execute", "access", "recents"} {
		for _, duplicate := range []bool{false, true} {
			t.Run(family+map[bool]string{false: "/unique", true: "/duplicate"}[duplicate], func(t *testing.T) {
				initial := catalog.SavedQueryReadRequest{Version: 2, Kind: family, LibraryID: cliLibrary, CollectionID: summary.CollectionID, PageSize: 1}
				if family == "directory" {
					initial.CollectionID, initial.State = "", "all"
				}
				recent := catalog.RecentsReadRequest{Version: 2, Kind: "list", LibraryID: cliLibrary, PageSize: 1}
				calls := 0
				var out bytes.Buffer
				err := streamSavedRead(context.Background(), func(_ context.Context, cursor string, writer io.Writer) (catalog.DisclosureResult, error) {
					if calls > 1 || calls == 0 && cursor != "" || calls == 1 && cursor != token {
						t.Fatal("unexpected cursor/call count")
					}
					var next *string
					if calls == 0 {
						next = &token
					}
					secondID := calls == 1 && !duplicate
					asset := cliAssetA
					if secondID {
						asset = cliAssetB
					}
					var page any
					switch family {
					case "directory":
						row := summary
						if secondID {
							row.CollectionID = "col_33333333333333333333333333333333"
						}
						page = catalog.SavedQueryDirectoryResponse{Version: 2, Kind: "directory", Collection: nil, CanCreate: true, Items: []catalog.SavedQuerySummary{row}, MatchedCount: 2, CountScope: "frozen_window", WindowComplete: true, NextPageToken: next, SortVersion: "saved_queries2", SnapshotScope: "frozen_snapshot"}
					case "execute":
						row := catalog.SavedQueryResult{ResultID: catalog.SavedQueryResultID{CollectionID: summary.CollectionID, AssetID: asset}, AssetID: asset, Label: "A", HeadRevisionID: nil, Reasons: []string{"stable_id"}}
						page = catalog.SavedQueryExecuteResponse{Version: 2, Kind: "execute", Collection: summary, QueryVersion: summary.DefinitionVersion, Items: []catalog.SavedQueryResult{row}, MatchedCount: 2, CountScope: "frozen_window", WindowComplete: true, NextPageToken: next, SortVersion: "discovery2", SnapshotScope: "frozen_snapshot"}
					case "access":
						row := cliSavedReadAccessFixture(summary)
						if secondID {
							row.GrantID = "cgr_22222222222222222222222222222222"
						}
						page = catalog.SavedQueryListResponse[catalog.CollectionAccessItem]{Version: 2, Kind: "access", Collection: summary, Items: []catalog.CollectionAccessItem{row}, MatchedCount: 2, CountScope: "frozen_window", WindowComplete: true, NextPageToken: next, SortVersion: "saved_queries2", SnapshotScope: "frozen_snapshot"}
					case "recents":
						row := catalog.RecentsItem{AssetID: asset, Label: "A", HeadRevisionID: nil, Reasons: []string{"declared_selection"}, SelectedUnix: "1700001000", ExpiresUnix: "1702593000"}
						page = catalog.RecentsResponse{Version: 2, Kind: "recents", Items: []catalog.RecentsItem{row}, MatchedCount: 2, CountScope: "frozen_authorized_recent", WindowComplete: true, NextPageToken: next, SortVersion: "recents1", SnapshotScope: "frozen_snapshot"}
					}
					calls++
					_, err := writer.Write(cliCollectionRaw(t, page))
					return catalog.DisclosureResult{}, err
				}, func(body []byte) (savedReadPageInfo, error) {
					if family == "recents" {
						return recentsReadPage(body, recent)
					}
					return savedQueryReadPage(body, initial)
				}, &collectionReadOutput{writer: &out, remaining: savedReadStreamLimit})
				if calls != 2 || (err != nil) != duplicate {
					t.Fatal("identity guard did not distinguish otherwise valid unique/duplicate rows")
				}
			})
		}
	}
}

func cliSavedOwnedReadFixture(t *testing.T, command string) (string, []byte, any) {
	t.Helper()
	path, authority, bearer := cliSavedQuerySetup(t)
	if command == "query" {
		cliSavedQueryMutate(t, path, bearer, cliSavedQueryCreate(t, 820))
		cliSavedQueryMutate(t, path, bearer, cliSavedQueryCreate(t, 821))
		return path, bearer, catalog.CollectionCLIDirectoryRequest{Version: 2, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1}
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: cliLibrary, ID: "owned-recent-policy", Administration: "local_owner", OperatorSubjectID: cliOwner, Changes: []catalog.PolicyChange{catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "owned-recent-grant", Value: catalog.GrantValue{SubjectID: cliOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"activity.record", "assets.discover"}}})}}
	if code, _, _ := invokeCLI(path, nil, cliRequest(t, policy.ID, authority.Incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy}), "policy", "apply"); code != 0 {
		t.Fatal("activity policy")
	}
	bearer = cliCollectionIssue(t, path, authority, "owned-recent-ceiling", cliOwner, []string{"activity.record", "assets.discover"})
	store, err := catalog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	service, err := catalog.NewDiscoveryService(context.Background(), store, cliLibrary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	for i, asset := range []continuity.AssetID{cliAssetA, cliAssetB} {
		if _, err := service.RecordPreference(context.Background(), bearer, catalog.PreferenceRequest{Version: 2, LibraryID: cliLibrary, OperationID: []string{"owned-select-a", "owned-select-b"}[i], Kind: "select", AssetID: asset, ExpiresUnix: time.Now().Add(10 * time.Minute).Unix()}, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	return path, bearer, catalog.RecentsCLIListRequest{Version: 2, Kind: "list", LibraryID: cliLibrary, PageSize: 1}
}

// These resources wrap the exact OS descriptors transferred to runConfigured.
// The stdout intercept cancels only after an actual complete page and its LF.
type cliSavedReadOwnedOutput struct {
	*faultOutput
	afterLine func()
	writes    int
}

func (w *cliSavedReadOwnedOutput) Write(body []byte) (int, error) {
	w.writes++
	n, err := w.faultOutput.Write(body)
	if err == nil && n == len(body) && bytes.Equal(body, []byte{'\n'}) && w.afterLine != nil {
		w.afterLine()
	}
	return n, err
}

type cliSavedReadOwnedDiagnostic struct {
	*faultOutput
	beforeClose []byte
	captureErr  error
}

func (w *cliSavedReadOwnedDiagnostic) Close() error {
	// Capture while the diagnostic sink is still open; later best-effort writes
	// after canceled/closed ownership are deliberately not part of this oracle.
	w.beforeClose, w.captureErr = os.ReadFile(w.Name())
	return w.faultOutput.Close()
}

func TestCLISavedReadsOwnedOutputCloseAndCancellation(t *testing.T) {
	for _, command := range []string{"query", "recents"} {
		for _, mode := range []string{"output-close-error", "cancel-after-first-page"} {
			t.Run(command+"/"+mode, func(t *testing.T) {
				path, bearer, request := cliSavedOwnedReadFixture(t, command)
				// This proves there is a later actual service page before exercising
				// the intercept. The fresh traversal after cleanup must also succeed.
				if lines := cliSavedReadLines(t, path, bearer, request, command); len(lines) != 2 {
					t.Fatal("fixture lacks a second actual page")
				}
				var originals []*os.File
				for _, name := range []string{"input", "output", "diagnostic"} {
					f, err := os.CreateTemp(t.TempDir(), name)
					if err != nil {
						t.Fatal(err)
					}
					defer f.Close()
					originals = append(originals, f)
				}
				if _, err := originals[0].Write(cliSavedReadBody(t, bearer, request)); err != nil {
					t.Fatal(err)
				}
				if _, err := originals[0].Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				input := &faultOutput{File: originals[0]}
				output := &cliSavedReadOwnedOutput{faultOutput: &faultOutput{File: originals[1]}}
				diagnostic := &cliSavedReadOwnedDiagnostic{faultOutput: &faultOutput{File: originals[2]}}
				if mode == "output-close-error" {
					output.closeErr = errors.New("DO-NOT-ECHO-owned-read-close")
				} else {
					output.afterLine = cancel
				}
				files := defaultFiles()
				restored := 0
				files.stdio = func(passed []*os.File) ([]fileResource, func() error, error) {
					if len(passed) != 3 || passed[0] != originals[0] || passed[1] != originals[1] || passed[2] != originals[2] {
						t.Fatal("replaced original I/O")
					}
					return []fileResource{input, output, diagnostic}, func() error { restored++; return nil }, nil
				}
				code := runConfigured(ctx, cliArgs(path, nil, command, "read"), originals[0], originals[1], originals[2], true, files)
				if code != 3 || input.closes != 1 || output.closes != 1 || diagnostic.closes != 1 || restored != 1 {
					t.Fatal("owned originals not closed/restored exactly once")
				}
				body, err := os.ReadFile(originals[1].Name())
				if err != nil {
					t.Fatal(err)
				}
				if mode == "output-close-error" {
					if bytes.Count(body, []byte{'\n'}) != 2 || output.writes != 4 {
						t.Fatal("close fault did not follow successful traversal")
					}
					if diagnostic.captureErr != nil || !bytes.Contains(diagnostic.beforeClose, []byte(`"partial_output":true`)) || !bytes.Contains(diagnostic.beforeClose, []byte(`"output_close_failed":true`)) || bytes.Contains(diagnostic.beforeClose, bearer) || bytes.Contains(diagnostic.beforeClose, []byte("DO-NOT-ECHO")) {
						t.Fatal("owned Close diagnostic missing before sink close or not redacted")
					}
				} else {
					if bytes.Count(body, []byte{'\n'}) != 1 || output.writes != 2 {
						t.Fatal("cancellation disclosed second page or retried output")
					}
					var first struct {
						Next  *string           `json:"next_page_token"`
						Count int               `json:"matched_count"`
						Items []json.RawMessage `json:"items"`
					}
					if json.Unmarshal(bytes.TrimSuffix(body, []byte{'\n'}), &first) != nil || first.Next == nil || first.Count != 2 || len(first.Items) != 1 {
						t.Fatal("cancellation did not follow a complete nonterminal actual page")
					}
					// Cancellation may have closed stderr before the failure path.
					// No diagnostic-write success is required for this case.
				}
				if lines := cliSavedReadLines(t, path, bearer, request, command); len(lines) != 2 {
					t.Fatal("fresh traversal after original cleanup failed")
				}
			})
		}
	}
}
