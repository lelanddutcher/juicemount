package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

func cliCollectionReadBody(t *testing.T, bearer []byte, request any) []byte {
	t.Helper()
	return cliCollectionRaw(t, catalog.CollectionCLIReadEnvelope{Version: 1, Bearer: string(bearer), Request: cliCollectionRaw(t, request)})
}

func cliCollectionReadFixture(t *testing.T) (string, catalog.AuthorityStatus, []byte, catalog.CollectionRequest, catalog.CollectionReceipt) {
	t.Helper()
	path, authority := cliCollectionSetup(t)
	bearer := cliCollectionIssue(t, path, authority, "read-owner", cliOwner, cliCollectionActions)
	create := cliCollectionCreate(t, 300)
	create.Value = cliCollectionRaw(t, catalog.CreateCollection{Name: "Read project", CreatorGrantID: "cgr_00000000000000000000000000000514", Entries: []catalog.NewCollectionEntry{
		{ID: "cent_0000000000000000000000000000012c", AssetID: cliAssetA},
		{ID: "cent_00000000000000000000000000000900", AssetID: cliAssetB},
	}})
	current := cliCollectionMutate(t, path, bearer, create)
	cliCollectionMutate(t, path, bearer, cliCollectionCreate(t, 301))
	return path, authority, bearer, create, current
}

func cliCollectionReadLines(t *testing.T, path string, bearer []byte, request any) [][]byte {
	t.Helper()
	code, out, diagnostic := invokeCLI(path, nil, cliCollectionReadBody(t, bearer, request), "collection", "read")
	if code != 0 || len(diagnostic) != 0 || len(out) == 0 || out[len(out)-1] != '\n' {
		t.Fatalf("collection read code=%d", code)
	}
	return bytes.Split(out[:len(out)-1], []byte{'\n'})
}

func TestCLICollectionReadProductionKindsAndTraversal(t *testing.T) {
	path, _, bearer, create, _ := cliCollectionReadFixture(t)
	for _, tc := range []struct {
		name    string
		request any
		pages   int
		kind    string
	}{
		{"directory", catalog.CollectionCLIDirectoryRequest{Version: 1, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1}, 2, "directory"},
		{"open", catalog.CollectionCLIOpenRequest{Version: 1, Kind: "open", LibraryID: cliLibrary, CollectionID: create.CollectionID}, 1, "collection"},
		{"members", catalog.CollectionCLIMembersRequest{Version: 1, Kind: "members", LibraryID: cliLibrary, CollectionID: create.CollectionID, State: "active", PageSize: 1}, 2, "members"},
		{"removed", catalog.CollectionCLIMembersRequest{Version: 1, Kind: "members", LibraryID: cliLibrary, CollectionID: create.CollectionID, State: "removed", PageSize: 1}, 1, "members"},
		{"history", catalog.CollectionCLIListRequest{Version: 1, Kind: "history", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}, 1, "history"},
		{"access", catalog.CollectionCLIListRequest{Version: 1, Kind: "access", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}, 1, "access"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pages := cliCollectionReadLines(t, path, bearer, tc.request)
			if len(pages) != tc.pages {
				t.Fatal("CLI stopped early or duplicated pages")
			}
			for i, body := range pages {
				var page map[string]json.RawMessage
				if json.Unmarshal(body, &page) != nil || string(page["kind"]) != fmt.Sprintf("%q", tc.kind) {
					t.Fatal("wrong read envelope")
				}
				if tc.name != "open" {
					if string(page["items"]) == "null" {
						t.Fatal("null items")
					}
					if (i == len(pages)-1) != (string(page["next_page_token"]) == "null") {
						t.Fatal("CLI omitted continuation or fabricated completion")
					}
				}
			}
		})
	}
}

func TestCLICollectionReadAccessProvidesActualCAS(t *testing.T) {
	path, authority, bearer, create, current := cliCollectionReadFixture(t)
	value := catalog.CollectionGrantValue{SubjectID: cliReaderA, Scope: catalog.CollectionGrantScope{Kind: "collection", CollectionID: create.CollectionID}, Actions: []string{"collections.edit", "collections.read"}}
	grantID := catalog.CollectionGrantID("cgr_77777777777777777777777777777777")
	share := catalog.CollectionRequest{Version: 1, LibraryID: cliLibrary, ID: "cop_77777777777777777777777777777777", CollectionID: create.CollectionID, ExpectedVersion: current.ResultVersion, Kind: "grants", Value: cliCollectionRaw(t, catalog.EditCollectionGrants{Changes: []catalog.CollectionGrantChange{{Kind: "put", ID: grantID, Value: &value}}})}
	cliCollectionMutate(t, path, bearer, share)
	reader := cliCollectionIssue(t, path, authority, "read-reader", cliReaderA, cliCollectionActions)
	request := catalog.CollectionCLIListRequest{Version: 1, Kind: "access", LibraryID: cliLibrary, CollectionID: create.CollectionID, PageSize: 1}
	pages := cliCollectionReadLines(t, path, bearer, request)
	if len(pages) != 2 {
		t.Fatal("access roster did not traverse")
	}
	var expected *catalog.ExpectedCollectionGrant
	for _, body := range pages {
		var page catalog.CollectionListResponse[catalog.CollectionAccessItem]
		if json.Unmarshal(body, &page) != nil {
			t.Fatal("invalid access response")
		}
		for _, item := range page.Items {
			if item.GrantID == grantID {
				copy := item.Expected
				expected = &copy
			}
		}
	}
	if expected == nil || expected.Removed || expected.Value.SubjectID != cliReaderA || expected.Version.Validate() != nil {
		t.Fatal("missing current grant CAS")
	}
	removal := cliCollectionPolicyBody(t, authority, 301, "security_remove", create.CollectionID, catalog.CollectionGrantChange{Kind: "remove", ID: grantID, Expected: expected})
	if code, _, _ := invokeCLI(path, nil, removal, "collection", "policy", "apply"); code != 0 {
		t.Fatal("CLI read CAS could not authorize exact owner removal")
	}
	for _, token := range [][]byte{reader, bearer} {
		code, out, _ := invokeCLI(path, nil, cliCollectionReadBody(t, token, request), "collection", "read")
		if bytes.Equal(token, reader) {
			if code != 3 || len(out) != 0 {
				t.Fatal("removed reader saw roster")
			}
		} else if code != 0 {
			t.Fatal("owner lost access")
		}
	}
	pages = cliCollectionReadLines(t, path, bearer, request)
	found := false
	for _, body := range pages {
		var page catalog.CollectionListResponse[catalog.CollectionAccessItem]
		if json.Unmarshal(body, &page) != nil {
			t.Fatal("invalid tombstone response")
		}
		for _, item := range page.Items {
			if item.GrantID == grantID {
				found = item.Expected.Removed && item.Expected.Version != expected.Version
			}
		}
	}
	if !found {
		t.Fatal("access omitted tombstone or current version")
	}
}

func TestCLICollectionReadCanonicalInputRefusals(t *testing.T) {
	path, _, bearer, create, _ := cliCollectionReadFixture(t)
	good := cliCollectionReadBody(t, bearer, catalog.CollectionCLIOpenRequest{Version: 1, Kind: "open", LibraryID: cliLibrary, CollectionID: create.CollectionID})
	cases := map[string][]byte{
		"unknown":              bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"DO-NOT-ECHO":0`), 1),
		"duplicate":            bytes.Replace(good, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"missing":              bytes.Replace(good, []byte(`"version":1,`), nil, 1),
		"null":                 bytes.Replace(good, []byte(`"kind":"open"`), []byte(`"kind":null`), 1),
		"wrong-library":        bytes.Replace(good, []byte(cliLibrary), []byte("lib_77777777777777777777777777777777"), 1),
		"inner-space":          bytes.Replace(good, []byte(`"kind":"open"`), []byte(`"kind": "open"`), 1),
		"non-ascii-whitespace": append([]byte{0xc2, 0xa0}, good...),
		"oversized":            bytes.Repeat([]byte{'x'}, (32<<10)+1),
		"external-page": cliCollectionReadBody(t, bearer, struct {
			Version int    `json:"version"`
			Kind    string `json:"kind"`
			Library string `json:"library_id"`
			Token   string `json:"token"`
		}{1, "page", string(cliLibrary), "page1_" + strings.Repeat("a", 64)}),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			code, out, diagnostic := invokeCLI(path, nil, body, "collection", "read")
			if code != 3 || len(out) != 0 || bytes.Contains(diagnostic, bearer) || bytes.Contains(diagnostic, []byte("DO-NOT-ECHO")) || bytes.Contains(diagnostic, []byte("page1_")) {
				t.Fatal("invalid read accepted or echoed")
			}
		})
	}
	var out, diagnostic bytes.Buffer
	reader := cliReadFunc(func(p []byte) (int, error) { return copy(p, good), errors.New("DO-NOT-ECHO-read") })
	if run(context.Background(), cliArgs(path, nil, "collection", "read"), reader, &out, &diagnostic) != 3 || out.Len() != 0 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
		t.Fatal("bytes-plus-error read accepted")
	}
	if code, _, _ := invokeCLI(path, nil, append(append([]byte(" \t\r\n"), good...), []byte("\n ")...), "collection", "read"); code != 0 {
		t.Fatal("outer ASCII whitespace refused")
	}
}

type cliCollectionReadIntercept struct {
	bytes.Buffer
	writes    int
	afterLine func()
	fail      bool
}

func (w *cliCollectionReadIntercept) Write(p []byte) (int, error) {
	w.writes++
	if w.fail {
		return 0, errors.New("DO-NOT-ECHO-output")
	}
	n, err := w.Buffer.Write(p)
	if bytes.Equal(p, []byte{'\n'}) && w.afterLine != nil {
		f := w.afterLine
		w.afterLine = nil
		f()
	}
	return n, err
}

func TestCLICollectionReadLaterRevocationAndAttemptedWrite(t *testing.T) {
	for _, mode := range []string{"revoke-after-first-page", "zero-byte-output-error"} {
		t.Run(mode, func(t *testing.T) {
			path, authority, bearer, _, _ := cliCollectionReadFixture(t)
			body := cliCollectionReadBody(t, bearer, catalog.CollectionCLIDirectoryRequest{Version: 1, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1})
			output := &cliCollectionReadIntercept{fail: mode == "zero-byte-output-error"}
			if !output.fail {
				output.afterLine = func() {
					selector := "cred_" + string(bearer[5:37])
					request := cliRequest(t, "read-revoke", authority.Incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: selector, ExpectedGeneration: 1})
					if code, _, _ := invokeCLI(path, nil, request, "credential", "revoke"); code != 0 {
						t.Fatal("fixture revoke failed")
					}
				}
			}
			var diagnostic bytes.Buffer
			code := run(context.Background(), cliArgs(path, nil, "collection", "read"), bytes.NewReader(body), output, &diagnostic)
			if code != 3 || !strings.Contains(diagnostic.String(), `"partial_output":true`) || !strings.Contains(diagnostic.String(), "partial output") || bytes.Contains(diagnostic.Bytes(), bearer) || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") {
				t.Fatal("read uncertainty not explicit/redacted")
			}
			if mode == "revoke-after-first-page" && (bytes.Count(output.Bytes(), []byte{'\n'}) != 1 || output.writes != 2) {
				t.Fatal("later revocation restarted or disclosed another page")
			}
			if output.fail && (output.writes != 1 || output.Len() != 0) {
				t.Fatal("failed first write retried")
			}
		})
	}
}

func TestCLICollectionReadCancellationDrainsService(t *testing.T) {
	path, _, bearer, _, _ := cliCollectionReadFixture(t)
	request := catalog.CollectionCLIDirectoryRequest{Version: 1, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	output := &cliCollectionReadIntercept{afterLine: cancel}
	var diagnostic bytes.Buffer
	code := run(ctx, cliArgs(path, nil, "collection", "read"), bytes.NewReader(cliCollectionReadBody(t, bearer, request)), output, &diagnostic)
	if code != 3 || bytes.Count(output.Bytes(), []byte{'\n'}) != 1 || output.writes != 2 {
		t.Fatal("cancelled traversal continued or claimed success")
	}
	// Existing cancellation closes owned stdio and refuses contextWriter writes.
	// A diagnostic is best-effort; nonzero and no second page remain mandatory.
	if bytes.Contains(diagnostic.Bytes(), bearer) {
		t.Fatal("cancellation leaked bearer")
	}
	if pages := cliCollectionReadLines(t, path, bearer, request); len(pages) != 2 {
		t.Fatal("fresh traversal after canceled cleanup failed")
	}
}

func TestCLICollectionReadOwnedInputAndCloseFailure(t *testing.T) {
	path, _, bearer, create, _ := cliCollectionReadFixture(t)
	body := cliCollectionReadBody(t, bearer, catalog.CollectionCLIOpenRequest{Version: 1, Kind: "open", LibraryID: cliLibrary, CollectionID: create.CollectionID})
	inputPath := filepath.Join(t.TempDir(), "private-input.json")
	if err := os.WriteFile(inputPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	if code, out, diagnostic := invokeCLI(path, []string{"--input", inputPath}, nil, "collection", "read"); code != 0 || len(out) == 0 || len(diagnostic) != 0 {
		t.Fatal("owned input read failed")
	}
	files := defaultFiles()
	var owned *faultOutput
	files.input = func(path string) (fileResource, error) {
		file, err := openInput(path)
		if err != nil {
			return nil, err
		}
		owned = &faultOutput{File: file, closeErr: errors.New("DO-NOT-ECHO-input-close")}
		return owned, nil
	}
	var out, diagnostic bytes.Buffer
	code := runConfigured(context.Background(), cliArgs(path, []string{"--input", inputPath}, "collection", "read"), bytes.NewReader(nil), &out, &diagnostic, false, files)
	if code != 3 || owned == nil || owned.closes != 1 || out.Len() != 0 || strings.Contains(diagnostic.String(), "DO-NOT-ECHO") || bytes.Contains(diagnostic.Bytes(), bearer) {
		t.Fatal("input close failure discarded or echoed")
	}
}

func cliCollectionSyntheticPage(t *testing.T, count int, next *string, items int) []byte {
	t.Helper()
	values := make([]catalog.CollectionSummary, items)
	for i := range values {
		values[i] = catalog.CollectionSummary{CollectionID: catalog.CollectionID(fmt.Sprintf("col_%032x", i+1)), Kind: "manual", Name: "Synthetic", DefinitionVersion: "cv_11111111111111111111111111111111", Creator: "you", Permissions: catalog.CollectionPermissions{Read: true}}
	}
	return cliCollectionRaw(t, catalog.CollectionDirectoryResponse{Version: 1, Kind: "directory", CanCreate: true, Items: values, MatchedCount: count, CountScope: "frozen_window", WindowComplete: true, NextPageToken: next, SortVersion: "collections1", SnapshotScope: "frozen_snapshot"})
}

func TestCLICollectionReadStreamingAndGuardOracles(t *testing.T) {
	initial := catalog.CollectionQueryRequest{Version: 1, Kind: "directory", LibraryID: cliLibrary, State: "all", PageSize: 1}
	token := "page1_" + strings.Repeat("a", 64)
	for _, mode := range []string{"forward-before-return", "cycle", "changed-count", "nonprogress", "later-query-error", "page-budget", "stream-budget", "incomplete-history"} {
		t.Run(mode, func(t *testing.T) {
			var stdout bytes.Buffer
			output := &collectionReadOutput{writer: &stdout, remaining: collectionReadStreamLimit}
			calls := 0
			first := cliCollectionSyntheticPage(t, 2, &token, 1)
			if mode == "cycle" || mode == "nonprogress" {
				first = cliCollectionSyntheticPage(t, 3, &token, 1)
			}
			if mode == "stream-budget" {
				output.remaining = int64(len(first) + 1)
			}
			request := initial
			if mode == "incomplete-history" {
				request = catalog.CollectionQueryRequest{Version: 1, Kind: "history", LibraryID: cliLibrary, CollectionID: "col_11111111111111111111111111111111", PageSize: 1}
			}
			query := func(_ context.Context, _ []byte, r catalog.CollectionQueryRequest, w io.Writer) (catalog.DisclosureResult, error) {
				calls++
				if calls > 1 && (r.Kind != "page" || r.Token != token) {
					t.Fatal("continuation not followed in same traversal")
				}
				body := first
				if calls == 2 {
					body = cliCollectionSyntheticPage(t, 2, nil, 1)
				}
				switch mode {
				case "cycle":
					if calls == 2 {
						body = cliCollectionSyntheticPage(t, 3, &token, 1)
					}
				case "changed-count":
					if calls == 2 {
						body = cliCollectionSyntheticPage(t, 3, nil, 1)
					}
				case "nonprogress":
					if calls == 2 {
						body = cliCollectionSyntheticPage(t, 3, &token, 0)
					}
				case "later-query-error":
					if calls == 2 {
						return catalog.DisclosureResult{}, errors.New("private-query-error")
					}
				case "page-budget":
					body = bytes.Repeat([]byte{'x'}, collectionReadPageLimit+1)
				case "incomplete-history":
					body = cliCollectionRaw(t, catalog.CollectionListResponse[catalog.CollectionHistoryItem]{Version: 1, Kind: "history", Collection: catalog.CollectionSummary{CollectionID: request.CollectionID, Kind: "manual", Name: "Synthetic", DefinitionVersion: "cv_11111111111111111111111111111111", Creator: "you", Permissions: catalog.CollectionPermissions{Read: true}}, Items: []catalog.CollectionHistoryItem{{Kind: "rename", RecordedUnixNano: "1800000000000000001", Actor: "you", Value: cliCollectionRaw(t, catalog.RenameCollection{Name: "Synthetic"})}}, MatchedCount: 1, CountScope: "frozen_window", WindowComplete: false, SortVersion: "collections1", SnapshotScope: "frozen_snapshot"})
				}
				before := stdout.Len()
				n, err := w.Write(body)
				if err == nil && stdout.Len() != before+len(body) {
					t.Fatal("publication delayed until after service return")
				}
				if n != len(body) && err == nil {
					t.Fatal("short write lost")
				}
				return catalog.DisclosureResult{Started: true}, err
			}
			err := streamCollectionRead(context.Background(), query, request, nil, output)
			wantSuccess := mode == "forward-before-return" || mode == "incomplete-history"
			if (err == nil) != wantSuccess {
				t.Fatal("guard oracle mismatch")
			}
			if calls > 2 {
				t.Fatal("guard did not stop bounded traversal")
			}
			if mode == "page-budget" && (stdout.Len() != 0 || output.attempted) {
				t.Fatal("oversized page forwarded")
			}
			if mode == "stream-budget" && (stdout.Len() != len(first)+1 || bytes.Count(stdout.Bytes(), []byte{'\n'}) != 1) {
				t.Fatal("stream cap forwarded excess output")
			}
		})
	}
}
