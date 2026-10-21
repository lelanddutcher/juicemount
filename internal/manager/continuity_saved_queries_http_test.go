package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

type continuitySavedFixture struct {
	continuityCollectionFixture
	query catalog.CollectionID
}

func newContinuitySavedFixture(t *testing.T) continuitySavedFixture {
	t.Helper()
	f := newContinuityCollectionFixture(t)
	if err := f.base.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpgradeSavedQueries(context.Background(), f.base.path, continuityTestLibrary); err != nil {
		t.Fatal(err)
	}
	var err error
	f.base.store, err = catalog.Open(f.base.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.base.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.base.owner, err = f.base.store.LocalOwner(continuityTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: continuityTestLibrary, ID: "saved-activity-grant", Administration: "local_owner", OperatorSubjectID: continuityTestOwner, Changes: []catalog.PolicyChange{catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "saved-activity", Value: catalog.GrantValue{SubjectID: continuityTestOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"activity.record", "assets.discover"}}})}}
	if _, err := f.base.owner.ApplyPolicy(context.Background(), continuityTestAdminRequest(policy.ID, f.base.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	issued, err := f.base.owner.IssueCredentialV2(context.Background(), catalog.AdminRequestV2{Version: 2, LibraryID: continuityTestLibrary, ID: "saved-read-owner-ceiling", ExpectedIncarnation: f.base.incarnation, Kind: "issue_credential", Value: continuityCollectionRaw(t, catalog.IssueCredentialRequestV2{SubjectID: continuityTestOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"activity.record", "assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})})
	if err != nil {
		t.Fatal(err)
	}
	f.owner = issued.Bearer
	t.Cleanup(func() { clear(issued.Bearer) })
	service, err := catalog.NewDiscoveryService(context.Background(), f.base.store, continuityTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for i := 8; i <= 9; i++ {
		q := catalog.SavedQueryRequest{Version: 2, LibraryID: continuityTestLibrary, ID: catalog.CollectionOperationID(fmt.Sprintf("cop_%032x", i)), CollectionID: catalog.CollectionID(fmt.Sprintf("col_%032x", i)), Kind: "create", Value: continuityCollectionRaw(t, catalog.CreateSavedQuery{Name: "Saved work", CreatorGrantID: catalog.CollectionGrantID(fmt.Sprintf("cgr_%032x", i+100)), Definition: catalog.SavedQueryDefinition{Version: 1, Kind: "ranked_assets", Text: "", ContextAssetIDs: []continuity.AssetID{}}})}
		var out bytes.Buffer
		if _, err := service.MutateSavedQuery(context.Background(), f.owner, q, &out); err != nil {
			t.Fatal(err)
		}
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return continuitySavedFixture{continuityCollectionFixture: f, query: "col_00000000000000000000000000000008"}
}

func continuitySavedBody(t *testing.T, kind string, id catalog.CollectionID, size int) string {
	t.Helper()
	switch kind {
	case "directory":
		return string(continuityCollectionRaw(t, catalog.CollectionDirectoryBody{Version: 2, LibraryID: continuityTestLibrary, State: "all", PageSize: size}))
	case "open", "definition":
		return string(continuityCollectionRaw(t, catalog.CollectionOpenBody{Version: 2, LibraryID: continuityTestLibrary, CollectionID: id}))
	default:
		return string(continuityCollectionRaw(t, catalog.CollectionListBody{Version: 2, LibraryID: continuityTestLibrary, CollectionID: id, PageSize: size}))
	}
}

func TestContinuitySavedQueryProductionRoutesAndCursorOwnership(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(prefix, func(t *testing.T) {
			f := newContinuitySavedFixture(t)
			_, server := continuityTestHost(t, f.base, prefix)
			base := server.URL + prefix + "/api/continuity/"
			for _, kind := range []string{"directory", "open", "definition", "execute", "history", "access"} {
				status, body, headers := continuityTestPOST(t, server.Client(), base+"v2/collections/"+kind, f.owner, continuitySavedBody(t, kind, f.query, 1))
				if status != 200 || headers.Get("Cache-Control") != "no-store" {
					t.Fatalf("route %s: %d", kind, status)
				}
				var page struct {
					Next       *string                    `json:"next_page_token"`
					Collection *catalog.SavedQuerySummary `json:"collection"`
				}
				if json.Unmarshal(body, &page) != nil {
					t.Fatal("response")
				}
				if kind != "directory" && (page.Collection == nil || page.Collection.Kind != "saved_query") {
					t.Fatal("manual DTO in saved family")
				}
				if page.Next != nil {
					pageBody := string(continuityCollectionRaw(t, catalog.SavedQueryPageBody{Version: 2, LibraryID: continuityTestLibrary, QueryKind: kind, CollectionID: func() catalog.CollectionID {
						if kind == "directory" {
							return ""
						}
						return f.query
					}(), Token: *page.Next}))
					status, first, _ := continuityTestPOST(t, server.Client(), base+"v2/collections/page", f.owner, pageBody)
					if status != 200 {
						t.Fatal("same-service continuation")
					}
					status, replay, _ := continuityTestPOST(t, server.Client(), base+"v2/collections/page", f.owner, pageBody)
					if status != 200 || !bytes.Equal(first, replay) {
						t.Fatal("cursor replay changed")
					}
					status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/collections/page", f.reader, pageBody)
					if status != 409 {
						t.Fatal("foreign principal cursor")
					}
					wrong := strings.Replace(pageBody, `"query_kind":"`+kind+`"`, `"query_kind":"access"`, 1)
					if kind != "access" && kind != "directory" {
						status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/collections/page", f.owner, wrong)
						if status != 409 {
							t.Fatal("wrong typed query")
						}
					}
					status, replay, _ = continuityTestPOST(t, server.Client(), base+"v2/collections/page", f.owner, pageBody)
					if status != 200 || !bytes.Equal(first, replay) {
						t.Fatal("foreign request mutated original window")
					}
				}
			}
			for _, pair := range []struct {
				route, body string
				bearer      []byte
			}{
				{"v2/collections/open", continuitySavedBody(t, "open", f.id, 0), f.owner},
				{"v1/collections/open", continuityCollectionBody(t, "open", f.query, 0), f.owner},
				{"v2/collections/open", continuitySavedBody(t, "open", f.query, 0), f.tool},
				{"v2/collections/open", continuitySavedBody(t, "open", f.query, 0), f.reader},
			} {
				status, body, _ := continuityTestPOST(t, server.Client(), base+pair.route, pair.bearer, pair.body)
				if status != 404 || string(body) != `{"version":1,"error":"not_found"}` {
					t.Fatal("family/person/current authority boundary")
				}
			}
			status, body, _ := continuityTestPOST(t, server.Client(), base+"v2/collections/open", f.owner, continuitySavedBody(t, "open", f.query, 0))
			var opened catalog.SavedQueryOpenResponse
			if status != 200 || json.Unmarshal(body, &opened) != nil {
				t.Fatal("open CAS")
			}
			q := catalog.SavedQueryRequest{Version: 2, LibraryID: continuityTestLibrary, ID: "cop_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", CollectionID: f.query, ExpectedVersion: opened.Collection.DefinitionVersion, Kind: "rename", Value: continuityCollectionRaw(t, catalog.RenameCollection{Name: "Changed via HTTP"})}
			canonical, _, err := catalog.CanonicalSavedQueryRequest(q)
			if err != nil {
				t.Fatal(err)
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"v2/collections/mutate", f.owner, string(canonical))
			if _, err := catalog.DecodeSavedQueryReceipt(body); status != 200 || err != nil {
				t.Fatal("real saved query mutation")
			}
		})
	}
}

func TestContinuityRecentsProductionPagingAndStaleMetadata(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(prefix, func(t *testing.T) {
			f := newContinuitySavedFixture(t)
			_, server := continuityTestHost(t, f.base, prefix)
			base := server.URL + prefix + "/api/continuity/"
			for i, id := range []continuity.AssetID{continuityTestA, continuityTestC} {
				status, _, _ := continuityTestPOST(t, server.Client(), base+"v2/select", f.owner, continuityPreferenceBody(fmt.Sprintf("saved-select-%d", i), id, nil, time.Now().Add(10*time.Minute).Unix()))
				if status != 200 {
					t.Fatal("explicit Remember")
				}
			}
			list := string(continuityCollectionRaw(t, catalog.RecentsListBody{Version: 2, LibraryID: continuityTestLibrary, PageSize: 1}))
			status, body, _ := continuityTestPOST(t, server.Client(), base+"v2/recents/list", f.owner, list)
			var page catalog.RecentsResponse
			if status != 200 || json.Unmarshal(body, &page) != nil || page.MatchedCount != 2 || len(page.Items) != 1 || page.Items[0].AssetID != continuityTestC || page.NextPageToken == nil {
				t.Fatal("recent first page")
			}
			pageBody := fmt.Sprintf(`{"version":2,"library_id":%q,"token":%q}`, continuityTestLibrary, *page.NextPageToken)
			status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/page", f.owner, pageBody)
			if status != 409 {
				t.Fatal("recent cursor crossed ad-hoc family")
			}
			status, next, _ := continuityTestPOST(t, server.Client(), base+"v2/recents/page", f.owner, pageBody)
			if status != 200 || json.Unmarshal(next, &page) != nil || len(page.Items) != 1 || page.Items[0].AssetID != continuityTestA || page.NextPageToken != nil {
				t.Fatal("recent continuation")
			}
			status, replay, _ := continuityTestPOST(t, server.Client(), base+"v2/recents/page", f.owner, pageBody)
			if status != 200 || !bytes.Equal(next, replay) {
				t.Fatal("recent replay")
			}
			if _, err := f.base.store.Apply(context.Background(), catalog.Operation{Version: 1, LibraryID: continuityTestLibrary, ID: "new-metadata", Actor: "fixture", Changes: []catalog.Change{{CreateAsset: &catalog.CreateAsset{AssetID: "asset_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Label: "Fresh"}}}}); err != nil {
				t.Fatal(err)
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"v2/recents/page", f.owner, pageBody)
			if status != 409 || string(body) != `{"version":1,"error":"restart_query"}` {
				t.Fatal("stale metadata disclosed")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/recents/list", f.tool, list)
			if status != 404 {
				t.Fatal("service recents")
			}
		})
	}
}

func TestContinuitySavedReadExactFieldsAndTransport(t *testing.T) {
	f := newContinuitySavedFixture(t)
	_, server := continuityTestHost(t, f.base, "")
	base := server.URL + "/api/continuity/v2/"
	good := continuitySavedBody(t, "open", f.query, 0)
	for name, body := range map[string]string{
		"unknown":      strings.Replace(good, `"version":2`, `"extra":0,"version":2`, 1),
		"duplicate":    strings.Replace(good, `"version":2`, `"version":2,"version":2`, 1),
		"null":         strings.Replace(good, `"collection_id":"`+string(f.query)+`"`, `"collection_id":null`, 1),
		"invalid-utf8": good[:len(good)-1] + ",\"x\":\"" + string([]byte{255}) + "\"}",
	} {
		t.Run(name, func(t *testing.T) {
			status, _, _ := continuityTestPOST(t, server.Client(), base+"collections/open", f.owner, body)
			if status != 400 {
				t.Fatal("nonclosed request")
			}
		})
	}
	// Read HTTP permits field order/whitespace while mutation bytes are canonical.
	reordered := fmt.Sprintf("{ \"collection_id\":%q, \"library_id\":%q, \"version\":2 }", f.query, continuityTestLibrary)
	if status, _, _ := continuityTestPOST(t, server.Client(), base+"collections/open", f.owner, reordered); status != 200 {
		t.Fatal("HTTP imposed CLI canonical order")
	}
	for _, kind := range []string{"GET", "origin", "query", "content-type", "authorization"} {
		req, err := http.NewRequest(http.MethodPost, base+"collections/open", strings.NewReader(good))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+string(f.owner))
		req.Header.Set("Content-Type", "application/json")
		switch kind {
		case "GET":
			req.Method = http.MethodGet
		case "origin":
			req.Header.Set("Origin", "https://foreign.invalid")
		case "query":
			req.URL.RawQuery = "x=1"
		case "content-type":
			req.Header.Set("Content-Type", "text/plain")
		case "authorization":
			req.Header.Add("Authorization", "Bearer invalid")
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil || closeErr != nil || resp.StatusCode == 200 || resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("transport defenses bypassed")
		}
	}
}
