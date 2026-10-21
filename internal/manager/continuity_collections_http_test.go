package manager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

type continuityCollectionFixture struct {
	base                continuityTestFixture
	owner, reader, tool []byte
	id                  catalog.CollectionID
}

func continuityCollectionRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newContinuityCollectionFixture(t *testing.T) continuityCollectionFixture {
	t.Helper()
	f := newContinuityTestFixture(t)
	ctx := context.Background()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpgradePreferences(ctx, f.path, continuityTestLibrary); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpgradeCollections(ctx, f.path, continuityTestLibrary); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = catalog.Open(f.path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.owner, err = f.store.LocalOwner(continuityTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: continuityTestLibrary, ID: "collection-owner-assets", Administration: "local_owner", OperatorSubjectID: continuityTestOwner, Changes: []catalog.PolicyChange{catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: "collection-owner-assets", Value: catalog.GrantValue{SubjectID: continuityTestOwner, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"assets.discover"}}})}}
	if _, err := f.owner.ApplyPolicy(ctx, continuityTestAdminRequest(policy.ID, f.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	value := catalog.CollectionGrantValue{SubjectID: continuityTestOwner, Scope: catalog.CollectionGrantScope{Kind: "library_create"}, Actions: []string{"collections.create"}}
	provision := catalog.CollectionPolicyRequest{Version: 2, LibraryID: continuityTestLibrary, ID: "cpo_11111111111111111111111111111111", Category: "library_create", Changes: []catalog.CollectionGrantChange{{Kind: "put", ID: "cgr_11111111111111111111111111111111", Value: &value}}}
	ownerRequest := catalog.CollectionOwnerRequest{Version: 1, LibraryID: continuityTestLibrary, ID: string(provision.ID), ExpectedIncarnation: f.incarnation, Kind: "apply_collection_policy", Value: continuityCollectionRaw(t, catalog.ApplyCollectionPolicyRequest{Operation: provision})}
	if _, err := f.owner.ApplyCollectionPolicy(ctx, ownerRequest); err != nil {
		t.Fatal(err)
	}
	result := continuityCollectionFixture{base: f, id: "col_11111111111111111111111111111111"}
	for i, subject := range []catalog.SubjectID{continuityTestOwner, continuityTestReader, continuityTestTool} {
		request := catalog.AdminRequestV2{Version: 2, LibraryID: continuityTestLibrary, ID: fmt.Sprintf("collection-credential-%d", i), ExpectedIncarnation: f.incarnation, Kind: "issue_credential", Value: continuityCollectionRaw(t, catalog.IssueCredentialRequestV2{SubjectID: subject, Scope: catalog.Scope{Kind: "library"}, Actions: []string{"assets.discover", "collections.admin", "collections.create", "collections.edit", "collections.read"}, ExpiresUnix: time.Now().Add(time.Hour).Unix()})}
		issued, err := f.owner.IssueCredentialV2(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		switch i {
		case 0:
			result.owner = issued.Bearer
		case 1:
			result.reader = issued.Bearer
		case 2:
			result.tool = issued.Bearer
		}
	}
	t.Cleanup(func() { clear(result.owner); clear(result.reader); clear(result.tool) })
	service, err := catalog.NewDiscoveryService(ctx, f.store, continuityTestLibrary)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Close(closeCtx); err != nil {
			t.Error(err)
		}
	})
	call := func(request catalog.CollectionRequest) catalog.CollectionReceipt {
		var out bytes.Buffer
		if _, err := service.MutateCollection(ctx, result.owner, request, &out); err != nil {
			t.Fatal(err)
		}
		receipt, err := catalog.DecodeCollectionReceipt(out.Bytes())
		if err != nil {
			t.Fatal(err)
		}
		return receipt
	}
	create := catalog.CollectionRequest{Version: 1, LibraryID: continuityTestLibrary, ID: "cop_11111111111111111111111111111111", CollectionID: result.id, Kind: "create", Value: continuityCollectionRaw(t, catalog.CreateCollection{Name: "Synthetic shared project", CreatorGrantID: "cgr_22222222222222222222222222222222", Entries: []catalog.NewCollectionEntry{{ID: "cent_11111111111111111111111111111111", AssetID: continuityTestA}, {ID: "cent_22222222222222222222222222222222", AssetID: continuityTestB}}})}
	created := call(create)
	readerGrant := catalog.CollectionGrantValue{SubjectID: continuityTestReader, Scope: catalog.CollectionGrantScope{Kind: "collection", CollectionID: result.id}, Actions: []string{"collections.edit", "collections.read"}}
	call(catalog.CollectionRequest{Version: 1, LibraryID: continuityTestLibrary, ID: "cop_22222222222222222222222222222222", CollectionID: result.id, ExpectedVersion: created.ResultVersion, Kind: "grants", Value: continuityCollectionRaw(t, catalog.EditCollectionGrants{Changes: []catalog.CollectionGrantChange{{Kind: "put", ID: "cgr_33333333333333333333333333333333", Value: &readerGrant}}})})
	call(catalog.CollectionRequest{Version: 1, LibraryID: continuityTestLibrary, ID: "cop_33333333333333333333333333333333", CollectionID: "col_22222222222222222222222222222222", Kind: "create", Value: continuityCollectionRaw(t, catalog.CreateCollection{Name: "Private project", CreatorGrantID: "cgr_44444444444444444444444444444444", Entries: []catalog.NewCollectionEntry{}})})
	if err := service.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return result
}

func continuityCollectionBody(t *testing.T, route string, id catalog.CollectionID, size int) string {
	t.Helper()
	switch route {
	case "directory":
		return string(continuityCollectionRaw(t, catalog.CollectionDirectoryBody{Version: 1, LibraryID: continuityTestLibrary, State: "all", PageSize: size}))
	case "open":
		return string(continuityCollectionRaw(t, catalog.CollectionOpenBody{Version: 1, LibraryID: continuityTestLibrary, CollectionID: id}))
	case "members":
		return string(continuityCollectionRaw(t, catalog.CollectionMembersBody{Version: 1, LibraryID: continuityTestLibrary, CollectionID: id, State: "active", PageSize: size}))
	case "history", "access":
		return string(continuityCollectionRaw(t, catalog.CollectionListBody{Version: 1, LibraryID: continuityTestLibrary, CollectionID: id, PageSize: size}))
	default:
		t.Fatal("unsupported fixture route")
		return ""
	}
}

func TestContinuityCollectionsProductionRoutesAndCAS(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(prefix, func(t *testing.T) {
			f := newContinuityCollectionFixture(t)
			_, server := continuityTestHost(t, f.base, prefix)
			base := server.URL + prefix + "/api/continuity/v1/collections/"
			status, body, _ := continuityTestPOST(t, server.Client(), base+"directory", f.owner, continuityCollectionBody(t, "directory", f.id, 1))
			var directory catalog.CollectionDirectoryResponse
			if status != 200 || json.Unmarshal(body, &directory) != nil || directory.MatchedCount != 2 || len(directory.Items) != 1 || directory.NextPageToken == nil || !directory.CanCreate {
				t.Fatal("directory first page")
			}
			pageBody := string(continuityCollectionRaw(t, catalog.CollectionPageBody{Version: 1, LibraryID: continuityTestLibrary, Token: *directory.NextPageToken}))
			status, body, _ = continuityTestPOST(t, server.Client(), base+"page", f.owner, pageBody)
			if status != 200 || json.Unmarshal(body, &directory) != nil || directory.NextPageToken != nil || len(directory.Items) != 1 {
				t.Fatal("directory continuation")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), server.URL+prefix+"/api/continuity/v1/page", f.owner, pageBody)
			if status != 409 {
				t.Fatal("collection token crossed family")
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"open", f.reader, continuityCollectionBody(t, "open", f.id, 0))
			var opened catalog.CollectionOpenResponse
			var fields map[string]json.RawMessage
			if status != 200 || json.Unmarshal(body, &opened) != nil || json.Unmarshal(body, &fields) != nil || len(fields) != 3 || opened.Kind != "collection" || !opened.Collection.Permissions.Edit || opened.Collection.Permissions.Admin {
				t.Fatal("open permissions or envelope")
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"members", f.reader, continuityCollectionBody(t, "members", f.id, 100))
			var members catalog.CollectionMembersResponse
			if status != 200 || json.Unmarshal(body, &members) != nil || members.MatchedCount != 1 || len(members.Items) != 1 || members.Items[0].AssetID != continuityTestA || bytes.Contains(body, []byte("hidden-b")) {
				t.Fatal("member authorization leaked")
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"history", f.reader, continuityCollectionBody(t, "history", f.id, 100))
			var history catalog.CollectionListResponse[catalog.CollectionHistoryItem]
			if status != 200 || json.Unmarshal(body, &history) != nil || len(history.Items) != 0 {
				t.Fatal("mixed hidden create/grant history disclosed")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"access", f.reader, continuityCollectionBody(t, "access", f.id, 100))
			if status != 404 {
				t.Fatal("read/edit exposed admin roster")
			}
			status, body, _ = continuityTestPOST(t, server.Client(), base+"access", f.owner, continuityCollectionBody(t, "access", f.id, 100))
			var access catalog.CollectionListResponse[catalog.CollectionAccessItem]
			if status != 200 || json.Unmarshal(body, &access) != nil || len(access.Items) != 2 {
				t.Fatal("owner roster unavailable")
			}
			var expected *catalog.ExpectedCollectionGrant
			for _, item := range access.Items {
				if item.Expected.Value.SubjectID == continuityTestReader {
					copy := item.Expected
					expected = &copy
				}
			}
			if expected == nil {
				t.Fatal("read failed to provide current full grant CAS")
			}
			remove := catalog.CollectionRequest{Version: 1, LibraryID: continuityTestLibrary, ID: "cop_44444444444444444444444444444444", CollectionID: f.id, ExpectedVersion: access.Collection.DefinitionVersion, Kind: "grants", Value: continuityCollectionRaw(t, catalog.EditCollectionGrants{Changes: []catalog.CollectionGrantChange{{Kind: "remove", ID: "cgr_33333333333333333333333333333333", Expected: expected}}})}
			request, _, err := catalog.CanonicalCollectionRequest(remove)
			if err != nil {
				t.Fatal(err)
			}
			status, receipt, _ := continuityTestPOST(t, server.Client(), base+"mutate", f.owner, string(request))
			if status != 200 {
				t.Fatal("HTTP access CAS could not drive mutation")
			}
			if _, err := catalog.DecodeCollectionReceipt(receipt); err != nil {
				t.Fatal(err)
			}
			status, retry, _ := continuityTestPOST(t, server.Client(), base+"mutate", f.owner, string(request))
			if status != 200 || !bytes.Equal(receipt, retry) {
				t.Fatal("HTTP mutation retry changed receipt")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"open", f.reader, continuityCollectionBody(t, "open", f.id, 0))
			if status != 404 {
				t.Fatal("removed reader retained access")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"directory", f.tool, continuityCollectionBody(t, "directory", f.id, 100))
			if status != 404 {
				t.Fatal("service credential admitted collection read")
			}
		})
	}
}

func TestContinuityCollectionsStrictDecoderKinds(t *testing.T) {
	id := catalog.CollectionID("col_11111111111111111111111111111111")
	for _, route := range []string{"directory", "open", "members", "history", "access", "page"} {
		t.Run(route, func(t *testing.T) {
			body := ""
			if route == "page" {
				body = string(continuityCollectionRaw(t, catalog.CollectionPageBody{Version: 1, LibraryID: continuityTestLibrary, Token: "page1_" + strings.Repeat("a", 64)}))
			} else {
				body = continuityCollectionBody(t, route, id, 1)
			}
			if request, err := decodeContinuityCollectionRequest(route, []byte(body)); err != nil || request.Kind != route {
				t.Fatal("valid route body refused")
			}
			for name, bad := range map[string]string{
				"unknown-kind":  strings.Replace(body, `"version":1`, `"version":1,"kind":"`+route+`"`, 1),
				"duplicate":     strings.Replace(body, `"version":1`, `"version":1,"version":1`, 1),
				"missing":       strings.Replace(body, `"version":1,`, "", 1),
				"null":          strings.Replace(body, `"version":1`, `"version":null`, 1),
				"exponent":      strings.Replace(body, `"version":1`, `"version":1e0`, 1),
				"wrong-version": strings.Replace(body, `"version":1`, `"version":2`, 1),
				"extra-field":   strings.Replace(body, `"version":1`, `"version":1,"actor_subject_id":"private"`, 1),
				"trailing":      body + `{}`,
			} {
				t.Run(name, func(t *testing.T) {
					got, err := decodeContinuityCollectionRequest(route, []byte(bad))
					if !errors.Is(err, catalog.ErrInvalid) || got != (catalog.CollectionQueryRequest{}) {
						t.Fatal("bad read request did not return zero/fixed error")
					}
				})
			}
			var object map[string]json.RawMessage
			if json.Unmarshal([]byte(body), &object) != nil {
				t.Fatal("fixture JSON")
			}
			reordered, err := json.MarshalIndent(object, "", " ")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeContinuityCollectionRequest(route, reordered); err != nil {
				t.Fatal("HTTP improperly requires canonical order")
			}
		})
	}
}

func TestContinuityCollectionsHTTPTransportRefusals(t *testing.T) {
	f := newContinuityCollectionFixture(t)
	_, server := continuityTestHost(t, f.base, "/manager")
	base := server.URL + "/manager/api/continuity/v1/collections/"
	for _, tc := range []struct {
		name, method, route, body, origin, authorization, contentType string
		status                                                        int
	}{
		{"method", "GET", "open", "", "", "Bearer " + string(f.owner), "application/json", 405},
		{"query", "POST", "open?private=1", continuityCollectionBody(t, "open", f.id, 0), "", "Bearer " + string(f.owner), "application/json", 400},
		{"origin", "POST", "open", continuityCollectionBody(t, "open", f.id, 0), "https://not-the-host.invalid", "Bearer " + string(f.owner), "application/json", 400},
		{"deployment-key", "POST", "open", continuityCollectionBody(t, "open", f.id, 0), "", "Bearer " + continuityTestAdmin, "application/json", 401},
		{"content-type", "POST", "open", continuityCollectionBody(t, "open", f.id, 0), "", "Bearer " + string(f.owner), "text/plain", 400},
		{"oversized", "POST", "open", strings.Repeat("x", continuityBodyLimit+1), "", "Bearer " + string(f.owner), "application/json", 400},
		{"canonical-mutate", "POST", "mutate", `{"version":1,"kind":"private"}`, "", "Bearer " + string(f.owner), "application/json", 400},
		{"unknown", "POST", "unknown", `{}`, "", "Bearer " + string(f.owner), "application/json", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(tc.method, base+tc.route, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Content-Type", tc.contentType)
			r.Header.Set("Authorization", tc.authorization)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			response, err := server.Client().Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != tc.status || response.Header.Get("Cache-Control") != "no-store" || bytes.Contains(body, f.owner) || bytes.Contains(body, []byte("private")) {
				t.Fatal("transport refusal incorrect or reflected input")
			}
		})
	}
}

func TestContinuityCollectionsSchemaRefusalAndOutputAudit(t *testing.T) {
	t.Run("schema3-no-upgrade", func(t *testing.T) {
		f := newContinuityTestFixture(t)
		_, server := continuityTestHost(t, f, "")
		status, body, _ := continuityTestPOST(t, server.Client(), server.URL+"/api/continuity/v1/collections/directory", f.a.Bearer, continuityCollectionBody(t, "directory", "", 1))
		if status != 503 || string(body) != `{"version":1,"error":"unavailable"}` {
			t.Fatal("schema3 silently accepted collection query")
		}
	})
	t.Run("attempted-write-no-second-body", func(t *testing.T) {
		f := newContinuityCollectionFixture(t)
		c, _ := continuityTestHost(t, f.base, "")
		var audit bytes.Buffer
		prior := log.Writer()
		log.SetOutput(&audit)
		defer log.SetOutput(prior)
		for _, mode := range []string{"partial", "full"} {
			r := httptest.NewRequest(http.MethodPost, "http://localhost/api/continuity/v1/collections/open", strings.NewReader(continuityCollectionBody(t, "open", f.id, 0)))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+string(f.owner))
			w := &continuityFailWriter{header: make(http.Header), mode: mode}
			c.serveContinuityHTTP(w, r, "/api/continuity/v1/collections/", 1, true)
			if w.writes != 1 || bytes.Contains(w.body, []byte(`"error"`)) {
				t.Fatal("attempted collection output appended second response")
			}
		}
		for _, private := range []string{string(f.owner), string(f.id), "Synthetic shared project", "transport-private"} {
			if strings.Contains(audit.String(), private) {
				t.Fatal("collection audit disclosed private data")
			}
		}
		if !strings.Contains(audit.String(), `"route":"collections.open"`) {
			t.Fatal("collection audit route not distinguished")
		}
	})
}

func TestContinuityCollectionsPartialBodyAndCloseDrain(t *testing.T) {
	f := newContinuityCollectionFixture(t)
	c, server := continuityTestHost(t, f.base, "/manager")
	host := strings.TrimPrefix(server.URL, "http://")
	connection, err := net.DialTimeout("tcp", host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(connection, "POST /manager/api/continuity/v1/collections/open HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{", host, f.owner); err != nil {
		t.Fatal("partial fixture write")
	}
	continuityWaitActive(t, c, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = c.Close(ctx)
	cancel()
	if !errors.Is(err, catalog.ErrDiscoveryDraining) {
		t.Fatal("Close claimed completed active HTTP drain")
	}
	response, readErr := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if readErr == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	continuityWaitActive(t, c, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal("collection handler did not drain")
	}
}
