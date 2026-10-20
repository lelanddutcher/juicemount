package manager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const continuityPreferenceOther catalog.SubjectID = "sub_44444444444444444444444444444444"

func newContinuityPreferenceFixture(t *testing.T) (continuityTestFixture, []byte, []byte) {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := continuityTestFixture{dir: dir, path: filepath.Join(dir, "preferences.sqlite")}
	var err error
	f.store, err = catalog.CreatePreferences(context.Background(), f.path, continuityTestLibrary)
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
	label := "synthetic owner"
	bootstrap, err := f.owner.Bootstrap(context.Background(), continuityTestAdminRequest("bootstrap", "", "bootstrap", catalog.BootstrapRequest{OwnerSubjectID: continuityTestOwner, CreateLabel: &label}))
	if err != nil {
		t.Fatal(err)
	}
	f.incarnation = bootstrap.Result.Incarnation
	_, err = f.store.Apply(context.Background(), catalog.Operation{Version: 1, LibraryID: continuityTestLibrary, ID: "assets", Actor: "synthetic fixture", Changes: []catalog.Change{
		{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestA, Label: "alpha"}},
		{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestB, Label: "hidden"}},
		{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestC, Label: "zulu"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	actions := []string{"activity.record", "assets.discover"}
	serviceActions := []string{"assets.discover"}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: continuityTestLibrary, ID: "preference-readers", Administration: "local_owner", OperatorSubjectID: continuityTestOwner}
	for index, subject := range []catalog.SubjectID{continuityTestReader, continuityPreferenceOther, continuityTestTool} {
		kind := "person"
		grantActions := actions
		if subject == continuityTestTool {
			kind = "service"
			grantActions = serviceActions
		}
		policy.Changes = append(policy.Changes, catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: subject, Value: catalog.SubjectValue{Kind: kind, Label: "synthetic reader", Enabled: true}}))
		for i, asset := range []continuity.AssetID{continuityTestA, continuityTestC} {
			policy.Changes = append(policy.Changes, catalog.NewPolicyChange("put_grant", catalog.PutGrant{ID: fmt.Sprintf("preference-grant-%d-%d", index, i), Value: catalog.GrantValue{SubjectID: subject, Scope: catalog.Scope{Kind: "asset", AssetID: asset}, Actions: grantActions}}))
		}
	}
	if _, err := f.owner.ApplyPolicy(context.Background(), continuityTestAdminRequest(policy.ID, f.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	issue := func(id string, subject catalog.SubjectID, ceiling []string) catalog.IssuanceResult {
		result, err := f.owner.IssueCredential(context.Background(), continuityTestAdminRequest(id, f.incarnation, "issue_credential", catalog.IssueCredentialRequest{SubjectID: subject, Scope: catalog.Scope{Kind: "library"}, Actions: ceiling, ExpiresUnix: time.Now().Add(time.Hour).Unix()}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { clear(result.Bearer) })
		return result
	}
	f.a = issue("preference-a", continuityTestReader, actions)
	f.b = issue("preference-b", continuityPreferenceOther, actions)
	tool := issue("preference-tool", continuityTestTool, serviceActions)
	limited := issue("preference-limited", continuityTestReader, []string{"assets.discover"})
	return f, tool.Bearer, limited.Bearer
}

func continuityPreferenceBody(id string, asset continuity.AssetID, token *string, expires int64) string {
	data, _ := json.Marshal(struct {
		Version   int                  `json:"version"`
		Library   continuity.LibraryID `json:"library_id"`
		Operation string               `json:"operation_id"`
		Asset     continuity.AssetID   `json:"asset_id"`
		Expected  *string              `json:"expected_token"`
		Expires   int64                `json:"expires_unix"`
	}{2, continuityTestLibrary, id, asset, token, expires})
	return string(data)
}

func continuityPreferenceStateBody(asset continuity.AssetID) string {
	return fmt.Sprintf(`{"version":2,"library_id":%q,"asset_id":%q}`, continuityTestLibrary, asset)
}

func continuityPreferenceSearchBody(version, size int) string {
	return fmt.Sprintf(`{"version":%d,"library_id":%q,"text":"","context_asset_ids":[],"page_size":%d}`, version, continuityTestLibrary, size)
}

func continuityPreferenceToken(t *testing.T, data []byte, receipt bool) *string {
	t.Helper()
	var fields map[string]json.RawMessage
	wantFields := []string{"version", "asset_id", "selection_token"}
	if receipt {
		wantFields = []string{"version", "operation_id", "kind", "selection_token", "receipt_scope"}
	}
	if json.Unmarshal(data, &fields) != nil || len(fields) != len(wantFields) {
		t.Fatal("unexpected preference response fields")
	}
	for _, name := range wantFields {
		if _, exists := fields[name]; !exists {
			t.Fatal("missing preference response field")
		}
	}
	var decoded struct {
		Version int     `json:"version"`
		Token   *string `json:"selection_token"`
		Scope   string  `json:"receipt_scope"`
	}
	if json.Unmarshal(data, &decoded) != nil || decoded.Version != 2 || receipt && decoded.Scope != "operation_result" {
		t.Fatal("invalid preference response")
	}
	if decoded.Token != nil && !continuityOpaqueToken(*decoded.Token, "sel1_") {
		t.Fatal("invalid opaque selection token")
	}
	for _, forbidden := range []string{"subject_id", "generation", "person_order", "expires_unix", "selected_unix", "incarnation", "selector"} {
		if bytes.Contains(data, []byte(forbidden)) {
			t.Fatalf("preference response disclosed %s", forbidden)
		}
	}
	return decoded.Token
}

func TestContinuityPreferencesProductionTCP(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			f, tool, limited := newContinuityPreferenceFixture(t)
			c, server := continuityTestHost(t, f, prefix)
			base := server.URL + prefix + "/api/continuity/"
			expires := time.Now().Add(10 * time.Minute).Unix()
			post := func(route string, bearer []byte, body string, want int) []byte {
				status, data, _ := continuityTestPOST(t, server.Client(), base+route, bearer, body)
				if status != want {
					t.Fatalf("%s status %d want %d", route, status, want)
				}
				return data
			}
			v1Before := post("v1/search", f.a.Bearer, continuityPreferenceSearchBody(1, 100), 200)
			if token := continuityPreferenceToken(t, post("v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC), 200), false); token != nil {
				t.Fatal("unexpected initial preference")
			}
			request := continuityPreferenceBody("select-c", continuityTestC, nil, expires)
			first := post("v2/select", f.a.Bearer, request, 200)
			firstToken := continuityPreferenceToken(t, first, true)
			if firstToken == nil {
				t.Fatal("select returned no token")
			}
			if retry := post("v2/select", f.a.Bearer, request, 200); !bytes.Equal(first, retry) {
				t.Fatal("exact retry changed original receipt")
			}
			if changed := post("v2/select", f.a.Bearer, continuityPreferenceBody("select-c", continuityTestC, nil, expires+1), 409); string(changed) != `{"version":1,"error":"conflict"}` {
				t.Fatal("changed retry was not fixed conflict")
			}
			status := post("v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC), 200)
			if token := continuityPreferenceToken(t, status, false); token == nil || *token != *firstToken {
				t.Fatal("current token was not recoverable")
			}
			if token := continuityPreferenceToken(t, post("v2/preference", f.b.Bearer, continuityPreferenceStateBody(continuityTestC), 200), false); token != nil {
				t.Fatal("another person's preference leaked")
			}
			for _, bearer := range [][]byte{tool, limited} {
				for _, route := range []string{"select", "clear", "preference"} {
					body := request
					if route == "clear" {
						body = continuityPreferenceBody("clear-denied", continuityTestC, firstToken, expires)
					}
					if route == "preference" {
						body = continuityPreferenceStateBody(continuityTestC)
					}
					if data := post("v2/"+route, bearer, body, 404); string(data) != `{"version":1,"error":"not_found"}` {
						t.Fatal("service/ceiling denial differs from hidden asset")
					}
				}
			}
			hidden := post("v2/select", f.a.Bearer, continuityPreferenceBody("hidden", continuityTestB, nil, expires), 404)
			missing := post("v2/select", f.a.Bearer, continuityPreferenceBody("missing", "asset_99999999999999999999999999999999", nil, expires), 404)
			if !bytes.Equal(hidden, missing) {
				t.Fatal("hidden and missing preference assets differ")
			}
			for _, entry := range []struct {
				bearer   []byte
				selected bool
			}{{f.a.Bearer, true}, {f.b.Bearer, false}, {tool, false}} {
				data := post("v2/search", entry.bearer, continuityPreferenceSearchBody(2, 100), 200)
				var envelope struct {
					Version int    `json:"version"`
					Sort    string `json:"sort_version"`
					Items   []struct {
						ID      continuity.AssetID `json:"asset_id"`
						Reasons []string           `json:"reasons"`
					} `json:"items"`
				}
				if json.Unmarshal(data, &envelope) != nil || envelope.Version != 2 || envelope.Sort != "discovery2" || len(envelope.Items) != 2 {
					t.Fatal("invalid v2 search envelope")
				}
				want := continuityTestA
				if entry.selected {
					want = continuityTestC
				}
				if envelope.Items[0].ID != want || bytes.Contains(data, []byte("declared_selection")) != entry.selected {
					t.Fatal("own selection ordering/isolation failed")
				}
			}
			if after := post("v1/search", f.a.Bearer, continuityPreferenceSearchBody(1, 100), 200); !bytes.Equal(v1Before, after) {
				t.Fatal("v1 search changed with private preference")
			}
			second := continuityPreferenceToken(t, post("v2/select", f.a.Bearer, continuityPreferenceBody("select-c-again", continuityTestC, nil, expires), 200), true)
			if second == nil || *second == *firstToken {
				t.Fatal("replacement selection reused token")
			}
			if retry := post("v2/select", f.a.Bearer, request, 200); !bytes.Equal(first, retry) {
				t.Fatal("retry ceased being historical operation result")
			}
			post("v2/clear", f.a.Bearer, continuityPreferenceBody("stale-clear", continuityTestC, firstToken, expires), 409)
			if expired := post("v2/select", f.a.Bearer, continuityPreferenceBody("expired", continuityTestA, nil, time.Now().Unix()-1), 409); string(expired) != `{"version":1,"error":"expired_request"}` {
				t.Fatal("expired request not mapped to fixed 409")
			}
			// Close every SQLite owner before constructing the replacement host.
			server.Close()
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			f.dir = filepath.Join(f.dir, "restart")
			if err := os.Mkdir(f.dir, 0700); err != nil {
				t.Fatal(err)
			}
			_, restarted := continuityTestHost(t, f, prefix)
			server = restarted
			base = server.URL + prefix + "/api/continuity/"
			if token := continuityPreferenceToken(t, post("v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC), 200), false); token == nil || *token != *second {
				t.Fatal("restart lost current token")
			}
			clearBody := continuityPreferenceBody("clear-current", continuityTestC, second, expires)
			cleared := post("v2/clear", f.a.Bearer, clearBody, 200)
			if continuityPreferenceToken(t, cleared, true) != nil {
				t.Fatal("clear receipt retained current token")
			}
			if retry := post("v2/clear", f.a.Bearer, clearBody, 200); !bytes.Equal(cleared, retry) {
				t.Fatal("clear retry changed receipt")
			}
			if continuityPreferenceToken(t, post("v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC), 200), false) != nil {
				t.Fatal("clear retained preference")
			}
			post("v1/asset", f.a.Bearer, continuityAssetBody(continuityTestC), 200)
		})
	}
}

func TestContinuityPreferencesRouteVersionBinding(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			f, _, _ := newContinuityPreferenceFixture(t)
			_, server := continuityTestHost(t, f, prefix)
			base := server.URL + prefix + "/api/continuity/"
			for _, version := range []int{1, 2} {
				status, data, _ := continuityTestPOST(t, server.Client(), fmt.Sprintf("%sv%d/search", base, version), f.a.Bearer, continuityPreferenceSearchBody(version, 1))
				var envelope struct {
					Next string `json:"next_page_token"`
				}
				if status != 200 || json.Unmarshal(data, &envelope) != nil || envelope.Next == "" {
					t.Fatal("missing page token")
				}
				other := 3 - version
				wrong := fmt.Sprintf(`{"version":%d,"library_id":%q,"token":%q}`, other, continuityTestLibrary, envelope.Next)
				status, denied, _ := continuityTestPOST(t, server.Client(), fmt.Sprintf("%sv%d/page", base, other), f.a.Bearer, wrong)
				if status != 409 || string(denied) != `{"version":1,"error":"restart_query"}` {
					t.Fatal("wrong-version page token disclosed")
				}
				right := fmt.Sprintf(`{"version":%d,"library_id":%q,"token":%q}`, version, continuityTestLibrary, envelope.Next)
				status, _, _ = continuityTestPOST(t, server.Client(), fmt.Sprintf("%sv%d/page", base, version), f.a.Bearer, right)
				if status != 200 {
					t.Fatal("wrong-version presentation retired original token")
				}
			}
			status, data, _ := continuityTestPOST(t, server.Client(), base+"v2/search", f.a.Bearer, continuityPreferenceSearchBody(2, 1))
			var before struct {
				Next string `json:"next_page_token"`
			}
			if status != 200 || json.Unmarshal(data, &before) != nil || before.Next == "" {
				t.Fatal("missing preference-bound page")
			}
			page := fmt.Sprintf(`{"version":2,"library_id":%q,"token":%q}`, continuityTestLibrary, before.Next)
			expires := time.Now().Add(10 * time.Minute).Unix()
			status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/select", f.b.Bearer, continuityPreferenceBody("other-select", continuityTestC, nil, expires))
			if status != 200 {
				t.Fatal("other person's select failed")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/page", f.a.Bearer, page)
			if status != 200 {
				t.Fatal("another person's select invalidated caller page")
			}
			status, _, _ = continuityTestPOST(t, server.Client(), base+"v2/select", f.a.Bearer, continuityPreferenceBody("own-select", continuityTestC, nil, expires))
			if status != 200 {
				t.Fatal("own select failed")
			}
			status, data, _ = continuityTestPOST(t, server.Client(), base+"v2/page", f.a.Bearer, page)
			if status != 409 || string(data) != `{"version":1,"error":"restart_query"}` {
				t.Fatal("own select released stale ranked page")
			}
		})
	}
}

func TestContinuityPreferencesStrictHTTP(t *testing.T) {
	f, _, _ := newContinuityPreferenceFixture(t)
	_, server := continuityTestHost(t, f, "")
	base := server.URL + "/api/continuity/"
	expires := time.Now().Add(10 * time.Minute).Unix()
	valid := continuityPreferenceBody("strict-select", continuityTestC, nil, expires)
	for _, tc := range []struct {
		name, route, body string
		status            int
		mutate            func(*http.Request)
	}{
		{"missing-token", "v2/select", strings.Replace(valid, `"expected_token":null,`, "", 1), 400, nil},
		{"select-string-token", "v2/select", strings.Replace(valid, `"expected_token":null`, `"expected_token":"sel1_`+strings.Repeat("a", 64)+`"`, 1), 400, nil},
		{"clear-null", "v2/clear", valid, 400, nil},
		{"duplicate-token", "v2/select", strings.Replace(valid, `"expected_token":null`, `"expected_token":null,"expected_token":null`, 1), 400, nil},
		{"client-kind", "v2/select", strings.Replace(valid, `"version":2`, `"version":2,"kind":"clear"`, 1), 400, nil},
		{"client-subject", "v2/select", strings.Replace(valid, `"version":2`, `"version":2,"subject_id":"private-value"`, 1), 400, nil},
		{"fractional-expiry", "v2/select", strings.Replace(valid, fmt.Sprintf(`"expires_unix":%d`, expires), `"expires_unix":0.5`, 1), 400, nil},
		{"null-expiry", "v2/select", strings.Replace(valid, fmt.Sprintf(`"expires_unix":%d`, expires), `"expires_unix":null`, 1), 400, nil},
		{"old-version", "v2/select", strings.Replace(valid, `"version":2`, `"version":1`, 1), 400, nil},
		{"null-operation", "v2/select", strings.Replace(valid, `"operation_id":"strict-select"`, `"operation_id":null`, 1), 400, nil},
		{"bad-operation", "v2/select", strings.Replace(valid, "strict-select", "private value", 1), 400, nil},
		{"trailing", "v2/select", valid + `{}`, 400, nil},
		{"invalid-utf8", "v2/select", valid + string([]byte{0xff}), 400, nil},
		{"body-limit", "v2/select", valid + strings.Repeat(" ", continuityBodyLimit), 400, nil},
		{"state-extra", "v2/preference", valid, 400, nil},
		{"v1-select-absent", "v1/select", valid, 404, nil},
		{"v2-asset-absent", "v2/asset", continuityPreferenceStateBody(continuityTestC), 404, nil},
		{"query", "v2/select?private=value", valid, 400, nil},
		{"get", "v2/select", valid, 405, func(r *http.Request) { r.Method = http.MethodGet }},
		{"origin", "v2/select", valid, 400, func(r *http.Request) { r.Header.Set("Origin", "https://other.invalid") }},
		{"cross-site", "v2/select", valid, 400, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }},
		{"content-type", "v2/select", valid, 400, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }},
		{"encoding", "v2/select", valid, 400, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"no-bearer", "v2/select", valid, 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"duplicate-bearer", "v2/select", valid, 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+string(f.a.Bearer)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPost, base+tc.route, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+string(f.a.Bearer))
			if tc.mutate != nil {
				tc.mutate(r)
			}
			response, err := server.Client().Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != tc.status || response.Header.Get("Cache-Control") != "no-store" {
				t.Fatalf("status %d want %d, read %v", response.StatusCode, tc.status, err)
			}
			if bytes.Contains(data, []byte("strict-select")) || bytes.Contains(data, f.a.Bearer) || bytes.Contains(data, []byte("private")) {
				t.Fatal("invalid input echoed")
			}
		})
	}
	status, data, _ := continuityTestPOST(t, server.Client(), base+"v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC))
	if status != 200 || continuityPreferenceToken(t, data, false) != nil {
		t.Fatal("rejected transport request changed preference")
	}
}

func TestContinuityPreferencesSchema3RefusesV2WithoutUpgrade(t *testing.T) {
	f := newContinuityTestFixture(t)
	_, server := continuityTestHost(t, f, "")
	for _, route := range []string{"search", "select", "clear", "preference"} {
		body := continuityPreferenceStateBody(continuityTestA)
		if route == "search" {
			body = continuityPreferenceSearchBody(2, 100)
		}
		if route == "select" {
			body = continuityPreferenceBody("select", continuityTestA, nil, time.Now().Add(10*time.Minute).Unix())
		}
		if route == "clear" {
			token := "sel1_" + strings.Repeat("a", 64)
			body = continuityPreferenceBody("clear", continuityTestA, &token, time.Now().Add(10*time.Minute).Unix())
		}
		status, data, _ := continuityTestPOST(t, server.Client(), server.URL+"/api/continuity/v2/"+route, f.a.Bearer, body)
		if status != 503 || string(data) != `{"version":1,"error":"unavailable"}` {
			t.Fatalf("schema3 v2 %s status %d", route, status)
		}
	}
	status, _, _ := continuityTestPOST(t, server.Client(), server.URL+"/api/continuity/v1/asset", f.a.Bearer, continuityAssetBody(continuityTestA))
	if status != 200 {
		t.Fatal("schema3 v1 capability regressed")
	}
}

func TestContinuityPreferencesOutputFailureKeepsRetryAndRedactsAudit(t *testing.T) {
	f, _, _ := newContinuityPreferenceFixture(t)
	c, server := continuityTestHost(t, f, "")
	var audit bytes.Buffer
	prior := log.Writer()
	log.SetOutput(&audit)
	defer log.SetOutput(prior)
	var secretTokens []string
	for _, mode := range []string{"partial", "full"} {
		t.Run(mode, func(t *testing.T) {
			body := continuityPreferenceBody("private-operation-"+mode, continuityTestC, nil, time.Now().Add(10*time.Minute).Unix())
			r := httptest.NewRequest(http.MethodPost, "http://localhost/api/continuity/v2/select", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+string(f.a.Bearer))
			w := &continuityFailWriter{header: make(http.Header), mode: mode}
			c.serveHTTPVersion(w, r, "/api/continuity/v2/", 2)
			if w.writes != 1 || bytes.Contains(w.body, []byte(`"error"`)) {
				t.Fatal("failed output appended another response")
			}
			status, retry, _ := continuityTestPOST(t, server.Client(), server.URL+"/api/continuity/v2/select", f.a.Bearer, body)
			if status != 200 {
				t.Fatal("uncertain output did not retain retry")
			}
			token := continuityPreferenceToken(t, retry, true)
			if token == nil {
				t.Fatal("retry lacked token")
			}
			secretTokens = append(secretTokens, *token)
			status, state, _ := continuityTestPOST(t, server.Client(), server.URL+"/api/continuity/v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC))
			current := continuityPreferenceToken(t, state, false)
			if status != 200 || current == nil || *current != *token {
				t.Fatal("retry changed committed current selection")
			}
		})
	}
	for _, forbidden := range append(secretTokens, string(f.a.Bearer), string(continuityTestC), "private-operation", "transport-private", "zulu") {
		if strings.Contains(audit.String(), forbidden) {
			t.Fatal("preference audit disclosed private input/output")
		}
	}
}

func TestContinuityPreferencesRevokedCredentialCannotDiscloseRetryOrState(t *testing.T) {
	f, _, _ := newContinuityPreferenceFixture(t)
	_, server := continuityTestHost(t, f, "")
	base := server.URL + "/api/continuity/v2/"
	body := continuityPreferenceBody("before-revoke", continuityTestC, nil, time.Now().Add(10*time.Minute).Unix())
	status, data, _ := continuityTestPOST(t, server.Client(), base+"select", f.a.Bearer, body)
	if status != 200 || continuityPreferenceToken(t, data, true) == nil {
		t.Fatal("initial select failed")
	}
	_, err := f.owner.RevokeCredential(context.Background(), continuityTestAdminRequest("revoke-preference-reader", f.incarnation, "revoke_credential", catalog.RevokeCredentialRequest{Selector: f.a.Receipt.Result.Selector, ExpectedGeneration: 1}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ route, body string }{{"select", body}, {"preference", continuityPreferenceStateBody(continuityTestC)}, {"search", continuityPreferenceSearchBody(2, 100)}} {
		status, data, _ := continuityTestPOST(t, server.Client(), base+tc.route, f.a.Bearer, tc.body)
		if status != 401 || string(data) != `{"version":1,"error":"unauthenticated"}` {
			t.Fatal("revoked credential disclosed preference data")
		}
	}
}

func TestContinuityPreferencesPartialBodyUsesTransportDeadline(t *testing.T) {
	f, _, _ := newContinuityPreferenceFixture(t)
	c, server := continuityTestHost(t, f, "/manager")
	host := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.DialTimeout("tcp", host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = fmt.Fprintf(conn, "POST /manager/api/continuity/v2/select HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{", host, f.a.Bearer)
	if err != nil {
		t.Fatal("partial request write failed")
	}
	continuityWaitActive(t, c, 1)
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err == nil {
		if response.StatusCode != 504 {
			t.Fatalf("partial body status %d", response.StatusCode)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond || elapsed > 4500*time.Millisecond {
		t.Fatal("v2 partial body did not use the bounded read deadline")
	}
	continuityWaitActive(t, c, 0)
	status, data, _ := continuityTestPOST(t, server.Client(), server.URL+"/manager/api/continuity/v2/preference", f.a.Bearer, continuityPreferenceStateBody(continuityTestC))
	if status != 200 || continuityPreferenceToken(t, data, false) != nil {
		t.Fatal("partial body mutated preference")
	}
}
