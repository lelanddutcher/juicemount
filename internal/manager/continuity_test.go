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
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const continuityTestLibrary continuity.LibraryID = "lib_11111111111111111111111111111111"
const continuityTestA continuity.AssetID = "asset_11111111111111111111111111111111"
const continuityTestB continuity.AssetID = "asset_22222222222222222222222222222222"
const continuityTestC continuity.AssetID = "asset_33333333333333333333333333333333"
const continuityTestOwner catalog.SubjectID = "sub_11111111111111111111111111111111"
const continuityTestReader catalog.SubjectID = "sub_22222222222222222222222222222222"
const continuityTestTool catalog.SubjectID = "sub_33333333333333333333333333333333"
const continuityTestAdmin = "synthetic-manager-deployment-key-000000000000"

type continuityTestFixture struct {
	dir         string
	path        string
	store       *catalog.Store
	owner       *catalog.LocalOwnerService
	incarnation string
	a, b        catalog.IssuanceResult
}

func continuityTestAdminRequest(id, incarnation, kind string, value any) catalog.AdminRequest {
	data, _ := json.Marshal(value)
	return catalog.AdminRequest{Version: 1, LibraryID: continuityTestLibrary, ID: id, ExpectedIncarnation: incarnation, Kind: kind, Value: data}
}

func newContinuityTestFixture(t *testing.T) continuityTestFixture {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	f := continuityTestFixture{dir: dir, path: filepath.Join(dir, "catalog.sqlite")}
	var err error
	f.store, err = catalog.CreateAuthority(context.Background(), f.path, continuityTestLibrary)
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
	_, err = f.store.Apply(context.Background(), catalog.Operation{
		Version: 1, LibraryID: continuityTestLibrary, ID: "assets", Actor: "synthetic fixture",
		Changes: []catalog.Change{
			{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestA, Label: "visible-a"}},
			{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestB, Label: "hidden-b"}},
			{CreateAsset: &catalog.CreateAsset{AssetID: continuityTestC, Label: "visible-c"}},
			{PutRelationship: &catalog.PutRelationship{ID: "hidden-edge", Value: catalog.RelationshipValue{
				SourceAssetID: continuityTestA, TargetAssetID: continuityTestB, Predicate: "related",
				Provenance: catalog.Claimed, Source: "synthetic fixture"}}},
			{PutLocation: &catalog.PutLocation{ID: "observation-a", Value: catalog.LocationValue{
				AssetID: continuityTestA, Source: "synthetic fixture", Locator: "observation:never-open"}}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	actions := []string{"assets.discover", "locations.read", "relationships.read", "revisions.read"}
	policy := catalog.PolicyOperation{Version: 1, LibraryID: continuityTestLibrary, ID: "readers", Administration: "local_owner", OperatorSubjectID: continuityTestOwner}
	policy.Changes = append(policy.Changes,
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: continuityTestReader, Value: catalog.SubjectValue{Kind: "person", Label: "synthetic reader", Enabled: true}}),
		catalog.NewPolicyChange("create_subject", catalog.CreateSubject{SubjectID: continuityTestTool, Value: catalog.SubjectValue{Kind: "service", Label: "synthetic tool", Enabled: true}}))
	for i, entry := range []struct {
		subject catalog.SubjectID
		asset   continuity.AssetID
	}{
		{continuityTestReader, continuityTestA}, {continuityTestReader, continuityTestC}, {continuityTestTool, continuityTestB},
	} {
		policy.Changes = append(policy.Changes, catalog.NewPolicyChange("put_grant", catalog.PutGrant{
			ID: fmt.Sprintf("grant-%d", i), Value: catalog.GrantValue{SubjectID: entry.subject, Scope: catalog.Scope{Kind: "asset", AssetID: entry.asset}, Actions: actions}}))
	}
	if _, err := f.owner.ApplyPolicy(context.Background(), continuityTestAdminRequest("readers", f.incarnation, "apply_policy", catalog.ApplyPolicyRequest{Operation: policy})); err != nil {
		t.Fatal(err)
	}
	f.a, err = f.owner.IssueCredential(context.Background(), continuityTestAdminRequest("issue-a", f.incarnation, "issue_credential", catalog.IssueCredentialRequest{
		SubjectID: continuityTestReader, Scope: catalog.Scope{Kind: "library"}, Actions: actions, ExpiresUnix: time.Now().Add(time.Hour).Unix()}))
	if err != nil {
		t.Fatal(err)
	}
	f.b, err = f.owner.IssueCredential(context.Background(), continuityTestAdminRequest("issue-b", f.incarnation, "issue_credential", catalog.IssueCredentialRequest{
		SubjectID: continuityTestTool, Scope: catalog.Scope{Kind: "asset", AssetID: continuityTestB}, Actions: actions, ExpiresUnix: time.Now().Add(time.Hour).Unix()}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(f.a.Bearer); clear(f.b.Bearer) })
	return f
}

func isolateContinuityManager(t *testing.T) {
	t.Helper()
	// Never inherit deployment credentials, proxy paths, provider endpoints or
	// Link activation into a constructor test. Tests using Setenv are serial.
	for _, pair := range os.Environ() {
		name, _, _ := strings.Cut(pair, "=")
		for _, prefix := range []string{"JM_", "AWS_", "MINIO_", "RCLONE_", "TAILSCALE_", "TS_"} {
			if strings.HasPrefix(name, prefix) {
				t.Setenv(name, "")
				break
			}
		}
	}
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		t.Setenv(name, "")
	}
}

func continuityTestHost(t *testing.T, f continuityTestFixture, prefix string) (*Continuity, *httptest.Server) {
	t.Helper()
	isolateContinuityManager(t)
	stateDir := filepath.Join(f.dir, "manager")
	sourceDir, fuseDir := filepath.Join(f.dir, "source"), filepath.Join(f.dir, "ordinary-root")
	for _, dir := range []string{stateDir, sourceDir, fuseDir} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	statePath := filepath.Join(stateDir, "state.json")
	if err := os.WriteFile(statePath, []byte(`{"schema_version":2,"jobs":{},"order":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: f.path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	mux := http.NewServeMux()
	jobs := Register(mux, prefix, Config{
		JuiceFSBin: filepath.Join(f.dir, "must-not-execute-juicefs"), FUSEMount: fuseDir,
		SourceRoots: []string{sourceDir}, DestMount: "/jfs", StateFile: statePath, AdminKey: continuityTestAdmin,
	})
	t.Cleanup(jobs.StopAll)
	if err := c.RegisterRoutes(mux, prefix); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(mux)
	server.Client().Timeout = 5 * time.Second
	t.Cleanup(server.Close)
	for _, filename := range []string{"principals-store.json", "principals-store.manager-id"} {
		info, err := os.Stat(filepath.Join(stateDir, filename))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("isolated principal state %s: %v", filename, err)
		}
	}
	return c, server
}

func continuityTestPOST(t *testing.T, client *http.Client, target string, bearer []byte, body string) (int, []byte, http.Header) {
	t.Helper()
	r, err := http.NewRequest(http.MethodPost, target, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	if bearer != nil {
		r.Header.Set("Authorization", "Bearer "+string(bearer))
	}
	response, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("Content-Type") != "application/json" ||
		response.Header.Get("X-Content-Type-Options") != "nosniff" || response.Header.Get("ETag") != "" {
		t.Fatal("discovery response missing disclosure headers")
	}
	return response.StatusCode, data, response.Header
}

func continuityAssetBody(id continuity.AssetID) string {
	return fmt.Sprintf(`{"version":1,"library_id":%q,"asset_id":%q}`, continuityTestLibrary, id)
}

func TestContinuityProductionTCPScopesPagesAndRevocation(t *testing.T) {
	for _, prefix := range []string{"", "/manager"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			f := newContinuityTestFixture(t)
			_, server := continuityTestHost(t, f, prefix)
			base := server.URL + prefix + "/api/continuity/v1/"
			status, hidden, _ := continuityTestPOST(t, server.Client(), base+"asset", f.a.Bearer, continuityAssetBody(continuityTestB))
			if status != 404 {
				t.Fatalf("hidden asset status %d", status)
			}
			status, absent, _ := continuityTestPOST(t, server.Client(), base+"asset", f.a.Bearer, continuityAssetBody("asset_99999999999999999999999999999999"))
			if status != 404 || !bytes.Equal(hidden, absent) {
				t.Fatal("absent and hidden asset responses differ")
			}
			for _, entry := range []struct {
				bearer []byte
				id     continuity.AssetID
				label  string
			}{
				{f.a.Bearer, continuityTestA, "visible-a"}, {f.b.Bearer, continuityTestB, "hidden-b"},
			} {
				status, data, _ := continuityTestPOST(t, server.Client(), base+"asset", entry.bearer, continuityAssetBody(entry.id))
				if status != 200 || !bytes.Contains(data, []byte(entry.label)) {
					t.Fatalf("authorized asset status %d", status)
				}
				for _, forbidden := range []string{"sequence", "operation_id", "selector", "subject_id", "incarnation"} {
					if bytes.Contains(data, []byte(forbidden)) {
						t.Fatalf("internal field %s leaked", forbidden)
					}
				}
			}
			status, counts, _ := continuityTestPOST(t, server.Client(), base+"counts", f.a.Bearer, continuityAssetBody(continuityTestA))
			if status != 200 || !bytes.Contains(counts, []byte(`"relationship_count":0`)) || !bytes.Contains(counts, []byte(`"location_count":1`)) {
				t.Fatalf("scoped counts: status %d body %s", status, counts)
			}
			search := fmt.Sprintf(`{"version":1,"library_id":%q,"text":"","context_asset_ids":[],"page_size":1}`, continuityTestLibrary)
			status, data, _ := continuityTestPOST(t, server.Client(), base+"search", f.a.Bearer, search)
			if status != 200 || bytes.Contains(data, []byte(continuityTestB)) || bytes.Contains(data, []byte("hidden-b")) {
				t.Fatalf("scoped search status %d", status)
			}
			var envelope struct {
				Next  string `json:"next_page_token"`
				Count int    `json:"matched_count"`
			}
			if json.Unmarshal(data, &envelope) != nil || envelope.Next == "" || envelope.Count != 2 {
				t.Fatal("search did not create the expected authorized page")
			}
			page, _ := json.Marshal(map[string]any{"version": 1, "library_id": continuityTestLibrary, "token": envelope.Next})
			status, denied, _ := continuityTestPOST(t, server.Client(), base+"page", f.b.Bearer, string(page))
			if status != 409 || string(denied) != `{"version":1,"error":"restart_query"}` {
				t.Fatal("foreign page was not opaque")
			}
			status, replay1, _ := continuityTestPOST(t, server.Client(), base+"page", f.a.Bearer, string(page))
			if status != 200 || !bytes.Contains(replay1, []byte(continuityTestC)) {
				t.Fatal("foreign page request destroyed owner's state")
			}
			status, replay2, _ := continuityTestPOST(t, server.Client(), base+"page", f.a.Bearer, string(page))
			if status != 200 || !bytes.Equal(replay1, replay2) {
				t.Fatal("page replay changed bytes")
			}
			_, err := f.owner.RevokeCredential(context.Background(), continuityTestAdminRequest("revoke-a", f.incarnation, "revoke_credential",
				catalog.RevokeCredentialRequest{Selector: f.a.Receipt.Result.Selector, ExpectedGeneration: 1}))
			if err != nil {
				t.Fatal(err)
			}
			status, data, _ = continuityTestPOST(t, server.Client(), base+"page", f.a.Bearer, string(page))
			if status != 401 || string(data) != `{"version":1,"error":"unauthenticated"}` {
				t.Fatal("revocation failed through production HTTP")
			}
		})
	}
}

func TestContinuityProductionHTTPRejectsMalformedRequests(t *testing.T) {
	f := newContinuityTestFixture(t)
	_, server := continuityTestHost(t, f, "")
	base := server.URL + "/api/continuity/v1/"
	valid := continuityAssetBody(continuityTestA)
	for _, tc := range []struct {
		name, route, body string
		status            int
		mutate            func(*http.Request)
	}{
		{"missing-bearer", "asset", valid, 401, func(r *http.Request) {
			r.Header.Del("Authorization")
			r.Header.Set("X-JuiceMount-Admin-Key", continuityTestAdmin)
		}},
		{"duplicate-bearer", "asset", valid, 401, func(r *http.Request) { r.Header.Add("Authorization", r.Header.Get("Authorization")) }},
		{"combined-bearer", "asset", valid, 401, func(r *http.Request) { r.Header.Set("Authorization", r.Header.Get("Authorization")+", Bearer ignored") }},
		{"cookie-only", "asset", valid, 401, func(r *http.Request) { r.Header.Del("Authorization"); r.Header.Set("Cookie", "session=synthetic") }},
		{"query", "asset?key=synthetic", valid, 400, nil},
		{"unknown", "owner", valid, 404, nil},
		{"method", "asset", valid, 405, func(r *http.Request) { r.Method = http.MethodGet }},
		{"content-type", "asset", valid, 400, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }},
		{"encoding", "asset", valid, 400, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"cross-origin", "asset", valid, 400, func(r *http.Request) { r.Header.Set("Origin", "https://other.example") }},
		{"cross-site", "asset", valid, 400, func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") }},
		{"duplicate-origin", "asset", valid, 400, func(r *http.Request) { r.Header.Add("Origin", server.URL); r.Header.Add("Origin", server.URL) }},
		{"duplicate-fetch", "asset", valid, 400, func(r *http.Request) { r.Header.Add("Sec-Fetch-Site", "none"); r.Header.Add("Sec-Fetch-Site", "none") }},
		{"unknown-field", "asset", strings.TrimSuffix(valid, "}") + `,"subject_id":"synthetic"}`, 400, nil},
		{"duplicate-field", "asset", strings.TrimSuffix(valid, "}") + `,"version":1}`, 400, nil},
		{"missing-field", "asset", fmt.Sprintf(`{"version":1,"library_id":%q}`, continuityTestLibrary), 400, nil},
		{"null", "asset", fmt.Sprintf(`{"version":1,"library_id":%q,"asset_id":null}`, continuityTestLibrary), 400, nil},
		{"trailing", "asset", valid + `{}`, 400, nil},
		{"invalid-utf8", "asset", valid + string([]byte{0xff}), 400, nil},
		{"oversize", "asset", strings.Repeat(" ", continuityBodyLimit+1), 400, nil},
		{"bad-token", "page", fmt.Sprintf(`{"version":1,"library_id":%q,"token":"raw-id"}`, continuityTestLibrary), 400, nil},
		{"null-context", "search", fmt.Sprintf(`{"version":1,"library_id":%q,"text":"","context_asset_ids":null,"page_size":1}`, continuityTestLibrary), 400, nil},
		{"duplicate-context", "search", fmt.Sprintf(`{"version":1,"library_id":%q,"text":"","context_asset_ids":[%q,%q],"page_size":1}`, continuityTestLibrary, continuityTestA, continuityTestA), 400, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, base+tc.route, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+string(f.a.Bearer))
			request.Header.Set("Content-Type", "application/json")
			if tc.mutate != nil {
				tc.mutate(request)
			}
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != tc.status {
				t.Fatalf("status %d want %d: %v", response.StatusCode, tc.status, err)
			}
			if tc.status == 405 && response.Header.Get("Allow") != "POST" {
				t.Fatal("missing Allow")
			}
			var errorBody map[string]any
			if json.Unmarshal(body, &errorBody) != nil || len(errorBody) != 2 || errorBody["version"] != float64(1) {
				t.Fatal("noncanonical error envelope")
			}
			if bytes.Contains(body, f.a.Bearer) {
				t.Fatal("response leaked bearer")
			}
		})
	}
}

func TestContinuityPrepareFailsClosedAndDoesNotCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing.sqlite")
	for _, cfg := range []ContinuityConfig{
		{CatalogPath: path, ManagerAdminKey: continuityTestAdmin},
		{LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin},
		{CatalogPath: path, LibraryID: string(continuityTestLibrary)},
		{CatalogPath: path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: strings.Repeat(" ", 40)},
		{CatalogPath: path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: "cHaNgEmE" + strings.Repeat("x", 32)},
		{CatalogPath: path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin},
	} {
		if c, err := PrepareContinuity(context.Background(), cfg); err == nil || c != nil {
			t.Fatal("invalid preparation succeeded")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("preparation created catalog: %v", err)
		}
	}
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{})
	if err != nil || c != nil || c.RegisterRoutes(nil, "bad") != nil || c.Close(context.Background()) != nil {
		t.Fatal("disabled compatibility")
	}
	for _, authority := range []bool{false, true} {
		p := filepath.Join(t.TempDir(), "inactive.sqlite")
		var s *catalog.Store
		if authority {
			s, err = catalog.CreateAuthority(context.Background(), p, continuityTestLibrary)
		} else {
			s, err = catalog.Create(p, continuityTestLibrary)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		if c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: p, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin}); err == nil || c != nil {
			t.Fatal("unready authority was adopted")
		}
	}
}

func TestContinuityCloseRetainsCatalogUntilBodyDrain(t *testing.T) {
	f := newContinuityTestFixture(t)
	c, server := continuityTestHost(t, f, "")
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "POST /api/continuity/v1/asset HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{", strings.TrimPrefix(server.URL, "http://"), f.a.Bearer)
	if err != nil {
		t.Fatal("write private request")
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		active := c.active
		c.mu.Unlock()
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("body handler never admitted")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = c.Close(ctx)
	cancel()
	if !errors.Is(err, catalog.ErrDiscoveryDraining) {
		t.Fatalf("close did not preserve incomplete drain: %v", err)
	}
	if _, err := c.store.LibraryID(); err != nil {
		t.Fatalf("closed catalog under active body: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.store.LibraryID(); !errors.Is(err, catalog.ErrClosed) {
		t.Fatal("completed drain retained catalog")
	}
}

type continuityFailWriter struct {
	header http.Header
	writes int
	mode   string
	body   []byte
}

func (w *continuityFailWriter) Header() http.Header              { return w.header }
func (w *continuityFailWriter) WriteHeader(int)                  {}
func (w *continuityFailWriter) SetReadDeadline(time.Time) error  { return nil }
func (w *continuityFailWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *continuityFailWriter) Write(p []byte) (int, error) {
	w.writes++
	n := len(p)
	if w.mode == "partial" {
		n /= 2
	}
	w.body = append(w.body, p[:n]...)
	return n, errors.New("synthetic transport-private error")
}

func TestContinuityWriteFailureNeverAppendsErrorOrLogsSecrets(t *testing.T) {
	f := newContinuityTestFixture(t)
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: f.path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	var audit bytes.Buffer
	prior := log.Writer()
	log.SetOutput(&audit)
	defer log.SetOutput(prior)
	for _, mode := range []string{"partial", "full"} {
		t.Run(mode, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "http://localhost/api/continuity/v1/asset", strings.NewReader(continuityAssetBody(continuityTestA)))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+string(f.a.Bearer))
			w := &continuityFailWriter{header: make(http.Header), mode: mode}
			c.serveHTTP(w, r, "/api/continuity/v1/")
			if w.writes != 1 || bytes.Contains(w.body, []byte(`"error"`)) {
				t.Fatal("transport error appended a second disclosure")
			}
		})
	}
	if bytes.Contains(audit.Bytes(), f.a.Bearer) || bytes.Contains(audit.Bytes(), []byte("transport-private")) || bytes.Contains(audit.Bytes(), []byte("visible-a")) {
		t.Fatal("audit disclosed secret/body/raw error")
	}
	if !bytes.Contains(audit.Bytes(), []byte(f.a.Receipt.Result.Selector)) {
		t.Fatal("audit missing nonsecret authenticated selector")
	}
}

func TestContinuityConcurrentCloseHonorsEachDeadline(t *testing.T) {
	f := newContinuityTestFixture(t)
	c, server := continuityTestHost(t, f, "")
	conn := continuityPartialConnection(t, server, f.a.Bearer)
	defer conn.Close()
	continuityWaitActive(t, c, 1)
	done := make(chan error, 1)
	go func() { done <- c.Close(context.Background()) }()
	joined := false
	t.Cleanup(func() {
		conn.Close()
		if !joined {
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(3 * time.Second):
				t.Error("original close did not join")
			}
		}
	})
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		closing := c.closing
		c.mu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first close did not acquire ownership")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	start := time.Now()
	err := c.Close(ctx)
	cancel()
	if !errors.Is(err, catalog.ErrDiscoveryDraining) || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("concurrent close ignored its deadline: %v", err)
	}
	conn.Close()
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("original close did not drain")
	}
}

func continuityPartialConnection(t *testing.T, server *httptest.Server, bearer []byte) net.Conn {
	t.Helper()
	host := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.DialTimeout("tcp", host, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	auth := ""
	if bearer != nil {
		auth = "Authorization: Bearer " + string(bearer) + "\r\n"
	}
	_, err = fmt.Fprintf(conn, "POST /api/continuity/v1/asset HTTP/1.1\r\nHost: %s\r\n%sContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{", host, auth)
	if err != nil {
		conn.Close()
		t.Fatal("partial request failed")
	}
	return conn
}

func continuityWaitActive(t *testing.T, c *Continuity, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		active := c.active
		c.mu.Unlock()
		if active == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admitted handlers %d want %d", active, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestContinuityEarlyErrorsAndBusyKeepTransportDeadline(t *testing.T) {
	for _, busy := range []bool{false, true} {
		t.Run(fmt.Sprintf("busy=%v", busy), func(t *testing.T) {
			f := newContinuityTestFixture(t)
			c, server := continuityTestHost(t, f, "")
			var holders []net.Conn
			defer func() {
				for _, conn := range holders {
					conn.Close()
				}
			}()
			if busy {
				for i := 0; i < 16; i++ {
					holders = append(holders, continuityPartialConnection(t, server, f.a.Bearer))
				}
				continuityWaitActive(t, c, 16)
			}
			start := time.Now()
			conn := continuityPartialConnection(t, server, nil)
			defer conn.Close()
			response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
			if err == nil {
				want := 401
				if busy {
					want = 503
				}
				if response.StatusCode != want {
					t.Fatalf("early status %d want %d", response.StatusCode, want)
				}
				_, _ = io.Copy(io.Discard, response.Body)
				response.Body.Close()
			}
			// A server may abort a response when the unfinished request body
			// reaches its read deadline. Either outcome must be bounded by
			// server deadlines, not this connection's five-second deadline.
			if time.Since(start) > 4500*time.Millisecond {
				t.Fatalf("early error escaped transport deadline: %v", err)
			}
		})
	}
}

func TestContinuityUnsupportedTransportAbortsBeforeQuery(t *testing.T) {
	f := newContinuityTestFixture(t)
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: f.path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	w := httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != http.ErrAbortHandler {
			t.Fatalf("unsupported transport did not abort: %v", recovered)
		}
		if w.Body.Len() != 0 {
			t.Fatal("unsupported transport attempted unbounded response")
		}
	}()
	c.serveHTTP(w, httptest.NewRequest(http.MethodPost, "http://localhost/api/continuity/v1/asset", nil), "/api/continuity/v1/")
}

func TestContinuityRouteRegistrationRejectsUnsafePrefixesAndReuse(t *testing.T) {
	f := newContinuityTestFixture(t)
	c, err := PrepareContinuity(context.Background(), ContinuityConfig{CatalogPath: f.path, LibraryID: string(continuityTestLibrary), ManagerAdminKey: continuityTestAdmin})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	for _, prefix := range []string{"relative", "/", "/manager/", "/a/../manager", "/manager?x", "/a//b", "/{capture}"} {
		if err := c.RegisterRoutes(http.NewServeMux(), prefix); err == nil {
			t.Fatalf("unsafe prefix accepted: %q", prefix)
		}
	}
	mux := http.NewServeMux()
	if err := c.RegisterRoutes(mux, "/manager"); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterRoutes(mux, "/manager"); err == nil {
		t.Fatal("duplicate registration accepted")
	}
}

type continuityPipeListener struct {
	conn      net.Conn
	accepted  sync.Once
	closed    chan struct{}
	closeOnce sync.Once
}

func (l *continuityPipeListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, net.ErrClosed
	default:
	}
	first := false
	l.accepted.Do(func() { first = true })
	if first {
		return l.conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}
func (l *continuityPipeListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}
func (l *continuityPipeListener) Addr() net.Addr { return l.conn.LocalAddr() }

type continuityObservedWrite struct {
	bytes  []byte
	result chan error
}
type continuityObservedConn struct {
	net.Conn
	writes chan continuityObservedWrite
}

func (c *continuityObservedConn) Write(p []byte) (int, error) {
	attempt := continuityObservedWrite{bytes: append([]byte(nil), p...), result: make(chan error, 1)}
	select {
	case c.writes <- attempt:
	default:
	}
	n, err := c.Conn.Write(p)
	attempt.result <- err
	return n, err
}

func TestContinuityBlockedResponseFlushHasActualTransportDeadline(t *testing.T) {
	for _, authorized := range []bool{true, false} {
		t.Run(fmt.Sprintf("authorized=%v", authorized), func(t *testing.T) {
			f := newContinuityTestFixture(t)
			_, production := continuityTestHost(t, f, "")
			clientConn, serverConn := net.Pipe()
			observed := &continuityObservedConn{Conn: serverConn, writes: make(chan continuityObservedWrite, 4)}
			listener := &continuityPipeListener{conn: observed, closed: make(chan struct{})}
			entered, handlerDone := make(chan struct{}), make(chan struct{})
			httpServer := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				defer close(handlerDone)
				production.Config.Handler.ServeHTTP(w, r)
			})}
			serveDone := make(chan error, 1)
			go func() { serveDone <- httpServer.Serve(listener) }()
			t.Cleanup(func() {
				clientConn.Close()
				serverConn.Close()
				httpServer.Close()
				listener.Close()
				select {
				case <-serveDone:
				case <-time.After(time.Second):
					t.Error("pipe Serve worker not joined")
				}
				select {
				case <-entered:
					select {
					case <-handlerDone:
					case <-time.After(time.Second):
						t.Error("pipe response handler not joined")
					}
				default:
				}
			})
			body := continuityAssetBody(continuityTestA)
			auth := ""
			if authorized {
				auth = "Authorization: Bearer " + string(f.a.Bearer) + "\r\n"
			}
			if err := clientConn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			_, err := fmt.Fprintf(clientConn, "POST /api/continuity/v1/asset HTTP/1.1\r\nHost: localhost\r\n%sContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", auth, len(body), body)
			if err != nil {
				t.Fatal("private pipe request failed")
			}
			// No client Read is ever issued. net.Pipe has no kernel send
			// buffer, so the first real response flush deterministically
			// blocks even for a tiny response.
			var attempt continuityObservedWrite
			select {
			case attempt = <-observed.writes:
			case <-time.After(3 * time.Second):
				t.Fatal("HTTP path did not attempt a transport flush")
			}
			status := "HTTP/1.1 401"
			if authorized {
				status = "HTTP/1.1 200"
			}
			if !bytes.HasPrefix(attempt.bytes, []byte(status)) {
				t.Fatalf("unexpected flush status; expected %s", status)
			}
			select {
			case err := <-attempt.result:
				var timeout net.Error
				if !errors.As(err, &timeout) || !timeout.Timeout() {
					t.Fatalf("blocked flush did not fail at transport deadline: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("blocked response flush escaped its write deadline")
			}
			select {
			case <-handlerDone:
			case <-time.After(time.Second):
				t.Fatal("bounded flush did not release admitted handler")
			}
		})
	}
}
