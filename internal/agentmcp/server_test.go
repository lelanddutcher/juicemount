package agentmcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

const (
	testToken = "jma_0123456789abcdef_" + "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
	testMount = "/Volumes/zpool"
	asset1    = "asset_00000000000000000000000000000001"
	asset2    = "asset_00000000000000000000000000000002"
	col1      = "col_0000000000000000000000000000000a"
	qry1      = "qry_0000000000000000000000000000000b"
)

// seen is one request the fake Manager received.
type seen struct {
	Method, Path string
	Query        url.Values
	Body         map[string]any
	Auth         string
	Header       http.Header
}

// fakeManager answers each "METHOD /path" with the next queued answer (or
// a 404), and records every request.
type fakeManager struct {
	t       *testing.T
	mu      sync.Mutex
	answers map[string][]fakeAnswer
	got     []seen
}

type fakeAnswer struct {
	status int
	body   any
	drop   bool // close the connection without an answer
}

func newFakeManager(t *testing.T) (*fakeManager, *httptest.Server) {
	f := &fakeManager{t: t, answers: map[string][]fakeAnswer{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeManager) on(key string, status int, body any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[key] = append(f.answers[key], fakeAnswer{status: status, body: body})
}

func (f *fakeManager) dropNext(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[key] = append(f.answers[key], fakeAnswer{drop: true})
}

func (f *fakeManager) requests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.got...)
}

func (f *fakeManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s := seen{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Auth: r.Header.Get("Authorization"), Header: r.Header.Clone()}
	if b, _ := io.ReadAll(r.Body); len(b) > 0 {
		if err := json.Unmarshal(b, &s.Body); err != nil {
			f.t.Errorf("request body is not JSON: %q", b)
		}
	}
	key := r.Method + " " + r.URL.Path
	f.mu.Lock()
	f.got = append(f.got, s)
	q := f.answers[key]
	var a fakeAnswer
	if len(q) == 0 {
		a = fakeAnswer{status: 404, body: map[string]string{"error": "no such route"}}
	} else {
		a, f.answers[key] = q[0], q[1:]
	}
	f.mu.Unlock()
	if a.drop {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(a.status)
	_ = json.NewEncoder(w).Encode(a.body)
}

// session is an MCP client talking to a Server over pipes.
type session struct {
	t  *testing.T
	in *io.PipeWriter
	// lines carries every line the server writes, read as it is written
	// (so an unexpected answer cannot block the server); closed at the end
	// of the output.
	lines  chan string
	done   chan error
	logs   *syncBuffer
	nextID int
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func testTools(base string, logger *log.Logger) *Tools {
	hc := NewHTTPClient()
	return &Tools{Mount: testMount, Log: logger, Sleep: func(time.Duration) {},
		Client: func() (*Client, error) { return NewAgentClient(base, testToken, hc), nil }}
}

// startSession runs a server whose tools call base; tools may be adjusted
// before the session starts.
func startSession(t *testing.T, base string, adjust ...func(*Tools)) *session {
	t.Helper()
	logs := &syncBuffer{}
	logger := log.New(logs, "", 0)
	tools := testTools(base, logger)
	for _, f := range adjust {
		f(tools)
	}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	s := &session{t: t, in: inW, lines: make(chan string, 1024), done: make(chan error, 1), logs: logs}
	go func() {
		defer close(s.lines)
		r := bufio.NewReaderSize(outR, 4<<20)
		for {
			l, err := r.ReadString('\n')
			if l != "" {
				s.lines <- l
			}
			if err != nil {
				return
			}
		}
	}()
	srv := &Server{Tools: tools, Log: logger, Version: "test"}
	go func() {
		err := srv.Serve(context.Background(), inR, outW)
		outW.Close()
		s.done <- err
	}()
	t.Cleanup(func() { inW.Close() })
	return s
}

func (s *session) sendRaw(line string) {
	s.t.Helper()
	if _, err := io.WriteString(s.in, line+"\n"); err != nil {
		s.t.Fatal(err)
	}
}

type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

// recv reads one message from the server (failing after 10 s).
func (s *session) recv() response {
	s.t.Helper()
	select {
	case line, ok := <-s.lines:
		if !ok {
			s.t.Fatal("the server's output ended")
		}
		var resp response
		if err := json.Unmarshal([]byte(line), &resp); err != nil {
			s.t.Fatalf("server wrote a non-JSON line %q", line)
		}
		if resp.JSONRPC != "2.0" {
			s.t.Fatalf("answer without jsonrpc 2.0: %s", line)
		}
		return resp
	case <-time.After(10 * time.Second):
		s.t.Fatal("no answer from the server")
	}
	return response{}
}

// request sends a request and returns its answer.
func (s *session) request(method string, params any) response {
	s.t.Helper()
	s.nextID++
	msg := map[string]any{"jsonrpc": "2.0", "id": s.nextID, "method": method}
	if params != nil {
		msg["params"] = params
	}
	b, _ := json.Marshal(msg)
	s.sendRaw(string(b))
	r := s.recv()
	if string(r.ID) != fmt.Sprint(s.nextID) {
		s.t.Fatalf("answer id %s, want %d", r.ID, s.nextID)
	}
	return r
}

func (s *session) initialize() {
	s.t.Helper()
	r := s.request("initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test", "version": "1"}})
	if r.Error != nil {
		s.t.Fatalf("initialize: %+v", r.Error)
	}
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
}

type callResult struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent"`
	IsError           bool            `json:"isError"`
}

// call runs a tool; the answer must be a tool result, not a protocol error.
func (s *session) call(name string, args any) (callResult, map[string]any) {
	s.t.Helper()
	r := s.request("tools/call", map[string]any{"name": name, "arguments": args})
	if r.Error != nil {
		s.t.Fatalf("tools/call %s: protocol error %+v", name, r.Error)
	}
	var cr callResult
	if err := json.Unmarshal(r.Result, &cr); err != nil || len(cr.Content) != 1 || cr.Content[0].Type != "text" {
		s.t.Fatalf("tools/call %s: result %s", name, r.Result)
	}
	var out map[string]any
	if !cr.IsError {
		if err := json.Unmarshal([]byte(cr.Content[0].Text), &out); err != nil {
			s.t.Fatalf("tool text is not JSON: %q", cr.Content[0].Text)
		}
		if !bytes.Equal(bytes.TrimSpace(cr.StructuredContent), []byte(cr.Content[0].Text)) {
			s.t.Fatalf("structuredContent %s differs from the text %s", cr.StructuredContent, cr.Content[0].Text)
		}
	}
	return cr, out
}

func TestInitializeNegotiatesVersion(t *testing.T) {
	f, srv := newFakeManager(t)
	for _, c := range []struct{ ask, want string }{
		{"2025-06-18", "2025-06-18"}, {"2025-03-26", "2025-06-18"}, {"2024-11-05", "2024-11-05"}, {"2099-01-01", "2025-06-18"},
	} {
		s := startSession(t, srv.URL)
		// Nothing but initialize and ping before initialize.
		if r := s.request("tools/list", nil); r.Error == nil || r.Error.Code != codeInvalidRequest {
			t.Fatalf("tools/list before initialize: %+v", r)
		}
		if r := s.request("ping", nil); r.Error != nil || string(r.Result) != "{}" {
			t.Fatalf("ping: %+v %s", r.Error, r.Result)
		}
		r := s.request("initialize", map[string]any{"protocolVersion": c.ask, "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "x", "version": "1"}})
		var res struct {
			ProtocolVersion string `json:"protocolVersion"`
			Capabilities    struct {
				Tools *struct {
					ListChanged bool `json:"listChanged"`
				} `json:"tools"`
			} `json:"capabilities"`
			ServerInfo struct {
				Name, Version string
			} `json:"serverInfo"`
			Instructions string `json:"instructions"`
		}
		if r.Error != nil || json.Unmarshal(r.Result, &res) != nil {
			t.Fatalf("initialize %s: %+v %s", c.ask, r.Error, r.Result)
		}
		if res.ProtocolVersion != c.want || res.Capabilities.Tools == nil || res.ServerInfo.Name != "juicemount-mcp" || res.ServerInfo.Version != "test" || res.Instructions == "" {
			t.Fatalf("initialize %s: %s", c.ask, r.Result)
		}
		// Only 2025-06-18 carries structuredContent.
		s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
		f.on("GET /api/collections", 200, map[string]any{"collections": []any{}})
		cr := s.request("tools/call", map[string]any{"name": "list_collections", "arguments": map[string]any{}})
		var tr callResult
		if err := json.Unmarshal(cr.Result, &tr); err != nil || tr.IsError || (tr.StructuredContent != nil) != (c.want == "2025-06-18") {
			t.Fatalf("call under %s: %s", c.want, cr.Result)
		}
	}
	s := startSession(t, srv.URL)
	if r := s.request("initialize", map[string]any{"capabilities": map[string]any{}}); r.Error == nil || r.Error.Code != codeInvalidParams {
		t.Fatalf("initialize without a version: %+v", r)
	}
}

func TestToolsListSchemas(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	r := s.request("tools/list", nil)
	var res struct {
		Tools []struct {
			Name        string          `json:"name"`
			Title       string          `json:"title"`
			Description string          `json:"description"`
			InputSchema json.RawMessage `json:"inputSchema"`
			Annotations map[string]bool `json:"annotations"`
		} `json:"tools"`
		NextCursor *string `json:"nextCursor"`
	}
	if r.Error != nil || json.Unmarshal(r.Result, &res) != nil || res.NextCursor != nil {
		t.Fatalf("tools/list: %+v %s", r.Error, r.Result)
	}
	want := []string{"find_files", "get_file", "list_collections", "get_collection_members", "create_collection",
		"rename_collection", "add_to_collection", "remove_from_collection", "list_saved_queries", "get_saved_query_results"}
	if len(res.Tools) != len(want) {
		t.Fatalf("%d tools, want %d", len(res.Tools), len(want))
	}
	for i, tool := range res.Tools {
		if tool.Name != want[i] || tool.Title == "" || len(tool.Description) < 60 {
			t.Errorf("tool %d: %q %q %q", i, tool.Name, tool.Title, tool.Description)
		}
		var schema struct {
			Type       string                     `json:"type"`
			Properties map[string]json.RawMessage `json:"properties"`
			Required   []string                   `json:"required"`
			Additional *bool                      `json:"additionalProperties"`
		}
		if err := json.Unmarshal(tool.InputSchema, &schema); err != nil || schema.Type != "object" || schema.Additional == nil || *schema.Additional {
			t.Errorf("%s: schema %s", tool.Name, tool.InputSchema)
		}
		for _, req := range schema.Required {
			if _, ok := schema.Properties[req]; !ok {
				t.Errorf("%s: required %q is not a property", tool.Name, req)
			}
		}
		c := jsonschema.NewCompiler()
		if err := c.AddResource("schema.json", bytes.NewReader(tool.InputSchema)); err != nil {
			t.Fatalf("%s: %v", tool.Name, err)
		}
		if _, err := c.Compile("schema.json"); err != nil {
			t.Errorf("%s: the input schema does not compile: %v", tool.Name, err)
		}
		readOnly := strings.HasPrefix(tool.Name, "get_") || strings.HasPrefix(tool.Name, "list_") || tool.Name == "find_files"
		if tool.Annotations["readOnlyHint"] != readOnly || tool.Annotations["openWorldHint"] {
			t.Errorf("%s: annotations %v", tool.Name, tool.Annotations)
		}
	}
}

func fileJSON(id, p string) map[string]any {
	return map[string]any{"asset_id": id, "name": p[strings.LastIndex(p, "/")+1:], "label": "first.mov", "path_hint": strings.TrimPrefix(p, "/"),
		"path": p, "size": 1234, "modified": "2026-10-01T12:00:00Z", "collections": []any{map[string]any{"id": col1, "name": "Selects"}}}
}

func TestToolRequestMapping(t *testing.T) {
	f, srv := newFakeManager(t)
	s := startSession(t, srv.URL+"/manager")
	s.initialize()
	check := func(i int, method, path string) seen {
		t.Helper()
		got := f.requests()
		if len(got) <= i {
			t.Fatalf("request %d not sent; got %+v", i, got)
		}
		g := got[i]
		if g.Method != method || g.Path != "/manager"+path || g.Auth != "Bearer "+testToken {
			t.Fatalf("request %d: %s %s (auth set: %v), want %s /manager%s", i, g.Method, g.Path, g.Auth != "", method, path)
		}
		if g.Header.Get("X-JuiceMount-Mac-Token") != "" {
			t.Fatal("an agent request carries the Mac's credential")
		}
		return g
	}

	// find_files: every filter.
	f.on("GET /manager/api/continuity/find", 200, map[string]any{"files": []any{fileJSON(asset1, "/Clients/A/one.mov")}, "scanned": 3, "resolved": 1})
	_, out := s.call("find_files", map[string]any{"name": "One", "folders": []string{"/Clients/A", "/Clients/B"}, "collection_id": col1,
		"modified_after": "2026-10-01T00:00:00Z", "seen_after": 7, "extensions": []string{"mov", ".mxf"}, "kinds": []string{"video"}, "limit": 20,
		"cursor": "jfs-x-1-hint"})
	g := check(0, "GET", "/api/continuity/find")
	wantQ := url.Values{"name": {"One"}, "folder": {"/Clients/A", "/Clients/B"}, "collection": {col1}, "modified_after": {"2026-10-01T00:00:00Z"},
		"seen_after": {"7"}, "ext": {"mov", ".mxf"}, "kind": {"video"}, "limit": {"20"}, "cursor": {"jfs-x-1-hint"}}
	if g.Query.Encode() != wantQ.Encode() {
		t.Fatalf("find query %v, want %v", g.Query, wantQ)
	}
	files := out["files"].([]any)
	if len(files) != 1 || files[0].(map[string]any)["mount_path"] != testMount+"/Clients/A/one.mov" || out["budget_exhausted"] != false {
		t.Fatalf("find: %v", out)
	}

	// get_file.
	f.on("GET /manager/api/continuity/asset/"+asset1, 200, fileJSON(asset1, "/Clients/A/one.mov"))
	_, out = s.call("get_file", map[string]any{"asset_id": asset1})
	check(1, "GET", "/api/continuity/asset/"+asset1)
	if out["mount_path"] != testMount+"/Clients/A/one.mov" || out["size"] != 1234.0 {
		t.Fatalf("get_file: %v", out)
	}

	// list_collections.
	f.on("GET /manager/api/collections", 200, map[string]any{"collections": []any{map[string]any{"id": col1, "name": "Selects", "members": 2}}, "next": col1})
	_, out = s.call("list_collections", map[string]any{"after": col1, "limit": 5})
	if g := check(2, "GET", "/api/collections"); g.Query.Encode() != "after="+col1+"&limit=5" {
		t.Fatalf("list query %v", g.Query)
	}
	if out["next"] != col1 || len(out["collections"].([]any)) != 1 {
		t.Fatalf("list_collections: %v", out)
	}

	// get_collection_members: paths gain mount_path; a missing member has
	// none; fields the Manager adds are passed on.
	f.on("GET /manager/api/collections/"+col1+"/members", 200, map[string]any{
		"collection": map[string]any{"id": col1, "name": "Selects"},
		"members": []any{
			map[string]any{"asset_id": asset1, "label": "one.mov", "path": "Clients/A/one.mov", "state": "present", "added_sequence": 4},
			map[string]any{"asset_id": asset2, "label": "gone.mov", "state": "missing", "added_sequence": 5},
		},
		"next": asset2, "withheld": 3,
	})
	_, out = s.call("get_collection_members", map[string]any{"collection_id": col1, "after": asset1, "limit": 2})
	if g := check(3, "GET", "/api/collections/"+col1+"/members"); g.Query.Encode() != "after="+asset1+"&limit=2" {
		t.Fatalf("members query %v", g.Query)
	}
	members := out["members"].([]any)
	m0, m1 := members[0].(map[string]any), members[1].(map[string]any)
	if m0["mount_path"] != testMount+"/Clients/A/one.mov" || m0["added_sequence"] != 4.0 || m1["mount_path"] != nil || m1["state"] != "missing" ||
		out["next"] != asset2 || out["withheld"] != 3.0 {
		t.Fatalf("members: %v", out)
	}

	// create_collection: members by asset ID, an operation_id generated.
	written := map[string]any{"collection": map[string]any{"id": col1, "name": "New", "members": 2}, "operation": map[string]any{"id": "col-x", "sequence": 9, "actor": "p1"}}
	f.on("POST /manager/api/collections", 200, written)
	_, out = s.call("create_collection", map[string]any{"name": "New", "asset_ids": []string{asset1, asset2}})
	g = check(4, "POST", "/api/collections")
	op, _ := g.Body["operation_id"].(string)
	if g.Body["name"] != "New" || fmt.Sprint(g.Body["members"]) != fmt.Sprintf("[map[asset_id:%s] map[asset_id:%s]]", asset1, asset2) ||
		!operationIDRe.MatchString(op) || out["operation_id"] != op || out["collection"].(map[string]any)["id"] != col1 {
		t.Fatalf("create: sent %v, got %v", g.Body, out)
	}
	// A second create without operation_id gets another one.
	f.on("POST /manager/api/collections", 200, written)
	s.call("create_collection", map[string]any{"name": "New"})
	if g := check(5, "POST", "/api/collections"); g.Body["operation_id"] == op || g.Body["members"] != nil {
		t.Fatalf("second create: %v", g.Body)
	}

	// rename_collection with the agent's own operation_id.
	f.on("POST /manager/api/collections/"+col1+"/rename", 200, written)
	_, out = s.call("rename_collection", map[string]any{"collection_id": col1, "name": "Renamed", "operation_id": "retry-1"})
	if g := check(6, "POST", "/api/collections/"+col1+"/rename"); g.Body["name"] != "Renamed" || g.Body["operation_id"] != "retry-1" || len(g.Body) != 2 || out["operation_id"] != "retry-1" {
		t.Fatalf("rename: %v %v", g.Body, out)
	}

	// add_to_collection and remove_from_collection.
	for i, verb := range []string{"add", "remove"} {
		f.on("POST /manager/api/collections/"+col1+"/members/"+verb, 200, written)
		s.call(verb+map[string]string{"add": "_to_collection", "remove": "_from_collection"}[verb], map[string]any{"collection_id": col1, "asset_ids": []string{asset2}})
		g := check(7+i, "POST", "/api/collections/"+col1+"/members/"+verb)
		if fmt.Sprint(g.Body["members"]) != fmt.Sprintf("[map[asset_id:%s]]", asset2) || !operationIDRe.MatchString(g.Body["operation_id"].(string)) || len(g.Body) != 2 {
			t.Fatalf("%s: %v", verb, g.Body)
		}
	}

	// list_saved_queries and get_saved_query_results.
	f.on("GET /manager/api/queries", 200, map[string]any{"queries": []any{map[string]any{"id": qry1, "name": "Recent", "folders": []string{"Clients/A"}}}})
	_, out = s.call("list_saved_queries", map[string]any{})
	if g := check(9, "GET", "/api/queries"); len(g.Query) != 0 || len(out["queries"].([]any)) != 1 {
		t.Fatalf("queries: %v %v", g.Query, out)
	}
	f.on("GET /manager/api/queries/"+qry1+"/results", 200, map[string]any{"query": map[string]any{"id": qry1}, "evaluated_at": "2026-10-01T12:00:00Z",
		"age_seconds": 12, "capped": true, "examined": 2000, "candidates": 2500,
		"results": []any{map[string]any{"asset_id": asset1, "path": "Clients/A/one.mov", "modified": "2026-10-01T11:00:00Z"}}})
	_, out = s.call("get_saved_query_results", map[string]any{"query_id": qry1})
	check(10, "GET", "/api/queries/"+qry1+"/results")
	res := out["results"].([]any)[0].(map[string]any)
	if res["mount_path"] != testMount+"/Clients/A/one.mov" || out["capped"] != true || out["examined"] != 2000.0 || out["age_seconds"] != 12.0 {
		t.Fatalf("results: %v", out)
	}

	// Bad arguments are the tool's error, and reach no Manager.
	before := len(f.requests())
	for name, args := range map[string]any{
		"get_file":               map[string]any{"asset_id": "../../etc"},
		"find_files":             map[string]any{"limit": 500},
		"add_to_collection":      map[string]any{"collection_id": col1, "asset_ids": []string{}},
		"rename_collection":      map[string]any{"collection_id": "col_x", "name": "n"},
		"create_collection":      map[string]any{"name": "n", "operation_id": "has space"},
		"get_collection_members": map[string]any{"collection_id": col1, "bogus": 1},
		"list_saved_queries":     map[string]any{"after": "x"},
	} {
		if cr, _ := s.call(name, args); !cr.IsError {
			t.Errorf("%s %v: not an error", name, args)
		}
	}
	if len(f.requests()) != before {
		t.Fatal("an invalid call reached the Manager")
	}
}

func TestToolErrorsAreResults(t *testing.T) {
	f, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	for _, c := range []struct {
		status int
		key    string
		tool   string
		args   map[string]any
		want   string
	}{
		{401, "GET /api/continuity/find", "find_files", map[string]any{}, "in JuiceMount's Settings (Connection, Agents), or with juicemount-mcp token create"},
		// A token revoked while an edit runs is refused the same way.
		{401, "POST /api/collections/" + col1 + "/members/add", "add_to_collection", map[string]any{"collection_id": col1, "asset_ids": []string{asset1}}, "in JuiceMount's Settings (Connection, Agents), or with juicemount-mcp token create"},
		{403, "POST /api/collections/" + col1 + "/rename", "rename_collection", map[string]any{"collection_id": col1, "name": "x"}, "project grants"},
		{404, "GET /api/continuity/asset/" + asset1, "get_file", map[string]any{"asset_id": asset1}, "not visible"},
		{409, "POST /api/collections", "create_collection", map[string]any{"name": "x"}, "Conflict"},
		{429, "GET /api/collections", "list_collections", map[string]any{}, "limiting"},
	} {
		f.on(c.key, c.status, map[string]string{"error": "manager says \x01 why"})
		cr, _ := s.call(c.tool, c.args)
		if !cr.IsError || !strings.Contains(cr.Content[0].Text, c.want) || !strings.Contains(cr.Content[0].Text, "manager says   why") {
			t.Errorf("%d on %s: %+v", c.status, c.tool, cr)
		}
		if strings.Contains(cr.Content[0].Text, testToken) {
			t.Fatal("an error carries the token")
		}
	}
	// No token stored, or one that cannot be read: every tool, read or
	// write, says how to get one, and nothing reaches the Manager.
	n := len(f.requests())
	for _, c := range []struct {
		err  error
		want string
	}{
		{fmt.Errorf("%w here", ErrNoToken), "No agent token is stored"},
		{io.ErrUnexpectedEOF, "cannot be read (unexpected EOF)"},
	} {
		s2 := startSession(t, srv.URL, func(tl *Tools) {
			tl.Client = func() (*Client, error) { return nil, c.err }
		})
		s2.initialize()
		for _, call := range []struct {
			tool string
			args map[string]any
		}{{"list_collections", map[string]any{}}, {"create_collection", map[string]any{"name": "x"}}} {
			cr, _ := s2.call(call.tool, call.args)
			// The app's Settings is the normal way to get one; the CLI is for
			// a machine without it.
			if !cr.IsError || !strings.Contains(cr.Content[0].Text, c.want) || !strings.Contains(cr.Content[0].Text, "juicemount-mcp token create") ||
				!strings.Contains(cr.Content[0].Text, "JuiceMount's Settings (Connection, Agents") ||
				strings.Contains(cr.Content[0].Text, "could not be reached") {
				t.Fatalf("%v, %s: %+v", c.err, call.tool, cr)
			}
		}
	}
	if len(f.requests()) != n {
		t.Fatal("a call without a token reached the Manager")
	}
	if strings.Contains(s.logs.String(), testToken) || strings.Contains(s.logs.String(), testToken[21:]) {
		t.Fatal("the log holds the token")
	}
}

func TestFindPagesThroughBudget(t *testing.T) {
	f, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	// A page cut short by the budget, with no files, still has next.
	f.on("GET /api/continuity/find", 200, map[string]any{"files": []any{}, "next": "jfs-v-10-hint", "scanned": 100000, "resolved": 0, "budget_exhausted": true})
	f.on("GET /api/continuity/find", 200, map[string]any{"files": []any{fileJSON(asset1, "/A/x.mov")}, "scanned": 5, "resolved": 1})
	_, p1 := s.call("find_files", map[string]any{"kinds": []string{"video"}})
	if p1["budget_exhausted"] != true || p1["next"] != "jfs-v-10-hint" || len(p1["files"].([]any)) != 0 {
		t.Fatalf("page 1: %v", p1)
	}
	_, p2 := s.call("find_files", map[string]any{"kinds": []string{"video"}, "cursor": p1["next"]})
	if p2["budget_exhausted"] != false || p2["next"] != nil || len(p2["files"].([]any)) != 1 {
		t.Fatalf("page 2: %v", p2)
	}
	got := f.requests()
	if len(got) != 2 || got[0].Query.Get("cursor") != "" || got[1].Query.Get("cursor") != "jfs-v-10-hint" || got[1].Query.Get("kind") != "video" {
		t.Fatalf("requests: %+v", got)
	}
}

func TestWriteRetriesKeepOperationID(t *testing.T) {
	f, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	key := "POST /api/collections/" + col1 + "/members/add"
	ok := map[string]any{"collection": map[string]any{"id": col1}, "operation": map[string]any{"id": "x"}}
	// A lost answer, then a busy Manager: each is sent again, unchanged.
	f.dropNext(key)
	f.on(key, 200, ok)
	f.on(key, 503, map[string]string{"error": "busy"})
	f.on(key, 200, ok)
	for i := range 2 {
		cr, out := s.call("add_to_collection", map[string]any{"collection_id": col1, "asset_ids": []string{asset1}})
		got := f.requests()[2*i:]
		if cr.IsError || len(got) != 2 {
			t.Fatalf("%d: %+v after %d requests", i, cr, len(got))
		}
		op := got[0].Body["operation_id"]
		if got[1].Body["operation_id"] != op || out["operation_id"] != op {
			t.Fatalf("%d: a retry changed operation_id: %v, result %v", i, got, out["operation_id"])
		}
	}
	n := 4
	// Every attempt lost: the agent is told the operation_id to retry with.
	for range writeAttempts {
		f.dropNext(key)
	}
	cr, _ := s.call("add_to_collection", map[string]any{"collection_id": col1, "asset_ids": []string{asset1}, "operation_id": "mine-1"})
	if !cr.IsError || !strings.Contains(cr.Content[0].Text, `operation_id "mine-1"`) || len(f.requests()) != n+writeAttempts {
		t.Fatalf("lost answers: %+v (%d requests)", cr, len(f.requests()))
	}
	n += writeAttempts
	// A lost answer, then a refusal: it may have been applied the first
	// time, so the agent still gets the operation_id.
	f.dropNext(key)
	f.on(key, 401, map[string]string{"error": "unknown or revoked agent token"})
	cr, _ = s.call("add_to_collection", map[string]any{"collection_id": col1, "asset_ids": []string{asset1}, "operation_id": "mine-2"})
	if !cr.IsError || !strings.Contains(cr.Content[0].Text, `operation_id "mine-2"`) || !strings.Contains(cr.Content[0].Text, "token create") {
		t.Fatalf("lost then 401: %+v", cr)
	}
	n += 2
	// A definite refusal on the first attempt is not retried, and nothing
	// was applied.
	f.on(key, 403, map[string]string{"error": "no grant"})
	cr, _ = s.call("add_to_collection", map[string]any{"collection_id": col1, "asset_ids": []string{asset1}})
	if !cr.IsError || strings.Contains(cr.Content[0].Text, "may or may not") || len(f.requests()) != n+1 {
		t.Fatalf("403: %+v", cr)
	}
}

func TestMalformedMessages(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	for _, c := range []struct {
		line string
		code int
		id   string
	}{
		{`{not json`, codeParse, "null"},
		{`"just a string"`, codeParse, "null"},
		{`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":{"a":1},"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":null,"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":1.5,"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":1.0,"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":1e3,"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"2.0","id":true,"method":"ping"}`, codeInvalidRequest, "null"},
		{`{"jsonrpc":"1.0","id":7,"method":"ping"}`, codeInvalidRequest, "7"},
		{`{"jsonrpc":"2.0","id":8}`, codeInvalidRequest, "8"},
		{`{"jsonrpc":"2.0","id":"s","method":"resources/list"}`, codeMethodNotFound, `"s"`},
		{`{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"read_file"}}`, codeInvalidParams, "9"},
		{`{"jsonrpc":"2.0","id":10,"method":"tools/call","params":[]}`, codeInvalidParams, "10"},
	} {
		s.sendRaw(c.line)
		r := s.recv()
		if r.Error == nil || r.Error.Code != c.code || string(r.ID) != c.id || r.Result != nil {
			t.Errorf("%s: %+v id %s", c.line, r.Error, r.ID)
		}
	}
	// Still serving.
	if r := s.request("ping", nil); r.Error != nil {
		t.Fatal(r.Error)
	}
}

func TestNotificationsGetNoAnswer(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3}}`)
	s.sendRaw(`{"jsonrpc":"2.0","method":"notifications/unknown"}`)
	s.sendRaw(`{"jsonrpc":"2.0","id":99,"result":{}}`) // a response to us: ignored
	s.sendRaw(``)
	// The next message read must be the ping's answer.
	if r := s.request("ping", nil); r.Error != nil {
		t.Fatal(r.Error)
	}
}

func TestOversizeLineIsRefused(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	big := `{"jsonrpc":"2.0","id":5,"method":"ping","params":{"pad":"` + strings.Repeat("x", MaxLine) + `"}}`
	s.sendRaw(big)
	r := s.recv()
	if r.Error == nil || r.Error.Code != codeInvalidRequest || string(r.ID) != "null" || !strings.Contains(r.Error.Message, "larger") {
		t.Fatalf("oversize: %+v", r)
	}
	// A message of exactly MaxLine bytes is read.
	pre := `{"jsonrpc":"2.0","id":6,"method":"ping","params":{"pad":"`
	post := `"}}`
	s.sendRaw(pre + strings.Repeat("y", MaxLine-len(pre)-len(post)) + post)
	if r := s.recv(); r.Error != nil || string(r.ID) != "6" {
		t.Fatalf("max-size message: %+v", r)
	}
	if r := s.request("ping", nil); r.Error != nil {
		t.Fatal(r.Error)
	}
	if !strings.Contains(s.logs.String(), "larger than") {
		t.Fatalf("not logged: %s", s.logs)
	}
}

func TestEOFShutsDown(t *testing.T) {
	_, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	// The last message may lack its newline.
	if _, err := io.WriteString(s.in, `{"jsonrpc":"2.0","id":1,"method":"ping"}`); err != nil {
		t.Fatal(err)
	}
	s.in.Close()
	if r := s.recv(); r.Error != nil || string(r.ID) != "1" {
		t.Fatalf("last message: %+v", r)
	}
	select {
	case err := <-s.done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return at EOF")
	}
	if l, ok := <-s.lines; ok {
		t.Fatalf("output after shutdown: %q", l)
	}
}
