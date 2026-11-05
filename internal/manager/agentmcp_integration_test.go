package manager

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/agentmcp"
)

// mcpMemStore is an agentmcp.TokenStore in memory.
type mcpMemStore struct{ rec *agentmcp.TokenRecord }

func (m *mcpMemStore) Save(r agentmcp.TokenRecord) error { m.rec = &r; return nil }
func (m *mcpMemStore) Delete() error                     { m.rec = nil; return nil }
func (m *mcpMemStore) Load() (agentmcp.TokenRecord, error) {
	if m.rec == nil {
		return agentmcp.TokenRecord{}, agentmcp.ErrNoToken
	}
	return *m.rec, nil
}

// mcpClient drives a juicemount-mcp server over pipes.
type mcpClient struct {
	t     *testing.T
	in    *io.PipeWriter
	lines chan string
	id    int
}

func (c *mcpClient) call(name string, args map[string]any) (string, bool, map[string]any) {
	c.t.Helper()
	c.id++
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": c.id, "method": "tools/call", "params": map[string]any{"name": name, "arguments": args}})
	if _, err := c.in.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
	var line string
	select {
	case line = <-c.lines:
	case <-time.After(30 * time.Second):
		c.t.Fatalf("%s: no answer", name)
	}
	var resp struct {
		ID     int `json:"id"`
		Result struct {
			Content []struct{ Text string } `json:"content"`
			IsError bool                    `json:"isError"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal([]byte(line), &resp); err != nil || resp.ID != c.id || resp.Error != nil || len(resp.Result.Content) != 1 {
		c.t.Fatalf("%s: answer %s", name, line)
	}
	text := resp.Result.Content[0].Text
	var out map[string]any
	if !resp.Result.IsError {
		if err := json.Unmarshal([]byte(text), &out); err != nil {
			c.t.Fatalf("%s: %s", name, text)
		}
	}
	return text, resp.Result.IsError, out
}

// TestAgentMCPAgainstManager runs juicemount-mcp (internal/agentmcp) against
// the real agent-token, discovery, collection and query handlers: a person
// mints a token from their Mac, their agent finds files and edits a
// collection within their grants, and a revoke stops it.
func TestAgentMCPAgainstManager(t *testing.T) {
	e := newDiscoveryEnv(t)
	srv := httptest.NewServer(e.mux)
	defer srv.Close()
	base := srv.URL + "/manager"
	hc := agentmcp.NewHTTPClient()
	store := &mcpMemStore{}
	var out, errb bytes.Buffer
	ann := e.macs["Ann"]
	tc := &agentmcp.TokenCommands{Store: store, HTTP: hc, Out: &out, Err: &errb, Mac: func() (agentmcp.MacCredentials, error) {
		return agentmcp.MacCredentials{ManagerURL: base, InstanceID: ann[0], PollToken: ann[1], Device: ann[2]}, nil
	}}
	ctx := context.Background()
	if err := tc.Create(ctx, "Claude Desktop", 7, false); err != nil {
		t.Fatal(err)
	}
	if store.rec == nil || strings.Contains(out.String()+errb.String(), store.rec.Token[21:]) {
		t.Fatalf("mint: stored %v; output %q %q", store.rec != nil, out.String(), errb.String())
	}
	if views := e.tokenList("Ann"); len(views) != 1 || views[0].ID != store.rec.ID || views[0].Name != "Claude Desktop" {
		t.Fatalf("Manager's list: %+v", views)
	}
	out.Reset()
	if err := tc.List(ctx); err != nil || !strings.Contains(out.String(), "*  "+store.rec.ID) {
		t.Fatalf("list: %v\n%s", err, out.String())
	}
	// An admin query, for the agent to read.
	qry := string(e.queryWrite("admin", "/api/queries", map[string]any{"name": "A clips", "folders": []string{"Clients/A"}, "extensions": []string{"mov"}}, 200).Query.ID)

	logs := &bytes.Buffer{}
	logger := log.New(logs, "", 0)
	tools := &agentmcp.Tools{Mount: "/Volumes/zpool", Log: logger, Sleep: func(time.Duration) {}, Client: func() (*agentmcp.Client, error) {
		rec, err := store.Load()
		if err != nil {
			return nil, err
		}
		return agentmcp.NewAgentClient(rec.ManagerURL, rec.Token, hc), nil
	}}
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &mcpClient{t: t, in: inW, lines: make(chan string, 64)}
	done := make(chan error, 1)
	go func() {
		done <- (&agentmcp.Server{Tools: tools, Log: logger, Version: "test"}).Serve(ctx, inR, outW)
		outW.Close()
	}()
	go func() {
		r := bufio.NewReader(outR)
		for {
			l, err := r.ReadString('\n')
			if l != "" {
				c.lines <- l
			}
			if err != nil {
				return
			}
		}
	}()
	hello := `{"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}` + "\n" +
		`{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"
	if _, err := io.WriteString(inW, hello); err != nil {
		t.Fatal(err)
	}
	if l := <-c.lines; !strings.Contains(l, `"protocolVersion":"2025-06-18"`) {
		t.Fatalf("initialize: %s", l)
	}

	// Find, a page at a time, within Ann's grants (/Clients/A).
	_, isErr, p1 := c.call("find_files", map[string]any{"name": "clip", "kinds": []string{"video"}, "limit": 15})
	files := p1["files"].([]any)
	if isErr || len(files) != 15 || p1["next"] == nil {
		t.Fatalf("page 1: %v", p1)
	}
	_, _, p2 := c.call("find_files", map[string]any{"name": "clip", "kinds": []string{"video"}, "limit": 15, "cursor": p1["next"]})
	files = append(files, p2["files"].([]any)...)
	if len(files) != 20 || p2["next"] != nil {
		t.Fatalf("page 2: %v", p2)
	}
	var ids []string
	for _, f := range files {
		m := f.(map[string]any)
		path := m["path"].(string)
		if !strings.HasPrefix(path, "/Clients/A/clip") || m["mount_path"] != "/Volumes/zpool"+path {
			t.Fatalf("file %v", m)
		}
		ids = append(ids, m["asset_id"].(string))
	}
	if _, _, none := c.call("find_files", map[string]any{"kinds": []string{"audio"}}); len(none["files"].([]any)) != 0 {
		t.Fatalf("Ann's agent sees B's sounds: %v", none)
	}
	_, _, f0 := c.call("get_file", map[string]any{"asset_id": ids[0]})
	if f0["mount_path"] != files[0].(map[string]any)["mount_path"] {
		t.Fatalf("get_file: %v", f0)
	}
	var bAsset string
	for _, f := range e.find("admin", map[string][]string{"folder": {"Clients/B"}, "limit": {"1"}}).Files {
		bAsset = string(f.AssetID)
	}
	if text, isErr, _ := c.call("get_file", map[string]any{"asset_id": bAsset}); !isErr || !strings.Contains(text, "Not found") {
		t.Fatalf("get_file outside the grants: %s", text)
	}

	// Collections: create, retry, add, members, rename, remove.
	_, isErr, created := c.call("create_collection", map[string]any{"name": "Agent picks", "asset_ids": ids[:2], "operation_id": "pick-1"})
	if isErr {
		t.Fatalf("create: %v", created)
	}
	col := created["collection"].(map[string]any)
	colID := col["id"].(string)
	if col["members"] != 2.0 || created["operation_id"] != "pick-1" {
		t.Fatalf("create: %v", created)
	}
	if _, _, again := c.call("create_collection", map[string]any{"name": "Agent picks", "asset_ids": ids[:2], "operation_id": "pick-1"}); again["collection"].(map[string]any)["id"] != colID {
		t.Fatalf("retry made another collection: %v", again)
	}
	if _, _, l := c.call("list_collections", map[string]any{}); len(l["collections"].([]any)) != 1 {
		t.Fatalf("list: %v", l)
	}
	if _, isErr, _ := c.call("add_to_collection", map[string]any{"collection_id": colID, "asset_ids": ids[2:4]}); isErr {
		t.Fatal("add")
	}
	if text, isErr, _ := c.call("add_to_collection", map[string]any{"collection_id": colID, "asset_ids": []string{bAsset}}); !isErr || !strings.Contains(text, "project grants") {
		t.Fatalf("add outside the grants: %s", text)
	}
	_, _, mem := c.call("get_collection_members", map[string]any{"collection_id": colID, "limit": 3})
	ms := mem["members"].([]any)
	if len(ms) != 3 || mem["next"] == nil {
		t.Fatalf("members: %v", mem)
	}
	for _, m := range ms {
		m := m.(map[string]any)
		if m["state"] != "present" || m["mount_path"] != "/Volumes/zpool/"+m["path"].(string) {
			t.Fatalf("member %v", m)
		}
	}
	if _, isErr, r := c.call("rename_collection", map[string]any{"collection_id": colID, "name": "Agent selects"}); isErr || r["collection"].(map[string]any)["name"] != "Agent selects" {
		t.Fatalf("rename: %v", r)
	}
	if _, isErr, r := c.call("remove_from_collection", map[string]any{"collection_id": colID, "asset_ids": ids[:1]}); isErr || r["collection"].(map[string]any)["members"] != 3.0 {
		t.Fatalf("remove: %v", r)
	}

	// Saved queries.
	if _, _, q := c.call("list_saved_queries", map[string]any{}); len(q["queries"].([]any)) != 1 {
		t.Fatalf("queries: %v", q)
	}
	_, isErr, qr := c.call("get_saved_query_results", map[string]any{"query_id": qry})
	if isErr || len(qr["results"].([]any)) != 23 { // 20 clips, one, two, sub/four
		t.Fatalf("results: %v", qr)
	}
	for _, r := range qr["results"].([]any) {
		r := r.(map[string]any)
		if r["mount_path"] != "/Volumes/zpool/"+r["path"].(string) {
			t.Fatalf("result %v", r)
		}
	}

	// The person revokes the token from their Mac: the agent stops at once.
	id := store.rec.ID
	stale := *store.rec
	out.Reset()
	if err := tc.Revoke(ctx, id, false); err != nil || store.rec != nil || !strings.Contains(out.String(), "Revoked token "+id) {
		t.Fatalf("revoke: %v %s", err, out.String())
	}
	store.rec = &stale // an agent holding the old token
	if text, isErr, _ := c.call("find_files", map[string]any{}); !isErr || !strings.Contains(text, "juicemount-mcp token create") {
		t.Fatalf("after revoke: %s", text)
	}
	if text, isErr, _ := c.call("add_to_collection", map[string]any{"collection_id": colID, "asset_ids": ids[:1]}); !isErr || !strings.Contains(text, "juicemount-mcp token create") {
		t.Fatalf("edit after revoke: %s", text)
	}
	if strings.Contains(logs.String(), stale.Token[21:]) || strings.Contains(logs.String(), ann[1]) {
		t.Fatal("the server log holds a secret")
	}
	inW.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the server did not stop at EOF")
	}
}
