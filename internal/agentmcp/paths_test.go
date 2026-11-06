package agentmcp

import (
	"strings"
	"testing"
)

func TestMountPathRefusesEscapes(t *testing.T) {
	for _, c := range []struct {
		p    string
		form pathForm
		want string // "" = refused
	}{
		{"/Clients/A/one.mov", rootedPath, "/Volumes/zpool/Clients/A/one.mov"},
		{"/Clients/./A//one.mov", rootedPath, "/Volumes/zpool/Clients/A/one.mov"},
		{"/a b/ü:x.mov", rootedPath, "/Volumes/zpool/a b/ü:x.mov"},
		{`/A/back\slash..mov`, rootedPath, `/Volumes/zpool/A/back\slash..mov`},
		{"Clients/A/one.mov", relativePath, "/Volumes/zpool/Clients/A/one.mov"},
		{"..hidden/x", relativePath, "/Volumes/zpool/..hidden/x"},
		{"", rootedPath, ""},
		{"/", rootedPath, ""},
		{"/.", rootedPath, ""},
		{"Clients/A/one.mov", rootedPath, ""}, // not the answer's form
		{"/Clients/A/one.mov", relativePath, ""},
		{"//etc/passwd", rootedPath, ""},
		{"/../etc/passwd", rootedPath, ""},
		{"/Clients/../../etc/passwd", rootedPath, ""},
		{"/Clients/A/..", rootedPath, ""},
		{"/Clients/../A/x", rootedPath, ""}, // any ".." is refused, even one that stays inside
		{"../zpool2/x", relativePath, ""},
		{"..", relativePath, ""},
		{".", relativePath, ""},
		{"a/\x00/b", relativePath, ""},
	} {
		got, ok := mountPath("/Volumes/zpool", c.p, c.form)
		if ok != (c.want != "") || got != c.want {
			t.Errorf("mountPath(%q, %d) = %q %v, want %q", c.p, c.form, got, ok, c.want)
		}
	}
	for _, m := range []string{"", "relative", "/", "/Volumes/zpool/", "/Volumes/../zpool"} {
		if _, err := CleanMount(m); err == nil {
			t.Errorf("CleanMount(%q) accepted", m)
		}
	}
}

// Results whose path would leave the mount root are dropped from every tool
// that returns paths, and counted in the log and the result.
func TestToolsSkipEscapingPaths(t *testing.T) {
	f, srv := newFakeManager(t)
	s := startSession(t, srv.URL)
	s.initialize()
	f.on("GET /api/continuity/find", 200, map[string]any{"files": []any{
		fileJSON(asset1, "/Clients/A/ok.mov"), fileJSON(asset2, "/../../etc/passwd"), fileJSON(asset2, "//etc/hosts"),
	}})
	_, out := s.call("find_files", map[string]any{})
	if files := out["files"].([]any); len(files) != 1 || out["skipped_unsafe_paths"] != 2.0 {
		t.Fatalf("find: %v", out)
	}
	f.on("GET /api/continuity/asset/"+asset2, 200, fileJSON(asset2, "/x/../../y"))
	if cr, _ := s.call("get_file", map[string]any{"asset_id": asset2}); !cr.IsError || !strings.Contains(cr.Content[0].Text, "mount root") {
		t.Fatalf("get_file: %+v", cr)
	}
	f.on("GET /api/collections/"+col1+"/members", 200, map[string]any{"members": []any{
		map[string]any{"asset_id": asset1, "path": "../outside", "state": "present"},
		map[string]any{"asset_id": asset2, "path": "/abs", "state": "present", "mount_path": "/etc/passwd"},
		map[string]any{"asset_id": asset2, "path": "A/in.mov", "state": "present", "mount_path": "/etc/passwd"},
	}})
	_, out = s.call("get_collection_members", map[string]any{"collection_id": col1})
	members := out["members"].([]any)
	if len(members) != 1 || members[0].(map[string]any)["mount_path"] != "/Volumes/zpool/A/in.mov" || out["skipped_unsafe_paths"] != 2.0 {
		t.Fatalf("members: %v", out)
	}
	f.on("GET /api/queries/"+qry1+"/results", 200, map[string]any{"results": []any{
		map[string]any{"asset_id": asset1, "path": "A/../../x"}, map[string]any{"asset_id": asset1, "path": "A/b.mov"},
	}, "skipped_unsafe_paths": 40})
	_, out = s.call("get_saved_query_results", map[string]any{"query_id": qry1})
	if len(out["results"].([]any)) != 1 || out["skipped_unsafe_paths"] != 1.0 {
		t.Fatalf("results: %v", out)
	}
	logs := s.logs.String()
	for _, want := range []string{"find_files: skipped 2 result(s)", "get_file: skipped 1", "get_collection_members: skipped 2", "get_saved_query_results: skipped 1"} {
		if !strings.Contains(logs, want) {
			t.Errorf("log lacks %q:\n%s", want, logs)
		}
	}
}
