package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckSyncSmartRoots(t *testing.T) {
	src := t.TempDir()
	for _, d := range []string{"Clients", "recents"} {
		if err := os.Mkdir(filepath.Join(src, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	plain := t.TempDir()
	if err := os.Mkdir(filepath.Join(plain, "Clients"), 0o755); err != nil {
		t.Fatal(err)
	}
	named := filepath.Join(t.TempDir(), "Collections")
	if err := os.Mkdir(named, 0o755); err != nil {
		t.Fatal(err)
	}
	env := func(kv ...string) func(string) string {
		m := map[string]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return func(k string) string { return m[k] }
	}
	off := env()
	recents := env("JM_SMART_FOLDERS", "on")
	both := env("JM_SMART_FOLDERS", "on", "JM_CONTINUITY", "on", "JM_SMART_FOLDERS_ROOT", " Recent ", "JM_SMART_FOLDERS_COLLECTIONS_ROOT", "Sets")
	withColl := env("JM_SMART_FOLDERS", "on", "JM_CONTINUITY", "on")
	for _, tc := range []struct {
		name            string
		getenv          func(string) string
		src, dst, mount string
		refused         bool
	}{
		{"off", off, src, "/jfs/Recents/x", "", false},
		{"off at the root", off, src, "/jfs", "", false},
		{"into Recents", recents, plain, "/jfs/Recents/x", "", true},
		{"into recents (case)", recents, plain, "/jfs/recents", "", true},
		{"into Recents, dotted", recents, plain, "/jfs/./Recents/", "", true},
		{"into a project", recents, src, "/jfs/Clients/A", "", false},
		{"Collections without continuity", recents, plain, "/jfs/Collections", "", false},
		{"into Collections", withColl, plain, "/jfs/Collections/x", "", true},
		{"configured names", both, plain, "/jfs/Sets/x", "", true},
		{"configured names, default not in use", both, plain, "/jfs/Recents/x", "", false},
		{"root, entry named like a root", recents, src, "/jfs", "", true},
		{"root, source named like a root", withColl, named, "/jfs/", "", true},
		{"root, plain source", withColl, plain, "/jfs", "", false},
		{"root, unreadable source", recents, filepath.Join(plain, "missing"), "/jfs", "", true},
		{"not the volume", recents, src, "/external/x", "", false},
		// Every form of a destination in the volume is checked.
		{"dot-dot back into the volume", recents, plain, "/jfs/../jfs/Recents", "", true},
		{"dot-dot within the volume", recents, plain, "/jfs/Clients/../Recents/x", "", true},
		{"dot-dot elsewhere", recents, plain, "/jfs/Clients/../Clients", "", true},
		{"jfs URL into Recents", recents, plain, "jfs://zpool/Recents/x", "", true},
		{"jfs URL, case", recents, plain, "JFS://zpool//recents", "", true},
		{"jfs URL into a project", recents, plain, "jfs://zpool/Clients", "", false},
		{"jfs URL at the root", recents, src, "jfs://zpool/", "", true},
		{"jfs URL at the root, plain source", recents, plain, "jfs://zpool", "", false},
		{"file URL under the FUSE mount", recents, plain, "file:///mnt/jfs/Recents/x", "/mnt/jfs", true},
		{"host path under the FUSE mount", recents, plain, "/mnt/jfs/Recents", "/mnt/jfs/", true},
		{"FUSE mount root", recents, src, "file:///mnt/jfs", "/mnt/jfs", true},
		{"file URL, no FUSE mount", recents, plain, "file:///mnt/jfs/Recents", "", false},
		{"object store", recents, plain, "s3://bucket/Recents", "", false},
	} {
		err := checkSyncSmartRoots(tc.getenv, os.ReadDir, tc.src, tc.dst, tc.mount)
		if (err != nil) != tc.refused {
			t.Errorf("%s: err = %v, refused want %v", tc.name, err, tc.refused)
		}
	}
}

// The direction check and RunSync itself (scheduled jobs) both refuse.
func TestSyncRefusesSmartFolderRoot(t *testing.T) {
	t.Setenv("JM_SMART_FOLDERS", "on")
	src := t.TempDir()
	if err := validateDirectionPair(DirectionIn, src, "/jfs/Recents/import"); err == nil || !strings.Contains(err.Error(), "smart-folder root") {
		t.Fatalf("validateDirectionPair = %v", err)
	}
	if err := validateDirectionPair(DirectionIn, src, "/jfs/Clients"); err != nil {
		t.Fatalf("validateDirectionPair into a project = %v", err)
	}
	for _, dst := range []string{"/jfs/Recents/import", "jfs://zpool/Recents/import", "/jfs/../jfs/Recents"} {
		err := RunSync(context.Background(), "/nonexistent/juicefs", RunSyncSpec{Mode: ModeStandalone, VolName: "zpool"}, src, dst, SyncOptions{}, nil, nil)
		if err == nil || !(strings.Contains(err.Error(), "smart-folder root") || strings.Contains(err.Error(), `".."`)) {
			t.Fatalf("RunSync to %s = %v", dst, err)
		}
	}
	err := RunSync(context.Background(), "/nonexistent/juicefs", RunSyncSpec{Mode: ModeEmbedded, FUSEMount: "/mnt/jfs"}, src, "file:///mnt/jfs/Recents/import", SyncOptions{}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "smart-folder root") {
		t.Fatalf("RunSync into the FUSE mount = %v", err)
	}
}
