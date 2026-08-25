package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// fakeQLDeps builds a qlDeps over an in-memory world, mirroring the
// fakeThumbDeps pattern used for /thumb-local: one known file
// ("clips/a.braw" → inode 42), one known dir ("clips"), a cache holding a
// preview blob only when requested.
func fakeQLDeps(t *testing.T, cachedInode uint64, populateOK bool) (*qlDeps, *[]string) {
	t.Helper()
	blob := filepath.Join(t.TempDir(), "qlpreview.mp4")
	if err := os.WriteFile(blob, []byte("\x00\x00\x00\x18ftypmp4bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	warmed := &[]string{}
	return &qlDeps{
		mount: "/Volumes/zpool",
		lookup: func(rel string) (uint64, bool, bool) {
			switch rel {
			case "clips/a.braw":
				return 42, false, true
			case "clips":
				return 7, true, true
			}
			return 0, false, false
		},
		cachePath: func(ino uint64) (string, string, bool) {
			if ino == cachedInode {
				return filepath.Dir(blob), filepath.Base(blob), true
			}
			return "", "", false
		},
		populate: func(ino uint64) (string, string, bool) {
			if populateOK {
				return filepath.Dir(blob), filepath.Base(blob), true
			}
			return "", "", false
		},
		warmDir: func(rel string) { *warmed = append(*warmed, rel) },
	}, warmed
}

func getQLPreview(t *testing.T, d *qlDeps, target string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	serveQLPreview(w, r, *d)
	return w
}

func TestQLPreviewCacheHit(t *testing.T) {
	d, warmed := fakeQLDeps(t, 42, false)
	w := getQLPreview(t, d, "/ql-preview?path=/Volumes/zpool/clips/a.braw")
	if w.Code != http.StatusOK || w.Header().Get("X-JM-QL") != "hit" {
		t.Fatalf("want 200/hit, got %d/%s", w.Code, w.Header().Get("X-JM-QL"))
	}
	if ct := w.Header().Get("Content-Type"); ct != "video/mp4" {
		t.Fatalf("Content-Type = %q, want video/mp4", ct)
	}
	if w.Header().Get("Accept-Ranges") != "bytes" {
		t.Fatal("ServeContent must advertise byte ranges (AVPlayer seek)")
	}
	if len(*warmed) != 0 {
		t.Fatal("hit path must not warm")
	}
}

func TestQLPreviewMissPopulates(t *testing.T) {
	d, _ := fakeQLDeps(t, 0, true)
	w := getQLPreview(t, d, "/ql-preview?path=/Volumes/zpool/clips/a.braw")
	if w.Code != http.StatusOK || w.Header().Get("X-JM-QL") != "populated" {
		t.Fatalf("want 200/populated, got %d/%s", w.Code, w.Header().Get("X-JM-QL"))
	}
}

func TestQLPreviewMissWarmsAnd404s(t *testing.T) {
	d, warmed := fakeQLDeps(t, 0, false)
	w := getQLPreview(t, d, "/ql-preview?path=/Volumes/zpool/clips/a.braw")
	if w.Code != http.StatusNotFound || w.Header().Get("X-JM-QL") != "miss" {
		t.Fatalf("want 404/miss, got %d/%s", w.Code, w.Header().Get("X-JM-QL"))
	}
	if len(*warmed) != 1 || (*warmed)[0] != "clips" {
		t.Fatalf("warm calls = %v, want [clips]", *warmed)
	}
}

func TestQLPreviewRejections(t *testing.T) {
	d, _ := fakeQLDeps(t, 42, false)
	cases := map[string]struct {
		target string
		code   int
		tag    string
	}{
		"no path":       {"/ql-preview", http.StatusBadRequest, ""},
		"relative path": {"/ql-preview?path=clips/a.braw", http.StatusBadRequest, ""},
		"outside mount": {"/ql-preview?path=/Users/x/a.braw", http.StatusNotFound, "outside-mount"},
		"prefix cousin": {"/ql-preview?path=/Volumes/zpool2/a.braw", http.StatusNotFound, "outside-mount"},
		"mount root":    {"/ql-preview?path=/Volumes/zpool", http.StatusNotFound, "filtered"},
		"filtered ns":   {"/ql-preview?path=/Volumes/zpool/.juicemount/derivatives/9/qlpreview.mp4", http.StatusNotFound, "filtered"},
		"unknown path":  {"/ql-preview?path=/Volumes/zpool/clips/nope.braw", http.StatusNotFound, "unknown-path"},
		"directory":     {"/ql-preview?path=/Volumes/zpool/clips", http.StatusNotFound, "unknown-path"},
	}
	for name, c := range cases {
		w := getQLPreview(t, d, c.target)
		if w.Code != c.code {
			t.Errorf("%s: code = %d, want %d", name, w.Code, c.code)
		}
		if c.tag != "" && w.Header().Get("X-JM-QL") != c.tag {
			t.Errorf("%s: tag = %q, want %q", name, w.Header().Get("X-JM-QL"), c.tag)
		}
	}
}

// --- F5 prewarm core: hot-project pick + recency collection over fake trees ---

func mkChild(path, name string, isDir bool, age time.Duration, size int64) qlChild {
	return qlChild{Inode: uint64(1000 + age.Milliseconds()), Path: path, Name: name,
		IsDir: isDir, Mtime: time.Now().Add(-age), Size: size}
}

func fakeTree(dirs map[string][]qlChild) qlTree {
	return qlTree{children: func(d string) []qlChild { return dirs[d] }}
}

func TestHotProjectDirPicksNewestSubtree(t *testing.T) {
	tree := fakeTree(map[string][]qlChild{
		".": {
			mkChild("Job A", "Job A", true, 9*time.Hour, 0),
			mkChild("Job B", "Job B", true, 9*time.Hour, 0),
		},
		// Job A's newest media is 8h old…
		"Job A": {mkChild("Job A/old.braw", "old.braw", false, 8*time.Hour, 1<<20)},
		// …Job B's is 5 minutes old → the hot project.
		"Job B": {
			mkChild("Job B/fresh.r3d", "fresh.r3d", false, 5*time.Minute, 2<<20),
			mkChild("Job B/take2.mov", "take2.mov", false, 6*time.Hour, 3<<20),
		},
	})
	if got := tree.hotProjectDir(); got != "Job B" {
		t.Fatalf("hotProjectDir = %q, want %q", got, "Job B")
	}
}

func TestHotProjectDirRootLooseMediaFallback(t *testing.T) {
	now := time.Now()
	tree := fakeTree(map[string][]qlChild{
		".": {mkChild("loose.mxf", "loose.mxf", false, time.Since(now), 4<<20)},
	})
	if got := tree.hotProjectDir(); got != "." {
		t.Fatalf("hotProjectDir = %q, want root fallback %q", got, ".")
	}
}

func TestCollectMediaNewestFirstAndFiltered(t *testing.T) {
	tree := fakeTree(map[string][]qlChild{
		"proj": {
			mkChild("proj/old.mov", "old.mov", false, 4*time.Hour, 1),
			mkChild("proj/new.braw", "new.braw", false, time.Minute, 2),
			mkChild("proj/mid.r3d", "mid.r3d", false, time.Hour, 3),
			// never collected:
			mkChild("proj/._old.mov", "._old.mov", false, time.Second, 9),
			mkChild("proj/notes.txt", "notes.txt", false, time.Second, 9),
			mkChild("proj/sub", "sub", true, time.Second, 0),
		},
		"proj/sub": {mkChild("proj/sub/deep.wav", "deep.wav", false, 2*time.Hour, 5)},
	})
	got := tree.collectMedia("proj")
	var names []string
	for _, f := range got {
		names = append(names, f.Name)
	}
	want := []string{"new.braw", "mid.r3d", "deep.wav", "old.mov"} // newest first, subdirs walked
	if len(names) != len(want) {
		t.Fatalf("collected %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("order = %v, want %v", names, want)
		}
	}
}
