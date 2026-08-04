package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// fakeThumbDeps builds a thumbLocalDeps over an in-memory world: one known
// file ("clips/a.mov" → inode 42), one known dir ("clips"), a cache that
// holds a poster for cachedInode, and recorders for populate/warm calls.
func fakeThumbDeps(t *testing.T, cachedInode uint64, populateOK bool) (thumbLocalDeps, *[]string, *[]uint64) {
	t.Helper()
	blob := filepath.Join(t.TempDir(), "poster.jpg")
	if err := os.WriteFile(blob, []byte("\xff\xd8jpegbytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	warmed := &[]string{}
	populated := &[]uint64{}
	return thumbLocalDeps{
		mount: "/Volumes/zpool",
		lookup: func(rel string) (uint64, bool, bool) {
			switch rel {
			case "clips/a.mov":
				return 42, false, true
			case "clips":
				return 7, true, true
			}
			return 0, false, false
		},
		// (root, rel): /thumb-local serves through the anchored open now, so the
		// fake must hand back a split path exactly as the real providers do.
		cachePath: func(ino uint64) (string, string, bool) {
			if ino == cachedInode {
				return filepath.Dir(blob), filepath.Base(blob), true
			}
			return "", "", false
		},
		populate: func(ino uint64) (string, string, bool) {
			*populated = append(*populated, ino)
			if populateOK {
				return filepath.Dir(blob), filepath.Base(blob), true
			}
			return "", "", false
		},
		warmDir: func(rel string) { *warmed = append(*warmed, rel) },
	}, warmed, populated
}

func getThumbLocal(t *testing.T, d thumbLocalDeps, target string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil)
	w := httptest.NewRecorder()
	serveThumbLocal(w, r, d)
	return w
}

func TestThumbLocalCacheHit(t *testing.T) {
	d, warmed, populated := fakeThumbDeps(t, 42, false)
	w := getThumbLocal(t, d, "/thumb-local?path=/Volumes/zpool/clips/a.mov&size=256")
	if w.Code != http.StatusOK || w.Header().Get("X-JM-Thumb") != "hit" {
		t.Fatalf("want 200/hit, got %d/%s", w.Code, w.Header().Get("X-JM-Thumb"))
	}
	if ct := w.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if len(*warmed) != 0 || len(*populated) != 0 {
		t.Fatal("hit path must not warm or populate")
	}
}

func TestThumbLocalMissPopulates(t *testing.T) {
	d, warmed, populated := fakeThumbDeps(t, 0, true) // nothing cached, populate succeeds
	w := getThumbLocal(t, d, "/thumb-local?path=/Volumes/zpool/clips/a.mov")
	if w.Code != http.StatusOK || w.Header().Get("X-JM-Thumb") != "populated" {
		t.Fatalf("want 200/populated, got %d/%s", w.Code, w.Header().Get("X-JM-Thumb"))
	}
	if len(*populated) != 1 || (*populated)[0] != 42 {
		t.Fatalf("populate calls = %v, want [42]", *populated)
	}
	if len(*warmed) != 0 {
		t.Fatal("successful populate must not also warm")
	}
}

func TestThumbLocalMissWarmsAnd404s(t *testing.T) {
	d, warmed, _ := fakeThumbDeps(t, 0, false) // nothing cached, populate fails
	w := getThumbLocal(t, d, "/thumb-local?path=/Volumes/zpool/clips/a.mov")
	if w.Code != http.StatusNotFound || w.Header().Get("X-JM-Thumb") != "miss" {
		t.Fatalf("want 404/miss, got %d/%s", w.Code, w.Header().Get("X-JM-Thumb"))
	}
	if len(*warmed) != 1 || (*warmed)[0] != "clips" {
		t.Fatalf("warm calls = %v, want [clips]", *warmed)
	}
}

func TestThumbLocalRejections(t *testing.T) {
	d, _, _ := fakeThumbDeps(t, 42, false)
	cases := map[string]struct {
		target string
		code   int
		tag    string
	}{
		"no path":        {"/thumb-local", http.StatusBadRequest, ""},
		"relative path":  {"/thumb-local?path=clips/a.mov", http.StatusBadRequest, ""},
		"outside mount":  {"/thumb-local?path=/Users/x/a.mov", http.StatusNotFound, "outside-mount"},
		"prefix cousin":  {"/thumb-local?path=/Volumes/zpool2/a.mov", http.StatusNotFound, "outside-mount"},
		"mount root":     {"/thumb-local?path=/Volumes/zpool", http.StatusNotFound, "filtered"},
		"filtered ns":    {"/thumb-local?path=/Volumes/zpool/.juicemount/derivatives/9/poster.jpg", http.StatusNotFound, "filtered"},
		"unknown path":   {"/thumb-local?path=/Volumes/zpool/clips/nope.mov", http.StatusNotFound, "unknown-path"},
		"directory":      {"/thumb-local?path=/Volumes/zpool/clips", http.StatusNotFound, "unknown-path"},
		"dot-dot escape": {"/thumb-local?path=/Volumes/zpool/../etc/passwd", http.StatusNotFound, "outside-mount"},
	}
	for name, c := range cases {
		w := getThumbLocal(t, d, c.target)
		if w.Code != c.code {
			t.Errorf("%s: code = %d, want %d", name, w.Code, c.code)
		}
		if c.tag != "" && w.Header().Get("X-JM-Thumb") != c.tag {
			t.Errorf("%s: tag = %q, want %q", name, w.Header().Get("X-JM-Thumb"), c.tag)
		}
	}
}
