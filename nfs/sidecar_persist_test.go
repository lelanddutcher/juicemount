package nfs

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSidecarPersistRoundTrip pins the snapshot contract: entries survive a
// save→load cycle into a fresh cache, per-serve validation still governs
// (an entry loads fine but misses when the mirror meta moved), and the
// put-side guards re-apply on load (a corrupted partial entry is refused).
func TestSidecarPersistRoundTrip(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "sidecars.gob")

	c1 := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c1.persistPath = snap
	body := []byte("applefile-bytes")
	c1.put("d/._clip.mov", body, 100, int64(len(body)))
	dsBody := []byte("ds-store-bytes")
	c1.put("d/.DS_Store", dsBody, 200, int64(len(dsBody)))
	c1.saveToDisk()

	// Fresh cache (a new process) restores both entries.
	c2 := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c2.persistPath = snap
	c2.loadFromDisk()
	if got, ok := c2.get("d/._clip.mov", 100, int64(len(body))); !ok || string(got) != string(body) {
		t.Fatalf("sidecar entry did not survive restart: ok=%v", ok)
	}
	if got, ok := c2.get("d/.DS_Store", 200, int64(len(dsBody))); !ok || string(got) != string(dsBody) {
		t.Fatalf(".DS_Store entry did not survive restart: ok=%v", ok)
	}
	// Mirror meta moved while the app was down → loaded entry must MISS.
	if _, ok := c2.get("d/._clip.mov", 101, int64(len(body))); ok {
		t.Fatal("stale loaded entry served after mtime change — validation hole")
	}
}

func TestSidecarPersistLoadGuards(t *testing.T) {
	snap := filepath.Join(t.TempDir(), "sidecars.gob")
	// Hand-craft a snapshot with one good and one corrupt (partial) entry.
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c.persistPath = snap
	c.m["good"] = &sidecarEntry{data: []byte("abcd"), mtime: 1, size: 4}
	c.m["partial"] = &sidecarEntry{data: []byte("ab"), mtime: 1, size: 999} // len != size
	c.bytes = 6
	c.saveToDisk()

	c2 := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c2.persistPath = snap
	c2.loadFromDisk()
	if _, ok := c2.get("good", 1, 4); !ok {
		t.Fatal("good entry not restored")
	}
	if _, ok := c2.get("partial", 1, 999); ok {
		t.Fatal("PARTIAL entry restored — the membuf-stale guard must re-apply on load")
	}
	// Unreadable snapshot degrades to cold, never errors.
	if err := os.WriteFile(snap, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	c3 := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c3.persistPath = snap
	c3.loadFromDisk() // must not panic
	if n, _ := c3.stats(); n != 0 {
		t.Fatalf("garbage snapshot loaded %d entries", n)
	}
}

func TestCacheableMetaName(t *testing.T) {
	cases := map[string]bool{
		"._clip.mov": true, ".DS_Store": true,
		"clip.mov": false, "DS_Store": false, ".ds_store": false, "._": false, "": false,
	}
	for name, want := range cases {
		if got := cacheableMetaName(name); got != want {
			t.Errorf("cacheableMetaName(%q)=%v want %v", name, got, want)
		}
	}
}
