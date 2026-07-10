package nfs

import (
	"bytes"
	"testing"
)

func TestSidecarCacheGetPutValidation(t *testing.T) {
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	body := []byte("applefile-resource-fork")
	c.put("d/._clip.mov", body, 100, int64(len(body)))

	// Hit only when (mtime,size) match the current mirror values.
	if got, ok := c.get("d/._clip.mov", 100, int64(len(body))); !ok || !bytes.Equal(got, body) {
		t.Fatalf("valid get failed: ok=%v", ok)
	}
	// Changed mtime => the sidecar was rewritten (Finder tag/comment) => MISS.
	if _, ok := c.get("d/._clip.mov", 101, int64(len(body))); ok {
		t.Fatal("served a stale body after mtime changed")
	}
	// Changed size => MISS.
	if _, ok := c.get("d/._clip.mov", 100, int64(len(body))+1); ok {
		t.Fatal("served a stale body after size changed")
	}
}

func TestSidecarCacheRefusesPartial(t *testing.T) {
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	// len(data) != size => the membuf-stale-partial guard must refuse it.
	c.put("d/._x", []byte("half"), 200, 999)
	if _, ok := c.get("d/._x", 200, 999); ok {
		t.Fatal("cached a PARTIAL body — the exact membuf-stale-image bug class")
	}
	// Oversized refused.
	big := make([]byte, sidecarMaxFile+1)
	c.put("d/._big", big, 200, int64(len(big)))
	if _, ok := c.get("d/._big", 200, int64(len(big))); ok {
		t.Fatal("cached an oversized body")
	}
}

func TestSidecarCacheLRUEviction(t *testing.T) {
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 300}
	mk := func(n int) []byte { return make([]byte, n) }
	c.put("a", mk(100), 1, 100)
	c.put("b", mk(100), 1, 100)
	c.put("c", mk(100), 1, 100)
	// Touch a (most-recent), then insert d forcing eviction of the LRU (b).
	c.get("a", 1, 100)
	c.put("d", mk(100), 1, 100)
	if _, ok := c.get("b", 1, 100); ok {
		t.Fatal("LRU victim b survived; a was touched so b was oldest")
	}
	if _, ok := c.get("a", 1, 100); !ok {
		t.Fatal("touched entry a was wrongly evicted")
	}
	if files, bytesN := c.stats(); int64(bytesN) > c.maxBytes || files == 0 {
		t.Fatalf("cap violated: files=%d bytes=%d max=%d", files, bytesN, c.maxBytes)
	}
}

func TestSidecarCacheReplacementBytes(t *testing.T) {
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c.put("p", make([]byte, 100), 1, 100)
	c.put("p", make([]byte, 40), 2, 40) // rewrite smaller
	f, b := c.stats()
	if f != 1 || b != 40 {
		t.Fatalf("replacement byte-accounting wrong: files=%d bytes=%d, want 1/40", f, b)
	}
}

func TestSidecarCacheInvalidate(t *testing.T) {
	c := &sidecarCache{enabled: true, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c.put("p", []byte("x"), 1, 1)
	c.invalidate("p")
	if _, ok := c.get("p", 1, 1); ok {
		t.Fatal("entry survived invalidate")
	}
	if _, b := c.stats(); b != 0 {
		t.Fatalf("bytes not reclaimed on invalidate: %d", b)
	}
}

func TestSidecarCacheDisabled(t *testing.T) {
	c := &sidecarCache{enabled: false, m: map[string]*sidecarEntry{}, maxBytes: 1 << 20}
	c.put("p", []byte("x"), 1, 1)
	if _, ok := c.get("p", 1, 1); ok {
		t.Fatal("disabled cache served content")
	}
}

func TestIsSidecarName(t *testing.T) {
	cases := map[string]bool{
		"._clip.mov": true, "._x": true,
		"clip.mov": false, "._": false, "_.x": false, ".DS_Store": false, "": false,
	}
	for name, want := range cases {
		if got := isSidecarName(name); got != want {
			t.Errorf("isSidecarName(%q)=%v want %v", name, got, want)
		}
	}
}
