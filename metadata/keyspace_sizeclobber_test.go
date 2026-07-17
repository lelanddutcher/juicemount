package metadata

import (
	"encoding/binary"
	"testing"
	"time"
)

// encodeTestAttr builds a minimal JuiceFS i-key attr blob that
// decodeInodeAttr accepts (>=59 bytes; mtime BE64 at 23, size BE64 at 51).
func encodeTestAttr(mtime, size int64) []byte {
	b := make([]byte, 59)
	binary.BigEndian.PutUint64(b[23:], uint64(mtime))
	binary.BigEndian.PutUint64(b[51:], uint64(size))
	return b
}

// TestReconcileChildSizeClobberFix pins the 2026-07-10 live corruption
// (1,922 mirror rows zeroed): a child whose attr read FAILED must never
// upsert size 0 over a known-good row.
func TestReconcileChildSizeClobberFix(t *testing.T) {
	now := time.Now()
	known := map[string]*Entry{
		"proj/clip.mp4":  {Path: "proj/clip.mp4", Inode: 11, Size: 444416879, Mtime: now},
		"proj/empty.txt": {Path: "proj/empty.txt", Inode: 12, Size: 4096, Mtime: now},
		"proj/grown.mov": {Path: "proj/grown.mov", Inode: 13, Size: 100, Mtime: now},
	}
	lookup := func(p string) *Entry { return known[p] }

	children := []dirChild{
		{inode: 11, childPath: "proj/clip.mp4"},         // attr read FAILED (absent from map)
		{inode: 12, childPath: "proj/empty.txt"},        // attr OK, size genuinely 0
		{inode: 13, childPath: "proj/grown.mov"},        // attr OK, size changed
		{inode: 14, childPath: "proj/new.wav"},          // NEW child, attr failed
		{inode: 15, childPath: "proj/sub", isDir: true}, // NEW dir
	}
	attrs := map[uint64][]byte{
		12: encodeTestAttr(now.Unix(), 0),
		13: encodeTestAttr(now.Unix(), 999),
		15: encodeTestAttr(now.Unix(), 0),
	}

	toUpsert, newDirs := buildReconcileChildEntries(children, attrs, lookup)

	byPath := map[string]*Entry{}
	for _, e := range toUpsert {
		byPath[e.Path] = e
	}
	// THE FIX: failed-attr child with a known row → NO upsert (size preserved).
	if _, clobbered := byPath["proj/clip.mp4"]; clobbered {
		t.Fatalf("failed-attr child was upserted (would clobber 444MB with %d)", byPath["proj/clip.mp4"].Size)
	}
	// A genuinely-empty file (attr OK, size 0) DOES update.
	if e := byPath["proj/empty.txt"]; e == nil || e.Size != 0 {
		t.Fatalf("legit empty file not upserted to 0: %+v", e)
	}
	// A real size change flows through.
	if e := byPath["proj/grown.mov"]; e == nil || e.Size != 999 {
		t.Fatalf("size change lost: %+v", e)
	}
	// A new child with a failed attr still inserts (size 0, heals later).
	if e := byPath["proj/new.wav"]; e == nil || e.Size != 0 {
		t.Fatalf("new failed-attr child not inserted: %+v", e)
	}
	// New dir collected for the B4' requeue.
	if len(newDirs) != 1 || newDirs[0] != 15 {
		t.Fatalf("newDirs = %v, want [15]", newDirs)
	}
}

// TestReconcileChildMtimePreservedOnFailedRead: the preserve covers mtime too
// (a zero mtime would also diff-trigger a wasted upsert).
func TestReconcileChildMtimePreservedOnFailedRead(t *testing.T) {
	mt := time.Unix(1700000000, 0)
	known := map[string]*Entry{
		"a/f": {Path: "a/f", Inode: 7, Size: 123, Mtime: mt},
	}
	toUpsert, _ := buildReconcileChildEntries(
		[]dirChild{{inode: 7, childPath: "a/f"}},
		map[uint64][]byte{}, // total attr-fetch failure
		func(p string) *Entry { return known[p] },
	)
	if len(toUpsert) != 0 {
		t.Fatalf("failed-attr unchanged child produced an upsert: %+v", toUpsert[0])
	}
}
