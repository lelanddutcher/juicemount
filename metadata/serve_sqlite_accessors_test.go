package metadata

import (
	"os"
	"sort"
	"testing"
	"time"
)

// --- Item 2: parity of the SQLite serve accessors vs the RAM accessors ---
//
// These tests compare the SQLite serve substrate (lookupByPathSQLite /
// lookupByInodeSQLite / listChildrenSQLite) against the RAM accessors on the
// SAME seeded store, proving JM_SERVE_FROM_SQLITE=1 returns identical results.
// seedStore lives in serve_sqlite_test.go (Item 1).

// entryEqual compares the load-bearing fields and ignores the lazy, RAM-only
// GETATTR cache.
func entryEqual(a, b *Entry) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Path == b.Path &&
		a.Name == b.Name &&
		a.ParentPath == b.ParentPath &&
		a.IsDir == b.IsDir &&
		a.Size == b.Size &&
		a.Mtime.Equal(b.Mtime) &&
		a.Inode == b.Inode &&
		a.Mode == b.Mode &&
		a.LocalOnly == b.LocalOnly
}

func TestServeParity_LookupByPathAndInode(t *testing.T) {
	const total = 500
	s := seedStore(t, "dir", total)
	defer s.Close()
	// Also seed a high-bit ("synthetic") inode and a local_only=true row and a
	// dir, to exercise the reinterpret / ModeDir / LocalOnly parity edges.
	now := time.Unix(1_700_000_500, 0)
	hi := MakeEntry("dir/highbit.bin", false, 42, now, 1<<63|99)
	if err := s.Insert(hi); err != nil {
		t.Fatalf("insert highbit: %v", err)
	}
	lo := MakeEntry("dir/localonly.tmp", false, 7, now, 2_000_001)
	lo.LocalOnly = true
	if err := s.Insert(lo); err != nil {
		t.Fatalf("insert localonly: %v", err)
	}
	sub := MakeEntry("dir/sub", true, 0, now, 2_000_002)
	if err := s.Insert(sub); err != nil {
		t.Fatalf("insert subdir: %v", err)
	}

	// Sample paths + inodes to compare across substrates.
	paths := []string{
		"dir/file_000000.mov",
		"dir/file_000123.mov",
		"dir/file_000499.mov",
		"dir/highbit.bin",
		"dir/localonly.tmp",
		"dir/sub",
		"dir/does_not_exist.mov", // must be nil on both
	}
	inodes := []uint64{1000, 1123, 1499, 1<<63 | 99, 2_000_001, 2_000_002, 999_999_999}

	// Bypass the public accessor (serveFromSQLite caches via sync.Once) and call
	// the *RAM / *SQLite helpers directly to compare both substrates in one
	// process without env/flag ordering games.
	for _, p := range paths {
		ram := s.lookupByPathRAM(p)
		sq := s.lookupByPathSQLite(p)
		if !entryEqual(ram, sq) {
			t.Fatalf("LookupByPath parity mismatch for %q:\n  RAM   =%+v\n  SQLite=%+v", p, ram, sq)
		}
	}
	for _, in := range inodes {
		ram := s.lookupByInodeRAM(in)
		sq := s.lookupByInodeSQLite(in)
		if !entryEqual(ram, sq) {
			t.Fatalf("LookupByInode parity mismatch for %d:\n  RAM   =%+v\n  SQLite=%+v", in, ram, sq)
		}
	}
}

func TestServeParity_ListChildren(t *testing.T) {
	const total = 1234
	s := seedStore(t, "dir", total)
	defer s.Close()

	ram, err := s.listChildrenRAM("dir")
	if err != nil {
		t.Fatalf("ram list: %v", err)
	}
	sq, err := s.listChildrenSQLite("dir")
	if err != nil {
		t.Fatalf("sqlite list: %v", err)
	}
	if len(ram) != len(sq) {
		t.Fatalf("child count mismatch: RAM=%d SQLite=%d", len(ram), len(sq))
	}
	// RAM order is map-iteration (nondeterministic); compare as sets keyed by
	// path, asserting field-level parity per entry.
	byPath := func(es []*Entry) map[string]*Entry {
		m := make(map[string]*Entry, len(es))
		for _, e := range es {
			m[e.Path] = e
		}
		return m
	}
	rm, sm := byPath(ram), byPath(sq)
	for p, re := range rm {
		se, ok := sm[p]
		if !ok {
			t.Fatalf("path %q present in RAM but missing in SQLite", p)
		}
		if !entryEqual(re, se) {
			t.Fatalf("ListChildren parity mismatch for %q:\n  RAM   =%+v\n  SQLite=%+v", p, re, se)
		}
	}
	// SQLite path must be name-sorted (its documented stronger contract).
	if !sort.SliceIsSorted(sq, func(i, j int) bool { return sq[i].Name < sq[j].Name }) {
		t.Fatalf("SQLite ListChildren not name-sorted")
	}
}

func TestServeParity_EmptyDir(t *testing.T) {
	s := seedStore(t, "dir", 0)
	defer s.Close()
	ram, _ := s.listChildrenRAM("dir")
	sq, _ := s.listChildrenSQLite("dir")
	// Both must return nil (not a non-nil empty slice) for parity — several
	// callers len()-check, but the nil-vs-empty distinction is asserted here to
	// lock the contract.
	if ram != nil {
		t.Fatalf("RAM empty dir returned non-nil: %#v", ram)
	}
	if sq != nil {
		t.Fatalf("SQLite empty dir returned non-nil: %#v", sq)
	}
}

// TestServeFlagOff_UsesRAM confirms the PUBLIC accessors read RAM when the flag
// is unset (default). The accessor returns a stable snapshot rather than
// exposing the mutable cache entry to concurrent Store writers.
func TestServeFlagOff_UsesRAM(t *testing.T) {
	if os.Getenv("JM_SERVE_FROM_SQLITE") == "1" {
		t.Skip("JM_SERVE_FROM_SQLITE=1 in env; this test asserts the default-off path")
	}
	s := seedStore(t, "dir", 3)
	defer s.Close()
	if serveFromSQLite() {
		t.Fatalf("serveFromSQLite() true with flag unset — default must be OFF")
	}
	// Both accessors use the RAM substrate and return equivalent, independent
	// snapshots. Pointer inequality is the concurrency-safety contract.
	pub := s.LookupByPath("dir/file_000001.mov")
	ram := s.lookupByPathRAM("dir/file_000001.mov")
	if pub == ram {
		t.Fatalf("public LookupByPath exposed the same mutable RAM cache pointer")
	}
	if !entryEqual(pub, ram) {
		t.Fatalf("public vs RAM entry differ with flag off")
	}
}
