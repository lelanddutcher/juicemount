package metadata

import (
	"path/filepath"
	"testing"
	"time"
)

// TestSyntheticHandleSurvivesInodeChurn is the regression guard for the
// 2026-06-14 ESTALE-mid-copy bug: a synthetic inode handed out by ToHandle
// loses its inodeCache entry when the reconcile replaces it with a real inode,
// but the client still holds the synthetic handle. The synthetic-handle map
// must keep the path resolvable regardless of inodeCache churn.
func TestSyntheticHandleSurvivesInodeChurn(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "m.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()

	const path = "Film Projects/Shot 1/A001.CR3"
	const synthetic = uint64(1)<<63 | 0x1234 // high bit = synthetic

	// ToHandle hands out the synthetic inode for a just-created path.
	s.InsertToCache(MakeEntry(path, false, 0, time.Now(), synthetic))
	s.RecordSyntheticHandle(synthetic, path)

	// Sanity: resolvable via inodeCache now.
	if e := s.LookupByInode(synthetic); e == nil {
		t.Fatal("synthetic inode not in inodeCache right after insert")
	}

	// Reconcile arrives: the SAME path now gets JuiceFS's real inode. This is
	// what strands the synthetic handle — BulkInsert replaces the entry, and
	// the synthetic key is dropped from inodeCache on the next rebuild.
	const real = uint64(987654321)
	if err := s.BulkInsert([]*Entry{MakeEntry(path, false, 4096, time.Now(), real)}, 100); err != nil {
		t.Fatalf("bulk insert: %v", err)
	}

	// The synthetic inode is (or may be) gone from inodeCache now — that's the
	// bug. The synthetic-handle map must still recover the path.
	if p, ok := s.SyntheticHandlePath(synthetic); !ok || p != path {
		t.Fatalf("SyntheticHandlePath(synthetic) = %q,%v; want %q,true (handle stranded → ESTALE)", p, ok, path)
	}

	// Non-synthetic inodes are never recorded.
	s.RecordSyntheticHandle(42 /* high bit clear */, "x")
	if _, ok := s.SyntheticHandlePath(42); ok {
		t.Fatal("non-synthetic inode should not be recorded")
	}
}
