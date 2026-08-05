package nfs

import (
	"hash/fnv"
	"os"
	"syscall"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
)

// A GETATTR on an in-flight (spool-active) file must report the file's REAL
// inode and owner — not a hash of its path, and not root:wheel.
//
// THE BUG (measured live 2026-08-05). spoolFileInfo.Sys() returned nil.
// nfslib.ToFileAttribute asks file.GetInfo(info) for the true attributes, that
// helper returns nil unless Sys() is a *syscall.Stat_t, and the nil branch
// FABRICATES the fileid as fnv.New64(path) while leaving UID/GID at their zero
// value. So for the entire spool window every stat() of an in-flight file
// reported an invented inode and root:wheel ownership — while READDIR, which
// takes a different path (metadata.FileInfo.Sys() DOES return a Stat_t), showed
// the correct values. `ls` looked right; `stat` did not.
//
// Live evidence: stat reported 8983717010171850480 (0x7cac9305e1ed6ef0) for a
// file READDIR correctly reported as 2002031.
//
// This is asserted through the REAL encoder (nfslib.ToFileAttribute) rather
// than by inspecting the struct, because the struct was never the problem — the
// loss happened in the conversion, which is exactly what a struct-level test
// cannot see.
func TestSpoolFileInfoReportsRealInodeAndOwner(t *testing.T) {
	const (
		path     = "REEL_0065/A001C001_260612_R1AB.mov"
		realIno  = uint64(2002031)
		fileSize = int64(4096)
	)
	fi := &spoolFileInfo{
		name:  "A001C001_260612_R1AB.mov",
		size:  fileSize,
		mtime: time.Unix(1785900000, 0),
		inode: realIno,
	}

	attr := nfslib.ToFileAttribute(fi, path)

	// The exact value the old code produced, recomputed here so the test names
	// the failure instead of just reporting a mismatch.
	h := fnv.New64()
	_, _ = h.Write([]byte(path))
	pathHash := h.Sum64()

	if attr.Fileid == pathHash {
		t.Fatalf("GETATTR fileid is the FNV hash of the path (%d) — the inode was "+
			"discarded by Sys() and fabricated by the encoder's fallback", pathHash)
	}
	if attr.Fileid != realIno {
		t.Errorf("fileid = %d, want the real inode %d", attr.Fileid, realIno)
	}
	if attr.UID != uint32(os.Getuid()) || attr.GID != uint32(os.Getgid()) {
		t.Errorf("owner = %d:%d, want %d:%d — the nil-Sys fallback left these at 0, "+
			"so an in-flight file appeared to be owned by root:wheel",
			attr.UID, attr.GID, os.Getuid(), os.Getgid())
	}
	if attr.Nlink != 1 {
		t.Errorf("Nlink = %d, want 1", attr.Nlink)
	}
	if attr.Filesize != uint64(fileSize) {
		t.Errorf("Filesize = %d, want %d — the Sys() change must not disturb the "+
			"in-flight size reporting that #85 depends on", attr.Filesize, fileSize)
	}
}

// Two different paths that share an inode (the same file seen twice) must report
// the SAME fileid. Under the path-hash fallback they could not: the fileid was a
// function of the path, so identity tracked the name rather than the file.
func TestSpoolFileInfoFileidFollowsInodeNotPath(t *testing.T) {
	const ino = uint64(2002031)
	mk := func(name string) *spoolFileInfo {
		return &spoolFileInfo{name: name, size: 1, mtime: time.Unix(1785900000, 0), inode: ino}
	}
	a := nfslib.ToFileAttribute(mk("clip.mov"), "A/clip.mov")
	b := nfslib.ToFileAttribute(mk("clip.mov"), "B/somewhere/else/clip.mov")
	if a.Fileid != b.Fileid {
		t.Errorf("same inode reported two different fileids (%d vs %d) — file identity "+
			"was following the PATH, which breaks hardlink detection and any client "+
			"using st_ino to tell files apart", a.Fileid, b.Fileid)
	}
	if a.Fileid != ino {
		t.Errorf("fileid = %d, want %d", a.Fileid, ino)
	}
}

// Sys() must return a type the encoder actually accepts. Returning some other
// struct would silently reinstate the fallback with no compile error.
func TestSpoolFileInfoSysIsStatT(t *testing.T) {
	fi := &spoolFileInfo{inode: 12345}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("Sys() returned %T, not *syscall.Stat_t — the encoder's GetInfo "+
			"helper type-asserts on exactly this and falls back to the path hash "+
			"on anything else", fi.Sys())
	}
	if st.Ino != 12345 {
		t.Errorf("Stat_t.Ino = %d, want 12345", st.Ino)
	}
}
