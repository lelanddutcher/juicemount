package derivatives

import (
	"os"
	"path/filepath"
	"testing"
)

// CommitStagedAt must be FAIL-CLOSED on fsync. The helper it replaced returned
// before renaming when the fsync failed; the first version here moved the fsync
// but not its error handling, and published regardless. fsync is exactly where
// deferred writeback errors surface (EIO, or ENOSPC/EDQUOT under delayed
// allocation) — the case where the bytes are NOT on disk.
func TestCommitStagedFailsClosedWhenFsyncCannotRun(t *testing.T) {
	mount := t.TempDir()
	const inode = 960001
	rel := DerivDirRel(inode)
	if err := EnsureDirUnder(mount, rel); err != nil {
		t.Fatal(err)
	}
	dir, err := OpenDirUnder(mount, rel)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	staged, abs, err := StageNameAt(dir, "proxy.mp4")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte("REAL-PROXY-BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Make the fsync reopen fail deterministically: strip read permission from
	// the staged file. (Skipped as root, where mode bits are advisory.)
	if os.Geteuid() == 0 {
		t.Skip("running as root: mode bits do not deny the reopen")
	}
	if err := os.Chmod(abs, 0o200); err != nil {
		t.Fatal(err)
	}

	err = CommitStagedAt(dir, staged, "proxy.mp4")
	if err == nil {
		t.Error("published without fsyncing — a blob we cannot confirm reached disk " +
			"was made visible under its real name")
	}
	if _, statErr := os.Lstat(filepath.Join(mount, DerivBlobRel(inode, "proxy.mp4"))); statErr == nil {
		t.Error("the blob was published under its real name despite the failure")
	}

	// The happy path still commits.
	staged2, abs2, err := StageNameAt(dir, "poster.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs2, []byte("JPEG"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CommitStagedAt(dir, staged2, "poster.jpg"); err != nil {
		t.Fatalf("healthy commit refused: %v", err)
	}
}
