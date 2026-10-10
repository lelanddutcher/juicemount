package fuse

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/meta"
	"golang.org/x/sys/unix"
)

func TestDarwinExclusiveRenameAdvertised(t *testing.T) {
	if renameCapabilities()&fuse.CAP_RENAME_EXCL == 0 {
		t.Fatal("exclusive rename would be refused before reaching the filesystem")
	}
}

func TestDarwinRenameFlagsCrossMetadataBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		raw    uint32
		want   uint32
		status syscall.Errno
	}{
		{"ordinary", 0, 0, 0},
		{"exchange", unix.RENAME_SWAP, meta.RenameExchange, 0},
		{"exclusive", unix.RENAME_EXCL, meta.RenameNoReplace, 0},
		{"linux-exclusive-value-is-not-darwin-exclusive", meta.RenameNoReplace, 0, syscall.EINVAL},
		{"combined-flags", unix.RENAME_SWAP | unix.RENAME_EXCL, 0, syscall.EINVAL},
		{"unknown-flag", 0x8, 0, syscall.EINVAL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, status := normalizeRenameFlags(tc.raw)
			if got != tc.want || status != tc.status {
				t.Fatalf("raw %#x: metadata flags %#x, status %v; want %#x, %v", tc.raw, got, status, tc.want, tc.status)
			}
		})
	}
}

func TestDarwinUnsupportedRenameStopsBeforeVFS(t *testing.T) {
	// A nil VFS makes a mistaken dispatch panic. These raw flags must be
	// refused at the FUSE boundary before any namespace operation is made.
	fs := &fileSystem{}
	for _, flags := range []uint32{meta.RenameNoReplace, unix.RENAME_SWAP | unix.RENAME_EXCL, 0x8} {
		status := fs.Rename(nil, &fuse.RenameIn{Flags: flags}, "stage", "existing")
		if status != fuse.EINVAL {
			t.Fatalf("raw %#x: status %v, want EINVAL", flags, status)
		}
	}
}
