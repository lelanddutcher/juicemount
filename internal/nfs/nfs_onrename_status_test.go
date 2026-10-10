package nfs

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

// TestRenameErrStatus pins the Rename errno -> NFS status mapping.
//
// The regression this guards (2026-08-03): EEXIST fell through to NFSStatusIO,
// which the macOS client surfaces as ioErr, so Finder aborted a folder move
// with error -36 and left the folder split across source and destination
// instead of resolving the name collision. See renameErrStatus.
func TestRenameErrStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want NFSStatus
	}{
		{
			// The exact shape os.Rename returns on a collision, which is what
			// reached this code path in the live repro.
			name: "LinkError wrapping EEXIST -> Exist, not IO",
			err:  &os.LinkError{Op: "rename", Old: "/a", New: "/b", Err: syscall.EEXIST},
			want: NFSStatusExist,
		},
		{
			name: "changed parent -> Jukebox retry",
			err:  syscall.EAGAIN,
			want: NFSStatusJukebox,
		},
		{
			name: "invalid export -> Stale",
			err:  syscall.ESTALE,
			want: NFSStatusStale,
		},
		{
			name: "destination requires directory -> NotDir",
			err:  syscall.ENOTDIR,
			want: NFSStatusNotDir,
		},
		{
			name: "destination requires file -> IsDir",
			err:  syscall.EISDIR,
			want: NFSStatusIsDir,
		},
		{
			name: "bare EEXIST -> Exist",
			err:  syscall.EEXIST,
			want: NFSStatusExist,
		},
		{
			name: "os.ErrExist -> Exist",
			err:  os.ErrExist,
			want: NFSStatusExist,
		},
		{
			name: "LinkError wrapping ENOTEMPTY -> NotEmpty",
			err:  &os.LinkError{Op: "rename", Old: "/a", New: "/b", Err: syscall.ENOTEMPTY},
			want: NFSStatusNotEmpty,
		},
		{
			name: "ENOENT still maps to NoEnt",
			err:  &os.LinkError{Op: "rename", Old: "/a", New: "/b", Err: syscall.ENOENT},
			want: NFSStatusNoEnt,
		},
		{
			name: "EACCES still maps to Access",
			err:  &os.LinkError{Op: "rename", Old: "/a", New: "/b", Err: syscall.EACCES},
			want: NFSStatusAccess,
		},
		{
			name: "a genuine I/O error is still IO",
			err:  &os.LinkError{Op: "rename", Old: "/a", New: "/b", Err: syscall.EIO},
			want: NFSStatusIO,
		},
		{
			name: "an unrecognized error stays IO",
			err:  errors.New("something else entirely"),
			want: NFSStatusIO,
		},
	}

	// The caller may use the scalar only when ToFileAttribute obtained an
	// inode from Stat_t; its generic pathname hash is never a proof.
	info, err := os.Stat(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if attr, proven := ToFileAttributeWithIdentity(info, "parent"); !proven || attr.Fileid == 0 {
		t.Fatalf("real Stat_t file ID not proved: attr=%+v proven=%v", attr, proven)
	}
	if attr, proven := ToFileAttributeWithIdentity(&mockFileInfo{name: "parent", mode: os.ModeDir, dir: true}, "parent"); proven || attr.Fileid == 0 {
		t.Fatalf("pathname-hash fallback gained identity authority: attr=%+v proven=%v", attr, proven)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := renameErrStatus(tc.err); got != tc.want {
				t.Errorf("renameErrStatus(%v) = %d, want %d", tc.err, got, tc.want)
			}
		})
	}
}
