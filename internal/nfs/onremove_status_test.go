package nfs

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

// TestRemoveErrStatus pins the REMOVE/RMDIR error→status mapping, especially
// ENOTEMPTY → NFS3ERR_NOTEMPTY (66): a normal user mistake ("folder isn't
// empty") previously fell through to NFS3ERR_IO and Finder showed an I/O
// error.
func TestRemoveErrStatus(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want NFSStatus
		ok   bool
	}{
		{"notexist", os.ErrNotExist, NFSStatusNoEnt, true},
		{"perm", os.ErrPermission, NFSStatusAccess, true},
		{"notempty", syscall.ENOTEMPTY, NFSStatusNotEmpty, true},
		// wrapped in a foreign error type to prove errors.Is unwrapping
		{"wrapped-notempty", &os.PathError{Op: "remove", Path: "/x", Err: syscall.ENOTEMPTY}, NFSStatusNotEmpty, true},
		{"io", errors.New("boom"), 0, false},
	}

	for _, tc := range cases {
		got, ok := removeErrStatus(tc.err)
		if ok != tc.ok {
			t.Errorf("%s: mapped=%v, want %v", tc.name, ok, tc.ok)
			continue
		}
		if !ok {
			continue
		}
		if got.NFSStatus != tc.want {
			t.Errorf("%s: status=%v, want %v", tc.name, got.NFSStatus, tc.want)
		}
	}
}

// TestMkdirDefaultModeIsOctal pins the decimal-755 bug: the fallback mode for
// MKDIR when the client omits mode must be 0o755, not decimal 755 (0o1363),
// which produced owner-no-read directories.
func TestMkdirDefaultModeIsOctal(t *testing.T) {
	mode := os.FileMode(mkdirDefaultMode)
	if mkdirDefaultMode != 0o755 {
		t.Fatalf("mkdirDefaultMode = %#o, want 0o755", mkdirDefaultMode)
	}
	if mode.Perm() != 0o755 {
		t.Fatalf("perm bits = %#o, want 0755", mode.Perm())
	}
}
