//go:build darwin

package mounttable

import (
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDarwinSnapshotOutputPreservesMountSyntax(t *testing.T) {
	var stat unix.Statfs_t
	copy(stat.Mntfromname[:], "JuiceFS:zpool")
	copy(stat.Mntonname[:], "/Users/test/.juicemount/fuse-internal")
	copy(stat.Fstypename[:], "macfuse")

	got := string(darwinSnapshotOutput([]unix.Statfs_t{stat}))
	want := "JuiceFS:zpool on /Users/test/.juicemount/fuse-internal (macfuse)\n"
	if got != want {
		t.Fatalf("snapshot output = %q, want %q", got, want)
	}
	if !strings.Contains(got, " /Users/test/.juicemount/fuse-internal ") {
		t.Fatal("snapshot output broke exact mountpoint ownership matching")
	}
}
