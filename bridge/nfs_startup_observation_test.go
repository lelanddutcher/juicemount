package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func TestStartupNFSConflictUsesStructuredCompleteObservation(t *testing.T) {
	mountNowTestMu.Lock()
	previous := observeOwnedNFS
	t.Cleanup(func() {
		observeOwnedNFS = previous
		mountNowTestMu.Unlock()
	})
	const path = "/Volumes/JuiceMount"
	root := mounttable.MountEntry{Path: "/", Source: "/dev/disk1s1", Type: "apfs", FSID: [2]int32{1, 2}}
	occupied := mounttable.MountEntry{Path: path, Source: "127.0.0.1:/", Type: "nfs", FSID: [2]int32{3, 4}}
	complete := func(entries ...mounttable.MountEntry) mounttable.Snapshot {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: entries}
	}
	for _, tc := range []struct {
		name     string
		snapshot mounttable.Snapshot
		err      error
		want     nfsMountOwnership
	}{
		{name: "unrelated root only", snapshot: complete(root), want: nfsMountAbsent},
		{name: "configured NFS present", snapshot: complete(root, occupied), want: nfsMountForeignOrUnknown},
		{name: "configured non NFS present", snapshot: complete(root, mounttable.MountEntry{Path: path, Source: "disk", Type: "apfs"}), want: nfsMountForeignOrUnknown},
		{name: "descendant present", snapshot: complete(root, mounttable.MountEntry{Path: path + "/child", Source: "disk", Type: "apfs"}), want: nfsMountForeignOrUnknown},
		{name: "case variant present", snapshot: complete(root, mounttable.MountEntry{Path: "/volumes/juicemount", Source: "disk", Type: "apfs"}), want: nfsMountForeignOrUnknown},
		{name: "duplicate configured mount", snapshot: complete(root, occupied, occupied), want: nfsMountForeignOrUnknown},
		{name: "missing identity", snapshot: mounttable.Snapshot{Version: 1, Entries: []mounttable.MountEntry{root}}, want: nfsMountForeignOrUnknown},
		{name: "missing entries", snapshot: mounttable.Snapshot{Version: 1, BootID: "boot"}, want: nfsMountForeignOrUnknown},
		{name: "empty entries", snapshot: complete(), want: nfsMountForeignOrUnknown},
		{name: "invalid entry", snapshot: complete(root, mounttable.MountEntry{Path: "/other", Type: "nfs"}), want: nfsMountForeignOrUnknown},
		{name: "observer error", err: errors.New("helper deadline"), want: nfsMountForeignOrUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			observeOwnedNFS = func(ctx context.Context) (mounttable.Snapshot, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Fatal("structured observer lacks deadline")
				}
				return tc.snapshot, tc.err
			}
			if got := inspectConfiguredNFSMount(path); got != tc.want {
				t.Fatalf("inspectConfiguredNFSMount = %v, want %v", got, tc.want)
			}
			conflict := preMountConflictCheck("", path)
			if (conflict != "") != (tc.want == nfsMountForeignOrUnknown) {
				t.Fatalf("preMountConflictCheck = %q, ownership = %v", conflict, tc.want)
			}
			if conflict != "" && !strings.Contains(conflict, path) {
				t.Fatalf("conflict omitted configured path: %q", conflict)
			}
			if calls != 2 {
				t.Fatalf("structured observer calls = %d, want 2", calls)
			}
		})
	}
}
