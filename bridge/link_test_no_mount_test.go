package main

import (
	"testing"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func TestLinkTestMountPathsClear(t *testing.T) {
	cfg := ServerConfig{FUSEPath: "/fixture/fuse", MountPoint: "/Volumes/fixture"}
	root := mounttable.MountEntry{Path: "/", Source: "/dev/disk-fixture", Type: "apfs"}
	mount := func(path string) mounttable.MountEntry {
		return mounttable.MountEntry{Path: path, Source: "fixture-source", Type: "nfs"}
	}
	cases := []struct {
		name      string
		cfg       ServerConfig
		entries   []mounttable.MountEntry
		wantClear bool
	}{
		{"empty target paths", ServerConfig{}, []mounttable.MountEntry{root}, false},
		{"clean initial setup", cfg, []mounttable.MountEntry{root}, true},
		{"unrelated mount", cfg, []mounttable.MountEntry{root, mount("/Volumes/other")}, true},
		{"similar prefix", cfg, []mounttable.MountEntry{root, mount("/fixture/fuse-other")}, true},
		{"FUSE occupant", cfg, []mounttable.MountEntry{root, mount("/fixture/fuse")}, false},
		{"FUSE descendant", cfg, []mounttable.MountEntry{root, mount("/fixture/fuse/child")}, false},
		{"NFS occupant", cfg, []mounttable.MountEntry{root, mount("/Volumes/fixture")}, false},
		{"NFS descendant", cfg, []mounttable.MountEntry{root, mount("/Volumes/fixture/child")}, false},
		{"case-folded occupant", cfg, []mounttable.MountEntry{root, mount("/VOLUMES/FIXTURE")}, false},
		{"incomplete entry", cfg, []mounttable.MountEntry{{Path: "/fixture/fuse"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := mounttable.Snapshot{Version: 1, BootID: "fixture-boot", Entries: tc.entries}
			got := linkTestMountPathsClear(tc.cfg, snapshot) == nil
			if got != tc.wantClear {
				t.Fatalf("clear=%t, want %t", got, tc.wantClear)
			}
		})
	}
	if err := linkTestMountPathsClear(cfg, mounttable.Snapshot{}); err == nil {
		t.Fatal("incomplete snapshot treated as absence")
	}
}
