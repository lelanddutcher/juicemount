package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func TestFUSEReuseRequiresExactCompleteSnapshotWhenAvailable(t *testing.T) {
	const path = "/private/fixture/fuse"
	entry := mounttable.MountEntry{Path: path, Type: "macfuse", Source: "JuiceFS:fixture", FSID: [2]int32{11, 22}}
	valid := mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{entry}}
	if !fuseSnapshotMatchesPath(valid, path) {
		t.Fatal("exact complete JuiceFS snapshot refused")
	}
	for name, change := range map[string]func(*mounttable.Snapshot){
		"missing":        func(s *mounttable.Snapshot) { s.Entries = nil },
		"wrong boot":     func(s *mounttable.Snapshot) { s.BootID = "" },
		"wrong version":  func(s *mounttable.Snapshot) { s.Version = 2 },
		"case variant":   func(s *mounttable.Snapshot) { s.Entries[0].Path = "/private/fixture/Fuse" },
		"foreign type":   func(s *mounttable.Snapshot) { s.Entries[0].Type = "nfs" },
		"foreign source": func(s *mounttable.Snapshot) { s.Entries[0].Source = "Other:fixture" },
		"empty source":   func(s *mounttable.Snapshot) { s.Entries[0].Source = "JuiceFS:" },
		"zero fsid":      func(s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{} },
		"duplicate":      func(s *mounttable.Snapshot) { s.Entries = append(s.Entries, entry) },
		"descendant": func(s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: path + "/other"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := valid
			s.Entries = append([]mounttable.MountEntry(nil), valid.Entries...)
			change(&s)
			if fuseSnapshotMatchesPath(s, path) {
				t.Fatal("accepted ambiguous or foreign snapshot")
			}
		})
	}
}

func TestFUSEReusePreservesOnlyObservationErrorFallback(t *testing.T) {
	const path = "/private/fixture/fuse"
	stableCalls := 0
	stable := func(got string, d time.Duration) bool {
		if got != path || d != 2*time.Second {
			t.Fatalf("stable args: %q %v", got, d)
		}
		stableCalls++
		return true
	}
	unknown := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{}, errors.New("observer blocked")
	}
	if !fuseLooksHealthyWith(path, unknown, stable) || stableCalls != 1 {
		t.Fatal("observation error lost existing bounded data-plane fallback")
	}
	completeMissing := func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot"}, nil
	}
	if fuseLooksHealthyWith(path, completeMissing, stable) || stableCalls != 1 {
		t.Fatal("complete absence reached data-plane fallback")
	}
}
