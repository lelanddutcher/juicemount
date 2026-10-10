package health

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

func healthSnapshot(entries ...mounttable.MountEntry) mounttable.Snapshot {
	if entries == nil {
		entries = []mounttable.MountEntry{}
	}
	return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: entries}
}

func TestExactHealthMountRequiresCompleteUnambiguousTarget(t *testing.T) {
	const path = "/Volumes/München Photos"
	good := mounttable.MountEntry{Path: path, Source: "127.0.0.1:/jm-1", Type: "nfs", FSID: [2]int32{4, 5}}
	sibling := mounttable.MountEntry{Path: path + "2", Source: "other", Type: "apfs", FSID: [2]int32{6, 7}}
	if present, err := exactHealthMount(healthSnapshot(sibling, good), path, "nfs"); err != nil || !present {
		t.Fatalf("exact Unicode/space NFS path: present=%v err=%v", present, err)
	}
	if present, err := exactHealthMount(healthSnapshot(sibling), path, "nfs"); err != nil || present {
		t.Fatalf("prefix sibling counted as mount: present=%v err=%v", present, err)
	}
	if present, err := exactHealthMount(healthSnapshot(), path, "nfs"); err != nil || present {
		t.Fatalf("complete empty snapshot: present=%v err=%v", present, err)
	}
	for name, change := range map[string]func(*mounttable.Snapshot){
		"missing entries": func(s *mounttable.Snapshot) { s.Entries = nil },
		"missing boot":    func(s *mounttable.Snapshot) { s.BootID = "" },
		"wrong version":   func(s *mounttable.Snapshot) { s.Version = 2 },
		"case alias":      func(s *mounttable.Snapshot) { s.Entries[0].Path = "/Volumes/MÜNCHEN Photos" },
		"duplicate":       func(s *mounttable.Snapshot) { s.Entries = append(s.Entries, good) },
		"foreign type":    func(s *mounttable.Snapshot) { s.Entries[0].Type = "apfs" },
		"zero FSID":       func(s *mounttable.Snapshot) { s.Entries[0].FSID = [2]int32{} },
		"empty source":    func(s *mounttable.Snapshot) { s.Entries[0].Source = "" },
		"relative entry": func(s *mounttable.Snapshot) {
			s.Entries = append(s.Entries, mounttable.MountEntry{Path: "relative", Source: "x", Type: "apfs"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := healthSnapshot(good)
			s.Entries = append([]mounttable.MountEntry(nil), s.Entries...)
			change(&s)
			if present, err := exactHealthMount(s, path, "nfs"); err == nil || present {
				t.Fatalf("ambiguous/incomplete mount accepted: present=%v err=%v", present, err)
			}
		})
	}
	fuse := mounttable.MountEntry{Path: path, Source: "JuiceFS:fixture", Type: "macfuse", FSID: [2]int32{8, 9}}
	if present, err := exactHealthMount(healthSnapshot(fuse), path, "macfuse"); err != nil || !present {
		t.Fatalf("exact macFUSE path: present=%v err=%v", present, err)
	}
}

func TestDarwinHealthRoutesThroughBundledSnapshotAndPreservesFUSEProcessGuard(t *testing.T) {
	const path = "/private/owned/fuse"
	oldAlive := isJuiceFSProcessAliveFn
	defer func() { isJuiceFSProcessAliveFn = oldAlive }()
	var snapshots, processes int
	deps := healthMountDeps{platform: "darwin",
		executable: func() (string, error) { return "/bundle/juicemount-service", nil },
		output:     func(context.Context) ([]byte, error) { t.Fatal("Darwin launched legacy mount(8)"); return nil, nil },
		snapshot: func(ctx context.Context, helper string) (mounttable.Snapshot, error) {
			snapshots++
			if helper != "/bundle/juicemount-mount-probe" {
				t.Fatalf("helper is not service sibling: %q", helper)
			}
			if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) <= 0 {
				t.Fatal("native snapshot lost its bounded context")
			}
			return healthSnapshot(mounttable.MountEntry{Path: path, Source: "JuiceFS:fixture",
				Type: "macfuse", FSID: [2]int32{1, 2}}), nil
		},
	}
	isJuiceFSProcessAliveFn = func(got string) bool { processes++; return got == path }
	m := New(Config{FUSEPath: path})
	if st := m.checkFUSEWith(deps); !st.Healthy || snapshots != 1 || processes != 1 {
		t.Fatalf("Darwin unowned FUSE did not use native snapshot + process guard: %+v snapshots=%d processes=%d", st, snapshots, processes)
	}
	isJuiceFSProcessAliveFn = func(string) bool { processes++; return false }
	if st := m.checkFUSEWith(deps); st.Healthy || processes != 2 {
		t.Fatalf("missing JuiceFS process reported healthy: %+v processes=%d", st, processes)
	}
	deps.snapshot = func(context.Context, string) (mounttable.Snapshot, error) {
		return healthSnapshot(mounttable.MountEntry{Path: path, Source: "foreign", Type: "nfs", FSID: [2]int32{1, 2}}), nil
	}
	if st := m.checkFUSEWith(deps); st.Healthy || processes != 2 {
		t.Fatalf("foreign FUSE type reached process liveness or healthy: %+v processes=%d", st, processes)
	}
	m.SetOwnedFUSEProbe(func() FUSESteadyEvidence {
		return FUSESteadyEvidence{Ready: true, ChildAlive: true, ChildProven: true}
	})
	deps.snapshot = func(context.Context, string) (mounttable.Snapshot, error) {
		t.Fatal("owned FUSE fast path queried native observer")
		return mounttable.Snapshot{}, nil
	}
	if st := m.checkFUSEWith(deps); !st.Healthy || processes != 2 {
		t.Fatalf("owned FUSE fast path changed: %+v processes=%d", st, processes)
	}
}

func TestDarwinNFSUnknownObserverCannotTriggerRecovery(t *testing.T) {
	var queries, remounts atomic.Int32
	deps := healthMountDeps{platform: "darwin",
		executable: func() (string, error) { return "/bundle/juicemount-service", nil },
		output:     func(context.Context) ([]byte, error) { t.Fatal("NFS used legacy mount(8)"); return nil, nil },
		snapshot: func(context.Context, string) (mounttable.Snapshot, error) {
			queries.Add(1)
			return mounttable.Snapshot{}, errors.New("original native observer held")
		},
	}
	const path = "/private/owned/nfs"
	m := New(Config{NFSMountPoint: path})
	m.EnableNFSRemount(func() error { remounts.Add(1); return nil })
	retirer := &countingRetirer{}
	m.SetStaleMountRetirer(retirer)
	status, done := m.checkNFSWithProbe(nfsProbeDeps{
		stat: func(string) (os.FileInfo, error) { return nil, nil },
		mountTable: func(path string, timeout time.Duration) (bool, error) {
			return healthMountPresentWith(path, "nfs", timeout, deps)
		},
	}, time.Second)
	<-done
	if status.Healthy || !status.nfsObserverUnavailable || !strings.Contains(status.Message, "native observer held") || queries.Load() != 1 {
		t.Fatalf("unknown NFS observation became absence: %+v queries=%d", status, queries.Load())
	}
	for i := 0; i < NFSStaleHardRemountThreshold+2; i++ {
		m.routeNFSRecovery(status, true)
	}
	if remounts.Load() != 0 || retirer.calls.Load() != 0 {
		t.Fatalf("unknown NFS observation mutated recovery: remount=%d retire=%d", remounts.Load(), retirer.calls.Load())
	}
}

func TestOtherPlatformPreservesLegacyTextPredicates(t *testing.T) {
	const path = "/Volumes/Family Photos"
	var queries int
	deps := healthMountDeps{platform: "linux",
		output: func(context.Context) ([]byte, error) {
			queries++
			return []byte("disk on " + path + "2 (nfs)\nserver on " + path + " (nfs)\n"), nil
		},
		executable: func() (string, error) { t.Fatal("non-Darwin resolved helper"); return "", nil },
		snapshot: func(context.Context, string) (mounttable.Snapshot, error) {
			t.Fatal("non-Darwin invoked native snapshot")
			return mounttable.Snapshot{}, nil
		},
	}
	if present, err := healthMountPresentWith(path, "nfs", time.Second, deps); err != nil || !present {
		t.Fatalf("legacy NFS delimiter changed: present=%v err=%v", present, err)
	}
	if present, err := healthMountPresentWith(path+"3", "nfs", time.Second, deps); err != nil || present {
		t.Fatalf("legacy NFS prefix sibling counted: present=%v err=%v", present, err)
	}
	if present, err := healthMountPresentWith(path, "macfuse", time.Second, deps); err != nil || !present || queries != 3 {
		t.Fatalf("legacy FUSE text predicate changed: present=%v err=%v queries=%d", present, err, queries)
	}
	deps.output = func(context.Context) ([]byte, error) { return nil, errors.New("old observer failed") }
	if present, err := healthMountPresentWith(path, "nfs", time.Second, deps); err == nil || present {
		t.Fatalf("legacy observer error became absence: present=%v err=%v", present, err)
	}
}
