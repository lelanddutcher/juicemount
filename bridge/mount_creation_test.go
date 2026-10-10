package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mountbroker"
	"github.com/lelanddutcher/juicemount/internal/mounttable"
)

type fakeNFSCustody struct {
	path   string
	record *mountbroker.Record
	start  func() error
	stop   func() error
	active bool
}

func (c *fakeNFSCustody) Path() string                { return c.path }
func (c *fakeNFSCustody) Record() *mountbroker.Record { return c.record }
func (c *fakeNFSCustody) Active() bool                { return c.active }
func (c *fakeNFSCustody) Start(context.Context) error {
	if c.start != nil {
		return c.start()
	}
	return nil
}
func (c *fakeNFSCustody) Stop(context.Context, time.Duration) error {
	if c.stop != nil {
		return c.stop()
	}
	return nil
}

func isolateNFSCustody(t *testing.T) {
	t.Helper()
	mountNowTestMu.Lock()
	oldFactory, oldCreator, oldObserve := newNFSController, newNFSMountCreator, observeOwnedNFS
	globalMu.Lock()
	oldController, oldPath, oldDB := globalNFSController, globalMountPath, globalDBPath
	globalNFSController, globalMountPath, globalDBPath = nil, "", "/fixture/meta.db"
	globalMu.Unlock()
	newNFSMountCreator = func() (mountbroker.Creator, error) { return mountbroker.Creator{}, nil }
	t.Cleanup(func() {
		newNFSController, newNFSMountCreator, observeOwnedNFS = oldFactory, oldCreator, oldObserve
		globalMu.Lock()
		globalNFSController, globalMountPath, globalDBPath = oldController, oldPath, oldDB
		globalMu.Unlock()
		mountNowTestMu.Unlock()
	})
}

func TestMountCreationRetainsPathBeforeCommandAndAfterUnknownResult(t *testing.T) {
	isolateNFSCustody(t)
	c := &fakeNFSCustody{path: "/fixture/volume"}
	c.start = func() error {
		globalMu.Lock()
		retained := globalNFSController == c && globalMountPath == c.path
		globalMu.Unlock()
		if !retained {
			t.Fatal("start before controller and path publication")
		}
		return errors.New("original operation pending")
	}
	calls := 0
	newNFSController = func(_ mountbroker.Creator, spec mountbroker.Spec, _ string, _ bool) (nfsMountController, error) {
		calls++
		if spec.MetadataPath != "/fixture/meta.db" || spec.ExportPath != mountNowTestExportPath {
			t.Fatal("creation identity lost")
		}
		return c, nil
	}
	if err := startOwnedNFSMount("127.0.0.1:12049", mountNowTestExportPath, c.path, false); err == nil {
		t.Fatal("pending start reported success")
	}
	if err := startOwnedNFSMount("127.0.0.1:12049", mountNowTestExportPath, c.path, false); err == nil || calls != 1 {
		t.Fatal("uncertain start launched a successor")
	}
	globalMu.Lock()
	retained := globalNFSController == c
	globalMu.Unlock()
	if !retained {
		t.Fatal("uncertain start discarded custody")
	}
}

func TestMountCreationPreflightFailureDoesNotEnterCommand(t *testing.T) {
	isolateNFSCustody(t)
	newNFSMountCreator = func() (mountbroker.Creator, error) { return mountbroker.Creator{}, errors.New("journal inaccessible") }
	newNFSController = func(mountbroker.Creator, mountbroker.Spec, string, bool) (nfsMountController, error) {
		t.Fatal("unrecorded mount")
		return nil, nil
	}
	if err := startOwnedNFSMount("127.0.0.1:12049", mountNowTestExportPath, "/fixture/volume", false); err == nil {
		t.Fatal("preflight error lost")
	}
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalNFSController != nil || globalMountPath != "" {
		t.Fatal("preflight acquired custody")
	}
}

func TestNFSCustodyStopRequiresSettlementAndFreshAbsence(t *testing.T) {
	for _, mode := range []string{"stop-pending", "observer-error", "nil-entries", "present", "descendant", "source-elsewhere", "fsid-elsewhere", "boot-changed", "absent"} {
		t.Run(mode, func(t *testing.T) {
			isolateNFSCustody(t)
			row := mounttable.MountEntry{Path: "/fixture/volume", Source: "127.0.0.1:" + mountNowTestExportPath, Type: "nfs", FSID: [2]int32{8, 9}}
			c := &fakeNFSCustody{path: row.Path, record: &mountbroker.Record{BootID: "boot", Spec: mountbroker.Spec{MountPath: row.Path, Source: row.Source}, Mount: &row}}
			globalMu.Lock()
			globalNFSController = c
			globalMu.Unlock()
			c.stop = func() error {
				if mode == "stop-pending" {
					return errors.New("pending")
				}
				return nil
			}
			observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) {
				if mode == "stop-pending" {
					t.Fatal("observed before settlement")
				}
				if mode == "observer-error" {
					return mounttable.Snapshot{}, errors.New("unknown")
				}
				snap := mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{}}
				e := row
				switch mode {
				case "nil-entries":
					snap.Entries = nil
				case "present":
					snap.Entries = []mounttable.MountEntry{e}
				case "descendant":
					e.Path += "/child"
					e.Source = "foreign"
					e.FSID = [2]int32{1, 2}
					snap.Entries = []mounttable.MountEntry{e}
				case "source-elsewhere":
					e.Path = "/other"
					e.FSID = [2]int32{1, 2}
					snap.Entries = []mounttable.MountEntry{e}
				case "fsid-elsewhere":
					e.Path = "/other"
					e.Source = "foreign"
					snap.Entries = []mounttable.MountEntry{e}
				case "boot-changed":
					snap.BootID = "other"
				}
				return snap, nil
			}
			got := stopOwnedNFSMount(c.path)
			if got != (mode == "absent") {
				t.Fatalf("settlement=%v", got)
			}
			globalMu.Lock()
			retained := globalNFSController == c
			globalMu.Unlock()
			if retained == got {
				t.Fatal("custody released without full settlement or retained after success")
			}
		})
	}
}

func TestNFSCustodyReadyRequiresLiveIdentity(t *testing.T) {
	isolateNFSCustody(t)
	row := mounttable.MountEntry{Path: "/fixture/volume", Source: "127.0.0.1:" + mountNowTestExportPath, Type: "nfs", FSID: [2]int32{8, 9}}
	c := &fakeNFSCustody{path: row.Path, active: true, record: &mountbroker.Record{BootID: "boot", Spec: mountbroker.Spec{MountPath: row.Path, Source: row.Source}, Mount: &row}}
	snap := mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{row}}
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) { return snap, nil }
	if currentOwnedNFSMount(row.Path) {
		t.Fatal("table row adopted without live controller")
	}
	globalMu.Lock()
	globalNFSController = c
	globalMu.Unlock()
	if !currentOwnedNFSMount(row.Path) {
		t.Fatal("exact live mount not recognized")
	}
	snap.Entries = append(snap.Entries, row)
	if currentOwnedNFSMount(row.Path) {
		t.Fatal("duplicate mount admitted")
	}
	snap.Entries = []mounttable.MountEntry{row}
	snap.Entries[0].FSID = [2]int32{3, 4}
	if currentOwnedNFSMount(row.Path) {
		t.Fatal("replacement admitted")
	}
}

func TestNFSCustodyStartStopStartKeepsSuccessorIndependent(t *testing.T) {
	isolateNFSCustody(t)
	const path = "/fixture/volume"
	created := 0
	newNFSController = func(_ mountbroker.Creator, s mountbroker.Spec, _ string, _ bool) (nfsMountController, error) {
		created++
		return &fakeNFSCustody{path: s.MountPath}, nil
	}
	observeOwnedNFS = func(context.Context) (mounttable.Snapshot, error) {
		return mounttable.Snapshot{Version: 1, BootID: "boot", Entries: []mounttable.MountEntry{}}, nil
	}
	if err := startOwnedNFSMount("127.0.0.1:12049", mountNowTestExportPath, path, false); err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	first := globalNFSController
	globalMu.Unlock()
	if !unmountAndClearNFS(path, unmountNFS) {
		t.Fatal("settled first generation could not retire")
	}
	if err := startOwnedNFSMount("127.0.0.1:12049", mountNowTestExportPath, path, false); err != nil {
		t.Fatal(err)
	}
	globalMu.Lock()
	second := globalNFSController
	globalMu.Unlock()
	if first == second || created != 2 {
		t.Fatal("successor reused original controller")
	}
}

func TestNFSCustodyWithoutRememberedPathStillRetainsServingStack(t *testing.T) {
	isolateNFSCustody(t)
	c := &fakeNFSCustody{path: "/fixture/volume"}
	globalMu.Lock()
	globalNFSController = c
	globalMountPath = ""
	globalMu.Unlock()
	globalLifecycleMu.Lock()
	settled := quiesceServerSerialized(time.Millisecond)
	globalLifecycleMu.Unlock()
	if settled {
		t.Fatal("controller without remembered path discarded serving stack")
	}
}

func TestNFSCustodyStopRefusesUnjoinedVerifier(t *testing.T) {
	for _, mode := range []string{"stop-mount", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			isolateNFSCustody(t)
			c := &fakeNFSCustody{path: "/fixture/volume", stop: func() error { t.Fatal("detach entered before verifier joined"); return nil }}
			globalMu.Lock()
			oldFlights, oldServer, oldMonitor := globalMountFlights, globalServer, globalMonitor
			globalNFSController, globalMountFlights, globalServer, globalMonitor = c, nil, nil, nil
			globalMu.Unlock()
			armed, generation := globalNFSWriteVerifyArmed.Load(), globalNFSWriteVerifyGeneration.Load()
			globalNFSWriteVerifyArmed.Store(true)
			globalNFSWriteVerifyGeneration.Store(99)
			defer func() {
				globalNFSWriteVerifyGeneration.Store(generation)
				globalNFSWriteVerifyArmed.Store(armed)
				globalMu.Lock()
				globalMountFlights, globalServer, globalMonitor = oldFlights, oldServer, oldMonitor
				globalMu.Unlock()
			}()
			if mode == "stop-mount" {
				if NFSServerStopMount() != 0 {
					t.Fatal("StopMount accepted unjoined verifier")
				}
			} else if shutdownServer(false) != 0 {
				t.Fatal("shutdown accepted unjoined verifier")
			}
		})
	}
}

func TestNFSMountProvisioningRequiresInteractiveCaller(t *testing.T) {
	for _, interactive := range []bool{false, true} {
		t.Run(map[bool]string{false: "unattended", true: "interactive"}[interactive], func(t *testing.T) {
			isolateNFSCustody(t)
			calls := 0
			newNFSController = func(_ mountbroker.Creator, s mountbroker.Spec, _ string, allow bool) (nfsMountController, error) {
				calls++
				if allow != interactive {
					t.Fatalf("provisioning permission=%v want%v", allow, interactive)
				}
				return &fakeNFSCustody{path: s.MountPath}, nil
			}
			mount := mountNFSNonInteractive
			if interactive {
				mount = mountNFSWithPrompt
			}
			if err := mount("127.0.0.1:12049", mountNowTestExportPath, "/fixture/volume"); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("controller not created exactly once")
			}
		})
	}
}
