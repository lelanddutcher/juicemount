package unmountd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// §6 matrix: a replacement mount at the same path has a different fsid. The
// helper must see our instance as gone and issue zero syscalls, even for
// "Eject now".
func TestReplacementAtSamePathIsNeverTouched(t *testing.T) {
	for _, req := range []RetireRequest{{Liveness: LivenessAlive}, {Liveness: LivenessDead}, {EjectNow: true}} {
		k := newFake(rootEntry(), ourEntry())
		svc, st := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		k.setEntries(rootEntry(), Entry{FSID: otherFSID, Owner: testUID, Type: "nfs", MntOnName: testPath, MntFromName: "127.0.0.1:/jm-" + otherToken})
		out, err := retire(t, svc, "op1", req, 2*time.Second)
		if err != nil || out.State != StateRetired {
			t.Fatalf("req %+v: outcome %+v err %v, want RETIRED", req, out, err)
		}
		if got := k.recorded(); len(got) != 0 {
			t.Fatalf("req %+v: replacement at the same path got %d unmount syscalls: %+v", req, len(got), got)
		}
		if _, err := st.Get(testUID, "op1"); !errors.Is(err, ErrNoReceipt) {
			t.Fatalf("retired receipt not deleted: %v", err)
		}
	}
}

// A replacement stacked at the same path while ours is still present: the
// syscall targets our fsid only.
func TestStackedReplacementOnlyOurFSIDIsTargeted(t *testing.T) {
	other := Entry{FSID: otherFSID, Owner: testUID, Type: "nfs", MntOnName: testPath, MntFromName: "127.0.0.1:/jm-" + otherToken}
	k := newFake(rootEntry(), ourEntry(), other)
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("outcome %+v err %v", out, err)
	}
	for _, c := range k.recorded() {
		if c.FSID != ourFSID {
			t.Fatalf("syscall hit fsid %s, not ours", c.FSID)
		}
	}
	if len(out.Anomaly) != 0 {
		t.Fatalf("unexpected anomaly %+v", out.Anomaly)
	}
}

func TestForeignOwnerIsRefused(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, st := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	foreign := ourEntry()
	foreign.Owner = 0 // e.g. root remounted the same strings with our fsid
	k.setEntries(rootEntry(), foreign)
	for _, req := range []RetireRequest{{Liveness: LivenessAlive}, {Liveness: LivenessDead, EjectNow: true}} {
		out, err := retire(t, svc, "op1", req, 2*time.Second)
		if err != nil || out.State != StateRefusedIdentity {
			t.Fatalf("outcome %+v err %v, want REFUSED_IDENTITY", out, err)
		}
		if n := len(k.recorded()); n != 0 {
			t.Fatalf("foreign-owner mount received %d unmount syscalls", n)
		}
	}
	if _, err := st.Get(testUID, "op1"); err != nil {
		t.Fatalf("refusal must keep the receipt: %v", err)
	}
	// Another uid's caller can never even reach our receipt.
	other := Caller{UID: 502, Role: RoleService}
	if _, err := svc.Retire(context.Background(), other, "op1", RetireRequest{Liveness: LivenessDead}); !errors.Is(err, ErrNoReceipt) {
		t.Fatalf("uid 502 reached uid 501's receipt: %v", err)
	}
}

func TestTokenMismatch(t *testing.T) {
	t.Run("register refuses a token the kernel strings do not carry", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		svc, _ := newTestService(t, k, nil)
		req := nfsRegister("op1")
		req.Token = strings.Repeat("f", 32)
		if _, err := svc.Register(service(), req); !errors.Is(err, ErrInvalidReceipt) {
			t.Fatalf("register with a foreign token: err %v, want ErrInvalidReceipt", err)
		}
	})
	t.Run("retire refuses a live entry carrying a different token", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		svc, _ := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		e := ourEntry()
		e.MntFromName = "127.0.0.1:/jm-" + strings.Repeat("f", 32)
		k.setEntries(rootEntry(), e)
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 2*time.Second)
		if err != nil || out.State != StateRefusedIdentity {
			t.Fatalf("outcome %+v err %v, want REFUSED_IDENTITY", out, err)
		}
		if n := len(k.recorded()); n != 0 {
			t.Fatalf("token mismatch received %d unmount syscalls", n)
		}
	})
}

// macFUSE `-o fsid=` lets a same-user process claim our fsid after our mount
// is gone (or alongside it). The fsid alone must never authorize anything.
func TestFSIDReusedBySameUserIsRefused(t *testing.T) {
	impostor := Entry{FSID: ourFSID, Owner: testUID, Type: "macfuse", MntOnName: "/Users/x/evil", MntFromName: "evilfs"}
	t.Run("our mount gone, impostor holds the fsid", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		svc, _ := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		k.setEntries(rootEntry(), impostor)
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead, EjectNow: true}, 2*time.Second)
		if err != nil || out.State != StateRefusedIdentity {
			t.Fatalf("outcome %+v err %v, want REFUSED_IDENTITY", out, err)
		}
		if n := len(k.recorded()); n != 0 {
			t.Fatalf("fsid impostor received %d unmount syscalls", n)
		}
	})
	t.Run("impostor shares the fsid with our live mount", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry())
		svc, _ := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		// Ours is listed first, so a first-match predicate would act on it while
		// the kernel's fsid lookup may resolve to the impostor.
		k.setEntries(rootEntry(), ourEntry(), impostor)
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 2*time.Second)
		if err != nil || out.State != StateRefusedIdentity || !strings.Contains(out.Reason, "ambiguous") {
			t.Fatalf("outcome %+v err %v, want ambiguous REFUSED_IDENTITY", out, err)
		}
		if n := len(k.recorded()); n != 0 {
			t.Fatalf("ambiguous fsid received %d unmount syscalls", n)
		}
	})
}

// A proven-dead or wedged server goes straight to MNT_FORCE: an ordinary
// attempt would set MNT_LUNMOUNT and park in VFS_SYNC, after which even
// MNT_FORCE returns EBUSY. The fake models exactly that trap.
func TestDeadOrWedgedEngineGoesStraightToForce(t *testing.T) {
	for _, l := range []Liveness{LivenessDead, LivenessWedged} {
		k := newFake(rootEntry(), ourEntry())
		k.dead[ourFSID] = true
		svc, _ := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: l}, 2*time.Second)
		if err != nil || out.State != StateRetired {
			t.Fatalf("liveness %d: outcome %+v err %v, want RETIRED (calls %q)", l, out, err, flagsOf(k.recorded()))
		}
		if got := flagsOf(k.recorded()); got != "F" {
			t.Fatalf("liveness %d: syscall flags %q, want exactly one MNT_FORCE and no ordinary attempt", l, got)
		}
	}
}

// A healthy busy volume is retried ordinarily, forever, and never forced.
func TestHealthyBusyVolumeIsNeverForced(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	k.busy[ourFSID] = true
	hs := fakeHolders{hs: []Holder{
		{PID: 900, UID: testUID, Name: "Adobe Premiere Pro", Files: 2},
		{PID: 1, UID: 0, Name: "launchd", Files: 1},
		{PID: 901, UID: 502, Name: "other-user-app", Files: 1},
	}}
	svc, _ := newTestService(t, k, hs)
	mustRegister(t, svc, nfsRegister("op1"))
	for _, l := range []Liveness{LivenessAlive, LivenessUnknown} {
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: l}, 300*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("busy retire returned %v / %+v; it must keep waiting", err, out)
		}
		calls := flagsOf(k.recorded())
		if strings.Contains(calls, "F") || len(calls) < 3 {
			t.Fatalf("healthy busy volume: syscall flags %q, want several MNT_NOBLOCK and no MNT_FORCE", calls)
		}
		if len(out.Holders) != 1 || out.Holders[0].UID != testUID {
			t.Fatalf("holders %+v, want only the caller's uid", out.Holders)
		}
	}
	// "Eject now" escalates the running job to a force.
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive, EjectNow: true}, 2*time.Second)
	if err != nil || out.State != StateRetired || !out.Forced {
		t.Fatalf("eject now: %+v err %v", out, err)
	}
}

// A mount nested inside ours blocks MNT_FORCE (whose recursion would remove
// it). A dead server issues nothing at all while waiting.
func TestNestedForeignMountWaits(t *testing.T) {
	child := Entry{FSID: FSID{0x2e000040, 0x2e}, Owner: 0, Type: "smbfs", MntOnName: testPath + "/Shared/child", MntFromName: "//guest@nas/share"}
	t.Run("dead engine: wait, then force once the child is gone", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry(), child)
		k.dead[ourFSID] = true
		// NestedRetry is an hour, so the job stays parked until a join wakes
		// it; sampling the VERIFY⇄WAIT_NESTED cycle at an instant was flaky.
		svc := parkedNestedService(t, k)
		mustRegister(t, svc, nfsRegister("op1"))
		waitParkedNested(t, svc, k, RetireRequest{Liveness: LivenessDead, EjectNow: true})
		k.setEntries(rootEntry(), ourEntry())
		out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 2*time.Second)
		if err != nil || out.State != StateRetired || flagsOf(k.recorded()) != "F" {
			t.Fatalf("after child left: %+v err %v calls %q", out, err, flagsOf(k.recorded()))
		}
	})
	t.Run("alive engine: ordinary retries only, even with eject now", func(t *testing.T) {
		k := newFake(rootEntry(), ourEntry(), child)
		k.busy[ourFSID] = true // the covered vnode keeps an ordinary unmount busy
		svc := parkedNestedService(t, k)
		mustRegister(t, svc, nfsRegister("op1"))
		_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, time.Millisecond)
		parked := func() bool {
			o, err := svc.Status(service(), "op1")
			return err == nil && o.State == StateWaitNested
		}
		if !waitFor(t, 5*time.Second, parked) {
			t.Fatal("ordinary EBUSY under a nested mount never parked in WAIT_NESTED")
		}
		if calls := flagsOf(k.recorded()); calls != "N" {
			t.Fatalf("calls %q, want one ordinary attempt before parking", calls)
		}
		k.mu.Lock()
		snapsBefore := k.snaps
		k.mu.Unlock()
		// The EjectNow join wakes the job; wait until that pass has taken
		// its snapshot and parked again.
		_, _ = retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive, EjectNow: true}, time.Millisecond)
		if !waitFor(t, 5*time.Second, func() bool {
			k.mu.Lock()
			n := k.snaps
			k.mu.Unlock()
			return n > snapsBefore && parked()
		}) {
			t.Fatal("the EjectNow pass never re-parked in WAIT_NESTED")
		}
		out, _ := svc.Status(service(), "op1")
		if calls := flagsOf(k.recorded()); strings.Contains(calls, "F") {
			t.Fatalf("forced while a foreign mount was nested: %q", calls)
		}
		if out.State != StateWaitNested || len(out.Descendants) != 1 {
			t.Fatalf("outcome %+v, want WAIT_NESTED with the child listed", out)
		}
	})
	t.Run("force needs two consecutive descendant-free snapshots", func(t *testing.T) {
		k := newFake()
		k.snapFn = func(n int) Snapshot {
			if n%2 == 0 { // the child flickers in and out on alternate snapshots
				return Snapshot{Boot: testBoot, Entries: []Entry{rootEntry(), ourEntry(), child}}
			}
			return Snapshot{Boot: testBoot, Entries: []Entry{rootEntry(), ourEntry()}}
		}
		svc, _ := newTestService(t, k, nil)
		mustRegister(t, svc, nfsRegister("op1"))
		_, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 200*time.Millisecond)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err %v", err)
		}
		if n := len(k.recorded()); n != 0 {
			t.Fatalf("forced (%q) without two consecutive descendant-free snapshots", flagsOf(k.recorded()))
		}
	})
}

// While an unmount call is IN_KERNEL, nothing more is issued for the fsid,
// even after the owner proves the server dead. When the call returns EBUSY,
// the next call follows the current request (now: force).
func TestInKernelIssuesNothingMore(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	release := make(chan struct{})
	k.block = release
	first := true
	k.blockFn = func(flags int) error {
		if first {
			first = false
			return errBusy
		}
		return nil
	}
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 400*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || out.State != StateInKernel || !out.StuckObserved {
		t.Fatalf("outcome %+v err %v, want IN_KERNEL", out, err)
	}
	// Escalation arrives while the call is still in the kernel.
	_, err = retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead, EjectNow: true}, 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v", err)
	}
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("calls while IN_KERNEL = %q, want exactly the one ordinary call", got)
	}
	k.mu.Lock()
	k.block = nil
	k.mu.Unlock()
	close(release)
	out, err = retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("after release: %+v err %v", out, err)
	}
	if got := flagsOf(k.recorded()); got != "NF" {
		t.Fatalf("calls = %q, want ordinary then force", got)
	}
}

func TestOtherBootReceiptIsVoid(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, st := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	k.mu.Lock()
	k.snap.Boot = "99999999-2222-3333-4444-555555555555"
	k.mu.Unlock()
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessDead}, 2*time.Second)
	if err != nil || out.State != StateVoid || len(k.recorded()) != 0 {
		t.Fatalf("outcome %+v err %v calls %d", out, err, len(k.recorded()))
	}
	if _, err := st.Get(testUID, "op1"); !errors.Is(err, ErrNoReceipt) {
		t.Fatalf("void receipt not deleted: %v", err)
	}
}

func TestPostStateAnomalyIsReported(t *testing.T) {
	bystander := Entry{FSID: FSID{0x30000050, 0x30}, Owner: testUID, Type: "smbfs", MntOnName: "/Volumes/other", MntFromName: "//nas/other"}
	k := newFake(rootEntry(), ourEntry(), bystander)
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	k.blockFn = func(int) error { k.remove(bystander.FSID); return nil }
	k.block = make(chan struct{})
	close(k.block)
	out, err := retire(t, svc, "op1", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateRetired || len(out.Anomaly) != 1 || out.Anomaly[0].FSID != bystander.FSID {
		t.Fatalf("outcome %+v err %v, want RETIRED with the bystander as anomaly", out, err)
	}
}

func TestRolesAreEnforced(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	gui := Caller{UID: testUID, Role: RoleGUI}
	if _, err := svc.Register(gui, nfsRegister("op1")); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("GUI register: %v", err)
	}
	mustRegister(t, svc, nfsRegister("op1"))
	if _, err := svc.Retire(context.Background(), gui, "op1", RetireRequest{EjectNow: true}); !errors.Is(err, ErrNotAuthorized) {
		t.Fatalf("GUI retire: %v", err)
	}
	if st, err := svc.Status(gui, "op1"); err != nil || st.State != StateRegistered {
		t.Fatalf("GUI status: %+v %v", st, err)
	}
	if n := len(k.recorded()); n != 0 {
		t.Fatalf("%d syscalls", n)
	}
}

func TestOrphanReclaimWithinWindowDoesNothing(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("op1"))
	if err := svc.Disconnected(testUID, "op1", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	if ok, err := svc.Reclaim(service(), "op1"); !ok || err != nil {
		t.Fatalf("reclaim: %v %v", ok, err)
	}
	time.Sleep(3 * fastPolicy().OrphanReclaim)
	if n := len(k.recorded()); n != 0 {
		t.Fatalf("reclaimed op got %d syscalls", n)
	}
	// Unreclaimed: the helper retires on its own, ordinary only.
	if err := svc.Disconnected(testUID, "op1", LivenessUnknown); err != nil {
		t.Fatal(err)
	}
	if !waitFor(t, 2*time.Second, func() bool { st, _ := svc.Status(service(), "op1"); return st.State == StateRetired }) {
		t.Fatalf("orphan never retired")
	}
	if got := flagsOf(k.recorded()); got != "N" {
		t.Fatalf("orphan retire flags %q, want one ordinary", got)
	}
}

func TestLaunchSweep(t *testing.T) {
	k := newFake(rootEntry(), ourEntry())
	svc, st := newTestService(t, k, nil)
	mustRegister(t, svc, nfsRegister("live"))
	gone := nfsRegister("gone")
	gone.FSID = otherFSID
	gone.MntOnName = "/Volumes/zpool2"
	gone.MntFromName = "127.0.0.1:/jm-" + otherToken
	gone.Token = otherToken
	k.setEntries(rootEntry(), ourEntry(), Entry{FSID: otherFSID, Owner: testUID, Type: "nfs", MntOnName: gone.MntOnName, MntFromName: gone.MntFromName})
	mustRegister(t, svc, gone)
	k.setEntries(rootEntry(), ourEntry())
	rep, err := svc.SweepAtLaunch()
	if err != nil || len(rep.Pending) != 1 || rep.Pending[0].OpID != "live" || len(rep.Retired) != 1 {
		t.Fatalf("sweep %v err %v", rep, err)
	}
	if n := len(k.recorded()); n != 0 {
		t.Fatalf("launch sweep issued %d syscalls", n)
	}
	k.mu.Lock()
	k.snap.Boot = "other-boot"
	k.mu.Unlock()
	rep, _ = svc.SweepAtLaunch()
	if len(rep.Voided) != 1 {
		t.Fatalf("sweep after reboot %v", rep)
	}
	if rs, _ := st.List(testUID); len(rs) != 0 {
		t.Fatalf("receipts left: %+v", rs)
	}
}

func TestMacFUSEReceiptTokenPlacement(t *testing.T) {
	nonce := "00112233445566778899aabbccddeeff"
	on := "/Users/u/Library/Application Support/JuiceMount/zpool.generations/" + nonce + "/000000000000002a/mount"
	e := Entry{FSID: FSID{0x31000060, 0x31}, Owner: testUID, Type: "macfuse", MntOnName: on, MntFromName: "JuiceFS:zpool"}
	k := newFake(rootEntry(), e)
	svc, _ := newTestService(t, k, nil)
	req := RegisterRequest{OpID: "fuse-2a", Type: TypeMacFUSE, FSID: e.FSID, Token: nonce, MntOnName: on, MntFromName: e.MntFromName, Generation: 0x2a}
	mustRegister(t, svc, req)
	bad := req
	bad.OpID, bad.Generation = "fuse-2b", 0x2b
	if _, err := svc.Register(service(), bad); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("generation not carried by mntonname must be refused: %v", err)
	}
}

// H3 L0 M1: the macFUSE source is "JuiceFS:<volume name>" for every
// generation and installation. A foreign mount of a same-named volume must
// not block retiring ours (rule 6 is NFS-only); ours is still identified by
// the nonce path, fsid and owner.
func TestMacFUSESharedSourceDoesNotBlockRetire(t *testing.T) {
	nonce := "00112233445566778899aabbccddeeff"
	on := "/Users/u/Library/Application Support/JuiceMount/zpool.generations/" + nonce + "/000000000000002a/mount"
	ours := Entry{FSID: FSID{0x31000060, 0x31}, Owner: testUID, Type: "macfuse", MntOnName: on, MntFromName: "JuiceFS:zpool"}
	foreign := Entry{FSID: FSID{0x31000061, 0x31}, Owner: testUID + 1, Type: "macfuse",
		MntOnName:   "/Users/other/Library/Application Support/JuiceMount/zpool.generations/ffeeddccbbaa99887766554433221100/0000000000000001/mount",
		MntFromName: "JuiceFS:zpool"}
	k := newFake(rootEntry(), ours, foreign)
	svc, _ := newTestService(t, k, nil)
	mustRegister(t, svc, RegisterRequest{OpID: "fuse-2a", Type: TypeMacFUSE, FSID: ours.FSID, Token: nonce, MntOnName: on, MntFromName: ours.MntFromName, Generation: 0x2a})
	out, err := retire(t, svc, "fuse-2a", RetireRequest{Liveness: LivenessAlive}, 2*time.Second)
	if err != nil || out.State != StateRetired {
		t.Fatalf("shared macFUSE source blocked retirement: %+v %v", out, err)
	}
	for _, c := range k.recorded() {
		if c.FSID != ours.FSID {
			t.Fatalf("syscall hit fsid %s, not ours", c.FSID)
		}
	}
	// NFS keeps rule 6: its source carries the token, so a duplicate is refused.
	snap := Snapshot{Boot: testBoot, Entries: []Entry{rootEntry(), ourEntry(), {FSID: otherFSID, Owner: testUID, Type: "nfs", MntOnName: "/elsewhere", MntFromName: ourSource}}}
	r := Receipt{Version: receiptVersion, UID: testUID, Boot: testBoot, OpID: "op-n", Type: TypeNFS, FSID: ourFSID, Token: testToken, MntOnName: testPath, MntFromName: ourSource}
	if v := Verify(r, service(), snap); v.Kind != VerdictRefused {
		t.Fatalf("duplicate NFS source not refused: %v %s", v.Kind, v.Reason)
	}
}

// §6: per-uid receipt limit.
func TestReceiptLimit(t *testing.T) {
	st, err := OpenStore(testRoot(t), 3)
	if err != nil {
		t.Fatal(err)
	}
	r := validReceipt("op0")
	for i := 0; i < 3; i++ {
		r.OpID = "op" + string(rune('a'+i))
		if err := st.Put(r); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	r.OpID = "opd"
	if err := st.Put(r); !errors.Is(err, ErrReceiptLimit) {
		t.Fatalf("4th receipt: %v, want ErrReceiptLimit", err)
	}
	r.UID = 502
	if err := st.Put(r); err != nil {
		t.Fatalf("other uid must have its own budget: %v", err)
	}
	if err := st.Delete(testUID, "opa"); err != nil {
		t.Fatal(err)
	}
	r.UID = testUID
	if err := st.Put(r); err != nil {
		t.Fatalf("after delete: %v", err)
	}
	if err := st.Put(r); !errors.Is(err, ErrReceiptExists) {
		t.Fatalf("duplicate op: %v", err)
	}
}

func validReceipt(op string) Receipt {
	return Receipt{Version: receiptVersion, UID: testUID, Boot: testBoot, OpID: op, Type: TypeNFS, FSID: ourFSID,
		Token: testToken, MntOnName: testPath, MntFromName: ourSource, CreatedAt: time.Unix(1_800_000_000, 0).UTC()}
}

// §6: a crash at any point of a receipt write leaves either no receipt or a
// complete one, never a torn file; temp files are swept on reopen.
func TestCrashDuringReceiptWrite(t *testing.T) {
	for _, stage := range []string{"partial-write", "before-rename"} {
		dir := testRoot(t)
		st, err := OpenStore(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		st.crash = func(s string) error {
			if s == stage {
				return errTestCrash
			}
			return nil
		}
		if err := st.Put(validReceipt("op1")); !errors.Is(err, errTestCrash) {
			t.Fatalf("%s: put %v", stage, err)
		}
		// "Restart": a fresh store over the same directory.
		st2, err := OpenStore(dir, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st2.Get(testUID, "op1"); !errors.Is(err, ErrNoReceipt) {
			t.Fatalf("%s: after crash Get = %v, want ErrNoReceipt (no torn receipt)", stage, err)
		}
		if rs, err := st2.List(testUID); err != nil || len(rs) != 0 {
			t.Fatalf("%s: after crash List = %+v %v", stage, rs, err)
		}
		names, _ := os.ReadDir(filepath.Join(dir, "501"))
		if len(names) != 0 {
			t.Fatalf("%s: leftover files after reopen: %v", stage, names)
		}
		if err := st2.Put(validReceipt("op1")); err != nil {
			t.Fatalf("%s: put after recovery: %v", stage, err)
		}
		if got, err := st2.Get(testUID, "op1"); err != nil || got.FSID != ourFSID {
			t.Fatalf("%s: reload %+v %v", stage, got, err)
		}
	}
}

func TestCorruptReceiptIsNotAbsent(t *testing.T) {
	dir := testRoot(t)
	st, _ := OpenStore(dir, 0)
	if err := st.Put(validReceipt("op1")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "501", "op1.json"), []byte(`{"version":1,"uid":501`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Get(testUID, "op1"); !errors.Is(err, ErrInvalidReceipt) {
		t.Fatalf("corrupt receipt: %v", err)
	}
	if _, err := st.List(testUID); err == nil {
		t.Fatalf("List must surface a corrupt receipt")
	}
}

func TestPredicateTruthTable(t *testing.T) {
	r := validReceipt("op1")
	c := service()
	base := func() Snapshot { return Snapshot{Boot: testBoot, Entries: []Entry{rootEntry(), ourEntry()}} }
	cases := []struct {
		name string
		mut  func(*Snapshot, *Caller)
		want VerdictKind
	}{
		{"ours", func(*Snapshot, *Caller) {}, VerdictAct},
		{"boot", func(s *Snapshot, _ *Caller) { s.Boot = "x" }, VerdictVoid},
		{"absent", func(s *Snapshot, _ *Caller) { s.Entries = s.Entries[:1] }, VerdictAbsent},
		{"caller uid", func(_ *Snapshot, c *Caller) { c.UID = 502 }, VerdictRefused},
		{"owner", func(s *Snapshot, _ *Caller) { s.Entries[1].Owner = 502 }, VerdictRefused},
		{"type", func(s *Snapshot, _ *Caller) { s.Entries[1].Type = "macfuse" }, VerdictRefused},
		{"mntonname", func(s *Snapshot, _ *Caller) { s.Entries[1].MntOnName = "/Volumes/zpool 2" }, VerdictRefused},
		{"mntfromname", func(s *Snapshot, _ *Caller) { s.Entries[1].MntFromName += "x" }, VerdictRefused},
		{"duplicate source", func(s *Snapshot, _ *Caller) {
			s.Entries = append(s.Entries, Entry{FSID: otherFSID, Owner: 0, Type: "nfs", MntOnName: "/elsewhere", MntFromName: ourSource})
		}, VerdictRefused},
		{"duplicate fsid", func(s *Snapshot, _ *Caller) {
			s.Entries = append(s.Entries, Entry{FSID: ourFSID, Owner: testUID, Type: "nfs", MntOnName: "/x", MntFromName: "y"})
		}, VerdictRefused},
	}
	for _, tc := range cases {
		s, cc := base(), c
		tc.mut(&s, &cc)
		if got := Verify(r, cc, s); got.Kind != tc.want {
			t.Errorf("%s: verdict %s (%s), want %s", tc.name, got.Kind, got.Reason, tc.want)
		}
	}
	s := base()
	s.Entries = append(s.Entries, Entry{FSID: otherFSID, Owner: 0, Type: "smbfs", MntOnName: "/VOLUMES/ZPOOL/child", MntFromName: "//x/y"})
	if v := Verify(r, c, s); v.Kind != VerdictAct || len(v.Descendants) != 1 {
		t.Errorf("case-folded descendant not reported: %+v", v)
	}
}
