package unmountd

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const (
	testBoot = "11111111-2222-3333-4444-555555555555"
	testUID  = uint32(501)
	testPath = "/Volumes/zpool"
)

// Synthetic 128-bit mount tokens (built, not literal, so secret scanners
// do not mistake fixtures for credentials).
var (
	testToken  = strings.Repeat("0123456789abcdef", 2)
	otherToken = strings.Repeat("fedcba9876543210", 2)
	ourFSID    = FSID{0x1a000031, 0x31}
	otherFSID  = FSID{0x1a000032, 0x31}
	ourSource  = "127.0.0.1:/jm-" + testToken
)

func ourEntry() Entry {
	return Entry{FSID: ourFSID, Owner: testUID, Type: "nfs", MntOnName: testPath, MntFromName: ourSource}
}

func rootEntry() Entry {
	return Entry{FSID: FSID{0x1000004, 0x1a}, Owner: 0, Type: "apfs", MntOnName: "/", MntFromName: "/dev/disk3s1s1"}
}

func service() Caller {
	return Caller{UID: testUID, Role: RoleService, SigningID: "com.juicemount.service"}
}

type kcallRec struct {
	FSID  FSID
	Flags int
}

// fakeKernel models the parts of XNU the helper depends on, including the
// MNT_LUNMOUNT trap: while an unmount is in flight, every other unmount of
// that mount returns EBUSY (vfs_syscalls.c:2722).
type fakeKernel struct {
	mu      sync.Mutex
	snap    Snapshot
	snapFn  func(n int) Snapshot // optional: per-call snapshot sequence
	snaps   int
	calls   []kcallRec
	busy    map[FSID]bool         // ordinary unmount returns EBUSY (files open)
	noop    map[FSID]bool         // unmount returns 0 but the mount stays (fs vfs_sysctl intercept)
	dead    map[FSID]bool         // server dead: a non-forced unmount blocks in VFS_SYNC
	block   chan struct{}         // if set, every call blocks until closed
	blockFn func(flags int) error // result after block releases (nil: remove)
	inLU    map[FSID]bool
	// snapBlock, if set, holds every Snapshot until closed.
	snapBlock chan struct{}
}

func newFake(entries ...Entry) *fakeKernel {
	return &fakeKernel{snap: Snapshot{Boot: testBoot, Entries: entries},
		busy: map[FSID]bool{}, noop: map[FSID]bool{}, dead: map[FSID]bool{}, inLU: map[FSID]bool{}}
}

func (k *fakeKernel) Snapshot() (Snapshot, error) {
	k.mu.Lock()
	gate := k.snapBlock
	k.mu.Unlock()
	if gate != nil {
		<-gate
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.snaps++
	if k.snapFn != nil {
		s := k.snapFn(k.snaps)
		return s, nil
	}
	s := k.snap
	s.Entries = append([]Entry(nil), s.Entries...)
	return s, nil
}

func (k *fakeKernel) setEntries(es ...Entry) {
	k.mu.Lock()
	k.snap.Entries = es
	k.mu.Unlock()
}

func (k *fakeKernel) remove(fsid FSID) {
	var keep []Entry
	for _, e := range k.snap.Entries {
		if e.FSID != fsid {
			keep = append(keep, e)
		}
	}
	k.snap.Entries = keep
}

func (k *fakeKernel) UnmountByFSID(fsid FSID, flags int) error {
	k.mu.Lock()
	k.calls = append(k.calls, kcallRec{fsid, flags})
	if k.inLU[fsid] {
		k.mu.Unlock()
		return syscall.EBUSY // MNT_LUNMOUNT: even MNT_FORCE cannot override
	}
	present := false
	for _, e := range k.snap.Entries {
		if e.FSID == fsid {
			present = true
		}
	}
	if !present {
		k.mu.Unlock()
		return syscall.ENOENT
	}
	block, blockFn := k.block, k.blockFn
	deadOrdinary := k.dead[fsid] && flags&FlagForce == 0
	if block != nil || deadOrdinary {
		k.inLU[fsid] = true
	}
	k.mu.Unlock()
	if deadOrdinary {
		select {} // VFS_SYNC(MNT_WAIT) against a dead server never returns
	}
	if block != nil {
		<-block
		k.mu.Lock()
		defer k.mu.Unlock()
		delete(k.inLU, fsid)
		if blockFn != nil {
			if err := blockFn(flags); err != nil {
				return err
			}
		}
		k.remove(fsid)
		return nil
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.busy[fsid] && flags&FlagForce == 0 {
		return syscall.EBUSY
	}
	if k.noop[fsid] {
		return nil
	}
	k.remove(fsid)
	return nil
}

func (k *fakeKernel) recorded() []kcallRec {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]kcallRec(nil), k.calls...)
}

type fakeHolders struct{ hs []Holder }

func (f fakeHolders) Holders(uint32, FSID) ([]Holder, error) { return f.hs, nil }

func fastPolicy() Policy {
	return Policy{
		Watch: 150 * time.Millisecond, InKernelPoll: 20 * time.Millisecond, SnapshotWatch: time.Second,
		BackoffMin: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond, NestedRetry: 15 * time.Millisecond,
		ForceConfirmGap: 5 * time.Millisecond, ErrorRetry: 10 * time.Millisecond, OrphanReclaim: 60 * time.Millisecond,
	}
}

func newTestService(t *testing.T, k Kernel, h HolderLister) (*Service, *Store) {
	t.Helper()
	st, err := OpenStore(testRoot(t), 0)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := NewService(Config{Store: st, Kernel: k, Holders: h, Policy: fastPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(svc.Close)
	return svc, st
}

func nfsRegister(op string) RegisterRequest {
	return RegisterRequest{OpID: op, Type: TypeNFS, FSID: ourFSID, Token: testToken, MntOnName: testPath, MntFromName: ourSource}
}

func mustRegister(t *testing.T, svc *Service, req RegisterRequest) Receipt {
	t.Helper()
	r, err := svc.Register(service(), req)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	return r
}

func retire(t *testing.T, svc *Service, op string, req RetireRequest, d time.Duration) (Outcome, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return svc.Retire(ctx, service(), op, req)
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func flagsOf(cs []kcallRec) string {
	var b strings.Builder
	for _, c := range cs {
		switch c.Flags {
		case FlagForce:
			b.WriteString("F")
		case FlagNoBlock:
			b.WriteString("N")
		default:
			b.WriteString("?")
		}
	}
	return b.String()
}

var errTestCrash = errors.New("simulated crash")
var errBusy error = syscall.EBUSY

// testRoot is a fresh 0700 receipt root (t.TempDir itself is 0755, which the
// store rightly refuses).
func testRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "unmountd")
}
