package nfs

import (
	"context"
	"crypto/sha256"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

type boundFDTest struct {
	store    *metadata.Store
	meta     *metadata.SpoolStore
	drainer  *Drainer
	lease    *boundFDPhase
	src, dst *os.File
	dest     string
	obs      metadata.BoundModeObservation
}

func newBoundFDTest(t *testing.T, name string, mode fs.FileMode) boundFDTest {
	t.Helper()
	store, err := metadata.Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err = metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	meta := metadata.NewSpoolStore(store.DB())
	store.SetSpoolStore(meta)
	root := t.TempDir()
	spool, err := NewSpoolStore(root, 0, meta)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(spool.Stop)
	drainer, err := NewDrainer(spool, DrainerConfig{FuseRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("exact private bytes")
	file := filepath.Join(root, SpoolFilesSubdir, "birth-"+name)
	src, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if _, err = src.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = src.Sync(); err != nil {
		t.Fatal(err)
	}
	path := name + ".mov"
	expected, err := store.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	birth, err := metadata.NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(path, false, int64(len(body)), time.Unix(1700000000, 0), 520)
	e.Mode = mode
	r, _, err := store.InsertBoundSpoolBirth(expected, e, file, birth)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if err = meta.MarkReady(r.SpoolID, int64(len(body)), digest[:]); err != nil {
		t.Fatal(err)
	}
	ok, err := meta.MarkDraining(r.SpoolID)
	if err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	obs, err := store.CaptureBoundMode(r.SpoolID, r.BirthID, path, file, r.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backing")
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if _, err = dst.Write(body); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := drainer.boundPhases.begin(store, obs, src, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	return boundFDTest{store, meta, drainer, lease, src, dst, dest, obs}
}

func addBoundFDSibling(t *testing.T, first boundFDTest, name string) boundFDTest {
	t.Helper()
	body := []byte("second exact image")
	file := filepath.Join(filepath.Dir(first.obs.SpoolFile), "birth-"+name)
	src, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	if _, err = src.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = src.Sync(); err != nil {
		t.Fatal(err)
	}
	path := name + ".mov"
	expected, err := first.store.CaptureMirror(path)
	if err != nil {
		t.Fatal(err)
	}
	birth, err := metadata.NewSpoolCustodyID()
	if err != nil {
		t.Fatal(err)
	}
	e := metadata.MakeEntry(path, false, int64(len(body)), time.Unix(1700000000, 0), 521)
	e.Mode = 0644
	r, _, err := first.store.InsertBoundSpoolBirth(expected, e, file, birth)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if err = first.meta.MarkReady(r.SpoolID, int64(len(body)), digest[:]); err != nil {
		t.Fatal(err)
	}
	ok, err := first.meta.MarkDraining(r.SpoolID)
	if err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	obs, err := first.store.CaptureBoundMode(r.SpoolID, r.BirthID, path, file, r.FileIdentity)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(t.TempDir(), "backing")
	dst, err := os.OpenFile(dest, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if _, err = dst.Write(body); err != nil {
		t.Fatal(err)
	}
	_, dev, _, err := fdPhaseIdentity(dst)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.drainer.boundPhases.begin(first.store, obs, src, dst, dest, dev)
	if err != nil {
		t.Fatal(err)
	}
	return boundFDTest{first.store, first.meta, first.drainer, lease, src, dst, dest, obs}
}

func TestBoundFDTimeoutReplacementAndStopCustody(t *testing.T) {
	for _, mode := range []fs.FileMode{0444, 0644} {
		t.Run(mode.String(), func(t *testing.T) {
			f := newBoundFDTest(t, "replacement", mode)
			started := make(chan struct{})
			release := make(chan struct{})
			mt := time.Unix(1800000000, 123456000)
			op := func(dst *os.File, m fs.FileMode, tm time.Time, withTime bool) error {
				close(started)
				<-release
				return defaultBoundFDOperation(dst, m, tm, withTime)
			}
			_, err := f.lease.settle(mt, 10*time.Millisecond, op)
			var pending *PendingBoundFDPhase
			if !errors.As(err, &pending) {
				t.Fatalf("settle timeout %v", err)
			}
			<-started
			if f.drainer.Stop(0) {
				t.Fatal("Stop released blocked FD")
			}
			if _, _, err = f.lease.takeForReconciliation(); err == nil {
				t.Fatal("live worker transferred")
			}
			old := f.dest + ".old"
			if err = os.Rename(f.dest, old); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(f.dest, []byte("successor"), 0600); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err = f.lease.join(context.Background()); !errors.Is(err, ErrBoundFDPhase) {
				t.Fatalf("replacement join %v", err)
			}
			fi, err := f.dst.Stat()
			if err != nil || fi.Mode().Perm() != mode.Perm() || fi.ModTime().UnixMicro() != mt.UnixMicro() {
				t.Fatalf("old FD %+v %v", fi, err)
			}
			successor, err := os.ReadFile(f.dest)
			if err != nil || string(successor) != "successor" {
				t.Fatalf("successor %q %v", successor, err)
			}
			fi, err = os.Stat(f.dest)
			if err != nil || fi.Mode().Perm() != 0600 {
				t.Fatalf("successor mode %+v %v", fi, err)
			}
			var holds int
			if err = f.store.DB().QueryRow("SELECT count(*) FROM spool_completion_holds").Scan(&holds); err != nil || holds != 0 {
				t.Fatalf("premature P40 hold %d %v", holds, err)
			}
			if err = f.store.CheckSpoolRecovery(); err == nil {
				t.Fatal("birth fence lost")
			}
			if f.drainer.Stop(10 * time.Millisecond) {
				t.Fatal("Stop released pending source")
			}
			src, dst, err := f.lease.takeForReconciliation()
			if err != nil || src != f.src || dst != f.dst {
				t.Fatalf("handoff %p %p %v", src, dst, err)
			}
			if !f.drainer.Stop(time.Second) {
				t.Fatal("Stop did not join reconciled phase")
			}
		})
	}
}

func TestBoundFDModeRetryAndBatchOwnership(t *testing.T) {
	f := newBoundFDTest(t, "batch", 0644)
	mt := time.Unix(1800000000, 123456000)
	start := make(chan struct{})
	release := make(chan struct{})
	op := func(dst *os.File, m fs.FileMode, tm time.Time, withTime bool) error {
		close(start)
		<-release
		return defaultBoundFDOperation(dst, m, tm, withTime)
	}
	next, err := f.lease.setattr(0444, 10*time.Millisecond, op)
	var pending *PendingBoundFDPhase
	if !errors.As(err, &pending) || next.Token.Revision != 0 {
		t.Fatalf("timed SETATTR %+v %v", next, err)
	}
	<-start
	if f.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("Stop released timed SETATTR")
	}
	if _, err = f.lease.settle(mt, 10*time.Millisecond, nil); !errors.As(err, &pending) {
		t.Fatalf("settle while busy %v", err)
	}
	close(release)
	if err = f.lease.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	obs, err := f.lease.settle(mt, time.Second, nil)
	if err != nil || obs.Mode.Perm() != 0444 || obs.Token.Revision != f.obs.Token.Revision+1 {
		t.Fatalf("retry %+v %v", obs, err)
	}
	batchObs, identity, err := f.lease.transferBatch()
	if err != nil || batchObs != obs {
		t.Fatalf("batch %+v %v", batchObs, err)
	}
	item := metadata.BoundDrainCommitItem{SpoolID: obs.SpoolID, BirthID: obs.BirthID, Path: obs.Path, SpoolFile: obs.SpoolFile, FileIdentity: obs.FileIdentity,
		BackingIdentity: identity, Source: f.src, FullSize: obs.FullSize, Expected: obs.Token, Mtime: mt}
	results, err := f.store.BatchCompleteBoundSpools([]metadata.BoundDrainCommitItem{item})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.drainer.boundPhases.acceptBatchResults([]*boundFDPhase{f.lease}, nil); !errors.As(err, &pending) {
		t.Fatalf("missing result %v", err)
	}
	if f.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("Stop released unacknowledged result")
	}
	if err = f.drainer.boundPhases.acceptBatchResults([]*boundFDPhase{f.lease}, results); err != nil {
		t.Fatal(err)
	}
	if f.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("Stop released validated but untransferred FD")
	}
	src, res, err := f.lease.takeForEffects()
	if err != nil || src != f.src || res.CompletionID != results[0].CompletionID {
		t.Fatalf("effects handoff %p %+v %v", src, res, err)
	}
	if !f.drainer.Stop(time.Second) {
		t.Fatal("Stop did not finish after explicit transfer")
	}
}

func TestBoundFDStartedStopWaitsForWorkerAndLease(t *testing.T) {
	f := newBoundFDTest(t, "started", 0644)
	f.drainer.Start()
	start, release := make(chan struct{}), make(chan struct{})
	op := func(dst *os.File, mode fs.FileMode, mt time.Time, withTime bool) error {
		close(start)
		<-release
		return defaultBoundFDOperation(dst, mode, mt, withTime)
	}
	_, err := f.lease.settle(time.Unix(1800000000, 0), 10*time.Millisecond, op)
	var pending *PendingBoundFDPhase
	if !errors.As(err, &pending) {
		t.Fatalf("blocked operation %v", err)
	}
	<-start
	if f.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("started Stop passed live worker")
	}
	close(release)
	if err = f.lease.join(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("started Stop passed still-owned source")
	}
	src, dst, err := f.lease.takeForReconciliation()
	if err != nil || src != f.src || dst != f.dst {
		t.Fatalf("reconciliation handoff %p %p %v", src, dst, err)
	}
	if !f.drainer.Stop(time.Second) {
		t.Fatal("started Stop did not finish after explicit handoff")
	}
}

func TestBoundFDEPERMRequiresExactFDMode(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "predecessor")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	deny := func(*os.File, fs.FileMode) error { return syscall.EPERM }
	if err = f.Chmod(0444); err != nil {
		t.Fatal(err)
	}
	identity, _, _, err := fdPhaseIdentity(f)
	if err != nil {
		t.Fatal(err)
	}
	if err = boundFDOperationWithChmod(f, 0444, time.Time{}, false, deny); err != nil {
		t.Fatalf("already-set mode %v", err)
	}
	got, _, fi, err := fdPhaseIdentity(f)
	if err != nil || got != identity || fi.Mode().Perm() != 0444 {
		t.Fatalf("FD after EPERM %+v %q %v", fi, got, err)
	}
	if err = f.Chmod(0600); err != nil {
		t.Fatal(err)
	}
	if err = boundFDOperationWithChmod(f, 0444, time.Time{}, false, deny); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("wrong mode EPERM %v", err)
	}
}

func TestBoundFDBatchReorderedSiblingReceipts(t *testing.T) {
	a := newBoundFDTest(t, "sibling-a", 0644)
	b := addBoundFDSibling(t, a, "sibling-b")
	mt := time.Unix(1800000000, 0)
	items := make([]metadata.BoundDrainCommitItem, 0, 2)
	for _, f := range []boundFDTest{a, b} {
		obs, err := f.lease.settle(mt, time.Second, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, identity, err := f.lease.transferBatch()
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, metadata.BoundDrainCommitItem{SpoolID: obs.SpoolID, BirthID: obs.BirthID, Path: obs.Path,
			SpoolFile: obs.SpoolFile, FileIdentity: obs.FileIdentity, BackingIdentity: identity, Source: f.src,
			FullSize: obs.FullSize, Expected: obs.Token, Mtime: mt})
	}
	results, err := a.store.BatchCompleteBoundSpools(items)
	if err != nil {
		t.Fatal(err)
	}
	leases := []*boundFDPhase{a.lease, b.lease}
	var pending *PendingBoundFDPhase
	if err = a.drainer.boundPhases.acceptBatchResults([]*boundFDPhase{a.lease, a.lease}, results); !errors.As(err, &pending) {
		t.Fatalf("duplicate lease omitted sibling %v", err)
	}
	if _, _, err = a.lease.takeForEffects(); err == nil {
		t.Fatal("duplicate cohort transferred private source")
	}
	if err = a.drainer.boundPhases.acceptBatchResults(leases, results[:1]); !errors.As(err, &pending) {
		t.Fatalf("missing sibling %v", err)
	}
	if a.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("missing sibling released registry")
	}
	if _, _, err = a.lease.takeForEffects(); err == nil {
		t.Fatal("unvalidated sibling transferred")
	}
	results[0], results[1] = results[1], results[0]
	if err = a.drainer.boundPhases.acceptBatchResults(leases, results); err != nil {
		t.Fatalf("reordered %v", err)
	}
	if a.drainer.Stop(10 * time.Millisecond) {
		t.Fatal("validated siblings released registry")
	}
	for _, l := range leases {
		if _, _, err = l.takeForEffects(); err != nil {
			t.Fatal(err)
		}
	}
	if !a.drainer.Stop(time.Second) {
		t.Fatal("explicit sibling handoff did not finish Stop")
	}
}

func TestBoundFDLiteralDrainerStopCompatibility(t *testing.T) {
	d := &Drainer{stop: make(chan struct{}), shutdownDone: make(chan struct{})}
	if !d.Stop(0) {
		t.Fatal("direct-literal drainer could not stop")
	}
	_, constructed := newTestDrainer(t, DrainerConfig{})
	if !constructed.Stop(0) {
		t.Fatal("empty constructed drainer did not stop synchronously")
	}
}
