package nfs

import (
	"os"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/metadata"
)

func newContinuationHandleForTest(t *testing.T) (*juiceFS, *SpoolStore, *continuationFS) {
	t.Helper()
	jfs, spool, _, _ := newSpoolWiredHandlerNoDrain(t)
	f, err := jfs.Create("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	handle := jfs.handler.ToHandle(jfs, []string{"clip.mov"})
	fs, _, err := jfs.handler.FromHandle(handle)
	if err != nil {
		t.Fatal(err)
	}
	bound, ok := fs.(*continuationFS)
	if !ok {
		t.Fatalf("newly created, active, durable handle did not qualify: %T", fs)
	}
	if bound.incarnation == "" || bound.rowID <= 0 || bound.entry.Inode() != bound.inode {
		t.Fatal("qualified wrapper lacks original inode/incarnation/row")
	}
	return jfs, spool, bound
}

func TestContinuationOriginalCohortRefusesReplacement(t *testing.T) {
	_, spool, bound := newContinuationHandleForTest(t)
	original := bound.entry
	// Same mirror incarnation and pathname, different indexed cohort. A
	// pathname-only OpenWrite would select the replacement; exact admission
	// must refuse before touching its refcount or backing file.
	replacement := &SpoolEntry{id: bound.rowID + 1, nfsPath: bound.path, store: spool}
	replacement.SetInode(bound.inode)
	replacement.cohortID.Store(replacement.id)
	replacement.writing.Store(true)
	spool.index.Insert(bound.path, replacement)
	defer spool.index.Insert(bound.path, original)
	if f, err := bound.OpenFile(bound.path, os.O_RDWR, 0); err == nil {
		_ = f.Close()
		t.Fatal("old handle adopted replacement spool cohort")
	}
	if replacement.refcount != 0 {
		t.Fatalf("replacement refcount changed: %d", replacement.refcount)
	}
}

func TestContinuationOldHandleRefusesMirrorABA(t *testing.T) {
	jfs, _, bound := newContinuationHandleForTest(t)
	if err := jfs.handler.store.Delete(bound.path); err != nil {
		t.Fatal(err)
	}
	newInode := bound.inode + 123
	if err := jfs.handler.store.Insert(metadata.MakeEntry(bound.path, false, 0, time.Now(), newInode)); err != nil {
		t.Fatal(err)
	}
	if f, err := bound.OpenFile(bound.path, os.O_RDWR, 0); err == nil {
		_ = f.Close()
		t.Fatal("old handle adopted same-path replacement mirror")
	}
	if got := jfs.handler.store.LookupByPath(bound.path); got == nil || got.Inode != newInode {
		t.Fatalf("replacement mirror changed: %+v", got)
	}
}

func TestContinuationReadyHandleKeepsExistingReopen(t *testing.T) {
	jfs, spool, bound := newContinuationHandleForTest(t)
	if err := bound.entry.Finalize(); err != nil {
		t.Fatal(err)
	}
	if bound.entry.writing.Load() {
		t.Fatal("READY entry remained eligible for FIFO continuation")
	}
	handle := jfs.handler.ToHandle(jfs, []string{"clip.mov"})
	fs, _, err := jfs.handler.FromHandle(handle)
	if err != nil {
		t.Fatal(err)
	}
	if _, wrapped := fs.(*continuationFS); wrapped {
		t.Fatal("READY handle bypassed existing READY reopen path")
	}
	f, err := fs.OpenFile(bound.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("existing READY reopen failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if current, ok := spool.LookupActive(bound.path); !ok || current != bound.entry {
		t.Fatal("READY reopen changed cohort")
	}
}

func TestContinuationIncompleteStopAlsoClosesNamespaceAdmission(t *testing.T) {
	jfs, _, _, _ := newSpoolWiredHandlerNoDrain(t)
	h := jfs.handler
	holder, err := h.continuations.acquire("clip.mov", time.Now().Add(5*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if holder != nil {
			holder()
		}
	}()
	if h.stopHandlerWithDeadline(10 * time.Millisecond) {
		t.Fatal("Stop completed with live opener holder")
	}
	if op := h.namespace.begin([]string{"unrelated.mov"}); op != nil {
		op.finish(nil)
		t.Fatal("incomplete Stop left global namespace admission open")
	}
	holder()
	holder = nil
	if !h.stopHandlerWithDeadline(5 * time.Second) {
		t.Fatal("Stop retry did not join released holder")
	}
}
