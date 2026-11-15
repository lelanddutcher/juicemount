package metadata

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func dirChildValue(inode uint32) string {
	v := make([]byte, 9)
	v[0] = jfsTypeFile
	binary.BigEndian.PutUint32(v[5:], inode)
	return string(v)
}

// Review H1 of #100: a folder update that meets a file the Mac's own save
// holds must be a wait. Before: reconcileDir took the failure path
// (scheduleAttrRetry): baseline debt, push DEGRADED, the persisted
// incomplete-baseline mark, and the same call failed "SCAN invalidated", so
// a teammate's new file in that folder never got its directory refresh.
func TestReconcileDirOwnedChildIsAWaitNotAFailure(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var mu sync.Mutex
	children := map[string]string{"b0.prproj": dirChildValue(42)}
	attrs := map[string][]byte{"i42": encodeTestAttr(1700000000, 4096)}
	rc := &RedisClient{
		store: store, stopCh: make(chan struct{}),
		testReadDirRaw: func(uint64) (map[string]string, error) {
			mu.Lock()
			defer mu.Unlock()
			out := map[string]string{}
			for k, v := range children {
				out[k] = v
			}
			return out, nil
		},
		testReadAttrs: func(keys []string) ([]interface{}, error) {
			mu.Lock()
			defer mu.Unlock()
			out := make([]interface{}, len(keys))
			for i, k := range keys {
				out[i] = string(attrs[k])
			}
			return out, nil
		},
	}
	defer close(rc.stopCh)
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	rc.setEngagement(keyspaceEnabled)
	if err := store.SetMeta(metaKeyIncompleteBaseline, "0"); err != nil {
		t.Fatal(err)
	}
	var requeued []uint64
	var rqMu sync.Mutex
	requeue := requeueFunc(func(inode uint64) { rqMu.Lock(); requeued = append(requeued, inode); rqMu.Unlock() })
	rc.keyspaceRequeue.Store(&requeue)
	oldBase := attrRetryBase
	attrRetryBase = 20 * time.Millisecond
	defer func() { attrRetryBase = oldBase }()

	// A teammate adds c0; the Mac's own save holds b0, whose size moved.
	mu.Lock()
	children["c0.prproj"] = dirChildValue(43)
	attrs["i43"] = encodeTestAttr(1700000050, 100)
	attrs["i42"] = encodeTestAttr(1700000060, 8192)
	mu.Unlock()
	store.SetForegroundOwner(func(p string) bool { return p == "b0.prproj" })
	rc.lifecycleMu.Lock()
	debt0, pub0 := rc.debtGeneration, rc.publicationGeneration
	rc.lifecycleMu.Unlock()

	if err := rc.reconcileDir(1); err != nil {
		t.Fatalf("reconcileDir with an owned child = %v, want nil", err)
	}
	if got := keyspaceEngagement(rc.engaged.Load()); got != keyspaceEnabled {
		t.Fatalf("engagement = %v, want ENABLED", got)
	}
	rc.lifecycleMu.Lock()
	debt1, pub1 := rc.debtGeneration, rc.publicationGeneration
	rc.lifecycleMu.Unlock()
	if debt1 != debt0 {
		t.Fatalf("baseline debt %d -> %d, want unchanged", debt0, debt1)
	}
	if pub1 <= pub0 {
		t.Fatalf("publication generation %d -> %d: the folder refresh (NoteDirectoryChanged) did not run", pub0, pub1)
	}
	if raw, _, _ := store.GetMeta(metaKeyIncompleteBaseline); raw != "0" {
		t.Fatalf("incomplete-baseline mark = %q, want 0", raw)
	}
	if store.LookupByPath("c0.prproj") == nil {
		t.Fatal("the teammate's new file was not mirrored")
	}
	if got := store.LookupByPath("b0.prproj"); got == nil || got.Size != 4096 {
		t.Fatalf("owned file was republished: %+v", got)
	}
	rc.attrRetryMu.Lock()
	n := len(rc.attrRetries)
	rc.attrRetryMu.Unlock()
	if n != 0 {
		t.Fatalf("failure-path retries scheduled: %d", n)
	}
	// The folder is requeued after attrRetryBase into the live coalescer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		rqMu.Lock()
		got := append([]uint64(nil), requeued...)
		rqMu.Unlock()
		if len(got) == 1 && got[0] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("requeued %v, want [1]", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Released: the requeued read publishes b0.
	store.SetForegroundOwner(nil)
	if err := rc.reconcileDir(1); err != nil {
		t.Fatal(err)
	}
	if got := store.LookupByPath("b0.prproj"); got.Size != 8192 {
		t.Fatalf("after release b0 size = %d, want 8192", got.Size)
	}
}

// Review M1: a deferral made while no push session runs waits for the next
// session, and a file deferral reaches the current attr coalescer, not the
// one that existed when it was deferred.
func TestOwnedDeferralSurvivesResubscribe(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	defer close(rc.stopCh)
	oldBase := attrRetryBase
	attrRetryBase = 10 * time.Millisecond
	defer func() { attrRetryBase = oldBase }()

	rc.deferOwnedFile(42) // no session at all
	time.Sleep(50 * time.Millisecond)
	var mu sync.Mutex
	var dirs, files []uint64
	dirFn := requeueFunc(func(i uint64) { mu.Lock(); dirs = append(dirs, i); mu.Unlock() })
	attrFn := requeueFunc(func(i uint64) { mu.Lock(); files = append(files, i); mu.Unlock() })
	rc.keyspaceRequeue.Store(&dirFn)
	rc.attrRequeue.Store(&attrFn)
	rc.resumeOwnedDeferrals() // what a new session does after wiring
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ok := len(files) == 1 && files[0] == 42
		mu.Unlock()
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("file deferral lost across the gap: files %v dirs %v", files, dirs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Review M2: without the attr push, a pub/sub update dropped for an owned
// file requeues its folder (nothing else would bring a teammate's change
// back before the backstop SCAN).
func TestDroppedFeedEventRequeuesFolderWithoutAttrPush(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Insert(MakeEntry("w", true, 0, testTime(0), 7)); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(MakeEntry("w/a.prproj", false, 10, testTime(0), 8)); err != nil {
		t.Fatal(err)
	}
	rc := &RedisClient{store: store, stopCh: make(chan struct{})}
	defer close(rc.stopCh)
	oldBase := attrRetryBase
	attrRetryBase = 10 * time.Millisecond
	defer func() { attrRetryBase = oldBase }()
	var mu sync.Mutex
	var dirs []uint64
	dirFn := requeueFunc(func(i uint64) { mu.Lock(); dirs = append(dirs, i); mu.Unlock() })
	rc.keyspaceRequeue.Store(&dirFn) // d-feed live, attr push off
	store.SetForegroundOwner(func(p string) bool { return p == "w/a.prproj" })
	rc.applyEvent(MetadataEvent{Op: "update", Path: "w/a.prproj", Size: 99, Mtime: 1700000100, Inode: 8})
	if got := store.LookupByPath("w/a.prproj"); got.Size != 10 {
		t.Fatalf("owned row changed by the feed: %+v", got)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		ok := len(dirs) == 1 && dirs[0] == 7
		mu.Unlock()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("folder requeues %v, want [7]", dirs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Review M3: the relaxed absent-destination check still fails when the
// destination folder changed during the backing rename.
func TestRenameOwnedAbsentDestinationChecksFolder(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, e := range []*Entry{MakeEntry("A", true, 0, testTime(0), 2), MakeEntry("B", true, 0, testTime(0), 3),
		MakeEntry("A/x", false, 5, testTime(0), 4)} {
		if err := store.Insert(e); err != nil {
			t.Fatal(err)
		}
	}
	store.SetForegroundOwner(func(p string) bool { return p == "A/x" || p == "B/x" })
	capture := func() (MirrorToken, MirrorToken, MirrorToken) {
		parent, err := store.CaptureMirror("B")
		if err != nil {
			t.Fatal(err)
		}
		src, dst, err := store.CaptureRename("A/x", "B/x")
		if err != nil {
			t.Fatal(err)
		}
		return src, dst, parent
	}
	src, dst, parent := capture()
	// Unrelated write: allowed (the #100 fix).
	if err := store.Insert(MakeEntry("C", true, 0, testTime(1), 9)); err != nil {
		t.Fatal(err)
	}
	// A teammate renames B away meanwhile (as the pub/sub feed applies it).
	if err := store.Delete("B"); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(MakeEntry("B2", true, 0, testTime(1), 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenameExpectedWithParent(src, dst, parent); err == nil {
		t.Fatal("published B/x under a folder renamed away meanwhile")
	}
	// With the folder unchanged, an unrelated write does not fail it.
	if err := store.Delete("B2"); err != nil {
		t.Fatal(err)
	}
	if err := store.Insert(MakeEntry("B", true, 0, testTime(1), 3)); err != nil {
		t.Fatal(err)
	}
	src, dst, parent = capture()
	if err := store.Insert(MakeEntry("D", true, 0, testTime(2), 10)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenameExpectedWithParent(src, dst, parent); err != nil {
		t.Fatalf("rename with an unchanged folder and an unrelated write = %v", err)
	}
}
