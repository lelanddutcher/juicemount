package cache

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func flushTestPair(r *Reader, key string, sliceID uint64, size int64) {
	r.sliceMu.Lock()
	r.sliceCache[key] = []SliceInfo{{Pos: 0, SliceID: sliceID, Size: uint32(size), Len: uint32(size)}}
	r.attrWitness[key] = ResidentAttr{Size: size, MtimeNS: 123, Valid: true}
	r.sliceMu.Unlock()
}

func TestFlushMappingSnapshotRequiresPath(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	if err := r.FlushMappingSnapshot(); err == nil {
		t.Fatal("flush claimed durability without a snapshot path")
	}
}

func TestFlushMappingSnapshotCannotBeOverwrittenByOlderCapture(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	path := filepath.Join(t.TempDir(), "maps.gob")
	if err := r.SetMappingSnapshotPath(path); err != nil {
		t.Fatal(err)
	}
	flushTestPair(r, "3_0", 11, 4096)
	captured := make(chan struct{})
	release := make(chan struct{})
	var captures atomic.Int32
	var releaseOnce sync.Once
	var writers sync.WaitGroup
	r.testAfterMappingSnapshotCapture = func() {
		if captures.Add(1) == 1 {
			close(captured)
			<-release
		}
	}
	older := make(chan error, 1)
	writers.Add(1)
	go func() {
		defer writers.Done()
		older <- r.persistMappingSnapshot()
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		writers.Wait()
	}()
	<-captured
	if r.mappingWriteMu.TryLock() {
		r.mappingWriteMu.Unlock()
		t.Fatal("snapshot capture occurred without holding writer serialization")
	}
	flushTestPair(r, "3_0", 22, 8192)
	newer := make(chan error, 1)
	writers.Add(1)
	newerStarted := make(chan struct{})
	go func() {
		defer writers.Done()
		close(newerStarted)
		newer <- r.FlushMappingSnapshot()
	}()
	<-newerStarted
	releaseOnce.Do(func() { close(release) })
	if err := <-older; err != nil {
		t.Fatal(err)
	}
	if err := <-newer; err != nil {
		t.Fatal(err)
	}
	// Both writers have settled. The durable state must be the later pair.
	reloaded := NewReader(r.cacheDir, DefaultBlockSize, nil)
	defer reloaded.Stop()
	if err := reloaded.SetMappingSnapshotPath(path); err != nil {
		t.Fatal(err)
	}
	reloaded.sliceMu.RLock()
	gotSlice := append([]SliceInfo(nil), reloaded.sliceCache["3_0"]...)
	gotAttr := reloaded.attrWitness["3_0"]
	reloaded.sliceMu.RUnlock()
	if !reflect.DeepEqual(gotSlice, []SliceInfo{{Pos: 0, SliceID: 22, Size: 8192, Len: 8192}}) || gotAttr != (ResidentAttr{Size: 8192, MtimeNS: 123, Valid: true}) {
		t.Fatalf("reopened pair: slices=%+v attr=%+v", gotSlice, gotAttr)
	}
}

func TestFlushMappingSnapshotStopJoinsActualIO(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	if err := r.SetMappingSnapshotPath(filepath.Join(t.TempDir(), "maps.gob")); err != nil {
		t.Fatal(err)
	}
	flushTestPair(r, "3_0", 11, 4096)
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var calls atomic.Int32
	r.testSyncMappingDirectory = func(directory *os.File) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return directory.Sync()
	}
	flushDone := make(chan error, 1)
	stopDone := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		flushDone <- r.FlushMappingSnapshot()
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		workers.Wait()
		r.Stop()
	}()
	<-entered
	workers.Add(1)
	go func() {
		defer workers.Done()
		r.Stop()
		close(stopDone)
	}()
	deadline := time.After(5 * time.Second)
	for !r.stopped.Load() {
		select {
		case <-deadline:
			t.Fatal("Stop did not begin")
		default:
		}
		runtime.Gosched()
	}
	select {
	case <-stopDone:
		t.Fatal("Stop returned before in-flight directory sync settled")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-flushDone; err != nil {
		t.Fatal(err)
	}
	<-stopDone
	if err := r.FlushMappingSnapshot(); err == nil {
		t.Fatal("late flush accepted after Stop joined")
	}
}

func TestFlushMappingSnapshotRejectsOversizedCapture(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	path := filepath.Join(t.TempDir(), "maps.gob")
	if err := r.SetMappingSnapshotPath(path); err != nil {
		t.Fatal(err)
	}
	flushTestPair(r, "3_0", 11, 4096)
	r.testMappingSnapshotMaxBytes = 1
	if err := r.FlushMappingSnapshot(); err == nil {
		t.Fatal("oversized snapshot was acknowledged")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized snapshot was installed: %v", err)
	}
	r.testMappingSnapshotMaxBytes = 0
	if err := r.FlushMappingSnapshot(); err != nil {
		t.Fatalf("valid retry: %v", err)
	}
}

func TestFlushMappingSnapshotRefusesMissingParent(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	path := filepath.Join(t.TempDir(), "absent", "maps.gob")
	if err := r.SetMappingSnapshotPath(path); err != nil {
		t.Fatal(err)
	}
	if err := r.FlushMappingSnapshot(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("flush without durable parent = %v", err)
	}
}

func TestFlushMappingSnapshotRejectsAfterStop(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	if err := r.SetMappingSnapshotPath(filepath.Join(t.TempDir(), "maps.gob")); err != nil {
		t.Fatal(err)
	}
	r.Stop()
	if err := r.FlushMappingSnapshot(); err == nil {
		t.Fatal("stopped reader accepted a late durable publication")
	}
}

func TestFlushMappingSnapshotReportsDirectorySyncFailure(t *testing.T) {
	r := NewReader(t.TempDir(), DefaultBlockSize, nil)
	defer r.Stop()
	if err := r.SetMappingSnapshotPath(filepath.Join(t.TempDir(), "maps.gob")); err != nil {
		t.Fatal(err)
	}
	flushTestPair(r, "3_0", 11, 4096)
	want := errors.New("injected directory sync failure")
	called := false
	r.testSyncMappingDirectory = func(directory *os.File) error {
		called = true
		if directory == nil {
			t.Fatal("directory descriptor missing")
		}
		return want
	}
	if err := r.FlushMappingSnapshot(); !errors.Is(err, want) || !called {
		t.Fatalf("directory sync failure not propagated: err=%v called=%t", err, called)
	}
	r.testSyncMappingDirectory = nil
	if err := r.FlushMappingSnapshot(); err != nil {
		t.Fatalf("retry after directory sync failure: %v", err)
	}
}
