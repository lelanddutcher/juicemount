//go:build latency_acceptance

package nfs

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/metrics"
	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

// A real background batch waits on the local SQL pool while the production
// NFS handler serves known metadata over TCP. This isolates the default RAM
// serving path; an intentionally exhausted SQLite reader pool is a different
// condition from writer-lock coupling and is not claimed to be available.
func TestNavigationSQLContention(t *testing.T) {
	for _, operation := range []string{"bulk", "scan-clear", "scan-delete", "rename", "publish-expected", "confirm-directory", "prepare-create", "abort-create", "complete-create", "begin-online", "complete-online"} {
		t.Run(operation, func(t *testing.T) { navigationSQLContention(t, operation) })
	}
}

func navigationSQLContention(t *testing.T, operation string) {
	// The pool wait must belong to the target batch. Disable FTS and join
	// prune-ladder GC below before holding SQL and recording the baseline.
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_READDIR_PAGINATED", "0")
	if metadata.ServeFromSQLite() {
		t.Fatal("SQL contention fixture requires JM_SERVE_FROM_SQLITE=0")
	}
	jfs, store, root := newDirRefreshHarness(t)
	stamp := time.Unix(1700000000, 0)
	entries := make([]*metadata.Entry, 0, 101)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("item-%03d.bin", i)
		if err := os.WriteFile(filepath.Join(root, name), make([]byte, i+1), 0o644); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, metadata.MakeEntry(name, false, int64(i+1), stamp, uint64(1001+i)))
	}
	if err := os.WriteFile(filepath.Join(root, "slow.bin"), []byte(latencyPayload), 0o644); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, metadata.MakeEntry("slow.bin", false, int64(len(latencyPayload)), stamp, 2001))
	if err := store.BulkInsert(entries, 101); err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", bindingRPCAddress(t, jfs.handler), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	client := newLatencyClient(conn)
	rootHandle := jfs.handler.ToHandle(jfs, nil)
	childHandle := jfs.handler.ToHandle(jfs, []string{"item-050.bin"})

	// This row is outside the listed directory, so completing a blocked scan
	// cannot change the navigation payload that the oracle verifies.
	background := metadata.MakeEntry("unrelated/old.mov", false, 42, stamp, 9001)
	background.LocalOnly = true
	if err := store.Insert(background); err != nil {
		t.Fatal(err)
	}
	epoch, err := store.CaptureStoreEpoch()
	if err != nil {
		t.Fatal(err)
	}
	source, destination, err := store.CaptureRename(background.Path, "unrelated/moved.mov")
	if err != nil {
		t.Fatal(err)
	}
	publishInput := metadata.MakeEntry("unrelated/published.mov", false, 84, stamp, 9002)
	publishExpected, err := store.CaptureMirror(publishInput.Path)
	if err != nil {
		t.Fatal(err)
	}
	directoryInput := metadata.MakeEntry("unrelated/confirmed", true, 0, stamp, 9003)
	directoryExpected, directoryEpoch, err := store.CaptureDirectoryForFUSE(directoryInput.Path)
	if err != nil {
		t.Fatal(err)
	}
	var createExpected metadata.MirrorToken
	var prepared metadata.PreparedCreate
	var onlineExpected metadata.MirrorToken
	mutation := metadata.OnlineMutation{Kind: "truncate", Path: background.Path, BackingIdentity: "dev=7,inode=9001", MountGeneration: "mount-test", OldEOF: 42, TargetEOF: 84}
	createInput := metadata.MakeEntry("unrelated/parent/created.mov", false, 42, stamp, 9005)
	if operation == "prepare-create" || operation == "abort-create" || operation == "complete-create" {
		if err := store.Insert(metadata.MakeEntry("unrelated/parent", true, 0, stamp, 9004)); err != nil {
			t.Fatal(err)
		}
		createExpected, err = store.CaptureMirror(createInput.Path)
		if err != nil {
			t.Fatal(err)
		}
		if operation != "prepare-create" {
			prepared, err = store.PrepareCreate(createExpected, "mount-test")
		}
	} else if operation == "begin-online" || operation == "complete-online" {
		onlineExpected, err = store.CaptureMirror(background.Path)
		if err != nil {
			t.Fatal(err)
		}
		if operation == "complete-online" {
			mutation, err = store.BeginOnlineMutation(onlineExpected, mutation)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.QuiesceForLatencyAcceptance(ctx); err != nil {
		t.Fatal(err)
	}
	store.DB().SetMaxOpenConns(1)
	held, err := store.DB().Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var releaseOnce sync.Once
	var releasedAt time.Time
	release := func() {
		releaseOnce.Do(func() {
			releasedAt = time.Now()
			_ = held.Close()
		})
	}
	defer release()
	waits := store.DB().Stats().WaitCount
	writerDone := make(chan error, 1)
	go func() {
		var err error
		switch operation {
		case "bulk":
			err = store.BulkInsert([]*metadata.Entry{metadata.MakeEntry("unrelated/new.mov", false, 1, stamp, 9000)}, 500)
		case "scan-clear":
			_, err = store.ScanClearLocalOnlyBatch([]string{background.Path}, epoch)
		case "scan-delete":
			_, err = store.ScanDeletePathsBatch([]string{background.Path}, epoch)
		case "rename":
			_, err = store.RenameExpected(source, destination)
		case "publish-expected":
			_, err = store.PublishExpected(publishExpected, publishInput)
		case "confirm-directory":
			_, err = store.ConfirmAbsentDirectoryFromFUSE(directoryExpected, directoryEpoch, directoryInput)
		case "prepare-create":
			_, err = store.PrepareCreate(createExpected, "mount-test")
		case "abort-create":
			err = store.AbortPreparedCreate(prepared)
		case "complete-create":
			_, err = store.CompletePreparedCreate(prepared, createInput)
		case "begin-online":
			_, err = store.BeginOnlineMutation(onlineExpected, mutation)
		case "complete-online":
			_, err = store.CompleteOnlineMutation(mutation, stamp.Add(time.Second))
		default:
			panic("unknown SQL fixture operation")
		}
		writerDone <- err
	}()
	defer func() {
		release()
		select {
		case err := <-writerDone:
			if err != nil {
				t.Errorf("background batch: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("background batch did not settle after release")
		}
	}()
	deadline := time.Now().Add(time.Second)
	for store.DB().Stats().WaitCount == waits && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.DB().Stats().WaitCount == waits {
		t.Fatal("background batch did not reach the held SQL pool")
	}
	foregroundBefore := metrics.Default().Snapshot().FUSEAttrib.BySource["foreground"].Calls
	var calls []latencyNavCall
	for _, spec := range []struct {
		kind string
		proc nfslib.NFSProcedure
		args any
	}{
		{"getattr", nfslib.NFSProcedureGetAttr, childHandle},
		{"lookup", nfslib.NFSProcedureLookup, nfslib.DirOpArg{Handle: rootHandle, Filename: []byte("item-050.bin")}},
		{"readdirplus", nfslib.NFSProcedureReadDirPlus, latencyDirArgs{Handle: rootHandle, DirCount: 65536, MaxCount: 65536}},
	} {
		p, err := client.send(100003, uint32(spec.proc), spec.args)
		if err != nil {
			t.Fatal(err)
		}
		calls = append(calls, latencyNavCall{spec.kind, p})
	}
	deadline = time.Now().Add(250 * time.Millisecond)
	completedBeforeRelease := 0
	for _, call := range calls {
		r, err := call.p.await(deadline)
		if err != nil {
			t.Errorf("cached %s did not finish while unrelated SQL was held: %v", call.kind, err)
			release()
			r, err = call.p.await(time.Now().Add(3 * time.Second))
		}
		if err != nil {
			t.Errorf("%s did not settle after release: %v", call.kind, err)
			continue
		}
		if releasedAt.IsZero() || r.at.Before(releasedAt) {
			completedBeforeRelease++
		}
		count, err := latencyCheckNav(call.kind, r, childHandle)
		if err != nil {
			t.Errorf("%s payload: %v", call.kind, err)
		}
		elapsed := r.at.Sub(call.p.sent)
		t.Logf("%s latency=%s listed_children=%d", call.kind, elapsed, count)
		if elapsed > 100*time.Millisecond {
			t.Errorf("%s latency %s exceeds 100ms budget", call.kind, elapsed)
		}
	}
	if completedBeforeRelease != len(calls) {
		t.Errorf("navigation completed before SQL release: %d/%d", completedBeforeRelease, len(calls))
	}
	foregroundAfter := metrics.Default().Snapshot().FUSEAttrib.BySource["foreground"].Calls
	if foregroundAfter != foregroundBefore {
		t.Errorf("cached navigation invoked foreground FUSE operations: before=%d after=%d", foregroundBefore, foregroundAfter)
	}
	t.Logf("foreground FUSE calls during cached navigation: %d", foregroundAfter-foregroundBefore)
}
