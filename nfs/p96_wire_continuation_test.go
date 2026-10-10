//go:build latency_acceptance

package nfs

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	nfslib "github.com/lelanddutcher/juicemount/internal/nfs"
	"github.com/lelanddutcher/juicemount/metadata"
)

// Both requests traverse the actual NFS WRITE handler. This file compiles on
// the original candidate; its intended old-source failure is a real JUKEBOX
// response to the second request, not a missing candidate-only symbol.
func TestP96WireContinuationAdmission(t *testing.T) {
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "10000")
	t.Setenv("JM_DRAIN_BATCH_INSERT", "0")
	tmpRoot := os.Getenv("TMPDIR")
	if !filepath.IsAbs(tmpRoot) {
		t.Fatal("isolated absolute TMPDIR required")
	}
	base, err := os.MkdirTemp(tmpRoot, "jm-p96-wire-")
	if err != nil {
		t.Fatal(err)
	}
	fullCleanupRegistered := false
	t.Cleanup(func() {
		if !fullCleanupRegistered { // no handler owns this failed setup
			if err := os.RemoveAll(base); err != nil {
				t.Errorf("remove unused fixture: %v", err)
			}
		}
	})
	backing := filepath.Join(base, "backing")
	if err := os.Mkdir(backing, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(base, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, backing)
	type rpcResult struct {
		status uint32
		err    error
	}
	type rpcCall struct {
		done   chan struct{}
		result rpcResult
	}
	var calls []*rpcCall
	var releaseConn interface{ Close() error }
	var releaseOnce sync.Once
	releaseSQL := func() {
		releaseOnce.Do(func() {
			if releaseConn != nil {
				if err := releaseConn.Close(); err != nil {
					t.Errorf("release SQL: %v", err)
				}
			}
		})
	}
	var listener net.Listener
	var server *nfslib.Server
	var serveDone chan error
	var spool *SpoolStore
	spoolOwnedByHandler := false
	// Every original client, server and handler is joined before closing SQLite.
	// A timeout is an error, never permission to claim cleanup or close its store.
	t.Cleanup(func() {
		retained := true
		defer func() {
			if retained {
				t.Logf("retained unresolved P96 wire fixture: %s", base)
			}
		}()
		releaseSQL()
		joined := true
		joinDeadline := time.Now().Add(12 * time.Second)
		for _, c := range calls {
			remaining := time.Until(joinDeadline)
			if remaining < 0 {
				remaining = 0
			}
			select {
			case <-c.done:
			case <-time.After(remaining):
				t.Error("original RPC client not joined")
				joined = false
			}
		}
		if listener != nil {
			if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("listener close: %v", err)
			}
		}
		if server != nil {
			if !server.Quiesce(5 * time.Second) {
				t.Error("RPC handlers not joined")
				joined = false
			}
			select {
			case err := <-serveDone:
				if err != nil && !errors.Is(err, net.ErrClosed) {
					t.Errorf("Serve: %v", err)
				}
			case <-time.After(time.Second):
				t.Error("Serve not joined")
				joined = false
			}
		}
		if !joined {
			return
		}
		if !h.StopHandler() {
			t.Error("handler ownership not joined")
			return
		}
		if spool != nil && !spoolOwnedByHandler {
			spool.Stop()
		}
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
			return
		}
		if t.Failed() {
			return
		}
		if err := os.RemoveAll(base); err != nil {
			t.Errorf("remove joined fixture: %v", err)
			return
		}
		retained = false
	})
	fullCleanupRegistered = true
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err = NewSpoolStore(filepath.Join(base, "spool"), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	d, err := NewDrainer(spool, DrainerConfig{FuseRoot: backing, Workers: 1, SmallWorkers: -1})
	if err != nil {
		t.Fatal(err)
	}
	h.SetSpool(spool, d) // no dispatch Start; drain only after RPC completion
	spoolOwnedByHandler = true
	h.spoolLifecycleMu.Lock()
	stopSweep := h.spoolSweeperStop
	h.spoolSweeperStop = nil
	h.spoolLifecycleMu.Unlock()
	if stopSweep == nil {
		t.Fatal("no sweeper join handle")
	}
	stopSweep()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = store.QuiesceForLatencyAcceptance(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	jfs := &juiceFS{handler: h}
	f, err := jfs.Create("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	entry, ok := spool.LookupActive("clip.mov")
	if !ok {
		t.Fatal("Create did not leave active spool")
	}
	rowID := entry.ID()
	before, err := store.CaptureMirror("clip.mov")
	if err != nil {
		t.Fatal(err)
	}
	handle := h.ToHandle(jfs, []string{"clip.mov"})
	rawInode, err := h.export.inode(handle)
	if err != nil {
		t.Fatal(err)
	}
	resolved, parts, err := h.FromHandle(handle)
	if err != nil || resolved == nil || strings.Join(parts, "/") != "clip.mov" {
		t.Fatalf("FromHandle eligibility: %T %v %v", resolved, parts, err)
	}
	snapshot := store.LookupByInode(rawInode)
	if snapshot == nil || snapshot.Inode != rawInode || rawInode != entry.Inode() || before.Incarnation == "" || before.Revision <= 0 || snapshot.Version.Incarnation != before.Incarnation || before.StoreID != "" {
		t.Fatalf("Create/ToHandle/FromHandle does not qualify: raw=%d entry=%d token=%+v snapshot=%+v", rawInode, entry.Inode(), before, snapshot)
	}
	queued := make(chan struct{})
	var queuedOnce sync.Once
	h.namespace.phase = func(phase, name string) {
		if phase == "continuation-queued" && name == "clip.mov" {
			queuedOnce.Do(func() { close(queued) })
		}
	}
	listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server = nfslib.NewManagedServer(h)
	serveDone = make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	address := listener.Addr().String()
	launch := func(offset uint64, body string) *rpcCall {
		c := &rpcCall{done: make(chan struct{})}
		calls = append(calls, c)
		go func() {
			c.result.status, c.result.err = bindingRPCStatus(address, nfslib.NFSProcedureWrite, bindingWriteArgs{Handle: handle, Offset: offset, Count: uint32(len(body)), Data: []byte(body)})
			close(c.done)
		}()
		return c
	}
	db := store.DB()
	db.SetMaxOpenConns(1)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	conn, err := db.Conn(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	releaseConn = conn
	waits := db.Stats().WaitCount
	first := launch(0, "head")
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.namespace.mu.Lock()
		active := len(h.namespace.active)
		exact := false
		failed := false
		for op, names := range h.namespace.active {
			failed = failed || op.failed
			exact = exact || (len(names) == 1 && names[0] == "clip.mov")
		}
		stopping := h.namespace.stopping
		h.namespace.mu.Unlock()
		if failed || stopping || active > 1 {
			t.Fatal("ambiguous first-opener namespace witness")
		}
		if active == 1 && exact && db.Stats().WaitCount > waits {
			break
		}
		select {
		case <-first.done:
			t.Fatalf("first RPC returned before SQL barrier: %+v", first.result)
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("first RPC did not own exact namespace while blocked on SQL")
		}
	}
	second := launch(4, "tail")
	select {
	case <-queued:
	case <-second.done:
		t.Fatalf("second WRITE returned before queue/release: status=%d err=%v; want queued continuation, old-source immediate JUKEBOX is the intended regression", second.result.status, second.result.err)
	case <-time.After(time.Second):
		t.Fatal("second WRITE never witnessed in continuation queue; possible hidden lookup/lock wait")
	}
	select {
	case <-first.done:
		t.Fatalf("first RPC completed while SQL held: %+v", first.result)
	default:
	}
	select {
	case <-second.done:
		t.Fatalf("queued RPC completed while SQL held: %+v", second.result)
	default:
	}
	releaseSQL()
	for i, c := range calls {
		select {
		case <-c.done:
			if c.result.err != nil || c.result.status != 0 {
				t.Fatalf("WRITE%d: %+v", i, c.result)
			}
		case <-time.After(4 * time.Second):
			t.Fatalf("WRITE%d did not finish", i)
		}
	}
	if current, ok := spool.LookupActive("clip.mov"); !ok || current != entry || current.ID() != rowID {
		t.Fatal("RPCs changed original spool cohort")
	}
	body, err := os.ReadFile(entry.SpoolFilePath())
	if err != nil || string(body) != "headtail" || entry.WrittenEnd() != 8 {
		t.Fatalf("spool bytes=%q length=%d err=%v", body, entry.WrittenEnd(), err)
	}
	after, err := store.CaptureMirror("clip.mov")
	if err != nil || after.Incarnation != before.Incarnation {
		t.Fatalf("mirror changed logical identity: %+v %v", after, err)
	}
	if err := entry.Finalize(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	n := d.DrainOnceForTest(ctx)
	cancel()
	if n != 1 {
		t.Fatalf("controlled production drain count=%d", n)
	}
	body, err = os.ReadFile(filepath.Join(backing, "clip.mov"))
	if err != nil || string(body) != "headtail" {
		t.Fatalf("drained bytes=%q err=%v", body, err)
	}
	row, err := spool.Meta().Get(rowID)
	if err != nil || row == nil || row.DrainState != metadata.DrainDone || row.Size != 8 {
		t.Fatalf("drained row=%+v err=%v", row, err)
	}
	final := store.LookupByPath("clip.mov")
	if final == nil || final.Size != 8 || final.Inode != rawInode || final.Version.Incarnation != before.Incarnation {
		t.Fatalf("postdrain exact identity/size: %+v", final)
	}
	t.Log("two real WRITE RPCs queued under one SQL-held opener and both acknowledged after release; exact headtail spool and drained bytes; original incarnation/inode/row retained; namespace/handler/server joins checked by cleanup")
}
