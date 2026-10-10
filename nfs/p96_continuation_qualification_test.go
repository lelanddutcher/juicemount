//go:build latency_acceptance

package nfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/cache/pin"
	"github.com/lelanddutcher/juicemount/metadata"
)

type p96Qualification struct {
	t       *testing.T
	h       *JuiceMountHandler
	store   *metadata.Store
	spool   *SpoolStore
	jfs     *juiceFS
	path    string
	holder  func()
	calls   []*p96OpenCall
	sqlConn interface{ Close() error }
	sqlOnce sync.Once
}

type p96OpenCall struct {
	done chan struct{}
	err  error
}

func newP96Qualification(t *testing.T, path string) *p96Qualification {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_DRAIN_BATCH_INSERT", "0")
	base := t.TempDir()
	backing := filepath.Join(base, "backing")
	if err := os.Mkdir(backing, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(base, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	q := &p96Qualification{t: t, store: store, h: NewHandler(store, backing), path: path}
	q.jfs = &juiceFS{handler: q.h}
	t.Cleanup(func() {
		q.releaseSQL()
		q.releaseHolder()
		joined := true
		for i, call := range q.calls {
			select {
			case <-call.done:
			case <-time.After(5 * time.Second):
				t.Errorf("opener %d not joined", i)
				joined = false
			}
		}
		if !joined {
			return
		}
		if !q.h.StopHandler() {
			t.Error("handler/namespace/sweeper not joined; retaining SQLite")
			return
		}
		if err := q.store.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(filepath.Join(base, "spool"), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	q.spool = spool
	q.h.SetSpool(spool, nil) // no drainer dispatch is born
	q.h.spoolLifecycleMu.Lock()
	stopSweep := q.h.spoolSweeperStop
	q.h.spoolSweeperStop = nil
	q.h.spoolLifecycleMu.Unlock()
	if stopSweep == nil {
		t.Fatal("missing owned sweeper stop")
	}
	stopSweep() // synchronous original sweeper join before any SQL hold
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = store.QuiesceForLatencyAcceptance(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	if parent := filepath.Dir(path); parent != "." {
		if err := q.jfs.MkdirAll(parent, 0755); err != nil {
			t.Fatal(err)
		}
	}
	q.create(path, "old")
	return q
}

func (q *p96Qualification) releaseHolder() {
	if q.holder != nil {
		q.holder()
		q.holder = nil
	}
}
func (q *p96Qualification) releaseSQL() {
	q.sqlOnce.Do(func() {
		if q.sqlConn != nil {
			if err := q.sqlConn.Close(); err != nil {
				q.t.Errorf("SQL release: %v", err)
			}
		}
	})
}
func (q *p96Qualification) create(path, body string) {
	q.t.Helper()
	f, err := q.jfs.Create(path)
	if err != nil {
		q.t.Fatal(err)
	}
	if body != "" {
		w, ok := f.(io.WriterAt)
		if !ok {
			_ = f.Close()
			q.t.Fatalf("Create file has no WriteAt: %T", f)
		}
		if n, err := w.WriteAt([]byte(body), 0); err != nil || n != len(body) {
			_ = f.Close()
			q.t.Fatalf("seed=(%d,%v)", n, err)
		}
	}
	if err := f.Close(); err != nil {
		q.t.Fatal(err)
	}
}
func (q *p96Qualification) resolved() (*continuationFS, []byte) {
	q.t.Helper()
	handle := q.h.ToHandle(q.jfs, splitPath(q.path))
	fs, parts, err := q.h.FromHandle(handle)
	if err != nil || strings.Join(parts, "/") != q.path {
		q.t.Fatalf("FromHandle=(%T,%v,%v)", fs, parts, err)
	}
	bound, ok := fs.(*continuationFS)
	if !ok || bound.entry == nil || bound.rowID <= 0 || bound.incarnation == "" {
		q.t.Fatalf("active handle did not qualify: %T", fs)
	}
	return bound, handle
}
func (q *p96Qualification) queue(bound *continuationFS) *p96OpenCall {
	q.t.Helper()
	holder, err := q.h.continuations.acquire(q.path, time.Now().Add(8*time.Second), nil)
	if err != nil {
		q.t.Fatal(err)
	}
	q.holder = holder
	queued := make(chan struct{})
	var once sync.Once
	q.h.namespace.phase = func(phase, name string) {
		if phase == "continuation-queued" && name == q.path {
			once.Do(func() { close(queued) })
		}
	}
	call := &p96OpenCall{done: make(chan struct{})}
	q.calls = append(q.calls, call)
	go func() {
		f, err := bound.OpenFile(q.path, os.O_RDWR, 0)
		if f != nil {
			if closeErr := f.Close(); err == nil {
				err = closeErr
			}
		}
		call.err = err
		close(call.done)
	}()
	select {
	case <-queued:
	case <-call.done:
		q.t.Fatalf("opener returned before queue: %v", call.err)
	case <-time.After(time.Second):
		q.t.Fatal("opener did not reach exact continuation queue")
	}
	select {
	case <-call.done:
		q.t.Fatalf("queued opener returned before release: %v", call.err)
	default:
	}
	return call
}
func (q *p96Qualification) await(call *p96OpenCall, wantError error) {
	q.t.Helper()
	q.releaseHolder()
	select {
	case <-call.done:
		err := call.err
		if wantError == nil && err != nil {
			q.t.Fatalf("same-identity continuation refused: %v", err)
		}
		if wantError != nil && !errors.Is(err, wantError) {
			q.t.Fatalf("old-handle identity refusal=%v, want %v", err, wantError)
		}
	case <-time.After(3 * time.Second):
		q.t.Fatal("queued opener did not join after release")
	}
}
func p96SpoolBody(t *testing.T, e *SpoolEntry) string {
	t.Helper()
	b, err := os.ReadFile(e.SpoolFilePath())
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestP96QueuedOriginalIdentity(t *testing.T) {
	t.Run("same_path_replacement", func(t *testing.T) {
		q := newP96Qualification(t, "clip.mov")
		old, _ := q.resolved()
		call := q.queue(old)
		if err := q.jfs.Remove(q.path); err != nil {
			t.Fatal(err)
		}
		q.create(q.path, "new")
		next, ok := q.spool.LookupActive(q.path)
		if !ok || next == old.entry || next.ID() == old.rowID {
			t.Fatal("real replacement did not rotate cohort")
		}
		token, err := q.store.CaptureMirror(q.path)
		if err != nil || token.Incarnation == old.incarnation {
			t.Fatalf("mirror replacement=(%+v,%v)", token, err)
		}
		q.await(call, metadata.ErrMirrorConflict)
		if got := p96SpoolBody(t, next); got != "new" {
			t.Fatalf("successor bytes %q", got)
		}
		now, err := q.store.CaptureMirror(q.path)
		if err != nil || now != token {
			t.Fatalf("successor mirror changed=(%+v,%v)", now, err)
		}
	})
	t.Run("parent_rename_replacement", func(t *testing.T) {
		q := newP96Qualification(t, "parent/clip.mov")
		old, _ := q.resolved()
		call := q.queue(old)
		if err := q.jfs.Rename("parent", "moved"); err != nil {
			t.Fatalf("real parent rename: %v", err)
		}
		if err := q.jfs.MkdirAll("parent", 0755); err != nil {
			t.Fatal(err)
		}
		q.create(q.path, "new")
		next, ok := q.spool.LookupActive(q.path)
		if !ok || next == old.entry {
			t.Fatal("parent replacement missing distinct cohort")
		}
		moved, ok := q.spool.LookupActive("moved/clip.mov")
		if !ok || moved != old.entry {
			t.Fatal("original cohort not under renamed parent")
		}
		token, err := q.store.CaptureMirror(q.path)
		if err != nil || token.Incarnation == old.incarnation {
			t.Fatalf("replacement token=(%+v,%v)", token, err)
		}
		q.await(call, metadata.ErrMirrorConflict)
		if got := p96SpoolBody(t, next); got != "new" {
			t.Fatalf("successor bytes %q", got)
		}
		if got := p96SpoolBody(t, moved); got != "old" {
			t.Fatalf("renamed original bytes %q", got)
		}
	})
	t.Run("same_incarnation_new_spool_row", func(t *testing.T) {
		q := newP96Qualification(t, "clip.mov")
		old, _ := q.resolved()
		originalToken, err := q.store.CaptureMirror(q.path)
		if err != nil {
			t.Fatal(err)
		}
		call := q.queue(old)
		if changed, err := q.spool.cancelForDelete(q.path); err != nil || !changed {
			t.Fatalf("cancel old cohort=(%v,%v)", changed, err)
		}
		successor, err := q.spool.OpenWrite(q.path)
		if err != nil {
			t.Fatal(err)
		}
		successor.SetInode(old.inode)
		if n, err := successor.WriteAt([]byte("new"), 0); err != nil || n != 3 {
			t.Fatalf("successor seed=(%d,%v)", n, err)
		}
		successor.ReleaseHandle()
		if successor == old.entry || successor.ID() == old.rowID {
			t.Fatal("spool row did not rotate")
		}
		token, err := q.store.CaptureMirror(q.path)
		if err != nil || token.Incarnation != originalToken.Incarnation {
			t.Fatalf("mirror changed with spool-only rotation=(%+v,%v)", token, err)
		}
		q.await(call, ErrSpoolBusy)
		if got := p96SpoolBody(t, successor); got != "new" {
			t.Fatalf("successor bytes %q", got)
		}
		row, err := q.spool.Meta().Get(successor.ID())
		if err != nil || row == nil || row.DrainState != metadata.DrainWriting {
			t.Fatalf("successor row=(%+v,%v)", row, err)
		}
	})
}

func TestP96QueuedRevisionAdvanceKeepsOriginal(t *testing.T) {
	q := newP96Qualification(t, "clip.mov")
	old, _ := q.resolved()
	call := q.queue(old)
	before, err := q.store.CaptureMirror(q.path)
	if err != nil {
		t.Fatal(err)
	}
	e := q.store.LookupByPath(q.path)
	if e == nil {
		t.Fatal("missing original entry")
	}
	e.Size++
	e.Mtime = time.Now()
	published, err := q.store.PublishExpected(before, e)
	if err != nil {
		t.Fatal(err)
	}
	if published.Version.Incarnation != before.Incarnation || published.Version.Revision <= before.Revision {
		t.Fatalf("not ordinary same-incarnation revision: before=%+v after=%+v", before, published.Version)
	}
	q.await(call, nil)
	current, ok := q.spool.LookupActive(q.path)
	if !ok || current != old.entry || current.ID() != old.rowID {
		t.Fatal("successful continuation changed cohort")
	}
}

func TestP96QualifiedFallbackZeroTokenAndAlias(t *testing.T) {
	q := newP96Qualification(t, "clip.mov")
	const legacy = "legacy.mov"
	if err := os.WriteFile(filepath.Join(q.h.fusePath, legacy), []byte("legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	const synthetic = uint64(1<<63 | 0x953)
	cacheOnly := metadata.MakeEntry(legacy, false, 6, time.Now(), synthetic)
	q.store.InsertToCache(cacheOnly)
	handle := q.h.ToHandle(q.jfs, []string{legacy})
	fs, parts, err := q.h.FromHandle(handle)
	if err != nil || strings.Join(parts, "/") != legacy {
		t.Fatalf("zero-token handle=(%T,%v,%v)", fs, parts, err)
	}
	if _, wrapped := fs.(*continuationFS); wrapped {
		t.Fatal("zero-token handle queued")
	}
	f, err := fs.OpenFile(legacy, os.O_RDWR, 0)
	if f != nil {
		_ = f.Close()
	}
	if !errors.Is(err, metadata.ErrMirrorConflict) {
		t.Fatalf("zero-token prior fail-closed result=%v", err)
	}
	if body, err := os.ReadFile(filepath.Join(q.h.fusePath, legacy)); err != nil || string(body) != "legacy" {
		t.Fatalf("zero-token changed backing bytes=%q err=%v", body, err)
	}
	real := metadata.MakeEntry(legacy, false, 6, time.Now(), synthetic+1)
	if err := q.store.Insert(real); err != nil {
		t.Fatal(err)
	}
	alias, aliasParts, err := q.h.FromHandle(handle)
	if err != nil || strings.Join(aliasParts, "/") != legacy {
		t.Fatalf("synthetic alias fallback=(%T,%v,%v)", alias, aliasParts, err)
	}
	if _, wrapped := alias.(*continuationFS); wrapped {
		t.Fatal("synthetic alias queued")
	}
}

func TestP96QualifiedFallbackDeletedTombstone(t *testing.T) {
	q := newP96Qualification(t, "clip.mov")
	oldOffline := pin.IsOffline()
	pin.SetOffline(true)
	t.Cleanup(func() { pin.SetOffline(oldOffline) })
	const deleted = "._finder"
	const inode = uint64(1<<63 | 0x9f6)
	q.store.InsertToCache(metadata.MakeEntry(deleted, false, 4096, time.Now(), inode))
	handle := q.h.ToHandle(q.jfs, []string{deleted})
	if err := q.jfs.Remove(deleted); err != nil {
		t.Fatal(err)
	}
	fs, parts, err := q.h.FromHandle(handle)
	if err != nil || strings.Join(parts, "/") != deleted {
		t.Fatalf("deleted FromHandle=(%T,%v,%v)", fs, parts, err)
	}
	if _, ok := fs.(*deletedHandleFS); !ok {
		t.Fatalf("deleted handle no longer uses tombstone: %T", fs)
	}
	f, err := fs.OpenFile(deleted, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("late tombstone open: %v", err)
	}
	w, ok := f.(io.WriterAt)
	if !ok {
		_ = f.Close()
		t.Fatalf("late tombstone writer: %T", f)
	}
	if n, err := w.WriteAt([]byte("meta"), 0); err != nil || n != 4 {
		_ = f.Close()
		t.Fatalf("late tombstone write=(%d,%v)", n, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if e := q.store.LookupByPath(deleted); e != nil {
		t.Fatalf("tombstone resurrected: %+v", e)
	}
}

func TestP96ExpiredQueuedWrapperNeverStarts(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "500")
	q := newP96Qualification(t, "clip.mov")
	old, _ := q.resolved()
	old.entry.mu.RLock()
	refs := old.entry.refcount
	old.entry.mu.RUnlock()
	call := q.queue(old)
	select {
	case <-call.done:
		if !errors.Is(call.err, errFUSETimeout) {
			t.Fatalf("queued expiry=%v", call.err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued opener did not expire")
	}
	q.releaseHolder()
	if !q.h.continuations.stop(time.Second) {
		t.Fatal("expired waiter retained gate ownership")
	}
	q.h.namespace.mu.Lock()
	active := len(q.h.namespace.active)
	q.h.namespace.mu.Unlock()
	old.entry.mu.RLock()
	refsAfter := old.entry.refcount
	old.entry.mu.RUnlock()
	if active != 0 || refsAfter != refs {
		t.Fatalf("expired opener started late: namespace=%d refs=%d->%d", active, refs, refsAfter)
	}
}

func TestP96WrapperSharedGateAndWorkerDeadline(t *testing.T) {
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "1000")
	q := newP96Qualification(t, "clip.mov")
	old, _ := q.resolved()
	holder, err := q.h.continuations.acquire(q.path, time.Now().Add(3*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	q.holder = holder
	queued := make(chan struct{})
	var once sync.Once
	q.h.namespace.phase = func(phase, name string) {
		if phase == "continuation-queued" && name == q.path {
			once.Do(func() { close(queued) })
		}
	}
	call := &p96OpenCall{done: make(chan struct{})}
	q.calls = append(q.calls, call)
	started := time.Now()
	go func() {
		f, err := old.OpenFile(q.path, os.O_RDWR, 0)
		if f != nil {
			_ = f.Close()
		}
		call.err = err
		close(call.done)
	}()
	select {
	case <-queued:
	case <-time.After(time.Second):
		t.Fatal("wrapper did not queue")
	}
	db := q.store.DB()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	conn, err := db.Conn(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	q.sqlConn = conn
	// Deliberately consume most of the one-second budget in the real gate.
	gateDelay := time.NewTimer(700 * time.Millisecond)
	select {
	case <-gateDelay.C:
	case <-call.done:
		t.Fatalf("queued caller returned before gate release: %v", call.err)
	}
	gateDelay.Stop()
	waits := db.Stats().WaitCount
	q.releaseHolder()
	witnessDeadline := time.NewTimer(220 * time.Millisecond)
	defer witnessDeadline.Stop()
	for {
		q.h.namespace.mu.Lock()
		active := len(q.h.namespace.active)
		q.h.namespace.mu.Unlock()
		if active == 1 && db.Stats().WaitCount > waits {
			break
		}
		select {
		case <-call.done:
			t.Fatal("wrapper returned before SQL worker witness")
		case <-time.After(time.Millisecond):
		case <-witnessDeadline.C:
			t.Fatal("worker did not enter SQL hold within remaining budget")
		}
	}
	select {
	case <-call.done:
		err := call.err
		if !errors.Is(err, errFUSETimeout) {
			t.Fatalf("shared deadline result=%v", err)
		}
		if elapsed := time.Since(started); elapsed > 1350*time.Millisecond {
			t.Fatalf("gate+worker got a fresh budget: %s", elapsed)
		}
	case <-time.After(1350*time.Millisecond - time.Since(started)):
		t.Fatal("wrapper exceeded original gate+worker deadline")
	}
	// A timed-out worker still owns the namespace until its original SQL hold is released.
	if op := q.h.namespace.begin([]string{q.path}); op != nil {
		op.finish(nil)
		t.Fatal("timed-out worker lost namespace ownership")
	}
	q.releaseSQL()
	if !q.h.namespace.stop(3 * time.Second) {
		t.Fatal("original worker not joined after SQL release")
	}
}

// compile-time reminder: the tested wrapper is an ordinary billy Filesystem.
var _ billy.Filesystem = (*continuationFS)(nil)
