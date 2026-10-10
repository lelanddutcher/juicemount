//go:build latency_acceptance

package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/lelanddutcher/juicemount/internal/metrics"
	"github.com/lelanddutcher/juicemount/metadata"
)

type p96MetadataFixture struct {
	h              *JuiceMountHandler
	store          *metadata.Store
	handles        map[string][]byte
	externalJoined *bool
}

func newP96MetadataFixture(t testing.TB) p96MetadataFixture {
	t.Helper()
	t.Setenv("JM_FTS_DEFER", "0")
	t.Setenv("JM_MUTATION_OP_TIMEOUT_MS", "5000")
	base := t.TempDir()
	backing := filepath.Join(base, "backing")
	if err := os.Mkdir(backing, 0700); err != nil {
		t.Fatal(err)
	}
	store, err := metadata.Open(filepath.Join(base, "mirror.db"))
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(store, backing)
	externalJoined := true
	t.Cleanup(func() {
		if !externalJoined {
			t.Error("metadata fixture retains store for unresolved external caller")
			return
		}
		if !h.StopHandler() {
			t.Error("metadata fixture handler did not join")
			return
		}
		if err := store.Close(); err != nil {
			t.Errorf("metadata fixture Close: %v", err)
		}
	})
	if err := metadata.InitSpoolSchema(store.DB()); err != nil {
		t.Fatal(err)
	}
	spool, err := NewSpoolStore(filepath.Join(base, "spool"), 0, metadata.NewSpoolStore(store.DB()))
	if err != nil {
		t.Fatal(err)
	}
	h.SetSpool(spool, nil)
	h.spoolLifecycleMu.Lock()
	stop := h.spoolSweeperStop
	h.spoolSweeperStop = nil
	h.spoolLifecycleMu.Unlock()
	if stop == nil {
		t.Fatal("sweeper owner missing")
	}
	stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = store.QuiesceForLatencyAcceptance(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	fs := &juiceFS{handler: h}
	f, err := fs.Create("active.mov")
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"resident.mov", "folder"} {
		if err := store.Insert(metadata.MakeEntry(name, i == 1, 0, time.Unix(1700000000, 0), uint64(8100+i))); err != nil {
			t.Fatal(err)
		}
	}
	handles := make(map[string][]byte)
	for _, name := range []string{"active.mov", "resident.mov", "folder"} {
		handles[name] = h.ToHandle(fs, []string{name})
		if len(handles[name]) == 0 {
			t.Fatal("empty handle", name)
		}
	}
	return p96MetadataFixture{h: h, store: store, handles: handles, externalJoined: &externalJoined}
}

// This is the production onGetAttr resolution path before XDR serialization.
// The names come from real durable/cache entries, and every result is consumed.
func p96CachedMetadata(h *JuiceMountHandler, handle []byte, name string) error {
	fs, parts, err := h.FromHandle(handle)
	if err != nil {
		return err
	}
	info, err := fs.Lstat(fs.Join(parts...))
	if err != nil {
		return err
	}
	if info.Name() != name || info.Size() != 0 || info.IsDir() != (name == "folder") {
		return fmt.Errorf("wrong metadata for %s: %s %d %v", name, info.Name(), info.Size(), info.IsDir())
	}
	return nil
}

func p96FUSECalls() uint64 {
	var calls uint64
	for _, c := range metrics.Default().Snapshot().FUSEAttrib.Cells {
		calls += c.Calls
	}
	return calls
}

func TestP96CachedMetadataDuringSQLBlockedWrite(t *testing.T) {
	f := newP96MetadataFixture(t)
	db := f.store.DB()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	conn, err := db.Conn(ctx)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			if err := conn.Close(); err != nil {
				t.Errorf("release SQL: %v", err)
			}
		})
	}
	var writeFile billy.File
	var writeErr error
	writerDone := make(chan struct{})
	metadataDone := make(chan error, 1)
	metadataLaunched := false
	writerLaunched := false
	*f.externalJoined = false
	t.Cleanup(func() {
		release()
		joined := true
		if writerLaunched {
			select {
			case <-writerDone:
				if writeFile != nil {
					if err := closeNamespaceFile(writeFile); err != nil {
						t.Errorf("late opener Close: %v", err)
					}
				}
			case <-time.After(6 * time.Second):
				t.Error("metadata fixture original opener not joined")
				joined = false
			}
		}
		if metadataLaunched {
			select {
			case <-metadataDone:
			case <-time.After(6 * time.Second):
				t.Error("metadata fixture caller not joined")
				joined = false
			}
		}
		*f.externalJoined = joined
	})
	waits := db.Stats().WaitCount
	fs, parts, err := f.h.FromHandle(f.handles["active.mov"])
	if err != nil {
		t.Fatal(err)
	}
	writerLaunched = true
	go func() { writeFile, writeErr = fs.OpenFile(fs.Join(parts...), os.O_RDWR, 0); close(writerDone) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		f.h.namespace.mu.Lock()
		exact := false
		for _, names := range f.h.namespace.active {
			exact = exact || (len(names) == 1 && names[0] == "active.mov")
		}
		count := len(f.h.namespace.active)
		f.h.namespace.mu.Unlock()
		if exact && count == 1 && db.Stats().WaitCount > waits {
			break
		}
		select {
		case <-writerDone:
			t.Fatalf("write returned before held-SQL witness: %v", writeErr)
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("no exact blocked write witness")
		}
	}
	before := db.Stats().WaitCount
	fuseBefore := p96FUSECalls()
	metadataLaunched = true
	go func() {
		for i := 0; i < 1000; i++ {
			for _, name := range []string{"active.mov", "resident.mov", "folder"} {
				if err := p96CachedMetadata(f.h, f.handles[name], name); err != nil {
					metadataDone <- err
					return
				}
			}
		}
		metadataDone <- nil
	}()
	select {
	case err := <-metadataDone:
		metadataLaunched = false
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cached metadata blocked while writer owns SQL wait")
	}
	if got := db.Stats().WaitCount; got != before {
		t.Fatalf("cached metadata entered SQL: WaitCount %d -> %d", before, got)
	}
	select {
	case <-writerDone:
		t.Fatalf("writer unexpectedly completed before release: %v", writeErr)
	default:
	}
	if got := p96FUSECalls(); got != fuseBefore {
		t.Fatalf("cached metadata entered tracked FUSE helpers: %d -> %d", fuseBefore, got)
	}
	t.Log("3000 exact cached metadata results completed during witnessed SQL-blocked write; zero added SQL WaitCount or tracked FUSE helper calls")
	release()
	select {
	case <-writerDone:
		if writeErr != nil || writeFile == nil {
			t.Fatalf("released writer: %v", writeErr)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("writer did not complete after release")
	}
}

func BenchmarkP96CachedMetadata(b *testing.B) {
	for _, name := range []string{"active.mov", "resident.mov", "folder"} {
		b.Run(name, func(b *testing.B) {
			f := newP96MetadataFixture(b)
			handle := f.handles[name]
			if err := p96CachedMetadata(f.h, handle, name); err != nil {
				b.Fatal(err)
			}
			before := f.store.DB().Stats().WaitCount
			fuseBefore := p96FUSECalls()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p96CachedMetadata(f.h, handle, name); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if got := p96FUSECalls(); got != fuseBefore {
				b.Fatalf("benchmark tracked FUSE calls: %d -> %d", fuseBefore, got)
			}
			if got := f.store.DB().Stats().WaitCount; got != before {
				b.Fatalf("benchmark had unexpected SQL contention: %d -> %d", before, got)
			}
		})
	}
}
