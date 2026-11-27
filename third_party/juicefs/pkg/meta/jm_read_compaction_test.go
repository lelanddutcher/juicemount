package meta

// JuiceMount: read compaction can be turned off (patch 0036).

import (
	"sync/atomic"
	"testing"
	"time"
)

// fragmented writes n 4 KiB slices side by side into chunk 0 of a new file.
func jmFragmented(t *testing.T, m Meta, name string, n int) Ino {
	t.Helper()
	ctx := Background()
	var inode Ino
	if st := m.Create(ctx, 1, name, 0644, 022, 0, &inode, &Attr{}); st != 0 {
		t.Fatalf("create %s: %s", name, st)
	}
	for i := 0; i < n; i++ {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("new slice: %s", st)
		}
		if st := m.Write(ctx, inode, 0, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now()); st != 0 {
			t.Fatalf("write: %s", st)
		}
	}
	return inode
}

// With read compaction off a Read of a fragmented chunk starts no
// compaction and returns the same slices; turned back on, the next Read
// compacts as upstream does. A forced compaction (gc --compact) runs
// either way.
func TestJMReadCompactionGate(t *testing.T) {
	m := NewClient("memkv://jm-read-compaction", DefaultConf())
	if err := m.Init(&Format{Name: "rc", DirStats: true}, true); err != nil {
		t.Fatal(err)
	}
	var started atomic.Int32
	m.OnMsg(CompactChunk, func(args ...interface{}) error {
		started.Add(1)
		return nil
	})
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	g, ok := m.(JMReadCompactionGate)
	if !ok {
		t.Fatal("meta has no read compaction gate")
	}
	if !g.JMReadCompaction() {
		t.Fatal("read compaction must be on by default (upstream)")
	}
	if was := g.JMSetReadCompaction(false); !was {
		t.Fatal("set off: want was=true")
	}
	if g.JMReadCompaction() {
		t.Fatal("gate still on")
	}

	ctx := Background()
	inode := jmFragmented(t, m, "f", 8)
	for i := 0; i < 3; i++ {
		var ss []Slice
		if st := m.Read(ctx, inode, 0, &ss); st != 0 || len(ss) != 8 {
			t.Fatalf("read %d: st %s, %d slices", i, st, len(ss))
		}
	}
	time.Sleep(200 * time.Millisecond)
	if n := started.Load(); n != 0 {
		t.Fatalf("read compaction off: %d compactions started", n)
	}

	// Forced compaction (gc --compact) ignores the gate.
	m.(compactor).compactChunk(inode, 0, false, true)
	if started.Load() == 0 {
		t.Fatal("forced compaction did not run with read compaction off")
	}
	var ss []Slice
	if st := m.Read(ctx, inode, 0, &ss); st != 0 || len(ss) != 1 {
		t.Fatalf("after forced compaction: st %s, %d slices", st, len(ss))
	}

	// Back on: a Read of a fragmented chunk compacts it in the background.
	if was := g.JMSetReadCompaction(true); was {
		t.Fatal("set on: want was=false")
	}
	before := started.Load()
	inode2 := jmFragmented(t, m, "g", 8)
	var ss2 []Slice
	if st := m.Read(ctx, inode2, 0, &ss2); st != 0 {
		t.Fatalf("read g: %s", st)
	}
	deadline := time.Now().Add(5 * time.Second)
	for started.Load() == before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if started.Load() == before {
		t.Fatal("read compaction on: no compaction started")
	}
}
