/*
 * JuiceMount-owned test (patch 0025). Not part of upstream JuiceFS.
 *
 * Needs JM_WATCH_BATCH_TEST_REDIS_URL, as redis_watch_batch_test.go.
 */

package meta

import (
	"sync"

	"github.com/redis/go-redis/v9"
	"syscall"
	"testing"
	"time"
)

func jmCreateFixtureSlice(t *testing.T, m *redisMeta, n uint32) Slice {
	t.Helper()
	var id uint64
	if st := m.NewSlice(Background(), &id); st != 0 {
		t.Fatalf("new slice: %v", st)
	}
	return Slice{Id: id, Size: n, Len: n}
}

func jmCounters(t *testing.T, m *redisMeta) (used, inodes int64) {
	t.Helper()
	ctx := Background()
	used, _ = m.rdb.Get(ctx, m.usedSpaceKey()).Int64()
	inodes, _ = m.rdb.Get(ctx, m.totalInodesKey()).Int64()
	return
}

// A new file with its data, mode and mtime is one transaction: 2 round trips
// (the drainer's stage path costs about 16), and the name holds the complete
// file when it appears.
func TestJMCreateFileOneTransaction(t *testing.T) {
	m, wc := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	if _, err := m.nextInode(); err != nil { // warm the inode batch
		t.Fatal(err)
	}
	slice := jmCreateFixtureSlice(t, m, 20480)
	used0, inodes0 := jmCounters(t, m)
	mtime := time.Unix(1794490456, 123456789)

	var ino Ino
	var attr Attr
	wc.n.Store(0)
	if st := m.JMCreateFile(ctx, RootInode, "small.xmp", 0666, 022, mtime, slice, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if got := wc.n.Load(); got != 2 {
		t.Errorf("create with data took %d round trips, want 2", got)
	}
	assertNoWatches(t, m)

	var got Ino
	var a Attr
	if st := m.doLookup(ctx, RootInode, "small.xmp", &got, &a); st != 0 || got != ino {
		t.Fatalf("lookup: %d %v (want %d)", got, st, ino)
	}
	if a.Typ != TypeFile || a.Length != 20480 || a.Mode != 0644 || a.Nlink != 1 || a.Parent != RootInode {
		t.Fatalf("attr %+v", a)
	}
	if a.Mtime != mtime.Unix() || a.Mtimensec != uint32(mtime.Nanosecond()) || a.Atime != mtime.Unix() {
		t.Fatalf("times: mtime %d.%d atime %d", a.Mtime, a.Mtimensec, a.Atime)
	}
	var slices []Slice
	if st := m.Read(ctx, ino, 0, &slices); st != 0 || len(slices) != 1 || slices[0].Id != slice.Id || slices[0].Len != 20480 {
		t.Fatalf("read slices: %v %+v", st, slices)
	}
	used1, inodes1 := jmCounters(t, m)
	if used1-used0 != align4K(20480) || inodes1-inodes0 != 1 {
		t.Fatalf("counters: space +%d inodes +%d", used1-used0, inodes1-inodes0)
	}

	// An existing name is refused with nothing written.
	other := jmCreateFixtureSlice(t, m, 100)
	var ino2 Ino
	if st := m.JMCreateFile(ctx, RootInode, "small.xmp", 0644, 0, mtime, other, &ino2, &attr); st != syscall.EEXIST {
		t.Fatalf("existing name: %v, want EEXIST", st)
	}
	if ino2 != 0 {
		t.Fatalf("refused create reported inode %d", ino2)
	}
	if st := m.doLookup(ctx, RootInode, "small.xmp", &got, &a); st != 0 || got != ino || a.Length != 20480 {
		t.Fatalf("existing file changed: %d %v %+v", got, st, a)
	}
	if u, i := jmCounters(t, m); u != used1 || i != inodes1 {
		t.Fatalf("refusal changed counters: %d %d", u-used1, i-inodes1)
	}
	assertNoWatches(t, m)

	// An empty file has no slice.
	if st := m.JMCreateFile(ctx, RootInode, "empty", 0644, 0, mtime, Slice{}, &ino2, &attr); st != 0 {
		t.Fatalf("empty: %v", st)
	}
	if st := m.Read(ctx, ino2, 0, &slices); st != 0 || len(slices) != 0 {
		t.Fatalf("empty file slices: %v %+v", st, slices)
	}
	if attr.Length != 0 {
		t.Fatalf("empty file length %d", attr.Length)
	}
}

func TestJMCreateFileRefusals(t *testing.T) {
	m, _ := countedMeta(t)
	ctx := Background()
	mtime := time.Now()
	var ino Ino
	var attr Attr
	if st := m.Mkdir(ctx, RootInode, "locked", 0555, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	locked := ino
	var file Ino
	if st := m.Create(ctx, RootInode, "plain", 0644, 0, 0, &file, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	user := NewContext(1, 1000, []uint32{1000})
	cases := []struct {
		name   string
		ctx    Context
		parent Ino
		file   string
		slice  Slice
		want   syscall.Errno
	}{
		{"parent is a file", ctx, file, "x", Slice{}, syscall.ENOTDIR},
		{"no write permission", user, locked, "x", Slice{}, syscall.EACCES},
		{"birth stage name", ctx, RootInode, ".juicemount-streaming-drain-replacement-0123456789abcdef0123456789abcdef", Slice{}, syscall.EINVAL},
		{"slice with offset", ctx, RootInode, "x", Slice{Id: 9, Size: 10, Off: 1, Len: 9}, syscall.EINVAL},
		{"too large", ctx, RootInode, "x", Slice{Id: 9, Size: JMCreateFileMaxBytes + 1, Len: JMCreateFileMaxBytes + 1}, syscall.EINVAL},
		{"data without slice", ctx, RootInode, "x", Slice{Size: 10, Len: 10}, syscall.EINVAL},
		{"dot", ctx, RootInode, ".", Slice{}, syscall.EEXIST},
		{"slash", ctx, RootInode, "a/b", Slice{}, syscall.EINVAL},
	}
	for _, tc := range cases {
		used0, inodes0 := jmCounters(t, m)
		var got Ino
		if st := m.JMCreateFile(tc.ctx, tc.parent, tc.file, 0644, 0, mtime, tc.slice, &got, &attr); st != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, st, tc.want)
		}
		if u, i := jmCounters(t, m); u != used0 || i != inodes0 {
			t.Errorf("%s: counters changed", tc.name)
		}
	}
	m.conf.ReadOnly = true
	if st := m.JMCreateFile(ctx, RootInode, "ro", 0644, 0, mtime, Slice{}, &ino, &attr); st != syscall.EROFS {
		t.Errorf("read-only: %v", st)
	}
	m.conf.ReadOnly = false
	assertNoWatches(t, m)
}

// Racing creates of one name: exactly one wins, every loser writes nothing.
func TestJMCreateFileRace(t *testing.T) {
	m, _ := countedMeta(t)
	// Several connections, so the transactions really race in Redis.
	opt := *m.rdb.(*redis.Client).Options()
	opt.Dialer, opt.PoolSize, opt.MaxIdleConns = nil, 8, 8
	_ = m.rdb.Close()
	m.rdb = redis.NewClient(&opt)
	ctx := Background()
	used0, inodes0 := jmCounters(t, m)
	const n = 8
	var wg sync.WaitGroup
	errs := make([]syscall.Errno, n)
	inos := make([]Ino, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var attr Attr
			errs[i] = m.JMCreateFile(ctx, RootInode, "same", 0644, 0, time.Now(), Slice{}, &inos[i], &attr)
		}(i)
	}
	wg.Wait()
	won := 0
	for i, e := range errs {
		switch e {
		case 0:
			won++
		case syscall.EEXIST:
			if inos[i] != 0 {
				t.Errorf("loser %d reported inode %d", i, inos[i])
			}
		default:
			t.Errorf("create %d: %v", i, e)
		}
	}
	if won != 1 {
		t.Fatalf("%d creates won, want 1", won)
	}
	if u, i := jmCounters(t, m); u-used0 != align4K(0) || i-inodes0 != 1 {
		t.Fatalf("counters: space +%d inodes +%d", u-used0, i-inodes0)
	}
	assertNoWatches(t, m)
}

// Several files of one folder: one transaction, still 2 round trips. A taken
// name and a name repeated in the batch get EEXIST; the others are written.
func TestJMCreateFilesInOneTransaction(t *testing.T) {
	m, wc := countedMeta(t)
	setWatchBatch(t, true)
	ctx := Background()
	var ino Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "taken", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create taken: %v", st)
	}
	if _, err := m.nextInode(); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1794490456, 7)
	var files []*JMNewFile
	for _, name := range []string{"a", "b", "taken", "c", "a", "d"} {
		files = append(files, &JMNewFile{Name: name, Mode: 0644, Mtime: mtime, Slice: jmCreateFixtureSlice(t, m, 1000)})
	}
	used0, inodes0 := jmCounters(t, m)
	wc.n.Store(0)
	if st := m.JMCreateFilesIn(ctx, RootInode, files); st != 0 {
		t.Fatalf("batch: %v", st)
	}
	if got := wc.n.Load(); got != 2 {
		t.Errorf("batch of %d files took %d round trips, want 2", len(files), got)
	}
	assertNoWatches(t, m)
	want := map[int]syscall.Errno{2: syscall.EEXIST, 4: syscall.EEXIST}
	for i, f := range files {
		if f.Errno != want[i] {
			t.Fatalf("file %d (%s): %v, want %v", i, f.Name, f.Errno, want[i])
		}
		if f.Errno != 0 {
			if f.Inode != 0 {
				t.Fatalf("refused file %d has inode %d", i, f.Inode)
			}
			continue
		}
		var got Ino
		var a Attr
		if st := m.doLookup(ctx, RootInode, f.Name, &got, &a); st != 0 || got != f.Inode || a.Length != 1000 || a.Mtime != mtime.Unix() {
			t.Fatalf("%s: lookup %d %v %+v", f.Name, got, st, a)
		}
		var slices []Slice
		if st := m.Read(ctx, f.Inode, 0, &slices); st != 0 || len(slices) != 1 || slices[0].Id != f.Slice.Id {
			t.Fatalf("%s: slices %v %+v", f.Name, st, slices)
		}
	}
	used1, inodes1 := jmCounters(t, m)
	if used1-used0 != 4*align4K(1000) || inodes1-inodes0 != 4 {
		t.Fatalf("counters: space +%d inodes +%d", used1-used0, inodes1-inodes0)
	}
}
