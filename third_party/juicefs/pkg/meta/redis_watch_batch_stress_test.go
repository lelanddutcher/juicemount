//go:build !noredis

package meta

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestJMWatchBatchStress races many writers in two sessions (two Redis
// clients, like two Macs) on shared and private folders, with renames that
// replace files racing writes to them. Afterwards the namespace, file
// lengths, global counters and folder statistics must agree with what the
// operations that succeeded imply. It runs the same way on a build without
// patch 0019, so the two can be compared.
//
// JM_WATCH_BATCH_STRESS_REDIS_URL=redis://host:port/db names a disposable
// database (it is flushed). JM_WATCH_BATCH_STRESS_OPS sets the operations per
// worker (default 300).
func TestJMWatchBatchStress(t *testing.T) {
	u := os.Getenv("JM_WATCH_BATCH_STRESS_REDIS_URL")
	if u == "" {
		t.Skip("set JM_WATCH_BATCH_STRESS_REDIS_URL to a disposable Redis database (it is flushed)")
	}
	addr := strings.TrimPrefix(u, "redis://")
	ops := 300
	if v := os.Getenv("JM_WATCH_BATCH_STRESS_OPS"); v != "" {
		fmt.Sscan(v, &ops)
	}
	const sessions, workers, shared, names = 2, 8, 4, 12

	var metas []*redisMeta
	for i := 0; i < sessions; i++ {
		store, err := newRedisMeta("redis", addr, testConfig())
		if err != nil {
			t.Fatal(err)
		}
		m := store.(*redisMeta)
		if i == 0 {
			if err := m.rdb.FlushDB(Background()).Err(); err != nil {
				t.Fatal(err)
			}
			if err := m.Init(&Format{Name: "stress", UUID: "watch-batch-stress", DirStats: true, TrashDays: 0}, true); err != nil {
				t.Fatal(err)
			}
		} else if _, err := m.Load(true); err != nil {
			t.Fatal(err)
		}
		if err := m.NewSession(true); err != nil {
			t.Fatal(err)
		}
		metas = append(metas, m)
	}
	defer func() {
		for _, m := range metas {
			_ = m.CloseSession()
			_ = m.Shutdown()
		}
	}()
	ctx := Background()
	m0 := metas[0]
	var dirs []Ino
	for i := 0; i < shared+sessions*workers; i++ {
		var ino Ino
		var attr Attr
		if st := m0.Mkdir(ctx, RootInode, fmt.Sprintf("d%02d", i), 0755, 0, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir: %v", st)
		}
		dirs = append(dirs, ino)
	}

	var mu sync.Mutex
	want := map[Ino]uint64{} // inode -> largest end of a successful write
	var known []Ino
	stats := map[string]int{}
	note := func(k string) { mu.Lock(); stats[k]++; mu.Unlock() }
	write := func(m *redisMeta, ino Ino, off uint32, n uint32) syscall.Errno {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			return st
		}
		st := m.Write(ctx, ino, 0, off, Slice{Id: id, Size: n, Len: n}, time.Now())
		if st == 0 {
			mu.Lock()
			if e := uint64(off + n); e > want[ino] {
				want[ino] = e
			}
			mu.Unlock()
		}
		return st
	}

	var wg sync.WaitGroup
	for s := 0; s < sessions; s++ {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(m *redisMeta, own Ino, seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				folder := func() Ino {
					if rng.Intn(3) == 0 {
						return own
					}
					return dirs[rng.Intn(shared)]
				}
				for i := 0; i < ops; i++ {
					switch r := rng.Intn(10); {
					case r < 5: // create and write a small file, then close
						var ino Ino
						var attr Attr
						st := m.Create(ctx, folder(), fmt.Sprintf("f%02d", rng.Intn(names)), 0644, 022, uint32(syscall.O_EXCL), &ino, &attr)
						if st != 0 {
							note("create " + st.Error())
							continue
						}
						note("create ok")
						mu.Lock()
						known = append(known, ino)
						mu.Unlock()
						for k, off := 0, uint32(0); k < 1+rng.Intn(3); k++ {
							n := uint32(1 + rng.Intn(70000))
							if st := write(m, ino, off, n); st != 0 {
								note("write-own " + st.Error())
							}
							off += n
						}
						_ = m.Close(ctx, ino)
					case r < 7: // write to any file, which may be renamed or replaced meanwhile
						mu.Lock()
						if len(known) == 0 {
							mu.Unlock()
							continue
						}
						ino := known[rng.Intn(len(known))]
						mu.Unlock()
						if st := write(m, ino, uint32(rng.Intn(200000)), uint32(1+rng.Intn(9000))); st != 0 {
							note("write-other " + st.Error())
						} else {
							note("write-other ok")
						}
					default: // rename, replacing the target if it exists
						src := folder()
						var es []*Entry
						if st := m.Readdir(ctx, src, 0, &es); st != 0 {
							continue
						}
						var files []string
						for _, e := range es {
							if n := string(e.Name); n != "." && n != ".." {
								files = append(files, n)
							}
						}
						if len(files) == 0 {
							continue
						}
						var ino Ino
						var attr Attr
						st := m.Rename(ctx, src, files[rng.Intn(len(files))], folder(), fmt.Sprintf("f%02d", rng.Intn(names)), 0, &ino, &attr)
						if st == 0 {
							note("rename ok")
						} else {
							note("rename " + st.Error())
						}
					}
				}
			}(metas[s], dirs[shared+s*workers+w], int64(1000*s+w))
		}
	}
	wg.Wait()
	t.Logf("operations: %v", stats)
	for _, m := range metas {
		m.doFlushDirStat()
	}
	time.Sleep(300 * time.Millisecond)

	// Walk the namespace: every file appears once, with the right parent,
	// one link and the length its successful writes imply.
	seen := map[Ino]bool{}
	var files, totalSpace int64
	drift := 0
	for _, d := range dirs {
		var es []*Entry
		if st := m0.Readdir(ctx, d, 1, &es); st != 0 {
			t.Fatalf("readdir %d: %v", d, st)
		}
		var ds dirStat
		for _, e := range es {
			n := string(e.Name)
			if n == "." || n == ".." {
				continue
			}
			var a Attr
			if st := m0.GetAttr(ctx, e.Inode, &a); st != 0 {
				t.Fatalf("getattr %s/%d: %v", n, e.Inode, st)
			}
			if seen[e.Inode] {
				t.Errorf("inode %d has two names", e.Inode)
			}
			seen[e.Inode] = true
			if a.Parent != d || a.Nlink != 1 || a.Typ != TypeFile {
				t.Errorf("inode %d in %d: parent %d nlink %d type %d", e.Inode, d, a.Parent, a.Nlink, a.Typ)
			}
			mu.Lock()
			w := want[e.Inode]
			mu.Unlock()
			if a.Length != w {
				t.Errorf("inode %d length %d, successful writes imply %d", e.Inode, a.Length, w)
			}
			files++
			totalSpace += align4K(a.Length)
			ds.length += int64(a.Length)
			ds.space += align4K(a.Length)
			ds.inodes++
		}
		got, st := m0.doGetDirStat(ctx, d, false)
		if st != 0 || got == nil {
			t.Fatalf("dir stat %d: %v", d, st)
		}
		// Folder statistics are best effort upstream: a write or rename
		// that races a rename of the same file credits the parent it saw,
		// and fsck repairs it. A build without patch 0019 drifts the same
		// way, so this is reported, not failed.
		if *got != ds {
			drift++
			t.Logf("folder %d stats %+v, its files imply %+v", d, *got, ds)
		}
	}
	// Global counters: every folder and file costs one inode and its 4 KiB
	// aligned length (a folder's is 4 KiB). A replaced file that another
	// session still had open is released when that session ends, so end
	// both sessions first.
	for _, m := range metas {
		_ = m.CloseSession()
	}
	wantInodes := files + int64(len(dirs))
	wantUsed := totalSpace + int64(len(dirs))*align4K(0)
	var used, inodes int64
	for i := 0; i < 50; i++ {
		var err error
		if used, err = m0.getCounter(usedSpace); err != nil {
			t.Fatal(err)
		}
		if inodes, err = m0.getCounter(totalInodes); err != nil {
			t.Fatal(err)
		}
		if inodes == wantInodes && used == wantUsed {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if inodes != wantInodes || used != wantUsed {
		t.Errorf("counters: inodes %d used %d, namespace implies %d and %d", inodes, used, wantInodes, wantUsed)
	}
	t.Logf("files %d, inodes %d, used %d; folders whose statistics drifted: %d of %d", files, inodes, used, drift, len(dirs))
}
