//go:build !noredis

package meta

// Chaos regression test for patch 0019 (found in review): random CLIENT KILL
// of this database's connections while three sessions of eight workers
// create, link, rename, unlink, write, truncate and chmod in six shared
// folders. Before the fix a held WATCH could be lost with a connection, and
// some runs panicked on an empty dentry read (in review: from 1 of 11 runs to
// 5 of 5, depending on the machine). This test is a broad, supplementary
// check; TestJMWatchBatchDialFailureKeepsWatch is the reliable regression
// test for that bug. Afterwards no connection may still hold a WATCH, and
// every dentry, inode, link count and parent must agree.
//
// JM_WATCH_BATCH_CHAOS_REDIS_URL=redis://host:port/db names a disposable
// database (it is flushed). The test kills connections in that database and
// in database 0, where a new connection sits until its handshake selects
// the database: a kill there fails the dial, which is what lost the WATCH.
// Run it only against a Redis server nothing else uses meanwhile.
// JM_WATCH_BATCH_CHAOS_OPS sets the operations per worker (default 400);
// JM_WATCH_BATCH_CHAOS_KILL=0 runs the workload without kills.

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestJMWatchBatchChaos(t *testing.T) {
	u := os.Getenv("JM_WATCH_BATCH_CHAOS_REDIS_URL")
	if u == "" {
		t.Skip("set JM_WATCH_BATCH_CHAOS_REDIS_URL to a disposable Redis database (it is flushed)")
	}
	addr := strings.TrimPrefix(u, "redis://")
	ops := 400
	if v := os.Getenv("JM_WATCH_BATCH_CHAOS_OPS"); v != "" {
		ops, _ = strconv.Atoi(v)
	}
	chaos := os.Getenv("JM_WATCH_BATCH_CHAOS_KILL") != "0" // 0: the same workload without kills
	const sessions, workers, ndirs, names = 3, 8, 6, 10

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
			if err := m.Init(&Format{Name: "chaos", UUID: "watch-batch-chaos", DirStats: true, TrashDays: 0}, true); err != nil {
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
	ctx := Background()
	m0 := metas[0]
	var dirs []Ino
	for i := 0; i < ndirs; i++ {
		var ino Ino
		var attr Attr
		if st := m0.Mkdir(ctx, RootInode, fmt.Sprintf("d%d", i), 0755, 0, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir: %v", st)
		}
		dirs = append(dirs, ino)
	}
	hp := strings.SplitN(addr, "/", 2)
	db, _ := strconv.Atoi(hp[1])
	admin := redis.NewClient(&redis.Options{Addr: hp[0], DB: db})
	defer admin.Close()

	stop := make(chan struct{})
	var kills atomic.Int64
	var cwg sync.WaitGroup
	if chaos {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				select {
				case <-stop:
					return
				case <-time.After(time.Duration(1+rand.Intn(8)) * time.Millisecond):
				}
				// Kill one random connection (not ours) of this database, or
				// one still in its handshake (database 0).
				list, err := admin.ClientList(Background()).Result()
				if err != nil {
					continue
				}
				me, _ := admin.ClientID(Background()).Result()
				var ids []string
				for _, l := range strings.Split(list, "\n") {
					var id string
					inDB := false
					for _, f := range strings.Fields(l) {
						if strings.HasPrefix(f, "id=") && f != "id="+strconv.FormatInt(me, 10) {
							id = f[3:]
						}
						if f == "db="+strconv.Itoa(db) || f == "db=0" {
							inDB = true
						}
					}
					if id != "" && inDB {
						ids = append(ids, id)
					}
				}
				if len(ids) > 0 {
					admin.Do(Background(), "CLIENT", "KILL", "ID", ids[rand.Intn(len(ids))])
					kills.Add(1)
				}
			}
		}()
	}

	var errs sync.Map
	var okOps atomic.Int64
	note := func(op string, st error) {
		if st == nil {
			okOps.Add(1)
			return
		}
		k := op + ":" + st.Error()
		v, _ := errs.LoadOrStore(k, new(atomic.Int64))
		v.(*atomic.Int64).Add(1)
	}
	var wg sync.WaitGroup
	for s := 0; s < sessions; s++ {
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(m *redisMeta, seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				pick := func() (Ino, string) { return dirs[r.Intn(len(dirs))], fmt.Sprintf("f%d", r.Intn(names)) }
				for i := 0; i < ops; i++ {
					p, n := pick()
					switch r.Intn(10) {
					case 0, 1:
						var ino Ino
						var a Attr
						note("mknod", chaosErr(m.Mknod(ctx, p, n, TypeFile, 0644, 0, 0, "", &ino, &a)))
					case 2:
						var ino Ino
						var a Attr
						note("mkdir", chaosErr(m.Mkdir(ctx, p, "s"+n, 0755, 0, 0, &ino, &a)))
					case 3:
						note("rmdir", chaosErr(m.Rmdir(ctx, p, "s"+n)))
					case 4:
						note("unlink", chaosErr(m.Unlink(ctx, p, n)))
					case 5, 6:
						p2, n2 := pick()
						var ino Ino
						var a Attr
						note("rename", chaosErr(m.Rename(ctx, p, n, p2, n2, 0, &ino, &a)))
					case 7:
						var a Attr
						if m.Lookup(ctx, p, n, new(Ino), &a, false) == 0 {
							var ino Ino
							_ = m.Lookup(ctx, p, n, &ino, &a, false)
							p2, n2 := pick()
							note("link", chaosErr(m.Link(ctx, ino, p2, n2, &a)))
						}
					case 8:
						var ino Ino
						var a Attr
						if m.Lookup(ctx, p, n, &ino, &a, false) == 0 && a.Typ == TypeFile {
							var id uint64
							if m.NewSlice(ctx, &id) == 0 {
								sz := uint32(1 + r.Intn(1<<16))
								note("write", chaosErr(m.Write(ctx, ino, uint32(r.Intn(2)), uint32(r.Intn(1<<20)), Slice{Id: id, Size: sz, Len: sz}, time.Now())))
							}
						}
					case 9:
						var ino Ino
						var a Attr
						if m.Lookup(ctx, p, n, &ino, &a, false) == 0 && a.Typ == TypeFile {
							if r.Intn(2) == 0 {
								note("truncate", chaosErr(m.Truncate(ctx, ino, 0, uint64(r.Intn(1<<21)), &a, true)))
							} else {
								a2 := Attr{Mode: uint16(0600 + r.Intn(0100))}
								note("setattr", chaosErr(m.SetAttr(ctx, ino, SetAttrMode, 0, &a2)))
							}
						}
					}
				}
			}(metas[s], int64(s*100+w))
		}
	}
	wg.Wait()
	close(stop)
	cwg.Wait()
	t.Logf("ok ops %d, kills %d", okOps.Load(), kills.Load())
	errs.Range(func(k, v any) bool {
		t.Logf("err %s x%d", k, v.(*atomic.Int64).Load())
		return true
	})

	// Leftover WATCH on any pooled connection?
	time.Sleep(200 * time.Millisecond)
	list, _ := admin.ClientList(Background()).Result()
	for _, l := range strings.Split(list, "\n") {
		if !strings.Contains(l, " db="+strconv.Itoa(db)+" ") {
			continue
		}
		if strings.Contains(l, " watch=") && !strings.Contains(l, " watch=0 ") {
			t.Errorf("connection still watching: %s", l)
		}
	}
	for _, m := range metas {
		_ = m.CloseSession()
		_ = m.Shutdown()
	}
	chaosCheckTree(t, admin)
}

func chaosErr(st syscall.Errno) error {
	if st == 0 {
		return nil
	}
	return st
}

func chaosCheckTree(t *testing.T, c *redis.Client) {
	ctx := Background()
	type ent struct {
		dir  Ino
		name string
		typ  uint8
	}
	refs := map[Ino][]ent{}
	subdirs := map[Ino]int{}
	attrs := map[Ino]*Attr{}
	rm := &redisMeta{}
	var cursor uint64
	for {
		keys, cur, err := c.Scan(ctx, cursor, "*", 1000).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			switch {
			case strings.HasPrefix(k, "i") && chaosIsNum(k[1:]):
				ino, _ := strconv.ParseUint(k[1:], 10, 64)
				b, err := c.Get(ctx, k).Bytes()
				if err != nil {
					t.Errorf("get %s: %v", k, err)
					continue
				}
				a := &Attr{}
				rm.parseAttr(b, a)
				attrs[Ino(ino)] = a
			case strings.HasPrefix(k, "d") && chaosIsNum(k[1:]):
				dir, _ := strconv.ParseUint(k[1:], 10, 64)
				h, err := c.HGetAll(ctx, k).Result()
				if err != nil {
					t.Fatal(err)
				}
				for name, v := range h {
					typ := v[0]
					ino := Ino(binary.BigEndian.Uint64([]byte(v)[1:]))
					refs[ino] = append(refs[ino], ent{Ino(dir), name, typ})
					if typ == TypeDirectory {
						subdirs[Ino(dir)]++
					}
				}
			}
		}
		cursor = cur
		if cursor == 0 {
			break
		}
	}
	bad := 0
	report := func(f string, a ...any) {
		bad++
		if bad < 40 {
			t.Errorf(f, a...)
		}
	}
	for ino, es := range refs {
		a := attrs[ino]
		if a == nil {
			report("entry %v -> missing inode %d", es, ino)
			continue
		}
		for _, e := range es {
			if e.typ != a.Typ {
				report("entry %v type %d != inode %d type %d", e, e.typ, ino, a.Typ)
			}
			if attrs[e.dir] == nil || attrs[e.dir].Typ != TypeDirectory {
				report("entry %v in non-directory", e)
			}
		}
	}
	for ino, a := range attrs {
		if ino == RootInode || ino >= TrashInode {
			continue
		}
		es := refs[ino]
		switch a.Typ {
		case TypeDirectory:
			if len(es) != 1 {
				report("dir %d has %d entries", ino, len(es))
			} else if a.Parent != es[0].dir {
				report("dir %d parent %d but entry in %d", ino, a.Parent, es[0].dir)
			}
			if a.Nlink != uint32(2+subdirs[ino]) {
				report("dir %d nlink %d want %d", ino, a.Nlink, 2+subdirs[ino])
			}
		default:
			if len(es) == 0 {
				report("orphan inode %d nlink %d", ino, a.Nlink)
			}
			if a.Nlink != uint32(len(es)) {
				report("file %d nlink %d but %d entries %v", ino, a.Nlink, len(es), es)
			}
			if len(es) == 1 && a.Parent != 0 && a.Parent != es[0].dir {
				report("file %d parent %d but entry in %d", ino, a.Parent, es[0].dir)
			}
		}
	}
	if a := attrs[RootInode]; a != nil && a.Nlink != uint32(2+subdirs[RootInode]) {
		report("root nlink %d want %d", a.Nlink, 2+subdirs[RootInode])
	}
	t.Logf("checked %d inodes, %d with entries, %d problems", len(attrs), len(refs), bad)
}

func chaosIsNum(s string) bool {
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil && s != ""
}
