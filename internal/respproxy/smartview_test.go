package respproxy

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Smart-folder roots added to buildTree's volume root: Recents (20),
// Collections (21), Queries (22), and Other (23), a top-level folder that
// is not a smart root.
const (
	svRecents, svCollections, svQueries, svOther = 20, 21, 22, 23
)

// svAttr is a 72-byte juicefs directory attr (mode 0755) with a link count.
func svAttr(typ byte, parent uint64, nlink uint32) string {
	b := make([]byte, 72)
	binary.BigEndian.PutUint16(b[1:3], uint16(typ)<<12|0o755)
	binary.BigEndian.PutUint64(b[23:31], 1700000000) // mtime
	binary.BigEndian.PutUint64(b[35:43], 1700000000) // ctime
	binary.BigEndian.PutUint32(b[47:51], nlink)
	binary.BigEndian.PutUint64(b[63:71], parent)
	return string(b)
}

// svBump is attr a with its link count changed by delta and new modify and
// change times, as juicefs's mkdir and rmdir write the parent.
func svBump(a string, delta int) string {
	b := []byte(a)
	now := uint64(time.Now().UnixNano())
	binary.BigEndian.PutUint64(b[23:31], now/1e9)
	binary.BigEndian.PutUint32(b[31:35], uint32(now%1e9))
	binary.BigEndian.PutUint64(b[35:43], now/1e9)
	binary.BigEndian.PutUint32(b[43:47], uint32(now%1e9))
	binary.BigEndian.PutUint32(b[47:51], uint32(int(binary.BigEndian.Uint32(b[47:51]))+delta))
	return string(b)
}

type svRig struct {
	*leaseRig
	db   *redis.Client
	ctx  context.Context
	mark string // the lease log's last id before the latest attempt
}

// markLog records where the lease log is, and moves the clock past the
// refusal log's one-per-second sampling.
func (r *svRig) markLog(t *testing.T) {
	t.Helper()
	r.clk.Add(2 * time.Second)
	r.mark = "0"
	if ents, err := r.db.XRevRangeN(r.ctx, LeaseLogKey, "+", "-", 1).Result(); err == nil && len(ents) == 1 {
		r.mark = ents[0].ID
	}
}

func startSmartViewRig(t *testing.T, roots []string) *svRig {
	addr := redisAddr(t, false)
	db := direct2(addr)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	r := startLeaseRigP(t, ModeEnforce, func(*Config) {
		// After buildTree, before the proxy and the engine start.
		pipe := db.Pipeline()
		for ino, name := range map[uint64]string{svRecents: "Recents", svCollections: "Collections", svQueries: "Queries", svOther: "Other"} {
			pipe.Set(ctx, "i"+inoStr(ino), svAttr(typeDir, 1, 2), 0)
			pipe.HSet(ctx, "d1", name, mkEntry(typeDir, ino))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}
	}, func(lc *LeaseConfig) { lc.SmartRoots = roots })
	return &svRig{leaseRig: r, db: db, ctx: ctx}
}

func (r *svRig) get(t *testing.T, key string) string {
	t.Helper()
	v, err := r.db.Get(r.ctx, key).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}

// mkdir creates directory ino named name in parent through the proxy as
// rc's principal, shaped like juicefs's doMknod of a directory.
func (r *svRig) mkdir(t *testing.T, rc *rawConn, parent, ino uint64, name string) {
	t.Helper()
	defer func() {
		if t.Failed() {
			r.tp.logs.mu.Lock()
			for _, l := range r.tp.logs.lines {
				t.Log(l)
			}
			r.tp.logs.mu.Unlock()
		}
	}()
	mustOK(t, txn(rc,
		[]string{"SET", "i" + inoStr(ino), svAttr(typeDir, parent, 2)},
		[]string{"SET", "i" + inoStr(parent), svBump(r.get(t, "i"+inoStr(parent)), 1)},
		[]string{"HSET", "d" + inoStr(parent), name, mkEntry(typeDir, ino)},
		[]string{"INCRBY", "usedSpace", "4096"},
		[]string{"INCR", "totalInodes"},
		[]string{"HSET", "dirUsedInodes", inoStr(ino), "0"}), "mkdir "+name)
}

// rmdirCmds is juicefs's rmdir txn body (redis.go doRmdir, no trash) for
// directory n named name in p, with p's attr as rmdir rewrites it.
func (r *svRig) rmdirCmds(t *testing.T, p, n uint64, name string) [][]string {
	ps, ns := inoStr(p), inoStr(n)
	cmds := [][]string{
		{"HDEL", "d" + ps, name},
		{"SET", "i" + ps, svBump(r.get(t, "i"+ps), -1)},
		{"DEL", "i" + ns},
		{"DEL", "x" + ns},
		{"INCRBY", "usedSpace", "-4096"},
		{"DECR", "totalInodes"},
	}
	for _, h := range smartViewDirStatHashes {
		cmds = append(cmds, []string{"HDEL", h, ns})
	}
	return cmds
}

// rmdir sends cmds the way juicefs's rmdir does: WATCH the parent, look the
// name up, WATCH the directory, then MULTI ... EXEC (on a new connection).
func (r *svRig) rmdir(t *testing.T, who string, p, n uint64, name string, cmds [][]string) Value {
	t.Helper()
	r.markLog(t)
	rc := r.conn(t, who)
	mustOK(t, rc.do("WATCH", "i"+inoStr(p), "d"+inoStr(p)), "WATCH parent")
	rc.do("HGET", "d"+inoStr(p), name)
	mustOK(t, rc.do("WATCH", "i"+inoStr(n), "d"+inoStr(n)), "WATCH dir")
	rc.do("HLEN", "d"+inoStr(n))
	return txn(rc, cmds...)
}

func (r *svRig) entryExists(t *testing.T, p uint64, name string) bool {
	t.Helper()
	ok, err := r.db.HExists(r.ctx, "d"+inoStr(p), name).Result()
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// wantRefused: v is the root-entry gate's refusal, logged with why, and
// nothing changed.
func (r *svRig) wantRefused(t *testing.T, v Value, p, n uint64, name, what, why string) {
	t.Helper()
	mustDeny(t, v, ReasonAdminOnly, what)
	deadline := time.Now().Add(2 * time.Second)
	for logged := false; !logged; {
		ents, err := r.db.XRange(r.ctx, LeaseLogKey, "("+r.mark, "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			d, _ := e.Values["detail"].(string)
			logged = logged || (e.Values["event"] == "deny" && e.Values["proj"] == inoStr(n) && strings.Contains(d, why))
		}
		if !logged && time.Now().After(deadline) {
			t.Fatalf("%s: no refusal logged for %d with %q in %v", what, n, why, ents)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !r.entryExists(t, p, name) {
		t.Fatalf("%s: the refused unit removed the entry", what)
	}
	if n, err := r.db.Exists(r.ctx, "i"+inoStr(n)).Result(); err != nil || n != 1 {
		t.Fatalf("%s: the refused unit removed the inode: %d %v", what, n, err)
	}
}

func TestSmartViewRemovalAllowedUnderEachRoot(t *testing.T) {
	r := startSmartViewRig(t, DefaultSmartRoots)
	mgr := r.conn(t, "mgr1")
	first, _ := allocInodes(t, mgr)
	for i, root := range []uint64{svRecents, svCollections, svQueries} {
		ino := first + 200 + uint64(i)
		r.mkdir(t, mgr, root, ino, "Shoot")
		mustOK(t, r.rmdir(t, "mgr1", root, ino, "Shoot", r.rmdirCmds(t, root, ino, "Shoot")), "manager rmdir of an empty view")
		if r.entryExists(t, root, "Shoot") {
			t.Fatalf("root %d: the view is still there", root)
		}
		if n, _ := r.db.Exists(r.ctx, "i"+inoStr(ino)).Result(); n != 0 {
			t.Fatalf("root %d: the view's inode is still there", root)
		}
		// The root's attr is rmdir's: link count back to 2.
		if a := r.get(t, "i"+inoStr(root)); binary.BigEndian.Uint32([]byte(a)[47:51]) != 2 {
			t.Fatalf("root %d link count after rmdir = %d", root, binary.BigEndian.Uint32([]byte(a)[47:51]))
		}
	}
	// Each admitted removal is in the lease log.
	deadline := time.Now().Add(2 * time.Second)
	for {
		ents, _ := r.db.XRange(r.ctx, LeaseLogKey, "-", "+").Result()
		n := 0
		for _, e := range ents {
			if e.Values["event"] == "smart_view_remove" && e.Values["actor"] == "mgr1" && e.Values["view"] == "Shoot" {
				n++
			}
		}
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease log has %d smart_view_remove events, want 3", n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSmartViewRemovalRefusals(t *testing.T) {
	r := startSmartViewRig(t, DefaultSmartRoots)
	mgr, mac := r.conn(t, "mgr1"), r.conn(t, "mac1")
	first, _ := allocInodes(t, mgr)
	macFirst, _ := allocInodes(t, mac)
	next := first + 300
	view := func(root uint64, name string) uint64 {
		t.Helper()
		next++
		r.mkdir(t, mgr, root, next, name)
		return next
	}

	t.Run("not_empty", func(t *testing.T) {
		n := view(svRecents, "Full")
		f := inoStr(first + 900)
		mustOK(t, txn(mgr,
			[]string{"SET", "i" + f, mkAttr(typeFile, n)},
			[]string{"SET", "i" + inoStr(n), svBump(r.get(t, "i"+inoStr(n)), 0)},
			[]string{"HSET", "d" + inoStr(n), "clip.mov", mkEntry(typeFile, first+900)},
			[]string{"INCRBY", "usedSpace", "0"},
			[]string{"INCR", "totalInodes"}), "file in the view")
		r.wantRefused(t, r.rmdir(t, "mgr1", svRecents, n, "Full", r.rmdirCmds(t, svRecents, n, "Full")), svRecents, n, "Full", "non-empty view", "directory "+inoStr(n)+" is not empty")
	})
	t.Run("smart_root_itself", func(t *testing.T) {
		// The root is directly under the volume root, not under a smart root.
		cmds := r.rmdirCmds(t, 1, svQueries, "Queries")
		r.wantRefused(t, r.rmdir(t, "mgr1", 1, svQueries, "Queries", cmds), 1, svQueries, "Queries", "removing a smart root", "directory 1 is not a smart-folder root")
	})
	t.Run("not_a_smart_root", func(t *testing.T) {
		n := view(svOther, "Shoot")
		r.wantRefused(t, r.rmdir(t, "mgr1", svOther, n, "Shoot", r.rmdirCmds(t, svOther, n, "Shoot")), svOther, n, "Shoot", "view under a non-smart root", "directory 23 is not a smart-folder root")
	})
	t.Run("not_the_manager", func(t *testing.T) {
		n := view(svRecents, "Theirs")
		for _, who := range []string{"mac1", "farm1"} {
			r.wantRefused(t, r.rmdir(t, who, svRecents, n, "Theirs", r.rmdirCmds(t, svRecents, n, "Theirs")), svRecents, n, "Theirs", who+" rmdir of a view", "above the project depth needs admin")
		}
	})
	t.Run("not_created_by_the_manager", func(t *testing.T) {
		n := macFirst + 50
		r.mkdir(t, mac, svCollections, n, "MacMade")
		r.wantRefused(t, r.rmdir(t, "mgr1", svCollections, n, "MacMade", r.rmdirCmds(t, svCollections, n, "MacMade")), svCollections, n, "MacMade", "rmdir of a Mac's empty folder", "was not created by mgr1")
	})
	t.Run("touches_other_keys", func(t *testing.T) {
		n := view(svCollections, "Extra")
		for _, extra := range [][]string{
			{"HSET", "x10", "user.k", "v"},                          // another project's key
			{"SET", "i" + inoStr(svRecents), svAttr(typeDir, 1, 2)}, // another root's attr
			{"DEL", "x" + inoStr(n)},                                // a command twice
		} {
			cmds := append(r.rmdirCmds(t, svCollections, n, "Extra"), extra)
			r.wantRefused(t, r.rmdir(t, "mgr1", svCollections, n, "Extra", cmds), svCollections, n, "Extra", "rmdir plus "+strings.Join(extra[:2], " "), "is not exactly an rmdir")
		}
		// A command missing (no counter update) is not juicefs's rmdir either.
		cmds := r.rmdirCmds(t, svCollections, n, "Extra")
		cmds = append(cmds[:5:5], cmds[6:]...)
		r.wantRefused(t, r.rmdir(t, "mgr1", svCollections, n, "Extra", cmds), svCollections, n, "Extra", "rmdir without DECR totalInodes", "is not exactly an rmdir")
		// The parent attr changes more than the link count and times.
		cmds = r.rmdirCmds(t, svCollections, n, "Extra")
		b := []byte(cmds[1][2])
		b[2] ^= 0o002 // mode
		cmds[1][2] = string(b)
		r.wantRefused(t, r.rmdir(t, "mgr1", svCollections, n, "Extra", cmds), svCollections, n, "Extra", "rmdir that changes the root's mode", "changes more than its link count and times")
	})
	t.Run("not_watched", func(t *testing.T) {
		n := view(svRecents, "NoWatch")
		r.markLog(t)
		rc := r.conn(t, "mgr1")
		mustOK(t, rc.do("WATCH", "i"+inoStr(svRecents), "d"+inoStr(svRecents)), "WATCH parent")
		r.wantRefused(t, txn(rc, r.rmdirCmds(t, svRecents, n, "NoWatch")...), svRecents, n, "NoWatch", "rmdir without WATCH of the directory", "is not WATCHed")
	})
	t.Run("renamed_away_root", func(t *testing.T) {
		n := view(svQueries, "Moved")
		admin := r.conn(t, "admin1")
		mustOK(t, txn(admin, []string{"HDEL", "d1", "Queries"}, []string{"HSET", "d1", "OldQueries", mkEntry(typeDir, svQueries)}), "admin renames the root")
		r.wantRefused(t, r.rmdir(t, "mgr1", svQueries, n, "Moved", r.rmdirCmds(t, svQueries, n, "Moved")), svQueries, n, "Moved", "rmdir under a renamed-away smart root", "directory 22 is not a smart-folder root")
	})
	t.Run("file_attr", func(t *testing.T) {
		// The entry says directory, the inode is a file.
		n := view(svRecents, "Odd")
		if err := r.db.Set(r.ctx, "i"+inoStr(n), svAttr(typeFile, svRecents, 1), 0).Err(); err != nil {
			t.Fatal(err)
		}
		r.wantRefused(t, r.rmdir(t, "mgr1", svRecents, n, "Odd", r.rmdirCmds(t, svRecents, n, "Odd")), svRecents, n, "Odd", "rmdir of an inode that is not a directory", "is not a directory in 20")
	})
	t.Run("rename_aside", func(t *testing.T) {
		// Moving the view to a hidden name stays admin-only.
		n := view(svCollections, "Aside")
		r.markLog(t)
		v := txn(mgr, []string{"HDEL", "d" + inoStr(svCollections), "Aside"},
			[]string{"HSET", "d" + inoStr(svCollections), ".juicemount-smart-folder.tmp-1", mkEntry(typeDir, n)})
		r.wantRefused(t, v, svCollections, n, "Aside", "rename of a view to a hidden name", "is not exactly an rmdir")
	})
}

func TestSmartViewRemovalOffWithoutRoots(t *testing.T) {
	r := startSmartViewRig(t, nil)
	mgr := r.conn(t, "mgr1")
	first, _ := allocInodes(t, mgr)
	n := first + 200
	r.mkdir(t, mgr, svRecents, n, "Shoot")
	r.wantRefused(t, r.rmdir(t, "mgr1", svRecents, n, "Shoot", r.rmdirCmds(t, svRecents, n, "Shoot")), svRecents, n, "Shoot", "rmdir with no smart roots configured", "no smart-folder roots configured")
}

func TestCleanSmartRoots(t *testing.T) {
	if ok, bad := CleanSmartRoots(DefaultSmartRoots); len(bad) != 0 || strings.Join(ok, ",") != "Recents,Collections,Queries" {
		t.Fatalf("defaults: %q %v", ok, bad)
	}
	ok, bad := CleanSmartRoots([]string{"", "a/b", ".trash", ".hidden", "..", "Recents", "Recents", "Views"})
	if len(bad) != 5 || strings.Join(ok, ",") != "Recents,Views" {
		t.Fatalf("CleanSmartRoots = %q, %v", ok, bad)
	}
}
