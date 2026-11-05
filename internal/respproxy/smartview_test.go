package respproxy

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// svRoots are the smart roots the tests configure. The rig adds to
// buildTree's volume root, through the proxy: Recents and Collections made
// by the manager principal, Queries made by a Mac, and Other, a top-level
// folder of the Manager's that is not a smart root.
var svRoots = []string{"Recents", "Collections", "Queries"}

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

	mgr, mac       *rawConn
	next, macNext  uint64 // the next unused inode of mgr1's and mac1's batches
	recents, colls uint64
	queries, other uint64
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
	r := &svRig{leaseRig: startLeaseRig(t, ModeEnforce, func(lc *LeaseConfig) { lc.SmartRoots = roots }),
		db: db, ctx: context.Background()}
	r.mgr, r.mac = r.conn(t, "mgr1"), r.conn(t, "mac1")
	first, _ := allocInodes(t, r.mgr)
	macFirst, _ := allocInodes(t, r.mac)
	r.next, r.macNext = first+100, macFirst+100 // clear of buildTree's inodes
	r.recents = r.mkdir(t, r.mgr, 1, "Recents")
	r.colls = r.mkdir(t, r.mgr, 1, "Collections")
	r.queries = r.mkdir(t, r.mac, 1, "Queries")
	r.other = r.mkdir(t, r.mgr, 1, "Other")
	return r
}

// recorded reports whether n is in the view record, waiting up to 2 s for
// want (the record is written once the creating unit's reply arrived).
func (r *svRig) recorded(t *testing.T, n uint64, want bool) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := r.db.SIsMember(r.ctx, SmartViewsKey, inoStr(n)).Result()
		if err != nil {
			t.Fatal(err)
		}
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// logged counts lease-log events of type event for inode n.
func (r *svRig) logged(t *testing.T, event string, n uint64) int {
	t.Helper()
	ents, err := r.db.XRange(r.ctx, LeaseLogKey, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	c := 0
	for _, e := range ents {
		if e.Values["event"] == event && e.Values["proj"] == inoStr(n) {
			c++
		}
	}
	return c
}

// view makes a view directory of the Manager's in root and waits for its
// record when root is one of the Manager's smart roots.
func (r *svRig) view(t *testing.T, root uint64, name string) uint64 {
	t.Helper()
	n := r.mkdir(t, r.mgr, root, name)
	if root == r.recents || root == r.colls {
		if !r.recorded(t, n, true) {
			t.Fatalf("view %s (%d) not recorded", name, n)
		}
	}
	return n
}

func (r *svRig) get(t *testing.T, key string) string {
	t.Helper()
	v, err := r.db.Get(r.ctx, key).Result()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	return v
}

// mkdir creates a directory named name in parent through the proxy as
// rc's principal (an inode of its own batch), shaped like juicefs's
// doMknod of a directory, and returns its inode.
func (r *svRig) mkdir(t *testing.T, rc *rawConn, parent uint64, name string) uint64 {
	t.Helper()
	ino := &r.next
	if rc == r.mac {
		ino = &r.macNext
	}
	*ino++
	n := *ino
	mustOK(t, txn(rc,
		[]string{"SET", "i" + inoStr(n), svAttr(typeDir, parent, 2)},
		[]string{"SET", "i" + inoStr(parent), svBump(r.get(t, "i"+inoStr(parent)), 1)},
		[]string{"HSET", "d" + inoStr(parent), name, mkEntry(typeDir, n)},
		[]string{"INCRBY", "usedSpace", "4096"},
		[]string{"INCR", "totalInodes"},
		[]string{"HSET", "dirUsedInodes", inoStr(n), "0"}), "mkdir "+name)
	return n
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
	r := startSmartViewRig(t, svRoots)
	for _, root := range []uint64{r.recents, r.colls} {
		n := r.view(t, root, "Shoot")
		if r.logged(t, "smart_view_create", n) != 1 {
			// queued with the record; give the log writer a moment
			time.Sleep(200 * time.Millisecond)
			if r.logged(t, "smart_view_create", n) != 1 {
				t.Fatalf("root %d: no smart_view_create event", root)
			}
		}
		mustOK(t, r.rmdir(t, "mgr1", root, n, "Shoot", r.rmdirCmds(t, root, n, "Shoot")), "manager rmdir of an empty view")
		if r.entryExists(t, root, "Shoot") {
			t.Fatalf("root %d: the view is still there", root)
		}
		if c, _ := r.db.Exists(r.ctx, "i"+inoStr(n)).Result(); c != 0 {
			t.Fatalf("root %d: the view's inode is still there", root)
		}
		if a := r.get(t, "i"+inoStr(root)); binary.BigEndian.Uint32([]byte(a)[47:51]) != 2 {
			t.Fatalf("root %d link count after rmdir = %d", root, binary.BigEndian.Uint32([]byte(a)[47:51]))
		}
		if r.recorded(t, n, false) {
			t.Fatalf("root %d: the removed view is still recorded", root)
		}
		deadline := time.Now().Add(2 * time.Second)
		for r.logged(t, "smart_view_remove", n) != 1 {
			if time.Now().After(deadline) {
				t.Fatalf("root %d: no smart_view_remove event", root)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// The removal is logged, and its record dropped, only once it committed.
func TestSmartViewRemovalLoggedOnlyWhenCommitted(t *testing.T) {
	r := startSmartViewRig(t, svRoots)
	t.Run("lease_held", func(t *testing.T) {
		n := r.view(t, r.colls, "Held")
		// The Mac takes the view's project with a write that changes nothing.
		mustOK(t, r.mac.do("SET", "i"+inoStr(n), r.get(t, "i"+inoStr(n))), "mac1 takes the view's lease")
		mustDeny(t, r.rmdir(t, "mgr1", r.colls, n, "Held", r.rmdirCmds(t, r.colls, n, "Held")), ReasonLeaseHeld, "rmdir of a view a Mac holds")
		time.Sleep(200 * time.Millisecond)
		if !r.entryExists(t, r.colls, "Held") || r.logged(t, "smart_view_remove", n) != 0 || !r.recorded(t, n, true) {
			t.Fatal("a refused removal was logged or dropped the record")
		}
	})
	t.Run("watch_abort", func(t *testing.T) {
		n := r.view(t, r.recents, "Abort")
		rc := r.conn(t, "mgr1")
		p := inoStr(r.recents)
		mustOK(t, rc.do("WATCH", "i"+p, "d"+p), "WATCH parent")
		mustOK(t, rc.do("WATCH", "i"+inoStr(n), "d"+inoStr(n)), "WATCH dir")
		cmds := r.rmdirCmds(t, r.recents, n, "Abort")
		// Another client rewrites the root's attr unchanged: the WATCH fires.
		if err := r.db.Set(r.ctx, "i"+p, r.get(t, "i"+p), 0).Err(); err != nil {
			t.Fatal(err)
		}
		if v := txn(rc, cmds...); v.Type == '-' || !v.Null {
			t.Fatalf("EXEC after the WATCH fired: %c %q null=%v, want a nil reply", v.Type, v.Str, v.Null)
		}
		time.Sleep(200 * time.Millisecond)
		if !r.entryExists(t, r.recents, "Abort") || r.logged(t, "smart_view_remove", n) != 0 || !r.recorded(t, n, true) {
			t.Fatal("an aborted removal was logged or dropped the record")
		}
		mustOK(t, r.rmdir(t, "mgr1", r.recents, n, "Abort", r.rmdirCmds(t, r.recents, n, "Abort")), "retry")
		if r.entryExists(t, r.recents, "Abort") || r.recorded(t, n, false) {
			t.Fatal("the retried removal did not commit or kept the record")
		}
	})
}

func TestSmartViewRemovalRefusals(t *testing.T) {
	r := startSmartViewRig(t, svRoots)
	mgr := r.mgr

	t.Run("not_empty", func(t *testing.T) {
		n := r.view(t, r.recents, "Full")
		f := r.next + 500
		mustOK(t, txn(mgr,
			[]string{"SET", "i" + inoStr(f), mkAttr(typeFile, n)},
			[]string{"SET", "i" + inoStr(n), svBump(r.get(t, "i"+inoStr(n)), 0)},
			[]string{"HSET", "d" + inoStr(n), "clip.mov", mkEntry(typeFile, f)},
			[]string{"INCRBY", "usedSpace", "0"},
			[]string{"INCR", "totalInodes"}), "file in the view")
		r.wantRefused(t, r.rmdir(t, "mgr1", r.recents, n, "Full", r.rmdirCmds(t, r.recents, n, "Full")), r.recents, n, "Full", "non-empty view", "directory "+inoStr(n)+" is not empty")
	})
	t.Run("smart_root_itself", func(t *testing.T) {
		// The root is directly under the volume root, not under a smart root.
		r.wantRefused(t, r.rmdir(t, "mgr1", 1, r.recents, "Recents", r.rmdirCmds(t, 1, r.recents, "Recents")), 1, r.recents, "Recents", "removing a smart root", "directory 1 is not a smart-folder root")
	})
	t.Run("not_a_smart_root", func(t *testing.T) {
		n := r.view(t, r.other, "Shoot")
		r.wantRefused(t, r.rmdir(t, "mgr1", r.other, n, "Shoot", r.rmdirCmds(t, r.other, n, "Shoot")), r.other, n, "Shoot", "view under a non-smart root", "is not a smart-folder root")
	})
	t.Run("mac_made_root", func(t *testing.T) {
		// A top-level Queries made by a Mac is not the Manager's root: a
		// folder the Manager makes there is no view.
		n := r.view(t, r.queries, "Shoot")
		if r.recorded(t, n, false) {
			t.Fatal("a folder under a Mac's root was recorded as a view")
		}
		r.wantRefused(t, r.rmdir(t, "mgr1", r.queries, n, "Shoot", r.rmdirCmds(t, r.queries, n, "Shoot")), r.queries, n, "Shoot", "rmdir under a Mac-made root", "smart-folder root Queries: inode "+inoStr(r.queries)+" was not created by mgr1")
	})
	t.Run("moved_in", func(t *testing.T) {
		// An empty folder the manager principal made elsewhere (as a
		// migration does), moved into the Manager's root and given the
		// view mode: not a recorded view.
		n := r.mkdir(t, mgr, 3, "migrated")
		a := []byte(svAttr(typeDir, r.colls, 2))
		binary.BigEndian.PutUint16(a[1:3], uint16(typeDir)<<12|0o705)
		mustOK(t, txn(mgr,
			[]string{"HDEL", "d3", "migrated"},
			[]string{"HSET", "d" + inoStr(r.colls), "migrated", mkEntry(typeDir, n)},
			[]string{"SET", "i" + inoStr(n), string(a)},
			[]string{"SET", "i3", svBump(mkAttr(typeDir, 2), 0)},
			[]string{"SET", "i" + inoStr(r.colls), svBump(r.get(t, "i"+inoStr(r.colls)), 1)}), "move the folder into Collections")
		if r.recorded(t, n, false) {
			t.Fatal("a moved-in folder was recorded as a view")
		}
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "migrated", r.rmdirCmds(t, r.colls, n, "migrated")), r.colls, n, "migrated", "rmdir of a moved-in folder", "is not a recorded smart-folder view")
	})
	t.Run("not_the_manager", func(t *testing.T) {
		n := r.view(t, r.recents, "Theirs")
		for _, who := range []string{"mac1", "farm1"} {
			r.wantRefused(t, r.rmdir(t, who, r.recents, n, "Theirs", r.rmdirCmds(t, r.recents, n, "Theirs")), r.recents, n, "Theirs", who+" rmdir of a view", "above the project depth needs admin")
		}
	})
	t.Run("not_created_by_the_manager", func(t *testing.T) {
		n := r.mkdir(t, r.mac, r.colls, "MacMade")
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "MacMade", r.rmdirCmds(t, r.colls, n, "MacMade")), r.colls, n, "MacMade", "rmdir of a Mac's empty folder", "was not created by mgr1")
	})
	t.Run("touches_other_keys", func(t *testing.T) {
		n := r.view(t, r.colls, "Extra")
		for _, extra := range [][]string{
			{"HSET", "x10", "user.k", "v"},                          // another project's key
			{"SET", "i" + inoStr(r.recents), svAttr(typeDir, 1, 2)}, // another root's attr
			{"DEL", "x" + inoStr(n)},                                // a command twice
		} {
			cmds := append(r.rmdirCmds(t, r.colls, n, "Extra"), extra)
			r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "Extra", cmds), r.colls, n, "Extra", "rmdir plus "+strings.Join(extra[:2], " "), "is not exactly an rmdir")
		}
		cmds := r.rmdirCmds(t, r.colls, n, "Extra")
		cmds = append(cmds[:5:5], cmds[6:]...)
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "Extra", cmds), r.colls, n, "Extra", "rmdir without DECR totalInodes", "is not exactly an rmdir")
		cmds = r.rmdirCmds(t, r.colls, n, "Extra")
		b := []byte(cmds[1][2])
		b[2] ^= 0o002 // mode
		cmds[1][2] = string(b)
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "Extra", cmds), r.colls, n, "Extra", "rmdir that changes the root's mode", "changes more than its link count and times")
		// Times far from the server's clock.
		cmds = r.rmdirCmds(t, r.colls, n, "Extra")
		b = []byte(cmds[1][2])
		binary.BigEndian.PutUint64(b[23:31], 1700000000)
		cmds[1][2] = string(b)
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "Extra", cmds), r.colls, n, "Extra", "rmdir with a stale mtime", "its times are not the server's current time")
	})
	t.Run("not_watched", func(t *testing.T) {
		n := r.view(t, r.recents, "NoWatch")
		r.markLog(t)
		rc := r.conn(t, "mgr1")
		mustOK(t, rc.do("WATCH", "i"+inoStr(r.recents), "d"+inoStr(r.recents)), "WATCH parent")
		r.wantRefused(t, txn(rc, r.rmdirCmds(t, r.recents, n, "NoWatch")...), r.recents, n, "NoWatch", "rmdir without WATCH of the directory", "is not WATCHed")
	})
	t.Run("file_attr", func(t *testing.T) {
		// The entry says directory, the inode is a file.
		n := r.view(t, r.recents, "Odd")
		if err := r.db.Set(r.ctx, "i"+inoStr(n), svAttr(typeFile, r.recents, 1), 0).Err(); err != nil {
			t.Fatal(err)
		}
		r.wantRefused(t, r.rmdir(t, "mgr1", r.recents, n, "Odd", r.rmdirCmds(t, r.recents, n, "Odd")), r.recents, n, "Odd", "rmdir of an inode that is not a directory", "is not a directory in "+inoStr(r.recents))
	})
	t.Run("rename_aside", func(t *testing.T) {
		// Moving the view to a hidden name stays admin-only.
		n := r.view(t, r.colls, "Aside")
		r.markLog(t)
		v := txn(mgr, []string{"HDEL", "d" + inoStr(r.colls), "Aside"},
			[]string{"HSET", "d" + inoStr(r.colls), ".juicemount-smart-folder.tmp-1", mkEntry(typeDir, n)})
		r.wantRefused(t, v, r.colls, n, "Aside", "rename of a view to a hidden name", "is not exactly an rmdir")
	})
	t.Run("renamed_away_root", func(t *testing.T) {
		n := r.view(t, r.colls, "Moved")
		admin := r.conn(t, "admin1")
		mustOK(t, txn(admin, []string{"HDEL", "d1", "Collections"}, []string{"HSET", "d1", "OldCollections", mkEntry(typeDir, r.colls)}), "admin renames the root")
		r.wantRefused(t, r.rmdir(t, "mgr1", r.colls, n, "Moved", r.rmdirCmds(t, r.colls, n, "Moved")), r.colls, n, "Moved", "rmdir under a renamed-away smart root", "directory "+inoStr(r.colls)+" is not a smart-folder root")
	})
}

func TestSmartViewRemovalOffWithoutRoots(t *testing.T) {
	r := startSmartViewRig(t, nil)
	n := r.mkdir(t, r.mgr, r.recents, "Shoot")
	if r.recorded(t, n, false) {
		t.Fatal("recorded a view with no smart roots configured")
	}
	r.wantRefused(t, r.rmdir(t, "mgr1", r.recents, n, "Shoot", r.rmdirCmds(t, r.recents, n, "Shoot")), r.recents, n, "Shoot", "rmdir with no smart roots configured", "no smart-folder roots configured")
}

func TestCleanSmartRoots(t *testing.T) {
	if ok, bad := CleanSmartRoots(svRoots); len(bad) != 0 || strings.Join(ok, ",") != "Recents,Collections,Queries" {
		t.Fatalf("roots: %q %v", ok, bad)
	}
	ok, bad := CleanSmartRoots([]string{"", "a/b", ".trash", ".hidden", "..", "Recents", "Recents", "Views"})
	if len(bad) != 5 || strings.Join(ok, ",") != "Recents,Views" {
		t.Fatalf("CleanSmartRoots = %q, %v", ok, bad)
	}
}
