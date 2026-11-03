package respproxy

// Records that bg's routine cleanup acts on must stay inside what the
// writer may write. bg deletes everything below an expired hourly trash
// entry (CleanupTrashBefore) and every inode in a stale session's sustained
// set (CleanStaleSessions -> doDeleteSustainedInode), with bg's rights. These
// tests run the vendored JuiceFS meta client through the real proxy on
// Redis in enforce mode: mac1 holds project A (3), mac2 holds project B (4).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

// cleanupScopeRig is buildTree's tree formatted as a JuiceFS volume (real
// attrs, a format, counters), with A leased to mac1 and B to mac2.
func cleanupScopeRig(t *testing.T, trashDays int) (*leaseRig, *redis.Client) {
	t.Helper()
	r := startLeaseRig(t, ModeEnforce)
	db := directLeaseView(t, r)
	ctx := context.Background()
	raw, _ := json.Marshal(meta.Format{Name: "cleanup-scope", UUID: "cleanup-scope-volume", Storage: "file", BlockSize: 4096, TrashDays: trashDays})
	pipe := db.Pipeline()
	pipe.Set(ctx, "setting", raw, 0)
	for n, p := range treeParent {
		pipe.Set(ctx, "i"+inoStr(n), (&meta.Attr{Typ: meta.TypeDirectory, Mode: 0777, Nlink: 2, Parent: meta.Ino(p), Length: 4096}).Marshal(), 0)
	}
	for n, p := range map[uint64]uint64{10: 3, 11: 4, 12: 5} {
		pipe.Set(ctx, "i"+inoStr(n), (&meta.Attr{Typ: meta.TypeFile, Mode: 0644, Nlink: 1, Parent: meta.Ino(p)}).Marshal(), 0)
	}
	if trashDays > 0 {
		pipe.Set(ctx, "i"+inoStr(TrashInode), (&meta.Attr{Typ: meta.TypeDirectory, Mode: 0555, Nlink: 2, Parent: 1, Length: 4096}).Marshal(), 0)
	}
	pipe.Set(ctx, "nextinode", "1000", 0)
	pipe.Set(ctx, "usedSpace", "1048576", 0)
	pipe.Set(ctx, "totalInodes", "100", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatal(err)
	}
	for who, proj := range map[string]string{"mac1": "ino:3", "mac2": "ino:4"} {
		if v := r.conn(t, who).do("JM.LEASE", "ACQUIRE", proj, "auto"); v.Type != '*' {
			t.Fatalf("%s grant %s: %+v", who, proj, v)
		}
	}
	return r, db
}

// scopeClient is a JuiceFS meta client through the proxy as principal who.
func scopeClient(t *testing.T, r *leaseRig, who string, mut func(*meta.Config)) meta.Meta {
	t.Helper()
	conf := meta.DefaultConf()
	conf.Retries = 0
	conf.MaxDeletes = 0
	conf.NoBGJob = true
	if mut != nil {
		mut(conf)
	}
	m := meta.NewClient("redis://"+who+":"+passOf(who)+"@"+r.tp.addr+"/15", conf)
	t.Cleanup(func() { _ = m.Shutdown() })
	if _, err := m.Load(false); err != nil {
		t.Fatal(err)
	}
	return m
}

// noRefusals fails if the proxy refused (or would have refused) anything
// principal who sent.
func noRefusals(t *testing.T, r *leaseRig, who, what string) {
	t.Helper()
	for _, d := range r.tp.Stats.Snapshot().Decisions {
		if d.Outcome != DecAllow && d.Principal == who {
			t.Fatalf("%s: legitimate operation refused: %+v", what, d)
		}
	}
}

// wantDeny records a refusal that did not happen and goes on, so the test
// still shows what bg's cleanup then does to project B.
func wantDeny(t *testing.T, v Value, reason Reason, what string) {
	t.Helper()
	if !isDeny(v, reason) {
		t.Errorf("%s: got %c %q, want -JMDENY %s", what, v.Type, v.Str, reason)
	}
}

// projectBIntact checks that B (4) and its file g (11) are where buildTree
// put them.
func projectBIntact(t *testing.T, db *redis.Client) {
	t.Helper()
	ctx := context.Background()
	if db.HGet(ctx, "d2", "B").Val() != mkEntry(typeDir, 4) || db.Exists(ctx, "i4").Val() != 1 ||
		db.HGet(ctx, "d4", "g").Val() != mkEntry(typeFile, 11) || db.Exists(ctx, "i11").Val() != 1 {
		t.Fatalf("project B was changed: d2.B=%q i4=%d d4=%v i11=%d", db.HGet(ctx, "d2", "B").Val(),
			db.Exists(ctx, "i4").Val(), db.HKeys(ctx, "d4").Val(), db.Exists(ctx, "i11").Val())
	}
	for _, m := range db.ZRange(ctx, "delfiles", 0, -1).Val() {
		if m == "11" || len(m) > 3 && m[:3] == "11:" {
			t.Fatalf("project B's file was queued for deletion: %q", m)
		}
	}
}

// A Mac with only project A cannot name B's directory (or anything but the
// fresh directory its own INCR nextTrash allocated) as an hourly trash
// entry, so bg's trash cleanup leaves B alone. juicefs's own first unlink
// into the trash, which creates the hour's directory, still passes.
func TestCleanupScopeTrashRootEntry(t *testing.T) {
	r, db := cleanupScopeRig(t, 1)
	ctx := context.Background()
	root := "d" + inoStr(TrashInode)
	// The real flow: mac1 unlinks A/x; checkTrash creates the hour.
	mac := scopeClient(t, r, "mac1", nil)
	var ino meta.Ino
	var attr meta.Attr
	if st := mac.Create(meta.Background(), 3, "x.mov", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := mac.Unlink(meta.Background(), 3, "x.mov"); st != 0 {
		t.Fatalf("unlink into trash: %v", st)
	}
	hours := db.HGetAll(ctx, root).Val()
	if len(hours) != 1 {
		t.Fatalf("trash root after the first unlink: %v", hours)
	}
	var hour string
	var hourIno uint64
	for name, e := range hours {
		hour = name
		hourIno, _ = entryInode([]byte(e))
	}
	if _, err := time.Parse(trashHourLayout, hour); err != nil || hourIno <= TrashInode {
		t.Fatalf("hourly directory %q -> %d", hour, hourIno)
	}
	if n := db.HLen(ctx, "d"+inoStr(hourIno)).Val(); n != 1 {
		t.Fatalf("unlinked file not in the hour's directory (%d entries)", n)
	}
	noRefusals(t, r, "mac1", "unlink into trash creating the hour")

	dirAttr := string((&meta.Attr{Typ: meta.TypeDirectory, Mode: 0555, Nlink: 2, Parent: meta.Ino(TrashInode), Length: 4096}).Marshal())
	// Each attempt uses its own expired name, so none masks another.
	old := func(i int) string { return "2000-01-01-0" + strconv.Itoa(i) }
	rc := r.conn(t, "mac1")
	// The bare entry: B's directory as an old hour.
	wantDeny(t, txn(rc, []string{"HSET", root, old(0), mkEntry(typeDir, 4)}), ReasonBGOnly, "trash root entry naming B")
	// The same with a fresh inode created alongside, as juicefs's mknod.
	alloc := func(rc *rawConn) uint64 {
		t.Helper()
		mustOK(t, rc.do("WATCH", "i"+inoStr(TrashInode), root), "WATCH")
		v := rc.do("INCR", "nextTrash")
		if v.Type != ':' {
			t.Fatalf("INCR nextTrash: %+v", v)
		}
		return uint64(v.Int)
	}
	n := alloc(rc)
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(TrashInode+n), dirAttr}, []string{"HSET", root, old(1), mkEntry(typeDir, 4)}),
		ReasonBGOnly, "mknod-shaped trash root entry naming B")
	// The fresh inode, but over an hour that exists (would orphan it).
	n = alloc(rc)
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(TrashInode+n), dirAttr}, []string{"HSET", root, hour, mkEntry(typeDir, TrashInode+n)}),
		ReasonBGOnly, "trash root entry replacing an existing hour")
	// The existing hour's directory under a second, old name.
	alloc(rc)
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(hourIno), dirAttr}, []string{"HSET", root, old(2), mkEntry(typeDir, hourIno)}),
		ReasonBGOnly, "trash root entry naming the current hour's directory")
	// A read-only principal writes no trash at all.
	dav := r.conn(t, "dav1")
	n = alloc(dav)
	wantDeny(t, txn(dav, []string{"SET", "i" + inoStr(TrashInode+n), dirAttr}, []string{"HSET", root, old(3), mkEntry(typeDir, TrashInode+n)}),
		ReasonPrincipalReadonly, "webdav trash directory")
	if db.HLen(ctx, root).Val() != 1 || db.HGet(ctx, root, hour).Val() != mkEntry(typeDir, hourIno) {
		t.Errorf("a refused trash root write landed: %v", db.HGetAll(ctx, root).Val())
	}

	// bg's routine cleanup of every hour before now.
	bg := scopeClient(t, r, "bg1", nil)
	bg.CleanupTrashBefore(meta.Background(), time.Now().Add(time.Hour), nil)
	if db.HExists(ctx, root, hour).Val() {
		t.Fatal("bg trash cleanup did not run (the hour's directory is still there)")
	}
	projectBIntact(t, db)

	// Two mounts at the turn of an hour: the other one's entry lands after
	// this txn's WATCH. Redis aborts the EXEC (juicefs then retries and
	// finds the hour); the proxy must not turn that into a refusal.
	n = alloc(rc)
	next := time.Now().UTC().Add(time.Hour).Format(trashHourLayout)
	other := mkEntry(typeDir, TrashInode+1000)
	db.HSet(ctx, root, next, other)
	v := txn(rc, []string{"SET", "i" + inoStr(TrashInode+n), dirAttr}, []string{"HSET", root, next, mkEntry(typeDir, TrashInode+n)})
	if v.Type != '*' || !v.Null || db.HGet(ctx, root, next).Val() != other {
		t.Fatalf("racing hour create: EXEC %c %q null=%v, entry %q", v.Type, v.Str, v.Null, db.HGet(ctx, root, next).Val())
	}
}

// A Mac's sustained-inode set (session<sid>) holds only inodes its own
// unlink-while-open txns wrote, and its heartbeat cannot be set into the
// past, so bg's stale-session cleanup deletes nothing of project B. The
// real heartbeat and an unlink of an open file still pass.
func TestCleanupScopeSustainedSet(t *testing.T) {
	r, db := cleanupScopeRig(t, 0)
	ctx := context.Background()
	// session mounts mac1 with a juicefs session. The id is allocated
	// through the proxy as juicefs's own NewSession does (INCRBY
	// nextsession 1) and handed in as Config.Sid, because the vendored
	// NewSession writes m.sid after starting its heartbeat goroutine, which
	// reads it unsynchronised (a race the race detector reports).
	session := func(heartbeat time.Duration) (meta.Meta, string) {
		t.Helper()
		v := r.conn(t, "mac1").do("INCRBY", "nextsession", "1")
		if v.Type != ':' {
			t.Fatalf("INCRBY nextsession: %+v", v)
		}
		m := scopeClient(t, r, "mac1", func(c *meta.Config) { c.Heartbeat, c.Sid = heartbeat, uint64(v.Int) })
		if err := m.NewSession(true); err != nil {
			t.Fatal(err)
		}
		sid := strconv.FormatInt(v.Int, 10)
		if db.ZScore(ctx, "allSessions", sid).Err() != nil {
			t.Fatalf("session %s not registered", sid)
		}
		return m, sid
	}

	// The real heartbeat (doRefreshSession every second) moves the score on.
	hb, hbSid := session(time.Second)
	first := db.ZScore(ctx, "allSessions", hbSid).Val()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && db.ZScore(ctx, "allSessions", hbSid).Val() <= first {
		time.Sleep(100 * time.Millisecond)
	}
	if db.ZScore(ctx, "allSessions", hbSid).Val() <= first {
		t.Fatal("the session heartbeat did not refresh the score")
	}

	// A mount that will crash: no heartbeat, so only this test moves its
	// score. Unlink of an open file puts the inode in its sustained set.
	mac, sid := session(0)
	var ino meta.Ino
	var attr meta.Attr
	if st := mac.Create(meta.Background(), 3, "open.mov", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := mac.Unlink(meta.Background(), 3, "open.mov"); st != 0 {
		t.Fatalf("unlink while open: %v", st)
	}
	open := strconv.FormatUint(uint64(ino), 10)
	if !db.SIsMember(ctx, "session"+sid, open).Val() {
		t.Fatalf("open unlinked inode %s is not in session%s: %v", open, sid, db.SMembers(ctx, "session"+sid).Val())
	}
	noRefusals(t, r, "mac1", "heartbeat and unlink of an open file")
	if err := hb.CloseSession(); err != nil {
		t.Fatal(err)
	}

	rc := r.conn(t, "mac1")
	wantDeny(t, rc.do("COPY", "locked"+sid, "session"+sid, "REPLACE"), ReasonBGOnly, "COPY into the sustained set")
	wantDeny(t, rc.do("SADD", "session"+sid, "11"), ReasonBGOnly, "bare SADD of B's inode")
	wantDeny(t, txn(rc, []string{"SET", "i10", string((&meta.Attr{Typ: meta.TypeFile, Mode: 0644, Nlink: 1, Parent: 3}).Marshal())},
		[]string{"SADD", "session" + sid, "11"}), ReasonBGOnly, "SADD of B's inode next to a write of A's")
	wantDeny(t, txn(rc, []string{"HSET", "lockf10", sid + "_0000000000000001", "W"}, []string{"SADD", "locked" + sid, "d4"}),
		ReasonBGOnly, "locked set naming B's directory hash")
	wantDeny(t, rc.do("ZADD", "allSessions", "1", sid), ReasonBGOnly, "heartbeat into the past")
	wantDeny(t, rc.do("ZADD", "allSessions", "LT", "1", sid), ReasonBGOnly, "heartbeat with options")
	wantDeny(t, rc.do("ZADD", "sessions", "1", sid), ReasonBGOnly, "legacy session index")
	stored := db.ZScore(ctx, "allSessions", sid).Val()
	mustOK(t, rc.do("ZADD", "allSessions", strconv.FormatInt(time.Now().Unix()+2, 10), sid), "a heartbeat below the stored score")
	if got := db.ZScore(ctx, "allSessions", sid).Val(); got != stored {
		t.Errorf("a heartbeat lowered the stored score from %v to %v", stored, got)
	}
	if db.SIsMember(ctx, "session"+sid, "11").Val() || db.SIsMember(ctx, "locked"+sid, "d4").Val() {
		t.Error("a refused session set member landed")
	}

	// Time passes the crashed mount's score; bg's routine cleanup runs.
	db.ZAdd(ctx, "allSessions", redis.Z{Score: 1, Member: sid})
	bg := scopeClient(t, r, "bg1", nil)
	bg.CleanStaleSessions(meta.Background())
	if db.ZScore(ctx, "allSessions", sid).Err() != redis.Nil || db.Exists(ctx, "i"+open).Val() != 0 {
		t.Fatal("bg stale-session cleanup did not run (session or its sustained inode is still there)")
	}
	projectBIntact(t, db)
}

// An entry for an inode that does not exist yet must create it, from the
// creator's own nextinode batch. So a Mac cannot plant an entry for (or
// create) the inode another project's mount is about to create, and bg's
// trash cleanup of the Mac's directory leaves that file alone. juicefs's
// own create, mkdir, mknod, symlink, link, rename and file clone pass, and
// a hard link still needs write access to its target's project.
func TestCleanupScopeNewInodeFromOwnBatch(t *testing.T) {
	r, db := cleanupScopeRig(t, 1)
	ctx := context.Background()
	root := "d" + inoStr(TrashInode)
	rc := r.conn(t, "mac1")
	fileAttr := func(parent uint64) string {
		return string((&meta.Attr{Typ: meta.TypeFile, Mode: 0644, Nlink: 1, Parent: meta.Ino(parent)}).Marshal())
	}
	// nextinode is 1000, so the next batch is 1001..2024 and its first
	// create is 1001: B's mount takes that batch below.
	const next = 1001
	wantDeny(t, txn(rc, []string{"HSET", "d5", "trap", mkEntry(typeFile, next)}), ReasonUnknownKey, "entry for an inode the unit does not create")
	wantDeny(t, create(rc, 5, next, "trap"), ReasonAdminOnly, "create of an inode from no batch of mac1's")
	if db.HExists(ctx, "d5", "trap").Val() || db.Exists(ctx, "i"+inoStr(next)).Val() != 0 {
		t.Fatal("a refused entry or inode landed")
	}

	// B's mount creates its file: inode 1001.
	mb := scopeClient(t, r, "mac2", nil)
	var bIno meta.Ino
	var attr meta.Attr
	if st := mb.Create(meta.Background(), 4, "b.mov", 0644, 0, 0, &bIno, &attr); st != 0 || bIno != next {
		t.Fatalf("B create: %v, inode %d (want %d)", st, bIno, next)
	}
	noRefusals(t, r, "mac2", "B's create")

	// mac1's own batch (2025..3048) creates; B's batch and odd shapes do not.
	lo, _ := allocInodes(t, rc)
	mustOK(t, create(rc, 5, lo, "mine"), "create from mac1's own batch")
	wantDeny(t, create(rc, 5, next+1, "theirs"), ReasonAdminOnly, "create from B's batch")
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(lo+1), fileAttr(3)}, []string{"HSET", "d5", "x", mkEntry(typeFile, lo+1)}),
		ReasonUnknownKey, "new inode whose Attr.Parent is another directory")
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(lo+1), mkAttr(typeDir, 5)}, []string{"HSET", "d5", "x", mkEntry(typeFile, lo+1)}),
		ReasonUnknownKey, "new inode whose attr and entry differ in type")
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(lo+1), fileAttr(5)}, []string{"HSET", "d5", "x", mkEntry(typeFile, lo+1)},
		[]string{"HSET", "d3", "y", mkEntry(typeFile, lo+1)}), ReasonUnknownKey, "new inode in two directories")
	v := rc.do("INCRBY", "nextinode", "1")
	if v.Type != ':' {
		t.Fatalf("INCRBY nextinode 1: %c %q", v.Type, v.Str)
	}
	wantDeny(t, create(rc, 5, uint64(v.Int), "small"), ReasonAdminOnly, "create from a step smaller than juicefs's batch")
	// A hard link to B's file needs B.
	wantDeny(t, txn(rc, []string{"SET", "i" + inoStr(next), string((&meta.Attr{Typ: meta.TypeFile, Mode: 0644, Nlink: 2}).Marshal())},
		[]string{"HSET", "d5", "link", mkEntry(typeFile, next)}, []string{"HINCRBY", "p" + inoStr(next), "5", "1"},
		[]string{"HINCRBY", "p" + inoStr(next), "4", "1"}), ReasonXProject, "hard link to B's file")

	// juicefs's own create paths, through mac1's mount.
	ma := scopeClient(t, r, "mac1", nil)
	var dir, file, ino meta.Ino
	if st := ma.Mkdir(meta.Background(), 3, "new", 0755, 0, 0, &dir, &attr); st != 0 {
		t.Fatalf("mkdir: %v", st)
	}
	if st := ma.Create(meta.Background(), dir, "f.mov", 0644, 0, 0, &file, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := ma.Mknod(meta.Background(), dir, "fifo", meta.TypeFIFO, 0644, 0, 0, "", &ino, &attr); st != 0 {
		t.Fatalf("mknod: %v", st)
	}
	if st := ma.Symlink(meta.Background(), dir, "sl", "f.mov", &ino, &attr); st != 0 {
		t.Fatalf("symlink: %v", st)
	}
	if st := ma.Link(meta.Background(), file, 3, "f-link.mov", &attr); st != 0 {
		t.Fatalf("link: %v", st)
	}
	if st := ma.Rename(meta.Background(), dir, "f.mov", 5, "moved.mov", 0, &ino, &attr); st != 0 {
		t.Fatalf("rename: %v", st)
	}
	var count, total uint64
	if st := ma.Clone(meta.Background(), 5, file, dir, "copy.mov", 0, 0, &count, &total); st != 0 {
		t.Fatalf("clone: %v", st)
	}
	// A planted directory moved into the trash takes only its own entries.
	if st := ma.Unlink(meta.Background(), 3, "f"); st != 0 {
		t.Fatalf("unlink into trash: %v", st)
	}
	var hourIno uint64
	for _, e := range db.HGetAll(ctx, root).Val() {
		hourIno, _ = entryInode([]byte(e))
	}
	wantDeny(t, txn(rc, []string{"HSET", "d3", "peek", mkEntry(typeDir, hourIno)}), ReasonBGOnly, "project entry naming the hour's directory")
	mustOK(t, txn(rc, []string{"HDEL", "d3", "sub"},
		[]string{"HSET", "d" + inoStr(hourIno), "3-5-sub", mkEntry(typeDir, 5)},
		[]string{"SET", "i5", string((&meta.Attr{Typ: meta.TypeDirectory, Mode: 0777, Nlink: 2, Parent: meta.Ino(hourIno), Length: 4096}).Marshal())}),
		"move A/sub into the trash")
	bg := scopeClient(t, r, "bg1", nil)
	bg.CleanupTrashBefore(meta.Background(), time.Now().Add(time.Hour), nil)
	if db.Exists(ctx, "i5").Val() != 0 {
		t.Fatal("bg trash cleanup did not run (A/sub is still there)")
	}
	if db.HGet(ctx, "d4", "b.mov").Val() != mkEntry(typeFile, next) || db.Exists(ctx, "i"+inoStr(next)).Val() != 1 {
		t.Fatalf("B's file was changed: d4=%v i%d=%d", db.HKeys(ctx, "d4").Val(), next, db.Exists(ctx, "i"+inoStr(next)).Val())
	}
	projectBIntact(t, db)
}

// The trash root takes only checkTrash's new hourly directory, also in a txn
// that writes a project dentry, and a trash entry never replaces one that
// names another inode, so bg's trash cleanup still finds every trashed file.
func TestCleanupScopeTrashRootFromProjectTxn(t *testing.T) {
	r, db := cleanupScopeRig(t, 1)
	ctx := context.Background()
	root := "d" + inoStr(TrashInode)
	// B's mount unlinks B/g (11): checkTrash creates the hour.
	mb := scopeClient(t, r, "mac2", nil)
	if st := mb.Unlink(meta.Background(), 4, "g"); st != 0 {
		t.Fatalf("B unlink: %v", st)
	}
	noRefusals(t, r, "mac2", "B's unlink into the trash")
	hours := db.HGetAll(ctx, root).Val()
	if len(hours) != 1 {
		t.Fatalf("trash root: %v", hours)
	}
	var hour string
	var hourIno uint64
	for k, v := range hours {
		hour = k
		hourIno, _ = entryInode([]byte(v))
	}
	hourDir := "d" + inoStr(hourIno)
	trashed := db.HGetAll(ctx, hourDir).Val()

	rc := r.conn(t, "mac1")
	dirAttr := func(parent uint64) string {
		return string((&meta.Attr{Typ: meta.TypeDirectory, Mode: 0777, Nlink: 2, Parent: meta.Ino(parent), Length: 4096}).Marshal())
	}
	wantDeny(t, txn(rc, []string{"HDEL", "d3", "sub"}, []string{"HSET", root, hour, mkEntry(typeDir, 5)}),
		ReasonBGOnly, "project txn replacing the hour's entry")
	wantDeny(t, txn(rc, []string{"HDEL", "d3", "sub"}, []string{"SET", "i5", dirAttr(TrashInode)},
		[]string{"HSET", root, time.Now().UTC().Add(time.Hour).Format(trashHourLayout), mkEntry(typeDir, 5)}),
		ReasonBGOnly, "project directory moved into the trash root")
	// An unlink into the hour that reuses B's trash entry name.
	for name := range trashed {
		wantDeny(t, txn(rc, []string{"HDEL", "d3", "f"},
			[]string{"SET", "i10", string((&meta.Attr{Typ: meta.TypeFile, Mode: 0644, Nlink: 1, Parent: meta.Ino(hourIno)}).Marshal())},
			[]string{"HSET", hourDir, name, mkEntry(typeFile, 10)}), ReasonBGOnly, "trash entry over B's trashed file")
	}
	if got := db.HGetAll(ctx, root).Val(); len(got) != 1 || got[hour] != hours[hour] {
		t.Fatalf("trash root changed: %v", got)
	}
	if got := db.HGetAll(ctx, hourDir).Val(); fmt.Sprint(got) != fmt.Sprint(trashed) {
		t.Fatalf("hour's entries changed: %v (were %v)", got, trashed)
	}
	// A's own unlink into the same hour still passes.
	ma := scopeClient(t, r, "mac1", nil)
	if st := ma.Unlink(meta.Background(), 3, "f"); st != 0 {
		t.Fatalf("A unlink into the existing hour: %v", st)
	}
	if n := db.HLen(ctx, hourDir).Val(); n != int64(len(trashed))+1 {
		t.Fatalf("A's trashed file not in the hour (%d entries)", n)
	}
}

// fakeScopeSide answers the guard's trash-root HEXISTS, CLIENT LIST and
// TIME (an error when noTime). The unit's own upstream is client id 2,
// named "u1", dirty or not; client id 1 carries the same name (another
// proxy's connection) and is always dirty. CLIENT LIST ID lists only the
// ids asked for.
type fakeScopeSide struct {
	exists, dirty, noTime bool
	now                   int64
}

func (f fakeScopeSide) Do(db int, args ...[]byte) (Value, error) {
	switch string(args[0]) {
	case "HEXISTS":
		if f.exists {
			return Value{Type: ':', Int: 1}, nil
		}
		return Value{Type: ':'}, nil
	case "CLIENT":
		flags := "x"
		if f.dirty {
			flags = "xd"
		}
		lines := map[string]string{"1": "id=1 name=u1 flags=xd db=15\n", "2": "id=2 name=u1 flags=" + flags + " db=15\n"}
		out := lines["1"] + lines["2"]
		if len(args) > 3 && string(args[2]) == "ID" {
			out = ""
			for _, id := range args[3:] {
				out += lines[string(id)]
			}
		}
		return Value{Type: '$', Str: []byte(out)}, nil
	case "TIME":
		if f.noTime {
			return Value{}, errors.New("side connection down")
		}
		return Value{Type: '*', Elems: []Value{{Type: '$', Str: []byte(strconv.FormatInt(f.now, 10))}, {Type: '$', Str: []byte("0")}}}, nil
	}
	return Value{}, errors.New("unexpected side read " + string(args[0]))
}

func scopeUnit(kind UnitKind, alloc uint64, watched []string, cmds ...[]string) *Unit {
	u := &Unit{Kind: kind, Watched: watched, TrashAlloc: alloc, UpstreamName: "u1", upstreamID: func() uint64 { return 2 }, beforeSide: func() error { return nil }}
	for _, c := range cmds {
		args := bytesArgs(c...)
		u.Cmds = append(u.Cmds, Cmd{Args: args, Info: Classify(args)})
	}
	return u
}

// juicefs's hourly trash mknod is the only trash-root write a non-trusted
// principal may make without a non-trash dentry in the same txn.
func TestGuardHourlyTrashCreate(t *testing.T) {
	mac := Principal{Name: "mac1", Kind: KindMac}
	dav := Principal{Name: "dav1", Kind: KindWebDAV}
	bg := Principal{Name: "bg1", Kind: KindBG}
	root := "d" + inoStr(TrashInode)
	watched := []string{"i" + inoStr(TrashInode), root}
	const alloc = 5
	hourly := uint64(TrashInode + alloc)
	dirAttr := mkAttr(typeDir, TrashInode)
	mknod := func(name, entry, attr string) [][]string {
		f := inoStr(hourly)
		return [][]string{{"SET", "i" + f, attr}, {"HSET", root, name, entry},
			{"HSET", "dirUsedInodes", f, "0"}, {"HSET", "dirDataLength", f, "0"}, {"HSET", "dirUsedSpace", f, "0"},
			{"INCRBY", "usedSpace", "4096"}, {"INCR", "totalInodes"}}
	}
	good := mknod("2026-11-03-10", mkEntry(typeDir, hourly), dirAttr)
	// Redis TIME is 10:30 UTC; hours within trashHourSkew of it pass.
	now := time.Date(2026, 11, 3, 10, 30, 0, 0, time.UTC).Unix()
	with := func(extra ...[]string) [][]string { return append(append([][]string{}, good...), extra...) }
	cases := []struct {
		name    string
		p       Principal
		alloc   uint64
		watched []string
		exists  bool
		cmds    [][]string
		want    Reason
	}{
		{"checkTrash mknod", mac, alloc, watched, false, good, ""},
		{"entry names a project directory", mac, alloc, watched, false, mknod("2026-11-03-10", mkEntry(typeDir, 4), dirAttr), ReasonBGOnly},
		{"entry names a file", mac, alloc, watched, false, mknod("2026-11-03-10", mkEntry(typeFile, hourly), dirAttr), ReasonBGOnly},
		{"entry names another trash inode", mac, alloc + 1, watched, false, good, ReasonBGOnly},
		{"no INCR nextTrash on this connection", mac, 0, watched, false, good, ReasonBGOnly},
		{"name is not an hour", mac, alloc, watched, false, mknod("2026100818", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"name has a suffix", mac, alloc, watched, false, mknod("2026-11-03-10/x", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"attr is not a directory in the trash root", mac, alloc, watched, false, mknod("2026-11-03-10", mkEntry(typeDir, hourly), mkAttr(typeDir, 1)), ReasonBGOnly},
		{"inode not created", mac, alloc, watched, false, [][]string{{"HSET", root, "2026-11-03-10", mkEntry(typeDir, hourly)}}, ReasonBGOnly},
		{"hour exists", mac, alloc, watched, true, good, ReasonBGOnly},
		{"trash root not WATCHed", mac, alloc, []string{"i" + inoStr(TrashInode)}, false, good, ReasonBGOnly},
		{"second trash root write", mac, alloc, watched, false, with([]string{"HDEL", root, "2026-11-01-00"}), ReasonBGOnly},
		{"two fields", mac, alloc, watched, false, [][]string{{"SET", "i" + inoStr(hourly), dirAttr},
			{"HSET", root, "2026-11-03-10", mkEntry(typeDir, hourly), "2000-01-01-00", mkEntry(typeDir, 4)}}, ReasonBGOnly},
		{"another trash inode written", mac, alloc, watched, false, with([]string{"SET", "i" + inoStr(TrashInode+9), dirAttr}), ReasonBGOnly},
		{"webdav", dav, alloc, watched, false, good, ReasonPrincipalReadonly},
		{"previous hour (clock behind)", mac, alloc, watched, false, mknod("2026-11-03-09", mkEntry(typeDir, hourly), dirAttr), ""},
		{"next hour (clock ahead)", mac, alloc, watched, false, mknod("2026-11-03-11", mkEntry(typeDir, hourly), dirAttr), ""},
		{"an expired hour", mac, alloc, watched, false, mknod("2000-01-01-00", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"two hours behind", mac, alloc, watched, false, mknod("2026-11-03-08", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"two hours ahead", mac, alloc, watched, false, mknod("2026-11-03-12", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"a far future hour", mac, alloc, watched, false, mknod("2099-01-01-00", mkEntry(typeDir, hourly), dirAttr), ReasonBGOnly},
		{"bg is trusted", bg, 0, nil, true, [][]string{{"HSET", root, "x", "y"}, {"SET", "i" + inoStr(hourly), "a"}}, ""},
	}
	for _, c := range cases {
		g := NewGuard(NewMemSessionStore(), fakeChunks{})
		g.side = fakeScopeSide{exists: c.exists, now: now}
		u := scopeUnit(UnitTxn, c.alloc, c.watched, c.cmds...)
		d, _ := g.Check(c.p, u)
		switch {
		case c.want == "" && d != nil:
			t.Errorf("%s: refused %s (%s)", c.name, d.Reason, d.Detail)
		case c.want != "" && (d == nil || d.Reason != c.want):
			t.Errorf("%s: got %+v, want %s", c.name, d, c.want)
		}
	}
	// Without Redis TIME the hour cannot be checked: refused.
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	g.side = fakeScopeSide{noTime: true}
	if d, _ := g.Check(mac, scopeUnit(UnitTxn, alloc, watched, good...)); d == nil || d.Reason != ReasonBGOnly {
		t.Errorf("hour create without Redis TIME: got %+v, want BG_ONLY", d)
	}
	// Two mounts at the turn of the hour: the other created it after this
	// txn's WATCH, so Redis aborts this EXEC and juicefs retries.
	g = NewGuard(NewMemSessionStore(), fakeChunks{})
	g.side = fakeScopeSide{exists: true, dirty: true, now: now}
	if d, _ := g.Check(mac, scopeUnit(UnitTxn, alloc, watched, good...)); d != nil {
		t.Errorf("racing hour create refused instead of left to the WATCH: %s", d.Detail)
	}
}

// The dirty flag that excuses an existing hour is read for the unit's own
// upstream connection by its CLIENT ID: a connection elsewhere with the same
// name (another proxy process names its upstreams the same way) whose WATCH
// failed says nothing about this txn.
func TestGuardHourlyTrashCreateOwnConnection(t *testing.T) {
	mac := Principal{Name: "mac1", Kind: KindMac}
	root := "d" + inoStr(TrashInode)
	hourly := uint64(TrashInode + 5)
	now := time.Date(2026, 11, 3, 10, 30, 0, 0, time.UTC).Unix()
	cmds := [][]string{{"SET", "i" + inoStr(hourly), mkAttr(typeDir, TrashInode)}, {"HSET", root, "2026-11-03-10", mkEntry(typeDir, hourly)}}
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	g.side = fakeScopeSide{exists: true, now: now}
	if d, _ := g.Check(mac, scopeUnit(UnitTxn, 5, []string{root}, cmds...)); d == nil || d.Reason != ReasonBGOnly {
		t.Errorf("existing hour replaced on a same-named connection's dirty flag: got %+v, want BG_ONLY", d)
	}
	u := scopeUnit(UnitTxn, 5, []string{root}, cmds...)
	u.upstreamID = func() uint64 { return 0 }
	g.side = fakeScopeSide{exists: true, dirty: true, now: now}
	if d, _ := g.Check(mac, u); d == nil || d.Reason != ReasonBGOnly {
		t.Errorf("existing hour without the upstream's client id: got %+v, want BG_ONLY", d)
	}
}

// session<sid> and locked<sid> members are anchored to the txn that adds
// them, and a heartbeat score must be ahead of Redis TIME and never lowers
// the stored one.
func TestGuardSessionSetsAndHeartbeat(t *testing.T) {
	mac := Principal{Name: "mac1", Kind: KindMac}
	bg := Principal{Name: "bg1", Kind: KindBG}
	const now = 1_800_000_000
	run := func(g *Guard, p Principal, kind UnitKind, cmds ...[]string) (*Denial, *Unit) {
		u := scopeUnit(kind, 0, nil, cmds...)
		d, _ := g.Check(p, u)
		return d, u
	}
	store := NewMemSessionStore()
	store.Claim(7, "mac1")
	g := NewGuard(store, fakeChunks{})
	g.side = fakeScopeSide{now: now}
	future, past := strconv.Itoa(now+60), strconv.Itoa(now)
	cases := []struct {
		name string
		p    Principal
		kind UnitKind
		cmds [][]string
		want Reason
	}{
		{"unlink of an open file", mac, UnitTxn, [][]string{{"HDEL", "d5", "f"}, {"SET", "i9", "a"}, {"SADD", "session7", "9"}}, ""},
		{"bare sustained SADD", mac, UnitBare, [][]string{{"SADD", "session7", "9"}}, ReasonBGOnly},
		{"sustained inode the txn does not write", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"SADD", "session7", "9", "11"}}, ReasonBGOnly},
		{"sustained inode only touched", mac, UnitTxn, [][]string{{"HSET", "x11", "k", "v"}, {"SADD", "session7", "11"}}, ReasonBGOnly},
		{"non-canonical member", mac, UnitTxn, [][]string{{"SET", "i9", "a"}, {"SADD", "session7", "09"}}, ReasonBGOnly},
		{"sustained SREM", mac, UnitTxn, [][]string{{"DEL", "i9"}, {"SREM", "session7", "9"}}, ""},
		{"DEL sustained set", mac, UnitBare, [][]string{{"DEL", "session7"}}, ReasonBGOnly},
		{"RENAME into the sustained set", mac, UnitBare, [][]string{{"RENAME", "locked7", "session7"}}, ReasonBGOnly},
		{"SMOVE into the sustained set", mac, UnitBare, [][]string{{"SMOVE", "locked7", "session7", "11"}}, ReasonBGOnly},
		{"flock", mac, UnitTxn, [][]string{{"HSET", "lockf9", "7_1", "W"}, {"SADD", "locked7", "lockf9"}}, ""},
		{"locked member not a lock key", mac, UnitTxn, [][]string{{"HSET", "lockf9", "7_1", "W"}, {"SADD", "locked7", "d4"}}, ReasonBGOnly},
		{"locked member not written", mac, UnitTxn, [][]string{{"HSET", "lockf9", "7_1", "W"}, {"SADD", "locked7", "lockp11"}}, ReasonBGOnly},
		{"heartbeat", mac, UnitBare, [][]string{{"ZADD", "allSessions", future, "7"}}, ""},
		{"heartbeat at the server clock", mac, UnitBare, [][]string{{"ZADD", "allSessions", past, "7"}}, ReasonBGOnly},
		{"fractional heartbeat", mac, UnitBare, [][]string{{"ZADD", "allSessions", future + ".5", "7"}}, ReasonBGOnly},
		{"heartbeat with options", mac, UnitBare, [][]string{{"ZADD", "allSessions", "GT", future, "7"}}, ReasonBGOnly},
		{"heartbeat in a txn", mac, UnitTxn, [][]string{{"ZADD", "allSessions", future, "7"}}, ReasonBGOnly},
		{"legacy index", mac, UnitBare, [][]string{{"ZADD", "sessions", future, "7"}}, ReasonBGOnly},
		{"own session end", mac, UnitBare, [][]string{{"ZREM", "allSessions", "7"}}, ""},
		{"bg marks a session", bg, UnitBare, [][]string{{"ZADD", "allSessions", "1", "7"}}, ""},
	}
	for _, c := range cases {
		d, u := run(g, c.p, c.kind, c.cmds...)
		switch {
		case c.want == "" && d != nil:
			t.Errorf("%s: refused %s (%s)", c.name, d.Reason, d.Detail)
		case c.want != "" && (d == nil || d.Reason != c.want):
			t.Errorf("%s: got %+v, want %s", c.name, d, c.want)
		}
		if c.name == "unlink of an open file" && (len(u.RefInodes) != 1 || u.RefInodes[0] != 9) {
			t.Errorf("sustained inode not handed to Decide: %v", u.RefInodes)
		}
		if c.name == "heartbeat" && string(bytes.Join(u.rewrite, []byte(" "))) != "ZADD allSessions GT "+future+" 7" {
			t.Errorf("heartbeat forwarded as %q", u.rewrite)
		}
	}
	// Without Redis TIME the heartbeat is still forwarded, as ZADD GT, so a
	// live session's score does not lapse while the side connection is down;
	// GT never lowers the stored score. It is logged. Another session's
	// heartbeat is still refused.
	for _, side := range []sideDoer{nil, fakeScopeSide{noTime: true}} {
		g := NewGuard(store, fakeChunks{})
		var logged []string
		g.logf = func(f string, a ...any) { logged = append(logged, fmt.Sprintf(f, a...)) }
		g.side = side
		d, u := run(g, mac, UnitBare, []string{"ZADD", "allSessions", past, "7"})
		if d != nil {
			t.Errorf("heartbeat without Redis TIME refused: %s (%s)", d.Reason, d.Detail)
		} else if got := string(bytes.Join(u.rewrite, []byte(" "))); got != "ZADD allSessions GT "+past+" 7" || len(logged) != 1 {
			t.Errorf("heartbeat without Redis TIME forwarded as %q, logged %q", got, logged)
		}
		if d, _ := run(g, mac, UnitBare, []string{"ZADD", "allSessions", future, "8"}); d == nil || d.Reason != ReasonBGOnly {
			t.Errorf("heartbeat of a session mac1 does not own: got %+v, want BG_ONLY", d)
		}
	}
}
