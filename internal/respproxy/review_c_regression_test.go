package respproxy

// Regression tests for the third H8B-L1 review (review-h8b-l1c,
// zz{e,f,g,m,n,o,p,q}). Each asserts the fixed behaviour.

import (
	"encoding/binary"
	"fmt"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// #1 (unit): decrements must be paid by references the txn really removes.
func TestGuardDecrementNeedsRemovedReference(t *testing.T) {
	mac := Principal{Name: "mac2", Kind: KindMac}
	chunks := fakeChunks{"c100_0": {mslice(777, 4096)}, "c9_0": {mslice(3, 4096), mslice(4, 4096), mslice(3, 4096)}}
	g := NewGuard(NewMemSessionStore(), chunks)
	for _, c := range []struct {
		name  string
		watch []string
		cmds  [][]string
		ok    bool
	}{
		{"no-op LTRIM + -1", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
		{"no-op LTRIM + delSlices", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "-1"}, {"HSET", "delSlices", "777_17", delayed(777)}}, false},
		{"trim keeping the slice + -1", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "0"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
		{"DEL + -1", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, true},
		{"DEL + -2", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"HINCRBY", "sliceRef", "k777_4096", "-2"}}, false},
		{"DEL + push back + -1", []string{"c100_0"}, [][]string{{"DEL", "c100_0"}, {"RPUSH", "c100_0", mslice(777, 4096)}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
		{"trim twice: second trim voids the count", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "1", "-1"}, {"LTRIM", "c100_0", "0", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
		{"push before trim voids the count", []string{"c100_0"}, [][]string{{"RPUSH", "c100_0", mslice(0, 4096)}, {"LTRIM", "c100_0", "1", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
		{"real trim removes both copies of 3", []string{"c9_0"}, [][]string{{"LTRIM", "c9_0", "3", "-1"}, {"HINCRBY", "sliceRef", "k3_4096", "-2"}, {"HINCRBY", "sliceRef", "k4_4096", "-1"}}, true},
		{"real trim, one decrement too many", []string{"c9_0"}, [][]string{{"LTRIM", "c9_0", "3", "-1"}, {"HINCRBY", "sliceRef", "k3_4096", "-3"}}, false},
		{"no-op re-push duplicates a reference", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "-1"}, {"RPUSH", "c100_0", mslice(777, 4096)}}, false},
		{"+1 on a slice the txn neither pushes nor removes", []string{"c100_0"}, [][]string{{"LTRIM", "c100_0", "0", "-1"}, {"HINCRBY", "sliceRef", "k777_4096", "1"}, {"HINCRBY", "sliceRef", "k777_4096", "-1"}}, false},
	} {
		d, _, _ := guardRunW(g, mac, UnitTxn, c.watch, c.cmds...)
		if (d == nil) != c.ok {
			t.Errorf("%s: denial=%v, want admitted=%v", c.name, d, c.ok)
		}
	}
}

// #4: the compaction EINVAL rollback of a never-linked slice the principal
// allocated is admitted once; anything else stays bg-only. The used bit is
// set only when a link or rollback COMMITS (review H8B-L1d #5).
func TestGuardCompactionRollback(t *testing.T) {
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	mac2 := Principal{Name: "mac2", Kind: KindMac}
	g.slices.Record(5000, 5000+4095, "mac1")
	rb := func(p Principal, id uint64, n string) (*Denial, []guardEffect) {
		d, _, eff := guardRun(g, p, UnitBare, []string{"HINCRBY", "sliceRef", fmt.Sprintf("k%d_8192", id), n})
		return d, eff
	}
	if d, _ := rb(mac2, 5001, "-1"); d == nil {
		t.Fatal("mac2 rolled back mac1's slice")
	}
	if d, _ := rb(mac1, 5001, "-2"); d == nil {
		t.Fatal("rollback by -2 admitted")
	}
	if d, _ := rb(mac1, 5001, "7"); d == nil {
		t.Fatal("bare +7 admitted")
	}
	d, eff := rb(mac1, 5001, "-1")
	if d != nil {
		t.Fatalf("rollback of an unlinked own slice refused: %+v", d)
	}
	g.Commit(eff)
	if d, _ := rb(mac1, 5001, "-1"); d == nil {
		t.Fatal("rollback admitted twice")
	}
	// A compaction txn that links slice 5002 but is WATCH-aborted leaves
	// it unused: its rollback is still admitted.
	d, _, eff = guardRun(g, mac1, UnitTxn, []string{"RPUSH", "c9_0", mslice(5002, 4096)}, []string{"SET", "i9", "a"})
	if d != nil {
		t.Fatalf("push of own slice: %+v", d)
	}
	// (not committed)
	if d, _ := rb(mac1, 5002, "-1"); d != nil {
		t.Fatalf("rollback after an ABORTED link refused: %+v", d)
	}
	// Once the link commits, rollback is refused.
	d, _, eff = guardRun(g, mac1, UnitTxn, []string{"RPUSH", "c9_0", mslice(5003, 4096)}, []string{"SET", "i9", "a"})
	if d != nil {
		t.Fatalf("push of own slice: %+v", d)
	}
	g.Commit(eff)
	if d, _ := rb(mac1, 5003, "-1"); d == nil {
		t.Fatal("rollback of a linked slice admitted")
	}
	if d, _ := rb(mac1, 99999, "-1"); d == nil {
		t.Fatal("rollback of a slice outside any allocation admitted")
	}
}

// #2 / L1d #6: in-bound usedSpace txns cannot overflow the counter, and
// each principal's net change per minute is bounded.
func TestReviewCUsedSpaceCeiling(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	start := counterLimit - 2*maxUsedSpaceDelta
	rdb.Set(ctx, "usedSpace", strconv.FormatInt(start, 10), 0)
	tp := startProxy(t, addr, ModeEnforce)
	conns := map[string]*rawConn{}
	for _, who := range []string{"mac1", "mac2", "mgr1"} {
		conns[who] = tp.raw(t)
		conns[who].login(who)
	}
	txn := func(who string, delta int64) Value {
		b := conns[who]
		b.send("MULTI")
		b.send("SET", "i100", "a")
		b.send("INCRBY", "usedSpace", strconv.FormatInt(delta, 10))
		b.send("EXEC")
		b.flush()
		b.read()
		b.read()
		b.read()
		return b.read()
	}
	// Rate: a principal's net growth per minute is bounded.
	if v := txn("mac1", maxUsedSpaceDelta); v.Type != '*' {
		t.Fatalf("mac1 1 TiB: %s", show(v))
	}
	mustDeny(t, txn("mac1", 4096), ReasonAdminOnly, "mac1 over its per-minute bound")
	if v := txn("mac1", -4096); v.Type != '*' {
		t.Fatalf("mac1 shrink within its window: %s", show(v))
	}
	if v := txn("mac1", 4096); v.Type != '*' {
		t.Fatalf("mac1 back within its net bound: %s", show(v))
	}
	// Ceiling: other principals together still cannot pass counterLimit.
	if v := txn("mac2", maxUsedSpaceDelta); v.Type != '*' {
		t.Fatalf("mac2 1 TiB: %s", show(v))
	}
	mustDeny(t, txn("mgr1", maxUsedSpaceDelta), ReasonAdminOnly, "txn past the counter limit")
	got, _ := rdb.Get(ctx, "usedSpace").Int64()
	if got != counterLimit {
		t.Fatalf("usedSpace = %d, want %d", got, counterLimit)
	}
}

// #3: the slice-owner table grows by at most one entry per allocation
// batch, and consecutive batches of one writer merge into one entry.
func TestReviewCSliceOwnerTableBounded(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Del(ctx, SliceOwnersKey)
	rdb.Set(ctx, "nextchunk", "1000", 0)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	mustDeny(t, b.do("INCRBY", "nextchunk", "1"), ReasonAdminOnly, "INCRBY nextchunk 1")
	mustDeny(t, b.do("INCR", "nextchunk"), ReasonAdminOnly, "INCR nextchunk")
	for i := 0; i < 200; i++ {
		b.send("INCRBY", "nextchunk", "4096")
	}
	b.flush()
	var last int64
	for i := 0; i < 200; i++ {
		last = b.read().Int
	}
	if n := rdb.ZCard(ctx, SliceOwnersKey).Val(); n != 1 {
		t.Fatalf("200 consecutive batches of one writer: %d entries, want 1 (%v)", n, rdb.ZRange(ctx, SliceOwnersKey, 0, 3).Val())
	}
	if m := rdb.ZRange(ctx, SliceOwnersKey, 0, -1).Val(); m[0] != fmt.Sprintf("1001:%d:mac2", last) {
		t.Fatalf("merged range %q", m[0])
	}
	a := tp.raw(t)
	a.login("mac1")
	for i := 0; i < 10; i++ { // interleaved writers: one entry per switch
		a.do("INCRBY", "nextchunk", "4096")
		b.do("INCRBY", "nextchunk", "4096")
	}
	if n := rdb.ZCard(ctx, SliceOwnersKey).Val(); n > 21 {
		t.Fatalf("interleaved: %d entries", n)
	}
	// Every allocated id still resolves to its owner.
	tp2 := startProxy(t, addr, ModeEnforce)
	b2 := tp2.raw(t)
	b2.login("mac2")
	b2.send("MULTI")
	b2.send("RPUSH", "c100_0", mslice(uint64(last-17), 4096))
	b2.send("SET", "i100", "a")
	b2.send("EXEC")
	b2.flush()
	b2.read()
	b2.read()
	b2.read()
	if v := b2.read(); v.Type != '*' {
		t.Fatalf("id inside a merged range after restart: %s", show(v))
	}
}

// #5: the pending-reply COUNT cap disconnects too.
func TestReviewCPendingReplyCountCap(t *testing.T) {
	addr := redisAddr(t, false)
	direct(t, addr)
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.MaxPendingReplies = 2000 })
	c, err := net.Dial("tcp", tp.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "*3\r\n$4\r\nAUTH\r\n$4\r\nmac2\r\n$9\r\nmac2-pass\r\n")
	time.Sleep(100 * time.Millisecond)
	one := "*3\r\n$3\r\nSET\r\n$7\r\nsetting\r\n$1\r\nx\r\n"
	chunk := []byte(strings.Repeat(one, 1000))
	c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < 200; i++ {
		if _, err := c.Write(chunk); err != nil {
			break
		}
	}
	time.Sleep(200 * time.Millisecond)
	if tp.Stats.overflowClosed.Load() != 1 {
		t.Fatalf("2000-reply cap not enforced (overflow closes %d)", tp.Stats.overflowClosed.Load())
	}
}

// #1 end to end, with the bundled juicefs: a Mac clones another file's slice
// into a chunk of its own, then tries to drive the slice's reference count
// negative with no-op LTRIM transactions. bg `juicefs gc --delete` and
// `juicefs fsck` must then find the victim file intact.
// Set JM_RESPPROXY_JUICEFS. Mounts ONLY a temp-dir mountpoint.
func TestLiveJuiceFSDecrementLoopNoDataLoss(t *testing.T) {
	jfs := os.Getenv("JM_RESPPROXY_JUICEFS")
	if jfs == "" {
		t.Skip("JM_RESPPROXY_JUICEFS not set")
	}
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	dir := t.TempDir()
	mnt, bucket := filepath.Join(dir, "mnt"), filepath.Join(dir, "obj")
	os.MkdirAll(mnt, 0o755)
	url := func(p string) string { return "redis://" + p + "@" + tp.addr + "/15" }
	run := func(principal string, args ...string) (string, error) {
		cmd := exec.Command(jfs, args...)
		cmd.Env = append(os.Environ(), "META_PASSWORD="+passOf(principal))
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := run("admin1", "format", "--no-update", "--storage", "file", "--bucket", bucket+"/",
		"--trash-days", "0", url("admin1"), "h8bdecr"); err != nil {
		t.Fatalf("format: %v\n%s", err, out)
	}
	if out, err := run("mac1", "mount", "-d", "--no-usage-report", "--no-bgjob", "--max-deletes", "0",
		"--cache-dir", filepath.Join(dir, "cache"), "--log", filepath.Join(dir, "jfs.log"),
		"--metrics", "127.0.0.1:58665", "-o", "nobrowse", url("mac1"), mnt); err != nil {
		t.Fatalf("mount: %v\n%s", err, out)
	}
	for i := 0; i < 100 && !isMount(mnt); i++ {
		time.Sleep(100 * time.Millisecond)
	}
	defer func() {
		exec.Command("/sbin/umount", mnt).Run()
		for i := 0; i < 50 && isMount(mnt); i++ {
			time.Sleep(100 * time.Millisecond)
		}
	}()
	victim := strings.Repeat("victim-data-", 20000)
	if err := os.WriteFile(filepath.Join(mnt, "victim"), []byte(victim), 0o644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	var vchunk string
	for _, k := range rdb.Keys(ctx, "c*_0").Val() {
		vchunk = k
	}
	elems := rdb.LRange(ctx, vchunk, 0, -1).Val()
	if vchunk == "" || len(elems) == 0 {
		t.Fatal("victim has no chunk")
	}
	e := []byte(elems[0])
	id, size := binary.BigEndian.Uint64(e[4:12]), binary.BigEndian.Uint32(e[12:16])
	field := fmt.Sprintf("k%d_%d", id, size)
	objects := func() int {
		n := 0
		filepath.WalkDir(bucket, func(p string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), fmt.Sprintf("%d_", id)) {
				n++
			}
			return nil
		})
		return n
	}
	if objects() == 0 {
		t.Fatalf("victim slice %d has no objects in the bucket", id)
	}

	mac2 := tp.raw(t)
	mac2.login("mac2")
	own := "c999999_0"
	txn := func(watch string, cmds ...[]string) Value {
		if watch != "" {
			mac2.do("WATCH", watch)
		}
		mac2.send("MULTI")
		for _, c := range cmds {
			mac2.send(c...)
		}
		mac2.send("EXEC")
		mac2.flush()
		for range cmds {
			mac2.read()
		}
		mac2.read()
		return mac2.read()
	}
	if v := txn("", []string{"RPUSH", own, elems[0]}, []string{"HINCRBY", "sliceRef", field, "1"}); v.Type != '*' {
		t.Fatalf("clone (+1) refused: %s", show(v))
	}
	// Refusals are checked with Errorf, not Fatalf, so a regression also
	// shows the consequence below (gc deleting the victim's data).
	refused := func(v Value, what string) {
		if !isDeny(v, ReasonBGOnly) {
			t.Errorf("%s: got %s, want -JMDENY BG_ONLY", what, show(v))
		}
	}
	for i := 0; i < 2; i++ {
		refused(txn(own, []string{"LTRIM", own, "0", "-1"}, []string{"HINCRBY", "sliceRef", field, "-1"}), "no-op LTRIM + -1")
	}
	refused(txn(vchunk, []string{"LTRIM", vchunk, "0", "-1"}, []string{"HINCRBY", "sliceRef", field, "-1"}), "no-op LTRIM of the victim's chunk + -1")
	if v := rdb.HGet(ctx, "sliceRef", field).Val(); v != "1" {
		t.Errorf("victim sliceRef = %q, want 1 (two references)", v)
	}
	refused(txn(own, []string{"LTRIM", own, "0", "-1"}, []string{"HSET", "delSlices", fmt.Sprintf("%d_%d", id, time.Now().Unix()), string(e[4:16])}), "no-op LTRIM + delSlices")
	if out, err := run("bg1", "gc", url("bg1"), "--delete"); err != nil {
		t.Fatalf("bg gc --delete: %v\n%s", err, out)
	}
	if objects() == 0 {
		t.Fatal("DATA LOSS: gc deleted the victim's slice objects")
	}
	out, err := run("bg1", "fsck", url("bg1"))
	if err != nil || strings.Contains(strings.ToLower(out), "lost") {
		t.Fatalf("fsck after gc: %v\n%s", err, out)
	}
	b, err := os.ReadFile(filepath.Join(mnt, "victim"))
	if err != nil || string(b) != victim {
		t.Fatalf("victim file read back: err=%v len=%d", err, len(b))
	}
}
