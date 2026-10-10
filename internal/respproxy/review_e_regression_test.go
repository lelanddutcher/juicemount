package respproxy

// L5 finding (2026-10-06): a manager `juicefs sync --delete-dst` with
// trash-days 0 drops its own slices' sliceRef fields with a bare HDEL.

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGuardOwnSliceDrop(t *testing.T) {
	g := NewGuard(NewMemSessionStore(), fakeChunks{})
	mac1 := Principal{Name: "mac1", Kind: KindMac}
	mac2 := Principal{Name: "mac2", Kind: KindMac}
	mgr := Principal{Name: "mgr1", Kind: KindManager}
	g.slices.Record(5000, 5000+4095, "mac1")
	d, u, _ := guardRun(g, mac1, UnitBare, []string{"HDEL", "sliceRef", "k5001_4096"})
	if d != nil || u.rewrite == nil || string(u.rewrite[0]) != "EVAL" || string(u.rewrite[1]) != SliceDropScript {
		t.Fatalf("own allocated slice drop: %+v rewrite=%q", d, u.rewrite)
	}
	if d, _, _ := guardRun(g, mac2, UnitBare, []string{"HDEL", "sliceRef", "k5001_4096"}); d == nil || d.Reason != ReasonBGOnly {
		t.Fatalf("mac2 drops mac1's slice: %+v", d)
	}
	if d, _, _ := guardRun(g, mgr, UnitBare, []string{"HDEL", "sliceRef", "k777_4096"}); d == nil {
		t.Fatal("drop of a slice the manager never took below zero admitted")
	}
	g.NoteDropped(mgr, "k777_4096")
	if d, _, _ := guardRun(g, mac2, UnitBare, []string{"HDEL", "sliceRef", "k777_4096"}); d == nil {
		t.Fatal("mac2 drops a field the manager took below zero")
	}
	if d, _, _ := guardRun(g, mgr, UnitBare, []string{"HDEL", "sliceRef", "k777_4096", "k778_4096"}); d == nil {
		t.Fatal("drop including a foreign field admitted")
	}
	d, _, eff := guardRun(g, mgr, UnitBare, []string{"HDEL", "sliceRef", "k777_4096"})
	if d != nil {
		t.Fatalf("manager's own drop refused: %+v", d)
	}
	g.Commit(eff)
	if d, _, _ := guardRun(g, mgr, UnitBare, []string{"HDEL", "sliceRef", "k777_4096"}); d == nil {
		t.Fatal("drop record kept after the drop committed")
	}
}

// The admitted drop only ever removes a field at <= 0: one still holding
// extra references survives.
func TestReviewEOwnSliceDropIsConditional(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Del(ctx, SliceOwnersKey)
	tp := startProxy(t, addr, ModeEnforce)
	b := tp.raw(t)
	b.login("mac2")
	hi := b.do("INCRBY", "nextchunk", "4096").Int
	a, z := "k"+itoa(uint64(hi))+"_4096", "k"+itoa(uint64(hi-1))+"_4096"
	rdb.HSet(ctx, "sliceRef", a, "2", z, "-1")
	if v := b.do("HDEL", "sliceRef", a); v.Type != ':' || v.Int != 0 {
		t.Fatalf("drop of a field with references: %s", show(v))
	}
	if v := b.do("HDEL", "sliceRef", z); v.Type != ':' || v.Int != 1 {
		t.Fatalf("drop of a field at -1: %s", show(v))
	}
	if rdb.HGet(ctx, "sliceRef", a).Val() != "2" || rdb.HExists(ctx, "sliceRef", z).Val() {
		t.Fatalf("sliceRef after drops: %v", rdb.HGetAll(ctx, "sliceRef").Val())
	}
}

// Real juicefs: the manager principal syncs a tree into the volume, then
// syncs again with --delete-dst after the source shrank, with trash-days 0,
// through the enforcing proxy. No legitimate refusal; the data and the
// sliceRef fields of deleted files are cleaned up. Also juicefs status and
// a read-only juicefs config as the manager. No mount: sync uses the SDK.
func TestLiveJuiceFSManagerSyncDeleteDst(t *testing.T) {
	jfs := os.Getenv("JM_RESPPROXY_JUICEFS")
	if jfs == "" {
		t.Skip("JM_RESPPROXY_JUICEFS not set")
	}
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	dir := t.TempDir()
	bucket := filepath.Join(dir, "obj")
	url := func(p string) string { return "redis://" + p + "@" + tp.addr + "/15" }
	run := func(principal string, env []string, args ...string) string {
		t.Helper()
		cmd := exec.Command(jfs, args...)
		cmd.Env = append(append(os.Environ(), "META_PASSWORD="+passOf(principal)), env...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("juicefs %v as %s: %v\n%s", args[0], principal, err, tail(string(out)))
		}
		return string(out)
	}
	run("admin1", nil, "format", "--no-update", "--storage", "file", "--bucket", bucket+"/", "--trash-days", "0", url("admin1"), "h8bsync")
	src := filepath.Join(dir, "src")
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	for i := 0; i < 20; i++ {
		os.WriteFile(filepath.Join(src, "sub", "f"+itoa(uint64(i))), []byte(strings.Repeat("d", 300000+i)), 0o644)
	}
	env := []string{"JMVOL=" + url("mgr1")}
	run("mgr1", env, "sync", "--update", src+"/", "jfs://JMVOL/Projects/Sync/")
	objectsAfterCopy := countFiles(bucket)
	for i := 0; i < 15; i++ {
		os.Remove(filepath.Join(src, "sub", "f"+itoa(uint64(i))))
	}
	run("mgr1", env, "sync", "--update", "--delete-dst", src+"/", "jfs://JMVOL/Projects/Sync/")
	run("mgr1", nil, "status", url("mgr1"))
	run("mgr1", nil, "config", url("mgr1"))
	time.Sleep(500 * time.Millisecond)

	snap := tp.Stats.Snapshot()
	for _, d := range snap.Decisions {
		if d.Outcome != DecAllow && d.Principal == "mgr1" {
			t.Errorf("legitimate manager operation refused: %+v", d)
		}
	}
	for _, r := range snap.Recent {
		t.Logf("refusal: %s %s %s %s", r.Principal, r.Reason, r.Unit, r.Detail)
	}
	drops := int64(0)
	for _, r := range snap.Rows {
		if r.Cmd == "HDEL" && r.Shape == "sliceRef" && r.Ctx == "bare" {
			drops += r.Count
		}
	}
	if drops == 0 {
		t.Fatalf("the bare HDEL sliceRef path never ran (objects %d)", objectsAfterCopy)
	}
	// juicefs deletes slices asynchronously (deleteSlice workers) and `sync`
	// exits right after its last unlink, so the last file's slice can be
	// left at -1 for bg gc, through the proxy or not (seen direct too: 7
	// objects instead of 6). A REFUSED drop fails above; here allow at most
	// that one straggler.
	left := 0
	for f, v := range rdb.HGetAll(ctx, "sliceRef").Val() {
		if strings.HasPrefix(v, "-") {
			left++
			t.Logf("sliceRef %s left at %s (async-delete straggler)", f, v)
		}
	}
	if left > 1 || drops < 14 {
		t.Errorf("%d fields left negative, %d drops (want <=1 and >=14 of 15)", left, drops)
	}
	if n := countFiles(bucket); n >= objectsAfterCopy {
		t.Errorf("objects of deleted files not deleted: %d -> %d", objectsAfterCopy, n)
	}
	t.Logf("bare sliceRef drops: %d; objects %d -> %d", drops, objectsAfterCopy, countFiles(bucket))
}

func countFiles(dir string) int {
	n := 0
	filepath.Walk(dir, func(p string, fi os.FileInfo, err error) error {
		if err == nil && !fi.IsDir() {
			n++
		}
		return nil
	})
	return n
}

func tail(s string) string {
	if len(s) > 2000 {
		return s[len(s)-2000:]
	}
	return s
}

// The manager deletes a file whose slice a Mac wrote: its committed
// decrement (EXEC result -1) makes the following bare HDEL its own drop.
// An aborted decrement records nothing.
func TestReviewEDropAfterOwnDecrement(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	rdb.Del(ctx, SliceOwnersKey)
	tp := startProxy(t, addr, ModeEnforce)
	mac := tp.raw(t)
	mac.login("mac2")
	id := uint64(mac.do("INCRBY", "nextchunk", "4096").Int)
	mac.send("MULTI")
	mac.send("RPUSH", "c300_0", mslice(id, 4096))
	mac.send("SET", "i300", "a")
	mac.send("EXEC")
	mac.flush()
	for i := 0; i < 3; i++ {
		mac.read()
	}
	if v := mac.read(); v.Type != '*' {
		t.Fatalf("mac write: %s", show(v))
	}
	f := fmt.Sprintf("k%d_4096", id)
	mgr := tp.raw(t)
	mgr.login("mgr1")
	mustDeny(t, mgr.do("HDEL", "sliceRef", f), ReasonBGOnly, "drop before any decrement")
	del := func(abort bool) Value {
		mgr.do("WATCH", "c300_0")
		if abort {
			rdb.Set(ctx, "i300", "x", 0)
			mgr.do("WATCH", "i300")
			rdb.Set(ctx, "i300", "y", 0)
		}
		mgr.send("MULTI")
		mgr.send("DEL", "c300_0")
		mgr.send("HINCRBY", "sliceRef", f, "-1")
		mgr.send("EXEC")
		mgr.flush()
		mgr.read()
		mgr.read()
		mgr.read()
		return mgr.read()
	}
	if v := del(true); !v.Null {
		t.Fatalf("aborted delete: %s", show(v))
	}
	mustDeny(t, mgr.do("HDEL", "sliceRef", f), ReasonBGOnly, "drop after an ABORTED decrement")
	if v := del(false); v.Type != '*' || v.Elems[1].Int != -1 {
		t.Fatalf("delete: %s", show(v))
	}
	if v := mgr.do("HDEL", "sliceRef", f); v.Type != ':' || v.Int != 1 {
		t.Fatalf("manager's own drop after its committed decrement: %s", show(v))
	}
	mac2 := tp.raw(t)
	mac2.login("mac1")
	rdb.HSet(ctx, "sliceRef", f, "-1")
	mustDeny(t, mac2.do("HDEL", "sliceRef", f), ReasonBGOnly, "another principal's drop")
}

// L2 request: each client's upstream is named p:<principal>:<connID>, so a
// kill by that name ends exactly one connection; the principal's others
// survive. The Unit and Outcome carry the ConnID.
func TestUpstreamNamePerConnection(t *testing.T) {
	addr := redisAddr(t, false)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	pol := &recordingPolicy{}
	tp := startProxy(t, addr, ModeEnforce, func(c *Config) { c.Policy = pol })
	a, b := tp.raw(t), tp.raw(t)
	a.login("mac1")
	b.login("mac1")
	mustOK(t, a.do("SET", "i5", "a"), "a")
	mustOK(t, b.do("SET", "i6", "b"), "b")
	units, outs := pol.snapshot()
	if len(units) != 2 || units[0].ConnID == units[1].ConnID || units[0].UpstreamName == units[1].UpstreamName || outs[0].ConnID != units[0].ConnID ||
		units[0].UpstreamName != UpstreamName("mac1", units[0].ConnID) {
		t.Fatalf("units %+v outcomes %+v", units, outs)
	}
	list := rdb.ClientList(ctx).Val()
	for _, u := range units {
		if !strings.Contains(list, "name="+u.UpstreamName+" ") {
			t.Fatalf("CLIENT LIST lacks %s:\n%s", u.UpstreamName, list)
		}
	}
	var killID string
	for _, l := range strings.Split(list, "\n") {
		if strings.Contains(l, " name="+units[0].UpstreamName+" ") {
			killID = strings.TrimPrefix(strings.Fields(l)[0], "id=")
		}
	}
	if n := rdb.Do(ctx, "CLIENT", "KILL", "ID", killID).Val(); n != int64(1) {
		t.Fatalf("CLIENT KILL ID %s = %v", killID, n)
	}
	// The killed client's connection ends; the other one keeps working.
	a.c.SetReadDeadline(time.Now().Add(2 * time.Second))
	a.send("PING")
	a.flush()
	if _, err := ReadValue(a.r); err == nil {
		t.Fatal("connection of the killed upstream still answers")
	}
	if v := b.do("PING"); string(v.Str) != "PONG" {
		t.Fatalf("the principal's other connection: %s", show(v))
	}
	if !strings.Contains(rdb.ClientList(ctx).Val(), "name="+units[1].UpstreamName+" ") {
		t.Fatal("the other connection's upstream was killed too")
	}
	// Side connections keep their own fixed name.
	if _, err := tp.Side().Do(testDB, strArgs("PING")...); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rdb.ClientList(ctx).Val(), "name=p:-proxy-side ") {
		t.Fatal("side connection not named p:-proxy-side")
	}
}
