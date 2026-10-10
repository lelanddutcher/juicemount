package respproxy

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLiveJuiceFSUnlinkWhileOpenNoDeny mounts a real juicefs through the
// enforcing proxy as a Mac principal with the Mac's flags (--no-bgjob
// --max-deletes 0, lane L3), unlinks a file while it is open, closes it,
// and also unlinks a closed file. Trash is off so juicefs deletes the data
// itself (doDeleteSustainedInode -> tryDeleteFileData -> bare ZREM
// delfiles). No legitimate operation may be refused, and the delfiles
// entries must be gone afterwards.
//
// Set JM_RESPPROXY_JUICEFS=<juicefs binary>. It mounts ONLY a mountpoint
// under the test's temp dir and unmounts only that.
func TestLiveJuiceFSUnlinkWhileOpenNoDeny(t *testing.T) {
	// The Mac mounts with --no-bgjob --max-deletes 0 (lane L3); the manager
	// principal with juicefs defaults (max-deletes 2), which also deletes
	// the slices itself (deleteChunk txn, then bare HDEL sliceRef).
	t.Run("mac", func(t *testing.T) { unlinkWhileOpen(t, "mac1", "--no-bgjob", "--max-deletes", "0") })
	t.Run("manager", func(t *testing.T) { unlinkWhileOpen(t, "mgr1", "--no-bgjob") })
}

func unlinkWhileOpen(t *testing.T, who string, flags ...string) {
	jfs := os.Getenv("JM_RESPPROXY_JUICEFS")
	if jfs == "" {
		t.Skip("JM_RESPPROXY_JUICEFS not set")
	}
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	ctx := ctx5(t)
	tp := startProxy(t, addr, ModeEnforce)
	dir := t.TempDir()
	mnt := filepath.Join(dir, "mnt")
	os.MkdirAll(mnt, 0o755)
	url := func(p string) string { return "redis://" + p + "@" + tp.addr + "/15" }
	run := func(principal string, args ...string) {
		t.Helper()
		cmd := exec.Command(jfs, args...)
		cmd.Env = append(os.Environ(), "META_PASSWORD="+passOf(principal))
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("juicefs %v: %v\n%s", args[0], err, out)
		}
	}
	run("admin1", "format", "--no-update", "--storage", "file", "--bucket", filepath.Join(dir, "obj")+"/",
		"--trash-days", "0", url("admin1"), "h8bunlink")
	run(who, append(append([]string{"mount", "-d", "--no-usage-report"}, flags...),
		"--cache-dir", filepath.Join(dir, "cache"), "--log", filepath.Join(dir, "jfs.log"),
		"--metrics", "127.0.0.1:58664", "-o", "nobrowse", url(who), mnt)...)
	mounted := false
	for i := 0; i < 100 && !mounted; i++ {
		time.Sleep(100 * time.Millisecond)
		mounted = isMount(mnt)
	}
	if !mounted {
		t.Fatal("mount did not appear")
	}
	defer func() {
		exec.Command("/sbin/umount", mnt).Run()
		for i := 0; i < 50 && isMount(mnt); i++ {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	open := filepath.Join(mnt, "open.bin")
	if err := os.WriteFile(open, make([]byte, 6<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(open)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(open); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	if _, err := f.Read(buf); err != nil {
		t.Fatalf("read after unlink: %v", err)
	}
	f.Close()
	closed := filepath.Join(mnt, "closed.bin")
	if err := os.WriteFile(closed, make([]byte, 3<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(closed); err != nil {
		t.Fatal(err)
	}
	// juicefs deletes data asynchronously after close/unlink.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && rdb.ZCard(ctx, "delfiles").Val() != 0 {
		time.Sleep(100 * time.Millisecond)
	}
	snap := tp.Stats.Snapshot()
	for _, d := range snap.Decisions {
		if d.Outcome != DecAllow && d.Principal == who {
			t.Errorf("legitimate operation refused: %+v", d)
		}
	}
	for _, r := range snap.Recent {
		t.Logf("refusal: %s %s %s %s", r.Principal, r.Reason, r.Unit, r.Detail)
	}
	if n := rdb.ZCard(ctx, "delfiles").Val(); n != 0 {
		t.Errorf("delfiles still has %d entries: %v", n, rdb.ZRange(ctx, "delfiles", 0, -1).Val())
	}
	if keys := rdb.Keys(ctx, "c*_*").Val(); len(keys) != 0 {
		t.Errorf("chunks of deleted files remain: %v", keys)
	}
	if b, _ := os.ReadFile(filepath.Join(dir, "jfs.log")); strings.Contains(string(b), "JMDENY") {
		t.Errorf("juicefs logged a refusal:\n%s", grepLines(string(b), "JMDENY"))
	}
	// Validity gate: the path under test must have run.
	seen := map[string]int64{}
	for _, r := range snap.Rows {
		if r.Shape == "delfiles" || r.Shape == ShapeChunk && r.Cmd == "DEL" {
			seen[r.Cmd+" "+r.Shape+" "+r.Ctx] += r.Count
		}
	}
	t.Logf("cleanup traffic: %v", seen)
	if who == "mgr1" {
		drops := int64(0)
		for _, r := range snap.Rows {
			if r.Cmd == "HDEL" && r.Shape == "sliceRef" && r.Ctx == "bare" {
				drops += r.Count
			}
		}
		t.Logf("manager bare sliceRef drops: %d", drops)
		if drops == 0 {
			t.Fatal("the manager's own slice-drop path never ran")
		}
	}
	if seen["ZADD delfiles txn"] < 2 || seen["ZREM delfiles bare"] < 2 || seen["DEL c<ino>_<idx> txn"] < 1 {
		t.Fatalf("unlink/close cleanup path did not run through the proxy: %v", seen)
	}
	allowed := 0
	for _, d := range snap.Decisions {
		if d.Outcome == DecAllow && d.Principal == who {
			allowed += int(d.Count)
		}
	}
	if allowed < 10 {
		t.Fatalf("only %d admitted %s units: the mount did not go through the proxy", allowed, who)
	}
}

func isMount(p string) bool {
	out, _ := exec.Command("/sbin/mount").Output()
	real, _ := filepath.EvalSymlinks(p)
	return strings.Contains(string(out), " on "+p+" ") || (real != "" && strings.Contains(string(out), " on "+real+" "))
}

func grepLines(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
