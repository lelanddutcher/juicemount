//go:build h8b_acceptance

package respproxy

// H-8 option B lane L2 acceptance REDs (H8_OPTION_B_PLAN.md §2 L2).
//
// Every test runs TWO real juicefs FUSE mounts (principals mac1 and mac2)
// through the proxy with the lease engine, on a disposable Redis:
//
//	JM_RESPPROXY_TEST_REDIS=127.0.0.1:<port> JM_H8B_SCRATCH=<dir> \
//	  go test -tags h8b_acceptance -run 'TestH8B' ./internal/respproxy/
//
// The juicefs binary is JM_H8B_JUICEFS, else the bundled build if present,
// else Homebrew's. Mountpoints live only under the scratch directory, and
// each test unmounts only what it mounted. Without Redis, juicefs or macFUSE
// these tests FAIL rather than skip.

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

// ---------------------------------------------------------------------------
// Harness.

type fakeClockA struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClockA) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClockA) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

type h8bEnv struct {
	t         *testing.T
	redis     string
	rdb       *redis.Client
	dir       string
	clk       *fakeClockA
	proxyAddr string
	modePoll  time.Duration
	mode      Mode
	p         *Proxy
	eng       *LeaseEngine
	logs      *logSink
	engMut    func(*LeaseConfig)
}

func juicefsBin(t *testing.T) string {
	if b := os.Getenv("JM_H8B_JUICEFS"); b != "" {
		return b
	}
	for _, c := range []string{"../../build/juicefs", "../../third_party/juicefs/juicefs", "../../app/Resources/juicefs"} {
		if st, err := os.Stat(c); err == nil && st.Mode().IsRegular() && st.Mode()&0o111 != 0 {
			abs, _ := filepath.Abs(c)
			return abs
		}
	}
	for _, c := range []string{"/opt/homebrew/bin/juicefs", "/usr/local/bin/juicefs"} {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Fatal("no juicefs binary: set JM_H8B_JUICEFS")
	return ""
}

func h8bScratch(t *testing.T) string {
	base := os.Getenv("JM_H8B_SCRATCH")
	if base == "" {
		base = os.Getenv("TMPDIR")
	}
	for _, bad := range []string{"/tmp", "/private/", "/var/"} {
		if base == "" || strings.HasPrefix(base, bad) {
			t.Fatalf("set JM_H8B_SCRATCH to a lane scratch directory (got %q; system temp dirs are not allowed)", base)
		}
	}
	d, err := os.MkdirTemp(base, "h8b-")
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func newH8BEnv(t *testing.T, mode Mode, mut ...func(*LeaseConfig)) *h8bEnv {
	addr := redisAddr(t, true)
	rdb := direct(t, addr)
	env := &h8bEnv{t: t, redis: addr, rdb: rdb, dir: h8bScratch(t), mode: mode,
		clk: &fakeClockA{t: time.Now()}, modePoll: 2 * time.Second}
	if len(mut) > 0 {
		env.engMut = mut[0]
	}
	ctx := ctx5(t)
	rdb.Del(ctx, LeaseLogKey)
	out, err := exec.Command(juicefsBin(t), "format", "--storage", "file",
		"--bucket", filepath.Join(env.dir, "bucket")+"/", "--trash-days", "1", "--no-update",
		fmt.Sprintf("redis://%s/%d", addr, testDB), "h8b").CombinedOutput()
	if err != nil {
		t.Fatalf("juicefs format: %v\n%s", err, out)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	env.proxyAddr = ln.Addr().String()
	env.startProxy(ln)
	t.Cleanup(env.stopProxy)
	return env
}

func (env *h8bEnv) startProxy(ln net.Listener) {
	t := env.t
	sink := &logSink{t: t}
	p, err := New(Config{Upstream: env.redis, Principals: principalsForTest(t), Mode: env.mode,
		VolumeDB: testDB, Logf: sink.Logf, ModePoll: env.modePoll})
	if err != nil {
		t.Fatal(err)
	}
	lc := LeaseConfig{Side: p.Side(), VolumeDB: testDB, Now: env.clk.Now, Logf: sink.Logf,
		SweepEvery: time.Hour, FlushEvery: 100 * time.Millisecond}
	if env.engMut != nil {
		env.engMut(&lc)
	}
	eng := NewLeaseEngine(lc)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	p.SetPolicy(eng)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	go p.Serve(ln)
	env.p, env.eng, env.logs = p, eng, sink
}

func (env *h8bEnv) stopProxy() {
	if env.p != nil {
		env.p.Close()
		env.eng.Close()
		env.p, env.eng = nil, nil
	}
}

// restartProxy replaces the proxy and its engine (empty caches) on the same
// address, so the running mounts reconnect to the new incarnation.
func (env *h8bEnv) restartProxy() {
	env.stopProxy()
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if ln, err = net.Listen("tcp", env.proxyAddr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		env.t.Fatalf("re-listen %s: %v", env.proxyAddr, err)
	}
	env.startProxy(ln)
}

type jfsMount struct {
	env    *h8bEnv
	name   string
	mp     string
	log    string
	cmd    *exec.Cmd
	exited chan struct{}
}

func cstr(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

func mountedAt(mp string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(mp, &st); err != nil {
		return false
	}
	return cstr(st.Mntonname[:]) == mp && strings.Contains(cstr(st.Fstypename[:]), "fuse")
}

func (env *h8bEnv) mount(name string) *jfsMount {
	t := env.t
	mp := filepath.Join(env.dir, "mp-"+name)
	if err := os.MkdirAll(mp, 0o755); err != nil {
		t.Fatal(err)
	}
	m := &jfsMount{env: env, name: name, mp: mp, log: filepath.Join(env.dir, "juicefs-"+name+".log"), exited: make(chan struct{})}
	lf, err := os.Create(m.log)
	if err != nil {
		t.Fatal(err)
	}
	m.cmd = exec.Command(juicefsBin(t), "mount", "--no-bgjob", "--no-usage-report", "--enable-xattr",
		"--attr-cache", "0", "--entry-cache", "0", "--dir-entry-cache", "0",
		"--cache-dir", filepath.Join(env.dir, "cache-"+name), "-o", "nobrowse",
		fmt.Sprintf("redis://%s@%s/%d", name, env.proxyAddr, testDB), mp)
	m.cmd.Env = append(os.Environ(), "META_PASSWORD="+passOf(name))
	m.cmd.Stdout, m.cmd.Stderr = lf, lf
	if err := m.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { m.cmd.Wait(); lf.Close(); close(m.exited) }()
	t.Cleanup(m.unmount)
	deadline := time.Now().Add(30 * time.Second)
	for !mountedAt(mp) {
		select {
		case <-m.exited:
			t.Fatalf("juicefs mount %s exited:\n%s", name, m.tail())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("juicefs mount %s not ready after 30s:\n%s", name, m.tail())
		}
		time.Sleep(100 * time.Millisecond)
	}
	return m
}

func (m *jfsMount) tail() string {
	b, _ := os.ReadFile(m.log)
	if len(b) > 3000 {
		b = b[len(b)-3000:]
	}
	return string(b)
}

// unmount unmounts only this test's own mountpoint.
func (m *jfsMount) unmount() {
	if mountedAt(m.mp) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		exec.CommandContext(ctx, juicefsBin(m.env.t), "umount", m.mp).Run()
		cancel()
	}
	select {
	case <-m.exited:
	case <-time.After(15 * time.Second):
	}
	if mountedAt(m.mp) {
		exec.Command("umount", "-f", m.mp).Run()
	}
	select {
	case <-m.exited:
	case <-time.After(5 * time.Second):
		m.cmd.Process.Kill()
		<-m.exited
	}
	if mountedAt(m.mp) {
		m.env.t.Errorf("mountpoint %s is still mounted after cleanup", m.mp)
	}
}

func (m *jfsMount) path(rel string) string { return filepath.Join(m.mp, filepath.FromSlash(rel)) }

func (m *jfsMount) ino(rel string) uint64 {
	m.env.t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(m.path(rel), &st); err != nil {
		m.env.t.Fatalf("stat %s via %s: %v", rel, m.name, err)
	}
	return st.Ino
}

func (m *jfsMount) write(rel, data string) error {
	return os.WriteFile(m.path(rel), []byte(data), 0o644)
}

func (m *jfsMount) mustWrite(rel, data string) {
	m.env.t.Helper()
	if err := m.write(rel, data); err != nil {
		m.env.t.Fatalf("%s write %s: %v\nrefusals: %s", m.name, rel, err, m.env.recent())
	}
}

func (m *jfsMount) mustMkdirAll(rel string) {
	m.env.t.Helper()
	if err := os.MkdirAll(m.path(rel), 0o755); err != nil {
		m.env.t.Fatalf("%s mkdir -p %s: %v", m.name, rel, err)
	}
}

func (env *h8bEnv) recent() string {
	var b strings.Builder
	for _, r := range env.p.Stats.Snapshot().Recent {
		fmt.Fprintf(&b, "\n  %s %s %s unit=[%s] detail=%q", r.Outcome, r.Reason, r.Principal, r.Unit, r.Detail)
	}
	return b.String()
}

func refused(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: succeeded, want refused", what)
	}
}

// raw opens an authenticated hand-driven connection to the proxy.
func (env *h8bEnv) raw(name string) *rawConn {
	c, err := net.Dial("tcp", env.proxyAddr)
	if err != nil {
		env.t.Fatal(err)
	}
	env.t.Cleanup(func() { c.Close() })
	rc := &rawConn{t: env.t, c: c}
	rc.r, rc.w = newRW(c)
	rc.login(name)
	return rc
}

// status returns JM.LEASE STATUS <proj> as a map.
func (env *h8bEnv) status(proj uint64) map[string]string {
	env.t.Helper()
	v := env.raw("admin1").do("JM.LEASE", "STATUS", "ino:"+inoStr(proj))
	if v.Type == '-' {
		env.t.Fatalf("STATUS: %s", v.Str)
	}
	out := map[string]string{}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		val := v.Elems[i+1]
		s := string(val.Str)
		if val.Type == ':' {
			s = strconv.FormatInt(val.Int, 10)
		}
		out[string(v.Elems[i].Str)] = s
	}
	return out
}

func (env *h8bEnv) mustHolder(proj uint64, want string) map[string]string {
	env.t.Helper()
	st := env.status(proj)
	if st["holder"] != want {
		env.t.Fatalf("project %d holder = %q, want %q (status %v)", proj, st["holder"], want, st)
	}
	return st
}

// setup mounts mac1 and mac2 and creates /Projects/A and /Projects/B, then
// frees both. Creating the folder is an ns:root operation, but macOS then
// sets com.apple.provenance on it (a bare HSET x<root>), which is a write
// to the new project and grants it to the creator.
func setupTwo(t *testing.T, mode Mode, mut ...func(*LeaseConfig)) (env *h8bEnv, m1, m2 *jfsMount, A, B uint64) {
	env = newH8BEnv(t, mode, mut...)
	m1 = env.mount("mac1")
	m2 = env.mount("mac2")
	m1.mustMkdirAll("Projects/A")
	m1.mustMkdirAll("Projects/B")
	A, B = m1.ino("Projects/A"), m1.ino("Projects/B")
	time.Sleep(200 * time.Millisecond)
	mac1 := env.raw("mac1")
	for _, pr := range []uint64{A, B} {
		if st := env.status(pr); st["holder"] == "mac1" {
			v := mac1.do("JM.LEASE", "RELEASE", "ino:"+inoStr(pr), st["epoch"])
			if len(v.Elems) != 2 || string(v.Elems[0].Str) != "RELEASED" {
				t.Fatalf("setup RELEASE %d: %c %q %v", pr, v.Type, v.Str, v.Elems)
			}
		}
	}
	env.mustHolder(A, "")
	env.mustHolder(B, "")
	return
}

func hdentry(t *testing.T, env *h8bEnv, dir uint64, name string) bool {
	t.Helper()
	n, err := env.rdb.HExists(ctx5(t), "d"+inoStr(dir), name).Result()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// ---------------------------------------------------------------------------
// REDs.

// A project held by mac1 refuses every kind of write from mac2 (create,
// write, rename, unlink, setxattr, mkdir, chmod); the other project stays
// writable, and the first write there grants it to mac2.
func TestH8BSecondPrincipalCannotWriteHeldProject(t *testing.T) {
	env, m1, m2, A, B := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f1", "hello")
	env.mustHolder(A, "mac1")
	f1 := m1.ino("Projects/A/f1")

	refused(t, m2.write("Projects/A/x", "nope"), "mac2 create in A")
	if hdentry(t, env, A, "x") {
		t.Fatal("refused create left a dentry")
	}
	fh, err := os.OpenFile(m2.path("Projects/A/f1"), os.O_WRONLY|os.O_APPEND, 0)
	if err == nil {
		fh.Write([]byte(" world"))
		err = fh.Close()
	}
	refused(t, err, "mac2 write to A/f1")
	if b, _ := os.ReadFile(m1.path("Projects/A/f1")); string(b) != "hello" {
		t.Fatalf("A/f1 = %q after a refused write", b)
	}
	refused(t, os.Rename(m2.path("Projects/A/f1"), m2.path("Projects/A/f2")), "mac2 rename in A")
	refused(t, os.Remove(m2.path("Projects/A/f1")), "mac2 unlink in A")
	refused(t, unix.Setxattr(m2.path("Projects/A/f1"), "user.k", []byte("v"), 0), "mac2 setxattr in A")
	if n, _ := env.rdb.HExists(ctx5(t), "x"+inoStr(f1), "user.k").Result(); n {
		t.Fatal("refused setxattr wrote user.k")
	}
	refused(t, os.Mkdir(m2.path("Projects/A/d"), 0o755), "mac2 mkdir in A")
	refused(t, os.Chmod(m2.path("Projects/A/f1"), 0o600), "mac2 chmod in A")
	if !hdentry(t, env, A, "f1") || hdentry(t, env, A, "f2") || hdentry(t, env, A, "d") {
		t.Fatal("A's entries changed after refused writes")
	}

	m2.mustWrite("Projects/B/y", "mine")
	env.mustHolder(B, "mac2")
	m1.mustWrite("Projects/A/f3", "still mine")
	refused(t, m1.write("Projects/B/z", "nope"), "mac1 create in B")
	if err := unix.Setxattr(m1.path("Projects/A/f1"), "user.k", []byte("v"), 0); err != nil {
		t.Fatalf("holder setxattr: %v", err)
	}
	mustHaveDenies(t, env, "mac2", ReasonLeaseHeld)
}

func mustHaveDenies(t *testing.T, env *h8bEnv, principal string, r Reason) {
	t.Helper()
	for _, d := range env.p.Stats.Snapshot().Decisions {
		if d.Principal == principal && d.Reason == r && d.Outcome == DecDeny && d.Count > 0 {
			return
		}
	}
	t.Fatalf("no %s refusal recorded for %s", r, principal)
}

// An unlink into the trash is a write to the SOURCE project: the holder's
// unlink lands in the trash, a non-holder's is refused, and an unlink in a
// free project grants that project.
func TestH8BTrashUnlinkAttributedToSourceProject(t *testing.T) {
	env, m1, m2, A, B := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f", "f")
	m1.mustWrite("Projects/A/g", "g")
	fIno := m1.ino("Projects/A/f")
	if err := os.Remove(m1.path("Projects/A/f")); err != nil {
		t.Fatalf("holder unlink into trash: %v\nrefusals: %s", err, env.recent())
	}
	if hdentry(t, env, A, "f") {
		t.Fatal("holder unlink left the dentry")
	}
	if !inTrash(t, env, fIno) {
		t.Fatal("unlinked file is not in the trash (trash disabled?)")
	}
	refused(t, os.Remove(m2.path("Projects/A/g")), "mac2 unlink in A")
	if !hdentry(t, env, A, "g") {
		t.Fatal("refused unlink removed A/g")
	}
	env.mustHolder(A, "mac1")

	// B: created by mac1, released, then unlinked by mac2.
	m1.mustWrite("Projects/B/h", "h")
	st := env.mustHolder(B, "mac1")
	if v := env.raw("mac1").do("JM.LEASE", "RELEASE", "ino:"+inoStr(B), st["epoch"]); len(v.Elems) != 2 || string(v.Elems[0].Str) != "RELEASED" {
		t.Fatalf("RELEASE B: %c %q %v", v.Type, v.Str, v.Elems)
	}
	if err := os.Remove(m2.path("Projects/B/h")); err != nil {
		t.Fatalf("mac2 unlink in free B: %v", err)
	}
	env.mustHolder(B, "mac2")
	env.mustHolder(A, "mac1")

	// Same-inode pairing: mac2 unlinks its own B file but also plants a
	// trash entry for A's g in the same txn (bg trash cleanup would then
	// delete A's data). Refused; nothing lands.
	m2.mustWrite("Projects/B/own", "o")
	own, g := m2.ino("Projects/B/own"), m1.ino("Projects/A/g")
	trashDir := trashDirOf(t, env)
	// A is free, so only the pairing rule (not A's lease) can refuse it.
	stA := env.mustHolder(A, "mac1")
	if v := env.raw("mac1").do("JM.LEASE", "RELEASE", "ino:"+inoStr(A), stA["epoch"]); string(v.Elems[0].Str) != "RELEASED" {
		t.Fatalf("RELEASE A: %v", v.Elems)
	}
	rc := env.raw("mac2")
	v := txnRaw(rc,
		[]string{"HDEL", "d" + inoStr(B), "own"},
		[]string{"HSET", "d" + inoStr(trashDir), inoStr(B) + "-" + inoStr(own) + "-own", string(append([]byte{1}, be64(own)...))},
		[]string{"HSET", "d" + inoStr(trashDir), inoStr(A) + "-" + inoStr(g) + "-g", string(append([]byte{1}, be64(g)...))})
	mustDeny(t, v, ReasonBGOnly, "trash entry for A's g paired with mac2's own unlink")
	if ok, _ := env.rdb.HExists(ctx5(t), "d"+inoStr(trashDir), inoStr(A)+"-"+inoStr(g)+"-g").Result(); ok {
		t.Fatal("forged trash entry for A's file landed")
	}
	if !hdentry(t, env, B, "own") {
		t.Fatal("refused txn removed B/own")
	}
	env.mustHolder(A, "")
}

func trashDirOf(t *testing.T, env *h8bEnv) uint64 {
	keys, err := env.rdb.Keys(ctx5(t), "d*").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if r := ParseKey(k); r.Shape == ShapeDentry && r.Trash && r.Ino != TrashInode {
			return r.Ino
		}
	}
	t.Fatal("no hourly trash directory")
	return 0
}

func txnRaw(rc *rawConn, cmds ...[]string) Value {
	rc.send("MULTI")
	for _, c := range cmds {
		rc.send(c...)
	}
	rc.send("EXEC")
	rc.flush()
	rc.read()
	for range cmds {
		rc.read()
	}
	return rc.read()
}

func inTrash(t *testing.T, env *h8bEnv, ino uint64) bool {
	ctx := ctx5(t)
	keys, err := env.rdb.Keys(ctx, "d*").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		r := ParseKey(k)
		if r.Shape != ShapeDentry || !r.Trash {
			continue
		}
		m, _ := env.rdb.HGetAll(ctx, k).Result()
		for f := range m {
			if strings.Contains(f, "-"+inoStr(ino)+"-") {
				return true
			}
		}
	}
	return false
}

// Moving a directory between projects moves ownership of its subtree: after
// the move a child is checked against the destination project, so the old
// holder of the source project is refused there.
func TestH8BCrossProjectDirRenameMovesOwnership(t *testing.T) {
	env, m1, m2, A, B := setupTwo(t, ModeEnforce)
	m1.mustMkdirAll("Projects/A/X/sub")
	m1.mustWrite("Projects/A/X/sub/file", "1")
	env.mustHolder(A, "mac1")
	refused(t, m2.write("Projects/A/X/sub/file2", "2"), "mac2 write in A/X/sub")

	if err := os.Rename(m1.path("Projects/A/X"), m1.path("Projects/B/X")); err != nil {
		t.Fatalf("mac1 move A/X -> B/X (B free): %v", err)
	}
	st := env.mustHolder(B, "mac1") // all-or-none grant of both projects
	if v := env.raw("mac1").do("JM.LEASE", "RELEASE", "ino:"+inoStr(B), st["epoch"]); len(v.Elems) != 2 || string(v.Elems[0].Str) != "RELEASED" {
		t.Fatalf("RELEASE B: %c %q", v.Type, v.Str)
	}
	m2.mustWrite("Projects/B/other", "x")
	env.mustHolder(B, "mac2")

	sub := m1.ino("Projects/B/X/sub")
	refused(t, m1.write("Projects/B/X/sub/file3", "3"), "old holder (mac1) write in moved B/X/sub")
	if hdentry(t, env, sub, "file3") {
		t.Fatal("refused write created B/X/sub/file3")
	}
	m2.mustWrite("Projects/B/X/sub/file4", "4")
	m1.mustWrite("Projects/A/still", "a")

	// Both leases or nothing: mac1 holds A, mac2 holds B.
	m1.mustMkdirAll("Projects/A/Y")
	refused(t, os.Rename(m1.path("Projects/A/Y"), m1.path("Projects/B/Y")), "mac1 move A/Y -> B/Y while mac2 holds B")
	if !hdentry(t, env, A, "Y") || hdentry(t, env, B, "Y") {
		t.Fatal("refused cross-project move changed the tree")
	}
	env.mustHolder(A, "mac1")
	env.mustHolder(B, "mac2")
	mustHaveDenies(t, env, "mac1", ReasonXProject)
}

// After a proxy restart (empty caches) a deep, pre-existing inode still
// resolves to its project, and the lease survives in Redis.
func TestH8BPreexistingInodeResolvedAfterProxyRestart(t *testing.T) {
	env, m1, m2, A, _ := setupTwo(t, ModeEnforce)
	m1.mustMkdirAll("Projects/A/deep/er")
	m1.mustWrite("Projects/A/deep/er/file.txt", "data")
	fIno := m1.ino("Projects/A/deep/er/file.txt")
	env.mustHolder(A, "mac1")
	time.Sleep(300 * time.Millisecond) // let the flusher persist touches

	env.restartProxy()
	waitMount(t, m1)
	waitMount(t, m2)
	if _, ok := env.eng.anc.files.get(fIno); ok {
		t.Fatal("fresh engine already knows the file (test proves nothing)")
	}
	env.mustHolder(A, "mac1")

	refused(t, unix.Setxattr(m2.path("Projects/A/deep/er/file.txt"), "user.k", []byte("v"), 0), "mac2 setxattr after restart")
	// The refusal came from resolving the file through Redis (a refused
	// unit commits nothing, so the side read stays cached).
	if p, ok := env.eng.anc.files.get(fIno); !ok || p != m1.ino("Projects/A/deep/er") {
		t.Fatalf("pre-existing file was not resolved through the engine (cached parent %d, %v)", p, ok)
	}
	refused(t, os.Chmod(m2.path("Projects/A/deep/er/file.txt"), 0o600), "mac2 chmod after restart")
	refused(t, m2.write("Projects/A/deep/er/new", "x"), "mac2 create after restart")
	if err := unix.Setxattr(m1.path("Projects/A/deep/er/file.txt"), "user.k", []byte("v"), 0); err != nil {
		t.Fatalf("holder setxattr after restart: %v", err)
	}
	m1.mustWrite("Projects/A/deep/er/file2", "y")
	env.mustHolder(A, "mac1")
}

// waitMount waits until a mount answers again (after a proxy restart).
func waitMount(t *testing.T, m *jfsMount) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.ReadDir(m.path("Projects")); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("mount %s did not recover after the proxy restart:\n%s", m.name, m.tail())
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// A transfer (BREAK, then a grant to another principal) waits for every
// EXEC decided under the old holder: none of them commits after the grant.
func TestH8BTransferFencesInflightExec(t *testing.T) {
	env, m1, _, A, _ := setupTwo(t, ModeEnforce, func(c *LeaseConfig) { c.FenceHoldMax = time.Second })
	m1.mustWrite("Projects/A/f", "")
	st := env.mustHolder(A, "mac1")
	oldEpoch, _ := strconv.ParseUint(st["epoch"], 10, 64)

	var seq atomic.Int64
	var mu sync.Mutex
	var commits []int64 // seq of mac1 commits decided under oldEpoch
	paused, resume := make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	armed.Store(true)
	admitted := func(p Principal, u *Unit) {
		if p.Name != "mac1" {
			return
		}
		for _, w := range u.Project {
			if w.Key.Shape == ShapeChunk && armed.CompareAndSwap(true, false) {
				close(paused)
				<-resume
				return
			}
		}
	}
	doneHook := func(p Principal, u *Unit, o Outcome, epochs map[uint64]uint64) {
		if p.Name == "mac1" && o.Committed && epochs[A] == oldEpoch {
			s := seq.Add(1)
			mu.Lock()
			commits = append(commits, s)
			mu.Unlock()
		}
	}
	env.eng.hooks.Store(&engineHooks{admitted: admitted, done: doneHook})
	defer env.eng.hooks.Store(nil)

	wrote := make(chan error, 1)
	go func() { wrote <- m1.write("Projects/A/f", strings.Repeat("x", 1<<16)) }()
	select {
	case <-paused:
	case <-time.After(10 * time.Second):
		close(resume)
		t.Fatal("mac1's data commit never reached the decision")
	}
	admin := env.raw("admin1")
	broke := make(chan Value, 1)
	go func() { broke <- admin.doLong(30*time.Second, "JM.LEASE", "BREAK", "ino:"+inoStr(A)) }()
	select {
	case v := <-broke:
		close(resume)
		t.Fatalf("BREAK returned %c %q %v while an EXEC decided under the old holder was in flight", v.Type, v.Str, v.Elems)
	case <-time.After(700 * time.Millisecond):
	}
	close(resume)
	var bv Value
	select {
	case bv = <-broke:
	case <-time.After(15 * time.Second):
		t.Fatal("BREAK never completed")
	}
	breakSeq := seq.Add(1)
	if len(bv.Elems) != 3 || string(bv.Elems[0].Str) != "BROKEN" {
		t.Fatalf("BREAK: %c %q %v", bv.Type, bv.Str, bv.Elems)
	}
	gv := env.raw("mac2").do("JM.LEASE", "ACQUIRE", "ino:"+inoStr(A), "auto")
	grantSeq := seq.Add(1)
	if len(gv.Elems) != 5 || string(gv.Elems[0].Str) != "GRANTED" || uint64(gv.Elems[1].Int) <= oldEpoch {
		t.Fatalf("mac2 ACQUIRE after BREAK: %c %q %v", gv.Type, gv.Str, gv.Elems)
	}
	if gv.Elems[4].Int != 1 {
		t.Fatalf("foreign_since = %d, want 1 (mac1 held A after mac2's last epoch... mac2 never held it)", gv.Elems[4].Int)
	}
	if err := <-wrote; err != nil {
		t.Fatalf("mac1's write decided before the break should have committed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(commits) == 0 {
		t.Fatal("no mac1 commit under the old epoch observed (test proves nothing)")
	}
	for _, c := range commits {
		if c > breakSeq || c > grantSeq {
			t.Fatalf("a unit decided under epoch %d committed after the transfer (commit seq %d, break %d, grant %d)", oldEpoch, c, breakSeq, grantSeq)
		}
	}
	refused(t, m1.write("Projects/A/after", "x"), "mac1 write after the transfer")
	env.mustHolder(A, "mac2")

	// Part 2: an admitted EXEC that has not reached Redis yet. mac2 (now
	// the holder) sends MULTI/HSET/EXEC followed by an incomplete command,
	// so the proxy keeps the decided EXEC in its upstream buffer. A BREAK
	// and a grant to mac1 must not let that EXEC commit afterwards: the
	// fence holds until the engine has killed mac2's upstream connection
	// (FenceHoldMax), after which the EXEC can never execute.
	fIno := m1.ino("Projects/A/f")
	stuck := env.raw("mac2")
	stuck.send("MULTI")
	stuck.send("HSET", "x"+inoStr(fIno), "user.fence", "late")
	stuck.send("EXEC")
	stuck.w.WriteString("*1\r\n$4\r\nPI") // incomplete PING
	stuck.flush()
	time.Sleep(300 * time.Millisecond)
	t0 := time.Now()
	bv = env.raw("admin1").doLong(20*time.Second, "JM.LEASE", "BREAK", "ino:"+inoStr(A))
	if len(bv.Elems) != 3 || string(bv.Elems[0].Str) != "BROKEN" {
		t.Fatalf("part 2 BREAK: %c %q %v", bv.Type, bv.Str, bv.Elems)
	}
	if took := time.Since(t0); took < 500*time.Millisecond {
		t.Fatalf("part 2: BREAK returned in %s while mac2's decided EXEC was still in flight", took)
	}
	gv = env.raw("mac1").do("JM.LEASE", "ACQUIRE", "ino:"+inoStr(A), "auto")
	if len(gv.Elems) != 5 || string(gv.Elems[0].Str) != "GRANTED" {
		t.Fatalf("part 2 mac1 ACQUIRE: %c %q %v", gv.Type, gv.Str, gv.Elems)
	}
	stuck.w.WriteString("NG\r\n") // complete the command: the proxy flushes
	stuck.w.Flush()
	time.Sleep(700 * time.Millisecond)
	if v, _ := env.rdb.HGet(ctx5(t), "x"+inoStr(fIno), "user.fence").Result(); v == "late" {
		t.Fatal("part 2: an EXEC decided under mac2 committed after A was granted to mac1")
	}
}

// An auto lease is never dropped for idleness while the heartbeat reports
// pending spool rows for the project; it drops after idle_s without them,
// after orphan_s without any sign of life, and a checkout survives offline
// until it ends.
func TestH8BIdleNeverDropsWithPendingSpool(t *testing.T) {
	env, m1, m2, A, _ := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f", "f")
	env.mustHolder(A, "mac1")
	mac1 := env.raw("mac1")
	hb := func(pending ...string) {
		args := append([]string{"JM.LEASE", "HEARTBEAT"}, pending...)
		v := mac1.do(args...)
		if v.Type == '-' {
			t.Fatalf("HEARTBEAT: %s", v.Str)
		}
	}
	for i := 0; i < 54; i++ { // 4.5 h of 5-minute steps
		env.clk.Advance(5 * time.Minute)
		hb("path:/Projects/A")
		env.eng.sweep()
	}
	env.mustHolder(A, "mac1")

	env.clk.Advance(5 * time.Minute)
	hb() // no pending spool any more: the idle rule applies
	env.eng.sweep()
	env.mustHolder(A, "")

	m1.mustWrite("Projects/A/g", "g") // re-granted
	env.mustHolder(A, "mac1")
	env.clk.Advance(9 * time.Minute)
	env.eng.sweep()
	env.mustHolder(A, "mac1")
	env.clk.Advance(2 * time.Minute) // 11 min with no heartbeat or write
	env.eng.sweep()
	env.mustHolder(A, "")

	v := mac1.do("JM.LEASE", "ACQUIRE", "path:/Projects/A", "checkout", "3600")
	if len(v.Elems) != 5 || string(v.Elems[0].Str) != "GRANTED" {
		t.Fatalf("checkout: %c %q %v", v.Type, v.Str, v.Elems)
	}
	env.clk.Advance(50 * time.Minute) // offline, no heartbeat
	env.eng.sweep()
	env.mustHolder(A, "mac1")
	refused(t, m2.write("Projects/A/x", "x"), "mac2 write during mac1's checkout")
	env.clk.Advance(11 * time.Minute)
	env.eng.sweep()
	env.mustHolder(A, "")
	m2.mustWrite("Projects/A/x", "x")
	env.mustHolder(A, "mac2")
}

// observe mode (switched through jmlease:v1:cfg within 2 s) admits a write
// enforce would refuse, logs WOULD_DENY to jmlease:v1:log, and transfers
// nothing.
func TestH8BObserveAdmitsAndLogsWouldDeny(t *testing.T) {
	env, m1, m2, A, _ := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f", "f")
	env.mustHolder(A, "mac1")
	refused(t, m2.write("Projects/A/x", "x"), "mac2 write in enforce")

	setMode := func(m Mode) {
		if err := env.rdb.HSet(ctx5(t), ConfigKey, "mode", string(m)).Err(); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2500 * time.Millisecond)
		for env.p.Mode() != m {
			if time.Now().After(deadline) {
				t.Fatalf("mode %s not applied within 2 s", m)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	setMode(ModeObserve)
	if err := m2.write("Projects/A/x", "x"); err != nil {
		t.Fatalf("observe mode refused mac2's write: %v", err)
	}
	if !hdentry(t, env, A, "x") {
		t.Fatal("observe-admitted write is not in Redis")
	}
	env.mustHolder(A, "mac1")

	deadline := time.Now().Add(3 * time.Second)
	found := false
	for !found && time.Now().Before(deadline) {
		ents, err := env.rdb.XRange(ctx5(t), LeaseLogKey, "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range ents {
			if e.Values["event"] == "would_deny" && e.Values["reason"] == string(ReasonLeaseHeld) &&
				e.Values["actor"] == "mac2" && e.Values["proj"] == inoStr(A) && e.Values["holder"] == "mac1" {
				found = true
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !found {
		t.Fatal("no would_deny LEASE_HELD entry for mac2 in jmlease:v1:log")
	}
	wd := false
	for _, d := range env.p.Stats.Snapshot().Decisions {
		if d.Principal == "mac2" && d.Outcome == DecWouldDeny && d.Reason == ReasonLeaseHeld {
			wd = true
		}
	}
	if !wd {
		t.Fatal("proxy stats have no would_deny for mac2")
	}
	setMode(ModeEnforce)
	refused(t, m2.write("Projects/A/y", "y"), "mac2 write after returning to enforce")
}

// The juicefs dir-stat flush (bare HINCRBY on dirUsedSpace etc.) of a
// project the principal released within 5 min is admitted; a principal
// that never held it, or released it longer ago, is refused BG_ONLY.
func TestH8BDirStatGraceAfterRelease(t *testing.T) {
	env, m1, _, A, _ := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f", strings.Repeat("d", 10000))
	st := env.mustHolder(A, "mac1")
	mac1, mac2 := env.raw("mac1"), env.raw("mac2")
	if v := mac1.do("JM.LEASE", "RELEASE", "ino:"+inoStr(A), st["epoch"]); len(v.Elems) != 2 || string(v.Elems[0].Str) != "RELEASED" {
		t.Fatalf("RELEASE: %c %q", v.Type, v.Str)
	}
	env.mustHolder(A, "")
	// The mount's own flush after the release must not be refused.
	m1.mustWrite("Projects/B/unrelated", "u") // keeps the mount busy past a flush period
	time.Sleep(1500 * time.Millisecond)
	for _, d := range env.p.Stats.Snapshot().Decisions {
		if d.Principal == "mac1" && d.Outcome == DecDeny {
			t.Fatalf("mac1 was refused after its release: %+v (recent %+v)", d, env.p.Stats.Snapshot().Recent)
		}
	}
	field := inoStr(A)
	mustOK(t, mac1.do("HINCRBY", "dirUsedSpace", field, "0"), "mac1 dir-stat flush within grace")
	mustOK(t, mac1.do("HINCRBY", "dirDataLength", field, "0"), "mac1 dir-stat flush within grace")
	mustDeny(t, mac2.do("HINCRBY", "dirUsedSpace", field, "0"), ReasonBGOnly, "mac2 never held A")
	env.clk.Advance(DirStatGrace + time.Minute)
	mustDeny(t, mac1.do("HINCRBY", "dirUsedSpace", field, "0"), ReasonBGOnly, "mac1 after the grace")
	mustOK(t, env.raw("bg1").do("HINCRBY", "dirUsedSpace", field, "0"), "bg flush")
	env.mustHolder(A, "") // dir-stat never grants
}

// No client, holder or not, can write the lease table; only an admin can
// break a lease, and the engine's view is unchanged by the attempts.
func TestH8BLeaseKeysClientWriteRefused(t *testing.T) {
	env, m1, m2, A, B := setupTwo(t, ModeEnforce)
	m1.mustWrite("Projects/A/f", "f")
	st := env.mustHolder(A, "mac1")
	lk := LeaseKeyPrefix + inoStr(A)
	for _, who := range []string{"mac1", "mac2", "farm1", "mgr1", "bg1"} {
		rc := env.raw(who)
		for _, cmd := range [][]string{
			{"HSET", lk, "holder", "mac2"},
			{"DEL", lk},
			{"SET", EpochKeyPrefix + inoStr(A), "0"},
			{"HSET", ConfigKey, "mode", "passthrough"},
			{"HDEL", ConfigKey, "mode"},
		} {
			mustDeny(t, rc.do(cmd...), ReasonAdminOnly, who+" "+strings.Join(cmd, " "))
		}
		mustDeny(t, rc.do("XADD", LeaseLogKey, "*", "event", "grant"), ReasonAdminOnly, who+" XADD log")
		// A forged lease write hidden inside an otherwise legitimate txn.
		rc.send("MULTI")
		rc.send("HSET", "d"+inoStr(B), "forged", string(append([]byte{1}, be64(999999)...)))
		rc.send("HSET", lk, "holder", who)
		rc.send("EXEC")
		rc.flush()
		rc.read()
		rc.read()
		rc.read()
		v := rc.read()
		mustDeny(t, v, ReasonAdminOnly, who+" lease write inside MULTI")
		if who != "mac1" {
			v := rc.do("JM.LEASE", "BREAK", "ino:"+inoStr(A))
			mustDeny(t, v, ReasonAdminOnly, who+" JM.LEASE BREAK")
		}
	}
	if hdentry(t, env, B, "forged") {
		t.Fatal("the refused txn's dentry write applied")
	}
	if h, _ := env.rdb.HGet(ctx5(t), lk, "holder").Result(); h != "mac1" {
		t.Fatalf("lease hash holder = %q", h)
	}
	if env.p.Mode() != ModeEnforce {
		t.Fatalf("mode changed to %s", env.p.Mode())
	}
	if got := env.mustHolder(A, "mac1"); got["epoch"] != st["epoch"] {
		t.Fatalf("epoch moved %s -> %s", st["epoch"], got["epoch"])
	}
	refused(t, m2.write("Projects/A/x", "x"), "mac2 write after the forgery attempts")
}

func appendTo(p, s string) error {
	fh, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	_, err = fh.Write([]byte(s))
	if e := fh.Close(); err == nil {
		err = e
	}
	return err
}

// Review H8B-L2 #1 with real mounts: B's holder cannot hard-link A's file
// into B, so it cannot write A's data through the link, and A's holder
// keeps writing its own file.
func TestH8BHardlinkAcrossProjectsRefused(t *testing.T) {
	env, m1, m2, A, B := setupTwo(t, ModeEnforce)
	m2.mustWrite("Projects/A/f", "owned-by-mac2")
	env.mustHolder(A, "mac2")
	m1.mustWrite("Projects/B/y", "mine")
	env.mustHolder(B, "mac1")
	refused(t, os.Link(m1.path("Projects/A/f"), m1.path("Projects/B/lnk")), "mac1 ln A/f B/lnk")
	if hdentry(t, env, B, "lnk") {
		t.Fatal("B/lnk exists")
	}
	f := m2.ino("Projects/A/f")
	if has, _ := env.rdb.HExists(ctx5(t), "x"+inoStr(f), "user.evil").Result(); has {
		t.Fatal("mac1 wrote an xattr on A's file")
	}
	if err := appendTo(m2.path("Projects/A/f"), "-mac2"); err != nil {
		t.Fatalf("A's holder append to its own file: %v", err)
	}
	if b, _ := os.ReadFile(m2.path("Projects/A/f")); string(b) != "owned-by-mac2-mac2" {
		t.Fatalf("A/f = %q", b)
	}
	// A link inside one project is fine.
	if err := os.Link(m1.path("Projects/B/y"), m1.path("Projects/B/y2")); err != nil {
		t.Fatalf("same-project hard link: %v", err)
	}
	m1.mustWrite("Projects/B/y2", "via-link")
	env.mustHolder(B, "mac1")
}
