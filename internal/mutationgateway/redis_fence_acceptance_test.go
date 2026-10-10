//go:build redisfenceacceptance

package mutationgateway

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These tests qualify a Redis primitive, not a media receipt or a production
// gateway takeover. They start only their own Unix-socket server. No endpoint
// argument is accepted, and the tagged gate fails if the pinned binary is absent.
type fenceRedisFixture struct {
	t           *testing.T
	ctx         context.Context
	dir, socket string
	binary      string
	adminSecret string
	admin       *redis.Client
	cmd         *exec.Cmd
	done        chan error
}

func newFenceRedis(t *testing.T) *fenceRedisFixture {
	t.Helper()
	binary := os.Getenv("JM_REDIS_FENCE_SERVER")
	want := os.Getenv("JM_REDIS_FENCE_SERVER_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("tagged gate requires an absolute JM_REDIS_FENCE_SERVER and its SHA256")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != want {
		t.Fatal("Redis binary differs from pinned SHA256")
	}
	version, err := exec.Command(binary, "--version").Output()
	if err != nil || !strings.Contains(string(version), "v=7.4.11 ") {
		t.Fatal("Redis gate requires the pinned 7.4.11 build")
	}
	base := os.Getenv("TMPDIR")
	clean := filepath.Clean(base)
	if !filepath.IsAbs(base) || clean == "/tmp" || clean == "/private/tmp" || clean == "/var/tmp" {
		t.Fatal("set TMPDIR to a private persistent scratch directory")
	}
	// Short names also keep the Unix socket below Darwin's path length limit.
	dir, err := os.MkdirTemp(base, "rf-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	f := &fenceRedisFixture{t: t, ctx: ctx, dir: dir, socket: filepath.Join(dir, "r.sock"), binary: binary, adminSecret: fenceSecret(t)}
	if len(f.socket) >= 104 {
		t.Fatal("fixture Unix socket path exceeds platform limit")
	}
	adminHash := sha256.Sum256([]byte(f.adminSecret))
	acl := fmt.Sprintf("user default reset off -@all\nuser control on #%x ~* &* +@all\n", adminHash)
	if err := os.WriteFile(filepath.Join(dir, "users.acl"), []byte(acl), 0600); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("port 0\nunixsocket %q\nunixsocketperm 700\ndir %q\naclfile %q\nsave \"\"\nappendonly yes\nappendfsync always\nlua-time-limit 50\nloglevel warning\n", f.socket, dir, filepath.Join(dir, "users.acl"))
	if err := os.WriteFile(filepath.Join(dir, "redis.conf"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.stop)
	f.start()
	return f
}

func fenceSecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func (f *fenceRedisFixture) client(user, secret string) *redis.Client {
	return redis.NewClient(&redis.Options{Network: "unix", Addr: f.socket, Username: user, Password: secret,
		Protocol: 2, DisableIdentity: true, MaxRetries: -1, PoolSize: 4,
		DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
}

func (f *fenceRedisFixture) start() {
	f.t.Helper()
	log, err := os.OpenFile(filepath.Join(f.dir, "server.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		f.t.Fatal(err)
	}
	f.cmd = exec.Command(f.binary, filepath.Join(f.dir, "redis.conf"))
	f.cmd.Stdout, f.cmd.Stderr = log, log
	if err := f.cmd.Start(); err != nil {
		_ = log.Close()
		f.cmd = nil
		f.t.Fatal(err)
	}
	f.t.Logf("owned Redis started pid=%d", f.cmd.Process.Pid)
	f.done = make(chan error, 1)
	cmd, done := f.cmd, f.done
	go func() { err := cmd.Wait(); _ = log.Close(); done <- err }()
	f.admin = f.client("control", f.adminSecret)
	f.until("owned Redis ready", func() bool { return f.admin.Ping(f.ctx).Err() == nil })
	info, err := f.admin.Info(f.ctx, "server", "replication", "cluster").Result()
	if err != nil || fenceInfo(info, "redis_version") != "7.4.11" || fenceInfo(info, "redis_mode") != "standalone" || fenceInfo(info, "role") != "master" || fenceInfo(info, "connected_slaves") != "0" || fenceInfo(info, "cluster_enabled") != "0" {
		f.t.Fatal("fixture is not a standalone unreplicated Redis 7.4.11 primary")
	}
	f.requireDefaultDenied()
}

func (f *fenceRedisFixture) stop() {
	if f.admin != nil {
		_ = f.admin.Close()
		f.admin = nil
	}
	if f.cmd == nil {
		return
	}
	pid := f.cmd.Process.Pid
	_ = f.cmd.Process.Signal(os.Interrupt)
	select {
	case <-f.done:
	case <-time.After(3 * time.Second):
		_ = f.cmd.Process.Kill()
		<-f.done
	}
	f.cmd = nil
	f.t.Logf("owned Redis reaped pid=%d", pid)
}

func (f *fenceRedisFixture) until(label string, predicate func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && f.ctx.Err() == nil {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("timed out waiting for %s", label)
}

func fenceInfo(info, key string) string {
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), key+":"); ok {
			return value
		}
	}
	return ""
}

func (f *fenceRedisFixture) runID() string {
	f.t.Helper()
	info, err := f.admin.Info(f.ctx, "server").Result()
	if err != nil || len(fenceInfo(info, "run_id")) != 40 {
		f.t.Fatal("cannot establish exact Redis process identity")
	}
	return fenceInfo(info, "run_id")
}

func (f *fenceRedisFixture) worker(user string) string {
	f.t.Helper()
	secret := fenceSecret(f.t)
	err := f.admin.ACLSetUser(f.ctx, user, "reset", "on", ">"+secret, "~*", "&*", "+get", "+set", "+ping", "+auth", "+hello", "+quit", "+select", "+multi", "+exec", "+discard", "+watch", "+unwatch", "+client|id", "+client|setinfo", "+blpop", "+lpush", "+eval", "+time").Err()
	if err != nil {
		f.t.Fatalf("configure restricted fixture worker: %v", err)
	}
	return secret
}

func (f *fenceRedisFixture) requireDefaultDenied() {
	f.t.Helper()
	value, err := f.admin.Do(f.ctx, "ACL", "GETUSER", "default").Result()
	if err != nil {
		f.t.Fatal("cannot verify default ACL")
	}
	row, ok := value.([]any)
	if !ok || len(row)%2 != 0 {
		f.t.Fatal("unrecognized default ACL response")
	}
	off, noCommands := false, false
	for i := 0; i < len(row); i += 2 {
		switch row[i] {
		case "flags":
			flags, ok := row[i+1].([]any)
			if !ok {
				f.t.Fatal("unrecognized default ACL flags")
			}
			for _, flag := range flags {
				if flag == "on" || flag == "nopass" {
					f.t.Fatal("default user permits an authentication bypass")
				}
				off = off || flag == "off"
			}
		case "commands":
			noCommands = row[i+1] == "-@all"
		}
	}
	if !off || !noCommands {
		f.t.Fatal("default user is not explicitly off with all commands denied")
	}
}

func (f *fenceRedisFixture) list() []map[string]string {
	f.t.Helper()
	text, err := f.admin.ClientList(f.ctx).Result()
	if err != nil {
		f.t.Fatal(err)
	}
	var out []map[string]string
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		row := map[string]string{}
		for _, field := range strings.Fields(line) {
			k, v, ok := strings.Cut(field, "=")
			if ok {
				row[k] = v
			}
		}
		out = append(out, row)
	}
	return out
}

// revoke is deliberately test-local. Production must additionally enforce
// credential isolation, generation persistence, and startup ACL provenance.
func (f *fenceRedisFixture) revoke(user string, ids []string) {
	f.t.Helper()
	before := f.runID()
	f.requireDefaultDenied()
	if n, err := f.admin.ACLDelUser(f.ctx, user).Result(); err != nil || n != 1 {
		f.t.Fatalf("revocation was not acknowledged: count=%d err=%v", n, err)
	}
	f.until("all retained old client IDs removed", func() bool {
		for _, row := range f.list() {
			if row["user"] == user {
				return false
			}
			for _, id := range ids {
				if row["id"] == id {
					return false
				}
			}
		}
		return true
	})
	if f.runID() != before {
		f.t.Fatal("Redis restarted during revocation; fence is invalid")
	}
	f.requireDefaultDenied()
	if value, err := f.admin.Do(f.ctx, "ACL", "GETUSER", user).Result(); err != redis.Nil || value != nil {
		f.t.Fatalf("revoked identity still exists: err=%v", err)
	}
}

type fenceWire struct {
	conn net.Conn
	r    *bufio.Reader
}

func (f *fenceRedisFixture) wire(user, secret string) *fenceWire {
	f.t.Helper()
	conn, err := net.DialTimeout("unix", f.socket, time.Second)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = conn.Close() })
	w := &fenceWire{conn: conn, r: bufio.NewReader(conn)}
	if _, err := w.do("AUTH", user, secret); err != nil {
		f.t.Fatal("fixture authentication failed")
	}
	return w
}

func (w *fenceWire) send(args ...string) error {
	_ = w.conn.SetDeadline(time.Now().Add(3 * time.Second))
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, s := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(s), s)
	}
	_, err := io.WriteString(w.conn, b.String())
	return err
}

func (w *fenceWire) read() (any, error) {
	line, err := w.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return nil, errors.New("invalid fixture RESP reply")
	}
	value := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return value, nil
	case '-':
		return nil, errors.New(value)
	case ':':
		return strconv.ParseInt(value, 10, 64)
	case '$', '*':
		n, err := strconv.Atoi(value)
		if err != nil || n < -1 || n > 1<<20 {
			return nil, errors.New("invalid fixture RESP length")
		}
		if n == -1 {
			return nil, nil
		}
		if line[0] == '$' {
			b := make([]byte, n+2)
			_, err := io.ReadFull(w.r, b)
			return string(b[:n]), err
		}
		items := make([]any, n)
		for i := range items {
			items[i], err = w.read()
			if err != nil {
				return nil, err
			}
		}
		return items, nil
	}
	return nil, errors.New("unknown fixture RESP type")
}

func (w *fenceWire) do(args ...string) (any, error) {
	if err := w.send(args...); err != nil {
		return nil, err
	}
	return w.read()
}

func wireMust(t *testing.T, w *fenceWire, args ...string) any {
	t.Helper()
	value, err := w.do(args...)
	if err != nil {
		t.Fatalf("fixture command %s failed: %v", args[0], err)
	}
	return value
}

func TestRedisFenceOffDoesNotRevokeRetainedConnection(t *testing.T) {
	f := newFenceRedis(t)
	secret := f.worker("old")
	old := f.wire("old", secret)
	wireMust(t, old, "SET", "retained", "before")
	if err := f.admin.ACLSetUser(f.ctx, "old", "off").Err(); err != nil {
		t.Fatal(err)
	}
	// This counterexample must succeed: off prevents new AUTH but is no fence
	// for a capability that was already authenticated.
	wireMust(t, old, "SET", "retained", "after-off")
	fresh := f.client("old", secret)
	defer fresh.Close()
	if err := fresh.Set(f.ctx, "fresh", "forbidden", 0).Err(); err == nil || !strings.HasPrefix(err.Error(), "WRONGPASS ") {
		t.Fatalf("disabled worker did not receive an authentication denial: %v", err)
	}
	if got := f.admin.Get(f.ctx, "retained").Val(); got != "after-off" {
		t.Fatal("counterexample did not exercise retained writer")
	}
}

func TestRedisFenceDelUserCutsRetainedCapabilities(t *testing.T) {
	f := newFenceRedis(t)
	secret := f.worker("old")
	idle := f.wire("old", secret)
	tx := f.wire("old", secret)
	blocked := f.wire("old", secret)
	var ids []string
	for _, w := range []*fenceWire{idle, tx, blocked} {
		ids = append(ids, fmt.Sprint(wireMust(t, w, "CLIENT", "ID")))
	}
	wireMust(t, idle, "SET", "value", "old-before")
	wireMust(t, tx, "WATCH", "value")
	wireMust(t, tx, "MULTI")
	if got := wireMust(t, tx, "SET", "value", "old-queued"); got != "QUEUED" {
		t.Fatal("transaction did not queue")
	}
	if err := blocked.send("BLPOP", "block", "0"); err != nil {
		t.Fatal(err)
	}
	f.until("old blocking operation admitted", func() bool {
		for _, row := range f.list() {
			if row["id"] == ids[2] && strings.Contains(row["flags"], "b") {
				return true
			}
		}
		return false
	})
	// Input already sent behind a blocked command must also be discarded.
	if err := blocked.send("SET", "value", "old-buffered"); err != nil {
		t.Fatal(err)
	}
	pool := f.client("old", secret)
	defer pool.Close()
	var held []*redis.Conn
	for i := 0; i < 4; i++ {
		c := pool.Conn()
		held = append(held, c)
		defer c.Close()
		id, err := c.ClientID(f.ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, strconv.FormatInt(id, 10))
	}
	unique := make(map[string]bool)
	for _, id := range ids {
		unique[id] = true
	}
	if len(unique) != 7 {
		t.Fatal("fixture did not establish seven distinct old connections")
	}
	for _, args := range [][]string{{"ACL", "SETUSER", "escape", "on", "nopass", "+@all"}, {"CONFIG", "REWRITE"}, {"AUTH", "default", ""}, {"AUTH", "control", secret}} {
		if _, err := idle.do(args...); err == nil {
			t.Fatalf("restricted worker acquired alternate authority with %s", args[0])
		}
	}
	wireMust(t, idle, "SET", "value", "old-before-fence")
	f.revoke("old", ids)
	nextSecret := f.worker("successor")
	next := f.wire("successor", nextSecret)
	wireMust(t, next, "SET", "value", "successor")
	for _, attempt := range []struct {
		w    *fenceWire
		args []string
	}{{idle, []string{"SET", "value", "old-idle"}}, {tx, []string{"EXEC"}}} {
		if _, err := attempt.w.do(attempt.args...); err == nil {
			t.Fatal("old retained connection executed after fence")
		}
	}
	if _, err := blocked.read(); err == nil {
		t.Fatal("old blocked command survived revocation")
	}
	for _, c := range held {
		if err := c.Set(f.ctx, "value", "old-pool", 0).Err(); err == nil {
			t.Fatal("retained pooled connection wrote after fence")
		}
		_ = c.Close()
	}
	if err := pool.Set(f.ctx, "value", "old-reconnect", 0).Err(); err == nil || !strings.HasPrefix(err.Error(), "WRONGPASS ") {
		t.Fatalf("old pool did not receive an authentication denial: %v", err)
	}
	if err := f.admin.LPush(f.ctx, "block", "release").Err(); err != nil {
		t.Fatal(err)
	}
	if got := wireMust(t, next, "GET", "value"); got != "successor" {
		t.Fatal("old queued or buffered mutation replaced successor")
	}
	if n := f.admin.LLen(f.ctx, "block").Val(); n != 1 {
		t.Fatal("old blocked command consumed after fence")
	}
	if n, err := f.admin.ACLDelUser(f.ctx, "old").Result(); err != nil || n != 0 {
		t.Fatal("repeating acknowledged deletion was not idempotent")
	}
}

func TestRedisFenceExecutingScriptMustSettle(t *testing.T) {
	f := newFenceRedis(t)
	secret := f.worker("old")
	old := f.wire("old", secret)
	id := fmt.Sprint(wireMust(t, old, "CLIENT", "ID"))
	// Redis yields to the event loop only after lua-time-limit; BUSY proves
	// the write script is executing when revocation is attempted.
	script := `redis.call('SET',KEYS[1],'entered'); local t=redis.call('TIME'); local start=t[1]*1000000+t[2]; repeat t=redis.call('TIME') until t[1]*1000000+t[2]-start>2000000; return redis.call('SET',KEYS[1],'settled')`
	if err := old.send("EVAL", script, "1", "script-value"); err != nil {
		t.Fatal(err)
	}
	f.until("executing write script BUSY response", func() bool {
		err := f.admin.Ping(f.ctx).Err()
		return err != nil && strings.HasPrefix(err.Error(), "BUSY ")
	})
	if err := f.admin.ACLDelUser(f.ctx, "old").Err(); err == nil || !strings.HasPrefix(err.Error(), "BUSY ") {
		t.Fatalf("running script revocation must remain uncertain: %v", err)
	}
	if value, err := old.read(); err != nil || value != "OK" {
		t.Fatalf("script did not settle: %v", err)
	}
	f.revoke("old", []string{id})
	if got := f.admin.Get(f.ctx, "script-value").Val(); got != "settled" {
		t.Fatal("fence lost the pre-fence script effect")
	}
}

func TestRedisFenceRestartRequiresStaticDenyBaseline(t *testing.T) {
	for _, persist := range []bool{false, true} {
		t.Run(fmt.Sprintf("saved_worker_%t", persist), func(t *testing.T) {
			f := newFenceRedis(t)
			secret := f.worker("old")
			old := f.wire("old", secret)
			id := fmt.Sprint(wireMust(t, old, "CLIENT", "ID"))
			wireMust(t, old, "SET", "durable-marker", "before-restart")
			if persist {
				if err := f.admin.Do(f.ctx, "ACL", "SAVE").Err(); err != nil {
					t.Fatal(err)
				}
			}
			f.revoke("old", []string{id})
			before := f.runID()
			f.stop()
			f.start()
			if f.runID() == before {
				t.Fatal("restart fixture did not change Redis identity")
			}
			if got := f.admin.Get(f.ctx, "durable-marker").Val(); got != "before-restart" {
				t.Fatal("restart did not reuse the same data directory")
			}
			stale := f.client("old", secret)
			defer stale.Close()
			err := stale.Set(f.ctx, "after-restart", "stale", 0).Err()
			if persist && err != nil {
				t.Fatalf("saved-ACL resurrection counterexample not reproduced: %v", err)
			}
			if !persist && (err == nil || !strings.HasPrefix(err.Error(), "WRONGPASS ")) {
				t.Fatalf("runtime-only identity did not receive an authentication denial: %v", err)
			}
		})
	}
}
