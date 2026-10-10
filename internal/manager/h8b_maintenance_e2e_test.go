package manager

// End-to-end: the Manager's scheduled maintenance (gc --delete, gc
// --compact, fsck) and its trash-days config write run through the real
// jm-respproxy in ENFORCE mode, as the principals the Manager now uses
// (bg for gc/compact/fsck, admin for config, manager for its own sync),
// with no refusal. Opt-in: it needs real binaries and a disposable Redis.
//
//	JM_H8B_RESPPROXY_BIN=<jm-respproxy built with the L2 lease engine>
//	JM_H8B_JUICEFS=<juicefs>
//	JM_H8B_REDIS_SERVER=<redis-server 7.x>
//	JM_H8B_E2E_PORTS=<redis port>,<proxy port>      (default 58708,58709)
//	go test ./internal/manager -run TestH8BMaintenanceThroughProxyEnforce -count=1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func h8bE2EEnv(t *testing.T) (proxyBin, juicefs, redisServer, redisPort, proxyPort string) {
	proxyBin, juicefs, redisServer = os.Getenv("JM_H8B_RESPPROXY_BIN"), os.Getenv("JM_H8B_JUICEFS"), os.Getenv("JM_H8B_REDIS_SERVER")
	if proxyBin == "" || juicefs == "" || redisServer == "" {
		t.Skip("set JM_H8B_RESPPROXY_BIN, JM_H8B_JUICEFS and JM_H8B_REDIS_SERVER to run the proxy end-to-end test")
	}
	redisPort, proxyPort = "58708", "58709"
	if p := os.Getenv("JM_H8B_E2E_PORTS"); p != "" {
		redisPort, proxyPort, _ = strings.Cut(p, ",")
	}
	return
}

func waitPort(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never listened", addr)
}

func TestH8BMaintenanceThroughProxyEnforce(t *testing.T) {
	proxyBin, juicefs, redisServer, redisPort, proxyPort := h8bE2EEnv(t)
	dir := t.TempDir()
	pw := map[string]string{}
	for _, n := range []string{"jm-proxy", "admin", "bg", "manager"} {
		pw[n] = n + "-e2e-0123456789abcdef0123456789"
	}

	// Redis with the production ACL shape.
	acl := "user default off resetpass resetkeys resetchannels -@all\n" +
		"user jm-proxy on #" + sha256Hex(pw["jm-proxy"]) + " ~* &* +@all\n"
	if err := os.WriteFile(filepath.Join(dir, "users.acl"), []byte(acl), 0o600); err != nil {
		t.Fatal(err)
	}
	rs := exec.Command(redisServer, "--port", redisPort, "--bind", "127.0.0.1", "--save", "", "--appendonly", "no",
		"--dir", dir, "--notify-keyspace-events", "KEA", "--aclfile", filepath.Join(dir, "users.acl"))
	rs.Stdout, rs.Stderr = os.Stderr, os.Stderr
	if err := rs.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rs.Process.Kill(); _ = rs.Wait() })
	waitPort(t, "127.0.0.1:"+redisPort)

	// Proxy in enforce with the server principals.
	var pf proxyPrincipalsFile
	for _, k := range []string{"admin", "bg", "manager"} {
		h, err := hashSecretPHC(pw[k])
		if err != nil {
			t.Fatal(err)
		}
		pf.Principals = append(pf.Principals, proxyPrincipalEntry{Name: k, Kind: k, Argon2id: h})
	}
	body, _ := json.Marshal(pf)
	if err := writeFileAtomic0600(filepath.Join(dir, "principals.json"), body); err != nil {
		t.Fatal(err)
	}
	cfg, _ := json.Marshal(map[string]any{
		"listen": "127.0.0.1:" + proxyPort, "upstream": "127.0.0.1:" + redisPort, "upstream_user": "jm-proxy",
		"upstream_password_env": "UPW", "stats": "", "principals": filepath.Join(dir, "principals.json"), "mode": "enforce",
	})
	if err := os.WriteFile(filepath.Join(dir, "proxy.json"), cfg, 0o600); err != nil {
		t.Fatal(err)
	}
	var plog bytes.Buffer
	px := exec.Command(proxyBin, "-config", filepath.Join(dir, "proxy.json"))
	px.Env = append(os.Environ(), "UPW="+pw["jm-proxy"])
	px.Stdout, px.Stderr = &plog, &plog
	if err := px.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = px.Process.Kill(); _ = px.Wait(); t.Logf("proxy log:\n%s", plog.String()) })
	waitPort(t, "127.0.0.1:"+proxyPort)

	host := "127.0.0.1:" + proxyPort
	run := func(env []string, args ...string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		c := exec.CommandContext(ctx, juicefs, args...)
		c.Env = append(os.Environ(), env...)
		if out, err := c.CombinedOutput(); err != nil {
			t.Fatalf("juicefs %v: %v\n%s", args, err, out)
		}
	}
	// Format as admin (the volume settings are admin-only).
	bucket := filepath.Join(dir, "bucket") + "/"
	run([]string{"META_PASSWORD=" + pw["admin"]}, "format", "--storage", "file", "--bucket", bucket, "--trash-days", "0",
		"redis://admin@"+host+"/1", "e2evol")
	// Data written the way the Manager writes it: juicefs sync as `manager`.
	src := filepath.Join(dir, "src")
	_ = os.MkdirAll(filepath.Join(src, "P"), 0o755)
	for i := 0; i < 20; i++ {
		_ = os.WriteFile(filepath.Join(src, "P", fmt.Sprintf("f%02d.bin", i)), bytes.Repeat([]byte{byte(i)}, 300_000+i), 0o644)
	}
	run([]string{"e2evol=redis://manager:" + pw["manager"] + "@" + host + "/1"}, "sync", src+"/", "jfs://e2evol/data/")
	run([]string{"e2evol=redis://manager:" + pw["manager"] + "@" + host + "/1"}, "sync", filepath.Join(src, "P")+"/", "jfs://e2evol/data/P2/")
	// Rewrite one file in place several times (compaction candidates).
	// Measured 2026-10-06: with this data neither gc --delete nor gc
	// --compact issued a bg-only write, so this test proves the Manager's
	// maintenance and config run clean in enforce under bg/admin; it does
	// NOT prove bg is required for gc (the config write's ADMIN_ONLY is
	// proven: run it as manager and the proxy refuses SET setting).
	frag := filepath.Join(dir, "frag")
	_ = os.MkdirAll(frag, 0o755)
	for i := 0; i < 6; i++ {
		_ = os.WriteFile(filepath.Join(frag, "f.bin"), bytes.Repeat([]byte{byte('a' + i)}, 100_000*(i+1)), 0o644)
		mt := time.Now().Add(time.Duration(i) * time.Minute)
		_ = os.Chtimes(filepath.Join(frag, "f.bin"), mt, mt)
		run([]string{"e2evol=redis://manager:" + pw["manager"] + "@" + host + "/1"}, "sync", "--update", "--inplace", frag+"/", "jfs://e2evol/data/P/")
	}
	// Delete P2's files (trash-days 0): their slices become deleted-file
	// records that gc --delete must reclaim, which is bg-only work.
	empty := filepath.Join(dir, "empty")
	_ = os.MkdirAll(empty, 0o755)
	run([]string{"e2evol=redis://manager:" + pw["manager"] + "@" + host + "/1"}, "sync", "--delete-dst", empty+"/", "jfs://e2evol/data/P2/")

	// Finding (reported to L1/L2, not this lane's code): with trash-days 0
	// the DELETING client cleans its own slices with a bare HDEL sliceRef,
	// which the proxy refuses as BG_ONLY for `manager`. Log setup refusals;
	// assert only on the maintenance phase below.
	time.Sleep(200 * time.Millisecond)
	setupLog := plog.String()
	for _, l := range strings.Split(setupLog, "\n") {
		if strings.Contains(l, "DENY") {
			t.Logf("setup refusal (sync --delete-dst as manager): %s", l)
		}
	}

	// The Manager's own wiring: JM_META is user@host, secrets in env.
	t.Setenv("JM_META_PASSWORD", pw["manager"])
	t.Setenv("JM_META_BG_PASSWORD", pw["bg"])
	t.Setenv("JM_META_ADMIN_PASSWORD", pw["admin"])
	metaURL := withEnvMetaPassword("redis://manager@" + host + "/1")
	mm := newMaintenanceManager(juicefs, "", metaURL, "/jfs")
	if strings.Contains(mm.metaURL, pw["bg"]) || strings.Contains(mm.metaURL, pw["manager"]) || !strings.HasPrefix(mm.metaURL, "redis://bg@") {
		t.Fatalf("maintenance argv URL %q must be bg@host with no password", mm.metaURL)
	}
	for _, kind := range []MaintenanceKind{MaintenanceGC, MaintenanceCompactMeta, MaintenanceFSCK} {
		argv, ok := mm.scheduledArgv(kind)
		if !ok {
			t.Fatalf("%s not schedulable", kind)
		}
		op, err := mm.tryStart(kind, argv)
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(2 * time.Minute)
		for {
			op.mu.Lock()
			st, out, msg := op.State, strings.Join(op.Output, "\n"), op.Error
			op.mu.Unlock()
			if st == MaintenanceDone {
				break
			}
			if st == MaintenanceError || time.Now().After(deadline) {
				t.Fatalf("scheduled %s through the proxy in enforce: %s %s\n%s", kind, st, msg, out)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	// Trash retention: read as manager, write as admin.
	ctx := context.Background()
	if err := setTrashConfig(ctx, juicefs, metaURL, 3); err != nil {
		t.Fatalf("trash-days write through the proxy: %v", err)
	}
	if days, err := getTrashConfig(ctx, juicefs, metaURL); err != nil || days != 3 {
		t.Fatalf("trash-days read back = %d, %v", days, err)
	}
	time.Sleep(300 * time.Millisecond)
	for _, l := range strings.Split(strings.TrimPrefix(plog.String(), setupLog), "\n") {
		if strings.Contains(l, "DENY") {
			t.Errorf("proxy refused a Manager operation in enforce: %s", l)
		}
	}
}

// juicefs 1.3.1 `juicefs config` prints JSON; the trash-retention read
// must parse it (found by the end-to-end test above: it read -1).
func TestH8BParseTrashConfigJSON(t *testing.T) {
	raw := []byte("{\n  \"Name\": \"vvv\",\n  \"TrashDays\": 2,\n  \"MetaVersion\": 1\n}\n")
	if n, err := parseTrashConfig(raw); err != nil || n != 2 {
		t.Fatalf("parseTrashConfig(JSON) = %d, %v; want 2", n, err)
	}
}
