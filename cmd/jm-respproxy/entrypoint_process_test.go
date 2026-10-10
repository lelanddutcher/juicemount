//go:build redisoptionsacceptance && linux

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/replaceguard"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// This test runs in the owned Linux container used by the acceptance runner.
// A failed assertion must leave that container's private roots and descendants
// intact for its owner to retire; killing just the shell could orphan the CLI.
func entrypointPinnedScript(t *testing.T) string {
	t.Helper()
	path, want := os.Getenv("JM_RESPPROXY_ENTRYPOINT"), os.Getenv("JM_RESPPROXY_ENTRYPOINT_SHA256")
	if !filepath.IsAbs(path) || len(want) != 64 {
		t.Fatal("pinned absolute JM_RESPPROXY_ENTRYPOINT and SHA256 required")
	}
	if _, err := hex.DecodeString(want); err != nil {
		t.Fatalf("invalid entrypoint SHA256: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("pinned entrypoint is not a regular file: %v", err)
	}
	if got := entrypointFileHash(t, path); got != strings.ToLower(want) {
		t.Fatalf("pinned entrypoint SHA256 mismatch: %s", got)
	}
	return path
}

func entrypointFileHash(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, copyErr := io.Copy(h, f)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("hash %s: copy=%v close=%v", path, copyErr, closeErr)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func entrypointStart(t *testing.T, f *proxyCLIFixture, stageBirth bool, stats string, secrets []string) (*proxyCLIProcess, string) {
	t.Helper()
	script, binary := entrypointPinnedScript(t), proxyCLIBinary(t)
	if len(secrets) != 5 {
		t.Fatal("five synthetic principal passwords required")
	}
	dir := filepath.Dir(f.config.Principals)
	runDir := filepath.Join(t.TempDir(), "run")
	logDir := t.TempDir()
	logPath := filepath.Join(logDir, "entrypoint.log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := logFile.Chmod(0600); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	upstreamPassword := strings.Repeat("u", 40)
	env := []string{
		"PATH=" + os.Getenv("PATH"), "GOMAXPROCS=2",
		"JM_RESPPROXY_BIN=" + binary, "JM_RESPPROXY_DIR=" + dir,
		"JM_RESPPROXY_RUN_DIR=" + runDir, "JM_RESPPROXY_MODE=enforce",
		"JM_RESPPROXY_LISTEN=" + f.config.Listen, "JM_RESPPROXY_STATS=" + stats,
		"JM_RESPPROXY_UPSTREAM=" + f.config.Upstream,
		"JM_RESPPROXY_UPSTREAM_PASSWORD=" + upstreamPassword,
	}
	for i, name := range []string{"FARM", "MANAGER", "BG", "WEBDAV", "ADMIN"} {
		env = append(env, "JM_PROXY_"+name+"_PASSWORD="+secrets[i])
	}
	if stageBirth {
		env = append(env, "JM_RESPPROXY_STAGE_BIRTH=true")
	}
	cmd := exec.Command("/bin/sh", script)
	cmd.Env, cmd.Stdout, cmd.Stderr = env, logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start owned entrypoint shell: %v", err)
	}
	p := &proxyCLIProcess{
		addr: f.config.Listen, cmd: cmd, done: make(chan struct{}), logPath: logPath,
		logFile: logFile, password: upstreamPassword,
		banner: "jm-respproxy: " + f.config.Listen + " -> " + f.config.Upstream + " mode=enforce principals=6 singleton=true incarnation=",
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	t.Cleanup(func() {
		if !p.accounted {
			data, _ := os.ReadFile(logPath)
			originalBytes := len(data)
			sum := sha256.Sum256(data)
			for _, secret := range append(append([]string(nil), secrets...), upstreamPassword) {
				data = bytes.ReplaceAll(data, []byte(secret), []byte("[REDACTED]"))
			}
			fmt.Fprintf(os.Stderr, "unaccounted entrypoint shell PID=%d; preserving owned container roots and descendants; log=%s bytes=%d SHA256=%x\n%s\n",
				cmd.Process.Pid, logPath, originalBytes, sum, data)
			os.Exit(2)
		}
	})
	t.Logf("entrypoint launched original shell PID=%d script SHA256=%s CLI SHA256=%s log=%s",
		cmd.Process.Pid, strings.ToLower(os.Getenv("JM_RESPPROXY_ENTRYPOINT_SHA256")), strings.ToLower(os.Getenv("JM_RESPPROXY_CLI_BINARY_SHA256")), logPath)
	return p, runDir
}

func entrypointAwaitReady(t *testing.T, p *proxyCLIProcess, stats string) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		data := p.logBytes(t)
		if bytes.Contains(data, []byte(p.banner)) {
			cli, cliErr := net.DialTimeout("tcp", p.addr, time.Second)
			stat, statErr := net.DialTimeout("tcp", stats, time.Second)
			if cliErr == nil && statErr == nil {
				_ = cli.Close()
				_ = stat.Close()
				select {
				case <-p.done:
					t.Fatalf("entrypoint shell exited after CLI banner: %v", p.err)
				default:
				}
				return
			}
			if cli != nil {
				_ = cli.Close()
			}
			if stat != nil {
				_ = stat.Close()
			}
		}
		select {
		case <-p.done:
			t.Fatalf("entrypoint shell exited before CLI readiness: %v", p.err)
		case <-deadline.C:
			t.Fatalf("entrypoint shell PID=%d did not become ready within 30s", p.cmd.Process.Pid)
		case <-tick.C:
		}
	}
}

func entrypointConfig(t *testing.T, runDir string, f *proxyCLIFixture, stats string, stageBirth bool) string {
	t.Helper()
	path := filepath.Join(runDir, "config.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	var config fileConfig
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	wantBirth := "false"
	if stageBirth {
		wantBirth = "true"
	}
	if string(fields["stage_birth"]) != wantBirth || fields["volume_db"] != nil || fields["allowed_dbs"] != nil || fields["singleton"] != nil ||
		config.Listen != f.config.Listen || config.Upstream != f.config.Upstream || config.Stats != stats ||
		config.Principals != f.config.Principals || config.Mode != "enforce" || config.UpstreamUser != "jm-proxy" ||
		config.UpstreamPasswordEnv != "JM_RESPPROXY_UPSTREAM_PASSWORD" || !config.ReloadHandshake {
		t.Fatalf("entrypoint rendered config differs from expected defaults/fields: %s", data)
	}
	effective, err := config.proxyConfig()
	if err != nil || effective.VolumeDB != 1 || !effective.Singleton || effective.StageBirth != stageBirth {
		t.Fatalf("entrypoint effective DB 1/singleton/stage birth: %+v %v", effective, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("entrypoint config is not private: %v %v", info, err)
	}
	return path
}

func entrypointPrincipals(t *testing.T, path string, original respproxy.PrincipalEntry) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("rendered principals are not private: %v %v", info, err)
	}
	if _, err := respproxy.ParsePrincipals(data); err != nil {
		t.Fatalf("rendered principals invalid: %v", err)
	}
	var parsed struct {
		Principals []respproxy.PrincipalEntry `json:"principals"`
	}
	if err := json.Unmarshal(data, &parsed); err != nil || len(parsed.Principals) != 6 {
		t.Fatalf("entrypoint principal count: %d %v", len(parsed.Principals), err)
	}
	want := []struct{ name, kind string }{{"farm", "farm"}, {"manager", "manager"}, {"bg", "bg"}, {"webdav", "webdav"}, {"admin", "admin"}}
	for i, w := range want {
		got := parsed.Principals[i]
		if got.Name != w.name || string(got.Kind) != w.kind || got.Argon2id == "" {
			t.Fatalf("rendered server principal %d differs: %+v", i, got)
		}
	}
	if !reflect.DeepEqual(parsed.Principals[5], original) {
		t.Fatal("entrypoint changed the existing synthetic Mac principal")
	}
}

func entrypointChildIdentity(t *testing.T, f *proxyCLIFixture, p *proxyCLIProcess, configPath string, secrets []string) string {
	t.Helper()
	incarnation, err := f.db.Get(proxyCLIContext(t), respproxy.SingletonKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(incarnation, ":")
	if len(parts) < 4 {
		t.Fatalf("invalid singleton incarnation shape: %q", incarnation)
	}
	pid, err := strconv.Atoi(parts[1])
	if err != nil || pid <= 0 || pid != p.cmd.Process.Pid {
		t.Fatalf("singleton does not match original exec process PID: %q %v", incarnation, err)
	}
	proc := fmt.Sprintf("/proc/%d", pid)
	status, err := os.ReadFile(filepath.Join(proc, "status"))
	if err != nil {
		t.Fatal(err)
	}
	parent := -1
	for _, line := range strings.Split(string(status), "\n") {
		if strings.HasPrefix(line, "PPid:") {
			parent, err = strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "PPid:")))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if parent != os.Getpid() {
		t.Fatalf("singleton CLI PID=%d parent=%d, want original test parent PID=%d", pid, parent, os.Getpid())
	}
	if got := entrypointFileHash(t, filepath.Join(proc, "exe")); got != strings.ToLower(os.Getenv("JM_RESPPROXY_CLI_BINARY_SHA256")) {
		t.Fatalf("singleton CLI executable hash differs: %s", got)
	}
	argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil || !bytes.Contains(argv, []byte(configPath)) || !bytes.Contains(argv, []byte("-config")) {
		t.Fatalf("singleton CLI lacks private config argv: %v", err)
	}
	env, err := os.ReadFile(filepath.Join(proc, "environ"))
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if bytes.Contains(argv, []byte(secret)) || bytes.Contains(env, []byte(secret)) {
			t.Fatal("principal secret leaked into CLI argv or environment")
		}
	}
	if !bytes.Contains(p.logBytes(t), []byte("incarnation="+incarnation)) {
		t.Fatal("CLI banner does not match owned singleton")
	}
	t.Logf("entrypoint exec PID=%d is CLI PID=%d incarnation=%s", p.cmd.Process.Pid, pid, incarnation)
	return incarnation
}

func entrypointCreateWithoutBirth(t *testing.T, f *proxyCLIFixture, p *proxyCLIProcess, operation string) {
	t.Helper()
	conf := meta.DefaultConf()
	conf.Retries, conf.MaxDeletes, conf.NoBGJob = 0, 0, true
	endpoint := (&url.URL{Scheme: "redis", Host: p.addr, User: url.UserPassword("mac-cli", "cli-password"), Path: "/1"}).String()
	m := meta.NewClient(endpoint, conf)
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = m.Shutdown()
		}
	})
	if format, err := m.Load(false); err != nil || format == nil || format.UUID != proxyCLIVolumeUUID {
		t.Fatalf("entrypoint default-off SDK format: %+v %v", format, err)
	}
	ctx := meta.WrapContext(proxyCLIContext(t))
	var ino meta.Ino
	var attr meta.Attr
	if st := m.Create(ctx, f.project, replaceguard.BirthStageName(operation), 0600, 0, syscall.O_EXCL, &ino, &attr); st != 0 {
		t.Fatalf("entrypoint default-off SDK reserved create: %v", st)
	}
	stored, err := f.db.Get(ctx, "i"+ino.String()).Bytes()
	if err != nil || len(stored) < 71 {
		t.Fatalf("default-off raw inode missing or short: %d %v", len(stored), err)
	}
	var observed meta.Attr
	observed.Unmarshal(stored)
	entry, err := f.db.HGet(ctx, "d"+f.project.String(), replaceguard.BirthStageName(operation)).Bytes()
	if err != nil || len(entry) != 9 || entry[0] != meta.TypeFile || binary.BigEndian.Uint64(entry[1:]) != uint64(ino) || observed.Typ != meta.TypeFile || observed.Length != 0 || observed.Nlink != 1 {
		t.Fatalf("default-off raw namespace or inode differs: %v", err)
	}
	if n, err := f.db.Exists(ctx, replaceguard.BirthOperationKey(operation), replaceguard.BirthInodeKey(uint64(ino))).Result(); err != nil || n != 0 {
		t.Fatalf("entrypoint default-off created birth receipt(s): %d %v", n, err)
	}
	if st := m.Close(ctx, ino); st != 0 {
		t.Fatal(st)
	}
	if err := m.Shutdown(); err != nil {
		t.Fatal(err)
	}
	stopped = true
}

func entrypointStop(t *testing.T, f *proxyCLIFixture, p *proxyCLIProcess, stats string, secrets []string) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("entrypoint shell exited before owned SIGTERM: %v", p.err)
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM original entrypoint shell PID=%d: %v", p.cmd.Process.Pid, err)
	}
	p.joined(t)
	proxyCLINoSingleton(t, f)
	for _, secret := range secrets {
		if bytes.Contains(p.logBytes(t), []byte(secret)) {
			t.Fatal("entrypoint log exposed a principal password")
		}
	}
	p.report(t, "")
	if p.err != nil {
		t.Fatalf("entrypoint original exec process exit=%v", p.err)
	}
	p.exclusiveBind(t)
	ln, err := net.Listen("tcp", stats)
	if err != nil {
		t.Fatalf("entrypoint stats address %s retained after original Wait: %v", stats, err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	p.accounted = true
}

func TestProxyEntrypointStageBirth(t *testing.T) {
	for _, tc := range []struct {
		name, operation string
		stageBirth      bool
	}{
		{"explicit-true", strings.Repeat("c", 32), true},
		{"omitted-default-false", strings.Repeat("d", 32), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newProxyCLIFixtureDB(t, 1)
			upstreamPassword := strings.Repeat("u", 40)
			if err := f.db.Do(proxyCLIContext(t), "ACL", "SETUSER", "jm-proxy", "reset", "on", ">"+upstreamPassword, "~*", "&*", "+@all").Err(); err != nil {
				t.Fatalf("configure owned Redis jm-proxy ACL: %v", err)
			}
			originalData, err := os.ReadFile(f.config.Principals)
			if err != nil {
				t.Fatal(err)
			}
			var original struct {
				Principals []respproxy.PrincipalEntry `json:"principals"`
			}
			if err := json.Unmarshal(originalData, &original); err != nil || len(original.Principals) != 1 {
				t.Fatalf("seeded synthetic Mac principal: %v", err)
			}
			secrets := []string{strings.Repeat("f", 40), strings.Repeat("m", 40), strings.Repeat("b", 40), strings.Repeat("w", 40), strings.Repeat("a", 40)}
			stats := proxyCLIFreeAddress(t)
			p, runDir := entrypointStart(t, f, tc.stageBirth, stats, secrets)
			entrypointAwaitReady(t, p, stats)
			configPath := entrypointConfig(t, runDir, f, stats, tc.stageBirth)
			entrypointPrincipals(t, f.config.Principals, original.Principals[0])
			incarnation := entrypointChildIdentity(t, f, p, configPath, secrets)
			client := proxyCLIClientDB(t, p.addr, 1)
			epoch := proxyCLIGrant(t, client, f.project, "GRANTED", 1)
			if tc.stageBirth {
				proxyCLICreateBirth(t, f, p, tc.operation, incarnation, epoch)
			} else {
				entrypointCreateWithoutBirth(t, f, p, tc.operation)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			entrypointStop(t, f, p, stats, secrets)
		})
	}
}
