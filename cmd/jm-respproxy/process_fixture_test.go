//go:build redisoptionsacceptance

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// proxyCLIProcess has one owner for the original Wait. The child log remains
// open until that Wait completes, including on a failed acceptance assertion.
type proxyCLIProcess struct {
	addr    string
	cmd     *exec.Cmd
	done    chan struct{}
	err     error // Written only by the Wait goroutine, read after done closes.
	logPath string

	logFile     *os.File
	password    string
	banner      string
	accounted   bool
	logReported bool
}

func proxyCLIBinary(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("JM_RESPPROXY_CLI_BINARY")
	want := os.Getenv("JM_RESPPROXY_CLI_BINARY_SHA256")
	if !filepath.IsAbs(binary) || len(want) != 64 {
		t.Fatal("pinned absolute JM_RESPPROXY_CLI_BINARY and SHA256 required")
	}
	if _, err := hex.DecodeString(want); err != nil {
		t.Fatalf("invalid pinned binary SHA256: %v", err)
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("pinned binary is not an executable regular file: %v", err)
	}
	f, err := os.Open(binary)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, hashErr := io.Copy(h, f)
	closeErr := f.Close()
	if hashErr != nil || closeErr != nil {
		t.Fatalf("hash pinned binary: copy=%v close=%v", hashErr, closeErr)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != strings.ToLower(want) {
		t.Fatalf("pinned binary SHA256 mismatch: got %s", got)
	}
	return binary
}

func proxyCLIWriteConfig(t *testing.T, dir string, config fileConfig) string {
	t.Helper()
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, "config.json")
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if n, err := f.Write(append(data, '\n')); err != nil || n != len(data)+1 {
		_ = f.Close()
		t.Fatalf("write proxy config bytes=%d err=%v", n, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return name
}

func startProxyCLI(t *testing.T, config fileConfig, upstreamPassword string) *proxyCLIProcess {
	t.Helper()
	binary := proxyCLIBinary(t)
	host, port, err := net.SplitHostPort(config.Listen)
	if err != nil {
		t.Fatalf("proxy listener address: %v", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		t.Fatal("proxy fixture requires a reserved loopback TCP address")
	}
	if !filepath.IsAbs(config.Principals) {
		t.Fatal("proxy fixture requires an absolute private principals file")
	}
	config.UpstreamPasswordEnv = "JM_RESPPROXY_CLI_UPSTREAM_SECRET"
	dir := t.TempDir()
	configPath := proxyCLIWriteConfig(t, dir, config)
	logPath := filepath.Join(dir, "child.log")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := logFile.Chmod(0600); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-config", configPath)
	cmd.Env = []string{"JM_RESPPROXY_CLI_UPSTREAM_SECRET=" + upstreamPassword, "GOMAXPROCS=2"}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start pinned proxy executable: %v", err)
	}
	p := &proxyCLIProcess{
		addr: config.Listen, cmd: cmd, done: make(chan struct{}), logPath: logPath,
		logFile: logFile, password: upstreamPassword,
		banner: "jm-respproxy: " + config.Listen + " -> " + config.Upstream + " mode=" + config.Mode + " principals=",
	}
	go func() {
		p.err = cmd.Wait()
		close(p.done)
	}()
	t.Logf("proxy CLI launched owned PID=%d executable SHA256=%s log=%s", cmd.Process.Pid,
		strings.ToLower(os.Getenv("JM_RESPPROXY_CLI_BINARY_SHA256")), logPath)
	t.Cleanup(func() { p.cleanup(t) })
	return p
}

func (p *proxyCLIProcess) logBytes(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(p.logPath)
	if err != nil {
		t.Fatalf("read full proxy child log: %v", err)
	}
	return data
}

func (p *proxyCLIProcess) report(t *testing.T, expectedRejection string) {
	t.Helper()
	if p.logReported {
		return
	}
	p.logReported = true
	if err := p.logFile.Sync(); err != nil {
		t.Errorf("sync proxy child log: %v", err)
	}
	if err := p.logFile.Close(); err != nil {
		t.Errorf("close proxy child log: %v", err)
	}
	data := p.logBytes(t)
	if p.password != "" && strings.Contains(string(data), p.password) {
		t.Error("proxy child log exposed supplied password")
		return
	}
	sum := sha256.Sum256(data)
	t.Logf("proxy CLI original Wait PID=%d result=%v log bytes=%d SHA256=%x\n%s",
		p.cmd.Process.Pid, p.err, len(data), sum, data)
	if strings.Contains(strings.ToUpper(string(data)), "WARNING: DATA RACE") ||
		strings.Contains(strings.ToLower(string(data)), "test timed out") {
		t.Error("proxy child emitted a race or timeout marker")
	}
	// An expected startup rejection is log.Fatal's diagnostic. Still scan
	// every other line for a panic, fatal, or test failure.
	bad := regexp.MustCompile(`(?im)(^|[^a-z])(panic|fatal|fail)([^a-z]|$)`)
	for _, line := range strings.Split(string(data), "\n") {
		if expectedRejection != "" && strings.Contains(line, expectedRejection) {
			line = strings.Replace(line, expectedRejection, "", 1)
		}
		if bad.MatchString(line) {
			t.Error("proxy child emitted unexpected panic/fatal/FAIL")
		}
	}
}

func (p *proxyCLIProcess) joined(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatalf("original proxy PID=%d did not join within 10s", p.cmd.Process.Pid)
	}
}

func (p *proxyCLIProcess) exclusiveBind(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		t.Fatalf("owned proxy address %s not exclusively bindable after original Wait: %v", p.addr, err)
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
}

func (p *proxyCLIProcess) awaitReady(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		data := p.logBytes(t)
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, p.banner) && strings.Contains(line, " incarnation=") {
				conn, err := net.DialTimeout("tcp", p.addr, time.Second)
				if err == nil {
					_ = conn.Close()
					select {
					case <-p.done:
						t.Fatalf("proxy exited after startup banner: %v", p.err)
					default:
					}
					return
				}
			}
		}
		select {
		case <-p.done:
			p.report(t, "")
			t.Fatalf("proxy exited before startup banner: %v", p.err)
		case <-deadline.C:
			t.Fatalf("proxy PID=%d did not become ready within 10s", p.cmd.Process.Pid)
		case <-tick.C:
		}
	}
}

func (p *proxyCLIProcess) reject(t *testing.T, wantSubstring string) {
	t.Helper()
	if wantSubstring == "" {
		t.Fatal("expected rejection diagnostic must be specified")
	}
	p.joined(t)
	p.accounted = true
	var exit *exec.ExitError
	if !errors.As(p.err, &exit) || exit.ExitCode() != 1 {
		p.report(t, wantSubstring)
		t.Fatalf("proxy rejection exit=%v, want exit status 1", p.err)
	}
	data := p.logBytes(t)
	if !strings.Contains(string(data), wantSubstring) || strings.Contains(string(data), "jm-respproxy: ") ||
		strings.Contains(string(data), "serve:") {
		p.report(t, wantSubstring)
		t.Fatalf("proxy rejection transcript lacks diagnostic or reached Serve: want %q", wantSubstring)
	}
	p.report(t, wantSubstring)
	p.exclusiveBind(t)
}

func (p *proxyCLIProcess) stop(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		p.report(t, "")
		t.Fatalf("proxy exited before owned SIGTERM: %v", p.err)
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM owned proxy PID=%d: %v", p.cmd.Process.Pid, err)
	}
	p.joined(t)
	p.accounted = true
	p.report(t, "")
	if p.err != nil {
		t.Fatalf("owned proxy SIGTERM exit=%v, want zero", p.err)
	}
	p.exclusiveBind(t)
}

func (p *proxyCLIProcess) cleanup(t *testing.T) {
	t.Helper()
	if !p.accounted {
		t.Errorf("owned proxy PID=%d was not explicitly rejected or stopped", p.cmd.Process.Pid)
		select {
		case <-p.done:
		default:
			if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Errorf("kill only owned proxy PID=%d on failed test: %v", p.cmd.Process.Pid, err)
			}
			select {
			case <-p.done:
			case <-time.After(10 * time.Second):
				fmt.Fprintf(os.Stderr, "owned proxy PID=%d still unjoined; preserving dependencies\n", p.cmd.Process.Pid)
				os.Exit(2)
			}
		}
		p.report(t, "")
	}
}
