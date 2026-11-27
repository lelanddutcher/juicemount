package enginehost

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/jmengine"
	"github.com/juicedata/juicefs/pkg/jmengine/wire"
)

// The test binary doubles as a fake helper (JM_ENGINEHOST_FAKE=<mode>), as a
// second process of the same user dialing the socket
// (JM_ENGINEHOST_DIAL=<path>), and as an app that is killed
// (JM_ENGINEHOST_APP=1).
func TestMain(m *testing.M) {
	if mode := os.Getenv("JM_ENGINEHOST_FAKE"); mode != "" {
		os.Exit(fakeHelper(mode))
	}
	if path := os.Getenv("JM_ENGINEHOST_DIAL"); path != "" {
		os.Exit(dialAsOther(path, os.Getenv("JM_ENGINEHOST_TOKEN")))
	}
	if os.Getenv("JM_ENGINEHOST_APP") == "1" {
		os.Exit(fakeApp())
	}
	os.Exit(m.Run())
}

type fakeEngine struct{}

func (fakeEngine) VolumeID() string   { return "vol-uuid" }
func (fakeEngine) VolumeName() string { return "vol" }
func (fakeEngine) SessionID() uint64  { return 0 }
func (fakeEngine) ReadOnly() bool     { return true }
func (fakeEngine) Metrics() string    { return "" }
func (fakeEngine) GetAttr(_ context.Context, ino uint64) (wire.Attr, syscall.Errno) {
	return wire.Attr{Inode: ino, Size: 11}, 0
}

func fakeHelper(mode string) int {
	start := os.NewFile(3, "start")
	msg, err := wire.ReadStartMessage(start)
	if err != nil {
		return 2
	}
	switch mode {
	case "crash-handshake":
		return 3
	case "check-secrets":
		// The secrets arrive by the pipe and are not in the environment.
		if msg.MetaPassword != "pw-secret" || msg.AccessKey != "ak" || msg.SecretKey != "sk" {
			return 4
		}
		for _, kv := range os.Environ() {
			if strings.Contains(kv, "pw-secret") || strings.Contains(kv, "=sk") {
				return 5
			}
		}
	}
	token, _ := hex.DecodeString(msg.Token)
	ln, err := net.FileListener(os.NewFile(4, "listen"))
	if err != nil {
		return 6
	}
	cfg := jmengine.Config{Listener: ln.(*net.UnixListener), Token: token, Engine: fakeEngine{}, Version: "test-ok"}
	var eb [8]byte
	_, _ = rand.Read(eb[:])
	cfg.Epoch = binary.BigEndian.Uint64(eb[:]) | 1
	switch mode {
	case "wrong-version":
		cfg.Version = "test-bad"
	case "proto":
		cfg.ProtoMajor = wire.ProtoMajor + 1
	}
	srv := jmengine.New(cfg)
	go srv.Serve()
	appGone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, start); close(appGone) }()
	if mode == "ignore-shutdown" {
		signal.Ignore(syscall.SIGHUP)
		select {} // only a signal ends it
	}
	select {
	case <-srv.ShutdownRequested():
	case <-appGone:
	}
	srv.Close(time.Second)
	return 0
}

func dialAsOther(path, token string) int {
	t, _ := hex.DecodeString(token)
	_, _, err := dialHello(path, t, wire.ProtoMajor, time.Now().Add(5*time.Second))
	var se *StatusError
	if errors.As(err, &se) && se.Status == syscall.EACCES {
		fmt.Println("refused")
		return 0
	}
	fmt.Println("accepted:", err)
	return 1
}

func fakeApp() int {
	s := New(fakeConfig("ok", nil))
	s.Start()
	st, err := s.WaitReady(context.Background())
	if err != nil {
		return 2
	}
	fmt.Println(st.PID)
	select {}
}

func fakeConfig(mode string, events func(Event)) Config {
	return Config{
		Bin:  os.Args[0],
		Args: []string{"jm-engine", "redis://fake/1"},
		Env:  []string{"JM_ENGINEHOST_FAKE=" + mode, "PATH=" + os.Getenv("PATH")},
		// Small values keep the tests short; the design's values are the defaults.
		StartTimeout: 10 * time.Second,
		PingInterval: 100 * time.Millisecond,
		PingTimeout:  200 * time.Millisecond,
		MissLimit:    3,
		Backoff:      []time.Duration{50 * time.Millisecond, 300 * time.Millisecond, time.Second},
		ResetAfter:   time.Minute,
		StopBound:    2 * time.Second,
		OnEvent:      events,
	}
}

type recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *recorder) add(e Event) {
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *recorder) count(kind string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, e := range r.events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func (r *recorder) waitFor(t *testing.T, kind string, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for r.count(kind) < n {
		if time.Now().After(deadline) {
			t.Fatalf("no %d %q events within %s; have %d", n, kind, within, r.count(kind))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startSupervisor(t *testing.T, cfg Config) *Supervisor {
	t.Helper()
	s := New(cfg)
	s.Start()
	t.Cleanup(func() { s.Stop() })
	return s
}

func waitReady(t *testing.T, s *Supervisor, within time.Duration) Status {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	st, err := s.WaitReady(ctx)
	if err != nil {
		t.Fatalf("not ready: %v (%+v)", err, s.Status())
	}
	return st
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func waitGone(t *testing.T, pid int, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for alive(pid) {
		if time.Since(start) > within {
			t.Fatalf("process %d still alive after %s", pid, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

func TestStartReadyAndStop(t *testing.T) {
	rec := &recorder{}
	s := startSupervisor(t, fakeConfig("ok", rec.add))
	st := waitReady(t, s, 10*time.Second)
	if st.PID == 0 || st.Epoch == 0 || st.Version != "test-ok" || st.VolumeID != "vol-uuid" {
		t.Fatalf("status %+v", st)
	}
	cl := s.Client()
	if cl == nil {
		t.Fatal("no client while ready")
	}
	if p, err := cl.Ping(time.Second); err != nil || p.Epoch != st.Epoch {
		t.Fatalf("ping %+v %v", p, err)
	}
	if a, err := cl.GetAttr(1, time.Second); err != nil || a.Size != 11 {
		t.Fatalf("getattr %+v %v", a, err)
	}
	if _, err := cl.Stats(time.Second); err != nil {
		t.Fatalf("stats: %v", err)
	}
	s.mu.Lock()
	dir := s.cur.dir
	s.mu.Unlock()
	how, took := s.Stop()
	if how != "shutdown" {
		t.Fatalf("stopped by %q, want shutdown", how)
	}
	waitGone(t, st.PID, 2*time.Second)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("socket folder left behind: %v", err)
	}
	if s.Status().State != StateStopped || rec.count(EventStopped) != 1 {
		t.Fatalf("status %+v after stop", s.Status())
	}
	t.Logf("stop took %s", took)
}

func TestRestartWithBackoff(t *testing.T) {
	rec := &recorder{}
	s := startSupervisor(t, fakeConfig("ok", rec.add))
	first := waitReady(t, s, 10*time.Second)
	_ = syscall.Kill(first.PID, syscall.SIGKILL)
	rec.waitFor(t, EventReady, 2, 10*time.Second)
	second := s.Status()
	if second.PID == first.PID || second.Epoch == first.Epoch || second.Restarts != 1 {
		t.Fatalf("after a kill: %+v (first %+v)", second, first)
	}
	_ = syscall.Kill(second.PID, syscall.SIGKILL)
	rec.waitFor(t, EventReady, 3, 10*time.Second)
	// Ready for less than ResetAfter: the back-off grew (50 ms, then 300 ms).
	var delays []time.Duration
	rec.mu.Lock()
	for _, e := range rec.events {
		if e.Kind == EventBackoff {
			delays = append(delays, e.Elapsed)
		}
	}
	rec.mu.Unlock()
	if len(delays) != 2 || delays[0] != 50*time.Millisecond || delays[1] != 300*time.Millisecond {
		t.Fatalf("back-off %v, want [50ms 300ms]", delays)
	}
	if s.Status().Restarts != 2 {
		t.Fatalf("restarts %d", s.Status().Restarts)
	}
}

func TestBackoffResetsAfterReadyPeriod(t *testing.T) {
	rec := &recorder{}
	cfg := fakeConfig("ok", rec.add)
	cfg.ResetAfter = 200 * time.Millisecond
	s := startSupervisor(t, cfg)
	for i := 1; i <= 3; i++ {
		st := waitReady(t, s, 10*time.Second)
		time.Sleep(300 * time.Millisecond) // past ResetAfter
		_ = syscall.Kill(st.PID, syscall.SIGKILL)
		rec.waitFor(t, EventReady, i+1, 10*time.Second)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, e := range rec.events {
		if e.Kind == EventBackoff && e.Elapsed != 50*time.Millisecond {
			t.Fatalf("back-off %s after a long ready period, want the first step", e.Elapsed)
		}
	}
}

func TestVersionMismatchRefused(t *testing.T) {
	rec := &recorder{}
	cfg := fakeConfig("wrong-version", rec.add)
	cfg.ExpectVersion = "test-ok"
	s := startSupervisor(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.WaitReady(ctx)
	if !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "test-bad") {
		t.Fatalf("WaitReady: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if n := rec.count(EventLaunch); n != 1 {
		t.Fatalf("%d launches after a refusal, want 1 (no restart loop)", n)
	}
	rec.mu.Lock()
	pid := rec.events[0].PID
	rec.mu.Unlock()
	waitGone(t, pid, 2*time.Second)
	if st := s.Status(); st.State != StateRefused || st.PID != 0 {
		t.Fatalf("status %+v", st)
	}
}

func TestProtocolMajorRefused(t *testing.T) {
	s := startSupervisor(t, fakeConfig("proto", nil))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := s.WaitReady(ctx); !errors.Is(err, ErrRefused) {
		t.Fatalf("WaitReady: %v", err)
	}
}

func TestCrashDuringHandshake(t *testing.T) {
	rec := &recorder{}
	cfg := fakeConfig("crash-handshake", rec.add)
	cfg.FailureReport = 3
	s := startSupervisor(t, cfg)
	rec.waitFor(t, EventPersistent, 1, 15*time.Second)
	if rec.count(EventReady) != 0 || rec.count(EventStartFailed) < 3 {
		t.Fatalf("events: ready %d, start-failed %d", rec.count(EventReady), rec.count(EventStartFailed))
	}
	st := s.Status()
	if st.State == StateReady || st.ConsecutiveFailures < 3 || !strings.Contains(st.LastError, "exited during start") {
		t.Fatalf("status %+v", st)
	}
	if how, _ := s.Stop(); how != "none" {
		t.Fatalf("stopped by %q with no ready helper", how)
	}
}

func TestUnresponsiveHelperReplaced(t *testing.T) {
	rec := &recorder{}
	s := startSupervisor(t, fakeConfig("ok", rec.add))
	first := waitReady(t, s, 10*time.Second)
	stopped := time.Now()
	_ = syscall.Kill(first.PID, syscall.SIGSTOP)
	rec.waitFor(t, EventReady, 2, 10*time.Second)
	took := time.Since(stopped)
	if rec.count(EventUnresponsive) != 1 || rec.count(EventPingMiss) < 3 {
		t.Fatalf("events: unresponsive %d, ping-miss %d", rec.count(EventUnresponsive), rec.count(EventPingMiss))
	}
	waitGone(t, first.PID, 2*time.Second)
	// interval + 3 × timeout + kill + back-off + start, with slack.
	if took > 3*time.Second {
		t.Fatalf("replacement took %s", took)
	}
}

func TestStopEscalatesToTerm(t *testing.T) {
	cfg := fakeConfig("ignore-shutdown", nil)
	cfg.StopBound = 300 * time.Millisecond
	s := startSupervisor(t, cfg)
	st := waitReady(t, s, 10*time.Second)
	how, took := s.Stop()
	if how != "term" {
		t.Fatalf("stopped by %q, want term", how)
	}
	waitGone(t, st.PID, 2*time.Second)
	if took > 3*time.Second {
		t.Fatalf("stop took %s", took)
	}
}

func TestStopBeforeStart(t *testing.T) {
	s := New(fakeConfig("ok", nil))
	if how, _ := s.Stop(); how != "none" {
		t.Fatalf("how %q", how)
	}
	s.Start() // no-op after Stop
	if _, err := s.WaitReady(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatalf("WaitReady after Stop: %v", err)
	}
}

func TestSecretsOnlyByPipe(t *testing.T) {
	cfg := fakeConfig("check-secrets", nil)
	cfg.MetaPassword = "pw-secret"
	cfg.ObjectKey = ObjectKey{AccessKey: "ak", SecretKey: "sk"}
	s := startSupervisor(t, cfg)
	waitReady(t, s, 10*time.Second)
	for _, a := range cfg.Args {
		if strings.Contains(a, "pw-secret") {
			t.Fatal("password in argv")
		}
	}
}

func TestSocketPermissionsAndPeers(t *testing.T) {
	s := startSupervisor(t, fakeConfig("ok", nil))
	waitReady(t, s, 10*time.Second)
	s.mu.Lock()
	dir, sock, token := s.cur.dir, s.cur.sock, s.cur.client.token
	s.mu.Unlock()
	for path, want := range map[string]os.FileMode{dir: 0o700, sock: 0o600} {
		fi, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != want {
			t.Fatalf("%s mode %v, want %v", path, fi.Mode().Perm(), want)
		}
		if st := fi.Sys().(*syscall.Stat_t); int(st.Uid) != os.Getuid() {
			t.Fatalf("%s owned by %d", path, st.Uid)
		}
	}
	// The app with a wrong token: refused.
	if _, _, err := dialHello(sock, make([]byte, 32), wire.ProtoMajor, time.Now().Add(2*time.Second)); err == nil {
		t.Fatal("wrong token accepted")
	}
	// Another process of the same user with the right token: refused by the
	// peer check (it is not the helper's parent).
	out, err := runSelf(t, nil, "JM_ENGINEHOST_DIAL="+sock, "JM_ENGINEHOST_TOKEN="+hex.EncodeToString(token))
	if err != nil || strings.TrimSpace(out) != "refused" {
		t.Fatalf("same user, other process: %q %v", out, err)
	}
	// Another user: the folder (0700) stops it before the helper sees it.
	if os.Getuid() != 0 {
		t.Log("not root: the other-user case runs in the lab (engine-life-lab.sh E5)")
		return
	}
	out, err = runSelf(t, &syscall.Credential{Uid: 65534, Gid: 65534},
		"JM_ENGINEHOST_DIAL="+sock, "JM_ENGINEHOST_TOKEN="+hex.EncodeToString(token))
	if err == nil || !strings.Contains(out, "permission denied") {
		t.Fatalf("other user: %q %v", out, err)
	}
}

func runSelf(t *testing.T, cred *syscall.Credential, env ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH")}, env...)
	if cred != nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// The app is killed: the pipe closes and the helper exits by itself.
func TestHelperExitsWhenAppKilled(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = []string{"JM_ENGINEHOST_APP=1", "PATH=" + os.Getenv("PATH")}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("app: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || !alive(pid) {
		_ = cmd.Process.Kill()
		t.Fatalf("helper pid %q", line)
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	took := waitGone(t, pid, 5*time.Second)
	t.Logf("helper gone %s after the app was killed", took)
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]Level{"": LevelFUSE, "fuse": LevelFUSE, " Shadow ": LevelShadow} {
		if got, err := ParseLevel(in); err != nil || got != want {
			t.Fatalf("ParseLevel(%q) = %v, %v", in, got, err)
		}
	}
	for _, in := range []string{"reads", "direct", "bogus"} {
		if got, err := ParseLevel(in); err == nil || got != LevelFUSE {
			t.Fatalf("ParseLevel(%q) = %v, %v; want fuse and an error", in, got, err)
		}
	}
}

func TestSocketPathBound(t *testing.T) {
	base := filepath.Join(t.TempDir(), strings.Repeat("d", 100))
	if err := os.MkdirAll(base, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := fakeConfig("ok", nil)
	cfg.SocketBase = base
	s := New(cfg)
	if _, err := s.launch(); err == nil || !strings.Contains(err.Error(), "socket path") {
		t.Fatalf("long socket path: %v", err)
	}
}
