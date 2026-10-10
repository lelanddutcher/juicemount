package respproxydeploy

// The real Manager store and production Go reloader share a private directory.
// These component tests cover the file protocol. Separate CLI acceptance covers
// actual process startup, signal handling, session closure and original Wait.

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

// startEntrypointHandshake uses the same serialized 2s reloader as the CLI.
func startEntrypointHandshake(t *testing.T, dir string) (stop func()) {
	t.Helper()
	entry := readFile(t, "entrypoint.sh")
	if !strings.Contains(entry, "reload_handshake: true") || strings.Contains(entry, "kill -HUP") || strings.Contains(entry, "> \"$DIR/.reload.ack") {
		t.Fatal("entrypoint must delegate applied reload authority to the proxy")
	}
	ps, err := respproxy.LoadPrincipals(filepath.Join(dir, "principals.json"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := respproxy.NewPrincipalReloader(filepath.Join(dir, "principals.json"), respproxy.NewAuthenticator(ps))
	if err != nil {
		t.Fatal(err)
	}
	done, quit := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(2 * time.Second)
		defer tick.Stop()
		for {
			_ = r.Tick() // errors deliberately leave the Manager pending
			select {
			case <-quit:
				return
			case <-tick.C:
			}
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { close(quit); <-done }) }
	t.Cleanup(stop)
	return stop
}

// startHandshakeBlock runs a handshake block (the current one, or an older
// image's) the same way.
func startHandshakeBlock(t *testing.T, dir, block string) (stop func()) {
	t.Helper()
	if _, err := exec.LookPath("sha256sum"); err != nil {
		t.Skip("sha256sum not on PATH (the container image has it)")
	}
	script := "set -eu\n" +
		"DIR=\"$1\"\nPRINCIPALS=\"$DIR/principals.json\"\n" +
		"log() { :; }\n" +
		"sh -c 'trap \"\" HUP; while :; do sleep 1; done' &\nchild=$!\n" +
		"trap 'kill -TERM \"$child\" 2>/dev/null || true; exit 0' TERM INT\n" +
		block
	cmd := exec.Command("/bin/sh", "-c", script, "entrypoint-handshake", dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	}
	t.Cleanup(stop)
	return stop
}

const (
	hsInstanceA = "4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F"
	hsInstanceB = "BBBB1111-2222-4C1A-9E2B-0A1B2C3D4E5F"
)

// Synthetic enrollment secrets (low entropy on purpose).
var hsSecretA, hsSecretB = strings.Repeat("a", 40), strings.Repeat("b", 40)

// TestH8BHandshakeStaleAckAfterProxyStops reproduces the reviewer's
// e2e-stale-ack run: approve with the proxy live, stop the proxy and its
// handshake loop, restart the Manager (the --proxy-rollback compose keeps
// JM_RESPPROXY_DIR), then Approve must be refused (HTTP 409 in the API).
func TestH8BHandshakeStaleAckAfterProxyStops(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "manager", "principals-store.json")
	proxyDir := filepath.Join(dir, "proxy")
	if err := os.MkdirAll(proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	s, err := manager.NewPrincipalStore(manager.PrincipalStoreOptions{StatePath: state, ProxyDir: proxyDir})
	if err != nil {
		t.Fatal(err)
	}
	stopProxy := startEntrypointHandshake(t, proxyDir)
	a, err := s.Enroll(manager.EnrollRequest{InstanceID: hsInstanceA, Host: "Edit-Bay-1", Secret: hsSecretA}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	// The new proxy applies the startup request itself; Approve may also
	// re-request while that first tick is pending.
	if err := s.Approve(a.Principal); err != nil {
		t.Fatalf("Approve with the proxy live: %v", err)
	}
	waitFor(t, 8*time.Second, "the proxy to acknowledge the approval's reload", func() bool {
		return s.Snapshot().ProxyLive
	})

	stopProxy()
	ack, _ := os.ReadFile(filepath.Join(proxyDir, "reload.ack"))
	if len(strings.Fields(string(ack))) < 3 {
		t.Fatalf("reload.ack %q does not repeat the whole request line", ack)
	}

	// Manager restart: same state, same proxy directory, no proxy process.
	s2, err := manager.NewPrincipalStore(manager.PrincipalStoreOptions{StatePath: state, ProxyDir: proxyDir})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Snapshot().ProxyLive {
		t.Fatal("a restarted Manager reports the stopped proxy as live")
	}
	b, err := s2.Enroll(manager.EnrollRequest{InstanceID: hsInstanceB, Host: "Edit-Bay-2", Secret: hsSecretB}, "192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := s2.Approve(b.Principal); !errors.Is(err, manager.ErrProxyNotLive) {
		t.Fatalf("Approve with no proxy process after a Manager restart = %v (after %v), want ErrProxyNotLive", err, time.Since(start).Round(time.Millisecond))
	}
	if _, err := s2.SetAutoApproveWindow(time.Hour); !errors.Is(err, manager.ErrProxyNotLive) {
		t.Fatalf("opening the auto-approve window with no proxy = %v, want ErrProxyNotLive", err)
	}

	// The proxy comes back: the same Approve now succeeds.
	startEntrypointHandshake(t, proxyDir)
	if err := s2.Approve(b.Principal); err != nil {
		t.Fatalf("Approve after the proxy came back: %v", err)
	}
}

func waitFor(t *testing.T, d time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func newHandshakeStore(t *testing.T) (*manager.PrincipalStore, string) {
	t.Helper()
	dir := t.TempDir()
	proxyDir := filepath.Join(dir, "proxy")
	if err := os.MkdirAll(proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	s, err := manager.NewPrincipalStore(manager.PrincipalStoreOptions{StatePath: filepath.Join(dir, "m", "s.json"), ProxyDir: proxyDir})
	if err != nil {
		t.Fatal(err)
	}
	return s, proxyDir
}

func hsInstance(i int) string { return fmt.Sprintf("%08X-7B3D-4C1A-9E2B-0A1B2C3D4E5F", 0x10000000+i) }

func hsEnroll(t *testing.T, s *manager.PrincipalStore, i int) string {
	t.Helper()
	r, err := s.Enroll(manager.EnrollRequest{InstanceID: hsInstance(i), Host: fmt.Sprintf("Bay-%d", i),
		Secret: strings.Repeat(string(rune('a'+i)), 40)}, fmt.Sprintf("192.0.2.%d", i+1))
	if err != nil {
		t.Fatal(err)
	}
	return r.Principal
}

// heartbeatLine is the current block's heartbeat; removing it gives the
// a861a46 image's block (whole-line ack, no heartbeat).
const heartbeatLine = `  date +%s > "$DIR/reload.heartbeat" 2>/dev/null || true` + "\n"

// TestH8BHandshakeOutdatedProxyImage: review 3 finding 2. A new Manager with
// a proxy image older than the heartbeat (the 05b421f entrypoint's two-field
// "sha256 seconds" ack, or a861a46's whole-line ack without a heartbeat):
// Approve and the auto-approve window are refused with
// ErrProxyImageOutdated, at once, never as live.
func TestH8BHandshakeOutdatedProxyImage(t *testing.T) {
	current := readFile(t, "testdata/handshake-before-applied.sh.txt")
	if !strings.Contains(current, heartbeatLine) {
		t.Fatal("entrypoint.sh handshake has no heartbeat line")
	}
	for name, block := range map[string]string{
		"before-a861a46":             readFile(t, "testdata/handshake-before-a861a46.sh.txt"),
		"a861a46":                    strings.Replace(current, heartbeatLine, "", 1),
		"signal-only-with-heartbeat": current,
	} {
		t.Run(name, func(t *testing.T) {
			s, proxyDir := newHandshakeStore(t)
			startHandshakeBlock(t, proxyDir, block)
			a := hsEnroll(t, s, 1)
			for i := 0; i < 2; i++ {
				start := time.Now()
				err := s.Approve(a)
				if !errors.Is(err, manager.ErrProxyImageOutdated) {
					t.Fatalf("attempt %d: Approve with the %s proxy image = %v, want ErrProxyImageOutdated", i, name, err)
				}
				if d := time.Since(start); d > 4500*time.Millisecond {
					t.Fatalf("attempt %d: refused after %v; an answering old image is reported at its first ack", i, d)
				}
			}
			if _, err := s.SetAutoApproveWindow(time.Hour); !errors.Is(err, manager.ErrProxyImageOutdated) {
				t.Fatalf("auto-approve window: %v", err)
			}
			if snap := s.Snapshot(); snap.ProxyLive || !strings.Contains(snap.ProxyProblem, "update the jm-respproxy image") {
				t.Fatalf("snapshot: live=%v problem=%q", snap.ProxyLive, snap.ProxyProblem)
			}
		})
	}
}

// TestH8BHandshakeOldManagerNewProxyImage: an old Manager (05b421f writes
// "sha256 ms" and compares the ack's first field) with the new entrypoint:
// the ack's first field is still the sha256.
func TestH8BHandshakeOldManagerNewProxyImage(t *testing.T) {
	dir := t.TempDir()
	body := []byte(`{"principals":[]}` + "\n")
	if err := os.WriteFile(filepath.Join(dir, "principals.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	sha := hex.EncodeToString(sum[:])
	startEntrypointHandshake(t, dir)
	time.Sleep(300 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(dir, "reload.request"), []byte(sha+" 1700000000000\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 6*time.Second, "ack", func() bool {
		b, _ := os.ReadFile(filepath.Join(dir, "reload.ack"))
		f := strings.Fields(string(b))
		return len(f) > 0 && f[0] == sha
	})
}

// TestH8BHandshakeApprovalBurst: review 3 finding 7. Eight approvals 400 ms
// apart against the real block: none may be refused while the proxy runs.
// (Before the fix 4 of 8 were refused: each approval's new request
// invalidated the proof for the next.) Run with -count=6 for the rate.
func TestH8BHandshakeApprovalBurst(t *testing.T) {
	s, proxyDir := newHandshakeStore(t)
	startEntrypointHandshake(t, proxyDir)
	const n = 8
	names := make([]string, n)
	for i := range names {
		names[i] = hsEnroll(t, s, i)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var refused []string
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			time.Sleep(time.Duration(i) * 400 * time.Millisecond)
			if err := s.Approve(names[i]); err != nil {
				mu.Lock()
				refused = append(refused, fmt.Sprintf("approve %d: %v", i, err))
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	t.Logf("burst: %d/%d refused in %v", len(refused), n, time.Since(start).Round(time.Millisecond))
	if len(refused) > 0 {
		t.Fatalf("approvals refused with a live proxy: %v", refused)
	}
}

// TestH8BHandshakeProxyStopsManagerKeepsRunning: review 3 finding 3. The
// proxy and its loop stop while the Manager keeps running: once the
// heartbeat is older than three ticks, Approve is refused (ErrProxyStopped).
func TestH8BHandshakeProxyStopsManagerKeepsRunning(t *testing.T) {
	s, proxyDir := newHandshakeStore(t)
	stop := startEntrypointHandshake(t, proxyDir)
	a, b := hsEnroll(t, s, 1), hsEnroll(t, s, 2)
	if err := s.Approve(a); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 8*time.Second, "ack of A's reload", func() bool { return s.Snapshot().ProxyLive })
	stop()
	time.Sleep(6500 * time.Millisecond) // past the 6 s heartbeat age
	start := time.Now()
	err := s.Approve(b)
	if !errors.Is(err, manager.ErrProxyStopped) {
		t.Fatalf("Approve B with no proxy process (Manager not restarted) = %v after %v, want ErrProxyStopped", err, time.Since(start).Round(time.Millisecond))
	}
}

// TestH8BGateCoversEntrypointHandshake enforces min-gate-commit's bump rule
// for entrypoint.sh: the last commit that changed it must be the gate commit
// or one of its ancestors. Otherwise rollout-compose.sh would accept a proxy
// image whose handshake the current Manager refuses (review 3 finding 2).
// Skipped outside a full git checkout, unless JM_REQUIRE_GATE_HISTORY=1
// (set by the CI workflow, which fetches full history): there a shallow or
// missing history fails the test, so CI cannot pass it by skipping.
func TestH8BGateCoversEntrypointHandshake(t *testing.T) {
	skip := t.Skip
	if os.Getenv("JM_REQUIRE_GATE_HISTORY") == "1" {
		skip = func(args ...any) {
			t.Helper()
			t.Fatal(append([]any{"JM_REQUIRE_GATE_HISTORY=1 but"}, args...)...)
		}
	}
	git := func(args ...string) (string, error) {
		out, err := exec.Command("git", args...).Output()
		return strings.TrimSpace(string(out)), err
	}
	if out, err := git("rev-parse", "--is-shallow-repository"); err != nil || out != "false" {
		skip("not a full git checkout (fetch full history: actions/checkout fetch-depth: 0)")
	}
	last, err := git("log", "-1", "--format=%H", "--", "entrypoint.sh")
	if err != nil || last == "" {
		skip("entrypoint.sh has no history here")
	}
	var gate string
	for _, line := range strings.Split(readFile(t, "min-gate-commit"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			gate = line
			break
		}
	}
	if _, err := git("merge-base", "--is-ancestor", last, gate); err != nil {
		t.Fatalf("entrypoint.sh last changed in %s, which min-gate-commit (%s) does not contain: bump min-gate-commit", last[:12], gate)
	}
}

// N12: JM_REQUIRE_GATE_HISTORY=1 turns the gate test's "no history" SKIP into
// a FAIL, so CI cannot pass it by skipping. The gate test is re-run in a
// child process with git pointed at a directory that is no repository.
func TestH8BRequireGateHistoryTurnsSkipIntoFail(t *testing.T) {
	if os.Getenv("JM_GATE_HISTORY_CHILD") == "1" {
		t.Skip("child process")
	}
	run := func(require string) string {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestH8BGateCoversEntrypointHandshake$", "-test.v", "-test.count=1")
		cmd.Env = append(os.Environ(), "JM_GATE_HISTORY_CHILD=1", "GIT_DIR="+filepath.Join(t.TempDir(), "no-repo"), "JM_REQUIRE_GATE_HISTORY="+require)
		out, _ := cmd.CombinedOutput()
		return string(out)
	}
	if out := run("0"); !strings.Contains(out, "--- SKIP: TestH8BGateCoversEntrypointHandshake") {
		t.Fatalf("without JM_REQUIRE_GATE_HISTORY the gate test did not skip on a missing history:\n%s", out)
	}
	if out := run("1"); !strings.Contains(out, "--- FAIL: TestH8BGateCoversEntrypointHandshake") || !strings.Contains(out, "JM_REQUIRE_GATE_HISTORY=1 but") {
		t.Fatalf("JM_REQUIRE_GATE_HISTORY=1 did not turn the missing-history skip into a failure:\n%s", out)
	}
}
