package manager

// Review 4: the Manager ID survives a restore of a pre-ID principal store
// (E2), and the proxy-liveness edges A1 (heartbeat in the future), A2 (proxy
// stuck behind a running heartbeat), A3 (proxy image rolled back to a861a46)
// and the first-tick window.

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// E2: restoring the principal store from a backup taken before IDs existed
// keeps the Manager ID (it is kept in its own file too). Losing the ID file
// alone keeps it as well (the store has it). Only losing both mints a new
// one.
func TestH8BManagerIDSurvivesPreIDStoreRestore(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "principals-store.json")
	idFile := filepath.Join(dir, "principals-store.manager-id")
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	id := s.ManagerID()
	stripID := func() {
		t.Helper()
		var f map[string]any
		if err := json.Unmarshal([]byte(readString(t, state)), &f); err != nil {
			t.Fatal(err)
		}
		delete(f, "manager_id")
		b, _ := json.Marshal(f)
		if err := os.WriteFile(state, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	reopen := func() string {
		t.Helper()
		s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state})
		if err != nil {
			t.Fatal(err)
		}
		return s.ManagerID()
	}

	stripID()
	if got := reopen(); got != id {
		t.Fatalf("restoring a pre-ID store minted a new Manager ID: %q -> %q", id, got)
	}
	if !strings.Contains(readString(t, state), id) {
		t.Fatal("the kept ID was not written back to the store")
	}
	if err := os.Remove(idFile); err != nil {
		t.Fatal(err)
	}
	if got := reopen(); got != id {
		t.Fatalf("losing the ID file alone changed the Manager ID: %q -> %q", id, got)
	}
	if st, err := os.Stat(idFile); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("ID file not rewritten 0600: %v %v", st, err)
	}
	stripID()
	if err := os.Remove(idFile); err != nil {
		t.Fatal(err)
	}
	if got := reopen(); got == id || !managerIDRe.MatchString(got) {
		t.Fatalf("with the ID lost everywhere: %q (was %q)", got, id)
	}
}

// approvedWithLiveProxy approves a with the fake proxy and waits until the
// snapshot shows it live.
func approvedWithLiveProxy(t *testing.T, s *PrincipalStore, dir, a string) (stop func()) {
	t.Helper()
	stop = startFakeProxyAck(t, dir)
	if err := s.Approve(a); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !s.Snapshot().ProxyLive && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !s.Snapshot().ProxyLive {
		t.Fatal("setup: the proxy never became live")
	}
	return stop
}

// A1: a heartbeat dated in the future (host clock stepped back, a hand
// `touch`) does not keep a stopped proxy live.
func TestH8BFutureHeartbeatIsStale(t *testing.T) {
	shortProxyAckWait(t)
	oldAge := proxyHeartbeatMaxAge
	proxyHeartbeatMaxAge = 300 * time.Millisecond
	t.Cleanup(func() { proxyHeartbeatMaxAge = oldAge })
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	a, b := enrollTestMac(t, s, 0), enrollTestMac(t, s, 1)
	stop := approvedWithLiveProxy(t, s, dir, a)
	stop()
	fut := time.Now().Add(time.Hour)
	if err := os.Chtimes(filepath.Join(dir, reloadHeartbeatName), fut, fut); err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(b); !errors.Is(err, ErrProxyStopped) {
		t.Fatalf("A1: Approve with a stopped proxy whose heartbeat is dated +1h = %v, want ErrProxyStopped", err)
	}
}

// A2: a proxy that keeps its heartbeat but stops acknowledging reloads is
// refused once the requests written after its last ack are older than
// proxyAckWait.
func TestH8BStalledProxyIsRefused(t *testing.T) {
	shortProxyAckWait(t)
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	macs := []string{enrollTestMac(t, s, 0), enrollTestMac(t, s, 1), enrollTestMac(t, s, 2), enrollTestMac(t, s, 3)}
	stop := approvedWithLiveProxy(t, s, dir, macs[0])
	stop()
	hbStop := make(chan struct{})
	hbDone := make(chan struct{})
	go func() { // heartbeat only, no acks
		defer close(hbDone)
		for {
			select {
			case <-hbStop:
				return
			case <-time.After(20 * time.Millisecond):
				_ = os.WriteFile(filepath.Join(dir, reloadHeartbeatName), []byte("1\n"), 0o600)
			}
		}
	}()
	defer func() { close(hbStop); <-hbDone }()
	// The revoke writes a request the stalled proxy never acknowledges.
	if err := s.Revoke(macs[0]); err != nil {
		t.Fatal(err)
	}
	refused := 0
	for _, m := range macs[1:] {
		time.Sleep(2 * proxyAckWait)
		if err := s.Approve(m); errors.Is(err, ErrProxyStalled) {
			refused++
		} else {
			t.Errorf("Approve %s with a stalled proxy = %v, want ErrProxyStalled", m, err)
		}
	}
	snap := s.Snapshot()
	if snap.ProxyLive || !strings.Contains(snap.ProxyProblem, "does not acknowledge") {
		t.Fatalf("snapshot with a stalled proxy: live=%v problem=%q", snap.ProxyLive, snap.ProxyProblem)
	}
	t.Logf("A2: %d/3 approvals refused while the proxy acked nothing new", refused)
}

// A3: the proxy image is rolled back to a861a46 (whole-line ack, no
// heartbeat) after a heartbeat image ran and left reload.heartbeat behind.
// The refusal says to update the image, not that the proxy stopped.
func TestH8BRollbackToPreHeartbeatImageSaysUpdate(t *testing.T) {
	old := proxyAckWait
	proxyAckWait = 3 * time.Second
	t.Cleanup(func() { proxyAckWait = old })
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	a := enrollTestMac(t, s, 0)
	past := time.Now().Add(-time.Hour)
	_ = os.WriteFile(filepath.Join(dir, reloadHeartbeatName), []byte("1\n"), 0o600)
	_ = os.Chtimes(filepath.Join(dir, reloadHeartbeatName), past, past)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // the a861a46 loop: whole-line ack, no heartbeat
		defer close(done)
		last := ""
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			req, err := os.ReadFile(filepath.Join(dir, reloadRequestName))
			if err != nil || string(req) == last {
				continue
			}
			_ = os.WriteFile(filepath.Join(dir, reloadAckName), req, 0o600)
			last = string(req)
		}
	}()
	defer func() { close(stop); <-done }()
	start := time.Now()
	err = s.Approve(a)
	if !errors.Is(err, ErrProxyImageOutdated) || !strings.Contains(proxyRefusal(err), "Update the jm-respproxy image") {
		t.Fatalf("A3: refusal is %v (%q), want ErrProxyImageOutdated", err, proxyRefusal(err))
	}
	if d := time.Since(start); d > 2500*time.Millisecond {
		t.Fatalf("A3: the outdated image was reported after %v", d)
	}
}

// First tick of a heartbeat image on a share that never had a heartbeat:
// the entrypoint writes reload.ack, then reload.heartbeat in the same tick.
// An Approve that reads between the two must not report "outdated".
func TestH8BFirstTickAckBeforeHeartbeatIsNotOutdated(t *testing.T) {
	old := proxyAckWait
	proxyAckWait = 3 * time.Second
	t.Cleanup(func() { proxyAckWait = old })
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	a := enrollTestMac(t, s, 0)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // ack at once, heartbeat 300 ms later (a slow first tick)
		defer close(done)
		last := readStringOK(filepath.Join(dir, reloadRequestName))
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			req := readStringOK(filepath.Join(dir, reloadRequestName))
			if req == last {
				continue
			}
			_ = os.WriteFile(filepath.Join(dir, reloadAckName), []byte(req), 0o600)
			last = req
			time.Sleep(300 * time.Millisecond)
			_ = os.WriteFile(filepath.Join(dir, reloadHeartbeatName), []byte("1\n"), 0o600)
		}
	}()
	defer func() { close(stop); <-done }()
	if err := s.Approve(a); err != nil {
		t.Fatalf("Approve on the proxy's first tick = %v, want nil", err)
	}
}
