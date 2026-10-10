package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// startFakeProxyAck plays the respproxy entrypoint's reload handshake
// (server/respproxy/entrypoint.sh): when reload.request names the sha256 of
// the current principals.json, it writes reload.ack repeating the whole
// request line. Like the entrypoint it starts from the request already in
// the file, so a proxy that starts after a request never answers that one
// (server/respproxy/handshake_h8b_test.go runs the real loop), and it rewrites
// reload.heartbeat on every tick. It stops when the test ends (the returned
// func stops it earlier, like a stopped proxy container).
func startFakeProxyAck(t *testing.T, dir string) (stopProxy func()) {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	last := ""
	if b, err := os.ReadFile(filepath.Join(dir, reloadRequestName)); err == nil {
		last = string(b)
	}
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			_ = os.WriteFile(filepath.Join(dir, reloadHeartbeatName), []byte("1\n"), 0o600)
			req, err := os.ReadFile(filepath.Join(dir, reloadRequestName))
			if err != nil || string(req) == last {
				continue
			}
			want := strings.Fields(string(req))
			body, err := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
			if len(want) == 0 || err != nil {
				continue
			}
			sum := sha256.Sum256(body)
			if hex.EncodeToString(sum[:]) != want[0] {
				continue // a newer write is in flight
			}
			_ = os.WriteFile(filepath.Join(dir, reloadAckName), []byte(strings.Join(want, " ")+"\n"), 0o600)
			last = string(req)
		}
	}()
	var once sync.Once
	stopProxy = func() { once.Do(func() { close(stop); <-done }) }
	t.Cleanup(stopProxy)
	return stopProxy
}

func shortProxyAckWait(t *testing.T) {
	t.Helper()
	old := proxyAckWait
	proxyAckWait = 150 * time.Millisecond
	t.Cleanup(func() { proxyAckWait = old })
}

// TestH8BApproveRefusedUntilProxyLive: before the proxy cutover nothing
// acknowledges reload.request, so Approve must refuse; an approved Mac would
// switch to a principal the plain Redis refuses (review finding 1b). Once
// the proxy acknowledges, Approve succeeds.
func TestH8BApproveRefusedUntilProxyLive(t *testing.T) {
	shortProxyAckWait(t)
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "Edit-Bay-2", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); !errors.Is(err, ErrProxyNotLive) {
		t.Fatalf("Approve with no proxy ack = %v, want ErrProxyNotLive", err)
	}
	if got := s.Snapshot().byName(r.Principal); got == nil || got.Status != PrincipalPending {
		t.Fatalf("refused approve changed the record: %+v", got)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, filepath.Join(dir, proxyPrincipalsName)), r.Principal); ok {
		t.Fatal("refused approve published the Mac to the proxy principals file")
	}
	// A stale ack (an older file) is not live either.
	if err := os.WriteFile(filepath.Join(dir, reloadAckName), []byte("0000 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); !errors.Is(err, ErrProxyNotLive) {
		t.Fatalf("Approve with a stale ack = %v, want ErrProxyNotLive", err)
	}
	// An ack naming the current file's sha256 but written for an earlier
	// request (a proxy that acknowledged, then stopped) is not live either
	// (review 2 finding F1): only the whole request line counts.
	req := strings.Fields(readString(t, filepath.Join(dir, reloadRequestName)))
	if err := os.WriteFile(filepath.Join(dir, reloadAckName), []byte(req[0]+" 1791334399\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); !errors.Is(err, ErrProxyNotLive) {
		t.Fatalf("Approve with an ack for an earlier request of the same file = %v, want ErrProxyNotLive", err)
	}
	// No request and no ack at all (both files gone): two empty lines are
	// not a match.
	for _, n := range []string{reloadRequestName, reloadAckName} {
		if err := os.Remove(filepath.Join(dir, n)); err != nil {
			t.Fatal(err)
		}
	}
	s.mu.Lock()
	live := s.proxyLiveLocked()
	s.mu.Unlock()
	if live {
		t.Fatal("no reload.request and no reload.ack reads as a live proxy")
	}

	// The proxy comes up after the last request was written: Approve
	// re-requests a reload and waits for the ack.
	startFakeProxyAck(t, dir)
	if err := s.Approve(r.Principal); err != nil {
		t.Fatalf("Approve with a live proxy: %v", err)
	}
	if got := s.Snapshot().byName(r.Principal); got == nil || got.Status != PrincipalApproved {
		t.Fatalf("approve with a live proxy: %+v", got)
	}
}

func readString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestH8BApproveRefusedWithoutProxyDir: no proxy directory means no proxy
// at all; nothing may be approved.
func TestH8BApproveRefusedWithoutProxyDir(t *testing.T) {
	shortProxyAckWait(t)
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json")})
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "h", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(r.Principal); !errors.Is(err, ErrProxyNotLive) {
		t.Fatalf("Approve with no proxy dir = %v, want ErrProxyNotLive", err)
	}
	if _, err := s.SetAutoApproveWindow(time.Hour); !errors.Is(err, ErrProxyNotLive) {
		t.Fatalf("open window with no proxy dir = %v, want ErrProxyNotLive", err)
	}
}

// TestH8BAutoApproveWindowRefusedUntilProxyLive: opening the window is
// refused before the cutover (closing always works), and over HTTP both
// refusals are a 409 with the cutover message.
func TestH8BAutoApproveWindowRefusedUntilProxyLive(t *testing.T) {
	shortProxyAckWait(t)
	mux, proxyDir, _, _ := newH8BAPINoProxy(t)
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 24})
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "proxy is not live") {
		t.Fatalf("open window before cutover: %d %s, want 409 proxy is not live", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 0}); rr.Code != http.StatusOK {
		t.Fatalf("close window: %d %s", rr.Code, rr.Body)
	}
	// A Mac enrolling now stays pending (the window never opened).
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", EnrollRequest{InstanceID: testInstanceID, Host: "Edit-Bay-2", Secret: testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	if er.Status != PrincipalPending {
		t.Fatalf("enroll after refused window: %+v", er)
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "proxy cutover") {
		t.Fatalf("approve before cutover: %d %s, want 409 with the cutover message", rr.Code, rr.Body)
	}
	startFakeProxyAck(t, proxyDir)
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		t.Fatalf("approve after cutover: %d %s", rr.Code, rr.Body)
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 24}); rr.Code != http.StatusOK {
		t.Fatalf("open window after cutover: %d %s", rr.Code, rr.Body)
	}
}

// TestH8BPrincipalsPageShowsIdentityNotes: the admin sees the self-reported
// previous instance ID and the device-mismatch flag (read-only).
func TestH8BPrincipalsPageShowsIdentityNotes(t *testing.T) {
	raw, err := staticFS.ReadFile("static/principals.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(raw)
	for _, want := range []string{"p.previous_instance_id", "p.device_mismatch", "p.mismatched_device_digest", "not verified", "+ identityNotes(p) +"} {
		if !strings.Contains(page, want) {
			t.Fatalf("principals.html does not render %q", want)
		}
	}
}

func enrollTestMac(t *testing.T, s *PrincipalStore, i int) string {
	t.Helper()
	id := strings.Replace(testInstanceID, testInstanceID[:2], []string{"11", "22", "33", "44", "55", "66", "77", "88", "99"}[i], 1)
	r, err := s.Enroll(EnrollRequest{InstanceID: id, Host: "Edit-Bay", Secret: strings.Repeat(string(rune('a'+i)), 40)}, "192.0.2."+string(rune('1'+i)))
	if err != nil {
		t.Fatal(err)
	}
	return r.Principal
}

// TestH8BProxyStopsWhileManagerRuns: review 3 finding 3. The proxy
// acknowledged, then stopped while the Manager kept running. Its last ack
// still repeats a request of this Manager, but its heartbeat goes stale, so
// Approve is refused with the "stopped" reason.
func TestH8BProxyStopsWhileManagerRuns(t *testing.T) {
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
	stop := startFakeProxyAck(t, dir)
	if err := s.Approve(a); err != nil {
		t.Fatalf("Approve A with a live proxy: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !s.Snapshot().ProxyLive && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	time.Sleep(2 * proxyHeartbeatMaxAge)
	if err := s.Approve(b); !errors.Is(err, ErrProxyStopped) {
		t.Fatalf("Approve B after the proxy stopped (Manager kept running) = %v, want ErrProxyStopped", err)
	}
	if snap := s.Snapshot(); snap.ProxyLive || !strings.Contains(snap.ProxyProblem, "stopped") {
		t.Fatalf("snapshot after the proxy stopped: live=%v problem=%q", snap.ProxyLive, snap.ProxyProblem)
	}
}

// TestH8BOutdatedProxyImageRefusedWithUpdateMessage: review 3 finding 2. A
// proxy image older than the heartbeat answers the handshake (old two-field
// "sha256 seconds" ack, or a861a46's whole-line ack) but writes no
// reload.heartbeat. Approve is refused at once, and the 409 says to update
// the jm-respproxy image, not to finish the cutover.
func TestH8BOutdatedProxyImageRefusedWithUpdateMessage(t *testing.T) {
	old := proxyAckWait
	proxyAckWait = 3 * time.Second
	t.Cleanup(func() { proxyAckWait = old })
	for _, style := range []string{"two-field", "whole-line"} {
		t.Run(style, func(t *testing.T) {
			mux, proxyDir, _, _ := newH8BAPINoProxy(t)
			stop := make(chan struct{})
			done := make(chan struct{})
			go func() { // the old entrypoint loop: acks, never a heartbeat
				defer close(done)
				for {
					select {
					case <-stop:
						return
					case <-time.After(5 * time.Millisecond):
					}
					req := strings.Fields(readStringOK(filepath.Join(proxyDir, reloadRequestName)))
					if len(req) < 3 {
						continue
					}
					ack := strings.Join(req, " ")
					if style == "two-field" {
						ack = req[0] + " 1791334399"
					}
					_ = os.WriteFile(filepath.Join(proxyDir, reloadAckName), []byte(ack+"\n"), 0o600)
				}
			}()
			t.Cleanup(func() { close(stop); <-done })
			rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", EnrollRequest{InstanceID: testInstanceID, Host: "Edit-Bay-2", Secret: testMacSecret})
			er := decodeEnroll(t, rr.Body.Bytes())
			start := time.Now()
			rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal})
			if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "Update the jm-respproxy image") ||
				strings.Contains(rr.Body.String(), "Finish the proxy cutover") {
				t.Fatalf("approve with an outdated proxy image: %d %s", rr.Code, rr.Body)
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("the outdated image was reported after %v; it answers at once", d)
			}
			rr = doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 24})
			if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "Update the jm-respproxy image") {
				t.Fatalf("open window with an outdated proxy image: %d %s", rr.Code, rr.Body)
			}
		})
	}
}

func readStringOK(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

// TestH8BSerialApprovalsWithLiveProxy: review 3 finding 7. Each Approve
// writes a new reload.request; when liveness required the ack of the newest
// request, an approval less than one entrypoint tick after another was
// refused. The fake proxy here answers on a 2 s tick, like the entrypoint.
// Eight approvals 400 ms apart span two ticks, so the stall rule (review 4
// A2: requests left unacknowledged for longer than proxyAckWait) is
// exercised against a live proxy too: none may be refused.
func TestH8BSerialApprovalsWithLiveProxy(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for i := 0; i < 9; i++ {
		names = append(names, enrollTestMac(t, s, i))
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { // the entrypoint's 2 s tick, starting from the current request
		defer close(done)
		last := readStringOK(filepath.Join(dir, reloadRequestName))
		for {
			_ = os.WriteFile(filepath.Join(dir, reloadHeartbeatName), []byte("1\n"), 0o600)
			if req := readStringOK(filepath.Join(dir, reloadRequestName)); req != last {
				body, _ := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
				sum := sha256.Sum256(body)
				if f := strings.Fields(req); len(f) > 0 && f[0] == hex.EncodeToString(sum[:]) {
					_ = os.WriteFile(filepath.Join(dir, reloadAckName), []byte(strings.Join(f, " ")+"\n"), 0o600)
					last = req
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
	if err := s.Approve(names[0]); err != nil {
		t.Fatalf("first approve: %v", err)
	}
	refused := 0
	for i, n := range names[1:] {
		time.Sleep(400 * time.Millisecond)
		start := time.Now()
		if err := s.Approve(n); err != nil {
			refused++
			t.Errorf("approve %d, 400 ms after the previous one: %v", i+1, err)
			continue
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("approve %d waited %v for a proxy that is live", i+1, d)
		}
	}
	t.Logf("serial approvals 400 ms apart: %d/%d refused", refused, len(names)-1)
}

// TestH8BManagerIDPersistedAndSent: review 3 finding 6. The Manager mints a
// random ID once, keeps it in its store across restarts, and sends it in
// every enroll and poll answer (body and header), in refusals (header), and
// on GET.
func TestH8BManagerIDPersistedAndSent(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "store.json")
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	id := s.ManagerID()
	if !managerIDRe.MatchString(id) {
		t.Fatalf("manager id %q", id)
	}
	if !strings.Contains(readString(t, state), id) {
		t.Fatal("the Manager ID is not persisted with the store")
	}
	s2, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state})
	if err != nil {
		t.Fatal(err)
	}
	if s2.ManagerID() != id {
		t.Fatalf("restart minted a new ID: %q != %q", s2.ManagerID(), id)
	}
	other, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "other.json")})
	if err != nil {
		t.Fatal(err)
	}
	if other.ManagerID() == id {
		t.Fatal("two Managers share an ID")
	}
	r, err := s2.Enroll(EnrollRequest{InstanceID: testInstanceID, Host: "h", Secret: testMacSecret}, "192.0.2.1")
	if err != nil || r.ManagerID != id {
		t.Fatalf("enroll answer manager_id=%q err=%v", r.ManagerID, err)
	}
	p, err := s2.Enroll(EnrollRequest{InstanceID: testInstanceID, PollToken: r.PollToken}, "192.0.2.1")
	if err != nil || p.ManagerID != id {
		t.Fatalf("poll answer manager_id=%q err=%v", p.ManagerID, err)
	}
	pa := &principalsAPI{store: s2}
	for _, tc := range []struct {
		method, body string
		code         int
	}{
		{http.MethodGet, "", http.StatusOK},
		{http.MethodPost, `{"instance_id":"` + testInstanceID + `","poll_token":"` + strings.Repeat("0", 64) + `"}`, http.StatusForbidden},
		{http.MethodPost, `{"instance_id":"` + testInstanceID + `","host":"h","secret":"` + strings.Repeat("z", 40) + `"}`, http.StatusConflict},
	} {
		req := httptest.NewRequest(tc.method, "/api/principals/enroll", strings.NewReader(tc.body))
		req.RemoteAddr = "192.0.2.9:1234"
		rr := httptest.NewRecorder()
		pa.handleEnroll(rr, req)
		if rr.Code != tc.code || rr.Header().Get(managerIDHeader) != id {
			t.Fatalf("%s %s: %d header=%q, want %d with the Manager ID", tc.method, tc.body, rr.Code, rr.Header().Get(managerIDHeader), tc.code)
		}
		if tc.method == http.MethodGet && !strings.Contains(rr.Body.String(), `"manager_id": "`+id+`"`) {
			t.Fatalf("GET body %s", rr.Body)
		}
	}
}
