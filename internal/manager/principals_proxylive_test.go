package manager

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
			if req, ok := fakeProxyAckOnce(dir, last); ok {
				last = req
			}
			fakeProxyWrite(dir, reloadHeartbeatName, "1")
		}
	}()
	var once sync.Once
	stopProxy = func() { once.Do(func() { close(stop); <-done }) }
	t.Cleanup(stopProxy)
	return stopProxy
}

// fakeProxyAckOnce acknowledges reload.request if it is new and matches the
// principals bytes, and returns the request it acknowledged.
func fakeProxyAckOnce(dir, last string) (string, bool) {
	req, err := os.ReadFile(filepath.Join(dir, reloadRequestName))
	if err != nil || string(req) == last {
		return "", false
	}
	want := strings.Fields(string(req))
	body, err := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
	if len(want) == 0 || err != nil {
		return "", false
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != want[0] {
		return "", false // a newer write is in flight
	}
	fakeProxyWrite(dir, reloadAckName, strings.Join(want, " "))
	return string(req), true
}

// fakeProxyWrite replaces a handshake file by rename, as jm-respproxy does
// (internal/respproxy writeReloadFile), so the Manager never reads a
// half-written receipt. An in-place truncating write let Approve read an empty
// reload.applied and wait for the next tick. The fake skips the fsync: only
// the rename's atomicity matters here.
func fakeProxyWrite(dir, name, line string) {
	tmp := filepath.Join(dir, ".fake-"+name)
	if os.WriteFile(tmp, []byte(line+"\n"), 0o600) == nil {
		_ = os.Rename(tmp, filepath.Join(dir, name))
	}
}

// approveIssuedRequests runs Approve and returns how many reload requests it
// wrote: one that the proxy must apply before Approve proceeds
// (requireProxyLive), and one for its own change.
func approveIssuedRequests(s *PrincipalStore, name string) (int, error) {
	count := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.issued)
	}
	before := count()
	err := s.Approve(name)
	return count() - before, err
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
// Eight approvals 400 ms apart span several ticks, so the stall rule (review
// 4 A2: requests left unacknowledged for longer than proxyAckWait) is
// exercised against a live proxy too: none may be refused. Each waits for
// the proxy to apply the request it wrote first (a receipt older than the
// call proves nothing about the proxy running now), and writes exactly that
// one besides its change's.
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
			if req, ok := fakeProxyAckOnce(dir, last); ok {
				last = req
			}
			fakeProxyWrite(dir, reloadHeartbeatName, "1")
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
		issued, err := approveIssuedRequests(s, n)
		if err != nil {
			refused++
			t.Errorf("approve %d, 400 ms after the previous one: %v", i+1, err)
			continue
		}
		if issued != 2 {
			t.Errorf("approve %d wrote %d reload requests, want 2 (its proof request and its change)", i+1, issued)
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

// TestH8BReceiptsWrittenBetweenReadsAreCurrent: the proxy writes
// reload.applied and then reload.ack in one tick. A tick that lands between
// the Manager's two receipt reads must not make a current image look like
// one that writes only the legacy reload.ack (the 409 "update the
// jm-respproxy image" on the first approval after a start).
func TestH8BReceiptsWrittenBetweenReadsAreCurrent(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "state.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	fakeProxyWrite(dir, reloadHeartbeatName, "1")
	req := requestLine(filepath.Join(dir, reloadRequestName))
	ticked := false
	proxyBetweenAckReads = func() {
		if !ticked {
			ticked = true
			fakeProxyWrite(dir, reloadAckName, req)
			fakeProxyWrite(dir, legacyReloadAckName, req)
		}
	}
	t.Cleanup(func() { proxyBetweenAckReads = nil })
	snap := s.Snapshot()
	if !ticked {
		t.Fatal("the receipt reads did not run the hook")
	}
	if !snap.ProxyLive || snap.ProxyProblem != "" {
		t.Fatalf("a current proxy's first ack, written between the receipt reads, read as live=%v problem=%q",
			snap.ProxyLive, snap.ProxyProblem)
	}
}

// TestH8BProxyStartBetweenReadsIsNotOutdated: a starting proxy deletes
// reload.ack and then reload.applied. A start that lands between the
// Manager's two receipt reads pairs the old reload.ack with an empty
// reload.applied, which must not read as an image that writes only the
// legacy receipt (a false 409 "update the jm-respproxy image").
func TestH8BProxyStartBetweenReadsIsNotOutdated(t *testing.T) {
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "state.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	fakeProxyWrite(dir, reloadHeartbeatName, "1")
	req := requestLine(filepath.Join(dir, reloadRequestName))
	fakeProxyWrite(dir, reloadAckName, req)
	fakeProxyWrite(dir, legacyReloadAckName, req)
	started := false
	proxyBetweenAckReads = func() {
		if !started {
			started = true
			os.Remove(filepath.Join(dir, legacyReloadAckName))
			os.Remove(filepath.Join(dir, reloadAckName))
		}
	}
	t.Cleanup(func() { proxyBetweenAckReads = nil })
	snap := s.Snapshot()
	if !started {
		t.Fatal("the receipt reads did not run the hook")
	}
	if snap.ProxyProblem == proxyProblemText(proxyOutdated) {
		t.Fatalf("a proxy start between the receipt reads read as an outdated image: %q", snap.ProxyProblem)
	}
}

// TestH8BDowngradedProxyStaleReceiptRefused: the proxy image is replaced,
// while the Manager keeps running, by one that predates reload.applied. The
// current proxy's last receipt stays on disk (an older image never deletes
// it) and repeats the newest request of this Manager, and the older image
// keeps reload.heartbeat fresh. That receipt proves nothing about the proxy
// running now: Approve and the auto-approve window must be refused, whether
// the older image answers in reload.ack only or not at all. A current image
// started again passes.
func TestH8BDowngradedProxyStaleReceiptRefused(t *testing.T) {
	for _, tc := range []struct {
		name      string
		legacyAck bool // the older image answers in reload.ack
		want      error
	}{
		{"signal-only-image", true, ErrProxyImageOutdated},
		{"silent-image", false, ErrProxyStalled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortProxyAckWait(t)
			dir := t.TempDir()
			s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			a, b, c := enrollTestMac(t, s, 0), enrollTestMac(t, s, 1), enrollTestMac(t, s, 2)
			stop := approvedWithLiveProxy(t, s, dir, a)
			deadline := time.Now().Add(2 * time.Second)
			for !s.Snapshot().ReloadApplied && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if !s.Snapshot().ReloadApplied {
				t.Fatal("setup: the proxy never applied the newest request")
			}
			stop()

			stopOld := startOlderProxyImage(t, dir, tc.legacyAck)
			time.Sleep(50 * time.Millisecond)

			if err := s.Approve(b); !errors.Is(err, tc.want) {
				t.Fatalf("Approve with the older proxy image and a stale receipt = %v, want %v", err, tc.want)
			}
			if got := s.Snapshot().byName(b); got == nil || got.Status != PrincipalPending {
				t.Fatalf("refused approve changed the record: %+v", got)
			}
			if _, err := s.SetAutoApproveWindow(time.Hour); !errors.Is(err, tc.want) {
				t.Fatalf("auto-approve window with the older proxy image = %v, want %v", err, tc.want)
			}
			stopOld()

			startCurrentProxyImage(t, dir)
			if err := s.Approve(c); err != nil {
				t.Fatalf("Approve after a current proxy image started again: %v", err)
			}
		})
	}
}

// waitStatus waits for an auto-approve pass to decide name.
func waitStatus(t *testing.T, s *PrincipalStore, name string, want PrincipalStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		p := s.Snapshot().byName(name)
		if p != nil && p.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: status %+v, want %s", name, p, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// pollUntilApproved polls the enroll route with the Mac's poll token until
// an auto-approve pass approved it, and returns that answer.
func pollUntilApproved(t *testing.T, mux http.Handler, instanceID, token string) (*httptest.ResponseRecorder, EnrollResponse) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": instanceID, "poll_token": token})
		got := decodeEnroll(t, rr.Body.Bytes())
		if got.Status == PrincipalApproved {
			return rr, got
		}
		if time.Now().After(deadline) {
			t.Fatalf("poll: %d %+v, want approved", rr.Code, got)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// startOlderProxyImage plays an image from before reload.applied: it starts
// from the request already in the file, keeps reload.heartbeat fresh and
// never touches reload.applied (a receipt a current image left stays). With
// legacyAck it answers new requests in reload.ack only.
func startOlderProxyImage(t *testing.T, dir string, legacyAck bool) (stop func()) {
	t.Helper()
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		last := readStringOK(filepath.Join(dir, reloadRequestName))
		for {
			select {
			case <-quit:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if req := readStringOK(filepath.Join(dir, reloadRequestName)); legacyAck && req != last && req != "" {
				fakeProxyWrite(dir, legacyReloadAckName, strings.TrimSpace(req))
				last = req
			}
			fakeProxyWrite(dir, reloadHeartbeatName, "1")
		}
	}()
	var once sync.Once
	stop = func() { once.Do(func() { close(quit); <-done }) }
	t.Cleanup(stop)
	return stop
}

// startCurrentProxyImage starts a current image: it deletes both receipts
// as it starts (respproxy.NewPrincipalReloader), then acknowledges.
func startCurrentProxyImage(t *testing.T, dir string) (stop func()) {
	t.Helper()
	for _, n := range []string{legacyReloadAckName, reloadAckName} {
		if err := os.Remove(filepath.Join(dir, n)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
	return startFakeProxyAck(t, dir)
}

func shortAutoApproveRetry(t *testing.T) {
	t.Helper()
	old := autoApproveRetry
	autoApproveRetry = 50 * time.Millisecond
	t.Cleanup(func() { autoApproveRetry = old })
}

// pollMac answers one poll of the Mac enrolled as r (what asks for an
// auto-approve pass while it waits).
func pollMac(t *testing.T, s *PrincipalStore, instanceID string, r EnrollResponse) {
	t.Helper()
	if _, err := s.Enroll(EnrollRequest{InstanceID: instanceID, PollToken: r.PollToken}, "192.0.2.200"); err != nil {
		t.Fatal(err)
	}
}

// TestH8BAutoApproveNeedsFreshProof: the auto-approve window was opened
// against a current proxy, then the image was replaced by one from before
// reload.applied while the Manager kept running. Macs enrolling in the
// window must stay pending (the window admitted them), with the reason in
// the snapshot, and be approved once a current image runs again.
func TestH8BAutoApproveNeedsFreshProof(t *testing.T) {
	for _, tc := range []struct {
		name      string
		legacyAck bool
		problem   string
	}{
		{"signal-only-image", true, "update the jm-respproxy image"},
		{"silent-image", false, "does not acknowledge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortProxyAckWait(t)
			shortAutoApproveRetry(t)
			dir := t.TempDir()
			s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			stop := startCurrentProxyImage(t, dir)
			if _, err := s.SetAutoApproveWindow(time.Hour); err != nil {
				t.Fatal(err)
			}
			stop()
			startOlderProxyImage(t, dir, tc.legacyAck)
			time.Sleep(50 * time.Millisecond)

			ids := []string{instanceN(1), instanceN(2)}
			var macs []EnrollResponse
			for i, id := range ids {
				r, err := s.Enroll(EnrollRequest{InstanceID: id, Host: "h", Secret: testMacSecret}, fmt.Sprintf("192.0.2.%d", i+1))
				if err != nil {
					t.Fatal(err)
				}
				macs = append(macs, r)
			}
			time.Sleep(4 * proxyAckWait)
			snap := s.Snapshot()
			for _, r := range macs {
				if p := snap.byName(r.Principal); p.Status != PrincipalPending || !p.AutoApprovePending {
					t.Fatalf("%s auto-approved against the older image: %+v", r.Principal, p)
				}
			}
			if !strings.Contains(snap.AutoApproveProblem, tc.problem) {
				t.Fatalf("auto_approve_problem = %q, want it to say %q", snap.AutoApproveProblem, tc.problem)
			}
			if snap.AutoApproveUsed != 2 {
				t.Fatalf("auto_approve_used = %d, want 2 (counted at admission)", snap.AutoApproveUsed)
			}

			startCurrentProxyImage(t, dir)
			for i, r := range macs {
				pollMac(t, s, ids[i], r)
				waitStatus(t, s, r.Principal, PrincipalApproved)
			}
			snap = s.Snapshot()
			if snap.AutoApproveProblem != "" || snap.AutoApproveUsed != 2 || snap.NeedsReview != 2 {
				t.Fatalf("after a current image: problem=%q used=%d review=%d", snap.AutoApproveProblem, snap.AutoApproveUsed, snap.NeedsReview)
			}
		})
	}
}

// TestH8BAutoApproveAfterManagerRestartNeedsProof: the window was opened
// against a current proxy, which then stopped and was replaced by an image
// from before reload.applied; the Manager restarts and reopens the window
// from its store. Its first auto-approval needs a fresh proof like any
// other: the Macs stay pending until a current image runs.
func TestH8BAutoApproveAfterManagerRestartNeedsProof(t *testing.T) {
	shortProxyAckWait(t)
	shortAutoApproveRetry(t)
	dir := t.TempDir()
	state := filepath.Join(dir, "store.json")
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state, ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	stop := startCurrentProxyImage(t, dir)
	if _, err := s.SetAutoApproveWindow(time.Hour); err != nil {
		t.Fatal(err)
	}
	a, err := s.Enroll(EnrollRequest{InstanceID: instanceN(1), Host: "h", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, s, a.Principal, PrincipalApproved)
	stop()
	startOlderProxyImage(t, dir, true)

	s2, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: state, ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Snapshot().AutoApproveUntil == 0 {
		t.Fatal("setup: the restarted Manager did not reopen the window")
	}
	b, err := s2.Enroll(EnrollRequest{InstanceID: instanceN(2), Host: "h", Secret: testMacSecret}, "192.0.2.2")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * proxyAckWait)
	pollMac(t, s2, instanceN(2), b)
	time.Sleep(4 * proxyAckWait)
	if p := s2.Snapshot().byName(b.Principal); p.Status != PrincipalPending || !p.AutoApprovePending {
		t.Fatalf("auto-approved after a Manager restart against the older image: %+v", p)
	}
	if prob := s2.Snapshot().AutoApproveProblem; !strings.Contains(prob, "update the jm-respproxy image") {
		t.Fatalf("auto_approve_problem = %q", prob)
	}
	startCurrentProxyImage(t, dir)
	pollMac(t, s2, instanceN(2), b)
	waitStatus(t, s2, b.Principal, PrincipalApproved)
}

// waitAutoIdleAtCleanup makes the test wait, at cleanup and before the
// timers it shortened are restored, for any auto-approve pass to finish.
func waitAutoIdleAtCleanup(t *testing.T, s *PrincipalStore) {
	t.Helper()
	t.Cleanup(func() { waitAutoIdle(t, s) })
}

// waitAutoIdle waits until s runs no auto-approve pass. A pass starts only
// when the store is asked for one, so a store nothing calls stays idle.
func waitAutoIdle(t *testing.T, s *PrincipalStore) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mu.Lock()
		running := s.autoRunning
		s.mu.Unlock()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("an auto-approve pass still runs after 10 s")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestH8BAutoApproveReplacedRecordWaitsForItsOwnProof: while a pass waits
// for its proof, the admitted Mac is revoked and forgotten, and the same
// instance ID enrolls again with another secret. The proof was written
// before the new record existed and must not approve it; the new record
// waits for a proof written after it.
func TestH8BAutoApproveReplacedRecordWaitsForItsOwnProof(t *testing.T) {
	shortAutoApproveRetry(t)
	dir := t.TempDir()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	waitAutoIdleAtCleanup(t, s)
	stop := startCurrentProxyImage(t, dir)
	if _, err := s.SetAutoApproveWindow(time.Hour); err != nil {
		t.Fatal(err)
	}
	stop()
	// A current proxy that applies a request only 1 s after it first sees
	// it, and, until release, only requests first seen before cutoff.
	var mu sync.Mutex
	var cutoff time.Time
	release := false
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		seen := map[string]time.Time{}
		for {
			select {
			case <-quit:
				return
			case <-time.After(5 * time.Millisecond):
			}
			req := strings.TrimSpace(readStringOK(filepath.Join(dir, reloadRequestName)))
			if req != "" {
				if _, ok := seen[req]; !ok {
					seen[req] = time.Now()
				}
				mu.Lock()
				ok := release || cutoff.IsZero() || seen[req].Before(cutoff)
				mu.Unlock()
				body, err := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
				if ok && err == nil && time.Since(seen[req]) > time.Second && sha256Hex(string(body)) == strings.Fields(req)[0] {
					fakeProxyWrite(dir, reloadAckName, req)
					fakeProxyWrite(dir, legacyReloadAckName, req)
				}
			}
			fakeProxyWrite(dir, reloadHeartbeatName, "1")
		}
	}()
	t.Cleanup(func() { close(quit); <-done })

	id := instanceN(4)
	first, err := s.Enroll(EnrollRequest{InstanceID: id, Host: "h", Secret: strings.Repeat("q", 40)}, "192.0.2.50")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // the pass has written its request and waits
	if err := s.Revoke(first.Principal); err != nil {
		t.Fatal(err)
	}
	if err := s.Forget(first.Principal); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	cutoff = time.Now()
	mu.Unlock()
	second, err := s.Enroll(EnrollRequest{InstanceID: id, Host: "other", Secret: strings.Repeat("z", 40)}, "192.0.2.51")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second) // the first proof has been applied
	if p := s.Snapshot().byName(second.Principal); p.Status != PrincipalPending || p.Host != "other" {
		t.Fatalf("the record enrolled during the proof: %+v, want pending", p)
	}
	mu.Lock()
	release = true
	mu.Unlock()
	s.mu.Lock()
	s.autoFailedAt = time.Time{} // end the retry wait
	s.mu.Unlock()
	pollMac(t, s, id, second)
	waitStatus(t, s, second.Principal, PrincipalApproved)
}

// TestH8BAutoApproveWindowChangeHandsWaitingMacsToAdmin: Macs a window
// admitted that still wait for a proof when the window is reopened (or had
// expired) are not auto-approved by the new window, whose counters do not
// count them: they wait for an admin. An expired window's admission counts
// for nothing even before any pass runs.
func TestH8BAutoApproveWindowChangeHandsWaitingMacsToAdmin(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(map[bool]string{false: "reopened", true: "expired-then-reopened"}[expire], func(t *testing.T) {
			shortProxyAckWait(t)
			dir := t.TempDir()
			var clockMu sync.Mutex
			clock := time.Now()
			now := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
			s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir, Now: now})
			if err != nil {
				t.Fatal(err)
			}
			waitAutoIdleAtCleanup(t, s)
			stop := startCurrentProxyImage(t, dir)
			if _, err := s.SetAutoApproveWindow(time.Hour, 2); err != nil {
				t.Fatal(err)
			}
			stop()
			stopOld := startOlderProxyImage(t, dir, false)
			var waiting []EnrollResponse
			for i := 1; i <= 2; i++ {
				r, err := s.Enroll(EnrollRequest{InstanceID: instanceN(i), Host: "h", Secret: testMacSecret}, fmt.Sprintf("192.0.2.%d", i))
				if err != nil {
					t.Fatal(err)
				}
				waiting = append(waiting, r)
			}
			deadline := time.Now().Add(3 * time.Second)
			for s.Snapshot().AutoApproveProblem == "" && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if s.Snapshot().AutoApproveProblem == "" {
				t.Fatal("setup: the first pass did not fail")
			}
			if expire {
				clockMu.Lock()
				clock = clock.Add(2 * time.Hour)
				clockMu.Unlock()
				for _, r := range waiting {
					if p := s.Snapshot().byName(r.Principal); p.Status != PrincipalPending || p.AutoApprovePending {
						t.Fatalf("after the window expired (no pass yet): %+v, want plain pending", p)
					}
				}
			}
			stopOld()
			startCurrentProxyImage(t, dir)
			if _, err := s.SetAutoApproveWindow(time.Hour, 2); err != nil {
				t.Fatal(err)
			}
			s.mu.Lock()
			s.autoFailedAt = time.Time{} // end the retry wait
			s.mu.Unlock()
			var fresh []EnrollResponse
			for i := 3; i <= 4; i++ {
				r, err := s.Enroll(EnrollRequest{InstanceID: instanceN(i), Host: "h", Secret: testMacSecret}, fmt.Sprintf("192.0.2.%d", i))
				if err != nil {
					t.Fatal(err)
				}
				fresh = append(fresh, r)
			}
			for i, r := range waiting {
				pollMac(t, s, instanceN(i+1), r)
			}
			for _, r := range fresh {
				waitStatus(t, s, r.Principal, PrincipalApproved)
			}
			time.Sleep(300 * time.Millisecond)
			snap := s.Snapshot()
			for _, r := range waiting {
				if p := snap.byName(r.Principal); p.Status != PrincipalPending || p.AutoApprovePending {
					t.Fatalf("a Mac the earlier window admitted: %+v, want pending for an admin", p)
				}
			}
			if snap.AutoApproveUsed != 2 || snap.NeedsReview != 2 {
				t.Fatalf("used=%d review=%d, want 2/2 (the new window's cap)", snap.AutoApproveUsed, snap.NeedsReview)
			}
		})
	}
}

// TestH8BAutoApproveWindowExpiresDuringProof: the auto-approve window
// expires while a pass waits for its proxy proof, and the proof then
// succeeds. Expiry clears no AutoApprovePending flag, so only the pass's
// check of the window after the proof keeps the Mac from being approved by
// a window that has ended. The Mac stays pending; the next poll clears the
// lapsed flag and the Mac waits for an admin, whose Approve still works.
func TestH8BAutoApproveWindowExpiresDuringProof(t *testing.T) {
	old := proxyAckWait
	proxyAckWait = 5 * time.Second // the proof waits for the release below
	t.Cleanup(func() { proxyAckWait = old })
	dir := t.TempDir()
	var clockMu sync.Mutex
	clock := time.Now()
	now := func() time.Time { clockMu.Lock(); defer clockMu.Unlock(); return clock }
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: filepath.Join(dir, "store.json"), ProxyDir: dir, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	waitAutoIdleAtCleanup(t, s)

	// A current proxy that applies new requests only while released; held,
	// it keeps the heartbeat fresh and leaves the request it has not
	// applied waiting.
	var released atomic.Bool
	released.Store(true)
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		last := ""
		for {
			select {
			case <-quit:
				return
			case <-time.After(5 * time.Millisecond):
			}
			if released.Load() {
				if req, ok := fakeProxyAckOnce(dir, last); ok {
					last = req
				}
			}
			fakeProxyWrite(dir, reloadHeartbeatName, "1")
		}
	}()
	t.Cleanup(func() { close(quit); <-done })

	if _, err := s.SetAutoApproveWindow(time.Hour); err != nil {
		t.Fatal(err)
	}
	released.Store(false)
	type autoState struct {
		running bool
		seq     uint64
		problem string
		flag    bool
	}
	state := func(name string) autoState {
		s.mu.Lock()
		defer s.mu.Unlock()
		st := autoState{running: s.autoRunning, seq: s.issuedSeq, problem: s.autoProblem}
		if r := s.recs[name]; r != nil {
			st.flag = r.AutoApprovePending
		}
		return st
	}
	waitUntil := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	seqBefore := state("").seq
	r, err := s.Enroll(EnrollRequest{InstanceID: instanceN(1), Host: "h", Secret: testMacSecret}, "192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != PrincipalPending {
		t.Fatalf("enroll in the window = %+v, want pending", r)
	}
	// The pass has written its proof request and waits for the proxy.
	waitUntil("the pass to wait for its proof", func() bool { st := state(r.Principal); return st.running && st.seq > seqBefore })

	clockMu.Lock()
	clock = clock.Add(2 * time.Hour) // the window ends during the proof
	clockMu.Unlock()
	released.Store(true)
	waitUntil("the pass to finish", func() bool { return !state(r.Principal).running })

	st := state(r.Principal)
	if st.problem != "" {
		t.Fatalf("setup: the proof failed (%q); the pass must reach the window check with a proof", st.problem)
	}
	p := s.Snapshot().byName(r.Principal)
	if p.Status != PrincipalPending || p.AutoApproved {
		t.Fatalf("approved by a window that expired during its proof: %+v", p)
	}
	if p.AutoApprovePending {
		t.Fatal("snapshot shows the expired window's admission as auto-approve pending")
	}
	if !st.flag {
		t.Fatal("setup: the lapsed flag was cleared before the next poll")
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, filepath.Join(dir, proxyPrincipalsName)), r.Principal); ok {
		t.Fatal("a Mac the expired window admitted reached the proxy principals file")
	}

	// The next poll asks for a pass, which hands the Mac to an admin.
	pollMac(t, s, instanceN(1), r)
	waitUntil("the poll's pass to clear the lapsed flag", func() bool { st := state(r.Principal); return !st.running && !st.flag })
	if p := s.Snapshot().byName(r.Principal); p.Status != PrincipalPending {
		t.Fatalf("after the poll: %+v, want pending for an admin", p)
	}
	if err := s.Approve(r.Principal); err != nil {
		t.Fatalf("admin Approve of the handed-over Mac: %v", err)
	}
	if p := s.Snapshot().byName(r.Principal); p.Status != PrincipalApproved || p.AutoApproved {
		t.Fatalf("after admin Approve: %+v, want approved, not auto", p)
	}
}
