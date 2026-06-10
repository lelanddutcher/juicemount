package main

// Tests for the /mount-now handler (LB-2 "Mount Now"). The handler's
// side effects are behind mountNowDeps so every branch is exercised
// without a live NFS server or exec'ing mount(8).

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// mountNowTestMu serializes tests that swap the package-level
// mountNowDeps / globalWantMountPoint state.
var mountNowTestMu sync.Mutex

type mountNowResp struct {
	OK             bool   `json:"ok"`
	MountPoint     string `json:"mount_point"`
	AlreadyMounted bool   `json:"already_mounted"`
	Error          string `json:"error"`
}

func withMountNowEnv(t *testing.T, wantMount string, isMounted func(string) bool,
	mount func(string, string) error, serverAddr func() (string, bool),
	fn func(t *testing.T)) {
	t.Helper()
	mountNowTestMu.Lock()
	defer mountNowTestMu.Unlock()

	savedDeps := mountNowDeps
	globalMu.Lock()
	savedWant := globalWantMountPoint
	savedCur := globalMountPath
	globalWantMountPoint = wantMount
	globalMountPath = ""
	globalMu.Unlock()
	defer func() {
		mountNowDeps = savedDeps
		globalMu.Lock()
		globalWantMountPoint = savedWant
		globalMountPath = savedCur
		globalMu.Unlock()
	}()

	mountNowDeps.isMounted = isMounted
	mountNowDeps.mount = mount
	mountNowDeps.serverAddr = serverAddr
	fn(t)
}

func doMountNow(t *testing.T, target string) (int, mountNowResp) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	handleMountNowHTTP(rec, req)
	var body mountNowResp
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not valid JSON: %v\nbody: %s", err, rec.Body.String())
	}
	return rec.Code, body
}

func TestMountNowServerNotRunning(t *testing.T) {
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { t.Fatal("isMounted must not be called"); return false },
		func(string, string) error { t.Fatal("mount must not be called"); return nil },
		func() (string, bool) { return "", false },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusServiceUnavailable {
				t.Fatalf("code = %d, want 503", code)
			}
			if body.OK || body.Error == "" {
				t.Fatalf("body = %+v, want ok:false with error", body)
			}
		})
}

func TestMountNowNoMountPointConfigured(t *testing.T) {
	withMountNowEnv(t, "", // no configured mount point
		func(string) bool { return false },
		func(string, string) error { return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusBadRequest {
				t.Fatalf("code = %d, want 400", code)
			}
			if body.OK {
				t.Fatalf("body = %+v, want ok:false", body)
			}
		})
}

func TestMountNowAlreadyMountedIsNoOpSuccess(t *testing.T) {
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(mp string) bool { return mp == "/Volumes/test-zpool" },
		func(string, string) error { t.Fatal("mount must not be called when already mounted"); return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusOK {
				t.Fatalf("code = %d, want 200", code)
			}
			if !body.OK || !body.AlreadyMounted || body.MountPoint != "/Volumes/test-zpool" {
				t.Fatalf("body = %+v, want ok:true already_mounted:true", body)
			}
		})
}

func TestMountNowMountsAndRecordsMountPath(t *testing.T) {
	var gotAddr, gotMP string
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { return false },
		func(addr, mp string) error { gotAddr, gotMP = addr, mp; return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusOK {
				t.Fatalf("code = %d, want 200", code)
			}
			if !body.OK || body.AlreadyMounted {
				t.Fatalf("body = %+v, want ok:true already_mounted:false", body)
			}
			if gotAddr != "127.0.0.1:11049" || gotMP != "/Volumes/test-zpool" {
				t.Fatalf("mount called with (%q, %q)", gotAddr, gotMP)
			}
			globalMu.Lock()
			recorded := globalMountPath
			globalMu.Unlock()
			if recorded != "/Volumes/test-zpool" {
				t.Fatalf("globalMountPath = %q, want recorded mount", recorded)
			}
		})
}

func TestMountNowQueryPathOverride(t *testing.T) {
	var gotMP string
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { return false },
		func(_, mp string) error { gotMP = mp; return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now?path=/Volumes/other")
			if code != http.StatusOK || !body.OK {
				t.Fatalf("code=%d body=%+v, want 200 ok:true", code, body)
			}
			if gotMP != "/Volumes/other" {
				t.Fatalf("mount called with %q, want query override", gotMP)
			}
		})
}

func TestMountNowFailureReportsError(t *testing.T) {
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { return false }, // not mounted before OR after
		func(string, string) error { return errors.New("osascript: user cancelled") },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusInternalServerError {
				t.Fatalf("code = %d, want 500", code)
			}
			if body.OK || body.Error == "" {
				t.Fatalf("body = %+v, want ok:false with error", body)
			}
		})
}

func TestMountNowSingleFlight(t *testing.T) {
	// While one /mount-now is parked inside mount() (e.g. on the macOS
	// admin-password prompt), a concurrent call must be rejected with 409
	// — NOT stack a second password prompt. After the first completes,
	// the gate must release so a later call goes through again.
	entered := make(chan struct{}) // closed when mount() is reached
	release := make(chan struct{}) // closed to let mount() return
	mountCalls := 0
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { return false },
		func(string, string) error {
			mountCalls++
			if mountCalls == 1 {
				close(entered)
				<-release
			}
			return nil
		},
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			type result struct {
				code int
				body mountNowResp
			}
			firstDone := make(chan result, 1)
			go func() {
				code, body := doMountNow(t, "/mount-now")
				firstDone <- result{code, body}
			}()
			<-entered // first request is now blocked inside mount()

			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusConflict {
				t.Fatalf("concurrent call: code = %d, want 409", code)
			}
			if body.OK || body.Error != "mount already in flight" {
				t.Fatalf("concurrent call: body = %+v, want ok:false error:\"mount already in flight\"", body)
			}

			close(release)
			first := <-firstDone
			if first.code != http.StatusOK || !first.body.OK {
				t.Fatalf("first call: code=%d body=%+v, want 200 ok:true", first.code, first.body)
			}

			// Gate released: a fresh call must be allowed through again.
			// isMounted still reports false, so it reaches mount() (call #2).
			code, body = doMountNow(t, "/mount-now")
			if code != http.StatusOK || !body.OK {
				t.Fatalf("post-release call: code=%d body=%+v, want 200 ok:true", code, body)
			}
			if mountCalls != 2 {
				t.Fatalf("mount() called %d times, want 2 (blocked first + post-release)", mountCalls)
			}
		})
}

func TestMountNowLostRaceStillSuccess(t *testing.T) {
	// mount() fails, but by the time we re-check, the volume IS mounted
	// (concurrent auto-remount won the race) — that's success.
	calls := 0
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { calls++; return calls > 1 }, // false first, true on re-check
		func(string, string) error { return errors.New("already mounted (umount it first)") },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now")
			if code != http.StatusOK {
				t.Fatalf("code = %d, want 200", code)
			}
			if !body.OK || !body.AlreadyMounted {
				t.Fatalf("body = %+v, want ok:true already_mounted:true", body)
			}
		})
}
