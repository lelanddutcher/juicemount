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
	"time"
)

// mountNowTestMu serializes tests that swap the package-level
// mountNowDeps / globalWantMountPoint state.
var mountNowTestMu sync.Mutex

const mountNowTestExportPath = "/jm-0123456789abcdef0123456789abcdef"

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
	savedFlights := globalMountFlights
	globalWantMountPoint = wantMount
	globalMountPath = ""
	globalMountFlights = newNFSMountFlights()
	globalMu.Unlock()
	defer func() {
		mountNowDeps = savedDeps
		globalMu.Lock()
		globalWantMountPoint = savedWant
		globalMountPath = savedCur
		globalMountFlights = savedFlights
		globalMu.Unlock()
	}()

	mountNowDeps.isMounted = isMounted
	mountNowDeps.mount = func(addr, exportPath, mp string) error {
		if exportPath != mountNowTestExportPath {
			t.Errorf("mount export path = %q, want server path %q", exportPath, mountNowTestExportPath)
		}
		return mount(addr, mp)
	}
	mountNowDeps.server = func() (string, string, bool) {
		addr, running := serverAddr()
		return addr, mountNowTestExportPath, running
	}
	fn(t)
}

func doMountNow(t *testing.T, target string) (int, mountNowResp) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, target, nil)
	req.Header.Set(localControlHeader, "1")
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

func TestMountNowRejectsUnavailableExportBeforeInspectOrMount(t *testing.T) {
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { t.Fatal("isMounted must not be called"); return false },
		func(string, string) error { t.Fatal("mount must not be called"); return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			for _, path := range []string{"", "/", "/jm-0123456789abcdef0123456789abcdeG"} {
				mountNowDeps.server = func() (string, string, bool) {
					return "127.0.0.1:11049", path, true
				}
				code, body := doMountNow(t, "/mount-now")
				if code != http.StatusServiceUnavailable || body.OK || body.Error == "" {
					t.Fatalf("path %q: code=%d body=%+v, want unavailable", path, code, body)
				}
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

func TestMountNowRejectsDifferentPath(t *testing.T) {
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { t.Fatal("unconfigured path was inspected"); return false },
		func(_, _ string) error { t.Fatal("unconfigured path was mounted"); return nil },
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			code, body := doMountNow(t, "/mount-now?path=/Volumes/other")
			if code != http.StatusBadRequest || body.OK {
				t.Fatalf("code=%d body=%+v, want 400 ok:false", code, body)
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

func TestShutdownRetainsStoresUntilBlockedMountPublishes(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	withMountNowEnv(t, "/Volumes/test-zpool",
		func(string) bool { return false },
		func(string, string) error {
			close(entered)
			<-release
			return nil
		},
		func() (string, bool) { return "127.0.0.1:11049", true },
		func(t *testing.T) {
			spool, drainer := shutdownTestDrainer(t)
			publishShutdownTestDrainer(t, spool, drainer, nil)
			requestDone := make(chan struct{})
			go func() {
				defer close(requestDone)
				req := httptest.NewRequest(http.MethodPost, "/mount-now", nil)
				req.Header.Set(localControlHeader, "1")
				handleMountNowHTTP(httptest.NewRecorder(), req)
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("mount did not enter blocked section")
			}
			if stopServerSerializedWithDeadline(20 * time.Millisecond) {
				t.Fatal("shutdown completed while mount was in flight")
			}
			globalMu.Lock()
			retained := globalSpool == spool && globalDrainer == drainer
			flights := globalMountFlights
			globalMu.Unlock()
			if !retained {
				t.Fatal("shutdown closed stores while mount was in flight")
			}
			if flights.begin() {
				flights.end()
				t.Fatal("retired lifecycle admitted a second mount")
			}
			close(release)
			select {
			case <-requestDone:
			case <-time.After(time.Second):
				t.Fatal("mount did not complete after release")
			}
			globalMu.Lock()
			published := globalMountPath
			globalMu.Unlock()
			if published != "/Volumes/test-zpool" {
				t.Fatalf("late mount was not retained for unmount retry: %q", published)
			}
			if stopServerSerializedWithDeadline(20 * time.Millisecond) {
				t.Fatal("shutdown closed stores with a published mount still live")
			}
		})
}
