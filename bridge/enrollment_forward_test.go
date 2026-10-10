//go:build serviceclient

package main

// Review 2 finding F3, desktop archive: the app process holds no Link node
// and no volume owner, so the approved-step login check runs in the serving
// process, which holds the mount's route.

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/servicecontrol"
)

// Bridge tests in the desktop (serviceclient) build never reach a real
// serving process: the default socket path is the user's live JuiceMount
// service. A test that needs a serving process stubs servingSocketPath (and
// callServingProcess) itself.
func init() {
	servingSocketPath = func() (string, error) {
		return "", errors.New("bridge tests never contact the user's serving process")
	}
}

func TestH8BDesktopLoginCheckRunsInServingProcess(t *testing.T) {
	installFakeLink(t, nil)
	var got []servicecontrol.Request
	installControlFake(t, func(_ context.Context, _ string, req servicecontrol.Request) (servicecontrol.Response, error) {
		got = append(got, req)
		return servicecontrol.Response{Version: 1, Text: `{"live":true,"route":"owner-link"}`}, nil
	})
	const principal, secret = "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"
	// The configured NAS address is unreachable from here: only the serving
	// process's answer can make this live.
	if ok, why := verifyPrincipalLogin(routeNASRedis, principal, secret); !ok {
		t.Fatalf("forwarded check: %s", why)
	}
	if len(got) != 1 || got[0].Method != "enroll-verify" || len(got[0].Strings) != 3 ||
		got[0].Strings[0] != routeNASRedis || got[0].Strings[1] != principal || got[0].Strings[2] != secret {
		t.Fatalf("forwarded request = %+v", got)
	}

	// No serving process (the control socket cannot be dialled, so nothing
	// was sent): the app checks over macOS networking itself.
	installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
		return servicecontrol.Response{}, &net.OpError{Op: "dial", Net: "unix", Err: syscall.ECONNREFUSED}
	})
	direct := newFakeRESP(t, principal, secret)
	if ok, why := verifyPrincipalLogin("redis://"+direct.ln.Addr().String()+"/1", principal, secret); !ok {
		t.Fatalf("fallback check over macOS networking: %s", why)
	}
}

// Review 3 finding 1: once the request reached the serving process, a lost
// or late answer is a failed check. The app never repeats the check over
// macOS networking (which would send the secret a second time, by another
// route) and says it checks again.
func TestH8BDesktopLoginCheckNoFallbackAfterSend(t *testing.T) {
	installFakeLink(t, nil)
	const principal, secret = "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa"
	direct := newFakeRESP(t, principal, secret)
	for _, sendErr := range []error{
		context.DeadlineExceeded,
		&net.OpError{Op: "read", Net: "unix", Err: errors.New("i/o timeout")},
		errors.New("service control response version mismatch"),
	} {
		installControlFake(t, func(context.Context, string, servicecontrol.Request) (servicecontrol.Response, error) {
			return servicecontrol.Response{}, sendErr
		})
		ok, why := verifyPrincipalLogin("redis://"+direct.ln.Addr().String()+"/1", principal, secret)
		if ok || !strings.Contains(why, "did not answer") {
			t.Fatalf("%v after send: ok=%v %q", sendErr, ok, why)
		}
	}
	direct.mu.Lock()
	defer direct.mu.Unlock()
	if len(direct.authUsers) != 0 {
		t.Fatalf("the app repeated the check over macOS networking after the request was sent: %v", direct.authUsers)
	}
}

// Review 3 finding 1, over the real control socket: the serving process
// answers live after 2.5 s (a Link check on a slow or cold link; the check
// itself is bounded at 5 s). The app must wait for that answer.
func TestH8BDesktopLoginCheckWaitsForSlowServingProcess(t *testing.T) {
	installFakeLink(t, nil)
	dir, err := os.MkdirTemp(os.Getenv("TMPDIR"), "sc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	_ = os.Chmod(dir, 0o700)
	sock := filepath.Join(dir, "c.sock")
	srv, err := servicecontrol.Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(func(req servicecontrol.Request) servicecontrol.Response {
		time.Sleep(2500 * time.Millisecond)
		b, _ := json.Marshal(enrollVerifyResult{Live: true, Route: "link"})
		return servicecontrol.Response{Version: 1, Text: string(b)}
	})
	t.Cleanup(func() { srv.Close() })
	oldCall, oldPath := callServingProcess, servingSocketPath
	callServingProcess, servingSocketPath = servicecontrol.Call, func() (string, error) { return sock, nil }
	t.Cleanup(func() { callServingProcess, servingSocketPath = oldCall, oldPath })

	start := time.Now()
	ok, why := verifyPrincipalLogin(routeNASRedis, "mac-4f1c2a9e7b3d", "principal-check-fixture-aaaaaaaaaaaaaaaaaaaaaa")
	if !ok {
		t.Fatalf("the serving process answered live after 2.5 s; the app reported %q after %v", why, time.Since(start).Round(time.Millisecond))
	}
}
