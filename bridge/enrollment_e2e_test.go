//go:build h8b_e2e

package main

// Scratch end-to-end check of Mac enrollment against the real Manager and
// the real jm-respproxy over a disposable Redis. Run only with the h8b_e2e tag
// and an environment the caller set up in a scratch directory:
//
//	JM_E2E_MANAGER    Manager base URL (juicemount-manager --listen)
//	JM_E2E_ADMIN_KEY  the Manager's admin key
//	JM_E2E_PROXY      jm-respproxy listen host:port (enforce mode)
//	JM_E2E_PROXY_PID  its PID: the test sends SIGHUP after approval, which is
//	                  what the respproxy entrypoint does on reload.request
//	JM_E2E_JUICEFS    the bundled juicefs binary
//	JM_E2E_DIR        scratch directory (instance identity, sync source)
//	JM_E2E_PROXY_DIR  the Manager's JM_RESPPROXY_DIR. When JM_E2E_ACK_LOOP is
//	                  unset the test acknowledges reload.request there itself
//	                  (reload.ack repeats the request line and
//	                  reload.heartbeat is written, as the entrypoint does),
//	                  because the Manager refuses Approve until the proxy is
//	                  live
//	JM_E2E_ACK_LOOP   optional: set when the entrypoint's own handshake loop
//	                  runs against JM_E2E_PROXY_DIR (it then SIGHUPs the
//	                  proxy and writes reload.ack; the test does neither)
//
// The volume must already be formatted through the proxy. The test enrolls
// through enrollStep (the NFSServerEnrollStep body), shows the proxy refuses
// the principal before approval, approves through the admin API, then writes
// and reads a file through the proxy as the principal with the issued
// secret in META_PASSWORD (never argv). No filesystem is mounted.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/enrollclient"
)

func TestH8BE2EEnrollApproveWriteThroughProxy(t *testing.T) {
	env := func(k string) string {
		v := os.Getenv(k)
		if v == "" {
			t.Skipf("%s not set", k)
		}
		return v
	}
	manager, adminKey, proxy := env("JM_E2E_MANAGER"), env("JM_E2E_ADMIN_KEY"), env("JM_E2E_PROXY")
	juicefs, dir := env("JM_E2E_JUICEFS"), env("JM_E2E_DIR")
	pid, _ := strconv.Atoi(env("JM_E2E_PROXY_PID"))
	db := filepath.Join(dir, "mac", "metadata.db")
	if err := os.MkdirAll(filepath.Dir(db), 0o700); err != nil {
		t.Fatal(err)
	}
	enrollHTTPClient = &http.Client{Timeout: 15 * time.Second}

	step := func(st enrollclient.State) enrollStepOutput {
		in, _ := json.Marshal(enrollStepInput{ManagerURL: manager, DBPath: db, Host: "e2e-mac", Label: "E2E Mac", State: st})
		var out enrollStepOutput
		if err := json.Unmarshal([]byte(enrollStep(string(in))), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	out := step(enrollclient.State{})
	out = step(out.State)
	if out.Phase != enrollclient.Waiting {
		t.Fatalf("enroll: %s %s", out.Phase, out.Message)
	}
	t.Logf("enrolled %s, fingerprint %s, pending", out.State.Principal, out.State.Fingerprint)
	if len(out.State.ManagerID) != 32 {
		t.Fatalf("the enrollment is not bound to the Manager's ID: %q", out.State.ManagerID)
	}
	principal, secret := out.State.Principal, out.State.Secret
	metaURL := "redis://" + principal + "@" + proxy + "/1"

	runJFS := func(extra []string, args ...string) (string, error) {
		for _, a := range args {
			if strings.Contains(a, secret) {
				t.Fatalf("a secret reached juicefs argv: %v", args)
			}
		}
		cmd := exec.Command(juicefs, args...)
		cmd.Env = append([]string{"HOME=" + dir, "PATH=/usr/bin:/bin", "META_PASSWORD=" + secret, "E2EVOL=" + metaURL}, extra...)
		var b bytes.Buffer
		cmd.Stdout, cmd.Stderr = &b, &b
		err := cmd.Run()
		return b.String(), err
	}
	if outText, err := runJFS(nil, "status", metaURL); err == nil {
		t.Fatalf("the proxy accepted a PENDING principal:\n%s", outText)
	} else {
		t.Logf("pending principal refused as expected: %s", lastLine(outText))
	}

	proxyDir := env("JM_E2E_PROXY_DIR")
	ackLoop := os.Getenv("JM_E2E_ACK_LOOP") != ""
	if b, err := os.ReadFile(filepath.Join(proxyDir, "reload.request")); !ackLoop && err == nil && len(strings.Fields(string(b))) > 0 {
		if err := os.WriteFile(filepath.Join(proxyDir, "reload.ack"), b, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(proxyDir, "reload.heartbeat"), []byte("1\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	body, _ := json.Marshal(map[string]string{"name": principal})
	req, _ := http.NewRequest(http.MethodPost, manager+"/api/principals/approve", bytes.NewReader(body))
	req.Header.Set("X-JuiceMount-Admin-Key", adminKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("approve: %v %v", err, resp)
	}
	resp.Body.Close()
	if ackLoop {
		// The approval rewrote principals.json and reload.request; wait for
		// the entrypoint loop to SIGHUP the proxy and echo the request.
		deadline := time.Now().Add(10 * time.Second)
		for {
			req, _ := os.ReadFile(filepath.Join(proxyDir, "reload.request"))
			ack, _ := os.ReadFile(filepath.Join(proxyDir, "reload.ack"))
			if len(req) > 0 && strings.TrimSpace(string(req)) == strings.TrimSpace(string(ack)) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the entrypoint handshake loop did not acknowledge the approval")
			}
			time.Sleep(100 * time.Millisecond)
		}
	} else if err := syscall.Kill(pid, syscall.SIGHUP); err != nil {
		t.Fatalf("SIGHUP proxy: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	out = step(out.State)
	if out.Phase != enrollclient.Approved {
		t.Fatalf("after approval: %s %s", out.Phase, out.Message)
	}
	if out.ObjectAccessKey == "" || out.ObjectSecretKey == "" {
		t.Fatal("approved without an object key (Manager has JM_OBJ_CLIENT_* set)")
	}
	minio := []string{"MINIO_ACCESS_KEY=" + out.ObjectAccessKey, "MINIO_SECRET_KEY=" + out.ObjectSecretKey}
	if text, err := runJFS(minio, "status", metaURL); err != nil {
		t.Fatalf("status as approved principal: %v\n%s", err, text)
	}
	src := filepath.Join(dir, "src", "Project-E2E", "Shoot1")
	if err := os.MkdirAll(src, 0o700); err != nil {
		t.Fatal(err)
	}
	payload := []byte("written by " + principal + " through the proxy\n")
	if err := os.WriteFile(filepath.Join(src, "clip.txt"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if text, err := runJFS(minio, "sync", filepath.Join(dir, "src")+"/", "jfs://E2EVOL/"); err != nil {
		t.Fatalf("write through proxy as %s: %v\n%s", principal, err, text)
	}
	back := filepath.Join(dir, "back")
	if text, err := runJFS(minio, "sync", "jfs://E2EVOL/", back+"/"); err != nil {
		t.Fatalf("read back: %v\n%s", err, text)
	}
	got, err := os.ReadFile(filepath.Join(back, "Project-E2E", "Shoot1", "clip.txt"))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("read back %q, %v", got, err)
	}
	t.Logf("approved principal %s wrote and read Project-E2E/Shoot1/clip.txt through the proxy", principal)
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}
