package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runFarmEnrollment(t *testing.T, a *API, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/farm/enrollment", strings.NewReader(body))
	rec := httptest.NewRecorder()
	a.handleFarmEnrollment(rec, req)
	return rec
}

func TestFarmEnrollmentFailsClosedWithoutExplicitNodeEndpoint(t *testing.T) {
	a := &API{farmQ: &fakeFarmQ{}}
	rec := runFarmEnrollment(t, a, `{"name":"render-a","role":"render"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("Cache-Control = %q", got)
	}
}

func TestFarmEnrollmentBuildsPersistentExactRolePlan(t *testing.T) {
	a := &API{
		farmQ:           &fakeFarmQ{},
		farmNodeMetaURL: "rediss://worker-user@nas.example:6379/1",
		farmRenderImage: "registry.example/juicefarm-gpu:rc-test",
	}
	rec := runFarmEnrollment(t, a, `{
		"name":"render-a",
		"role":"render",
		"state_path":"/srv/juice farm/render-a/state",
		"cache_path":"/srv/juice farm/render-a/cache"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		OK   bool               `json:"ok"`
		Plan farmEnrollmentPlan `json:"plan"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Plan.Role != "render" || response.Plan.Image != a.farmRenderImage {
		t.Fatalf("plan identity = %+v", response.Plan)
	}
	for _, marker := range []string{
		"--restart unless-stopped", "--device /dev/fuse", "--device /dev/dri",
		"JM_WORKER_NAME=render-a", "JM_WORKER_ROLE=render",
		"JM_FARM_KINDS=derivatives,proxy,transcript", "JM_FARM_VCODEC=hevc_vaapi",
		"'/srv/juice farm/render-a/state:/state'", "'/srv/juice farm/render-a/cache:/jfs-cache'",
	} {
		if !strings.Contains(response.Plan.DockerCommand, marker) {
			t.Errorf("docker plan missing %q", marker)
		}
	}
	for _, marker := range []string{
		"Description=JuiceFarm RC worker (render)",
		"Requires=docker.service", "docker wait juicefarm-render-a",
		"docker start juicefarm-render-a",
		"ExecStop=/usr/bin/docker stop --time=30 juicefarm-render-a",
		"Restart=on-failure", "WantedBy=multi-user.target",
	} {
		if !strings.Contains(response.Plan.RecoveryUnit, marker) {
			t.Errorf("recovery unit missing %q", marker)
		}
	}
	for _, marker := range []string{
		"cat <<'UNIT' | sudo tee /etc/systemd/system/juicefarm-render-a.service",
		"sudo systemctl daemon-reload",
		"sudo systemctl enable --now juicefarm-render-a.service",
		"[Install]",
	} {
		if !strings.Contains(response.Plan.RecoveryCommand, marker) {
			t.Errorf("recovery install command missing %q", marker)
		}
	}
	if response.Plan.LinkCommand != "" {
		t.Fatal("local enrollment unexpectedly minted Link credentials")
	}
}

func TestFarmEnrollmentRejectsUnsafeOrAmbiguousInputs(t *testing.T) {
	a := &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://farm@nas.example:6379/1"}
	for _, body := range []string{
		`{"name":"../render","role":"render"}`,
		`{"name":"render-a","role":"gpu-ish"}`,
		`{"name":"render-a","role":"render","state_path":"relative","cache_path":"/cache"}`,
		`{"name":"render-a","role":"render","state_path":"/same","cache_path":"/same"}`,
		`{"name":"render-a","role":"render","state_path":"/","cache_path":"/cache"}`,
	} {
		rec := runFarmEnrollment(t, a, body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q status = %d: %s", body, rec.Code, rec.Body.String())
		}
	}
}

func TestReleaseGPURecoveryIdentity(t *testing.T) {
	name := "gpu-rc0.5"
	if !validWorkerName(name) {
		t.Fatalf("release GPU worker name %q is invalid", name)
	}
	unit := farmWorkerRecoveryUnit(name, "render")
	command := farmWorkerRecoveryInstallCommand(name, unit)
	for _, want := range []string{
		"docker wait juicefarm-gpu-rc0.5",
		"docker start juicefarm-gpu-rc0.5",
	} {
		if !strings.Contains(unit, want) {
			t.Errorf("recovery unit missing %q", want)
		}
	}
	if strings.Contains(unit, "--sig-proxy") {
		t.Fatal("recovery unit uses unsupported docker start flag")
	}
	guide, err := os.ReadFile("../../server/juicefarm/REDEPLOY_BINARY_ONLY.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, "ExecStart=") && !strings.Contains(string(guide), line) {
			t.Fatal("GPU recovery guide does not match generated ExecStart")
		}
	}
	for _, want := range []string{
		"/etc/systemd/system/juicefarm-gpu-rc0.5.service",
		"systemctl enable --now juicefarm-gpu-rc0.5.service",
	} {
		if !strings.Contains(command, want) {
			t.Errorf("recovery command missing %q", want)
		}
	}
}

func TestFarmWorkerRecoveryUnitSupervisesContainerExit(t *testing.T) {
	const name = "juicefarm-gpu-rc0.5"
	unit := farmWorkerRecoveryUnit("gpu-rc0.5", "render")
	const prefix = "ExecStart=:/bin/sh -c '"
	var script string
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, prefix) && strings.HasSuffix(line, "'") {
			script = strings.TrimSuffix(strings.TrimPrefix(line, prefix), "'")
		}
	}
	if script == "" || !strings.Contains(unit, "Restart=on-failure") ||
		!strings.Contains(unit, "ExecStop=/usr/bin/docker stop --time=30 "+name) {
		t.Fatal("recovery unit lacks executable supervisor or stop/restart policy")
	}

	for _, tc := range []struct {
		name           string
		initialRunning bool
		operatorStop   bool
		wantExit       int
	}{
		{"stopped container, normal stop", false, true, 0},
		{"already running, normal stop", true, true, 0},
		{"stopped container, abnormal exit", false, false, 17},
		{"already running, abnormal exit", true, false, 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stub := filepath.Join(dir, "docker")
			stubScript := `#!/bin/sh
set -eu
dir="$DOCKER_STUB_DIR"
printf '%s\n' "$*" >> "$dir/calls"
cmd="$1"
shift
case "$cmd" in
  inspect)
    [ "$#" -eq 3 ] && [ "$1" = -f ] && [ "$2" = '{{.State.Running}}' ] && [ "$3" = 'juicefarm-gpu-rc0.5' ] || exit 22
    cat "$dir/running" ;;
  start)
    [ "$#" -eq 1 ] && [ "$1" = 'juicefarm-gpu-rc0.5' ] && [ "$(cat "$dir/running")" = false ] || exit 22
    printf 'true\n' > "$dir/running" ;;
  wait)
    [ "$#" -eq 1 ] && [ "$1" = 'juicefarm-gpu-rc0.5' ] || exit 22
    touch "$dir/waiting"
    while [ "$(cat "$dir/running")" = true ]; do sleep 0.01; done
    cat "$dir/exit_code" ;;
  stop)
    [ "$#" -eq 2 ] && [ "$1" = '--time=30' ] && [ "$2" = 'juicefarm-gpu-rc0.5' ] || exit 22
    printf '0\n' > "$dir/exit_code"
    printf 'false\n' > "$dir/running" ;;
  *) exit 22 ;;
esac
`
			if err := os.WriteFile(stub, []byte(stubScript), 0700); err != nil {
				t.Fatal(err)
			}
			initial := "false\n"
			if tc.initialRunning {
				initial = "true\n"
			}
			if err := os.WriteFile(filepath.Join(dir, "running"), []byte(initial), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "exit_code"), []byte("0\n"), 0600); err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			var output bytes.Buffer
			cmd := exec.CommandContext(ctx, "/bin/sh", "-c", strings.ReplaceAll(script, "/usr/bin/docker", shellQuote(stub)))
			cmd.Env = append(os.Environ(), "DOCKER_STUB_DIR="+dir)
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waiting := filepath.Join(dir, "waiting")
			for {
				if _, err := os.Stat(waiting); err == nil {
					break
				}
				if ctx.Err() != nil {
					_ = cmd.Wait()
					t.Fatalf("supervisor never entered docker wait: %s", output.String())
				}
				time.Sleep(10 * time.Millisecond)
			}
			if tc.operatorStop {
				stop := exec.Command(stub, "stop", "--time=30", name)
				stop.Env = append(os.Environ(), "DOCKER_STUB_DIR="+dir)
				if out, err := stop.CombinedOutput(); err != nil {
					t.Fatalf("operator stop: %v: %s", err, out)
				}
			} else {
				if err := os.WriteFile(filepath.Join(dir, "exit_code"), []byte("17\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "running"), []byte("false\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			err := cmd.Wait()
			gotExit := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("supervisor: %v: %s", err, output.String())
				}
				gotExit = exitErr.ExitCode()
			}
			if gotExit != tc.wantExit || ctx.Err() != nil {
				t.Fatalf("supervisor exit = %d (want %d), context = %v: %s", gotExit, tc.wantExit, ctx.Err(), output.String())
			}
			calls, err := os.ReadFile(filepath.Join(dir, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			got := string(calls)
			if strings.Contains(got, "start "+name) == tc.initialRunning ||
				!strings.Contains(got, "wait "+name) ||
				strings.Contains(got, "--sig-proxy") {
				t.Fatalf("unexpected docker calls: %s", got)
			}
		})
	}
}

func TestRemoteFarmEnrollmentRejectsLANOnlyLink(t *testing.T) {
	t.Setenv("JM_NET_HEADSCALE", "on")
	t.Setenv("JM_NET_SERVER_URL", "http://192.168.0.197:30193")
	t.Setenv("JM_NET_ALLOW_INSECURE", "1")
	t.Setenv("JM_NET_ALLOW_LAN_ONLY", "0")
	a := &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://farm@nas.example:6379/1"}
	rec := runFarmEnrollment(t, a, `{"name":"render-a","role":"render","remote":true}`)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "not remote-ready") {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestShellJoinQuotesMetacharacters(t *testing.T) {
	got := shellJoin([]string{"docker", "-e", "TOKEN=a b'c", "image;bad"})
	if got != `docker -e 'TOKEN=a b'"'"'c' 'image;bad'` {
		t.Fatalf("shellJoin = %q", got)
	}
}

// TestH8BFarmEnrollmentKeepsSecretOutOfCommand: the generated node command
// names the farm principal and passes its secret by NAME only
// (`-e JM_META_PASSWORD`, copied from the operator's shell), and a
// JM_FARM_NODE_META_URL with a password is refused outright, so the secret
// never reaches shell history, the node's argv, or this response.
func TestH8BFarmEnrollmentKeepsSecretOutOfCommand(t *testing.T) {
	body := `{"name":"gpu-a","role":"render","state_path":"/srv/jf/state","cache_path":"/srv/jf/cache"}`
	for _, bad := range []string{
		"redis://farm:hunter2-not-a-secret@nas.example:30179/1",
		"redis://:hunter2-not-a-secret@nas.example:30179/1",
		"redis://nas.example:30179/1", // no principal: AUTH_REQUIRED loop on the node
	} {
		a := &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: bad}
		rec := runFarmEnrollment(t, a, body)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("JM_FARM_NODE_META_URL %q: status %d, want 503", bad, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "hunter2") {
			t.Fatalf("refusal echoes the password: %s", rec.Body.String())
		}
	}
	a := &API{farmQ: &fakeFarmQ{}, farmNodeMetaURL: "redis://farm@nas.example:30179/1"}
	rec := runFarmEnrollment(t, a, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Plan farmEnrollmentPlan `json:"plan"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	cmd := response.Plan.DockerCommand
	if !strings.Contains(cmd, "-e JM_META=redis://farm@nas.example:30179/1 ") {
		t.Fatalf("docker command lacks the user@host JM_META: %s", cmd)
	}
	if !strings.Contains(cmd, "-e JM_META_PASSWORD ") || strings.Contains(cmd, "JM_META_PASSWORD=") {
		t.Fatalf("docker command must pass JM_META_PASSWORD by name only: %s", cmd)
	}
	if !strings.Contains(strings.Join(response.Plan.Checks, "\n"), "read -rs JM_META_PASSWORD") {
		t.Fatal("plan does not tell the operator how to supply the secret without echo")
	}
	// H-8B §2.1: the node's object key (jm-client) also goes by name only.
	for _, name := range []string{"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"} {
		if !strings.Contains(cmd, "-e "+name+" ") || strings.Contains(cmd, name+"=") {
			t.Fatalf("docker command must pass %s by name only: %s", name, cmd)
		}
	}
	if !strings.Contains(strings.Join(response.Plan.Checks, "\n"), "read -rs MINIO_SECRET_KEY") {
		t.Fatal("plan does not tell the operator how to supply the object secret without echo")
	}
}
