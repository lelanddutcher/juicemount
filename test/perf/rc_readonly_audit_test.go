package perf

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRCReadOnlyAuditRemainsNonMutating(t *testing.T) {
	body, err := os.ReadFile("../../scripts/rc-readonly-audit.sh")
	if err != nil {
		t.Fatal(err)
	}
	script := string(body)
	for _, required := range []string{
		"BatchMode=yes",
		"PasswordAuthentication=no",
		"KbdInteractiveAuthentication=no",
		"juicefarm:storage:permit",
		"juicefarm:queue:proxy:cpu",
		"zpool list -H -p -o health,free,cap zSSD",
		"RESULT NO-SHIP",
	} {
		if !strings.Contains(script, required) {
			t.Fatalf("read-only audit is missing %q", required)
		}
	}
	for _, forbidden := range []string{
		"docker start",
		"docker restart",
		"docker stop",
		"systemctl enable",
		"systemctl restart",
		"systemctl start",
		"docker rm",
		"docker run",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("read-only audit contains mutating command %q", forbidden)
		}
	}
}

func TestRCReadOnlyAuditBindsImagesToFrozenSource(t *testing.T) {
	original, err := os.ReadFile("../../scripts/rc-readonly-audit.sh")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "scripts"), 0755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(root, "scripts", "rc-readonly-audit.sh")
	if err := os.WriteFile(script, original, 0755); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	git("add", "scripts/rc-readonly-audit.sh")
	git("-c", "user.name=RC Test", "-c", "user.email=rc-test@example.invalid", "commit", "-qm", "frozen candidate")
	commit := git("rev-parse", "HEAD")

	bin := t.TempDir()
	ssh := `#!/usr/bin/env bash
command="${@: -1}"
case "$command" in
  *"docker image inspect"*) printf '%s\n' "$JM_TEST_IMAGE_COMMIT" ;;
  *"docker ps --format"*) printf '%s\n' "${JM_TEST_CPU_NAMES:-ix-juicemount-juicefarm-1}" ;;
  *"docker inspect -f '{{range .Config.Env}}"*) printf '%s\n' "JM_NET_HEADSCALE=${JM_TEST_HEADSCALE:-off}" 'JM_NET_NAS_NODE=off' 'JM_NET_ALLOW_INSECURE=0' 'JM_NET_ALLOW_LAN_ONLY=0' ;;
  *"docker inspect -f '{{.Config.Image}}"*)
    case "$command" in
      *"ix-juicemount-juicemount-manager-1"*) digest="$JM_EXPECTED_MANAGER" ;;
      *"ix-juicemount-juicefarm-1"*) digest="$JM_EXPECTED_CPU" ;;
      *"juicefarm-gpu-rc0.5"*) digest="$JM_EXPECTED_GPU" ;;
    esac
    printf 'image@%s|running|sha256:%064d\n' "$digest" 1 ;;
  *"systemctl is-enabled"*) printf '%s\n' enabled ;;
  *"systemctl is-active"*) printf '%s\n' active ;;
  *"LLEN juicefarm:queue"*) printf '0\n0\n0\n' ;;
  *"juicefarm:storage:permit"*) printf '%s\n' '{"available_bytes":1000000000000}' 'ONLINE 1000000000000 10' ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "ssh"), []byte(ssh), 0755); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	run := func(imageCommit string, extraEnv ...string) (string, error) {
		t.Helper()
		cmd := exec.Command("bash", script)
		cmd.Env = append(os.Environ(),
			"PATH="+bin+":"+os.Getenv("PATH"),
			"JM_EXPECTED_MANAGER="+digest,
			"JM_EXPECTED_CPU="+digest,
			"JM_EXPECTED_GPU="+digest,
			"JM_TEST_IMAGE_COMMIT="+imageCommit,
		)
		cmd.Env = append(cmd.Env, extraEnv...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := run(commit); err != nil || !strings.Contains(out, "RESULT PASS") {
		t.Fatalf("frozen source and matching images: err=%v output=%s", err, out)
	}
	if out, err := run(strings.Repeat("0", 40)); err == nil || !strings.Contains(out, "RESULT NO-SHIP") {
		t.Fatalf("stale image revision passed: err=%v output=%s", err, out)
	}
	if out, err := run(commit, "JM_TEST_HEADSCALE=on"); err == nil || !strings.Contains(out, "remote posture differs") {
		t.Fatalf("embedded Link posture passed: err=%v output=%s", err, out)
	}
	if out, err := run(commit, "JM_TEST_CPU_NAMES=ix-juicemount-juicefarm-1\njuicefarm-worker-rc0.5-other"); err == nil || !strings.Contains(out, "exactly one active CPU worker") {
		t.Fatalf("duplicate CPU workers passed: err=%v output=%s", err, out)
	}
	if err := os.Mkdir(filepath.Join(root, "cmd"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cmd", "untracked.go"), []byte("package main\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if out, err := run(commit); err == nil || !strings.Contains(out, "untracked build inputs") {
		t.Fatalf("untracked source passed: err=%v output=%s", err, out)
	}
}
