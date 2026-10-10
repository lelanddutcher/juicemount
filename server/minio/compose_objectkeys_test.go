package miniodeploy

// Compose and step-B tooling contract for the H-8B object keys: `setting`
// never receives a key, each server client gets its own key in its
// environment (bg mount and Manager: jm-gc; farm and WebDAV: jm-client),
// and no object secret is ever on a command line.

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// service returns one top-level compose service block, comments removed.
func service(t *testing.T, name string) string {
	t.Helper()
	lines := strings.Split(mustRead(t, "../docker-compose.yml"), "\n")
	start := -1
	for i, l := range lines {
		if l == "  "+name+":" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("compose has no service %q", name)
	}
	var out []string
	for _, l := range lines[start+1:] {
		trim := strings.TrimSpace(l)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		if !strings.HasPrefix(l, "   ") {
			break
		}
		if i := strings.Index(l, " #"); i >= 0 && !strings.Contains(l[:i], `"`) {
			l = l[:i]
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

func TestComposeSettingNeverGetsAnObjectKey(t *testing.T) {
	compose := mustRead(t, "../docker-compose.yml")
	for _, flag := range []string{"--access-key", "--secret-key"} {
		for _, l := range strings.Split(compose, "\n") {
			if strings.Contains(l, flag) && !strings.HasPrefix(strings.TrimSpace(l), "#") {
				t.Errorf("compose passes %s (it would store a key in `setting`, and a secret on argv): %s", flag, strings.TrimSpace(l))
			}
		}
	}
	initBlock := service(t, "juicefs-init")
	if !strings.Contains(initBlock, "juicefs format") {
		t.Fatal("juicefs-init no longer formats")
	}
	if strings.Contains(initBlock, "MINIO_ROOT") {
		t.Error("juicefs-init still receives the MinIO root credential")
	}
	if !strings.Contains(initBlock, `MINIO_ACCESS_KEY: "${JM_OBJ_GC_ACCESS_KEY:-jm-gc}"`) ||
		!strings.Contains(initBlock, `MINIO_SECRET_KEY: "${JM_OBJ_GC_SECRET_KEY:?`) {
		t.Error("juicefs-init's format storage test must use jm-gc from the environment")
	}
	if !strings.Contains(initBlock, "minio-keys:\n        condition: service_completed_successfully") {
		t.Error("juicefs-init must wait for the object users to exist")
	}
}

func TestComposeBgMountGetsGCKeyWebdavGetsClientKey(t *testing.T) {
	block := service(t, "juicefs")
	for _, want := range []string{
		`JM_OBJ_GC_SECRET_KEY: "${JM_OBJ_GC_SECRET_KEY:?`,
		`JM_OBJ_CLIENT_SECRET_KEY: "${JM_OBJ_CLIENT_SECRET_KEY:?`,
		"MINIO_ACCESS_KEY=\"$$JM_OBJ_GC_ACCESS_KEY\" MINIO_SECRET_KEY=\"$$JM_OBJ_GC_SECRET_KEY\" juicefs mount",
		"MINIO_ACCESS_KEY=\"$$JM_OBJ_CLIENT_ACCESS_KEY\" MINIO_SECRET_KEY=\"$$JM_OBJ_CLIENT_SECRET_KEY\" juicefs webdav",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("juicefs service lacks %q", want)
		}
	}
}

func TestComposeManagerGetsGCKeyFarmGetsClientKey(t *testing.T) {
	mgr := service(t, "juicemount-manager")
	for _, want := range []string{
		`JM_OBJ_GC_ACCESS_KEY: "${JM_OBJ_GC_ACCESS_KEY:-jm-gc}"`,
		`JM_OBJ_GC_SECRET_KEY: "${JM_OBJ_GC_SECRET_KEY:?`,
		`JM_OBJ_CLIENT_SECRET_KEY: "${JM_OBJ_CLIENT_SECRET_KEY:?`, // handed to approved Macs
	} {
		if !strings.Contains(mgr, want) {
			t.Errorf("manager lacks %q", want)
		}
	}
	// The Manager adds MINIO_* per juicefs subprocess; a container-wide
	// MINIO_ACCESS_KEY would also reach unrelated user destinations.
	if strings.Contains(mgr, "MINIO_ACCESS_KEY:") {
		t.Error("manager sets MINIO_ACCESS_KEY container-wide")
	}

	farm := service(t, "juicefarm")
	if !strings.Contains(farm, `MINIO_ACCESS_KEY: "${JM_OBJ_CLIENT_ACCESS_KEY:-jm-client}"`) ||
		!strings.Contains(farm, `MINIO_SECRET_KEY: "${JM_OBJ_CLIENT_SECRET_KEY:?`) {
		t.Error("juicefarm must get the jm-client key")
	}
	if strings.Contains(farm, "JM_OBJ_GC") {
		t.Error("juicefarm can see the delete-capable jm-gc key")
	}
}

func TestComposeProvisionsUsersFromEnv(t *testing.T) {
	keys := service(t, "minio-keys")
	for _, want := range []string{
		`"/jm-minio/provision-object-keys.sh"`, "./minio:/jm-minio:ro",
		`JM_OBJ_CLIENT_SECRET_KEY: "${JM_OBJ_CLIENT_SECRET_KEY:?`, `JM_OBJ_GC_SECRET_KEY: "${JM_OBJ_GC_SECRET_KEY:?`,
		`condition: service_healthy`,
	} {
		if !strings.Contains(keys, want) {
			t.Errorf("minio-keys lacks %q", want)
		}
	}
	env := mustRead(t, "../release.env.example")
	for _, v := range []string{"JM_OBJ_CLIENT_SECRET_KEY=", "JM_OBJ_GC_SECRET_KEY="} {
		if !strings.Contains(env, "\n"+v) {
			t.Errorf("release.env.example lacks %s", v)
		}
	}
}

// fakeDocker answers `docker ps` and `docker exec` the way the switch
// script uses them, and records every argv plus the values of the env
// variables each exec passes by name.
const fakeDocker = `#!/bin/bash
st="$FAKE_DOCKER_STATE"
printf '%s\x1f' "$@" >> "$st/argv"; echo >> "$st/argv"
case "$1" in
ps) echo "c-${4#label=com.docker.compose.service=}" ;;
exec)
  shift; names=()
  while [ "$1" = -e ]; do names+=("$2"); shift 2; done
  c="$1"; shift
  case "$1" in
  mountpoint) exit 0 ;;
  juicefs)
    if [ "$2" = status ]; then
      if [ -n "${FAKE_STATUS:-}" ]; then echo "$FAKE_STATUS"; else echo '{"Setting":{"Name":"zpool"}}'; fi
      exit 0
    fi
    { printf 'CALL'; printf ' %q' "$@"; echo; for n in "${names[@]}"; do echo "$n=${!n}"; done; } >> "$st/calls" ;;
  sh)
    case "$3" in
    *'juicefs config'*) { echo "CALL sh-config"; for n in "${names[@]}"; do echo "$n=${!n}"; done; } >> "$st/calls" ;;
    *) var="${3#*\{}"; var="${var%%:*}"
       for m in $FAKE_MISSING; do [ "$m" = "$c:$var" ] && exit 1; done; exit 0 ;;
    esac ;;
  esac ;;
esac
`

func runSwitch(t *testing.T, env []string, args ...string) (state, out string, err error) {
	t.Helper()
	state = t.TempDir()
	d := filepath.Join(state, "docker")
	if err := os.WriteFile(d, []byte(fakeDocker), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/bin/bash", append([]string{"object-key-switch.sh"}, args...)...)
	cmd.Env = append([]string{
		"PATH=/usr/bin:/bin", "DOCKER=" + d, "FAKE_DOCKER_STATE=" + state,
		"JM_PROXY_ADMIN_PASSWORD=adminsecret-0123456789abcdef",
		"JM_OBJ_GC_ACCESS_KEY=jm-gc", "JM_OBJ_GC_SECRET_KEY=" + strings.Repeat("g", 40),
		"MINIO_ROOT_PASSWORD=" + strings.Repeat("r", 24),
	}, env...)
	b, err := cmd.CombinedOutput()
	return state, string(b), err
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func readOpt(path string) string {
	b, _ := os.ReadFile(path)
	return string(b)
}

func TestSwitchClearEmptiesSettingKeyWithSecretsOffArgv(t *testing.T) {
	state, out, err := runSwitch(t, nil, "clear", "--all-macs-have-keys")
	if err != nil {
		t.Fatalf("clear: %v\n%s", err, out)
	}
	calls := readOpt(filepath.Join(state, "calls"))
	if !strings.Contains(calls, "CALL juicefs config redis://admin@respproxy:6379/1 --access-key '' --secret-key '' --yes") {
		t.Fatalf("clear did not run `juicefs config --access-key \"\" --secret-key \"\"` as admin:\n%s", calls)
	}
	for _, want := range []string{"META_PASSWORD=adminsecret-0123456789abcdef", "MINIO_ACCESS_KEY=jm-gc", "MINIO_SECRET_KEY=" + strings.Repeat("g", 40)} {
		if !strings.Contains(calls, want) {
			t.Errorf("config call env lacks %s", strings.SplitN(want, "=", 2)[0])
		}
	}
	argv := readOpt(filepath.Join(state, "argv"))
	for _, secret := range []string{"adminsecret", strings.Repeat("g", 40), strings.Repeat("r", 24)} {
		if strings.Contains(argv, secret) {
			t.Fatalf("a secret reached the docker command line:\n%s", argv)
		}
	}
}

func TestSwitchClearRefusesUntilEveryClientHasItsKey(t *testing.T) {
	cases := map[string][]string{
		"no ack flag":           nil,
		"farm lacks client key": {"FAKE_MISSING=c-juicefarm:MINIO_ACCESS_KEY"},
		"bg mount lacks gc key": {"FAKE_MISSING=c-juicefs:JM_OBJ_GC_ACCESS_KEY"},
		"manager lacks gc key":  {"FAKE_MISSING=c-juicemount-manager:JM_OBJ_GC_ACCESS_KEY"},
		"gc secret not loaded":  {"JM_OBJ_GC_SECRET_KEY="},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			args := []string{"clear", "--all-macs-have-keys"}
			if name == "no ack flag" {
				args = args[:1]
			}
			state, out, err := runSwitch(t, env, args...)
			if err == nil {
				t.Fatalf("clear ran:\n%s", out)
			}
			if calls := readOpt(filepath.Join(state, "calls")); calls != "" {
				t.Fatalf("juicefs config ran despite the refusal:\n%s", calls)
			}
		})
	}
}

func TestSwitchClearFailsIfSettingStillHoldsAKey(t *testing.T) {
	_, out, err := runSwitch(t, []string{`FAKE_STATUS={"Setting":{"AccessKey":"juicemount","SecretKey":"removed"}}`}, "clear", "--all-macs-have-keys")
	if err == nil || !strings.Contains(out, "still holds an object key") {
		t.Fatalf("err=%v out=%s", err, out)
	}
}

func TestSwitchRollbackRestoresGCKeyByName(t *testing.T) {
	state, out, err := runSwitch(t, nil, "rollback")
	if err != nil {
		t.Fatalf("rollback: %v\n%s", err, out)
	}
	calls := readOpt(filepath.Join(state, "calls"))
	if !strings.Contains(calls, "JM_RB_AK=jm-gc") || !strings.Contains(calls, "JM_RB_SK="+strings.Repeat("g", 40)) {
		t.Fatalf("rollback did not hand jm-gc to juicefs config:\n%s", calls)
	}
	if argv := readOpt(filepath.Join(state, "argv")); strings.Contains(argv, strings.Repeat("g", 40)) || strings.Contains(argv, "adminsecret") {
		t.Fatalf("rollback put a secret on the docker command line:\n%s", argv)
	}
	state, out, err = runSwitch(t, nil, "rollback", "--root")
	if err != nil || !strings.Contains(readOpt(filepath.Join(state, "calls")), "JM_RB_AK=juicemount") {
		t.Fatalf("rollback --root: %v\n%s", err, out)
	}
}

// Per-Mac object keys: minio-keyd holds the scoped key admin and nothing
// stronger; the Manager holds no MinIO admin credential at all; the
// objkeys directory (it holds per-Mac secrets) is shared by those two only.
func TestComposeKeydHoldsOnlyKeyAdmin(t *testing.T) {
	keyd := service(t, "minio-keyd")
	for _, want := range []string{
		`"/jm-minio/keyd.sh"`, "./minio:/jm-minio:ro",
		"/mnt/zSSD/juicemount/objkeys:/var/lib/jm-objkeys",
		`JM_OBJKEYS_DIR: "/var/lib/jm-objkeys"`,
		`JM_OBJ_KEYADMIN_SECRET_KEY: "${JM_OBJ_KEYADMIN_SECRET_KEY:-}"`,
		`JM_OBJ_PER_MAC_SERVER: "${JM_OBJ_PER_MAC_SERVER:-0}"`,
		`profiles: ["per-mac"]`,
	} {
		if !strings.Contains(keyd, want) {
			t.Errorf("minio-keyd lacks %q", want)
		}
	}
	// A stop during a key create is held up to keyd's HOLD_MAX (+5 s to
	// remove an undeliverable key); docker must wait longer than that, or
	// it SIGKILLs keyd and strands a live key with no delivered secret.
	grace := regexp.MustCompile(`(?m)^\s*stop_grace_period:\s*(\d+)s\s*$`).FindStringSubmatch(keyd)
	hold := regexp.MustCompile(`(?m)^HOLD_MAX="\$\{JM_KEYD_HOLD_MAX:-(\d+)\}"$`).FindStringSubmatch(mustRead(t, "keyd.sh"))
	if grace == nil || hold == nil {
		t.Errorf("minio-keyd stop_grace_period (%q) or keyd.sh HOLD_MAX default (%q) not found", grace, hold)
	} else if g, h := atoi(t, grace[1]), atoi(t, hold[1]); h+5 >= g {
		t.Errorf("keyd holds a stop up to %d+5 s but docker kills it at %d s", h, g)
	}
	for _, banned := range []string{"MINIO_ROOT", "JM_OBJ_GC", "JM_OBJ_CLIENT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "JM_PROXY_", "JM_ADMIN_KEY"} {
		if strings.Contains(keyd, banned) {
			t.Errorf("minio-keyd receives %s; it must hold the key admin credential only", banned)
		}
	}
	// Same pinned image as minio and minio-keys, so mc matches the server.
	img := func(svc string) string {
		for _, l := range strings.Split(service(t, svc), "\n") {
			if strings.HasPrefix(strings.TrimSpace(l), "image:") {
				return strings.TrimSpace(l)
			}
		}
		t.Fatalf("%s has no image", svc)
		return ""
	}
	if k := img("minio-keyd"); k != img("minio") || k != img("minio-keys") || !strings.Contains(k, "@sha256:") {
		t.Errorf("minio-keyd image %q differs from the pinned minio image %q", k, img("minio"))
	}

	mgr := service(t, "juicemount-manager")
	if strings.Contains(mgr, "KEYADMIN") || strings.Contains(mgr, "MINIO_ROOT") {
		t.Error("the Manager receives a MinIO admin credential")
	}
	for _, want := range []string{"/mnt/zSSD/juicemount/objkeys:/var/lib/jm-objkeys", `JM_OBJKEYS_DIR: "/var/lib/jm-objkeys"`, `JM_OBJ_PER_MAC: "${JM_OBJ_PER_MAC:-0}"`} {
		if !strings.Contains(mgr, want) {
			t.Errorf("manager lacks %q", want)
		}
	}
	// One switch per step: M1 (JM_OBJ_PER_MAC_SERVER) reaches minio-keys and
	// minio-keyd only; M2 (JM_OBJ_PER_MAC) reaches the Manager only. One
	// shared variable would switch the Manager on at its next redeploy.
	if strings.Contains(mgr, "JM_OBJ_PER_MAC_SERVER") {
		t.Error("the Manager reads the server-side M1 switch")
	}
	for _, svc := range []string{"minio-keys", "minio-keyd"} {
		if strings.Contains(service(t, svc), "JM_OBJ_PER_MAC:") || strings.Contains(service(t, svc), "${JM_OBJ_PER_MAC:") {
			t.Errorf("%s reads the Manager's M2 switch JM_OBJ_PER_MAC", svc)
		}
	}
	// Only keyd and the Manager see the directory that holds per-Mac secrets.
	compose := mustRead(t, "../docker-compose.yml")
	if n := strings.Count(compose, "/mnt/zSSD/juicemount/objkeys:"); n != 2 {
		t.Errorf("objkeys is mounted %d times, want 2 (minio-keyd, Manager)", n)
	}
	if strings.Contains(service(t, "respproxy"), "objkeys") {
		t.Error("respproxy can read per-Mac secrets")
	}

	// Nothing changes for today's deployment: the new inputs are optional.
	keys := service(t, "minio-keys")
	for _, want := range []string{`JM_OBJ_PER_MAC_SERVER: "${JM_OBJ_PER_MAC_SERVER:-0}"`, `JM_OBJ_KEYADMIN_SECRET_KEY: "${JM_OBJ_KEYADMIN_SECRET_KEY:-}"`} {
		if !strings.Contains(keys, want) {
			t.Errorf("minio-keys lacks %q", want)
		}
	}
	for _, l := range strings.Split(compose, "\n") {
		if (strings.Contains(l, "JM_OBJ_PER_MAC") || strings.Contains(l, "KEYADMIN")) && strings.Contains(l, ":?") {
			t.Errorf("a per-Mac input is required at render time: %s", strings.TrimSpace(l))
		}
	}
	env := mustRead(t, "../release.env.example")
	for _, v := range []string{"# JM_OBJ_PER_MAC_SERVER=1", "# JM_OBJ_PER_MAC=1", "# JM_OBJ_KEYADMIN_SECRET_KEY="} {
		if !strings.Contains(env, "\n"+v) {
			t.Errorf("release.env.example lacks the commented %s", v)
		}
	}
}
