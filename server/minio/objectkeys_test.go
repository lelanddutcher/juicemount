package miniodeploy

// Contract tests for the H-8B object keys (design v2 §2.1): the client
// policy can never delete, the gc policy can, and the provisioning script
// creates exactly those users with exactly those policies, idempotently,
// without the root credential on argv.

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type policyDoc struct {
	Version   string
	Statement []struct {
		Sid       string
		Effect    string
		Action    []string
		Resource  []string
		Condition map[string]any `json:",omitempty"`
	}
}

func loadPolicy(t *testing.T, name string) policyDoc {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	var p policyDoc
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return p
}

func allowedActions(t *testing.T, p policyDoc) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, s := range p.Statement {
		if s.Effect != "Allow" {
			t.Fatalf("unexpected effect %q: these policies are allow-lists only", s.Effect)
		}
		if len(s.Condition) != 0 {
			t.Fatalf("unexpected condition %v", s.Condition)
		}
		for _, a := range s.Action {
			out[a] = append(out[a], s.Resource...)
		}
	}
	return out
}

// The S3 calls juicefs 1.3.1 made against a recording S3 endpoint
// (measured 2026-10-06): format/config storage test = Put, Get, Delete;
// writes = PutObject (If-None-Match, patch 0002); reads = ranged
// GetObject; client unlink and gc = DeleteObject; gc and the format
// emptiness check = ListObjectsV2. HEAD is authorized by s3:GetObject.
func TestClientPolicyCannotDelete(t *testing.T) {
	acts := allowedActions(t, loadPolicy(t, "jm-client-policy.json"))
	want := map[string][]string{
		"s3:GetObject":  {"arn:aws:s3:::zpool/*"},
		"s3:PutObject":  {"arn:aws:s3:::zpool/*"},
		"s3:ListBucket": {"arn:aws:s3:::zpool"},
	}
	for a, res := range acts {
		lower := strings.ToLower(a)
		if strings.Contains(lower, "delete") || strings.Contains(a, "*") ||
			strings.HasPrefix(lower, "admin:") || strings.Contains(lower, "bucketpolicy") ||
			strings.Contains(lower, "putbucket") || strings.Contains(lower, "lifecycle") ||
			strings.Contains(lower, "abortmultipart") {
			t.Errorf("jm-client policy grants %s on %v", a, res)
		}
		w, ok := want[a]
		if !ok {
			t.Errorf("jm-client policy grants unexpected %s", a)
			continue
		}
		if !slices.Equal(res, w) {
			t.Errorf("%s resources = %v, want %v", a, res, w)
		}
	}
	for a := range want {
		if _, ok := acts[a]; !ok {
			t.Errorf("jm-client policy lacks %s, which a juicefs mount needs", a)
		}
	}
}

func TestGCPolicyDeletesButNoBucketAdmin(t *testing.T) {
	acts := allowedActions(t, loadPolicy(t, "jm-gc-policy.json"))
	if _, ok := acts["s3:DeleteObject"]; !ok {
		t.Fatal("jm-gc policy lacks s3:DeleteObject: gc and trash cleanup could never free space")
	}
	for a, res := range acts {
		lower := strings.ToLower(a)
		if strings.Contains(a, "*") || strings.HasPrefix(lower, "admin:") || strings.Contains(lower, "deletebucket") ||
			strings.Contains(lower, "putbucket") || strings.Contains(lower, "bucketpolicy") || strings.Contains(lower, "lifecycle") {
			t.Errorf("jm-gc policy grants bucket/admin action %s", a)
		}
		for _, r := range res {
			if r != "arn:aws:s3:::zpool" && r != "arn:aws:s3:::zpool/*" {
				t.Errorf("jm-gc %s reaches beyond the zpool bucket: %s", a, r)
			}
		}
	}
}

// fakeMC records every invocation and the native mc config supplied to it.
// It keeps users, policies and attachments in files, the way MinIO would.
const fakeMC = `#!/bin/sh
PATH=/usr/bin:/bin
st="$FAKE_MC_STATE"
[ -z "${MC_HOST_jmkeys+x}" ] && [ -z "${MC_CONFIG_ENV_FILE+x}" ] || { echo "inherited mc alias override" >&2; exit 8; }
[ -r "$MC_CONFIG_DIR/config.json" ] || { echo "missing native mc config" >&2; exit 8; }
printf '%s\n' "$MC_CONFIG_DIR" >> "$st/config-dirs"
cp "$MC_CONFIG_DIR/config.json" "$st/config.json"
for path in "$MC_CONFIG_DIR" "$MC_CONFIG_DIR/config.json"; do
  mode=$(stat -c %a "$path" 2>/dev/null) || mode=$(stat -f %Lp "$path") || exit 8
  printf '%s\n' "$mode" >> "$st/config-modes"
done
printf '%s\n' "$*" >> "$st/argv"
json=0
[ "$1" = --json ] && { json=1; shift; }
case "$1 $2 $3" in
"admin policy create") cp "$6" "$st/policy.$5" ;;
"admin user add")
  [ -n "$6" ] && { echo "secret on argv" >&2; exit 8; }
  [ "$5" = "${FAKE_MC_FAIL_USER:-}" ] && { echo "mc: user add $5 failed" >&2; exit 1; }
  IFS= read -r sk; printf '%s' "$sk" > "$st/user.$5" ;;
"admin policy attach")
  u="$7"
  [ -f "$st/attach.$u" ] && { echo "policy already attached" >&2; exit 1; }
  [ -f "$st/user.$u" ] || exit 1
  printf '%s' "$5" > "$st/attach.$u" ;;
"admin user info")
  [ -f "$st/user.$5" ] || exit 1
  printf '{"status":"success","accessKey":"%s","policyName":"%s","userStatus":"enabled"}\n' "$5" "$(cat "$st/attach.$5" 2>/dev/null)" ;;
*) echo "fake mc: unexpected $*" >&2; exit 9 ;;
esac
`

// Keep the root user distinct from repository path components: argv may
// legitimately contain absolute policy paths under a juicemount checkout.
const fixtureRootUser = "root-fixture-7c42"

type nativeMCAlias struct {
	URL       string `json:"url"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	API       string `json:"api"`
	Path      string `json:"path"`
}

func readNativeMCConfig(t *testing.T, state string) nativeMCAlias {
	t.Helper()
	var cfg struct {
		Version string                   `json:"version"`
		Aliases map[string]nativeMCAlias `json:"aliases"`
	}
	b, err := os.ReadFile(filepath.Join(state, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("native mc config is invalid JSON: %v", err)
	}
	if cfg.Version != "10" || len(cfg.Aliases) != 1 {
		t.Fatalf("native mc config has version %q, aliases %v", cfg.Version, cfg.Aliases)
	}
	a, ok := cfg.Aliases["jmkeys"]
	if !ok || a.API != "S3v4" || a.Path != "auto" {
		t.Fatalf("native mc alias = %+v", a)
	}
	return a
}

func assertPrivateMCConfigRetired(t *testing.T, state string) {
	t.Helper()
	dirsRaw := strings.TrimSuffix(readOpt(filepath.Join(state, "config-dirs")), "\n")
	if dirsRaw == "" {
		t.Fatal("mc never saw a config directory")
	}
	dirs := strings.Split(dirsRaw, "\n")
	for _, dir := range dirs {
		if _, err := os.Lstat(dir); !os.IsNotExist(err) {
			t.Fatalf("private mc config directory %q survived: %v", dir, err)
		}
	}
	modes := strings.Fields(readOpt(filepath.Join(state, "config-modes")))
	if len(modes) != 2*len(dirs) {
		t.Fatalf("saw %d mc config paths but %d recorded modes", len(dirs), len(modes))
	}
	for i, mode := range modes {
		want := "700"
		if i%2 == 1 {
			want = "600"
		}
		if mode != want {
			t.Fatalf("mc config mode %d = %q, want %q", i, mode, want)
		}
	}
}

func runProvision(t *testing.T, state string, env ...string) (string, error) {
	t.Helper()
	mc := filepath.Join(state, "mc")
	if err := os.WriteFile(mc, []byte(fakeMC), 0o755); err != nil {
		t.Fatal(err)
	}
	cwd, _ := os.Getwd()
	cmd := exec.Command("/bin/bash", "provision-object-keys.sh")
	cmd.Env = append([]string{
		"PATH=/usr/bin:/bin", "TMPDIR=" + state, "MC=" + mc, "FAKE_MC_STATE=" + state,
		"MINIO_ROOT_USER=" + fixtureRootUser, "MINIO_ROOT_PASSWORD=root/pass@word",
		"JM_MINIO_POLICY_DIR=" + cwd,
		"JM_OBJ_CLIENT_SECRET_KEY=" + strings.Repeat("c", 40),
		"JM_OBJ_GC_SECRET_KEY=" + strings.Repeat("g", 40),
	}, env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func TestProvisionCreatesExactlyTwoUsersIdempotently(t *testing.T) {
	state := t.TempDir()
	for run := 1; run <= 2; run++ {
		out, err := runProvision(t, state)
		if err != nil {
			t.Fatalf("run %d: %v\n%s", run, err, out)
		}
	}
	read := func(n string) string {
		if !strings.HasPrefix(n, "jm-") {
			n = filepath.Join(state, n)
		}
		b, err := os.ReadFile(n)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		return string(b)
	}
	if got := read("attach.jm-client"); got != "jm-client" {
		t.Errorf("jm-client attached to %q", got)
	}
	if got := read("attach.jm-gc"); got != "jm-gc" {
		t.Errorf("jm-gc attached to %q", got)
	}
	if read("policy.jm-client") != read("jm-client-policy.json") {
		// policy.<name> is a copy of the file the script handed to mc.
		t.Error("jm-client policy is not jm-client-policy.json")
	}
	if strings.Contains(read("policy.jm-client"), "Delete") {
		t.Error("the policy given to jm-client grants a Delete action")
	}
	argv := read("argv")
	for _, secret := range []string{"root/pass@word", fixtureRootUser, strings.Repeat("c", 40), strings.Repeat("g", 40)} {
		if strings.Contains(argv, secret) {
			t.Errorf("a credential (%.6s...) reached mc argv:\n%s", secret, argv)
		}
	}
	if read("user.jm-client") != strings.Repeat("c", 40) || read("user.jm-gc") != strings.Repeat("g", 40) {
		t.Error("user secrets did not reach mc through stdin")
	}
	if got := readNativeMCConfig(t, state); got != (nativeMCAlias{URL: "http://127.0.0.1:9000", AccessKey: fixtureRootUser, SecretKey: "root/pass@word", API: "S3v4", Path: "auto"}) {
		t.Errorf("native mc alias = %+v", got)
	}
	assertPrivateMCConfigRetired(t, state)
	if strings.Contains(argv, "juicefs") || strings.Contains(argv, "config") {
		t.Errorf("provisioning touched the volume setting:\n%s", argv)
	}
}

// The pinned mc treats MC_HOST credentials literally, so its native config
// must carry every byte of the root credentials without URL encoding.
func TestProvisionNativeMCConfigPreservesRootCredentials(t *testing.T) {
	state := t.TempDir()
	rootUser := "Root:admin+%/\"\\é\t"
	rootPass := "p:@+%/\"\\é\r\n\t\x01\x7f"
	endpoint := "https://minio.example.test:9000"
	out, err := runProvision(t, state,
		"MINIO_ROOT_USER="+rootUser,
		"MINIO_ROOT_PASSWORD="+rootPass,
		"JM_MINIO_ENDPOINT="+endpoint)
	if err != nil {
		t.Fatalf("provisioning with special root credentials: %v\n%s", err, out)
	}
	if got := readNativeMCConfig(t, state); got != (nativeMCAlias{URL: endpoint, AccessKey: rootUser, SecretKey: rootPass, API: "S3v4", Path: "auto"}) {
		t.Fatalf("root credentials changed in native mc config: %+v", got)
	}
	argv := readOpt(filepath.Join(state, "argv"))
	for _, secret := range []string{rootUser, rootPass} {
		if strings.Contains(argv, secret) {
			t.Fatal("root credential reached mc argv")
		}
	}
	assertPrivateMCConfigRetired(t, state)
}

func TestProvisionIgnoresForeignMCConfigAndRetiresOnlyOwnedDirectory(t *testing.T) {
	state := t.TempDir()
	foreign := filepath.Join(state, "foreign-mc-config")
	if err := os.Mkdir(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(foreign, "keep")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runProvision(t, state,
		"MC_CONFIG_DIR="+foreign,
		"MC_CONFIG_ENV_FILE="+sentinel,
		"MC_HOST_jmkeys=http://attacker:credential@elsewhere.invalid")
	if err != nil {
		t.Fatalf("foreign mc config override affected provisioning: %v\n%s", err, out)
	}
	if got := readOpt(sentinel); got != "unchanged" {
		t.Fatalf("caller config changed: %q", got)
	}
	if got := readNativeMCConfig(t, state); got.AccessKey != fixtureRootUser || got.SecretKey != "root/pass@word" {
		t.Fatalf("foreign alias overrode private config: %+v", got)
	}
	for _, dir := range strings.Split(strings.TrimSuffix(readOpt(filepath.Join(state, "config-dirs")), "\n"), "\n") {
		if dir == foreign {
			t.Fatal("mc used the caller's config directory")
		}
	}
	assertPrivateMCConfigRetired(t, state)
}

func TestProvisionNativeMCConfigCleanupOnFailure(t *testing.T) {
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "attach.jm-client"), []byte("jm-client,readwrite"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err := runProvision(t, state)
	if err == nil || !strings.Contains(out, "want exactly 'jm-client'") {
		t.Fatalf("expected policy failure, got %v\n%s", err, out)
	}
	assertPrivateMCConfigRetired(t, state)
}

func TestProvisionRefusesUnsafeConfigParentBeforeMC(t *testing.T) {
	state := t.TempDir()
	foreign := filepath.Join(state, "foreign")
	if err := os.Mkdir(foreign, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(foreign, "keep")
	if err := os.WriteFile(sentinel, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(state, "symlink-parent")
	if err := os.Symlink(foreign, link); err != nil {
		t.Fatal(err)
	}
	for _, parent := range []string{link, filepath.Join(state, "missing-parent")} {
		out, err := runProvision(t, state, "TMPDIR="+parent)
		if err == nil || !strings.Contains(out, "TMPDIR must be an existing directory") {
			t.Fatalf("unsafe TMPDIR %q accepted: %v\n%s", parent, err, out)
		}
		if _, err := os.Stat(filepath.Join(state, "argv")); !os.IsNotExist(err) {
			t.Fatalf("mc ran despite unsafe TMPDIR: %v", err)
		}
	}
	if got := readOpt(sentinel); got != "unchanged" {
		t.Fatalf("foreign directory changed: %q", got)
	}
}

func TestProvisionRefusesUnsafeInputs(t *testing.T) {
	cases := map[string][]string{
		"gc key equals client key": {"JM_OBJ_GC_ACCESS_KEY=jm-client"},
		"key is root user":         {"JM_OBJ_CLIENT_ACCESS_KEY=" + fixtureRootUser},
		"short secret":             {"JM_OBJ_CLIENT_SECRET_KEY=abc"},
		"same secrets":             {"JM_OBJ_GC_SECRET_KEY=" + strings.Repeat("c", 40)},
		"no root password":         {"MINIO_ROOT_PASSWORD="},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			out, err := runProvision(t, state, env...)
			if err == nil {
				t.Fatalf("accepted unsafe input:\n%s", out)
			}
			if _, statErr := os.Stat(filepath.Join(state, "argv")); statErr == nil {
				t.Fatalf("called mc before refusing:\n%s", out)
			}
		})
	}
}

// A user that already carries a second policy (e.g. readwrite, which
// includes DeleteObject) must fail the run, not pass silently.
func TestProvisionFailsWhenClientHasExtraPolicy(t *testing.T) {
	state := t.TempDir()
	if err := os.WriteFile(filepath.Join(state, "attach.jm-client"), []byte("jm-client,readwrite"), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := runProvision(t, state)
	if err == nil || !strings.Contains(out, "want exactly 'jm-client'") {
		t.Fatalf("err=%v, out:\n%s", err, out)
	}
}

// The key admin may create, remove and list service accounts and nothing
// else: no S3 action, no wildcard, no user or policy administration, and
// no UpdateServiceAccount (MinIO checks only the action for an update, so
// it would let the holder reset the secret of any account). What this
// credential does NOT limit: AddServiceAccount accepts any non-root
// target (design A7), so "mints only under jm-macs" is keyd.sh's job.
func TestKeyAdminPolicyIsServiceAccountsOnly(t *testing.T) {
	p := loadPolicy(t, "jm-keyadmin-policy.json")
	got := map[string]bool{}
	for _, s := range p.Statement {
		if s.Effect != "Allow" || len(s.Condition) != 0 || len(s.Resource) != 0 {
			t.Fatalf("unexpected statement %+v", s)
		}
		for _, a := range s.Action {
			got[a] = true
		}
	}
	if got["admin:UpdateServiceAccount"] {
		t.Fatal("jm-keyadmin may update service accounts: it could reset any account's secret")
	}
	want := []string{"admin:CreateServiceAccount", "admin:RemoveServiceAccount", "admin:ListServiceAccounts"}
	if len(got) != len(want) {
		t.Fatalf("jm-keyadmin actions = %v, want exactly %v", got, want)
	}
	for _, a := range want {
		if !got[a] {
			t.Errorf("jm-keyadmin lacks %s", a)
		}
	}
}

func perMacEnv() []string {
	return []string{"JM_OBJ_PER_MAC_SERVER=1", "JM_OBJ_KEYADMIN_SECRET_KEY=" + strings.Repeat("k", 40)}
}

// With JM_OBJ_PER_MAC_SERVER unset or 0, provisioning is exactly what it
// was. The Manager's switch (JM_OBJ_PER_MAC, step M2) does not provision.
func TestProvisionPerMacOffTouchesNoNewUser(t *testing.T) {
	for name, env := range map[string][]string{
		"unset":               nil,
		"zero":                {"JM_OBJ_PER_MAC_SERVER=0"},
		"empty":               {"JM_OBJ_PER_MAC_SERVER="},
		"manager switch only": {"JM_OBJ_PER_MAC=1", "JM_OBJ_KEYADMIN_SECRET_KEY=" + strings.Repeat("k", 40)},
	} {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			out, err := runProvision(t, state, env...)
			if err != nil {
				t.Fatalf("%v\n%s", err, out)
			}
			argv := readOpt(filepath.Join(state, "argv"))
			for _, u := range []string{"jm-macs", "jm-keyadmin"} {
				if strings.Contains(argv, u) {
					t.Fatalf("per-Mac mode off, but mc saw %s:\n%s", u, argv)
				}
			}
			if strings.Contains(out, "ERROR") {
				t.Fatalf("off is not an error:\n%s", out)
			}
		})
	}
}

// assertSharedReadyPerMacSkipped: the run exited 0 (juicefs-init can
// start), jm-client and jm-gc were provisioned and verified, the per-Mac
// users were not, and the log says so loudly.
func assertSharedReadyPerMacSkipped(t *testing.T, state, out string, err error, why string) {
	t.Helper()
	if err != nil {
		t.Fatalf("a per-Mac failure failed minio-keys, which blocks juicefs-init: %v\n%s", err, out)
	}
	if readOpt(filepath.Join(state, "attach.jm-client")) != "jm-client" || readOpt(filepath.Join(state, "attach.jm-gc")) != "jm-gc" {
		t.Fatalf("jm-client / jm-gc not provisioned:\n%s", out)
	}
	for _, want := range []string{"[object-keys] jm-client: policy jm-client, enabled", "[object-keys] jm-gc: policy jm-gc, enabled",
		"ERROR: per-Mac object keys NOT provisioned", "per-Mac keys are OFF", why} {
		if !strings.Contains(out, want) {
			t.Fatalf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(readOpt(filepath.Join(state, "argv")), "jm-keyadmin") {
		t.Fatalf("provisioned jm-keyadmin despite the failure:\n%s", readOpt(filepath.Join(state, "argv")))
	}
}

func TestProvisionPerMacCreatesParentAndKeyAdmin(t *testing.T) {
	state := t.TempDir()
	for run := 1; run <= 2; run++ {
		if out, err := runProvision(t, state, perMacEnv()...); err != nil {
			t.Fatalf("run %d: %v\n%s", run, err, out)
		}
	}
	if got := readOpt(filepath.Join(state, "attach.jm-macs")); got != "jm-client" {
		t.Errorf("jm-macs attached to %q, want jm-client (the Macs' exact policy)", got)
	}
	if got := readOpt(filepath.Join(state, "attach.jm-keyadmin")); got != "jm-keyadmin" {
		t.Errorf("jm-keyadmin attached to %q", got)
	}
	if readOpt(filepath.Join(state, "policy.jm-keyadmin")) != mustRead(t, "jm-keyadmin-policy.json") {
		t.Error("jm-keyadmin policy is not jm-keyadmin-policy.json")
	}
	if readOpt(filepath.Join(state, "user.jm-keyadmin")) != strings.Repeat("k", 40) {
		t.Error("key admin secret did not reach mc through stdin")
	}
	parentSecret := readOpt(filepath.Join(state, "user.jm-macs"))
	if len(parentSecret) != 40 || parentSecret == strings.Repeat("c", 40) {
		t.Errorf("jm-macs secret %q is not a fresh random 40-character value", parentSecret)
	}
	argv := readOpt(filepath.Join(state, "argv"))
	for _, secret := range []string{strings.Repeat("k", 40), parentSecret} {
		if strings.Contains(argv, secret) {
			t.Fatalf("a secret reached mc argv:\n%s", argv)
		}
	}
	// The parent of live service accounts is created once, never re-keyed.
	if n := strings.Count(argv, "admin user add jmkeys jm-macs"); n != 1 {
		t.Fatalf("jm-macs added %d times across two runs:\n%s", n, argv)
	}
}

// Unsafe per-Mac inputs are refused before any per-Mac mc call, and the
// refusal skips the per-Mac part only.
func TestProvisionPerMacRefusesUnsafeInputs(t *testing.T) {
	cases := map[string]struct {
		env []string
		why string
	}{
		"no key admin secret":    {[]string{"JM_OBJ_KEYADMIN_SECRET_KEY="}, "JM_OBJ_KEYADMIN_SECRET_KEY must be 32-40"},
		"key admin is jm-client": {[]string{"JM_OBJ_KEYADMIN_ACCESS_KEY=jm-client"}, "the key admin must not be 'jm-client'"},
		"key admin is jm-macs":   {[]string{"JM_OBJ_KEYADMIN_ACCESS_KEY=jm-macs"}, "the key admin must not be 'jm-macs'"},
		"key admin is root":      {[]string{"JM_OBJ_KEYADMIN_ACCESS_KEY=" + fixtureRootUser}, "the key admin must not be '" + fixtureRootUser + "'"},
		"key admin reuses gc":    {[]string{"JM_OBJ_KEYADMIN_SECRET_KEY=" + strings.Repeat("g", 40)}, "must differ from the client and gc secrets"},
		"flag true":              {[]string{"JM_OBJ_PER_MAC_SERVER=true"}, "JM_OBJ_PER_MAC_SERVER='true' is not 1 or 0"},
		"flag yes":               {[]string{"JM_OBJ_PER_MAC_SERVER=yes"}, "JM_OBJ_PER_MAC_SERVER='yes' is not 1 or 0"},
		"flag space one":         {[]string{"JM_OBJ_PER_MAC_SERVER= 1"}, "JM_OBJ_PER_MAC_SERVER=' 1' is not 1 or 0"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			state := t.TempDir()
			out, err := runProvision(t, state, append(perMacEnv(), c.env...)...)
			assertSharedReadyPerMacSkipped(t, state, out, err, c.why)
			if strings.Contains(readOpt(filepath.Join(state, "argv")), "jm-macs") {
				t.Fatalf("called mc for jm-macs despite the refusal:\n%s", out)
			}
		})
	}
	// A client named jm-macs with per-Mac on: the parent would be a shared
	// user. Refused; and the shared users themselves are unaffected.
	state := t.TempDir()
	out, err := runProvision(t, state, append(perMacEnv(), "JM_OBJ_CLIENT_ACCESS_KEY=jm-macs")...)
	if err != nil || !strings.Contains(out, "'jm-macs' is reserved for the per-Mac key parent") || strings.Contains(readOpt(filepath.Join(state, "argv")), "jm-keyadmin") {
		t.Fatalf("err=%v out=%s", err, out)
	}
}

// mc failures and a missing tr/head in the per-Mac part are per-Mac
// failures too; a failure for jm-client or jm-gc still fails the run.
func TestProvisionPerMacFailureDoesNotBlockStack(t *testing.T) {
	t.Run("jm-macs user add fails", func(t *testing.T) {
		state := t.TempDir()
		out, err := runProvision(t, state, append(perMacEnv(), "FAKE_MC_FAIL_USER=jm-macs")...)
		assertSharedReadyPerMacSkipped(t, state, out, err, "user add jm-macs failed")
	})
	t.Run("jm-keyadmin user add fails", func(t *testing.T) {
		state := t.TempDir()
		out, err := runProvision(t, state, append(perMacEnv(), "FAKE_MC_FAIL_USER=jm-keyadmin")...)
		if err != nil || !strings.Contains(out, "user add jm-keyadmin failed") || !strings.Contains(out, "per-Mac keys are OFF") {
			t.Fatalf("err=%v out=%s", err, out)
		}
	})
	t.Run("no tr or head", func(t *testing.T) {
		state := t.TempDir()
		limited := t.TempDir() // config setup can run, but tr/head cannot
		for _, name := range []string{"mkdir", "rm"} {
			binary, err := exec.LookPath(name)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(binary, filepath.Join(limited, name)); err != nil {
				t.Fatal(err)
			}
		}
		out, err := runProvision(t, state, append(perMacEnv(), "PATH="+limited)...)
		assertSharedReadyPerMacSkipped(t, state, out, err, "could not generate a secret for jm-macs")
	})
	t.Run("jm-gc fails with per-Mac on", func(t *testing.T) {
		state := t.TempDir()
		out, err := runProvision(t, state, append(perMacEnv(), "FAKE_MC_FAIL_USER=jm-gc")...)
		if err == nil {
			t.Fatalf("a jm-gc failure passed:\n%s", out)
		}
		if strings.Contains(readOpt(filepath.Join(state, "argv")), "jm-macs") {
			t.Fatal("went on to the per-Mac users after a shared failure")
		}
	})
	t.Run("shared input invalid with per-Mac on", func(t *testing.T) {
		state := t.TempDir()
		if out, err := runProvision(t, state, append(perMacEnv(), "JM_OBJ_CLIENT_SECRET_KEY=abc")...); err == nil {
			t.Fatalf("a bad client secret passed:\n%s", out)
		}
	})
}

func TestProvisionRefusesKeyAdminPolicyWithS3(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"jm-client-policy.json", "jm-gc-policy.json"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte(mustRead(t, f)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	bad := strings.Replace(mustRead(t, "jm-keyadmin-policy.json"), `"admin:ListServiceAccounts"`, `"admin:ListServiceAccounts", "s3:DeleteObject"`, 1)
	if err := os.WriteFile(filepath.Join(dir, "jm-keyadmin-policy.json"), []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	out, err := runProvision(t, state, append(perMacEnv(), "JM_MINIO_POLICY_DIR="+dir)...)
	if err != nil || !strings.Contains(out, "grants an S3 or wildcard action") ||
		strings.Contains(readOpt(filepath.Join(state, "argv")), "jm-keyadmin") {
		t.Fatalf("err=%v out=%s", err, out)
	}
}
