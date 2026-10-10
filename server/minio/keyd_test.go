package miniodeploy

// Contract tests for minio-keyd (per-Mac object keys, H-8 per-device
// design §4 and §10 K1-K4). A fake `mc` keeps service accounts as files
// and records every argv, so the tests see exactly what keyd asked MinIO
// to do.

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeKeydMC models `mc admin user svcacct {ls,info,add,disable,rm}`.
// State: sa.<name> holds "<parent> <status>"; parent.<name> overrides
// the parent that info reports (K4). The JSON shapes follow mc's
// acctMessage (accessKey, secretKey, parentUser, accountStatus,
// impliedPolicy); see the unverified list in the design §13.
const fakeKeydMC = `#!/bin/bash
st="$FAKE_MC_STATE"
printf '%s\n' "$*" >> "$st/argv"
printf '%s\n' "$MC_HOST_jmkeyd" >> "$st/hosts"
[ -n "${JM_OBJ_KEYADMIN_SECRET_KEY+set}" ] && echo "JM_OBJ_KEYADMIN_SECRET_KEY reached mc" >> "$st/leaked-env"
[ "$1" = --json ] && shift
[ "$1 $2 $3" = "admin user svcacct" ] || { echo "fake mc: unexpected $*" >&2; exit 9; }
op="$4"; alias="$5"; shift 5
[ "$alias" = jmkeyd ] || { echo "fake mc: alias $alias" >&2; exit 9; }
err() { printf '{"status":"error","error":{"message":"%s"}}\n' "$1"; exit 1; }
case "$op" in
ls)
  [ -n "${FAKE_MC_FAIL_LS:-}" ] && err "connection refused"
  for f in "$st"/sa.*; do
    [ -e "$f" ] || continue
    read -r p s < "$f"
    [ "$p" = "$1" ] || [ -e "$st/parent.${f##*/sa.}" ] || continue
    printf '{"status":"success","accessKey":"%s","expiration":"1970-01-01T00:00:00Z"}\n' "${f##*/sa.}"
  done ;;
info)
  [ -e "$st/sa.$1" ] || err "The specified service account is not found"
  read -r p s < "$st/sa.$1"
  [ -e "$st/parent.$1" ] && p="$(cat "$st/parent.$1")"
  printf '{"status":"success","accessKey":"%s","parentUser":"%s","accountStatus":"%s","impliedPolicy":true,"policy":{"Version":"2012-10-17"}}\n' "$1" "$p" "$s" ;;
add)
  parent="$1"; shift; ak=''; sk='Gen0'"$(printf '%036d' "$(ls "$st" | wc -l)")"
  while [ $# -gt 0 ]; do
    case "$1" in --access-key) ak="$2"; shift 2 ;; --secret-key) sk="$2"; shift 2 ;; *) err "flag $1" ;; esac
  done
  [ -e "$st/sa.$ak" ] && err "access key already exists"
  [ "${FAKE_MC_FAIL_ADD:-}" = "$ak" ] && err "fake add failure"
  [ -n "${FAKE_MC_ADD_SLEEP:-}" ] && { : > "$st/adding"; sleep "$FAKE_MC_ADD_SLEEP"; }
  # MinIO made the key, then the reply never comes back.
  [ -n "${FAKE_MC_ADD_HANG_AFTER:-}" ] && { echo "$parent on" > "$st/sa.$ak"; : > "$st/adding"; sleep "$FAKE_MC_ADD_HANG_AFTER"; }
  [ -n "${FAKE_MC_WRONG_AK:-}" ] && { echo "$parent on" > "$st/sa.$ak"; printf '{"status":"success","accessKey":"%s","secretKey":"%s"}\n' "$FAKE_MC_WRONG_AK" "$sk"; exit 0; }
  [ -n "${FAKE_MC_BAD_ADD:-}" ] && { echo "$parent on" > "$st/sa.$ak"; printf '{"status":"success","accessKey":"%s"}\n' "$ak"; exit 0; }
  echo "$parent on" > "$st/sa.$ak"
  printf '%s' "$sk" > "$st/secret.$ak"
  printf '{"status":"success","accessKey":"%s","secretKey":"%s","accountStatus":"on"}\n' "$ak" "$sk" ;;
disable)
  [ -e "$st/sa.$1" ] || err "not found"
  read -r p s < "$st/sa.$1"; echo "$p off" > "$st/sa.$1" ;;
rm)
  [ -e "$st/sa.$1" ] || err "not found"
  rm -f "$st/sa.$1" "$st/secret.$1" ;;
*) err "op $op" ;;
esac
`

const keyadminSecret = "KeyAdminSecret0123456789abcdefABCDEF"

type keydRig struct {
	t     *testing.T
	state string // fake MinIO
	dir   string // shared objkeys directory
}

func newKeydRig(t *testing.T) *keydRig {
	t.Helper()
	r := &keydRig{t: t, state: t.TempDir(), dir: filepath.Join(t.TempDir(), "objkeys")}
	if err := os.WriteFile(filepath.Join(r.state, "mc"), []byte(fakeKeydMC), 0o755); err != nil {
		t.Fatal(err)
	}
	return r
}

// key creates a service account in the fake MinIO.
func (r *keydRig) key(name, parent string) {
	r.t.Helper()
	if err := os.WriteFile(filepath.Join(r.state, "sa."+name), []byte(parent+" on\n"), 0o644); err != nil {
		r.t.Fatal(err)
	}
}

func (r *keydRig) exists(name string) bool {
	_, err := os.Stat(filepath.Join(r.state, "sa."+name))
	return err == nil
}

func (r *keydRig) desired(body string) {
	r.t.Helper()
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		r.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, "desired.json"), []byte(body), 0o600); err != nil {
		r.t.Fatal(err)
	}
}

func (r *keydRig) run(env ...string) (string, error) {
	r.t.Helper()
	cmd := exec.Command("/bin/bash", "keyd.sh")
	cmd.Env = append([]string{
		"PATH=/usr/bin:/bin", "TMPDIR=" + r.state, "MC=" + filepath.Join(r.state, "mc"),
		"FAKE_MC_STATE=" + r.state, "JM_OBJ_PER_MAC_SERVER=1", "JM_KEYD_ONCE=1",
		"JM_OBJKEYS_DIR=" + r.dir, "JM_MINIO_ENDPOINT=http://minio:9000",
		"JM_OBJ_KEYADMIN_SECRET_KEY=" + keyadminSecret,
	}, env...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (r *keydRig) mustRun(env ...string) string {
	r.t.Helper()
	out, err := r.run(env...)
	if err != nil {
		r.t.Fatalf("keyd: %v\n%s", err, out)
	}
	return out
}

func (r *keydRig) argv() string { return readOpt(filepath.Join(r.state, "argv")) }

// calls returns the recorded mc operations of one kind ("add", "rm", ...).
func (r *keydRig) calls(op string) []string {
	var out []string
	for _, l := range strings.Split(r.argv(), "\n") {
		if strings.HasPrefix(l, "--json admin user svcacct "+op+" ") {
			out = append(out, l)
		}
	}
	return out
}

type appliedKey struct {
	Name          string `json:"name"`
	Exists        bool   `json:"exists"`
	Status        string `json:"status"`
	Parent        string `json:"parent"`
	ImpliedPolicy bool   `json:"implied_policy"`
}

type appliedDoc struct {
	Seq        int64        `json:"seq"`
	VerifiedMS int64        `json:"verified_ms"`
	Error      string       `json:"error"`
	Keys       []appliedKey `json:"keys"`
}

func (r *keydRig) applied() appliedDoc {
	r.t.Helper()
	b, err := os.ReadFile(filepath.Join(r.dir, "applied.json"))
	if err != nil {
		r.t.Fatal(err)
	}
	var a appliedDoc
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&a); err != nil {
		r.t.Fatalf("applied.json is not valid JSON: %v\n%s", err, b)
	}
	return a
}

func (a appliedDoc) key(name string) (appliedKey, bool) {
	for _, k := range a.Keys {
		if k.Name == name {
			return k, true
		}
	}
	return appliedKey{}, false
}

func macName(i, gen int) string { return fmt.Sprintf("mac-%012x-%d", 0x4f1c2a9e7b00+i, gen) }

func desiredJSON(seq int, extra string, names ...string) string {
	var ks []string
	for _, n := range names {
		ks = append(ks, fmt.Sprintf(`{"name":%q,"state":"active"}`, n))
	}
	return fmt.Sprintf(`{"seq":%d,"store_count":%d%s,"keys":[%s]}`, seq, len(names), extra, strings.Join(ks, ","))
}

func removeJSON(names ...string) string {
	var quoted []string
	for _, n := range names {
		quoted = append(quoted, fmt.Sprintf("%q", n))
	}
	return `,"remove":[` + strings.Join(quoted, ",") + `]`
}

// K3: the secret is generated by MinIO, so it never appears on any argv,
// and it reaches the Manager only through results/<name>.json (0600).
func TestKeydMintsWithoutSecretOnArgv(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.desired(desiredJSON(1, "", name))
	r.mustRun()

	adds := r.calls("add")
	if len(adds) != 1 || adds[0] != "--json admin user svcacct add jmkeyd jm-macs --access-key "+name {
		t.Fatalf("svcacct add calls = %q", adds)
	}
	secret := readOpt(filepath.Join(r.state, "secret."+name))
	if len(secret) != 40 {
		t.Fatalf("fake MinIO did not generate a secret: %q", secret)
	}
	if strings.Contains(r.argv(), "--secret-key") || strings.Contains(r.argv(), secret) {
		t.Fatalf("a secret reached mc argv:\n%s", r.argv())
	}
	if strings.Contains(r.argv(), keyadminSecret) {
		t.Fatalf("the key admin secret reached mc argv:\n%s", r.argv())
	}
	if h := readOpt(filepath.Join(r.state, "hosts")); !strings.Contains(h, "http://jm-keyadmin:"+keyadminSecret+"@minio:9000") {
		t.Fatalf("mc did not authenticate as jm-keyadmin:\n%s", h)
	}
	resPath := filepath.Join(r.dir, "results", name+".json")
	fi, err := os.Stat(resPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("result mode %v, want 0600", fi.Mode().Perm())
	}
	var res struct {
		Name      string `json:"name"`
		SecretKey string `json:"secret_key"`
		CreatedMS int64  `json:"created_ms"`
	}
	if err := json.Unmarshal([]byte(mustRead(t, resPath)), &res); err != nil {
		t.Fatal(err)
	}
	if res.Name != name || res.SecretKey != secret || res.CreatedMS <= 0 {
		t.Fatalf("result = %+v", res)
	}
	if di, _ := os.Stat(r.dir); di.Mode().Perm() != 0o700 {
		t.Errorf("objkeys dir mode %v, want 0700", di.Mode().Perm())
	}
	// No other file in objkeys/ holds the secret.
	_ = filepath.Walk(r.dir, func(p string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() && p != resPath && strings.Contains(mustRead(t, p), secret) {
			t.Errorf("%s contains the secret", p)
		}
		return nil
	})
	a := r.applied()
	if k, ok := a.key(name); !ok || !k.Exists || k.Status != "on" || k.Parent != "jm-macs" || !k.ImpliedPolicy {
		t.Fatalf("applied = %+v", a)
	}
	if a.Seq != 1 || a.Error != "" {
		t.Fatalf("applied seq/error = %d %q", a.Seq, a.Error)
	}

	// Idempotent: a second cycle creates nothing.
	r.mustRun()
	if n := len(r.calls("add")); n != 1 {
		t.Fatalf("second cycle added again (%d adds)", n)
	}
}

// K1: names that fail the regex, and any parent other than jm-macs, are
// refused, and the refusal is whole-file: nothing is created or removed.
func TestKeydRefusesBadNamesAndForeignParents(t *testing.T) {
	good := macName(1, 1)
	cases := map[string]string{
		"gc user name":       desiredJSON(2, "", good, "jm-gc"),
		"uppercase hex":      desiredJSON(2, "", good, "mac-4F1C2A9E7B3D-1"),
		"short hex":          desiredJSON(2, "", good, "mac-4f1c2a9e7b3-1"),
		"gen zero":           desiredJSON(2, "", good, "mac-4f1c2a9e7b3d-0"),
		"gen 1000":           desiredJSON(2, "", good, "mac-4f1c2a9e7b3d-1000"),
		"shell metachar":     desiredJSON(2, "", good, "mac-4f1c2a9e7b3d-1;rm"),
		"duplicate":          desiredJSON(2, "", good, good),
		"top-level parent":   desiredJSON(2, `,"parent":"jm-gc"`, good, macName(2, 1)),
		"entry parent":       `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + macName(2, 1) + `","state":"active","parent":"jm-client"}]}`,
		"state not active":   `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + macName(2, 1) + `","state":"removing"}]}`,
		"escaped name":       `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"mac-4f1c2a9e7b3d-` + "\\u0031" + `","state":"active"}]}`,
		"no seq":             `{"keys":[{"name":"` + macName(2, 1) + `","state":"active"}]}`,
		"entry without name": `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"state":"active"}]}`,
		"garbage in keys":    `{"seq":2,"keys":[{"name":"` + good + `","state":"active"} x {"name":"` + macName(2, 1) + `","state":"active"}]}`,
		"second keys array":  `{"seq":2,"keys":[{"name":"` + good + `","state":"active"}],"keys":[]}`,
		"duplicate removal":  desiredJSON(2, removeJSON(good, good)),
		"wanted and removed": desiredJSON(2, removeJSON(good), good),
		"invalid removal":    desiredJSON(2, removeJSON("jm-gc"), good),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			r := newKeydRig(t)
			r.key(good, "jm-macs")
			r.key(macName(3, 1), "jm-macs") // absent from every desired set above
			r.desired(body)
			out := r.mustRun()
			for _, op := range []string{"add", "rm", "disable"} {
				if c := r.calls(op); len(c) != 0 {
					t.Fatalf("keyd ran %s despite a refused desired.json: %q\n%s", op, c, out)
				}
			}
			a := r.applied()
			if !strings.HasPrefix(a.Error, "refused desired.json") {
				t.Fatalf("applied.error = %q\n%s", a.Error, out)
			}
			if a.Seq != 0 {
				t.Fatalf("refused file advanced applied seq to %d", a.Seq)
			}
		})
	}
}

// K2: an empty or truncated desired set must not cut off every Mac.
func TestKeydMassDeleteGuard(t *testing.T) {
	r := newKeydRig(t)
	var all []string
	for i := 1; i <= 10; i++ {
		r.key(macName(i, 1), "jm-macs")
		all = append(all, macName(i, 1))
	}
	r.desired(desiredJSON(5, removeJSON(all...)))
	r.mustRun()
	if c := append(r.calls("rm"), r.calls("disable")...); len(c) != 0 {
		t.Fatalf("empty desired removed keys: %q", c)
	}
	if a := r.applied(); !strings.Contains(a.Error, "REFUSED mass delete of 10 of 10") || len(a.Keys) != 10 {
		t.Fatalf("applied = %+v", a)
	}

	// Four of ten (over MASS_MAX) is refused too.
	r.desired(desiredJSON(6, removeJSON(all[:4]...), all[4:]...))
	r.mustRun()
	if c := r.calls("rm"); len(c) != 0 {
		t.Fatalf("4-of-10 delete ran: %q", c)
	}

	// A single revoke always proceeds, even in a two-Mac fleet.
	r.desired(desiredJSON(7, removeJSON(all[0]), all[1:]...))
	r.mustRun()
	if c := r.calls("rm"); len(c) != 1 || r.exists(all[0]) {
		t.Fatalf("single revoke: rm calls %q", c)
	}

	// A confirmation from an earlier seq does not confirm this one: the
	// admin confirmed a different desired set.
	r.desired(desiredJSON(8, `,"allow_mass_delete":7`+removeJSON(all[1:]...)))
	r.mustRun()
	if c := r.calls("rm"); len(c) != 1 {
		t.Fatalf("allow_mass_delete 7 confirmed a seq 8 mass delete: rm calls %q", c)
	}
	if a := r.applied(); !strings.Contains(a.Error, "REFUSED mass delete of 9 of 9") {
		t.Fatalf("applied = %+v", a)
	}

	// Confirmed with allow_mass_delete == seq: everything goes.
	r.desired(desiredJSON(9, `,"allow_mass_delete":9`+removeJSON(all[1:]...)))
	r.mustRun()
	for _, n := range all {
		if r.exists(n) {
			t.Fatalf("%s survived a confirmed mass delete", n)
		}
	}
	// jm-keyadmin has no UpdateServiceAccount, so keyd never disables.
	if c := r.calls("disable"); len(c) != 0 {
		t.Fatalf("keyd called svcacct disable, which jm-keyadmin may not do: %q", c)
	}
}

func TestKeydPreservesOmittedNamesEvenWithMassFlag(t *testing.T) {
	r := newKeydRig(t)
	known, late := macName(1, 1), macName(2, 1)
	r.key(known, "jm-macs")
	r.key(late, "jm-macs") // appeared after the Manager's snapshot
	r.desired(desiredJSON(1, `,"allow_mass_delete":1`, known))
	r.mustRun()
	if !r.exists(late) || len(r.calls("rm")) != 0 {
		t.Fatal("omission or mass flag deleted an unobserved key")
	}
	if a := r.applied(); !strings.Contains(a.Error, "preserved") {
		t.Fatalf("unobserved key not reported: %+v", a)
	} else if k, ok := a.key(late); !ok || !k.Exists {
		t.Fatalf("unobserved key absent from applied: %+v", a)
	}
	r.desired(desiredJSON(2, removeJSON(late), known))
	r.mustRun()
	if r.exists(late) || !r.exists(known) {
		t.Fatal("named removal failed or deleted the wanted key")
	}
}

func TestKeydNamedMassRemovalPreservesUnobservedKey(t *testing.T) {
	r := newKeydRig(t)
	a, b, wanted, late := macName(1, 1), macName(2, 1), macName(3, 1), macName(4, 1)
	for _, n := range []string{a, b, wanted, late} {
		r.key(n, "jm-macs")
	}
	r.desired(desiredJSON(1, `,"allow_mass_delete":1`+removeJSON(a, b), wanted))
	r.mustRun()
	if r.exists(a) || r.exists(b) || !r.exists(wanted) || !r.exists(late) {
		t.Fatal("named mass removal did not preserve the unobserved key")
	}
	if c := r.calls("rm"); len(c) != 2 {
		t.Fatalf("removed %d keys, want the two named ones: %q", len(c), c)
	}
	if ap := r.applied(); !strings.Contains(ap.Error, "preserved") {
		t.Fatalf("orphan not reported: %+v", ap)
	}
}

// MASS_MAX: in a large fleet, more than 3 removals at once need a
// confirmation even when they are under 25% of the keys.
func TestKeydMassDeleteAbsoluteCap(t *testing.T) {
	r := newKeydRig(t)
	var all []string
	for i := 1; i <= 16; i++ {
		r.key(macName(i, 1), "jm-macs")
		all = append(all, macName(i, 1))
	}
	r.desired(desiredJSON(1, removeJSON(all[:4]...), all[4:]...)) // 4 of 16 = 25%, over the cap of 3
	r.mustRun()
	if c := r.calls("rm"); len(c) != 0 {
		t.Fatalf("4-of-16 delete ran without confirmation: %q", c)
	}
	if a := r.applied(); !strings.Contains(a.Error, "REFUSED mass delete of 4 of 16") {
		t.Fatalf("applied = %+v", a)
	}
	r.desired(desiredJSON(2, removeJSON(all[:3]...), all[3:]...)) // 3 of 16: allowed
	r.mustRun()
	if c := r.calls("rm"); len(c) != 3 {
		t.Fatalf("3-of-16 delete: rm calls %q", c)
	}
}

func TestKeydTwoMacFleetAllowsOneRevokeRefusesBoth(t *testing.T) {
	r := newKeydRig(t)
	r.key(macName(1, 1), "jm-macs")
	r.key(macName(2, 1), "jm-macs")
	r.desired(desiredJSON(1, removeJSON(macName(1, 1), macName(2, 1))))
	r.mustRun()
	if !r.exists(macName(1, 1)) || !r.exists(macName(2, 1)) {
		t.Fatal("2-of-2 delete was not refused")
	}
}

// K4: applied.json reports what MinIO says, not what keyd intended. A key
// MinIO places under jm-client shows parent jm-client, so the Manager's
// readiness check (parent == jm-macs) cannot pass.
func TestKeydAppliedFromInfoNotIntent(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.key(name, "jm-macs")
	if err := os.WriteFile(filepath.Join(r.state, "parent."+name), []byte("jm-client"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.state, "sa."+name), []byte("jm-macs off\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r.desired(desiredJSON(1, "", name, macName(2, 1)))
	r.mustRun(`FAKE_MC_BAD_ADD=`)
	a := r.applied()
	k, ok := a.key(name)
	if !ok || !k.Exists || k.Parent != "jm-client" || k.Status != "off" {
		t.Fatalf("applied did not report MinIO's view of %s: %+v", name, a)
	}
	if len(r.calls("info")) == 0 {
		t.Fatal("applied.json was written without asking MinIO (svcacct info)")
	}
}

// Desired keys whose creation failed are reported as absent, not present.
func TestKeydAppliedShowsFailedCreateAsAbsent(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.desired(desiredJSON(1, "", name))
	r.mustRun("FAKE_MC_BAD_ADD=1")
	if r.exists(name) {
		t.Fatal("a key whose secret was never returned was left in MinIO")
	}
	if _, err := os.Stat(filepath.Join(r.dir, "results", name+".json")); err == nil {
		t.Fatal("result written without a secret")
	}
	if k, ok := r.applied().key(name); !ok || k.Exists {
		t.Fatalf("applied = %+v", r.applied())
	}
}

func TestKeydRefusesLowerSeq(t *testing.T) {
	r := newKeydRig(t)
	a, b := macName(1, 1), macName(2, 1)
	r.desired(desiredJSON(5, "", a, b))
	r.mustRun()
	r.desired(desiredJSON(4, removeJSON(b), a)) // an older Manager writing a stale revoke
	r.mustRun()
	if !r.exists(b) || len(r.calls("rm")) != 0 {
		t.Fatal("a lower seq removed a key")
	}
	if got := r.applied(); got.Seq != 5 || !strings.Contains(got.Error, "lower than applied seq 5") {
		t.Fatalf("applied = %+v", got)
	}
}

// keyd removes only what MinIO reports under jm-macs.
func TestKeydWillNotRemoveForeignParentKey(t *testing.T) {
	r := newKeydRig(t)
	keep, foreign := macName(1, 1), macName(2, 1)
	r.key(keep, "jm-macs")
	r.key(foreign, "jm-macs")
	if err := os.WriteFile(filepath.Join(r.state, "parent."+foreign), []byte("jm-gc"), 0o644); err != nil {
		t.Fatal(err)
	}
	r.desired(desiredJSON(1, removeJSON(foreign), keep))
	out := r.mustRun()
	if !r.exists(foreign) || len(r.calls("rm"))+len(r.calls("disable")) != 0 {
		t.Fatalf("keyd removed a key whose parent is jm-gc:\n%s", out)
	}
	if !strings.Contains(out, "REFUSED removing "+foreign) {
		t.Fatalf("no refusal logged:\n%s", out)
	}
}

func TestKeydNoDesiredMeansNoChange(t *testing.T) {
	r := newKeydRig(t)
	r.key(macName(1, 1), "jm-macs")
	r.mustRun()
	if len(r.calls("rm"))+len(r.calls("add")) != 0 || !r.exists(macName(1, 1)) {
		t.Fatal("missing desired.json changed MinIO")
	}
	if a := r.applied(); len(a.Keys) != 1 || !a.Keys[0].Exists {
		t.Fatalf("applied = %+v", a)
	}
}

func TestKeydListFailureWritesNothing(t *testing.T) {
	r := newKeydRig(t)
	r.key(macName(1, 1), "jm-macs")
	r.desired(desiredJSON(1, removeJSON(macName(1, 1))))
	if out, err := r.run("FAKE_MC_FAIL_LS=1"); err == nil {
		t.Fatalf("cycle reported success when MinIO could not be listed:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(r.dir, "applied.json")); err == nil {
		t.Fatal("applied.json written without a verified listing")
	}
	if len(r.calls("rm")) != 0 {
		t.Fatal("removed keys without a listing")
	}
}

// §4 step 4: a key the Manager never got a secret for is re-created, but
// never while an unconsumed result exists.
func TestKeydRecreatesKeyThatNeedsSecret(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.key(name, "jm-macs")
	body := `{"seq":3,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`
	if err := os.MkdirAll(filepath.Join(r.dir, "results"), 0o700); err != nil {
		t.Fatal(err)
	}
	res := filepath.Join(r.dir, "results", name+".json")
	if err := os.WriteFile(res, []byte(`{"name":"x"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	r.desired(body)
	r.mustRun()
	if len(r.calls("rm")) != 0 {
		t.Fatal("re-created a key whose result is still unconsumed")
	}
	if err := os.Remove(res); err != nil {
		t.Fatal(err)
	}
	r.mustRun()
	if len(r.calls("rm")) != 1 || len(r.calls("add")) != 1 {
		t.Fatalf("rm=%q add=%q", r.calls("rm"), r.calls("add"))
	}
	if !strings.Contains(mustRead(t, res), readOpt(filepath.Join(r.state, "secret."+name))) {
		t.Fatal("new secret not delivered")
	}
}

// needs_secret is honoured once per desired seq. The Manager consumes the
// result between cycles but does not rewrite desired.json at once; keyd
// must not re-create the key it just delivered (that would invalidate the
// secret the Manager stored). A higher seq is a new request.
func TestKeydNeedsSecretRecreatesOncePerSeq(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.key(name, "jm-macs")
	r.desired(`{"seq":3,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`)
	res := filepath.Join(r.dir, "results", name+".json")
	var delivered []string
	for cycle := 1; cycle <= 3; cycle++ {
		r.mustRun() // each run is a new keyd process: the record is persisted
		if b, err := os.ReadFile(res); err == nil {
			delivered = append(delivered, string(b))
			if err := os.Remove(res); err != nil { // the Manager consumes it
				t.Fatal(err)
			}
		}
	}
	if rm, add := r.calls("rm"), r.calls("add"); len(rm) != 1 || len(add) != 1 {
		t.Fatalf("3 cycles at seq 3: rm=%d add=%d, want exactly one re-create\n%s", len(rm), len(add), r.argv())
	}
	if len(delivered) != 1 || !strings.Contains(delivered[0], readOpt(filepath.Join(r.state, "secret."+name))) {
		t.Fatalf("delivered %q; the live secret is not the one the Manager got", delivered)
	}
	// A new request re-creates again, once. The Manager took the result
	// above while the flag was still set, so (KEYD_PROTOCOL.md) it must
	// first write a desired.json without the flag; then a higher seq with
	// the flag is a new request.
	r.desired(`{"seq":4,"keys":[{"name":"` + name + `","state":"active"}]}`)
	r.mustRun()
	r.desired(`{"seq":5,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`)
	r.mustRun()
	_ = os.Remove(res)
	r.mustRun()
	if add := r.calls("add"); len(add) != 2 {
		t.Fatalf("seq 5 needs_secret: %d adds in total, want 2", len(add))
	}
}

// A stop (docker stop sends TERM to keyd) while MinIO is creating a key
// must not lose the secret: keyd finishes writing the result, then exits.
func TestKeydTermDuringAddKeepsSecret(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.desired(desiredJSON(1, "", name))
	cmd := exec.Command("/bin/bash", "keyd.sh")
	var out strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &out
	cmd.Env = []string{
		"PATH=/usr/bin:/bin", "TMPDIR=" + r.state, "MC=" + filepath.Join(r.state, "mc"),
		"FAKE_MC_STATE=" + r.state, "JM_OBJ_PER_MAC_SERVER=1", "JM_KEYD_INTERVAL=1",
		"JM_OBJKEYS_DIR=" + r.dir, "JM_MINIO_ENDPOINT=http://minio:9000",
		"JM_OBJ_KEYADMIN_SECRET_KEY=" + keyadminSecret, "FAKE_MC_ADD_SLEEP=2",
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(r.state, "adding")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("svcacct add never started:\n%s", out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("keyd exit: %v\n%s", err, out.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("keyd did not stop after TERM:\n%s", out.String())
	}
	if !r.exists(name) {
		t.Fatal("the key was not created")
	}
	b, err := os.ReadFile(filepath.Join(r.dir, "results", name+".json"))
	if err != nil || !strings.Contains(string(b), readOpt(filepath.Join(r.state, "secret."+name))) {
		t.Fatalf("TERM during svcacct add lost the secret of a live key (result: %q, %v)\n%s", b, err, out.String())
	}
	if !strings.Contains(out.String(), "stop requested while creating a key") {
		t.Fatalf("no log line for the held stop:\n%s", out.String())
	}
}

// keyd reads desired.json with patterns, so anything a JSON decoder could
// read differently is refused as a whole.
func TestKeydRefusesNonStrictJSON(t *testing.T) {
	good, other := macName(1, 1), macName(2, 1)
	bs := "\\"
	cases := map[string]string{
		"trailing comma in keys":   `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"},]}`,
		"trailing comma at top":    `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}],}`,
		"missing final brace":      `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}]`,
		"duplicate seq":            `{"seq":1,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}],"seq":2}`,
		"duplicate state in entry": `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active","state":"removing"}]}`,
		"duplicate name in entry":  `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","name":"` + macName(4, 1) + `","state":"active"}]}`,
		"nested object at top":     `{"seq":2,"x":{"allow_mass_delete":2},"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}]}`,
		"leading zero":             `{"seq":02,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}]}`,
		"data after the object":    `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active"}]} {}`,
		// Go decodes the escaped member as a second "state" (last wins:
		// "removing"); a pattern reader sees only "active".
		"escape in an unread member": `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},{"name":"` + other + `","state":"active","st` + bs + `u0061te":"removing"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			r := newKeydRig(t)
			r.key(good, "jm-macs")
			r.key(macName(3, 1), "jm-macs")
			r.desired(body)
			out := r.mustRun()
			for _, op := range []string{"add", "rm"} {
				if c := r.calls(op); len(c) != 0 {
					t.Fatalf("keyd ran %s for a non-strict desired.json: %q\n%s", op, c, out)
				}
			}
			a := r.applied()
			if !strings.HasPrefix(a.Error, "refused desired.json: not strict JSON") || a.Seq != 0 {
				t.Fatalf("applied = %+v\n%s", a, out)
			}
		})
	}
	// The escape case names the escape (it is the check that refuses it).
	r := newKeydRig(t)
	r.desired(cases["escape in an unread member"])
	r.mustRun()
	if a := r.applied(); !strings.Contains(a.Error, "escape sequences are not allowed") {
		t.Fatalf("applied = %+v", a)
	}
	// Whitespace and member order are fine.
	r = newKeydRig(t)
	r.desired("{\n\t\"keys\" : [ {\"state\":\"active\", \"name\":\"" + good + "\"} ],\n\t\"seq\" : 1\n}\n")
	r.mustRun()
	if !r.exists(good) {
		t.Fatalf("a strict, reformatted desired.json was refused: %+v", r.applied())
	}
}

// A key under jm-macs whose name keyd did not mint (made by hand, or by a
// later tool) is not keyd's: never removed, never counted.
func TestKeydLeavesUnmanagedNamesAlone(t *testing.T) {
	r := newKeydRig(t)
	keep := macName(1, 1)
	r.key(keep, "jm-macs")
	r.key("jmmanualkey0001", "jm-macs")
	r.desired(desiredJSON(1, "", keep))
	out := r.mustRun()
	if !r.exists("jmmanualkey0001") || len(r.calls("rm")) != 0 {
		t.Fatalf("keyd removed a key it does not manage:\n%s", out)
	}
	if _, ok := r.applied().key("jmmanualkey0001"); ok {
		t.Fatal("applied.json reports a key keyd does not manage")
	}
}

// The reply to svcacct add must name the key keyd asked for; otherwise the
// secret belongs to some other account and must not be delivered.
func TestKeydRefusesAddReplyForAnotherKey(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.desired(desiredJSON(1, "", name))
	out := r.mustRun("FAKE_MC_WRONG_AK=" + macName(9, 1))
	if _, err := os.Stat(filepath.Join(r.dir, "results", name+".json")); err == nil {
		t.Fatalf("delivered a secret from a reply for another key:\n%s", out)
	}
	if r.exists(name) {
		t.Fatal("the undeliverable key was left in MinIO")
	}
}

// The key admin secret reaches mc only inside MC_HOST_jmkeyd, never as its
// own variable in mc's (or any child's) environment.
func TestKeydUnsetsKeyAdminSecretVariable(t *testing.T) {
	r := newKeydRig(t)
	r.desired(desiredJSON(1, "", macName(1, 1)))
	r.mustRun()
	if l := readOpt(filepath.Join(r.state, "leaked-env")); l != "" {
		t.Fatalf("mc saw the secret variable: %s", l)
	}
}

// docker creates a missing bind-mount directory as 0755; keyd must close
// it to 0700 before writing secrets into it.
func TestKeydTightensExistingSharedDir(t *testing.T) {
	r := newKeydRig(t)
	if err := os.MkdirAll(filepath.Join(r.dir, "results"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{r.dir, filepath.Join(r.dir, "results")} {
		if err := os.Chmod(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	r.desired(desiredJSON(1, "", macName(1, 1)))
	r.mustRun()
	for _, d := range []string{r.dir, filepath.Join(r.dir, "results")} {
		if fi, err := os.Stat(d); err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode %v (err %v), want 0700", d, fi.Mode().Perm(), err)
		}
	}
}

// keyd holds the key admin credential only.
func TestKeydRefusesForeignCredentials(t *testing.T) {
	cases := map[string][]string{
		"root password":      {"MINIO_ROOT_PASSWORD=" + strings.Repeat("r", 24)},
		"gc secret":          {"JM_OBJ_GC_SECRET_KEY=" + strings.Repeat("g", 40)},
		"client secret":      {"JM_OBJ_CLIENT_SECRET_KEY=" + strings.Repeat("c", 40)},
		"admin is jm-macs":   {"JM_OBJ_KEYADMIN_ACCESS_KEY=jm-macs"},
		"admin is root user": {"JM_OBJ_KEYADMIN_ACCESS_KEY=juicemount"},
		"admin is jm-gc":     {"JM_OBJ_KEYADMIN_ACCESS_KEY=jm-gc"},
		"short secret":       {"JM_OBJ_KEYADMIN_SECRET_KEY=abc"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			r := newKeydRig(t)
			r.desired(desiredJSON(1, "", macName(1, 1)))
			if out, err := r.run(env...); err == nil {
				t.Fatalf("keyd started:\n%s", out)
			}
			if r.argv() != "" {
				t.Fatal("mc ran before the refusal")
			}
		})
	}
}

func TestKeydIdleWhenPerMacOff(t *testing.T) {
	r := newKeydRig(t)
	r.desired(desiredJSON(1, "", macName(1, 1)))
	out, err := r.run("JM_OBJ_PER_MAC_SERVER=0", "JM_OBJ_KEYADMIN_SECRET_KEY=")
	if err != nil || r.argv() != "" || !strings.Contains(out, "off") {
		t.Fatalf("err=%v argv=%q out=%s", err, r.argv(), out)
	}
}

// keyd is step M1 (JM_OBJ_PER_MAC_SERVER). The Manager's switch
// (JM_OBJ_PER_MAC, step M2) does not start it, and a value that is not
// exactly 1 or 0 is reported, not guessed.
func TestKeydSwitchIsStrictAndSeparateFromManager(t *testing.T) {
	cases := map[string][]string{
		"manager switch only": {"JM_OBJ_PER_MAC_SERVER=", "JM_OBJ_PER_MAC=1"},
		"true":                {"JM_OBJ_PER_MAC_SERVER=true"},
		"yes":                 {"JM_OBJ_PER_MAC_SERVER=yes"},
		"space one":           {"JM_OBJ_PER_MAC_SERVER= 1"},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			r := newKeydRig(t)
			r.desired(desiredJSON(1, "", macName(1, 1)))
			out, err := r.run(env...)
			if err != nil || r.argv() != "" || !strings.Contains(out, "idle") {
				t.Fatalf("err=%v argv=%q out=%s", err, r.argv(), out)
			}
			if name != "manager switch only" && !strings.Contains(out, "is not 1 or 0") {
				t.Fatalf("an invalid value was not reported:\n%s", out)
			}
		})
	}
}

// keydLoop is keyd running as the daemon does (no JM_KEYD_ONCE), so state
// carried between cycles in one process (the parse cache, the hold) is
// exercised.
type keydLoop struct {
	r    *keydRig
	cmd  *exec.Cmd
	out  *strings.Builder
	done chan error
}

func (r *keydRig) startLoop(env ...string) *keydLoop {
	r.t.Helper()
	l := &keydLoop{r: r, out: &strings.Builder{}, done: make(chan error, 1)}
	l.cmd = exec.Command("/bin/bash", "keyd.sh")
	l.cmd.Stdout, l.cmd.Stderr = l.out, l.out
	l.cmd.Env = append([]string{
		"PATH=/usr/bin:/bin", "TMPDIR=" + r.state, "MC=" + filepath.Join(r.state, "mc"),
		"FAKE_MC_STATE=" + r.state, "JM_OBJ_PER_MAC_SERVER=1", "JM_KEYD_INTERVAL=1",
		"JM_OBJKEYS_DIR=" + r.dir, "JM_MINIO_ENDPOINT=http://minio:9000",
		"JM_OBJ_KEYADMIN_SECRET_KEY=" + keyadminSecret,
	}, env...)
	if err := l.cmd.Start(); err != nil {
		r.t.Fatal(err)
	}
	go func() { l.done <- l.cmd.Wait() }()
	r.t.Cleanup(func() {
		if l.cmd.ProcessState == nil {
			_ = l.cmd.Process.Kill()
		}
	})
	return l
}

// waitFor polls cond until it holds or the deadline passes.
func (l *keydLoop) waitFor(what string, d time.Duration, cond func() bool) {
	l.r.t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			_ = l.cmd.Process.Kill()
			l.r.t.Fatalf("timed out waiting for %s:\n%s", what, l.out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// cycles waits for n more completed cycles. Every cycle starts with
// svcacct ls, so n+1 new ls calls mean n cycles ran to the end.
func (l *keydLoop) cycles(n int) {
	l.r.t.Helper()
	start := len(l.r.calls("ls"))
	l.waitFor(fmt.Sprintf("%d keyd cycles", n), time.Duration(n+1)*20*time.Second, func() bool {
		return len(l.r.calls("ls")) >= start+n+1
	})
}

// stop sends TERM and returns how long keyd took to exit.
func (l *keydLoop) stop(limit time.Duration) time.Duration {
	l.r.t.Helper()
	start := time.Now()
	if err := l.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		l.r.t.Fatal(err)
	}
	select {
	case err := <-l.done:
		if err != nil {
			l.r.t.Fatalf("keyd exit: %v\n%s", err, l.out.String())
		}
	case <-time.After(limit):
		_ = l.cmd.Process.Kill()
		l.r.t.Fatalf("keyd did not exit within %v of TERM:\n%s", limit, l.out.String())
	}
	return time.Since(start)
}

// desiredDoc is the shape the Manager (L2) marshals; Label and Note stand
// for any extra text field it may add.
type desiredDoc struct {
	Seq             uint64           `json:"seq"`
	StoreCount      int              `json:"store_count"`
	AllowMassDelete uint64           `json:"allow_mass_delete,omitempty"`
	Note            string           `json:"note,omitempty"`
	Keys            []desiredKeySpec `json:"keys"`
	Remove          []string         `json:"remove,omitempty"`
}

type desiredKeySpec struct {
	Name        string `json:"name"`
	State       string `json:"state"`
	NeedsSecret bool   `json:"needs_secret,omitempty"`
	Label       string `json:"label,omitempty"`
}

func marshalDesired(t *testing.T, d desiredDoc, indent bool) string {
	t.Helper()
	var b []byte
	var err error
	if indent {
		b, err = json.MarshalIndent(d, "", "  ")
	} else {
		b, err = json.Marshal(d)
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// Finding A: whatever Go's json.Marshal writes for this shape is accepted:
// extra text fields Go escapes (& < >), quotes, brackets inside strings,
// UTF-8, indented output, and in any locale.
func TestKeydAcceptsGoMarshalledDesired(t *testing.T) {
	a, b := macName(1, 1), macName(2, 1)
	doc := desiredDoc{Seq: 7, StoreCount: 2, Note: `admin "Leland" <ops> & co`, Keys: []desiredKeySpec{
		{Name: a, State: "active", Label: `Bob & Alice's <Mac> "studio" x}{][`},
		{Name: b, State: "active", Label: "Leland’s MacBook Pro — 日本 \\ \t"},
	}}
	for _, indent := range []bool{false, true} {
		for _, loc := range []string{"C", "en_US.UTF-8"} {
			t.Run(fmt.Sprintf("indent=%v/%s", indent, loc), func(t *testing.T) {
				r := newKeydRig(t)
				body := marshalDesired(t, doc, indent)
				if !strings.Contains(body, `\u0026`) || !strings.Contains(body, `\"`) {
					t.Fatalf("the fixture lost its escapes: %s", body)
				}
				r.desired(body)
				out := r.mustRun("LANG="+loc, "LC_ALL="+loc)
				if !r.exists(a) || !r.exists(b) {
					t.Fatalf("a json.Marshal desired.json was not applied:\n%s\n%s", body, out)
				}
				if ap := r.applied(); ap.Error != "" || ap.Seq != 7 {
					t.Fatalf("applied = %+v", ap)
				}
			})
		}
	}
}

// Finding A: Go marshals an empty (nil) list as null. Revoking the last Mac
// must remove its key; the mass-delete guard still applies to null.
func TestKeydNullKeysIsAnEmptyList(t *testing.T) {
	r := newKeydRig(t)
	last := macName(1, 1)
	r.key(last, "jm-macs")
	body := marshalDesired(t, desiredDoc{Seq: 3, Remove: []string{last}}, false)
	if !strings.Contains(body, `"keys":null`) {
		t.Fatalf("fixture: %s", body)
	}
	r.desired(body)
	out := r.mustRun()
	if r.exists(last) || len(r.calls("rm")) != 1 {
		t.Fatalf("revoking the last Mac (keys:null) did not remove its key:\n%s", out)
	}
	if a := r.applied(); a.Error != "" || a.Seq != 3 {
		t.Fatalf("applied = %+v", a)
	}

	r = newKeydRig(t)
	var all []string
	for i := 1; i <= 10; i++ {
		n := macName(i, 1)
		r.key(n, "jm-macs")
		all = append(all, n)
	}
	r.desired(marshalDesired(t, desiredDoc{Seq: 3, Remove: all}, false))
	r.mustRun()
	if c := r.calls("rm"); len(c) != 0 {
		t.Fatalf("keys:null removed %d of 10 keys without allow_mass_delete", len(c))
	}
	if a := r.applied(); !strings.Contains(a.Error, "REFUSED mass delete of 10 of 10") {
		t.Fatalf("applied = %+v", a)
	}
}

// A field keyd reads must be read exactly as Go reads it: no escape in its
// value or name, and no case variant (Go matches member names without
// regard to case, so "Seq" or "STATE" would set the field).
func TestKeydRefusesAmbiguousReadFields(t *testing.T) {
	good, other := macName(1, 1), macName(2, 1)
	e := func(entry string) string {
		return `{"seq":2,"keys":[{"name":"` + good + `","state":"active"},` + entry + `]}`
	}
	cases := map[string]string{
		"case variant at top":      `{"seq":2,"Seq":9,"keys":[{"name":"` + good + `","state":"active"}]}`,
		"case variant in entry":    e(`{"name":"` + other + `","state":"active","STATE":"removing"}`),
		"case variant of keys":     `{"seq":2,"keys":[{"name":"` + good + `","state":"active"}],"KEYS":[]}`,
		"escape in state":          e(`{"name":"` + other + `","state":"activ\u0065"}`),
		"escape in parent":         e(`{"name":"` + other + `","state":"active","parent":"jm-mac\u0073"}`),
		"escape in a member name":  e(`{"name":"` + other + `","state":"active","n\u0061me":"` + macName(4, 1) + `"}`),
		"non-ASCII member name":    e(`{"name":"` + other + `","state":"active","ſtate":"removing"}`),
		"needs_secret not a bool":  e(`{"name":"` + other + `","state":"active","needs_secret":"true"}`),
		"seq with a fraction":      `{"seq":2.0,"keys":[{"name":"` + good + `","state":"active"}]}`,
		"keys an object":           `{"seq":2,"keys":{}}`,
		"raw control char in note": `{"seq":2,"note":"a` + "\x01" + `b","keys":[{"name":"` + good + `","state":"active"}]}`,
		// bash drops NUL bytes when it reads a file; Go refuses them.
		"NUL byte": `{"seq":2,"keys":[{"name":"` + good + `","state":"active"}]}` + "\x00",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			r := newKeydRig(t)
			r.key(good, "jm-macs")
			r.key(macName(3, 1), "jm-macs")
			r.desired(body)
			out := r.mustRun()
			if c := append(r.calls("add"), r.calls("rm")...); len(c) != 0 {
				t.Fatalf("keyd acted on an ambiguous desired.json: %q\n%s", c, out)
			}
			if a := r.applied(); !strings.HasPrefix(a.Error, "refused desired.json") || a.Seq != 0 {
				t.Fatalf("applied = %+v\n%s", a, out)
			}
		})
	}
}

// Finding M5: the parse cache must never hold a refused file. In one keyd
// process, a refused file stays refused on every cycle, and a refused
// file that follows an accepted one does not reuse the accepted result.
func TestKeydLoopNeverActsOnRefusedFile(t *testing.T) {
	r := newKeydRig(t)
	old, a, b := macName(1, 1), macName(2, 1), macName(3, 1)
	r.key(old, "jm-macs")
	// If accepted, this would add a and remove old.
	r.desired(`{"seq":2,"keys":[{"name":"` + a + `","state":"active"},]}`)
	l := r.startLoop()
	l.cycles(4)
	if c := append(r.calls("add"), r.calls("rm")...); len(c) != 0 {
		t.Fatalf("a refused desired.json was applied on a later cycle: %q\n%s", c, l.out.String())
	}
	r.desired(`{"seq":3,"keys":[{"name":"` + old + `","state":"active"},{"name":"` + a + `","state":"active"}]}`)
	l.waitFor("the valid file to be applied", 15*time.Second, func() bool { return r.exists(a) })
	l.cycles(1)
	r.desired(`{"seq":4,"keys":[{"name":"` + b + `","state":"active"},]}`)
	l.cycles(4)
	if r.exists(b) || !r.exists(old) || !r.exists(a) || len(r.calls("rm")) != 0 {
		t.Fatalf("a refused desired.json after an accepted one was applied:\n%s\n%s", r.argv(), l.out.String())
	}
	if ap := r.applied(); !strings.HasPrefix(ap.Error, "refused desired.json: not strict JSON") || ap.Seq != 3 {
		t.Fatalf("applied = %+v", ap)
	}
	l.stop(10 * time.Second)
}

// Finding M6: recording a new create must keep every other key's record.
// Losing it makes keyd re-create a key whose secret was already delivered.
func TestKeydSeqRecordKeepsOtherKeys(t *testing.T) {
	r := newKeydRig(t)
	a, b := macName(1, 1), macName(2, 1)
	r.key(a, "jm-macs")
	r.desired(`{"seq":3,"keys":[{"name":"` + a + `","state":"active","needs_secret":true},{"name":"` + b + `","state":"active"}]}`)
	res := filepath.Join(r.dir, "results", a+".json")
	r.mustRun("FAKE_MC_FAIL_ADD=" + b) // a re-created; b fails once
	if !r.exists(a) || r.exists(b) {
		t.Fatalf("setup: a=%v b=%v", r.exists(a), r.exists(b))
	}
	if err := os.Remove(res); err != nil { // the Manager took a's secret
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		r.mustRun()
		_ = os.Remove(res)
	}
	if !r.exists(b) {
		t.Fatal("b was never created")
	}
	var addsA int
	for _, c := range r.calls("add") {
		if strings.HasSuffix(c, " "+a) {
			addsA++
		}
	}
	if addsA != 1 {
		t.Fatalf("a (delivered at seq 3) was created %d times; recording b lost a's record:\n%s\nseq file:\n%s",
			addsA, r.argv(), readOpt(filepath.Join(r.dir, "keyd-created-seq")))
	}
}

// Finding M8: a stop during the re-create of a needs_secret key keeps the
// new secret, as on the first create.
func TestKeydTermDuringRecreateKeepsSecret(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.key(name, "jm-macs")
	r.desired(`{"seq":3,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`)
	l := r.startLoop("FAKE_MC_ADD_SLEEP=2")
	l.waitFor("svcacct add", 10*time.Second, func() bool { return fileExists(filepath.Join(r.state, "adding")) })
	l.stop(15 * time.Second)
	if len(r.calls("rm")) != 1 || len(r.calls("add")) != 1 {
		t.Fatalf("the re-create did not run: %q", r.argv())
	}
	if !r.exists(name) {
		t.Fatalf("TERM during a re-create left no key: the old key was removed and the new create abandoned\n%s", l.out.String())
	}
	b, err := os.ReadFile(filepath.Join(r.dir, "results", name+".json"))
	if err != nil || !strings.Contains(string(b), readOpt(filepath.Join(r.state, "secret."+name))) {
		t.Fatalf("TERM during a re-create lost the new secret of a live key (result %q, %v)\n%s", b, err, l.out.String())
	}
}

// Finding B: the held stop is bounded below docker's stop_grace_period. A
// reply that never comes is given up, the key MinIO made is removed (it
// can never be delivered), and keyd exits.
func TestKeydStopHoldIsBounded(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.desired(desiredJSON(1, "", name))
	l := r.startLoop("FAKE_MC_ADD_HANG_AFTER=40", "JM_KEYD_HOLD_MAX=2")
	l.waitFor("svcacct add", 10*time.Second, func() bool { return fileExists(filepath.Join(r.state, "adding")) })
	took := l.stop(15 * time.Second)
	if took > 9*time.Second {
		t.Fatalf("keyd took %v to stop with a 2 s hold bound", took)
	}
	if !strings.Contains(l.out.String(), "GAVE UP") {
		t.Fatalf("no loud log line for the abandoned create:\n%s", l.out.String())
	}
	if r.exists(name) {
		t.Fatalf("the undeliverable key was left in MinIO:\n%s", l.out.String())
	}
	if fileExists(filepath.Join(r.dir, "results", name+".json")) {
		t.Fatal("a result was written for an abandoned create")
	}
}

// Finding C: the Manager took a re-created key's secret but left
// needs_secret set; a later seq raise for another reason must not
// re-create the key (that would kill the delivered secret). After the
// Manager clears the flag, a new request works again.
func TestKeydIgnoresStaleNeedsSecretAfterDelivery(t *testing.T) {
	r := newKeydRig(t)
	a, other := macName(1, 1), macName(2, 1)
	r.key(a, "jm-macs")
	r.key(other, "jm-macs")
	res := filepath.Join(r.dir, "results", a+".json")
	flagged := func(seq int, names ...string) string {
		var ks []string
		for _, n := range names {
			ks = append(ks, `{"name":"`+n+`","state":"active"`+map[bool]string{true: `,"needs_secret":true`}[n == a]+`}`)
		}
		return fmt.Sprintf(`{"seq":%d,"keys":[%s]}`, seq, strings.Join(ks, ","))
	}
	r.desired(flagged(3, a, other))
	r.mustRun()
	if len(r.calls("add")) != 1 {
		t.Fatalf("setup: %q", r.calls("add"))
	}
	delivered := mustRead(t, res)
	if err := os.Remove(res); err != nil { // taken, flag left set
		t.Fatal(err)
	}
	r.mustRun()
	r.desired(strings.TrimSuffix(flagged(4, a), "}") + removeJSON(other) + "}") // revoke other
	r.mustRun()
	if n := len(r.calls("add")); n != 1 || r.exists(other) {
		t.Fatalf("a stale needs_secret on a seq raise re-created a delivered key (%d adds)\n%s", n, r.argv())
	}
	if !strings.Contains(delivered, readOpt(filepath.Join(r.state, "secret."+a))) {
		t.Fatal("the delivered secret is no longer the live one")
	}
	if ap := r.applied(); !strings.Contains(ap.Error, "needs_secret for "+a+" ignored") {
		t.Fatalf("the ignored flag is not reported: %+v", ap)
	}
	// Cleared, then requested again: honoured once.
	r.desired(`{"seq":5,"keys":[{"name":"` + a + `","state":"active"}]}`)
	r.mustRun()
	if ap := r.applied(); ap.Error != "" {
		t.Fatalf("applied = %+v", ap)
	}
	r.desired(flagged(6, a))
	r.mustRun()
	if n := len(r.calls("add")); n != 2 {
		t.Fatalf("a new request after the flag was cleared: %d adds, want 2", n)
	}
}

// Finding F: a directory at a file keyd writes is reported, not swallowed
// by mv.
func TestKeydReportsDirectoryAtWriteTarget(t *testing.T) {
	t.Run("keyd-created-seq", func(t *testing.T) {
		r := newKeydRig(t)
		if err := os.MkdirAll(filepath.Join(r.dir, "keyd-created-seq"), 0o700); err != nil {
			t.Fatal(err)
		}
		r.desired(desiredJSON(1, "", macName(1, 1)))
		r.mustRun()
		if a := r.applied(); !strings.Contains(a.Error, "keyd-created-seq is a directory") {
			t.Fatalf("applied = %+v", a)
		}
		if ents, _ := os.ReadDir(filepath.Join(r.dir, "keyd-created-seq")); len(ents) != 0 {
			t.Fatalf("keyd wrote into the directory: %v", ents)
		}
	})
	t.Run("result file", func(t *testing.T) {
		r := newKeydRig(t)
		name := macName(1, 1)
		if err := os.MkdirAll(filepath.Join(r.dir, "results", name+".json"), 0o700); err != nil {
			t.Fatal(err)
		}
		r.desired(desiredJSON(1, "", name))
		r.mustRun()
		if r.exists(name) {
			t.Fatal("the undeliverable key was left in MinIO")
		}
		if a := r.applied(); !strings.Contains(a.Error, name+".json is a directory") {
			t.Fatalf("applied = %+v", a)
		}
	})
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// The seq gate on its own: a key recorded as created at seq 3 with no flag
// (a record from the keyd version before flags, or a flag that could not
// be saved) is not re-created by needs_secret at the same seq 3.
func TestKeydSeqGateAloneBlocksSameSeq(t *testing.T) {
	r := newKeydRig(t)
	name := macName(1, 1)
	r.key(name, "jm-macs")
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.dir, "keyd-created-seq"), []byte(name+" 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r.desired(`{"seq":3,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`)
	r.mustRun()
	if c := append(r.calls("rm"), r.calls("add")...); len(c) != 0 {
		t.Fatalf("needs_secret at the recorded seq re-created the key: %q", c)
	}
	r.desired(`{"seq":4,"keys":[{"name":"` + name + `","state":"active","needs_secret":true}]}`)
	r.mustRun()
	if len(r.calls("add")) != 1 {
		t.Fatalf("seq 4 is a new request: adds %q", r.calls("add"))
	}
}
