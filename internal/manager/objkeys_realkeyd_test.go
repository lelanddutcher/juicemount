package manager

// End to end with the REAL minio-keyd (server/minio/keyd.sh) and a fake
// `mc` that keeps service accounts as files: the Manager's desired.json is
// read by keyd's own strict parser, and keyd's results/ and applied.json by
// the Manager. This is the protocol cross-check the Go fake cannot give.

import (
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// realKeydMC: `mc admin user svcacct {ls,info,add,rm}`. sa.<name> holds
// "<parent> <status>". Secrets contain '+' and '/', as MinIO's may.
const realKeydMC = `#!/bin/bash
st="$FAKE_MC_STATE"
printf '%s\n' "$*" >> "$st/argv"
[ "$1" = --json ] && shift
[ "$1 $2 $3" = "admin user svcacct" ] || { echo "fake mc: unexpected $*" >&2; exit 9; }
op="$4"; alias="$5"; shift 5
err() { printf '{"status":"error","error":{"message":"%s"}}\n' "$1"; exit 1; }
case "$op" in
ls)
  for f in "$st"/sa.*; do
    [ -e "$f" ] || continue
    read -r p s < "$f"
    [ "$p" = "$1" ] || continue
    printf '{"status":"success","accessKey":"%s"}\n' "${f##*/sa.}"
  done ;;
info)
  [ -e "$st/sa.$1" ] || err "The specified service account is not found"
  read -r p s < "$st/sa.$1"
  printf '{"status":"success","accessKey":"%s","parentUser":"%s","accountStatus":"%s","impliedPolicy":true}\n' "$1" "$p" "$s" ;;
add)
  parent="$1"; shift; ak=''
  while [ $# -gt 0 ]; do
    case "$1" in --access-key) ak="$2"; shift 2 ;; *) err "flag $1" ;; esac
  done
  [ -e "$st/sa.$ak" ] && err "access key already exists"
  n=$(( $(cat "$st/n" 2>/dev/null || echo 0) + 1 )); echo "$n" > "$st/n"
  sk="Ab+/$(printf '%036d' "$n")"
  echo "$parent on" > "$st/sa.$ak"
  printf '%s' "$sk" > "$st/secret.$ak"
  echo "$ak" >> "$st/created"
  printf '{"status":"success","accessKey":"%s","secretKey":"%s","accountStatus":"on"}\n' "$ak" "$sk" ;;
rm)
  [ -e "$st/sa.$1" ] || err "not found"
  rm -f "$st/sa.$1" "$st/secret.$1" ;;
*) err "op $op" ;;
esac
`

type realKeyd struct {
	t       *testing.T
	state   string
	dir     string
	mu      sync.Mutex
	stop    chan struct{}
	exited  chan struct{}
	lastOut string
}

func newRealKeyd(t *testing.T, dir string) *realKeyd {
	t.Helper()
	if _, err := os.Stat("../../server/minio/keyd.sh"); err != nil {
		t.Skipf("keyd.sh not found: %v", err)
	}
	if _, err := exec.LookPath("/bin/bash"); err != nil {
		t.Skip("no /bin/bash")
	}
	k := &realKeyd{t: t, state: t.TempDir(), dir: dir}
	if err := os.WriteFile(filepath.Join(k.state, "mc"), []byte(realKeydMC), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(k.down)
	return k
}

func (k *realKeyd) once() {
	cmd := exec.Command("/bin/bash", "../../server/minio/keyd.sh")
	cmd.Env = []string{
		"PATH=/usr/bin:/bin", "TMPDIR=" + k.state, "MC=" + filepath.Join(k.state, "mc"),
		"FAKE_MC_STATE=" + k.state, "JM_OBJ_PER_MAC_SERVER=1", "JM_KEYD_ONCE=1",
		"JM_OBJKEYS_DIR=" + k.dir, "JM_MINIO_ENDPOINT=http://minio:9000",
		"JM_OBJ_KEYADMIN_SECRET_KEY=KeyAdminSecret0123456789abcdefABCDEF",
	}
	out, err := cmd.CombinedOutput()
	k.mu.Lock()
	k.lastOut = string(out)
	k.mu.Unlock()
	if err != nil && !strings.Contains(string(out), "failed") {
		k.t.Logf("keyd.sh: %v\n%s", err, out)
	}
}

func (k *realKeyd) up() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.stop != nil {
		return
	}
	k.stop, k.exited = make(chan struct{}), make(chan struct{})
	stop, exited := k.stop, k.exited
	go func() {
		defer close(exited)
		for {
			k.once()
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
}

func (k *realKeyd) down() {
	k.mu.Lock()
	stop, exited := k.stop, k.exited
	k.stop, k.exited = nil, nil
	k.mu.Unlock()
	if stop != nil {
		close(stop)
		<-exited
	}
}

func (k *realKeyd) exists(name string) bool { return fileExists(filepath.Join(k.state, "sa."+name)) }

func (k *realKeyd) secret(name string) string {
	b, _ := os.ReadFile(filepath.Join(k.state, "secret."+name))
	return string(b)
}

func (k *realKeyd) argv() string {
	b, _ := os.ReadFile(filepath.Join(k.state, "argv"))
	return string(b)
}

// The Manager (HTTP, JM_OBJ_PER_MAC=1) and the real keyd.sh: approve
// delivers the Mac's own key (secret with '+' and '/') through the poll,
// revoke removes it from "MinIO", Forget waits for that.
func TestObjKeysEndToEndWithRealKeyd(t *testing.T) {
	shortObjKeyTimers(t)
	objKeyWait = 5 * time.Second // each keyd.sh cycle forks bash + mc
	mux, proxyDir, _, _ := newObjAPI(t)
	kd := newRealKeyd(t, os.Getenv("JM_OBJKEYS_DIR"))

	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	key := er.Principal + "-1"

	// keyd not running: 409, nothing created.
	objKeyWait = 300 * time.Millisecond
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusConflict {
		t.Fatalf("approve with keyd stopped: %d %s", rr.Code, rr.Body)
	}
	objKeyWait = 5 * time.Second

	kd.up()
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		kd.mu.Lock()
		out := kd.lastOut
		kd.mu.Unlock()
		t.Fatalf("approve with keyd.sh running: %d %s\nkeyd: %s", rr.Code, rr.Body, out)
	}
	if !kd.exists(key) {
		t.Fatalf("keyd.sh did not create %s", key)
	}
	poll := map[string]string{"instance_id": testInstanceID, "poll_token": er.PollToken}
	got := decodeEnroll(t, doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", poll).Body.Bytes())
	if got.ObjectAccessKey != key || got.ObjectSecretKey != kd.secret(key) || !strings.ContainsAny(got.ObjectSecretKey, "+/") {
		t.Fatalf("poll = %s/%q, want %s/%q", got.ObjectAccessKey, got.ObjectSecretKey, key, kd.secret(key))
	}
	if got.ObjectAccessKey == testClientAK {
		t.Fatal("shared key delivered")
	}
	if strings.Contains(kd.argv(), "--secret-key") || strings.Contains(kd.argv(), got.ObjectSecretKey) {
		t.Fatalf("a secret reached mc argv:\n%s", kd.argv())
	}
	// keyd's applied.json accepted every desired.json the Manager wrote.
	if a, _ := os.ReadFile(filepath.Join(os.Getenv("JM_OBJKEYS_DIR"), "applied.json")); strings.Contains(string(a), "refused") {
		t.Fatalf("keyd.sh refused the Manager's desired.json: %s", a)
	}
	if fileExists(filepath.Join(os.Getenv("JM_OBJKEYS_DIR"), "results", key+".json")) {
		t.Fatal("result file not consumed")
	}

	kd.down()
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		t.Fatalf("revoke with keyd down: %d %s", rr.Code, rr.Body)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, filepath.Join(proxyDir, proxyPrincipalsName)), er.Principal); ok {
		t.Fatal("revoked Mac still in the proxy file")
	}
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/forget", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusConflict {
		t.Fatalf("forget before keyd removed the key: %d %s", rr.Code, rr.Body)
	}
	kd.up()
	deadline := time.Now().Add(10 * time.Second)
	for {
		rr := doJSON(t, mux, http.MethodPost, "/api/principals/forget", h8bTestAdminKey, map[string]string{"name": er.Principal})
		if rr.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forget never allowed: %d %s", rr.Code, rr.Body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if kd.exists(key) {
		t.Fatal("forget allowed while keyd.sh had not removed the key")
	}
}
