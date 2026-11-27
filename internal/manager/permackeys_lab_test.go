package manager

// Per-Mac object keys against a real object store (test/per-mac-keys-lab.sh
// starts SILO, provisions it with JM_OBJ_PER_MAC_SERVER=1, runs the real
// minio-keyd in its own container and sets the variables; without them the
// test is skipped).
//
// The Manager side is the real PrincipalStore with per-Mac keys on and the
// production timers (keyd and the reconciler both every 2 s, approve waits
// at most 10 s). A Mac is played by the test: it keeps the key its poll
// carried and makes signed S3 requests with it.
//
// Each check prints one "LAB |" row: gap, check, target, measured value.
// A row that misses its target fails the test.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

type pmLab struct {
	t                   *testing.T
	bucketURL, endpoint string
	ctl, objDir         string
	statePath, proxyDir string
	sharedSK            string
	seen                *os.File
	s                   *PrincipalStore
	stopRec             func()
	ip                  int
	failed              int
}

func TestPerMacKeyLab(t *testing.T) {
	l := &pmLab{t: t, bucketURL: os.Getenv("JM_LAB_BUCKET_URL"), endpoint: os.Getenv("JM_LAB_ENDPOINT"),
		ctl: os.Getenv("JM_LAB_CTL"), objDir: os.Getenv("JM_LAB_OBJKEYS_DIR"), sharedSK: os.Getenv("JM_OBJ_CLIENT_SECRET_KEY")}
	if l.bucketURL == "" || l.ctl == "" || l.objDir == "" || l.sharedSK == "" {
		t.Skip("lab only: run test/per-mac-keys-lab.sh")
	}
	trials := 20
	if n, err := strconv.Atoi(os.Getenv("JM_LAB_TRIALS")); err == nil && n > 0 {
		trials = n
	}
	if p := os.Getenv("JM_LAB_SEEN"); p != "" {
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		l.seen = f
	}
	dir := t.TempDir()
	l.statePath = filepath.Join(dir, "store", "principals-store.json")
	l.proxyDir = filepath.Join(dir, "respproxy")
	if err := os.MkdirAll(l.proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, l.proxyDir)
	l.waitKeyd()
	l.open()
	defer l.close()

	macs := l.g1g5(trials)
	l.g2()
	l.g3(macs[0])
	l.g4(macs)
	l.g6KeydStopped(trials)
	l.g6SiloRestart()
	l.g6StoreRestored()
	l.g6FilesLost()
	if l.failed > 0 {
		t.Errorf("%d LAB row(s) missed their target", l.failed)
	}
}

// ---- rig ---------------------------------------------------------------------

func (l *pmLab) row(gap, check, want, got string, ok bool) {
	mark := "PASS"
	if !ok {
		mark = "FAIL"
		l.failed++
	}
	l.t.Logf("LAB | %-3s | %-62s | want %-24s | got %-28s | %s", gap, check, want, got, mark)
}

func (l *pmLab) note(gap, msg string) { l.t.Logf("LAB | %-3s | note: %s", gap, msg) }

func (l *pmLab) open() {
	l.t.Helper()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: l.statePath, ProxyDir: l.proxyDir,
		PerMacKeys: true, ObjKeysDir: l.objDir, ObjectKey: func() (string, string) { return "jm-client", l.sharedSK }})
	if err != nil {
		l.t.Fatalf("open store: %v", err)
	}
	l.s = s
	l.stopRec = s.StartObjKeyReconciler()
}

// openErr: open, but report the error instead of failing.
func (l *pmLab) openErr() error {
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: l.statePath, ProxyDir: l.proxyDir,
		PerMacKeys: true, ObjKeysDir: l.objDir, ObjectKey: func() (string, string) { return "jm-client", l.sharedSK }})
	if err != nil {
		return err
	}
	l.s = s
	l.stopRec = s.StartObjKeyReconciler()
	return nil
}

func (l *pmLab) close() {
	if l.stopRec != nil {
		l.stopRec()
		l.stopRec = nil
	}
	if l.s != nil {
		waitAutoIdle(l.t, l.s)
	}
}

func (l *pmLab) ctlRun(args ...string) (string, error) {
	return l.ctlIn("", args...)
}

func (l *pmLab) ctlIn(stdin string, args ...string) (string, error) {
	cmd := exec.Command(l.ctl, args...)
	cmd.Stdin = strings.NewReader(stdin)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (l *pmLab) waitKeyd() {
	l.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if fileExists(filepath.Join(l.objDir, objKeyAppliedName)) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	out, _ := l.ctlRun("keyd-log")
	l.t.Fatalf("minio-keyd wrote no applied.json in 30 s:\n%s", out)
}

func (l *pmLab) nextIP() string {
	l.ip++
	return fmt.Sprintf("10.%d.%d.%d", 90+l.ip/65536%100, l.ip/256%256, l.ip%256)
}

func (l *pmLab) enroll(i int) (string, string) {
	l.t.Helper()
	resp, err := l.s.Enroll(EnrollRequest{InstanceID: instanceN(1000 + i), Host: fmt.Sprintf("Lab-Mac-%d", i),
		Secret: fmt.Sprintf("%040d", 7000+i)}, l.nextIP())
	if err != nil {
		l.t.Fatalf("enroll %d: %v", i, err)
	}
	return resp.Principal, resp.PollToken
}

func (l *pmLab) poll(i int, tok string) EnrollResponse {
	l.t.Helper()
	resp, err := l.s.Enroll(EnrollRequest{InstanceID: instanceN(1000 + i), PollToken: tok}, l.nextIP())
	if err != nil {
		l.t.Fatalf("poll %d: %v", i, err)
	}
	if resp.ObjectSecretKey != "" && l.seen != nil {
		fmt.Fprintln(l.seen, resp.ObjectSecretKey)
	}
	return resp
}

type labMac struct {
	i      int
	name   string
	tok    string
	ak, sk string
}

func (m labMac) cred() aws.Credentials { return aws.Credentials{AccessKeyID: m.ak, SecretAccessKey: m.sk} }

// s3 makes one signed request on the lab bucket: key "" lists it. It
// returns the HTTP status and the S3 error code ("" on 2xx).
func (l *pmLab) s3(method, key string, body []byte, c aws.Credentials) (int, string, []byte) {
	target := l.bucketURL + "?list-type=2&max-keys=1"
	if key != "" {
		target = l.bucketURL + "/" + key
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return 0, err.Error(), nil
	}
	sum := sha256.Sum256(body)
	ph := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", ph)
	if c.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", c.SessionToken)
	}
	if err := v4.NewSigner().SignHTTP(ctx, c, req, ph, "s3", "us-east-1", time.Now()); err != nil {
		return 0, err.Error(), nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "transport: " + err.Error(), nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 == 2 {
		return resp.StatusCode, "", b
	}
	var e struct {
		Code string `xml:"Code"`
	}
	_ = xml.Unmarshal(b, &e)
	if e.Code == "" {
		e.Code = "HTTP " + strconv.Itoa(resp.StatusCode)
	}
	return resp.StatusCode, e.Code, b
}

// usable: list, put and read back one object with the key.
func (l *pmLab) usable(m labMac) (bool, string) {
	if _, code, _ := l.s3(http.MethodGet, "", nil, m.cred()); code != "" {
		return false, "list " + code
	}
	obj := "lab/" + m.ak + "/probe"
	body := []byte("per-mac lab " + m.ak)
	if _, code, _ := l.s3(http.MethodPut, obj, body, m.cred()); code != "" {
		return false, "put " + code
	}
	_, code, got := l.s3(http.MethodGet, obj, nil, m.cred())
	if code != "" {
		return false, "get " + code
	}
	if !bytes.Equal(got, body) {
		return false, "get: wrong body"
	}
	return true, "OK"
}

// listCode: the S3 answer to one list with c ("" = accepted).
func (l *pmLab) listCode(c aws.Credentials) string {
	_, code, _ := l.s3(http.MethodGet, "", nil, c)
	return code
}

func refusedCode(code string) bool {
	return code == "InvalidAccessKeyId" || code == "SignatureDoesNotMatch" || code == "AccessDenied" ||
		code == "InvalidClientTokenId" || code == "ExpiredToken" || code == "InvalidTokenId"
}

func (l *pmLab) applied() *objKeysApplied {
	b, err := os.ReadFile(filepath.Join(l.objDir, objKeyAppliedName))
	if err != nil {
		return nil
	}
	var a objKeysApplied
	if json.Unmarshal(b, &a) != nil {
		return nil
	}
	return &a
}

// rootList: the mac-* access keys SILO lists under jm-macs (root view).
func (l *pmLab) rootList() []string {
	out, err := l.ctlRun("mc-root", "admin", "user", "svcacct", "ls", "a", "jm-macs")
	if err != nil {
		l.t.Logf("svcacct ls as root: %v %s", err, out)
	}
	var keys []string
	for _, line := range strings.Split(out, "\n") {
		var v struct {
			AccessKey string `json:"accessKey"`
		}
		if json.Unmarshal([]byte(line), &v) == nil && strings.HasPrefix(v.AccessKey, "mac-") {
			keys = append(keys, v.AccessKey)
		}
	}
	sort.Strings(keys)
	return keys
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(float64(len(s))*p+0.999999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

func ms(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) + " ms" }

// approveTimed enrolls Mac i, approves it and polls for its key.
func (l *pmLab) approveTimed(i int) (labMac, time.Duration, error) {
	name, tok := l.enroll(i)
	start := time.Now()
	err := l.s.Approve(name)
	d := time.Since(start)
	m := labMac{i: i, name: name, tok: tok}
	if err != nil {
		return m, d, err
	}
	r := l.poll(i, tok)
	m.ak, m.sk = r.ObjectAccessKey, r.ObjectSecretKey
	return m, d, nil
}

// ---- G1 + G5: create and approve ---------------------------------------------

func (l *pmLab) g1g5(n int) []labMac {
	var (
		durs                       []time.Duration
		macs                       []labMac
		ok, ownKey, shared, fields int
		noKey                      int
	)
	for i := 1; i <= n; i++ {
		m, d, err := l.approveTimed(i)
		durs = append(durs, d)
		if err != nil {
			l.t.Logf("approve %d: %v (%s)", i, err, ms(d))
			continue
		}
		if m.ak == "" || m.sk == "" {
			noKey++
			continue
		}
		if m.ak == "jm-client" || m.sk == l.sharedSK {
			shared++
			continue
		}
		if strings.HasPrefix(m.ak, m.name+"-") {
			ownKey++
		}
		if a := l.applied(); a != nil {
			if k, found := a.key(m.ak); found && k.Exists && k.Parent == objKeysParent && k.Status == "on" && k.ImpliedPolicy {
				fields++
			} else {
				l.t.Logf("applied.json entry for %s: %+v (found %v)", m.ak, k, found)
			}
		}
		if good, why := l.usable(m); good {
			ok++
		} else {
			l.t.Logf("key %s not usable: %s", m.ak, why)
		}
		macs = append(macs, m)
	}
	var each []string
	for _, d := range durs {
		each = append(each, strconv.FormatInt(d.Milliseconds(), 10))
	}
	l.note("G5", "approve ms in order (keys live: 0.."+strconv.Itoa(n-1)+"): "+strings.Join(each, " "))
	nn := strconv.Itoa(n)
	l.row("G1", "approved Mac's poll carries its own mac-* key", nn+"/"+nn, fmt.Sprintf("%d/%d", ownKey, n), ownKey == n)
	l.row("G1", "own key lists, writes and reads back on SILO", nn+"/"+nn, fmt.Sprintf("%d/%d", ok, n), ok == n)
	l.row("G1", "applied.json: exists, parent jm-macs, on, implied policy", nn+"/"+nn, fmt.Sprintf("%d/%d", fields, n), fields == n)
	l.row("G5", "approvals answered with no key or the shared key", "0", strconv.Itoa(noKey+shared), noKey+shared == 0)
	l.row("G5", "approve -> 200 with active key, p50", "(info)", ms(pct(durs, 0.5)), true)
	l.row("G5", "approve -> 200 with active key, p95", "<= 4000 ms", ms(pct(durs, 0.95)), pct(durs, 0.95) <= 4*time.Second)
	l.row("G5", "approve -> 200 with active key, max", "<= 10000 ms", ms(pct(durs, 1)), pct(durs, 1) <= 10*time.Second)
	if len(macs) == 0 {
		l.t.Fatal("no Mac got a key; nothing else can be measured")
	}
	return macs
}

// ---- G2: what the key admin credential may do ---------------------------------

func (l *pmLab) g2() {
	for _, parent := range []string{"jm-gc", "jm-client", "juicemount"} {
		out, err := l.ctlRun("keyadmin-probe", parent)
		if err != nil {
			l.t.Logf("keyadmin-probe %s: %v %s", parent, err, out)
		}
		got := strings.Join(strings.Fields(out), " ")
		// Recorded, not a target: the plan accepts either answer and
		// writes it into COMPOSE-ROLLOUT.md.
		l.row("G2", "jm-keyadmin key under "+parent, "(record)", got, true)
	}
	out, _ := l.ctlRun("mc-keyadmin", "admin", "user", "svcacct", "ls", "a", "jm-gc")
	l.row("G2", "jm-keyadmin lists service accounts of jm-gc", "(record)", okOrRefused(out), true)
}

func okOrRefused(out string) string {
	if strings.Contains(out, `"status":"error"`) || strings.Contains(out, `"status": "error"`) {
		return "refused"
	}
	return "allowed"
}

func firstMsg(out string) string {
	var v struct {
		Error struct {
			Message string `json:"message"`
			Cause   struct {
				Message string `json:"message"`
			} `json:"cause"`
		} `json:"error"`
	}
	for _, line := range strings.Split(out, "\n") {
		if json.Unmarshal([]byte(line), &v) == nil && v.Error.Message != "" {
			if v.Error.Cause.Message != "" {
				return v.Error.Message + ": " + v.Error.Cause.Message
			}
			return v.Error.Message
		}
	}
	if len(out) > 160 {
		out = out[:160]
	}
	return strings.TrimSpace(out)
}

// ---- G3: a Mac's key cannot make more credentials --------------------------------

type stsCreds struct {
	AccessKeyID     string `xml:"AssumeRoleResult>Credentials>AccessKeyId"`
	SecretAccessKey string `xml:"AssumeRoleResult>Credentials>SecretAccessKey"`
	SessionToken    string `xml:"AssumeRoleResult>Credentials>SessionToken"`
	Expiration      string `xml:"AssumeRoleResult>Credentials>Expiration"`
}

func (l *pmLab) assumeRole(c aws.Credentials) (*stsCreds, string) {
	form := url.Values{"Action": {"AssumeRole"}, "Version": {"2011-06-15"}, "DurationSeconds": {"900"}}.Encode()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, l.endpoint+"/", strings.NewReader(form))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	sum := sha256.Sum256([]byte(form))
	ph := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", ph)
	if err := v4.NewSigner().SignHTTP(ctx, c, req, ph, "sts", "us-east-1", time.Now()); err != nil {
		return nil, err.Error()
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Code string `xml:"Error>Code"`
		}
		_ = xml.Unmarshal(b, &e)
		return nil, "refused (" + e.Code + ")"
	}
	var r stsCreds
	if err := xml.Unmarshal(b, &r); err != nil || r.AccessKeyID == "" {
		return nil, "unreadable answer"
	}
	if l.seen != nil {
		fmt.Fprintln(l.seen, r.SecretAccessKey)
		fmt.Fprintln(l.seen, r.SessionToken)
	}
	return &r, "allowed"
}

// stsAfterRevoke is set by g3 when the Mac's key could mint a temporary
// credential; g4 then measures whether the revoke also cuts it.
var stsAfterRevoke *stsCreds

func (l *pmLab) g3(m labMac) {
	out, _ := l.ctlIn(m.sk+"\n", "mc-as", m.ak, "admin", "user", "svcacct", "add", "a", "jm-macs", "--access-key", "labg3-svc")
	got := okOrRefused(out)
	if got == "allowed" {
		_, _ = l.ctlRun("mc-root", "admin", "user", "svcacct", "rm", "a", "labg3-svc")
	} else {
		l.note("G3", "svcacct add as a Mac key: "+firstMsg(out))
	}
	l.row("G3", "Mac key: svcacct add (a new service account)", "refused", got, got == "refused")

	out, _ = l.ctlIn(m.sk+"\n", "mc-as", m.ak, "admin", "user", "svcacct", "add", "a", m.ak, "--access-key", "labg3-own")
	got = okOrRefused(out)
	if got == "allowed" {
		_, _ = l.ctlRun("mc-root", "admin", "user", "svcacct", "rm", "a", "labg3-own")
	}
	l.row("G3", "Mac key: svcacct add under itself", "refused", got, got == "refused")

	// user add with a throwaway lab secret on argv (it is not a real key:
	// the call must be refused, and the name is removed if it was not).
	out, _ = l.ctlIn(m.sk+"\n", "mc-as", m.ak, "admin", "user", "add", "a", "labg3-user", "LabThrowaway0123456789abcdefABCDEF")
	got = okOrRefused(out)
	if got == "allowed" {
		_, _ = l.ctlRun("mc-root", "admin", "user", "rm", "a", "labg3-user")
	} else {
		l.note("G3", "user add as a Mac key: "+firstMsg(out))
	}
	l.row("G3", "Mac key: user add", "refused", got, got == "refused")

	// Delete is not in the policy.
	_, code, _ := l.s3(http.MethodDelete, "lab/"+m.ak+"/probe", nil, m.cred())
	l.row("G3", "Mac key: DeleteObject", "refused (AccessDenied)", "refused ("+code+")", code == "AccessDenied")

	cr, res := l.assumeRole(m.cred())
	l.row("G3", "Mac key: STS AssumeRole", "(record)", res, true)
	if cr != nil {
		l.note("G3", "temporary credential expires "+cr.Expiration)
		c := aws.Credentials{AccessKeyID: cr.AccessKeyID, SecretAccessKey: cr.SecretAccessKey, SessionToken: cr.SessionToken}
		code := l.listCode(c)
		if code == "" {
			code = "OK"
		}
		l.row("G3", "temporary credential lists the bucket before revoke", "(record)", code, true)
		stsAfterRevoke = cr
	}
}

// ---- G4: revoke -> the Mac's key refused ---------------------------------------

func (l *pmLab) g4(macs []labMac) {
	var durs []time.Duration
	var never, redisLeft, putAfter int
	for idx, m := range macs {
		if code := l.listCode(m.cred()); code != "" {
			l.t.Logf("key %s refused before its revoke: %s", m.ak, code)
		}
		start := time.Now()
		if _, err := l.s.RevokeConfirmed(m.name, false); err != nil {
			l.t.Logf("revoke %s: %v", m.name, err)
			never++
			continue
		}
		if _, ok := findProxyPrincipal(readProxyPrincipals(l.t, filepath.Join(l.proxyDir, proxyPrincipalsName)), m.name); ok {
			redisLeft++
		}
		refusedAt := time.Duration(-1)
		for time.Since(start) < 15*time.Second {
			if code := l.listCode(m.cred()); refusedCode(code) {
				refusedAt = time.Since(start)
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if refusedAt < 0 {
			never++
			l.t.Logf("key %s still accepted 15 s after its revoke", m.ak)
			continue
		}
		durs = append(durs, refusedAt)
		if _, code, _ := l.s3(http.MethodPut, "lab/"+m.ak+"/after-revoke", []byte("x"), m.cred()); !refusedCode(code) {
			putAfter++
		}
		if idx == 0 && stsAfterRevoke != nil {
			l.stsResidual(start)
		}
	}
	var each []string
	for _, d := range durs {
		each = append(each, strconv.FormatInt(d.Milliseconds(), 10))
	}
	l.note("G4", "revoke->refused ms in order: "+strings.Join(each, " "))
	n := len(macs)
	l.row("G4", "revoked Mac still in the Redis proxy file at once", "0", strconv.Itoa(redisLeft), redisLeft == 0)
	l.row("G4", "revoke -> first refused LIST with the old key, p50", "(info)", ms(pct(durs, 0.5)), true)
	l.row("G4", "revoke -> first refused LIST with the old key, p95", "<= 5000 ms", ms(pct(durs, 0.95)), len(durs) > 0 && pct(durs, 0.95) <= 5*time.Second)
	l.row("G4", "revoke -> first refused LIST with the old key, max", "<= 10000 ms", ms(pct(durs, 1)), len(durs) > 0 && pct(durs, 1) <= 10*time.Second && never == 0)
	l.row("G4", "old key refused within 15 s", fmt.Sprintf("%d/%d", n, n), fmt.Sprintf("%d/%d", n-never, n), never == 0)
	l.row("G4", "PUT with the old key accepted after refusal", "0", strconv.Itoa(putAfter), putAfter == 0)
	// Forget is allowed once keyd confirmed every removal.
	deadline := time.Now().Add(10 * time.Second)
	left := 0
	for {
		left = 0
		for _, m := range macs {
			if err := l.s.Forget(m.name); err != nil && !errors.Is(err, ErrPrincipalNotFound) {
				left++
			}
		}
		if left == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	l.row("G4", "revoked Macs forgettable (removal confirmed) within 10 s", "0 left", fmt.Sprintf("%d left", left), left == 0)
	rl := l.rootList()
	l.row("G4", "mac-* keys left at SILO after all revokes", "0", strconv.Itoa(len(rl)), len(rl) == 0)
}

// stsResidual: after the revoke of the Mac whose key made a temporary
// credential, is that credential refused, and when?
func (l *pmLab) stsResidual(start time.Time) {
	c := aws.Credentials{AccessKeyID: stsAfterRevoke.AccessKeyID, SecretAccessKey: stsAfterRevoke.SecretAccessKey, SessionToken: stsAfterRevoke.SessionToken}
	for time.Since(start) < 30*time.Second {
		if code := l.listCode(c); refusedCode(code) {
			l.row("G3", "temporary credential after its key's revoke", "refused, or <= 15 min", "refused after "+ms(time.Since(start)), true)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	exp, err := time.Parse(time.RFC3339, stsAfterRevoke.Expiration)
	left := "unknown"
	ok := false
	if err == nil {
		left = time.Until(exp).Round(time.Second).String()
		ok = time.Until(exp) <= 15*time.Minute
	}
	l.row("G3", "temporary credential after its key's revoke", "refused, or <= 15 min", "still accepted at 30 s; expires in "+left, ok)
}

// ---- G6: failure cases ----------------------------------------------------------

func (l *pmLab) g6KeydStopped(base int) {
	a, _, err := l.approveTimed(base + 1)
	if err != nil {
		l.t.Fatalf("approve before the keyd stop: %v", err)
	}
	if out, err := l.ctlRun("keyd-stop"); err != nil {
		l.t.Fatalf("keyd-stop: %v %s", err, out)
	}
	name, tok := l.enroll(base + 2)
	start := time.Now()
	err = l.s.Approve(name)
	d := time.Since(start)
	got := "approved"
	if errors.Is(err, ErrObjectKeyNotReady) {
		got = "409 after " + ms(d)
		l.note("G6", "approve with keyd stopped: "+err.Error())
	}
	l.row("G6", "keyd stopped: approve refused with a reason within 11 s", "409 <= 11000 ms", got, errors.Is(err, ErrObjectKeyNotReady) && d <= 11*time.Second)
	if r := l.poll(base+2, tok); r.ObjectSecretKey != "" {
		l.row("G6", "keyd stopped: refused Mac's poll carries a key", "no key", r.ObjectAccessKey, false)
	}
	good, why := l.usable(a)
	l.row("G6", "keyd stopped: an existing key keeps working", "OK", why, good)

	if _, err := l.s.RevokeConfirmed(a.name, false); err != nil {
		l.t.Fatalf("revoke with keyd stopped: %v", err)
	}
	_, inProxy := findProxyPrincipal(readProxyPrincipals(l.t, filepath.Join(l.proxyDir, proxyPrincipalsName)), a.name)
	l.row("G6", "keyd stopped: revoke cuts the Redis login at once", "cut", map[bool]string{true: "still there", false: "cut"}[inProxy], !inProxy)
	code := l.listCode(a.cred())
	if code == "" {
		code = "OK"
	}
	l.row("G6", "keyd stopped: revoked key at SILO (expected: still works)", "OK", code, code == "OK")
	time.Sleep(3 * time.Second)
	keydBack := time.Now()
	if out, err := l.ctlRun("keyd-start"); err != nil {
		l.t.Fatalf("keyd-start: %v %s", err, out)
	}
	refused := time.Duration(-1)
	for time.Since(keydBack) < 20*time.Second {
		if refusedCode(l.listCode(a.cred())) {
			refused = time.Since(keydBack)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	got = "still accepted at 20 s"
	if refused >= 0 {
		got = "refused after " + ms(refused)
	}
	l.row("G6", "keyd back: the revoked key goes (from keyd start)", "<= 10000 ms", got, refused >= 0 && refused <= 10*time.Second)

	// The refused Mac, approved again once keyd is back.
	time.Sleep(2 * time.Second)
	start = time.Now()
	err = l.s.Approve(name)
	d = time.Since(start)
	r := l.poll(base+2, tok)
	ok := err == nil && strings.HasPrefix(r.ObjectAccessKey, name+"-") && r.ObjectSecretKey != ""
	if ok {
		good, why = l.usable(labMac{ak: r.ObjectAccessKey, sk: r.ObjectSecretKey})
		ok = good
		if !good {
			l.t.Logf("re-approved key: %s", why)
		}
	}
	l.row("G6", "keyd back: the refused Mac approved again, key works", "OK <= 4000 ms", fmt.Sprintf("%v in %s", map[bool]string{true: "OK", false: "FAILED"}[ok], ms(d)), ok && d <= 4*time.Second)
	l.cleanupMac(name)
}

// cleanupMac revokes and forgets a Mac, waiting for keyd.
func (l *pmLab) cleanupMac(name string) {
	_, _ = l.s.RevokeConfirmed(name, false)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if err := l.s.Forget(name); err == nil || errors.Is(err, ErrPrincipalNotFound) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
	l.t.Logf("cleanup: %s not forgettable in 15 s", name)
}

// strandedKeys: keys SILO lists under jm-macs that no store record can
// deliver (no secret) and that are not being removed or re-created.
func (l *pmLab) strandedKeys() []string {
	l.s.mu.Lock()
	ok := map[string]bool{}
	for _, r := range l.s.recs {
		if r.ObjKeyName != "" && (r.ObjKeySecret != "" || r.ObjKeyNeedsSecret || r.ObjKeyState == ObjKeyMinting) {
			ok[r.ObjKeyName] = true
		}
		for _, rm := range r.ObjKeyRemoving {
			ok[rm.Name] = true
		}
	}
	for _, o := range l.s.objk.orphans {
		ok[o] = true // listed for the admin, never silently kept
	}
	l.s.mu.Unlock()
	var out []string
	for _, k := range l.rootList() {
		if !ok[k] {
			out = append(out, k)
		}
	}
	return out
}

func (l *pmLab) g6SiloRestart() {
	const trials = 5
	var delivered, stranded int
	var worst time.Duration
	for tr := 0; tr < trials; tr++ {
		i := 200 + tr
		name, tok := l.enroll(i)
		errc := make(chan error, 1)
		go func() { errc <- l.s.Approve(name) }()
		// Stagger the restart across keyd's create step.
		time.Sleep(time.Duration(tr) * 400 * time.Millisecond)
		if out, err := l.ctlRun("silo-restart"); err != nil {
			l.t.Fatalf("silo-restart: %v %s", err, out)
		}
		back := time.Now()
		aerr := <-errc
		if aerr != nil {
			l.t.Logf("approve during SILO restart %d: %v", tr, aerr)
		}
		// Two keyd cycles plus two reconciles after SILO is back, then the
		// invariant must hold.
		time.Sleep(5 * time.Second)
		if s := l.strandedKeys(); len(s) > 0 {
			stranded += len(s)
			l.t.Logf("restart %d: stranded keys %v", tr, s)
		}
		// The Mac ends with a working key: at once, or after an approve
		// retry by the admin.
		var r EnrollResponse
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			r = l.poll(i, tok)
			if r.ObjectSecretKey != "" {
				break
			}
			if err := l.s.Approve(name); err != nil {
				l.t.Logf("re-approve %d: %v", tr, err)
			}
		}
		if r.ObjectSecretKey != "" {
			if good, why := l.usable(labMac{ak: r.ObjectAccessKey, sk: r.ObjectSecretKey}); good {
				delivered++
				if d := time.Since(back); d > worst {
					worst = d
				}
			} else {
				l.t.Logf("restart %d: key %s not usable: %s", tr, r.ObjectAccessKey, why)
			}
		}
		l.cleanupMac(name)
	}
	l.row("G6", "SILO restarted mid-create: stranded live keys after 2 cycles", "0", strconv.Itoa(stranded), stranded == 0)
	l.row("G6", "SILO restarted mid-create: Mac ends with a working key", fmt.Sprintf("%d/%d", trials, trials), fmt.Sprintf("%d/%d (worst %s)", delivered, trials, ms(worst)), delivered == trials)
}

// g6StoreRestored: the principal store is put back from an older copy.
// Keys issued after the copy are listed as orphans and kept until the
// admin removes them; no generation is ever reused.
func (l *pmLab) g6StoreRestored() {
	a, _, err := l.approveTimed(300)
	if err != nil {
		l.t.Fatalf("approve A: %v", err)
	}
	backup, err := os.ReadFile(l.statePath)
	if err != nil {
		l.t.Fatal(err)
	}
	// After the backup: B approved; A revoked and approved again (gen 2).
	b, _, err := l.approveTimed(301)
	if err != nil {
		l.t.Fatalf("approve B: %v", err)
	}
	if _, err := l.s.RevokeConfirmed(a.name, false); err != nil {
		l.t.Fatal(err)
	}
	if err := l.s.Approve(a.name); err != nil {
		l.t.Fatalf("re-approve A: %v", err)
	}
	a2 := l.poll(300, a.tok)
	l.note("G6", fmt.Sprintf("before restore: A %s -> %s, B %s", a.ak, a2.ObjectAccessKey, b.ak))

	l.close()
	if err := os.WriteFile(l.statePath, backup, 0o600); err != nil {
		l.t.Fatal(err)
	}
	l.open()
	time.Sleep(6 * time.Second) // keyd and reconciler cycles

	snap := l.s.Snapshot()
	var orphans []string
	for _, o := range snap.OrphanKeys {
		orphans = append(orphans, o.Name)
	}
	sort.Strings(orphans)
	wantOrph := []string{a2.ObjectAccessKey, b.ak}
	sort.Strings(wantOrph)
	l.row("G6", "store restored: keys made after the backup listed as orphans", strings.Join(wantOrph, " "), strings.Join(orphans, " "), strings.Join(orphans, " ") == strings.Join(wantOrph, " "))
	b2 := labMac{ak: a2.ObjectAccessKey, sk: a2.ObjectSecretKey}
	gk, _ := l.usable(b)
	ga, _ := l.usable(b2)
	l.row("G6", "store restored: orphan keys kept (still work) until admin", "both work", fmt.Sprintf("B %v, A gen2 %v", gk, ga), gk && ga)

	// A, approved in the old store with gen 1 (since removed): it must get
	// a generation above 2.
	r := l.poll(300, a.tok)
	deadline := time.Now().Add(12 * time.Second)
	for r.ObjectSecretKey == "" && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		r = l.poll(300, a.tok)
	}
	_, gen, _ := splitObjKeyName(r.ObjectAccessKey)
	l.row("G6", "store restored: A's next key generation (gen 1, 2 used)", ">= 3", fmt.Sprintf("%s (gen %d)", r.ObjectAccessKey, gen), gen >= 3)
	if r.ObjectSecretKey != "" {
		good, why := l.usable(labMac{ak: r.ObjectAccessKey, sk: r.ObjectSecretKey})
		l.row("G6", "store restored: A's new key works", "OK", why, good)
	}
	// The admin removes the orphans.
	if err := l.s.RemoveOrphanObjKeys(orphans); err != nil {
		l.t.Logf("remove orphans: %v", err)
	}
	start := time.Now()
	gone := time.Duration(-1)
	for time.Since(start) < 15*time.Second {
		if refusedCode(l.listCode(b.cred())) && refusedCode(l.listCode(b2.cred())) {
			gone = time.Since(start)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	l.row("G6", "store restored: admin removes orphans -> both refused", "<= 10000 ms", ms(gone), gone >= 0 && gone <= 10*time.Second)
	l.cleanupMac(a.name)
}

// g6FilesLost: the objkeys folder is emptied while keys are live (keyd
// keeps running). The Manager first keeps running, then restarts before
// any new key is minted (so manager-gens.json is not written again).
func (l *pmLab) g6FilesLost() {
	m, _, err := l.approveTimed(400)
	if err != nil {
		l.t.Fatalf("approve: %v", err)
	}
	gens, _ := os.ReadFile(filepath.Join(l.objDir, objKeyGensName))
	wipe := func() {
		entries, _ := os.ReadDir(l.objDir)
		for _, e := range entries {
			_ = os.RemoveAll(filepath.Join(l.objDir, e.Name()))
		}
	}
	wipe()
	time.Sleep(6 * time.Second)
	good, why := l.usable(m)
	l.row("G6", "objkeys files lost (Manager running): live key kept", "OK", why, good)
	rl := l.rootList()
	l.row("G6", "objkeys files lost (Manager running): keys at SILO", "1 ("+m.ak+")", strings.Join(rl, " "), len(rl) == 1 && rl[0] == m.ak)

	l.close()
	err = l.openErr()
	got := "store opens"
	if err != nil {
		got = "store refused: " + err.Error()
	}
	l.row("G6", "objkeys files lost, Manager restarted", "(record)", got, true)
	good, why = l.usable(m)
	l.row("G6", "objkeys files lost, Manager restarted: live key kept", "OK", why, good)
	if err != nil {
		// The documented repair without a backup of the folder (the store
		// itself is current): manager-gens.json from the store's own
		// obj_key_gens, as COMPOSE-ROLLOUT.md's jq line writes it.
		var f struct {
			Gens map[string]int `json:"obj_key_gens"`
		}
		b, err := os.ReadFile(l.statePath)
		if err != nil || json.Unmarshal(b, &f) != nil {
			l.t.Fatalf("read store: %v", err)
		}
		body, _ := json.Marshal(struct {
			Gens map[string]int `json:"gens"`
		}{f.Gens})
		if f.Gens == nil {
			body = []byte(`{"gens":{}}`)
		}
		var lost, rebuilt map[string]any
		_ = json.Unmarshal(gens, &lost)
		_ = json.Unmarshal(body, &rebuilt)
		l.note("G6", fmt.Sprintf("manager-gens.json rebuilt from the store equals the lost one: %v", fmt.Sprint(lost) == fmt.Sprint(rebuilt)))
		if err := os.WriteFile(filepath.Join(l.objDir, objKeyGensName), body, 0o600); err != nil {
			l.t.Fatal(err)
		}
		err = l.openErr()
		got = "store opens"
		if err != nil {
			got = "store refused: " + err.Error()
		}
		l.row("G6", "objkeys files lost: manager-gens.json rebuilt from store", "store opens", got, err == nil)
		if err != nil {
			return
		}
	}
	time.Sleep(5 * time.Second)
	_, d, err := l.approveTimed(401)
	got = "OK in " + ms(d)
	if err != nil {
		got = "refused: " + err.Error()
	}
	l.row("G6", "objkeys files lost: a new approval after the restart", "OK <= 4000 ms", got, err == nil && d <= 4*time.Second)
	good, why = l.usable(m)
	l.row("G6", "objkeys files lost: the first Mac's key still works", "OK", why, good)
}
