package manager

// Per-Mac object keys (objkeys.go), design §10 U1-U7. A fake minio-keyd
// (fakeKeyd) implements server/minio/keyd.sh's contract over the shared
// directory: strict desired.json, seq refusal, mass-delete guard,
// needs_secret honoured once per seq, results/ before applied.json, and
// applied.json built from its "MinIO", not from desired.json.
// objkeys_realkeyd_test.go drives the real keyd.sh end to end.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- fake keyd -------------------------------------------------------------

type fakeMinioKey struct{ parent, status, secret string }

type fakeKeyd struct {
	t   *testing.T
	dir string

	mu         sync.Mutex
	keys       map[string]fakeMinioKey
	createdSeq map[string]int64
	lastSeq    int64
	creates    []string // every svcacct add, in order
	removes    []string
	n          int
	// crashNextCreate: the next create makes the key in MinIO, then keyd
	// "dies" before writing its result (and this cycle's applied.json).
	crashNextCreate bool
	// down: keyd "died" (after crashNextCreate); steps do nothing until
	// restart.
	down      bool
	downSteps int
	stayDown  bool // tests that restart keyd themselves
	refusals  []string
}

func newFakeKeyd(t *testing.T, dir string) *fakeKeyd {
	return &fakeKeyd{t: t, dir: dir, keys: map[string]fakeMinioKey{}, createdSeq: map[string]int64{}}
}

func writeAtomicTest(path string, b []byte) error {
	tmp := path + ".tmp.fake"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type fakeDesired struct {
	Seq             int64    `json:"seq"`
	StoreCount      int      `json:"store_count"`
	AllowMassDelete int64    `json:"allow_mass_delete"`
	Remove          []string `json:"remove"`
	Keys            []struct {
		Name        string `json:"name"`
		State       string `json:"state"`
		NeedsSecret bool   `json:"needs_secret"`
	} `json:"keys"`
}

// step is one keyd cycle.
func (k *fakeKeyd) step() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.down {
		// Docker restarts a dead keyd: it is back after a few cycles.
		if k.downSteps++; k.downSteps < 3 || k.stayDown {
			return
		}
		k.down, k.downSteps = false, 0
	}
	_ = os.MkdirAll(filepath.Join(k.dir, "results"), 0o700)
	errStr := ""
	var dnames []string
	raw, err := os.ReadFile(filepath.Join(k.dir, "desired.json"))
	if os.Getenv("OBJ_TRACE") != "" {
		ab, _ := os.ReadFile(filepath.Join(k.dir, "applied.json"))
		k.t.Logf("keyd step: keys=%v last=%d desired=%s applied=%s", k.keys, k.lastSeq, raw, ab)
	}
	if err == nil {
		var d fakeDesired
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		ok := true
		switch {
		case bytes.ContainsRune(raw, '\\'):
			errStr, ok = "refused desired.json: escape sequences are not allowed", false
		case bytes.Contains(raw, []byte(`"keys":null`)):
			errStr, ok = "refused desired.json: keys is not an array", false
		}
		if ok {
			if err := dec.Decode(&d); err != nil {
				errStr, ok = "refused desired.json: "+err.Error(), false
			}
		}
		seen := map[string]bool{}
		needs := map[string]bool{}
		if ok {
			for _, e := range d.Keys {
				if !objKeyNameRe.MatchString(e.Name) || seen[e.Name] || e.State != "active" {
					errStr, ok = "refused desired.json: bad entry "+e.Name, false
					break
				}
				seen[e.Name] = true
				dnames = append(dnames, e.Name)
				if e.NeedsSecret {
					needs[e.Name] = true
				}
			}
		}
		if ok && d.Seq < k.lastSeq {
			errStr, ok = fmt.Sprintf("refused desired.json: seq %d is lower than applied seq %d", d.Seq, k.lastSeq), false
		}
		if ok {
			removed := map[string]bool{}
			for _, name := range d.Remove {
				if !objKeyNameRe.MatchString(name) || removed[name] || seen[name] {
					errStr, ok = "refused desired.json: invalid or conflicting removal "+name, false
					break
				}
				removed[name] = true
			}
		}
		if !ok {
			dnames = nil
			k.refusals = append(k.refusals, errStr)
		} else if k.apply(d, seen, needs) {
			k.lastSeq = d.Seq
		} else {
			errStr = k.refusals[len(k.refusals)-1]
			dnames = nil
		}
		if k.down {
			return
		}
	}
	k.writeApplied(dnames, errStr)
}

func (k *fakeKeyd) managed() []string {
	var out []string
	for n, v := range k.keys {
		if v.parent == objKeysParent && objKeyNameRe.MatchString(n) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

func (k *fakeKeyd) apply(d fakeDesired, want, needs map[string]bool) bool {
	var remove []string
	m := k.managed()
	for _, n := range m {
		if slices.Contains(d.Remove, n) {
			remove = append(remove, n)
		}
	}
	r, e := len(remove), len(m)
	if r > 0 && (r > 3 || (r > 1 && r*100 > 25*e)) && d.AllowMassDelete != d.Seq {
		k.refusals = append(k.refusals, fmt.Sprintf("REFUSED mass delete of %d of %d keys", r, e))
		return false
	}
	var fresh, again []string
	for _, x := range d.Keys {
		n := x.Name
		if _, ok := k.keys[n]; !ok {
			fresh = append(fresh, n)
		} else if needs[n] && !fileExists(filepath.Join(k.dir, "results", n+".json")) && d.Seq > k.createdSeq[n] {
			again = append(again, n)
		}
	}
	for _, n := range append(append([]string(nil), fresh...), again...) {
		k.createdSeq[n] = d.Seq
	}
	for _, n := range fresh {
		if !k.create(n) {
			return true
		}
	}
	for _, n := range again {
		delete(k.keys, n)
		k.removes = append(k.removes, n)
		if !k.create(n) {
			return true
		}
	}
	for _, n := range remove {
		delete(k.keys, n)
		k.removes = append(k.removes, n)
	}
	return true
}

// create returns false when keyd "crashed" after creating the key.
func (k *fakeKeyd) create(name string) bool {
	k.n++
	// MinIO-generated secrets may contain '+' and '/'.
	secret := fmt.Sprintf("Ab+/%036d", k.n)
	k.keys[name] = fakeMinioKey{parent: objKeysParent, status: "on", secret: secret}
	k.creates = append(k.creates, name)
	if k.crashNextCreate {
		k.crashNextCreate = false
		k.down = true
		return false
	}
	b, _ := json.Marshal(map[string]any{"name": name, "secret_key": secret, "created_ms": time.Now().UnixMilli()})
	if err := writeAtomicTest(filepath.Join(k.dir, "results", name+".json"), b); err != nil {
		k.t.Errorf("fake keyd: %v", err)
	}
	return true
}

func (k *fakeKeyd) writeApplied(dnames []string, errStr string) {
	names := k.managed()
	for _, n := range dnames {
		if _, ok := k.keys[n]; !ok {
			names = append(names, n)
		}
	}
	type ent struct {
		Name          string `json:"name"`
		Exists        bool   `json:"exists"`
		Status        string `json:"status,omitempty"`
		Parent        string `json:"parent,omitempty"`
		ImpliedPolicy bool   `json:"implied_policy,omitempty"`
	}
	doc := struct {
		Seq        int64  `json:"seq"`
		VerifiedMS int64  `json:"verified_ms"`
		Error      string `json:"error"`
		Keys       []ent  `json:"keys"`
	}{Seq: k.lastSeq, VerifiedMS: time.Now().UnixMilli(), Error: errStr, Keys: []ent{}}
	for _, n := range names {
		v, ok := k.keys[n]
		if !ok {
			doc.Keys = append(doc.Keys, ent{Name: n})
			continue
		}
		doc.Keys = append(doc.Keys, ent{Name: n, Exists: true, Status: v.status, Parent: v.parent, ImpliedPolicy: true})
	}
	b, _ := json.Marshal(doc)
	if err := writeAtomicTest(filepath.Join(k.dir, "applied.json"), b); err != nil {
		k.t.Errorf("fake keyd: %v", err)
	}
}

// restart clears the crash mark (keyd came back).
func (k *fakeKeyd) restart() {
	k.mu.Lock()
	k.down, k.downSteps = false, 0
	k.mu.Unlock()
}

func (k *fakeKeyd) minio(name string) (fakeMinioKey, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.keys[name]
	return v, ok
}

func (k *fakeKeyd) createCount(name string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	n := 0
	for _, c := range k.creates {
		if c == name {
			n++
		}
	}
	return n
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

// ---- rig -------------------------------------------------------------------

type objRig struct {
	t         *testing.T
	statePath string
	proxyDir  string
	objDir    string
	kd        *fakeKeyd
	s         *PrincipalStore

	kdMu   sync.Mutex
	kdStop chan struct{}
	kdDone chan struct{}
}

func shortObjKeyTimers(t *testing.T) {
	t.Helper()
	w, p, r := objKeyWait, objKeyPollEvery, objKeyReconcileEvery
	objKeyWait, objKeyPollEvery, objKeyReconcileEvery = 400*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { objKeyWait, objKeyPollEvery, objKeyReconcileEvery = w, p, r })
}

func newObjRig(t *testing.T) *objRig {
	t.Helper()
	shortObjKeyTimers(t)
	dir := t.TempDir()
	r := &objRig{t: t, statePath: filepath.Join(dir, "store", "principals-store.json"),
		proxyDir: filepath.Join(dir, "respproxy"), objDir: filepath.Join(dir, "objkeys")}
	for _, d := range []string{r.proxyDir, r.objDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	r.kd = newFakeKeyd(t, r.objDir)
	startFakeProxyAck(t, r.proxyDir)
	r.open()
	t.Cleanup(r.keydDown)
	return r
}

func (r *objRig) open() {
	r.t.Helper()
	s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
		PerMacKeys: true, ObjKeysDir: r.objDir, ObjectKey: clientObjectKey})
	if err != nil {
		r.t.Fatal(err)
	}
	r.s = s
}

// keydUp runs the fake keyd every 5 ms until keydDown.
func (r *objRig) keydUp() {
	r.kdMu.Lock()
	defer r.kdMu.Unlock()
	r.kd.restart()
	if r.kdStop != nil {
		return
	}
	stop, done := make(chan struct{}), make(chan struct{})
	r.kdStop, r.kdDone = stop, done
	go func() {
		defer close(done)
		for {
			r.kd.step()
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
}

func (r *objRig) keydDown() {
	r.kdMu.Lock()
	defer r.kdMu.Unlock()
	if r.kdStop == nil {
		return
	}
	close(r.kdStop)
	<-r.kdDone
	r.kdStop, r.kdDone = nil, nil
}

var objRigIP = 0

// enroll enrolls Mac i (distinct source address per call: the per-IP
// hashing budget is 6 a minute).
func (r *objRig) enroll(i int) (string, string) {
	r.t.Helper()
	objRigIP++
	resp, err := r.s.Enroll(EnrollRequest{InstanceID: instanceN(i), Host: fmt.Sprintf("Mac-%d", i),
		Secret: strings.Repeat(string(rune('a'+i%26)), 40)}, fmt.Sprintf("10.%d.%d.%d", objRigIP/65536%256, objRigIP/256%256, objRigIP%256))
	if err != nil {
		r.t.Fatalf("enroll %d: %v", i, err)
	}
	return resp.Principal, resp.PollToken
}

func (r *objRig) rec(name string) MacPrincipal {
	r.t.Helper()
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	p := r.s.recs[name]
	if p == nil {
		r.t.Fatalf("no record %s", name)
	}
	return *p
}

func (r *objRig) poll(i int, token string) EnrollResponse {
	r.t.Helper()
	objRigIP++
	resp, err := r.s.Enroll(EnrollRequest{InstanceID: instanceN(i), PollToken: token}, fmt.Sprintf("10.200.%d.%d", objRigIP/256%256, objRigIP%256))
	if err != nil {
		r.t.Fatalf("poll %d: %v", i, err)
	}
	return resp
}

func (r *objRig) desired() fakeDesired {
	r.t.Helper()
	b, err := os.ReadFile(filepath.Join(r.objDir, "desired.json"))
	if err != nil {
		r.t.Fatal(err)
	}
	var d fakeDesired
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		r.t.Fatalf("desired.json: %v\n%s", err, b)
	}
	return d
}

func (d fakeDesired) has(name string) (needs, ok bool) {
	for _, k := range d.Keys {
		if k.Name == name {
			return k.NeedsSecret, true
		}
	}
	return false, false
}

func (r *objRig) proxyHas(name string) bool {
	r.t.Helper()
	_, ok := findProxyPrincipal(readProxyPrincipals(r.t, filepath.Join(r.proxyDir, proxyPrincipalsName)), name)
	return ok
}

// waitFor polls cond (running a reconcile each time) until true or 3 s.
func (r *objRig) waitFor(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if err := r.s.ReconcileObjKeys(); err != nil {
			r.t.Logf("reconcile: %v", err)
		}
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			r.s.mu.Lock()
			var dump []string
			for _, p := range r.s.recs {
				dump = append(dump, fmt.Sprintf("%s status=%s autoPending=%v key=%s state=%s needs=%v mintSeq=%d secret?=%v removing=%v",
					p.Name, p.Status, p.AutoApprovePending, p.ObjKeyName, p.ObjKeyState, p.ObjKeyNeedsSecret, p.ObjKeyMintSeq, p.ObjKeySecret != "", p.ObjKeyRemoving))
			}
			r.s.mu.Unlock()
			d, _ := os.ReadFile(filepath.Join(r.objDir, "desired.json"))
			a, _ := os.ReadFile(filepath.Join(r.objDir, "applied.json"))
			r.kd.mu.Lock()
			ks := fmt.Sprint(r.kd.keys, r.kd.createdSeq, r.kd.lastSeq, r.kd.down)
			r.kd.mu.Unlock()
			r.t.Fatalf("timed out waiting for: %s\n%s\ndesired=%s\napplied=%s\nkeyd=%s", what, strings.Join(dump, "\n"), d, a, ks)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// checkInvariant: every approved Mac has an active key that MinIO holds
// with the same secret, and the proxy file names exactly the approved set.
func (r *objRig) checkInvariant(step string) {
	r.t.Helper()
	r.s.mu.Lock()
	recs := map[string]MacPrincipal{}
	for k, v := range r.s.recs {
		recs[k] = *v
	}
	r.s.mu.Unlock()
	proxy := readProxyPrincipals(r.t, filepath.Join(r.proxyDir, proxyPrincipalsName))
	for name, p := range recs {
		_, inProxy := findProxyPrincipal(proxy, name)
		if inProxy != (p.Status == PrincipalApproved) {
			r.t.Fatalf("%s: %s status %s but proxy entry=%v", step, name, p.Status, inProxy)
		}
		if p.Status != PrincipalApproved {
			continue
		}
		if !objKeyDeliverable(&p) {
			r.t.Fatalf("%s: INVARIANT: %s is approved without an active key: state=%q name=%q secret?=%v",
				step, name, p.ObjKeyState, p.ObjKeyName, p.ObjKeySecret != "")
		}
		v, ok := r.kd.minio(p.ObjKeyName)
		if !ok || v.secret != p.ObjKeySecret || v.parent != objKeysParent {
			r.t.Fatalf("%s: INVARIANT: %s's key %s is not in MinIO with the stored secret (in MinIO=%v)", step, name, p.ObjKeyName, ok)
		}
	}
}

// ---- U1 --------------------------------------------------------------------

// U1: keyd silent -> Approve is refused (ErrObjectKeyNotReady), the Mac
// stays pending, and the proxy file does not change. When keyd answers,
// the next Approve succeeds with the SAME key name (no second mint).
func TestObjKeysApproveRefusedWhileKeydSilent(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(1)
	before, _ := os.ReadFile(filepath.Join(r.proxyDir, proxyPrincipalsName))

	err := r.s.Approve(name)
	if !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("Approve with keyd silent = %v, want ErrObjectKeyNotReady", err)
	}
	if !strings.Contains(err.Error(), "minio-keyd not answering") {
		t.Errorf("refusal does not say keyd is not answering: %v", err)
	}
	p := r.rec(name)
	if p.Status != PrincipalPending || p.ObjKeyState != ObjKeyMinting || p.ObjKeyName != name+"-1" {
		t.Fatalf("after refused approve: status=%s state=%s name=%s", p.Status, p.ObjKeyState, p.ObjKeyName)
	}
	after, _ := os.ReadFile(filepath.Join(r.proxyDir, proxyPrincipalsName))
	if !bytes.Equal(before, after) {
		t.Fatalf("proxy principals file changed on a refused approve:\n%s\n->\n%s", before, after)
	}
	r.checkInvariant("refused approve")

	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatalf("Approve with keyd up: %v", err)
	}
	p = r.rec(name)
	if p.Status != PrincipalApproved || p.ObjKeyName != name+"-1" || p.ObjKeyGen != 1 {
		t.Fatalf("after approve: status=%s name=%s gen=%d (want the first key reused)", p.Status, p.ObjKeyName, p.ObjKeyGen)
	}
	if n := r.kd.createCount(name + "-1"); n != 1 {
		t.Fatalf("%s created %d times, want 1", name+"-1", n)
	}
	r.checkInvariant("approved")
}

// U1 over HTTP: 409 with the admin-facing reason.
func TestObjKeysApproveHTTP409(t *testing.T) {
	shortObjKeyTimers(t)
	mux, _, dir, _ := newObjAPI(t)
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "storage key could not be created") {
		t.Fatalf("approve with keyd silent: %d %s", rr.Code, rr.Body)
	}
	if !strings.Contains(rr.Body.String(), "minio-keyd not answering") {
		t.Errorf("409 does not name the cause: %s", rr.Body)
	}
	_ = dir
}

// The wait itself: with keyd running, Approve succeeds on the first try
// (the key is minted and verified inside Approve).
func TestObjKeysApproveWaitsForKeyd(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, tok := r.enroll(2)
	if err := r.s.Approve(name); err != nil {
		t.Fatalf("Approve with keyd up: %v", err)
	}
	r.checkInvariant("approve")
	got := r.poll(2, tok)
	p := r.rec(name)
	if got.ObjectAccessKey != p.ObjKeyName || got.ObjectSecretKey != p.ObjKeySecret || !strings.Contains(got.ObjectSecretKey, "+") {
		t.Fatalf("poll = %s/%q, want %s and its '+' secret", got.ObjectAccessKey, got.ObjectSecretKey, p.ObjKeyName)
	}
}

// A revoke that lands between Approve's key wait and its commit must not
// produce an approved Mac without a key (the commit re-checks).
func TestObjKeysApproveRacingRevokeNeverApprovesKeyless(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(3)
	approveAfterKeyWait = func(n string) {
		if err := r.s.Revoke(n); err != nil {
			t.Errorf("revoke: %v", err)
		}
	}
	t.Cleanup(func() { approveAfterKeyWait = nil })
	err := r.s.Approve(name)
	if !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("Approve racing a revoke = %v, want ErrObjectKeyNotReady", err)
	}
	if p := r.rec(name); p.Status != PrincipalRevoked {
		t.Fatalf("status %s, want revoked", p.Status)
	}
	r.checkInvariant("approve raced revoke")
}

// ---- U2 --------------------------------------------------------------------

// U2: random sequences of enroll, approve, revoke, forget, auto-approve
// window, keyd up/down/crash and Manager restart never leave an approved
// Mac without an active key that MinIO holds.
func TestObjKeysInvariantRandomSequences(t *testing.T) {
	for seed := int64(1); seed <= 12; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			r := newObjRig(t)
			objKeyWait = 150 * time.Millisecond
			rng := rand.New(rand.NewSource(seed))
			type mac struct {
				name, token string
			}
			macs := map[int]*mac{}
			next := 0
			approvedOnce := 0
			for step := 0; step < 40; step++ {
				op := rng.Intn(10)
				var label string
				switch op {
				case 0, 1:
					if len(macs) >= 5 {
						continue
					}
					next++
					n, tok := r.enroll(next)
					macs[next] = &mac{n, tok}
					label = "enroll " + n
				case 2, 3:
					for i, m := range macs {
						label = "approve " + m.name
						if err := r.s.Approve(m.name); err == nil {
							approvedOnce++
						} else if !errors.Is(err, ErrObjectKeyNotReady) {
							t.Fatalf("approve %d: %v", i, err)
						}
						break
					}
				case 4:
					for _, m := range macs {
						label = "revoke " + m.name
						if err := r.s.Revoke(m.name); err != nil {
							t.Fatalf("revoke: %v", err)
						}
						break
					}
				case 5:
					for i, m := range macs {
						label = "forget " + m.name
						err := r.s.Forget(m.name)
						switch {
						case err == nil:
							delete(macs, i)
						case errors.Is(err, ErrPrincipalState):
						default:
							t.Fatalf("forget: %v", err)
						}
						break
					}
				case 6:
					if rng.Intn(2) == 0 {
						label = "keyd up"
						r.keydUp()
					} else {
						label = "keyd down"
						r.keydDown()
					}
				case 7:
					label = "window"
					if _, err := r.s.SetAutoApproveWindow(time.Duration(rng.Intn(2))*time.Hour, 20); err != nil {
						t.Fatalf("window: %v", err)
					}
				case 8:
					label = "restart"
					r.open()
				case 9:
					label = "keyd crash on next create"
					r.kd.mu.Lock()
					r.kd.crashNextCreate = true
					r.kd.mu.Unlock()
				}
				if err := r.s.ReconcileObjKeys(); err != nil {
					t.Logf("reconcile: %v", err)
				}
				r.checkInvariant(fmt.Sprintf("seed %d step %d (%s)", seed, step, label))
				for i, m := range macs {
					got := r.poll(i, m.token)
					p := r.rec(m.name)
					if got.Status == PrincipalApproved && (got.ObjectAccessKey != p.ObjKeyName || got.ObjectSecretKey == "") {
						t.Fatalf("seed %d step %d: approved poll for %s carries %q, want its own key", seed, step, m.name, got.ObjectAccessKey)
					}
				}
			}
			// Converge: keyd up, everything pending-auto settles, no
			// approved Mac lacks a key.
			r.keydUp()
			r.waitFor("auto-approvals settle", func() bool {
				r.s.mu.Lock()
				defer r.s.mu.Unlock()
				for _, p := range r.s.recs {
					if p.AutoApprovePending || p.ObjKeyNeedsSecret {
						return false
					}
				}
				return true
			})
			r.checkInvariant("converged")
			t.Logf("seed %d: %d approvals succeeded", seed, approvedOnce)
		})
	}
}

// Auto-approve with per-Mac keys: the Mac stays pending until its key is
// active, then the reconciler approves it (and its poll carries the key).
func TestObjKeysAutoApproveWaitsForKey(t *testing.T) {
	r := newObjRig(t)
	if _, err := r.s.SetAutoApproveWindow(time.Hour, 5); err != nil {
		t.Fatal(err)
	}
	name, tok := r.enroll(4)
	p := r.rec(name)
	if p.Status != PrincipalPending || !p.AutoApprovePending || p.ObjKeyState != ObjKeyMinting {
		t.Fatalf("auto-admitted with keyd down: status=%s pending=%v state=%s", p.Status, p.AutoApprovePending, p.ObjKeyState)
	}
	if r.proxyHas(name) {
		t.Fatal("auto-admitted Mac reached the proxy before its key")
	}
	if got := r.poll(4, tok); got.Status != PrincipalPending || got.ObjectAccessKey != "" {
		t.Fatalf("poll while waiting = %+v", got)
	}
	if snap := r.s.Snapshot(); snap.byName(name).ObjKeyStatus != "minting" || snap.KeydProblem == "" {
		t.Fatalf("snapshot while waiting: status=%q keyd problem=%q", snap.byName(name).ObjKeyStatus, snap.KeydProblem)
	}
	r.keydUp()
	r.waitFor("auto-approval", func() bool { return r.rec(name).Status == PrincipalApproved })
	if p := r.rec(name); !p.AutoApproved || p.AutoApprovePending {
		t.Fatalf("auto-approved flags: %+v", p)
	}
	if !r.proxyHas(name) {
		t.Fatal("approved Mac missing from the proxy file")
	}
	if got := r.poll(4, tok); got.ObjectAccessKey != name+"-1" {
		t.Fatalf("poll after auto-approval = %+v", got)
	}
	r.checkInvariant("auto")
}

// ---- U3 --------------------------------------------------------------------

// U3: with per-Mac mode on and JM_OBJ_CLIENT_*/JM_OBJ_GC_* set, an approved
// poll returns the Mac's own key, never jm-client or jm-gc. A Mac approved
// before the switch gets no key in its answer (it keeps the one it holds)
// until its own key is active.
func TestObjKeysPollNeverSharedKey(t *testing.T) {
	setObjectKeyEnv(t)
	r := newObjRig(t)

	// Approved before the switch.
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	r.s = off
	legacy, legacyTok := r.enroll(5)
	if err := off.Approve(legacy); err != nil {
		t.Fatal(err)
	}
	if got := r.poll(5, legacyTok); got.ObjectAccessKey != testClientAK {
		t.Fatalf("mode off: poll = %+v, want jm-client", got)
	}

	r.open() // mode on, keyd down
	got := r.poll(5, legacyTok)
	if got.Status != PrincipalApproved || got.ObjectAccessKey != "" || got.ObjectSecretKey != "" {
		t.Fatalf("legacy Mac, own key not ready: poll = %+v, want approved with no key", got)
	}
	if st := r.s.Snapshot().byName(legacy).ObjKeyStatus; st != "minting" {
		t.Fatalf("legacy Mac key status %q, want minting (backfill)", st)
	}

	name, tok := r.enroll(6)
	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	r.waitFor("backfill", func() bool { return r.rec(legacy).ObjKeyState == ObjKeyActive })
	for i, tk := range map[int]string{5: legacyTok, 6: tok} {
		got := r.poll(i, tk)
		b, _ := json.Marshal(got)
		if strings.Contains(string(b), testClientSK) || strings.Contains(string(b), testGCSK) ||
			got.ObjectAccessKey == testClientAK || got.ObjectAccessKey == testGCAK {
			t.Fatalf("Mac %d got a shared key: %s", i, b)
		}
		if !objKeyNameRe.MatchString(got.ObjectAccessKey) || got.ObjectSecretKey == "" {
			t.Fatalf("Mac %d: poll = %s, want its own key", i, b)
		}
	}
}

// With JM_OBJ_PER_MAC off (unset, "0", or anything but exactly "1") the
// Manager behaves as before: jm-client delivered, no per-Mac fields stored
// or shown, nothing written to the keyd directory.
func TestObjKeysOffIsUnchanged(t *testing.T) {
	setObjectKeyEnv(t)
	for _, v := range []string{"", "0", "true", " 1", "1 ", "yes"} {
		t.Run(fmt.Sprintf("%q", v), func(t *testing.T) {
			if parsePerMacFlag(v) {
				t.Fatalf("JM_OBJ_PER_MAC=%q parsed as on", v)
			}
			t.Setenv("JM_OBJ_PER_MAC", v)
			objDir := filepath.Join(t.TempDir(), "objkeys")
			t.Setenv("JM_OBJKEYS_DIR", objDir)
			mux, _, dir, _ := newH8BAPI(t)
			rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
			er := decodeEnroll(t, rr.Body.Bytes())
			if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
				t.Fatalf("approve: %d %s", rr.Code, rr.Body)
			}
			rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "poll_token": er.PollToken})
			if got := decodeEnroll(t, rr.Body.Bytes()); got.ObjectAccessKey != testClientAK || got.ObjectSecretKey != testClientSK {
				t.Fatalf("poll = %+v, want jm-client", got)
			}
			list := doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil).Body.String()
			store, _ := os.ReadFile(filepath.Join(dir, "principals-store.json"))
			for _, s := range []string{list, string(store)} {
				if strings.Contains(s, "obj_key") || strings.Contains(s, "per_mac") || strings.Contains(s, "keyd") || strings.Contains(s, "auto_approve_pending") {
					t.Fatalf("mode off shows per-Mac fields:\n%s", s)
				}
			}
			if _, err := os.Stat(objDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("mode off touched JM_OBJKEYS_DIR: %v", err)
			}
		})
	}
}

// ---- U4 --------------------------------------------------------------------

// U4: no secret in desired.json, applied.json, the store snapshot or the
// admin API; only in results/ (transiently) and the 0600 store.
func TestObjKeysSecretsStayOutOfSharedFiles(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(7)
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	p := r.rec(name)
	secret := p.ObjKeySecret
	if secret == "" {
		t.Fatal("no secret stored")
	}
	_ = filepath.WalkDir(r.objDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			if d != nil && d.IsDir() && d.Name() == "results" {
				return filepath.SkipDir
			}
			return nil
		}
		b, _ := os.ReadFile(path)
		if bytes.Contains(b, []byte(secret)) {
			t.Errorf("secret found in %s", path)
		}
		if bytes.Contains(b, []byte(`\`)) {
			t.Errorf("%s has a JSON escape keyd refuses", path)
		}
		return nil
	})
	snap, _ := json.Marshal(r.s.Snapshot())
	if bytes.Contains(snap, []byte(secret)) || bytes.Contains(snap, []byte("obj_key_secret")) {
		t.Fatalf("Snapshot carries the secret: %s", snap)
	}
	if !bytes.Contains(snap, []byte(`"obj_key_status":"active"`)) {
		t.Fatalf("Snapshot lacks the key column: %s", snap)
	}
	fi, err := os.Stat(r.statePath)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("store mode %v %v, want 0600", fi.Mode().Perm(), err)
	}
	if b, _ := os.ReadFile(r.statePath); !bytes.Contains(b, []byte(secret)) {
		t.Fatal("the store does not hold the secret (a poll could not re-deliver it)")
	}
	if fi, err := os.Stat(filepath.Join(r.objDir, "desired.json")); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("desired.json mode: %v %v", fi.Mode().Perm(), err)
	}
	// keys is never null, and desired.json is compact.
	if b, _ := os.ReadFile(filepath.Join(r.objDir, "desired.json")); bytes.Contains(b, []byte("null")) || bytes.Contains(b, []byte("\n  ")) {
		t.Fatalf("desired.json not in keyd's form: %s", b)
	}
}

// ---- U5 --------------------------------------------------------------------

// U5: Revoke with keyd down succeeds and removes the Redis login at once;
// Forget is refused until keyd confirms the key gone.
func TestObjKeysRevokeWithKeydDownForgetWaits(t *testing.T) {
	shortObjKeyTimers(t)
	mux, proxyDir, dir, _ := newObjAPI(t)
	objDir := os.Getenv("JM_OBJKEYS_DIR")
	kd := newFakeKeyd(t, objDir)
	stop := runFakeKeyd(kd)

	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", rr.Code, rr.Body)
	}
	key := er.Principal + "-1"
	if _, ok := kd.minio(key); !ok {
		t.Fatal("key not in MinIO after approve")
	}
	stop() // keyd down

	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		t.Fatalf("revoke with keyd down: %d %s", rr.Code, rr.Body)
	}
	if _, ok := findProxyPrincipal(readProxyPrincipals(t, filepath.Join(proxyDir, proxyPrincipalsName)), er.Principal); ok {
		t.Fatal("revoked Mac still in the proxy file")
	}
	list := doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil).Body.String()
	if !strings.Contains(list, `"obj_key_status": "removing"`) {
		t.Fatalf("list does not show the pending removal: %s", list)
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/forget", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "storage key removal not confirmed") {
		t.Fatalf("forget before removal confirmed: %d %s", rr.Code, rr.Body)
	}
	if _, ok := kd.minio(key); !ok {
		t.Fatal("key vanished without keyd")
	}

	stop = runFakeKeyd(kd) // keyd back; the Manager's reconciler confirms
	defer stop()
	deadline := time.Now().Add(3 * time.Second)
	for {
		rr = doJSON(t, mux, http.MethodPost, "/api/principals/forget", h8bTestAdminKey, map[string]string{"name": er.Principal})
		if rr.Code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("forget never allowed after keyd came back: %d %s", rr.Code, rr.Body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := kd.minio(key); ok {
		t.Fatal("forget allowed while the key still exists in MinIO")
	}
	_ = dir
}

func runFakeKeyd(kd *fakeKeyd) (stop func()) {
	done, exited := make(chan struct{}), make(chan struct{})
	kd.restart()
	go func() {
		defer close(exited)
		for {
			kd.step()
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done); <-exited }) }
}

// newObjAPI is newH8BAPI with JM_OBJ_PER_MAC=1 and a keyd directory; the
// Manager's reconciler is stopped when the test ends.
func newObjAPI(t *testing.T) (*http.ServeMux, string, string, *JobManager) {
	t.Helper()
	setObjectKeyEnv(t)
	t.Setenv("JM_OBJ_PER_MAC", "1")
	objDir := filepath.Join(t.TempDir(), "objkeys")
	if err := os.MkdirAll(objDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("JM_OBJKEYS_DIR", objDir)
	var stops []func()
	var mu sync.Mutex
	onObjKeyReconcilerStart = func(stop func()) { mu.Lock(); stops = append(stops, stop); mu.Unlock() }
	t.Cleanup(func() {
		onObjKeyReconcilerStart = nil
		mu.Lock()
		defer mu.Unlock()
		for _, s := range stops {
			s()
		}
	})
	return newH8BAPI(t)
}

// ---- U6 --------------------------------------------------------------------

// U6a: the Manager dies after persisting a new generation but before
// writing desired.json. On restart desired.json is rewritten from the
// store, the same name is minted (not a new one, not a reused one), and
// Approve converges.
func TestObjKeysCrashAfterGenPersisted(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(8)
	r.s.mu.Lock()
	next := *r.s.recs[name]
	if err := r.s.startObjKeyMintLocked(&next); err != nil {
		t.Fatal(err)
	}
	r.s.recs[name] = &next
	if err := r.s.saveStoreLocked(); err != nil {
		t.Fatal(err)
	}
	r.s.mu.Unlock()
	if fileExists(filepath.Join(r.objDir, "desired.json")) {
		if _, ok := r.desired().has(name + "-1"); ok {
			t.Fatal("setup: desired.json already lists the key")
		}
	}
	r.open() // "restart"
	if _, ok := r.desired().has(name + "-1"); !ok {
		t.Fatalf("restart did not rewrite desired.json from the store: %+v", r.desired())
	}
	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	if p := r.rec(name); p.ObjKeyName != name+"-1" {
		t.Fatalf("approved with %s, want %s-1", p.ObjKeyName, name)
	}
	r.checkInvariant("after crash")
}

// U6b: keyd creates the key, then dies before writing its result. The
// Manager sees the key in applied.json with no result, asks for a
// re-creation with needs_secret at a higher seq, adopts the new secret, and
// clears needs_secret in desired.json BEFORE deleting the result file, so a
// later seq never re-creates (and so invalidates) the delivered key.
func TestObjKeysCrashBeforeResultRecreates(t *testing.T) {
	r := newObjRig(t)
	name, tok := r.enroll(9)
	key := name + "-1"
	r.kd.mu.Lock()
	r.kd.crashNextCreate, r.kd.stayDown = true, true
	r.kd.mu.Unlock()
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with keyd silent: %v", err)
	}
	r.kd.step() // creates the key, "crashes" before the result
	if _, ok := r.kd.minio(key); !ok {
		t.Fatal("setup: key not created")
	}
	r.kd.restart()
	r.kd.step() // keyd back: applied.json shows the key, no result exists
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	d := r.desired()
	if needs, ok := d.has(key); !ok || !needs {
		t.Fatalf("Manager did not ask for a re-creation: %+v", d)
	}
	r.kd.mu.Lock()
	cs := r.kd.createdSeq[key]
	r.kd.mu.Unlock()
	if d.Seq <= cs {
		t.Fatalf("needs_secret at seq %d, not above keyd's create seq %d", d.Seq, cs)
	}
	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatalf("approve after recovery: %v", err)
	}
	r.keydDown()
	if needs, _ := r.desired().has(key); needs {
		t.Fatal("desired.json still has needs_secret after the secret was taken")
	}
	if fileExists(filepath.Join(r.objDir, "results", key+".json")) {
		t.Fatal("result file not consumed")
	}
	// Another Mac's approval raises seq; keyd must not re-create this key.
	r.keydUp()
	other, _ := r.enroll(10)
	if err := r.s.Approve(other); err != nil {
		t.Fatal(err)
	}
	r.waitFor("keyd applied the newer seq", func() bool {
		a, _ := r.s.objk.readApplied()
		return a != nil && a.Seq >= r.desired().Seq
	})
	if n := r.kd.createCount(key); n != 2 {
		t.Fatalf("%s created %d times, want 2 (original + one re-creation)", key, n)
	}
	r.checkInvariant("after recovery")
	if got := r.poll(9, tok); got.ObjectSecretKey != r.rec(name).ObjKeySecret {
		t.Fatal("poll does not deliver the re-created secret")
	}
}

// A second crash during a needs_secret re-create also loses the result.
// Each fully applied request without a result needs a new seq; an older
// applied.json must not cause the Manager to keep issuing requests.
func TestObjKeysRepeatedCrashBeforeResultReissues(t *testing.T) {
	r := newObjRig(t)
	name, tok := r.enroll(9)
	key := name + "-1"
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with keyd silent: %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		requested := r.desired()
		r.kd.mu.Lock()
		r.kd.crashNextCreate = true
		r.kd.mu.Unlock()
		r.kd.step() // MinIO made the key, but keyd died before the result
		if fileExists(filepath.Join(r.objDir, "results", key+".json")) {
			t.Fatalf("attempt %d unexpectedly wrote a result", attempt)
		}
		if _, ok := r.kd.minio(key); !ok {
			t.Fatalf("attempt %d did not create the key", attempt)
		}
		r.kd.restart()
		r.kd.step() // applied.json reports the key, still without a result
		applied, _ := r.s.objk.readApplied()
		if applied == nil || applied.Seq != requested.Seq || !applied.exists(key) {
			t.Fatalf("attempt %d did not apply request %d with the key present: %+v", attempt, requested.Seq, applied)
		}
		if err := r.s.ReconcileObjKeys(); err != nil {
			t.Fatal(err)
		}
		retry := r.desired()
		if needs, ok := retry.has(key); !ok || !needs || retry.Seq <= requested.Seq {
			t.Fatalf("attempt %d did not request a new re-create: before=%+v after=%+v", attempt, requested, retry)
		}
		if p := r.rec(name); p.ObjKeyMintSeq != retry.Seq || p.ObjKeySecret != "" || p.Status == PrincipalApproved {
			t.Fatalf("attempt %d committed an unready key: %+v", attempt, p)
		}
		for i := 0; i < 3; i++ {
			if err := r.s.ReconcileObjKeys(); err != nil {
				t.Fatal(err)
			}
			if got := r.desired().Seq; got != retry.Seq {
				t.Fatalf("stale applied seq raised request %d from %d to %d", attempt, retry.Seq, got)
			}
		}
	}

	r.kd.step() // third create succeeds and writes its result first
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Approve(name); err != nil {
		t.Fatalf("approve after repeated crash recovery: %v", err)
	}
	if needs, _ := r.desired().has(key); needs {
		t.Fatal("delivered secret left needs_secret set")
	}
	if got := r.kd.createCount(key); got != 3 {
		t.Fatalf("keyd created %s %d times, want two crashes and one delivery", key, got)
	}
	r.checkInvariant("after repeated crash recovery")
	secret := r.rec(name).ObjKeySecret
	if got := r.poll(9, tok); got.ObjectSecretKey != secret {
		t.Fatal("poll did not deliver the recovered secret")
	}

	// A later, unrelated desired rewrite must not re-create a delivered key.
	if err := r.s.syncObjKeys(); err != nil {
		t.Fatal(err)
	}
	r.kd.step()
	if got := r.kd.createCount(key); got != 3 || r.rec(name).ObjKeySecret != secret {
		t.Fatalf("delivered key was re-created after a later seq: creates=%d", got)
	}
	r.checkInvariant("after later seq")
}

// U6c: a result read whose persist fails is not lost: the file stays and
// the next cycle delivers the same secret.
func TestObjKeysResultKeptUntilPersisted(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(11)
	key := name + "-1"
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatal(err)
	}
	r.kd.step()
	res := filepath.Join(r.objDir, "results", key+".json")
	if !fileExists(res) {
		t.Fatal("setup: no result")
	}
	atomicWriteHook = func(f *os.File, data []byte) error {
		if strings.Contains(f.Name(), "principals-store.json") {
			return errors.New("injected: disk full")
		}
		_, err := f.Write(data)
		return err
	}
	err := r.s.ReconcileObjKeys()
	atomicWriteHook = nil
	if err == nil {
		t.Fatal("reconcile did not report the failed persist")
	}
	if !fileExists(res) {
		t.Fatal("result deleted although its secret was never persisted")
	}
	if p := r.rec(name); p.ObjKeySecret != "" {
		t.Fatal("in-memory record kept a secret the store does not hold")
	}
	minio, _ := r.kd.minio(key)
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	if p := r.rec(name); p.ObjKeySecret != minio.secret || p.ObjKeyState != ObjKeyActive {
		t.Fatalf("after retry: state=%s secret matches=%v", p.ObjKeyState, p.ObjKeySecret == minio.secret)
	}
	if fileExists(res) {
		t.Fatal("result not deleted after persist")
	}
}

// U6d: a store restored from an older copy (keyd reports a newer
// generation than the store knows): the Manager never re-uses a name keyd
// has seen; it mints past it.
func TestObjKeysRestoredStoreNeverReusesName(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(12)
	backup, _ := os.ReadFile(r.statePath) // before any key
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	r.keydDown()
	if err := os.WriteFile(r.statePath, backup, 0o600); err != nil {
		t.Fatal(err)
	}
	r.open()
	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	p := r.rec(name)
	if p.ObjKeyName == name+"-1" {
		t.Fatalf("restored store re-used %s", p.ObjKeyName)
	}
	if n := r.kd.createCount(name + "-1"); n != 1 {
		t.Fatalf("%s-1 created %d times", name, n)
	}
	r.checkInvariant("restored")
}

// ---- U7 --------------------------------------------------------------------

// U7: generations rise across revoke, re-approve and forget + re-enroll;
// no name is ever created twice.
func TestObjKeysGenMonotonic(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(13)
	want := []string{name + "-1", name + "-2", name + "-3"}
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Revoke(name); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Approve(name); err != nil { // re-approve before removal confirmed
		t.Fatal(err)
	}
	if err := r.s.Revoke(name); err != nil {
		t.Fatal(err)
	}
	r.waitFor("removals confirmed", func() bool { return r.s.Forget(name) == nil })
	r.open() // restart between forget and re-enroll
	name2, _ := r.enroll(13)
	if name2 != name {
		t.Fatal("setup")
	}
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	if got := r.rec(name).ObjKeyName; got != want[2] {
		t.Fatalf("third key = %s, want %s", got, want[2])
	}
	for _, k := range want {
		if n := r.kd.createCount(k); n != 1 {
			t.Fatalf("%s created %d times, want once", k, n)
		}
	}
	for _, k := range want[:2] {
		if _, ok := r.kd.minio(k); ok {
			t.Fatalf("revoked key %s still in MinIO", k)
		}
	}
	r.checkInvariant("u7")
}

// ---- mass delete -----------------------------------------------------------

// Revoking several Macs while keyd is down: the Manager confirms the mass
// delete (allow_mass_delete = seq) because each removal is an explicit
// revoke, so keyd removes them all.
func TestObjKeysMassRevokeConfirmed(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	var names []string
	for i := 20; i < 24; i++ {
		n, _ := r.enroll(i)
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	r.keydDown()
	for _, n := range names {
		if err := r.s.Revoke(n); err != nil {
			t.Fatal(err)
		}
	}
	r.keydUp()
	r.waitFor("all four removed", func() bool {
		for _, n := range names {
			if _, ok := r.kd.minio(n + "-1"); ok {
				return false
			}
		}
		return true
	})
	for _, n := range names {
		if err := r.s.Forget(n); err != nil {
			r.waitFor("removal confirmed "+n, func() bool { return r.s.Forget(n) == nil })
		}
	}
}

// A lost store (empty) must not remove the keys it does not know, nor
// confirm a mass delete: they stay in desired.json as orphans (no
// allow_mass_delete), keyd accepts the file, and every Mac keeps its key.
func TestObjKeysLostStoreDoesNotMassDelete(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	var names []string
	for i := 30; i < 34; i++ {
		n, _ := r.enroll(i)
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
		names = append(names, n)
	}
	r.keydDown()
	if err := os.Remove(r.statePath); err != nil {
		t.Fatal(err)
	}
	r.open()
	d := r.desired()
	if d.AllowMassDelete != 0 || len(d.Keys) != len(names) {
		t.Fatalf("lost store desired = %+v, want the %d orphan keys kept and no mass-delete confirmation", d, len(names))
	}
	for _, n := range names {
		if needs, ok := d.has(n + "-1"); !ok || needs {
			t.Fatalf("orphan %s-1 not kept as is in desired.json: %+v", n, d)
		}
	}
	r.keydUp()
	r.waitFor("keyd applied the lost store's desired.json", func() bool {
		a, _ := r.s.objk.readApplied()
		return a != nil && a.Seq >= d.Seq
	})
	if a, _ := r.s.objk.readApplied(); a.Error != "" {
		t.Fatalf("keyd refused the lost store's desired.json: %s", a.Error)
	}
	if got := len(r.s.Snapshot().OrphanKeys); got != len(names) {
		t.Fatalf("Principals page lists %d orphan keys, want %d", got, len(names))
	}
	for _, n := range names {
		if _, ok := r.kd.minio(n + "-1"); !ok {
			t.Fatalf("%s-1 removed after a store loss", n)
		}
	}
}

// The Principals page renders the key column and the red removal badge.
func TestObjKeysPrincipalsPageKeyColumn(t *testing.T) {
	raw, err := staticFS.ReadFile("static/principals.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(raw)
	for _, want := range []string{"keyCell(p)", "p.obj_key_status", "Storage key NOT yet removed", "badge-bad", "s.keyd_problem",
		"waiting for storage key", "p.auto_approve_pending", "s.per_mac_keys"} {
		if !strings.Contains(page, want) {
			t.Fatalf("principals.html does not render %q", want)
		}
	}
}

// U6e: the store (generation, seq) is persisted BEFORE desired.json names
// a key. If the store cannot be saved, keyd never hears of the name, so a
// crash can never leave keyd minting a name the store does not record.
func TestObjKeysStorePersistedBeforeDesired(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(40)
	atomicWriteHook = func(f *os.File, data []byte) error {
		if strings.Contains(f.Name(), "principals-store.json") {
			return errors.New("injected: disk full")
		}
		_, err := f.Write(data)
		return err
	}
	err := r.s.Approve(name)
	atomicWriteHook = nil
	if !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with the store unwritable = %v", err)
	}
	if _, ok := r.desired().has(name + "-1"); ok {
		t.Fatal("desired.json names a key the store never recorded")
	}
	r.keydUp()
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	r.checkInvariant("after store failure")
}

// A refused Approve of a revoked Mac leaves a key being minted; revoking
// it again retires that key, and Forget is possible once keyd removed it.
func TestObjKeysRevokeAgainRetiresMintingKey(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(41)
	if err := r.s.Revoke(name); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with keyd silent: %v", err)
	}
	r.kd.step() // keyd creates the key after all
	if err := r.s.Forget(name); !errors.Is(err, ErrObjectKeyRemovalPending) {
		t.Fatalf("forget with a key minted = %v, want removal pending", err)
	}
	if err := r.s.Revoke(name); err != nil {
		t.Fatal(err)
	}
	r.keydUp()
	r.waitFor("forget allowed", func() bool { return r.s.Forget(name) == nil })
	if _, ok := r.kd.minio(name + "-1"); ok {
		t.Fatal("forgotten Mac's key still in MinIO")
	}
}
