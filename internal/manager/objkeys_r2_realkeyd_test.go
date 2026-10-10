package manager

// Round 2 with the REAL keyd.sh (round-2 keyd: needs_secret latch,
// mass-delete guard). keyd is stepped one cycle at a time (once) or run in
// the background (up).

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// realKeydStrandedKey: keyd made the key but its result never reached the
// Manager (keyd killed between create and result). The Manager asks for a
// re-creation with needs_secret.
func realKeydStrandedKey(t *testing.T, i int) (*objRig, *realKeyd, string, string) {
	t.Helper()
	r := newObjRig(t)
	kd := newRealKeyd(t, r.objDir)
	name, _ := r.enroll(i)
	key := name + "-1"
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with keyd down: %v", err)
	}
	if err := os.WriteFile(filepath.Join(kd.state, "sa."+key), []byte("jm-macs on\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	kd.once()
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	if needs, ok := r.desired().has(key); !ok || !needs {
		t.Fatalf("Manager did not ask for a re-creation: %+v", r.desired())
	}
	return r, kd, name, key
}

// The needs_secret path with the real keyd: one re-creation, and the
// delivered secret survives later seq raises (keyd's latch).
func TestObjKeysRealKeydStrandedKeyRecovers(t *testing.T) {
	r, kd, name, key := realKeydStrandedKey(t, 40)
	kd.once()
	if !fileExists(filepath.Join(r.objDir, "results", key+".json")) {
		t.Fatalf("keyd did not re-create %s", key)
	}
	for i := 0; i < 3; i++ {
		if err := r.s.ReconcileObjKeys(); err != nil {
			t.Fatal(err)
		}
		kd.once()
	}
	if r.rec(name).ObjKeySecret != kd.secret(key) || r.rec(name).ObjKeyState != ObjKeyActive {
		t.Fatalf("secret not stored / not active: %+v", r.rec(name).ObjKeyState)
	}
	objKeyWait = 5 * time.Second
	other, _ := r.enroll(41)
	kd.up()
	if err := r.s.Approve(other); err != nil {
		t.Fatal(err)
	}
	kd.down()
	if r.rec(name).ObjKeySecret != kd.secret(key) {
		t.Fatal("delivered secret killed by a later seq raise")
	}
	a, _ := os.ReadFile(filepath.Join(r.objDir, "applied.json"))
	if strings.Contains(string(a), "ignored") || strings.Contains(string(a), "refused") {
		t.Fatalf("applied: %s", a)
	}
}

// The result of a requested re-creation is unusable. The Manager clears
// needs_secret (and its mint seq) in desired.json before it discards the
// file, so keyd's latch releases, and the next request re-creates the key.
func TestObjKeysRealKeydCorruptResultDuringRecreate(t *testing.T) {
	r, kd, name, key := realKeydStrandedKey(t, 42)
	kd.once()
	rp := filepath.Join(r.objDir, "results", key+".json")
	if !fileExists(rp) {
		t.Fatalf("keyd did not re-create %s", key)
	}
	if err := os.WriteFile(rp, []byte(`{"name":"`+key+`","secret_key":"short","created_ms":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	if fileExists(rp) {
		t.Fatal("setup: Manager kept the unusable result")
	}
	if needs, _ := r.desired().has(key); needs {
		t.Fatal("unusable result discarded while desired.json still has needs_secret")
	}
	for i := 0; i < 8 && r.rec(name).ObjKeySecret == ""; i++ {
		kd.once()
		if err := r.s.ReconcileObjKeys(); err != nil {
			t.Fatal(err)
		}
	}
	p := r.rec(name)
	if p.ObjKeySecret == "" || p.ObjKeySecret != kd.secret(key) {
		a, _ := os.ReadFile(filepath.Join(r.objDir, "applied.json"))
		sf, _ := os.ReadFile(filepath.Join(r.objDir, "keyd-created-seq"))
		t.Fatalf("STUCK: %s never gets a secret. needs=%v state=%s mintSeq=%d\napplied=%s\nkeyd-created-seq=%s", key, p.ObjKeyNeedsSecret, p.ObjKeyState, p.ObjKeyMintSeq, a, sf)
	}
}

// A lost store with the real keyd: approvals after re-enrollment are not
// refused for a mass delete (the old keys are kept as orphans), and the
// admin's removal of the orphans is accepted by keyd's guard.
func TestObjKeysRealKeydLostStoreOrphans(t *testing.T) {
	r := newObjRig(t)
	objKeyWait = 5 * time.Second
	kd := newRealKeyd(t, r.objDir)
	kd.up()
	var names []string
	for i := 80; i < 82; i++ {
		n, _ := r.enroll(i)
		if err := r.s.Approve(n); err != nil {
			t.Fatalf("approve %s: %v", n, err)
		}
		names = append(names, n)
	}
	if err := os.Remove(r.statePath); err != nil {
		t.Fatal(err)
	}
	r.open()
	for i := 80; i < 82; i++ {
		n, _ := r.enroll(i)
		if err := r.s.Approve(n); err != nil {
			a, _ := os.ReadFile(filepath.Join(r.objDir, "applied.json"))
			t.Fatalf("approve %s after store loss: %v\napplied=%s", n, err, a)
		}
	}
	for _, n := range names {
		if !kd.exists(n+"-1") || !kd.exists(n+"-2") {
			t.Fatalf("%s: -1 (orphan) %v, -2 (new) %v; want both", n, kd.exists(n+"-1"), kd.exists(n+"-2"))
		}
	}
	orphans := []string{names[0] + "-1", names[1] + "-1"}
	if err := r.s.RemoveOrphanObjKeys(orphans); err != nil {
		t.Fatal(err)
	}
	r.waitFor("orphans removed", func() bool { return len(r.s.Snapshot().OrphanKeys) == 0 })
	for _, n := range names {
		if kd.exists(n+"-1") || !kd.exists(n+"-2") {
			t.Fatalf("%s after orphan removal: -1 %v, -2 %v", n, kd.exists(n+"-1"), kd.exists(n+"-2"))
		}
	}
	a, _ := os.ReadFile(filepath.Join(r.objDir, "applied.json"))
	if strings.Contains(string(a), "REFUSED") {
		t.Fatalf("applied: %s", a)
	}
}

// A key created after the Manager's applied snapshot is not covered by an
// unrelated revoke or by allow_mass_delete. keyd reports it for admin review.
func TestObjKeysRealKeydPreservesUnobservedKeyDuringNamedRemoval(t *testing.T) {
	r := newObjRig(t)
	objKeyWait = 5 * time.Second
	kd := newRealKeyd(t, r.objDir)
	kd.up()
	a, _ := r.enroll(91)
	b, _ := r.enroll(92)
	for _, n := range []string{a, b} {
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
	}
	kd.down()
	if err := r.s.Revoke(a); err != nil {
		t.Fatal(err)
	}
	d := r.desired()
	if len(d.Remove) != 1 || d.Remove[0] != a+"-1" {
		t.Fatalf("revoke authority = %v", d.Remove)
	}
	const orphan = "mac-aabbccddeeff-7"
	if _, ok := d.has(orphan); ok {
		t.Fatal("fixture already observed orphan")
	}
	if err := os.WriteFile(filepath.Join(kd.state, "sa."+orphan), []byte("jm-macs on\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	kd.once()
	if kd.exists(a+"-1") || !kd.exists(b+"-1") || !kd.exists(orphan) {
		t.Fatalf("named revoke removed wrong keys: revoked=%v retained=%v orphan=%v", kd.exists(a+"-1"), kd.exists(b+"-1"), kd.exists(orphan))
	}
	if !strings.Contains(kd.argv(), "svcacct ls") {
		t.Fatal("VALIDITY: real keyd did not enumerate fake MinIO")
	}
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, o := range r.s.Snapshot().OrphanKeys {
		if o.Name == orphan {
			found = true
		}
	}
	if !found {
		t.Fatal("unobserved key not reported to Manager")
	}
	if err := r.s.RemoveOrphanObjKeys([]string{orphan}); err != nil {
		t.Fatal(err)
	}
	kd.once()
	if kd.exists(orphan) || !kd.exists(b+"-1") {
		t.Fatal("confirmed orphan removal affected another key or did not apply")
	}
}

// The old format and allow_mass_delete alone grant no deletion authority.
func TestObjKeysRealKeydMassFlagCannotAuthorizeUnnamedKey(t *testing.T) {
	r := newObjRig(t)
	kd := newRealKeyd(t, r.objDir)
	const orphan = "mac-aabbccddeeff-8"
	if err := os.WriteFile(filepath.Join(kd.state, "sa."+orphan), []byte("jm-macs on\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := r.desired()
	legacy := fmt.Sprintf(`{"seq":%d,"allow_mass_delete":%d,"keys":[]}`, d.Seq+1, d.Seq+1)
	if err := os.WriteFile(filepath.Join(r.objDir, objKeyDesiredName), []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	kd.once()
	if !kd.exists(orphan) {
		t.Fatal("allow_mass_delete authorized an unnamed key")
	}
	a, _ := os.ReadFile(filepath.Join(r.objDir, objKeyAppliedName))
	if !strings.Contains(string(a), orphan) || !strings.Contains(string(a), "preserved") {
		t.Fatalf("unobserved key not reported: %s", a)
	}
}
