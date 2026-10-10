package manager

// Per-Mac object keys, review round 2 (L2 review F1-F5, keyd round-2
// combined review, L3 interop). Fake keyd unless the name says RealKeyd.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---- F1: lost or restored store ---------------------------------------------

// A store lost with 4 approved Macs: the Macs re-enroll and the admin
// approves each. No approval is wedged by the old keys (they are orphans,
// kept in desired.json), the orphans are listed with a red badge on each
// Mac's row, and only the admin's "Remove orphan keys" removes them, with
// allow_mass_delete.
func TestObjKeysLostStoreOrphansDoNotWedge(t *testing.T) {
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
	r.keydUp()
	for i := 30; i < 34; i++ {
		n, _ := r.enroll(i)
		if err := r.s.Approve(n); err != nil {
			a, _ := r.s.objk.readApplied()
			t.Fatalf("approve %s after a store loss: %v (keyd: %q)", n, err, a.Error)
		}
	}
	snap := r.s.Snapshot()
	if len(snap.OrphanKeys) != 4 {
		t.Fatalf("orphan keys listed = %+v, want the 4 old keys", snap.OrphanKeys)
	}
	for _, n := range names {
		p := snap.byName(n)
		if p.ObjKeyName != n+"-2" || len(p.ObjKeyOrphans) != 1 || p.ObjKeyOrphans[0] != n+"-1" {
			t.Fatalf("%s: key %s orphans %v, want new key -2 and orphan -1 on its row", n, p.ObjKeyName, p.ObjKeyOrphans)
		}
		if _, ok := r.kd.minio(n + "-1"); !ok {
			t.Fatalf("%s-1 removed before the admin confirmed it", n)
		}
	}
	if d := r.desired(); d.AllowMassDelete != 0 {
		t.Fatalf("allow_mass_delete %d sent without the admin's confirmation", d.AllowMassDelete)
	}
	var orphans []string
	for _, o := range snap.OrphanKeys {
		orphans = append(orphans, o.Name)
	}
	if err := r.s.RemoveOrphanObjKeys(orphans); err != nil {
		t.Fatal(err)
	}
	if d := r.desired(); d.AllowMassDelete != d.Seq {
		t.Fatalf("orphan removal desired = %+v, want allow_mass_delete = seq", d)
	}
	r.waitFor("orphans removed and confirmed", func() bool {
		return len(r.s.Snapshot().OrphanKeys) == 0
	})
	for _, n := range names {
		if _, ok := r.kd.minio(n + "-1"); ok {
			t.Fatalf("orphan %s-1 still in MinIO", n)
		}
	}
	r.checkInvariant("after orphan removal")
}

// Only current orphans can be removed: a Mac's own key, an unknown name,
// or the action with per-Mac keys off is refused and changes nothing.
func TestObjKeysRemoveOrphanRefusesNonOrphan(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	n, _ := r.enroll(35)
	if err := r.s.Approve(n); err != nil {
		t.Fatal(err)
	}
	before := r.desired().Seq
	for _, bad := range []string{n + "-1", n + "-9", "mac-0123456789ab-1"} {
		if err := r.s.RemoveOrphanObjKeys([]string{bad}); !errors.Is(err, ErrNotOrphanKey) {
			t.Fatalf("remove %s = %v, want ErrNotOrphanKey", bad, err)
		}
	}
	if r.desired().Seq != before {
		t.Fatal("a refused orphan removal wrote desired.json")
	}
	if _, ok := r.kd.minio(n + "-1"); !ok {
		t.Fatal("approved Mac's key removed")
	}
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := off.RemoveOrphanObjKeys([]string{n + "-1"}); !errors.Is(err, ErrObjKeysOff) {
		t.Fatalf("flag off: %v", err)
	}
}

// HTTP: the route is admin-only, refuses malformed names (400) and
// non-orphans (409, with the reason), and answers the snapshot on success.
func TestObjKeysOrphanRouteHTTP(t *testing.T) {
	mux, _, _, _ := newObjAPI(t)
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/orphan-keys/remove", "", map[string]any{"names": []string{"mac-0123456789ab-1"}}); rr.Code != http.StatusUnauthorized {
		t.Fatalf("no admin key: %d", rr.Code)
	}
	for _, body := range []any{map[string]any{"names": []string{}}, map[string]any{"names": []string{"jm-client"}}, map[string]any{"names": []string{"mac-0123456789ab-1"}, "x": 1}} {
		if rr := doJSON(t, mux, http.MethodPost, "/api/principals/orphan-keys/remove", h8bTestAdminKey, body); rr.Code != http.StatusBadRequest {
			t.Fatalf("%v: %d %s", body, rr.Code, rr.Body)
		}
	}
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/orphan-keys/remove", h8bTestAdminKey, map[string]any{"names": []string{"mac-0123456789ab-1"}})
	if rr.Code != http.StatusConflict || !strings.Contains(rr.Body.String(), "is not an orphan storage key now") {
		t.Fatalf("non-orphan: %d %s", rr.Code, rr.Body)
	}
}

// A store restored from a backup taken before two Macs were re-keyed
// (gen 2 live in MinIO and in each Mac). The store adopts gen 2 as the
// floor and mints gen 3; gen 1 (its own) is removed, gen 2 is an orphan
// kept live (the Macs use it) with a red badge. Approvals are not wedged,
// and a revoked Mac's live orphan stays flagged on its row.
func TestObjKeysRestoreAfterRotationNoWedge(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	a1, _ := r.enroll(50)
	a2, _ := r.enroll(51)
	for _, n := range []string{a1, a2} {
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
	}
	backup, _ := os.ReadFile(r.statePath)
	for _, n := range []string{a1, a2} {
		if err := r.s.Revoke(n); err != nil {
			t.Fatal(err)
		}
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
	}
	r.waitFor("gen1 removed", func() bool {
		_, x := r.kd.minio(a1 + "-1")
		_, y := r.kd.minio(a2 + "-1")
		return !x && !y
	})
	r.keydDown()
	if err := os.WriteFile(r.statePath, backup, 0o600); err != nil {
		t.Fatal(err)
	}
	r.open()
	r.keydUp()
	for _, n := range []string{a1, a2} {
		n := n
		r.waitFor(n+" gen 3 active", func() bool { p := r.rec(n); return p.ObjKeyName == n+"-3" && p.ObjKeyState == ObjKeyActive })
		if _, ok := r.kd.minio(n + "-2"); !ok {
			t.Fatalf("%s-2 (the key the Mac holds) removed without the admin", n)
		}
	}
	n3, _ := r.enroll(52)
	if err := r.s.Approve(n3); err != nil {
		t.Fatalf("approve after restore: %v", err)
	}
	if err := r.s.Revoke(a1); err != nil {
		t.Fatal(err)
	}
	r.waitFor("a1 gen3 removal confirmed", func() bool { return len(r.rec(a1).ObjKeyRemoving) == 0 })
	if p := r.s.Snapshot().byName(a1); len(p.ObjKeyOrphans) != 1 || p.ObjKeyOrphans[0] != a1+"-2" {
		t.Fatalf("revoked %s with live orphan %s-2: row orphans = %v, want the red badge", a1, a1, p.ObjKeyOrphans)
	}
	for _, k := range []string{a1 + "-1", a1 + "-2", a1 + "-3", a2 + "-3"} {
		if n := r.kd.createCount(k); n > 1 {
			t.Fatalf("%s created %d times", k, n)
		}
	}
	if err := r.s.RemoveOrphanObjKeys([]string{a1 + "-2"}); err != nil {
		t.Fatal(err)
	}
	r.waitFor("a1-2 removed", func() bool { _, ok := r.kd.minio(a1 + "-2"); return !ok })
}

// ---- F2: a failed Revoke write is retried ---------------------------------

// Revoke's desired.json write fails once (EIO on the keyd directory) after
// the store was saved: the reconciler writes it again and keyd removes the
// key.
func TestObjKeysRevokeDesiredFailureRetried(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(93)
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	atomicWriteHook = func(f *os.File, data []byte) error {
		if filepath.Dir(f.Name()) == r.objDir {
			return errors.New("injected: EIO on objkeys")
		}
		_, err := f.Write(data)
		return err
	}
	err := r.s.Revoke(name)
	atomicWriteHook = nil
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	r.waitFor("revoked key removed", func() bool { _, ok := r.kd.minio(name + "-1"); return !ok })
	r.waitFor("forget allowed", func() bool { return r.s.Forget(name) == nil })
}

// Revoke's store save inside the desired.json write fails (the revoke
// itself was committed): the removal has no seq yet, so the reconciler
// writes desired.json and keyd removes the key.
func TestObjKeysRevokeStoreFailureRetried(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	name, _ := r.enroll(95)
	if err := r.s.Approve(name); err != nil {
		t.Fatal(err)
	}
	r.keydDown()
	writes := 0
	atomicWriteHook = func(f *os.File, data []byte) error {
		if strings.Contains(f.Name(), "principals-store.json") {
			if writes++; writes == 2 { // 1 = the revoke's commit, 2 = desired.json's
				return errors.New("injected: disk full")
			}
		}
		_, err := f.Write(data)
		return err
	}
	err := r.s.Revoke(name)
	atomicWriteHook = nil
	if err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if rm := r.rec(name).ObjKeyRemoving; len(rm) != 1 || rm[0].Seq != 0 {
		t.Fatalf("setup: removal %+v, want one without a seq", rm)
	}
	r.keydUp()
	r.waitFor("revoked key removed", func() bool { _, ok := r.kd.minio(name + "-1"); return !ok })
}

// Revoke returns an error (HTTP 500), not 200, when it cannot persist its
// intent at all; the Mac stays approved and nothing is retired.
func TestObjKeysRevokeUnpersistedIsAnError(t *testing.T) {
	mux, _, _, _ := newObjAPI(t)
	stop := runFakeKeyd(newFakeKeyd(t, os.Getenv("JM_OBJKEYS_DIR")))
	defer stop()
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal}); rr.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", rr.Code, rr.Body)
	}
	atomicWriteHook = func(f *os.File, data []byte) error { return errors.New("injected: disk full") }
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": er.Principal})
	atomicWriteHook = nil
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("revoke with nothing writable: %d %s", rr.Code, rr.Body)
	}
	var snap PrincipalsSnapshot
	json.Unmarshal(doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil).Body.Bytes(), &snap)
	if p := snap.byName(er.Principal); p.Status != PrincipalApproved || p.ObjKeyStatus != "active" {
		t.Fatalf("after a failed revoke: %+v", p)
	}
}

// ---- F3: the auto-approve window --------------------------------------------

// Closing the window stops a Mac it admitted that still waits for its key:
// it becomes an ordinary pending request at once, and is not approved when
// the key arrives.
func TestObjKeysClosedWindowStopsPendingAutoApproval(t *testing.T) {
	r := newObjRig(t)
	if _, err := r.s.SetAutoApproveWindow(time.Hour, 5); err != nil {
		t.Fatal(err)
	}
	name, _ := r.enroll(94)
	if !r.rec(name).AutoApprovePending {
		t.Fatal("setup: not auto-admitted")
	}
	if _, err := r.s.SetAutoApproveWindow(0); err != nil {
		t.Fatal(err)
	}
	if r.rec(name).AutoApprovePending {
		t.Fatal("closing the window left the Mac waiting for auto-approval")
	}
	r.keydUp()
	r.waitFor("key active", func() bool { return r.rec(name).ObjKeyState == ObjKeyActive })
	for i := 0; i < 20; i++ {
		_ = r.s.ReconcileObjKeys()
	}
	if p := r.rec(name); p.Status != PrincipalPending || p.AutoApproved {
		t.Fatalf("Mac approved after the window closed: %+v", p.Status)
	}
	if err := r.s.Approve(name); err != nil {
		t.Fatalf("admin approve: %v", err)
	}
}

// A window that EXPIRED (not closed) while keyd was down: the reconciler
// re-checks it at approval time and does not approve.
func TestObjKeysExpiredWindowStopsPendingAutoApproval(t *testing.T) {
	r := newObjRig(t)
	if _, err := r.s.SetAutoApproveWindow(time.Hour, 5); err != nil {
		t.Fatal(err)
	}
	name, _ := r.enroll(96)
	r.s.mu.Lock()
	r.s.autoUntil = time.Now().Add(-time.Second)
	r.s.mu.Unlock()
	r.keydUp()
	r.waitFor("key active", func() bool { return r.rec(name).ObjKeyState == ObjKeyActive })
	for i := 0; i < 20; i++ {
		_ = r.s.ReconcileObjKeys()
	}
	if p := r.rec(name); p.Status != PrincipalPending {
		t.Fatalf("Mac auto-approved after the window expired: %s", p.Status)
	}
}

// ---- 5/6: object_key_in_use and object_key_pending --------------------------

// The Mac's in-use report is accepted (strict decoder), validated (only
// its own per-Mac key names or the shared name; never another Mac's key or
// anything secret-shaped), recorded and shown.
func TestObjKeysEnrollObjectKeyInUse(t *testing.T) {
	mux, _, _, _ := newObjAPI(t)
	stop := runFakeKeyd(newFakeKeyd(t, os.Getenv("JM_OBJKEYS_DIR")))
	defer stop()
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret})
	er := decodeEnroll(t, rr.Body.Bytes())
	own := er.Principal + "-1"
	poll := func(inUse string) int {
		return doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "poll_token": er.PollToken, "object_key_in_use": inUse}).Code
	}
	for _, bad := range []string{"mac-0123456789ab-1", testClientSK, er.Principal, own + "x", "jm-gc"} {
		if code := poll(bad); code != http.StatusBadRequest {
			t.Fatalf("in-use %q: %d, want 400", bad, code)
		}
	}
	if code := poll(own); code != http.StatusAccepted {
		t.Fatalf("in-use own key: %d", code)
	}
	var snap PrincipalsSnapshot
	json.Unmarshal(doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil).Body.Bytes(), &snap)
	if got := snap.byName(er.Principal).ObjKeyInUse; got != own {
		t.Fatalf("in use shown = %q, want %q", got, own)
	}
	if code := poll(testClientAK); code != http.StatusAccepted {
		t.Fatalf("in-use shared key: %d", code)
	}
	json.Unmarshal(doJSON(t, mux, http.MethodGet, "/api/principals", h8bTestAdminKey, nil).Body.Bytes(), &snap)
	if got := snap.byName(er.Principal).ObjKeyInUse; got != testClientAK {
		t.Fatalf("in use shown = %q, want the shared key", got)
	}
}

// An approved Mac whose own key is not active gets object_key_pending
// (and no key), so it polls at the pending interval; once the key is
// active it gets the key and no pending flag.
func TestObjKeysApprovedMacGetsPendingFlag(t *testing.T) {
	setObjectKeyEnv(t)
	r := newObjRig(t)
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	r.s = off
	name, tok := r.enroll(97)
	if err := off.Approve(name); err != nil {
		t.Fatal(err)
	}
	if got := r.poll(97, tok); got.ObjectKeyPending {
		t.Fatalf("mode off: pending flag sent: %+v", got)
	}
	r.open() // mode on, keyd down: backfill minting
	got := r.poll(97, tok)
	if got.Status != PrincipalApproved || !got.ObjectKeyPending || got.ObjectAccessKey != "" {
		t.Fatalf("approved, key minting: poll = %+v, want object_key_pending and no key", got)
	}
	r.keydUp()
	r.waitFor("key active", func() bool { return r.rec(name).ObjKeyState == ObjKeyActive })
	got = r.poll(97, tok)
	if got.ObjectKeyPending || got.ObjectAccessKey != name+"-1" {
		t.Fatalf("key active: poll = %+v", got)
	}
}

// ---- 7: names are never reused across a restore -----------------------------

// Approve, back up, revoke, forget, restore the backup: the restored
// store's key is gone from keyd, so it is retired and a new generation is
// minted; the old name is never created a second time.
func TestObjKeysRestoreAfterForgetNeverReusesName(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	n, _ := r.enroll(60)
	if err := r.s.Approve(n); err != nil {
		t.Fatal(err)
	}
	backup, _ := os.ReadFile(r.statePath)
	if err := r.s.Revoke(n); err != nil {
		t.Fatal(err)
	}
	r.waitFor("forget", func() bool { return r.s.Forget(n) == nil })
	r.keydDown()
	os.WriteFile(r.statePath, backup, 0o600)
	r.open()
	r.keydUp()
	r.waitFor("new key active", func() bool { p := r.rec(n); return p.ObjKeyName != n+"-1" && p.ObjKeyState == ObjKeyActive })
	if c := r.kd.createCount(n + "-1"); c != 1 {
		t.Fatalf("NAME REUSE: %s-1 created %d times", n, c)
	}
	r.checkInvariant("restored after forget")
}

// The same after the Mac went through three generations before it was
// forgotten: manager-gens.json keeps the high-water mark the restored store
// lost, so the new key is gen 4, not a second gen 2.
func TestObjKeysRestoreUsesGensFile(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	n, _ := r.enroll(61)
	if err := r.s.Approve(n); err != nil {
		t.Fatal(err)
	}
	backup, _ := os.ReadFile(r.statePath)
	for i := 0; i < 2; i++ {
		if err := r.s.Revoke(n); err != nil {
			t.Fatal(err)
		}
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.s.Revoke(n); err != nil {
		t.Fatal(err)
	}
	r.waitFor("forget", func() bool { return r.s.Forget(n) == nil })
	r.keydDown()
	os.WriteFile(r.statePath, backup, 0o600)
	r.open()
	r.keydUp()
	r.waitFor("new key active", func() bool { p := r.rec(n); return p.ObjKeyName != n+"-1" && p.ObjKeyState == ObjKeyActive })
	if got := r.rec(n).ObjKeyName; got != n+"-4" {
		t.Fatalf("restored store minted %s, want %s-4", got, n)
	}
	for g := 1; g <= 4; g++ {
		if c := r.kd.createCount(fmt.Sprintf("%s-%d", n, g)); c != 1 {
			t.Fatalf("%s-%d created %d times", n, g, c)
		}
	}
}

// A damaged high-water record cannot be silently overwritten by an older
// restored store; that would allow a previously issued key name to recur.
func TestObjKeysRestoreRefusesDamagedGenerationHistory(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	n, _ := r.enroll(91)
	if err := r.s.Approve(n); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := r.s.Revoke(n); err != nil {
			t.Fatal(err)
		}
		if err := r.s.Approve(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.s.Revoke(n); err != nil {
		t.Fatal(err)
	}
	r.waitFor("forget", func() bool { return r.s.Forget(n) == nil })
	r.keydDown()
	for g := 1; g <= 3; g++ {
		if c := r.kd.createCount(fmt.Sprintf("%s-%d", n, g)); c != 1 {
			t.Fatalf("VALIDITY: generation %d created %d times", g, c)
		}
	}
	if err := os.WriteFile(r.statePath, backup, 0o600); err != nil {
		t.Fatal(err)
	}
	gensPath := filepath.Join(r.objDir, objKeyGensName)
	for label, body := range map[string]string{
		"broken_json":    `{corrupt history`,
		"missing_gens":   `{}`,
		"missing_name":   `{"gens":{}}`,
		"null_gens":      `{"gens":null}`,
		"invalid_range":  fmt.Sprintf(`{"gens":{"%s":1000}}`, n),
		"duplicate_name": fmt.Sprintf(`{"gens":{"%s":3,"%s":1}}`, n, n),
	} {
		t.Run(label, func(t *testing.T) {
			if err := os.WriteFile(gensPath, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
				PerMacKeys: true, ObjKeysDir: r.objDir, ObjectKey: clientObjectKey})
			if err == nil || !strings.Contains(err.Error(), objKeyGensName) {
				t.Fatalf("damaged history accepted: %v", err)
			}
			got, e := os.ReadFile(gensPath)
			if e != nil || string(got) != body {
				t.Fatalf("damaged evidence overwritten: %q, %v", got, e)
			}
		})
	}
	t.Run("missing_file", func(t *testing.T) {
		if err := os.Remove(gensPath); err != nil {
			t.Fatal(err)
		}
		_, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
			PerMacKeys: true, ObjKeysDir: r.objDir, ObjectKey: clientObjectKey})
		if err == nil || !strings.Contains(err.Error(), objKeyGensName) {
			t.Fatalf("lost history accepted: %v", err)
		}
	})
	t.Run("missing_both_sidecars", func(t *testing.T) {
		if err := os.Remove(filepath.Join(r.objDir, objKeyDesiredName)); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(r.objDir, objKeyAppliedName)); err != nil {
			t.Fatalf("VALIDITY: applied history absent: %v", err)
		}
		_, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
			PerMacKeys: true, ObjKeysDir: r.objDir, ObjectKey: clientObjectKey})
		if err == nil || !strings.Contains(err.Error(), objKeyGensName) {
			t.Fatalf("lost sidecars reused a name: %v", err)
		}
	})
}

func TestObjKeysFreshKeydFirstEmptyHistoryBootstraps(t *testing.T) {
	r := newObjRig(t)
	for _, p := range []string{r.statePath, filepath.Join(r.objDir, objKeyGensName), filepath.Join(r.objDir, objKeyDesiredName)} {
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
	r.kd.writeApplied(nil, "") // keyd started before Manager, no keys issued
	if err := os.MkdirAll(filepath.Join(r.objDir, objKeyResultsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
		PerMacKeys: true, ObjKeysDir: r.objDir, ObjectKey: clientObjectKey}); err != nil {
		t.Fatalf("fresh keyd-first startup refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(r.objDir, objKeyGensName)); err != nil {
		t.Fatalf("generation floor not initialized: %v", err)
	}
}

func TestObjKeysWriteAheadFloorSurvivesStoreFailureAndRetry(t *testing.T) {
	r := newObjRig(t)
	n, _ := r.enroll(95)
	atomicWriteHook = func(f *os.File, data []byte) error {
		if f.Name() == r.statePath+".tmp" || strings.Contains(f.Name(), "principals-store.json") {
			return errors.New("injected store failure")
		}
		_, err := f.Write(data)
		return err
	}
	firstErr := r.s.ensureObjKey(n)
	atomicWriteHook = nil
	if firstErr == nil || !strings.Contains(firstErr.Error(), "injected store failure") {
		t.Fatalf("store failure was not reached: %v", firstErr)
	}
	var floor struct {
		Gens map[string]int `json:"gens"`
	}
	if b, err := os.ReadFile(filepath.Join(r.objDir, objKeyGensName)); err != nil {
		t.Fatal(err)
	} else if err := json.Unmarshal(b, &floor); err != nil {
		t.Fatal(err)
	}
	if floor.Gens[n] != 1 {
		t.Fatalf("floor before failed store = %d, want 1", floor.Gens[n])
	}
	if err := r.s.ensureObjKey(n); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("keyd-down retry = %v", err)
	}
	if _, ok := r.desired().has(n + "-2"); !ok {
		t.Fatal("retry reused generation 1 after failed store")
	}
}

func TestObjKeysObservedOrphanFloorPersistsBeforeStore(t *testing.T) {
	r := newObjRig(t)
	n, _ := r.enroll(96)
	r.kd.mu.Lock()
	r.kd.keys[n+"-7"] = fakeMinioKey{parent: objKeysParent, status: "on"}
	r.kd.mu.Unlock()
	r.kd.writeApplied(nil, "")
	atomicWriteHook = func(f *os.File, data []byte) error {
		if strings.Contains(f.Name(), "principals-store.json") {
			return errors.New("injected store failure")
		}
		_, err := f.Write(data)
		return err
	}
	err := r.s.ReconcileObjKeys()
	atomicWriteHook = nil
	if err == nil {
		t.Fatal("store failure was not observed")
	}
	var floor struct {
		Gens map[string]int `json:"gens"`
	}
	if b, err := os.ReadFile(filepath.Join(r.objDir, objKeyGensName)); err != nil {
		t.Fatal(err)
	} else if err := json.Unmarshal(b, &floor); err != nil {
		t.Fatal(err)
	}
	if floor.Gens[n] != 7 || r.s.objk.gens[n] != 7 {
		t.Fatalf("observed generation floor rolled back: disk=%d memory=%d", floor.Gens[n], r.s.objk.gens[n])
	}
}

// An active key that vanishes from MinIO (removed by hand, or MinIO lost
// it) is replaced by a new generation. If keyd reports it gone first, the
// Manager retires it; if keyd re-creates it first (desired.json still
// named it), the Manager sees a second secret for a delivered name and
// retires it. Either way the re-used name is never delivered, and keyd
// removes it.
func TestObjKeysVanishedActiveKeyReplaced(t *testing.T) {
	for _, keydFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("keydRecreatesFirst=%v", keydFirst), func(t *testing.T) {
			r := newObjRig(t)
			r.keydUp()
			n, tok := r.enroll(62)
			if err := r.s.Approve(n); err != nil {
				t.Fatal(err)
			}
			first := r.rec(n).ObjKeySecret
			r.keydDown()
			r.kd.mu.Lock()
			delete(r.kd.keys, n+"-1")
			r.kd.mu.Unlock()
			if keydFirst {
				r.kd.step() // re-creates n-1 with a new secret
			} else {
				r.kd.writeApplied(nil, "") // reports it gone
			}
			if err := r.s.ReconcileObjKeys(); err != nil {
				t.Fatal(err)
			}
			if p := r.rec(n); p.ObjKeyName == n+"-1" {
				t.Fatalf("vanished %s-1 kept (secret changed=%v)", n, p.ObjKeySecret != first)
			}
			if got := r.poll(62, tok); got.ObjectAccessKey == n+"-1" {
				t.Fatalf("re-used name delivered: %+v", got)
			}
			r.keydUp()
			r.waitFor("replacement active", func() bool { p := r.rec(n); return p.ObjKeyName == n+"-2" && p.ObjKeyState == ObjKeyActive })
			r.waitFor("old name removed", func() bool { _, ok := r.kd.minio(n + "-1"); return !ok })
			r.checkInvariant("vanished key replaced")
		})
	}
}

// With the flag off (an M2 rollback), per-Mac keys the store still records
// are shown, with the reason they are not removed.
func TestObjKeysFlagOffShowsKeysLeft(t *testing.T) {
	r := newObjRig(t)
	r.keydUp()
	n, _ := r.enroll(70)
	if err := r.s.Approve(n); err != nil {
		t.Fatal(err)
	}
	r.keydDown()
	off, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir, ObjectKey: clientObjectKey})
	if err != nil {
		t.Fatal(err)
	}
	if err := off.Revoke(n); err != nil {
		t.Fatal(err)
	}
	snap := off.Snapshot()
	if snap.PerMacKeysLeft != 1 || snap.PerMacKeys {
		t.Fatalf("flag off: per_mac_keys_left = %d, per_mac_keys = %v", snap.PerMacKeysLeft, snap.PerMacKeys)
	}
	if st := snap.byName(n).ObjKeyStatus; st != "removing" {
		t.Fatalf("revoked Mac's key status with the flag off = %q, want removing (red)", st)
	}
	raw, _ := staticFS.ReadFile("static/principals.html")
	if !strings.Contains(string(raw), "s.per_mac_keys_left") || !strings.Contains(string(raw), "set JM_OBJ_PER_MAC=1") {
		t.Fatal("principals.html does not explain per-Mac keys left with the flag off")
	}
}

// ---- 8: a removal counts as confirmed only at its seq -----------------------

// keyd's applied.json from BEFORE the seq that omitted the key (keyd may
// still be creating it from an older desired.json) does not confirm the
// removal, even though it does not list the key.
func TestObjKeysRemovalNeedsKeydSeq(t *testing.T) {
	r := newObjRig(t)
	name, _ := r.enroll(98)
	if err := r.s.Approve(name); !errors.Is(err, ErrObjectKeyNotReady) {
		t.Fatalf("approve with keyd silent: %v", err)
	}
	mintSeq := r.rec(name).ObjKeyMintSeq
	if err := r.s.Revoke(name); err != nil {
		t.Fatal(err)
	}
	rm := r.rec(name).ObjKeyRemoving
	if len(rm) != 1 || rm[0].Seq <= mintSeq {
		t.Fatalf("setup: removal %+v", rm)
	}
	stale, _ := json.Marshal(map[string]any{"seq": rm[0].Seq - 1, "verified_ms": time.Now().UnixMilli(), "error": "", "keys": []any{}})
	if err := os.WriteFile(filepath.Join(r.objDir, "applied.json"), stale, 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := r.s.ReconcileObjKeys(); err != nil {
			t.Fatal(err)
		}
	}
	if len(r.rec(name).ObjKeyRemoving) != 1 {
		t.Fatal("removal confirmed by an applied.json older than its seq")
	}
	if err := r.s.Forget(name); !errors.Is(err, ErrObjectKeyRemovalPending) {
		t.Fatalf("forget = %v, want removal pending", err)
	}
	cur, _ := json.Marshal(map[string]any{"seq": r.desired().Seq, "verified_ms": time.Now().UnixMilli(), "error": "", "keys": []any{}})
	if err := os.WriteFile(filepath.Join(r.objDir, "applied.json"), cur, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := r.s.ReconcileObjKeys(); err != nil {
		t.Fatal(err)
	}
	if err := r.s.Forget(name); err != nil {
		t.Fatalf("forget after keyd confirmed: %v", err)
	}
}

// The Principals page renders the orphan list, its confirm action, the
// per-row orphan badge and the in-use report.
func TestObjKeysPrincipalsPageOrphans(t *testing.T) {
	raw, err := staticFS.ReadFile("static/principals.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(raw)
	for _, want := range []string{"s.orphan_keys", "principals/orphan-keys/remove", "Remove orphan keys", "p.obj_key_orphans",
		"Unknown storage key still live", "p.obj_key_in_use"} {
		if !strings.Contains(page, want) {
			t.Fatalf("principals.html does not render %q", want)
		}
	}
}
