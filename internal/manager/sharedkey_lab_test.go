package manager

// The shared-key revoke against a real object store (server/minio/shared-key-lab.sh
// starts it and sets the variables; without them the test is skipped).
//
// It acts as a revoked Mac would: the key the Manager handed it, kept by the
// Mac, makes a signed ListBucket on the volume's bucket. It checks
//  1. a revoke without the confirmation changes nothing;
//  2. after a confirmed revoke the Mac's stored key still lists the bucket
//     (what the confirmation warns about, and the proof that the check
//     detects a working key);
//  3. after the documented rotation (remove the user, minio-keys again with
//     a new secret) the stored key is refused;
//  4. the remaining Mac gets the new key at its next poll, and it works;
//  5. the Manager's verifier clears the pending rotation from the object
//     store's own answer.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestSharedKeyRevokeRotationLab(t *testing.T) {
	bucketURL := os.Getenv("JM_LAB_BUCKET_URL")
	rotate := os.Getenv("JM_LAB_ROTATE")
	oldSK := os.Getenv("JM_LAB_CLIENT_SECRET")
	if bucketURL == "" || rotate == "" || oldSK == "" {
		t.Skip("lab only: run test/shared-key-lab.sh")
	}
	const ak = "jm-client"
	probe := NewObjectStoreProbe(bucketURL, "")
	list := func(sk string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		code, err := probe(ctx, ak, sk)
		if err != nil {
			t.Fatalf("ListBucket: no usable answer: %v", err)
		}
		if code == "" {
			return "OK"
		}
		return code
	}

	dir := t.TempDir()
	r := &sharedKeyRig{t: t, statePath: filepath.Join(dir, "store", "principals-store.json"),
		proxyDir: filepath.Join(dir, "respproxy"), ak: ak, sk: oldSK}
	if err := os.MkdirAll(r.proxyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	startFakeProxyAck(t, r.proxyDir)
	openLab := func() {
		s, err := NewPrincipalStore(PrincipalStoreOptions{StatePath: r.statePath, ProxyDir: r.proxyDir,
			ObjectKey: func() (string, string) { return r.ak, r.sk }, ObjectStoreProbe: probe})
		if err != nil {
			t.Fatal(err)
		}
		r.s = s
	}
	openLab()

	a, aTok := r.approved(1)
	b, bTok := r.approved(2)
	aKey := r.poll(1, aTok) // what the Mac stores
	if aKey.ObjectSecretKey != oldSK {
		t.Fatalf("approved Mac %s got no shared key", a)
	}
	row := func(step, want, got string) {
		t.Logf("LAB | %-58s | want %-22s | got %s", step, want, got)
		if got != want {
			t.Errorf("%s: got %s, want %s", step, got, want)
		}
	}
	row("revoked Mac's stored key, before any revoke", "OK", list(aKey.ObjectSecretKey))

	_, err := r.s.RevokeConfirmed(a, false)
	got := "revoked"
	if errors.Is(err, ErrSharedKeyConfirmRequired) && r.s.Snapshot().byName(a).Status == PrincipalApproved {
		got = "refused, unchanged"
	}
	row("revoke without confirm_shared_key", "refused, unchanged", got)

	if _, err := r.s.RevokeConfirmed(a, true); err != nil {
		t.Fatal(err)
	}
	row("stored key after confirmed revoke, before rotation", "OK", list(aKey.ObjectSecretKey))
	row("pending rotation listed", "1", strconv.Itoa(len(r.s.Snapshot().SharedKeyStillValid)))

	newSK := newProbeSecret() // 40 hex characters, as minio-keys requires
	secretFile := filepath.Join(dir, "new-secret")
	if err := os.WriteFile(secretFile, []byte(newSK), 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	out, err := exec.Command(rotate, secretFile).CombinedOutput()
	if err != nil {
		t.Fatalf("rotation failed: %v\n%s", err, out)
	}
	t.Logf("LAB | rotation (user removed, minio-keys with the new secret) took %s", time.Since(start).Round(time.Millisecond))
	r.sk = newSK // the Manager recreated with the new server/.env
	openLab()

	refused := list(aKey.ObjectSecretKey)
	if refused == "SignatureDoesNotMatch" || refused == "InvalidAccessKeyId" {
		refused = "refused (" + refused + ")"
	}
	row("stored key after the rotation", "refused (SignatureDoesNotMatch)", refused)

	bKey := r.poll(2, bTok)
	row("remaining Mac's next poll carries the new key", "true", boolStr(bKey.ObjectSecretKey == newSK))
	row("remaining Mac's new key", "OK", list(bKey.ObjectSecretKey))
	row("revoked Mac's poll carries a key", "false", boolStr(r.poll(1, aTok).ObjectSecretKey != ""))
	r.s.VerifySharedKeyRotation(context.Background())
	snap := r.s.Snapshot()
	row("pending rotation listed after the verifier", "0", strconv.Itoa(len(snap.SharedKeyStillValid)))
	if snap.SharedKeyProblem != "" {
		t.Logf("LAB | problem: %s", snap.SharedKeyProblem)
	}
	_ = b
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
