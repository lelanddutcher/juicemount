package manager

// H-8B §2.1 object keys: the volume `setting` key holds no object key, so
// every juicefs client brings its own as MINIO_ACCESS_KEY/MINIO_SECRET_KEY.
// The Manager runs gc/compact and gets the delete-capable jm-gc key; Macs
// get the no-delete jm-client key from enrollment, after approval only.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	testClientAK = "jm-client"
	testClientSK = "clientsecret0123456789abcdef0123456789ab"
	testGCAK     = "jm-gc"
	testGCSK     = "gcsecret0123456789abcdef0123456789abcdef"
)

func setObjectKeyEnv(t *testing.T) {
	t.Helper()
	t.Setenv("JM_OBJ_CLIENT_ACCESS_KEY", testClientAK)
	t.Setenv("JM_OBJ_CLIENT_SECRET_KEY", testClientSK)
	t.Setenv("JM_OBJ_GC_ACCESS_KEY", testGCAK)
	t.Setenv("JM_OBJ_GC_SECRET_KEY", testGCSK)
}

func decodeEnroll(t *testing.T, body []byte) EnrollResponse {
	t.Helper()
	var er EnrollResponse
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatalf("%v: %s", err, body)
	}
	return er
}

func assertNoObjectKey(t *testing.T, step string, body []byte) {
	t.Helper()
	s := string(body)
	if strings.Contains(s, "object_access_key") || strings.Contains(s, testClientSK) || strings.Contains(s, testGCSK) {
		t.Fatalf("%s: response carries an object key: %s", step, s)
	}
}

func TestH8BEnrollObjectKeyOnlyAfterApproval(t *testing.T) {
	setObjectKeyEnv(t)
	mux, _, _, _ := newH8BAPI(t)
	enroll := map[string]string{"instance_id": testInstanceID, "host": "Edit-Bay-2", "secret": testMacSecret}

	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", enroll)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("enroll: %d %s", rr.Code, rr.Body)
	}
	assertNoObjectKey(t, "pending enroll", rr.Body.Bytes())
	er := decodeEnroll(t, rr.Body.Bytes())
	token := er.PollToken
	poll := map[string]string{"instance_id": testInstanceID, "poll_token": token}

	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", poll)
	assertNoObjectKey(t, "pending poll", rr.Body.Bytes())

	rr = doJSON(t, mux, http.MethodPost, "/api/principals/approve", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusOK {
		t.Fatalf("approve: %d %s", rr.Code, rr.Body)
	}

	// A wrong poll token gets nothing, even for an approved Mac.
	bad := map[string]string{"instance_id": testInstanceID, "poll_token": strings.Repeat("0", 64)}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", bad)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("wrong poll token: %d %s, want 403", rr.Code, rr.Body)
	}
	assertNoObjectKey(t, "wrong-token poll", rr.Body.Bytes())

	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", poll)
	got := decodeEnroll(t, rr.Body.Bytes())
	if got.Status != PrincipalApproved || got.ObjectAccessKey != testClientAK || got.ObjectSecretKey != testClientSK {
		t.Fatalf("approved poll = %+v, want jm-client object key", got)
	}
	if strings.Contains(rr.Body.String(), testGCSK) {
		t.Fatal("the delete-capable jm-gc secret reached a Mac")
	}
	if cc := rr.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}

	rr = doJSON(t, mux, http.MethodPost, "/api/principals/revoke", h8bTestAdminKey, map[string]string{"name": er.Principal})
	if rr.Code != http.StatusOK {
		t.Fatalf("revoke: %d %s", rr.Code, rr.Body)
	}
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", poll)
	assertNoObjectKey(t, "revoked poll", rr.Body.Bytes())
}

// Auto-approved enrollment returns the key at once (same trust as the
// principal itself); with no client key configured, nothing is added.
func TestH8BEnrollObjectKeyAutoApproveAndUnset(t *testing.T) {
	setObjectKeyEnv(t)
	mux, _, _, _ := newH8BAPI(t)
	if rr := doJSON(t, mux, http.MethodPost, "/api/principals/auto-approve", h8bTestAdminKey, map[string]int{"hours": 1}); rr.Code != http.StatusOK {
		t.Fatalf("auto-approve: %d %s", rr.Code, rr.Body)
	}
	rr := doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Laptop", "secret": testMacSecret})
	if got := decodeEnroll(t, rr.Body.Bytes()); got.Status != PrincipalApproved || got.ObjectSecretKey != testClientSK {
		t.Fatalf("auto-approved enroll = %+v", got)
	}

	t.Setenv("JM_OBJ_CLIENT_SECRET_KEY", "")
	rr = doJSON(t, mux, http.MethodPost, "/api/principals/enroll", "", map[string]string{"instance_id": testInstanceID, "host": "Laptop", "secret": testMacSecret})
	assertNoObjectKey(t, "no client key configured", rr.Body.Bytes())
}

func envHas(env []string, kv string) bool { return slices.Contains(env, kv) }

func TestH8BManagerJuicefsSubprocessesGetGCKeyInEnvOnly(t *testing.T) {
	setObjectKeyEnv(t)
	t.Setenv("JM_META_BG_PASSWORD", "bgpassword-0123456789abcdef0123456789")
	t.Setenv("JM_META_ADMIN_PASSWORD", "adminpassword-0123456789abcdef01234567")
	meta := "redis://manager@respproxy:6379/1"

	mm := newMaintenanceManager("juicefs", "", meta, "/jfs")
	for _, kind := range []MaintenanceKind{MaintenanceGC, MaintenanceFSCK, MaintenanceCompactMeta} {
		argv, ok := mm.scheduledArgv(kind)
		if !ok {
			t.Fatalf("%s: no argv", kind)
		}
		if strings.Contains(strings.Join(argv, " "), testGCSK) {
			t.Fatalf("%s argv carries the gc secret: %v", kind, argv)
		}
	}
	if !envHas(mm.metaEnv, "MINIO_ACCESS_KEY="+testGCAK) || !envHas(mm.metaEnv, "MINIO_SECRET_KEY="+testGCSK) {
		t.Fatalf("gc/compact/fsck env lacks the jm-gc key: %v", redactEnv(mm.metaEnv))
	}
	if envHas(mm.metaEnv, "MINIO_ACCESS_KEY="+testClientAK) {
		t.Fatal("gc runs with the no-delete client key: deleted data would never be freed")
	}

	for _, role := range []metaRole{metaRoleSelf, metaRoleBG, metaRoleAdmin} {
		cmd := juicefsMetaCmd(context.Background(), "juicefs", "config", meta, role, "--trash-days", "7")
		if strings.Contains(strings.Join(cmd.Args, " "), testGCSK) {
			t.Fatalf("role %d argv carries the gc secret", role)
		}
		if !envHas(cmd.Env, "MINIO_ACCESS_KEY="+testGCAK) || !envHas(cmd.Env, "MINIO_SECRET_KEY="+testGCSK) {
			t.Fatalf("role %d juicefs env lacks the jm-gc key", role)
		}
	}
}

func redactEnv(env []string) []string {
	out := make([]string, len(env))
	for i, e := range env {
		k, _, _ := strings.Cut(e, "=")
		out[i] = k + "=…"
	}
	return out
}

// RunSync into the volume (standalone mode, jfs:// destination) hands the
// jm-gc key to juicefs sync through its environment, not argv.
func TestH8BSyncIntoVolumeGetsGCKeyInEnv(t *testing.T) {
	setObjectKeyEnv(t)
	dir := t.TempDir()
	envOut := filepath.Join(dir, "env.txt")
	argOut := filepath.Join(dir, "args.txt")
	fake := filepath.Join(dir, "juicefs")
	script := "#!/bin/sh\nenv > '" + envOut + "'\nprintf '%s\\n' \"$@\" > '" + argOut + "'\nexit 0\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	progress := make(chan ProgressEvent, 1024)
	spec := RunSyncSpec{Mode: ModeStandalone, MetaURL: "redis://manager@respproxy:6379/1", VolName: "zpool"}
	if err := RunSync(context.Background(), fake, spec, src, "/jfs/Project", DefaultSyncOptions(), nil, progress); err != nil {
		t.Fatalf("RunSync: %v", err)
	}
	env, _ := os.ReadFile(envOut)
	args, _ := os.ReadFile(argOut)
	lines := strings.Split(string(env), "\n")
	if !slices.Contains(lines, "MINIO_ACCESS_KEY="+testGCAK) || !slices.Contains(lines, "MINIO_SECRET_KEY="+testGCSK) {
		t.Fatal("juicefs sync into the volume did not get the jm-gc key in its environment")
	}
	if strings.Contains(string(args), testGCSK) || strings.Contains(string(args), testClientSK) {
		t.Fatalf("juicefs sync argv carries an object secret:\n%s", args)
	}
}
