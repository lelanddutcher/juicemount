package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
)

const (
	enrollTestAK = "jm-client"
	enrollTestSK = "objsecret-0123456789abcdef0123456789"
	enrollToken  = "abababababababababababababababababababababababababababababababab"
)

// stubManager: pending until approve() is called, then approved with the
// object key. It records every request body.
type stubManager struct {
	mu       sync.Mutex
	approved bool
	bodies   []map[string]string
}

func (s *stubManager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m map[string]string
	_ = json.NewDecoder(r.Body).Decode(&m)
	s.bodies = append(s.bodies, m)
	name := "mac-" + strings.ToLower(strings.ReplaceAll(m["instance_id"], "-", ""))[:12]
	resp := map[string]string{"principal": name, "fingerprint": "ABCD-1234", "status": "pending"}
	if m["secret"] != "" {
		resp["poll_token"] = enrollToken
	}
	code := http.StatusAccepted
	if s.approved {
		code = http.StatusOK
		resp["status"] = "approved"
		resp["object_access_key"], resp["object_secret_key"] = enrollTestAK, enrollTestSK
	}
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(resp)
}

func runEnrollStep(t *testing.T, mgr, db string, st enrollclient.State) enrollStepOutput {
	t.Helper()
	in, _ := json.Marshal(enrollStepInput{ManagerURL: mgr, DBPath: db, Host: "Edit-Bay-2", Label: "Edit Bay 2", State: st})
	var out enrollStepOutput
	if err := json.Unmarshal([]byte(enrollStep(string(in))), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// captureLogs sends jmlog and the standard logger to a buffer.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	if err := jmlog.Init(jmlog.Config{Stderr: &buf, Level: jmlog.LevelDebug}); err != nil {
		t.Fatal(err)
	}
	log.SetOutput(&buf)
	t.Cleanup(func() {
		_ = jmlog.Init(jmlog.Config{})
		log.SetOutput(os.Stderr)
	})
	return &buf
}

// TestH8BEnrollStepCloneReEnrollClearsNotice: the bridge reads this Mac's
// identity beside metadata.db. After a clone rotation it enrolls the new
// instance ID with the previous ID and the device digest, and clears the
// clone notice only once the Manager approves. No secret reaches a log.
func TestH8BEnrollStepCloneReEnrollClearsNotice(t *testing.T) {
	buf := captureLogs(t)
	db := filepath.Join(t.TempDir(), "metadata.db")
	// Bind an instance ID on "Mac A", then load it on "Mac B": a clone.
	t.Cleanup(deviceid.SetSourceForTest(func() (string, error) { return "AAAAAAAA-0000-4000-8000-00000000000A", nil }))
	original := cplane.LoadInstanceIdentity(db).ID
	deviceid.SetSourceForTest(func() (string, error) { return "BBBBBBBB-0000-4000-8000-00000000000B", nil })

	stub := &stubManager{}
	srv := httptest.NewServer(stub)
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })

	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	if out.InstanceID == original || out.CloneNotice == "" {
		t.Fatalf("clone not detected: id %s (original %s), notice %q", out.InstanceID, original, out.CloneNotice)
	}
	if !out.Persist || out.State.Secret == "" || len(stub.bodies) != 0 {
		t.Fatal("first step must only make a secret")
	}
	secret := out.State.Secret
	out = runEnrollStep(t, srv.URL, db, out.State)
	if out.Phase != enrollclient.Waiting {
		t.Fatalf("enroll phase %s: %s", out.Phase, out.Message)
	}
	first := stub.bodies[0]
	if first["previous_instance_id"] != original || first["instance_id"] != out.InstanceID || first["device_digest"] != deviceid.Digest("BBBBBBBB-0000-4000-8000-00000000000B") {
		t.Fatalf("clone enroll body: previous=%q instance=%q digest=%q", first["previous_instance_id"], first["instance_id"], first["device_digest"])
	}
	if _, ok := cplane.ReadCloneNotice(db); !ok {
		t.Fatal("clone notice cleared while still pending")
	}

	stub.approved = true
	out = runEnrollStep(t, srv.URL, db, out.State)
	if out.Phase != enrollclient.Approved || out.ObjectSecretKey != enrollTestSK || !out.CloneCleared || out.CloneNotice != "" {
		t.Fatalf("approval: phase %s cleared=%v", out.Phase, out.CloneCleared)
	}
	if _, ok := cplane.ReadCloneNotice(db); ok {
		t.Fatal("clone notice still on disk after approval")
	}
	if !strings.Contains(buf.String(), "enrollment step") {
		t.Fatal("no enrollment log line captured; the leak check would prove nothing")
	}
	for _, s := range []string{secret, enrollToken, enrollTestSK} {
		if strings.Contains(buf.String(), s) {
			t.Fatalf("a secret reached the log: %q", s)
		}
	}
}

// A refused input echoes the caller's state: the app must never lose a
// secret the Manager already holds because one call failed.
func TestH8BEnrollStepFailureKeepsState(t *testing.T) {
	st := enrollclient.State{ManagerURL: "http://nas:30190", InstanceID: "X", Secret: "keep-me-0123456789abcdef0123456789"}
	in, _ := json.Marshal(enrollStepInput{ManagerURL: "http://nas:30190", State: st})
	var out enrollStepOutput
	if err := json.Unmarshal([]byte(enrollStep(string(in))), &out); err != nil {
		t.Fatal(err)
	}
	if out.Phase != enrollclient.Failed || out.State.Secret != st.Secret || out.Persist {
		t.Fatalf("failure output %+v dropped the state", out.Phase)
	}
}

// The export's process-level output: the secret appears in the JSON handed
// to the app (it must store it) but in nothing written to stdout/stderr.
func TestH8BEnrollStepWritesNoSecretToStdio(t *testing.T) {
	db := filepath.Join(t.TempDir(), "metadata.db")
	srv := httptest.NewServer(&stubManager{approved: true})
	defer srv.Close()
	old := enrollHTTPClient
	enrollHTTPClient = srv.Client()
	t.Cleanup(func() { enrollHTTPClient = old })
	r, w, _ := os.Pipe()
	stderr, stdout := os.Stderr, os.Stdout
	os.Stderr, os.Stdout = w, w
	_ = jmlog.Init(jmlog.Config{Stderr: w})
	log.SetOutput(w)
	out := runEnrollStep(t, srv.URL, db, enrollclient.State{})
	out = runEnrollStep(t, srv.URL, db, out.State)
	os.Stderr, os.Stdout = stderr, stdout
	_ = jmlog.Init(jmlog.Config{})
	log.SetOutput(os.Stderr)
	w.Close()
	printed, _ := io.ReadAll(r)
	if out.Phase != enrollclient.Approved || len(printed) == 0 {
		t.Fatalf("phase %s, %d bytes printed", out.Phase, len(printed))
	}
	for _, s := range []string{out.State.Secret, enrollToken, enrollTestSK} {
		if strings.Contains(string(printed), s) {
			t.Fatalf("secret printed: %q", s)
		}
	}
}
