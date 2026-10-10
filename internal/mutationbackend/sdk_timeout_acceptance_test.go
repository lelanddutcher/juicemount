//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func timeoutPayload() []byte { return bytes.Repeat([]byte("exact timeout payload\x00"), 701) }

type timeoutPhase struct {
	Phase  string
	Length uint64
}

// The retained pre-fix counterexample is reversed by a condition evaluated at
// provider commit. This is object identity acceptance, not a media receipt.
func TestSDKTimeoutCannotOverwriteCompletedObject(t *testing.T) { runSDKDelayedCreate(t, false) }

func TestSDKDeadProcessCannotOverwriteSuccessorObject(t *testing.T) { runSDKDelayedCreate(t, true) }

func runSDKDelayedCreate(t *testing.T, processDeath bool) {
	f := backendfixture.New(t, StaticACL)
	var mu sync.Mutex
	var key string
	var first, stored []byte
	puts, gets, deletes := 0, 0, 0
	late := false
	firstBuffered, retryStored, firstRejected := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	probe := newCreateOnlyProbeFixture(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if probe(w, req) {
			return
		}
		switch req.Method {
		case http.MethodPut:
			if req.Header.Get("If-None-Match") != "*" {
				t.Error("payload PUT missing create-only condition")
				w.WriteHeader(400)
				return
			}
			body, err := io.ReadAll(io.LimitReader(req.Body, 128<<10))
			if err != nil || len(body) >= 128<<10 {
				t.Error("invalid bounded provider PUT")
				w.WriteHeader(500)
				return
			}
			mu.Lock()
			puts++
			attempt := puts
			if attempt == 1 {
				key, first = req.URL.Path, body
				close(firstBuffered)
			} else if req.URL.Path != key || attempt != 2 {
				mu.Unlock()
				t.Error("unexpected SDK object key or extra retry")
				w.WriteHeader(500)
				return
			}
			mu.Unlock()
			if attempt == 1 {
				select {
				case <-release:
				case <-f.Context.Done():
					w.WriteHeader(500)
					return
				}
			}
			mu.Lock()
			if len(stored) != 0 {
				late = true
				close(firstRejected)
				mu.Unlock()
				w.WriteHeader(412)
				return
			}
			stored = body
			close(retryStored)
			mu.Unlock()
			w.WriteHeader(200)
		case http.MethodGet:
			mu.Lock()
			gets++
			body, rightKey := append([]byte(nil), stored...), req.URL.Path == key
			mu.Unlock()
			if len(body) == 0 || !rightKey {
				w.WriteHeader(404)
				return
			}
			sum := sha256.Sum256(body)
			w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
			_, _ = w.Write(body)
		case http.MethodDelete:
			mu.Lock()
			deletes++
			mu.Unlock()
			w.WriteHeader(204)
		default:
			w.WriteHeader(405)
		}
	}))
	defer server.Close()
	defer releaseFirst()
	wait := func(ch <-chan struct{}, label string) {
		t.Helper()
		select {
		case <-ch:
		case <-f.Context.Done():
			t.Fatal("provider barrier expired:", label)
		}
	}
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	format := meta.Format{Name: "fixture", UUID: "volume", Storage: "minio", Bucket: server.URL + "/bucket", AccessKey: "fixture-key", SecretKey: "fixture-secret", BlockSize: 64, Compression: "none", HashPrefix: true, DirStats: true,
		EncryptKey: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privateKey)})), EncryptAlgo: "aes256gcm-rsa"}
	setting := f.SeedExisting(t, &format)
	pathStyle := true
	cfg := SDKConfig{Redis: RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting), Region: "us-east-1", PathStyle: &pathStyle}
	configPath := filepath.Join(f.Dir, "sdk.json")
	config, _ := json.Marshal(cfg)
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.Context, 40*time.Second)
	defer cancel()
	var stderr bytes.Buffer
	startChild := func() (*exec.Cmd, io.WriteCloser, io.ReadCloser) {
		t.Helper()
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKTimeoutChild$")
		cmd.Env = append(os.Environ(), "JM_TIMEOUT_CONFIG="+configPath, "JM_TIMEOUT_STATE="+f.Dir)
		if processDeath {
			cmd.Env = append(cmd.Env, "JM_TIMEOUT_RAW=1")
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, stdin, stdout
	}
	cmd, stdin, stdout := startChild()
	waited := false
	defer func() {
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	wait(firstBuffered, "first fully buffered PUT")
	if processDeath {
		oldPID := cmd.Process.Pid
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := cmd.Wait(); err == nil {
			t.Fatal("old process was not killed")
		}
		waited = true
		_ = stdin.Close()
		oldJournal, err := mutationauthority.Open(filepath.Join(f.Dir, "authority.db"))
		if err != nil {
			t.Fatal("dead process retained OS journal lock", err)
		}
		if err := oldJournal.Close(); err != nil {
			t.Fatal(err)
		}
		cmd, stdin, stdout = startChild()
		waited = false
		t.Logf("old SDK child pid=%d killed/reaped before successor pid=%d admission", oldPID, cmd.Process.Pid)
	}
	wait(retryStored, "retry of same object key")
	decoder := json.NewDecoder(stdout)
	var phase timeoutPhase
	expectedPhase := "fsynced"
	if processDeath {
		expectedPhase = "created"
	}
	if err := decoder.Decode(&phase); err != nil || phase.Phase != expectedPhase || phase.Length != uint64(len(timeoutPayload())) {
		t.Fatal("real Fsync/Redis length did not finish while first PUT was held", err, phase)
	}
	mu.Lock()
	before := append([]byte(nil), stored...)
	wasLate := late
	mu.Unlock()
	if wasLate || bytes.Equal(before, first) {
		t.Fatal("retry did not establish distinct ciphertext before first PUT release")
	}
	releaseFirst()
	wait(firstRejected, "old PUT rejected at provider commit")
	mu.Lock()
	after := append([]byte(nil), stored...)
	mu.Unlock()
	if bytes.Equal(after, first) || !bytes.Equal(before, after) {
		t.Fatal("late first PUT changed the acknowledged physical object")
	}
	encryption, err := object.NewDataEncryptor(object.NewRSAEncryptor(privateKey), format.EncryptAlgo)
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range [][]byte{before, after} {
		plain, err := encryption.Decrypt(body)
		if err != nil || !bytes.Equal(plain, timeoutPayload()) {
			t.Fatal("retained ciphertext failed exact plaintext verification")
		}
	}
	if err := json.NewEncoder(stdin).Encode(timeoutPhase{Phase: "verify"}); err != nil {
		t.Fatal(err)
	}
	if err := decoder.Decode(&phase); err != nil || phase.Phase != "retained" {
		t.Fatal("SDK late readback/close/pin verification failed", err, phase)
	}
	other, err := mutationauthority.Open(filepath.Join(f.Dir, "authority.db"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, mutationauthority.ErrLocked) {
		t.Fatal("live child released its authority", err)
	}
	_ = stdin.Close()
	err = cmd.Wait()
	waited = true
	server.Close()
	mu.Lock()
	defer mu.Unlock()
	if puts != 2 || gets < 2 || deletes != 0 {
		t.Fatal("unexpected provider lifecycle", puts, gets, deletes)
	}
	t.Logf("create-only: processDeath=%t completion=%s before old PUT release; PUT=%d GET=%d DELETE=%d before=%s after=%s identity unchanged", processDeath, expectedPhase, puts, gets, deletes, digest(before), digest(after))
	if stderr.Len() != 0 {
		if dir := os.Getenv("JM_SDK_DIAGNOSTIC_DIR"); dir != "" {
			info, statErr := os.Stat(dir)
			if statErr != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
				t.Fatal("invalid private SDK diagnostic directory")
			}
			file, fileErr := os.OpenFile(filepath.Join(dir, fmt.Sprintf("sdk-timeout-%d.stderr", cmd.Process.Pid)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if fileErr != nil {
				t.Fatal("cannot retain SDK timeout diagnostics")
			}
			_, writeErr := file.Write(stderr.Bytes())
			closeErr := file.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatal("cannot retain complete SDK timeout diagnostics")
			}
		}
		t.Fatalf("SDK timeout emitted diagnostics: bytes=%d sha256=%s", stderr.Len(), digest(stderr.Bytes()))
	}
	if err != nil || ctx.Err() != nil {
		t.Fatal("SDK child did not exit successfully", err, ctx.Err())
	}
	next, err := mutationauthority.Open(filepath.Join(f.Dir, "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("owned SDK timeout child pid=%d reaped", cmd.Process.Pid)
}

func TestSDKTimeoutChild(t *testing.T) {
	path := os.Getenv("JM_TIMEOUT_CONFIG")
	if path == "" {
		return
	}
	cfg, err := ReadSDKConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(os.Getenv("JM_TIMEOUT_STATE"), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenSDK(context.Background(), j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("JM_TIMEOUT_RAW") == "1" {
		// Same physical key across independently fenced SDK processes. This is
		// deliberately below namespace allocation, so it tests provider custody.
		key := "chunks/process-death-fixture"
		payload := timeoutPayload()
		if err := r.objects.Put(key, bytes.NewReader(payload)); err != nil {
			t.Fatal(err)
		}
		encoder := json.NewEncoder(os.Stdout)
		if err := encoder.Encode(timeoutPhase{Phase: "created", Length: uint64(len(payload))}); err != nil {
			t.Fatal(err)
		}
		var phase timeoutPhase
		if err := json.NewDecoder(os.Stdin).Decode(&phase); err != nil || phase.Phase != "verify" {
			t.Fatal("missing verification barrier", err)
		}
		reader, err := r.objects.Get(key, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil || closeErr != nil || !bytes.Equal(got, payload) {
			t.Fatal("successor object changed")
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if err := j.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
			t.Fatal("successor released process pin", err)
		}
		if err := encoder.Encode(timeoutPhase{Phase: "retained"}); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	file, eno := r.filesystem.Create(fctx, "/timeout-payload", 0600, 0077)
	if eno != 0 {
		t.Fatal(eno)
	}
	payload := timeoutPayload()
	if n, eno := file.Pwrite(fctx, payload, 0); eno != 0 || n != len(payload) {
		t.Fatal("SDK timeout write failed", n, eno)
	}
	if eno := file.Fsync(fctx); eno != 0 {
		t.Fatal("SDK retry did not satisfy Fsync", eno)
	}
	var attr meta.Attr
	if eno := r.metadata.GetAttr(fctx, file.Inode(), &attr); eno != 0 || attr.Length != uint64(len(payload)) {
		t.Fatal("SDK Fsync did not publish exact metadata length", eno, attr.Length)
	}
	encoder := json.NewEncoder(os.Stdout)
	if err := encoder.Encode(timeoutPhase{Phase: "fsynced", Length: attr.Length}); err != nil {
		t.Fatal(err)
	}
	var phase timeoutPhase
	if err := json.NewDecoder(os.Stdin).Decode(&phase); err != nil || phase.Phase != "verify" {
		t.Fatal("missing late-provider verification barrier", err)
	}
	got := make([]byte, len(payload))
	if n, err := file.Pread(fctx, got, 0); err != nil || n != len(got) || !bytes.Equal(got, payload) {
		t.Fatal("SDK plaintext readback failed after late PUT")
	}
	if eno := file.Close(fctx); eno != 0 {
		t.Fatal(eno)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
		t.Fatal("SDK timeout released process authority", err)
	}
	if err := encoder.Encode(timeoutPhase{Phase: "retained"}); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
