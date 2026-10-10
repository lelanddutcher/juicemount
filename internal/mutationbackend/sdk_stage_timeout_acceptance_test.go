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
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

type delayedStageReport struct {
	Stage   mutationauthority.MediaStage
	Fsynced bool
	Length  uint64
}

// Exercise the stage API itself. The older timeout oracle proves create-only
// object identity without an operation manifest; it cannot prove these gates.
func TestSDKStageDelayedUpload(t *testing.T) {
	for _, killEarly := range []bool{false, true} {
		name := "timeout-retry"
		if killEarly {
			name = "commit-after-process-death"
		}
		t.Run(name, func(t *testing.T) { runSDKStageDelay(t, killEarly, false) })
	}
}

func TestSDKStageDelayedRecovery(t *testing.T) { runSDKStageDelay(t, true, true) }

func runSDKStageDelay(t *testing.T, killEarly, recoverStage bool) {
	f := backendfixture.New(t, StaticACL)
	var mu sync.Mutex
	var key string
	var bodies [][]byte
	var stored []byte
	gets, deletes, rejected := 0, 0, 0
	successorObjects := make(map[string][]byte)
	successorPuts, successorGets := 0, 0
	firstBuffered, firstFinished, retryStored := make(chan struct{}), make(chan struct{}), make(chan struct{})
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
				t.Error("stage payload PUT lacks conditional create")
				w.WriteHeader(400)
				return
			}
			body, err := io.ReadAll(io.LimitReader(req.Body, 128<<10))
			if err != nil || len(body) == 0 || len(body) >= 128<<10 {
				t.Error("invalid bounded stage payload")
				w.WriteHeader(500)
				return
			}
			mu.Lock()
			if recoverStage && key != "" && req.URL.Path != key {
				successorPuts++
				if _, exists := successorObjects[req.URL.Path]; exists {
					mu.Unlock()
					w.WriteHeader(412)
					return
				}
				successorObjects[req.URL.Path] = append([]byte(nil), body...)
				mu.Unlock()
				w.WriteHeader(200)
				return
			}
			bodies = append(bodies, body)
			attempt := len(bodies)
			if attempt == 1 {
				key = req.URL.Path
			}
			valid := req.URL.Path == key && attempt <= 2
			mu.Unlock()
			if !valid {
				t.Error("unexpected stage retry or key")
				w.WriteHeader(500)
				return
			}
			if attempt == 1 {
				defer close(firstFinished)
				close(firstBuffered)
				// A remote provider can finish an already buffered request even
				// after its client is dead. Deliberately ignore req.Context here.
				select {
				case <-release:
				case <-f.Context.Done():
					w.WriteHeader(500)
					return
				}
			}
			mu.Lock()
			if stored != nil {
				rejected++
				mu.Unlock()
				w.WriteHeader(412)
				return
			}
			stored = append([]byte(nil), body...)
			mu.Unlock()
			if attempt == 2 {
				close(retryStored)
			}
			w.WriteHeader(200)
		case http.MethodGet:
			mu.Lock()
			body, rightKey := append([]byte(nil), stored...), req.URL.Path == key
			if recoverStage && !rightKey {
				successorGets++
				body = append([]byte(nil), successorObjects[req.URL.Path]...)
				rightKey = len(body) != 0
			} else {
				gets++
			}
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
			t.Fatal("stage barrier expired", label)
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
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKStageDelayedChild$")
	cmd.Env = append(os.Environ(), "JM_STAGE_DELAY_CONFIG="+configPath, "JM_STAGE_DELAY_STATE="+f.Dir)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		stdin.Close()
		if !waited {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	kill := func() {
		t.Helper()
		if err := cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		err := cmd.Wait()
		waited = true
		if err == nil || ctx.Err() != nil || stderr.Len() != 0 {
			t.Fatal("stage child lifecycle/diagnostic failure", err, ctx.Err(), stderr.Len())
		}
	}
	wait(firstBuffered, "first request fully buffered")
	var reported delayedStageReport
	var before []byte
	journalPath := filepath.Join(f.Dir, "authority.db")
	if killEarly {
		kill()
	} else {
		wait(retryStored, "retry stored while first remains held")
		if err := json.NewDecoder(stdout).Decode(&reported); err != nil {
			t.Fatal("stage did not report refusal after Fsync", err)
		}
		if !reported.Fsynced || reported.Length != uint64(len(timeoutPayload())) {
			t.Fatal("missing actual Fsync/metadata boundary")
		}
		mu.Lock()
		before = append([]byte(nil), stored...)
		mu.Unlock()
		other, err := mutationauthority.Open(journalPath)
		if other != nil {
			other.Close()
		}
		if !errors.Is(err, mutationauthority.ErrLocked) {
			t.Fatal("live stage lost process pin", err)
		}
	}
	// In the early-death case, observe the journal and refuse a successor
	// before allowing the still-buffered request to create its physical object.
	var early mutationauthority.MediaStage
	var recovered stageRecoveryReport
	var stopSuccessor func()
	if killEarly {
		j, err := mutationauthority.Open(journalPath)
		if err != nil {
			t.Fatal(err)
		}
		early, err = j.InspectStage(context.Background(), "volume", "delayed-stage")
		if err != nil {
			j.Close()
			t.Fatal(err)
		}
		if _, err = j.Claim(context.Background(), "volume", "successor", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
			j.Close()
			t.Fatal("successor admitted before late commit", err)
		}
		if err := j.Close(); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		alreadyStored := len(stored)
		attempts := len(bodies)
		mu.Unlock()
		if alreadyStored != 0 || attempts != 1 {
			t.Fatal("early death missed its precommit boundary", alreadyStored, attempts)
		}
		if recoverStage {
			recovered, stopSuccessor = runStageSuccessor(t, ctx, configPath, f.Dir)
			if len(recovered.Prior) != 1 || !reflect.DeepEqual(recovered.Prior[0], early) || recovered.Lease.Epoch != early.Request.Epoch+1 {
				t.Fatal("successor lost exact retained intent")
			}
		}
	}
	releaseFirst()
	wait(firstFinished, "first request finishes after release")
	server.Close() // join all owned handlers before recording final counters
	if stopSuccessor != nil {
		stopSuccessor()
	}
	if !killEarly {
		kill()
	}
	j, err := mutationauthority.Open(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	stage, err := j.InspectStage(context.Background(), "volume", "delayed-stage")
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.Inspect(context.Background(), "volume", "delayed-stage")
	if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil || stage.Closed || stage.Manifest != "" || stage.Inode <= 1 || len(stage.Slices) != 1 {
		t.Fatal("uncertain stage became settled", err)
	}
	if _, err = j.Claim(context.Background(), "volume", "successor", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
		t.Fatal("late commit admitted successor", err)
	}
	if stage.Intent.SHA256 != digest(timeoutPayload()) || stage.Intent.Size != int64(len(timeoutPayload())) || digest(stage.Intent.Body) != stage.Request.Digest {
		t.Fatal("canonical stage intent changed")
	}
	mu.Lock()
	defer mu.Unlock()
	expectedPuts, expectedGets, expectedRejects := 2, 1, 1
	if killEarly {
		expectedPuts, expectedGets, expectedRejects = 1, 0, 0
	}
	if len(bodies) != expectedPuts || gets != expectedGets || rejected != expectedRejects || deletes != 0 || len(stage.Objects) != expectedPuts {
		t.Fatal("unexpected physical/journal lifecycle", len(bodies), gets, rejected, deletes, len(stage.Objects))
	}
	for i, a := range stage.Objects {
		if a.Slice != stage.Slices[0].ID || a.Provider != "minio" || a.Bucket != format.Bucket || a.Shard != 0 || "/bucket/"+a.Key != key || a.Size != int64(len(bodies[i])) || a.SHA256 != digest(bodies[i]) || a.Verified != (i == 1) {
			t.Fatal("provider attempt escaped exact journal accounting", i)
		}
	}
	if killEarly {
		if !reflect.DeepEqual(early, stage) || stage.Slices[0].Written || !bytes.Equal(stored, bodies[0]) {
			t.Fatal("effect after death was adopted or lost")
		}
	} else {
		if !reflect.DeepEqual(reported.Stage, stage) || !stage.Slices[0].Written || bytes.Equal(bodies[0], bodies[1]) || !bytes.Equal(before, stored) {
			t.Fatal("late retry changed ciphertext or uncertainty")
		}
	}
	encryption, err := object.NewDataEncryptor(object.NewRSAEncryptor(privateKey), format.EncryptAlgo)
	if err != nil {
		t.Fatal(err)
	}
	// JuiceFS AEAD decryption reuses the ciphertext backing array. Preserve the
	// provider bytes so later hashes still measure the retained raw object.
	plain, err := encryption.Decrypt(append([]byte(nil), stored...))
	if err != nil || !bytes.Equal(plain, timeoutPayload()) {
		t.Fatal("retained ciphertext differs from exact input")
	}
	if recoverStage {
		if successorPuts != 1 || successorGets != 1 || len(successorObjects) != 1 || len(recovered.Next.Objects) != 1 {
			t.Fatal("unexpected successor provider effects", successorPuts, successorGets)
		}
		a := recovered.Next.Objects[0]
		raw := successorObjects["/bucket/"+a.Key]
		plain, err := encryption.Decrypt(append([]byte(nil), raw...))
		if err != nil || !bytes.Equal(plain, []byte("new owner payload")) || digest(raw) != a.SHA256 || a.Size != int64(len(raw)) || !a.Verified {
			t.Fatalf("successor raw oracle: decrypt=%v plaintextLen=%d expectedLen=%d plaintextMatch=%t rawHashMatch=%t rawSize=%d manifestSize=%d verified=%t", err, len(plain), len("new owner payload"), bytes.Equal(plain, []byte("new owner payload")), digest(raw) == a.SHA256, len(raw), a.Size, a.Verified)
		}
	}
	t.Logf("killEarly=%t PUT=%d GET=%d DELETE=%d attempts=%d PREPARED/open retained raw=%s; child=%d reaped", killEarly, len(bodies), gets, deletes, len(stage.Objects), digest(stored), cmd.Process.Pid)
}

func TestSDKStageDelayedChild(t *testing.T) {
	path := os.Getenv("JM_STAGE_DELAY_CONFIG")
	if path == "" {
		return
	}
	cfg, err := ReadSDKConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(os.Getenv("JM_STAGE_DELAY_STATE"), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenSDK(context.Background(), j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	l, err := j.Claim(context.Background(), "volume", "stage-session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var fsynced atomic.Bool
	r.media.hook = func(phase string) {
		if phase == "fsynced" {
			fsynced.Store(true)
		}
	}
	payload := timeoutPayload()
	spec := MediaStageRequest{Volume: "volume", Session: l.Session, Epoch: l.Epoch, ID: "delayed-stage", FileID: "incarnation", ExpectedRevision: "parent@3,file@5", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("owned custody fixture"))}
	if _, err := r.StageMedia(context.Background(), spec, bytes.NewReader(payload)); !errors.Is(err, mutationauthority.ErrUncertain) {
		t.Fatal("pending SDK attempt did not refuse stage closure", err)
	}
	s, err := j.InspectStage(context.Background(), "volume", spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if s.Closed || s.Manifest != "" || len(s.Objects) != 2 || s.Objects[0].Verified || !s.Objects[1].Verified {
		t.Fatal("pending attempt missing after SDK Fsync")
	}
	fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
	var attr meta.Attr
	if eno := r.metadata.GetAttr(fctx, meta.Ino(s.Inode), &attr); eno != 0 {
		t.Fatal("stage inode missing", eno)
	}
	if _, err := r.StageMedia(context.Background(), spec, bytes.NewReader(payload)); !errors.Is(err, mutationauthority.ErrUncertain) {
		t.Fatal("uncertain stage replayed", err)
	}
	if err := json.NewEncoder(os.Stdout).Encode(delayedStageReport{Stage: s, Fsynced: fsynced.Load(), Length: attr.Length}); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
