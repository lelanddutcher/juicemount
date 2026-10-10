//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func mediaStagePayload() []byte {
	b := make([]byte, (4<<20)+19)
	for i := range b {
		b[i] = byte((i*37 + i/251) % 251)
	}
	return b
}

func TestSDKMediaStageManifestAndCrashCustody(t *testing.T) {
	for _, phase := range []string{"intent", "created", "marker", "bound", "object-prepared", "object-readback", "fsynced", "closed", "success", "concurrent"} {
		t.Run(phase, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			key, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				t.Fatal(err)
			}
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects-%d") + "/", Shards: 2, BlockSize: 64, Compression: "none", HashPrefix: true, DirStats: true, EncryptKey: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})), EncryptAlgo: "aes256gcm-rsa"}
			setting := f.SeedExisting(t, &format)
			cfg := SDKConfig{Redis: RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(setting)}
			configPath := filepath.Join(f.Dir, "sdk.json")
			body, _ := json.Marshal(cfg)
			if err := os.WriteFile(configPath, body, 0600); err != nil {
				t.Fatal(err)
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.Context, 40*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKMediaStageChild$")
			cmd.Env = append(os.Environ(), "JM_MEDIA_STAGE_CONFIG="+configPath, "JM_MEDIA_STAGE_STATE="+f.Dir, "JM_MEDIA_STAGE_PHASE="+phase)
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
			var reported struct{ Phase string }
			decoder := json.NewDecoder(stdout)
			if err := decoder.Decode(&reported); err != nil || reported.Phase != phase {
				cmd.Process.Kill()
				cmd.Wait()
				waited = true
				remainder, _ := io.ReadAll(decoder.Buffered())
				t.Fatalf("stage child did not reach %s: %v; diagnostic=%s stderr-bytes=%d", phase, err, remainder, stderr.Len())
			}
			journalPath := filepath.Join(f.Dir, "authority.db")
			other, err := mutationauthority.Open(journalPath)
			if other != nil {
				other.Close()
			}
			if !errors.Is(err, mutationauthority.ErrLocked) {
				t.Fatal("live SDK stage lost process pin", err)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			waited = true
			if err == nil || ctx.Err() != nil || stderr.Len() != 0 {
				t.Fatal("stage process lifecycle failed", err, ctx.Err(), stderr.Len())
			}
			j, err := mutationauthority.Open(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			defer j.Close()
			stage, err := j.InspectStage(context.Background(), "volume", "media-stage")
			if err != nil {
				t.Fatal(err)
			}
			op, err := j.Inspect(context.Background(), "volume", "media-stage")
			if err != nil || op.State != mutationauthority.Prepared || op.Receipt != nil {
				t.Fatal("stage manufactured a receipt", op.State, err)
			}
			if _, err := j.Claim(context.Background(), "volume", "successor", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
				t.Fatal("uncertain stage admitted successor", err)
			}
			if phase == "intent" || phase == "created" || phase == "marker" {
				if stage.Inode != 0 {
					t.Fatal("unbound inode adopted")
				}
			}
			if phase == "bound" && (stage.Inode <= 1 || len(stage.Slices) != 0 || len(stage.Objects) != 0) {
				t.Fatal("payload effect before bound barrier")
			}
			if phase == "object-prepared" || phase == "object-readback" {
				pending := 0
				for _, a := range stage.Objects {
					if !a.Verified {
						pending++
					}
				}
				if pending == 0 || stage.Closed {
					t.Fatal("crashed object attempt lost uncertainty")
				}
				if phase == "object-readback" {
					found := false
					for _, a := range stage.Objects {
						b, e := os.ReadFile(filepath.Join(a.Bucket, a.Key))
						if e == nil && int64(len(b)) == a.Size && digest(b) == a.SHA256 && !a.Verified {
							found = true
						}
					}
					if !found {
						t.Fatal("readback crash did not retain exact uncertain object")
					}
				}
			}
			if phase == "success" || phase == "closed" || phase == "concurrent" {
				if !stage.Closed || stage.Manifest == "" || len(stage.Slices) != 2 || len(stage.Objects) != 65 {
					t.Fatal("incomplete normal stage", stage.Closed, len(stage.Slices), len(stage.Objects))
				}
				seenShards := map[int]bool{}
				for _, a := range stage.Objects {
					b, err := os.ReadFile(filepath.Join(a.Bucket, a.Key))
					if err != nil || !a.Verified || int64(len(b)) != a.Size || digest(b) != a.SHA256 {
						t.Fatal("manifest is not exact raw provider bytes", err)
					}
					seenShards[a.Shard] = true
				}
				if len(seenShards) != 2 {
					t.Fatal("manifest missed a configured shard")
				}
				if phase == "concurrent" {
					other, err := j.InspectStage(context.Background(), "volume", "other-stage")
					if err != nil || !other.Closed || other.Inode == stage.Inode || len(other.Objects) != 65 || len(other.Slices) != 2 {
						t.Fatal("concurrent operation attribution failed", err)
					}
					keys := map[string]bool{}
					for _, a := range stage.Objects {
						keys[a.Key] = true
					}
					for _, a := range other.Objects {
						b, err := os.ReadFile(filepath.Join(a.Bucket, a.Key))
						if keys[a.Key] || err != nil || !a.Verified || int64(len(b)) != a.Size || digest(b) != a.SHA256 {
							t.Fatal("concurrent stage shared or lost an exact object", err)
						}
					}
				}
			} else if stage.Closed {
				t.Fatal("premature manifest closure", phase)
			}
			t.Logf("phase=%s child=%d reaped; PREPARED inode=%d slices=%d objects=%d closed=%t", phase, cmd.Process.Pid, stage.Inode, len(stage.Slices), len(stage.Objects), stage.Closed)
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, stop := runStageSuccessor(t, ctx, configPath, f.Dir)
			if recovered.Lease.Epoch != stage.Request.Epoch+1 || !recovered.Next.Closed {
				t.Fatal("successor did not advance and stage")
			}
			stop()
			again, err := mutationauthority.Open(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			defer again.Close()
			if _, err := again.Claim(context.Background(), "volume", "unfenced-third", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
				t.Fatal("old certificate survived restart as admission", err)
			}
		})
	}
}

func TestSDKMediaStageChild(t *testing.T) {
	configPath := os.Getenv("JM_MEDIA_STAGE_CONFIG")
	if configPath == "" {
		return
	}
	cfg, err := ReadSDKConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	j, err := mutationauthority.Open(filepath.Join(os.Getenv("JM_MEDIA_STAGE_STATE"), "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	r, err := OpenSDK(context.Background(), j, "volume", *cfg)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := j.Claim(context.Background(), "volume", "stage-session", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	phase := os.Getenv("JM_MEDIA_STAGE_PHASE")
	var once sync.Once
	report := func(name string) {
		once.Do(func() {
			if err := json.NewEncoder(os.Stdout).Encode(struct{ Phase string }{name}); err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, os.Stdin)
		})
	}
	r.media.hook = func(at string) {
		if at == phase {
			report(at)
		}
	}
	payload := mediaStagePayload()
	spec := MediaStageRequest{Volume: "volume", Session: lease.Session, Epoch: lease.Epoch, ID: "media-stage", FileID: "file-incarnation", ExpectedRevision: "parent@3,file@5", Size: int64(len(payload)), SHA256: digest(payload), CustodyArtifactSHA256: digest([]byte("owned immutable custody fixture"))}
	if phase == "concurrent" {
		entered, release := make(chan struct{}, 2), make(chan struct{})
		results := make(chan error, 2)
		for _, id := range []string{"media-stage", "other-stage"} {
			s := spec
			s.ID, s.FileID = id, id+"-incarnation"
			go func() {
				_, err := r.StageMedia(context.Background(), s, &heldStageReader{Reader: bytes.NewReader(payload), entered: entered, release: release})
				results <- err
			}()
		}
		// Both exact stage inodes must reach their first payload read before
		// either is released, forcing overlapping operations on the shared SDK.
		for i := 0; i < 2; i++ {
			<-entered
		}
		close(release)
		for i := 0; i < 2; i++ {
			if err := <-results; err != nil {
				t.Fatal("concurrent stage failed", err)
			}
		}
		report("concurrent")
		return
	}
	checkpoint, err := r.StageMedia(context.Background(), spec, bytes.NewReader(payload))
	if err != nil {
		t.Fatal("stage failed", err)
	}
	if !checkpoint.Closed || len(checkpoint.Slices) != 2 {
		t.Fatal("missing closed stage checkpoint")
	}
	// A callback delayed until after closure must fail before any provider PUT.
	// A new canonical block for an existing slice would succeed at the
	// create-only provider if CLOSED admission were accidentally bypassed.
	rawKey := r.media.key(checkpoint.Slices[0].ID, 66, 1)
	h := fnv.New32a()
	_, _ = h.Write([]byte(rawKey))
	bucket := fmt.Sprintf(r.media.format.Bucket, int(h.Sum32()%2))
	objectPath := filepath.Join(bucket, rawKey)
	if _, err := os.Stat(objectPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("late-attempt target already exists", err)
	}
	key := strings.TrimPrefix(rawKey, "fixture/")
	if err := r.objects.Put(key, bytes.NewReader([]byte("late callback"))); err == nil {
		t.Fatal("closed raw gate admitted a late callback")
	}
	if _, err := os.Stat(objectPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("late callback reached provider storage", err)
	}
	unknownKey := r.media.key(999999, 0, 1)
	h.Reset()
	_, _ = h.Write([]byte(unknownKey))
	unknownPath := filepath.Join(fmt.Sprintf(r.media.format.Bucket, int(h.Sum32()%2)), unknownKey)
	if _, err := os.Stat(unknownPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unknown slice target already exists", err)
	}
	if err := r.objects.Put(strings.TrimPrefix(unknownKey, "fixture/"), bytes.NewReader([]byte("unknown slice"))); err == nil {
		t.Fatal("unknown background slice admitted")
	}
	if _, err := os.Stat(unknownPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unknown slice reached provider storage", err)
	}
	if _, err := r.StageMedia(context.Background(), spec, bytes.NewReader(payload)); !errors.Is(err, mutationauthority.ErrUncertain) {
		t.Fatal("stage replayed", err)
	}
	report("success")
}

type heldStageReader struct {
	io.Reader
	once    sync.Once
	entered chan<- struct{}
	release <-chan struct{}
}

func (r *heldStageReader) Read(p []byte) (int, error) {
	r.once.Do(func() { r.entered <- struct{}{}; <-r.release })
	return r.Reader.Read(p)
}
