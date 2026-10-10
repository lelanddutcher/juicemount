//go:build sdkacceptance

package mutationbackend

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"database/sql"
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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/redis/go-redis/v9"
)

// Observe the real Redis reply, not only the final refusal: an unbounded GET
// followed by a length check would reject the value but fail this oracle.
type settingReadHook struct {
	command string
	args    string
	bytes   int
}

func (h *settingReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *settingReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *settingReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "get" || cmd.Name() == "getrange" {
			h.command, h.args = cmd.Name(), fmt.Sprint(cmd.Args())
			if value, ok := cmd.(*redis.StringCmd); ok {
				h.bytes = len(value.Val())
			}
		}
		return err
	}
}

func TestSDKSettingReadBoundsWireReply(t *testing.T) {
	f := backendfixture.New(t, StaticACL)
	client := f.Admin()
	defer client.Close()
	hook := &settingReadHook{}
	client.AddHook(hook)
	for _, size := range []int{64 << 10, 128 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			body := strings.Repeat("x", size)
			if err := client.Set(f.Context, "setting", body, 0).Err(); err != nil {
				t.Fatal(err)
			}
			sdk := &SDK{reader: client, formatHash: digest([]byte(body))}
			got, err := sdk.readSetting(f.Context)
			wantBytes := size
			if size > 64<<10 {
				wantBytes = (64 << 10) + 1
				if !errors.Is(err, ErrUnavailable) || got != nil {
					t.Fatal("oversized setting was not refused")
				}
			} else if err != nil || string(got) != body {
				t.Fatal("maximum-size setting was not preserved")
			}
			if hook.command != "getrange" || hook.args != "[getrange setting 0 65536]" || hook.bytes != wantBytes {
				t.Fatalf("unbounded setting response: command=%s args=%s bytes=%d", hook.command, hook.args, hook.bytes)
			}
		})
	}
}

func TestSDKExistingVolumeProcessLifecycle(t *testing.T) {
	for _, mode := range []string{"success", "legacy-binding", "missing", "format-hash", "wrong-volume", "unknown-field", "oversized-format", "ranger", "partial-start", "changed-format", "ambient", "object-corrupt", "sharded-encrypted"} {
		t.Run(mode, func(t *testing.T) {
			f := backendfixture.New(t, StaticACL)
			format := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "lz4", HashPrefix: true, DirStats: true}
			var objectPuts, objectGets, objectDeletes atomic.Int64
			var objectMu sync.Mutex
			acknowledged, retained := make(map[string][]byte), make(map[string][]byte)
			var corruptServer *httptest.Server
			var providerConfig SDKConfig
			if mode == "object-corrupt" {
				probe := newCreateOnlyProbeFixture(t)
				corruptServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if probe(w, req) {
						return
					}
					if req.Method == http.MethodPut {
						body, err := io.ReadAll(req.Body)
						if err != nil {
							w.WriteHeader(500)
							return
						}
						objectMu.Lock()
						if req.Header.Get("If-None-Match") != "*" {
							objectMu.Unlock()
							t.Error("missing conditional payload PUT")
							w.WriteHeader(400)
							return
						}
						if _, exists := retained[req.URL.Path]; exists {
							objectMu.Unlock()
							w.WriteHeader(412)
							return
						}
						acknowledged[req.URL.Path], retained[req.URL.Path] = body, body
						objectMu.Unlock()
						objectPuts.Add(1)
						w.WriteHeader(200)
					} else if req.Method == http.MethodGet {
						objectGets.Add(1)
						wrong := []byte("wrong persisted object")
						sum := sha256.Sum256(wrong)
						w.Header().Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
						_, _ = w.Write(wrong)
					} else if req.Method == http.MethodDelete {
						objectDeletes.Add(1)
						objectMu.Lock()
						delete(retained, req.URL.Path)
						objectMu.Unlock()
						w.WriteHeader(204)
					} else {
						w.WriteHeader(405)
					}
				}))
				defer corruptServer.Close()
				format.Storage, format.Bucket, format.AccessKey, format.SecretKey = "minio", corruptServer.URL+"/bucket", "fixture-key", "fixture-secret"
				pathStyle := true
				providerConfig.Region, providerConfig.PathStyle = "us-east-1", &pathStyle
			}
			if mode == "sharded-encrypted" {
				key, err := rsa.GenerateKey(rand.Reader, 2048)
				if err != nil {
					t.Fatal(err)
				}
				format.Shards, format.Bucket = 2, filepath.Join(f.Dir, "objects-%d")+"/"
				format.EncryptKey = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
				format.EncryptAlgo = "aes256gcm-rsa"
			}
			body := f.SeedExisting(t, &format)
			cfg := SDKConfig{Redis: RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: digest(body), Region: providerConfig.Region, PathStyle: providerConfig.PathStyle}
			admin := f.Admin()
			switch mode {
			case "legacy-binding":
				journalPath := filepath.Join(f.Dir, "authority.db")
				j, err := mutationauthority.Open(journalPath)
				if err != nil {
					t.Fatal(err)
				}
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
				old, _ := json.Marshal(struct {
					Kind, Volume string
					Config       SDKConfig
				}{"juicefs-1.3.1-sdk-config-v1", "volume", cfg})
				db, err := sql.Open("sqlite", journalPath)
				if err != nil {
					t.Fatal(err)
				}
				_, insertErr := db.Exec("INSERT INTO backend_configuration(id,volume,digest) VALUES(1,?,?)", "volume", digest(old))
				closeErr := db.Close()
				if insertErr != nil || closeErr != nil {
					t.Fatal(insertErr, closeErr)
				}
			case "missing":
				if err := admin.Del(f.Context, "setting").Err(); err != nil {
					t.Fatal(err)
				}
			case "format-hash":
				cfg.FormatSHA256 = strings.Repeat("0", 64)
			case "oversized-format":
				body = append(body, bytes.Repeat([]byte(" "), 128<<10)...)
			case "wrong-volume":
				format.UUID = "foreign"
				body, _ = json.Marshal(format)
			case "unknown-field":
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(body, &fields); err != nil {
					t.Fatal(err)
				}
				fields["UnrecognizedSemantics"] = json.RawMessage("true")
				body, _ = json.Marshal(fields)
			case "ranger":
				format.RangerRestUrl = "https://unconfigured.invalid"
				body, _ = json.Marshal(format)
			case "partial-start":
				// JuiceFS counterKey lowercases nextSession to nextsession.
				if err := admin.Set(f.Context, "nextsession", "not-an-integer", 0).Err(); err != nil {
					t.Fatal(err)
				}
				if got, err := admin.Get(f.Context, "nextsession").Result(); err != nil || got != "not-an-integer" {
					t.Fatal("session fault preflight readback failed")
				}
				if err := admin.IncrBy(f.Context, "nextsession", 1).Err(); err == nil || !strings.Contains(err.Error(), "not an integer") {
					t.Fatal("session fault preflight did not reject SDK counter command")
				}
			}
			if mode == "wrong-volume" || mode == "unknown-field" || mode == "ranger" || mode == "oversized-format" {
				if err := admin.Set(f.Context, "setting", body, 0).Err(); err != nil {
					t.Fatal(err)
				}
				cfg.FormatSHA256 = digest(body)
			}
			_ = admin.Close()
			configPath := filepath.Join(f.Dir, "sdk.json")
			b, _ := json.Marshal(cfg)
			if err := os.WriteFile(configPath, b, 0600); err != nil {
				t.Fatal(err)
			}
			runSDKChild(t, f, mode, configPath)
			if mode == "object-corrupt" {
				// The child is reaped. Join all provider handlers before inventorying
				// acknowledged bytes, including sibling uploads after the first error.
				corruptServer.Close()
				objectMu.Lock()
				defer objectMu.Unlock()
				if objectPuts.Load() == 0 || objectGets.Load() == 0 || len(acknowledged) == 0 {
					t.Fatal("SDK corruption discriminator did not reach provider PUT/GET")
				}
				if objectDeletes.Load() != 0 {
					t.Errorf("failed SDK upload attempted %d uncertain object deletions", objectDeletes.Load())
				}
				for key, body := range acknowledged {
					if got, ok := retained[key]; !ok || !bytes.Equal(got, body) {
						t.Error("failed SDK upload removed or changed acknowledged provider bytes")
					}
				}
				t.Logf("failed SDK upload: PUT=%d GET=%d DELETE=%d acknowledged=%d retained=%d", objectPuts.Load(), objectGets.Load(), objectDeletes.Load(), len(acknowledged), len(retained))
			}
			if mode == "sharded-encrypted" {
				runSDKChild(t, f, "verify-sharded", configPath)
			}
			if mode == "success" {
				runSDKChild(t, f, "verify", configPath)
				cfg.Region = "changed-routing"
				b, _ := json.Marshal(cfg)
				if err := os.WriteFile(configPath, b, 0600); err != nil {
					t.Fatal(err)
				}
				runSDKChild(t, f, "changed-binding", configPath)
			}
		})
	}
}

func runSDKChild(t *testing.T, f *backendfixture.Fixture, mode, path string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.Context, 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestSDKLifecycleChild$")
	cmd.Env = append(os.Environ(), "JM_SDK_CASE="+mode, "JM_SDK_CONFIG="+path, "JM_SDK_STATE="+f.Dir)
	if mode == "ambient" {
		cmd.Env = append(cmd.Env, "AWS_ACCESS_KEY_ID=unexpected-fixture-key")
	}
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
		_ = stdin.Close()
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	var result struct {
		Retained bool
		Phase    string
	}
	d := json.NewDecoder(stdout)
	if err := d.Decode(&result); err != nil || !result.Retained || result.Phase != mode {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		waited = true
		b, _ := io.ReadAll(d.Buffered())
		t.Fatalf("SDK child failed before retained-pin proof: %v; %s %s", err, b, stderr.String())
	}
	journalPath := filepath.Join(f.Dir, "authority.db")
	other, err := mutationauthority.Open(journalPath)
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, mutationauthority.ErrLocked) {
		t.Fatalf("child released live authority: %v", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err = cmd.Wait()
	waited = true
	if err == nil || ctx.Err() != nil {
		t.Fatalf("SDK child termination: %v (context %v)", err, ctx.Err())
	}
	if stderr.Len() != 0 {
		// Explicit private evidence destination preserves unexpected child diagnostics
		// without echoing potential provider values into the test/application output.
		if dir := os.Getenv("JM_SDK_DIAGNOSTIC_DIR"); dir != "" {
			info, err := os.Stat(dir)
			if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
				t.Fatal("invalid private SDK diagnostic directory")
			}
			path := filepath.Join(dir, fmt.Sprintf("sdk-%s-%d.stderr", mode, cmd.Process.Pid))
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal("could not retain SDK diagnostics")
			}
			_, writeErr := f.Write(stderr.Bytes())
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatal("could not retain complete SDK diagnostics")
			}
		}
		t.Fatalf("SDK startup/close emitted library diagnostics: bytes=%d sha256=%s", stderr.Len(), digest(stderr.Bytes()))
	}
	next, err := mutationauthority.Open(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("owned SDK child pid=%d reaped; phase=%s", cmd.Process.Pid, mode)
}

func sdkPayload() []byte {
	b := make([]byte, 4*(64<<10)+19)
	for i := range b {
		b[i] = byte((i*37 + i/251 + 17) % 251)
	}
	return b
}

func TestSDKLifecycleChild(t *testing.T) {
	mode := os.Getenv("JM_SDK_CASE")
	if mode == "" {
		return
	}
	cfg, err := ReadSDKConfig(os.Getenv("JM_SDK_CONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	dir := os.Getenv("JM_SDK_STATE")
	j, err := mutationauthority.Open(filepath.Join(dir, "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	r, err := OpenSDK(ctx, j, "volume", *cfg)
	success := mode == "success" || mode == "verify" || mode == "changed-format" || mode == "object-corrupt" || mode == "sharded-encrypted" || mode == "verify-sharded"
	if success {
		if err != nil {
			t.Fatal(err)
		}
		if err := r.Check(ctx); err != nil {
			t.Fatal(err)
		}
		if mode == "object-corrupt" {
			fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
			file, eno := r.filesystem.Create(fctx, "/unverified", 0600, 0077)
			if eno != 0 {
				t.Fatal(eno)
			}
			n, eno := file.Pwrite(fctx, sdkPayload(), 0)
			if eno != 0 || n != len(sdkPayload()) {
				t.Fatal(n, eno)
			}
			if eno = file.Fsync(fctx); eno == 0 {
				t.Fatal("SDK Fsync accepted corrupt provider acknowledgement")
			}
			var attr meta.Attr
			if eno = r.metadata.GetAttr(fctx, file.Inode(), &attr); eno != 0 || attr.Length != 0 {
				t.Fatalf("unverified bytes reached metadata: length=%d errno=%d", attr.Length, eno)
			}
			// Retain the failed writer until this pinned process exits; no cancellation
			// or failed Fsync permits cleanup of an uncertain remote object.
		} else if mode == "changed-format" {
			body, err := r.reader.Get(ctx, "setting").Bytes()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.reader.Set(ctx, "setting", append(body, ' '), 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.Check(ctx); !errors.Is(err, ErrUnavailable) {
				t.Fatal("changed raw format admitted")
			}
			if err := r.reader.Set(ctx, "setting", body, 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := r.Check(ctx); !errors.Is(err, ErrUnavailable) {
				t.Fatal("format restoration reopened admission")
			}
		} else {
			fctx := meta.NewContext(uint32(os.Getpid()), uint32(os.Getuid()), []uint32{uint32(os.Getgid())})
			file, eno := r.filesystem.Open(fctx, "/payload", meta.MODE_MASK_R)
			if mode == "success" || mode == "sharded-encrypted" {
				file, eno = r.filesystem.Create(fctx, "/payload", 0600, 0077)
				if eno != 0 {
					t.Fatalf("SDK create errno=%d", eno)
				}
				n, eno := file.Pwrite(fctx, sdkPayload(), 0)
				if eno != 0 || n != len(sdkPayload()) {
					t.Fatalf("SDK write n=%d errno=%d", n, eno)
				}
				if eno := file.Fsync(fctx); eno != 0 {
					t.Fatalf("SDK fsync errno=%d", eno)
				}
			}
			if eno != 0 {
				t.Fatalf("SDK open errno=%d", eno)
			}
			got := make([]byte, len(sdkPayload()))
			n, e := file.Pread(fctx, got, 0)
			if e != nil || n != len(got) || !bytes.Equal(got, sdkPayload()) {
				t.Fatal("full SDK readback differs")
			}
			if eno := file.Close(fctx); eno != 0 {
				t.Fatalf("SDK file close errno=%d", eno)
			}
			roots := []string{filepath.Join(dir, "objects")}
			if strings.Contains(mode, "sharded") {
				roots = []string{filepath.Join(dir, "objects-0"), filepath.Join(dir, "objects-1")}
			}
			objects := 0
			for _, root := range roots {
				err := filepath.WalkDir(filepath.Join(root, "fixture", "chunks"), func(_ string, d os.DirEntry, err error) error {
					if os.IsNotExist(err) {
						return nil
					}
					if err != nil {
						return err
					}
					if !d.IsDir() {
						objects++
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			if objects == 0 {
				t.Fatal("upstream prefixed object layout missing")
			}
		}
		closeErr := r.Close()
		if closeErr != nil && mode != "object-corrupt" {
			t.Fatal(closeErr)
		}
		if err := r.Close(); err != closeErr {
			t.Fatal(err)
		}
		if err := r.Check(ctx); !errors.Is(err, ErrUnavailable) {
			t.Fatal("closed SDK reopened")
		}
	} else {
		if err == nil || r != nil {
			t.Fatal("unsafe constructor succeeded")
		}
		if (mode == "changed-binding" || mode == "legacy-binding") && !strings.Contains(err.Error(), "configuration binding") {
			t.Fatalf("successor binding changed: %v", err)
		}
		if mode == "partial-start" && !strings.Contains(err.Error(), "session startup") {
			t.Fatalf("did not exercise partial session startup: %v", err)
		}
	}
	if _, err := OpenSDK(ctx, j, "volume", *cfg); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
		t.Fatal("SDK constructor retried")
	}
	for range 2 {
		if err := j.Close(); !errors.Is(err, mutationauthority.ErrProcessLifetime) {
			t.Fatal("SDK released journal pin")
		}
	}
	other, err := mutationauthority.Open(filepath.Join(dir, "authority.db"))
	if other != nil {
		_ = other.Close()
	}
	if !errors.Is(err, mutationauthority.ErrLocked) {
		t.Fatal("same-process successor admitted")
	}
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Retained bool
		Phase    string
	}{true, mode}); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
