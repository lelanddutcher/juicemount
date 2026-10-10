package mutationbackend

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

func TestSDKPrivateConfiguration(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "sdk.json")
	valid := `{"redis":{"socket":"/private/socket","acl_file":"/private/acl","control_secret_file":"/private/secret"},"format_sha256":"` + strings.Repeat("a", 64) + `"}`
	for _, data := range []string{valid, valid + ` {}`, strings.Replace(valid, "format_sha256", "unknown", 1), `null`} {
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := ReadSDKConfig(path)
		if (err == nil) != (data == valid) {
			t.Fatalf("configuration acceptance mismatch: %v", err)
		}
	}
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadSDKConfig(path); err == nil {
		t.Fatal("nonprivate configuration accepted")
	}
}

func TestSDKFormatRefusesUnsupportedSemantics(t *testing.T) {
	base := meta.Format{Name: "fixture", UUID: "volume", BlockSize: 64, Compression: "lz4"}
	if !validSDKFormat(base) {
		t.Fatal("valid baseline refused")
	}
	for _, change := range []func(*meta.Format){
		func(f *meta.Format) { f.Name = "../escape" }, func(f *meta.Format) { f.BlockSize = 65 },
		func(f *meta.Format) { f.Compression = "unknown" }, func(f *meta.Format) { f.Shards = 257 },
		func(f *meta.Format) { f.RangerRestUrl = "https://unconfigured.invalid" },
		func(f *meta.Format) { f.UploadLimit = -1 }, func(f *meta.Format) { f.EncryptAlgo = "aes256gcm" },
	} {
		f := base
		change(&f)
		if validSDKFormat(f) {
			t.Fatal("unsupported format accepted")
		}
	}
	for _, bucket := range []string{"https://host/bucket?tls-insecure-skip-verify=true", "https://host/bucket?ca-certs=/private/ca", "https://host/bucket/ignored", "https://host/bucket?unknown=true", "https://user:secret@host/bucket"} {
		if validSDKBucket("minio", bucket, "us-east-1") {
			t.Fatal("ambiguous provider input accepted")
		}
	}
	if validSDKBucket("s3", "https://s3.eu-west-1.amazonaws.com/bucket", "us-east-1") {
		t.Fatal("region disagreement accepted")
	}
}

func TestSDKStoragePrefixShardsAndEncryption(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			f := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(root, "objects-%d") + "/", BlockSize: 64, Shards: 2}
			if encrypted {
				key, err := rsa.GenerateKey(rand.Reader, 2048)
				if err != nil {
					t.Fatal(err)
				}
				f.EncryptKey = string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
				f.EncryptAlgo = "aes256gcm-rsa"
			}
			blob, err := sdkStorage(f, SDKConfig{})
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("exact prefixed binary object\x00with retained bytes")
			for _, key := range []string{"chunks/a", "chunks/b", "chunks/c", "chunks/d"} {
				if err := blob.Put(key, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
				if err := blob.Put(key, bytes.NewReader([]byte("replacement"))); err == nil {
					t.Fatal("duplicate create replaced the object")
				}
				if err := blob.Delete(key); !errors.Is(err, errObjectReclamation) {
					t.Fatal("SDK object deletion was not refused", err)
				}
				r, err := blob.Get(key, 0, -1)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				_ = r.Close()
				if err != nil || !bytes.Equal(got, data) {
					t.Fatal("object readback differs")
				}
			}
			count, probes := 0, 0
			err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					return nil
				}
				if strings.Contains(path, "/fixture/"+createOnlyProbePrefix) {
					probes++
					return nil
				}
				count++
				if !strings.Contains(path, "/fixture/chunks/") {
					t.Fatal("wrong existing-volume prefix")
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Equal(raw, data) == encrypted {
					t.Fatal("encryption wrapper order differs")
				}
				return nil
			})
			if err != nil || count != 4 || probes != 2 {
				t.Fatalf("stored object count=%d err=%v", count, err)
			}
		})
	}
	f := meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: t.TempDir() + "/", StorageClass: "unsupported"}
	if _, err := sdkStorage(f, SDKConfig{}); err == nil {
		t.Fatal("file storage class silently ignored")
	}
	f.StorageClass = ""
	f.KeyEncrypted = true
	f.SecretKey = "AA=="
	if _, err := sdkStorage(f, SDKConfig{}); err == nil {
		t.Fatal("short encrypted metadata accepted")
	}
}

func TestSDKProviderIsolation(t *testing.T) {
	for _, provider := range []string{"s3", "minio"} {
		t.Run(provider, func(t *testing.T) {
			for _, key := range []string{"AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE", "AWS_EC2_METADATA_DISABLED", "AWS_REGION", "AWS_DEFAULT_REGION", "MINIO_REGION", "JFS_S3_VHOST_STYLE"} {
				t.Setenv(key, os.Getenv(key))
			}
			poison := filepath.Join(t.TempDir(), "poisoned-aws-config")
			if err := os.WriteFile(poison, []byte("[default]\nregion = wrong-region\nendpoint_url = http://unconfigured.invalid\n"), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AWS_CONFIG_FILE", poison)
			if cleanSDKEnvironment() {
				t.Fatal("ambient provider configuration accepted")
			}
			gotPaths := make(chan string, 4)
			var stored []byte
			var storedMu sync.Mutex
			probe := newCreateOnlyProbeFixture(t)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if probe(w, r) {
					return
				}
				storedMu.Lock()
				defer storedMu.Unlock()
				gotPaths <- r.Method + " " + r.URL.Path
				if !strings.Contains(r.Header.Get("Authorization"), "/us-east-1/s3/") {
					t.Error("explicit signing region lost")
				}
				if r.Header.Get("X-Amz-Security-Token") != "fixture-token" {
					t.Error("explicit session token lost")
				}
				if r.Method == http.MethodPut {
					stored, _ = io.ReadAll(r.Body)
					w.WriteHeader(http.StatusOK)
				} else if r.Method == http.MethodGet {
					_, _ = w.Write(stored)
				} else {
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer srv.Close()
			style := true
			f := meta.Format{Name: "fixture", Storage: provider, Bucket: srv.URL + "/bucket", AccessKey: "fixture-key", SecretKey: "fixture-secret", SessionToken: "fixture-token"}
			blob, err := sdkStorage(f, SDKConfig{Region: "us-east-1", PathStyle: &style})
			if err != nil {
				t.Fatal(err)
			}
			if err := blob.Put("chunks/a", bytes.NewReader([]byte("object"))); err != nil {
				t.Fatal("explicit provider request failed")
			}
			gotPath := <-gotPaths
			if gotPath != "PUT /bucket/fixture/chunks/a" {
				t.Fatalf("provider layout=%q", gotPath)
			}
			if got := <-gotPaths; got != "GET /bucket/fixture/chunks/a" {
				t.Fatalf("verification path=%q", got)
			}
			if os.Getenv("AWS_CONFIG_FILE") != os.DevNull || os.Getenv("AWS_SHARED_CREDENTIALS_FILE") != os.DevNull {
				t.Fatal("shared config discovery not disabled")
			}
		})
	}
}
