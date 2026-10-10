//go:build sdkacceptance

package mutationgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/lelanddutcher/juicemount/internal/backendfixture"
	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationbackend"
)

func sdkGatewayFixture(t *testing.T) (*backendfixture.Fixture, Config, string) {
	t.Helper()
	f := backendfixture.New(t, mutationbackend.StaticACL)
	body := f.SeedExisting(t, &meta.Format{Name: "fixture", UUID: "volume", Storage: "file", Bucket: filepath.Join(f.Dir, "objects") + "/", BlockSize: 64, Compression: "none", DirStats: true})
	cfg := Config{Directory: filepath.Join(f.Dir, "gateway"), Volume: "volume", LeaseTTL: time.Minute,
		SDK: &mutationbackend.SDKConfig{Redis: mutationbackend.RedisConfig{Socket: f.Socket, ACLFile: f.ACLFile, SecretFile: f.SecretFile}, FormatSHA256: hash(body)}}
	if err := os.Mkdir(cfg.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.Dir, "sdk.json")
	b, _ := json.Marshal(cfg.SDK)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	return f, cfg, path
}

func TestGatewaySDKFailureRetainsAuthority(t *testing.T) {
	for _, mode := range []string{"wrong-format", "successful-close"} {
		t.Run(mode, func(t *testing.T) {
			f, cfg, path := sdkGatewayFixture(t)
			if mode == "wrong-format" {
				cfg.SDK.FormatSHA256 = strings.Repeat("0", 64)
				b, _ := json.Marshal(cfg.SDK)
				if err := os.WriteFile(path, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(f.Context, exe, "-test.run=^TestGatewaySDKChild$")
			cmd.Env = append(os.Environ(), "JM_GATEWAY_SDK_CASE="+mode, "JM_GATEWAY_SDK_CONFIG="+path, "JM_GATEWAY_SDK_STATE="+cfg.Directory)
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
			d := json.NewDecoder(stdout)
			var retained bool
			if err := d.Decode(&retained); err != nil || !retained {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				waited = true
				b, _ := io.ReadAll(d.Buffered())
				t.Fatalf("gateway SDK child handshake: %v %s %s", err, b, stderr.String())
			}
			other, err := mutationauthority.Open(filepath.Join(cfg.Directory, "authority.db"))
			if other != nil {
				_ = other.Close()
			}
			if !errors.Is(err, mutationauthority.ErrLocked) {
				t.Fatal("live constructor released authority")
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			waited = true
			if err == nil || f.Context.Err() != nil {
				t.Fatalf("owned child exit: %v", err)
			}
			other, err = mutationauthority.Open(filepath.Join(cfg.Directory, "authority.db"))
			if err != nil {
				t.Fatal(err)
			}
			_ = other.Close()
			t.Logf("owned gateway SDK child pid=%d reaped", cmd.Process.Pid)
		})
	}
}

func TestGatewaySDKChild(t *testing.T) {
	mode := os.Getenv("JM_GATEWAY_SDK_CASE")
	if mode == "" {
		return
	}
	sdk, err := mutationbackend.ReadSDKConfig(os.Getenv("JM_GATEWAY_SDK_CONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Directory: os.Getenv("JM_GATEWAY_SDK_STATE"), Volume: "volume", LeaseTTL: time.Minute, SDK: sdk}
	g, err := Open(cfg)
	if mode == "wrong-format" {
		if g != nil || !errors.Is(err, mutationauthority.ErrProcessLifetime) {
			t.Fatal("failed constructor published gateway or released pin")
		}
	} else {
		if err != nil {
			t.Fatal(err)
		}
		_, _, _ = exerciseCloseDuringUpload(t, cfg, g, true)
		for range 2 {
			if err := g.Close(); err != mutationauthority.ErrProcessLifetime {
				t.Fatalf("SDK Close result: %v", err)
			}
		}
	}
	cfg.SDK = nil
	if other, err := Open(cfg); other != nil || !errors.Is(err, mutationauthority.ErrProcessLifetime) {
		t.Fatal("custody-only fallback admitted after SDK attempt")
	}
	cfg.Directory = filepath.Join(filepath.Dir(cfg.Directory), "fallback")
	if err := os.Mkdir(cfg.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(cfg); other != nil || !errors.Is(err, mutationauthority.ErrProcessLifetime) {
		t.Fatal("different state bypassed terminal SDK attempt")
	}
	if err := json.NewEncoder(os.Stdout).Encode(true); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestGatewaySDKExecutableCustodyAndRestart(t *testing.T) {
	binary := os.Getenv("JM_GATEWAY_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("SDK executable gate requires exact JM_GATEWAY_BINARY")
	}
	f, cfg, path := sdkGatewayFixture(t)
	certs := testCertificates(t)
	key, err := x509.MarshalPKCS8PrivateKey(certs.server.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"server.pem": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certs.server.Certificate[0]}), "key.pem": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), "clients.pem": certs.caPEM} {
		if err := os.WriteFile(filepath.Join(f.Dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var u Upload
	var receipt CustodyReceipt
	for generation := 1; generation <= 2; generation++ {
		ctx, cancel := context.WithTimeout(f.Context, 20*time.Second)
		cmd := exec.CommandContext(ctx, binary, "--state", cfg.Directory, "--volume", cfg.Volume, "--listen", "127.0.0.1:0", "--tls-cert", filepath.Join(f.Dir, "server.pem"), "--tls-key", filepath.Join(f.Dir, "key.pem"), "--client-ca", filepath.Join(f.Dir, "clients.pem"), "--sdk-config", path)
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		cmd.Stderr = cmd.Stdout
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		ready := make(chan string, 1)
		scanned := make(chan struct{})
		go func() {
			defer close(scanned)
			s := bufio.NewScanner(pipe)
			for s.Scan() {
				if addr, ok := strings.CutPrefix(s.Text(), "custody gateway listening "); ok {
					select {
					case ready <- addr:
					default:
					}
				}
			}
		}()
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		stopped := false
		// Use a nested function so every generation drains/reaps before the next.
		func() {
			defer func() {
				if !stopped {
					_ = cmd.Process.Kill()
					<-done
				}
				<-scanned
				cancel()
			}()
			var addr string
			select {
			case addr = <-ready:
			case err := <-done:
				stopped = true
				t.Fatalf("SDK executable exited before listener: %v", err)
			case <-ctx.Done():
				t.Fatal("SDK executable startup timeout")
			}
			c, err := NewClient("https://"+addr, certs.a, certs.roots)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if generation == 1 {
				lease, err := c.Claim(ctx)
				if err != nil {
					t.Fatal(err)
				}
				u = testUpload(lease, "sdk-executable")
				receipt, err = c.Upload(ctx, u)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				r, err := c.Status(ctx, u.Chunk)
				if err != nil || r != receipt {
					t.Fatal("SDK restart changed custody receipt")
				}
			}
			data, err := c.Readback(ctx, receipt)
			if err != nil || !bytes.Equal(data, u.Data) || receipt.Kind != format {
				t.Fatal("SDK attachment changed custody semantics")
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			err = <-done
			stopped = true
			if err != nil {
				t.Fatalf("SDK orderly shutdown: %v", err)
			}
		}()
		t.Logf("owned SDK executable pid=%d reaped; generation=%d", cmd.Process.Pid, generation)
	}
	withoutSDK := cfg
	withoutSDK.SDK = nil
	if other, err := Open(withoutSDK); other != nil || err == nil {
		t.Fatal("fresh process omitted durable SDK attachment")
	}
	j, err := mutationauthority.Open(filepath.Join(cfg.Directory, "authority.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	rows, err := j.BackendProcesses(context.Background(), cfg.Volume)
	if err != nil || len(rows) != 2 || rows[1].Generation != 2 {
		t.Fatalf("SDK executable generations: %d %v", len(rows), err)
	}
}
