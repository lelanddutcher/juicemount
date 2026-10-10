//go:build gateway_acceptance

package mutationgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The build owner supplies an exact candidate binary; this gate never compiles
// in a test subprocess or silently substitutes an in-process handler.
func TestGatewayExecutableTLS(t *testing.T) {
	binary := os.Getenv("JM_GATEWAY_BINARY")
	if !filepath.IsAbs(binary) {
		t.Fatal("JM_GATEWAY_BINARY must name the built candidate")
	}
	cfg := testConfig(t)
	certs := testCertificates(t)
	fixtures := t.TempDir()
	key, err := x509.MarshalPKCS8PrivateKey(certs.server.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"server.pem":     pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certs.server.Certificate[0]}),
		"server-key.pem": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}),
		"clients.pem":    certs.caPEM,
	} {
		if err := os.WriteFile(filepath.Join(fixtures, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "--state", cfg.Directory, "--volume", cfg.Volume, "--listen", "127.0.0.1:0", "--tls-cert", filepath.Join(fixtures, "server.pem"), "--tls-key", filepath.Join(fixtures, "server-key.pem"), "--client-ca", filepath.Join(fixtures, "clients.pem"))
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
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
	defer func() {
		if !stopped {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			<-done
		}
		<-scanned
	}()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		stopped = true
		t.Fatalf("gateway exited before startup: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	c, err := NewClient("https://"+addr, certs.a, certs.roots)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	l, err := c.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	u := testUpload(l, "real-command")
	r, err := c.Upload(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Readback(ctx, r); err != nil || !bytes.Equal(got, u.Data) {
		t.Fatalf("command readback: %q %v", got, err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	err = <-done
	stopped = true
	if err != nil {
		t.Fatalf("graceful gateway exit: %v", err)
	}
	// A fresh process-equivalent owner recovers the actual command's journal.
	g := testGateway(t, cfg)
	stored, data, err := g.custody(context.Background(), u.Chunk)
	if err != nil || stored != r || !bytes.Equal(data, u.Data) {
		t.Fatalf("command custody did not survive shutdown: %+v %v", stored, err)
	}
}
