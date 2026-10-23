//go:build gateway_acceptance

package mutationgateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

// This gate consumes an externally built, hash-pinned executable. Every child
// remains owned through Wait and scanner completion, including failure paths.
// An observation deadline triggers normal SIGTERM cleanup, never an auto-Kill
// or abandonment of the original child. Run with -timeout=0 under the retained
// outer qualification owner so Go's test timeout cannot bypass that cleanup.
type imageExecutableScan struct {
	err              error
	listening        int
	invalidAuthority bool
}
type imageExecutableOriginal struct {
	t                            *testing.T
	name                         string
	cmd                          *exec.Cmd
	done                         chan error
	scanned                      chan imageExecutableScan
	ready                        chan string
	waited, scanJoined, signaled bool
	waitErr, signalErr           error
	scan                         imageExecutableScan
}

func imageExecutableRecord(t *testing.T, fields map[string]any) {
	t.Helper()
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("image_executable %s", b)
}
func imageExecutableCheck(t *testing.T, name string) {
	t.Helper()
	imageExecutableRecord(t, map[string]any{"kind": "check", "name": name, "pass": true})
}
func imageExecutableVerifyBinary(t *testing.T) (string, string) {
	t.Helper()
	path, want := os.Getenv("JM_GATEWAY_BINARY"), os.Getenv("JM_GATEWAY_BINARY_SHA256")
	if !filepath.IsAbs(path) || !validHash(want) {
		t.Fatal("absolute JM_GATEWAY_BINARY and lowercase exact JM_GATEWAY_BINARY_SHA256 are required")
	}
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !before.Mode().IsRegular() || before.Mode().Perm()&0111 == 0 {
		t.Fatal("candidate must be a regular executable, not a symlink")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	opened, statErr := f.Stat()
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	closeErr := f.Close()
	after, afterErr := os.Lstat(path)
	if statErr != nil || readErr != nil || closeErr != nil || afterErr != nil {
		t.Fatal("candidate hashing failed", statErr, readErr, closeErr, afterErr)
	}
	if !os.SameFile(before, opened) || !os.SameFile(opened, after) || hex.EncodeToString(h.Sum(nil)) != want {
		t.Fatal("candidate executable identity/hash changed")
	}
	return path, want
}
func imageExecutableStart(t *testing.T, name string, args []string) *imageExecutableOriginal {
	t.Helper()
	binary, digest := imageExecutableVerifyBinary(t)
	cmd := exec.Command(binary, args...)
	// The custody-only executable receives no provider or SDK credentials.
	cmd.Env = []string{"PATH=/usr/bin:/bin", "GOMAXPROCS=2"}
	if tmp := os.Getenv("TMPDIR"); tmp != "" {
		cmd.Env = append(cmd.Env, "TMPDIR="+tmp)
	}
	output, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout = writer
	cmd.Stderr = writer
	p := &imageExecutableOriginal{t: t, name: name, cmd: cmd, done: make(chan error, 1), scanned: make(chan imageExecutableScan, 1), ready: make(chan string, 1)}
	if err = cmd.Start(); err != nil {
		_ = writer.Close()
		_ = output.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.finish()
		if p.signalErr != nil && !errors.Is(p.signalErr, os.ErrProcessDone) {
			t.Errorf("original %s SIGTERM: %v", name, p.signalErr)
		}
		if p.scan.err != nil {
			t.Errorf("original %s scanner: %v", name, p.scan.err)
		}
	})
	// Start both retained consumers before any later operation can fail.
	go func() { p.done <- cmd.Wait() }()
	go func() {
		var result imageExecutableScan
		scanner := bufio.NewScanner(output)
		scanner.Buffer(make([]byte, 1024), 8192)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "gateway: "+mutationauthority.ErrInvalid.Error() {
				result.invalidAuthority = true
			}
			if addr, ok := strings.CutPrefix(line, "custody gateway listening "); ok {
				result.listening++
				select {
				case p.ready <- addr:
				default:
				}
			}
		}
		result.err = errors.Join(scanner.Err(), output.Close())
		p.scanned <- result
	}()
	imageExecutableRecord(t, map[string]any{"kind": "original", "event": "start", "name": name, "pid": cmd.Process.Pid, "binary_sha256": digest})
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}
func (p *imageExecutableOriginal) recordWait(err error) {
	p.waited = true
	p.waitErr = err
	state := p.cmd.ProcessState
	exit, signaled := -1, false
	if state != nil {
		exit = state.ExitCode()
		if status, ok := state.Sys().(syscall.WaitStatus); ok {
			signaled = status.Signaled()
		}
	}
	imageExecutableRecord(p.t, map[string]any{"kind": "original", "event": "wait", "name": p.name, "pid": p.cmd.Process.Pid, "wait_completed": true, "exit_code": exit, "signaled": signaled, "wait_error": err != nil})
}
func (p *imageExecutableOriginal) finish() {
	if !p.waited {
		select {
		case err := <-p.done:
			p.recordWait(err)
		default:
		}
	}
	if !p.waited {
		if !p.signaled {
			p.signaled = true
			p.signalErr = p.cmd.Process.Signal(syscall.SIGTERM)
			imageExecutableRecord(p.t, map[string]any{"kind": "original", "event": "signal", "name": p.name, "pid": p.cmd.Process.Pid, "signal": "SIGTERM", "signal_error": p.signalErr != nil})
		}
		// No timeout abandons this exact original, even if signal delivery failed.
		p.recordWait(<-p.done)
	}
	if !p.scanJoined {
		p.scan = <-p.scanned
		p.scanJoined = true
		imageExecutableRecord(p.t, map[string]any{"kind": "original", "event": "scanner_join", "name": p.name, "pid": p.cmd.Process.Pid, "scanner_error": p.scan.err != nil, "listening_lines": p.scan.listening, "invalid_authority": p.scan.invalidAuthority})
	}
}
func (p *imageExecutableOriginal) endpoint() string {
	p.t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case addr := <-p.ready:
		host, port, err := net.SplitHostPort(addr)
		number, numberErr := strconv.Atoi(port)
		if err != nil || host != "127.0.0.1" || numberErr != nil || number < 1 || number > 65535 {
			p.t.Fatal("unexpected gateway readiness address")
		}
		return "https://" + addr
	case err := <-p.done:
		p.recordWait(err)
		p.finish()
		p.t.Fatalf("original %s exited before startup: %v", p.name, err)
	case <-timer.C:
		imageExecutableRecord(p.t, map[string]any{"kind": "original", "event": "startup_observation_deadline", "name": p.name, "pid": p.cmd.Process.Pid})
		p.t.Fatal("gateway startup observation deadline; cleanup retains and joins original")
	}
	return ""
}
func (p *imageExecutableOriginal) stopNormally() {
	p.t.Helper()
	p.finish()
	if !p.signaled || p.signalErr != nil || p.waitErr != nil || p.cmd.ProcessState == nil || p.cmd.ProcessState.ExitCode() != 0 || p.scan.err != nil || p.scan.listening != 1 {
		p.t.Fatalf("original %s did not stop normally: sigterm=%v signal_error=%v wait=%v scanner=%v listening=%d", p.name, p.signaled, p.signalErr, p.waitErr, p.scan.err, p.scan.listening)
	}
}
func (p *imageExecutableOriginal) requireIdentityRefusal() {
	p.t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case err := <-p.done:
		p.recordWait(err)
	case <-p.ready:
		p.t.Fatal("wrong store identity admitted a listener")
	case <-timer.C:
		p.t.Fatal("wrong-store startup did not terminate; cleanup retains and joins original")
	}
	p.finish()
	var exit *exec.ExitError
	if !errors.As(p.waitErr, &exit) || exit.ExitCode() != 1 || p.scan.err != nil || p.scan.listening != 0 || !p.scan.invalidAuthority {
		p.t.Fatalf("wrong-store startup did not give exact authority refusal: wait=%v scanner=%v listening=%d invalid=%v", p.waitErr, p.scan.err, p.scan.listening, p.scan.invalidAuthority)
	}
}
func imageExecutableCertificates(t *testing.T) (certificates, tls.Certificate) {
	t.Helper()
	caPublic, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "image-executable-test-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, caPublic, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	readerPublic, readerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	mint := func(serial int64, server, sameReaderKey bool) tls.Certificate {
		public, key := readerPublic, readerKey
		if !sameReaderKey {
			var err error
			public, key, err = ed25519.GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "same-image-client-name"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		der, err := x509.CreateCertificate(rand.Reader, template, ca, public, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	}
	return certificates{server: mint(2, true, false), a: mint(3, false, false), b: mint(4, false, true), roots: pool, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})}, mint(5, false, true)
}
func imageExecutableWrite(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
}
func imageExecutableClient(t *testing.T, endpoint string, cert tls.Certificate, roots *x509.CertPool) *Client {
	t.Helper()
	client, err := NewClient(endpoint, cert, roots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}
func imageExecutableFullRead(t *testing.T, ctx context.Context, client *Client, view ImageView, expected []byte) {
	t.Helper()
	if len(view.Chunks) != 3 {
		t.Fatal("complete image must expose all three chunks")
	}
	var actual []byte
	for i := range view.Chunks {
		data, err := client.ImageReadback(ctx, view, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, data...)
	}
	sum := sha256.Sum256(actual)
	if !bytes.Equal(actual, expected) || int64(len(actual)) != view.Content.Size || hex.EncodeToString(sum[:]) != view.Content.SHA256 || sum != sha256.Sum256(expected) {
		t.Fatal("complete delegated image bytes differ")
	}
}

func TestGatewayImageExecutableAuthority(t *testing.T) {
	// Preconditions are checked again immediately before each of three launches.
	_, _ = imageExecutableVerifyBinary(t)
	cfg := imageConfig(t)
	certs, replacement := imageExecutableCertificates(t)
	fixtures := t.TempDir()
	key, err := x509.MarshalPKCS8PrivateKey(certs.server.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, caPath, configPath := filepath.Join(fixtures, "server.pem"), filepath.Join(fixtures, "server-key.pem"), filepath.Join(fixtures, "clients.pem"), filepath.Join(fixtures, "images.json")
	imageExecutableWrite(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certs.server.Certificate[0]}))
	imageExecutableWrite(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	imageExecutableWrite(t, caPath, certs.caPEM)
	imageExecutableWrite(t, configPath, imageJSON(cfg.Images))
	args := func(images, listen string) []string {
		return []string{"--state", cfg.Directory, "--volume", cfg.Volume, "--listen", listen, "--tls-cert", certPath, "--tls-key", keyPath, "--client-ca", caPath, "--images-config", images}
	}
	first := imageExecutableStart(t, "initial", args(configPath, "127.0.0.1:0"))
	endpoint := first.endpoint()
	producer := imageExecutableClient(t, endpoint, certs.a, certs.roots)
	reader := imageExecutableClient(t, endpoint, certs.b, certs.roots)
	fresh := imageExecutableClient(t, endpoint, replacement, certs.roots)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	lease, err := producer.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{bytes.Repeat([]byte("start\x00"), 4096), bytes.Repeat([]byte{0, 1, 2, 255}, 64*1024), bytes.Repeat([]byte("middle-and-end"), 8192)}
	var complete []byte
	var raw []CustodyReceipt
	receipts := make([]ImageCustodyReceipt, 0, len(parts))
	for i, data := range parts {
		sum := sha256.Sum256(data)
		upload := Upload{Chunk{lease.Session, lease.Epoch, fmt.Sprintf("executable-part-%d", i), "executable-incarnation-1", int64(len(complete)), int64(len(data)), hex.EncodeToString(sum[:])}, data}
		receipt, err := producer.Upload(ctx, upload)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, receipt)
		receipts = append(receipts, ImageCustody(receipt))
		complete = append(complete, data...)
	}
	imageExecutableCheck(t, "producer-uploaded-three-committed-chunks")
	sum := sha256.Sum256(complete)
	seal := ImageSeal{1, cfg.Images.StoreID, cfg.Volume, "lib_11111111111111111111111111111111", "img_11111111111111111111111111111111", "iop_11111111111111111111111111111111", "executable-incarnation-1", hash(certs.a.Certificate[0]), hash(certs.b.Certificate[0]), ImageContent{int64(len(complete)), hex.EncodeToString(sum[:])}, receipts}
	sealed, err := producer.SealImage(ctx, seal)
	if err != nil {
		t.Fatal(err)
	}
	imageExecutableCheck(t, "images-config-enabled-producer-seal")
	// Keep this early named assertion as an executable authority causal oracle.
	_, err = producer.ImageStatus(ctx, imageRef(seal))
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "producer-denied-image-status")
	_, err = fresh.ImageStatus(ctx, imageRef(seal))
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "same-key-new-leaf-denied-before-rebind")
	view, err := reader.ImageStatus(ctx, imageRef(seal))
	if err != nil {
		t.Fatal(err)
	}
	imageExecutableFullRead(t, ctx, reader, view, complete)
	imageExecutableCheck(t, "delegated-complete-three-chunk-bytes")
	if got, err := producer.Readback(ctx, raw[0]); err != nil || !bytes.Equal(got, parts[0]) {
		t.Fatal("producer raw readback", err)
	}
	_, err = reader.Status(ctx, raw[0].Chunk)
	requireHTTP(t, err, "403")
	_, err = reader.call(ctx, http.MethodPost, "/v1/custody/readback", raw[0].Chunk)
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "raw-custody-remains-producer-owned")
	newReader := hash(replacement.Certificate[0])
	change := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_11111111111111111111111111111111", 1, &newReader}
	_, err = reader.RebindImageReader(ctx, change)
	requireHTTP(t, err, "403")
	rebound, err := producer.RebindImageReader(ctx, change)
	if err != nil || rebound.Generation != 2 {
		t.Fatal(rebound, err)
	}
	_, err = reader.ImageStatus(ctx, imageRef(seal))
	requireHTTP(t, err, "403")
	freshView, err := fresh.ImageStatus(ctx, imageRef(seal))
	if err != nil || freshView.Generation != 2 || freshView.ImageDigest != view.ImageDigest {
		t.Fatal(freshView, err)
	}
	imageExecutableFullRead(t, ctx, fresh, freshView, complete)
	_, err = fresh.Status(ctx, raw[0].Chunk)
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "producer-rebinds-only-image-reader")
	stale := change
	stale.OperationID = "irg_22222222222222222222222222222222"
	_, err = producer.RebindImageReader(ctx, stale)
	requireHTTP(t, err, "409")
	imageExecutableCheck(t, "stale-reader-cas-refused")
	revoked, err := producer.RebindImageReader(ctx, ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_33333333333333333333333333333333", 2, nil})
	if err != nil || revoked.Generation != 3 {
		t.Fatal(revoked, err)
	}
	old, err := producer.RebindImageReader(ctx, change)
	if err != nil || !bytes.Equal(imageJSON(old), imageJSON(rebound)) {
		t.Fatal("historical grant receipt changed", err)
	}
	retry, err := producer.SealImage(ctx, seal)
	if err != nil || retry != sealed {
		t.Fatal("historical seal receipt changed", err)
	}
	_, err = reader.ImageStatus(ctx, imageRef(seal))
	requireHTTP(t, err, "403")
	_, err = fresh.ImageStatus(ctx, imageRef(seal))
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "revoke-survives-historical-grant-and-seal-retries")
	producer.Close()
	reader.Close()
	fresh.Close()
	cancel()
	first.stopNormally()
	imageExecutableCheck(t, "initial-executable-sigterm-joined-zero")

	cfg.Images.Initialize = false
	wrong := *cfg.Images
	wrong.StoreID = "ims_22222222222222222222222222222222"
	wrongPath := filepath.Join(fixtures, "wrong-images.json")
	imageExecutableWrite(t, wrongPath, imageJSON(wrong))
	// A missing-port sentinel also bounds the counterfactual where admission
	// regresses: it cannot create a listener or wait indefinitely for a signal.
	denied := imageExecutableStart(t, "wrong-store", args(wrongPath, "image-test-invalid-listen-address"))
	denied.requireIdentityRefusal()
	imageExecutableCheck(t, "wrong-store-executable-startup-refused-before-listen")

	imageExecutableWrite(t, configPath, imageJSON(cfg.Images))
	second := imageExecutableStart(t, "restart", args(configPath, "127.0.0.1:0"))
	endpoint = second.endpoint()
	producer2 := imageExecutableClient(t, endpoint, certs.a, certs.roots)
	reader2 := imageExecutableClient(t, endpoint, certs.b, certs.roots)
	fresh2 := imageExecutableClient(t, endpoint, replacement, certs.roots)
	resumedCtx, resumedCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer resumedCancel()
	status, err := producer2.ImageSealStatus(resumedCtx, seal)
	if err != nil || status.State != "SEALED" || status.Receipt == nil || *status.Receipt != sealed {
		t.Fatal("persistent seal receipt changed", status, err)
	}
	retry, err = producer2.SealImage(resumedCtx, seal)
	if err != nil || retry != sealed {
		t.Fatal("restart seal retry changed", err)
	}
	imageExecutableCheck(t, "initialize-false-restart-preserves-immutable-image-receipt")
	_, err = reader2.ImageStatus(resumedCtx, imageRef(seal))
	requireHTTP(t, err, "403")
	_, err = fresh2.ImageStatus(resumedCtx, imageRef(seal))
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "restart-preserves-revoked-grant")
	readerLeaf := hash(certs.b.Certificate[0])
	regrant, err := producer2.RebindImageReader(resumedCtx, ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_44444444444444444444444444444444", 3, &readerLeaf})
	if err != nil || regrant.Generation != 4 {
		t.Fatal(regrant, err)
	}
	final, err := reader2.ImageStatus(resumedCtx, imageRef(seal))
	if err != nil || final.Generation != 4 || final.ImageDigest != sealed.ImageDigest || !bytes.Equal(imageJSON(final.Chunks), imageJSON(view.Chunks)) {
		t.Fatal("restart image identity changed", final, err)
	}
	imageExecutableFullRead(t, resumedCtx, reader2, final, complete)
	imageExecutableCheck(t, "explicit-regrant-after-restart-reads-complete-same-image")
	if got, err := producer2.Readback(resumedCtx, raw[2]); err != nil || !bytes.Equal(got, parts[2]) {
		t.Fatal("restart producer raw readback", err)
	}
	_, err = reader2.Status(resumedCtx, raw[2].Chunk)
	requireHTTP(t, err, "403")
	_, err = fresh2.ImageStatus(resumedCtx, imageRef(seal))
	requireHTTP(t, err, "403")
	imageExecutableCheck(t, "restart-preserves-raw-owner-and-leaf-isolation")
	producer2.Close()
	reader2.Close()
	fresh2.Close()
	resumedCancel()
	second.stopNormally()
	imageExecutableCheck(t, "restart-executable-sigterm-joined-zero")
}
