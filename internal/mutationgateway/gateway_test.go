package mutationgateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

type certificates struct {
	server, a, b tls.Certificate
	roots        *x509.CertPool
	caPEM        []byte
}

func testCertificates(t *testing.T) certificates {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "custody test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	mint := func(serial int64, server bool) tls.Certificate {
		p, k, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		x := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			x.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
			x.DNSNames = []string{"localhost"}
		}
		d, e := x509.CreateCertificate(rand.Reader, x, ca, p, key)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{d}, PrivateKey: k}
	}
	return certificates{server: mint(2, true), a: mint(3, false), b: mint(4, false), roots: pool, caPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func testConfig(t *testing.T) Config {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return Config{Directory: dir, Volume: "volume", LeaseTTL: time.Minute}
}

func testGateway(t *testing.T, cfg Config) *Gateway {
	t.Helper()
	g, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	return g
}

func testListener(t *testing.T, g *Gateway, certs certificates) (*Client, *Client, func()) {
	t.Helper()
	srv, err := g.Server(certs.server, certs.roots)
	if err != nil {
		t.Fatal(err)
	}
	srv.ErrorLog = log.New(io.Discard, "", 0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(ln, "", "") }()
	makeClient := func(cert tls.Certificate) *Client {
		c, e := NewClient("https://"+ln.Addr().String(), cert, certs.roots)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	a, b := makeClient(certs.a), makeClient(certs.b)
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		a.Close()
		b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}
	t.Cleanup(stop)
	return a, b, stop
}

func testUpload(l Lease, id string) Upload {
	data := []byte("exact remote custody\x00with binary payload")
	return Upload{Chunk: Chunk{Session: l.Session, Epoch: l.Epoch, ID: id, FileID: "file-incarnation-1", Offset: 64, Size: int64(len(data)), SHA256: hash(data)}, Data: data}
}

func requireHTTP(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), "HTTP "+code+":") {
		t.Fatalf("want HTTP %s, got %v", code, err)
	}
}

func TestTLSCustodyOwnershipAndImmutableReadback(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	g := testGateway(t, cfg)
	certs := testCertificates(t)
	a, b, _ := testListener(t, g, certs)
	l, err := a.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(l.Session, hash(certs.a.Certificate[0])+".") || !validSession(l.Session) {
		t.Fatalf("unbound session: %+v", l)
	}
	if _, err := a.Claim(ctx); err == nil {
		t.Fatal("same certificate created another live owner")
	}
	if _, err := b.Claim(ctx); err == nil {
		t.Fatal("second certificate claimed live volume")
	}
	renewed, err := a.Renew(ctx, l)
	if err != nil || renewed.Epoch != l.Epoch {
		t.Fatalf("renew: %+v %v", renewed, err)
	}
	_, err = b.Renew(ctx, l)
	requireHTTP(t, err, "403")
	wrongVolume := l
	wrongVolume.Volume = "other"
	_, err = a.Renew(ctx, wrongVolume)
	requireHTTP(t, err, "403")
	u := testUpload(l, "operation-1")
	r, err := a.Upload(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := a.Upload(ctx, u)
	if err != nil || duplicate != r {
		t.Fatalf("unstable duplicate: %+v %v", duplicate, err)
	}
	got, err := a.Readback(ctx, r)
	if err != nil || !bytes.Equal(got, u.Data) {
		t.Fatalf("readback: %q %v", got, err)
	}
	status, err := a.Status(ctx, u.Chunk)
	if err != nil || status != r {
		t.Fatalf("status: %+v %v", status, err)
	}
	tampered := r
	tampered.ReceiptID = strings.Repeat("0", 64)
	if _, err := a.Readback(ctx, tampered); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("forged commit identity accepted: %v", err)
	}
	_, err = b.Readback(ctx, r)
	requireHTTP(t, err, "403")
	_, err = b.Upload(ctx, u)
	requireHTTP(t, err, "403")
	changed := u
	changed.Chunk.Offset++
	_, err = a.Upload(ctx, changed)
	requireHTTP(t, err, "409")
	changed = u
	changed.Data = []byte("changed bytes")
	changed.Chunk.Size = int64(len(changed.Data))
	changed.Chunk.SHA256 = hash(changed.Data)
	_, err = a.Upload(ctx, changed)
	requireHTTP(t, err, "409")
	unknown := testCertificates(t)
	rogue, err := NewClient(a.base, unknown.a, certs.roots)
	if err != nil {
		t.Fatal(err)
	}
	defer rogue.Close()
	if _, err := rogue.Claim(ctx); err == nil {
		t.Fatal("unknown CA admitted")
	}
	plain := &http.Request{}
	if _, err := identity(plain); !errors.Is(err, ErrUnauthorized) {
		t.Fatal("plaintext identity admitted")
	}
	if _, err := NewClient("http://127.0.0.1", certs.a, certs.roots); err == nil {
		t.Fatal("plaintext client configured")
	}
	// A lookalike publication receipt is rejected by the custody decoder, and
	// custody has no media mutation route or row-retirement API.
	bad := r
	bad.Kind = "media-published-v1"
	if _, err := a.Readback(ctx, bad); !errors.Is(err, ErrIntegrity) {
		t.Fatal("receipt kind ignored")
	}
	_, err = a.call(ctx, http.MethodPost, "/v1/media/publish", r)
	requireHTTP(t, err, "404")
}

func TestExpiredLeaseAndSuccessorPreserveOldReadback(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	cfg.LeaseTTL = 500 * time.Millisecond
	g := testGateway(t, cfg)
	a, b, _ := testListener(t, g, testCertificates(t))
	l, err := a.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	u := testUpload(l, "before-expiry")
	r, err := a.Upload(ctx, u)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(time.Unix(0, l.ExpiresAtNS)) + time.Millisecond)
	if _, err := a.Renew(ctx, l); err == nil {
		t.Fatal("expired renewal advanced epoch")
	}
	_, err = a.Upload(ctx, testUpload(l, "expired-new"))
	requireHTTP(t, err, "409")
	dup, err := a.Upload(ctx, u)
	if err != nil || dup != r {
		t.Fatalf("expired exact retry: %+v %v", dup, err)
	}
	next, err := b.Claim(ctx)
	if err != nil || next.Epoch != l.Epoch+1 {
		t.Fatalf("successor: %+v %v", next, err)
	}
	_, err = a.Upload(ctx, u)
	requireHTTP(t, err, "409")
	if got, err := a.Readback(ctx, r); err != nil || !bytes.Equal(got, u.Data) {
		t.Fatalf("old custody lost: %q %v", got, err)
	}
	if got, err := a.Status(ctx, u.Chunk); err != nil || got != r {
		t.Fatalf("old status: %+v %v", got, err)
	}
}

func TestUploadRejectsInvalidDataBeforePrepared(t *testing.T) {
	ctx := context.Background()
	g := testGateway(t, testConfig(t))
	a, _, _ := testListener(t, g, testCertificates(t))
	l, err := a.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, which := range []string{"size", "hash", "path", "oversize", "trailing"} {
		t.Run(which, func(t *testing.T) {
			u := testUpload(l, which)
			switch which {
			case "size":
				u.Chunk.Size++
			case "hash":
				u.Chunk.SHA256 = strings.Repeat("0", 64)
			case "path":
				u.Chunk.FileID = "../media"
			case "oversize":
				u.Data = make([]byte, MaxChunkBytes+1)
				u.Chunk.Size = int64(len(u.Data))
				u.Chunk.SHA256 = hash(u.Data)
			}
			var err error
			if which == "trailing" {
				_, err = a.call(ctx, http.MethodPut, "/v1/custody/upload", map[string]any{"chunk": u.Chunk, "data": u.Data, "foreign": "ignored?"})
			} else {
				_, err = a.call(ctx, http.MethodPut, "/v1/custody/upload", u)
			}
			requireHTTP(t, err, "400")
			if _, err := g.journal.Inspect(ctx, g.volume, format+":"+which); !errors.Is(err, mutationauthority.ErrNoSuchOperation) {
				t.Fatalf("invalid input prepared: %v", err)
			}
		})
	}
}

func TestCommittedArtifactIntegrityAndNoFollow(t *testing.T) {
	for _, mode := range []string{"corrupt", "trailing", "missing", "symlink", "hardlink"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfig(t)
			g := testGateway(t, cfg)
			a, _, _ := testListener(t, g, testCertificates(t))
			l, err := a.Claim(ctx)
			if err != nil {
				t.Fatal(err)
			}
			u := testUpload(l, "integrity")
			r, err := a.Upload(ctx, u)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(cfg.Directory, g.leaf(format+":"+u.Chunk.ID))
			switch mode {
			case "trailing":
				f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.Write([]byte{0})
				if closeErr := f.Close(); err != nil || closeErr != nil {
					t.Fatalf("append: %v %v", err, closeErr)
				}
			case "corrupt":
				if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := path + "-original"
				if err := os.Rename(path, target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(path, path+"-alias"); err != nil {
					t.Fatal(err)
				}
			}
			_, err = a.Status(ctx, u.Chunk)
			requireHTTP(t, err, "409")
			_, err = a.Readback(ctx, r)
			requireHTTP(t, err, "409")
			_, err = a.Upload(ctx, u)
			requireHTTP(t, err, "409")
		})
	}
}

func TestExistingStageCannotBeOverwritten(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t)
	g := testGateway(t, cfg)
	a, _, _ := testListener(t, g, testCertificates(t))
	l, err := a.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	u := testUpload(l, "collision")
	path := filepath.Join(cfg.Directory, g.leaf(format+":"+u.Chunk.ID))
	before := []byte("existing custody")
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	_, err = a.Upload(ctx, u)
	requireHTTP(t, err, "409")
	got, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(got, before) {
		t.Fatalf("overwritten: %q %v", got, err)
	}
	op, err := g.journal.Inspect(ctx, g.volume, format+":"+u.Chunk.ID)
	if err != nil || op.State != mutationauthority.Prepared {
		t.Fatalf("lost uncertain row: %+v %v", op, err)
	}
}

func TestUnknownStateAndForeignPreparedAreRefused(t *testing.T) {
	t.Run("unknown database", func(t *testing.T) {
		cfg := testConfig(t)
		if err := os.WriteFile(filepath.Join(cfg.Directory, "authority.db"), nil, 0600); err != nil {
			t.Fatal(err)
		}
		if g, err := Open(cfg); err == nil {
			g.Close()
			t.Fatal("adopted unknown DB")
		}
	})
	t.Run("wrong volume", func(t *testing.T) {
		cfg := testConfig(t)
		g := testGateway(t, cfg)
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		cfg.Volume = "other"
		if g, err := Open(cfg); err == nil {
			g.Close()
			t.Fatal("adopted different volume")
		}
	})
	t.Run("foreign prepared", func(t *testing.T) {
		cfg := testConfig(t)
		g := testGateway(t, cfg)
		ctx := context.Background()
		l, err := g.journal.Claim(ctx, g.volume, strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, err = g.journal.Execute(ctx, mutationauthority.Request{Lease: l, ID: "media:foreign", Digest: hash([]byte("foreign")), ExpectedRevision: "media"}, errorBackend{})
		if !errors.Is(err, mutationauthority.ErrUncertain) {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if g, err := Open(cfg); err == nil {
			g.Close()
			t.Fatal("foreign operation reconciled")
		}
	})
	t.Run("foreign volume", func(t *testing.T) {
		cfg := testConfig(t)
		g := testGateway(t, cfg)
		ctx := context.Background()
		l, err := g.journal.Claim(ctx, "other-volume", strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		_, err = g.journal.Execute(ctx, mutationauthority.Request{Lease: l, ID: format + ":foreign", Digest: hash([]byte("foreign")), ExpectedRevision: format}, errorBackend{})
		if !errors.Is(err, mutationauthority.ErrUncertain) {
			t.Fatal(err)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if g, err := Open(cfg); err == nil {
			g.Close()
			t.Fatal("foreign volume hidden from reconciliation")
		}
	})
	for _, state := range []string{"committed", "no-effect"} {
		t.Run("foreign "+state, func(t *testing.T) {
			cfg := testConfig(t)
			g := testGateway(t, cfg)
			ctx := context.Background()
			l, err := g.journal.Claim(ctx, g.volume, strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			r := mutationauthority.Request{Lease: l, ID: "media:terminal", Digest: hash([]byte("foreign")), ExpectedRevision: "media"}
			_, err = g.journal.Execute(ctx, r, errorBackend{})
			if !errors.Is(err, mutationauthority.ErrUncertain) {
				t.Fatal(err)
			}
			if state == "committed" {
				_, err = g.journal.ResolveCommitted(ctx, r, mutationauthority.DurabilityProof{Revision: "fixture", Evidence: "fixture"})
			} else {
				err = g.journal.ResolveNoEffect(ctx, r, "fixture")
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if g, err := Open(cfg); err == nil {
				g.Close()
				t.Fatal("foreign terminal effect admitted")
			}
		})
	}
}

type errorBackend struct{}

func (errorBackend) Apply(context.Context, mutationauthority.Request) (mutationauthority.DurabilityProof, error) {
	return mutationauthority.DurabilityProof{}, errors.New("fixture uncertain")
}

func TestGatewayCrashHelper(t *testing.T) {
	if os.Getenv("JM_CUSTODY_CRASH_HELPER") != "1" {
		return
	}
	cfg := Config{Directory: os.Getenv("JM_CUSTODY_STATE"), Volume: "volume", LeaseTTL: time.Minute}
	g, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	l, err := g.journal.Claim(ctx, g.volume, strings.Repeat("a", 64)+"."+strings.Repeat("b", 64), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u := testUpload(wireLease(l), "crash")
	phase := os.Getenv("JM_CUSTODY_CRASH_PHASE")
	g.stageHook = func(at string) {
		if at == phase {
			os.Exit(77)
		}
	}
	if _, err := g.journal.Execute(ctx, g.request(u.Chunk), stageBackend{g: g, chunk: u.Chunk, data: u.Data}); err != nil {
		t.Fatal(err)
	}
	if phase == "committed" {
		os.Exit(77)
	}
	t.Fatal("crash phase not reached")
}

func TestProcessCrashReconciliation(t *testing.T) {
	for _, phase := range []string{"prepared", "partial", "written", "file-synced", "durable", "committed"} {
		t.Run(phase, func(t *testing.T) {
			cfg := testConfig(t)
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(exe, "-test.run=^TestGatewayCrashHelper$")
			cmd.Env = append(os.Environ(), "JM_CUSTODY_CRASH_HELPER=1", "JM_CUSTODY_STATE="+cfg.Directory, "JM_CUSTODY_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 77 {
				t.Fatalf("helper: %v %s", err, out)
			}
			g := testGateway(t, cfg)
			ctx := context.Background()
			op, err := g.journal.Inspect(ctx, g.volume, format+":crash")
			if err != nil {
				t.Fatal(err)
			}
			u := testUpload(wireLease(op.Lease), "crash")
			if phase == "prepared" || phase == "partial" {
				if op.State != mutationauthority.Prepared {
					t.Fatal(op.State)
				}
				if _, err := g.journal.Claim(ctx, g.volume, "successor", time.Minute); !errors.Is(err, mutationauthority.ErrUnsettled) {
					t.Fatalf("missing bytes admitted successor: %v", err)
				}
				if _, _, err := g.custody(ctx, u.Chunk); !errors.Is(err, mutationauthority.ErrUncertain) {
					t.Fatal(err)
				}
				return
			}
			if op.State != mutationauthority.Committed {
				t.Fatalf("valid artifact unreconciled: %s", op.State)
			}
			r, data, err := g.custody(ctx, u.Chunk)
			if err != nil || !bytes.Equal(data, u.Data) {
				t.Fatalf("reconciled custody: %+v %q %v", r, data, err)
			}
			if _, err := g.journal.Claim(ctx, g.volume, "successor", time.Minute); err != nil {
				t.Fatal(err)
			}
			r2, _, err := g.custody(ctx, u.Chunk)
			if err != nil || r2 != r {
				t.Fatalf("receipt changed: %+v %v", r2, err)
			}
		})
	}
}

func TestReceiptRejectsChangedArtifactIdentity(t *testing.T) {
	l := Lease{Volume: "volume", Session: strings.Repeat("a", 64) + "." + strings.Repeat("b", 64), Epoch: 1}
	u := testUpload(l, "receipt")
	header := artifactHeader{Kind: format, Volume: "volume", Chunk: u.Chunk}
	headerBytes, _ := json.Marshal(header)
	artifact := artifactBytes(header, u.Data)
	r := CustodyReceipt{Kind: format, Volume: "volume", Chunk: u.Chunk, ArtifactSHA256: hash(artifact), ArtifactSize: int64(len(artifact)), RequestSHA256: hash(headerBytes), ReceiptID: strings.Repeat("c", 64), CommittedAtUnixNS: 1}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := decodeReceipt(b, u.Chunk, nil); err != nil || got != r {
		t.Fatalf("invalid baseline receipt: %+v %v", got, err)
	}
	changed := u.Chunk
	changed.Offset++
	if _, err := decodeReceipt(b, changed, nil); !errors.Is(err, ErrIntegrity) {
		t.Fatal("changed receipt accepted")
	}
	for _, field := range []string{"request", "length", "commit", "kind"} {
		t.Run(field, func(t *testing.T) {
			bad := r
			switch field {
			case "request":
				bad.RequestSHA256 = strings.Repeat("0", 64)
			case "length":
				bad.ArtifactSize++
			case "commit":
				bad.ReceiptID = "invalid"
			case "kind":
				bad.Kind = "media"
			}
			b, _ := json.Marshal(bad)
			if _, err := decodeReceipt(b, u.Chunk, nil); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("changed %s accepted", field)
			}
		})
	}
}
