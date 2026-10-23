package mutationgateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
)

func imageConfig(t *testing.T) Config {
	cfg := testConfig(t)
	cfg.Images = &ImageConfig{1, "ims_11111111111111111111111111111111", true, 8, 1 << 30, 1 << 20}
	return cfg
}

// F is a new leaf with the SAME key, CN and CA as B. Only exact DER hash
// distinguishes the two principals; neither inherits the other's grant.
func imageCertificates(t *testing.T, readerExpiry time.Time) (certificates, tls.Certificate) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "image-test-ca"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	readerPublic, readerKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	mint := func(serial int64, server, reuse bool, expiry time.Time) tls.Certificate {
		p, k := readerPublic, readerKey
		if !reuse {
			var e error
			p, k, e = ed25519.GenerateKey(rand.Reader)
			if e != nil {
				t.Fatal(e)
			}
		}
		x := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "same-name"}, NotBefore: now.Add(-time.Minute), NotAfter: expiry, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
		if server {
			x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
			x.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
		}
		d, e := x509.CreateCertificate(rand.Reader, x, ca, p, key)
		if e != nil {
			t.Fatal(e)
		}
		return tls.Certificate{Certificate: [][]byte{d}, PrivateKey: k}
	}
	if readerExpiry.IsZero() {
		readerExpiry = now.Add(time.Hour)
	}
	return certificates{server: mint(2, true, false, now.Add(time.Hour)), a: mint(3, false, false, now.Add(time.Hour)), b: mint(4, false, true, readerExpiry), roots: roots}, mint(5, false, true, now.Add(time.Hour))
}
func fixtureImage(t *testing.T, c *Client, cfg Config, certs certificates) (ImageSeal, []byte) {
	t.Helper()
	ctx := context.Background()
	lease, err := c.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("first retained bytes\x00"), bytes.Repeat([]byte("middle-and-end"), 1024)}
	var complete []byte
	receipts := make([]ImageCustodyReceipt, 0, len(parts))
	for i, data := range parts {
		id := "chunk-a"
		if i == 1 {
			id = "chunk-b"
		}
		sum := sha256.Sum256(data)
		u := Upload{Chunk{lease.Session, lease.Epoch, id, "incarnation-1", int64(len(complete)), int64(len(data)), hex.EncodeToString(sum[:])}, data}
		r, err := c.Upload(ctx, u)
		if err != nil {
			t.Fatal(err)
		}
		receipts = append(receipts, ImageCustody(r))
		complete = append(complete, data...)
	}
	sum := sha256.Sum256(complete)
	seal := ImageSeal{1, cfg.Images.StoreID, cfg.Volume, "lib_11111111111111111111111111111111", "img_11111111111111111111111111111111", "iop_11111111111111111111111111111111", "incarnation-1", hash(certs.a.Certificate[0]), hash(certs.b.Certificate[0]), ImageContent{int64(len(complete)), hex.EncodeToString(sum[:])}, receipts}
	return seal, complete
}
func emptyGatewayImage(cfg Config, producer, reader string) ImageSeal {
	return ImageSeal{1, cfg.Images.StoreID, cfg.Volume, "lib_11111111111111111111111111111111", "img_11111111111111111111111111111111", "iop_11111111111111111111111111111111", "empty-1", producer, reader, ImageContent{0, hash(nil)}, []ImageCustodyReceipt{}}
}
func imageThirdClient(t *testing.T, a *Client, cert tls.Certificate, roots *x509.CertPool) *Client {
	t.Helper()
	c, err := NewClient(a.base, cert, roots)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestImageMTLSCompleteBytesRebindRevokeAndRestart(t *testing.T) {
	ctx := context.Background()
	cfg := imageConfig(t)
	g := testGateway(t, cfg)
	certs, newLeaf := imageCertificates(t, time.Time{})
	a, b, stop := testListener(t, g, certs)
	f := imageThirdClient(t, a, newLeaf, certs.roots)
	seal, expected := fixtureImage(t, a, cfg, certs)
	ref := imageRef(seal)
	receipt, err := a.SealImage(ctx, seal)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := a.SealImage(ctx, seal)
	if err != nil || retry != receipt {
		t.Fatal("seal retry changed", retry, err)
	}
	changed := seal
	changed.InitialReader = hash(newLeaf.Certificate[0])
	_, err = a.SealImage(ctx, changed)
	requireHTTP(t, err, "409")
	_, err = b.SealImage(ctx, seal)
	requireHTTP(t, err, "403")
	status, err := a.ImageSealStatus(ctx, seal)
	if err != nil || status.State != "SEALED" || status.Receipt == nil || *status.Receipt != receipt {
		t.Fatal(status, err)
	}
	_, err = b.ImageSealStatus(ctx, seal)
	requireHTTP(t, err, "403")
	_, err = a.ImageStatus(ctx, ref)
	requireHTTP(t, err, "403")
	_, err = f.ImageStatus(ctx, ref)
	requireHTTP(t, err, "403")
	view, err := b.ImageStatus(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	forged := view
	forged.Content.SHA256 = strings.Repeat("f", 64)
	if _, err := b.ImageReadback(ctx, forged, 0); !errors.Is(err, ErrIntegrity) {
		t.Fatal("shape-valid forged view bypassed online equality", err)
	}
	var actual []byte
	for i := range view.Chunks {
		p, err := b.ImageReadback(ctx, view, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, p...)
	}
	if !bytes.Equal(actual, expected) || sha256.Sum256(actual) != sha256.Sum256(expected) {
		t.Fatal("complete retained image bytes changed")
	}
	raw := seal.Receipts[0]
	_, err = b.call(ctx, http.MethodPost, "/v1/custody/status", imageChunk(raw.Chunk))
	requireHTTP(t, err, "403")
	_, err = b.call(ctx, http.MethodPost, "/v1/custody/readback", imageChunk(raw.Chunk))
	requireHTTP(t, err, "403")
	_, err = f.call(ctx, http.MethodPost, "/v1/custody/status", imageChunk(raw.Chunk))
	requireHTTP(t, err, "403")
	newReader := hash(newLeaf.Certificate[0])
	change := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_11111111111111111111111111111111", 1, &newReader}
	_, err = b.RebindImageReader(ctx, change)
	requireHTTP(t, err, "403")
	granted, err := a.RebindImageReader(ctx, change)
	if err != nil || granted.Generation != 2 {
		t.Fatal(granted, err)
	}
	changedGrant := change
	changedGrant.Reader = nil
	_, err = a.RebindImageReader(ctx, changedGrant)
	requireHTTP(t, err, "409")
	_, err = b.ImageStatus(ctx, ref)
	requireHTTP(t, err, "403")
	_, err = b.ImageReadback(ctx, view, 0)
	requireHTTP(t, err, "403")
	newView, err := f.ImageStatus(ctx, ref)
	if err != nil || newView.ImageDigest != view.ImageDigest || newView.Generation != 2 {
		t.Fatal(newView, err)
	}
	if _, err = f.ImageReadback(ctx, newView, 1); err != nil {
		t.Fatal(err)
	}
	stale := change
	stale.OperationID = "irg_22222222222222222222222222222222"
	_, err = a.RebindImageReader(ctx, stale)
	requireHTTP(t, err, "409")
	revoke := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_33333333333333333333333333333333", 2, nil}
	revoked, err := a.RebindImageReader(ctx, revoke)
	if err != nil || revoked.Generation != 3 {
		t.Fatal(revoked, err)
	}
	if old, err := a.RebindImageReader(ctx, change); err != nil || !bytes.Equal(imageJSON(old), imageJSON(granted)) {
		t.Fatal("historical rebind retry changed", err)
	}
	if again, err := a.SealImage(ctx, seal); err != nil || again != receipt {
		t.Fatal("historical seal retry changed", err)
	}
	_, err = f.ImageStatus(ctx, ref)
	requireHTTP(t, err, "403")
	if got, err := a.Status(ctx, imageChunk(raw.Chunk)); err != nil || ImageCustody(got) != raw {
		t.Fatal("producer lost raw custody", err)
	}
	stop()
	if err = g.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Images.Initialize = false
	reopened := testGateway(t, cfg)
	a2, b2, _ := testListener(t, reopened, certs)
	_, err = b2.ImageStatus(ctx, ref)
	requireHTTP(t, err, "403")
	post, err := a2.ImageSealStatus(ctx, seal)
	if err != nil || *post.Receipt != receipt {
		t.Fatal(post, err)
	}
	reader := hash(certs.b.Certificate[0])
	restore := ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_44444444444444444444444444444444", 3, &reader}
	if _, err = a2.RebindImageReader(ctx, restore); err != nil {
		t.Fatal(err)
	}
	final, err := b2.ImageStatus(ctx, ref)
	if err != nil || final.Generation != 4 || final.ImageDigest != receipt.ImageDigest {
		t.Fatal(final, err)
	}
	got, err := b2.ImageReadback(ctx, final, 1)
	if err != nil || !bytes.Equal(got, expected[seal.Receipts[1].Chunk.Offset:]) {
		t.Fatal("restart bytes changed", err)
	}
}

func TestImageSealRejectsUnverifiedAndMalformedCohorts(t *testing.T) {
	for _, mode := range []string{"full-hash", "missing", "corrupt", "receipt", "gap", "overlap", "duplicate", "mixed-session", "mixed-epoch", "mixed-file", "mixed-volume", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			cfg := imageConfig(t)
			g := testGateway(t, cfg)
			certs, _ := imageCertificates(t, time.Time{})
			a, b, _ := testListener(t, g, certs)
			seal, _ := fixtureImage(t, a, cfg, certs)
			seal.Receipts = append([]ImageCustodyReceipt{}, seal.Receipts...)
			switch mode {
			case "full-hash":
				seal.Content.SHA256 = strings.Repeat("f", 64)
			case "missing", "corrupt":
				path := filepath.Join(cfg.Directory, g.leaf(format+":"+seal.Receipts[1].Chunk.ID))
				var err error
				if mode == "missing" {
					err = os.Remove(path)
				} else {
					err = os.WriteFile(path, []byte("corrupt"), 0600)
				}
				if err != nil {
					t.Fatal(err)
				}
			case "receipt":
				seal.Receipts[1].ReceiptID = strings.Repeat("f", 64)
			case "gap":
				seal.Receipts[1].Chunk.Offset++
			case "overlap":
				seal.Receipts[1].Chunk.Offset--
			case "duplicate":
				seal.Receipts[1] = seal.Receipts[0]
			case "mixed-session":
				seal.Receipts[1].Chunk.Session = seal.Producer + "." + strings.Repeat("f", 64)
			case "mixed-epoch":
				seal.Receipts[1].Chunk.Epoch++
			case "mixed-file":
				seal.Receipts[1].Chunk.FileID = "different"
			case "mixed-volume":
				seal.Receipts[1].Volume = "different"
			case "incomplete":
				seal.Receipts = seal.Receipts[:1]
			}
			// Send raw canonical JSON so client-side syntax checks cannot supply the
			// server refusal this test is intended to establish.
			_, err := a.call(ctx, http.MethodPost, "/v1/images/seal", seal)
			if mode == "full-hash" || mode == "missing" || mode == "corrupt" {
				requireHTTP(t, err, "409")
			} else {
				requireHTTP(t, err, "400")
			}
			_, err = b.ImageStatus(ctx, imageRef(seal))
			requireHTTP(t, err, "403")
			if mode == "full-hash" || mode == "missing" || mode == "corrupt" {
				state, err := a.ImageSealStatus(ctx, seal)
				if err != nil || state.State != "RESERVED" || state.Receipt != nil {
					t.Fatal("unverified image sealed", state, err)
				}
			}
		})
	}
}

func TestImageReadRechecksGrantBeforeDisclosureAndJoins(t *testing.T) {
	ctx := context.Background()
	cfg := imageConfig(t)
	g := testGateway(t, cfg)
	certs, _ := imageCertificates(t, time.Time{})
	a, b, _ := testListener(t, g, certs)
	seal, _ := fixtureImage(t, a, cfg, certs)
	if _, err := a.SealImage(ctx, seal); err != nil {
		t.Fatal(err)
	}
	view, err := b.ImageStatus(ctx, imageRef(seal))
	if err != nil {
		t.Fatal(err)
	}
	admitted, release := make(chan struct{}), make(chan struct{})
	g.imageHook = func(at string) {
		if at == "read-verified" {
			close(admitted)
			<-release
		}
	}
	done := make(chan error, 1)
	released, joined := false, false
	var readErr error
	releaseAndJoin := func() error {
		if !released {
			close(release)
			released = true
		}
		if !joined {
			readErr = <-done
			joined = true
		}
		return readErr
	}
	go func() { _, e := b.ImageReadback(ctx, view, 0); done <- e }()
	t.Cleanup(func() { releaseAndJoin() })
	select {
	case <-admitted:
	case <-time.After(5 * time.Second):
		t.Fatal("read did not reach verified boundary")
	}
	revoked, err := a.RebindImageReader(ctx, ImageReaderChange{1, seal.StoreID, seal.ImageID, "irg_11111111111111111111111111111111", 1, nil})
	if err != nil || revoked.Generation != 2 {
		t.Fatal(err)
	}
	if err = g.Close(); !errors.Is(err, mutationauthority.ErrInFlight) {
		t.Fatal("admitted read was not retained", err)
	}
	requireHTTP(t, releaseAndJoin(), "403")
}

func TestImageCertificateExpiryOnReusedTLSConnection(t *testing.T) {
	expiry := time.Now().Add(2500 * time.Millisecond).Truncate(time.Second)
	cfg := imageConfig(t)
	g := testGateway(t, cfg)
	certs, _ := imageCertificates(t, expiry)
	a, b, _ := testListener(t, g, certs)
	ctx := context.Background()
	seal := emptyGatewayImage(cfg, hash(certs.a.Certificate[0]), hash(certs.b.Certificate[0]))
	if _, err := a.SealImage(ctx, seal); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ImageStatus(ctx, imageRef(seal)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(expiry) + 20*time.Millisecond)
	reused := false
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused }}
	_, err := b.ImageStatus(httptrace.WithClientTrace(ctx, trace), imageRef(seal))
	requireHTTP(t, err, "403")
	if !reused {
		t.Fatal("expiry check did not exercise an established TLS connection")
	}
}

func TestImageReservationCancellationRetryAndQuota(t *testing.T) {
	ctx := context.Background()
	cfg := imageConfig(t)
	cfg.Images.MaxImages = 1
	g := testGateway(t, cfg)
	certs, _ := imageCertificates(t, time.Time{})
	a, _, stop := testListener(t, g, certs)
	seal, expected := fixtureImage(t, a, cfg, certs)
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g.imageHook = func(at string) {
		if at == "reserved" {
			cancel()
		}
	}
	// A trusted verifier cancellation at the durable reservation boundary is
	// deterministic; all custody creation and subsequent resumption use real TLS.
	if _, err := g.sealImage(cancelCtx, seal.Producer, seal, func() error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled verifier advanced reservation", err)
	}
	// Restart after the stopped verifier; no reservation is replayed by Open.
	stop()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Images.Initialize = false
	reopened := testGateway(t, cfg)
	a2, b2, _ := testListener(t, reopened, certs)
	state, err := a2.ImageSealStatus(ctx, seal)
	if err != nil || state.State != "RESERVED" {
		t.Fatal(state, err)
	}
	empty := emptyGatewayImage(cfg, seal.Producer, seal.InitialReader)
	empty.ImageID = "img_22222222222222222222222222222222"
	empty.OperationID = "iop_22222222222222222222222222222222"
	_, err = a2.SealImage(ctx, empty)
	requireHTTP(t, err, "409")
	if _, err = a2.SealImage(ctx, seal); err != nil {
		t.Fatal("exact reservation retry failed", err)
	}
	view, err := b2.ImageStatus(ctx, imageRef(seal))
	if err != nil {
		t.Fatal(err)
	}
	data, err := b2.ImageReadback(ctx, view, 0)
	if err != nil || !bytes.Equal(data, expected[:seal.Receipts[0].Chunk.Size]) {
		t.Fatal(err)
	}
}

func TestImageEmptySealNeedsNoClaimAndCountsQuota(t *testing.T) {
	ctx := context.Background()
	cfg := imageConfig(t)
	cfg.Images.MaxImages = 1
	cfg.Images.MaxLogicalBytes = 0
	g := testGateway(t, cfg)
	certs, _ := imageCertificates(t, time.Time{})
	a, b, _ := testListener(t, g, certs)
	seal := emptyGatewayImage(cfg, hash(certs.a.Certificate[0]), hash(certs.b.Certificate[0]))
	if _, err := a.SealImage(ctx, seal); err != nil {
		t.Fatal(err)
	}
	view, err := b.ImageStatus(ctx, imageRef(seal))
	if err != nil || view.Content.Size != 0 || len(view.Chunks) != 0 {
		t.Fatal(view, err)
	}
	if _, err := a.SealImage(ctx, seal); err != nil {
		t.Fatal("empty exact retry charged", err)
	}
	other := seal
	other.ImageID = "img_22222222222222222222222222222222"
	other.OperationID = "iop_22222222222222222222222222222222"
	other.Incarnation = "empty-2"
	_, err = a.SealImage(ctx, other)
	requireHTTP(t, err, "409")
	missing := imageRef(seal)
	missing.ImageID = other.ImageID
	_, err = b.ImageStatus(ctx, missing)
	requireHTTP(t, err, "403")
	// An empty seal never needs a fabricated owner row or epoch.
	if _, err := a.Claim(ctx); err != nil {
		t.Fatal("image seal claimed a volume lease", err)
	}
}

func TestImageStartupConfigAndInactiveStore(t *testing.T) {
	for _, mode := range []string{"omitted", "identity", "quota", "inactive"} {
		t.Run(mode, func(t *testing.T) {
			cfg := imageConfig(t)
			g := testGateway(t, cfg)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "omitted":
				cfg.Images = nil
			case "identity":
				cfg.Images.StoreID = "ims_22222222222222222222222222222222"
			case "quota":
				cfg.Images.MaxImages--
			case "inactive":
				db, err := sql.Open("sqlite", filepath.Join(cfg.Directory, "authority.db"))
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec(`UPDATE image_store SET active=0`)
				closeErr := db.Close()
				if err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
				}
			}
			opened, err := Open(cfg)
			if err == nil {
				opened.Close()
				t.Fatal("mismatched/inactive image authority admitted")
			}
		})
	}
}

func TestImagesConfigPrivateCanonicalAndNoFollow(t *testing.T) {
	cfg := imageConfig(t)
	path := filepath.Join(t.TempDir(), "images.json")
	data, _ := json.Marshal(cfg.Images)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if read, err := ReadImagesConfig(path); err != nil || *read != *cfg.Images {
		t.Fatal(read, err)
	}
	for _, mode := range []string{"public", "symlink", "hardlink", "duplicate", "unknown", "trailing"} {
		t.Run(mode, func(t *testing.T) {
			leaf := filepath.Join(t.TempDir(), "images.json")
			b := append([]byte{}, data...)
			switch mode {
			case "duplicate":
				b = bytes.Replace(b, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "unknown":
				b = append([]byte(`{"foreign":1,`), b[1:]...)
			case "trailing":
				b = append(b, '\n')
			}
			if mode == "symlink" {
				if err := os.Symlink(path, leaf); err != nil {
					t.Fatal(err)
				}
			} else if mode == "hardlink" {
				if err := os.Link(path, leaf); err != nil {
					t.Fatal(err)
				}
			} else {
				modeBits := os.FileMode(0600)
				if mode == "public" {
					modeBits = 0644
				}
				if err := os.WriteFile(leaf, b, modeBits); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(leaf, modeBits); err != nil {
					t.Fatal(err)
				}
				info, err := os.Lstat(leaf)
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode() != modeBits {
					t.Fatalf("fixture mode = %v, want %v", info.Mode(), modeBits)
				}
			}
			if _, err := ReadImagesConfig(leaf); err == nil {
				t.Fatal("unsafe config admitted", mode)
			}
			if mode == "hardlink" {
				if err := os.Remove(leaf); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
