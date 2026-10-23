package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lelanddutcher/juicemount/internal/mutationauthority"
	"github.com/lelanddutcher/juicemount/internal/mutationgateway"
)

func TestGatewayRequiresExplicitTLSConfiguration(t *testing.T) {
	for _, args := range [][]string{nil, {"--unknown"}, {"unexpected"}, {"--state", "missing", "--volume", "media"}} {
		if err := run(args, io.Discard); err == nil {
			t.Fatalf("incomplete configuration accepted: %v", args)
		}
	}
}

func TestGatewayImagesFlagReachesPersistentStoreIdentityCheck(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := mutationgateway.Config{Directory: state, Volume: "volume", LeaseTTL: time.Minute, Images: &mutationgateway.ImageConfig{Version: 1, StoreID: "ims_11111111111111111111111111111111", Initialize: true, MaxImages: 1, MaxLogicalBytes: 0, MaxMetadataBytes: 1 << 20}}
	g, err := mutationgateway.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Close(); err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, configPath := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "images.json")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Images.StoreID = "ims_22222222222222222222222222222222"
	b, err := json.Marshal(cfg.Images)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(configPath, b, 0600); err != nil {
		t.Fatal(err)
	}
	// If identity admission regresses, address parsing must fail before any
	// listener can start, so the assertion still reports the distinct error.
	err = run([]string{"--state", state, "--volume", "volume", "--tls-cert", certPath, "--tls-key", keyPath, "--client-ca", certPath, "--images-config", configPath, "--listen", "image-test-invalid-listen-address"}, io.Discard)
	if !errors.Is(err, mutationauthority.ErrInvalid) {
		t.Fatal("images flag did not enforce persistent identity before listen", err)
	}
}
