//go:build !noetcd
// +build !noetcd

package object

// These tests use in-memory TLS connections, never an etcd server or a listener.
import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type etcdTLSFixture struct {
	ca, cert, key string
	certificate   tls.Certificate
	roots         *x509.CertPool
}

func newEtcdTLSFixture(t *testing.T) etcdTLSFixture {
	t.Helper()
	dir := t.TempDir()
	rootPublic, rootKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	root := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "etcd fixture CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, rootPublic, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), DNSNames: []string{"etcd.fixture.invalid"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, root, public, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	f := etcdTLSFixture{
		ca: filepath.Join(dir, "ca.pem"), cert: filepath.Join(dir, "cert.pem"),
		key: filepath.Join(dir, "key.pem"), roots: x509.NewCertPool(),
	}
	for path, data := range map[string][]byte{f.ca: caPEM, f.cert: certPEM, f.key: keyPEM} {
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if !f.roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("fixture CA could not be loaded")
	}
	f.certificate, err = tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func etcdTLSHandshake(clientConfig, serverConfig *tls.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	done := make(chan error, 1)
	go func() { done <- tls.Server(serverConn, serverConfig).HandshakeContext(ctx) }()
	clientErr := tls.Client(clientConn, clientConfig).HandshakeContext(ctx)
	if clientErr != nil {
		_ = clientConn.Close()
	}
	serverErr := <-done
	if clientErr != nil {
		return clientErr
	}
	return serverErr
}

func TestEtcdTLSConfigTrustHostnameAndClientIdentity(t *testing.T) {
	f, foreign := newEtcdTLSFixture(t), newEtcdTLSFixture(t)
	for _, tc := range []struct {
		name, ca, serverName string
		clientIdentity       bool
		requireClient        bool
		wantError            bool
	}{
		{"trusted CA", f.ca, "etcd.fixture.invalid", false, false, false},
		{"wrong hostname", f.ca, "other.fixture.invalid", false, false, true},
		{"untrusted CA", foreign.ca, "etcd.fixture.invalid", false, false, true},
		{"mutual TLS", f.ca, "etcd.fixture.invalid", true, true, false},
		{"missing client identity", f.ca, "etcd.fixture.invalid", false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := url.Values{"cacert": {tc.ca}, "server-name": {tc.serverName}}
			if tc.clientIdentity {
				q.Set("cert", f.cert)
				q.Set("key", f.key)
			}
			cfg, err := buildTlsConfig(&url.URL{RawQuery: q.Encode()})
			if err != nil || cfg == nil {
				t.Fatalf("TLS configuration: config=%v err=%v", cfg, err)
			}
			if cfg.InsecureSkipVerify {
				t.Fatal("TLS verification was disabled")
			}
			server := &tls.Config{
				Certificates: []tls.Certificate{f.certificate}, MinVersion: tls.VersionTLS12,
				SessionTicketsDisabled: true,
			}
			if tc.requireClient {
				server.ClientAuth, server.ClientCAs = tls.RequireAndVerifyClientCert, f.roots
			}
			if err := etcdTLSHandshake(cfg, server); (err != nil) != tc.wantError {
				t.Fatalf("TLS handshake error=%v, wantError=%t", err, tc.wantError)
			}
		})
	}
}

func TestEtcdTLSConfigInvalidFilesAndOptionalSettings(t *testing.T) {
	f, foreign := newEtcdTLSFixture(t), newEtcdTLSFixture(t)
	for name, q := range map[string]url.Values{
		"missing CA":          {"cacert": {filepath.Join(t.TempDir(), "missing.pem")}},
		"invalid CA":          {"cacert": {f.key}},
		"cert without key":    {"cert": {f.cert}},
		"key without cert":    {"key": {f.key}},
		"mismatched key":      {"cert": {f.cert}, "key": {foreign.key}},
		"invalid certificate": {"cert": {f.ca}, "key": {f.key}},
	} {
		t.Run(name, func(t *testing.T) {
			if cfg, err := buildTlsConfig(&url.URL{RawQuery: q.Encode()}); err == nil {
				t.Fatalf("invalid TLS input accepted: %v", cfg)
			}
		})
	}
	// Preserve the existing activation rule: the skip flag alone does not
	// activate TLS; a named server or certificate setting does.
	for _, q := range []url.Values{nil, {"insecure-skip-verify": {"1"}}} {
		if cfg, err := buildTlsConfig(&url.URL{RawQuery: q.Encode()}); err != nil || cfg != nil {
			t.Fatalf("empty TLS settings changed: config=%v err=%v", cfg, err)
		}
	}
	for _, skip := range []string{"", "1"} {
		q := url.Values{"server-name": {"etcd.fixture.invalid"}, "insecure-skip-verify": {skip}}
		cfg, err := buildTlsConfig(&url.URL{RawQuery: q.Encode()})
		if err != nil || cfg == nil {
			t.Fatalf("server-only TLS configuration: config=%v err=%v", cfg, err)
		}
		if cfg.ServerName != "etcd.fixture.invalid" || cfg.InsecureSkipVerify != (skip != "") {
			t.Fatalf("explicit TLS settings changed: server=%q skip=%t", cfg.ServerName, cfg.InsecureSkipVerify)
		}
	}
}
