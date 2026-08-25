package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTLSConfigBlock(t *testing.T) {
	cases := []struct {
		cert, key, want string
	}{
		{"", "", ""},
		{"/data/tls/cert.pem", "", ""},
		{"", "/data/tls/key.pem", ""},
		{"/data/tls/fullchain.pem", "/data/tls/privkey.pem",
			"tls_cert_path: /data/tls/fullchain.pem\ntls_key_path: /data/tls/privkey.pem\n"},
	}
	for _, c := range cases {
		if got := tlsConfigBlock(c.cert, c.key); got != c.want {
			t.Errorf("tlsConfigBlock(%q,%q) = %q, want %q", c.cert, c.key, got, c.want)
		}
	}
}

// TestEnsureConfigIncludesTLS proves the env vars reach the GENERATED yaml:
// with both set, the TLS stanza lands in config.yaml; with either missing,
// it must not (a half-set stanza would crash headscale at boot).
func TestEnsureConfigIncludesTLS(t *testing.T) {
	oldDir, oldURL, oldCert, oldKey :=
		hsDataDir, os.Getenv("JM_NET_SERVER_URL"),
		os.Getenv("JM_NET_TLS_CERT"), os.Getenv("JM_NET_TLS_KEY")
	t.Cleanup(func() {
		hsDataDir = oldDir
		os.Setenv("JM_NET_SERVER_URL", oldURL)
		os.Setenv("JM_NET_TLS_CERT", oldCert)
		os.Setenv("JM_NET_TLS_KEY", oldKey)
	})

	for _, tc := range []struct {
		name        string
		cert, key   string
		wantPresent bool
	}{
		{"both set", "/data/tls/fullchain.pem", "/data/tls/privkey.pem", true},
		{"cert only", "/data/tls/fullchain.pem", "", false},
		{"neither set", "", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hsDataDir = t.TempDir()
			t.Setenv("JM_NET_SERVER_URL", "https://nas.example:30193")
			t.Setenv("JM_NET_TLS_CERT", tc.cert)
			t.Setenv("JM_NET_TLS_KEY", tc.key)

			cfgPath, err := ensureConfig()
			if err != nil {
				t.Fatalf("ensureConfig: %v", err)
			}
			raw, err := os.ReadFile(cfgPath)
			if err != nil {
				t.Fatal(err)
			}
			cfg := string(raw)
			got := strings.Contains(cfg, "tls_cert_path:") && strings.Contains(cfg, "tls_key_path:")
			if got != tc.wantPresent {
				t.Errorf("config has TLS stanza = %v, want %v:\n%s", got, tc.wantPresent, cfg)
			}
			// Invariant regardless of TLS: the coordination URL and sqlite
			// paths must survive untouched.
			if !strings.Contains(cfg, "server_url: https://nas.example:30193") {
				t.Errorf("server_url lost:\n%s", cfg)
			}
			if !strings.Contains(cfg, filepath.Join(hsDataDir, "db.sqlite")) {
				t.Errorf("sqlite path lost:\n%s", cfg)
			}
		})
	}
}

// T2.2 helpers: where the embedded NAS node dials coordination.
func TestNasNodeControlURL(t *testing.T) {
	oldURL, oldCert := os.Getenv("JM_NET_SERVER_URL"), os.Getenv("JM_NET_TLS_CERT")
	defer func() {
		os.Setenv("JM_NET_SERVER_URL", oldURL)
		os.Setenv("JM_NET_TLS_CERT", oldCert)
	}()

	os.Unsetenv("JM_NET_SERVER_URL")
	os.Unsetenv("JM_NET_TLS_CERT")
	url, err := nasNodeControlURL()
	if err != nil || url != "http://127.0.0.1:8091" {
		t.Errorf("loopback fallback = (%q,%v), want loopback listener", url, err)
	}

	// TLS on without an external URL must REFUSE: tsnet cannot verify a
	// self-referenced certificate against 127.0.0.1.
	os.Setenv("JM_NET_TLS_CERT", "/data/tls/c.pem")
	if _, err := nasNodeControlURL(); err == nil {
		t.Error("expected refusal when TLS configured but no external URL")
	}

	os.Setenv("JM_NET_SERVER_URL", "https://nas.example:30193")
	url, err = nasNodeControlURL()
	if err != nil || url != "https://nas.example:30193" {
		t.Errorf("external URL preferred under TLS = (%q,%v)", url, err)
	}
}
