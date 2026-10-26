package nativeclient

import (
	"errors"
	"strings"
	"testing"
)

func TestNativeEndpointCanonicalAuthority(t *testing.T) {
	maxHost := strings.Join([]string{strings.Repeat("a", 63), strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61)}, ".")
	maximum := "https://" + maxHost + ":65535/manager"
	if len(maximum) != 275 {
		t.Fatalf("maximum endpoint recipe length: %d", len(maximum))
	}
	for _, test := range []struct {
		input                string
		allowHTTP            bool
		scheme, host, prefix string
		port                 uint16
	}{
		{"https://example.test", false, "https", "example.test", "", 443},
		{"https://xn--bcher-kva.example/manager", false, "https", "xn--bcher-kva.example", "/manager", 443},
		{"https://127.0.0.1:8443/manager", false, "https", "127.0.0.1", "/manager", 8443},
		{"https://[::1]/manager", false, "https", "::1", "/manager", 443},
		{maximum, false, "https", maxHost, "/manager", 65535},
		{"http://127.0.0.1:8080", true, "http", "127.0.0.1", "", 8080},
		{"http://[::1]/manager", true, "http", "::1", "/manager", 80},
	} {
		got, err := ParseEndpoint(test.input, test.allowHTTP)
		if err != nil || got.URL != test.input || got.Scheme != test.scheme || got.Host != test.host || got.Port != test.port || got.Prefix != test.prefix {
			t.Fatalf("canonical endpoint %q: %+v %v", test.input, got, err)
		}
	}
	for _, input := range []string{
		"", "https://example.test/", "https://example.test/other", "https://example.test/../manager", "https://example.test/%6danager",
		"https://user@example.test", "https://example.test?x=1", "https://example.test#fragment", "https://example.test?",
		"https://example.test:443", "https://example.test:0", "https://example.test:65536", "https://example.test:08080", "https://example.test:",
		"https://EXAMPLE.test", "https://example.test.", "https://bücher.example", "https://-bad.example", "https://bad-.example",
		"https://" + strings.Repeat("a", 64) + ".test", maximum + "x", "https://127.0.0.01", "https://[fe80::1%25en0]",
		"http://127.0.0.1", "http://example.test", "http://192.0.2.1", "ftp://example.test", "HTTPS://example.test",
	} {
		if _, err := ParseEndpoint(input, false); !errors.Is(err, ErrInvalidEndpoint) {
			t.Fatalf("invalid endpoint admitted without HTTP opt-in: %q: %v", input, err)
		}
	}
	for _, input := range []string{"http://example.test", "http://192.0.2.1", "http://127.0.0.01", "http://[fe80::1]"} {
		if _, err := ParseEndpoint(input, true); !errors.Is(err, ErrInvalidEndpoint) {
			t.Fatalf("noncanonical or nonloopback HTTP admitted: %q: %v", input, err)
		}
	}
}

func TestNativeBasenameBoundsAndEscaping(t *testing.T) {
	for _, name := range []string{"photo.jpg", "video clip.mp4", strings.Repeat("&", 255), strings.Repeat("é", 127) + "a"} {
		if err := ValidateBasename(name); err != nil {
			t.Fatalf("safe basename %q: %v", name, err)
		}
	}
	for _, name := range []string{"", ".", "..", ".hidden", "a/b", "a\\b", "a:b", "a\x00b", "a\nb", strings.Repeat("a", 256), strings.Repeat("é", 128), string([]byte{0xff})} {
		if err := ValidateBasename(name); !errors.Is(err, ErrInvalidBasename) {
			t.Fatalf("unsafe basename admitted: %q: %v", name, err)
		}
	}
}
