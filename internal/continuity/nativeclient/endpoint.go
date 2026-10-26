package nativeclient

import (
	"errors"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/idna"
)

var ErrInvalidEndpoint = errors.New("invalid native delivery endpoint")
var ErrInvalidBasename = errors.New("invalid native delivery basename")

// Endpoint is a canonical configured authority, not a request-derived or
// forwarded-header authority. Its URL has no credential, query, or fragment.
type Endpoint struct {
	URL    string
	Scheme string
	Host   string
	Port   uint16
	Prefix string
}

// ParseEndpoint requires HTTPS under normal OS trust. HTTP is admitted only
// with an explicit caller opt-in and a canonical numeric loopback literal.
func ParseEndpoint(raw string, allowLoopbackHTTP bool) (Endpoint, error) {
	if len(raw) == 0 || len(raw) > 275 {
		return Endpoint{}, ErrInvalidEndpoint
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x21 || raw[i] > 0x7e || raw[i] == '%' || raw[i] == '\\' {
			return Endpoint{}, ErrInvalidEndpoint
		}
	}
	if !strings.HasPrefix(raw, "https://") && !strings.HasPrefix(raw, "http://") {
		return Endpoint{}, ErrInvalidEndpoint
	}
	u, err := url.Parse(raw)
	if err != nil || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" || u.String() != raw || (u.Path != "" && u.Path != "/manager") {
		return Endpoint{}, ErrInvalidEndpoint
	}
	host := u.Hostname()
	if host == "" || strings.HasSuffix(host, ".") || strings.Contains(host, ":") && !strings.HasPrefix(u.Host, "[") {
		return Endpoint{}, ErrInvalidEndpoint
	}
	port := uint16(443)
	if u.Scheme == "http" {
		port = 80
	}
	if text := u.Port(); text != "" {
		if len(text) > 5 || text[0] == '0' {
			return Endpoint{}, ErrInvalidEndpoint
		}
		value, err := strconv.Atoi(text)
		if err != nil || value < 1 || value > 65535 || value == int(port) {
			return Endpoint{}, ErrInvalidEndpoint
		}
		port = uint16(value)
	} else if strings.HasSuffix(u.Host, ":") {
		return Endpoint{}, ErrInvalidEndpoint
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if ip.Zone() != "" || ip.String() != host || (ip.Is6() && !strings.HasPrefix(u.Host, "[")) || (ip.Is4() && strings.HasPrefix(u.Host, "[")) {
			return Endpoint{}, ErrInvalidEndpoint
		}
		if u.Scheme == "http" && (!allowLoopbackHTTP || !ip.IsLoopback()) {
			return Endpoint{}, ErrInvalidEndpoint
		}
	} else {
		if u.Scheme != "https" || strings.Contains(host, ":") || !canonicalDNS(host) {
			return Endpoint{}, ErrInvalidEndpoint
		}
	}
	return Endpoint{URL: raw, Scheme: u.Scheme, Host: host, Port: port, Prefix: u.Path}, nil
}

func canonicalDNS(host string) bool {
	if len(host) == 0 || len(host) > 253 || host != strings.ToLower(host) {
		return false
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil || ascii != host {
		return false
	}
	allNumericDots := true
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || !dnsAlnum(label[0]) || !dnsAlnum(label[len(label)-1]) {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !dnsAlnum(c) && c != '-' {
				return false
			}
			if c < '0' || c > '9' {
				allNumericDots = false
			}
		}
	}
	return !allNumericDots
}

func dnsAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

// ValidateBasename accepts a single display/copy leaf, including an approved
// extension. It does not infer one or turn the name into an authority to copy.
func ValidateBasename(name string) error {
	if name == "" || len(name) > 255 || !utf8.ValidString(name) || strings.HasPrefix(name, ".") || strings.ContainsAny(name, "/\\:") {
		return ErrInvalidBasename
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return ErrInvalidBasename
		}
	}
	return nil
}
