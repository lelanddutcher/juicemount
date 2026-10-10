// Package linkconfig owns the deployment contract for the JuiceMount Link
// coordination URL.  A linked Mac must be able to reach this URL before its
// encrypted data-plane identity can come online, including after a cold app
// launch away from the NAS LAN.
package linkconfig

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

var carrierGradeNAT = func() *net.IPNet {
	_, block, _ := net.ParseCIDR("100.64.0.0/10")
	return block
}()

// ParseServerURL validates the URL shape shared by the embedded Headscale
// supervisor, Manager status endpoint, and pairing endpoint.
func ParseServerURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("JM_NET_SERVER_URL not set (use the public HTTPS address Macs can reach away from the NAS)")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, fmt.Errorf("JM_NET_SERVER_URL must be an absolute http or https URL")
	}
	if u.User != nil {
		return nil, fmt.Errorf("JM_NET_SERVER_URL must not contain credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, fmt.Errorf("JM_NET_SERVER_URL must contain only scheme, host, and optional port")
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("JM_NET_SERVER_URL must contain a hostname or IP address")
	}
	return u, nil
}

// HostIsLANOnly recognizes destinations that cannot serve as a cold remote
// bootstrap address.  DNS names are intentionally classified without doing a
// live lookup: Manager startup must not depend on the WAN being up.
func HostIsLANOnly(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	if host == "" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
			ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() ||
			carrierGradeNAT.Contains(ip)
	}
	if !strings.Contains(host, ".") {
		return true
	}
	for _, suffix := range []string{".local", ".localhost", ".lan", ".home", ".internal", ".invalid", ".test"} {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

// RemoteReady reports whether raw is a public HTTPS coordination URL.  It is
// deliberately stricter than "syntactically valid": plaintext and LAN-only
// URLs can be useful in explicit development installs, but they cannot satisfy
// the release remote-access gate.
func RemoteReady(raw string) bool {
	u, err := ParseServerURL(raw)
	return err == nil && u.Scheme == "https" && !HostIsLANOnly(u.Hostname())
}

// ValidateServerURL applies both security and reachability policy.  LAN-only
// mode is a separate opt-in from plaintext because JM_NET_ALLOW_INSECURE=1 by
// itself previously let a private URL masquerade as a production remote URL.
func ValidateServerURL(raw string, allowInsecure, allowLANOnly bool) error {
	u, err := ParseServerURL(raw)
	if err != nil {
		return err
	}
	lanOnly := HostIsLANOnly(u.Hostname())
	if lanOnly && !allowLANOnly {
		return fmt.Errorf("JM_NET_SERVER_URL %q is LAN-only and cannot cold-start Link away from the NAS; use a public HTTPS URL or set JM_NET_ALLOW_LAN_ONLY=1 only for a deliberate local-development deployment", strings.TrimRight(raw, "/"))
	}
	if u.Scheme == "http" {
		if !allowInsecure {
			return fmt.Errorf("refusing plaintext JuiceMount Link control traffic; use https")
		}
		if !lanOnly || !allowLANOnly {
			return fmt.Errorf("JM_NET_ALLOW_INSECURE=1 is restricted to an explicitly LAN-only development deployment")
		}
	}
	return nil
}
