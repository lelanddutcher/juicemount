// Package credstore reads the secrets the JuiceMount app keeps out of every
// file it writes: the Redis metadata password, the volume's object-store key
// and the Link pairing key.
//
// The app (app/JuiceMount/.../Core/RedisCredentialStore.swift) owns writing.
// It stores one Keychain generic-password item per slot under the service
// "com.juicemount.redis-metadata", with the item's account attribute naming the
// slot and its generic attribute naming the endpoint the secret belongs to. The
// item's access list names the bundled service binary, so the LaunchAgent can
// read it when launchd restarts the service with no app running (automatic
// recovery). That works for Developer ID builds (app and service share the
// team partition); ad-hoc builds get a per-cdhash partition and the service is
// refused (see RedisCredentialStore.swift for the measurement). A lookup for
// another endpoint finds nothing, so a secret is never sent to a server it was
// not saved for.
//
// An isolated acceptance root (internal/appisolation) keeps the real account
// HOME unchanged. In that explicit mode the app writes a private 0600 JSON
// file per slot inside the selected root, and this package reads those files
// rather than consulting the login Keychain.
//
// Device binding. Every secret is stamped with the internal/deviceid digest of
// the Mac that saved it (Keychain: the item's comment attribute, as
// DeviceStampPrefix+digest; isolated file: the "device" field). This is a
// legacy login-keychain item, where kSecAttrAccessible...ThisDeviceOnly is not
// stored at all, and the login keychain moves with Migration Assistant (and,
// before macOS 26.4, with a Time Machine restore or a cloned disk). A secret
// stamped for another Mac is refused with ErrOtherDevice: it belongs to the
// other Mac's Manager principal and must never be sent from this one. An
// unstamped secret (saved by a build before the binding) is accepted; the app
// stamps it on its next read. The service only reads; the app discards.
package credstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/lelanddutcher/juicemount/internal/appisolation"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
)

// Slot names one stored secret. The string is the Keychain account attribute
// and the isolated file stem; both sides must agree.
type Slot string

const (
	RedisPassword Slot = "metadata"
	LinkAuthKey   Slot = "link-authkey"
	// ObjectKey is the volume's no-delete object-store key (MinIO user
	// jm-client) the Manager hands an approved Mac at enrollment. It is
	// bound to the same endpoint account as the Redis password (the proxy
	// URL with this Mac's principal as the user), stored as
	// EncodeObjectKey(accessKey, secretKey), and given to juicefs as
	// MINIO_ACCESS_KEY / MINIO_SECRET_KEY.
	ObjectKey Slot = "object-key"
)

// The app also keeps its pending enrollment (secret and poll token) in the
// slot "enrollment", and the login it used before an approval switched it to
// its principal (for a rollback) in the slot "metadata-previous"; only the
// app reads them, so they have no constants here.

// EncodeObjectKey is the stored form of an object key: the access key, a
// newline, the secret key. Neither half may be empty or contain a newline.
// Must match ObjectKeyCredential in RedisCredentialStore.swift.
func EncodeObjectKey(accessKey, secretKey string) (string, bool) {
	if accessKey == "" || secretKey == "" || strings.ContainsAny(accessKey, "\r\n") || strings.ContainsAny(secretKey, "\r\n") {
		return "", false
	}
	return accessKey + "\n" + secretKey, true
}

// DecodeObjectKey splits a stored object key; ok is false for any other shape.
func DecodeObjectKey(stored string) (accessKey, secretKey string, ok bool) {
	accessKey, secretKey, found := strings.Cut(stored, "\n")
	if !found || accessKey == "" || secretKey == "" || strings.ContainsAny(secretKey, "\r\n") || strings.ContainsRune(accessKey, '\r') {
		return "", "", false
	}
	return accessKey, secretKey, true
}

// LoadObjectKey returns the object key stored for the Redis endpoint account
// (RedisAccount). A malformed item reads as ErrNotFound.
func LoadObjectKey(account string) (accessKey, secretKey string, err error) {
	stored, err := Load(ObjectKey, account)
	if err != nil {
		return "", "", err
	}
	accessKey, secretKey, ok := DecodeObjectKey(stored)
	if !ok {
		return "", "", ErrNotFound
	}
	return accessKey, secretKey, nil
}

const keychainService = "com.juicemount.redis-metadata"

// ErrNotFound means no secret is stored for that slot and endpoint.
var ErrNotFound = errors.New("credential not stored for this endpoint")

// ErrOtherDevice means a secret is stored for the endpoint but was saved on a
// different Mac (migrated, restored or cloned). It is never returned to the
// caller; this Mac must enroll as a new principal.
var ErrOtherDevice = errors.New("credential was saved on another Mac (migrated or cloned); this Mac must be approved in the Manager as a new Mac")

// DeviceStampPrefix prefixes the device digest in the Keychain item's comment
// attribute. Must match RedisCredentialStore.swift.
const DeviceStampPrefix = "jm-device:"

// deviceFromComment extracts the digest from a Keychain comment attribute. A
// comment without the prefix was not written by JuiceMount: unstamped.
func deviceFromComment(comment string) string {
	device, ok := strings.CutPrefix(comment, DeviceStampPrefix)
	if !ok {
		return ""
	}
	return device
}

// checkDevice applies the binding to a stored stamp ("" = unstamped).
func checkDevice(stamp string) error {
	if stamp == "" {
		return nil // legacy item: adopted (the app stamps it)
	}
	device, err := deviceid.Fingerprint() // tests: deviceid.SetSourceForTest
	if err != nil {
		return nil // no fingerprint to compare against; cannot verify
	}
	if stamp != device {
		return ErrOtherDevice
	}
	return nil
}

// RedisAccount is the endpoint a Redis password is bound to, in the one
// canonical form both the app (RedisURLCredential.account) and this package
// produce, checked against testdata/endpoint_vectors.json on both sides:
//
//	<scheme lower>://[<user>@]<host lower>:<port>
//
// user is the decoded username re-encoded with EscapeUnreserved; an IPv6
// literal is written in brackets; port defaults to 6379. Never the password,
// database or query.
func RedisAccount(raw string) (string, bool) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return "", false
	}
	user := ""
	if u.User != nil && u.User.Username() != "" {
		user = EscapeUnreserved(u.User.Username()) + "@"
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	port := u.Port()
	if port == "" {
		port = "6379"
	}
	return strings.ToLower(u.Scheme) + "://" + user + host + ":" + port, true
}

// EscapeUnreserved percent-encodes every byte except the RFC 3986 unreserved
// set A-Z a-z 0-9 - . _ ~, with upper-case hex. It is the single encoding for
// userinfo in canonical URLs and endpoint keys, matching the app's
// RedisURLCredential (Swift addingPercentEncoding with the same set).
func EscapeUnreserved(s string) string {
	const hex = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&15])
	}
	return b.String()
}

// CanonicalRedisURL rewrites a Redis URL so that the same endpoint, user and
// password always give the same string, however the password was encoded
// ("a+b", "a%2Bb" and "a%2bb" are one password). The service canonicalizes
// every config it receives and recovery rebuilds URLs the same way, so a
// claim made by the app and the same claim replayed by recovery are equal.
// Userinfo is written with EscapeUnreserved; the rest of the URL is Go's
// url.URL serialization. A URL that does not parse is returned unchanged.
func CanonicalRedisURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw
	}
	name := u.User.Username()
	password, _ := u.User.Password()
	return WithPassword(withUser(u, name), password)
}

// WithPassword attaches password to a URL that carries none, in canonical
// form. An empty password, or a URL that does not parse, leaves it unchanged
// (apart from canonical userinfo).
func WithPassword(raw, password string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	name := ""
	if u.User != nil {
		name = u.User.Username()
	}
	base := withUser(u, "")
	if name == "" && password == "" {
		return base
	}
	scheme, rest, ok := strings.Cut(base, "://")
	if !ok {
		return raw
	}
	info := EscapeUnreserved(name)
	if password != "" {
		info += ":" + EscapeUnreserved(password)
	}
	return scheme + "://" + info + "@" + rest
}

// withUser serializes u with no userinfo when name is empty; callers add
// canonical userinfo themselves.
func withUser(u *url.URL, name string) string {
	c := *u
	c.User = nil
	if name != "" {
		// Re-added by WithPassword in canonical form.
		s := c.String()
		if scheme, rest, ok := strings.Cut(s, "://"); ok {
			return scheme + "://" + EscapeUnreserved(name) + "@" + rest
		}
		return s
	}
	return c.String()
}

// LinkAccount is the endpoint a Link pairing key is bound to: the control URL
// without surrounding whitespace or a trailing slash.
func LinkAccount(controlURL string) string {
	return strings.TrimRight(strings.TrimSpace(controlURL), "/")
}

// Load returns the secret stored for slot and endpoint account. It never shows
// UI: the Keychain read fails instead of prompting, and it disables Keychain
// UI for the whole calling process (see keychain_darwin.go).
func Load(slot Slot, account string) (string, error) {
	if account == "" {
		return "", ErrNotFound
	}
	scope, isolated, err := appisolation.Current()
	if err != nil {
		return "", err
	}
	if isolated {
		return loadIsolated(filepath.Join(scope.Root, isolatedLeaf(slot)), account)
	}
	secret, stamp, err := keychainRead(keychainService, string(slot), account)
	if err != nil {
		return "", err
	}
	if err := checkDevice(deviceFromComment(stamp)); err != nil {
		return "", err
	}
	return secret, nil
}

// keychainRead is the Keychain reader Load uses; tests substitute it so the
// device check on the Keychain path is exercised without the real Keychain.
var keychainRead = keychainLoad

func isolatedLeaf(slot Slot) string { return "credential-" + string(slot) + ".json" }

type isolatedEntry struct {
	Account string `json:"account"`
	Secret  string `json:"secret"`
	Device  string `json:"device,omitempty"` // deviceid digest of the saving Mac
}

func loadIsolated(path string, account string) (string, error) {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	sys, ok := st.Sys().(*syscall.Stat_t)
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 || !ok || int(sys.Uid) != os.Geteuid() || sys.Nlink != 1 {
		return "", fmt.Errorf("isolated credential file must be a private 0600 file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var e isolatedEntry
	if err := json.Unmarshal(data, &e); err != nil {
		return "", fmt.Errorf("isolated credential file unreadable")
	}
	if e.Account != account || e.Secret == "" {
		return "", ErrNotFound
	}
	if err := checkDevice(e.Device); err != nil {
		return "", err
	}
	return e.Secret, nil
}
