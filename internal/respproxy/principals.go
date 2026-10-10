package respproxy

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"golang.org/x/crypto/argon2"
)

// PrincipalEntry is one row of the principals file
// (/etc/jm-respproxy/principals.json):
//
//	{"principals":[{"name":"edit-bay-2","kind":"mac","host":"Edit-Bay-2",
//	                "argon2id":"$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>"}]}
//
// The hash is a PHC string (RawStdEncoding base64), as produced by
// HashPassword or `jm-respproxy hash`.
type PrincipalEntry struct {
	Name     string `json:"name"`
	Kind     Kind   `json:"kind"`
	Host     string `json:"host,omitempty"`
	Argon2id string `json:"argon2id"`
	Disabled bool   `json:"disabled,omitempty"`
}

type principalsFile struct {
	Principals []PrincipalEntry `json:"principals"`
}

// argon2Hash is a parsed PHC argon2id string.
type argon2Hash struct {
	time, memory uint32
	threads      uint8
	salt, key    []byte
}

// Default hashing parameters: OWASP's argon2id minimum (19 MiB, t=2, p=1).
const (
	DefaultArgonTime    = 2
	DefaultArgonMemory  = 19 * 1024
	DefaultArgonThreads = 1
	argonKeyLen         = 32
	argonSaltLen        = 16
	// Refuse parameters that would let a crafted principals file make one
	// AUTH allocate gigabytes.
	maxArgonMemory = 1 << 20 // KiB = 1 GiB
	maxArgonTime   = 16
)

// HashPassword returns a PHC argon2id string for pass.
func HashPassword(pass string, time, memoryKiB uint32, threads uint8) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(pass), salt, time, memoryKiB, threads, argonKeyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, memoryKiB, time, threads, enc.EncodeToString(salt), enc.EncodeToString(key)), nil
}

func parseArgon2id(s string) (argon2Hash, error) {
	var h argon2Hash
	parts := strings.Split(s, "$")
	// "", "argon2id", "v=19", "m=..,t=..,p=..", salt, key
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return h, errors.New("not a PHC argon2id string")
	}
	if parts[2] != "v="+strconv.Itoa(argon2.Version) {
		return h, fmt.Errorf("unsupported argon2 version %q", parts[2])
	}
	for _, kv := range strings.Split(parts[3], ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			return h, fmt.Errorf("bad parameter %q", kv)
		}
		n, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return h, fmt.Errorf("bad parameter %q", kv)
		}
		switch k {
		case "m":
			h.memory = uint32(n)
		case "t":
			h.time = uint32(n)
		case "p":
			if n == 0 || n > 255 {
				return h, fmt.Errorf("bad parallelism %d", n)
			}
			h.threads = uint8(n)
		default:
			return h, fmt.Errorf("unknown parameter %q", k)
		}
	}
	if h.time == 0 || h.time > maxArgonTime || h.memory < 8 || h.memory > maxArgonMemory || h.threads == 0 {
		return h, fmt.Errorf("argon2id parameters out of range (m=%d t=%d p=%d)", h.memory, h.time, h.threads)
	}
	enc := base64.RawStdEncoding
	var err error
	if h.salt, err = enc.DecodeString(parts[4]); err != nil || len(h.salt) < 8 {
		return h, errors.New("bad salt")
	}
	if h.key, err = enc.DecodeString(parts[5]); err != nil || len(h.key) < 16 {
		return h, errors.New("bad hash")
	}
	return h, nil
}

func (h argon2Hash) verify(pass string) bool {
	got := argon2.IDKey([]byte(pass), h.salt, h.time, h.memory, h.threads, uint32(len(h.key)))
	return subtle.ConstantTimeCompare(got, h.key) == 1
}

type principalRec struct {
	p    Principal
	hash argon2Hash
}

// Principals is a validated, immutable principal table.
type Principals struct {
	byName map[string]principalRec
}

// ParsePrincipals validates a principals file body. Every entry must have a
// unique name, a known kind and a valid argon2id hash. Kind "anon" does not
// exist: there is no unauthenticated access.
func ParsePrincipals(data []byte) (*Principals, error) {
	var f principalsFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("principals: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return nil, errors.New("principals: trailing JSON or data")
	}
	ps := &Principals{byName: map[string]principalRec{}}
	for i, e := range f.Principals {
		if e.Name == "" || strings.ContainsAny(e.Name, " \t\r\n@") {
			return nil, fmt.Errorf("principals[%d]: bad name %q", i, e.Name)
		}
		switch e.Kind {
		case KindMac, KindFarm, KindManager, KindBG, KindWebDAV, KindAdmin:
		default:
			return nil, fmt.Errorf("principals[%d] %s: unknown kind %q", i, e.Name, e.Kind)
		}
		if _, dup := ps.byName[e.Name]; dup {
			return nil, fmt.Errorf("principals[%d]: duplicate name %q", i, e.Name)
		}
		h, err := parseArgon2id(e.Argon2id)
		if err != nil {
			return nil, fmt.Errorf("principals[%d] %s: %w", i, e.Name, err)
		}
		if e.Disabled {
			continue
		}
		ps.byName[e.Name] = principalRec{p: Principal{Name: e.Name, Kind: e.Kind, Host: e.Host}, hash: h}
	}
	return ps, nil
}

// LoadPrincipals reads and validates a principals file. The file holds
// password hashes, so it must not be readable or writable by group or other
// (mode 0600 or 0400): a looser file is refused.
func LoadPrincipals(path string) (*Principals, error) {
	ps, _, err := loadPrincipalsWithDigest(path)
	return ps, err
}

// loadPrincipalsWithDigest binds validation to the exact opened bytes. A
// concurrent atomic replacement cannot change the digest after parsing.
func loadPrincipalsWithDigest(path string) (ps *Principals, digest string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		if closeErr := f.Close(); err == nil && closeErr != nil {
			ps, digest, err = nil, "", closeErr
		}
	}()
	st, err := f.Stat()
	if err != nil {
		return nil, "", err
	}
	if !st.Mode().IsRegular() {
		return nil, "", fmt.Errorf("principals %s: not a regular file", path)
	}
	if perm := st.Mode().Perm(); perm&0o077 != 0 {
		return nil, "", fmt.Errorf("principals %s: mode %04o is readable or writable by group/other; chmod 600", path, perm)
	}
	const maxPrincipalsBytes = 16 << 20
	data, err := io.ReadAll(io.LimitReader(f, maxPrincipalsBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(data) > maxPrincipalsBytes {
		return nil, "", errors.New("principals: file exceeds 16 MiB")
	}
	ps, err = ParsePrincipals(data)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	return ps, hex.EncodeToString(sum[:]), nil
}

// Len is the number of enabled principals.
func (ps *Principals) Len() int { return len(ps.byName) }
