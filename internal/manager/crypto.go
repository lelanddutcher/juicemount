// Package manager — SLICE 4 credential encryption helpers.
//
// This file contains ONLY the low-level cryptographic primitives used
// by destinations.go to encrypt and decrypt remote-endpoint credentials
// at rest. There is no CRUD logic here. The split is deliberate so the
// security reviewer can audit a small, self-contained surface.
//
// Scheme (locked in docs/ROADMAP/juicemount-manager.md §3.2):
//
//   - KDF:    HKDF-SHA256(JM_ADMIN_KEY, info="juicemount-manager v1 cred-key")
//             → 32 bytes (suitable for AES-256-GCM)
//   - Cipher: AES-256-GCM (AEAD — confidentiality + integrity in one pass)
//   - Nonce:  12 random bytes per secret, generated with crypto/rand,
//             NEVER reused under the same key
//   - Tag:    16 bytes (GCM default), appended by Seal, verified by Open
//   - Wire:   <12B nonce><ciphertext><16B GCM tag>, base64-encoded
//             inside JSON
//
// The info string MUST stay literal — changing it would invalidate
// every credential previously written to disk because HKDF would derive
// a different key. Same reason the version "v1" is baked into the
// string: when we ever bump the KDF, we'll add a v2 info string and
// keep the v1 path readable for one release so admins can rotate
// without losing destinations.
package manager

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// credKeyInfo is the HKDF "info" parameter that namespaces this key
// derivation. Locked by §3.2 of the manager roadmap — if it ever
// changes, every previously-encrypted credential becomes undecryptable
// without an explicit migration path. The "v1" segment is the version
// hook for that future migration.
//
// Distinct info strings let us derive multiple, unrelated subkeys from
// the same admin key in future slices (e.g. a separate signing key for
// audit-log entries) without weakening the credential-encryption key.
const credKeyInfo = "juicemount-manager v1 cred-key"

// credKeyLen is the derived key length in bytes. 32 bytes selects
// AES-256-GCM in aes.NewCipher (which keys to AES-128/192/256 based on
// key length). AES-256 is the conservative choice — no measurable
// performance penalty for the small credential payloads we encrypt
// (typically a few hundred bytes per Destination), and it sidesteps the
// "is AES-128 enough" debate entirely.
const credKeyLen = 32

// gcmNonceLen is the AES-GCM nonce length in bytes. 12 bytes (96 bits)
// is the GCM standard recommended by NIST SP 800-38D §5.2.1.1 and the
// default for cipher.NewGCM. Other nonce sizes are technically allowed
// but force a different (slower, less-reviewed) GHASH initialization.
// We stick with 12 to keep the wire format aligned with the standard
// and the verification surface narrow.
const gcmNonceLen = 12

// gcmTagLen is the AES-GCM authentication tag length in bytes. The Go
// crypto/cipher package always appends the full 16-byte tag in Seal and
// verifies all 16 bytes in Open. We hard-code the value here so the
// constant appears in code review next to the wire-format comments
// rather than buried in stdlib defaults — a future maintainer can grep
// for "16" and see the rationale.
const gcmTagLen = 16

// deriveCredKey derives a 32-byte AES-256 key from the operator's
// JM_ADMIN_KEY using HKDF-SHA256. HKDF is RFC 5869; the empty salt is
// allowed (RFC §3.1) and the spec falls back to a zeroed salt of HashLen
// bytes in that case — fine for our threat model because (a) the input
// admin key is already high-entropy operator-provided material, not a
// low-entropy password, and (b) we get our domain separation from the
// fixed info string. The output is suitable for direct use as an
// AES-256 key.
//
// adminKey must be non-empty; callers should reject empty admin keys
// earlier in the request lifecycle so they never reach this function.
//
// Returned slice is exactly credKeyLen bytes long. Errors propagate
// from hkdf.New's io.Reader (which can theoretically fail only if the
// underlying SHA-256 implementation does, which Go's stdlib does not).
func deriveCredKey(adminKey []byte) ([]byte, error) {
	if len(adminKey) == 0 {
		return nil, errors.New("crypto: admin key is empty (set JM_ADMIN_KEY to enable credential encryption)")
	}
	// hkdf.New(SHA-256, secret, salt, info) returns an io.Reader that
	// yields key material. We Read exactly credKeyLen bytes — io.ReadFull
	// ensures we don't get a short read in the (unreachable) case where
	// the reader returns 0,nil.
	r := hkdf.New(sha256.New, adminKey, nil, []byte(credKeyInfo))
	out := make([]byte, credKeyLen)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, fmt.Errorf("crypto: hkdf read: %w", err)
	}
	return out, nil
}

// encryptSecret encrypts plaintext under key (which MUST be exactly
// credKeyLen bytes — i.e. the output of deriveCredKey). Returns the
// concatenated <nonce||ciphertext||tag> blob suitable for base64
// encoding into the persisted JSON.
//
// The nonce is fresh random bytes from crypto/rand per call. AES-GCM
// catastrophically fails if a (key, nonce) pair is ever reused: an
// attacker who sees two ciphertexts under the same nonce can recover
// the XOR of the plaintexts AND forge new messages under that key. We
// never reuse: every call reads gcmNonceLen new bytes. With 96 bits of
// nonce and a credentials store likely measured in the dozens of
// entries, the birthday-bound collision probability is negligible
// (≈ 2^-96 per pair).
//
// AES-GCM is malleability-resistant: Open returns an error if even one
// bit of the ciphertext OR the appended tag is modified.
func encryptSecret(plaintext, key []byte) ([]byte, error) {
	if len(key) != credKeyLen {
		// Defensive — every caller routes key through deriveCredKey
		// which returns exactly credKeyLen bytes, but a refactor that
		// passes the raw admin key by accident would otherwise produce
		// AES-128/192 silently.
		return nil, fmt.Errorf("crypto: key length %d, want %d", len(key), credKeyLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		// aes.NewCipher only errors on bad key length; defensive return.
		return nil, fmt.Errorf("crypto: aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm: %w", err)
	}
	// Sanity-check the GCM implementation's nonce size matches the
	// constant we documented in the wire format. cipher.NewGCM returns
	// the standard 12-byte variant, but a future stdlib change or a
	// caller swapping to NewGCMWithNonceSize would diverge from the doc
	// without this guard.
	if aead.NonceSize() != gcmNonceLen {
		return nil, fmt.Errorf("crypto: unexpected nonce size %d (want %d)", aead.NonceSize(), gcmNonceLen)
	}
	nonce := make([]byte, gcmNonceLen)
	// crypto/rand on Linux uses getrandom(2); on Darwin /dev/urandom.
	// Both are fine sources of cryptographic randomness.
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("crypto: read nonce: %w", err)
	}
	// Seal(dst, nonce, plaintext, additionalData) writes
	// <ciphertext||tag> into dst's tail. We pass nonce as dst so the
	// returned slice is <nonce||ciphertext||tag> — the exact wire
	// format documented in §3.2 of the roadmap.
	//
	// Additional Authenticated Data (AAD) is nil here: we're not
	// binding the ciphertext to an external context. If a future slice
	// wants per-destination AAD (e.g. binding to destination name to
	// prevent ciphertext swapping across entries), it goes in this
	// argument; the encryption helper signature stays compatible.
	return aead.Seal(nonce, nonce, plaintext, nil), nil
}

// decryptSecret reverses encryptSecret. blob is the wire form
// <12B nonce><ciphertext><16B tag>; returns the plaintext.
//
// A wrong key, a corrupted ciphertext, or a corrupted tag all surface
// as the SAME error (the AEAD verification failure from gcm.Open) so a
// caller can't distinguish them and use the error type as an oracle.
// This is intentional — it's the standard AEAD discipline.
func decryptSecret(blob, key []byte) ([]byte, error) {
	if len(key) != credKeyLen {
		return nil, fmt.Errorf("crypto: key length %d, want %d", len(key), credKeyLen)
	}
	if len(blob) < gcmNonceLen+gcmTagLen {
		// Smallest legal blob is empty-plaintext: nonce + tag = 28 bytes.
		// Anything shorter is malformed (truncated on disk, wrong
		// encoding, etc.). Catch it before handing to Open so the
		// failure message is actionable.
		return nil, fmt.Errorf("crypto: blob too short (%d bytes; need at least %d)", len(blob), gcmNonceLen+gcmTagLen)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: aes cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: gcm: %w", err)
	}
	if aead.NonceSize() != gcmNonceLen {
		return nil, fmt.Errorf("crypto: unexpected nonce size %d (want %d)", aead.NonceSize(), gcmNonceLen)
	}
	nonce := blob[:gcmNonceLen]
	ciphertextAndTag := blob[gcmNonceLen:]
	// Open(dst, nonce, ciphertextAndTag, additionalData) returns the
	// plaintext on success or an error if the tag doesn't verify (wrong
	// key, tampered ciphertext, truncated tag, etc.). dst=nil means
	// "allocate a new slice" — we don't try to reuse a buffer because
	// credentials are small and the convenience of returning a fresh
	// slice outweighs the (negligible) allocation cost.
	plaintext, err := aead.Open(nil, nonce, ciphertextAndTag, nil)
	if err != nil {
		// Don't echo blob content in the error message — even a length-
		// based message could give a side channel about which secrets
		// exist. Generic-but-actionable phrasing only.
		return nil, fmt.Errorf("crypto: AEAD verification failed (wrong key or corrupted ciphertext)")
	}
	return plaintext, nil
}
