package manager

// sharedkey.go: what a revoke leaves behind at the object store when the
// Mac held the shared jm-client key.
//
// With JM_OBJ_PER_MAC unset (the default) every approved Mac is handed the
// same no-delete MinIO key, jm-client (withObjectKey). Revoke removes the
// Mac's Redis login, but the Manager cannot change that key: it holds no
// MinIO admin credential, the key's secret comes from server/.env (where
// minio-keys sets it again on every `up`), and the server's WebDAV and farm
// containers use it too. So a revoked Mac's copy keeps working at MinIO
// until the operator rotates JM_OBJ_CLIENT_SECRET_KEY on the server.
//
// Revoke records each such Mac with a salted check value of the shared key
// as it was then (never the key). The Principals page and the revoke answer
// say so, and list the rotation as a pending action. An entry clears by
// from the page once the Manager runs with a different shared key (the
// operator rotated it and recreated the Manager with the new server/.env),
// and is dropped when the Mac is approved again.

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// sharedKeyHolder is a revoked Mac that was given the shared object key
// (stored in the principal store).
type sharedKeyHolder struct {
	Name      string `json:"name"`
	Host      string `json:"host,omitempty"`
	RevokedAt int64  `json:"revoked_at_ms"`
	// KeyCheck: "<salt hex>:<sha256 hex>" over the shared key at revoke
	// time. It tells whether the key has changed since; it cannot recover
	// the key.
	KeyCheck string `json:"key_check"`
}

// SharedKeyHolder is one entry of the Principals page's pending "rotate
// the shared storage key" action.
type SharedKeyHolder struct {
	Name      string `json:"name"`
	Host      string `json:"host,omitempty"`
	RevokedAt int64  `json:"revoked_at_ms"`
}

// RevokeResult is the revoke answer's account of the Mac's object-store
// access after the revoke.
type RevokeResult struct {
	Name string `json:"name"`
	// ObjectKey is one of the RevokeObjectKey* values.
	ObjectKey string `json:"object_key"`
	Message   string `json:"message"`
}

const (
	// RevokeObjectKeyStillValid: the Mac holds the shared key, which keeps
	// working at the object store until it is rotated on the server.
	RevokeObjectKeyStillValid = "still_valid"
	// RevokeObjectKeyRemoving: the Mac's own per-Mac key is being removed;
	// minio-keyd has not yet confirmed it gone.
	RevokeObjectKeyRemoving = "removing"
	// RevokeObjectKeyRemoved: the Mac's own per-Mac key is gone.
	RevokeObjectKeyRemoved = "removed"
	// RevokeObjectKeyNone: the Manager knows of no object key of the Mac
	// that still works (it was never given one, or the shared key it was
	// given has been rotated since).
	RevokeObjectKeyNone = "none"
)

// sharedKeyCheck is the check value of (ak, sk) under salt.
func sharedKeyCheck(salt []byte, ak, sk string) string {
	h := sha256.New()
	h.Write([]byte("juicemount shared object key check\x00"))
	h.Write(salt)
	h.Write([]byte{0})
	h.Write([]byte(ak))
	h.Write([]byte{0})
	h.Write([]byte(sk))
	return hex.EncodeToString(salt) + ":" + hex.EncodeToString(h.Sum(nil))
}

// newSharedKeyCheck salts a fresh check value for (ak, sk).
func newSharedKeyCheck(ak, sk string) string {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "" // unreadable check: the entry stays until the Mac is approved again
	}
	return sharedKeyCheck(salt, ak, sk)
}

// sharedKeyUnchanged reports whether check was made over (ak, sk). An
// unreadable check counts as unchanged: the warning stays rather than
// clearing without proof.
func sharedKeyUnchanged(check, ak, sk string) bool {
	saltHex, _, ok := strings.Cut(check, ":")
	salt, err := hex.DecodeString(saltHex)
	if !ok || err != nil || len(salt) == 0 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(sharedKeyCheck(salt, ak, sk)), []byte(check)) == 1
}

// heldSharedKeyLocked: p (as it is before a revoke) was given the shared
// key. Shared mode: every approved Mac, while a client key is configured.
// Per-Mac mode hands the shared key to no one, but a Mac approved before
// the switch keeps it until its own key is active.
func (s *PrincipalStore) heldSharedKeyLocked(p *MacPrincipal) bool {
	if p.Status != PrincipalApproved {
		return false
	}
	if ak, sk := s.opts.ObjectKey(); ak == "" || sk == "" {
		return false
	}
	return s.objk == nil || !objKeyDeliverable(p)
}

// sharedKeyHoldersLocked: the recorded holders whose shared key still
// works, as far as the Manager can tell: the Mac is not approved again
// (then it holds the key legitimately), and the Manager's shared key is the
// one it was given (or no key is configured now, so nothing proves a
// rotation).
func (s *PrincipalStore) sharedKeyHoldersLocked() []sharedKeyHolder {
	if len(s.sharedHolders) == 0 {
		return nil
	}
	ak, sk := s.opts.ObjectKey()
	var out []sharedKeyHolder
	for _, h := range s.sharedHolders {
		if r := s.recs[h.Name]; r != nil && r.Status == PrincipalApproved {
			continue
		}
		if ak != "" && sk != "" && !sharedKeyUnchanged(h.KeyCheck, ak, sk) {
			continue
		}
		out = append(out, h)
	}
	return out
}

// sharedKeyHoldersToKeepLocked: what the store keeps. A Mac approved again
// is dropped (a later revoke records it afresh). An entry whose key has
// since been rotated is kept: if the old secret is ever put back (an older
// server/.env restored), its Mac can use it again, and the page shows it.
func (s *PrincipalStore) sharedKeyHoldersToKeepLocked() []sharedKeyHolder {
	var out []sharedKeyHolder
	for _, h := range s.sharedHolders {
		if r := s.recs[h.Name]; r != nil && r.Status == PrincipalApproved {
			continue
		}
		out = append(out, h)
	}
	return out
}

// sharedKeyHolderViewLocked is the Principals page's list.
func (s *PrincipalStore) sharedKeyHolderViewLocked() []SharedKeyHolder {
	var out []SharedKeyHolder
	for _, h := range s.sharedKeyHoldersLocked() {
		out = append(out, SharedKeyHolder{Name: h.Name, Host: h.Host, RevokedAt: h.RevokedAt})
	}
	return out
}

// withSharedKeyHolderLocked returns the holder list with p recorded (any
// earlier entry for the same name replaced).
func (s *PrincipalStore) withSharedKeyHolderLocked(p *MacPrincipal, at int64) []sharedKeyHolder {
	ak, sk := s.opts.ObjectKey()
	out := make([]sharedKeyHolder, 0, len(s.sharedHolders)+1)
	for _, h := range s.sharedHolders {
		if h.Name != p.Name {
			out = append(out, h)
		}
	}
	return append(out, sharedKeyHolder{Name: p.Name, Host: p.Host, RevokedAt: at, KeyCheck: newSharedKeyCheck(ak, sk)})
}

// revokeResultLocked describes p's object-store access after its revoke.
func (s *PrincipalStore) revokeResultLocked(p *MacPrincipal) RevokeResult {
	res := RevokeResult{Name: p.Name}
	for _, h := range s.sharedKeyHoldersLocked() {
		if h.Name == p.Name {
			ak, _ := s.opts.ObjectKey()
			res.ObjectKey = RevokeObjectKeyStillValid
			res.Message = p.Name + " is revoked: its Redis login is removed, but the shared storage key (" + ak +
				") it was given STILL WORKS at the object store. It can still read file data and overwrite stored data there " +
				"directly until that key is rotated on the server. See \"Rotate the shared storage key\" on this page."
			return res
		}
	}
	switch {
	case len(p.ObjKeyRemoving) > 0:
		res.ObjectKey = RevokeObjectKeyRemoving
		res.Message = p.Name + " is revoked. Its own storage key is being removed by minio-keyd; until minio-keyd confirms it, the key may still work."
	case p.ObjKeyGen > 0:
		res.ObjectKey = RevokeObjectKeyRemoved
		res.Message = p.Name + " is revoked. Its own storage key is removed."
	default:
		res.ObjectKey = RevokeObjectKeyNone
		res.Message = p.Name + " is revoked. The Manager knows of no storage key of this Mac that still works."
	}
	return res
}
