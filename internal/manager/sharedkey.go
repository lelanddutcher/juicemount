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
// Every Mac the Manager approves while it hands out the shared key is
// marked SharedKeyIssued in the same store write as the approval, before
// any answer can carry the key. Revoke records each such Mac (a "holder")
// with the access key in plain text and a salted check value of the secret
// (never the secret). The Principals page and the revoke answer say so, and
// list the rotation as a pending action.
//
// A holder leaves the page only on proof from the object store
// (VerifySharedKeyRotation), never because the Manager's own environment
// changed:
//   - same access key, different secret: a signed request with the
//     Manager's current key succeeds. MinIO keeps one secret per user
//     (`mc admin user add` replaces it), so the old secret is then refused.
//   - a different access key (JM_OBJ_CLIENT_ACCESS_KEY renamed): in the same
//     check the current key succeeds and the object store answers
//     InvalidAccessKeyId for the old access key, i.e. the old user was
//     removed. minio-keys never removes it by itself.
//
// Either proves that the user's own secret changed (or the user is gone),
// not that no credential made with the old key is left: MinIO lets a user
// mint service accounts and temporary credentials for itself. The jm-client
// policy denies service accounts (server/minio/jm-client-policy.json), the
// compose file caps temporary credentials at 15 minutes, and the rotation
// steps check for service accounts made before the deny.
//
// A holder is dropped when the Mac is approved again. Records revoked by a
// Manager from before holder records are recorded at start-up as "legacy"
// holders (they may have been only pending requests).

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// sharedKeyHolder is a revoked Mac that was given the shared object key
// (stored in the principal store).
type sharedKeyHolder struct {
	Name      string `json:"name"`
	Host      string `json:"host,omitempty"`
	RevokedAt int64  `json:"revoked_at_ms"`
	// AccessKey: the shared key's access key at revoke time (not a secret).
	AccessKey string `json:"access_key"`
	// SecretCheck: "<salt hex>:<sha256 hex>" over the shared secret at
	// revoke time. It tells whether the secret has changed since; it cannot
	// recover it.
	SecretCheck string `json:"secret_check"`
	// Legacy: revoked by a Manager that did not record holders. The record
	// may have been only a pending request.
	Legacy bool `json:"legacy,omitempty"`
}

// SharedKeyHolder is one entry of the Principals page's pending "rotate
// the shared storage key" action.
type SharedKeyHolder struct {
	Name      string `json:"name"`
	Host      string `json:"host,omitempty"`
	RevokedAt int64  `json:"revoked_at_ms,omitempty"`
	AccessKey string `json:"access_key"`
	Legacy    bool   `json:"legacy,omitempty"`
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
	// that still works (it was never given one, or the object store has
	// confirmed that the shared key it was given was rotated).
	RevokeObjectKeyNone = "none"
)

// ObjectStoreProbe makes one signed request to the volume's bucket with
// (accessKey, secretKey). It returns "" when the object store accepted it,
// the S3 error code (for example InvalidAccessKeyId, SignatureDoesNotMatch)
// when it answered with one, and an error when there was no usable answer.
type ObjectStoreProbe func(ctx context.Context, accessKey, secretKey string) (code string, err error)

// sharedKeyProof is the last verification result (memory only; it is
// repeated every sharedKeyVerifyEvery and after a start).
type sharedKeyProof struct {
	// ak, sk: the Manager's key that the object store accepted (ok).
	ak, sk string
	ok     bool
	// gone: old access keys the object store reported unknown.
	gone map[string]bool
	// problem: why a pending rotation is not confirmed ("" = nothing to say).
	problem string
}

// noProbeProblem: the Manager cannot ask the object store.
const noProbeProblem = "the Manager has no object-store address (JM_MINIO_URL and JM_VOL_NAME, or JM_OBJ_BUCKET_URL), so it cannot confirm a rotation"

// sharedKeyVerifyEvery: how often the verifier asks the object store
// (a variable for tests).
var sharedKeyVerifyEvery = time.Minute

// sharedKeyProbeTimeout bounds one probe request.
var sharedKeyProbeTimeout = 10 * time.Second

// sharedKeyCheck is the check value of the secret sk under salt.
func sharedKeyCheck(salt []byte, sk string) string {
	h := sha256.New()
	h.Write([]byte("juicemount shared object secret check\x00"))
	h.Write(salt)
	h.Write([]byte{0})
	h.Write([]byte(sk))
	return hex.EncodeToString(salt) + ":" + hex.EncodeToString(h.Sum(nil))
}

// newSharedKeyCheck salts a fresh check value for sk.
func newSharedKeyCheck(sk string) string {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "" // unreadable check: the entry stays until the Mac is approved again
	}
	return sharedKeyCheck(salt, sk)
}

// sharedSecretUnchanged reports whether check was made over sk. An
// unreadable check counts as unchanged: the entry stays rather than
// clearing without proof.
func sharedSecretUnchanged(check, sk string) bool {
	saltHex, _, ok := strings.Cut(check, ":")
	salt, err := hex.DecodeString(saltHex)
	if !ok || err != nil || len(salt) == 0 {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(sharedKeyCheck(salt, sk)), []byte(check)) == 1
}

// sharedKeyConfigured: the Manager hands out the shared key (mode off and
// a client key configured).
func (s *PrincipalStore) sharedKeyConfigured() bool {
	ak, sk := s.opts.ObjectKey()
	return s.objk == nil && ak != "" && sk != ""
}

// heldSharedKeyLocked: p (as it is before a revoke) holds the shared key:
// it was issued the key (SharedKeyIssued), or it is an approved Mac still
// on the shared key in per-Mac mode (approved before the switch, by a
// Manager that did not mark it).
func (s *PrincipalStore) heldSharedKeyLocked(p *MacPrincipal) bool {
	if p.Status != PrincipalApproved {
		return false
	}
	if p.SharedKeyIssued {
		return true
	}
	ak, sk := s.opts.ObjectKey()
	return ak != "" && sk != "" && (s.objk == nil || !objKeyDeliverable(p))
}

// holderClearedLocked: the object store has proved h's key no longer works.
func (s *PrincipalStore) holderClearedLocked(h sharedKeyHolder) bool {
	ak, sk := s.opts.ObjectKey()
	pr := s.sharedProof
	if h.AccessKey != "" && h.AccessKey != ak {
		return pr.gone[h.AccessKey]
	}
	return ak != "" && sk != "" && !sharedSecretUnchanged(h.SecretCheck, sk) &&
		pr.ok && pr.ak == ak && pr.sk == sk
}

// sharedKeyHoldersLocked: the recorded holders whose key may still work:
// the Mac is not approved again (then it holds the key legitimately), and
// the object store has not proved the key rotated.
func (s *PrincipalStore) sharedKeyHoldersLocked() []sharedKeyHolder {
	var out []sharedKeyHolder
	for _, h := range s.sharedKeyHoldersToKeepLocked() {
		if !s.holderClearedLocked(h) {
			out = append(out, h)
		}
	}
	return out
}

// sharedKeyHoldersToKeepLocked: what the store keeps. A Mac approved again
// is dropped (a later revoke records it afresh). An entry the object store
// proved rotated is kept: if the old secret is ever put back (an older
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
		out = append(out, SharedKeyHolder{Name: h.Name, Host: h.Host, RevokedAt: h.RevokedAt, AccessKey: h.AccessKey, Legacy: h.Legacy})
	}
	return out
}

// sharedKeyProblemLocked explains why listed holders are not cleared
// although the Manager's key differs from theirs ("" otherwise).
func (s *PrincipalStore) sharedKeyProblemLocked(listed []sharedKeyHolder) string {
	ak, sk := s.opts.ObjectKey()
	for _, h := range listed {
		if (h.AccessKey != "" && h.AccessKey != ak) || (ak != "" && sk != "" && !sharedSecretUnchanged(h.SecretCheck, sk)) {
			if s.opts.ObjectStoreProbe == nil {
				return noProbeProblem
			}
			if s.sharedProof.problem != "" {
				return s.sharedProof.problem
			}
			return "the object store has not yet confirmed the rotation (the Manager checks every minute)"
		}
	}
	return ""
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
	return append(out, sharedKeyHolder{Name: p.Name, Host: p.Host, RevokedAt: at, AccessKey: ak, SecretCheck: newSharedKeyCheck(sk)})
}

// sharedKeyTrackingVersion is the store marker (shared_key_tracking) that
// says adoptSharedKeyStateOnLoad has run on this store.
const sharedKeyTrackingVersion = 1

// onSharedKeyVerifierStart, when set, receives the stop function of the
// verifier registerH8B starts (tests stop it).
var onSharedKeyVerifierStart func(stop func())

// adoptSharedKeyStateOnLoad brings a store from a Manager that did not
// track the shared key up to date, once (the store marker), conservatively
// and whatever the per-Mac mode: an earlier Manager may have handed the
// shared key to any Mac it approved, including one that later got its own
// key. So every approved Mac is marked SharedKeyIssued, and every revoked
// Mac without a holder entry becomes a legacy holder (it may have been only
// a pending request). Without a configured client key nothing is adopted
// and the marker is not set. The store is saved when a record changed.
func (s *PrincipalStore) adoptSharedKeyStateOnLoad() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sharedTracking >= sharedKeyTrackingVersion {
		return nil
	}
	ak, sk := s.opts.ObjectKey()
	if ak == "" || sk == "" {
		return nil
	}
	before := make(map[string]*MacPrincipal, len(s.recs))
	for k, v := range s.recs {
		before[k] = v
	}
	prevHolders := s.sharedHolders
	have := map[string]bool{}
	for _, h := range s.sharedHolders {
		have[h.Name] = true
	}
	changed := false
	for _, name := range s.sortedNamesLocked() {
		r := s.recs[name]
		switch {
		case r.Status == PrincipalApproved && !r.SharedKeyIssued:
			next := *r
			next.SharedKeyIssued = true
			s.recs[name] = &next
			changed = true
		case r.Status == PrincipalRevoked && !have[name]:
			s.sharedHolders = append(s.sharedHolders, sharedKeyHolder{Name: name, Host: r.Host, RevokedAt: r.DecidedAt,
				AccessKey: ak, SecretCheck: newSharedKeyCheck(sk), Legacy: true})
			changed = true
			log.Printf("manager: principal %s was revoked by an earlier Manager version and may still hold the shared object key; listed until the object store confirms a rotation", name)
		}
	}
	s.sharedTracking = sharedKeyTrackingVersion
	if !changed {
		return nil // saved with the next store write
	}
	if err := s.saveStoreLocked(); err != nil {
		s.recs, s.sharedHolders, s.sharedTracking = before, prevHolders, 0
		return fmt.Errorf("record shared object key holders: %w", err)
	}
	return nil
}

// noteSharedKeyIssuedLocked persists SharedKeyIssued for rec before a poll
// answer hands it the shared key. A failed save leaves the record unmarked,
// and withObjectKey then withholds the key.
func (s *PrincipalStore) noteSharedKeyIssuedLocked(rec *MacPrincipal) {
	if rec == nil || rec.SharedKeyIssued {
		return
	}
	next := *rec
	s.markSharedKeyIssuedLocked(&next)
	if !next.SharedKeyIssued {
		return
	}
	s.recs[rec.Name] = &next
	if err := s.saveStoreLocked(); err != nil {
		s.recs[rec.Name] = rec
		log.Printf("manager: principal %s: shared object key issue could not be recorded: %v", rec.Name, err)
	}
}

// markSharedKeyIssuedLocked marks p (a copy the caller commits) as given
// the shared key when this Manager hands it out.
func (s *PrincipalStore) markSharedKeyIssuedLocked(p *MacPrincipal) {
	if p.Status == PrincipalApproved && s.sharedKeyConfigured() {
		p.SharedKeyIssued = true
	}
}

// VerifySharedKeyRotation asks the object store whether the keys that
// listed holders were given still work (see the file comment). It changes
// only the in-memory proof.
func (s *PrincipalStore) VerifySharedKeyRotation(ctx context.Context) {
	s.mu.Lock()
	ak, sk := s.opts.ObjectKey()
	probe := s.opts.ObjectStoreProbe
	needCurrent := false
	old := map[string]bool{}
	for _, h := range s.sharedKeyHoldersToKeepLocked() {
		switch {
		case h.AccessKey != "" && h.AccessKey != ak:
			old[h.AccessKey] = true
		case ak != "" && sk != "" && !sharedSecretUnchanged(h.SecretCheck, sk):
			needCurrent = true
		}
	}
	s.mu.Unlock()

	pr := sharedKeyProof{gone: map[string]bool{}}
	var problems []string
	switch {
	case !needCurrent && len(old) == 0:
	case probe == nil:
		problems = append(problems, noProbeProblem)
	case ak == "" || sk == "":
		problems = append(problems, "the Manager has no client key configured, so it cannot confirm a rotation")
	default:
		// The current key is asked first, in every cycle: an answer about
		// an old access key counts only from the object store that just
		// accepted the current key (the same URL and bucket).
		code, err := probeWithTimeout(ctx, probe, ak, sk)
		switch {
		case err != nil:
			problems = append(problems, "the object store could not be asked whether the Manager's key works: "+err.Error())
		case code != "":
			problems = append(problems, "the object store refuses the Manager's key ("+code+"), so an old one may still work. "+
				"Change the key at MinIO (minio-keys) as in the rotation steps")
		default:
			pr.ak, pr.sk, pr.ok = ak, sk, true
		}
		for oldAK := range old {
			if !pr.ok {
				break
			}
			code, err := probeWithTimeout(ctx, probe, oldAK, newProbeSecret())
			switch {
			case err != nil:
				problems = append(problems, "the object store could not be asked about the old access key "+oldAK+": "+err.Error())
			case code == "InvalidAccessKeyId":
				pr.gone[oldAK] = true
			default:
				problems = append(problems, "the object store still knows the old access key "+oldAK+
					" (JM_OBJ_CLIENT_ACCESS_KEY was changed, and minio-keys does not remove the old user). Remove that user at MinIO")
			}
		}
	}
	pr.problem = strings.Join(problems, "; ")
	s.mu.Lock()
	s.sharedProof = pr
	s.mu.Unlock()
}

func probeWithTimeout(ctx context.Context, probe ObjectStoreProbe, ak, sk string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, sharedKeyProbeTimeout)
	defer cancel()
	return probe(ctx, ak, sk)
}

// newProbeSecret is a random secret for asking about an access key: only
// whether the object store knows the access key matters.
func newProbeSecret() string {
	b := make([]byte, 20)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// StartSharedKeyVerifier runs VerifySharedKeyRotation now and then every
// sharedKeyVerifyEvery until stop is called (only with an ObjectStoreProbe).
func (s *PrincipalStore) StartSharedKeyVerifier() (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		t := time.NewTicker(sharedKeyVerifyEvery)
		defer t.Stop()
		for {
			s.VerifySharedKeyRotation(ctx)
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return func() {
		cancel()
		<-exited
	}
}

// emptyPayloadSHA256 is the SHA-256 of an empty request body.
const emptyPayloadSHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// NewObjectStoreProbe returns a probe that lists at most one object of the
// bucket at bucketURL (path style, e.g. http://minio:9000/zpool) with an
// AWS signature V4 request. The jm-client policy allows ListBucket.
func NewObjectStoreProbe(bucketURL, region string) ObjectStoreProbe {
	if region == "" {
		region = "us-east-1"
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	base := strings.TrimRight(bucketURL, "/")
	bucket := base[strings.LastIndex(base, "/")+1:]
	target := base + "?list-type=2&max-keys=1"
	return func(ctx context.Context, ak, sk string) (string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("X-Amz-Content-Sha256", emptyPayloadSHA256)
		if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: ak, SecretAccessKey: sk}, req,
			emptyPayloadSHA256, "s3", region, time.Now()); err != nil {
			return "", err
		}
		resp, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if resp.StatusCode == http.StatusOK {
			// Only a listing of this very bucket counts: a proxy, a login
			// page or another service answering 200 proves nothing.
			var lr struct {
				XMLName xml.Name
				Name    string `xml:"Name"`
			}
			if xml.Unmarshal(body, &lr) != nil || lr.XMLName.Local != "ListBucketResult" || lr.Name != bucket {
				return "", fmt.Errorf("the answer is not a listing of bucket %q", bucket)
			}
			return "", nil
		}
		var e struct {
			Code string `xml:"Code"`
		}
		if xml.Unmarshal(body, &e) == nil && e.Code != "" {
			return e.Code, nil
		}
		return "", fmt.Errorf("HTTP %d without an S3 error code", resp.StatusCode)
	}
}

// revokeResultLocked describes p's object-store access after its revoke.
func (s *PrincipalStore) revokeResultLocked(p *MacPrincipal) RevokeResult {
	res := RevokeResult{Name: p.Name}
	for _, h := range s.sharedKeyHoldersLocked() {
		if h.Name == p.Name {
			res.ObjectKey = RevokeObjectKeyStillValid
			res.Message = p.Name + " is revoked: its Redis login is removed, but the shared storage key (" + h.AccessKey +
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
