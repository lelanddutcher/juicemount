package manager

// principals.go: H-8 option B Mac enrollment and the proxy principals file
// (H8_OPTION_B_PLAN.md §1.6).
//
// Flow:
//  1. A 0.5 Mac POSTs /api/principals/enroll with its instance ID (cplane
//     instance-id) and a secret it generated and keeps in the Keychain. The
//     route needs no admin key: the Mac does not have one. The Manager
//     hashes the secret at once (argon2id for the proxy, sha256 for the
//     rollback ACL) and never stores it.
//  2. The request is pending until an admin approves it in the Manager
//     (POST /api/principals/approve), unless an admin opened the optional
//     auto-approve window for the cutover week.
//  3. On approve/revoke the Manager rewrites the proxy's principals file
//     (shared directory JM_RESPPROXY_DIR) atomically with mode 0600, keeping
//     the server principals the respproxy entrypoint generated, and writes
//     reload.request naming the new file's sha256. The entrypoint answers
//     by applying the exact requested bytes and writing reload.applied.
//  4. The Mac polls the same enroll route with the same secret and learns
//     "approved"; it then logs in to the proxy as the returned principal.
//
// The file format is internal/respproxy/principals.go's (unknown fields are
// refused there): {"principals":[{name,kind,host,argon2id,disabled}]}.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/argon2"
)

// PrincipalStatus is the enrollment state of one Mac.
type PrincipalStatus string

const (
	PrincipalPending  PrincipalStatus = "pending"
	PrincipalApproved PrincipalStatus = "approved"
	PrincipalRevoked  PrincipalStatus = "revoked"
)

const (
	proxyPrincipalsName = "principals.json"
	rollbackMacsName    = "rollback-macs.acl"
	reloadRequestName   = "reload.request"
	reloadAckName       = "reload.applied"
	legacyReloadAckName = "reload.ack"
	// reloadHeartbeatName: the respproxy entrypoint rewrites this file on
	// every handshake tick (every 2 s) while jm-respproxy runs. Its mtime is
	// how the Manager learns the proxy is still running between reloads.
	reloadHeartbeatName = "reload.heartbeat"
	principalsLockName  = ".principals.lock"

	// The longest auto-approve window an admin can open (two weeks); the
	// founder's default is one week of cutover.
	maxAutoApproveWindow = 14 * 24 * time.Hour
	// Unauthenticated callers can create at most this many pending requests.
	defaultMaxPending = 64
)

// proxyPrincipalEntry mirrors respproxy.PrincipalEntry exactly.
type proxyPrincipalEntry struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Host     string `json:"host,omitempty"`
	Argon2id string `json:"argon2id"`
	Disabled bool   `json:"disabled,omitempty"`
}

type proxyPrincipalsFile struct {
	Principals []proxyPrincipalEntry `json:"principals"`
}

// argon2id parameters: the proxy's defaults (respproxy.DefaultArgon*), the
// OWASP minimum. The proxy refuses m > 1 GiB or t > 16 when it loads.
const (
	argonTime    = 2
	argonMemory  = 19 * 1024
	argonThreads = 1
	argonKeyLen  = 32
	argonSaltLen = 16
)

func hashSecretPHC(secret string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(secret), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	enc := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemory, argonTime, argonThreads, enc.EncodeToString(salt), enc.EncodeToString(key)), nil
}

type phcHash struct {
	time, memory uint32
	threads      uint8
	salt, key    []byte
}

// parsePHCArgon2id accepts exactly what respproxy.parseArgon2id accepts.
func parsePHCArgon2id(s string) (phcHash, error) {
	var h phcHash
	parts := strings.Split(s, "$")
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
	if h.time == 0 || h.time > 16 || h.memory < 8 || h.memory > 1<<20 || h.threads == 0 {
		return h, errors.New("argon2id parameters out of range")
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

func verifySecretPHC(phc, secret string) bool {
	h, err := parsePHCArgon2id(phc)
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(secret), h.salt, h.time, h.memory, h.threads, uint32(len(h.key)))
	return subtle.ConstantTimeCompare(got, h.key) == 1
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// enrollFingerprint is a short value the Mac shows next to its request so
// the admin can tell a genuine request from a raced one. It reveals nothing
// usable about the secret.
func enrollFingerprint(secret string) string {
	h := sha256Hex("jm-enroll-fingerprint\x00" + secret)
	return strings.ToUpper(h[0:4] + "-" + h[4:8])
}

// EnrollRequest is the body of POST /api/principals/enroll.
//
// The first request carries the secret; the response carries a poll
// token. Later polls send instance_id + poll_token only, so the secret
// crosses the wire once and a poll costs one sha256, not an argon2id.
type EnrollRequest struct {
	InstanceID string `json:"instance_id"`
	Host       string `json:"host"`
	Label      string `json:"label"`
	Secret     string `json:"secret"`
	PollToken  string `json:"poll_token"`
	// PreviousInstanceID is the instance ID this Mac used before its
	// identity was rotated (cplane CloneNotice: the disk was migrated,
	// restored or cloned from another Mac). Recorded with a new request so
	// the admin can see which principal the copy came from. Optional.
	PreviousInstanceID string `json:"previous_instance_id,omitempty"`
	// DeviceDigest is the internal/deviceid digest of the requesting Mac
	// (the /whoami device_digest). Recorded on the first request; a later
	// request for the same principal from a different digest flags the
	// principal (DeviceMismatch) for the admin. Optional.
	DeviceDigest string `json:"device_digest,omitempty"`
	// ObjectKeyInUse is the access-key ID (never the secret) of the object
	// key the Mac's running mount uses: one of its own per-Mac keys
	// (mac-<12 hex>-<gen>) or the shared key's name. Self-reported; shown
	// on the Principals page (per-Mac keys on) and grants nothing. Optional.
	ObjectKeyInUse string `json:"object_key_in_use,omitempty"`
}

// EnrollResponse never carries the secret or any hash of it. PollToken is
// set only when a secret was verified or recorded (first enroll, or a
// re-enroll after the Mac lost its token); it replaces any earlier token.
//
// ObjectAccessKey / ObjectSecretKey are the volume's no-delete object key
// (MinIO user jm-client, H-8B §2.1). They are set only when the principal
// is approved, and only in a response to a request that proved itself
// (poll token or enrollment secret). The Mac stores them in its Keychain
// and gives them to juicefs as MINIO_ACCESS_KEY / MINIO_SECRET_KEY. Absent
// when the Manager has no client key configured.
//
// ManagerID is this Manager's random identity (PrincipalStore.ManagerID),
// sent in every enroll and poll response and in the X-JM-Manager-ID header.
// The Mac binds its enrollment to it. It is not a secret: it stops a Mac
// sending its secret to a different Manager by accident (a respelled or
// reused address), not an impersonator who has read it.
type EnrollResponse struct {
	ManagerID       string          `json:"manager_id,omitempty"`
	Status          PrincipalStatus `json:"status"`
	Principal       string          `json:"principal"`
	Fingerprint     string          `json:"fingerprint"`
	PollToken       string          `json:"poll_token,omitempty"`
	ObjectAccessKey string          `json:"object_access_key,omitempty"`
	ObjectSecretKey string          `json:"object_secret_key,omitempty"`
	// ObjectKeyPending (per-Mac keys on): approved, but this Mac's own key
	// is not active yet. The Mac keeps the key it has and polls at its
	// pending interval instead of the approved one.
	ObjectKeyPending bool `json:"object_key_pending,omitempty"`
}

// withObjectKey adds this Manager's ID to a response, and the object key to
// an approved one: with per-Mac keys on, rec's own key when it is active and
// nothing otherwise (never the shared jm-client key: a Mac approved before
// the switch keeps the key it already holds until its own is active);
// with the mode off, the shared jm-client key.
func (s *PrincipalStore) withObjectKey(r EnrollResponse, rec *MacPrincipal) EnrollResponse {
	r.ManagerID = s.managerID
	if r.Status != PrincipalApproved {
		return r
	}
	if s.objk != nil {
		if objKeyDeliverable(rec) {
			r.ObjectAccessKey, r.ObjectSecretKey = rec.ObjKeyName, rec.ObjKeySecret
		} else {
			r.ObjectKeyPending = true
		}
		return r
	}
	ak, sk := s.opts.ObjectKey()
	if ak != "" && sk != "" {
		r.ObjectAccessKey, r.ObjectSecretKey = ak, sk
	}
	return r
}

var (
	ErrEnrollInvalid     = errors.New("invalid enrollment request")
	ErrEnrollConflict    = errors.New("this instance is already enrolled with a different secret")
	ErrEnrollTooMany     = errors.New("too many pending enrollment requests")
	ErrEnrollRateLimited = errors.New("too many enrollment requests from this address")
	ErrEnrollBusy        = errors.New("enrollment is busy; retry shortly")
	ErrEnrollBadToken    = errors.New("unknown instance or wrong poll token")
	ErrPrincipalNotFound = errors.New("no such principal")
	ErrPrincipalState    = errors.New("principal is not in a state that allows this")
	// ErrProxyNotLive: the Redis proxy has not acknowledged the current
	// principals file (reload.applied does not name the sha256 in
	// reload.request), or no proxy directory is configured. Approving a Mac
	// then would hand it a principal nothing accepts yet: the Mac would log
	// in as that principal and be refused (WRONGPASS) by the pre-cutover
	// Redis.
	ErrProxyNotLive = errors.New("the Redis proxy is not live: it has not acknowledged the current principals file (reload.applied does not echo the latest reload.request)")
	// ErrProxyImageOutdated: a proxy answers the reload handshake, but its
	// image predates the heartbeat (server/respproxy/min-gate-commit): it
	// writes no reload.heartbeat, or it acknowledges with the old two-field
	// "sha256 seconds" line. The cutover is done; the image must be updated.
	ErrProxyImageOutdated = fmt.Errorf("%w: the jm-respproxy image lacks current applied-reload proof; update the jm-respproxy image", ErrProxyNotLive)
	// ErrProxyStopped: the proxy acknowledged earlier, but its heartbeat
	// stopped (the container stopped or crashed while the Manager kept
	// running).
	ErrProxyStopped = fmt.Errorf("%w: the proxy's reload heartbeat stopped", ErrProxyNotLive)
	// ErrProxyStalled: the proxy's heartbeat runs, but it has stopped
	// acknowledging reload requests (review 4 A2).
	ErrProxyStalled = fmt.Errorf("%w: the proxy's heartbeat runs but it does not acknowledge reload requests", ErrProxyNotLive)
)

var (
	instanceIDRe = regexp.MustCompile(`^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$`)
	// The secret goes into a Redis URL / META_PASSWORD unchanged.
	secretRe    = regexp.MustCompile(`^[A-Za-z0-9._~-]{32,256}$`)
	pollTokenRe = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// internal/deviceid digest: 16 bytes, lower-case hex.
	deviceDigestRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
	macNameRe      = regexp.MustCompile(`^mac-[0-9a-f]{12}$`)
)

func macPrincipalName(instanceID string) string {
	return "mac-" + strings.ToLower(strings.ReplaceAll(instanceID, "-", ""))[:12]
}

func cleanText(s string, max int) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(s) {
		if r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	out := b.String()
	if len(out) > max {
		out = out[:max]
	}
	return out
}

// MacPrincipal is one Mac's enrollment record (Manager-private store).
type MacPrincipal struct {
	Name        string          `json:"name"`
	InstanceID  string          `json:"instance_id"`
	Host        string          `json:"host"`
	Label       string          `json:"label,omitempty"`
	Status      PrincipalStatus `json:"status"`
	Argon2id    string          `json:"argon2id,omitempty"`
	RedisSHA256 string          `json:"redis_sha256,omitempty"` // rollback ACL (#<sha256>)
	Fingerprint string          `json:"fingerprint"`
	SourceIP    string          `json:"source_ip,omitempty"`
	RequestedAt int64           `json:"requested_at_ms"`
	DecidedAt   int64           `json:"decided_at_ms,omitempty"`
	// AutoApproved: approved by the cutover window, not by a person. It
	// stays set (the admin's "needs review" list) until an admin confirms
	// it with Approve, or revokes it.
	AutoApproved    bool   `json:"auto_approved,omitempty"`
	PollTokenSHA256 string `json:"poll_token_sha256,omitempty"`
	// PreviousInstanceID: the instance ID this Mac reported as its own
	// before a clone rotation (EnrollRequest.PreviousInstanceID).
	//
	// It is SELF-REPORTED AND UNPROVEN: the requesting Mac states it, and
	// nothing checks that the Mac ever held that instance ID or its secret.
	// A Mac can also simply omit it. The Principals page shows it to help the
	// admin decide, never as evidence; it grants nothing.
	PreviousInstanceID string `json:"previous_instance_id,omitempty"`
	// DeviceDigest: the device digest of the first request.
	DeviceDigest string `json:"device_digest,omitempty"`
	// DeviceMismatch: a later request for this principal carried a
	// different device digest, so two Macs may hold one identity (a copy
	// made before the device binding existed, or a forged digest). It stays
	// set until an admin revokes and forgets the principal.
	DeviceMismatch   bool   `json:"device_mismatch,omitempty"`
	MismatchedDigest string `json:"mismatched_device_digest,omitempty"`

	// Per-Mac object keys (objkeys.go; JM_OBJ_PER_MAC=1). All empty when
	// the mode was never on, so such a store is byte-identical to before.
	//
	// AutoApprovePending: the auto-approve window admitted this Mac, but
	// its storage key is not active yet; the reconciler approves it when it
	// is. Until then the Mac sees "pending".
	AutoApprovePending bool `json:"auto_approve_pending,omitempty"`
	// ObjKeyGen / ObjKeyName: the current key's generation and name
	// (mac-<12 hex>-<gen>).
	ObjKeyGen  int    `json:"obj_key_gen,omitempty"`
	ObjKeyName string `json:"obj_key_name,omitempty"`
	// ObjKeySecret: the key's MinIO secret. Store only (0600); never in
	// Snapshot, never in desired.json.
	ObjKeySecret string `json:"obj_key_secret,omitempty"`
	// ObjKeyState: "" (none), minting or active.
	ObjKeyState string `json:"obj_key_state,omitempty"`
	// ObjKeyNeedsSecret: keyd created the key but its secret never arrived;
	// desired.json asks keyd to re-create it.
	ObjKeyNeedsSecret bool `json:"obj_key_needs_secret,omitempty"`
	// ObjKeyMintSeq: the desired.json seq that asked for the key (or for
	// its re-creation); 0 = not yet written.
	ObjKeyMintSeq int64 `json:"obj_key_mint_seq,omitempty"`
	// ObjKeyRemoving: keys of this Mac that the Manager stopped asking for
	// and keyd has not yet reported gone. Forget waits for this to empty.
	ObjKeyRemoving []ObjKeyRemoval `json:"obj_key_removing,omitempty"`
	// ObjKeyStatus: the Principals page's key column (Snapshot only, never
	// stored): active, minting, recreating, removing, or shared (approved
	// before per-Mac keys; still on the shared key).
	ObjKeyStatus string `json:"obj_key_status,omitempty"`
	// ObjKeyInUse: the key the Mac last reported its mount uses
	// (EnrollRequest.ObjectKeyInUse; per-Mac keys on). Self-reported.
	ObjKeyInUse string `json:"obj_key_in_use,omitempty"`
	// ObjKeyOrphans (Snapshot only): keys of this Mac that keyd reports and
	// the store does not know (a restored store). They work at MinIO until
	// the admin removes them.
	ObjKeyOrphans []string `json:"obj_key_orphans,omitempty"`
}

// PrincipalsSnapshot is the admin view (no hashes).
type PrincipalsSnapshot struct {
	Principals       []MacPrincipal `json:"principals"`
	AutoApproveUntil int64          `json:"auto_approve_until_ms"`
	AutoApproveMax   int            `json:"auto_approve_max"`
	AutoApproveUsed  int            `json:"auto_approve_used"`
	NeedsReview      int            `json:"needs_review"`
	ProxyDir         bool           `json:"proxy_configured"`
	ReloadRequested  string         `json:"reload_requested_sha256,omitempty"`
	ReloadAcked      string         `json:"reload_acked_sha256,omitempty"`
	// ReloadApplied distinguishes the latest applied request from a live
	// proxy still processing its short, bounded backlog.
	ReloadApplied bool `json:"reload_applied"`
	// ProxyLive: reload.applied repeats a reload.request line this Manager wrote
	// and reload.heartbeat is fresh (proxyStateLocked).
	ProxyLive bool `json:"proxy_live"`
	// ProxyProblem explains ProxyLive=false ("" when live or unconfigured).
	ProxyProblem string `json:"proxy_problem,omitempty"`
	// PerMacKeys: JM_OBJ_PER_MAC=1. KeydProblem explains why minio-keyd is
	// not delivering keys ("" when it looks healthy or the mode is off).
	PerMacKeys  bool   `json:"per_mac_keys,omitempty"`
	KeydProblem string `json:"keyd_problem,omitempty"`
	// OrphanKeys: keys keyd reports that the store does not know, and
	// orphan removals not yet confirmed (per-Mac keys on).
	OrphanKeys []OrphanObjKey `json:"orphan_keys,omitempty"`
	// PerMacKeysLeft (per-Mac keys OFF): per-Mac storage keys the store
	// still records (current, being removed, orphan). With the mode off the
	// Manager does not remove them, so a revoked Mac's key keeps working.
	PerMacKeysLeft int `json:"per_mac_keys_left,omitempty"`
}

func (s PrincipalsSnapshot) byName(name string) *MacPrincipal {
	for i := range s.Principals {
		if s.Principals[i].Name == name {
			return &s.Principals[i]
		}
	}
	return nil
}

type principalStoreFile struct {
	Version int `json:"version"`
	// ManagerID: see PrincipalStore.ManagerID. Minted once and kept in its
	// own file (managerIDPath) as well as here, so restoring either file
	// alone keeps it.
	ManagerID        string         `json:"manager_id,omitempty"`
	AutoApproveUntil int64          `json:"auto_approve_until_ms"`
	AutoApproveMax   int            `json:"auto_approve_max,omitempty"`
	AutoApproveUsed  int            `json:"auto_approve_used,omitempty"`
	AutoApprovePerIP map[string]int `json:"auto_approve_per_ip,omitempty"`
	Principals       []MacPrincipal `json:"principals"`
	// Per-Mac object keys: the last desired.json seq, and the highest key
	// generation ever used per principal (kept after Forget and while the
	// mode is off, so a key name is never reused).
	ObjKeysSeq int64          `json:"obj_keys_seq,omitempty"`
	ObjKeyGens map[string]int `json:"obj_key_gens,omitempty"`
	// Orphan keys keyd last reported, and orphan removals the admin
	// confirmed that keyd has not yet reported done.
	ObjKeyOrphans        []string        `json:"obj_key_orphans,omitempty"`
	ObjKeyOrphanRemoving []ObjKeyRemoval `json:"obj_key_orphan_removing,omitempty"`
}

// ProxySignaler asks the proxy to reload the principals file whose sha256
// is given.
type ProxySignaler interface {
	Reload(sha string) error
}

// fileSignaler writes reload.request in the shared directory; the respproxy
// proxy polls requests and acknowledges only after installing the named file.
type fileSignaler struct{ dir string }

// The request line is "<sha256> <unix ms> <nonce>". The proxy echoes the
// whole line into reload.applied, and proxyLiveLocked compares whole lines: the
// sha256 alone is the same for every request about an unchanged file, so a
// reload.applied left behind by a proxy that has since stopped would otherwise
// match the request a restarted Manager writes (review 2 finding F1).
func (f fileSignaler) Reload(sha string) error {
	_, err := f.reloadLine(sha)
	return err
}

// reloadLine writes the request and returns its line (requestLine form).
func (f fileSignaler) reloadLine(sha string) (string, error) {
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	line := sha + " " + strconv.FormatInt(time.Now().UnixMilli(), 10) + " " + hex.EncodeToString(nonce)
	return line, writeFileAtomic0600(filepath.Join(f.dir, reloadRequestName), []byte(line+"\n"))
}

// PrincipalStoreOptions configures a PrincipalStore.
type PrincipalStoreOptions struct {
	StatePath  string // Manager-private enrollment store (0600)
	ProxyDir   string // shared with respproxy; empty = enrollment only, no proxy file
	Now        func() time.Time
	Signal     ProxySignaler // nil + ProxyDir set = reload.request file
	MaxPending int
	// ObjectKey returns the jm-client object key for approved Macs; nil =
	// none (responses then carry no object key). Unused when PerMacKeys.
	ObjectKey func() (accessKey, secretKey string)
	// PerMacKeys: JM_OBJ_PER_MAC=1. Each approved Mac gets its own MinIO
	// key from minio-keyd through ObjKeysDir (objkeys.go); the shared
	// ObjectKey is never handed out.
	PerMacKeys bool
	ObjKeysDir string
}

// PrincipalStore holds Mac enrollments and renders the proxy's files.
type PrincipalStore struct {
	mu        sync.Mutex
	opts      PrincipalStoreOptions
	recs      map[string]*MacPrincipal // by principal name
	autoUntil time.Time
	autoMax   int            // approvals the open window may still grant in total
	autoUsed  int            // approvals granted by the current window
	autoPerIP map[string]int // approvals granted by the current window, per source IP

	// Abuse bounds for the unauthenticated route (in memory).
	reqLimit  *ipLimiter    // every enroll/poll request
	hashLimit *ipLimiter    // requests that cost an argon2id hash
	hashSem   chan struct{} // concurrent argon2id hashes (19 MiB each)

	// managerID: ManagerID.
	managerID string
	// storeManagerID: the ID the store file held when loaded ("" for a
	// store from before IDs).
	storeManagerID string
	// issued: the reload.request lines this process wrote (newest last,
	// bounded). An ack counts only when it repeats one of them, so an ack
	// left by a proxy before this Manager started never counts (F1).
	issued []issuedRequest
	// ackSeen, ackSeenAt: the reload.applied line last read, and when this
	// process first read it (proxyOutdatedGrace).
	ackSeen   string
	ackSeenAt time.Time

	// objk: per-Mac object keys (nil = off). keepObjSeq/keepObjGens carry
	// the stored values through a run with the mode off.
	objk                *objKeys
	keepObjSeq          int64
	keepObjGens         map[string]int
	keepObjOrphans      []string
	keepObjOrphanRemove []ObjKeyRemoval
}

// issuedRequest is one reload.request line this process wrote and when.
type issuedRequest struct {
	line string
	at   time.Time
}

// maxIssuedRequests bounds PrincipalStore.issued.
const maxIssuedRequests = 64

// managerIDRe: 16 random bytes, lower-case hex.
var managerIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// managerIDPath is the file that keeps the Manager ID apart from the
// principal store, beside it: <store name without extension>.manager-id.
// Restoring the store from a backup taken before the ID existed must not
// mint a new ID (review 4 E2): every Mac would then see a different Manager.
func managerIDPath(statePath string) string {
	return strings.TrimSuffix(statePath, filepath.Ext(statePath)) + ".manager-id"
}

// loadManagerID settles the Manager ID: a valid ID in the store wins (the
// store's enrollments were made under it), else the ID file (a store
// restored from before IDs), else a new one. The result is written to both,
// so either one alone keeps it. Store first (review 5 X3): a store copied
// onto a Manager that already minted its own ID file must keep the ID its
// Macs are bound to.
func (s *PrincipalStore) loadManagerID() error {
	idPath := managerIDPath(s.opts.StatePath)
	fileID := ""
	b, err := os.ReadFile(idPath)
	switch {
	case err == nil:
		if id := strings.TrimSpace(string(b)); managerIDRe.MatchString(id) {
			fileID = id
		}
	case errors.Is(err, os.ErrNotExist):
	default:
		return fmt.Errorf("principal store: read the Manager ID %s: %w", idPath, err)
	}
	s.managerID = s.storeManagerID
	if s.managerID == "" {
		s.managerID = fileID // a store from before IDs, or one restored without its ID
	}
	if s.managerID == "" {
		nb := make([]byte, 16)
		if _, err := rand.Read(nb); err != nil {
			return err
		}
		s.managerID = hex.EncodeToString(nb)
	}
	if fileID != s.managerID {
		if err := os.MkdirAll(filepath.Dir(idPath), 0o700); err != nil {
			return err
		}
		if err := writeFileAtomic0600(idPath, []byte(s.managerID+"\n")); err != nil {
			return fmt.Errorf("principal store: persist the Manager ID: %w", err)
		}
	}
	if s.storeManagerID != s.managerID {
		s.mu.Lock()
		err := s.saveStoreLocked()
		s.mu.Unlock()
		if err != nil {
			return fmt.Errorf("principal store: persist the Manager ID: %w", err)
		}
	}
	return nil
}

// ManagerID is this Manager's random identity: minted on first start,
// persisted in its own file and in the enrollment store, sent with every
// enroll and poll answer.
// A Mac binds its enrollment to it, so a respelled address of this Manager
// keeps the Mac's secret and a different Manager at a reused address gets a
// fresh request instead. It is NOT a secret and proves nothing against an
// impersonator who has read it; only TLS with a pinned certificate does.
func (s *PrincipalStore) ManagerID() string { return s.managerID }

// Enrollment abuse bounds. An argon2id hash costs ~19 MiB and tens of
// milliseconds; the review measured 200 concurrent polls at 3.8 GiB.
const (
	maxConcurrentHashes   = 2
	maxPendingPerIP       = 4
	maxAutoApprovePerIP   = 2
	defaultAutoApproveMax = 20
	maxAutoApproveMax     = 200
)

// hashWaitTimeout bounds how long an enroll waits for a hashing slot
// (a variable for tests).
var hashWaitTimeout = 3 * time.Second

// hashSecret is the argon2id step (a variable so tests can count it).
var hashSecret = hashSecretPHC

// verifySecret is the argon2id check of a re-enroll (a variable for tests).
var verifySecret = verifySecretPHC

// ipLimiter is a per-source-IP token bucket.
type ipLimiter struct {
	mu      sync.Mutex
	rate    float64 // tokens per second
	burst   float64
	buckets map[string]*ipBucket
	now     func() time.Time
}

type ipBucket struct {
	tokens float64
	last   time.Time
}

func newIPLimiter(perMinute int, now func() time.Time) *ipLimiter {
	return &ipLimiter{rate: float64(perMinute) / 60, burst: float64(perMinute), buckets: map[string]*ipBucket{}, now: now}
}

func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	b := l.buckets[ip]
	if b == nil {
		if len(l.buckets) > 4096 { // bound memory: forget idle sources
			for k, v := range l.buckets {
				if now.Sub(v.last) > 10*time.Minute {
					delete(l.buckets, k)
				}
			}
		}
		b = &ipBucket{tokens: l.burst, last: now}
		l.buckets[ip] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// hashSlot bounds argon2id work: per-IP budget first, then a global
// semaphore with a short wait.
func (s *PrincipalStore) hashSlot(ip string) (func(), error) {
	if !s.hashLimit.allow(ip) {
		return nil, ErrEnrollRateLimited
	}
	t := time.NewTimer(hashWaitTimeout)
	defer t.Stop()
	select {
	case s.hashSem <- struct{}{}:
		return func() { <-s.hashSem }, nil
	case <-t.C:
		return nil, ErrEnrollBusy
	}
}

func newPollToken() (token, sha string, err error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	token = hex.EncodeToString(b)
	return token, sha256Hex(token), nil
}

// NewPrincipalStore loads the store and reconciles the proxy file with it.
func NewPrincipalStore(opts PrincipalStoreOptions) (*PrincipalStore, error) {
	if opts.StatePath == "" {
		return nil, errors.New("principal store needs a state path")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = defaultMaxPending
	}
	if opts.ObjectKey == nil {
		opts.ObjectKey = func() (string, string) { return "", "" }
	}
	if opts.Signal == nil && opts.ProxyDir != "" {
		opts.Signal = fileSignaler{dir: opts.ProxyDir}
	}
	s := &PrincipalStore{opts: opts, recs: map[string]*MacPrincipal{}, autoPerIP: map[string]int{},
		reqLimit: newIPLimiter(30, opts.Now), hashLimit: newIPLimiter(6, opts.Now),
		hashSem: make(chan struct{}, maxConcurrentHashes)}
	b, err := os.ReadFile(opts.StatePath)
	switch {
	case err == nil:
		var f principalStoreFile
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("principal store %s: %w", opts.StatePath, err)
		}
		for i := range f.Principals {
			p := f.Principals[i]
			if !macNameRe.MatchString(p.Name) {
				return nil, fmt.Errorf("principal store: bad name %q", p.Name)
			}
			s.recs[p.Name] = &p
		}
		if f.AutoApproveUntil > 0 {
			s.autoUntil = time.UnixMilli(f.AutoApproveUntil)
		}
		s.autoMax, s.autoUsed = f.AutoApproveMax, f.AutoApproveUsed
		if managerIDRe.MatchString(f.ManagerID) {
			s.storeManagerID = f.ManagerID
		}
		for ip, n := range f.AutoApprovePerIP {
			s.autoPerIP[ip] = n
		}
		s.keepObjSeq, s.keepObjGens = f.ObjKeysSeq, f.ObjKeyGens
		s.keepObjOrphans, s.keepObjOrphanRemove = f.ObjKeyOrphans, f.ObjKeyOrphanRemoving
	case errors.Is(err, os.ErrNotExist):
	default:
		return nil, err
	}
	if opts.PerMacKeys {
		s.objk = &objKeys{dir: opts.ObjKeysDir, seq: s.keepObjSeq, gens: map[string]int{},
			orphans: s.keepObjOrphans, orphanRemoving: s.keepObjOrphanRemove}
		for k, v := range s.keepObjGens {
			s.objk.gens[k] = v
		}
		for _, r := range s.recs {
			if r.ObjKeyName != "" || len(r.ObjKeyRemoving) != 0 {
				s.objk.keyRefs = true
			}
			if r.ObjKeyGen > s.objk.gens[r.Name] {
				s.objk.gens[r.Name] = r.ObjKeyGen
			}
		}
		// A store restored from an older copy: rise past every generation
		// this Manager ever asked keyd for.
		if err := s.objk.loadGensFile(); err != nil {
			return nil, fmt.Errorf("storage-key generation history: %w", err)
		}
	}
	if err := s.loadManagerID(); err != nil {
		return nil, err
	}
	if opts.ProxyDir != "" {
		s.mu.Lock()
		err := s.writeProxyFilesLocked()
		s.mu.Unlock()
		if err != nil {
			return nil, fmt.Errorf("reconcile proxy principals: %w", err)
		}
	}
	if s.objk != nil {
		// The store is the source of truth: rewrite desired.json from it
		// (a Manager that stopped after persisting a new key but before
		// writing desired.json converges here).
		s.mu.Lock()
		err := s.reconcileObjKeysLocked(true)
		s.mu.Unlock()
		if err != nil {
			log.Printf("manager: objkeys: start-up reconcile: %v", err)
		}
	}
	return s, nil
}

// An auto-admitted Mac waiting for its storage key (AutoApprovePending)
// counts toward the window's caps, not the pending caps.
func (s *PrincipalStore) pendingCountLocked() int {
	n := 0
	for _, r := range s.recs {
		if r.Status == PrincipalPending && !r.AutoApprovePending {
			n++
		}
	}
	return n
}

func (s *PrincipalStore) pendingFromIPLocked(ip string) int {
	n := 0
	for _, r := range s.recs {
		if r.Status == PrincipalPending && !r.AutoApprovePending && r.SourceIP == ip {
			n++
		}
	}
	return n
}

// autoApprovableLocked reports whether the open window may approve one
// more Mac from ip: the window is open, its total cap is not used up, and
// ip has not used its per-address share.
func (s *PrincipalStore) autoApprovableLocked(now time.Time, ip string) bool {
	return now.Before(s.autoUntil) && s.autoUsed < s.autoMax && s.autoPerIP[ip] < maxAutoApprovePerIP
}

// Enroll records a request, or answers a poll for an existing one.
//
// Abuse bounds on this unauthenticated path, cheapest first: a per-IP
// request budget; poll-token polls (one sha256); the pending caps (global
// and per IP) BEFORE any hashing; then a per-IP hashing budget and a global
// semaphore around every argon2id.
func (s *PrincipalStore) Enroll(req EnrollRequest, sourceIP string) (EnrollResponse, error) {
	req.InstanceID = strings.TrimSpace(req.InstanceID)
	if !instanceIDRe.MatchString(req.InstanceID) {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	req.PreviousInstanceID = strings.TrimSpace(req.PreviousInstanceID)
	if req.PreviousInstanceID != "" && (!instanceIDRe.MatchString(req.PreviousInstanceID) || strings.EqualFold(req.PreviousInstanceID, req.InstanceID)) {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	if req.DeviceDigest != "" && !deviceDigestRe.MatchString(req.DeviceDigest) {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	name := macPrincipalName(req.InstanceID)
	if req.ObjectKeyInUse != "" && !s.validObjectKeyInUse(req.ObjectKeyInUse, name) {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	if !s.reqLimit.allow(sourceIP) {
		return EnrollResponse{}, ErrEnrollRateLimited
	}

	if req.Secret == "" {
		// Poll by token.
		if !pollTokenRe.MatchString(req.PollToken) {
			return EnrollResponse{}, ErrEnrollInvalid
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		rec := s.recs[name]
		if rec == nil || rec.PollTokenSHA256 == "" ||
			subtle.ConstantTimeCompare([]byte(sha256Hex(req.PollToken)), []byte(rec.PollTokenSHA256)) != 1 {
			return EnrollResponse{}, ErrEnrollBadToken
		}
		s.noteDeviceLocked(rec, req.DeviceDigest)
		s.noteInUseLocked(s.recs[name], req.ObjectKeyInUse)
		return s.withObjectKey(EnrollResponse{Status: rec.Status, Principal: name, Fingerprint: rec.Fingerprint}, s.recs[name]), nil
	}

	if !secretRe.MatchString(req.Secret) {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	host := cleanText(req.Host, 64)
	if host == "" {
		return EnrollResponse{}, ErrEnrollInvalid
	}
	fp := enrollFingerprint(req.Secret)

	s.mu.Lock()
	existing := s.recs[name]
	if existing == nil && !s.autoApprovableLocked(s.opts.Now(), sourceIP) {
		// This request would be pending: refuse over the caps before
		// spending any hashing work on it.
		if s.pendingCountLocked() >= s.opts.MaxPending || s.pendingFromIPLocked(sourceIP) >= maxPendingPerIP {
			s.mu.Unlock()
			return EnrollResponse{}, ErrEnrollTooMany
		}
	}
	s.mu.Unlock()

	release, err := s.hashSlot(sourceIP)
	if err != nil {
		return EnrollResponse{}, err
	}
	defer release()

	if existing != nil {
		// Re-enroll with the secret (the Mac lost its poll token). The
		// secret is the only proof of identity this route has: a mismatch
		// never changes anything.
		if !strings.EqualFold(existing.InstanceID, req.InstanceID) || !verifySecret(existing.Argon2id, req.Secret) {
			return EnrollResponse{}, ErrEnrollConflict
		}
		token, tokenSHA, err := newPollToken()
		if err != nil {
			return EnrollResponse{}, err
		}
		var resp EnrollResponse
		err = s.transition(name, func(p *MacPrincipal) (bool, error) {
			p.PollTokenSHA256 = tokenSHA
			markDeviceMismatch(p, req.DeviceDigest)
			if s.objk != nil && req.ObjectKeyInUse != "" {
				p.ObjKeyInUse = req.ObjectKeyInUse
			}
			resp = s.withObjectKey(EnrollResponse{Status: p.Status, Principal: name, Fingerprint: p.Fingerprint, PollToken: token}, p)
			return false, nil
		})
		return resp, err
	}

	phc, err := hashSecret(req.Secret)
	if err != nil {
		return EnrollResponse{}, err
	}
	token, tokenSHA, err := newPollToken()
	if err != nil {
		return EnrollResponse{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recs[name] != nil { // raced with another request for the same instance
		return EnrollResponse{}, ErrEnrollConflict
	}
	now := s.opts.Now()
	rec := &MacPrincipal{
		Name: name, InstanceID: strings.ToUpper(req.InstanceID), Host: host, Label: cleanText(req.Label, 128),
		Status: PrincipalPending, Argon2id: phc, RedisSHA256: sha256Hex(req.Secret), Fingerprint: fp,
		SourceIP: sourceIP, RequestedAt: now.UnixMilli(), PollTokenSHA256: tokenSHA,
		PreviousInstanceID: strings.ToUpper(req.PreviousInstanceID), DeviceDigest: req.DeviceDigest,
	}
	if s.objk != nil {
		rec.ObjKeyInUse = req.ObjectKeyInUse
	}
	auto := s.autoApprovableLocked(now, sourceIP)
	if auto && s.objk != nil {
		// Enroll holds s.mu and must not wait for keyd: the Mac stays
		// pending, and the reconciler approves it once its key is active.
		if err := s.startObjKeyMintLocked(rec); err != nil {
			return EnrollResponse{}, err
		}
		rec.AutoApprovePending = true
		s.autoUsed++
		s.autoPerIP[sourceIP]++
	} else if auto {
		rec.Status = PrincipalApproved
		rec.DecidedAt = now.UnixMilli()
		rec.AutoApproved = true
		s.autoUsed++
		s.autoPerIP[sourceIP]++
	} else if s.pendingCountLocked() >= s.opts.MaxPending || s.pendingFromIPLocked(sourceIP) >= maxPendingPerIP {
		return EnrollResponse{}, ErrEnrollTooMany
	}
	s.recs[name] = rec
	err = s.commitLocked(rec.Status == PrincipalApproved)
	if err == nil && rec.AutoApprovePending {
		app, _ := s.objk.readApplied()
		err = s.writeObjKeysDesiredLocked(app)
	}
	if err != nil {
		delete(s.recs, name)
		if auto {
			s.autoUsed--
			s.autoPerIP[sourceIP]--
		}
		if rec.AutoApprovePending {
			// Preserve the skipped generation: its write-ahead floor may
			// already be durable even if enrollment rolls back.
			// The store may already hold the record (desired.json failed
			// after the save): persist the rollback.
			if serr := s.saveStoreLocked(); serr != nil {
				log.Printf("manager: principal %s: rollback could not be saved: %v", name, serr)
			}
		}
		return EnrollResponse{}, err
	}
	if rec.PreviousInstanceID != "" {
		log.Printf("manager: principal enrollment %s reports a migrated or cloned identity (previously %s)", name, macPrincipalName(rec.PreviousInstanceID))
	}
	log.Printf("manager: principal enrollment %s host=%q from %s: %s (auto=%v)", name, host, sourceIP, rec.Status, auto)
	if rec.AutoApprovePending {
		log.Printf("manager: principal %s admitted by the auto-approve window; approved once storage key %s is active", name, rec.ObjKeyName)
	}
	return s.withObjectKey(EnrollResponse{Status: rec.Status, Principal: name, Fingerprint: fp, PollToken: token}, rec), nil
}

// markDeviceMismatch flags p when digest is a device digest other than the
// one p was first seen with. It reports whether p changed. A request with no
// digest (or a record from before digests were recorded) changes nothing.
func markDeviceMismatch(p *MacPrincipal, digest string) bool {
	if digest == "" || p.DeviceDigest == "" || digest == p.DeviceDigest || p.DeviceMismatch {
		return false
	}
	p.DeviceMismatch = true
	p.MismatchedDigest = digest
	log.Printf("manager: WARNING: principal %s was used from a second Mac (device digest differs from its enrollment); review it on the Principals page", p.Name)
	return true
}

// validObjectKeyInUse: v is an access-key ID a Mac may report as in use:
// one of principal's own per-Mac key names, or the shared key's name. Any
// other value (another Mac's key, anything secret-shaped) is refused.
func (s *PrincipalStore) validObjectKeyInUse(v, principal string) bool {
	if p, _, ok := splitObjKeyName(v); ok {
		return p == principal
	}
	if ak, _ := s.opts.ObjectKey(); ak != "" && v == ak {
		return true
	}
	return v == sharedObjectKeyName
}

// sharedObjectKeyName is the shared no-delete MinIO user (H-8B §2.1).
const sharedObjectKeyName = "jm-client"

// noteInUseLocked records the key a Mac reports its mount uses (per-Mac
// keys on; store file only). A failed save leaves the record unchanged.
func (s *PrincipalStore) noteInUseLocked(rec *MacPrincipal, inUse string) {
	if s.objk == nil || rec == nil || inUse == "" || inUse == rec.ObjKeyInUse {
		return
	}
	next := *rec
	next.ObjKeyInUse = inUse
	s.recs[rec.Name] = &next
	if err := s.saveStoreLocked(); err != nil {
		s.recs[rec.Name] = rec
		log.Printf("manager: principal %s in-use storage key could not be saved: %v", rec.Name, err)
	}
}

// noteDeviceLocked records a device mismatch seen on a poll. The flag is
// persisted (store file only; the proxy table does not change). A failed
// save leaves the record unflagged so a later poll tries again.
func (s *PrincipalStore) noteDeviceLocked(rec *MacPrincipal, digest string) {
	next := *rec
	if !markDeviceMismatch(&next, digest) {
		return
	}
	s.recs[rec.Name] = &next
	if err := s.saveStoreLocked(); err != nil {
		s.recs[rec.Name] = rec
		log.Printf("manager: principal %s device mismatch could not be saved: %v", rec.Name, err)
	}
}

// transition applies fn to a copy of the record and commits it; on any
// write failure the in-memory record is left unchanged, so a retry is
// honest and the proxy's table never disagrees with what the UI shows.
func (s *PrincipalStore) transition(name string, fn func(*MacPrincipal) (proxyChanged bool, err error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.recs[name]
	if !ok {
		return ErrPrincipalNotFound
	}
	next := *cur
	changed, err := fn(&next)
	if err != nil {
		return err
	}
	s.recs[name] = &next
	if err := s.commitLocked(changed); err != nil {
		s.recs[name] = cur
		return err
	}
	return nil
}

// proxyAckWait bounds how long Approve and SetAutoApproveWindow wait for the
// proxy to acknowledge a reload they requested (the respproxy entrypoint
// checks reload.request every 2 seconds). A variable for tests.
var proxyAckWait = 6 * time.Second

// proxyHeartbeatMaxAge: reload.heartbeat older than this (three entrypoint
// ticks of 2 s) means the handshake loop, and so jm-respproxy, stopped. A
// variable for tests.
var proxyHeartbeatMaxAge = 6 * time.Second

// proxyState is what the proxy files say about the proxy.
type proxyState int

const (
	proxyLive proxyState = iota
	// proxyNoAck: nothing acknowledged a request of this Manager (no proxy
	// yet: before the cutover, or a proxy that started after the request).
	proxyNoAck
	// proxyStopped: an acknowledged proxy whose heartbeat is stale.
	proxyStopped
	// proxyOutdated: a proxy image older than the heartbeat answers.
	proxyOutdated
	// proxyStalled: the heartbeat runs, but the proxy has not acknowledged
	// the requests written after its last ack for longer than proxyAckWait.
	proxyStalled
)

// proxyOutdatedGrace: how long a whole-line ack may stand without a newer
// heartbeat before it counts as an image from before the heartbeat. The
// proxy writes reload.applied and then, in the same tick, reload.heartbeat,
// so a heartbeat image leaves this state within milliseconds. A variable for
// tests.
var proxyOutdatedGrace = time.Second

// proxyStateLocked reads the handshake files.
//
// Live means ALL of:
//   - reload.applied repeats a reload.request line this Manager process wrote
//     (sha256, time and nonce: an ack left behind by a proxy that stopped
//     before this Manager started never matches, review 2 finding F1);
//   - reload.heartbeat was rewritten within proxyHeartbeatMaxAge, and is not
//     dated more than proxyHeartbeatMaxAge in the future (a proxy that
//     stopped while this Manager kept running, review 3 finding 3; a
//     future mtime from a clock step or a hand edit, review 4 A1);
//   - the ack is the LATEST request, or the oldest request written after
//     the acked one is younger than proxyAckWait.
//
// The ack need not repeat the latest request: every Approve writes a new
// request, and requiring the newest one made each approval invalidate the
// proof for the next, so approvals less than one tick apart were refused
// (review 3 finding 7). A live proxy acks the newest request at its next 2 s
// tick, so its backlog is never older than one tick. A proxy that keeps its
// heartbeat but stops acknowledging has a backlog that ages past
// proxyAckWait and is refused (review 4 A2).
func (s *PrincipalStore) proxyStateLocked() proxyState {
	return s.proxyStateForAckLocked(requestLine(filepath.Join(s.opts.ProxyDir, reloadAckName)))
}

func (s *PrincipalStore) issuedAckLocked(ack string) int {
	for i, r := range s.issued {
		if ack != "" && ack == r.line {
			return i
		}
	}
	return -1
}

func (s *PrincipalStore) proxyStateForAckLocked(ack string) proxyState {
	if s.opts.ProxyDir == "" {
		return proxyNoAck
	}
	acked := s.issuedAckLocked(ack)
	ours := acked >= 0
	legacy := requestLine(filepath.Join(s.opts.ProxyDir, legacyReloadAckName))
	if !ours && (s.issuedAckLocked(legacy) >= 0 || s.oldStyleAckLocked(legacy)) {
		return proxyOutdated // signal-only receipts never prove applied credentials
	}
	hb, err := os.Stat(filepath.Join(s.opts.ProxyDir, reloadHeartbeatName))
	if !ours && s.oldStyleAckLocked(ack) {
		// The two-field "sha256 seconds" ack of an entrypoint older than
		// a861a46: nothing newer writes it.
		return proxyOutdated
	}
	if ack != "" && ack != s.ackSeen {
		// An empty read (a writer that truncates before it writes) keeps
		// the line already seen and its time.
		s.ackSeen, s.ackSeenAt = ack, time.Now()
	}
	if ours && s.ackWithoutHeartbeatLocked(acked, hb, err) {
		// A whole-line ack that no heartbeat followed: the a861a46 image,
		// also after a heartbeat image ran and left a stale file (review 4
		// A3).
		return proxyOutdated
	}
	if err != nil {
		return proxyNoAck
	}
	if age := time.Since(hb.ModTime()); age > proxyHeartbeatMaxAge || age < -proxyHeartbeatMaxAge {
		if ours {
			return proxyStopped
		}
		return proxyNoAck
	}
	if !ours {
		return proxyNoAck
	}
	if acked == len(s.issued)-1 || time.Since(s.issued[acked+1].at) < proxyAckWait {
		return proxyLive
	}
	return proxyStalled
}

// ackWithoutHeartbeatLocked: the whole-line ack of issued[acked] has been
// read for longer than proxyOutdatedGrace (since this process first read
// it), and reload.heartbeat is missing or older
// than the request it acknowledges (by more than a second, for coarse file
// times). The proxy writes the heartbeat right after the ack in the
// same tick, so only an image without the heartbeat (a861a46) leaves an ack
// newer than the heartbeat. A proxy that stopped leaves an ack OLDER than
// its last heartbeat, so it reads as stopped, not outdated.
func (s *PrincipalStore) ackWithoutHeartbeatLocked(acked int, hb os.FileInfo, hbErr error) bool {
	if time.Since(s.ackSeenAt) < proxyOutdatedGrace {
		return false
	}
	if hbErr != nil {
		return true
	}
	return hb.ModTime().Before(s.issued[acked].at.Add(-time.Second))
}

// oldStyleAckLocked: ack is the two-field "sha256 seconds" line of an
// entrypoint older than a861a46, for a request this Manager wrote.
func (s *PrincipalStore) oldStyleAckLocked(ack string) bool {
	f := strings.Fields(ack)
	if len(f) != 2 {
		return false
	}
	for _, r := range s.issued {
		if strings.Fields(r.line)[0] == f[0] {
			return true
		}
	}
	return false
}

func (s *PrincipalStore) proxyLiveLocked() bool { return s.proxyStateLocked() == proxyLive }

// proxyStateErr is the refusal for a non-live state.
func proxyStateErr(st proxyState) error {
	switch st {
	case proxyOutdated:
		return ErrProxyImageOutdated
	case proxyStopped:
		return ErrProxyStopped
	case proxyStalled:
		return ErrProxyStalled
	}
	return ErrProxyNotLive
}

// requireProxyLive returns nil when the proxy is live. Otherwise it rewrites
// the proxy files (the same idempotent write NewPrincipalStore does, which
// also writes a fresh reload.request: a proxy started after the last request
// never answers that one) and waits up to proxyAckWait for the ack. An
// outdated proxy image is reported as soon as it answers.
func (s *PrincipalStore) requireProxyLive() error {
	s.mu.Lock()
	if s.proxyLiveLocked() {
		s.mu.Unlock()
		return nil
	}
	if s.opts.ProxyDir == "" {
		s.mu.Unlock()
		return fmt.Errorf("%w; no proxy directory is configured (JM_RESPPROXY_DIR)", ErrProxyNotLive)
	}
	err := s.writeProxyFilesLocked()
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("%w; re-requesting a reload failed: %v", ErrProxyNotLive, err)
	}
	deadline := time.Now().Add(proxyAckWait)
	for {
		s.mu.Lock()
		st := s.proxyStateLocked()
		s.mu.Unlock()
		if st == proxyLive {
			return nil
		}
		if st == proxyOutdated || !time.Now().Before(deadline) {
			return proxyStateErr(st)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// approveAfterKeyWait runs between Approve's key wait and its commit
// (tests: a revoke racing an approval).
var approveAfterKeyWait func(name string)

// Approve lets a pending (or revoked) Mac in. It refuses (ErrProxyNotLive)
// until the Redis proxy is live: before the cutover the approved Mac would
// switch to a principal the plain Redis refuses. Confirming an
// auto-approved Mac changes nothing at the proxy and is always allowed.
func (s *PrincipalStore) Approve(name string) error {
	s.mu.Lock()
	cur, ok := s.recs[name]
	needsProxy := ok && cur.Status != PrincipalApproved
	s.mu.Unlock()
	if !ok {
		return ErrPrincipalNotFound
	}
	if needsProxy {
		if err := s.requireProxyLive(); err != nil {
			return err
		}
		if s.objk != nil {
			// Per-Mac keys: approved only once the Mac's own key is ready
			// (design §5). keyd silent = 409, the Mac stays pending.
			if err := s.ensureObjKey(name); err != nil {
				return err
			}
			if approveAfterKeyWait != nil {
				approveAfterKeyWait(name)
			}
		}
	}
	err := s.transition(name, func(p *MacPrincipal) (bool, error) {
		if p.Status == PrincipalApproved {
			// Confirming an auto-approved Mac clears it from the review list.
			p.AutoApproved = false
			return false, nil
		}
		if s.objk != nil && !objKeyDeliverable(p) {
			// Invariant: approved => active key. Also covers a revoke or a
			// re-mint that raced the wait above.
			return false, fmt.Errorf("%w: %s has no active storage key", ErrObjectKeyNotReady, p.Name)
		}
		p.AutoApprovePending = false
		p.Status = PrincipalApproved
		p.DecidedAt = s.opts.Now().UnixMilli()
		p.AutoApproved = false
		return true, nil
	})
	if err == nil {
		log.Printf("manager: principal %s approved", name)
	}
	return err
}

// Revoke removes a Mac's credential from the proxy (pending requests are
// rejected the same way). The record stays so the same secret cannot
// silently re-enroll; Forget deletes it.
func (s *PrincipalStore) Revoke(name string) error {
	err := s.transition(name, func(p *MacPrincipal) (bool, error) {
		if p.Status == PrincipalRevoked {
			// A refused re-approval may have left a key being minted for a
			// revoked Mac: revoking again retires it.
			retireObjKey(p)
			return false, nil
		}
		was := p.Status
		p.Status = PrincipalRevoked
		p.AutoApproved = false
		p.AutoApprovePending = false
		p.DecidedAt = s.opts.Now().UnixMilli()
		// The storage key goes after the Redis login (which this commit
		// removes at once): keyd deletes it in the background, and Forget
		// waits for keyd to confirm.
		retireObjKey(p)
		return was == PrincipalApproved, nil
	})
	if err == nil {
		log.Printf("manager: principal %s revoked", name)
		if kerr := s.syncObjKeys(); kerr != nil {
			// The revoke and the key's removal are in the store; the
			// reconciler writes desired.json until it is on disk.
			log.Printf("manager: principal %s revoked, but desired.json could not be rewritten (retried by the reconciler): %v", name, kerr)
		}
	}
	return err
}

// Forget deletes a revoked record so that Mac may enroll afresh.
func (s *PrincipalStore) Forget(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.recs[name]
	if !ok {
		return ErrPrincipalNotFound
	}
	if cur.Status != PrincipalRevoked {
		return ErrPrincipalState
	}
	if cur.ObjKeyName != "" || len(cur.ObjKeyRemoving) > 0 {
		// The Mac's storage key may still work at MinIO; forgetting the
		// record would hide that.
		return ErrObjectKeyRemovalPending
	}
	delete(s.recs, name)
	if err := s.commitLocked(false); err != nil {
		s.recs[name] = cur
		return err
	}
	return nil
}

// SetAutoApproveWindow opens the window for d (capped at two weeks) with
// at most maxApprovals approvals in total (default 20, at most 200) and at
// most two per source address, or closes it when d <= 0. Opening resets the
// counters. It returns the end of the window. Opening is refused
// (ErrProxyNotLive) until the Redis proxy is live, for the same reason as
// Approve; closing is always allowed.
func (s *PrincipalStore) SetAutoApproveWindow(d time.Duration, maxApprovals ...int) (time.Time, error) {
	if d > 0 {
		if err := s.requireProxyLive(); err != nil {
			s.mu.Lock()
			until := s.autoUntil
			s.mu.Unlock()
			return until, err
		}
	}
	if d > maxAutoApproveWindow {
		d = maxAutoApproveWindow
	}
	limit := defaultAutoApproveMax
	if len(maxApprovals) > 0 && maxApprovals[0] > 0 {
		limit = maxApprovals[0]
	}
	if limit > maxAutoApproveMax {
		limit = maxAutoApproveMax
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	prevUntil, prevMax, prevUsed, prevPerIP := s.autoUntil, s.autoMax, s.autoUsed, s.autoPerIP
	var unPended map[string]*MacPrincipal // originals of the records changed below
	if d <= 0 {
		s.autoUntil = time.Time{}
		// Macs the window admitted that still wait for their storage key are
		// not auto-approved later: they wait for an admin.
		for name, r := range s.recs {
			if r.Status == PrincipalPending && r.AutoApprovePending {
				if unPended == nil {
					unPended = map[string]*MacPrincipal{}
				}
				unPended[name] = r
				next := *r
				next.AutoApprovePending = false
				s.recs[name] = &next
			}
		}
	} else {
		s.autoUntil = s.opts.Now().Add(d)
		s.autoMax, s.autoUsed, s.autoPerIP = limit, 0, map[string]int{}
	}
	if err := s.saveStoreLocked(); err != nil {
		s.autoUntil, s.autoMax, s.autoUsed, s.autoPerIP = prevUntil, prevMax, prevUsed, prevPerIP
		for name, r := range unPended {
			s.recs[name] = r
		}
		return prevUntil, err
	}
	for name := range unPended {
		log.Printf("manager: principal %s: auto-approve closed before its storage key was ready; it now waits for an admin", name)
	}
	log.Printf("manager: principal auto-approve window until %v (max %d approvals, %d per address)", s.autoUntil, s.autoMax, maxAutoApprovePerIP)
	return s.autoUntil, nil
}

// Snapshot returns the admin view, without hashes.
func (s *PrincipalStore) Snapshot() PrincipalsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(false)
}

func (s *PrincipalStore) snapshotLocked(withSecrets bool) PrincipalsSnapshot {
	out := PrincipalsSnapshot{ProxyDir: s.opts.ProxyDir != "", AutoApproveMax: s.autoMax, AutoApproveUsed: s.autoUsed}
	var orphansOf map[string][]string
	if !withSecrets {
		orphans, orphanRemoving := s.keepObjOrphans, s.keepObjOrphanRemove
		if s.objk != nil {
			orphans, orphanRemoving = s.objk.orphans, s.objk.orphanRemoving
			out.OrphanKeys = s.orphanObjKeysLocked(orphans, orphanRemoving)
		} else {
			out.PerMacKeysLeft = s.perMacKeysLeftLocked()
		}
		for _, n := range orphans {
			if p, _, ok := splitObjKeyName(n); ok {
				if orphansOf == nil {
					orphansOf = map[string][]string{}
				}
				orphansOf[p] = append(orphansOf[p], n)
			}
		}
	}
	if s.opts.Now().Before(s.autoUntil) {
		out.AutoApproveUntil = s.autoUntil.UnixMilli()
	}
	for _, r := range s.recs {
		c := *r
		if !withSecrets {
			c.Argon2id, c.RedisSHA256, c.PollTokenSHA256 = "", "", ""
			c.ObjKeySecret = ""
			c.ObjKeyStatus = s.objKeyStatusLocked(r)
			c.ObjKeyOrphans = orphansOf[r.Name]
		}
		if c.AutoApproved && c.Status == PrincipalApproved {
			out.NeedsReview++
		}
		out.Principals = append(out.Principals, c)
	}
	sort.Slice(out.Principals, func(i, j int) bool { return out.Principals[i].Name < out.Principals[j].Name })
	if s.opts.ProxyDir != "" {
		request := requestLine(filepath.Join(s.opts.ProxyDir, reloadRequestName))
		if fields := strings.Fields(request); len(fields) > 0 {
			out.ReloadRequested = fields[0]
		}
		ack := requestLine(filepath.Join(s.opts.ProxyDir, reloadAckName))
		acked := s.issuedAckLocked(ack)
		if acked >= 0 {
			out.ReloadAcked = strings.Fields(ack)[0]
		}
		st := s.proxyStateForAckLocked(ack)
		out.ProxyLive = st == proxyLive
		out.ReloadApplied = out.ProxyLive && acked == len(s.issued)-1 && acked >= 0 && ack == request
		if !out.ProxyLive {
			out.ProxyProblem = proxyProblemText(st)
		}
	}
	if s.objk != nil && !withSecrets {
		out.PerMacKeys = true
		out.KeydProblem = s.objKeydProblemLocked()
	}
	return out
}

// perMacKeysLeftLocked counts the per-Mac key names the store records
// (current, being removed, orphan, orphan being removed).
func (s *PrincipalStore) perMacKeysLeftLocked() int {
	n := len(s.keepObjOrphans) + len(s.keepObjOrphanRemove)
	for _, r := range s.recs {
		if r.ObjKeyName != "" {
			n++
		}
		n += len(r.ObjKeyRemoving)
	}
	return n
}

// requestLine is a reload.request or reload.applied line with its whitespace
// normalized ("" when absent or empty).
func requestLine(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.Join(strings.Fields(string(b)), " ")
}

func firstField(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

// commitLocked persists the store and, when the proxy-visible set changed
// (or always, cheaply, to self-heal), the proxy files.
func (s *PrincipalStore) commitLocked(proxyChanged bool) error {
	if s.opts.ProxyDir != "" && proxyChanged {
		if err := s.writeProxyFilesLocked(); err != nil {
			return err
		}
	}
	return s.saveStoreLocked()
}

func (s *PrincipalStore) saveStoreLocked() error {
	// Persist every increased generation floor before the store can name it.
	// A failed store write may skip a generation, which is safe; a restored
	// older store must never reuse a name that was already issued.
	if s.objk != nil {
		if err := s.objk.writeGensFile(); err != nil {
			return fmt.Errorf("persist storage-key generation floor: %w", err)
		}
	}
	f := principalStoreFile{Version: 1, ManagerID: s.managerID, AutoApproveMax: s.autoMax, AutoApproveUsed: s.autoUsed}
	if !s.autoUntil.IsZero() {
		f.AutoApproveUntil = s.autoUntil.UnixMilli()
	}
	if len(s.autoPerIP) > 0 {
		f.AutoApprovePerIP = s.autoPerIP
	}
	f.Principals = s.snapshotLocked(true).Principals
	f.ObjKeysSeq, f.ObjKeyGens = s.keepObjSeq, s.keepObjGens
	if s.objk != nil {
		f.ObjKeysSeq = s.objk.seq
		if len(s.objk.gens) > 0 {
			f.ObjKeyGens = s.objk.gens
		}
		f.ObjKeyOrphans, f.ObjKeyOrphanRemoving = s.objk.orphans, s.objk.orphanRemoving
	} else {
		f.ObjKeyOrphans, f.ObjKeyOrphanRemoving = s.keepObjOrphans, s.keepObjOrphanRemove
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.opts.StatePath), 0o700); err != nil {
		return err
	}
	return writeFileAtomic0600(s.opts.StatePath, append(b, '\n'))
}

// approvedLocked returns approved Macs sorted by name.
func (s *PrincipalStore) approvedLocked() []*MacPrincipal {
	var out []*MacPrincipal
	for _, r := range s.recs {
		if r.Status == PrincipalApproved {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// writeProxyFilesLocked rewrites principals.json (server entries kept
// verbatim, Mac entries = approved set) and rollback-macs.acl, then signals
// the proxy. It holds the shared flock the respproxy entrypoint also takes.
func (s *PrincipalStore) writeProxyFilesLocked() error {
	dir := s.opts.ProxyDir
	lf, err := os.OpenFile(filepath.Join(dir, principalsLockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()
	if err := syscall.Flock(int(lf.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	defer syscall.Flock(int(lf.Fd()), syscall.LOCK_UN)

	path := filepath.Join(dir, proxyPrincipalsName)
	var cur proxyPrincipalsFile
	prev, err := os.ReadFile(path)
	if err == nil {
		if err := json.Unmarshal(prev, &cur); err != nil {
			// Never replace a table we cannot read: that could drop the
			// server principals and take the whole stack offline.
			return fmt.Errorf("existing %s is not valid JSON: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var next proxyPrincipalsFile
	for _, e := range cur.Principals {
		if e.Kind != "mac" {
			next.Principals = append(next.Principals, e)
		}
	}
	var acl strings.Builder
	acl.WriteString("# Enrolled Macs as plain Redis ACL users for the H-8B severity-3 rollback.\n")
	acl.WriteString("# Written by juicemount-manager; read by rollout-compose.sh --proxy-rollback.\n")
	for _, r := range s.approvedLocked() {
		next.Principals = append(next.Principals, proxyPrincipalEntry{Name: r.Name, Kind: "mac", Host: r.Host, Argon2id: r.Argon2id})
		acl.WriteString(rollbackACLLine(r.Name, r.RedisSHA256))
	}
	if next.Principals == nil {
		next.Principals = []proxyPrincipalEntry{}
	}
	body, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	body = append(body, '\n')
	// principals.json first: it is what the proxy enforces. The rollback
	// fragment must never disagree with it, so if the fragment cannot be
	// written the principals file is put back and the change fails as a
	// whole.
	if err := writeFileAtomic0600(path, body); err != nil {
		return err
	}
	if err := writeFileAtomic0600(filepath.Join(dir, rollbackMacsName), []byte(acl.String())); err != nil {
		var undo error
		if prev != nil {
			undo = writeFileAtomic0600(path, prev)
		} else {
			undo = os.Remove(path)
		}
		if undo != nil {
			log.Printf("manager: CRITICAL: %s could not be restored after a failed rollback-fragment write: %v", path, undo)
		}
		return err
	}
	if s.opts.Signal != nil {
		sum := sha256.Sum256(body)
		sha := hex.EncodeToString(sum[:])
		var err error
		if fs, ok := s.opts.Signal.(fileSignaler); ok {
			// Only a line this process wrote can prove the proxy live.
			var line string
			if line, err = fs.reloadLine(sha); err == nil {
				s.issued = append(s.issued, issuedRequest{line: line, at: time.Now()})
				if len(s.issued) > maxIssuedRequests {
					s.issued = append([]issuedRequest(nil), s.issued[len(s.issued)-maxIssuedRequests:]...)
				}
			}
		} else {
			err = s.opts.Signal.Reload(sha)
		}
		if err != nil {
			// The file is already correct; the proxy picks it up at its next
			// signal or restart. Report, do not undo.
			log.Printf("manager: principals written but proxy reload signal failed: %v", err)
		}
	}
	return nil
}

func rollbackACLLine(name, sha string) string {
	return "user " + name + " on #" + sha + " ~* &* +@all\n"
}

// RollbackUser is a server principal for RenderRollbackACL.
type RollbackUser struct{ Name, Secret string }

// RenderRollbackACL renders plan §3 rollback step 3: every principal as a
// plain Redis ACL user with the same name and secret (sha256 form, never
// plaintext) and +@all. Pending and revoked Macs are excluded.
// rollout-compose.sh --proxy-rollback produces the same lines from
// server/.env plus the Manager's rollback-macs.acl fragment.
func RenderRollbackACL(server []RollbackUser, macs PrincipalsSnapshot) string {
	var b strings.Builder
	for _, u := range server {
		b.WriteString(rollbackACLLine(u.Name, sha256Hex(u.Secret)))
	}
	for _, p := range macs.Principals {
		if p.Status == PrincipalApproved && p.RedisSHA256 != "" {
			b.WriteString(rollbackACLLine(p.Name, p.RedisSHA256))
		}
	}
	return b.String()
}

// RollbackSnapshot is the store including the rollback hashes (never served
// over HTTP).
func (s *PrincipalStore) RollbackSnapshot() PrincipalsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(true)
}

// atomicWriteHook replaces the body write in tests (failure injection).
var atomicWriteHook func(f *os.File, data []byte) error

// Floor-only directory hooks allow tests to inject failures after rename.
// Production uses os.Open and File.Sync when the hooks are nil.
var atomicFloorDirOpenHook func(dir string) (*os.File, error)
var atomicFloorDirSyncHook func(dir *os.File) error

// writeFileAtomic0600 preserves the existing best-effort directory sync
// contract for its other callers. The generation floor uses the strict form.
func writeFileAtomic0600(path string, data []byte) error {
	return writeFileAtomic0600WithDurability(path, data, false)
}

func writeFileAtomic0600Durable(path string, data []byte) error {
	return writeFileAtomic0600WithDurability(path, data, true)
}

// The temporary file is synced before rename. After rename, the new file
// may already be visible even if opening or syncing its parent fails.
func writeFileAtomic0600WithDurability(path string, data []byte, strict bool) (err error) {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	// CreateTemp uses 0600 already; chmod anyway so the umask and any
	// platform default can never widen it.
	if err = tmp.Chmod(0o600); err != nil {
		return err
	}
	if atomicWriteHook != nil {
		err = atomicWriteHook(tmp, data)
	} else {
		_, err = tmp.Write(data)
	}
	if err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	openDir := os.Open
	if strict && atomicFloorDirOpenHook != nil {
		openDir = atomicFloorDirOpenHook
	}
	d, derr := openDir(dir)
	if derr != nil {
		if strict {
			return fmt.Errorf("open parent directory after rename: %w", derr)
		}
		return nil
	}
	syncDir := (*os.File).Sync
	if strict && atomicFloorDirSyncHook != nil {
		syncDir = atomicFloorDirSyncHook
	}
	serr := syncDir(d)
	cerr := d.Close()
	if strict {
		if serr != nil {
			return fmt.Errorf("sync parent directory after rename: %w", serr)
		}
		if cerr != nil {
			return fmt.Errorf("close parent directory after sync: %w", cerr)
		}
	}
	return nil
}

// ---- HTTP -----------------------------------------------------------------

type principalsAPI struct {
	store *PrincipalStore
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (p *principalsAPI) ready(w http.ResponseWriter) bool {
	if p == nil || p.store == nil {
		http.Error(w, "principal enrollment is not configured (set JM_STATE_FILE or JM_PRINCIPALS_STATE)", http.StatusServiceUnavailable)
		return false
	}
	return true
}

// managerIDHeader carries PrincipalStore.ManagerID on every answer of the
// enroll route, including refusals (403, 409), so a Mac can tell which
// Manager refused it before it re-sends its secret.
const managerIDHeader = "X-JM-Manager-ID"

// handleEnroll is POST /api/principals/enroll. No admin key: the Mac proves
// itself only by its secret on later polls.
//
// GET answers {"manager_id": ...} and nothing else. A Mac whose Manager
// address changed spelling asks this first and sends its secret only when
// the ID is the one its enrollment is bound to.
func (p *principalsAPI) handleEnroll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "POST only (GET returns this Manager's ID)", http.StatusMethodNotAllowed)
		return
	}
	if !p.ready(w) {
		return
	}
	w.Header().Set(managerIDHeader, p.store.ManagerID())
	if r.Method == http.MethodGet {
		if !p.store.reqLimit.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "30")
			http.Error(w, "too many enrollment requests from this address; retry later", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, map[string]string{"manager_id": p.store.ManagerID()})
		return
	}
	var req EnrollRequest
	dec := json.NewDecoder(io.LimitReader(r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	resp, err := p.store.Enroll(req, clientIP(r))
	switch {
	case errors.Is(err, ErrEnrollInvalid):
		http.Error(w, "instance_id must be the app's instance UUID; send host + secret (32-256 URL-safe characters) to enroll, or poll_token to poll", http.StatusBadRequest)
		return
	case errors.Is(err, ErrEnrollConflict):
		http.Error(w, "this instance is already enrolled with a different secret; an admin must revoke and forget it first", http.StatusConflict)
		return
	case errors.Is(err, ErrEnrollTooMany):
		http.Error(w, "too many pending enrollment requests; ask an admin to review them", http.StatusTooManyRequests)
		return
	case errors.Is(err, ErrEnrollRateLimited):
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many enrollment requests from this address; retry later", http.StatusTooManyRequests)
		return
	case errors.Is(err, ErrEnrollBusy):
		w.Header().Set("Retry-After", "5")
		http.Error(w, "enrollment is busy; retry shortly", http.StatusServiceUnavailable)
		return
	case errors.Is(err, ErrEnrollBadToken):
		http.Error(w, "unknown instance or wrong poll token; enroll again with the secret", http.StatusForbidden)
		return
	case err != nil:
		log.Printf("manager: enrollment failed: %v", err)
		http.Error(w, "enrollment could not be recorded", http.StatusInternalServerError)
		return
	}
	code := http.StatusOK
	if resp.Status == PrincipalPending {
		code = http.StatusAccepted
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, resp)
}

func (p *principalsAPI) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if !p.ready(w) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, p.store.Snapshot())
}

// proxyNotLiveMessage is what the admin sees when Approve or opening the
// auto-approve window is refused before the cutover.
const proxyNotLiveMessage = "Not approved: the Redis proxy is not live yet. It has not acknowledged the current principals file " +
	"(reload.applied does not echo the latest reload.request). Approving a Mac before the proxy cutover would switch it to a login the " +
	"current Redis refuses. Finish the proxy cutover (jm-respproxy running with JM_RESPPROXY_DIR shared with the Manager), " +
	"then approve again."

// proxyOutdatedMessage: a proxy answers, but its image predates the reload
// applied-reload contract. The cutover itself is done.
const proxyOutdatedMessage = "Not approved: the jm-respproxy image is older than this Manager. It answers the reload handshake " +
	"without the required proof of applied credentials and a current heartbeat. Update the jm-respproxy image to one " +
	"that contains the commit in server/respproxy/min-gate-commit (rollout-compose.sh --cutover-proxy with the new digest), " +
	"then approve again."

// proxyStoppedMessage: the proxy acknowledged earlier but stopped.
const proxyStoppedMessage = "Not approved: the Redis proxy has stopped. Its reload heartbeat is more than 6 seconds old. " +
	"Start the jm-respproxy container again, then approve again."

// proxyStalledMessage: the heartbeat runs, but reloads are not acknowledged.
const proxyStalledMessage = "Not approved: the Redis proxy is running but has not acknowledged the principals file for more than 6 seconds. " +
	"Check the jm-respproxy container's log and restart it, then approve again."

// proxyRefusal is the 409 text for a refused Approve or window.
func proxyRefusal(err error) string {
	switch {
	case errors.Is(err, ErrProxyImageOutdated):
		return proxyOutdatedMessage
	case errors.Is(err, ErrProxyStopped):
		return proxyStoppedMessage
	case errors.Is(err, ErrProxyStalled):
		return proxyStalledMessage
	}
	return proxyNotLiveMessage
}

// proxyProblemText is the short reason the Principals page shows.
func proxyProblemText(st proxyState) string {
	switch st {
	case proxyOutdated:
		return "the jm-respproxy image predates the reload heartbeat; update the jm-respproxy image"
	case proxyStopped:
		return "the Redis proxy has stopped (its reload heartbeat is stale)"
	case proxyStalled:
		return "the Redis proxy runs but does not acknowledge reloads; check and restart the jm-respproxy container"
	}
	return "the Redis proxy has not acknowledged a reload from this Manager (proxy cutover not finished, or the proxy is not running)"
}

// objectKeyRefusal is the 409 text for an Approve that got no storage key.
func objectKeyRefusal(err error) string {
	return "Not approved: the storage key could not be created (minio-keyd not answering?). The Mac stays pending; " +
		"approve again once minio-keyd runs. Detail: " + strings.TrimPrefix(err.Error(), ErrObjectKeyNotReady.Error()+": ")
}

func (p *principalsAPI) handleDecision(op func(string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if !p.ready(w) {
			return
		}
		var body struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil || !macNameRe.MatchString(body.Name) {
			http.Error(w, "name must be a Mac principal (mac-<12 hex>)", http.StatusBadRequest)
			return
		}
		switch err := op(body.Name); {
		case errors.Is(err, ErrProxyNotLive):
			http.Error(w, proxyRefusal(err), http.StatusConflict)
		case errors.Is(err, ErrObjectKeyNotReady):
			http.Error(w, objectKeyRefusal(err), http.StatusConflict)
		case errors.Is(err, ErrPrincipalNotFound):
			http.Error(w, "no such principal", http.StatusNotFound)
		case errors.Is(err, ErrObjectKeyRemovalPending):
			http.Error(w, "Not forgotten: storage key removal not confirmed. minio-keyd has not yet reported this Mac's storage key "+
				"removed, so it may still work at the object store. Check that minio-keyd is running, then forget again.", http.StatusConflict)
		case errors.Is(err, ErrPrincipalState):
			http.Error(w, "only a revoked principal can be forgotten", http.StatusConflict)
		case err != nil:
			log.Printf("manager: principal %s update failed: %v", body.Name, err)
			http.Error(w, "the proxy principals file could not be written; nothing changed", http.StatusInternalServerError)
		default:
			writeJSON(w, http.StatusOK, p.store.Snapshot())
		}
	}
}

// handleOrphanKeysRemove is POST /api/principals/orphan-keys/remove
// {"names":[...]}: the admin's confirmation to remove orphan storage keys
// (keys minio-keyd reports that this Manager's store does not know).
func (p *principalsAPI) handleOrphanKeysRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !p.ready(w) {
		return
	}
	var body struct {
		Names []string `json:"names"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 256<<10))
	dec.DisallowUnknownFields()
	ok := dec.Decode(&body) == nil && len(body.Names) > 0
	for _, n := range body.Names {
		ok = ok && objKeyNameRe.MatchString(n)
	}
	if !ok {
		http.Error(w, "names must list one or more storage keys (mac-<12 hex>-<generation>)", http.StatusBadRequest)
		return
	}
	switch err := p.store.RemoveOrphanObjKeys(body.Names); {
	case errors.Is(err, ErrObjKeysOff):
		http.Error(w, orphanKeysOffMessage, http.StatusConflict)
	case errors.Is(err, ErrNotOrphanKey):
		http.Error(w, "Not removed: "+strings.TrimPrefix(err.Error(), ErrNotOrphanKey.Error()+": ")+" is not an orphan storage key now "+
			"(minio-keyd no longer reports it, or it belongs to a Mac this Manager knows). Nothing was removed. Reload the page and check the list again.",
			http.StatusConflict)
	case err != nil:
		log.Printf("manager: orphan key removal failed: %v", err)
		http.Error(w, "the removal could not be recorded; nothing changed", http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusOK, p.store.Snapshot())
	}
}

// orphanKeysOffMessage: the orphan action with per-Mac keys off.
const orphanKeysOffMessage = "Not removed: per-Mac storage keys are off (JM_OBJ_PER_MAC is not 1), so the Manager does not manage them. " +
	"Set JM_OBJ_PER_MAC=1 and restart the Manager; it then lists these keys and can remove them."

func (p *principalsAPI) handleAutoApprove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !p.ready(w) {
		return
	}
	var body struct {
		Hours        int `json:"hours"`
		MaxApprovals int `json:"max_approvals"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&body); err != nil || body.Hours < 0 || body.Hours > int(maxAutoApproveWindow/time.Hour) ||
		body.MaxApprovals < 0 || body.MaxApprovals > maxAutoApproveMax {
		http.Error(w, "hours must be 0 (close) to 336; max_approvals 0 (default 20) to 200", http.StatusBadRequest)
		return
	}
	if _, err := p.store.SetAutoApproveWindow(time.Duration(body.Hours)*time.Hour, body.MaxApprovals); err != nil {
		if errors.Is(err, ErrProxyNotLive) {
			http.Error(w, proxyRefusal(err), http.StatusConflict)
			return
		}
		http.Error(w, "could not persist the window", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, p.store.Snapshot())
}

// ---- wiring ---------------------------------------------------------------

// registerH8B wires the H-8 option B routes: principal enrollment, the
// leases admin view and the mode switch, and gives the JobManager a project
// leaser so sync and trash restore check out their target projects.
//
// Environment (all optional; each feature reports 503 when unset):
//
//	JM_RESPPROXY_DIR         shared principals directory with respproxy
//	JM_PRINCIPALS_STATE      enrollment store (default: <dir of JM_STATE_FILE>/principals-store.json)
//	JM_META_PASSWORD         the Manager principal's secret (JM_META is user@host)
//	JM_META_BG_PASSWORD      `bg` principal: gc, compact, fsck
//	JM_META_ADMIN_PASSWORD   `admin` principal: lease break, mode switch, juicefs config writes
func (a *API) registerH8B(mux *http.ServeMux, prefix string, cfg Config) {
	statePath := strings.TrimSpace(os.Getenv("JM_PRINCIPALS_STATE"))
	if statePath == "" && cfg.StateFile != "" {
		statePath = filepath.Join(filepath.Dir(cfg.StateFile), "principals-store.json")
	}
	pa := &principalsAPI{}
	if statePath != "" {
		perMac := parsePerMacFlag(os.Getenv("JM_OBJ_PER_MAC"))
		objDir := ""
		if perMac {
			objDir = strings.TrimSpace(os.Getenv("JM_OBJKEYS_DIR"))
			if objDir == "" {
				// Fail closed: approvals are refused with this reason; the
				// shared key is never handed out instead.
				log.Printf("manager: ERROR: JM_OBJ_PER_MAC=1 but JM_OBJKEYS_DIR is not set; no Mac can be approved until it is")
			}
		}
		st, err := NewPrincipalStore(PrincipalStoreOptions{
			StatePath:  statePath,
			ProxyDir:   strings.TrimSpace(os.Getenv("JM_RESPPROXY_DIR")),
			ObjectKey:  clientObjectKey,
			PerMacKeys: perMac,
			ObjKeysDir: objDir,
		})
		if err != nil {
			log.Printf("manager: principal enrollment unavailable: %v", err)
		} else {
			pa.store = st
			if perMac {
				log.Printf("manager: per-Mac object keys ON (minio-keyd directory %q)", objDir)
				stop := st.StartObjKeyReconciler()
				if onObjKeyReconcilerStart != nil {
					onObjKeyReconcilerStart(stop)
				}
			}
		}
	}
	mux.HandleFunc(prefix+"/api/principals/enroll", pa.handleEnroll) // no admin key: Macs enroll
	mux.HandleFunc(prefix+"/api/principals", a.auth(pa.handleList))
	if pa.store != nil {
		mux.HandleFunc(prefix+"/api/principals/approve", a.auth(pa.handleDecision(pa.store.Approve)))
		mux.HandleFunc(prefix+"/api/principals/revoke", a.auth(pa.handleDecision(pa.store.Revoke)))
		mux.HandleFunc(prefix+"/api/principals/forget", a.auth(pa.handleDecision(pa.store.Forget)))
		mux.HandleFunc(prefix+"/api/principals/orphan-keys/remove", a.auth(pa.handleOrphanKeysRemove))
	} else {
		unavailable := a.auth(func(w http.ResponseWriter, r *http.Request) { pa.ready(w) })
		for _, op := range []string{"approve", "revoke", "forget", "orphan-keys/remove"} {
			mux.HandleFunc(prefix+"/api/principals/"+op, unavailable)
		}
	}
	mux.HandleFunc(prefix+"/api/principals/auto-approve", a.auth(pa.handleAutoApprove))

	metaURL := cfg.MetaURL
	if metaURL == "" {
		metaURL = cfg.OverviewMetaURL
	}
	la := &leasesAdmin{}
	act := &activation{}
	if metaURL != "" {
		if c, err := NewRedisLeaseClient(metaURL, ""); err == nil {
			a.jobs.SetProjectLeaser(NewProjectLeaser(c))
		} else {
			log.Printf("manager: lease client unavailable: %v", err)
		}
		// The admin principal is needed for exactly three things: lease
		// break, the mode switch (jmlease:* is admin-only) and `juicefs
		// config` writes (trash-days). Unset = those three report 503.
		if pw := os.Getenv("JM_META_ADMIN_PASSWORD"); pw != "" {
			user := envOrDefault("JM_META_ADMIN_USER", "admin")
			if c, err := NewRedisLeaseClientAs(metaURL, user, pw); err == nil {
				la.client = c
				act.store = c
			} else {
				log.Printf("manager: lease admin client unavailable: %v", err)
			}
		}
	}
	mux.HandleFunc(prefix+"/api/leases", a.auth(la.handleLeases))
	mux.HandleFunc(prefix+"/api/leases/break", a.auth(la.handleLeaseBreak))
	mux.HandleFunc(prefix+"/api/leases/mode", a.auth(act.handleMode))
}

// ---- volume credentials ---------------------------------------------------
//
// H-8B: no secret in any process's argv. The Manager's own URL (JM_META,
// --meta) carries only user@host; the password comes from JM_META_PASSWORD.
// In-process Redis clients get it inlined (withEnvMetaPassword); every
// juicefs subprocess gets the user@host URL in argv and META_PASSWORD in its
// environment (juicefs reads it when the URL has none).
//
// Principals per task (plan §1.6, review 2026-10-06):
//   - self  (manager): sync, status, trash-days read
//   - bg    (JM_META_BG_PASSWORD): gc, gc --compact, fsck. delSlices,
//     sliceRef and trash/session cleanup are bg-only at the proxy.
//   - admin (JM_META_ADMIN_PASSWORD): `juicefs config` writes (the volume
//     `setting` key is admin-only), lease break, the mode switch.
// When a role's password is unset the URL's own principal is used, which is
// the pre-proxy behaviour.

type metaRole int

const (
	metaRoleSelf metaRole = iota
	metaRoleBG
	metaRoleAdmin
)

func parseMetaURL(raw string) (*url.URL, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	if !strings.Contains(raw, "://") {
		raw = "redis://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, false
	}
	return u, true
}

// withEnvMetaPassword inlines JM_META_PASSWORD into a URL that names a
// user but no password, for in-process Redis clients only.
func withEnvMetaPassword(raw string) string {
	pw := os.Getenv("JM_META_PASSWORD")
	u, ok := parseMetaURL(raw)
	if !ok || pw == "" || u.User == nil {
		return raw
	}
	if _, has := u.User.Password(); has {
		return raw
	}
	u.User = url.UserPassword(u.User.Username(), pw)
	return u.String()
}

// metaArgvEnv returns the URL to put in a juicefs argv (never a password)
// and the environment that carries the password for the given role.
func metaArgvEnv(raw string, role metaRole) (string, []string) {
	u, ok := parseMetaURL(raw)
	if !ok {
		return raw, nil
	}
	user, pw := "", ""
	if u.User != nil {
		user = u.User.Username()
		pw, _ = u.User.Password()
	}
	if pw == "" {
		pw = os.Getenv("JM_META_PASSWORD")
	}
	switch role {
	case metaRoleBG:
		if p := os.Getenv("JM_META_BG_PASSWORD"); p != "" {
			user, pw = envOrDefault("JM_META_BG_USER", "bg"), p
		}
	case metaRoleAdmin:
		if p := os.Getenv("JM_META_ADMIN_PASSWORD"); p != "" {
			user, pw = envOrDefault("JM_META_ADMIN_USER", "admin"), p
		}
	}
	if user != "" {
		u.User = url.User(user)
	} else {
		u.User = nil
	}
	var env []string
	if pw != "" {
		env = []string{"META_PASSWORD=" + pw}
	}
	return u.String(), append(env, objectStoreEnv()...)
}

// objectStoreEnv is the object key every juicefs subprocess of the Manager
// runs with (H-8B §2.1). The volume `setting` key holds no object key, and
// stock juicefs then reads MINIO_ACCESS_KEY / MINIO_SECRET_KEY from the
// environment. The Manager runs gc --delete and compact, so it gets the
// delete-capable jm-gc key, from JM_OBJ_GC_ACCESS_KEY/JM_OBJ_GC_SECRET_KEY.
// While `setting` still holds a key, juicefs ignores these.
func objectStoreEnv() []string {
	ak, sk := strings.TrimSpace(os.Getenv("JM_OBJ_GC_ACCESS_KEY")), os.Getenv("JM_OBJ_GC_SECRET_KEY")
	if ak == "" || sk == "" {
		return nil
	}
	return []string{"MINIO_ACCESS_KEY=" + ak, "MINIO_SECRET_KEY=" + sk}
}

// clientObjectKey is the no-delete jm-client key handed to approved Macs.
func clientObjectKey() (ak, sk string) {
	ak, sk = strings.TrimSpace(os.Getenv("JM_OBJ_CLIENT_ACCESS_KEY")), os.Getenv("JM_OBJ_CLIENT_SECRET_KEY")
	if ak == "" || sk == "" {
		return "", ""
	}
	return ak, sk
}

func envOrDefault(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}

// juicefsMetaCmd builds `juicefs <sub> <url> args...` for role with the
// password in the environment only.
func juicefsMetaCmd(ctx context.Context, bin, sub, metaURL string, role metaRole, args ...string) *exec.Cmd {
	argURL, env := metaArgvEnv(metaURL, role)
	argv := append([]string{sub, argURL}, args...)
	cmd := exec.CommandContext(ctx, bin, argv...)
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}
	return cmd
}
