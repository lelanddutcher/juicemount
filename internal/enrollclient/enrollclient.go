// Package enrollclient is the Mac side of H-8 option B enrollment
// (H8_OPTION_B_PLAN.md §1.6; Manager side internal/manager/principals.go).
//
// What it is. Under the per-project Redis proxy every Mac logs in as its own
// principal, mac-<12 hex of its instance ID>. The Mac gets that principal by
// asking the Manager (POST /api/principals/enroll) with a random secret it
// keeps; an admin approves the request in the Manager; the secret then
// becomes this Mac's Redis password, and the approved response also carries
// the volume's no-delete object-store key.
//
// Who runs it. The app, through the bridge export NFSServerEnrollStep: the
// app owns the Keychain, so it persists State between steps and stores the
// password and object key on approval. Each Step makes at most one HTTP
// request and says how long to wait before the next one (bounded backoff).
//
// Secrets. The secret, poll token and object key travel only in request and
// response bodies and in State/Result, which the caller keeps in memory and in
// the Keychain. Nothing here logs; Result.Message is built only from the
// status code and fixed text, never from a secret.
package enrollclient

import (
	"bytes"
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
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Phase is what the app shows.
type Phase string

const (
	// NotEnrolled: no request has been sent (or one must be sent again).
	NotEnrolled Phase = "not_enrolled"
	// Waiting: the Manager holds the request until an admin approves it.
	Waiting Phase = "waiting"
	// Approved: this Mac may log in as State.Principal.
	Approved Phase = "approved"
	// Revoked: an admin removed this Mac's access.
	Revoked Phase = "revoked"
	// Conflict: the Manager has a different secret on record for this
	// instance ID; an admin must revoke and forget the principal first.
	Conflict Phase = "conflict"
	// Failed: the Manager could not be reached or refused the request;
	// Step is retried after RetryAfterMS.
	Failed Phase = "failed"
)

// Bounded backoff. A pending request is polled every PendingPoll; failures
// back off exponentially from ErrorBase to MaxBackoff. The Manager allows 30
// requests a minute per address (principals.go reqLimit).
var (
	PendingPoll  = 15 * time.Second
	ApprovedPoll = 30 * time.Minute // notices a revoke without hammering
	SettledPoll  = 10 * time.Minute // revoked / conflict: an admin must act
	ErrorBase    = 5 * time.Second
	MaxBackoff   = 5 * time.Minute
)

// Identity is this Mac as the Manager should see it. The bridge fills it from
// internal/cplane (instance ID, clone notice, device digest).
type Identity struct {
	InstanceID string
	Host       string
	Label      string
	// DeviceDigest is the internal/deviceid digest ("" when unavailable).
	DeviceDigest string
	// PreviousInstanceID is set while a clone notice is pending: the ID this
	// install had before it was rotated.
	PreviousInstanceID string
	// ObjectKeyInUse is the access-key ID (never the secret) the running
	// juicefs uses, "" when none runs. It is reported to the Manager as
	// object_key_in_use only when it is one of this Mac's own per-Mac keys
	// (PerMacKeyGen), so the Manager can tell when a delivered key is in use
	// (H-8 per-Mac keys, design §7 M3). The shared jm-client key is never
	// reported: a Manager that delivers it predates the field.
	ObjectKeyInUse string
}

// State is what the app persists between steps (in the Keychain: it holds
// the secret and poll token).
//
// ManagerID binds the enrollment to one Manager (the Manager's manager_id,
// learned from its first answer). ManagerURL is only how that Manager is
// reached: a respelled address that answers with the same ID keeps the
// secret; a different ID starts a fresh enrollment. "" = not bound yet, or
// a Manager too old to send an ID; then the address decides (ManagerKey).
type State struct {
	ManagerURL  string `json:"manager_url"`
	ManagerID   string `json:"manager_id,omitempty"`
	InstanceID  string `json:"instance_id"`
	Secret      string `json:"secret,omitempty"`
	PollToken   string `json:"poll_token,omitempty"`
	Principal   string `json:"principal,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
	Status      string `json:"status,omitempty"` // Manager status: pending, approved, revoked
	Failures    int    `json:"failures,omitempty"`
}

// Result is one step's outcome. State replaces the caller's state; when
// Persist is true it changed and must be saved before the next step.
type Result struct {
	State           State  `json:"state"`
	Persist         bool   `json:"persist"`
	Phase           Phase  `json:"phase"`
	ObjectAccessKey string `json:"object_access_key,omitempty"`
	ObjectSecretKey string `json:"object_secret_key,omitempty"`
	// ObjectKeyPending: approved, but the Manager is still creating this
	// Mac's storage key (per-Mac keys). No key is delivered; the step is
	// repeated after PendingPoll, and the key the Mac has stays in use.
	ObjectKeyPending bool   `json:"object_key_pending,omitempty"`
	RetryAfterMS     int64  `json:"retry_after_ms"`
	Message          string `json:"message,omitempty"`
	// ClearCloneNotice: approved while Identity.PreviousInstanceID was set,
	// so the clone notice has been answered.
	ClearCloneNotice bool `json:"clear_clone_notice,omitempty"`
}

// Doer is the HTTP client (tests substitute it).
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// NewSecret returns a fresh enrollment secret: 32 random bytes, base64url
// without padding (43 characters, inside the Manager's [A-Za-z0-9._~-]{32,256}).
func NewSecret() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// NormalizeManagerURL validates the Manager base URL: http or https, a host,
// no userinfo, query or fragment. The trailing slash is removed.
func NormalizeManagerURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errors.New("the Manager address must be an http:// or https:// URL, for example http://nas.local:30190")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("the Manager address must not contain a user name, query or fragment")
	}
	return strings.TrimRight(u.String(), "/"), nil
}

// ManagerKey is the canonical form of a Manager address. For an enrollment
// bound to a Manager ID (State.ManagerID) it only says whether the address
// changed; the ID then decides. For an unbound enrollment (a Manager that
// sends no ID) it decides whether two spellings name the same Manager (so a
// respelled address keeps the enrollment secret instead of minting a new one
// and landing in Conflict). It lower-cases the scheme and host, drops a trailing dot on the
// host, drops the scheme's default port (80, 443), writes IP literals in
// their canonical form, and drops trailing slashes on the path. Two
// different host names or IP addresses of one server are still different
// keys: proving they are one server would need a Manager identity the API
// does not expose, and a new secret (Conflict until an admin forgets the
// old request) is the safe failure.
//
// Loopback addresses get no special treatment. "localhost", 127.0.0.1,
// 127.0.0.2 and ::1 are separate listeners, and each can be a tunnel (ssh -L,
// a VPN client) to a different NAS. Folding them into one key let two
// tunnels share one enrollment, and a 403 from the second NAS then re-sent
// this Mac's secret to it (review 2 finding F4).
func ManagerKey(raw string) (string, error) {
	base, err := NormalizeManagerURL(raw)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(u.Scheme)
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	port := u.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" {
		host += ":" + port
	}
	return scheme + "://" + host + strings.TrimRight(u.EscapedPath(), "/"), nil
}

// sameManager reports whether two Manager addresses name the same Manager
// (ManagerKey). An address that does not parse matches nothing.
func sameManager(a, b string) bool {
	ka, err := ManagerKey(a)
	if err != nil {
		return false
	}
	kb, err := ManagerKey(b)
	return err == nil && ka == kb
}

// PrincipalFor is the only principal name this Mac may adopt: mac- and
// the first 12 hex digits of its instance ID, lower case (the Manager's
// macPrincipalName). "" when the instance ID is too short.
func PrincipalFor(instanceID string) string {
	hex := strings.ToLower(strings.ReplaceAll(instanceID, "-", ""))
	if len(hex) < 12 || !hex12Re.MatchString(hex[:12]) {
		return ""
	}
	return "mac-" + hex[:12]
}

var (
	hex12Re = regexp.MustCompile(`^[0-9a-f]{12}$`)
	// A per-Mac object key's access key (H-8 per-Mac keys, design §2a):
	// mac-<12 hex>-<gen>, gen a decimal 1-999. The mac-<12 hex> part is the
	// Redis principal of the Mac the key belongs to.
	perMacKeyRe = regexp.MustCompile(`^(mac-[0-9a-f]{12})-([1-9][0-9]{0,2})$`)
	// An object-store key the Mac will hand to juicefs: printable ASCII
	// with no space (MinIO: access key at least 3, secret key at least 8
	// characters).
	objectAccessKeyRe = regexp.MustCompile(`^[\x21-\x7e]{3,128}$`)
	objectSecretKeyRe = regexp.MustCompile(`^[\x21-\x7e]{8,128}$`)
)

// validObjectKey reports whether an approved response's object key may be
// adopted by the Mac whose principal is principal: both halves empty (no key
// configured), or both well formed. A secret may hold any printable ASCII
// but space; MinIO-generated secrets contain '+' and '/'.
//
// A per-Mac access key (anything starting "mac-") must be one of THIS Mac's
// keys, mac-<this Mac's 12 hex>-<gen>. Another Mac's key is refused: a Mac
// that wrote with it would make that Mac's revoke cut this one off, and the
// Manager's records would name the wrong Mac. Any other name is the shared
// key path (jm-client), unchanged.
func validObjectKey(ak, sk, principal string) bool {
	if ak == "" && sk == "" {
		return true
	}
	if !objectAccessKeyRe.MatchString(ak) || !objectSecretKeyRe.MatchString(sk) {
		return false
	}
	if strings.HasPrefix(ak, "mac-") {
		owner, _, ok := PerMacKeyGen(ak)
		return ok && principal != "" && owner == principal
	}
	return true
}

// PerMacKeyGen splits a per-Mac access key mac-<12 hex>-<gen> into the
// principal it belongs to and its generation. ok is false for any other
// name (the shared jm-client key, a malformed name).
func PerMacKeyGen(accessKey string) (principal string, gen int, ok bool) {
	m := perMacKeyRe.FindStringSubmatch(accessKey)
	if m == nil {
		return "", 0, false
	}
	gen, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return m[1], gen, true
}

type enrollRequest struct {
	InstanceID         string `json:"instance_id"`
	Host               string `json:"host,omitempty"`
	Label              string `json:"label,omitempty"`
	Secret             string `json:"secret,omitempty"`
	PollToken          string `json:"poll_token,omitempty"`
	PreviousInstanceID string `json:"previous_instance_id,omitempty"`
	DeviceDigest       string `json:"device_digest,omitempty"`
	// ObjectKeyInUse: see Identity.ObjectKeyInUse. Omitted unless it is
	// this Mac's per-Mac key: a Manager from before per-Mac keys decodes
	// requests strictly (DisallowUnknownFields) and answers 400 to it.
	ObjectKeyInUse string `json:"object_key_in_use,omitempty"`
}

type enrollResponse struct {
	ManagerID       string `json:"manager_id"`
	Status          string `json:"status"`
	Principal       string `json:"principal"`
	Fingerprint     string `json:"fingerprint"`
	PollToken       string `json:"poll_token"`
	ObjectAccessKey string `json:"object_access_key"`
	ObjectSecretKey string `json:"object_secret_key"`
	// ObjectKeyPending (per-Mac keys): approved, but this Mac's storage key
	// is not created yet. Fields this client does not know are ignored.
	ObjectKeyPending bool `json:"object_key_pending"`
}

// inUseRefused records Managers (by ManagerKey) that answered 400 to a
// request carrying object_key_in_use, so later requests omit it. In memory
// only: a Manager image rolled back to one without the field keeps polls
// working, and a restarted app tries the field again.
var inUseRefused sync.Map

// reportInUse is the object_key_in_use value for a request to base: the
// in-use key when it is this Mac's own per-Mac key and that Manager has not
// refused the field, else "".
func reportInUse(base string, id Identity) string {
	owner, _, ok := PerMacKeyGen(id.ObjectKeyInUse)
	if !ok || owner != PrincipalFor(id.InstanceID) {
		return ""
	}
	if key, err := ManagerKey(base); err == nil {
		if _, refused := inUseRefused.Load(key); refused {
			return ""
		}
	}
	return id.ObjectKeyInUse
}

// ManagerIDHeader carries the Manager's ID on every enroll-route answer
// (internal/manager managerIDHeader).
const ManagerIDHeader = "X-JM-Manager-ID"

var managerIDRe = regexp.MustCompile(`^[0-9a-f]{32}$`)

// answerManagerID is the Manager ID an answer carries: the header, else the
// JSON body's manager_id. "" when it carries none (a Manager from before IDs,
// or something that is not a Manager) or a malformed one.
func answerManagerID(h http.Header, raw []byte) string {
	id := strings.TrimSpace(h.Get(ManagerIDHeader))
	if id == "" && len(raw) > 0 {
		var body struct {
			ManagerID string `json:"manager_id"`
		}
		if json.Unmarshal(raw, &body) == nil {
			id = body.ManagerID
		}
	}
	if !managerIDRe.MatchString(id) {
		return ""
	}
	return id
}

// probeManagerID asks base for its Manager ID (GET on the enroll route; no
// credential is sent). ok is false when the address gave no answer to go by
// (network error, redirect, 429 or 5xx: retry later). Any other answer is
// definitive: id is the ID it carries, "" for a Manager from before IDs (405)
// or something that is not a Manager.
func probeManagerID(ctx context.Context, client Doer, base string) (id string, ok bool) {
	hreq, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/principals/enroll", nil)
	if err != nil {
		return "", false
	}
	resp, err := client.Do(hreq)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	switch code := resp.StatusCode; {
	case code >= 300 && code < 400, code == http.StatusTooManyRequests, code >= 500:
		return "", false
	case code == http.StatusOK:
		return answerManagerID(resp.Header, raw), true
	}
	return answerManagerID(resp.Header, nil), true
}

// freshEnrollment starts over with a new secret for base. The old request
// belongs to another Manager or another principal.
func freshEnrollment(st State, base string, id Identity, msg string) Result {
	secret, err := NewSecret()
	if err != nil {
		return Result{State: st, Phase: Failed, Message: "could not generate a secret", RetryAfterMS: ErrorBase.Milliseconds()}
	}
	return Result{State: State{ManagerURL: base, InstanceID: id.InstanceID, Secret: secret}, Persist: true, Phase: NotEnrolled, Message: msg}
}

func backoff(failures int) time.Duration {
	d := ErrorBase
	for i := 1; i < failures && d < MaxBackoff; i++ {
		d *= 2
	}
	return min(d, MaxBackoff)
}

// retryAfter reads a Retry-After header in seconds, bounded to
// [ErrorBase, MaxBackoff]; absent or malformed uses the backoff.
func retryAfter(h http.Header, failures int) time.Duration {
	if s, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After"))); err == nil && s > 0 {
		d := time.Duration(s) * time.Second
		return min(max(d, ErrorBase, backoff(failures)), MaxBackoff)
	}
	return backoff(failures)
}

// Step advances enrollment by at most one request.
//
//   - No secret yet (or a new Manager or instance ID): make one and return
//     with Persist set and no request sent, so the secret is saved before it
//     ever leaves this Mac. A secret lost after it reached the Manager would
//     leave the instance in Conflict until an admin forgets it.
//   - A poll token: poll with it (the secret crosses the wire once).
//   - Otherwise: send the secret (first request, or the token was lost).
//
// Manager identity (State.ManagerID). The first answer that carries a
// Manager ID binds the enrollment to it. When the address later changes, the
// new address is asked for its ID first (no credential sent): the same ID
// keeps the enrollment under the new address, any other answer starts a
// fresh one. At the same address, a 200 or 202 that carries this Mac's
// fingerprint (the server knows this Mac's secret) rebinds to the ID it
// carries and keeps the bound one when it carries none; one with another ID,
// or none, and no matching fingerprint changes nothing (Failed), and a
// "revoked" answer is taken only with the bound ID (Failed otherwise); a 403 or
// 409 with a different well-formed ID starts a fresh enrollment (a new
// secret; the approved one is never sent to it), one without an ID changes
// nothing (Failed), and a 403 re-sends the secret only when its ID is the
// bound one. The ID is not a
// secret: this stops accidents, not an impersonator who has read it. An
// unbound enrollment (a Manager that sends no ID) keeps the address rule:
// ManagerKey decides, and a 403 re-sends the secret to the same address.
func Step(ctx context.Context, client Doer, managerURL string, id Identity, st State) Result {
	base, err := NormalizeManagerURL(managerURL)
	if err != nil {
		return Result{State: st, Phase: Failed, Message: err.Error(), RetryAfterMS: SettledPoll.Milliseconds()}
	}
	if id.InstanceID == "" {
		return Result{State: st, Phase: Failed, Message: "this Mac has no instance ID yet", RetryAfterMS: ErrorBase.Milliseconds()}
	}
	if !strings.EqualFold(st.InstanceID, id.InstanceID) || st.Secret == "" {
		// A rotated (cloned) instance ID, or no secret yet: the old request
		// belongs to another principal. Start over.
		return freshEnrollment(st, base, id, "")
	}
	if !sameManager(st.ManagerURL, base) {
		if st.ManagerID == "" {
			// Unbound: a different address is a different Manager.
			return freshEnrollment(st, base, id, "")
		}
		got, ok := probeManagerID(ctx, client, base)
		if !ok {
			return failed(st, "the Manager at "+base+" could not be asked which Manager it is; nothing was sent", nil)
		}
		if got != st.ManagerID {
			return freshEnrollment(st, base, id, "a different Manager answers at "+base+"; requesting access from it afresh")
		}
		// The same Manager under a new address: keep the secret.
		next := st
		next.ManagerURL, next.Failures = base, 0
		return Result{State: next, Persist: true, Phase: phaseOf(st.Status), Message: "the Manager address changed; it is the same Manager, so this Mac's request is kept"}
	}

	req := enrollRequest{InstanceID: id.InstanceID, DeviceDigest: id.DeviceDigest, ObjectKeyInUse: reportInUse(base, id)}
	usingToken := st.PollToken != ""
	if usingToken {
		req.PollToken = st.PollToken
	} else {
		req.Host, req.Label, req.Secret = id.Host, id.Label, st.Secret
		req.PreviousInstanceID = id.PreviousInstanceID
	}
	body, err := json.Marshal(req)
	if err != nil {
		return Result{State: st, Phase: Failed, Message: "could not encode the request", RetryAfterMS: ErrorBase.Milliseconds()}
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/principals/enroll", bytes.NewReader(body))
	if err != nil {
		return Result{State: st, Phase: Failed, Message: "invalid Manager address", RetryAfterMS: SettledPoll.Milliseconds()}
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(hreq)
	if err != nil {
		return failed(st, "the Manager at "+base+" could not be reached", nil)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	// Which Manager answered (bound enrollments only; review 4 E1/E2).
	//   - 200/202: checked below like any answer. When its ID is not the
	//     bound one it must also carry this Mac's fingerprint, which proves
	//     it knows this Mac's secret; then it rebinds the enrollment to the
	//     ID it carries, or keeps the bound one when it carries none (a
	//     Manager image from before IDs, a principal store restored without
	//     one). Dropping the secret here would orphan an approved Mac in
	//     Conflict.
	//   - 403/409 with a well-formed different ID: another Manager now
	//     answers at this address; start afresh.
	//   - 403/409 without an ID: nothing says which Manager this is (an old
	//     image, a gateway in front of the Manager). Undecided: report a
	//     failure and change nothing, so the secret is neither dropped nor
	//     re-sent.
	// A 429 or 503 (or a gateway's error page) changes nothing either way.
	answerID := answerManagerID(resp.Header, raw)
	switch resp.StatusCode {
	case http.StatusForbidden, http.StatusConflict:
		if st.ManagerID != "" && answerID == "" {
			return failed(st, fmt.Sprintf("the Manager at %s refused (HTTP %d) without saying which Manager it is; this Mac's request is kept", base, resp.StatusCode), nil)
		}
		if st.ManagerID != "" && answerID != st.ManagerID {
			return freshEnrollment(st, base, id, "a different Manager now answers at "+base+"; requesting access from it afresh")
		}
	}

	switch code := resp.StatusCode; {
	case code >= 300 && code < 400:
		// The client never follows a redirect (the bridge's HTTP client
		// returns the 3xx as is): following one would replay the secret to
		// whatever host the Location names.
		return failed(st, fmt.Sprintf("the Manager answered with a redirect (HTTP %d); check the Manager address (redirects are not followed)", code), nil)
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
	case http.StatusForbidden:
		if usingToken {
			// The Manager no longer knows this token (store restored,
			// principal forgotten). Prove ourselves with the secret next.
			next := st
			next.PollToken, next.Failures = "", 0
			return Result{State: next, Persist: true, Phase: phaseOf(st.Status), Message: "re-sending this Mac's request"}
		}
		return failed(st, "the Manager refused the request (403)", nil)
	case http.StatusConflict:
		next := st
		next.Failures = 0
		return Result{State: next, Persist: true, Phase: Conflict, RetryAfterMS: SettledPoll.Milliseconds(),
			Message: "the Manager already has a different request on record for this Mac; an admin must revoke and forget " + principalOr(st, id) + " in the Manager, then request access again"}
	case http.StatusTooManyRequests:
		return failed(st, "the Manager is limiting requests (429); retrying", resp.Header)
	case http.StatusServiceUnavailable:
		return failed(st, "the Manager is busy or enrollment is not configured there (503); retrying", resp.Header)
	case http.StatusBadRequest:
		if req.ObjectKeyInUse != "" {
			// A Manager from before per-Mac keys refuses the unknown
			// field. Omit it from now on; the next step is the plain
			// request that Manager accepts.
			if key, err := ManagerKey(base); err == nil {
				inUseRefused.Store(key, true)
			}
			return failed(st, "the Manager does not accept the in-use storage key report (400); asking again without it", nil)
		}
		return failed(st, "the Manager rejected the request as invalid (400)", nil)
	default:
		return failed(st, fmt.Sprintf("the Manager answered HTTP %d", resp.StatusCode), nil)
	}

	var er enrollResponse
	if err := json.Unmarshal(raw, &er); err != nil || er.Principal == "" {
		return failed(st, "the Manager's answer could not be read", nil)
	}
	// The principal is a function of this Mac's instance ID. Any other name
	// (another Mac's, or a server principal such as "admin") is refused, so
	// a wrong or hostile Manager cannot make this Mac log in as someone else.
	if want := PrincipalFor(id.InstanceID); want == "" || er.Principal != want {
		return failed(st, "the Manager answered for a principal that is not this Mac's; nothing was adopted", nil)
	}
	if !validObjectKey(er.ObjectAccessKey, er.ObjectSecretKey, er.Principal) {
		return failed(st, "the Manager answered with a malformed object-store key, or another Mac's; nothing was adopted", nil)
	}
	// A bound enrollment adopts a 200/202 that carries another ID, or none,
	// only from a server that holds this Mac's request (review 5 X1, X2,
	// X4). The principal check above proves nothing: the principal is a
	// function of the instance ID every poll carries, so any server can echo
	// it. The fingerprint proves the server knows this Mac's secret. That
	// is weaker than "holds this Mac's request": the secret is also this
	// Mac's Redis password, sent in a plain AUTH to every Redis endpoint the
	// Mac logs in to (review 6 F1). So the gate stops accidents and
	// off-network impostors that never saw the Mac's login, not a server
	// the Mac authenticates to. Without it the answer is undecided: nothing
	// is rebound, no key or status is adopted.
	if st.ManagerID != "" && answerID != st.ManagerID && !fingerprintMatches(er.Fingerprint, st.Secret) {
		return failed(st, "an answer from a server that does not hold this Mac's request was ignored; this Mac's request is kept", nil)
	}
	// Revoke is the one outcome that deletes data (the app forgets the
	// object key and the saved previous login), so a bound enrollment takes
	// it only from the bound Manager ID, never on a fingerprint alone
	// (review 6 A1c). Otherwise it is undecided like any unproven answer.
	if st.ManagerID != "" && answerID != st.ManagerID && phaseOf(er.Status) == Revoked {
		return failed(st, "a 'revoked' answer that does not carry this Mac's Manager ID was ignored; this Mac's request is kept", nil)
	}
	next := st
	next.Failures = 0
	next.Principal, next.Fingerprint, next.Status = er.Principal, er.Fingerprint, er.Status
	if answerID != "" {
		// Bind (or rebind) to the Manager that holds the request. No ID in
		// the answer keeps the bound one.
		next.ManagerID = answerID
	}
	if er.PollToken != "" {
		next.PollToken = er.PollToken
	}
	r := Result{State: next, Persist: next != st, Phase: phaseOf(er.Status)}
	switch r.Phase {
	case Approved:
		r.RetryAfterMS = ApprovedPoll.Milliseconds()
		if er.ObjectKeyPending {
			// The Manager is still creating this Mac's key (design §5,
			// test C1): adopt no key, even one sent alongside, and ask
			// again soon instead of in 30 minutes.
			r.ObjectKeyPending = true
			r.RetryAfterMS = PendingPoll.Milliseconds()
		} else if er.ObjectAccessKey != "" && er.ObjectSecretKey != "" {
			r.ObjectAccessKey, r.ObjectSecretKey = er.ObjectAccessKey, er.ObjectSecretKey
		}
		r.ClearCloneNotice = id.PreviousInstanceID != ""
	case Waiting:
		r.RetryAfterMS = PendingPoll.Milliseconds()
	case Revoked:
		r.RetryAfterMS = SettledPoll.Milliseconds()
	default:
		return failed(st, "the Manager answered with an unknown status", nil)
	}
	return r
}

// Fingerprint is the short value the Manager records and returns with this
// Mac's request: the first 8 hex digits of sha256("jm-enroll-fingerprint\x00"
// + secret), upper case, as XXXX-XXXX. It must match the Manager's
// enrollFingerprint.
func Fingerprint(secret string) string {
	sum := sha256.Sum256([]byte("jm-enroll-fingerprint\x00" + secret))
	h := hex.EncodeToString(sum[:])
	return strings.ToUpper(h[0:4] + "-" + h[4:8])
}

// fingerprintMatches reports whether fp is the fingerprint of secret.
func fingerprintMatches(fp, secret string) bool {
	if secret == "" || fp == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(fp), []byte(Fingerprint(secret))) == 1
}

func phaseOf(status string) Phase {
	switch status {
	case "approved":
		return Approved
	case "pending":
		return Waiting
	case "revoked":
		return Revoked
	case "":
		return NotEnrolled
	}
	return Failed
}

func principalOr(st State, _ Identity) string {
	if st.Principal != "" {
		return st.Principal
	}
	return "this Mac's principal"
}

func failed(st State, msg string, h http.Header) Result {
	next := st
	next.Failures++
	wait := backoff(next.Failures)
	if h != nil {
		wait = retryAfter(h, next.Failures)
	}
	// Persist stays false: the failure count only paces retries in memory.
	return Result{State: next, Phase: Failed, Message: msg, RetryAfterMS: wait.Milliseconds()}
}
