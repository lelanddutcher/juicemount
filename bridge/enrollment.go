package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/lelanddutcher/juicemount/health"
	"github.com/lelanddutcher/juicemount/internal/cplane"
	"github.com/lelanddutcher/juicemount/internal/enrollclient"
	"github.com/lelanddutcher/juicemount/internal/jmlog"
	"github.com/lelanddutcher/juicemount/internal/redisurl"
)

// H-8B Mac enrollment (internal/enrollclient). The app drives it: it keeps
// the enrollment State in its Keychain, calls NFSServerEnrollStep, saves the
// returned State when Persist is set, stores the Redis password and object
// key on approval, and waits RetryAfterMS before the next step. It runs in
// the app's own process (both archive builds) and needs no serving service:
// a Mac enrolls before it can mount.
//
// The returned JSON carries secrets (the enrollment secret, poll token and,
// once approved, the object key) for the app to store. It is never logged.

type enrollStepInput struct {
	ManagerURL string             `json:"manager_url"`
	DBPath     string             `json:"db_path"`
	Host       string             `json:"host"`
	Label      string             `json:"label"`
	State      enrollclient.State `json:"state"`
	// RedisURL is the configured metadata endpoint WITHOUT a password (the
	// app strips it). On an approved step the bridge logs in there as the
	// principal with the enrollment secret before the app may switch to it.
	RedisURL string `json:"redis_url,omitempty"`
	// Op "object-key-status" asks only for the object-key status
	// (objectKeyStatusOutput) and contacts no Manager. "" = an enrollment
	// step.
	Op string `json:"op,omitempty"`
}

// enrollOpObjectKeyStatus is the Op of a status-only call.
const enrollOpObjectKeyStatus = "object-key-status"

type enrollStepOutput struct {
	enrollclient.Result
	InstanceID   string `json:"instance_id"`
	CloneNotice  string `json:"clone_notice,omitempty"`
	CloneCleared bool   `json:"clone_cleared,omitempty"`
	// CredentialsLive: approved, AND the configured Redis endpoint accepted
	// AUTH <principal> <secret> just now. The app switches this Mac's Redis
	// login (URL user + Keychain password) and stores the object key only
	// when this is true. Before the server's proxy cutover the plain Redis
	// refuses the principal (WRONGPASS), so an early approval leaves the
	// Mac's working login untouched.
	CredentialsLive bool `json:"credentials_live,omitempty"`
	// CredentialCheck explains a failed check (fixed text, never a secret).
	CredentialCheck string `json:"credential_check,omitempty"`
	// ObjectKeyInUse is the access-key ID the running juicefs uses ("" =
	// none, or no serving process answered). Never a secret.
	ObjectKeyInUse string `json:"object_key_in_use,omitempty"`
	// ObjectKeyRestartNeeded: this approved step delivered a key other than
	// the one juicefs runs with. It is stored now and used after the volume
	// restarts (juicefs reads it from its environment at start).
	ObjectKeyRestartNeeded bool `json:"object_key_restart_needed,omitempty"`
	// ObjectKeyRefused: the object store refused the key the running
	// juicefs uses (health.ObjectKeyRefusalCode) within the last
	// RefusalExpiry; RefusedCode is the S3 code.
	ObjectKeyRefused bool   `json:"object_key_refused,omitempty"`
	RefusedCode      string `json:"refused_code,omitempty"`
}

// objectKeyStatusOutput answers an Op "object-key-status" call. The app
// asks every few seconds while this Mac is approved; PollNow tells it to run
// an enrollment step now instead of at the 30-minute approved poll.
type objectKeyStatusOutput struct {
	// Known: a serving process answered (false: none runs, or it did not
	// answer; every other field is then empty).
	Known bool   `json:"known"`
	InUse string `json:"object_key_in_use,omitempty"`
	// Refused: a refusal within the last RefusalExpiry (refusalActive).
	Refused     bool   `json:"object_key_refused,omitempty"`
	RefusedCode string `json:"refused_code,omitempty"`
	PollNow     bool   `json:"poll_now,omitempty"`
}

// RefusalPollMin is the shortest time between two enrollment polls that a
// refusal triggers. The Manager allows 30 requests a minute per address; a
// refused upload is retried by juicefs every few seconds, so without a floor
// every retry would be a poll.
var RefusalPollMin = 60 * time.Second

// RefusalPollMax caps the backoff. While refusals continue, the wait
// between refusal-triggered polls doubles from RefusalPollMin up to this:
// a key the Manager cannot fix (it stays refused) costs about 7 polls in
// the first hour and 4 an hour after that, not 60 an hour (review keys-l3
// F3). The 30-minute approved poll runs regardless.
var RefusalPollMax = 15 * time.Minute

// RefusalExpiry is how long after the last refusal this Mac still counts
// as refused. juicefs retries a refused upload every few seconds, so a
// refusal that is still happening renews itself well inside it; once
// access is restored (a new key, or the admin fixed the policy) the red
// "refused" state clears without a restart (review keys-l3 F3).
var RefusalExpiry = 5 * time.Minute

// refusalActive reports whether st holds a refusal no older than
// RefusalExpiry at now. A refusal time in the future (clock change) counts.
func refusalActive(st health.ObjectKeyStatus, now time.Time) bool {
	if st.Refusals == 0 || st.RefusedAtMS == 0 {
		return false
	}
	return now.Sub(time.UnixMilli(st.RefusedAtMS)) < RefusalExpiry
}

// refusalPollGate decides when a refusal triggers a poll: once per new
// refusal (the count grew since the last poll) that is still active, with
// a wait between polls that doubles from RefusalPollMin to RefusalPollMax
// while refusals continue. A new juicefs child (another key in use), or a
// refusal that expired, starts over.
type refusalPollGate struct {
	mu    sync.Mutex
	inUse string
	seen  uint64
	last  time.Time
	wait  time.Duration // 0 = RefusalPollMin
}

var objectKeyPollGate refusalPollGate

func (g *refusalPollGate) resetIfNewChild(st health.ObjectKeyStatus) {
	if st.InUse != g.inUse || st.Refusals < g.seen {
		g.inUse, g.seen, g.wait = st.InUse, 0, 0
	}
}

// pollNow reports whether st calls for an immediate enrollment poll, and
// records that one is made.
func (g *refusalPollGate) pollNow(st health.ObjectKeyStatus, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resetIfNewChild(st)
	if !refusalActive(st, now) {
		// Refusals stopped (none for RefusalExpiry, which is longer than
		// RefusalPollMin): the next refusal starts a new run, polled at
		// once and then at the short wait again.
		g.wait, g.last = 0, time.Time{}
		return false
	}
	if st.Refusals <= g.seen {
		return false
	}
	wait := max(g.wait, RefusalPollMin)
	if !g.last.IsZero() && now.Sub(g.last) < wait {
		return false
	}
	if !g.last.IsZero() {
		// A poll that follows an earlier one in the same run of refusals:
		// the Manager did not fix it, so wait longer next time.
		g.wait = wait * 2
		if g.wait > RefusalPollMax {
			g.wait = RefusalPollMax
		}
	}
	g.seen, g.last = st.Refusals, now
	return true
}

// notePolled records that an enrollment step saw st: refusals it already
// carried need no extra poll.
func (g *refusalPollGate) notePolled(st health.ObjectKeyStatus) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resetIfNewChild(st)
	g.seen = max(g.seen, st.Refusals)
}

// enrollVerifyOpObjectKeyStatus is the first string of an "enroll-verify"
// service call that asks only for the serving process's object-key status
// (objectKeyStatusInServingProcess): {enrollVerifyOpObjectKeyStatus, "", ""}.
// It is not a Redis URL, so it cannot be mistaken for a login check, and a
// login check never carries it. Any other first string is a login check
// with the full restrictions (enrollVerifyInServingProcess).
//
// The status rides on "enroll-verify" because the control-socket method
// list (bridge/service_control.go: the allowed methods with their argument
// counts, and the dispatch switch) is outside the enrollment files; a
// method of its own, "object-key-status", is the cleaner shape once that
// file can change.
const enrollVerifyOpObjectKeyStatus = "op:object-key-status"

// currentObjectKeyStatus is the object-key status of the process that runs
// juicefs: this process, or in the desktop archive the serving process,
// asked through "enroll-verify" {enrollVerifyOpObjectKeyStatus, "", ""}.
// known is false when it did not answer. A test substitutes it.
var currentObjectKeyStatus = func() (st health.ObjectKeyStatus, known bool) {
	if !serviceClientBuild {
		return health.CurrentObjectKeyStatus(), true
	}
	res, reached := forwardServiceCallReached("enroll-verify", []string{enrollVerifyOpObjectKeyStatus, "", ""}, nil)
	if !reached || res.Error != "" {
		return health.ObjectKeyStatus{}, false
	}
	var out enrollVerifyResult
	if err := json.Unmarshal([]byte(res.Text), &out); err != nil || out.ObjectKey == nil {
		return health.ObjectKeyStatus{}, false
	}
	return *out.ObjectKey, true
}

// objectKeyStatusJSON answers an Op "object-key-status" call.
func objectKeyStatusJSON(now time.Time) string {
	st, known := currentObjectKeyStatus()
	out := objectKeyStatusOutput{Known: known}
	if known {
		out.InUse = st.InUse
		if refusalActive(st, now) {
			out.Refused, out.RefusedCode = true, st.RefusedCode
		}
		out.PollNow = objectKeyPollGate.pollNow(st, now)
		if out.PollNow {
			jmlog.Warn("the object store refused this Mac's storage key; asking the Manager now",
				"code", st.RefusedCode, "key", st.InUse, "refusals", st.Refusals)
		}
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// newEnrollHTTPClient is the Manager client. It never follows a redirect:
// following a 307/308 replays the POST body, so the enrollment secret would
// go to whatever host the Location names. A 3xx is returned as is and
// enrollclient.Step reports it as a failure.
func newEnrollHTTPClient() *http.Client { return managerHTTPClient(nil) }

// enrollHTTPClient is the Manager client; tests substitute it.
var enrollHTTPClient enrollclient.Doer = newEnrollHTTPClient()

// contextDial is a net.Dialer.DialContext-shaped function.
type contextDial = func(ctx context.Context, network, address string) (net.Conn, error)

// inProcessLinkDial returns the dialer of the embedded JuiceMount Link node
// when it runs in this process (nil otherwise). It is the dialer the mount's
// adaptive loopback proxies use: Link first, the LAN only when the paired
// NAS reports exactly that private endpoint. A test substitutes it.
var inProcessLinkDial = func() contextDial {
	linkMu.Lock()
	node := globalLinkNode
	linkMu.Unlock()
	if node == nil {
		return nil
	}
	return node.AdaptiveDialContext
}

// enrollManagerClient is the client for one enrollment request: through the
// in-process Link node when there is one (the Manager is on the NAS that
// Link reaches), else enrollHTTPClient. Neither follows a redirect. The
// desktop archive's app process has no Link node and uses servingDoer
// instead (enrollManagerDoer, bridge/enrollment_manager_route.go).
func enrollManagerClient() enrollclient.Doer {
	dial := inProcessLinkDial()
	if dial == nil {
		return enrollHTTPClient
	}
	return managerHTTPClient(dial)
}

// principalLoginRoute is how the approved-step login check reaches Redis:
// the same way the mount does (review 2 finding F3).
type principalLoginRoute struct {
	Name string // "link", "owner-link", "system"
	// Dial replaces the operating-system dialer ("link").
	Dial contextDial
	// Addr replaces the URL's host:port ("owner-link": the volume owner's
	// loopback Redis proxy, which forwards through the owner's Link node).
	Addr string
}

// currentPrincipalLoginRoute picks the mount's Redis route for redisURL:
//   - the embedded Link node in this process: its adaptive dialer;
//   - a selected volume owner with Link enabled whose configured Redis is
//     redisURL's host:port: the owner receipt's loopback Redis URL;
//   - otherwise macOS networking (no Link, or "Use existing private
//     network"), which is also what the mount uses then.
//
// A test substitutes it.
var currentPrincipalLoginRoute = func(redisURL string) principalLoginRoute {
	if dial := inProcessLinkDial(); dial != nil {
		return principalLoginRoute{Name: "link", Dial: dial}
	}
	globalMu.Lock()
	owner := globalOwner
	globalMu.Unlock()
	if addr, ok := owner.linkRedisLoopback(redisURL); ok {
		return principalLoginRoute{Name: "owner-link", Addr: addr}
	}
	return principalLoginRoute{Name: "system"}
}

// sameRedisEndpoint reports whether two Redis URLs name one host:port.
func sameRedisEndpoint(a, b string) bool {
	oa, err := redisurl.Parse(a)
	if err != nil {
		return false
	}
	ob, err := redisurl.Parse(b)
	return err == nil && strings.EqualFold(oa.Addr, ob.Addr)
}

// isLoopbackHostPort: a literal loopback IP with a port.
func isLoopbackHostPort(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	return err == nil && port != "" && ip != nil && ip.IsLoopback()
}

// principalAuthTimeout bounds the approved-step login check.
const principalAuthTimeout = 5 * time.Second

// enrollVerifyDeadline is the app's wait for the serving process's
// "enroll-verify" answer: the check's own bound plus margin for the route
// lookup.
const enrollVerifyDeadline = 10 * time.Second

// servingRedisURL is the metadata endpoint this serving process was last
// started with (the logical URL, before Link or debug rewrites). It is the
// only endpoint "enroll-verify" checks.
var servingRedisURL atomic.Pointer[string]

// noteServingRedisURL records the endpoint a Start was configured with.
func noteServingRedisURL(redisURL string) { servingRedisURL.Store(&redisURL) }

// macPrincipalRe: the only principals "enroll-verify" logs in as.
var macPrincipalRe = regexp.MustCompile(`^mac-[0-9a-f]{12}$`)

// verifyPrincipalLogin logs in to the Redis endpoint redisURL (no password
// in it) as principal/secret and sends PING, over the route the mount uses
// (currentPrincipalLoginRoute). It reports whether the endpoint accepted the
// login, and a fixed-text reason when not. The secret is never part of the
// reason. A test substitutes it.
//
// The desktop archive's app process holds no Link node and no volume owner:
// the serving process does. There the check runs in the serving process
// ("enroll-verify", verifyPrincipalLoginForwarded). The app checks over macOS
// networking itself only when the serving process's control socket cannot be
// reached at all; once the request was sent, a lost or late answer is a
// failed check, never a second check by another route (service_control.go:
// no failed call is replaced by an in-process operation).
var verifyPrincipalLogin = func(redisURL, principal, secret string) (bool, string) {
	if serviceClientBuild {
		if ok, why, forwarded := verifyPrincipalLoginForwarded(redisURL, principal, secret); forwarded {
			return ok, why
		}
	}
	return verifyPrincipalLoginVia(currentPrincipalLoginRoute(redisURL), redisURL, principal, secret)
}

// verifyPrincipalLoginForwarded asks the serving process to run the check
// (its Link node or volume owner is the mount's route). forwarded is false
// only when the control socket could not be reached, so nothing was sent.
func verifyPrincipalLoginForwarded(redisURL, principal, secret string) (ok bool, why string, forwarded bool) {
	res, reached := forwardServiceCallReached("enroll-verify", []string{redisURL, principal, secret}, nil)
	if !reached {
		jmlog.Info("enrollment login check: no serving process; checking from the app over macOS networking")
		return false, "", false
	}
	if res.Error != "" {
		jmlog.Info("enrollment login check: the serving process did not answer; not checking by another route")
		return false, "the JuiceMount service did not answer the login check in time; it is checked again automatically", true
	}
	var out enrollVerifyResult
	if err := json.Unmarshal([]byte(res.Text), &out); err != nil {
		return false, "the JuiceMount service's answer to the login check could not be read; it is checked again automatically", true
	}
	return out.Live, out.Reason, true
}

// enrollVerifyResult is the "enroll-verify" answer (never a secret).
type enrollVerifyResult struct {
	Live   bool   `json:"live"`
	Reason string `json:"reason,omitempty"`
	Route  string `json:"route"`
	// ObjectKey is this serving process's object-key status: the
	// access-key ID its juicefs uses and the refusals seen since that child
	// started (never a secret). Set only on an object-key status call
	// (Route "status"; enrollVerifyOpObjectKeyStatus).
	ObjectKey *health.ObjectKeyStatus `json:"object_key,omitempty"`
}

// objectKeyStatusInServingProcess answers the object-key status call
// ("enroll-verify" {enrollVerifyOpObjectKeyStatus, "", ""}, sent by
// currentObjectKeyStatus in the desktop archive): this process's
// health.CurrentObjectKeyStatus. No login, no dial, no secret.
func objectKeyStatusInServingProcess() string {
	st := health.CurrentObjectKeyStatus()
	b, _ := json.Marshal(enrollVerifyResult{Live: false, Route: "status", ObjectKey: &st})
	return string(b)
}

// enrollVerifyInServingProcess is the serving process's side of
// "enroll-verify": the check over this process's mount route.
//
// The caller does not choose the target (review 3 finding 4): the check runs
// only against the endpoint this process was started with, and only as a Mac
// principal (mac-<12 hex>). Otherwise any same-user process could use this
// process's Link node to try logins against any host:port on the tailnet.
func enrollVerifyInServingProcess(redisURL, principal, secret string) string {
	refuse := func(why string) string {
		b, _ := json.Marshal(enrollVerifyResult{Live: false, Reason: why, Route: "refused"})
		return string(b)
	}
	if redisURL == enrollVerifyOpObjectKeyStatus && principal == "" && secret == "" {
		return objectKeyStatusInServingProcess()
	}
	if !macPrincipalRe.MatchString(principal) {
		return refuse("the login check only checks a Mac principal (mac-<12 hex>)")
	}
	configured := servingRedisURL.Load()
	if configured == nil || strings.TrimSpace(*configured) == "" {
		return refuse("the JuiceMount service has not been started with a Redis address yet, so this Mac's new login was not checked")
	}
	if !sameRedisEndpoint(*configured, redisURL) {
		return refuse("the Redis address in Settings is not the one the JuiceMount service runs with, so this Mac's new login was not checked")
	}
	route := currentPrincipalLoginRoute(*configured)
	ok, why := verifyPrincipalLoginVia(route, *configured, principal, secret)
	b, _ := json.Marshal(enrollVerifyResult{Live: ok, Reason: why, Route: route.Name})
	return string(b)
}

func verifyPrincipalLoginVia(route principalLoginRoute, redisURL, principal, secret string) (bool, string) {
	opts, err := redisurl.Parse(redisURL)
	if err != nil {
		return false, "the Redis address in Settings could not be read, so this Mac's new login was not checked"
	}
	if (route.Dial != nil || route.Addr != "") && opts.TLSConfig != nil {
		return false, "the Redis address uses TLS, which the JuiceMount Link check does not carry, so this Mac's new login was not checked"
	}
	switch {
	case route.Dial != nil:
		opts.Dialer = route.Dial
	case route.Addr != "":
		if !isLoopbackHostPort(route.Addr) {
			return false, "the mount's Redis route is not a local proxy, so this Mac's new login was not checked"
		}
		opts.Addr = route.Addr
	}
	jmlog.Info("enrollment login check", "route", route.Name, "principal", principal)
	opts.Username, opts.Password = principal, secret
	opts.Protocol = 2 // plain AUTH user pass: what both Redis and jm-respproxy answer
	opts.DisableIdentity = true
	opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout = principalAuthTimeout, principalAuthTimeout, principalAuthTimeout
	opts.ContextTimeoutEnabled = true
	opts.PoolSize, opts.MaxRetries = 1, -1
	client := redis.NewClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), principalAuthTimeout)
	defer cancel()
	err = client.Ping(ctx).Err()
	if err == nil {
		return true, ""
	}
	msg := err.Error()
	for _, refused := range []string{"WRONGPASS", "NOAUTH", "NOPERM", "invalid username-password", "user is disabled"} {
		if strings.Contains(msg, refused) {
			return false, "the server does not accept this Mac's new login yet (it has not switched to the per-Mac proxy)"
		}
	}
	return false, "the metadata server could not be reached to check this Mac's new login"
}

//export NFSServerEnrollStep
func NFSServerEnrollStep(input *C.char) *C.char {
	return C.CString(enrollStep(C.GoString(input)))
}

func enrollStep(raw string) string {
	var in enrollStepInput
	if err := json.Unmarshal([]byte(raw), &in); err != nil {
		return enrollFailure(in.State, "invalid enrollment input")
	}
	if in.Op == enrollOpObjectKeyStatus {
		return objectKeyStatusJSON(time.Now())
	}
	if in.Op != "" {
		return enrollFailure(in.State, "unknown enrollment operation")
	}
	if strings.TrimSpace(in.DBPath) == "" {
		return enrollFailure(in.State, "no metadata database path; the instance ID cannot be located")
	}
	// The same identity the service reports on /whoami: the instance ID
	// beside metadata.db, rotated (with a clone notice) when it was copied
	// from another Mac.
	identity := cplane.LoadInstanceIdentity(in.DBPath)
	id := enrollclient.Identity{
		InstanceID:   identity.ID,
		Host:         strings.TrimSpace(in.Host),
		Label:        strings.TrimSpace(in.Label),
		DeviceDigest: cplane.DeviceDigest(),
	}
	if id.Host == "" {
		id.Host, _ = os.Hostname()
	}
	// The key the running juicefs uses: reported to the Manager (access-key
	// ID only) and compared with what this step delivers.
	keyStatus, keyKnown := currentObjectKeyStatus()
	if keyKnown {
		id.ObjectKeyInUse = keyStatus.InUse
		objectKeyPollGate.notePolled(keyStatus)
	}
	notice, hasNotice := cplane.ReadCloneNotice(in.DBPath)
	if hasNotice && strings.EqualFold(notice.InstanceID, identity.ID) {
		id.PreviousInstanceID = notice.PreviousInstanceID
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	doer, viaService := enrollManagerDoer()
	r := enrollclient.Step(ctx, doer, in.ManagerURL, id, in.State)
	if viaService != nil && viaService.note != "" && r.Phase == enrollclient.Failed {
		// Say which route failed: "could not be reached" alone reads as a
		// Manager outage when the cause is the route (review 2 F3).
		r.Message += " (" + viaService.note + ")"
	}
	out := enrollStepOutput{Result: r, InstanceID: identity.ID}
	if keyKnown {
		out.ObjectKeyInUse = keyStatus.InUse
		if refusalActive(keyStatus, time.Now()) {
			out.ObjectKeyRefused, out.RefusedCode = true, keyStatus.RefusedCode
		}
		out.ObjectKeyRestartNeeded = r.Phase == enrollclient.Approved && keyStatus.InUse != "" &&
			r.ObjectAccessKey != "" && r.ObjectAccessKey != keyStatus.InUse
	}
	if r.Phase == enrollclient.Approved {
		switch redisURL := strings.TrimSpace(in.RedisURL); {
		case redisURL == "":
			out.CredentialCheck = "no Redis address is set, so this Mac's new login was not checked"
		case r.State.Principal == "" || r.State.Secret == "":
			out.CredentialCheck = "the approval carries no principal or secret"
		default:
			out.CredentialsLive, out.CredentialCheck = verifyPrincipalLogin(redisURL, r.State.Principal, r.State.Secret)
		}
	}
	if hasNotice {
		out.CloneNotice = notice.Message
	}
	if r.ClearCloneNotice && hasNotice {
		if err := cplane.ClearCloneNotice(in.DBPath); err != nil {
			jmlog.Warn("enrollment approved but the clone notice could not be cleared", "error", err.Error())
		} else {
			out.CloneNotice, out.CloneCleared = "", true
		}
	}
	// Names and outcomes only: never the enrollment secret, the poll token
	// or the object secret key. The object access-key IDs (in use and
	// delivered) are logged: an ID is not a secret (MinIO lists it in
	// `mc admin user svcacct list`, and the Manager shows it).
	jmlog.Info("enrollment step", "phase", string(r.Phase), "principal", r.State.Principal,
		"fingerprint", r.State.Fingerprint, "retry_ms", r.RetryAfterMS, "message", r.Message,
		"clone_rotated", id.PreviousInstanceID != "", "credentials_live", out.CredentialsLive,
		"object_key_in_use", out.ObjectKeyInUse, "object_key_delivered", r.ObjectAccessKey,
		"object_key_pending", r.ObjectKeyPending, "object_key_refused", out.ObjectKeyRefused)
	b, err := json.Marshal(out)
	if err != nil {
		return enrollFailure(in.State, "could not encode the enrollment result")
	}
	return string(b)
}

// enrollFailure echoes the caller's state unchanged: a refused input must
// never make the app drop a secret the Manager already holds.
func enrollFailure(st enrollclient.State, msg string) string {
	b, _ := json.Marshal(enrollStepOutput{Result: enrollclient.Result{
		State: st, Phase: enrollclient.Failed, Message: msg, RetryAfterMS: enrollclient.SettledPoll.Milliseconds(),
	}})
	return string(b)
}
