package manager

// collections.go: continuity slice 3, Collections (experimental, behind
// JM_CONTINUITY like the binder; with it off every route is 404). A
// collection is a named set of catalog assets, kept in the continuity
// catalog, so every change is an event in its ordered history with the
// actor that made it: "admin", or the Teams person's ID.
//
// Who. The admin key, or a Teams person through one of their Macs: the Mac
// sends its instance ID, the enrollment poll token it already holds for the
// Manager and its device digest, as on the enrollment poll (principals.go;
// X-JuiceMount-Mac-Instance, -Token, -Device). The Mac must be approved and
// assigned to a person, and not flagged as possibly cloned (a digest that
// differs from the enrollment's flags it, here as on the poll; a Mac
// enrolled before digests existed has none, so the first digest seen here
// is recorded and any later different one flags it: trust on first use).
// Each Mac has a request budget, charged once its token is verified, and
// each source address a looser one before that (an office behind one NAT
// is not starved, and fake tokens cannot drain a Mac's budget). With no admin key
// configured (LAN/dev mode) the admin path is open, so it may not write
// while grants are in force. An agent token (agenttokens.go) acts as its
// person, with the budget of the Mac that minted it, on the requests
// agentguard.go allows.
//
// What. Reads are shared (Teams v1: any authenticated caller), except that
// an agent reads only members inside its person's grants (the rest are
// withheld, counted without names; discovery.go). A person may
// write only where their project write grants (people.go) cover the files
// the change touches, checked live on the volume: the members added or
// removed, and for a rename or delete every member of the collection. A
// file passes only when every one of its names outside the trash is
// covered, hidden names included: the proxy ties a write to a hard-linked
// file to all its parents (respproxy/txnunit.go), and a file moved into
// /Clients/B/.old is B's, not gone. A file whose inode is gone, or that is
// only in the trash, needs no grant; one whose names cannot be read (or
// with over binder.MaxLinks links, unless a name already fails) refuses the
// change with 503, since unknown is not gone. A person with no write access
// at all writes nothing. A rename or delete carries the collection's
// sequence as read for its check of every member, so a membership change in
// between makes it 409; adding or removing members depends only on the
// named members, so it carries none. A refusal is 403
// and is logged (at most once a minute per person). The catalog's event
// history is the record of what was changed and by whom; an agent's changes
// are also in the Manager's catalog audit log, with its token
// (agenttokens.go; the proxy's audit log has a single writer).
//
// Cost. Requests that read the volume have a 30 s deadline (an agent's,
// 10 s) and take a slot: people's Macs share two, agents one of their own,
// the admin has one, and each actor (a person with their agents) has at
// most one such request at a time (a second gets 503). A member page holds
// at most 100 members. Residual: jfsmeta reads take no context, so a hung
// volume read holds its slot past the deadline until it returns.
//
// Members are asset IDs, added by ID or by a volume path the binder resolves
// (never through a path hint). A member's path is resolved when it is read;
// a member whose file is gone is listed as missing, not removed.
//
// Retries. A write may carry operation_id (1-64 letters, digits, - or _).
// The catalog operation ID is derived from it and the actor, so the same
// caller repeating the same request gets the original result (without a
// second authorization: the change was authorized when it committed), and a
// different request under the same operation_id is 409. A retry is matched
// against the recorded operation before its members must resolve: a member
// path that no longer names a catalogued file is not held against it, so a
// file that has gone since does not make a committed change 404, while a
// path that names a file the operation did not is 409 (replayMembers).

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const (
	collectionAdminActor = "admin"
	// One change per member, plus the precondition or the create.
	maxCollectionRequestMembers = catalog.MaxChanges - 1
	maxCollectionBody           = 256 << 10
	maxMemberPage               = 100
	collectionVolumeSlots       = 2
	// collectionAgentSlots: every agent shares this pool, apart from the
	// people's, so agents finding files never hold up a Mac.
	collectionAgentSlots = 1
	// A Mac's budget: 5 requests a second sustained, bursts of 60, enough
	// for a person paging through members (one request per page of 100)
	// or an agent filing a batch, while a runaway client is cut off in
	// seconds. Before the token is checked an address gets 600 a minute:
	// an office of Macs behind one NAT, but not an unbounded token guess.
	collectionMacPerMinute    = 300
	collectionMacBurst        = 60
	collectionIPPerMinute     = 600
	collectionRefusalLogEvery = time.Minute
)

var (
	collectionKeyRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

	errMacUnknown      = errors.New("unknown Mac or wrong token")
	errMacDevice       = errors.New("send the Mac's device digest (X-JuiceMount-Mac-Device), as on the enrollment poll")
	errMacNotApproved  = errors.New("this Mac is not approved")
	errMacNoPerson     = errors.New("this Mac belongs to no person; an admin assigns it on the People page")
	errMacDeviceClash  = errors.New("this Mac's identity may be held by two Macs; an admin must revoke and re-enroll it")
	errCollectionInput = errors.New("invalid collection request")

	// collectionBeforeApply runs between a write's authorization and the
	// write (tests).
	collectionBeforeApply func()
	// Variables for tests.
	collectionRequestTimeout = 30 * time.Second
	collectionBusyWait       = 5 * time.Second
)

// macPerson authenticates a Mac by the enrollment poll token it holds and
// returns its principal name and a copy of its person. The device digest is
// noted as on the poll, so a second Mac using the same identity flags it;
// a Mac with no recorded digest gets this one (trust on first use).
func (s *PrincipalStore) macPerson(instanceID, pollToken, device string) (string, Person, error) {
	instanceID = strings.TrimSpace(instanceID)
	if !instanceIDRe.MatchString(instanceID) || !pollTokenRe.MatchString(pollToken) {
		return "", Person{}, errMacUnknown
	}
	if !deviceDigestRe.MatchString(device) {
		return "", Person{}, errMacDevice
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name := macPrincipalName(instanceID)
	rec := s.recs[name]
	if rec == nil || rec.PollTokenSHA256 == "" || !strings.EqualFold(rec.InstanceID, instanceID) ||
		subtle.ConstantTimeCompare([]byte(sha256Hex(pollToken)), []byte(rec.PollTokenSHA256)) != 1 {
		return "", Person{}, errMacUnknown
	}
	if rec.DeviceDigest == "" {
		next := *rec
		next.DeviceDigest = device
		s.recs[name] = &next
		if err := s.saveStoreLocked(); err != nil {
			s.recs[name] = rec
			return "", Person{}, fmt.Errorf("recording the Mac's device digest: %w", err)
		}
		log.Printf("manager: principal %s had no device digest; recorded the first one seen (collections API)", name)
	} else {
		s.noteDeviceLocked(rec, device)
	}
	rec = s.recs[name]
	switch {
	case rec.Status != PrincipalApproved:
		return "", Person{}, errMacNotApproved
	case rec.DeviceMismatch:
		return "", Person{}, errMacDeviceClash
	case rec.Person == "" || s.people[rec.Person] == nil:
		return "", Person{}, errMacNoPerson
	}
	p := *s.people[rec.Person]
	p.Grants = append([]string(nil), p.Grants...)
	return name, p, nil
}

// grantsInForce mirrors PeopleSnapshot's GrantsInForce, cheaply.
func (s *PrincipalStore) grantsInForce() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.grantsInForceLocked()
}

// collectionActor is who is calling: the admin, or a person through one of
// their Macs or through an agent token (agenttokens.go), which acts as its
// person.
type collectionActor struct {
	person *Person // nil: admin
	// open: the admin path without a key (none is configured).
	open bool
	// mac: the person's Mac principal (for an agent, the Mac that minted
	// its token).
	mac string
	// agent: the agent token the request came with (nil: none).
	agent *AgentToken
}

func (c collectionActor) id() string {
	if c.person == nil {
		return collectionAdminActor
	}
	return c.person.ID
}

// opKey scopes operation_id: an agent's retries are its own, never its
// person's Mac's.
func (c collectionActor) opKey() string {
	if c.agent != nil {
		return c.id() + "\x00agent\x00" + c.agent.ID
	}
	return c.id()
}

// writesAnywhere: the person has some write access (access "all", or at
// least one grant).
func (c collectionActor) writesAnywhere() bool {
	return c.person == nil || c.person.Access == personAccessAll || len(c.person.Grants) > 0
}

// covers: the actor may write at volumePath (volume-relative, as the
// binder resolves it). A grant covers its folder and everything below it
// (/Clients/A covers /Clients/A/x, not /Clients/AB/x).
func (c collectionActor) covers(volumePath string) bool {
	if c.person == nil || c.person.Access == personAccessAll {
		return true
	}
	full := "/" + strings.TrimPrefix(volumePath, "/")
	for _, g := range c.person.Grants {
		if full == g || strings.HasPrefix(full, g+"/") {
			return true
		}
	}
	return false
}

// refusalLog logs a person's refusals at most once per interval, counting
// the ones it held back.
type refusalLog struct {
	mu    sync.Mutex
	every time.Duration
	last  map[string]time.Time
	held  map[string]int
}

func (l *refusalLog) note(person, line string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if t, ok := l.last[person]; ok && now.Sub(t) < l.every {
		l.held[person]++
		return
	}
	if n := l.held[person]; n > 0 {
		line += fmt.Sprintf(" (and %d more refusals of this person since the last line)", n)
	}
	l.last[person], l.held[person] = now, 0
	log.Print(line)
}

type collectionsAPI struct {
	api       *API
	people    *PrincipalStore // nil: only the admin key
	prefix    string
	slots     chan struct{} // people's volume-read slots (their Macs)
	agentSlot chan struct{} // agents' own, so they never hold up a Mac
	adminSlot chan struct{} // the admin's own
	mu        sync.Mutex
	inflight  map[string]bool // actors with a volume-reading request running
	ipLimit   *ipLimiter      // per source address, before the token is checked
	macLimit  *ipLimiter      // per Mac principal, after
	refusals  *refusalLog
	roots     func() []string // the smart-folder roots the Manager owns (queries.go)
	audit     *agentAudit     // the catalog audit log (agenttokens.go)
	// queryCreate is held by a query create from its per-person count to
	// its write (queries.go).
	queryCreate sync.Mutex
}

// registerCollections wires /api/collections, /api/queries (queries.go),
// /api/continuity/find and /api/continuity/asset/ (discovery.go) and
// /api/agent-tokens (agenttokens.go), sharing the authentication, budgets
// and slots. The handlers do their own authentication (admin key, a
// person's Mac, or an agent token where agentguard.go allows one) and read
// a.continuity on every request, so they can be registered before the
// binder starts.
func (a *API) registerCollections(mux routeMux, prefix string, people *PrincipalStore) {
	h := newCollectionsAPI(a, prefix, people)
	if a.jobs != nil {
		a.jobs.onStopAll(h.audit.Close)
	}
	mux.HandleFunc(prefix+"/api/collections", h.serve)
	mux.HandleFunc(prefix+"/api/collections/", h.serve)
	mux.HandleFunc(prefix+"/api/queries", h.serveQueries)
	mux.HandleFunc(prefix+"/api/queries/", h.serveQueries)
	mux.HandleFunc(prefix+"/api/continuity/find", h.serveFind)
	mux.HandleFunc(prefix+"/api/continuity/asset/", h.serveAsset)
	mux.HandleFunc(prefix+"/api/agent-tokens", h.serveAgentTokens)
	mux.HandleFunc(prefix+"/api/agent-tokens/", h.serveAgentTokens)
}

func newCollectionsAPI(a *API, prefix string, people *PrincipalStore) *collectionsAPI {
	return &collectionsAPI{api: a, people: people, prefix: prefix,
		slots:     make(chan struct{}, collectionVolumeSlots),
		agentSlot: make(chan struct{}, collectionAgentSlots),
		adminSlot: make(chan struct{}, 1),
		inflight:  map[string]bool{},
		ipLimit:   newIPLimiter(collectionIPPerMinute, time.Now),
		macLimit: &ipLimiter{rate: collectionMacPerMinute / 60.0, burst: collectionMacBurst,
			buckets: map[string]*ipBucket{}, now: time.Now},
		refusals: &refusalLog{every: collectionRefusalLogEvery, last: map[string]time.Time{}, held: map[string]int{}},
		roots:    a.ownedSmartRoots,
		audit:    &agentAudit{dir: catalogAuditDir(people)},
	}
}

// httpError is an error with its status and message for the caller.
type httpError struct {
	code int
	msg  string
}

func (e *httpError) Error() string { return e.msg }

func failf(code int, format string, args ...any) error {
	return &httpError{code, fmt.Sprintf(format, args...)}
}

func (h *collectionsAPI) authenticate(r *http.Request) (collectionActor, error) {
	key := r.Header.Get("X-JuiceMount-Admin-Key")
	instance, token := r.Header.Get("X-JuiceMount-Mac-Instance"), r.Header.Get("X-JuiceMount-Mac-Token")
	switch {
	case bearerScheme(r):
		// First: an agent is never the open admin path.
		if key != "" || instance != "" || token != "" {
			return collectionActor{}, failf(http.StatusBadRequest, "send one credential: an agent token, a Mac's or the admin key")
		}
		return h.authenticateAgent(r)
	case h.api.adminKey == "" && instance == "" && token == "":
		return collectionActor{open: true}, nil // no admin key configured: LAN/dev mode, as a.auth
	case key != "":
		if subtle.ConstantTimeCompare([]byte(key), []byte(h.api.adminKey)) != 1 {
			return collectionActor{}, failf(http.StatusUnauthorized, "missing or invalid X-JuiceMount-Admin-Key")
		}
		return collectionActor{}, nil
	case instance != "" || token != "":
		if h.people == nil {
			return collectionActor{}, failf(http.StatusUnauthorized, "Mac enrollment is not configured on this Manager")
		}
		if !h.ipLimit.allow(clientIP(r)) {
			return collectionActor{}, failf(http.StatusTooManyRequests, "too many requests from this address; wait a minute")
		}
		mac, p, err := h.people.macPerson(instance, token, r.Header.Get("X-JuiceMount-Mac-Device"))
		switch {
		case errors.Is(err, errMacUnknown), errors.Is(err, errMacDevice):
			return collectionActor{}, failf(http.StatusUnauthorized, "%v", err)
		case errors.Is(err, errMacNotApproved), errors.Is(err, errMacDeviceClash), errors.Is(err, errMacNoPerson):
			return collectionActor{}, failf(http.StatusForbidden, "%v", err)
		case err != nil:
			return collectionActor{}, err
		}
		if !h.macLimit.allow(mac) {
			return collectionActor{}, failf(http.StatusTooManyRequests, "too many requests from this Mac; slow down")
		}
		return collectionActor{person: &p, mac: mac}, nil
	default:
		return collectionActor{}, failf(http.StatusUnauthorized, "missing or invalid X-JuiceMount-Admin-Key")
	}
}

// authenticateAgent checks an agent token (agenttokens.go): it acts as its
// person and spends the budget of the Mac that minted it. A refusal is
// logged (at most once a minute) with the token's ID when it names one,
// never the token.
func (h *collectionsAPI) authenticateAgent(r *http.Request) (collectionActor, error) {
	if h.people == nil {
		return collectionActor{}, failf(http.StatusUnauthorized, "agent tokens need Mac enrollment, which is not configured on this Manager")
	}
	if !h.ipLimit.allow(clientIP(r)) {
		return collectionActor{}, failf(http.StatusTooManyRequests, "too many requests from this address; wait a minute")
	}
	_, bearer, _ := strings.Cut(strings.TrimSpace(r.Header.Get("Authorization")), " ")
	mac, p, t, err := h.people.agentPerson(strings.TrimSpace(bearer))
	if err != nil {
		named := "a malformed or unknown token"
		if t.ID != "" {
			named = fmt.Sprintf("agent token %s (%q) of %s", t.ID, t.Name, t.Person)
			h.audit.refusedToken(t, t.Mac, fmt.Sprintf("%s %s from %s: %v", r.Method, r.URL.Path, clientIP(r), err))
		}
		h.refusals.note("agent-tokens", fmt.Sprintf("manager: collections: refused %s %s with %s from %s: %v", r.Method, r.URL.Path, named, clientIP(r), err))
		return collectionActor{}, failf(http.StatusUnauthorized, "%v", err)
	}
	if !h.macLimit.allow(mac) {
		return collectionActor{}, failf(http.StatusTooManyRequests, "too many requests from this Mac; slow down")
	}
	return collectionActor{person: &p, mac: mac, agent: &t}, nil
}

// readsVolume: the request resolves files on the volume (every write, and
// member pages), so it takes a slot.
func readsVolume(method string, parts []string) bool {
	return method != http.MethodGet || len(parts) == 2 && parts[1] == "members"
}

// routeParts splits the request path below base into its segments, one
// trailing slash allowed. A path with an empty, "." or ".." segment (an
// encoded slash makes one) is refused (ok false), never cleaned: the slot
// decision and the routing read these same parts, so they cannot disagree.
func routeParts(p, base string) (parts []string, ok bool) {
	rest, ok := strings.CutPrefix(p, base)
	if !ok {
		return nil, false
	}
	rest = strings.TrimSuffix(rest, "/")
	if rest == "" {
		return []string{}, true
	}
	if rest[0] != '/' {
		return nil, false
	}
	parts = strings.Split(rest[1:], "/")
	for _, s := range parts {
		if s == "" || s == "." || s == ".." {
			return nil, false
		}
	}
	return parts, true
}

// acquire takes a volume-read slot for actor: one request per actor at a
// time (a person's Macs and agents count as one actor); people's Macs share
// the common slots, agents their own pool, the admin has its own.
func (h *collectionsAPI) acquire(ctx context.Context, actor collectionActor) (release func(), err error) {
	key, slots := actor.id(), h.slots
	switch {
	case actor.person == nil:
		slots = h.adminSlot
	case actor.agent != nil:
		slots = h.agentSlot
	}
	h.mu.Lock()
	if h.inflight[key] {
		h.mu.Unlock()
		return nil, failf(http.StatusServiceUnavailable, "a previous collection request of yours is still being checked; try again when it finishes")
	}
	h.inflight[key] = true
	h.mu.Unlock()
	done := func() {
		h.mu.Lock()
		delete(h.inflight, key)
		h.mu.Unlock()
	}
	wait := time.NewTimer(collectionBusyWait)
	defer wait.Stop()
	select {
	case slots <- struct{}{}:
		return func() { <-slots; done() }, nil
	case <-wait.C:
	case <-ctx.Done():
	}
	done()
	return nil, failf(http.StatusServiceUnavailable, "the Manager is busy checking other collection requests; try again")
}

func (h *collectionsAPI) serve(w http.ResponseWriter, r *http.Request) {
	h.serveWith(w, r, "/api/collections", readsVolume, h.route)
}

// serveWith splits the path below base (refusing a non-canonical one),
// authenticates, takes a volume-read slot when reads says so for these
// parts, and routes them.
func (h *collectionsAPI) serveWith(w http.ResponseWriter, r *http.Request, base string, reads func(method string, parts []string) bool,
	route func(http.ResponseWriter, *http.Request, *continuityService, collectionActor, []string) error) {
	svc := h.api.continuity
	if svc == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "continuity is off (JM_CONTINUITY)"})
		return
	}
	parts, ok := routeParts(r.URL.Path, h.prefix+base)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no such route"})
		return
	}
	actor, err := h.authenticate(r)
	// An agent's request is shorter, so the one agent slot turns over.
	timeout := collectionRequestTimeout
	if actor.agent != nil {
		timeout = agentRequestTimeout
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	r = r.WithContext(ctx)
	if err == nil && actor.agent != nil && !agentMay(r.Method, base, parts) {
		err = failf(http.StatusForbidden, "agent tokens may not %s %s (agents read, find files and edit collections)", r.Method, strings.Join(append([]string{base}, parts...), "/"))
	}
	if err == nil && reads(r.Method, parts) {
		var release func()
		if release, err = h.acquire(ctx, actor); err == nil {
			defer release()
		}
	}
	if err == nil {
		err = route(w, r, svc, actor, parts)
	}
	if err == nil {
		return
	}
	var he *httpError
	if !errors.As(err, &he) {
		he = collectionFailure(err)
	}
	if he.code == http.StatusForbidden && actor.person != nil {
		by := ""
		if actor.agent != nil {
			by = fmt.Sprintf(" through agent token %s (%q)", actor.agent.ID, actor.agent.Name)
		}
		h.refusals.note(actor.person.ID, fmt.Sprintf("manager: collections: refused %s %s for %s (%s)%s: %s", r.Method, r.URL.Path, actor.person.ID, actor.person.Name, by, he.msg))
	}
	if actor.agent != nil && r.Method != http.MethodGet && (he.code == http.StatusForbidden || he.code == http.StatusNotFound) {
		h.audit.refused(actor.auditWho(), "refused", fmt.Sprintf("%s %s: %s", r.Method, strings.TrimPrefix(r.URL.Path, h.prefix), he.msg))
	}
	writeJSON(w, he.code, map[string]string{"error": he.msg})
}

func collectionFailure(err error) *httpError {
	switch {
	case errors.Is(err, errCollectionInput), errors.Is(err, catalog.ErrInvalid):
		return &httpError{http.StatusBadRequest, err.Error()}
	case errors.Is(err, catalog.ErrNotFound):
		return &httpError{http.StatusNotFound, "no such collection or asset"}
	case errors.Is(err, catalog.ErrChangedRequest):
		return &httpError{http.StatusConflict, "operation_id was already used for a different request"}
	case errors.Is(err, catalog.ErrQueryLimit):
		return &httpError{http.StatusConflict, fmt.Sprintf("a library holds at most %d saved queries; delete one first", catalog.MaxQueries)}
	case errors.Is(err, catalog.ErrCollectionFull):
		return &httpError{http.StatusConflict, fmt.Sprintf("a collection holds at most %d members", catalog.MaxCollectionMembers)}
	case errors.Is(err, catalog.ErrConflict):
		return &httpError{http.StatusConflict, "the collection changed while this change was being checked; read it again and retry"}
	case errors.Is(err, binder.ErrNotReady):
		return &httpError{http.StatusServiceUnavailable, err.Error()}
	case errors.Is(err, context.DeadlineExceeded):
		return &httpError{http.StatusServiceUnavailable, "checking the files took too long; try again, or with fewer members"}
	default:
		log.Printf("manager: collections: %v", err)
		return &httpError{http.StatusInternalServerError, "the catalog request failed"}
	}
}

// route dispatches
//
//	GET    /api/collections                      list (after, limit)
//	POST   /api/collections                      create {name, members?, operation_id?}
//	GET    /api/collections/{id}                 one collection
//	DELETE /api/collections/{id}                 delete (body optional: {operation_id})
//	POST   /api/collections/{id}/rename          {name, operation_id?}
//	POST   /api/collections/{id}/delete          {operation_id?} or no body
//	GET    /api/collections/{id}/members         members with current paths (after, limit)
//	POST   /api/collections/{id}/members/add     {members, operation_id?}
//	POST   /api/collections/{id}/members/remove  {members, operation_id?}
func (h *collectionsAPI) route(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor, parts []string) error {
	if r.Method != http.MethodGet && actor.open && h.people != nil && h.people.grantsInForce() {
		return failf(http.StatusForbidden, "no admin key is configured while people's grants are in force; set an admin key to change collections as the admin")
	}
	library, err := svc.store.LibraryID()
	if err != nil {
		return err
	}
	c := &collectionCall{svc: svc, actor: actor, library: library, w: w, r: r, audit: h.audit, people: h.people}
	want := func(method string) error {
		if r.Method != method {
			return failf(http.StatusMethodNotAllowed, "%s only", method)
		}
		return nil
	}
	if len(parts) == 0 {
		switch r.Method {
		case http.MethodGet:
			return c.list()
		case http.MethodPost:
			return c.create()
		}
		return failf(http.StatusMethodNotAllowed, "GET or POST only")
	}
	id := catalog.CollectionID(parts[0])
	if id.Validate() != nil {
		return failf(http.StatusNotFound, "no such collection")
	}
	switch strings.Join(parts[1:], "/") {
	case "":
		switch r.Method {
		case http.MethodGet:
			return c.get(id)
		case http.MethodDelete:
			return c.remove(id)
		}
		return failf(http.StatusMethodNotAllowed, "GET or DELETE only")
	case "rename":
		if err := want(http.MethodPost); err != nil {
			return err
		}
		return c.rename(id)
	case "delete":
		if err := want(http.MethodPost); err != nil {
			return err
		}
		return c.remove(id)
	case "members":
		if err := want(http.MethodGet); err != nil {
			return err
		}
		return c.members(id)
	case "members/add", "members/remove":
		if err := want(http.MethodPost); err != nil {
			return err
		}
		return c.editMembers(id, parts[2] == "add")
	}
	return failf(http.StatusNotFound, "no such route")
}

type collectionCall struct {
	svc     *continuityService
	actor   collectionActor
	library continuity.LibraryID
	w       http.ResponseWriter
	r       *http.Request
	audit   *agentAudit     // records an agent's edits
	people  *PrincipalStore // re-checks an agent's token before an edit
}

func (c *collectionCall) ctx() context.Context { return c.r.Context() }

// CollectionView is one collection as the API returns it.
type CollectionView struct {
	ID              catalog.CollectionID `json:"id"`
	Name            string               `json:"name"`
	Members         int64                `json:"members"`
	CreatedSequence int64                `json:"created_sequence"`
	UpdatedSequence int64                `json:"updated_sequence"`
	Removed         bool                 `json:"removed,omitempty"`
}

// MemberView is one member, its file's current path resolved from the
// volume: state "present" (path set), "missing" (no visible name: the file
// is gone, in the trash or under hidden names only; the member stays) or
// "unknown" (the volume could not be read).
type MemberView struct {
	AssetID       continuity.AssetID `json:"asset_id"`
	Label         string             `json:"label"`
	Path          string             `json:"path,omitempty"`
	State         string             `json:"state"`
	Error         string             `json:"error,omitempty"`
	AddedSequence int64              `json:"added_sequence"`
}

func collectionView(c catalog.Collection) CollectionView {
	return CollectionView{c.ID, c.Name, c.Members, c.CreatedSequence, c.UpdatedSequence, c.Removed}
}

func memberState(v binder.AssetView) string {
	switch {
	case v.ResolveError != "":
		return "unknown"
	case v.Path != "":
		return "present"
	default:
		return "missing"
	}
}

func pageArgs(r *http.Request, def, max int) (string, int, error) {
	q := r.URL.Query()
	limit := def
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > max {
			return "", 0, fmt.Errorf("%w: limit is 1-%d", errCollectionInput, max)
		}
		limit = n
	}
	return q.Get("after"), limit, nil
}

func (c *collectionCall) list() error {
	after, limit, err := pageArgs(c.r, 100, catalog.MaxPageSize)
	if err != nil {
		return err
	}
	page, err := c.svc.store.ListCollections(c.ctx(), c.library, catalog.CollectionID(after), limit)
	if err != nil {
		return err
	}
	out := struct {
		Collections []CollectionView `json:"collections"`
		Next        string           `json:"next,omitempty"`
	}{Collections: []CollectionView{}}
	for _, col := range page {
		out.Collections = append(out.Collections, collectionView(col))
	}
	if len(page) == limit {
		out.Next = string(page[len(page)-1].ID)
	}
	writeJSON(c.w, http.StatusOK, out)
	return nil
}

// active reads a collection that has not been removed.
func (c *collectionCall) active(id catalog.CollectionID) (catalog.Collection, error) {
	col, err := c.svc.store.GetCollection(c.ctx(), c.library, id)
	if err == nil && col.Removed {
		err = catalog.ErrNotFound
	}
	if errors.Is(err, catalog.ErrNotFound) {
		return col, failf(http.StatusNotFound, "no such collection")
	}
	return col, err
}

func (c *collectionCall) get(id catalog.CollectionID) error {
	col, err := c.active(id)
	if err != nil {
		return err
	}
	writeJSON(c.w, http.StatusOK, collectionView(col))
	return nil
}

func (c *collectionCall) members(id catalog.CollectionID) error {
	col, err := c.active(id)
	if err != nil {
		return err
	}
	after, limit, err := pageArgs(c.r, 50, maxMemberPage)
	if err != nil {
		return err
	}
	// Withheld: members an agent may not see (agentVisible), counted
	// without names; the page then reads on, up to agentFindResolveBudget
	// members, to fill itself. Macs and the admin see every member.
	out := struct {
		Collection CollectionView `json:"collection"`
		Members    []MemberView   `json:"members"`
		Next       string         `json:"next,omitempty"`
		Withheld   int            `json:"withheld,omitempty"`
	}{Collection: collectionView(col), Members: []MemberView{}}
	cursor, read := continuity.AssetID(after), 0
	for {
		page, err := c.svc.store.ListCollectionMembers(c.ctx(), c.library, id, cursor, limit)
		if err != nil {
			return err
		}
		stopped := false
		for _, m := range page {
			if err := c.ctx().Err(); err != nil {
				return err
			}
			if c.actor.agent != nil && read >= agentFindResolveBudget {
				stopped = true
				break
			}
			read++
			v, err := c.svc.binder.AssetByID(c.ctx(), m.AssetID)
			if err != nil {
				return err
			}
			cursor = m.AssetID
			if !c.agentVisible(v.Path, v.ResolveError) {
				out.Withheld++
				continue
			}
			label := v.Label
			if c.actor.agent != nil && label != path.Base(v.Path) {
				label = "" // a first name from outside the grants
			}
			out.Members = append(out.Members, MemberView{AssetID: m.AssetID, Label: label, Path: v.Path,
				State: memberState(v), Error: v.ResolveError, AddedSequence: m.UpdatedSequence})
			if len(out.Members) == limit {
				stopped = true
				break
			}
		}
		if stopped {
			out.Next = string(cursor)
			break
		}
		if len(page) < limit {
			break
		}
	}
	writeJSON(c.w, http.StatusOK, out)
	return nil
}

// agentVisible: an agent may see a file at volumePath (its live path, read
// without error) only inside its person's grants, as discovery does;
// everyone else sees every file (Teams v1 reads are shared).
func (c *collectionCall) agentVisible(volumePath, resolveError string) bool {
	if c.actor.agent == nil {
		return true
	}
	return volumePath != "" && resolveError == "" && c.actor.covers(volumePath)
}

// memberRef names a member by asset ID or by volume path (exactly one).
type memberRef struct {
	AssetID continuity.AssetID `json:"asset_id,omitempty"`
	Path    string             `json:"path,omitempty"`
}

type collectionBody struct {
	Name        *string     `json:"name"`
	Members     []memberRef `json:"members"`
	OperationID string      `json:"operation_id"`
}

// body decodes the request body; an empty body is an empty request.
func (c *collectionCall) body() (collectionBody, error) {
	var b collectionBody
	dec := json.NewDecoder(io.LimitReader(c.r.Body, maxCollectionBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil && err != io.EOF {
		return b, fmt.Errorf("%w: invalid JSON", errCollectionInput)
	}
	if b.OperationID != "" && !collectionKeyRe.MatchString(b.OperationID) {
		return b, fmt.Errorf("%w: operation_id is 1-64 letters, digits, - or _", errCollectionInput)
	}
	if len(b.Members) > maxCollectionRequestMembers {
		return b, fmt.Errorf("%w: at most %d members per request", errCollectionInput, maxCollectionRequestMembers)
	}
	return b, nil
}

func cleanCollectionName(p *string) (string, error) {
	if p == nil {
		return "", fmt.Errorf("%w: name is required", errCollectionInput)
	}
	n := strings.TrimSpace(*p)
	if n == "" || len(n) > 256 || strings.ContainsRune(n, 0) {
		return "", fmt.Errorf("%w: a name is 1-256 bytes", errCollectionInput)
	}
	return n, nil
}

// operationID derives the catalog operation ID from the caller's key (a
// random one when it gave none) and the actor.
func (c *collectionCall) operationID(key string) (string, error) {
	if key == "" {
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}
		key = hex.EncodeToString(b)
	}
	sum := sha256.Sum256([]byte("juicemount.collections.operation\x00" + c.actor.opKey() + "\x00" + key))
	return "col-" + hex.EncodeToString(sum[:20]), nil
}

// resolve turns member references into asset IDs, in request order,
// without duplicates. A path is looked up on the volume, never through a
// path hint.
func (c *collectionCall) resolve(refs []memberRef) ([]continuity.AssetID, error) {
	out := make([]continuity.AssetID, 0, len(refs))
	seen := map[continuity.AssetID]bool{}
	for _, ref := range refs {
		var id continuity.AssetID
		var err error
		switch {
		case (ref.AssetID == "") == (ref.Path == ""):
			return nil, fmt.Errorf("%w: a member names exactly one of asset_id or path", errCollectionInput)
		case ref.Path != "":
			id, err = c.svc.binder.AssetAt(c.ctx(), ref.Path)
			if errors.Is(err, catalog.ErrNotFound) {
				return nil, failf(http.StatusNotFound, "no catalogued file at %q", ref.Path)
			}
		default:
			id = ref.AssetID
			_, err = c.svc.store.GetAsset(c.ctx(), c.library, id)
			if errors.Is(err, catalog.ErrNotFound) {
				return nil, failf(http.StatusNotFound, "asset %s is not in the catalog", ref.AssetID)
			}
		}
		switch {
		case errors.Is(err, catalog.ErrInvalid):
			return nil, fmt.Errorf("%w: bad asset_id or path", errCollectionInput)
		case errors.Is(err, binder.ErrUnresolved):
			return nil, failf(http.StatusServiceUnavailable, "cannot read %q on the volume; try again", ref.Path)
		case err != nil:
			return nil, err
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// authorize checks a person's grants against the files of the given
// members, one at a time, stopping at the first refusal. The admin is
// always allowed.
func (c *collectionCall) authorize(members []continuity.AssetID) error {
	if c.actor.person == nil {
		return nil
	}
	if !c.actor.writesAnywhere() {
		return failf(http.StatusForbidden, "%s has no write access, so may not change collections", c.actor.person.Name)
	}
	for _, id := range members {
		a, err := c.svc.binder.CheckAccess(c.ctx(), id, c.actor.covers)
		switch {
		case errors.Is(err, binder.ErrUnresolved):
			return failf(http.StatusServiceUnavailable, "cannot check the names of %s against grants (%v); try again", id, err)
		case err != nil:
			return err
		case a.Missing || a.Allowed:
		case c.actor.agent != nil:
			// An agent is not told names outside its person's grants.
			return failf(http.StatusForbidden, "%s may not change %s: no project grant covers it", c.actor.person.Name, id)
		default:
			return failf(http.StatusForbidden, "%s may not write at /%s: no project grant covers it", c.actor.person.Name, a.Name)
		}
	}
	return nil
}

// authorizeAll checks every active member of a collection (rename, delete).
func (c *collectionCall) authorizeAll(id catalog.CollectionID) error {
	if c.actor.person == nil {
		return nil
	}
	if err := c.authorize(nil); err != nil {
		return err
	}
	var after continuity.AssetID
	for {
		page, err := c.svc.store.ListCollectionMembers(c.ctx(), c.library, id, after, 200)
		if err != nil || len(page) == 0 {
			return err
		}
		ids := make([]continuity.AssetID, len(page))
		for i, m := range page {
			ids[i] = m.AssetID
		}
		if err := c.authorize(ids); err != nil {
			return err
		}
		after = page[len(page)-1].AssetID
	}
}

// recorded is the operation already recorded under opID (nil: none).
func (c *collectionCall) recorded(opID string) (*catalog.Operation, error) {
	ev, err := c.svc.store.GetEvent(c.ctx(), c.library, opID)
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return &ev.Operation, nil
}

// recordedMembers: the members an operation added or removed, in order.
func recordedMembers(op *catalog.Operation) []continuity.AssetID {
	var out []continuity.AssetID
	for _, ch := range op.Changes {
		switch {
		case ch.AddCollectionMember != nil:
			out = append(out, ch.AddCollectionMember.AssetID)
		case ch.RemoveCollectionMember != nil:
			out = append(out, ch.RemoveCollectionMember.AssetID)
		}
	}
	return out
}

// replayMembers gives a retry's members from the operation it repeats, so
// a member whose file has gone since does not make the retry 404. A member
// named by asset ID is taken as given; a path is looked up on the volume,
// and one that no longer names a catalogued file is left out (gone). With
// none gone the members are the request's, in order, as resolve gives them
// (the catalog compares them with the recorded ones). Otherwise every member
// found must be one the operation named, and the gone paths must be enough
// to account for the rest; the recorded members are replayed. A request
// that cannot be the original is ErrChangedRequest (409).
func (c *collectionCall) replayMembers(refs []memberRef, recorded []continuity.AssetID) ([]continuity.AssetID, error) {
	var ids []continuity.AssetID
	seen := map[continuity.AssetID]bool{}
	gone := 0
	for _, ref := range refs {
		id := ref.AssetID
		switch {
		case (ref.AssetID == "") == (ref.Path == ""):
			return nil, fmt.Errorf("%w: a member names exactly one of asset_id or path", errCollectionInput)
		case ref.Path != "":
			var err error
			id, err = c.svc.binder.AssetAt(c.ctx(), ref.Path)
			switch {
			case errors.Is(err, catalog.ErrNotFound):
				gone++
				continue
			case errors.Is(err, catalog.ErrInvalid):
				return nil, fmt.Errorf("%w: bad asset_id or path", errCollectionInput)
			case errors.Is(err, binder.ErrUnresolved):
				return nil, failf(http.StatusServiceUnavailable, "cannot read %q on the volume; try again", ref.Path)
			case err != nil:
				return nil, err
			}
		}
		if !seen[id] {
			seen[id] = true
			ids = append(ids, id)
		}
	}
	if gone == 0 {
		return ids, nil
	}
	named := make(map[continuity.AssetID]bool, len(recorded))
	for _, id := range recorded {
		named[id] = true
	}
	for _, id := range ids {
		if !named[id] {
			return nil, catalog.ErrChangedRequest
		}
	}
	if len(ids)+gone < len(recorded) {
		return nil, catalog.ErrChangedRequest
	}
	return recorded, nil
}

// change records one write. An exact retry (prior: its operation ID is in
// the history) returns the original result without a second authorization:
// it was authorized when it committed, and only its own actor can name it.
// Otherwise a guarded change (rename, delete: authorized against every
// member) reads the collection's sequence, authorizes, and writes with that
// sequence as a precondition (ExpectCollection), so a change to the
// collection in between makes it 409.
func (c *collectionCall) change(opID string, prior *catalog.Operation, id catalog.CollectionID, guarded bool, authorize func() error, changes []catalog.Change) error {
	expect := func(seq int64) []catalog.Change {
		return append([]catalog.Change{{ExpectCollection: &catalog.ExpectCollection{CollectionID: id, UpdatedSequence: seq}}}, changes...)
	}
	if prior != nil {
		if guarded {
			for _, ch := range prior.Changes {
				if ch.ExpectCollection != nil {
					changes = expect(ch.ExpectCollection.UpdatedSequence)
					break
				}
			}
		}
		return c.apply(opID, id, changes)
	}
	if guarded {
		col, err := c.active(id)
		if err != nil {
			return err
		}
		changes = expect(col.UpdatedSequence)
	}
	if err := authorize(); err != nil {
		return err
	}
	if collectionBeforeApply != nil {
		collectionBeforeApply()
	}
	return c.apply(opID, id, changes)
}

// apply records the change and answers with the collection as it is now.
// An agent's change is in the catalog audit log first (agenttokens.go).
func (c *collectionCall) apply(opID string, id catalog.CollectionID, changes []catalog.Change) error {
	var unit uint64
	if c.actor.agent != nil {
		// The token may have been revoked since the request began.
		if err := c.people.agentTokenWorks(c.actor.agent.ID); err != nil {
			return failf(http.StatusUnauthorized, "%v", err)
		}
		rec := c.actor.auditWho()
		rec.Action, rec.Detail = describeCollectionChange(opID, id, changes)
		var err error
		if unit, err = c.audit.intent(rec); err != nil {
			return err
		}
		// And again once the intent is on disk (the write can take a while).
		if err := c.people.agentTokenWorks(c.actor.agent.ID); err != nil {
			refused := failf(http.StatusUnauthorized, "%v", err)
			c.audit.outcome(unit, c.actor.auditWho(), refused)
			return refused
		}
	}
	receipt, err := c.svc.store.Apply(c.ctx(), catalog.Operation{
		Version: catalog.OperationVersion, LibraryID: c.library, ID: opID, Actor: c.actor.id(), Changes: changes})
	if c.actor.agent != nil {
		c.audit.outcome(unit, c.actor.auditWho(), err)
	}
	if err != nil {
		return err
	}
	c.svc.collectionsChanged()
	col, err := c.svc.store.GetCollection(c.ctx(), c.library, id)
	if err != nil {
		return err
	}
	writeJSON(c.w, http.StatusOK, map[string]any{
		"collection": collectionView(col),
		"operation":  map[string]any{"id": opID, "sequence": receipt.Sequence, "actor": c.actor.id()},
	})
	return nil
}

func (c *collectionCall) create() error {
	b, err := c.body()
	if err != nil {
		return err
	}
	name, err := cleanCollectionName(b.Name)
	if err != nil {
		return err
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	w, err := c.writeMembers(opID, b.Members)
	if err != nil {
		return err
	}
	// The identity follows from the operation, so a retry names the same one.
	sum := sha256.Sum256([]byte("juicemount.collections.id\x00" + opID))
	id := catalog.CollectionID("col_" + hex.EncodeToString(sum[:16]))
	changes := []catalog.Change{{CreateCollection: &catalog.CreateCollection{CollectionID: id, Name: name}}}
	for _, m := range w.members {
		changes = append(changes, catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: m}})
	}
	return c.change(opID, w.op, id, false, func() error { return c.authorize(w.members) }, changes)
}

func (c *collectionCall) rename(id catalog.CollectionID) error {
	b, err := c.body()
	if err != nil {
		return err
	}
	name, err := cleanCollectionName(b.Name)
	if err != nil {
		return err
	}
	if len(b.Members) > 0 {
		return fmt.Errorf("%w: rename takes no members", errCollectionInput)
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	prior, err := c.recorded(opID)
	if err != nil {
		return err
	}
	return c.change(opID, prior, id, true, func() error { return c.authorizeAll(id) },
		[]catalog.Change{{RenameCollection: &catalog.RenameCollection{CollectionID: id, Name: name}}})
}

func (c *collectionCall) remove(id catalog.CollectionID) error {
	b, err := c.body()
	if err != nil {
		return err
	}
	if b.Name != nil || len(b.Members) > 0 {
		return fmt.Errorf("%w: delete takes only operation_id", errCollectionInput)
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	prior, err := c.recorded(opID)
	if err != nil {
		return err
	}
	return c.change(opID, prior, id, true, func() error { return c.authorizeAll(id) },
		[]catalog.Change{{RemoveCollection: &catalog.RemoveCollection{CollectionID: id}}})
}

func (c *collectionCall) editMembers(id catalog.CollectionID, add bool) error {
	b, err := c.body()
	if err != nil {
		return err
	}
	if b.Name != nil || len(b.Members) == 0 {
		return fmt.Errorf("%w: give members (and no name)", errCollectionInput)
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	w, err := c.writeMembers(opID, b.Members)
	if err != nil {
		return err
	}
	members := w.members
	changes := make([]catalog.Change, 0, len(members))
	for _, m := range members {
		if add {
			changes = append(changes, catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: m}})
		} else {
			changes = append(changes, catalog.Change{RemoveCollectionMember: &catalog.RemoveCollectionMember{CollectionID: id, AssetID: m}})
		}
	}
	// Only the named members' grants matter: concurrent adders do not
	// conflict. The catalog refuses a removed collection (404).
	return c.change(opID, w.op, id, false, func() error { return c.authorize(members) }, changes)
}

// memberWrite is a member write's recorded operation (nil: a new request)
// and its members: for a retry, checked against the operation it repeats
// (replayMembers); otherwise resolved on the volume.
type memberWrite struct {
	op      *catalog.Operation
	members []continuity.AssetID
}

func (c *collectionCall) writeMembers(opID string, refs []memberRef) (memberWrite, error) {
	op, err := c.recorded(opID)
	if err != nil {
		return memberWrite{}, err
	}
	var members []continuity.AssetID
	if op != nil {
		members, err = c.replayMembers(refs, recordedMembers(op))
	} else {
		members, err = c.resolve(refs)
	}
	return memberWrite{op, members}, err
}

// describeCollectionChange names a collection write for the audit log: the
// action and what it changes.
func describeCollectionChange(opID string, id catalog.CollectionID, changes []catalog.Change) (string, string) {
	action, name := "collection-change", ""
	var added, removed []string
	for _, ch := range changes {
		switch {
		case ch.CreateCollection != nil:
			action, name = "collection-create", ch.CreateCollection.Name
		case ch.RenameCollection != nil:
			action, name = "collection-rename", ch.RenameCollection.Name
		case ch.RemoveCollection != nil:
			action = "collection-delete"
		case ch.AddCollectionMember != nil:
			added = append(added, string(ch.AddCollectionMember.AssetID))
		case ch.RemoveCollectionMember != nil:
			removed = append(removed, string(ch.RemoveCollectionMember.AssetID))
		}
	}
	switch {
	case action != "collection-change":
	case len(added) > 0 && len(removed) == 0:
		action = "collection-add-members"
	case len(removed) > 0 && len(added) == 0:
		action = "collection-remove-members"
	}
	detail := "collection " + string(id)
	if name != "" {
		detail += fmt.Sprintf(" name %q", name)
	}
	if len(added) > 0 {
		detail += " add " + strings.Join(added, " ")
	}
	if len(removed) > 0 {
		detail += " remove " + strings.Join(removed, " ")
	}
	return action, detail + "; operation " + opID
}
