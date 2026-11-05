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
// while grants are in force.
//
// What. Reads are shared (Teams v1: any authenticated caller). A person may
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
// and is logged (at most once a minute per person); the Manager writes no
// Teams audit records (the proxy's audit log has a single writer), so the
// catalog's event history is the record of what was changed and by whom.
//
// Cost. Requests that read the volume have a 30 s deadline and take a slot:
// people share two, the admin has one of its own, and each actor has at
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
// different request under the same operation_id is 409.

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

// collectionActor is who is calling: the admin, or a person.
type collectionActor struct {
	person *Person // nil: admin
	// open: the admin path without a key (none is configured).
	open bool
}

func (c collectionActor) id() string {
	if c.person == nil {
		return collectionAdminActor
	}
	return c.person.ID
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
	slots     chan struct{} // people's volume-read slots
	adminSlot chan struct{} // the admin's own
	mu        sync.Mutex
	inflight  map[string]bool // actors with a volume-reading request running
	ipLimit   *ipLimiter      // per source address, before the token is checked
	macLimit  *ipLimiter      // per Mac principal, after
	refusals  *refusalLog
	roots     func() []string // the smart-folder roots the Manager owns (queries.go)
}

// registerCollections wires /api/collections and /api/queries (queries.go,
// sharing the authentication, budgets and slots). The handlers do their own
// authentication (admin key or a person's Mac) and read a.continuity on
// every request, so they can be registered before the binder starts.
func (a *API) registerCollections(mux *http.ServeMux, prefix string, people *PrincipalStore) {
	h := newCollectionsAPI(a, prefix, people)
	mux.HandleFunc(prefix+"/api/collections", h.serve)
	mux.HandleFunc(prefix+"/api/collections/", h.serve)
	mux.HandleFunc(prefix+"/api/queries", h.serveQueries)
	mux.HandleFunc(prefix+"/api/queries/", h.serveQueries)
}

func newCollectionsAPI(a *API, prefix string, people *PrincipalStore) *collectionsAPI {
	return &collectionsAPI{api: a, people: people, prefix: prefix,
		slots:     make(chan struct{}, collectionVolumeSlots),
		adminSlot: make(chan struct{}, 1),
		inflight:  map[string]bool{},
		ipLimit:   newIPLimiter(collectionIPPerMinute, time.Now),
		macLimit: &ipLimiter{rate: collectionMacPerMinute / 60.0, burst: collectionMacBurst,
			buckets: map[string]*ipBucket{}, now: time.Now},
		refusals: &refusalLog{every: collectionRefusalLogEvery, last: map[string]time.Time{}, held: map[string]int{}},
		roots:    a.ownedSmartRoots,
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
		return collectionActor{person: &p}, nil
	default:
		return collectionActor{}, failf(http.StatusUnauthorized, "missing or invalid X-JuiceMount-Admin-Key")
	}
}

// readsVolume: the request resolves files on the volume (every write, and
// member pages), so it takes a slot.
func readsVolume(r *http.Request) bool {
	return r.Method != http.MethodGet || strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/members")
}

// acquire takes a volume-read slot for actor: one request per actor at a
// time; people share the common slots, the admin has its own.
func (h *collectionsAPI) acquire(ctx context.Context, actor collectionActor) (release func(), err error) {
	key, slots := actor.id(), h.slots
	if actor.person == nil {
		slots = h.adminSlot
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
	h.serveWith(w, r, readsVolume(r), h.route)
}

// serveWith authenticates, takes a volume-read slot when reads is set, and
// routes.
func (h *collectionsAPI) serveWith(w http.ResponseWriter, r *http.Request, reads bool,
	route func(http.ResponseWriter, *http.Request, *continuityService, collectionActor) error) {
	svc := h.api.continuity
	if svc == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "continuity is off (JM_CONTINUITY)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), collectionRequestTimeout)
	defer cancel()
	r = r.WithContext(ctx)
	actor, err := h.authenticate(r)
	if err == nil && reads {
		var release func()
		if release, err = h.acquire(ctx, actor); err == nil {
			defer release()
		}
	}
	if err == nil {
		err = route(w, r, svc, actor)
	}
	if err == nil {
		return
	}
	var he *httpError
	if !errors.As(err, &he) {
		he = collectionFailure(err)
	}
	if he.code == http.StatusForbidden && actor.person != nil {
		h.refusals.note(actor.person.ID, fmt.Sprintf("manager: collections: refused %s %s for %s (%s): %s", r.Method, r.URL.Path, actor.person.ID, actor.person.Name, he.msg))
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
func (h *collectionsAPI) route(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor) error {
	rest := strings.TrimPrefix(r.URL.Path, h.prefix+"/api/collections")
	rest = strings.Trim(rest, "/")
	parts := []string{}
	if rest != "" {
		parts = strings.Split(rest, "/")
	}
	if r.Method != http.MethodGet && actor.open && h.people != nil && h.people.grantsInForce() {
		return failf(http.StatusForbidden, "no admin key is configured while people's grants are in force; set an admin key to change collections as the admin")
	}
	library, err := svc.store.LibraryID()
	if err != nil {
		return err
	}
	c := &collectionCall{svc: svc, actor: actor, library: library, w: w, r: r}
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
	page, err := c.svc.store.ListCollectionMembers(c.ctx(), c.library, id, continuity.AssetID(after), limit)
	if err != nil {
		return err
	}
	out := struct {
		Collection CollectionView `json:"collection"`
		Members    []MemberView   `json:"members"`
		Next       string         `json:"next,omitempty"`
	}{Collection: collectionView(col), Members: []MemberView{}}
	for _, m := range page {
		if err := c.ctx().Err(); err != nil {
			return err
		}
		v, err := c.svc.binder.AssetByID(c.ctx(), m.AssetID)
		if err != nil {
			return err
		}
		out.Members = append(out.Members, MemberView{AssetID: m.AssetID, Label: v.Label, Path: v.Path,
			State: memberState(v), Error: v.ResolveError, AddedSequence: m.UpdatedSequence})
	}
	if len(page) == limit {
		out.Next = string(page[len(page)-1].AssetID)
	}
	writeJSON(c.w, http.StatusOK, out)
	return nil
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
	sum := sha256.Sum256([]byte("juicemount.collections.operation\x00" + c.actor.id() + "\x00" + key))
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

// change records one write. An exact retry (its operation ID is in the
// history) returns the original result without a second authorization: it
// was authorized when it committed, and only its own actor can name it.
// Otherwise a guarded change (rename, delete: authorized against every
// member) reads the collection's sequence, authorizes, and writes with that
// sequence as a precondition (ExpectCollection), so a change to the
// collection in between makes it 409.
func (c *collectionCall) change(opID string, id catalog.CollectionID, guarded bool, authorize func() error, changes []catalog.Change) error {
	expect := func(seq int64) []catalog.Change {
		return append([]catalog.Change{{ExpectCollection: &catalog.ExpectCollection{CollectionID: id, UpdatedSequence: seq}}}, changes...)
	}
	prior, err := c.svc.store.GetEvent(c.ctx(), c.library, opID)
	switch {
	case err == nil:
		if guarded {
			for _, ch := range prior.Operation.Changes {
				if ch.ExpectCollection != nil {
					changes = expect(ch.ExpectCollection.UpdatedSequence)
					break
				}
			}
		}
		return c.apply(opID, id, changes)
	case !errors.Is(err, catalog.ErrNotFound):
		return err
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
func (c *collectionCall) apply(opID string, id catalog.CollectionID, changes []catalog.Change) error {
	receipt, err := c.svc.store.Apply(c.ctx(), catalog.Operation{
		Version: catalog.OperationVersion, LibraryID: c.library, ID: opID, Actor: c.actor.id(), Changes: changes})
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
	members, err := c.resolve(b.Members)
	if err != nil {
		return err
	}
	opID, err := c.operationID(b.OperationID)
	if err != nil {
		return err
	}
	// The identity follows from the operation, so a retry names the same one.
	sum := sha256.Sum256([]byte("juicemount.collections.id\x00" + opID))
	id := catalog.CollectionID("col_" + hex.EncodeToString(sum[:16]))
	changes := []catalog.Change{{CreateCollection: &catalog.CreateCollection{CollectionID: id, Name: name}}}
	for _, m := range members {
		changes = append(changes, catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: m}})
	}
	return c.change(opID, id, false, func() error { return c.authorize(members) }, changes)
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
	return c.change(opID, id, true, func() error { return c.authorizeAll(id) },
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
	return c.change(opID, id, true, func() error { return c.authorizeAll(id) },
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
	members, err := c.resolve(b.Members)
	if err != nil {
		return err
	}
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
	return c.change(opID, id, false, func() error { return c.authorize(members) }, changes)
}
