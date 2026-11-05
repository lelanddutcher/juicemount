package manager

// collections.go: continuity slice 3, Collections (experimental, behind
// JM_CONTINUITY like the binder; with it off every route is 404). A
// collection is a named set of catalog assets, kept in the continuity
// catalog, so every change is an event in its ordered history with the
// actor that made it: "admin", or the Teams person's ID.
//
// Who. The admin key, or a Teams person through one of their Macs: the Mac
// sends its instance ID and the enrollment poll token it already holds for
// the Manager (principals.go; X-JuiceMount-Mac-Instance and
// X-JuiceMount-Mac-Token). The Mac must be approved and assigned to a
// person, and must not be flagged as possibly cloned.
//
// What. Reads are shared (Teams v1: any authenticated caller). A person may
// write only where their project write grants (people.go) cover the current
// path of every member the change touches, resolved live from the volume:
// the members added or removed, and for a rename or delete every member of
// the collection. A member whose file is gone needs no grant (it is in no
// project now); one whose path cannot be read refuses the change (503),
// since unknown is not gone. A person with no write access at all writes
// nothing. A refusal is 403 and is logged; the Manager writes no Teams audit
// records (the proxy's audit log has a single writer), so the catalog's
// event history is the record of what was changed and by whom.
//
// Members are asset IDs, added by ID or by a volume path the binder resolves
// (never through a path hint). A member's path is resolved when it is read;
// a member whose file is gone is listed as missing, not removed.
//
// Retries. A write may carry operation_id (1-64 letters, digits, - or _).
// The catalog operation ID is derived from it and the actor, so the same
// caller repeating the same request gets the original result, and a
// different request under the same operation_id is 409.

import (
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

	"github.com/lelanddutcher/juicemount/internal/continuity"
	"github.com/lelanddutcher/juicemount/internal/continuity/binder"
	"github.com/lelanddutcher/juicemount/internal/continuity/catalog"
)

const (
	collectionAdminActor = "admin"
	// One change per member, plus one for the collection itself.
	maxCollectionRequestMembers = catalog.MaxChanges - 1
	maxCollectionBody           = 256 << 10
	maxMemberPage               = 500
)

var (
	collectionKeyRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

	errMacUnknown      = errors.New("unknown Mac or wrong token")
	errMacNotApproved  = errors.New("this Mac is not approved")
	errMacNoPerson     = errors.New("this Mac belongs to no person; an admin assigns it on the People page")
	errMacDeviceClash  = errors.New("this Mac's identity may be held by two Macs; an admin must revoke and re-enroll it")
	errCollectionInput = errors.New("invalid collection request")
)

// macPerson authenticates a Mac by the enrollment poll token it holds and
// returns a copy of its person.
func (s *PrincipalStore) macPerson(instanceID, pollToken string) (Person, error) {
	instanceID = strings.TrimSpace(instanceID)
	if !instanceIDRe.MatchString(instanceID) || !pollTokenRe.MatchString(pollToken) {
		return Person{}, errMacUnknown
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.recs[macPrincipalName(instanceID)]
	if rec == nil || rec.PollTokenSHA256 == "" || !strings.EqualFold(rec.InstanceID, instanceID) ||
		subtle.ConstantTimeCompare([]byte(sha256Hex(pollToken)), []byte(rec.PollTokenSHA256)) != 1 {
		return Person{}, errMacUnknown
	}
	switch {
	case rec.Status != PrincipalApproved:
		return Person{}, errMacNotApproved
	case rec.DeviceMismatch:
		return Person{}, errMacDeviceClash
	case rec.Person == "" || s.people[rec.Person] == nil:
		return Person{}, errMacNoPerson
	}
	p := *s.people[rec.Person]
	p.Grants = append([]string(nil), p.Grants...)
	return p, nil
}

// collectionActor is who is calling: the admin, or a person.
type collectionActor struct {
	person *Person // nil: admin
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
// binder resolves it). A grant covers its folder and everything below it.
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

type collectionsAPI struct {
	api    *API
	people *PrincipalStore // nil: only the admin key
	prefix string
}

// registerCollections wires /api/collections. The handlers do their own
// authentication (admin key or a person's Mac) and read a.continuity on
// every request, so they can be registered before the binder starts.
func (a *API) registerCollections(mux *http.ServeMux, prefix string, people *PrincipalStore) {
	h := &collectionsAPI{api: a, people: people, prefix: prefix}
	mux.HandleFunc(prefix+"/api/collections", h.serve)
	mux.HandleFunc(prefix+"/api/collections/", h.serve)
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
	case key != "":
		if h.api.adminKey != "" && subtle.ConstantTimeCompare([]byte(key), []byte(h.api.adminKey)) != 1 {
			return collectionActor{}, failf(http.StatusUnauthorized, "missing or invalid X-JuiceMount-Admin-Key")
		}
		return collectionActor{}, nil
	case instance != "" || token != "":
		if h.people == nil {
			return collectionActor{}, failf(http.StatusUnauthorized, "Mac enrollment is not configured on this Manager")
		}
		p, err := h.people.macPerson(instance, token)
		switch {
		case errors.Is(err, errMacUnknown):
			return collectionActor{}, failf(http.StatusUnauthorized, "%v", err)
		case err != nil:
			return collectionActor{}, failf(http.StatusForbidden, "%v", err)
		}
		return collectionActor{person: &p}, nil
	case h.api.adminKey == "":
		return collectionActor{}, nil // no admin key configured: LAN/dev mode, as a.auth
	default:
		return collectionActor{}, failf(http.StatusUnauthorized, "missing or invalid X-JuiceMount-Admin-Key")
	}
}

func (h *collectionsAPI) serve(w http.ResponseWriter, r *http.Request) {
	svc := h.api.continuity
	if svc == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "continuity is off (JM_CONTINUITY)"})
		return
	}
	actor, err := h.authenticate(r)
	if err == nil {
		err = h.route(w, r, svc, actor)
	}
	if err == nil {
		return
	}
	var he *httpError
	if !errors.As(err, &he) {
		he = catalogFailure(err)
	}
	if he.code == http.StatusForbidden && actor.person != nil {
		log.Printf("manager: collections: refused %s %s for %s (%s): %s", r.Method, r.URL.Path, actor.person.ID, actor.person.Name, he.msg)
	}
	writeJSON(w, he.code, map[string]string{"error": he.msg})
}

func catalogFailure(err error) *httpError {
	switch {
	case errors.Is(err, errCollectionInput), errors.Is(err, catalog.ErrInvalid):
		return &httpError{http.StatusBadRequest, err.Error()}
	case errors.Is(err, catalog.ErrNotFound):
		return &httpError{http.StatusNotFound, "no such collection or asset"}
	case errors.Is(err, catalog.ErrChangedRequest):
		return &httpError{http.StatusConflict, "operation_id was already used for a different request"}
	case errors.Is(err, catalog.ErrCollectionFull):
		return &httpError{http.StatusConflict, fmt.Sprintf("a collection holds at most %d members", catalog.MaxCollectionMembers)}
	case errors.Is(err, catalog.ErrConflict):
		return &httpError{http.StatusConflict, "the collection changed meanwhile; try again"}
	case errors.Is(err, binder.ErrNotReady):
		return &httpError{http.StatusServiceUnavailable, err.Error()}
	default:
		log.Printf("manager: collections: %v", err)
		return &httpError{http.StatusInternalServerError, "the catalog request failed"}
	}
}

// route dispatches
//
//	GET  /api/collections                      list (after, limit)
//	POST /api/collections                      create {name, members?, operation_id?}
//	GET  /api/collections/{id}                 one collection
//	POST /api/collections/{id}/rename          {name, operation_id?}
//	POST /api/collections/{id}/delete          {operation_id?}
//	GET  /api/collections/{id}/members         members with current paths (after, limit)
//	POST /api/collections/{id}/members/add     {members, operation_id?}
//	POST /api/collections/{id}/members/remove  {members, operation_id?}
func (h *collectionsAPI) route(w http.ResponseWriter, r *http.Request, svc *continuityService, actor collectionActor) error {
	rest := strings.TrimPrefix(r.URL.Path, h.prefix+"/api/collections")
	rest = strings.Trim(rest, "/")
	parts := []string{}
	if rest != "" {
		parts = strings.Split(rest, "/")
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
		if err := want(http.MethodGet); err != nil {
			return err
		}
		return c.get(id)
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
// volume: state "present" (path set), "missing" (the file is gone; the
// member stays) or "unknown" (the volume could not be read).
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
	page, err := c.svc.store.ListCollections(c.r.Context(), c.library, catalog.CollectionID(after), limit)
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
	col, err := c.svc.store.GetCollection(c.r.Context(), c.library, id)
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
	after, limit, err := pageArgs(c.r, 100, maxMemberPage)
	if err != nil {
		return err
	}
	page, err := c.svc.store.ListCollectionMembers(c.r.Context(), c.library, id, continuity.AssetID(after), limit)
	if err != nil {
		return err
	}
	out := struct {
		Collection CollectionView `json:"collection"`
		Members    []MemberView   `json:"members"`
		Next       string         `json:"next,omitempty"`
	}{Collection: collectionView(col), Members: []MemberView{}}
	for _, m := range page {
		v, err := c.svc.binder.AssetByID(c.r.Context(), m.AssetID)
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

func (c *collectionCall) body() (collectionBody, error) {
	var b collectionBody
	dec := json.NewDecoder(io.LimitReader(c.r.Body, maxCollectionBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
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

// resolve turns member references into asset views, in request order,
// without duplicates.
func (c *collectionCall) resolve(refs []memberRef) ([]binder.AssetView, error) {
	out := make([]binder.AssetView, 0, len(refs))
	seen := map[continuity.AssetID]bool{}
	for _, ref := range refs {
		var v binder.AssetView
		var err error
		switch {
		case (ref.AssetID == "") == (ref.Path == ""):
			return nil, fmt.Errorf("%w: a member names exactly one of asset_id or path", errCollectionInput)
		case ref.Path != "":
			v, err = c.svc.binder.AssetByPath(c.r.Context(), ref.Path)
			if errors.Is(err, catalog.ErrNotFound) {
				return nil, failf(http.StatusNotFound, "no catalogued file at %q", ref.Path)
			}
		default:
			v, err = c.svc.binder.AssetByID(c.r.Context(), ref.AssetID)
			if errors.Is(err, catalog.ErrNotFound) {
				return nil, failf(http.StatusNotFound, "asset %s is not in the catalog", ref.AssetID)
			}
		}
		if errors.Is(err, catalog.ErrInvalid) {
			return nil, fmt.Errorf("%w: bad asset_id or path", errCollectionInput)
		}
		if err != nil {
			return nil, err
		}
		if !seen[v.AssetID] {
			seen[v.AssetID] = true
			out = append(out, v)
		}
	}
	return out, nil
}

// authorize checks a person's grants against the current path of every
// member view. The admin is always allowed.
func (c *collectionCall) authorize(members []binder.AssetView) error {
	if c.actor.person == nil {
		return nil
	}
	if !c.actor.writesAnywhere() {
		return failf(http.StatusForbidden, "%s has no write access, so may not change collections", c.actor.person.Name)
	}
	for _, v := range members {
		switch memberState(v) {
		case "unknown":
			return failf(http.StatusServiceUnavailable, "cannot resolve the current path of %s to check grants (%s); try again", v.AssetID, v.ResolveError)
		case "present":
			if !c.actor.covers(v.Path) {
				return failf(http.StatusForbidden, "%s may not write at /%s: no project grant covers it", c.actor.person.Name, v.Path)
			}
		}
	}
	return nil
}

// authorizeAll checks every active member of a collection (rename, delete).
func (c *collectionCall) authorizeAll(id catalog.CollectionID) error {
	if c.actor.person == nil {
		return nil
	}
	var after continuity.AssetID
	for {
		page, err := c.svc.store.ListCollectionMembers(c.r.Context(), c.library, id, after, catalog.MaxPageSize)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return c.authorize(nil) // a person with no write access, empty collection
		}
		views := make([]binder.AssetView, 0, len(page))
		for _, m := range page {
			v, err := c.svc.binder.AssetByID(c.r.Context(), m.AssetID)
			if err != nil {
				return err
			}
			views = append(views, v)
		}
		if err := c.authorize(views); err != nil {
			return err
		}
		after = page[len(page)-1].AssetID
	}
}

// isRetry: this operation ID is already in the catalog's history, so the
// request may be an exact retry of a change that committed.
func (c *collectionCall) isRetry(opID string) (bool, error) {
	_, err := c.svc.store.GetEvent(c.r.Context(), c.library, opID)
	if errors.Is(err, catalog.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// apply records the change and answers with the collection as it is now.
func (c *collectionCall) apply(opID string, id catalog.CollectionID, changes []catalog.Change) error {
	receipt, err := c.svc.store.Apply(c.r.Context(), catalog.Operation{
		Version: catalog.OperationVersion, LibraryID: c.library, ID: opID, Actor: c.actor.id(), Changes: changes})
	if err != nil {
		return err
	}
	col, err := c.svc.store.GetCollection(c.r.Context(), c.library, id)
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
	if len(b.Members) > maxCollectionRequestMembers-1 {
		return fmt.Errorf("%w: at most %d members when creating", errCollectionInput, maxCollectionRequestMembers-1)
	}
	members, err := c.resolve(b.Members)
	if err != nil {
		return err
	}
	if err := c.authorize(members); err != nil {
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
	for _, v := range members {
		changes = append(changes, catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: v.AssetID}})
	}
	return c.apply(opID, id, changes)
}

// existing reads the target of a change to an existing collection. A
// removed one is gone, except for an exact retry of the change that may
// have removed it.
func (c *collectionCall) existing(id catalog.CollectionID, opID string) error {
	col, err := c.svc.store.GetCollection(c.r.Context(), c.library, id)
	if errors.Is(err, catalog.ErrNotFound) {
		return failf(http.StatusNotFound, "no such collection")
	}
	if err != nil {
		return err
	}
	if col.Removed {
		retry, err := c.isRetry(opID)
		if err != nil {
			return err
		}
		if !retry {
			return failf(http.StatusNotFound, "no such collection")
		}
	}
	return nil
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
	if err := c.existing(id, opID); err != nil {
		return err
	}
	if err := c.authorizeAll(id); err != nil {
		return err
	}
	return c.apply(opID, id, []catalog.Change{{RenameCollection: &catalog.RenameCollection{CollectionID: id, Name: name}}})
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
	if err := c.existing(id, opID); err != nil {
		return err
	}
	if err := c.authorizeAll(id); err != nil {
		return err
	}
	return c.apply(opID, id, []catalog.Change{{RemoveCollection: &catalog.RemoveCollection{CollectionID: id}}})
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
	if err := c.existing(id, opID); err != nil {
		return err
	}
	members, err := c.resolve(b.Members)
	if err != nil {
		return err
	}
	if err := c.authorize(members); err != nil {
		return err
	}
	changes := make([]catalog.Change, 0, len(members))
	for _, v := range members {
		if add {
			changes = append(changes, catalog.Change{AddCollectionMember: &catalog.AddCollectionMember{CollectionID: id, AssetID: v.AssetID}})
		} else {
			changes = append(changes, catalog.Change{RemoveCollectionMember: &catalog.RemoveCollectionMember{CollectionID: id, AssetID: v.AssetID}})
		}
	}
	return c.apply(opID, id, changes)
}
