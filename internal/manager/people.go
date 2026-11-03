package manager

// people.go: Teams slice 1. People, the Macs that belong to them, and their
// write grants, kept in the principal store and sent to the RESP proxy in
// the principals file (internal/respproxy/grants.go enforces them).
//
// A person starts with access "all": their Macs write everywhere, exactly
// like a Mac with no person. The first grant switches the person to access
// "granted": from then on their Macs may write only inside granted folders
// (a client folder, "/Clients", or a project, "/Clients/A"). Removing the
// last grant leaves the person with no write access at all; only setting
// access back to "all" lifts the restriction.
//
// File format. People travel in a version-2 principals file
// ({"version":2,"principals":[...],"people":[...]}). A proxy built before
// version 2 refuses such a file (unknown fields), so the Manager writes it
// only while the proxy's reload heartbeat advertises version 2
// ("principals-format=2"). With no fresh heartbeat it keeps the last format
// it saw (or the one of the file on disk at start), so a proxy restart
// cannot strip grants; a fresh heartbeat without the token (an older proxy
// image) switches it back to version 1, without people. While no person
// exists the file is exactly the version-1 file of before.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

// Person is one member of the team (Manager store).
type Person struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Access    string   `json:"access"` // personAccessAll or personAccessGranted
	Grants    []string `json:"grants,omitempty"`
	CreatedAt int64    `json:"created_at_ms"`
}

const (
	personAccessAll     = "all"
	personAccessGranted = "granted"
	// maxGrantLevels: a grant names a client folder (1) or a project (2).
	maxGrantLevels   = 2
	maxPeopleInStore = 1024
	maxGrantsPerUser = 256
	// heartbeatFormatToken mirrors respproxy.HeartbeatFormatToken.
	heartbeatFormatToken = "principals-format="
)

var (
	ErrPersonNotFound = errors.New("no such person")
	ErrPersonInvalid  = errors.New("invalid person request")
)

// proxyPersonEntry mirrors respproxy.PersonEntry.
type proxyPersonEntry struct {
	ID      string   `json:"id"`
	Name    string   `json:"name,omitempty"`
	Access  string   `json:"access"`
	Grants  []string `json:"grants,omitempty"`
	Devices []string `json:"devices,omitempty"`
}

// cleanGrantPath validates a grant: an absolute path of one or two folder
// names, no "." or "..", no control characters, no trailing slash. The
// proxy (respproxy.CleanGrantPath) checks the same rules again.
func cleanGrantPath(p string) (string, error) {
	p = strings.TrimSpace(p)
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%w: a grant is an absolute folder path such as /Clients or /Clients/Project", ErrPersonInvalid)
	}
	t := strings.TrimSuffix(p, "/")
	parts := strings.Split(strings.TrimPrefix(t, "/"), "/")
	if t == "" || len(parts) > maxGrantLevels {
		return "", fmt.Errorf("%w: grant a client folder (/Clients) or a project (/Clients/Project)", ErrPersonInvalid)
	}
	for _, c := range parts {
		if c == "" || c == "." || c == ".." || len(c) > 255 {
			return "", fmt.Errorf("%w: bad folder name in %q", ErrPersonInvalid, p)
		}
		for i := 0; i < len(c); i++ {
			if c[i] < 0x20 || c[i] == 0x7f {
				return "", fmt.Errorf("%w: control character in %q", ErrPersonInvalid, p)
			}
		}
	}
	return t, nil
}

// heartbeatFormat reads the principals format a fresh heartbeat names.
// ok=false: no fresh heartbeat.
func heartbeatFormat(dir string) (format int, ok bool) {
	path := filepath.Join(dir, reloadHeartbeatName)
	st, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	if age := time.Since(st.ModTime()); age > proxyHeartbeatMaxAge || age < -proxyHeartbeatMaxAge {
		return 0, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	format = 1
	for _, f := range strings.Fields(string(b)) {
		if v, found := strings.CutPrefix(f, heartbeatFormatToken); found {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				format = n
			}
		}
	}
	return format, true
}

// fileFormat is the version of the principals file on disk (1 if absent).
func fileFormat(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, proxyPrincipalsName))
	if err != nil {
		return 1
	}
	var f struct {
		Version int `json:"version"`
	}
	if json.Unmarshal(b, &f) != nil || f.Version < 1 {
		return 1
	}
	return f.Version
}

// proxyFormatLocked is the principals file version to write (see the file
// comment).
func (s *PrincipalStore) proxyFormatLocked() int {
	if s.opts.ProxyDir == "" {
		return 1
	}
	if f, ok := heartbeatFormat(s.opts.ProxyDir); ok {
		s.proxyFormat = f
	}
	if s.proxyFormat == 0 {
		s.proxyFormat = fileFormat(s.opts.ProxyDir)
	}
	return s.proxyFormat
}

// proxyPeopleLocked returns the people section for the proxy file, or nil
// when the file must stay version 1.
func (s *PrincipalStore) proxyPeopleLocked() []proxyPersonEntry {
	if len(s.people) == 0 || s.proxyFormatLocked() < 2 {
		return nil
	}
	devices := map[string][]string{}
	for _, r := range s.approvedLocked() {
		if r.Person != "" {
			devices[r.Person] = append(devices[r.Person], r.Name)
		}
	}
	out := make([]proxyPersonEntry, 0, len(s.people))
	for _, p := range s.peopleLocked() {
		out = append(out, proxyPersonEntry{ID: p.ID, Name: p.Name, Access: p.Access,
			Grants: append([]string(nil), p.Grants...), Devices: devices[p.ID]})
	}
	return out
}

func (s *PrincipalStore) peopleLocked() []*Person {
	out := make([]*Person, 0, len(s.people))
	for _, p := range s.people {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// loadPeople installs the people of a store file.
func (s *PrincipalStore) loadPeople(people []Person) error {
	s.people = map[string]*Person{}
	for i := range people {
		p := people[i]
		if p.ID == "" || s.people[p.ID] != nil {
			return fmt.Errorf("principal store: bad or duplicate person id %q", p.ID)
		}
		if p.Access != personAccessAll && p.Access != personAccessGranted {
			return fmt.Errorf("principal store: person %s has access %q", p.ID, p.Access)
		}
		s.people[p.ID] = &p
	}
	for _, r := range s.recs {
		if r.Person != "" && s.people[r.Person] == nil {
			log.Printf("manager: principal store: %s names unknown person %s; unassigned", r.Name, r.Person)
			r.Person = ""
		}
	}
	return nil
}

// peopleState is a copy of what a people change may modify, to undo a
// change whose proxy file or store write failed.
type peopleState struct {
	people  map[string]Person
	persons map[string]string
}

func (s *PrincipalStore) savePeopleStateLocked() peopleState {
	st := peopleState{people: map[string]Person{}, persons: map[string]string{}}
	for id, p := range s.people {
		c := *p
		c.Grants = append([]string(nil), p.Grants...)
		st.people[id] = c
	}
	for n, r := range s.recs {
		st.persons[n] = r.Person
	}
	return st
}

func (s *PrincipalStore) restorePeopleStateLocked(st peopleState) {
	s.people = map[string]*Person{}
	for id, p := range st.people {
		c := p
		s.people[id] = &c
	}
	for n, r := range s.recs {
		r.Person = st.persons[n]
	}
}

// changePeople runs fn under the store lock and commits (store + proxy
// file); on any error nothing changes.
func (s *PrincipalStore) changePeople(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.people == nil {
		s.people = map[string]*Person{}
	}
	before := s.savePeopleStateLocked()
	if err := fn(); err != nil {
		s.restorePeopleStateLocked(before)
		return err
	}
	if err := s.commitLocked(s.opts.ProxyDir != ""); err != nil {
		s.restorePeopleStateLocked(before)
		return err
	}
	return nil
}

func (s *PrincipalStore) person(id string) (*Person, error) {
	p := s.people[id]
	if p == nil {
		return nil, ErrPersonNotFound
	}
	return p, nil
}

func cleanPersonName(name string) (string, error) {
	n := cleanText(name, 128)
	if n == "" {
		return "", fmt.Errorf("%w: a person needs a name", ErrPersonInvalid)
	}
	return n, nil
}

// CreatePerson adds a person with access "all".
func (s *PrincipalStore) CreatePerson(name string) (Person, error) {
	n, err := cleanPersonName(name)
	if err != nil {
		return Person{}, err
	}
	var out Person
	err = s.changePeople(func() error {
		if len(s.people) >= maxPeopleInStore {
			return fmt.Errorf("%w: at most %d people", ErrPersonInvalid, maxPeopleInStore)
		}
		for _, p := range s.people {
			if strings.EqualFold(p.Name, n) {
				return fmt.Errorf("%w: a person named %q exists", ErrPersonInvalid, n)
			}
		}
		b := make([]byte, 8)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		p := &Person{ID: "p-" + hex.EncodeToString(b), Name: n, Access: personAccessAll, CreatedAt: s.opts.Now().UnixMilli()}
		s.people[p.ID] = p
		out = *p
		return nil
	})
	return out, err
}

// UpdatePerson renames a person and/or sets its access.
func (s *PrincipalStore) UpdatePerson(id string, name, access *string) error {
	return s.changePeople(func() error {
		p, err := s.person(id)
		if err != nil {
			return err
		}
		if name != nil {
			n, err := cleanPersonName(*name)
			if err != nil {
				return err
			}
			p.Name = n
		}
		if access != nil {
			if *access != personAccessAll && *access != personAccessGranted {
				return fmt.Errorf("%w: access is %q or %q", ErrPersonInvalid, personAccessAll, personAccessGranted)
			}
			p.Access = *access
		}
		return nil
	})
}

// DeletePerson removes a person; its Macs keep working without a person.
func (s *PrincipalStore) DeletePerson(id string) error {
	return s.changePeople(func() error {
		if _, err := s.person(id); err != nil {
			return err
		}
		delete(s.people, id)
		for _, r := range s.recs {
			if r.Person == id {
				r.Person = ""
			}
		}
		return nil
	})
}

// AssignDevice gives the Mac principal device to a person ("" = none).
func (s *PrincipalStore) AssignDevice(device, personID string) error {
	return s.changePeople(func() error {
		r := s.recs[device]
		if r == nil {
			return ErrPrincipalNotFound
		}
		if personID != "" {
			if _, err := s.person(personID); err != nil {
				return err
			}
		}
		r.Person = personID
		return nil
	})
}

// AddGrant lets a person write in path; the first grant limits the person
// to its grants.
func (s *PrincipalStore) AddGrant(id, path string) error {
	c, err := cleanGrantPath(path)
	if err != nil {
		return err
	}
	return s.changePeople(func() error {
		p, err := s.person(id)
		if err != nil {
			return err
		}
		for _, g := range p.Grants {
			if g == c {
				return nil
			}
		}
		if len(p.Grants) >= maxGrantsPerUser {
			return fmt.Errorf("%w: at most %d grants", ErrPersonInvalid, maxGrantsPerUser)
		}
		p.Grants = append(p.Grants, c)
		sort.Strings(p.Grants)
		p.Access = personAccessGranted
		return nil
	})
}

// RemoveGrant takes a grant away. The person stays limited to its grants.
func (s *PrincipalStore) RemoveGrant(id, path string) error {
	c, err := cleanGrantPath(path)
	if err != nil {
		return err
	}
	return s.changePeople(func() error {
		p, err := s.person(id)
		if err != nil {
			return err
		}
		kept := p.Grants[:0:0]
		for _, g := range p.Grants {
			if g != c {
				kept = append(kept, g)
			}
		}
		if len(kept) == len(p.Grants) {
			return fmt.Errorf("%w: %s has no grant %q", ErrPersonInvalid, p.Name, c)
		}
		p.Grants = kept
		return nil
	})
}

// PersonView is a person with its Macs.
type PersonView struct {
	Person
	Devices []string `json:"devices"`
}

// DeviceView is one Mac principal and its person.
type DeviceView struct {
	Name   string          `json:"name"`
	Host   string          `json:"host,omitempty"`
	Label  string          `json:"label,omitempty"`
	Status PrincipalStatus `json:"status"`
	Person string          `json:"person,omitempty"`
}

// PeopleSnapshot is the admin view of people and enforcement.
type PeopleSnapshot struct {
	People  []PersonView `json:"people"`
	Devices []DeviceView `json:"devices"`
	// ProxyFormat: the principals file version the proxy reads (1: it
	// predates people, grants cannot be enforced).
	ProxyFormat int `json:"proxy_format"`
	// GrantsSent: the principals file carries the people; GrantsApplied:
	// the proxy also applied that exact file.
	GrantsSent    bool   `json:"grants_sent"`
	GrantsApplied bool   `json:"grants_applied"`
	Problem       string `json:"problem,omitempty"`
}

// PeopleSnapshot returns people, devices and the enforcement state.
func (s *PrincipalStore) PeopleSnapshot() PeopleSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := PeopleSnapshot{People: []PersonView{}, Devices: []DeviceView{}, ProxyFormat: s.proxyFormatLocked()}
	devices := map[string][]string{}
	names := make([]string, 0, len(s.recs))
	for n := range s.recs {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		r := s.recs[n]
		out.Devices = append(out.Devices, DeviceView{Name: r.Name, Host: r.Host, Label: r.Label, Status: r.Status, Person: r.Person})
		if r.Person != "" {
			devices[r.Person] = append(devices[r.Person], r.Name)
		}
	}
	scoped := false
	for _, p := range s.peopleLocked() {
		v := PersonView{Person: *p, Devices: devices[p.ID]}
		if v.Devices == nil {
			v.Devices = []string{}
		}
		out.People = append(out.People, v)
		scoped = scoped || p.Access == personAccessGranted
	}
	out.GrantsSent = s.peopleWritten
	if s.opts.ProxyDir != "" {
		ack := requestLine(filepath.Join(s.opts.ProxyDir, reloadAckName))
		acked := s.issuedAckLocked(ack)
		out.GrantsApplied = out.GrantsSent && acked >= 0 && acked == len(s.issued)-1 &&
			ack == requestLine(filepath.Join(s.opts.ProxyDir, reloadRequestName))
	}
	switch {
	case !scoped:
	case s.opts.ProxyDir == "":
		out.Problem = "no proxy directory is configured (JM_RESPPROXY_DIR): grants are not enforced"
	case !out.GrantsSent:
		out.Problem = "the Redis proxy does not support person grants (update the jm-respproxy image): grants are not enforced"
	case !out.GrantsApplied:
		out.Problem = "the Redis proxy has not applied the latest grants yet"
	}
	return out
}

// syncPeopleLocked rewrites the proxy file when the people it should carry
// differ from what was last written (the proxy image changed).
func (s *PrincipalStore) syncPeopleLocked() error {
	want := len(s.people) > 0 && s.proxyFormatLocked() >= 2
	if want == s.peopleWritten {
		return nil
	}
	return s.writeProxyFilesLocked()
}

// peopleSyncEvery: how often StartPeopleSync checks the proxy's format.
var peopleSyncEvery = 2 * time.Second

// StartPeopleSync keeps the principals file's format in step with the
// proxy (an upgrade gets the people, a downgrade a file it can read).
func (s *PrincipalStore) StartPeopleSync() (stop func()) {
	if s.opts.ProxyDir == "" {
		return func() {}
	}
	done, exited := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(exited)
		t := time.NewTicker(peopleSyncEvery)
		defer t.Stop()
		lastErr := ""
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			s.mu.Lock()
			err := s.syncPeopleLocked()
			s.mu.Unlock()
			if err != nil && err.Error() != lastErr {
				log.Printf("manager: people: rewriting the proxy principals file: %v", err)
			}
			if err != nil {
				lastErr = err.Error()
			} else {
				lastErr = ""
			}
		}
	}()
	var once bool
	return func() {
		if !once {
			once = true
			close(done)
			<-exited
		}
	}
}

// ---- HTTP -----------------------------------------------------------------

type peopleAPI struct {
	store    *PrincipalStore
	auditDir string
}

func (p *peopleAPI) ready(w http.ResponseWriter) bool {
	if p == nil || p.store == nil {
		http.Error(w, "people are not available: principal enrollment is not configured", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func (p *peopleAPI) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if !p.ready(w) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, p.store.PeopleSnapshot())
}

// peopleRequest is the body of every people POST.
type peopleRequest struct {
	ID     string  `json:"id"`
	Name   *string `json:"name"`
	Access *string `json:"access"`
	Device string  `json:"device"`
	Person string  `json:"person"`
	Path   string  `json:"path"`
}

// handleChange wraps one people mutation: POST, JSON body, snapshot reply.
func (p *peopleAPI) handleChange(op func(peopleRequest) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		if !p.ready(w) {
			return
		}
		var body peopleRequest
		dec := json.NewDecoder(io.LimitReader(r.Body, 8192))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		switch err := op(body); {
		case errors.Is(err, ErrPersonInvalid):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, ErrPersonNotFound):
			http.Error(w, "no such person", http.StatusNotFound)
		case errors.Is(err, ErrPrincipalNotFound):
			http.Error(w, "no such Mac", http.StatusNotFound)
		case err != nil:
			log.Printf("manager: people change failed: %v", err)
			http.Error(w, "the change could not be written; nothing changed", http.StatusInternalServerError)
		default:
			writeJSON(w, http.StatusOK, p.store.PeopleSnapshot())
		}
	}
}

func parseMillis(v string) (int64, error) {
	if v == "" {
		return 0, nil
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// handleAudit is GET /api/audit: one page of the proxy's mutation log,
// newest first. Filters: person, principal, project (a path: that folder
// and below), since/until (unix ms or RFC 3339), type; paging: before (the
// previous page's "next"), limit (default 100, at most 1000).
func (p *peopleAPI) handleAudit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if p.auditDir == "" {
		http.Error(w, "the audit log is not configured (JM_RESPPROXY_DIR or JM_AUDIT_DIR)", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	f := auditlog.Filter{Person: q.Get("person"), Principal: q.Get("principal"), Project: q.Get("project")}
	if t := q.Get("type"); t != "" {
		f.Types = strings.Split(t, ",")
	}
	var err error
	if f.Since, err = parseMillis(q.Get("since")); err == nil {
		f.Until, err = parseMillis(q.Get("until"))
	}
	var before uint64
	if err == nil && q.Get("before") != "" {
		before, err = strconv.ParseUint(q.Get("before"), 10, 64)
	}
	limit := 100
	if err == nil && q.Get("limit") != "" {
		limit, err = strconv.Atoi(q.Get("limit"))
	}
	if err != nil || limit < 1 || limit > 1000 {
		http.Error(w, "bad query: since/until are unix ms or RFC 3339, before a sequence number, limit 1-1000", http.StatusBadRequest)
		return
	}
	page, err := auditlog.Query(p.auditDir, f, before, limit)
	if err != nil {
		log.Printf("manager: audit query: %v", err)
		http.Error(w, "the audit log could not be read", http.StatusInternalServerError)
		return
	}
	if page.Records == nil {
		page.Records = []auditlog.Record{}
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, page)
}

// handleAuditVerify is GET /api/audit/verify: checks the whole hash chain.
func (p *peopleAPI) handleAuditVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if p.auditDir == "" {
		http.Error(w, "the audit log is not configured (JM_RESPPROXY_DIR or JM_AUDIT_DIR)", http.StatusServiceUnavailable)
		return
	}
	rep, err := auditlog.Verify(p.auditDir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Printf("manager: audit verify: %v", err)
		http.Error(w, "the audit log could not be read", http.StatusInternalServerError)
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		rep = auditlog.Report{OK: true}
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, rep)
}

// registerPeople wires the people and audit routes (admin key, like every
// other Manager route).
func (a *API) registerPeople(mux *http.ServeMux, prefix string, store *PrincipalStore) {
	pa := &peopleAPI{store: store, auditDir: strings.TrimSpace(os.Getenv("JM_AUDIT_DIR"))}
	if pa.auditDir == "" {
		if dir := strings.TrimSpace(os.Getenv("JM_RESPPROXY_DIR")); dir != "" {
			pa.auditDir = filepath.Join(dir, "audit")
		}
	}
	if store != nil {
		a.jobs.onStopAll(store.StartPeopleSync())
	}
	mux.HandleFunc(prefix+"/api/people", a.auth(pa.handleList))
	mux.HandleFunc(prefix+"/api/people/create", a.auth(pa.handleChange(func(b peopleRequest) error {
		if b.Name == nil {
			return fmt.Errorf("%w: name is required", ErrPersonInvalid)
		}
		_, err := pa.store.CreatePerson(*b.Name)
		return err
	})))
	mux.HandleFunc(prefix+"/api/people/update", a.auth(pa.handleChange(func(b peopleRequest) error {
		return pa.store.UpdatePerson(b.ID, b.Name, b.Access)
	})))
	mux.HandleFunc(prefix+"/api/people/delete", a.auth(pa.handleChange(func(b peopleRequest) error {
		return pa.store.DeletePerson(b.ID)
	})))
	mux.HandleFunc(prefix+"/api/people/assign", a.auth(pa.handleChange(func(b peopleRequest) error {
		if !macNameRe.MatchString(b.Device) {
			return fmt.Errorf("%w: device must be a Mac principal (mac-<12 hex>)", ErrPersonInvalid)
		}
		return pa.store.AssignDevice(b.Device, b.Person)
	})))
	mux.HandleFunc(prefix+"/api/people/grant", a.auth(pa.handleChange(func(b peopleRequest) error {
		return pa.store.AddGrant(b.ID, b.Path)
	})))
	mux.HandleFunc(prefix+"/api/people/ungrant", a.auth(pa.handleChange(func(b peopleRequest) error {
		return pa.store.RemoveGrant(b.ID, b.Path)
	})))
	mux.HandleFunc(prefix+"/api/audit", a.auth(pa.handleAudit))
	mux.HandleFunc(prefix+"/api/audit/verify", a.auth(pa.handleAuditVerify))
}
