package manager

// agenttokens.go: continuity slice 6a, agent tokens (experimental, behind
// JM_CONTINUITY like collections; with it off every route is 404).
//
// An agent token lets a program a person runs (an AI agent, through the
// JuiceMount MCP server on their Mac) find files and edit collections as
// that person: the same grant checks, request budget and volume-read slot as
// the person's own Mac, on an allowlist of routes (agentguard.go). It never
// reaches file contents; agents read bytes through the mount.
//
// Minting. A person mints a token from one of their own approved Macs, with
// the Mac authentication collections use: POST /api/agent-tokens {name,
// expires_in_days?}. A token lives 90 days at most (the default); a person
// holds at most agentTokensPerPerson live tokens. The answer carries the
// token once: "jma_", its ID (16 hex), "_" and 32 random bytes in hex. The
// principal store keeps only a SHA-256 of it (the secret is 256 random bits,
// so a slow hash adds nothing), written as every other store change.
//
// Binding. A token belongs to its person and to the Mac that minted it, as
// that Mac was enrolled and approved then (its request and decision times).
// It works only while it has not expired or been revoked, its person exists,
// and that Mac is still approved, not flagged as cloned, assigned to that
// person and in the same approval. Every request checks this under the
// store lock, so any of those changes stops the token at once; and every
// store write drops the tokens that fail it, so revoking a Mac and approving
// it again, or reassigning it away and back, does not bring a token back.
//
// Lists and revokes. GET /api/agent-tokens: a person (from a Mac) lists
// their own tokens, the admin every token. POST /api/agent-tokens/{id}/revoke
// or DELETE /api/agent-tokens/{id}: a person revokes their own, the admin
// any. No agent may use these routes.
//
// Audit. The Manager keeps a catalog audit log (internal/auditlog format) in
// a directory of its own beside the principal store, opened on first use:
// a mint, a revoke and every catalog edit an agent makes is an intent record
// (who: the person, the minting Mac as principal, the token's ID and name;
// what: Action and Detail) and an outcome record. A mint and an agent's edit
// are refused (503) unless their intent is on disk first; a revoke goes
// ahead even when the log cannot record it. GET /api/audit?log=catalog reads
// it.

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
)

const (
	agentTokenMaxDays    = 90
	agentTokensPerPerson = 10
	agentTokenNameBytes  = 64
	// agentAuditDirName: the catalog audit log, beside the principal store.
	agentAuditDirName = "catalog-audit"
)

var (
	agentTokenRe   = regexp.MustCompile(`^jma_([0-9a-f]{16})_[0-9a-f]{64}$`)
	agentTokenIDRe = regexp.MustCompile(`^[0-9a-f]{16}$`)

	errAgentTokenInvalid  = errors.New("unknown or revoked agent token")
	errAgentTokenExpired  = errors.New("this agent token has expired; mint a new one on your Mac")
	errAgentTokenUnbound  = errors.New("this agent token no longer works: its Mac or its person changed")
	errAgentTokenNotFound = errors.New("no such agent token")

	// agentAuditTimeout bounds the wait for an intent record to reach the
	// disk (a variable for tests).
	agentAuditTimeout = 5 * time.Second
)

// AgentToken is one agent token as the principal store keeps it.
type AgentToken struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Person string `json:"person"`
	// Mac: the principal that minted it, and that Mac's enrollment request
	// and approval times then (its approval epoch).
	Mac            string `json:"mac"`
	MacRequestedAt int64  `json:"mac_requested_at_ms"`
	MacDecidedAt   int64  `json:"mac_decided_at_ms"`
	SecretSHA256   string `json:"secret_sha256"`
	CreatedAt      int64  `json:"created_at_ms"`
	ExpiresAt      int64  `json:"expires_at_ms"`
}

// AgentTokenView is a token as the API lists it (no hash).
type AgentTokenView struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Person     string `json:"person"`
	PersonName string `json:"person_name,omitempty"`
	Mac        string `json:"mac"`
	CreatedAt  int64  `json:"created_at_ms"`
	ExpiresAt  int64  `json:"expires_at_ms"`
}

func agentTokenHash(token string) string {
	return sha256Hex("juicemount.agent-token\x00" + token)
}

// newAgentToken returns a fresh token and its ID.
func newAgentToken() (id, token string, err error) {
	b := make([]byte, 8+32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	id = hex.EncodeToString(b[:8])
	return id, "jma_" + id + "_" + hex.EncodeToString(b[8:]), nil
}

// loadAgentTokens installs the tokens of a store file; a malformed entry is
// dropped (it can only fail closed).
func (s *PrincipalStore) loadAgentTokens(tokens []AgentToken) {
	s.agentTokens = map[string]*AgentToken{}
	for i := range tokens {
		t := tokens[i]
		if !agentTokenIDRe.MatchString(t.ID) || len(t.SecretSHA256) != 64 || s.agentTokens[t.ID] != nil || t.Person == "" || t.Mac == "" {
			log.Printf("manager: principal store: dropping a malformed agent token entry")
			continue
		}
		s.agentTokens[t.ID] = &t
	}
}

// agentTokenProblemLocked says why t no longer works (nil: it works).
func (s *PrincipalStore) agentTokenProblemLocked(t *AgentToken, now time.Time) error {
	if now.UnixMilli() >= t.ExpiresAt {
		return errAgentTokenExpired
	}
	mac := s.recs[t.Mac]
	if mac == nil || mac.Status != PrincipalApproved || mac.DeviceMismatch || mac.Person != t.Person ||
		mac.RequestedAt != t.MacRequestedAt || mac.DecidedAt != t.MacDecidedAt || s.people[t.Person] == nil {
		return errAgentTokenUnbound
	}
	return nil
}

// agentTokensToKeepLocked drops the tokens that no longer work and returns
// the rest, by ID, for the store file.
func (s *PrincipalStore) agentTokensToKeepLocked() []AgentToken {
	now := s.opts.Now()
	var out []AgentToken
	for id, t := range s.agentTokens {
		if err := s.agentTokenProblemLocked(t, now); err != nil {
			log.Printf("manager: agent token %s (%q) of person %s from %s dropped: %v", t.ID, t.Name, t.Person, t.Mac, err)
			delete(s.agentTokens, id)
			continue
		}
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// mintAgentToken records a token the Mac mac of person personID mints.
func (s *PrincipalStore) mintAgentToken(mac, personID, id, token, name string, days int) (AgentToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.recs[mac]
	if rec == nil || rec.Status != PrincipalApproved || rec.DeviceMismatch || rec.Person != personID || s.people[personID] == nil {
		return AgentToken{}, failf(http.StatusForbidden, "this Mac no longer belongs to that person")
	}
	now := s.opts.Now()
	n := 0
	for _, t := range s.agentTokens {
		if t.Person == personID && s.agentTokenProblemLocked(t, now) == nil {
			n++
		}
	}
	if n >= agentTokensPerPerson {
		return AgentToken{}, failf(http.StatusConflict, "%s already has %d agent tokens, the most one person may have; revoke one first", s.people[personID].Name, n)
	}
	if s.agentTokens[id] != nil {
		return AgentToken{}, errors.New("agent token ID collision")
	}
	t := &AgentToken{ID: id, Name: name, Person: personID, Mac: mac, MacRequestedAt: rec.RequestedAt, MacDecidedAt: rec.DecidedAt,
		SecretSHA256: agentTokenHash(token), CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(time.Duration(days) * 24 * time.Hour).UnixMilli()}
	s.agentTokens[id] = t
	if err := s.saveStoreLocked(); err != nil {
		delete(s.agentTokens, id)
		return AgentToken{}, err
	}
	return *t, nil
}

// agentPerson authenticates an agent token: the Mac that minted it, a copy
// of its person and the token. The secret is compared in constant time.
func (s *PrincipalStore) agentPerson(token string) (string, Person, AgentToken, error) {
	m := agentTokenRe.FindStringSubmatch(token)
	if m == nil {
		return "", Person{}, AgentToken{}, errAgentTokenInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.agentTokens[m[1]]
	if t == nil || subtle.ConstantTimeCompare([]byte(agentTokenHash(token)), []byte(t.SecretSHA256)) != 1 {
		return "", Person{}, AgentToken{}, errAgentTokenInvalid
	}
	if err := s.agentTokenProblemLocked(t, s.opts.Now()); err != nil {
		return "", Person{}, *t, err
	}
	p := *s.people[t.Person]
	p.Grants = append([]string(nil), p.Grants...)
	return t.Mac, p, *t, nil
}

// agentTokenViews lists the working tokens of a person ("" = everyone's),
// by person, then creation.
func (s *PrincipalStore) agentTokenViews(personID string) []AgentTokenView {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.opts.Now()
	out := []AgentTokenView{}
	for _, t := range s.agentTokens {
		if personID != "" && t.Person != personID || s.agentTokenProblemLocked(t, now) != nil {
			continue
		}
		v := AgentTokenView{ID: t.ID, Name: t.Name, Person: t.Person, Mac: t.Mac, CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt}
		if p := s.people[t.Person]; p != nil {
			v.PersonName = p.Name
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Person != out[j].Person {
			return out[i].Person < out[j].Person
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// revokeAgentToken deletes a token: any (personID "") or one of personID's.
func (s *PrincipalStore) revokeAgentToken(id, personID string) (AgentToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.agentTokens[id]
	if t == nil || personID != "" && t.Person != personID {
		return AgentToken{}, errAgentTokenNotFound
	}
	delete(s.agentTokens, id)
	if err := s.saveStoreLocked(); err != nil {
		s.agentTokens[id] = t
		return AgentToken{}, err
	}
	return *t, nil
}

// ---- the catalog audit log -------------------------------------------------

// catalogAuditDir is the catalog audit log's directory for a principal
// store ("" without one).
func catalogAuditDir(people *PrincipalStore) string {
	if people == nil || people.opts.StatePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(people.opts.StatePath), agentAuditDirName)
}

// agentAudit is the Manager's catalog audit log, opened on first use (so a
// Manager that never sees an agent creates nothing).
type agentAudit struct {
	dir string
	mu  sync.Mutex
	w   *auditlog.Writer
}

func (a *agentAudit) writer() *auditlog.Writer {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.w == nil && a.dir != "" {
		w, err := auditlog.Open(a.dir, auditlog.Options{})
		if err != nil {
			// w starts failed and retries; intents are refused meanwhile.
			log.Printf("manager: catalog audit log %s: %v", a.dir, err)
		}
		a.w = w
	}
	return a.w
}

// Close flushes and closes the log, if it was opened.
func (a *agentAudit) Close() {
	a.mu.Lock()
	w := a.w
	a.mu.Unlock()
	if w != nil {
		_ = w.Close()
	}
}

// intent records what is about to happen and waits until it is on disk;
// otherwise the change must not happen (503).
func (a *agentAudit) intent(r auditlog.Record) (uint64, error) {
	w := a.writer()
	if w == nil {
		return 0, failf(http.StatusServiceUnavailable, "the catalog audit log is not configured; nothing was changed")
	}
	r.Type = auditlog.TypeIntent
	seq, err := w.AppendDurable(&r, agentAuditTimeout)
	if err != nil {
		return 0, failf(http.StatusServiceUnavailable, "the catalog audit log cannot record this (%v); nothing was changed, try again", err)
	}
	return seq, nil
}

// outcome records how the intent unit ended (best effort: logged when it
// cannot be written).
func (a *agentAudit) outcome(unit uint64, who auditlog.Record, err error) {
	w := a.writer()
	if w == nil {
		return
	}
	rec := auditlog.Record{Type: auditlog.TypeOutcome, Unit: unit, Outcome: auditlog.OutcomeCommitted,
		Principal: who.Principal, Kind: who.Kind, Person: who.Person, PersonName: who.PersonName, Agent: who.Agent, AgentName: who.AgentName}
	if err != nil {
		rec.Outcome, rec.Reason = auditlog.OutcomeRefused, auditReason(err)
	}
	if _, aerr := w.Append(&rec); aerr != nil {
		log.Printf("manager: catalog audit log: outcome of record %d not written: %v", unit, aerr)
	}
}

// refused records a refused agent write (best effort).
func (a *agentAudit) refused(who auditlog.Record, action, detail string) {
	w := a.writer()
	if w == nil {
		return
	}
	who.Type, who.Action, who.Detail = auditlog.TypeRefused, action, detail
	if _, err := w.Append(&who); err != nil {
		log.Printf("manager: catalog audit log: refusal not written: %v", err)
	}
}

func auditReason(err error) string {
	var he *httpError
	if errors.As(err, &he) {
		return he.msg
	}
	return err.Error()
}

// auditWho is the actor as an audit record names it.
func (c collectionActor) auditWho() auditlog.Record {
	switch {
	case c.person == nil:
		return auditlog.Record{Principal: collectionAdminActor, Kind: "admin"}
	case c.agent != nil:
		return auditlog.Record{Principal: c.mac, Kind: "agent", Person: c.person.ID, PersonName: c.person.Name,
			Agent: c.agent.ID, AgentName: c.agent.Name}
	default:
		return auditlog.Record{Principal: c.mac, Kind: "mac", Person: c.person.ID, PersonName: c.person.Name}
	}
}

// ---- HTTP -----------------------------------------------------------------

func (h *collectionsAPI) serveAgentTokens(w http.ResponseWriter, r *http.Request) {
	h.serveWith(w, r, "/api/agent-tokens", func(string, []string) bool { return false }, h.routeAgentTokens)
}

// routeAgentTokens dispatches
//
//	GET    /api/agent-tokens              list (a person: theirs; the admin: all)
//	POST   /api/agent-tokens              mint {name, expires_in_days?} (a person's Mac only)
//	POST   /api/agent-tokens/{id}/revoke  revoke
//	DELETE /api/agent-tokens/{id}         revoke
func (h *collectionsAPI) routeAgentTokens(w http.ResponseWriter, r *http.Request, _ *continuityService, actor collectionActor, parts []string) error {
	if h.people == nil {
		return failf(http.StatusServiceUnavailable, "agent tokens need Mac enrollment (the principal store), which is not configured")
	}
	if r.Method != http.MethodGet && actor.open && h.people.grantsInForce() {
		return failf(http.StatusForbidden, "no admin key is configured while people's grants are in force; set an admin key to revoke agent tokens as the admin")
	}
	w.Header().Set("Cache-Control", "no-store")
	switch {
	case len(parts) == 0 && r.Method == http.MethodGet:
		personID := ""
		if actor.person != nil {
			personID = actor.person.ID
		}
		writeJSON(w, http.StatusOK, map[string]any{"agent_tokens": h.people.agentTokenViews(personID)})
		return nil
	case len(parts) == 0 && r.Method == http.MethodPost:
		return h.mintAgentToken(w, r, actor)
	case len(parts) == 0:
		return failf(http.StatusMethodNotAllowed, "GET or POST only")
	case !agentTokenIDRe.MatchString(parts[0]):
		return failf(http.StatusNotFound, "no such agent token")
	case len(parts) == 1 && r.Method == http.MethodDelete,
		len(parts) == 2 && parts[1] == "revoke" && r.Method == http.MethodPost:
		return h.revokeAgentToken(w, actor, parts[0])
	case len(parts) <= 2:
		return failf(http.StatusMethodNotAllowed, "POST .../revoke or DELETE only")
	}
	return failf(http.StatusNotFound, "no such route")
}

func (h *collectionsAPI) mintAgentToken(w http.ResponseWriter, r *http.Request, actor collectionActor) error {
	if actor.person == nil || actor.mac == "" {
		return failf(http.StatusForbidden, "an agent token is minted by its person, from one of their own Macs")
	}
	var b struct {
		Name          string `json:"name"`
		ExpiresInDays int    `json:"expires_in_days"`
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, 4096))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&b); err != nil {
		return fmt.Errorf("%w: invalid JSON", errCollectionInput)
	}
	name := strings.ToValidUTF8(cleanText(b.Name, agentTokenNameBytes), "")
	if name == "" || !utf8.ValidString(name) {
		return fmt.Errorf("%w: name the token (1-%d bytes), e.g. the agent that will use it", errCollectionInput, agentTokenNameBytes)
	}
	days := b.ExpiresInDays
	if days == 0 {
		days = agentTokenMaxDays
	}
	if days < 1 || days > agentTokenMaxDays {
		return fmt.Errorf("%w: expires_in_days is 1-%d (default %d)", errCollectionInput, agentTokenMaxDays, agentTokenMaxDays)
	}
	id, token, err := newAgentToken()
	if err != nil {
		return err
	}
	who := actor.auditWho()
	rec := who
	rec.Action, rec.Detail = "agent-token-mint", fmt.Sprintf("agent token %s %q, expires in %d days", id, name, days)
	unit, err := h.audit.intent(rec)
	if err != nil {
		return err
	}
	t, err := h.people.mintAgentToken(actor.mac, actor.person.ID, id, token, name, days)
	h.audit.outcome(unit, who, err)
	if err != nil {
		return err
	}
	log.Printf("manager: agent token %s (%q) minted by %s (%s) from %s", t.ID, t.Name, actor.person.ID, actor.person.Name, actor.mac)
	writeJSON(w, http.StatusOK, map[string]any{
		"token": token,
		"agent_token": AgentTokenView{ID: t.ID, Name: t.Name, Person: t.Person, PersonName: actor.person.Name, Mac: t.Mac,
			CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt},
	})
	return nil
}

func (h *collectionsAPI) revokeAgentToken(w http.ResponseWriter, actor collectionActor, id string) error {
	personID := ""
	if actor.person != nil {
		personID = actor.person.ID
	}
	who := actor.auditWho()
	rec := who
	rec.Action, rec.Detail = "agent-token-revoke", "agent token "+id
	// A revoke goes ahead even when the log cannot record it.
	unit, aerr := h.audit.intent(rec)
	t, err := h.people.revokeAgentToken(id, personID)
	if aerr == nil {
		h.audit.outcome(unit, who, err)
	} else {
		log.Printf("manager: agent token %s: revoke not audited: %v", id, aerr)
	}
	if errors.Is(err, errAgentTokenNotFound) {
		return failf(http.StatusNotFound, "no such agent token")
	}
	if err != nil {
		return err
	}
	log.Printf("manager: agent token %s (%q) of person %s revoked by %s", t.ID, t.Name, t.Person, actor.id())
	writeJSON(w, http.StatusOK, map[string]any{"revoked": t.ID})
	return nil
}
